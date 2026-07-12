"""Queue component for scrapy-extension.

This module provides a Scrapy queue component that uses backend queue interfaces.
"""

from __future__ import annotations

import base64
import binascii
import logging
import time
import warnings
from collections import deque
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from scrapy.utils.request import request_from_dict

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.exceptions import SerializationError
from scrapy_extension.monitor import NullMonitor, ScrapyStatsMonitor
from scrapy_extension.monitor.base import DEFAULT_POP_RATE_WINDOW_S, Monitor
from scrapy_extension.queue.strategies.base import QueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.http import Request

  from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)

#: Default per-item serialized-byte cap (1 MiB — matches Memcached's 1 MB ceiling).
DEFAULT_QUEUE_MAX_ITEM_BYTES = 1_048_576

#: Meta key carrying the backend ack token from pop → request → response → ack.
#: Atomic-pop backends set this to ``None`` (harmless); message-queue backends
#: (Kafka, RabbitMQ) set it to a backend-specific token so the scheduler can
#: ack the *specific* message that produced this request — correct under
#: ``CONCURRENT_REQUESTS > 1``.
BACKEND_ACK_TOKEN_META_KEY = "_backend_ack_token"  # nosec B105


class BackendQueue:
  """Scrapy queue implementation using backend queue interface.

  This class wraps a QueueBackend to provide Scrapy-compatible
  queue operations for requests.

  Attributes:
      connection_manager: The connection manager for backend access.
      queue_name: The name of the queue.
      serializer: Serializer for encoding/decoding requests.
      spider: Optional spider reference for callback/errback resolution during deserialization.
  """

  #: Default depth-sampling window (U4 — see ``__init__`` depth_sample_every).
  DEFAULT_DEPTH_SAMPLE_EVERY = 100

  def __init__(
    self,
    connection_manager: ConnectionManager,
    queue_name: str,
    *,
    spider: Spider | None = None,
    queue_strategy: QueueStrategy | None = None,
    max_item_bytes: int = DEFAULT_QUEUE_MAX_ITEM_BYTES,
    monitor: Monitor | None = None,
    depth_sample_every: int = DEFAULT_DEPTH_SAMPLE_EVERY,
    pop_rate_window_s: float = DEFAULT_POP_RATE_WINDOW_S,
  ) -> None:
    """Initialize the backend queue.

    Args:
        connection_manager: Connection manager for backend access.
        queue_name: Name of the queue.
        spider: Optional spider reference for restoring callback/errback
            functions during request deserialization.
        queue_strategy: Optional queue-semantics strategy. When ``None``
            (default), a ``PassthroughQueueStrategy`` delegates push/pop to the
            QueueBackend unchanged — preserving the pre-strategy behavior.
        max_item_bytes: Maximum serialized bytes permitted for a single queued
            request. Oversize payloads raise ``SerializationError`` at push
            time (D2 — DoS guard against capped storage backends).
        monitor: Optional observability monitor. When ``None`` (default),
            resolved default-on: if ``spider.crawler.stats`` is reachable a
            :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` is wired;
            otherwise a :class:`~scrapy_extension.monitor.NullMonitor` (no-op,
            no crash). Emitted hooks are additive — existing stat keys are
            unchanged.
        depth_sample_every: U4 perf — only probe real backend depth
            (``queue_len`` / ZCARD) once every N calls while the cached depth
            is non-zero; in between, return the cached depth. Default ``100``
            cuts ~25% off pop-path RTT (depth changes slowly vs pop rate; 1/100
            sampling keeps variance ~1%). ``1`` preserves the pre-U4 behavior
            (probe every call). Emptiness is always fresh: when the cached
            depth is ``0`` (or unknown) every call re-probes for real, so the
            drain surfaces on the very next call and Scrapy idle detection
            stays correct — sampling only amortizes the RPC while the queue is
            observably non-empty (the active-crawl steady state).
        pop_rate_window_s: U2 operability — rolling window (seconds) over
            which the ``queue/pop_rate`` gauge is computed. Default
            :data:`~scrapy_extension.monitor.base.DEFAULT_POP_RATE_WINDOW_S`
            (60.0). Round-14 R14-C: threaded via
            ``BackendScheduler.from_settings`` so the window is tunable
            without code changes (round-12 U2 left it stuck at the default).
    """
    self.connection_manager = connection_manager
    self.queue_name = queue_name
    self._spider = spider
    self.max_item_bytes = max_item_bytes
    self.depth_sample_every = max(1, int(depth_sample_every))
    self._pop_rate_window_s = pop_rate_window_s
    self._strategy: QueueStrategy = (
      queue_strategy
      if queue_strategy is not None
      else PassthroughQueueStrategy(connection_manager)
    )
    self._monitor: Monitor = monitor if monitor is not None else self._resolve_monitor(spider)
    # U4 depth-sampling state — see ``_probe_depth``. ``None`` forces the next
    # probe through to the backend; a real ``0`` is always cached verbatim so
    # emptiness is never masked by a stale non-zero value.
    self._cached_depth: int | None = None
    self._depth_probe_counter = 0
    # U2 rolling pop-rate state. A deque of ``time.monotonic()`` timestamps,
    # one per pop, evicted from the left on every pop to drop entries older
    # than ``_pop_rate_window_s``. Cheap: each pop is an append + amortized
    # popleft (older entries batch-evict only when the window advances). The
    # rate itself is only computed + emitted on the same sampling cadence as
    # the depth probe (``depth_sample_every``) — keeps the hot path O(1) and
    # avoids per-pop stat RPCs, mirroring the U4 perf discipline. ``deque``
    # is thread-safe for append/popleft at the CPython level (GIL-protected),
    # matching the existing single-thread-per-worker Scrapy engine model; the
    # scheduler drives pop serially per worker.
    # ``_pop_rate_window_s`` is set from the constructor kwarg (R14-C thread).
    self._pop_timestamps: deque[float] = deque()
    # U2 pop-rate sampling counter — independent of ``_depth_probe_counter``
    # (which resets on every real probe, so it can't be reused to gate the
    # rate emission). Counts pops since the last rate emission; emits once
    # per ``depth_sample_every`` pops, aligned to the same perf cadence as
    # the depth probe so both operability signals ride the same sampling.
    self._pop_rate_counter = 0
    # Initiative #3: restore in-process strategy state (e.g. Delay's held
    # heap) from a prior-shutdown snapshot. Best-effort — storage-incapable
    # backends and missing snapshots are silent no-ops; failures log + start
    # clean rather than crash startup.
    self._restore_snapshot()

  @cached_property
  def _serializer(self) -> JSONSerializer:
    """Lazy-initialized JSON serializer."""
    return JSONSerializer()

  def _request_to_dict(self, request: Request) -> dict[str, Any]:
    """Convert a Request to a dictionary.

    The body is base64-encoded (pure ASCII) so binary POST bodies round-trip
    losslessly through JSON + UTF-8. The previous UTF-8/latin-1 fallback
    corrupted non-ASCII bodies because Scrapy's request_from_dict re-encodes
    the string as UTF-8 — different bytes than the original latin-1 decode.

    Args:
        request: The Request to convert.

    Returns:
        Dictionary representation of the request.
    """
    body_value = None
    if request.body:
      body_value = base64.b64encode(request.body).decode("ascii")

    return {
      "url": request.url,
      "callback": request.callback.__name__ if request.callback else None,
      "errback": request.errback.__name__ if request.errback else None,
      "method": request.method,
      "headers": dict(request.headers.to_unicode_dict()),
      "body": body_value,
      "cookies": request.cookies,
      "meta": request.meta,
      "cb_kwargs": request.cb_kwargs,
      "encoding": request.encoding,
      "priority": request.priority,
      "dont_filter": request.dont_filter,
      "flags": request.flags,
    }

  def push(self, request: Request, priority: float = 0.0) -> None:
    """Push a request to the queue.

    .. breaking:: R14-F (retry + delay/source storm prevention)
        The ``delay`` and ``source`` keys are read from ``request.meta`` and
        then **popped** (consumed) before forwarding to the queue strategy.
        Pre-fix they were read but left in place, so when Scrapy's retry
        middleware re-queued the *same* request object (carrying the same
        meta), the original delay was re-applied — potentially forever
        (retry + delay storm), and the source tag was pinned to the retry
        (defeating round-robin fairness on the retry path).

        **Migration:** callers that push the same request object more than
        once AND want ``delay`` / ``source`` to apply on each push must
        re-set ``request.meta['delay']`` / ``request.meta['source']``
        between pushes. The common case (push once, retry middleware owns
        the re-push) is fixed for free by this consumption.

    Args:
        request: The Scrapy request to push.
        priority: Priority of the request (higher = more urgent).

    Raises:
        SerializationError: If the request cannot be serialized.
    """
    try:
      request_dict = self._request_to_dict(request)
      data = self._serializer.serialize(request_dict)
    except Exception as e:
      # R14-D: emit on_error so serialization failures surface as
      # ``errors/push`` instead of being dead observability (the hook
      # previously had zero call sites). Raised below — emit BEFORE the
      # raise so the counter is incremented even though we re-raise.
      self._monitor.on_error("push", e)
      msg = f"Failed to serialize request: {e}"
      raise SerializationError(
        msg,
        data=request,
        serializer="json",
      ) from e

    if len(data) > self.max_item_bytes:
      self._inc_stat("scheduler/queue/oversize_dropped")
      msg = (
        f"Serialized request ({len(data)} bytes) exceeds max_item_bytes "
        f"({self.max_item_bytes}). Rejecting push to avoid silent drop by "
        f"capped storage backends."
      )
      raise SerializationError(msg, data=request, serializer="json")

    # R14-F: read delay/source from meta, then POP them so a re-pushed
    # retry (Scrapy retry middleware re-queues the same request object with
    # the same meta) does NOT re-apply the original delay indefinitely
    # (retry + delay storm) and is not pinned to the original source tag
    # (which would defeat round-robin fairness on the retry path). Callers
    # that want delay/source on every push must re-set them between pushes
    # — see the breaking-change note in the docstring.
    delay = float(request.meta.pop("delay", 0.0) or 0.0)
    source = str(request.meta.pop("source", "default") or "default")
    self._strategy.push(
      self.queue_name, data, priority=priority, delay=delay, source=source
    )
    self._monitor.on_push(self.queue_name, priority)

  def pop(self, timeout: float = 0.0) -> Request | None:
    """Pop a request from the queue.

    Calls the backend's ``pop_with_ack`` and injects the returned ack token
    into ``request.meta["_backend_ack_token"]`` so the scheduler can ack the
    *specific* message that produced this request — correct under
    ``CONCURRENT_REQUESTS > 1``. For atomic-pop backends the token is
    ``None`` (harmless). The scheduler reads it on ``response_received`` /
    ``spider_error`` and forwards it to :meth:`BackendQueue.ack` /
    :meth:`BackendQueue.nack`.

    Args:
        timeout: Seconds to wait for an item (0 = non-blocking).

    Returns:
        The popped request, or None if the queue is empty.

    Raises:
        SerializationError: If the request cannot be deserialized.
    """
    data, ack_token = self._pop_with_ack(timeout)
    # Emit on every pop call — ``queue/pop_attempt_count`` (R14-D rename) is
    # the consumer-liveness signal (pop attempts per second), independent of
    # whether an item was returned. A worker popping an empty queue is itself
    # operability signal.
    self._monitor.on_pop(self.queue_name)
    # U2 operability: record this pop into the rolling window, then emit the
    # derived rate on the same sampling cadence as the depth probe below —
    # keeps the hot path O(1) amortized and avoids per-pop stat RPCs. A
    # monotonic clock is used so wall-clock skew can't corrupt the window.
    self._record_pop_timestamp()
    self._pop_rate_counter += 1
    if self._pop_rate_counter >= self.depth_sample_every:
      self._pop_rate_counter = 0
      try:
        self._emit_pop_rate()
      except Exception:  # noqa: BLE001
        logger.debug("monitor.on_pop_rate raised; ignored", exc_info=True)
    # Sample depth after each pop — this is the backpressure signal (architect's
    # #1 operability gap). Cheaper than a periodic timer and aligns the sample
    # with an event that already touched the backend. U4: routed through
    # ``_probe_depth`` so the real ``queue_len`` RPC only fires once per
    # ``depth_sample_every`` pops; cached value fills the gaps. Guarded so a
    # depth-sampling failure can never break a successful pop.
    try:
      self._monitor.on_queue_depth(self.queue_name, self._probe_depth())
    except Exception:  # noqa: BLE001
      logger.debug("monitor.on_queue_depth raised; ignored", exc_info=True)

    if data is None:
      return None

    try:
      request_dict = cast("dict[str, Any]", self._serializer.deserialize(data))
      self._decode_body(request_dict)
      # Coerce priority to int: newer Scrapy rejects float priority at Request
      # __init__ (``TypeError: Request priority not an integer: 0.0``). JSON
      # round-trip and some backends (ES stores priority as a float field) can
      # hand back 0.0 even when the original request.priority was int 0.
      if "priority" in request_dict and request_dict["priority"] is not None:
        request_dict["priority"] = int(request_dict["priority"])
      request = request_from_dict(request_dict, spider=self._spider)
    except Exception as e:
      # R14-D: emit on_error so deserialize failures surface as ``errors/pop``
      # instead of being dead observability. Emitted BEFORE the raise so the
      # counter is incremented even though we re-raise.
      self._monitor.on_error("pop", e)
      msg = f"Failed to deserialize request: {e}"
      raise SerializationError(
        msg,
        data=data,
        serializer="json",
      ) from e
    # Carry the backend ack token through the request so the scheduler can
    # correlate ack/nack back to the specific message that was popped. Only
    # inject when there's an actual token — atomic-pop backends return None
    # and we leave request.meta untouched (keeps the roundtrip byte-identical
    # for them; the scheduler reads .get() which returns None either way).
    if ack_token is not None:
      request.meta[BACKEND_ACK_TOKEN_META_KEY] = ack_token
    return request

  def _pop_with_ack(self, timeout: float) -> tuple[bytes | None, Any | None]:
    """Pop bytes + ack token, delegating to the strategy's ``pop_with_ack``.

    Each strategy owns whether it can thread a backend per-message ack token
    (#28). ``PassthroughQueueStrategy`` / ``PriorityQueueStrategy`` /
    ``WorkStealingQueueStrategy`` override ``pop_with_ack`` to call
    ``QueueBackend.pop_with_ack`` and carry the token (correct under
    ``CONCURRENT_REQUESTS > 1``). In-process strategies (round_robin /
    ring_buffer) and the holding strategies inherit the ABC default
    ``(pop(), None)`` -- correct, since they hold no broker message to ack.
    """
    return self._strategy.pop_with_ack(self.queue_name, timeout)

  @staticmethod
  def _decode_body(request_dict: dict[str, Any]) -> None:
    """Decode base64 body back to bytes in-place.

    Reverses ``_request_to_dict``'s base64 encoding so Scrapy's
    ``request_from_dict`` receives raw bytes.

    Legacy migration (D1): pre-base64 package versions wrote raw UTF-8/latin-1
    bodies to the queue. On rolling upgrade those items would hit
    ``b64decode(validate=True)`` and raise, causing the scheduler to silently
    drop them. To preserve those items, a body that fails base64 validation
    but is valid UTF-8 is migrated to its UTF-8 bytes with a one-time
    ``DeprecationWarning``. Structural corruption (neither valid base64 nor
    valid UTF-8) still raises ``SerializationError``.

    Args:
        request_dict: The deserialized request dict to mutate.
    """
    body = request_dict.get("body")
    if body is None:
      return
    try:
      request_dict["body"] = base64.b64decode(body, validate=True)
    except (binascii.Error, ValueError):
      # D1: attempt legacy migration — pre-base64 bodies were raw UTF-8.
      if isinstance(body, str):
        try:
          legacy_bytes = body.encode("utf-8")
        except UnicodeEncodeError:
          legacy_bytes = None
      else:
        legacy_bytes = None
      if legacy_bytes is not None:
        warnings.warn(
          "legacy non-base64 queue body; will be unsupported after the "
          "next major. Re-queue the request with a current package version "
          "to migrate it.",
          DeprecationWarning,
          stacklevel=2,
        )
        request_dict["body"] = legacy_bytes
        return
      msg = "Invalid base64 body in queued request: body is not valid base64"
      raise SerializationError(msg, data=body, serializer="json")

  def _probe_depth(self) -> int:
    """U4 — sample backend depth at most once per ``depth_sample_every`` calls.

    Cuts ~25% off pop-path RTT by skipping the ``queue_len`` RPC (e.g. ZCARD)
    on the gaps between samples; the cached non-zero depth fills them. Depth
    changes slowly relative to pop rate, so 1/100 sampling keeps variance ~1%.

    Emptiness-correctness invariant (MUST preserve): sampling only applies to
    the *non-zero* depth probe. When the cached value is ``0`` (or unknown),
    every call probes the backend for real so the drain is detected the moment
    it happens — Scrapy idle detection depends on depth reporting ``0`` the
    instant a queue empties. Concretely: the moment the real RPC returns ``0``
    it is cached, and the very next call re-probes (no stale masking) while
    subsequent in-window ``len()``/pop calls also re-probe until depth goes
    non-zero again. The perf win therefore rides the active-crawl steady state
    (non-zero depth, the common case); idle/empty queues pay the RPC each call
    — which is exactly when idle detection needs freshness most.

    Returns:
        The sampled queue depth (cached between probes only while non-zero).
    """
    # Spec rule of thumb: "sampling only applies to the non-zero depth probe".
    # While the cache holds 0 (or is uninitialized) we MUST probe every call —
    # that is what makes emptiness detection immediate. Only a non-zero cached
    # value is eligible for the windowed skip.
    cached = self._cached_depth
    window_open = cached is not None and cached != 0
    self._depth_probe_counter += 1
    must_probe = (
      not window_open
      or self._depth_probe_counter >= self.depth_sample_every
    )
    if not must_probe:
      # Cached non-zero depth still inside the window — return it as-is.
      return cached  # type: ignore[return-value]

    # Window elapsed (or empty/uninitialized) — hit the backend once, reset
    # the counter, cache result.
    self._depth_probe_counter = 0
    # Risk 1: let depth-query errors propagate. The pop-path monitor call to
    # ``_probe_depth`` (the ``on_queue_depth`` emit) is already BLE001-guarded
    # so a raising ``queue_len`` cannot crash the pop loop; and the scheduler's
    # ``has_pending_requests`` catches a raising ``__len__`` and returns True
    # (conservative — a depth-query error must NOT make the scheduler idle /
    # shut down prematurely). Swallowing here would break that conservative
    # contract. The backend-side contract is standardized separately (RocketMQ
    # ``queue_len`` returns 0 instead of raising) so real backends no longer
    # raise; these exceptions now surface only from direct mock injection.
    real_depth = self._strategy.queue_len(self.queue_name)
    self._cached_depth = real_depth
    return real_depth

  def _record_pop_timestamp(self) -> None:
    """U2 — append a monotonic timestamp for this pop to the rolling window.

    Evicts entries older than :attr:`_pop_rate_window_s` from the left so the
    deque holds only timestamps inside the trailing window. Older entries
    batch-evict only when the window has actually advanced (a tight inner
    loop in the same second hits zero poplefts), keeping the amortized cost
    O(1) per pop. Called on every pop; the rate is derived on the sampling
    cadence in :meth:`_emit_pop_rate`.
    """
    now = time.monotonic()
    cutoff = now - self._pop_rate_window_s
    ts = self._pop_timestamps
    ts.append(now)
    # Evict everything strictly older than the cutoff. ``while`` (not ``if``)
    # because the window can advance by more than one entry between pops when
    # the consumer pauses; popleft is O(1).
    while ts and ts[0] < cutoff:
      ts.popleft()

  def _emit_pop_rate(self) -> None:
    """U2 — compute + emit the rolling pop rate (pops/sec over the window).

    Rate = (timestamps in the trailing window) / window_s. On a fresh window
    (no timestamps yet — e.g. the very first pop, or the consumer stalled so
    long the deque emptied between samples) the rate is ``0.0`` so a stalled
    consumer surfaces as a clean falling-edge rather than a stale nonzero
    reading. The window length itself is the divisor: a half-aged window is
    not the denominator (the operator's contract is "rate over 60s", not
    "rate since the last pop").
    """
    count = len(self._pop_timestamps)
    rate = count / self._pop_rate_window_s if count else 0.0
    self._monitor.on_pop_rate(self._pop_rate_window_s, rate)

  def __len__(self) -> int:
    """Get the number of requests in the queue.

    U4: routed through ``_probe_depth`` so repeated ``len()`` probes amortize
    the backend RPC (shared counter with the pop-path depth emit). The depth
    is always fresh when empty — see ``_probe_depth``'s emptiness invariant.

    Returns:
        Number of requests.
    """
    return self._probe_depth()

  def clear(self) -> None:
    """Clear all requests from the queue."""
    self._strategy.clear(self.queue_name)

  def ack(self, *, token: Any | None = None) -> None:
    """Acknowledge the popped request identified by ``token``.

    Atomic backends (Redis, MongoDB, ElasticSearch, RocketMQ) implement
    this as a no-op. Message-queue backends (Kafka, RabbitMQ) commit the
    offset / ack the delivery so the message isn't re-delivered.

    When ``token`` is provided (read from
    ``request.meta["_backend_ack_token"]`` by the scheduler), the backend
    acks the *specific* message — correct under
    ``CONCURRENT_REQUESTS > 1``. When ``None``, the backend acks its
    last-popped message (legacy single-slot path).

    Args:
        token: Opaque ack token from ``BackendQueue.pop``'s meta injection,
            or ``None``.
    """
    backend = self.connection_manager.get_queue_backend()
    if token is not None:
      backend.ack(self.queue_name, token=token)
    else:
      backend.ack(self.queue_name)

  def nack(self, *, token: Any | None = None) -> None:
    """Negatively acknowledge the popped request identified by ``token``.

    Atomic backends: no-op. Message-queue backends: requeue the message
    so another consumer (or this one, later) can retry.

    Args:
        token: Opaque ack token from ``BackendQueue.pop``'s meta injection,
            or ``None``.
    """
    backend = self.connection_manager.get_queue_backend()
    if token is not None:
      backend.nack(self.queue_name, token=token)
    else:
      backend.nack(self.queue_name)

  def _inc_stat(self, stat_name: str) -> None:
    """Increment a Scrapy stat, tolerating missing spider/crawler/stats.

    Defensively chains ``self._spider.crawler.stats`` via ``getattr`` because
    the queue may be constructed without a spider (e.g. in tests) and legacy
    spiders may not expose ``crawler``. Silent skip when the chain is broken —
    the ``SerializationError`` already surfaced the condition; a missing
    counter is preferable to crashing the push path. Mirrors the pipeline's
    ``_inc_stat``.

    Args:
        stat_name: The Scrapy stats key to increment.
    """
    crawler = getattr(self._spider, "crawler", None) if self._spider else None
    stats = getattr(crawler, "stats", None) if crawler is not None else None
    if stats is not None:
      stats.inc_value(stat_name)

  @staticmethod
  def _resolve_monitor(spider: Spider | None) -> Monitor:
    """Default-on monitor resolution from a spider.

    When a spider is present and exposes ``crawler.stats``, wire a
    :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` so observability is
    on without an explicit ``monitor=`` kwarg. Otherwise (no spider, no
    crawler, or no stats) return a :class:`~scrapy_extension.monitor.NullMonitor`
    — the no-op default that never crashes a hook call.

    The ``getattr`` chain mirrors :meth:`_inc_stat`: the queue is often built
    without a spider (unit tests, ad-hoc use), and legacy spiders may not
    expose ``crawler``. Default-on where possible, safe everywhere else.

    Args:
        spider: Optional spider to resolve a stats collector from.

    Returns:
        A ``ScrapyStatsMonitor`` if ``spider.crawler.stats`` is reachable,
        else a ``NullMonitor``.
    """
    crawler = getattr(spider, "crawler", None) if spider is not None else None
    stats = getattr(crawler, "stats", None) if crawler is not None else None
    if stats is not None:
      return ScrapyStatsMonitor(stats)
    return NullMonitor()

  def close(self) -> None:
    """Close the queue, delegating to the queue strategy's lifecycle hook.

    Forwards to ``self._strategy.close()`` so strategies that hold in-process
    state (e.g. ``DelayQueueStrategy``'s held-item heap) can emit shutdown
    warnings / release resources. Then persists the strategy's snapshot
    (initiative #3) so in-process held state survives restart. The backend
    connection itself is owned by the ``ConnectionManager`` and closed
    separately by the scheduler.

    Safe to call when no strategy lifecycle work is needed — the default
    ``QueueStrategy.close()`` is a no-op and ``snapshot()`` returns ``None``.

    Order matters: :meth:`_persist_snapshot` runs BEFORE
    ``strategy.close()`` so the snapshot captures the held state before the
    strategy clears it (e.g. ``DelayQueueStrategy.close()`` clears its heap).
    """
    self._persist_snapshot()
    self._strategy.close()

  #: Storage-key prefix for strategy snapshots (initiative #3). Full key is
  #: ``<prefix><spider.name>:<queue_name>`` when a named spider is attached
  #: (initiative #16 — one snapshot per spider+queue, so two spiders sharing
  #: a storage backend with the same ``queue_name`` cannot overwrite each
  #: other's snapshot), or ``<prefix><queue_name>`` when no named spider is
  #: present (test stubs, no-spider construction — pre-#16 shape). No TTL:
  #: the snapshot is cheap to overwrite and represents last-shutdown state.
  _SNAPSHOT_KEY_PREFIX = "queue:snapshot:"

  def _snapshot_key(self) -> str:
    """Build the storage key for this queue's strategy snapshot.

    Includes the spider name when available so that two spiders sharing a
    storage backend with the same ``queue_name`` (multi-spider in one
    process, or multi-worker with shared Redis/Mongo/ES) cannot overwrite
    each other's strategy snapshot on close — and on restart cannot restore
    the wrong spider's Delay heap (initiative #16). When ``spider`` is
    ``None`` or exposes no ``name`` attribute, falls back to
    ``<prefix><queue_name>`` to preserve the pre-#16 key shape.
    """
    spider_name = getattr(self._spider, "name", None)
    if spider_name:
      return f"{self._SNAPSHOT_KEY_PREFIX}{spider_name}:{self.queue_name}"
    return f"{self._SNAPSHOT_KEY_PREFIX}{self.queue_name}"

  def _persist_snapshot(self) -> None:
    """Persist the strategy's in-process state on close (initiative #3).

    Calls ``strategy.snapshot()``; if it returns non-None bytes, stores them
    via the connection manager's storage backend. Storage-incapable backends
    (queue-only: Kafka/RabbitMQ/Pulsar/SQS/RocketMQ) raise
    ``NotImplementedError`` from ``get_storage_backend()`` — the snapshot is
    skipped (no KV store to persist to). Connection managers without a
    ``get_storage_backend`` attribute (e.g. test stubs) also skip. Best-effort:
    any failure is logged, never crashes :meth:`close`.
    """
    try:
      state = self._strategy.snapshot()
    except Exception:  # noqa: BLE001 — snapshot must not crash close
      logger.exception("strategy.snapshot() raised; skipping persist")
      return
    if state is None:
      return  # nothing to persist (passthrough / empty held heap)
    get_storage = getattr(self.connection_manager, "get_storage_backend", None)
    if get_storage is None:
      return  # connection manager exposes no storage interface
    try:
      storage = get_storage()
    except NotImplementedError:
      logger.info(
        "Queue backend is not storage-capable; cannot persist strategy "
        "snapshot for queue %r — in-process held state (e.g. delayed items) "
        "will not survive restart. Pair with a storage-capable backend "
        "(Redis/MongoDB/ElasticSearch) to enable snapshot/restore.",
        self.queue_name,
      )
      return
    except Exception:  # noqa: BLE001 — resolver must not crash close
      logger.exception(
        "Failed to resolve storage backend for queue %r; skipping snapshot persist.",
        self.queue_name,
      )
      return
    try:
      storage.store(self._snapshot_key(), state)
    except Exception:  # noqa: BLE001 — store must not crash close
      logger.exception(
        "Failed to persist strategy snapshot for queue %r; continuing.",
        self.queue_name,
      )

  def _restore_snapshot(self) -> None:
    """Restore the strategy's in-process state on startup (initiative #3).

    Loads the snapshot bytes from the storage backend (when storage-capable)
    and passes them to ``strategy.restore()``. Storage-incapable backends
    (queue-only) and connection managers without ``get_storage_backend`` skip
    silently. Only real ``bytes``/``bytearray`` are restored — a non-bytes
    retrieve result (e.g. a mock in tests) is skipped. Best-effort: any
    failure is logged, never crashes startup.
    """
    get_storage = getattr(self.connection_manager, "get_storage_backend", None)
    if get_storage is None:
      return  # connection manager exposes no storage interface
    try:
      storage = get_storage()
    except NotImplementedError:
      return  # storage-incapable backend — no prior snapshot to restore
    except Exception:  # noqa: BLE001 — resolver must not crash startup
      logger.exception(
        "Failed to resolve storage backend for queue %r; starting clean.",
        self.queue_name,
      )
      return
    try:
      state = storage.retrieve(self._snapshot_key())
    except Exception:  # noqa: BLE001 — retrieve must not crash startup
      logger.exception(
        "Failed to retrieve strategy snapshot for queue %r; starting clean.",
        self.queue_name,
      )
      return
    # Only restore real bytes — None (no prior snapshot) or any non-bytes
    # value (unexpected type / mock) is a no-op, never passed to restore().
    if not isinstance(state, (bytes, bytearray)):
      return
    try:
      self._strategy.restore(bytes(state))
    except Exception:  # noqa: BLE001 — restore must not crash startup (docstring)
      logger.exception(
        "strategy.restore() raised for queue %r; starting clean.", self.queue_name
      )
