"""Scheduler component for scrapy-extension.

This module provides a Scrapy scheduler component using backend queue
and duplicate filter interfaces.
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import AsyncIterable, Callable, Iterable, Mapping
from inspect import getattr_static, isawaitable
from typing import TYPE_CHECKING, Any

from scrapy import signals
from scrapy.http import Request
from scrapy.utils.misc import load_object
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure as TwistedFailure

from scrapy_extension.backends.base import BackendType, _validate_key_name
from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.backends.connectors import (
  _CONNECTION_MANAGER_SCOPE_KEY,
  _CONSUMER_SCOPED_BACKENDS,
  ConnectionManager,
  resolve_backend_config,
)
from scrapy_extension.exceptions import (
  BackendConnectionError,
  BackendError,
  ConfigurationError,
  QueueError,
  SerializationError,
)
from scrapy_extension.queue.queue import BACKEND_ACK_TOKEN_META_KEY, BackendQueue
from scrapy_extension.queue.strategies.base import _QueueAckToken
from scrapy_extension.utils._config import (
  get_bool_setting,
  parse_float_setting,
  parse_int_setting,
)

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.crawler import Crawler
  from scrapy.http import Response
  from scrapy.settings import Settings
  from scrapy.statscollectors import StatsCollector
  from twisted.internet.defer import Deferred
  from twisted.python.failure import Failure

  from scrapy_extension.queue.strategies.base import QueueStrategy

logger = logging.getLogger(__name__)

_LIFECYCLE_NEW = "new"
_LIFECYCLE_OPEN = "open"
_LIFECYCLE_CLOSED = "closed"
_MISSING_STATIC_ATTRIBUTE = object()


def _static_declaration_rank(component: object, name: str) -> int | None:
  """Return how close a class-declared capability is in the concrete MRO.

  Per-instance attributes (including autospec-created ``MagicMock`` methods)
  and dynamic ``__getattr__`` values are not stable protocol declarations.
  """
  if (
    getattr_static(component, name, _MISSING_STATIC_ATTRIBUTE)
    is _MISSING_STATIC_ATTRIBUTE
  ):
    return None
  for index, cls in enumerate(type(component).__mro__):
    if name in vars(cls):
      return index
  return None


def _atomic_dupefilter_methods(
  dupefilter: object,
) -> tuple[
  Callable[[Request, object], Any],
  Callable[[object], None],
  Callable[[object], None] | None,
  Callable[[object], None],
  Callable[[object], None],
] | None:
  """Resolve an explicitly compatible transactional dupefilter extension."""
  try:
    instance_attributes = object.__getattribute__(dupefilter, "__dict__")
  except (AttributeError, TypeError):
    instance_attributes = {}
  protocol_names = (
    "request_seen_with_reservation",
    "commit_reservation",
    "rollback_reservation",
    "rollback_reservation_intent",
  )
  if isinstance(instance_attributes, Mapping) and any(
    name in instance_attributes for name in protocol_names
  ):
    # A coherent extension is a class-level protocol. Per-instance shadows
    # (including autospec mocks) can expose an arbitrary partial combination.
    return None
  if isinstance(instance_attributes, Mapping) and "request_seen" in instance_attributes:
    # Scrapy's stable hook is intentionally monkeypatchable per instance. An
    # inherited extension must not bypass that closer policy override.
    return None
  atomic_rank = _static_declaration_rank(
    dupefilter,
    "request_seen_with_reservation",
  )
  commit_rank = _static_declaration_rank(dupefilter, "commit_reservation")
  rollback_rank = _static_declaration_rank(dupefilter, "rollback_reservation")
  intent_rank = _static_declaration_rank(
    dupefilter,
    "rollback_reservation_intent",
  )
  if (
    atomic_rank is None
    or commit_rank is None
    or rollback_rank is None
    or intent_rank is None
  ):
    return None

  # Existing BackendDupeFilter subclasses may override only Scrapy's stable
  # request_seen() hook. An inherited newer extension must not bypass that
  # custom policy unless the subclass also declares the atomic method at least
  # as close in the MRO.
  standard_rank = _static_declaration_rank(dupefilter, "request_seen")
  if standard_rank is not None and standard_rank < atomic_rank:
    return None
  canonical_rank = _static_declaration_rank(
    dupefilter,
    "_atomic_protocol_request_seen",
  )
  if canonical_rank is not None and canonical_rank == standard_rank:
    canonical_standard = getattr_static(
      dupefilter,
      "_atomic_protocol_request_seen",
    )
    current_standard = getattr_static(dupefilter, "request_seen")
    if current_standard is not canonical_standard:
      return None

  atomic = getattr(dupefilter, "request_seen_with_reservation")
  commit = getattr(dupefilter, "commit_reservation")
  volatile_commit: Callable[[object], None] | None = None
  if _static_declaration_rank(dupefilter, "commit_volatile_reservation") is not None:
    candidate = getattr(dupefilter, "commit_volatile_reservation")
    if callable(candidate):
      volatile_commit = candidate
  rollback = getattr(dupefilter, "rollback_reservation")
  rollback_intent = getattr(dupefilter, "rollback_reservation_intent")
  if (
    not callable(atomic)
    or not callable(commit)
    or not callable(rollback)
    or not callable(rollback_intent)
  ):
    return None
  return atomic, commit, volatile_commit, rollback, rollback_intent


def _push_queue_with_durability(
  queue: object,
  request: Request,
  *,
  priority: float,
) -> bool | None:
  """Push while preserving the stable public queue return contract.

  The bundled queue exposes a package-private durability result. A custom
  queue, subclass override, or instance monkeypatch continues through its
  public ``push`` method and is treated as having unknown durability for
  backward compatibility.
  """
  declared_push = getattr_static(queue, "push", _MISSING_STATIC_ATTRIBUTE)
  canonical_push = getattr_static(
    queue,
    "_scheduler_protocol_push",
    _MISSING_STATIC_ATTRIBUTE,
  )
  if isinstance(queue, BackendQueue) and declared_push is canonical_push:
    return queue._push_with_durability(request, priority=priority)
  push = getattr(queue, "push")
  push(request, priority=priority)
  return None


class _DeferredReplacementAckGroup:
  """Settle one source only after an errback output stream is committed.

  Each replacement request gets a distinct child token. A child becomes
  complete only when ``BackendQueue.push`` reaches its commit boundary (or the
  scheduler terminally rejects an invalid replacement). The source is
  acknowledged after the output iterable is exhausted *and* every registered
  child is complete. An output-iteration failure aborts the group with a nack.
  """

  def __init__(
    self,
    scheduler: BackendScheduler,
    source_token: Any,
  ) -> None:
    self._scheduler = scheduler
    self._source_token = source_token
    self._lock = threading.Lock()
    self._pending: set[int] = set()
    self._next_child = 0
    self._sealed = False
    self._terminal = False

  def new_child(self) -> _DeferredReplacementAckToken | None:
    """Register one replacement unless this group already aborted."""
    with self._lock:
      if self._terminal:
        return None
      child_id = self._next_child
      self._next_child += 1
      self._pending.add(child_id)
    return _DeferredReplacementAckToken(self, child_id)

  def seal(self) -> None:
    """Declare output enumeration complete and ack if no child remains."""
    with self._lock:
      if self._terminal:
        return
      self._sealed = True
      if self._pending:
        return
      if self._scheduler._ack_token(
        self._source_token,
        log_message=(
          "Failed to ack source message after errback replacements committed"
        ),
      ):
        self._terminal = True

  def accept(self, child_id: int) -> None:
    """Mark one replacement committed and settle a sealed final child."""
    with self._lock:
      if self._terminal or child_id not in self._pending:
        return
      if not self._sealed or len(self._pending) > 1:
        self._pending.remove(child_id)
        return
      if self._scheduler._ack_token(
        self._source_token,
        log_message=(
          "Failed to ack source message after errback replacements committed"
        ),
      ):
        self._pending.remove(child_id)
        self._terminal = True

  def abort(self) -> None:
    """Nack a source whose replacement output failed during enumeration."""
    with self._lock:
      if self._terminal:
        return
      self._scheduler._nack_token(
        self._source_token,
        log_message="Failed to nack source after errback output failure",
      )
      # Even if the broker call failed, visibility timeout/redelivery is the
      # only safe recovery. Never let late child commits turn this into an ack.
      self._pending.clear()
      self._terminal = True


class _DeferredReplacementAckToken(_QueueAckToken):
  """One idempotent child completion in a deferred source-ack group."""

  __slots__ = ("_child_id", "_group")

  def __init__(self, group: _DeferredReplacementAckGroup, child_id: int) -> None:
    self._group = group
    self._child_id = child_id

  def ack(self) -> None:
    """Record that this replacement reached its queue commit boundary."""
    self._group.accept(self._child_id)

  def nack(self) -> None:
    """Abort the source when a child is negatively acknowledged locally."""
    self._group.abort()


class _BackendDownloadFailureErrback:
  """Settle a downloader failure around the user errback's final output.

  Downloader middleware handles retry/redirect before Scrapy invokes a
  request's spider errback. Installing this wrapper only on popped deliveries
  leaves middleware replacements on the existing enqueue-then-ack path. A user
  errback's replacement requests receive child tokens so the source is acked
  only after the complete output stream commits; an unhandled/failed stream is
  nacked. The wrapper is removed before a request is serialized back into the
  backend queue.
  """

  def __init__(self, scheduler: BackendScheduler, original: Any | None) -> None:
    self.scheduler = scheduler
    self.original = original

  def __call__(self, failure: Any) -> Any:
    request = getattr(failure, "request", None)
    if self.original is None:
      return self._finish_failure(request, failure)
    try:
      result = self.original(failure)
    except BaseException:
      self._finish_failure(request, failure)
      raise
    if isinstance(result, Deferred):
      result.addCallbacks(
        lambda value: self._finish_success(request, value),
        lambda error: self._finish_failure(request, error),
      )
      return result
    if isawaitable(result):
      return self._finish_awaitable(request, result)
    return self._finish_success(request, result)

  def _finish_success(self, request: Any, result: Any) -> Any:
    """Settle handled failures after any replacement output is committed."""
    if isinstance(result, TwistedFailure):
      return self._finish_failure(request, result)
    if request is None:
      return result
    if isinstance(result, Request):
      return self._transfer_request(request, result)
    if isinstance(result, AsyncIterable):
      return self._transfer_async_iterable(request, result)
    if isinstance(result, Iterable) and not isinstance(
      result,
      (str, bytes, bytearray, Mapping),
    ):
      return self._transfer_iterable(request, result)
    self.scheduler._ack_request_token(
      request,
      log_message="Failed to ack message after handled download failure",
    )
    return result

  def _new_group(
    self,
    request: Request,
  ) -> tuple[_DeferredReplacementAckGroup, Any] | None:
    """Move the source token out of its request and into a deferred group."""
    token = request.meta.get(BACKEND_ACK_TOKEN_META_KEY)
    if token is None:
      return None
    group = _DeferredReplacementAckGroup(self.scheduler, token)
    request.meta.pop(BACKEND_ACK_TOKEN_META_KEY, None)
    return group, token

  def _attach_replacement(
    self,
    group: _DeferredReplacementAckGroup,
    source_token: Any,
    replacement: Request,
  ) -> None:
    """Attach one commit-tracking child without overwriting another delivery."""
    existing = replacement.meta.get(BACKEND_ACK_TOKEN_META_KEY)
    if existing is not None and existing is not source_token:
      logger.error(
        "Errback replacement already carries a different backend ack token; "
        "nacking the source instead of overwriting either delivery"
      )
      if self.scheduler.stats:
        self.scheduler.stats.inc_value("scheduler/ack_transfer_conflict")
      group.abort()
      return
    child = group.new_child()
    if child is not None:
      replacement.meta[BACKEND_ACK_TOKEN_META_KEY] = child

  def _transfer_request(self, request: Request, replacement: Request) -> Request:
    """Transfer one source delivery to a replacement request commit token."""
    group_and_token = self._new_group(request)
    if group_and_token is None:
      return replacement
    group, source_token = group_and_token
    self._attach_replacement(group, source_token, replacement)
    group.seal()
    return replacement

  def _transfer_iterable(
    self,
    request: Request,
    result: Iterable[Any],
  ) -> Iterable[Any]:
    """Stream synchronous errback output while tracking replacement commits."""
    group: _DeferredReplacementAckGroup | None = None
    source_token: Any = None
    try:
      for value in result:
        if isinstance(value, Request):
          if group is None:
            group_and_token = self._new_group(request)
            if group_and_token is not None:
              group, source_token = group_and_token
          if group is not None:
            self._attach_replacement(group, source_token, value)
        yield value
    except BaseException:
      if group is not None:
        group.abort()
      else:
        self._finish_failure(request, None)
      raise
    else:
      if group is not None:
        group.seal()
      else:
        self.scheduler._ack_request_token(
          request,
          log_message="Failed to ack message after handled download failure",
        )

  async def _transfer_async_iterable(
    self,
    request: Request,
    result: AsyncIterable[Any],
  ) -> Any:
    """Stream asynchronous errback output while tracking replacement commits."""
    group: _DeferredReplacementAckGroup | None = None
    source_token: Any = None
    try:
      async for value in result:
        if isinstance(value, Request):
          if group is None:
            group_and_token = self._new_group(request)
            if group_and_token is not None:
              group, source_token = group_and_token
          if group is not None:
            self._attach_replacement(group, source_token, value)
        yield value
    except BaseException:
      if group is not None:
        group.abort()
      else:
        self._finish_failure(request, None)
      raise
    else:
      if group is not None:
        group.seal()
      else:
        self.scheduler._ack_request_token(
          request,
          log_message="Failed to ack message after handled download failure",
        )

  def _finish_failure(self, request: Any, failure: Any) -> Any:
    """Nack an unhandled or failed errback while preserving its Failure."""
    if request is not None:
      self.scheduler._nack_request_token(
        request,
        log_message="Failed to nack message after download failure",
      )
    return failure

  async def _finish_awaitable(self, request: Any, awaitable: Any) -> Any:
    """Finalize an async errback after its awaited outcome is known."""
    try:
      result = await awaitable
    except BaseException:
      self._finish_failure(request, None)
      raise
    return self._finish_success(request, result)


class BackendScheduler:
  """Scrapy scheduler implementation using backend interfaces.

  Uses QueueBackend for request queueing and applies duplicate filtering
  through the configured ``DUPEFILTER_CLASS`` when present.

  Ack/nack semantics (important — read before tuning concurrency):

  1. **Ack fires on ``response_received``, NOT on callback/pipeline
     completion.** For message-queue backends (Kafka, RabbitMQ, SQS,
     Pulsar), a message is acked as soon as Scrapy's downloader delivers
     the response (``signals.response_received``) and nacked on
     ``signals.spider_error``. The ack is *download-level*: it does **not**
     wait for the spider callback, the item pipeline, or any post-download
     processing. A crash between ack and pipeline completion drops the
     item (at-most-once for the pipeline side); a crash before ack
     re-delivers the message (at-least-once for the download side).

  2. **Concurrent-ack correctness is per-backend, gated at from_settings.**
     Backends declare ``QueueBackend.requires_ack`` /
     ``supports_concurrent_ack``:

     - **Atomic-pop backends** (Redis, MongoDB, ElasticSearch):
       ``requires_ack=False``. pop removes the item in one step; ack/nack
       are no-ops. ``CONCURRENT_REQUESTS`` is unrestricted.
     - **Per-message-ack (MQ) backends** (Kafka, RabbitMQ, RocketMQ, SQS,
       Pulsar): ``requires_ack=True``, ``supports_concurrent_ack=True``.
       ``pop_with_ack`` returns a per-message token (in-flight set /
       ReceiptHandle / MessageId); ``ack(token=…)`` commits that specific
       message. Correct under ``CONCURRENT_REQUESTS > 1`` — unrestricted.
       (2026-07-10: every bundled backend is in one of these two buckets;
       the historical third "single-slot ack" bucket is empty. A 3rd-party
       backend that can hold only one ack slot may still set
       ``supports_concurrent_ack=False`` — the ``from_settings`` gate then
       raises ``ConfigurationError`` under ``CONCURRENT_REQUESTS > 1``
       unless ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`` is set.)

  3. **At-least-once on crash is inherent.** A worker crash before ack
     fires leaves the message unacked (Kafka: offset uncommitted; RabbitMQ:
     delivery unacked; SQS: visibility timeout expires; Pulsar: retry
     policy redelivers) → it is re-delivered on reconnect/restart. This is
     the intended at-least-once guarantee, not a defect.

  4. **Dedup outage does not crash the spider.** ``enqueue_request`` runs
     ``dupefilter.request_seen`` INSIDE its try-block; a ``QueueError`` /
     ``BackendError`` from the dedup backend degrades to default-enqueue
     (the URL is not lost) + a ``scheduler/dupefilter_error`` stat bump.

  Backpressure depth gate (round-4, BP-2):

  When ``backpressure_pause_at`` is set (not None), ``next_request`` slows
  consumption once the queue depth reaches ``pause_at`` (depth source:
  ``len(self._queue)``, fresh — same source ``has_pending_requests`` trusts).
  The first paused poll returns ``None``; while depth remains above
  ``resume_at``, subsequent polls alternate between returning ``None`` and one
  non-blocking progress pop. This bounded probe cadence lets a sole consumer
  reduce the same depth that controls its gate instead of self-locking.
  Full-speed popping resumes after depth drains to ``resume_at`` (hysteresis,
  prevents flapping). ``resume_at`` defaults to ``pause_at`` when unset (no
  hysteresis — single threshold). The gate bumps two additive stats:
  ``scheduler/backpressure_pause`` and ``scheduler/backpressure_resume``.
  Default-off (``pause_at is None``) → byte-identical behavior to the pre-fix
  pop path. A ``QueueError`` / ``NotImplementedError`` from ``len(self._queue)``
  disables the gate for that poll and falls through to ``pop`` (degraded
  safely; an unavailable depth signal cannot stall consumption).

  Attributes:
      connection_manager: The connection manager for backend access.
      queue_key: The key for the request queue.
      stats: Optional stats collector for metrics.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    queue_key: str = "scheduler-queue",
    stats: StatsCollector | None = None,
    dupefilter: Any | None = None,
    queue_strategy: QueueStrategy | None = None,
    *,
    backpressure_pause_at: int | None = None,
    backpressure_resume_at: int | None = None,
    queue_depth_sample_every: int = 100,
    queue_max_item_bytes: int = 1_048_576,
    monitor_backpressure_threshold: int = 1_000,
    monitor_pop_rate_window_s: float = 60.0,
    queue_snapshot_owner: str | None = None,
    owns_connection_manager: bool = True,
  ) -> None:
    """Initialize the scheduler.

    Args:
        connection_manager: Connection manager for backend access.
        queue_key: Key for the request queue.
        stats: Optional stats collector for metrics.
        dupefilter: Optional dupefilter implementing Scrapy's request_seen/log API.
        queue_strategy: Optional queue-semantics strategy threaded into the
            BackendQueue. When ``None`` (default), BackendQueue uses
            PassthroughQueueStrategy (current behavior).
        backpressure_pause_at: Optional depth threshold — at and above this
            depth, ``next_request`` begins alternating paused returns with
            bounded progress pops (depth read fresh from ``len(self._queue)``).
            ``None`` (default) disables the gate (byte-identical to prior
            behavior).
        backpressure_resume_at: Optional resume threshold — depth must drain
            to this value before popping resumes (hysteresis). When ``None``
            and ``backpressure_pause_at`` is set, defaults to ``pause_at``
            (single-threshold, no hysteresis).
        queue_depth_sample_every: Round-14 R14-C — U4 depth-probe sampling
            window forwarded to ``BackendQueue(depth_sample_every=…)`` in
            ``open()``. Default ``100`` (U4 default).
        queue_max_item_bytes: Round-14 R14-C — D2 per-item serialized-byte cap
            forwarded to ``BackendQueue(max_item_bytes=…)`` in ``open()``.
            Default 1 MiB (matches Memcached ceiling).
        monitor_backpressure_threshold: Round-14 R14-C — U2 depth above which
            ``queue/backpressure`` flips on. Forwarded to the resolved
            ``ScrapyStatsMonitor`` in ``open()``. Default ``1_000`` (U2).
        monitor_pop_rate_window_s: Round-14 R14-C — U2 trailing window
            (seconds) for the ``queue/pop_rate`` gauge. Forwarded to both
            ``BackendQueue(pop_rate_window_s=…)`` and the resolved monitor
            in ``open()``. Default ``60.0`` (U2).
        queue_snapshot_owner: Stable per-worker identity for isolating local
            strategy snapshots. ``None`` preserves the legacy single-worker
            key shape.
        owns_connection_manager: Whether :meth:`close` releases the supplied
            manager. Defaults to True for standalone schedulers; composite
            owners can pass False and release their shared acquire after the
            scheduler has quiesced its queue and signals.
    """
    self.connection_manager = connection_manager
    self._queue_key_template = queue_key
    self.queue_key = queue_key
    self.stats = stats
    self.dupefilter = dupefilter
    self._owns_dupefilter: bool = False
    self._dupefilter_open: bool = False
    self._dupefilter_released: bool = False
    self._queue_strategy = queue_strategy
    self._queue: BackendQueue | None = None
    self._spider: Spider | None = None
    self._signals_connected: bool = False
    self._connected_signals = None
    self._manager_released: bool = False
    self._owns_connection_manager = owns_connection_manager
    # Backpressure gate config (round-4 BP-2). resume_at defaults to pause_at
    # (single-threshold) when unset — computed once here, not per-call.
    self._pause_at = backpressure_pause_at
    self._resume_at = (
      backpressure_resume_at
      if backpressure_resume_at is not None
      else backpressure_pause_at
    )
    # Per-spider paused state; reset on open(spider).
    self._backpressure_paused: bool = False
    # A paused sole consumer must still be able to lower its own queue depth.
    # ``True`` permits the next paused poll to make one progress pop; ``False``
    # returns None and arms the following poll. This deterministic 50% cadence
    # preserves the slowdown signal without allowing a consumer-side deadlock.
    self._backpressure_probe_due: bool = False
    # R14-C operability knobs — carried from from_settings → open() so the
    # BackendQueue / strategy / monitor constructors receive them. Pre-R14-C
    # these were stuck at constructor defaults (the settings existed only in
    # the runbook's "tune via settings" hand-wave). See ``open()`` for the
    # threading site.
    self._queue_depth_sample_every = queue_depth_sample_every
    self._queue_max_item_bytes = queue_max_item_bytes
    self._monitor_backpressure_threshold = monitor_backpressure_threshold
    self._monitor_pop_rate_window_s = monitor_pop_rate_window_s
    self._queue_snapshot_owner = queue_snapshot_owner
    # A scheduler owns one ConnectionManager acquire and is therefore a
    # single-lifecycle object. Serializing open/close prevents concurrent
    # callers from replacing a live queue or releasing its manager midway
    # through construction.
    self._lifecycle_lock = threading.RLock()
    self._lifecycle_state = _LIFECYCLE_NEW

  @classmethod
  def from_settings(
    cls,
    settings: Settings,
    *,
    spider_name: str | None = None,
  ) -> BackendScheduler:
    """Create scheduler from Scrapy settings.

    Selects the queue strategy from ``SCRAPY_QUEUE_STRATEGY`` (default
    ``passthrough``). The delay strategy reads ``SCRAPY_QUEUE_DELAY_DEFAULT``.

    Backend selection: ``SCRAPY_QUEUE_BACKEND_TYPE`` /
    ``SCRAPY_QUEUE_BACKEND_SETTINGS`` override the global
    ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so the queue can
    bind to a different backend than the dedup filter or storage pipeline
    (multi-backend coexistence). Unset → falls back to the global keys.

    **Ack-concurrency gate (round-2 C1 fix; 2026-07-10 correction).** After the queue backend is
    resolved, the backend's ``QueueBackend.requires_ack`` /
    ``supports_concurrent_ack`` class attributes are inspected. If the
    backend requires ack but does NOT support concurrent ack AND
    ``CONCURRENT_REQUESTS > 1`` AND the explicit
    ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`` opt-out is NOT set, this
    raises :class:`ConfigurationError`. **Note (2026-07-10):** every bundled
    backend — atomic (Redis/Mongo/ES) and all five MQ backends (Kafka/RabbitMQ/
    RocketMQ/SQS/Pulsar) — sets ``supports_concurrent_ack=True``, so this gate
    is unreachable for bundled backends; it remains a defensive backstop for a
    hypothetical 3rd-party single-slot backend. Read the opt-out via ``settings.get(..., False)`` — it is
    NOT a pydantic field.

    **Strategy+MQ ack-bypass warning (2026-07-10, §B).** After the queue
    strategy is resolved, if it is non-passthrough AND the backend
    ``requires_ack=True``, a WARNING is logged: ``BackendQueue._pop_with_ack``
    returns ``token=None`` for non-passthrough strategies, silently disabling
    MQ per-message ack (35 misconfig combinations). See
    ``_warn_strategy_mq_ack_bypass``.
    """
    from scrapy_extension.queue.strategies.factory import (
      QueueStrategyType,
      build_queue_strategy,
    )
    from scrapy_extension.queue.strategies.priority import MAX_PRIORITY_LEVELS
    from scrapy_extension.queue.strategies.throttle import (
      THROTTLE_MAX_MIN_INTERVAL_S,
    )
    from scrapy_extension.queue.strategies.time_wheel import MAX_WHEEL_SIZE

    strategy_value = settings.get(
      "SCRAPY_QUEUE_STRATEGY",
      QueueStrategyType.PASSTHROUGH.value,
    )
    ring_buffer_full_policy = settings.get(
      "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY",
      "reject",
    )
    if ring_buffer_full_policy not in ("reject", "drop_oldest", "block"):
      raise ConfigurationError(
        "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY must be one of "
        "'reject', 'drop_oldest', or 'block'.",
        setting_name="SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY",
        setting_value=ring_buffer_full_policy,
      )
    if (
      strategy_value == QueueStrategyType.RING_BUFFER.value
      and ring_buffer_full_policy == "block"
    ):
      raise ConfigurationError(
        "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY='block' is unsafe with "
        "BackendScheduler: enqueue_request runs on Scrapy's reactor thread, "
        "so a full ring buffer would block the same thread that must drain it. "
        "Use 'reject' or 'drop_oldest'.",
        setting_name="SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY",
        setting_value=ring_buffer_full_policy,
      )

    backend_type, backend_settings = resolve_backend_config(
      settings,
      type_key="SCRAPY_QUEUE_BACKEND_TYPE",
      settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
      required_capabilities={"queue"},
      component_name="queue",
    )
    queue_key = settings.get("SCRAPY_QUEUE_KEY", "scheduler-queue")
    if not isinstance(queue_key, str):
      raise ConfigurationError(
        f"SCRAPY_QUEUE_KEY must be a string, got {queue_key!r}.",
        setting_name="SCRAPY_QUEUE_KEY",
        setting_value=queue_key,
      )
    if spider_name is not None:
      try:
        _validate_key_name(spider_name, "spider.name")
      except ValueError as exc:
        raise ConfigurationError(
          str(exc),
          setting_name="spider.name",
          setting_value=spider_name,
        ) from exc
    resolved_queue_key = (
      queue_key.replace("{spider}", spider_name)
      if spider_name is not None
      else queue_key
    )
    try:
      _validate_key_name(
        resolved_queue_key.replace("{spider}", "spider"),
        "SCRAPY_QUEUE_KEY",
      )
    except ValueError as exc:
      raise ConfigurationError(
        str(exc),
        setting_name="SCRAPY_QUEUE_KEY",
        setting_value=queue_key,
      ) from exc
    if backend_type in _CONSUMER_SCOPED_BACKENDS:
      # Kafka and RocketMQ each keep one mutable consumer on the backend
      # instance. Sharing that instance across logical queues makes Kafka
      # replace its subscription on every alternating pop and makes RocketMQ
      # accumulate both subscriptions on one receive loop. Add a registry-only
      # discriminator so schedulers for different queues get independent
      # consumers; ConnectionManager strips it before Pydantic validation.
      scope = resolved_queue_key
      if spider_name is None and "{spider}" in queue_key:
        # Direct ``from_settings`` has no crawler/spider identity yet. Sharing
        # the literal template would join unrelated future queues to one
        # mutable consumer, so this scheduler gets a registry-only opaque scope.
        scope = f"unresolved-{uuid.uuid4().hex}"
      backend_settings = {
        **backend_settings,
        _CONNECTION_MANAGER_SCOPE_KEY: scope,
      }
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=backend_settings,
    )
    try:
      # Ack-concurrency gate (round-2 C1 fix). Inspect the backend CLASS —
      # no instantiation/connection needed. NOTE (2026-07-10): every bundled
      # backend sets supports_concurrent_ack=True, so this gate is unreachable
      # for bundled backends — it remains a defensive backstop for a
      # hypothetical 3rd-party single-slot backend.
      BackendScheduler._enforce_ack_concurrency_gate(settings, backend_type)

      try:
        strategy_type = QueueStrategyType(strategy_value)
      except ValueError as e:
        valid = ", ".join(repr(member.value) for member in QueueStrategyType)
        raise ConfigurationError(
          f"Invalid SCRAPY_QUEUE_STRATEGY {strategy_value!r}. Valid: {valid}.",
          setting_name="SCRAPY_QUEUE_STRATEGY",
          setting_value=str(strategy_value),
        ) from e
      default_delay = parse_float_setting(
        settings.get("SCRAPY_QUEUE_DELAY_DEFAULT", 0.0),
        "SCRAPY_QUEUE_DELAY_DEFAULT",
        minimum=0.0,
      )
      min_interval = parse_float_setting(
        settings.get("SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL", 0.0),
        "SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL",
        minimum=0.0,
        maximum=THROTTLE_MAX_MIN_INTERVAL_S,
      )
      # A non-positive max_held intentionally disables the soft warning; retain
      # that documented opt-out while still rejecting non-integer inputs.
      delay_max_held_raw = settings.get("SCRAPY_QUEUE_DELAY_MAX_HELD")
      delay_max_held = (
        parse_int_setting(delay_max_held_raw, "SCRAPY_QUEUE_DELAY_MAX_HELD")
        if delay_max_held_raw is not None
        else None
      )
      priority_levels = parse_int_setting(
        settings.get("SCRAPY_QUEUE_PRIORITY_LEVELS", 3),
        "SCRAPY_QUEUE_PRIORITY_LEVELS",
        minimum=1,
        maximum=MAX_PRIORITY_LEVELS,
      )
      wheel_size = parse_int_setting(
        settings.get("SCRAPY_QUEUE_TIME_WHEEL_SIZE", 60),
        "SCRAPY_QUEUE_TIME_WHEEL_SIZE",
        minimum=1,
        maximum=MAX_WHEEL_SIZE,
      )
      ticks_per_second = parse_float_setting(
        settings.get("SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND", 1.0),
        "SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND",
        minimum=0.0,
        minimum_exclusive=True,
      )
      steal_timeout = parse_float_setting(
        settings.get("SCRAPY_QUEUE_STEAL_TIMEOUT", 0.05),
        "SCRAPY_QUEUE_STEAL_TIMEOUT",
        minimum=0.0,
      )
      capacity = parse_int_setting(
        settings.get("SCRAPY_QUEUE_RING_BUFFER_CAPACITY", 1024),
        "SCRAPY_QUEUE_RING_BUFFER_CAPACITY",
        minimum=1,
      )
      worker_id_raw = settings.get("SCRAPY_QUEUE_WORKER_ID")
      if worker_id_raw is None:
        worker_id = None
      elif not isinstance(worker_id_raw, str) or not worker_id_raw.strip():
        raise ConfigurationError(
          "SCRAPY_QUEUE_WORKER_ID must be a non-empty string or unset.",
          setting_name="SCRAPY_QUEUE_WORKER_ID",
          setting_value=worker_id_raw,
        )
      else:
        worker_id = worker_id_raw.strip()
      # Accept Scrapy's comma-separated form as well as native list/tuple values
      # commonly used in settings.py. Avoid .getlist because unconfigured test
      # doubles expose it as a non-iterable Mock.
      peer_ids_raw = settings.get("SCRAPY_QUEUE_PEER_IDS")
      peer_id_values: list[Any] | tuple[Any, ...]
      if peer_ids_raw is None:
        peer_id_values = ()
      elif isinstance(peer_ids_raw, str):
        peer_id_values = peer_ids_raw.split(",")
      elif isinstance(peer_ids_raw, (list, tuple)):
        peer_id_values = peer_ids_raw
      else:
        raise ConfigurationError(
          "SCRAPY_QUEUE_PEER_IDS must be a comma-separated string, list, or tuple.",
          setting_name="SCRAPY_QUEUE_PEER_IDS",
          setting_value=peer_ids_raw,
        )
      if any(not isinstance(peer_id, str) for peer_id in peer_id_values):
        raise ConfigurationError(
          "SCRAPY_QUEUE_PEER_IDS entries must all be strings.",
          setting_name="SCRAPY_QUEUE_PEER_IDS",
          setting_value=peer_ids_raw,
        )
      peer_ids = tuple(
        peer_id.strip()
        for peer_id in peer_id_values
        if peer_id.strip()
      )
      try:
        queue_strategy = build_queue_strategy(
          strategy_type,
          manager,
          default_delay=default_delay,
          min_interval=min_interval,
          max_held=delay_max_held,
          priority_levels=priority_levels,
          wheel_size=wheel_size,
          ticks_per_second=ticks_per_second,
          worker_id=worker_id,
          peer_ids=peer_ids,
          steal_timeout=steal_timeout,
          capacity=capacity,
          full_policy=ring_buffer_full_policy,
        )
      except (TypeError, ValueError, OverflowError) as exc:
        constructor_setting = {
          QueueStrategyType.DELAY: "SCRAPY_QUEUE_DELAY_DEFAULT",
          QueueStrategyType.THROTTLE: "SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL",
          QueueStrategyType.PRIORITY: "SCRAPY_QUEUE_PRIORITY_LEVELS",
          QueueStrategyType.TIME_WHEEL: (
            "SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND"
          ),
          QueueStrategyType.WORK_STEALING: "SCRAPY_QUEUE_PEER_IDS",
          QueueStrategyType.RING_BUFFER: "SCRAPY_QUEUE_RING_BUFFER_CAPACITY",
        }.get(strategy_type, "SCRAPY_QUEUE_STRATEGY")
        raise ConfigurationError(
          f"Invalid {constructor_setting}: {exc}",
          setting_name=constructor_setting,
          setting_value=settings.get(constructor_setting),
        ) from exc
      # Strategy+MQ ack-bypass warning (2026-07-10 §B, refined 2026-07-11 #28):
      # fires only for strategies that do NOT override pop_with_ack (so they
      # lose the MQ per-message token) paired with a requires_ack backend.
      BackendScheduler._warn_strategy_mq_ack_bypass(queue_strategy, backend_type)
      pause_raw = settings.get("SCRAPY_BACKPRESSURE_PAUSE_AT")
      resume_raw = settings.get("SCRAPY_BACKPRESSURE_RESUME_AT")
      pause_at = (
        parse_int_setting(
          pause_raw,
          "SCRAPY_BACKPRESSURE_PAUSE_AT",
          minimum=0,
        )
        if pause_raw is not None
        else None
      )
      resume_at = (
        parse_int_setting(
          resume_raw,
          "SCRAPY_BACKPRESSURE_RESUME_AT",
          minimum=0,
        )
        if resume_raw is not None
        else None
      )
      if pause_at is not None and resume_at is not None and resume_at > pause_at:
        raise ConfigurationError(
          "SCRAPY_BACKPRESSURE_RESUME_AT must be <= "
          "SCRAPY_BACKPRESSURE_PAUSE_AT.",
          setting_name="SCRAPY_BACKPRESSURE_RESUME_AT",
          setting_value=resume_raw,
        )
      depth_sample_every = parse_int_setting(
        settings.get("SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY", 100),
        "SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY",
        minimum=1,
      )
      queue_max_item_bytes = parse_int_setting(
        settings.get("SCRAPY_QUEUE_MAX_ITEM_BYTES", 1_048_576),
        "SCRAPY_QUEUE_MAX_ITEM_BYTES",
        minimum=1,
      )
      monitor_backpressure_threshold = parse_int_setting(
        settings.get("SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD", 1_000),
        "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD",
        minimum=0,
      )
      monitor_pop_rate_window_s = parse_float_setting(
        settings.get("SCRAPY_MONITOR_POP_RATE_WINDOW_S", 60.0),
        "SCRAPY_MONITOR_POP_RATE_WINDOW_S",
        minimum=0.0,
        minimum_exclusive=True,
      )
      snapshot_owner_raw = settings.get("SCRAPY_QUEUE_SNAPSHOT_OWNER")
      queue_snapshot_owner = (
        snapshot_owner_raw if snapshot_owner_raw is not None else worker_id
      )
      if queue_snapshot_owner is not None:
        if not isinstance(queue_snapshot_owner, str):
          raise ConfigurationError(
            "SCRAPY_QUEUE_SNAPSHOT_OWNER must be a non-empty string or unset.",
            setting_name="SCRAPY_QUEUE_SNAPSHOT_OWNER",
            setting_value=snapshot_owner_raw,
          )
        try:
          _validate_key_name(
            queue_snapshot_owner,
            "SCRAPY_QUEUE_SNAPSHOT_OWNER",
          )
        except ValueError as exc:
          raise ConfigurationError(
            str(exc),
            setting_name="SCRAPY_QUEUE_SNAPSHOT_OWNER",
            setting_value=snapshot_owner_raw,
          ) from exc
      return cls(
        connection_manager=manager,
        queue_key=resolved_queue_key,
        queue_strategy=queue_strategy,
        backpressure_pause_at=pause_at,
        backpressure_resume_at=resume_at,
        queue_depth_sample_every=depth_sample_every,
        queue_max_item_bytes=queue_max_item_bytes,
        monitor_backpressure_threshold=monitor_backpressure_threshold,
        monitor_pop_rate_window_s=monitor_pop_rate_window_s,
        queue_snapshot_owner=queue_snapshot_owner,
      )
    except BaseException:
      try:
        manager.close()
      except BaseException:
        try:
          logger.exception(
            "Failed to release ConnectionManager after scheduler factory failure"
          )
        except BaseException:
          pass
      raise

  @staticmethod
  def _resolve_monitor_for_spider(
    spider: Spider,
    *,
    backpressure_threshold: int,
    pop_rate_window_s: float,
  ) -> Any:
    """Resolve a ScrapyStatsMonitor threaded with the R14-C U2 knobs.

    Pre-R14-C the ``BackendQueue`` resolved its own monitor internally with
    constructor defaults, so the operator-tuned ``SCRAPY_MONITOR_*`` settings
    could never reach it. R14-C moves monitor resolution to the scheduler
    (which holds the threaded values) and forwards the monitor into
    ``BackendQueue`` explicitly, so the U2 ``backpressure_threshold`` +
    ``pop_rate_window_s`` knobs take effect.

    Falls back to ``NullMonitor`` when ``spider.crawler.stats`` is unreachable
    (no spider, no crawler, or no stats — e.g. unit-test spiders), mirroring
    ``BackendQueue._resolve_monitor``.

    Args:
        spider: The spider to resolve a stats collector from.
        backpressure_threshold: Depth above which ``queue/backpressure``
            flips on (forwarded to ``ScrapyStatsMonitor``).
        pop_rate_window_s: Trailing window for ``queue/pop_rate`` (forwarded
            to ``ScrapyStatsMonitor``).

    Returns:
        A ``ScrapyStatsMonitor`` if ``spider.crawler.stats`` is reachable,
        else a ``NullMonitor``.
    """
    from scrapy_extension.monitor import NullMonitor, ScrapyStatsMonitor

    crawler = getattr(spider, "crawler", None)
    stats = getattr(crawler, "stats", None) if crawler is not None else None
    if stats is None:
      return NullMonitor()
    return ScrapyStatsMonitor(
      stats,
      backpressure_threshold=backpressure_threshold,
      pop_rate_window_s=pop_rate_window_s,
    )

  @staticmethod
  def _enforce_ack_concurrency_gate(settings: Settings, backend_type: Any) -> None:
    """Raise ConfigurationError for single-slot-ack backends under concurrency.

    Reads ``QueueBackend.requires_ack`` / ``supports_concurrent_ack`` from
    the backend CLASS (no instantiation — pure attribute read via the
    registry descriptor's ``backend_cls_path``). A single-slot-ack backend
    (``supports_concurrent_ack=False``) silently loses N-1 of N acks under
    ``CONCURRENT_REQUESTS > 1``; this gate makes that loud unless the
    operator opts in via ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS``.

    Note: every bundled backend sets ``supports_concurrent_ack=True`` (2026-
    07-10), so this gate is unreachable for the 10 bundled backends — it
    remains a defensive backstop for a hypothetical 3rd-party single-slot
    backend.

    Args:
        settings: Scrapy settings (read ``CONCURRENT_REQUESTS`` + opt-out).
        backend_type: The resolved ``BackendType`` for the queue component.

    Raises:
        ConfigurationError: If the backend requires ack, does not support
            concurrent ack, ``CONCURRENT_REQUESTS > 1``, and the opt-out
            is not set.
    """
    from scrapy_extension.backends.connectors import _load_object
    from scrapy_extension.backends.registry import get_descriptor

    descriptor = get_descriptor(str(backend_type))
    backend_cls = _load_object(descriptor.backend_cls_path)
    requires_ack = getattr(backend_cls, "requires_ack", False)
    supports_concurrent = getattr(backend_cls, "supports_concurrent_ack", True)
    if not requires_ack or supports_concurrent:
      return
    concurrent = parse_int_setting(
      settings.get("CONCURRENT_REQUESTS", 16),
      "CONCURRENT_REQUESTS",
      minimum=1,
    )
    if concurrent <= 1:
      return
    opt_out = get_bool_setting(
      settings,
      "SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS",
    )
    if opt_out:
      return
    # ``backend_type`` is the registry-key string; format it bare (no repr
    # quoting) so the message reads naturally for both BackendType members
    # and plain strings.
    bt_name = (
      backend_type.value if isinstance(backend_type, BackendType) else backend_type
    )
    msg = (
      f"Backend {bt_name!r} requires explicit ack but does NOT "
      f"support concurrent ack (single-slot ack). Under "
      f"CONCURRENT_REQUESTS={concurrent} (>1), only the last-popped "
      f"message is ackable and the rest are silently lost (at-least-once "
      f"violation). Either (a) pin CONCURRENT_REQUESTS=1, (b) switch to a "
      f"backend with supports_concurrent_ack=True (all bundled MQ backends "
      f"qualify: Kafka/RabbitMQ/RocketMQ/SQS/Pulsar), or (c) set "
      f"SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=True to opt in to the "
      f"known-broken mode (NOT recommended — silent data loss)."
    )
    raise ConfigurationError(
      msg,
      setting_name="CONCURRENT_REQUESTS",
      setting_value=concurrent,
    )

  @staticmethod
  def _warn_strategy_mq_ack_bypass(queue_strategy: Any, backend_type: Any) -> None:
    """Warn when the resolved queue strategy does NOT thread the MQ per-message
    ack token AND the backend requires one (#28).

    A strategy threads the token iff its class overrides ``pop_with_ack``.
    Every backend-delegating bundled strategy does so; round-robin and ring
    buffer are fully in-process and inherit ``(pop(), None)``. Pairing an
    unknown non-threading strategy with an MQ backend is ambiguous: a backend
    pop would lose its ack token, while local storage bypasses broker durability
    entirely. Surface either case so operators choose it deliberately.
    """
    # Strategies that override pop_with_ack thread the MQ token — no warning.
    if "pop_with_ack" in type(queue_strategy).__dict__:
      return
    from scrapy_extension.backends.connectors import _load_object
    from scrapy_extension.backends.registry import get_descriptor

    descriptor = get_descriptor(str(backend_type))
    backend_cls = _load_object(descriptor.backend_cls_path)
    if not getattr(backend_cls, "requires_ack", False):
      return
    bt_name = (
      backend_type.value if isinstance(backend_type, BackendType) else backend_type
    )
    logger.warning(
      "Queue strategy %s paired with MQ backend %r (requires_ack=True) does "
      "not override pop_with_ack. A backend-delegating strategy would lose "
      "per-message ack correlation; a local strategy bypasses broker "
      "durability. Use a backend-threading strategy "
      "(passthrough/delay/throttle/priority/time_wheel/work_stealing), or "
      "accept the local-storage tradeoff deliberately.",
      type(queue_strategy).__name__,
      bt_name,
    )

  @classmethod
  def from_crawler(cls, crawler: Crawler) -> BackendScheduler:
    """Create scheduler from crawler."""
    scheduler = cls.from_settings(
      crawler.settings,
      spider_name=cls._crawler_spider_name(crawler),
    )
    try:
      scheduler.stats = crawler.stats
      dupefilter_path = crawler.settings.get("DUPEFILTER_CLASS")
      if dupefilter_path:
        dupefilter_cls = load_object(dupefilter_path)
        scheduler.dupefilter = dupefilter_cls.from_crawler(crawler)
        scheduler._owns_dupefilter = True
      return scheduler
    except BaseException:
      try:
        scheduler.close("crawler-factory-failed")
      except BaseException:
        try:
          logger.exception(
            "Failed to close scheduler after crawler factory failure"
          )
        except BaseException:
          pass
      raise

  @staticmethod
  def _crawler_spider_name(crawler: Crawler) -> str | None:
    """Return an attached instance or crawler spider-class name when known."""
    for owner in (
      getattr(crawler, "spider", None),
      getattr(crawler, "spidercls", None),
    ):
      name = getattr(owner, "name", None)
      if isinstance(name, str) and name:
        return name
    return None

  def open(self, spider: Spider) -> Deferred[None] | None:
    """Open the scheduler for a spider and wire ack/nack signals.

    Return type matches Scrapy's ``Scheduler.open`` protocol
    (``Deferred[None] | None``). This implementation is synchronous —
    returns ``None`` — which Scrapy's engine handles correctly via
    ``yield self.scheduler.open(spider)`` (yielding None is a no-op in
    both inlineCallbacks and async-first reactor modes).

    **Queue-key templating (round-2, C8 fix).** If ``self.queue_key``
    contains the literal token ``{spider}``, the token is substituted with
    ``spider.name`` BEFORE constructing the BackendQueue. This lets two
    spiders on the same backend use disjoint queues
    (``SCRAPY_QUEUE_KEY="q:{spider}"``) — without it, the default
    ``scheduler-queue`` is shared across spiders (silent cross-spider
    request leakage / contamination). Default key unchanged → existing
    single-spider deployments are unaffected. Multi-spider footgun: with
    templating, the dedup set is still shared unless separately templated
    (see dupefilter_key in BackendDupeFilter).

    Args:
        spider: The spider instance.

    Raises:
        ValueError: If ``spider.name`` contains characters unsafe for use as
            a backend key (only ``[a-zA-Z0-9._:-]`` allowed). Surfaces the
            misconfiguration at open time rather than as a confusing
            "_validate_key_name" failure deep inside the first push.
    """
    with self._lifecycle_lock:
      if self._lifecycle_state == _LIFECYCLE_CLOSED:
        raise RuntimeError("Scheduler is closed and cannot be reopened")
      if self._lifecycle_state == _LIFECYCLE_OPEN:
        if self._spider is spider:
          return None
        raise RuntimeError("Scheduler is already open for a different spider")

      try:
        _validate_key_name(spider.name, field_name="spider.name")
        self._spider = spider
        # Resolve {spider} template in queue_key at open() (round-2 C8 fix).
        # str.replace (not str.format) so brace-bearing keys like
        # "q:{spider}-{date}" don't raise KeyError on the unrelated {date};
        # matches the dupefilter path's .replace() substitution.
        self.queue_key = self._queue_key_template.replace("{spider}", spider.name)
        if (
          self._owns_dupefilter
          and self.dupefilter is not None
          and not self._dupefilter_open
          and not self._dupefilter_released
        ):
          self.dupefilter.open(spider)
          self._dupefilter_open = True
        # R14-C: resolve the monitor FIRST so it can be threaded into BackendQueue
        # with the operator-tuned backpressure_threshold + pop_rate_window_s.
        # Pre-R14-C the BackendQueue resolved its own monitor internally (default
        # ScrapyStatsMonitor with constructor defaults) — but that path could not
        # see the SCRAPY_MONITOR_* settings, so the U2 knobs were stuck at
        # defaults. Resolving here + passing explicitly closes the loop.
        monitor = BackendScheduler._resolve_monitor_for_spider(
          spider,
          backpressure_threshold=self._monitor_backpressure_threshold,
          pop_rate_window_s=self._monitor_pop_rate_window_s,
        )
        # R14-D follow-up: thread the resolved monitor into the ConnectionManager
        # so the connection-lifecycle hooks (on_connect/on_disconnect/on_retry →
        # backend/{connect,disconnect,retry}_count) actually fire in production.
        # Without this, ConnectionManager defaults to NullMonitor and the hooks
        # R14-D wired are dead observability outside the queue path.
        self.connection_manager.set_monitor(monitor)
        self._queue = BackendQueue(
          connection_manager=self.connection_manager,
          queue_name=self.queue_key,
          spider=spider,
          queue_strategy=self._queue_strategy,
          max_item_bytes=self._queue_max_item_bytes,
          monitor=monitor,
          depth_sample_every=self._queue_depth_sample_every,
          pop_rate_window_s=self._monitor_pop_rate_window_s,
          snapshot_owner=self._queue_snapshot_owner,
        )
        self._connect_ack_signals(spider)
      except BaseException:
        try:
          self._close_locked("open-failed")
        except BaseException:
          try:
            logger.exception("Failed to clean up scheduler after open failure")
          except BaseException:
            pass
        raise

      # Reset backpressure gate for a clean per-spider start (round-4 BP-2).
      self._backpressure_paused = False
      self._backpressure_probe_due = False
      self._lifecycle_state = _LIFECYCLE_OPEN
      logger.info("Scheduler opened for spider %s", spider.name)
      return None

  def _connect_ack_signals(self, spider: Spider) -> None:
    """Wire response_received → ack, spider_error → nack.

    Uses ``spider.crawler.signals`` so the scheduler doesn't need a
    crawler reference at construction time. Idempotent: guarded by
    ``_signals_connected`` so re-open doesn't double-register.
    """
    if self._signals_connected:
      return
    crawler = getattr(spider, "crawler", None)
    if crawler is None:
      logger.warning(
        "spider has no 'crawler' attribute — ack/nack signals not wired. "
        "Kafka/RabbitMQ messages will re-deliver on consumer restart "
        "(at-least-once) but won't be acked in-session. "
        "Ensure the spider is created via CrawlerProcess/CrawlerRunner."
      )
      return
    sig = crawler.signals
    signal_handlers = (
      (self._on_response_received, signals.response_received),
      (self._on_spider_error, signals.spider_error),
    )
    connected: list[tuple[Any, Any]] = []
    try:
      for handler, signal in signal_handlers:
        sig.connect(handler, signal=signal)
        connected.append((handler, signal))
    except BaseException:
      for handler, signal in reversed(connected):
        try:
          sig.disconnect(handler, signal=signal)
        except Exception:
          try:
            logger.exception(
              "Failed to roll back %s after signal registration failure",
              signal,
            )
          except BaseException:
            pass
      raise
    self._connected_signals = sig
    self._signals_connected = True

  def _on_response_received(
    self,
    response: Response,
    request: Request,
    spider: Spider,
  ) -> None:
    """Ack the specific popped message after the download succeeded.

    Reads the ack token the pop path injected into
    ``request.meta["_backend_ack_token"]`` and forwards it to
    ``BackendQueue.ack(token=…)`` so the backend acks the *specific*
    message (Kafka contiguous watermark / RabbitMQ per-tag basic_ack) —
    correct under ``CONCURRENT_REQUESTS > 1``.
    """
    del response, spider
    self._ack_request_token(
      request,
      log_message="Failed to ack message after response_received",
    )

  def _on_spider_error(
    self,
    failure: Failure,
    response: Response,
    spider: Spider,
  ) -> None:
    """Nack the specific popped message so it re-delivers for retry.

    Reads the ack token from ``response.request.meta`` (the request that
    failed) and forwards it to ``BackendQueue.nack(token=…)``.
    """
    del failure, spider
    failed_request = getattr(response, "request", None)
    if failed_request is None:
      return
    self._nack_request_token(
      failed_request,
      log_message="Failed to nack message after spider_error",
    )

  def _ack_token(self, token: Any, *, log_message: str) -> bool:
    """Best-effort ack of one explicit token, returning settlement success."""
    queue = self._queue
    if queue is None:
      return False
    try:
      queue.ack(token=token)
    except BackendError:
      if self.stats:
        self.stats.inc_value("scheduler/ack_error")
      logger.exception(log_message)
      return False
    return True

  def _ack_request_token(self, request: Request, *, log_message: str) -> None:
    """Best-effort ack of the token carried by ``request``."""
    if getattr(request, "meta", None) is None:
      return
    token = request.meta.get(BACKEND_ACK_TOKEN_META_KEY)
    if token is None:
      return
    if self._ack_token(token, log_message=log_message):
      request.meta.pop(BACKEND_ACK_TOKEN_META_KEY, None)

  def _nack_token(self, token: Any, *, log_message: str) -> bool:
    """Best-effort nack of one explicit token, returning settlement success."""
    queue = self._queue
    if queue is None:
      return False
    try:
      queue.nack(token=token)
    except BackendError:
      if self.stats:
        self.stats.inc_value("scheduler/nack_error")
      logger.exception(log_message)
      return False
    return True

  def _nack_request_token(self, request: Request, *, log_message: str) -> None:
    """Best-effort nack of the token carried by ``request``."""
    if getattr(request, "meta", None) is None:
      return
    token = request.meta.get(BACKEND_ACK_TOKEN_META_KEY)
    if token is None:
      return
    if self._nack_token(token, log_message=log_message):
      request.meta.pop(BACKEND_ACK_TOKEN_META_KEY, None)

  def _restore_original_errback(self, request: Request) -> None:
    """Remove this scheduler's transient failure wrapper before enqueue."""
    errback = request.errback
    if isinstance(errback, _BackendDownloadFailureErrback):
      request.errback = errback.original

  def _wrap_download_failure(self, request: Request) -> None:
    """Install terminal downloader-failure handling on one popped delivery."""
    if request.meta.get(BACKEND_ACK_TOKEN_META_KEY) is None:
      return
    if isinstance(request.errback, _BackendDownloadFailureErrback):
      return
    request.errback = _BackendDownloadFailureErrback(self, request.errback)

  def close(self, reason: str) -> Deferred[None] | None:
    """Close the scheduler."""
    with self._lifecycle_lock:
      self._close_locked(reason)
    return None

  def _close_locked(self, reason: str) -> None:
    """Release one scheduler lifecycle while ``_lifecycle_lock`` is held."""
    if self._lifecycle_state == _LIFECYCLE_CLOSED:
      return None
    self._lifecycle_state = _LIFECYCLE_CLOSED
    logger.info("Scheduler closed: %s", reason)
    if self._connected_signals is not None:
      signal_handlers = (
        (self._on_response_received, signals.response_received),
        (self._on_spider_error, signals.spider_error),
      )
      for handler, signal in signal_handlers:
        try:
          self._connected_signals.disconnect(handler, signal=signal)
        except Exception:
          # Each stale/already-disconnected tuple is independent: one failure
          # must not leave the other handler registered or block later cleanup.
          logger.exception("Failed to disconnect %s during shutdown", signal)
    # Close the queue strategy FIRST so it can warn about / release any
    # in-process held state (e.g. DelayQueueStrategy's delayed items) while
    # the backend is still connected. Must precede connection_manager.close().
    if self._queue is not None:
      try:
        self._queue.close()
      except Exception:
        logger.exception("Failed to close queue strategy during shutdown")
    if (
      self._owns_dupefilter
      and self.dupefilter is not None
      and not self._dupefilter_released
    ):
      self._dupefilter_released = True
      try:
        self.dupefilter.close(reason)
      except Exception:
        logger.exception("Failed to close dupefilter during shutdown")
      finally:
        self._dupefilter_open = False
    self._queue = None
    self._spider = None
    self._connected_signals = None
    self._signals_connected = False
    self._backpressure_paused = False
    self._backpressure_probe_due = False
    if self._owns_connection_manager and not self._manager_released:
      # ``from_settings`` acquired one shared-manager reference for this
      # scheduler. Pair it with exactly one release even if Scrapy (or a
      # caller) delivers duplicate close notifications.
      self._manager_released = True
      self.connection_manager.close()
    return None

  def enqueue_request(self, request: Request) -> bool:
    """Enqueue a request.

    Applies duplicate filtering through the configured ``DUPEFILTER_CLASS``
    unless ``request.dont_filter`` is set.

    **Dedup-outage envelope (round-2, C6 fix).** The
    ``dupefilter.request_seen`` call is INSIDE the try-block. A
    ``QueueError`` / ``BackendError`` from the dedup backend (partial
    connectivity: queue up, dedup backend down) is logged, the
    ``scheduler/dupefilter_error`` stat is incremented, and the request is
    default-enqueued (NOT dropped) so no URL is lost. The spider stays up
    in degraded mode rather than crashing on an unhandled exception.

    Args:
        request: The request to enqueue.

    Returns:
        True if the request was enqueued, False on duplicate or push failure.
    """
    queue = self._queue
    if queue is None:
      msg = "Scheduler not opened"
      raise RuntimeError(msg)

    # Retry/redirect middleware copies the popped request, including our
    # transient errback wrapper. Restore the user's serializable errback before
    # duplicate filtering or queue serialization. The old ack token remains in
    # meta until the replacement push, durable duplicate handoff, or ordinary
    # tokenless duplicate drop commits.
    self._restore_original_errback(request)
    priority = request.priority
    phase = "dedup"
    dedup_reserved = False
    reservation: object | None = None
    reservation_intent: object | None = None
    commit_reservation: Callable[[object], None] | None = None
    commit_volatile_reservation: Callable[[object], None] | None = None
    rollback_reservation: Callable[[object], None] | None = None
    rollback_reservation_intent: Callable[[object], None] | None = None
    try:
      # Dedup check is INSIDE the try (round-2 C6 fix) so a dedup-backend
      # outage degrades to default-enqueue instead of crashing the spider.
      # `phase` distinguishes WHICH call raised so the stat + retry are
      # attributed correctly (review follow-up: the prior branch couldn't
      # tell a dedup raise from a push raise → wrong stat + redundant retry).
      if self.dupefilter is not None and not request.dont_filter:
        atomic_methods = _atomic_dupefilter_methods(self.dupefilter)
        if atomic_methods is not None:
          (
            atomic_decision,
            commit_reservation,
            commit_volatile_reservation,
            rollback_reservation,
            rollback_reservation_intent,
          ) = atomic_methods
          reservation_intent = object()
          decision = atomic_decision(request, reservation_intent)
          seen = bool(decision.seen)
          reservation = decision.reservation
          observational = bool(getattr(decision, "observational", False))
        else:
          observational = False
          seen = self.dupefilter.request_seen(request)
          if not seen:
            consume_reservation = getattr(
              self.dupefilter,
              "consume_reservation",
              None,
            )
            # Scrapy's standard dupefilter protocol has no reservation-result
            # API. Preserve the precise legacy extension when available and
            # the historical conservative rollback for custom filters that
            # provide neither optional method.
            dedup_reserved = (
              bool(consume_reservation(request))
              if callable(consume_reservation)
              else True
            )
        if seen:
          # Monitor callbacks are telemetry, not new scheduling attempts. The
          # exact originating Request is suppressed without touching its token;
          # the outer enqueue still owns the durable handoff.
          if observational:
            return False
          if request.meta.get(BACKEND_ACK_TOKEN_META_KEY) is None:
            if self._spider is not None:
              self.dupefilter.log(request, self._spider)
            return False
          # A marker alone cannot prove that another worker's queue push has
          # committed. For a broker-sourced replacement, transfer the request
          # to durable queue storage before BackendQueue acknowledges its token.
          # This may replay a committed duplicate, but cannot lose the source.
      phase = "push"
      push_is_durable = _push_queue_with_durability(
        queue,
        request,
        priority=priority,
      )
      if (
        reservation is not None
        and commit_reservation is not None
        and rollback_reservation is not None
      ):
        completed_reservation = reservation
        reservation = None
        if push_is_durable is not True:
          # A process-local strategy accepted the request, but publishing a
          # persistent dedup marker would turn a hard crash into permanent
          # loss. The bundled filter records only a lifecycle-local shadow;
          # third-party atomic filters without that extension remain unmarked.
          if commit_volatile_reservation is not None:
            self._commit_atomic_reservation(
              completed_reservation,
              commit_volatile_reservation,
            )
            if self.stats:
              self.stats.inc_value("scheduler/dupefilter_volatile_marker")
          else:
            self._rollback_atomic_reservation(
              completed_reservation,
              rollback_reservation,
              preserve_primary=False,
            )
            if self.stats:
              self.stats.inc_value("scheduler/dupefilter_volatile_unmarked")
        else:
          self._commit_atomic_reservation(
            completed_reservation,
            commit_reservation,
          )
        # Retain the owner intent until the commit/rollback call returns. If a
        # process-control signal interrupts finalization, the outer handler can
        # still discard bookkeeping without touching an ambiguous marker.
        reservation_intent = None
      if self.stats:
        self.stats.inc_value("scheduler/enqueued")
    except SerializationError:
      if reservation is not None and rollback_reservation is not None:
        self._rollback_atomic_reservation(
          reservation,
          rollback_reservation,
          preserve_primary=False,
        )
      elif (
        reservation_intent is not None
        and rollback_reservation_intent is not None
      ):
        self._rollback_atomic_reservation(
          reservation_intent,
          rollback_reservation_intent,
          preserve_primary=False,
        )
      elif dedup_reserved:
        self._rollback_dupefilter_reservation(request)
      logger.exception("Failed to serialize request for enqueue")
      if self.stats:
        self.stats.inc_value("scheduler/serialization_errors")
      return False
    except (QueueError, BackendError):
      if phase == "dedup":
        if (
          reservation_intent is not None
          and rollback_reservation_intent is not None
        ):
          self._rollback_atomic_reservation(
            reservation_intent,
            rollback_reservation_intent,
            preserve_primary=False,
          )
        # Dedup-backend outage: degrade to enqueue (don't lose the URL),
        # attribute to the dedup-error stat.
        logger.exception("Failed to consult dupefilter; defaulting to enqueue")
        if self.stats:
          self.stats.inc_value("scheduler/dupefilter_error")
        try:
          queue.push(request, priority=priority)
          if self.stats:
            self.stats.inc_value("scheduler/enqueued")
        except (QueueError, SerializationError, BackendError):
          logger.exception("Failed to enqueue request after dedup outage")
          return False
        return True
      # phase == "push": a plain queue-push failure (not a dedup outage).
      if reservation is not None and rollback_reservation is not None:
        self._rollback_atomic_reservation(
          reservation,
          rollback_reservation,
          preserve_primary=False,
        )
      elif (
        reservation_intent is not None
        and rollback_reservation_intent is not None
      ):
        self._rollback_atomic_reservation(
          reservation_intent,
          rollback_reservation_intent,
          preserve_primary=False,
        )
      elif dedup_reserved:
        self._rollback_dupefilter_reservation(request)
      logger.exception("Failed to enqueue request")
      if self.stats:
        self.stats.inc_value("scheduler/queue_error")
      return False
    except BaseException:
      # Process-control interruption after receipt handoff but before a
      # confirmed push follows the package's at-least-once policy: compensate
      # best-effort, preserve the original signal, and accept possible replay
      # rather than leave a permanent ghost fingerprint.
      try:
        if (
          reservation_intent is not None
          and rollback_reservation_intent is not None
        ):
          # Intent rollback is deliberately telemetry-free and cannot remove
          # an ambiguous marker. It is therefore the safest process-control
          # cleanup both before and after the queue commit boundary.
          rollback_reservation_intent(reservation_intent)
        elif reservation is not None and rollback_reservation is not None:
          self._rollback_atomic_reservation(
            reservation,
            rollback_reservation,
            preserve_primary=True,
          )
        elif dedup_reserved:
          self._rollback_dupefilter_reservation(request, preserve_primary=True)
      except BaseException:
        # This outer guard also covers an asynchronous signal before the
        # cleanup callee establishes its own try-region. Retry the silent
        # owner fence once; it is idempotent and does not mutate membership.
        if (
          reservation_intent is not None
          and rollback_reservation_intent is not None
        ):
          try:
            rollback_reservation_intent(reservation_intent)
          except BaseException:
            pass
        try:
          logger.exception(
            "Failed to compensate enqueue interruption while preserving signal"
          )
        except BaseException:
          pass
      raise
    else:
      return True

  def _rollback_atomic_reservation(
    self,
    reservation: object,
    rollback: Callable[[object], None],
    *,
    preserve_primary: bool,
  ) -> None:
    """Roll back one receipt with explicit process-control precedence."""
    try:
      rollback(reservation)
    except Exception:  # noqa: BLE001 - preserve the triggering queue failure
      if preserve_primary:
        try:
          logger.exception("Failed to roll back atomic dupefilter reservation")
          if self.stats:
            self.stats.inc_value("scheduler/dupefilter_rollback_error")
        except BaseException:
          pass
      else:
        logger.exception("Failed to roll back atomic dupefilter reservation")
        if self.stats:
          self.stats.inc_value("scheduler/dupefilter_rollback_error")
    except BaseException:
      if not preserve_primary:
        raise
      try:
        logger.exception("Failed to roll back atomic dupefilter reservation")
      except BaseException:
        pass
      try:
        if self.stats:
          self.stats.inc_value("scheduler/dupefilter_rollback_error")
      except BaseException:
        pass

  def _commit_atomic_reservation(
    self,
    reservation: object,
    commit: Callable[[object], None],
  ) -> None:
    """Finalize receipt bookkeeping after the queue commit boundary.

    An ordinary bookkeeping failure cannot reclassify an already durable push.
    Process-control signals still propagate, with caller state already cleared
    so the outer handler cannot roll back the committed marker.
    """
    try:
      commit(reservation)
    except Exception:  # noqa: BLE001 - queue durability is authoritative
      try:
        logger.exception("Failed to finalize atomic dupefilter reservation")
      except BaseException:
        pass
      if self.stats:
        self.stats.inc_value("scheduler/dupefilter_commit_error")

  def _rollback_dupefilter_reservation(
    self,
    request: Request,
    *,
    preserve_primary: bool = False,
  ) -> None:
    """Best-effort compensation for request_seen followed by a failed push.

    ``forget`` is an optional extension to Scrapy's dupefilter protocol. The
    bundled ``BackendDupeFilter`` implements it with atomic removal or a
    bounded one-shot retry allowance. Keep this call duck-typed for custom
    dupefilters; unsupported or failed compensation leaves the original push
    failure intact and surfaces an explicit rollback-error stat.
    """
    forget = getattr(self.dupefilter, "forget", None)
    if not callable(forget):
      logger.warning(
        "Dupefilter %s cannot roll back a fingerprint after queue push failure",
        type(self.dupefilter).__name__,
      )
      if self.stats:
        self.stats.inc_value("scheduler/dupefilter_rollback_error")
      return

    try:
      forget(request)
    except Exception:  # noqa: BLE001 - preserve the triggering queue failure
      if preserve_primary:
        try:
          logger.exception("Failed to roll back dupefilter reservation")
          if self.stats:
            self.stats.inc_value("scheduler/dupefilter_rollback_error")
        except BaseException:
          pass
      else:
        logger.exception("Failed to roll back dupefilter reservation")
        if self.stats:
          self.stats.inc_value("scheduler/dupefilter_rollback_error")
    except BaseException:  # compensation must not hide process-control primary
      if not preserve_primary:
        raise
      try:
        logger.exception("Failed to roll back dupefilter reservation")
      except BaseException:
        pass
      try:
        if self.stats:
          self.stats.inc_value("scheduler/dupefilter_rollback_error")
      except BaseException:
        pass

  def next_request(self) -> Request | None:
    """Get the next request from the queue.

    Returns:
        The next request, or None if the queue is empty or paused under the
        backpressure gate.
    """
    try:
      queue = self._queue
      if queue is None:
        msg = "Scheduler not opened"
        raise RuntimeError(msg)
      # Backpressure depth gate (round-4 BP-2). Depth source is
      # len(self._queue) — fresh, same source has_pending_requests trusts.
      # A failed depth lookup disables the gate for that poll and falls through
      # to pop (degraded safely, with no depth-dependent stall).
      if self._pause_at is not None:
        # Read depth once. len() can raise QueueError, or NotImplementedError
        # on backends whose queue_len is unsupported (e.g. RocketMQ). On either,
        # the gate can't read depth → skip it (degrade to pop) rather than
        # crash or stall — matches has_pending_requests' error handling.
        try:
          depth = len(queue)
        except (QueueError, NotImplementedError):
          depth = None
        if depth is not None:
          # _resume_at defaults to _pause_at in __init__, so it is non-None
          # whenever _pause_at is non-None; bind a narrowed local for the type
          # checker (the attribute itself stays int | None).
          resume_at = self._resume_at
          # bandit B101 accepted — type-checker narrowing (_resume_at
          # defaults to _pause_at in __init__, so non-None here), not a
          # security control.
          assert resume_at is not None  # nosec B101
          if not self._backpressure_paused and depth >= self._pause_at:
            self._backpressure_paused = True
            self._backpressure_probe_due = True
            if self.stats:
              self.stats.inc_value("scheduler/backpressure_pause")
            return None
          if self._backpressure_paused:
            if depth <= resume_at:
              self._backpressure_paused = False
              self._backpressure_probe_due = False
              if self.stats:
                self.stats.inc_value("scheduler/backpressure_resume")
            elif self._backpressure_probe_due:
              self._backpressure_probe_due = False
            else:
              self._backpressure_probe_due = True
              return None  # paused — next poll is the bounded progress probe
      request = queue.pop(timeout=0)
      if request:
        self._wrap_download_failure(request)
        if self.stats:
          self.stats.inc_value("scheduler/dequeued")
    except SerializationError:
      logger.exception("Failed to deserialize queued request")
      if self.stats:
        self.stats.inc_value("scheduler/deserialization_errors")
      return None
    except (QueueError, BackendConnectionError, CircuitBreakerOpenError):
      logger.exception("Failed to get next request")
      return None
    else:
      return request

  def has_pending_requests(self) -> bool:
    """Check if there are pending requests.

    Returns:
        True if there are pending requests.
    """
    try:
      return len(self) > 0
    except (
      NotImplementedError,
      QueueError,
      BackendConnectionError,
      CircuitBreakerOpenError,
    ):
      logger.warning(
        "Queue length lookup is unavailable; assuming pending requests exist"
      )
      return True

  def __len__(self) -> int:
    """Get the number of pending requests.

    Returns:
        Number of pending requests.
    """
    queue = self._queue
    if queue is None:
      return 0
    return len(queue)
