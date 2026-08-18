"""Queue component for scrapy-extension.

This module provides a Scrapy queue component that uses backend queue interfaces.
"""

from __future__ import annotations

import base64
import binascii
import inspect
import logging
import math
import sys
import threading
import time
import warnings
from collections import deque
from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from scrapy.http import FormRequest, JsonRequest, Request, XmlRpcRequest

from scrapy_extension.backends.base import (
    JSONSerializer,
    QueueBackend,
    _validate_key_name,
)
from scrapy_extension.exceptions import QueueError, SerializationError
from scrapy_extension.exceptions._redaction import serialization_error_boundary
from scrapy_extension.monitor import NullMonitor, ScrapyStatsMonitor
from scrapy_extension.monitor.base import DEFAULT_POP_RATE_WINDOW_S, Monitor
from scrapy_extension.queue.snapshot import (
    DEFAULT_SNAPSHOT_CHUNK_BYTES,
    DEFAULT_SNAPSHOT_MAX_BYTES,
    MAX_SNAPSHOT_CHUNK_BYTES,
    MAX_SNAPSHOT_CHUNKS,
    SnapshotRepository,
    SnapshotRepositoryError,
)
from scrapy_extension.queue.strategies.base import (
    QueueStrategy,
    _BoundQueueAckToken,
    _PreparedQueuePush,
    _QueueAckToken,
    normalize_queue_timeout,
)
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy

if TYPE_CHECKING:
    from scrapy import Spider

    from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)

#: Default per-item serialized-byte cap (1 MiB — matches Memcached's 1 MB ceiling).
DEFAULT_QUEUE_MAX_ITEM_BYTES = 1_048_576

_QUEUE_PUSH_SERIALIZATION_FAILURE = "Failed to serialize request."
_QUEUE_POP_SERIALIZATION_FAILURE = "Failed to deserialize request."
_QUEUE_PUSH_MONITOR_FAILURE = "Queue push serialization failed."
_QUEUE_POP_MONITOR_FAILURE = "Queue pop deserialization failed."

_STRATEGY_CLEANUP_NOT_STARTED = "not-started"
_STRATEGY_CLEANUP_STARTED = "started"
_STRATEGY_CLEANUP_SUCCEEDED = "succeeded"
_STRATEGY_CLEANUP_FAILED = "failed"
_STRATEGY_CLEANUP_INDETERMINATE = "indeterminate"

#: R25-B/R26-A: ceiling (bytes) for a restored strategy snapshot. A corrupt or
#: malicious multi-GB blob at the snapshot key would OOM-kill worker startup
#: (``bytes(state)`` copy + ``json.loads`` full materialization) before the
#: ``except`` in :meth:`BackendQueue._restore_snapshot` can recover. 128 MiB
#: sits well above any legitimate in-process strategy state (a delay heap at
#: the ``queue_delay_max_held`` default of 100k items x ~2.7 KB/entry tops out
#: around 270 MB only when completely full; realistic held sets are far
#: smaller) and well below the memory danger zone; mirrors the push-path
#: ``max_item_bytes`` guard. R26-A raised this from the original 16 MiB, which
#: silently dropped legitimate large delay-heap snapshots on restart (persist
#: was uncapped, restore was capped → asymmetric data-loss trap).
_MAX_SNAPSHOT_BYTES = DEFAULT_SNAPSHOT_MAX_BYTES

#: Opaque payload for the private key that fences an eligible legacy snapshot
#: during an empty-state transition. Presence of the separate key, not this
#: value, is authoritative so arbitrary strategy snapshot bytes stay valid.
_EMPTY_SNAPSHOT_TOMBSTONE_MARKER = b"1"

#: Meta key carrying the backend ack token from pop → request → response → ack.
#: Atomic-pop backends set this to ``None`` (harmless); message-queue backends
#: set it to an opaque token bound to the issuing backend incarnation and
#: physical queue so the scheduler can ack the *specific* message that produced
#: this request — correct across concurrency and manager replacement.
BACKEND_ACK_TOKEN_META_KEY = "_backend_ack_token"  # nosec B105

#: Version marker separating current base64 bodies from legacy raw UTF-8 bodies.
_BODY_CODEC_FIELD = "_scrapy_extension_body_codec"
_BODY_CODEC_BASE64_V1 = "base64-v1"

# Fixed classes only: never pass a queue-controlled dotted path to Scrapy's
# dynamic ``load_object`` request reconstruction path.
_REQUEST_CLASSES: dict[str, type[Request]] = {
    f"{request_cls.__module__}.{request_cls.__name__}": request_cls
    for request_cls in (Request, FormRequest, JsonRequest, XmlRpcRequest)
}


class _CloseOwnerToken:
    """Invocation-scoped close ownership reclaimable after its frame unwinds."""

    __slots__ = ("thread_id",)

    def __init__(self) -> None:
        self.thread_id = threading.get_ident()

    @property
    def active(self) -> bool:
        try:
            frame = sys._current_frames().get(self.thread_id)  # noqa: SLF001
        except Exception:  # noqa: BLE001 - stale close ownership is retryable
            return False
        while frame is not None:
            try:
                if frame.f_locals.get("owner_token") is self:
                    return True
            except Exception:  # noqa: BLE001 - fail toward bounded reclamation
                return False
            frame = frame.f_back
        return False


def _invalid_routing_value_description(value: object) -> str:
    """Describe invalid input without invoking an arbitrary or unbounded ``repr``."""
    return f"<{type(value).__name__}>"


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
        wall_clock: Callable[[], float] = time.time,
        snapshot_owner: str | None = None,
        snapshot_connection_manager: ConnectionManager | None = None,
        snapshot_max_bytes: int | None = None,
        snapshot_chunk_bytes: int = DEFAULT_SNAPSHOT_CHUNK_BYTES,
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
                which the ``queue/pop_rate_1m`` gauge is computed. Default
                :data:`~scrapy_extension.monitor.base.DEFAULT_POP_RATE_WINDOW_S`
                (60.0). Round-14 R14-C: threaded via
                ``BackendScheduler.from_settings`` so the window is tunable
                without code changes (round-12 U2 left it stuck at the default).
                This event-sampled gauge may freeze when no further pops occur;
                use ``queue/last_pop_epoch`` to derive external liveness age.
            wall_clock: Wall-clock epoch source for ``queue/last_pop_epoch``.
                Injectable for deterministic tests; defaults to :func:`time.time`.
            snapshot_owner: Optional stable worker identity used to isolate
                in-process strategy snapshots in multi-worker deployments. When
                omitted, a delimiter-safe v3 spider+queue key is used. A missing
                v3 checkpoint falls back only for a safely attributable unscoped
                pre-v3 key, then retires it after the next successful clean
                checkpoint. Multi-worker callers should keep this value stable
                across restarts and unique per worker.
            snapshot_connection_manager: Optional storage-capable connection manager
                used only to restore and persist in-process strategy snapshots.
                Queue operations continue to use ``connection_manager``. The
                caller retains ownership of this manager and must release its
                acquire after :meth:`close` completes.
            snapshot_max_bytes: Maximum logical strategy snapshot size accepted on
                both commit and restore. Defaults to 128 MiB.
            snapshot_chunk_bytes: Maximum immutable generation chunk size. Defaults
                to and cannot exceed the universal backend-safe cap of 256 KiB;
                it also must not exceed ``snapshot_max_bytes``.
        """
        self.connection_manager = connection_manager
        self.queue_name = queue_name
        self._spider = spider
        self.max_item_bytes = max_item_bytes
        self.depth_sample_every = max(1, int(depth_sample_every))
        self._pop_rate_window_s = pop_rate_window_s
        self._wall_clock = wall_clock
        if snapshot_owner is not None:
            _validate_key_name(snapshot_owner, "snapshot_owner")
        self._snapshot_owner = snapshot_owner
        self._snapshot_connection_manager = snapshot_connection_manager
        # Set when startup could not read an eligible legacy checkpoint. Until a
        # successful replacement commit, close must keep that legacy state reachable.
        self._defer_legacy_retirement = False
        resolved_snapshot_max_bytes = (
            _MAX_SNAPSHOT_BYTES if snapshot_max_bytes is None else snapshot_max_bytes
        )
        resolved_snapshot_chunk_bytes = (
            min(snapshot_chunk_bytes, resolved_snapshot_max_bytes)
            if snapshot_max_bytes is None
            else snapshot_chunk_bytes
        )
        self._snapshot_max_bytes = resolved_snapshot_max_bytes
        self._snapshot_chunk_bytes = resolved_snapshot_chunk_bytes
        minimum_snapshot_chunk_bytes = (
            (resolved_snapshot_max_bytes + MAX_SNAPSHOT_CHUNKS - 1)
            // MAX_SNAPSHOT_CHUNKS
            if isinstance(resolved_snapshot_max_bytes, int)
            and not isinstance(resolved_snapshot_max_bytes, bool)
            else 1
        )
        if (
            isinstance(resolved_snapshot_max_bytes, bool)
            or not isinstance(resolved_snapshot_max_bytes, int)
            or resolved_snapshot_max_bytes < 1
            or isinstance(resolved_snapshot_chunk_bytes, bool)
            or not isinstance(resolved_snapshot_chunk_bytes, int)
            or resolved_snapshot_chunk_bytes < minimum_snapshot_chunk_bytes
            or resolved_snapshot_chunk_bytes > resolved_snapshot_max_bytes
            or resolved_snapshot_chunk_bytes > MAX_SNAPSHOT_CHUNK_BYTES
        ):
            raise ValueError("Invalid snapshot size limits.")
        self._strategy: QueueStrategy = (
            queue_strategy
            if queue_strategy is not None
            else PassthroughQueueStrategy(connection_manager)
        )
        # In-process strategies own one logical queue's state. Bind before
        # restore so a shared strategy cannot load one snapshot key and later
        # deliver those items through a different BackendQueue.
        self._strategy.bind(queue_name)
        # One BackendQueue instance represents one lifecycle generation. Closing
        # permanently stops admission on this instance; a scheduler reopen builds a
        # fresh BackendQueue around the reopened strategy.
        self._operation_gate = threading.Condition()
        self._accepting_operations = True
        self._active_operations = 0
        self._close_complete = False
        self._close_in_progress = False
        self._close_attempt = 0
        self._close_owner_token: _CloseOwnerToken | None = None
        # Retain an attempt outcome until every caller that observed that exact
        # attempt has consumed it. Per-caller tokens make registration and
        # idempotent cleanup safe when process-control exceptions interrupt either.
        self._close_attempt_outcomes: dict[int, bool] = {}
        self._close_attempt_terminal: dict[int, bool] = {}
        self._close_attempt_waiters: dict[int, set[object]] = {}
        self._begin_close_complete = False
        self._checkpoint_complete = False
        # Destructive strategy cleanup is at-most-once. ``started`` is published
        # before invoking extension code; every later state is terminal, including
        # ``indeterminate`` when process control interrupts the call boundary.
        self._strategy_cleanup_state = _STRATEGY_CLEANUP_NOT_STARTED
        self._monitor: Monitor = (
            monitor if monitor is not None else self._resolve_monitor(spider)
        )
        # R21-B: forward the monitor to strategies that own operability gauges
        # (DelayQueueStrategy emits queue/delay_depth). Without this the strategy
        # keeps its NullMonitor default and the gauge is silently dead in
        # production. Generic getattr form (mirrors BackendPipeline) so strategies
        # without a set_monitor hook (passthrough/round_robin/throttle/...) are
        # unaffected.
        strategy_set_monitor = getattr(self._strategy, "set_monitor", None)
        if callable(strategy_set_monitor):
            strategy_set_monitor(self._monitor)
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
        self._strategy.open()
        self._restore_snapshot()

    def set_monitor(self, monitor: Monitor) -> None:
        """Replace the queue monitor and forward it to monitor-aware strategies."""
        self._monitor = monitor
        strategy_set_monitor = getattr(self._strategy, "set_monitor", None)
        if callable(strategy_set_monitor):
            strategy_set_monitor(monitor)

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
        # Scrapy's public conversion is the source of truth for callback/errback
        # resolution and the ``_class`` discriminator used by request subclasses.
        # It fails fast when a callback is not an instance method of ``self._spider``
        # instead of enqueueing a payload that can only fail during pop.
        request_dict = request.to_dict(spider=self._spider)
        request_class = request_dict.get("_class")
        if request_class is not None and request_class not in _REQUEST_CLASSES:
            raise ValueError(f"Unsupported queued request class: {request_class!r}")

        body_value = None
        if request.body:
            body_value = base64.b64encode(request.body).decode("ascii")

        # Strip transient scheduler controls from the serialized meta. Ack tokens
        # are opaque and often non-JSON-serializable; delay/source are consumed by
        # push and must not reappear after a persisted queue round-trip, otherwise
        # retries re-apply the original routing policy. This copy is non-mutating:
        # push consumes delay/source from the live request after serialization, and
        # the live ack token remains available to the response signal handlers.
        serialized_meta = {
            k: v
            for k, v in request.meta.items()
            if k not in {BACKEND_ACK_TOKEN_META_KEY, "delay", "source"}
        }

        # ``Headers.to_unicode_dict`` joins repeated values with commas. That is
        # irreversible for headers such as Set-Cookie, whose values must remain
        # separate. Header names are ASCII by the HTTP grammar; values stay bytes
        # and are handled losslessly by JSONSerializer's tagged-bytes codec.
        request_dict["headers"] = {
            name.decode("ascii"): list(values)
            for name, values in request.headers.items()
        }
        request_dict["body"] = body_value
        request_dict["meta"] = serialized_meta
        if body_value is not None:
            request_dict[_BODY_CODEC_FIELD] = _BODY_CODEC_BASE64_V1
        return request_dict

    @serialization_error_boundary(
        _QUEUE_PUSH_SERIALIZATION_FAILURE,
        serializer="json",
    )
    def push(self, request: Request, priority: float = 0.0) -> None:
        """Push a request to the queue.

        .. breaking:: R14-F (retry + delay/source storm prevention)
            The ``delay`` and ``source`` keys are read from ``request.meta`` and
            then **popped** (consumed) after the queue strategy accepts the item.
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
        self._push_with_durability(request, priority)

    # Preserve the stable public hook identity so scheduler dispatch cannot
    # bypass a direct class-level monkeypatch of ``BackendQueue.push``.
    _scheduler_protocol_push = push

    @serialization_error_boundary(
        _QUEUE_PUSH_SERIALIZATION_FAILURE,
        serializer="json",
        monitor_operation="push",
        monitor_messages=(_QUEUE_PUSH_MONITOR_FAILURE,),
        logger=logger,
    )
    def _push_with_durability(
        self,
        request: Request,
        priority: float = 0.0,
    ) -> bool:
        """Push and report durability to the bundled scheduler.

        ``push`` intentionally retains its stable ``None`` return contract. This
        package-private extension lets :class:`BackendScheduler` decide whether a
        persistent dedup marker is safe after a strategy accepts into process-local
        state.
        """
        self._begin_operation("push")
        try:
            return self._push(request, priority)
        finally:
            self._end_operation()

    def _push(self, request: Request, priority: float) -> bool:
        """Execute an admitted push operation."""
        replacement_ack_token = request.meta.get(BACKEND_ACK_TOKEN_META_KEY)
        raw_delay = request.meta.get("delay", 0.0)
        if isinstance(raw_delay, bool) or not isinstance(raw_delay, (int, float)):
            error = QueueError(
                "Invalid queue delay "
                f"{_invalid_routing_value_description(raw_delay)}: expected a finite number >= 0",
                queue_name=self.queue_name,
                operation="push",
            )
            self._terminate_invalid_replacement(request, replacement_ack_token)
            raise error
        delay = 0.0
        delay_error: Exception | None = None
        try:
            delay = float(raw_delay or 0.0)
        except (OverflowError, TypeError, ValueError) as caught_error:
            # Finish this exception suite before acknowledging/logging the rejected
            # replacement. Either hook can be application-provided and must not
            # observe the conversion failure through ``sys.exc_info()``.
            delay_error = caught_error
        if delay_error is not None:
            error = QueueError(
                "Invalid queue delay "
                f"{_invalid_routing_value_description(raw_delay)}: expected a finite number >= 0",
                queue_name=self.queue_name,
                operation="push",
            )
            self._terminate_invalid_replacement(request, replacement_ack_token)
            raise error from delay_error
        if not math.isfinite(delay) or delay < 0:
            error = QueueError(
                "Invalid queue delay "
                f"{_invalid_routing_value_description(raw_delay)}: expected a finite number >= 0",
                queue_name=self.queue_name,
                operation="push",
            )
            self._terminate_invalid_replacement(request, replacement_ack_token)
            raise error
        try:
            valid_priority = (
                not isinstance(priority, bool)
                and isinstance(priority, (int, float))
                and math.isfinite(priority)
            )
        except OverflowError:
            valid_priority = False
        if not valid_priority:
            error = QueueError(
                "Invalid queue priority "
                f"{_invalid_routing_value_description(priority)}: expected a finite number",
                queue_name=self.queue_name,
                operation="push",
            )
            self._terminate_invalid_replacement(request, replacement_ack_token)
            raise error
        raw_source = request.meta.get("source", "default")
        source = ""
        source_error: Exception | None = None
        try:
            source = str(raw_source or "default")
        except Exception as caught_error:
            # See the delay-normalization path above: external cleanup must run
            # only after the raw conversion failure has unwound from this frame.
            source_error = caught_error
        if source_error is not None:
            error = QueueError(
                f"Invalid queue source of type {type(raw_source).__name__}",
                queue_name=self.queue_name,
                operation="push",
            )
            self._terminate_invalid_replacement(request, replacement_ack_token)
            raise error from source_error
        if isinstance(self._strategy, QueueStrategy):
            prepared_push = self._strategy._prepare_push(
                self.queue_name,
                priority=priority,
                delay=delay,
                source=source,
            )
        else:
            # Legacy duck-typed strategies remain usable for ordinary pushes, but
            # their historical ``is_push_durable`` claim is not commit evidence.
            def publish_legacy(item: bytes) -> None:
                self._strategy.push(
                    self.queue_name,
                    item,
                    priority=priority,
                    delay=delay,
                    source=source,
                )

            prepared_push = _PreparedQueuePush.local(
                queue_name=self.queue_name,
                strategy_name=type(self._strategy).__name__,
                publish=publish_legacy,
            )

        if replacement_ack_token is not None and not prepared_push.backend_route:
            # A source delivery can be terminated only after its replacement crosses
            # a crash-durable boundary. Delay/time-wheel holding state, round-robin
            # deques, and ring buffers are process-local; accepting into them and then
            # acking the broker source creates a deterministic hard-crash loss window.
            # Fail closed before serialization or strategy mutation. The scheduler
            # keeps the source token unresolved, rolls back any dedup reservation, and
            # broker redelivery can retry under a durable strategy/configuration.
            self._inc_stat("scheduler/queue/volatile_replacement_rejected")
            raise QueueError(
                "Cannot transfer a broker source delivery through the selected "
                f"{type(self._strategy).__name__} route because it is not "
                "worker-crash durable",
                queue_name=self.queue_name,
                operation="push",
            )
        serialization_failed = False
        try:
            request_dict = self._request_to_dict(request)
            data = self._serializer.serialize(request_dict)
        except Exception:
            # Do not terminate a replacement from this suite. Acknowledgement and
            # stats hooks are externally extensible and would otherwise receive the
            # raw request-conversion failure through ``sys.exc_info()``.
            serialization_failed = True
        if serialization_failed:
            self._terminate_invalid_replacement(request, replacement_ack_token)
            raise SerializationError(
                _QUEUE_PUSH_MONITOR_FAILURE,
                serializer="json",
            ) from None

        if len(data) > self.max_item_bytes:
            self._inc_stat("scheduler/queue/oversize_dropped")
            self._terminate_invalid_replacement(request, replacement_ack_token)
            raise SerializationError(
                _QUEUE_PUSH_SERIALIZATION_FAILURE,
                serializer="json",
            )

        # R14-F: after a successful strategy push, consume delay/source so a re-pushed
        # retry (Scrapy retry middleware re-queues the same request object with
        # the same meta) does NOT re-apply the original delay indefinitely
        # (retry + delay storm) and is not pinned to the original source tag
        # (which would defeat round-robin fairness on the retry path). Callers
        # that want delay/source on every push must re-set them between pushes
        # — see the breaking-change note in the docstring.
        push_is_durable = prepared_push.commit(
            data,
            require_durable=replacement_ack_token is not None,
        )
        if replacement_ack_token is not None and push_is_durable is not True:
            # Defense in depth for a malformed custom prepared route. The item may
            # have been published, so keep the source unresolved and permit replay;
            # never acknowledge it or promote an unknown result to durable proof.
            self._inc_stat("scheduler/queue/volatile_replacement_rejected")
            raise QueueError(
                "Queue push returned no valid worker-crash durability receipt",
                queue_name=self.queue_name,
                operation="push",
            )
        # Routing controls are consumed only after the enqueue commits. If the
        # strategy/backend rejects the push, the caller can retry the same Request
        # without silently losing its delay or source semantics.
        request.meta.pop("delay", None)
        request.meta.pop("source", None)
        replacement_ack_failed = False
        if replacement_ack_token is not None:
            # This push already owns an operation lease. Use the admitted primitive
            # directly so a concurrent close cannot reject the terminal ack between
            # the replacement enqueue and completion of this push.
            try:
                self._ack(token=replacement_ack_token)
            except Exception:  # noqa: BLE001 - replacement is already committed
                # The strategy push is the commit boundary. Reclassifying this as a
                # failed enqueue makes the scheduler roll back its dedup reservation
                # and can let the broker's source redelivery publish a second
                # replacement. Keep the token unresolved, report the terminal failure,
                # and return success for the durable replacement. A later redelivery
                # carrying its own token is durably handed off again before that token
                # is acked; this can replay work but cannot lose the source delivery.
                replacement_ack_failed = True
            else:
                request.meta.pop(BACKEND_ACK_TOKEN_META_KEY, None)
        if replacement_ack_failed:
            self._inc_stat("scheduler/ack_error")
            # The replacement commit is already visible. A broken logging handler
            # must not reclassify that committed enqueue as a failed push.
            try:
                logger.error(
                    "Failed to acknowledge source delivery after replacement committed"
                )
            except BaseException:
                pass
        push_monitor_failed = False
        try:
            self._monitor.on_push(self.queue_name, priority)
        except Exception:  # noqa: BLE001 - enqueue has already committed
            push_monitor_failed = True
        if push_monitor_failed:
            # Keep the monitor's ordinary failure swallowed even when its fallback
            # diagnostic handler raises a control-flow exception.
            try:
                logger.debug("monitor.on_push raised; ignored")
            except BaseException:
                pass
        return push_is_durable

    @serialization_error_boundary(
        _QUEUE_POP_SERIALIZATION_FAILURE,
        serializer="json",
        monitor_operation="pop",
        monitor_messages=(_QUEUE_POP_MONITOR_FAILURE,),
        logger=logger,
    )
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
        self._begin_operation("pop")
        try:
            try:
                normalized_timeout = normalize_queue_timeout(timeout)
            except ValueError as e:
                raise QueueError(
                    str(e),
                    queue_name=self.queue_name,
                    operation="pop",
                ) from e
            return self._pop(normalized_timeout)
        finally:
            self._end_operation()

    def _pop(self, timeout: float) -> Request | None:
        """Execute an admitted pop operation."""
        data, ack_token = self._pop_with_ack(timeout)
        if ack_token is not None and not isinstance(ack_token, _BoundQueueAckToken):
            # A custom strategy that consumes a deferred-ack backend but returns the
            # raw broker token cannot prove which backend incarnation issued it.
            # Fail closed before processing: the source remains unacked and can be
            # redelivered, rather than a late completion being sent to a replacement
            # backend. Built-in backend-delegating strategies all use the binding
            # helpers; in-process strategies return no token.
            backend = self.connection_manager.get_queue_backend()
            wrapped_backend = getattr(backend, "_backend", None)
            capability_backend = (
                wrapped_backend
                if isinstance(wrapped_backend, QueueBackend)
                else backend
            )
            if getattr(capability_backend, "requires_ack", False) is True:
                raise QueueError(
                    "queue strategy returned an unbound deferred-ack token; custom "
                    "strategies must use QueueStrategy._pop_backend_with_ack() or "
                    "_pop_backend_instance_with_ack()",
                    queue_name=self.queue_name,
                    operation="pop",
                )
        # Emit on every pop call — ``queue/pop_attempt_count`` (R14-D rename) is
        # the consumer-liveness signal (pop attempts per second), independent of
        # whether an item was returned. A worker popping an empty queue is itself
        # operability signal.
        pop_monitor_failed = False
        try:
            self._monitor.on_pop(self.queue_name)
        except Exception:  # noqa: BLE001 - atomic backends already removed the item
            pop_monitor_failed = True
        if pop_monitor_failed:
            try:
                logger.debug("monitor.on_pop raised; ignored")
            except BaseException:
                pass
        # Additive wall-clock liveness primitive. Unlike the sampled pop-rate
        # gauge this epoch remains useful after events stop: an external observer
        # computes ``now - last_pop_epoch`` without a queue-owned timer thread.
        last_pop_monitor_failed = False
        try:
            self._monitor.on_last_pop_epoch(self._wall_clock())
        except Exception:  # noqa: BLE001 - telemetry cannot alter the pop result
            last_pop_monitor_failed = True
        if last_pop_monitor_failed:
            try:
                logger.debug("monitor.on_last_pop_epoch raised; ignored")
            except BaseException:
                pass
        # U2 operability: record this pop into the rolling window, then emit the
        # derived rate on the same sampling cadence as the depth probe below —
        # keeps the hot path O(1) amortized and avoids per-pop stat RPCs. A
        # monotonic clock is used so wall-clock skew can't corrupt the window.
        self._record_pop_timestamp()
        self._pop_rate_counter += 1
        if self._pop_rate_counter >= self.depth_sample_every:
            self._pop_rate_counter = 0
            pop_rate_monitor_failed = False
            try:
                self._emit_pop_rate()
            except Exception:  # noqa: BLE001
                pop_rate_monitor_failed = True
            if pop_rate_monitor_failed:
                try:
                    logger.debug("monitor.on_pop_rate raised; ignored")
                except BaseException:
                    pass
        # Sample depth after each pop — this is the backpressure signal (architect's
        # #1 operability gap). Cheaper than a periodic timer and aligns the sample
        # with an event that already touched the backend. U4: routed through
        # ``_probe_depth`` so the real ``queue_len`` RPC only fires once per
        # ``depth_sample_every`` pops; cached value fills the gaps. Guarded so a
        # depth-sampling failure can never break a successful pop.
        depth_monitor_failed = False
        try:
            self._monitor.on_queue_depth(self.queue_name, self._probe_depth())
        except Exception:  # noqa: BLE001
            depth_monitor_failed = True
        if depth_monitor_failed:
            try:
                logger.debug("monitor.on_queue_depth raised; ignored")
            except BaseException:
                pass

        if data is None:
            if ack_token is not None:
                try:
                    # Kafka tombstones (and equivalent broker-side empty deliveries) are
                    # real records with offsets/tokens. Treating them as an empty poll
                    # without a terminal transition pins the partition watermark forever.
                    self._ack(token=ack_token)
                except Exception:
                    try:
                        # Preserve redelivery when the terminal commit itself failed.
                        self._nack(token=ack_token)
                    except Exception:  # noqa: BLE001 - preserve the ack failure
                        try:
                            logger.error(
                                "Failed to nack empty payload after ack failure"
                            )
                        except BaseException:
                            pass
                    raise
                self._inc_stat("scheduler/queue/empty_payload_dropped")
            return None

        deserialization_failed = False
        try:
            if len(data) > self.max_item_bytes:
                self._inc_stat("scheduler/queue/oversize_dropped")
                raise ValueError(
                    f"Queued payload ({len(data)} bytes) exceeds max_item_bytes "
                    f"({self.max_item_bytes})"
                )
            decoded = self._serializer.deserialize(data)
            if not isinstance(decoded, dict):
                raise TypeError(
                    f"queued request must be a JSON object, got {type(decoded).__name__}"
                )
            request_dict = cast("dict[str, Any]", decoded)
            self._decode_body(request_dict)
            self._validate_request_dict(request_dict)
            request = self._request_from_dict(request_dict)
        except Exception:
            # The fixed error/poison handling below deliberately runs after this
            # suite. Custom stats and logging hooks must not recover a malformed
            # broker payload from the caught parser error.
            deserialization_failed = True
        if deserialization_failed:
            poison_terminated = ack_token is None
            malformed_ack_failed = False
            if ack_token is not None:
                try:
                    # Deserialization failures are deterministic for the same bytes. A
                    # nack would redeliver the identical corrupt payload forever and can
                    # pin Kafka partition progress or keep a broker queue permanently
                    # hot. The pop already owns an operation lease, so terminate the
                    # unrecoverable delivery inside that lease.
                    self._ack(token=ack_token)
                    poison_terminated = True
                except Exception:  # noqa: BLE001 - preserve the deserialize failure
                    malformed_ack_failed = True
            if malformed_ack_failed:
                try:
                    logger.error("Failed to acknowledge malformed payload")
                except BaseException:
                    pass
            if poison_terminated:
                self._inc_stat("scheduler/queue/poison_dropped")
            raise SerializationError(
                _QUEUE_POP_MONITOR_FAILURE,
                serializer="json",
            ) from None
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
        (#28). Every backend-delegating strategy overrides ``pop_with_ack`` and
        carries the token (correct under ``CONCURRENT_REQUESTS > 1``). The fully
        in-process round-robin and ring-buffer strategies inherit the ABC default
        ``(pop(), None)`` because they never pop a broker message.
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
        codec = request_dict.pop(_BODY_CODEC_FIELD, None)
        if codec not in {None, _BODY_CODEC_BASE64_V1}:
            raise SerializationError(
                f"Unsupported queued request body codec: {codec!r}",
                data=codec,
                serializer="json",
            )
        body = request_dict.get("body")
        if body is None:
            return
        legacy_bytes: bytes | None = None
        try:
            request_dict["body"] = base64.b64decode(body, validate=True)
        except (binascii.Error, ValueError):
            if codec == _BODY_CODEC_BASE64_V1:
                msg = "Invalid base64 body in queued request: body is not valid base64"
                raise SerializationError(msg, data=body, serializer="json") from None
            # D1: attempt legacy migration — pre-base64 bodies were raw UTF-8.
            if isinstance(body, str):
                try:
                    legacy_bytes = body.encode("utf-8")
                except UnicodeEncodeError:
                    pass
            if legacy_bytes is not None:
                pass
            else:
                msg = "Invalid base64 body in queued request: body is not valid base64"
                raise SerializationError(msg, data=body, serializer="json")
        if legacy_bytes is not None:
            warnings.warn(
                "legacy non-base64 queue body; will be unsupported after the "
                "next major. Re-queue the request with a current package version "
                "to migrate it.",
                DeprecationWarning,
                stacklevel=2,
            )
            request_dict["body"] = legacy_bytes

    @staticmethod
    def _validate_request_dict(request_dict: dict[str, Any]) -> None:
        """Reject wire-type drift before Scrapy silently coerces request fields."""

        def require_type(field: str, expected: type | tuple[type, ...]) -> None:
            if field in request_dict and not isinstance(request_dict[field], expected):
                expected_names = (
                    expected.__name__
                    if isinstance(expected, type)
                    else " or ".join(kind.__name__ for kind in expected)
                )
                raise TypeError(
                    f"queued request field {field!r} must be {expected_names}, "
                    f"got {type(request_dict[field]).__name__}"
                )

        require_type("url", str)
        require_type("method", str)
        require_type("encoding", str)
        require_type("headers", dict)
        require_type("meta", dict)
        require_type("cb_kwargs", dict)
        require_type("cookies", (dict, list))
        require_type("body", (bytes, type(None)))
        require_type("callback", (str, type(None)))
        require_type("errback", (str, type(None)))
        require_type("dont_filter", bool)
        require_type("flags", list)
        require_type("_class", str)
        # R26-D: dumps_kwargs is the lone JsonRequest-specific attribute Scrapy
        # round-trips through request_from_dict (.get(..., {}) then iterates it).
        # A crafted non-dict value passes the other checks and would surface as an
        # opaque AttributeError deep in deserialization; validate it here for a
        # clean TypeError naming the field. Gated on field presence, so ordinary
        # (non-JsonRequest) payloads are unaffected.
        require_type("dumps_kwargs", dict)

        flags = request_dict.get("flags")
        if isinstance(flags, list) and not all(isinstance(flag, str) for flag in flags):
            raise TypeError("queued request field 'flags' must contain only strings")

        headers = request_dict.get("headers")
        if isinstance(headers, dict):
            for name, values in headers.items():
                if not isinstance(name, str):
                    raise TypeError("queued request header names must be strings")
                if isinstance(values, (str, bytes)):
                    continue
                if not isinstance(values, list) or not all(
                    isinstance(value, (str, bytes)) for value in values
                ):
                    raise TypeError(
                        f"queued request header {name!r} must be text/bytes or a list of them"
                    )

        if "priority" in request_dict and request_dict["priority"] is not None:
            priority = request_dict["priority"]
            if (
                isinstance(priority, bool)
                or not isinstance(priority, (int, float))
                or not math.isfinite(priority)
                or not float(priority).is_integer()
            ):
                raise TypeError(
                    "queued request field 'priority' must be a finite integer"
                )
            # Legacy payloads may contain 0.0; normalize only after strict numeric
            # validation so strings and fractional values cannot change semantics.
            request_dict["priority"] = int(priority)

    def _request_from_dict(self, request_dict: dict[str, Any]) -> Request:
        """Rebuild an allowlisted Request class without queue-controlled imports."""
        class_path = request_dict.get("_class")
        if class_path is None:
            request_cls: type[Request] = Request
        else:
            resolved_class = _REQUEST_CLASSES.get(class_path)
            if resolved_class is None:
                raise ValueError(f"Unsupported queued request class: {class_path!r}")
            request_cls = resolved_class

        kwargs = {
            key: value
            for key, value in request_dict.items()
            if key in request_cls.attributes
        }
        for field in ("callback", "errback"):
            method_name = request_dict.get(field)
            if method_name and self._spider is not None:
                # R25-A: reject dunder names (__init__, __reduce__, __setstate__, ...)
                # before getattr. A crafted/migrated payload carrying callback='__init__'
                # would otherwise pass the __self__-is-spider guard — getattr(spider,
                # '__init__') is a bound method whose __self__ is the spider — and let
                # Scrapy dispatch spider.__init__(response), re-initializing the spider
                # and corrupting crawler state. Single-underscore private callbacks
                # (_cb) remain allowed. Defense-in-depth (requires queue write access).
                if isinstance(method_name, str) and method_name.startswith("__"):
                    raise ValueError(
                        f"Request {field} {method_name!r} must not be a dunder method "
                        f"of: {self._spider}"
                    )
                try:
                    method = getattr(self._spider, method_name)
                except AttributeError:
                    raise ValueError(
                        f"Method {method_name!r} not found in: {self._spider}"
                    ) from None
                if (
                    not callable(method)
                    or not inspect.ismethod(method)
                    or getattr(method, "__self__", None) is not self._spider
                ):
                    raise ValueError(
                        f"Request {field} {method_name!r} is not an instance method "
                        f"of: {self._spider}"
                    )
                kwargs[field] = method
        return request_cls(**kwargs)

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
            not window_open or self._depth_probe_counter >= self.depth_sample_every
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
        # contract. Backends that cannot query broker depth (currently Pulsar and
        # RocketMQ) deliberately raise ``NotImplementedError`` here; the scheduler
        # interprets that signal conservatively as pending work and still polls.
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
        self._begin_operation("len")
        try:
            return self._probe_depth()
        finally:
            self._end_operation()

    def clear(self) -> None:
        """Clear all requests from the queue."""
        self._begin_operation("clear")
        try:
            self._strategy.clear(self.queue_name)
            self._cached_depth = None
            self._depth_probe_counter = 0
        finally:
            self._end_operation()

    def _begin_operation(self, operation: str) -> None:
        """Admit one lifecycle-bound mutating operation."""
        with self._operation_gate:
            if not self._accepting_operations:
                raise QueueError(
                    "backend queue is closing or closed; operation rejected",
                    queue_name=self.queue_name,
                    operation=operation,
                )
            self._active_operations += 1

    def _end_operation(self) -> None:
        """Release one operation lease and wake a waiting close."""
        with self._operation_gate:
            self._active_operations -= 1
            if self._active_operations == 0:
                self._operation_gate.notify_all()

    def ack(self, *, token: Any | None = None) -> None:
        """Acknowledge the popped request identified by ``token``.

        Atomic backends (Redis, MongoDB, ElasticSearch) implement this as a no-op.
        Deferred-ack backends (Kafka, RabbitMQ, RocketMQ, Pulsar, SQS) commit the
        offset or acknowledge the delivery so the message is not re-delivered.

        When ``token`` is provided (read from
        ``request.meta["_backend_ack_token"]`` by the scheduler), the backend
        acks the *specific* message — correct under
        ``CONCURRENT_REQUESTS > 1``. When ``None``, the backend acks its
        last-popped message (legacy single-slot path).

        Args:
            token: Opaque, issuer-bound ack token from ``BackendQueue.pop``'s meta
                injection, or ``None``.
        """
        self._begin_operation("ack")
        try:
            self._ack(token=token)
        finally:
            self._end_operation()

    def _ack(self, *, token: Any | None = None) -> None:
        """Execute an already-admitted acknowledgement."""
        if isinstance(token, _QueueAckToken):
            token.ack()
            return
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
        self._begin_operation("nack")
        try:
            self._nack(token=token)
        finally:
            self._end_operation()

    def _nack(self, *, token: Any | None = None) -> None:
        """Execute an already-admitted negative acknowledgement."""
        if isinstance(token, _QueueAckToken):
            token.nack()
            return
        backend = self.connection_manager.get_queue_backend()
        if token is not None:
            backend.nack(self.queue_name, token=token)
        else:
            backend.nack(self.queue_name)

    def _terminate_invalid_replacement(
        self,
        request: Request,
        token: Any | None,
    ) -> None:
        """Drop a deterministic-invalid replacement's consumed broker delivery."""
        if token is None:
            return
        acknowledgement_failed = False
        try:
            self._ack(token=token)
        except Exception:  # noqa: BLE001 - preserve the local validation error
            # Keep the diagnostic outside this exception suite so a logging handler
            # cannot inspect the acknowledgement failure through ``sys.exc_info()``.
            acknowledgement_failed = True
        if acknowledgement_failed:
            try:
                logger.error("Failed to acknowledge invalid replacement")
            except BaseException:
                pass
            return
        request.meta.pop(BACKEND_ACK_TOKEN_META_KEY, None)
        self._inc_stat("scheduler/queue/replacement_poison_dropped")

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
        stats_failed = False
        if stats is not None:
            try:
                stats.inc_value(stat_name)
            except Exception:  # noqa: BLE001 - stats cannot mask the queue result
                stats_failed = True
        if stats_failed:
            try:
                logger.debug("stats.inc_value raised; ignored")
            except BaseException:
                pass

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

    def _cleanup_close_waiter_locked(self, attempt: int, waiter_token: object) -> None:
        """Idempotently reclaim one waiter and its consumed outcome."""
        waiters = self._close_attempt_waiters.get(attempt)
        if waiters is not None:
            waiters.discard(waiter_token)
        if not waiters:
            self._close_attempt_waiters.pop(attempt, None)
            self._close_attempt_outcomes.pop(attempt, None)
            self._close_attempt_terminal.pop(attempt, None)

    def _wait_for_close_attempt_locked(self, attempt: int) -> None:
        """Register, await, and reclaim one close-attempt observation."""
        waiter_token = object()
        failure: BaseException | None = None
        observed_success = False
        observed_terminal = False
        try:
            self._close_attempt_waiters.setdefault(attempt, set()).add(waiter_token)
            while attempt not in self._close_attempt_outcomes:
                owner = self._close_owner_token
                if owner is None or not owner.active:
                    observed_terminal = (
                        self._strategy_cleanup_state != _STRATEGY_CLEANUP_NOT_STARTED
                    )
                    break
                # Periodically re-check liveness so an interrupted owner cannot
                # strand peers without a notification publication.
                self._operation_gate.wait(timeout=0.05)
            else:
                observed_success = self._close_attempt_outcomes[attempt]
                observed_terminal = self._close_attempt_terminal.get(attempt, False)
        except BaseException as exc:
            failure = exc
        finally:
            self._cleanup_close_waiter_locked(attempt, waiter_token)
        if failure is not None:
            raise failure
        if not observed_success:
            if observed_terminal:
                raise QueueError(
                    "Queue strategy cleanup failed or was interrupted; close is terminal."
                )
            raise QueueError("Queue close failed; checkpoint can be retried.")

    def _publish_strategy_cleanup_outcome(self, outcome: str) -> None:
        """Replace ``started`` with one terminal, idempotent outcome."""
        if self._strategy_cleanup_state == _STRATEGY_CLEANUP_STARTED:
            self._strategy_cleanup_state = outcome

    def _publish_close_attempt(
        self,
        attempt: int,
        owner_token: _CloseOwnerToken,
        *,
        succeeded: bool,
    ) -> None:
        """Publish one idempotent terminal attempt outcome in a bounded pass."""
        with self._operation_gate:
            owns_attempt = self._close_owner_token is owner_token
            cleanup_started = (
                self._strategy_cleanup_state != _STRATEGY_CLEANUP_NOT_STARTED
            )
            if owns_attempt and (succeeded or cleanup_started):
                self._close_complete = True
            waiters = self._close_attempt_waiters.get(attempt)
            if waiters:
                self._close_attempt_outcomes[attempt] = succeeded
                self._close_attempt_terminal[attempt] = cleanup_started
            else:
                self._close_attempt_waiters.pop(attempt, None)
                self._close_attempt_outcomes.pop(attempt, None)
                self._close_attempt_terminal.pop(attempt, None)
            self._operation_gate.notify_all()
            if owns_attempt:
                self._close_in_progress = False
                self._close_owner_token = None

    def _repair_close_finalization(
        self,
        attempt: int,
        owner_token: _CloseOwnerToken,
        *,
        succeeded: bool,
    ) -> None:
        """Make one bounded idempotent finalization pass.

        An interruption may leave package state retryable. A later close reclaims
        inactive ownership and never replays destructive strategy cleanup.
        """
        if self._strategy_cleanup_state == _STRATEGY_CLEANUP_STARTED:
            self._publish_strategy_cleanup_outcome(_STRATEGY_CLEANUP_INDETERMINATE)
        if self._close_owner_token is owner_token:
            self._publish_close_attempt(attempt, owner_token, succeeded=succeeded)

    def close(self, *, lossy: bool = False) -> None:
        """Transactionally checkpoint and close the strategy.

        A checkpoint failure leaves strategy state and both managers usable for a
        later close retry. Callers waiting on the same attempt observe a fresh,
        redacted failure rather than a false success. ``lossy=True`` is the explicit
        abort path for discarding nonempty state when no durable checkpoint can be
        made.
        """
        owner_token = _CloseOwnerToken()
        attempt = 0
        failure: BaseException | None = None
        publication_failure: BaseException | None = None
        cleanup_publication_failure: BaseException | None = None
        finalization_interruption: BaseException | None = None
        succeeded = False
        try:
            try:
                with self._operation_gate:
                    if self._close_complete:
                        return
                    if self._close_in_progress:
                        current_owner = self._close_owner_token
                        if current_owner is not None and current_owner.active:
                            if current_owner.thread_id == owner_token.thread_id:
                                raise QueueError("Queue close is already in progress.")
                            self._wait_for_close_attempt_locked(self._close_attempt)
                            return
                        # The prior frame unwound before publication. Reclaim only
                        # package ownership; opaque cleanup is never replayed.
                        self._close_in_progress = False
                        self._close_owner_token = None
                        cleanup_state = self._strategy_cleanup_state
                        if cleanup_state != _STRATEGY_CLEANUP_NOT_STARTED:
                            self._close_complete = True
                            self._operation_gate.notify_all()
                            if cleanup_state == _STRATEGY_CLEANUP_SUCCEEDED:
                                return
                            raise QueueError(
                                "Queue strategy cleanup failed or was interrupted; "
                                "close is terminal."
                            )
                    self._close_attempt += 1
                    attempt = self._close_attempt
                    self._close_owner_token = owner_token
                    self._close_in_progress = True
                    self._accepting_operations = False

                if not self._begin_close_complete:
                    self._strategy.begin_close()
                    self._begin_close_complete = True
                with self._operation_gate:
                    while self._active_operations > 0:
                        self._operation_gate.wait()
                if not self._checkpoint_complete:
                    if not lossy:
                        self._persist_snapshot()
                    self._checkpoint_complete = True

                # Mark destructive cleanup before invocation. After this write no
                # close or lossy retry may call strategy.close() again: it may have
                # raised after mutation, or process control may have landed after
                # the call returned but before Python could observe that success.
                self._strategy_cleanup_state = _STRATEGY_CLEANUP_STARTED
                cleanup_outcome = _STRATEGY_CLEANUP_INDETERMINATE
                cleanup_failure: BaseException | None = None
                try:
                    self._strategy.close()
                except Exception as exc:
                    cleanup_failure = exc
                    cleanup_outcome = _STRATEGY_CLEANUP_FAILED
                except BaseException as exc:
                    cleanup_failure = exc
                else:
                    cleanup_outcome = _STRATEGY_CLEANUP_SUCCEEDED
                finally:
                    try:
                        self._publish_strategy_cleanup_outcome(cleanup_outcome)
                    except BaseException as exc:
                        cleanup_publication_failure = exc
                if cleanup_failure is not None:
                    raise cleanup_failure
                if cleanup_publication_failure is not None:
                    raise cleanup_publication_failure
                succeeded = True
            except BaseException as exc:
                failure = exc
        finally:
            # One bounded package-state pass. If it is interrupted, the invocation
            # token becomes inactive on unwind and a later close can reclaim it.
            try:
                self._repair_close_finalization(
                    attempt, owner_token, succeeded=succeeded
                )
            except BaseException as exc:
                finalization_interruption = exc

        # Preserve the originating control-flow exception ahead of errors raised
        # while repairing it.  Otherwise a control-flow interruption encountered by
        # finalization is selected ahead of ordinary operation/publication failures.
        if failure is not None and not isinstance(failure, Exception):
            raise failure
        if finalization_interruption is not None and not isinstance(
            finalization_interruption, Exception
        ):
            raise finalization_interruption
        if cleanup_publication_failure is not None:
            raise cleanup_publication_failure
        if publication_failure is not None:
            raise publication_failure
        if finalization_interruption is not None:
            raise finalization_interruption
        if failure is not None:
            raise failure

    #: Storage-key prefix for strategy snapshots (initiative #3). The default
    #: v3 identity length-prefixes both spider and queue components so valid
    #: colon-bearing names cannot collide. A configured stable snapshot owner
    #: keeps its existing v2 identity for compatibility. Legacy fallback is only
    #: safe for unscoped names without ``:``; a named old key can also name an
    #: unscoped queue. No TTL: the snapshot is cheap to overwrite and represents
    #: last-shutdown state.
    _SNAPSHOT_KEY_PREFIX = "queue:snapshot:"
    _SNAPSHOT_TOMBSTONE_KEY_PREFIX = "queue:snapshot-tombstone:"

    def _snapshot_key(self) -> str:
        """Build the storage key for this queue's strategy snapshot.

        Includes the spider name when available so different spiders do not share
        state. When ``snapshot_owner`` is configured, preserves the existing v2
        worker identity. Otherwise v3 length-prefixes both logical components so
        valid ``:`` characters cannot make distinct spider/queue pairs collide.
        """
        snapshot_key = ""
        spider_component = ""
        owner = ""
        try:
            spider_name = getattr(self._spider, "name", None)
            if self._snapshot_owner is not None:
                owner = self._snapshot_owner
                spider_component = str(spider_name) if spider_name else ""
                snapshot_key = (
                    f"{self._SNAPSHOT_KEY_PREFIX}v2:{len(owner)}:{owner}:"
                    f"{len(spider_component)}:{spider_component}:{self.queue_name}"
                )
                return snapshot_key
            spider_component = str(spider_name) if spider_name else ""
            snapshot_key = (
                f"{self._SNAPSHOT_KEY_PREFIX}v3:{len(spider_component)}:"
                f"{spider_component}:{len(self.queue_name)}:{self.queue_name}"
            )
            return snapshot_key
        finally:
            snapshot_key = ""
            spider_component = ""
            owner = ""

    def _legacy_snapshot_key(self) -> str | None:
        """Return the only safely attributable pre-v3 key for compatibility.

        A named legacy identity ``<spider>:<queue>`` can also be the unscoped
        queue name ``<spider>:<queue>``. The old blob contains no owner metadata,
        so no named identity can prove that it owns the key. An unscoped name is
        unique only when it contains no ``:``. Leave all other legacy values
        untouched rather than loading or deleting another queue's checkpoint.
        """
        legacy_key = ""
        try:
            if self._snapshot_owner is not None:
                return None
            spider_name = getattr(self._spider, "name", None)
            if spider_name or ":" in self.queue_name:
                return None
            legacy_key = f"{self._SNAPSHOT_KEY_PREFIX}{self.queue_name}"
            return legacy_key
        finally:
            legacy_key = ""

    def _empty_snapshot_tombstone_key(self) -> str:
        """Build the private marker key for an empty legacy-migration transition."""
        snapshot_key = ""
        snapshot_identity = ""
        tombstone_key = ""
        try:
            snapshot_key = self._snapshot_key()
            snapshot_identity = snapshot_key.removeprefix(self._SNAPSHOT_KEY_PREFIX)
            snapshot_key = ""
            tombstone_key = f"{self._SNAPSHOT_TOMBSTONE_KEY_PREFIX}{snapshot_identity}"
            return tombstone_key
        finally:
            snapshot_key = ""
            snapshot_identity = ""
            tombstone_key = ""

    def _snapshot_storage(self, *, strict: bool = False) -> Any | None:
        """Resolve snapshot storage, optionally surfacing retryable failures."""
        manager = (
            self._snapshot_connection_manager
            if self._snapshot_connection_manager is not None
            else self.connection_manager
        )
        get_storage = getattr(manager, "get_storage_backend", None)
        if get_storage is None:
            return None
        unsupported = False
        resolution_failed = False
        try:
            storage = get_storage()
        except NotImplementedError:
            unsupported = True
        except Exception:
            resolution_failed = True
        if unsupported:
            try:
                logger.info("Strategy snapshot storage is not available")
            except BaseException:
                pass
            return None
        if resolution_failed:
            try:
                logger.error("Failed to resolve strategy snapshot storage")
            except BaseException:
                pass
            if strict:
                raise QueueError("Strategy snapshot storage is unavailable.") from None
            return None
        return storage

    def _snapshot_repository(self, storage: Any) -> SnapshotRepository:
        return SnapshotRepository(
            storage,
            max_bytes=self._snapshot_max_bytes,
            chunk_bytes=self._snapshot_chunk_bytes,
        )

    def _persist_snapshot(self) -> None:
        """Commit a chunked strategy snapshot or raise a redacted retryable error."""
        state: bytes | None = None
        snapshot_key = ""
        legacy_key: str | None = None
        tombstone_key = ""
        try:
            snapshot_failed = False
            try:
                state = self._strategy.snapshot()
            except Exception:
                snapshot_failed = True
                state = None
            if snapshot_failed:
                try:
                    logger.error("Strategy snapshot creation failed")
                except BaseException:
                    pass
                raise QueueError("Strategy snapshot creation failed.") from None

            storage = self._snapshot_storage(strict=True)
            if storage is None:
                if state is None and not self._defer_legacy_retirement:
                    return
                if state is not None:
                    raise QueueError(
                        "Nonempty strategy state requires snapshot storage."
                    )
                raise QueueError("Strategy snapshot storage is unavailable.")
            repository = self._snapshot_repository(storage)
            legacy_key = self._legacy_snapshot_key()

            # If startup could not read a legacy checkpoint and this lifecycle has
            # no replacement state, publishing an empty current manifest would make
            # the preserved legacy value unreachable forever. Keep the current key
            # absent and remove the old empty-state tombstone so a later restart can
            # retry the legacy read. Failure is retryable and prevents destructive
            # strategy cleanup.
            if self._defer_legacy_retirement and state is None:
                cleanup_failed = False
                try:
                    tombstone_key = self._empty_snapshot_tombstone_key()
                    storage.delete(tombstone_key)
                    tombstone_key = ""
                except Exception:
                    cleanup_failed = True
                if cleanup_failed:
                    try:
                        logger.error(
                            "Failed to preserve unread legacy strategy snapshot"
                        )
                    except BaseException:
                        pass
                    raise QueueError(
                        "Unread legacy strategy snapshot cannot be preserved."
                    ) from None
                self._defer_legacy_retirement = False
                return

            commit_failed = False
            try:
                snapshot_key = self._snapshot_key()
                repository.commit(snapshot_key, state)
                snapshot_key = ""
            except SnapshotRepositoryError:
                commit_failed = True
            if commit_failed:
                try:
                    logger.error("Strategy snapshot commit failed")
                except BaseException:
                    pass
                raise QueueError("Strategy snapshot commit failed.")

            # A committed manifest, including an empty manifest, is authoritative.
            # Legacy cleanup therefore happens only after the manifest-last commit.
            self._defer_legacy_retirement = False
            if legacy_key is None:
                return
            cleanup_failed = False
            try:
                storage.delete(legacy_key)
                legacy_key = None
                tombstone_key = self._empty_snapshot_tombstone_key()
                storage.delete(tombstone_key)
                tombstone_key = ""
            except Exception:
                cleanup_failed = True
            if cleanup_failed:
                try:
                    logger.error("Failed to retire legacy strategy snapshot")
                except BaseException:
                    pass
        finally:
            state = None
            snapshot_key = ""
            legacy_key = None
            tombstone_key = ""

    def _restore_snapshot(self) -> None:
        """Restore a validated v6/v5/v4 manifest or compatible raw value."""
        storage = self._snapshot_storage()
        if storage is None:
            return
        repository = self._snapshot_repository(storage)
        result = None
        tombstone: object = None
        state: bytes | None = None
        snapshot_key = ""
        legacy_key: str | None = None
        tombstone_key = ""
        try:
            read_failed = False
            try:
                snapshot_key = self._snapshot_key()
                result = repository.read(snapshot_key)
                snapshot_key = ""
            except SnapshotRepositoryError:
                read_failed = True
            if read_failed:
                try:
                    logger.error("Failed to read strategy snapshot; starting clean")
                except BaseException:
                    pass
                return
            assert result is not None

            if not result.found:
                legacy_key = self._legacy_snapshot_key()
                if legacy_key is not None:
                    tombstone_failed = False
                    try:
                        tombstone_key = self._empty_snapshot_tombstone_key()
                        tombstone = storage.retrieve(tombstone_key)
                        tombstone_key = ""
                    except Exception:
                        tombstone_failed = True
                    if tombstone_failed:
                        try:
                            logger.error(
                                "Failed to retrieve empty strategy snapshot tombstone; "
                                "checking legacy checkpoint"
                            )
                        except BaseException:
                            pass
                    elif tombstone is not None:
                        return
                    legacy_read_failed = False
                    try:
                        result = repository.read(legacy_key)
                    except SnapshotRepositoryError:
                        legacy_read_failed = True
                    if legacy_read_failed:
                        self._defer_legacy_retirement = True
                        try:
                            logger.error(
                                "Failed to read legacy strategy snapshot; starting clean"
                            )
                        except BaseException:
                            pass
                        return
            if not result.found or result.state is None:
                return
            state = result.state
            result = None
            tombstone = None
            restore_failed = False
            try:
                self._strategy.restore(state)
            except Exception:
                restore_failed = True
            if restore_failed:
                try:
                    logger.error("Strategy snapshot restore failed; starting clean")
                except BaseException:
                    pass
        finally:
            result = None
            state = None
            tombstone = None
            snapshot_key = ""
            legacy_key = None
            tombstone_key = ""
