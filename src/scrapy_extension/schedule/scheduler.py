"""Scheduler component for scrapy-extension.

This module provides a Scrapy scheduler component using backend queue
and duplicate filter interfaces.
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from collections.abc import AsyncIterable, Callable, Iterable, Mapping, MutableMapping
from inspect import getattr_static, isawaitable
from typing import TYPE_CHECKING, Any, cast

from pydispatch.errors import DispatcherKeyError
from scrapy import signals
from scrapy.http import Request
from scrapy.utils.misc import load_object
from twisted.internet.defer import Deferred, DeferredList, fail
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure as TwistedFailure

from scrapy_extension.backends.base import BackendType, _validate_key_name
from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.backends.connectors import (
    _CONNECTION_MANAGER_SCOPE_KEY,
    _CONSUMER_SCOPED_BACKENDS,
    ConnectionManager,
    ConnectionManagerLease,
    release_manager_acquire,
    resolve_backend_config,
)
from scrapy_extension.backends.registry import has_capability
from scrapy_extension.exceptions import (
    BackendConnectionError,
    BackendError,
    BackendOperationTimeout,
    ConfigurationError,
    QueueError,
    SerializationError,
)
from scrapy_extension.queue.queue import BACKEND_ACK_TOKEN_META_KEY, BackendQueue
from scrapy_extension.queue.snapshot import (
    DEFAULT_SNAPSHOT_CHUNK_BYTES,
    DEFAULT_SNAPSHOT_MAX_BYTES,
)
from scrapy_extension.queue.strategies.base import _QueueAckToken
from scrapy_extension.utils._config import (
    get_bool_setting,
    parse_int_setting,
)
from scrapy_extension.utils.identity import (
    DEFAULT_PROJECT_NAME,
    DEFAULT_QUEUE_KEY_TEMPLATE,
    project_name_from_spider,
    resolve_identity_template,
)
from scrapy_extension.utils.reactor import (
    DEFAULT_REACTOR_IO_TIMEOUT_S,
    bounded_deferred,
    defer_to_thread_ordered,
    reactor_is_running,
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

from scrapy_extension.schedule._dupefilter_compat import (
    _MISSING_STATIC_ATTRIBUTE as _MISSING_STATIC_ATTRIBUTE,
)
from scrapy_extension.schedule._dupefilter_compat import (
    _atomic_dupefilter_methods as _atomic_dupefilter_methods,
)
from scrapy_extension.schedule._dupefilter_compat import (
    _backend_dupefilter_lifecycle as _backend_dupefilter_lifecycle,
)
from scrapy_extension.schedule._dupefilter_compat import (
    _call_dupefilter_open as _call_dupefilter_open,
)
from scrapy_extension.schedule._dupefilter_compat import (
    _static_declaration_rank as _static_declaration_rank,
)
from scrapy_extension.schedule._lifecycle import (
    _chain_lifecycle_result as _chain_lifecycle_result,
)
from scrapy_extension.schedule._lifecycle import (
    _DeferredLifecycleResult as _DeferredLifecycleResult,
)
from scrapy_extension.schedule._lifecycle import (
    _lifecycle_operation as _lifecycle_operation,
)
from scrapy_extension.schedule._lifecycle import (
    _LifecycleContinuation as _LifecycleContinuation,
)
from scrapy_extension.schedule._lifecycle import (
    _ResponseSignalReceiver as _ResponseSignalReceiver,
)
from scrapy_extension.schedule._lifecycle import (
    _SchedulerAttemptToken as _SchedulerAttemptToken,
)
from scrapy_extension.schedule._lifecycle import (
    _SignalLease as _SignalLease,
)
from scrapy_extension.schedule._lifecycle import (
    _SignalReceiver as _SignalReceiver,
)
from scrapy_extension.schedule._lifecycle import (
    _SpiderErrorSignalReceiver as _SpiderErrorSignalReceiver,
)
from scrapy_extension.schedule._queue_config import (
    _QueueComponentConfig as _QueueComponentConfig,
)

logger = logging.getLogger(__name__)


def _submit_thread(
    function: Callable[..., Any], *args: Any, **kwargs: Any
) -> Deferred[Any]:
    """Submit one worker without leaking a synchronous adapter failure.

    ``deferToThread`` is a lifecycle boundary, not an infallible constructor.
    Returning an already-failed Deferred lets the surrounding lifecycle chain
    perform its normal rollback and leaves a later close/open attempt retryable.
    """
    try:
        return deferToThread(function, *args, **kwargs)
    except BaseException as exc:
        failed: Deferred[Any] = Deferred()
        failed._reactor_submission_failure = "thread"  # type: ignore[attr-defined]
        failed.errback(exc)
        return failed


def _emit_diagnostic(method: Any, message: str, *args: Any) -> None:
    """Emit a static post-unwind diagnostic without changing control flow."""
    try:
        method(message, *args)
    except BaseException:
        pass


_LIFECYCLE_NEW = "new"

_LIFECYCLE_OPENING = "opening"

_LIFECYCLE_OPEN = "open"

_LIFECYCLE_CLOSING = "closing"

_LIFECYCLE_CLOSED = "closed"

_EnqueueDiagnostic = tuple[str, str, str | None]


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
        operation_context = getattr(queue, "_operation_context", None)
        if isinstance(operation_context, threading.local):
            return queue._push_with_durability(
                request,
                priority=priority,
                _preserve_post_commit_marker=True,
            )
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
        self._settlement_in_progress = False
        self._settlement_operation: Deferred[Any] | None = None

    def _settle_source(self, *, negative: bool) -> None:
        """Settle outside the group lock and retain async authority until done."""
        if reactor_is_running():
            try:
                ordered = self._scheduler._settle_token_async_ordered(
                    self._source_token,
                    negative=negative,
                    log_message=(
                        "Failed to nack source after errback output failure"
                        if negative
                        else "Failed to ack source message after errback replacements committed"
                    ),
                )
            except BaseException:
                with self._lock:
                    self._settlement_in_progress = False
                raise
            if ordered is None:
                with self._lock:
                    self._settlement_in_progress = False
                return
            operation, _bounded = ordered
            with self._lock:
                self._settlement_operation = operation

            def settled_success(_value: Any) -> Any:
                with self._lock:
                    if self._settlement_operation is operation:
                        self._settlement_operation = None
                        self._settlement_in_progress = False
                        if not negative:
                            self._pending.clear()
                            self._terminal = True
                return _value

            def settled_failure(failure: Any) -> Any:
                with self._lock:
                    if self._settlement_operation is operation:
                        self._settlement_operation = None
                        self._settlement_in_progress = False
                return failure

            # The adapter may return an already-fired Deferred. No group mutex is
            # held while registering callbacks that can run synchronously.
            operation.addCallbacks(settled_success, settled_failure)
            operation.addErrback(lambda _failure: None)
            return

        try:
            settled = (
                self._scheduler._nack_token(
                    self._source_token,
                    log_message="Failed to nack source after errback output failure",
                )
                if negative
                else self._scheduler._ack_token(
                    self._source_token,
                    log_message=(
                        "Failed to ack source message after errback replacements committed"
                    ),
                )
            )
        except BaseException:
            with self._lock:
                self._settlement_in_progress = False
            raise
        with self._lock:
            self._settlement_in_progress = False
            if settled and not negative:
                self._pending.clear()
                self._terminal = True

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
        settle = False
        with self._lock:
            if self._terminal:
                return
            self._sealed = True
            if not self._pending and not self._settlement_in_progress:
                self._settlement_in_progress = True
                settle = True
        if settle:
            self._settle_source(negative=False)

    def accept(self, child_id: int) -> None:
        """Mark one replacement committed and settle a sealed final child."""
        settle = False
        with self._lock:
            if self._terminal or child_id not in self._pending:
                return
            if not self._sealed or len(self._pending) > 1:
                self._pending.remove(child_id)
                return
            if not self._settlement_in_progress:
                self._settlement_in_progress = True
                settle = True
        if settle:
            self._settle_source(negative=False)

    def abort(self) -> None:
        """Nack a source whose replacement output failed during enumeration."""
        with self._lock:
            if self._terminal:
                return
            # An ACK/NACK worker already owns the source transition. It cannot be
            # cancelled safely; do not issue a second broker transition from this
            # abort path. A late failure leaves the source available for redelivery.
            if self._settlement_in_progress:
                self._pending.clear()
                self._terminal = True
                return
            # Even if the broker call fails, visibility timeout/redelivery is the
            # only safe recovery. Never let late child commits turn this into an ack.
            self._pending.clear()
            self._terminal = True
            self._settlement_in_progress = True
        self._settle_source(negative=True)


class _DeferredReplacementAckToken(_QueueAckToken):
    """One idempotent child completion in a deferred source-ack group."""

    __slots__ = ("_child_id", "_group")
    _reactor_safe_settlement = True

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
            # The replacement owns another live delivery, so source settlement is
            # already decided: nack it before emitting best-effort diagnostics.
            # A logging handler or stats collector can raise BaseException; neither
            # may leave the deferred source group pending and later ackable.
            group.abort()
            try:
                logger.error(
                    "Errback replacement already carries a different backend ack token; "
                    "nacking the source instead of overwriting either delivery"
                )
            except BaseException:
                pass
            if self.scheduler.stats:
                try:
                    self.scheduler.stats.inc_value("scheduler/ack_transfer_conflict")
                except BaseException:
                    pass
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
        queue_key: str = DEFAULT_QUEUE_KEY_TEMPLATE,
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
        queue_snapshot_max_bytes: int = DEFAULT_SNAPSHOT_MAX_BYTES,
        queue_snapshot_chunk_bytes: int = DEFAULT_SNAPSHOT_CHUNK_BYTES,
        project_name: str | None = None,
        allow_cross_spider: bool = False,
        snapshot_connection_manager: ConnectionManager | None = None,
        owns_snapshot_connection_manager: bool = False,
        owns_connection_manager: bool = True,
        connection_manager_lease: ConnectionManagerLease | None = None,
        snapshot_connection_manager_lease: ConnectionManagerLease | None = None,
        reactor_io_timeout: float = DEFAULT_REACTOR_IO_TIMEOUT_S,
    ) -> None:
        """Initialize the scheduler.

        Args:
            connection_manager: Connection manager for backend access.
            queue_key: Key/template for the request queue. The default is
                ``scheduler-queue:{project}:{spider}``; literal legacy/shared keys
                remain available by explicit configuration.
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
                (seconds) for the ``queue/pop_rate_1m`` gauge. Forwarded to both
                ``BackendQueue(pop_rate_window_s=…)`` and the resolved monitor
                in ``open()``. Default ``60.0`` (U2).
            queue_snapshot_owner: Stable per-worker identity for isolating local
                strategy snapshots. ``None`` preserves the legacy single-worker
                key shape.
            project_name: Optional project identity used by ``{project}`` key
                templates. Defaults to Scrapy's ``BOT_NAME`` or ``default``.
            allow_cross_spider: Explicitly disable request-envelope identity
                fencing for a deliberately shared/legacy queue.
            snapshot_connection_manager: Optional storage-capable manager used
                exclusively by ``BackendQueue`` snapshot restore/persist. The
                queue's normal operations continue to use ``connection_manager``.
            owns_snapshot_connection_manager: Whether :meth:`close` releases
                ``snapshot_connection_manager`` after closing the queue. Factory
                callers that acquire it set this; direct callers retain ownership
                by leaving it false.
            owns_connection_manager: Whether :meth:`close` releases the supplied
                manager. Defaults to True for standalone schedulers; composite
                owners can pass False and release their shared acquire after the
                scheduler has quiesced its queue and signals.
            reactor_io_timeout: Caller-visible wait budget for Deferred lifecycle,
                queue-close, and acknowledgement adapters. Synchronous scheduler
                methods remain synchronous because Scrapy requires their return types.
        """
        self.connection_manager = connection_manager
        self._queue_key_template = queue_key
        self.queue_key = queue_key
        self._project_name = project_name or DEFAULT_PROJECT_NAME
        self._allow_cross_spider = allow_cross_spider
        self.stats = stats
        self.dupefilter = dupefilter
        self._owns_dupefilter: bool = dupefilter is not None
        self._dupefilter_open: bool = False
        self._dupefilter_released: bool = False
        self._queue_strategy = queue_strategy
        self._queue: BackendQueue | None = None
        self._spider: Spider | None = None
        self._signals_connected: bool = False
        self._connected_signals = None
        # ``None`` retains compatibility with schedulers constructed by older
        # callers/tests that set only ``_connected_signals``. Once this scheduler
        # successfully registers a handler, the list becomes the authoritative
        # ownership record, including during a partial-registration rollback.
        self._connected_ack_signal_handlers: list[tuple[Any, Any]] | None = None
        self._signal_leases: list[_SignalLease] = []
        self._manager_released: bool = False
        self._owns_connection_manager = owns_connection_manager
        self._connection_manager_lease = connection_manager_lease
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
        self._queue_snapshot_max_bytes = queue_snapshot_max_bytes
        self._queue_snapshot_chunk_bytes = queue_snapshot_chunk_bytes
        self._reactor_io_timeout = reactor_io_timeout
        self._snapshot_connection_manager = snapshot_connection_manager
        self._owns_snapshot_connection_manager = owns_snapshot_connection_manager
        self._snapshot_connection_manager_lease = snapshot_connection_manager_lease
        self._snapshot_manager_released = False
        self._queue_terminal = False
        self._terminal_queue_error: BaseException | None = None
        self._dupefilter_release_owner = object()
        if dupefilter is not None:
            if _backend_dupefilter_lifecycle(dupefilter) is not None:
                dupefilter._authorize_release_owner_alias(  # noqa: SLF001
                    self._dupefilter_release_owner
                )
        # A scheduler owns one ConnectionManager acquire and is therefore a
        # single-lifecycle object. Serializing open/close prevents concurrent
        # callers from replacing a live queue or releasing its manager midway
        # through construction.
        self._lifecycle_lock = threading.RLock()
        self._lifecycle_state = _LIFECYCLE_NEW
        # Opening has its own authoritative worker. A public timeout only ends
        # the bounded view; close must still fence that worker before teardown.
        self._opening_operation: Deferred[Any] | None = None
        self._opening_settled = True
        self._opening_failure: Any | None = None
        self._open_close_requested = False
        self._close_attempt_owner: _SchedulerAttemptToken | None = None
        self._close_attempt_thread_id: int | None = None
        # The worker/lifecycle Deferred remains authoritative after the bounded
        # close view times out. Composite owners use it to avoid releasing shared
        # resources while the scheduler is still finishing teardown.
        self._close_completion_deferred: Deferred[Any] | None = None
        self._close_retain_authoritative_failure = False
        # Replacement source settlements retain their authoritative worker
        # Deferred until the broker call completes. Close waits on this generation's
        # set before releasing the queue manager.
        self._settlement_lock = threading.Lock()
        self._pending_settlements: set[Deferred[Any]] = set()

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        spider_name: str | None = None,
        project_name: str | None = None,
        queue_key: str | None = None,
        connection_manager: ConnectionManager | None = None,
        backend_type_override: str | None = None,
        owns_connection_manager: bool | None = None,
        stats: StatsCollector | None = None,
        dupefilter: Any | None = None,
        snapshot_connection_manager: ConnectionManager | None = None,
        snapshot_connection_manager_lease: ConnectionManagerLease | None = None,
        owns_snapshot_connection_manager: bool | None = None,
    ) -> BackendScheduler:
        """Create scheduler from Scrapy settings.

        Selects the queue strategy from ``SCRAPY_QUEUE_STRATEGY`` (default
        ``passthrough``). The delay strategy reads ``SCRAPY_QUEUE_DELAY_DEFAULT``.
        Fan-out strategies (``priority``/``work_stealing``) read
        ``SCRAPY_QUEUE_NAME_GENERATION`` (default ``v2``; ``legacy_v1`` is a
        migration-only quiescent backlog-drain mode, never dual-read).

        Backend selection: ``SCRAPY_QUEUE_BACKEND_TYPE`` /
        ``SCRAPY_QUEUE_BACKEND_SETTINGS`` override the global
        ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so the queue can
        bind to a different backend than the dedup filter or storage pipeline
        (multi-backend coexistence). Unset → falls back to the global keys.
        For ``delay``, ``round_robin``, ``time_wheel``, and ``ring_buffer`` on
        a queue-only backend, the configured ``SCRAPY_STORAGE_BACKEND_*``
        component is acquired separately for clean-close snapshots; queue
        delivery itself remains bound to the queue manager.

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

        **Strategy+MQ ack-bypass warning.** After the queue strategy is resolved,
        the scheduler warns only if an ack-requiring backend is paired with a
        strategy that does not override ``pop_with_ack``. Built-in strategies that
        thread broker tokens through their override do not warn; the gate remains a
        defensive diagnostic for custom or local-only strategies. See
        ``_warn_strategy_mq_ack_bypass``.
        """
        from scrapy_extension.queue.strategies.factory import (
            QueueStrategyType,
            build_queue_strategy,
        )

        queue_config = _QueueComponentConfig.from_early_settings(settings)

        backend_type, backend_settings = resolve_backend_config(
            settings,
            type_key="SCRAPY_QUEUE_BACKEND_TYPE",
            settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
            required_capabilities={"queue"},
            component_name="queue",
        )
        if connection_manager is not None and not (
            settings.get("SCRAPY_QUEUE_BACKEND_TYPE")
            or settings.get("SCRAPY_BACKEND_TYPE")
            or os.environ.get("SCRAPY_QUEUE_BACKEND_TYPE")
            or os.environ.get("SCRAPY_BACKEND_TYPE")
        ):
            # A mixin's manager is configured by its class attributes rather than
            # Scrapy's global component settings.  Use its actual descriptor for
            # capability, ACK, and snapshot decisions when no component override
            # is present; otherwise the standard settings resolver remains the
            # source of truth.
            candidate: Any = backend_type_override
            if candidate is None:
                candidate = getattr(
                    connection_manager, "_backend_type_for_operations", None
                )
                if callable(candidate):
                    candidate = candidate()
            if candidate is None:
                candidate = getattr(connection_manager, "backend_type", None)
            candidate = getattr(candidate, "value", candidate)
            if isinstance(candidate, str) and has_capability(candidate, "queue"):
                backend_type = candidate
                backend_settings = {}
        queue_config = queue_config.with_queue_key(
            settings,
            spider_name=spider_name,
            project_name=project_name,
            queue_key_override=queue_key,
        )
        assert queue_config.queue_key is not None
        assert queue_config.queue_key_template is not None
        if backend_type in _CONSUMER_SCOPED_BACKENDS:
            # Kafka and RocketMQ each keep one mutable consumer on the backend
            # instance. Sharing that instance across logical queues makes Kafka
            # replace its subscription on every alternating pop and makes RocketMQ
            # accumulate both subscriptions on one receive loop. Add a registry-only
            # discriminator so schedulers for different queues get independent
            # consumers; ConnectionManager strips it before Pydantic validation.
            scope = queue_config.queue_key
            if spider_name is None and "{spider}" in queue_config.queue_key_template:
                # Direct ``from_settings`` has no crawler/spider identity yet. Sharing
                # the literal template would join unrelated future queues to one
                # mutable consumer, so this scheduler gets a registry-only opaque scope.
                scope = f"unresolved-{uuid.uuid4().hex}"
            backend_settings = {
                **backend_settings,
                _CONNECTION_MANAGER_SCOPE_KEY: scope,
            }
        manager_lease: ConnectionManagerLease | None = None
        if connection_manager is None:
            manager_lease = ConnectionManager.acquire_lease(
                backend_type=backend_type,
                settings=backend_settings,
            )
            manager = manager_lease.manager
        else:
            # Composite owners (notably BackendSpiderMixin) already hold the
            # queue manager's acquire.  Reuse that exact manager instead of
            # creating a second registry reference; the owner will release it
            # only after every borrowed component has quiesced.
            manager = connection_manager
        if snapshot_connection_manager is not None:
            snapshot_manager_lease = snapshot_connection_manager_lease
        else:
            snapshot_manager_lease = None
        # An injected snapshot manager/lease is an ownership transfer from a
        # composite factory.  The normal settings path below fills these values.
        snapshot_manager_owned = (
            owns_snapshot_connection_manager
            if owns_snapshot_connection_manager is not None
            else snapshot_connection_manager is not None
        )
        factory_failure: BaseException | None = None
        try:
            # Ack-concurrency gate (round-2 C1 fix). Inspect the backend CLASS —
            # no instantiation/connection needed. NOTE (2026-07-10): every bundled
            # backend sets supports_concurrent_ack=True, so this gate is unreachable
            # for bundled backends — it remains a defensive backstop for a
            # hypothetical 3rd-party single-slot backend.
            ack_backend = (
                manager if type(manager) is ConnectionManager else backend_type
            )
            BackendScheduler._enforce_ack_concurrency_gate(settings, ack_backend)

            queue_config = queue_config.with_strategy_settings(settings)
            assert queue_config.strategy_type is not None
            assert queue_config.default_delay is not None
            assert queue_config.min_interval is not None
            assert queue_config.priority_levels is not None
            assert queue_config.wheel_size is not None
            assert queue_config.ticks_per_second is not None
            assert queue_config.steal_timeout is not None
            assert queue_config.capacity is not None
            try:
                queue_strategy = build_queue_strategy(
                    queue_config.strategy_type,
                    manager,
                    default_delay=queue_config.default_delay,
                    min_interval=queue_config.min_interval,
                    max_held=queue_config.delay_max_held,
                    priority_levels=queue_config.priority_levels,
                    wheel_size=queue_config.wheel_size,
                    ticks_per_second=queue_config.ticks_per_second,
                    worker_id=queue_config.worker_id,
                    peer_ids=queue_config.peer_ids,
                    steal_timeout=queue_config.steal_timeout,
                    capacity=queue_config.capacity,
                    full_policy=queue_config.ring_buffer_full_policy,
                    name_generation=queue_config.name_generation,
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
                }.get(queue_config.strategy_type, "SCRAPY_QUEUE_STRATEGY")
                # R88: WorkStealingQueueStrategy validates worker_id before peer_ids, so a
                # key-unsafe SCRAPY_QUEUE_WORKER_ID (survives with_strategy_settings
                # .strip()-only) raises here -- attribute it to the setting the operator
                # configured, not the default SCRAPY_QUEUE_PEER_IDS. Sibling of the
                # snapshot_owner attribution fix (R80/fe72f30); the worker_id-is-None guard
                # mirrors work_stealing.py:108 (None auto-generates a uuid, never validates).
                if (
                    queue_config.strategy_type is QueueStrategyType.WORK_STEALING
                    and queue_config.worker_id is not None
                ):
                    try:
                        _validate_key_name(queue_config.worker_id, "worker_id")
                    except ValueError:
                        constructor_setting = "SCRAPY_QUEUE_WORKER_ID"
                raise ConfigurationError(
                    f"Invalid {constructor_setting}: {exc}",
                    setting_name=constructor_setting,
                    setting_value=settings.get(constructor_setting),
                ) from exc
            # Strategy+MQ ack-bypass warning (2026-07-10 §B, refined 2026-07-11 #28):
            # fires only for strategies that do NOT override pop_with_ack (so they
            # lose the MQ per-message token) paired with a requires_ack backend.
            BackendScheduler._warn_strategy_mq_ack_bypass(
                queue_strategy,
                ack_backend,
            )
            if (
                snapshot_connection_manager is None
                and queue_config.strategy_type
                in {
                    QueueStrategyType.DELAY,
                    QueueStrategyType.ROUND_ROBIN,
                    QueueStrategyType.TIME_WHEEL,
                    QueueStrategyType.RING_BUFFER,
                }
                and not has_capability(backend_type, "storage")
            ):
                storage_type_override = settings.get("SCRAPY_STORAGE_BACKEND_TYPE")
                has_explicit_storage_type = storage_type_override not in (
                    None,
                    "",
                ) or bool(os.environ.get("SCRAPY_STORAGE_BACKEND_TYPE"))
                try:
                    snapshot_backend_type, snapshot_backend_settings = (
                        resolve_backend_config(
                            settings,
                            type_key="SCRAPY_STORAGE_BACKEND_TYPE",
                            settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
                            required_capabilities={"storage"},
                            component_name="storage",
                        )
                    )
                except ConfigurationError:
                    if has_explicit_storage_type:
                        raise
                    # A legacy queue-only global backend has no storage component
                    # configured. Preserve its best-effort no-snapshot behavior;
                    # an explicit invalid storage override remains fail-fast.
                else:
                    snapshot_manager_lease = ConnectionManager.acquire_lease(
                        backend_type=snapshot_backend_type,
                        settings=snapshot_backend_settings,
                    )
                    snapshot_connection_manager = snapshot_manager_lease.manager
                    snapshot_manager_owned = True
            queue_config = queue_config.with_runtime_settings(settings)
            assert queue_config.queue_key is not None
            assert queue_config.queue_depth_sample_every is not None
            assert queue_config.queue_max_item_bytes is not None
            assert queue_config.monitor_backpressure_threshold is not None
            assert queue_config.monitor_pop_rate_window_s is not None
            assert queue_config.queue_snapshot_max_bytes is not None
            assert queue_config.queue_snapshot_chunk_bytes is not None
            assert queue_config.reactor_io_timeout is not None
            return cls(
                connection_manager=manager,
                queue_key=queue_config.queue_key,
                queue_strategy=queue_strategy,
                backpressure_pause_at=queue_config.backpressure_pause_at,
                backpressure_resume_at=queue_config.backpressure_resume_at,
                queue_depth_sample_every=queue_config.queue_depth_sample_every,
                queue_max_item_bytes=queue_config.queue_max_item_bytes,
                monitor_backpressure_threshold=(
                    queue_config.monitor_backpressure_threshold
                ),
                monitor_pop_rate_window_s=queue_config.monitor_pop_rate_window_s,
                queue_snapshot_owner=queue_config.queue_snapshot_owner,
                queue_snapshot_max_bytes=queue_config.queue_snapshot_max_bytes,
                queue_snapshot_chunk_bytes=queue_config.queue_snapshot_chunk_bytes,
                reactor_io_timeout=queue_config.reactor_io_timeout,
                project_name=queue_config.project_name,
                allow_cross_spider=queue_config.allow_cross_spider,
                stats=stats,
                dupefilter=dupefilter,
                snapshot_connection_manager=snapshot_connection_manager,
                owns_snapshot_connection_manager=snapshot_manager_owned,
                owns_connection_manager=(
                    owns_connection_manager
                    if owns_connection_manager is not None
                    else True
                ),
                connection_manager_lease=manager_lease,
                snapshot_connection_manager_lease=snapshot_manager_lease,
            )
        except BaseException as exc:
            # Leave the factory exception suite before releasing and diagnosing
            # acquires.  Static diagnostics must not inherit the raw exception
            # graph through sys.exc_info().
            factory_failure = exc

        assert factory_failure is not None
        release_failures: list[str] = []
        if snapshot_manager_lease is not None:
            try:
                release_manager_acquire(snapshot_manager_lease, exact=True)
            except BaseException:
                release_failures.append("snapshot")
        if manager_lease is not None:
            try:
                release_manager_acquire(manager_lease, exact=True)
            except BaseException:
                release_failures.append("queue")
        for name in release_failures:
            _emit_diagnostic(
                logger.error,
                "Failed to release %s ConnectionManager after scheduler factory failure",
                name,
            )
        raise factory_failure

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
            pop_rate_window_s: Trailing window for ``queue/pop_rate_1m`` (forwarded
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
        if isinstance(backend_type, ConnectionManager):
            manager = backend_type
            backend_type = manager._backend_type_for_operations()
            requires_ack, supports_concurrent = manager._static_ack_capabilities()
        else:
            from scrapy_extension.backends.connectors import (
                _load_static_ack_capabilities,
            )
            from scrapy_extension.backends.registry import get_descriptor

            descriptor = get_descriptor(str(backend_type))
            requires_ack, supports_concurrent = _load_static_ack_capabilities(
                descriptor
            )
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
            backend_type.value
            if isinstance(backend_type, BackendType)
            else backend_type
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
        if isinstance(backend_type, ConnectionManager):
            manager = backend_type
            backend_type = manager._backend_type_for_operations()
            requires_ack, _supports_concurrent = manager._static_ack_capabilities()
        else:
            from scrapy_extension.backends.connectors import (
                _load_static_ack_capabilities,
            )
            from scrapy_extension.backends.registry import get_descriptor

            descriptor = get_descriptor(str(backend_type))
            requires_ack, _supports_concurrent = _load_static_ack_capabilities(
                descriptor
            )
        if not requires_ack:
            return
        bt_name = (
            backend_type.value
            if isinstance(backend_type, BackendType)
            else backend_type
        )
        # This is advisory after all descriptor and strategy controls have run.
        # A custom logging handler must not make an otherwise valid configuration
        # fail, including when it raises a process-control BaseException.
        try:
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
        except BaseException:
            pass

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> BackendScheduler:
        """Create scheduler from crawler."""
        scheduler = cls.from_settings(
            crawler.settings,
            spider_name=cls._crawler_spider_name(crawler),
        )
        factory_failure: BaseException | None = None
        try:
            scheduler.stats = crawler.stats
            dupefilter_path = crawler.settings.get("DUPEFILTER_CLASS")
            if dupefilter_path:
                dupefilter_cls = load_object(dupefilter_path)
                scheduler.dupefilter = dupefilter_cls.from_crawler(crawler)
                scheduler._owns_dupefilter = True
            return scheduler
        except BaseException as exc:
            factory_failure = exc
        # No scheduler is returned on this path. Use a normal close first so any
        # authoritative checkpoint is preserved; a failed checkpoint/close then
        # transitions to the explicit lossy abort, which skips replacing the last
        # valid checkpoint and still releases both exact managers.
        rollback_failed = False
        try:
            scheduler._rollback_factory_failure()
        except BaseException:
            rollback_failed = True
        if rollback_failed:
            _emit_diagnostic(
                logger.error, "Failed to schedule scheduler factory rollback"
            )
        assert factory_failure is not None
        raise factory_failure

    def _force_factory_manager_release(self) -> None:
        """Release both factory-owned managers after a lossy abort attempt."""
        primary_error: BaseException | None = None
        managers = (
            (
                "snapshot",
                self._snapshot_connection_manager,
                self._snapshot_connection_manager_lease,
                self._snapshot_manager_released,
                self._owns_snapshot_connection_manager,
            ),
            (
                "queue",
                self.connection_manager,
                self._connection_manager_lease,
                self._manager_released,
                self._owns_connection_manager,
            ),
        )
        for name, manager, lease, released, owns in managers:
            if not owns or manager is None or released:
                continue
            try:
                if lease is not None:
                    release_manager_acquire(lease, exact=True)
                else:
                    release_manager_acquire(manager)
            except BaseException as exc:
                if primary_error is None:
                    primary_error = exc
                # The fixed diagnostic is emitted after this exception suite has
                # unwound below, so handlers cannot inspect the provider failure.
                release_failed = True
            else:
                release_failed = False
            if release_failed:
                _emit_diagnostic(
                    logger.error,
                    "Failed to release %s ConnectionManager after scheduler "
                    "factory abort",
                    name,
                )
            else:
                if name == "snapshot":
                    self._snapshot_manager_released = True
                else:
                    self._manager_released = True
        if primary_error is not None:
            raise primary_error

    def _observe_factory_cleanup(
        self,
        result: Deferred[Any] | None,
        *,
        on_failure: Callable[[Any], None],
    ) -> None:
        """Observe a factory cleanup Deferred without creating an unhandled branch."""
        if not isinstance(result, Deferred):
            return

        def consume_failure(failure: Any) -> None:
            try:
                on_failure(failure)
            except BaseException:
                try:
                    self._force_factory_manager_release()
                except BaseException:
                    pass
            return None

        observed = result.addErrback(consume_failure)
        observed.addErrback(lambda _failure: None)

    def _rollback_factory_failure(self) -> None:
        """Retryably unwind an unpublished scheduler after crawler wiring fails."""
        abort_started = False

        def lossy_abort(_failure: Any = None) -> None:
            nonlocal abort_started
            if abort_started:
                return
            abort_started = True
            try:
                result = self.abort("crawler-factory-failed")
            except BaseException:
                try:
                    self._force_factory_manager_release()
                except BaseException:
                    pass
                return
            authority = getattr(self, "_close_completion_deferred", None)
            if not isinstance(authority, Deferred):
                authority = result
            public = result if isinstance(result, Deferred) else None
            if public is not None and public is not authority:
                # A timeout view has no owner once the factory returns. Its
                # failure is advisory; the authoritative abort below owns retry.
                public.addErrback(lambda _failure: None)
            self._observe_factory_cleanup(
                authority,
                on_failure=lambda _abort_failure: self._force_factory_manager_release(),
            )

        try:
            result = self.close("crawler-factory-failed")
        except BaseException:
            lossy_abort()
            return
        authority = getattr(self, "_close_completion_deferred", None)
        if not isinstance(authority, Deferred):
            authority = result
        public = result if isinstance(result, Deferred) else None
        if public is not None and public is not authority:
            public.addErrback(lambda _failure: None)
        self._observe_factory_cleanup(authority, on_failure=lossy_abort)

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

    def _warm_connections(self) -> None:
        """Warm queue and snapshot managers outside the reactor thread."""
        self.connection_manager.get_queue_backend()
        if self._snapshot_connection_manager is not None:
            self._snapshot_connection_manager.get_storage_backend()

    def open(self, spider: Spider) -> Deferred[None] | None:
        """Open the scheduler and await asynchronous lifecycle hooks.

        Scrapy 2.17 awaits a Deferred returned by a scheduler lifecycle method.
        Do not publish an open scheduler, or construct its queue, until a generic
        dupefilter's ``open()`` Deferred has succeeded.  The bundled
        ``BackendDupeFilter`` remains spider-aware through the compatibility
        helper above.
        """
        with self._lifecycle_lock:
            if self._lifecycle_state == _LIFECYCLE_CLOSED:
                raise RuntimeError("Scheduler is closed and cannot be reopened")
            if self._lifecycle_state in {_LIFECYCLE_OPENING, _LIFECYCLE_CLOSING}:
                raise RuntimeError(
                    "Scheduler lifecycle transition is already in progress"
                )
            if self._lifecycle_state == _LIFECYCLE_OPEN:
                if self._spider is spider:
                    return None
                raise RuntimeError("Scheduler is already open for a different spider")
            self._lifecycle_state = _LIFECYCLE_OPENING
            self._spider = spider
            self._opening_settled = False
            self._opening_failure = None
            self._open_close_requested = False

        open_result: Any = None
        open_error: BaseException | None = None
        try:
            if (
                self._owns_dupefilter
                and self.dupefilter is not None
                and not self._dupefilter_open
                and not self._dupefilter_released
            ):
                lifecycle = _backend_dupefilter_lifecycle(self.dupefilter)
                authoritative_open = getattr(
                    self.dupefilter,
                    "_open_authoritative_async",
                    None,
                )
                if (
                    reactor_is_running()
                    and lifecycle is not None
                    and callable(authoritative_open)
                ):
                    # BackendDupeFilter's synchronous MembershipFilter callback
                    # must never execute on Scrapy's reactor thread. Keep the
                    # authoritative worker Deferred in the scheduler's opening
                    # chain; the public view is bounded below.
                    open_result, bounded_open = authoritative_open(
                        spider,
                        timeout=self._reactor_io_timeout,
                    )
                    # The scheduler exposes its own bounded opening view below;
                    # consume this package-level compatibility view so a late
                    # worker failure is not reported a second time as an
                    # unhandled Deferred.
                    if (
                        isinstance(bounded_open, Deferred)
                        and bounded_open is not open_result
                    ):
                        bounded_open.addErrback(lambda _failure: None)
                elif reactor_is_running() and lifecycle is not None:
                    # A plugin may intentionally replace the private adapter. The
                    # bundled capability still fences its synchronous hook off the
                    # reactor rather than assuming that replacement is nonblocking.
                    open_result = _submit_thread(
                        _call_dupefilter_open,
                        self.dupefilter,
                        spider,
                    )
                else:
                    open_result = _call_dupefilter_open(self.dupefilter, spider)
        except BaseException as exc:
            open_error = exc
        if open_error is not None:
            return self._finish_open_failure(open_error)

        opening_authority: Deferred[Any] | None = None
        generic_public_signal: Deferred[Any] | None = None
        generic_public_view: Deferred[Any] | None = None
        warmup_public_view: Deferred[Any] | None = None
        public_open_result: Deferred[Any] | None = None

        def bridge_open_authority(result: Any) -> Any:
            """Publish the full opening result for a later close request."""
            if opening_authority is not None and not opening_authority.called:
                if isinstance(result, TwistedFailure):
                    opening_authority.errback(result)
                else:
                    opening_authority.callback(result)
            return result

        def finish_after_dupefilter(_ignored: Any = None) -> Any:
            """Run warm-up and return its authoritative operation, not its view."""
            nonlocal warmup_public_view
            if not reactor_is_running():
                with self._lifecycle_lock:
                    close_requested = self._open_close_requested
                if close_requested:
                    with self._lifecycle_lock:
                        self._opening_settled = True
                    return None
                try:
                    self._finish_open(spider)
                except BaseException as exc:
                    open_failure = exc
                    with self._lifecycle_lock:
                        self._opening_settled = True
                        self._opening_failure = TwistedFailure(  # type: ignore[no-untyped-call]
                            open_failure
                        )
                    if opening_authority is None:
                        # The synchronous non-generic path lets the outer open()
                        # boundary perform exactly one rollback attempt.
                        raise open_failure
                    cleanup = self._cleanup_after_open_failure("open-failed")
                    if isinstance(cleanup, Deferred):
                        return cleanup.addBoth(lambda _ignored: fail(open_failure))
                    raise open_failure
                with self._lifecycle_lock:
                    self._opening_settled = True
                if (
                    not reactor_is_running()
                    and public_open_result is not None
                    and not public_open_result.called
                ):
                    public_open_result.callback(None)
                return None

            # Keep the backend warm-up in the thread pool. The returned operation
            # remains authoritative after the caller-facing timeout, so a timed
            # out thread cannot publish OPEN or race close/manager release.
            operation = _submit_thread(self._warm_connections)
            with self._lifecycle_lock:
                if opening_authority is None:
                    self._opening_operation = operation

            def finish_success(value: Any) -> Any:
                # Keep the opening barrier active through queue construction,
                # signal registration, and the final OPEN publication.  Marking it
                # settled before ``_finish_open`` lets a concurrent close observe
                # an opening scheduler with no authoritative operation and either
                # race those callbacks or fail spuriously.
                with self._lifecycle_lock:
                    close_requested = self._open_close_requested
                if close_requested:
                    with self._lifecycle_lock:
                        self._opening_settled = True
                    return fail(RuntimeError("Scheduler open cancelled by close"))

                def publish_success(_ignored: Any) -> Any:
                    with self._lifecycle_lock:
                        self._opening_settled = True
                        if self._open_close_requested:
                            return fail(
                                RuntimeError("Scheduler open cancelled by close")
                            )
                    return value

                def publish_failure(failure: Any) -> Any:
                    # Queue construction runs as a child operation of the warm-up
                    # Deferred. Keep its failure authoritative while cleanup owns
                    # the partially opened dupefilter/manager resources.
                    with self._lifecycle_lock:
                        self._opening_settled = True
                        self._opening_failure = failure
                        close_requested = self._open_close_requested
                    if close_requested:
                        return failure
                    if (
                        getattr(finish_result, "_reactor_submission_failure", None)
                        == "thread"
                    ):
                        self._reset_open_after_thread_submission_failure()
                        return failure
                    cleanup = self._cleanup_after_open_failure("open-failed")
                    if isinstance(cleanup, Deferred):
                        return cleanup.addBoth(lambda _ignored: failure)
                    return failure

                try:
                    finish_result = self._finish_open(spider)
                except BaseException as exc:
                    self._opening_failure = TwistedFailure(  # type: ignore[no-untyped-call]
                        exc
                    )
                    with self._lifecycle_lock:
                        self._opening_settled = True
                    cleanup = self._cleanup_after_open_failure("open-failed")
                    if isinstance(cleanup, Deferred):

                        def restore_open_failure(
                            _ignored: Any, failure: BaseException
                        ) -> Deferred[None]:
                            return fail(failure)

                        return cleanup.addBoth(restore_open_failure, exc)
                    return fail(exc)
                if isinstance(finish_result, Deferred):
                    try:
                        finish_result.addCallbacks(publish_success, publish_failure)
                    except BaseException as exc:
                        try:
                            finish_result.addCallbacks(publish_success, publish_failure)
                        except BaseException:
                            failed: Deferred[Any] = Deferred()
                            failed.errback(exc)
                            finish_result.addErrback(lambda _failure: None)
                            return failed
                    return finish_result
                return publish_success(finish_result)

            def finish_failure(failure: Any) -> Any:
                with self._lifecycle_lock:
                    self._opening_settled = True
                    self._opening_failure = failure
                    close_requested = self._open_close_requested
                if close_requested:
                    return failure
                if getattr(operation, "_reactor_submission_failure", None) == "thread":
                    self._reset_open_after_thread_submission_failure()
                    return failure
                cleanup = self._cleanup_after_open_failure("open-failed")
                if isinstance(cleanup, Deferred):
                    return cleanup.addBoth(lambda _ignored: failure)
                return failure

            try:
                operation.addCallbacks(finish_success, finish_failure)
            except BaseException as exc:
                # Keep one accepted warm-up worker fenced if a Deferred rejects
                # its first lifecycle callback attachment.
                try:
                    operation.addCallbacks(finish_success, finish_failure)
                except BaseException:
                    operation._reactor_callback_failure = True  # type: ignore[attr-defined]
                    failed: Deferred[Any] = Deferred()
                    failed.errback(exc)
                    operation.addErrback(lambda _failure: None)
                    return failed
            if opening_authority is not None:
                # The generic-open public stage transitions to this bounded warm-up
                # view only after the authoritative callback has started it and the
                # ownership/publication callbacks above have been attached.
                warmup_public_view = bounded_deferred(
                    operation,
                    timeout=self._reactor_io_timeout,
                    operation="scheduler connection warm-up",
                )
                if (
                    generic_public_signal is not None
                    and not generic_public_signal.called
                ):
                    generic_public_signal.callback(None)
                operation.addErrback(lambda _failure: None)
            return operation

        if isinstance(open_result, Deferred):
            # A generic dupefilter has two views: this private authority drives
            # warm-up/publication and close, while the public stages bound the
            # generic open and the later warm-up independently.
            opening_authority = Deferred()
            generic_public_signal = Deferred()
            public_open_result = Deferred()
            with self._lifecycle_lock:
                self._opening_operation = opening_authority

            def handle_dupefilter_failure(failure: Any) -> Any:
                if (
                    getattr(open_result, "_reactor_submission_failure", None)
                    == "thread"
                ):
                    self._reset_open_after_thread_submission_failure()
                    result = failure
                else:
                    result = self._handle_open_failure(failure, "open-failed")

                def publish_failure(_ignored: Any) -> Any:
                    if reactor_is_running() and not generic_public_signal.called:
                        generic_public_signal.errback(failure)
                    return failure

                if isinstance(result, Deferred):
                    return result.addBoth(publish_failure)
                publish_failure(None)
                return result

            try:
                authoritative = open_result.addCallbacks(
                    finish_after_dupefilter,
                    handle_dupefilter_failure,
                )
            except BaseException as exc:
                try:
                    authoritative = open_result.addCallbacks(
                        finish_after_dupefilter,
                        handle_dupefilter_failure,
                    )
                except BaseException:
                    failed: Deferred[Any] = Deferred()
                    failed.errback(exc)
                    open_result.addErrback(lambda _failure: None)
                    return failed
            try:
                authoritative.addBoth(bridge_open_authority)
            except BaseException:
                try:
                    authoritative.addBoth(bridge_open_authority)
                except BaseException:
                    pass
            # In synchronous/non-reactor use the source Deferred is the public
            # scheduler.open result for Scrapy compatibility. Leave its terminal
            # Failure untouched so the caller observes it; the separate authority
            # bridge is consumed only for close bookkeeping.
            if not reactor_is_running():
                DeferredList(
                    [opening_authority],
                    fireOnOneErrback=False,
                    consumeErrors=True,
                )
                return open_result

            # The public generic/warm-up views are separate Deferreds. Consume
            # this private chain's terminal Failure after it has bridged both
            # close authority and the caller-facing signal.
            authoritative.addErrback(lambda _failure: None)
            # If no close owner exists, DeferredList consumes only the gate's late
            # Failure. A close attached before settlement remains authoritative and
            # uses _opening_failure to preserve the original open error.
            DeferredList(
                [opening_authority],
                fireOnOneErrback=False,
                consumeErrors=True,
            )

            def publish_generic_success(_value: Any) -> Any:
                if public_open_result.called:
                    return None
                if warmup_public_view is None:
                    public_open_result.callback(None)
                else:
                    warmup_public_view.addCallbacks(
                        lambda value: (
                            public_open_result.callback(value)
                            if not public_open_result.called
                            else value
                        ),
                        lambda failure: (
                            public_open_result.errback(failure)
                            if not public_open_result.called
                            else None
                        ),
                    )
                return None

            def publish_generic_failure(failure: Any) -> None:
                if not public_open_result.called:
                    public_open_result.errback(failure)
                return None

            generic_public_view = bounded_deferred(
                generic_public_signal,
                timeout=self._reactor_io_timeout,
                operation="scheduler dupefilter open",
            )
            try:
                generic_public_view.addCallbacks(
                    publish_generic_success,
                    publish_generic_failure,
                )
            except BaseException as exc:
                try:
                    generic_public_view.addCallbacks(
                        publish_generic_success,
                        publish_generic_failure,
                    )
                except BaseException:
                    if not public_open_result.called:
                        public_open_result.errback(exc)
            # ``generic_public_signal`` is an internal bridge.  The bounded view
            # observes its failure for the caller-facing Deferred, but
            # ``bounded_deferred`` deliberately preserves source failures for
            # authoritative owners.  This bridge has no later owner, so consume it
            # only after the cleanup chain has published the public result.  Without
            # this observer a generic dupefilter failure is reported again as an
            # unhandled Deferred during GC.
            generic_public_signal.addErrback(lambda _failure: None)
            return public_open_result

        open_error = None
        open_deferred: Deferred[None] | None = None
        try:
            result = finish_after_dupefilter()
            if isinstance(result, Deferred):
                open_deferred = bounded_deferred(
                    result,
                    timeout=self._reactor_io_timeout,
                    operation="scheduler connection warm-up",
                )
                result.addErrback(lambda _failure: None)
        except BaseException as exc:
            open_error = exc
        if open_error is not None:
            return self._finish_open_failure(open_error)
        return open_deferred

    def _reset_open_after_thread_submission_failure(self) -> None:
        """Return an unstarted open attempt to NEW without releasing its owner.

        No worker was accepted on this path, so no queue callback can publish or
        clean up this generation.  Keep the already-acquired manager handles for
        the caller's next open attempt; a later explicit close still owns the
        normal dupefilter/manager teardown.
        """
        with self._lifecycle_lock:
            if self._lifecycle_state == _LIFECYCLE_OPENING:
                self._lifecycle_state = _LIFECYCLE_NEW
            self._spider = None
            self._opening_operation = None
            self._opening_settled = True
            self._opening_failure = None
            self._open_close_requested = False

    def _finish_open_failure(self, error: BaseException) -> Deferred[None] | None:
        """Clean up outside the original exception handler and preserve its error."""
        cleanup = self._cleanup_after_open_failure("open-failed")
        if isinstance(cleanup, Deferred):

            def fail_open(_ignored: Any, original: BaseException) -> Deferred[None]:
                return fail(original)

            return cleanup.addBoth(fail_open, error)
        raise error

    def _finish_open(self, spider: Spider) -> Deferred[Any] | None:
        """Construct and publish the scheduler after dupefilter open succeeds."""
        with self._lifecycle_lock:
            if (
                self._open_close_requested
                or self._lifecycle_state != _LIFECYCLE_OPENING
            ):
                return None
        _validate_key_name(spider.name, field_name="spider.name")
        if self._project_name == DEFAULT_PROJECT_NAME:
            self._project_name = project_name_from_spider(spider)
        _validate_key_name(self._project_name, field_name="project_name")
        queue_key = resolve_identity_template(
            self._queue_key_template,
            spider_name=spider.name,
            project_name=self._project_name,
        )
        _validate_key_name(queue_key, field_name="queue_key")
        if (
            self._owns_dupefilter
            and self.dupefilter is not None
            and not self._dupefilter_open
            and not self._dupefilter_released
        ):
            with self._lifecycle_lock:
                self._dupefilter_open = True
        monitor = BackendScheduler._resolve_monitor_for_spider(
            spider,
            backpressure_threshold=self._monitor_backpressure_threshold,
            pop_rate_window_s=self._monitor_pop_rate_window_s,
        )
        self.connection_manager.set_monitor(monitor)
        if self._snapshot_connection_manager is not None:
            self._snapshot_connection_manager.set_monitor(monitor)

        def construct_queue() -> BackendQueue:
            # BackendQueue.__init__ opens the strategy and restores its snapshot;
            # the latter can perform synchronous storage I/O. Keep that boundary
            # off Scrapy's reactor while leaving signal registration and lifecycle
            # publication on the callback thread.
            return BackendQueue(
                connection_manager=self.connection_manager,
                queue_name=queue_key,
                spider=spider,
                project_name=self._project_name,
                allow_cross_spider=self._allow_cross_spider,
                queue_strategy=self._queue_strategy,
                max_item_bytes=self._queue_max_item_bytes,
                monitor=monitor,
                depth_sample_every=self._queue_depth_sample_every,
                pop_rate_window_s=self._monitor_pop_rate_window_s,
                snapshot_owner=self._queue_snapshot_owner,
                snapshot_connection_manager=self._snapshot_connection_manager,
                snapshot_max_bytes=self._queue_snapshot_max_bytes,
                snapshot_chunk_bytes=self._queue_snapshot_chunk_bytes,
                reactor_io_timeout=self._reactor_io_timeout,
            )

        def publish_queue(queue: BackendQueue) -> None:
            with self._lifecycle_lock:
                self.queue_key = queue_key
                self._queue = queue
            self._connect_ack_signals(spider)
            with self._lifecycle_lock:
                if (
                    self._open_close_requested
                    or self._lifecycle_state != _LIFECYCLE_OPENING
                ):
                    return None
                # OPEN is the first success publication.
                self._lifecycle_state = _LIFECYCLE_OPEN
                # Publish the settled opening barrier with OPEN, before the
                # diagnostic logger below can re-enter close().  Otherwise a logger
                # callback could observe OPEN plus an unsettled opening Deferred and
                # create a self-referential close continuation.
                self._opening_settled = True
                self._backpressure_paused = False
                self._backpressure_probe_due = False
            try:
                logger.info("Scheduler opened for spider %s", spider.name)
            except BaseException:
                pass
            return None

        if reactor_is_running():
            operation = _submit_thread(construct_queue)
            try:
                result = operation.addCallback(publish_queue)
            except BaseException as exc:
                # A worker accepted by deferToThread remains the construction
                # authority. Retry the callback attachment once for provider/test
                # Deferreds that reject one registration attempt; never publish an
                # apparently-open scheduler without the queue callback.
                try:
                    result = operation.addCallback(publish_queue)
                except BaseException:
                    failed: Deferred[Any] = Deferred()
                    failed._reactor_callback_failure = True  # type: ignore[attr-defined]
                    failed.errback(exc)
                    operation.addErrback(lambda _failure: None)
                    return failed
            if getattr(operation, "_reactor_submission_failure", None) == "thread":
                result._reactor_submission_failure = "thread"  # type: ignore[attr-defined]
            return result
        publish_queue(construct_queue())
        return None

    def _cleanup_after_open_failure(self, reason: str) -> Deferred[None] | None:
        """Close partially opened resources without hiding the primary failure."""
        cleanup_failed = False
        try:
            return self._close_attempt(reason, allow_opening=True)
        except BaseException:
            cleanup_failed = True
        if cleanup_failed:
            _emit_diagnostic(
                logger.error, "Failed to clean up scheduler after open failure"
            )
        return None

    def _handle_open_failure(self, failure: Any, reason: str) -> Any:
        """Wait for cleanup, then preserve the original Deferred failure."""
        with self._lifecycle_lock:
            self._opening_settled = True
            self._opening_failure = failure
        cleanup = self._cleanup_after_open_failure(reason)
        if isinstance(cleanup, Deferred):
            return cleanup.addBoth(lambda _ignored: failure)
        return failure

    def _connect_ack_signals(self, spider: Spider) -> None:
        """Wire response_received → ack, spider_error → nack.

        Uses ``spider.crawler.signals`` so the scheduler doesn't need a
        crawler reference at construction time. Idempotent: guarded by
        ``_signals_connected`` so re-open doesn't double-register.
        """
        if self._signal_leases:
            return
        crawler = getattr(spider, "crawler", None)
        if crawler is None:
            # The no-crawler fallback is deliberately graceful. Its warning must
            # remain observational so an interrupted logger cannot abort open().
            try:
                logger.warning(
                    "spider has no 'crawler' attribute — ack/nack signals not wired. "
                    "Kafka/RabbitMQ messages will re-deliver on consumer restart "
                    "(at-least-once) but won't be acked in-session. "
                    "Ensure the spider is created via CrawlerProcess/CrawlerRunner."
                )
            except BaseException:
                pass
            return
        sig = crawler.signals
        signal_handlers = (
            (self._on_response_received, signals.response_received),
            (self._on_spider_error, signals.spider_error),
        )
        for handler, signal in signal_handlers:
            # Publish one unique receiver before connect(). A manager that registers
            # and then raises is still repaired by keyed disconnect during close.
            receiver = (
                _ResponseSignalReceiver(handler)
                if signal is signals.response_received
                else _SpiderErrorSignalReceiver(handler)
            )
            lease = _SignalLease(sig, signal, receiver)
            self._signal_leases.append(lease)
            self._sync_signal_compatibility_views()
            sig.connect(receiver, signal=signal)

    def _sync_signal_compatibility_views(self) -> None:
        """Maintain legacy signal fields as non-authoritative views."""
        if not self._signal_leases:
            self._connected_signals = None
            self._connected_ack_signal_handlers = None
            self._signals_connected = False
            return
        self._connected_signals = self._signal_leases[0].manager
        self._connected_ack_signal_handlers = [
            (lease.receiver, lease.signal) for lease in self._signal_leases
        ]
        self._signals_connected = True

    def _disconnect_signal_leases(self) -> None:
        """Release signal registrations without invoking managers under the lock."""
        while True:
            with self._lifecycle_lock:
                if not self._signal_leases:
                    return
                lease = self._signal_leases[0]
            try:
                lease.manager.disconnect(lease.receiver, signal=lease.signal)
            except DispatcherKeyError:
                # The unique registration is already absent: this is the successful
                # retry result for an effect-then-raise disconnect.
                pass
            with self._lifecycle_lock:
                if self._signal_leases and self._signal_leases[0] is lease:
                    self._signal_leases.pop(0)
                self._sync_signal_compatibility_views()

    def _on_response_received(
        self,
        response: Response,
        request: Request,
        spider: Spider,
    ) -> Any:
        """Ack the specific popped message after the download succeeded.

        Reads the ack token the pop path injected into
        ``request.meta["_backend_ack_token"]`` and forwards it to
        ``BackendQueue.ack(token=…)`` so the backend acks the *specific*
        message (Kafka contiguous watermark / RabbitMQ per-tag basic_ack) —
        correct under ``CONCURRENT_REQUESTS > 1``.
        """
        del response, spider
        return self._ack_request_token(
            request,
            log_message="Failed to ack message after response_received",
        )

    def _on_spider_error(
        self,
        failure: Failure,
        response: Response,
        spider: Spider,
    ) -> Any:
        """Nack the specific popped message so it re-delivers for retry.

        Reads the ack token from ``response.request.meta`` (the request that
        failed) and forwards it to ``BackendQueue.nack(token=…)``.
        """
        del failure, spider
        failed_request = getattr(response, "request", None)
        if failed_request is None:
            return None
        return self._nack_request_token(
            failed_request,
            log_message="Failed to nack message after spider_error",
        )

    def _ack_token(self, token: Any, *, log_message: str) -> bool:
        """Best-effort synchronous ack of one explicit token."""
        queue = self._queue
        if queue is None:
            return False
        settlement_failed = False
        try:
            queue.ack(token=token)
        except BackendError:
            settlement_failed = True
        if settlement_failed:
            # The BackendError has left its ``except`` suite before either stats or
            # logging runs. Both collaborators are extension code and must not be
            # able to inspect the queue error through ``sys.exc_info()``.
            self._record_stat("scheduler/ack_error")
            try:
                logger.error(log_message)
            except BaseException:
                pass
            return False
        return True

    def _track_authoritative_settlement(self, operation: Deferred[Any]) -> None:
        """Retain one worker operation until its backend call really completes."""
        with self._settlement_lock:
            self._pending_settlements.add(operation)

        def complete(result: Any) -> Any:
            with self._settlement_lock:
                self._pending_settlements.discard(operation)
            return result

        # The adapter may return an already-fired Deferred. Attach outside the
        # registry lock so synchronous callback execution cannot re-enter it.
        try:
            operation.addBoth(complete)
        except BaseException:
            try:
                operation.addBoth(complete)
            except BaseException:
                # Keep a pending operation in the registry. If it was already
                # settled, no callback can race a release anymore.
                if operation.called:
                    with self._settlement_lock:
                        self._pending_settlements.discard(operation)

    def _settle_token_async_ordered(
        self,
        token: Any,
        *,
        negative: bool,
        log_message: str,
    ) -> tuple[Deferred[Any], Deferred[bool]] | None:
        """Return authoritative and bounded views of one reactor settlement."""
        queue = self._queue
        if queue is None:
            return None
        operation, bounded = defer_to_thread_ordered(
            queue.nack if negative else queue.ack,
            token=token,
            timeout=self._reactor_io_timeout,
            operation="scheduler nack" if negative else "scheduler ack",
        )
        self._track_authoritative_settlement(operation)

        def success(_value: Any) -> bool:
            return True

        bounded._scheduler_public_failed = False  # type: ignore[attr-defined]
        bounded._scheduler_timed_out = False  # type: ignore[attr-defined]

        def failure(failure_value: Any) -> bool:
            bounded._scheduler_public_failed = True  # type: ignore[attr-defined]
            if isinstance(failure_value, TwistedFailure) and failure_value.check(  # type: ignore[no-untyped-call]
                BackendOperationTimeout
            ):
                bounded._scheduler_timed_out = True  # type: ignore[attr-defined]
            self._record_stat(
                "scheduler/nack_error" if negative else "scheduler/ack_error"
            )
            try:
                logger.error(log_message)
            except BaseException:
                pass
            return False

        # ``bounded`` is only the caller-facing diagnostic view. Group state is
        # finalized from ``operation`` so a timeout cannot permit a late duplicate.
        try:
            public = bounded.addCallbacks(success, failure)
        except BaseException as exc:
            # The worker was accepted and remains the token's authority even when
            # a provider rejects both attempts to attach the public observer.
            # Return a settled diagnostic view instead of letting a signal callback
            # escape synchronously and strand the settlement transfer.
            try:
                public = bounded.addCallbacks(success, failure)
            except BaseException:
                failed: Deferred[bool] = Deferred()
                try:
                    failed.errback(exc)
                except BaseException:
                    pass
                try:
                    bounded.addErrback(lambda _failure: None)
                except BaseException:
                    pass
                public = failed
        return operation, public

    @staticmethod
    def _remove_request_token_if_same(request: Any, token: Any) -> None:
        """Remove only the delivery token that this settlement actually owned."""
        meta = getattr(request, "meta", None)
        if (
            isinstance(meta, MutableMapping)
            and meta.get(BACKEND_ACK_TOKEN_META_KEY) is token
        ):
            meta.pop(BACKEND_ACK_TOKEN_META_KEY, None)

    def _settle_token_async(
        self,
        token: Any,
        *,
        negative: bool,
        log_message: str,
        on_authoritative_success: Callable[[], None] | None = None,
    ) -> Deferred[bool] | None:
        """Settle one broker token off-reactor while retaining best-effort policy."""
        ordered = self._settle_token_async_ordered(
            token,
            negative=negative,
            log_message=log_message,
        )
        if ordered is None:
            return None
        operation, bounded = ordered
        if on_authoritative_success is not None:

            def authoritative_success(value: Any) -> Any:
                if not getattr(bounded, "_scheduler_public_failed", False) or getattr(
                    bounded, "_scheduler_timed_out", False
                ):
                    on_authoritative_success()
                return value

            try:
                operation.addCallback(authoritative_success)
            except BaseException:
                try:
                    operation.addCallback(authoritative_success)
                except BaseException:
                    # A provider-specific worker may reject observers after it
                    # has accepted the backend call. Conservatively retain the
                    # request token rather than claiming settlement without proof.
                    pass
        # The authoritative failure is already represented by the bounded view's
        # one diagnostic callback. Consume only this worker chain's late failure;
        # callers still receive the public timeout/failure Deferred.
        try:
            operation.addErrback(lambda _failure: None)
        except BaseException:
            try:
                operation.addErrback(lambda _failure: None)
            except BaseException:
                pass
        return bounded

    def _ack_request_token(self, request: Request, *, log_message: str) -> Any:
        """Best-effort ack of the token carried by ``request``."""
        if getattr(request, "meta", None) is None:
            return None
        token = request.meta.get(BACKEND_ACK_TOKEN_META_KEY)
        if token is None:
            return None
        if reactor_is_running():
            result = self._settle_token_async(
                token,
                negative=False,
                log_message=log_message,
                on_authoritative_success=lambda: self._remove_request_token_if_same(
                    request, token
                ),
            )
            if result is None:
                return None
            return result
        if self._ack_token(token, log_message=log_message):
            self._remove_request_token_if_same(request, token)
        return None

    def _pending_settlement_barrier(self) -> Deferred[Any] | None:
        """Wait for all authoritative source settlements before queue teardown."""
        with self._settlement_lock:
            pending = tuple(self._pending_settlements)
        if not pending:
            return None
        return DeferredList(
            pending,
            fireOnOneErrback=False,
            consumeErrors=True,
        ).addCallback(lambda _results: None)

    def _nack_token(self, token: Any, *, log_message: str) -> bool:
        """Best-effort nack of one explicit token, returning settlement success."""
        queue = self._queue
        if queue is None:
            return False
        settlement_failed = False
        try:
            queue.nack(token=token)
        except BackendError:
            settlement_failed = True
        if settlement_failed:
            # The BackendError has left its ``except`` suite before either stats or
            # logging runs. Both collaborators are extension code and must not be
            # able to inspect the queue error through ``sys.exc_info()``.
            self._record_stat("scheduler/nack_error")
            try:
                logger.error(log_message)
            except BaseException:
                pass
            return False
        return True

    def _nack_request_token(self, request: Request, *, log_message: str) -> Any:
        """Best-effort nack of the token carried by ``request``."""
        if getattr(request, "meta", None) is None:
            return None
        token = request.meta.get(BACKEND_ACK_TOKEN_META_KEY)
        if token is None:
            return None
        if reactor_is_running():
            result = self._settle_token_async(
                token,
                negative=True,
                log_message=log_message,
                on_authoritative_success=lambda: self._remove_request_token_if_same(
                    request, token
                ),
            )
            if result is None:
                return None
            return result
        if self._nack_token(token, log_message=log_message):
            self._remove_request_token_if_same(request, token)
        return None

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
        """Close the scheduler, propagating asynchronous dupefilter teardown."""
        return self._close_attempt(reason)

    def abort(self, reason: str) -> Deferred[None] | None:
        """Explicitly discard uncheckpointed queue state and finish teardown."""
        return self._close_attempt(reason, lossy=True)

    def _close_attempt(
        self,
        reason: str,
        *,
        lossy: bool = False,
        allow_opening: bool = False,
    ) -> Deferred[None] | None:
        """Reserve one close pass and retain ownership until Deferred hooks settle."""
        owner_token = _SchedulerAttemptToken()
        with self._lifecycle_lock:
            if self._lifecycle_state == _LIFECYCLE_CLOSED:
                return None
            if self._lifecycle_state == _LIFECYCLE_OPENING and not allow_opening:
                if not self._opening_settled and self._opening_operation is not None:
                    # A caller-facing open timeout does not cancel the worker. Close
                    # becomes the cancellation/teardown request and remains fenced
                    # by that worker's authoritative completion.
                    self._open_close_requested = True
                else:
                    raise RuntimeError("Scheduler open is already in progress")
            if self._lifecycle_state == _LIFECYCLE_CLOSING:
                current_owner = self._close_attempt_owner
                if current_owner is not None and current_owner.active:
                    if current_owner.thread_id == owner_token.thread_id:
                        # Re-entrant close from an untrusted callback is bounded and
                        # leaves the outer attempt authoritative.
                        return None
                    raise RuntimeError("Scheduler close is already in progress")
                # The prior attempt unwound before it could clear package ownership.
                # Reclaim the stale token; provider handles remain authoritative and
                # make the resumed teardown idempotent.
            self._lifecycle_state = _LIFECYCLE_CLOSING
            self._close_attempt_owner = owner_token
            self._close_attempt_thread_id = owner_token.thread_id

        try:
            result = self._close_locked(reason, lossy=lossy)
        except BaseException:
            with self._lifecycle_lock:
                if self._close_attempt_owner is owner_token:
                    self._close_attempt_owner = None
                    self._close_attempt_thread_id = None
                self._close_retain_authoritative_failure = False
            raise
        public_result: Deferred[Any] | None = None
        authoritative_result: Deferred[Any] | None = None
        if isinstance(result, _DeferredLifecycleResult):
            authoritative_result = result.operation
            public_result = result.bounded
        elif isinstance(result, Deferred):
            authoritative_result = result
            public_result = result

        if authoritative_result is not None:
            owner_token.pending = True
            self._close_completion_deferred = authoritative_result
            retain_authoritative_failure = self._close_retain_authoritative_failure

            def finish(value: Any) -> Any:
                owner_token.pending = False
                with self._lifecycle_lock:
                    if self._close_attempt_owner is owner_token:
                        self._close_attempt_owner = None
                        self._close_attempt_thread_id = None
                    if self._close_completion_deferred is authoritative_result:
                        self._close_completion_deferred = None
                    self._close_retain_authoritative_failure = False
                return value

            try:
                try:
                    authoritative_result.addBoth(finish)
                except BaseException:
                    try:
                        authoritative_result.addBoth(finish)
                    except BaseException:
                        owner_token.pending = False
                        with self._lifecycle_lock:
                            if self._close_attempt_owner is owner_token:
                                self._close_attempt_owner = None
                                self._close_attempt_thread_id = None
                            if self._close_completion_deferred is authoritative_result:
                                self._close_completion_deferred = None
                        failed: Deferred[Any] = Deferred()
                        failed.errback(
                            RuntimeError("scheduler close callback attachment failed")
                        )
                        return failed
                if (
                    not retain_authoritative_failure
                    and public_result is authoritative_result
                    and self._opening_operation is not None
                    and not self._opening_settled
                ):
                    # There is no separate timeout view for an opening-stage close.
                    # Fork a public result before consuming the authoritative
                    # failure, so callers still observe it without leaving a late
                    # worker Failure unhandled.
                    public_view: Deferred[Any] = Deferred()

                    def publish_success(value: Any) -> Any:
                        if not public_view.called:
                            public_view.callback(value)
                        return value

                    def publish_failure(failure: Any) -> None:
                        if not public_view.called:
                            public_view.errback(failure)
                        return None

                    authoritative_result.addCallbacks(
                        publish_success,
                        publish_failure,
                    )
                    public_result = public_view
                elif (
                    not retain_authoritative_failure
                    and public_result is not authoritative_result
                ):
                    # A distinct bounded public view has its own failure callback.
                    authoritative_result.addErrback(lambda _failure: None)
                assert public_result is not None
                return public_result
            except BaseException:
                owner_token.pending = False
                with self._lifecycle_lock:
                    if self._close_attempt_owner is owner_token:
                        self._close_attempt_owner = None
                        self._close_attempt_thread_id = None
                    if self._close_completion_deferred is authoritative_result:
                        self._close_completion_deferred = None
                    self._close_retain_authoritative_failure = False
                raise
        with self._lifecycle_lock:
            if self._close_attempt_owner is owner_token:
                self._close_attempt_owner = None
                self._close_attempt_thread_id = None
            self._close_retain_authoritative_failure = False
        return None

    def _bound_close_operation(
        self,
        operation: Deferred[Any],
        *,
        operation_name: str = "scheduler connection close",
    ) -> _DeferredLifecycleResult:
        """Expose a bounded waiter while retaining the real teardown chain."""
        bounded = (
            bounded_deferred(
                operation,
                timeout=self._reactor_io_timeout,
                operation=operation_name,
            )
            if reactor_is_running()
            else operation
        )
        return _DeferredLifecycleResult(operation, bounded)

    def _handle_queue_close_failure(
        self,
        queue: BackendQueue | Any | None,
        failure: BaseException,
        *,
        lossy: bool,
    ) -> None:
        """Apply the existing queue retry/terminal policy after a worker call."""
        if lossy:
            queue_incomplete = False
        elif isinstance(queue, BackendQueue):
            queue_incomplete = not queue._close_complete
        else:
            queue_incomplete = isinstance(failure, QueueError)
        if queue_incomplete:
            raise failure
        if not isinstance(failure, Exception):
            self._terminal_queue_error = failure
        else:
            try:
                logger.error("Failed to close queue strategy during shutdown")
            except BaseException:
                pass

    def _close_locked(
        self, reason: str, *, lossy: bool = False
    ) -> _DeferredLifecycleResult | Deferred[None] | None:
        """Run one reserved teardown pass; extension callbacks run unlocked."""
        if self._lifecycle_state == _LIFECYCLE_CLOSED:
            return None

        if not self._opening_settled:
            opening = self._opening_operation
            if opening is not None:
                continuation = _chain_lifecycle_result(
                    opening,
                    lambda _ignored: self._close_locked(reason, lossy=lossy),
                    preserve_failure=lambda: self._opening_failure,
                )
                # The generic-open public view already bounds the caller's wait.
                # Keep close directly on the authoritative gate so this request
                # cannot publish before the late open operation has been fenced.
                return continuation

        if not self._queue_terminal:
            pending_settlements = self._pending_settlement_barrier()
            if pending_settlements is not None:
                # Do not release the queue generation while a replacement source
                # settlement is still authoritative. The callback performs no
                # backend work itself and re-enters this method only after the
                # worker Deferreds have settled.
                continuation = _chain_lifecycle_result(
                    pending_settlements,
                    lambda _ignored: self._close_locked(reason, lossy=lossy),
                )
                return self._bound_close_operation(
                    continuation,
                    operation_name="scheduler replacement settlement",
                )
            queue = self._queue
            if queue is not None and reactor_is_running():
                # BackendQueue.close() includes synchronous strategy and storage
                # calls. Never execute that work on Scrapy's reactor thread.
                worker = _submit_thread(
                    queue.close,
                    **({"lossy": True} if lossy else {}),
                )
                submission_failed = (
                    getattr(worker, "_reactor_submission_failure", None) == "thread"
                )

                def queue_success(_value: Any) -> Any:
                    self._queue_terminal = True
                    return self._close_after_queue(reason, lossy=lossy)

                def queue_failure(failure: Any) -> Any:
                    if submission_failed:
                        # No queue worker was accepted.  Keep the queue generation
                        # and manager ownership intact for the next close pass.
                        return failure
                    error = (
                        failure.value
                        if isinstance(failure, TwistedFailure)
                        else failure
                    )
                    self._handle_queue_close_failure(
                        queue,
                        cast(BaseException, error),
                        lossy=lossy,
                    )
                    self._queue_terminal = True
                    return self._close_after_queue(reason, lossy=lossy)

                try:
                    worker.addCallbacks(queue_success, queue_failure)
                except BaseException as exc:
                    try:
                        worker.addCallbacks(queue_success, queue_failure)
                    except BaseException:
                        failed: Deferred[Any] = Deferred()
                        failed.errback(exc)
                        worker.addErrback(lambda _failure: None)
                        return self._bound_close_operation(
                            failed,
                            operation_name="scheduler queue close",
                        )
                return self._bound_close_operation(
                    worker,
                    operation_name="scheduler queue close",
                )

            queue_close_error: BaseException | None = None
            if queue is not None:
                try:
                    queue.close(lossy=True) if lossy else queue.close()
                except BaseException as exc:
                    queue_close_error = exc
            if queue_close_error is not None:
                self._handle_queue_close_failure(
                    queue,
                    queue_close_error,
                    lossy=lossy,
                )
            self._queue_terminal = True

        remainder = self._close_after_queue(reason, lossy=lossy)
        if isinstance(remainder, Deferred):
            return self._bound_close_operation(remainder)
        return remainder

    def _close_after_queue(
        self,
        reason: str,
        *,
        lossy: bool,
    ) -> Deferred[Any] | None:
        """Release signals, filters, and managers after queue quiescence."""
        del lossy
        # Every following handle remains authoritative until its provider confirms
        # release. A failing pass leaves CLOSING and a later call resumes here.
        if self._signal_leases:
            self._disconnect_signal_leases()
        elif self._connected_signals is not None:
            # Compatibility-only state predating per-registration leases. Its
            # opaque signal manager cannot provide the stronger retry guarantee.
            handlers = self._connected_ack_signal_handlers or [
                (self._on_response_received, signals.response_received),
                (self._on_spider_error, signals.spider_error),
            ]
            for handler, signal in handlers:
                try:
                    self._connected_signals.disconnect(handler, signal=signal)
                except Exception:
                    # Compatibility-only registrations predate signal leases;
                    # keyed disconnect is intentionally best-effort here.
                    del handler
                    del signal
            self._connected_signals = None
            self._connected_ack_signal_handlers = None
            self._signals_connected = False

        if (
            self._owns_dupefilter
            and self.dupefilter is not None
            and not self._dupefilter_released
        ):
            lifecycle = _backend_dupefilter_lifecycle(self.dupefilter)
            if lifecycle is not None:
                if lifecycle.uses_release_hook:
                    authoritative_close = getattr(
                        self.dupefilter,
                        "_release_authoritative_async",
                        None,
                    )
                    close_args: tuple[Any, ...] = (
                        self._dupefilter_release_owner,
                        reason,
                    )
                else:
                    # A subclass close override is part of its public contract.
                    # Do not bypass it with BackendDupeFilter.release(); invoke it
                    # through the subclass-aware close adapter instead.
                    authoritative_close = getattr(
                        self.dupefilter,
                        "_close_authoritative_async",
                        None,
                    )
                    close_args = (reason,)
                if reactor_is_running() and callable(authoritative_close):
                    try:
                        operation, bounded_close = authoritative_close(
                            *close_args,
                            timeout=self._reactor_io_timeout,
                        )
                    except BaseException as exc:
                        # Adapter construction is part of the close attempt. A
                        # synchronous rejection must become the authoritative
                        # failed Deferred so the close owner is released and the
                        # next call can retry the same dupefilter lease.
                        failed: Deferred[Any] = Deferred()
                        failed.errback(exc)
                        return failed
                    # _close_after_queue chains the authoritative operation into
                    # scheduler teardown and therefore does not expose this
                    # nested bounded view to its caller.
                    if (
                        isinstance(bounded_close, Deferred)
                        and bounded_close is not operation
                    ):
                        bounded_close.addErrback(lambda _failure: None)

                    def finish_backend_dupefilter_close(_ignored: Any) -> Any:
                        self._dupefilter_released = True
                        self._dupefilter_open = False
                        return self._finish_close_after_dupefilter(reason)

                    try:
                        result = operation.addCallback(finish_backend_dupefilter_close)
                    except BaseException as exc:
                        try:
                            result = operation.addCallback(
                                finish_backend_dupefilter_close
                            )
                        except BaseException:
                            callback_failed: Deferred[Any] = Deferred()
                            callback_failed.errback(exc)
                            operation.addErrback(lambda _failure: None)
                            return callback_failed
                    return cast(Deferred[Any], result)
                if reactor_is_running():
                    fallback = (
                        self.dupefilter.release
                        if lifecycle.uses_release_hook
                        else self.dupefilter.close
                    )
                    operation = _submit_thread(
                        fallback,
                        *close_args,
                    )

                    def finish_fallback_dupefilter_close(_ignored: Any) -> Any:
                        self._dupefilter_released = True
                        self._dupefilter_open = False
                        # Already running on a worker thread, so release the
                        # manager synchronously and publish CLOSED without
                        # submitting another worker.
                        self._release_managers()
                        self._publish_closed(reason)
                        return None

                    try:
                        return operation.addCallback(finish_fallback_dupefilter_close)
                    except BaseException as exc:
                        try:
                            return operation.addCallback(
                                finish_fallback_dupefilter_close
                            )
                        except BaseException:
                            fallback_callback_failed: Deferred[Any] = Deferred()
                            fallback_callback_failed.errback(exc)
                            operation.addErrback(lambda _failure: None)
                            return fallback_callback_failed
                if lifecycle.uses_release_hook:
                    self.dupefilter.release(self._dupefilter_release_owner, reason)
                else:
                    # The reactor branch above is authoritative for potentially
                    # blocking subclass hooks.  Direct use without a reactor keeps
                    # the historical synchronous contract.
                    close_result = self.dupefilter.close(reason)
                    if isinstance(close_result, Deferred):

                        def finish_dupefilter_close(_ignored: Any) -> Any:
                            self._dupefilter_released = True
                            self._dupefilter_open = False
                            return self._finish_close_after_dupefilter(reason)

                        try:
                            return close_result.addCallback(finish_dupefilter_close)
                        except BaseException as exc:
                            try:
                                return close_result.addCallback(finish_dupefilter_close)
                            except BaseException:
                                direct_callback_failed: Deferred[Any] = Deferred()
                                direct_callback_failed.errback(exc)
                                close_result.addErrback(lambda _failure: None)
                                return direct_callback_failed
            else:
                # Generic Scrapy dupefilters receive the standard close(reason)
                # hook.  Scrapy 2.17 permits that hook to return a Deferred; keep
                # every later release behind it so a delayed close cannot race the
                # manager teardown.
                close_result = self.dupefilter.close(reason)
                if isinstance(close_result, Deferred):

                    def finish_dupefilter_close(_ignored: Any) -> Any:
                        self._dupefilter_released = True
                        self._dupefilter_open = False
                        return self._finish_close_after_dupefilter(reason)

                    try:
                        return close_result.addCallback(finish_dupefilter_close)
                    except BaseException as exc:
                        try:
                            return close_result.addCallback(finish_dupefilter_close)
                        except BaseException:
                            generic_callback_failed: Deferred[Any] = Deferred()
                            generic_callback_failed.errback(exc)
                            close_result.addErrback(lambda _failure: None)
                            return generic_callback_failed
            self._dupefilter_released = True
            self._dupefilter_open = False

        return self._finish_close_after_dupefilter(reason)

    def _release_managers(self) -> None:
        """Release every owned acquire, preserving the first failure."""
        primary_error: BaseException | None = None
        if (
            self._owns_snapshot_connection_manager
            and self._snapshot_connection_manager is not None
            and not self._snapshot_manager_released
        ):
            try:
                if self._snapshot_connection_manager_lease is not None:
                    self._snapshot_connection_manager_lease.release()
                else:
                    self._snapshot_connection_manager.close()
            except BaseException as exc:
                primary_error = exc
            else:
                self._snapshot_manager_released = True

        if self._owns_connection_manager and not self._manager_released:
            try:
                if self._connection_manager_lease is not None:
                    self._connection_manager_lease.release()
                else:
                    self.connection_manager.close()
            except BaseException as exc:
                if primary_error is None:
                    primary_error = exc
            else:
                self._manager_released = True

        if primary_error is not None:
            raise primary_error

    def _publish_closed(self, reason: str) -> None:
        """Publish CLOSED only after every manager release has completed."""
        # No ownership handle remains. Publish final package state under the lock;
        # every provider callback above completed while it was released.
        with self._lifecycle_lock:
            self._queue = None
            self._spider = None
            self._connected_signals = None
            self._connected_ack_signal_handlers = None
            self._signals_connected = False
            self._backpressure_paused = False
            self._backpressure_probe_due = False
            self._opening_operation = None
            self._opening_settled = True
            self._opening_failure = None
            self._open_close_requested = False
            self._lifecycle_state = _LIFECYCLE_CLOSED
        try:
            logger.info("Scheduler closed: %s", reason)
        except BaseException:
            pass

        terminal_error = self._terminal_queue_error
        self._terminal_queue_error = None
        if terminal_error is not None:
            raise terminal_error

    def _finish_close_after_dupefilter(self, reason: str) -> Deferred[Any] | None:
        """Release managers off-reactor, then publish CLOSED."""
        if reactor_is_running():
            operation = _submit_thread(self._release_managers)
            callback = lambda _value: self._publish_closed(reason)
            try:
                return operation.addCallback(callback)
            except BaseException as exc:
                try:
                    return operation.addCallback(callback)
                except BaseException:
                    failed: Deferred[Any] = Deferred()
                    failed.errback(exc)
                    operation.addErrback(lambda _failure: None)
                    return failed

        self._release_managers()
        self._publish_closed(reason)
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
            True if the request was enqueued, False on duplicate or deterministic
            serialization rejection. Transient queue/backend push failures raise
            ``QueueError`` after dedup compensation so Scrapy cannot classify them
            as ``request_dropped``.
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
        settle_legacy_reservation: Callable[[Request], Any] | None = None
        deferred_diagnostics: list[_EnqueueDiagnostic] = []
        ordinary_outcome: bool | None = None
        retry_after_dupefilter_failure = False
        terminal_push_failure = False
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
                        # Pre-arm the rollback gate BEFORE the interruptible call: an
                        # add-on-check filter (bundled BackendDupeFilter's legacy arm, and
                        # the common SADD-style custom filter) has ALREADY recorded the
                        # fingerprint inside ``request_seen`` above. ``dedup_reserved`` is
                        # the gate every cleanup arm uses to call ``forget``; assigning it
                        # only after ``consume_reservation`` returns would leave it ``False``
                        # if a BaseException (or any non-(QueueError/BackendError/
                        # SerializationError) exception) lands during that call, leaking the
                        # fingerprint as a permanent ghost marker. Pre-arm, then refine on
                        # success — ``_rollback_dupefilter_reservation``→``forget`` is
                        # guarded for the filter-full-miss case where nothing was recorded.
                        dedup_reserved = True
                        dedup_reserved = (
                            bool(consume_reservation(request))
                            if callable(consume_reservation)
                            else True
                        )
                        candidate_settle = getattr(
                            self.dupefilter,
                            "settle_reservation",
                            None,
                        )
                        if callable(candidate_settle):
                            settle_legacy_reservation = candidate_settle
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
            if settle_legacy_reservation is not None and dedup_reserved:
                # The queue push is now the durable handoff.  Clear the rollback
                # gate before this bookkeeping-only call so a process-control
                # interruption cannot compensate a request that is already queued.
                dedup_reserved = False
                try:
                    settle_legacy_reservation(request)
                except Exception:  # noqa: BLE001 - durable enqueue wins
                    self._record_stat("scheduler/dupefilter_settle_error")

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
                        self._record_stat("scheduler/dupefilter_volatile_marker")
                    else:
                        self._rollback_atomic_reservation(
                            completed_reservation,
                            rollback_reservation,
                            preserve_primary=False,
                        )
                        self._record_stat("scheduler/dupefilter_volatile_unmarked")
                else:
                    self._commit_atomic_reservation(
                        completed_reservation,
                        commit_reservation,
                    )
                # Retain the owner intent until the commit/rollback call returns. If a
                # process-control signal interrupts finalization, the outer handler can
                # still discard bookkeeping without touching an ambiguous marker.
                reservation_intent = None
            self._record_stat("scheduler/enqueued")
        except SerializationError:
            if reservation is not None and rollback_reservation is not None:
                self._rollback_atomic_reservation(
                    reservation,
                    rollback_reservation,
                    preserve_primary=False,
                    deferred_diagnostics=deferred_diagnostics,
                )
            elif (
                reservation_intent is not None
                and rollback_reservation_intent is not None
            ):
                self._rollback_atomic_reservation(
                    reservation_intent,
                    rollback_reservation_intent,
                    preserve_primary=False,
                    deferred_diagnostics=deferred_diagnostics,
                )
            elif dedup_reserved:
                self._rollback_dupefilter_reservation(
                    request,
                    deferred_diagnostics=deferred_diagnostics,
                )
            deferred_diagnostics.append(
                (
                    "error",
                    "Failed to serialize request for enqueue",
                    "scheduler/serialization_errors",
                )
            )
            ordinary_outcome = False
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
                        deferred_diagnostics=deferred_diagnostics,
                    )
                # Dedup-backend outage: degrade to enqueue (don't lose the URL),
                # attribute to the dedup-error stat. The fallback push itself moves
                # below the handler so its diagnostic code cannot inherit this raw
                # dupefilter failure through ``sys.exc_info()``.
                deferred_diagnostics.append(
                    (
                        "error",
                        "Failed to consult dupefilter; defaulting to enqueue",
                        "scheduler/dupefilter_error",
                    )
                )
                retry_after_dupefilter_failure = True
            else:
                # phase == "push": a plain queue-push failure (not a dedup outage).
                if reservation is not None and rollback_reservation is not None:
                    self._rollback_atomic_reservation(
                        reservation,
                        rollback_reservation,
                        preserve_primary=False,
                        deferred_diagnostics=deferred_diagnostics,
                    )
                elif (
                    reservation_intent is not None
                    and rollback_reservation_intent is not None
                ):
                    self._rollback_atomic_reservation(
                        reservation_intent,
                        rollback_reservation_intent,
                        preserve_primary=False,
                        deferred_diagnostics=deferred_diagnostics,
                    )
                elif dedup_reserved:
                    self._rollback_dupefilter_reservation(
                        request,
                        deferred_diagnostics=deferred_diagnostics,
                    )
                deferred_diagnostics.append(
                    ("error", "Failed to enqueue request", "scheduler/queue_error")
                )
                # A queue/backend push failure is not a deterministic rejection.
                # Returning False makes Scrapy emit request_dropped without a retry,
                # which can lose the request even after the dedup reservation was
                # correctly rolled back.  Keep False for duplicates and
                # SerializationError only; publish a terminal error after all
                # compensation diagnostics have been prepared.
                terminal_push_failure = True
        except BaseException:
            # A queue monitor runs after the physical push commits, but its
            # process-control interruption must not make the scheduler roll back
            # a dedup reservation as if the push had failed. Discard only the
            # owner intent bookkeeping; the queue item remains authoritative and
            # the original signal remains observable to the caller.
            post_commit_push = False
            if (
                phase == "push"
                and _static_declaration_rank(
                    queue,
                    "_consume_post_commit_push",
                )
                is not None
            ):
                consume_commit = getattr(queue, "_consume_post_commit_push")
                if callable(consume_commit):
                    post_commit_push = bool(consume_commit())
            try:
                if post_commit_push:
                    if settle_legacy_reservation is not None and dedup_reserved:
                        # A queue monitor may interrupt after the physical push.
                        # Treat that boundary like the normal successful handoff;
                        # never leave a legacy receipt behind or compensate a
                        # marker for work already present in the queue.
                        dedup_reserved = False
                        try:
                            settle_legacy_reservation(request)
                        except Exception:  # noqa: BLE001 - preserve primary signal
                            deferred_diagnostics.append(
                                (
                                    "error",
                                    "Failed to settle legacy dupefilter handoff",
                                    "scheduler/dupefilter_settle_error",
                                )
                            )
                        except BaseException:
                            # The queue commit remains authoritative; preserve the
                            # queue monitor's process-control signal. Close will
                            # cancel any receipt that an interrupted setter left.
                            pass
                    if (
                        reservation_intent is not None
                        and rollback_reservation_intent is not None
                    ):
                        rollback_reservation_intent(reservation_intent)
                    reservation = None
                    reservation_intent = None
                elif (
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
                        deferred_diagnostics=deferred_diagnostics,
                    )
                elif dedup_reserved:
                    self._rollback_dupefilter_reservation(
                        request,
                        preserve_primary=True,
                        deferred_diagnostics=deferred_diagnostics,
                    )
            except BaseException:
                # This outer guard also covers an asynchronous signal before the
                # cleanup callee establishes its own try-region. Retry the silent
                # owner fence once; it is idempotent and does not mutate membership.
                if (
                    (post_commit_push or reservation_intent is not None)
                    and rollback_reservation_intent is not None
                    and reservation_intent is not None
                ):
                    try:
                        rollback_reservation_intent(reservation_intent)
                    except BaseException:
                        pass
            raise

        if retry_after_dupefilter_failure:
            # All of the dupefilter failure handling above has unwound. Preserve the
            # historical diagnostic-before-fallback ordering without exposing the
            # raw failure to logger or stats extensions.
            self._flush_enqueue_diagnostics(deferred_diagnostics)
            try:
                queue.push(request, priority=priority)
            except SerializationError:
                self._record_enqueue_diagnostic(
                    "error",
                    "Failed to serialize request after dedup outage",
                    stat="scheduler/serialization_errors",
                )
                return False
            except (QueueError, BackendError):
                self._record_enqueue_diagnostic(
                    "error",
                    "Failed to enqueue request after dedup outage",
                    stat="scheduler/queue_error",
                )
                raise QueueError(
                    "Queue push failed after dedup outage; request was not dropped.",
                    operation="push",
                ) from None
            self._record_stat("scheduler/enqueued")
            return True

        self._flush_enqueue_diagnostics(deferred_diagnostics)
        if terminal_push_failure:
            raise QueueError(
                "Queue push failed; request was not dropped and must be retried.",
                operation="push",
            ) from None
        if ordinary_outcome is not None:
            return ordinary_outcome
        return True

    def _rollback_atomic_reservation(
        self,
        reservation: object,
        rollback: Callable[[object], None],
        *,
        preserve_primary: bool,
        deferred_diagnostics: list[_EnqueueDiagnostic] | None = None,
    ) -> None:
        """Roll back one receipt with explicit process-control precedence."""
        rollback_failed = False
        control_rollback_failed = False
        try:
            rollback(reservation)
        except Exception:  # noqa: BLE001 - preserve the triggering queue failure
            rollback_failed = True
        except BaseException:
            if not preserve_primary:
                raise
            control_rollback_failed = True
        if rollback_failed or control_rollback_failed:
            self._record_or_defer_enqueue_diagnostic(
                deferred_diagnostics,
                "error",
                "Failed to roll back atomic dupefilter reservation",
                stat="scheduler/dupefilter_rollback_error",
            )

    def _commit_atomic_reservation(
        self,
        reservation: object,
        commit: Callable[[object], None],
        *,
        deferred_diagnostics: list[_EnqueueDiagnostic] | None = None,
    ) -> None:
        """Finalize receipt bookkeeping after the queue commit boundary.

        An ordinary bookkeeping failure cannot reclassify an already durable push.
        Process-control signals still propagate, with caller state already cleared
        so the outer handler cannot roll back the committed marker.
        """
        commit_failed = False
        try:
            commit(reservation)
        except Exception:  # noqa: BLE001 - queue durability is authoritative
            commit_failed = True
        if commit_failed:
            self._record_or_defer_enqueue_diagnostic(
                deferred_diagnostics,
                "error",
                "Failed to finalize atomic dupefilter reservation",
                stat="scheduler/dupefilter_commit_error",
            )

    def _rollback_dupefilter_reservation(
        self,
        request: Request,
        *,
        preserve_primary: bool = False,
        deferred_diagnostics: list[_EnqueueDiagnostic] | None = None,
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
            self._record_or_defer_enqueue_diagnostic(
                deferred_diagnostics,
                "warning",
                "Dupefilter cannot roll back a fingerprint after queue push failure",
                stat="scheduler/dupefilter_rollback_error",
            )
            return

        rollback_failed = False
        control_rollback_failed = False
        try:
            forget(request)
        except Exception:  # noqa: BLE001 - preserve the triggering queue failure
            rollback_failed = True
        except BaseException:  # compensation must not hide process-control primary
            if not preserve_primary:
                raise
            control_rollback_failed = True
        if rollback_failed or control_rollback_failed:
            self._record_or_defer_enqueue_diagnostic(
                deferred_diagnostics,
                "error",
                "Failed to roll back dupefilter reservation",
                stat="scheduler/dupefilter_rollback_error",
            )

    def _record_or_defer_enqueue_diagnostic(
        self,
        deferred_diagnostics: list[_EnqueueDiagnostic] | None,
        level: str,
        message: str,
        *,
        stat: str | None = None,
    ) -> None:
        """Record a fixed continuation diagnostic now or after an outer catch."""
        if deferred_diagnostics is None:
            self._record_enqueue_diagnostic(level, message, stat=stat)
            return
        deferred_diagnostics.append((level, message, stat))

    def _flush_enqueue_diagnostics(
        self,
        deferred_diagnostics: list[_EnqueueDiagnostic],
    ) -> None:
        """Dispatch diagnostics after the enclosing operational error unwinds."""
        for level, message, stat in deferred_diagnostics:
            self._record_enqueue_diagnostic(level, message, stat=stat)

    def _record_stat(self, key: str) -> None:
        """Record advisory scheduler telemetry without changing the result.

        Stats collection is intentionally not a scheduling or settlement control.
        A collector may fail with an ordinary exception or a process-control
        ``BaseException``; after a scheduler outcome is established, neither may
        rewrite it. Direct queue and process-control calls stay outside this
        helper and retain their normal propagation contract.
        """
        try:
            if self.stats:
                self.stats.inc_value(key)
        except BaseException:
            pass

    def _record_enqueue_diagnostic(
        self,
        level: str,
        message: str,
        *args: object,
        stat: str | None = None,
    ) -> None:
        """Emit failure diagnostics without replacing a settled enqueue outcome."""
        try:
            logger_method = (
                logger.error if level == "exception" else getattr(logger, level)
            )
            logger_method(message, *args)
        except BaseException:
            pass
        if stat is not None:
            self._record_stat(stat)

    def next_request(self) -> Request | None:
        """Get the next request from the queue.

        Returns:
            The next request, or None if the queue is empty or paused under the
            backpressure gate.
        """
        fallback_diagnostic: tuple[str, str | None] | None = None
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
                        self._record_stat("scheduler/backpressure_pause")
                        return None
                    if self._backpressure_paused:
                        if depth <= resume_at:
                            self._backpressure_paused = False
                            self._backpressure_probe_due = False
                            self._record_stat("scheduler/backpressure_resume")
                        elif self._backpressure_probe_due:
                            self._backpressure_probe_due = False
                        else:
                            self._backpressure_probe_due = True
                            return (
                                None  # paused — next poll is the bounded progress probe
                            )
            request = queue.pop(timeout=0)
            if request:
                self._wrap_download_failure(request)
                self._record_stat("scheduler/dequeued")
        except SerializationError:
            fallback_diagnostic = (
                "Failed to deserialize queued request",
                "scheduler/deserialization_errors",
            )
        except (QueueError, BackendConnectionError, CircuitBreakerOpenError):
            fallback_diagnostic = ("Failed to get next request", None)

        if fallback_diagnostic is not None:
            # The queue error has left its exception suite before telemetry runs, so
            # neither a logging handler nor stats collector can access its raw
            # traceback through ``sys.exc_info()``.
            message, stat = fallback_diagnostic
            try:
                logger.error(message)
            except BaseException:
                pass
            if stat is not None:
                self._record_stat(stat)
            return None
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
            pass

        # The length failure has unwound before the diagnostic handler is invoked,
        # preventing it from recovering backend details through ``sys.exc_info()``.
        try:
            logger.warning(
                "Queue length lookup is unavailable; assuming pending requests exist"
            )
        except BaseException:
            pass
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
