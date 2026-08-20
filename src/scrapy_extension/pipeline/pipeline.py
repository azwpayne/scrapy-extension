"""Pipeline component for scrapy-extension.

This module provides a Scrapy item pipeline using backend storage interfaces.
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from functools import cached_property, wraps
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from itemadapter import ItemAdapter, is_item
from twisted.internet.defer import Deferred, succeed
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure as TwistedFailure

from scrapy_extension.backends.base import JSONSerializer, _validate_key_name
from scrapy_extension.exceptions import (
    BackendConnectionError,
    BackendError,
    ConfigurationError,
    SerializationError,
    StorageBackpressureError,
    StorageError,
)
from scrapy_extension.exceptions._redaction import (
    serialization_error_boundary,
    storage_operation_error_boundary,
)
from scrapy_extension.monitor.base import Monitor, NullMonitor
from scrapy_extension.storage.strategies import (
    StorageStrategy,
    create_storage_strategy,
)
from scrapy_extension.storage.strategies.passthrough import (
    PassthroughStorageStrategy,
)
from scrapy_extension.utils._config import parse_float_setting, parse_int_setting
from scrapy_extension.utils.policy import DEFAULT_PIPELINE_MAX_STORAGE_ERRORS
from scrapy_extension.utils.reactor import (
    DEFAULT_REACTOR_IO_TIMEOUT_S,
    MAX_REACTOR_IO_TIMEOUT_S,
    bounded_deferred,
    defer_to_thread_ordered,
    reactor_is_running,
)

if TYPE_CHECKING:
    from scrapy import Spider
    from scrapy.crawler import Crawler
    from scrapy.settings import Settings

    from scrapy_extension.backends.connectors import (
        ConnectionManager,
        ConnectionManagerLease,
    )

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")
_T = TypeVar("_T")

#: Default per-item serialized-byte cap (1 MiB — matches Memcached's 1 MB ceiling).
DEFAULT_PIPELINE_MAX_ITEM_BYTES = 1_048_576

#: Risk 5 — default ceiling on consecutive storage errors before the pipeline
#: re-raises a fixed ``BackendError`` instead of swallowing forever. The
#: pre-Risk-5 from_settings default was ``None`` (infinite swallow), which meant
#: a persistent storage outage was silently absorbed as success-shaped item
#: returns — silent data loss at fleet scale. ``10`` surfaces a sustained
#: outage loudly after 11 consecutive failures while tolerating transient
#: blips. Operators who want the old infinite-swallow behavior can pass
#: ``max_storage_errors=None`` to the constructor directly.
DEFAULT_MAX_STORAGE_ERRORS = DEFAULT_PIPELINE_MAX_STORAGE_ERRORS

_PIPELINE_STORAGE_FAILURE_MESSAGE = "Pipeline storage operation failed."
_PIPELINE_STORAGE_THRESHOLD_MESSAGE = "Pipeline storage failure threshold exceeded."
_PIPELINE_SERIALIZATION_FAILURE_MESSAGE = "Failed to serialize item."
_PIPELINE_SERIALIZATION_MONITOR_FAILURE = "Pipeline item serialization failed."
_PIPELINE_CLOSE_FAILURE_MESSAGE = "Pipeline storage close failed."
_BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE = "Batched storage flush failed."


class _PipelineStorageFailureThresholdExceeded(Exception):
    """Private control signal selecting the public storage threshold error."""


def _pipeline_store_error_boundary(
    function: Callable[_P, _T],
) -> Callable[_P, _T]:
    """Rebuild pipeline store-path terminal errors outside private frames.

    ``process_item`` keeps the item, generated key, serialized payload, and
    storage strategy in its implementation frames.  Re-raising a backpressure
    result or threshold failure directly from that path would expose those
    values through traceback inspection even when its message is static.  The
    wrapped public boundary waits until those frames unwind, then constructs a
    fresh, fixed public error. An outer serialization boundary handles the
    separately versioned serialization contract after this wrapper releases its
    own frames. Lifecycle failures deliberately pass through unchanged.
    """

    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        backpressure = False
        threshold_exceeded = False
        try:
            return function(*args, **kwargs)
        except StorageBackpressureError:
            backpressure = True
        except _PipelineStorageFailureThresholdExceeded:
            threshold_exceeded = True
        except BaseException:
            del args
            del kwargs
            raise

        del args
        del kwargs
        if backpressure:
            del backpressure
            del threshold_exceeded
            raise StorageBackpressureError(operation="store")

        assert threshold_exceeded
        del backpressure
        del threshold_exceeded
        raise BackendError(_PIPELINE_STORAGE_THRESHOLD_MESSAGE)

    return wrapped


def _emit_diagnostic(method: Any, message: str, *args: Any, **kwargs: Any) -> None:
    """Emit non-semantic logging without letting a handler alter control flow."""
    try:
        method(message, *args, **kwargs)
    except BaseException:
        # A logger handler is observability infrastructure.  Its cancellation-style
        # failures must not turn a successfully published pipeline or an otherwise
        # best-effort storage outcome into a lifecycle failure.
        pass


class BackendPipeline:
    """Scrapy item pipeline using backend storage interface.

    This pipeline stores items in the backend storage with optional TTL.

    Attributes:
        connection_manager: The connection manager for backend access.
        key_prefix: Prefix for stored item keys.
        ttl: Optional TTL in seconds for items.
        serializer: Serializer for item encoding.
        storage_strategy: Strategy layer governing how items reach the backend
            (passthrough default — byte-identical to pre-strategy behavior;
            ``batched`` buffers + flushes on threshold/close).
        max_storage_errors: C2 escalation threshold. ``10`` (default) re-raises a
            fixed :class:`~scrapy_extension.exceptions.BackendError` after the
            eleventh consecutive failure. Pass ``None`` explicitly to opt into
            best-effort swallow-and-stat behavior (counter resets on a successful
            store).
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        key_prefix: str = "items",
        ttl: int | None = None,
        max_item_bytes: int = DEFAULT_PIPELINE_MAX_ITEM_BYTES,
        storage_strategy: StorageStrategy | None = None,
        *,
        max_storage_errors: int | None = DEFAULT_MAX_STORAGE_ERRORS,
        monitor: Monitor | None = None,
        owns_connection_manager: bool = True,
        connection_manager_lease: ConnectionManagerLease | None = None,
        reactor_io_timeout: float = DEFAULT_REACTOR_IO_TIMEOUT_S,
    ) -> None:
        """Initialize the pipeline.

        Args:
            connection_manager: Connection manager for backend access.
            key_prefix: Prefix for stored item keys.
            ttl: Optional TTL in seconds for items.
            max_item_bytes: Maximum serialized bytes permitted for a single stored
                item. Oversize payloads raise ``SerializationError`` at store time
                (D2 — DoS guard against capped storage backends like Memcached
                1 MB, DynamoDB 400 KB).
            storage_strategy: Persistence strategy. ``None`` defaults to
                :class:`PassthroughStorageStrategy` (byte-identical to the
                pre-strategy store call). Selected via ``SCRAPY_STORAGE_STRATEGY``
                in :meth:`from_settings`.
            max_storage_errors: C2 escalation. ``10`` (default) tracks consecutive
                failures and re-raises a fixed
                :class:`~scrapy_extension.exceptions.BackendError` on the eleventh
                failure. Pass ``None`` explicitly to opt into best-effort behavior
                (storage errors swallowed, item returned,
                ``pipeline/storage_errors`` stat incremented). The counter resets
                to 0 on every successful store.
            monitor: Optional observability monitor. When ``None`` (default),
                :class:`~scrapy_extension.monitor.base.NullMonitor` (no-op). Wired
                to a :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` in
                :meth:`from_crawler` when ``crawler.stats`` is available, so the
                ``pipeline/store_count`` stat is default-on. Emitted hooks are
                additive — existing component stats untouched.
            owns_connection_manager: Whether close releases the supplied manager.
                Composite owners can pass ``False`` and release their shared acquire
                after all borrowed components are closed.
            reactor_io_timeout: Maximum caller-visible wait for a synchronous backend
                operation moved to Twisted's thread pool. The underlying operation
                remains ordered until its worker returns if this budget expires.
        """
        _validate_key_name(key_prefix, "key_prefix")
        self.connection_manager = connection_manager
        self.key_prefix = key_prefix
        self.ttl = ttl
        self.max_item_bytes = max_item_bytes
        self.storage_strategy: StorageStrategy = (
            storage_strategy
            if storage_strategy is not None
            else PassthroughStorageStrategy()
        )
        self.max_storage_errors = max_storage_errors
        self._consecutive_storage_errors = 0
        self._storage_supported: bool | None = None
        self._monitor: Monitor = monitor if monitor is not None else NullMonitor()
        self._owns_connection_manager = owns_connection_manager
        self._connection_manager_lease = connection_manager_lease
        self._reactor_io_timeout = reactor_io_timeout
        self._async_tail: Deferred[Any] = Deferred()
        self._async_tail.callback(None)
        self._manager_released = False
        self._lifecycle_lock = threading.Lock()
        self._store_condition = threading.Condition(self._lifecycle_lock)
        self._opened = False
        self._opened_spider: Spider | None = None
        self._crawler: Crawler | None = None
        self._closed = False
        self._closing = False
        self._close_owner_thread_id: int | None = None
        self._close_async_pending = False
        self._close_waiting_for_stores = False
        self._pending_close_error: BaseException | None = None
        self._active_store_count = 0
        self._active_store_threads: dict[int, int] = {}
        self._lifecycle_generation = 0
        self._opening_generation: int | None = None
        self._opening_owner_thread_id: int | None = None
        self._open_close_requested = False
        self._opening = False
        self._opening_operation: Deferred[None] | None = None
        self._opening_failure: TwistedFailure | None = None
        set_monitor = getattr(self.storage_strategy, "set_monitor", None)
        if callable(set_monitor):
            set_monitor(self._monitor)

    @cached_property
    def _serializer(self) -> JSONSerializer:
        """Lazy-initialized JSON serializer."""
        return JSONSerializer()

    @classmethod
    def from_settings(cls, settings: Settings) -> BackendPipeline:
        """Create pipeline from Scrapy settings.

        Backend selection: ``SCRAPY_STORAGE_BACKEND_TYPE`` /
        ``SCRAPY_STORAGE_BACKEND_SETTINGS`` override the global
        ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so item storage
        can bind to a different backend than the queue or dedup set
        (multi-backend coexistence). Unset → falls back to the global keys.

        Args:
            settings: Scrapy settings object.

        Returns:
            A new BackendPipeline instance.
        """
        from scrapy_extension.backends.connectors import (
            ConnectionManager,
            release_manager_acquire,
            resolve_backend_config,
        )

        backend_type, backend_settings = resolve_backend_config(
            settings,
            type_key="SCRAPY_STORAGE_BACKEND_TYPE",
            settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
            required_capabilities={"storage"},
            component_name="storage",
        )
        manager = ConnectionManager.get_manager(
            backend_type=backend_type,
            settings=backend_settings,
        )
        manager_lease = ConnectionManager._adopt_latest_legacy_lease(manager)
        try:
            storage_strategy_name = settings.get(
                "SCRAPY_STORAGE_STRATEGY", "passthrough"
            )
            # Risk 2: thread the crash-before-flush loss-window cap through to the
            # batched strategy (None = disabled = pre-Risk-2 behavior; passthrough
            # ignores it).
            raw_age = settings.get("SCRAPY_STORAGE_BUFFER_MAX_AGE_S")
            buffer_max_age_s = (
                parse_float_setting(
                    raw_age,
                    "SCRAPY_STORAGE_BUFFER_MAX_AGE_S",
                    minimum=0.0,
                    minimum_exclusive=True,
                )
                if raw_age is not None
                else None
            )
            raw_max_pending = settings.get("SCRAPY_STORAGE_BUFFER_MAX_PENDING")
            buffer_max_pending = (
                parse_int_setting(
                    raw_max_pending,
                    "SCRAPY_STORAGE_BUFFER_MAX_PENDING",
                    minimum=1,
                )
                if raw_max_pending is not None
                else None
            )
            try:
                storage_strategy = create_storage_strategy(
                    storage_strategy_name,
                    max_buffer_age_s=buffer_max_age_s,
                    max_pending=buffer_max_pending,
                )
            except ConfigurationError as exc:
                if exc.setting_name == "storage_strategy":
                    raise ConfigurationError(
                        str(exc),
                        setting_name="SCRAPY_STORAGE_STRATEGY",
                        setting_value=storage_strategy_name,
                    ) from exc
                if exc.setting_name == "max_pending":
                    raise ConfigurationError(
                        str(exc),
                        setting_name="SCRAPY_STORAGE_BUFFER_MAX_PENDING",
                        setting_value=raw_max_pending,
                    ) from exc
                raise
            except (TypeError, ValueError, OverflowError) as exc:
                raise ConfigurationError(
                    f"Invalid SCRAPY_STORAGE_STRATEGY configuration: {exc}",
                    setting_name="SCRAPY_STORAGE_STRATEGY",
                    setting_value=storage_strategy_name,
                ) from exc
            raw_max_errors = settings.get("SCRAPY_PIPELINE_MAX_STORAGE_ERRORS")
            # The reliability-safe ceiling is the public default. An explicitly
            # stored None is the documented opt-in to best-effort loss; distinguish
            # it from an unset Scrapy setting.
            try:
                max_errors_priority = settings.getpriority(
                    "SCRAPY_PIPELINE_MAX_STORAGE_ERRORS"
                )
            except (AttributeError, TypeError):
                max_errors_priority = None
            explicitly_configured = isinstance(max_errors_priority, int)
            if explicitly_configured and raw_max_errors is None:
                max_storage_errors = None
            elif explicitly_configured:
                max_storage_errors = parse_int_setting(
                    raw_max_errors,
                    "SCRAPY_PIPELINE_MAX_STORAGE_ERRORS",
                    minimum=0,
                )
            else:
                max_storage_errors = DEFAULT_MAX_STORAGE_ERRORS
            ttl_raw = settings.get("SCRAPY_PIPELINE_TTL", 0)
            ttl = parse_int_setting(
                ttl_raw,
                "SCRAPY_PIPELINE_TTL",
                minimum=0,
            )
            max_item_bytes = parse_int_setting(
                settings.get(
                    "SCRAPY_PIPELINE_MAX_ITEM_BYTES",
                    DEFAULT_PIPELINE_MAX_ITEM_BYTES,
                ),
                "SCRAPY_PIPELINE_MAX_ITEM_BYTES",
                minimum=1,
            )
            reactor_io_timeout = parse_float_setting(
                settings.get(
                    "SCRAPY_REACTOR_IO_TIMEOUT",
                    DEFAULT_REACTOR_IO_TIMEOUT_S,
                ),
                "SCRAPY_REACTOR_IO_TIMEOUT",
                minimum=0.0,
                minimum_exclusive=True,
                maximum=MAX_REACTOR_IO_TIMEOUT_S,
            )
            key_prefix = settings.get("SCRAPY_PIPELINE_KEY_PREFIX", "items")
            if not isinstance(key_prefix, str):
                raise ConfigurationError(
                    f"SCRAPY_PIPELINE_KEY_PREFIX must be a string, got {key_prefix!r}.",
                    setting_name="SCRAPY_PIPELINE_KEY_PREFIX",
                    setting_value=key_prefix,
                )
            try:
                _validate_key_name(key_prefix, "SCRAPY_PIPELINE_KEY_PREFIX")
            except ValueError as exc:
                raise ConfigurationError(
                    str(exc),
                    setting_name="SCRAPY_PIPELINE_KEY_PREFIX",
                    setting_value=key_prefix,
                ) from exc
            return cls(
                connection_manager=manager,
                key_prefix=key_prefix,
                ttl=ttl or None,
                max_item_bytes=max_item_bytes,
                storage_strategy=storage_strategy,
                max_storage_errors=max_storage_errors,
                connection_manager_lease=manager_lease,
                reactor_io_timeout=reactor_io_timeout,
            )
        except BaseException:
            # A successful get_manager() is an acquire. No partially-built component
            # exists to own that reference, so the factory must release it here even
            # for cancellation-style BaseException subclasses.
            try:
                if manager_lease is not None:
                    release_manager_acquire(manager_lease, exact=True)
                else:
                    release_manager_acquire(manager)
            except BaseException:
                try:
                    logger.exception(
                        "Failed to release ConnectionManager after pipeline factory failure"
                    )
                except BaseException:
                    # Diagnostics must not replace the factory failure being unwound.
                    pass
            raise

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> BackendPipeline:
        """Create pipeline from crawler.

        Default-on observability: wires a
        :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` when
        ``crawler.stats`` is available so the ``pipeline/store_count`` stat shows
        up on the Scrapy stats dump without an explicit ``monitor=`` kwarg.
        Additive — existing stats (``pipeline/storage_errors`` etc.) untouched.

        Args:
            crawler: The Scrapy crawler instance.

        Returns:
            A new BackendPipeline instance.
        """
        pipeline = cls.from_settings(crawler.settings)
        pipeline._crawler = crawler
        factory_failure: BaseException | None = None
        try:
            # Default-on observability — mirrors the dupefilter wiring. Only override
            # when no explicit monitor was provided (operators passing a custom monitor
            # via from_settings win over the default).
            stats = getattr(crawler, "stats", None)
            if stats is not None and isinstance(pipeline._monitor, NullMonitor):
                from scrapy_extension.monitor import ScrapyStatsMonitor

                pipeline._monitor = ScrapyStatsMonitor(stats)
            # Risk 2: share the pipeline's monitor with the storage strategy so
            # ``on_buffer_depth`` (batched) emits through the same collector. No-op
            # for strategies without a ``set_monitor`` hook (passthrough).
            set_monitor = getattr(pipeline.storage_strategy, "set_monitor", None)
            if callable(set_monitor):
                set_monitor(pipeline._monitor)
            # R25-F: also thread the monitor into the pipeline's ConnectionManager so
            # backend/{connect,disconnect,retry}_count cover the storage backend in
            # multi-backend deployments (queue≠storage). All component monitors wrap
            # the same crawler.stats, so counters aggregate across managers.
            pipeline.connection_manager.set_monitor(pipeline._monitor)
        except BaseException as exc:
            factory_failure = exc

        if factory_failure is None:
            return pipeline

        # The pipeline was never published and therefore admitted no durable work.
        # Run rollback after the factory exception suite has unwound so cleanup and
        # diagnostics cannot inspect or replace that original failure.
        cleanup_failed = False
        try:
            pipeline._close_after_factory_failure()
        except BaseException:
            cleanup_failed = True
        if cleanup_failed:
            _emit_diagnostic(
                logger.error,
                "Failed to close pipeline after crawler factory failure",
            )
        raise factory_failure

    def _close_after_factory_failure(self) -> None:
        """Close an unpublished pipeline when ``from_crawler`` cannot return it."""
        self._close_locked(force_manager_release=True)

    def _resolve_spider(self, spider: Spider | None) -> Spider:
        """Resolve old explicit-spider and new crawler-owned Scrapy calls."""
        if spider is not None:
            return spider
        if self._opened_spider is not None:
            return self._opened_spider
        crawler_spider = self._crawler.spider if self._crawler is not None else None
        if crawler_spider is None:
            raise RuntimeError(
                "BackendPipeline has no spider; construct it with from_crawler() or "
                "pass spider explicitly"
            )
        return crawler_spider

    def open_spider(self, spider: Spider | None = None) -> Deferred[None] | None:
        """Open synchronously outside the reactor, or return a bounded Deferred.

        Scrapy accepts a Deferred lifecycle hook.  The synchronous backend
        capability probe is therefore moved to the thread pool while the legacy
        direct-call contract remains unchanged outside a running reactor.
        """
        try:
            if reactor_is_running():
                result = self._open_spider_async(spider)
                del spider
                return result
            self._open_spider_sync(spider)
        except BaseException:
            del self
            del spider
            raise
        del spider
        return None

    def _open_spider_async(self, spider: Spider | None) -> Deferred[None]:
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("pipeline is closed")
            if self._closing or self._opening:
                raise RuntimeError(
                    "pipeline lifecycle transition is already in progress"
                )
            resolved_spider = self._resolve_spider(spider)
            self._lifecycle_generation += 1
            opening_generation = self._lifecycle_generation
            self._opening = True
            self._opening_generation = opening_generation
            self._opening_owner_thread_id = None
            self._open_close_requested = False
            self._opening_failure = None
        operation = deferToThread(
            self._open_spider_sync,
            resolved_spider,
            opening_generation,
        )
        with self._lifecycle_lock:
            if self._opening_generation == opening_generation and self._opening:
                self._opening_operation = operation
        # Attach the ownership fence before the caller-facing timeout adapter. A
        # slow SDK call must not let a timeout callback expose an apparently-open
        # pipeline while its worker still owns the manager.
        operation.addBoth(self._finish_async_open)
        bounded = bounded_deferred(
            operation,
            timeout=self._reactor_io_timeout,
            operation="pipeline open",
        )
        # No lifecycle owner is attached to a timed-out open view itself. Keep a
        # terminal observer on the dropped worker while _finish_async_open retains
        # its Failure for a close request that is already fencing the operation.
        operation.addErrback(lambda _failure: None)
        return bounded

    def _finish_async_open(self, result: Any) -> Any:
        # _open_spider_sync owns the opening token and clears it only after its
        # callbacks and rollback have finished.  This observer must not publish a
        # second, earlier lifecycle transition on behalf of a timed-out worker.
        with self._lifecycle_lock:
            if isinstance(result, TwistedFailure):
                self._opening_failure = result
                if self._opening:
                    self._opening = False
                    self._opening_generation = None
                    self._opening_owner_thread_id = None
                    self._opening_operation = None
                    self._store_condition.notify_all()
            elif self._opening is False and self._opening_operation is not None:
                self._opening_operation = None
        return result

    def _open_spider_sync(
        self,
        spider: Spider | None = None,
        opening_generation: int | None = None,
    ) -> None:
        """Called when a spider opens.

        Detects whether the configured backend supports storage. If not
        (Kafka, RabbitMQ, RocketMQ), the pipeline degrades to a no-op and
        logs a warning so the operator knows items aren't being persisted.
        A *transient* connection blip is neither — the pipeline leaves the
        capability unconfirmed and retries storage lazily in ``process_item``
        (whose own try/except handles ongoing failures best-effort) rather
        than aborting the crawl at startup.

        Args:
            spider: The spider instance for legacy Scrapy/direct calls. Current
                Scrapy omits it and the pipeline resolves ``crawler.spider``.
        """
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("pipeline is closed")
            if self._closing and opening_generation is None:
                raise RuntimeError("pipeline close must be retried")
            resolved_spider = self._resolve_spider(spider)
            if self._opened:
                if resolved_spider is self._opened_spider:
                    return
                raise RuntimeError("pipeline is already open for a different spider")
            if opening_generation is None:
                self._lifecycle_generation += 1
                opening_generation = self._lifecycle_generation
                self._opening_generation = opening_generation
                self._open_close_requested = False
            elif self._opening_generation != opening_generation:
                raise RuntimeError("pipeline open attempt is no longer current")
            # Admission is fenced while extension callbacks run, but the callbacks
            # themselves must never run while this non-reentrant state lock is held.
            self._opening = True
            self._opening_owner_thread_id = threading.get_ident()

        open_diagnostic: str | None = None
        open_failure: BaseException | None = None
        try:
            _validate_key_name(resolved_spider.name, "spider.name")
            self.storage_strategy.open()
            try:
                self.connection_manager.get_storage_backend()
                self._storage_supported = True
            except NotImplementedError:
                self._storage_supported = False
                open_diagnostic = "unsupported_storage"
            except BackendConnectionError:
                open_diagnostic = "unreachable_storage"
        except BaseException as exc:
            with self._lifecycle_lock:
                if self._opening_generation == opening_generation:
                    self._opening = False
                    self._opening_generation = None
                    self._opening_owner_thread_id = None
                    self._opening_operation = None
                    self._store_condition.notify_all()
            open_failure = exc

        if open_failure is not None:
            cleanup_failed = False
            try:
                # No item can enter before open succeeds. Rollback runs after the
                # exception suite has unwound so callbacks cannot inspect it.
                self._close_locked(force_manager_release=True)
            except BaseException:
                cleanup_failed = True
            if cleanup_failed:
                try:
                    logger.error("Pipeline cleanup failed during open rollback")
                except BaseException:
                    # Diagnostics must not replace the open failure being unwound.
                    pass
            raise open_failure

        with self._lifecycle_lock:
            current = (
                self._opening_generation == opening_generation
                and not self._closed
                and not self._closing
                and not self._open_close_requested
            )
            if current:
                self._opened = True
                self._opened_spider = resolved_spider
            if self._opening_generation == opening_generation:
                self._opening = False
                self._opening_generation = None
                self._opening_owner_thread_id = None
                self._opening_operation = None
                self._store_condition.notify_all()
                self._open_close_requested = False
        if not current:
            # A close that arrived during open owns the terminal transition.  Do
            # not resurrect OPENED after the callback returns; finish teardown now
            # (or let the already-running close finish it).
            if not self._closed and not self._close_async_pending:
                self._close_locked(force_manager_release=True)
            return
        if open_diagnostic == "unsupported_storage":
            # The expected capability exception has fully unwound. A custom handler
            # must not recover it through ``sys.exc_info()``.
            _emit_diagnostic(
                logger.warning,
                "Configured backend does not support storage; pipeline will not "
                "persist items.",
            )
        elif open_diagnostic == "unreachable_storage":
            # Keep this operational continuation static and outside its catch
            # suite; backend types and driver messages may be sensitive.
            _emit_diagnostic(
                logger.warning,
                "Storage backend not reachable at spider open; pipeline will retry "
                "storage lazily on each item.",
            )
        _emit_diagnostic(
            logger.info,
            "Pipeline opened for spider %s",
            resolved_spider.name,
        )

    def close_spider(self, spider: Spider | None = None) -> Deferred[None] | None:
        """Close synchronously outside the reactor, or return a bounded Deferred."""
        try:
            if reactor_is_running():
                result = self._close_spider_async(spider)
                del spider
                return result
            self._close_spider_sync(spider)
        except BaseException:
            del self
            del spider
            raise
        del spider
        return None

    def _close_spider_async(self, spider: Spider | None) -> Deferred[None]:
        with self._lifecycle_lock:
            if self._closed:
                return succeed(None)
            opening_operation = self._opening_operation
            if self._opening and opening_operation is not None:
                self._open_close_requested = True
                self._close_async_pending = True

                # A caller may time out waiting for open while its worker is still
                # probing the backend. Queue close behind that exact operation so
                # manager ownership cannot race the probe. Preserve a late open
                # failure as the close result after teardown completes.
                def continue_after_open(_ignored: Any) -> Any:
                    close_result = self._close_spider_async(spider)
                    opening_failure = self._opening_failure
                    if opening_failure is None:
                        return close_result
                    return close_result.addBoth(lambda _value: opening_failure)

                return opening_operation.addBoth(continue_after_open)
            if self._opening:
                raise RuntimeError("pipeline open is still in progress")
            current_thread = threading.get_ident()
            if self._closing:
                if self._close_owner_thread_id == current_thread:
                    # A storage callback recursively closing the same pipeline is
                    # already covered by the outer attempt.
                    return succeed(None)
                raise RuntimeError("pipeline close is already in progress")
            self._closing = True
            self._close_async_pending = True
            resolved_spider = self._resolve_spider(spider) if self._opened else None

        # ``_async_tail`` is an operation-completion chain, not the caller-facing
        # timeout Deferred. This preserves FIFO writes and prevents close from
        # releasing the manager while a timed-out worker is still using it.
        public_result: Deferred[None] = Deferred()

        def start_close(_ignored: Any) -> Deferred[None]:
            def run_reserved_close() -> None:
                with self._lifecycle_lock:
                    self._close_async_pending = False
                    # Transfer the reserved close to _close_locked rather than
                    # making it look like a recursive callback from its owner.
                    self._close_owner_thread_id = None
                self._close_spider_sync(resolved_spider)

            operation, bounded = defer_to_thread_ordered(
                run_reserved_close,
                timeout=self._reactor_io_timeout,
                operation="pipeline close",
            )

            def publish_success(value: Any) -> Any:
                if not public_result.called:
                    public_result.callback(value)
                return value

            def publish_failure(failure: Any) -> None:
                if not public_result.called:
                    public_result.errback(failure)
                # The internal bounded view is no longer exposed; consume its
                # failure after publishing the same Failure to the caller.
                return None

            bounded.addCallbacks(publish_success, publish_failure)
            # The authoritative tail still has to be observed after a public
            # timeout/failure; otherwise a late close worker Failure is unhandled.
            operation.addErrback(lambda _failure: None)
            return operation

        close_operation = self._async_tail.addBoth(start_close)
        self._async_tail = close_operation
        return public_result

    @storage_operation_error_boundary(
        "store",
        _PIPELINE_CLOSE_FAILURE_MESSAGE,
        "storage-strategy",
        safe_messages=(_BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,),
    )
    def _close_spider_sync(self, spider: Spider | None = None) -> None:
        """Called when a spider closes.

        Flushes any buffered items via the storage strategy before shutting the
        connection manager down (so batched strategies drain on spider close).

        Args:
            spider: The spider instance for legacy Scrapy/direct calls. Current
                Scrapy omits it and the pipeline uses the opened/crawler spider.
        """
        with self._lifecycle_lock:
            if self._closed:
                return
            if self._opening:
                self._open_close_requested = True
                if self._opening_owner_thread_id == threading.get_ident():
                    # The opener cannot wait for itself.  Its completion checkpoint
                    # owns the bounded re-entrant close policy and will tear down
                    # after the open callback returns.
                    return
                while self._opening:
                    self._store_condition.wait()
            try:
                resolved_spider = self._resolve_spider(spider)
            except Exception:
                # _resolve_spider raises when there is no opened spider and no
                # crawler (direct/from_settings use, or after an open_spider
                # failure). Teardown must still run -- _close_locked() releases
                # the storage strategy and the connection manager. The spider
                # name only feeds the diagnostic log below; mirror the
                # log-handler guard so a resolution failure cannot skip it.
                resolved_spider = None
        # The strategy and manager are extension callbacks. _close_locked claims
        # the state under the lock but invokes both callbacks after releasing it.
        self._close_locked()
        try:
            if resolved_spider is not None:
                logger.info("Pipeline closed for spider %s", resolved_spider.name)
        except BaseException:
            # This is diagnostic-only and follows the durability barrier and
            # manager release, so a handler cannot alter lifecycle state.
            pass

    def _close_locked(self, *, force_manager_release: bool = False) -> None:
        """Release one pipeline lifecycle, invoking callbacks without the lock.

        Normal close keeps manager ownership when strategy cleanup fails, allowing
        a later call to retry the durability barrier. Unpublished open/factory
        rollback has admitted no items, so ``force_manager_release`` prevents a
        broken strategy cleanup from leaking the manager acquire.
        """
        current_thread = threading.get_ident()
        with self._lifecycle_lock:
            if self._closed:
                return
            if self._closing:
                if self._close_owner_thread_id == current_thread:
                    if not self._close_waiting_for_stores:
                        # Recursive close from strategy cleanup is covered by the
                        # outer attempt and must not wait on its own lock.
                        return
                elif self._close_async_pending:
                    raise RuntimeError("pipeline close is already in progress")
                elif self._close_owner_thread_id is not None:
                    raise RuntimeError("pipeline close is already in progress")
            else:
                self._closing = True
                self._close_owner_thread_id = current_thread
            self._close_async_pending = False
            self._close_owner_thread_id = current_thread
            while self._active_store_count:
                if self._active_store_threads.get(current_thread, 0):
                    # Same-thread close cannot wait for its own admitted store.
                    # Leave closing latched; the store's finally block resumes
                    # this teardown after the backend call returns.
                    self._close_waiting_for_stores = True
                    return
                self._store_condition.wait()
            self._close_waiting_for_stores = False

        primary_error: BaseException | None = None
        try:
            self.storage_strategy.close()
        except BaseException as exc:
            with self._lifecycle_lock:
                self._close_owner_thread_id = None
                self._close_waiting_for_stores = False
                self._store_condition.notify_all()
            if not force_manager_release:
                # A normal close failure may retain an unchanged batched retry tail.
                # Do not publish terminal state or release its backend capability.
                raise
            primary_error = exc

        # Either the durability barrier succeeded or this unpublished lifecycle is
        # being forcibly rolled back. Publish terminal state before invoking the
        # manager callback so re-entrant close is an idempotent no-op.
        with self._lifecycle_lock:
            self._closed = True
            self._closing = False
            self._close_async_pending = False
            self._close_owner_thread_id = None
            self._opened = False
            self._opened_spider = None
            self._open_close_requested = False
            self._store_condition.notify_all()
            release_manager = (
                self._owns_connection_manager and not self._manager_released
            )
            if release_manager:
                # A BaseException after the provider decrements its refcount must
                # not cause a retry to release the same acquire twice.
                self._manager_released = True

        secondary_manager_close_failed = False
        if release_manager:
            try:
                if self._connection_manager_lease is not None:
                    self._connection_manager_lease.release()
                else:
                    self.connection_manager.close()
            except BaseException as exc:
                if primary_error is None:
                    primary_error = exc
                else:
                    secondary_manager_close_failed = True

        if secondary_manager_close_failed:
            _emit_diagnostic(
                logger.error,
                "connection_manager.close() failed during teardown",
            )
        if primary_error is not None:
            raise primary_error

    def process_item(self, item: Any, spider: Spider | None = None) -> Any:
        """Process an item, moving synchronous storage off a running reactor."""
        try:
            if reactor_is_running():
                result = self._process_item_async(item, spider)
                del item
                del spider
                return result
            result = self._process_item_sync(item, spider)
        except BaseException:
            del self
            del item
            del spider
            raise
        del item
        del spider
        return result

    def _process_item_async(
        self,
        item: Any,
        spider: Spider | None,
    ) -> Deferred[Any]:
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("pipeline is closed")
            if self._closing:
                raise RuntimeError("pipeline close must be retried")
            if self._opening:
                raise RuntimeError("pipeline open is still in progress")
            resolved_spider = self._resolve_spider(spider)

        # The adapter cannot be created until this item reaches the FIFO tail.
        # Publish its bounded result through a placeholder, while the tail keeps
        # the authoritative worker operation for ordering and late outcomes.
        public_result: Deferred[Any] = Deferred()

        def start(_ignored: Any) -> Deferred[Any]:
            operation, bounded = defer_to_thread_ordered(
                self._process_item_sync,
                item,
                resolved_spider,
                timeout=self._reactor_io_timeout,
                operation="pipeline store",
            )

            def publish_success(value: Any) -> Any:
                if not public_result.called:
                    public_result.callback(value)
                return value

            def publish_failure(failure: Any) -> None:
                if not public_result.called:
                    public_result.errback(failure)
                # ``bounded`` is internal after its result has been published.
                # Returning success here consumes its timeout/worker Failure so
                # neither inner timeout nor late authoritative failure is lost.
                return None

            bounded.addCallbacks(publish_success, publish_failure)
            operation.addErrback(lambda _failure: None)
            return operation

        operation = self._async_tail.addBoth(start)
        self._async_tail = operation
        return public_result

    @serialization_error_boundary(
        _PIPELINE_SERIALIZATION_FAILURE_MESSAGE,
        serializer="json",
        monitor_operation="store",
        monitor_messages=(_PIPELINE_SERIALIZATION_MONITOR_FAILURE,),
        logger=logger,
    )
    @_pipeline_store_error_boundary
    def _process_item_sync(self, item: Any, spider: Spider | None = None) -> Any:
        """Process one admitted item while fencing terminal teardown."""
        thread_id = threading.get_ident()
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("pipeline is closed")
            if self._closing:
                raise RuntimeError("pipeline close must be retried")
            resolved_spider = self._resolve_spider(spider)
            self._active_store_count += 1
            self._active_store_threads[thread_id] = (
                self._active_store_threads.get(thread_id, 0) + 1
            )
        try:
            # Storage strategy, backend, monitor, stats, and logging hooks are all
            # extension callbacks. Keep them outside the lifecycle state lock.
            return self._process_item_unlocked(item, resolved_spider)
        finally:
            self._leave_store(thread_id)

    def _leave_store(self, thread_id: int) -> None:
        """Release one store admission and resume a re-entrant close if needed."""
        resume_close = False
        with self._lifecycle_lock:
            self._active_store_count -= 1
            remaining = self._active_store_threads.get(thread_id, 0) - 1
            if remaining:
                self._active_store_threads[thread_id] = remaining
            else:
                self._active_store_threads.pop(thread_id, None)
            self._store_condition.notify_all()
            if (
                self._active_store_count == 0
                and self._closing
                and self._close_waiting_for_stores
            ):
                self._close_waiting_for_stores = False
                self._close_owner_thread_id = None
                resume_close = True
        if resume_close:
            try:
                self._close_locked()
            except BaseException as exc:
                # The recursive caller has already returned its store result. Keep
                # cleanup ownership retryable rather than masking that result from
                # the admitted store.
                with self._lifecycle_lock:
                    self._pending_close_error = exc
                    self._closing = False
                    self._close_owner_thread_id = None
                    self._store_condition.notify_all()

    def _process_item_unlocked(self, item: Any, spider: Spider) -> Any:
        """Process and store an item.

        Best-effort: catches storage errors so a temporary backend failure
        doesn't kill the spider. The item is returned unchanged either way
        so downstream pipelines continue. Storage errors are logged and
        counted in spider stats.

        Args:
            item: The item to process.
            spider: The spider instance.

        Returns:
            The processed item (always).
        """
        if self._storage_supported is False:
            self._inc_stat(spider, "pipeline/storage_skipped")
            return item

        key = self._generate_item_key(spider)
        serialization_failed = False
        try:
            data = self._serialize_item(item)
        except SerializationError:
            raise
        except Exception:
            # The stat collector can be application-provided. Run it only after the
            # serializer failure has left this frame so it cannot inspect private
            # item state through ``sys.exc_info()``.
            serialization_failed = True
        if serialization_failed:
            self._inc_stat(spider, "pipeline/serialization_errors")
            raise SerializationError(
                _PIPELINE_SERIALIZATION_MONITOR_FAILURE,
                serializer="json",
            ) from None

        # D2: reject oversize payloads loudly (DoS guard). Unlike a transient
        # storage error (swallowed below to keep the spider alive), this is a
        # deterministic validation failure — surfacing it prevents the silent
        # drop that capped storage backends (Memcached 1 MB, DynamoDB 400 KB)
        # would otherwise cause.
        if len(data) > self.max_item_bytes:
            # Risk 5: renamed ``oversize_dropped`` → ``oversize_rejected`` (the item
            # is rejected+raised, not silently dropped — the old name misled). The
            # legacy key is still incremented for one release so existing dashboards
            # keep working (mirrors monitor/stats.py ``queue/pop_count`` aliasing).
            self._inc_stat(spider, "pipeline/oversize_dropped")
            self._inc_stat(spider, "pipeline/oversize_rejected")
            raise SerializationError(
                _PIPELINE_SERIALIZATION_FAILURE_MESSAGE,
                serializer="json",
            )

        storage_failed = False
        try:
            self._store_item(key, data)
        except StorageBackpressureError:
            # This item was never admitted to the volatile strategy buffer. It is
            # not a retryable backend failure, so best-effort storage handling must
            # not return success-shaped data to Scrapy.  The public boundary rebuilds
            # a fresh result only after this frame releases key, payload, and item.
            raise
        except (
            ConfigurationError,
            SerializationError,
            TypeError,
            ValueError,
            OverflowError,
        ):
            # Local validation/configuration failures are deterministic. Returning
            # the item would report success-shaped data loss and retrying cannot heal
            # it, so preserve their original typed failure for Scrapy/operator policy.
            raise
        except Exception:
            # Do not invoke logging, stats, or a custom monitor from this suite: a
            # handler can otherwise observe the raw backend failure via
            # ``sys.exc_info()`` even when the record itself is static.
            storage_failed = True

        if storage_failed:
            reported_error = StorageError(
                _PIPELINE_STORAGE_FAILURE_MESSAGE,
                operation="store",
            )
            _emit_diagnostic(
                logger.warning,
                "Failed to store item. Item will not be persisted.",
            )
            self._inc_stat(spider, "pipeline/storage_errors")
            # R32-B: emit one sanitized store error for observability parity with the
            # sibling serialization arm (above), the batched age-flusher, queue, and
            # dupefilter — so a ScrapyStatsMonitor increments the documented
            # ``errors/store`` counter for synchronous store failures (the most common
            # storage failure mode), not just serialization + batched-flush failures.
            monitor_failed = False
            try:
                self._monitor.on_error("store", reported_error)
            except Exception:  # noqa: BLE001 - telemetry cannot mask storage
                monitor_failed = True
            if monitor_failed:
                _emit_diagnostic(
                    logger.debug,
                    "Pipeline store monitor callback raised; ignored.",
                )
            # C2: reliability-safe default is a finite ceiling. Track consecutive
            # failures and re-raise a fixed error once the count exceeds N. The
            # explicit ``max_storage_errors=None`` opt-in retains best-effort
            # swallow-and-stat behavior for applications that accept item loss.
            if self.max_storage_errors is not None:
                self._consecutive_storage_errors += 1
                if self._consecutive_storage_errors > self.max_storage_errors:
                    raise _PipelineStorageFailureThresholdExceeded
            return item
        # Strategy acceptance resets the consecutive error counter. Direct
        # strategies emit on_store here; buffering strategies emit it only when
        # their later durable backend write succeeds.
        self._consecutive_storage_errors = 0
        if not self.storage_strategy.emits_store_events:
            monitor_failed = False
            try:
                self._monitor.on_store(key)
            except Exception:  # noqa: BLE001 - storage has already succeeded
                monitor_failed = True
            if monitor_failed:
                _emit_diagnostic(
                    logger.debug,
                    "Pipeline store-success monitor callback raised; ignored.",
                )
        _emit_diagnostic(logger.debug, "Stored item: %s", key)
        return item

    @staticmethod
    def _inc_stat(spider: Spider, stat_name: str) -> None:
        """Increment a Scrapy stat, tolerating missing crawler/stats.

        Defensively chains ``spider.crawler.stats`` via ``getattr`` because
        legacy spider classes (or test doubles) may not expose ``crawler``.
        Silent skip when the chain is broken — the spider continues either
        way; a missing counter is preferable to crashing the pipeline.

        Args:
            spider: The spider instance (must have ``crawler.stats`` for the
                stat to be recorded).
            stat_name: The Scrapy stats key to increment.
        """
        crawler = getattr(spider, "crawler", None)
        stats = getattr(crawler, "stats", None) if crawler is not None else None
        if stats is not None:
            stats_failed = False
            try:
                stats.inc_value(stat_name)
            except Exception:  # noqa: BLE001 - stats cannot mask the pipeline result
                stats_failed = True
            if stats_failed:
                _emit_diagnostic(
                    logger.debug,
                    "Pipeline stats collector callback raised; ignored.",
                )

    def _generate_item_key(self, spider: Spider) -> str:
        """Generate a unique key for the item.

        Args:
            spider: The spider instance.

        Returns:
            A unique storage key.
        """
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        unique_id = uuid.uuid4().hex[:8]
        return f"{self.key_prefix}:{spider.name}:{timestamp}:{unique_id}"

    def _serialize_item(self, item: Any) -> bytes:
        """Serialize an item.

        Args:
            item: The item to serialize.

        Returns:
            Serialized item bytes.
        """
        if not is_item(item):
            raise TypeError(f"Unsupported pipeline item type: {type(item).__name__}")
        item_dict = ItemAdapter(item).asdict()
        return self._serializer.serialize(item_dict)

    def _store_item(self, key: str, data: bytes) -> None:
        """Store serialized item via the configured storage strategy.

        The default :class:`PassthroughStorageStrategy` delegates straight to
        ``storage_backend.store(key, data, ttl=self.ttl)`` — byte-identical to the
        pre-strategy behavior. Batched strategies buffer the item and flush later.

        Args:
            key: Storage key.
            data: Serialized item data.
        """
        self.storage_strategy.store(
            self.connection_manager.get_storage_backend(),
            key,
            data,
            ttl=self.ttl,
        )
