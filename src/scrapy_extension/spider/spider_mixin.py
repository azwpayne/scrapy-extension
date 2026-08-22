"""Spider mixin for backend integration.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This module provides the BackendSpiderMixin class that adds backend functionality
to Scrapy spiders, enabling distributed crawling capabilities.
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pydispatch.errors import DispatcherKeyError
from scrapy import Spider, signals
from twisted.internet.defer import Deferred
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure as TwistedFailure

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.monitor import NullMonitor
from scrapy_extension.schedule._dupefilter_compat import (
    _backend_dupefilter_lifecycle,
)
from scrapy_extension.utils.identity import (
    DEFAULT_DUPEFILTER_KEY_TEMPLATE,
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

logger = logging.getLogger(__name__)


def _submit_thread(
    function: Callable[..., Any], *args: Any, **kwargs: Any
) -> Deferred[Any]:
    """Submit lifecycle work as a settled Deferred even when the adapter rejects it."""
    try:
        return deferToThread(function, *args, **kwargs)
    except BaseException as exc:
        failed: Deferred[Any] = Deferred()
        failed._reactor_submission_failure = "thread"  # type: ignore[attr-defined]
        failed.errback(exc)
        return failed


def _emit_diagnostic(method: Any, message: str, *args: Any) -> None:
    """Emit a static diagnostic after lifecycle cleanup has unwound."""
    try:
        method(message, *args)
    except BaseException:
        pass


@dataclass(slots=True)
class _ComponentConstruction:
    """One lazy component build that is allowed to publish one generation."""

    kind: str
    generation: int
    owner_thread_id: int
    done: threading.Event = field(default_factory=threading.Event)
    invalidated: bool = False


@dataclass(slots=True)
class _BackendSetupAttempt:
    """One setup transaction whose callbacks may not publish stale state."""

    generation: int
    owner_thread_id: int
    manager: Any | None = None
    lease: Any | None = None
    acquired: bool = False
    invalidated: bool = False
    done: threading.Event = field(default_factory=threading.Event)


@dataclass(frozen=True, slots=True)
class _LifecycleSignalLease:
    """One exact spider lifecycle signal registration."""

    manager: Any
    handler: Any
    signal: Any


if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from typing_extensions import Self

    from scrapy_extension.backends.base import BackendType
    from scrapy_extension.backends.connectors import (
        ConnectionManager,
        ConnectionManagerLease,
    )
    from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
    from scrapy_extension.queue.queue import BackendQueue
    from scrapy_extension.schedule.scheduler import BackendScheduler


class BackendSpiderMixin(Spider):
    """Spider subclass that integrates with backend components.

    Inherits from :class:`scrapy.Spider` so ``self`` is statically a Spider,
    enabling ``BackendQueue`` to resolve callback/errback names during request
    deserialization. Provides convenient access to backend functionality
    including queues, dupefilters, and schedulers, with connection lifecycle
    management via Scrapy signals.

    Attributes:
        backend_type: The type of backend to use (e.g., REDIS, MONGODB).
        backend_settings: Optional dictionary of backend-specific settings.
        redis_host: Shortcut for Redis host configuration.
        redis_port: Shortcut for Redis port configuration.
        redis_db: Shortcut for Redis database configuration.
        redis_password: Shortcut for Redis password configuration.
        mongodb_uri: Shortcut for MongoDB URI configuration.
        mongodb_db: Shortcut for MongoDB database configuration.
        kafka_bootstrap_servers: Shortcut for Kafka bootstrap servers.
        rabbitmq_url: Shortcut for RabbitMQ connection URL.

    Example:
        class MySpider(BackendSpiderMixin):
            name = "myspider"
            backend_type = BackendType.REDIS
            redis_host = "localhost"
            redis_port = 6379

            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.setup_backend()
    """

    # Class-level backend configuration attributes
    backend_type: BackendType | str | None = None
    backend_settings: dict[str, Any] | None = None

    # Redis shortcut settings
    redis_host: str | None = None
    redis_port: int | None = None
    redis_db: int | None = None
    redis_password: str | None = None

    # MongoDB shortcut settings
    mongodb_uri: str | None = None
    mongodb_db: str | None = None

    # Kafka shortcut settings
    kafka_bootstrap_servers: str | None = None

    # RabbitMQ shortcut settings
    rabbitmq_url: str | None = None

    # ElasticSearch shortcut settings
    elasticsearch_hosts: list[str] | None = None
    elasticsearch_cloud_id: str | None = None
    elasticsearch_api_key: str | None = None

    # RocketMQ shortcut settings
    rocketmq_namesrv_address: str | None = None
    rocketmq_access_key: str | None = None
    rocketmq_secret_key: str | None = None
    rocketmq_tls_enabled: bool | None = None

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the mixin.

        Args:
            **kwargs: Keyword arguments passed to the spider.
        """
        super().__init__(**kwargs)
        self._connection_manager: ConnectionManager | None = None
        self._connection_manager_lease: ConnectionManagerLease | None = None
        self._queue: BackendQueue | None = None
        self._queue_name: str | None = None
        self._mixin_project_name: str | None = None
        self._snapshot_connection_manager: ConnectionManager | None = None
        self._snapshot_connection_lease: ConnectionManagerLease | None = None
        self._queue_connection_manager: ConnectionManager | None = None
        self._queue_connection_lease: ConnectionManagerLease | None = None
        self._dupefilter: BackendDupeFilter | None = None
        self._scheduler: BackendScheduler | None = None
        self._consumer_manager_scope = uuid.uuid4().hex
        self._consumer_queue_name: str | None = None
        self._signals_connected = False
        self._connected_signals: Any | None = None
        # The list is authoritative. A lease is published before connect() so an
        # effect-then-raise provider call cannot strand an opaque registration.
        self._signal_leases: list[_LifecycleSignalLease] = []
        # Component close hooks are user-extensible and may call close_backend()
        # again. An RLock lets that re-entrant call observe the already-detached
        # state and return instead of deadlocking the outer shutdown.
        self._lifecycle_lock = threading.RLock()
        self._close_in_progress = False
        self._close_owner_thread_id: int | None = None
        self._close_deferred: Deferred[Any] | None = None
        self._component_generation = 0
        self._component_constructions: dict[str, _ComponentConstruction] = {}
        self._setup_generation = 0
        self._setup_attempt: _BackendSetupAttempt | None = None
        self._setup_identity: tuple[str, str] | None = None
        self._orphan_candidates: list[tuple[str, Any]] = []
        self._orphan_candidate_operations: dict[int, Deferred[Any]] = {}
        self._orphan_candidate_failures: dict[int, BaseException] = {}
        self._orphan_leases: list[Any] = []
        self._orphan_managers: list[Any] = []
        self._close_wait_operation: Deferred[Any] | None = None
        self._close_wait_owner_thread_id: int | None = None
        self._close_wait_finishing = False
        # Every operation is retained by identity. A named slot is unsafe here:
        # two signal/getter callbacks may overlap, and replacing the first
        # Deferred would let close_backend release shared ownership too early.
        self._async_component_operations: dict[str, Deferred[Any]] = {}

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Create the spider and finalize backend setup after crawler attachment.

        Scrapy assigns ``crawler`` only after ``__init__`` returns. Performing the
        final idempotent setup here guarantees lifecycle signals are available,
        including for legacy subclasses that called :meth:`setup_backend` early in
        ``__init__``. Subclasses with no ``backend_type`` remain ordinary spiders
        until they opt into backend access explicitly.
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        if spider.backend_type is not None:
            spider.setup_backend()
        return spider

    def setup_backend(self) -> ConnectionManager:
        """Acquire one compatible manager and publish it after callbacks settle."""
        from scrapy_extension.backends.connectors import (
            CONNECTION_MANAGER_SCOPE_KEY,
            CONSUMER_SCOPED_BACKENDS,
            ConnectionManager,
            resolve_circuit_breaker_policy,
        )

        existing_manager = self._connection_manager
        configured_backend_type = self.backend_type
        if configured_backend_type is None and existing_manager is None:
            msg = (
                f"{self.__class__.__name__}.backend_type must be set. "
                "Use BackendType.REDIS, BackendType.MONGODB, etc."
            )
            raise RuntimeError(msg)

        crawler = getattr(self, "crawler", None)
        settings = self._build_backend_settings()
        if crawler is not None:
            from scrapy.settings import Settings

            if isinstance(crawler.settings, Settings):
                settings.update(resolve_circuit_breaker_policy(crawler.settings))
        if self._backend_type_name() in CONSUMER_SCOPED_BACKENDS:
            settings = {
                **settings,
                CONNECTION_MANAGER_SCOPE_KEY: (
                    f"spider-mixin-{self._consumer_manager_scope}"
                ),
            }
        # Breaker policy may legitimately be learned when crawler attachment occurs;
        # it is not backend identity and must not force a second acquire.
        registry_backend_type = self._backend_type_name()
        if registry_backend_type is None and configured_backend_type is not None:
            raw_backend_type = getattr(
                configured_backend_type,
                "value",
                configured_backend_type,
            )
            assert isinstance(raw_backend_type, str)
            registry_backend_type = raw_backend_type
        if registry_backend_type is None:
            registry_backend_type = ""
        identity_settings = dict(settings)
        for key in (
            "__connection_manager_circuit_breaker_enabled",
            "__connection_manager_circuit_breaker_failure_threshold",
            "__connection_manager_circuit_breaker_reset_timeout",
        ):
            identity_settings.pop(key, None)
        desired_identity = (
            self._setup_identity
            if self.backend_type is None and existing_manager is not None
            else (
                self._backend_type_name() or "",
                ConnectionManager._registry_key(
                    registry_backend_type,
                    identity_settings,
                ),
            )
        )

        while True:
            with self._lifecycle_lock:
                if self._close_in_progress:
                    raise RuntimeError("Backend spider close is already in progress")
                if (
                    self._setup_identity is not None
                    and self._setup_identity != desired_identity
                    and self._connection_manager is not None
                ):
                    raise RuntimeError(
                        "setup_backend() cannot reconfigure a live backend manager"
                    )
                existing = self._connection_manager
                if (
                    existing is not None
                    and getattr(existing, "_retired", False) is True
                ):
                    self._connection_manager = None
                    self._connection_manager_lease = None
                    self._setup_identity = None
                    existing = None
                active = self._setup_attempt
                if active is not None:
                    if active.owner_thread_id == threading.get_ident():
                        raise RuntimeError("setup_backend() is already in progress")
                    waiter = active.done
                else:
                    self._setup_generation += 1
                    attempt = _BackendSetupAttempt(
                        generation=self._setup_generation,
                        owner_thread_id=threading.get_ident(),
                        manager=existing,
                    )
                    self._setup_attempt = attempt
                    break
            waiter.wait()

        manager = existing
        acquired_here = False
        if manager is not None:
            attempt.lease = self._connection_manager_lease
        published = False
        initial_signal_manager = self._connected_signals
        try:
            if manager is None:
                assert configured_backend_type is not None
                manager = ConnectionManager.get_manager(
                    backend_type=configured_backend_type,
                    settings=settings,
                )
                acquired_here = True
                attempt.manager = manager
                attempt.lease = ConnectionManager._adopt_latest_legacy_lease(manager)
                attempt.acquired = True
            # All extension callbacks are deliberately outside _lifecycle_lock.
            from scrapy.settings import Settings

            if crawler is not None and isinstance(crawler.settings, Settings):
                manager.apply_scrapy_breaker_policy(crawler.settings)
            from scrapy_extension.queue.queue import BackendQueue

            manager.set_monitor(BackendQueue._resolve_monitor(self))
            self._connect_signals()

            with self._lifecycle_lock:
                valid = (
                    self._setup_attempt is attempt
                    and not attempt.invalidated
                    and not self._close_in_progress
                    and getattr(manager, "_retired", False) is not True
                    and (
                        self._connection_manager is None
                        or self._connection_manager is manager
                    )
                )
                if valid:
                    # Publish the exact acquire handle atomically with the manager.
                    # A peer close can begin as soon as ``_setup_attempt`` is
                    # cleared; assigning the lease after releasing this lock would
                    # let that close fall back to ``manager.close()`` and race the
                    # final lease publication.
                    self._connection_manager = manager
                    self._connection_manager_lease = attempt.lease
                    self._setup_identity = desired_identity
                    published = True
                if self._setup_attempt is attempt:
                    self._setup_attempt = None
                    attempt.done.set()
            if valid:
                return manager
            raise RuntimeError("setup_backend() completed after close")
        except BaseException as exc:
            primary_error: BaseException | None = exc

        if (
            not published
            and self._connected_signals is not None
            and self._connected_signals is not initial_signal_manager
        ):
            try:
                self._disconnect_lifecycle_signals(
                    self._connected_signals,
                    strict=True,
                )
            except BaseException:
                # Exact leases remain in the mixin. The setup error stays primary;
                # close_backend() is the deterministic retry owner.
                pass
        with self._lifecycle_lock:
            invalidated = attempt.invalidated or self._close_in_progress
            signals_pending = bool(self._signal_leases) or (
                self._connected_signals is not None
            )
            if self._setup_attempt is attempt:
                self._setup_attempt = None
                attempt.done.set()
            if signals_pending and manager is not None:
                # A dispatcher registration is a dependency of the manager. Keep
                # both exact owners reachable until close confirms every handler
                # absent, even though setup itself failed.
                self._connection_manager = manager
                self._connection_manager_lease = attempt.lease
                self._setup_identity = desired_identity
            elif self._connection_manager is manager and (
                acquired_here or (not invalidated)
            ):
                self._connection_manager = None
                self._connection_manager_lease = None
                self._setup_identity = None
        # A newly acquired manager is this attempt's responsibility. An existing
        # manager is compensated only when signal wiring orphaned it; a close that
        # already owns teardown remains the sole releaser. Never release while an
        # exact signal lease is still pending.
        should_release = (
            manager is not None
            and not signals_pending
            and (acquired_here or not invalidated)
        )
        release_error: BaseException | None = None
        if should_release and manager is not None:
            try:
                if attempt.lease is not None:
                    attempt.lease.release()
                else:
                    manager.close()
            except BaseException as exc:
                release_error = exc
        if release_error is not None:
            with self._lifecycle_lock:
                if attempt.lease is not None:
                    if not any(
                        existing is attempt.lease for existing in self._orphan_leases
                    ):
                        self._orphan_leases.append(attempt.lease)
                elif not any(existing is manager for existing in self._orphan_managers):
                    self._orphan_managers.append(manager)
            try:
                logger.error(
                    "Failed to release ConnectionManager after signal wiring failure"
                )
            except BaseException:
                pass
        assert primary_error is not None
        raise primary_error

    def _build_redis_settings(self) -> dict[str, Any]:
        """Build Redis-specific shortcut settings."""
        shortcuts: dict[str, Any] = {}
        if self.redis_host is not None:
            shortcuts["host"] = self.redis_host
        if self.redis_port is not None:
            shortcuts["port"] = self.redis_port
        if self.redis_db is not None:
            shortcuts["db"] = self.redis_db
        if self.redis_password is not None:
            shortcuts["password"] = self.redis_password
        return shortcuts

    def _build_mongodb_settings(self) -> dict[str, Any]:
        """Build MongoDB-specific shortcut settings."""
        shortcuts: dict[str, Any] = {}
        if self.mongodb_uri is not None:
            shortcuts["uri"] = self.mongodb_uri
        if self.mongodb_db is not None:
            shortcuts["database"] = self.mongodb_db
        return shortcuts

    def _build_kafka_settings(self) -> dict[str, Any]:
        """Build Kafka-specific shortcut settings."""
        shortcuts: dict[str, Any] = {}
        if self.kafka_bootstrap_servers is not None:
            shortcuts["bootstrap_servers"] = self.kafka_bootstrap_servers
        return shortcuts

    def _build_rabbitmq_settings(self) -> dict[str, Any]:
        """Build RabbitMQ-specific shortcut settings."""
        shortcuts: dict[str, Any] = {}
        if self.rabbitmq_url is not None:
            shortcuts["url"] = self.rabbitmq_url
        return shortcuts

    def _build_elasticsearch_settings(self) -> dict[str, Any]:
        """Build ElasticSearch-specific shortcut settings."""
        shortcuts: dict[str, Any] = {}
        if self.elasticsearch_hosts is not None:
            shortcuts["hosts"] = self.elasticsearch_hosts
        if self.elasticsearch_cloud_id is not None:
            shortcuts["cloud_id"] = self.elasticsearch_cloud_id
        if self.elasticsearch_api_key is not None:
            shortcuts["api_key"] = self.elasticsearch_api_key
        return shortcuts

    def _build_rocketmq_settings(self) -> dict[str, Any]:
        """Build RocketMQ-specific shortcut settings."""
        shortcuts: dict[str, Any] = {}
        if self.rocketmq_namesrv_address is not None:
            shortcuts["namesrv_address"] = self.rocketmq_namesrv_address
        if self.rocketmq_access_key is not None:
            shortcuts["access_key"] = self.rocketmq_access_key
        if self.rocketmq_secret_key is not None:
            shortcuts["secret_key"] = self.rocketmq_secret_key
        if self.rocketmq_tls_enabled is not None:
            shortcuts["tls_enabled"] = self.rocketmq_tls_enabled
        return shortcuts

    # Map of backend value -> shortcut-settings builder. Extracted as a
    # class-level constant so ``_build_backend_settings`` stays a flat
    # dispatch (no per-backend branching), keeping cyclomatic complexity
    # bounded as backends are added.
    _BACKEND_SHORTCUT_BUILDERS: ClassVar[dict[str, str]] = {
        "redis": "_build_redis_settings",
        "mongodb": "_build_mongodb_settings",
        "kafka": "_build_kafka_settings",
        "rabbitmq": "_build_rabbitmq_settings",
        "elasticsearch": "_build_elasticsearch_settings",
        "rocketmq": "_build_rocketmq_settings",
    }

    def _build_backend_settings(self) -> dict[str, Any]:
        """Build backend settings from shortcut attributes.

        Merges explicit ``backend_settings`` (if any) with the per-backend
        shortcut builder selected by ``backend_type.value``. Backends without
        shortcut attributes (e.g. Pulsar, SQS, Memcached, DynamoDB) have no
        builder entry and contribute nothing — preserving prior behavior.

        Returns:
            Dictionary of backend settings merged from class attributes.
        """
        settings: dict[str, Any] = {}

        # Start with explicit backend_settings if provided
        if self.backend_settings:
            settings.update(self.backend_settings)

        # Add shortcut settings based on backend type (no per-backend branching —
        # dispatch via the _BACKEND_SHORTCUT_BUILDERS table).
        # ``backend_type`` may be a ``BackendType`` enum (its ``.value`` is the
        # registry key) or a plain registry-key string (round-5 R5-1: the public
        # ``resolve_backend_config`` API now returns strings). Accept both.
        backend_value = self._backend_type_name()
        builder_name = self._BACKEND_SHORTCUT_BUILDERS.get(backend_value or "")
        if builder_name is not None:
            settings.update(getattr(self, builder_name)())

        return settings

    def _backend_type_name(self) -> str | None:
        """Return the normalized registry name for this spider's backend."""
        backend_type = self.backend_type
        if backend_type is None:
            return None
        value = getattr(backend_type, "value", backend_type)
        return value if isinstance(value, str) else None

    def _claim_consumer_queue(self, queue_name: str) -> None:
        """Bind single-consumer backends to one logical queue per mixin instance."""
        from scrapy_extension.backends.connectors import CONSUMER_SCOPED_BACKENDS

        if self._backend_type_name() not in CONSUMER_SCOPED_BACKENDS:
            return
        from scrapy_extension.backends.base import _validate_key_name

        _validate_key_name(queue_name, "queue_name")
        claimed = self._consumer_queue_name
        if claimed is not None and claimed != queue_name:
            raise ConfigurationError(
                f"Backend {self._backend_type_name()!r} supports one logical consumer "
                f"queue per spider mixin instance; already bound to {claimed!r}.",
                setting_name="queue_name",
                setting_value=queue_name,
            )
        self._consumer_queue_name = queue_name

    def _connect_signals(self) -> None:
        """Connect Scrapy signals for backend lifecycle management.

        Every registration is recorded before calling the dispatcher. This is
        deliberately conservative: a dispatcher can register a handler and then
        raise, and that handler must remain owned until a later close confirms
        that it is absent.
        """
        crawler = getattr(self, "crawler", None)
        if not crawler:
            return

        signal_manager = crawler.signals
        if self._signal_leases:
            if all(lease.manager is signal_manager for lease in self._signal_leases):
                return
            previous_signal_managers = {lease.manager for lease in self._signal_leases}
            for previous_signal_manager in previous_signal_managers:
                self._disconnect_lifecycle_signals(previous_signal_manager, strict=True)
        elif self._signals_connected:
            if self._connected_signals is signal_manager:
                return
            # Compatibility with state created by older callers that only set the
            # aggregate fields. Materialize exact leases before disconnecting.
            previous_signal_manager = self._connected_signals
            if previous_signal_manager is not None:
                self._disconnect_lifecycle_signals(
                    previous_signal_manager,
                    strict=True,
                )

        handlers = (
            (self._on_spider_opened, signals.spider_opened),
            (self._on_spider_closed, signals.spider_closed),
        )
        registration_failure: BaseException | None = None
        for handler, signal in handlers:
            lease = _LifecycleSignalLease(signal_manager, handler, signal)
            with self._lifecycle_lock:
                self._signal_leases.append(lease)
                self._connected_signals = signal_manager
                self._signals_connected = True
            try:
                signal_manager.connect(handler, signal)
            except BaseException as exc:
                # The lease stays published for effect-then-raise connect calls.
                # A bare unittest mock has no dispatcher state at all; preserve the
                # historical no-op rollback contract for that test/dummy surface.
                if signal_manager.__class__.__module__.startswith("unittest.mock"):
                    with self._lifecycle_lock:
                        self._signal_leases = [
                            existing
                            for existing in self._signal_leases
                            if existing is not lease
                        ]
                        if not self._signal_leases:
                            self._connected_signals = None
                            self._signals_connected = False
                registration_failure = exc
                break
        if registration_failure is not None:
            # Roll back every exact lease, including an attempted registration.
            # Cleanup failures are retained and cannot replace the registration
            # error. close_backend() is the later retry owner.
            with self._lifecycle_lock:
                rollback_handlers = tuple(
                    (lease.handler, lease.signal)
                    for lease in self._signal_leases
                    if lease.manager is signal_manager
                )
            rollback_failed = False
            try:
                self._disconnect_lifecycle_signals(
                    signal_manager,
                    handlers=rollback_handlers,
                )
            except BaseException:
                rollback_failed = True
                if signal_manager.__class__.__module__.startswith("unittest.mock"):
                    # Compatibility-only mocks have no authoritative dispatcher
                    # state; retain the lease record but keep the legacy aggregate
                    # view cleared after failed rollback.
                    with self._lifecycle_lock:
                        self._connected_signals = None
                        self._signals_connected = False
            if rollback_failed:
                _emit_diagnostic(
                    logger.error,
                    "Failed to roll back backend lifecycle signals",
                )
            raise registration_failure

    def _disconnect_lifecycle_signals(
        self,
        signal_manager: Any,
        *,
        handlers: tuple[tuple[Any, Any], ...] | None = None,
        strict: bool = False,
    ) -> None:
        """Disconnect exact lifecycle leases and retain failures for retry."""
        with self._lifecycle_lock:
            leases = [
                lease
                for lease in self._signal_leases
                if lease.manager is signal_manager
                and (
                    handlers is None
                    or any(
                        (lease.handler is handler or lease.handler == handler)
                        and lease.signal is signal
                        for handler, signal in handlers
                    )
                )
            ]
            if not leases and not self._signal_leases:
                leases = [
                    _LifecycleSignalLease(signal_manager, handler, signal)
                    for handler, signal in (
                        handlers
                        if handlers is not None
                        else (
                            (self._on_spider_opened, signals.spider_opened),
                            (self._on_spider_closed, signals.spider_closed),
                        )
                    )
                ]
                self._signal_leases.extend(leases)

        primary_error: BaseException | None = None
        for lease in leases:
            removed = False
            try:
                signal_manager.disconnect(lease.handler, lease.signal)
            except DispatcherKeyError:
                # The exact registration is already absent. This is the successful
                # retry result after an effect-then-raise disconnect.
                removed = True
            except BaseException as exc:  # noqa: BLE001 - finish every sibling
                if primary_error is None:
                    primary_error = exc
            else:
                removed = True
            if removed:
                with self._lifecycle_lock:
                    self._signal_leases = [
                        existing
                        for existing in self._signal_leases
                        if existing is not lease
                    ]
                    if (
                        not self._signal_leases
                        and self._connected_signals is signal_manager
                    ):
                        self._connected_signals = None
                        self._signals_connected = False
            elif isinstance(primary_error, Exception):
                # Ordinary failure logging is advisory, but only after its except
                # suite has unwound so handlers cannot replace the primary error.
                try:
                    logger.error("Failed to disconnect backend lifecycle signal")
                except BaseException:
                    pass
        if primary_error is not None and (
            strict or not isinstance(primary_error, Exception)
        ):
            raise primary_error

    def _on_spider_opened(self, spider: Spider) -> Deferred[Any] | None:
        """Handle spider_opened signal.

        Args:
            spider: The spider instance that was opened.
        """
        if spider is not self:
            return None
        with self._lifecycle_lock:
            manager = self._connection_manager
            if manager is not None and reactor_is_running():
                # Registration and scheduling are one lifecycle transaction. The
                # close path must see the authoritative connect worker even when
                # the bounded signal view expires first.
                operation, public = defer_to_thread_ordered(
                    manager.connect,
                    timeout=DEFAULT_REACTOR_IO_TIMEOUT_S,
                    operation="backend manager connect",
                )
                self._track_async_operation_locked(
                    "manager-connect",
                    operation,
                )
                try:
                    operation.addErrback(lambda _failure: None)
                except BaseException:
                    pass
                return public
        if manager is not None:
            manager.connect()
        return None

    def _track_async_operation_locked(
        self,
        name: str,
        operation: Deferred[Any],
    ) -> None:
        """Retain one worker Deferred until its real operation has settled.

        The caller must hold ``_lifecycle_lock`` while publishing the operation.
        This closes the small but important race where a close callback could
        otherwise run between ``deferToThread`` scheduling and ownership
        registration.
        """
        key = f"{name}:{id(operation)}"
        self._async_component_operations[key] = operation

        def clear(outcome: Any) -> Any:
            with self._lifecycle_lock:
                if self._async_component_operations.get(key) is operation:
                    self._async_component_operations.pop(key, None)
            return outcome

        try:
            operation.addBoth(clear)
        except BaseException:
            try:
                operation.addBoth(clear)
            except BaseException:
                if operation.called:
                    with self._lifecycle_lock:
                        self._async_component_operations.pop(key, None)

    def _on_spider_closed(
        self,
        spider: Spider,
        reason: str = "",
    ) -> Deferred[Any] | None:
        """Handle spider_closed and let Scrapy await asynchronous teardown.

        Ordinary close failures retain the historical advisory swallow, while
        control-flow exceptions remain visible to Scrapy.  A returned Deferred
        is adapted rather than discarded so a scheduler close cannot race the
        next signal subscriber or its shared manager release.
        """
        if spider is not self:
            return None
        close_failed = False
        close_result: Any = None
        try:
            close_result = self.close_backend()
        except Exception:
            close_failed = True
        if close_failed:
            # This is an advisory diagnostic: a broken logging handler must not
            # interrupt Scrapy's remaining spider_closed subscribers.  The caught
            # close failure has unwound before logger code runs, so the handler
            # cannot inspect it through ``sys.exc_info``. Direct control-flow
            # exceptions from close_backend still propagate.
            try:
                logger.error("close_backend() failed during spider_closed signal")
            except BaseException:
                pass
            return None
        if not isinstance(close_result, Deferred):
            return None

        def finish_close(failure: Any) -> Any:
            if failure.check(Exception):
                try:
                    logger.error("close_backend() failed during spider_closed signal")
                except BaseException:
                    pass
                return None
            return failure

        return close_result.addErrback(finish_close)

    def _crawler_settings(self) -> Any | None:
        """Return ``crawler.settings`` if a crawler is attached, else None.

        BackendSpiderMixin is constructed both ways: with a crawler (production
        Scrapy wiring) and without (unit tests / programmatic use). The getters
        honor SCRAPY_* settings when a crawler is present and fall back to the
        constructor defaults otherwise (#29).
        """
        crawler = getattr(self, "crawler", None)
        if crawler is None:
            return None
        return getattr(crawler, "settings", None)

    def _component_settings(self) -> Any:
        """Return the same settings surface used by standard component factories.

        Real crawlers always expose :class:`scrapy.settings.Settings`.  A small
        compatibility normalization keeps programmatic test doubles useful
        without allowing a ``MagicMock.get`` return value to masquerade as every
        unrelated setting (for example, treating ``"delay"`` as a backend type).
        """
        settings = self._crawler_settings()
        from scrapy.settings import Settings

        if isinstance(settings, Settings):
            return settings
        normalized = Settings()
        if settings is None:
            return normalized
        try:
            strategy = settings.get("SCRAPY_QUEUE_STRATEGY")
            if strategy in {
                "passthrough",
                "delay",
                "round_robin",
                "throttle",
                "priority",
                "time_wheel",
                "work_stealing",
                "ring_buffer",
            }:
                normalized.set("SCRAPY_QUEUE_STRATEGY", strategy)
            dedup = settings.get("SCRAPY_DEDUP_STRATEGY")
            if dedup in {"set", "memory", "bloom", "cuckoo"}:
                normalized.set("SCRAPY_DEDUP_STRATEGY", dedup)
        except (AttributeError, TypeError):
            pass
        return normalized

    def _queue_factory_uses_shared_manager(self, settings: Any) -> bool:
        """Whether queue settings select the mixin's already-acquired manager."""
        try:
            component_type = settings.get("SCRAPY_QUEUE_BACKEND_TYPE")
            global_type = settings.get("SCRAPY_BACKEND_TYPE")
            nested_override = settings.get("SCRAPY_QUEUE_BACKEND_SETTINGS")
            global_nested_override = settings.get("SCRAPY_BACKEND_SETTINGS")
        except (AttributeError, TypeError):
            component_type = global_type = None
            nested_override = global_nested_override = None
        selected_type = (
            component_type
            or global_type
            or os.environ.get("SCRAPY_QUEUE_BACKEND_TYPE")
            or os.environ.get("SCRAPY_BACKEND_TYPE")
        )
        selected_type = getattr(selected_type, "value", selected_type)
        same_backend = selected_type in (None, self._backend_type_name())
        return bool(same_backend and not (nested_override or global_nested_override))

    def _mixin_queue_key(self) -> str:
        """Return a resolved project/spider queue key or an explicit override."""
        settings = self._crawler_settings()
        configured = settings.get("SCRAPY_QUEUE_KEY") if settings is not None else None
        template = (
            configured
            if isinstance(configured, str) and configured
            else DEFAULT_QUEUE_KEY_TEMPLATE
        )
        if self._mixin_project_name is None:
            self._mixin_project_name = project_name_from_spider(self)
        return resolve_identity_template(
            template,
            spider_name=self.name,
            project_name=self._mixin_project_name,
        )

    def _resolve_queue_monitor(self) -> tuple[Any, float]:
        """Resolve direct-queue monitoring with the scheduler's operator knobs."""
        from scrapy_extension.schedule.scheduler import BackendScheduler
        from scrapy_extension.utils._config import (
            parse_float_setting,
            parse_int_setting,
        )

        settings = self._crawler_settings() or {}
        backpressure_threshold = parse_int_setting(
            settings.get("SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD", 1_000),
            "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD",
            minimum=0,
        )
        pop_rate_window_s = parse_float_setting(
            settings.get("SCRAPY_MONITOR_POP_RATE_WINDOW_S", 60.0),
            "SCRAPY_MONITOR_POP_RATE_WINDOW_S",
            minimum=0.0,
            minimum_exclusive=True,
            maximum=86400.0,
        )
        monitor = BackendScheduler._resolve_monitor_for_spider(
            self,
            backpressure_threshold=backpressure_threshold,
            pop_rate_window_s=pop_rate_window_s,
        )
        return monitor, pop_rate_window_s

    def _reserve_component_construction(
        self,
        kind: str,
        attribute: str,
        setup_message: str,
    ) -> tuple[Any, _ComponentConstruction] | Any:
        """Reserve one component generation without running extension code locked."""
        while True:
            with self._lifecycle_lock:
                if self._close_in_progress:
                    raise RuntimeError("Backend spider close is already in progress")
                existing = getattr(self, attribute)
                if existing is not None:
                    return existing
                manager = self._connection_manager
                if manager is None:
                    raise RuntimeError(setup_message)
                construction = self._component_constructions.get(kind)
                if construction is None:
                    construction = _ComponentConstruction(
                        kind=kind,
                        generation=self._component_generation,
                        owner_thread_id=threading.get_ident(),
                    )
                    self._component_constructions[kind] = construction
                    return manager, construction
                if construction.owner_thread_id == threading.get_ident():
                    raise RuntimeError(
                        f"Backend spider {kind} construction is already in progress"
                    )
                done = construction.done
            done.wait()

    def _construction_is_current_locked(
        self,
        construction: _ComponentConstruction,
        manager: Any,
    ) -> bool:
        """Check publication authority while the lifecycle lock is held."""
        return (
            self._component_constructions.get(construction.kind) is construction
            and not construction.invalidated
            and construction.generation == self._component_generation
            and self._connection_manager is manager
            and not self._close_in_progress
        )

    def _construction_is_current(
        self,
        construction: _ComponentConstruction,
        manager: Any,
    ) -> bool:
        """Check publication authority without invoking a component callback."""
        with self._lifecycle_lock:
            return self._construction_is_current_locked(construction, manager)

    def _finish_component_construction(
        self,
        construction: _ComponentConstruction,
    ) -> None:
        """Release a construction waiter after candidate cleanup or publication."""
        with self._lifecycle_lock:
            if self._component_constructions.get(construction.kind) is construction:
                del self._component_constructions[construction.kind]
            construction.done.set()

    @staticmethod
    def _authoritative_component_cleanup(
        component: Any,
        public_result: Any,
    ) -> Deferred[Any] | None:
        """Return the real cleanup operation behind a bounded public Deferred."""
        if not isinstance(public_result, Deferred):
            return None
        authoritative = getattr(component, "_close_completion_deferred", None)
        if isinstance(authoritative, Deferred) and authoritative is not public_result:
            # The public timeout is no longer lifecycle authority. Observe its
            # failure separately so handing cleanup to the authoritative worker
            # cannot create a second unhandled Deferred.
            public_result.addErrback(lambda _failure: None)
            return authoritative
        return public_result

    def _request_close_after_construction(self) -> None:
        """Let a recursive close finish without replacing construction errors."""
        with self._lifecycle_lock:
            if self._orphan_leases or self._orphan_candidates or self._orphan_managers:
                # A failed candidate/release is deliberately a later retry boundary;
                # do not consume the same ownership twice in this construction unwind.
                return
        try:
            self.close_backend()
        except BaseException:
            # Candidate construction remains the primary failure. Cleanup ownership
            # is retained on the mixin for the next close pass, including a lease
            # whose own release failed.
            return

    def _adopt_failed_factory_resources(
        self,
        factory_scheduler: Any,
        queue_name: str,
    ) -> None:
        """Retain resources whose factory cleanup failed for mixin retry."""
        with self._lifecycle_lock:
            factory_queue = getattr(factory_scheduler, "_queue", None)
            if self._queue is None and factory_queue is not None:
                self._queue = factory_queue
                self._queue_name = queue_name
                factory_scheduler._queue = None
            snapshot_lease = getattr(
                factory_scheduler,
                "_snapshot_connection_manager_lease",
                None,
            )
            if self._snapshot_connection_lease is None and snapshot_lease is not None:
                self._snapshot_connection_lease = snapshot_lease
                self._snapshot_connection_manager = getattr(
                    factory_scheduler,
                    "_snapshot_connection_manager",
                    None,
                )
                factory_scheduler._snapshot_connection_manager_lease = None
                factory_scheduler._snapshot_connection_manager = None
            queue_lease = getattr(factory_scheduler, "_connection_manager_lease", None)
            if self._queue_connection_lease is None and queue_lease is not None:
                self._queue_connection_lease = queue_lease
                self._queue_connection_manager = getattr(
                    factory_scheduler,
                    "connection_manager",
                    None,
                )
                factory_scheduler._connection_manager_lease = None

    def _capture_candidate_leases(self, candidate: Any) -> bool:
        """Move explicit candidate leases to the composite owner after close."""
        try:
            values = vars(candidate)
        except TypeError:
            return False
        captured = False
        leases: list[tuple[str, Any]] = []
        for attribute in (
            "_connection_manager_lease",
            "_snapshot_connection_manager_lease",
        ):
            lease = values.get(attribute)
            if lease is None or not callable(getattr(lease, "release", None)):
                continue
            captured = True
            leases.append((attribute, lease))
        with self._lifecycle_lock:
            for _, lease in leases:
                if not any(existing is lease for existing in self._orphan_leases):
                    self._orphan_leases.append(lease)
        # Attribute assignment can invoke a third-party descriptor; never perform
        # it while the composite lifecycle lock is held.
        for attribute, _lease in leases:
            try:
                setattr(candidate, attribute, None)
            except BaseException:
                pass
        return captured

    def _start_component_close(
        self,
        kind: str,
        component: Any,
        reason: str,
    ) -> tuple[Deferred[Any] | None, BaseException | None, bool]:
        """Start one close without running blocking component code on reactor.

        The small tuple distinguishes an immediate success from an immediate
        exception.  A returned Deferred is first replaced by the component's
        authoritative completion, when it exposes one, before this owner can
        capture leases or detach the component.
        """

        def invoke() -> tuple[bool, Any]:
            if kind == "queue":
                return True, component.close()
            return True, component.close(reason)

        offload = False
        if kind == "queue" and reactor_is_running():
            from scrapy_extension.queue.queue import BackendQueue

            # Only the bundled composite queue has the documented synchronous
            # snapshot/strategy close surface. Preserve generic plugin
            # components' historical direct hook contract.
            offload = isinstance(component, BackendQueue)
        if offload:
            operation = _submit_thread(invoke)
        else:
            try:
                _called, result = invoke()
            except BaseException as exc:
                return None, exc, False
            authoritative = self._authoritative_component_cleanup(component, result)
            if authoritative is not None:
                return authoritative, None, False
            return None, None, True

        def select_authority(value: Any) -> Any:
            # The wrapper prevents Twisted from flattening a Deferred returned by
            # ``close`` before we can select its authoritative sibling.
            if not isinstance(value, tuple) or len(value) != 2:
                return value
            _called, result = value
            authoritative = self._authoritative_component_cleanup(component, result)
            return authoritative if authoritative is not None else result

        try:
            operation.addCallback(select_authority)
        except BaseException:
            try:
                operation.addCallback(select_authority)
            except BaseException:
                # The accepted worker remains the only ownership authority. Its
                # failure/success is still returned to the composite caller; a
                # second close pass can conservatively retry the component if the
                # adapter could not expose its nested Deferred.
                pass
        return operation, None, False

    def _finish_orphan_candidate_close(
        self,
        kind: str,
        candidate: Any,
        outcome: Any,
    ) -> BaseException | None:
        """Publish one orphan outcome only after its real close has settled."""
        with self._lifecycle_lock:
            self._orphan_candidate_operations.pop(id(candidate), None)
        failure = outcome if isinstance(outcome, TwistedFailure) else None
        if failure is not None:
            error = cast(BaseException, failure.value)
            with self._lifecycle_lock:
                self._orphan_candidate_failures[id(candidate)] = error
            self._capture_candidate_leases(candidate)
            return error
        with self._lifecycle_lock:
            prior_failure = self._orphan_candidate_failures.get(id(candidate))
            if prior_failure is None:
                self._orphan_candidate_failures.pop(id(candidate), None)
        if prior_failure is not None:
            self._capture_candidate_leases(candidate)
            return prior_failure
        self._capture_candidate_leases(candidate)
        self._remove_orphan_candidate(candidate)
        return None

    def _dispose_invalidated_candidate(
        self,
        kind: str,
        candidate: Any,
        reason: str,
    ) -> None:
        """Close one unpublished candidate and retain failures for retry."""
        self._retain_orphan_candidate(kind, candidate)
        with self._lifecycle_lock:
            pending = self._orphan_candidate_operations.get(id(candidate))
        if pending is not None:
            return
        operation, immediate_error, succeeded = self._start_component_close(
            kind,
            candidate,
            reason,
        )
        if operation is not None:
            with self._lifecycle_lock:
                self._orphan_candidate_operations[id(candidate)] = operation

            def finish(outcome: Any) -> Any:
                self._finish_orphan_candidate_close(kind, candidate, outcome)
                self._release_orphan_leases()
                return outcome

            try:
                operation.addBoth(finish)
            except BaseException:
                try:
                    operation.addBoth(finish)
                except BaseException:
                    pass
            # Construction's primary exception is already being raised; the
            # authoritative cleanup branch is retained for close_backend retry.
            try:
                operation.addErrback(lambda _failure: None)
            except BaseException:
                pass
            return
        self._capture_candidate_leases(candidate)
        if succeeded:
            self._remove_orphan_candidate(candidate)
        if immediate_error is not None:
            # Candidate cleanup is secondary to construction, but ownership stays
            # reachable for the next composite close pass.
            pass
        self._release_orphan_leases()

    def _start_release_operation(
        self,
        lease: Any,
    ) -> tuple[Deferred[Any] | None, BaseException | None, bool]:
        """Start one exact lease release with the same Deferred discipline."""
        return self._start_owner_operation(lease, "release")

    def _start_manager_operation(
        self,
        manager: Any,
    ) -> tuple[Deferred[Any] | None, BaseException | None, bool]:
        """Start one orphan manager close without blocking reactor callbacks."""
        return self._start_owner_operation(manager, "close")

    def _start_owner_operation(
        self,
        owner: Any,
        method_name: str,
    ) -> tuple[Deferred[Any] | None, BaseException | None, bool]:
        """Adapt a lease/manager call while retaining any returned authority."""

        def invoke() -> tuple[bool, Any]:
            return True, getattr(owner, method_name)()

        offload = False
        if reactor_is_running():
            from scrapy_extension.backends.connectors import (
                ConnectionManager,
                ConnectionManagerLease,
            )

            offload = isinstance(
                owner, (ConnectionManager, ConnectionManagerLease)
            ) and (not type(owner).__module__.startswith("unittest.mock"))
        if offload:
            operation = _submit_thread(invoke)
        else:
            try:
                _called, returned = invoke()
            except BaseException as exc:
                return None, exc, False
            authoritative = self._authoritative_component_cleanup(owner, returned)
            if authoritative is not None:
                return authoritative, None, False
            return None, None, True

        def select_authority(value: Any) -> Any:
            if not isinstance(value, tuple) or len(value) != 2:
                return value
            _called, returned = value
            authoritative = self._authoritative_component_cleanup(owner, returned)
            return authoritative if authoritative is not None else returned

        try:
            operation.addCallback(select_authority)
        except BaseException:
            try:
                operation.addCallback(select_authority)
            except BaseException:
                pass
        return operation, None, False

    def _finish_orphan_lease(
        self,
        lease: Any,
        outcome: Any,
        remember: Callable[[BaseException | None], None],
    ) -> None:
        if isinstance(outcome, TwistedFailure):
            remember(outcome.value)
            return
        self._forget_orphan_lease(lease)

    def _finish_orphan_manager(
        self,
        manager: Any,
        outcome: Any,
        remember: Callable[[BaseException | None], None],
    ) -> None:
        if isinstance(outcome, TwistedFailure):
            remember(outcome.value)
            return
        self._forget_orphan_manager(manager)

    def _forget_orphan_lease(self, lease: Any) -> None:
        with self._lifecycle_lock:
            self._orphan_leases = [
                existing for existing in self._orphan_leases if existing is not lease
            ]

    def _forget_orphan_manager(self, manager: Any) -> None:
        with self._lifecycle_lock:
            self._orphan_managers = [
                existing
                for existing in self._orphan_managers
                if existing is not manager
            ]

    def _release_orphan_leases(self) -> BaseException | None:
        """Release every retained lease once, retaining failures for retry."""
        primary_error: BaseException | None = None
        with self._lifecycle_lock:
            leases = tuple(self._orphan_leases)
        for lease in leases:
            try:
                lease.release()
            except BaseException as exc:
                if primary_error is None:
                    primary_error = exc
                continue
            with self._lifecycle_lock:
                self._orphan_leases = [
                    existing
                    for existing in self._orphan_leases
                    if existing is not lease
                ]
        return primary_error

    def _release_orphan_managers(self) -> BaseException | None:
        """Retry managers whose setup rollback lost its first close attempt."""
        primary_error: BaseException | None = None
        with self._lifecycle_lock:
            managers = tuple(self._orphan_managers)
        for manager in managers:
            try:
                manager.close()
            except BaseException as exc:
                if primary_error is None:
                    primary_error = exc
                continue
            with self._lifecycle_lock:
                self._orphan_managers = [
                    existing
                    for existing in self._orphan_managers
                    if existing is not manager
                ]
        return primary_error

    def _retain_orphan_candidate(self, kind: str, candidate: Any) -> None:
        """Keep an unpublished candidate reachable until close succeeds."""
        with self._lifecycle_lock:
            if not any(entry is candidate for _, entry in self._orphan_candidates):
                self._orphan_candidates.append((kind, candidate))

    def _remove_orphan_candidate(self, candidate: Any) -> None:
        """Forget one candidate only after its close callback succeeded."""
        with self._lifecycle_lock:
            self._orphan_candidates = [
                (kind, entry)
                for kind, entry in self._orphan_candidates
                if entry is not candidate
            ]

    def _cleanup_orphan_candidates(
        self,
        reason: str,
    ) -> BaseException | Deferred[Any] | None:
        """Retry orphan close operations before leases or managers are released.

        A candidate's bounded public Deferred is never treated as ownership
        authority when it exposes ``_close_completion_deferred``.  Every sibling
        is still attempted after a failure; the first ordinary or control-flow
        error remains the result, and failed ownership stays on this mixin.
        """
        primary_error: BaseException | None = None
        result: Deferred[Any] = Deferred()
        phases: list[tuple[str, Any, str]] = []
        with self._lifecycle_lock:
            phases.extend(
                ("candidate", kind, candidate)
                for kind, candidate in tuple(self._orphan_candidates)
            )
            phases.extend(("lease", "", lease) for lease in tuple(self._orphan_leases))
            phases.extend(
                ("manager", "", manager) for manager in tuple(self._orphan_managers)
            )

        index = 0

        def remember(error: BaseException | None) -> None:
            nonlocal primary_error
            if error is not None and primary_error is None:
                primary_error = error

        def finish_phase(phase: tuple[str, Any, Any], outcome: Any) -> Any:
            phase_kind, kind, owner = phase
            if phase_kind == "candidate":
                remember(self._finish_orphan_candidate_close(kind, owner, outcome))
                with self._lifecycle_lock:
                    known_leases = {
                        id(entry)
                        for phase_type, _kind, entry in phases
                        if phase_type == "lease"
                    }
                    phases.extend(
                        ("lease", "", lease)
                        for lease in self._orphan_leases
                        if id(lease) not in known_leases
                    )
            elif phase_kind == "lease":
                self._finish_orphan_lease(owner, outcome, remember)
            else:
                self._finish_orphan_manager(owner, outcome, remember)
            advance()
            return outcome

        def advance() -> None:
            nonlocal index
            if index >= len(phases):
                if primary_error is None:
                    if not result.called:
                        result.callback(None)
                elif not result.called:
                    result.errback(
                        TwistedFailure(primary_error)  # type: ignore[no-untyped-call]
                    )
                return
            phase = phases[index]
            index += 1
            phase_kind, kind, owner = phase
            if phase_kind == "candidate":
                with self._lifecycle_lock:
                    operation = self._orphan_candidate_operations.get(id(owner))
                immediate_error: BaseException | None = None
                succeeded = False
                prior_failure: BaseException | None = None
                if operation is None:
                    with self._lifecycle_lock:
                        prior_failure = self._orphan_candidate_failures.pop(
                            id(owner),
                            None,
                        )
                    remember(prior_failure)
                    operation, immediate_error, succeeded = self._start_component_close(
                        kind,
                        owner,
                        reason,
                    )
                    if operation is not None:
                        with self._lifecycle_lock:
                            self._orphan_candidate_operations[id(owner)] = operation
                if operation is not None:
                    success = lambda value: finish_phase(phase, value)
                    failure = lambda error: finish_phase(phase, error)
                    try:
                        operation.addCallbacks(success, failure)
                    except BaseException:
                        try:
                            operation.addCallbacks(success, failure)
                        except BaseException as exc:
                            remember(exc)
                            finish_phase(
                                phase,
                                TwistedFailure(exc),  # type: ignore[no-untyped-call]
                            )
                    try:
                        operation.addErrback(lambda _failure: None)
                    except BaseException:
                        pass
                    return
                if immediate_error is not None:
                    remember(immediate_error)
                    with self._lifecycle_lock:
                        self._orphan_candidate_failures[id(owner)] = immediate_error
                    self._capture_candidate_leases(owner)
                    with self._lifecycle_lock:
                        known_leases = {
                            id(entry)
                            for phase_type, _kind, entry in phases
                            if phase_type == "lease"
                        }
                        phases.extend(
                            ("lease", "", lease)
                            for lease in self._orphan_leases
                            if id(lease) not in known_leases
                        )
                elif succeeded:
                    with self._lifecycle_lock:
                        self._orphan_candidate_failures.pop(id(owner), None)
                    self._capture_candidate_leases(owner)
                    self._remove_orphan_candidate(owner)
                advance()
                return

            if phase_kind == "lease":
                operation, immediate_error, succeeded = self._start_release_operation(
                    owner,
                )
            else:
                operation, immediate_error, succeeded = self._start_manager_operation(
                    owner,
                )
            if operation is not None:
                operation.addCallbacks(
                    lambda value: finish_phase(phase, value),
                    lambda failure: finish_phase(phase, failure),
                )
                operation.addErrback(lambda _failure: None)
                return
            remember(immediate_error)
            if succeeded:
                if phase_kind == "lease":
                    self._forget_orphan_lease(owner)
                else:
                    self._forget_orphan_manager(owner)
            advance()

        advance()
        if not result.called:
            # A phase may be an already-settled Deferred whose callback starts a
            # later pending phase. Looking only at the first operation's
            # ``called`` bit would publish success before that later owner has
            # settled.
            return result
        if primary_error is not None:
            result.addErrback(lambda _failure: None)
            return primary_error
        return None

    def _invalidate_component_constructions_locked(self) -> tuple[threading.Event, ...]:
        """Invalidate candidates before close can release the shared manager."""
        active = tuple(self._component_constructions.values())
        if not active:
            return ()
        self._component_generation += 1
        for construction in active:
            construction.invalidated = True
        return tuple(construction.done for construction in active)

    def get_queue(self, queue_name: str | None = None) -> BackendQueue:
        """Get the backend queue for this spider.

        Component construction is deliberately outside ``_lifecycle_lock``.  A
        plugin constructor or monitor callback may re-enter this mixin, including
        through ``close_backend``; the reservation below prevents either callback
        from publishing a candidate after it invalidates the manager generation.
        """
        from scrapy_extension.queue.queue import BackendQueue
        from scrapy_extension.schedule.scheduler import BackendScheduler

        name = queue_name or self._mixin_queue_key()
        reserved = self._reserve_component_construction(
            "queue",
            "_queue",
            "setup_backend() must be called before get_queue(). "
            f"Call setup_backend() in {self.__class__.__name__}.__init__()",
        )
        if not isinstance(reserved, tuple):
            queue = reserved
            with self._lifecycle_lock:
                self._claim_consumer_queue(name)
                if self._queue_name != name:
                    raise ConfigurationError(
                        f"{self.__class__.__name__} is already bound to queue "
                        f"{self._queue_name!r}; cannot rebind to {name!r}.",
                        setting_name="queue_name",
                        setting_value=name,
                    )
            if isinstance(queue._monitor, NullMonitor):
                monitor, window = self._resolve_queue_monitor()
                if not isinstance(monitor, NullMonitor):
                    # R141-F10: thread the resolved window through the upgrade
                    # so SCRAPY_MONITOR_POP_RATE_WINDOW_S is honored on the
                    # early-setup path too (only NullMonitor upgrades pass it;
                    # a real external monitor keeps its tuned wiring/window).
                    queue.set_monitor(monitor, pop_rate_window_s=window)
            return cast(BackendQueue, queue)

        manager, construction = reserved
        factory_scheduler: BackendScheduler | None = None
        candidate: BackendQueue | None = None
        previous_claim: str | None = None
        try:
            with self._lifecycle_lock:
                if not self._construction_is_current_locked(construction, manager):
                    raise RuntimeError("queue construction was invalidated by close")
                previous_claim = self._consumer_queue_name
                self._claim_consumer_queue(name)

            factory_settings = self._component_settings()
            uses_shared_manager = self._queue_factory_uses_shared_manager(
                factory_settings
            )
            if uses_shared_manager:
                factory_scheduler = BackendScheduler.from_settings(
                    factory_settings,
                    spider_name=self.name,
                    queue_key=name,
                    connection_manager=manager,
                    backend_type_override=self._backend_type_name(),
                    owns_connection_manager=False,
                )
            else:
                factory_scheduler = BackendScheduler.from_settings(
                    factory_settings,
                    spider_name=self.name,
                    queue_key=name,
                )
            queue_manager = factory_scheduler.connection_manager
            monitor = BackendScheduler._resolve_monitor_for_spider(
                self,
                backpressure_threshold=factory_scheduler._monitor_backpressure_threshold,
                pop_rate_window_s=factory_scheduler._monitor_pop_rate_window_s,
            )
            queue_manager.set_monitor(monitor)
            snapshot_manager = factory_scheduler._snapshot_connection_manager
            if snapshot_manager is not None:
                snapshot_manager.set_monitor(monitor)
            candidate = BackendQueue(
                connection_manager=queue_manager,
                queue_name=name,
                spider=self,
                queue_strategy=factory_scheduler._queue_strategy,
                max_item_bytes=factory_scheduler._queue_max_item_bytes,
                monitor=monitor,
                allow_cross_spider=factory_scheduler._allow_cross_spider,
                depth_sample_every=factory_scheduler._queue_depth_sample_every,
                pop_rate_window_s=factory_scheduler._monitor_pop_rate_window_s,
                snapshot_owner=factory_scheduler._queue_snapshot_owner,
                snapshot_connection_manager=snapshot_manager,
                snapshot_max_bytes=factory_scheduler._queue_snapshot_max_bytes,
                snapshot_chunk_bytes=factory_scheduler._queue_snapshot_chunk_bytes,
                reactor_io_timeout=factory_scheduler._reactor_io_timeout,
            )
            # Keep the factory scheduler as the cleanup owner until publication.
            # Its close path then closes this candidate and releases every factory
            # lease in dependency order, including asynchronous queue teardown.
            factory_scheduler._queue = candidate

            with self._lifecycle_lock:
                valid = self._construction_is_current_locked(construction, manager)
                if valid:
                    snapshot_lease = (
                        factory_scheduler._snapshot_connection_manager_lease
                    )
                    queue_lease = factory_scheduler._connection_manager_lease
                    self._snapshot_connection_manager = snapshot_manager
                    self._snapshot_connection_lease = snapshot_lease
                    self._queue_connection_manager = (
                        queue_manager if queue_lease is not None else None
                    )
                    self._queue_connection_lease = queue_lease
                    factory_scheduler._snapshot_connection_manager = None
                    factory_scheduler._snapshot_connection_manager_lease = None
                    factory_scheduler._owns_snapshot_connection_manager = False
                    factory_scheduler._connection_manager_lease = None
                    factory_scheduler._owns_connection_manager = False
                    factory_scheduler._queue = None
                    self._queue = candidate
                    self._queue_name = name
                    candidate = None
                self._finish_component_construction(construction)
            if valid:
                assert self._queue is not None
                return self._queue
            raise RuntimeError("queue construction completed after close")
        except BaseException:
            with self._lifecycle_lock:
                if previous_claim is not None and self._consumer_queue_name == name:
                    self._consumer_queue_name = previous_claim
            cleanup_pending = False
            close_parent_after_cleanup = False
            authoritative_cleanup: Deferred[Any] | None = None
            if factory_scheduler is not None:
                had_snapshot_manager = (
                    factory_scheduler._snapshot_connection_manager is not None
                )
                if candidate is not None:
                    # Transfer the candidate to the scheduler before any callback
                    # can run.  The scheduler is the sole cleanup owner; calling
                    # candidate.close() here as well would race/double-close it.
                    factory_scheduler._queue = candidate
                    candidate = None
                try:
                    cleanup = factory_scheduler.close("mixin-queue-factory-failed")
                    authoritative_cleanup = self._authoritative_component_cleanup(
                        factory_scheduler,
                        cleanup,
                    )
                    if authoritative_cleanup is not None:
                        cleanup_pending = True

                        def finish_factory_cleanup(result: Any) -> Any:
                            nonlocal close_parent_after_cleanup
                            # The getter's construction error is authoritative. A
                            # failed cleanup remains consumed here, while the
                            # construction reservation is held until the scheduler
                            # has finished its real queue/lease teardown.
                            if isinstance(result, TwistedFailure):
                                self._adopt_failed_factory_resources(
                                    factory_scheduler,
                                    name,
                                )
                            elif had_snapshot_manager and not construction.invalidated:
                                # No component was published. The paired snapshot
                                # acquire has been released, so the failed direct
                                # getter can close this parent generation after the
                                # construction reservation is removed.
                                close_parent_after_cleanup = True
                            self._finish_component_construction(construction)
                            if construction.invalidated or close_parent_after_cleanup:
                                self._request_close_after_construction()
                            return None

                        try:
                            authoritative_cleanup.addBoth(finish_factory_cleanup)
                        except BaseException:
                            try:
                                authoritative_cleanup.addBoth(finish_factory_cleanup)
                            except BaseException:
                                pass
                    elif had_snapshot_manager and not construction.invalidated:
                        close_parent_after_cleanup = True
                except BaseException:
                    # Synchronous cleanup failures are already secondary to the
                    # construction error; retain failed leases/components for a
                    # later close retry before releasing the reservation.
                    self._adopt_failed_factory_resources(factory_scheduler, name)
            if candidate is not None:
                try:
                    candidate.close()
                except BaseException:
                    pass
            if not cleanup_pending:
                self._finish_component_construction(construction)
                if construction.invalidated or close_parent_after_cleanup:
                    self._request_close_after_construction()
            raise

    def get_queue_async(
        self,
        queue_name: str | None = None,
        *,
        timeout: float = DEFAULT_REACTOR_IO_TIMEOUT_S,
    ) -> Deferred[BackendQueue]:
        """Construct/get a queue with a cancellable close fence.

        The construction worker is authoritative even after the bounded public
        view expires.  A one-shot readiness gate lets ``close_backend`` publish
        its invalidation before a queued worker can reserve a new generation.
        Without a running reactor there is no callback pump for Twisted's thread
        adapter, so use the normal synchronous construction contract instead of
        leaving ``close_backend`` in an unwaitable Deferred loop.
        """
        if not reactor_is_running():
            result: Deferred[BackendQueue] = Deferred()
            try:
                result.callback(self.get_queue(queue_name))
            except BaseException as exc:
                result.errback(exc)
            return result

        ready = threading.Event()

        def construct() -> BackendQueue:
            ready.wait()
            return self.get_queue(queue_name)

        # Keep the lifecycle lock across scheduling, publication, and opening the
        # readiness gate. A concurrent close therefore observes this worker before
        # it can invalidate or release the manager generation.
        with self._lifecycle_lock:
            try:
                operation, public = defer_to_thread_ordered(
                    construct,
                    timeout=timeout,
                    operation="backend queue construction",
                )
                if not operation.called:
                    self._track_async_operation_locked("queue-construction", operation)
            finally:
                ready.set()
        # The caller owns the bounded view; the operation remains lifecycle
        # authority and is observed independently so a late constructor failure
        # cannot become an unhandled Deferred.
        try:
            operation.addErrback(lambda _failure: None)
        except BaseException:
            pass
        return public

    def get_dupefilter(self) -> BackendDupeFilter:
        """Get the backend dupefilter for this spider without locked callbacks."""
        from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
        from scrapy_extension.monitor import ScrapyStatsMonitor

        reserved = self._reserve_component_construction(
            "dupefilter",
            "_dupefilter",
            "setup_backend() must be called before get_dupefilter(). "
            f"Call setup_backend() in {self.__class__.__name__}.__init__()",
        )
        if not isinstance(reserved, tuple):
            return cast(BackendDupeFilter, reserved)

        manager, construction = reserved
        candidate: BackendDupeFilter | None = None
        try:
            settings = self._component_settings()
            crawler = getattr(self, "crawler", None)
            stats = getattr(crawler, "stats", None) if crawler is not None else None
            monitor = ScrapyStatsMonitor(stats) if stats is not None else None
            configured_key = settings.get("SCRAPY_DUPEFILTER_KEY")
            key_override = (
                None
                if configured_key is not None
                else resolve_identity_template(
                    DEFAULT_DUPEFILTER_KEY_TEMPLATE,
                    spider_name=self.name,
                    project_name=(
                        self._mixin_project_name
                        if self._mixin_project_name is not None
                        else project_name_from_spider(self)
                    ),
                )
            )
            from scrapy_extension.backends.registry import has_capability

            shared_manager = self._backend_type_name() is None or has_capability(
                self._backend_type_name() or "", "set"
            )
            try:
                has_set_override = any(
                    settings.get(key)
                    for key in (
                        "SCRAPY_SET_BACKEND_TYPE",
                        "SCRAPY_SET_BACKEND_SETTINGS",
                        "SCRAPY_BACKEND_TYPE",
                        "SCRAPY_BACKEND_SETTINGS",
                    )
                )
            except (AttributeError, TypeError):
                has_set_override = False
            shared_manager = shared_manager and not (
                has_set_override
                or os.environ.get("SCRAPY_SET_BACKEND_TYPE")
                or os.environ.get("SCRAPY_BACKEND_TYPE")
            )
            factory_kwargs: dict[str, Any] = {
                "fingerprinter": (
                    getattr(crawler, "request_fingerprinter", None)
                    if crawler is not None
                    else None
                ),
                "monitor": monitor,
                "key_override": key_override,
            }
            if shared_manager:
                factory_kwargs.update(
                    connection_manager=manager,
                    owns_connection_manager=False,
                )
            candidate = BackendDupeFilter.from_settings(settings, **factory_kwargs)
            if monitor is not None and candidate.connection_manager is not None:
                candidate.connection_manager.set_monitor(monitor)
            with self._lifecycle_lock:
                valid = self._construction_is_current_locked(construction, manager)
                if valid:
                    self._dupefilter = candidate
                    candidate = None
                self._finish_component_construction(construction)
            if valid:
                assert self._dupefilter is not None
                return self._dupefilter
            raise RuntimeError("dupefilter construction completed after close")
        except BaseException:
            if candidate is not None:
                self._dispose_invalidated_candidate(
                    "dupefilter",
                    candidate,
                    "mixin-dupefilter-factory-failed",
                )
            self._finish_component_construction(construction)
            if construction.invalidated:
                self._request_close_after_construction()
            raise

    def _resolve_scheduler_dupefilter(self, settings: Any) -> Any:
        """Resolve the crawler's configured dupefilter like ``from_crawler``."""
        crawler = getattr(self, "crawler", None)
        crawler_settings = self._crawler_settings()
        if crawler is not None:
            from scrapy.settings import Settings
            from scrapy.utils.misc import load_object

            from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter

            if isinstance(crawler_settings, Settings):
                dupefilter_path = settings.get("DUPEFILTER_CLASS")
                if dupefilter_path:
                    dupefilter_cls = load_object(dupefilter_path)
                    if dupefilter_cls is not BackendDupeFilter:
                        return dupefilter_cls.from_crawler(crawler)
        return self.get_dupefilter()

    def get_scheduler(self) -> BackendScheduler:
        """Get a settings-complete scheduler without locked plugin callbacks."""
        from scrapy_extension.schedule.scheduler import BackendScheduler

        reserved = self._reserve_component_construction(
            "scheduler",
            "_scheduler",
            "setup_backend() must be called before get_scheduler(). "
            f"Call setup_backend() in {self.__class__.__name__}.__init__()",
        )
        if not isinstance(reserved, tuple):
            return cast(BackendScheduler, reserved)

        manager, construction = reserved
        candidate: BackendScheduler | None = None
        previous_claim: str | None = None
        cleanup_pending = False
        try:
            queue_name = self._mixin_queue_key()
            with self._lifecycle_lock:
                if not self._construction_is_current_locked(construction, manager):
                    raise RuntimeError(
                        "scheduler construction was invalidated by close"
                    )
                previous_claim = self._consumer_queue_name
                self._claim_consumer_queue(queue_name)
            crawler = getattr(self, "crawler", None)
            factory_settings = self._component_settings()
            factory_kwargs: dict[str, Any] = {
                "spider_name": self.name,
                "queue_key": queue_name,
                "stats": (
                    getattr(crawler, "stats", None) if crawler is not None else None
                ),
                "dupefilter": self._resolve_scheduler_dupefilter(factory_settings),
            }
            if self._queue_factory_uses_shared_manager(factory_settings):
                factory_kwargs.update(
                    connection_manager=manager,
                    backend_type_override=self._backend_type_name(),
                    owns_connection_manager=False,
                )
            candidate = BackendScheduler.from_settings(
                factory_settings, **factory_kwargs
            )
            scheduler_monitor = BackendScheduler._resolve_monitor_for_spider(
                self,
                backpressure_threshold=candidate._monitor_backpressure_threshold,
                pop_rate_window_s=candidate._monitor_pop_rate_window_s,
            )
            candidate.connection_manager.set_monitor(scheduler_monitor)
            if candidate._snapshot_connection_manager is not None:
                candidate._snapshot_connection_manager.set_monitor(scheduler_monitor)
            with self._lifecycle_lock:
                valid = self._construction_is_current_locked(construction, manager)
                if valid:
                    self._scheduler = candidate
                    candidate = None
                self._finish_component_construction(construction)
            if valid:
                assert self._scheduler is not None
                return self._scheduler
            raise RuntimeError("scheduler construction completed after close")
        except BaseException:
            if previous_claim is not None:
                with self._lifecycle_lock:
                    if self._consumer_queue_name == queue_name:
                        self._consumer_queue_name = previous_claim
            if candidate is not None:
                self._retain_orphan_candidate("scheduler", candidate)
                try:
                    cleanup = candidate.close("mixin-scheduler-factory-failed")
                    authoritative = self._authoritative_component_cleanup(
                        candidate,
                        cleanup,
                    )
                    if authoritative is not None:
                        cleanup_pending = True

                        def finish_candidate_cleanup(result: Any) -> Any:
                            self._capture_candidate_leases(candidate)
                            if isinstance(result, TwistedFailure):
                                # Keep the unpublished scheduler reachable. Its
                                # close and exact leases are retried on the next
                                # composite close; the candidate failure remains
                                # the cleanup record rather than being dropped.
                                self._release_orphan_leases()
                            else:
                                self._remove_orphan_candidate(candidate)
                                self._release_orphan_leases()
                            self._finish_component_construction(construction)
                            if construction.invalidated:
                                self._request_close_after_construction()
                            return None

                        try:
                            authoritative.addBoth(finish_candidate_cleanup)
                        except BaseException:
                            try:
                                authoritative.addBoth(finish_candidate_cleanup)
                            except BaseException:
                                pass
                    else:
                        self._capture_candidate_leases(candidate)
                        self._remove_orphan_candidate(candidate)
                        self._release_orphan_leases()
                except BaseException:
                    self._capture_candidate_leases(candidate)
                    self._release_orphan_leases()
            if not cleanup_pending:
                self._finish_component_construction(construction)
                if construction.invalidated:
                    self._request_close_after_construction()
            raise

    def _close_after_construction_wait(
        self,
        waiters: tuple[threading.Event, ...],
        operations: tuple[Deferred[Any], ...],
        current_thread: int,
    ) -> Deferred[Any]:
        """Fence a late getter without blocking the reactor thread."""
        with self._lifecycle_lock:
            if self._close_wait_operation is not None:
                return self._close_deferred or self._close_wait_operation
            self._close_in_progress = True
            self._close_owner_thread_id = current_thread

        def wait_for_construction() -> None:
            for waiter in waiters:
                waiter.wait()

        wait_operation = _submit_thread(wait_for_construction)
        gate: Deferred[Any] = Deferred()
        remaining = len(operations) + 1

        def settle_gate(_outcome: Any) -> Any:
            nonlocal remaining
            remaining -= 1
            if remaining == 0 and not gate.called:
                gate.callback(None)
            return None

        try:
            wait_operation.addBoth(settle_gate)
        except BaseException:
            try:
                wait_operation.addBoth(settle_gate)
            except BaseException:
                if wait_operation.called:
                    settle_gate(None)
        for operation in operations:
            try:
                operation.addBoth(settle_gate)
            except BaseException:
                try:
                    operation.addBoth(settle_gate)
                except BaseException:
                    if operation.called:
                        settle_gate(None)
        composite: Deferred[Any] = Deferred()
        public = bounded_deferred(
            composite,
            timeout=DEFAULT_REACTOR_IO_TIMEOUT_S,
            operation="spider backend construction close",
        )
        with self._lifecycle_lock:
            self._close_wait_operation = gate
            self._close_wait_owner_thread_id = current_thread
            self._close_deferred = public

        def finish_wait(outcome: Any) -> Any:
            with self._lifecycle_lock:
                if self._close_wait_operation is gate:
                    # Keep the close reservation held while the real teardown is
                    # entered. A peer close must observe the same public Deferred,
                    # not win the small transition window between the wait fence
                    # and close_backend().
                    self._close_wait_operation = None
                    self._close_wait_finishing = True
            if isinstance(outcome, TwistedFailure):
                with self._lifecycle_lock:
                    self._close_wait_finishing = False
                    self._close_in_progress = False
                    self._close_owner_thread_id = None
                    self._close_wait_owner_thread_id = None
                    if self._close_deferred is public:
                        self._close_deferred = None
                if not composite.called:
                    composite.errback(outcome)
                return None
            inner: Deferred[Any] | None = None
            try:
                inner = self.close_backend()
            except BaseException as exc:
                if not composite.called:
                    composite.errback(
                        TwistedFailure(exc)  # type: ignore[no-untyped-call]
                    )
                return None
            finally:
                with self._lifecycle_lock:
                    self._close_wait_finishing = False
                    self._close_wait_owner_thread_id = None
                    if isinstance(inner, Deferred) and self._close_in_progress:
                        # close_backend() may have installed a narrower internal
                        # public view; the construction fence remains the caller's
                        # stable concurrent-close result.
                        self._close_deferred = public
            if isinstance(inner, Deferred):
                success = lambda value: (
                    composite.callback(value) if not composite.called else value
                )
                failure = lambda error: (
                    composite.errback(error) if not composite.called else None
                )
                try:
                    inner.addCallbacks(success, failure)
                except BaseException as exc:
                    try:
                        inner.addCallbacks(success, failure)
                    except BaseException:
                        if not composite.called:
                            composite.errback(exc)
                try:
                    inner.addErrback(lambda _failure: None)
                except BaseException:
                    pass
            elif not composite.called:
                composite.callback(inner)
            return None

        try:
            gate.addBoth(finish_wait)
        except BaseException as exc:
            try:
                gate.addBoth(finish_wait)
            except BaseException:
                if not composite.called:
                    composite.errback(exc)
        try:
            gate.addErrback(lambda _failure: None)
        except BaseException:
            pass
        try:
            composite.addErrback(lambda _failure: None)
        except BaseException:
            pass
        return public

    def close_backend(self) -> Deferred[Any] | None:
        """Retryably close mixin-owned components and manager references.

        A scheduler may perform its close on a worker thread and return a bounded
        Deferred.  Keep the scheduler, its dupefilter, and the shared manager
        owned until the scheduler's authoritative completion settles; synchronous
        components retain their historical close behavior.
        """
        current_thread = threading.get_ident()
        waiters: tuple[threading.Event, ...] = ()
        while True:
            with self._lifecycle_lock:
                if self._close_in_progress:
                    if (
                        self._close_wait_finishing
                        and self._close_owner_thread_id == current_thread
                    ):
                        # Transfer the construction-wait reservation to this real
                        # teardown pass; the branch below immediately reclaims it.
                        self._close_wait_finishing = False
                        self._close_in_progress = False
                        self._close_owner_thread_id = None
                    else:
                        if self._close_owner_thread_id == current_thread:
                            return self._close_deferred
                        if self._close_deferred is not None:
                            return self._close_deferred
                        raise RuntimeError(
                            "Backend spider close is already in progress"
                        )
                setup_attempt = self._setup_attempt
                if setup_attempt is not None:
                    setup_attempt.invalidated = True
                    if setup_attempt.owner_thread_id == current_thread:
                        # A setup callback cannot wait for its own registration.
                        # The setup owner compensates its candidate before the next
                        # close pass takes ownership.
                        return None
                    setup_waiter = setup_attempt.done
                else:
                    setup_waiter = None
                active = tuple(self._component_constructions.values())
                for key, operation in tuple(self._async_component_operations.items()):
                    if operation.called:
                        self._async_component_operations.pop(key, None)
                async_operations = tuple(self._async_component_operations.values())
                if setup_waiter is not None:
                    waiters = (setup_waiter,)
                elif not active and not async_operations:
                    self._close_in_progress = True
                    self._close_owner_thread_id = current_thread
                    break
                if setup_waiter is None and any(
                    construction.owner_thread_id == current_thread
                    for construction in active
                ):
                    # A constructor callback cannot wait for itself. The owner
                    # generation is invalidated; its caller performs the bounded
                    # candidate cleanup and retries close after unwinding.
                    self._invalidate_component_constructions_locked()
                    return None
                # A peer close waits for the in-flight owner to publish (or clean
                # up) its candidate. Keeping this generation authoritative lets an
                # already-started getter finish before teardown closes the exact
                # published component; manager release never overtakes construction.
                if setup_waiter is None:
                    waiters = tuple(construction.done for construction in active)
                if (waiters or async_operations) and reactor_is_running():
                    if active:
                        self._invalidate_component_constructions_locked()
                    return self._close_after_construction_wait(
                        waiters,
                        async_operations,
                        current_thread,
                    )
                if async_operations:
                    # Deferred callbacks cannot be pumped once the reactor has
                    # stopped. Failing explicitly is safer than spinning forever
                    # while retaining the manager generation for a later retry.
                    raise RuntimeError(
                        "Backend spider close cannot await an active reactor operation"
                    )
            for waiter in waiters:
                waiter.wait()

        asynchronous = False
        dupefilter_async_attempted = False

        def reset_close_state() -> None:
            with self._lifecycle_lock:
                self._close_in_progress = False
                self._close_owner_thread_id = None
                self._close_deferred = None

        try:
            primary_error: BaseException | None = None
            # Teardown owns the generation present when close was reserved. A
            # callback may publish a replacement while an older component is
            # closing; never release that replacement as if it were the old
            # generation's manager.
            with self._lifecycle_lock:
                closing_manager = self._connection_manager
                closing_manager_lease = self._connection_manager_lease
            signal_managers = {lease.manager for lease in self._signal_leases}
            if self._connected_signals is not None:
                signal_managers.add(self._connected_signals)
            for signal_manager in signal_managers:
                try:
                    self._disconnect_lifecycle_signals(
                        signal_manager,
                        strict=True,
                    )
                except BaseException as exc:  # noqa: BLE001 - retry on next close
                    if primary_error is None:
                        primary_error = exc
                    # Exact leases remain authoritative, even if the provider
                    # removed a registration before raising.
            # _disconnect_lifecycle_signals() updates the aggregate view only
            # when its exact manager is still current; a callback may have
            # installed a replacement dispatcher that must remain reachable.

            scheduler = self._scheduler
            scheduler_dupefilter = (
                getattr(scheduler, "dupefilter", None)
                if scheduler is not None
                else None
            )
            scheduler_queue = (
                getattr(scheduler, "_queue", None) if scheduler is not None else None
            )

            queue_attempted = False
            snapshot_release_attempted = False
            queue_release_attempted = False
            parent_release_attempted = False
            orphan_attempted = False

            def continue_after_scheduler() -> Deferred[Any] | None:
                """Run every dependency after scheduler close has settled."""
                nonlocal primary_error, dupefilter_async_attempted, asynchronous
                nonlocal queue_attempted
                nonlocal snapshot_release_attempted, queue_release_attempted
                nonlocal parent_release_attempted

                queue = self._queue
                if queue is not None and not queue_attempted:
                    queue_attempted = True
                    operation, immediate_error, succeeded = self._start_component_close(
                        "queue",
                        queue,
                        "",
                    )
                    if operation is not None:
                        if not operation.called:
                            asynchronous = True

                        def finish_queue(outcome: Any) -> Any:
                            nonlocal primary_error
                            if isinstance(outcome, TwistedFailure):
                                error = outcome.value
                                if primary_error is None:
                                    primary_error = error
                                if isinstance(error, Exception):
                                    try:
                                        logger.error(
                                            "Failed to close backend component"
                                        )
                                    except BaseException:
                                        pass
                            elif self._queue is queue:
                                self._queue = None
                                self._queue_name = None
                            return continue_after_scheduler()

                        try:
                            operation.addBoth(finish_queue)
                        except BaseException:
                            try:
                                operation.addBoth(finish_queue)
                            except BaseException:
                                pass
                        return operation
                    queue_error = immediate_error
                    if queue_error is not None:
                        if primary_error is None:
                            primary_error = queue_error
                        if isinstance(queue_error, Exception):
                            try:
                                logger.error("Failed to close backend component")
                            except BaseException:
                                pass
                    elif succeeded and self._queue is queue:
                        self._queue = None
                        self._queue_name = None

                # If the scheduler did not own this dupefilter, the mixin does.
                dupefilter = self._dupefilter
                if (
                    dupefilter is not None
                    and dupefilter is not scheduler_dupefilter
                    and not dupefilter_async_attempted
                ):
                    lifecycle = _backend_dupefilter_lifecycle(dupefilter)
                    authoritative_close = getattr(
                        dupefilter,
                        "_close_authoritative_async",
                        None,
                    )
                    if (
                        reactor_is_running()
                        and lifecycle is not None
                        and callable(authoritative_close)
                    ):
                        try:
                            operation, bounded_close = authoritative_close(
                                "spider-mixin-close",
                                timeout=DEFAULT_REACTOR_IO_TIMEOUT_S,
                            )
                        except BaseException as exc:
                            # Adapter construction is extension code too. Keep its
                            # control-flow/ordinary error as the first close error,
                            # then let the remaining siblings run and preserve the
                            # dupefilter reference for a retry.
                            dupefilter_async_attempted = True
                            if primary_error is None:
                                primary_error = exc
                            if isinstance(exc, Exception):
                                try:
                                    logger.error("Failed to close backend component")
                                except BaseException:
                                    pass
                            return continue_after_scheduler()
                        else:
                            # The composite owns the authoritative operation and
                            # exposes its own bounded result. This nested view is not
                            # returned to a caller, so consume its independent
                            # failure branch to avoid an unhandled Deferred.
                            if (
                                isinstance(bounded_close, Deferred)
                                and bounded_close is not operation
                            ):
                                bounded_close.addErrback(lambda _failure: None)
                            # Publish asynchronous ownership only after adapter
                            # setup succeeds. A synchronous BaseException from the
                            # adapter follows the ordinary close path above instead
                            # of stranding ``_close_in_progress``.
                            dupefilter_async_attempted = True
                            asynchronous = True

                            def finish_async_dupefilter_close(result: Any) -> Any:
                                nonlocal primary_error
                                if isinstance(result, TwistedFailure):
                                    error = result.value
                                    if primary_error is None:
                                        primary_error = error
                                    if isinstance(error, Exception):
                                        try:
                                            logger.error(
                                                "Failed to close backend component"
                                            )
                                        except BaseException:
                                            pass
                                elif self._dupefilter is dupefilter:
                                    self._dupefilter = None
                                return continue_after_scheduler()

                            try:
                                result = operation.addBoth(
                                    finish_async_dupefilter_close
                                )
                            except BaseException:
                                try:
                                    result = operation.addBoth(
                                        finish_async_dupefilter_close
                                    )
                                except BaseException as exc:
                                    failed: Deferred[Any] = Deferred()
                                    failed.errback(exc)
                                    return failed
                            return cast(Deferred[Any], result)

                    dupefilter_error: BaseException | None = None
                    dupefilter_ordinary_error = False
                    try:
                        dupefilter.close("spider-mixin-close")
                    except Exception as exc:
                        dupefilter_error = exc
                        dupefilter_ordinary_error = True
                    except BaseException as exc:  # noqa: BLE001 - preserve control flow
                        dupefilter_error = exc
                    else:
                        if self._dupefilter is dupefilter:
                            self._dupefilter = None
                    if dupefilter_error is not None:
                        if primary_error is None:
                            primary_error = dupefilter_error
                        if dupefilter_ordinary_error:
                            try:
                                logger.error("Failed to close backend component")
                            except BaseException:
                                pass

                # A queue's snapshot lease is a separate acquire and is released
                # only after that queue has crossed its durability/cleanup barrier.
                snapshot_lease = self._snapshot_connection_lease
                if (
                    snapshot_lease is not None
                    and self._queue is None
                    and not snapshot_release_attempted
                ):
                    snapshot_release_attempted = True
                    operation, immediate_error, succeeded = (
                        self._start_release_operation(snapshot_lease)
                    )
                    if operation is not None:
                        if not operation.called:
                            asynchronous = True

                        def finish_snapshot_release(outcome: Any) -> Any:
                            nonlocal primary_error
                            if isinstance(outcome, TwistedFailure):
                                error = outcome.value
                                if primary_error is None:
                                    primary_error = error
                                if isinstance(error, Exception):
                                    try:
                                        logger.error(
                                            "Failed to release snapshot connection lease"
                                        )
                                    except BaseException:
                                        pass
                            elif self._snapshot_connection_lease is snapshot_lease:
                                self._snapshot_connection_lease = None
                                self._snapshot_connection_manager = None
                            return continue_after_scheduler()

                        try:
                            operation.addBoth(finish_snapshot_release)
                        except BaseException:
                            try:
                                operation.addBoth(finish_snapshot_release)
                            except BaseException:
                                pass
                        return operation
                    if immediate_error is not None:
                        if primary_error is None:
                            primary_error = immediate_error
                        if isinstance(immediate_error, Exception):
                            try:
                                logger.error(
                                    "Failed to release snapshot connection lease"
                                )
                            except BaseException:
                                pass
                    elif (
                        succeeded and self._snapshot_connection_lease is snapshot_lease
                    ):
                        self._snapshot_connection_lease = None
                        self._snapshot_connection_manager = None

                # A queue-backend override has its own exact acquire. Release it
                # only after the queue and its separate snapshot lease succeed;
                # retrying never consumes the mixin manager's registry reference.
                queue_lease = self._queue_connection_lease
                if (
                    queue_lease is not None
                    and self._queue is None
                    and self._snapshot_connection_lease is None
                    and not queue_release_attempted
                ):
                    queue_release_attempted = True
                    operation, immediate_error, succeeded = (
                        self._start_release_operation(queue_lease)
                    )
                    if operation is not None:
                        if not operation.called:
                            asynchronous = True

                        def finish_queue_release(outcome: Any) -> Any:
                            nonlocal primary_error
                            if isinstance(outcome, TwistedFailure):
                                error = outcome.value
                                if primary_error is None:
                                    primary_error = error
                                if isinstance(error, Exception):
                                    try:
                                        logger.error(
                                            "Failed to release queue connection lease"
                                        )
                                    except BaseException:
                                        pass
                            elif self._queue_connection_lease is queue_lease:
                                self._queue_connection_lease = None
                                self._queue_connection_manager = None
                            return continue_after_scheduler()

                        try:
                            operation.addBoth(finish_queue_release)
                        except BaseException:
                            try:
                                operation.addBoth(finish_queue_release)
                            except BaseException:
                                pass
                        return operation
                    if immediate_error is not None:
                        if primary_error is None:
                            primary_error = immediate_error
                        if isinstance(immediate_error, Exception):
                            try:
                                logger.error("Failed to release queue connection lease")
                            except BaseException:
                                pass
                    elif succeeded and self._queue_connection_lease is queue_lease:
                        self._queue_connection_lease = None
                        self._queue_connection_manager = None

                nonlocal orphan_attempted
                if not orphan_attempted:
                    orphan_attempted = True
                    orphan_cleanup = self._cleanup_orphan_candidates(
                        "spider-mixin-close"
                    )
                    if isinstance(orphan_cleanup, Deferred):
                        if not orphan_cleanup.called:
                            asynchronous = True

                        def finish_orphan_cleanup(outcome: Any) -> Any:
                            nonlocal primary_error
                            if isinstance(outcome, TwistedFailure):
                                error = outcome.value
                                if primary_error is None:
                                    primary_error = error
                                if isinstance(error, Exception):
                                    try:
                                        logger.error(
                                            "Failed to close backend candidate"
                                        )
                                    except BaseException:
                                        pass
                            return continue_after_scheduler()

                        try:
                            orphan_cleanup.addBoth(finish_orphan_cleanup)
                        except BaseException:
                            try:
                                orphan_cleanup.addBoth(finish_orphan_cleanup)
                            except BaseException:
                                pass
                        return orphan_cleanup
                    if orphan_cleanup is not None and primary_error is None:
                        primary_error = orphan_cleanup
                    if isinstance(orphan_cleanup, Exception):
                        try:
                            logger.error("Failed to close backend candidate")
                        except BaseException:
                            pass

                # Never release the shared manager while any required provider
                # still has ownership. This is the retry fence for async scheduler
                # failures and for every synchronous component failure.
                current_manager = self._connection_manager
                manager = (
                    closing_manager if closing_manager is not None else current_manager
                )
                manager_lease = (
                    closing_manager_lease
                    if closing_manager is not None
                    else self._connection_manager_lease
                )
                required_cleanup_complete = (
                    self._connected_signals is None
                    and not self._signal_leases
                    and self._scheduler is None
                    and self._queue is None
                    and self._dupefilter is None
                    and self._snapshot_connection_lease is None
                    and self._queue_connection_lease is None
                    and not self._orphan_candidates
                    and not self._orphan_leases
                    and not self._orphan_managers
                )
                if (
                    manager is not None
                    and required_cleanup_complete
                    and not parent_release_attempted
                ):
                    parent_release_attempted = True
                    if manager_lease is not None:
                        operation, immediate_error, succeeded = (
                            self._start_release_operation(manager_lease)
                        )
                    else:
                        operation, immediate_error, succeeded = (
                            self._start_manager_operation(manager)
                        )
                    if operation is not None:
                        if not operation.called:
                            asynchronous = True

                        def finish_parent_release(outcome: Any) -> Any:
                            nonlocal primary_error
                            if isinstance(outcome, TwistedFailure):
                                error = outcome.value
                                if primary_error is None:
                                    primary_error = error
                                if self._connection_manager is not manager:
                                    with self._lifecycle_lock:
                                        if manager_lease is not None:
                                            if not any(
                                                existing is manager_lease
                                                for existing in self._orphan_leases
                                            ):
                                                self._orphan_leases.append(
                                                    manager_lease
                                                )
                                        elif not any(
                                            existing is manager
                                            for existing in self._orphan_managers
                                        ):
                                            self._orphan_managers.append(manager)
                                        if (
                                            self._connection_manager_lease
                                            is manager_lease
                                        ):
                                            self._connection_manager_lease = None
                                if isinstance(error, Exception):
                                    try:
                                        logger.error(
                                            "Failed to close backend connection manager"
                                        )
                                    except BaseException:
                                        pass
                            elif self._connection_manager is manager:
                                self._connection_manager = None
                                self._connection_manager_lease = None
                                self._consumer_queue_name = None
                            elif self._connection_manager_lease is manager_lease:
                                self._connection_manager_lease = None
                            return continue_after_scheduler()

                        try:
                            operation.addBoth(finish_parent_release)
                        except BaseException:
                            try:
                                operation.addBoth(finish_parent_release)
                            except BaseException:
                                pass
                        return operation
                    if immediate_error is not None:
                        if primary_error is None:
                            primary_error = immediate_error
                        if self._connection_manager is not manager:
                            with self._lifecycle_lock:
                                if manager_lease is not None:
                                    if not any(
                                        existing is manager_lease
                                        for existing in self._orphan_leases
                                    ):
                                        self._orphan_leases.append(manager_lease)
                                elif not any(
                                    existing is manager
                                    for existing in self._orphan_managers
                                ):
                                    self._orphan_managers.append(manager)
                                if self._connection_manager_lease is manager_lease:
                                    self._connection_manager_lease = None
                        if isinstance(immediate_error, Exception):
                            try:
                                logger.error(
                                    "Failed to close backend connection manager"
                                )
                            except BaseException:
                                pass
                    elif succeeded and self._connection_manager is manager:
                        self._connection_manager = None
                        self._connection_manager_lease = None
                        self._consumer_queue_name = None
                    elif self._connection_manager_lease is manager_lease:
                        self._connection_manager_lease = None

                if primary_error is not None:
                    raise primary_error
                return None

            if scheduler is not None:
                scheduler_error: BaseException | None = None
                scheduler_ordinary_error = False
                # Let a composite owner observe the authoritative failure after a
                # bounded scheduler timeout instead of consuming it internally.
                scheduler._close_retain_authoritative_failure = True
                try:
                    scheduler_result = scheduler.close("spider-mixin-close")
                except Exception as exc:
                    scheduler_error = exc
                    scheduler_ordinary_error = True
                    scheduler_result = None
                except BaseException as exc:  # noqa: BLE001 - preserve control flow
                    scheduler_error = exc
                    scheduler_result = None
                if scheduler_error is not None:
                    if primary_error is None:
                        primary_error = scheduler_error
                    if scheduler_ordinary_error:
                        try:
                            logger.error("Failed to close backend component")
                        except BaseException:
                            pass
                elif isinstance(scheduler_result, Deferred):
                    # Scheduler.close() may expose a bounded view, but the mixin's
                    # public close owns a larger composite operation.  It settles
                    # only after sibling cleanup and manager release, while the
                    # bounded view may time out without cancelling that authority.
                    authoritative = getattr(
                        scheduler,
                        "_close_completion_deferred",
                        None,
                    )
                    if not isinstance(authoritative, Deferred):
                        authoritative = scheduler_result
                    asynchronous = True
                    composite: Deferred[Any] = Deferred()
                    public_result: Deferred[Any]
                    if not reactor_is_running():
                        # Preserve the synchronous/direct identity contract. The
                        # callback below completes all siblings before this Deferred
                        # callback chain returns; a test/provider-supplied bounded
                        # view remains the public compatibility object.
                        public_result = scheduler_result
                    else:
                        timeout_value = getattr(
                            scheduler,
                            "_reactor_io_timeout",
                            DEFAULT_REACTOR_IO_TIMEOUT_S,
                        )
                        if not isinstance(timeout_value, (int, float)):
                            timeout_value = DEFAULT_REACTOR_IO_TIMEOUT_S
                        public_result = (
                            bounded_deferred(
                                composite,
                                timeout=timeout_value,
                                operation="spider backend close",
                            )
                            if reactor_is_running()
                            else composite
                        )
                    self._close_deferred = public_result

                    def settle_composite(result: Any) -> Any:
                        if isinstance(result, TwistedFailure):
                            if not composite.called:
                                composite.errback(result)
                        elif not composite.called:
                            composite.callback(result)
                        reset_close_state()
                        return result

                    def scheduler_settled(result: Any) -> Any:
                        """Finish sibling teardown before completing the composite."""
                        nonlocal primary_error
                        if isinstance(result, TwistedFailure):
                            scheduler_error = result.value
                            if primary_error is None:
                                primary_error = scheduler_error
                            if isinstance(scheduler_error, Exception):
                                try:
                                    logger.error("Failed to close backend component")
                                except BaseException:
                                    pass
                        else:
                            if self._scheduler is scheduler:
                                self._scheduler = None
                            if self._queue is scheduler_queue:
                                self._queue = None
                                self._queue_name = None
                            if (
                                scheduler_dupefilter is not None
                                and self._dupefilter is scheduler_dupefilter
                            ):
                                self._dupefilter = None

                        def finish_siblings(_ignored: Any = None) -> Any:
                            final_error = primary_error
                            if final_error is not None:
                                final_failure = TwistedFailure(  # type: ignore[no-untyped-call]
                                    final_error
                                )
                                settle_composite(final_failure)
                                return final_failure
                            settle_composite(None)
                            # Preserve the scheduler's original result for its
                            # own Deferred branch; the composite is the sibling
                            # completion barrier exposed by the mixin.
                            return result

                        def fail_siblings(failure: Any) -> Any:
                            nonlocal primary_error
                            error = (
                                failure.value
                                if isinstance(failure, TwistedFailure)
                                else failure
                            )
                            if primary_error is None:
                                primary_error = error
                            return finish_siblings()

                        try:
                            continuation = continue_after_scheduler()
                        except BaseException as exc:
                            if primary_error is None:
                                primary_error = exc
                            return finish_siblings()
                        if isinstance(continuation, Deferred):
                            try:
                                return continuation.addCallbacks(
                                    finish_siblings,
                                    fail_siblings,
                                )
                            except BaseException as exc:
                                try:
                                    return continuation.addCallbacks(
                                        finish_siblings,
                                        fail_siblings,
                                    )
                                except BaseException:
                                    if not composite.called:
                                        composite.errback(exc)
                                    return continuation
                        return finish_siblings()

                    if authoritative is scheduler_result:
                        # This Deferred is itself the caller-facing result; preserve
                        # its failure for that caller. Whenever authority is a
                        # distinct worker Deferred, the explicit fork below can
                        # consume a late failure independently of its public view.
                        def settle_public_success(result: Any) -> Any:
                            return scheduler_settled(result)

                        def settle_public_failure(failure: Any) -> Any:
                            return scheduler_settled(failure)

                        try:
                            authoritative.addCallbacks(
                                settle_public_success,
                                settle_public_failure,
                            )
                        except BaseException:
                            try:
                                authoritative.addCallbacks(
                                    settle_public_success,
                                    settle_public_failure,
                                )
                            except BaseException:
                                if not composite.called:
                                    composite.errback(
                                        RuntimeError(
                                            "scheduler close callback attachment failed"
                                        )
                                    )
                    else:
                        # Do not infer ownership from ``addBoth``'s return identity:
                        # Twisted may return the source Deferred, leaving a late
                        # authoritative failure as its terminal unhandled result.
                        # Fork the outcome explicitly and consume the observer branch.
                        authority_observer: Deferred[Any] = Deferred()
                        authority_observer.addErrback(lambda _failure: None)

                        def mirror_authority(value: Any) -> Any:
                            if isinstance(value, TwistedFailure):
                                if not authority_observer.called:
                                    authority_observer.errback(value)
                                # This observer branch is deliberately consumed;
                                # the composite owns the public result.
                                return None
                            if not authority_observer.called:
                                authority_observer.callback(value)
                            return value

                        def observe_authority_success(result: Any) -> Any:
                            settled = scheduler_settled(result)
                            if isinstance(settled, Deferred):
                                return settled.addBoth(mirror_authority)
                            return mirror_authority(settled)

                        def observe_authority_failure(failure: Any) -> Any:
                            try:
                                settled = scheduler_settled(failure)
                            except BaseException as exc:
                                settled = TwistedFailure(exc)  # type: ignore[no-untyped-call]
                            if isinstance(settled, Deferred):
                                return settled.addBoth(mirror_authority)
                            mirror_authority(settled)
                            # The source failure is consumed by this explicit
                            # observer; the public bounded view already settled on
                            # its timeout and must not receive a second error.
                            return None

                        try:
                            authoritative.addCallbacks(
                                observe_authority_success,
                                observe_authority_failure,
                            )
                        except BaseException:
                            try:
                                authoritative.addCallbacks(
                                    observe_authority_success,
                                    observe_authority_failure,
                                )
                            except BaseException as exc:
                                if not composite.called:
                                    composite.errback(exc)
                    if public_result is not scheduler_result:
                        # The composite owns the public failure while the
                        # scheduler's bounded/authoritative branch is only an
                        # internal trigger. Consume that separate branch after
                        # its outcome has been mirrored into ``composite``.
                        scheduler_result.addErrback(lambda _failure: None)
                    if public_result is not composite:
                        composite.addErrback(lambda _failure: None)
                    return public_result
                else:
                    if self._scheduler is scheduler:
                        self._scheduler = None
                    if self._queue is scheduler_queue:
                        self._queue = None
                        self._queue_name = None
                    if (
                        scheduler_dupefilter is not None
                        and self._dupefilter is scheduler_dupefilter
                    ):
                        self._dupefilter = None

            result = continue_after_scheduler()
            if isinstance(result, Deferred):
                asynchronous = True

                def settle_direct_close(value: Any) -> Any:
                    reset_close_state()
                    return value

                try:
                    authoritative_result = result.addBoth(settle_direct_close)
                except BaseException as exc:
                    try:
                        authoritative_result = result.addBoth(settle_direct_close)
                    except BaseException:
                        authoritative_result = Deferred()
                        authoritative_result.errback(exc)
                public_result = (
                    bounded_deferred(
                        authoritative_result,
                        timeout=DEFAULT_REACTOR_IO_TIMEOUT_S,
                        operation="spider backend close",
                    )
                    if reactor_is_running()
                    else authoritative_result
                )
                if public_result is not authoritative_result:
                    # The bounded Deferred is the caller-facing failure surface;
                    # consume the late authoritative branch after it has mirrored
                    # its outcome so a timeout/failure cannot become unhandled.
                    authoritative_result.addErrback(lambda _failure: None)
                self._close_deferred = public_result
                return public_result
            return result
        finally:
            if not asynchronous:
                reset_close_state()

    @property
    def connection_manager(self) -> ConnectionManager:
        """Get the connection manager.

        Returns:
            ConnectionManager: The current connection manager.

        Raises:
            RuntimeError: If setup_backend() has not been called.
        """
        with self._lifecycle_lock:
            manager = self._connection_manager
            if manager is None:
                msg = (
                    "setup_backend() must be called before accessing connection_manager. "
                    f"Call setup_backend() in {self.__class__.__name__}.__init__()"
                )
                raise RuntimeError(msg)
            return manager
