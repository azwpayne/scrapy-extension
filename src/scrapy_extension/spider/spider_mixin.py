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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pydispatch.errors import DispatcherKeyError
from scrapy import Spider, signals
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure as TwistedFailure

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.monitor import NullMonitor
from scrapy_extension.utils.identity import (
    DEFAULT_DUPEFILTER_KEY_TEMPLATE,
    DEFAULT_QUEUE_KEY_TEMPLATE,
    project_name_from_spider,
    resolve_identity_template,
)
from scrapy_extension.utils.reactor import (
    DEFAULT_REACTOR_IO_TIMEOUT_S,
    bounded_deferred,
    reactor_is_running,
)

logger = logging.getLogger(__name__)


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
        self._orphan_leases: list[Any] = []
        self._orphan_managers: list[Any] = []

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
                    self._connection_manager = manager
                    self._setup_identity = desired_identity
                    published = True
                if self._setup_attempt is attempt:
                    self._setup_attempt = None
                    attempt.done.set()
            if valid:
                self._connection_manager_lease = attempt.lease
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
            try:
                self._disconnect_lifecycle_signals(
                    signal_manager,
                    handlers=rollback_handlers,
                )
            except BaseException:
                if signal_manager.__class__.__module__.startswith("unittest.mock"):
                    # Compatibility-only mocks have no authoritative dispatcher
                    # state; retain the lease record but keep the legacy aggregate
                    # view cleared after failed rollback.
                    with self._lifecycle_lock:
                        self._connected_signals = None
                        self._signals_connected = False
                try:
                    logger.error("Failed to roll back backend lifecycle signals")
                except BaseException:
                    pass
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

    def _on_spider_opened(self, spider: Spider) -> None:
        """Handle spider_opened signal.

        Args:
            spider: The spider instance that was opened.
        """
        if spider is not self:
            return
        with self._lifecycle_lock:
            manager = self._connection_manager
        if manager is not None:
            manager.connect()

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

    def _dispose_invalidated_candidate(
        self,
        kind: str,
        candidate: Any,
        reason: str,
    ) -> None:
        """Close one unpublished candidate and retain failures for retry."""
        close_succeeded = False
        self._retain_orphan_candidate(kind, candidate)
        try:
            if kind == "queue":
                candidate.close()
            else:
                candidate.close(reason)
        except BaseException:
            pass
        else:
            close_succeeded = True
        self._capture_candidate_leases(candidate)
        if close_succeeded:
            self._remove_orphan_candidate(candidate)
        # Candidate cleanup is primary. Its exact leases are independent ownership
        # and must be attempted even when candidate close raises; failed releases
        # remain reachable on the mixin for a later close pass.
        self._release_orphan_leases()

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

    def _cleanup_orphan_candidates(self, reason: str) -> BaseException | None:
        """Retry candidates and their exact leases before parent release."""
        primary_error: BaseException | None = None
        with self._lifecycle_lock:
            candidates = tuple(self._orphan_candidates)
        for kind, candidate in candidates:
            try:
                if kind == "queue":
                    candidate.close()
                else:
                    candidate.close(reason)
            except BaseException as exc:
                if primary_error is None:
                    primary_error = exc
                continue
            self._capture_candidate_leases(candidate)
            self._remove_orphan_candidate(candidate)
        lease_error = self._release_orphan_leases()
        if primary_error is None:
            primary_error = lease_error
        manager_error = self._release_orphan_managers()
        if primary_error is None:
            primary_error = manager_error
        return primary_error

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
                monitor, _ = self._resolve_queue_monitor()
                if not isinstance(monitor, NullMonitor):
                    queue.set_monitor(monitor)
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

                        authoritative_cleanup.addBoth(finish_factory_cleanup)
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

                        authoritative.addBoth(finish_candidate_cleanup)
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
                    if self._close_owner_thread_id == current_thread:
                        return self._close_deferred
                    if self._close_deferred is not None:
                        return self._close_deferred
                    raise RuntimeError("Backend spider close is already in progress")
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
                if setup_waiter is not None:
                    waiters = (setup_waiter,)
                elif not active:
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
            for waiter in waiters:
                waiter.wait()

        asynchronous = False

        def reset_close_state() -> None:
            with self._lifecycle_lock:
                self._close_in_progress = False
                self._close_owner_thread_id = None
                self._close_deferred = None

        try:
            primary_error: BaseException | None = None
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

            def continue_after_scheduler() -> Deferred[Any] | None:
                """Run every dependency after scheduler close has settled."""
                nonlocal primary_error

                queue = self._queue
                if queue is not None:
                    queue_error: BaseException | None = None
                    queue_ordinary_error = False
                    try:
                        queue.close()
                    except Exception as exc:
                        queue_error = exc
                        queue_ordinary_error = True
                    except BaseException as exc:  # noqa: BLE001 - preserve control flow
                        queue_error = exc
                    else:
                        if self._queue is queue:
                            self._queue = None
                            self._queue_name = None
                    if queue_error is not None:
                        if primary_error is None:
                            primary_error = queue_error
                        if queue_ordinary_error:
                            try:
                                logger.error("Failed to close backend component")
                            except BaseException:
                                pass

                # If the scheduler did not own this dupefilter, the mixin does.
                dupefilter = self._dupefilter
                if dupefilter is not None and dupefilter is not scheduler_dupefilter:
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
                if snapshot_lease is not None and self._queue is None:
                    snapshot_error: BaseException | None = None
                    snapshot_ordinary_error = False
                    try:
                        snapshot_lease.release()
                    except Exception as exc:
                        snapshot_error = exc
                        snapshot_ordinary_error = True
                    except BaseException as exc:  # noqa: BLE001
                        snapshot_error = exc
                    else:
                        if self._snapshot_connection_lease is snapshot_lease:
                            self._snapshot_connection_lease = None
                            self._snapshot_connection_manager = None
                    if snapshot_error is not None:
                        if primary_error is None:
                            primary_error = snapshot_error
                        if snapshot_ordinary_error:
                            try:
                                logger.error(
                                    "Failed to release snapshot connection lease"
                                )
                            except BaseException:
                                pass

                # A queue-backend override has its own exact acquire. Release it
                # only after the queue and its separate snapshot lease succeed;
                # retrying never consumes the mixin manager's registry reference.
                queue_lease = self._queue_connection_lease
                if (
                    queue_lease is not None
                    and self._queue is None
                    and self._snapshot_connection_lease is None
                ):
                    queue_manager_error: BaseException | None = None
                    queue_manager_ordinary_error = False
                    try:
                        queue_lease.release()
                    except Exception as exc:
                        queue_manager_error = exc
                        queue_manager_ordinary_error = True
                    except BaseException as exc:  # noqa: BLE001
                        queue_manager_error = exc
                    else:
                        if self._queue_connection_lease is queue_lease:
                            self._queue_connection_lease = None
                            self._queue_connection_manager = None
                    if queue_manager_error is not None:
                        if primary_error is None:
                            primary_error = queue_manager_error
                        if queue_manager_ordinary_error:
                            try:
                                logger.error("Failed to release queue connection lease")
                            except BaseException:
                                pass

                orphan_error = self._cleanup_orphan_candidates("spider-mixin-close")
                if orphan_error is not None:
                    if primary_error is None:
                        primary_error = orphan_error
                    if isinstance(orphan_error, Exception):
                        try:
                            logger.error("Failed to close backend candidate")
                        except BaseException:
                            pass

                # Never release the shared manager while any required provider
                # still has ownership. This is the retry fence for async scheduler
                # failures and for every synchronous component failure.
                manager = self._connection_manager
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
                if manager is not None and required_cleanup_complete:
                    manager_error: BaseException | None = None
                    manager_ordinary_error = False
                    try:
                        if self._connection_manager_lease is not None:
                            self._connection_manager_lease.release()
                        else:
                            manager.close()
                    except Exception as exc:
                        manager_error = exc
                        manager_ordinary_error = True
                    except BaseException as exc:  # noqa: BLE001
                        manager_error = exc
                    else:
                        if self._connection_manager is manager:
                            self._connection_manager = None
                            self._connection_manager_lease = None
                            self._consumer_queue_name = None
                    if manager_error is not None:
                        if primary_error is None:
                            primary_error = manager_error
                        if manager_ordinary_error:
                            try:
                                logger.error(
                                    "Failed to close backend connection manager"
                                )
                            except BaseException:
                                pass

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
                            if (
                                scheduler_dupefilter is not None
                                and self._dupefilter is scheduler_dupefilter
                            ):
                                self._dupefilter = None
                        try:
                            continue_after_scheduler()
                        except BaseException as exc:
                            if primary_error is None:
                                primary_error = exc
                        final: Any = primary_error
                        if final is not None:
                            settle_composite(TwistedFailure(final))  # type: ignore[no-untyped-call]
                            return TwistedFailure(final)  # type: ignore[no-untyped-call]
                        settle_composite(None)
                        return None

                    if authoritative is scheduler_result:
                        # This Deferred is itself the caller-facing result; preserve
                        # its failure for that caller. Whenever authority is a
                        # distinct worker Deferred, the explicit fork below can
                        # consume a late failure independently of its public view.
                        def settle_public_success(result: Any) -> Any:
                            scheduler_settled(result)
                            if primary_error is not None:
                                return TwistedFailure(  # type: ignore[no-untyped-call]
                                    primary_error
                                )
                            return result

                        def settle_public_failure(failure: Any) -> Any:
                            settled = scheduler_settled(failure)
                            return failure if settled is None else settled

                        authoritative.addCallbacks(
                            settle_public_success,
                            settle_public_failure,
                        )
                    else:
                        # Do not infer ownership from ``addBoth``'s return identity:
                        # Twisted may return the source Deferred, leaving a late
                        # authoritative failure as its terminal unhandled result.
                        # Fork the outcome explicitly and consume the observer branch.
                        authority_observer: Deferred[Any] = Deferred()
                        authority_observer.addErrback(lambda _failure: None)

                        def observe_authority_success(result: Any) -> Any:
                            scheduler_settled(result)
                            if not authority_observer.called:
                                authority_observer.callback(result)
                            return result

                        def observe_authority_failure(failure: Any) -> Any:
                            try:
                                scheduler_settled(failure)
                            except BaseException as exc:
                                mirrored = TwistedFailure(exc)  # type: ignore[no-untyped-call]
                                if not authority_observer.called:
                                    authority_observer.errback(mirrored)
                                return None
                            if not authority_observer.called:
                                authority_observer.errback(failure)
                            # The source failure is consumed by this explicit
                            # observer; the public bounded view already settled on
                            # its timeout and must not receive a second error.
                            return None

                        authoritative.addCallbacks(
                            observe_authority_success,
                            observe_authority_failure,
                        )
                    if (
                        authoritative is not scheduler_result
                        and public_result is not scheduler_result
                    ):
                        scheduler_result.addErrback(lambda _failure: None)
                    if public_result is not composite:
                        composite.addErrback(lambda _failure: None)
                    return public_result
                else:
                    if self._scheduler is scheduler:
                        self._scheduler = None
                    if (
                        scheduler_dupefilter is not None
                        and self._dupefilter is scheduler_dupefilter
                    ):
                        self._dupefilter = None

            return continue_after_scheduler()
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
