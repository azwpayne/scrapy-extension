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
from typing import TYPE_CHECKING, Any, ClassVar

from scrapy import Spider, signals

from scrapy_extension.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from typing_extensions import Self

    from scrapy_extension.backends.base import BackendType
    from scrapy_extension.backends.connectors import ConnectionManager
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
    backend_type: BackendType | None = None
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
        self._queue: BackendQueue | None = None
        self._queue_name: str | None = None
        self._snapshot_connection_manager: ConnectionManager | None = None
        self._dupefilter: BackendDupeFilter | None = None
        self._scheduler: BackendScheduler | None = None
        self._consumer_manager_scope = uuid.uuid4().hex
        self._consumer_queue_name: str | None = None
        self._signals_connected = False
        self._connected_signals: Any | None = None
        # Component close hooks are user-extensible and may call close_backend()
        # again. An RLock lets that re-entrant call observe the already-detached
        # state and return instead of deadlocking the outer shutdown.
        self._lifecycle_lock = threading.RLock()

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
        """Initialize and return the connection manager.

        This method creates a ConnectionManager instance using the spider's
        backend configuration. It also connects Scrapy signals for automatic
        connection lifecycle management. Repeated calls are idempotent: the
        existing manager is returned without acquiring another registry reference
        or registering duplicate signal handlers.

        Returns:
            ConnectionManager: The initialized connection manager.

        Raises:
            RuntimeError: If backend_type is not set.
            ImportError: If required backend dependencies are not installed.
        """
        with self._lifecycle_lock:
            manager = self._connection_manager
            acquired_here = False
            if manager is None:
                if self.backend_type is None:
                    msg = (
                        f"{self.__class__.__name__}.backend_type must be set. "
                        "Use BackendType.REDIS, BackendType.MONGODB, etc."
                    )
                    raise RuntimeError(msg)

                settings = self._build_backend_settings()
                from scrapy_extension.backends.connectors import (
                    _CONNECTION_MANAGER_SCOPE_KEY,
                    _CONSUMER_SCOPED_BACKENDS,
                    ConnectionManager,
                    resolve_circuit_breaker_policy,
                )

                # R135-B: fold Scrapy-level breaker settings into the manager
                # settings for parity with the component-factory path
                # (resolve_backend_config -> _merge_connection_manager_settings),
                # so a breaker configured in Scrapy settings applies to every
                # backend object the mixin hands out. An empty dict (no source
                # anywhere) is a no-op, leaving the lazy env fallback intact.
                crawler = getattr(self, "crawler", None)
                if crawler is not None:
                    settings.update(resolve_circuit_breaker_policy(crawler.settings))

                if self._backend_type_name() in _CONSUMER_SCOPED_BACKENDS:
                    settings = {
                        **settings,
                        _CONNECTION_MANAGER_SCOPE_KEY: (
                            f"spider-mixin-{self._consumer_manager_scope}"
                        ),
                    }

                manager = ConnectionManager.get_manager(
                    backend_type=self.backend_type,
                    settings=settings,
                )
                self._connection_manager = manager
                acquired_here = True

            # R14-D: thread default-on telemetry into the shared manager so the
            # connection-lifecycle hooks (on_connect/on_disconnect/on_disconnect_
            # result/on_retry -> backend/{connect,disconnect,retry}_count) fire
            # in production. Parity with pipeline/dupefilter/scheduler from_crawler,
            # all of which call connection_manager.set_monitor(...); spider_mixin
            # was the lone exception, leaving the hooks dead for get_queue/
            # get_dupefilter-direct spiders. Resolved on every setup_backend call
            # so the legacy early-setup path (setup_backend in __init__ before
            # crawler is attached -> resolves NullMonitor) is re-covered when
            # from_crawler's idempotent second call runs with the crawler attached.
            # A later scheduler.open() overwrites this with the same
            # crawler.stats-backed monitor, so there is no conflict.
            from scrapy_extension.queue.queue import BackendQueue

            manager.set_monitor(BackendQueue._resolve_monitor(self))

            signal_wiring_failure: BaseException | None = None
            try:
                self._connect_signals()
            except BaseException as exc:
                signal_wiring_failure = exc
            if signal_wiring_failure is not None:
                cleanup_failed = False
                # Release the manager when this call orphaned it: either we acquired it
                # here (acquired_here), or _connect_signals detached the old crawler's
                # spider_closed handler and then failed to wire the new one (leaving
                # _connected_signals None) -- in the latter case spider_closed can no
                # longer fire close_backend, so the manager must be released here or it
                # leaks (registry refcount never decremented) for the life of the process.
                if acquired_here or self._connected_signals is None:
                    self._connection_manager = None
                    try:
                        manager.close()
                    except BaseException:
                        cleanup_failed = True
                if cleanup_failed:
                    # Signal registration is the primary operation. An arbitrary
                    # logging handler must not replace that registration failure.
                    try:
                        logger.error(
                            "Failed to release ConnectionManager after signal wiring failure"
                        )
                    except BaseException:
                        pass
                raise signal_wiring_failure

            return manager

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
        from scrapy_extension.backends.connectors import _CONSUMER_SCOPED_BACKENDS

        if self._backend_type_name() not in _CONSUMER_SCOPED_BACKENDS:
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

        Connects spider_opened signal to initialize backend connections
        and spider_closed signal to cleanup connections.
        """
        crawler = getattr(self, "crawler", None)
        if not crawler:
            return

        signal_manager = crawler.signals
        if self._signals_connected:
            if self._connected_signals is signal_manager:
                return
            # A programmatic spider can be moved to a replacement crawler between
            # setup calls. Detach the old dispatcher before wiring the new one so a
            # stale spider_closed event cannot close the current manager generation.
            previous_signal_manager = self._connected_signals
            self._connected_signals = None
            self._signals_connected = False
            if previous_signal_manager is not None:
                self._disconnect_lifecycle_signals(previous_signal_manager)

        handlers = (
            (self._on_spider_opened, signals.spider_opened),
            (self._on_spider_closed, signals.spider_closed),
        )
        connected: list[tuple[Any, Any]] = []
        registration_failure: BaseException | None = None
        try:
            for handler, signal in handlers:
                signal_manager.connect(handler, signal)
                connected.append((handler, signal))
        except BaseException as exc:
            # The registration failure is the operation's primary error.  A control
            # signal raised while undoing the successfully registered handlers must
            # not replace it, but every registered handler still needs an attempt.
            registration_failure = exc
        if registration_failure is not None:
            # R61: run the rollback OUTSIDE the except so sys.exc_info() is clear
            # during _disconnect_lifecycle_signals and logger.error attaches no
            # exc_info (the 6b28166 invariant — mirrors scheduler.open /
            # BackendDupeFilter.open).
            cleanup_failed = False
            try:
                self._disconnect_lifecycle_signals(
                    signal_manager,
                    handlers=tuple(reversed(connected)),
                )
            except BaseException:  # noqa: BLE001 - preserve registration failure
                cleanup_failed = True
            if cleanup_failed:
                try:
                    logger.error("Failed to roll back backend lifecycle signals")
                except BaseException:
                    pass
            raise registration_failure
        self._connected_signals = signal_manager
        self._signals_connected = True

    def _disconnect_lifecycle_signals(
        self,
        signal_manager: Any,
        *,
        handlers: tuple[tuple[Any, Any], ...] | None = None,
    ) -> None:
        """Best-effort disconnect of mixin lifecycle handlers."""
        targets = (
            handlers
            if handlers is not None
            else (
                (self._on_spider_opened, signals.spider_opened),
                (self._on_spider_closed, signals.spider_closed),
            )
        )
        primary_error: BaseException | None = None
        for handler, signal in targets:
            disconnect_failed = False
            try:
                signal_manager.disconnect(handler, signal)
            except Exception:
                disconnect_failed = True
            except BaseException as exc:  # noqa: BLE001 - finish sibling cleanup
                # This is the primary lifecycle-control failure.  Keep cleaning up
                # sibling handlers, then re-raise it below without invoking telemetry
                # while its exception context is active.
                if primary_error is None:
                    primary_error = exc
            if disconnect_failed:
                # The ordinary signal-manager error has left its ``except`` suite.
                # A custom logging handler cannot recover it through ``sys.exc_info``.
                try:
                    logger.error("Failed to disconnect backend lifecycle signal")
                except BaseException:
                    pass
        if primary_error is not None:
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

    def _on_spider_closed(self, spider: Spider, reason: str = "") -> None:
        """Handle spider_closed signal.

        Wrapped in try/except so a failure in ``close_backend`` doesn't break
        Scrapy's signal chain — other spider_closed handlers (stats, logging,
        extensions) still need to fire.

        Args:
            spider: The spider instance that was closed.
            reason: The reason for closing the spider (unused, provided by Scrapy).
        """
        if spider is not self:
            return
        close_failed = False
        try:
            self.close_backend()
        except Exception:
            close_failed = True
        if close_failed:
            # This is an advisory diagnostic: a broken logging handler must not
            # interrupt Scrapy's remaining spider_closed subscribers.  The caught
            # close failure has unwound before logger code runs, so the handler
            # cannot inspect it through ``sys.exc_info()``. Direct control-flow
            # exceptions from close_backend still propagate.
            try:
                logger.error("close_backend() failed during spider_closed signal")
            except BaseException:
                pass

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

    def _build_queue_strategy_from_settings(
        self, connection_manager: ConnectionManager
    ) -> Any | None:
        """Honor ``SCRAPY_QUEUE_STRATEGY`` from crawler.settings.

        Returns ``None`` (→ BackendQueue defaults to PassthroughQueueStrategy)
        when there is no crawler, no setting, or the setting is ``passthrough``.
        Per-strategy knobs use factory defaults; operators needing fine-tuning
        should use the settings-driven SCHEDULER path (``from_crawler``).
        """
        settings = self._crawler_settings()
        if settings is None:
            return None
        raw = settings.get("SCRAPY_QUEUE_STRATEGY")
        if not raw or str(raw) == "passthrough":
            return None
        from scrapy_extension.queue.strategies.factory import (
            QueueStrategyType,
            build_queue_strategy,
        )

        return build_queue_strategy(QueueStrategyType(str(raw)), connection_manager)

    def _resolve_snapshot_connection_manager(
        self, queue_strategy: Any
    ) -> ConnectionManager | None:
        """Acquire a storage manager for a stateful strategy on a queue-only backend.

        Mirrors the scheduler factory pairing (``BackendScheduler.from_settings``):
        delay/round_robin/time_wheel/ring_buffer strategies hold in-process state
        that must survive shutdown; when the queue backend itself has no storage
        capability, the configured storage component provides the snapshot
        backend. No explicit storage override → best-effort skip (the legacy
        queue-only behavior); an explicit but invalid override stays fail-fast.
        """
        from scrapy_extension.backends.registry import has_capability
        from scrapy_extension.queue.strategies import (
            DelayQueueStrategy,
            RingBufferQueueStrategy,
            RoundRobinQueueStrategy,
            TimeWheelQueueStrategy,
        )

        stateful_strategy_types = (
            DelayQueueStrategy,
            RoundRobinQueueStrategy,
            TimeWheelQueueStrategy,
            RingBufferQueueStrategy,
        )
        if not isinstance(queue_strategy, stateful_strategy_types):
            return None
        if has_capability(self._backend_type_name() or "", "storage"):
            return None
        settings = self._crawler_settings()
        if settings is None:
            return None
        storage_type_override = settings.get("SCRAPY_STORAGE_BACKEND_TYPE")
        has_explicit_storage_type = storage_type_override not in (None, "") or bool(
            os.environ.get("SCRAPY_STORAGE_BACKEND_TYPE")
        )
        from scrapy_extension.backends.connectors import (
            ConnectionManager,
            resolve_backend_config,
        )

        try:
            snapshot_backend_type, snapshot_backend_settings = resolve_backend_config(
                settings,
                type_key="SCRAPY_STORAGE_BACKEND_TYPE",
                settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
                required_capabilities={"storage"},
                component_name="storage",
            )
        except ConfigurationError:
            if has_explicit_storage_type:
                raise
            # A queue-only global backend has no storage component configured.
            # Preserve its best-effort no-snapshot behavior; an explicit
            # invalid storage override remains fail-fast.
            return None
        return ConnectionManager.get_manager(
            backend_type=snapshot_backend_type,
            settings=snapshot_backend_settings,
        )

    def _build_membership_filter_from_settings(
        self, connection_manager: ConnectionManager, key: str
    ) -> Any | None:
        """Honor ``SCRAPY_DEDUP_STRATEGY`` from crawler.settings.

        Returns ``None`` (→ BackendDupeFilter defaults to SetMembershipFilter)
        when there is no crawler, no setting, or the setting is ``set``.
        """
        settings = self._crawler_settings()
        if settings is None:
            return None
        raw = settings.get("SCRAPY_DEDUP_STRATEGY")
        if not raw or str(raw) == "set":
            return None
        from scrapy_extension.dupefilter.filters.factory import (
            DedupeStrategy,
            build_membership_filter,
        )

        return build_membership_filter(
            DedupeStrategy(str(raw)), connection_manager, key=key
        )

    def get_queue(self, queue_name: str | None = None) -> BackendQueue:
        """Get the backend queue for this spider.

        Args:
            queue_name: Optional name for the queue. If not provided,
                defaults to "{spider_name}:queue".

        Returns:
            BackendQueue: The backend queue instance.

        Raises:
            RuntimeError: If setup_backend() has not been called.
        """
        with self._lifecycle_lock:
            manager = self._connection_manager
            if manager is None:
                msg = (
                    "setup_backend() must be called before get_queue(). "
                    f"Call setup_backend() in {self.__class__.__name__}.__init__()"
                )
                raise RuntimeError(msg)

            from scrapy_extension.queue.queue import BackendQueue

            name = queue_name or f"{self.name}:queue"
            previous_claim = self._consumer_queue_name
            self._claim_consumer_queue(name)
            snapshot_manager: ConnectionManager | None = None
            try:
                if self._queue is None:
                    queue_strategy = self._build_queue_strategy_from_settings(manager)
                    snapshot_manager = self._resolve_snapshot_connection_manager(
                        queue_strategy
                    )
                    self._queue = BackendQueue(
                        connection_manager=manager,
                        queue_name=name,
                        spider=self,
                        queue_strategy=queue_strategy,
                        snapshot_connection_manager=snapshot_manager,
                    )
                    self._queue_name = name
                    self._snapshot_connection_manager = snapshot_manager
                elif self._queue_name != name:
                    # R60: a non-consumer backend (Redis/MongoDB/ES/RabbitMQ/
                    # Pulsar/SQS) skips _claim_consumer_queue's name check, so
                    # without this guard a second get_queue(different_name)
                    # silently returns the stale cached queue (data misrouting).
                    # Track the name separately — self._queue may be a Mock in
                    # tests, so reading self._queue.queue_name would mis-compare.
                    raise ConfigurationError(
                        f"{self.__class__.__name__} is already bound to queue "
                        f"{self._queue_name!r}; cannot rebind to {name!r}.",
                        setting_name="queue_name",
                        setting_value=name,
                    )
            except BaseException:
                self._consumer_queue_name = previous_claim
                if snapshot_manager is not None:
                    # The BackendQueue was not constructed with this acquire;
                    # release it so the failed get_queue cannot leak it.
                    snapshot_release_failed = False
                    try:
                        snapshot_manager.close()
                    except BaseException:
                        snapshot_release_failed = True
                    if snapshot_release_failed:
                        try:
                            logger.error(
                                "Failed to release snapshot ConnectionManager "
                                "after queue construction failure"
                            )
                        except BaseException:
                            pass
                raise

            return self._queue

    def get_dupefilter(self) -> BackendDupeFilter:
        """Get the backend dupefilter for this spider.

        Returns:
            BackendDupeFilter: The backend dupefilter instance.

        Raises:
            RuntimeError: If setup_backend() has not been called.
        """
        with self._lifecycle_lock:
            manager = self._connection_manager
            if manager is None:
                msg = (
                    "setup_backend() must be called before get_dupefilter(). "
                    f"Call setup_backend() in {self.__class__.__name__}.__init__()"
                )
                raise RuntimeError(msg)

            if self._dupefilter is None:
                from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
                from scrapy_extension.queue.queue import BackendQueue

                key = f"{self.name}:dupefilter"
                self._dupefilter = BackendDupeFilter(
                    connection_manager=manager,
                    key=key,
                    fingerprinter=getattr(
                        getattr(self, "crawler", None),
                        "request_fingerprinter",
                        None,
                    ),
                    membership_filter=self._build_membership_filter_from_settings(
                        manager, key
                    ),
                    monitor=BackendQueue._resolve_monitor(self),
                    owns_connection_manager=False,
                )

            return self._dupefilter

    def get_scheduler(self) -> BackendScheduler:
        """Get the backend scheduler for this spider.

        Returns:
            BackendScheduler: The backend scheduler instance.

        Raises:
            RuntimeError: If setup_backend() has not been called.
        """
        with self._lifecycle_lock:
            manager = self._connection_manager
            if manager is None:
                msg = (
                    "setup_backend() must be called before get_scheduler(). "
                    f"Call setup_backend() in {self.__class__.__name__}.__init__()"
                )
                raise RuntimeError(msg)

            if self._scheduler is None:
                from scrapy_extension.schedule.scheduler import BackendScheduler

                queue_name = f"{self.name}:queue"
                previous_claim = self._consumer_queue_name
                self._claim_consumer_queue(queue_name)
                try:
                    self._scheduler = BackendScheduler(
                        connection_manager=manager,
                        queue_key=queue_name,
                        queue_strategy=self._build_queue_strategy_from_settings(
                            manager
                        ),
                        owns_connection_manager=False,
                    )
                except BaseException:
                    self._consumer_queue_name = previous_claim
                    raise

            return self._scheduler

    def close_backend(self) -> None:
        """Cleanup backend connections.

        This method should be called when the spider is closed to ensure
        all backend connections are properly released. It is automatically
        called when the spider_closed signal is received.
        """
        with self._lifecycle_lock:
            signal_manager = self._connected_signals
            queue = self._queue
            dupefilter = self._dupefilter
            scheduler = self._scheduler
            manager = self._connection_manager
            snapshot_manager = self._snapshot_connection_manager

            # Clear shared state before invoking user-extensible close hooks so
            # duplicate/re-entrant shutdown cannot release the manager twice.
            self._connected_signals = None
            self._signals_connected = False
            self._queue = None
            self._dupefilter = None
            self._scheduler = None
            self._connection_manager = None
            self._snapshot_connection_manager = None
            self._consumer_queue_name = None

            primary_error: BaseException | None = None
            if signal_manager is not None:
                try:
                    self._disconnect_lifecycle_signals(signal_manager)
                except BaseException as exc:  # noqa: BLE001 - manager release is mandatory
                    primary_error = exc

            # Components borrow the mixin's single manager acquire, so each must
            # quiesce its own resources without releasing the manager. The final
            # manager close remains last, while strategies can still persist state.
            for component, args, _label in (
                (scheduler, ("spider-mixin-close",), "scheduler"),
                (queue, (), "queue"),
                (dupefilter, ("spider-mixin-close",), "dupefilter"),
            ):
                if component is None:
                    continue
                component_close_failed = False
                try:
                    component.close(*args)
                except Exception:
                    component_close_failed = True
                except BaseException as exc:  # noqa: BLE001 - preserve process control
                    if primary_error is None:
                        primary_error = exc
                if component_close_failed:
                    # The component's ordinary close error is no longer active, so a
                    # custom logger cannot observe it through ``sys.exc_info()``.
                    try:
                        logger.error("Failed to close backend component")
                    except BaseException:  # noqa: BLE001 - teardown must continue
                        pass

            # R135-C: the snapshot manager must outlive the queue close (the
            # strategy persists its final checkpoint there) and precede the
            # mixin's own manager release — the scheduler's close ordering.
            if snapshot_manager is not None:
                snapshot_close_failed = False
                try:
                    snapshot_manager.close()
                except Exception:
                    snapshot_close_failed = True
                except BaseException as exc:  # noqa: BLE001 - do not mask component error
                    if primary_error is None:
                        primary_error = exc
                if snapshot_close_failed:
                    try:
                        logger.error("Failed to close snapshot connection manager")
                    except BaseException:  # noqa: BLE001 - teardown must continue
                        pass

            if manager is not None:
                manager_close_failed = False
                try:
                    manager.close()
                except Exception:
                    manager_close_failed = True
                except BaseException as exc:  # noqa: BLE001 - do not mask component error
                    if primary_error is None:
                        primary_error = exc
                if manager_close_failed:
                    # Keep the manager release independent from diagnostic handlers for
                    # the same reason as component cleanup above. Its ordinary error has
                    # already left the handler before this log call.
                    try:
                        logger.error("Failed to close backend connection manager")
                    except BaseException:  # noqa: BLE001 - teardown must continue
                        pass
            if primary_error is not None:
                raise primary_error

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
