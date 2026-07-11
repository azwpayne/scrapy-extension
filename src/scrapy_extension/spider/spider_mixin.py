"""Spider mixin for backend integration.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This module provides the BackendSpiderMixin class that adds backend functionality
to Scrapy spiders, enabling distributed crawling capabilities.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

from scrapy import Spider, signals

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
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

  def __init__(self, **kwargs: Any) -> None:
    """Initialize the mixin.

    Args:
        **kwargs: Keyword arguments passed to the spider.
    """
    super().__init__(**kwargs)
    self._connection_manager: ConnectionManager | None = None
    self._queue: BackendQueue | None = None
    self._dupefilter: BackendDupeFilter | None = None
    self._scheduler: BackendScheduler | None = None

  def setup_backend(self) -> ConnectionManager:
    """Initialize and return the connection manager.

    This method creates a ConnectionManager instance using the spider's
    backend configuration. It also connects Scrapy signals for automatic
    connection lifecycle management.

    Returns:
        ConnectionManager: The initialized connection manager.

    Raises:
        RuntimeError: If backend_type is not set.
        ImportError: If required backend dependencies are not installed.
    """
    if self.backend_type is None:
      msg = (
        f"{self.__class__.__name__}.backend_type must be set. "
        "Use BackendType.REDIS, BackendType.MONGODB, etc."
      )
      raise RuntimeError(msg)

    # Build settings dict from shortcut attributes
    settings = self._build_backend_settings()

    # Import here to avoid circular imports
    from scrapy_extension.backends.connectors import ConnectionManager

    # 2026-07-10 (DEEP-INSIGHT-2026-07-10 §C): acquire via the refcounted
    # singleton registry rather than constructing directly. Direct
    # construction bypasses the registry, defeating refcounting + LRU
    # eviction and the co-located-sharing model (two components that should
    # share one Redis would each build their own). ``close_backend()``'s
    # ``close()`` is the paired release — it decrements ``_users``; only the
    # last holder disconnects.
    self._connection_manager = ConnectionManager.get_manager(
      backend_type=self.backend_type,
      settings=settings,
    )

    # Connect Scrapy signals for lifecycle management
    self._connect_signals()

    return self._connection_manager

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
    if not self.backend_type:
      backend_value = None
    elif hasattr(self.backend_type, "value"):
      backend_value = self.backend_type.value
    else:
      backend_value = self.backend_type
    builder_name = self._BACKEND_SHORTCUT_BUILDERS.get(backend_value or "")
    if builder_name is not None:
      settings.update(getattr(self, builder_name)())

    return settings

  def _connect_signals(self) -> None:
    """Connect Scrapy signals for backend lifecycle management.

    Connects spider_opened signal to initialize backend connections
    and spider_closed signal to cleanup connections.
    """
    if hasattr(self, "crawler") and self.crawler:
      self.crawler.signals.connect(self._on_spider_opened, signals.spider_opened)
      self.crawler.signals.connect(self._on_spider_closed, signals.spider_closed)

  def _on_spider_opened(self, spider: Spider) -> None:
    """Handle spider_opened signal.

    Args:
        spider: The spider instance that was opened.
    """
    if spider is self and self._connection_manager is not None:
      self._connection_manager.connect()

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
    try:
      self.close_backend()
    except Exception:
      logger.exception("close_backend() failed during spider_closed signal")

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
    if self._connection_manager is None:
      msg = (
        "setup_backend() must be called before get_queue(). "
        f"Call setup_backend() in {self.__class__.__name__}.__init__()"
      )
      raise RuntimeError(msg)

    if self._queue is None:
      from scrapy_extension.queue.queue import BackendQueue

      name = queue_name or f"{self.name}:queue"
      self._queue = BackendQueue(
        connection_manager=self._connection_manager,
        queue_name=name,
        spider=self,
        queue_strategy=self._build_queue_strategy_from_settings(
          self._connection_manager
        ),
      )

    return self._queue

  def get_dupefilter(self) -> BackendDupeFilter:
    """Get the backend dupefilter for this spider.

    Returns:
        BackendDupeFilter: The backend dupefilter instance.

    Raises:
        RuntimeError: If setup_backend() has not been called.
    """
    if self._connection_manager is None:
      msg = (
        "setup_backend() must be called before get_dupefilter(). "
        f"Call setup_backend() in {self.__class__.__name__}.__init__()"
      )
      raise RuntimeError(msg)

    if self._dupefilter is None:
      from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter

      key = f"{self.name}:dupefilter"
      self._dupefilter = BackendDupeFilter(
        connection_manager=self._connection_manager,
        key=key,
        membership_filter=self._build_membership_filter_from_settings(
          self._connection_manager, key
        ),
      )

    return self._dupefilter

  def get_scheduler(self) -> BackendScheduler:
    """Get the backend scheduler for this spider.

    Returns:
        BackendScheduler: The backend scheduler instance.

    Raises:
        RuntimeError: If setup_backend() has not been called.
    """
    if self._connection_manager is None:
      msg = (
        "setup_backend() must be called before get_scheduler(). "
        f"Call setup_backend() in {self.__class__.__name__}.__init__()"
      )
      raise RuntimeError(msg)

    if self._scheduler is None:
      from scrapy_extension.schedule.scheduler import BackendScheduler

      self._scheduler = BackendScheduler(
        connection_manager=self._connection_manager,
        queue_key=f"{self.name}:queue",
      )

    return self._scheduler

  def close_backend(self) -> None:
    """Cleanup backend connections.

    This method should be called when the spider is closed to ensure
    all backend connections are properly released. It is automatically
    called when the spider_closed signal is received.
    """
    # Close component references
    self._queue = None
    self._dupefilter = None
    self._scheduler = None

    # Close connection manager
    if self._connection_manager is not None:
      self._connection_manager.close()
      self._connection_manager = None

  @property
  def connection_manager(self) -> ConnectionManager:
    """Get the connection manager.

    Returns:
        ConnectionManager: The current connection manager.

    Raises:
        RuntimeError: If setup_backend() has not been called.
    """
    if self._connection_manager is None:
      msg = (
        "setup_backend() must be called before accessing connection_manager. "
        f"Call setup_backend() in {self.__class__.__name__}.__init__()"
      )
      raise RuntimeError(msg)
    return self._connection_manager
