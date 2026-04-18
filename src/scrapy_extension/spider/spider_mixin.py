"""Spider mixin for backend integration.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This module provides the BackendSpiderMixin class that adds backend functionality
to Scrapy spiders, enabling distributed crawling capabilities.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from scrapy import Spider, signals

if TYPE_CHECKING:
  from scrapy_extension.backends.base import BackendType
  from scrapy_extension.backends.connectors import ConnectionManager
  from scrapy_extension.components.dupefilter import BackendDupeFilter
  from scrapy_extension.components.queue import BackendQueue
  from scrapy_extension.components.scheduler import BackendScheduler


class BackendSpiderMixin:
  name: str  # provided by Spider when used as mixin
  """Mixin class for spiders to integrate with backend components.

  This mixin provides convenient access to backend functionality including
  queues, dupefilters, and schedulers. It handles connection lifecycle
  management through Scrapy signals.

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
      class MySpider(BackendSpiderMixin, Spider):
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

    self._connection_manager = ConnectionManager(
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

  def _build_backend_settings(self) -> dict[str, Any]:
    """Build backend settings from shortcut attributes.

    Returns:
        Dictionary of backend settings merged from class attributes.
    """
    settings: dict[str, Any] = {}

    # Start with explicit backend_settings if provided
    if self.backend_settings:
      settings.update(self.backend_settings)

    # Add shortcut settings based on backend type
    backend_value = self.backend_type.value if self.backend_type else None
    if backend_value == "redis":
      settings.update(self._build_redis_settings())
    elif backend_value == "mongodb":
      if self.mongodb_uri is not None:
        settings["uri"] = self.mongodb_uri
      if self.mongodb_db is not None:
        settings["database"] = self.mongodb_db
    elif backend_value == "kafka":
      if self.kafka_bootstrap_servers is not None:
        settings["bootstrap_servers"] = self.kafka_bootstrap_servers
    elif backend_value == "rabbitmq" and self.rabbitmq_url is not None:
      settings["url"] = self.rabbitmq_url

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

  def _on_spider_closed(self, spider: Spider, reason: str = "") -> None:  # noqa: ARG002
    """Handle spider_closed signal.

    Args:
        spider: The spider instance that was closed.
        reason: The reason for closing the spider (unused, provided by Scrapy).
    """
    if spider is self:
      self.close_backend()

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
      from scrapy_extension.components.queue import BackendQueue

      name = queue_name or f"{self.name}:queue"
      self._queue = BackendQueue(
        connection_manager=self._connection_manager,
        queue_name=name,
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
      from scrapy_extension.components.dupefilter import BackendDupeFilter

      self._dupefilter = BackendDupeFilter(
        connection_manager=self._connection_manager,
        key=f"{self.name}:dupefilter",
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
      from scrapy_extension.components.scheduler import BackendScheduler

      self._scheduler = BackendScheduler(
        connection_manager=self._connection_manager,
        queue_key=f"{self.name}:queue",
        dupefilter_key=f"{self.name}:dupefilter",
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
