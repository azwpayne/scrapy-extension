"""Connection manager for backend connections.

This module provides a lazy singleton connection manager with retry logic
for all backend types.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from scrapy_extension.backends.base import (
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.exceptions import BackendConnectionError

if TYPE_CHECKING:
  from scrapy_extension.backends.base import Backend

logger = logging.getLogger(__name__)


class ConnectionManager:
  """Lazy singleton connection manager for backends.

  This class manages backend connections with:
  - Lazy initialization (connects on first use)
  - Thread-safe singleton pattern
  - Automatic retry with exponential backoff
  - Connection pooling

  Attributes:
      backend_type: The type of backend to manage.
      settings: Backend-specific settings.
      _backend: The backend instance (None until connected).
      _lock: Threading lock for thread safety.
  """

  # Class-level registry of managers
  _managers: dict[str, ConnectionManager] = {}
  _registry_lock = threading.Lock()

  def __init__(
    self,
    backend_type: BackendType,
    settings: dict[str, Any] | None = None,
  ) -> None:
    """Initialize connection manager.

    Args:
        backend_type: The type of backend to manage.
        settings: Backend-specific settings dictionary.
    """
    self.backend_type = backend_type
    self.settings = settings or {}
    self._backend: Backend | None = None
    self._lock = threading.Lock()

  @classmethod
  def get_manager(
    cls,
    backend_type: BackendType,
    settings: dict[str, Any] | None = None,
  ) -> ConnectionManager:
    """Get or create a connection manager.

    Args:
        backend_type: The type of backend.
        settings: Backend-specific settings.

    Returns:
        A ConnectionManager instance for the given backend.
    """
    normalized_settings = settings or {}
    try:
      settings_key = json.dumps(
        normalized_settings,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
      )
    except (TypeError, ValueError):
      settings_key = str(sorted(normalized_settings.items()))

    key = f"{backend_type.value}:{settings_key}"

    with cls._registry_lock:
      if key not in cls._managers:
        cls._managers[key] = cls(backend_type, normalized_settings)
      return cls._managers[key]

  def _create_backend(self) -> Backend:
    """Create a backend instance based on type.

    Returns:
        A new backend instance.

    Raises:
        ValueError: If the backend type is not supported.
    """
    if self.backend_type == BackendType.REDIS:
      from scrapy_extension.backends.redis_backend import RedisBackend
      from scrapy_extension.settings import RedisSettings

      config = RedisSettings(**self.settings)
      return RedisBackend(config)
    if self.backend_type == BackendType.MONGODB:
      from scrapy_extension.backends.mongodb_backend import MongoDBBackend
      from scrapy_extension.settings import MongoDBSettings

      config = MongoDBSettings(**self.settings)
      return MongoDBBackend(config)
    if self.backend_type == BackendType.KAFKA:
      from scrapy_extension.backends.kafka_backend import KafkaBackend
      from scrapy_extension.settings import KafkaSettings

      config = KafkaSettings(**self.settings)
      return KafkaBackend(config)
    if self.backend_type == BackendType.RABBITMQ:
      from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
      from scrapy_extension.settings import RabbitMQSettings

      config = RabbitMQSettings(**self.settings)
      return RabbitMQBackend(config)
    msg = f"Unsupported backend type: {self.backend_type}"
    raise ValueError(msg)

  def connect(self) -> None:
    """Establish connection with retry logic.

    Attempts to connect with exponential backoff based on
    retry_attempts and retry_delay settings.

    Raises:
        ConnectionError: If all retry attempts fail.
    """
    retry_attempts = self.settings.get("retry_attempts", 3)
    retry_delay = self.settings.get("retry_delay", 1.0)

    last_exception: Exception | None = None
    for attempt in range(retry_attempts):
      try:
        self._attempt_connection()
      except Exception as e:  # noqa: PERF203
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
          raise
        last_exception = e
        logger.warning(
          "Connection attempt %d/%d failed: %s", attempt + 1, retry_attempts, e
        )
        if attempt < retry_attempts - 1:
          time.sleep(retry_delay * (2**attempt))
      else:
        logger.debug("Connected to %s", self.backend_type.value)
        return

    if last_exception is not None:
      msg = f"Failed to connect after {retry_attempts} attempts: {last_exception}"
      raise BackendConnectionError(
        msg,
        backend_type=self.backend_type.value,
      ) from last_exception

  def _attempt_connection(self) -> None:
    """Attempt a single connection.

    Raises:
        Exception: If the connection attempt fails.
    """
    self._backend = self._create_backend()
    self._backend.connect()

  def close(self) -> None:
    """Close the backend connection.

    Closes the connection and cleans up resources.
    """
    with self._lock:
      if self._backend:
        try:
          self._backend.disconnect()
          logger.debug("Disconnected from %s", self.backend_type.value)
        except (RuntimeError, ValueError, AttributeError) as e:
          logger.warning("Error during disconnect: %s", e)
        finally:
          self._backend = None

  @property
  def backend(self) -> Backend:
    """Get the backend instance, connecting if necessary.

    Returns:
        The backend instance.

    Raises:
        ConnectionError: If connection fails.
    """
    if self._backend is None:
      with self._lock:
        if self._backend is None:
          self.connect()
    assert self._backend is not None
    return self._backend

  def is_connected(self) -> bool:
    """Check if backend is connected.

    Returns:
        True if connected, False otherwise.
    """
    if self._backend is None:
      return False
    return self._backend.is_connected()

  def get_queue_backend(self) -> QueueBackend:
    """Get the queue backend interface.

    Returns:
        The QueueBackend interface of the backend.
    """
    backend = self.backend
    if not isinstance(backend, QueueBackend):
      msg = f"Backend {backend.__class__.__name__} does not support queue operations"
      raise NotImplementedError(msg)
    return backend

  def get_set_backend(self) -> SetBackend:
    """Get the set backend interface.

    Returns:
        The SetBackend interface of the backend.
    """
    backend = self.backend
    if not isinstance(backend, SetBackend):
      msg = f"Backend {backend.__class__.__name__} does not support set operations"
      raise NotImplementedError(msg)
    return backend

  def get_storage_backend(self) -> StorageBackend:
    """Get the storage backend interface.

    Returns:
        The StorageBackend interface of the backend.
    """
    backend = self.backend
    if not isinstance(backend, StorageBackend):
      msg = f"Backend {backend.__class__.__name__} does not support storage operations"
      raise NotImplementedError(msg)
    return backend
