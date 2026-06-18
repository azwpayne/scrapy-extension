"""Connection manager for backend connections.

This module provides a lazy singleton connection manager with retry logic
for all backend types.
"""

from __future__ import annotations

import contextlib
import json
import logging
import random
import threading
import time
from typing import TYPE_CHECKING, Any, ClassVar

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
  _managers: ClassVar[dict[str, ConnectionManager]] = {}
  _registry_lock: ClassVar[threading.Lock] = threading.Lock()

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
    key = cls._registry_key(backend_type, normalized_settings)

    with cls._registry_lock:
      if key not in cls._managers:
        cls._managers[key] = cls(backend_type, normalized_settings)
      return cls._managers[key]

  @staticmethod
  def _registry_key(
    backend_type: BackendType,
    settings: dict[str, Any],
  ) -> str:
    """Compute the registry cache key for a backend type + settings pair."""
    try:
      settings_key = json.dumps(
        settings,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
      )
    except (TypeError, ValueError):
      settings_key = str(sorted(settings.items()))
    return f"{backend_type.value}:{settings_key}"

  def _create_backend(self) -> Backend:
    """Create a backend instance based on type.

    Returns:
        A new backend instance.

    Raises:
        ValueError: If the backend type is not supported.
    """
    match self.backend_type:
      case BackendType.REDIS:
        from scrapy_extension.backends.redis import RedisBackend
        from scrapy_extension.settings import RedisSettings

        config = RedisSettings(**self.settings)
        return RedisBackend(config)
      case BackendType.MONGODB:
        from scrapy_extension.backends.mongodb import MongoDBBackend
        from scrapy_extension.settings import MongoDBSettings

        return MongoDBBackend(MongoDBSettings(**self.settings))
      case BackendType.KAFKA:
        from scrapy_extension.backends.kafka import KafkaBackend
        from scrapy_extension.settings import KafkaSettings

        return KafkaBackend(KafkaSettings(**self.settings))
      case BackendType.RABBITMQ:
        from scrapy_extension.backends.rabbitmq import RabbitMQBackend
        from scrapy_extension.settings import RabbitMQSettings

        return RabbitMQBackend(RabbitMQSettings(**self.settings))
      case BackendType.ELASTICSEARCH:
        from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
        from scrapy_extension.settings import ElasticSearchSettings

        return ElasticSearchBackend(ElasticSearchSettings(**self.settings))
      case BackendType.ROCKETMQ:
        from scrapy_extension.backends.rocketmq import RocketMQBackend
        from scrapy_extension.settings import RocketMQSettings

        return RocketMQBackend(RocketMQSettings(**self.settings))
      case _:
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
      except Exception as e:
        # Intentional broad catch: any backend connection error should trigger retry.
        # Backend-specific exceptions (RedisError, PyMongoError, KafkaError, AMQPError)
        # all inherit from Exception. Explicitly re-raise KeyboardInterrupt/SystemExit.
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
          raise
        last_exception = e
        logger.warning(
          "Connection attempt %d/%d failed: %s", attempt + 1, retry_attempts, e
        )
        if attempt < retry_attempts - 1:
          # Full jitter: random.uniform(0, delay). Prevents thundering herd
          # when many workers retry simultaneously after a coordinated
          # outage (e.g., Redis failover). See AWS Architecture Blog:
          # "Exponential Backoff and Jitter".
          delay = retry_delay * (2**attempt)
          time.sleep(random.uniform(0, delay))
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

    Builds the backend and connects it. The instance attribute is only
    assigned after ``connect()`` succeeds, so a failure leaves ``_backend``
    in its previous state (typically None) instead of a half-constructed
    object that callers would mistake for a usable backend.

    On failure, ``backend.disconnect()`` is invoked so resources allocated
    before the failure (e.g., a Redis connection pool created by the client
    constructor, then orphaned when ``ping()`` fails) are released. Without
    this, each retry leaks one connection pool; a tight retry loop on
    network failure exhausts the broker's connection limit.

    Raises:
        Exception: If the connection attempt fails.
    """
    backend = self._create_backend()
    try:
      backend.connect()
    except Exception:
      with contextlib.suppress(Exception):
        backend.disconnect()
      raise
    self._backend = backend

  def close(self) -> None:
    """Close the backend connection.

    Closes the connection and cleans up resources. Also removes the
    instance from the class-level registry so subsequent
    ``get_manager()`` calls create a fresh manager.
    """
    with self._lock:
      if self._backend:
        try:
          self._backend.disconnect()
          logger.debug("Disconnected from %s", self.backend_type.value)
        except Exception as e:
          # Broad catch — mirrors R25-A1's connect-path cleanup
          # (contextlib.suppress(Exception)). Disconnecting a possibly-broken
          # backend can raise anything: an OSError from the socket layer that
          # the backend's own disconnect didn't self-suppress, or a
          # backend-specific error. close() must still complete registry
          # eviction (below) — never propagate out of the close chain.
          logger.warning("Error during disconnect: %s", e)
        finally:
          self._backend = None
      # Remove from registry so the next get_manager() doesn't return
      # a closed instance. Compute the key the same way get_manager does.
      key = type(self)._registry_key(self.backend_type, self.settings)
      cls = type(self)
      with cls._registry_lock:
        cls._managers.pop(key, None)

  @classmethod
  def clear_registry(cls) -> None:
    """Close and clear all registered managers.

    Intended for test isolation: the class-level ``_managers`` dict
    otherwise accumulates entries across test runs, causing both a
    slow memory leak and cross-test pollution (one test's manager is
    returned for another test's get_manager call).
    """
    with cls._registry_lock:
      managers = list(cls._managers.values())
      cls._managers.clear()
    for manager in managers:
      with contextlib.suppress(Exception):
        manager.close()

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
