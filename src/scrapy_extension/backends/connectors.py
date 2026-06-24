"""Connection manager for backend connections.

This module provides a lazy singleton connection manager with retry logic
for all backend types.
"""

from __future__ import annotations

import contextlib
import importlib
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
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError

if TYPE_CHECKING:
  from scrapy_extension.backends.base import Backend

logger = logging.getLogger(__name__)


QUEUE_CAPABLE_BACKENDS: set[BackendType] = {
  BackendType.REDIS,
  BackendType.MONGODB,
  BackendType.KAFKA,
  BackendType.RABBITMQ,
  BackendType.ELASTICSEARCH,
  BackendType.ROCKETMQ,
  BackendType.PULSAR,
  BackendType.SQS,
}
SET_CAPABLE_BACKENDS: set[BackendType] = {
  BackendType.REDIS,
  BackendType.MONGODB,
  BackendType.ELASTICSEARCH,
}
STORAGE_CAPABLE_BACKENDS: set[BackendType] = {
  BackendType.REDIS,
  BackendType.MONGODB,
  BackendType.ELASTICSEARCH,
  BackendType.MEMCACHED,
  BackendType.DYNAMODB,
}

# Dispatch table mapping each BackendType to its lazy import paths
# (backend_class_path, settings_class_path). Imports happen on demand via
# importlib.import_module + getattr, which mirrors the original per-arm
# ``from scrapy_extension.backends.<x> import XBackend`` semantics — keeping
# the import path stable so tests that patch the canonical module attribute
# (e.g. ``mocker.patch("scrapy_extension.backends.redis.RedisBackend")``)
# continue to intercept construction.
_BACKEND_FACTORIES: dict[BackendType, tuple[str, str]] = {
  BackendType.REDIS: (
    "scrapy_extension.backends.redis.RedisBackend",
    "scrapy_extension.settings.RedisSettings",
  ),
  BackendType.MONGODB: (
    "scrapy_extension.backends.mongodb.MongoDBBackend",
    "scrapy_extension.settings.MongoDBSettings",
  ),
  BackendType.KAFKA: (
    "scrapy_extension.backends.kafka.KafkaBackend",
    "scrapy_extension.settings.KafkaSettings",
  ),
  BackendType.RABBITMQ: (
    "scrapy_extension.backends.rabbitmq.RabbitMQBackend",
    "scrapy_extension.settings.RabbitMQSettings",
  ),
  BackendType.ELASTICSEARCH: (
    "scrapy_extension.backends.elasticsearch.ElasticSearchBackend",
    "scrapy_extension.settings.ElasticSearchSettings",
  ),
  BackendType.ROCKETMQ: (
    "scrapy_extension.backends.rocketmq.RocketMQBackend",
    "scrapy_extension.settings.RocketMQSettings",
  ),
  BackendType.PULSAR: (
    "scrapy_extension.backends.pulsar.PulsarBackend",
    "scrapy_extension.settings.PulsarSettings",
  ),
  BackendType.SQS: (
    "scrapy_extension.backends.sqs.SqsBackend",
    "scrapy_extension.settings.SqsSettings",
  ),
  BackendType.MEMCACHED: (
    "scrapy_extension.backends.memcached.MemcachedBackend",
    "scrapy_extension.settings.MemcachedSettings",
  ),
  BackendType.DYNAMODB: (
    "scrapy_extension.backends.dynamodb.DynamoDBBackend",
    "scrapy_extension.settings.DynamoDBSettings",
  ),
}


def _load_object(dotted_path: str) -> Any:
  """Lazily import and return the attribute at ``dotted_path``.

  Mirrors ``from <module> import <name>`` so tests that patch the canonical
  module attribute (e.g. ``scrapy_extension.backends.redis.RedisBackend``)
  still intercept the resolved class.

  Args:
      dotted_path: Fully-qualified ``module.submodule.Attr`` path.

  Returns:
      The resolved attribute.

  Raises:
      ValueError: If the path has no attribute separator.
      ImportError: If the module cannot be imported.
      AttributeError: If the attribute is missing from the module.
  """
  module_path, _, name = dotted_path.rpartition(".")
  if not module_path:
    msg = f"Invalid dotted path: {dotted_path!r}"
    raise ValueError(msg)
  module = importlib.import_module(module_path)
  return getattr(module, name)


def resolve_backend_config(
  settings: Any,
  type_key: str,
  settings_key: str,
  *,
  required_capabilities: set[BackendType] | None = None,
  component_name: str = "",
) -> tuple[BackendType, dict[str, Any]]:
  """Resolve a component's backend config, preferring per-component keys.

  Multi-backend coexistence: each component (queue / set / storage) can bind
  to its own backend via a per-component key pair — e.g. queue seeds in
  Redis-Cluster while dedup fingerprints live in MongoDB. When the
  per-component ``type_key`` is set, the component uses the per-component
  ``settings_key``; otherwise it falls back to the global
  ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so existing
  single-backend configurations keep working unchanged.

  Capability validation (I-1): when ``required_capabilities`` is supplied,
  the resolved backend type must be in that set or ``ConfigurationError``
  is raised at config time (fail-fast). This prevents a late, confusing
  crash mid-crawl — e.g. configuring Kafka (queue-only) for dedup and only
  discovering it when ``request_seen()`` fires on the first request.

  Empty-string normalization (I-3): ``SCRAPY_BACKEND_TYPE=""`` (e.g. from
  an empty env var) is treated as unset and falls back to ``"redis"``,
  rather than raising ``ValueError`` inside ``BackendType("")``.

  Args:
      settings: A Scrapy Settings-like object exposing ``get``/``getdict``.
      type_key: The per-component backend-type setting key.
      settings_key: The per-component backend-settings setting key.
      required_capabilities: Optional allowlist of backend types that satisfy
          this component's interface (queue/set/storage). ``None`` skips
          validation (backward compatible).
      component_name: Human-readable component name for error messages
          (e.g. ``"queue"``, ``"set"``, ``"storage"``).

  Returns:
      A ``(backend_type, settings_dict)`` tuple ready for
      ``ConnectionManager.get_manager(...)``.

  Raises:
      ConfigurationError: If ``required_capabilities`` is set and the
          resolved backend type is not in it.
  """
  per_component_type = settings.get(type_key)
  if per_component_type:
    backend_type = BackendType(per_component_type)
    backend_settings = settings.getdict(settings_key, {})
    source_key = type_key
  else:
    backend_type = BackendType(settings.get("SCRAPY_BACKEND_TYPE") or "redis")
    backend_settings = settings.getdict("SCRAPY_BACKEND_SETTINGS", {})
    source_key = "SCRAPY_BACKEND_TYPE"

  if required_capabilities is not None and backend_type not in required_capabilities:
    capable = sorted(b.value for b in required_capabilities)
    msg = (
      f"Backend {backend_type.value!r} does not support the {component_name} "
      f"interface required by this component. Capable backends: {capable}."
    )
    raise ConfigurationError(msg, setting_name=source_key)

  return backend_type, backend_settings


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

    Dispatches via the module-level ``_BACKEND_FACTORIES`` table to keep
    this method's cyclomatic complexity flat regardless of how many backends
    exist. Each entry's class path is resolved lazily (via
    ``importlib.import_module`` + ``getattr``), preserving the original
    per-arm lazy-import semantics so optional backend dependencies stay
    loaded-on-demand and tests that patch the canonical module attribute
    still intercept construction.

    Returns:
        A new backend instance.

    Raises:
        ValueError: If the backend type is not supported.
    """
    try:
      backend_path, settings_path = _BACKEND_FACTORIES[self.backend_type]
    except KeyError:
      msg = f"Unsupported backend type: {self.backend_type}"
      raise ValueError(msg) from None
    backend_cls = _load_object(backend_path)
    settings_cls = _load_object(settings_path)
    return backend_cls(settings_cls(**self.settings))

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
          # nosec B311: random.uniform is intentional full-jitter backoff,
          # not a cryptographic primitive. Switching to secrets would remove
          # the bounded-range API we rely on without improving security.
          time.sleep(random.uniform(0, delay))  # nosec B311 - jitter, not cryptographic
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

    Idempotent and safe under shared-manager scenarios (I-2): when multiple
    components (e.g. dupefilter + pipeline) resolve to the same
    ``backend_type:settings_hash`` registry key, they share one
    ``ConnectionManager`` instance. Each component's ``close()`` calls this
    method; the first call disconnects and evicts the registry entry, and
    subsequent calls are no-ops (guarded by ``if self._backend:``). Scrapy
    serializes component shutdown today, so no concurrent-close hazard —
    but the idempotent contract is what makes co-located backends safe.
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
    if self._backend is None:
      # Defensive: connect() should either set _backend (success) or raise.
      # If we land here, connect() returned without connecting and without
      # raising — that is a contract violation, not a user input problem.
      # The explicit guard (rather than ``assert``) keeps the check live
      # under ``python -O`` and produces a clear, typed error instead of a
      # bare AssertionError.
      msg = "connect() did not produce a backend"
      raise BackendConnectionError(msg, backend_type=self.backend_type.value)
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
