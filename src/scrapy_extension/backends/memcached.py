"""Memcached backend (StorageBackend) — distributed KV cache (subsystem ③).

Implements StorageBackend using Memcached (key-value, TTL via ``expire``).
Does NOT implement QueueBackend or SetBackend — Memcached has no native
ordered queue or set data structure. Adds a NoSQL key-value backend
complementary to the existing Redis/MongoDB/ES storage backends.

pymemcache API used (stable):
- ``pymemcache.client.base.Client((host, port))``
- ``client.set(key, value, expire=ttl)``
- ``client.get(key)``
- ``client.delete(key)``
- ``client.flush_all()``
- ``client.stats()``
- ``client.close()``
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

try:
  from pymemcache.client.base import Client as MemcachedClient
except ImportError as e:
  raise ImportError(
    "Memcached backend requires 'pymemcache'. "
    "Install with: pip install scrapy-extension[memcached]"
  ) from e

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  StorageBackend,
  _validate_key_name,
)
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import MemcachedMode

if TYPE_CHECKING:
  from scrapy_extension.settings import MemcachedSettings

logger = logging.getLogger(__name__)


class MemcachedBackend(Backend, StorageBackend):
  """Memcached storage backend (KV with TTL).

  Stores values under keys with an optional TTL (``expire``). Limitations
  (Memcached has no native support): ``ttl()`` always returns ``None``
  (remaining TTL not exposed); ``clear_storage(prefix)`` flushes ALL keys
  (prefix filtering not supported).

  Attributes:
      config: MemcachedSettings instance.
      _client: The pymemcache Client (None until connected).
  """

  def __init__(self, config: MemcachedSettings) -> None:
    """Initialize the Memcached backend.

    Args:
        config: Configuration for the Memcached connection.
    """
    self.config = config
    self._client: Any = None

  def connect(self) -> None:
    """Connect to Memcached and verify with a stats() call.

    On failure the half-created client is closed and ``_client`` is reset to
    ``None`` so :meth:`is_connected` reports ``False`` truthfully (R-mcc,
    mirrors RabbitMQ R25-A1) -- a failed ``stats()`` probe must not leave a
    never-connected client that lies "connected" and wedges the backend.

    Raises:
        BackendConnectionError: If the connection cannot be established.
    """
    if self.config.mode is not MemcachedMode.STANDALONE:
      raise BackendConnectionError(
        f"Unsupported Memcached mode: {self.config.mode}",
        backend_type="memcached",
      )
    try:
      self._client = MemcachedClient((self.config.host, self.config.port))
      self._client.stats()
      logger.debug("Connected to Memcached at %s:%s", self.config.host, self.config.port)
    except Exception as e:
      # R-mcc: null the half-created client so is_connected() stays truthful.
      # pymemcache's Client ctor is lazy (no network I/O); stats() is the real
      # probe, so a failed stats() leaves _client pointing at a never-connected
      # client. Without this reset, is_connected() (``return self._client is not
      # None``) returns True after a connect() that already raised
      # BackendConnectionError -- ConnectionManager.is_connected() delegates
      # here (connectors.py), so external health checks would see a lying True
      # and skip reconnect, wedging the backend "connected-but-dead". Mirrors
      # RabbitMQ R25-A1 null-on-failure (rabbitmq.py:246). The ``is not None``
      # guard also covers the ctor-raises path (client never assigned -> skip).
      if self._client is not None:
        with _swallow():
          self._client.close()
        self._client = None
      raise BackendConnectionError(
        f"Failed to connect to Memcached ({self.config.host}:{self.config.port}): {e}",
        backend_type="memcached",
      ) from e

  def disconnect(self) -> None:
    """Close the Memcached client."""
    if self._client is not None:
      with _swallow():
        self._client.close()
      self._client = None

  def is_connected(self) -> bool:
    """Return True if the client has been created."""
    return self._client is not None

  def ping(self) -> bool:
    """Check Memcached health via stats().

    Returns:
        True if stats() succeeds.
    """
    if self._client is None:
      return False
    try:
      self._client.stats()
      return True
    except Exception:
      return False

  @property
  def backend_type(self) -> BackendType:
    """Return BackendType.MEMCACHED."""
    return BackendType.MEMCACHED

  # StorageBackend implementation
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store ``data`` under ``key`` with optional TTL.

    Args:
        key: Storage key.
        data: Data to store (bytes).
        ttl: Optional time-to-live in seconds.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the underlying client raises (was previously
            silently swallowed to ``return None``, masking data loss).
    """
    _validate_key_name(key, "key")
    try:
      self._client.set(key, data, expire=ttl)
    except Exception as e:
      msg = f"Failed to store key {key!r} in Memcached: {e}"
      raise StorageError(msg, operation="store", key=key) from e

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key.

    Args:
        key: Storage key.

    Returns:
        Stored data, or None if not found.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the underlying client raises (was previously
            silently swallowed to ``return None``).
    """
    _validate_key_name(key, "key")
    try:
      return cast("bytes | None", self._client.get(key))
    except Exception as e:
      msg = f"Failed to retrieve key {key!r} from Memcached: {e}"
      raise StorageError(msg, operation="retrieve", key=key) from e

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if the key existed and was deleted, False otherwise.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the underlying client raises (was previously
            silently swallowed to ``return False``).
    """
    _validate_key_name(key, "key")
    try:
      return bool(self._client.delete(key))
    except Exception as e:
      msg = f"Failed to delete key {key!r} in Memcached: {e}"
      raise StorageError(msg, operation="delete", key=key) from e

  def exists(self, key: str) -> bool:
    """Check if a key exists.

    Args:
        key: Storage key.

    Returns:
        True if the key exists.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the underlying client raises (was previously
            silently swallowed to ``return False``).
    """
    _validate_key_name(key, "key")
    try:
      return self._client.get(key) is not None
    except Exception as e:
      msg = f"Failed to check existence of key {key!r} in Memcached: {e}"
      raise StorageError(msg, operation="exists", key=key) from e

  def ttl(self, key: str) -> int | None:
    """Return None — Memcached does not expose remaining TTL.

    Args:
        key: Storage key.

    Returns:
        Always None (unsupported by Memcached).

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    return None

  def clear_storage(self, prefix: str | None = None) -> None:
    """Flush ALL keys (prefix scoping is unsupported by Memcached).

    Args:
        prefix: If ``None``, flush the entire cache. If non-None, rejected —
            Memcached ``flush_all`` cannot scope to a prefix, so accepting
            one would silently destroy other tenants on a shared cluster.

    Raises:
        ValueError: If ``prefix`` contains invalid characters.
        NotImplementedError: If ``prefix`` is not ``None`` (protocol limit).
        StorageError: If the underlying client raises (was previously
            silently swallowed).
    """
    if prefix is not None:
      _validate_key_name(prefix, "prefix")
      raise NotImplementedError(
        "Memcached flush_all does not support prefix scoping; pass "
        "prefix=None to flush the entire cache."
      )
    try:
      self._client.flush_all()
    except Exception as e:
      msg = f"Failed to flush Memcached: {e}"
      raise StorageError(msg, operation="clear_storage", key=None) from e


class _swallow:
  """Context manager that swallows cleanup-path errors (close() etc.)."""

  def __enter__(self) -> _swallow:
    return self

  def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
    if exc_type is None:
      return False
    # R-swallow: suppress only regular cleanup Exceptions -- NEVER BaseException
    # (KeyboardInterrupt / SystemExit / GeneratorExit). Pre-fix this returned
    # True for any non-None exc_type, trapping Ctrl+C during close()/disconnect
    # (the operator's shutdown signal disappeared into a debug log).
    if not isinstance(exc, Exception):
      return False
    logger.debug("Suppressed memcached cleanup error: %s", exc)
    return True
