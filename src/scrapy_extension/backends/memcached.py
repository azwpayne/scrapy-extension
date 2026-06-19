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
from typing import TYPE_CHECKING, Any

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
    """
    _validate_key_name(key, "key")
    try:
      self._client.set(key, data, expire=ttl)
    except Exception as e:
      logger.warning("Failed to store key %s: %s", key, e)

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key.

    Args:
        key: Storage key.

    Returns:
        Stored data, or None if not found.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      return self._client.get(key)
    except Exception as e:
      logger.warning("Failed to retrieve key %s: %s", key, e)
      return None

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if the key existed and was deleted, False otherwise.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      return bool(self._client.delete(key))
    except Exception:
      return False

  def exists(self, key: str) -> bool:
    """Check if a key exists.

    Args:
        key: Storage key.

    Returns:
        True if the key exists.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      return self._client.get(key) is not None
    except Exception:
      return False

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
    """Flush ALL keys (prefix filtering not supported by Memcached).

    Args:
        prefix: Ignored — Memcached flush_all clears everything.

    Raises:
        ValueError: If prefix contains invalid characters.
    """
    if prefix:
      _validate_key_name(prefix, "prefix")
    try:
      self._client.flush_all()
    except Exception as e:
      logger.warning("Failed to flush Memcached: %s", e)


class _swallow:
  """Context manager that swallows cleanup-path errors (close() etc.)."""

  def __enter__(self) -> _swallow:
    return self

  def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
    if exc_type is None:
      return False
    logger.debug("Suppressed memcached cleanup error: %s", exc)
    return True
