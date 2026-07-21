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
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, cast

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
  from pymemcache.client.base import Client as MemcachedClient
except ImportError as e:
  if not _is_missing_optional_dependency(e, "pymemcache"):
    raise
  raise ImportError(
    "Memcached backend requires 'pymemcache'. "
    "Install with: pip install scrapy-extension[memcached]"
  ) from e

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  StorageBackend,
  _validate_key_name,
  _validate_ttl,
)
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import MemcachedMode
from scrapy_extension.settings.memcached import (
  is_memcached_loopback,
  validate_memcached_connection,
  validate_memcached_flush_policy,
)

if TYPE_CHECKING:
  from scrapy_extension.settings import MemcachedSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MemcachedConnectionSnapshot:
  """One validated set of values used by a Memcached connect attempt."""

  mode: MemcachedMode
  host: str
  port: int
  allow_remote_plaintext: bool
  allow_flush_all: bool


class MemcachedBackend(Backend, StorageBackend):
  """Memcached storage backend (KV with TTL).

  Stores values under keys with an optional TTL (``expire``). Limitations
  (Memcached has no native support): ``ttl()`` always returns ``None``
  (remaining TTL not exposed). Memcached cannot enumerate or prefix-filter
  keys, so ``clear_storage`` is disabled by default; the destructive
  server-wide ``flush_all`` operation requires ``allow_flush_all=True``.

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
    self._connection_snapshot: _MemcachedConnectionSnapshot | None = None
    # pymemcache's ordinary Client owns one request/response socket and is not
    # thread-safe. Serialize every SDK transaction with connect/disconnect so
    # replies cannot cross-wire and teardown cannot race an active operation.
    self._operation_lock = Lock()
    self._connect_lock = Lock()
    self._lifecycle_lock = Lock()
    self._lifecycle_generation = 0

  def _capture_connection_snapshot(self) -> _MemcachedConnectionSnapshot:
    """Capture and revalidate every value used by one connect attempt."""
    mode, host, port, allow_remote = validate_memcached_connection(
      self.config.mode,
      self.config.host,
      self.config.port,
      self.config.allow_remote_plaintext,
    )
    allow_flush_all = validate_memcached_flush_policy(
      self.config.allow_flush_all
    )
    return _MemcachedConnectionSnapshot(
      mode=mode,
      host=host,
      port=port,
      allow_remote_plaintext=allow_remote,
      allow_flush_all=allow_flush_all,
    )

  def connect(self) -> None:
    """Connect to Memcached and verify with a stats() call.

    The candidate remains private until ``stats()`` succeeds. On failure it is
    closed without ever publishing ``_client``, so :meth:`is_connected`
    truthfully remains false. Repeated calls while connected are idempotent.

    Raises:
        BackendConnectionError: If the connection cannot be established.
    """
    with self._connect_lock:
      with self._lifecycle_lock:
        if self._client is not None:
          return
        generation = self._lifecycle_generation
      snapshot = self._capture_connection_snapshot()
      candidate: Any = None
      try:
        # pymemcache defaults ``default_noreply=True``. In that mode set,
        # delete, and flush can return success after only writing the command
        # to the socket; the server's STORED/DELETED/error response is never
        # read. StorageBackend success is a commit boundary, so require replies
        # for every mutating operation on this client generation.
        candidate = MemcachedClient(
          (snapshot.host, snapshot.port), default_noreply=False
        )
        candidate.stats()
      except Exception:
        if candidate is not None:
          with _swallow():
            candidate.close()
        raise BackendConnectionError(
          "Failed to connect to Memcached.", backend_type="memcached"
        ) from None
      with self._operation_lock:
        with self._lifecycle_lock:
          # A concurrent disconnect fences this private probe by advancing the
          # lifecycle generation. Never resurrect a client after teardown.
          publish = generation == self._lifecycle_generation
          if publish:
            self._client = candidate
            self._connection_snapshot = snapshot
      if not publish:
        with _swallow():
          candidate.close()
        return
      if not is_memcached_loopback(snapshot.host):
        logger.warning(
          "Remote Memcached plaintext was explicitly enabled for %s:%d; "
          "use only an isolated trusted network.",
          snapshot.host,
          snapshot.port,
        )
      logger.debug(
        "Connected to Memcached at %s:%s", snapshot.host, snapshot.port
      )

  def disconnect(self) -> None:
    """Close the Memcached client."""
    with self._operation_lock:
      with self._lifecycle_lock:
        self._lifecycle_generation += 1
        client = self._client
        self._client = None
        self._connection_snapshot = None
      if client is not None:
        with _swallow():
          client.close()

  def is_connected(self) -> bool:
    """Return True if the client has been created."""
    with self._lifecycle_lock:
      return self._client is not None

  def ping(self) -> bool:
    """Check Memcached health via stats().

    Returns:
        True if stats() succeeds.
    """
    with self._operation_lock:
      with self._lifecycle_lock:
        client = self._client
      if client is None:
        return False
      try:
        client.stats()
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
    _validate_ttl(ttl)
    with self._operation_lock:
      with self._lifecycle_lock:
        client = self._client
      try:
        stored = client.set(key, data, expire=0 if ttl is None else ttl)
      except Exception as e:
        msg = f"Failed to store key {key!r} in Memcached: {e}"
        raise StorageError(msg, operation="store", key=key) from e
    if stored is not True:
      raise StorageError(
        f"Memcached rejected the write for key {key!r}",
        operation="store",
        key=key,
      )

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
    with self._operation_lock:
      with self._lifecycle_lock:
        client = self._client
      try:
        return cast("bytes | None", client.get(key))
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
    with self._operation_lock:
      with self._lifecycle_lock:
        client = self._client
      try:
        return bool(client.delete(key))
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
    with self._operation_lock:
      with self._lifecycle_lock:
        client = self._client
      try:
        return client.get(key) is not None
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
    """Flush all server keys only when explicitly enabled.

    Args:
        prefix: A non-None prefix is always rejected because Memcached cannot
            scope ``flush_all``. ``None`` is accepted only when the backend
            was configured with ``allow_flush_all=True``.

    Raises:
        ValueError: If ``prefix`` contains invalid characters.
        NotImplementedError: If prefix scoping is requested or the destructive
            global flush has not been explicitly enabled.
        StorageError: If the underlying client raises (was previously
            silently swallowed).
    """
    if prefix is not None:
      _validate_key_name(prefix, "prefix")
      raise NotImplementedError(
        "Memcached flush_all does not support prefix scoping; pass "
        "prefix=None only when a server-wide flush is explicitly acceptable."
      )
    with self._operation_lock:
      with self._lifecycle_lock:
        client = self._client
        snapshot = self._connection_snapshot
      if snapshot is None or not snapshot.allow_flush_all:
        raise NotImplementedError(
          "Memcached clear_storage would flush every key on the server. Set "
          "SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=true (allow_flush_all=True) only "
          "for a dedicated cache where that destructive scope is intended."
        )
      try:
        flushed = client.flush_all()
      except Exception as e:
        msg = f"Failed to flush Memcached: {e}"
        raise StorageError(msg, operation="clear_storage", key=None) from e
      if flushed is not True:
        raise StorageError(
          "Memcached rejected the server-wide flush.",
          operation="clear_storage",
          key=None,
        )


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
