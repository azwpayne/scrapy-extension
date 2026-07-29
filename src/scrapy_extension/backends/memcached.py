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
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from threading import Lock
from typing import Any, ParamSpec, TypeVar, cast

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
from scrapy_extension.exceptions._redaction import (
  backend_connection_error_boundary,
  configuration_error_boundary,
  storage_operation_error_boundary,
)
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import MemcachedMode, MemcachedSettings
from scrapy_extension.settings.memcached import (
  is_memcached_loopback,
  validate_memcached_connection,
  validate_memcached_flush_policy,
)

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")
_T = TypeVar("_T")

_MEMCACHED_CONFIGURATION_SETTING_NAMES: frozenset[str] = frozenset(
  MemcachedSettings.model_fields
)
_MEMCACHED_SAFE_CONNECTION_MESSAGES: frozenset[str] = frozenset(
  {"Failed to connect to Memcached."}
)
_MEMCACHED_STORAGE_STORE_ERROR = "Memcached storage store failed."
_MEMCACHED_STORAGE_RETRIEVE_ERROR = "Memcached storage retrieve failed."
_MEMCACHED_STORAGE_DELETE_ERROR = "Memcached storage delete failed."
_MEMCACHED_STORAGE_EXISTS_ERROR = "Memcached storage existence check failed."
_MEMCACHED_STORAGE_CLEAR_ERROR = "Memcached storage clear failed."
_MEMCACHED_CLEAR_STORAGE_PREFIX_UNSUPPORTED_MESSAGE = (
  "Memcached flush_all does not support prefix scoping; pass "
  "prefix=None only when a server-wide flush is explicitly acceptable."
)
_MEMCACHED_CLEAR_STORAGE_DISABLED_MESSAGE = (
  "Memcached clear_storage would flush every key on the server. Set "
  "SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=true (allow_flush_all=True) only "
  "for a dedicated cache where that destructive scope is intended."
)
_MEMCACHED_CLEAR_STORAGE_CAPABILITY_MESSAGES: frozenset[str] = frozenset(
  {
    _MEMCACHED_CLEAR_STORAGE_PREFIX_UNSUPPORTED_MESSAGE,
    _MEMCACHED_CLEAR_STORAGE_DISABLED_MESSAGE,
  }
)


def _validate_storage_key_argument(
  _backend: object,
  key: str,
  *_args: Any,
  **_kwargs: Any,
) -> None:
  """Validate a direct Memcached storage key before implementation frames."""
  _validate_key_name(key, "key")


def _validate_store_arguments(
  _backend: object,
  key: str,
  data: bytes,
  ttl: int | None = None,
) -> None:
  """Validate a direct Memcached storage write before its terminal boundary."""
  del data
  _validate_key_name(key, "key")
  _validate_ttl(ttl)


def _validate_storage_prefix_argument(
  _backend: object,
  prefix: str | None = None,
) -> None:
  """Validate a non-empty clear prefix before backend implementation frames."""
  if prefix is not None:
    _validate_key_name(prefix, "prefix")


def _clear_storage_capability_error_boundary(
  function: Callable[_P, _T],
) -> Callable[_P, _T]:
  """Rebuild the two documented Memcached clear capability errors safely.

  ``clear_storage`` intentionally has two distinct static
  :class:`NotImplementedError` contracts: prefix-scoped flushing is not
  possible and a server-wide flush needs an explicit connected-generation
  opt-in.  Both literals are public API, but raising either from the backend
  method retains its configuration and caller prefix in traceback locals.
  Reconstruct only those exact built-in errors after all implementation frames
  unwind; subclasses and unknown behavior keep their established contract.
  """

  @wraps(function)
  def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
    caught_error: NotImplementedError | None = None
    try:
      return function(*args, **kwargs)
    except NotImplementedError as error:
      if type(error) is not NotImplementedError:
        del args
        del kwargs
        raise
      caught_error = error
    except BaseException:
      del args
      del kwargs
      raise

    assert caught_error is not None
    replacement_message = _MEMCACHED_CLEAR_STORAGE_DISABLED_MESSAGE
    raw_args: object = caught_error.args
    if (
      type(raw_args) is tuple
      and len(raw_args) == 1
      and type(raw_args[0]) is str
      and raw_args[0] in _MEMCACHED_CLEAR_STORAGE_CAPABILITY_MESSAGES
    ):
      replacement_message = raw_args[0]
    sanitized_error = NotImplementedError(replacement_message)
    del args
    del kwargs
    del caught_error
    del raw_args
    del replacement_message
    raise sanitized_error

  return wrapped


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

  @configuration_error_boundary(
    "Memcached configuration is invalid.",
    _MEMCACHED_CONFIGURATION_SETTING_NAMES,
  )
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

  @backend_connection_error_boundary(
    "Failed to connect to Memcached.",
    "memcached",
    safe_messages=_MEMCACHED_SAFE_CONNECTION_MESSAGES,
  )
  @configuration_error_boundary(
    "Memcached configuration is invalid.",
    _MEMCACHED_CONFIGURATION_SETTING_NAMES,
    pass_through_exception_types=(BackendConnectionError,),
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
      startup_error: BackendConnectionError | None = None
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
          _close_failed_candidate(candidate)
        startup_error = BackendConnectionError(
          "Failed to connect to Memcached.", backend_type="memcached"
        )
      except BaseException:
        # R17-C: a Ctrl+C/SystemExit during the stats() probe (the first command
        # to open the TCP socket — pymemcache is lazy) must still close the
        # candidate socket. 'except Exception' cannot catch BaseException, so
        # without this arm a KeyboardInterrupt raised by stats() escapes before
        # candidate.close() runs, leaking the open FD. Candidate is never
        # published (generation-fenced at the publish step below), so
        # is_connected() stays truthful — bounded to a single FD per occurrence.
        # Mirror the R16-A kafka/rocketmq/dynamodb connect() BaseException arms.
        if candidate is not None:
          _close_failed_candidate(candidate)
        raise
      if startup_error is not None:
        # Raise outside the driver exception handler so endpoint/credential
        # text cannot survive through ``__cause__`` or ``__context__``.
        raise startup_error
      with self._operation_lock:
        with self._lifecycle_lock:
          # A concurrent disconnect fences this private probe by advancing the
          # lifecycle generation. Never resurrect a client after teardown.
          publish = generation == self._lifecycle_generation
          if publish:
            self._client = candidate
            self._connection_snapshot = snapshot
      if not publish:
        cleanup = _swallow()
        with cleanup:
          candidate.close()
        if cleanup.did_suppress:
          _log_suppressed_cleanup_error()
        return
      if not is_memcached_loopback(snapshot.host):
        # The client is already live. Diagnostics must not make a successful
        # connect appear to fail or cause callers to roll back this generation.
        try:
          logger.warning(
            "Remote Memcached plaintext was explicitly enabled; use only an "
            "isolated trusted network."
          )
        except BaseException:
          pass
      try:
        logger.debug("Connected to Memcached.")
      except BaseException:
        pass

  def disconnect(self) -> None:
    """Close the Memcached client."""
    with self._operation_lock:
      with self._lifecycle_lock:
        self._lifecycle_generation += 1
        client = self._client
        self._client = None
        self._connection_snapshot = None
      if client is not None:
        cleanup = _swallow()
        with cleanup:
          client.close()
        if cleanup.did_suppress:
          _log_suppressed_cleanup_error()

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
  @storage_operation_error_boundary(
    "store",
    _MEMCACHED_STORAGE_STORE_ERROR,
    "memcached",
    validator=_validate_store_arguments,
  )
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

  @storage_operation_error_boundary(
    "retrieve",
    _MEMCACHED_STORAGE_RETRIEVE_ERROR,
    "memcached",
    validator=_validate_storage_key_argument,
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

  @storage_operation_error_boundary(
    "delete",
    _MEMCACHED_STORAGE_DELETE_ERROR,
    "memcached",
    validator=_validate_storage_key_argument,
  )
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

  @storage_operation_error_boundary(
    "exists",
    _MEMCACHED_STORAGE_EXISTS_ERROR,
    "memcached",
    validator=_validate_storage_key_argument,
  )
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

  @_clear_storage_capability_error_boundary
  @storage_operation_error_boundary(
    "clear_storage",
    _MEMCACHED_STORAGE_CLEAR_ERROR,
    "memcached",
    validator=_validate_storage_prefix_argument,
  )
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
      raise NotImplementedError(_MEMCACHED_CLEAR_STORAGE_PREFIX_UNSUPPORTED_MESSAGE)
    with self._operation_lock:
      with self._lifecycle_lock:
        client = self._client
        snapshot = self._connection_snapshot
      if snapshot is None or not snapshot.allow_flush_all:
        raise NotImplementedError(_MEMCACHED_CLEAR_STORAGE_DISABLED_MESSAGE)
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
  """Suppress regular cleanup errors and report that suppression to callers.

  ``__exit__`` deliberately does not log: it executes while the cleanup
  exception remains active in ``sys.exc_info()``.  The caller can inspect
  :attr:`did_suppress` after the ``with`` statement has unwound and emit
  static telemetry without exposing that exception to a logging handler.
  """

  def __init__(self) -> None:
    self.did_suppress = False

  def __enter__(self) -> _swallow:
    self.did_suppress = False
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
    self.did_suppress = True
    return True


def _log_suppressed_cleanup_error() -> None:
  """Report a suppressed cleanup failure after its exception context unwinds."""
  try:
    logger.debug("Suppressed memcached cleanup error")
  except BaseException:
    # A diagnostic handler must not turn best-effort teardown into a failure.
    pass


def _close_failed_candidate(candidate: Any) -> None:
  """Best-effort cleanup for a private connect candidate.

  The caller is already handling the causal probe exception.  Unlike
  ``_swallow``, this must not run diagnostics that can replace that exception;
  close-time control signals are therefore intentionally suppressed here.
  """
  try:
    candidate.close()
  except BaseException:
    pass
