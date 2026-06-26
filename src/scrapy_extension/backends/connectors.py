"""Connection manager for backend connections.

This module provides a lazy singleton connection manager with retry logic
for all backend types.

Round-5 R5-1: the four prior hand-synced registries (``_BACKEND_FACTORIES``
+ ``QUEUE_CAPABLE_BACKENDS`` / ``SET_CAPABLE_BACKENDS`` /
``STORAGE_CAPABLE_BACKENDS``) have been consolidated into the single
:class:`~scrapy_extension.backends.registry.BackendDescriptor` table in
``registry.py``. The capability sets below are THIN BACKWARD-COMPAT
DELEGATIONS computed from the registry's single source of truth — they
exist so existing imports (``from scrapy_extension.backends.connectors
import QUEUE_CAPABLE_BACKENDS``) keep working, NOT as a parallel
registry to maintain.
"""

from __future__ import annotations

import contextlib
import importlib
import json
import logging
import random
import threading
import time
from typing import TYPE_CHECKING, Any, ClassVar, cast

from scrapy_extension.backends.base import (
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.circuit_breaker import CircuitBreaker
from scrapy_extension.backends.registry import (
  BackendDescriptor,
  get_descriptor,
  get_registry,
  has_capability,
)
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError

if TYPE_CHECKING:
  from scrapy_extension.backends.base import Backend

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Backward-compat capability sets (computed from the registry — single source
# of truth). These are ``frozenset[str]`` so membership tests against both
# plain strings and ``BackendType`` members (which compare equal to their
# string ``.value``) work unchanged.
# ---------------------------------------------------------------------------
# Kept as module-level constants so existing call sites and tests that import
# them (e.g. ``tests/test_rocketmq_backend.py``) continue to compile. The
# underlying data lives in ``registry._BUNDLED_DESCRIPTORS``; if a 3rd-party
# plugin registers additional capabilities they are picked up here too
# (the sets are rebuilt from the live registry).

def _capable_backends(capability: str) -> frozenset[str]:
  """Return the set of backend-type strings declaring ``capability``.

  Computed from the registry so the capability matrix has ONE source of
  truth (round-5 R5-1). Returns a frozen copy so callers can't mutate the
  cached registry.
  """
  return frozenset(
    name
    for name, descriptor in get_registry().items()
    if capability in descriptor.capabilities
  )


#: Backends implementing :class:`~scrapy_extension.backends.base.QueueBackend`.
#: Computed from the registry; replaces the hand-synced module-level set.
QUEUE_CAPABLE_BACKENDS: frozenset[str] = _capable_backends("queue")
#: Backends implementing :class:`~scrapy_extension.backends.base.SetBackend`.
SET_CAPABLE_BACKENDS: frozenset[str] = _capable_backends("set")
#: Backends implementing :class:`~scrapy_extension.backends.base.StorageBackend`.
STORAGE_CAPABLE_BACKENDS: frozenset[str] = _capable_backends("storage")


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
  required_capabilities: set[str] | None = None,
  component_name: str = "",
) -> tuple[str, dict[str, Any]]:
  """Resolve a component's backend config, preferring per-component keys.

  Multi-backend coexistence: each component (queue / set / storage) can bind
  to its own backend via a per-component key pair — e.g. queue seeds in
  Redis-Cluster while dedup fingerprints live in MongoDB. When the
  per-component ``type_key`` is set, the component uses the per-component
  ``settings_key``; otherwise it falls back to the global
  ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so existing
  single-backend configurations keep working unchanged.

  Capability validation (round-5 R5-1): when ``required_capabilities`` is
  supplied, the resolved backend's descriptor must declare EVERY capability
  in the set, else :class:`ConfigurationError` is raised at config time
  (fail-fast). This prevents a late, confusing crash mid-crawl — e.g.
  configuring Kafka (queue-only) for dedup and only discovering it when
  ``request_seen()`` fires on the first request.

  Round-5 R5-1 change: ``backend_type`` is now an opaque STRING validated
  against the descriptor table (was coerced to ``BackendType`` enum). This
  lets 3rd-party backends (plain strings, registered via entry-points)
  route through the same code path as bundled backends. ``BackendType``
  members still work — they're ``str`` subclasses whose ``.value`` is the
  registry key.

  Empty-string normalization (I-3): ``SCRAPY_BACKEND_TYPE=""`` (e.g. from
  an empty env var) is treated as unset and falls back to ``"redis"``,
  rather than raising.

  Args:
      settings: A Scrapy Settings-like object exposing ``get``/``getdict``.
      type_key: The per-component backend-type setting key.
      settings_key: The per-component backend-settings setting key.
      required_capabilities: Optional set of capability strings
          (``"queue"`` / ``"set"`` / ``"storage"``) the resolved backend
          must ALL declare. ``None`` skips validation (backward compatible).
      component_name: Human-readable component name for error messages
          (e.g. ``"queue"``, ``"set"``, ``"storage"``).

  Returns:
      A ``(backend_type, settings_dict)`` tuple ready for
      ``ConnectionManager.get_manager(...)``. ``backend_type`` is the
      registry-key string.

  Raises:
      ConfigurationError: If the resolved backend type is not registered,
          or if ``required_capabilities`` is set and the backend does not
          declare all of them.
  """
  per_component_type = settings.get(type_key)
  if per_component_type:
    backend_type, source_key = (
      _normalize_backend_type(per_component_type, type_key),
      type_key,
    )
    backend_settings = settings.getdict(settings_key, {})
  else:
    backend_type = _normalize_backend_type(
      settings.get("SCRAPY_BACKEND_TYPE") or "redis", "SCRAPY_BACKEND_TYPE"
    )
    backend_settings = settings.getdict("SCRAPY_BACKEND_SETTINGS", {})
    source_key = "SCRAPY_BACKEND_TYPE"

  if required_capabilities is not None:
    missing = [
      cap
      for cap in required_capabilities
      if not has_capability(backend_type, cap)
    ]
    if missing:
      capable = sorted(
        name
        for name, descriptor in get_registry().items()
        if all(cap in descriptor.capabilities for cap in required_capabilities)
      )
      msg = (
        f"Backend {backend_type!r} does not support the {component_name} "
        f"interface required by this component (missing capabilities: "
        f"{sorted(missing)}). Capable backends: {capable}."
      )
      raise ConfigurationError(msg, setting_name=source_key)

  return backend_type, backend_settings


def _normalize_backend_type(value: object, setting_name: str) -> str:
  """Normalize a config value into a backend-type registry string.

  Round-5 R5-1: this replaces the prior ``_coerce_backend_type`` that
  forced ``BackendType(value)``. The registry now keys on plain strings
  so 3rd-party backends (registered via entry-points) route through the
  same path. ``BackendType`` members pass through via their string
  ``.value``; plain strings pass through unchanged; anything else is
  stringified then validated against the registry (unknown → typed
  ``ConfigurationError`` with the setting name + value attached).

  Args:
      value: The raw setting value (``BackendType``, ``str``, or other).
      setting_name: The setting key the value came from — attached to the
          raised ``ConfigurationError`` for operator triage.

  Returns:
      The normalized backend-type registry string.

  Raises:
      ConfigurationError: If ``value`` does not map to a registered backend.
  """
  if isinstance(value, BackendType):
    return value.value
  if isinstance(value, str):
    candidate = value
  else:
    candidate = str(value)
  try:
    get_descriptor(candidate)
  except ConfigurationError:
    valid = sorted(get_registry().keys())
    msg = (
      f"Invalid backend type {value!r} for setting {setting_name!r}. "
      f"Valid values: {valid}."
    )
    raise ConfigurationError(
      msg, setting_name=setting_name, setting_value=value
    ) from None
  return candidate


class ConnectionManager:
  """Lazy singleton connection manager for backends.

  This class manages backend connections with:
  - Lazy initialization (connects on first use)
  - Thread-safe singleton pattern
  - Automatic retry with exponential backoff
  - Connection pooling

  Attributes:
      backend_type: The type of backend to manage (registry-key string).
      settings: Backend-specific settings.
      _backend: The backend instance (None until connected).
      _lock: Threading lock for thread safety.
  """

  # Class-level registry of managers
  _managers: ClassVar[dict[str, ConnectionManager]] = {}
  _registry_lock: ClassVar[threading.Lock] = threading.Lock()

  def __init__(
    self,
    backend_type: str,
    settings: dict[str, Any] | None = None,
  ) -> None:
    """Initialize connection manager.

    Args:
        backend_type: The backend-type registry string (e.g. ``"redis"``,
            or a ``BackendType`` member which is a ``str`` subclass).
        settings: Backend-specific settings dictionary.
    """
    self.backend_type = backend_type
    self.settings = settings or {}
    self._backend: Backend | None = None
    self._lock = threading.Lock()
    # Refcount of outstanding ``get_manager()`` acquire calls sharing this
    # instance (A1). The manager is only created via ``get_manager()``, so
    # the constructor sets the initial count to 0; ``get_manager()`` then
    # increments to 1 on first insertion. Each subsequent acquire bumps it;
    # each ``close()`` (release) decrements; only the last holder actually
    # disconnects + evicts the registry entry.
    self._users: int = 0
    # Single-connect ownership flag (A2). The first thread to enter the slow
    # path takes ownership under ``_lock``; peers wait on ``_connected_event``
    # until the owner finishes connect() — which runs its retry loop (and
    # time.sleep backoff) WITHOUT holding ``_lock``, so peers backing off on
    # a slow backend don't block threads that merely want to READ _backend.
    self._connecting: bool = False
    self._connected_event = threading.Event()
    # Circuit-breaker holder. Lazily constructed on first
    # ``get_*_backend()`` call from the env-loaded ``Settings``
    # (``SCRAPY_CIRCUIT_BREAKER_ENABLED``). ``None`` while disabled — which
    # is the default, so the default path returns the raw backend with zero
    # overhead and byte-identical behavior.
    self._breaker: CircuitBreaker | None = None
    self._breaker_configured: bool = False

  @classmethod
  def get_manager(
    cls,
    backend_type: str,
    settings: dict[str, Any] | None = None,
  ) -> ConnectionManager:
    """Get or create a connection manager (acquire semantics).

    Each call registers an acquire on the returned instance: the shared
    manager's ``_users`` refcount is incremented under the registry lock.
    Callers MUST pair every successful ``get_manager()`` with a ``close()``
    (release). Only the LAST holder's ``close()`` disconnects the backend
    and evicts the registry entry — earlier holders' closes are no-ops on
    the backend, so co-located components (e.g. scheduler queue +
    dupefilter sharing one Redis) don't tear each other's connection down
    during shutdown.

    Args:
        backend_type: The backend-type registry string (or ``BackendType``
            member, which is a ``str`` subclass).
        settings: Backend-specific settings.

    Returns:
        A ConnectionManager instance for the given backend.
    """
    normalized_settings = settings or {}
    key = cls._registry_key(backend_type, normalized_settings)

    with cls._registry_lock:
      manager = cls._managers.get(key)
      if manager is None:
        manager = cls(backend_type, normalized_settings)
        cls._managers[key] = manager
      manager._users += 1
      return manager

  @staticmethod
  def _registry_key(
    backend_type: str,
    settings: dict[str, Any],
  ) -> str:
    """Compute the registry cache key for a backend type + settings pair.

    Round-5 R5-1: ``backend_type`` is the registry-key string. When a
    ``BackendType`` enum member is passed (a ``str`` subclass), its ``str()``
    is the repr-like ``"BackendType.REDIS"`` — NOT the registry key — so we
    extract ``.value`` explicitly. Plain strings pass through unchanged.
    Keys stay byte-identical to the pre-refactor ``f"{bt.value}:..."`` form.
    """
    bt_key = (
      backend_type.value
      if isinstance(backend_type, BackendType)
      else backend_type
    )
    try:
      settings_key = json.dumps(
        settings,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
      )
    except (TypeError, ValueError):
      settings_key = str(sorted(settings.items()))
    return f"{bt_key}:{settings_key}"

  def _create_backend(self) -> Backend:
    """Create a backend instance based on type.

    Dispatches via the registry's :class:`BackendDescriptor` table to keep
    this method's cyclomatic complexity flat regardless of how many backends
    exist. The descriptor's class/settings paths are resolved lazily (via
    ``importlib.import_module`` + ``getattr``), preserving the original
    per-arm lazy-import semantics so optional backend dependencies stay
    loaded-on-demand and tests that patch the canonical module attribute
    still intercept construction.

    Returns:
        A new backend instance.

    Raises:
        ConfigurationError: If the backend type is not registered.
    """
    descriptor: BackendDescriptor = get_descriptor(
      self.backend_type.value
      if isinstance(self.backend_type, BackendType)
      else self.backend_type
    )
    backend_cls = _load_object(descriptor.backend_cls_path)
    settings_cls = _load_object(descriptor.settings_cls_path)
    # Both loaded objects are dynamically-discovered plugin classes (typed as
    # ``Any``); cast narrows to the concrete ``Backend`` instance we construct.
    return cast("Backend", backend_cls(settings_cls(**self.settings)))

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
        logger.debug("Connected to %s", self.backend_type)
        return

    if last_exception is not None:
      msg = f"Failed to connect after {retry_attempts} attempts: {last_exception}"
      raise BackendConnectionError(
        msg,
        backend_type=str(self.backend_type),
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
    """Release this holder's acquire on the shared manager (refcount).

    Pairs with ``get_manager()`` (acquire). Decrements ``_users`` under the
    registry lock; only when the count drops to zero (the LAST holder) does
    this method actually disconnect the backend and evict the registry
    entry. Earlier holders' closes are no-ops on the backend — so
    co-located components (e.g. scheduler queue + dupefilter sharing one
    Redis) don't tear each other's connection down during shutdown.

    Disconnect-path error handling mirrors R25-A1's connect-path cleanup
    (broad ``Exception`` catch): disconnecting a possibly-broken backend
    can raise anything (OSError from the socket layer, a backend-specific
    error). ``close()`` must still complete the registry eviction so the
    next ``get_manager()`` creates a fresh manager — never propagate out of
    the close chain.
    """
    cls = type(self)
    key = cls._registry_key(self.backend_type, self.settings)
    with cls._registry_lock:
      # A ``close()`` without a matching ``get_manager()`` (e.g. a bare
      # ``ConnectionManager(...)`` constructed in tests) has _users == 0;
      # clamp at zero and fall through to the teardown path so such an
      # instance still disconnects its backend.
      if self._users > 0:
        self._users -= 1
      is_last_holder = self._users <= 0
      if is_last_holder:
        cls._managers.pop(key, None)

    if not is_last_holder:
      return

    with self._lock:
      if self._backend:
        try:
          self._backend.disconnect()
          logger.debug("Disconnected from %s", self.backend_type)
        except Exception as e:
          # Broad catch — mirrors R25-A1's connect-path cleanup
          # (contextlib.suppress(Exception)). Disconnecting a possibly-broken
          # backend can raise anything: an OSError from the socket layer that
          # the backend's own disconnect didn't self-suppress, or a
          # backend-specific error. close() must still complete registry
          # eviction (above) — never propagate out of the close chain.
          logger.warning("Error during disconnect: %s", e)
        finally:
          self._backend = None

  @classmethod
  def clear_registry(cls) -> None:
    """Close and clear all registered managers (force-teardown).

    Intended for test isolation: the class-level ``_managers`` dict
    otherwise accumulates entries across test runs, causing both a
    slow memory leak and cross-test pollution (one test's manager is
    returned for another test's get_manager call). Bypasses the refcount
    (each registered manager's backend is disconnected unconditionally)
    so a full teardown is possible even if some holders skipped their
    paired ``close()``.
    """
    with cls._registry_lock:
      managers = list(cls._managers.values())
      cls._managers.clear()
    for manager in managers:
      with manager._lock:
        if manager._backend:
          with contextlib.suppress(Exception):
            manager._backend.disconnect()
          manager._backend = None

  @property
  def backend(self) -> Backend:
    """Get the backend instance, connecting if necessary.

    A2 — fast path / slow path split with single-connect ownership:

    - Fast path: lock-free read of ``self._backend``. A non-None value is
      stable (only ever transitioned ``None``→backend under ``_lock``), so a
      lock-free read is safe for the already-connected case and avoids
      contending on ``_lock`` at all once warm.
    - Slow path: under ``_lock``, take ownership of connecting via the
      ``_connecting`` flag. Peers that find ``_connecting`` set wait on
      ``_connected_event`` (released by the owner once connect resolves) —
      they do NOT spin on ``_lock`` while the owner backs off.
    - The owner runs ``connect()`` (which performs ``time.sleep`` between
      retry attempts) WITHOUT holding ``_lock``. This is the load-bearing
      fix: a slow-connecting backend no longer blocks every peer thread
      sharing the manager.

    Single-connect invariant preserved: exactly one ``connect()`` fires on
    first access; all peers see the same connected backend.

    Returns:
        The backend instance.

    Raises:
        BackendConnectionError: If connection fails or ``connect()``
            violates its contract (returns without setting ``_backend``).
    """
    # Fast path: lock-free read.
    if self._backend is not None:
      return self._backend

    while True:
      with self._lock:
        # Re-check under lock: another thread may have connected while we
        # were waiting on _lock.
        if self._backend is not None:
          return self._backend
        if not self._connecting:
          # Take ownership of connecting.
          self._connecting = True
          self._connected_event.clear()
          break
        # Another thread owns the connect; wait OUTSIDE the lock below.
        wait_event = self._connected_event

      # Wait for the owner to resolve connect() — without holding _lock.
      wait_event.wait()

    # Owner path: connect WITHOUT holding _lock so the retry-loop
    # time.sleep backoff does not block peer threads (A2).
    connect_error: BaseException | None = None
    try:
      self.connect()
    except BaseException as e:  # noqa: BLE001 - re-signal to all waiters
      connect_error = e
    finally:
      with self._lock:
        self._connecting = False
        self._connected_event.set()

    if connect_error is not None:
      raise connect_error

    if self._backend is None:
      # Defensive: connect() should either set _backend (success) or raise.
      # If we land here, connect() returned without connecting and without
      # raising — that is a contract violation, not a user input problem.
      # The explicit guard (rather than ``assert``) keeps the check live
      # under ``python -O`` and produces a clear, typed error instead of a
      # bare AssertionError.
      msg = "connect() did not produce a backend"
      raise BackendConnectionError(msg, backend_type=str(self.backend_type))
    return self._backend

  def is_connected(self) -> bool:
    """Check if backend is connected.

    Returns:
        True if connected, False otherwise.
    """
    if self._backend is None:
      return False
    return self._backend.is_connected()

  def _get_breaker(self) -> CircuitBreaker | None:
    """Lazily resolve the per-manager circuit breaker from env settings.

    Reads the breaker config once (``SCRAPY_CIRCUIT_BREAKER_ENABLED`` +
    threshold + reset-timeout) and caches the result on the instance:

    - When disabled (the default), ``_breaker`` is set to ``None`` and the
      ``get_*_backend()`` methods return the raw backend unchanged —
      byte-identical to pre-breaker behavior, zero proxy overhead.
    - When enabled, a single :class:`CircuitBreaker` is constructed and
      shared by every wrapped interface returned from this manager, so a
      queue+set+storage on the same backend share one failure signal.

    The Settings object is constructed lazily inside the lock so import-time
    side effects (pydantic env scan) are deferred to first use — important
    because this module is imported eagerly via ``backends/__init__`` and the
    env may not be fully populated at import time.

    Returns:
        The manager's breaker, or ``None`` when the feature is disabled.
    """
    if self._breaker_configured:
      return self._breaker
    with self._lock:
      if self._breaker_configured:
        return self._breaker
      # Imported lazily to avoid a settings-module import cycle at module
      # load time and to keep the breaker config read deferred to first use.
      from scrapy_extension.settings import Settings

      settings = Settings()
      if settings.circuit_breaker_enabled:
        bt_key = (
          self.backend_type.value
          if isinstance(self.backend_type, BackendType)
          else self.backend_type
        )
        self._breaker = CircuitBreaker(
          name=f"{bt_key}-backend",
          failure_threshold=settings.circuit_breaker_failure_threshold,
          reset_timeout=settings.circuit_breaker_reset_timeout,
        )
      else:
        self._breaker = None
      self._breaker_configured = True
      return self._breaker

  def get_queue_backend(self) -> QueueBackend:
    """Get the queue backend interface.

    When the circuit breaker is enabled, the returned backend's hot-path
    ops (``push`` / ``pop`` / ``queue_len``) are wrapped under the breaker;
    non-network methods (``clear_queue``, ``ack``, ``nack``,
    ``is_connected``) forward unchanged. When disabled (default) the raw
    backend is returned byte-identically.

    Returns:
        The QueueBackend interface of the backend.
    """
    backend = self.backend
    if not isinstance(backend, QueueBackend):
      msg = f"Backend {backend.__class__.__name__} does not support queue operations"
      raise NotImplementedError(msg)
    breaker = self._get_breaker()
    if breaker is None:
      return backend
    from scrapy_extension.backends.circuit_breaker import wrap_queue_backend

    return wrap_queue_backend(backend, breaker)

  def get_set_backend(self) -> SetBackend:
    """Get the set backend interface.

    When the circuit breaker is enabled, the returned backend's hot-path
    ops (``add`` / ``contains`` / ``remove``) are wrapped under the breaker;
    non-network methods forward unchanged. When disabled (default) the raw
    backend is returned byte-identically.

    Returns:
        The SetBackend interface of the backend.
    """
    backend = self.backend
    if not isinstance(backend, SetBackend):
      msg = f"Backend {backend.__class__.__name__} does not support set operations"
      raise NotImplementedError(msg)
    breaker = self._get_breaker()
    if breaker is None:
      return backend
    from scrapy_extension.backends.circuit_breaker import wrap_set_backend

    return wrap_set_backend(backend, breaker)

  def get_storage_backend(self) -> StorageBackend:
    """Get the storage backend interface.

    When the circuit breaker is enabled, the returned backend's hot-path
    ops (``store`` / ``retrieve`` / ``delete``) are wrapped under the
    breaker; non-network methods (``exists``, ``ttl``, ``clear_storage``)
    forward unchanged. When disabled (default) the raw backend is returned
    byte-identically.

    Returns:
        The StorageBackend interface of the backend.
    """
    backend = self.backend
    if not isinstance(backend, StorageBackend):
      msg = f"Backend {backend.__class__.__name__} does not support storage operations"
      raise NotImplementedError(msg)
    breaker = self._get_breaker()
    if breaker is None:
      return backend
    from scrapy_extension.backends.circuit_breaker import wrap_storage_backend

    return wrap_storage_backend(backend, breaker)
