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
import hashlib
import importlib
import json
import logging
import math
import os
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime, timedelta
from datetime import time as datetime_time
from decimal import Decimal
from difflib import get_close_matches
from enum import Enum
from json import JSONEncoder
from pathlib import PurePath
from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar, cast
from uuid import UUID

from pydantic import BaseModel, SecretBytes, SecretStr, ValidationError

from scrapy_extension.backends._retry import compute_full_jitter_backoff
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
from scrapy_extension.exceptions import (
  BackendConnectionError,
  BackendError,
  ConfigurationError,
)
from scrapy_extension.monitor.base import Monitor, NullMonitor

if TYPE_CHECKING:
  from scrapy_extension.backends.base import Backend

logger = logging.getLogger(__name__)

_BUNDLED_BACKEND_TYPES: frozenset[str] = frozenset(
  backend_type.value for backend_type in BackendType
)
_CONNECTION_MANAGER_SETTING_NAMES: frozenset[str] = frozenset(
  {"retry_attempts", "retry_delay"}
)
_CONNECTION_MANAGER_INTERNAL_KEYS: dict[str, str] = {
  "retry_attempts": "__connection_manager_retry_attempts",
  "retry_delay": "__connection_manager_retry_delay",
}
_CONNECTION_MANAGER_DIRECT_KEYS: dict[str, str] = {
  "retry_attempts": "manager_retry_attempts",
  "retry_delay": "manager_retry_delay",
}
# Registry-only discriminator used by components whose backend owns mutable
# consumer state tied to one logical queue. It participates in ``_registry_key``
# but is stripped before constructing the backend's Pydantic settings model.
_CONNECTION_MANAGER_SCOPE_KEY = "__connection_manager_queue_scope"
_CONNECTION_MANAGER_BACKEND_EXCLUDED_KEYS: frozenset[str] = frozenset(
  {
    *_CONNECTION_MANAGER_INTERNAL_KEYS.values(),
    *_CONNECTION_MANAGER_DIRECT_KEYS.values(),
    _CONNECTION_MANAGER_SCOPE_KEY,
  }
)
_CONNECTION_MANAGER_SCRAPY_KEYS: dict[str, str] = {
  "retry_attempts": "SCRAPY_RETRY_ATTEMPTS",
  "retry_delay": "SCRAPY_RETRY_DELAY",
}
_CONNECTION_MANAGER_DEFAULTS: dict[str, int | float] = {
  "retry_attempts": 3,
  "retry_delay": 1.0,
}


def _model_field_names(settings_cls: Any) -> frozenset[str]:
  """Return declared Pydantic field names without assuming a model class."""
  fields = getattr(settings_cls, "model_fields", None)
  if not isinstance(fields, Mapping):
    return frozenset()
  return frozenset(name for name in fields if isinstance(name, str))


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
  Redis-Cluster while dedup fingerprints live in MongoDB. Backend-type
  precedence is Scrapy per-component, Scrapy global, environment
  per-component, environment global, then Redis. A per-component type source
  uses the matching per-component ``settings_key``; global/default sources use
  ``SCRAPY_BACKEND_SETTINGS``.

  Bundled backend fields may be supplied as flat Scrapy settings using the
  Pydantic model's environment prefix (for example ``SCRAPY_REDIS_HOST``).
  Explicit nested backend settings take precedence over those flat values.
  Plugin and non-Pydantic settings classes are left untouched.

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
  scrapy_component_type = settings.get(type_key)
  scrapy_global_type = settings.get("SCRAPY_BACKEND_TYPE")
  if scrapy_component_type:
    raw_backend_type = scrapy_component_type
    source_key = type_key
    nested_settings_key = settings_key
  elif scrapy_global_type:
    raw_backend_type = scrapy_global_type
    source_key = "SCRAPY_BACKEND_TYPE"
    nested_settings_key = "SCRAPY_BACKEND_SETTINGS"
  else:
    environment_component_type = os.environ.get(type_key)
    environment_global_type = os.environ.get("SCRAPY_BACKEND_TYPE")
    if environment_component_type:
      raw_backend_type = environment_component_type
      source_key = type_key
      nested_settings_key = settings_key
    else:
      raw_backend_type = environment_global_type or "redis"
      source_key = "SCRAPY_BACKEND_TYPE"
      nested_settings_key = "SCRAPY_BACKEND_SETTINGS"

  backend_type = _normalize_backend_type(raw_backend_type, source_key)
  backend_settings = _adapt_backend_settings(
    settings,
    backend_type,
    settings.getdict(nested_settings_key, {}),
  )

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


def _adapt_backend_settings(
  settings: Any,
  backend_type: str,
  nested_settings: Mapping[str, Any],
) -> dict[str, Any]:
  """Validate and merge flat/nested settings for a bundled backend model."""
  if backend_type not in _BUNDLED_BACKEND_TYPES:
    return _merge_connection_manager_settings(
      settings,
      {},
      nested_settings,
      frozenset(),
    )

  descriptor = get_descriptor(backend_type)
  settings_cls = _load_object(descriptor.settings_cls_path)
  if not isinstance(settings_cls, type) or not issubclass(settings_cls, BaseModel):
    return _merge_connection_manager_settings(
      settings,
      {},
      nested_settings,
      frozenset(),
    )

  env_prefix = settings_cls.model_config.get("env_prefix")
  if not isinstance(env_prefix, str) or not env_prefix:
    return _merge_connection_manager_settings(
      settings,
      {},
      nested_settings,
      frozenset(settings_cls.model_fields),
    )

  field_names = frozenset(settings_cls.model_fields)
  allowed_nested_names = field_names | _CONNECTION_MANAGER_SETTING_NAMES
  for setting_name in nested_settings:
    if not isinstance(setting_name, str) or setting_name not in allowed_nested_names:
      raise _unknown_backend_setting(
        str(setting_name),
        allowed_nested_names,
        backend_type,
      )

  flat_key_to_field = {
    f"{env_prefix}{field_name.upper()}".upper(): field_name
    for field_name in field_names
  }
  flat_settings: dict[str, Any] = {}
  if isinstance(settings, Mapping):
    for setting_name, value in settings.items():
      if not isinstance(setting_name, str):
        continue
      normalized_name = setting_name.upper()
      field_name = flat_key_to_field.get(normalized_name)
      if field_name is not None:
        flat_settings[field_name] = value
      elif normalized_name.startswith(env_prefix.upper()):
        raise _unknown_backend_setting(
          setting_name,
          frozenset(flat_key_to_field),
          backend_type,
        )
  else:
    missing = object()
    for setting_name, field_name in flat_key_to_field.items():
      value = settings.get(setting_name, missing)
      if value is not missing:
        flat_settings[field_name] = value

  for setting_name in os.environ:
    normalized_name = setting_name.upper()
    if normalized_name.startswith(env_prefix.upper()) and (
      normalized_name not in flat_key_to_field
    ):
      raise _unknown_backend_setting(
        setting_name,
        frozenset(flat_key_to_field),
        backend_type,
      )

  return _merge_connection_manager_settings(
    settings,
    flat_settings,
    nested_settings,
    field_names,
  )


def _merge_connection_manager_settings(
  settings: Any,
  backend_settings: Mapping[str, Any],
  nested_settings: Mapping[str, Any],
  backend_field_names: frozenset[str],
) -> dict[str, Any]:
  """Separate generic connection retries from backend model fields.

  ``retry_delay`` is also a RabbitMQ model field. Keeping the generic retry
  under the same key made one value drive both pika's inner connection loop
  and ConnectionManager's outer loop. Internal keys preserve the public
  nested setting names while letting each layer consume only its own value.
  """
  merged_backend_settings = dict(backend_settings)
  merged_nested_settings = dict(nested_settings)
  manager_settings: dict[str, Any] = {}

  for public_name, internal_name in _CONNECTION_MANAGER_INTERNAL_KEYS.items():
    scrapy_key = _CONNECTION_MANAGER_SCRAPY_KEYS[public_name]
    global_value = settings.get(scrapy_key)
    if global_value is None:
      global_value = os.environ.get(scrapy_key)
    if global_value is not None:
      manager_settings[internal_name] = global_value

    if public_name in merged_nested_settings:
      if public_name in backend_field_names:
        # This is a backend-specific field with a colliding name. Keep it for
        # the backend and ensure the outer manager uses its independent global
        # value (or the documented default).
        manager_settings.setdefault(
          internal_name,
          _CONNECTION_MANAGER_DEFAULTS[public_name],
        )
      else:
        manager_settings[internal_name] = merged_nested_settings.pop(public_name)

    if public_name in merged_backend_settings:
      manager_settings.setdefault(
        internal_name,
        _CONNECTION_MANAGER_DEFAULTS[public_name],
      )

  merged_backend_settings.update(merged_nested_settings)
  merged_backend_settings.update(manager_settings)
  return merged_backend_settings


def _unknown_backend_setting(
  setting_name: str,
  valid_names: frozenset[str],
  backend_type: str,
) -> ConfigurationError:
  """Build a value-free typo error with a best-effort setting suggestion."""
  suggestions = get_close_matches(setting_name, sorted(valid_names), n=1, cutoff=0.6)
  suggestion = f" Did you mean {suggestions[0]!r}?" if suggestions else ""
  return ConfigurationError(
    f"Unknown {backend_type!r} backend setting {setting_name!r}.{suggestion}",
    setting_name=setting_name,
  )


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


class _ConnectionAttempt:
  """Result shared by every caller waiting on one connection attempt."""

  def __init__(self) -> None:
    self.event = threading.Event()
    self.error: BaseException | None = None


def _registry_type_name(value: object) -> str:
  """Return a process-stable, module-qualified type name."""
  value_type = type(value)
  return f"{value_type.__module__}.{value_type.__qualname__}"


def _canonical_registry_json(value: Any) -> str:
  """Encode an already-normalized value without a lossy string fallback."""
  return JSONEncoder(
    ensure_ascii=True,
    allow_nan=False,
    sort_keys=True,
    separators=(",", ":"),
  ).encode(value)


def _normalize_registry_value(value: Any, active_ids: set[int]) -> Any:
  """Build a deterministic, type-tagged JSON value for registry hashing.

  ``active_ids`` tracks only the current recursion path. Repeated references
  outside that path are normalized by value, while actual cycles receive a
  deterministic type marker instead of an address-bearing ``repr``.
  """
  if isinstance(value, SecretStr):
    return ["secret-str", value.get_secret_value()]
  if isinstance(value, SecretBytes):
    return ["secret-bytes", value.get_secret_value().hex()]
  if isinstance(value, Enum):
    return [
      "enum",
      _registry_type_name(value),
      value.name,
      _normalize_registry_value(value.value, active_ids),
    ]
  if value is None:
    return ["none"]
  if isinstance(value, bool):
    return ["bool", value]
  if isinstance(value, int):
    return ["int", str(value)]
  if isinstance(value, float):
    return ["float", value.hex()]
  if isinstance(value, str):
    return ["str", value]
  if isinstance(value, bytes):
    return ["bytes", value.hex()]
  if isinstance(value, bytearray):
    return ["bytearray", bytes(value).hex()]
  if isinstance(value, memoryview):
    return ["memoryview", value.tobytes().hex()]
  if isinstance(value, datetime):
    return ["datetime", _registry_type_name(value), value.isoformat(), value.fold]
  if isinstance(value, date):
    return ["date", _registry_type_name(value), value.isoformat()]
  if isinstance(value, datetime_time):
    return ["time", _registry_type_name(value), value.isoformat(), value.fold]
  if isinstance(value, timedelta):
    return ["timedelta", value.days, value.seconds, value.microseconds]
  if isinstance(value, Decimal):
    return ["decimal", str(value)]
  if isinstance(value, UUID):
    return ["uuid", value.hex]
  if isinstance(value, PurePath):
    return ["path", _registry_type_name(value), str(value)]
  if isinstance(value, range):
    return ["range", value.start, value.stop, value.step]
  if isinstance(value, complex):
    return ["complex", value.real.hex(), value.imag.hex()]
  if isinstance(value, type):
    return ["class", value.__module__, value.__qualname__]
  if isinstance(value, ModuleType):
    return ["module", value.__name__]

  value_id = id(value)
  if value_id in active_ids:
    return ["cycle", _registry_type_name(value)]

  active_ids.add(value_id)
  try:
    if isinstance(value, Mapping):
      entries = [
        [
          _normalize_registry_value(key, active_ids),
          _normalize_registry_value(item, active_ids),
        ]
        for key, item in value.items()
      ]
      entries.sort(key=_canonical_registry_json)
      return ["mapping", _registry_type_name(value), entries]

    if isinstance(value, (list, tuple)):
      return [
        "sequence",
        _registry_type_name(value),
        [_normalize_registry_value(item, active_ids) for item in value],
      ]

    if isinstance(value, (set, frozenset)):
      items = [_normalize_registry_value(item, active_ids) for item in value]
      items.sort(key=_canonical_registry_json)
      return ["set", _registry_type_name(value), items]

    module_name = getattr(value, "__module__", None)
    qualified_name = getattr(value, "__qualname__", None)
    if (
      callable(value)
      and isinstance(module_name, str)
      and isinstance(qualified_name, str)
    ):
      return [
        "callable",
        _registry_type_name(value),
        module_name,
        qualified_name,
      ]

    state: list[Any] = []
    try:
      instance_dict = vars(value)
    except TypeError:
      instance_dict = None
    if instance_dict is not None:
      state.append(["dict", _normalize_registry_value(instance_dict, active_ids)])

    slot_state: list[Any] = []
    for owner in type(value).__mro__:
      declared_slots = owner.__dict__.get("__slots__", ())
      if isinstance(declared_slots, str):
        declared_slots = (declared_slots,)
      for slot in declared_slots:
        if slot in {"__dict__", "__weakref__"}:
          continue
        attribute_name = slot
        if slot.startswith("__") and not slot.endswith("__"):
          attribute_name = f"_{owner.__name__.lstrip('_')}{slot}"
        try:
          slot_value = getattr(value, attribute_name)
        except (AttributeError, TypeError, ValueError):
          continue
        slot_state.append(
          [
            f"{owner.__module__}.{owner.__qualname__}:{slot}",
            _normalize_registry_value(slot_value, active_ids),
          ]
        )
    if slot_state:
      slot_state.sort(key=_canonical_registry_json)
      state.append(["slots", slot_state])

    if state:
      return ["object", _registry_type_name(value), state]
    return ["opaque", _registry_type_name(value)]
  finally:
    active_ids.remove(value_id)


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

  Lock-order invariant (Risk 6 documentation): when a code path needs BOTH
  locks, acquire ``_registry_lock`` (class-level) BEFORE the instance ``_lock``.
  Reversing the order risks deadlock — ``get_manager`` takes ``_registry_lock``
  and then (via ``connect``) may take ``_lock``, while peer code holding
  ``_lock`` must never reach back for ``_registry_lock``. The A2 owner-gate
  runs ``connect`` + backoff OUTSIDE ``_lock`` so a slow backend cannot block
  peer threads sharing the manager. Do not narrow without verifying these
  two orderings hold.
  """

  # Class-level registry of managers. R14-E: this is an LRU-bounded
  # ``OrderedDict`` (``MAX_MANAGERS``) so settings churn — per-spider creds,
  # unique ``group_id``, rotating endpoints — cannot leak live ``Backend``
  # instances + their open sockets forever. On overflow the oldest
  # genuinely-orphaned entry (``_users <= 0``) is evicted and disconnected;
  # actively-used managers (``_users > 0``) are never evicted.
  _managers: ClassVar[OrderedDict[str, ConnectionManager]] = OrderedDict()
  _registry_lock: ClassVar[threading.Lock] = threading.Lock()
  # One-shot guard for the "registry over cap with all entries live" warning
  # so we don't spam logs on every get_manager() once the cap is saturated.
  _over_cap_warned: ClassVar[bool] = False
  #: Cap on the registry size. 32 is comfortably above any realistic
  #: single-process multi-backend coexistence (10 bundled backends x 3
  #: components) while bounding the worst-case leak from settings churn to
  #: ~32 live sockets. Exceeding this with ALL entries actively held
  #: (``_users > 0``) is a real leak elsewhere — we log a warning and
  #: stop evicting rather than tearing down a live manager.
  MAX_MANAGERS: ClassVar[int] = 32

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
    # Serialize the complete create/connect/publish transaction. The lazy
    # ``backend`` property already elects one owner among property callers, but
    # ``connect()`` is public and is called directly by spider lifecycle
    # signals. Without a separate lock, two direct callers can each create a
    # backend and the later publish overwrites (and leaks) the earlier one.
    # Keep this distinct from ``_lock`` so retry backoff and network I/O remain
    # outside the shared state lock.
    self._connect_lock = threading.Lock()
    # Terminal lifecycle marker. Once the final holder releases (or registry
    # teardown evicts this manager), a slow in-progress connect must not
    # publish a backend into the now-unowned instance.
    self._retired = False
    # Refcount of outstanding ``get_manager()`` acquire calls sharing this
    # instance (A1). The manager is only created via ``get_manager()``, so
    # the constructor sets the initial count to 0; ``get_manager()`` then
    # increments to 1 on first insertion. Each subsequent acquire bumps it;
    # each ``close()`` (release) decrements; only the last holder actually
    # disconnects + evicts the registry entry.
    self._users: int = 0
    # Single-connect ownership flag (A2). The first thread to enter the slow
    # path takes ownership under ``_lock``; peers capture the same attempt and
    # wait for its result. A distinct result object per attempt is necessary:
    # after a failure, a later caller may start a fresh attempt before older
    # peers are scheduled, but those peers must still receive the failure they
    # waited for instead of joining the new attempt or retrying serially.
    self._connecting: bool = False
    self._connected_event = threading.Event()
    self._connect_attempt: _ConnectionAttempt | None = None
    # Circuit-breaker holder. Lazily constructed on first
    # ``get_*_backend()`` call from the env-loaded ``Settings``
    # (``SCRAPY_CIRCUIT_BREAKER_ENABLED``). ``None`` while disabled — which
    # is the default, so the default path returns the raw backend with zero
    # overhead and byte-identical behavior.
    self._breaker: CircuitBreaker | None = None
    self._breaker_configured: bool = False
    # R14-D: observability monitor for connection-lifecycle hooks
    # (on_connect / on_disconnect / on_retry). Defaults to NullMonitor so the
    # hooks are no-ops unless a caller (scheduler / dupefilter factory) threads
    # a real monitor via :meth:`set_monitor`. Threading into the scheduler
    # factory is a follow-up (scheduler.py is out of R14-D scope); the hooks
    # + their stat keys (backend/connect_count, etc.) are wired here so the
    # observability contract is in place the moment a monitor is attached.
    self._monitor: Monitor = NullMonitor()

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
    # Hash and retain the same deep snapshot. Otherwise a caller can mutate a
    # nested value after hashing and make the old registry key point at new
    # connection settings.
    settings_snapshot = deepcopy(settings) if settings is not None else {}
    key = cls._registry_key(backend_type, settings_snapshot)

    victims: list[ConnectionManager] = []
    with cls._registry_lock:
      manager = cls._managers.get(key)
      if manager is None:
        # R14-E: before inserting a brand-new entry, evict from the FRONT
        # of the LRU until we're under the cap. Only genuinely-orphaned
        # entries (``_users <= 0``) are evicted — an actively-held manager
        # is skipped (it will be evicted later when its last holder
        # releases). If every entry is actively held we stop evicting and
        # log a one-shot warning rather than tear down a live connection
        # (a real leak, but not one ``get_manager`` should fix by force).
        #
        # Victims are collected (popped) UNDER the lock but disconnected
        # AFTER release (see the loop below) so a slow victim disconnect
        # does not serialize peer get_manager() calls.
        victims = cls._collect_orphans_under_lock()
        manager = cls(backend_type, settings_snapshot)
        cls._managers[key] = manager
      else:
        # LRU touch — recently-used entries move to the back (newest).
        cls._managers.move_to_end(key)
      manager._users += 1

    # Disconnect evicted victims OUTSIDE _registry_lock via the shared
    # teardown primitive. Victims are already popped (peers can't see them)
    # and have _users <= 0 (no outstanding acquire) — same invariant close()
    # relies on (L587-615). Disconnecting here rather than under the lock
    # avoids serializing every get_manager() on the slowest backend
    # disconnect during overflow.
    for victim in victims:
      cls._disconnect_backend_safely(victim)

    return manager

  @classmethod
  def _collect_orphans_under_lock(cls) -> list[ConnectionManager]:
    """Pop orphaned managers from the front of the LRU until under cap.

    R14-E evolution: victims are collected (popped) here under
    ``_registry_lock`` but RETURNED to the caller, which disconnects them
    AFTER releasing the registry lock (see the disconnect loop in
    :meth:`get_manager`). This mirrors :meth:`close`'s teardown pattern
    (pop under lock at L587-596, disconnect after release at L601-615) so a
    slow victim disconnect does not serialize peer ``get_manager()``
    calls — the load-bearing fix guarded by regression test
    ``test_evict_disconnects_victim_OUTSIDE_registry_lock``.

    Entries with ``_users > 0`` are NEVER collected — they're actively held
    and force-eviction would corrupt the holder's connection. If the cap
    can't be reached by collecting orphans alone, stop and warn once per
    process so operators know the registry is over budget with all entries
    live.

    Must be called UNDER ``_registry_lock`` — it mutates ``_managers`` and
    reads ``_users`` without per-instance locking.

    Returns:
        Victims the caller MUST disconnect outside the registry lock.
    """
    victims: list[ConnectionManager] = []
    while len(cls._managers) >= cls.MAX_MANAGERS:
      # Find the front-most orphan. Can't ``popitem(last=False)`` blindly
      # because the oldest entry may be actively held (``_users > 0``) and
      # force-eviction would corrupt its holder.
      orphan_key: str | None = None
      for candidate_key, candidate in cls._managers.items():
        if candidate._users <= 0:
          orphan_key = candidate_key
          break
      if orphan_key is None:
        # Every entry is actively held — registry is genuinely over budget.
        # Warn once per process; do not force-evict a live manager.
        if not cls._over_cap_warned:
          cls._over_cap_warned = True
          logger.warning(
            "ConnectionManager registry at cap (%d) with all entries "
            "actively held; not force-evicting live managers. This "
            "indicates genuine unbounded backend coexistence — investigate "
            "the source of distinct backend settings.",
            cls.MAX_MANAGERS,
          )
        return victims
      victims.append(cls._managers.pop(orphan_key))
    return victims

  @staticmethod
  def _disconnect_backend_safely(manager: ConnectionManager) -> None:
    """Disconnect ``manager._backend`` under its lock, suppressing errors.

    Shared teardown primitive for evicted victims (:meth:`get_manager`) and
    force-teardown (:meth:`clear_registry`). :meth:`close` does NOT use this
    — it logs disconnect errors and emits ``on_disconnect`` / breaker-reset
    hooks that ``suppress()`` would skip, so its teardown stays inline.
    """
    with manager._lock:
      manager._retired = True
      if manager._backend is not None:
        with contextlib.suppress(Exception):
          manager._backend.disconnect()
        manager._backend = None

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

    Settings are recursively normalized into a type-tagged JSON structure,
    then the complete structure is reduced to a SHA-256 digest. Pydantic
    ``SecretStr`` / ``SecretBytes`` values contribute their underlying secret,
    so distinct credentials never share a manager, while neither those values
    nor plain-string credentials remain in the class registry key. The
    normalization avoids address-bearing or secret-bearing ``repr`` fallbacks
    and is deterministic across equivalent settings objects.
    """
    bt_key = (
      backend_type.value
      if isinstance(backend_type, BackendType)
      else backend_type
    )

    normalized_settings = [
      "connection-manager-registry-v1",
      _normalize_registry_value(settings, set()),
    ]
    try:
      settings_key = json.dumps(
        normalized_settings,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
      )
    except (TypeError, ValueError):
      # ``normalized_settings`` contains JSON-native values only. This branch
      # keeps key generation available if the module-level JSON facade is
      # replaced/fails, without falling back to a plaintext ``repr``.
      settings_key = _canonical_registry_json(normalized_settings)
    settings_digest = hashlib.sha256(settings_key.encode("utf-8")).hexdigest()
    return f"{bt_key}:{settings_digest}"

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
    backend_field_names = _model_field_names(settings_cls)
    manager_only_names = _CONNECTION_MANAGER_SETTING_NAMES - backend_field_names
    backend_settings = {
      name: value
      for name, value in self.settings.items()
      if name not in _CONNECTION_MANAGER_BACKEND_EXCLUDED_KEYS
      and name not in manager_only_names
    }
    # Both loaded objects are dynamically-discovered plugin classes (typed as
    # ``Any``); cast narrows to the concrete ``Backend`` instance we construct.
    return cast("Backend", backend_cls(settings_cls(**backend_settings)))

  def connect(self) -> None:
    """Establish connection with retry logic.

    Makes one initial connection attempt, then up to ``retry_attempts`` retries
    with exponential backoff based on ``retry_delay``. Concurrent direct calls
    share the resulting backend: the complete retry transaction is serialized,
    and each waiter re-checks the connected fast path after acquiring the
    connection lock.

    Raises:
        BackendConnectionError: If all network retry attempts fail.
        ConfigurationError: If generic retry controls are invalid.
        ValidationError: If backend-specific Pydantic settings are invalid.
        ImportError: If the selected backend's optional dependency is missing.
    """
    with self._connect_lock:
      self._connect_with_retries()

  def _connect_with_retries(self) -> None:
    """Run one serialized connection transaction for :meth:`connect`."""
    stale_backend: Backend | None = None
    while True:
      with self._lock:
        if self._retired:
          raise BackendConnectionError(
            "Cannot connect a released ConnectionManager",
            backend_type=str(self.backend_type),
          )
        backend = self._backend
      if backend is None:
        break

      # A published object can outlive its network connection. Run the health
      # probe outside ``_lock`` because Redis/MongoDB/ElasticSearch probes may
      # perform network I/O; holding the shared state lock here would block
      # close() and peer access for the entire timeout window.
      try:
        connected = backend.is_connected()
      except Exception:
        connected = False
        logger.debug(
          "Backend health check failed before reconnect",
          exc_info=True,
        )

      with self._lock:
        if self._retired:
          raise BackendConnectionError(
            "Cannot connect a released ConnectionManager",
            backend_type=str(self.backend_type),
          )
        # Re-check identity after the unlocked health probe. A lifecycle race
        # may have detached the inspected backend; retry against current state
        # instead of publishing a decision about an obsolete object.
        if self._backend is not backend:
          continue
        if connected:
          return
        self._backend = None
        # Backend and breaker form one connection generation. Replace the
        # breaker while holding the same state lock that detaches the backend
        # so interface accessors can validate a coherent pair. Performing this
        # later, after disconnect(), exposes ``None/old-breaker`` and then
        # ``replacement/old-breaker`` windows to racing accessors.
        if self._breaker is not None:
          self._breaker = self._breaker.new_generation()
        stale_backend = backend
        break

    if stale_backend is not None:
      try:
        stale_backend.disconnect()
      except Exception as exc:
        logger.warning("Error disconnecting stale backend: %s", exc)
      self._notify_monitor("on_disconnect", str(self.backend_type), None)

    retry_attempts, retry_delay = self._retry_policy()
    total_attempts = retry_attempts + 1

    last_exception: Exception | None = None
    for attempt in range(total_attempts):
      try:
        self._attempt_connection()
      except (ConfigurationError, ValidationError, ImportError):
        # Invalid settings and missing optional dependencies cannot recover via
        # network backoff. Preserve their actionable exception and avoid
        # constructing/sleeping through the remaining retry attempts.
        raise
      except Exception as e:
        # Intentional broad catch: any backend connection error should trigger retry.
        # Backend-specific exceptions (RedisError, PyMongoError, KafkaError, AMQPError)
        # all inherit from Exception. KeyboardInterrupt/SystemExit inherit from
        # BaseException (not Exception), so ``except Exception`` does NOT catch them
        # — they propagate out of the retry loop naturally. (A prior ``isinstance(e,
        # (KeyboardInterrupt, SystemExit)): raise`` here was unreachable dead code:
        # nothing caught by ``except Exception`` can be an instance of either.)
        last_exception = e
        logger.warning(
          "Connection attempt %d/%d failed: %s", attempt + 1, total_attempts, e
        )
        with self._lock:
          retired = self._retired
        if retired:
          break
        if attempt < retry_attempts:
          # R14-D: emit on_retry before each exponential-backoff sleep so a
          # flapping backend surfaces as ``backend/retry_count``. ``attempt``
          # here is the 0-based just-failed index; the retry is 1-based
          # (attempt+1 = first retry = second overall attempt). No-op on the
          # default NullMonitor.
          self._notify_monitor("on_retry", str(self.backend_type), attempt + 1)
          time.sleep(compute_full_jitter_backoff(attempt, retry_delay))
      else:
        logger.debug("Connected to %s", self.backend_type)
        # R14-D: emit on_connect on the success path so ``backend/connect_count``
        # reflects successful connections. No-op on the default NullMonitor.
        self._notify_monitor("on_connect", str(self.backend_type))
        return

    if last_exception is not None:
      attempt_word = "attempt" if total_attempts == 1 else "attempts"
      msg = (
        f"Failed to connect after {total_attempts} {attempt_word}: {last_exception}"
      )
      raise BackendConnectionError(
        msg,
        backend_type=str(self.backend_type),
      ) from last_exception

  def _retry_policy(self) -> tuple[int, float]:
    """Normalize and validate generic connection retry controls.

    ConnectionManager consumes these values before the backend-specific
    Pydantic model is constructed, so relying on that later model would allow
    malformed strings to crash arithmetic and huge raw integers to drive an
    unbounded retry loop. The bounds mirror ``settings.Settings``.

    Returns:
        ``(retry_attempts, retry_delay_seconds)``.

    Raises:
        ConfigurationError: If either raw setting is invalid.
    """
    descriptor = get_descriptor(
      self.backend_type.value
      if isinstance(self.backend_type, BackendType)
      else self.backend_type
    )
    settings_cls = _load_object(descriptor.settings_cls_path)
    backend_field_names = _model_field_names(settings_cls)

    raw_attempts = self.settings.get(
      _CONNECTION_MANAGER_INTERNAL_KEYS["retry_attempts"],
      self.settings.get(
        _CONNECTION_MANAGER_DIRECT_KEYS["retry_attempts"],
        (
          _CONNECTION_MANAGER_DEFAULTS["retry_attempts"]
          if "retry_attempts" in backend_field_names
          else self.settings.get(
            "retry_attempts", _CONNECTION_MANAGER_DEFAULTS["retry_attempts"]
          )
        ),
      ),
    )
    try:
      if isinstance(raw_attempts, bool):
        raise ValueError
      retry_attempts = int(raw_attempts)
      if isinstance(raw_attempts, float) and not raw_attempts.is_integer():
        raise ValueError
    except (TypeError, ValueError, OverflowError) as e:
      raise ConfigurationError(
        "retry_attempts must be an integer between 0 and 20",
        setting_name="retry_attempts",
        setting_value=raw_attempts,
      ) from e
    if not 0 <= retry_attempts <= 20:
      raise ConfigurationError(
        "retry_attempts must be between 0 and 20",
        setting_name="retry_attempts",
        setting_value=raw_attempts,
      )

    raw_delay = self.settings.get(
      _CONNECTION_MANAGER_INTERNAL_KEYS["retry_delay"],
      self.settings.get(
        _CONNECTION_MANAGER_DIRECT_KEYS["retry_delay"],
        (
          _CONNECTION_MANAGER_DEFAULTS["retry_delay"]
          if "retry_delay" in backend_field_names
          else self.settings.get(
            "retry_delay", _CONNECTION_MANAGER_DEFAULTS["retry_delay"]
          )
        ),
      ),
    )
    try:
      if isinstance(raw_delay, bool):
        raise ValueError
      retry_delay = float(raw_delay)
    except (TypeError, ValueError, OverflowError) as e:
      raise ConfigurationError(
        "retry_delay must be a finite non-negative number",
        setting_name="retry_delay",
        setting_value=raw_delay,
      ) from e
    if not math.isfinite(retry_delay) or retry_delay < 0:
      raise ConfigurationError(
        "retry_delay must be a finite non-negative number",
        setting_name="retry_delay",
        setting_value=raw_delay,
      )
    return retry_attempts, retry_delay

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
    with self._lock:
      if self._retired:
        raise BackendConnectionError(
          "Cannot connect a released ConnectionManager",
          backend_type=str(self.backend_type),
        )
    backend = self._create_backend()
    try:
      backend.connect()
    except Exception:
      with contextlib.suppress(Exception):
        backend.disconnect()
      raise
    with self._lock:
      if not self._retired:
        self._backend = backend
        return

    # The final holder released while backend.connect() was in flight. Dispose
    # the successful handle instead of resurrecting an evicted manager.
    with contextlib.suppress(Exception):
      backend.disconnect()
    raise BackendConnectionError(
      "Connection completed after ConnectionManager release; backend discarded",
      backend_type=str(self.backend_type),
    )

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
        # Evict by IDENTITY, not key — a bare ``ConnectionManager(...)`` (not
        # inserted via get_manager) sharing this key must not evict a registered
        # peer. A plain pop(key, None) would silently remove the peer while it's
        # still held, so the next get_manager(same key) creates a second live
        # manager (split-brain / connection leak). See
        # test_close_bare_instance_does_not_evict_registered_peer.
        if cls._managers.get(key) is self:
          cls._managers.pop(key, None)

    if not is_last_holder:
      return

    with self._lock:
      # Make the final release terminal before inspecting the backend. A
      # connection attempt runs backend.connect() outside this lock; when it
      # later tries to publish the successful handle, _attempt_connection()
      # observes this marker and disposes that handle instead. Without this
      # assignment, an in-flight connect can resurrect an evicted manager and
      # leak an unowned connection.
      self._retired = True
      if self._backend is not None:
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
        # R14-D: emit on_disconnect so ``backend/disconnect_count`` reflects
        # teardowns. ``reason`` is not available at this layer (the Scrapy
        # engine close reason lives in the scheduler/pipeline that owns the
        # manager), so ``None`` is passed — the lifecycle hook fires
        # regardless. No-op on the default NullMonitor. Inside the non-null
        # backend block so a no-op close (already disconnected) does not
        # double-count. Truthiness is deliberately irrelevant: third-party
        # backends may validly define ``__bool__`` or ``__len__``.
        self._notify_monitor("on_disconnect", str(self.backend_type), None)
      # R14-E: reset the circuit breaker so a manager that reconnects after
      # teardown (or an orphan-evicted manager re-created from the same
      # settings) does not inherit a stale OPEN state from the prior
      # incarnation's failure run. ``reset()`` is a no-op when the breaker
      # was never constructed (disabled).
      if self._breaker is not None:
        self._breaker.reset()

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
      # Reset the one-shot over-cap warning so a fresh test suite run
      # re-warns if it overflows the cap (otherwise the warning is
      # permanently suppressed after the first overflow across tests).
      cls._over_cap_warned = False
    for manager in managers:
      cls._disconnect_backend_safely(manager)
      # Reset any breaker state too (mirrors close()).
      with manager._lock:
        if manager._breaker is not None:
          manager._breaker.reset()

  def set_monitor(self, monitor: Monitor) -> None:
    """Attach an observability monitor for connection-lifecycle hooks (R14-D).

    Wired hooks: ``on_connect`` (connect success), ``on_disconnect`` (last
    holder releases), ``on_retry`` (before each exponential-backoff sleep).
    Idempotent — calling it again replaces the prior monitor. The default
    (:class:`~scrapy_extension.monitor.base.NullMonitor`) makes every hook a
    no-op until a real monitor is attached.

    Intended for use by the scheduler / dupefilter factories that construct
    a ``ConnectionManager`` and want connection-lifecycle stats. The bundled
    factories resolve their own monitor for queue/dupefilter use; threading
    it into the manager is a follow-up (scheduler.py is out of R14-D scope).

    Args:
        monitor: The monitor to emit connection-lifecycle hooks through.
    """
    self._monitor = monitor

  def _notify_monitor(self, hook_name: str, *args: Any) -> None:
    """Emit one lifecycle hook without letting telemetry alter control flow."""
    try:
      getattr(self._monitor, hook_name)(*args)
    except Exception:
      logger.debug("Monitor.%s raised; ignored", hook_name, exc_info=True)

  @property
  def backend(self) -> Backend:
    """Get the backend instance, connecting if necessary.

    A2 — fast path / slow path split with single-connect ownership:

    - Fast path: lock-free reads of the terminal marker and ``self._backend``.
      The terminal marker is checked on both sides of the backend read so a
      released manager is never deliberately handed out as reusable.
    - Slow path: under ``_lock``, take ownership of connecting via the
      ``_connecting`` flag. Peers that find ``_connecting`` set capture that
      attempt and wait on its event (released by the owner once connect
      resolves). They do NOT spin on ``_lock`` while the owner backs off. A
      failed attempt is fanned out to its waiter cohort; only a later,
      independent call starts a new attempt.
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
    # Fast path: lock-free read. The second terminal-state check closes the
    # common ordering where close() retires the manager between the first
    # check and the backend read. The slow path remains the synchronization
    # boundary for first-connect and close-during-connect races.
    if not self._retired:
      backend = self._backend
      if backend is not None and not self._retired:
        return backend

    attempt: _ConnectionAttempt
    while True:
      with self._lock:
        if self._retired:
          raise BackendConnectionError(
            "Cannot access a released ConnectionManager",
            backend_type=str(self.backend_type),
          )
        # Re-check under lock: another thread may have connected while we
        # were waiting on _lock.
        if self._backend is not None:
          return self._backend
        if not self._connecting:
          # Take ownership of connecting.
          attempt = _ConnectionAttempt()
          self._connecting = True
          self._connect_attempt = attempt
          # Keep this alias for diagnostics and backward-compatible tests.
          self._connected_event = attempt.event
          break
        # Another thread owns the connect; wait OUTSIDE the lock below.
        current_attempt = self._connect_attempt
        if current_attempt is None:  # Defensive: _connecting implies an attempt.
          continue
        attempt = current_attempt

      # Wait for the owner to resolve connect() — without holding _lock.
      attempt.event.wait()
      if attempt.error is not None:
        raise attempt.error

    # Owner path: connect WITHOUT holding _lock so the retry-loop
    # time.sleep backoff does not block peer threads (A2).
    connect_error: BaseException | None = None
    try:
      self.connect()
    except BaseException as e:  # noqa: BLE001 - re-signal to all waiters
      connect_error = e
    published_backend: Backend | None = None
    with self._lock:
      # A final-holder close can win after connect() publishes a backend but
      # before this owner fans the result out. close() has already detached
      # and disconnected that backend, so convert the apparent success into
      # a terminal error before waking peers.
      if connect_error is None and self._retired:
        connect_error = BackendConnectionError(
          "ConnectionManager was released while connecting",
          backend_type=str(self.backend_type),
        )
      if connect_error is None:
        # Capture the published handle exactly once under the same state lock
        # used by reconnect/close. Returning a second ``self._backend`` read
        # allowed reconnect to detach it after a non-null guard, leaking None
        # through this property and into interface-proxy construction.
        published_backend = self._backend
        if published_backend is None:
          connect_error = BackendConnectionError(
            "connect() did not produce a backend",
            backend_type=str(self.backend_type),
          )
      attempt.error = connect_error
      self._connecting = False
      attempt.event.set()

    if connect_error is not None:
      raise connect_error

    # Keep the local-value guard explicit so the contract remains true under
    # future refactors and ``python -O`` as well as in the static type system.
    if published_backend is None:
      msg = "connect() did not produce a backend"
      raise BackendConnectionError(msg, backend_type=str(self.backend_type))
    return published_backend

  def is_connected(self) -> bool:
    """Check if backend is connected.

    Returns:
        True if connected, False otherwise.
    """
    backend = self._backend
    if backend is None:
      return False
    return backend.is_connected()

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

    The Settings object is constructed lazily (deferred to first use so the
    pydantic env scan does not run at import time — important because this
    module is imported eagerly via ``backends/__init__`` and the env may not
    be fully populated at import time). The config read runs OUTSIDE
    ``self._lock`` (initiative #15): the env scan is process-global
    idempotent state, not connection-manager state, and this lock is shared
    with ``get_manager()`` / ``close()`` / the A2 slow-path owner gate —
    holding it across the scan serialized peer threads' warm-up. A lost race
    constructs a second transient ``Settings()`` (GC'd); correct, and cheaper
    than blocking every peer on the lock.

    Returns:
        The manager's breaker, or ``None`` when the feature is disabled.
    """
    if self._breaker_configured:
      return self._breaker
    # Read the breaker config OUTSIDE self._lock (#15). Imported lazily to
    # avoid a settings-module import cycle at module load time and to keep
    # the breaker config read deferred to first use.
    from scrapy_extension.settings import Settings

    settings = Settings()
    enabled = settings.circuit_breaker_enabled
    failure_threshold = settings.circuit_breaker_failure_threshold
    reset_timeout = settings.circuit_breaker_reset_timeout
    with self._lock:
      if self._breaker_configured:
        return self._breaker
      bt_key = (
        self.backend_type.value
        if isinstance(self.backend_type, BackendType)
        else self.backend_type
      )
      if enabled:
        self._breaker = CircuitBreaker(
          name=f"{bt_key}-backend",
          failure_threshold=failure_threshold,
          reset_timeout=reset_timeout,
          failure_exceptions=(BackendError,),
        )
      else:
        self._breaker = None
      self._breaker_configured = True
      return self._breaker

  def _get_backend_breaker_snapshot(
    self,
  ) -> tuple[Backend, CircuitBreaker | None]:
    """Return a coherent backend/circuit-breaker generation snapshot.

    Accessors cannot simply read :attr:`backend` and then ``_breaker``: a
    reconnect may replace both between those reads, producing a proxy that
    binds a retired backend to the live generation's breaker. Read both
    outside ``_lock`` (``backend`` may connect and ``_get_breaker`` may load
    settings), then validate their identities together under ``_lock``. A
    concurrent generation change makes the loop retry; the result is always
    either the complete old generation or the complete replacement.

    Returns:
        The backend and breaker belonging to one connection generation.

    Raises:
        BackendConnectionError: If the manager is released while taking the
            snapshot.
    """
    while True:
      backend = self.backend
      if backend is None:
        # Defense in depth for subclasses/test doubles that violate the
        # property contract. Never accept ``None is self._backend`` as a
        # coherent generation and build a misleading NoneType proxy error.
        raise BackendConnectionError(
          "connect() did not produce a backend",
          backend_type=str(self.backend_type),
        )
      breaker = self._get_breaker()
      with self._lock:
        if self._retired:
          raise BackendConnectionError(
            "Cannot access a released ConnectionManager",
            backend_type=str(self.backend_type),
          )
        if backend is self._backend and breaker is self._breaker:
          return backend, breaker

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
    backend, breaker = self._get_backend_breaker_snapshot()
    if not isinstance(backend, QueueBackend):
      msg = f"Backend {backend.__class__.__name__} does not support queue operations"
      raise NotImplementedError(msg)
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
    backend, breaker = self._get_backend_breaker_snapshot()
    if not isinstance(backend, SetBackend):
      msg = f"Backend {backend.__class__.__name__} does not support set operations"
      raise NotImplementedError(msg)
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
    backend, breaker = self._get_backend_breaker_snapshot()
    if not isinstance(backend, StorageBackend):
      msg = f"Backend {backend.__class__.__name__} does not support storage operations"
      raise NotImplementedError(msg)
    if breaker is None:
      return backend
    from scrapy_extension.backends.circuit_breaker import wrap_storage_backend

    return wrap_storage_backend(backend, breaker)
