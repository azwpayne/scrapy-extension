"""ConnectionManager: lazy singleton with retry, lease lifecycle, and breaker policy."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import date, datetime, timedelta
from datetime import time as datetime_time
from decimal import Decimal
from enum import Enum
from functools import wraps
from json import JSONEncoder
from pathlib import PurePath
from types import ModuleType
from typing import Any, ClassVar, cast
from uuid import UUID

from pydantic import SecretBytes, SecretStr, ValidationError

from scrapy_extension.backends._retry import compute_full_jitter_backoff
from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
    _DurablePushRequired,
    _QueuePushReceipt,
)
from scrapy_extension.backends.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
)
from scrapy_extension.backends.connectors._config import (
    _parse_circuit_breaker_policy,
    _safe_manager_setting_name,
    resolve_circuit_breaker_policy,
)
from scrapy_extension.backends.connectors._constants import (
    _BUNDLED_BACKEND_TYPES,
    _CONNECTION_MANAGER_BACKEND_EXCLUDED_KEYS,
    _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS,
    _CONNECTION_MANAGER_DEFAULTS,
    _CONNECTION_MANAGER_DIRECT_KEYS,
    _CONNECTION_MANAGER_INTERNAL_KEYS,
    _CONNECTION_MANAGER_SETTING_NAMES,
    _MANAGER_CONFIGURATION_SETTING_NAMES,
    _SAFE_MANAGER_CONNECTION_MESSAGES,
)
from scrapy_extension.backends.connectors._diagnostics import (
    _P,
    _T,
    _log_diagnostic,
    _wait_for_retry_backoff,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _DeferredAckPluginQueueBackend,
    _invalid_plugin_ack_contract,
    _is_safe_manager_configuration_message,
    _load_descriptor_object,
    _load_static_ack_capabilities,
    _model_field_names,
    _PluginAckCapabilitySnapshot,
    _validate_backend_contract,
    _validate_plugin_ack_class,
)
from scrapy_extension.backends.registry import (
    BackendDescriptor,
    get_descriptor,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    BackendError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.exceptions._redaction import configuration_error_boundary
from scrapy_extension.monitor.base import Monitor, NullMonitor
from scrapy_extension.utils.reactor import (
    MAX_REACTOR_IO_TIMEOUT_S,
)

# Historical logger name preserved on purpose: tests patch/caplog
# "scrapy_extension.backends.connectors.logger.*" and pin this name;
# the package split must not fork the logger object.
logger = logging.getLogger("scrapy_extension.backends.connectors")


_MonitorEvent = tuple[str, tuple[Any, ...]]


class _ConnectionAttempt:
    """Result shared by every caller waiting on one connection attempt."""

    def __init__(self) -> None:
        self.event = threading.Event()
        self.error: BaseException | None = None


class _ManagerConstructionAttempt:
    """Owner gate for one pooled-manager construction generation."""

    def __init__(self, owner: int, epoch: int) -> None:
        self.owner = owner
        self.epoch = epoch
        self.event = threading.Event()
        self.waiters = 0


class _LazyConnectionContext(threading.local):
    """Per-thread lazy-owner state used while lifecycle hooks are dispatched.

    ``ConnectionManager.connect()`` remains a public, independently callable
    method.  A thread-local marker distinguishes the lazy ``backend`` owner
    from a concurrent direct caller, and lets a lifecycle callback read the
    result that its owner has already published without starting a recursive
    connection attempt.
    """

    def __init__(self) -> None:
        self.owner_attempt: _ConnectionAttempt | None = None
        self.dispatch_attempt: _ConnectionAttempt | None = None


def _normalized_manager_backend_type(value: object) -> str | None:
    """Accept only a bundled enum member or an exact backend registry key."""
    if isinstance(value, BackendType):
        return value.value
    if type(value) is str:
        return value
    return None


def _safe_manager_connection_message(error: BackendConnectionError) -> str:
    """Keep only deterministic manager startup messages after redaction."""
    fallback = "Connection manager failed to connect to the selected backend."
    if type(error) is not BackendConnectionError:
        return fallback
    raw_args = error.args
    if type(raw_args) is not tuple or len(raw_args) != 1:
        return fallback
    message = raw_args[0]
    if type(message) is not str:
        return fallback
    if message in _SAFE_MANAGER_CONNECTION_MESSAGES:
        return message
    prefix = "Failed to connect after "
    if not message.startswith(prefix):
        return fallback
    rendered_count, separator, suffix = message[len(prefix) :].partition(" ")
    if separator != " " or not rendered_count.isascii() or not rendered_count.isdigit():
        return fallback
    if len(rendered_count) > 2:
        return fallback
    count = int(rendered_count)
    if not 1 <= count <= 21:
        return fallback
    expected_suffix = "attempt." if count == 1 else "attempts."
    return message if suffix == expected_suffix else fallback


def _safe_manager_connection_backend_type(error: BackendConnectionError) -> str:
    """Keep an exact bundled backend label, otherwise use a static label."""
    if type(error) is BackendConnectionError:
        backend_type = error.backend_type
        if type(backend_type) is str and backend_type in _BUNDLED_BACKEND_TYPES:
            return backend_type
        if type(backend_type) is str:
            for bundled_type in BackendType:
                if backend_type == str(bundled_type):
                    return bundled_type.value
    return "connection-manager"


def _manager_terminal_error_boundary(
    unsupported_capability: str | None = None,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
    """Replace startup errors after all manager frames have unwound.

    ``ConnectionManager`` stores the selected backend configuration on ``self``.
    Re-raising even a previously-redacted error from a public accessor restores
    a traceback frame that exposes that configuration to introspection.  This
    outer boundary therefore rebuilds operational startup errors only after
    the accessor's inner frames have been removed.  Its companion configuration
    boundary handles validation errors and drops its own arguments before this
    wrapper observes them.
    """

    def decorate(function: Callable[_P, _T]) -> Callable[_P, _T]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            connection_error: BackendConnectionError | None = None
            import_failed = False
            unsupported = False
            try:
                return function(*args, **kwargs)
            except BackendConnectionError as error:
                connection_error = error
            except ImportError:
                import_failed = True
            except NotImplementedError:
                unsupported = True
            except BaseException:
                del args
                del kwargs
                raise
            if connection_error is not None:
                message = _safe_manager_connection_message(connection_error)
                backend_type = _safe_manager_connection_backend_type(connection_error)
                sanitized_error = BackendConnectionError(
                    message, backend_type=backend_type
                )
                del args
                del kwargs
                del connection_error
                del import_failed
                del unsupported
                del message
                del backend_type
                raise sanitized_error
            if import_failed:
                sanitized_import_error = ImportError(
                    "Selected backend could not be initialized because an import failed."
                )
                del args
                del kwargs
                del connection_error
                del import_failed
                del unsupported
                raise sanitized_import_error
            if unsupported:
                operation = unsupported_capability or "requested"
                sanitized_not_implemented = NotImplementedError(
                    f"Selected backend does not support {operation} operations"
                )
                del args
                del kwargs
                del connection_error
                del import_failed
                del unsupported
                del operation
                raise sanitized_not_implemented
            raise AssertionError("manager terminal boundary did not select an error")

        return wrapped

    return decorate


_SAFE_DURABLE_PUSH_QUEUE_MESSAGES: frozenset[str] = frozenset(
    {
        "Selected queue backend generation is not worker-crash durable",
        "Queue backend returned no valid worker-crash durability receipt",
        "Backend operation failed.",
    }
)

_DURABLE_PUSH_QUEUE_ERROR_MESSAGE = "Queue backend push failed."


def _durable_push_queue_error_boundary(
    function: Callable[_P, _T],
) -> Callable[_P, _T]:
    """Publish a terminal, queue-name-free error for durable queue pushes.

    ``_push_queue_with_durability`` spans manager snapshots, an optional breaker
    proxy, and backend methods. A ``QueueError`` from any of those layers can
    retain the logical queue, serialized item, backend configuration, and a
    driver cause. This outermost boundary rebuilds that public error after all
    inner manager and backend frames have unwound. It also rebuilds the static
    OPEN-breaker result for the same traceback reason. Other documented input,
    capability, connection, import, and control-flow exceptions retain their
    established contracts.
    """

    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        caught_error: QueueError | None = None
        circuit_open_error = False
        try:
            return function(*args, **kwargs)
        except QueueError as error:
            caught_error = error
        except CircuitBreakerOpenError:
            # The breaker error is already a static operational result, but its
            # traceback still crosses this manager method and retains queue/item/
            # configuration locals. Rebuild it after those frames unwind.
            circuit_open_error = True
        except BaseException:
            del args
            del kwargs
            raise

        if circuit_open_error:
            sanitized_open_error = CircuitBreakerOpenError("backend-operation")
            del args
            del kwargs
            del caught_error
            del circuit_open_error
            raise sanitized_open_error

        assert caught_error is not None
        message = _DURABLE_PUSH_QUEUE_ERROR_MESSAGE
        raw_args: object = None
        if type(caught_error) is QueueError:
            raw_args = caught_error.args
            if (
                type(raw_args) is tuple
                and len(raw_args) == 1
                and type(raw_args[0]) is str
                and raw_args[0] in _SAFE_DURABLE_PUSH_QUEUE_MESSAGES
            ):
                message = raw_args[0]
        sanitized_error = QueueError(message, operation="push")
        del args
        del kwargs
        del caught_error
        del circuit_open_error
        del raw_args
        del message
        raise sanitized_error

    return wrapped


def _rebuild_connect_attempt_error(error: BaseException) -> BaseException:
    """Build the terminal ``connect()`` contract without raising ``error``.

    A lazy attempt stores one result for multiple waiters.  Raising that shared
    object, even through a decorator, attaches a traceback to the prototype and
    can retain the owner's mutable settings.  This mirrors the two public
    ``connect()`` boundaries by inspecting only exact built-in error types and
    returning a fresh, traceback-free replacement.
    """
    if isinstance(error, ImportError):
        return ImportError(
            "Selected backend could not be initialized because an import failed."
        )

    if isinstance(error, BackendConnectionError):
        return BackendConnectionError(
            _safe_manager_connection_message(error),
            backend_type=_safe_manager_connection_backend_type(error),
        )

    if type(error) is ConfigurationError:
        raw_args = error.args
        raw_message = (
            raw_args[0] if type(raw_args) is tuple and len(raw_args) == 1 else None
        )
        message = "Connection manager configuration is invalid."
        if type(raw_message) is str:
            try:
                if _is_safe_manager_configuration_message(raw_message):
                    message = raw_message
            except Exception:  # noqa: BLE001 - fail closed on custom error state
                message = "Connection manager configuration is invalid."
        setting_name = error.setting_name
        if (
            type(setting_name) is not str
            or setting_name not in _MANAGER_CONFIGURATION_SETTING_NAMES
        ):
            setting_name = "configuration"
        return ConfigurationError(message, setting_name=setting_name)

    if isinstance(error, Exception):
        return ConfigurationError(
            "Connection manager configuration is invalid.",
            setting_name="configuration",
        )

    # Control-flow ``BaseException`` values intentionally retain their public
    # semantics; they are never used as normal connection-attempt diagnostics.
    return error


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


class _RetirementFinalizerToken:
    """Invocation-scoped retirement ownership that can be reclaimed after unwind."""

    __slots__ = ("thread_id",)

    def __init__(self) -> None:
        self.thread_id = threading.get_ident()

    @property
    def active(self) -> bool:
        """Whether a live frame still owns this exact token."""
        try:
            frame = sys._current_frames().get(self.thread_id)  # noqa: SLF001
        except Exception:  # noqa: BLE001 - stale ownership must be reclaimable
            return False
        while frame is not None:
            try:
                if frame.f_locals.get("finalizer_token") is self:
                    return True
            except Exception:  # noqa: BLE001 - fail closed to reclaimable ownership
                return False
            frame = frame.f_back
        return False


class ConnectionManagerLease:
    """Acquire-specific, idempotently releasable manager ownership.

    The legacy :meth:`ConnectionManager.get_manager` API cannot identify which
    holder calls ``manager.close()``.  Components that must repair an interrupted
    teardown use this additive lease API instead: retrying ``release()`` always
    targets the same opaque acquire token and can never consume a peer's hold.
    """

    __slots__ = ("_manager", "_token")

    def __init__(self, manager: ConnectionManager, token: object) -> None:
        self._manager = manager
        self._token = token

    @property
    def manager(self) -> ConnectionManager:
        """Return the shared manager owned by this lease."""
        return self._manager

    @property
    def released(self) -> bool:
        """Whether this lease's ownership token is no longer active."""
        return self._manager._is_acquire_released(self._token)

    def release(self) -> None:
        """Release this exact acquire; repeated calls are idempotent."""
        try:
            self._manager._release_acquire(self._token)
        except BaseException:
            # The lease object may be lost immediately after a factory rollback.
            # Keep it reachable through the manager/registry until a later owner
            # can retry the same opaque token; never turn an active users=1 hold
            # into an unreachable leak.
            self._manager._retain_failed_lease(self)
            raise
        else:
            self._manager._forget_failed_lease(self)


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

    Registry mutation and instance mutation are kept in separate critical
    sections. In particular, pooled construction, plugin discovery and class
    validation, lifecycle callbacks, and manager teardown all run without
    ``_registry_lock`` held. The A2 owner-gate likewise runs ``connect`` and
    backoff outside ``_lock`` so a slow backend cannot block peer threads.
    """

    # Class-level registry of managers. R14-E: this is an LRU-bounded
    # ``OrderedDict`` (``MAX_MANAGERS``) so settings churn — per-spider creds,
    # unique ``group_id``, rotating endpoints — cannot leak live ``Backend``
    # instances + their open sockets forever. On overflow the oldest
    # genuinely-orphaned entry (``_users <= 0``) is evicted and disconnected;
    # actively-used managers (``_users > 0``) are never evicted.
    _managers: ClassVar[OrderedDict[str, ConnectionManager]] = OrderedDict()
    _registry_lock: ClassVar[threading.Lock] = threading.Lock()
    # Per-key construction gates keep expensive and potentially untrusted plugin
    # discovery outside the registry lock while preserving singleton publication.
    # The epoch fences candidates started before ``clear_registry()``.
    _manager_constructions: ClassVar[dict[str, _ManagerConstructionAttempt]] = {}
    _manager_construction_owners: ClassVar[set[int]] = set()
    _registry_epoch: ClassVar[int] = 0
    # One-shot guard for the "registry over cap with all entries live" warning
    # so we don't spam logs on every get_manager() once the cap is saturated.
    _over_cap_warned: ClassVar[bool] = False
    # Failed factory rollback must not make an exact lease unreachable. These
    # package-owned records keep the manager alive until a later registry pass
    # can retry the same token; they are intentionally separate from active
    # registry entries and never consume a peer's acquire.
    _pending_release_leases: ClassVar[list[ConnectionManagerLease]] = []
    _pending_release_managers: ClassVar[list[ConnectionManager]] = []
    _pending_release_retry_threads: ClassVar[set[int]] = set()
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
        normalized_backend_type = _normalized_manager_backend_type(backend_type)
        if normalized_backend_type is None or (
            settings is not None and type(settings) is not dict
        ):
            input_error = ConfigurationError(
                "Connection manager requires a backend registry-key string and settings dictionary.",
                setting_name="backend_settings",
            )
            del backend_type
            del settings
            raise input_error
        descriptor = get_descriptor(normalized_backend_type)
        plugin_backend_cls: object | None = None
        plugin_ack_capabilities: _PluginAckCapabilitySnapshot | None = None
        plugin_class_load_failed = False
        ack_contract_failed = False
        if descriptor.backend_type not in _BUNDLED_BACKEND_TYPES:
            backend_cls: object | None = None
            try:
                backend_cls = _load_descriptor_object(
                    descriptor,
                    descriptor.backend_cls_path,
                )
            except Exception:  # noqa: BLE001 - plugin loader details stay private
                plugin_class_load_failed = True
            if not plugin_class_load_failed:
                try:
                    plugin_ack_capabilities = _validate_plugin_ack_class(
                        descriptor,
                        backend_cls,
                    )
                    # The exact object whose static contract was accepted is the
                    # only class this manager may later construct. Re-resolving an
                    # untrusted module attribute would create a validation/use gap.
                    plugin_backend_cls = backend_cls
                except Exception:  # noqa: BLE001 - static plugin metadata is untrusted
                    ack_contract_failed = True
        if plugin_class_load_failed or ack_contract_failed:
            invalid_plugin_class = plugin_class_load_failed
            del backend_type
            del settings
            del normalized_backend_type
            del descriptor
            del backend_cls
            del plugin_backend_cls
            del plugin_ack_capabilities
            del plugin_class_load_failed
            del ack_contract_failed
            if invalid_plugin_class:
                del invalid_plugin_class
                raise ConfigurationError(
                    "Selected backend has an invalid plugin class path.",
                    setting_name="SCRAPY_BACKEND_TYPE",
                )
            del invalid_plugin_class
            raise _invalid_plugin_ack_contract()
        self.backend_type = (
            backend_type
            if isinstance(backend_type, BackendType)
            else normalized_backend_type
        )
        self.settings = settings if settings is not None else {}
        self._plugin_backend_cls = plugin_backend_cls
        # This immutable construction-time snapshot is the sole source for every
        # plugin ACK gate and adapter decision. Class metadata may be mutated by
        # third-party code later; only a newly constructed manager revalidates it.
        self._plugin_ack_capabilities = plugin_ack_capabilities
        self._plugin_queue_backend_source: (
            tuple[Backend, CircuitBreaker | None] | None
        ) = None
        self._plugin_queue_backend: _DeferredAckPluginQueueBackend | None = None
        # ``get_manager()`` fills these fields when it inserts the instance into
        # the shared registry. Pooled managers use the acquire-time values for
        # every operation and for eventual eviction, so mutations of the public
        # compatibility attributes cannot retarget or strand them. Bare direct
        # constructors leave the fields unset and retain their dynamic behavior.
        self._registry_token: str | None = None
        self._registry_backend_type: str | None = None
        self._registry_settings: dict[str, Any] | None = None
        self._backend: Backend | None = None
        self._connection_generation = 0
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
        # publish a backend into the now-unowned instance. The event mirrors that
        # terminal transition so a retry backoff wakes without polling or waiting
        # for the full delay.
        self._retired = False
        self._retirement_event = threading.Event()
        # Authoritative acquire ownership.  Every pooled acquisition has one
        # opaque identity token; legacy ``get_manager()`` calls additionally place
        # their token in ``_legacy_acquires`` so each ``manager.close()`` consumes
        # one legacy hold.  ``_users`` is retained as an observational compatibility
        # count only and is always synchronized from ``_active_acquires``.
        self._active_acquires: set[object] = set()
        self._legacy_acquires: list[object] = []
        # ``get_manager`` retains this per-thread handoff briefly so a composite
        # owner that needs exact rollback can adopt the token it just acquired.
        self._legacy_acquire_handoffs: dict[int, list[object]] = {}
        self._users: int = 0
        # Retirement is a repairable generation state.  Removing the final token
        # and publishing ``_retired`` happen atomically under ``_registry_lock``;
        # a retry with an already-absent token still completes this teardown.
        self._retirement_complete = False
        self._retirement_finalizing = False
        self._retirement_finalizer_thread_id: int | None = None
        self._retirement_finalizer_token: _RetirementFinalizerToken | None = None
        self._retirement_finalization_event = threading.Event()
        self._retiring_backend: Backend | None = None
        self._retirement_disconnect_started = False
        self._retiring_adapter: _DeferredAckPluginQueueBackend | None = None
        self._retiring_adapter_source: tuple[Backend, CircuitBreaker | None] | None = (
            None
        )
        # Single-connect ownership flag (A2). The first thread to enter the slow
        # path takes ownership under ``_lock``; peers capture the same attempt and
        # wait for its result. A distinct result object per attempt is necessary:
        # after a failure, a later caller may start a fresh attempt before older
        # peers are scheduled, but those peers must still receive the failure they
        # waited for instead of joining the new attempt or retrying serially.
        self._connecting: bool = False
        self._connected_event = threading.Event()
        self._connect_attempt: _ConnectionAttempt | None = None
        # ``backend`` owners mark only their own thread here before calling the
        # public ``connect()`` method.  That lets ``connect()`` resolve and signal
        # this lazy attempt before it invokes a user-supplied lifecycle monitor,
        # without changing the independent direct-connect path.
        self._lazy_connection_context = _LazyConnectionContext()
        # Circuit-breaker holder. Lazily constructed on first
        # ``get_*_backend()`` call from the resolved Scrapy policy, or from the
        # env-loaded ``Settings`` when this manager was constructed directly.
        # ``None`` while disabled — which is the default, so the default path
        # returns the raw backend with zero overhead and byte-identical behavior.
        self._breaker: CircuitBreaker | None = None
        self._breaker_configured: bool = False
        self._breaker_resolved_from_env_fallback: bool = False
        self._breaker_policy_values: tuple[bool, int, float] | None = None
        self._dropped_breaker_policy_warned: bool = False
        # R14-D: observability monitor for connection-lifecycle hooks
        # (on_connect / on_disconnect / on_retry). Defaults to NullMonitor so the
        # hooks are no-ops unless a caller (scheduler / dupefilter factory) threads
        # a real monitor via :meth:`set_monitor`. Threading into the scheduler
        # factory is a follow-up (scheduler.py is out of R14-D scope); the hooks
        # + their stat keys (backend/connect_count, etc.) are wired here so the
        # observability contract is in place the moment a monitor is attached.
        self._monitor: Monitor = NullMonitor()

    def _backend_type_for_operations(self) -> str:
        """Return the pinned pooled type or the direct constructor's public type."""
        if self._registry_token is not None:
            assert self._registry_backend_type is not None
            return self._registry_backend_type
        return self.backend_type

    def _settings_for_operations(self) -> dict[str, Any]:
        """Return pinned pooled settings or direct constructor public settings."""
        if self._registry_token is not None:
            assert self._registry_settings is not None
            return self._registry_settings
        return self.settings

    @classmethod
    def _retain_failed_lease(cls, lease: ConnectionManagerLease) -> None:
        """Keep one failed exact release reachable for a later retry."""
        with cls._registry_lock:
            if not any(existing is lease for existing in cls._pending_release_leases):
                cls._pending_release_leases.append(lease)

    @classmethod
    def _forget_failed_lease(cls, lease: ConnectionManagerLease) -> None:
        """Drop a failed-release record after the exact token is settled."""
        with cls._registry_lock:
            cls._pending_release_leases = [
                existing
                for existing in cls._pending_release_leases
                if existing is not lease
            ]

    @classmethod
    def _retain_failed_manager(cls, manager: ConnectionManager) -> None:
        """Keep a legacy manager finalizer reachable after interruption."""
        with cls._registry_lock:
            if not any(
                existing is manager for existing in cls._pending_release_managers
            ):
                cls._pending_release_managers.append(manager)

    @classmethod
    def _forget_failed_manager(cls, manager: ConnectionManager) -> None:
        """Drop a legacy finalizer record after retirement is published."""
        with cls._registry_lock:
            cls._pending_release_managers = [
                existing
                for existing in cls._pending_release_managers
                if existing is not manager
            ]

    @classmethod
    def retry_pending_releases(cls) -> None:
        """Retry exact factory rollbacks without consuming unrelated holds."""
        thread_id = threading.get_ident()
        with cls._registry_lock:
            if thread_id in cls._pending_release_retry_threads:
                return
            cls._pending_release_retry_threads.add(thread_id)
            leases = tuple(cls._pending_release_leases)
            managers = tuple(cls._pending_release_managers)
        try:
            for lease in leases:
                try:
                    lease.release()
                except BaseException:
                    pass
            for manager in managers:
                try:
                    manager.close()
                except BaseException:
                    pass
        finally:
            with cls._registry_lock:
                cls._pending_release_retry_threads.discard(thread_id)

    @classmethod
    def get_manager(
        cls,
        backend_type: str,
        settings: dict[str, Any] | None = None,
    ) -> ConnectionManager:
        """Get a shared manager and register one legacy ``close()`` acquire."""
        manager, token = cls._get_manager_with_token(
            backend_type,
            settings,
            legacy=True,
        )
        return manager

    @classmethod
    def _adopt_latest_legacy_lease(
        cls,
        manager: ConnectionManager,
    ) -> ConnectionManagerLease | None:
        """Adopt the current thread's just-created legacy token exactly once."""
        if not isinstance(manager, cls):
            return None
        try:
            handoffs = manager._legacy_acquire_handoffs
        except AttributeError:
            return None
        with cls._registry_lock:
            handoff = handoffs.get(threading.get_ident())
            while handoff:
                token = handoff.pop()
                if token in manager._active_acquires:
                    try:
                        manager._legacy_acquires.remove(token)
                    except ValueError:
                        pass
                    if not handoff:
                        manager._legacy_acquire_handoffs.pop(
                            threading.get_ident(), None
                        )
                    return ConnectionManagerLease(manager, token)
            manager._legacy_acquire_handoffs.pop(threading.get_ident(), None)
        return None

    @classmethod
    def acquire_lease(
        cls,
        backend_type: str,
        settings: dict[str, Any] | None = None,
    ) -> ConnectionManagerLease:
        """Get a shared manager with acquire-specific idempotent ownership."""
        manager, token = cls._get_manager_with_token(
            backend_type,
            settings,
            legacy=False,
        )
        return ConnectionManagerLease(manager, token)

    @classmethod
    def _get_manager_with_token(
        cls,
        backend_type: str,
        settings: dict[str, Any] | None,
        *,
        legacy: bool,
    ) -> tuple[ConnectionManager, object]:
        """Acquire one pooled manager and publish one opaque ownership token."""
        cls.retry_pending_releases()
        thread_id = threading.get_ident()
        acquire_token = object()
        with cls._registry_lock:
            if thread_id in cls._manager_construction_owners:
                raise ConfigurationError(
                    "Recursive pooled connection manager construction is not supported.",
                    setting_name="backend_settings",
                )

        # Hash and retain one operational deep snapshot, with a separate public
        # copy. Otherwise a caller can mutate a nested value after hashing and make
        # the old registry key point at new connection settings. Do not invoke
        # truthiness, hashing, deepcopy, or
        # normalisation on arbitrary container/type subclasses before this public
        # configuration boundary has verified their outer shape.
        normalized_backend_type = _normalized_manager_backend_type(backend_type)
        if normalized_backend_type is None or (
            settings is not None and type(settings) is not dict
        ):
            input_error = ConfigurationError(
                "Connection manager requires a backend registry-key string and settings dictionary.",
                setting_name="backend_settings",
            )
            del backend_type
            del settings
            raise input_error

        settings_snapshot: dict[str, Any] | None = None
        public_settings_snapshot: dict[str, Any] | None = None
        key: str | None = None
        snapshot_failed = False
        try:
            settings_snapshot = deepcopy(settings) if settings is not None else {}
            # The public mapping remains mutable for compatibility, but it must not
            # alias the operational snapshot retained by a pooled manager.
            public_settings_snapshot = deepcopy(settings_snapshot)
            key = cls._registry_key(normalized_backend_type, settings_snapshot)
        except Exception:  # noqa: BLE001 - nested config values are untrusted
            snapshot_failed = True
        if snapshot_failed:
            input_error = ConfigurationError(
                "Connection manager settings cannot be normalized.",
                setting_name="backend_settings",
            )
            del backend_type
            del settings
            del settings_snapshot
            del public_settings_snapshot
            del key
            raise input_error
        assert settings_snapshot is not None
        assert public_settings_snapshot is not None
        assert key is not None

        while True:
            victims: list[ConnectionManager] = []
            attempt: _ManagerConstructionAttempt | None = None
            construct = False
            manager: ConnectionManager | None = None

            with cls._registry_lock:
                # Constructor-time plugin callbacks may not recursively acquire
                # any pooled manager, even one that was already published. Allowing
                # that special case would make recursion depend on registry history
                # and could expose an instance while its caller is half-constructed.
                if thread_id in cls._manager_construction_owners:
                    raise ConfigurationError(
                        "Recursive pooled connection manager construction is not supported.",
                        setting_name="backend_settings",
                    )

                manager = cls._managers.get(key)
                if manager is not None and manager._retired:
                    # A stale terminal entry must not be reacquired. Detach it by
                    # identity now and tear it down after releasing the lock.
                    if cls._managers.get(key) is manager:
                        cls._managers.pop(key)
                        victims.append(manager)
                    manager = None

                if manager is not None:
                    cls._managers.move_to_end(key)
                    cls._register_acquire_under_lock(
                        manager,
                        acquire_token,
                        legacy=legacy,
                    )
                else:
                    attempt = cls._manager_constructions.get(key)
                    if attempt is None:
                        attempt = _ManagerConstructionAttempt(
                            thread_id, cls._registry_epoch
                        )
                        cls._manager_constructions[key] = attempt
                        cls._manager_construction_owners.add(thread_id)
                        construct = True
                    else:
                        attempt.waiters += 1

            # Neither stale-manager teardown nor waiting on another constructor may
            # hold the registry lock. A teardown backend is application/plugin code.
            for victim in victims:
                cls._disconnect_backend_safely(victim)
            if manager is not None:
                return manager, acquire_token
            assert attempt is not None
            if not construct:
                attempt.event.wait()
                continue

            candidate: ConnectionManager | None = None
            try:
                # This is deliberately outside ``_registry_lock``: construction can
                # discover entry points, load plugin classes, and validate arbitrary
                # class attributes.
                candidate = cls(backend_type, public_settings_snapshot)
            except BaseException:
                # KeyboardInterrupt and other control-flow exceptions must release
                # the single-flight gate just like ordinary constructor failures.
                with cls._registry_lock:
                    if cls._manager_constructions.get(key) is attempt:
                        cls._manager_constructions.pop(key)
                    cls._manager_construction_owners.discard(thread_id)
                    attempt.event.set()
                raise

            publish_victims: list[ConnectionManager] = []
            selected: ConnectionManager | None = None
            warn_over_cap = False
            with cls._registry_lock:
                gate_is_current = (
                    cls._manager_constructions.get(key) is attempt
                    and cls._registry_epoch == attempt.epoch
                )
                if cls._manager_constructions.get(key) is attempt:
                    cls._manager_constructions.pop(key)
                cls._manager_construction_owners.discard(thread_id)

                existing = cls._managers.get(key)
                if existing is not None and existing._retired:
                    if cls._managers.get(key) is existing:
                        cls._managers.pop(key)
                        publish_victims.append(existing)
                    existing = None

                if gate_is_current and existing is None:
                    # LRU enforcement belongs to publication, not construction: a
                    # concurrent clear or publisher may have changed registry size
                    # while this candidate was being built.
                    collected, warn_over_cap = cls._collect_orphans_under_lock()
                    publish_victims.extend(collected)
                    candidate._registry_backend_type = candidate.backend_type
                    candidate._registry_settings = settings_snapshot
                    candidate._registry_token = key
                    cls._register_acquire_under_lock(
                        candidate,
                        acquire_token,
                        legacy=legacy,
                    )
                    cls._managers[key] = candidate
                    selected = candidate
                    candidate = None
                elif existing is not None:
                    # Double-check publication: a post-clear generation may already
                    # have won. Reuse it and preserve one acquire per successful call.
                    cls._managers.move_to_end(key)
                    cls._register_acquire_under_lock(
                        existing,
                        acquire_token,
                        legacy=legacy,
                    )
                    selected = existing

                # Publish/abort state is complete before peers wake and re-check.
                attempt.event.set()

            # A candidate invalidated by clear_registry(), or one that lost the
            # publication double-check, is terminally disposed outside every
            # registry critical section. Then retry if no current manager existed.
            if candidate is not None:
                cls._disconnect_backend_safely(candidate)
            for victim in publish_victims:
                cls._disconnect_backend_safely(victim)
            if warn_over_cap:
                _log_diagnostic(
                    logger.warning,
                    "ConnectionManager registry at cap (%d) with all entries "
                    "actively held; not force-evicting live managers. This "
                    "indicates genuine unbounded backend coexistence — investigate "
                    "the source of distinct backend settings.",
                    cls.MAX_MANAGERS,
                )
            if selected is not None:
                return selected, acquire_token

    @classmethod
    def _register_acquire_under_lock(
        cls,
        manager: ConnectionManager,
        token: object,
        *,
        legacy: bool,
    ) -> None:
        """Publish one active token while ``_registry_lock`` is held."""
        manager._active_acquires.add(token)
        if legacy:
            manager._legacy_acquires.append(token)
            manager._legacy_acquire_handoffs.setdefault(
                threading.get_ident(), []
            ).append(token)
        manager._users = len(manager._active_acquires)

    @classmethod
    def _collect_orphans_under_lock(
        cls,
    ) -> tuple[list[ConnectionManager], bool]:
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
            Victims the caller MUST disconnect outside the registry lock, plus
            whether the caller must emit the one-shot saturation warning there.
        """
        victims: list[ConnectionManager] = []
        warn_over_cap = False
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
                    warn_over_cap = True
                return victims, warn_over_cap
            victims.append(cls._managers.pop(orphan_key))
        return victims, warn_over_cap

    @staticmethod
    def _disconnect_backend_safely(manager: ConnectionManager) -> None:
        """Force one token generation terminal, suppressing teardown failures."""
        cls = type(manager)
        with cls._registry_lock:
            manager._active_acquires.clear()
            manager._legacy_acquires.clear()
            manager._legacy_acquire_handoffs.clear()
            manager._users = 0
            manager._retired = True
            manager._retirement_event.set()
        try:
            manager._finalize_retirement()
        except BaseException:
            # Forced registry teardown is best-effort: one broken victim must not
            # strand later victims or invalidate a newly returned manager acquire.
            pass

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
        bt_key = _normalized_manager_backend_type(backend_type)
        if bt_key is None or type(settings) is not dict:
            input_error = ConfigurationError(
                "Connection manager requires a backend registry-key string and settings dictionary.",
                setting_name="backend_settings",
            )
            del backend_type
            del settings
            raise input_error

        normalized_settings: list[Any] | None = None
        settings_key: str | None = None
        normalization_failed = False
        try:
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
                # ``normalized_settings`` contains JSON-native values only. This
                # retains the former JSON-facade fallback without plaintext repr.
                settings_key = _canonical_registry_json(normalized_settings)
        except Exception:  # noqa: BLE001 - nested settings can run custom code
            normalization_failed = True
        if normalization_failed:
            input_error = ConfigurationError(
                "Connection manager settings cannot be normalized.",
                setting_name="backend_settings",
            )
            del backend_type
            del settings
            del normalized_settings
            del settings_key
            raise input_error
        assert settings_key is not None
        settings_digest = hashlib.sha256(settings_key.encode("utf-8")).hexdigest()
        return f"{bt_key}:{settings_digest}"

    def _detach_plugin_queue_backend_under_lock(
        self,
    ) -> tuple[
        _DeferredAckPluginQueueBackend | None,
        tuple[Backend, CircuitBreaker | None] | None,
    ]:
        """Detach one cached adapter generation while retaining strong locals.

        The caller must keep the returned references alive until after ``_lock`` is
        released.  Adapters own active identity tokens, and a token destructor is
        arbitrary plugin code that may acquire or re-enter this manager.
        """
        retired_adapter = self._plugin_queue_backend
        retired_source = self._plugin_queue_backend_source
        self._plugin_queue_backend = None
        self._plugin_queue_backend_source = None
        return retired_adapter, retired_source

    @property
    def _deferred_ack_plugin(self) -> bool:
        """Backward-compatible view of the pinned plugin adapter decision."""
        snapshot = self._plugin_ack_capabilities
        return snapshot is not None and snapshot.deferred_ack_plugin

    @property
    def _plugin_supports_concurrent_ack(self) -> bool:
        """Backward-compatible view of the pinned plugin concurrency claim."""
        snapshot = self._plugin_ack_capabilities
        return snapshot is not None and snapshot.supports_concurrent_ack

    def _static_ack_capabilities(self) -> tuple[bool, bool]:
        """Return ACK flags pinned when this plugin manager was constructed."""
        snapshot = self._plugin_ack_capabilities
        if snapshot is not None:
            return snapshot.requires_ack, snapshot.supports_concurrent_ack
        backend_type = self._backend_type_for_operations()
        descriptor = get_descriptor(
            backend_type.value
            if isinstance(backend_type, BackendType)
            else backend_type
        )
        return _load_static_ack_capabilities(descriptor)

    @_manager_terminal_error_boundary()
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        pass_through_exception_types=(ImportError,),
    )
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
        backend_type = self._backend_type_for_operations()
        descriptor: BackendDescriptor = get_descriptor(
            backend_type.value
            if isinstance(backend_type, BackendType)
            else backend_type
        )
        backend_cls = self._plugin_backend_cls
        if backend_cls is None:
            backend_cls = _load_descriptor_object(
                descriptor,
                descriptor.backend_cls_path,
            )
        settings_cls = _load_descriptor_object(descriptor, descriptor.settings_cls_path)
        if not callable(backend_cls) or not callable(settings_cls):
            msg = "Selected backend must provide callable backend and settings classes."
            raise ConfigurationError(msg, setting_name="SCRAPY_BACKEND_TYPE")
        backend_field_names = _model_field_names(settings_cls)
        manager_only_names = _CONNECTION_MANAGER_SETTING_NAMES - backend_field_names
        backend_settings = {
            name: value
            for name, value in self._settings_for_operations().items()
            if name not in _CONNECTION_MANAGER_BACKEND_EXCLUDED_KEYS
            and name not in manager_only_names
        }
        bundled_descriptor = descriptor.backend_type in _BUNDLED_BACKEND_TYPES
        settings_obj: Any = None
        settings_error: ConfigurationError | None = None
        try:
            settings_obj = settings_cls(**backend_settings)
        except ImportError:
            if bundled_descriptor:
                raise
            settings_error = ConfigurationError(
                "Invalid backend setting 'backend_settings'.",
                setting_name="backend_settings",
            )
        except (ConfigurationError, ValidationError, TypeError) as error:
            setting_name = _safe_manager_setting_name(
                error, descriptor.backend_type, backend_field_names
            )
            settings_error = ConfigurationError(
                f"Invalid backend setting '{setting_name}'.",
                setting_name=setting_name,
            )
        except Exception as error:  # noqa: BLE001 - plugin settings are untrusted
            if bundled_descriptor:
                raise
            setting_name = _safe_manager_setting_name(
                error, descriptor.backend_type, backend_field_names
            )
            settings_error = ConfigurationError(
                f"Invalid backend setting '{setting_name}'.",
                setting_name=setting_name,
            )
        if settings_error is not None:
            # Pydantic errors retain raw input in their error graph. Keep manager
            # construction typed and non-retryable without publishing that input.
            raise settings_error

        backend: object | None = None
        constructor_error: ConfigurationError | None = None
        try:
            backend = backend_cls(settings_obj)
        except ImportError:
            if bundled_descriptor:
                raise
            constructor_error = ConfigurationError(
                "Selected backend could not be constructed.",
                setting_name="SCRAPY_BACKEND_TYPE",
            )
        except TypeError:
            constructor_error = ConfigurationError(
                "Selected backend could not be constructed.",
                setting_name="SCRAPY_BACKEND_TYPE",
            )
        except Exception:  # noqa: BLE001 - plugin constructors are untrusted
            if bundled_descriptor:
                raise
            # A plugin's constructor contract is static configuration. Retrying it
            # as though it were a transient network failure only delays a useful
            # error and can leave operators with a misleading BackendConnectionError.
            constructor_error = ConfigurationError(
                "Selected backend could not be constructed.",
                setting_name="SCRAPY_BACKEND_TYPE",
            )
        if constructor_error is not None:
            raise constructor_error
        # Bundled descriptors and implementations ship together and are covered by
        # their backend contract suite.  Third-party descriptors are executable
        # metadata from another distribution, so enforce the runtime contract at
        # this lazy first-use boundary before any connection attempt can start.
        if descriptor.backend_type not in _BUNDLED_BACKEND_TYPES:
            return _validate_backend_contract(backend, descriptor)
        return cast("Backend", backend)

    @_manager_terminal_error_boundary()
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        sanitize_exception_types=(ValidationError,),
        pass_through_exception_types=(BackendConnectionError, ImportError),
    )
    def connect(self) -> None:
        """Establish connection with retry logic.

        Makes one initial connection attempt, then up to ``retry_attempts`` retries
        with exponential backoff based on ``retry_delay``. Concurrent direct calls
        share the resulting backend: the complete retry transaction is serialized,
        and each waiter re-checks the connected fast path after acquiring the
        connection lock.

        Raises:
            BackendConnectionError: If all network retry attempts fail.
            ConfigurationError: If generic or backend-specific settings are invalid.
            ImportError: If the selected backend's optional dependency is missing.
        """
        # A lifecycle hook may itself invoke ``connect()``.  During a lazy-owner
        # callback, the owner has already published a terminal result, so replay
        # that result instead of starting a nested transaction from the callback.
        if self._lazy_connection_context.dispatch_attempt is not None:
            self._backend_from_reentrant_lazy_monitor()
            return

        monitor_events: list[_MonitorEvent] = []
        lazy_attempt = self._lazy_connection_context.owner_attempt
        if lazy_attempt is not None:
            # Publish the result, then leave the exception handler before dispatch.
            # A callback that re-enters ``backend`` must not inherit the owner's raw
            # failure as ``__context__`` merely because the monitor ran in a
            # ``finally`` while that failure was being propagated.
            lazy_terminal_error: BaseException | None = None
            try:
                self._run_connect_transaction(monitor_events)
            except BaseException as error:
                lazy_terminal_error = error
            else:
                lazy_terminal_error = self._complete_lazy_connection_attempt(
                    lazy_attempt, None
                )

            if lazy_terminal_error is not None:
                lazy_terminal_error = self._complete_lazy_connection_attempt(
                    lazy_attempt,
                    lazy_terminal_error,
                )
                assert lazy_terminal_error is not None
            # Monitor implementations are user-extensible and may call back into
            # the manager. Dispatch after both the connection transaction and lazy
            # attempt publication have completed, outside every manager lock.
            self._dispatch_monitor_events(monitor_events, lazy_attempt)
            if lazy_terminal_error is not None:
                raise _rebuild_connect_attempt_error(lazy_terminal_error)
            return

        terminal_error: BaseException | None = None
        try:
            self._run_connect_transaction(monitor_events)
        except BaseException as error:
            # Capture every terminal result, including control-flow exceptions, so
            # the handler ends before user-extensible monitor callbacks run. A
            # ``finally`` would expose the raw primary through ``sys.exc_info()``
            # to a callback dispatched for an earlier retry event.
            terminal_error = error

        # Keep the direct-call contract: buffered events still dispatch after a
        # control-flow failure, and a callback's own BaseException takes
        # precedence because it exits before the pending terminal result is
        # re-raised below.
        self._dispatch_monitor_events(monitor_events)
        if terminal_error is not None:
            raise terminal_error

    def _complete_lazy_connection_attempt(
        self,
        attempt: _ConnectionAttempt,
        connect_error: BaseException | None,
    ) -> BaseException | None:
        """Publish one lazy owner result before its monitor callbacks run.

        ``connect()`` invokes this after releasing ``_connect_lock`` and before
        dispatching its deferred lifecycle events.  The owner property repeats
        the call after ``connect()`` returns so mocked or third-party overrides of
        ``connect()`` retain the same defensive completion contract.  The event
        makes the operation idempotent: a callback's control-flow exception can
        never undo the result already delivered to waiting peers.
        """
        # A mocked or third-party ``connect()`` override can bypass the public
        # decorator stack.  Never let that raw failure become the shared result
        # observed by waiters or a reentrant monitor callback.
        if connect_error is not None:
            connect_error = _rebuild_connect_attempt_error(connect_error)

        with self._lock:
            if attempt.event.is_set():
                return attempt.error

            if connect_error is None and self._retired:
                connect_error = BackendConnectionError(
                    "ConnectionManager was released while connecting",
                    backend_type=str(self._backend_type_for_operations()),
                )

            if connect_error is None:
                # Capture the published handle exactly once under the same state lock
                # used by reconnect/close. Returning a second ``self._backend`` read
                # allowed reconnect to detach it after a non-null guard, leaking None
                # through this property and into interface-proxy construction.
                if self._backend is None:
                    connect_error = BackendConnectionError(
                        "connect() did not produce a backend",
                        backend_type=str(self._backend_type_for_operations()),
                    )

            attempt.error = connect_error
            self._connecting = False
            attempt.event.set()
            return connect_error

    def _backend_from_reentrant_lazy_monitor(self) -> Backend | None:
        """Return the result published for the active lazy monitor callback.

        A retry callback on a terminally failed lazy attempt must observe that
        attempt's typed error rather than electing itself as a new owner.  On a
        successful attempt it receives the exact backend that was published for
        the owner and peer cohort.  Calls outside this narrow callback context
        return ``None`` and follow the ordinary fast/slow accessor paths.
        """
        attempt = self._lazy_connection_context.dispatch_attempt
        if attempt is None:
            return None
        if attempt.error is not None:
            raise _rebuild_connect_attempt_error(attempt.error)
        with self._lock:
            if self._retired:
                raise BackendConnectionError(
                    "ConnectionManager was released while connecting",
                    backend_type=str(self._backend_type_for_operations()),
                )
            backend = self._backend
        if backend is not None:
            return backend
        raise BackendConnectionError(
            "connect() did not produce a backend",
            backend_type=str(self._backend_type_for_operations()),
        )

    def _detach_stale_backend(self) -> tuple[Backend | None, int | None]:
        """Detach an unhealthy generation without invoking its driver callback."""
        with self._lock:
            if self._retired:
                raise BackendConnectionError(
                    "Cannot connect a released ConnectionManager",
                    backend_type=str(self._backend_type_for_operations()),
                )
            backend = self._backend
            generation = self._connection_generation
        if backend is None:
            return None, None

        health_check_failed = False
        try:
            connected = backend.is_connected()
        except Exception:
            connected = False
            health_check_failed = True
        if health_check_failed:
            _log_diagnostic(
                logger.debug,
                "Backend health check failed before reconnect",
            )

        with self._lock:
            if self._retired:
                raise BackendConnectionError(
                    "Cannot connect a released ConnectionManager",
                    backend_type=str(self._backend_type_for_operations()),
                )
            # The probe ran unlocked. Reconcile by identity and generation rather
            # than applying a stale health result to a replacement candidate.
            if (
                self._backend is not backend
                or self._connection_generation != generation
            ):
                return None, None
            if connected:
                return None, None
            self._backend = None
            retired_adapter, retired_source = (
                self._detach_plugin_queue_backend_under_lock()
            )
            # Keep these strong locals alive until this method returns and the
            # state-lock critical section is over; token destructors are plugin code.
            if self._breaker is not None:
                self._breaker = self._breaker.new_generation()
            self._connection_generation += 1
            return backend, self._connection_generation

    def _run_connect_transaction(self, monitor_events: list[_MonitorEvent]) -> None:
        """Serialize connection work, but disconnect stale generations unlocked."""
        while True:
            with self._connect_lock:
                stale_backend, stale_generation = self._connect_with_retries(
                    monitor_events,
                    defer_stale_disconnect=True,
                )
            if stale_backend is None:
                return

            # Driver teardown is arbitrary application I/O.  In particular it may
            # recursively call connect() or close(); neither may wait on this
            # manager's non-reentrant connect lock.
            stale_disconnect_failed = False
            try:
                stale_backend.disconnect()
            except Exception:
                stale_disconnect_failed = True
            if stale_disconnect_failed:
                _log_diagnostic(logger.warning, "Error disconnecting stale backend")
            monitor_events.append(
                ("on_disconnect", (str(self._backend_type_for_operations()), None))
            )
            with self._lock:
                if self._retired:
                    raise BackendConnectionError(
                        "ConnectionManager was released while reconnecting",
                        backend_type=str(self._backend_type_for_operations()),
                    )
                # A recursive/concurrent connect may have published a fresh
                # generation while disconnect ran. The next serialized pass then
                # health-checks that exact candidate instead of overwriting it.
                if (
                    stale_generation is not None
                    and self._connection_generation != stale_generation
                ):
                    continue

    def _connect_with_retries(
        self,
        monitor_events: list[_MonitorEvent],
        *,
        defer_stale_disconnect: bool = False,
    ) -> tuple[Backend | None, int | None]:
        """Run one transaction and optionally return stale work to its owner."""
        stale_backend, stale_generation = self._detach_stale_backend()
        if stale_backend is not None:
            if defer_stale_disconnect:
                return stale_backend, stale_generation
            stale_disconnect_failed = False
            try:
                stale_backend.disconnect()
            except Exception:
                stale_disconnect_failed = True
            if stale_disconnect_failed:
                _log_diagnostic(logger.warning, "Error disconnecting stale backend")
            monitor_events.append(
                ("on_disconnect", (str(self._backend_type_for_operations()), None))
            )
        with self._lock:
            if self._retired:
                raise BackendConnectionError(
                    "Cannot connect a released ConnectionManager",
                    backend_type=str(self._backend_type_for_operations()),
                )
            if self._backend is not None:
                return None, None

        retry_attempts, retry_delay = self._retry_policy()
        total_attempts = retry_attempts + 1
        # Scheduler-facing synchronous APIs cannot yield while the manager is
        # reconnecting. Bound only the retry *wait* here; the selected backend's
        # own socket/RPC timeout remains responsible for bounding one attempt.
        retry_deadline = time.monotonic() + self._reactor_io_timeout()

        failed_attempt = False
        for attempt in range(total_attempts):
            attempt_failed = False
            try:
                self._attempt_connection()
            except (ConfigurationError, ValidationError, ImportError):
                # Invalid settings and missing optional dependencies cannot recover via
                # network backoff. Preserve their actionable exception and avoid
                # constructing/sleeping through the remaining retry attempts.
                raise
            except Exception:
                # Intentional broad catch: any backend connection error should trigger retry.
                # Backend-specific exceptions (RedisError, PyMongoError, KafkaError, AMQPError)
                # all inherit from Exception. KeyboardInterrupt/SystemExit inherit from
                # BaseException (not Exception), so ``except Exception`` does NOT catch them
                # — they propagate out of the retry loop naturally. (A prior ``isinstance(e,
                # (KeyboardInterrupt, SystemExit)): raise`` here was unreachable dead code:
                # nothing caught by ``except Exception`` can be an instance of either.)
                failed_attempt = True
                attempt_failed = True
                with self._lock:
                    retired = self._retired
                if retired:
                    # A concurrent release won the race. Preserve the active typed
                    # release error instead of replacing it with retry exhaustion.
                    raise

            if attempt_failed:
                # The driver exception is no longer active here.  Keep continuation
                # telemetry fixed and do not expose a backend failure via a custom
                # logging handler's ``sys.exc_info()``.
                _log_diagnostic(
                    logger.warning,
                    "Connection attempt failed.",
                )
                if attempt < retry_attempts:
                    # Record each retry while its transaction is serialized. User
                    # callbacks are dispatched only after ``_connect_lock`` is released;
                    # ``attempt`` here is the 0-based just-failed index, so the public
                    # retry number is 1-based.
                    monitor_events.append(
                        (
                            "on_retry",
                            (str(self._backend_type_for_operations()), attempt + 1),
                        )
                    )
                    remaining = retry_deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    interrupted = _wait_for_retry_backoff(
                        self._retirement_event,
                        min(
                            remaining,
                            compute_full_jitter_backoff(attempt, retry_delay),
                        ),
                    )
                    if interrupted:
                        break
                continue

            _log_diagnostic(
                logger.debug, "Connected to %s", self._backend_type_for_operations()
            )
            # Preserve transaction order, then dispatch outside ``_connect_lock``.
            monitor_events.append(
                ("on_connect", (str(self._backend_type_for_operations()),))
            )
            return None, None

        if failed_attempt:
            attempt_word = "attempt" if total_attempts == 1 else "attempts"
            raise BackendConnectionError(
                f"Failed to connect after {total_attempts} {attempt_word}.",
                backend_type=str(self._backend_type_for_operations()),
            )
        return None, None

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
        backend_type = self._backend_type_for_operations()
        settings = self._settings_for_operations()
        descriptor = get_descriptor(
            backend_type.value
            if isinstance(backend_type, BackendType)
            else backend_type
        )
        settings_cls = _load_descriptor_object(descriptor, descriptor.settings_cls_path)
        backend_field_names = _model_field_names(settings_cls)

        raw_attempts = settings.get(
            _CONNECTION_MANAGER_INTERNAL_KEYS["retry_attempts"],
            settings.get(
                _CONNECTION_MANAGER_DIRECT_KEYS["retry_attempts"],
                (
                    _CONNECTION_MANAGER_DEFAULTS["retry_attempts"]
                    if "retry_attempts" in backend_field_names
                    else settings.get(
                        "retry_attempts", _CONNECTION_MANAGER_DEFAULTS["retry_attempts"]
                    )
                ),
            ),
        )
        retry_attempts: int | None = None
        if type(raw_attempts) is int:
            retry_attempts = raw_attempts
        elif type(raw_attempts) is str and (
            raw_attempts == "0"
            or (
                1 <= len(raw_attempts) <= 2
                and "1" <= raw_attempts[0] <= "9"
                and raw_attempts.isascii()
                and raw_attempts.isdecimal()
            )
        ):
            # ``raw_attempts`` is an exact ``str`` containing at most two ASCII
            # digits, so conversion cannot invoke user code or hit Python's
            # integer-string length limit. Alternate spellings (signs, whitespace,
            # leading zeroes, decimal/exponent notation) are deliberately rejected.
            retry_attempts = int(raw_attempts)
        if retry_attempts is None:
            policy_error = ConfigurationError(
                "retry_attempts must be an integer between 0 and 20",
                setting_name="retry_attempts",
            )
            del raw_attempts
            raise policy_error
        if not 0 <= retry_attempts <= 20:
            policy_error = ConfigurationError(
                "retry_attempts must be between 0 and 20",
                setting_name="retry_attempts",
            )
            del raw_attempts
            raise policy_error

        raw_delay = settings.get(
            _CONNECTION_MANAGER_INTERNAL_KEYS["retry_delay"],
            settings.get(
                _CONNECTION_MANAGER_DIRECT_KEYS["retry_delay"],
                (
                    _CONNECTION_MANAGER_DEFAULTS["retry_delay"]
                    if "retry_delay" in backend_field_names
                    else settings.get(
                        "retry_delay", _CONNECTION_MANAGER_DEFAULTS["retry_delay"]
                    )
                ),
            ),
        )
        retry_delay: float | None = None
        invalid_delay = False
        try:
            if isinstance(raw_delay, bool):
                raise ValueError
            retry_delay = float(raw_delay)
        except Exception:  # noqa: BLE001 - custom numeric coercion is untrusted
            invalid_delay = True
        if invalid_delay:
            policy_error = ConfigurationError(
                "retry_delay must be a finite non-negative number",
                setting_name="retry_delay",
            )
            del raw_delay
            raise policy_error
        assert retry_delay is not None
        if not math.isfinite(retry_delay) or retry_delay < 0:
            policy_error = ConfigurationError(
                "retry_delay must be a finite non-negative number",
                setting_name="retry_delay",
            )
            del raw_delay
            raise policy_error
        return retry_attempts, retry_delay

    def _reactor_io_timeout(self) -> float:
        """Read the finite manager retry-wait budget.

        This is deliberately separate from ``_retry_policy`` so existing
        third-party callers/tests that consume its two-value return contract do
        not change. The setting is also consumed by the Deferred adapters for
        lifecycle and pipeline calls.
        """
        settings = self._settings_for_operations()
        raw_timeout = settings.get(
            _CONNECTION_MANAGER_INTERNAL_KEYS["reactor_io_timeout"],
            settings.get(
                _CONNECTION_MANAGER_DIRECT_KEYS["reactor_io_timeout"],
                settings.get(
                    "reactor_io_timeout",
                    _CONNECTION_MANAGER_DEFAULTS["reactor_io_timeout"],
                ),
            ),
        )
        try:
            if isinstance(raw_timeout, bool):
                raise ValueError
            timeout = float(raw_timeout)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ConfigurationError(
                "reactor_io_timeout must be finite and between 0 and 60 seconds",
                setting_name="SCRAPY_REACTOR_IO_TIMEOUT",
            ) from exc
        if not math.isfinite(timeout) or not 0 < timeout <= MAX_REACTOR_IO_TIMEOUT_S:
            raise ConfigurationError(
                "reactor_io_timeout must be finite and between 0 and 60 seconds",
                setting_name="SCRAPY_REACTOR_IO_TIMEOUT",
            )
        return timeout

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
                    backend_type=str(self._backend_type_for_operations()),
                )
        backend = self._create_backend()
        try:
            backend.connect()
        except BaseException:
            try:
                backend.disconnect()
            except BaseException:
                # Cleanup must never replace the original failed connection signal.
                pass
            with self._lock:
                retired_adapter, retired_source = (
                    self._detach_plugin_queue_backend_under_lock()
                )
            del retired_adapter, retired_source
            raise
        with self._lock:
            if not self._retired:
                self._backend = backend
                self._connection_generation += 1
                return

        # The final holder released while backend.connect() was in flight. Dispose
        # the successful handle instead of resurrecting an evicted manager.
        disconnect_failed = False
        try:
            backend.disconnect()
        except BaseException:  # noqa: BLE001 - teardown remains best-effort
            # This teardown is best-effort.  In particular, a control-flow signal
            # from a broken backend must not replace the typed error which explains
            # why the successful connection was discarded.
            disconnect_failed = True
        if disconnect_failed:
            # Leave the cleanup handler before diagnostics. A custom logging handler
            # is application code and must not be able to inspect the raw teardown
            # exception through ``sys.exc_info()``.
            _log_diagnostic(logger.warning, "Error disconnecting released backend")
        raise BackendConnectionError(
            "Connection completed after ConnectionManager release; backend discarded",
            backend_type=str(self._backend_type_for_operations()),
        )

    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
    )
    def close(self) -> None:
        """Release one legacy acquire, preserving the historical API."""
        cls = type(self)
        if self._registry_token is None:
            # Preserve direct-constructor validation and teardown behavior.
            cls._registry_key(self.backend_type, self.settings)
            with cls._registry_lock:
                if self._active_acquires:
                    return
                self._retired = True
                self._retirement_event.set()
            try:
                self._finalize_retirement()
            except BaseException:
                cls._retain_failed_manager(self)
                raise
            return

        should_finalize = False
        try:
            with cls._registry_lock:
                # Claim and consume the exact legacy token in one registry
                # transaction. Two concurrent close() calls therefore cannot both
                # select the same token and accidentally leave a distinct legacy
                # acquire active.
                legacy_token = (
                    self._legacy_acquires[0] if self._legacy_acquires else None
                )
                if legacy_token is not None:
                    should_finalize = self._release_acquire_under_lock(legacy_token)
                elif self._retired:
                    # Repair a prior final-release interruption without consuming a
                    # peer.
                    should_finalize = not self._retirement_complete
        except BaseException:
            cls._retain_failed_manager(self)
            raise
        if should_finalize:
            try:
                self._finalize_retirement()
            except BaseException:
                cls._retain_failed_manager(self)
                raise

    def _is_acquire_released(self, acquire_token: object) -> bool:
        """Return whether one opaque acquire token is no longer authoritative."""
        with type(self)._registry_lock:
            return acquire_token not in self._active_acquires

    def _release_acquire_under_lock(self, acquire_token: object) -> bool:
        """Consume one exact token; called only while the registry lock is held."""
        cls = type(self)
        self._active_acquires.discard(acquire_token)
        # A token-aware release can race legacy cleanup only through misuse;
        # keep the compatibility queue synchronized without relying on it.
        try:
            self._legacy_acquires.remove(acquire_token)
        except ValueError:
            pass
        for thread_id, handoff in tuple(self._legacy_acquire_handoffs.items()):
            self._legacy_acquire_handoffs[thread_id] = [
                token for token in handoff if token is not acquire_token
            ]
            if not self._legacy_acquire_handoffs[thread_id]:
                self._legacy_acquire_handoffs.pop(thread_id, None)
        self._users = len(self._active_acquires)
        if self._active_acquires:
            return False
        self._retired = True
        self._retirement_event.set()
        registry_token = self._registry_token
        if registry_token is not None and cls._managers.get(registry_token) is self:
            cls._managers.pop(registry_token, None)
        return not self._retirement_complete

    def _release_acquire(self, acquire_token: object) -> None:
        """Release one exact token and repair final retirement when necessary."""
        cls = type(self)
        with cls._registry_lock:
            should_finalize = self._release_acquire_under_lock(acquire_token)
        if should_finalize:
            self._finalize_retirement()

    def _publish_retirement_complete(self) -> None:
        """Publish package-owned retirement state in one retryable lock pass."""
        with self._lock:
            self._retirement_complete = True
            self._retirement_finalizing = False
            self._retirement_finalizer_thread_id = None
            self._retirement_finalizer_token = None
            self._retirement_finalization_event.set()
            self._retiring_backend = None
            retired_adapter = self._retiring_adapter
            retired_source = self._retiring_adapter_source
            self._retiring_adapter = None
            self._retiring_adapter_source = None
        type(self)._forget_failed_manager(self)
        # Plugin token destruction must remain outside the manager lock.
        del retired_adapter, retired_source

    def _finalize_retirement(self) -> None:
        """Complete one manager retirement without replaying opaque teardown."""
        backend_to_disconnect: Backend | None = None
        wait_for_finalizer: threading.Event | None = None
        finalizer_token = _RetirementFinalizerToken()
        with self._lock:
            self._retired = True
            self._retirement_event.set()
            if self._retirement_complete:
                # A prior publication may have been interrupted after its first
                # assignment. Repair every waiter/ownership field before returning.
                self._retirement_finalizing = False
                self._retirement_finalizer_thread_id = None
                self._retirement_finalizer_token = None
                self._retirement_finalization_event.set()
                return
            current_owner = self._retirement_finalizer_token
            if (
                self._retirement_finalizing
                and current_owner is not None
                and current_owner.active
            ):
                if current_owner.thread_id == finalizer_token.thread_id:
                    # Opaque teardown re-entry observes retirement in progress; the
                    # outer owner will publish completion before returning.
                    return
                wait_for_finalizer = self._retirement_finalization_event
            else:
                # No live owner remains (for example, package-state publication was
                # interrupted). Reclaim the existing event rather than replacing it,
                # so already-waiting peers are never stranded.
                self._retirement_finalizing = True
                self._retirement_finalizer_thread_id = finalizer_token.thread_id
                self._retirement_finalizer_token = finalizer_token
            if wait_for_finalizer is None:
                if self._retiring_backend is None and self._backend is not None:
                    self._retiring_backend = self._backend
                    self._backend = None
                    self._connection_generation += 1
                if self._retiring_adapter is None:
                    (
                        self._retiring_adapter,
                        self._retiring_adapter_source,
                    ) = self._detach_plugin_queue_backend_under_lock()
                if not self._retirement_disconnect_started:
                    self._retirement_disconnect_started = True
                    backend_to_disconnect = self._retiring_backend
                if self._breaker is not None:
                    self._breaker.reset()

        if wait_for_finalizer is not None:
            # The owner can unwind after this contender observes it but before it
            # publishes the event. Periodically re-check frame-scoped liveness so
            # a one-shot control exception cannot strand every later releaser.
            while not wait_for_finalizer.wait(timeout=0.01):
                with self._lock:
                    if self._retirement_complete:
                        return
                    current_owner = self._retirement_finalizer_token
                    if (
                        self._retirement_finalizing
                        and current_owner is not None
                        and current_owner.active
                    ):
                        continue
                    self._retirement_finalizing = True
                    self._retirement_finalizer_thread_id = finalizer_token.thread_id
                    self._retirement_finalizer_token = finalizer_token
                    wait_for_finalizer = None
                    if self._retiring_backend is None and self._backend is not None:
                        self._retiring_backend = self._backend
                        self._backend = None
                        self._connection_generation += 1
                    if self._retiring_adapter is None:
                        (
                            self._retiring_adapter,
                            self._retiring_adapter_source,
                        ) = self._detach_plugin_queue_backend_under_lock()
                    if not self._retirement_disconnect_started:
                        self._retirement_disconnect_started = True
                        backend_to_disconnect = self._retiring_backend
                    if self._breaker is not None:
                        self._breaker.reset()
                    break
            if wait_for_finalizer is not None:
                return

        disconnect_failed = False
        control_error: BaseException | None = None
        if backend_to_disconnect is not None:
            try:
                backend_to_disconnect.disconnect()
            except Exception:
                disconnect_failed = True
            except BaseException as error:
                # Opaque disconnect effects cannot be replayed exactly. Publish the
                # package-owned retirement state, then preserve control flow.
                control_error = error

        publication_error: BaseException | None = None
        try:
            self._publish_retirement_complete()
        except BaseException as error:
            publication_error = error
            # The bounded guarantee permits one interruption of package-owned
            # publication. One repair pass completes event/finalizer ownership;
            # a second interruption remains retryable by a later release.
            self._publish_retirement_complete()

        # Hostile monitor callbacks run only after all manager state is terminal.
        if disconnect_failed:
            _log_diagnostic(logger.warning, "Error during disconnect")
        elif backend_to_disconnect is not None:
            _log_diagnostic(
                logger.debug,
                "Disconnected from %s",
                self._backend_type_for_operations(),
            )
        if backend_to_disconnect is not None:
            self._notify_monitor(
                "on_disconnect", str(self._backend_type_for_operations()), None
            )
            self._notify_monitor(
                "on_disconnect_result",
                str(self._backend_type_for_operations()),
                not disconnect_failed and control_error is None,
            )
        if control_error is not None:
            raise control_error
        if publication_error is not None:
            raise publication_error

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
                manager._active_acquires.clear()
                manager._legacy_acquires.clear()
                manager._legacy_acquire_handoffs.clear()
                manager._users = 0
                manager._retired = True
                manager._retirement_event.set()
            # Invalidate candidates that began before this clear boundary and wake
            # every waiter so a fresh generation can elect its own constructor.
            cls._registry_epoch += 1
            construction_attempts = list(cls._manager_constructions.values())
            cls._manager_constructions.clear()
            for attempt in construction_attempts:
                attempt.event.set()
            # Owner thread ids intentionally remain marked until their old
            # constructors return; constructor callbacks must still fail re-entry.
            # Reset the one-shot over-cap warning so a fresh test suite run
            # re-warns if it overflows the cap (otherwise the warning is
            # permanently suppressed after the first overflow across tests).
            cls._over_cap_warned = False
        for manager in managers:
            try:
                manager._finalize_retirement()
            except BaseException:
                pass

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
        monitor_failed = False
        try:
            getattr(self._monitor, hook_name)(*args)
        except Exception:
            monitor_failed = True

        if monitor_failed:
            # The callback's exception handler has ended, so the diagnostic cannot
            # expose monitor internals through a logging handler's ``sys.exc_info``.
            _log_diagnostic(
                logger.debug,
                "Connection monitor callback raised; ignored.",
            )

    def _dispatch_monitor_events(
        self,
        events: list[_MonitorEvent],
        lazy_attempt: _ConnectionAttempt | None = None,
    ) -> None:
        """Dispatch ordered lifecycle events after manager locks are released.

        Direct calls keep the original behavior.  A lazy owner's callback gets a
        thread-local view of its already-published result so reentry cannot join
        an owner that is currently waiting for the callback to return.
        """
        previous_attempt = self._lazy_connection_context.dispatch_attempt
        self._lazy_connection_context.dispatch_attempt = lazy_attempt
        try:
            for hook_name, args in events:
                self._notify_monitor(hook_name, *args)
        finally:
            self._lazy_connection_context.dispatch_attempt = previous_attempt

    @property
    @_manager_terminal_error_boundary()
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        pass_through_exception_types=(BackendConnectionError, ImportError),
    )
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
        - The owner runs ``connect()`` (which performs an interruptible wait
          between retry attempts) WITHOUT holding ``_lock``. This is the load-bearing
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
        reentrant_backend = self._backend_from_reentrant_lazy_monitor()
        if reentrant_backend is not None:
            return reentrant_backend

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
                        backend_type=str(self._backend_type_for_operations()),
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
                if (
                    current_attempt is None
                ):  # Defensive: _connecting implies an attempt.
                    continue
                attempt = current_attempt

            # Wait for the owner to resolve connect() — without holding _lock.
            attempt.event.wait()
            if attempt.error is not None:
                raise _rebuild_connect_attempt_error(attempt.error)

        # Owner path: connect WITHOUT holding _lock so the retry loop's
        # interruptible backoff does not block peer threads (A2).
        connect_error: BaseException | None = None
        self._lazy_connection_context.owner_attempt = attempt
        try:
            self.connect()
        except BaseException as e:  # noqa: BLE001 - re-signal to all waiters
            connect_error = e
        finally:
            self._lazy_connection_context.owner_attempt = None

        # The real ``connect()`` path publishes before its monitor callbacks; a
        # mocked override does not know about that internal handoff, so complete
        # defensively here as well.  The attempt event makes this idempotent and
        # returns the exact safe terminal result that all already-waiting peers
        # received.
        connect_error = self._complete_lazy_connection_attempt(
            attempt,
            connect_error,
        )

        if connect_error is not None:
            raise _rebuild_connect_attempt_error(connect_error)

        with self._lock:
            # Capture the published handle exactly once under the same state lock
            # used by reconnect/close. Returning a second ``self._backend`` read
            # allowed reconnect to detach it after a non-null guard, leaking None
            # through this property and into interface-proxy construction.
            published_backend = self._backend

        # Keep the local-value guard explicit so the contract remains true under
        # future refactors and ``python -O`` as well as in the static type system.
        if published_backend is None:
            msg = "connect() did not produce a backend"
            raise BackendConnectionError(
                msg, backend_type=str(self._backend_type_for_operations())
            )
        return published_backend

    @_manager_terminal_error_boundary()
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        pass_through_exception_types=(BackendConnectionError, ImportError),
    )
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
        """Lazily resolve the per-manager circuit breaker policy.

        Reads the breaker config once (``SCRAPY_CIRCUIT_BREAKER_ENABLED`` +
        threshold + reset-timeout) and caches the result on the instance:

        - When disabled (the default), ``_breaker`` is set to ``None`` and the
          ``get_*_backend()`` methods return the raw backend unchanged —
          byte-identical to pre-breaker behavior, zero proxy overhead.
        - When enabled, a single :class:`CircuitBreaker` is constructed and
          shared by every wrapped interface returned from this manager, so a
          queue+set+storage on the same backend share one failure signal.

        A manager created through ``resolve_backend_config`` receives a parsed,
        private Scrapy policy. Directly constructed managers retain the lazy
        ``Settings`` environment fallback. That fallback runs OUTSIDE
        ``self._lock`` (initiative #15): the env scan is process-global
        idempotent state, not connection-manager state, and this lock is shared
        with ``get_manager()`` / ``close()`` / the A2 slow-path owner gate.

        Returns:
            The manager's breaker, or ``None`` when the feature is disabled.
        """
        if self._breaker_configured:
            return self._breaker
        policy_keys = _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS
        operational_settings = self._settings_for_operations()
        if all(key in operational_settings for key in policy_keys.values()):
            enabled, failure_threshold, reset_timeout = _parse_circuit_breaker_policy(
                operational_settings[policy_keys["enabled"]],
                operational_settings[policy_keys["failure_threshold"]],
                operational_settings[policy_keys["reset_timeout"]],
            )
            from_env_fallback = False
        else:
            # Read the breaker config OUTSIDE self._lock (#15). Imported lazily
            # to avoid a settings-module import cycle at module load time and to
            # keep the direct-construction fallback deferred to first use.
            from scrapy_extension.settings import Settings

            settings = Settings()
            enabled = settings.circuit_breaker_enabled
            failure_threshold = settings.circuit_breaker_failure_threshold
            reset_timeout = settings.circuit_breaker_reset_timeout
            from_env_fallback = True
        with self._lock:
            if self._breaker_configured:
                return self._breaker
            self._install_breaker_locked(
                enabled,
                failure_threshold,
                reset_timeout,
                from_env_fallback=from_env_fallback,
            )
            return self._breaker

    def _install_breaker_locked(
        self,
        enabled: bool,
        failure_threshold: int,
        reset_timeout: float,
        *,
        from_env_fallback: bool,
    ) -> None:
        """Install one breaker policy while the manager lock is held."""
        backend_type = self._backend_type_for_operations()
        bt_key = (
            backend_type.value
            if isinstance(backend_type, BackendType)
            else backend_type
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
        self._breaker_resolved_from_env_fallback = from_env_fallback
        self._breaker_policy_values = (enabled, failure_threshold, reset_timeout)

    def apply_scrapy_breaker_policy(self, settings: Any) -> None:
        """Apply an explicit Scrapy breaker policy without changing pool identity."""
        policy = resolve_circuit_breaker_policy(settings)
        if not policy:
            return
        policy_keys = _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS
        enabled, failure_threshold, reset_timeout = _parse_circuit_breaker_policy(
            policy[policy_keys["enabled"]],
            policy[policy_keys["failure_threshold"]],
            policy[policy_keys["reset_timeout"]],
        )
        policy_values = (enabled, failure_threshold, reset_timeout)
        warn_differing_policy = False
        with self._lock:
            if self._breaker_configured and not (
                self._breaker_resolved_from_env_fallback
            ):
                if (
                    self._breaker_policy_values != policy_values
                    and not self._dropped_breaker_policy_warned
                ):
                    self._dropped_breaker_policy_warned = True
                    warn_differing_policy = True
            else:
                self._install_breaker_locked(
                    enabled,
                    failure_threshold,
                    reset_timeout,
                    from_env_fallback=False,
                )
        if warn_differing_policy:
            _log_diagnostic(
                logger.warning,
                "Dropping a differing circuit breaker policy applied to an "
                "already-resolved shared connection manager; the first explicit "
                "policy remains in effect.",
            )

    @_manager_terminal_error_boundary()
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        pass_through_exception_types=(BackendConnectionError, ImportError),
    )
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
                    backend_type=str(self._backend_type_for_operations()),
                )
            breaker = self._get_breaker()
            with self._lock:
                if self._retired:
                    raise BackendConnectionError(
                        "Cannot access a released ConnectionManager",
                        backend_type=str(self._backend_type_for_operations()),
                    )
                if backend is self._backend and breaker is self._breaker:
                    return backend, breaker

    @_manager_terminal_error_boundary("queue")
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        pass_through_exception_types=(
            BackendConnectionError,
            ImportError,
            NotImplementedError,
        ),
    )
    def get_queue_backend(self) -> QueueBackend:
        """Get the queue backend interface.

        When the circuit breaker is enabled, traffic operations (``push``,
        ``_push_with_durability``, ``pop``, ``pop_with_ack``, ``ack``, and
        ``nack``) are wrapped under the breaker. Administrative and lifecycle
        methods (including ``queue_len``, ``clear_queue``, and ``is_connected``)
        forward unchanged. When disabled (default) the raw backend is returned
        byte-identically.

        Returns:
            The QueueBackend interface of the backend.
        """
        while True:
            backend, breaker = self._get_backend_breaker_snapshot()
            if not isinstance(backend, QueueBackend):
                msg = f"Backend {backend.__class__.__name__} does not support queue operations"
                raise NotImplementedError(msg)
            published_backend: QueueBackend
            if breaker is None:
                published_backend = backend
            else:
                from scrapy_extension.backends.circuit_breaker import wrap_queue_backend

                published_backend = wrap_queue_backend(backend, breaker)
            plugin_ack_capabilities = self._plugin_ack_capabilities
            if (
                plugin_ack_capabilities is None
                or not plugin_ack_capabilities.deferred_ack_plugin
            ):
                return published_backend

            source = (backend, breaker)
            with self._lock:
                if self._retired:
                    raise BackendConnectionError(
                        "Cannot access a released ConnectionManager",
                        backend_type=str(self._backend_type_for_operations()),
                    )
                if backend is not self._backend or breaker is not self._breaker:
                    continue
                cached_source = self._plugin_queue_backend_source
                if (
                    cached_source is not None
                    and cached_source[0] is backend
                    and cached_source[1] is breaker
                ):
                    assert self._plugin_queue_backend is not None
                    return self._plugin_queue_backend
                contract_backend = _DeferredAckPluginQueueBackend(
                    published_backend,
                    supports_concurrent_ack=(
                        plugin_ack_capabilities.supports_concurrent_ack
                    ),
                )
                retired_adapter, retired_source = (
                    self._detach_plugin_queue_backend_under_lock()
                )
                self._plugin_queue_backend_source = source
                self._plugin_queue_backend = contract_backend
            # Never overwrite the cache's last strong references under ``_lock``:
            # adapter teardown may run identity-token destructors that re-enter us.
            del retired_adapter, retired_source
            return contract_backend

    @_durable_push_queue_error_boundary
    @_manager_terminal_error_boundary()
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        sanitize_exception_types=(ValidationError,),
        pass_through_exception_types=(
            BackendConnectionError,
            CircuitBreakerOpenError,
            ImportError,
            QueueError,
        ),
        catch_unexpected=False,
    )
    def _push_queue_with_durability(
        self,
        queue_name: str,
        item: bytes,
        priority: float = 0.0,
        *,
        require_durable: bool = False,
    ) -> _QueuePushReceipt:
        """Push through one exact backend/breaker generation and return its receipt.

        The backend identity and its breaker are snapshotted together before the
        operation.  A concurrent reconnect may retire that generation, but it
        cannot redirect the admitted push to a replacement backend after the
        durability decision has been made.

        A backend's ``_DurablePushRequired`` is an input-policy rejection rather
        than a network failure.  It passes through the breaker uncounted and is
        translated here, outside the breaker, to the queue-facing error contract.
        """
        backend, breaker = self._get_backend_breaker_snapshot()
        if not isinstance(backend, QueueBackend):
            msg = f"Backend {backend.__class__.__name__} does not support queue operations"
            raise NotImplementedError(msg)

        published_backend: QueueBackend
        if breaker is None:
            published_backend = backend
        else:
            from scrapy_extension.backends.circuit_breaker import wrap_queue_backend

            published_backend = wrap_queue_backend(backend, breaker)

        try:
            raw_receipt = published_backend._push_with_durability(
                queue_name,
                item,
                priority,
                require_durable=require_durable,
            )
        except _DurablePushRequired as e:
            raise QueueError(
                "Selected queue backend generation is not worker-crash durable",
                queue_name=queue_name,
                operation="push",
            ) from e

        # A malformed future/plugin override must never promote an unknown value
        # (including truthy sentinels) into commit evidence.
        durable = (
            isinstance(raw_receipt, _QueuePushReceipt)
            and raw_receipt.worker_crash_durable is True
        )
        if require_durable and not durable:
            raise QueueError(
                "Queue backend returned no valid worker-crash durability receipt",
                queue_name=queue_name,
                operation="push",
            )
        return _QueuePushReceipt(worker_crash_durable=durable)

    @_manager_terminal_error_boundary("set")
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        pass_through_exception_types=(
            BackendConnectionError,
            ImportError,
            NotImplementedError,
        ),
    )
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
            msg = (
                f"Backend {backend.__class__.__name__} does not support set operations"
            )
            raise NotImplementedError(msg)
        if breaker is None:
            return backend
        from scrapy_extension.backends.circuit_breaker import wrap_set_backend

        return wrap_set_backend(backend, breaker)

    @_manager_terminal_error_boundary("storage")
    @configuration_error_boundary(
        "Connection manager configuration is invalid.",
        _MANAGER_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_message_predicate=_is_safe_manager_configuration_message,
        pass_through_exception_types=(
            BackendConnectionError,
            ImportError,
            NotImplementedError,
        ),
    )
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


def release_manager_acquire(owner: Any, *, exact: bool = False) -> None:
    """Rollback one factory acquire while preserving the primary failure.

    Release can fail before its token is consumed or after retirement has taken
    effect. The opaque lease/manager APIs are idempotent, so one immediate retry
    is safe and repairs both windows. If both attempts fail, the connection layer
    retains the exact owner for a later registry retry; callers still receive the
    first cleanup error so it cannot replace the factory's primary exception.
    """
    release = owner.release if exact else owner.close
    try:
        release()
    except BaseException as first_error:
        try:
            release()
        except BaseException:
            pass
        raise first_error
