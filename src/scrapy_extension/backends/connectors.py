"""Connection manager for backend connections.

This module provides a lazy singleton connection manager with retry logic
for all backend types.

Round-5 R5-1: the four prior hand-synced registries (``_BACKEND_FACTORIES``
+ ``QUEUE_CAPABLE_BACKENDS`` / ``SET_CAPABLE_BACKENDS`` /
``STORAGE_CAPABLE_BACKENDS``) have been consolidated into the single
:class:`~scrapy_extension.backends.registry.BackendDescriptor` table in
``registry.py``. The capability sets below are backward-compatible,
built-in-only immutable snapshots. They exist so importing this module never
triggers third-party discovery; callers that need installed plugin capabilities
must opt in through :func:`capable_backends` or the registry API.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import json
import logging
import math
import os
import threading
from abc import ABC
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from copy import deepcopy
from datetime import date, datetime, timedelta
from datetime import time as datetime_time
from decimal import Decimal
from difflib import get_close_matches
from enum import Enum
from functools import wraps
from inspect import (
    getattr_static,
    isasyncgen,
    isasyncgenfunction,
    isawaitable,
    iscoroutinefunction,
    isgeneratorfunction,
    signature,
)
from json import JSONEncoder
from pathlib import PurePath
from types import CoroutineType, FunctionType, GeneratorType, ModuleType
from typing import Any, ClassVar, NamedTuple, ParamSpec, TypeVar, cast
from uuid import UUID

from pydantic import BaseModel, SecretBytes, SecretStr, ValidationError

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
    CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S,
    CircuitBreaker,
    CircuitBreakerOpenError,
)
from scrapy_extension.backends.registry import (
    _BUNDLED_DESCRIPTORS,
    BackendDescriptor,
    get_descriptor,
    get_registry,
    has_capability,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    BackendError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.exceptions._redaction import configuration_error_boundary
from scrapy_extension.monitor.base import Monitor, NullMonitor
from scrapy_extension.utils._config import (
    parse_bool_setting,
    parse_float_setting,
    parse_int_setting,
)

logger = logging.getLogger(__name__)

_MonitorEvent = tuple[str, tuple[Any, ...]]
_P = ParamSpec("_P")
_T = TypeVar("_T")


def _wait_for_retry_backoff(
    retirement_event: threading.Event,
    delay: float,
) -> bool:
    """Wait for retry delay, returning early when the manager is retired."""
    return retirement_event.wait(delay)


def _log_diagnostic(
    log_call: Callable[..., object],
    message: str,
    *args: object,
    **kwargs: object,
) -> None:
    """Emit best-effort diagnostics without changing lifecycle control flow.

    Backend operations and monitor callbacks retain their existing exception
    semantics.  Only a logging handler is untrusted here: an application may
    install a handler that raises a control-flow ``BaseException``, but that
    diagnostic must not interrupt an already-selected recovery or teardown path.
    """
    try:
        log_call(message, *args, **kwargs)
    except BaseException:
        pass


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
# Public since BackendSpiderMixin (a Stable component) consumes them; the
# private aliases below remain for internal callers.
CONNECTION_MANAGER_SCOPE_KEY = "__connection_manager_queue_scope"
CONSUMER_SCOPED_BACKENDS: frozenset[str] = frozenset(
    {BackendType.KAFKA.value, BackendType.ROCKETMQ.value}
)
_CONNECTION_MANAGER_SCOPE_KEY = CONNECTION_MANAGER_SCOPE_KEY
_CONSUMER_SCOPED_BACKENDS = CONSUMER_SCOPED_BACKENDS
_CONNECTION_MANAGER_SCRAPY_KEYS: dict[str, str] = {
    "retry_attempts": "SCRAPY_RETRY_ATTEMPTS",
    "retry_delay": "SCRAPY_RETRY_DELAY",
}
_CONNECTION_MANAGER_DEFAULTS: dict[str, int | float] = {
    "retry_attempts": 3,
    "retry_delay": 1.0,
}
_CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS: dict[str, str] = {
    "enabled": "__connection_manager_circuit_breaker_enabled",
    "failure_threshold": "__connection_manager_circuit_breaker_failure_threshold",
    "reset_timeout": "__connection_manager_circuit_breaker_reset_timeout",
}
_CONNECTION_MANAGER_CIRCUIT_BREAKER_SCRAPY_KEYS: dict[str, str] = {
    "enabled": "SCRAPY_CIRCUIT_BREAKER_ENABLED",
    "failure_threshold": "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
    "reset_timeout": "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT",
}
_CONNECTION_MANAGER_CIRCUIT_BREAKER_DEFAULTS: dict[str, bool | int | float] = {
    "enabled": False,
    "failure_threshold": 5,
    "reset_timeout": 30.0,
}
_CONNECTION_MANAGER_BACKEND_EXCLUDED_KEYS: frozenset[str] = frozenset(
    {
        *_CONNECTION_MANAGER_INTERNAL_KEYS.values(),
        *_CONNECTION_MANAGER_DIRECT_KEYS.values(),
        *_CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS.values(),
        _CONNECTION_MANAGER_SCOPE_KEY,
    }
)
_MANAGER_CONFIGURATION_SETTING_NAMES: frozenset[str] = frozenset(
    {
        "SCRAPY_BACKEND_TYPE",
        "SCRAPY_CIRCUIT_BREAKER_ENABLED",
        "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT",
        "api_key",
        "backend_settings",
        "retry_attempts",
        "retry_delay",
    }
)
_RESOLVED_BACKEND_SETTING_NAMES: frozenset[str] = frozenset(
    {
        "SCRAPY_BACKEND_TYPE",
        "SCRAPY_QUEUE_BACKEND_TYPE",
        "SCRAPY_SET_BACKEND_TYPE",
        "SCRAPY_STORAGE_BACKEND_TYPE",
        "SCRAPY_CIRCUIT_BREAKER_ENABLED",
        "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT",
        "backend_settings",
    }
)
_SAFE_BACKEND_SETTING_HINTS: frozenset[str] = frozenset(
    {
        "database",
        "SCRAPY_MONGO_DATABASE",
    }
)
_SAFE_MANAGER_MESSAGES: frozenset[str] = frozenset(
    {
        "Invalid backend setting 'backend_settings'.",
        "Selected backend could not be constructed.",
        "Selected backend has an invalid plugin class path.",
        "Selected backend must provide callable backend and settings classes.",
        "Selected third-party queue backend has an invalid acknowledgement contract.",
        (
            "Selected backend type is not a registered backend type. "
            f"Valid bundled values: {', '.join(repr(name) for name in sorted(_BUNDLED_BACKEND_TYPES))}."
        ),
    }
)
_SAFE_MANAGER_CONNECTION_MESSAGES: frozenset[str] = frozenset(
    {
        "Cannot connect a released ConnectionManager",
        "Cannot access a released ConnectionManager",
        "ConnectionManager was released while connecting",
        "connect() did not produce a backend",
        "Connection completed after ConnectionManager release; backend discarded",
    }
)
_SAFE_BACKEND_SETTING_MESSAGES: frozenset[str] = frozenset(
    {
        "Unknown bundled backend setting.",
        *(
            f"Unknown bundled backend setting. Did you mean {hint!r}?"
            for hint in _SAFE_BACKEND_SETTING_HINTS
        ),
    }
)


def _is_safe_capability_message(message: str) -> bool:
    """Accept only deterministic capability diagnostics with bundled names."""
    if len(message) > 512:
        return False
    prefix = "Selected "
    separator = " does not support the "
    suffix = " interface and is missing capabilities. Capable bundled backends: "
    if (
        not message.startswith(prefix)
        or separator not in message
        or suffix not in message
    ):
        return False
    selected, remainder = message[len(prefix) :].split(separator, maxsplit=1)
    if suffix not in remainder:
        return False
    component, rendered_backends = remainder.split(suffix, maxsplit=1)
    if selected not in _BUNDLED_BACKEND_TYPES | {"third-party backend"}:
        return False
    if component not in {"queue", "set", "storage"} or not rendered_backends.endswith(
        "."
    ):
        return False
    try:
        capable = ast.literal_eval(rendered_backends[:-1])
    except (SyntaxError, ValueError):
        return False
    return (
        type(capable) is list
        and capable == sorted(capable)
        and all(
            type(name) is str and name in _BUNDLED_BACKEND_TYPES for name in capable
        )
    )


def _is_safe_resolved_backend_message(message: str) -> bool:
    """Allow only static typo hints or capability diagnostics at this boundary."""
    return message in _SAFE_BACKEND_SETTING_MESSAGES or _is_safe_capability_message(
        message
    )


def _is_safe_manager_configuration_message(message: str) -> bool:
    """Allow static plugin-boundary diagnostics without trusting plugin text."""
    if message in _SAFE_MANAGER_MESSAGES:
        return True
    setting_prefix = "Invalid backend setting '"
    if message.startswith(setting_prefix) and message.endswith("'."):
        setting_name = message[len(setting_prefix) : -2]
        return setting_name in _MANAGER_CONFIGURATION_SETTING_NAMES
    contract_prefix = "Selected "
    contract_separator = " does not implement its declared contract: missing "
    if (
        not message.startswith(contract_prefix)
        or contract_separator not in message
        or not message.endswith(".")
    ):
        return False
    selected, rendered_interfaces = message[len(contract_prefix) : -1].split(
        contract_separator,
        maxsplit=1,
    )
    if selected not in _BUNDLED_BACKEND_TYPES | {"third-party backend"}:
        return False
    interfaces = rendered_interfaces.split(", ")
    allowed_interfaces = {"Backend", "QueueBackend", "SetBackend", "StorageBackend"}
    return bool(interfaces) and all(name in allowed_interfaces for name in interfaces)


def _model_field_names(settings_cls: Any) -> frozenset[str]:
    """Return declared field names without trusting plugin metadata access."""
    try:
        fields = getattr(settings_cls, "model_fields", None)
        if not isinstance(fields, Mapping):
            return frozenset()
        return frozenset(name for name in fields if type(name) is str)
    except Exception:  # noqa: BLE001 - third-party metadata is untrusted
        # Field names are used only to preserve retry-setting ownership. A broken
        # plugin declaration must fail closed rather than publish its diagnostic.
        return frozenset()


def _load_descriptor_object(descriptor: BackendDescriptor, dotted_path: str) -> Any:
    """Load a descriptor object without exposing third-party loader details.

    Bundled optional dependencies intentionally retain their established
    ``ImportError`` behavior. Third-party descriptors are untrusted metadata,
    so any loader failure becomes one static configuration error after the
    handler has finished and cannot retain a plugin path or import diagnostic.
    """
    loaded: Any = None
    plugin_load_failed = False
    try:
        loaded = _load_object(dotted_path)
    except ImportError:
        if descriptor.backend_type in _BUNDLED_BACKEND_TYPES:
            raise
        plugin_load_failed = True
    except Exception:  # noqa: BLE001 - third-party loader diagnostics are private
        if descriptor.backend_type in _BUNDLED_BACKEND_TYPES:
            raise
        plugin_load_failed = True
    if plugin_load_failed:
        raise ConfigurationError(
            "Selected backend has an invalid plugin class path.",
            setting_name="SCRAPY_BACKEND_TYPE",
        )
    return loaded


class _BundledOptionalDependencyFailure(Exception):
    """Private resolver sentinel with no loader diagnostic or traceback chain."""


def _bundled_optional_dependency_boundary(
    function: Callable[_P, _T],
) -> Callable[_P, _T]:
    """Publish a fresh static ImportError after resolver frames have unwound."""

    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        dependency_failure = False
        try:
            return function(*args, **kwargs)
        except _BundledOptionalDependencyFailure:
            dependency_failure = True
        except BaseException:
            del args
            del kwargs
            raise
        assert dependency_failure
        sanitized_error = ImportError(
            "A bundled backend optional dependency is unavailable."
        )
        del args
        del kwargs
        del dependency_failure
        raise sanitized_error

    return wrapped


def _load_static_ack_capabilities(
    descriptor: BackendDescriptor,
) -> tuple[bool, bool]:
    """Load and sanitize exact class-level acknowledgement declarations."""
    backend_cls = _load_descriptor_object(descriptor, descriptor.backend_cls_path)
    requires_ack = getattr_static(backend_cls, "requires_ack", False)
    supports_concurrent = getattr_static(
        backend_cls,
        "supports_concurrent_ack",
        False,
    )
    if type(requires_ack) is not bool or type(supports_concurrent) is not bool:
        raise _invalid_plugin_ack_contract()
    return requires_ack, supports_concurrent


def _load_resolver_settings_class(descriptor: BackendDescriptor) -> Any:
    """Load a resolver model while tagging only bundled missing dependencies."""
    try:
        return _load_descriptor_object(descriptor, descriptor.settings_cls_path)
    except ImportError:
        if descriptor.backend_type in _BUNDLED_BACKEND_TYPES:
            raise _BundledOptionalDependencyFailure from None
        raise


def _safe_manager_setting_name(
    error: Exception,
    backend_type: str,
    field_names: frozenset[str],
) -> str:
    """Return a verified bundled field name without retaining error input."""
    if backend_type not in _BUNDLED_BACKEND_TYPES:
        return "backend_settings"
    candidate: object = None
    if type(error) is ConfigurationError:
        candidate = error.setting_name
    elif type(error) is ValidationError:
        try:
            errors = error.errors()
            detail = errors[0] if type(errors) is list and errors else None
            location = detail.get("loc", ()) if type(detail) is dict else ()
            candidate = location[0] if type(location) is tuple and location else None
        except Exception:  # noqa: BLE001 - plugin error renderers are untrusted
            return "backend_settings"
    if type(candidate) is str and candidate in field_names:
        return candidate
    return "backend_settings"


_CAPABILITY_INTERFACES: dict[str, type[ABC]] = {
    "queue": QueueBackend,
    "set": SetBackend,
    "storage": StorageBackend,
}


def _invalid_plugin_ack_contract() -> ConfigurationError:
    """Build the static fail-closed error for untrusted ACK declarations."""
    return ConfigurationError(
        "Selected third-party queue backend has an invalid acknowledgement contract.",
        setting_name="SCRAPY_BACKEND_TYPE",
    )


class _PluginAckCapabilitySnapshot(NamedTuple):
    """Immutable exact booleans accepted from one plugin class generation."""

    requires_ack: bool
    supports_concurrent_ack: bool
    deferred_ack_plugin: bool


def _validate_plugin_ack_class(
    descriptor: BackendDescriptor,
    backend_cls: object,
) -> _PluginAckCapabilitySnapshot:
    """Validate and snapshot deferred-ACK metadata before plugin broker I/O.

    Legacy queue plugins inherit ``requires_ack=False`` and remain compatible.
    A plugin opting into deferred acknowledgement must use literal boolean
    metadata and concrete token-bearing methods rather than the permissive ABC
    defaults. The immutable result pins the flags for the manager's lifetime.
    """
    if "queue" not in descriptor.capabilities:
        return _PluginAckCapabilitySnapshot(False, False, False)
    # A queue descriptor is executable configuration, but its target must be a
    # concrete class from the queue ABC hierarchy.  Reject factories and callable
    # instances before reading ACK metadata or invoking the object: even atomic-pop
    # plugins (``requires_ack=False``) participate in queue contract dispatch.
    if not isinstance(backend_cls, type) or not issubclass(backend_cls, QueueBackend):
        raise _invalid_plugin_ack_contract()
    requires_ack = getattr_static(backend_cls, "requires_ack", None)
    if type(requires_ack) is not bool:
        raise _invalid_plugin_ack_contract()
    supports_concurrent = getattr_static(
        backend_cls,
        "supports_concurrent_ack",
        None,
    )
    if requires_ack is False:
        if (
            isinstance(backend_cls, type)
            and issubclass(backend_cls, QueueBackend)
            and type(supports_concurrent) is not bool
        ):
            raise _invalid_plugin_ack_contract()
        return _PluginAckCapabilitySnapshot(
            requires_ack,
            supports_concurrent if type(supports_concurrent) is bool else False,
            False,
        )
    if not isinstance(backend_cls, type) or not issubclass(backend_cls, QueueBackend):
        raise _invalid_plugin_ack_contract()
    if type(supports_concurrent) is not bool:
        raise _invalid_plugin_ack_contract()
    method_calls: dict[str, tuple[tuple[object, ...], dict[str, object]]] = {
        "pop_with_ack": ((object(), "queue", 0.0), {}),
        "ack": ((object(), "queue"), {"token": object()}),
        "nack": ((object(), "queue"), {"token": object()}),
    }
    for method_name, (args, kwargs) in method_calls.items():
        method = getattr_static(backend_cls, method_name, None)
        default_method = getattr_static(QueueBackend, method_name, None)
        if (
            type(method) is not FunctionType
            or method is default_method
            or getattr_static(method, "__isabstractmethod__", False) is True
            or iscoroutinefunction(method)
            or isasyncgenfunction(method)
            or isgeneratorfunction(method)
        ):
            raise _invalid_plugin_ack_contract()
        try:
            signature(method).bind(*args, **kwargs)
        except (TypeError, ValueError):
            raise _invalid_plugin_ack_contract() from None
    return _PluginAckCapabilitySnapshot(
        requires_ack,
        supports_concurrent,
        True,
    )


def _validate_backend_contract(
    backend: object, descriptor: BackendDescriptor
) -> Backend:
    """Verify a selected backend fulfils its advertised runtime contract.

    Registry discovery intentionally stores dotted paths only, so a plugin can
    remain lazy until it is selected.  This is the corresponding first-use
    boundary: after construction, its lifecycle and every declared capability
    must be backed by the project's ABCs.  A descriptor is configuration, not
    an assertion to trust blindly.
    """
    missing: list[str] = []
    if not isinstance(backend, Backend):
        missing.append("Backend")
    for capability in sorted(descriptor.capabilities):
        interface = _CAPABILITY_INTERFACES[capability]
        if not isinstance(backend, interface):
            missing.append(interface.__name__)
    if missing:
        backend_label = (
            descriptor.backend_type
            if descriptor.backend_type in _BUNDLED_BACKEND_TYPES
            else "Selected third-party backend"
        )
        msg = (
            f"{backend_label} does not implement its declared "
            f"contract: missing {', '.join(missing)}."
        )
        raise ConfigurationError(msg, setting_name="SCRAPY_BACKEND_TYPE")
    return cast("Backend", backend)


_UNUSABLE_ACK_VALUE_MESSAGE = "Deferred-ack acknowledgement token is unusable"
_INVALID_ACK_QUEUE_NAME_MESSAGE = (
    "Deferred-ack queue name must be an exact built-in string"
)


def _ack_token_key(token: Any) -> tuple[object, ...]:
    """Return a usable, non-disclosing key without invoking plugin protocols."""
    key: tuple[object, ...]
    if type(token) in {str, bytes, int}:
        key = ("value", type(token), token)
    else:
        key = ("identity", id(token))
    try:
        # Dict/set operations repeat this hash while holding the contract lock.
        # Prove here that the exact built-in-only key is usable before then.
        hash(key)
    except Exception:
        raise QueueError(_UNUSABLE_ACK_VALUE_MESSAGE) from None
    return key


def _is_empty_ack_token(token: Any) -> bool:
    """Recognize exact empty built-ins and fail closed for unusable values."""
    if type(token) not in {
        str,
        bytes,
        bytearray,
        memoryview,
        tuple,
        list,
        dict,
        set,
        frozenset,
    }:
        return False
    try:
        return len(token) == 0
    except Exception:
        # A released memoryview raises ValueError here. Do not let that raw
        # exception cross the adapter boundary or treat the token as usable.
        raise QueueError(_UNUSABLE_ACK_VALUE_MESSAGE) from None


def _require_exact_ack_queue_name(queue_name: object, operation: str) -> None:
    """Reject plugin-controlled string subclasses before delegation or locks."""
    if type(queue_name) is not str:
        raise QueueError(_INVALID_ACK_QUEUE_NAME_MESSAGE, operation=operation)


_DEFERRED_ACK_QUEUE_ERROR_MESSAGES: dict[str, frozenset[str]] = {
    "pop": frozenset(
        {
            "Deferred-ack backend returned an invalid delivery result",
            "Deferred-ack backend returned a generator delivery result",
            "Deferred-ack backend returned an iterator delivery result",
            "Deferred-ack backend returned an awaitable delivery result",
            "Deferred-ack backend returned an asynchronous-generator delivery result",
            "Deferred-ack backend returned a delivery without an acknowledgement token",
            "Deferred-ack backend returned an empty acknowledgement token",
            "Deferred-ack backend reused an active acknowledgement token",
            _UNUSABLE_ACK_VALUE_MESSAGE,
            _INVALID_ACK_QUEUE_NAME_MESSAGE,
        }
    ),
    "ack": frozenset(
        {
            "Deferred-ack settlement requires an issued acknowledgement token",
            "Deferred-ack settlement rejected an unknown acknowledgement token",
            "Deferred-ack backend returned a non-None settlement result",
            "Deferred-ack backend returned a generator settlement result",
            "Deferred-ack backend returned an iterator settlement result",
            "Deferred-ack backend returned an awaitable settlement result",
            "Deferred-ack backend returned an asynchronous-generator settlement result",
            _UNUSABLE_ACK_VALUE_MESSAGE,
            _INVALID_ACK_QUEUE_NAME_MESSAGE,
        }
    ),
    "nack": frozenset(
        {
            "Deferred-ack settlement requires an issued acknowledgement token",
            "Deferred-ack settlement rejected an unknown acknowledgement token",
            "Deferred-ack backend returned a non-None settlement result",
            "Deferred-ack backend returned a generator settlement result",
            "Deferred-ack backend returned an iterator settlement result",
            "Deferred-ack backend returned an awaitable settlement result",
            "Deferred-ack backend returned an asynchronous-generator settlement result",
            _UNUSABLE_ACK_VALUE_MESSAGE,
            _INVALID_ACK_QUEUE_NAME_MESSAGE,
        }
    ),
}


def _reject_lazy_ack_result(result: object, operation: str) -> None:
    """Reject lazy plugin results without advancing plugin-controlled code."""
    if type(result) is CoroutineType:
        # Closing an unstarted native coroutine only releases its frame; it does
        # not execute the body and prevents a later ``never awaited`` warning.
        CoroutineType.close(result)
    if type(result) is GeneratorType:
        # Likewise, closing a never-started native generator releases its frame
        # without executing the generator body or broker side effects in it.
        GeneratorType.close(result)
        result_kind = "generator"
    elif isawaitable(result):
        result_kind = "awaitable"
    elif isasyncgen(result):
        # Do not call ``aclose``: that would manufacture another awaitable which
        # synchronous callers cannot consume without leaking a warning.
        result_kind = "asynchronous-generator"
    elif isinstance(result, Iterator):
        # Never call ``next`` or an untrusted iterator's optional ``close`` hook.
        result_kind = "iterator"
    else:
        return
    result_role = "delivery" if operation == "pop" else "settlement"
    article = (
        "an"
        if result_kind in {"awaitable", "asynchronous-generator", "iterator"}
        else "a"
    )
    raise QueueError(
        f"Deferred-ack backend returned {article} {result_kind} {result_role} result",
        operation=operation,
    )


def _deferred_ack_queue_error_boundary(
    operation: str,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
    """Rebuild adapter errors after private delivery and token frames unwind."""

    def decorate(function: Callable[_P, _T]) -> Callable[_P, _T]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            caught_error: QueueError | None = None
            try:
                return function(*args, **kwargs)
            except QueueError as error:
                caught_error = error
            except BaseException:
                del args
                del kwargs
                raise

            message = "Deferred-ack queue operation failed."
            raw_args: object = None
            assert caught_error is not None
            if type(caught_error) is QueueError:
                raw_args = caught_error.args
                if (
                    type(raw_args) is tuple
                    and len(raw_args) == 1
                    and type(raw_args[0]) is str
                    and raw_args[0] in _DEFERRED_ACK_QUEUE_ERROR_MESSAGES[operation]
                ):
                    message = raw_args[0]
            sanitized_error = QueueError(message, operation=operation)
            del args
            del kwargs
            del caught_error
            del raw_args
            del message
            raise sanitized_error

        return wrapped

    return decorate


class _DeferredAckPluginQueueBackend(QueueBackend):
    """Runtime token fence for a statically conforming deferred-ACK plugin."""

    requires_ack = True

    def __init__(
        self,
        backend: QueueBackend,
        *,
        supports_concurrent_ack: bool,
    ) -> None:
        # Deliberately not named ``_backend``: queue strategies reserve that
        # attribute for unwrapping the circuit-breaker proxy. Exposing it here
        # would make a breaker-enabled strategy skip this token-validating
        # ``pop_with_ack`` override and fall back to tokenless ``pop``.
        self._delegate = backend
        self.supports_concurrent_ack = supports_concurrent_ack
        self._ack_contract_lock = threading.Lock()
        # Values retain the exact issued object. Identity-keyed tokens therefore
        # cannot be collected and have their id reused while they remain active.
        self._active_ack_tokens: dict[
            str,
            dict[tuple[object, ...], object],
        ] = {}
        # A reservation fences concurrent and reentrant settlement while plugin
        # descriptors and hooks run without holding the adapter's project lock.
        self._settling_ack_tokens: set[tuple[str, tuple[object, ...]]] = set()

    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        _require_exact_ack_queue_name(queue_name, "push")
        self._delegate.push(queue_name, item, priority)

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        _require_exact_ack_queue_name(queue_name, "pop")
        return self._delegate.pop(queue_name, timeout)

    def queue_len(self, queue_name: str) -> int:
        _require_exact_ack_queue_name(queue_name, "queue_len")
        return self._delegate.queue_len(queue_name)

    def clear_queue(self, queue_name: str) -> None:
        _require_exact_ack_queue_name(queue_name, "clear_queue")
        self._delegate.clear_queue(queue_name)

    @_deferred_ack_queue_error_boundary("pop")
    def pop_with_ack(
        self,
        queue_name: str,
        timeout: float = 0.0,
    ) -> tuple[bytes | None, Any | None]:
        _require_exact_ack_queue_name(queue_name, "pop")
        result = self._delegate.pop_with_ack(queue_name, timeout)
        _reject_lazy_ack_result(result, "pop")
        if type(result) is not tuple or len(result) != 2:
            raise QueueError(
                "Deferred-ack backend returned an invalid delivery result",
                operation="pop",
            )
        item, token = result
        if token is None:
            if item is None:
                return (None, None)
            raise QueueError(
                "Deferred-ack backend returned a delivery without an acknowledgement token",
                operation="pop",
            )
        if _is_empty_ack_token(token):
            raise QueueError(
                "Deferred-ack backend returned an empty acknowledgement token",
                operation="pop",
            )
        key = _ack_token_key(token)
        with self._ack_contract_lock:
            active = self._active_ack_tokens.setdefault(queue_name, {})
            if key in active:
                raise QueueError(
                    "Deferred-ack backend reused an active acknowledgement token",
                    operation="pop",
                )
            active[key] = token
        return (item, token)

    @_deferred_ack_queue_error_boundary("ack")
    def ack(self, queue_name: str, *, token: Any | None = None) -> None:
        self._settle("ack", queue_name, token)

    @_deferred_ack_queue_error_boundary("nack")
    def nack(self, queue_name: str, *, token: Any | None = None) -> None:
        self._settle("nack", queue_name, token)

    def _settle(self, operation: str, queue_name: str, token: Any | None) -> None:
        _require_exact_ack_queue_name(queue_name, operation)
        if token is None or _is_empty_ack_token(token):
            raise QueueError(
                "Deferred-ack settlement requires an issued acknowledgement token",
                operation=operation,
            )
        key = _ack_token_key(token)
        reservation = (queue_name, key)
        with self._ack_contract_lock:
            active = self._active_ack_tokens.get(queue_name)
            issued_token = active.get(key) if active is not None else None
            if (
                issued_token is None
                or (key[0] == "identity" and issued_token is not token)
                or reservation in self._settling_ack_tokens
            ):
                raise QueueError(
                    "Deferred-ack settlement rejected an unknown acknowledgement token",
                    operation=operation,
                )
            self._settling_ack_tokens.add(reservation)

        try:
            # Attribute resolution itself may invoke a plugin descriptor, so it
            # belongs outside the contract lock along with the settlement hook.
            settle_hook = cast(
                "Callable[..., Any]",
                self._delegate.ack if operation == "ack" else self._delegate.nack,
            )
            result = settle_hook(queue_name, token=token)
            _reject_lazy_ack_result(result, operation)
            if result is not None:
                raise QueueError(
                    "Deferred-ack backend returned a non-None settlement result",
                    operation=operation,
                )
        except BaseException:
            with self._ack_contract_lock:
                self._settling_ack_tokens.discard(reservation)
            raise

        with self._ack_contract_lock:
            self._settling_ack_tokens.remove(reservation)
            active = self._active_ack_tokens[queue_name]
            active.pop(key)
            if not active:
                self._active_ack_tokens.pop(queue_name)


# ---------------------------------------------------------------------------
# Backward-compat built-in capability sets. These are ``frozenset[str]`` so
# importing connectors never enumerates third-party entry points. Membership
# tests against both
# plain strings and ``BackendType`` members (which compare equal to their
# string ``.value``) work unchanged.
# ---------------------------------------------------------------------------
# Kept as module-level constants so existing call sites and tests that import
# them (e.g. ``tests/test_rocketmq_backend.py``) continue to compile. The
# underlying data lives in ``registry._BUNDLED_DESCRIPTORS``. Installed plugin
# capabilities are intentionally available only through ``capable_backends``.


def capable_backends(capability: str) -> frozenset[str]:
    """Explicitly discover and return backends declaring ``capability``."""
    return frozenset(
        name
        for name, descriptor in get_registry().items()
        if capability in descriptor.capabilities
    )


def _bundled_capable_backends(capability: str) -> frozenset[str]:
    """Return immutable built-in capability data without plugin discovery."""
    return frozenset(
        name
        for name, descriptor in _BUNDLED_DESCRIPTORS.items()
        if capability in descriptor.capabilities
    )


#: Built-in backends implementing :class:`~scrapy_extension.backends.base.QueueBackend`.
#: Third-party capability discovery is available explicitly via :func:`capable_backends`.
QUEUE_CAPABLE_BACKENDS: frozenset[str] = _bundled_capable_backends("queue")
#: Built-in backends implementing :class:`~scrapy_extension.backends.base.SetBackend`.
SET_CAPABLE_BACKENDS: frozenset[str] = _bundled_capable_backends("set")
#: Built-in backends implementing :class:`~scrapy_extension.backends.base.StorageBackend`.
STORAGE_CAPABLE_BACKENDS: frozenset[str] = _bundled_capable_backends("storage")


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


@_bundled_optional_dependency_boundary
@configuration_error_boundary(
    "Backend configuration is invalid.",
    _RESOLVED_BACKEND_SETTING_NAMES,
    preserve_static_message=True,
    safe_message_predicate=_is_safe_resolved_backend_message,
    pass_through_exception_types=(_BundledOptionalDependencyFailure,),
)
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
    safe_component_name = (
        component_name
        if type(component_name) is str and component_name in {"queue", "set", "storage"}
        else "component"
    )
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
                if name in _BUNDLED_BACKEND_TYPES
                and all(cap in descriptor.capabilities for cap in required_capabilities)
            )
            selected_backend = (
                backend_type
                if backend_type in _BUNDLED_BACKEND_TYPES
                else "third-party backend"
            )
            msg = (
                f"Selected {selected_backend} does not support the {safe_component_name} "
                f"interface and is missing capabilities. Capable bundled backends: "
                f"{capable}."
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
        # Plugins deliberately skip bundled flat-setting extraction and nested-key
        # validation.  Their declared model fields still matter to the manager:
        # a plugin may legitimately own ``retry_attempts`` or ``retry_delay``.
        # Resolve the descriptor only for that collision information so those
        # public values stay with the plugin while ``manager_retry_*`` continues
        # to configure ConnectionManager independently.
        descriptor = get_descriptor(backend_type)
        settings_cls = _load_resolver_settings_class(descriptor)
        return _merge_connection_manager_settings(
            settings,
            {},
            nested_settings,
            _model_field_names(settings_cls),
        )

    descriptor = get_descriptor(backend_type)
    settings_cls = _load_resolver_settings_class(descriptor)
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
        if type(setting_name) is not str or setting_name not in allowed_nested_names:
            raise _unknown_backend_setting(
                setting_name, allowed_nested_names, backend_type
            )

    flat_key_to_field = {
        f"{env_prefix}{field_name.upper()}".upper(): field_name
        for field_name in field_names
    }
    flat_settings: dict[str, Any] = {}
    if isinstance(settings, Mapping):
        for setting_name, value in settings.items():
            if type(setting_name) is not str:
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
                # value (or the documented default). A per-manager alias is the local
                # override when no global Scrapy retry value was supplied.
                manager_settings.setdefault(
                    internal_name,
                    merged_nested_settings.get(
                        _CONNECTION_MANAGER_DIRECT_KEYS[public_name],
                        _CONNECTION_MANAGER_DEFAULTS[public_name],
                    ),
                )
            else:
                manager_settings[internal_name] = merged_nested_settings.pop(
                    public_name
                )

        if public_name in merged_backend_settings:
            manager_settings.setdefault(
                internal_name,
                _CONNECTION_MANAGER_DEFAULTS[public_name],
            )

    manager_settings.update(_resolve_circuit_breaker_policy(settings))
    merged_backend_settings.update(merged_nested_settings)
    merged_backend_settings.update(manager_settings)
    return merged_backend_settings


def _resolve_circuit_breaker_policy(
    settings: Any,
) -> dict[str, bool | int | float]:
    """Resolve explicit Scrapy breaker values before their environment fallback.

    No source means the manager retains its existing lazy ``Settings`` fallback;
    the absent internal keys therefore represent the all-default policy without
    changing the public backend-settings mapping returned by the resolver.
    """
    raw_values: dict[str, object] = {}
    has_source = False
    for (
        policy_name,
        scrapy_key,
    ) in _CONNECTION_MANAGER_CIRCUIT_BREAKER_SCRAPY_KEYS.items():
        value = settings.get(scrapy_key)
        if value is None:
            value = os.environ.get(scrapy_key)
        if value is not None:
            has_source = True
        raw_values[policy_name] = (
            _CONNECTION_MANAGER_CIRCUIT_BREAKER_DEFAULTS[policy_name]
            if value is None
            else value
        )

    if not has_source:
        return {}

    enabled, failure_threshold, reset_timeout = _parse_circuit_breaker_policy(
        raw_values["enabled"],
        raw_values["failure_threshold"],
        raw_values["reset_timeout"],
    )
    return {
        _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS["enabled"]: enabled,
        _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS[
            "failure_threshold"
        ]: failure_threshold,
        _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS["reset_timeout"]: (
            reset_timeout
        ),
    }


def _parse_circuit_breaker_policy(
    raw_enabled: object,
    raw_failure_threshold: object,
    raw_reset_timeout: object,
) -> tuple[bool, int, float]:
    """Parse one breaker policy with the Settings-model bounds."""
    return (
        parse_bool_setting(
            raw_enabled,
            _CONNECTION_MANAGER_CIRCUIT_BREAKER_SCRAPY_KEYS["enabled"],
        ),
        parse_int_setting(
            raw_failure_threshold,
            _CONNECTION_MANAGER_CIRCUIT_BREAKER_SCRAPY_KEYS["failure_threshold"],
            minimum=1,
        ),
        parse_float_setting(
            raw_reset_timeout,
            _CONNECTION_MANAGER_CIRCUIT_BREAKER_SCRAPY_KEYS["reset_timeout"],
            minimum=0.0,
            maximum=CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S,
        ),
    )


def _unknown_backend_setting(
    setting_name: object,
    valid_names: frozenset[str],
    backend_type: str,
) -> ConfigurationError:
    """Build a static typo error without retaining an untrusted setting key."""
    suggestions = (
        get_close_matches(setting_name, sorted(valid_names), n=1, cutoff=0.6)
        if type(setting_name) is str
        else []
    )
    suggestion = (
        f" Did you mean {suggestions[0]!r}?"
        if suggestions and suggestions[0] in _SAFE_BACKEND_SETTING_HINTS
        else ""
    )
    scope = "bundled backend" if backend_type in _BUNDLED_BACKEND_TYPES else "backend"
    return ConfigurationError(
        f"Unknown {scope} setting.{suggestion}",
        setting_name="backend_settings",
    )


def _normalize_backend_type(value: object, setting_name: str) -> str:
    """Normalize a config value into a backend-type registry string.

    Round-5 R5-1: this replaces the prior ``_coerce_backend_type`` that
    forced ``BackendType(value)``. The registry now keys on plain strings
    so 3rd-party backends (registered via entry-points) route through the
    same path. ``BackendType`` members pass through via their string
    ``.value``; plain strings pass through unchanged. Values outside that
    contract and unknown backend names receive static typed errors instead of
    being stringified or reflected into public configuration diagnostics.

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
    if type(value) is not str:
        raise ConfigurationError(
            "Selected backend type is not registered.", setting_name=setting_name
        )
    candidate = value
    is_registered = True
    try:
        get_descriptor(candidate)
    except ConfigurationError:
        is_registered = False
    if not is_registered:
        raise ConfigurationError(
            "Selected backend type is not registered.", setting_name=setting_name
        )
    return candidate


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
        self._manager._release_acquire(self._token)


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
        self._users: int = 0
        # Retirement is a repairable generation state.  Removing the final token
        # and publishing ``_retired`` happen atomically under ``_registry_lock``;
        # a retry with an already-absent token still completes this teardown.
        self._retirement_complete = False
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
    def get_manager(
        cls,
        backend_type: str,
        settings: dict[str, Any] | None = None,
    ) -> ConnectionManager:
        """Get a shared manager and register one legacy ``close()`` acquire."""
        manager, _ = cls._get_manager_with_token(
            backend_type,
            settings,
            legacy=True,
        )
        return manager

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
                with self._connect_lock:
                    self._connect_with_retries(monitor_events)
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
            with self._connect_lock:
                self._connect_with_retries(monitor_events)
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

    def _connect_with_retries(self, monitor_events: list[_MonitorEvent]) -> None:
        """Run one serialized transaction and record deferred monitor events."""
        stale_backend: Backend | None = None
        retired_adapter: _DeferredAckPluginQueueBackend | None = None
        retired_source: tuple[Backend, CircuitBreaker | None] | None = None
        while True:
            with self._lock:
                if self._retired:
                    raise BackendConnectionError(
                        "Cannot connect a released ConnectionManager",
                        backend_type=str(self._backend_type_for_operations()),
                    )
                backend = self._backend
            if backend is None:
                break

            # A published object can outlive its network connection. Run the health
            # probe outside ``_lock`` because Redis/MongoDB/ElasticSearch probes may
            # perform network I/O; holding the shared state lock here would block
            # close() and peer access for the entire timeout window.
            health_check_failed = False
            try:
                connected = backend.is_connected()
            except Exception:
                connected = False
                health_check_failed = True
            if health_check_failed:
                # Leave the health-probe handler before diagnostics.  Logging handlers
                # are application code and must not inherit the driver's raw failure
                # through ``sys.exc_info()``.
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
                # Re-check identity after the unlocked health probe. A lifecycle race
                # may have detached the inspected backend; retry against current state
                # instead of publishing a decision about an obsolete object.
                if self._backend is not backend:
                    continue
                if connected:
                    return
                self._backend = None
                retired_adapter, retired_source = (
                    self._detach_plugin_queue_backend_under_lock()
                )
                # Backend and breaker form one connection generation. Replace the
                # breaker while holding the same state lock that detaches the backend
                # so interface accessors can validate a coherent pair. Performing this
                # later, after disconnect(), exposes ``None/old-breaker`` and then
                # ``replacement/old-breaker`` windows to racing accessors.
                if self._breaker is not None:
                    self._breaker = self._breaker.new_generation()
                stale_backend = backend
                break

        # Drop token-owning adapter state only after the generation lock is free.
        del retired_adapter, retired_source
        if stale_backend is not None:
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

        retry_attempts, retry_delay = self._retry_policy()
        total_attempts = retry_attempts + 1

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

            if attempt_failed:
                # The driver exception is no longer active here.  Keep continuation
                # telemetry fixed and do not expose a backend failure via a custom
                # logging handler's ``sys.exc_info()``.
                _log_diagnostic(
                    logger.warning,
                    "Connection attempt failed.",
                )
                with self._lock:
                    retired = self._retired
                if retired:
                    break
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
                    interrupted = _wait_for_retry_backoff(
                        self._retirement_event,
                        compute_full_jitter_backoff(attempt, retry_delay),
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
            return

        if failed_attempt:
            attempt_word = "attempt" if total_attempts == 1 else "attempts"
            raise BackendConnectionError(
                f"Failed to connect after {total_attempts} {attempt_word}.",
                backend_type=str(self._backend_type_for_operations()),
            )

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
            self._finalize_retirement()
            return

        with cls._registry_lock:
            legacy_token = (
                self._legacy_acquires.pop(0) if self._legacy_acquires else None
            )
        if legacy_token is not None:
            self._release_acquire(legacy_token)
        elif self._retired:
            # Repair a prior final-release interruption without consuming a peer.
            self._finalize_retirement()

    def _is_acquire_released(self, acquire_token: object) -> bool:
        """Return whether one opaque acquire token is no longer authoritative."""
        with type(self)._registry_lock:
            return acquire_token not in self._active_acquires

    def _release_acquire(self, acquire_token: object) -> None:
        """Release one exact token and repair final retirement when necessary."""
        cls = type(self)
        should_finalize = False
        with cls._registry_lock:
            self._active_acquires.discard(acquire_token)
            # A token-aware release can race legacy cleanup only through misuse;
            # keep the compatibility queue synchronized without relying on it.
            try:
                self._legacy_acquires.remove(acquire_token)
            except ValueError:
                pass
            self._users = len(self._active_acquires)
            if not self._active_acquires:
                self._retired = True
                self._retirement_event.set()
                registry_token = self._registry_token
                if (
                    registry_token is not None
                    and cls._managers.get(registry_token) is self
                ):
                    cls._managers.pop(registry_token, None)
                should_finalize = not self._retirement_complete
        if should_finalize:
            self._finalize_retirement()

    def _finalize_retirement(self) -> None:
        """Complete one manager retirement without replaying opaque teardown."""
        backend_to_disconnect: Backend | None = None
        with self._lock:
            self._retired = True
            self._retirement_event.set()
            if self._retirement_complete:
                return
            if self._retiring_backend is None and self._backend is not None:
                self._retiring_backend = self._backend
                self._backend = None
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

        with self._lock:
            self._retirement_complete = True
            self._retiring_backend = None
            retired_adapter = self._retiring_adapter
            retired_source = self._retiring_adapter_source
            self._retiring_adapter = None
            self._retiring_adapter_source = None

        # Hostile plugin token destruction and monitor callbacks run only after all
        # registry/manager state is terminal and unlocked.
        del retired_adapter, retired_source
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
        else:
            # Read the breaker config OUTSIDE self._lock (#15). Imported lazily
            # to avoid a settings-module import cycle at module load time and to
            # keep the direct-construction fallback deferred to first use.
            from scrapy_extension.settings import Settings

            settings = Settings()
            enabled = settings.circuit_breaker_enabled
            failure_threshold = settings.circuit_breaker_failure_threshold
            reset_timeout = settings.circuit_breaker_reset_timeout
        with self._lock:
            if self._breaker_configured:
                return self._breaker
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
            return self._breaker

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
