"""Third-party backend plugin validation and the deferred-ack adapter."""

from __future__ import annotations

import ast
import threading
from abc import ABC
from collections.abc import Callable, Iterator, Mapping
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
from types import CoroutineType, FunctionType, GeneratorType
from typing import Any, NamedTuple, cast

from scrapy_extension.backends.base import (
    Backend,
    QueueBackend,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.backends.connectors._capabilities import _load_object
from scrapy_extension.backends.connectors._constants import (
    _BUNDLED_BACKEND_TYPES,
    _MANAGER_CONFIGURATION_SETTING_NAMES,
    _SAFE_BACKEND_SETTING_MESSAGES,
    _SAFE_MANAGER_MESSAGES,
)
from scrapy_extension.backends.connectors._diagnostics import _P, _T
from scrapy_extension.backends.registry import (
    BackendDescriptor,
)
from scrapy_extension.exceptions import (
    ConfigurationError,
    QueueError,
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
