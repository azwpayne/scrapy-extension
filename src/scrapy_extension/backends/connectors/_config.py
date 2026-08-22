"""Per-component backend configuration resolution and breaker policy parsing."""

from __future__ import annotations

import os
from collections.abc import Mapping
from difflib import get_close_matches
from typing import Any

from pydantic import BaseModel, ValidationError

from scrapy_extension.backends.base import (
    BackendType,
)
from scrapy_extension.backends.circuit_breaker import (
    CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S,
)
from scrapy_extension.backends.connectors._constants import (
    _BUNDLED_BACKEND_TYPES,
    _CONNECTION_MANAGER_CIRCUIT_BREAKER_DEFAULTS,
    _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS,
    _CONNECTION_MANAGER_CIRCUIT_BREAKER_SCRAPY_KEYS,
    _CONNECTION_MANAGER_DEFAULTS,
    _CONNECTION_MANAGER_DIRECT_KEYS,
    _CONNECTION_MANAGER_INTERNAL_KEYS,
    _CONNECTION_MANAGER_SCRAPY_KEYS,
    _CONNECTION_MANAGER_SETTING_NAMES,
    _RESOLVED_BACKEND_SETTING_NAMES,
    _SAFE_BACKEND_SETTING_HINTS,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _bundled_optional_dependency_boundary,
    _BundledOptionalDependencyFailure,
    _is_safe_resolved_backend_message,
    _load_descriptor_object,
    _model_field_names,
)
from scrapy_extension.backends.registry import (
    BackendDescriptor,
    get_descriptor,
    get_registry,
    has_capability,
)
from scrapy_extension.exceptions import (
    ConfigurationError,
)
from scrapy_extension.exceptions._redaction import configuration_error_boundary
from scrapy_extension.utils._config import (
    parse_bool_setting,
    parse_float_setting,
    parse_int_setting,
)


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

    manager_settings.update(resolve_circuit_breaker_policy(settings))
    merged_backend_settings.update(merged_nested_settings)
    merged_backend_settings.update(manager_settings)
    return merged_backend_settings


def resolve_circuit_breaker_policy(
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


# Backward-compatible alias for callers of the former private name.
_resolve_circuit_breaker_policy = resolve_circuit_breaker_policy


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
