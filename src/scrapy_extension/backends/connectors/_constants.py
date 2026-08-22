"""Shared module-level constants for the connectors package.

Settings-key maps, safe-message allow-lists, and the bundled backend-type
snapshot consumed across the connector submodules."""

from __future__ import annotations

from scrapy_extension.backends.base import (
    BackendType,
)
from scrapy_extension.utils.reactor import (
    DEFAULT_REACTOR_IO_TIMEOUT_S,
)

_BUNDLED_BACKEND_TYPES: frozenset[str] = frozenset(
    backend_type.value for backend_type in BackendType
)

_CONNECTION_MANAGER_SETTING_NAMES: frozenset[str] = frozenset(
    {"retry_attempts", "retry_delay", "reactor_io_timeout"}
)

_CONNECTION_MANAGER_INTERNAL_KEYS: dict[str, str] = {
    "retry_attempts": "__connection_manager_retry_attempts",
    "retry_delay": "__connection_manager_retry_delay",
    "reactor_io_timeout": "__connection_manager_reactor_io_timeout",
}

_CONNECTION_MANAGER_DIRECT_KEYS: dict[str, str] = {
    "retry_attempts": "manager_retry_attempts",
    "retry_delay": "manager_retry_delay",
    "reactor_io_timeout": "manager_reactor_io_timeout",
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
    "reactor_io_timeout": "SCRAPY_REACTOR_IO_TIMEOUT",
}

_CONNECTION_MANAGER_DEFAULTS: dict[str, int | float] = {
    "retry_attempts": 3,
    "retry_delay": 1.0,
    "reactor_io_timeout": DEFAULT_REACTOR_IO_TIMEOUT_S,
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
        "SCRAPY_REACTOR_IO_TIMEOUT",
        "SCRAPY_CIRCUIT_BREAKER_ENABLED",
        "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT",
        "api_key",
        "backend_settings",
        "retry_attempts",
        "retry_delay",
        "reactor_io_timeout",
    }
)

_RESOLVED_BACKEND_SETTING_NAMES: frozenset[str] = frozenset(
    {
        "SCRAPY_BACKEND_TYPE",
        "SCRAPY_REACTOR_IO_TIMEOUT",
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
