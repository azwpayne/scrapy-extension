"""Connection manager for backend connections (package facade).

The former single-module ``connectors.py`` is split by responsibility into
private submodules; this facade re-exports the historical module surface
so every ``scrapy_extension.backends.connectors`` import path is unchanged.

Submodules: ``_constants`` / ``_diagnostics`` / ``_capabilities`` /
``_plugin_contract`` / ``_config`` / ``_manager``."""

from __future__ import annotations

import json as json  # noqa: F401 — historical module attribute (patch seam)

__all__ = [
    "CONNECTION_MANAGER_SCOPE_KEY",
    "CONSUMER_SCOPED_BACKENDS",
    "QUEUE_CAPABLE_BACKENDS",
    "SET_CAPABLE_BACKENDS",
    "STORAGE_CAPABLE_BACKENDS",
    "ConnectionManager",
    "ConnectionManagerLease",
    "capable_backends",
    "release_manager_acquire",
    "resolve_backend_config",
    "resolve_circuit_breaker_policy",
]

from scrapy_extension.backends._retry import (
    compute_full_jitter_backoff as compute_full_jitter_backoff,
)
from scrapy_extension.backends.circuit_breaker import CircuitBreaker as CircuitBreaker
from scrapy_extension.backends.connectors._capabilities import (
    QUEUE_CAPABLE_BACKENDS,
    SET_CAPABLE_BACKENDS,
    STORAGE_CAPABLE_BACKENDS,
    capable_backends,
)
from scrapy_extension.backends.connectors._capabilities import (
    _bundled_capable_backends as _bundled_capable_backends,
)
from scrapy_extension.backends.connectors._capabilities import (
    _load_object as _load_object,
)
from scrapy_extension.backends.connectors._config import (
    _adapt_backend_settings as _adapt_backend_settings,
)
from scrapy_extension.backends.connectors._config import (
    _load_resolver_settings_class as _load_resolver_settings_class,
)
from scrapy_extension.backends.connectors._config import (
    _merge_connection_manager_settings as _merge_connection_manager_settings,
)
from scrapy_extension.backends.connectors._config import (
    _normalize_backend_type as _normalize_backend_type,
)
from scrapy_extension.backends.connectors._config import (
    _parse_circuit_breaker_policy as _parse_circuit_breaker_policy,
)
from scrapy_extension.backends.connectors._config import (
    _resolve_circuit_breaker_policy as _resolve_circuit_breaker_policy,
)
from scrapy_extension.backends.connectors._config import (
    _safe_manager_setting_name as _safe_manager_setting_name,
)
from scrapy_extension.backends.connectors._config import (
    _unknown_backend_setting as _unknown_backend_setting,
)
from scrapy_extension.backends.connectors._config import (
    resolve_backend_config,
    resolve_circuit_breaker_policy,
)
from scrapy_extension.backends.connectors._constants import (
    _BUNDLED_BACKEND_TYPES as _BUNDLED_BACKEND_TYPES,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_BACKEND_EXCLUDED_KEYS as _CONNECTION_MANAGER_BACKEND_EXCLUDED_KEYS,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_CIRCUIT_BREAKER_DEFAULTS as _CONNECTION_MANAGER_CIRCUIT_BREAKER_DEFAULTS,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS as _CONNECTION_MANAGER_CIRCUIT_BREAKER_INTERNAL_KEYS,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_CIRCUIT_BREAKER_SCRAPY_KEYS as _CONNECTION_MANAGER_CIRCUIT_BREAKER_SCRAPY_KEYS,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_DEFAULTS as _CONNECTION_MANAGER_DEFAULTS,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_DIRECT_KEYS as _CONNECTION_MANAGER_DIRECT_KEYS,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_INTERNAL_KEYS as _CONNECTION_MANAGER_INTERNAL_KEYS,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_SCOPE_KEY as _CONNECTION_MANAGER_SCOPE_KEY,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_SCRAPY_KEYS as _CONNECTION_MANAGER_SCRAPY_KEYS,
)
from scrapy_extension.backends.connectors._constants import (
    _CONNECTION_MANAGER_SETTING_NAMES as _CONNECTION_MANAGER_SETTING_NAMES,
)
from scrapy_extension.backends.connectors._constants import (
    _CONSUMER_SCOPED_BACKENDS as _CONSUMER_SCOPED_BACKENDS,
)
from scrapy_extension.backends.connectors._constants import (
    _MANAGER_CONFIGURATION_SETTING_NAMES as _MANAGER_CONFIGURATION_SETTING_NAMES,
)
from scrapy_extension.backends.connectors._constants import (
    _RESOLVED_BACKEND_SETTING_NAMES as _RESOLVED_BACKEND_SETTING_NAMES,
)
from scrapy_extension.backends.connectors._constants import (
    _SAFE_BACKEND_SETTING_HINTS as _SAFE_BACKEND_SETTING_HINTS,
)
from scrapy_extension.backends.connectors._constants import (
    _SAFE_BACKEND_SETTING_MESSAGES as _SAFE_BACKEND_SETTING_MESSAGES,
)
from scrapy_extension.backends.connectors._constants import (
    _SAFE_MANAGER_CONNECTION_MESSAGES as _SAFE_MANAGER_CONNECTION_MESSAGES,
)
from scrapy_extension.backends.connectors._constants import (
    _SAFE_MANAGER_MESSAGES as _SAFE_MANAGER_MESSAGES,
)
from scrapy_extension.backends.connectors._constants import (
    CONNECTION_MANAGER_SCOPE_KEY,
    CONSUMER_SCOPED_BACKENDS,
)
from scrapy_extension.backends.connectors._diagnostics import _P as _P
from scrapy_extension.backends.connectors._diagnostics import _T as _T
from scrapy_extension.backends.connectors._diagnostics import (
    _log_diagnostic as _log_diagnostic,
)
from scrapy_extension.backends.connectors._diagnostics import (
    _wait_for_retry_backoff as _wait_for_retry_backoff,
)
from scrapy_extension.backends.connectors._manager import (
    _DURABLE_PUSH_QUEUE_ERROR_MESSAGE as _DURABLE_PUSH_QUEUE_ERROR_MESSAGE,
)
from scrapy_extension.backends.connectors._manager import (
    _SAFE_DURABLE_PUSH_QUEUE_MESSAGES as _SAFE_DURABLE_PUSH_QUEUE_MESSAGES,
)
from scrapy_extension.backends.connectors._manager import (
    ConnectionManager,
    ConnectionManagerLease,
    release_manager_acquire,
)
from scrapy_extension.backends.connectors._manager import (
    _canonical_registry_json as _canonical_registry_json,
)
from scrapy_extension.backends.connectors._manager import (
    _ConnectionAttempt as _ConnectionAttempt,
)
from scrapy_extension.backends.connectors._manager import (
    _durable_push_queue_error_boundary as _durable_push_queue_error_boundary,
)
from scrapy_extension.backends.connectors._manager import (
    _LazyConnectionContext as _LazyConnectionContext,
)
from scrapy_extension.backends.connectors._manager import (
    _manager_terminal_error_boundary as _manager_terminal_error_boundary,
)
from scrapy_extension.backends.connectors._manager import (
    _ManagerConstructionAttempt as _ManagerConstructionAttempt,
)
from scrapy_extension.backends.connectors._manager import _MonitorEvent as _MonitorEvent
from scrapy_extension.backends.connectors._manager import (
    _normalize_registry_value as _normalize_registry_value,
)
from scrapy_extension.backends.connectors._manager import (
    _normalized_manager_backend_type as _normalized_manager_backend_type,
)
from scrapy_extension.backends.connectors._manager import (
    _rebuild_connect_attempt_error as _rebuild_connect_attempt_error,
)
from scrapy_extension.backends.connectors._manager import (
    _registry_type_name as _registry_type_name,
)
from scrapy_extension.backends.connectors._manager import (
    _RetirementFinalizerToken as _RetirementFinalizerToken,
)
from scrapy_extension.backends.connectors._manager import (
    _safe_manager_connection_backend_type as _safe_manager_connection_backend_type,
)
from scrapy_extension.backends.connectors._manager import (
    _safe_manager_connection_message as _safe_manager_connection_message,
)
from scrapy_extension.backends.connectors._manager import logger as logger
from scrapy_extension.backends.connectors._plugin_contract import (
    _CAPABILITY_INTERFACES as _CAPABILITY_INTERFACES,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _DEFERRED_ACK_QUEUE_ERROR_MESSAGES as _DEFERRED_ACK_QUEUE_ERROR_MESSAGES,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _INVALID_ACK_QUEUE_NAME_MESSAGE as _INVALID_ACK_QUEUE_NAME_MESSAGE,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _UNUSABLE_ACK_VALUE_MESSAGE as _UNUSABLE_ACK_VALUE_MESSAGE,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _ack_token_key as _ack_token_key,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _bundled_optional_dependency_boundary as _bundled_optional_dependency_boundary,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _BundledOptionalDependencyFailure as _BundledOptionalDependencyFailure,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _deferred_ack_queue_error_boundary as _deferred_ack_queue_error_boundary,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _DeferredAckPluginQueueBackend as _DeferredAckPluginQueueBackend,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _invalid_plugin_ack_contract as _invalid_plugin_ack_contract,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _is_empty_ack_token as _is_empty_ack_token,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _is_safe_capability_message as _is_safe_capability_message,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _is_safe_manager_configuration_message as _is_safe_manager_configuration_message,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _is_safe_resolved_backend_message as _is_safe_resolved_backend_message,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _load_descriptor_object as _load_descriptor_object,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _load_static_ack_capabilities as _load_static_ack_capabilities,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _model_field_names as _model_field_names,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _PluginAckCapabilitySnapshot as _PluginAckCapabilitySnapshot,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _reject_lazy_ack_result as _reject_lazy_ack_result,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _require_exact_ack_queue_name as _require_exact_ack_queue_name,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _validate_backend_contract as _validate_backend_contract,
)
from scrapy_extension.backends.connectors._plugin_contract import (
    _validate_plugin_ack_class as _validate_plugin_ack_class,
)
from scrapy_extension.backends.registry import get_descriptor as get_descriptor
