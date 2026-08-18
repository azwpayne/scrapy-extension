"""Shared safe rendering boundary for Pydantic settings validation errors."""

from __future__ import annotations

import math
from collections.abc import Mapping
from importlib import import_module
from typing import Annotated, Any, cast, get_args, get_origin

from pydantic import ValidationError, model_validator
from pydantic_core import InitErrorDetails
from pydantic_settings import BaseSettings, SettingsError

from scrapy_extension.exceptions._redaction import sanitize_configuration_error
from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._aws import _AWS_SAFE_CONFIGURATION_MESSAGES
from scrapy_extension.settings._broker_endpoints import (
    KAFKA_BROKER_ENDPOINTS_ERROR,
    ROCKETMQ_NAMESRV_ENDPOINTS_ERROR,
)

_SAFE_SETTINGS_ERROR_NAMES: frozenset[str] = frozenset(
    {
        "SCRAPY_BACKEND_TYPE",
        "collection_names",
    }
)
_SAFE_SETTINGS_CONFIGURATION_MESSAGES: frozenset[str] = frozenset(
    {
        "api_key and basic-auth (username/password) are mutually exclusive; remove one authentication method.",
        "api_key must not be blank when supplied.",
        "password must not be blank when supplied.",
        "Authenticated Pulsar connections require 'pulsar+ssl://' transport.",
        "CLOUD mode requires SDK certificate verification.",
        "ElasticSearch CLOUD mode requires 'cloud_id' to be set.",
        "ElasticSearch CLOUD mode requires an auth method: set 'api_key' or both 'username' and 'password'. Elastic Cloud always rejects an anonymous client (401), so a no-auth config would surface as an opaque health-check failure at connect() rather than here.",
        "Kafka CONFLUENT mode requires 'confluent_api_key' to be set. Without them the client could fall back to an unauthenticated SDK transport.",
        "Kafka CONFLUENT mode requires 'confluent_api_key and confluent_api_secret' to be set. Without them the client could fall back to an unauthenticated SDK transport.",
        "Kafka CONFLUENT mode requires 'confluent_api_secret' to be set. Without them the client could fall back to an unauthenticated SDK transport.",
        "Kafka TLS connections require ssl_check_hostname=True.",
        "Kafka TLS client authentication requires both certificate and key files.",
        KAFKA_BROKER_ENDPOINTS_ERROR,
        "min_pool_size must be <= max_pool_size — an inverted pair makes the connection pool unable to satisfy any checkout (deadlock under load).",
        "MongoDB ATLAS mode requires an explicit 'mongodb+srv://' uri. atlas_cluster_name cannot replace uri because the backend uses uri verbatim and a complete Atlas SRV hostname cannot be derived from a cluster display name.",
        "MongoDB REPLICA_SET mode requires 'replica_set_name' to be set, or a uri that already carries a '?replicaSet=...' query.",
        "MongoDB TLS uses a single combined certificate+key file (tlsCertificateKeyFile); set tls_cert_file OR tls_key_file, not both -- setting both silently drops the key.",
        "MongoDB TLS certificate settings require tls_enabled=True or mode='atlas'.",
        "Pulsar cluster service_url must use a single scheme followed by a comma-separated endpoint list.",
        "Redis SENTINEL mode requires 'sentinel_master_name' to be set. No endpoint or credential values are included in this error.",
        "Redis SENTINEL mode requires 'sentinels' to be set. No endpoint or credential values are included in this error.",
        "Redis SENTINEL mode requires 'sentinels and sentinel_master_name' to be set. No endpoint or credential values are included in this error.",
        ROCKETMQ_NAMESRV_ENDPOINTS_ERROR,
        "SASL credentials (sasl_username / sasl_password / sasl_mechanism) require a 'SASL_'-prefixed security_protocol ('SASL_SSL'); kafka-python silently ignores the SASL fields otherwise (auth never attempted).",
        "ssl_enabled=True requires 'ssl_cafile' to be set (path to a CA certificate bundle).",
        "username must not be blank when supplied.",
        "A SASL security_protocol requires an explicit sasl_mechanism.",
        "An amqps:// URL cannot be downgraded with ssl_enabled=False.",
        "allow_remote_http must be a boolean.",
        "Authenticated MongoDB connections require verified TLS unless they are direct standalone loopback development connections.",
        "Authenticated Pulsar connections require certificate verification.",
        "Authenticated Pulsar connections require hostname verification.",
        "Remote Pulsar TLS connections require certificate verification.",
        "Remote Pulsar TLS connections require hostname verification.",
        "Remote Redis TLS connections require ssl_check_hostname=True.",
        "Remote standalone HTTP endpoints require allow_remote_http=True. Use this override only for an explicitly trusted private network.",
        "Remote or authenticated Elasticsearch TLS requires verify_certs=True.",
        "Authenticated RocketMQ connections require tls_enabled=True.",
        "Cloud mode requires access_key and secret_key.",
        "Confluent API credentials require mode='confluent'; other modes ignore them.",
        "Credentials over http:// (cleartext) are not permitted; use https:// for any authenticated host or remove the credentials.",
        "Each Pulsar endpoint must be a host with an optional valid port.",
        "Explicit AWS credentials cannot be sent to a remote HTTP endpoint; use HTTPS or a loopback LocalStack endpoint.",
        "allow_insecure_connection applies only to pulsar+ssl://.",
        "ca_certs is unsupported in CLOUD mode because this backend does not pass it to the SDK.",
        "ca_certs requires every standalone Elasticsearch host to use https://.",
        "GSSAPI uses ambient Kerberos credentials; sasl_username and sasl_password would be ignored.",
        "Kafka CONFLUENT mode requires a real Confluent Cloud endpoint: set 'confluent_bootstrap_servers' (e.g. pkc-xxx.us-east-1.aws.confluent.cloud:9092) or override 'bootstrap_servers' with a real endpoint. An empty, whitespace, or localhost:9092 (the STANDALONE default) value cannot reach Confluent Cloud.",
        "Kafka QueueBackend requires acks=1 or acks='all'; acks=0 cannot confirm broker acceptance.",
        "Kafka acks must be 1 or 'all', not a boolean.",
        "Memcached host must be a bare hostname or IP address.",
        "Memcached host must be a non-empty hostname or IP address.",
        "Memcached host must not include a port or URL scheme.",
        "Memcached port must be between 1 and 65535.",
        "Memcached timeout must be finite, greater than 0, and at most 86400 seconds.",
        "MongoDB 'auth_source' must be a non-empty string.",
        "MongoDB 'database' name must be a non-empty string.",
        "MongoDB 'password' must be a string or SecretStr.",
        "MongoDB 'password' must be non-empty.",
        "MongoDB 'replica_set_name' must be a non-empty string when set.",
        "MongoDB 'username' must be non-empty.",
        "MongoDB GSSAPI authentication requires a username.",
        "MongoDB MONGODB-AWS username and password must be configured together.",
        "MongoDB MONGODB-X509 authentication does not support a password.",
        "MongoDB URI is malformed.",
        "MongoDB URI must contain valid server endpoint authorities.",
        "MongoDB URI must include at least one server endpoint.",
        "MongoDB URI must not contain authentication, credential, or TLS query options; configure dedicated settings instead.",
        "MongoDB URI must not contain fragments.",
        "MongoDB URI must not contain proxy query options.",
        "MongoDB URI must not contain userinfo; configure username/password settings instead.",
        "MongoDB URI must not disable TLS certificate or hostname verification.",
        "MongoDB auth_mechanism is unsupported.",
        "MongoDB capability collection names must be built-in strings.",
        "MongoDB capability collection names must be non-empty.",
        "MongoDB external authentication requires auth_source='$external'.",
        "MongoDB mode is unsupported.",
        "MongoDB mutations require an acknowledged write concern (w >= 1).",
        "MongoDB password authentication requires username and password.",
        "MongoDB queue, set, and storage capability domains must use distinct physical collection names.",
        "MongoDB seed endpoints must be a list or tuple of endpoint strings.",
        "MongoDB seed endpoints must be host, host:port, or '[IPv6]:port' values.",
        "MongoDB tls_allow_invalid_certificates must be a boolean.",
        "MongoDB tls_enabled must be a boolean.",
        "tls_allow_invalid_certificates requires an SDK TLS mode.",
        "MongoDB username and password must be configured together.",
        "MongoDB w must be a positive integer or 'majority', not a boolean.",
        "MongoDB w must be a positive integer or 'majority'.",
        "MongoDB w_timeout_ms must be a non-negative integer or None.",
        "OAUTHBEARER is unsupported because this backend does not expose the token-provider object required by kafka-python.",
        "Pulsar service_url must not contain URL userinfo; configure auth_token separately.",
        "tls_trust_certs_file requires a pulsar+ssl:// service_url.",
        "tls_validate_hostname applies only to pulsar+ssl:// and must remain true for plaintext.",
        "verify_certs=False is invalid when every Elasticsearch host uses http://.",
        "RabbitMQ CLUSTER mode requires 'cluster_nodes' to be set (a non-empty list of host:port). Without it the client connects to a single host:port, losing cluster topology.",
        "RabbitMQ IPv6 cluster nodes with a port must use '[IPv6]:port'.",
        "RabbitMQ MIRRORED_QUEUES mode requires 'ha_mode' to be set (one of: all, exactly, nodes). Without it the connect path silently skips HA policy setup — the queue is non-mirrored despite the mode name.",
        "RabbitMQ TLS client authentication requires both certificate and key files.",
        "RabbitMQ TLS requires CERT_REQUIRED certificate and hostname verification.",
        "RabbitMQ URL must include a host.",
        "RabbitMQ URL userinfo is not allowed; use explicit credential settings.",
        "RabbitMQ cluster node has an invalid bracketed IPv6 host.",
        "RabbitMQ cluster node must use '[IPv6]:port' syntax.",
        "RabbitMQ cluster node port cannot be empty.",
        "RabbitMQ cluster node port must be an integer.",
        "RabbitMQ cluster node port must be between 1 and 65535.",
        "RabbitMQ cluster nodes must be non-empty host or host:port values.",
        "RabbitMQ connections outside loopback require verified TLS.",
        "RabbitMQ host must be a non-empty hostname or IP address.",
        "RabbitMQ password must be explicitly set and cannot be blank.",
        "RabbitMQ port must be between 1 and 65535.",
        "RabbitMQ ssl_enabled must be a boolean.",
        "RabbitMQ username must be explicitly set and cannot be blank.",
        "RabbitMQ's guest user is restricted to loopback endpoints.",
        "Redis Cluster supports only database 0; use namespace for isolation.",
        "Redis TLS certificate settings require ssl_enabled=True.",
        "Redis TLS client authentication requires both certificate and key files.",
        "Redis authentication outside a direct literal-loopback standalone connection requires ssl_check_hostname=True.",
        "Redis authentication outside a direct literal-loopback standalone connection requires ssl_enabled=True.",
        "Redis cluster_startup_nodes require mode='cluster'.",
        "Redis replica routing is unsupported; read_from_replicas must be false.",
        "Redis replica routing is unsupported; replicas must remain empty.",
        "Redis sentinels require mode='sentinel'.",
        "Redis setting 'masters' is unsupported; use cluster_startup_nodes.",
        "Remote Memcached uses an unauthenticated plaintext protocol. Set allow_remote_plaintext=True only for an explicitly trusted private network.",
        "RocketMQ 'consumer_group' must be non-empty.",
        "SASL credentials require SASL_SSL; SASL_PLAINTEXT transmits them without TLS.",
        "STANDALONE mode requires at least one 'hosts' entry (e.g. http://host:9200 or https://host:9200). Got hosts=[]. CLOUD mode uses 'cloud_id' and does not require hosts.",
        "Selected backend type is not a registered backend type.",
        "Unsupported Memcached mode.",
        "Unsupported RocketMQ mode.",
        "access_key is required when secret_key is configured.",
        "allow_flush_all must be a boolean.",
        "allow_insecure_connection must be a boolean.",
        "allow_remote_plaintext must be a boolean.",
        "auth_token must be a string when explicitly configured.",
        "auth_token must be non-empty when explicitly configured.",
        "backpressure_pause_at must be >= 0.",
        "backpressure_resume_at must be <= backpressure_pause_at (otherwise the resume condition can never be reached once paused).",
        "backpressure_resume_at must be >= 0.",
        "each hosts entry must be a non-empty http:// or https:// endpoint.",
        "each hosts entry must be an http:// or https:// endpoint without userinfo, query, or fragment.",
        "each hosts entry must contain a valid network authority.",
        "hosts entries must not contain whitespace or control characters.",
        "min_insync_replicas cannot exceed replication_factor.",
        "num_partitions and max_priority_partitions must match because Kafka priority values map directly to physical partitions.",
        "queue_index, set_index, and storage_index must be pairwise distinct so a capability clear cannot delete another capability's data.",
        "sasl_mechanism must be supported by this Kafka backend.",
        "secret_key is required when access_key is configured.",
        "security_protocol must be a supported Kafka protocol.",
        "service_url must be a string.",
        "service_url must contain one or more non-empty Pulsar endpoints.",
        "service_url must start with 'pulsar://' or 'pulsar+ssl://'.",
        "tls_allow_invalid_certificates=True is not permitted for remote or production-tier MongoDB connections.",
        "tls_enabled must be a boolean.",
        "tls_trust_certs_file must be a non-empty path when configured.",
        "tls_validate_hostname must be a boolean.",
        "uri must start with 'mongodb://' or 'mongodb+srv://'.",
        "url must be a valid 'amqp://' or 'amqps://' connection URL.",
        "verify_certs must be enabled for authenticated standalone connections.",
    }
    | _AWS_SAFE_CONFIGURATION_MESSAGES
)
_TRUSTED_SETTINGS_CLASSES: frozenset[tuple[str, str]] = frozenset(
    {
        ("scrapy_extension.settings.base", "Settings"),
        ("scrapy_extension.settings.dynamodb", "DynamoDBSettings"),
        ("scrapy_extension.settings.elasticsearch", "ElasticSearchSettings"),
        ("scrapy_extension.settings.kafka", "KafkaSettings"),
        ("scrapy_extension.settings.memcached", "MemcachedSettings"),
        ("scrapy_extension.settings.mongodb", "MongoDBSettings"),
        ("scrapy_extension.settings.pulsar", "PulsarSettings"),
        ("scrapy_extension.settings.rabbitmq", "RabbitMQSettings"),
        ("scrapy_extension.settings.redis", "RedisSettings"),
        ("scrapy_extension.settings.rocketmq", "RocketMQSettings"),
        ("scrapy_extension.settings.sqs", "SqsSettings"),
    }
)


def _safe_location(value: object, field_names: frozenset[str]) -> tuple[str | int, ...]:
    """Keep a verified top-level field location without retaining raw keys."""
    if not isinstance(value, (list, tuple)) or not value:
        return ("configuration",)
    first = value[0]
    if not isinstance(first, str) or first not in field_names:
        return ("configuration",)
    location: list[str | int] = [first]
    for part in value[1:]:
        location.append(part if isinstance(part, int) else "value")
    return tuple(location)


def _redacted_validation_error(
    error: ValidationError, field_names: frozenset[str]
) -> ValidationError:
    """Rebuild a typed Pydantic error without input or validator diagnostics."""
    safe_lines: list[InitErrorDetails] = []
    for detail in error.errors():
        location = detail.get("loc") if isinstance(detail, Mapping) else None
        safe_lines.append(
            {
                "type": "value_error",
                "loc": _safe_location(location, field_names),
                "input": None,
                "ctx": {"error": ValueError("Invalid configuration value.")},
            }
        )
    if not safe_lines:
        safe_lines.append(
            {
                "type": "value_error",
                "loc": ("configuration",),
                "input": None,
                "ctx": {"error": ValueError("Invalid configuration value.")},
            }
        )
    return ValidationError.from_exception_data("Settings", safe_lines)


def _trusted_settings_fields(
    settings_type: type[BaseSettings],
) -> Mapping[str, Any] | None:
    """Return model fields only for the extension's exact bundled models."""
    try:
        module_name = settings_type.__module__
        qualified_name = settings_type.__qualname__
        if (
            type(module_name) is not str
            or type(qualified_name) is not str
            or (module_name, qualified_name) not in _TRUSTED_SETTINGS_CLASSES
        ):
            return None
        module = import_module(module_name)
        if getattr(module, qualified_name, None) is not settings_type:
            return None
        fields = settings_type.model_fields
    except Exception:  # noqa: BLE001 - third-party metadata is not trusted
        return None
    return fields if isinstance(fields, Mapping) else None


def _trusted_settings_field_names(instance: BaseSettings) -> frozenset[str]:
    """Return field names only for the extension's exact bundled models."""
    fields = _trusted_settings_fields(type(instance))
    if fields is None:
        return frozenset()
    return frozenset(name for name in fields if type(name) is str)


def _scalar_annotation_kind(annotation: object) -> tuple[type[object], bool] | None:
    """Identify exact scalar and optional-scalar annotations, including Annotated."""
    while get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    if annotation in (bool, int, float):
        return annotation, False
    args = get_args(annotation)
    if len(args) != 2 or type(None) not in args:
        return None
    inner = args[0] if args[1] is type(None) else args[1]
    while get_origin(inner) is Annotated:
        inner = get_args(inner)[0]
    if inner in (bool, int, float):
        return inner, True
    return None


def _canonical_unsigned_decimal(value: str) -> bool:
    """Return whether text is canonical, unsigned ASCII base-10 notation."""
    return value == "0" or (
        bool(value) and "1" <= value[0] <= "9" and value.isascii() and value.isdecimal()
    )


def _canonical_negative_decimal(value: str) -> bool:
    """Return whether text is canonical negative ASCII base-10 notation."""
    return (
        value.startswith("-")
        and _canonical_unsigned_decimal(value[1:])
        and value[1] != "0"
    )


def _canonical_float_text(value: str) -> bool:
    """Return whether text is unambiguous finite unsigned decimal notation."""
    integer, separator, fraction = value.partition(".")
    return _canonical_unsigned_decimal(integer) and (
        not separator
        or (bool(fraction) and fraction.isascii() and fraction.isdecimal())
    )


_NEGATIVE_TEXT_FIELDS: frozenset[tuple[str, str, str]] = frozenset(
    {
        (
            "scrapy_extension.settings.base",
            "Settings",
            "queue_delay_max_held",
        )
    }
)


def _normalize_bundled_scalar(
    settings_type: type[BaseSettings],
    field_name: str,
    annotation: object,
    value: object,
) -> object:
    """Normalize one exact bundled scalar without Pydantic's permissive coercions."""
    scalar = _scalar_annotation_kind(annotation)
    if scalar is None:
        return value
    scalar_type, optional = scalar
    if value is None and optional:
        return None
    if scalar_type is bool:
        if type(value) is bool:
            return value
        if type(value) is str:
            normalized = value.lower()
            if normalized in {"true", "1"}:
                return True
            if normalized in {"false", "0"}:
                return False
    elif scalar_type is int:
        if type(value) is int:
            return value
        if type(value) is str:
            negative_allowed = (
                settings_type.__module__,
                settings_type.__qualname__,
                field_name,
            ) in _NEGATIVE_TEXT_FIELDS
            if _canonical_unsigned_decimal(value) or (
                negative_allowed and _canonical_negative_decimal(value)
            ):
                return int(value)
    elif scalar_type is float:
        if type(value) in (int, float):
            normalized_float = float(cast("int | float", value))
            if math.isfinite(normalized_float):
                return normalized_float
        elif type(value) is str and _canonical_float_text(value):
            normalized_float = float(value)
            if math.isfinite(normalized_float):
                return normalized_float
    if scalar_type is float:
        raise ValueError("Bundled floating-point setting has an invalid value.")
    raise ConfigurationError(
        "Bundled scalar setting has an invalid value.",
        setting_name=field_name,
    )


class RedactedBaseSettings(BaseSettings):
    """Base settings that retain typed errors without retaining raw input.

    ``hide_input_in_errors`` only affects Pydantic's human-readable rendering;
    its ``errors()`` and ``json()`` APIs otherwise retain the original input.
    This boundary rebuilds any Pydantic validation failure with verified field
    locations and ``input=None`` after the original handler exits.
    """

    @model_validator(mode="before")
    @classmethod
    def _enforce_bundled_scalar_grammar(cls, values: object) -> object:
        """Apply exact scalar coercion only to the package's bundled models."""
        fields = _trusted_settings_fields(cls)
        if fields is None or not isinstance(values, Mapping):
            return values
        normalized = dict(values)
        for field_name, field in fields.items():
            if (
                type(field_name) is str
                and field_name in normalized
                and not (
                    cls.__module__ == "scrapy_extension.settings.memcached"
                    and field_name in {"connect_timeout", "socket_timeout"}
                )
            ):
                normalized[field_name] = _normalize_bundled_scalar(
                    cls,
                    field_name,
                    getattr(field, "annotation", None),
                    normalized[field_name],
                )
        return normalized

    def __init__(self, **values: Any) -> None:
        validation_error: ValidationError | None = None
        source_error: SettingsError | None = None
        configuration_error: ConfigurationError | None = None
        unexpected_failure = False
        try:
            super().__init__(**values)
        except ValidationError as error:
            validation_error = error
        except SettingsError as error:
            source_error = error
        except ConfigurationError as error:
            configuration_error = error
        except Exception:  # noqa: BLE001 - settings inputs and sources are untrusted
            unexpected_failure = True
        if source_error is not None:
            # The exception traceback retains this frame. Drop raw input and the
            # source error before raising so frame-local introspection cannot reach
            # Pydantic's JSON decoder diagnostics.
            del values
            del source_error
            del self
            raise ConfigurationError(
                "Settings source contains an invalid configuration value.",
                setting_name="settings",
            )
        if validation_error is not None:
            field_names = _trusted_settings_field_names(self)
            redacted_error = _redacted_validation_error(validation_error, field_names)
            # As above, the new exception carries this frame in its traceback. Its
            # temporary source data must not remain available through ``f_locals``.
            del values
            del validation_error
            del self
            raise redacted_error
        if configuration_error is not None:
            if type(configuration_error) is ConfigurationError:
                field_names = _trusted_settings_field_names(self)
                original_message = (
                    configuration_error.args[0] if configuration_error.args else None
                )
                sanitized_error = sanitize_configuration_error(
                    configuration_error,
                    field_names | _SAFE_SETTINGS_ERROR_NAMES,
                    message=(
                        original_message
                        if (
                            field_names
                            and type(original_message) is str
                            and original_message
                            in _SAFE_SETTINGS_CONFIGURATION_MESSAGES
                        )
                        else "Settings contain an invalid configuration value."
                    ),
                    fallback_setting_name="settings",
                )
                del original_message
            else:
                sanitized_error = ConfigurationError(
                    "Settings contain an invalid configuration value.",
                    setting_name="settings",
                )
            # Validator-raised ConfigurationError instances carry their original
            # input frames.  Rebuild a static error outside the handler and remove
            # the original input/model references from this new traceback frame.
            del values
            del configuration_error
            del self
            raise sanitized_error
        if unexpected_failure:
            del values
            del self
            raise ConfigurationError(
                "Settings contain an invalid configuration value.",
                setting_name="settings",
            )
