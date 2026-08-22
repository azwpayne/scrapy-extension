"""High-risk backend settings contract regressions."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from pydantic_settings import BaseSettings
from scrapy.settings import Settings as ScrapySettings

from scrapy_extension.backends.connectors import (
    ConnectionManager,
    resolve_backend_config,
)
from scrapy_extension.backends.rabbitmq import RabbitMQBackend
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import (
    DynamoDBMode,
    DynamoDBSettings,
    ElasticSearchMode,
    ElasticSearchSettings,
    KafkaSettings,
    MemcachedSettings,
    MongoDBMode,
    MongoDBSettings,
    PulsarSettings,
    RabbitMQSettings,
    RedisSettings,
    RocketMQSettings,
    Settings,
    SqsMode,
    SqsSettings,
)

pytestmark = pytest.mark.unit


_BUNDLED_SETTINGS_CLASSES: tuple[type[BaseSettings], ...] = (
    Settings,
    RedisSettings,
    MongoDBSettings,
    ElasticSearchSettings,
    MemcachedSettings,
    KafkaSettings,
    PulsarSettings,
    RabbitMQSettings,
    RocketMQSettings,
    SqsSettings,
    DynamoDBSettings,
)


@pytest.mark.parametrize(
    "settings_cls",
    _BUNDLED_SETTINGS_CLASSES,
    ids=lambda settings_cls: settings_cls.__name__,
)
def test_direct_bundled_settings_reject_unknown_fields(
    settings_cls: type[BaseSettings],
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        settings_cls(definitely_not_a_setting=True)

    errors = exc_info.value.errors()
    assert errors
    assert all(error["type"] == "value_error" for error in errors)
    assert all(error["loc"] == ("configuration",) for error in errors)
    assert all(error["input"] is None for error in errors)


@pytest.mark.parametrize(
    ("settings_cls", "env_name", "field_name", "raw_value", "expected"),
    [
        (Settings, "SCRAPY_RETRY_ATTEMPTS", "retry_attempts", "8", 8),
        (RedisSettings, "SCRAPY_REDIS_PORT", "port", "6381", 6381),
        (MongoDBSettings, "SCRAPY_MONGO_DATABASE", "database", "env_db", "env_db"),
        (
            ElasticSearchSettings,
            "SCRAPY_ELASTICSEARCH_REQUEST_TIMEOUT",
            "request_timeout",
            "12.5",
            12.5,
        ),
        (MemcachedSettings, "SCRAPY_MEMCACHED_PORT", "port", "11212", 11212),
        (KafkaSettings, "SCRAPY_KAFKA_GROUP_ID", "group_id", "env-group", "env-group"),
        (
            PulsarSettings,
            "SCRAPY_PULSAR_SUBSCRIPTION_NAME",
            "subscription_name",
            "env-subscription",
            "env-subscription",
        ),
        (
            RabbitMQSettings,
            "SCRAPY_RABBITMQ_HOST",
            "host",
            "rabbit.internal",
            "rabbit.internal",
        ),
        (
            RocketMQSettings,
            "SCRAPY_ROCKETMQ_SEND_TIMEOUT",
            "send_timeout",
            "5000",
            5000,
        ),
        (
            SqsSettings,
            "SCRAPY_SQS_QUEUE_NAME_PREFIX",
            "queue_name_prefix",
            "env-sqs-",
            "env-sqs-",
        ),
        (
            DynamoDBSettings,
            "SCRAPY_DYNAMODB_TABLE_NAME",
            "table_name",
            "env-dynamodb",
            "env-dynamodb",
        ),
    ],
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
def test_bundled_settings_environment_loading_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    settings_cls: type[BaseSettings],
    env_name: str,
    field_name: str,
    raw_value: str,
    expected: object,
) -> None:
    monkeypatch.setenv(env_name, raw_value)
    if settings_cls is RabbitMQSettings:
        monkeypatch.setenv("SCRAPY_RABBITMQ_USERNAME", "crawler")
        monkeypatch.setenv("SCRAPY_RABBITMQ_PASSWORD", "secret")
        monkeypatch.setenv("SCRAPY_RABBITMQ_SSL_ENABLED", "true")

    settings = settings_cls()

    assert settings.model_dump()[field_name] == expected


def _resolve_queue(settings: ScrapySettings) -> tuple[str, dict[str, object]]:
    return resolve_backend_config(
        settings,
        type_key="SCRAPY_QUEUE_BACKEND_TYPE",
        settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
    )


def test_elasticsearch_cloud_credentials_ignore_standalone_default_hosts() -> None:
    settings = ElasticSearchSettings(
        mode=ElasticSearchMode.CLOUD,
        cloud_id="deployment:encoded",
        api_key="cloud-secret",  # type: ignore[arg-type]
    )

    assert settings.cloud_id == "deployment:encoded"
    assert settings.api_key is not None


def test_mongodb_atlas_mode_requires_srv_uri() -> None:
    """ATLAS mode without an ``mongodb+srv://`` uri fails fast — the backend
    connects with ``uri`` verbatim (a cluster display name cannot replace it;
    the former ``atlas_cluster_name`` label was never consumed)."""
    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(mode=MongoDBMode.ATLAS)

    assert exc_info.value.setting_name == "uri"
    assert "mongodb+srv://" in str(exc_info.value)


def test_mongodb_atlas_cluster_name_removed_as_dead_config() -> None:
    """R141-F16: ``atlas_cluster_name`` was accepted but zero-consumed dead
    config (only ever echoed in the ATLAS error text). The field is removed;
    re-adding it must reject via ``extra="forbid"`` instead of being silently
    accepted, mirroring the R25-H RocketMQ tombstone."""
    with pytest.raises(ValidationError) as exc_info:
        MongoDBSettings(
            mode=MongoDBMode.ATLAS,
            atlas_cluster_name="cluster0",  # type: ignore[call-overload]
        )
    errors = exc_info.value.errors()
    assert errors
    assert all(error["type"] == "value_error" for error in errors)
    assert all(error["loc"] == ("configuration",) for error in errors)


@pytest.mark.parametrize(
    ("mode", "uri", "expected_setting"),
    [
        (
            MongoDBMode.STANDALONE,
            "http://alice:super-secret@mongo.internal/database",
            "uri",
        ),
        (
            MongoDBMode.REPLICA_SET,
            "mongodb://alice:super-secret@mongo.internal/database",
            "uri",
        ),
    ],
)
def test_mongodb_configuration_errors_do_not_retain_uri_credentials(
    mode: MongoDBMode,
    uri: str,
    expected_setting: str,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(mode=mode, uri=uri)

    assert exc_info.value.setting_name == expected_setting
    assert "super-secret" not in str(exc_info.value)
    assert "super-secret" not in repr(exc_info.value.__dict__)


def test_rabbitmq_url_populates_connection_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SCRAPY_RABBITMQ_USERNAME", raising=False)
    monkeypatch.delenv("SCRAPY_RABBITMQ_PASSWORD", raising=False)

    settings = RabbitMQSettings(
        url="amqps://rabbit.internal:5671/%2F",  # type: ignore[arg-type]
        username="crawler",
        password="p@ss",  # type: ignore[arg-type]
    )

    assert settings.host == "rabbit.internal"
    assert settings.port == 5671
    assert settings.username == "crawler"
    assert settings.password.get_secret_value() == "p@ss"
    assert settings.virtual_host == "/"
    assert settings.ssl_enabled is True
    assert settings.url is not None
    assert "p@ss" not in repr(settings)


def test_rabbitmq_url_normalizes_ipv6_host_for_pika(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SCRAPY_RABBITMQ_USERNAME", raising=False)
    monkeypatch.delenv("SCRAPY_RABBITMQ_PASSWORD", raising=False)

    settings = RabbitMQSettings(
        url="amqp://[::1]/vhost",  # type: ignore[arg-type]
        username="crawler",
        password="secret",  # type: ignore[arg-type]
    )

    assert settings.host == "::1"
    assert settings.port == 5672


def test_rabbitmq_explicit_fields_override_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SCRAPY_RABBITMQ_USERNAME", raising=False)
    monkeypatch.delenv("SCRAPY_RABBITMQ_PASSWORD", raising=False)

    settings = RabbitMQSettings(
        url="amqps://url-host/url-vhost",  # type: ignore[arg-type]
        host="explicit-host",
        port=5679,
        username="explicit-user",
        password="explicit-pass",  # type: ignore[arg-type]
        virtual_host="explicit-vhost",
        ssl_enabled=True,
    )

    assert settings.host == "explicit-host"
    assert settings.port == 5679
    assert settings.username == "explicit-user"
    assert settings.password.get_secret_value() == "explicit-pass"
    assert settings.virtual_host == "explicit-vhost"
    assert settings.ssl_enabled is True


def test_rabbitmq_url_rejects_non_amqp_scheme_without_leaking_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SCRAPY_RABBITMQ_USERNAME", raising=False)
    monkeypatch.delenv("SCRAPY_RABBITMQ_PASSWORD", raising=False)

    with pytest.raises(ConfigurationError) as exc_info:
        RabbitMQSettings(  # type: ignore[call-arg]
            url="https://user:do-not-leak@rabbit.internal/vhost"  # type: ignore[arg-type]
        )

    assert exc_info.value.setting_name == "url"
    assert "do-not-leak" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


@pytest.mark.parametrize(
    ("settings_cls", "expected_endpoint"),
    [
        (SqsSettings, "http://localhost:4566"),
        (DynamoDBSettings, "http://localhost:4566"),
    ],
)
def test_aws_standalone_defaults_to_local_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    settings_cls: type[SqsSettings] | type[DynamoDBSettings],
    expected_endpoint: str,
) -> None:
    monkeypatch.delenv("SCRAPY_SQS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("SCRAPY_DYNAMODB_ENDPOINT_URL", raising=False)

    settings = settings_cls()

    assert settings.endpoint_url == expected_endpoint


@pytest.mark.parametrize(
    ("settings_cls", "mode", "endpoint_env"),
    [
        pytest.param(SqsSettings, SqsMode.CLOUD, "SCRAPY_SQS_ENDPOINT_URL", id="sqs"),
        pytest.param(
            DynamoDBSettings,
            DynamoDBMode.CLOUD,
            "SCRAPY_DYNAMODB_ENDPOINT_URL",
            id="dynamodb",
        ),
    ],
)
def test_aws_cloud_keeps_default_endpoint_chain(
    monkeypatch: pytest.MonkeyPatch,
    settings_cls: type[SqsSettings] | type[DynamoDBSettings],
    mode: SqsMode | DynamoDBMode,
    endpoint_env: str,
) -> None:
    monkeypatch.delenv(endpoint_env, raising=False)

    settings = settings_cls(mode=mode)  # type: ignore[arg-type]

    assert settings.endpoint_url is None


@pytest.mark.parametrize(
    ("settings_cls", "mode"),
    [
        pytest.param(SqsSettings, SqsMode.CLOUD, id="sqs"),
        pytest.param(DynamoDBSettings, DynamoDBMode.CLOUD, id="dynamodb"),
    ],
)
def test_aws_cloud_rejects_explicit_plaintext_endpoint(
    settings_cls: type[SqsSettings] | type[DynamoDBSettings],
    mode: SqsMode | DynamoDBMode,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        settings_cls(  # type: ignore[call-arg]
            mode=mode,
            endpoint_url="http://aws-proxy.internal:4566",
        )

    assert exc_info.value.setting_name == "endpoint_url"
    assert "HTTPS" in str(exc_info.value)


@pytest.mark.parametrize("settings_cls", [SqsSettings, DynamoDBSettings])
def test_aws_endpoint_rejects_userinfo_without_leaking_it(
    settings_cls: type[SqsSettings] | type[DynamoDBSettings],
) -> None:
    password = "do-not-leak-endpoint-password"

    with pytest.raises(ConfigurationError) as exc_info:
        settings_cls(endpoint_url=f"http://operator:{password}@localhost:4566")

    assert exc_info.value.setting_name == "endpoint_url"
    assert password not in str(exc_info.value)
    assert password not in repr(exc_info.value)
    assert password not in repr(exc_info.value.setting_value)
    assert exc_info.value.__cause__ is None


@pytest.mark.parametrize("settings_cls", [SqsSettings, DynamoDBSettings])
@pytest.mark.parametrize(
    "endpoint_url",
    [
        "https://aws.example:4566?X-Amz-Security-Token=endpoint-secret",
        "https://aws.example:4566#endpoint-secret",
        "https://aws.example" + chr(127) + ":4566",
        "https://aws" + chr(0) + ".example:4566",
    ],
)
def test_aws_endpoint_rejects_queries_fragments_and_hidden_controls(
    settings_cls: type[SqsSettings] | type[DynamoDBSettings], endpoint_url: str
) -> None:
    """Custom AWS endpoints cannot smuggle URL data or hidden controls."""
    with pytest.raises(ConfigurationError) as exc_info:
        settings_cls(mode="cloud", endpoint_url=endpoint_url)

    error = exc_info.value
    assert error.setting_name == "endpoint_url"
    assert "endpoint-secret" not in str(error)
    assert endpoint_url not in repr(error)
    assert error.__cause__ is None


@pytest.mark.parametrize("settings_cls", [SqsSettings, DynamoDBSettings])
@pytest.mark.parametrize(
    ("access_key", "secret_key", "invalid_setting"),
    [
        ("", None, "aws_access_key_id"),
        (None, "", "aws_secret_access_key"),
        ("", "", "aws_access_key_id"),
        ("AKIAEXAMPLE", "", "aws_secret_access_key"),
        ("", "secret", "aws_access_key_id"),
    ],
)
def test_aws_explicit_empty_credentials_do_not_fall_back_to_ambient_chain(
    settings_cls: type[SqsSettings] | type[DynamoDBSettings],
    access_key: str | None,
    secret_key: str | None,
    invalid_setting: str,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        settings_cls(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    assert exc_info.value.setting_name == invalid_setting


def test_flat_backend_setting_typo_fails_fast() -> None:
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "mongodb",
            "SCRAPY_MONGO_DATABSE": "misspelled",
        }
    )

    with pytest.raises(ConfigurationError) as exc_info:
        _resolve_queue(settings)

    assert exc_info.value.setting_name == "backend_settings"
    assert "SCRAPY_MONGO_DATABASE" in str(exc_info.value)


def test_nested_backend_setting_typo_fails_fast() -> None:
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "mongodb",
            "SCRAPY_BACKEND_SETTINGS": {"databse": "misspelled"},
        }
    )

    with pytest.raises(ConfigurationError) as exc_info:
        _resolve_queue(settings)

    assert exc_info.value.setting_name == "backend_settings"
    assert "database" in str(exc_info.value)


def test_unknown_backend_setting_does_not_retain_the_raw_key() -> None:
    marker = "nested-backend-setting-secret-marker"
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "mongodb",
            "SCRAPY_BACKEND_SETTINGS": {marker: "value"},
        }
    )

    with pytest.raises(ConfigurationError) as exc_info:
        _resolve_queue(settings)

    error = exc_info.value
    assert error.setting_name == "backend_settings"
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert error.__cause__ is None
    assert error.__context__ is None


def test_backend_setting_typo_in_environment_fails_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCRAPY_MONGO_DATABSE", "misspelled")
    settings = ScrapySettings({"SCRAPY_BACKEND_TYPE": "mongodb"})

    with pytest.raises(ConfigurationError) as exc_info:
        _resolve_queue(settings)

    assert exc_info.value.setting_name == "backend_settings"


def test_nested_connection_manager_retry_settings_remain_valid() -> None:
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "mongodb",
            "SCRAPY_BACKEND_SETTINGS": {
                "uri": "mongodb://mongo.internal:27017",
                "retry_attempts": 2,
                "retry_delay": 0.25,
            },
        }
    )

    _, backend_settings = _resolve_queue(settings)

    assert backend_settings["uri"] == "mongodb://mongo.internal:27017"
    manager = ConnectionManager("mongodb", backend_settings)
    assert manager._retry_policy() == (2, 0.25)


def test_global_retry_settings_reach_connection_manager() -> None:
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "redis",
            "SCRAPY_RETRY_ATTEMPTS": 7,
            "SCRAPY_RETRY_DELAY": 0.125,
        }
    )

    backend_type, backend_settings = _resolve_queue(settings)

    manager = ConnectionManager(backend_type, backend_settings)
    assert manager._retry_policy() == (7, 0.125)


def test_redis_socket_timeouts_reject_non_finite_and_over_cap() -> None:
    """R23-B: socket_timeout/socket_connect_timeout used Field(ge=0) which accepts
    inf/NaN/huge-finite; redis-py then calls socket.settimeout(inf) which raises
    OverflowError (an ArithmeticError, NOT an OSError) that escapes redis-py's
    except-OSError trap and wedges connect retries. le=86400 rejects inf/NaN
    (inf<=86400 is False, nan<=86400 is False) and caps at a 24h ceiling."""
    with pytest.raises(ValidationError):
        RedisSettings(socket_timeout=float("inf"))
    with pytest.raises(ValidationError):
        RedisSettings(socket_timeout=float("nan"))
    with pytest.raises(ValidationError):
        RedisSettings(socket_timeout=1e10)
    with pytest.raises(ValidationError):
        RedisSettings(socket_connect_timeout=float("inf"))
    ok = RedisSettings(socket_timeout=86400.0, socket_connect_timeout=30.0)
    assert ok.socket_timeout == 86400.0


def test_elasticsearch_request_timeout_rejects_non_finite_and_over_cap() -> None:
    """R23-C: request_timeout used Field(ge=0) which accepted inf; the ES client
    transport converts it to a socket timeout and socket.settimeout(inf) raises
    OverflowError, wrapping every op (push/pop/store/etc) in ConnectionError.
    le=86400 rejects inf/NaN/huge-finite."""
    with pytest.raises(ValidationError):
        ElasticSearchSettings(request_timeout=float("inf"))
    with pytest.raises(ValidationError):
        ElasticSearchSettings(request_timeout=float("nan"))
    with pytest.raises(ValidationError):
        ElasticSearchSettings(request_timeout=1e10)
    assert ElasticSearchSettings(request_timeout=86400.0).request_timeout == 86400.0


def test_rabbitmq_heartbeat_caps_at_amqp_unsigned_short_bound() -> None:
    """R23-D: heartbeat used Field(ge=0) with no upper bound; the AMQP
    Connection.Tune-Ok frame marshals heartbeat as struct.pack('>H', heartbeat),
    so any value >65535 crashes negotiation with an opaque struct.error deep in
    pika. le=65535 mirrors the sibling max_priority le=255 (same file) which
    already enforces its AMQP protocol bound."""
    with pytest.raises(ValidationError):
        RabbitMQSettings(heartbeat=70000)
    assert RabbitMQSettings(heartbeat=65535).heartbeat == 65535
    assert RabbitMQSettings(heartbeat=0).heartbeat == 0


def test_monitor_pop_rate_window_capped_at_24h() -> None:
    """R23-E: monitor_pop_rate_window_s was unbounded (gt=0 only); the
    pop-timestamp deque retains ~pop_rate x window_s entries, so a huge-finite
    window disables eviction and grows without bound (soft-OOM-by-misconfig).
    le=86400.0 (24h) covers any legit rolling-rate window while rejecting the
    pathological case."""
    with pytest.raises(ValidationError):
        Settings(monitor_pop_rate_window_s=1e6)
    with pytest.raises(ValidationError):
        Settings(monitor_pop_rate_window_s=float("inf"))
    assert (
        Settings(monitor_pop_rate_window_s=86400.0).monitor_pop_rate_window_s == 86400.0
    )
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "rabbitmq",
            "SCRAPY_RABBITMQ_USERNAME": "crawler",
            "SCRAPY_RABBITMQ_PASSWORD": "secret",
            "SCRAPY_RABBITMQ_RETRY_DELAY": 9,
            "SCRAPY_RETRY_DELAY": 0.25,
        }
    )

    backend_type, backend_settings = _resolve_queue(settings)
    manager = ConnectionManager(backend_type, backend_settings)
    backend = manager._create_backend()

    assert manager._retry_policy() == (3, 0.25)
    assert isinstance(backend, RabbitMQBackend)
    assert backend.config.retry_delay == 9
