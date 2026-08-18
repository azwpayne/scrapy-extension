"""Canonical scalar grammar for every bundled settings model."""

from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import (
    DynamoDBSettings,
    ElasticSearchSettings,
    KafkaSettings,
    MemcachedSettings,
    MongoDBSettings,
    PulsarSettings,
    RabbitMQSettings,
    RedisSettings,
    RocketMQSettings,
    Settings,
    SqsSettings,
)

_BUNDLED_BOOL_FIELDS: tuple[tuple[type[Any], str, str], ...] = (
    (Settings, "dedup_strict", "SCRAPY_DEDUP_STRICT"),
    (RedisSettings, "retry_on_timeout", "SCRAPY_REDIS_RETRY_ON_TIMEOUT"),
    (MongoDBSettings, "journal", "SCRAPY_MONGO_JOURNAL"),
    (
        ElasticSearchSettings,
        "retry_on_timeout",
        "SCRAPY_ELASTICSEARCH_RETRY_ON_TIMEOUT",
    ),
    (MemcachedSettings, "allow_flush_all", "SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL"),
    (KafkaSettings, "enable_auto_commit", "SCRAPY_KAFKA_ENABLE_AUTO_COMMIT"),
    (
        PulsarSettings,
        "allow_insecure_connection",
        "SCRAPY_PULSAR_ALLOW_INSECURE_CONNECTION",
    ),
    (RabbitMQSettings, "durable", "SCRAPY_RABBITMQ_DURABLE"),
    (RocketMQSettings, "tls_enabled", "SCRAPY_ROCKETMQ_TLS_ENABLED"),
    (SqsSettings, "allow_remote_http", "SCRAPY_SQS_ALLOW_REMOTE_HTTP"),
    (
        DynamoDBSettings,
        "allow_unfenced_legacy_clear",
        "SCRAPY_DYNAMODB_ALLOW_UNFENCED_LEGACY_CLEAR",
    ),
)

_BUNDLED_INTEGER_FIELDS: tuple[tuple[type[Any], str, str, int], ...] = (
    (Settings, "retry_attempts", "SCRAPY_RETRY_ATTEMPTS", 8),
    (RedisSettings, "db", "SCRAPY_REDIS_DB", 8),
    (MongoDBSettings, "min_pool_size", "SCRAPY_MONGO_MIN_POOL_SIZE", 8),
    (
        ElasticSearchSettings,
        "max_retries",
        "SCRAPY_ELASTICSEARCH_MAX_RETRIES",
        8,
    ),
    (MemcachedSettings, "port", "SCRAPY_MEMCACHED_PORT", 11212),
    (KafkaSettings, "retries", "SCRAPY_KAFKA_RETRIES", 8),
    (
        PulsarSettings,
        "negative_ack_redelivery_delay_ms",
        "SCRAPY_PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MS",
        8,
    ),
    (RabbitMQSettings, "heartbeat", "SCRAPY_RABBITMQ_HEARTBEAT", 8),
    (RocketMQSettings, "send_timeout", "SCRAPY_ROCKETMQ_SEND_TIMEOUT", 8),
    (SqsSettings, "visibility_timeout", "SCRAPY_SQS_VISIBILITY_TIMEOUT", 8),
)


def _required_kwargs(settings_type: type[Any]) -> dict[str, object]:
    if settings_type is RabbitMQSettings:
        return {"username": "crawler", "password": "secret", "ssl_enabled": True}
    if settings_type is PulsarSettings:
        return {"service_url": "pulsar+ssl://localhost:6651"}
    return {}


@pytest.mark.parametrize(
    "settings_type,field_name,_env_name",
    _BUNDLED_BOOL_FIELDS,
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [(True, True), (False, False)],
)
def test_bundled_boolean_direct_matrix_accepts_only_exact_values(
    settings_type: type[Any],
    field_name: str,
    _env_name: str,
    raw_value: object,
    expected: bool,
) -> None:
    settings = settings_type(
        **_required_kwargs(settings_type), **{field_name: raw_value}
    )

    assert getattr(settings, field_name) is expected


@pytest.mark.parametrize(
    "settings_type,field_name,_env_name",
    _BUNDLED_BOOL_FIELDS,
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
@pytest.mark.parametrize(
    "raw_value",
    [1, 0, "true", "FALSE", "1", "0", "yes", "on", " true ", 1.0, [], {}],
)
def test_bundled_boolean_direct_matrix_rejects_ambiguous_values(
    settings_type: type[Any], field_name: str, _env_name: str, raw_value: object
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        settings_type(**_required_kwargs(settings_type), **{field_name: raw_value})

    assert exc_info.value.setting_name == field_name
    assert exc_info.value.setting_value is None


@pytest.mark.parametrize(
    "settings_type,field_name,env_name",
    _BUNDLED_BOOL_FIELDS,
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
@pytest.mark.parametrize(("raw_value", "expected"), [("TrUe", True), ("0", False)])
def test_bundled_boolean_environment_matrix_remains_usable(
    monkeypatch: pytest.MonkeyPatch,
    settings_type: type[Any],
    field_name: str,
    env_name: str,
    raw_value: str,
    expected: bool,
) -> None:
    monkeypatch.setenv(env_name, raw_value)
    if settings_type is RabbitMQSettings:
        monkeypatch.setenv("SCRAPY_RABBITMQ_USERNAME", "crawler")
        monkeypatch.setenv("SCRAPY_RABBITMQ_PASSWORD", "secret")
        monkeypatch.setenv("SCRAPY_RABBITMQ_SSL_ENABLED", "true")
    if settings_type is PulsarSettings:
        monkeypatch.setenv("SCRAPY_PULSAR_SERVICE_URL", "pulsar+ssl://localhost:6651")

    assert getattr(settings_type(), field_name) is expected


@pytest.mark.parametrize(
    "settings_type,field_name,_env_name,_expected",
    _BUNDLED_INTEGER_FIELDS,
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
@pytest.mark.parametrize(
    "raw_value",
    [True, False, 1.0, 0.0, "1", "01", "+1", " 1", "1 ", "1.0", [], {}],
)
def test_bundled_integer_direct_matrix_rejects_coercion(
    settings_type: type[Any],
    field_name: str,
    _env_name: str,
    _expected: int,
    raw_value: object,
) -> None:
    expected_error = (
        ValidationError if settings_type is Settings else ConfigurationError
    )
    with pytest.raises(expected_error) as exc_info:
        settings_type(**_required_kwargs(settings_type), **{field_name: raw_value})

    if isinstance(exc_info.value, ConfigurationError):
        assert exc_info.value.setting_name == field_name
        assert exc_info.value.setting_value is None
    else:
        assert all(error["input"] is None for error in exc_info.value.errors())


@pytest.mark.parametrize(
    "settings_type,field_name,env_name,expected",
    _BUNDLED_INTEGER_FIELDS,
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
def test_bundled_integer_direct_and_environment_matrix_accepts_exact_and_decimal(
    monkeypatch: pytest.MonkeyPatch,
    settings_type: type[Any],
    field_name: str,
    env_name: str,
    expected: int,
) -> None:
    direct = settings_type(**_required_kwargs(settings_type), **{field_name: expected})
    assert getattr(direct, field_name) == expected

    monkeypatch.setenv(env_name, str(expected))
    if settings_type is RabbitMQSettings:
        monkeypatch.setenv("SCRAPY_RABBITMQ_USERNAME", "crawler")
        monkeypatch.setenv("SCRAPY_RABBITMQ_PASSWORD", "secret")
        monkeypatch.setenv("SCRAPY_RABBITMQ_SSL_ENABLED", "true")
    assert getattr(settings_type(), field_name) == expected


@pytest.mark.parametrize(
    "settings_type,field_name",
    [
        (Settings, "retry_delay"),
        (RedisSettings, "socket_timeout"),
        (ElasticSearchSettings, "request_timeout"),
        (MemcachedSettings, "connect_timeout"),
    ],
)
@pytest.mark.parametrize(
    "raw_value",
    [True, False, "1", "1.0", float("nan"), float("inf"), float("-inf")],
)
def test_bundled_float_duration_fields_reject_coercion_and_nonfinite_values(
    settings_type: type[Any], field_name: str, raw_value: object
) -> None:
    with pytest.raises((ConfigurationError, ValidationError)):
        settings_type(**{field_name: raw_value})


@pytest.mark.parametrize("field_name", ["connect_timeout", "socket_timeout"])
@pytest.mark.parametrize(
    "raw_value",
    [" 1 ", "+1", "01", "1e2", "1.", ".1", "-1", "nan", "inf"],
)
def test_memcached_timeout_direct_text_is_rejected(
    field_name: str, raw_value: str
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        MemcachedSettings(**{field_name: raw_value})

    assert exc_info.value.setting_name == field_name
    assert exc_info.value.setting_value is None


@pytest.mark.parametrize("field_name", ["connect_timeout", "socket_timeout"])
@pytest.mark.parametrize(
    "raw_value",
    [" 1 ", "+1", "01", "1e2", "1.", ".1", "-1", "nan", "inf"],
)
def test_memcached_timeout_environment_text_is_canonical(
    monkeypatch: pytest.MonkeyPatch, field_name: str, raw_value: str
) -> None:
    monkeypatch.setenv(f"SCRAPY_MEMCACHED_{field_name.upper()}", raw_value)

    with pytest.raises(ConfigurationError) as exc_info:
        MemcachedSettings()

    assert exc_info.value.setting_value is None


@pytest.mark.parametrize(
    "raw_value,expected", [("1", 1.0), ("1.25", 1.25), ("0.5", 0.5)]
)
def test_memcached_timeout_environment_accepts_canonical_decimal(
    monkeypatch: pytest.MonkeyPatch, raw_value: str, expected: float
) -> None:
    monkeypatch.setenv("SCRAPY_MEMCACHED_CONNECT_TIMEOUT", raw_value)

    assert MemcachedSettings().connect_timeout == expected


def test_programmatic_scalar_strings_are_rejected_by_model_validate() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        KafkaSettings.model_validate({"retries": "8"}, strict=True)

    assert exc_info.value.setting_name == "retries"
    assert exc_info.value.setting_value is None


def test_model_validate_redacts_rejected_scalar_without_mutating_input() -> None:
    marker = "model-validate-scalar-private-marker"
    supplied = {"retries": f"8-{marker}"}
    original = supplied.copy()

    with pytest.raises(ConfigurationError) as exc_info:
        KafkaSettings.model_validate(supplied, strict=True)

    error = exc_info.value
    assert supplied == original
    assert str(error) == "Settings contain an invalid configuration value."
    assert error.setting_name == "retries"
    assert error.setting_value is None
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    for frame, _ in traceback.walk_tb(error.__traceback__):
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            assert marker not in repr(frame.f_locals)


def test_model_validate_redacts_pydantic_input_for_rejected_scalar() -> None:
    marker = "model-validate-pydantic-private-marker"
    supplied = {"retry_delay": marker}
    original = supplied.copy()

    with pytest.raises(ValidationError) as exc_info:
        Settings.model_validate(supplied, strict=True)

    error = exc_info.value
    assert supplied == original
    assert all(detail["input"] is None for detail in error.errors())
    assert {detail["msg"] for detail in error.errors()} == {
        "Value error, Invalid configuration value."
    }
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in error.json()
    assert marker not in "".join(traceback.format_exception(error))
    for frame, _ in traceback.walk_tb(error.__traceback__):
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            assert marker not in repr(frame.f_locals)


def test_documented_negative_integer_text_remains_available_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCRAPY_QUEUE_DELAY_MAX_HELD", "-1")

    assert Settings().queue_delay_max_held == -1


def test_init_scalar_overrides_invalid_environment_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCRAPY_KAFKA_RETRIES", "not-canonical")

    assert KafkaSettings(retries=8).retries == 8


def test_environment_scalar_overrides_invalid_dotenv_text(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("SCRAPY_KAFKA_RETRIES=not-canonical\n", encoding="utf-8")
    monkeypatch.setenv("SCRAPY_KAFKA_RETRIES", "8")

    assert KafkaSettings(_env_file=dotenv).retries == 8


def test_scalar_errors_do_not_retain_raw_input() -> None:
    marker = "scalar-coercion-secret-marker"

    with pytest.raises(ConfigurationError) as exc_info:
        KafkaSettings(retries=f"1{marker}")

    error = exc_info.value
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
