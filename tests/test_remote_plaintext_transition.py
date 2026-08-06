"""Compatibility warnings for unauthenticated remote plaintext transports."""

from __future__ import annotations

import warnings
from typing import Any

import pytest

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import (
    ElasticSearchSettings,
    KafkaSettings,
    MongoDBSettings,
    PulsarSettings,
    RedisSettings,
    RocketMQSettings,
)

_REMOTE_PLAINTEXT_CASES: list[tuple[str, type[Any], dict[str, object], str]] = [
    (
        "redis",
        RedisSettings,
        {"host": "redis.internal"},
        "SCRAPY_REDIS_ALLOW_REMOTE_PLAINTEXT",
    ),
    (
        "mongodb",
        MongoDBSettings,
        {"uri": "mongodb://mongo.internal:27017"},
        "SCRAPY_MONGO_ALLOW_REMOTE_PLAINTEXT",
    ),
    (
        "elasticsearch",
        ElasticSearchSettings,
        {"hosts": ["http://es.internal:9200"]},
        "SCRAPY_ELASTICSEARCH_ALLOW_REMOTE_PLAINTEXT",
    ),
    (
        "kafka",
        KafkaSettings,
        {"bootstrap_servers": "kafka.internal:9092"},
        "SCRAPY_KAFKA_ALLOW_REMOTE_PLAINTEXT",
    ),
    (
        "pulsar",
        PulsarSettings,
        {"service_url": "pulsar://pulsar.internal:6650"},
        "SCRAPY_PULSAR_ALLOW_REMOTE_PLAINTEXT",
    ),
    (
        "rocketmq",
        RocketMQSettings,
        {"namesrv_address": "rocketmq.internal:8081"},
        "SCRAPY_ROCKETMQ_ALLOW_REMOTE_PLAINTEXT",
    ),
]


def _future_warnings(
    factory: type[Any], **kwargs: object
) -> list[warnings.WarningMessage]:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", FutureWarning)
        factory(**kwargs)
    return [
        warning for warning in captured if issubclass(warning.category, FutureWarning)
    ]


@pytest.mark.parametrize(
    ("backend", "settings_type", "remote_kwargs", "_environment_name"),
    _REMOTE_PLAINTEXT_CASES,
)
def test_loopback_plaintext_defaults_do_not_emit_transition_warning(
    backend: str,
    settings_type: type[Any],
    remote_kwargs: dict[str, object],
    _environment_name: str,
) -> None:
    """The existing local defaults remain quiet and usable during migration."""
    del backend, remote_kwargs

    settings = settings_type()

    assert settings.allow_remote_plaintext is False
    assert not _future_warnings(settings_type)


@pytest.mark.parametrize(
    ("backend", "settings_type", "remote_kwargs", "_environment_name"),
    _REMOTE_PLAINTEXT_CASES,
)
def test_remote_unauthenticated_plaintext_emits_transition_warning(
    backend: str,
    settings_type: type[Any],
    remote_kwargs: dict[str, object],
    _environment_name: str,
) -> None:
    """Remote plaintext stays compatible for now but becomes conspicuous."""
    del _environment_name

    with pytest.warns(
        FutureWarning,
        match=(
            rf"(?i)Remote unauthenticated plaintext {backend} connections are deprecated"
        ),
    ):
        settings = settings_type(**remote_kwargs)

    assert settings.allow_remote_plaintext is False


@pytest.mark.parametrize(
    ("backend", "settings_type", "remote_kwargs", "environment_name"),
    _REMOTE_PLAINTEXT_CASES,
)
def test_explicit_remote_plaintext_opt_in_suppresses_transition_warning(
    backend: str,
    settings_type: type[Any],
    remote_kwargs: dict[str, object],
    environment_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The setting is an intentional, namespaced acknowledgement of the risk."""
    del backend
    monkeypatch.setenv(environment_name, "true")

    settings = settings_type(**remote_kwargs)

    assert settings.allow_remote_plaintext is True
    assert not _future_warnings(settings_type, **remote_kwargs)


@pytest.mark.parametrize(
    ("settings_type", "insecure_kwargs", "setting_name"),
    [
        (
            RedisSettings,
            {
                "host": "redis.internal",
                "password": "secret",
                "allow_remote_plaintext": True,
            },
            "ssl_enabled",
        ),
        (
            MongoDBSettings,
            {
                "uri": "mongodb://mongo.internal:27017",
                "username": "crawler",
                "password": "secret",
                "allow_remote_plaintext": True,
            },
            "tls_enabled",
        ),
        (
            ElasticSearchSettings,
            {
                "hosts": ["http://es.internal:9200"],
                "api_key": "secret",
                "allow_remote_plaintext": True,
            },
            "hosts",
        ),
        (
            KafkaSettings,
            {
                "bootstrap_servers": "kafka.internal:9092",
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "crawler",
                "sasl_password": "secret",
                "allow_remote_plaintext": True,
            },
            "security_protocol",
        ),
        (
            PulsarSettings,
            {
                "service_url": "pulsar://pulsar.internal:6650",
                "auth_token": "secret",
                "allow_remote_plaintext": True,
            },
            "service_url",
        ),
        (
            RocketMQSettings,
            {
                "namesrv_address": "rocketmq.internal:8081",
                "access_key": "key",
                "secret_key": "secret",
                "allow_remote_plaintext": True,
            },
            "tls_enabled",
        ),
    ],
)
def test_authenticated_plaintext_still_fails_fast(
    settings_type: type[Any],
    insecure_kwargs: dict[str, object],
    setting_name: str,
) -> None:
    """The compatibility escape hatch never weakens credential/TLS policy."""
    with pytest.raises(ConfigurationError) as exc_info:
        settings_type(**insecure_kwargs)

    assert exc_info.value.setting_name == setting_name


@pytest.mark.parametrize(
    ("settings_type", "secure_kwargs"),
    [
        (
            RedisSettings,
            {
                "host": "redis.internal",
                "password": "secret",
                "ssl_enabled": True,
                "ssl_cafile": "/tls/ca.pem",
            },
        ),
        (
            MongoDBSettings,
            {
                "uri": "mongodb://mongo.internal:27017",
                "username": "crawler",
                "password": "secret",
                "tls_enabled": True,
            },
        ),
        (
            ElasticSearchSettings,
            {"hosts": ["https://es.internal:9200"], "api_key": "secret"},
        ),
        (
            KafkaSettings,
            {
                "bootstrap_servers": "kafka.internal:9092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "crawler",
                "sasl_password": "secret",
            },
        ),
        (
            PulsarSettings,
            {
                "service_url": "pulsar+ssl://pulsar.internal:6651",
                "auth_token": "secret",
            },
        ),
        (
            RocketMQSettings,
            {
                "namesrv_address": "rocketmq.internal:8081",
                "access_key": "key",
                "secret_key": "secret",
                "tls_enabled": True,
            },
        ),
    ],
)
def test_authenticated_tls_paths_do_not_emit_transition_warning(
    settings_type: type[Any], secure_kwargs: dict[str, object]
) -> None:
    """Verified transports remain the migration destination without warning noise."""
    assert not _future_warnings(settings_type, **secure_kwargs)
