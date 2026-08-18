"""Fail-closed remote TLS verification and ignored-intent regression tests."""

from __future__ import annotations

import traceback

import pytest

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.backends.pulsar import PulsarBackend
from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import (
    ElasticSearchSettings,
    MongoDBSettings,
    PulsarSettings,
    RedisSettings,
)

_TLS_LOOPBACK_URLS = (
    "https://localhost:9200",
    "https://localhost.:9200",
    "https://127.0.0.1:9200",
    "https://[::1]:9200",
)
_TLS_REMOTE_HOSTS = (
    "es.example",
    "attacker.localhost",
    "localhost..",
    "127.1",
    "0177.0.0.1",
    "2130706433",
    "[::ffff:127.0.0.1]",
    "0.0.0.0",
    "[::]",
    "[::1%25lo0]",
    "192.0.2.1",
    "[2001:db8::1]",
)


@pytest.mark.parametrize(
    ("factory", "kwargs", "setting_name"),
    [
        (
            RedisSettings,
            {
                "host": "redis.example",
                "ssl_enabled": True,
                "ssl_cafile": "/tls/ca.pem",
                "ssl_check_hostname": False,
            },
            "ssl_check_hostname",
        ),
        (
            ElasticSearchSettings,
            {"hosts": ["https://es.example:9200"], "verify_certs": False},
            "verify_certs",
        ),
        (
            PulsarSettings,
            {
                "service_url": "pulsar+ssl://pulsar.example:6651",
                "allow_insecure_connection": True,
            },
            "allow_insecure_connection",
        ),
        (
            PulsarSettings,
            {
                "service_url": "pulsar+ssl://pulsar.example:6651",
                "tls_validate_hostname": False,
            },
            "tls_validate_hostname",
        ),
    ],
)
def test_remote_tls_requires_certificate_and_hostname_verification(
    factory: type[object], kwargs: dict[str, object], setting_name: str
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        factory(**kwargs)  # type: ignore[call-arg]

    assert exc_info.value.setting_name == setting_name
    assert exc_info.value.__cause__ is None


@pytest.mark.parametrize(
    ("factory", "kwargs"),
    [
        (
            RedisSettings,
            {
                "host": "redis.example",
                "ssl_enabled": True,
                "ssl_cafile": "/tls/ca.pem",
                "ssl_check_hostname": True,
            },
        ),
        (
            ElasticSearchSettings,
            {"hosts": ["https://es.example:9200"], "verify_certs": True},
        ),
        (
            PulsarSettings,
            {
                "service_url": "pulsar+ssl://pulsar.example:6651",
                "allow_insecure_connection": False,
                "tls_validate_hostname": True,
            },
        ),
    ],
)
def test_verified_remote_tls_settings_are_accepted(
    factory: type[object], kwargs: dict[str, object]
) -> None:
    factory(**kwargs)  # type: ignore[call-arg]


@pytest.mark.parametrize("url", _TLS_LOOPBACK_URLS)
def test_exact_loopback_tls_verification_opt_out_remains_available(url: str) -> None:
    settings = ElasticSearchSettings(hosts=[url], verify_certs=False)

    assert settings.verify_certs is False


@pytest.mark.parametrize(
    ("kwargs", "setting_name"),
    [
        ({"hosts": ["http://localhost:9200"], "ca_certs": "/tls/ca.pem"}, "ca_certs"),
        ({"hosts": ["http://localhost:9200"], "verify_certs": False}, "verify_certs"),
        (
            {
                "mode": "cloud",
                "cloud_id": "deployment:encoded",
                "api_key": "secret",
                "ca_certs": "/tls/ca.pem",
            },
            "ca_certs",
        ),
    ],
)
def test_elasticsearch_rejects_tls_settings_ignored_by_mode_or_scheme(
    kwargs: dict[str, object], setting_name: str
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        ElasticSearchSettings(**kwargs)

    assert exc_info.value.setting_name == setting_name


def _assert_marker_absent(error: BaseException, marker: str) -> None:
    rendered = "".join(traceback.format_exception(error))
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in rendered
    assert getattr(error, "setting_value", None) is None
    assert error.__cause__ is None
    assert error.__context__ is None


def test_redis_mutated_remote_tls_opt_out_rejects_before_sdk(mocker) -> None:
    marker = "redis-remote-tls-mutation-marker"
    settings = RedisSettings(
        ssl_enabled=True, ssl_cafile="/tls/ca.pem", ssl_check_hostname=False
    )
    settings.host = marker
    sdk = mocker.patch("scrapy_extension.backends.redis.Redis")

    with pytest.raises(ConfigurationError) as exc_info:
        RedisBackend(settings).connect()

    sdk.assert_not_called()
    _assert_marker_absent(exc_info.value, marker)


@pytest.mark.parametrize("host", _TLS_REMOTE_HOSTS)
def test_elasticsearch_mutated_remote_tls_opt_out_rejects_before_sdk(
    host: str, mocker
) -> None:
    settings = ElasticSearchSettings(
        hosts=["https://localhost:9200"], verify_certs=False
    )
    settings.hosts = [f"https://{host}:9200"]
    sdk = mocker.patch("scrapy_extension.backends.elasticsearch.Elasticsearch")

    with pytest.raises(ConfigurationError) as exc_info:
        ElasticSearchBackend(settings).connect()

    assert exc_info.value.setting_name == "verify_certs"
    sdk.assert_not_called()
    _assert_marker_absent(exc_info.value, host)


def test_pulsar_mutated_remote_tls_opt_out_rejects_before_sdk(mocker) -> None:
    marker = "pulsar-remote-tls-mutation-marker"
    settings = PulsarSettings(
        service_url="pulsar+ssl://localhost:6651", allow_insecure_connection=True
    )
    settings.service_url = f"pulsar+ssl://{marker}:6651"
    sdk = mocker.patch("scrapy_extension.backends.pulsar.pulsar.Client")

    with pytest.raises(ConfigurationError) as exc_info:
        PulsarBackend(settings).connect()

    sdk.assert_not_called()
    _assert_marker_absent(exc_info.value, marker)


def test_mongodb_mutated_plaintext_tls_intent_rejects_before_sdk(mocker) -> None:
    marker = "mongodb-ignored-ca-mutation-marker"
    settings = MongoDBSettings()
    settings.tls_ca_file = f"/tls/{marker}.pem"
    sdk = mocker.patch("scrapy_extension.backends.mongodb.MongoClient")

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBBackend(settings).connect()

    sdk.assert_not_called()
    _assert_marker_absent(exc_info.value, marker)
