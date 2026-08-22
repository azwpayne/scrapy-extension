"""Boundary contracts for strict broker authorities and backend names."""

from __future__ import annotations

import traceback

import pytest

from scrapy_extension.backends.memcached import MemcachedBackend
from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.backends.pulsar import PulsarBackend
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import (
    MemcachedSettings,
    MongoDBSettings,
    PulsarSettings,
    RabbitMQSettings,
    RocketMQSettings,
)
from scrapy_extension.settings._broker_endpoints import (
    normalize_kafka_broker_endpoints,
    normalize_rocketmq_namesrv_endpoints,
)
from scrapy_extension.settings.memcached import normalize_memcached_host
from scrapy_extension.settings.mongodb import (
    validate_mongodb_database,
    validate_mongodb_uri,
)
from scrapy_extension.settings.pulsar import validate_pulsar_subscription_name
from scrapy_extension.settings.rabbitmq import (
    normalize_rabbitmq_host,
    parse_rabbitmq_node,
)
from scrapy_extension.settings.rocketmq import validate_rocketmq_topic_prefix


class _MaliciousString(str):
    """A string subclass whose methods must never be used by endpoint parsers."""

    def strip(self, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("validation must reject the subclass before dispatch")

    def lower(self) -> str:
        raise AssertionError("validation must reject the subclass before dispatch")


@pytest.mark.parametrize(
    "host",
    [
        "cache\x00.internal",
        "cache\n.internal",
        "cache\t.internal",
        "cache internal",
        "caché.internal",
        "invalid..internal",
        "-invalid.internal",
        "invalid-.internal",
        "127.1",
        "2130706433",
        "[127.0.0.1]",
        "[fe80::1%eth0]",
        "cache.internal:11211",
        "https://cache.internal",
        "cache.internal ",
        "a" * 254,
        _MaliciousString("cache.internal"),
    ],
)
def test_memcached_authority_rejects_malformed_text_before_connection(
    host: object,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        MemcachedSettings(host=host, allow_remote_plaintext=True)  # type: ignore[arg-type]

    assert exc_info.value.setting_name == "host"
    assert host not in str(exc_info.value)
    assert host not in repr(exc_info.value)


@pytest.mark.parametrize(
    "host", ["localhost", "127.0.0.1", "[::1]", "2001:db8::1", "cache.internal"]
)
def test_memcached_accepts_supported_authority_kinds(host: str) -> None:
    settings = MemcachedSettings(host=host, allow_remote_plaintext=True)
    assert settings.host


@pytest.mark.parametrize(
    "node",
    [
        "node\x00.internal:5672",
        "node\n.internal:5672",
        "node\t.internal:5672",
        "node internal:5672",
        "nödé.internal:5672",
        "node..internal:5672",
        "node:0",
        "node:65536",
        "node:abc",
        "http://node:5672",
        "[2001:db8::1]:bad",
        "[fe80::1%eth0]:5672",
        _MaliciousString("node.internal:5672"),
    ],
)
def test_rabbitmq_cluster_authority_rejects_malformed_text(node: object) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        parse_rabbitmq_node(node, 5672)  # type: ignore[arg-type]

    assert exc_info.value.setting_name == "cluster_nodes"
    assert node not in str(exc_info.value)
    assert node not in "".join(traceback.format_exception(exc_info.value))


@pytest.mark.parametrize(
    "host",
    [
        "rabbit\x00.internal",
        "rabbit\n.internal",
        "rabbit\t.internal",
        "rabbit internal",
        "râbbit.internal",
        "rabbit..internal",
        "rabbit:5672",
        "http://rabbit.internal",
        "a" * 254,
        _MaliciousString("rabbit.internal"),
    ],
)
def test_rabbitmq_main_authority_rejects_malformed_text(host: object) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RabbitMQSettings(
            host=host,  # type: ignore[arg-type]
            username="crawler",
            password="secret",
            ssl_enabled=True,
        )

    assert exc_info.value.setting_name == "host"
    assert host not in str(exc_info.value)


@pytest.mark.parametrize(
    "host", ["localhost", "127.0.0.1", "::1", "2001:db8::1", "rabbit.internal"]
)
def test_rabbitmq_accepts_supported_authority_kinds(host: str) -> None:
    settings = RabbitMQSettings(
        host=host,
        username="crawler",
        password="secret",
        ssl_enabled=True,
    )
    assert settings.host


@pytest.mark.parametrize(
    "service_url",
    [
        "pulsar://broker\x00.internal:6650",
        "pulsar://broker\n.internal:6650",
        "pulsar://broker\t.internal:6650",
        "pulsar://broker internal:6650",
        "pulsar://bröker.internal:6650",
        "pulsar://broker..internal:6650",
        "pulsar://127.1:6650",
        "pulsar://broker:0",
        "pulsar://broker:65536",
        "pulsar://broker:abc",
        "pulsar://[2001:db8::1]:abc",
        "pulsar://[fe80::1%eth0]:6650",
        "pulsar://broker/path",
        "pulsar://user@broker:6650",
        "pulsar://localhost:6650 ",
        _MaliciousString("pulsar://localhost:6650"),
    ],
)
def test_pulsar_authority_rejects_malformed_text_before_sdk_io(
    service_url: object,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        PulsarSettings(service_url=service_url)  # type: ignore[arg-type]

    assert exc_info.value.setting_name == "service_url"
    assert "broker" not in str(exc_info.value)


@pytest.mark.parametrize(
    "service_url",
    [
        "pulsar://localhost:6650",
        "pulsar://127.0.0.1:6650",
        "pulsar://[::1]:6650",
        "pulsar://2001:db8::1",
        "pulsar://broker.internal:6650",
    ],
)
def test_pulsar_accepts_supported_authority_kinds(service_url: str) -> None:
    settings = PulsarSettings(
        service_url=service_url,
        allow_remote_plaintext=True,
    )
    assert settings.service_url


@pytest.mark.parametrize(
    "database",
    [
        "",
        "database name",
        "database\x00name",
        "database\nname",
        "database\tname",
        "database.name",
        "database/name",
        "database\\name",
        'database"name',
        "$database",
        "a" * 64,
        "é" * 32,
        _MaliciousString("database"),
    ],
)
def test_mongodb_database_name_rejects_grammar_and_size_boundaries(
    database: object,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_mongodb_database(database)

    assert exc_info.value.setting_name == "database"
    assert "database" not in str(exc_info.value)


def test_mongodb_database_name_uses_utf8_byte_limit() -> None:
    assert validate_mongodb_database("a" * 63) == "a" * 63
    assert validate_mongodb_database("é" * 31) == "é" * 31
    assert MongoDBSettings(database="é" * 31).database == "é" * 31


@pytest.mark.parametrize(
    "subscription_name",
    [
        "",
        "subscription name",
        "subscription\x00name",
        "subscription\nname",
        "subscription\tname",
        "sub/scription",
        "sub#scription",
        "\u0441\u0443\u0431scription",
        "a" * 256,
        _MaliciousString("subscription"),
    ],
)
def test_pulsar_subscription_name_rejects_grammar_and_size_boundaries(
    subscription_name: object,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_pulsar_subscription_name(subscription_name)

    assert exc_info.value.setting_name == "subscription_name"


def test_pulsar_subscription_name_accepts_character_limit() -> None:
    value = "a" * 255
    assert PulsarSettings(subscription_name=value).subscription_name == value


@pytest.mark.parametrize(
    "topic_prefix",
    [
        "",
        "topic prefix",
        "topic\x00prefix",
        "topic\nprefix",
        "topic\tprefix",
        "tópico",
        "topic/prefix",
        "a" * 128,
        _MaliciousString("topic-prefix"),
    ],
)
def test_rocketmq_topic_prefix_rejects_grammar_and_size_boundaries(
    topic_prefix: object,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        validate_rocketmq_topic_prefix(topic_prefix)

    assert exc_info.value.setting_name == "topic_prefix"


def test_rocketmq_topic_prefix_accepts_character_limit() -> None:
    value = "a" * 127
    assert RocketMQSettings(topic_prefix=value).topic_prefix == value
    assert validate_rocketmq_topic_prefix("topic.v1") == "topic.v1"


def test_direct_host_normalizers_reject_malicious_subclasses() -> None:
    value = _MaliciousString("localhost")
    with pytest.raises(ConfigurationError):
        normalize_memcached_host(value)
    with pytest.raises(ConfigurationError):
        normalize_rabbitmq_host(value)


def test_shared_broker_normalizers_reject_malicious_subclasses() -> None:
    value = _MaliciousString("localhost:8081")
    with pytest.raises(ConfigurationError):
        normalize_kafka_broker_endpoints(value, "bootstrap_servers")
    with pytest.raises(ConfigurationError):
        normalize_rocketmq_namesrv_endpoints(value)


@pytest.mark.parametrize(
    "endpoint",
    [
        "host:" + ("9" * 5000),
        "[2001:db8::1]:" + ("9" * 5000),
    ],
)
def test_numeric_port_fuzzing_is_rejected_without_conversion_errors(
    endpoint: str,
) -> None:
    with pytest.raises(ConfigurationError):
        parse_rabbitmq_node(endpoint, 5672)
    with pytest.raises(ConfigurationError):
        PulsarSettings(service_url=f"pulsar://{endpoint}")


def test_mongodb_uri_rejects_raw_controls_and_malformed_percent_escapes() -> None:
    marker = "mongodb-uri-marker"
    for uri in (
        f"mongodb://host:27017/{marker}\n",
        f"mongodb://host:27017/?appName={marker}%ZZ",
    ):
        with pytest.raises(ConfigurationError) as exc_info:
            validate_mongodb_uri(uri)
        assert marker not in str(exc_info.value)
        assert marker not in repr(exc_info.value)


@pytest.mark.parametrize("url", ["amqp://localhost:0", "amqp://localhost:00000"])
def test_rabbitmq_url_does_not_turn_port_zero_into_the_default(url: str) -> None:
    with pytest.raises((ConfigurationError, ValueError)):
        RabbitMQSettings(url=url, username="crawler", password="secret")


def test_rabbitmq_url_validates_authority_even_when_explicit_host_wins() -> None:
    marker = "rabbit-url-marker"
    with pytest.raises(ConfigurationError) as exc_info:
        RabbitMQSettings(
            url=f"amqps://{marker}%2eexample:5671",
            host="localhost",
            username="crawler",
            password="secret",
            ssl_enabled=True,
        )
    assert exc_info.value.setting_name == "url"
    assert marker not in str(exc_info.value)


def test_mapped_loopback_is_not_treated_as_a_local_trust_boundary() -> None:
    with pytest.raises(ConfigurationError) as memcached_error:
        MemcachedSettings(host="::ffff:127.0.0.1")
    assert memcached_error.value.setting_name == "allow_remote_plaintext"

    with pytest.raises(ConfigurationError) as rabbit_error:
        RabbitMQSettings(
            host="::ffff:127.0.0.0",
            username="crawler",
            password="secret",
        )
    assert rabbit_error.value.setting_name == "ssl_enabled"


def test_memcached_snapshot_rejects_mutated_authority_before_sdk_io(mocker) -> None:
    settings = MemcachedSettings()
    settings.host = "cache\n.internal"
    backend = MemcachedBackend(settings)
    client = mocker.patch("scrapy_extension.backends.memcached.MemcachedClient")

    with pytest.raises(ConfigurationError):
        backend.connect()

    client.assert_not_called()


def test_pulsar_snapshot_rejects_mutated_subscription_before_sdk_io(mocker) -> None:
    settings = PulsarSettings()
    settings.subscription_name = "s" * 256
    backend = PulsarBackend(settings)
    client = mocker.patch("scrapy_extension.backends.pulsar.pulsar.Client")

    with pytest.raises(ConfigurationError):
        backend.connect()

    client.assert_not_called()


def test_mongodb_snapshot_rejects_mutated_database_before_sdk_io(mocker) -> None:
    settings = MongoDBSettings()
    settings.database = "d" * 65
    backend = MongoDBBackend(settings)
    client = mocker.patch("scrapy_extension.backends.mongodb.MongoClient")

    with pytest.raises(ConfigurationError):
        backend.connect()

    client.assert_not_called()
