"""No-I/O endpoint grammar regression tests for Kafka and RocketMQ."""

from __future__ import annotations

import builtins
import traceback

import pytest

from scrapy_extension.backends.kafka import KafkaBackend
from scrapy_extension.backends.rocketmq import RocketMQBackend
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import KafkaMode, KafkaSettings, RocketMQSettings
from scrapy_extension.settings._broker_endpoints import (
    _parse_kafka_endpoint,
    _parse_rocketmq_endpoint,
    normalize_kafka_broker_endpoints,
    normalize_rocketmq_namesrv_endpoints,
)


@pytest.fixture(autouse=True)
def _authorize_legacy_remote_broker_fixtures(monkeypatch) -> None:
    monkeypatch.setenv("SCRAPY_KAFKA_ALLOW_REMOTE_PLAINTEXT", "true")
    monkeypatch.setenv("SCRAPY_ROCKETMQ_ALLOW_REMOTE_PLAINTEXT", "true")


def test_kafka_normalizes_supported_endpoint_kinds() -> None:
    """Kafka accepts each SDK-supported host form and canonicalizes delimiters."""
    settings = KafkaSettings(
        bootstrap_servers=(
            " broker.example:9092, 192.0.2.10:9093, [2001:0db8::1]:9094, ::1 "
        ),
        cluster_brokers=[" cluster.example:9092 ", "2001:db8::2"],
    )

    assert settings.bootstrap_servers == (
        "broker.example:9092,192.0.2.10:9093,[2001:db8::1]:9094,[::1]"
    )
    assert settings.cluster_brokers == ["cluster.example:9092", "[2001:db8::2]"]


def test_kafka_raw_ipv6_is_unambiguously_treated_as_portless() -> None:
    """Valid raw IPv6 is preserved as a no-port address because intent is opaque."""
    settings = KafkaSettings(bootstrap_servers="2001:db8::1:9092")

    assert settings.bootstrap_servers == "[2001:db8::1:9092]"


def test_kafka_parser_canonicalizes_bracketed_portless_ipv6() -> None:
    """The member parser accepts an explicit bracketed IPv6 address without a port."""
    assert _parse_kafka_endpoint("[2001:0db8::1]") == "[2001:db8::1]"


@pytest.mark.parametrize(
    "endpoint",
    [
        "broker:\x009092",
        "invalid..broker:9092",
        "-invalid.broker:9092",
        "[]",
        "[not-an-ipv6]",
    ],
)
def test_kafka_member_parser_rejects_malformed_internal_grammar(endpoint: str) -> None:
    """Each parser-only malformed path stays rejected before SDK construction."""
    assert _parse_kafka_endpoint(endpoint) is None


def test_endpoint_normalizers_reject_non_string_values_before_parsing() -> None:
    """Direct callers cannot bypass the text-only endpoint grammar contract."""
    with pytest.raises(ConfigurationError) as kafka_error:
        normalize_kafka_broker_endpoints(["broker.example:9092"], "bootstrap_servers")
    with pytest.raises(ConfigurationError) as rocketmq_error:
        normalize_rocketmq_namesrv_endpoints({"endpoint": "broker.example:8081"})

    assert kafka_error.value.setting_name == "bootstrap_servers"
    assert rocketmq_error.value.setting_name == "namesrv_address"


def test_rocketmq_member_parser_rejects_control_characters() -> None:
    """The inner RocketMQ member parser also rejects controls before splitting."""
    assert _parse_rocketmq_endpoint("broker:\x008081") is None


def test_kafka_confluent_empty_override_keeps_bootstrap_fallback() -> None:
    """The optional Confluent value may stay empty without weakening validation."""
    settings = KafkaSettings(
        mode=KafkaMode.CONFLUENT,
        bootstrap_servers="pkc.example.confluent.cloud:9092",
        confluent_bootstrap_servers="   ",
        confluent_api_key="key",
        confluent_api_secret="secret",
    )

    assert settings.confluent_bootstrap_servers == ""
    assert KafkaBackend(settings)._bootstrap_servers() == (
        "pkc.example.confluent.cloud:9092"
    )


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "broker:0",
        "broker:65536",
        "broker:+1",
        "http://broker:9092",
        "user@broker:9092",
        "broker/path",
        "broker:9092?option=value",
        "broker:9092#fragment",
        "br\N{LATIN SMALL LETTER O WITH STROKE}ker:9092",
        "999.1.1.1:9092",
        "127.1:9092",
        "0x7f.0.0.1:9092",
        "2130706433:9092",
        "broker:9092,,other:9092",
        "broker:\x009092",
        "[::1]:0",
        "[fe80::1%eth0]:9092",
        "[::1]suffix",
    ],
)
def test_kafka_rejects_malformed_endpoint_lists_without_echo(endpoint: str) -> None:
    """Invalid Kafka syntax has one static diagnostic and no raw endpoint echo."""
    with pytest.raises(ConfigurationError) as exc_info:
        KafkaSettings(bootstrap_servers=endpoint)

    error = exc_info.value
    assert error.setting_name == "bootstrap_servers"
    if endpoint:
        assert endpoint not in str(error)
        assert endpoint not in repr(error)
        assert endpoint not in "".join(traceback.format_exception(error))


@pytest.mark.parametrize(
    "endpoint",
    [
        "broker.example:8081",
        " 192.0.2.10:8081 ; 192.0.2.11:8082 ",
    ],
)
def test_rocketmq_accepts_its_documented_proxy_endpoint_forms(endpoint: str) -> None:
    settings = RocketMQSettings(namesrv_address=endpoint)

    expected = (
        "broker.example:8081"
        if endpoint.startswith("broker")
        else "192.0.2.10:8081;192.0.2.11:8082"
    )
    assert settings.namesrv_address == expected


@pytest.mark.parametrize(
    "endpoint",
    [
        "one.example:8081;two.example:8082",
        "one.example:8081;192.0.2.10:8081",
        "[::1]:8081",
        "::1:8081",
        "http://broker:8081",
        "user@broker:8081",
        "broker:0",
        "192.0.2.10:65536",
        "999.1.1.1:8081",
        "0x7f.0.0.1:8081",
        "192.0.2.10:8081;;192.0.2.11:8081",
    ],
)
def test_rocketmq_rejects_unsupported_proxy_endpoint_forms(endpoint: str) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RocketMQSettings(namesrv_address=endpoint)

    error = exc_info.value
    assert error.setting_name == "namesrv_address"
    assert endpoint not in str(error)
    assert endpoint not in repr(error)
    assert endpoint not in "".join(traceback.format_exception(error))


def test_kafka_mutated_endpoint_fails_before_sdk_construction(mocker) -> None:
    """A post-settings mutation cannot defer bad bootstrap syntax to the SDK."""
    settings = KafkaSettings()
    backend = KafkaBackend(settings)
    marker = "https://runtime-kafka-marker.invalid:9092?token=secret"
    settings.bootstrap_servers = marker
    producer = mocker.patch("scrapy_extension.backends.kafka.KafkaProducer")
    admin = mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient")

    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()

    error = exc_info.value
    assert error.setting_name == "bootstrap_servers"
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    producer.assert_not_called()
    admin.assert_not_called()


def test_rocketmq_mutated_endpoint_fails_before_sdk_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The proxy parser runs before the optional RocketMQ dependency is imported."""
    backend = RocketMQBackend(RocketMQSettings())
    marker = "https://runtime-rocketmq-marker.invalid:8081?token=secret"
    backend.config.namesrv_address = marker
    imported_rocketmq = False
    original_import = builtins.__import__

    def guarded_import(name: str, *args: object, **kwargs: object) -> object:
        nonlocal imported_rocketmq
        if name == "rocketmq" or name.startswith("rocketmq."):
            imported_rocketmq = True
            raise AssertionError("RocketMQ SDK import must not run for invalid config")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()

    error = exc_info.value
    assert error.setting_name == "namesrv_address"
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    assert imported_rocketmq is False
