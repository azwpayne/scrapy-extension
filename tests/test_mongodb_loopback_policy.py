"""Exact MongoDB loopback transport-policy regression tests."""

from __future__ import annotations

import traceback
from typing import Any

import pytest

from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import MongoDBMode, MongoDBSettings

_REJECTED_DIRECT_URIS = (
    ("mongodb://127.0.0.1.:27017", "tls_enabled"),
    ("mongodb://localhost..:27017", "uri"),
    ("mongodb://[::ffff:127.0.0.1]:27017", "tls_enabled"),
    ("mongodb://[::]:27017", "tls_enabled"),
    ("mongodb://[127.0.0.1]:27017", "uri"),
    ("mongodb://[::1%25lo0]:27017", "uri"),
    ("mongodb://%2Ftmp%2Fmongodb-27017.sock", "uri"),
)
_VALID_DIRECT_LOOPBACK_URIS = (
    "mongodb://localhost:27017",
    "mongodb://localhost.:27017",
    "mongodb://127.0.0.1:27017",
    "mongodb://[::1]:27017",
    "mongodb://[0:0:0:0:0:0:0:1]:27017",
)
_SEED_FIELDS = (
    (
        MongoDBMode.REPLICA_SET,
        "replica_set_members",
        {"replica_set_name": "rs0"},
    ),
    (MongoDBMode.SHARDED_CLUSTER, "mongos_routers", {}),
)
_REJECTED_SEEDS = (
    ("127.0.0.1.:27017", "tls_enabled"),
    ("localhost..:27017", None),
    ("[::ffff:127.0.0.1]:27017", "tls_enabled"),
    ("[::]:27017", "tls_enabled"),
    ("[127.0.0.1]:27017", None),
    ("[::1%lo0]:27017", None),
)
_VALID_LOOPBACK_SEEDS = (
    "localhost:27017",
    "localhost.:27017",
    "127.0.0.1:27017",
    "[::1]:27017",
    "[0:0:0:0:0:0:0:1]:27017",
)


def _assert_static_error_graph(error: ConfigurationError, marker: str) -> None:
    """Assert rejected endpoint text is absent from the complete error graph."""
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert getattr(error, "setting_value", None) is None
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(("uri", "setting_name"), _REJECTED_DIRECT_URIS)
def test_constructor_rejects_non_exact_direct_loopback_before_mongo_client(
    mocker: Any, uri: str, setting_name: str
) -> None:
    """Lookalike, malformed, scoped, and Unix authorities fail closed."""
    client = mocker.patch("scrapy_extension.backends.mongodb.MongoClient")

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(uri=uri, username="crawler", password="constructor-secret")

    assert exc_info.value.setting_name == setting_name
    client.assert_not_called()
    _assert_static_error_graph(exc_info.value, uri)


@pytest.mark.parametrize(("uri", "setting_name"), _REJECTED_DIRECT_URIS)
def test_mutated_non_exact_direct_loopback_rejects_before_mongo_client(
    mocker: Any, uri: str, setting_name: str
) -> None:
    """Post-construction URI mutation receives the same exact classification."""
    settings = MongoDBSettings()
    settings.uri = uri
    settings.username = "crawler"
    settings.password = "mutation-secret"
    client = mocker.patch("scrapy_extension.backends.mongodb.MongoClient")

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBBackend(settings).connect()

    assert exc_info.value.setting_name == setting_name
    client.assert_not_called()
    _assert_static_error_graph(exc_info.value, uri)


@pytest.mark.parametrize("uri", _VALID_DIRECT_LOOPBACK_URIS)
def test_exact_direct_loopback_forms_keep_local_development_contract(
    mocker: Any, uri: str
) -> None:
    """Exact localhost and strict IPv4/IPv6 loopbacks remain available."""
    settings = MongoDBSettings(uri=uri, username="crawler", password="local-secret")
    client = mocker.patch("scrapy_extension.backends.mongodb.MongoClient")

    MongoDBBackend(settings).connect()

    client.assert_called_once()
    assert client.call_args.kwargs["directConnection"] is True


@pytest.mark.parametrize(("mode", "field", "mode_kwargs"), _SEED_FIELDS)
@pytest.mark.parametrize(("seed", "security_error"), _REJECTED_SEEDS)
def test_constructor_rejects_non_exact_seed_loopback(
    mode: MongoDBMode,
    field: str,
    mode_kwargs: dict[str, object],
    seed: str,
    security_error: str | None,
) -> None:
    """Replica and mongos seed lists share exact endpoint classification."""
    expected_name = security_error or field

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(
            mode=mode,
            username="crawler" if security_error else None,
            password="seed-secret" if security_error else None,
            **mode_kwargs,
            **{field: [seed]},
        )

    assert exc_info.value.setting_name == expected_name
    _assert_static_error_graph(exc_info.value, seed)


@pytest.mark.parametrize(("mode", "field", "mode_kwargs"), _SEED_FIELDS)
@pytest.mark.parametrize(("seed", "security_error"), _REJECTED_SEEDS)
def test_mutated_non_exact_seed_loopback_rejects_before_mongo_client(
    mocker: Any,
    mode: MongoDBMode,
    field: str,
    mode_kwargs: dict[str, object],
    seed: str,
    security_error: str | None,
) -> None:
    """Mutated replica and mongos seeds fail before driver construction."""
    settings = MongoDBSettings(mode=mode, **mode_kwargs)
    setattr(settings, field, [seed])
    if security_error:
        settings.username = "crawler"
        settings.password = "seed-mutation-secret"
    client = mocker.patch("scrapy_extension.backends.mongodb.MongoClient")

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBBackend(settings).connect()

    assert exc_info.value.setting_name == (security_error or field)
    client.assert_not_called()
    _assert_static_error_graph(exc_info.value, seed)


@pytest.mark.parametrize(("mode", "field", "mode_kwargs"), _SEED_FIELDS)
@pytest.mark.parametrize("seed", _VALID_LOOPBACK_SEEDS)
def test_exact_loopback_seeds_remain_local(
    mode: MongoDBMode,
    field: str,
    mode_kwargs: dict[str, object],
    seed: str,
) -> None:
    """Every generated-URI seed path retains strict local forms."""
    settings = MongoDBSettings(mode=mode, **mode_kwargs, **{field: [seed]})

    assert len(getattr(settings, field)) == 1
    assert settings.allow_remote_plaintext is False


def test_srv_uri_keeps_implicit_tls_contract(mocker: Any) -> None:
    """SRV discovery remains non-local while its driver-default TLS stays valid."""
    uri = "mongodb+srv://cluster.example.test"
    settings = MongoDBSettings(uri=uri, username="crawler", password="srv-secret")
    client = mocker.patch("scrapy_extension.backends.mongodb.MongoClient")

    MongoDBBackend(settings).connect()

    client.assert_called_once()
    assert client.call_args.args == (uri,)
    assert "directConnection" not in client.call_args.kwargs
