"""R14-B contract: every static settings ``ConfigurationError`` message is safe-listed.

The ``RedactedBaseSettings`` boundary (``settings/_redacted.py``) preserves a
validator-raised ``ConfigurationError`` message ONLY when the exact static string is a
member of ``_SAFE_SETTINGS_CONFIGURATION_MESSAGES``; otherwise it substitutes the generic
``"Settings contain an invalid configuration value."``. This contract test AST-scans the
settings package so a newly-added validator message that forgets the safe-list fails here
instead of silently regressing to the generic message at runtime.

F-string messages (interpolating field/setting names) cannot be exact-safe-listed and are
out of scope; only statically resolvable first-arg literals are asserted.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest
from pydantic import model_validator
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._redacted import (
    _SAFE_SETTINGS_CONFIGURATION_MESSAGES,
    RedactedBaseSettings,
    _settings_message_is_safe,
)
from scrapy_extension.settings.base import Settings
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings
from scrapy_extension.settings.kafka import KafkaSettings
from scrapy_extension.settings.mongodb import MongoDBSettings
from scrapy_extension.settings.rabbitmq import RabbitMQSettings
from scrapy_extension.settings.redis import RedisMode, RedisSettings

# importlib.import_module returns the settings submodule itself (unlike
# ``__import__("...settings")`` which returns the top-level ``scrapy_extension`` package),
# so this resolves to ``.../scrapy_extension/settings/`` — NOT the package root. The R14-B
# invariant governs messages raised during settings construction (RedactedBaseSettings);
# backend runtime raises are a different boundary and intentionally out of scope.
_SETTINGS_MODULE = importlib.import_module("scrapy_extension.settings")
_SETTINGS_DIR = Path(_SETTINGS_MODULE.__file__).resolve().parent


class _UnknownInterpolatedSettings(RedactedBaseSettings):
    """Untrusted model whose error includes caller-controlled text."""

    marker: str

    @model_validator(mode="after")
    def _raise_interpolated_error(self) -> Self:
        raise ConfigurationError(
            f"unknown settings diagnostic: {self.marker}", setting_name="marker"
        )


def _is_configuration_error_call(node: ast.Call) -> bool:
    func = node.func
    if isinstance(func, ast.Name) and func.id == "ConfigurationError":
        return True
    return isinstance(func, ast.Attribute) and func.attr == "ConfigurationError"


def _resolve_static(arg: ast.AST) -> str | None:
    """Return the static str for a pure literal (incl implicit/explicit concatenation)."""
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    if isinstance(arg, ast.BinOp) and isinstance(arg.op, ast.Add):
        left = _resolve_static(arg.left)
        right = _resolve_static(arg.right)
        if left is not None and right is not None:
            return left + right
    return None


def _static_configuration_error_messages() -> set[str]:
    """Every statically resolvable ``ConfigurationError`` message raised in settings/."""
    messages: set[str] = set()
    for source in sorted(_SETTINGS_DIR.rglob("*.py")):
        # _redacted.py raises are the sanitizer internals (the generic fallbacks), not
        # validator messages — exclude them from the contract.
        if source.name in {"__init__.py", "_redacted.py"}:
            continue
        tree = ast.parse(source.read_text(), filename=str(source))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Raise)
                and node.exc is not None
                and isinstance(node.exc, ast.Call)
                and _is_configuration_error_call(node.exc)
                and node.exc.args
            ):
                static = _resolve_static(node.exc.args[0])
                if static is not None:
                    messages.add(static)
    return messages


def test_every_static_settings_configuration_message_is_safe_listed() -> None:
    """R14-B: each static settings message must survive the redaction boundary."""
    messages = _static_configuration_error_messages()
    missing = sorted(
        m for m in messages if m not in _SAFE_SETTINGS_CONFIGURATION_MESSAGES
    )
    assert not missing, (
        "Static ConfigurationError messages missing from "
        "_SAFE_SETTINGS_CONFIGURATION_MESSAGES (they get sanitized to generic):\n"
        + "\n".join(repr(m) for m in missing)
    )


# Functional survival tests: the precise message must reach the caller, not the generic.
# (ElasticSearch message-survival is covered by R74/R75 in test_elasticsearch_backend.py.)


def test_base_backend_type_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        Settings(backend_type=123)  # type: ignore[arg-type]
    assert (
        exc_info.value.args[0]
        == "Selected backend type is not a registered backend type."
    )


def test_redis_cluster_db_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RedisSettings(mode=RedisMode.CLUSTER, db=1)
    assert exc_info.value.args[0] == (
        "Redis Cluster supports only database 0; use namespace for isolation."
    )


def test_mongodb_blank_username_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(username="   ")
    assert exc_info.value.args[0] == "MongoDB 'username' must be non-empty."


def test_kafka_boolean_acks_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        KafkaSettings(acks=True)  # type: ignore[arg-type]
    assert exc_info.value.args[0] == "Kafka acks must be 1 or 'all', not a boolean."


# R141-F15: validator messages that interpolate only validator-controlled
# constants (field/mechanism names) or a non-negative integer index must also
# survive the boundary instead of collapsing to the generic placeholder.


def test_redis_address_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RedisSettings(host="bad host")
    assert exc_info.value.args[0] == "Redis setting 'host' contains an invalid address."
    with pytest.raises(ConfigurationError) as exc_info:
        # A text port is intercepted earlier by the bundled scalar grammar gate
        # ("Bundled scalar setting has an invalid value."), so exercise the
        # address validator with an out-of-range integer instead.
        RedisSettings(port=0)
    assert exc_info.value.args[0] == "Redis setting 'port' contains an invalid address."


def test_redis_indexed_address_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RedisSettings(sentinels=["bad"])
    assert exc_info.value.args[0] == (
        "Redis setting 'sentinels' contains an invalid address at index 0."
    )


def test_redis_endpoint_list_type_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RedisSettings(sentinels="not json")  # type: ignore[arg-type]
    assert exc_info.value.args[0] == (
        "Redis setting 'sentinels' must be a JSON endpoint list."
    )


def test_redis_blank_credential_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RedisSettings(username="   ")
    assert exc_info.value.args[0] == "Redis 'username' must be non-empty."


def test_kafka_sasl_mechanism_credential_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        KafkaSettings(
            security_protocol="SASL_SSL",  # type: ignore[arg-type]
            sasl_mechanism="SCRAM-SHA-256",
            sasl_password="secret",
        )
    assert exc_info.value.args[0] == (
        "SCRAM-SHA-256 authentication requires sasl_username."
    )


def test_elasticsearch_basic_auth_message_survives_sanitization() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        ElasticSearchSettings(username="crawler")
    assert exc_info.value.args[0] == (
        "basic authentication requires both username and password; set 'password'."
    )


def test_endpoint_message_survives_without_leaking_invalid_marker() -> None:
    marker = "settings-marker-must-not-leak"
    with pytest.raises(ConfigurationError) as exc_info:
        RabbitMQSettings(
            host=f"{marker}.example host",
            username="crawler",
            password="secret",
            ssl_enabled=True,
        )

    assert exc_info.value.args[0] == (
        "RabbitMQ host must be a valid DNS name, IPv4 address, or IPv6 address."
    )
    assert marker not in str(exc_info.value)
    assert marker not in repr(exc_info.value)


def test_unknown_interpolated_message_remains_generic() -> None:
    """Only exact static messages may cross the settings redaction boundary."""
    marker = "settings-marker-must-remain-hidden"
    with pytest.raises(ConfigurationError) as exc_info:
        _UnknownInterpolatedSettings(marker=marker)

    assert exc_info.value.args[0] == "Settings contain an invalid configuration value."
    assert marker not in str(exc_info.value)
    assert marker not in repr(exc_info.value)


def test_indexed_address_pattern_matching_is_fail_closed() -> None:
    """R141-F15: the indexed-address allowance admits only its exact template.

    The pattern exists because ``at index {n}`` cannot be enumerated as exact
    strings; everything except the known field names plus a bare non-negative
    decimal index must still be rejected (and the input never echoed).
    """
    assert _settings_message_is_safe(
        "Redis setting 'cluster_startup_nodes' contains an invalid address at index 12."
    )
    # Trailing caller-controlled text after the index is not the template.
    assert not _settings_message_is_safe(
        "Redis setting 'sentinels' contains an invalid address at index 0 (evil)."
    )
    # Unknown field names are not part of the validator's constant set.
    assert not _settings_message_is_safe(
        "Redis setting 'unknown' contains an invalid address at index 0."
    )
    # Non-list fields never raise the indexed form.
    assert not _settings_message_is_safe(
        "Redis setting 'host' contains an invalid address at index 0."
    )
    # Negative or non-decimal indexes are not produced by enumerate().
    assert not _settings_message_is_safe(
        "Redis setting 'sentinels' contains an invalid address at index -1."
    )
    assert not _settings_message_is_safe(
        "Redis setting 'sentinels' contains an invalid address at index 0x0."
    )
    assert not _settings_message_is_safe(None)
