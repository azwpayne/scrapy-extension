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

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._redacted import _SAFE_SETTINGS_CONFIGURATION_MESSAGES
from scrapy_extension.settings.base import Settings
from scrapy_extension.settings.kafka import KafkaSettings
from scrapy_extension.settings.mongodb import MongoDBSettings
from scrapy_extension.settings.redis import RedisMode, RedisSettings

# importlib.import_module returns the settings submodule itself (unlike
# ``__import__("...settings")`` which returns the top-level ``scrapy_extension`` package),
# so this resolves to ``.../scrapy_extension/settings/`` — NOT the package root. The R14-B
# invariant governs messages raised during settings construction (RedactedBaseSettings);
# backend runtime raises are a different boundary and intentionally out of scope.
_SETTINGS_MODULE = importlib.import_module("scrapy_extension.settings")
_SETTINGS_DIR = Path(_SETTINGS_MODULE.__file__).resolve().parent


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
