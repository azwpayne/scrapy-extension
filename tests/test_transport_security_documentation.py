"""Public documentation contracts for authenticated loopback plaintext."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import MongoDBSettings, RedisSettings

_DOCUMENTS = ("README.md", ".github/CHANGELOG.md", "docs/migration-guide.md")
_LOCAL_EXCEPTION = (
    "authenticated plaintext is preserved only for direct, literal-loopback "
    "standalone redis and mongodb development connections."
)
_REMOTE_REQUIREMENT = (
    "remote authenticated redis and mongodb connections require verified tls"
)


@pytest.mark.parametrize("relative_path", _DOCUMENTS)
def test_public_transport_docs_qualify_authenticated_plaintext(
    relative_path: str,
) -> None:
    text = (Path(__file__).resolve().parents[1] / relative_path).read_text(
        encoding="utf-8"
    )
    normalized = " ".join(text.lower().split())

    assert _LOCAL_EXCEPTION in normalized
    assert _REMOTE_REQUIREMENT in normalized


@pytest.mark.parametrize(
    "settings_type, local_kwargs",
    [
        (RedisSettings, {"host": "127.0.0.1", "password": "local-secret"}),
        (
            MongoDBSettings,
            {
                "uri": "mongodb://[::1]:27017",
                "username": "crawler",
                "password": "local-secret",
            },
        ),
    ],
)
def test_direct_loopback_authenticated_plaintext_contract_is_preserved(
    settings_type: type[Any], local_kwargs: dict[str, object]
) -> None:
    settings = settings_type(**local_kwargs)

    assert settings.allow_remote_plaintext is False
    transport_tls = (
        settings.ssl_enabled
        if isinstance(settings, RedisSettings)
        else settings.tls_enabled
    )
    assert transport_tls is False


@pytest.mark.parametrize(
    "settings_type, remote_kwargs, expected_setting",
    [
        (
            RedisSettings,
            {"host": "redis.internal", "password": "remote-secret"},
            "ssl_enabled",
        ),
        (
            MongoDBSettings,
            {
                "uri": "mongodb://mongo.internal:27017",
                "username": "crawler",
                "password": "remote-secret",
            },
            "tls_enabled",
        ),
    ],
)
def test_remote_authenticated_plaintext_still_requires_tls(
    settings_type: type[Any],
    remote_kwargs: dict[str, object],
    expected_setting: str,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        settings_type(**remote_kwargs)

    assert exc_info.value.setting_name == expected_setting
    assert "remote-secret" not in str(exc_info.value)
