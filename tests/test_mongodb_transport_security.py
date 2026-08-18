"""Transport-security contracts for MongoDB settings."""

from __future__ import annotations

import pytest

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import MongoDBSettings


def test_tls_rejects_both_cert_and_key_file() -> None:
    """R65: MongoDB TLS uses a single combined cert+key PEM (tlsCertificateKeyFile);
    setting both tls_cert_file and tls_key_file is ambiguous -- the backend
    silently drops the key (tls_cert_file wins), so mTLS auth fails at the broker
    with no local signal. Reject at construction with a precise error. (Opposite
    of Kafka's R64 cert+key PAIR model -- MongoDB treats them as alternatives.)"""
    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(
            tls_enabled=True,
            tls_cert_file="/tls/client.pem",
            tls_key_file="/tls/client.key",
        )

    assert exc_info.value.setting_name == "tls_key_file"
    assert "set tls_cert_file OR tls_key_file, not both" in str(exc_info.value)


def test_tls_accepts_cert_only() -> None:
    """R65 no-regression: cert-only (a combined PEM holding cert+key) is valid."""
    settings = MongoDBSettings(tls_enabled=True, tls_cert_file="/tls/client.pem")
    assert settings.tls_cert_file == "/tls/client.pem"
    assert settings.tls_key_file is None


def test_tls_accepts_key_only() -> None:
    """R65 no-regression: key-only (the alternative combined PEM) is valid."""
    settings = MongoDBSettings(tls_enabled=True, tls_key_file="/tls/client.key")
    assert settings.tls_key_file == "/tls/client.key"
    assert settings.tls_cert_file is None


@pytest.mark.parametrize(
    ("setting_name", "value"),
    [
        ("tls_ca_file", "/tls/ca.pem"),
        ("tls_cert_file", "/tls/client.pem"),
        ("tls_key_file", "/tls/client.key"),
    ],
)
def test_tls_material_without_sdk_tls_is_rejected(
    setting_name: str, value: str
) -> None:
    """Certificate intent must not be silently ignored on a plaintext scheme."""
    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(**{setting_name: value})

    assert exc_info.value.setting_name == setting_name
