"""Transport-security contracts for Kafka settings."""

from __future__ import annotations

import pytest

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import KafkaSettings


@pytest.mark.parametrize(
    ("certfile", "keyfile", "missing_name"),
    [
        ("/tls/client.pem", None, "ssl_keyfile"),
        (None, "/tls/client.key", "ssl_certfile"),
    ],
)
def test_tls_client_certificate_must_be_a_pair(
    certfile: str | None, keyfile: str | None, missing_name: str
) -> None:
    """R64: mTLS client cert/key must be configured as a pair (parity with
    Redis/RabbitMQ). Setting exactly one of ssl_certfile/ssl_keyfile under TLS
    must fail at construction -- the key-without-cert case is otherwise silent
    (kafka-python skips load_cert_chain when ssl_certfile is None, so mTLS auth
    fails at the broker with no local signal).
    """
    with pytest.raises(ConfigurationError) as exc_info:
        KafkaSettings(
            security_protocol="SSL",
            ssl_certfile=certfile,
            ssl_keyfile=keyfile,
        )

    assert exc_info.value.setting_name == missing_name
    assert "requires both certificate and key files" in str(exc_info.value)


def test_tls_accepts_complete_client_cert_pair() -> None:
    """R64 no-regression: a complete mTLS cert+key pair under TLS constructs OK."""
    settings = KafkaSettings(
        security_protocol="SSL",
        ssl_certfile="/tls/client.pem",
        ssl_keyfile="/tls/client.key",
    )
    assert settings.ssl_certfile == "/tls/client.pem"
    assert settings.ssl_keyfile == "/tls/client.key"


def test_tls_accepts_no_client_cert_for_server_auth_only() -> None:
    """R64 no-regression: server-auth-only TLS (neither cert nor key) is valid."""
    settings = KafkaSettings(security_protocol="SSL")
    assert settings.ssl_certfile is None
    assert settings.ssl_keyfile is None
