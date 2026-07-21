"""RocketMQ settings and configuration."""

from __future__ import annotations

import re
from enum import Enum

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError

# host:port — host is any non-colon run of chars (DNS name, IPv4, IPv6-bracketed
# forms are accepted by the client); port is digits only. Rejects bare host,
# bare port, and values with a scheme prefix.
_NAMESRV_PATTERN = re.compile(r"^[^:]+:\d+$")


class RocketMQMode(str, Enum):
    """RocketMQ deployment modes."""

    STANDALONE = "standalone"  # Single namesrv + broker
    CLUSTER = "cluster"  # Multi-broker HA
    CLOUD = "cloud"  # Alibaba Cloud RocketMQ


def _credential_value(
    value: SecretStr | str | None, setting_name: str
) -> str | None:
    """Extract a credential without retaining or echoing invalid values."""
    if value is None:
        return None
    if isinstance(value, SecretStr):
        text = value.get_secret_value()
    elif isinstance(value, str):
        text = value
    else:
        raise ConfigurationError(
            f"{setting_name} must be a string when explicitly configured.",
            setting_name=setting_name,
        )
    if not text.strip():
        raise ConfigurationError(
            f"{setting_name} must be non-empty when explicitly configured.",
            setting_name=setting_name,
        )
    return text


def validate_rocketmq_connection(
    mode: RocketMQMode,
    namesrv_address: str,
    access_key: SecretStr | str | None,
    secret_key: SecretStr | str | None,
    tls_enabled: bool,
) -> tuple[RocketMQMode, str, str | None, str | None, bool]:
    """Validate and return one coherent RocketMQ connection snapshot."""
    if mode not in (
        RocketMQMode.STANDALONE,
        RocketMQMode.CLUSTER,
        RocketMQMode.CLOUD,
    ):
        try:
            mode_text = str(mode)
        except (TypeError, ValueError):
            mode_text = getattr(mode, "value", repr(mode))
        raise ConfigurationError(
            f"Unsupported RocketMQ mode: {mode_text}",
            setting_name="mode",
            setting_value=mode,
        )

    if not isinstance(namesrv_address, str) or not _NAMESRV_PATTERN.match(
        namesrv_address.strip()
    ):
        raise ConfigurationError(
            "namesrv_address must match 'host:port' (e.g. 'localhost:8081').",
            setting_name="namesrv_address",
        )
    if not isinstance(tls_enabled, bool):
        raise ConfigurationError(
            "tls_enabled must be a boolean.",
            setting_name="tls_enabled",
        )

    key_text = _credential_value(access_key, "access_key")
    secret_text = _credential_value(secret_key, "secret_key")
    if key_text is None and secret_text is not None:
        raise ConfigurationError(
            "access_key is required when secret_key is configured.",
            setting_name="access_key",
        )
    if key_text is not None and secret_text is None:
        raise ConfigurationError(
            "secret_key is required when access_key is configured.",
            setting_name="secret_key",
        )
    if mode == RocketMQMode.CLOUD and key_text is None:
        raise ConfigurationError(
            "Cloud mode requires access_key and secret_key.",
            setting_name="access_key",
        )
    if key_text is not None and not tls_enabled:
        raise ConfigurationError(
            "Authenticated RocketMQ connections require tls_enabled=True.",
            setting_name="tls_enabled",
        )
    return mode, namesrv_address, key_text, secret_text, tls_enabled


class RocketMQSettings(BaseSettings):
    """Configuration for RocketMQ backend."""

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_ROCKETMQ_",
        case_sensitive=False,
        extra="forbid",
    )

    # === Mode Selection ===
    mode: RocketMQMode = Field(default=RocketMQMode.STANDALONE)

    # === Connection ===
    # gRPC PROXY endpoint (apache rocketmq-python-client 5.1.1) — the broker
    # must run with ``--enable-proxy``. Legacy NameServer port was 9876; the
    # gRPC client cannot speak the legacy namesrv protocol, so the default
    # follows the documented proxy port.
    namesrv_address: str = Field(default="localhost:8081")
    access_key: SecretStr | None = Field(default=None)
    secret_key: SecretStr | None = Field(default=None)
    tls_enabled: bool = Field(
        default=False,
        description="Use TLS for the RocketMQ 5.x gRPC proxy connection",
    )

    # === Consumer Group ===
    consumer_group: str = Field(default="scrapy-extension-consumer")
    producer_group: str = Field(default="scrapy-extension-producer")

    # === Queue/Priority Settings ===
    max_message_size: int = Field(default=1024 * 1024, ge=0)  # 1MB default
    send_timeout: int = Field(default=3000, ge=0)  # ms
    invisible_duration: int = Field(
        default=300,
        ge=10,
        le=12 * 60 * 60,
        description=(
            "Maximum message processing time in seconds before RocketMQ "
            "makes an unacked delivery available for retry"
        ),
    )

    # === Topic Settings ===
    topic_prefix: str = Field(default="scrapy-queue")
    set_topic_prefix: str = Field(default="scrapy-set")
    storage_topic_prefix: str = Field(default="scrapy-storage")

    @model_validator(mode="after")
    def _validate_connection(self) -> Self:
        """Validate endpoint, credential completeness, and TLS policy.

        The rocketmq-client-python ``NameServerAddress`` resolver accepts a
        bare ``host:port`` (no scheme). Typos like ``localhost:9876abc`` or
        scheme-prefixed ``http://namesrv:9876`` otherwise surface as an
        opaque resolution failure at producer/consumer start. Empty strings
        are rejected (no resolvable name server).

        Raises:
            ConfigurationError: if ``namesrv_address`` does not match
                ``host:port``.
        """
        validate_rocketmq_connection(
            self.mode,
            self.namesrv_address,
            self.access_key,
            self.secret_key,
            self.tls_enabled,
        )
        return self
