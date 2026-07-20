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
    def _validate_namesrv_address_format(self) -> Self:
        """SV4: ``namesrv_address`` must match ``host:port``.

        The rocketmq-client-python ``NameServerAddress`` resolver accepts a
        bare ``host:port`` (no scheme). Typos like ``localhost:9876abc`` or
        scheme-prefixed ``http://namesrv:9876`` otherwise surface as an
        opaque resolution failure at producer/consumer start. Empty strings
        are rejected (no resolvable name server).

        Raises:
            ConfigurationError: if ``namesrv_address`` does not match
                ``host:port``.
        """
        addr = self.namesrv_address.strip()
        if not _NAMESRV_PATTERN.match(addr):
            raise ConfigurationError(
                (
                    "namesrv_address must match 'host:port' "
                    "(e.g. 'localhost:8081'). "
                    f"Got namesrv_address={self.namesrv_address!r}."
                ),
                setting_name="namesrv_address",
                setting_value=self.namesrv_address,
            )
        return self
