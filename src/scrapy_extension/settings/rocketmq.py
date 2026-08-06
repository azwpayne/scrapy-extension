"""RocketMQ settings and configuration."""

from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._broker_endpoints import (
    normalize_rocketmq_namesrv_endpoints,
)
from scrapy_extension.settings._redacted import RedactedBaseSettings
from scrapy_extension.settings._transport_security import (
    is_loopback_host,
    validate_allow_remote_plaintext,
    warn_remote_unauthenticated_plaintext,
)


class RocketMQMode(str, Enum):
    """RocketMQ deployment modes."""

    STANDALONE = "standalone"  # Single namesrv + broker
    CLUSTER = "cluster"  # Multi-broker HA
    CLOUD = "cloud"  # Alibaba Cloud RocketMQ


def _credential_value(value: SecretStr | str | None, setting_name: str) -> str | None:
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


def _rocketmq_namesrv_endpoints_are_loopback(endpoints: str) -> bool:
    """Return whether every normalized RocketMQ proxy endpoint is local."""
    hosts = [endpoint.rsplit(":", 1)[0] for endpoint in endpoints.split(";")]
    return bool(hosts) and all(is_loopback_host(host) for host in hosts)


def validate_rocketmq_connection(
    mode: RocketMQMode,
    namesrv_address: str,
    access_key: SecretStr | str | None,
    secret_key: SecretStr | str | None,
    tls_enabled: bool,
    allow_remote_plaintext: object = False,
) -> tuple[RocketMQMode, str, str | None, str | None, bool]:
    """Validate and return one coherent RocketMQ connection snapshot."""
    if mode not in (
        RocketMQMode.STANDALONE,
        RocketMQMode.CLUSTER,
        RocketMQMode.CLOUD,
    ):
        raise ConfigurationError(
            "Unsupported RocketMQ mode.",
            setting_name="mode",
        )

    namesrv_address = normalize_rocketmq_namesrv_endpoints(namesrv_address)
    if not isinstance(tls_enabled, bool):
        raise ConfigurationError(
            "tls_enabled must be a boolean.",
            setting_name="tls_enabled",
        )
    normalized_allow_remote_plaintext = validate_allow_remote_plaintext(
        allow_remote_plaintext
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
    if (
        key_text is None
        and not tls_enabled
        and not _rocketmq_namesrv_endpoints_are_loopback(namesrv_address)
    ):
        warn_remote_unauthenticated_plaintext(
            "RocketMQ", normalized_allow_remote_plaintext
        )
    return mode, namesrv_address, key_text, secret_text, tls_enabled


class RocketMQSettings(RedactedBaseSettings):
    """Configuration for RocketMQ backend."""

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_ROCKETMQ_",
        case_sensitive=False,
        extra="forbid",
        hide_input_in_errors=True,
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
    allow_remote_plaintext: bool = Field(
        default=False,
        description=(
            "Acknowledge an unauthenticated plaintext connection to a non-loopback "
            "RocketMQ proxy on a trusted private network"
        ),
    )

    # === Consumer Group ===
    # The apache rocketmq-python-client 5.1.1 gRPC Producer is group-less
    # (group is consumer-side only in this client — Producer(config, topics,
    # tls_enable) takes no group arg), so there is no producer_group setting.
    # Do NOT re-add one without a wire at the Producer() construction in
    # backends/rocketmq.py (R25-G removed a dead, unconsumed producer_group).
    # R27-RMQ-2: min_length=1 rejects empty; the field_validator below rejects
    # whitespace — both surface as opaque SimpleConsumer errors at connect.
    consumer_group: str = Field(default="scrapy-extension-consumer", min_length=1)

    # === Queue/Priority Settings ===
    # 1MB default
    # R27-RMQ-1: gt=0 (not ge=0) — zero would make every non-empty push fail.
    max_message_size: int = Field(default=1024 * 1024, gt=0)
    # ms. Ceiling 300_000 (5 min) mirrors the cap discipline on
    # ``invisible_duration`` and the R21 timeout caps (circuit_breaker/backoff/
    # throttle): without it a stray-zero typo (e.g. a microseconds copy-paste)
    # flows through the ``request_timeout = send_timeout // 1000`` conversion
    # into an unbounded gRPC per-RPC deadline that wedges the producer/consumer
    # for hours on a stalled broker. Defense-in-depth cap in ``rocketmq.py``.
    send_timeout: int = Field(default=3000, ge=0, le=300_000)  # ms
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
    # RocketMQ is queue-only by design (RocketMQSetBackend / RocketMQStorageBackend
    # raise ConfigurationError; resolve_backend_config excludes RocketMQ from
    # SET_CAPABLE_BACKENDS / STORAGE_CAPABLE_BACKENDS), so there are no
    # set_topic_prefix / storage_topic_prefix settings — do NOT re-add them
    # (R25-H removed vestigial, unconsumed dead config).
    topic_prefix: str = Field(default="scrapy-queue")

    @field_validator("consumer_group", mode="after")
    @classmethod
    def _reject_blank_consumer_group(cls, value: str) -> str:
        """R27-RMQ-2: reject whitespace ``consumer_group`` (``min_length=1`` on
        the field admits ``"   "``) — opaque SimpleConsumer error at connect.
        """
        if not value.strip():
            raise ConfigurationError(
                "RocketMQ 'consumer_group' must be non-empty.",
                setting_name="consumer_group",
                setting_value=value,
            )
        return value

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
        _, self.namesrv_address, _, _, _ = validate_rocketmq_connection(
            self.mode,
            self.namesrv_address,
            self.access_key,
            self.secret_key,
            self.tls_enabled,
            self.allow_remote_plaintext,
        )
        return self
