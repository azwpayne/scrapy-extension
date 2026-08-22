# @author  : azwpayne(https://github.com/azwpayne)
# @name    : rabbitmq.py
# @time    : 2026/3/18 20:40 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :
from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from enum import Enum
from typing import Any, Literal
from urllib.parse import unquote

from pydantic import AmqpDsn, Field, SecretStr, field_validator, model_validator
from pydantic_settings import SettingsConfigDict

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._endpoint_validation import (
    has_invalid_percent_escape,
    parse_endpoint_host,
    parse_host_port_authority,
)
from scrapy_extension.settings._redacted import RedactedBaseSettings
from scrapy_extension.settings._transport_security import is_loopback_host


class RabbitMQMode(str, Enum):
    """RabbitMQ deployment modes.

    Attributes:
        STANDALONE: Single RabbitMQ node (default).
        CLUSTER: Multi-node RabbitMQ cluster.
        MIRRORED_QUEUES: Cluster with mirrored queues for HA.
    """

    STANDALONE = "standalone"
    CLUSTER = "cluster"
    MIRRORED_QUEUES = "mirrored_queues"


def _secret_text(value: object) -> str | None:
    """Extract a secret for validation without retaining it in an exception."""
    if value is None:
        return None
    if type(value) is SecretStr:
        return value.get_secret_value()
    if type(value) is str:
        return value
    return None


def normalize_rabbitmq_host(host: object) -> str:
    """Normalize a strict RabbitMQ host for Pika and loopback classification."""
    normalized = parse_endpoint_host(host)
    if normalized is None:
        raise ConfigurationError(
            "RabbitMQ host must be a valid DNS name, IPv4 address, or IPv6 address.",
            setting_name="host",
        )
    return normalized


def validate_rabbitmq_virtual_host(value: object) -> str:
    """Validate the vhost identifier before it reaches Pika."""
    if (
        type(value) is not str
        or not value
        or any(
            character.isspace() or unicodedata.category(character).startswith("C")
            for character in value
        )
    ):
        raise ConfigurationError(
            "RabbitMQ virtual_host must be a non-empty value without whitespace or controls.",
            setting_name="virtual_host",
        )
    return value


def _decode_rabbitmq_virtual_host(path: str | None) -> str:
    """Decode a URL vhost while rejecting malformed escapes and controls."""
    raw_path = path or ""
    if has_invalid_percent_escape(raw_path):
        raise ConfigurationError(
            "RabbitMQ URL virtual host contains an invalid percent escape.",
            setting_name="url",
        )
    decoded = unquote(raw_path[1:] if raw_path.startswith("/") else raw_path)
    return validate_rabbitmq_virtual_host(decoded or "/")


def parse_rabbitmq_node(node: str, default_port: int) -> tuple[str, int]:
    """Parse one cluster node with strict host and port grammar."""
    if type(default_port) is not int or not 1 <= default_port <= 65535:
        raise ConfigurationError(
            "RabbitMQ cluster node port must be between 1 and 65535.",
            setting_name="cluster_nodes",
        )
    parsed = parse_host_port_authority(node, default_port=default_port)
    if parsed is None:
        raise ConfigurationError(
            "RabbitMQ cluster nodes must be valid host or host:port values.",
            setting_name="cluster_nodes",
        )
    host, port = parsed
    assert port is not None
    return host, port


def _is_loopback_host(host: str) -> bool:
    normalized = normalize_rabbitmq_host(host)
    return is_loopback_host(normalized)


def validate_rabbitmq_connection(
    *,
    host: str,
    port: int,
    cluster_nodes: tuple[str, ...],
    username: str | None,
    password: str | None,
    ssl_enabled: bool,
    ssl_cafile: str | None,
    ssl_certfile: str | None,
    ssl_keyfile: str | None,
    ssl_verify_mode: str,
) -> tuple[str, tuple[tuple[str, int], ...]]:
    """Validate a complete connection snapshot and return parsed endpoints."""
    normalized_host = normalize_rabbitmq_host(host)
    if type(port) is not int or not 1 <= port <= 65535:
        raise ConfigurationError(
            "RabbitMQ port must be between 1 and 65535.",
            setting_name="port",
        )
    if type(username) is not str or not username.strip():
        raise ConfigurationError(
            "RabbitMQ username must be explicitly set and cannot be blank.",
            setting_name="username",
        )
    if type(password) is not str or not password.strip():
        raise ConfigurationError(
            "RabbitMQ password must be explicitly set and cannot be blank.",
            setting_name="password",
        )
    if type(ssl_enabled) is not bool:
        raise ConfigurationError(
            "RabbitMQ ssl_enabled must be a boolean.",
            setting_name="ssl_enabled",
        )

    if type(cluster_nodes) not in (list, tuple):
        raise ConfigurationError(
            "RabbitMQ cluster nodes must be a list or tuple of host:port values.",
            setting_name="cluster_nodes",
        )
    tls_paths = {
        "ssl_cafile": ssl_cafile,
        "ssl_certfile": ssl_certfile,
        "ssl_keyfile": ssl_keyfile,
    }
    for setting_name, value in tls_paths.items():
        if value is not None and (type(value) is not str or not value.strip()):
            raise ConfigurationError(
                f"RabbitMQ TLS setting '{setting_name}' cannot be blank.",
                setting_name=setting_name,
            )
    if (ssl_certfile is None) != (ssl_keyfile is None):
        missing_name = "ssl_keyfile" if ssl_certfile is not None else "ssl_certfile"
        raise ConfigurationError(
            "RabbitMQ TLS client authentication requires both certificate and key files.",
            setting_name=missing_name,
        )
    if type(ssl_verify_mode) is not str or (
        ssl_enabled and ssl_verify_mode != "CERT_REQUIRED"
    ):
        raise ConfigurationError(
            "RabbitMQ TLS requires CERT_REQUIRED certificate and hostname verification.",
            setting_name="ssl_verify_mode",
        )

    parsed_nodes = tuple(parse_rabbitmq_node(node, port) for node in cluster_nodes)
    endpoint_hosts = (normalized_host, *(node_host for node_host, _ in parsed_nodes))
    has_remote_endpoint = any(not _is_loopback_host(item) for item in endpoint_hosts)
    if has_remote_endpoint and not ssl_enabled:
        raise ConfigurationError(
            "RabbitMQ connections outside loopback require verified TLS.",
            setting_name="ssl_enabled",
        )
    if has_remote_endpoint and username == "guest":
        raise ConfigurationError(
            "RabbitMQ's guest user is restricted to loopback endpoints.",
            setting_name="username",
        )
    return normalized_host, parsed_nodes


class RabbitMQSettings(RedactedBaseSettings):
    """RabbitMQ-specific settings for all deployment modes.

    These settings configure the RabbitMQ connection and can be set
    via environment variables with the SCRAPY_RABBITMQ_ prefix.

    Supports three deployment modes:
    - standalone: Single RabbitMQ node (default)
    - cluster: Multi-node RabbitMQ cluster
    - mirrored_queues: Cluster with mirrored queues for HA
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_RABBITMQ_",
        case_sensitive=False,
        extra="forbid",
        hide_input_in_errors=True,
    )

    # === Mode Selection ===
    mode: RabbitMQMode = Field(
        default=RabbitMQMode.STANDALONE,
        description="RabbitMQ deployment mode (standalone, cluster, mirrored_queues)",
    )

    # === Connection Settings ===
    url: SecretStr | None = Field(
        default=None,
        description=(
            "Credential-free AMQP connection URL shortcut. Values from explicit "
            "host/port fields take precedence over URL components."
        ),
    )
    host: str = Field(
        default="localhost",
        min_length=1,
        description="RabbitMQ server hostname",
    )
    port: int = Field(
        default=5672,
        ge=1,
        le=65535,
        description="RabbitMQ server port",
    )
    username: str = Field(
        description=(
            "RabbitMQ username (REQUIRED). No default is provided to prevent "
            "silent fallback to the guest account; set via SCRAPY_RABBITMQ_USERNAME."
        ),
    )
    password: SecretStr = Field(
        description=(
            "RabbitMQ password (REQUIRED). No default is provided to prevent "
            "silent fallback to the guest account; set via SCRAPY_RABBITMQ_PASSWORD."
        ),
    )
    virtual_host: str = Field(
        default="/",
        description="RabbitMQ virtual host",
    )

    # === Cluster Settings ===
    cluster_nodes: list[str] = Field(
        default_factory=list,
        description="List of cluster node host:port (for cluster/mirrored_queues mode)",
    )
    # There is no cluster_node_type setting — do NOT re-add it (R141-F16
    # removed vestigial, unconsumed dead config: the disc/ram node type only
    # steers broker-side clustering via rabbitmqctl and was never sent to the
    # AMQP client, mirroring the R25-H RocketMQ dead-config removal).

    # === Mirrored Queue Settings (HA) ===
    ha_mode: str | None = Field(
        default=None,
        description="HA mode for mirrored queues (all, exactly, nodes)",
    )
    ha_params: str | None = Field(
        default=None,
        description="HA parameters (number of replicas or node names)",
    )
    ha_sync_mode: str = Field(
        default="automatic",
        description="HA sync mode (automatic or manual)",
    )

    # === SSL/TLS Settings ===
    ssl_enabled: bool = Field(
        default=False,
        description="Enable SSL/TLS connection",
    )
    ssl_cafile: str | None = Field(
        default=None,
        description="Path to CA certificate file",
    )
    ssl_certfile: str | None = Field(
        default=None,
        description="Path to client certificate file",
    )
    ssl_keyfile: str | None = Field(
        default=None,
        description="Path to client private key file",
    )
    ssl_verify_mode: Literal["CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED"] = Field(
        default="CERT_REQUIRED",
        description="SSL verification mode (CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED)",
    )

    # === Connection Settings ===
    max_priority: int = Field(
        default=255,
        ge=1,
        le=255,
        description="Maximum priority level (1-255)",
    )
    heartbeat: int = Field(
        default=600,
        ge=0,
        le=65535,
        description="Heartbeat interval in seconds (AMQP Tune-Ok encodes as unsigned short, 0-65535)",
    )
    blocked_connection_timeout: int = Field(
        default=300,
        ge=0,
        description="Blocked connection timeout in seconds",
    )
    connection_attempts: int = Field(
        default=1,
        ge=1,
        description="Connection retry attempts",
    )
    retry_delay: int = Field(
        default=1,
        ge=0,
        description="Delay between connection retries in seconds",
    )

    # === Queue Settings ===
    durable: bool = Field(
        default=True,
        description="Create durable queues",
    )
    auto_delete: bool = Field(
        default=False,
        description="Auto-delete queues when last consumer unsubscribes",
    )
    exclusive: bool = Field(
        default=False,
        description="Create exclusive queues",
    )
    delivery_mode: int = Field(
        default=2,
        ge=1,
        le=2,
        description="Message delivery mode (1=transient, 2=persistent)",
    )

    # === Prefetch Settings ===
    prefetch_count: int = Field(
        default=0,
        ge=0,
        description=(
            "QoS prefetch count (0 = unlimited). RabbitMQ applies prefetch "
            "to push (basic_consume) deliveries only; this backend consumes "
            "via basic_get, which prefetch does not bound."
        ),
    )
    prefetch_size: int = Field(
        default=0,
        ge=0,
        description=(
            "QoS prefetch size in bytes; must be 0 (rejected otherwise). "
            "RabbitMQ does not implement byte-based prefetch — a nonzero "
            "value closes the channel at connect."
        ),
    )

    @field_validator("virtual_host", mode="before")
    @classmethod
    def _validate_virtual_host(cls, value: object) -> str:
        """Reject vhost subclasses and control/whitespace identifiers."""
        return validate_rabbitmq_virtual_host(value)

    @field_validator("host", mode="before")
    @classmethod
    def _validate_host(cls, value: object) -> str:
        """Reject host subclasses before pydantic can normalize them."""
        if type(value) is str and value == "":
            return value
        return normalize_rabbitmq_host(value)

    @field_validator("prefetch_size", mode="after")
    @classmethod
    def _reject_byte_based_prefetch(cls, value: int) -> int:
        """R139-F4: reject ``prefetch_size != 0`` at configuration time.

        RabbitMQ does not implement byte-based prefetch: the broker answers a
        nonzero ``prefetch_size`` with NOT_IMPLEMENTED and closes the
        just-opened channel at connect. Failing fast here turns a runtime
        channel-killing connect into an explicit configuration error.
        """
        if value != 0:
            raise ConfigurationError(
                (
                    "RabbitMQ does not implement byte-based prefetch; "
                    "prefetch_size must be 0. A nonzero value is rejected by "
                    "the broker and closes the channel at connect."
                ),
                setting_name="prefetch_size",
            )
        return value

    @model_validator(mode="before")
    @classmethod
    def _expand_connection_url(cls, data: Any) -> Any:
        """Validate an AMQP URL and fill missing discrete connection fields."""
        if not isinstance(data, Mapping):
            return data
        raw_url = data.get("url")
        if raw_url is None:
            return data

        url_value = (
            raw_url.get_secret_value() if type(raw_url) is SecretStr else raw_url
        )
        if (
            type(url_value) is not str
            or not url_value.isascii()
            or any(
                character.isspace() or ord(character) < 32 or ord(character) == 127
                for character in url_value
            )
        ):
            raise ConfigurationError(
                "url must use an ASCII AMQP authority without whitespace or controls.",
                setting_name="url",
            )
        try:
            parsed = AmqpDsn(url_value)
        except ValueError:
            raise ConfigurationError(
                "url must be a valid 'amqp://' or 'amqps://' connection URL.",
                setting_name="url",
            ) from None

        if parsed.username is not None or parsed.password is not None:
            raise ConfigurationError(
                "RabbitMQ URL userinfo is not allowed; use explicit credential settings.",
                setting_name="url",
            ) from None
        if parsed.host is None:
            raise ConfigurationError(
                "RabbitMQ URL must include a host.",
                setting_name="url",
            )
        if parsed.query or parsed.fragment:
            raise ConfigurationError(
                "RabbitMQ URL must not contain a query or fragment.",
                setting_name="url",
            )
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ConfigurationError(
                "RabbitMQ URL port must be between 1 and 65535.",
                setting_name="url",
            )
        normalized_url_host = parse_endpoint_host(parsed.host)
        if normalized_url_host is None:
            raise ConfigurationError(
                "RabbitMQ URL must contain a valid DNS or IP host.",
                setting_name="url",
            )

        values = dict(data)
        values.setdefault("host", normalized_url_host)
        values.setdefault(
            "port",
            parsed.port
            if parsed.port is not None
            else (5671 if parsed.scheme == "amqps" else 5672),
        )
        virtual_host = _decode_rabbitmq_virtual_host(parsed.path)
        values.setdefault("virtual_host", virtual_host)
        if parsed.scheme == "amqps" and "ssl_enabled" in values:
            explicit_ssl = values["ssl_enabled"]
            false_text = type(explicit_ssl) is str and explicit_ssl.strip().lower() in {
                "0",
                "false",
                "no",
                "off",
            }
            if explicit_ssl is False or explicit_ssl == 0 or false_text:
                raise ConfigurationError(
                    "An amqps:// URL cannot be downgraded with ssl_enabled=False.",
                    setting_name="ssl_enabled",
                )
        values.setdefault("ssl_enabled", parsed.scheme == "amqps")
        return values

    @model_validator(mode="after")
    def _validate_mode_requirements(self) -> RabbitMQSettings:
        """SV2: mode-specific required fields for CLUSTER and MIRRORED_QUEUES.

        - CLUSTER: requires non-empty ``cluster_nodes``. Without it the client
          connects to a single ``host:port`` — the operator asked for a cluster
          but only one node is wired.
        - STANDALONE: rejects non-empty ``cluster_nodes``. The standalone
          connect path dials ``host:port`` only, but the nodes still counted
          as endpoints for TLS/guest classification — the security posture
          and the actual connection surface disagreed (a multi-node config
          silently degraded to a single point). Non-selected topology nodes
          fail rather than being ignored, mirroring the Redis mode-intent
          rejection contract.
        - MIRRORED_QUEUES: requires ``ha_mode``. Without it the connect path
          silently skips HA policy setup (the queue is non-mirrored despite the
          mode name). ``cluster_nodes`` is intentionally NOT required for
          MIRRORED_QUEUES — single-node-mirrored (HA policy on a standalone
          node) is a valid dev topology and the backend connects via
          ``host:port`` when ``cluster_nodes`` is empty.

        Raises:
            ConfigurationError: if a mode-specific required field is missing
                or contradicts the selected mode's connection surface.
        """
        if type(self.mode) is not RabbitMQMode:
            raise ConfigurationError(
                "RabbitMQ mode is unsupported.", setting_name="mode"
            )
        if type(self.cluster_nodes) is not list:
            raise ConfigurationError(
                "RabbitMQ cluster nodes must be a list of host or host:port values.",
                setting_name="cluster_nodes",
            )
        if self.mode == RabbitMQMode.CLUSTER and not self.cluster_nodes:
            raise ConfigurationError(
                (
                    "RabbitMQ CLUSTER mode requires 'cluster_nodes' to be set "
                    "(a non-empty list of host:port). Without it the client connects "
                    "to a single host:port, losing cluster topology."
                ),
                setting_name="cluster_nodes",
            )
        # R141-F16: standalone mode would silently ignore ``cluster_nodes`` for
        # connecting while the nodes still drove TLS/guest classification.
        if self.mode == RabbitMQMode.STANDALONE and self.cluster_nodes:
            raise ConfigurationError(
                (
                    "RabbitMQ cluster_nodes require mode='cluster' or mode='mirrored_queues'. "
                    "STANDALONE mode connects to a single host:port and would silently "
                    "ignore the configured nodes, so they are rejected rather than "
                    "ignored — remove cluster_nodes or switch the mode."
                ),
                setting_name="cluster_nodes",
            )
        # R30-B: strip-aware — whitespace ``ha_mode`` (``not "  "`` is False)
        # bypassed the bare truthiness check and surfaced later as a misleading
        # 'ha_mode required' at connect. Mirrors R29-D's pattern.
        ha_mode = self.ha_mode
        if self.mode == RabbitMQMode.MIRRORED_QUEUES and (
            ha_mode is None or type(ha_mode) is not str or not ha_mode.strip()
        ):
            raise ConfigurationError(
                (
                    "RabbitMQ MIRRORED_QUEUES mode requires 'ha_mode' to be set "
                    "(one of: all, exactly, nodes). Without it the connect path "
                    "silently skips HA policy setup — the queue is non-mirrored "
                    "despite the mode name."
                ),
                setting_name="ha_mode",
            )
        validate_rabbitmq_connection(
            host=self.host,
            port=self.port,
            cluster_nodes=tuple(self.cluster_nodes),
            username=self.username,
            password=_secret_text(self.password),
            ssl_enabled=self.ssl_enabled,
            ssl_cafile=self.ssl_cafile,
            ssl_certfile=self.ssl_certfile,
            ssl_keyfile=self.ssl_keyfile,
            ssl_verify_mode=self.ssl_verify_mode,
        )
        return self
