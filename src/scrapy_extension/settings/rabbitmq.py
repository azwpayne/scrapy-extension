# @author  : azwpayne(https://github.com/azwpayne)
# @name    : rabbitmq.py
# @time    : 2026/3/18 20:40 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :
from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from ipaddress import ip_address
from typing import Any, Literal
from urllib.parse import unquote

from pydantic import AmqpDsn, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrapy_extension.exceptions.base import ConfigurationError


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


def _secret_text(value: SecretStr | str | None) -> str | None:
  """Extract a secret for validation without retaining it in an exception."""
  if isinstance(value, SecretStr):
    return value.get_secret_value()
  return value


def normalize_rabbitmq_host(host: str) -> str:
  """Normalize a RabbitMQ hostname for Pika and loopback classification."""
  if not isinstance(host, str):
    raise ConfigurationError(
      "RabbitMQ host must be a non-empty hostname or IP address.",
      setting_name="host",
    )
  normalized = host.strip()
  if normalized.startswith("[") and normalized.endswith("]"):
    normalized = normalized[1:-1]
  if not normalized or any(char in normalized for char in "/@?#"):
    raise ConfigurationError(
      "RabbitMQ host must be a non-empty hostname or IP address.",
      setting_name="host",
    )
  return normalized


def parse_rabbitmq_node(node: str, default_port: int) -> tuple[str, int]:
  """Parse one cluster node without confusing bracketed or bare IPv6 hosts."""
  if not isinstance(node, str) or not node.strip():
    raise ConfigurationError(
      "RabbitMQ cluster nodes must be non-empty host or host:port values.",
      setting_name="cluster_nodes",
    )
  text = node.strip()
  port = default_port
  if text.startswith("["):
    closing = text.find("]")
    if closing < 0:
      raise ConfigurationError(
        "RabbitMQ cluster node has an invalid bracketed IPv6 host.",
        setting_name="cluster_nodes",
      )
    host = text[1:closing]
    remainder = text[closing + 1 :]
    if remainder:
      if not remainder.startswith(":") or not remainder[1:]:
        raise ConfigurationError(
          "RabbitMQ cluster node must use '[IPv6]:port' syntax.",
          setting_name="cluster_nodes",
        )
      port_text = remainder[1:]
      try:
        port = int(port_text)
      except ValueError:
        raise ConfigurationError(
          "RabbitMQ cluster node port must be an integer.",
          setting_name="cluster_nodes",
        ) from None
  else:
    try:
      ip_address(text)
    except ValueError:
      if text.count(":") > 1:
        raise ConfigurationError(
          "RabbitMQ IPv6 cluster nodes with a port must use '[IPv6]:port'.",
          setting_name="cluster_nodes",
        ) from None
      if ":" in text:
        host, port_text = text.rsplit(":", 1)
        if not port_text:
          raise ConfigurationError(
            "RabbitMQ cluster node port cannot be empty.",
            setting_name="cluster_nodes",
          )
        try:
          port = int(port_text)
        except ValueError:
          raise ConfigurationError(
            "RabbitMQ cluster node port must be an integer.",
            setting_name="cluster_nodes",
          ) from None
      else:
        host = text
    else:
      host = text

  normalized_host = normalize_rabbitmq_host(host)
  if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
    raise ConfigurationError(
      "RabbitMQ cluster node port must be between 1 and 65535.",
      setting_name="cluster_nodes",
    )
  return normalized_host, port


def _is_loopback_host(host: str) -> bool:
  normalized = normalize_rabbitmq_host(host).lower().rstrip(".")
  if normalized == "localhost" or normalized.endswith(".localhost"):
    return True
  try:
    return ip_address(normalized).is_loopback
  except ValueError:
    return False


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
  if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
    raise ConfigurationError(
      "RabbitMQ port must be between 1 and 65535.",
      setting_name="port",
    )
  if not isinstance(username, str) or not username.strip():
    raise ConfigurationError(
      "RabbitMQ username must be explicitly set and cannot be blank.",
      setting_name="username",
    )
  if not isinstance(password, str) or not password.strip():
    raise ConfigurationError(
      "RabbitMQ password must be explicitly set and cannot be blank.",
      setting_name="password",
    )
  if not isinstance(ssl_enabled, bool):
    raise ConfigurationError(
      "RabbitMQ ssl_enabled must be a boolean.",
      setting_name="ssl_enabled",
    )

  tls_paths = {
    "ssl_cafile": ssl_cafile,
    "ssl_certfile": ssl_certfile,
    "ssl_keyfile": ssl_keyfile,
  }
  for setting_name, value in tls_paths.items():
    if value is not None and (not isinstance(value, str) or not value.strip()):
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
  if ssl_enabled and ssl_verify_mode != "CERT_REQUIRED":
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


class RabbitMQSettings(BaseSettings):
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
  cluster_node_type: Literal["disc", "ram"] = Field(
    default="disc",
    description="Node type for cluster (disc or ram)",
  )

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
  ssl_verify_mode: Literal["CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED"] = (
    Field(
      default="CERT_REQUIRED",
      description="SSL verification mode (CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED)",
    )
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
    description="Heartbeat interval in seconds",
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
    description="QoS prefetch count (0 = unlimited)",
  )
  prefetch_size: int = Field(
    default=0,
    ge=0,
    description="QoS prefetch size in bytes (0 = unlimited)",
  )

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
      raw_url.get_secret_value() if isinstance(raw_url, SecretStr) else str(raw_url)
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

    values = dict(data)
    if parsed.host is not None:
      host = parsed.host
      if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
      values.setdefault("host", host)
    values.setdefault(
      "port", parsed.port or (5671 if parsed.scheme == "amqps" else 5672)
    )
    path = parsed.path or ""
    virtual_host = unquote(path[1:] if path.startswith("/") else path)
    values.setdefault("virtual_host", virtual_host or "/")
    if parsed.scheme == "amqps" and "ssl_enabled" in values:
      explicit_ssl = values["ssl_enabled"]
      false_text = (
        isinstance(explicit_ssl, str)
        and explicit_ssl.strip().lower() in {"0", "false", "no", "off"}
      )
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
    - MIRRORED_QUEUES: requires ``ha_mode``. Without it the connect path
      silently skips HA policy setup (the queue is non-mirrored despite the
      mode name). ``cluster_nodes`` is intentionally NOT required for
      MIRRORED_QUEUES — single-node-mirrored (HA policy on a standalone
      node) is a valid dev topology and the backend connects via
      ``host:port`` when ``cluster_nodes`` is empty.

    Raises:
        ConfigurationError: if a mode-specific required field is missing.
    """
    if self.mode == RabbitMQMode.CLUSTER and not self.cluster_nodes:
      raise ConfigurationError(
        (
          "RabbitMQ CLUSTER mode requires 'cluster_nodes' to be set "
          "(a non-empty list of host:port). Without it the client connects "
          f"to a single host:port, losing cluster topology. "
          f"Got cluster_nodes={self.cluster_nodes!r}."
        ),
        setting_name="cluster_nodes",
        setting_value=self.cluster_nodes,
      )
    if (
      self.mode == RabbitMQMode.MIRRORED_QUEUES
      and not self.ha_mode
    ):
      raise ConfigurationError(
        (
          "RabbitMQ MIRRORED_QUEUES mode requires 'ha_mode' to be set "
          "(one of: all, exactly, nodes). Without it the connect path "
          "silently skips HA policy setup — the queue is non-mirrored "
          f"despite the mode name. Got ha_mode={self.ha_mode!r}."
        ),
        setting_name="ha_mode",
        setting_value=self.ha_mode,
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
