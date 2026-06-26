# @author  : azwpayne(https://github.com/azwpayne)
# @name    : rabbitmq.py
# @time    : 2026/3/18 20:40 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import Field, SecretStr, model_validator
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
    extra="ignore",
  )

  # === Mode Selection ===
  mode: RabbitMQMode = Field(
    default=RabbitMQMode.STANDALONE,
    description="RabbitMQ deployment mode (standalone, cluster, mirrored_queues)",
  )

  # === Connection Settings ===
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
    return self
