# @author  : azwpayne(https://github.com/azwpayne)
# @name    : redis.py
# @time    : 2026/3/18 20:38 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :
from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisMode(str, Enum):
  """Redis deployment modes.

  Attributes:
      STANDALONE: Single Redis instance (default).
      MASTER_SLAVE: Master with replica(s) for read scaling.
      SENTINEL: High availability with automatic failover.
      CLUSTER: Redis Cluster with automatic sharding.
  """

  STANDALONE = "standalone"
  MASTER_SLAVE = "master_slave"
  SENTINEL = "sentinel"
  CLUSTER = "cluster"


class RedisSettings(BaseSettings):
  """Redis-specific settings for all deployment modes.

  These settings configure the Redis connection and can be set
  via environment variables with the SCRAPY_REDIS_ prefix.

  Supports four deployment modes:
  - standalone: Single Redis instance (default)
  - master_slave: Master with replica(s) for read scaling
  - sentinel: High availability with automatic failover
  - cluster: Redis Cluster with automatic sharding

  Attributes:
      mode: Redis deployment mode.
      host: Redis server hostname (standalone/master_slave).
      port: Redis server port (standalone/master_slave).
      db: Redis database number (standalone/master_slave/sentinel).
      password: Redis authentication password.
      socket_timeout: Socket timeout in seconds.
      socket_connect_timeout: Socket connection timeout in seconds.
      retry_on_timeout: Whether to retry on timeout.
      masters: List of master nodes for cluster mode.
      replicas: List of replica nodes for master_slave mode.
      sentinels: List of sentinel nodes for sentinel mode.
      sentinel_master_name: Master name for sentinel mode.
      sentinel_password: Separate password for sentinel authentication.
      cluster_startup_nodes: List of startup nodes for cluster mode.
      cluster_skip_full_coverage_check: Skip full coverage check for cluster.
      decode_responses: Whether to decode responses to strings.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_REDIS_",
    case_sensitive=False,
    extra="ignore",
  )

  # === Mode Selection ===
  mode: RedisMode = Field(
    default=RedisMode.STANDALONE,
    description="Redis deployment mode (standalone, master_slave, sentinel, cluster)",
  )

  # === Standalone / Master-Slave Settings ===
  host: str = Field(
    default="localhost",
    description="Redis server hostname (standalone/master_slave)",
  )
  port: int = Field(
    default=6379,
    ge=1,
    le=65535,
    description="Redis server port (standalone/master_slave)",
  )
  db: int = Field(
    default=0,
    ge=0,
    description="Redis database number (standalone/master_slave/sentinel)",
  )

  # === Authentication ===
  password: SecretStr | None = Field(
    default=None,
    description="Redis authentication password",
  )
  username: str | None = Field(
    default=None,
    description="Redis ACL username (Redis 6+)",
  )

  # === Connection Settings ===
  socket_timeout: float | None = Field(
    default=30.0,
    ge=0,
    description="Socket timeout in seconds",
  )
  socket_connect_timeout: float | None = Field(
    default=5.0,
    ge=0,
    description="Socket connection timeout in seconds",
  )
  retry_on_timeout: bool = Field(
    default=True,
    description="Whether to retry on timeout",
  )
  max_connections: int | None = Field(
    default=None,
    ge=1,
    description="Maximum connections in pool (None = unlimited)",
  )
  decode_responses: bool = Field(
    default=False,
    description="Decode responses from bytes to strings",
  )

  # === Master-Slave Mode Settings ===
  replicas: list[str] = Field(
    default_factory=list,
    description="List of replica host:port for master_slave mode",
  )
  read_from_replicas: bool = Field(
    default=True,
    description="Allow read operations from replicas",
  )

  # === Sentinel Mode Settings ===
  sentinels: list[str] = Field(
    default_factory=list,
    description="List of sentinel host:port (e.g., ['sentinel1:26379', 'sentinel2:26379'])",
  )
  sentinel_master_name: str = Field(
    default="mymaster",
    description="Master name configured in sentinel",
  )
  sentinel_password: SecretStr | None = Field(
    default=None,
    description="Password for sentinel authentication (if different from redis)",
  )
  sentinel_username: str | None = Field(
    default=None,
    description="Username for sentinel authentication (Redis 6+)",
  )
  min_other_sentinels: int = Field(
    default=0,
    ge=0,
    description="Minimum other sentinels for quorum verification",
  )
  sentinel_retry_on_timeout: bool = Field(
    default=True,
    description="Retry on timeout when connecting to sentinels",
  )

  # === Cluster Mode Settings ===
  cluster_startup_nodes: list[str] = Field(
    default_factory=list,
    description="List of startup host:port for cluster mode",
  )
  cluster_skip_full_coverage_check: bool = Field(
    default=False,
    description="Skip check that all slots are covered by cluster",
  )
  cluster_max_redirects: int = Field(
    default=5,
    ge=0,
    description="Maximum MOVED/ASK redirects to follow",
  )

  # === SSL/TLS Settings ===
  ssl_enabled: bool = Field(
    default=False,
    description="Enable SSL/TLS connection",
  )
  ssl_cafile: str | None = Field(
    default=None,
    description="Path to CA certificate file for SSL",
  )
  ssl_certfile: str | None = Field(
    default=None,
    description="Path to client certificate file for SSL",
  )
  ssl_keyfile: str | None = Field(
    default=None,
    description="Path to client private key file for SSL",
  )
  ssl_check_hostname: bool = Field(
    default=False,
    description="Verify hostname matches certificate",
  )

  @model_validator(mode="after")
  def validate_mode_requirements(self) -> RedisSettings:
    """Validate that mode-specific settings are present for the chosen mode.

    Sentinel mode requires ``sentinels`` and ``sentinel_master_name``.
    Cluster mode benefits from ``cluster_startup_nodes`` but falls back
    to ``host:port`` if not set (warning only).

    Raises:
        ValueError: If a mode-specific required field is missing.
    """
    if self.mode == RedisMode.SENTINEL:
      missing = []
      if not self.sentinels:
        missing.append("sentinels")
      if not self.sentinel_master_name:
        missing.append("sentinel_master_name")
      if missing:
        fields = " and ".join(missing)
        msg = (
          f"Redis SENTINEL mode requires '{fields}' to be set. "
          f"Got sentinels={self.sentinels!r}, "
          f"sentinel_master_name={self.sentinel_master_name!r}."
        )
        raise ValueError(msg)
    return self
