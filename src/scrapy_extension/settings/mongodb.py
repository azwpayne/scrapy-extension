# @author  : azwpayne(https://github.com/azwpayne)
# @name    : mongodb.py
# @time    : 2026/3/18 20:39 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :

from __future__ import annotations

from enum import Enum
from typing import ClassVar, Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError


class MongoDBMode(str, Enum):
  """MongoDB deployment modes.

  Attributes:
      STANDALONE: Single MongoDB instance (default).
      REPLICA_SET: Replica set for high availability.
      SHARDED_CLUSTER: Sharded cluster for horizontal scaling.
      ATLAS: MongoDB Atlas cloud service.
  """

  STANDALONE = "standalone"
  REPLICA_SET = "replica_set"
  SHARDED_CLUSTER = "sharded_cluster"
  ATLAS = "atlas"


class MongoDBSettings(BaseSettings):
  """MongoDB-specific settings for all deployment modes.

  These settings configure the MongoDB connection and can be set
  via environment variables with the SCRAPY_MONGO_ prefix.

  Supports four deployment modes:
  - standalone: Single MongoDB instance (default)
  - replica_set: Replica set for high availability
  - sharded_cluster: Sharded cluster for horizontal scaling
  - atlas: MongoDB Atlas cloud service
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_MONGO_",
    case_sensitive=False,
    extra="ignore",
  )

  # === Mode Selection ===
  mode: MongoDBMode = Field(
    default=MongoDBMode.STANDALONE,
    description="MongoDB deployment mode (standalone, replica_set, sharded_cluster, atlas)",
  )

  # === Connection Settings ===
  uri: str = Field(
    default="mongodb://localhost:27017",
    description="MongoDB connection URI (used for all modes)",
  )
  database: str = Field(
    default="scrapy_extension",
    description="MongoDB database name",
  )

  # === Collection Names ===
  queue_collection: str = Field(
    default="queues",
    description="Collection name for queue storage",
  )
  set_collection: str = Field(
    default="sets",
    description="Collection name for set storage",
  )
  storage_collection: str = Field(
    default="storage",
    description="Collection name for key-value storage",
  )

  # === Replica Set Settings ===
  replica_set_name: str | None = Field(
    default=None,
    description="Replica set name (for replica_set mode)",
  )
  replica_set_members: list[str] = Field(
    default_factory=list,
    description="List of replica set member host:port",
  )
  read_preference: Literal[
    "primary",
    "primaryPreferred",
    "secondary",
    "secondaryPreferred",
    "nearest",
  ] = Field(
    default="primary",
    description="Read preference (primary, secondary, nearest, primaryPreferred, secondaryPreferred)",
  )

  # === Sharded Cluster Settings ===
  mongos_routers: list[str] = Field(
    default_factory=list,
    description="List of mongos router host:port (for sharded_cluster mode)",
  )

  # === Atlas Settings ===
  atlas_cluster_name: str | None = Field(
    default=None,
    description="Atlas cluster name (for atlas mode)",
  )

  # === Connection Pool Settings ===
  min_pool_size: int = Field(
    default=1,
    ge=0,
    description="Minimum connection pool size",
  )
  max_pool_size: int = Field(
    default=10,
    ge=1,
    description="Maximum connection pool size",
  )
  max_idle_time_ms: int = Field(
    default=60000,
    ge=0,
    description="Maximum connection idle time in milliseconds",
  )
  wait_queue_timeout_ms: int = Field(
    default=5000,
    ge=0,
    description="Maximum wait time for connection from pool",
  )

  # === Authentication Settings ===
  username: str | None = Field(
    default=None,
    description="MongoDB username",
  )
  password: SecretStr | None = Field(
    default=None,
    description="MongoDB password",
  )
  auth_source: str = Field(
    default="admin",
    description="Authentication database",
  )
  auth_mechanism: (
    Literal[
      "SCRAM-SHA-1",
      "SCRAM-SHA-256",
      "MONGODB-CR",
      "PLAIN",
      "GSSAPI",
      "MONGODB-X509",
      "MONGODB-AWS",
    ]
    | None
  ) = Field(
    default=None,
    description="Authentication mechanism (SCRAM-SHA-1, SCRAM-SHA-256, MONGODB-CR, PLAIN, GSSAPI, MONGODB-X509, MONGODB-AWS)",
  )

  # === TLS/SSL Settings ===
  tls_enabled: bool = Field(
    default=False,
    description="Enable TLS/SSL connection",
  )
  tls_ca_file: str | None = Field(
    default=None,
    description="Path to CA certificate file",
  )
  tls_cert_file: str | None = Field(
    default=None,
    description="Path to client certificate file",
  )
  tls_key_file: str | None = Field(
    default=None,
    description="Path to client private key file",
  )
  tls_allow_invalid_certificates: bool = Field(
    default=False,
    description="Allow invalid certificates (not recommended for production)",
  )

  # === Write Concern ===
  w: int | str = Field(
    default=1,
    description="Write concern (1, 'majority', or integer)",
  )
  journal: bool = Field(
    default=True,
    description="Wait for journal commit",
  )
  w_timeout_ms: int | None = Field(
    default=None,
    description="Write concern timeout in milliseconds",
  )

  # === Server Selection ===
  server_selection_timeout_ms: int = Field(
    default=30000,
    ge=0,
    description="Server selection timeout in milliseconds",
  )
  heartbeat_frequency_ms: int = Field(
    default=10000,
    ge=0,
    description="Heartbeat frequency in milliseconds",
  )

  # Production-tier modes where disabling cert validation is virtually always
  # a misconfiguration / dev shortcut that must not ship. STANDALONE stays
  # permissive (local dev with a self-signed mongod). ClassVar so pydantic
  # does not treat it as a setting field.
  _PRODUCTION_MODES: ClassVar[frozenset[MongoDBMode]] = frozenset(
    {MongoDBMode.ATLAS, MongoDBMode.SHARDED_CLUSTER, MongoDBMode.REPLICA_SET}
  )

  @model_validator(mode="after")
  def _validate_tls_insecure_not_in_production_mode(self) -> Self:
    """SEC-2: forbid ``tls_allow_invalid_certificates=True`` in production modes.

    Disabling certificate validation strips TLS of its MITM protection. In
    multi-host, production-tier deployments (ATLAS / SHARDED_CLUSTER /
    REPLICA_SET) this is virtually always a misconfiguration or a developer
    shortcut that must not ship — fail-fast at construction rather than
    silently degrading the connection's security posture. STANDALONE remains
    permissive for local dev (e.g. a self-signed local mongod).

    Mirrors the Redis ``ssl_check_hostname`` guidance and the RabbitMQ
    guest-guard pattern (raise, not warn — deterministic + the project's
    ``error::UserWarning`` filter makes warn-by-default risky).

    Raises:
        ConfigurationError: if ``tls_allow_invalid_certificates`` is True and
            ``mode`` is one of the production-tier modes.
    """
    if (
      self.tls_allow_invalid_certificates
      and self.mode in self._PRODUCTION_MODES
    ):
      raise ConfigurationError(
        (
          "tls_allow_invalid_certificates=True disables certificate "
          "validation; not permitted in production-tier MongoDB modes "
          f"(mode={self.mode.value!r}). Either set "
          "tls_allow_invalid_certificates=False or use STANDALONE for local "
          "dev with a self-signed certificate."
        ),
        setting_name="tls_allow_invalid_certificates",
        setting_value=True,
      )
    return self
