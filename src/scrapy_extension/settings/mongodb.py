# @author  : azwpayne(https://github.com/azwpayne)
# @name    : mongodb.py
# @time    : 2026/3/18 20:39 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :

from __future__ import annotations

from enum import Enum
from typing import ClassVar, Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError

_VALID_MONGO_SCHEMES: tuple[str, ...] = ("mongodb://", "mongodb+srv://")


def validate_mongodb_write_concern(
  w: object, w_timeout_ms: object
) -> tuple[int | str, int | None]:
  """Return a write concern whose completion confirms server acknowledgement."""
  normalized_w: int | str
  if isinstance(w, bool):
    raise ConfigurationError(
      "MongoDB w must be a positive integer or 'majority', not a boolean.",
      setting_name="w",
    )
  if isinstance(w, int):
    if w < 1:
      raise ConfigurationError(
        "MongoDB mutations require an acknowledged write concern (w >= 1).",
        setting_name="w",
      )
    normalized_w = w
  elif isinstance(w, str):
    candidate = w.strip()
    if candidate == "majority":
      normalized_w = candidate
    else:
      try:
        numeric_w = int(candidate, 10)
      except ValueError:
        raise ConfigurationError(
          "MongoDB w must be a positive integer or 'majority'.",
          setting_name="w",
        ) from None
      if numeric_w < 1:
        raise ConfigurationError(
          "MongoDB mutations require an acknowledged write concern (w >= 1).",
          setting_name="w",
        )
      normalized_w = numeric_w
  else:
    raise ConfigurationError(
      "MongoDB w must be a positive integer or 'majority'.",
      setting_name="w",
    )

  if w_timeout_ms is None:
    normalized_timeout = None
  elif isinstance(w_timeout_ms, bool):
    raise ConfigurationError(
      "MongoDB w_timeout_ms must be a non-negative integer or None.",
      setting_name="w_timeout_ms",
    )
  elif isinstance(w_timeout_ms, int):
    if w_timeout_ms < 0:
      raise ConfigurationError(
        "MongoDB w_timeout_ms must be a non-negative integer or None.",
        setting_name="w_timeout_ms",
      )
    normalized_timeout = w_timeout_ms
  elif isinstance(w_timeout_ms, str):
    try:
      normalized_timeout = int(w_timeout_ms.strip(), 10)
    except ValueError:
      raise ConfigurationError(
        "MongoDB w_timeout_ms must be a non-negative integer or None.",
        setting_name="w_timeout_ms",
      ) from None
    if normalized_timeout < 0:
      raise ConfigurationError(
        "MongoDB w_timeout_ms must be a non-negative integer or None.",
        setting_name="w_timeout_ms",
      )
  else:
    raise ConfigurationError(
      "MongoDB w_timeout_ms must be a non-negative integer or None.",
      setting_name="w_timeout_ms",
    )
  return normalized_w, normalized_timeout


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
    extra="forbid",
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
    description=(
      "Optional Atlas cluster label. It cannot replace uri because an Atlas "
      "SRV hostname cannot be derived from the display name alone."
    ),
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
    description="Acknowledged write concern (positive integer or 'majority')",
  )
  journal: bool = Field(
    default=True,
    description="Wait for journal commit",
  )
  w_timeout_ms: int | None = Field(
    default=None,
    description="Non-negative write concern timeout in milliseconds",
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

  @field_validator("w", mode="before")
  @classmethod
  def _normalize_write_concern(cls, value: object) -> int | str:
    """Normalize numeric environment text and reject bool before coercion."""
    normalized, _timeout = validate_mongodb_write_concern(value, None)
    return normalized

  @field_validator("w_timeout_ms", mode="before")
  @classmethod
  def _reject_invalid_write_timeout(cls, value: object) -> int | None:
    """Reject booleans and negatives before pydantic can coerce them."""
    _w, normalized = validate_mongodb_write_concern(1, value)
    return normalized

  @model_validator(mode="after")
  def _validate_write_concern(self) -> Self:
    """Require a server-acknowledged public mutation boundary."""
    validate_mongodb_write_concern(self.w, self.w_timeout_ms)
    return self

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

  @field_validator("uri")
  @classmethod
  def _validate_uri_scheme(cls, v: str) -> str:
    """SV4: ``uri`` must start with ``mongodb://`` or ``mongodb+srv://``.

    A bare ``host:port`` or empty string otherwise surfaces as an opaque
    ``InvalidURI`` / ``ConfigurationError`` at ``MongoClient`` construction.
    ``min_length=1`` rejects the empty string at the Field level; this
    validator guards the scheme. ``mongodb+srv://`` is required for Atlas
    (DNS SRV records).

    Raises:
        ConfigurationError: if ``uri`` does not start with a valid scheme.
    """
    if not v or not v.lower().startswith(_VALID_MONGO_SCHEMES):
      raise ConfigurationError(
        "uri must start with 'mongodb://' or 'mongodb+srv://'.",
        setting_name="uri",
      )
    return v

  @model_validator(mode="after")
  def _validate_mode_requirements(self) -> Self:
    """SV2: mode-specific required fields for REPLICA_SET and ATLAS.

    - REPLICA_SET: requires ``replica_set_name`` OR a ``uri`` that already
      carries a ``?replicaSet=`` query (the driver-recognized way to declare
      the RS in the URI). This preserves the documented URI-verbatim fallback
      (mongodb.py:_connect_replica_set) while catching the genuine footgun
      (REPLICA_SET mode with neither name nor URI hint → opaque driver error).
    - ATLAS: requires an explicit ``mongodb+srv://`` URI. The backend connects
      with ``uri`` verbatim; ``atlas_cluster_name`` is only a label and lacks
      the deployment-specific DNS suffix needed to construct a connection URI.

    Mirrors the Redis SENTINEL validator (raise, not warn). STANDALONE and
    SHARDED_CLUSTER are unaffected (SHARDED_CLUSTER uses mongos routers in
    the URI; no extra hint needed).

    Raises:
        ConfigurationError: if a mode-specific required field is missing.
    """
    if self.mode == MongoDBMode.REPLICA_SET:
      uri_has_rs = "replicaSet=" in self.uri
      if not self.replica_set_name and not uri_has_rs:
        raise ConfigurationError(
          (
            "MongoDB REPLICA_SET mode requires 'replica_set_name' to be set, "
            "or a uri that already carries a '?replicaSet=...' query."
          ),
          setting_name="replica_set_name",
          setting_value=self.replica_set_name,
        )
    elif self.mode == MongoDBMode.ATLAS:
      uri_is_srv = self.uri.lower().startswith("mongodb+srv://")
      if not uri_is_srv:
        raise ConfigurationError(
          (
            "MongoDB ATLAS mode requires an explicit 'mongodb+srv://' uri. "
            "atlas_cluster_name cannot replace uri because the backend uses "
            "uri verbatim and a complete Atlas SRV hostname cannot be derived "
            "from a cluster display name."
          ),
          setting_name="uri",
        )
    return self

  @model_validator(mode="after")
  def _validate_pool_size_ordering(self) -> Self:
    """SV3-4 (M): ``min_pool_size <= max_pool_size``.

    An inverted pair (min > max) makes pymongo's connection pool unable to
    ever satisfy a checkout → opaque ``ConnectionFailure`` / deadlock under
    load. Catch at config time. Both bounds are individually constrained by
    Field-level ``ge`` (min ≥ 0, max ≥ 1); this validator guards their
    relative ordering.

    Raises:
        ConfigurationError: if ``min_pool_size > max_pool_size``.
    """
    if self.min_pool_size > self.max_pool_size:
      raise ConfigurationError(
        (
          "min_pool_size must be <= max_pool_size — an inverted pair makes "
          "the connection pool unable to satisfy any checkout (deadlock "
          "under load). "
          f"Got min_pool_size={self.min_pool_size}, "
          f"max_pool_size={self.max_pool_size}."
        ),
        setting_name="min_pool_size",
        setting_value=self.min_pool_size,
      )
    return self
