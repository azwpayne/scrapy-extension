# @author  : azwpayne(https://github.com/azwpayne)
# @name    : kafka.py
# @time    : 2026/3/18 20:39 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrapy_extension.exceptions.base import ConfigurationError


class KafkaMode(str, Enum):
  """Kafka deployment modes.

  Attributes:
      STANDALONE: Single Kafka broker (default).
      CLUSTER: Multi-broker Kafka cluster.
      CONFLUENT: Confluent Cloud configuration.
  """

  STANDALONE = "standalone"
  CLUSTER = "cluster"
  CONFLUENT = "confluent"


_KAFKA_SECURITY_PROTOCOLS = frozenset(
  {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
)
_PASSWORD_SASL_MECHANISMS = frozenset(
  {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
)


def _kafka_credential_value(value: object, field_name: str) -> str | None:
  """Return a non-empty credential without retaining it in failures."""
  if value is None:
    return None
  if isinstance(value, SecretStr):
    raw_value = value.get_secret_value()
  elif isinstance(value, str):
    raw_value = value
  else:
    raise ConfigurationError(
      f"{field_name} must be a string when explicitly configured.",
      setting_name=field_name,
    )
  if not raw_value.strip():
    raise ConfigurationError(
      f"{field_name} must be non-empty when explicitly configured.",
      setting_name=field_name,
    )
  return raw_value


def validate_kafka_authentication(
  mode: object,
  security_protocol: object,
  sasl_mechanism: object,
  sasl_username: object,
  sasl_password: object,
  confluent_api_key: object,
  confluent_api_secret: object,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
  """Validate one mechanism-aware Kafka authentication value set.

  The raw values returned here are intended only for immediate SDK config
  construction. Callers retaining them must use a repr-redacting wrapper.
  Invalid credentials are never attached to the raised exception.
  """
  if security_protocol not in _KAFKA_SECURITY_PROTOCOLS:
    raise ConfigurationError(
      "security_protocol must be a supported Kafka protocol.",
      setting_name="security_protocol",
    )
  protocol = str(security_protocol)
  sasl_fields_set = (
    sasl_username is not None
    or sasl_password is not None
    or sasl_mechanism is not None
  )
  sasl_enabled = protocol.startswith("SASL_")
  if sasl_fields_set and not sasl_enabled:
    raise ConfigurationError(
      (
        "SASL credentials (sasl_username / sasl_password / sasl_mechanism) "
        "require a 'SASL_'-prefixed security_protocol "
        "('SASL_PLAINTEXT' or 'SASL_SSL'); kafka-python silently ignores "
        "the SASL fields otherwise (auth never attempted). "
        f"Got security_protocol={protocol!r}."
      ),
      setting_name="security_protocol",
      setting_value=protocol,
    )

  mechanism: str | None = None
  username: str | None = None
  password: str | None = None
  if sasl_enabled:
    if not isinstance(sasl_mechanism, str) or not sasl_mechanism:
      raise ConfigurationError(
        "A SASL security_protocol requires an explicit sasl_mechanism.",
        setting_name="sasl_mechanism",
      )
    mechanism = sasl_mechanism
    if mechanism in _PASSWORD_SASL_MECHANISMS:
      username = _kafka_credential_value(sasl_username, "sasl_username")
      if username is None:
        raise ConfigurationError(
          f"{mechanism} authentication requires sasl_username.",
          setting_name="sasl_username",
        )
      password = _kafka_credential_value(sasl_password, "sasl_password")
      if password is None:
        raise ConfigurationError(
          f"{mechanism} authentication requires sasl_password.",
          setting_name="sasl_password",
        )
    elif mechanism == "GSSAPI":
      if sasl_username is not None or sasl_password is not None:
        raise ConfigurationError(
          "GSSAPI uses ambient Kerberos credentials; sasl_username and "
          "sasl_password would be ignored.",
          setting_name=(
            "sasl_username" if sasl_username is not None else "sasl_password"
          ),
        )
    elif mechanism == "OAUTHBEARER":
      raise ConfigurationError(
        "OAUTHBEARER is unsupported because this backend does not expose the "
        "token-provider object required by kafka-python.",
        setting_name="sasl_mechanism",
      )
    else:
      raise ConfigurationError(
        "sasl_mechanism must be supported by this Kafka backend.",
        setting_name="sasl_mechanism",
      )

  key: str | None = None
  secret: str | None = None
  confluent_fields_set = (
    confluent_api_key is not None or confluent_api_secret is not None
  )
  if mode == KafkaMode.CONFLUENT:
    missing = []
    if confluent_api_key is None:
      missing.append("confluent_api_key")
    if confluent_api_secret is None:
      missing.append("confluent_api_secret")
    if missing:
      fields = " and ".join(missing)
      raise ConfigurationError(
        (
          f"Kafka CONFLUENT mode requires '{fields}' to be set. "
          "Without them the client could fall back to an unauthenticated "
          "SDK transport."
        ),
        setting_name=missing[0],
      )
    key = _kafka_credential_value(confluent_api_key, "confluent_api_key")
    secret = _kafka_credential_value(
      confluent_api_secret, "confluent_api_secret"
    )
  elif confluent_fields_set:
    raise ConfigurationError(
      "Confluent API credentials require mode='confluent'; other modes ignore them.",
      setting_name="mode",
    )

  return mechanism, username, password, key, secret


def _kafka_policy_int(value: object, field_name: str, minimum: int) -> int:
  """Return a bounded policy integer, rejecting bools after model mutation."""
  if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
    raise ConfigurationError(
      f"{field_name} must be an integer greater than or equal to {minimum}.",
      setting_name=field_name,
    )
  return value


def validate_kafka_delivery_policy(
  acks: object,
  max_priority_partitions: object,
  num_partitions: object,
  replication_factor: object,
  retention_ms: object,
  min_insync_replicas: object,
) -> tuple[int | str, int, int, int, int]:
  """Validate the broker-confirmed enqueue and new-topic durability policy."""
  if isinstance(acks, bool) or acks not in (1, "all"):
    raise ConfigurationError(
      "Kafka QueueBackend requires acks=1 or acks='all'; acks=0 cannot "
      "confirm broker acceptance.",
      setting_name="acks",
    )
  normalized_acks: int | str = acks
  priority_partitions = _kafka_policy_int(
    max_priority_partitions, "max_priority_partitions", 1
  )
  configured_partitions = _kafka_policy_int(
    num_partitions, "num_partitions", 1
  )
  if configured_partitions != priority_partitions:
    raise ConfigurationError(
      "num_partitions and max_priority_partitions must match because Kafka "
      "priority values map directly to physical partitions.",
      setting_name="num_partitions",
    )
  replicas = _kafka_policy_int(replication_factor, "replication_factor", 1)
  retention = _kafka_policy_int(retention_ms, "retention_ms", 0)
  min_isr = _kafka_policy_int(
    min_insync_replicas, "min_insync_replicas", 1
  )
  if min_isr > replicas:
    raise ConfigurationError(
      "min_insync_replicas cannot exceed replication_factor.",
      setting_name="min_insync_replicas",
    )
  return normalized_acks, priority_partitions, replicas, retention, min_isr


class KafkaSettings(BaseSettings):
  """Kafka-specific settings for all deployment modes.

  These settings configure the Kafka connection and can be set
  via environment variables with the SCRAPY_KAFKA_ prefix.

  Supports three deployment modes:
  - standalone: Single Kafka broker (default)
  - cluster: Multi-broker Kafka cluster
  - confluent: Confluent Cloud configuration
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_KAFKA_",
    case_sensitive=False,
    extra="forbid",
  )

  # === Mode Selection ===
  mode: KafkaMode = Field(
    default=KafkaMode.STANDALONE,
    description="Kafka deployment mode (standalone, cluster, confluent)",
  )

  # === Connection Settings ===
  bootstrap_servers: str = Field(
    default="localhost:9092",
    description="Kafka bootstrap servers (comma-separated for cluster)",
  )

  # === Cluster Settings ===
  cluster_brokers: list[str] = Field(
    default_factory=list,
    description="List of broker host:port for cluster mode",
  )

  # === SASL/SSL Authentication ===
  security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = (
    Field(
      default="PLAINTEXT",
      description="Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)",
    )
  )
  sasl_mechanism: (
    Literal[
      "PLAIN",
      "SCRAM-SHA-256",
      "SCRAM-SHA-512",
      "GSSAPI",
      "OAUTHBEARER",
    ]
    | None
  ) = Field(
    default=None,
    description="SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER)",
  )
  sasl_username: str | None = Field(
    default=None,
    description="SASL username",
  )
  sasl_password: SecretStr | None = Field(
    default=None,
    description="SASL password",
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
    default=True,
    description="Verify broker hostname matches certificate",
  )

  # === Confluent Cloud Settings ===
  confluent_api_key: SecretStr | None = Field(
    default=None,
    description="Confluent Cloud API key",
  )
  confluent_api_secret: SecretStr | None = Field(
    default=None,
    description="Confluent Cloud API secret",
  )
  confluent_bootstrap_servers: str | None = Field(
    default=None,
    description="Confluent Cloud bootstrap servers (e.g., pkc-xxx.us-east-1.aws.confluent.cloud:9092)",
  )

  # === Priority Queue Settings ===
  max_priority_partitions: int = Field(
    default=10,
    ge=1,
    le=255,
    description="Number of partitions for priority support",
  )

  # === Producer Settings ===
  acks: str | int = Field(
    default="all",
    description="Producer acks (0, 1, or 'all')",
  )
  retries: int = Field(
    default=3,
    ge=0,
    description="Number of send retries",
  )
  batch_size: int = Field(
    default=16384,
    ge=0,
    description="Batch size in bytes",
  )
  linger_ms: int = Field(
    default=5,
    ge=0,
    description="Time to wait for batching",
  )
  compression_type: Literal["gzip", "snappy", "lz4", "zstd"] | None = Field(
    default=None,
    description="Compression type (gzip, snappy, lz4, zstd)",
  )
  max_in_flight_requests_per_connection: int = Field(
    default=5,
    ge=1,
    description="Max in-flight requests per connection",
  )

  # === Consumer Settings ===
  group_id: str = Field(
    default="scrapy-extension",
    description="Consumer group ID",
  )
  auto_offset_reset: Literal["earliest", "latest", "none"] = Field(
    default="earliest",
    description="Auto offset reset (earliest, latest, none)",
  )
  enable_auto_commit: bool = Field(
    default=False,
    description=(
      "Enable auto commit. Defaults to False so callers control ack timing "
      "via QueueBackend.ack(); auto-commit acks before processing and "
      "loses messages if the worker crashes mid-request."
    ),
  )
  auto_commit_interval_ms: int = Field(
    default=5000,
    ge=0,
    description="Auto commit interval in ms",
  )
  max_poll_records: int = Field(
    default=500,
    ge=1,
    description="Max records per poll",
  )
  session_timeout_ms: int = Field(
    default=10000,
    ge=0,
    description="Session timeout in ms",
  )
  request_timeout_ms: int = Field(
    default=40000,
    ge=0,
    description="Request timeout in ms",
  )

  # === Topic Settings ===
  replication_factor: int = Field(
    default=1,
    ge=1,
    description="Topic replication factor",
  )
  num_partitions: int = Field(
    default=10,
    ge=1,
    description="Number of topic partitions",
  )
  retention_ms: int = Field(
    default=604800000,
    ge=0,
    description="Retention time in ms (7 days)",
  )
  min_insync_replicas: int = Field(
    default=1,
    ge=1,
    description="Minimum in-sync replicas for producer acks",
  )

  @field_validator("acks", mode="before")
  @classmethod
  def _reject_boolean_acks(cls, value: object) -> object:
    """Normalize env text while preventing bool-to-int coercion."""
    if isinstance(value, bool):
      raise ConfigurationError(
        "Kafka acks must be 1 or 'all', not a boolean.", setting_name="acks"
      )
    if value == "1":
      return 1
    return value

  @model_validator(mode="after")
  def _validate_authentication(self) -> KafkaSettings:
    """Fail fast on incomplete or mechanism-inconsistent authentication."""
    validate_kafka_authentication(
      self.mode,
      self.security_protocol,
      self.sasl_mechanism,
      self.sasl_username,
      self.sasl_password,
      self.confluent_api_key,
      self.confluent_api_secret,
    )
    return self

  @model_validator(mode="after")
  def _validate_delivery_policy(self) -> KafkaSettings:
    """Require confirmed sends and a coherent new-topic durability policy."""
    validate_kafka_delivery_policy(
      self.acks,
      self.max_priority_partitions,
      self.num_partitions,
      self.replication_factor,
      self.retention_ms,
      self.min_insync_replicas,
    )
    return self
