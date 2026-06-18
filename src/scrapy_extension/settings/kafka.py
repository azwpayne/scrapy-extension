# @author  : azwpayne(https://github.com/azwpayne)
# @name    : kafka.py
# @time    : 2026/3/18 20:39 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :
from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    extra="ignore",
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
  security_protocol: str = Field(
    default="PLAINTEXT",
    description="Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)",
  )
  sasl_mechanism: str | None = Field(
    default=None,
    description="SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI)",
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
  compression_type: str | None = Field(
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
  auto_offset_reset: str = Field(
    default="earliest",
    description="Auto offset reset (earliest, latest)",
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
