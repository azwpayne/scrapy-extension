"""Settings classes for scrapy-extension.

This module provides pydantic-settings based configuration for all
backend types with environment variable support.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrapy_extension.backends.base import BackendType


class Settings(BaseSettings):
  """Base settings for scrapy-extension.

  These settings apply to all backend types and can be configured
  via environment variables with the SCRAPY_ prefix.

  Attributes:
      backend_type: The type of backend to use.
      serializer: The serializer to use for data encoding.
      retry_attempts: Number of connection retry attempts.
      retry_delay: Delay between retry attempts in seconds.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_",
    case_sensitive=False,
    extra="ignore",
  )

  backend_type: BackendType = Field(
    default=BackendType.REDIS,
    description="Backend type for distributed crawling",
  )
  serializer: Literal["json"] = Field(
    default="json",
    description="Serializer to use for data encoding",
  )
  retry_attempts: int = Field(
    default=3,
    ge=0,
    description="Number of connection retry attempts",
  )
  retry_delay: float = Field(
    default=1.0,
    ge=0,
    description="Delay between retry attempts in seconds",
  )


class RedisSettings(BaseSettings):
  """Redis-specific settings.

  These settings configure the Redis connection and can be set
  via environment variables with the SCRAPY_REDIS_ prefix.

  Attributes:
      host: Redis server hostname.
      port: Redis server port.
      db: Redis database number.
      password: Redis authentication password.
      socket_timeout: Socket timeout in seconds.
      socket_connect_timeout: Socket connection timeout in seconds.
      retry_on_timeout: Whether to retry on timeout.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_REDIS_",
    case_sensitive=False,
    extra="ignore",
  )

  host: str = Field(
    default="localhost",
    description="Redis server hostname",
  )
  port: int = Field(
    default=6379,
    ge=1,
    le=65535,
    description="Redis server port",
  )
  db: int = Field(
    default=0,
    ge=0,
    description="Redis database number",
  )
  password: str | None = Field(
    default=None,
    description="Redis authentication password",
  )
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


class MongoDBSettings(BaseSettings):
  """MongoDB-specific settings.

  These settings configure the MongoDB connection and can be set
  via environment variables with the SCRAPY_MONGO_ prefix.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_MONGO_",
    case_sensitive=False,
    extra="ignore",
  )

  uri: str = Field(
    default="mongodb://localhost:27017",
    description="MongoDB connection URI",
  )
  database: str = Field(
    default="scrapy_extension",
    description="MongoDB database name",
  )
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

  # Connection pool settings
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

  # Write concern
  w: int | str = Field(
    default=1,
    description="Write concern (1, 'majority', or integer)",
  )
  journal: bool = Field(
    default=True,
    description="Wait for journal commit",
  )
  read_preference: str = Field(
    default="primary",
    description="Read preference (primary, secondary, nearest)",
  )


class KafkaSettings(BaseSettings):
  """Kafka-specific settings.

  These settings configure the Kafka connection and can be set
  via environment variables with the SCRAPY_KAFKA_ prefix.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_KAFKA_",
    case_sensitive=False,
    extra="ignore",
  )

  bootstrap_servers: str = Field(
    default="localhost:9092",
    description="Kafka bootstrap servers",
  )
  max_priority_partitions: int = Field(
    default=10,
    ge=1,
    le=255,
    description="Number of partitions for priority support",
  )

  # Producer settings
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

  # Consumer settings
  group_id: str = Field(
    default="scrapy-extension",
    description="Consumer group ID",
  )
  auto_offset_reset: str = Field(
    default="earliest",
    description="Auto offset reset (earliest, latest)",
  )
  enable_auto_commit: bool = Field(
    default=True,
    description="Enable auto commit",
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

  # Topic settings
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


class RabbitMQSettings(BaseSettings):
  """RabbitMQ-specific settings.

  These settings configure the RabbitMQ connection and can be set
  via environment variables with the SCRAPY_RABBITMQ_ prefix.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_RABBITMQ_",
    case_sensitive=False,
    extra="ignore",
  )

  host: str = Field(
    default="localhost",
    description="RabbitMQ server hostname",
  )
  port: int = Field(
    default=5672,
    ge=1,
    le=65535,
    description="RabbitMQ server port",
  )
  username: str = Field(
    default="guest",
    description="RabbitMQ username",
  )
  password: str = Field(
    default="guest",
    description="RabbitMQ password",
  )
  virtual_host: str = Field(
    default="/",
    description="RabbitMQ virtual host",
  )

  # Connection settings
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

  # Queue settings
  durable: bool = Field(
    default=True,
    description="Create durable queues",
  )
  auto_delete: bool = Field(
    default=False,
    description="Auto-delete queues when last consumer unsubscribes",
  )
  delivery_mode: int = Field(
    default=2,
    ge=1,
    le=2,
    description="Message delivery mode (1=transient, 2=persistent)",
  )
