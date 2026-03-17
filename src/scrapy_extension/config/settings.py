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
