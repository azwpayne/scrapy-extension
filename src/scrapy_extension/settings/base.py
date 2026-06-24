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
  queue_max_item_bytes: int = Field(
    default=1_048_576,
    gt=0,
    description=(
      "Maximum serialized bytes allowed for a single queued request. "
      "Requests exceeding this are rejected with SerializationError at push "
      "time (preventing silent drops by capped storage backends). Default "
      "1 MiB matches the Memcached 1 MB ceiling."
    ),
  )
  pipeline_max_item_bytes: int = Field(
    default=1_048_576,
    gt=0,
    description=(
      "Maximum serialized bytes allowed for a single stored item. "
      "Items exceeding this are rejected with SerializationError at store "
      "time (preventing silent drops by capped storage backends like "
      "Memcached 1 MB, DynamoDB 400 KB)."
    ),
  )
  storage_strategy: Literal["passthrough", "batched"] = Field(
    default="passthrough",
    description=(
      "Item-persistence strategy for BackendPipeline. ``passthrough`` (default) "
      "writes each item straight to the backend — byte-identical to the "
      "pre-strategy behavior. ``batched`` buffers items and flushes in bulk at "
      "a threshold / on spider close."
    ),
  )
  circuit_breaker_enabled: bool = Field(
    default=False,
    description=(
      "Opt-in circuit breaker for hot-path backend ops (push/pop/add/contains/"
      "store/retrieve/delete). When False (default) the ConnectionManager returns "
      "raw backends unchanged — byte-identical to pre-breaker behavior, zero "
      "overhead. When True, each returned backend is wrapped so a degraded "
      "backend trips the breaker (fail-fast BackendError) instead of silently "
      "dropping requests forever."
    ),
  )
  circuit_breaker_failure_threshold: int = Field(
    default=5,
    ge=1,
    description=(
      "Consecutive failures required to trip a CLOSED breaker to OPEN. "
      "Effective only when ``circuit_breaker_enabled`` is True."
    ),
  )
  circuit_breaker_reset_timeout: float = Field(
    default=30.0,
    ge=0,
    description=(
      "Seconds an OPEN breaker waits before allowing a HALF_OPEN probe call. "
      "Effective only when ``circuit_breaker_enabled`` is True."
    ),
  )
