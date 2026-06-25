"""Settings classes for scrapy-extension.

This module provides pydantic-settings based configuration for all
backend types with environment variable support.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.backends.base import BackendType
from scrapy_extension.exceptions.base import ConfigurationError


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
  pipeline_max_storage_errors: int | None = Field(
    default=None,
    description=(
      "C2 escalation: max consecutive storage errors before the pipeline "
      "re-raises (wrapped as BackendError) instead of swallowing. ``None`` "
      "(default) preserves the best-effort swallow-and-stat behavior — zero "
      "compat break. When set to N, the consecutive counter is reset to 0 on "
      "every successful store; a persistent outage surfaces loudly after N+1 "
      "consecutive failures instead of being silently absorbed as success."
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
  backpressure_pause_at: int | None = Field(
    default=None,
    description=(
      "Round-4 BP-1: queue depth at/above which the scheduler pauses "
      "``next_request`` (returns None — Scrapy's contract-correct \"slow "
      "down\" signal). ``None`` (default) disables the backpressure gate "
      "entirely — byte-identical to pre-BP behavior. When set, the engine "
      "stops pulling new requests once the pending depth reaches this "
      "threshold, protecting a downstream backend that cannot keep up. "
      "Must be ``>= 0`` (enforced by ``_validate_backpressure_thresholds``)."
    ),
  )
  backpressure_resume_at: int | None = Field(
    default=None,
    description=(
      "Round-4 BP-1: queue depth at/below which the scheduler resumes "
      "``next_request`` after a backpressure pause (hysteresis — prevents "
      "flapping around the pause threshold). ``None`` (default) means the "
      "scheduler treats ``resume_at`` as equal to ``pause_at`` at consume "
      "time (no hysteresis). Must be ``<= pause_at`` when both are set, "
      "else ``ConfigurationError`` (otherwise the resume condition would "
      "never be reachable)."
    ),
  )

  @model_validator(mode="after")
  def _validate_backpressure_thresholds(self) -> Self:
    """Round-4 BP-1: cross-validate backpressure pause/resume thresholds.

    - Each value, when set, must be ``>= 0``. Non-negativity is enforced ONLY
      by this validator — the Field declarations intentionally omit ``ge=0`` so
      violations raise ``ConfigurationError`` (not pydantic's ValidationError).
    - When BOTH ``pause_at`` and ``resume_at`` are set, ``resume_at`` must be
      ``<= pause_at`` — otherwise the resume condition (depth <= resume_at)
      could never become true once paused (depth >= pause_at > resume_at),
      deadlocking the scheduler. When only one is set the cross-check is
      skipped: the scheduler defaults ``resume_at := pause_at`` at consume
      time, so a single-value config is always self-consistent.

    Raises:
        ConfigurationError: on negative values or an inverted hysteresis band.
    """
    if self.backpressure_pause_at is not None and self.backpressure_pause_at < 0:
      raise ConfigurationError(
        (
          "backpressure_pause_at must be >= 0 "
          f"(got {self.backpressure_pause_at!r})"
        ),
        setting_name="backpressure_pause_at",
        setting_value=self.backpressure_pause_at,
      )
    if self.backpressure_resume_at is not None and self.backpressure_resume_at < 0:
      raise ConfigurationError(
        (
          "backpressure_resume_at must be >= 0 "
          f"(got {self.backpressure_resume_at!r})"
        ),
        setting_name="backpressure_resume_at",
        setting_value=self.backpressure_resume_at,
      )
    if (
      self.backpressure_pause_at is not None
      and self.backpressure_resume_at is not None
      and self.backpressure_resume_at > self.backpressure_pause_at
    ):
      raise ConfigurationError(
        (
          "backpressure_resume_at must be <= backpressure_pause_at "
          "(otherwise the resume condition can never be reached once "
          f"paused): resume_at={self.backpressure_resume_at!r} > "
          f"pause_at={self.backpressure_pause_at!r}"
        ),
        setting_name="backpressure_resume_at",
        setting_value=self.backpressure_resume_at,
      )
    return self
