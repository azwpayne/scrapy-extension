"""Settings classes for scrapy-extension.

This module provides pydantic-settings based configuration for all
backend types with environment variable support.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator, model_validator
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

  backend_type: BackendType | str = Field(
    default=BackendType.REDIS,
    description=(
      "Backend type for distributed crawling. Accepts any bundled "
      "``BackendType`` member (e.g. ``'redis'``) OR any 3rd-party backend "
      "string registered via the ``scrapy_extension.backends`` entry-point "
      "group (round-5 R5-1). Unknown values raise ``ConfigurationError`` "
      "(not pydantic ``ValidationError``) so the exception family is "
      "consistent with every other settings-validation path (round-14 R14-B)."
    ),
  )

  @field_validator("backend_type", mode="before")
  @classmethod
  def _validate_backend_type(cls, value: object) -> BackendType | str:
    """Accept any ``BackendType`` member OR any registry-known string.

    Round-14 R14-B: round-9 regressed round-5 R5-1 — the ``BackendType`` enum
    field rejected 3rd-party strings with pydantic ``ValidationError`` before
    the registry-aware ``resolve_backend_config`` could accept them. This
    validator restores the registry-aware contract AND routes unknown values
    through ``ConfigurationError`` (the project's config-error family) so the
    exception family is uniform across every settings-validation path and the
    ``setting_name`` attribute is preserved for downstream log handlers
    (frozen Stable in STABILITY.md).

    Resolution order:
      1. ``BackendType`` member → returned as-is (bundled-backend fast path).
      2. Bundled ``BackendType`` value string (``'redis'``) → coerced to the
         member (preserves the byte-identical default-behavior invariant).
      3. Registry-known 3rd-party string (``'myplugin'``) → returned as-is so
         ``resolve_backend_config`` can dispatch via the entry-point path.
      4. Anything else → ``ConfigurationError(setting_name='SCRAPY_BACKEND_TYPE')``.

    Args:
        value: The raw input (``BackendType``, ``str``, or invalid).

    Returns:
        A ``BackendType`` member (bundled) or registry-known ``str`` (3rd-party).

    Raises:
        ConfigurationError: If ``value`` is not a ``BackendType`` and not a
            registry-known string.
    """
    # (1) Already a BackendType member — bundled-backend fast path.
    if isinstance(value, BackendType):
      return value
    # (2) & (3) String — try bundled-member coercion, then registry lookup.
    if isinstance(value, str):
      try:
        return BackendType(value)
      except ValueError:
        pass
      # Not a bundled member — is it a registered 3rd-party backend?
      # Imported lazily to avoid an import cycle at module-load time
      # (registry imports exceptions, which is fine, but settings is imported
      # extremely early — keep the registry import inside the validator).
      from scrapy_extension.backends.registry import get_registry

      if value in get_registry():
        return value
      valid = ", ".join(repr(k) for k in sorted(get_registry()))
      msg = (
        f"{value!r} is not a registered backend type. "
        f"Valid values: {valid}."
      )
      raise ConfigurationError(msg, setting_name="SCRAPY_BACKEND_TYPE")
    # Non-str, non-BackendType input (e.g. int) → ConfigurationError, NOT
    # pydantic ValidationError (consistent exception family).
    raise ConfigurationError(
      f"backend_type must be a string or BackendType, got {type(value).__name__}: "
      f"{value!r}",
      setting_name="SCRAPY_BACKEND_TYPE",
      setting_value=value,
    )
  serializer: Literal["json"] = Field(
    default="json",
    description="Serializer to use for data encoding",
  )
  retry_attempts: int = Field(
    default=3,
    ge=0,
    le=20,
    description=(
      "Number of connection retry attempts (0 = no retries; capped at 20 to "
      "prevent runaway retry storms on a misconfigured backend)."
    ),
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
      "1 MiB matches the Memcached 1 MB ceiling. Round-14 R14-C: this field "
      "is now read by ``BackendScheduler.from_settings`` (round-9 D2 left "
      "it orphaned — the constructor default was the only path)."
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
  queue_depth_sample_every: int = Field(
    default=100,
    ge=1,
    description=(
      "Round-14 R14-C: depth-probe sampling window for ``BackendQueue`` "
      "(round-9 U4). The pop-path depth RPC (e.g. ZCARD) fires at most once "
      "per ``N`` pops while the cached depth is non-zero, reclaiming ~25% "
      "of pop-path RTT. ``100`` (default) keeps depth-signal variance ~1%; "
      "``1`` restores per-pop probing (pre-U4 behavior). Emptiness is always "
      "fresh — sampling only amortizes the RPC while the queue is observably "
      "non-empty. Threaded by ``BackendScheduler.from_settings``."
    ),
  )
  queue_delay_max_held: int = Field(
    default=100_000,
    description=(
      "Round-14 R14-C: soft cap on the ``DelayQueueStrategy`` in-process "
      "holding heap (round-9 U5 — OOM prevention). When the heap exceeds "
      "this size a one-time WARNING fires (warn-only — items are NEVER "
      "refused, since dropping a delayed item would silently lose data). "
      "Default ``100_000``. Pass ``<= 0`` to disable the warning (advanced "
      "opt-out — accepts the unbounded-growth risk). Threaded by "
      "``BackendScheduler.from_settings`` → ``build_queue_strategy(max_held=…)``."
    ),
  )
  monitor_backpressure_threshold: int = Field(
    default=1_000,
    ge=0,
    description=(
      "Round-14 R14-C: depth above which ``queue/backpressure`` flips on "
      "(round-12 U2 operability). The default ``1_000`` makes the "
      "backpressure signal default-on without throttling (action is a later "
      "tier). Threaded by ``BackendScheduler.from_settings`` → "
      "``ScrapyStatsMonitor(backpressure_threshold=…)``."
    ),
  )
  monitor_pop_rate_window_s: float = Field(
    default=60.0,
    gt=0,
    description=(
      "Round-14 R14-C: rolling window (seconds) over which ``queue/pop_rate`` "
      "is computed (round-12 U2 operability). Default ``60.0`` matches the "
      "architect's 'calls/sec over a 1m window' contract. Threaded by "
      "``BackendScheduler.from_settings`` → ``BackendQueue(pop_rate_window_s=…)`` "
      "+ ``ScrapyStatsMonitor(pop_rate_window_s=…)``."
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
