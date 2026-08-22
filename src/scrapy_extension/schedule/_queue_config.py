"""Validated queue-component configuration passed to the queue/strategy seams.

Extracted from ``scheduler.py`` (pure move). This module must not log:
caplog/logger tests pin the ``scrapy_extension.schedule.scheduler``
logger name; if logging is ever needed here, reuse that historical name."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from scrapy_extension.backends.base import _validate_key_name
from scrapy_extension.exceptions import (
    ConfigurationError,
)
from scrapy_extension.queue.snapshot import (
    DEFAULT_SNAPSHOT_CHUNK_BYTES,
    DEFAULT_SNAPSHOT_MAX_BYTES,
    MAX_SNAPSHOT_CHUNK_BYTES,
    MAX_SNAPSHOT_CHUNKS,
)
from scrapy_extension.queue.strategies._names import (
    CURRENT_FANOUT_NAME_GENERATION,
    LEGACY_FANOUT_NAME_GENERATION,
)
from scrapy_extension.utils._config import (
    get_bool_setting,
    parse_float_setting,
    parse_int_setting,
)
from scrapy_extension.utils.identity import (
    DEFAULT_QUEUE_KEY_TEMPLATE,
    project_name_from_settings,
    resolve_identity_template,
)
from scrapy_extension.utils.reactor import (
    DEFAULT_REACTOR_IO_TIMEOUT_S,
    MAX_REACTOR_IO_TIMEOUT_S,
)

if TYPE_CHECKING:
    from scrapy.settings import Settings


@dataclass(frozen=True, slots=True)
class _QueueComponentConfig:
    """Validated immutable values passed to the queue and strategy seams."""

    strategy_value: Any
    ring_buffer_full_policy: str
    queue_key_template: str | None = None
    queue_key: str | None = None
    project_name: str | None = None
    allow_cross_spider: bool = False
    strategy_type: Any | None = None
    default_delay: float | None = None
    min_interval: float | None = None
    delay_max_held: int | None = None
    priority_levels: int | None = None
    wheel_size: int | None = None
    ticks_per_second: float | None = None
    steal_timeout: float | None = None
    capacity: int | None = None
    worker_id: str | None = None
    peer_ids: tuple[str, ...] = ()
    backpressure_pause_at: int | None = None
    backpressure_resume_at: int | None = None
    queue_depth_sample_every: int | None = None
    queue_max_item_bytes: int | None = None
    monitor_backpressure_threshold: int | None = None
    monitor_pop_rate_window_s: float | None = None
    queue_snapshot_owner: str | None = None
    queue_snapshot_max_bytes: int | None = None
    queue_snapshot_chunk_bytes: int | None = None
    reactor_io_timeout: float | None = None
    name_generation: str = CURRENT_FANOUT_NAME_GENERATION

    @classmethod
    def from_early_settings(
        cls,
        settings: Settings,
    ) -> _QueueComponentConfig:
        """Capture the strategy selector and validate the ring-buffer policy."""
        from scrapy_extension.queue.strategies.factory import QueueStrategyType

        strategy_value = settings.get(
            "SCRAPY_QUEUE_STRATEGY",
            QueueStrategyType.PASSTHROUGH.value,
        )
        ring_buffer_full_policy = settings.get(
            "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY",
            "reject",
        )
        if ring_buffer_full_policy not in ("reject", "drop_oldest", "block"):
            raise ConfigurationError(
                "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY must be one of "
                "'reject', 'drop_oldest', or 'block'.",
                setting_name="SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY",
                setting_value=ring_buffer_full_policy,
            )
        if (
            strategy_value == QueueStrategyType.RING_BUFFER.value
            and ring_buffer_full_policy == "block"
        ):
            raise ConfigurationError(
                "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY='block' is unsafe with "
                "BackendScheduler: enqueue_request runs on Scrapy's reactor thread, "
                "so a full ring buffer would block the same thread that must drain it. "
                "Use 'reject' or 'drop_oldest'.",
                setting_name="SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY",
                setting_value=ring_buffer_full_policy,
            )
        return cls(
            strategy_value=strategy_value,
            ring_buffer_full_policy=ring_buffer_full_policy,
        )

    def with_queue_key(
        self,
        settings: Settings,
        *,
        spider_name: str | None,
        project_name: str | None = None,
        queue_key_override: str | None = None,
    ) -> _QueueComponentConfig:
        """Validate and resolve the queue key at the original factory checkpoint.

        ``queue_key_override`` is used by composite owners such as
        :class:`BackendSpiderMixin`. It keeps validation and identity-placeholder
        handling in this one factory while allowing explicit legacy key overrides.
        """
        queue_key = (
            settings.get("SCRAPY_QUEUE_KEY", DEFAULT_QUEUE_KEY_TEMPLATE)
            if queue_key_override is None
            else queue_key_override
        )
        if not isinstance(queue_key, str):
            raise ConfigurationError(
                f"SCRAPY_QUEUE_KEY must be a string, got {queue_key!r}.",
                setting_name="SCRAPY_QUEUE_KEY",
                setting_value=queue_key,
            )
        if spider_name is not None:
            try:
                _validate_key_name(spider_name, "spider.name")
            except ValueError as exc:
                raise ConfigurationError(
                    str(exc),
                    setting_name="spider.name",
                    setting_value=spider_name,
                ) from exc
        resolved_project_name = project_name or project_name_from_settings(settings)
        resolved_queue_key = resolve_identity_template(
            queue_key,
            spider_name=spider_name,
            project_name=resolved_project_name,
        )
        try:
            _validate_key_name(
                resolved_queue_key.replace("{spider}", "spider").replace(
                    "{project}", "project"
                ),
                "SCRAPY_QUEUE_KEY",
            )
        except ValueError as exc:
            raise ConfigurationError(
                str(exc),
                setting_name="SCRAPY_QUEUE_KEY",
                setting_value=queue_key,
            ) from exc
        return replace(
            self,
            queue_key_template=queue_key,
            queue_key=resolved_queue_key,
            project_name=resolved_project_name,
            allow_cross_spider=get_bool_setting(
                settings,
                "SCRAPY_QUEUE_ALLOW_CROSS_SPIDER",
            ),
        )

    def with_strategy_settings(self, settings: Settings) -> _QueueComponentConfig:
        """Parse strategy construction settings after the ack-concurrency gate."""
        from scrapy_extension.queue.strategies.factory import QueueStrategyType
        from scrapy_extension.queue.strategies.priority import MAX_PRIORITY_LEVELS
        from scrapy_extension.queue.strategies.throttle import (
            THROTTLE_MAX_MIN_INTERVAL_S,
        )
        from scrapy_extension.queue.strategies.time_wheel import MAX_WHEEL_SIZE

        try:
            strategy_type = QueueStrategyType(self.strategy_value)
        except ValueError as exc:
            valid = ", ".join(repr(member.value) for member in QueueStrategyType)
            raise ConfigurationError(
                f"Invalid SCRAPY_QUEUE_STRATEGY {self.strategy_value!r}. Valid: {valid}.",
                setting_name="SCRAPY_QUEUE_STRATEGY",
                setting_value=str(self.strategy_value),
            ) from exc
        default_delay = parse_float_setting(
            settings.get("SCRAPY_QUEUE_DELAY_DEFAULT", 0.0),
            "SCRAPY_QUEUE_DELAY_DEFAULT",
            minimum=0.0,
        )
        min_interval = parse_float_setting(
            settings.get("SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL", 0.0),
            "SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL",
            minimum=0.0,
            maximum=THROTTLE_MAX_MIN_INTERVAL_S,
        )
        delay_max_held_raw = settings.get("SCRAPY_QUEUE_DELAY_MAX_HELD")
        delay_max_held = (
            parse_int_setting(delay_max_held_raw, "SCRAPY_QUEUE_DELAY_MAX_HELD")
            if delay_max_held_raw is not None
            else None
        )
        priority_levels = parse_int_setting(
            settings.get("SCRAPY_QUEUE_PRIORITY_LEVELS", 3),
            "SCRAPY_QUEUE_PRIORITY_LEVELS",
            minimum=1,
            maximum=MAX_PRIORITY_LEVELS,
        )
        wheel_size = parse_int_setting(
            settings.get("SCRAPY_QUEUE_TIME_WHEEL_SIZE", 60),
            "SCRAPY_QUEUE_TIME_WHEEL_SIZE",
            minimum=1,
            maximum=MAX_WHEEL_SIZE,
        )
        ticks_per_second = parse_float_setting(
            settings.get("SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND", 1.0),
            "SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND",
            minimum=0.0,
            minimum_exclusive=True,
        )
        steal_timeout = parse_float_setting(
            settings.get("SCRAPY_QUEUE_STEAL_TIMEOUT", 0.05),
            "SCRAPY_QUEUE_STEAL_TIMEOUT",
            minimum=0.0,
        )
        capacity = parse_int_setting(
            settings.get("SCRAPY_QUEUE_RING_BUFFER_CAPACITY", 1024),
            "SCRAPY_QUEUE_RING_BUFFER_CAPACITY",
            minimum=1,
        )
        worker_id_raw = settings.get("SCRAPY_QUEUE_WORKER_ID")
        if worker_id_raw is None:
            worker_id = None
        elif not isinstance(worker_id_raw, str) or not worker_id_raw.strip():
            raise ConfigurationError(
                "SCRAPY_QUEUE_WORKER_ID must be a non-empty string or unset.",
                setting_name="SCRAPY_QUEUE_WORKER_ID",
                setting_value=worker_id_raw,
            )
        else:
            worker_id = worker_id_raw.strip()
        peer_ids_raw = settings.get("SCRAPY_QUEUE_PEER_IDS")
        peer_id_values: list[Any] | tuple[Any, ...]
        if peer_ids_raw is None:
            peer_id_values = ()
        elif isinstance(peer_ids_raw, str):
            peer_id_values = peer_ids_raw.split(",")
        elif isinstance(peer_ids_raw, (list, tuple)):
            peer_id_values = peer_ids_raw
        else:
            raise ConfigurationError(
                "SCRAPY_QUEUE_PEER_IDS must be a comma-separated string, list, or tuple.",
                setting_name="SCRAPY_QUEUE_PEER_IDS",
                setting_value=peer_ids_raw,
            )
        if any(not isinstance(peer_id, str) for peer_id in peer_id_values):
            raise ConfigurationError(
                "SCRAPY_QUEUE_PEER_IDS entries must all be strings.",
                setting_name="SCRAPY_QUEUE_PEER_IDS",
                setting_value=peer_ids_raw,
            )
        peer_ids = tuple(
            peer_id.strip() for peer_id in peer_id_values if peer_id.strip()
        )
        # Fan-out strategies (priority/work_stealing) default to the versioned
        # v2 physical names. legacy_v1 (old colon-delimited names) is a
        # migration-only, quiescent drain mode and must be selected explicitly;
        # there is no dual-read fallback. Keep the rejection message static so it
        # can be matched without echoing operator input (ring-buffer precedent).
        name_generation = settings.get(
            "SCRAPY_QUEUE_NAME_GENERATION",
            CURRENT_FANOUT_NAME_GENERATION,
        )
        if not isinstance(name_generation, str) or name_generation not in {
            CURRENT_FANOUT_NAME_GENERATION,
            LEGACY_FANOUT_NAME_GENERATION,
        }:
            raise ConfigurationError(
                "SCRAPY_QUEUE_NAME_GENERATION must be one of 'v2' or 'legacy_v1'.",
                setting_name="SCRAPY_QUEUE_NAME_GENERATION",
                setting_value=name_generation,
            )
        return replace(
            self,
            strategy_type=strategy_type,
            default_delay=default_delay,
            min_interval=min_interval,
            delay_max_held=delay_max_held,
            priority_levels=priority_levels,
            wheel_size=wheel_size,
            ticks_per_second=ticks_per_second,
            steal_timeout=steal_timeout,
            capacity=capacity,
            worker_id=worker_id,
            peer_ids=peer_ids,
            name_generation=name_generation,
        )

    def with_runtime_settings(self, settings: Settings) -> _QueueComponentConfig:
        """Parse queue and monitor settings after strategy diagnostics."""
        pause_raw = settings.get("SCRAPY_BACKPRESSURE_PAUSE_AT")
        resume_raw = settings.get("SCRAPY_BACKPRESSURE_RESUME_AT")
        pause_at = (
            parse_int_setting(
                pause_raw,
                "SCRAPY_BACKPRESSURE_PAUSE_AT",
                minimum=0,
            )
            if pause_raw is not None
            else None
        )
        resume_at = (
            parse_int_setting(
                resume_raw,
                "SCRAPY_BACKPRESSURE_RESUME_AT",
                minimum=0,
            )
            if resume_raw is not None
            else None
        )
        if pause_at is not None and resume_at is not None and resume_at > pause_at:
            raise ConfigurationError(
                "SCRAPY_BACKPRESSURE_RESUME_AT must be <= "
                "SCRAPY_BACKPRESSURE_PAUSE_AT.",
                setting_name="SCRAPY_BACKPRESSURE_RESUME_AT",
                setting_value=resume_raw,
            )
        queue_depth_sample_every = parse_int_setting(
            settings.get("SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY", 100),
            "SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY",
            minimum=1,
        )
        queue_max_item_bytes = parse_int_setting(
            settings.get("SCRAPY_QUEUE_MAX_ITEM_BYTES", 1_048_576),
            "SCRAPY_QUEUE_MAX_ITEM_BYTES",
            minimum=1,
        )
        monitor_backpressure_threshold = parse_int_setting(
            settings.get("SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD", 1_000),
            "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD",
            minimum=0,
        )
        monitor_pop_rate_window_s = parse_float_setting(
            settings.get("SCRAPY_MONITOR_POP_RATE_WINDOW_S", 60.0),
            "SCRAPY_MONITOR_POP_RATE_WINDOW_S",
            minimum=0.0,
            minimum_exclusive=True,
            maximum=86400.0,
        )
        queue_snapshot_max_bytes = parse_int_setting(
            settings.get("SCRAPY_QUEUE_SNAPSHOT_MAX_BYTES", DEFAULT_SNAPSHOT_MAX_BYTES),
            "SCRAPY_QUEUE_SNAPSHOT_MAX_BYTES",
            minimum=1,
        )
        queue_snapshot_chunk_bytes = parse_int_setting(
            settings.get(
                "SCRAPY_QUEUE_SNAPSHOT_CHUNK_BYTES", DEFAULT_SNAPSHOT_CHUNK_BYTES
            ),
            "SCRAPY_QUEUE_SNAPSHOT_CHUNK_BYTES",
            minimum=max(
                1,
                (queue_snapshot_max_bytes + MAX_SNAPSHOT_CHUNKS - 1)
                // MAX_SNAPSHOT_CHUNKS,
            ),
            maximum=min(queue_snapshot_max_bytes, MAX_SNAPSHOT_CHUNK_BYTES),
        )
        reactor_io_timeout = parse_float_setting(
            settings.get("SCRAPY_REACTOR_IO_TIMEOUT", DEFAULT_REACTOR_IO_TIMEOUT_S),
            "SCRAPY_REACTOR_IO_TIMEOUT",
            minimum=0.0,
            minimum_exclusive=True,
            maximum=MAX_REACTOR_IO_TIMEOUT_S,
        )
        snapshot_owner_raw = settings.get("SCRAPY_QUEUE_SNAPSHOT_OWNER")
        queue_snapshot_owner = (
            snapshot_owner_raw if snapshot_owner_raw is not None else self.worker_id
        )
        if queue_snapshot_owner is not None:
            if not isinstance(queue_snapshot_owner, str):
                raise ConfigurationError(
                    "SCRAPY_QUEUE_SNAPSHOT_OWNER must be a non-empty string or unset.",
                    setting_name="SCRAPY_QUEUE_SNAPSHOT_OWNER",
                    setting_value=snapshot_owner_raw,
                )
            # When SNAPSHOT_OWNER is unset and defaulted from worker_id, attribute a
            # key-name failure to SCRAPY_QUEUE_WORKER_ID (the setting the operator
            # configured), not the unset SCRAPY_QUEUE_SNAPSHOT_OWNER. worker_id is
            # parsed with .strip() only and delay/time_wheel ignore it, so a
            # key-unsafe value can survive to here.
            owner_setting = (
                "SCRAPY_QUEUE_SNAPSHOT_OWNER"
                if snapshot_owner_raw is not None
                else "SCRAPY_QUEUE_WORKER_ID"
            )
            try:
                _validate_key_name(queue_snapshot_owner, owner_setting)
            except ValueError as exc:
                raise ConfigurationError(
                    str(exc),
                    setting_name=owner_setting,
                    setting_value=queue_snapshot_owner,
                ) from exc
        return replace(
            self,
            backpressure_pause_at=pause_at,
            backpressure_resume_at=resume_at,
            queue_depth_sample_every=queue_depth_sample_every,
            queue_max_item_bytes=queue_max_item_bytes,
            monitor_backpressure_threshold=monitor_backpressure_threshold,
            monitor_pop_rate_window_s=monitor_pop_rate_window_s,
            queue_snapshot_owner=queue_snapshot_owner,
            queue_snapshot_max_bytes=queue_snapshot_max_bytes,
            queue_snapshot_chunk_bytes=queue_snapshot_chunk_bytes,
            reactor_io_timeout=reactor_io_timeout,
        )
