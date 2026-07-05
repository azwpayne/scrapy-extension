"""Factory selecting a queue strategy from settings (subsystem ②).

Maps a :class:`QueueStrategyType` to a concrete :class:`QueueStrategy`. Used
by ``BackendScheduler.from_settings`` so the queueing semantics are chosen
via ``SCRAPY_QUEUE_STRATEGY`` with no code change.
"""

from __future__ import annotations

__all__ = ["QueueStrategyType", "build_queue_strategy"]

from enum import Enum
from typing import TYPE_CHECKING

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.queue.strategies.base import QueueStrategy
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy
from scrapy_extension.queue.strategies.priority import PriorityQueueStrategy
from scrapy_extension.queue.strategies.ring_buffer import RingBufferQueueStrategy
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy
from scrapy_extension.queue.strategies.throttle import ThrottleQueueStrategy
from scrapy_extension.queue.strategies.time_wheel import TimeWheelQueueStrategy
from scrapy_extension.queue.strategies.work_stealing import WorkStealingQueueStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


class QueueStrategyType(str, Enum):
  """Selectable queue strategies.

  Attributes:
      PASSTHROUGH: Default — delegates to QueueBackend unchanged.
      DELAY: Holds items until ready, then moves to the live queue.
      ROUND_ROBIN: Fairness across ``source`` tags.
      THROTTLE: Rate-limited pop (min seconds between pops).
      PRIORITY: N-level physical bucket priority — strategy-layer priority
          that works on backends without native priority (SQS Standard, Kafka).
      TIME_WHEEL: O(1) hashed timing wheel for many short delays; overflow
          heap for long delays. Faster than DELAY's heap on big short-delay
          workloads.
      WORK_STEALING: Pop-side load balancing — own queue first, steal from
          peer queues when idle.
      RING_BUFFER: Bounded in-process circular buffer with explicit overflow
          policy (reject / drop_oldest / block).
  """

  PASSTHROUGH = "passthrough"
  DELAY = "delay"
  ROUND_ROBIN = "round_robin"
  THROTTLE = "throttle"
  PRIORITY = "priority"
  TIME_WHEEL = "time_wheel"
  WORK_STEALING = "work_stealing"
  RING_BUFFER = "ring_buffer"

  @classmethod
  def _missing_(cls, value: object) -> QueueStrategyType:
    valid = ", ".join(repr(m.value) for m in cls)
    raise ValueError(f"{value!r} is not a valid {cls.__name__}. Valid: {valid}.")


def build_queue_strategy(
  strategy_type: QueueStrategyType,
  connection_manager: ConnectionManager,
  *,
  default_delay: float = 0.0,
  min_interval: float = 0.0,
  max_held: int | None = None,
  priority_levels: int = 3,
  wheel_size: int = 60,
  ticks_per_second: float = 1.0,
  worker_id: str | None = None,
  peer_ids: tuple[str, ...] = (),
  steal_timeout: float = 0.05,
  capacity: int = 1024,
  full_policy: str = "reject",
) -> QueueStrategy:
  """Build the queue strategy for ``strategy_type``.

  Args:
      strategy_type: Which queue strategy to instantiate.
      connection_manager: Connection manager for backend access.
      default_delay: Default delay seconds for the ``delay`` and ``time_wheel``
          strategies.
      min_interval: Min seconds between pops for the ``throttle`` strategy.
      max_held: Soft cap on the ``delay`` strategy's holding heap. ``None`` →
          constructor default (``100_000``); non-positive disables the warning.
      priority_levels: Discrete priority-bucket count for the ``priority``
          strategy (default 3).
      wheel_size: Slot count for the ``time_wheel`` strategy (default 60).
      ticks_per_second: Slot granularity for ``time_wheel`` (default 1.0).
      worker_id: Own worker ID for ``work_stealing`` (``None`` → auto UUID).
      peer_ids: Peer worker IDs to steal from for ``work_stealing``.
      steal_timeout: Per-peer pop timeout for ``work_stealing`` (default 0.05s).
      capacity: Slot count for ``ring_buffer`` (default 1024).
      full_policy: Overflow policy for ``ring_buffer`` — ``reject`` (default),
          ``drop_oldest``, or ``block``.

  Returns:
      A concrete QueueStrategy instance.

  Raises:
      ConfigurationError: If ``strategy_type`` is not a known QueueStrategyType.
  """
  if strategy_type is QueueStrategyType.PASSTHROUGH:
    return PassthroughQueueStrategy(connection_manager)
  if strategy_type is QueueStrategyType.DELAY:
    if max_held is None:
      return DelayQueueStrategy(connection_manager, default_delay=default_delay)
    return DelayQueueStrategy(
      connection_manager,
      default_delay=default_delay,
      max_held=max_held,
    )
  if strategy_type is QueueStrategyType.ROUND_ROBIN:
    return RoundRobinQueueStrategy(connection_manager)
  if strategy_type is QueueStrategyType.THROTTLE:
    return ThrottleQueueStrategy(connection_manager, min_interval=min_interval)
  if strategy_type is QueueStrategyType.PRIORITY:
    return PriorityQueueStrategy(connection_manager, levels=priority_levels)
  if strategy_type is QueueStrategyType.TIME_WHEEL:
    return TimeWheelQueueStrategy(
      connection_manager,
      wheel_size=wheel_size,
      ticks_per_second=ticks_per_second,
      default_delay=default_delay,
    )
  if strategy_type is QueueStrategyType.WORK_STEALING:
    return WorkStealingQueueStrategy(
      connection_manager,
      worker_id=worker_id,
      peer_ids=peer_ids,
      steal_timeout=steal_timeout,
    )
  if strategy_type is QueueStrategyType.RING_BUFFER:
    return RingBufferQueueStrategy(
      connection_manager,
      capacity=capacity,
      full_policy=full_policy,  # type: ignore[arg-type]
    )
  raise ConfigurationError(f"Unknown queue strategy: {strategy_type!r}")  # pragma: no cover
