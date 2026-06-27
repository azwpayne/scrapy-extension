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
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy
from scrapy_extension.queue.strategies.throttle import ThrottleQueueStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


class QueueStrategyType(str, Enum):
  """Selectable queue strategies.

  Attributes:
      PASSTHROUGH: Default — delegates to QueueBackend unchanged.
      DELAY: Holds items until ready, then moves to the live queue.
  """

  PASSTHROUGH = "passthrough"
  DELAY = "delay"
  ROUND_ROBIN = "round_robin"
  THROTTLE = "throttle"

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
) -> QueueStrategy:
  """Build the queue strategy for ``strategy_type``.

  Args:
      strategy_type: Which queue strategy to instantiate.
      connection_manager: Connection manager for backend access.
      default_delay: Default delay seconds for the ``delay`` strategy.
      min_interval: Min seconds between pops for the ``throttle`` strategy.
      max_held: Round-14 R14-C — soft cap on the ``delay`` strategy's
          in-process holding heap (round-9 U5). When ``None`` (default) the
          ``DelayQueueStrategy`` constructor default applies (``100_000``);
          otherwise the value is forwarded verbatim. Ignored for non-delay
          strategies. Non-positive disables the over-cap warning (advanced
          opt-out — accepts unbounded-growth risk).

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
  raise ConfigurationError(f"Unknown queue strategy: {strategy_type!r}")  # pragma: no cover
