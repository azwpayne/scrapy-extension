"""Queue-semantics strategies for pluggable task-queue types (subsystem ②).

Exports the strategy interface and concrete strategies. The
:class:`~scrapy_extension.queue.strategies.factory.QueueStrategyType` enum and
``build_queue_strategy`` factory live in the ``factory`` submodule.
"""

from __future__ import annotations

__all__ = [
  "DelayQueueStrategy",
  "PassthroughQueueStrategy",
  "PriorityQueueStrategy",
  "QueueStrategy",
  "RingBufferQueueStrategy",
  "RoundRobinQueueStrategy",
  "ThrottleQueueStrategy",
  "TimeWheelQueueStrategy",
  "WorkStealingQueueStrategy",
]

from scrapy_extension.queue.strategies.base import QueueStrategy
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy
from scrapy_extension.queue.strategies.priority import PriorityQueueStrategy
from scrapy_extension.queue.strategies.ring_buffer import RingBufferQueueStrategy
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy
from scrapy_extension.queue.strategies.throttle import ThrottleQueueStrategy
from scrapy_extension.queue.strategies.time_wheel import TimeWheelQueueStrategy
from scrapy_extension.queue.strategies.work_stealing import WorkStealingQueueStrategy
