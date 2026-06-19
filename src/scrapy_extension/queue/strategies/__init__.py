"""Queue-semantics strategies for pluggable task-queue types (subsystem ②).

Exports the strategy interface and concrete strategies. The
:class:`~scrapy_extension.queue.strategies.factory.QueueStrategyType` enum and
``build_queue_strategy`` factory live in the ``factory`` submodule.
"""

from __future__ import annotations

__all__ = [
  "DelayQueueStrategy",
  "PassthroughQueueStrategy",
  "QueueStrategy",
]

from scrapy_extension.queue.strategies.base import QueueStrategy
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy
