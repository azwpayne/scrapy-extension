"""Cross-strategy queue-name and timeout contracts."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.base import QueueStrategy
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy
from scrapy_extension.queue.strategies.priority import PriorityQueueStrategy
from scrapy_extension.queue.strategies.ring_buffer import RingBufferQueueStrategy
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy
from scrapy_extension.queue.strategies.throttle import ThrottleQueueStrategy
from scrapy_extension.queue.strategies.time_wheel import TimeWheelQueueStrategy
from scrapy_extension.queue.strategies.work_stealing import WorkStealingQueueStrategy

StrategyFactory = Callable[[MagicMock], QueueStrategy]

_STRATEGIES: tuple[tuple[str, StrategyFactory], ...] = (
  ("passthrough", PassthroughQueueStrategy),
  ("delay", DelayQueueStrategy),
  ("round_robin", RoundRobinQueueStrategy),
  ("throttle", ThrottleQueueStrategy),
  ("priority", PriorityQueueStrategy),
  ("time_wheel", TimeWheelQueueStrategy),
  (
    "work_stealing",
    lambda manager: WorkStealingQueueStrategy(manager, worker_id="worker-a"),
  ),
  ("ring_buffer", RingBufferQueueStrategy),
)

_INVALID_TIMEOUTS = (True, -1.0, float("nan"), float("inf"), float("-inf"))


@pytest.mark.parametrize(
  ("_name", "factory"), _STRATEGIES, ids=[x[0] for x in _STRATEGIES]
)
@pytest.mark.parametrize("timeout", _INVALID_TIMEOUTS)
def test_pop_rejects_invalid_timeout_before_backend_access(
  _name: str,
  factory: StrategyFactory,
  timeout: float,
) -> None:
  manager = MagicMock(name="ConnectionManager")
  strategy = factory(manager)
  manager.reset_mock()

  with pytest.raises(ValueError, match="timeout must be a finite non-negative"):
    strategy.pop("jobs", timeout=timeout)

  manager.get_queue_backend.assert_not_called()


@pytest.mark.parametrize(
  ("_name", "factory"), _STRATEGIES, ids=[x[0] for x in _STRATEGIES]
)
@pytest.mark.parametrize("timeout", _INVALID_TIMEOUTS)
def test_pop_with_ack_rejects_invalid_timeout_before_backend_access(
  _name: str,
  factory: StrategyFactory,
  timeout: float,
) -> None:
  manager = MagicMock(name="ConnectionManager")
  strategy = factory(manager)
  manager.reset_mock()

  with pytest.raises(ValueError, match="timeout must be a finite non-negative"):
    strategy.pop_with_ack("jobs", timeout=timeout)

  manager.get_queue_backend.assert_not_called()


def test_backend_queue_normalizes_invalid_timeout_to_queue_error() -> None:
  manager = MagicMock(name="ConnectionManager")
  manager.get_storage_backend.side_effect = NotImplementedError
  queue = BackendQueue(manager, "jobs")
  manager.reset_mock()

  with pytest.raises(QueueError, match="timeout must be a finite non-negative"):
    queue.pop(timeout=float("nan"))

  manager.get_queue_backend.assert_not_called()


@pytest.mark.parametrize(
  "factory",
  (
    lambda manager: DelayQueueStrategy(manager, default_delay=10.0),
    lambda manager: TimeWheelQueueStrategy(manager, default_delay=10.0),
    RoundRobinQueueStrategy,
    RingBufferQueueStrategy,
  ),
  ids=("delay", "time_wheel", "round_robin", "ring_buffer"),
)
def test_in_process_strategy_rejects_cross_queue_delivery(
  factory: StrategyFactory,
) -> None:
  manager = MagicMock(name="ConnectionManager")
  manager.get_queue_backend.return_value.queue_len.return_value = 0
  strategy = factory(manager)
  strategy.push("queue-a", b"owned-by-a")
  manager.reset_mock()

  with pytest.raises(ValueError, match="already bound to logical queue"):
    strategy.pop("queue-b")

  manager.get_queue_backend.assert_not_called()
  assert strategy.queue_len("queue-a") == 1


def test_backend_queue_binds_strategy_before_snapshot_restore() -> None:
  manager = MagicMock(name="ConnectionManager")
  manager.get_storage_backend.side_effect = NotImplementedError
  strategy = RingBufferQueueStrategy(manager)
  BackendQueue(manager, "queue-a", queue_strategy=strategy)
  manager.reset_mock()

  with pytest.raises(ValueError, match="already bound to logical queue"):
    BackendQueue(manager, "queue-b", queue_strategy=strategy)

  manager.get_storage_backend.assert_not_called()
