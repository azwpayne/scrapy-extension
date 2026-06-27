"""Tests for queue-semantics strategies + DelayQueue + factory (subsystem ②)."""

from __future__ import annotations

import pytest
from scrapy.http import Request

from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.base import QueueStrategy
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.factory import (
  QueueStrategyType,
  build_queue_strategy,
)
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy


def _clock(now: list[float]):
  """Return a clock callable backed by a mutable single-element list."""
  return lambda: now[0]


class TestPassthroughQueueStrategy:
  """Default strategy delegates straight through to the QueueBackend."""

  def test_push_delegates_with_priority(self, mock_connection_manager) -> None:
    strat = PassthroughQueueStrategy(mock_connection_manager)
    strat.push("q", b"x", priority=3.0)
    mock_connection_manager.get_queue_backend().push.assert_called_once_with(
      "q", b"x", 3.0
    )

  def test_push_ignores_delay(self, mock_connection_manager) -> None:
    strat = PassthroughQueueStrategy(mock_connection_manager)
    strat.push("q", b"x", delay=5.0)
    mock_connection_manager.get_queue_backend().push.assert_called_once_with(
      "q", b"x", 0.0
    )

  def test_pop_delegates(self, mock_connection_manager) -> None:
    mock_connection_manager.get_queue_backend().pop.return_value = b"y"
    strat = PassthroughQueueStrategy(mock_connection_manager)
    assert strat.pop("q", timeout=1.0) == b"y"
    mock_connection_manager.get_queue_backend().pop.assert_called_once_with("q", 1.0)

  def test_queue_len_delegates(self, mock_connection_manager) -> None:
    mock_connection_manager.get_queue_backend().queue_len.return_value = 7
    strat = PassthroughQueueStrategy(mock_connection_manager)
    assert strat.queue_len("q") == 7

  def test_clear_delegates(self, mock_connection_manager) -> None:
    strat = PassthroughQueueStrategy(mock_connection_manager)
    strat.clear("q")
    mock_connection_manager.get_queue_backend().clear_queue.assert_called_once_with("q")


class TestDelayQueueStrategy:
  """Holding + ready-drain semantics, clock-injected for determinism."""

  def test_no_delay_goes_straight_to_live(self, mock_connection_manager) -> None:
    now = [100.0]
    strat = DelayQueueStrategy(mock_connection_manager, clock=_clock(now))
    strat.push("q", b"x")
    mock_connection_manager.get_queue_backend().push.assert_called_once_with(
      "q", b"x", 0.0
    )
    assert len(strat._holding) == 0

  def test_delayed_item_held(self, mock_connection_manager) -> None:
    now = [100.0]
    strat = DelayQueueStrategy(mock_connection_manager, clock=_clock(now))
    strat.push("q", b"x", delay=10.0)
    mock_connection_manager.get_queue_backend().push.assert_not_called()
    assert len(strat._holding) == 1

  def test_delayed_item_not_poppable_before_ready(
    self, mock_connection_manager
  ) -> None:
    now = [100.0]
    strat = DelayQueueStrategy(mock_connection_manager, clock=_clock(now))
    strat.push("q", b"x", delay=10.0)  # ready_at = 110
    mock_connection_manager.get_queue_backend().pop.return_value = None
    now[0] = 109.0  # before ready
    assert strat.pop("q") is None
    assert len(strat._holding) == 1  # still held

  def test_delayed_item_drained_when_ready(self, mock_connection_manager) -> None:
    now = [100.0]
    strat = DelayQueueStrategy(mock_connection_manager, clock=_clock(now))
    strat.push("q", b"x", delay=10.0)  # ready_at = 110
    now[0] = 111.0  # past ready
    mock_connection_manager.get_queue_backend().pop.return_value = b"next"
    strat.pop("q")
    # R14-F: drained items are re-pushed with their priority (default 0.0).
    mock_connection_manager.get_queue_backend().push.assert_called_once_with("q", b"x", 0.0)
    assert len(strat._holding) == 0

  def test_default_delay_used_when_omitted(self, mock_connection_manager) -> None:
    now = [0.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=5.0, clock=_clock(now)
    )
    strat.push("q", b"x")  # no explicit delay -> default 5.0
    assert len(strat._holding) == 1

  def test_explicit_delay_zero_falls_back_to_default(
    self, mock_connection_manager
  ) -> None:
    """delay=0 is 'unspecified' and falls back to default_delay."""
    now = [0.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=5.0, clock=_clock(now)
    )
    strat.push("q", b"x", delay=0.0)
    assert len(strat._holding) == 1  # held via default_delay

  def test_len_includes_held(self, mock_connection_manager) -> None:
    now = [0.0]
    strat = DelayQueueStrategy(mock_connection_manager, clock=_clock(now))
    mock_connection_manager.get_queue_backend().queue_len.return_value = 3
    strat.push("q", b"a", delay=5.0)
    strat.push("q", b"b", delay=5.0)
    assert strat.queue_len("q") == 5  # 3 live + 2 held

  def test_clear_clears_held(self, mock_connection_manager) -> None:
    now = [0.0]
    strat = DelayQueueStrategy(mock_connection_manager, clock=_clock(now))
    strat.push("q", b"a", delay=5.0)
    strat.clear("q")
    assert len(strat._holding) == 0
    mock_connection_manager.get_queue_backend().clear_queue.assert_called_once_with("q")

  def test_invalid_default_delay_raises(self, mock_connection_manager) -> None:
    with pytest.raises(ValueError, match="default_delay"):
      DelayQueueStrategy(mock_connection_manager, default_delay=-1.0)

  def test_fifo_among_simultaneously_ready(self, mock_connection_manager) -> None:
    """Same ready_at drains in insertion order (seq tiebreak)."""
    now = [100.0]
    strat = DelayQueueStrategy(mock_connection_manager, clock=_clock(now))
    strat.push("q", b"first", delay=10.0)
    strat.push("q", b"second", delay=10.0)
    now[0] = 111.0
    strat.pop("q")
    pushes = [
      c.args[1]
      for c in mock_connection_manager.get_queue_backend().push.call_args_list
    ]
    assert pushes == [b"first", b"second"]


class TestQueueStrategyFactory:
  def test_passthrough(self, mock_connection_manager) -> None:
    strat = build_queue_strategy(QueueStrategyType.PASSTHROUGH, mock_connection_manager)
    assert isinstance(strat, PassthroughQueueStrategy)

  def test_delay(self, mock_connection_manager) -> None:
    strat = build_queue_strategy(
      QueueStrategyType.DELAY, mock_connection_manager, default_delay=2.0
    )
    assert isinstance(strat, DelayQueueStrategy)

  def test_every_type_returns_strategy(self, mock_connection_manager) -> None:
    for t in QueueStrategyType:
      assert isinstance(build_queue_strategy(t, mock_connection_manager), QueueStrategy)

  def test_invalid_strategy_string(self) -> None:
    with pytest.raises(ValueError, match="not a valid QueueStrategyType"):
      QueueStrategyType("bogus")


class TestBackendQueueWithDelayStrategy:
  """End-to-end: BackendQueue honors per-request + default delay."""

  def test_per_request_meta_delay_holds_then_drains(
    self, mock_connection_manager
  ) -> None:
    now = [0.0]
    strat = DelayQueueStrategy(mock_connection_manager, clock=_clock(now))
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      queue_strategy=strat,
    )
    queue.push(Request(url="https://example.com/a", meta={"delay": 10.0}))
    # held — not yet pushed to the live backend
    assert mock_connection_manager.get_queue_backend().push.call_count == 0

    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue.pop()  # before ready
    assert mock_connection_manager.get_queue_backend().push.call_count == 0

    now[0] = 11.0
    queue.pop()  # past ready -> drained to live
    assert mock_connection_manager.get_queue_backend().push.call_count == 1

  def test_default_strategy_is_passthrough(self, mock_connection_manager) -> None:
    """BackendQueue without an explicit strategy uses Passthrough (back-comat)."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager, queue_name="q"
    )
    assert isinstance(queue._strategy, PassthroughQueueStrategy)
