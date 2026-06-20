"""Tests for RoundRobinQueueStrategy (subsystem ②)."""

from __future__ import annotations

import pytest

from scrapy_extension.queue.strategies.factory import (
  QueueStrategyType,
  build_queue_strategy,
)
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy


class TestRoundRobinQueueStrategy:
  def test_single_source_is_fifo(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a", source="x")
    s.push("q", b"b", source="x")
    assert s.pop("q") == b"a"
    assert s.pop("q") == b"b"
    assert s.pop("q") is None

  def test_round_robin_across_two_sources(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a1", source="A")
    s.push("q", b"a2", source="A")
    s.push("q", b"b1", source="B")
    s.push("q", b"b2", source="B")
    results = [s.pop("q") for _ in range(4)]
    # Fair interleaving: A, B, A, B (neither source served twice before the other)
    assert results == [b"a1", b"b1", b"a2", b"b2"]

  def test_no_source_starvation(self, mock_connection_manager) -> None:
    """A source with many items must not starve another source."""
    s = RoundRobinQueueStrategy(mock_connection_manager)
    for i in range(5):
      s.push("q", f"a{i}".encode(), source="A")
    s.push("q", b"b0", source="B")
    assert s.pop("q") == b"a0"
    assert s.pop("q") == b"b0"  # B served on the second pop despite A having 5

  def test_returns_none_when_all_empty(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a", source="A")
    s.pop("q")  # drains A
    assert s.pop("q") is None

  def test_default_source_when_unspecified(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"x")  # no source -> "default"
    assert s.pop("q") == b"x"

  def test_len_totals_all_sources(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a", source="A")
    s.push("q", b"b", source="A")
    s.push("q", b"c", source="B")
    assert s.queue_len("q") == 3

  def test_clear_empties_all_sources(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a", source="A")
    s.push("q", b"b", source="B")
    s.clear("q")
    assert s.queue_len("q") == 0
    assert s.pop("q") is None

  def test_reuses_source_after_drain(self, mock_connection_manager) -> None:
    """A source that drained can receive and serve new items."""
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a1", source="A")
    s.pop("q")  # A now empty
    s.push("q", b"a2", source="A")  # reuse same source
    assert s.pop("q") == b"a2"


class TestFactoryRoundRobin:
  def test_build_round_robin(self, mock_connection_manager) -> None:
    s = build_queue_strategy(QueueStrategyType.ROUND_ROBIN, mock_connection_manager)
    assert isinstance(s, RoundRobinQueueStrategy)

  def test_invalid_strategy_string(self) -> None:
    with pytest.raises(ValueError, match="not a valid QueueStrategyType"):
      QueueStrategyType("bogus")
