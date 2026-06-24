"""Tests for DelayQueueStrategy (subsystem ②)."""

from __future__ import annotations

import logging

import pytest

from scrapy_extension.queue.strategies.delay import DelayQueueStrategy


def _clock(now: list[float]):
  """Return a clock callable backed by a mutable single-element list."""
  return lambda: now[0]


class TestDelayQueueStrategy:
  def test_push_holds_until_ready(self, mock_connection_manager) -> None:
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, clock=_clock(now)
    )
    strat.push("q", b"x")
    # Held — not yet in the live queue.
    assert len(strat._holding) == 1
    mock_connection_manager.get_queue_backend().push.assert_not_called()

  def test_close_with_held_items_warns_and_clears(
    self, mock_connection_manager, caplog
  ) -> None:
    """close() must emit a WARNING naming the held-item count, then clear."""
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, clock=_clock(now)
    )
    strat.push("q", b"a")
    strat.push("q", b"b")
    strat.push("q", b"c")
    assert len(strat._holding) == 3

    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      strat.close()

    assert len(strat._holding) == 0
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "DelayQueueStrategy" in msg
    assert "3" in msg  # held-item count

  def test_close_empty_is_quiet(self, mock_connection_manager, caplog) -> None:
    """close() with an empty holding list must emit NO warning."""
    strat = DelayQueueStrategy(mock_connection_manager)
    assert len(strat._holding) == 0

    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      strat.close()

    assert len(strat._holding) == 0
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []

  @pytest.mark.parametrize("default_delay", [-0.01, -1.0])
  def test_invalid_delay_raises(self, mock_connection_manager, default_delay: float) -> None:
    with pytest.raises(ValueError, match="default_delay"):
      DelayQueueStrategy(mock_connection_manager, default_delay=default_delay)
