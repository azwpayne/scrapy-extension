"""Tests for DelayQueueStrategy (subsystem ②)."""

from __future__ import annotations

import logging

import pytest

from scrapy_extension.queue.strategies.delay import (
  DEFAULT_DELAY_MAX_HELD,
  DelayQueueStrategy,
)


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

  def test_default_max_held_threshold(self, mock_connection_manager) -> None:
    """Constructor ships a 100k default soft cap on the holding heap (SPEC U5)."""
    strat = DelayQueueStrategy(mock_connection_manager)
    assert strat._max_held == DEFAULT_DELAY_MAX_HELD == 100_000

  def test_soft_cap_warns_once_when_exceeded(
    self, mock_connection_manager, caplog
  ) -> None:
    """Holding >max_held items fires ONE warning; further pushes stay quiet."""
    import scrapy_extension.queue.strategies.delay as mod
    mod._over_cap_warned = False  # reset module-level flag for a clean slate
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, max_held=3, clock=_clock(now)
    )
    # Fill up to the cap — no warning yet.
    strat.push("q", b"a")
    strat.push("q", b"b")
    strat.push("q", b"c")
    assert len(strat._holding) == 3

    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      strat.push("q", b"d")  # exceeds cap → warn
      strat.push("q", b"e")  # still over → no second warning (warn-once)
      strat.push("q", b"f")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    # Exactly one warning (the soft-cap one); close() would add another but is not called here.
    cap_warnings = [
      w for w in warnings if "max_held" in w.getMessage() or "delay" in w.getMessage().lower()
    ]
    assert len(cap_warnings) == 1
    msg = cap_warnings[0].getMessage()
    # Warn points at the unbounded-growth risk + distributed-delay roadmap (U10).
    assert "max_held" in msg or "holding" in msg

  def test_soft_cap_does_not_block_push(
    self, mock_connection_manager, caplog
  ) -> None:
    """The cap is a SOFT cap (warn-only) — push still succeeds past the cap."""
    import scrapy_extension.queue.strategies.delay as mod
    mod._over_cap_warned = False
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, max_held=1, clock=_clock(now)
    )
    strat.push("q", b"a")
    strat.push("q", b"b")  # over cap
    strat.push("q", b"c")  # further over
    # Nothing dropped: soft cap warns but never refuses items.
    assert len(strat._holding) == 3

  def test_explicit_max_held_zero_disables_warning(
    self, mock_connection_manager, caplog
  ) -> None:
    """max_held<=0 disables the soft-cap warning (explicit opt-out for advanced users)."""
    import scrapy_extension.queue.strategies.delay as mod
    mod._over_cap_warned = False
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, max_held=0, clock=_clock(now)
    )
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      for i in range(10):
        strat.push("q", str(i).encode())
    cap_warnings = [
      r for r in caplog.records
      if r.levelno == logging.WARNING and ("max_held" in r.getMessage() or "holding" in r.getMessage())
    ]
    assert cap_warnings == []

  @pytest.mark.parametrize("max_held", [0, -1])
  def test_invalid_max_held_disables(
    self, mock_connection_manager, max_held: int
  ) -> None:
    """Non-positive max_held is accepted (= disabled), per opt-out contract."""
    strat = DelayQueueStrategy(mock_connection_manager, max_held=max_held)
    assert strat._max_held == max_held
