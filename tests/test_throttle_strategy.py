"""Tests for ThrottleQueueStrategy (subsystem ②)."""

from __future__ import annotations

import pytest

from scrapy_extension.queue.strategies.factory import (
  QueueStrategyType,
  build_queue_strategy,
)
from scrapy_extension.queue.strategies.throttle import ThrottleQueueStrategy


def _clock(now: list[float]):
  return lambda: now[0]


class TestThrottleQueueStrategy:
  def test_no_interval_always_pops(self, mock_connection_manager) -> None:
    now = [0.0]
    strat = ThrottleQueueStrategy(mock_connection_manager, clock=_clock(now))
    mock_connection_manager.get_queue_backend().pop.side_effect = [b"a", b"b"]
    assert strat.pop("q") == b"a"
    assert strat.pop("q") == b"b"

  def test_pop_with_ack_threads_mq_token(self, mock_connection_manager, mocker) -> None:
    # throttle.pop_with_ack must delegate to _pop_backend_with_ack (which
    # threads the MQ per-message ack token) -- pre-fix the inherited base
    # default dropped it and silently fell back to atomic pop() (at-most-once
    # for MQ backends).
    now = [100.0]
    strat = ThrottleQueueStrategy(
      mock_connection_manager, min_interval=5.0, clock=_clock(now)
    )
    deleg = mocker.patch.object(
      strat, "_pop_backend_with_ack", return_value=(b"item", "ack-token-123")
    )
    data, token = strat.pop_with_ack("q")
    assert (data, token) == (b"item", "ack-token-123")
    deleg.assert_called_once_with("q", 0.0)

  def test_pop_with_ack_throttled_returns_none_none(
    self, mock_connection_manager, mocker
  ) -> None:
    now = [100.0]
    strat = ThrottleQueueStrategy(
      mock_connection_manager, min_interval=5.0, clock=_clock(now)
    )
    deleg = mocker.patch.object(
      strat, "_pop_backend_with_ack", return_value=(b"first", "t1")
    )
    assert strat.pop_with_ack("q") == (b"first", "t1")
    now[0] = 103.0  # within min_interval
    # Throttled pop must NOT touch the backend (no delegation).
    assert strat.pop_with_ack("q") == (None, None)
    deleg.assert_called_once_with("q", 0.0)

  def test_throttled_within_interval(self, mock_connection_manager) -> None:
    now = [100.0]
    strat = ThrottleQueueStrategy(
      mock_connection_manager, min_interval=5.0, clock=_clock(now)
    )
    mock_connection_manager.get_queue_backend().pop.return_value = b"a"
    assert strat.pop("q") == b"a"  # first pop ok
    now[0] = 103.0  # within 5s
    assert strat.pop("q") is None  # throttled
    # backend.pop must NOT have been called during the throttled pop
    assert mock_connection_manager.get_queue_backend().pop.call_count == 1

  def test_pop_allowed_after_interval(self, mock_connection_manager) -> None:
    now = [100.0]
    strat = ThrottleQueueStrategy(
      mock_connection_manager, min_interval=5.0, clock=_clock(now)
    )
    mock_connection_manager.get_queue_backend().pop.return_value = b"a"
    assert strat.pop("q") == b"a"
    now[0] = 106.0  # past 5s
    mock_connection_manager.get_queue_backend().pop.return_value = b"b"
    assert strat.pop("q") == b"b"

  def test_empty_pop_does_not_reset_timer(self, mock_connection_manager) -> None:
    """An empty pop (None) must not count as a successful pop for throttling."""
    now = [100.0]
    strat = ThrottleQueueStrategy(
      mock_connection_manager, min_interval=5.0, clock=_clock(now)
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None
    assert strat.pop("q") is None  # empty
    assert strat._last_pop is None  # timer not set
    now[0] = 101.0  # only 1s later, but timer never started
    mock_connection_manager.get_queue_backend().pop.return_value = b"x"
    assert strat.pop("q") == b"x"  # allowed (no prior successful pop)

  def test_push_delegates_to_backend(self, mock_connection_manager) -> None:
    now = [0.0]
    strat = ThrottleQueueStrategy(mock_connection_manager, clock=_clock(now))
    strat.push("q", b"x", priority=3.0)
    mock_connection_manager.get_queue_backend().push.assert_called_once_with(
      "q", b"x", 3.0
    )

  def test_clear_resets_timer(self, mock_connection_manager) -> None:
    now = [100.0]
    strat = ThrottleQueueStrategy(
      mock_connection_manager, min_interval=5.0, clock=_clock(now)
    )
    mock_connection_manager.get_queue_backend().pop.return_value = b"a"
    strat.pop("q")
    assert strat._last_pop is not None
    strat.clear("q")
    assert strat._last_pop is None
    mock_connection_manager.get_queue_backend().clear_queue.assert_called_once_with("q")

  def test_invalid_interval_raises(self, mock_connection_manager) -> None:
    with pytest.raises(ValueError, match="min_interval"):
      ThrottleQueueStrategy(mock_connection_manager, min_interval=-1.0)

  # ----- R14-F MED: min_interval must be bounded -----

  def test_min_interval_upper_bound_rejects_misconfig(
    self, mock_connection_manager
  ) -> None:
    """R14-F MED: ``min_interval`` above the documented ceiling (3600s = 1h)
    is rejected as a ConfigurationError. A pathologically large value
    (e.g. ``1e9``) would make the queue look permanently empty for the
    process lifetime — a soft DoS via misconfig. Reject loudly instead.
    """
    from scrapy_extension.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError, match="min_interval"):
      ThrottleQueueStrategy(mock_connection_manager, min_interval=3600.1)
    # Exactly at the ceiling is accepted (inclusive bound).
    strat = ThrottleQueueStrategy(mock_connection_manager, min_interval=3600.0)
    assert strat._min_interval == 3600.0

  def test_min_interval_upper_bound_rejects_extreme(
    self, mock_connection_manager
  ) -> None:
    """R14-F MED: extreme misconfig (``1e9``) is rejected, not silently
    accepted as a permanently-empty-looking queue."""
    from scrapy_extension.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError):
      ThrottleQueueStrategy(mock_connection_manager, min_interval=1e9)


class TestFactoryThrottle:
  def test_build_throttle(self, mock_connection_manager) -> None:
    strat = build_queue_strategy(
      QueueStrategyType.THROTTLE, mock_connection_manager, min_interval=2.0
    )
    assert isinstance(strat, ThrottleQueueStrategy)
