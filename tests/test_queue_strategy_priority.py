"""Tests for PriorityQueueStrategy — strategy-layer priority via N physical buckets.

Covers:
- Priority → level mapping (higher priority → lower level index, popped first)
- Clamping (priority > 1.0 / < 0.0)
- Pop scans p0..p(N-1), returns first non-empty
- Pop falls through to blocking wait on level 0 when all empty + timeout > 0
- queue_len sums all levels
- clear clears all levels
- snapshot/restore ABC defaults (None / no-op — state is backend-side)
- Config validation: levels >= 1
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scrapy_extension.queue.strategies.priority import PriorityQueueStrategy


def _strategy(levels: int = 3) -> tuple[PriorityQueueStrategy, MagicMock]:
  """Build a strategy with a mocked ConnectionManager + QueueBackend."""
  cm = MagicMock(name="ConnectionManager")
  qb = MagicMock(name="QueueBackend")
  qb.pop.return_value = None
  cm.get_queue_backend.return_value = qb
  return PriorityQueueStrategy(cm, levels=levels), qb


def test_pop_with_ack_threads_backend_token_from_first_non_empty_bucket():
  """#28: pop_with_ack scans buckets via backend.pop_with_ack and returns
  (data, token) so MQ backends paired with the priority strategy keep
  per-message ack correlation (previously pop() returned data only → the
  scheduler saw token=None → silent at-least-once hazard under MQ)."""
  s, qb = _strategy(levels=3)
  qb.pop_with_ack.return_value = (b"item", "TOKEN-P0")
  data, token = s.pop_with_ack("q", 0.0)
  assert data == b"item"
  assert token == "TOKEN-P0"
  # First non-empty bucket (p0, highest priority) — does not scan further.
  qb.pop_with_ack.assert_called_once_with("q:p0", 0.0)


# ---------------------------------------------------------------------------
# push — priority → level mapping
# ---------------------------------------------------------------------------


def test_push_highest_priority_goes_to_level_zero():
  """priority=1.0 → level 0 (popped first)."""
  s, qb = _strategy(levels=3)
  s.push("q", b"urgent", priority=1.0)
  qb.push.assert_called_once_with("q:p0", b"urgent")


def test_push_lowest_priority_goes_to_last_level():
  """priority=0.0 → level N-1 (popped last)."""
  s, qb = _strategy(levels=3)
  s.push("q", b"bulk", priority=0.0)
  qb.push.assert_called_once_with("q:p2", b"bulk")


def test_push_mid_priority_goes_to_middle_level():
  """priority=0.5 → level 1 (middle of 3)."""
  s, qb = _strategy(levels=3)
  s.push("q", b"normal", priority=0.5)
  qb.push.assert_called_once_with("q:p1", b"normal")


def test_push_priority_above_one_clamps_to_level_zero():
  """priority > 1.0 clamps to 1.0 → level 0."""
  s, qb = _strategy(levels=3)
  s.push("q", b"x", priority=99.0)
  qb.push.assert_called_once_with("q:p0", b"x")


def test_push_negative_priority_clamps_to_last_level():
  """priority < 0.0 clamps to 0.0 → level N-1."""
  s, qb = _strategy(levels=3)
  s.push("q", b"x", priority=-5.0)
  qb.push.assert_called_once_with("q:p2", b"x")


def test_push_default_priority_goes_to_last_level():
  """Caller omitting priority uses the ABC default 0.0 → level N-1."""
  s, qb = _strategy(levels=3)
  s.push("q", b"x")
  qb.push.assert_called_once_with("q:p2", b"x")


def test_push_two_levels():
  """levels=2 splits at priority=0.5."""
  s, qb = _strategy(levels=2)
  s.push("q", b"high", priority=0.9)  # → level 0
  s.push("q", b"low", priority=0.1)   # → level 1
  assert qb.push.call_args_list[0].args == ("q:p0", b"high")
  assert qb.push.call_args_list[1].args == ("q:p1", b"low")


# ---------------------------------------------------------------------------
# pop — scan high → low, blocking fallback
# ---------------------------------------------------------------------------


def test_pop_returns_from_highest_non_empty_level():
  """p0 empty, p1 has item → returns p1's item after scanning p0."""
  s, qb = _strategy(levels=3)
  qb.pop.side_effect = [None, b"from-p1", None]
  item = s.pop("q")
  assert item == b"from-p1"
  # Scanned p0 (None), then p1 (hit) — stopped, didn't check p2.
  assert qb.pop.call_count == 2
  assert qb.pop.call_args_list[0].args == ("q:p0", 0.0)
  assert qb.pop.call_args_list[1].args == ("q:p1", 0.0)


def test_pop_returns_none_when_all_levels_empty_no_timeout():
  """All levels empty, timeout=0 → scans all levels, returns None."""
  s, qb = _strategy(levels=3)
  qb.pop.return_value = None
  assert s.pop("q") is None
  assert qb.pop.call_count == 3  # scanned all 3 levels non-blocking


def test_pop_blocking_fallback_on_level_zero_when_timeout():
  """All empty + timeout > 0 → non-blocking scan + 1 blocking pop on p0."""
  s, qb = _strategy(levels=3)
  qb.pop.side_effect = [None, None, None, b"arrived"]
  item = s.pop("q", timeout=5.0)
  assert item == b"arrived"
  # 3 non-blocking scans (all None) + 1 blocking pop on p0 with full timeout.
  assert qb.pop.call_count == 4
  assert qb.pop.call_args_list[3].args == ("q:p0", 5.0)


def test_pop_skip_blocking_when_scan_finds_item():
  """If the non-blocking scan finds an item, NO blocking pop follows."""
  s, qb = _strategy(levels=2)
  qb.pop.side_effect = [b"fast"]
  s.pop("q", timeout=10.0)
  assert qb.pop.call_count == 1  # found in p0, no blocking fallback


# ---------------------------------------------------------------------------
# queue_len, clear
# ---------------------------------------------------------------------------


def test_queue_len_sums_all_levels():
  s, qb = _strategy(levels=3)
  qb.queue_len.side_effect = [10, 20, 30]
  assert s.queue_len("q") == 60
  assert qb.queue_len.call_args_list[0].args == ("q:p0",)
  assert qb.queue_len.call_args_list[2].args == ("q:p2",)


def test_clear_clears_all_levels():
  s, qb = _strategy(levels=3)
  s.clear("q")
  assert qb.clear_queue.call_count == 3
  cleared = [c.args[0] for c in qb.clear_queue.call_args_list]
  assert cleared == ["q:p0", "q:p1", "q:p2"]


# ---------------------------------------------------------------------------
# snapshot / restore — ABC defaults (no in-process state)
# ---------------------------------------------------------------------------


def test_snapshot_returns_none():
  s, _ = _strategy()
  assert s.snapshot() is None


def test_restore_is_noop():
  s, _ = _strategy()
  s.restore(b"anything")  # no crash
  s.restore(None)


# ---------------------------------------------------------------------------
# config validation
# ---------------------------------------------------------------------------


def test_levels_zero_raises_value_error():
  with pytest.raises(ValueError, match="levels must be >= 1"):
    _strategy(levels=0)


def test_levels_negative_raises_value_error():
  with pytest.raises(ValueError, match="levels must be >= 1"):
    _strategy(levels=-1)


def test_levels_one_routes_everything_to_p0():
  """Edge: levels=1 collapses to a single bucket — degenerate but valid."""
  s, qb = _strategy(levels=1)
  s.push("q", b"x", priority=1.0)
  s.push("q", b"y", priority=0.0)
  assert all(
    c.args == ("q:p0", expected)
    for c, expected in zip(qb.push.call_args_list, [b"x", b"y"], strict=True)
  )
