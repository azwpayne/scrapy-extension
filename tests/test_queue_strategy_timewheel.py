"""Tests for TimeWheelQueueStrategy — O(1) hashed timing wheel + overflow heap.

Covers:
- push delay=0 → straight to live queue
- push short delay (< wheel_duration) → correct slot
- push long delay (> wheel_duration) → overflow heap
- pop drains due wheel slots then due overflow, then live
- queue_len sums wheel + overflow + live
- clear clears all three
- snapshot/restore round-trip (versioned JSON, base64 items)
- restore skips corrupt/unknown-format
- wrap-around (slot index mod wheel_size)
- drain-span cap (idle > wheel_duration drains at most one rotation)
- config validation
"""

from __future__ import annotations

import base64
import json
from collections import deque
from unittest.mock import MagicMock

import pytest

from scrapy_extension.queue.strategies.time_wheel import TimeWheelQueueStrategy


def _strategy(
  *,
  wheel_size: int = 60,
  ticks_per_second: float = 1.0,
  clock_value: float = 100.0,
  default_delay: float = 0.0,
) -> tuple[TimeWheelQueueStrategy, MagicMock, list]:
  """Build a strategy with a mocked CM + frozen clock."""
  clock_state = [clock_value]
  cm = MagicMock(name="ConnectionManager")
  qb = MagicMock(name="QueueBackend")
  qb.pop.return_value = None
  cm.get_queue_backend.return_value = qb
  s = TimeWheelQueueStrategy(
    cm,
    wheel_size=wheel_size,
    ticks_per_second=ticks_per_second,
    default_delay=default_delay,
    clock=lambda: clock_state[0],
  )
  return s, qb, clock_state


# ---------------------------------------------------------------------------
# push routing
# ---------------------------------------------------------------------------


def test_push_no_delay_goes_straight_to_live():
  s, qb, _ = _strategy()
  s.push("q", b"now", priority=1.0)
  qb.push.assert_called_once_with("q", b"now", 1.0)


def test_push_default_delay_when_omitted():
  s, qb, _ = _strategy(default_delay=5.0, clock_value=100.0)
  s.push("q", b"x")  # no explicit delay → default_delay
  # default_delay=5 < wheel_duration=60 → lands in slot, not live
  qb.push.assert_not_called()
  assert sum(len(slot) for slot in s._wheel) == 1


def test_push_short_delay_lands_in_correct_slot():
  """delay=5s, ticks=1/s, now=100 → ready_at=105 → slot 105 % 60 = 45."""
  s, qb, _ = _strategy(wheel_size=60, ticks_per_second=1.0, clock_value=100.0)
  s.push("q", b"x", delay=5.0)
  qb.push.assert_not_called()  # held, not live
  slot = 105 % 60
  assert s._wheel[slot] == deque([(105.0, b"x", 0.0)])


def test_push_long_delay_goes_to_overflow_heap():
  """delay > wheel_duration → overflow heap."""
  s, qb, _ = _strategy(wheel_size=60, ticks_per_second=1.0, clock_value=100.0)
  s.push("q", b"long", delay=120.0)  # wheel_duration = 60s
  qb.push.assert_not_called()
  assert len(s._overflow) == 1
  assert s._overflow[0][0] == 220.0  # ready_at = 100 + 120
  assert s._overflow[0][2] == b"long"


def test_push_negative_delay_clamped_to_zero_live():
  """Negative delay is treated as 0 (live now)."""
  s, qb, _ = _strategy()
  s.push("q", b"x", delay=-5.0)
  qb.push.assert_called_once_with("q", b"x", 0.0)


# ---------------------------------------------------------------------------
# pop drain semantics
# ---------------------------------------------------------------------------


def test_pop_drains_due_wheel_slot_before_live():
  """A held item past its ready_at drains into live, then pop returns it."""
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  s.push("q", b"held", delay=5.0)  # ready_at=105, slot=45
  # Live empty initially. Advance clock past ready_at.
  clock[0] = 106.0
  qb.pop.return_value = None
  # After drain, the held item should be pushed to live; we then pop None
  # (mock), but the drain push should fire.
  s.pop("q")
  qb.push.assert_called_once_with("q", b"held", 0.0)


def test_pop_does_not_drain_future_held_items():
  """Held item not yet ready stays in the wheel across pops."""
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  s.push("q", b"future", delay=50.0)  # ready_at=150
  clock[0] = 110.0  # not yet ready
  s.pop("q")
  qb.push.assert_not_called()  # nothing drained


def test_pop_drains_overflow_when_due():
  """Long-delay overflow items drain when ready_at <= now."""
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  s.push("q", b"long", delay=120.0)  # ready_at=220
  clock[0] = 225.0  # past ready
  s.pop("q")
  qb.push.assert_called_once_with("q", b"long", 0.0)


def test_pop_returns_live_item_after_drain():
  """Drain runs first, then the live pop fetches whatever is there."""
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  qb.pop.return_value = b"from-live"
  item = s.pop("q")
  assert item == b"from-live"


def test_pop_with_ack_threads_mq_token_after_drain(mocker):
  """Time-wheel must preserve a backend's per-message ack token."""
  s, qb, _ = _strategy(wheel_size=60, clock_value=100.0)
  delegated = mocker.patch.object(
    s,
    "_pop_backend_with_ack",
    return_value=(b"from-live", "ack-token-123"),
  )

  assert s.pop_with_ack("q", timeout=2.0) == (b"from-live", "ack-token-123")
  delegated.assert_called_once_with("q", 2.0)
  qb.pop.assert_not_called()


def test_drain_span_capped_to_one_rotation():
  """Idle > wheel_duration: drain at most wheel_size slots (one full rotation)."""
  s, qb, clock = _strategy(wheel_size=10, ticks_per_second=1.0, clock_value=0.0)
  # Place an item in a slot for ready_at = 5s.
  s.push("q", b"x", delay=5.0)  # slot = 5
  # Idle a LONG time (more than one full rotation = 10s).
  clock[0] = 100.0
  s.pop("q")
  # The item in slot 5 was due at t=5; we're now at t=100. After drain it should
  # have been pushed exactly once (not lost, not double-pushed).
  qb.push.assert_called_once_with("q", b"x", 0.0)


def test_long_idle_drain_does_not_release_future_wheel_item_early():
  """A future wheel item pushed DURING a long idle (> one rotation) must NOT
  be released early by the catch-up full-rotation drain.

  The drain caps the span to wheel_size (one full rotation). After a long idle
  the cap covers every slot — including a slot holding a future item pushed
  during the idle. Without a ready_at check on drain, that item is released
  before its delay elapses (the slot stored only (item, priority), so the drain
  couldn't tell future from due).
  """
  s, qb, clock = _strategy(wheel_size=4, ticks_per_second=1.0, clock_value=0.0)
  # _last_tick = 0. Idle past one full rotation (4s) to tick 5.
  clock[0] = 5.0
  # Push a future item: delay=3 -> ready_at=8, slot = 8 % 4 = 0.
  s.push("q", b"future", delay=3.0)
  # Pop at tick 5: the catch-up drain (span=min(5,4)=4) covers ticks 1-4
  # (slots 1,2,3,0). Slot 0 is hit via tick 4. Without the fix the future
  # item (ready_at=8) is released 3s early.
  s.pop("q")
  qb.push.assert_not_called()  # future item NOT released early

  # Advance to ready_at=8 and pop — now it should be released.
  clock[0] = 8.0
  s.pop("q")
  qb.push.assert_called_once_with("q", b"future", 0.0)


def test_wrap_around_drains_correct_slot():
  """Wheel wrap: slot index mod wheel_size handles rotation past the end."""
  s, qb, clock = _strategy(wheel_size=10, ticks_per_second=1.0, clock_value=8.0)
  s.push("q", b"x", delay=5.0)  # ready_at=13, slot=13%10=3
  clock[0] = 14.0
  s.pop("q")
  qb.push.assert_called_once_with("q", b"x", 0.0)


# ---------------------------------------------------------------------------
# queue_len, clear
# ---------------------------------------------------------------------------


def test_queue_len_sums_live_wheel_overflow():
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  s.push("q", b"a", delay=10.0)   # wheel
  s.push("q", b"b", delay=100.0)  # overflow
  qb.queue_len.return_value = 5   # live
  total = s.queue_len("q")
  assert total == 5 + 1 + 1


def test_clear_clears_wheel_overflow_and_live():
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  s.push("q", b"a", delay=10.0)
  s.push("q", b"b", delay=100.0)
  s.clear("q")
  assert all(len(slot) == 0 for slot in s._wheel)
  assert len(s._overflow) == 0
  qb.clear_queue.assert_called_once_with("q")


# ---------------------------------------------------------------------------
# snapshot / restore
# ---------------------------------------------------------------------------


def test_snapshot_empty_returns_none():
  s, _, _ = _strategy()
  assert s.snapshot() is None


def test_snapshot_serializes_wheel_and_overflow():
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  s.push("q", b"wheel-item", delay=5.0, priority=2.0)
  s.push("q", b"overflow-item", delay=100.0, priority=1.0)
  blob = s.snapshot()
  assert blob is not None
  data = json.loads(blob.decode())
  assert data["version"] == 1
  assert data["strategy"] == "time_wheel"
  assert len(data["slots_flat"]) == 1  # one wheel item
  assert base64.b64decode(data["slots_flat"][0]["item_b64"]) == b"wheel-item"
  assert len(data["overflow"]) == 1
  assert base64.b64decode(data["overflow"][0]["item_b64"]) == b"overflow-item"


def test_restore_round_trip_rebuilds_state():
  """Restore preserves ready_at — a future wheel item is re-placed in the wheel
  at its recomputed slot (NOT routed to overflow), so its remaining delay
  survives the restart. Overflow items keep ready_at. Past-due wheel items
  route to overflow so they drain on the next pop.
  """
  s1, _, clock = _strategy(wheel_size=60, clock_value=100.0)
  s1.push("q", b"w", delay=5.0, priority=2.0)   # ready_at=105 (future at restore)
  s1.push("q", b"o", delay=100.0, priority=1.0)  # ready_at=200 (overflow)
  blob = s1.snapshot()

  s2, _, _ = _strategy(wheel_size=60, clock_value=100.0)
  s2.restore(blob)
  # The future wheel item (ready_at=105 > now=100) is re-placed in the wheel
  # at slot 105 % 60 = 45; the overflow item (ready_at=200) stays in overflow.
  assert sum(len(slot) for slot in s2._wheel) == 1
  assert len(s2._overflow) == 1


def test_restore_none_is_noop():
  s, _, _ = _strategy()
  s.restore(None)
  s.restore(b"")
  assert sum(len(slot) for slot in s._wheel) == 0
  assert len(s._overflow) == 0


def test_restore_corrupt_skipped():
  s, _, _ = _strategy()
  s.restore(b"\x00 not json \x00")
  assert len(s._overflow) == 0


def test_restore_unknown_format_skipped():
  s, _, _ = _strategy()
  blob = json.dumps({"version": 99, "strategy": "other"}).encode()
  s.restore(blob)
  assert len(s._overflow) == 0


# ---------------------------------------------------------------------------
# config validation
# ---------------------------------------------------------------------------


def test_wheel_size_zero_raises():
  cm = MagicMock()
  with pytest.raises(ValueError, match="wheel_size must be >= 1"):
    TimeWheelQueueStrategy(cm, wheel_size=0)


def test_ticks_per_second_zero_raises():
  cm = MagicMock()
  with pytest.raises(ValueError, match="ticks_per_second must be > 0"):
    TimeWheelQueueStrategy(cm, ticks_per_second=0.0)


def test_negative_default_delay_raises():
  cm = MagicMock()
  with pytest.raises(ValueError, match="default_delay must be >= 0"):
    TimeWheelQueueStrategy(cm, default_delay=-1.0)
