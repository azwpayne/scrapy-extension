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
import math
import threading
from collections import deque
from unittest.mock import MagicMock

import pytest

from scrapy_extension.queue.strategies.time_wheel import (
  MAX_WHEEL_SIZE,
  TimeWheelQueueStrategy,
)


def _strategy(
  *,
  wheel_size: int = 60,
  ticks_per_second: float = 1.0,
  clock_value: float = 100.0,
  default_delay: float = 0.0,
  wall_clock_value: float = 1_000.0,
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
    wall_clock=lambda: wall_clock_value,
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


def test_push_negative_delay_is_rejected():
  """Direct strategy calls follow BackendQueue's non-negative contract."""
  s, qb, _ = _strategy()
  with pytest.raises(ValueError, match="delay must be >= 0"):
    s.push("q", b"x", delay=-5.0)
  qb.push.assert_not_called()


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


def test_sub_tick_delay_releases_on_next_tick_not_after_full_rotation():
  """A fractional ready time must be assigned to the next drainable tick."""
  s, qb, clock = _strategy(
    wheel_size=60,
    ticks_per_second=1.0,
    clock_value=100.2,
  )
  s.push("q", b"soon", delay=0.2)  # ready_at=100.4

  clock[0] = 100.5
  s.pop("q")
  qb.push.assert_not_called()

  clock[0] = 101.0
  s.pop("q")
  qb.push.assert_called_once_with("q", b"soon", 0.0)


def test_negative_monotonic_epoch_uses_floor_for_tick_boundary():
  """Monotonic epochs are arbitrary; negative fractional ticks stay valid."""
  s, qb, clock = _strategy(
    wheel_size=60,
    ticks_per_second=1.0,
    clock_value=-0.2,
  )
  s.push("q", b"soon", delay=0.1)  # ready_at=-0.1, next tick is 0

  clock[0] = 0.0
  s.pop("q")

  qb.push.assert_called_once_with("q", b"soon", 0.0)


def test_pop_drains_overflow_when_due():
  """Long-delay overflow items drain when ready_at <= now."""
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  s.push("q", b"long", delay=120.0)  # ready_at=220
  clock[0] = 225.0  # past ready
  s.pop("q")
  qb.push.assert_called_once_with("q", b"long", 0.0)


def test_failed_wheel_drain_keeps_due_item_for_retry():
  """A transient live-queue push failure must not discard a wheel item."""
  s, qb, clock = _strategy(wheel_size=60, clock_value=100.0)
  s.push("q", b"retry-me", delay=1.0)
  clock[0] = 101.0
  qb.push.side_effect = [RuntimeError("temporary"), None]

  with pytest.raises(RuntimeError, match="temporary"):
    s.pop("q")
  assert sum(len(slot) for slot in s._wheel) == 1

  s.pop("q")
  assert sum(len(slot) for slot in s._wheel) == 0
  assert qb.push.call_count == 2


def test_failed_overflow_drain_keeps_due_item_for_retry():
  """Overflow entries follow the same commit-after-live-push rule."""
  s, qb, clock = _strategy(wheel_size=10, clock_value=0.0)
  s.push("q", b"retry-overflow", delay=20.0)
  clock[0] = 20.0
  qb.push.side_effect = [RuntimeError("temporary"), None]

  with pytest.raises(RuntimeError, match="temporary"):
    s.pop("q")
  assert len(s._overflow) == 1

  s.pop("q")
  assert len(s._overflow) == 0
  assert qb.push.call_count == 2


def test_concurrent_pops_submit_due_overflow_item_exactly_once():
  """Two drainers must not both publish the same uncommitted heap head."""
  s, qb, clock = _strategy(wheel_size=10, clock_value=0.0)
  s.push("q", b"once", delay=20.0)
  clock[0] = 20.0
  first_push_entered = threading.Event()
  release_first_push = threading.Event()
  second_push_entered = threading.Event()
  call_lock = threading.Lock()
  push_calls = 0
  errors: list[BaseException] = []

  def blocking_push(*_args):
    nonlocal push_calls
    with call_lock:
      push_calls += 1
      call_number = push_calls
    if call_number == 1:
      first_push_entered.set()
      if not release_first_push.wait(timeout=2.0):
        raise AssertionError("first backend push was not released")
    else:
      second_push_entered.set()

  qb.push.side_effect = blocking_push

  def pop() -> None:
    try:
      s.pop("q")
    except BaseException as exc:
      errors.append(exc)

  first = threading.Thread(target=pop, daemon=True)
  second = threading.Thread(target=pop, daemon=True)
  first.start()
  assert first_push_entered.wait(timeout=2.0)
  second.start()
  second_push_entered.wait(timeout=0.25)
  release_first_push.set()
  first.join(timeout=2.0)
  second.join(timeout=2.0)

  assert not first.is_alive()
  assert not second.is_alive()
  assert errors == []
  assert push_calls == 1
  assert s._overflow == []


def test_concurrent_push_cannot_be_cleared_between_slot_copy_and_clear():
  """A push racing a drain must remain held instead of being silently lost."""
  s, qb, clock = _strategy(wheel_size=4, clock_value=0.0)
  s.push("q", b"due", delay=1.0)
  clock[0] = 1.0
  snapshot_captured = threading.Event()
  release_snapshot = threading.Event()
  append_completed = threading.Event()
  errors: list[BaseException] = []

  class PausingDeque(deque):
    def __iter__(self):
      snapshot = tuple(super().__iter__())
      snapshot_captured.set()
      if not release_snapshot.wait(timeout=2.0):
        raise AssertionError("slot snapshot was not released")
      return iter(snapshot)

    def append(self, value):
      super().append(value)
      append_completed.set()

  s._wheel[1] = PausingDeque(s._wheel[1])

  def pop() -> None:
    try:
      s.pop("q")
    except BaseException as exc:
      errors.append(exc)

  def push() -> None:
    try:
      # ready_at=5, the same physical slot currently being drained.
      s.push("q", b"future", delay=4.0)
    except BaseException as exc:
      errors.append(exc)

  pop_thread = threading.Thread(target=pop, daemon=True)
  push_thread = threading.Thread(target=push, daemon=True)
  pop_thread.start()
  assert snapshot_captured.wait(timeout=2.0)
  push_thread.start()
  append_completed.wait(timeout=0.25)
  release_snapshot.set()
  pop_thread.join(timeout=2.0)
  push_thread.join(timeout=2.0)

  assert not pop_thread.is_alive()
  assert not push_thread.is_alive()
  assert errors == []
  held = [entry for slot in s._wheel for entry in slot]
  assert held == [(5.0, b"future", 0.0)]
  qb.push.assert_called_once_with("q", b"due", 0.0)


def test_snapshot_cannot_duplicate_overflow_item_during_live_publish():
  """Snapshot must observe either side of a drain commit, never both."""
  s, qb, clock = _strategy(wheel_size=10, clock_value=0.0)
  s.push("q", b"moving", delay=20.0)
  clock[0] = 20.0
  publish_entered = threading.Event()
  release_publish = threading.Event()
  snapshot_done = threading.Event()
  snapshots: list[bytes | None] = []
  errors: list[Exception] = []

  def blocking_push(*_args):
    publish_entered.set()
    if not release_publish.wait(timeout=2.0):
      raise AssertionError("backend publish was not released")

  qb.push.side_effect = blocking_push

  def pop() -> None:
    try:
      s.pop("q")
    except Exception as exc:
      errors.append(exc)

  def snapshot() -> None:
    try:
      snapshots.append(s.snapshot())
    except Exception as exc:
      errors.append(exc)
    finally:
      snapshot_done.set()

  pop_thread = threading.Thread(target=pop, daemon=True)
  snapshot_thread = threading.Thread(target=snapshot, daemon=True)
  pop_thread.start()
  assert publish_entered.wait(timeout=2.0)
  snapshot_thread.start()
  assert not snapshot_done.wait(timeout=0.25)
  release_publish.set()
  pop_thread.join(timeout=2.0)
  snapshot_thread.join(timeout=2.0)

  assert not pop_thread.is_alive()
  assert not snapshot_thread.is_alive()
  assert errors == []
  assert snapshots == [None]


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
  s.push("q", b"a", delay=10.0)  # wheel
  s.push("q", b"b", delay=100.0)  # overflow
  qb.queue_len.return_value = 5  # live
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
  assert data["version"] == 2
  assert data["strategy"] == "time_wheel"
  assert data["snapshot_wall_time"] == 1_000.0
  assert len(data["slots_flat"]) == 1  # one wheel item
  assert base64.b64decode(data["slots_flat"][0]["item_b64"]) == b"wheel-item"
  assert len(data["overflow"]) == 1
  assert base64.b64decode(data["overflow"][0]["item_b64"]) == b"overflow-item"


def test_snapshot_restore_preserves_overflow_heap_stable_order():
  source, _, _ = _strategy(wheel_size=10, clock_value=0.0)
  for index, delay in enumerate((20.0, 21.0, 21.0, 20.0)):
    source.push("q", str(index).encode(), delay=delay)
  expected = [entry[2] for entry in sorted(source._overflow)]

  restored, _, _ = _strategy(wheel_size=10, clock_value=0.0)
  restored.restore(source.snapshot())
  actual = [entry[2] for entry in sorted(restored._overflow)]

  assert actual == expected


def test_restore_round_trip_rebuilds_state():
  """Restore preserves ready_at — a future wheel item is re-placed in the wheel
  at its recomputed slot (NOT routed to overflow), so its remaining delay
  survives the restart. Overflow items keep ready_at. Past-due wheel items
  route to overflow so they drain on the next pop.
  """
  s1, _, clock = _strategy(wheel_size=60, clock_value=100.0)
  s1.push("q", b"w", delay=5.0, priority=2.0)  # ready_at=105 (future at restore)
  s1.push("q", b"o", delay=100.0, priority=1.0)  # ready_at=200 (overflow)
  blob = s1.snapshot()

  s2, _, _ = _strategy(wheel_size=60, clock_value=100.0)
  s2.restore(blob)
  # The future wheel item (ready_at=105 > now=100) is re-placed in the wheel
  # at slot 105 % 60 = 45; the overflow item (ready_at=200) stays in overflow.
  assert sum(len(slot) for slot in s2._wheel) == 1
  assert len(s2._overflow) == 1


def test_restore_atomically_replaces_existing_state_and_is_idempotent():
  source, _, _ = _strategy(clock_value=100.0)
  source.push("q", b"snapshot-item", delay=2.0)
  blob = source.snapshot()

  restored, _, _ = _strategy(clock_value=100.0)
  restored.push("q", b"stale-local-item", delay=1.0)
  restored.restore(blob)
  restored.restore(blob)

  held = [entry for slot in restored._wheel for entry in slot]
  assert held == [(102.0, b"snapshot-item", 0.0)]
  assert restored._overflow == []


def test_restore_rebases_deadlines_across_monotonic_epoch_and_downtime():
  source, _, _ = _strategy(
    wheel_size=60,
    clock_value=5_000.0,
    wall_clock_value=10_000.0,
  )
  source.push("q", b"w", delay=20.0)
  blob = source.snapshot()

  clock = [2.0]
  manager = MagicMock()
  backend = manager.get_queue_backend.return_value
  restored = TimeWheelQueueStrategy(
    manager,
    wheel_size=60,
    clock=lambda: clock[0],
    wall_clock=lambda: 10_005.0,
  )
  restored.restore(blob)

  entries = [entry for slot in restored._wheel for entry in slot]
  assert entries == [(17.0, b"w", 0.0)]
  clock[0] = 16.9
  restored.pop("q")
  backend.push.assert_not_called()
  clock[0] = 17.0
  restored.pop("q")
  backend.push.assert_called_once_with("q", b"w", 0.0)


def test_restore_v1_deadline_is_due_instead_of_cross_boot_stall():
  blob = json.dumps(
    {
      "version": 1,
      "strategy": "time_wheel",
      "slots_flat": [
        {
          "ready_at": 999_999.0,
          "item_b64": base64.b64encode(b"legacy").decode(),
          "priority": 0.0,
        }
      ],
      "overflow": [],
    }
  ).encode()
  restored, _, clock = _strategy(clock_value=3.0)

  restored.restore(blob)

  assert restored._overflow[0][0] == clock[0]


@pytest.mark.parametrize("version", [1, 2])
def test_restore_expired_items_preserve_original_deadline_order(version):
  deadline_field = "ready_at" if version == 1 else "remaining"
  data = {
    "version": version,
    "strategy": "time_wheel",
    "slots_flat": [
      {
        deadline_field: 10.0 if version == 2 else 200.0,
        "item_b64": base64.b64encode(b"later").decode(),
        "priority": 0.0,
      }
    ],
    "overflow": [
      {
        deadline_field: 5.0 if version == 2 else 100.0,
        "item_b64": base64.b64encode(b"earlier").decode(),
        "priority": 0.0,
      }
    ],
  }
  if version == 2:
    data["snapshot_wall_time"] = 1_000.0
  restored, qb, _ = _strategy(
    clock_value=50.0,
    wall_clock_value=1_020.0,
  )

  restored.restore(json.dumps(data).encode())
  restored.pop("q")

  assert [call.args[1] for call in qb.push.call_args_list] == [b"earlier", b"later"]


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


@pytest.mark.parametrize("field", ["slots_flat", "overflow"])
def test_restore_non_list_collection_is_skipped_without_crashing(field):
  s, _, _ = _strategy()
  data = {
    "version": 2,
    "strategy": "time_wheel",
    "snapshot_wall_time": 1_000.0,
    "slots_flat": [],
    "overflow": [],
  }
  data[field] = 42

  s.restore(json.dumps(data).encode())

  assert all(not slot for slot in s._wheel)
  assert s._overflow == []


# ---------------------------------------------------------------------------
# config validation
# ---------------------------------------------------------------------------


def test_wheel_size_zero_raises():
  cm = MagicMock()
  with pytest.raises(ValueError, match="wheel_size must be >= 1"):
    TimeWheelQueueStrategy(cm, wheel_size=0)


@pytest.mark.parametrize("wheel_size", [True, 1.5])
def test_wheel_size_requires_integer(wheel_size):
  with pytest.raises(ValueError, match="wheel_size must be >= 1"):
    TimeWheelQueueStrategy(MagicMock(), wheel_size=wheel_size)


def test_wheel_size_rejects_eager_allocation_above_guard():
  with pytest.raises(ValueError, match=f"wheel_size must be <= {MAX_WHEEL_SIZE}"):
    TimeWheelQueueStrategy(MagicMock(), wheel_size=MAX_WHEEL_SIZE + 1)


def test_ticks_per_second_zero_raises():
  cm = MagicMock()
  with pytest.raises(ValueError, match="ticks_per_second must be > 0"):
    TimeWheelQueueStrategy(cm, ticks_per_second=0.0)


def test_negative_default_delay_raises():
  cm = MagicMock()
  with pytest.raises(ValueError, match="default_delay must be >= 0"):
    TimeWheelQueueStrategy(cm, default_delay=-1.0)


@pytest.mark.parametrize("ticks_per_second", [math.nan, math.inf])
def test_non_finite_ticks_per_second_raises(ticks_per_second):
  with pytest.raises(ValueError, match="ticks_per_second must be finite"):
    TimeWheelQueueStrategy(MagicMock(), ticks_per_second=ticks_per_second)


@pytest.mark.parametrize(
  ("field", "kwargs"),
  [
    ("wheel_size", {"wheel_size": True}),
    ("ticks_per_second", {"ticks_per_second": True}),
    ("default_delay", {"default_delay": True}),
  ],
)
def test_bool_constructor_numbers_are_rejected(field, kwargs):
  with pytest.raises(ValueError, match=field):
    TimeWheelQueueStrategy(MagicMock(), **kwargs)


def test_ticks_per_second_rejects_infinite_wheel_duration():
  with pytest.raises(ValueError, match="wheel duration must be finite"):
    TimeWheelQueueStrategy(MagicMock(), ticks_per_second=5e-324)


@pytest.mark.parametrize("default_delay", [math.nan, math.inf])
def test_non_finite_default_delay_raises(default_delay):
  with pytest.raises(ValueError, match="default_delay must be finite"):
    TimeWheelQueueStrategy(MagicMock(), default_delay=default_delay)


@pytest.mark.parametrize("delay", [True, math.nan, math.inf])
def test_push_rejects_non_finite_delay(delay):
  s, qb, _ = _strategy()

  with pytest.raises(ValueError, match="delay must be finite"):
    s.push("q", b"never", delay=delay)

  qb.push.assert_not_called()
  assert all(not slot for slot in s._wheel)
  assert s._overflow == []


@pytest.mark.parametrize("priority", [math.nan, math.inf, -math.inf])
def test_push_rejects_non_finite_priority(priority):
  s, qb, _ = _strategy()

  with pytest.raises(ValueError, match="priority must be finite"):
    s.push("q", b"blocked", priority=priority, delay=1.0)

  qb.push.assert_not_called()
  assert all(not slot for slot in s._wheel)
  assert s._overflow == []


def test_non_finite_runtime_clock_preserves_held_state():
  s, qb, clock = _strategy(clock_value=0.0)
  s.push("q", b"held", delay=1.0)
  clock[0] = math.inf

  with pytest.raises(ValueError, match="clock must return a finite value"):
    s.pop("q")

  qb.push.assert_not_called()
  assert sum(len(slot) for slot in s._wheel) == 1
