"""Coverage-closure tests for time_wheel + ring_buffer strategies.

Closes the remaining branch-coverage gaps surfaced by the project's 95% floor:
- TimeWheel: close() warn-on-held, snapshot with overflow-only, restore
  malformed-entry skip, restore recovery info log
- RingBuffer: snapshot when buffer empty but dropped > 0
"""

from __future__ import annotations

import base64
import json
import logging
from unittest.mock import MagicMock

from scrapy_extension.queue.strategies.ring_buffer import RingBufferQueueStrategy
from scrapy_extension.queue.strategies.time_wheel import TimeWheelQueueStrategy


def _timewheel(*, clock_value: float = 100.0, wheel_size: int = 60):
  cm = MagicMock()
  cm.get_queue_backend.return_value = MagicMock()
  return TimeWheelQueueStrategy(
    cm,
    wheel_size=wheel_size,
    ticks_per_second=1.0,
    clock=lambda: clock_value,
  )


# ---------------------------------------------------------------------------
# TimeWheel — close() warns when held items are discarded
# ---------------------------------------------------------------------------


def test_timewheel_close_warns_when_held_items(caplog):
  s = _timewheel(clock_value=100.0)
  s.push("q", b"a", delay=10.0)   # wheel slot
  s.push("q", b"b", delay=200.0)  # overflow
  with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.time_wheel"):
    s.close()
  assert any("discarding 2 held" in r.message for r in caplog.records)


def test_timewheel_close_silent_when_empty(caplog):
  s = _timewheel()
  with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.time_wheel"):
    s.close()
  assert not caplog.records


# ---------------------------------------------------------------------------
# TimeWheel — snapshot with overflow-only (no wheel items)
# ---------------------------------------------------------------------------


def test_timewheel_snapshot_only_overflow_serializes():
  s = _timewheel(clock_value=100.0)
  s.push("q", b"long", delay=200.0)  # only overflow, no wheel
  blob = s.snapshot()
  assert blob is not None
  data = json.loads(blob.decode())
  assert data["slots_flat"] == []
  assert len(data["overflow"]) == 1


# ---------------------------------------------------------------------------
# TimeWheel — restore: malformed slot/overflow entries skipped, recovered log
# ---------------------------------------------------------------------------


def test_timewheel_restore_malformed_slot_entry_skipped(caplog):
  s = _timewheel()
  blob = json.dumps(
    {
      "version": 1,
      "strategy": "time_wheel",
      "wheel_size": 60,
      "slots_flat": [{"item_b64": "!!!", "priority": "bad"}],  # malformed
      "overflow": [],
    }
  ).encode()
  with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.time_wheel"):
    s.restore(blob)
  assert any("skipping malformed wheel entry" in r.message for r in caplog.records)


def test_timewheel_restore_malformed_overflow_entry_skipped():
  s = _timewheel()
  blob = json.dumps(
    {
      "version": 1,
      "strategy": "time_wheel",
      "slots_flat": [],
      "overflow": [{"ready_at": "x", "item_b64": "!!!"}],  # malformed
    }
  ).encode()
  s.restore(blob)
  assert len(s._overflow) == 0


def test_timewheel_restore_logs_recovered_count(caplog):
  s = _timewheel()
  item_b64 = base64.b64encode(b"ok").decode()
  blob = json.dumps(
    {
      "version": 1,
      "strategy": "time_wheel",
      "slots_flat": [],
      "overflow": [{"ready_at": 999.0, "item_b64": item_b64, "priority": 1.0}],
    }
  ).encode()
  with caplog.at_level(logging.INFO, logger="scrapy_extension.queue.strategies.time_wheel"):
    s.restore(blob)
  assert any("recovered 1" in r.message for r in caplog.records)


def test_timewheel_restore_skips_non_list_slots(caplog):
  s = _timewheel()
  blob = json.dumps(
    {
      "version": 1,
      "strategy": "time_wheel",
      "slots_flat": "not-a-list",  # wrong type
      "overflow": [],
    }
  ).encode()
  s.restore(blob)
  # slots_flat is iterated as chars of the string — each char fails base64
  # decode → skipped. Overflow stays empty.
  assert len(s._overflow) == 0


# ---------------------------------------------------------------------------
# RingBuffer — snapshot when buffer empty but dropped > 0
# ---------------------------------------------------------------------------


def test_ringbuffer_snapshot_when_buffer_empty_but_dropped():
  cm = MagicMock()
  cm.get_queue_backend.return_value = MagicMock()
  s = RingBufferQueueStrategy(cm, capacity=2, full_policy="drop_oldest")
  s.push("q", b"a")
  s.push("q", b"b")
  s.push("q", b"c")  # drop b"a", _dropped=1
  s.pop("q")         # buffer empty now (b left then popped... actually pop b)
  s.pop("q")         # pop c → buffer empty, dropped=1
  blob = s.snapshot()
  # Even though buffer is empty, dropped > 0 → snapshot is non-None.
  assert blob is not None
  data = json.loads(blob.decode())
  assert data["dropped"] == 1
  assert data["items"] == []


def test_ringbuffer_snapshot_items_not_list_skipped():
  cm = MagicMock()
  cm.get_queue_backend.return_value = MagicMock()
  s = RingBufferQueueStrategy(cm, capacity=5)
  blob = json.dumps(
    {
      "version": 1,
      "strategy": "ring_buffer",
      "capacity": 5,
      "items": "not-a-list",
      "dropped": 0,
    }
  ).encode()
  s.restore(blob)
  assert s.queue_len("q") == 0


def test_ringbuffer_restore_preserves_dropped_counter():
  cm = MagicMock()
  cm.get_queue_backend.return_value = MagicMock()
  s = RingBufferQueueStrategy(cm, capacity=5)
  item_b64 = base64.b64encode(b"x").decode()
  blob = json.dumps(
    {
      "version": 1,
      "strategy": "ring_buffer",
      "capacity": 5,
      "items": [item_b64],
      "dropped": 7,
    }
  ).encode()
  s.restore(blob)
  assert s._dropped == 7
