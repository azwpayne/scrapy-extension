"""Tests for RingBufferQueueStrategy — bounded in-process circular buffer.

Covers:
- push/pop round-trip with FIFO order
- capacity bound enforced
- full_policy=reject → QueueError on overflow
- full_policy=drop_oldest → overwrites oldest, increments _dropped
- full_policy=block → push blocks until pop frees a slot (threaded test)
- pop returns None when empty
- queue_len / clear operate on the buffer (backend is unused)
- snapshot/restore round-trip (versioned JSON, base64 items, dropped count)
- restore skips corrupt/unknown-format
- config validation: capacity >= 1, full_policy in allowed set
- ignores the ConnectionManager's backend (buffer IS the storage)
"""

from __future__ import annotations

import base64
import json
import threading
from unittest.mock import MagicMock

import pytest

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.strategies.ring_buffer import RingBufferQueueStrategy


def _strategy(
  *,
  capacity: int = 3,
  full_policy: str = "reject",
) -> tuple[RingBufferQueueStrategy, MagicMock]:
  cm = MagicMock(name="ConnectionManager")
  qb = MagicMock(name="QueueBackend")
  cm.get_queue_backend.return_value = qb
  return RingBufferQueueStrategy(cm, capacity=capacity, full_policy=full_policy), qb


# ---------------------------------------------------------------------------
# push / pop round-trip
# ---------------------------------------------------------------------------


def test_push_then_pop_returns_item():
  s, _ = _strategy()
  s.push("q", b"x")
  assert s.pop("q") == b"x"


def test_fifo_order_preserved():
  s, _ = _strategy(capacity=5)
  for i in range(5):
    s.push("q", str(i).encode())
  for i in range(5):
    assert s.pop("q") == str(i).encode()


def test_pop_empty_returns_none():
  s, _ = _strategy()
  assert s.pop("q") is None


def test_push_ignores_backend_queuebackend():
  """RingBuffer is in-process — push MUST NOT touch the backend QueueBackend."""
  s, qb = _strategy()
  s.push("q", b"x")
  qb.push.assert_not_called()


def test_pop_ignores_backend_queuebackend():
  s, qb = _strategy()
  s.push("q", b"x")
  s.pop("q")
  qb.pop.assert_not_called()


# ---------------------------------------------------------------------------
# capacity + full_policy
# ---------------------------------------------------------------------------


def test_push_below_capacity_succeeds():
  s, _ = _strategy(capacity=3)
  s.push("q", b"a")
  s.push("q", b"b")
  s.push("q", b"c")  # at capacity, no overflow yet
  assert s.queue_len("q") == 3


def test_push_full_reject_raises_queue_error():
  s, _ = _strategy(capacity=2, full_policy="reject")
  s.push("q", b"a")
  s.push("q", b"b")
  with pytest.raises(QueueError, match="ring buffer full"):
    s.push("q", b"c")
  # Buffer state unchanged after the rejected push.
  assert s.queue_len("q") == 2


def test_push_full_drop_oldest_overwrites_and_counts():
  s, _ = _strategy(capacity=2, full_policy="drop_oldest")
  s.push("q", b"a")
  s.push("q", b"b")
  s.push("q", b"c")  # overwrites b"a"
  assert s._dropped == 1
  # Oldest (a) is gone; b and c remain in FIFO order.
  assert s.pop("q") == b"b"
  assert s.pop("q") == b"c"


def test_push_full_block_waits_for_pop():
  """full_policy=block: push blocks until a pop frees a slot."""
  s, _ = _strategy(capacity=1, full_policy="block")
  s.push("q", b"first")  # buffer full

  completed = threading.Event()

  def push_second():
    s.push("q", b"second")  # blocks until pop frees a slot
    completed.set()

  t = threading.Thread(target=push_second)
  t.start()
  # The blocked push has not completed yet.
  assert not completed.wait(timeout=0.1), "push should be blocked"
  # Pop frees a slot → the blocked push unblocks.
  assert s.pop("q") == b"first"
  assert completed.wait(timeout=2.0), "push should complete after pop"
  t.join(timeout=2.0)
  # The second item is now in the buffer.
  assert s.pop("q") == b"second"


# ---------------------------------------------------------------------------
# queue_len, clear
# ---------------------------------------------------------------------------


def test_queue_len_reflects_buffer_size():
  s, _ = _strategy(capacity=5)
  s.push("q", b"a")
  s.push("q", b"b")
  assert s.queue_len("q") == 2


def test_clear_empties_buffer():
  s, _ = _strategy(capacity=5)
  s.push("q", b"a")
  s.push("q", b"b")
  s.clear("q")
  assert s.queue_len("q") == 0
  assert s.pop("q") is None


def test_clear_does_not_touch_backend():
  s, qb = _strategy()
  s.push("q", b"a")
  s.clear("q")
  qb.clear_queue.assert_not_called()


# ---------------------------------------------------------------------------
# snapshot / restore
# ---------------------------------------------------------------------------


def test_snapshot_empty_returns_none():
  s, _ = _strategy()
  assert s.snapshot() is None


def test_snapshot_serializes_buffer_and_dropped():
  s, _ = _strategy(capacity=2, full_policy="drop_oldest")
  s.push("q", b"a")
  s.push("q", b"b")
  s.push("q", b"c")  # drop b"a", _dropped=1
  blob = s.snapshot()
  assert blob is not None
  data = json.loads(blob.decode())
  assert data["version"] == 1
  assert data["strategy"] == "ring_buffer"
  assert data["capacity"] == 2
  assert data["dropped"] == 1
  items = [base64.b64decode(i).decode() for i in data["items"]]
  assert items == ["b", "c"]


def test_restore_round_trip_rebuilds_buffer():
  s1, _ = _strategy(capacity=5)
  s1.push("q", b"x")
  s1.push("q", b"y")
  blob = s1.snapshot()

  s2, _ = _strategy(capacity=5)
  s2.restore(blob)
  assert s2.queue_len("q") == 2
  assert s2.pop("q") == b"x"
  assert s2.pop("q") == b"y"


def test_restore_none_is_noop():
  s, _ = _strategy()
  s.restore(None)
  s.restore(b"")
  assert s.queue_len("q") == 0


def test_restore_corrupt_skipped():
  s, _ = _strategy()
  s.restore(b"\x00 not json \x00")
  assert s.queue_len("q") == 0


def test_restore_unknown_format_skipped():
  s, _ = _strategy()
  blob = json.dumps({"version": 99, "strategy": "other"}).encode()
  s.restore(blob)
  assert s.queue_len("q") == 0


def test_restore_truncates_oldest_when_over_capacity():
  """Restoring more items than capacity truncates the oldest (logged)."""
  s1, _ = _strategy(capacity=5)
  for i in range(5):
    s1.push("q", str(i).encode())
  blob = s1.snapshot()

  s2, _ = _strategy(capacity=3)  # smaller capacity
  s2.restore(blob)
  # Only the 3 newest items fit (truncates oldest 2).
  assert s2.queue_len("q") == 3
  assert s2.pop("q") == b"2"
  assert s2.pop("q") == b"3"
  assert s2.pop("q") == b"4"


# ---------------------------------------------------------------------------
# config validation
# ---------------------------------------------------------------------------


def test_capacity_zero_raises():
  with pytest.raises(ValueError, match="capacity must be >= 1"):
    _strategy(capacity=0)


def test_capacity_negative_raises():
  with pytest.raises(ValueError, match="capacity must be >= 1"):
    _strategy(capacity=-1)


def test_invalid_full_policy_raises():
  with pytest.raises(ValueError, match="full_policy must be one of"):
    _strategy(full_policy="bogus")


# ---------------------------------------------------------------------------
# close releases blocked pushers
# ---------------------------------------------------------------------------


def test_close_does_not_crash_with_blocked_pusher():
  """close() acquires the lock; should not deadlock if a pusher is blocked."""
  s, _ = _strategy(capacity=1, full_policy="block")
  s.push("q", b"first")

  completed = threading.Event()

  def push_second():
    try:
      s.push("q", b"second")
    except Exception:
      pass
    completed.set()

  t = threading.Thread(target=push_second)
  t.start()
  assert not completed.wait(timeout=0.1)
  s.close()  # should not deadlock
  # The blocked pusher is still waiting — let's not leak the thread.
  s.pop("q")  # frees a slot so pusher can complete
  assert completed.wait(timeout=2.0)
  t.join(timeout=2.0)
