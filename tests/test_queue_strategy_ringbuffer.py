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
from scrapy.http import Request

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BackendQueue
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


def test_restore_skips_non_base64_item():
  """Alphabet-invalid Base64 must not silently become an empty item."""
  s, _ = _strategy()
  blob = json.dumps(
    {
      "version": 1,
      "strategy": "ring_buffer",
      "capacity": 3,
      "items": ["%%%"],
      "dropped": 0,
    }
  ).encode()

  s.restore(blob)

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


@pytest.mark.parametrize("capacity", [True, 1.5])
def test_capacity_must_be_an_integer(capacity):
  with pytest.raises(ValueError, match="positive integer"):
    _strategy(capacity=capacity)


def test_invalid_full_policy_raises():
  with pytest.raises(ValueError, match="full_policy must be one of"):
    _strategy(full_policy="bogus")


# ---------------------------------------------------------------------------
# close releases blocked pushers
# ---------------------------------------------------------------------------


def test_close_wakes_blocked_pusher_with_queue_error(monkeypatch):
  """close() terminates a blocked push without needing a compensating pop."""
  s, _ = _strategy(capacity=1, full_policy="block")
  s.push("q", b"first")

  waiting = threading.Event()
  completed = threading.Event()
  errors: list[QueueError] = []

  original_wait = s._not_full.wait

  def observed_wait(timeout: float | None = None) -> bool:
    waiting.set()
    return original_wait(timeout)

  monkeypatch.setattr(s._not_full, "wait", observed_wait)

  def push_second():
    try:
      s.push("q", b"second")
    except QueueError as exc:
      errors.append(exc)
    finally:
      completed.set()

  t = threading.Thread(target=push_second, daemon=True)
  t.start()
  assert waiting.wait(timeout=2.0), "push should reach the blocking wait"

  s.close()

  assert completed.wait(timeout=2.0), "close should terminate the blocked push"
  t.join(timeout=2.0)
  assert not t.is_alive()
  assert len(errors) == 1
  assert "closed" in str(errors[0])
  assert s.queue_len("q") == 1


@pytest.mark.parametrize("full_policy", ["reject", "drop_oldest", "block"])
def test_push_after_close_raises_queue_error(full_policy):
  s, _ = _strategy(capacity=1, full_policy=full_policy)
  s.close()

  with pytest.raises(QueueError, match="closed"):
    s.push("q", b"item")

  assert s.queue_len("q") == 0


def test_backend_queue_reopens_closed_ring_buffer():
  cm = MagicMock(name="ConnectionManager")
  cm.get_storage_backend.side_effect = NotImplementedError
  s = RingBufferQueueStrategy(cm, capacity=1)

  first_queue = BackendQueue(cm, "q", queue_strategy=s)
  first_queue.close()
  with pytest.raises(QueueError, match="closed"):
    s.push("q", b"while-closed")

  second_queue = BackendQueue(cm, "q", queue_strategy=s)
  with pytest.raises(QueueError, match="clos"):
    first_queue.push(Request("https://example.com/stale-queue"))
  second_queue.push(Request("https://example.com/after-reopen"))

  restored = second_queue.pop()
  assert restored is not None
  assert restored.url == "https://example.com/after-reopen"


def test_reopen_does_not_admit_blocked_pusher_from_closed_lifecycle(monkeypatch):
  s, _ = _strategy(capacity=1, full_policy="block")
  s.push("q", b"first")

  waiting = threading.Event()
  resume_old_waiter = threading.Event()
  completed = threading.Event()
  errors: list[Exception] = []
  wait_calls = 0
  original_wait = s._not_full.wait

  def controlled_wait(timeout: float | None = None) -> bool:
    nonlocal wait_calls
    wait_calls += 1
    if wait_calls > 1:
      return original_wait(timeout)
    waiting.set()
    # Hold this waiter outside the Condition lock across close -> open.
    s._lock.release()
    try:
      if not resume_old_waiter.wait(timeout=2.0):
        raise AssertionError("old pusher was not resumed")
    finally:
      s._lock.acquire()
    return True

  monkeypatch.setattr(s._not_full, "wait", controlled_wait)

  def push_from_old_lifecycle():
    try:
      s.push("q", b"old-waiter")
    except Exception as exc:
      errors.append(exc)
    finally:
      completed.set()

  thread = threading.Thread(target=push_from_old_lifecycle, daemon=True)
  thread.start()
  assert waiting.wait(timeout=2.0), "old pusher should reach the blocking wait"

  try:
    s.close()
    s.open()
    assert s.pop("q") == b"first"
    s.push("q", b"new-lifecycle")
    resume_old_waiter.set()
    assert completed.wait(timeout=2.0), "old pusher should reject after reopen"
  finally:
    resume_old_waiter.set()
    if thread.is_alive():
      s.close()
    thread.join(timeout=2.0)

  assert not thread.is_alive()
  assert len(errors) == 1
  assert isinstance(errors[0], QueueError)
  assert "closed" in str(errors[0])
  assert s.pop("q") == b"new-lifecycle"


def test_restore_that_frees_capacity_wakes_blocked_pusher(monkeypatch):
  s, _ = _strategy(capacity=1, full_policy="block")
  s.push("q", b"old")

  waiting = threading.Event()
  completed = threading.Event()
  original_wait = s._not_full.wait

  def observed_wait(timeout: float | None = None) -> bool:
    waiting.set()
    return original_wait(timeout)

  monkeypatch.setattr(s._not_full, "wait", observed_wait)

  thread = threading.Thread(
    target=lambda: (s.push("q", b"new"), completed.set()), daemon=True
  )
  thread.start()
  assert waiting.wait(timeout=2.0), "push should reach the blocking wait"

  empty_snapshot = json.dumps(
    {
      "version": 1,
      "strategy": "ring_buffer",
      "capacity": 1,
      "items": [],
      "dropped": 1,
    }
  ).encode()
  s.restore(empty_snapshot)

  woke_from_restore = completed.wait(timeout=2.0)
  if not woke_from_restore:
    # Do not leak the intentionally blocked regression-test thread on failure.
    s.clear("q")
    assert completed.wait(timeout=2.0)
  thread.join(timeout=2.0)

  assert woke_from_restore, "restore freed a slot but did not wake the blocked push"
  assert not thread.is_alive()
  assert s.pop("q") == b"new"
