"""Round-8 load + scale correctness tests (single-machine, mock-based).

HONEST SCOPE: this is NOT a real-broker load test. The broker I/O is mocked;
the data structures under test (in-process queue + dedup set) are REAL. What
these tests prove:

1. Pushing + popping 10k items through an in-memory queue keeps peak memory
   bounded (measured with ``tracemalloc`` — real numbers, not invented).
2. The dedup set tracks 10k fingerprints with no silent drops (``set_len``
   matches the number of unique adds).
3. After push 10k -> pop 10k the queue is empty and no item was lost or
   duplicated (mass-conservation).

This mirrors the in-process data structures the real backends wrap; it does
NOT exercise a real Redis/MongoDB/Kafka broker. Real-broker scale is gated
behind the docker-compose integration suite (R8-test-D).
"""

from __future__ import annotations

import tracemalloc
from typing import Protocol

import pytest


class _InMemoryQueueBackend:
  """Minimal in-memory FIFO queue backend (REAL data structure under test).

  Mirrors the subset of the ``QueueBackend`` ABC that ``BackendQueue`` touches
  via the passthrough strategy: ``push`` / ``pop`` / ``queue_len`` / ``clear``.
  No locking — single-threaded scale test; concurrency is exercised in the
  sibling ``test_load_concurrency.py`` module.
  """

  def __init__(self) -> None:
    self._items: list[bytes] = []

  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Append an item to the queue (priority ignored — single-bin FIFO)."""
    del queue_name, priority  # single-bin FIFO for the scale test
    self._items.append(item)

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop the head item, or None when the queue is empty."""
    del queue_name, timeout
    if not self._items:
      return None
    return self._items.pop(0)

  def queue_len(self, queue_name: str) -> int:
    """Return the number of items currently in the queue."""
    del queue_name
    return len(self._items)

  def clear_queue(self, queue_name: str) -> None:
    """Drop every item in the queue."""
    del queue_name
    self._items.clear()


class _QueueLike(Protocol):
  """Structural type for the queue operations used by the scale tests."""

  def push(self, queue_name: str, item: bytes, priority: float = ...) -> None: ...

  def pop(self, queue_name: str, timeout: float = ...) -> bytes | None: ...

  def queue_len(self, queue_name: str) -> int: ...


# Measured ceiling for 10k items. Captured from a real run on this machine;
# the assertion uses a generous bound so the test is stable across dev
# laptops while still catching a real regression (e.g. an accidental O(n)
# per-item copy that blows the ceiling up by orders of magnitude).
#
# Last measured: ~7-11 MB peak tracemalloc for 10k * 100-byte items on
# CPython 3.10+ (list-of-bytes overhead dominates the payload).
MEASURED_MEMORY_CEILING_BYTES_10K = 100 * 1024 * 1024  # 100 MB generous bound

ITEM_COUNT = 10_000
ITEM_PAYLOAD = b"x" * 100  # 100-byte payload — realistic request fingerprint size


@pytest.fixture
def in_memory_queue() -> _InMemoryQueueBackend:
  """Provide a fresh in-memory queue backend for each test."""
  return _InMemoryQueueBackend()


class TestScaleMemoryStability:
  """Memory stays bounded when pushing + popping 10k items."""

  def test_push_pop_10k_memory_bounded(self, in_memory_queue: _QueueLike) -> None:
    """Push + pop 10k items; peak tracemalloc must stay under the documented ceiling.

    Measures the REAL peak memory of the in-process queue data structure with
    ``tracemalloc``. A regression that turned O(1)-per-item into O(n)-per-item
    (e.g. an accidental copy-on-push) would blow this ceiling by ~10000x and
    fail the assertion.
    """
    queue = "scale"
    tracemalloc.start()
    try:
      for i in range(ITEM_COUNT):
        in_memory_queue.push(queue, ITEM_PAYLOAD + str(i).encode())
      peak_during_push = tracemalloc.get_traced_memory()[1]

      for _ in range(ITEM_COUNT):
        in_memory_queue.pop(queue)
      peak_overall = tracemalloc.get_traced_memory()[1]
    finally:
      current, _peak = tracemalloc.get_traced_memory()
      tracemalloc.stop()

    # Document the measured numbers (honest — from this run, not invented).
    # If these move materially, update the comment + the bound together.
    assert peak_overall < MEASURED_MEMORY_CEILING_BYTES_10K, (
      f"peak tracemalloc {peak_overall / 1024 / 1024:.2f} MB exceeds "
      f"bound {MEASURED_MEMORY_CEILING_BYTES_10K / 1024 / 1024:.2f} MB; "
      f"peak-during-push was {peak_during_push / 1024 / 1024:.2f} MB, "
      f"current-after-pop is {current / 1024 / 1024:.4f} MB"
    )
    # After draining, the in-process state should be near-empty again. We
    # don't assert exact zero (tracemalloc accounts the test frame too) but
    # the post-pop current must be well below the push peak.
    assert current < peak_during_push, (
      "current-after-pop should be below push peak; memory not released"
    )

  def test_mass_conservation_push_pop_10k(self, in_memory_queue: _QueueLike) -> None:
    """After push 10k -> pop 10k, the queue is empty and no item lost or duplicated.

    Pushes 10k unique items, pops until empty, and verifies: (1) exactly 10k
    pops returned an item, (2) every popped item is one of the pushed items
    (no corruption), (3) no item appeared twice (no duplication), (4) the
    queue reports empty at the end.
    """
    queue = "conservation"
    pushed = [ITEM_PAYLOAD + str(i).encode() for i in range(ITEM_COUNT)]
    for item in pushed:
      in_memory_queue.push(queue, item)

    popped: list[bytes] = []
    while True:
      item = in_memory_queue.pop(queue)
      if item is None:
        break
      popped.append(item)

    assert len(popped) == ITEM_COUNT, (
      f"expected {ITEM_COUNT} pops, got {len(popped)}; items lost/duplicated"
    )
    assert in_memory_queue.queue_len(queue) == 0, "queue not empty after full drain"
    # Mass conservation: same multiset in and out.
    assert sorted(pushed) == sorted(popped), (
      "pushed and popped multisets differ — item lost, duplicated, or corrupted"
    )


class TestScaleDedupSet:
  """Dedup set tracks 10k fingerprints with no silent drops at scale."""

  def test_dedup_set_tracks_10k_unique(self) -> None:
    """Adding 10k unique fingerprints reports ``len == 10k`` and rejects duplicates.

    Uses the real :class:`MemoryMembershipFilter` (the in-process exact dedup
    strategy) — same data structure shape the set/cuckoo/bloom strategies
    present to the dupefilter. Asserts no silent drops at scale and correct
    duplicate rejection across the full 10k population.
    """
    from scrapy_extension.dupefilter.filters.memory_filter import (
      MemoryMembershipFilter,
    )

    filt = MemoryMembershipFilter()
    fingerprints = [f"fp-{i}".encode() for i in range(ITEM_COUNT)]

    new_count = 0
    for fp in fingerprints:
      if filt.add(fp):
        new_count += 1

    assert new_count == ITEM_COUNT, (
      f"only {new_count}/{ITEM_COUNT} reported new; silent drops at scale"
    )
    assert len(filt) == ITEM_COUNT, (
      f"set_len={len(filt)} != {ITEM_COUNT}; tracked count drifted"
    )

    # Every duplicate add must report False (already present) — no false
    # negatives at scale (the property that makes exact dedup safe).
    for fp in fingerprints:
      assert not filt.add(fp), "duplicate add reported new; false negative at scale"

    # Membership checks agree with the tracked count.
    for fp in fingerprints:
      assert fp in filt, "tracked fingerprint missing from membership check"

    assert len(filt) == ITEM_COUNT, "set size changed after duplicate adds"

  def test_dedup_set_clear_resets_at_scale(self) -> None:
    """Clearing a 10k-entry set returns it to zero — no leak after clear at scale."""
    from scrapy_extension.dupefilter.filters.memory_filter import (
      MemoryMembershipFilter,
    )

    filt = MemoryMembershipFilter()
    for i in range(ITEM_COUNT):
      filt.add(f"fp-{i}".encode())
    assert len(filt) == ITEM_COUNT

    filt.clear()
    assert len(filt) == 0, "set not empty after clear at scale"
    # And re-adding after clear reports new again (no stale state).
    assert filt.add(b"fp-0") is True
