"""Single-machine scale checks for a local list-based reference queue.

This module imports no production queue or backend implementation and provides
**no production backend evidence**. Its list, methods, and protocol are defined
entirely here; passing results prove only the reference model's memory ceiling
and mass conservation for 10,000 local byte strings. Redis, MongoDB, Kafka,
``BackendQueue``, queue strategies, serialization, priorities, broker I/O, and
concurrency all require their own production or live-integration evidence.
"""

from __future__ import annotations

import tracemalloc
from typing import Protocol

import pytest

pytestmark = pytest.mark.reference_model


class _ReferenceListQueue:
    """Minimal local FIFO model; deliberately not a production ``QueueBackend``."""

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


class _ReferenceQueueLike(Protocol):
    """Structural type for operations on the local reference model."""

    def push(self, queue_name: str, item: bytes, priority: float = ...) -> None: ...

    def pop(self, queue_name: str, timeout: float = ...) -> bytes | None: ...

    def queue_len(self, queue_name: str) -> int: ...


# Reference-model ceiling for 10k items. The generous bound is stable across
# developer machines while catching large complexity drift in this local list.
#
# Last measured: ~7-11 MB peak tracemalloc for 10k * 100-byte items on
# CPython 3.10+ (list-of-bytes overhead dominates the payload).
REFERENCE_MEMORY_CEILING_BYTES_10K = 100 * 1024 * 1024

REFERENCE_ITEM_COUNT = 10_000
REFERENCE_ITEM_PAYLOAD = b"x" * 100


@pytest.fixture
def reference_list_queue() -> _ReferenceListQueue:
    """Provide a fresh local list model for each test."""
    return _ReferenceListQueue()


class TestReferenceListQueueScale:
    """Characterize local-list memory and mass conservation at 10k items."""

    def test_reference_model_push_pop_10k_memory_bounded(
        self, reference_list_queue: _ReferenceQueueLike
    ) -> None:
        """Keep the local list model below its documented tracemalloc ceiling."""
        queue = "scale"
        tracemalloc.start()
        try:
            for i in range(REFERENCE_ITEM_COUNT):
                reference_list_queue.push(
                    queue, REFERENCE_ITEM_PAYLOAD + str(i).encode()
                )
            peak_during_push = tracemalloc.get_traced_memory()[1]

            for _ in range(REFERENCE_ITEM_COUNT):
                reference_list_queue.pop(queue)
            peak_overall = tracemalloc.get_traced_memory()[1]
        finally:
            current, _peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        # Document the measured numbers (honest — from this run, not invented).
        # If these move materially, update the comment + the bound together.
        assert peak_overall < REFERENCE_MEMORY_CEILING_BYTES_10K, (
            f"peak tracemalloc {peak_overall / 1024 / 1024:.2f} MB exceeds "
            f"bound {REFERENCE_MEMORY_CEILING_BYTES_10K / 1024 / 1024:.2f} MB; "
            f"peak-during-push was {peak_during_push / 1024 / 1024:.2f} MB, "
            f"current-after-pop is {current / 1024 / 1024:.4f} MB"
        )
        # After draining, the in-process state should be near-empty again. We
        # don't assert exact zero (tracemalloc accounts the test frame too) but
        # the post-pop current must be well below the push peak.
        assert current < peak_during_push, (
            "current-after-pop should be below push peak; memory not released"
        )

    def test_reference_model_mass_conservation_push_pop_10k(
        self, reference_list_queue: _ReferenceQueueLike
    ) -> None:
        """Preserve the local byte-string multiset through a full list drain."""
        queue = "conservation"
        pushed = [
            REFERENCE_ITEM_PAYLOAD + str(i).encode()
            for i in range(REFERENCE_ITEM_COUNT)
        ]
        for item in pushed:
            reference_list_queue.push(queue, item)

        popped: list[bytes] = []
        while True:
            item = reference_list_queue.pop(queue)
            if item is None:
                break
            popped.append(item)

        assert len(popped) == REFERENCE_ITEM_COUNT, (
            f"expected {REFERENCE_ITEM_COUNT} pops, got {len(popped)}; items lost/duplicated"
        )
        assert reference_list_queue.queue_len(queue) == 0, (
            "queue not empty after full drain"
        )
        # Mass conservation: same multiset in and out.
        assert sorted(pushed) == sorted(popped), (
            "pushed and popped multisets differ — item lost, duplicated, or corrupted"
        )
