"""Delay queue strategy — items held until ready, then moved to the live queue (subsystem ②).

A task-queue type "beyond queue/stack/priority": an item pushed with a delay
is not poppable until ``ready_at = now + delay`` elapses. Crucially
direction-independent — held items bypass the (inconsistent) priority
contract entirely, so this strategy is correct regardless of backend
priority ordering. See the design spec for distributed-scope notes.
"""

from __future__ import annotations

__all__ = ["DelayQueueStrategy"]

import heapq
import itertools
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from scrapy_extension.queue.strategies.base import QueueStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


class DelayQueueStrategy(QueueStrategy):
  """Holds items for a per-item delay, then enqueues them to the live queue.

  Single-process holding (v1): a ``heapq`` of ``(ready_at, seq, item)``.
  ``push`` with ``delay > 0`` parks the item until ready; ``pop`` drains all
  due held items into the live queue first, then pops the live queue.
  ``delay == 0`` (or unset) goes straight to the live queue.

  Cross-worker holding is not supported in v1 — each process holds its own
  delayed items. See ``docs/superpowers/specs/2026-06-19-queue-semantics-design.md``.

  Attributes:
      _default_delay: Default delay when push omits an explicit delay.
      _clock: Monotonic clock callable (injectable for tests).
      _holding: Min-heap of (ready_at, seq, item).
      _seq: Tie-break counter for stable heap ordering.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    *,
    default_delay: float = 0.0,
    clock: Callable[[], float] = time.monotonic,
  ) -> None:
    """Initialize the delay strategy.

    Args:
        connection_manager: Connection manager providing the QueueBackend.
        default_delay: Default delay seconds when push omits ``delay``.
        clock: Monotonic clock callable returning seconds (injectable for tests).

    Raises:
        ValueError: If default_delay is negative.
    """
    super().__init__(connection_manager)
    if default_delay < 0:
      raise ValueError(f"default_delay must be >= 0, got {default_delay}")
    self._default_delay = default_delay
    self._clock = clock
    self._holding: list[tuple[float, int, bytes]] = []
    self._seq = itertools.count()

  def push(
    self, queue_name: str, item: bytes, *, priority: float = 0.0, delay: float = 0.0
  ) -> None:
    """Push an item, holding it until ready if a delay is set.

    Args:
        queue_name: The queue name.
        item: Serialized item bytes.
        priority: Priority for the live-queue push (used once drained).
        delay: Delay seconds; 0 falls back to ``default_delay``.
    """
    effective = delay if delay > 0 else self._default_delay
    if effective <= 0:
      self._connection_manager.get_queue_backend().push(queue_name, item, priority)
      return
    ready_at = self._clock() + effective
    heapq.heappush(self._holding, (ready_at, next(self._seq), item))

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Drain due held items, then pop the live queue.

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        The next ready item, or None if empty.
    """
    self._drain_ready(queue_name)
    return self._connection_manager.get_queue_backend().pop(queue_name, timeout)

  def _drain_ready(self, queue_name: str) -> None:
    """Move all due held items into the live queue.

    Args:
        queue_name: The queue name to drain into.
    """
    qb = self._connection_manager.get_queue_backend()
    now = self._clock()
    while self._holding and self._holding[0][0] <= now:
      _, _, item = heapq.heappop(self._holding)
      qb.push(queue_name, item)

  def queue_len(self, queue_name: str) -> int:
    """Return live-queue length plus held-item count.

    Args:
        queue_name: The queue name.

    Returns:
        Number of pending live items plus held (delayed) items.
    """
    return (
      self._connection_manager.get_queue_backend().queue_len(queue_name)
      + len(self._holding)
    )

  def clear(self, queue_name: str) -> None:
    """Clear the live queue and all held items.

    Args:
        queue_name: The queue name.
    """
    self._connection_manager.get_queue_backend().clear_queue(queue_name)
    self._holding.clear()
