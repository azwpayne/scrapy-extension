"""Delay queue strategy — items held until ready, then moved to the live queue (subsystem ②).

A task-queue type "beyond queue/stack/priority": an item pushed with a delay
is not poppable until ``ready_at = now + delay`` elapses. Crucially
direction-independent — held items bypass the (inconsistent) priority
contract entirely, so this strategy is correct regardless of backend
priority ordering. See the design spec for distributed-scope notes.
"""

from __future__ import annotations

__all__ = ["DEFAULT_DELAY_MAX_HELD", "DelayQueueStrategy"]

import heapq
import itertools
import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from scrapy_extension.queue.strategies.base import QueueStrategy

logger = logging.getLogger(__name__)

# SPEC-round8-tier1 U5 — OOM prevention default for the in-process holding
# heap. The heap grows unboundedly when push-rate outpaces ready-rate (e.g. a
# burst of long-delay items). 100k is a soft cap: warn-once, never refuse —
# refusing would silently drop delayed items. Distributed-delay (U10) is the
# long-term fix; until then this surfaces the growth risk to operators.
DEFAULT_DELAY_MAX_HELD: int = 100_000

# Module-level cache so the over-cap warning fires once per process even when
# many strategies are constructed. Mirrors dupefilter/filters/factory.py
# `_warned`. Tests reset this to verify the warn-once contract from a clean
# slate.
_over_cap_warned: bool = False

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
      _max_held: Soft cap on ``_holding`` size; non-positive disables the
          warning. Defaults to :data:`DEFAULT_DELAY_MAX_HELD` (100_000).
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    *,
    default_delay: float = 0.0,
    clock: Callable[[], float] = time.monotonic,
    max_held: int = DEFAULT_DELAY_MAX_HELD,
  ) -> None:
    """Initialize the delay strategy.

    Args:
        connection_manager: Connection manager providing the QueueBackend.
        default_delay: Default delay seconds when push omits ``delay``.
        clock: Monotonic clock callable returning seconds (injectable for tests).
        max_held: Soft cap on the in-process holding heap. When the heap
            exceeds this size a one-time WARNING fires (warn-only — items are
            never refused, since dropping a delayed item would silently lose
            data). Defaults to :data:`DEFAULT_DELAY_MAX_HELD` (100_000) for
            OOM prevention. Pass ``<= 0`` to disable the warning (advanced
            opt-out — accepts the unbounded-growth risk).

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
    self._max_held = max_held

  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Push an item, holding it until ready if a delay is set.

    Args:
        queue_name: The queue name.
        item: Serialized item bytes.
        priority: Priority for the live-queue push (used once drained).
        delay: Delay seconds; 0 falls back to ``default_delay``.
        source: Ignored (delay strategy holds by ready-time, not source).
    """
    del source
    effective = delay if delay > 0 else self._default_delay
    if effective <= 0:
      self._connection_manager.get_queue_backend().push(queue_name, item, priority)
      return
    ready_at = self._clock() + effective
    heapq.heappush(self._holding, (ready_at, next(self._seq), item))
    self._warn_over_cap_once()

  def _warn_over_cap_once(self) -> None:
    """Emit a one-time per-process WARNING when the holding heap exceeds cap.

    The holding heap grows unboundedly whenever push-rate outpaces
    ready-rate (e.g. a burst of long-delay items). The cap is SOFT: this
    never refuses the push — dropping a delayed item would silently lose
    data, which is worse than loud memory pressure. Idempotent via the
    module-level ``_over_cap_warned`` flag so a multi-spider process does
    not spam the log. Points at the distributed-delay roadmap (U10).
    """
    global _over_cap_warned
    if _over_cap_warned:
      return
    if self._max_held <= 0:
      return
    if len(self._holding) <= self._max_held:
      return
    _over_cap_warned = True
    logger.warning(
      "DelayQueueStrategy holding heap exceeded max_held=%d items "
      "(now=%d). The in-process heap grows unboundedly when push-rate "
      "outpaces ready-rate; long bursts of long-delay items can exhaust "
      "memory. This is a SOFT cap — items are NOT dropped. Raise "
      "max_held, drain sooner, or wait for distributed-delay support "
      "(roadmap U10).",
      self._max_held,
      len(self._holding),
    )

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

    Unlike :meth:`close`, this does NOT warn about discarded held items:
    ``clear`` is an explicit flush requested by the caller, so silent
    discard is the intended contract. ``close`` warns because held items
    present at shutdown indicate unexpected loss.

    Args:
        queue_name: The queue name.
    """
    self._connection_manager.get_queue_backend().clear_queue(queue_name)
    self._holding.clear()

  def close(self) -> None:
    """Release resources, warning about any held (delayed) items.

    Held items live in-process, so any still-pending delayed items are
    lost on close/restart. Make that loss non-silent: emit a WARNING with
    the discarded count, then clear the holding heap.

    If ``_holding`` is empty, this is a quiet no-op (clears nothing).
    """
    held = len(self._holding)
    if held > 0:
      logger.warning(
        "DelayQueueStrategy close: discarding %d held delayed item(s) "
        "from the in-process holding queue; these delayed items are lost "
        "on close/restart (non-silent data loss).",
        held,
      )
    self._holding.clear()
