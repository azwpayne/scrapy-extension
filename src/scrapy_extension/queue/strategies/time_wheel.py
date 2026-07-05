"""Time-wheel queue strategy — O(1) hashed timing wheel + overflow heap (subsystem ②).

A task-queue type for workloads with **many short-delay items**: a binary heap
(:class:`~scrapy_extension.queue.strategies.delay.DelayQueueStrategy`) costs
``O(log n)`` per push/pop; a hashed timing wheel is ``O(1)`` per tick when
delays are evenly distributed.

Layout:

- Primary wheel of ``wheel_size`` slots, each slot = ``1 / ticks_per_second``
  seconds. Wheel duration = ``wheel_size / ticks_per_second`` (default 60s).
- Overflow heap (``(ready_at, seq, item, priority)``) for delays longer than
  one wheel rotation — graceful degradation to Delay's behavior.

``push``: ``delay ≤ wheel_duration`` → slot index
``int(ready_at * ticks_per_second) % wheel_size``; ``delay > wheel_duration`` →
overflow heap. ``delay ≤ 0`` → straight to the live queue.

``pop``: advance the wheel by draining every slot from ``_last_tick+1`` to
``now_tick`` (capped to one full rotation), then drain due overflow, then pop
the live queue.

Single-process holding (v1); ``snapshot``/``restore`` preserve wheel + overflow
for restart recovery (initiative #3), mirroring the Delay pattern.
"""

from __future__ import annotations

__all__ = [
  "DEFAULT_TICKS_PER_SECOND",
  "DEFAULT_WHEEL_SIZE",
  "TimeWheelQueueStrategy",
]

import base64
import heapq
import itertools
import json
import logging
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING

from scrapy_extension.queue.strategies.base import QueueStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)

#: Default slot count (one slot per second → 60s wheel).
DEFAULT_WHEEL_SIZE: int = 60
#: Default slot granularity (1 slot per second).
DEFAULT_TICKS_PER_SECOND: float = 1.0


class TimeWheelQueueStrategy(QueueStrategy):
  """O(1) hashed timing wheel with overflow heap for long delays.

  Attributes:
      _wheel_size: Number of slots in the primary wheel.
      _ticks_per_second: Slot granularity.
      _wheel_duration: ``wheel_size / ticks_per_second`` — max delay the
          primary wheel holds without overflow.
      _default_delay: Default delay seconds when push omits ``delay``.
      _clock: Monotonic clock callable (injectable for tests).
      _wheel: ``[deque((item, priority), ...)]`` per slot.
      _overflow: Min-heap of ``(ready_at, seq, item, priority)`` for long delays.
      _seq: Tie-break counter for stable heap ordering.
      _last_tick: Tick up to which the wheel has been drained.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    *,
    wheel_size: int = DEFAULT_WHEEL_SIZE,
    ticks_per_second: float = DEFAULT_TICKS_PER_SECOND,
    default_delay: float = 0.0,
    clock: Callable[[], float] = time.monotonic,
  ) -> None:
    """Initialize the time-wheel strategy.

    Args:
        connection_manager: Connection manager providing the QueueBackend.
        wheel_size: Number of slots in the primary wheel (default 60).
        ticks_per_second: Slot granularity (default 1.0 → 1 slot/sec).
        default_delay: Default delay seconds when push omits ``delay``.
        clock: Monotonic clock callable returning seconds (injectable for tests).

    Raises:
        ValueError: If ``wheel_size < 1``, ``ticks_per_second <= 0``, or
            ``default_delay < 0``.
    """
    super().__init__(connection_manager)
    if wheel_size < 1:
      raise ValueError(f"wheel_size must be >= 1, got {wheel_size}")
    if ticks_per_second <= 0:
      raise ValueError(f"ticks_per_second must be > 0, got {ticks_per_second}")
    if default_delay < 0:
      raise ValueError(f"default_delay must be >= 0, got {default_delay}")
    self._wheel_size = wheel_size
    self._ticks_per_second = ticks_per_second
    self._wheel_duration = wheel_size / ticks_per_second
    self._default_delay = default_delay
    self._clock = clock
    self._wheel: list[deque[tuple[bytes, float]]] = [
      deque() for _ in range(wheel_size)
    ]
    self._overflow: list[tuple[float, int, bytes, float]] = []
    self._seq = itertools.count()
    self._last_tick = int(self._clock() * ticks_per_second)

  # ------------------------------------------------------------------ push

  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Push with a delay; short delays → wheel slot, long delays → overflow.

    Args:
        queue_name: The queue name.
        item: Serialized item bytes.
        priority: Priority for the live-queue push (preserved across the delay).
        delay: Delay seconds; 0 falls back to ``default_delay``.
        source: Ignored (time-wheel routes by ready-time, not source).
    """
    del source
    effective = delay if delay > 0 else self._default_delay
    if effective <= 0:
      self._connection_manager.get_queue_backend().push(queue_name, item, priority)
      return
    ready_at = self._clock() + effective
    if effective <= self._wheel_duration:
      slot = int(ready_at * self._ticks_per_second) % self._wheel_size
      self._wheel[slot].append((item, priority))
    else:
      heapq.heappush(self._overflow, (ready_at, next(self._seq), item, priority))

  # ------------------------------------------------------------------ pop

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Drain due wheel + overflow items, then pop the live queue.

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        The next ready item, or None if empty.
    """
    self._drain_ready(queue_name)
    return self._connection_manager.get_queue_backend().pop(queue_name, timeout)

  def _drain_ready(self, queue_name: str) -> None:
    """Move all due held items (wheel + overflow) into the live queue.

    Drains every wheel slot between ``_last_tick+1`` and ``now_tick``,
    capped to one full rotation (so a long idle doesn't loop more than
    ``wheel_size`` times). Then drains overflow items whose ``ready_at``
    has passed.

    Args:
        queue_name: The queue name to drain into.
    """
    qb = self._connection_manager.get_queue_backend()
    now = self._clock()
    now_tick = int(now * self._ticks_per_second)
    # Cap the drain span to one full rotation — every slot is covered once.
    span = max(0, min(now_tick - self._last_tick, self._wheel_size))
    for i in range(span):
      tick = self._last_tick + 1 + i
      slot = tick % self._wheel_size
      dq = self._wheel[slot]
      while dq:
        item, priority = dq.popleft()
        qb.push(queue_name, item, priority)
    # Drain due overflow.
    while self._overflow and self._overflow[0][0] <= now:
      _, _, item, priority = heapq.heappop(self._overflow)
      qb.push(queue_name, item, priority)
    self._last_tick = now_tick

  # ------------------------------------------------------------------ len/clear

  def queue_len(self, queue_name: str) -> int:
    """Live-queue length + held wheel items + held overflow items."""
    live = self._connection_manager.get_queue_backend().queue_len(queue_name)
    held_wheel = sum(len(slot) for slot in self._wheel)
    return live + held_wheel + len(self._overflow)

  def clear(self, queue_name: str) -> None:
    """Clear live queue, all wheel slots, and the overflow heap."""
    self._connection_manager.get_queue_backend().clear_queue(queue_name)
    for slot in self._wheel:
      slot.clear()
    self._overflow.clear()

  def close(self) -> None:
    """Warn about any held items being discarded at shutdown."""
    held = sum(len(slot) for slot in self._wheel) + len(self._overflow)
    if held > 0:
      logger.warning(
        "TimeWheelQueueStrategy close: discarding %d held delayed item(s) "
        "from the in-process wheel + overflow; these are lost on close/restart "
        "(non-silent data loss).",
        held,
      )
    for slot in self._wheel:
      slot.clear()
    self._overflow.clear()

  # ------------------------------------------------------------------ snapshot/restore

  def snapshot(self) -> bytes | None:
    """Serialize the wheel + overflow for restart recovery.

    Returns ``None`` when empty. Otherwise a versioned JSON blob:
    ``{"version":1,"strategy":"time_wheel","wheel_size":..,"slots_flat":[
    {"item_b64":..,"priority":..},...],"overflow":[{ready_at,item_b64,priority},...]}``.

    ``slots_flat`` collapses the per-slot deques into one flat list (slot
    identity is NOT preserved — on restore, items are re-pushed into the live
    queue if due, or back into the wheel by recomputing the slot from a stored
    ``ready_at``).
    """
    held_wheel = sum(len(slot) for slot in self._wheel)
    if held_wheel == 0 and not self._overflow:
      return None
    slots_flat = [
      {"item_b64": base64.b64encode(item).decode("ascii"), "priority": priority}
      for slot in self._wheel
      for item, priority in slot
    ]
    overflow = [
      {
        "ready_at": ready_at,
        "item_b64": base64.b64encode(item).decode("ascii"),
        "priority": priority,
      }
      for ready_at, _seq, item, priority in self._overflow
    ]
    return json.dumps(
      {
        "version": 1,
        "strategy": "time_wheel",
        "wheel_size": self._wheel_size,
        "slots_flat": slots_flat,
        "overflow": overflow,
      }
    ).encode("utf-8")

  def restore(self, state: bytes | None) -> None:
    """Re-populate the wheel + overflow from a prior :meth:`snapshot`.

    Items in ``slots_flat`` are restored as if newly pushed with the snapshot's
    implied ready-time (we re-place them in the wheel by their original slot
    bucket — but since slots_flat doesn't carry ready_at, we restore them as
    overflow with the current clock so they drain on the next pop). Corrupt or
    unknown-format state is logged + skipped.

    Args:
        state: The bytes blob from a prior :meth:`snapshot`, or ``None``.
    """
    if not state:
      return
    try:
      data = json.loads(state.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
      logger.warning(
        "TimeWheelQueueStrategy restore: corrupt snapshot (%s); starting clean.", e
      )
      return
    if (
      not isinstance(data, dict)
      or data.get("strategy") != "time_wheel"
      or data.get("version") != 1
    ):
      logger.warning(
        "TimeWheelQueueStrategy restore: unknown snapshot format; starting clean."
      )
      return
    now = self._clock()
    recovered = 0
    # Wheel items have no ready_at in the snapshot — re-place as due overflow
    # so they drain on the next pop (preserves "must not lose" contract).
    for entry in data.get("slots_flat", []) or []:
      try:
        item = base64.b64decode(entry["item_b64"])
        priority = float(entry["priority"])
      except (KeyError, TypeError, ValueError) as e:
        logger.warning(
          "TimeWheelQueueStrategy restore: skipping malformed wheel entry (%s).", e
        )
        continue
      heapq.heappush(self._overflow, (now, next(self._seq), item, priority))
      recovered += 1
    for entry in data.get("overflow", []) or []:
      try:
        ready_at = float(entry["ready_at"])
        item = base64.b64decode(entry["item_b64"])
        priority = float(entry["priority"])
      except (KeyError, TypeError, ValueError) as e:
        logger.warning(
          "TimeWheelQueueStrategy restore: skipping malformed overflow entry (%s).", e
        )
        continue
      heapq.heappush(self._overflow, (ready_at, next(self._seq), item, priority))
      recovered += 1
    if recovered:
      logger.info(
        "TimeWheelQueueStrategy restore: recovered %d held item(s) from snapshot.",
        recovered,
      )
