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
``ceil(ready_at * ticks_per_second) % wheel_size``; ``delay > wheel_duration`` →
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
  "MAX_WHEEL_SIZE",
  "TimeWheelQueueStrategy",
]

import base64
import binascii
import heapq
import itertools
import json
import logging
import math
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from scrapy_extension.queue.strategies.base import (
  QueueStrategy,
  _PreparedQueuePush,
  normalize_queue_timeout,
)

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)

#: Default slot count (one slot per second → 60s wheel).
DEFAULT_WHEEL_SIZE: int = 60
#: Default slot granularity (1 slot per second).
DEFAULT_TICKS_PER_SECOND: float = 1.0


def _finite_number(value: object, name: str) -> float:
  """Normalize a numeric input without accepting bool or non-finite values."""
  if isinstance(value, bool) or not isinstance(value, (int, float)):
    raise ValueError(f"{name} must be finite, got {value!r}")
  try:
    normalized = float(value)
  except (OverflowError, TypeError, ValueError) as e:
    raise ValueError(f"{name} must be finite, got {value!r}") from e
  if not math.isfinite(normalized):
    raise ValueError(f"{name} must be finite, got {value!r}")
  return normalized


#: Hard allocation guard: every slot eagerly owns a deque at construction.
MAX_WHEEL_SIZE: int = 100_000


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
      _state_lock: Serializes every compound transition over held state.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    *,
    wheel_size: int = DEFAULT_WHEEL_SIZE,
    ticks_per_second: float = DEFAULT_TICKS_PER_SECOND,
    default_delay: float = 0.0,
    clock: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], float] = time.time,
  ) -> None:
    """Initialize the time-wheel strategy.

    Args:
        connection_manager: Connection manager providing the QueueBackend.
        wheel_size: Number of slots in the primary wheel (default 60).
        ticks_per_second: Slot granularity (default 1.0 → 1 slot/sec).
        default_delay: Default delay seconds when push omits ``delay``.
        clock: Monotonic clock callable returning seconds (injectable for tests).
        wall_clock: Unix wall clock used only to account for downtime between
            snapshot and restore.

    Raises:
        ValueError: If wheel sizing, tick granularity, or default delay is
            outside the supported finite range.
    """
    super().__init__(connection_manager)
    if (
      isinstance(wheel_size, bool) or not isinstance(wheel_size, int) or wheel_size < 1
    ):
      raise ValueError(f"wheel_size must be >= 1, got {wheel_size}")
    if wheel_size > MAX_WHEEL_SIZE:
      raise ValueError(
        f"wheel_size must be <= {MAX_WHEEL_SIZE}, got {wheel_size}; "
        "each slot eagerly allocates a deque"
      )
    ticks_per_second = _finite_number(ticks_per_second, "ticks_per_second")
    if ticks_per_second <= 0:
      raise ValueError(f"ticks_per_second must be > 0, got {ticks_per_second}")
    default_delay = _finite_number(default_delay, "default_delay")
    if default_delay < 0:
      raise ValueError(f"default_delay must be >= 0, got {default_delay}")
    self._wheel_size = wheel_size
    self._ticks_per_second = ticks_per_second
    self._wheel_duration = wheel_size / ticks_per_second
    if not math.isfinite(self._wheel_duration):
      raise ValueError(
        "wheel duration must be finite; increase ticks_per_second "
        f"(got {ticks_per_second})"
      )
    self._default_delay = default_delay
    self._clock = clock
    self._wall_clock = wall_clock
    self._wheel: list[deque[tuple[float, bytes, float]]] = [
      deque() for _ in range(wheel_size)
    ]
    self._overflow: list[tuple[float, int, bytes, float]] = []
    self._seq = itertools.count()
    self._state_lock = threading.RLock()
    self._last_tick = self._tick_at(self._clock_now())

  def bind(self, queue_name: str) -> None:
    """Bind this in-process wheel to one logical queue."""
    self._bind_single_queue(queue_name)

  def _clock_now(self) -> float:
    """Return a finite monotonic timestamp from the injected clock."""
    value = self._clock()
    try:
      return _finite_number(value, "clock")
    except ValueError as e:
      raise ValueError(f"clock must return a finite value, got {value!r}") from e

  def _tick_at(self, timestamp: float) -> int:
    """Convert a timestamp to its wheel tick without accepting overflow."""
    scaled = timestamp * self._ticks_per_second
    if not math.isfinite(scaled):
      raise ValueError(f"clock tick must be finite, got {scaled}")
    return math.floor(scaled)

  def _slot_at(self, ready_at: float) -> int:
    """Return the first wheel slot whose tick is not before ``ready_at``."""
    scaled = ready_at * self._ticks_per_second
    if not math.isfinite(scaled):
      raise ValueError(f"ready time tick must be finite, got {scaled}")
    return math.ceil(scaled) % self._wheel_size

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
    priority = _finite_number(priority, "priority")
    delay = _finite_number(delay, "delay")
    if delay < 0:
      raise ValueError(f"delay must be >= 0, got {delay}")
    self.bind(queue_name)
    effective = delay if delay > 0 else self._default_delay
    if effective <= 0:
      self._connection_manager.get_queue_backend().push(queue_name, item, priority)
      return
    with self._state_lock:
      ready_at = self._clock_now() + effective
      if not math.isfinite(ready_at):
        raise ValueError(f"ready time must be finite, got {ready_at}")
      if effective <= self._wheel_duration:
        slot = self._slot_at(ready_at)
        # Store ready_at in the slot entry so _drain_ready can skip items whose
        # delay hasn't elapsed (matters after a long idle — see _drain_ready).
        self._wheel[slot].append((ready_at, item, priority))
      else:
        heapq.heappush(self._overflow, (ready_at, next(self._seq), item, priority))

  def is_push_durable(self, *, delay: float, source: str) -> bool:
    """Return false while a delayed item would live only in wheel state."""
    del source
    effective = delay if delay > 0 else self._default_delay
    return effective <= 0

  def _prepare_push(
    self,
    queue_name: str,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> _PreparedQueuePush:
    """Freeze the live-backend versus wheel/overflow route exactly once."""
    del source
    self.bind(queue_name)
    effective = delay if delay > 0 else self._default_delay

    if effective <= 0:

      def commit(item: bytes, require_durable: bool) -> bool:
        normalized_priority = _finite_number(priority, "priority")
        normalized_delay = _finite_number(delay, "delay")
        if normalized_delay < 0:
          raise ValueError(f"delay must be >= 0, got {normalized_delay}")
        return self._push_backend_prepared(
          queue_name,
          item,
          priority=normalized_priority,
          require_durable=require_durable,
        )

      return _PreparedQueuePush(backend_route=True, _commit=commit)

    def publish(item: bytes) -> None:
      normalized_priority = _finite_number(priority, "priority")
      normalized_delay = _finite_number(delay, "delay")
      if normalized_delay < 0:
        raise ValueError(f"delay must be >= 0, got {normalized_delay}")
      with self._state_lock:
        ready_at = self._clock_now() + effective
        if not math.isfinite(ready_at):
          raise ValueError(f"ready time must be finite, got {ready_at}")
        if effective <= self._wheel_duration:
          slot = self._slot_at(ready_at)
          self._wheel[slot].append((ready_at, item, normalized_priority))
        else:
          heapq.heappush(
            self._overflow,
            (ready_at, next(self._seq), item, normalized_priority),
          )

    return _PreparedQueuePush.local(
      queue_name=queue_name,
      strategy_name=type(self).__name__,
      publish=publish,
    )

  # ------------------------------------------------------------------ pop

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Drain due wheel + overflow items, then pop the live queue.

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        The next ready item, or None if empty.
    """
    timeout = normalize_queue_timeout(timeout)
    self.bind(queue_name)
    self._drain_ready(queue_name)
    return self._connection_manager.get_queue_backend().pop(queue_name, timeout)

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, object | None]:
    """Drain due items, then pop while preserving the backend ack token.

    Time-wheel holding happens before items enter the live backend queue. Once
    an item is due, the final pop has the same deferred-ack requirements as a
    passthrough pop, so MQ tokens must be carried to the scheduler.
    """
    timeout = normalize_queue_timeout(timeout)
    self.bind(queue_name)
    self._drain_ready(queue_name)
    return self._pop_backend_with_ack(queue_name, timeout)

  def _drain_ready(self, queue_name: str) -> None:
    """Move all due held items (wheel + overflow) into the live queue.

    Drains every wheel slot between ``_last_tick+1`` and ``now_tick``,
    capped to one full rotation (so a long idle doesn't loop more than
    ``wheel_size`` times). Then drains overflow items whose ``ready_at``
    has passed.

    Args:
        queue_name: The queue name to drain into.
    """
    with self._state_lock:
      qb = self._connection_manager.get_queue_backend()
      now = self._clock_now()
      now_tick = self._tick_at(now)
      # Cap the drain span to one full rotation — every slot is covered once.
      span = max(0, min(now_tick - self._last_tick, self._wheel_size))
      for i in range(span):
        tick = self._last_tick + 1 + i
        slot = tick % self._wheel_size
        dq = self._wheel[slot]
        if not dq:
          continue
        # Release only items whose ready_at has passed; KEEP future items in the
        # slot (re-checked on the next drain). After a long idle (> one rotation)
        # the catch-up drain covers every slot — without this check a future item
        # sharing a slot position with a past tick is released before its delay
        # elapses. In normal operation (span < wheel_size) the filter is a no-op:
        # a slot is only reached when its tick is in the drain window, at which
        # point ready_at <= now.
        # Keep every entry in the slot until its backend push returns. Future
        # entries remain in place, so interruption never needs compensating
        # cleanup and the deque always contains only valid business entries.
        # Successfully published due entries alone are removed. A signal after
        # backend acceptance but before deletion may replay that one ambiguous
        # entry, which is the safe at-least-once side of the remote commit
        # boundary. The common all-due path deletes index zero in O(1); only the
        # rare mixed future/due catch-up path pays indexed-deque deletion cost.
        entry_index = 0
        entries_to_scan = len(dq)
        for _ in range(entries_to_scan):
          ready_at_h, item, priority = dq[entry_index]
          if ready_at_h > now:
            entry_index += 1
            continue
          qb.push(queue_name, item, priority)
          del dq[entry_index]
      # Drain due overflow. The lock spans backend push through heap removal:
      # another pop must not publish the same uncommitted heap head.
      while self._overflow and self._overflow[0][0] <= now:
        _, _, item, priority = self._overflow[0]
        qb.push(queue_name, item, priority)
        heapq.heappop(self._overflow)
      self._last_tick = now_tick

  # ------------------------------------------------------------------ len/clear

  def queue_len(self, queue_name: str) -> int:
    """Live-queue length + held wheel items + held overflow items."""
    self.bind(queue_name)
    with self._state_lock:
      live = self._connection_manager.get_queue_backend().queue_len(queue_name)
      held_wheel = sum(len(slot) for slot in self._wheel)
      return live + held_wheel + len(self._overflow)

  def clear(self, queue_name: str) -> None:
    """Clear live queue, all wheel slots, and the overflow heap."""
    self.bind(queue_name)
    with self._state_lock:
      self._connection_manager.get_queue_backend().clear_queue(queue_name)
      for slot in self._wheel:
        slot.clear()
      self._overflow.clear()

  def close(self) -> None:
    """Warn about any held items being discarded at shutdown."""
    with self._state_lock:
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

    Returns ``None`` when empty. Version 2 stores remaining delays and a wall
    clock snapshot so restore can rebase process-local monotonic deadlines and
    subtract time spent offline.

    ``slots_flat`` collapses the per-slot deques into one flat list. Slot
    identity is not persisted; restore recomputes it from each remaining
    delay after rebasing the deadline onto the new monotonic clock.
    """
    with self._state_lock:
      held_wheel = sum(len(slot) for slot in self._wheel)
      if held_wheel == 0 and not self._overflow:
        return None
      snapshot_now = self._clock_now()
      snapshot_wall_time = self._wall_clock()
      if not math.isfinite(snapshot_wall_time):
        raise ValueError(
          f"wall_clock must return a finite value, got {snapshot_wall_time}"
        )
      slots_flat = [
        {
          "remaining": max(0.0, ready_at - snapshot_now),
          "item_b64": base64.b64encode(item).decode("ascii"),
          "priority": priority,
        }
        for slot in self._wheel
        for ready_at, item, priority in slot
      ]
      overflow = [
        {
          "remaining": max(0.0, ready_at - snapshot_now),
          "item_b64": base64.b64encode(item).decode("ascii"),
          "priority": priority,
        }
        for ready_at, _seq, item, priority in sorted(self._overflow)
      ]
      return json.dumps(
        {
          "version": 2,
          "strategy": "time_wheel",
          "snapshot_wall_time": snapshot_wall_time,
          "wheel_size": self._wheel_size,
          "slots_flat": slots_flat,
          "overflow": overflow,
        }
      ).encode("utf-8")

  def restore(self, state: bytes | None) -> None:
    """Re-populate the wheel + overflow from a prior :meth:`snapshot`.

    Version 2 remaining delays are rebased onto the current monotonic clock
    after subtracting wall-clock downtime. Version 1 absolute monotonic
    deadlines are unrecoverable across processes, so those items become due
    immediately. Corrupt or unknown-format state is logged and skipped.

    Args:
        state: The bytes blob from a prior :meth:`snapshot`, or ``None``.
    """
    if not state:
      return
    try:
      data = json.loads(state.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
      logger.warning(
        "TimeWheelQueueStrategy restore: corrupt snapshot (%s); starting clean.",
        e,
      )
      return
    if (
      not isinstance(data, dict)
      or data.get("strategy") != "time_wheel"
      or data.get("version") not in (1, 2)
    ):
      logger.warning(
        "TimeWheelQueueStrategy restore: unknown snapshot format; starting clean."
      )
      return
    slots_flat = data.get("slots_flat")
    overflow_entries = data.get("overflow")
    if not isinstance(slots_flat, list) or not isinstance(overflow_entries, list):
      logger.warning(
        "TimeWheelQueueStrategy restore: snapshot collections must be lists; "
        "starting clean."
      )
      return
    version = int(data["version"])
    with self._state_lock:
      now = self._clock_now()
      downtime = 0.0
      if version == 2:
        try:
          snapshot_wall_time = float(data["snapshot_wall_time"])
          current_wall_time = float(self._wall_clock())
          if not math.isfinite(snapshot_wall_time) or not math.isfinite(
            current_wall_time
          ):
            raise ValueError("wall clock is not finite")
          downtime = max(0.0, current_wall_time - snapshot_wall_time)
        except (KeyError, TypeError, ValueError) as e:
          logger.warning(
            "TimeWheelQueueStrategy restore: invalid v2 clock metadata (%s); "
            "starting clean.",
            e,
          )
          return

      def restored_timing(entry: dict[str, Any]) -> tuple[float, float]:
        if version == 1:
          # v1's absolute monotonic epoch cannot be recovered safely.
          old_ready_at = float(entry.get("ready_at", now))
          if not math.isfinite(old_ready_at):
            raise ValueError("legacy ready time is not finite")
          return now, old_ready_at
        remaining = float(entry["remaining"])
        if not math.isfinite(remaining):
          raise ValueError("remaining delay is not finite")
        ready_at = now + max(0.0, remaining - downtime)
        if not math.isfinite(ready_at):
          raise ValueError("restored ready time is not finite")
        return ready_at, remaining

      staged: list[tuple[float, float, int, bytes, float]] = []
      input_order = 0
      # Re-place wheel items by their rebased ready time: recompute the slot for
      # future entries and stage due entries in overflow for the next pop.
      for entry in slots_flat:
        entry_order = input_order
        input_order += 1
        try:
          item = base64.b64decode(entry["item_b64"], validate=True)
          priority = float(entry["priority"])
          if not math.isfinite(priority):
            raise ValueError("priority is not finite")
          ready_at, original_deadline = restored_timing(entry)
        except (KeyError, TypeError, ValueError, binascii.Error) as e:
          logger.warning(
            "TimeWheelQueueStrategy restore: skipping malformed wheel entry (%s).",
            e,
          )
          continue
        staged.append((ready_at, original_deadline, entry_order, item, priority))
      for entry in overflow_entries:
        entry_order = input_order
        input_order += 1
        try:
          ready_at, original_deadline = restored_timing(entry)
          item = base64.b64decode(entry["item_b64"], validate=True)
          priority = float(entry["priority"])
          if not math.isfinite(priority):
            raise ValueError("priority is not finite")
        except (KeyError, TypeError, ValueError, binascii.Error) as e:
          logger.warning(
            "TimeWheelQueueStrategy restore: skipping malformed overflow entry (%s).",
            e,
          )
          continue
        staged.append((ready_at, original_deadline, entry_order, item, priority))

      recovered_wheel: list[deque[tuple[float, bytes, float]]] = [
        deque() for _ in range(self._wheel_size)
      ]
      recovered_overflow: list[tuple[float, int, bytes, float]] = []
      recovered_seq = itertools.count()
      # Expired deadlines collapse to ``now``. Retain their original deadline
      # as a secondary key, then the stable snapshot order as a final tie-break,
      # so downtime cannot reverse already-established delivery order.
      staged.sort(
        key=lambda staged_entry: (
          staged_entry[0],
          staged_entry[1],
          staged_entry[2],
        )
      )
      for ready_at, _original_deadline, _order, item, priority in staged:
        use_wheel = ready_at > now and ready_at - now <= self._wheel_duration
        if use_wheel:
          recovered_wheel[self._slot_at(ready_at)].append((ready_at, item, priority))
        else:
          heapq.heappush(
            recovered_overflow,
            (ready_at, next(recovered_seq), item, priority),
          )
      # A valid persisted snapshot is authoritative startup state. Swap the
      # fully decoded replacement in one lock-held commit so restore is
      # idempotent and readers never observe a partially rebuilt wheel.
      self._wheel = recovered_wheel
      self._overflow = recovered_overflow
      self._seq = recovered_seq
      self._last_tick = self._tick_at(now)
      recovered = len(staged)
    if recovered:
      logger.info(
        "TimeWheelQueueStrategy restore: recovered %d held item(s) from snapshot.",
        recovered,
      )
