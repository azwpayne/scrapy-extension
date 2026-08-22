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
import heapq
import itertools
import json
import logging
import math
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from scrapy_extension.queue.strategies.base import (
    QueueStrategy,
    QueueStrategyRestoreError,
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
        _slot_min_deadlines: Minimum ``ready_at`` held by each wheel slot.
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
            isinstance(wheel_size, bool)
            or not isinstance(wheel_size, int)
            or wheel_size < 1
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
        # Wheel entries retain the same process-wide sequence space as overflow
        # entries.  Keeping sequence numbers beside the legacy three-field wheel
        # tuples preserves the in-process compatibility shape while allowing one
        # stable (ready_at, sequence) drain order across both structures.
        self._wheel_sequences: list[deque[int]] = [deque() for _ in range(wheel_size)]
        self._slot_min_deadlines: list[float | None] = [None] * wheel_size
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

    def _append_wheel_entry(self, slot: int, entry: tuple[float, bytes, float]) -> None:
        """Append one entry and update its slot's derived minimum in O(1)."""
        ready_at = entry[0]
        entries = self._wheel[slot]
        sequences = self._wheel_sequences[slot]
        entry_length = len(entries)
        sequence_length = len(sequences)
        previous_minimum = self._slot_min_deadlines[slot]
        try:
            entries.append(entry)
            sequences.append(next(self._seq))
            current_minimum = previous_minimum
            if current_minimum is None or ready_at < current_minimum:
                current_minimum = ready_at
            self._slot_min_deadlines[slot] = current_minimum
        except BaseException:
            # Keep the parallel wheel and sequence deques, plus their derived
            # minimum, in lockstep even when a process-control signal lands
            # between the individual container mutations.
            while len(entries) > entry_length:
                entries.pop()
            while len(sequences) > sequence_length:
                sequences.pop()
            self._slot_min_deadlines[slot] = previous_minimum
            raise

    def _recompute_slot_min_deadline(self, slot: int) -> None:
        """Rebuild the derived minimum after a slot has been drained."""
        self._slot_min_deadlines[slot] = min(
            (ready_at for ready_at, _item, _priority in self._wheel[slot]),
            default=None,
        )

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
            self._connection_manager.get_queue_backend().push(
                queue_name, item, priority
            )
            return
        with self._state_lock:
            ready_at = self._clock_now() + effective
            if not math.isfinite(ready_at):
                raise ValueError(f"ready time must be finite, got {ready_at}")
            if effective <= self._wheel_duration:
                slot = self._slot_at(ready_at)
                # Store ready_at in the slot entry so _drain_ready can skip items whose
                # delay hasn't elapsed (matters after a long idle — see _drain_ready).
                self._append_wheel_entry(slot, (ready_at, item, priority))
            else:
                heapq.heappush(
                    self._overflow, (ready_at, next(self._seq), item, priority)
                )

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
                    self._append_wheel_entry(
                        slot, (ready_at, item, normalized_priority)
                    )
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
        return cast(
            bytes | None,
            self._pop_until_deadline(
                queue_name,
                timeout,
                lambda wait: self._connection_manager.get_queue_backend().pop(
                    queue_name, wait
                ),
            ),
        )

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
        return cast(
            tuple[bytes | None, object | None],
            self._pop_until_deadline(
                queue_name,
                timeout,
                lambda wait: self._pop_backend_with_ack(queue_name, wait),
                has_item=lambda result: result[0] is not None or result[1] is not None,
                empty=(None, None),
            ),
        )

    def _wheel_release_at(self, ready_at: float) -> float:
        return math.ceil(ready_at * self._ticks_per_second) / self._ticks_per_second

    def _next_release_at(self) -> float | None:
        """Return the next release after scanning only slot minima + overflow head."""
        with self._state_lock:
            next_release = self._overflow[0][0] if self._overflow else None
            for ready_at in self._slot_min_deadlines:
                if ready_at is None:
                    continue
                release_at = self._wheel_release_at(ready_at)
                if next_release is None or release_at < next_release:
                    next_release = release_at
            return next_release

    def _pop_until_deadline(
        self,
        queue_name: str,
        timeout: float,
        backend_pop: Callable[[float], Any],
        *,
        has_item: Callable[[Any], bool] | None = None,
        empty: Any = None,
    ) -> Any:
        """Drain at local release deadlines without exceeding the caller budget."""
        is_item = has_item if has_item is not None else lambda item: item is not None
        deadline: float | None = None
        while True:
            self._drain_ready(queue_name)
            before = self._clock_now()
            if deadline is None:
                deadline = before + timeout
            remaining = max(0.0, deadline - before)
            next_release = self._next_release_at()
            wait = remaining
            if next_release is not None:
                wait = min(wait, max(0.0, next_release - before))
            result = backend_pop(wait)
            if is_item(result):
                return result
            after = self._clock_now()
            if timeout == 0.0 or after >= deadline or after <= before:
                return empty

    def _drain_ready(self, queue_name: str) -> None:
        """Move every releasable wheel/overflow entry in stable deadline order.

        The physical wheel is only an indexing structure. Once an entry's wheel
        release tick (or overflow deadline) is due, all candidates are merged and
        ordered by their original ``(ready_at, sequence)`` pair.  Each candidate
        remains in its source container until the live backend push returns, so a
        normal failure or ``BaseException`` leaves that item and the untouched
        tail owned for retry.
        """
        with self._state_lock:
            qb = self._connection_manager.get_queue_backend()
            now = self._clock_now()
            now_tick = self._tick_at(now)
            # (ready_at, sequence, source, slot, item, priority)
            candidates: list[tuple[float, int, str, int, bytes, float]] = []
            # Preserve the wheel's tick-gated release contract (a sub-tick item
            # waits for the next tick), while scanning a full rotation after a
            # long idle so no due slot is stranded.  Ordering itself is global
            # and no longer depends on the physical slot number.
            span = max(0, min(now_tick - self._last_tick, self._wheel_size))
            slots_to_scan = {
                (self._last_tick + 1 + offset) % self._wheel_size
                for offset in range(span)
            }
            for slot in slots_to_scan:
                dq = self._wheel[slot]
                sequences = self._wheel_sequences[slot]
                for index in range(len(dq)):
                    ready_at, item, priority = dq[index]
                    if ready_at <= now:
                        candidates.append(
                            (
                                ready_at,
                                sequences[index],
                                "wheel",
                                slot,
                                item,
                                priority,
                            )
                        )
            for ready_at, sequence, item, priority in self._overflow:
                if ready_at <= now:
                    candidates.append(
                        (ready_at, sequence, "overflow", -1, item, priority)
                    )
            candidates.sort(key=lambda candidate: (candidate[0], candidate[1]))

            for ready_at, sequence, source, slot, item, priority in candidates:
                if source == "wheel":
                    dq = self._wheel[slot]
                    sequences = self._wheel_sequences[slot]
                    try:
                        entry_index = sequences.index(sequence)
                    except ValueError:
                        # A prior candidate can only remove its own entry; this
                        # guard keeps a defensive custom container from replaying
                        # an already-settled candidate.
                        continue
                    entry_count = len(dq)
                    sequence_count = len(sequences)
                    try:
                        qb.push(queue_name, item, priority)
                        del dq[entry_index]
                        del sequences[entry_index]
                    except BaseException:
                        # The backend push is the transfer boundary.  If local
                        # bookkeeping is interrupted after that boundary, roll
                        # back the local mutation so a retry still owns exactly
                        # the same item and sequence pair.  (An ambiguous backend
                        # push remains conservatively retryable, as before.)
                        if len(dq) < entry_count:
                            dq.insert(entry_index, (ready_at, item, priority))
                        if len(sequences) < sequence_count:
                            sequences.insert(entry_index, sequence)
                        raise
                    finally:
                        self._recompute_slot_min_deadline(slot)
                else:
                    # Heap ordering is the same (ready_at, sequence) order as the
                    # merged list, so every remaining overflow candidate is its
                    # root when reached.  Leave it in place until push succeeds.
                    # A defensively malformed heap must not strand a ready
                    # non-root candidate until a later drain: settle it in
                    # merged order by removing the exact tuple instead of
                    # trusting root-ness (sequence is unique per entry).
                    candidate = (ready_at, sequence, item, priority)
                    if self._overflow and self._overflow[0] == candidate:
                        qb.push(queue_name, item, priority)
                        heapq.heappop(self._overflow)
                        continue
                    try:
                        entry_index = self._overflow.index(candidate)
                    except ValueError:
                        # A prior candidate can only remove its own entry; this
                        # guard keeps a replayed candidate from double-push.
                        continue
                    qb.push(queue_name, item, priority)
                    del self._overflow[entry_index]
                    heapq.heapify(self._overflow)
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
            previous_state = (
                self._wheel,
                self._wheel_sequences,
                self._slot_min_deadlines,
                self._overflow,
            )
            try:
                self._wheel = [deque() for _ in range(self._wheel_size)]
                self._wheel_sequences = [deque() for _ in range(self._wheel_size)]
                self._slot_min_deadlines = [None] * self._wheel_size
                self._overflow = []
            except BaseException:
                (
                    self._wheel,
                    self._wheel_sequences,
                    self._slot_min_deadlines,
                    self._overflow,
                ) = previous_state
                raise

    def close(self) -> None:
        """Warn about any held items being discarded at shutdown."""
        with self._state_lock:
            held = sum(len(slot) for slot in self._wheel) + len(self._overflow)
            previous_state = (
                self._wheel,
                self._wheel_sequences,
                self._slot_min_deadlines,
                self._overflow,
            )
            try:
                self._wheel = [deque() for _ in range(self._wheel_size)]
                self._wheel_sequences = [deque() for _ in range(self._wheel_size)]
                self._slot_min_deadlines = [None] * self._wheel_size
                self._overflow = []
            except BaseException:
                (
                    self._wheel,
                    self._wheel_sequences,
                    self._slot_min_deadlines,
                    self._overflow,
                ) = previous_state
                raise

        if held > 0:
            try:
                logger.warning(
                    "TimeWheelQueueStrategy close: discarding %d held delayed item(s) "
                    "from the in-process wheel + overflow; these are lost on close/restart "
                    "(non-silent data loss).",
                    held,
                )
            except BaseException:
                # State was detached under the lock; shutdown diagnostics must not
                # replace that completed terminal transition.
                pass

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
                    # Keep the process-wide tie-breaker so a restart cannot
                    # reorder equal-deadline entries that happened to occupy
                    # different physical containers.
                    "sequence": sequence,
                }
                for slot, sequences in zip(
                    self._wheel, self._wheel_sequences, strict=True
                )
                for (ready_at, item, priority), sequence in zip(
                    slot, sequences, strict=True
                )
            ]
            overflow = [
                {
                    "remaining": max(0.0, ready_at - snapshot_now),
                    "item_b64": base64.b64encode(item).decode("ascii"),
                    "priority": priority,
                    "sequence": sequence,
                }
                for ready_at, sequence, item, priority in sorted(self._overflow)
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
        """Restore a complete, validated wheel snapshot atomically."""
        if state is None:
            return
        restore_failed = False
        try:
            data = json.loads(state.decode("utf-8"))
            if (
                not isinstance(data, dict)
                or data.get("strategy") != "time_wheel"
                or type(data.get("version")) is not int
                or data.get("version") not in (1, 2)
                or not isinstance(data.get("slots_flat"), list)
                or not isinstance(data.get("overflow"), list)
            ):
                raise ValueError("invalid time wheel snapshot")
            version = data["version"]
            with self._state_lock:
                now = self._clock_now()
                downtime = 0.0
                if version == 2:
                    snapshot_wall_time = float(data["snapshot_wall_time"])
                    current_wall_time = float(self._wall_clock())
                    if not math.isfinite(snapshot_wall_time) or not math.isfinite(
                        current_wall_time
                    ):
                        raise ValueError("wall clock is not finite")
                    downtime = max(0.0, current_wall_time - snapshot_wall_time)

                def restored_timing(entry: dict[str, Any]) -> tuple[float, float]:
                    if not isinstance(entry, dict):
                        raise ValueError("invalid timing entry")
                    if version == 1:
                        old_ready_at = float(entry.get("ready_at", now))
                        if not math.isfinite(old_ready_at):
                            raise ValueError("legacy ready time is not finite")
                        return now, old_ready_at
                    remaining = _finite_number(entry["remaining"], "remaining")
                    if remaining < 0:
                        raise ValueError("remaining delay must be >= 0")
                    ready_at = now + max(0.0, remaining - downtime)
                    if not math.isfinite(ready_at):
                        raise ValueError("restored ready time is not finite")
                    return ready_at, remaining

                staged: list[tuple[float, float, int, int | None, bytes, float]] = []
                serialized_sequences: set[int] = set()
                all_entries = [
                    *data["slots_flat"],
                    *data["overflow"],
                ]
                for entry_order, entry in enumerate(all_entries):
                    ready_at, original_deadline = restored_timing(entry)
                    item = base64.b64decode(entry["item_b64"], validate=True)
                    priority = float(entry["priority"])
                    if not math.isfinite(priority):
                        raise ValueError("priority is not finite")
                    raw_sequence = entry.get("sequence")
                    sequence: int | None = None
                    if raw_sequence is not None:
                        if (
                            isinstance(raw_sequence, bool)
                            or not isinstance(raw_sequence, int)
                            or raw_sequence < 0
                            or raw_sequence in serialized_sequences
                        ):
                            raise ValueError("sequence is invalid")
                        sequence = raw_sequence
                        serialized_sequences.add(raw_sequence)
                    staged.append(
                        (
                            ready_at,
                            original_deadline,
                            entry_order,
                            sequence,
                            item,
                            priority,
                        )
                    )

                recovered_wheel: list[deque[tuple[float, bytes, float]]] = [
                    deque() for _ in range(self._wheel_size)
                ]
                recovered_wheel_sequences: list[deque[int]] = [
                    deque() for _ in range(self._wheel_size)
                ]
                recovered_slot_min_deadlines: list[float | None] = [
                    None
                ] * self._wheel_size
                recovered_overflow: list[tuple[float, int, bytes, float]] = []
                recovered_seq = itertools.count()
                staged.sort(
                    key=lambda staged_entry: (
                        (staged_entry[0], 0, staged_entry[3], staged_entry[2])
                        if staged_entry[3] is not None
                        else (staged_entry[0], 1, staged_entry[1], staged_entry[2])
                    )
                )
                for (
                    ready_at,
                    _original_deadline,
                    _order,
                    _sequence,
                    item,
                    priority,
                ) in staged:
                    use_wheel = (
                        ready_at > now and ready_at - now <= self._wheel_duration
                    )
                    if use_wheel:
                        slot = self._slot_at(ready_at)
                        recovered_wheel[slot].append((ready_at, item, priority))
                        recovered_wheel_sequences[slot].append(next(recovered_seq))
                        slot_minimum = recovered_slot_min_deadlines[slot]
                        if slot_minimum is None or ready_at < slot_minimum:
                            recovered_slot_min_deadlines[slot] = ready_at
                    else:
                        heapq.heappush(
                            recovered_overflow,
                            (ready_at, next(recovered_seq), item, priority),
                        )
                # Publish every derived container as one state transition.  A
                # control-flow interruption between assignments must restore the
                # old wheel rather than exposing mismatched entry/sequence
                # deques or a new wheel with an old tie-breaker counter.
                previous_state = (
                    self._wheel,
                    self._wheel_sequences,
                    self._slot_min_deadlines,
                    self._overflow,
                    self._seq,
                    self._last_tick,
                )
                try:
                    self._wheel = recovered_wheel
                    self._wheel_sequences = recovered_wheel_sequences
                    self._slot_min_deadlines = recovered_slot_min_deadlines
                    self._overflow = recovered_overflow
                    self._seq = recovered_seq
                    self._last_tick = self._tick_at(now)
                except BaseException:
                    (
                        self._wheel,
                        self._wheel_sequences,
                        self._slot_min_deadlines,
                        self._overflow,
                        self._seq,
                        self._last_tick,
                    ) = previous_state
                    raise
        except Exception:
            restore_failed = True
        if restore_failed:
            raise QueueStrategyRestoreError()

        if staged:
            try:
                logger.info(
                    "TimeWheelQueueStrategy restore: recovered %d held item(s) from snapshot.",
                    len(staged),
                )
            except BaseException:
                pass
