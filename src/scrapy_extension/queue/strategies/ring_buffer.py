"""Ring-buffer queue strategy — bounded in-process circular buffer (subsystem ②).

Backend queues can grow unboundedly. Some workloads — streaming, real-time
ingestion, memory-constrained workers — want **bounded memory + explicit
backpressure** semantics: lag beyond capacity should be SIGNALED (or
controlled), not silently buffered forever.

This strategy keeps a fixed-capacity in-process circular buffer (the buffer IS
the storage — the :class:`~scrapy_extension.backends.base.QueueBackend` from the
connection manager is intentionally ignored). When the buffer is full, a
configurable ``full_policy`` decides:

- ``reject`` (default) — raise :class:`~scrapy_extension.exceptions.QueueError`
- ``drop_oldest`` — overwrite the oldest item, increment a ``_dropped`` counter
- ``block`` — wait on a :class:`threading.Condition` until a ``pop`` frees a
  slot (cooperative backpressure; may block indefinitely if no pop happens)

Trade-off: items are in-process and lost on crash/restart — the snapshot/
restore path mitigates this for the buffered items at close time, but a
mid-run crash still loses what's in-flight. Documented.

Thread-safe via a single :class:`threading.Lock` (the ``block`` policy uses a
:class:`threading.Condition` so blocked pushers wake when a pop frees a slot).
Closing the strategy rejects new pushes and wakes blocked pushers with a
:class:`~scrapy_extension.exceptions.QueueError`. An explicit ``open()``
starts a new lifecycle without admitting pushers blocked in the prior one.
"""

from __future__ import annotations

__all__ = [
    "DEFAULT_RING_BUFFER_CAPACITY",
    "DEFAULT_RING_BUFFER_FULL_POLICY",
    "RingBufferQueueStrategy",
]

import base64
import json
import logging
import threading
from collections import deque
from typing import TYPE_CHECKING, Literal

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.strategies.base import (
    QueueStrategy,
    QueueStrategyRestoreError,
    normalize_queue_timeout,
)

if TYPE_CHECKING:
    from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)

#: Default slot count — chosen to bound memory while keeping most streaming
#: workloads from throttling under typical burst sizes.
DEFAULT_RING_BUFFER_CAPACITY: int = 1024
#: Default overflow behavior — fail-fast over silent loss.
DEFAULT_RING_BUFFER_FULL_POLICY: str = "reject"

_FullPolicy = Literal["reject", "drop_oldest", "block"]


class RingBufferQueueStrategy(QueueStrategy):
    """Bounded in-process circular buffer with configurable overflow policy.

    Ignores the connection manager's QueueBackend — the buffer IS the storage.
    Use this when you want bounded memory + explicit backpressure, NOT when you
    need cross-process distribution or persistence.

    Attributes:
        _capacity: Maximum items held.
        _full_policy: Overflow behavior (reject / drop_oldest / block).
        _buffer: :class:`collections.deque` of buffered item bytes (FIFO).
        _dropped: Count of items dropped by ``drop_oldest`` overflows.
        _closed: Whether the strategy has stopped accepting pushes.
        _generation: Monotonic lifecycle epoch used to reject stale pushers.
        _lock: Thread-safety lock.
        _not_full: Condition signaled by ``pop`` to wake blocked ``push`` calls.
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        *,
        capacity: int = DEFAULT_RING_BUFFER_CAPACITY,
        full_policy: _FullPolicy = DEFAULT_RING_BUFFER_FULL_POLICY,  # type: ignore[assignment]
    ) -> None:
        """Initialize the ring-buffer strategy.

        Args:
            connection_manager: Connection manager (accepted for ABC compliance;
                the backend QueueBackend is intentionally unused — the buffer is
                the storage).
            capacity: Maximum items held (default 1024).
            full_policy: Overflow behavior — ``reject`` (raise QueueError),
                ``drop_oldest`` (overwrite oldest + count), or ``block`` (wait
                for a pop to free a slot).

        Raises:
            ValueError: If ``capacity`` is not a positive integer or ``full_policy``
                is not one of the allowed values.
        """
        super().__init__(connection_manager)
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError(
                f"capacity must be >= 1 (positive integer), got {capacity!r}"
            )
        if full_policy not in ("reject", "drop_oldest", "block"):
            raise ValueError(
                f"full_policy must be one of 'reject', 'drop_oldest', 'block'; got {full_policy!r}"
            )
        self._capacity = capacity
        self._full_policy = full_policy
        self._buffer: deque[bytes] = deque()
        self._dropped = 0
        self._closed = False
        self._generation = 0
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)

    def bind(self, queue_name: str) -> None:
        """Bind this in-process buffer to one logical queue."""
        self._bind_single_queue(queue_name)

    def is_push_durable(self, *, delay: float, source: str) -> bool:
        """Ring-buffer items exist only in this process until consumed."""
        del delay, source
        return False

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
        """Append one item to the tail, applying the configured ``full_policy``.

        Args:
            queue_name: Ignored (the buffer is single-queue; pass-through callers
                still supply it).
            item: Serialized item bytes.
            priority: Ignored (FIFO buffer; no priority ordering).
            delay: Ignored (not a delay queue).
            source: Ignored.

        Raises:
            QueueError: If the strategy is closed, or if
                ``full_policy='reject'`` and the buffer is full.
        """
        self.bind(queue_name)
        del priority, delay, source
        with self._not_full:
            generation = self._generation
            while True:
                if self._closed or generation != self._generation:
                    raise QueueError(
                        "ring buffer lifecycle closed; push rejected",
                        queue_name=queue_name,
                        operation="push",
                    )
                if len(self._buffer) < self._capacity:
                    self._buffer.append(item)
                    return
                if self._full_policy == "reject":
                    raise QueueError(
                        f"ring buffer full (capacity={self._capacity}, full_policy=reject)",
                        queue_name=queue_name,
                        operation="push",
                    )
                if self._full_policy == "drop_oldest":
                    # Stage the complete replacement off to the side.  Mutating
                    # the live deque first (popleft, counter increment, append)
                    # can lose the resident item when append raises MemoryError
                    # or a process-control signal arrives between those steps.
                    # Neither allocation nor append below touches live state; the
                    # two plain attribute assignments publish one replacement.
                    replacement = type(self._buffer)(self._buffer)
                    replacement.popleft()
                    replacement.append(item)
                    dropped = self._dropped + 1
                    previous_buffer = self._buffer
                    previous_dropped = self._dropped
                    try:
                        # Publish both pieces of state as one logical transition.
                        # The assignments are normally infallible, but keeping a
                        # rollback boundary here also handles deterministic fault
                        # injection and an asynchronous process-control exception
                        # between the two assignments without exposing a mixed
                        # buffer/counter state.
                        self._buffer = replacement
                        self._dropped = dropped
                    except BaseException:
                        self._buffer = previous_buffer
                        self._dropped = previous_dropped
                        raise
                    return
                # block — wait for a pop to free a slot. Loop re-checks capacity
                # and closed state against spurious wakeups and concurrent pushes.
                self._not_full.wait()

    # ------------------------------------------------------------------ pop

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop the oldest item from the head. ``timeout`` is ignored in v1
        (returns None immediately when empty — caller's responsibility to retry).

        Args:
            queue_name: Ignored.
            timeout: Ignored (v1 does not block on empty; documented).

        Returns:
            The oldest buffered item, or None if empty.
        """
        timeout = normalize_queue_timeout(timeout)
        self.bind(queue_name)
        del timeout
        with self._not_full:
            if not self._buffer:
                return None
            item = self._buffer.popleft()
            # Notify ONE blocked pusher (if any) that a slot freed.
            self._not_full.notify()
            return item

    # ------------------------------------------------------------------ len/clear

    def queue_len(self, queue_name: str) -> int:
        """Buffer size (backend is unused)."""
        self.bind(queue_name)
        with self._lock:
            return len(self._buffer)

    def clear(self, queue_name: str) -> None:
        """Empty the buffer; wake all blocked pushers (slots are now free)."""
        self.bind(queue_name)
        with self._not_full:
            self._buffer.clear()
            self._not_full.notify_all()

    def open(self) -> None:
        """Start a new push lifecycle after :meth:`close`."""
        with self._not_full:
            if self._closed:
                self._generation += 1
                self._closed = False

    def begin_close(self) -> None:
        """Stop accepting pushes and wake blocked pushers without clearing state."""
        with self._not_full:
            self._closed = True
            self._not_full.notify_all()

    def close(self) -> None:
        """Stop accepting pushes and wake blocked pushers with ``QueueError``."""
        self.begin_close()

    # ------------------------------------------------------------------ snapshot/restore

    def snapshot(self) -> bytes | None:
        """Serialize buffer + dropped count for restart recovery.

        Returns ``None`` when both the buffer and the dropped counter are empty.
        Otherwise a versioned JSON blob:
        ``{"version":1,"strategy":"ring_buffer","capacity":..,"items":[item_b64,..],"dropped":N}``.
        """
        with self._lock:
            if not self._buffer and self._dropped == 0:
                return None
            items = [base64.b64encode(item).decode("ascii") for item in self._buffer]
            return json.dumps(
                {
                    "version": 1,
                    "strategy": "ring_buffer",
                    "capacity": self._capacity,
                    "items": items,
                    "dropped": self._dropped,
                }
            ).encode("utf-8")

    def restore(self, state: bytes | None) -> None:
        """Restore a complete, validated buffer snapshot atomically."""
        if state is None:
            return
        restore_failed = False
        try:
            data = json.loads(state.decode("utf-8"))
            if (
                not isinstance(data, dict)
                or data.get("strategy") != "ring_buffer"
                or type(data.get("version")) is not int
                or data.get("version") != 1
                or not isinstance(data.get("items"), list)
            ):
                raise ValueError("invalid ring buffer snapshot")
            decoded = [
                base64.b64decode(entry, validate=True) for entry in data["items"]
            ]
            snapshot_dropped = data.get("dropped", 0)
            if (
                isinstance(snapshot_dropped, bool)
                or not isinstance(snapshot_dropped, int)
                or snapshot_dropped < 0
            ):
                raise ValueError("invalid dropped count")
            if len(decoded) > self._capacity:
                decoded = decoded[len(decoded) - self._capacity :]
        except Exception:
            restore_failed = True
        if restore_failed:
            raise QueueStrategyRestoreError()

        restored_buffer = deque(decoded)
        with self._not_full:
            previous_buffer = self._buffer
            previous_dropped = self._dropped
            try:
                # Do not clear and refill the live deque in place: an
                # interruption between those mutations would destroy the only
                # still-owned buffer. Publish the replacement and its counter
                # together, rolling both back if publication is interrupted.
                self._buffer = restored_buffer
                self._dropped = snapshot_dropped
            except BaseException:
                self._buffer = previous_buffer
                self._dropped = previous_dropped
                raise
            self._not_full.notify_all()
            if decoded:
                try:
                    logger.info(
                        "RingBufferQueueStrategy restore: recovered %d item(s) from snapshot.",
                        len(decoded),
                    )
                except BaseException:
                    pass
