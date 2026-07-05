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
from scrapy_extension.queue.strategies.base import QueueStrategy

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
        ValueError: If ``capacity < 1`` or ``full_policy`` is not one of the
            allowed values.
    """
    super().__init__(connection_manager)
    if capacity < 1:
      raise ValueError(f"capacity must be >= 1, got {capacity}")
    if full_policy not in ("reject", "drop_oldest", "block"):
      raise ValueError(
        f"full_policy must be one of 'reject', 'drop_oldest', 'block'; got {full_policy!r}"
      )
    self._capacity = capacity
    self._full_policy = full_policy
    self._buffer: deque[bytes] = deque()
    self._dropped = 0
    self._lock = threading.Lock()
    self._not_full = threading.Condition(self._lock)

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
        QueueError: If ``full_policy='reject'`` and the buffer is full.
    """
    del queue_name, priority, delay, source
    with self._not_full:
      while len(self._buffer) >= self._capacity:
        if self._full_policy == "reject":
          raise QueueError(
            f"ring buffer full (capacity={self._capacity}, full_policy=reject)"
          )
        if self._full_policy == "drop_oldest":
          self._buffer.popleft()
          self._dropped += 1
          self._buffer.append(item)
          return
        # block — wait for a pop to free a slot. Loop re-checks capacity
        # against spurious wakeups and concurrent pushes.
        self._not_full.wait()
      self._buffer.append(item)

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
    del queue_name, timeout
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
    del queue_name
    with self._lock:
      return len(self._buffer)

  def clear(self, queue_name: str) -> None:
    """Empty the buffer; wake all blocked pushers (slots are now free)."""
    del queue_name
    with self._not_full:
      self._buffer.clear()
      self._not_full.notify_all()

  def close(self) -> None:
    """Wake any blocked pushers so they don't outlive the strategy."""
    with self._not_full:
      self._not_full.notify_all()

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
      items = [
        base64.b64encode(item).decode("ascii") for item in self._buffer
      ]
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
    """Re-populate the buffer from a prior :meth:`snapshot`.

    Items are re-appended in insertion order. If the snapshot's item count
    exceeds this strategy's capacity, the OLDEST items are truncated (logged).
    Corrupt or unknown-format state is logged + skipped.

    Args:
        state: The bytes blob from a prior :meth:`snapshot`, or ``None``.
    """
    if not state:
      return
    try:
      data = json.loads(state.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
      logger.warning(
        "RingBufferQueueStrategy restore: corrupt snapshot (%s); starting clean.", e
      )
      return
    if (
      not isinstance(data, dict)
      or data.get("strategy") != "ring_buffer"
      or data.get("version") != 1
    ):
      logger.warning(
        "RingBufferQueueStrategy restore: unknown snapshot format; starting clean."
      )
      return
    raw_items = data.get("items")
    if not isinstance(raw_items, list):
      logger.warning(
        "RingBufferQueueStrategy restore: snapshot 'items' not a list; starting clean."
      )
      return
    decoded: list[bytes] = []
    for entry in raw_items:
      try:
        decoded.append(base64.b64decode(entry))
      except (TypeError, ValueError) as e:
        logger.warning(
          "RingBufferQueueStrategy restore: skipping malformed item (%s).", e
        )
        continue
    # Truncate oldest if the snapshot carries more than capacity.
    if len(decoded) > self._capacity:
      dropped = len(decoded) - self._capacity
      logger.warning(
        "RingBufferQueueStrategy restore: snapshot had %d items, capacity=%d; "
        "truncating the OLDEST %d item(s).",
        len(decoded),
        self._capacity,
        dropped,
      )
      decoded = decoded[dropped:]
    with self._not_full:
      self._buffer.clear()
      self._buffer.extend(decoded)
      # Best-effort: preserve dropped counter from snapshot if present.
      snapshot_dropped = data.get("dropped", 0)
      if isinstance(snapshot_dropped, int):
        self._dropped = snapshot_dropped
      if decoded:
        logger.info(
          "RingBufferQueueStrategy restore: recovered %d item(s) from snapshot.",
          len(decoded),
        )
