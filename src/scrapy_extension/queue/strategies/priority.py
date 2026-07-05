"""Priority queue strategy — strategy-layer priority via N physical buckets (subsystem ②).

Backend priority support is inconsistent: Redis has ZADD/ZPOPMIN, MongoDB sorts
via index, **SQS Standard has no priority**, Kafka uses partition. This strategy
implements priority at the strategy layer by partitioning into N physical
bucket-queues — ``<queue_name>:p<level>`` for ``level ∈ [0, levels)`` — so
priority semantics are uniform across every backend, including those without
native priority.

Higher caller priority → lower level index → popped first (matches the project
convention "priority: higher = more urgent"). The caller's float ``priority`` is
clamped to ``[0.0, 1.0]`` and mapped via ``level = int((1.0 - p) * levels)``,
clamped to ``[0, levels-1]``.

``pop`` scans ``p0, p1, …, p(N-1)`` non-blocking and returns the first non-empty
level's item. If all levels are empty AND ``timeout > 0``, one blocking pop on
``p0`` (the highest-priority bucket) follows — this preserves the caller's
"wait for an item" contract without multiplying the wait across N levels.

All state lives backend-side (no in-process holding); ``snapshot`` returns
``None`` and ``restore`` is a no-op (ABC defaults).
"""

from __future__ import annotations

__all__ = ["DEFAULT_PRIORITY_LEVELS", "PriorityQueueStrategy"]

from typing import TYPE_CHECKING

from scrapy_extension.queue.strategies.base import QueueStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager

#: Default discrete priority-bucket count (high / normal / low).
DEFAULT_PRIORITY_LEVELS: int = 3


class PriorityQueueStrategy(QueueStrategy):
  """Strategy-layer priority via N physical bucket-queues.

  Pushes route to ``<queue_name>:p<level>`` where ``level`` is derived from the
  caller's float ``priority ∈ [0.0, 1.0]`` (higher → lower level index → popped
  first). Pop scans levels high-priority-first. Works on every backend,
  including those without native priority (SQS Standard, Kafka).

  Attributes:
      _levels: Number of discrete priority buckets.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    *,
    levels: int = DEFAULT_PRIORITY_LEVELS,
  ) -> None:
    """Initialize the priority strategy.

    Args:
        connection_manager: Connection manager providing the QueueBackend.
        levels: Number of discrete priority buckets. ``priority ∈ [0,1]`` is
            mapped to ``[0, levels-1]``. Default 3 (high / normal / low).

    Raises:
        ValueError: If ``levels < 1``.
    """
    super().__init__(connection_manager)
    if levels < 1:
      raise ValueError(f"levels must be >= 1, got {levels}")
    self._levels = levels

  def _level_for(self, priority: float) -> int:
    """Map a caller priority float to a level index in ``[0, levels-1]``.

    Higher priority → lower level index (popped first). Clamp to ``[0.0, 1.0]``
    first so out-of-range callers (Scrapy allows any float) are handled
    deterministically.

    Args:
        priority: Caller priority (higher = more urgent).

    Returns:
        Level index in ``[0, levels-1]``.
    """
    clamped = max(0.0, min(1.0, float(priority)))
    level = int((1.0 - clamped) * self._levels)
    return min(self._levels - 1, max(0, level))

  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Push to the priority bucket selected by ``priority``.

    Args:
        queue_name: The logical queue name (the strategy suffixes it).
        item: Serialized item bytes.
        priority: Caller priority ``[0.0, 1.0]`` (higher = more urgent →
            popped first). Out-of-range values clamp.
        delay: Ignored (priority strategy is not a delay queue).
        source: Ignored (priority strategy routes by priority, not source).
    """
    del delay, source
    level = self._level_for(priority)
    self._connection_manager.get_queue_backend().push(
      f"{queue_name}:p{level}", item
    )

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Scan levels high-priority-first; fall through to a blocking wait on p0.

    Non-blocking scan of ``p0..p(N-1)`` returns the first non-empty level's
    item. If all are empty AND ``timeout > 0``, one blocking ``pop(p0, timeout)``
    follows so the caller's wait contract is honored without multiplying the
    wait across N levels.

    Args:
        queue_name: The logical queue name.
        timeout: Seconds to block on ``p0`` if the non-blocking scan is empty.

    Returns:
        The next highest-priority item, or None if all levels empty.
    """
    qb = self._connection_manager.get_queue_backend()
    for level in range(self._levels):
      item = qb.pop(f"{queue_name}:p{level}", 0.0)
      if item is not None:
        return item
    if timeout > 0:
      return qb.pop(f"{queue_name}:p0", timeout)
    return None

  def queue_len(self, queue_name: str) -> int:
    """Sum the lengths of all priority buckets.

    Args:
        queue_name: The logical queue name.

    Returns:
        Total items across all ``N`` levels.
    """
    qb = self._connection_manager.get_queue_backend()
    return sum(qb.queue_len(f"{queue_name}:p{level}") for level in range(self._levels))

  def clear(self, queue_name: str) -> None:
    """Clear all priority buckets.

    Args:
        queue_name: The logical queue name.
    """
    qb = self._connection_manager.get_queue_backend()
    for level in range(self._levels):
      qb.clear_queue(f"{queue_name}:p{level}")
