"""Priority queue strategy — strategy-layer priority via N physical buckets (subsystem ②).

Backend priority support is inconsistent: Redis has ZADD/ZPOPMIN, MongoDB sorts
via index, **SQS Standard has no priority**, Kafka uses partition. This strategy
implements priority at the strategy layer by partitioning into N physical
bucket-queues for ``level ∈ [0, levels)`` — so
priority semantics are uniform across backends that can isolate multiple
physical queues. Kafka and RocketMQ are rejected because their bundled clients
cannot preserve requested-topic isolation while scanning multiple buckets.

Higher caller priority → lower level index → popped first (matches Scrapy's
contract). Scrapy accepts arbitrary signed integer priorities and defaults to
zero, so zero maps to the middle bucket; positive/negative values move toward
the high/low end and saturate at the configured bounds.

``pop`` scans ``p0, p1, …, p(N-1)`` non-blocking and returns the first non-empty
level's item. If all levels are empty AND ``timeout > 0``, one blocking pop on
``p0`` (the highest-priority bucket) follows — this preserves the caller's
"wait for an item" contract without multiplying the wait across N levels.

All state lives backend-side (no in-process holding); ``snapshot`` returns
``None`` and ``restore`` is a no-op (ABC defaults).
"""

from __future__ import annotations

__all__ = [
  "DEFAULT_PRIORITY_LEVELS",
  "MAX_PRIORITY_LEVELS",
  "PriorityQueueStrategy",
]

import math
from typing import TYPE_CHECKING

from scrapy_extension.queue.strategies._names import (
  ensure_fanout_backend_supported,
  physical_strategy_queue_name,
)
from scrapy_extension.queue.strategies.base import (
  QueueStrategy,
  normalize_queue_timeout,
)

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager

#: Default discrete priority-bucket count (high / normal / low).
DEFAULT_PRIORITY_LEVELS: int = 3
#: Hard cap on bucket fan-out (each pop/len/clear performs one RPC per bucket).
MAX_PRIORITY_LEVELS: int = 256


class PriorityQueueStrategy(QueueStrategy):
  """Strategy-layer priority via N physical bucket-queues.

  Pushes route to a stable physical bucket name where ``level`` is derived from
  Scrapy's arbitrary signed integer priority (higher → lower level index →
  popped first). Pop scans levels high-priority-first. Works on backends that
  support isolated physical queues, including SQS Standard.

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
        levels: Number of discrete priority buckets centered around Scrapy's
            default priority ``0``. Default 3 (positive / zero / negative).

    Raises:
        ValueError: If ``levels`` is not an integer in the supported range.
    """
    super().__init__(connection_manager)
    ensure_fanout_backend_supported(connection_manager, strategy="priority")
    if (
      isinstance(levels, bool)
      or not isinstance(levels, int)
      or not 1 <= levels <= MAX_PRIORITY_LEVELS
    ):
      raise ValueError(
        f"levels must be an integer in [1, {MAX_PRIORITY_LEVELS}], got {levels!r}"
      )
    self._levels = levels

  def _level_for(self, priority: float) -> int:
    """Map a caller priority to a level index in ``[0, levels-1]``.

    Higher priority → lower level index (popped first). Scrapy priorities are
    signed integers with zero as the default. The finite bucket count preserves
    that ordering monotonically and saturates values outside its representable
    range. Direct API callers may supply finite fractional values; those are
    rounded to the nearest bucket boundary.

    Args:
        priority: Caller priority (higher = more urgent).

    Returns:
        Level index in ``[0, levels-1]``.
    """
    center = self._levels // 2
    if isinstance(priority, int):
      level = center - priority
    else:
      numeric = float(priority)
      if not math.isfinite(numeric):
        raise ValueError(f"priority must be finite, got {priority!r}")
      level = math.floor(center - numeric + 0.5)
    return min(self._levels - 1, max(0, level))

  def _bucket_queue(self, queue_name: str, level: int) -> str:
    """Return the stable, backlog-compatible name for one priority level."""
    return physical_strategy_queue_name(
      self._connection_manager,
      queue_name=queue_name,
      namespace="priority",
      discriminator=str(level),
      legacy_name=f"{queue_name}:p{level}",
    )

  def is_push_durable(self, *, delay: float, source: str) -> bool:
    """Report that priority buckets are all backend-backed queues."""
    del delay, source
    return True

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
        queue_name: The logical queue name (the strategy derives a physical name).
        item: Serialized item bytes.
        priority: Scrapy priority (higher = more urgent → popped first).
        delay: Ignored (priority strategy is not a delay queue).
        source: Ignored (priority strategy routes by priority, not source).
    """
    del delay, source
    level = self._level_for(priority)
    self._connection_manager.get_queue_backend().push(
      self._bucket_queue(queue_name, level), item
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
    timeout = normalize_queue_timeout(timeout)
    qb = self._connection_manager.get_queue_backend()
    for level in range(self._levels):
      item = qb.pop(self._bucket_queue(queue_name, level), 0.0)
      if item is not None:
        return item
    if timeout > 0:
      return qb.pop(self._bucket_queue(queue_name, 0), timeout)
    return None

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, object | None]:
    """Scan levels high-priority-first via ``pop_with_ack``, threading the MQ
    per-message ack token (#28). Mirrors ``pop`` but returns ``(data, token)``
    so MQ backends paired with the priority strategy keep per-message ack
    correlation (previously the token was silently None).
    """
    timeout = normalize_queue_timeout(timeout)
    qb = self._connection_manager.get_queue_backend()
    for level in range(self._levels):
      physical_queue = self._bucket_queue(queue_name, level)
      data, token = self._pop_backend_instance_with_ack(qb, physical_queue, 0.0)
      if data is not None:
        return (data, token)
    if timeout > 0:
      return self._pop_backend_instance_with_ack(
        qb,
        self._bucket_queue(queue_name, 0),
        timeout,
      )
    return (None, None)

  def queue_len(self, queue_name: str) -> int:
    """Sum the lengths of all priority buckets.

    Args:
        queue_name: The logical queue name.

    Returns:
        Total items across all ``N`` levels.
    """
    qb = self._connection_manager.get_queue_backend()
    return sum(
      qb.queue_len(self._bucket_queue(queue_name, level))
      for level in range(self._levels)
    )

  def clear(self, queue_name: str) -> None:
    """Clear all priority buckets.

    Args:
        queue_name: The logical queue name.
    """
    qb = self._connection_manager.get_queue_backend()
    for level in range(self._levels):
      qb.clear_queue(self._bucket_queue(queue_name, level))
