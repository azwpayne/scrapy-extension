"""Round-robin queue strategy — fair dispatch across sources (subsystem ②).

A task-queue type "beyond queue/stack/priority": items tagged with a
``source`` are dispatched fairly, cycling through non-empty sources so no
single source starves the others. In-process (single-worker) in v1;
``BackendQueue`` tags items via ``request.meta['source']``.
"""

from __future__ import annotations

__all__ = ["RoundRobinQueueStrategy"]

from collections import OrderedDict, deque

from scrapy_extension.queue.strategies.base import QueueStrategy


class RoundRobinQueueStrategy(QueueStrategy):
  """Fair round-robin dispatch across per-source sub-queues.

  Each distinct ``source`` gets its own deque; ``pop`` cycles through sources
  in rotation, skipping empty ones, so every non-empty source is served before
  any source is served twice. ``priority`` and ``delay`` are ignored.

  Items are held in-process — not shared across workers. For distributed
  fairness, use a backend with native fairness; this strategy gives
  per-worker round-robin ordering.

  Attributes:
      _sources: OrderedDict source -> deque (insertion-ordered for stable rotation).
  """

  def __init__(self, connection_manager: object) -> None:
    """Initialize the round-robin strategy.

    Args:
        connection_manager: Accepted for protocol parity; unused (in-process).
    """
    super().__init__(connection_manager)  # type: ignore[arg-type]
    self._sources: OrderedDict[str, deque[bytes]] = OrderedDict()
    self._idx = 0

  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Append ``item`` to the ``source`` sub-queue.

    Args:
        queue_name: The queue name (unused; items held per-source in-process).
        item: Serialized item bytes.
        priority: Ignored.
        delay: Ignored.
        source: Source tag for round-robin fairness (default ``"default"``).
    """
    del queue_name, priority, delay
    dq = self._sources.get(source)
    if dq is None:
      dq = deque()
      self._sources[source] = dq
    dq.append(item)

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop the next item, cycling through non-empty sources.

    Args:
        queue_name: The queue name (unused).
        timeout: Ignored (non-blocking rotation).

    Returns:
        The next item in round-robin order, or None if all sources are empty.
    """
    del queue_name, timeout
    rotation = list(self._sources)
    n = len(rotation)
    for offset in range(n):
      source = rotation[(self._idx + offset) % n]
      dq = self._sources[source]
      if dq:
        self._idx = (self._idx + offset + 1) % n
        return dq.popleft()
    return None

  def queue_len(self, queue_name: str) -> int:
    """Return total items across all sources.

    Args:
        queue_name: The queue name (unused).

    Returns:
        Sum of all per-source deque lengths.
    """
    del queue_name
    return sum(len(dq) for dq in self._sources.values())

  def clear(self, queue_name: str) -> None:
    """Clear all sources.

    Args:
        queue_name: The queue name (unused).
    """
    del queue_name
    self._sources.clear()
    self._idx = 0
