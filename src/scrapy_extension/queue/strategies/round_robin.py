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
  any source is served twice. Drained-source keys are evicted from
  ``_sources`` (R14-F) so the rotation state stays bounded at the live source
  set — pre-fix the strategy leaked every source key ever seen, making
  ``_sources`` unbounded and every pop O(n) in historical-source count on a
  long crawl with transient sources. ``priority`` and ``delay`` are ignored.

  Items are held in-process — not shared across workers. For distributed
  fairness, use a backend with native fairness; this strategy gives
  per-worker round-robin ordering.

  Attributes:
      _sources: OrderedDict source -> deque (insertion-ordered for stable rotation).
      _idx: Rotation cursor into the live ``_sources`` key order.
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

    Drained-source keys are evicted from ``_sources`` (R14-F): once a
    source's deque empties, its key is removed so the rotation state stays
    bounded at the live source set. The cursor (``_idx``) is re-pointed at
    the next source in rotation *by identity* (not by index) so fairness
    ordering survives the eviction — index-based positioning would break
    because eviction shifts the rotation list. Without eviction, a long
    crawl with transient sources would leak every source key ever seen
    into ``_sources`` and make every pop O(n) in historical-source count.

    Args:
        queue_name: The queue name (unused).
        timeout: Ignored (non-blocking rotation).

    Returns:
        The next item in round-robin order, or None if all sources are empty.
    """
    del queue_name, timeout
    while self._sources:
      rotation = list(self._sources)
      n = len(rotation)
      if n == 0:
        return None
      idx = self._idx % n
      source = rotation[idx]
      dq = self._sources[source]
      if dq:
        item = dq.popleft()
        # Remember the NEXT source in rotation (by identity) BEFORE any
        # eviction shifts the rotation list. The cursor must land on this
        # source so the fairness order survives the deletion.
        next_source = rotation[(idx + 1) % n]
        # R14-F: evict drained sources to keep _sources bounded at the
        # live set. Empty sources left behind would (a) leak unboundedly
        # under source churn and (b) make every pop O(n) in the historical
        # source count as the rotation walks past empty slots.
        if not dq:
          del self._sources[source]
        # Re-point the cursor at the remembered next source. If it was
        # evicted too (or the dict is now empty), clamp by size.
        if next_source in self._sources:
          new_rotation = list(self._sources)
          self._idx = new_rotation.index(next_source) % len(new_rotation)
        else:
          self._idx = 0
        return item
      # Defensive: an empty deque at the cursor (shouldn't normally happen
      # since we evict on drain) — evict it and let the loop retry from a
      # fresh rotation rather than spin on an empty slot.
      del self._sources[source]
      self._idx = 0
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
