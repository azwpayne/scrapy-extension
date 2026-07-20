"""Round-robin queue strategy — fair dispatch across sources (subsystem ②).

A task-queue type "beyond queue/stack/priority": items tagged with a
``source`` are dispatched fairly, cycling through non-empty sources so no
single source starves the others. In-process (single-worker) in v1;
``BackendQueue`` tags items via ``request.meta['source']``.
"""

from __future__ import annotations

__all__ = ["RoundRobinQueueStrategy"]

import base64
import binascii
import json
import logging
import threading
from collections import OrderedDict, deque

from scrapy_extension.queue.strategies.base import (
  QueueStrategy,
  normalize_queue_timeout,
)

logger = logging.getLogger(__name__)


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
      The first key in ``_sources`` is the next source to serve. Successful
      pops move a still-live source to the end in O(1).
      _lock: Protects source membership, rotation, and per-source deques.
  """

  def __init__(self, connection_manager: object) -> None:
    """Initialize the round-robin strategy.

    Args:
        connection_manager: Accepted for protocol parity; unused (in-process).
    """
    super().__init__(connection_manager)  # type: ignore[arg-type]
    self._sources: OrderedDict[str, deque[bytes]] = OrderedDict()
    self._lock = threading.Lock()

  def bind(self, queue_name: str) -> None:
    """Bind this in-process fairness state to one logical queue."""
    self._bind_single_queue(queue_name)

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
    self.bind(queue_name)
    del priority, delay
    with self._lock:
      dq = self._sources.get(source)
      if dq is None:
        dq = deque()
        self._sources[source] = dq
      dq.append(item)

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop the next item, cycling through non-empty sources.

    The first ordered-dict entry is the rotation cursor. A source that still
    has items moves to the end via :meth:`OrderedDict.move_to_end`; a drained
    source is deleted. Both operations are O(1), so draining N one-item
    sources is O(N), not O(N²) from repeatedly materializing/searching keys.

    Args:
        queue_name: The queue name (unused).
        timeout: Ignored (non-blocking rotation).

    Returns:
        The next item in round-robin order, or None if all sources are empty.
    """
    timeout = normalize_queue_timeout(timeout)
    self.bind(queue_name)
    del timeout
    with self._lock:
      while self._sources:
        source, dq = next(iter(self._sources.items()))
        if not dq:
          del self._sources[source]
          continue
        item = dq.popleft()
        if dq:
          self._sources.move_to_end(source)
        else:
          del self._sources[source]
        return item
      return None

  def queue_len(self, queue_name: str) -> int:
    """Return total items across all sources.

    Args:
        queue_name: The queue name (unused).

    Returns:
        Sum of all per-source deque lengths.
    """
    self.bind(queue_name)
    with self._lock:
      return sum(len(dq) for dq in self._sources.values())

  def clear(self, queue_name: str) -> None:
    """Clear all sources.

    Args:
        queue_name: The queue name (unused).
    """
    self.bind(queue_name)
    with self._lock:
      self._sources.clear()

  def snapshot(self) -> bytes | None:
    """Serialize pending items and ordered-source cursor for restart recovery."""
    with self._lock:
      snapshot_sources = [
        (source, tuple(items)) for source, items in self._sources.items() if items
      ]
    if not snapshot_sources:
      return None
    sources = [
      {
        "source": source,
        "items": [base64.b64encode(item).decode("ascii") for item in items],
      }
      for source, items in snapshot_sources
    ]
    return json.dumps(
      {"version": 1, "strategy": "round_robin", "sources": sources},
      separators=(",", ":"),
    ).encode("utf-8")

  def restore(self, state: bytes | None) -> None:
    """Restore pending items and fairness cursor from a versioned snapshot.

    The serialized source order starts with the next source to serve. Parsing
    happens into a temporary ordered dict so malformed snapshots cannot leave
    partially restored live state.
    """
    if not state:
      return
    try:
      data = json.loads(state.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
      logger.warning(
        "RoundRobinQueueStrategy restore: corrupt snapshot (%s); starting clean.",
        e,
      )
      return
    if (
      not isinstance(data, dict)
      or data.get("strategy") != "round_robin"
      or data.get("version") != 1
    ):
      logger.warning(
        "RoundRobinQueueStrategy restore: unknown snapshot format; starting clean."
      )
      return
    raw_sources = data.get("sources")
    if not isinstance(raw_sources, list):
      logger.warning(
        "RoundRobinQueueStrategy restore: snapshot 'sources' not a list; "
        "starting clean."
      )
      return

    recovered: OrderedDict[str, deque[bytes]] = OrderedDict()
    for entry in raw_sources:
      if not isinstance(entry, dict):
        logger.warning(
          "RoundRobinQueueStrategy restore: skipping malformed source entry."
        )
        continue
      source = entry.get("source")
      raw_items = entry.get("items")
      if not isinstance(source, str) or not isinstance(raw_items, list):
        logger.warning(
          "RoundRobinQueueStrategy restore: skipping malformed source entry."
        )
        continue
      if source in recovered:
        logger.warning(
          "RoundRobinQueueStrategy restore: skipping duplicate source %r.",
          source,
        )
        continue
      items: deque[bytes] = deque()
      for raw_item in raw_items:
        try:
          items.append(base64.b64decode(raw_item, validate=True))
        except (binascii.Error, TypeError, ValueError) as e:
          logger.warning(
            "RoundRobinQueueStrategy restore: skipping malformed item for "
            "source %r (%s).",
            source,
            e,
          )
      if items:
        recovered[source] = items

    recovered_count = sum(len(items) for items in recovered.values())
    recovered_sources = len(recovered)
    with self._lock:
      self._sources = recovered
    if recovered_sources:
      logger.info(
        "RoundRobinQueueStrategy restore: recovered %d item(s) across %d source(s).",
        recovered_count,
        recovered_sources,
      )
