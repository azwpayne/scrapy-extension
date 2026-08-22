"""Round-robin queue strategy — fair dispatch across sources (subsystem ②).

A task-queue type "beyond queue/stack/priority": items tagged with a
``source`` are dispatched fairly, cycling through non-empty sources so no
single source starves the others. In-process (single-worker) in v1;
``BackendQueue`` tags items via ``request.meta['source']``.
"""

from __future__ import annotations

__all__ = ["RoundRobinQueueStrategy"]

import base64
import json
import logging
import threading
from collections import OrderedDict, deque

from scrapy_extension.queue.strategies.base import (
    QueueStrategy,
    QueueStrategyRestoreError,
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

    def is_push_durable(self, *, delay: float, source: str) -> bool:
        """Round-robin items remain volatile until this process consumes them."""
        del delay, source
        return False

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
                # Stage the head before publishing the destructive pop.  The
                # deque and ordered-dict updates are separate Python mutations;
                # roll them back if a process-control signal interrupts between
                # them so a retry cannot lose or rotate the head item.
                item = dq[0]
                before_length = len(dq)
                try:
                    dq.popleft()
                    if dq:
                        self._sources.move_to_end(source)
                    else:
                        del self._sources[source]
                except BaseException:
                    if len(dq) < before_length:
                        dq.appendleft(item)
                    if source not in self._sources:
                        self._sources[source] = dq
                    self._sources.move_to_end(source, last=False)
                    raise
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
                (source, tuple(items))
                for source, items in self._sources.items()
                if items
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
        """Restore a complete, validated source map atomically."""
        if state is None:
            return
        restore_failed = False
        try:
            data = json.loads(state.decode("utf-8"))
            if (
                not isinstance(data, dict)
                or data.get("strategy") != "round_robin"
                or type(data.get("version")) is not int
                or data.get("version") != 1
                or not isinstance(data.get("sources"), list)
            ):
                raise ValueError("invalid round robin snapshot")

            recovered: OrderedDict[str, deque[bytes]] = OrderedDict()
            seen_sources: set[str] = set()
            for entry in data["sources"]:
                if not isinstance(entry, dict):
                    raise ValueError("invalid round robin source")
                source = entry.get("source")
                raw_items = entry.get("items")
                if not isinstance(source, str) or not isinstance(raw_items, list):
                    raise ValueError("invalid round robin source")
                if source in seen_sources:
                    raise ValueError("duplicate round robin source")
                seen_sources.add(source)
                items = deque(
                    base64.b64decode(raw_item, validate=True) for raw_item in raw_items
                )
                if items:
                    recovered[source] = items
        except Exception:
            restore_failed = True
        if restore_failed:
            raise QueueStrategyRestoreError()

        recovered_count = sum(len(items) for items in recovered.values())
        recovered_sources = len(recovered)
        with self._lock:
            self._sources = recovered
        if recovered_sources:
            try:
                logger.info(
                    "RoundRobinQueueStrategy restore: recovered %d item(s) across %d source(s).",
                    recovered_count,
                    recovered_sources,
                )
            except BaseException:
                pass
