"""Delay queue strategy — items held until ready, then moved to the live queue (subsystem ②).

A task-queue type "beyond queue/stack/priority": an item pushed with a delay
is not poppable until ``ready_at = now + delay`` elapses. Crucially
direction-independent — held items bypass the (inconsistent) priority
contract entirely, so this strategy is correct regardless of backend
priority ordering. See the design spec for distributed-scope notes.
"""

from __future__ import annotations

__all__ = ["DEFAULT_DELAY_MAX_HELD", "DelayQueueStrategy"]

import base64
import heapq
import itertools
import json
import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from scrapy_extension.monitor.base import Monitor, NullMonitor
from scrapy_extension.queue.strategies.base import QueueStrategy

logger = logging.getLogger(__name__)

# SPEC-round8-tier1 U5 — OOM prevention default for the in-process holding
# heap. The heap grows unboundedly when push-rate outpaces ready-rate (e.g. a
# burst of long-delay items). 100k is a soft cap: warn-once, never refuse —
# refusing would silently drop delayed items. Distributed-delay (U10) is the
# long-term fix; until then this surfaces the growth risk to operators.
DEFAULT_DELAY_MAX_HELD: int = 100_000

# Module-level cache so the over-cap warning fires once per process even when
# many strategies are constructed. Mirrors dupefilter/filters/factory.py
# `_warned`. Tests reset this to verify the warn-once contract from a clean
# slate.
_over_cap_warned: bool = False

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


class DelayQueueStrategy(QueueStrategy):
  """Holds items for a per-item delay, then enqueues them to the live queue.

  Single-process holding (v1): a ``heapq`` of ``(ready_at, seq, item, priority)``.
  ``push`` with ``delay > 0`` parks the item until ready; ``pop`` drains all
  due held items into the live queue first, then pops the live queue.
  ``delay == 0`` (or unset) goes straight to the live queue.

  The heap tuple stores the caller's ``priority`` so :meth:`_drain_ready`
  can re-pass it to the live queue on drain — without it, every delayed
  item would silently land at priority 0 (priority inversion for any user
  mixing ``delay`` + ``priority``). R14-F.

  Cross-worker holding is not supported in v1 — each process holds its own
  delayed items. See ``docs/superpowers/specs/2026-06-19-queue-semantics-design.md``.

  Attributes:
      _default_delay: Default delay when push omits an explicit delay.
      _clock: Monotonic clock callable (injectable for tests).
      _holding: Min-heap of ``(ready_at, seq, item, priority)``.
      _seq: Tie-break counter for stable heap ordering.
      _max_held: Soft cap on ``_holding`` size; non-positive disables the
          warning. Defaults to :data:`DEFAULT_DELAY_MAX_HELD` (100_000).
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    *,
    default_delay: float = 0.0,
    clock: Callable[[], float] = time.monotonic,
    max_held: int = DEFAULT_DELAY_MAX_HELD,
    monitor: Monitor | None = None,
  ) -> None:
    """Initialize the delay strategy.

    Args:
        connection_manager: Connection manager providing the QueueBackend.
        default_delay: Default delay seconds when push omits ``delay``.
        clock: Monotonic clock callable returning seconds (injectable for tests).
        max_held: Soft cap on the in-process holding heap. When the heap
            exceeds this size a one-time WARNING fires (warn-only — items are
            never refused, since dropping a delayed item would silently lose
            data). Defaults to :data:`DEFAULT_DELAY_MAX_HELD` (100_000) for
            OOM prevention. Pass ``<= 0`` to disable the warning (advanced
            opt-out — accepts the unbounded-growth risk).
        monitor: Risk 3 — optional observability monitor. When ``None``
            (default) :class:`~scrapy_extension.monitor.base.NullMonitor`.
            Emits ``on_delay_depth(len(holding))`` after each held item so a
            wired collector can alert before the in-process delay heap grows
            unbounded (the held-delay state is in-process and lost on crash).

    Raises:
        ValueError: If default_delay is negative.
    """
    super().__init__(connection_manager)
    if default_delay < 0:
      raise ValueError(f"default_delay must be >= 0, got {default_delay}")
    self._default_delay = default_delay
    self._clock = clock
    # R14-F: heap tuple gains a `priority` slot so the drain path can re-pass
    # the caller's priority to the live queue. Without it every delayed item
    # would silently land at priority 0 (priority inversion for callers
    # mixing delay + priority). Tuple order is (ready_at, seq, item, priority)
    # — `ready_at` is still the heap key; `seq` is the FIFO tie-breaker; the
    # trailing two slots are payload that never participate in ordering.
    self._holding: list[tuple[float, int, bytes, float]] = []
    self._seq = itertools.count()
    self._max_held = max_held
    self._monitor: Monitor = monitor if monitor is not None else NullMonitor()

  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Push an item, holding it until ready if a delay is set.

    Args:
        queue_name: The queue name.
        item: Serialized item bytes.
        priority: Priority for the live-queue push (used once drained).
        delay: Delay seconds; 0 falls back to ``default_delay``.
        source: Ignored (delay strategy holds by ready-time, not source).
    """
    del source
    effective = delay if delay > 0 else self._default_delay
    if effective <= 0:
      self._connection_manager.get_queue_backend().push(queue_name, item, priority)
      return
    ready_at = self._clock() + effective
    # R14-F: stash priority in the heap tuple so _drain_ready re-passes it
    # to the live queue (preserves caller priority across the delay hop).
    heapq.heappush(self._holding, (ready_at, next(self._seq), item, priority))
    # Risk 3: emit the held-depth gauge so operators can alert before the
    # in-process delay heap grows unbounded (held-delay state is lost on
    # crash). No-op under NullMonitor; BLE001-guarded so a misbehaving
    # monitor cannot crash the push path.
    try:
      self._monitor.on_delay_depth(len(self._holding))
    except Exception:  # noqa: BLE001 — monitor must never crash push
      logger.debug("on_delay_depth hook raised", exc_info=True)
    self._warn_over_cap_once()

  def _warn_over_cap_once(self) -> None:
    """Emit a one-time per-process WARNING when the holding heap exceeds cap.

    The holding heap grows unboundedly whenever push-rate outpaces
    ready-rate (e.g. a burst of long-delay items). The cap is SOFT: this
    never refuses the push — dropping a delayed item would silently lose
    data, which is worse than loud memory pressure. Idempotent via the
    module-level ``_over_cap_warned`` flag so a multi-spider process does
    not spam the log. Points at the distributed-delay roadmap (U10).
    """
    global _over_cap_warned
    if _over_cap_warned:
      return
    if self._max_held <= 0:
      return
    if len(self._holding) <= self._max_held:
      return
    _over_cap_warned = True
    logger.warning(
      "DelayQueueStrategy holding heap exceeded max_held=%d items "
      "(now=%d). The in-process heap grows unboundedly when push-rate "
      "outpaces ready-rate; long bursts of long-delay items can exhaust "
      "memory. This is a SOFT cap — items are NOT dropped. Raise "
      "max_held, drain sooner, or wait for distributed-delay support "
      "(roadmap U10).",
      self._max_held,
      len(self._holding),
    )

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Drain due held items, then pop the live queue.

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        The next ready item, or None if empty.
    """
    self._drain_ready(queue_name)
    return self._connection_manager.get_queue_backend().pop(queue_name, timeout)

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, object | None]:
    """Drain due held items, then pop the live queue (threads ack token, #28).

    Same drain-then-pop flow as :meth:`pop`, but delegates the live pop to
    :meth:`QueueStrategy._pop_backend_with_ack` so MQ backends keep their
    deferred-ack token instead of silently falling back to atomic ``pop()``
    (pre-fix the inherited base default dropped the token). Held items are
    re-pushed (not popped), so no token is involved in the drain.
    """
    self._drain_ready(queue_name)
    return self._pop_backend_with_ack(queue_name, timeout)

  def _drain_ready(self, queue_name: str) -> None:
    """Move all due held items into the live queue.

    Each drained item is re-pushed with the priority the caller originally
    passed to :meth:`push` (R14-F). Pre-fix this dropped priority on drain,
    silently landing every delayed item at priority 0 — a priority inversion
    for any user mixing ``delay`` + ``priority``. Priority is the 4th slot
    of the heap tuple; the live push uses the keyword form so backend
    ``push(queue_name, item, priority)`` signatures are honored either way.

    Args:
        queue_name: The queue name to drain into.
    """
    qb = self._connection_manager.get_queue_backend()
    now = self._clock()
    while self._holding and self._holding[0][0] <= now:
      _, _, item, priority = heapq.heappop(self._holding)
      qb.push(queue_name, item, priority)

  def queue_len(self, queue_name: str) -> int:
    """Return live-queue length plus held-item count.

    Args:
        queue_name: The queue name.

    Returns:
        Number of pending live items plus held (delayed) items.
    """
    return (
      self._connection_manager.get_queue_backend().queue_len(queue_name)
      + len(self._holding)
    )

  def clear(self, queue_name: str) -> None:
    """Clear the live queue and all held items.

    Unlike :meth:`close`, this does NOT warn about discarded held items:
    ``clear`` is an explicit flush requested by the caller, so silent
    discard is the intended contract. ``close`` warns because held items
    present at shutdown indicate unexpected loss.

    Args:
        queue_name: The queue name.
    """
    self._connection_manager.get_queue_backend().clear_queue(queue_name)
    self._holding.clear()

  def close(self) -> None:
    """Release resources, warning about any held (delayed) items.

    Held items live in-process, so any still-pending delayed items are
    lost on close/restart. Make that loss non-silent: emit a WARNING with
    the discarded count, then clear the holding heap.

    If ``_holding`` is empty, this is a quiet no-op (clears nothing).
    """
    held = len(self._holding)
    if held > 0:
      logger.warning(
        "DelayQueueStrategy close: discarding %d held delayed item(s) "
        "from the in-process holding queue; these delayed items are lost "
        "on close/restart (non-silent data loss).",
        held,
      )
    self._holding.clear()

  def snapshot(self) -> bytes | None:
    """Serialize the holding heap for restart recovery (initiative #3).

    Returns ``None`` when the heap is empty (nothing to persist). Otherwise
    a versioned JSON blob: ``{"version":1,"strategy":"delay","items":[
    {"ready_at":..,"item_b64":..,"priority":..},...]}``.

    The ``seq`` tie-breaker is deliberately NOT persisted — it's a monotonic
    process-local counter; :meth:`restore` re-sequences with a fresh ``_seq``,
    preserving heap order (seq only breaks ``ready_at`` ties, and the
    serialized list order preserves relative order among equal-``ready_at``
    items, so the rebuilt heap is equivalent).
    """
    if not self._holding:
      return None
    items = [
      {
        "ready_at": ready_at,
        "item_b64": base64.b64encode(item).decode("ascii"),
        "priority": priority,
      }
      for ready_at, _seq, item, priority in self._holding
    ]
    return json.dumps(
      {"version": 1, "strategy": "delay", "items": items}
    ).encode("utf-8")

  def restore(self, state: bytes | None) -> None:
    """Re-populate the holding heap from a prior :meth:`snapshot` (initiative #3).

    Past-ready items (``ready_at <= now``) stay in the heap and drain
    naturally on the next :meth:`pop`. Corrupt or unknown-format state is
    logged + skipped — restore never crashes the spider.

    Args:
        state: The bytes blob from a prior :meth:`snapshot`, or ``None``.
    """
    if not state:
      return
    try:
      data = json.loads(state.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
      logger.warning(
        "DelayQueueStrategy restore: corrupt snapshot (%s); starting clean.", e
      )
      return
    if (
      not isinstance(data, dict)
      or data.get("strategy") != "delay"
      or data.get("version") != 1
    ):
      logger.warning(
        "DelayQueueStrategy restore: unknown snapshot format "
        "(strategy=%r, version=%r); starting clean.",
        data.get("strategy") if isinstance(data, dict) else None,
        data.get("version") if isinstance(data, dict) else None,
      )
      return
    items = data.get("items")
    if not isinstance(items, list):
      logger.warning(
        "DelayQueueStrategy restore: snapshot 'items' not a list; starting clean."
      )
      return
    recovered = 0
    for entry in items:
      if not isinstance(entry, dict):
        continue
      try:
        ready_at = float(entry["ready_at"])
        item = base64.b64decode(entry["item_b64"])
        priority = float(entry["priority"])
      except (KeyError, TypeError, ValueError) as e:
        logger.warning(
          "DelayQueueStrategy restore: skipping malformed entry (%s).", e
        )
        continue
      heapq.heappush(self._holding, (ready_at, next(self._seq), item, priority))
      recovered += 1
    if recovered:
      logger.info(
        "DelayQueueStrategy restore: recovered %d held delayed item(s) from snapshot.",
        recovered,
      )
