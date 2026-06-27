"""In-process exact membership filter with optional LRU cap (subsystem ①).

Per-process, non-distributed: state is NOT shared across workers. Use for
single-worker crawls or as a fast local cache. For cross-worker exact dedup
use the ``set`` strategy (SetBackend).
"""

from __future__ import annotations

__all__ = ["DEFAULT_MEMORY_MAXSIZE", "MemoryMembershipFilter"]

import logging
from collections import OrderedDict
from typing import TYPE_CHECKING

from scrapy_extension.dupefilter.filters.base import MembershipFilter

if TYPE_CHECKING:
  from scrapy_extension.monitor.base import Monitor

logger = logging.getLogger(__name__)

# SPEC-round8-tier1 U5 — OOM prevention default. Measured ~481 B/entry ⇒
# ~366 MB at 1M entries, ~3.58 GB at 10M. The 1M default bounds a long crawl's
# memory footprint without an explicit setting; the LRU eviction mechanism
# already existed, we ship a sane default. Explicit maxsize=None remains the
# advanced opt-out for users who need unbounded growth and accept the risk.
DEFAULT_MEMORY_MAXSIZE: int = 1_000_000

# Module-level cache so the eviction warning fires once per process even when
# many filters are constructed (multi-spider process). Mirrors the
# dupefilter/filters/factory.py `_warned` pattern. Tests reset this to verify
# the warn-once contract from a clean slate.
_evicted_warned: bool = False


class MemoryMembershipFilter(MembershipFilter):
  """Exact, in-process membership filter with an optional LRU bound.

  Stores fingerprints in an :class:`~collections.OrderedDict`; re-adding an
  item marks it most-recently-used. When ``maxsize`` is set and the filter
  is full, the least-recently-used item is evicted on the next insert.

  State is local to the process — not shared across distributed workers.
  For multi-worker exact dedup, use the ``set`` strategy.

  Eviction tradeoff (SPEC U5): an evicted fingerprint is forgotten, so a
  re-crawl of that URL becomes possible (false-negative on dedup). This is
  surfaced via a one-time per-process WARNING at first eviction so operators
  can choose a higher ``maxsize`` or switch to the backend-backed ``set``
  strategy for exact cross-process dedup.

  Attributes:
      _maxsize: Capacity cap; None = unbounded (advanced opt-out).
          Defaults to :data:`DEFAULT_MEMORY_MAXSIZE` (1_000_000).
      _data: Insertion/access-ordered mapping of fingerprints.
      _monitor: Optional observability monitor threaded in by
          :class:`~scrapy_extension.dupefilter.dupefilter.BackendDupeFilter`
          so LRU eviction can emit ``on_filter_saturation`` (R14-D).
          ``None`` (default) → eviction stays silent (logs only).
  """

  def __init__(self, *, maxsize: int | None = DEFAULT_MEMORY_MAXSIZE) -> None:
    """Initialize the memory filter.

    Args:
        maxsize: Maximum items to retain before evicting the
            least-recently-used. Defaults to :data:`DEFAULT_MEMORY_MAXSIZE`
            (1_000_000) to prevent silent OOM on long crawls. Pass ``None``
            for unbounded growth (advanced opt-out — accepts the OOM risk).

    Raises:
        ValueError: If maxsize is a non-positive integer.
    """
    if maxsize is not None and maxsize <= 0:
      raise ValueError(
        f"maxsize must be a positive integer or None, got {maxsize}"
      )
    self._maxsize = maxsize
    self._data: OrderedDict[bytes, None] = OrderedDict()
    # R14-D: monitor is threaded in AFTER construction by the dupefilter
    # (which owns the monitor). Stays ``None`` when the filter is used
    # standalone → eviction is silent (warn-only), preserving prior
    # behavior outside the dupefilter context.
    self._monitor: Monitor | None = None

  def set_monitor(self, monitor: Monitor) -> None:
    """Thread the dupefilter's monitor so eviction can emit saturation (R14-D).

    Called by :meth:`BackendDupeFilter.__init__
    <scrapy_extension.dupefilter.dupefilter.BackendDupeFilter.__init__>` once
    it has resolved its own monitor. Idempotent; safe to call before or after
    the first eviction. No-op effect on a NullMonitor (the no-op default) —
    the eviction warning log is independent of this hook.

    Args:
        monitor: The monitor to emit ``on_filter_saturation`` through.
    """
    self._monitor = monitor

  def _warn_evicted_once(self) -> None:
    """Emit a one-time per-process warning when LRU eviction first fires.

    Eviction means an already-seen URL can be re-admitted (dedup
    false-negative → re-crawl). Make that tradeoff non-silent once per
    process. Idempotent via the module-level ``_evicted_warned`` flag so a
    multi-spider process does not spam the log.
    """
    global _evicted_warned
    if _evicted_warned:
      return
    _evicted_warned = True
    logger.warning(
      "MemoryMembershipFilter reached its maxsize cap (%s) and is now "
      "evicting least-recently-used fingerprints. Evicted entries are "
      "forgotten, so their URLs may be re-crawled (dedup false-negative). "
      "Raise maxsize, pass maxsize=None for unbounded growth (OOM risk), "
      "or switch to the backend-backed 'set' strategy for exact cross-"
      "process dedup.",
      f"{self._maxsize:,}" if self._maxsize is not None else "None",
    )

  def add(self, item: bytes) -> bool:
    """Record an item; True if new, False if already present.

    Re-adding an existing item updates its LRU position so frequently-seen
    fingerprints are not evicted.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if newly added, False if already present.
    """
    if item in self._data:
      self._data.move_to_end(item)
      return False
    if self._maxsize is not None and len(self._data) >= self._maxsize:
      self._data.popitem(last=False)  # evict least-recently-used
      self._warn_evicted_once()
    self._data[item] = None
    # R14-D: emit ``on_filter_saturation`` when the filter is at cap (after
    # the eviction + insert, len == maxsize). This is the saturation ceiling
    # the operator cares about — "the filter is full and evicting". Emitted
    # unconditionally when a maxsize is set AND the filter is at cap, so a
    # sustained eviction storm keeps the gauge pinned at 1.0 (matching the
    # cuckoo/bloom ``used/capacity`` contract). No-op when no monitor was
    # threaded (standalone filter use).
    if (
      self._monitor is not None
      and self._maxsize is not None
      and len(self._data) >= self._maxsize
    ):
      self._monitor.on_filter_saturation(len(self._data), self._maxsize)
    return True

  def __contains__(self, item: bytes) -> bool:
    """Check membership (read-only — does not affect LRU order).

    Args:
        item: Fingerprint bytes.

    Returns:
        True if the item is tracked.
    """
    return item in self._data

  def __len__(self) -> int:
    """Return the number of tracked items.

    Returns:
        Current item count.
    """
    return len(self._data)

  def clear(self) -> None:
    """Remove all tracked items."""
    self._data.clear()

  def remove(self, item: bytes) -> bool:
    """Remove an item.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if the item was present and removed, False otherwise.
    """
    if item in self._data:
      del self._data[item]
      return True
    return False
