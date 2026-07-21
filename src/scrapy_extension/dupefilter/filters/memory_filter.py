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
      _monitor: Optional standalone observability monitor. A containing
          :class:`~scrapy_extension.dupefilter.dupefilter.BackendDupeFilter`
          installs a ``NullMonitor`` here and records the equivalent event for
          ordered dispatch after its lock is released. ``None`` (default)
          means capacity events stay silent (the eviction warning still logs).
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
    # Standalone filters may opt into direct saturation telemetry. A containing
    # dupefilter installs NullMonitor and publishes its recorded equivalent
    # outside the lifecycle lock instead.
    self._monitor: Monitor | None = None

  def set_monitor(self, monitor: Monitor) -> None:
    """Thread a monitor so finite-capacity saturation can be emitted (R14-D).

    A containing :class:`BackendDupeFilter
    <scrapy_extension.dupefilter.dupefilter.BackendDupeFilter>` installs a
    ``NullMonitor`` because it dispatches the equivalent recorded event after
    releasing its own lock. Standalone callers may provide a real monitor.
    Idempotent; safe before or after the first capacity event. The eviction
    warning log is independent of this hook.

    Args:
        monitor: The monitor to emit ``on_filter_saturation`` through.
    """
    self._monitor = monitor

  def _emit_saturation(self, used: int, capacity: int) -> None:
    """Publish saturation without letting telemetry reject an insertion."""
    if self._monitor is None:
      return
    try:
      self._monitor.on_filter_saturation(used, capacity)
    except Exception:  # noqa: BLE001 - telemetry must not alter filter state
      try:
        logger.debug(
          "Memory filter saturation monitor hook raised; ignored",
          exc_info=True,
        )
      except Exception:  # noqa: BLE001 - diagnostics are best effort too
        return

  @property
  def saturation(self) -> float | None:
    """Return the finite-cap signal only once the filter is full."""
    if self._maxsize is None or len(self._data) < self._maxsize:
      return None
    return 1.0

  @property
  def capacity(self) -> int | None:
    """Return the configured finite item cap, or ``None`` when unbounded."""
    return self._maxsize

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
    # R14-D: emit after a successful insert first reaches or remains at cap,
    # with or without an eviction (len == maxsize). This is the saturation
    # ceiling the operator cares about. Sustained eviction keeps the gauge
    # pinned at 1.0 (matching the cuckoo/bloom ``used/capacity`` contract).
    # No-op when no monitor was threaded (standalone filter use).
    if (
      self._monitor is not None
      and self._maxsize is not None
      and len(self._data) >= self._maxsize
    ):
      self._emit_saturation(len(self._data), self._maxsize)
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
