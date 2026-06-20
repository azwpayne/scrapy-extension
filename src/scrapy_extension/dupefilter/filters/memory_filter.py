"""In-process exact membership filter with optional LRU cap (subsystem ①).

Per-process, non-distributed: state is NOT shared across workers. Use for
single-worker crawls or as a fast local cache. For cross-worker exact dedup
use the ``set`` strategy (SetBackend).
"""

from __future__ import annotations

__all__ = ["MemoryMembershipFilter"]

from collections import OrderedDict

from scrapy_extension.dupefilter.filters.base import MembershipFilter


class MemoryMembershipFilter(MembershipFilter):
  """Exact, in-process membership filter with an optional LRU bound.

  Stores fingerprints in an :class:`~collections.OrderedDict`; re-adding an
  item marks it most-recently-used. When ``maxsize`` is set and the filter
  is full, the least-recently-used item is evicted on the next insert.

  State is local to the process — not shared across distributed workers.
  For multi-worker exact dedup, use the ``set`` strategy.

  Attributes:
      _maxsize: Optional capacity cap; None = unbounded (grows indefinitely).
      _data: Insertion/access-ordered mapping of fingerprints.
  """

  def __init__(self, *, maxsize: int | None = None) -> None:
    """Initialize the memory filter.

    Args:
        maxsize: Maximum items to retain before evicting the
            least-recently-used. None (default) = unbounded.

    Raises:
        ValueError: If maxsize is a non-positive integer.
    """
    if maxsize is not None and maxsize <= 0:
      raise ValueError(
        f"maxsize must be a positive integer or None, got {maxsize}"
      )
    self._maxsize = maxsize
    self._data: OrderedDict[bytes, None] = OrderedDict()

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
    self._data[item] = None
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
