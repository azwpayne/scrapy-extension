"""Abstract membership-filter interface for pluggable dedup (subsystem ①).

Defines :class:`MembershipFilter` — the strategy interface that
:class:`~scrapy_extension.dupefilter.dupefilter.BackendDupeFilter` delegates
duplicate detection to. Concrete strategies live alongside this module
(``set_filter``, ``memory_filter``, ``bloom_filter``, ``cuckoo_filter``).
"""

from __future__ import annotations

__all__ = ["FilterFull", "MembershipFilter"]

from abc import ABC, abstractmethod


class FilterFull(RuntimeError):
  """The membership filter is at capacity and cannot accept more items.

  Raised by bounded-capacity probabilistic filters (currently the cuckoo
  filter, when insertion exhausts its ``_MAX_KICKS`` budget). Callers that
  can degrade — e.g. :class:`~scrapy_extension.dupefilter.dupefilter.BackendDupeFilter`,
  which treats an overflow request as not-seen rather than crashing the
  crawl — catch this; others let it propagate.

  Lives on the abstract interface (not on a concrete filter) so the
  dupefilter layer catches ``FilterFull`` by type without importing concrete
  strategy classes — preserving layering and decoupling the catch from any
  particular filter's error-message wording.
  """


class MembershipFilter(ABC):
  """Strategy interface for duplicate detection.

  A membership filter answers "has this item been seen before?". The
  dupefilter calls :meth:`add` with a request fingerprint and treats a
  ``False`` return as "duplicate". Implementations are either exact
  (``set``, ``memory``) or probabilistic (``bloom``, ``cuckoo``).

  Probabilistic filters never produce false negatives but may produce
  false positives (an unseen item reported as already seen). Exactness
  and cross-worker sharing vary by implementation — see each strategy's
  docstring for its guarantees.
  """

  @abstractmethod
  def add(self, item: bytes) -> bool:
    """Record an item, returning whether it was newly added.

    Args:
        item: The item to record (a request fingerprint as bytes).

    Returns:
        True if the item was not present before this call (newly added),
        False if it was already present (a duplicate). Probabilistic
        filters return False when their bits/fingerprint already match,
        which may be a false positive.
    """

  @abstractmethod
  def __contains__(self, item: bytes) -> bool:
    """Check membership without adding.

    Args:
        item: The item to check.

    Returns:
        True if the item is (probably) present.
    """

  @abstractmethod
  def __len__(self) -> int:
    """Return the (approximate) number of tracked items.

    Returns:
        Item count. Probabilistic filters return an estimate.
    """

  @abstractmethod
  def clear(self) -> None:
    """Remove all tracked items."""

  def open(self) -> None:  # noqa: B027
    """Lifecycle hook — prepare the filter for use. Default no-op.

    Concrete strategies override this when they need setup (e.g. a
    backend-backed probabilistic filter loading prior state). The default
    is intentionally an empty implementation, not abstract — silence
    B027 explicitly.
    """

  def close(self) -> None:  # noqa: B027
    """Lifecycle hook — release resources. Default no-op."""

  def remove(self, item: bytes) -> bool:
    """Remove an item.

    Args:
        item: The item to remove.

    Returns:
        True if the item was present and removed.

    Raises:
        NotImplementedError: If this strategy does not support removal.
    """
    del item
    raise NotImplementedError(
      f"{type(self).__name__} does not support item removal"
    )
