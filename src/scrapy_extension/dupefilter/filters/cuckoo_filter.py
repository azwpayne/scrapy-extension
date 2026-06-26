"""Stdlib Cuckoo-filter membership strategy (subsystem ①).

Probabilistic, in-process, space-efficient, and supports deletion (unlike
Bloom). Never produces false negatives: fingerprints are only ever moved
between their two valid buckets during eviction, never dropped. State is
per-process, not shared across workers.
"""

from __future__ import annotations

__all__ = ["CuckooMembershipFilter"]

import hashlib
import math
import random

from scrapy_extension.dupefilter.filters.base import FilterFull, MembershipFilter


class CuckooMembershipFilter(MembershipFilter):
  """Pure-stdlib Cuckoo filter (Fan et al. 2014).

  Bucket array of ``m`` buckets (power of two), each holding up to
  ``b = 4`` fingerprints. An item maps to a fingerprint ``fp`` and a
  primary index ``i1``; its alternate index is ``i2 = i1 ^ hash(fp)``,
  so either bucket can be found from the other given ``fp``.

  Insertion places ``fp`` in a free slot of ``i1`` or ``i2``; if both are
  full it evicts a random occupant (a "kick") and re-inserts that
  occupant into its alternate bucket, bounded by ``_MAX_KICKS``. Because
  every placement lives in one of the fingerprint's two valid buckets,
  ``contains`` and ``remove`` always find a previously-inserted item.

  Attributes:
      _num_buckets: Bucket count ``m`` (power of two).
      _fp_len: Fingerprint length in bytes.
      _buckets: ``list[list[bytes]]`` of length ``m``.
      _count: Number of distinct items recorded.
      _rng: Source of randomness for eviction slot selection.
  """

  _BUCKET_SIZE = 4
  _MAX_KICKS = 500
  _TARGET_LOAD = 0.85

  def __init__(self, *, capacity: int, error_rate: float) -> None:
    """Size the filter for a target capacity and false-positive rate.

    Args:
        capacity: Expected number of items (n).
        error_rate: Target false-positive probability at ``capacity`` items.

    Raises:
        ValueError: If capacity is not positive or error_rate is outside (0, 1).
    """
    if capacity <= 0:
      raise ValueError(f"capacity must be a positive integer, got {capacity}")
    if not 0.0 < error_rate < 1.0:
      raise ValueError(
        f"error_rate must be in the open interval (0, 1), got {error_rate}"
      )
    b = self._BUCKET_SIZE
    fp_bits = math.ceil(math.log2(1 / error_rate) + math.log2(2 * b))
    self._fp_len = max(1, (fp_bits + 7) >> 3)
    # Size buckets for ~85% load, rounded up to a power of two so the
    # two-index xor scheme can mask with (m - 1).
    ideal = math.ceil(capacity / b / self._TARGET_LOAD)
    m = 2
    while m < ideal:
      m <<= 1
    self._num_buckets = m
    self._buckets: list[list[bytes]] = [[] for _ in range(m)]
    self._count = 0
    # nosec B311: random.Random drives only the cuckoo-kick eviction slot
    # selection, a non-cryptographic choice. Switching to ``secrets`` would
    # change the distribution and subtly alter the filter's false-positive
    # behavior without any security benefit — fingerprints are keyed by
    # SHA-256, not by this RNG.
    self._rng = random.Random()  # nosec B311 - eviction-slot jitter, not cryptographic

  @property
  def num_buckets(self) -> int:
    """Number of buckets (m)."""
    return self._num_buckets

  @property
  def fp_len(self) -> int:
    """Fingerprint length in bytes."""
    return self._fp_len

  @property
  def capacity(self) -> int:
    """Hard capacity in items (``m * _BUCKET_SIZE``).

    The filter is sized for ~85% load (see :meth:`__init__`), so steady-state
    saturation hovers near 0.85 and :meth:`add` raises
    :class:`~scrapy_extension.dupefilter.filters.base.FilterFull` once kicks
    exhaust ``_MAX_KICKS`` (effectively at/above capacity). Exposed so the
    dupefilter can emit a leading saturation signal (U2 operability) as the
    filter APPROACHES full — ``used / capacity`` rises through ~0.9 before
    the overflow signal ever fires.
    """
    return self._num_buckets * self._BUCKET_SIZE

  @property
  def saturation(self) -> float:
    """Current fill ratio (``used / capacity``), in ``[0.0, ~1.0]``.

    Used by :meth:`BackendDupeFilter.request_seen
    <scrapy_extension.dupefilter.dupefilter.BackendDupeFilter.request_seen>`
    to emit ``on_filter_saturation`` after each add. ``used`` is the count of
    inserted fingerprints still present (``len(self)``); ``capacity`` is the
    hard bucket-slot count. The target load is 0.85 by design, so a healthy
    filter reads ~0.85 at its configured capacity — operators should alert
    on a rising edge past ~0.90, not on reaching 0.85.
    """
    return len(self) / self.capacity

  def _fingerprint(self, item: bytes) -> tuple[bytes, int]:
    """Derive the fingerprint and primary index for ``item``.

    Args:
        item: Fingerprint bytes.

    Returns:
        ``(fp, i1)`` — the nonzero fingerprint and its primary bucket index.
    """
    digest = hashlib.sha256(item).digest()
    fp = digest[: self._fp_len]
    if fp == b"\x00" * self._fp_len:
      # Zero is the empty-slot sentinel; nudge to a nonzero value.
      fp = b"\x01" + fp[1:]
    i1 = int.from_bytes(digest[8:16], "big") & (self._num_buckets - 1)
    return fp, i1

  def _hash_fp(self, fp: bytes) -> int:
    """Masked hash of a fingerprint, used to compute alternate indices.

    Args:
        fp: A fingerprint.

    Returns:
        A bucket-offset in [0, m) suitable for xor with an index.
    """
    digest = hashlib.sha256(fp).digest()
    return int.from_bytes(digest[:8], "big") & (self._num_buckets - 1)

  def _alt_index(self, index: int, fp: bytes) -> int:
    """Return the alternate bucket index for ``fp`` given one of its indices.

    Args:
        index: One of fp's two bucket indices.
        fp: The fingerprint.

    Returns:
        The other valid bucket index for ``fp``.
    """
    return index ^ self._hash_fp(fp)

  def add(self, item: bytes) -> bool:
    """Record item; True if newly added, False if (probably) already present.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if the fingerprint was newly inserted, False if already present.

    Raises:
        FilterFull: If the filter is full (exceeded ``_MAX_KICKS`` on
            insertion). Increase ``capacity`` or switch strategy.
    """
    fp, i1 = self._fingerprint(item)
    i2 = self._alt_index(i1, fp)
    buckets = self._buckets
    if fp in buckets[i1] or fp in buckets[i2]:
      return False
    if self._insert(fp, i1, i2):
      self._count += 1
      return True
    raise FilterFull(
      f"Cuckoo filter is full (capacity reached, {self._num_buckets} buckets); "
      f"increase capacity or use a different dedup strategy"
    )

  def _insert(self, fp: bytes, i1: int, i2: int) -> bool:
    """Place ``fp`` in a free slot of i1/i2, kicking if both full.

    Every placement lands in one of fp's two valid buckets, so the table
    stays consistent even when kicks exhaust ``_MAX_KICKS`` and insertion
    fails.

    Args:
        fp: Fingerprint to insert.
        i1: Primary bucket index.
        i2: Alternate bucket index.

    Returns:
        True if inserted, False if the table is full.
    """
    b = self._BUCKET_SIZE
    buckets = self._buckets
    if len(buckets[i1]) < b:
      buckets[i1].append(fp)
      return True
    if len(buckets[i2]) < b:
      buckets[i2].append(fp)
      return True
    index = i1
    for _ in range(self._MAX_KICKS):
      slot = self._rng.randrange(b)
      # Swap fp into bucket[index][slot]; the evicted value becomes fp.
      fp, buckets[index][slot] = buckets[index][slot], fp
      index = self._alt_index(index, fp)
      if len(buckets[index]) < b:
        buckets[index].append(fp)
        return True
    return False

  def __contains__(self, item: bytes) -> bool:
    """Check (probabilistic) membership.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if the fingerprint is found in either of its two buckets.
    """
    fp, i1 = self._fingerprint(item)
    i2 = self._alt_index(i1, fp)
    buckets = self._buckets
    return fp in buckets[i1] or fp in buckets[i2]

  def __len__(self) -> int:
    """Return the number of distinct items recorded.

    Returns:
        Count of successfully inserted fingerprints still present.
    """
    return self._count

  def clear(self) -> None:
    """Reset the filter to empty."""
    self._buckets = [[] for _ in range(self._num_buckets)]
    self._count = 0

  def remove(self, item: bytes) -> bool:
    """Remove an item.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if the fingerprint was found and removed, False otherwise.
    """
    fp, i1 = self._fingerprint(item)
    i2 = self._alt_index(i1, fp)
    buckets = self._buckets
    if fp in buckets[i1]:
      buckets[i1].remove(fp)
      self._count -= 1
      return True
    if fp in buckets[i2]:
      buckets[i2].remove(fp)
      self._count -= 1
      return True
    return False
