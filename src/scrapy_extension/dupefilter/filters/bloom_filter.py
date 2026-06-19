"""Stdlib Bloom-filter membership strategy (subsystem ①).

Probabilistic, in-process, space-efficient. Never produces false negatives;
false-positive rate is bounded by ``error_rate`` at ``capacity`` items. Does
not support deletion — use the cuckoo strategy for that. State is per-process,
not shared across workers.
"""

from __future__ import annotations

__all__ = ["BloomMembershipFilter"]

import hashlib
import math

from scrapy_extension.dupefilter.filters.base import MembershipFilter


class BloomMembershipFilter(MembershipFilter):
  """Pure-stdlib Bloom filter.

  Uses a ``bytearray`` bit-vector and Kirsch-Mitzenmacher double hashing:
  ``g_i(x) = (h1 + i·h2) mod m``, where ``h1`` and ``h2`` are the two 64-bit
  halves of ``sha256(item)``. ``k`` hash functions are derived from just two.

  Attributes:
      _num_bits: Bit-vector length ``m``.
      _num_hashes: Number of hash functions ``k``.
      _bits: The bytearray backing the bit-vector.
      _count: Number of items that set at least one new bit (approx count).
  """

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
    ln2 = math.log(2)
    m = max(8, math.ceil(-capacity * math.log(error_rate) / (ln2 * ln2)))
    k = max(1, round((m / capacity) * ln2))
    self._num_bits = m
    self._num_hashes = k
    self._bits = bytearray((m + 7) >> 3)
    self._count = 0

  @property
  def num_bits(self) -> int:
    """Number of bits in the filter (m)."""
    return self._num_bits

  @property
  def num_hashes(self) -> int:
    """Number of hash functions (k)."""
    return self._num_hashes

  def _indices(self, item: bytes):
    """Yield the k bit positions for ``item`` via double hashing.

    Two 64-bit seeds come from a single ``sha256``; the k indices are
    ``g_i = (h1 + i·h2) mod m`` for i in 0..k-1.

    Args:
        item: Fingerprint bytes.

    Yields:
        Bit positions in [0, m).
    """
    digest = hashlib.sha256(item).digest()
    h1 = int.from_bytes(digest[:8], "big")
    h2 = int.from_bytes(digest[8:16], "big")
    m = self._num_bits
    for i in range(self._num_hashes):
      yield (h1 + i * h2) % m

  def add(self, item: bytes) -> bool:
    """Record item; True if newly added, False if (probably) already present.

    Never returns False for an item not previously inserted (no false
    negatives). A False return may be a false positive.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if at least one bit was previously unset (new), False if all
        k bits were already set (probably seen).
    """
    bits = self._bits
    already_present = True
    for idx in self._indices(item):
      byte_idx = idx >> 3
      mask = 1 << (idx & 7)
      if not (bits[byte_idx] & mask):
        already_present = False
        bits[byte_idx] |= mask
    if not already_present:
      self._count += 1
    return not already_present

  def __contains__(self, item: bytes) -> bool:
    """Check (probabilistic) membership.

    Args:
        item: Fingerprint bytes.

    Returns:
        True if all k bits are set (item probably present).
    """
    bits = self._bits
    for idx in self._indices(item):
      if not (bits[idx >> 3] & (1 << (idx & 7))):
        return False
    return True

  def __len__(self) -> int:
    """Return the approximate number of recorded items.

    Returns:
        Count of items that set at least one new bit.
    """
    return self._count

  def clear(self) -> None:
    """Reset the filter to empty."""
    self._bits = bytearray(len(self._bits))
    self._count = 0
