"""Stdlib Bloom-filter membership strategy (subsystem ①).

Probabilistic, in-process, space-efficient. Never produces false negatives;
false-positive rate is bounded by ``error_rate`` at ``capacity`` items. Does
not support deletion — use the cuckoo strategy for that. State is per-process,
not shared across workers.

Each item maps to ``k`` distinct positions. The sizing bound therefore uses
sampling without replacement within an item rather than assuming that repeated
hash positions provide ``k`` independent checks. Allocations are capped at a
conservative 128 MiB per filter so extreme, otherwise-valid settings fail before
``bytearray`` can exhaust the process.
"""

from __future__ import annotations

__all__ = ["BloomMembershipFilter"]

import hashlib
import math
from collections.abc import Iterator

from scrapy_extension.dupefilter.filters.base import MembershipFilter

_MAX_HASHES = 64
_HASH_DOMAIN = b"scrapy-extension:bloom:v1\x00"
_HASH_RANGE = 1 << 256
# A process can host multiple spiders/filters. Keep a single Bloom vector at or
# below 128 MiB so configuration mistakes cannot consume all typical worker RAM.
_MAX_FILTER_BYTES = 128 * 1024 * 1024


class _BloomFilterAllocationError(ValueError):
    """The requested Bloom sizing exceeds the process-safe allocation budget."""


class BloomMembershipFilter(MembershipFilter):
    """Pure-stdlib Bloom filter.

    Uses a ``bytearray`` bit-vector. Domain-separated SHA-256 samples drive
    Floyd's algorithm for uniform sampling without replacement, so every item
    maps to exactly ``k`` distinct positions.

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
            TypeError: If capacity is not an integer or error_rate is not a float.
            ValueError: If capacity is not positive, error_rate is not finite and
                inside (0, 1), or the resulting vector exceeds the 128 MiB budget.
        """
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise TypeError(
                f"capacity must be a positive integer, got {type(capacity).__name__}"
            )
        if capacity <= 0:
            raise ValueError(f"capacity must be a positive integer, got {capacity}")
        if isinstance(error_rate, bool) or not isinstance(error_rate, float):
            raise TypeError(
                f"error_rate must be a float, got {type(error_rate).__name__}"
            )
        if not math.isfinite(error_rate) or not 0.0 < error_rate < 1.0:
            raise ValueError(
                f"error_rate must be a finite float in the open interval (0, 1), "
                f"got {error_rate}"
            )

        # The continuous optimum is -log2(p). Choose an integer k first, cap
        # hash work at a practical constant, and then solve m for that exact k.
        # Capping k does not relax the requested bound: m grows as necessary.
        k = min(_MAX_HASHES, max(1, round(-math.log2(error_rate))))
        max_bits = _MAX_FILTER_BYTES * 8
        # Every viable vector has m > n. This cheap integer-only check handles
        # arbitrarily large Python ints without first converting them to float.
        if capacity >= max_bits:
            self._raise_allocation_error(capacity + 1)
        target_root = math.exp(math.log(error_rate) / k)
        denominator = -math.expm1(math.log1p(-target_root) / capacity)
        m = max(k + 1, math.ceil(k / denominator))
        self._check_allocation(m)

        # Guard against downward floating-point rounding at the boundary.
        # Compare in log space so very small representable rates stay stable.
        log_error_rate = math.log(error_rate)
        while self._log_false_positive_bound(capacity, m, k) > log_error_rate:
            m += 1
            self._check_allocation(m)

        self._num_bits = m
        self._num_hashes = k
        self._bits = bytearray((m + 7) >> 3)
        self._count = 0
        # R14-D: store the configured item-capacity so :attr:`capacity` +
        # :attr:`saturation` mirror the cuckoo filter and let
        # ``BackendDupeFilter.request_seen`` emit the leading saturation signal
        # for Bloom filters (was cuckoo-only). ``_num_bits`` is the bit-vector
        # length, NOT an item-capacity, so a separate field is required.
        self._capacity = capacity

    @property
    def num_bits(self) -> int:
        """Number of bits in the filter (m)."""
        return self._num_bits

    @property
    def num_hashes(self) -> int:
        """Number of hash functions (k)."""
        return self._num_hashes

    @property
    def capacity(self) -> int:
        """Configured item-capacity ``n`` used to size the filter (R14-D).

        The Bloom filter never hard-refuses an insert (unlike the cuckoo filter,
        which raises :class:`FilterFull`), so ``capacity`` is the SIZING target —
        the false-positive rate is bounded by ``error_rate`` AT ``capacity``
        items. Exposed so :meth:`BackendDupeFilter.request_seen
        <scrapy_extension.dupefilter.dupefilter.BackendDupeFilter.request_seen>`
        can emit a leading ``on_filter_saturation`` signal as the count
        approaches the configured capacity (mirror of the cuckoo property).
        """
        return self._capacity

    @property
    def saturation(self) -> float:
        """Current fill ratio (``len / capacity``), in ``[0.0, ~1.0+]`` (R14-D).

        Used by :meth:`BackendDupeFilter.request_seen
        <scrapy_extension.dupefilter.dupefilter.BackendDupeFilter.request_seen>`
        to emit ``on_filter_saturation`` after each add. ``used`` is the
        approximate count of inserted items (``len(self)``); ``capacity`` is the
        configured sizing target. A healthy filter reads ~0.85 at its configured
        capacity — operators should alert on a rising edge past ~0.90. May
        exceed ``1.0`` if more items than ``capacity`` are inserted (the Bloom
        filter has no hard cap, but false-positive rate degrades past it);
        ``ScrapyStatsMonitor.on_filter_saturation`` clamps the gauge to 1.0.
        """
        return len(self) / self._capacity

    @staticmethod
    def _log_false_positive_bound(
        capacity: int, num_bits: int, num_hashes: int
    ) -> float:
        """Return the log FPR upper bound for distinct per-item positions."""
        log_unset_probability = capacity * math.log1p(-num_hashes / num_bits)
        set_probability = -math.expm1(log_unset_probability)
        return num_hashes * math.log(set_probability)

    @staticmethod
    def _raise_allocation_error(num_bits: int) -> None:
        """Raise a bounded-allocation error without constructing the vector."""
        required_bytes = (num_bits + 7) >> 3
        raise _BloomFilterAllocationError(
            f"capacity/error_rate pair requires at least {required_bytes} bytes, "
            f"exceeding the {_MAX_FILTER_BYTES}-byte memory budget"
        )

    @classmethod
    def _check_allocation(cls, num_bits: int) -> None:
        """Reject a rounded byte length above the documented memory budget."""
        if (num_bits + 7) >> 3 > _MAX_FILTER_BYTES:
            cls._raise_allocation_error(num_bits)

    @staticmethod
    def _uniform_index(item: bytes, hash_index: int, upper_bound: int) -> int:
        """Derive an unbiased index in ``range(upper_bound)`` for one domain."""
        limit = _HASH_RANGE - (_HASH_RANGE % upper_bound)
        attempt = 0
        while True:
            digest = hashlib.sha256(
                _HASH_DOMAIN
                + hash_index.to_bytes(1, "big")
                + attempt.to_bytes(4, "big")
                + item
            ).digest()
            sample = int.from_bytes(digest, "big")
            if sample < limit:
                return sample % upper_bound
            attempt += 1

    def _indices(self, item: bytes) -> Iterator[int]:
        """Yield exactly k distinct, domain-separated positions for ``item``.

        Floyd's sampling algorithm selects a uniform k-subset of the m bit
        positions without allocating an m-sized auxiliary collection. Each
        sample has its own hash domain; rejection avoids modulo bias.

        Args:
            item: Fingerprint bytes.

        Yields:
            Distinct bit positions in [0, m).
        """
        m = self._num_bits
        k = self._num_hashes
        selected: set[int] = set()
        for hash_index, upper_index in enumerate(range(m - k, m)):
            position = self._uniform_index(item, hash_index, upper_index + 1)
            if position in selected:
                position = upper_index
            selected.add(position)
            yield position

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
