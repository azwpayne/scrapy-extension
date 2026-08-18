"""Tests for BloomMembershipFilter — stdlib probabilistic dedup (subsystem ①)."""

from __future__ import annotations

import builtins
import math

import pytest

from scrapy_extension.dupefilter.filters import bloom_filter as bloom_filter_module
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter


def _false_positive_upper_bound(flt: BloomMembershipFilter) -> float:
    """Upper-bound the FPR at capacity for k distinct positions per item."""
    unset_probability = math.exp(
        flt.capacity * math.log1p(-flt.num_hashes / flt.num_bits)
    )
    return (1.0 - unset_probability) ** flt.num_hashes


class TestBloomMembershipFilterSizing:
    """Capacity/error-rate validation and derived m, k."""

    @pytest.mark.parametrize("capacity", [0, -1])
    def test_invalid_capacity_value(self, capacity: int) -> None:
        with pytest.raises(ValueError, match="capacity"):
            BloomMembershipFilter(capacity=capacity, error_rate=0.01)

    @pytest.mark.parametrize("capacity", [True, 1.0, "100", None])
    def test_invalid_capacity_type(self, capacity: object) -> None:
        with pytest.raises(TypeError, match="capacity"):
            BloomMembershipFilter(capacity=capacity, error_rate=0.01)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "error_rate", [0.0, 1.0, 1.5, -0.1, math.inf, -math.inf, math.nan]
    )
    def test_invalid_error_rate_value(self, error_rate: float) -> None:
        with pytest.raises(ValueError, match="error_rate"):
            BloomMembershipFilter(capacity=100, error_rate=error_rate)

    @pytest.mark.parametrize("error_rate", [True, 1, "0.01", None])
    def test_invalid_error_rate_type(self, error_rate: object) -> None:
        with pytest.raises(TypeError, match="error_rate"):
            BloomMembershipFilter(capacity=100, error_rate=error_rate)  # type: ignore[arg-type]

    def test_sizing_positive(self) -> None:
        flt = BloomMembershipFilter(capacity=1000, error_rate=0.01)
        assert flt.num_bits > 0
        assert flt.num_hashes >= 1

    def test_smaller_error_rate_uses_more_bits(self) -> None:
        loose = BloomMembershipFilter(capacity=1000, error_rate=0.1)
        tight = BloomMembershipFilter(capacity=1000, error_rate=0.001)
        assert tight.num_bits > loose.num_bits
        assert tight.num_hashes >= loose.num_hashes

    @pytest.mark.parametrize(
        ("capacity", "error_rate"),
        [
            (1, 0.9),
            (1, 0.01),
            (2, 0.9),
            (3, 0.5),
            (5, 0.1),
            (100, 0.9),
            (100, 0.01),
        ],
    )
    def test_integer_hash_sizing_honors_requested_bound(
        self, capacity: int, error_rate: float
    ) -> None:
        """m is recomputed after k is integral, including tiny/high-FPR cases."""
        flt = BloomMembershipFilter(capacity=capacity, error_rate=error_rate)

        assert _false_positive_upper_bound(flt) <= error_rate

        if flt.num_bits - 1 == flt.num_hashes:
            smaller_bound = 1.0
        else:
            smaller_unset_probability = math.exp(
                capacity * math.log1p(-flt.num_hashes / (flt.num_bits - 1))
            )
            smaller_bound = (1.0 - smaller_unset_probability) ** flt.num_hashes
        assert smaller_bound > error_rate

    def test_default_capacity_minimum_float_is_rejected_before_allocation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fail_allocation(_size: int) -> bytearray:
            raise AssertionError("bytearray allocation was attempted")

        monkeypatch.setattr(
            bloom_filter_module, "bytearray", fail_allocation, raising=False
        )

        with pytest.raises(ValueError, match="memory budget"):
            BloomMembershipFilter(
                capacity=1_000_000,
                error_rate=math.nextafter(0.0, 1.0),
            )

    def test_allocation_budget_uses_rounded_byte_length(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(bloom_filter_module, "_MAX_FILTER_BYTES", 1)

        at_boundary = BloomMembershipFilter(capacity=1, error_rate=0.0625)
        assert at_boundary.num_bits == 8
        assert len(at_boundary._bits) == 1

        with pytest.raises(ValueError, match=r"2 bytes.*1-byte memory budget"):
            BloomMembershipFilter(capacity=1, error_rate=0.0442)

    def test_high_error_capacity_can_exceed_budget_bits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A high target FPR can need fewer vector bits than item slots."""
        monkeypatch.setattr(bloom_filter_module, "_MAX_FILTER_BYTES", 1)

        flt = BloomMembershipFilter(capacity=8, error_rate=0.9)

        assert flt.num_bits == 4
        assert len(flt._bits) == 1

    def test_real_budget_accepts_large_high_error_capacity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sizing above max_bits remains valid when its vector fits 128 MiB."""
        requested_sizes: list[int] = []

        def record_allocation(size: int) -> bytearray:
            requested_sizes.append(size)
            return builtins.bytearray()

        monkeypatch.setattr(
            bloom_filter_module, "bytearray", record_allocation, raising=False
        )
        max_bits = bloom_filter_module._MAX_FILTER_BYTES * 8

        flt = BloomMembershipFilter(capacity=2 * max_bits, error_rate=0.9)

        expected_bytes = (flt.num_bits + 7) >> 3
        assert requested_sizes == [expected_bytes]
        assert bloom_filter_module._MAX_FILTER_BYTES // 2 < expected_bytes
        assert expected_bytes <= bloom_filter_module._MAX_FILTER_BYTES

    def test_hostile_huge_capacity_is_rejected_before_allocation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fail_allocation(_size: int) -> bytearray:
            raise AssertionError("bytearray allocation was attempted")

        monkeypatch.setattr(
            bloom_filter_module, "bytearray", fail_allocation, raising=False
        )

        with pytest.raises(ValueError, match="memory budget"):
            BloomMembershipFilter(capacity=10**100_000, error_rate=0.9)

    @pytest.mark.parametrize(
        ("capacity", "error_rate"), [(1, 0.9), (2, 0.5), (3, 0.1), (20, 0.01)]
    )
    def test_indices_are_exactly_k_distinct_positions(
        self, capacity: int, error_rate: float
    ) -> None:
        flt = BloomMembershipFilter(capacity=capacity, error_rate=error_rate)

        for item_number in range(100):
            indices = list(flt._indices(f"item-{item_number}".encode()))
            assert len(indices) == flt.num_hashes
            assert len(set(indices)) == flt.num_hashes
            assert all(0 <= index < flt.num_bits for index in indices)


class TestBloomMembershipFilterOps:
    """Core add/contains/clear semantics + the no-false-negative guarantee."""

    def test_add_new_returns_true(self) -> None:
        flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
        assert flt.add(b"a") is True

    def test_add_duplicate_returns_false(self) -> None:
        flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
        flt.add(b"a")
        assert flt.add(b"a") is False  # no false negatives on re-add

    def test_contains_after_add(self) -> None:
        flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
        flt.add(b"a")
        assert b"a" in flt

    def test_no_false_negatives(self) -> None:
        """Cardinal guarantee: every inserted item reports as present."""
        flt = BloomMembershipFilter(capacity=500, error_rate=0.01)
        items = [f"item-{i}".encode() for i in range(500)]
        for it in items:
            flt.add(it)
        for it in items:
            assert it in flt, f"false negative for {it!r}"

    def test_clear_resets(self) -> None:
        flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
        flt.add(b"a")
        flt.clear()
        assert b"a" not in flt
        assert len(flt) == 0
        assert flt.add(b"a") is True  # reusable after clear

    def test_len_tracks_distinct_adds(self) -> None:
        flt = BloomMembershipFilter(capacity=1000, error_rate=0.01)
        flt.add(b"a")
        flt.add(b"b")
        flt.add(b"a")  # duplicate — does not increment
        assert len(flt) == 2

    def test_remove_not_supported(self) -> None:
        flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
        flt.add(b"a")
        with pytest.raises(NotImplementedError):
            flt.remove(b"a")

    def test_false_positive_rate_bounded(self) -> None:
        """Observed FPR stays below a deterministic one-sided Hoeffding bound."""
        capacity = 2000
        target = 0.05
        trials = 20_000
        significance = 1e-9
        flt = BloomMembershipFilter(capacity=capacity, error_rate=target)
        for i in range(capacity):
            flt.add(f"seen-{i}".encode())

        false_positives = sum(f"unseen-{i}".encode() in flt for i in range(trials))
        allowed = math.ceil(
            trials * target + math.sqrt(trials * math.log(1.0 / significance) / 2.0)
        )
        assert false_positives <= allowed, (
            f"observed {false_positives}/{trials} false positives; "
            f"one-sided bound allows {allowed}/{trials}"
        )

    def test_open_close_noops(self) -> None:
        flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
        flt.open()
        flt.close()
