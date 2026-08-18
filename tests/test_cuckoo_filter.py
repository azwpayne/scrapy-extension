"""Tests for CuckooMembershipFilter — stdlib probabilistic dedup (subsystem ①)."""

from __future__ import annotations

import math
import random

import pytest
from hypothesis import HealthCheck, Verbosity, given, settings
from hypothesis import strategies as st

from scrapy_extension.dupefilter.filters.base import FilterFull
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter

_HYPOTHESIS_DEFAULT_BEFORE_CUCKOO_LOCAL_SETTINGS = settings.default


class TestCuckooMembershipFilterSizing:
    def test_invalid_capacity(self) -> None:
        with pytest.raises(ValueError, match="capacity"):
            CuckooMembershipFilter(capacity=0, error_rate=0.01)
        with pytest.raises(ValueError, match="capacity"):
            CuckooMembershipFilter(capacity=-5, error_rate=0.01)

    @pytest.mark.parametrize(
        "error_rate",
        [0.0, 1.0, float("nan"), float("inf"), float("-inf")],
    )
    def test_invalid_error_rate(self, error_rate: float) -> None:
        with pytest.raises(ValueError, match="error_rate"):
            CuckooMembershipFilter(capacity=100, error_rate=error_rate)

    @pytest.mark.parametrize("expected_bytes", range(1, 33))
    def test_exact_byte_boundaries_never_under_allocate(
        self, expected_bytes: int
    ) -> None:
        threshold = math.ldexp(
            2.0 * CuckooMembershipFilter._BUCKET_SIZE,  # noqa: SLF001
            -(8 * expected_bytes),
        )

        for error_rate in (math.nextafter(threshold, math.inf), threshold):
            flt = CuckooMembershipFilter(capacity=100, error_rate=error_rate)
            assert flt.fp_len == expected_bytes
            assert error_rate >= math.ldexp(
                2.0 * flt._BUCKET_SIZE,  # noqa: SLF001
                -(8 * flt.fp_len),
            )

        below = math.nextafter(threshold, 0.0)
        if expected_bytes < 32:
            flt = CuckooMembershipFilter(capacity=100, error_rate=below)
            assert flt.fp_len == expected_bytes + 1
        else:
            with pytest.raises(ValueError, match="exceeds the 32-byte SHA-256 digest"):
                CuckooMembershipFilter(capacity=100, error_rate=below)

    @pytest.mark.parametrize(
        ("error_rate", "expected_bytes"),
        [
            (0.5, 1),  # ceil(4 bits) then ceil to one byte
            (math.ldexp(1.0, -5), 1),  # exactly eight required bits
            (math.ldexp(1.0, -6), 2),  # nine required bits rounds to two bytes
            (0.01, 2),  # ceil(9.64 bits) then ceil to two bytes
        ],
    )
    def test_fingerprint_bits_round_up_without_reciprocal_math(
        self, error_rate: float, expected_bytes: int
    ) -> None:
        flt = CuckooMembershipFilter(capacity=100, error_rate=error_rate)
        assert flt.fp_len == expected_bytes

    def test_sizing_positive(self) -> None:
        flt = CuckooMembershipFilter(capacity=1000, error_rate=0.01)
        assert flt.num_buckets >= 2
        assert flt.fp_len >= 1

    def test_buckets_are_power_of_two(self) -> None:
        """Two-index xor scheme needs power-of-two bucket count for masking."""
        for cap in (10, 100, 1000, 5000):
            flt = CuckooMembershipFilter(capacity=cap, error_rate=0.01)
            assert (flt.num_buckets & (flt.num_buckets - 1)) == 0

    def test_capacity_properties_distinguish_target_from_physical_slots(self) -> None:
        flt = CuckooMembershipFilter(capacity=1_000, error_rate=1e-12)

        assert flt.configured_capacity == 1_000
        assert flt.slot_capacity == 2_048
        assert flt.capacity == flt.slot_capacity  # backward-compatible meaning

        for i in range(1_000):
            assert flt.add(f"target-{i}".encode()) is True
        assert len(flt) == 1_000
        assert flt.saturation == 1.0


class TestCuckooMembershipFilterOps:
    def test_add_new_returns_true(self) -> None:
        flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
        assert flt.add(b"a") is True

    def test_add_duplicate_returns_false(self) -> None:
        flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
        flt.add(b"a")
        assert flt.add(b"a") is False

    def test_contains_after_add(self) -> None:
        flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
        flt.add(b"a")
        assert b"a" in flt

    def test_no_false_negatives(self) -> None:
        """Cardinal guarantee: every inserted item reports as present."""
        flt = CuckooMembershipFilter(capacity=500, error_rate=0.01)
        items = [f"item-{i}".encode() for i in range(500)]
        for it in items:
            flt.add(it)
        for it in items:
            assert it in flt, f"false negative for {it!r}"

    def test_remove_is_unsupported_and_preserves_present_item(self) -> None:
        flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
        flt.add(b"a")

        with pytest.raises(NotImplementedError, match="does not support item removal"):
            flt.remove(b"a")

        assert b"a" in flt
        assert len(flt) == 1

    def test_remove_false_positive_cannot_delete_colliding_resident(self) -> None:
        """Attested two-byte collision cannot turn arbitrary removal into an FN."""
        flt = CuckooMembershipFilter(capacity=8, error_rate=0.01)
        resident = b"collision-23"
        false_positive = b"collision-313"
        resident_fp, resident_index = flt._fingerprint(resident)
        false_positive_fp, false_positive_index = flt._fingerprint(false_positive)
        assert resident_fp == false_positive_fp == bytes.fromhex("d95d")
        assert resident_index == false_positive_index == 2
        assert flt.add(resident) is True
        assert false_positive in flt

        with pytest.raises(NotImplementedError, match="does not support item removal"):
            flt.remove(false_positive)

        assert resident in flt
        assert len(flt) == 1
        assert flt.add(resident) is False

    def test_clear(self) -> None:
        flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
        flt.add(b"a")
        flt.clear()
        assert b"a" not in flt
        assert len(flt) == 0
        assert flt.add(b"a") is True

    def test_len_tracks_distinct_adds(self) -> None:
        flt = CuckooMembershipFilter(capacity=1000, error_rate=0.01)
        flt.add(b"a")
        flt.add(b"b")
        flt.add(b"a")  # duplicate
        assert len(flt) == 2

    def test_full_filter_raises(self) -> None:
        """Massively overfilling a tiny filter surfaces a 'full' error."""
        flt = CuckooMembershipFilter(capacity=8, error_rate=0.01)
        with pytest.raises(RuntimeError, match="[Ff]ull"):
            for i in range(1000):
                flt.add(f"x-{i}".encode())

    def test_filter_full_preserves_existing_membership(self) -> None:
        flt = CuckooMembershipFilter(capacity=8, error_rate=0.01)
        flt._rng = random.Random(0)  # noqa: SLF001 - deterministic kick path
        existing = [f"x-{i}".encode() for i in range(flt.capacity)]
        for item in existing:
            assert flt.add(item) is True
        overflow = f"x-{flt.capacity}".encode()
        assert overflow not in flt
        count_before = len(flt)

        with pytest.raises(FilterFull):
            flt.add(overflow)

        assert len(flt) == count_before
        missing = [item for item in existing if item not in flt]
        assert not missing, f"failed insertion lost existing items: {missing!r}"
        assert overflow not in flt

    @pytest.mark.parametrize(
        ("exception_type", "kick_number"),
        [(KeyboardInterrupt, 1), (MemoryError, 250)],
        ids=["keyboard-interrupt-after-early-swap", "memory-error-after-mid-swap"],
    )
    def test_interrupted_kicks_restore_exact_state(
        self,
        monkeypatch: pytest.MonkeyPatch,
        exception_type: type[BaseException],
        kick_number: int,
    ) -> None:
        flt = CuckooMembershipFilter(capacity=8, error_rate=0.01)
        flt._rng = random.Random(0)  # noqa: SLF001 - deterministic kick path
        existing = [f"x-{i}".encode() for i in range(flt.capacity)]
        for item in existing:
            assert flt.add(item) is True
        overflow = f"x-{flt.capacity}".encode()
        buckets_before = [bucket.copy() for bucket in flt._buckets]
        count_before = len(flt)
        interruption = exception_type(f"injected after kick {kick_number}")
        original_alt_index = flt._alt_index
        alt_index_calls = 0

        def interrupt_after_swap(index: int, fp: bytes) -> int:
            nonlocal alt_index_calls
            alt_index_calls += 1
            if alt_index_calls == kick_number + 1:
                raise interruption
            return original_alt_index(index, fp)

        monkeypatch.setattr(flt, "_alt_index", interrupt_after_swap)

        with pytest.raises(exception_type) as raised:
            flt.add(overflow)

        assert raised.value is interruption
        assert alt_index_calls == kick_number + 1
        assert flt._buckets == buckets_before
        assert len(flt) == count_before
        missing = [item for item in existing if item not in flt]
        assert not missing, f"interrupted insertion lost existing items: {missing!r}"
        assert overflow not in flt

    def test_false_positive_rate_bounded(self) -> None:
        """FP rate stays within a generous multiple of target (seeded)."""
        capacity = 2000
        target = 0.05
        flt = CuckooMembershipFilter(capacity=capacity, error_rate=target)
        for i in range(capacity):
            flt.add(f"seen-{i}".encode())
        rng = random.Random(777)
        fp = sum(
            1 for _ in range(2000) if f"u-{rng.randrange(1 << 60)}".encode() in flt
        )
        rate = fp / 2000
        assert rate < target * 5, f"FP rate {rate:.3f} exceeded {target * 5}"

    def test_open_close_noops(self) -> None:
        flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
        flt.open()
        flt.close()


class TestCuckooMembershipFilterProperties:
    """Hypothesis property tests for the cardinal claims (subsystem ①).

    These complement ``test_false_positive_rate_bounded`` (a seeded loop) with
    two claim-verifying properties:

    - ``test_no_false_negatives_property``: every inserted item reports present.
    - ``test_false_positive_rate_property``: FP rate over unseen keys stays
      bounded by a generous multiple of ``error_rate`` — probabilistic filters
      must never false-negative and their FP must stay within design bounds.

    The generated property has local ``@settings`` so importing or executing
    this module never replaces Hypothesis' process-wide default profile.
    """

    @settings(
        derandomize=True,
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
        verbosity=Verbosity.normal,
    )
    @given(
        items=st.lists(
            st.binary(min_size=1, max_size=32),
            min_size=0,
            max_size=200,
            unique=True,
        )
    )
    def test_no_false_negatives_property(self, items: list[bytes]) -> None:
        """Cardinal guarantee: for any inserted set, ``item in filter`` is always True.

        The cuckoo filter only ever moves a fingerprint between its two valid
        buckets during eviction — it never drops one — so containment of an
        inserted item is invariant. This property pins that contract against
        arbitrary item sets up to ~85% load (200 items, default sizing).
        """
        flt = CuckooMembershipFilter(capacity=250, error_rate=0.01)
        for item in items:
            flt.add(item)
        for item in items:
            assert item in flt, f"cuckoo false negative for {item!r}"

    def test_local_hypothesis_settings_do_not_mutate_global_default(self) -> None:
        local = getattr(
            type(self).test_no_false_negatives_property,
            "_hypothesis_internal_use_settings",
        )
        assert local.derandomize is True
        assert local.max_examples == 50
        assert settings.default is _HYPOTHESIS_DEFAULT_BEFORE_CUCKOO_LOCAL_SETTINGS

    def test_false_positive_rate_property(self) -> None:
        """FP rate over unseen keys stays within 5x target (derandomized, deterministic).

        Inserts ~capacity distinct items, then probes ``capacity`` unseen keys.
        Asserts ``rate < target * 5``. Mirrors ``test_bloom_filter.py``'s
        ``test_false_positive_rate_bounded``. Deterministic because the filter's
        internal eviction RNG is seeded with a fixed value and the probe keys are
        generated from a fixed seed.
        """
        capacity = 2000
        target = 0.05
        # Fixed internal RNG so eviction-slot selection is reproducible.
        flt = CuckooMembershipFilter(capacity=capacity, error_rate=target)
        flt._rng = random.Random(424242)  # noqa: SLF001 — pin eviction jitter
        for i in range(capacity):
            flt.add(f"seen-{i}".encode())
        # Confirm no false negatives first (the never-FN guarantee).
        for i in range(capacity):
            assert f"seen-{i}".encode() in flt
        rng = random.Random(987654321)  # fixed probe seed → reproducible FP sample
        probes = 4000
        fp = sum(
            1
            for _ in range(probes)
            if f"unseen-{rng.randrange(1 << 60)}".encode() in flt
        )
        rate = fp / probes
        # 5x target margin: proves low FP without flakiness; cuckoo's theoretical
        # FP at target load is ~2b·error_rate, well under this bound.
        assert rate < target * 5, f"cuckoo FP rate {rate:.3f} exceeded {target * 5}"
