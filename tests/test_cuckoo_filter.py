"""Tests for CuckooMembershipFilter — stdlib probabilistic dedup with deletion (subsystem ①)."""

from __future__ import annotations

import random

import pytest
from hypothesis import HealthCheck, Verbosity, given, settings
from hypothesis import strategies as st

from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter


class TestCuckooMembershipFilterSizing:
  def test_invalid_capacity(self) -> None:
    with pytest.raises(ValueError, match="capacity"):
      CuckooMembershipFilter(capacity=0, error_rate=0.01)
    with pytest.raises(ValueError, match="capacity"):
      CuckooMembershipFilter(capacity=-5, error_rate=0.01)

  def test_invalid_error_rate(self) -> None:
    with pytest.raises(ValueError, match="error_rate"):
      CuckooMembershipFilter(capacity=100, error_rate=0.0)
    with pytest.raises(ValueError, match="error_rate"):
      CuckooMembershipFilter(capacity=100, error_rate=1.0)

  def test_sizing_positive(self) -> None:
    flt = CuckooMembershipFilter(capacity=1000, error_rate=0.01)
    assert flt.num_buckets >= 2
    assert flt.fp_len >= 1

  def test_buckets_are_power_of_two(self) -> None:
    """Two-index xor scheme needs power-of-two bucket count for masking."""
    for cap in (10, 100, 1000, 5000):
      flt = CuckooMembershipFilter(capacity=cap, error_rate=0.01)
      assert (flt.num_buckets & (flt.num_buckets - 1)) == 0


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

  def test_remove_present(self) -> None:
    flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
    flt.add(b"a")
    assert flt.remove(b"a") is True
    assert b"a" not in flt

  def test_remove_absent(self) -> None:
    flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
    assert flt.remove(b"a") is False

  def test_remove_then_readd_reports_new(self) -> None:
    """Deletion is real: a removed item is forgotten and re-added as new."""
    flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
    flt.add(b"a")
    flt.remove(b"a")
    assert flt.add(b"a") is True

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
    flt.remove(b"a")
    assert len(flt) == 1

  def test_full_filter_raises(self) -> None:
    """Massively overfilling a tiny filter surfaces a 'full' error."""
    flt = CuckooMembershipFilter(capacity=8, error_rate=0.01)
    with pytest.raises(RuntimeError, match="[Ff]ull"):
      for i in range(1000):
        flt.add(f"x-{i}".encode())

  def test_false_positive_rate_bounded(self) -> None:
    """FP rate stays within a generous multiple of target (seeded)."""
    capacity = 2000
    target = 0.05
    flt = CuckooMembershipFilter(capacity=capacity, error_rate=target)
    for i in range(capacity):
      flt.add(f"seen-{i}".encode())
    rng = random.Random(777)
    fp = sum(1 for _ in range(2000) if f"u-{rng.randrange(1 << 60)}".encode() in flt)
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

  Uses ``hypothesis`` with a ``derandomize``-style seeded profile so CI is
  reproducible: the same ``--hYPOTHESIS_SEED`` / fixed ``random_factory``
  produces the same generated cases every run.
  """

  @pytest.fixture(autouse=True)
  def _derandomize_profile(self) -> None:
    """Pin hypothesis to a derandomized profile for deterministic FP-rate checks."""
    settings.register_profile(
      "ci_derandomized",
      derandomize=True,
      max_examples=50,
      deadline=None,
      suppress_health_check=[HealthCheck.too_slow],
      verbosity=Verbosity.normal,
    )
    settings.load_profile("ci_derandomized")

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
      1 for _ in range(probes) if f"unseen-{rng.randrange(1 << 60)}".encode() in flt
    )
    rate = fp / probes
    # 5x target margin: proves low FP without flakiness; cuckoo's theoretical
    # FP at target load is ~2b·error_rate, well under this bound.
    assert rate < target * 5, f"cuckoo FP rate {rate:.3f} exceeded {target * 5}"
