"""Tests for CuckooMembershipFilter — stdlib probabilistic dedup with deletion (subsystem ①)."""

from __future__ import annotations

import random

import pytest

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
