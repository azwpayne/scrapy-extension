"""Tests for BloomMembershipFilter — stdlib probabilistic dedup (subsystem ①)."""

from __future__ import annotations

import random

import pytest

from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter


class TestBloomMembershipFilterSizing:
  """Capacity/error-rate validation and derived m, k."""

  def test_invalid_capacity(self) -> None:
    with pytest.raises(ValueError, match="capacity"):
      BloomMembershipFilter(capacity=0, error_rate=0.01)
    with pytest.raises(ValueError, match="capacity"):
      BloomMembershipFilter(capacity=-1, error_rate=0.01)

  def test_invalid_error_rate(self) -> None:
    with pytest.raises(ValueError, match="error_rate"):
      BloomMembershipFilter(capacity=100, error_rate=0.0)
    with pytest.raises(ValueError, match="error_rate"):
      BloomMembershipFilter(capacity=100, error_rate=1.0)
    with pytest.raises(ValueError, match="error_rate"):
      BloomMembershipFilter(capacity=100, error_rate=1.5)

  def test_sizing_positive(self) -> None:
    flt = BloomMembershipFilter(capacity=1000, error_rate=0.01)
    assert flt.num_bits > 0
    assert flt.num_hashes >= 1

  def test_smaller_error_rate_uses_more_bits(self) -> None:
    loose = BloomMembershipFilter(capacity=1000, error_rate=0.1)
    tight = BloomMembershipFilter(capacity=1000, error_rate=0.001)
    assert tight.num_bits > loose.num_bits
    assert tight.num_hashes >= loose.num_hashes


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
    """FP rate stays within a generous multiple of target (seeded → deterministic)."""
    capacity = 2000
    target = 0.05
    flt = BloomMembershipFilter(capacity=capacity, error_rate=target)
    for i in range(capacity):
      flt.add(f"seen-{i}".encode())
    rng = random.Random(12345)  # fixed seed → reproducible
    fp = sum(1 for _ in range(2000) if f"u-{rng.randrange(1 << 60)}".encode() in flt)
    rate = fp / 2000
    # 5x target margin: proves low FP without flakiness.
    assert rate < target * 5, f"FP rate {rate:.3f} exceeded {target * 5}"

  def test_open_close_noops(self) -> None:
    flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
    flt.open()
    flt.close()
