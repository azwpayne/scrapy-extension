"""Tests for MemoryMembershipFilter — in-process exact dedup (subsystem ①)."""

from __future__ import annotations

import pytest

from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter


class TestMemoryMembershipFilter:
  """Exact in-process filter; LRU eviction when maxsize is set."""

  def test_add_new_returns_true(self) -> None:
    flt = MemoryMembershipFilter()
    assert flt.add(b"a") is True

  def test_add_duplicate_returns_false(self) -> None:
    flt = MemoryMembershipFilter()
    assert flt.add(b"a") is True
    assert flt.add(b"a") is False

  def test_contains(self) -> None:
    flt = MemoryMembershipFilter()
    flt.add(b"a")
    assert b"a" in flt
    assert b"b" not in flt

  def test_len(self) -> None:
    flt = MemoryMembershipFilter()
    flt.add(b"a")
    flt.add(b"b")
    assert len(flt) == 2

  def test_clear(self) -> None:
    flt = MemoryMembershipFilter()
    flt.add(b"a")
    flt.clear()
    assert len(flt) == 0
    assert b"a" not in flt

  def test_remove_present(self) -> None:
    flt = MemoryMembershipFilter()
    flt.add(b"a")
    assert flt.remove(b"a") is True
    assert b"a" not in flt

  def test_remove_absent(self) -> None:
    flt = MemoryMembershipFilter()
    assert flt.remove(b"a") is False

  def test_invalid_maxsize_raises(self) -> None:
    with pytest.raises(ValueError, match="maxsize"):
      MemoryMembershipFilter(maxsize=0)
    with pytest.raises(ValueError, match="maxsize"):
      MemoryMembershipFilter(maxsize=-1)

  def test_unbounded_no_eviction(self) -> None:
    """maxsize=None grows without bound."""
    flt = MemoryMembershipFilter()
    for i in range(1000):
      assert flt.add(str(i).encode()) is True
    assert len(flt) == 1000

  def test_maxsize_evicts_oldest(self) -> None:
    """Inserting past maxsize evicts the least-recently-used item."""
    flt = MemoryMembershipFilter(maxsize=3)
    flt.add(b"a")
    flt.add(b"b")
    flt.add(b"c")
    assert flt.add(b"d") is True  # evicts "a"
    assert b"a" not in flt
    assert b"d" in flt
    assert len(flt) == 3

  def test_maxsize_evicted_item_readded_as_new(self) -> None:
    """An evicted item is forgotten — re-adding reports it as new."""
    flt = MemoryMembershipFilter(maxsize=2)
    flt.add(b"a")
    flt.add(b"b")
    flt.add(b"c")  # evicts "a"
    assert flt.add(b"a") is True

  def test_readd_updates_lru_order(self) -> None:
    """Re-adding an existing item marks it recently-used, sparing it from eviction."""
    flt = MemoryMembershipFilter(maxsize=2)
    flt.add(b"a")
    flt.add(b"b")
    assert flt.add(b"a") is False  # seen, but moves "a" to most-recent
    flt.add(b"c")  # should evict "b" (oldest), not "a"
    assert b"a" in flt
    assert b"b" not in flt

  def test_open_close_noops(self) -> None:
    flt = MemoryMembershipFilter()
    flt.open()
    flt.close()
