"""Tests for MemoryMembershipFilter — in-process exact dedup (subsystem ①)."""

from __future__ import annotations

import logging

import pytest

from scrapy_extension.dupefilter.filters.memory_filter import (
  DEFAULT_MEMORY_MAXSIZE,
  MemoryMembershipFilter,
)


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
    """maxsize=None grows without bound (explicit advanced opt-out)."""
    flt = MemoryMembershipFilter(maxsize=None)
    for i in range(1000):
      assert flt.add(str(i).encode()) is True
    assert len(flt) == 1000

  def test_default_maxsize_is_bounded(self) -> None:
    """Default constructor ships a 1M LRU cap (OOM prevention, SPEC U5)."""
    flt = MemoryMembershipFilter()
    assert flt._maxsize == DEFAULT_MEMORY_MAXSIZE == 1_000_000

  def test_default_cap_evicts_at_threshold_not_unbounded(self, caplog) -> None:
    """Past the default cap the filter evicts (LRU) + warns once; no infinite growth."""
    import scrapy_extension.dupefilter.filters.memory_filter as mod
    mod._evicted_warned = False  # reset module-level flag for a clean slate
    flt = MemoryMembershipFilter()  # default cap = 1_000_000
    # Saturate the cap; the first item is the LRU eviction candidate.
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.dupefilter.filters.memory_filter"):
      for i in range(DEFAULT_MEMORY_MAXSIZE):
        flt.add(i.to_bytes(4, "big"))
      assert len(flt) == DEFAULT_MEMORY_MAXSIZE
      assert flt.add((DEFAULT_MEMORY_MAXSIZE).to_bytes(4, "big")) is True
    # Length stays at the cap — no unbounded growth.
    assert len(flt) == DEFAULT_MEMORY_MAXSIZE
    # The oldest (first-inserted) fingerprint was evicted → re-admit risk surfaced.
    assert (0).to_bytes(4, "big") not in flt
    # Eviction warning fired (non-silent: surfaces re-crawl tradeoff).
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("evict" in r.getMessage().lower() or "LRU" in r.getMessage() for r in warnings)

  def test_default_cap_warn_fires_once(self, caplog) -> None:
    """The eviction warning is idempotent across many evictions (warn-once)."""
    import scrapy_extension.dupefilter.filters.memory_filter as mod
    mod._evicted_warned = False  # reset module-level flag for a clean slate
    flt = MemoryMembershipFilter(maxsize=2)
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.dupefilter.filters.memory_filter"):
      flt.add(b"a")
      flt.add(b"b")
      flt.add(b"c")  # evict
      flt.add(b"d")  # evict again
      flt.add(b"e")  # evict a third time
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1  # warn-once, not per-eviction

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
