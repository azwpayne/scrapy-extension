# SPEC-round70 — MemoryMembershipFilter.__contains__ must refresh LRU on a hit

> Back-nav: [../insight index](LEDGER.md). Related: R79 scan (scheduler-internals +
> dupefilter dims). Fire: R79.

## Context and audit evidence

`MemoryMembershipFilter` (`src/scrapy_extension/dupefilter/filters/memory_filter.py`) is an
in-process LRU membership filter. `add()` (L162-192) refreshes LRU on a duplicate
(`move_to_end`, L175) so frequently-seen fingerprints are not evicted. But `__contains__()`
(L194-203) is strictly read-only (`return item in self._data`; docstring "read-only — does
not affect LRU order") — it does NOT refresh LRU on a hit.

The dupefilter's **transactional** read path
`BackendDupeFilter._request_seen_for_scheduler_unlocked` (`dupefilter.py:1142`,
`seen = encoded_fingerprint in self._filter`) checks membership via `__contains__`. This is
the **default** path for the bundled `BackendDupeFilter`+`BackendScheduler` combo: the
scheduler's `_atomic_dupefilter_methods` resolves the four-method atomic protocol on a
vanilla `BackendDupeFilter`, so `request_seen_with_reservation` →
`_request_seen_for_scheduler_unlocked` runs (not the legacy `request_seen` → `add()` path).
For a duplicate (`seen=True`) the scheduler drops the request and never calls
`commit_reservation`, so `add()` never runs and the fingerprint's LRU position is frozen at
its original INSERT.

Concrete failure (R79 verifier-confirmed, walked end-to-end):
`SCRAPY_DEDUP_STRATEGY=memory`, `SCRAPY_DEDUP_MEMORY_MAXSIZE=3`; insert A,B,C (cap reached, A
is LRU); re-request A as a duplicate → `__contains__` returns True but does NOT refresh A;
insert D → evicts A (still oldest); re-request A → `__contains__` returns False → A is
re-admitted → **A is re-crawled (dedup false-negative)**. The legacy non-transactional path
(`request_seen` → `add()`) would have refreshed A on the duplicate check and spared it. So
identical filter+key behaves differently depending on which scheduler path runs, and the
**default bundled path is the worse one**.

`__contains__` has exactly one production call site (`dupefilter.py:1142`); no test asserts
read-only intent. `add()` behavior is unchanged.

## Goal

A membership read via `in` refreshes LRU on a hit, so the transactional dedup read path
matches the non-transactional `add()` path's hot-item retention — eliminating the dedup
false-negative (re-crawl) for hot duplicates at the cap.

## Specification

In `MemoryMembershipFilter.__contains__` (`memory_filter.py:194-203`), call
`self._data.move_to_end(item)` on a hit before returning `True`, mirroring `add()`'s re-add
refresh. Update the docstring (it currently claims "read-only — does not affect LRU order",
which becomes false).

```python
def __contains__(self, item: bytes) -> bool:
    if item in self._data:
        self._data.move_to_end(item)
        return True
    return False
```

Thread-safety unchanged: the production call site runs under the dupefilter's lifecycle lock;
standalone use is the caller's responsibility, identical to `add()`. No signature change; no
change to `add()`/`remove()`/`clear()`/`__len__`.

## Plan and independently verifiable tasks

- **R70-1 (RED)**: Add `test_contains_refreshes_lru_on_hit` — at `maxsize=2`, add a,b;
  `assert b"a" in flt` (read hit); add c; assert a survives and b is evicted. Run — **FAILS**
  (currently c evicts a, the unrefreshed LRU).
- **R70-2 (GREEN)**: `__contains__` does `move_to_end` on hit + docstring update. Re-run —
  **PASSES**. Confirm `test_readd_updates_lru_order` (existing `add()`-path test) still
  passes.
- **R70-3 (gate)**: `ruff check .`, `ruff format --check src tests conftest.py`, `pytest`,
  `mypy --strict src/scrapy_extension` green.
- **R70-4 (ship)**: atomic commit + ff-merge to `main`; CI green.

## Acceptance criteria

- A read hit (`item in flt`) at the cap refreshes the item's LRU position so it survives a
  subsequent eviction (matches `add()`'s re-add behavior).
- The default transactional dedup path and the legacy `add()` path now have identical
  hot-item retention — no dedup false-negative divergence.
- `add()`/`remove()`/`clear()`/`__len__` unchanged; existing `test_readd_updates_lru_order`
  still green.
- `ruff check`, `ruff format --check`, `pytest`, `mypy --strict` green; CI on `main` green.
- No dirty file touched (`memory_filter.py` + test are clean — not in the dirty list).
