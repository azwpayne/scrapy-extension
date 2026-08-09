# SPEC-round62 — get_dupefilter wires NullMonitor even when crawler.stats is available

## Context and audit evidence

Found via the R68 deep-insight scan (dim `spider-mixin`), confirmed REAL by an
opus adversarial verifier (low/medium), and **independently re-verified by hand**
against the current tree.

`BackendSpiderMixin.get_dupefilter` (`src/scrapy_extension/spider/spider_mixin.py:620-633`)
constructs `BackendDupeFilter` directly with **no `monitor=` kwarg**:

```python
            if self._dupefilter is None:
                from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter

                key = f"{self.name}:dupefilter"
                self._dupefilter = BackendDupeFilter(
                    connection_manager=manager,
                    key=key,
                    membership_filter=self._build_membership_filter_from_settings(manager, key),
                    owns_connection_manager=False,
                )
```

`BackendDupeFilter.__init__` defaults `monitor` to `NullMonitor()`
(`dupefilter.py:223`) and has **no spider-based auto-resolution** (unlike
`BackendQueue`, which takes `spider=` and calls `_resolve_monitor(spider)` at
`queue.py:195`). So a spider using the mixin's `get_dupefilter()` convenience
getter gets a `NullMonitor` — dedup hit/miss counts and Bloom/Cuckoo
`on_filter_saturation` telemetry are **silently dead**, even when `crawler.stats`
is fully available. The queue getter honors the monitor subsystem
(`get_queue` → `BackendQueue(spider=self)` → `_resolve_monitor` →
`ScrapyStatsMonitor`); the dupefilter getter does not — an inconsistency, not a
documented limitation. (`BackendDupeFilter.from_crawler`/`from_settings` DO wire
`ScrapyStatsMonitor`, but the mixin bypasses those factories.)

**Severity: low/medium.** An observability hole (dead dedup stats for mixin
users), not a correctness bug — but concrete user-facing value when fixed
(stats that should appear on the Scrapy stats dump finally do).

## Goal

Make `get_dupefilter` honor the monitor subsystem with parity to `get_queue`:
when `crawler.stats` is reachable, wire a `ScrapyStatsMonitor`; otherwise fall
back to `NullMonitor` (safe default).

## Specification

Resolve the monitor in `get_dupefilter` and pass it explicitly, reusing the
canonical resolver `BackendQueue._resolve_monitor` (a `@staticmethod` at
`queue.py:1183` that returns `ScrapyStatsMonitor(stats)` when
`spider.crawler.stats` is reachable, else `NullMonitor()`). The mixin already
imports `BackendQueue` (in `get_queue`), and the mixin instance IS the spider
(`BackendSpiderMixin` subclasses `Spider`), so `BackendQueue._resolve_monitor(self)`
resolves from `self.crawler.stats`:

```python
            if self._dupefilter is None:
                from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
                from scrapy_extension.queue.queue import BackendQueue

                key = f"{self.name}:dupefilter"
                self._dupefilter = BackendDupeFilter(
                    connection_manager=manager,
                    key=key,
                    membership_filter=self._build_membership_filter_from_settings(
                        manager, key
                    ),
                    monitor=BackendQueue._resolve_monitor(self),
                    owns_connection_manager=False,
                )
```

Reusing `_resolve_monitor` (rather than duplicating its 3-line `getattr` chain)
keeps a single source of truth for monitor resolution and avoids drift.
`BackendDupeFilter.__init__` already accepts `monitor=` (`dupefilter.py:181`);
no public-API change. When `crawler.stats` is absent (unit tests, ad-hoc use),
`_resolve_monitor` returns `NullMonitor()` — the current behavior — so those
callers are unaffected.

## Plan and independently verifiable tasks

- **R62-1 — RED test.** Add a test to `TestGetDupefilter`
  (`tests/test_spider_mixin.py`): construct a mixin spider with
  `crawler.stats` set (a `MagicMock`), call `get_dupefilter()`, assert
  `isinstance(dupefilter._monitor, ScrapyStatsMonitor)`. → verify: FAILS on
  current code (`_monitor` is `NullMonitor`).
- **R62-2 — GREEN fix.** Add `monitor=BackendQueue._resolve_monitor(self)` to
  the `BackendDupeFilter(...)` construction in `get_dupefilter`. → verify: the
  R62-1 test PASSES.
- **R62-3 — no-regression.** Add a negative test: with no `crawler.stats`,
  `get_dupefilter()._monitor` is still `NullMonitor` (the safe default
  preserved). Full spider_mixin test file green; `ruff check` +
  `ruff format --check` + `mypy --strict` green.

## Acceptance criteria

1. `get_dupefilter()._monitor` is a `ScrapyStatsMonitor` when `crawler.stats` is
   available.
2. `get_dupefilter()._monitor` is a `NullMonitor` when `crawler.stats` is absent
   (current safe behavior preserved).
3. Gate green: `uv run ruff check .` + `uv run ruff format --check src tests
   conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension`.
4. One atomic commit, ff-merged to `main`; CI green.
