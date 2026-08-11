# SPEC — R73: get_dupefilter() ignores REQUEST_FINGERPRINTER_CLASS

> Back-navigation: [../insight/](.) · Round log in [MEMORY index](../../../../.claude/projects/-Users-payne-WorkSpace-project-individual-dev-web-crawler-scrapy-extension/memory/MEMORY.md).
> R82 fire (2026-08-11), 4-dim scan (ndiff-regression + queue-strategies + storage-strategies + monitor wiring). 4 findings confirmed; this is the top (HIGH, ndiff sibling of R70/R77). The other 3 (priority.py re-scan-after-block, batched.py close-retry, spider_mixin manager-monitor) are queued for future fires.

## Context and audit evidence

`BackendSpiderMixin.get_dupefilter()` (spider_mixin.py:625-633) constructs the shared `BackendDupeFilter` for mixin users but omits the `fingerprinter=` kwarg:

```python
self._dupefilter = BackendDupeFilter(
    connection_manager=manager,
    key=key,
    membership_filter=self._build_membership_filter_from_settings(manager, key),
    monitor=BackendQueue._resolve_monitor(self),
    owns_connection_manager=False,
)
```

`BackendDupeFilter.__init__` (dupefilter.py:179,222) defaults `fingerprinter: _Fingerprinter | None = None` → `self._fingerprinter = fingerprinter`. `request_fingerprint()` (dupefilter.py:1525-1527) then branches:

```python
if self._fingerprinter is not None:
    return self._fingerprinter.fingerprint(request).hex()
return request_fingerprint(request)   # legacy scrapy.utils.request.fingerprint
```

The factory path `BackendDupeFilter.from_crawler` (dupefilter.py:657) **does** thread it: `dupefilter._fingerprinter = getattr(crawler, "request_fingerprinter", None)`. So the dupefilter honors a custom `REQUEST_FINGERPRINTER_CLASS` when built via `from_crawler` (the scheduler/pipeline path) but **silently ignores it** when built via `get_dupefilter` (the mixin-producer path).

The method's own docstring (dupefilter.py:1512-1517) names custom `REQUEST_FINGERPRINTER_CLASS` as *exactly the case that should diverge* — `get_dupefilter` defeats that intent.

**ndiff evidence (why this is a fresh sibling, not a re-report):**
- R70 (`9208988`) wired `monitor=BackendQueue._resolve_monitor(self)` into this exact construction block but left `fingerprinter=` unfixed.
- R77 (`3540bf8`) fixed the identical shape in the sibling `get_scheduler` (a mixin getter that constructed a component without threading a settings-driven param that `from_crawler` threads).
- Grep confirms `fingerprinter`/`request_fingerprinter` appears **nowhere else** in spider_mixin.py — `get_dupefilter` is the sole gap.

## Goal

Make `get_dupefilter()` honor the operator's configured `REQUEST_FINGERPRINTER_CLASS`, restoring parity with the `from_crawler` factory path so the mixin-producer dedup path fingerprints with the same identity function Scrapy's engine/scheduler use.

## Specification

In `get_dupefilter`'s `BackendDupeFilter(...)` construction, add one kwarg that mirrors `from_crawler`'s seam (dupefilter.py:657) using the safe crawler-access idiom from `_resolve_monitor` (queue.py:1203: `getattr(spider, "crawler", None)`):

```python
fingerprinter=getattr(
    getattr(self, "crawler", None),
    "request_fingerprinter",
    None,
),
```

Semantics:
- **No crawler** (unit-test spiders, ad-hoc use) → inner `getattr` is `getattr(None, ..., None)` → `None` → byte-identical to today's default `_fingerprinter = None`.
- **Crawler without `request_fingerprinter`** → `None` → same.
- **Crawler with `request_fingerprinter`** → threaded through → honors `REQUEST_FINGERPRINTER_CLASS`.

No other construction kwarg changes; `monitor=` (R70) and `owns_connection_manager=False` stay as-is.

## Plan and independently verifiable tasks

- **R73-1 (RED):** Add `test_get_dupefilter_wires_crawler_request_fingerprinter` to `tests/test_spider_mixin.py` mirroring R70's `test_get_dupefilter_wires_scrapystats_monitor_when_stats_available` (L1456): build a `TestSpider`, attach a mock `crawler` with `crawler.request_fingerprinter = sentinel`, call `get_dupefilter()`, assert `dupefilter._fingerprinter is sentinel`. Fails before fix (`_fingerprinter is None`).
- **R73-2 (GREEN):** Add the `fingerprinter=getattr(getattr(self, "crawler", None), "request_fingerprinter", None)` kwarg in spider_mixin.py:625.
- **R73-3 (no-regression):** Add `test_get_dupefilter_fingerprinter_none_without_crawler` asserting `dupefilter._fingerprinter is None` when no crawler is attached (mirrors R70's NullMonitor fallback test at L1478). Existing R70 monitor tests + `test_get_dupefilter_honors_dedup_strategy_setting` (L2142) stay green.
- **R73-4 (GATE):** `uv run ruff check .` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension` all green (sandbox-off, default uv cache).
- **R73-5 (ship):** atomic commit + `git push origin HEAD:main` (ff) + verify CI green.

## Acceptance criteria

- `get_dupefilter()` on a crawler-backed spider sets `dupefilter._fingerprinter` to `crawler.request_fingerprinter` (test R73-1 passes).
- `get_dupefilter()` on a crawler-less spider leaves `dupefilter._fingerprinter is None` — byte-identical to pre-fix (test R73-3 passes).
- All existing spider_mixin / dupefilter tests stay green; full suite has N+2 tests vs R81's 5107.
- ruff check + mypy --strict clean; CI on `main` green.

## Confidence / risk

- **Confidence:** high — every link source-verified; direct sibling of shipped R70 (same construction block) and R77 (same getter-shape).
- **Scope-risk:** narrow — one kwarg, one clean file (spider_mixin.py is NOT in the dirty tree).
- **Constraint:** touch only `spider_mixin.py` + the 2 new tests.
- **Directive:** mixin getters that construct a component must thread every settings/crawler-derived param the `from_crawler` factory threads — `fingerprinter` here joins `monitor` (R70) and `queue_strategy` (R77). When adding a new param to `BackendDupeFilter.__init__` + `from_crawler`, also thread it in `get_dupefilter`.
