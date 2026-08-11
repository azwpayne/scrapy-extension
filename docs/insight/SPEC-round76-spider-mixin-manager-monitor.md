# SPEC — R76: spider_mixin never wires a monitor into the shared ConnectionManager

> Back-navigation: [../insight/](.) · Round log in [MEMORY index](../../../../.claude/projects/-Users-payne-WorkSpace-project-individual-dev-web-crawler-scrapy-extension/memory/MEMORY.md).
> R85 fire (2026-08-11). No fresh scan — ships the LAST R82-queued finding (#4, confirmed real/surgical/clean-file). R82's 4-confirmed scan is now fully shipped (fingerprinter→R82, batched→R83, priority→R84, this). Next fire does a fresh ndiff+rotate scan.

## Context and audit evidence

`BackendSpiderMixin.setup_backend` (spider/spider_mixin.py:152-213) creates the spider's shared `ConnectionManager` (L178-182) but **never calls `manager.set_monitor(...)`** — confirmed by grep: spider_mixin.py has zero `set_monitor` references. The manager defaults to `NullMonitor` (connectors.py:1366: `self._monitor: Monitor = NullMonitor()`), so the R14-D connection-lifecycle hooks `on_connect`/`on_disconnect`/`on_disconnect_result`/`on_retry` (wired in connectors.py, dispatched via `_notify_monitor` → `getattr(self._monitor, hook_name)`, e.g. connectors.py:2242) are **no-ops**.

**The asymmetry (the smoking gun):** every OTHER `connection_manager` owner threads a monitor into the manager:
- `pipeline.from_crawler` → `pipeline.connection_manager.set_monitor(pipeline._monitor)` (pipeline.py:408).
- `dupefilter.from_crawler` → `dupefilter.connection_manager.set_monitor(dupefilter._monitor)` (dupefilter.py:669, comment: "so backend/{connect,disconnect,retry}_count cover the set backend").
- `scheduler.open` → `self.connection_manager.set_monitor(monitor)` (scheduler.py:~1492, comment: *"Without this, ConnectionManager defaults to NullMonitor and the hooks R14-D wired are dead observability outside the queue path."*).

`spider_mixin.setup_backend` is the **lone exception**. `get_queue` and `get_dupefilter` build their components on this shared manager but neither threads the monitor INTO the manager: `get_dupefilter` sets the dupefilter's OWN monitor (L631, R70), and `get_queue` relies on `BackendQueue.__init__`'s `_resolve_monitor` fallback which sets the STRATEGY's monitor but never the MANAGER's.

### Concrete failure scenario

A spider extends `BackendSpiderMixin` with `backend_type=REDIS`, calls `get_queue()` to push seed URLs (a documented distributed-crawling producer pattern), and does NOT use `get_scheduler` (no `SCHEDULER=BackendScheduler`). On `spider_opened`, `manager.connect()` runs and succeeds, firing `on_connect` (connectors.py) — but the manager is `NullMonitor`, so `backend/connect_count` is never incremented. The operator's connectivity dashboard reads `backend/connect_count == 0` and incorrectly concludes the spider never connected. Likewise `backend/disconnect_count`, `backend/disconnect_success_count`, `backend/disconnect_failure_count`, and `backend/retry_count` stay 0 for the spider's real connect/disconnect/retry events. (If `get_scheduler()` IS also called, `scheduler.open` overwrites the manager's monitor at scheduler.py:~1492, so the gap only bites get_queue/get_dupefilter-direct spiders.)

Existing test `test_monitor.py` proves the stat fires ONLY when `manager.set_monitor(ScrapyStatsMonitor(stats))` is explicitly called — which spider_mixin never does.

`spider_mixin.py` is clean (NOT in the dirty tree).

## Goal

Thread default-on telemetry into the shared `ConnectionManager` so the R14-D connection-lifecycle hooks fire for mixin-producer spiders (get_queue/get_dupefilter-direct), restoring parity with the pipeline/dupefilter/scheduler `from_crawler` paths.

## Specification

In `setup_backend`, immediately after the manager-acquire `if/else` block (where `manager` is bound in BOTH branches — L153 existing / L178 new) and before the signal-wiring block (L185), add a `set_monitor` call resolved via the established `_resolve_monitor` staticmethod (mirrors R70's `get_dupefilter` pattern at L631):

```python
                self._connection_manager = manager
                acquired_here = True

            # R14-D: thread default-on telemetry into the shared manager so the
            # connection-lifecycle hooks (on_connect/on_disconnect/on_disconnect_
            # result/on_retry -> backend/{connect,disconnect,retry}_count) fire in
            # production. Parity with pipeline/dupefilter/scheduler from_crawler,
            # all of which call connection_manager.set_monitor(...); spider_mixin
            # was the lone exception. Resolved on every setup_backend call so the
            # legacy early-setup path (setup_backend in __init__ before crawler is
            # attached -> resolves NullMonitor) is re-covered when from_crawler's
            # idempotent second call runs with the crawler attached. A later
            # scheduler.open() overwrites this with the same crawler.stats-backed
            # monitor, so there is no conflict.
            from scrapy_extension.queue.queue import BackendQueue

            manager.set_monitor(BackendQueue._resolve_monitor(self))

            signal_wiring_failure: BaseException | None = None
```

Semantics:
- **Modern path** (`from_crawler` attaches crawler THEN calls `setup_backend` once): crawler attached → `_resolve_monitor` returns `ScrapyStatsMonitor(stats)` → manager wired. **(The fix.)**
- **Legacy early-setup path** (subclass calls `setup_backend()` in `__init__` before crawler): 1st call resolves `NullMonitor` (no crawler); `from_crawler`'s idempotent 2nd call (crawler attached) re-resolves → `ScrapyStatsMonitor(stats)` → manager wired. (Covered by placing the call OUTSIDE the acquire `if`.)
- **No crawler / no stats** (unit-test spiders, ad-hoc use): `_resolve_monitor` returns `NullMonitor` → `manager.set_monitor(NullMonitor())` — byte-identical to today's default. No regression.
- **Spider also uses `get_scheduler`**: `scheduler.open` (on `spider_opened`) overwrites the manager's monitor with the operator-tuned one wrapping the same `crawler.stats` — no conflict, equivalent value.

`_resolve_monitor` (queue.py, `@staticmethod`) does `getattr(spider, "crawler", None)` → `getattr(crawler, "stats", None)` → `ScrapyStatsMonitor(stats)` else `NullMonitor()` — safe everywhere (returns `NullMonitor` when crawler/stats absent).

## Plan and independently verifiable tasks

- **R76-1 (RED):** `test_setup_backend_wires_scrapystats_monitor_into_manager` — build a `BackendSpiderMixin` spider with a mock `crawler` exposing `crawler.stats`, call `setup_backend()`; assert `isinstance(spider._connection_manager._monitor, ScrapyStatsMonitor)`. Fails before fix (`_monitor` is `NullMonitor`).
- **R76-2 (GREEN):** add the `manager.set_monitor(BackendQueue._resolve_monitor(self))` call after the acquire if/else in `setup_backend`.
- **R76-3 (no-regression):** `test_setup_backend_monitor_null_without_crawler` — spider without a crawler → `isinstance(manager._monitor, NullMonitor)`. Plus: existing `setup_backend`/`from_crawler`/signal-wiring tests stay green (set_monitor is additive, runs every call, NullMonitor when no crawler).
- **R76-4 (legacy-path coverage):** `test_from_crawler_rewires_monitor_after_early_setup` — subclass calls `setup_backend()` in `__init__` (no crawler → NullMonitor), then `from_crawler(crawler)` attaches crawler + idempotent 2nd call; assert `manager._monitor` is `ScrapyStatsMonitor` after `from_crawler`. (Proves the every-call placement covers the legacy path.)
- **R76-5 (GATE):** `uv run ruff check .` + `uv run ruff format --check src tests conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension` all green (sandbox-off, default uv cache). Format-check IS enforced (R64).
- **R76-6 (ship):** atomic commit + `git push origin HEAD:main` (ff) + verify CI green.

## Acceptance criteria

- `setup_backend()` on a crawler-backed spider sets `manager._monitor` to a `ScrapyStatsMonitor` (R76-1 passes).
- Without a crawler, `manager._monitor` stays `NullMonitor` — byte-identical to pre-fix (R76-3 passes).
- The legacy early-setup path is covered: after `from_crawler`, `manager._monitor` is `ScrapyStatsMonitor` even when `setup_backend` ran first in `__init__` (R76-4 passes).
- All existing spider_mixin tests stay green; full suite = R84's 5113 + new tests.
- ruff check + ruff format --check + mypy --strict clean; CI on `main` green.

## Confidence / risk

- **Confidence:** high — source-verified (zero set_monitor in spider_mixin; NullMonitor default; the 3-other-owners asymmetry with their own comments naming the gap); scheduler.py's comment literally describes this exact defect; R82's opus verifier could not refute.
- **Scope-risk:** narrow — one `set_monitor` call + lazy import in `setup_backend`, placed to run every call (covering both construction paths); no signature change; NullMonitor when no crawler = byte-identical default.
- **Constraint:** touch only `spider_mixin.py` (clean — NOT in the dirty tree) + new tests.
- **Rejected:** place inside the acquire `if manager is None` block (R82 verifier's sketch) | only covers the modern path; the legacy early-setup path's idempotent 2nd call skips that block, leaving those spiders with NullMonitor. Placing outside the `if` covers both paths at the same single site.
- **Rejected:** wire in `from_crawler` instead | also correct, but `setup_backend` is the named setup surface and already runs finalization (signal wiring) on every call; co-locating the monitor wiring there matches the existing structure and the finding's framing.
- **Directive:** every owner of a shared `ConnectionManager` MUST thread a monitor into the manager (not just into components built on it) — otherwise the R14-D connection-lifecycle hooks are dead observability. `pipeline`/`dupefilter`/`scheduler` from_crawler do this; `spider_mixin.setup_backend` now does too. A future component that creates a `ConnectionManager` must call `manager.set_monitor(...)`.
- **Not-tested:** real `crawler.stats` round-trip (mock seam); multi-backend aggregation (R25-F — all component monitors wrap the same crawler.stats so counters aggregate; out of scope).
