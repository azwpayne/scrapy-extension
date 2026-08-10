# SPEC-round69 — get_scheduler() must honor SCRAPY_QUEUE_STRATEGY

> Back-nav: [../insight index](LEDGER.md). Related: SPEC-round68 (R76 safe-list
> completion). Fire: R77 (queue-ship of an R76-confirmed finding; no new scan —
> cap-aware, weekly cap is binding).

## Context and audit evidence

`BackendSpiderMixin.get_scheduler()` (`src/scrapy_extension/spider/spider_mixin.py:662`)
constructs `BackendScheduler` **without** `queue_strategy`:

```python
self._scheduler = BackendScheduler(
    connection_manager=manager,
    queue_key=queue_name,
    owns_connection_manager=False,
)
```

`BackendScheduler.__init__` (`schedule/scheduler.py:881`) defaults
`queue_strategy: QueueStrategy | None = None`; `None` → `BackendQueue` uses
`PassthroughQueueStrategy` (documented at `scheduler.py:901-903`). So a spider that sets
`SCRAPY_QUEUE_STRATEGY='delay'` (or any non-default strategy) and uses the mixin's
`get_scheduler()` **silently gets Passthrough** — the configured queue semantics are
ignored.

The asymmetry: `get_queue()` (spider_mixin.py:574-581) already honors the setting via
`queue_strategy=self._build_queue_strategy_from_settings(manager)`. The normal Scrapy path
(`SCHEDULER = "...BackendScheduler"` → `from_crawler` → `from_settings`,
`scheduler.py:1098,1174`) also builds and threads `queue_strategy`. Only the mixin's
`get_scheduler()` bypasses it.

Confirmed by reading the source directly (no model tokens): `_build_queue_strategy_from_settings`
(spider_mixin.py:499-520) reads `SCRAPY_QUEUE_STRATEGY` from `crawler.settings` and returns a
`build_queue_strategy(QueueStrategyType(str(raw)), manager)`; `BackendScheduler.__init__` accepts
`queue_strategy`; `open()` (`scheduler.py:1490-1494`) threads `self._queue_strategy` into the
internal `BackendQueue`. The finding was adversarially confirmed in R76's scan with a concrete
repro (`crawler.settings['SCRAPY_QUEUE_STRATEGY']='delay'` → `get_scheduler()` →
`scheduler._queue_strategy is None`).

## Goal

`get_scheduler()` honors `SCRAPY_QUEUE_STRATEGY` exactly as `get_queue()` does, so a
spider using the mixin's scheduler getter gets the queue semantics it configured.

## Specification

Add `queue_strategy=self._build_queue_strategy_from_settings(manager)` to the
`BackendScheduler(...)` construction in `get_scheduler()` (`spider_mixin.py:662-666`),
mirroring `get_queue()`. One keyword argument; no signature change to `BackendScheduler`
(it already accepts `queue_strategy`).

Out of scope (deferred, documented): `get_scheduler()` also omits the settings-driven
monitor/depth/bytes/backpressure params (`queue_depth_sample_every`,
`queue_max_item_bytes`, `monitor_*`, `backpressure_*`). Those have sane `__init__` defaults
(not wrong behavior, just not customized); threading them needs `_build_*_from_settings`
helpers that may not exist. The `queue_strategy` omission is the functional bug (silent
semantic divergence Delay→Passthrough) and the sole target of this round.

## Plan and independently verifiable tasks

- **R69-1 (RED)**: Add a test `test_get_scheduler_honors_queue_strategy_setting` that sets
  `SCRAPY_QUEUE_STRATEGY='delay'` on the crawler and asserts `spider.get_scheduler()._queue_strategy`
  is a `DelayQueueStrategy` (not `None`). Run it — **FAILS** (`_queue_strategy is None`).
- **R69-2 (GREEN)**: Add `queue_strategy=self._build_queue_strategy_from_settings(manager)` to
  `get_scheduler()`'s `BackendScheduler(...)` call. Re-run — **PASSES**.
- **R69-3 (gate)**: `ruff check .`, `ruff format --check src tests conftest.py`, `pytest`,
  `mypy --strict src/scrapy_extension` all green.
- **R69-4 (ship)**: atomic commit + ff-merge to `main`; CI green.

## Acceptance criteria

- `spider.get_scheduler()._queue_strategy` reflects `SCRAPY_QUEUE_STRATEGY` (a `DelayQueueStrategy`
  for `'delay'`), not `None`/Passthrough.
- `get_scheduler()` and `get_queue()` now resolve the strategy identically.
- A spider with no crawler / default setting still gets `None` → Passthrough (no behavior change
  for the common case).
- `ruff check`, `ruff format --check`, `pytest`, `mypy --strict` green; CI on `main` green.
- No dirty file touched (`spider_mixin.py` + test are clean — not in the dirty list).
