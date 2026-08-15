# R137 PLAN — breaker-apply key stability + mixin queue-monitor knobs

> Spec: [R137-breaker-apply-key-stability-SPEC.md](R137-breaker-apply-key-stability-SPEC.md)

## Phase 1 — TDD RED

- A1 (`tests/test_connection_manager.py`, F1): unique-settings manager via
  `get_manager` → `apply_scrapy_breaker_policy(policy-carrying ScrapySettings)`
  → `manager.close()` → `get_manager` with the ORIGINAL settings must return a
  FRESH manager (`m2 is not m1`, backend access does not raise). Current tree:
  m2 IS the retired m1 → RED.
- A2 (F2): manager → `_get_breaker()` with env cleared (caches disabled
  fallback) → `apply(policy True/5/30.0)` → `_get_breaker()` must return the
  breaker with threshold 5. Current: None → RED.
- A3 (F3): `apply(policyA True/3/7.5)` then `apply(policyB True/9/1.0)` →
  warning logged once (caplog), `_get_breaker().failure_threshold == 3` (first
  wins). Current: no warning, 9 → RED. Guard: same-policy re-apply warns
  never.
- B1 (`tests/test_spider_mixin.py`, F4): crawler settings with
  `SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD=7` + `SCRAPY_MONITOR_POP_RATE_WINDOW_S=12.5`
  → `get_queue()` → `queue._monitor.backpressure_threshold == 7` and
  `queue._pop_rate_window_s == 12.5`. Current: 1000 / 60 → RED.
- B2 (F5): early `get_queue()` (no crawler) → `_monitor` is NullMonitor;
  attach crawler with stats, call `get_queue()` again → upgraded to
  ScrapyStatsMonitor. Current: stays Null → RED. Guard: a queue already
  carrying a non-Null monitor is never re-wired by `get_queue`.

## Phase 2 — Implement (GREEN)

- A: connectors.py — `__init__` gains `_breaker_resolved_from_env_fallback`
  and `_breaker_policy_values`; `_get_breaker` records which branch resolved
  (+ values); `apply_scrapy_breaker_policy` installs directly under `_lock`
  (no settings mutation), overrides env-fallback resolutions, warns once on a
  dropped differing explicit policy. Breaker construction mirrors
  `_get_breaker` (name, failure_exceptions).
- B: queue.py — `BackendQueue.set_monitor` (replace + R21-B strategy
  forward); spider_mixin.py — `_resolve_queue_monitor()` helper (parse knobs
  with the scheduler's defaults via `utils._config` helpers, delegate to
  `BackendScheduler._resolve_monitor_for_spider`), pass `monitor=` +
  `pop_rate_window_s=` at construction, NullMonitor-only upgrade on cached
  re-entry.

## Phase 3 — Gate (plain commands)

```bash
uv run --frozen ruff check src tests conftest.py
uv run --frozen ruff format --check src tests conftest.py
uv run --frozen pytest
uv run --frozen mypy --strict src
```

## Phase 4 — Ship

Two fix commits + docs commit; LEDGER rows (F1-F5 LANDED, F6 DEFERRED,
race REFUTED); push HEAD:main; memory round entry + MEMORY.md.
