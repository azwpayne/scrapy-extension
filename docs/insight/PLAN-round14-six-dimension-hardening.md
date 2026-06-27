# Round 14 Six-Dimension Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: this plan is executed by the repo's `/goal` fan-out model (one executor agent per unit, each does its own TDD; orchestrator integrates + commits). proven on rounds 9-13. If using superpowers instead, use `superpowers:subagent-driven-development` (one subagent per unit). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the 8 hardening units (R14-A…H) surfaced by the round-14
six-dimension full-coverage audit, re-establishing v1.0-tag defensibility
(R14-B + R14-C are pre-tag blockers).

**Architecture:** Units are file-disjoint where possible and grouped into 3
dependency-ordered waves. Wave 1 (A/F/H) and Wave 2 (B/E) parallelize cleanly;
Wave 3 (C→D→G) is sequenced on the shared `settings/base.py` + `monitor/` +
`tests/` seams. Every unit is TDD with verifiable acceptance + the 3 hard gates.

**Tech Stack:** Python 3.10+, pydantic-settings, pytest + pytest-mock + Hypothesis,
mypy --strict, ruff, uv.

## Global Constraints (every task inherits these)

- **Hard gates (must stay green after every unit):** `uv run pytest -q` (currently
  **1555 passed, 36 skipped**), `uv run ruff check src/ tests/` (clean),
  `uv run mypy --strict src/scrapy_extension/` (0 errors, 67 files).
- **Coverage floor:** backend-layer files ≥95% (total is 95.19% but backends are
  87-95% — R14-G closes this); never let total drop below 95%.
- **No silent swallows / no raw client-lib leaks:** storage ops raise
  `StorageError(BackendError)` (R14-A); queue ops already raise `QueueError`.
- **No breaking change without a CHANGELOG "Breaking" entry** (R14-B).
- **Validators raise `ConfigurationError(setting_name=…)`**, never pydantic
  `ValidationError`, for user-facing config errors (R14-B).
- **New Scrapy settings** follow the `SCRAPY_*` env-var convention and are
  documented in `docs/runbook.md` + `STABILITY.md` (R14-C).
- **No new unbounded growth** (R14-E): every registry/cache/diagnostic-set ships
  with a cap or pruning rule.

---

## File-structure map (who touches what — conflict-avoidance)

| Unit | Owns (edit) | Shared seam (sequence on) |
|---|---|---|
| R14-A | `exceptions/base.py`, `backends/{memcached,dynamodb,mongodb}.py` | — |
| R14-F | `queue/strategies/{delay,round_robin,throttle}.py`, `queue/queue.py` | — |
| R14-H | `__init__.py`, `backends/__init__.py`, `backends/rocketmq.py` | — |
| R14-B | `CHANGELOG.md`, `STABILITY.md`, `README.md`, `settings/base.py`, `backends/base.py`, `exceptions/base.py` | `exceptions/base.py` (with A), `settings/base.py` (with C) |
| R14-E | `backends/connectors.py`, `backends/{kafka,rabbitmq,pulsar,sqs}.py`, `backends/circuit_breaker.py` | — |
| R14-C | `settings/base.py`, `schedule/scheduler.py`, `queue/strategies/factory.py`, `monitor/stats.py`, `queue/queue.py` | `settings/base.py` (after B), `monitor/` + `queue/queue.py` (with D) |
| R14-D | `monitor/{base,stats}.py`, `queue/queue.py`, `dupefilter/**`, `backends/connectors.py` | `monitor/` + `queue/queue.py` (after C), `connectors.py` (after E) |
| R14-G | `tests/**`, `conftest.py` | tests the new behavior → last |

---

## Dependency graph + execution waves

```
Wave 1 (parallel, fully disjoint):  R14-A   R14-F   R14-H
Wave 2 (parallel, disjoint):        R14-B   R14-E
Wave 3 (sequenced on shared seams):  R14-C  →  R14-D  →  R14-G
```

- **Wave 1** = 3 executors, no shared files (storage-backends / queue-strategies / imports).
- **Wave 2** = 2 executors; R14-B touches `exceptions/base.py`+`settings/base.py`+docs, R14-E touches `backends/connectors.py`+backend files — disjoint ✓.
- **Wave 3** = solo chain: C threads settings through `settings/base.py`+`monitor/`+`queue.py`; D adds hooks to `monitor/`+`queue.py`+`connectors.py`; G adds tests for everything. Sequence avoids merge conflicts on the shared seams.

A single `/goal` can take one wave at a time (fan-out within the wave), then the
next `/goal` takes the next wave.

---

## Task R14-A — StorageBackend error-contract uniformity

**Files:** Modify `src/scrapy_extension/exceptions/base.py` (add `StorageError`),
`backends/memcached.py`, `backends/dynamodb.py`, `backends/mongodb.py`; tests in
`tests/test_{memcached,dynamodb,mongodb}_backend.py`.

**Interfaces:**
- Produces: `class StorageError(BackendError)` with `operation: str` + `key: str | None` kwargs.
- Consumes: existing `BackendError.__init__` signature.

- [ ] **Step 1 — RED:** add tests asserting each storage op's failure raises `StorageError`:
  - `test_memcached_store_failure_raises_storage_error` (mock client `.set` → raise) — currently returns `None`.
  - `test_dynamodb_delete_throttling_raises_storage_error` (mock `client.delete_item` → `ClientError(error={"Code":"ThrottlingException"})`) — currently returns `False`.
  - `test_mongodb_retrieve_connection_error_raises_storage_error` (mock `find_one` → `AutoReconnect`) — currently leaks raw.
  - Run `uv run pytest tests/test_{memcached,dynamodb,mongodb}_backend.py -q` → FAIL (wrong sentinel / raw leak).
- [ ] **Step 2 — implement `StorageError`** in `exceptions/base.py` (subclass `BackendError`, add `operation`/`key`).
- [ ] **Step 3 — GREEN memcached:** wrap `store/retrieve/delete/exists` to catch the client-lib error → `raise StorageError(...) from e` (stop the silent `return None/False`).
- [ ] **Step 4 — GREEN dynamodb:** selective `except ClientError as e:` — raise `StorageError` on `ThrottlingException`/`ProvisionedThroughputExceededException`/`LimitExceededException`; only swallow `ResourceNotFoundException` (genuine "missing").
- [ ] **Step 5 — GREEN mongodb:** wrap `store/retrieve/delete/exists/ttl/clear_storage` to catch `pymongo.errors.PyMongoError` → `raise StorageError(...) from e` (mirror the existing queue-op wrap pattern at `mongodb.py:368/430/457`).
- [ ] **Step 6 — verify gates:** `pytest -q` green, `ruff` clean, `mypy --strict` 0 errors.
- [ ] **Step 7 — commit:** `fix(backends): R14-A uniform StorageError across storage ops (3 data-loss/leak bugs)`.

**Acceptance:** `except BackendError` catches every storage-path failure across memcached/dynamodb/mongodb; `on_error` hook wiring (R14-D) now has real failures to emit.

---

## Task R14-F — Queue-strategy correctness (delay priority, RR cleanup, retry+delay storm)

**Files:** Modify `queue/strategies/{delay,round_robin,throttle}.py`, `queue/queue.py`; tests in `tests/test_{delay_strategy,round_robin_strategy,throttle_strategy,queue}.py`.

**Interfaces:**
- Produces: delay heap tuple gains a `priority` slot; `round_robin._sources` evicts empty keys; `BackendQueue.push` pops `delay`/`source` from `request.meta` after reading.
- Consumes: existing `QueueStrategy.push/pop` ABC.

- [ ] **Step 1 — RED:** `test_delay_drains_at_original_priority` (push priority=10 + delay → drain → assert re-pushed at priority 10, not 0); `test_round_robin_evicts_empty_source` (drain a source → assert key removed from `_sources`); `test_retry_does_not_re_delay` (pop a delayed request, re-push it → assert NOT re-delayed). All FAIL today.
- [ ] **Step 2 — GREEN delay:** change heap tuple to `(ready_at, seq, item, priority)`; store priority in `push`; pass `priority=` in `_drain_ready`.
- [ ] **Step 3 — GREEN round_robin:** after `dq.popleft()`, if `not dq: del self._sources[source]`; reset `_idx` to the popped slot's neighbor.
- [ ] **Step 4 — GREEN retry-storm:** in `BackendQueue.push`, after reading `delay`/`source` from `request.meta`, `request.meta.pop("delay"/"source", None)` so a re-push doesn't re-apply (document the behavior change in the docstring).
- [ ] **Step 5 — GREEN throttle:** document per-instance rate semantics in the docstring; bound `min_interval` (e.g. `le=3600`) with a `ConfigurationError` on misconfig.
- [ ] **Step 6 — verify gates + commit:** `fix(queue): R14-F retain delay priority, evict RR empty sources, kill retry+delay storm`.

**Acceptance:** priority survives delay; RR bounded under source churn; retries don't storm; throttle misconfig rejected.

---

## Task R14-H — Lazy-import hygiene + polish

**Files:** Modify `__init__.py`, `backends/__init__.py`, `backends/rocketmq.py`; tests in `tests/test_lazy_imports.py` (or `test_init.py`).

- [ ] **Step 1 — RED:** `test_real_import_error_surfaces_chain` — inject a backend module that raises `ImportError("real bug")` for a non-dep reason; assert the raised error carries the real chain, NOT the "Install with pip install scrapy-extension[redis]" hint. FAIL today.
- [ ] **Step 2 — GREEN:** in `__getattr__`, narrow the except — only re-wrap as the install hint when `isinstance(e, ModuleNotFoundError) and e.name in <optional-dep-module-set>`; else `raise` the original.
- [ ] **Step 3 — polish:** drop the redundant `except OSError` arms in `rocketmq.py:115,228,266` (or give them a distinct message); fix the bloom/memory `_count` docstring nits.
- [ ] **Step 4 — verify gates + commit:** `fix: R14-H narrow lazy-import error wrapping + rocketmq polish`.

**Acceptance:** a genuine bug inside a backend module surfaces its real traceback.

---

## Task R14-B — v1.0 breaking-change disclosure + public-contract freeze

**Files:** Modify `CHANGELOG.md`, `STABILITY.md`, `README.md`, `settings/base.py`, `backends/base.py`; tests in `tests/test_config.py`, `tests/test_settings_validation.py`.

**Interfaces:**
- Produces: `Settings.backend_type: BackendType | str` (accepts registered 3rd-party strings); `BackendType._missing_` raises `ConfigurationError`.
- Consumes: `backends/registry.get_registry()` (to validate the string is known).

- [ ] **Step 1 — RED:** `test_backend_type_accepts_registered_third_party_string` (register a fake backend, `Settings(backend_type="fakebackend")` succeeds) — FAIL today (`ValidationError`). `test_unknown_backend_type_raises_configuration_error` (not `ValidationError`).
- [ ] **Step 2 — GREEN:** widen `Settings.backend_type` field type + add a `@field_validator` that accepts any `BackendType` OR any string in `get_registry()`; unknown → `raise ConfigurationError(setting_name="SCRAPY_BACKEND_TYPE", ...)`. Update `BackendType._missing_` to raise `ConfigurationError` (or route validation solely through the settings validator).
- [ ] **Step 3 — docs Breaking section:** add a prominent **Breaking** block to `CHANGELOG.md` `[Unreleased]` naming: (a) Pulsar `auth_token` now requires `pulsar+ssl://` service_url; (b) Redis `ssl_enabled=True` now requires `ssl_cafile`; (c) `SCRAPY_BACKEND_TYPE` validation now raises `ConfigurationError` (was pydantic `ValidationError`). Add a "Round-9 hardening (breaking)" row to `STABILITY.md`.
- [ ] **Step 4 — freeze contract:** add a Stable row to `STABILITY.md` for `ConfigurationError` attributes (`setting_name`, `setting_value`, `_SENSITIVE_NAME_FRAGMENTS` redaction). Cross-link from `README.md` Guarantees.
- [ ] **Step 5 — verify gates + commit:** `docs(settings): R14-B disclose v1.0 breaking changes + freeze ConfigurationError contract`.

**Acceptance:** 3rd-party backend selectable via `SCRAPY_BACKEND_TYPE`; breaking changes loudly documented; `ConfigurationError` attrs frozen.

---

## Task R14-E — Lifecycle bounds (long-run leak prevention)

**Files:** Modify `backends/connectors.py`, `backends/{kafka,rabbitmq,pulsar,sqs}.py`, `backends/circuit_breaker.py`; tests in `tests/test_connection_manager.py`, `tests/test_{kafka,rabbitmq,pulsar,sqs}_backend.py`.

- [ ] **Step 1 — RED:** `test_managers_registry_capped_under_settings_churn` (create N=64 distinct settings → assert registry size ≤ MAX_MANAGERS=32 and victim was `disconnect()`-ed). `test_rabbitmq_qos_failure_nulls_connection` (`basic_qos` raises → `is_connected()` is False). `test_kafka_prunes_empty_partition_keys`. All FAIL today.
- [ ] **Step 2 — GREEN registry cap:** convert `_managers` to an `OrderedDict` with `MAX_MANAGERS=32`; on overflow, `popitem(last=False)` + call `victim.close()`/`disconnect()`.
- [ ] **Step 3 — GREEN rabbitmq:** move `_setup_qos()` immediately after `self._channel = …`; on `AMQPError` set `self._channel = None; self._connection = None` before raising. Same for the HA-policy block in `_connect_mirrored_queues`.
- [ ] **Step 4 — GREEN kafka:** on `_ack_token`, when `_in_flight[partition]` empties, `del self._in_flight[partition]`; prune stale `_high_water`/`_watermarks`.
- [ ] **Step 5 — GREEN diagnostic sets + breaker:** bound `_in_flight` sets in pulsar/sqs/rabbitmq (LRU cap `_MAX_IN_FLIGHT=10000`, warn-once on overflow); call `self._breaker.reset()` in `ConnectionManager.close()`.
- [ ] **Step 6 — verify gates + commit:** `fix(backends): R14-E bound registry/partition/diagnostic growth + RabbitMQ partial-state`.

**Acceptance:** no unbounded growth under settings/partition churn; `is_connected()` truthful post partial-failure.

---

## Task R14-C — Operability configurability (deferred settings-wiring)

**Files:** Modify `settings/base.py`, `schedule/scheduler.py`, `queue/strategies/factory.py`, `monitor/stats.py`, `queue/queue.py`, `docs/runbook.md`; tests in `tests/test_config.py`, `tests/test_scheduler.py`.

**Sequencing:** runs **after R14-B** (shared `settings/base.py`).

**Interfaces:**
- Produces: 5 new `SCRAPY_*` settings; `BackendQueue.__init__` + `build_queue_strategy` + `ScrapyStatsMonitor.__init__` accept the threaded values.
- Consumes: the landed U4 (`depth_sample_every`), U5 (`max_held`), U2 (`pop_rate_window_s`, `backpressure_threshold`) defaults.

- [ ] **Step 1 — RED:** `test_depth_sample_every_threaded_from_settings` (set `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY=5` → constructed `BackendQueue.depth_sample_every == 5`). Same for the other 4 settings. FAIL today (stuck at defaults).
- [ ] **Step 2 — GREEN settings:** add the 5 fields to `settings/base.py` (`SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY`, `SCRAPY_QUEUE_MAX_ITEM_BYTES`, `SCRAPY_QUEUE_DELAY_MAX_HELD`, `SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD`, `SCRAPY_MONITOR_POP_RATE_WINDOW_S`). Wire the orphaned `queue_max_item_bytes`/`pipeline_max_item_bytes`.
- [ ] **Step 3 — GREEN scheduler:** in `BackendScheduler.from_settings`, read the 5 settings and pass to `BackendQueue(...)`, `build_queue_strategy(..., max_held=…)`, `ScrapyStatsMonitor(...)` at `scheduler.py:343`.
- [ ] **Step 4 — GREEN factory + monitor + queue:** `build_queue_strategy` accepts `max_held`; `ScrapyStatsMonitor` accepts `backpressure_threshold` + `pop_rate_window_s`; `BackendQueue` accepts `pop_rate_window_s`.
- [ ] **Step 5 — runbook:** document each setting in `docs/runbook.md` (replace the "tune via settings" hand-wave with the actual env-var).
- [ ] **Step 6 — verify gates + commit:** `feat(settings): R14-C thread U4/U5/U2 knobs via Scrapy settings`.

**Acceptance:** every runbook "tune via settings" instruction has a real `SCRAPY_*` behind it.

---

## Task R14-D — Observability completeness

**Files:** Modify `monitor/{base,stats}.py`, `queue/queue.py`, `dupefilter/dupefilter.py`, `dupefilter/filters/{bloom,memory}_filter.py`, `backends/connectors.py`; tests in `tests/test_monitor.py`, `tests/test_dupefilter.py`.

**Sequencing:** runs **after R14-C** (shared `monitor/` + `queue/queue.py`) and **after R14-E** (shared `connectors.py`).

- [ ] **Step 1 — RED:** `test_on_error_emitted_on_push_failure`; `test_bloom_saturation_property`; `test_memory_filter_eviction_emits_saturation`; `test_on_connect_emitted`; `test_pop_count_stat_name_matches_behavior`. FAIL today.
- [ ] **Step 2 — GREEN on_error:** wire `self._monitor.on_error("push"/"pop", e)` at `queue.py:189` (push except) + `queue.py:268` (deserialize fail); remove the bypassing direct `stats.inc_value` in `scheduler.py:504,576` in favor of the hook. (If the team prefers deletion: remove `on_error` from the protocol + `ScrapyStatsMonitor` — but wiring is preferred.)
- [ ] **Step 3 — GREEN filter saturation:** add `capacity` + `saturation` properties to `BloomMembershipFilter`; in `MemoryMembershipFilter._warn_evicted_once`, also emit `monitor.on_filter_saturation(len, maxsize)` (thread the monitor ref or emit via the dupefilter).
- [ ] **Step 4 — GREEN on_pop semantics:** fix the `base.py:87` + `stats.py:75` docstrings to say "per pop attempt"; rename the stat to `queue/pop_attempt_count` (or split into `on_pop_attempt`/`on_pop_success` — pick the lighter rename).
- [ ] **Step 5 — GREEN connection hooks:** add `on_connect`/`on_disconnect`/`on_retry` to `Monitor` + `ScrapyStatsMonitor`; wire from `ConnectionManager` (stats: `backend/{connect,retry,disconnect}_count`).
- [ ] **Step 6 — verify gates + commit:** `feat(monitor): R14-D wire on_error, Bloom/Memory saturation, connection hooks, fix on_pop semantics`.

**Acceptance:** every documented monitor hook is emitted on the path it claims; Bloom/Memory saturation observable in stats.

---

## Task R14-G — Test-coverage hardening (backend layer → 95%+)

**Files:** Modify `tests/**` (new + extended), `tests/conftest.py`, root `conftest.py`; possibly `pyproject.toml` `[tool.pytest]`.

**Sequencing:** runs **last** (after A/E/F) so it tests the new behavior too.

- [ ] **Step 1 — MongoDB not-connected guards:** extend `tests/test_mongodb_backend.py` (or new `test_mongodb_modes_coverage.py`) with `test_*_raises_when_disconnected` for push/pop/add/contains/store/retrieve across the 3 collections (set `_collection = None`).
- [ ] **Step 2 — registry plugin-error paths:** new `tests/test_registry.py::TestPluginDiscoveryErrors` — entry-point callable raises / wrong return type / unknown capabilities → assert warn + skip (not crash). Use `mocker.patch("importlib.metadata.entry_points")`.
- [ ] **Step 3 — connectors owner-fail:** threading test in `tests/test_connection_manager.py` — owner's `connect()` raises → peer waiters receive the same exception + `_connected_event` set (no permanent hang).
- [ ] **Step 4 — fix the flake:** scope the registry-mock clearing in `tests/conftest.py` autouse fixture so `test_create_backend_redis` stops being order-dependent.
- [ ] **Step 5 — property tests:** new `tests/test_property_settings.py` (Hypothesis) — SV3 cross-field consistency, U4 sampling boundaries, U5 cap eviction; assert validators either accept or raise `ConfigurationError` with stable `setting_name`, never crash.
- [ ] **Step 6 — integration-tier gate:** add `@pytest.mark.integration` to `tests/integration/*`; add a `pytest_collection_modifyitems` skip-unless-`--integration-only` (or env-var) hook in root `conftest.py` so the tier is runnable but doesn't rot silently.
- [ ] **Step 7 — verify gates + coverage:** `pytest --cov=src/scrapy_extension --cov-report=term-missing -q` → every backend file ≥95%; total ≥95%. Commit `test: R14-G backend coverage to 95%+, property tests, integration-tier gate, flake fix`.

**Acceptance:** backend files ≥95%; property tests green; integration tier runnable via marker; `test_create_backend_redis` flake gone.

---

## Self-review (run before handing off)

**1. Spec coverage:** each SPEC unit (R14-A…H) maps to exactly one Task above. ✓
The 2 CRITICAL findings (breaking-change disclosure → R14-B; deferred settings-wiring → R14-C) and the dead-`on_error` CRITICAL (→ R14-D) all have tasks. ✓

**2. Placeholder scan:** every step names exact files (with line refs from the
audit) + a concrete RED test + a GREEN action + the commit message. No "TBD"/"add
appropriate handling". The one judgment call left to the executor (R14-D step 2:
wire vs delete `on_error`) is explicit — wiring is the recommended default.

**3. Type/interface consistency:** `StorageError(BackendError)` introduced in
R14-A is the type R14-D's `on_error` emits for storage failures; `Settings.backend_type`
widened in R14-B is the field R14-C reads; the `saturation` property added to
Bloom in R14-D mirrors the existing cuckoo one (same name/signature). The shared
seams (`settings/base.py`, `monitor/`, `queue/queue.py`, `connectors.py`,
`exceptions/base.py`) are explicitly sequenced in the wave plan to avoid
merge conflicts. ✓

**Risk register (carry into commits):**
- R14-A changes storage-op failure semantics (was silent/leak → now raise) —
  any downstream code catching the old sentinel needs updating; flag as Breaking
  in CHANGELOG (coordinate with R14-B).
- R14-D renaming `pop_count` → `pop_attempt_count` is a stats-key change — note
  in CHANGELOG.
- R14-E registry-cap eviction calls `disconnect()` on a victim a live (but
  settings-orphaned) manager — confirm no component still holds a refcount (the
  eviction only triggers on overflow, i.e. genuinely-orphaned entries).

---

## Execution handoff

Plan complete and saved to `docs/insight/PLAN-round14-six-dimension-hardening.md`.

This plan is sized for the repo's **`/goal` fan-out model**: one `/goal` per
wave (Wave 1 → 3 parallel executors; Wave 2 → 2; Wave 3 → sequenced C→D→G),
orchestrator integrates + commits each unit (proven on rounds 9-13).

Two options for the next move:
1. **`/goal` per wave (recommended, matches the repo cadence)** — start with
   Wave 1 (`/goal` "execute Wave 1 of PLAN-round14: R14-A + R14-F + R14-H,
   file-disjoint fan-out").
2. **Inline / subagent-driven now** — execute Wave 1 immediately via
   `superpowers:subagent-driven-development`.
