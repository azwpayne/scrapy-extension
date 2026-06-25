# PLAN — Round 4: Backpressure Action Hook (Tier-2, B1)

Architect-selected round-4 pick (see decision memo in commit/INSIGHTS). The highest-leverage
tractable Tier-2 item: convert the already-emitted-but-unconsumed `queue/backpressure` signal
into a real distributed-crawler behavior (depth-driven pull-rate throttle). Companion to
[`INSIGHTS-2026-06-25.md`](./INSIGHTS-2026-06-25.md) (B1) and
[`PLAN-round3-sqs-pulsar-real-ack.md`](./PLAN-round3-sqs-pulsar-real-ack.md).

## Why

`queue/backpressure` is emitted on every pop (`queue/queue.py:198-205` → `monitor/stats.py:79-96`)
and `monitor/base.py` flags it as "the #1 operability gap". **Nothing consumes it today.** A
distributed crawler whose workers pull faster than downstream can store/proc floods the backend.
Round-4 lands the consumer.

## Decision (architect, with file:line anchors)

- **Scheduler-side depth gate on `next_request`** — return `None` (the existing, contract-correct
  "slow down" signal Scrapy's engine already handles) when depth ≥ pause threshold; resume when
  depth ≤ resume threshold (hysteresis, prevents flapping). Hook point: `scheduler.py:473-495`.
- **Read depth from `len(self._queue)`** (fresh; same source `has_pending_requests` trusts at
  `scheduler.py:497-509`), NOT the `queue/backpressure` stats gauge (sampling-stale, couples
  control to the stats layer).
- **Rejected**: mutate `DOWNLOAD_DELAY` (wrong lever — per-request politeness, not pull-rate;
  racy mid-run settings mutation); new `Monitor.on_backpressure_action` hook (inverts ownership —
  scheduler owns pull-rate); distributed Bloom as round-4 target (NOT a correctness gap — `set`
  strategy is distributed-exact; round-2 D3 warning already surfaces the per-process caveat).

## New settings (additive, default-off → zero compat break)
- `SCRAPY_BACKPRESSURE_PAUSE_AT: int | None = None` — depth `>=` this → `next_request` returns None.
- `SCRAPY_BACKPRESSURE_RESUME_AT: int | None = None` — depth `<=` this to resume (hysteresis).
  Defaults to `PAUSE_AT` when unset (simple, no hysteresis). Validator: `resume_at <= pause_at`
  else `ConfigurationError`; both `>= 0`.

## Units (parallel fan-out; disjoint files)

### Unit BP-1 — `settings/base.py` + `tests/test_config.py`
- Two pydantic fields + validators (`resume_at <= pause_at`; `>= 0`).
- Validation test in `test_config.py`: `resume_at > pause_at` → `ConfigurationError`.
- **Files**: `src/scrapy_extension/settings/base.py`, `tests/test_config.py` ONLY.

### Unit BP-2 — `schedule/scheduler.py` + `tests/test_scheduler_backpressure.py` (NEW)
- `__init__`: add `backpressure_pause_at: int | None = None`, `backpressure_resume_at: int | None = None`
  (keyword-only, default None) + `self._backpressure_paused = False`.
- `from_settings`: read both via `settings.getint("SCRAPY_BACKPRESSURE_PAUSE_AT")` /
  `..._RESUME_AT")` (default None); thread into `cls(...)`. `resume_at` defaults to `pause_at`.
- `next_request`: insert the depth gate BEFORE the existing pop:
  - not paused + `len(self._queue) >= pause_at` → `_backpressure_paused=True`, bump `scheduler/backpressure_pause`.
  - paused + `len(self._queue) <= resume_at` → `_backpressure_paused=False`, bump `scheduler/backpressure_resume`.
  - paused else → `return None`.
  - `len()` raising `QueueError`/`NotImplementedError` propagates to the existing `next_request`
    `except QueueError` arm (`scheduler.py:491`) → degraded safely, no stuck pause flag.
- `open(spider)`: reset `_backpressure_paused = False` (clean per-spider start).
- Docstring: document the backpressure contract (depth source, hysteresis, stat names, default-off).
- **Files**: `src/scrapy_extension/schedule/scheduler.py`, `tests/test_scheduler_backpressure.py` ONLY.

## Tests (TDD — RED first, then GREEN), `tests/test_scheduler_backpressure.py`, mock-queue only
1. default-off (`pause_at=None`) → `next_request` pops (current behavior pinned).
2. `pause_at=10`, `len=10` → first `next_request` returns None, pop NOT called, `scheduler/backpressure_pause` bumped. (RED pre-fix.)
3. hysteresis: `pause_at=10, resume_at=5`, paused, `len=7` → still None; drain to `len=5` → pops, `scheduler/backpressure_resume` bumped. (RED pre-fix.)
4. flap: `pause_at=10` only, paused at 10, drain to 9 → resumes (pops). `RESUME_AT := PAUSE_AT`.
5. stat names exactly `{scheduler/backpressure_pause, scheduler/backpressure_resume}` (additive).
6. `open()` resets `_backpressure_paused` to False.
7. (BP-1, in `test_config.py`) `resume_at > pause_at` → `ConfigurationError`.
8. `len(self._queue)` raises `QueueError` → propagates to existing arm, returns None, no crash, flag not stuck.

## Acceptance
- `uv run pytest -q -p no:randomly` green (existing 1278 + new); ruff clean; mypy clean (65 files).
- Tests 2 + 3 RED pre-fix, GREEN post-fix.
- Default-off verified by test 1 (byte-identical behavior when settings unset).
- Public API: additive only (2 keyword-only `__init__` kwargs default None; 2 settings default None;
  1 internal attr; 2 additive stat keys). No backend/queue/monitor/dupefilter/pipeline change.
- Independent verifier + code-reviewer approval lane: APPROVE, 0 CRITICAL/HIGH.

## Non-goals (remain Tier-2/3)
- Distributed Bloom/Cuckoo (memory optimization, not a correctness gap — `set` is distributed-exact).
- Entry-point plugin registration (round-5 candidate). Distributed Delay/Throttle. Security-parity
  cluster. Sentinel failover re-discovery. rocketmq-client replacement. B5 reconnect test.
