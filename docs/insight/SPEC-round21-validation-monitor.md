# SPEC — Round 21: input-validation (inf/NaN/huge-finite) cluster + delay monitor wiring + CLAUDE.md sync

> Back-nav: [Round 20 SPEC](SPEC-round20-healthprobe-close.md) · [Iterative hardening](ITERATIVE-HARDENING-2026-07-21.md)
> Scan: ultracode `wf_86a1a975-f0f` (6-dim find+verify; **fresh-eyes type-safety** dimension productive, 12 agents, ~2.27M tokens)
> Tree: `main 0ed53f6` (R20 head)

## Context & frontier state

Round 21 ran the **close()-BaseException-swallow audit** (centerpiece) + a **fresh-eyes type-safety**
dimension (inf/NaN/huge-finite/settings-validation — never systematically audited before) + r20-diff-regression.

**Result: 6 raw → 5 confirmed, 0 refuted** (1 thrash on fresh-eyes queue.py verify). The centerpiece
audit returned **EMPTY** (the cleanup-BaseException-swallow cluster is CLOSED — connectors/circuit_breaker/
queue-strategies/backends all clean or use primary_error). r20-diff-regression returned **EMPTY** (4th
consecutive round my own diffs survived adversarial review). race-correctness + api-contract EMPTY.
The **fresh-eyes dimension was the productive one** — uncovered a real input-validation cluster.

## Problem statement

### A — CircuitBreaker accepts `reset_timeout=inf`/huge-finite → OPEN wedged forever — MED
`CircuitBreaker.__init__` (circuit_breaker.py:117-122) validates only `failure_threshold < 1` and
`reset_timeout < 0` — no `isfinite`, no upper bound. The settings Field `circuit_breaker_reset_timeout`
(base.py:220) is `Field(ge=0)` which **accepts `inf`** (`inf >= 0`). `_get_breaker` threads it straight
in. The OPEN→HALF_OPEN test `(now - opened_at) >= reset_timeout` is **always False for inf** → a tripped
breaker never recovers → permanent fail-fast across the whole backend. **Throttle has the exact guard**
(`THROTTLE_MAX_MIN_INTERVAL_S=3600.0` + `math.isfinite` + cap, throttle.py:44/95/103-105); CircuitBreaker
is inconsistent. Requires `SCRAPY_CIRCUIT_BREAKER_ENABLED=true` + misconfig → MED.

### B — `queue/delay_depth` operability gauge is dead (DelayQueueStrategy monitor never wired) — MED
The Stable Monitor ABC exposes `on_delay_depth`; `ScrapyStatsMonitor` emits `queue/delay_depth`
(stats.py:307-316, docstring promises "alert before the delay heap grows unbounded"); `DelayQueueStrategy`
calls `self._monitor.on_delay_depth(...)` (delay.py:194,249). But `BackendQueue.__init__` calls
`self._strategy.bind()` (queue.py:155) + stores its own `self._monitor` (163) — **never forwards it to
the strategy**. `DelayQueueStrategy` keeps its `NullMonitor()` default (delay.py:149) → the gauge never
fires. The pipeline DOES forward (pipeline.py:135-137 `getattr(...set_monitor...)`) and
`BatchedStorageStrategy.set_monitor` exists (batched.py:112) — the queue strategy was missed (sibling of
the R14-D ConnectionManager follow-up). Current delay tests pass `monitor=` directly to the ctor,
**bypassing the production wiring gap** (false-green risk).

### C — `compute_full_jitter_backoff` overflows to inf for huge finite `retry_delay` — LOW
`_retry.py:46` `delay = base_delay * (2**attempt)` has no cap. A huge-but-finite `retry_delay` (e.g.
`1e303`, passes `Field(ge=0)` + `_retry_policy`'s `isfinite`) × `2**18` overflows IEEE-754 → `inf` →
`random.uniform(0, inf)` → `inf` → `time.sleep(inf)` raises `OverflowError` inside the retry `except`
arm (connectors.py:1090), escaping as an opaque error instead of `BackendConnectionError`. Mirror
throttle's ceiling discipline.

### D — `BatchedStorageStrategy` accepts `max_buffer_age_s=NaN` — LOW
`batched.py:86` `if max_buffer_age_s is not None and max_buffer_age_s <= 0` — **NaN bypasses it**
(`nan <= 0` is False). A NaN age makes the flusher's wake `wait(timeout=nan)` return immediately
(hot-spin) AND the age comparison `>= nan` always False (never flushes) → unbounded crash-before-flush
loss window + CPU burn. The `threshold < 1` guard (83) has the same NaN-bypass. Settings-layer
`Field(gt=0)` rejects NaN, so exploit is direct-construction/test only. Fix with `math.isfinite`.

### E — CLAUDE.md Optional Dependencies drift — LOW
CLAUDE.md:336-347 lists only 6 of 10 backend extras (omits pulsar/sqs/memcached/dynamodb) and names
the Kafka dep `kafka-python` (pyproject pins `kafka-python-ng`). README:38-48 + pyproject.toml:58-72
are correct. Sync.

## Non-goals (DO-NOT-RE-FLAG — accumulated)
- All prior closed clusters (bloom/cuckoo, connect() from-None redaction, _RedactedStr, dynamodb
  clear_storage TOCTOU, _push_is_durable pin, connect()-BaseException [9], exception-catch-breadth,
  cleanup-BaseException-swallow [dupefilter/pipeline/scheduler], ES add() R-dupe-1, on_pop_rate docs).
- R17-R20 just-shipped arms.

## Units (5)
| ID | Sev | Surface | Fix |
|----|-----|---------|-----|
| A | MED | circuit_breaker.py:117, base.py:220 | `isfinite` + `CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S` cap + settings `le=` (mirror throttle) |
| B | MED | queue.py:163, delay.py | `DelayQueueStrategy.set_monitor` + forward in `BackendQueue.__init__` (mirror pipeline/batched) |
| C | LOW | _retry.py:46 | cap computed delay `min(base*2**attempt, _MAX_BACKOFF_S)` |
| D | LOW | batched.py:83,86 | `math.isfinite` on `max_buffer_age_s` + `threshold` (NaN-bypass) |
| E | LOW | CLAUDE.md:336-347 | sync 10 extras + `kafka-python-ng` |

## Success criteria
- ruff clean; mypy --strict 0 issues; pytest ≥ 3770 passed / 46 skipped (unsandboxed); coverage ≥ 95%.
- Each unit: ONE atomic commit; TDD for A–D; E is docs.
- All merged to `main`; only `main` remains. Claude-only.
