# PLAN — Round 21: input-validation cluster + delay monitor wiring + CLAUDE.md sync

> Spec: [SPEC-round21-validation-monitor.md](SPEC-round21-validation-monitor.md)
> Workflow: worktree `round21-validation-monitor` → execute A→E (atomic commits) → gate → ff-merge to `main` → delete branch.

## Design notes
- **A**: mirror throttle exactly. Add module const `CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S: float = 3600.0`
  in circuit_breaker.py; in `__init__` add (after the `< 0` check) `if not math.isfinite(reset_timeout)
  or reset_timeout > CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S: raise ValueError(...)`. For the settings Field,
  add `le=CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S` — import the const from circuit_breaker (verify no circular
  import: circuit_breaker.py must not import settings/base at module level; it takes values as params).
  Also tighten `failure_threshold` to reject bool (mirror delay.py's `isinstance(max_held, bool)` guard).
- **B**: add `set_monitor(self, monitor: Monitor) -> None` to `DelayQueueStrategy` (mirror
  `BatchedStorageStrategy.set_monitor`); in `BackendQueue.__init__`, AFTER the `self._monitor`
  assignment (queue.py:163), forward via the exact pipeline.py:135-137 `getattr` pattern (generic —
  future-proofs any monitor-aware strategy). CRITICAL test must exercise the PRODUCTION path
  (BackendQueue→strategy via wiring), not pass `monitor=` directly to the strategy ctor (that bypasses
  the gap — the existing delay tests do this, a false-green).
- **C**: in `compute_full_jitter_backoff`, `delay = min(base_delay * (2 ** attempt), _MAX_BACKOFF_S)`
  with `_MAX_BACKOFF_S = 3600.0` (mirror throttle's ceiling). Bounds the sleep regardless of base/attempt.
- **D**: batched.py:86 → `if max_buffer_age_s is not None and (not math.isfinite(max_buffer_age_s) or
  max_buffer_age_s <= 0)`; same `isfinite` fix on the `threshold < 1` guard (83) for consistency (NaN-bypass).
- **E**: replace CLAUDE.md:336-347 with the README:38-48 / pyproject.toml:58-72 list (10 extras) +
  `kafka-python-ng`.

## Phases
1. **TDD RED (A–D)** — see TASK.
2. **Implement (GREEN)** — A (breaker cap), B (monitor wiring), C (backoff cap), D (NaN guards), E (docs).
3. **Gate** — `uv run ruff check src/ tests/`; `uv run mypy --strict src/`; `uv run pytest -q` (sandbox OFF).
4. **Merge to main** — ff-only, push, `worktree remove --force` + `branch -d` (sandbox-off).

## Fan-out (Claude-only)
5 small units → main-loop sequential. Ultracode 12-agent scan was the insight fan-out.

## Risk notes
- **A**: confirm circuit_breaker.py doesn't import settings/base at module level before importing the
  const into base.py (else circular). If it does, use a literal `le=3600.0` with a cross-ref comment.
- **B**: the test must build the production wiring (BackendQueue with a ScrapyStatsMonitor + a delayed
  push) and assert `crawler.stats.get_value("queue/delay_depth")` is non-None — NOT construct
  DelayQueueStrategy(monitor=...) directly.
