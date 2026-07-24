# TASK — Round 21: input-validation cluster + delay monitor wiring + CLAUDE.md sync

> Spec: [SPEC-round21-validation-monitor.md](SPEC-round21-validation-monitor.md)
> Plan:  [PLAN-round21-validation-monitor.md](PLAN-round21-validation-monitor.md)
> Constraints: one atomic commit per unit · all → main · Claude-only.

## Unit A — CircuitBreaker reset_timeout cap (MED)
**File:** `src/scrapy_extension/backends/circuit_breaker.py:117`; `src/scrapy_extension/settings/base.py:220`. Ref throttle.py:44/95/103-105.
1. [RED] Test: `CircuitBreaker(reset_timeout=float('inf'))` raises ValueError; same for `1e308`.
   Run → RED (currently accepted).
2. [GREEN] Add `CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S: float = 3600.0` (module const). In `__init__`,
   after the `< 0` check: `if not math.isfinite(reset_timeout) or reset_timeout >
   CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S: raise ValueError(...)`. Add `failure_threshold` bool guard
   (`isinstance(failure_threshold, bool) or not isinstance(failure_threshold, int)`). In base.py Field
   add `le=` (import const if no circular import; else literal 3600.0 + comment).
3. Run gate; commit `fix(circuit_breaker): reject inf/huge reset_timeout + bool failure_threshold (R21-A, mirror throttle cap)`.

## Unit B — DelayQueueStrategy monitor wiring (MED)
**File:** `src/scrapy_extension/queue/queue.py:163`; `src/scrapy_extension/queue/strategies/delay.py`. Ref pipeline.py:135-137, batched.py:112.
1. [RED] Test: build a BackendQueue with a real ScrapyStatsMonitor (via crawler.stats), strategy=delay,
   push a delayed item; assert `crawler.stats.get_value("queue/delay_depth")` is not None. Run → RED
   (production wiring never forwards the monitor; gauge absent).
2. [GREEN] Add `set_monitor(self, monitor: Monitor) -> None` to `DelayQueueStrategy` (store
   `self._monitor = monitor`; mirror BatchedStorageStrategy.set_monitor). In `BackendQueue.__init__`,
   AFTER the `self._monitor` assignment, forward: `set_monitor = getattr(self._strategy, "set_monitor",
   None); if callable(set_monitor): set_monitor(self._monitor)`.
3. Run gate; commit `fix(queue): wire the monitor into DelayQueueStrategy so queue/delay_depth emits (R21-B)`.

## Unit C — backoff overflow cap (LOW)
**File:** `src/scrapy_extension/backends/_retry.py:46`. Ref throttle ceiling.
1. [RED] Test: `compute_full_jitter_backoff(attempt=20, base_delay=1e303)` returns a finite value <=
   _MAX_BACKOFF_S (not inf). Run → RED (returns inf).
2. [GREEN] Add `_MAX_BACKOFF_S = 3600.0`; `delay = min(base_delay * (2 ** attempt), _MAX_BACKOFF_S)`
   before `random.uniform`.
3. Run gate; commit `fix(retry): cap full-jitter backoff so a huge retry_delay cannot overflow to inf/sleep(inf) (R21-C)`.

## Unit D — BatchedStorageStrategy NaN guard (LOW)
**File:** `src/scrapy_extension/storage/strategies/batched.py:83,86`. Ref delay.py `_require_finite`.
1. [RED] Test: `BatchedStorageStrategy(threshold=10, max_buffer_age_s=float('nan'))` raises ValueError.
   Run → RED (accepted).
2. [GREEN] Line 86 → `if max_buffer_age_s is not None and (not math.isfinite(max_buffer_age_s) or
   max_buffer_age_s <= 0)`. Line 83 `threshold` guard → same `isfinite` discipline (NaN-bypass).
3. Run gate; commit `fix(storage): reject NaN max_buffer_age_s/threshold in BatchedStorageStrategy (R21-D)`.

## Unit E — CLAUDE.md extras sync (LOW, docs)
**File:** `.claude/CLAUDE.md:336-347`. Ref README.md:38-48, pyproject.toml:58-72.
1. Replace the Optional Dependencies list with all 10 extras (redis/mongodb/kafka/rabbitmq/elasticsearch/
   rocketmq/pulsar/sqs/memcached/dynamodb) + `all`; correct `kafka-python` → `kafka-python-ng`.
2. Commit `docs(claude): sync Optional Dependencies — 10 extras + kafka-python-ng (R21-E)`.

## Definition of done
- [ ] ruff clean · mypy --strict 0 issues · pytest ≥3770 passed (unsandboxed) · coverage ≥95%
- [ ] 5 atomic commits on `worktree-round21-validation-monitor`
- [ ] ff-merged to `main`, pushed, worktree branch deleted
- [ ] memory updated (R21 close-out note in `deep-insight-2026-07-23-ultracode.md`)
