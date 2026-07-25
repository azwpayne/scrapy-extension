# Round 23 — SPEC: flush-lock close-drain regression + validation-caps cluster

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`
> (`25 0-13,19-23 * * *`), the successor to the retired `/loop 6a1a4470`.
> Scan: ultracode workflow `wf_69bc5952-da2` (6-dim find + adversarial verify;
> 17 agents, 0 errors, ~3.5M tokens). Base: `main` @ `9f1fedf` (post-R22).

## Scan result

**11 raw findings → 9 confirmed, 2 refuted.** 6 dimensions: resource-leak,
concurrency-locks, input-validation, docs-drift, **r22-diff-regression**
(adversarial), error-propagation. The fresh-eyes **resource-leak** dimension
and the standing **input-validation continuation** were the productive ones;
race/api-contract thin again. **The r22-diff-regression dimension caught the
headline MED** — a self-shipped R22-B regression — extending the streak
(R17-B, R18-C, R19-B, now R23-A) where the adversarial N-diff dimension earns
its keep by catching a regression in code I shipped in a prior round.

## Ship set (7 units)

| ID | Sev | Surface | Defect (one line) |
|----|-----|---------|-------------------|
| **A** | MED | `storage/strategies/batched.py:251` | R22-B bounded `_flush_lock` acquire makes `close()` abandon BUFFERED items (appended after the age-flusher's snapshot) for slow-but-healthy cross-region backends whose flush exceeds the 10s join+acquire window — a data-loss regression vs the pre-R22-B blocking acquire. Docstrings "never fires in normal operation" (L46) + "no at-least-once guarantee is lost" (L248-249) are inaccurate. |
| **B** | MED | `settings/redis.py:310` | `socket_timeout`/`socket_connect_timeout` `Field(ge=0)` accepts `inf` → `socket.settimeout(inf)` raises `OverflowError` (an `ArithmeticError`, NOT `OSError`) that escapes redis-py's `except OSError` trap. |
| **C** | MED | `settings/elasticsearch.py:105` | `request_timeout` `Field(ge=0)` accepts `inf` → ES client transport converts to socket timeout → `OverflowError` wraps every op in `ConnectionError`. |
| **D** | MED | `settings/rabbitmq.py:329` | `heartbeat` `Field(ge=0)` no upper bound; AMQP `Connection.Tune-Ok` marshals heartbeat as `struct.pack('>H', …)` (unsigned short) → values >65535 crash negotiation with opaque `struct.error`. Sibling `max_priority` already enforces `le=255`. |
| **E** | LOW | `settings/base.py:294` | `monitor_pop_rate_window_s` unbounded → `_record_pop_timestamp` deque grows without eviction (soft-OOM-by-misconfig). |
| **F** | LOW/docs | `.github/CHANGELOG.md:696` | `[Unreleased]` stops at R10/R11 era (#14, 2026-07-04); R17–R22 operator-visible behavior changes undocumented. |
| **G** | LOW | `backends/dynamodb.py:528` | `connect()` publish step has no `except BaseException` cleanup arm — candidate generation's already-open HTTP client (from `table.load`/`wait_until_exists`) leaks on Ctrl+C during operation_lock contention. Build arm (L327) covers pre-return only. Mirror `rabbitmq.py:540-558`. |

## Deferred set (2 units — documented, NOT fixed this round)

| Surface | Why deferred |
|---------|--------------|
| `queue/strategies/delay.py:351` | Verifier (MEDIUM conf, downgraded to LOW): `_drain_ready` holds `_state_lock` across `qb.push()`. The proposed pop-then-commit fix opens a NEW crash-loss window between heappop and push-success; the **current peek→push→pop ordering is strictly MORE durable** (item always in heap OR backend). Documented intentional atomicity; not reachable in standard single-threaded-per-spider Scrapy. Fix would regress durability — leave as-is. |
| `dupefilter/dupefilter.py:778` | `clear()` propagates `BackendConnectionError` raw when `clear_on_open=True`. 10-round backlog (R13 fire-13). Verifier: fail-fast is **defensible** here — silent degradation would run with stale fingerprints (silently skipping URLs the operator asked to re-crawl). No data loss; state stays consistent. Leave fail-fast; do not mirror `request_seen` graceful-degradation for the clear path. |

## Refuted (2) — guardrails held

- `backends/memcached.py:167` publish-step BaseException — microsecond contention
  window (`_client is None` → fail-fast), single GC-able FD, no bg threads;
  rabbitmq/redis rationale (heartbeat thread / FD pressure) does not transfer.
  Closed-cluster guardrail applies.
- `queue/strategies/time_wheel.py:363` lock-across-push — no background thread,
  not reachable in standard Scrapy, audited-clean sibling (delay.py), suggested
  fix predicated on a fix that does not exist.

## DO-NOT-RE-FLAG additions after R23

- BatchedStorageStrategy `close()` loops the flusher join to a hard
  `_CLOSE_DRAIN_DEADLINE_S` before the final `flush()` (R23-A); per-acquire
  `_FLUSH_LOCK_TIMEOUT_S` bound retained as the wedge guard.
- Redis socket timeouts + ES request_timeout reject non-finite + cap 86400s
  (R23-B/C); RabbitMQ heartbeat capped 65535 (R23-D).
- DynamoDB connect publish step carries `except BaseException` (R23-G) —
  cluster of "publish-window cleanup arms" now spans rabbitmq/redis/dynamodb.

## Out of scope / unchanged

- delay.py lock-across-push: intentional, more-durable-than-the-alternative.
- dupefilter.clear() fail-fast: defensible, 10-round backlog.
- Configurable `_CLOSE_DRAIN_DEADLINE_S` via Scrapy setting: noted as future
  work if operators on high-latency cross-region backends need to extend it.
