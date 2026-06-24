# PLAN — Hardening Pass (2026-06-24)

Phased execution. **Tier 1 = 5 file-disjoint work units** (parallel-safe fan-out). Tier 2/3 deferred.
See [INSIGHT.md](./INSIGHT.md) for evidence, [SPEC.md](./SPEC.md) for goals/constraints.

## STATUS — live tracker (branch `fix/hardening-tier1-correctness-security`)

| Commit | Scope | State |
|---|---|---|
| `3651fc0` | **Tier-1** — 10 correctness/security fixes (Units A–E) | ✅ done — 1090 passed |
| `2a34648` | **Tier-2/3 r2** — observability (F), storage strategy (G), property tests (I) | ✅ done — 1153 passed |
| `4950a58` | **Tier-2/3 r3** — circuit-breaker (J), integration CI (K), Sentinel/Cluster tests (L), kafka-ng (M) | ✅ done — 1190 passed |
| `08f2a69` | **Tier-2 H** — ack in-flight-set (concurrency-correct at-least-once) | ✅ done — 1203 passed |

**Final verify:** 1203 passed / 27 skipped / 0 failed · ruff clean · mypy clean (65 files) · bandit clean.

**Residual (not done — small / needs infra):**
- Sentinel/Cluster malformed-entry errors propagate raw `ValueError` (round-3 pinned this; wrap in `BackendConnectionError`).
- No explicit Sentinel failover re-discovery path (delegated to redis-py `master_for` proxy).
- RocketMQ integration tests (need a runner image with native `librocketmq`).
- SQS/Pulsar in-flight-set ack (H shipped signature compat only; pin `CONCURRENT_REQUESTS=1` for them).
- Entry-point plugin registration for backends (architect bet #2). ES atomic pop (bet #6).


## Tier 1 — In-scope (parallel fan-out; disjoint files)

### Unit A — `backends/connectors.py`  *(CRITICAL + HIGH + MEDIUM)*
- **A1 refcounting**: `get_manager()` increments `_users`; `close()` decrements; the **last holder**
  disconnects + evicts the registry entry. Thread-safe under `_registry_lock`. Fixes the colocated
  close-ordering hazard.
- **A2 lock-during-retry**: split the fast connected-check (lock-free `_backend` read) from the slow
  `connect()`; release the lock during `time.sleep` so retry no longer stalls peer threads.
- **A3 enum normalization**: `resolve_backend_config` accepts a `BackendType` passthrough; wrap
  `BackendType(...)` in `try/except ValueError` → `ConfigurationError`.
- **Tests** (`tests/test_connectors.py`, `tests/test_connection_manager.py`): concurrency stress
  (N threads × same key → 1 manager; N threads × distinct keys → N managers); reconnect-after-close
  (fresh manager, `id` differs); colocated-close refcount (close one of two holders → backend stays
  alive); enum normalization (str, enum, invalid → ConfigurationError).
- **Files**: `backends/connectors.py`, `tests/test_connectors.py`, `tests/test_connection_manager.py`.

### Unit B — `backends/redis.py`  *(HIGH)*
- **B1 pop race**: `_POP_LUA` — return **distinct signals** for empty-queue vs lost-payload-race; the
  consumer treats a lost payload as "item consumed elsewhere" (DEBUG log, return `None`) instead of
  `QueueError`. Reserve `QueueError` for structural corruption only. Drop the integer `-1` sentinel.
- **Tests** (`tests/test_backends.py`, redis section): lost-payload race → returns `None` (no raise);
  empty → `None`; structural corruption → `QueueError`.
- **Files**: `backends/redis.py`, `tests/test_backends.py`.

### Unit C — `settings/redis.py` + `settings/rabbitmq.py`  *(HIGH × 2)*
- **C1**: `redis.py` `ssl_check_hostname` default → `True` (field description notes opt-out for
  IP-only service discovery).
- **C2**: `rabbitmq.py` — remove `default="guest"` (username) and `default=SecretStr("guest")`
  (password); make both required. Update any test relying on the default to pass explicit creds.
- **Tests** (`tests/test_config.py`): redis default asserts `ssl_check_hostname is True`; rabbitmq
  raises a validation error when creds are unset.
- **Files**: `settings/redis.py`, `settings/rabbitmq.py`, `tests/test_config.py`.

### Unit D — `queue/queue.py` + `pipeline/pipeline.py`  *(MEDIUM × 2)*
- **D1 legacy body**: `_decode_body` — detect non-base64-but-valid-UTF8 → fallback
  `body.encode("utf-8")` + one-time `DeprecationWarning` (rolling-upgrade safety, no silent drop).
- **D2 max-item-bytes**: configurable `SCRAPY_QUEUE_MAX_ITEM_BYTES` /
  `SCRAPY_PIPELINE_MAX_ITEM_BYTES` (default ~1 MB); reject oversize payloads with `SerializationError`
  + a stat counter rather than silent drop.
- **Tests** (`tests/test_queue.py`, `tests/test_pipeline.py`): legacy body round-trips; oversize
  rejected with stat increment.
- **Files**: `queue/queue.py`, `pipeline/pipeline.py`, `tests/test_queue.py`, `tests/test_pipeline.py`.

### Unit E — `schedule/scheduler.py` + `backends/{kafka,rabbitmq,rocketmq}.py`  *(HIGH + LOW × 2)*
- **E1 ack fail-fast**: at scheduler init (or backend connect), if backend is Kafka/RabbitMQ **and**
  `CONCURRENT_REQUESTS>1` **and** not `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=true` → raise
  `ConfigurationError`. Upgrades the existing warn → fail-fast (opt-out preserves escape hatch).
- **E2 confluent redaction**: wrap `confluent_api_key` / `confluent_api_secret` in `_RedactedStr`
  (`kafka.py:218-219`), matching the SASL path.
- **E3 rocketmq gating**: replace the dead `NotImplementedError` Set/Storage stubs with a
  class-level guard that raises `ConfigurationError` if RocketMQ is selected for set/storage (fail at
  construction, typed).
- **Tests**: fail-fast raises on `CONCURRENT_REQUESTS>1`; opt-out allows it; confluent `repr` redacted;
  rocketmq set/storage selection → `ConfigurationError`.
- **Files**: `schedule/scheduler.py`, `backends/kafka.py`, `backends/rabbitmq.py`,
  `backends/rocketmq.py`, relevant test files.

---

## Tier 2 — Deferred (spec'd, not in this pass)
- Full ack in-flight-set correlation (meta-stashed ack tokens) — the real fix behind E1.
- Observability: open `monitor/` namespace; `Monitor` protocol + `ScrapyStatsMonitor` default;
  backpressure hook on queue depth.
- `StorageBackend` strategy layer (`passthrough` + `batched`) — close the dedup/queue asymmetry.
- Entry-point plugin registration for backends (replace 4 hand-synced registries).
- Circuit-breaker around hot-path backend ops (beyond connect).
- ES atomic pop (optimistic concurrency via `seq_no`/`primary_term`).

## Tier 3 — Test / infra (deferred)
- Re-enable integration CI job (`.github/workflows/ci.yml:47`) with `services:` blocks
  (redis/mongodb/elasticsearch/rabbitmq/kafka).
- `hypothesis` property tests (serialization round-trip, round-robin fairness, filter FP-rate).
- Cuckoo FP-rate assertion; Sentinel/Cluster failover tests (subset of concurrency lands in Unit A).
- `kafka-python` → `kafka-python-ng` migration.

---

## Execution & verification
- **Fan-out**: Units A–E in parallel (disjoint files → no edit collision). Each executor:
  RED regression test → minimal fix → smoke-import own module. Do **not** run the full suite
  (concurrent mid-edit noise); the orchestrator runs the single authoritative full-suite verify.
- **Central verify (orchestrator)**: `uv run pytest` + `uv run ruff check` + mypy on changed files;
  fix any cross-unit fallout.
- **No commit without user sign-off** (orchestrator guidance); present a consolidated result + diff.
