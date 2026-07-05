# scrapy-extension — Codebase Deep Insight

> **Generated:** 2026-07-05 (incremental from `/loop` + author deep-read of core ABCs)
> **Scope:** systematic, structured deep-dive. Covers all subsystems at architectural depth.
> **Sources:** `backends/base.py`, `exceptions/base.py`, the 3 strategy ABCs, `settings/base.py` (full reads); `connectors.py` / `scheduler.py` / `queue.py` (structural + prior agent findings); CLAUDE.md; coverage data; `.omc/plans/backlog-2026-07-03.md`.

---

## 1. System Purpose & Boundary

**`scrapy-extension`** is a Scrapy extension that turns a single-process crawler into a **distributed** one by externalizing Scrapy's in-process scheduler state, dedup set, and item sink onto pluggable backends.

**In scope:**
- Distributed request queue (priority-ordered, FIFO within priority)
- Distributed deduplication (exact or probabilistic membership)
- Distributed item storage (KV with TTL)
- 10 backend adapters (Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ, Pulsar, SQS, Memcached, DynamoDB)
- 3 pluggable strategy layers above the backends
- Resilience: retry, circuit breaker, backpressure, snapshot/restore
- Observability: Scrapy stats hooks

**Out of scope:**
- Scraping logic itself (spiders are user-authored; this package provides the *plumbing*)
- Proxy rotation, rate limiting at the HTTP layer, JS rendering
- Backend administration (brokers are externally managed)

**Scale:** 17,138 LOC in `src/`, 73 unit-test files + 9 integration suites, 1,881 unit tests / 37 skipped, 95%+ coverage floor, 26/26 integration green.

---

## 2. The Layered Mental Model

Five layers, each independently substitutable:

```
┌──────────────────────────────────────────────────────────────────┐
│  L5  Scrapy components    scheduler / dupefilter / queue /        │
│      (Scrapy-facing)      pipeline / spider_mixin                │
├──────────────────────────────────────────────────────────────────┤
│  L4  Strategy layers      ① MembershipFilter (dedup)             │
│      (pluggable)          ② QueueStrategy (queue semantics)      │
│                           ③ StorageStrategy (persistence)        │
├──────────────────────────────────────────────────────────────────┤
│  L3  Backend ABCs         Backend / QueueBackend /               │
│      (the contract)       SetBackend / StorageBackend            │
├──────────────────────────────────────────────────────────────────┤
│  L2  ConnectionManager    registry, lifecycle, retry, breaker    │
│      (lifecycle)                                                 │
├──────────────────────────────────────────────────────────────────┤
│  L1  Backend adapters     redis / mongodb / kafka / ... (10)     │
│      (implementations)    + registry + circuit_breaker + _redaction │
└──────────────────────────────────────────────────────────────────┘
        Configuration (pydantic-settings) cuts vertically through all layers
```

Each layer talks **only downward** through injected interfaces. Strategies receive a `ConnectionManager`; components receive strategies+backends via `from_settings()` / `from_crawler()` factories. No layer reaches up.

---

## 3. The Backend Contract (L3) — `backends/base.py`

### 3.1 The 4-ABC family

| ABC | Methods | Capability |
|-----|---------|-----------|
| `Backend` | `connect`, `disconnect`, `is_connected`, `ping`, `backend_type` (property) | Lifecycle — every backend implements this |
| `QueueBackend` | `push`, `pop`, `queue_len`, `clear_queue` + `pop_with_ack`, `ack`, `nack` (defaults) | Priority queue |
| `SetBackend` | `add`, `remove`, `contains`, `set_len`, `clear_set` | Membership (dedup) |
| `StorageBackend` | `store`, `retrieve`, `delete`, `exists`, `ttl`, `clear_storage` | KV with TTL |

A concrete backend (e.g. `RedisBackend`) inherits `Backend` + whichever capability ABCs it supports. `BackendType` enum (`REDIS`, `MONGODB`, `KAFKA`, `RABBITMQ`, `ELASTICSEARCH`, `ROCKETMQ`, `PULSAR`, `MEMCACHED`, `SQS`, `DYNAMODB`) — 10 members, with a `_missing_` hook that's the **defensive backstop** for direct `BackendType(x)` calls; user-facing validation goes through `Settings._validate_backend_type` → `ConfigurationError` (round-14 R14-B).

### 3.2 The Ack-Capability Contract (round-2) — the most subtle design

`QueueBackend` declares two class-level flags:

```python
requires_ack: bool = False          # True for MQ backends (Kafka/RabbitMQ/SQS/Pulsar)
supports_concurrent_ack: bool = True  # False for single-slot ack backends (SQS/Pulsar)
```

- **Atomic-pop backends** (Redis, MongoDB, ElasticSearch, RocketMQ): `requires_ack=False`. Their `pop` removes the item in one step; `ack`/`nack` are no-ops; `pop_with_ack` returns `(item, None)`.
- **Message-queue backends** (Kafka, RabbitMQ): `requires_ack=True`, `supports_concurrent_ack=True`. `pop_with_ack` returns `(item, token)`; the scheduler carries the token in `request.meta["_backend_ack_token"]` and hands it to `ack(token=...)` so the *specific* message is acked. Correct under `CONCURRENT_REQUESTS > 1` because each pop carries its own token.
- **Single-slot ack backends** (SQS, Pulsar): `requires_ack=True`, `supports_concurrent_ack=False`. N pops before any ack would overwrite a single receipt slot. The scheduler's `from_settings` gate **raises `ConfigurationError`** for `requires_ack and not supports_concurrent_ack` under `CONCURRENT_REQUESTS > 1` unless the explicit `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS` opt-out is set.

This contract is the reason the package can claim correct at-least-once semantics across heterogeneous MQ backends. It is non-obvious and load-bearing — touching it requires understanding all three buckets.

### 3.3 Serialization

`JSONSerializer` + `_json_default` handles the non-JSON-native types that real Scrapy `request.meta` values contain: `datetime`/`date` → ISO, `bytes` → base64, `Decimal` → str, `UUID` → str, `set` → list, `Enum` → `.value`, `Path` → str. Unhandled types raise `TypeError` with a clear message — **no silent `str()` coercion** (the old behavior produced `"b'x'"` and lost data). `Serializer` is a `Protocol`, so users can substitute their own.

### 3.4 Capability matrix (verified vs CLAUDE.md)

| Backend | Queue | Set | Storage | Notes |
|---------|-------|-----|---------|-------|
| Redis | ✅ | ✅ | ✅ | ZADD/ZPOPMIN; full |
| MongoDB | ✅ | ✅ | ✅ | Full |
| ElasticSearch | ✅ | ✅ | ✅ | Full |
| Kafka | ✅ | — | — | MQ; `requires_ack=True` |
| RabbitMQ | ✅ | — | — | MQ; `requires_ack=True` |
| RocketMQ | ✅ (deferred-ack) | Guard | Guard | `queue_len` unsupported — `NotImplementedError`, callers degrade |
| Pulsar | ✅ | — | — | Shared subscription; single-slot ack |
| SQS | ✅ | — | — | Standard queues; single-slot ack |
| Memcached | — | — | ✅ | KV+TTL only |
| DynamoDB | — | — | ✅ | KV+TTL; app-level TTL |

3rd-party backends register via `[project.entry-points."scrapy_extension.backends"]` (entry-point name = backend-type string → callable returning a `BackendDescriptor`). The 10 bundled are statically seeded in `backends/registry.py` as dotted-path strings; **lazy-import is preserved** (paths only, no backend module imported at registry-build time).

---

## 4. The Three Strategy Layers (L4)

All three follow the same shape: ABC + concrete strategies + factory, selected via a `SCRAPY_*_STRATEGY` setting, defaults byte-identical to pre-strategy behavior.

### 4.1 ① Dedup — `MembershipFilter` (`dupefilter/filters/`)

| Strategy | `SCRAPY_DEDUP_STRATEGY` | Exactness | Cross-worker | Capacity |
|----------|-------------------------|-----------|--------------|----------|
| `SetMembershipFilter` | `set` (default) | Exact | Yes (via SetBackend) | Backend-bound |
| `MemoryMembershipFilter` | `memory` | Exact | No (per-process) | RAM-bound |
| `BloomMembershipFilter` | `bloom` | Probabilistic (no false-neg) | Depends on backing store | Fixed |
| `CuckooMembershipFilter` | `cuckoo` | Probabilistic (no false-neg) | Depends on backing store | Bounded (raises `FilterFull`) |

`FilterFull` lives on the abstract interface (not the concrete filter) so the dupefilter layer catches it by type without importing concrete strategies. `BackendDupeFilter` treats `FilterFull` as "treat as not-seen" rather than crashing the crawl — graceful degradation.

### 4.2 ② Queue semantics — `QueueStrategy` (`queue/strategies/`)

| Strategy | `SCRAPY_QUEUE_STRATEGY` | Behavior |
|----------|-------------------------|----------|
| `PassthroughQueueStrategy` | `passthrough` (default) | Direct delegate to QueueBackend |
| `DelayQueueStrategy` | `delay` | In-process holding heap; pop blocks until ready |
| `RoundRobinQueueStrategy` | `round_robin` | Fairness across `source` tags |
| `ThrottleQueueStrategy` | `throttle` | Rate-limited pop |

Each strategy receives a `ConnectionManager` and drives the underlying QueueBackend (and StorageBackend where needed). **Snapshot/restore** (`snapshot()` / `restore()`, initiative #3) lets strategies with in-process held state (e.g. Delay's heap) persist state for crash/restart recovery — `BackendQueue` calls `snapshot()` on `close()` and `restore()` on startup. Default is `None` (nothing to persist).

### 4.3 ③ Storage — `StorageStrategy` (`storage/strategies/`)

| Strategy | `SCRAPY_STORAGE_STRATEGY` | Behavior |
|----------|---------------------------|----------|
| `PassthroughStorageStrategy` | `passthrough` (default) | Direct write per item |
| `BatchedStorageStrategy` | `batched` | Buffer + flush at threshold; at-least-once under partial failure (un-written tail re-enqueued); crash-before-flush loses in-flight batch (documented) |

**Backend-agnostic:** each `store()` call receives the `StorageBackend` (the pipeline owns the backend lifecycle). This is the cleanest of the three strategy layers.

---

## 5. Scrapy Components (L5)

All five follow Scrapy's `from_settings()` / `from_crawler()` factory pattern.

| Component | File | Responsibility |
|-----------|------|----------------|
| `BackendScheduler` | `schedule/scheduler.py` (731 LOC) | Scrapy scheduler; uses `BackendQueue` + dedup; gates ack-unsafe concurrency; backpressure pause/resume |
| `BackendDupeFilter` | `dupefilter/dupefilter.py` (403 LOC) | Delegates to a `MembershipFilter`; handles `FilterFull` gracefully |
| `BackendQueue` | `queue/queue.py` (725 LOC) | Request serialization/deserialization; delegates push/pop to a `QueueStrategy`; carries ack tokens in `request.meta`; depth-probe sampling (U4) |
| `BackendPipeline` | `pipeline/pipeline.py` (351 LOC) | Item storage via a `StorageStrategy` + `StorageBackend`; C2 escalation (max consecutive errors) |
| `BackendSpiderMixin` | `spider/spider_mixin.py` (395 LOC) | Spider mixin; `setup_backend()` in `__init__` |

`resolve_backend_config()` (in `connectors.py`) is the central config resolver used by all three component factories — enables **multi-backend coexistence**: queue in Redis, dedup in MongoDB, storage in ElasticSearch, each via independent connection managers keyed separately by `backend_type:settings_hash`.

---

## 6. Connection Management (L2) — `backends/connectors.py` (939 LOC)

`ConnectionManager` is the most complex single class in the codebase:

- **Lazy singleton registry** — class-level `_managers` dict keyed by `backend_type:settings_hash`; thread-safe via `_registry_lock`
- **LRU eviction at cap** — `MAX_MANAGERS` (default unbounded-configurable); `_collect_orphans_under_lock()` pops orphaned (refcount=0) entries under the lock, returns victims; **caller disconnects AFTER lock release** (round-14 R14-E fix — was a verified lock-mismatch bug, fixed in `.omc/ultragoal/plans/connection-manager-suite-lock-fix/`)
- **Exponential backoff retry** — full-jitter; `retry_attempts` (capped 0-20), `retry_delay`; `on_retry` monitor hook
- **A2 single-connect ownership** — N threads racing on `.backend` → exactly one owner takes the slow connect path; peers wait on `_connected_event`; owner errors propagate to all waiters
- **Circuit-breaker wiring** — `_get_breaker()` reads `SCRAPY_CIRCUIT_BREAKER_ENABLED` lazily; `get_queue_backend()` / `get_set_backend()` / `get_storage_backend()` return wrapped proxies when enabled, raw backends when disabled (byte-identical default)
- **`_create_backend()`** — imports backend classes dynamically (lazy)

`resolve_backend_config(settings, type_key, settings_key)` — module-level helper; per-component backend override (`SCRAPY_QUEUE_BACKEND_TYPE` etc.) with fallback to global `SCRAPY_BACKEND_TYPE`.

---

## 7. Cross-Cutting Concerns

### 7.1 Lazy Import (PEP 562)

`__init__.py` uses `_OPTIONAL_IMPORTS` dict + `__getattr__`. **Critically (R14-H)**: `__getattr__` distinguishes "genuine missing optional dep" (→ wrap as install hint) vs "real bug inside backend module" (→ re-raise original traceback) by checking `ModuleNotFoundError.name` against `_OPTIONAL_DEP_MODULES`. This prevents masking real bugs as "install scrapy-extension[X]". `pip install scrapy-extension` works without any backend deps.

### 7.2 Multi-Backend Coexistence

The killer feature. Override per component:
```
SCRAPY_QUEUE_BACKEND_TYPE=redis     + SCRAPY_QUEUE_BACKEND_SETTINGS=...
SCRAPY_SET_BACKEND_TYPE=mongodb     + SCRAPY_SET_BACKEND_SETTINGS=...
SCRAPY_STORAGE_BACKEND_TYPE=elasticsearch + SCRAPY_STORAGE_BACKEND_SETTINGS=...
```
Unset keys fall back to `SCRAPY_BACKEND_TYPE` / `SCRAPY_BACKEND_SETTINGS`. Each component gets its own `ConnectionManager` (keyed separately in the registry).

### 7.3 Resilience — four independent mechanisms

1. **Retry** — `ConnectionManager.connect()` full-jitter exponential backoff (max 20 attempts)
2. **Circuit breaker** (`backends/circuit_breaker.py`, 440 LOC, **100% coverage**) — opt-in (`SCRAPY_CIRCUIT_BREAKER_ENABLED`); CLOSED→OPEN on N consecutive failures; OPEN→HALF_OPEN after timeout; HALF_OPEN→CLOSED on success or back to OPEN on failure. Wraps backends via `_QueueBackendProxy` / `_SetBackendProxy` / `_StorageBackendProxy`.
3. **Backpressure** (`backpressure_pause_at` / `backpressure_resume_at`) — depth-based pause/resume with **hysteresis** (resume ≤ pause, else `ConfigurationError`); scheduler returns `None` (Scrapy's contract-correct "slow down" signal)
4. **Snapshot/restore** — queue strategies persist in-process state on close, restore on startup (initiative #3); corrupt state is logged + skipped, never crashes

### 7.4 Observability — `monitor/` (Unit F Tier-2)

| Class | Role |
|-------|------|
| `Monitor` (ABC) | Interface every component accepts |
| `NullMonitor` | Safe default (no crawler, no stats) |
| `ScrapyStatsMonitor` | Wired by `from_crawler` when `crawler.stats` available |

Components accept `monitor: Monitor = NullMonitor()` and call hooks: `on_queue_depth(depth)`, `on_pop_rate(count)`, `on_filter_saturation(ratio)`. Wiring is **additive** — existing stat keys unchanged. Gated by `SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD` (1000) and `SCRAPY_MONITOR_POP_RATE_WINDOW_S` (60.0).

### 7.5 Defensive Secret Redaction — `exceptions/base.py`

`ConfigurationError(setting_name, setting_value)` **auto-redacts** when `_is_sensitive_name(name)` (fragments: password/secret/api_key/apikey/token/credential) OR `_is_secret_value(value)` (pydantic SecretStr/SecretBytes detected by type-name — no pydantic import). Raw value never retained on the exception once redacted. Prevents accidental secret leaks via `repr(exc)` or debug-logging.

### 7.6 Input Validation at Boundaries

`_validate_key_name()` (regex `^[a-zA-Z0-9._:-]+$`) prevents queue/set/index name injection. `_hash_item()` SHA256 for fingerprints. `queue_max_item_bytes` / `pipeline_max_item_bytes` (1 MiB default) reject oversized items at push/store time — prevents silent drops by capped backends (Memcached 1 MB, DynamoDB 400 KB).

---

## 8. Configuration Surface — `settings/base.py` (323 LOC)

`Settings(BaseSettings)` — pydantic-settings, `env_prefix="SCRAPY_"`, `case_sensitive=False`, `extra="ignore"`.

**Categories:**

| Category | Settings |
|----------|----------|
| Backend selection | `backend_type`, per-component `_BACKEND_TYPE` overrides |
| Connection | `retry_attempts` (0-20), `retry_delay` |
| Sizing | `queue_max_item_bytes`, `pipeline_max_item_bytes` (1 MiB each) |
| Strategies | `storage_strategy` (passthrough/batched); dedup/queue strategies via their own keys |
| Pipeline escalation | `pipeline_max_storage_errors` (C2: None=swallow, N=raise after N+1 consecutive) |
| Circuit breaker | `circuit_breaker_enabled` (False default), `_failure_threshold` (5), `_reset_timeout` (30s) |
| Backpressure | `backpressure_pause_at`, `backpressure_resume_at` (BP-1, hysteresis cross-validated) |
| Queue sampling | `queue_depth_sample_every` (100, round-9 U4), `queue_delay_max_held` (100k, round-9 U5 OOM cap) |
| Observability | `monitor_backpressure_threshold` (1000), `monitor_pop_rate_window_s` (60.0) |

Cross-validation: `_validate_backpressure_thresholds` (round-4 BP-1) — pause/resume non-negative, resume ≤ pause.

Per-backend settings modules: `redis.py` (4 modes), `mongodb.py` (4 modes), `kafka.py` (3), `rabbitmq.py` (3), `elasticsearch.py` (2), `rocketmq.py` (3), `pulsar.py` (2), `sqs.py` (2), `memcached.py` (1), `dynamodb.py` (2).

---

## 9. Quality Posture

| Dimension | Status |
|-----------|--------|
| Unit coverage floor | **95%+** (project rule: never below 95%) |
| `circuit_breaker.py` | **100%** (137/137 stmts, 30/30 branches) — full state machine |
| `monitor/` (base + stats) | **100%** |
| `storage/strategies/` | 99-100% (batched.py 98.36% — 1 empty-flush branch) |
| `connectors.py` | 97.67% (post connection-manager-suite-lock-fix) |
| Integration suites | 26/26 green (skip-by-default; two-layer gate `SCRAPY_TEST_INTEGRATION=1` + per-backend URL) |
| Ruff | clean |
| Type hints | full; `py.typed` marker |
| Test count | 1,881 unit + integration tests |

Test architecture: pytest with mocked backends (no real services for unit); real-broker integration in `tests/integration/` (docker-compose bring-up).

---

## 10. Tech Debt & Risk Posture

### Mature / low-risk
- Backend ABCs and the 4-capability model — stable since early rounds
- Ack-capability contract — non-obvious but well-tested
- Circuit breaker — 100% coverage, 14-test suite
- ConnectionManager — hardened by connection-manager-suite-lock-fix (R14-E)
- Lazy import with R14-H dep-vs-bug discrimination

### Architect-deferred (per 2026-07-03 backlog)
- #17 Depth-probe before backpressure gate (MED) — bounded jitter; costs per-pop RPC
- #18 Snapshot versioning + restore diagnostics (MED) — forward-looking
- #19 Lock-free-read invariant doc + test (LOW)
- #20 Staleness doc on queue/depth (LOW)
- #21 Cross-instance Throttle via shared backend counter (LOW)
- #22 RoundRobin cross-worker fairness doc (LOW)

### Recent additions (less battle-tested)
- `monitor/stats.py` — Unit F Tier-2; 100% covered but newer
- `storage/strategies/batched.py` — at-least-once semantics documented; crash-before-flush data loss is a known separate failure mode
- RocketMQ integration suite — flake-tolerant (skips on apache proxy NPE; string-matches broker error text — fragile to broker version bumps)

### Operational cautions
- `monitor/` and `StorageStrategy` are NOT in package `__all__` — internal-only surface (deliberate; separate API-export decision pending)
- RocketMQ `queue_len` permanently `NotImplementedError` (apache 5.x SimpleConsumer has no depth API) — all 3 callers gracefully degrade, but operators reading depth stats need to know
- Batched storage + crash = in-flight batch lost — documented but worth flagging in ops guides
- `/goal` bridge dead in this environment (omc CLI missing) — ultragoal artifacts execute inline

---

## 11. Round History (the codebase carries its evolution)

The code is annotated with `round-N` / initiative markers — this codebase has been through deliberate architectural iterations:

| Round | Theme | Representative markers |
|-------|-------|------------------------|
| 2 | Ack-capability contract | `requires_ack`, `supports_concurrent_ack`, `pop_with_ack` |
| 4 | Backpressure (BP-1) | `backpressure_pause_at` / `_resume_at`, hysteresis |
| 5 | 3rd-party backend registry (R5-1) | `BackendType \| str`, entry-points |
| 9 | Queue hot-path (U4 sampling, U5 OOM cap, D2 size cap) | `queue_depth_sample_every`, `queue_delay_max_held`, `queue_max_item_bytes` |
| 12 | Operability (U2) | `monitor_backpressure_threshold`, `monitor_pop_rate_window_s` |
| 13 | Storage error family | `StorageError` exception |
| 14 | Documentation + hardening (R14-B/C/E/H) | `_validate_backend_type` ConfigurationError, registry lazy-import, victim-disconnect-outside-lock, dep-vs-bug discrimination |

This is a **mature, deliberately-evolved** codebase. Each round left the code more defensive and the contract more explicit. The "diminishing returns" verdict from the 2026-07-03 backlog reflects this — the architectural surface is saturated; remaining work is feature-driven (new backends, new strategies) not surveillance-driven.

---

## 12. Onboarding Map

**Read in this order:**
1. `CLAUDE.md` (project root) — system overview, source structure, capability matrix
2. `src/scrapy_extension/backends/base.py` — the 4 ABCs + ack contract (this is the contract everything else implements)
3. `src/scrapy_extension/exceptions/base.py` — error model + secret redaction
4. `src/scrapy_extension/settings/base.py` — global config (cross-validated)
5. `src/scrapy_extension/backends/connectors.py` — `ConnectionManager` (the lifecycle + registry + retry + breaker wiring)
6. Pick ONE backend impl (e.g. `redis.py`) — see how the ABCs are realized
7. The 3 strategy ABCs (`dupefilter/filters/base.py`, `queue/strategies/base.py`, `storage/strategies/base.py`) — see the pluggability shape
8. `schedule/scheduler.py` — see how everything is wired into Scrapy
9. `docs/runbook.md` — operational settings reference
10. `.omc/plans/backlog-2026-07-03.md` — architect-deferred items + discounted false-positives

**Run to verify health:**
```bash
uv sync
uv run pytest -q                    # ~8s, 1881 passed / 37 skipped
uv run pytest --cov=scrapy_extension --cov-report=term-missing  # 95%+ floor
uv run ruff check src/ tests/       # clean
```

---

## 13. TL;DR

`scrapy-extension` is a **layered, capability-gated, strategy-pluggable** Scrapy distribution layer. Its core insight: model backends as a 4-ABC family (`Backend` + `Queue` + `Set` + `Storage`), let concrete backends declare which they implement, and gate unsupported combinations at config time (`ConfigurationError`). Above that, three strategy ABCs (`MembershipFilter`, `QueueStrategy`, `StorageStrategy`) make dedup / queue semantics / persistence pluggable without touching backends. `ConnectionManager` owns lifecycle (singleton registry, retry, breaker, eviction). The ack-capability contract (round-2) is the most subtle load-bearing design — it's what makes at-least-once correctness uniform across heterogeneous MQ backends.

The codebase is **mature and saturated** (95%+ coverage, 100% on critical modules, 26/26 integration green). Future value is feature-driven (new backends, new strategies, new monitor hooks), not surveillance-driven — `/loop`-style scanning has diminishing returns here.
