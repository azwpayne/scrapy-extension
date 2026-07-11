# Parallel Deep-Insight Survey — scrapy-extension (2026-07-10, **verified**)

> **Provenance.** Produced by a 14-agent parallel workflow (10 subsystem readers → barrier → 4 cross-cutting synthesis analysts), 3.04M subagent tokens, 251 tool-uses, ~10.6 min. Unlike the 2026-07-09 run (which thrashed to 6/18 readers), **all 14 agents completed**. The two CRITICAL claims below were then **re-verified in the main session by reading the cited code directly** (`queue.py:335-374`, `scheduler.py:358-422`), and the quality claims were checked by **running the suite**. Read this as the verified register; every finding carries a `file:line` citation.

**Evidence baseline (re-run 2026-07-10):** `uv run pytest -q --cov` → **1,972 passed, 37 skipped (2,009 collected), 15.88 s, total coverage 99.34 %**. `ruff check` → *All checks passed!* `mypy --strict` → *Success: no issues found in 71 source files*. HEAD `c661d8b`. Source ~18.2 k LOC / 71 files. Doc under edit (`docs/codebase-deep-insight.md`) was mid-revision; this register supersedes its §3.2/§3.4 where they conflict.

**Relationship to the 2026-07-09 register:** that doc was honest but *narrower* than first claimed and **inconsistent on Pulsar/SQS** (it labels Pulsar "single-slot" but SQS merely "deferred", while both backends set `supports_concurrent_ack=True`). This register corrects that: the single-slot bucket is **empty** for every bundled backend.

---

## Executive summary — the 4 findings that survived cross-corroboration

Four independent agents (≥) converged on each of the high-severity items below — they are not single-agent hallucinations.

1. **The §3.2 "three-bucket ack model" is stale; the code has two buckets.** SQS (`sqs.py:140`) and Pulsar (`pulsar.py:205`) both set `supports_concurrent_ack=True` with per-message tokens. The single-slot third bucket is empty; `_enforce_ack_concurrency_gate` (`scheduler.py:388`) is unreachable for all 10 bundled backends. The drift is baked into the `QueueBackend` ABC docstring (`base.py:388-404`) and the scheduler docstring/error message (`scheduler.py:368,408`). *(verified in main session)*
2. **`strategy × MQ` silently breaks per-message ack — the largest risk the prior docs missed entirely.** `BackendQueue._pop_with_ack` returns `token=None` for every non-passthrough strategy (`queue.py:374`), dropping MQ backends onto their legacy/no-token ack path. 7 strategies × 5 MQ backends = 35 silent-misconfig combinations; no gate, no warning, no test. *(verified in main session — the in-repo docstring at `queue.py:336-354` admits the design but is itself stale: it names only delay/round_robin/throttle and only Kafka/RabbitMQ/RocketMQ)*
3. **`BackendSpiderMixin` is a public-API trapdoor.** `setup_backend` (`spider_mixin.py:126`) constructs `ConnectionManager(...)` directly (bypassing the `get_manager` singleton), and `get_queue/get_dupefilter/get_scheduler` (`spider_mixin.py:298-360`) construct components via direct `__init__` instead of `from_settings/from_crawler` — silently losing monitor, dedup-strategy, queue-strategy, size caps, backpressure, and the ack gate. The mixin is in `__all__` and documented as the convenience integration path.
4. **The circuit breaker does not wrap `pop_with_ack`.** `_HOT_PATH = ('push','pop')` only (`circuit_breaker.py:373`); `pop_with_ack` is in neither `_HOT_PATH` nor `_FORWARDED`, so on MQ backends it resolves via `__getattr__` to the **raw** backend method. The exact broker-degradation scenario the breaker protects against goes un-tripped on the MQ ack-pop path.

**Verdict on maturity:** the codebase is genuinely high-quality on the happy path (99.34 % coverage, strict types, green integration) — but the "saturated / diminishing-returns" self-assessment (prior doc §11/§13) is **complacency, not evidence**: the same release window carries a stale load-bearing correctness model (§A), an undocumented 35-combination silent-correctness hazard (§B), and a public integration path that bypasses every safety gate (§C).

---

## §A — Ack-capability model drift (HIGH, doc + docstring)

| Doc claim | Code reality | Verdict |
|---|---|---|
| §3.2/§3.4: SQS + Pulsar = single-slot (`supports_concurrent_ack=False`) | `sqs.py:140` & `pulsar.py:205` both `True`; per-message `_SqsAckToken(receipt_handle)` / `_PulsarAckToken(message_id)` | **inaccurate** |
| §3.2: scheduler `from_settings` raises `ConfigurationError` for single-slot under `CONCURRENT_REQUESTS>1` | `scheduler.py:388` `if not requires_ack or supports_concurrent: return` — fires for none of the 10 bundled backends | **dead code** |
| Gate error msg "switch to a concurrency-safe backend (Kafka/RabbitMQ)" | `scheduler.py:408` — SQS/Pulsar are also concurrency-safe | **stale** |
| `QueueBackend` ABC docstring teaches the 3-bucket model | `base.py:388-404` — the contract surface 3rd-party authors copy | **stale in source** |

**Correct model (2 buckets):** atomic-pop (Redis/MongoDB/ES, `requires_ack=False`) vs per-message-ack (Kafka/RabbitMQ/RocketMQ/SQS/Pulsar, all `requires_ack=True, supports_concurrent_ack=True`). The gate remains a defensible backstop for a hypothetical 3rd-party single-slot backend — but it should be documented as such, not as a load-bearing bundled safety mechanism.

---

## §B — `strategy × MQ` ack bypass (CRITICAL, undocumented) ⭐

**Site:** `src/scrapy_extension/queue/queue.py:359-374`

```python
if isinstance(self._strategy, PassthroughQueueStrategy):
    # token-correlated path — ONLY here
    ...
return (self._strategy.pop(self.queue_name, timeout), None)   # line 374: token=None for ALL other strategies
```

**Mechanism:** the per-message ack token is injected into `request.meta["_backend_ack_token"]` only under `PassthroughQueueStrategy`. Any other strategy × any MQ backend → `token=None` → ack falls back to the backend's legacy/no-token path → silent at-least-once hazard.

**Why it matters:** the doc's headline §3.2 promise ("uniform at-least-once across heterogeneous MQ backends") does **not** hold for `delay+Kafka`, `priority+SQS`, etc. Round-15's 4 new strategies expanded the misconfig surface from 15 to **35** combinations. The in-repo docstring (`queue.py:336-354`) calls this "acceptable" — a design tradeoff admitted in source but absent from user-facing docs, and the docstring's own enumerations are stale (omits priority/time_wheel/work_stealing/ring_buffer; omits SQS/Pulsar).

**Recommended fix (tracked):** emit a `logger.warning` at `BackendScheduler.from_settings` when `strategy != passthrough` AND backend `requires_ack=True`. A warning (not a hard gate) preserves the intentional tradeoff while surfacing it to operators.

---

## §C — `BackendSpiderMixin` public-API trapdoor (HIGH)

| Site | Problem |
|---|---|
| `spider_mixin.py:126-129` | `ConnectionManager(...)` direct — bypasses `get_manager` singleton registry (defeats refcounting + LRU) |
| `spider_mixin.py:298-360` | `get_queue/get_dupefilter/get_scheduler` use direct `__init__` — bypass `from_settings/from_crawler`, losing monitor / dedup-strategy (always Set) / queue-strategy / `max_item_bytes` / backpressure / ack gate |

**Risk:** an operator using `BackendSpiderMixin` as the primary integration (which `__all__` and the docstring encourage) silently runs with every round-2/4/9/12/14 hardening disabled.

**Fix scope split (honest):**
- *Safe, in-session:* route `setup_backend` through `ConnectionManager.get_manager(...)` (drop-in; one existing test pins the current buggy constructor path and must be updated).
- *Needs design, deferred to issue:* routing `get_queue/get_dupefilter/get_scheduler` through `from_settings` requires deciding the crawler-less fallback (the mixin is used without a crawler in tests) and whether to honor `SCRAPY_*` settings. Rushing this would risk a public-API regression in a 99.34 %-coverage codebase.

---

## §D — Circuit-breaker `pop_with_ack` gap (HIGH)

**Site:** `src/scrapy_extension/backends/circuit_breaker.py:373`

`_QueueBackendProxy._HOT_PATH = ("push", "pop")`. `pop_with_ack` is absent from both `_HOT_PATH` and `_FORWARDED`, so for MQ backends it resolves via `__getattr__` (`circuit_breaker.py:338-343`) to the **raw** backend method — breaker records neither success nor failure. A broker degradation on the MQ ack-pop path (the scenario the breaker exists for) trips nothing.

**Fix:** add `"pop_with_ack"` to `_HOT_PATH`. Safe — `BackendQueue._pop_with_ack` only calls `backend.pop_with_ack` for backends that override it (MQ), so atomic backends are unaffected; an empty pop (`data=None`) is a success, not a failure.

---

## §E — Resilience-mechanism interactions (MED, undocumented)

The doc lists the 4 mechanisms individually (§7.3) but never analyzes how they **compose**:

- **Retry × breaker:** `ConnectionManager` retries up to 20× (full-jitter backoff). Because `pop_with_ack` is breaker-blind (§D) and RocketMQ `ping()` is local-state-only (`rocketmq.py:196-207`, false-healthy after outage), a broker outage can be retried-into without the breaker seeing it.
- **Snapshot-restore × backpressure:** `DelayQueueStrategy.close()` warns held items are "lost" (`delay.py:226-243`) but `BackendQueue._persist_snapshot` runs *first* (`queue.py:616`) — so the warning is misleading when storage is capable. `TimeWheelQueueStrategy.restore()` (`time_wheel.py:292-303`) re-pushes wheel items as `overflow@now`, losing future `ready_at` → up to `wheel_duration` (default 60 s) of delivery acceleration after restart.
- **Dedup-outage × at-least-once:** `scheduler.py:651-664` — a `dupefilter.request_seen` error default-enqueues the request **without** recording the fingerprint → the filter is permanently blinded to that URL **and** the URL is re-dispatched (auto-correlated double-dispatch).

---

## §F — Undocumented failure modes (MED–LOW)

| # | Site | Failure mode |
|---|---|---|
| F1 | `work_stealing.py:76` | default `worker_id=uuid4()`; restart without `SCRAPY_QUEUE_WORKER_ID` orphans the prior own-queue (stranded items, `queue_len` reports 0, no warning) |
| F2 | `dynamodb.py:316-336` | `clear_storage` scans only the first 1 MB page — silently partial on larger tables; `connect()` (`:124-135`) catches `except Exception` from `_table.load()` → spurious `create_table` on transient throttle |
| F3 | `memcached.py:219-236` | `clear_storage(prefix)` ignores `prefix` and `flush_all()`s globally |
| F4 | `rocketmq.py:467-479` | `clear_queue` is a silent no-op (inconsistent with `queue_len`'s loud `NotImplementedError`); `push(priority)` silently ignored (`:273-275`) |
| F5 | `pulsar.py:631-653` | `_ensure_consumer` leaks the prior consumer on topic change |
| F6 | `base.py:112-116` | `_decode_bytes_tag` has no try/except — a stored value shaped `{"__b64__": "<invalid>"}` crashes the whole pop deserialize via `binascii.Error` |
| F7 | `base.py:40-87` | `_json_default` raises `TypeError` on pydantic `SecretStr/SecretBytes` (untested) — inconsistent with §7.5's defensive redaction posture |
| F8 | `elasticsearch.py:421` vs `mongodb.py:636` | ES `store/retrieve/delete` raise raw `TransportError`, not `StorageError` → `except StorageError` catches MongoDB but misses ES |
| F9 | ttl() divergence | Redis→`None` / Mongo→`0` / ES→`-1` for expired — none matches the `StorageBackend` ABC contract |
| F10 | `kafka.py:706-734` | `nack` holds offset uncommitted; no in-session re-delivery (only on consumer restart) — operators expecting RabbitMQ-style fast retry get none |

**Supply-chain + test posture (structural blind spot):** `pymemcache` ~1300 days stale (U20); `kafka-python-ng` fork; `rocketmq-python-client` 5.1.1 young; `pulsar-client` range-pinned. **Every MQ unit test mocks the client lib**, so real-API drift is invisible to the suite — the exact failure mode that left the prior RocketMQ backend unconnected since project inception.

---

## §G — Doc-accuracy scorecard (`docs/codebase-deep-insight.md`)

Across ~140 doc claims checked by the 10 readers: **~75 % accurate, ~15 % drift, ~7 % inaccurate, ~3 % unverifiable.** Notable:

- **Inaccurate:** §3.2/§3.4 SQS+Pulsar single-slot (§A); §4.1 Bloom/Cuckoo cross-worker = "Depends on backing store" (both are per-process — `bloom_filter.py:55`, `cuckoo_filter.py:72`, `factory.py:60`); §8/R14-B "uniform ConfigurationError family" (`elasticsearch.py:183` raises bare `ValueError`).
- **Drift:** §9 `connectors.py` "100 %" (targeted measure ≈ 84.6 % CM-only / 90.1 % full-L2; missing `_registry_key` JSON-fallback + `connect()` contract-violation guard); §10 "14-test suite" (actual 28); §5 scheduler "731 LOC" (753); test counts 1972/1881 vs 2009 collected / 1972 passed.
- **Accurate (load-bearing, verified):** 4-ABC method tables; `BackendType` 10 members + `_missing_`; atomic-pop `requires_ack=False`; **`__b64__` symmetric round-trip** (P0, pinned by `test_backends.py:104`); registry lazy-import + entry-point discovery; **R14-H dep-vs-bug discrimination**; **R14-E victim-disconnect-outside-lock**; A2 single-connect; Redis Lua atomic pop + cluster hash-tag + FIFO counter; ES optimistic-lock pop; FilterFull graceful degradation; C2 "raise after N+1 consecutive"; SecretStr-by-type-name detection without importing pydantic; full-jitter backoff.

---

## §H — What holds up (balanced read)

The package's *core* is honest and well-built. The 4-ABC contract, the two-layer capability gate (config-time `resolve_backend_config` exclusion **plus** guard classes), `ConnectionManager`'s lock discipline (R14-E), the serializer's P0 symmetry fix, Redis's Lua-atomic pop, and ES's optimistic-lock pop all do real, verified work. The issues above are **integration-path and documentation** gaps layered on a sound core — not architectural decay. The `BackendSpiderMixin` problem, the strategy×MQ interaction, and the stale ack doc are each fixable without disturbing the 4-ABC spine.

---

## §I — Recommended actions (priority order, with `file:line`)

| # | Action | Site | Status |
|---|---|---|---|
| 1 | Correct §3.2/§3.4 ack model (3→2 buckets) + source docstrings | `docs/codebase-deep-insight.md` §3.2/§3.4; `base.py:388-404`; `scheduler.py:368,408` | **done (doc) / issue (docstrings)** |
| 2 | Warn on `strategy ≠ passthrough` × MQ backend | `scheduler.py` `from_settings` | **in-session (TDD)** |
| 3 | Fix/deprecate `BackendSpiderMixin` direct-`__init__` path | `spider_mixin.py:126,298-360` | **partial (get_manager) + issue** |
| 4 | Add `pop_with_ack` to breaker `_HOT_PATH` | `circuit_breaker.py:373` | **in-session (TDD)** |
| 5 | Sync metrics (test count, LOC, breaker tests) | doc §1/§9/§10/§12 | **done** |
| 6 | ES storage ops → wrap `StorageError`; unify `ttl()` contract | `elasticsearch.py:421`, three backends | **issue** |

---

## Withdrawn / caveats (honesty)

- The connectors.py coverage figures (84.6 % / 90.1 %) come from a **targeted** per-file agent measurement; the **full-suite** total is 99.34 %. Both are reported; do not read the lower number as the project coverage.
- The agents are static readers; backend behavior claims (e.g. Kafka nack semantics) are corroborated across multiple independent agents but were **not** exercised against a real broker in this pass (integration suites are deselected). The RocketMQ integration suite (`tests/integration/`) remains the authoritative real-broker check.
- "Dead code" verdicts on `_enforce_ack_concurrency_gate` and the strategy-specific `ConfigurationError` (`factory.py:143`, `# pragma: no cover`) are correct for **bundled** backends; both remain defensible backstops for 3rd-party plugins and were intentionally retained.

---

## §J — Resolution log (2026-07-11, in-session follow-up)

A TDD follow-up landed three fixes, each adversarially reviewed (4-dimension workflow). The review **caught that §D's first cut was incomplete** (proxy class-level shadowing of `pop_with_ack`) and the fix was extended in a second pass — the honest loop working as intended.

| Finding | Status | What landed |
|---|---|---|
| §A ack-model drift | **doc fixed** | `codebase-deep-insight.md` §3.2/§3.4 (3→2 buckets); `base.py` QueueBackend ABC docstring; `scheduler.py` `from_settings` docstring. Gate confirmed unreachable for bundled backends. Error message (`scheduler.py:408`) still stale — tracked in an issue. |
| §B strategy+MQ ack bypass | **warning landed** | `BackendScheduler._warn_strategy_mq_ack_bypass` fires at `from_settings` for non-passthrough strategy + `requires_ack` backend (parametrized test over 7 strategies × 3 MQ backends). The deeper fix (token-aware non-passthrough strategies) remains open. |
| §C `BackendSpiderMixin` | **partial** | `setup_backend` now acquires via `ConnectionManager.get_manager` (singleton; sharing test added). `get_queue/get_dupefilter/get_scheduler` `from_settings` routing deferred (design decision — issue). |
| §D breaker `pop_with_ack` | **fully fixed** | `_HOT_PATH += pop_with_ack` AND `queue.py:_pop_with_ack` unwraps the proxy (`getattr(backend, "_backend", backend)`) so the class-level override detection sees the real backend — MQ per-message ack tokens now survive under `SCRAPY_CIRCUIT_BREAKER_ENABLED`. End-to-end test (`test_backend_queue_pop_with_ack_token_survives_breaker_proxy`) pins it. |
| §F failure modes | **open** | WorkStealing orphan / DynamoDB partial-clear / Memcached flush / RocketMQ silent no-ops — tracked as issues. |
| §I Action 6 (ES `StorageError`/`ttl()`) | **open** | issue. |

**Suite after fixes:** `1,989 passed / 37 skipped (2,026 collected)`, coverage **99.42 %**, ruff `All checks passed!`, mypy --strict `Success: no issues found in 71 source files`. +17 tests vs the 2026-07-10 baseline (1,972).
