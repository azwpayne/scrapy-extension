# Parallel Deep-Insight Survey — scrapy-extension (2026-07-09, **verified**)

> **Provenance.** Produced by a 25-agent parallel workflow (18 single-scope readers → 5 cross-cutting analysts → synthesis → critique). The environment thrashed the subagents (small per-agent context + provider 429s): only **6/18 readers** and **2/5 analysts** completed, the critique never ran. The synthesis report that survived was therefore **unverified**. This document is the **main-session verification pass**: every claim below was re-checked against the code with `grep`/`sed`/`Read`, the suite was re-run, and **four synthesis overstatements were caught and withdrawn**. Read this as the corrected record, not the raw synthesis.

**Evidence baseline:** unit suite `1970 passed, 7 skipped, 30 deselected` (integration-gated), 8.69 s. HEAD `312210c`. Source ~17.1k LOC / 71 files.

---

## Executive summary — the 5 truths that survived verification

1. **The architecture is honest and well-layered.** The 4-ABC contract, two-layer capability gates (config-time exclusion in `resolve_backend_config` *plus* guard classes), and `resolve_backend_config()` as a single source of truth for `QUEUE/SET/STORAGE_CAPABLE_BACKENDS` all do real work. The codebase does not silently lie about what a backend cannot do.
2. **The `JSONSerializer` bytes asymmetry was real but narrower than first claimed — and is now fixed.** `serialize` base64-encodes `bytes` while `deserialize` didn't decode them. **However, request bodies were never affected** — `queue.py:170-171` pre-base64-encodes the body and `_decode_body` (`queue.py:398`) decodes it back. The asymmetry only bit `bytes` in `meta`/`cookies`/`cb_kwargs` (raw pass-through, `queue.py:180-182`), and was an **R17-pinned one-way contract** (`test_backends.py:101` asserted `bytes → base64 str`), not a silent bug. **Fixed 2026-07-09 (P0):** tagged `{"__b64__": ...}` marker + `object_hook` decode; body path unchanged. *The original synthesis claim "every bytes body corrupts" was wrong — corrected during P0 landing.*
3. **There is a real doc/code drift on the RocketMQ ack model**, *not* a concurrency bug. `base.py` docstrings (lines 344, 418, 447) call RocketMQ "atomic-pop", but `rocketmq.py:66` sets `requires_ack = True`. The docstrings are stale. (The synthesis reported a `_pending`-map stale-reference leak — **no `_pending` map exists**; see Withdrawn #1.)
4. **The deferred-ack MQ backends (Kafka/RabbitMQ/RocketMQ) have solid concurrent-ack designs** — verified per-partition in-flight tracking (Kafka) and bounded delivery-tag sets (RabbitMQ). The residual risk is narrow and token-dependent, not structural.
5. **Observability and resilience have correctable seams, not crises** — the monitor gauge is emit-only by design (`stats.py:131` says so), throttling lives separately on the scheduler (`scheduler.py:94-103`); RabbitMQ multiplies retry counts (`rabbitmq.py:235`); BatchedStorage has no upstream backpressure under a sustained flush-stall.

---

## 1. Backend × capability matrix (verified)

| Backend | Queue | Set | Storage | Ack model (verified) |
|---|:---:|:---:|:---:|---|
| Redis | ✓ | ✓ | ✓ | atomic (Lua `ZADD`/`ZPOPMIN`, `redis.py:50,65`) |
| MongoDB | ✓ | ✓ | ✓ | atomic (`find_one_and_delete`, TTL index `expireAfterSeconds=0`, `mongodb.py:332-346,454`) |
| ElasticSearch | ✓ | ✓ | ✓ | atomic (sorted doc) |
| Kafka | ✓ | — | — | deferred, **concurrent-safe** (`requires_ack=True`, per-partition low-watermark commit, `kafka.py:72-139`) |
| RabbitMQ | ✓ | — | — | deferred, **concurrent-safe** (`basic_ack(delivery_tag=token)`, bounded `_in_flight_tags`, `rabbitmq.py:80,564-579`) |
| RocketMQ | ✓ | guard | guard | deferred (`requires_ack=True`, `rocketmq.py:66`) — **base.py docstrings mislabel it atomic** |
| Pulsar | ✓ | — | — | deferred (single-slot; nack-redelivery delay IS configurable, `settings/pulsar.py:71`) |
| SQS | ✓ | — | — | deferred (base64 `MessageBody`, `delete_message` ack, `sqs.py:266,410`) |
| Memcached | — | — | ✓ | n/a — `ttl()` always None, `clear_storage` flushes ALL (both documented, `memcached.py:51,205,220`) |
| DynamoDB | — | — | ✓ | n/a — app-level TTL `expire_at` checked on read (`dynamodb.py:166`) |

---

## 2. Confirmed findings (file:line verified)

### F1 — `JSONSerializer` bytes round-trip asymmetry  · **MEDIUM → FIXED (2026-07-09, P0)**
**Scope correction:** the original synthesis (and this doc's first draft) claimed "every non-empty bytes body corrupts." That was **wrong**. `queue.py:170-171` pre-base64-encodes the **body** before the serializer sees it, and `_decode_body` (`queue.py:398`) decodes it back — request bodies round-trip correctly. The asymmetry only affected `bytes` in **`meta`/`cookies`/`cb_kwargs`** (raw pass-through, `queue.py:180-182`). It was also an **R17-pinned one-way contract** (`test_backends.py:101` asserted `bytes → base64 str`), not a silent bug.

**Fix (landed):** `JSONSerializer` is now symmetric — `_json_default` emits a tagged `{"__b64__": "<b64>"}` marker for `bytes`/`bytearray`; `deserialize` uses an `object_hook` (`_decode_bytes_tag`) to reverse it. Plain ASCII strings (even valid base64) are never decoded. `"__b64__"` is a reserved meta key. Regression coverage: 3 unit tests in `test_backends.py` + the hypothesis property test now generates `bytes` in meta/cb_kwargs end-to-end. Body path untouched. Suite: 1972 passed; ruff clean.

### F2 — `base.py` docstrings mislabel RocketMQ as atomic-pop  · **MEDIUM (docs/code drift)**
`base.py:344, 418, 447` group RocketMQ with "atomic-pop backends (Redis, MongoDB, ElasticSearch, RocketMQ)". `rocketmq.py:66` sets `requires_ack = True` and `:67` `supports_concurrent_ack = True` — it is deferred-ack. Anyone reasoning from the docstring will mis-model RocketMQ's delivery semantics. Fix the docstrings.

### F3 — RabbitMQ retry-count multiplication  · **MEDIUM (resilience)**
`rabbitmq.py:235-236` passes pika's `connection_attempts`/`retry_delay` *inside* `ConnectionManager`'s own retry loop. Effective TCP attempts are approximately a product, not a sum (`retry_attempts=3` means one initial attempt plus three retries, or up to 12 TCP attempts under default pika). Surprising blast under network partitions.

### F4 — Backpressure gauge vs throttle knob are two mechanisms  · **MEDIUM (observability)**
`stats.py:136-139` emits `queue/backpressure` as a gauge; its docstring (`stats.py:131`) explicitly states "Observability only — no throttling". Actual throttling is the scheduler's `backpressure_pause_at`/`resume_at` hysteresis gate (`scheduler.py:94-103`). Verified bifurcation. (The synthesis claimed "no doc cross-reference in code" — `stats.py:131` *does* disclaim; the synthesis overstated this nuance.)

### F5 — BatchedStorage has no upstream backpressure under flush-stall  · **MEDIUM (resilience)**
`batched.py` auto-flushes at `threshold` (default 100, `:28,:83`) but there is no high-watermark that signals upstream when `flush()` itself blocks (slow/throttled DynamoDB or ES). Under sustained downstream latency the buffer grows unbounded between blocking flushes. Normal operation is bounded; the stall path is not.

### F6 — Exception-layer redaction is `ConfigurationError`-only  · **LOW-MEDIUM (security)**
`exceptions/base.py:149-160`: only `ConfigurationError` redacts its payload (`setting_value`). `BackendConnectionError`/`QueueError`/`StorageError` (`:34-100`) propagate whatever message was constructed. **However** — backend *connection-kwargs* redaction is broadly applied: 7 backends wrap secrets via `_redact()`/`_RedactedStr` (`mongodb.py:213`, `elasticsearch.py:75,79`, `kafka.py:245,272-275`, `pulsar.py:259`, `rabbitmq.py:209`, `sqs.py:186-187`, `dynamodb.py:120-121`). Redis uses `secret_value()` (discrete password kwarg, not a `redis://:pass@` URL) without `_redact()` wrapping. Net: the practical leak surface is narrower than the synthesis implied; the defensible gap is the exception layer + Redis.

---

## 3. Withdrawn / corrected (where the unverified synthesis overstepped)

> This section is the reason the verification pass exists. The synthesis was built on 6/18 readers and 2/5 analysts under context pressure; it produced four claims that the code does not support.

- **Withdrawn #1 — "RocketMQ `_pending` map stale-reference leak under concurrent pop/ack" (synthesis Top-Risk #4, HIGH).** No `_pending` dict/map exists. `rocketmq.py` uses a single `_last_msg` slot (`:83`), same legacy pattern as Pulsar/SQS. The cited lines (`353, 378, 427-429`) are `pop` / `pop_with_ack` / `ack` operating on `_last_msg`. With the scheduler passing `token=msg` (`ack` uses `target = token if token is not None else self._last_msg`, `:420`), per-message ack is correct. The only residual is the legacy `token=None` path — narrow and token-dependent, not a HIGH concurrency bug.
- **Withdrawn #2 — "only 4 of 10 backends redact kwargs; Redis/Kafka/RabbitMQ do not redact at all" (synthesis §3.10, §4.5).** Kafka, RabbitMQ, Pulsar, MongoDB, ES, SQS, DynamoDB all redact (7 backends). Redis is the one genuine gap and it uses discrete kwargs, not a URL. See F6.
- **Withdrawn #3 — "Pulsar's ack-timeout is unconfigurable" (synthesis §3.2, Top-Risk #8).** `settings/pulsar.py:71` exposes `negative_ack_redelivery_delay_ms`, wired through at `pulsar.py:645`. A nack-redelivery knob exists and is operator-configurable. (A positive-ack timeout may still be absent, but the absolute "unconfigurable" claim is wrong.)
- **Withdrawn #4 — "monitor gauge has no doc cross-reference in code" (synthesis §3.7).** `stats.py:131` explicitly states "Observability only — no throttling". The bifurcation is real (F4); the "undocumented" framing is not.

---

## 4. What's genuinely good (verified)

1. **Two-layer capability gates** — config-time exclusion in `resolve_backend_config` *plus* guard classes (`RocketMQSetBackend`/`RocketMQStorageBackend` raise `ConfigurationError`). Defense in depth, honest.
2. **`resolve_backend_config()` single source of truth** — all three component factories route through it; the capability frozensets don't drift.
3. **Kafka concurrent-ack** — token carries `(partition, offset)`; per-partition in-flight set; ack commits the contiguous low-watermark (`kafka.py:72-139`). Correct under `CONCURRENT_REQUESTS > 1`.
4. **RabbitMQ concurrent-ack** — `basic_ack(delivery_tag=token, multiple=False)` over a bounded `_in_flight_tags` set (`rabbitmq.py:564-579`).
5. **Atomic full backends done right** — Redis Lua-scripted `ZADD`/`ZPOPMIN` (`redis.py:50,65,456-457`), MongoDB `find_one_and_delete` + server-side TTL index (`mongodb.py:332-346`).
6. **Strategy snapshot/restore** — `delay.py:245` and `time_wheel.py:222` persist in-process state on `close()` and restore on startup; correct crash-recovery shape.
7. **Honest limitation docs** — Memcached `ttl()`/`clear_storage`, RocketMQ `clear_queue` no-op, and BatchedStorage crash-before-flush loss are documented, not hidden.

---

## 5. Verified risk ranking

| # | Severity | Location | Verdict |
|---|---|---|---|
| 1 | ~~HIGH~~ → **FIXED (P0)** | `base.py` `_json_default`/`deserialize` + `queue.py:180-182` | `bytes` in meta/cookies/cb_kwargs came back as base64 str (bodies were always safe). R17-pinned one-way contract. **Fixed:** tagged `{"__b64__":...}` marker + `object_hook`. |
| 2 | **MEDIUM** | `base.py:344,418,447` vs `rocketmq.py:66` | Stale docstrings mislabel RocketMQ ack model. Confirmed. |
| 3 | **MEDIUM** | `rabbitmq.py:235-236` | Retry-count product with pika. Confirmed. |
| 4 | **MEDIUM** | `stats.py:136-139` vs `scheduler.py:94-103` | Backpressure gauge/throttle bifurcation. Confirmed (docstring caveat). |
| 5 | **MEDIUM** | `batched.py:28,83` | No upstream backpressure under flush-stall. Confirmed. |
| 6 | **LOW-MED** | `exceptions/base.py:149-160`; `redis.py` (no `_redact`) | Exception-layer redaction is `ConfigurationError`-only; Redis kwarg not wrapped. Narrower than first claimed. |
| — | **WITHDRAWN** | ~~`rocketmq.py:353,378,427-429`~~ | No `_pending` map; fabricated by the unverified synthesis. |

---

## 6. Recommended next investigations (scoped, prioritized)

1. **(P0) Serializer round-trip regression.** Add a `bytes`-body round-trip test to `tests/test_backends.py`; fix `JSONSerializer.deserialize` to decode tagged base64 (or use a sentinel-bearing container). One-line conceptual fix, broad test surface.
2. **(P1) Fix `base.py` RocketMQ ack-model docstrings** (lines 344, 418, 447) to list RocketMQ as deferred-ack, matching `rocketmq.py:66`. Pure doc fix; prevents reasoning errors.
3. **(P1) RabbitMQ retry semantics.** Decide product-vs-sum explicitly; document the effective attempt count, or zero out pika's inner `connection_attempts` when the ConnectionManager wraps it.
4. **(P2) BatchedStorage high-watermark.** Add a buffer-size backpressure signal (monitor hook or blocking `store`) before OOM under downstream stall.
5. **(P2) Exception-layer redaction parity.** Apply `_redact()` to connection-string-bearing messages in `BackendConnectionError` construction paths; add a test asserting no exception emits a `://user:pass@` substring.
6. **(P3) Unify backpressure signal.** Either cross-reference the emit-only gauge to the scheduler throttle knob in code comments, or fold them into one signal.

---

## 7. Method note (what this pass cost)

Two full workflow runs (~3.4M subagent tokens combined) were consumed fighting the environment: per-agent context windows too small for this repo's large files (`connectors.py` 939 LOC, `scheduler.py` 753 LOC), plus provider 429 rate limits. The fan-out pattern thrashed on autocompact regardless of read-discipline instructions. The materially-different move that worked: **recover the partial synthesis, then verify + gap-fill in the main session** (large context, no thrash). For future deep-insight passes on this repo, prefer a **small fan-out of 4–6 opus agents each scoped to a single ≤300-line file with pre-extracted slices**, or do it inline.
