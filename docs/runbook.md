# Operations Runbook

Common operational tasks for `scrapy-extension` deployments. Each recipe
assumes the Scrapy settings wiring is already in place (see
[`README.md`](../README.md) → *Quick Start*).

## Switch dedup strategy

Select a `MembershipFilter` via `SCRAPY_DEDUP_STRATEGY` — no code change
required.

| Strategy | When to use | Setting knobs |
|---|---|---|
| `set` (default) | Multi-worker exact dedup; cross-worker safe. | — |
| `memory` | Single-worker, in-process, optional LRU cap. | `SCRAPY_DEDUP_MEMORY_MAXSIZE` (default 1,000,000) |
| `bloom` | Single-worker, large cardinality, tolerates false positives. Never false-negatives. | `SCRAPY_DEDUP_BLOOM_CAPACITY`, `SCRAPY_DEDUP_BLOOM_ERROR_RATE` |
| `cuckoo` | Single-worker, large cardinality, needs deletion. Never false-negatives. | `SCRAPY_DEDUP_CUCKOO_CAPACITY`, `SCRAPY_DEDUP_CUCKOO_ERROR_RATE` |

```python
# settings.py
SCRAPY_DEDUP_STRATEGY = "cuckoo"
SCRAPY_DEDUP_CUCKOO_CAPACITY = 10_000_000
SCRAPY_DEDUP_CUCKOO_ERROR_RATE = 0.001
```

**Caveat (see [Guarantees](../README.md#guarantees)):** `memory`, `bloom`,
and `cuckoo` are per-process. For multi-worker exact dedup, use `set`.

When Cuckoo hits capacity it raises `FilterFull`, which `BackendDupeFilter`
catches, warn-once's, and degrades to passthrough for that fingerprint; the
`dupefilter/filter_full` stat increments on each occurrence.

## Switch queue semantics

Select a `QueueStrategy` via `SCRAPY_QUEUE_STRATEGY` — no code change
required.

| Strategy | When to use | Setting knobs |
|---|---|---|
| `passthrough` (default) | Backend queue semantics unchanged; distributed-exact. | — |
| `delay` | Per-request polite delay (e.g. rate-limit per domain). In-process; lost on crash. | `SCRAPY_QUEUE_DELAY_DEFAULT` |
| `round_robin` | Fair dispatch across `request.meta['source']`; no starvation. In-process. | — |
| `throttle` | Rate-limited pops. Effective rate under N workers = `N × (1 / min_interval)`. | `SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL` |
| `priority` | Strategy-layer priority buckets for backends without native priority. | `SCRAPY_QUEUE_PRIORITY_LEVELS` |
| `time_wheel` | Hashed timing wheel for many short delays. In-process. | `SCRAPY_QUEUE_TIME_WHEEL_SIZE`, `SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND` |
| `work_stealing` | Own queue first, then peer queues when idle. | `SCRAPY_QUEUE_WORKER_ID`, `SCRAPY_QUEUE_PEER_IDS`, `SCRAPY_QUEUE_STEAL_TIMEOUT` |
| `ring_buffer` | Bounded circular buffer with explicit overflow behavior. | `SCRAPY_QUEUE_RING_BUFFER_CAPACITY`, `SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY` |

```python
# settings.py
SCRAPY_QUEUE_STRATEGY = "delay"
SCRAPY_QUEUE_DELAY_DEFAULT = 2.0  # seconds
```

**Caveat:** every non-`passthrough` queue strategy keeps at least some state in-process. Only `passthrough` is distributed-exact. Treat delay heaps, timing wheels, rate limiters, work-stealing cursors, and ring buffers as performance/fairness tools rather than durable scheduling logs.

## Switch storage strategy

Select a `StorageStrategy` via `SCRAPY_STORAGE_STRATEGY` — no code change required.

| Strategy | When to use | Durability boundary |
|---|---|---|
| `passthrough` (default) | Item loss is unacceptable; backend round trips are acceptable. | Each item is written directly to the selected `StorageBackend`. |
| `batched` | Higher throughput is more important than immediate persistence. | Items sit in an in-process buffer until threshold / spider close; hard crash before flush loses the buffer. Store exceptions re-enqueue the unwritten tail. |

```python
# settings.py
SCRAPY_STORAGE_STRATEGY = "batched"
```

Use `passthrough` for the strongest persistence semantics. Use `batched` only with idempotent downstream consumers and an explicit crash-before-flush tolerance.

## Storage TTL semantics (expired = absent)

All five storage-capable backends (Redis, MongoDB, ElasticSearch, Memcached,
DynamoDB) uniformly enforce **an expired key is absent** — `retrieve` and
`exists` never surface a key whose TTL has elapsed. The mechanism differs by
backend family; the operator-visible contract does not.

| Backend family | TTL mechanism | `ttl()` return |
|---|---|---|
| Redis, Memcached | Native server-side expiry (`SET ... EX` / `client.set(..., expire=)`). The server reaps before the client sees the key. | Redis: seconds remaining (or `-1` no-TTL / `-2` no-key). Memcached: always `None` (no native introspection). |
| MongoDB | Native TTL index on the `expireAt` field (created in `_create_indexes`). The server background sweeper reaps. | Seconds remaining, or `-1` in the rare race before the sweeper reaches it. |
| ElasticSearch, DynamoDB | App-level TTL via an `expireAt` / `expire_at` field (neither has native TTL). `retrieve` / `exists` / `ttl` **lazily reap** an expired document on read (`_lazy_reap_if_expired`) and report it absent. | Seconds remaining, or `0` for an expired-or-missing key. |

This closes a stale-data gap where a read could surface an already-expired
document before the (infrequent) server reaper reached it. No setting change;
the expired-is-absent contract is now uniform across all five storage backends.

## Ack and durability matrix

| Surface | Ack / state boundary | Crash behavior | Operator action |
|---|---|---|---|
| Redis / MongoDB / ElasticSearch queue pop | Atomic pop removes the item from the backend queue. | A worker crash after pop can lose the request unless the spider re-enqueues or the backend implementation provides its own recovery. | Use idempotent callbacks and durable item storage for critical crawls. |
| Kafka / RabbitMQ queue pop | `pop_with_ack()` returns a per-message token; scheduler acks on Scrapy `response_received`. | Crash before ack redelivers. Crash after response but before callback/pipeline completion can lose downstream work. | Treat ack as downloader-level, not end-to-end completion. |
| RocketMQ queue pop | Deferred-ack queue support via the Apache gRPC client; set/storage are guarded. | Queue is at-least-once around ack, but callback/pipeline completion is still outside the ack boundary. | Pair with storage/dedup backends via per-component settings. |
| SQS / Pulsar queue pop | Per-message ack token (`pop_with_ack`) in a bounded in-flight set; acked on `response_received`. | Same download-level semantics as Kafka/RabbitMQ; crash before ack redelivers. | Safe under `CONCURRENT_REQUESTS > 1` — these backends ship a real in-flight ack set, not a single slot. |
| Backend/plugin declaring `supports_concurrent_ack=False` | Single ack slot only. | `CONCURRENT_REQUESTS > 1` raises at startup unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=True` is set. | Keep concurrency at 1, or pick a backend with a real in-flight ack set. |
| Stateful queue strategies | In-process scheduling/fairness/rate state. | Hard crash can lose held strategy state; snapshot/restore is best-effort where implemented. | Prefer `passthrough` when distributed durability beats scheduling policy. |
| `batched` storage | In-process write buffer. | Hard crash before flush loses buffered items. | Prefer `passthrough` when persistence must happen before item acknowledgement. |

## Multi-backend coexistence

Bind each component (queue / dedup / storage) to a *different* backend via
per-component type keys. Unset keys fall back to the global `SCRAPY_BACKEND_TYPE`.

```python
# Queue in Redis-Cluster, dedup + data in MongoDB
SCRAPY_QUEUE_BACKEND_TYPE = "redis"
SCRAPY_QUEUE_BACKEND_SETTINGS = {"mode": "cluster", "startup_nodes": [...]}

SCRAPY_SET_BACKEND_TYPE = "mongodb"
SCRAPY_SET_BACKEND_SETTINGS = {"uri": "mongodb://mongo:27017", "database": "scrapy"}

SCRAPY_STORAGE_BACKEND_TYPE = "mongodb"
SCRAPY_STORAGE_BACKEND_SETTINGS = {"uri": "mongodb://mongo:27017", "database": "scrapy"}
```

Each backend must implement the interface its component needs (see
[capabilities matrix](../README.md#backend-capabilities)). The
`ConnectionManager` registry keys one pooled connection per
`backend_type:settings_hash`, so co-located backends (set + storage both
MongoDB, same URI) share a single connection.

## Diagnose a `ConfigurationError` at startup

`ConfigurationError` is raised at config time (pydantic validators +
`model_validator`s in `settings/*.py`). The exception carries `setting_name`
and `setting_value` context attributes — surface them in your logs to find
the offending field.

Common causes (round-9 SV1–SV5 close all of these):

| Symptom | Likely cause | Where to look |
|---|---|---|
| "is not a valid Mode" / "valid values: …" | Mode field typo (SV1) — must be a member of the `Literal` enum | `settings/{kafka,pulsar,rabbitmq,mongodb}.py` mode fields |
| "X is required for mode Y" | Mode-specific required field missing (SV2) | `settings/{mongodb,redis,kafka,rabbitmq}.py` `model_validator`s |
| "SASL username without password" / "TLS cert without key" | Cross-field auth incoherence (SV3) | `settings/{kafka,pulsar,redis,mongodb,elasticsearch,sqs,dynamodb}.py` |
| "must start with a valid scheme" / "missing scheme" | Malformed host URL (SV4) | `settings/{mongodb,pulsar,rocketmq,elasticsearch,sqs,dynamodb}.py` |
| "must be >= 0" / "must be a positive integer" | Unbounded-int / empty-string gap (SV5) | `settings/{memcached,redis,rabbitmq,base}.py` |
| "tls_allow_invalid_certificates=True disables certificate verification" | Insecure TLS in production mode (SEC-2) | `settings/mongodb.py` |
| "credentials over cleartext http://" | ES cloud creds over http (SEC-3) | `settings/elasticsearch.py` |
| "endpoint_url must be http:// or https://" | LocalStack/AWS endpoint scheme (SEC-4) | `settings/{sqs,dynamodb}.py` |
| "aws_access_key_id and aws_secret_access_key must both be set" | Half-configured AWS creds | `settings/{sqs,dynamodb}.py` (config-time), `backends/connectors.py` (connect-time SEC-7) |

If the error is in a backend's `from_settings` / `from_crawler` factory,
check `backends/connectors.py:resolve_backend_config` — it resolves
per-component config and is the single chokepoint for the fallback chain.

## The depth-sampling knob (round-9 U4)

By default, `BackendQueue` probes real backend queue depth at most once per
100 pops (`depth_sample_every=100`), keeping the depth signal fresh for
backpressure gates while reclaiming ~25% of the pop-path RTT budget.

- **Tune:** set `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY=<N>` (default `100`).
  Raise for faster backpressure response (more RTT), lower for higher
  throughput. `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY=1` restores per-pop behavior.
  (Round-14 R14-C: this setting was deferred in round-9 — the constructor
  default was the only path; the setting now exists and is threaded by
  `BackendScheduler.from_settings` → `BackendQueue(depth_sample_every=…)`.)
- **Inspect:** the probe counter and cached depth are private (`_depth_probe_counter`,
  `_cached_depth`); the externally visible signal is `monitor.on_queue_depth`
  emitting from the cached sample.
- **Backpressure interaction:** `backpressure_pause_at` /
  `backpressure_resume_at` compare against the sampled depth; sampling
  keeps the comparison within ~1% variance of the real depth at default
  config.

## The per-item byte cap (round-9 D2)

`SCRAPY_QUEUE_MAX_ITEM_BYTES=<bytes>` (default `1048576` — 1 MiB, matches
the Memcached 1 MB ceiling). Requests exceeding this are rejected with
`SerializationError` at push time, preventing silent drops by capped storage
backends. (Round-14 R14-C: this setting was deferred in round-9 — the
constructor default was the only path; the setting now exists and is
threaded by `BackendScheduler.from_settings` → `BackendQueue(max_item_bytes=…)`.)

## The memory-cap knobs (round-9 U5)

Two unbounded-growth paths are now capped by default:

| Surface | Default cap | Setting | Behavior on overflow |
|---|---|---|---|
| `MemoryMembershipFilter` | 1,000,000 entries | `SCRAPY_DEDUP_MEMORY_MAXSIZE` | LRU eviction (oldest entry dropped); warn-once at threshold |
| `DelayQueueStrategy` holding heap | 100,000 items | `SCRAPY_QUEUE_DELAY_MAX_HELD` (soft cap; round-14 R14-C) | Warn-once; non-positive disables |

**Advanced opt-out:** pass `maxsize=None` to `MemoryMembershipFilter` (or set
`SCRAPY_DEDUP_MEMORY_MAXSIZE=None`) for unbounded growth — only do this if
you have an external memory budget; the unbounded default was the round-9
finding (~366 MB @ 1M entries, ~3.58 GB @ 10M).

## The operability-monitor knobs (round-12 U2)

`ScrapyStatsMonitor` surfaces two operability gauges whose thresholds are
now operator-tunable (round-14 R14-C — round-12 shipped the gauges with
constructor defaults only; the settings now exist):

| Setting | Default | Surface |
|---|---|---|
| `SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD` | `1000` | Depth above which `queue/backpressure` flips on |
| `SCRAPY_MONITOR_POP_RATE_WINDOW_S` | `60.0` | Trailing window (seconds) for the `queue/pop_rate` gauge |

Both are threaded by `BackendScheduler.from_settings` → the resolved
`ScrapyStatsMonitor` (and `pop_rate_window_s` is also forwarded to
`BackendQueue`, which computes the rolling rate).

## Diagnose a stuck crawl (page on zero pop rate)

Before round-10's operability signals (U2 — `on_pop_rate` /
`on_filter_saturation`, not yet landed), a stuck crawl must be diagnosed
from the existing monitor stats:

| Stat | What it tells you |
|---|---|
| `queue/depth` (sampled, U4) | Depth of the backend queue. `0` = empty queue (backend drained). |
| `dupefilter/filtered` | Count of duplicates filtered. High + rising = dedup saturated. |
| `dupefilter/filter_full` | Cuckoo filter hit capacity (each occurrence increments). Non-zero = Cuckoo at capacity, degrading to passthrough. |
| `pipeline/storage_skipped` | Items skipped because backend has no `StorageBackend`. Non-zero on a storage-expected backend = misconfigured `SCRAPY_STORAGE_BACKEND_TYPE`. |
| `scheduler/ack_error` | Ack commit to the queue backend failed (`QueueError` on `ack`). Non-zero on a deferred-ack backend (Kafka/RabbitMQ/RocketMQ/Pulsar/SQS) = the broker is rejecting commits; messages redeliver at-least-once via visibility-timeout — investigate broker health. |
| `scheduler/nack_error` | Nack to the queue backend failed (`QueueError` on `nack`). Non-zero = the broker rejected the requeue; the message will redeliver via visibility-timeout rather than being explicitly requeued. |

Differential diagnosis:

- **Queue empty + zero pop rate** → backend drained; produce more requests
  (or your scheduler is paused via backpressure — check
  `backpressure_pause_at`).
- **Queue non-empty + zero pop rate** → backend-down (check `backend.ping()`
  in your logs), throttle-strategy pinned, or `CONCURRENT_REQUESTS=0`.
- **Queue non-empty + pop rising + items not landing** → ack path broken
  (Kafka/RabbitMQ under `CONCURRENT_REQUESTS > 1` without
  `supports_concurrent_ack`); check for the `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`
  gate error at startup.
- **`dupefilter/filter_full` rising** → Cuckoo at capacity; raise
  `SCRAPY_DEDUP_CUCKOO_CAPACITY` or switch to `set` for unbounded dedup.
- **`scheduler/ack_error` or `scheduler/nack_error` rising** → the broker is
  rejecting ack/nack commits on a deferred-ack backend. Delivery keeps working
  (at-least-once redelivery via visibility-timeout), but duplicates rise —
  check broker connectivity/permissions and watch `dupefilter/filtered` for the
  redelivery side-effect.

## Cutting a release

High-level release flow (a dedicated `docs/release-runbook.md` does not yet
exist; this is the canonical procedure until it lands):

1. **Bump version:** `uv version <bump>` (or edit `pyproject.toml`
   `[project] version`).
2. **Sync lockfile:** `uv lock` (verify `uv lock --check` passes).
3. **Update CHANGELOG:** move the [`Unreleased`](../CHANGELOG.md) entries
   into a new `## [X.Y.Z] — YYYY-MM-DD` section.
4. **Tag:** `git tag vX.Y.Z` and push the tag.
5. **Publish:** `uv build && uv publish`.
6. **Verify gates:** `uv run ruff check`, `uv run pytest -m "not integration"`, and `uv run pytest --cov=scrapy_extension --cov-report=term-missing` (fails below 95%). For live backends, set the relevant `SCRAPY_TEST_*` variables and pass `--force-enable-socket`.
7. **Verify install:** in a fresh venv, `pip install scrapy-extension==X.Y.Z` and
   import the package + one backend; confirm `__version__` matches.

For the stability commitment each release makes, see
[`STABILITY.md`](../STABILITY.md).
