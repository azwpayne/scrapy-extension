# Operations Runbook

Common operational tasks for `scrapy-extension` deployments. Each recipe
assumes the Scrapy settings wiring is already in place (see
[`README.md`](../README.md) → *Quick Start*).

Before upgrading a persistent deployment, read the
[migration guide](migration-guide.md). It covers Redis physical-key changes,
strategy snapshot ownership, and queued-request wire compatibility.

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
| `priority` | Strategy-layer priority buckets for backends without native priority. Not supported with Kafka or RocketMQ. | `SCRAPY_QUEUE_PRIORITY_LEVELS` |
| `time_wheel` | Hashed timing wheel for many short delays. In-process. | `SCRAPY_QUEUE_TIME_WHEEL_SIZE`, `SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND` |
| `work_stealing` | Own queue first, then peer queues when idle. Not supported with Kafka or RocketMQ. | `SCRAPY_QUEUE_WORKER_ID`, `SCRAPY_QUEUE_PEER_IDS`, `SCRAPY_QUEUE_STEAL_TIMEOUT` |
| `ring_buffer` | Bounded circular buffer with explicit overflow behavior. | `SCRAPY_QUEUE_RING_BUFFER_CAPACITY`, `SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY` |

```python
# settings.py
SCRAPY_QUEUE_STRATEGY = "delay"
SCRAPY_QUEUE_DELAY_DEFAULT = 2.0  # seconds
```

**Caveat:** every non-`passthrough` queue strategy keeps at least some state in-process. Only `passthrough` is distributed-exact. Treat delay heaps, timing wheels, rate limiters, work-stealing cursors, and ring buffers as performance/fairness tools rather than durable scheduling logs.

`priority` and `work_stealing` create multiple physical queues. Their factories
raise `ConfigurationError` with Kafka and RocketMQ because those backends use a
single consumer that cannot isolate a pop to the requested strategy topic.
`round_robin` and `ring_buffer` are fully local: pairing them with an MQ backend
intentionally bypasses broker durability. Other bundled backend-delegating
strategies preserve per-message ack tokens.

### Snapshot ownership

`delay`, `round_robin`, `time_wheel`, and `ring_buffer` can snapshot local state
on a clean close. This is best-effort and works only when the **queue backend's
own connection manager** also implements `StorageBackend`; configuring a
separate item-storage backend does not redirect queue snapshots. Kafka,
RabbitMQ, RocketMQ, Pulsar, and SQS therefore cannot persist strategy snapshots.

For multiple workers running the same spider, set a stable unique identity:

```python
SCRAPY_QUEUE_WORKER_ID = "worker-a"
# Optional explicit override; otherwise WORKER_ID is used.
SCRAPY_QUEUE_SNAPSHOT_OWNER = "worker-a"
```

An owner selects a length-prefixed v2 storage key and prevents workers from
overwriting one another. Without an owner, the legacy spider+queue key remains
for single-worker compatibility. A successful restore consumes and deletes the
snapshot; the next clean close writes current state again. Hard crashes can
still lose changes since the last close.

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

## Redis namespace rollout

Set `SCRAPY_REDIS_NAMESPACE` to a stable value unique to the application and
environment. The default is `scrapy-extension`, which separates queue/set/
storage domains but does not distinguish two unrelated deployments sharing the
same Redis database.

Current keys do not fall back to the legacy raw layout. During an upgrade:

1. Stop old writers and take a backup.
2. Prefer draining the legacy request queue with the old version.
3. Explicitly migrate retained set/storage keys into the selected namespace,
   using the physical mapping in the [migration guide](migration-guide.md#redis-physical-key-layout).
4. Start all new workers with the same namespace. Do not run old and new writers
   against one logical backlog: they address different physical keys.

`clear_storage(None)` scans only the selected namespace's storage domain and
never flushes the Redis database.

## Storage TTL semantics (expired = absent)

All five storage-capable backends (Redis, MongoDB, ElasticSearch, Memcached,
DynamoDB) uniformly enforce **an expired key is absent** — `retrieve` and
`exists` never surface a key whose TTL has elapsed. The mechanism differs by
backend family; the operator-visible contract does not.

| Backend family | TTL mechanism | `ttl()` return |
|---|---|---|
| Redis | Native server-side expiry (`SET ... EX`). | Non-negative seconds remaining, or `None` for a missing, permanent, or expired key. Redis `-1`/`-2` sentinels are normalized away. |
| Memcached | Native server-side expiry (`client.set(..., expire=)`). | Always `None`; Memcached does not expose remaining TTL, even for a live expiring key. |
| MongoDB | Native TTL index on the `expireAt` field plus read-time expiry enforcement. | Non-negative seconds remaining, or `None` for missing, permanent, or expired. |
| ElasticSearch, DynamoDB | App-level expiry field. `retrieve` / `exists` / `ttl` lazily reap an expired document and report it absent. | Non-negative seconds remaining, or `None` for missing, permanent, or expired. |

This closes a stale-data gap where a read could surface an already-expired
document before the (infrequent) server reaper reached it. No setting change;
the expired-is-absent contract is now uniform across all five storage backends.

The direct `StorageBackend.store(..., ttl=...)` contract accepts `None`
(permanent) or a positive integer number of seconds. Zero, negatives, floats,
and booleans raise `ValueError` so all backends behave consistently. The Scrapy
pipeline keeps its setting-level compatibility rule:
`SCRAPY_PIPELINE_TTL = 0` is normalized to `None` before storage.

## Safe storage clearing

- Redis clear operations scan only `<namespace>:storage:*`; they never issue
  `FLUSHDB` or `FLUSHALL`.
- MongoDB, ElasticSearch, and DynamoDB honor a validated logical prefix.
- Memcached cannot enumerate or prefix-delete keys. Any non-`None` prefix raises
  `NotImplementedError`; `clear_storage(None)` also raises unless
  `SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=True`. Enabling it issues server-wide
  `flush_all`, so use it only on a dedicated instance.

## Ack and durability matrix

| Surface | Ack / state boundary | Crash behavior | Operator action |
|---|---|---|---|
| Redis / MongoDB / ElasticSearch queue pop | Atomic pop removes the item from the backend queue. | A worker crash after pop can lose the request unless the spider re-enqueues or the backend implementation provides its own recovery. | Use idempotent callbacks and durable item storage for critical crawls. |
| Kafka / RabbitMQ / Pulsar queue pop | `pop_with_ack()` returns a per-message token; scheduler acks on Scrapy `response_received`. Kafka also binds each token to its consumer generation, assignment epoch, and delivery attempt. | Crash before ack redelivers. Kafka nacks/rebalances retire the old attempt so its late completion cannot commit the replacement. Crash after response but before callback/pipeline completion can lose downstream work. | Treat ack as downloader-level, not end-to-end completion. RabbitMQ push waits for a publisher confirm and raises on unroutable/nacked delivery. |
| SQS queue pop | Receipt-handle token plus `SCRAPY_SQS_VISIBILITY_TIMEOUT` (default 300s). | The message can be delivered again if the pop-to-response interval exceeds the visibility lease. | No automatic renewal. Size the lease above worst-case download time. Explicit nack sets visibility to 0 for immediate redelivery. |
| RocketMQ queue pop | Message token plus `SCRAPY_ROCKETMQ_INVISIBLE_DURATION` (default 300s). | The message can be delivered again if the pop-to-response interval exceeds the invisibility lease. | No automatic renewal. Explicit nack shortens the lease to RocketMQ's 10-second minimum. Pair set/storage through per-component backends. |
| Backend/plugin declaring `supports_concurrent_ack=False` | Single ack slot only. | `CONCURRENT_REQUESTS > 1` raises at startup unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=True` is set. | Keep concurrency at 1, or pick a backend with a real in-flight ack set. |
| Stateful queue strategies | In-process scheduling/fairness/rate state. | Hard crash can lose held strategy state; a token-bearing replacement is rejected before it enters volatile delay/time-wheel/round-robin/ring-buffer state. | Use a backend-durable push path when replacing an unacked broker delivery; zero effective delay remains a direct backend push. |
| `batched` storage | In-process write buffer. | Hard crash before flush loses buffered items. | Prefer `passthrough` when persistence must happen before item acknowledgement. |

All five bundled deferred-ack backends use per-message tokens and support
`CONCURRENT_REQUESTS > 1`. The unsafe-concurrency gate remains for third-party
plugins that explicitly declare `supports_concurrent_ack=False`.

## Multi-backend coexistence

Bind each component (queue / dedup / storage) to a *different* backend via
per-component type keys. Unset keys fall back to the global `SCRAPY_BACKEND_TYPE`.

```python
# Queue in Redis-Cluster, dedup + data in MongoDB
SCRAPY_QUEUE_BACKEND_TYPE = "redis"
SCRAPY_QUEUE_BACKEND_SETTINGS = {
    "mode": "cluster",
    "cluster_startup_nodes": ["redis-1:6379", "redis-2:6379"],
    "namespace": "crawler-prod",
}

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

The project uses two validation exception families:

- `ConfigurationError` for backend capability checks, unknown/typoed flat or
  nested names, and project cross-field security rules. It carries
  `setting_name` and `setting_value` context attributes; sensitive setting
  names redact the value.
- `pydantic.ValidationError` for model field types, bounds, enum values, and
  direct `extra="forbid"` failures.

Do not catch only one family when building an operator-facing config checker.
Validation happens during component factory/connection startup before normal
crawl traffic.

For bundled backends, effective value precedence is explicit nested
`*_BACKEND_SETTINGS`, flat Scrapy setting, OS environment variable, then model
default. Scrapy project values therefore cannot be overridden by a same-named
environment variable. Unknown settings under the selected backend prefix fail
fast with a nearest-name suggestion.

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

### Connection retry controls

| Setting | Default | Contract |
|---|---:|---|
| `SCRAPY_RETRY_ATTEMPTS` | `3` | Retries **after** the initial attempt; range 0..20. Default therefore permits at most four total attempts. |
| `SCRAPY_RETRY_DELAY` | `1.0` | Base seconds for full-jitter exponential backoff: retry `n` sleeps uniformly between 0 and `base * 2**n`. Must be finite and non-negative. |

These are ConnectionManager-level controls. Backend-native retry settings are
separate. In particular, `SCRAPY_RABBITMQ_CONNECTION_ATTEMPTS` and
`SCRAPY_RABBITMQ_RETRY_DELAY` configure pika's inner connection policy; they do
not replace the generic manager settings above.

Every successful `ConnectionManager.get_manager()` acquisition must be paired
with exactly one `close()`. The registry is reference-counted; releasing the
same acquisition twice can prematurely retire a manager still expected by a
different holder.

## The depth-sampling knob (round-9 U4)

By default, `BackendQueue` probes real backend queue depth at most once per
100 pops (`depth_sample_every=100`), keeping the depth signal fresh for
backpressure gates while reclaiming ~25% of the pop-path RTT budget.

- **Tune:** set `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY=<N>` (default `100`).
  Lower for faster backpressure response (more depth RPCs); raise for lower
  pop-path overhead and a staler non-zero sample.
  `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY=1` restores per-pop behavior.
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

This knob cannot create a depth signal where the client has no backlog API.
Pulsar and RocketMQ `queue_len()` raise `NotImplementedError`; the scheduler
then skips depth-based backpressure for that poll, assumes pending work for idle
detection, and continues popping. Monitor those backends through pop rate and
broker-native tooling rather than `queue/depth`.

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
| `queue/depth` (sampled, U4) | Depth of a backend that supports `queue_len`. `0` = empty only when a sample was available; Pulsar/RocketMQ do not emit a broker depth. |
| `dupefilter/filtered` | Count of duplicates filtered. High + rising = dedup saturated. |
| `dupefilter/filter_full` | Cuckoo filter hit capacity (each occurrence increments). Non-zero = Cuckoo at capacity, degrading to passthrough. |
| `pipeline/storage_skipped` | Items skipped because backend has no `StorageBackend`. Non-zero on a storage-expected backend = misconfigured `SCRAPY_STORAGE_BACKEND_TYPE`. |
| `scheduler/ack_error` | Ack commit to the queue backend failed (`QueueError` on `ack`). Non-zero on a deferred-ack backend means the broker is rejecting commits; native offset/unacked/visibility semantics may redeliver the message. |
| `scheduler/nack_error` | Nack to the queue backend failed (`QueueError` on `nack`). The broker's native retry or lease-expiry behavior, if any, determines redelivery. |
| `scheduler/queue/poison_dropped` | A deterministic-invalid serialized request was terminally acked/dropped so it cannot pin a Kafka partition or hot-loop in a broker queue. |
| `scheduler/queue/empty_payload_dropped` | A broker record with a real ack token but no request payload (for example a Kafka tombstone) was terminally consumed. |
| `scheduler/queue/replacement_poison_dropped` | A retry/redirect replacement was locally invalid, so its original broker delivery was terminally consumed. |
| `scheduler/queue/volatile_replacement_rejected` | A replacement still owned an unacked broker source but its selected strategy would retain it only in process. The push was rejected before local mutation; use a backend-durable strategy/path. |

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
  rejecting ack/nack commits on a deferred-ack backend. Native broker redelivery
  may keep delivery moving, but duplicates rise —
  check broker connectivity/permissions and watch `dupefilter/filtered` for the
  redelivery side-effect. If the failure follows a committed retry/redirect
  replacement, that replacement remains accepted and dedup-reserved; the source
  token stays unresolved so broker redelivery can reach the duplicate-ack path.

## Poison payload handling

Malformed JSON, an invalid request schema/body codec, and oversize backend
payloads are deterministic for the same bytes. When the backend supplied an ack
token, `BackendQueue` attempts to ack and drop that delivery rather than nack it
into an infinite redelivery loop. The pop still raises `SerializationError`, and
the poison-drop stat increments only when the terminal transition succeeded.
If ack fails, the original deserialization error remains primary and the broker
may redeliver according to its normal policy.

For an upgrade carrying old queued requests, drain with the old version when
possible. Unmarked legacy bodies can be ambiguous: an old raw UTF-8 body that
happens to be valid Base64 cannot be distinguished from an intermediate Base64
wire format. See [queued-request wire format](migration-guide.md#queued-request-wire-format).

## Secret-bearing payloads

The JSON codec prevents code execution; it does not encrypt data. Request meta,
request bodies, and scraped items may contain credentials or personal data, and
supported secret wrapper objects are serialized to their underlying value.
Require TLS in transit, least-privilege queue/database ACLs, and encryption at
rest. Where the backend does not provide adequate at-rest protection, encrypt
the sensitive field in application code before enqueue/store. Avoid credentials
inside plain MongoDB/other DSN strings because caller logging or settings reprs
can expose them.

For RocketMQ, set `SCRAPY_ROCKETMQ_TLS_ENABLED=True` whenever access/secret
credentials are configured; cloud mode requires that combination. Anonymous
standalone/cluster plaintext remains available only for explicitly trusted
local deployments. Both producer and consumer use the same captured TLS,
endpoint, credential, timeout, and consumer-group snapshot for each connection
attempt.

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
6. **Inspect artifacts:** list the wheel and sdist. Confirm `py.typed`, public
   docs, and examples are present; confirm `.omc`, local `*.db`, ignored scratch
   plans, credentials, and editor state are absent. Render the wheel `METADATA`
   description and verify every non-anchor README link is an absolute web URL.
7. **Verify gates:** `uv run ruff check`, `uv run pytest -m "not integration"`, and `uv run pytest --cov=scrapy_extension --cov-report=term-missing` (fails below 95%). For live backends, set the relevant `SCRAPY_TEST_*` variables and pass `--force-enable-socket`.
8. **Verify install:** in a fresh venv, `pip install scrapy-extension==X.Y.Z` and
   import the package + one backend; confirm `__version__` matches.

For the stability commitment each release makes, see
[`STABILITY.md`](../STABILITY.md).
