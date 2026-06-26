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

```python
# settings.py
SCRAPY_QUEUE_STRATEGY = "delay"
SCRAPY_QUEUE_DELAY_DEFAULT = 2.0  # seconds
```

**Caveat:** `delay`, `round_robin`, and `throttle` are per-process. Only
`passthrough` is distributed-exact.

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

- **Inspect:** the probe counter and cached depth are private (`_depth_probe_counter`,
  `_cached_depth`); the externally visible signal is `monitor.on_queue_depth`
  emitting from the cached sample.
- **Tune:** raise for faster backpressure response (more RTT), lower for
  higher throughput. `depth_sample_every=1` restores per-pop behavior.
- **Backpressure interaction:** `backpressure_pause_at` /
  `backpressure_resume_at` compare against the sampled depth; sampling
  keeps the comparison within ~1% variance of the real depth at default
  config.

## The memory-cap knobs (round-9 U5)

Two unbounded-growth paths are now capped by default:

| Surface | Default cap | Setting | Behavior on overflow |
|---|---|---|---|
| `MemoryMembershipFilter` | 1,000,000 entries | `SCRAPY_DEDUP_MEMORY_MAXSIZE` | LRU eviction (oldest entry dropped); warn-once at threshold |
| `DelayQueueStrategy` holding heap | 100,000 items | constructor `max_held` (soft cap) | Warn-once; non-positive disables |

**Advanced opt-out:** pass `maxsize=None` to `MemoryMembershipFilter` (or set
`SCRAPY_DEDUP_MEMORY_MAXSIZE=None`) for unbounded growth — only do this if
you have an external memory budget; the unbounded default was the round-9
finding (~366 MB @ 1M entries, ~3.58 GB @ 10M).

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
6. **Verify:** in a fresh venv, `pip install scrapy-extension==X.Y.Z` and
   import the package + one backend; confirm `__version__` matches.

For the stability commitment each release makes, see
[`STABILITY.md`](../STABILITY.md).
