# Stability

This document describes the stability tiers of `scrapy-extension`'s public
surface, so downstream users can decide what is safe to depend on.

The tiers follow [Semantic Versioning 2.0.0](https://semver.org/), including its
pre-1.0 rule: a `0.x` minor release may contain a breaking change. For this
project:

- **Stable** — no intentional breaking change in a patch release. Before 1.0,
  an unavoidable break requires a minor version bump, an explicit changelog
  entry, and migration guidance. At and after 1.0 it requires a major bump.
- **Experimental** — usable, but the signature, setting name, wire format, or
  semantics may change in a pre-1.0 minor release with changelog notice.
- **Internal** — `_`-prefixed symbols and unlisted implementation helpers. No
  compatibility promise; do not import them directly.

Public surface is determined by the owning namespace, not only the package
root. It includes names in `scrapy_extension.__all__`, names exported by a
subpackage's documented `__all__`, and the fully qualified public symbols
explicitly listed below (for example
`scrapy_extension.backends.connectors.resolve_backend_config`). A name's tier
comes from this document; being exported does not automatically make it Stable.

## Components and tiers

| Surface | Tier | Notes |
|---|---|---|
| `BackendScheduler` (`schedule/scheduler.py`) | Stable | `from_settings` / `from_crawler` factory; backpressure gates (round-4) are separately flagged Experimental below. |
| `BackendDupeFilter` (`dupefilter/dupefilter.py`) | Stable | Strategy selection via `SCRAPY_DEDUP_STRATEGY` is Stable; per-strategy tiers in the next table. |
| `BackendPipeline` (`pipeline/pipeline.py`) | Stable | |
| `BackendQueue` (`queue/queue.py`) | Stable | `depth_sample_every` (round-9 U4) is Stable with a safe default. |
| `BackendSpiderMixin` (`spider/spider_mixin.py`) | Stable | |
| `Backend` / `QueueBackend` / `SetBackend` / `StorageBackend` ABCs | Stable | The abstract contract 3rd-party backends implement. |
| `ConnectionManager` (`backends/connectors.py`) | Stable | Lazy shared registry keyed by `backend_type:settings_digest`. Each `get_manager()` acquisition requires exactly one `close()` release. |
| `scrapy_extension.backends.connectors.resolve_backend_config()` | Stable | Public fully qualified import used by all three component factories. |
| `scrapy_extension.monitor.Monitor` / `NullMonitor` / `ScrapyStatsMonitor` | Stable | Public subpackage exports. The hook set is additive; fresh hooks are tiered below. |
| `BackendType`, `Serializer`, `JSONSerializer`, and `Settings` | Stable | Root-package exports and core extension contracts. |
| Root-exported exception classes | Stable | `BackendError`, `BackendConnectionError`, `QueueError`, `StorageError`, `SerializationError`, and `ConfigurationError`; documented context attributes are part of each concrete exception's contract. |
| Root-exported concrete backend, mode, and backend-settings classes | Inherit backend tier | Stable except the Memcached classes, which are Experimental with that backend. |
| Root-exported membership filters, `DedupeStrategy`, and `build_membership_filter()` | Inherit strategy tier | See the strategy table below. |
| Established component/backend selection settings | Stable | Includes `SCRAPY_BACKEND_TYPE`, per-component backend type/settings pairs, and dedup/queue/storage strategy selectors. Fresh settings and hooks are listed separately below. |
| `ConfigurationError.setting_name` / `.setting_value` attributes | Stable | Operators use these for structured config diagnostics; a rename would break downstream log handlers. The sensitive-name redaction heuristic (password/secret/api_key/apikey/token/credential → `***REDACTED***`) is part of the contract. |
| `StorageError(BackendError)` exception | Stable | Round-14 R14-A. Storage ops raise this instead of returning a silent sentinel; `except BackendError` catches every storage-path failure uniformly. `operation` + `key` kwargs are part of the contract. |

### Strategy tiers

| Strategy | Tier | Notes |
|---|---|---|
| Dedup `set` | Stable | Byte-identical to pre-strategy behavior; backed by `SetBackend`. |
| Dedup `memory` | Stable | LRU eviction; `SCRAPY_DEDUP_MEMORY_MAXSIZE` default 1,000,000 (round-9 U5). |
| Dedup `bloom` | Stable | Pure-stdlib; configurable FP rate; never false-negatives. |
| Dedup `cuckoo` | Stable | Pure-stdlib; raises `FilterFull` at capacity → degrades to passthrough + warn-once. |
| Queue `passthrough` | Stable | Default; delegates to `QueueBackend`. |
| Queue `delay` | Experimental | In-process `heapq`; hard crashes lose unsnapshotted state. Clean-close snapshot requires the queue backend itself to support storage and a unique owner in multi-worker deployments. |
| Queue `round_robin` | Experimental | Fully in-process per-worker queues/fairness cursor; same snapshot capability limits as `delay`; bypasses an MQ broker when paired with one. |
| Queue `throttle` | Experimental | In-process rate limiter; effective rate scales with worker count. |
| Queue `priority` | Experimental | Backend-side physical buckets; rejected with Kafka/RocketMQ because their consumers cannot isolate physical-topic scans. |
| Queue `time_wheel` | Experimental | In-process timing wheel + overflow heap; same snapshot capability limits as `delay`. |
| Queue `work_stealing` | Experimental | Backend-side worker queues; requires stable worker IDs and an explicit peer list; rejected with Kafka/RocketMQ. |
| Queue `ring_buffer` | Experimental | Bounded fully in-process storage; bypasses an MQ broker; hard crashes lose unsnapshotted items. |
| Storage `passthrough` | Stable | Default; writes directly to `StorageBackend`. |
| Storage `batched` | Experimental | In-process buffer; hard crashes can lose an unflushed batch. |

### Fresh hooks / settings (may evolve in a minor bump)

| Surface | Tier | Why |
|---|---|---|
| `Monitor.on_filter_full()` + `FilterFull` exception path | Experimental | Round-7; want flexibility on the degrade contract. |
| `backpressure_pause_at` / `backpressure_resume_at` | Experimental | Round-4; fresh hysteresis semantics. |
| `SCRAPY_QUEUE_SNAPSHOT_OWNER` and snapshot-key v2 | Experimental | New worker-isolation and consumed-snapshot semantics; key format may evolve before 1.0. |
| `SCRAPY_REDIS_NAMESPACE` physical-key layout | Stable | Persistent data boundary. Future changes require explicit migration guidance. |
| `SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL` | Stable | Destructive-operation safety gate; default remains false. |
| SQS/RocketMQ visibility/invisibility settings | Stable | Finite non-renewing lease semantics are operationally significant. |
| `SCRAPY_RETRY_ATTEMPTS` / `SCRAPY_RETRY_DELAY` | Stable | Initial attempt plus bounded retries with full-jitter exponential backoff. |
| `scrapy_extension.backends.registry.BackendDescriptor` entry-point registration | Experimental | Public fully qualified import; no broad 3rd-party ecosystem yet. |

### Internal (no stability promise)

Everything `_`-prefixed, including `_RedactedStr`, `_json_default`,
`_validate_key_name`, `_hash_item`, `_get_mode_text`, and backend `_connect_*`
methods, is Internal. Unlisted helpers that are not exported by their owning
namespace are also Internal. These may be renamed, moved, or removed in any
release.

## Backend maturity tiers

| Backend | Queue | Set | Storage | Tier | Notes |
|---|---|---|---|---|---|
| Redis | Yes | Yes | Yes | Stable — full | Namespaced queue/set/storage physical domains; atomic Lua queue operations; 4 modes. Legacy unnamespaced keys require explicit migration. |
| MongoDB | Yes | Yes | Yes | Stable — full | TLS/auth; 4 modes (standalone, replica_set, sharded_cluster, atlas). |
| ElasticSearch | Yes | Yes | Yes | Stable — full | 2 modes (standalone, cloud). |
| Kafka | Yes | No | No | Stable — queue-only | SASL/SSL; concurrent-safe per-message topic/partition/offset tokens. `priority`/`work_stealing` strategies are rejected. |
| RabbitMQ | Yes | No | No | Stable — queue-only | Priority queues; per-message channel-generation tokens; mandatory synchronous publisher confirms. HA policy remains operator-managed. |
| Pulsar | Yes | No | No | Stable — queue-only | Topic-bound concurrent ack tokens. Queue depth and purge require an admin API and are unsupported here. |
| SQS | Yes | No | No | Stable — queue-only | Standard queues; LocalStack + AWS; per-message receipt tokens; approximate depth; finite visibility lease without auto-renewal. |
| RocketMQ | Yes | Guard | Guard | Stable — queue-only | gRPC proxy (`--enable-proxy`, port 8081); per-message deferred ack; finite invisibility lease; queue depth unsupported; `priority`/`work_stealing` rejected. |
| DynamoDB | No | No | Yes | Stable — storage-only | KV+TTL; LocalStack + AWS. |
| Memcached | No | No | Yes | **Experimental** | KV+TTL (storage-only); TTL introspection and prefix clear unsupported; server-wide clear requires explicit `allow_flush_all`. **Supply-chain caveat:** the supported `pymemcache>=4,<5` range currently resolves to the unmaintained 4.0.0 release. |

A "Guard" Set/Storage column means configuring that backend for the set/storage
component is rejected at config time (`ConfigurationError`) — the backend has
no native set/KV semantics. Pair it with a full backend (Redis, MongoDB,
ElasticSearch, Memcached, or DynamoDB) for dedup/storage.

## Round-9 hardening (this release)

The round-9 arc tightened the contract in three places that operators should
know about:

- **SV1–SV5 — config-time validation.** ~32 settings footguns closed across
  `settings/*.py`: `Literal` enum types (SV1), mode-conditional
  `model_validator`s (SV2), cross-field auth/transport coherence (SV3 —
  security cluster, 3 high-severity credential bugs), URL/scheme format
  guards (SV4), and empty-string + unbounded-int gaps (SV5). Project
  cross-field/capability/unknown-name checks raise `ConfigurationError`;
  Pydantic field type/range/enum failures raise `ValidationError`. See the
  [settings-validation spec](https://github.com/azwpayne/scrapy-extension/blob/main/docs/insight/SPEC-round8-settings-validation.md).
- **U4 — `queue_len` depth sampling.** `BackendQueue(depth_sample_every=100)`
  probes real backend depth at most once per 100 pops, reclaiming ~25% of
  the pop-path RTT budget at default config (`queue/queue.py`). Backpressure
  gates use the sampled depth when the backend exposes one; set
  `depth_sample_every=1` to restore per-pop behavior. Pulsar and RocketMQ have
  no configured depth API, so their scheduler degrades conservatively to
  continued polling without depth-based backpressure.
- **U5 — memory default cap.** `MemoryMembershipFilter(maxsize=1_000_000)`
  default + `DelayQueueStrategy(max_held=100_000)` soft-cap warn-once
  prevent silent OOM on long high-cardinality crawls. Explicit
  `maxsize=None` / non-positive `max_held` remain as advanced opt-outs.

## Round-9/14 hardening (breaking)

The round-9c SV3 security cluster + the round-14 R14-A/R14-B/R14-F units
introduced **breaking** config-time validators. Each item is security- or
correctness-motivated; none are revertible without re-opening the footgun.

- **Pulsar `auth_token` requires `pulsar+ssl://`** (round-9c SV3-2). Token
  over plaintext leaks on the wire.
- **Redis `ssl_enabled=True` requires `ssl_cafile`** (round-9c SV3-3). TLS
  without a pinned CA is MITM-vulnerable.
- **`BackendQueue.push` pops `delay`/`source` from `request.meta`**
  (round-14 R14-F). Re-pushes no longer re-delay / re-source.
- **Memcached / DynamoDB / MongoDB storage ops RAISE `StorageError`**
  instead of returning a silent sentinel (round-14 R14-A). Fixes a
  data-loss contract bug — `store()` returning `None` was treated as success.
- **`SCRAPY_BACKEND_TYPE` raises `ConfigurationError`** (not pydantic
  `ValidationError`) for unknown values (round-14 R14-B). Additive:
  registered 3rd-party backend strings are now ACCEPTED at the Settings
  layer (restores round-5 R5-1).
- **Redis physical keys are namespaced and domain-separated.** Current code does
  not read legacy raw queue/set/storage keys. Persistent deployments must drain
  or explicitly migrate them before switching versions.
- **Storage TTL is uniform.** Direct storage calls accept only `None` or a
  positive integer; `ttl()` normalizes missing/permanent/expired states to
  `None` rather than backend-native negative/zero sentinels.
- **Memcached global clear is opt-in.** `clear_storage(None)` now raises unless
  `SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=True`; prefix clear remains unsupported.
- **Unknown bundled backend settings fail fast.** Nested extras and typoed flat
  prefix names no longer disappear into defaults.
- **Unsupported depth is explicit.** Pulsar/RocketMQ `queue_len()` now raises
  `NotImplementedError` instead of reporting a false zero; scheduler pending
  detection remains conservative.
- **Kafka/RocketMQ reject fanout queue strategies** (`priority` and
  `work_stealing`) instead of running with incorrect consumer/topic isolation.
- **Deterministic poison deliveries are terminally consumed.** Malformed queued
  payloads with an ack token are acked/dropped, surfaced as
  `SerializationError`, and counted rather than redelivered forever.

See the **Breaking** section of [`CHANGELOG.md`](CHANGELOG.md) `[Unreleased]`
and the [migration guide](docs/migration-guide.md) for operator steps.

For the full change history, see [`CHANGELOG.md`](CHANGELOG.md).
