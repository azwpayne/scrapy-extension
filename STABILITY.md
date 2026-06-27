# Stability

This document describes the stability tiers of `scrapy-extension`'s public
surface, so downstream users can decide what is safe to depend on.

The tiers follow [Semantic Versioning 2.0.0](https://semver.org/):
- **Stable** — frozen public API. Breaking changes are gated behind a major
  version bump.
- **Experimental** — usable in production, but the surface (signature, setting
  name, or semantics) may change in a minor (`0.x`) bump. Field-tested but
  young.
- **Internal** — `_`-prefixed symbols and anything not listed in
  [`__init__.py`](src/scrapy_extension/__init__.py)'s `__all__`. No stability
  promise; do not import these directly.

## Components and tiers

| Surface | Tier | Notes |
|---|---|---|
| `BackendScheduler` (`schedule/scheduler.py`) | Stable | `from_settings` / `from_crawler` factory; backpressure gates (round-4) are separately flagged Experimental below. |
| `BackendDupeFilter` (`dupefilter/dupefilter.py`) | Stable | Strategy selection via `SCRAPY_DEDUP_STRATEGY` is Stable; per-strategy tiers in the next table. |
| `BackendPipeline` (`pipeline/pipeline.py`) | Stable | |
| `BackendQueue` (`queue/queue.py`) | Stable | `depth_sample_every` (round-9 U4) is Stable with a safe default. |
| `BackendSpiderMixin` (`spider/spider_mixin.py`) | Stable | |
| `Backend` / `QueueBackend` / `SetBackend` / `StorageBackend` ABCs | Stable | The abstract contract 3rd-party backends implement. |
| `ConnectionManager` (`backends/connectors.py`) | Stable | Lazy-singleton registry keyed by `backend_type:settings_hash`. |
| `resolve_backend_config()` | Stable | Used by all three component factories for multi-backend coexistence. |
| `Monitor` ABC + `ScrapyStatsMonitor` (`monitor/`) | Stable | The hook *set* is additive; see Experimental for fresh hooks. |
| All `SCRAPY_*` settings shipped in `settings/base.py` | Stable | Including `SCRAPY_BACKEND_TYPE`, `SCRAPY_{QUEUE,SET,STORAGE}_BACKEND_TYPE`, `SCRAPY_DEDUP_STRATEGY`, `SCRAPY_QUEUE_STRATEGY`. |
| `ConfigurationError.setting_name` / `.setting_value` attributes | Stable | Round-14 R14-B freeze. Operators catch + log these (README:386 names them in prose); a rename would silently break downstream log handlers. The `_SENSITIVE_NAME_FRAGMENTS` redaction heuristic (password/secret/api_key/apikey/token/credential → `***REDACTED***`) is part of the contract — never log raw `setting_value` for sensitive names. |
| `StorageError(BackendError)` exception | Stable | Round-14 R14-A. Storage ops raise this instead of returning a silent sentinel; `except BackendError` catches every storage-path failure uniformly. `operation` + `key` kwargs are part of the contract. |

### Strategy tiers

| Strategy | Tier | Notes |
|---|---|---|
| Dedup `set` | Stable | Byte-identical to pre-strategy behavior; backed by `SetBackend`. |
| Dedup `memory` | Stable | LRU eviction; `SCRAPY_DEDUP_MEMORY_MAXSIZE` default 1,000,000 (round-9 U5). |
| Dedup `bloom` | Stable | Pure-stdlib; configurable FP rate; never false-negatives. |
| Dedup `cuckoo` | Stable | Pure-stdlib; raises `FilterFull` at capacity → degrades to passthrough + warn-once. |
| Queue `passthrough` | Stable | Default; delegates to `QueueBackend`. |
| Queue `delay` | Experimental | In-process `heapq`; lost on crash. Soft-cap warn (`max_held`). Distributed-delay is a post-1.0 roadmap item. |
| Queue `round_robin` | Experimental | In-process per-worker index. |
| Queue `throttle` | Experimental | In-process rate limiter; effective rate scales with worker count. |

### Fresh hooks / settings (may evolve in a minor bump)

| Surface | Tier | Why |
|---|---|---|
| `Monitor.on_filter_full()` + `FilterFull` exception path | Experimental | Round-7; want flexibility on the degrade contract. |
| `backpressure_pause_at` / `backpressure_resume_at` | Experimental | Round-4; fresh hysteresis semantics. |
| `BackendDescriptor` entry-point registration | Experimental | Round-5; no 3rd-party ecosystem yet to validate the contract. |

### Internal (no stability promise)

Everything `_`-prefixed: `_RedactedStr`, `_filter_full_warned`,
`_json_default`, `_validate_key_name`, `_hash_item`, `_get_mode_text`, all
`_connect_*` methods on backends, and any helper not re-exported from
`src/scrapy_extension/__init__.py`. These may be renamed, moved, or removed
in any release.

## Backend maturity tiers

| Backend | Queue | Set | Storage | Tier | Notes |
|---|---|---|---|---|---|
| Redis | Yes | Yes | Yes | Stable — full | ZADD/ZPOPMIN queue, SADD sets, KV+TTL; 4 modes. |
| MongoDB | Yes | Yes | Yes | Stable — full | TLS/auth; 4 modes (standalone, replica_set, sharded_cluster, atlas). |
| ElasticSearch | Yes | Yes | Yes | Stable — full | 2 modes (standalone, cloud). |
| Kafka | Yes | No | No | Stable — queue-only | SASL/SSL; ack under `CONCURRENT_REQUESTS > 1` via per-message token. |
| RabbitMQ | Yes | No | No | Stable — queue-only | Priority queues (`x-max-priority`); HA policy; per-message ack. |
| Pulsar | Yes | No | No | Stable — queue-only | Shared subscription; single-slot ack (`supports_concurrent_ack=False`). |
| SQS | Yes | No | No | Stable — queue-only | Standard queues; LocalStack + AWS; single-slot ack. |
| RocketMQ | Yes | Stub | Stub | Experimental | Queue functional; Set/Storage raise `NotImplementedError`. **Supply-chain caveat:** depends on `rocketmq-client-python` (round-7 accepted unmaintained dep). |
| DynamoDB | No | No | Yes | Stable — storage-only | KV+TTL; LocalStack + AWS. |
| Memcached | No | No | Yes | **Experimental** | KV+TTL (storage-only). **Supply-chain caveat:** depends on `pymemcache==4.0.0` — unmaintained (last release 2022-10-17, ~1300+ days stale at the time of writing); tracked as U20. The label is applied to `pyproject.toml` separately; this document records the status. |

A "Stub" Set/Storage column means the method signatures exist but raise
`NotImplementedError` at runtime — pair RocketMQ with a full backend for
dedup/storage.

## Round-9 hardening (this release)

The round-9 arc tightened the contract in three places that operators should
know about:

- **SV1–SV5 — config-time validation.** ~32 settings footguns closed across
  `settings/*.py`: `Literal` enum types (SV1), mode-conditional
  `model_validator`s (SV2), cross-field auth/transport coherence (SV3 —
  security cluster, 3 high-severity credential bugs), URL/scheme format
  guards (SV4), and empty-string + unbounded-int gaps (SV5). Invalid values
  now raise `ConfigurationError` at startup. See
  [`docs/insight/SPEC-round8-settings-validation.md`](docs/insight/SPEC-round8-settings-validation.md).
- **U4 — `queue_len` depth sampling.** `BackendQueue(depth_sample_every=100)`
  probes real backend depth at most once per 100 pops, reclaiming ~25% of
  the pop-path RTT budget at default config (`queue/queue.py`). Backpressure
  gates still trip at the right depth; set `depth_sample_every=1` to restore
  per-pop behavior.
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

See the **Breaking** section of [`CHANGELOG.md`](CHANGELOG.md) `[Unreleased]`
for migration guidance.

For the full change history, see [`CHANGELOG.md`](CHANGELOG.md).
