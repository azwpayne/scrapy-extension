# Scrapy Extension

Distributed crawling for Scrapy with pluggable backends (**Redis**, **MongoDB**, **Kafka**, **RabbitMQ**, **ElasticSearch**, **RocketMQ**, **Pulsar**, **SQS**, **Memcached**, **DynamoDB**) and pluggable strategy layers for dedup and queue semantics.

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://pypi.org/project/scrapy-extension/)

## Contents

- [Features](#features) · [Installation](#installation) · [Quick Start](#quick-start)
- [Backend Configuration](#backend-configuration) · [Backend Capabilities](#backend-capabilities)
- [Guarantees](#guarantees) · [Multi-Backend Coexistence](#multi-backend-coexistence)
- [Pluggable Strategy Layers](#pluggable-strategy-layers) — incl. the [Ack & durability matrix](#ack-and-durability-matrix)
- [Architecture](#architecture) · [Scrapy Components](#scrapy-components) · [Exceptions](#exceptions)
- [Examples](#examples) · [Security](#security) · [Testing](#testing) · [License](#license)

> **Deeper docs:** operations → [`docs/runbook.md`](docs/runbook.md) · plugin authors → [`docs/backend-plugins.md`](docs/backend-plugins.md) · API/maturity → [`STABILITY.md`](STABILITY.md) · runnable spiders → [`examples/README.md`](examples/README.md)

## Features

- **10 Backends**: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ, Pulsar, SQS, Memcached, DynamoDB
- **Multi-Mode**: Standalone, cluster, cloud deployments per backend
- **Pluggable Dedup**: Set / Memory / **Bloom** / **Cuckoo** filters via `SCRAPY_DEDUP_STRATEGY`
- **Pluggable Queue Semantics**: 8 strategies — Passthrough / **Delay** / **RoundRobin** / **Throttle** / **Priority** / **TimeWheel** / **WorkStealing** / **RingBuffer** via `SCRAPY_QUEUE_STRATEGY`
- **Multi-Backend Coexistence**: bind queue / dedup / storage to *different* backends via `SCRAPY_{QUEUE,SET,STORAGE}_BACKEND_TYPE` (e.g. queue in Redis, dedup + data in MongoDB)
- **Distributed Queue**: Priority-based request queue across spiders
- **Duplicate Filtering**: Cross-instance URL deduplication (default: `SetBackend`)
- **Item Storage**: Key-value storage with TTL support via `StorageBackend`
- **Type Safe**: Full type annotations, `py.typed` marker
- **Secure**: Input validation on key names, topic names, and queue identifiers

## Installation

```bash
pip install scrapy-extension                  # Core (no backend deps required)
pip install scrapy-extension[redis]           # Redis backend
pip install scrapy-extension[mongodb]         # MongoDB backend (pymongo)
pip install scrapy-extension[kafka]           # Kafka backend (kafka-python)
pip install scrapy-extension[rabbitmq]        # RabbitMQ backend (pika)
pip install scrapy-extension[elasticsearch]   # ElasticSearch backend
pip install scrapy-extension[rocketmq]        # RocketMQ backend
pip install scrapy-extension[pulsar]          # Pulsar backend (pulsar-client)
pip install scrapy-extension[sqs]             # Amazon SQS backend (boto3)
pip install scrapy-extension[memcached]       # Memcached backend (pymemcache)
pip install scrapy-extension[dynamodb]        # DynamoDB backend (boto3)
pip install scrapy-extension[all]             # All backends
```

Backends are loaded lazily via PEP 562 — the core package works without any backend deps installed. Backend-specific dependencies are only loaded when a backend class is first accessed.

## Quick Start

```python
import scrapy
from scrapy_extension import BackendSpiderMixin


class MySpider(BackendSpiderMixin, scrapy.Spider):
    name = "example"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setup_backend()

    def parse(self, response):
        yield {"url": response.url}
```

`settings.py`:

```python
SCHEDULER = "scrapy_extension.schedule.scheduler.BackendScheduler"
DUPEFILTER_CLASS = "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"
ITEM_PIPELINES = {"scrapy_extension.pipeline.pipeline.BackendPipeline": 300}

SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_HOST = "localhost"
```

## Backend Configuration

All settings use `SCRAPY_<BACKEND>_<KEY>` env vars via pydantic-settings.

### Redis (standalone, master_slave, sentinel, cluster)

```python
SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_HOST = "localhost"
SCRAPY_REDIS_PORT = 6379
```

### MongoDB (standalone, replica_set, sharded_cluster, atlas)

```python
SCRAPY_BACKEND_TYPE = "mongodb"
SCRAPY_MONGO_URI = "mongodb://localhost:27017"
SCRAPY_MONGO_DATABASE = "scrapy"
```

### Kafka (standalone, cluster, confluent)

```python
SCRAPY_BACKEND_TYPE = "kafka"
SCRAPY_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
```

### RabbitMQ (standalone, cluster, mirrored_queues)

```python
SCRAPY_BACKEND_TYPE = "rabbitmq"
SCRAPY_RABBITMQ_HOST = "localhost"
SCRAPY_RABBITMQ_PORT = 5672
```

### ElasticSearch (standalone, cloud)

```python
SCRAPY_BACKEND_TYPE = "elasticsearch"
SCRAPY_ELASTICSEARCH_HOSTS = ["http://localhost:9200"]
```

### RocketMQ (standalone, cluster, cloud)

```python
SCRAPY_BACKEND_TYPE = "rocketmq"
# Point at the broker's gRPC PROXY (port 8081), not the legacy NameServer.
# The broker must run with --enable-proxy (apache rocketmq-python-client 5.1.1).
SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "localhost:8081"
```

<details><summary><b>Topic creation — required setup</b> (the first push otherwise fails)</summary>

The apache rocketmq 5.x gRPC proxy does **not** auto-create topics via the
`QueryRoute` path by default — a first push to a fresh topic fails with
`failed to fetch topic route`. `broker.conf`'s `autoCreateTopicEnable=true`
covers only the legacy remoting path, not the gRPC path the python client
speaks. Pick **one** of:

**Option A — enable proxy-side auto-create (recommended for dev):**

Mount an `rmq-proxy.json` and point the broker at it via `-pc`:
```json
{
  "rocketMQClusterName": "DefaultCluster",
  "enableAutoTopicCreation": true,
  "topicQueueConfig": { "defaultReadQueueNum": 8, "defaultWriteQueueNum": 8 }
}
```
```bash
sh mqbroker -n namesrv:9876 -c /path/broker.conf --enable-proxy -pc /path/rmq-proxy.json
```

**Option B — pre-create topics (production / locked-down brokers):**
```bash
mqadmin updateTopic -n namesrv:9876 -b broker:10911 -t scrapy-queue_<your-queue>
```

Also set `brokerIP1` in `broker.conf` to an address your client can resolve
(`127.0.0.1` for single-host docker; the broker's real IP for remote clients)
— the proxy returns this to clients, and the default container hostname is
usually unreachable from the host.

</details>

> **At-least-once delivery:** RocketMQ uses a deferred-ack model — `pop`
> returns a message body **without** acking; the scheduler acks via
> `ack(token=msg)` after the request completes. A crash before ack → the
> broker's invisible-duration window redelivers (at-least-once, not exactly-once).

### Pulsar (standalone, cluster)

```python
SCRAPY_BACKEND_TYPE = "pulsar"
SCRAPY_PULSAR_SERVICE_URL = "pulsar://localhost:6655"
```

### Amazon SQS (standalone=LocalStack, cloud=AWS)

```python
SCRAPY_BACKEND_TYPE = "sqs"
SCRAPY_SQS_REGION_NAME = "us-east-1"
SCRAPY_SQS_ENDPOINT_URL = "http://localhost:4566"  # LocalStack
```

### Memcached (standalone, NoSQL KV)

```python
SCRAPY_BACKEND_TYPE = "memcached"
SCRAPY_MEMCACHED_HOST = "localhost"
SCRAPY_MEMCACHED_PORT = 11211
```

### DynamoDB (standalone=LocalStack, cloud=AWS, NoSQL KV)

```python
SCRAPY_BACKEND_TYPE = "dynamodb"
SCRAPY_DYNAMODB_TABLE_NAME = "scrapy-extension"
SCRAPY_DYNAMODB_REGION_NAME = "us-east-1"
SCRAPY_DYNAMODB_ENDPOINT_URL = "http://localhost:4566"  # LocalStack
```

See [`examples/`](examples) for representative runnable spiders and deployment-mode recipes (Sentinel, Cluster, Atlas, Confluent, etc).

## Backend Capabilities

| Backend       | Queue | Set | Storage | Modes                                        |
|---------------|-------|-----|---------|----------------------------------------------|
| Redis         | Yes   | Yes | Yes     | standalone, master_slave, sentinel, cluster  |
| MongoDB       | Yes   | Yes | Yes     | standalone, replica_set, sharded_cluster, atlas |
| ElasticSearch | Yes   | Yes | Yes     | standalone, cloud                            |
| Kafka         | Yes   | No  | No      | standalone, cluster, confluent               |
| RabbitMQ      | Yes   | No  | No      | standalone, cluster, mirrored_queues         |
| RocketMQ      | Yes   | Guard | Guard  | standalone, cluster, cloud                   |
| Pulsar        | Yes   | No  | No      | standalone, cluster                          |
| SQS           | Yes   | No  | No      | standalone (LocalStack), cloud (AWS)         |
| Memcached     | No    | No  | Yes     | standalone                                   |
| DynamoDB      | No    | No  | Yes     | standalone (LocalStack), cloud (AWS)         |

- **Yes** — fully implemented
- **No** — not implemented (this backend doesn't expose the interface)
- **Guard** — rejected at config time (`ConfigurationError`); a guard class fails-fast if the capability gate is bypassed (RocketMQ set/storage)

**Kafka, RabbitMQ, Pulsar, SQS**: Queue-only. For deduplication and storage, use Redis, MongoDB, ElasticSearch, Memcached, or DynamoDB.

**RocketMQ**: Queue is functional. Set/Storage are rejected at config time (`ConfigurationError`) — pair with a full-featured backend (Redis, MongoDB, ElasticSearch, Memcached, or DynamoDB) for dedup/storage.

**Memcached, DynamoDB**: Storage-only (key-value with TTL). Pair with a queue-capable backend for request distribution.

## Guarantees

What the library contractually promises — and just as importantly, what it does **not**. Read this before relying on any feature in production. Every claim below is backed by code in `src/scrapy_extension/`; follow the linked file anchors to verify.

### Per-feature cross-worker behavior

| Layer | Strategy | Cross-worker safe? | Notes |
|---|---|---|---|
| Queue | `passthrough` (default) | Yes | Items live in the backend queue; atomic pop on every backend (`backends/base.py:359`). |
| Queue | `delay` | Per-process | In-process `heapq`; **survives clean restart** via snapshot/restore persisted to the storage backend on `close()` (initiatives #3 / #13) — **lost on hard crash** (no `close()` runs). Snapshots are **spider-scoped** — key `queue:snapshot:<spider.name>:<queue_name>` (initiative #16) — so two spiders sharing a storage backend with the same `queue_name` cannot clobber each other's snapshot. Soft-cap `max_held` warns once when exceeded (`queue/strategies/delay.py`). |
| Queue | `round_robin` | Per-process | Fair dispatch across `request.meta['source']` using a per-worker index. |
| Queue | `throttle` | Per-process | Effective rate under N workers = `N × (1 / min_interval)`. |
| Dedup | `set` (default) | Yes — exact | Backend `SADD`/`SISMEMBER` semantics; byte-identical to pre-strategy behavior (`dupefilter/filters/set_filter.py`). |
| Dedup | `memory` | Per-process | In-process; optional LRU cap via `SCRAPY_DEDUP_MEMORY_MAXSIZE` (default 1,000,000; round-9 U5). |
| Dedup | `bloom` | Per-process | Pure-stdlib bit-vector; **never produces false negatives** (a seen URL is always reported seen); false-positive rate is configurable. |
| Dedup | `cuckoo` | Per-process | Pure-stdlib; **never produces false negatives**; supports deletion; raises `FilterFull` at capacity (degrades to passthrough + warn-once). |
| Storage | all storage-capable backends | Yes | Via backend KV+TTL (`backends/base.py:525`). |

**Defaults are distributed-exact.** `set` dedup + `passthrough` queue are safe for multi-worker crawls out of the box. `delay` / `throttle` / `round_robin` / `memory` / `bloom` / `cuckoo` are **per-process opt-in** — safe for single-worker politeness/dedup; for multi-worker politeness or shared probabilistic dedup, run one process per backend or wait for the distributed-strategies roadmap.

### Contractual promises

| Promise | Where enforced |
|---|---|
| **Config-time validation.** Invalid settings (bad mode, bad scheme, half-configured AWS creds, insecure-TLS-in-prod, negative backpressure thresholds, unknown backend type) raise `ConfigurationError` at startup, not an opaque runtime stack trace. `ConfigurationError.setting_name` / `.setting_value` are frozen Stable attributes (round-14 R14-B) — downstream log handlers can rely on them. Sensitive names (`password`/`secret`/`api_key`/`token`/`credential`) are auto-redacted to `***REDACTED***`. | `settings/base.py`, `settings/{kafka,pulsar,redis,mongodb,elasticsearch,sqs,dynamodb,rocketmq}.py` (round-6 SEC-1..7 + round-9 SV1..SV5 + round-14 R14-B); see [`STABILITY.md`](STABILITY.md) for the frozen attribute contract |
| **Credentials are never logged.** Passwords / SASL tokens / API keys flow through `_RedactedStr`, whose `__repr__` / `__str__` return `***` rather than the raw value. | `backends/_redaction.py:22`, wired into Kafka/RabbitMQ config builders |
| **No code execution on the data path.** Serialization is JSON only — never `pickle`, never `eval`. Unknown types raise `TypeError` instead of being silently `str()`-ed. | `backends/base.py:34` (`_json_default`), `backends/base.py:131` (`JSONSerializer`) |
| **Input names are validated.** Queue / set / index / topic names match `^[a-zA-Z0-9._:-]+$` (topic names a stricter subset); injection-shaped inputs are rejected before use. | `backends/base.py:170` (`KEY_NAME_PATTERN`, `_validate_key_name`) |
| **Ack correctness under `CONCURRENT_REQUESTS > 1`.** Message-queue backends (Kafka, RabbitMQ) carry a per-message ack token so the *specific* popped message is acked; the scheduler's `from_settings` gate refuses unsafe configs unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS` is set. | `backends/base.py:313` (`QueueBackend` ack contract), `schedule/scheduler.py` |
| **Lazy optional deps.** `pip install scrapy-extension` works with **zero** backend deps. Each backend's optional dep loads on first access via PEP 562, with `ImportError` install hints. | `__init__.py`, `backends/__init__.py`, every `backends/*.py` |
| **Probabilistic dedup never false-negatives.** Bloom and Cuckoo may produce false positives (a fresh URL reported as "seen"); they will never let a seen URL through as fresh. | `dupefilter/filters/bloom_filter.py`, `dupefilter/filters/cuckoo_filter.py` |
| **Backend capability honesty.** A backend never silently no-ops on an unsupported interface: queue-only backends omit `SetBackend`/`StorageBackend` entirely; RocketMQ set/storage are rejected at config time (`ConfigurationError` guard). The matrix above is the contract. | `backends/base.py` ABCs; `backends/connectors.py` capability gates |
| **`py.typed` marker shipped.** Full type annotations on the public surface; downstream type-checkers consume the shipped typing. | `src/scrapy_extension/py.typed` |

### What is **not** promised

- **Cross-worker behavior of `delay` / `throttle` / `round_robin` / `memory` / `bloom` / `cuckoo` strategies** — they are per-process by design (see table above).
- **Stability of the entry-point registration API** (`BackendDescriptor`) — round-5 surface, no 3rd-party ecosystem yet; expect possible minor-bump changes. See [`STABILITY.md`](STABILITY.md).
- **Stability of fresh hooks** — `on_filter_full` (round-7) and `backpressure_pause_at` / `backpressure_resume_at` (round-4) are new; the hook signatures and setting semantics may evolve in a minor bump.
- **Wire compatibility for the SQS / Memcached / DynamoDB LocalStack paths** — exercised via LocalStack in CI; not certified against every AWS region or Memcached server version.

For the full stability/maturity tiering per backend, see [`STABILITY.md`](STABILITY.md). To report a security issue, see [`SECURITY.md`](SECURITY.md). For what changed in each release, see [`CHANGELOG.md`](CHANGELOG.md).

## Multi-Backend Coexistence

The three components — **Scheduler** (queue), **DupeFilter** (set), **Pipeline** (storage) — can each bind to a *different* backend. This unlocks hybrid topologies that play to each backend's strengths: a high-throughput queue in Kafka, exact cross-worker dedup in Redis, durable item storage in MongoDB.

| Component | Per-component keys | Fallback (backward compat) |
|-----------|-------------------|------|
| Scheduler (queue) | `SCRAPY_QUEUE_BACKEND_TYPE` / `SCRAPY_QUEUE_BACKEND_SETTINGS` | `SCRAPY_BACKEND_TYPE` / `SCRAPY_BACKEND_SETTINGS` |
| DupeFilter (set) | `SCRAPY_SET_BACKEND_TYPE` / `SCRAPY_SET_BACKEND_SETTINGS` | `SCRAPY_BACKEND_TYPE` / `SCRAPY_BACKEND_SETTINGS` |
| Pipeline (storage) | `SCRAPY_STORAGE_BACKEND_TYPE` / `SCRAPY_STORAGE_BACKEND_SETTINGS` | `SCRAPY_BACKEND_TYPE` / `SCRAPY_BACKEND_SETTINGS` |

When a per-component type key is set, that component uses its per-component settings; otherwise it falls back to the global keys — so existing single-backend configurations keep working unchanged.

**Example: queue in Redis-Cluster, dedup fingerprints + scraped data in MongoDB**

```python
# settings.py
SCHEDULER = "scrapy_extension.schedule.scheduler.BackendScheduler"
DUPEFILTER_CLASS = "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"
ITEM_PIPELINES = {"scrapy_extension.pipeline.pipeline.BackendPipeline": 300}

# Queue: Redis-Cluster
SCRAPY_QUEUE_BACKEND_TYPE = "redis"
SCRAPY_QUEUE_BACKEND_SETTINGS = {"mode": "cluster", "startup_nodes": [...]}

# Dedup fingerprints: MongoDB
SCRAPY_SET_BACKEND_TYPE = "mongodb"
SCRAPY_SET_BACKEND_SETTINGS = {"uri": "mongodb://mongo:27017", "database": "scrapy"}

# Scraped data: MongoDB (same cluster, separate connection manager entry)
SCRAPY_STORAGE_BACKEND_TYPE = "mongodb"
SCRAPY_STORAGE_BACKEND_SETTINGS = {"uri": "mongodb://mongo:27017", "database": "scrapy"}
```

> **Constraint**: each backend must implement the interface its component needs — queue backends must implement `QueueBackend`, dedup backends `SetBackend`, storage backends `StorageBackend` (see the [capabilities matrix](#backend-capabilities)). The `ConnectionManager` registry keys one pooled connection per `backend_type:settings_hash`, so co-located backends (e.g. set + storage both MongoDB, same URI) share a single connection.

## Pluggable Strategy Layers

Three strategy layers sit above the backend interfaces, selected via Scrapy settings — no code change required. Defaults preserve prior behavior exactly.

### Dedup strategy — `SCRAPY_DEDUP_STRATEGY`

`BackendDupeFilter` delegates to a `MembershipFilter`:

| Strategy | Exact? | Cross-worker? | Delete? | Notes |
|----------|--------|---------------|---------|-------|
| `set` (default) | yes | yes | yes | `SetBackend`-backed; byte-identical to prior behavior |
| `memory` | yes | no | yes | in-process, optional LRU cap (`SCRAPY_DEDUP_MEMORY_MAXSIZE`) |
| `bloom` | no (FP) | no | no | pure-stdlib bit-vector; `SCRAPY_DEDUP_BLOOM_CAPACITY` / `_ERROR_RATE` |
| `cuckoo` | no (FP) | no | yes | pure-stdlib; `SCRAPY_DEDUP_CUCKOO_CAPACITY` / `_ERROR_RATE` |

Probabilistic filters never produce false negatives; in-memory filters are per-process (single-worker). Use `set` for multi-worker exact dedup.

### Queue semantics — `SCRAPY_QUEUE_STRATEGY`

`BackendQueue` delegates bytes-level push/pop to a `QueueStrategy` (task-queue types beyond queue/stack/priority):

| Strategy | Behavior |
|----------|----------|
| `passthrough` (default) | delegates to `QueueBackend` unchanged (prior behavior) |
| `delay` | holds items until `now + delay`; per-request via `request.meta['delay']` or `SCRAPY_QUEUE_DELAY_DEFAULT` |
| `round_robin` | fair dispatch across `request.meta['source']` (no starvation) |
| `throttle` | rate-limited pops (`SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL`); limiter state is per-process |
| `priority` | strategy-layer priority buckets for backends without native priority |
| `time_wheel` | hashed timing wheel for many short delays; in-process timing state |
| `work_stealing` | own queue first, then steal from peer queues when idle |
| `ring_buffer` | bounded in-process circular buffer with explicit overflow policy |

`delay`, `round_robin`, `throttle`, `time_wheel`, `work_stealing`, and `ring_buffer` keep some state in-process. `passthrough` is the distributed-exact default. See [Ack and durability matrix](#ack-and-durability-matrix) before using a stateful strategy in production.

### Storage strategy — `SCRAPY_STORAGE_STRATEGY`

`BackendPipeline` delegates item writes to a `StorageStrategy`:

| Strategy | Behavior | Durability note |
|----------|----------|-----------------|
| `passthrough` (default) | writes each serialized item directly to the selected `StorageBackend` | backend durability applies immediately after `store()` returns |
| `batched` | buffers `(key, value, ttl)` triples and flushes at threshold / spider close | improves throughput, but a hard crash before flush loses the in-process batch; store exceptions re-enqueue the unwritten tail |

Use `passthrough` when item loss is unacceptable. Use `batched` only when throughput is worth the crash-before-flush trade-off and duplicate writes are acceptable after partial flush retry.

### Ack and durability matrix

| Surface | Ack / state boundary | Crash behavior | Operational guidance |
|---|---|---|---|
| Redis / MongoDB / ElasticSearch / RocketMQ queue pop | atomic-pop or deferred-ack handled by the backend implementation; scheduler ack is inert or token-specific | item is removed/owned once popped; callback/pipeline crash after downloader response can still lose downstream item work | pair with idempotent callbacks/pipelines when end-to-end exactly-once matters |
| Kafka / RabbitMQ queue pop | per-message token stored in request meta and acked on Scrapy `response_received` | crash before ack redelivers; crash after downloader response but before callback/pipeline completion can drop downstream processing | understand this is download-level ack, not pipeline-completion ack |
| SQS / Pulsar queue pop | per-message ack token (`pop_with_ack`) tracked in a bounded in-flight set; acked on `response_received` | same download-level semantics as Kafka/RabbitMQ — crash before ack redelivers | safe under `CONCURRENT_REQUESTS > 1` (a real in-flight ack set, not a single slot) |
| Backend/plugin declaring `supports_concurrent_ack=False` | single ack slot only | `CONCURRENT_REQUESTS > 1` raises at startup unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=True` | keep `CONCURRENT_REQUESTS=1` for such backends, or choose one with a real in-flight ack set |
| Stateful queue strategies | in-process scheduling/fairness/rate/buffer state, with best-effort snapshot only where implemented | hard crash can lose held strategy state even if backend queue survives | prefer `passthrough` for strongest distributed durability |
| `batched` storage | in-process item buffer before backend `store()` | hard crash before flush loses buffered items; partial store exceptions retry the unwritten tail | prefer `passthrough` when persistence must happen before item acknowledgement |

Ack is tied to Scrapy downloader response delivery, not spider callback or item pipeline completion. If a crawl must tolerate process death after response download but before item persistence, make item processing idempotent and use a durable storage strategy/topology.


## Architecture

### Interface Hierarchy

```
Backend (ABC)
├── connect(), disconnect(), is_connected(), ping()
│
├── QueueBackend (ABC)
│   ├── push(queue_name, item, priority)
│   ├── pop(queue_name, timeout)
│   ├── queue_len(queue_name)
│   └── clear_queue(queue_name)
│
├── SetBackend (ABC)
│   ├── add(set_name, item)
│   ├── remove(set_name, item)
│   ├── contains(set_name, item)
│   ├── set_len(set_name)
│   └── clear_set(set_name)
│
└── StorageBackend (ABC)
    ├── store(key, data, ttl)
    ├── retrieve(key)
    ├── delete(key)
    ├── exists(key)
    ├── ttl(key)
    └── clear_storage(prefix)
```

All backends implement `Backend` plus the capability interfaces declared in the [Backend Capabilities](#backend-capabilities) matrix. Redis, MongoDB, and ElasticSearch implement all three capability interfaces; queue-only and storage-only backends are rejected at config time when selected for an unsupported component.

### Connection Management

```python
from scrapy_extension import ConnectionManager, BackendType

manager = ConnectionManager.get_manager(backend_type=BackendType.REDIS)
queue = manager.get_queue_backend()
queue.push("my_queue", b"item_data", priority=1.0)
```

`ConnectionManager` provides:
- **Lazy singleton**: thread-safe registry keyed by `backend_type:settings_hash`
- **Retry logic**: exponential backoff on connection failures
- **Lifecycle management**: automatic connect/disconnect

### Per-Spider Settings

```python
class MySpider(BackendSpiderMixin, scrapy.Spider):
    backend_type = BackendType.REDIS
    backend_settings = {"host": "localhost", "port": 6379}
```

## Scrapy Components

| Component    | Class                | Purpose                                          |
|--------------|----------------------|--------------------------------------------------|
| Scheduler    | `BackendScheduler`   | Distributes requests across spider instances      |
| DupeFilter   | `BackendDupeFilter`  | Filters duplicate requests using `SetBackend`     |
| Pipeline     | `BackendPipeline`    | Stores scraped items using `StorageBackend`       |
| Queue        | `BackendQueue`       | Serializes/deserializes Scrapy requests           |
| SpiderMixin  | `BackendSpiderMixin` | Convenient access to backend components           |

All components follow Scrapy's `from_settings()` / `from_crawler()` factory pattern.

### Scheduler

```python
SCHEDULER = "scrapy_extension.schedule.scheduler.BackendScheduler"
```

Integrates `BackendQueue` for request distribution and `BackendDupeFilter` for deduplication (when the backend supports sets). For Kafka/RabbitMQ, ack/nack is tied to Scrapy's `response_received` signal only; it does not wait for callback or item pipeline completion.

### DupeFilter

```python
DUPEFILTER_CLASS = "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"
```

Uses a `MembershipFilter` for duplicate detection (default: `SetBackend.add()`). Select the strategy via `SCRAPY_DEDUP_STRATEGY` (see [Pluggable Strategy Layers](#pluggable-strategy-layers)). Gracefully skips dedup for queue-only backends (Kafka, RabbitMQ, Pulsar, SQS).

### Pipeline

```python
ITEM_PIPELINES = {"scrapy_extension.pipeline.pipeline.BackendPipeline": 300}
SCRAPY_PIPELINE_KEY_PREFIX = "items"
SCRAPY_PIPELINE_TTL = 3600  # seconds, optional
```

Stores items as JSON with keys: `{prefix}:{spider_name}:{timestamp}:{uuid}`.

## Exceptions

```
BackendError (base)
├── BackendConnectionError   — connection failures (includes backend_type)
├── QueueError               — queue operation failures (includes queue_name, operation)
├── SerializationError       — serialization failures (includes data, serializer)
└── ConfigurationError       — invalid settings (includes setting_name, setting_value)
```

All exceptions carry context attributes for debugging.

## Examples

See [`examples/`](examples) — representative runnable spiders and recipes. Some shipped backends are documented as settings recipes rather than dedicated spiders:

| Spider | Backend | Features Demonstrated |
|--------|---------|----------------------|
| `quotes_redis` | Redis | Full queue + set + storage |
| `quotes_mongodb` | MongoDB | Document storage, replica set |
| `quotes_kafka` | Kafka | High-throughput streaming (queue only) |
| `quotes_rabbitmq` | RabbitMQ | Priority queues, HA (queue only) |
| `quotes_elasticsearch` | ElasticSearch | Full-text search storage |
| `quotes_multi_mode` | All | Multi-mode configurations |
| `quotes_connection_manager` | All | Direct `ConnectionManager` API |
| `quotes_programmatic` | Redis | Per-spider `backend_settings` dict |
| `quotes_crawl` | None | CrawlSpider variant |
| RocketMQ / Pulsar / SQS / Memcached / DynamoDB | recipes | Backend-specific settings; pair partial-capability backends with queue/set/storage-capable partners as needed |
| `quotes` | None | Basic Scrapy spider (no backend) |

## Security

- **Key name validation**: validated against `^[a-zA-Z0-9._:-]+$`
- **Topic name validation**: Kafka topics validated against `^[a-zA-Z0-9._-]+$`
- **Input sanitization**: all user-provided queue/set names validated before use
- **No code execution**: JSON serialization only — never pickle or eval

## Testing

```bash
# Run all tests
uv run pytest

# Run specific backend tests
uv run pytest tests/test_mongodb_backend.py

# Run with coverage report
uv run pytest --cov=scrapy_extension --cov-report=term-missing

# Run full matrix (all Python versions)
uv run poe test
```

Test infrastructure includes: pytest-xdist (parallel), pytest-randomly (randomized order), pytest-mock, pytest-cov (coverage with `fail_under = 95`), pytest-ruff (lint), pytest-socket (unit tests run with sockets disabled by default), and more. Live integration tests require explicit backend env vars plus `--force-enable-socket`.

## License

MIT — see [LICENSE](LICENSE).


