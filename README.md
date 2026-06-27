# Scrapy Extension

Distributed crawling for Scrapy with pluggable backends (**Redis**, **MongoDB**, **Kafka**, **RabbitMQ**, **ElasticSearch**, **RocketMQ**, **Pulsar**, **SQS**, **Memcached**, **DynamoDB**) and pluggable strategy layers for dedup and queue semantics.

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://pypi.org/project/scrapy-extension/)

## Features

- **10 Backends**: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ, Pulsar, SQS, Memcached, DynamoDB
- **Multi-Mode**: Standalone, cluster, cloud deployments per backend
- **Pluggable Dedup**: Set / Memory / **Bloom** / **Cuckoo** filters via `SCRAPY_DEDUP_STRATEGY`
- **Pluggable Queue Semantics**: Passthrough / **Delay** / **RoundRobin** / **Throttle** via `SCRAPY_QUEUE_STRATEGY`
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
SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "localhost:9876"
```

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

See [`examples/`](examples) for all deployment modes (Sentinel, Cluster, Atlas, Confluent, etc).

## Backend Capabilities

| Backend       | Queue | Set | Storage | Modes                                        |
|---------------|-------|-----|---------|----------------------------------------------|
| Redis         | Yes   | Yes | Yes     | standalone, master_slave, sentinel, cluster  |
| MongoDB       | Yes   | Yes | Yes     | standalone, replica_set, sharded_cluster, atlas |
| ElasticSearch | Yes   | Yes | Yes     | standalone, cloud                            |
| Kafka         | Yes   | No  | No      | standalone, cluster, confluent               |
| RabbitMQ      | Yes   | No  | No      | standalone, cluster, mirrored_queues         |
| RocketMQ      | Yes   | Stub | Stub   | standalone, cluster, cloud                   |
| Pulsar        | Yes   | No  | No      | standalone, cluster                          |
| SQS           | Yes   | No  | No      | standalone (LocalStack), cloud (AWS)         |
| Memcached     | No    | No  | Yes     | standalone                                   |
| DynamoDB      | No    | No  | Yes     | standalone (LocalStack), cloud (AWS)         |

- **Yes** — fully implemented
- **No** — not available (raises `NotImplementedError`)
- **Stub** — method signatures exist but raise `NotImplementedError` at runtime

**Kafka, RabbitMQ, Pulsar, SQS**: Queue-only. For deduplication and storage, use Redis, MongoDB, ElasticSearch, Memcached, or DynamoDB.

**RocketMQ**: Queue is functional. Set and Storage methods exist but raise `NotImplementedError` at runtime. Pair with a full-featured backend for dedup/storage.

**Memcached, DynamoDB**: Storage-only (key-value with TTL). Pair with a queue-capable backend for request distribution.

## Guarantees

What the library contractually promises — and just as importantly, what it does **not**. Read this before relying on any feature in production. Every claim below is backed by code in `src/scrapy_extension/`; follow the linked file anchors to verify.

### Per-feature cross-worker behavior

| Layer | Strategy | Cross-worker safe? | Notes |
|---|---|---|---|
| Queue | `passthrough` (default) | Yes | Items live in the backend queue; atomic pop on every backend (`backends/base.py:359`). |
| Queue | `delay` | Per-process | In-process `heapq`; **lost on crash**. `DelayQueueStrategy.close()` warns; soft-cap `max_held` warns once when exceeded (`queue/strategies/delay.py`). |
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
| **Backend capability honesty.** A backend that does not implement `QueueBackend` / `SetBackend` / `StorageBackend` raises `NotImplementedError` on first call — never silently no-ops. The matrix above is the contract. | `backends/base.py` ABCs; `backends/connectors.py` capability gates |
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

Two strategy layers sit above the backend interfaces, selected via Scrapy settings — no code change required. Defaults preserve prior behavior exactly.

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
| `throttle` | rate-limited pops (`SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL`) |

`delay` / `round_robin` hold state in-process (single-worker v1); `passthrough` / `throttle` use the backend queue.

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

All backends implement `Backend` + `QueueBackend`. Redis, MongoDB, and ElasticSearch also implement `SetBackend` + `StorageBackend`.

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

See [`examples/`](examples) — working spiders covering all backends:

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
| `quotes_crawl` | Redis | CrawlSpider variant |
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
uv run pytest --cov=src/scrapy_extension --cov-report=term-missing

# Run full matrix (all Python versions)
uv run poe test
```

Test infrastructure includes: pytest-xdist (parallel), pytest-randomly (randomized order), pytest-mock, pytest-cov (coverage), pytest-ruff (lint), pytest-socket (network isolation), and more.

## License

MIT — see [LICENSE](LICENSE).


