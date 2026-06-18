# Scrapy Extension

Distributed crawling for Scrapy with pluggable backends: **Redis**, **MongoDB**, **Kafka**, **RabbitMQ**, **ElasticSearch**, and **RocketMQ**.

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://pypi.org/project/scrapy-extension/)

## Features

- **6 Backends**: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ
- **Multi-Mode**: Standalone, cluster, cloud deployments per backend
- **Distributed Queue**: Priority-based request queue across spiders
- **Duplicate Filtering**: Cross-instance URL deduplication via `SetBackend`
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

- **Yes** — fully implemented
- **No** — not available (raises `NotImplementedError`)
- **Stub** — method signatures exist but raise `NotImplementedError` at runtime

**Kafka, RabbitMQ**: Queue-only. For deduplication and storage, use Redis, MongoDB, or ElasticSearch.

**RocketMQ**: Queue is functional. Set and Storage methods exist but raise `NotImplementedError` at runtime. Pair with a full-featured backend for dedup/storage.

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

Uses `SetBackend.add()` for atomic duplicate detection. Gracefully skips dedup for queue-only backends (Kafka, RabbitMQ).

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
