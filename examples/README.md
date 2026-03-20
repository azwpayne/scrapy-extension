# Scrapy Extension Examples

Working examples demonstrating all features of [scrapy-extension](../README.md) —
distributed crawling with pluggable backends (Redis, MongoDB, Kafka, RabbitMQ).

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) or pip
- At least one backend running locally (Redis by default)

```bash
# Quick Redis setup (Docker)
docker run -d -p 6379:6379 redis:7

# Or install locally
# brew install redis && redis-server
```

## Quick Start

```bash
# Install the package (from repo root)
uv sync

# Run the Redis example
cd examples
scrapy crawl quotes_redis
```

## Project Structure

```
examples/
├── scrapy.cfg                  # Scrapy project config
├── examples/
│   ├── __init__.py
│   ├── items.py                # QuoteItem definition
│   ├── settings.py             # Backend-enabled Scrapy settings
│   ├── middlewares.py          # Default spider/downloader middlewares
│   ├── pipelines.py            # Pipeline examples (standard + BackendPipeline)
│   └── spiders/
│       ├── quotes_redis.py               # Redis backend spider
│       ├── quotes_mongodb.py             # MongoDB backend spider
│       ├── quotes_kafka.py               # Kafka backend spider (queue only)
│       ├── quotes_rabbitmq.py            # RabbitMQ backend spider (queue only)
│       ├── quotes_programmatic.py        # Programmatic settings configuration
│       ├── quotes_multi_mode.py          # Redis Sentinel/Cluster modes
│       ├── quotes_connection_manager.py  # Low-level ConnectionManager API
│       ├── quotes.py                     # Basic Scrapy spider (no backend)
│       └── quotes_crawl.py              # Basic CrawlSpider (no backend)
└── README.md
```

## Examples

### 1. Redis Backend (`quotes_redis`)

Full-featured distributed crawling using Redis. Demonstrates `BackendSpiderMixin` with
scheduler, dupefilter, and item pipeline all backed by Redis.

```bash
scrapy crawl quotes_redis
```

**Key concepts:**

- `BackendSpiderMixin` integration
- `BackendScheduler` for distributed request queue
- `BackendDupeFilter` for cross-instance deduplication
- `BackendPipeline` for item storage

```python
import scrapy
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesRedisSpider(BackendSpiderMixin, scrapy.Spider):
    backend_type = BackendType.REDIS

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setup_backend()
```

### 2. MongoDB Backend (`quotes_mongodb`)

Uses MongoDB for distributed crawling. Shows `mongodb_uri` and `mongodb_db` shortcut
attributes.

```bash
# Requires: docker run -d -p 27017:27017 mongo:7
scrapy crawl quotes_mongodb
```

**Key concepts:**

- MongoDB connection shortcuts (`mongodb_uri`, `mongodb_db`)
- Full Queue + Set + Storage support

### 3. Kafka Backend (`quotes_kafka`)

Uses Kafka for request queueing. Kafka only supports Queue operations (no Set for dedup,
no Storage for items).

```bash
# Requires Kafka running on localhost:9092
scrapy crawl quotes_kafka
```

**Key concepts:**

- `kafka_bootstrap_servers` shortcut
- Queue-only limitation (no `get_dupefilter()` or storage)

### 4. RabbitMQ Backend (`quotes_rabbitmq`)

Uses RabbitMQ for request queueing. RabbitMQ only supports Queue operations.

```bash
# Requires: docker run -d -p 5672:5672 rabbitmq:3
scrapy crawl quotes_rabbitmq
```

**Key concepts:**

- `rabbitmq_url` shortcut
- Queue-only limitation (no `get_dupefilter()` or storage)

### 5. Programmatic Configuration (`quotes_programmatic`)

Configures the backend entirely within the spider class using `backend_settings` dict,
instead of `settings.py`. Useful when different spiders need different configurations.

```bash
scrapy crawl quotes_programmatic
```

**Key concepts:**

- `backend_settings` dict overrides `settings.py`
- Per-spider configuration isolation
- All `RedisSettings` fields available as dict keys

### 6. Multi-Mode Deployment (`quotes_multi_mode`)

Demonstrates Redis Sentinel (high availability) configuration. Includes commented-out
Cluster mode config.

```bash
# Requires Redis Sentinel cluster running
scrapy crawl quotes_multi_mode
```

**Key concepts:**

- Sentinel mode: automatic failover
- Cluster mode: automatic sharding
- Mode selection via `backend_settings["mode"]`

### 7. ConnectionManager API (`quotes_connection_manager`)

Low-level approach using `ConnectionManager` directly instead of `BackendSpiderMixin`.
Shows how to use `QueueBackend`, `SetBackend`, and `StorageBackend` interfaces
programmatically.

```bash
scrapy crawl quotes_connection_manager
```

**Key concepts:**

- `ConnectionManager.get_manager()` singleton
- `get_queue_backend()`, `get_set_backend()`, `get_storage_backend()`
- Direct `push()`, `pop()`, `add()`, `contains()`, `store()`, `retrieve()` calls

## Configuration

### settings.py (Recommended)

The default `settings.py` uses Redis. To switch backends, change `SCRAPY_BACKEND_TYPE`:

```python
# SCRAPY_BACKEND_TYPE = "redis"  # Default
# SCRAPY_BACKEND_TYPE = "mongodb"  # MongoDB
# SCRAPY_BACKEND_TYPE = "kafka"  # Kafka (queue only)
# SCRAPY_BACKEND_TYPE = "rabbitmq"  # RabbitMQ (queue only)
```

See `settings.py` for full configuration blocks for each backend with all deployment
modes.

### Environment Variables

All settings can be overridden via environment variables:

```bash
export SCRAPY_BACKEND_TYPE=redis
export SCRAPY_REDIS_HOST=localhost
export SCRAPY_REDIS_PORT=6379
```

### Programmatic (Per-Spider)

```python
import scrapy
from scrapy_extension import BackendSpiderMixin, BackendType


class MySpider(BackendSpiderMixin, scrapy.Spider):
    backend_type = BackendType.REDIS
    backend_settings = {"host": "localhost", "port": 6379, "db": 0}
```

## Backend Capabilities

| Backend  | Queue | Set (Dedup) | Storage | Modes                                           |
|----------|-------|-------------|---------|-------------------------------------------------|
| Redis    | ✅     | ✅           | ✅       | standalone, master_slave, sentinel, cluster     |
| MongoDB  | ✅     | ✅           | ✅       | standalone, replica_set, sharded_cluster, atlas |
| Kafka    | ✅     | ❌           | ❌       | standalone, cluster, confluent                  |
| RabbitMQ | ✅     | ❌           | ❌       | standalone, cluster, mirrored_queues            |

## Troubleshooting

**`RuntimeError: setup_backend() must be called`**
→ Ensure `self.setup_backend()` is called in `__init__` before accessing backend
components.

**`BackendConnectionError`**
→ Verify the backend is running and accessible. Check host/port in settings.

**`NotImplementedError` for `get_set_backend()` or `get_storage_backend()`**
→ Kafka and RabbitMQ only support Queue operations. Use Redis or MongoDB for full
features.

**Spider not found**
→ Run from the `examples/` directory, not the repo root:

```bash
cd examples && scrapy list
```
