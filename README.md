# Scrapy Extension

Distributed crawling for Scrapy with pluggable backends: **Redis**, **MongoDB**, **Kafka**, **RabbitMQ**, **ElasticSearch**, and **RocketMQ**.

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Scrapy](https://raw.githubusercontent.com/scrapy/scrapy/master/docs/_static/logo.svg)](https://docs.scrapy.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/uv-lib/scrapy-extension/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://pypi.org/project/scrapy-extension/)

## Features

- **Multi-Backend**: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ
- **Multi-Mode**: Standalone, cluster, cloud deployments per backend
- **Distributed Queue**: Priority-based request queue across spiders
- **Duplicate Filtering**: Cross-instance URL deduplication
- **Item Storage**: Key-value storage with TTL support
- **Type Safe**: Full type annotations, `py.typed` marker
- **Security**: Input validation to prevent injection attacks (Redis key names, Kafka topic names, MongoDB prefixes)

## Installation

```bash
pip install scrapy-extension                  # Core (Redis only)
pip install scrapy-extension[mongodb]         # + MongoDB
pip install scrapy-extension[kafka]          # + Kafka
pip install scrapy-extension[rabbitmq]        # + RabbitMQ
pip install scrapy-extension[elasticsearch]  # + ElasticSearch
pip install scrapy-extension[rocketmq]      # + RocketMQ
pip install scrapy-extension[all]            # All backends
```

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

All settings use `SCRAPY_<BACKEND>_<KEY>` env vars with pydantic-settings.

### Redis (supports standalone, master_slave, sentinel, cluster)

```python
SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_HOST = "localhost"
SCRAPY_REDIS_PORT = 6379
```

### MongoDB (supports standalone, replica_set, sharded_cluster, atlas)

```python
SCRAPY_BACKEND_TYPE = "mongodb"
SCRAPY_MONGO_URI = "mongodb://localhost:27017"
SCRAPY_MONGO_DATABASE = "scrapy"
```

### Kafka (supports standalone, cluster, confluent)

```python
SCRAPY_BACKEND_TYPE = "kafka"
SCRAPY_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
```

### RabbitMQ (supports standalone, cluster, mirrored_queues)

```python
SCRAPY_BACKEND_TYPE = "rabbitmq"
SCRAPY_RABBITMQ_HOST = "localhost"
SCRAPY_RABBITMQ_PORT = 5672
```

### ElasticSearch (supports standalone, cloud)

```python
SCRAPY_BACKEND_TYPE = "elasticsearch"
SCRAPY_ELASTICSEARCH_HOSTS = ["http://localhost:9200"]
```

### RocketMQ (supports standalone, cluster, cloud)

```python
SCRAPY_BACKEND_TYPE = "rocketmq"
SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "localhost:9876"
```

See [`examples/`](examples) for all deployment modes (Sentinel, Cluster, Atlas, Confluent, etc).

## Backend Capabilities

| Backend       | Queue | Set | Storage | Best For                     |
|---------------|-------|-----|---------|------------------------------|
| Redis         | Yes   | Yes | Yes     | Full-featured, fast          |
| MongoDB       | Yes   | Yes | Yes     | Document storage, queries    |
| ElasticSearch | Yes   | Yes | Yes     | Full-text search              |
| Kafka         | Yes   | No  | No      | High throughput streaming     |
| RabbitMQ      | Yes   | No  | No      | Reliable messaging            |
| RocketMQ      | Yes   | No  | No      | Alibaba Cloud, low latency    |

**Note:** Kafka, RabbitMQ, and RocketMQ only implement `QueueBackend`. For deduplication and storage, use Redis, MongoDB, or ElasticSearch.

## Advanced Usage

### Connection Manager

```python
from scrapy_extension import ConnectionManager, BackendType

manager = ConnectionManager.get_manager(backend_type=BackendType.REDIS)
queue = manager.get_queue_backend()
queue.push("my_queue", b"item_data", priority=1.0)
```

### Per-Spider Settings

```python
class MySpider(BackendSpiderMixin, scrapy.Spider):
    backend_type = BackendType.REDIS
    backend_settings = {"host": "localhost", "port": 6379}
```

## Examples

See [`examples/`](examples) — 10 working spiders covering all backends and config modes:

- `quotes_redis.py` - Basic Redis backend
- `quotes_mongodb.py` - MongoDB backend
- `quotes_kafka.py` - Kafka backend
- `quotes_rabbitmq.py` - RabbitMQ backend
- `quotes_elasticsearch.py` - ElasticSearch backend
- `quotes_rocketmq.py` - RocketMQ backend
- `quotes_multi_mode.py` - Multi-mode configurations
- `quotes_connection_manager.py` - Direct ConnectionManager usage
- `quotes_crawl.py` - CrawlSpider variant
- `quotes_programmatic.py` - Programmatic spider control

## License

MIT — see [LICENSE](LICENSE).
