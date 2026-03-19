# Scrapy Extension

A Scrapy extension providing distributed crawling capabilities with pluggable backends:
**Redis**, **MongoDB**, **Kafka**, and **RabbitMQ**.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

## Features

- **Multi-Backend Support**: Choose from Redis, MongoDB, Kafka, or RabbitMQ
- **Multi-Mode Deployment**: Each backend supports multiple deployment modes (
  standalone, cluster, cloud)
- **Distributed Queue**: Priority-based request queue across multiple spiders
- **Duplicate Filtering**: Distributed deduplication using Set operations
- **Item Storage**: Key-value storage with TTL support
- **Type Safe**: Full type annotations with `py.typed` marker

## Installation

```bash
# Install with uv
uv add scrapy-extension

# Or with pip
pip install scrapy-extension

# With specific backend support
pip install scrapy-extension[mongodb]   # MongoDB support
pip install scrapy-extension[kafka]     # Kafka support
pip install scrapy-extension[rabbitmq]  # RabbitMQ support
pip install scrapy-extension[all]       # All backends
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
        # Your parsing logic
        yield {"url": response.url}
```

Configure in `settings.py`:

```python
# Required settings
SCHEDULER = "scrapy_extension.components.scheduler.BackendScheduler"
DUPEFILTER_CLASS = "scrapy_extension.components.dupefilter.BackendDupeFilter"
ITEM_PIPELINES = {
    "scrapy_extension.components.pipeline.BackendPipeline": 300,
}

# Backend selection (redis, mongodb, kafka, rabbitmq)
SCRAPY_BACKEND_TYPE = "redis"

# Redis settings
SCRAPY_REDIS_HOST = "localhost"
SCRAPY_REDIS_PORT = 6379
```

## Backend Configuration

### Redis

Supports 4 deployment modes:

#### Standalone (Default)

```python
# settings.py
SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_HOST = "localhost"
SCRAPY_REDIS_PORT = 6379
SCRAPY_REDIS_DB = 0
SCRAPY_REDIS_PASSWORD = "secret"  # Optional
```

#### Master-Slave

```python
SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_MODE = "master_slave"
SCRAPY_REDIS_HOST = "master.redis.com"
SCRAPY_REDIS_PORT = 6379
SCRAPY_REDIS_REPLICAS = ["replica1.redis.com:6379", "replica2.redis.com:6379"]
SCRAPY_REDIS_READ_FROM_REPLICAS = True
```

#### Sentinel (High Availability)

```python
SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_MODE = "sentinel"
SCRAPY_REDIS_SENTINELS = ["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"]
SCRAPY_REDIS_SENTINEL_MASTER_NAME = "mymaster"
SCRAPY_REDIS_SENTINEL_PASSWORD = "sentinel_secret"  # Optional
SCRAPY_REDIS_PASSWORD = "redis_secret"
```

#### Cluster

```python
SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_MODE = "cluster"
SCRAPY_REDIS_CLUSTER_STARTUP_NODES = ["node1:7000", "node2:7000", "node3:7000"]
SCRAPY_REDIS_PASSWORD = "secret"
SCRAPY_REDIS_CLUSTER_MAX_REDIRECTS = 5
```

### MongoDB

Supports 4 deployment modes:

#### Standalone (Default)

```python
# settings.py
SCRAPY_BACKEND_TYPE = "mongodb"
SCRAPY_MONGO_URI = "mongodb://localhost:27017"
SCRAPY_MONGO_DATABASE = "scrapy"
```

#### Replica Set

```python
SCRAPY_BACKEND_TYPE = "mongodb"
SCRAPY_MONGO_MODE = "replica_set"
SCRAPY_MONGO_REPLICA_SET_NAME = "myReplicaSet"
SCRAPY_MONGO_REPLICA_SET_MEMBERS = ["host1:27017", "host2:27017", "host3:27017"]
SCRAPY_MONGO_DATABASE = "scrapy"
SCRAPY_MONGO_TLS_ENABLED = True
```

#### Sharded Cluster

```python
SCRAPY_BACKEND_TYPE = "mongodb"
SCRAPY_MONGO_MODE = "sharded_cluster"
SCRAPY_MONGO_MONGOS_ROUTERS = ["router1:27017", "router2:27017"]
SCRAPY_MONGO_DATABASE = "scrapy"
```

#### Atlas (MongoDB Cloud)

```python
SCRAPY_BACKEND_TYPE = "mongodb"
SCRAPY_MONGO_MODE = "atlas"
SCRAPY_MONGO_URI = "mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/scrapy?retryWrites=true&w=majority"
```

### Kafka

Supports 3 deployment modes:

#### Standalone (Default)

```python
# settings.py
SCRAPY_BACKEND_TYPE = "kafka"
SCRAPY_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCRAPY_KAFKA_GROUP_ID = "scrapy-spiders"
```

#### Cluster

```python
SCRAPY_BACKEND_TYPE = "kafka"
SCRAPY_KAFKA_MODE = "cluster"
SCRAPY_KAFKA_CLUSTER_BROKERS = ["broker1:9092", "broker2:9092", "broker3:9092"]
SCRAPY_KAFKA_REPLICATION_FACTOR = 3
```

#### Confluent Cloud

```python
SCRAPY_BACKEND_TYPE = "kafka"
SCRAPY_KAFKA_MODE = "confluent"
SCRAPY_KAFKA_CONFLUENT_BOOTSTRAP_SERVERS = "pkc-xxx.us-east-1.aws.confluent.cloud:9092"
SCRAPY_KAFKA_CONFLUENT_API_KEY = "API_KEY"
SCRAPY_KAFKA_CONFLUENT_API_SECRET = "API_SECRET"
```

### RabbitMQ

Supports 3 deployment modes:

#### Standalone (Default)

```python
# settings.py
SCRAPY_BACKEND_TYPE = "rabbitmq"
SCRAPY_RABBITMQ_HOST = "localhost"
SCRAPY_RABBITMQ_PORT = 5672
SCRAPY_RABBITMQ_USERNAME = "guest"
SCRAPY_RABBITMQ_PASSWORD = "guest"
SCRAPY_RABBITMQ_VIRTUAL_HOST = "/"
```

#### Cluster

```python
SCRAPY_BACKEND_TYPE = "rabbitmq"
SCRAPY_RABBITMQ_MODE = "cluster"
SCRAPY_RABBITMQ_HOST = "node1"
SCRAPY_RABBITMQ_PORT = 5672
SCRAPY_RABBITMQ_CLUSTER_NODES = ["node2:5672", "node3:5672"]
```

#### Mirrored Queues (High Availability)

```python
SCRAPY_BACKEND_TYPE = "rabbitmq"
SCRAPY_RABBITMQ_MODE = "mirrored_queues"
SCRAPY_RABBITMQ_HOST = "node1"
SCRAPY_RABBITMQ_PORT = 5672
SCRAPY_RABBITMQ_HA_MODE = "exactly"  # or "all", "nodes"
SCRAPY_RABBITMQ_HA_PARAMS = "2"  # Number of replicas
SCRAPY_RABBITMQ_HA_SYNC_MODE = "automatic"
```

#### SSL/TLS Configuration

```python
SCRAPY_BACKEND_TYPE = "rabbitmq"
SCRAPY_RABBITMQ_SSL_ENABLED = True
SCRAPY_RABBITMQ_SSL_CAFILE = "/path/to/ca.pem"
SCRAPY_RABBITMQ_SSL_CERTFILE = "/path/to/cert.pem"
SCRAPY_RABBITMQ_SSL_KEYFILE = "/path/to/key.pem"
SCRAPY_RABBITMQ_SSL_VERIFY_MODE = "CERT_REQUIRED"  # or "CERT_NONE", "CERT_OPTIONAL"
```

## Programmatic Configuration

```python
from scrapy_extension import (
    RedisSettings, RedisMode,
    MongoDBSettings, MongoDBMode,
    KafkaSettings, KafkaMode,
    RabbitMQSettings, RabbitMQMode,
)

# Redis Sentinel
redis_settings = RedisSettings(
    mode=RedisMode.SENTINEL,
    sentinels=["sentinel1:26379", "sentinel2:26379"],
    sentinel_master_name="mymaster",
    password="secret",
)

# MongoDB Replica Set
mongo_settings = MongoDBSettings(
    mode=MongoDBMode.REPLICA_SET,
    replica_set_name="myReplicaSet",
    replica_set_members=["host1:27017", "host2:27017"],
    tls_enabled=True,
)

# Kafka Confluent Cloud
kafka_settings = KafkaSettings(
    mode=KafkaMode.CONFLUENT,
    confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
    confluent_api_key="API_KEY",
    confluent_api_secret="API_SECRET",
)

# RabbitMQ Mirrored Queues
rabbitmq_settings = RabbitMQSettings(
    mode=RabbitMQMode.MIRRORED_QUEUES,
    host="node1",
    port=5672,
    ha_mode="exactly",
    ha_params="2",
)
```

## Advanced Usage

### Custom Connection Manager

```python
from scrapy_extension import ConnectionManager, BackendType

# Get connection manager for specific backend
manager = ConnectionManager.get_manager(backend_type=BackendType.REDIS)

# Access backend interfaces
queue_backend = manager.get_queue_backend()
set_backend = manager.get_set_backend()
storage_backend = manager.get_storage_backend()

# Use directly
queue_backend.push("my_queue", b"item_data", priority=1.0)
item = queue_backend.pop("my_queue")
```

### Backend Capabilities

| Backend  | Queue | Set | Storage | Best For                            |
|----------|-------|-----|---------|-------------------------------------|
| Redis    | Yes   | Yes | Yes     | Full-featured, fast, simple         |
| MongoDB  | Yes   | Yes | Yes     | Document storage, complex queries   |
| Kafka    | Yes   | No  | No      | High throughput, event streaming    |
| RabbitMQ | Yes   | No  | No      | Reliable messaging, priority queues |

### Environment Variables

All settings can be configured via environment variables:

```bash
# General
export SCRAPY_BACKEND_TYPE=redis
export SCRAPY_RETRY_ATTEMPTS=3
export SCRAPY_RETRY_DELAY=1.0

# Redis
export SCRAPY_REDIS_HOST=localhost
export SCRAPY_REDIS_PORT=6379
export SCRAPY_REDIS_MODE=cluster
export SCRAPY_REDIS_CLUSTER_STARTUP_NODES="node1:7000,node2:7000"

# MongoDB
export SCRAPY_MONGO_URI="mongodb://localhost:27017"
export SCRAPY_MONGO_MODE=replica_set
export SCRAPY_MONGO_REPLICA_SET_NAME="myReplicaSet"

# Kafka
export SCRAPY_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SCRAPY_KAFKA_MODE=confluent
export SCRAPY_KAFKA_CONFLUENT_API_KEY="API_KEY"

# RabbitMQ
export SCRAPY_RABBITMQ_HOST=localhost
export SCRAPY_RABBITMQ_MODE=mirrored_queues
export SCRAPY_RABBITMQ_HA_MODE=all
```

## Development

### Setup

```bash
# Clone repository
git clone https://github.com/yourusername/scrapy-extension.git
cd scrapy-extension

# Install dependencies
uv sync

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=scrapy_extension
```

### Project Structure

```
scrapy-extension/
├── src/scrapy_extension/
│   ├── backends/          # Backend implementations
│   │   ├── base.py        # Backend protocols
│   │   ├── redis_backend.py
│   │   ├── mongodb_backend.py
│   │   ├── kafka_backend.py
│   │   └── rabbitmq_backend.py
│   ├── components/        # Scrapy components
│   │   ├── queue.py
│   │   ├── scheduler.py
│   │   ├── dupefilter.py
│   │   └── pipeline.py
│   ├── config/            # Configuration
│   │   └── settings.py
│   ├── connection/        # Connection management
│   │   └── manager.py
│   ├── spider_mixin.py    # Spider integration
│   └── exceptions.py      # Custom exceptions
├── tests/                 # Test suite
├── CLAUDE.md             # Claude Code guidance
└── README.md             # This file
```

## License

MIT License - see LICENSE file for details.
