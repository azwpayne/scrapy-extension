# Scrapy Extension Examples

Runnable examples for
[scrapy-extension](https://github.com/azwpayne/scrapy-extension). The example
project enables the extension components globally with Redis as their default
backend. Individual spiders use `custom_settings` where a different component
topology is required.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) or pip
- Redis for the default scheduler, distributed deduplication, and item storage
- The backend named by the spider you run

```bash
# From the repository root
uv sync
docker run -d --name scrapy-redis -p 6379:6379 redis:7-alpine

cd examples
scrapy list
scrapy crawl quotes_redis
```

## How Configuration Is Split

There are two independent configuration paths:

1. `BackendSpiderMixin.backend_type` and `backend_settings` configure the
   spider's direct `ConnectionManager` access.
2. `SCRAPY_QUEUE_BACKEND_*`, `SCRAPY_SET_BACKEND_*`, and
   `SCRAPY_STORAGE_BACKEND_*` configure the Scheduler, DupeFilter, and Pipeline.

A mixin class attribute does not retarget those Scrapy components. The bundled
Kafka and RabbitMQ spiders therefore select their queue backend with
`custom_settings` and keep Redis for distributed deduplication and storage.
Normal Scrapy construction automatically calls the mixin's idempotent setup
from `from_crawler()`; do not call `setup_backend()` in a spider `__init__`.

Backend type precedence for components is:

```text
Scrapy component setting > Scrapy global setting >
environment component setting > environment global setting > redis
```

For backend fields, an explicit nested Scrapy dictionary wins over flat Scrapy
settings, which win over environment variables and model defaults. Thus an
environment variable does not override a value already present in
`settings.py`, `custom_settings`, or a `-s` command-line setting.

## Backend Capabilities

| Backend | Queue | Set | Storage | Modes |
|---|---:|---:|---:|---|
| Redis | yes | yes | yes | standalone, master_slave, sentinel, cluster |
| MongoDB | yes | yes | yes | standalone, replica_set, sharded_cluster, atlas |
| Kafka | yes | no | no | standalone, cluster, confluent |
| RabbitMQ | yes | no | no | standalone, cluster, mirrored_queues |
| ElasticSearch | yes | yes | yes | standalone, cloud |
| RocketMQ | yes | no | no | standalone, cluster, cloud |
| Pulsar | yes | no | no | standalone, cluster |
| SQS | yes | no | no | standalone (LocalStack), cloud (AWS) |
| Memcached | no | no | yes | standalone |
| DynamoDB | no | no | yes | standalone (LocalStack), cloud (AWS) |

Selecting a backend for an unsupported component raises `ConfigurationError`
at startup. The table describes interface support, not every optional
operation: Pulsar and RocketMQ cannot report queue depth, SQS depth is
approximate, and Memcached cannot prefix-clear keys. A server-wide Memcached
clear is disabled unless `SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=True`.

Queue-only backends should normally be deployed as a hybrid:

```python
SCRAPY_QUEUE_BACKEND_TYPE = "kafka"       # or rabbitmq/pulsar/sqs/rocketmq
SCRAPY_SET_BACKEND_TYPE = "redis"
SCRAPY_STORAGE_BACKEND_TYPE = "redis"
```

A local `memory`, `bloom`, or `cuckoo` dedup strategy can run without a Set
backend, but it does not deduplicate across workers.

## Example Inventory

| Spider | Demonstrates |
|---|---|
| `quotes_redis` | Redis for all three extension components |
| `quotes_mongodb` | MongoDB for all three components |
| `quotes_kafka` | Kafka queue plus Redis set/storage |
| `quotes_rabbitmq` | RabbitMQ queue plus Redis set/storage |
| `quotes_elasticsearch` | ElasticSearch for all three components |
| `quotes_programmatic` | Mixin `backend_settings` for direct manager access |
| `quotes_multi_mode` | Selectable Redis Sentinel or Cluster config |
| `quotes_connection_manager` | Low-level manager and capability accessors |
| `quotes`, `quotes_crawl` | Plain Scrapy scheduler/dupefilter, no extension pipeline |

## Redis (`quotes_redis`)

```bash
scrapy crawl quotes_redis
```

Common settings:

```python
SCRAPY_REDIS_MODE = "standalone"
SCRAPY_REDIS_HOST = "localhost"
SCRAPY_REDIS_PORT = 6379
SCRAPY_REDIS_DB = 0
SCRAPY_REDIS_NAMESPACE = "quotes-dev"
# SCRAPY_REDIS_USERNAME = "crawler"
# SCRAPY_REDIS_PASSWORD = "secret"
```

Every deployment sharing a Redis database should have a distinct namespace.
The default is `scrapy-extension`; it separates queue/set/storage domains but
does not distinguish two applications. New namespaced keys do not fall back to
the legacy unnamespaced layout. Drain or migrate persistent deployments before
upgrading; see the
[migration guide](https://github.com/azwpayne/scrapy-extension/blob/main/docs/migration-guide.md).

Deployment modes:

```python
# Master/replicas
SCRAPY_REDIS_MODE = "master_slave"
SCRAPY_REDIS_HOST = "master.redis.internal"
SCRAPY_REDIS_REPLICAS = ["replica1:6379", "replica2:6379"]
SCRAPY_REDIS_READ_FROM_REPLICAS = True

# Sentinel
SCRAPY_REDIS_MODE = "sentinel"
SCRAPY_REDIS_SENTINELS = ["sentinel1:26379", "sentinel2:26379"]
SCRAPY_REDIS_SENTINEL_MASTER_NAME = "mymaster"

# Cluster
SCRAPY_REDIS_MODE = "cluster"
SCRAPY_REDIS_CLUSTER_STARTUP_NODES = ["node1:7000", "node2:7000"]
SCRAPY_REDIS_CLUSTER_MAX_REDIRECTS = 5

# TLS fields use these exact names
SCRAPY_REDIS_SSL_ENABLED = True
SCRAPY_REDIS_SSL_CAFILE = "/etc/ssl/redis-ca.pem"
SCRAPY_REDIS_SSL_CHECK_HOSTNAME = True
```

The mixin spider needs only the backend declaration:

```python
class QuotesRedisSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
    name = "quotes_redis"
    backend_type = BackendType.REDIS
```

## MongoDB (`quotes_mongodb`)

```bash
docker run -d --name scrapy-mongo -p 27017:27017 mongo:7
scrapy crawl quotes_mongodb
```

The spider's `custom_settings` sets `SCRAPY_BACKEND_TYPE=mongodb`, so Queue,
Set, and Storage all use MongoDB. Configure modes through Scrapy settings, not
ad-hoc spider attributes:

```python
SCRAPY_MONGO_MODE = "standalone"
SCRAPY_MONGO_URI = "mongodb://localhost:27017"
SCRAPY_MONGO_DATABASE = "scrapy_quotes"

# Replica set
SCRAPY_MONGO_MODE = "replica_set"
SCRAPY_MONGO_URI = "mongodb://rs1:27017,rs2:27017,rs3:27017"
SCRAPY_MONGO_REPLICA_SET_NAME = "crawler-rs"
SCRAPY_MONGO_READ_PREFERENCE = "secondary"

# Sharded cluster
SCRAPY_MONGO_MODE = "sharded_cluster"
SCRAPY_MONGO_MONGOS_ROUTERS = ["mongos1:27017", "mongos2:27017"]

# Atlas requires an explicit SRV URI
SCRAPY_MONGO_MODE = "atlas"
SCRAPY_MONGO_URI = "mongodb+srv://cluster.example.net/scrapy"
```

Prefer `SCRAPY_MONGO_USERNAME` and `SCRAPY_MONGO_PASSWORD` over embedding
credentials in the plain-string URI, and never log the full URI.

## Kafka (`quotes_kafka`)

Kafka is queue-only. The example selects Kafka only for the queue and uses
Redis for Set and Storage:

```bash
scrapy crawl quotes_kafka
```

```python
SCRAPY_QUEUE_BACKEND_TYPE = "kafka"
SCRAPY_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCRAPY_KAFKA_GROUP_ID = "scrapy-extension"
SCRAPY_SET_BACKEND_TYPE = "redis"
SCRAPY_STORAGE_BACKEND_TYPE = "redis"
```

Confluent Cloud uses SASL/SSL settings:

```python
SCRAPY_KAFKA_MODE = "confluent"
SCRAPY_KAFKA_CONFLUENT_BOOTSTRAP_SERVERS = "pkc.example:9092"
SCRAPY_KAFKA_CONFLUENT_API_KEY = "api-key"
SCRAPY_KAFKA_CONFLUENT_API_SECRET = "api-secret"
```

Do not select `priority` or `work_stealing` with Kafka; both strategies are
rejected because one logical queue fans out to physical topics that the
consumer cannot isolate safely.

## RabbitMQ (`quotes_rabbitmq`)

RabbitMQ is queue-only. The example uses its queue with Redis Set/Storage:

```bash
docker run -d --name scrapy-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
scrapy crawl quotes_rabbitmq
```

Use either a URL containing credentials or explicit required username/password
fields:

```python
SCRAPY_QUEUE_BACKEND_TYPE = "rabbitmq"
SCRAPY_RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

# Equivalent explicit form
# SCRAPY_RABBITMQ_HOST = "localhost"
# SCRAPY_RABBITMQ_PORT = 5672
# SCRAPY_RABBITMQ_USERNAME = "guest"
# SCRAPY_RABBITMQ_PASSWORD = "guest"
# SCRAPY_RABBITMQ_VIRTUAL_HOST = "/"

SCRAPY_SET_BACKEND_TYPE = "redis"
SCRAPY_STORAGE_BACKEND_TYPE = "redis"
```

Mirrored queue settings use real Scrapy keys:

```python
SCRAPY_RABBITMQ_MODE = "mirrored_queues"
SCRAPY_RABBITMQ_CLUSTER_NODES = ["rabbit2:5672", "rabbit3:5672"]
SCRAPY_RABBITMQ_HA_MODE = "all"
```

Publishing uses confirms plus mandatory routing. An unroutable or negatively
acknowledged publish raises instead of returning success.

## RocketMQ Recipe

RocketMQ is queue-only and connects to a RocketMQ 5 gRPC proxy, not the legacy
NameServer protocol port:

```python
SCRAPY_QUEUE_BACKEND_TYPE = "rocketmq"
SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "localhost:8081"
SCRAPY_ROCKETMQ_INVISIBLE_DURATION = 300
SCRAPY_SET_BACKEND_TYPE = "redis"
SCRAPY_STORAGE_BACKEND_TYPE = "redis"
```

`invisible_duration` is 10..43200 seconds. The extension does not renew the
lease; set it above the maximum time from queue pop to downloader response.
Nack uses RocketMQ's 10-second minimum. Queue depth is unavailable, so
depth-driven backpressure and queue-depth metrics cannot operate. `priority`
and `work_stealing` are rejected for the same consumer-isolation reason as
Kafka.

## ElasticSearch (`quotes_elasticsearch`)

```bash
docker run -d --name scrapy-elastic -p 9200:9200 \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  elasticsearch:8.12.0
scrapy crawl quotes_elasticsearch
```

```python
SCRAPY_ELASTICSEARCH_MODE = "standalone"
SCRAPY_ELASTICSEARCH_HOSTS = ["http://localhost:9200"]
```

Pydantic list environment values must be JSON, not comma-separated text:

```bash
export SCRAPY_ELASTICSEARCH_HOSTS='["https://es1.example:9200","https://es2.example:9200"]'
```

Cloud mode:

```python
SCRAPY_ELASTICSEARCH_MODE = "cloud"
SCRAPY_ELASTICSEARCH_CLOUD_ID = "deployment:encoded-value"
SCRAPY_ELASTICSEARCH_API_KEY = "api-key"
```

Credential-bearing standalone connections over cleartext `http://` fail
configuration validation. Use HTTPS and certificate verification.

## Programmatic Mixin Configuration (`quotes_programmatic`)

`backend_settings` configures direct mixin access. It does not replace the
project's Scheduler/DupeFilter/Pipeline settings.

```python
class TenantSpider(BackendSpiderMixin, scrapy.Spider):
    name = "tenant"
    backend_type = BackendType.REDIS
    backend_settings = {
        "host": "redis.internal",
        "port": 6379,
        "db": 0,
        "namespace": "tenant-a",
        "ssl_enabled": True,
        "ssl_cafile": "/etc/ssl/redis-ca.pem",
    }
```

Scrapy-created spiders initialize this automatically. Direct construction is a
different lifecycle and must be explicit:

```python
spider = TenantSpider()
try:
    manager = spider.setup_backend()
    storage = manager.get_storage_backend()
    storage.store("probe", b"ok")
finally:
    spider.close_backend()
```

Do not reuse a namespace between tenants. A Redis database number alone is not
portable isolation for Cluster deployments.

## Redis Multi-Mode (`quotes_multi_mode`)

The example selects Sentinel by default and Cluster when requested:

```bash
SCRAPY_EXAMPLE_REDIS_MODE=sentinel scrapy crawl quotes_multi_mode
SCRAPY_EXAMPLE_REDIS_MODE=cluster scrapy crawl quotes_multi_mode
```

Its selected dictionary is applied both to the mixin and to
`SCRAPY_BACKEND_SETTINGS`, so the extension components use the same topology.
Use `cluster_startup_nodes`, not `startup_nodes`.

## Low-Level ConnectionManager (`quotes_connection_manager`)

Each successful registry acquisition owns exactly one release:

```python
manager = ConnectionManager.get_manager(
    backend_type=BackendType.REDIS,
    settings={"host": "localhost", "port": 6379, "namespace": "manual"},
)
try:
    queue = manager.get_queue_backend()
    seen = manager.get_set_backend()
    storage = manager.get_storage_backend()
finally:
    manager.close()
```

Managers with the same backend type and normalized settings share a raw
connection through reference counting. Calling `close()` twice for one
acquisition can release another holder's reference; pair each acquisition with
exactly one `close()`, normally in `finally`. Accessing an unsupported
capability raises `NotImplementedError`; configuration-time component binding
normally catches the same mismatch earlier as `ConfigurationError`.

The example spider wires its own close signal because it acquires the manager
directly. The mixin already performs that signal wiring and should not be wired
again.

## Plain Scrapy Spiders

`quotes` and `quotes_crawl` override the example project's global components:

```bash
scrapy crawl quotes -O quotes.json
scrapy crawl quotes_crawl -O quotes-crawl.json
```

They use Scrapy's standard scheduler and `RFPDupeFilter` and disable the
extension item pipeline. They are useful controls when diagnosing whether a
failure is in the target site, Scrapy, or a backend integration.

## Troubleshooting

**Backend did not change after exporting an environment variable**

Inspect the spider's `custom_settings` and project `settings.py`. Explicit
Scrapy values have higher precedence. Override them with Scrapy's command-line
priority, for example:

```bash
scrapy crawl quotes_redis -s SCRAPY_REDIS_NAMESPACE=one-off-test
```

**Queue-only backend fails capability validation**

Bind only `SCRAPY_QUEUE_BACKEND_TYPE` to Kafka, RabbitMQ, Pulsar, SQS, or
RocketMQ. Keep a set-capable backend for the default distributed dedup strategy
and a storage-capable backend for the pipeline, or replace/disable those
components intentionally.

**`ValidationError` versus `ConfigurationError`**

Pydantic field type, range, enum, and extra-field failures raise
`ValidationError`. Unknown adapter keys, unsupported component capabilities,
and cross-field project constraints raise `ConfigurationError`.

**A renamed Redis deployment starts empty**

The current namespace layout intentionally does not read old unnamespaced keys.
Do not use `FLUSHDB` as a migration tool on a shared database. Follow the
[migration guide](https://github.com/azwpayne/scrapy-extension/blob/main/docs/migration-guide.md).

**Spider not found**

Run commands from the example Scrapy project:

```bash
cd examples
scrapy list
```
