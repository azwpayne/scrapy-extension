# Scrapy Extension Examples

Working examples demonstrating all features of [scrapy-extension](../README.md) —
distributed crawling with pluggable backends (Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ).

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
│       ├── quotes_elasticsearch.py       # ElasticSearch backend spider
│       ├── quotes_rocketmq.py             # RocketMQ backend spider (queue only)
│       ├── quotes_programmatic.py        # Programmatic settings configuration
│       ├── quotes_multi_mode.py          # Redis Sentinel/Cluster modes
│       ├── quotes_connection_manager.py  # Low-level ConnectionManager API
│       ├── quotes.py                     # Basic Scrapy spider (no backend)
│       └── quotes_crawl.py              # Basic CrawlSpider (no backend)
└── README.md
```

## Backend Capabilities

| Backend       | Queue | Set (Dedup) | Storage | Modes                                           |
|---------------|-------|-------------|---------|-------------------------------------------------|
| Redis         | Yes   | Yes         | Yes     | standalone, master_slave, sentinel, cluster     |
| MongoDB       | Yes   | Yes         | Yes     | standalone, replica_set, sharded_cluster, atlas|
| Kafka         | Yes   | No          | No      | standalone, cluster, confluent                  |
| RabbitMQ      | Yes   | No          | No      | standalone, cluster, mirrored_queues           |
| ElasticSearch | Yes   | Yes         | Yes     | standalone, cloud                               |
| RocketMQ      | Yes   | No          | No      | standalone, cluster, cloud                       |

**Note:** Kafka, RabbitMQ, and RocketMQ only implement QueueBackend. For deduplication and storage, use Redis, MongoDB, or ElasticSearch.

---

## 1. Redis Backend (`quotes_redis`)

Full-featured distributed crawling using Redis. Demonstrates `BackendSpiderMixin` with
scheduler, dupefilter, and item pipeline all backed by Redis.

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
scrapy crawl quotes_redis
```

### Configuration Options

| Variable | Default | Description |
|----------|---------|-------------|
| `SCRAPY_REDIS_HOST` | `localhost` | Redis hostname |
| `SCRAPY_REDIS_PORT` | `6379` | Redis port |
| `SCRAPY_REDIS_DB` | `0` | Redis database number |
| `SCRAPY_REDIS_PASSWORD` | _(none)_ | Authentication password |
| `SCRAPY_REDIS_MODE` | `standalone` | `standalone`, `master_slave`, `sentinel`, `cluster` |

### Redis Deployment Modes

**Standalone (default)** — single Redis node:

```bash
export SCRAPY_REDIS_MODE=standalone
export SCRAPY_REDIS_HOST=localhost
export SCRAPY_REDIS_PORT=6379
```

**Master-Slave with Read Replicas:**

```python
SCRAPY_REDIS_MODE = "master_slave"
SCRAPY_REDIS_HOST = "master.redis.com"
SCRAPY_REDIS_REPLICAS = ["replica1.redis.com:6379", "replica2.redis.com:6379"]
SCRAPY_REDIS_READ_FROM_REPLICAS = True
```

**Sentinel (High Availability):**

```python
SCRAPY_REDIS_MODE = "sentinel"
SCRAPY_REDIS_SENTINELS = ["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"]
SCRAPY_REDIS_SENTINEL_MASTER_NAME = "mymaster"
SCRAPY_REDIS_PASSWORD = "redis_secret"
```

**Redis Cluster (Sharding):**

```python
SCRAPY_REDIS_MODE = "cluster"
SCRAPY_REDIS_CLUSTER_STARTUP_NODES = ["node1:7000", "node2:7000", "node3:7000"]
SCRAPY_REDIS_CLUSTER_MAX_REDIRECTS = 5
```

### Spider Code

```python
import scrapy

from examples.items import QuotesParsingMixin
from scrapy_extension import BackendSpiderMixin, BackendType


class QuotesRedisSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
    name = "quotes_redis"
    backend_type = BackendType.REDIS

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setup_backend()
```

### Common Errors

**Connection refused** — Redis not running:

```bash
docker start redis
redis-cli ping  # should return PONG
```

**Authentication failure** — wrong password:

```bash
export SCRAPY_REDIS_PASSWORD="your_password"
```

---

## 2. MongoDB Backend (`quotes_mongodb`)

Uses MongoDB for distributed crawling with full Queue + Set + Storage support.

```bash
docker run -d --name mongodb -p 27017:27017 mongo:7
scrapy crawl quotes_mongodb
```

### Configuration Options

| Attribute | Env Variable | Default | Description |
|-----------|-------------|---------|-------------|
| `mongodb_uri` | `SCRAPY_MONGO_URI` | `mongodb://localhost:27017` | Connection URI |
| `mongodb_db` | `SCRAPY_MONGO_DATABASE` | `scrapy_extension` | Database name |
| `mongodb_queue_collection` | `SCRAPY_MONGO_QUEUE_COLLECTION` | `queues` | Queue collection |
| `mongodb_set_collection` | `SCRAPY_MONGO_SET_COLLECTION` | `sets` | Deduplication set collection |
| `mongodb_storage_collection` | `SCRAPY_MONGO_STORAGE_COLLECTION` | `storage` | Key-value storage collection |

### MongoDB-Specific Modes

**REPLICA_SET:**

```python
from scrapy_extension.settings import MongoDBMode

backend_type = BackendType.MONGODB
mongodb_uri = "mongodb://rs1:27017,rs2:27017,rs3:27017"
mongodb_mode = MongoDBMode.REPLICA_SET
mongodb_replica_set_name = "myReplicaSet"
mongodb_read_preference = "secondary"
```

**SHARDED_CLUSTER:**

```python
from scrapy_extension.settings import MongoDBMode

mongodb_mode = MongoDBMode.SHARDED_CLUSTER
mongodb_mongos_routers = ["mongos1:27017", "mongos2:27017"]
```

**ATLAS (TLS enabled automatically):**

```python
from scrapy_extension.settings import MongoDBMode

mongodb_mode = MongoDBMode.ATLAS
mongodb_uri = "mongodb+srv://<atlas-cluster>/?retryWrites=true"
```

### Common Errors

**Connection refused:**

```bash
docker ps | grep mongodb
# or
pgrep -a mongo
```

**Authentication failed** — check URI format:

```python
mongodb_uri = "mongodb://user:password@localhost:27017"
mongodb_auth_source = "admin"
```

---

## 3. Kafka Backend (`quotes_kafka`)

Uses Kafka for distributed request queuing. **Queue-only** — no Set (dedup) or Storage support.

```bash
# Start Zookeeper and Kafka
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:7.5.0
docker run -d --name kafka -p 9092:9092 --link zookeeper \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  wurstmeister/kafka:3.6.0

scrapy crawl quotes_kafka
```

### Configuration Options

| Setting | Env Variable | Default | Description |
|---------|-------------|---------|-------------|
| `bootstrap_servers` | `SCRAPY_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `group_id` | `SCRAPY_KAFKA_GROUP_ID` | `scrapy-extension` | Consumer group ID |
| `max_priority_partitions` | `SCRAPY_KAFKA_MAX_PRIORITY_PARTITIONS` | `10` | Priority partitions |
| `retention_ms` | `SCRAPY_KAFKA_RETENTION_MS` | `604800000` | Retention time (7 days) |

### Kafka Modes

**Confluent Cloud (SASL/SSL):**

```python
SCRAPY_KAFKA_MODE = "confluent"
SCRAPY_KAFKA_CONFLUENT_BOOTSTRAP_SERVERS = "pkc-xxx.us-east-1.aws.confluent.cloud:9092"
SCRAPY_KAFKA_CONFLUENT_API_KEY = "your_api_key"
SCRAPY_KAFKA_CONFLUENT_API_SECRET = "your_api_secret"
```

### Important Limitation

Kafka only implements `QueueBackend`. For deduplication, pair with Redis or MongoDB, or use Scrapy's built-in `RFPDupeFilter`.

### Common Errors

**Connection refused** — verify Kafka is running:

```bash
nc -zv localhost 9092
docker logs kafka
```

**TopicAlreadyExistsError** — this is expected; the backend catches it and continues.

---

## 4. RabbitMQ Backend (`quotes_rabbitmq`)

Uses RabbitMQ for distributed request queuing. **Queue-only** — no Set or Storage support.

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
scrapy crawl quotes_rabbitmq
```

### Configuration Options

| Setting | Env Variable | Default | Description |
|---------|-------------|---------|-------------|
| `rabbitmq_url` | `SCRAPY_RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | Connection URL |
| `SCRAPY_RABBITMQ_MAX_PRIORITY` | `255` | Max priority (1-255) |
| `SCRAPY_RABBITMQ_DURABLE` | `True` | Durable queues |

### RabbitMQ Modes

**MIRRORED_QUEUES (HA):**

```python
from scrapy_extension.settings import RabbitMQMode

rabbitmq_settings = {
    "mode": RabbitMQMode.MIRRORED_QUEUES,
    "cluster_nodes": ["rabbit1:5672", "rabbit2:5672", "rabbit3:5672"],
    "ha_mode": "all",
}
```

### Common Errors

**Queue with x-max-priority not available** — queue was declared without priority support. Delete and recreate the queue, or use a different queue name.

**Connection refused:**

```bash
docker start rabbitmq
docker logs rabbitmq
```

---

## 5. RocketMQ Backend (`quotes_rocketmq`)

Uses RocketMQ for distributed request queuing. **Queue-only** — no Set or Storage support.

```bash
# Start RocketMQ nameserver
docker run -d --name rocketmq-namesrv -p 9876:9876 apache/rocketmq:5.0 namesrv

# Start RocketMQ broker
docker run -d --name rocketmq-broker -p 10911:10911 -p 10909:10909 \
  --link rocketmq-namesrv \
  -e NAMESRV_ADDR=rocketmq-namesrv:9876 \
  apache/rocketmq:5.0 broker

scrapy crawl quotes_rocketmq
```

### Configuration Options

| Setting | Env Variable | Default | Description |
|---------|-------------|---------|-------------|
| `namesrv_address` | `SCRAPY_ROCKETMQ_NAMESRV_ADDRESS` | `localhost:9876` | RocketMQ nameserver address |
| `access_key` | `SCRAPY_ROCKETMQ_ACCESS_KEY` | — | Alibaba Cloud access key |
| `secret_key` | `SCRAPY_ROCKETMQ_SECRET_KEY` | — | Alibaba Cloud secret key |
| `consumer_group` | `SCRAPY_ROCKETMQ_CONSUMER_GROUP` | `scrapy-extension-consumer` | Consumer group |
| `producer_group` | `SCRAPY_ROCKETMQ_PRODUCER_GROUP` | `scrapy-extension-producer` | Producer group |
| `send_timeout` | `SCRAPY_ROCKETMQ_SEND_TIMEOUT` | `3000` | Send timeout in ms |

### RocketMQ Modes

**Standalone (default):**

```python
SCRAPY_BACKEND_TYPE = "rocketmq"
SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "localhost:9876"
```

**Alibaba Cloud RocketMQ:**

```python
SCRAPY_ROCKETMQ_MODE = "cloud"
SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "your-namesrv.addr.aliyun.com:8080"
SCRAPY_ROCKETMQ_ACCESS_KEY = "your_access_key"
SCRAPY_ROCKETMQ_SECRET_KEY = "your_secret_key"
```

### Important Limitations

RocketMQ only implements `QueueBackend`. The following operations raise `NotImplementedError`:
- `SetBackend.add()` — RocketMQ does not support atomic add-or-skip set operations
- `SetBackend.contains()` — RocketMQ does not support set membership queries
- `SetBackend.set_len()` — RocketMQ does not support set size queries
- `StorageBackend.retrieve()` — RocketMQ does not support point-in-time key retrieval

For deduplication, pair with Redis or MongoDB.

### Common Errors

**Connection refused** — verify RocketMQ is running:

```bash
docker ps | grep rocketmq
nc -zv localhost 9876
```

**Authentication failed** — check access_key and secret_key for cloud mode:

```bash
export SCRAPY_ROCKETMQ_ACCESS_KEY="your_access_key"
export SCRAPY_ROCKETMQ_SECRET_KEY="your_secret_key"
```

---

## 6. ElasticSearch Backend (`quotes_elasticsearch`)

Full-featured distributed crawling using ElasticSearch. Supports Queue + Set + Storage.

```bash
docker run -d --name elasticsearch -p 9200:9200 \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  elasticsearch:8.12.0

scrapy crawl quotes_elasticsearch
```

### Configuration Options

| Variable | Default | Description |
|----------|---------|-------------|
| `SCRAPY_ELASTICSEARCH_MODE` | `standalone` | `standalone` or `cloud` |
| `SCRAPY_ELASTICSEARCH_HOSTS` | `http://localhost:9200` | Comma-separated host URLs |
| `SCRAPY_ELASTICSEARCH_CLOUD_ID` | — | Elastic Cloud identifier |
| `SCRAPY_ELASTICSEARCH_API_KEY` | — | API key authentication |
| `SCRAPY_ELASTICSEARCH_QUEUE_INDEX` | `scrapy_queue` | Queue index name |
| `SCRAPY_ELASTICSEARCH_SET_INDEX` | `scrapy_set` | Dedup index name |
| `SCRAPY_ELASTICSEARCH_STORAGE_INDEX` | `scrapy_storage` | Storage index name |

### Cloud Mode

```bash
export SCRAPY_ELASTICSEARCH_MODE=cloud
export SCRAPY_ELASTICSEARCH_CLOUD_ID=your_cloud_id
export SCRAPY_ELASTICSEARCH_API_KEY=your_api_key
```

### Common Errors

**Connection refused** — verify ElasticSearch is running:

```bash
curl http://localhost:9200
```

**Authentication failure** — ensure credentials are set:

```bash
export SCRAPY_ELASTICSEARCH_API_KEY=your_api_key
```

---

## 7. Programmatic Configuration (`quotes_programmatic`)

Configures the backend entirely within the spider class using `backend_settings` dict,
instead of `settings.py`. Useful for multi-tenant crawlers or testing against
different backends without modifying project files.

```bash
scrapy crawl quotes_programmatic
```

### When to Use

| Approach | Use Case |
|---------|---------|
| `settings.py` | Project-wide defaults, single backend per deployment |
| `backend_settings` | Multi-tenant crawlers, testing, different configs per spider |

### All Redis `backend_settings` Fields

```python
backend_settings = {
    # Connection
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": None,

    # Timeouts
    "socket_timeout": 30.0,
    "socket_connect_timeout": 5.0,

    # Retry
    "retry_on_timeout": True,

    # Optional
    # "mode": "STANDALONE",
    # "ssl": False,
    # "ssl_cert_reqs": "required",
}
```

### Multi-Tenant Example

```python
class TenantASpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
    name = "tenant_a"
    backend_type = BackendType.REDIS
    backend_settings = {"host": "redis-a.internal", "db": 0}

class TenantBSpider(QuotesParsingMixin, BackendSpiderMixin, scrapy.Spider):
    name = "tenant_b"
    backend_type = BackendType.REDIS
    backend_settings = {"host": "redis-b.internal", "db": 0}
```

### Common Errors

**`AttributeError: 'XSpider' object has no attribute 'setup_backend'`** — must call
`self.setup_backend()` in `__init__`.

**Spiders sharing the same Redis instance** — use distinct `db` values to isolate:
`backend_settings = {"db": 0}` vs `backend_settings = {"db": 1}`.

---

## 8. Multi-Mode Deployment (`quotes_multi_mode`)

Demonstrates Redis Sentinel (high availability) and Cluster (sharding) configurations.

```bash
# Requires Redis Sentinel or Cluster running
scrapy crawl quotes_multi_mode
```

### Sentinel Mode (HA with Automatic Failover)

```python
SENTINEL_CONFIG = {
    "mode": "sentinel",
    "sentinels": ["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"],
    "sentinel_master_name": "mymaster",
    "sentinel_password": os.environ.get("REDIS_SENTINEL_PASSWORD", "changeme"),
    "password": os.environ.get("REDIS_PASSWORD", "changeme"),
    "db": 0,
}
```

### Cluster Mode (Horizontal Sharding)

```python
CLUSTER_CONFIG = {
    "mode": "cluster",
    "cluster_startup_nodes": ["node1:7000", "node2:7000", "node3:7000"],
    "password": os.environ.get("REDIS_PASSWORD", None),
    "cluster_max_redirects": 5,
}
```

### Docker Compose for Local Sentinel Testing

```yaml
services:
  redis-master:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --requirepass masterpass

  sentinel1:
    image: redis:7-alpine
    ports:
      - "26379:26379"
    command: |
      redis-sentinel --sentinel announce-ip sentinel1 \
        --sentinel monitor mymaster redis-master 6379 2 \
        --sentinel down-after-milliseconds mymaster 5000 \
        --sentinel failover-timeout mymaster 10000 \
        --sentinel auth-pass mymaster masterpass
```

### Common Errors

**`MasterNotFoundError: No master found`** — Sentinel cannot reach the master.
Check master container logs and network connectivity.

**`ClusterDownError: Cluster is not initialized`** — run cluster creation command
before use.

---

## 9. ConnectionManager API (`quotes_connection_manager`)

Low-level approach using `ConnectionManager` directly instead of `BackendSpiderMixin`.
Shows how to use `QueueBackend`, `SetBackend`, and `StorageBackend` interfaces
programmatically for custom deduplication and storage logic.

```bash
scrapy crawl quotes_connection_manager
```

### When to Use

| Aspect | `BackendSpiderMixin` | `ConnectionManager` |
|--------|---------------------|---------------------|
| Best for | Standard crawl workflows | Custom dedup, custom storage keys, multi-backend coordination |
| Boilerplate | Less | More |
| Flexibility | Fixed component roles | Full control |

### Key API Patterns

**Singleton manager access:**

```python
self._manager = ConnectionManager.get_manager(
    backend_type=BackendType.REDIS,
    settings={"host": "localhost", "port": 6379, "db": 0},
)
```

**Backend interface accessors:**

```python
queue_backend = self._manager.get_queue_backend()    # push, pop, queue_len
set_backend = self._manager.get_set_backend()       # add, contains, remove
storage_backend = self._manager.get_storage_backend() # store, retrieve, delete
```

**NotImplementedError guard (Kafka/RabbitMQ don't support Set/Storage):**

```python
try:
    set_backend = self._manager.get_set_backend()
except NotImplementedError:
    set_backend = None
```

**from_crawler() factory for signal registration:**

```python
@classmethod
def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider._on_spider_closed, signals.spider_closed)
    return spider
```

**Spider-identity guard in signal handlers:**

```python
def _on_spider_closed(self, spider, reason=""):
    if spider is self:
        self._manager.close()
```

### Custom Deduplication Example

```python
def parse(self, response):
    queue_backend = self._manager.get_queue_backend()
    set_backend = self._get_set_backend()

    for quote in response.css("div.quote"):
        item_key = f"quote:{quote_author}:{content_hash}"

        # Atomic dedup — False means already existed
        if set_backend is not None and not set_backend.add("seen_quotes", item_key.encode()):
            continue  # already scraped

        queue_backend.push("quote_queue", item_key.encode(), priority=0.0)
        yield item
```

### Common Errors

**Connection refused** — verify Redis is running: `redis-cli ping`

**Duplicate items despite deduplication** — verify the set key is consistent across
spider restarts. Redis keys persist between runs; flush with `redis-cli FLUSHDB`
for fresh state.

---

## 10. Basic Spiders (`quotes`, `quotes_crawl`)

Reference implementations without the scrapy-extension backend. Useful for
understanding Scrapy fundamentals before adding distributed components.

### QuotesSpider (Plain Spider)

```bash
cd examples
scrapy crawl quotes -o quotes.json
```

```python
import scrapy
from examples.items import QuoteItem

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com"]

    def parse(self, response):
        for quote in response.css("div.quote"):
            item = QuoteItem()
            item["text"] = quote.css("span.text::text").get()
            item["author"] = quote.css("small.author::text").get()
            item["tags"] = quote.css("div.tags a.tag::text").getall()
            yield item

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, self.parse)
```

### QuotesCrawlSpider (Rule-Based Spider)

```bash
scrapy crawl quotes_crawl -o quotes_crawl.json
```

```python
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

class QuotesCrawlSpider(CrawlSpider):
    name = "quotes_crawl"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com"]

    rules = (Rule(LinkExtractor(allow=r"/page/\d+"), callback="parse_item", follow=True),)

    def parse_item(self, response):
        for quote in response.css("div.quote"):
            yield {
                "text": quote.css("span.text::text").get(),
                "author": quote.css("small.author::text").get(),
                "tags": quote.css("div.tags a.tag::text").getall(),
            }
```

### Spider vs CrawlSpider

| Aspect | Spider | CrawlSpider |
|--------|--------|------------|
| Link following | Manual via `response.follow()` | Automatic via `Rule` objects |
| Pagination | Explicit conditional in `parse()` | Declared as pattern in `LinkExtractor` |
| Callback naming | Any method name | Cannot be named `parse` (reserved) |
| Flexibility | Full control | Rule-based |

### Adding scrapy-extension to Either

Enable in `settings.py`:

```python
SCHEDULER = "scrapy_extension.schedule.scheduler.BackendScheduler"
DUPEFILTER_CLASS = "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"
ITEM_PIPELINES = {"scrapy_extension.pipeline.pipeline.BackendPipeline": 300}
SCRAPY_BACKEND_TYPE = "redis"
```

### Common Errors

**`AllowedDomains list accepts only domains, not URLs`**:

```python
# Wrong:
allowed_domains = ["https://quotes.toscrape.com"]
# Correct:
allowed_domains = ["quotes.toscrape.com"]
```

**`CrawlSpider callback cannot be named 'parse'`** — use `parse_item` or any other name.

---

## Configuration

### settings.py (Recommended)

The default `settings.py` uses Redis. To switch backends:

```python
SCRAPY_BACKEND_TYPE = "redis"    # Default
SCRAPY_BACKEND_TYPE = "mongodb"
SCRAPY_BACKEND_TYPE = "kafka"    # Queue only
SCRAPY_BACKEND_TYPE = "rabbitmq" # Queue only
SCRAPY_BACKEND_TYPE = "elasticsearch"
```

### Environment Variables

All settings can be overridden via environment variables:

```bash
export SCRAPY_BACKEND_TYPE=redis
export SCRAPY_REDIS_HOST=localhost
export SCRAPY_REDIS_PORT=6379
```

### Programmatic (Per-Spider)

```python
class MySpider(BackendSpiderMixin, scrapy.Spider):
    backend_type = BackendType.REDIS
    backend_settings = {"host": "localhost", "port": 6379, "db": 0}
```

---

## Troubleshooting

**`RuntimeError: setup_backend() must be called`**
→ Ensure `self.setup_backend()` is called in `__init__` before accessing backend
components.

**`BackendConnectionError`**
→ Verify the backend is running and accessible. Check host/port in settings.

**`NotImplementedError` for `get_set_backend()` or `get_storage_backend()`**
→ Kafka and RabbitMQ only support Queue operations. Use Redis, MongoDB, or
ElasticSearch for full features.

**Spider not found**
→ Run from the `examples/` directory, not the repo root:

```bash
cd examples && scrapy list
```
