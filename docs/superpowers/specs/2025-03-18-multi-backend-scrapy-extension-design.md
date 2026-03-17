# Multi-Backend Scrapy Extension Design Spec

**Date:** 2025-03-18
**Status:** Draft
**Author:** Claude Code

## 1. Overview

A Scrapy extension providing distributed crawling capabilities with support for multiple
backends: Redis, MongoDB, Kafka, and RabbitMQ. Inspired by scrapy-redis but generalized
to work with any supported backend.

### 1.1 Goals

- Provide backend-agnostic distributed crawling components
- Support multiple backend deployment modes (standalone, clustered, HA)
- Type-safe configuration using pydantic-settings
- Lazy singleton connection management
- Compatible with standard Scrapy spiders via mixin

### 1.2 Non-Goals

- Custom spider implementations (only mixins provided)
- Backend-specific optimizations that break abstraction
- Automatic backend migration tools

### 1.3 MVP Scope

**Day 1 Requirements:**

- All 4 backends (Redis, MongoDB, Kafka, RabbitMQ) with full implementations
- All 5 components (Scheduler, DupeFilter, Pipeline, Queue, SpiderMixin) working with
  all backends
- Configuration via environment variables or direct Python instantiation
- Basic error handling with connection retry

**Out of Scope for MVP:**

- Async backend implementations
- Health check endpoints
- Metrics integration
- Automatic failover between backends
- Migration tools between backends

## 2. Architecture

### 2.1 High-Level Design

```
┌─────────────────────────────────────────────────────────┐
│                    Scrapy Components                    │
├─────────────┬──────────────┬─────────────┬──────────────┤
│  Scheduler  │ Dup Filter   │   Pipeline  │Spider Mixin  │
└──────┬──────┴──────┬───────┴──────┬──────┴──────┬───────┘
       │             │              │             │
       └─────────────┴──────┬───────┴─────────────┘
                            │
                    ┌───────▼───────┐
                    │BackendInterface│
                    │  (abstract)    │
                    └───────┬───────┘
         ┌────────────────┬─┴────────────────┐
         │                │                  │
    ┌────▼────┐     ┌────▼────┐      ┌──────▼──────┐
    │  Redis  │     │ MongoDB │      │    Kafka    │
    │Backend  │     │Backend  │      │   Backend   │
    └─────────┘     └─────────┘      └─────────────┘
    ┌─────────┐     ┌─────────┐      ┌─────────────┐
    │Sentinel │     │Replica  │      │   RabbitMQ  │
    │ Cluster │     │  Set    │      │   Backend   │
    └─────────┘     └─────────┘      └─────────────┘
```

### 2.2 Key Design Decisions

| Decision              | Choice                               | Rationale                                                            |
|-----------------------|--------------------------------------|----------------------------------------------------------------------|
| Architecture Pattern  | Unified Backend Interface            | Components are backend-agnostic, backends implement common interface |
| Connection Management | Lazy Singleton                       | Shared connection across components, created on first use            |
| Configuration         | Pydantic Settings with Discriminator | Type-safe, validates at startup, clear error messages                |
| Spider URL Source     | Poll + Pub/Sub                       | Flexible for different use cases and backend capabilities            |
| Serialization         | JSON                                 | Safe, human-readable, no code execution risk                         |

## 3. Components

### 3.1 Backend Interface

```python
from abc import ABC, abstractmethod
from typing import Iterator


class BackendInterface(ABC):
    """Abstract interface for all backend implementations."""

    # ========== Connection Lifecycle ==========
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to backend."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connection to backend."""
        pass

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if backend is connected."""
        pass

    # ========== Queue Operations ==========
    @abstractmethod
    def push(self, queue_name: str, item: bytes, priority: int = 0) -> None:
        """Push item to queue with optional priority."""
        pass

    @abstractmethod
    def pop(self, queue_name: str, timeout: int = 0) -> bytes | None:
        """Pop item from queue. Block for timeout seconds if empty (0 = non-blocking)."""
        pass

    @abstractmethod
    def queue_len(self, queue_name: str) -> int:
        """Get queue length."""
        pass

    @abstractmethod
    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue."""
        pass

    # ========== Duplicate Filter Operations ==========
    @abstractmethod
    def add_fingerprint(self, key: str, fingerprint: str) -> bool:
        """Add fingerprint to set. Return True if added, False if already exists."""
        pass

    @abstractmethod
    def is_fingerprint_exists(self, key: str, fingerprint: str) -> bool:
        """Check if fingerprint exists."""
        pass

    @abstractmethod
    def clear_fingerprints(self, key: str) -> None:
        """Clear all fingerprints for key."""
        pass

    # ========== Pipeline/Storage Operations ==========
    @abstractmethod
    def store_item(self, collection: str, item: dict) -> None:
        """Store item in collection/table/topic."""
        pass

    @abstractmethod
    def store_items_batch(self, collection: str, items: list[dict]) -> None:
        """Store multiple items (batch insert for efficiency)."""
        pass

    # ========== Spider Start URL Operations ==========
    @abstractmethod
    def subscribe(self, channel: str) -> Iterator[bytes]:
        """Subscribe to channel for pub/sub mode. Yields messages.

        Error Handling:
        - Connection errors: Should retry with exponential backoff
        - Graceful shutdown: Should raise StopIteration on close()
        - Backend-specific: Redis uses pub/sub, Kafka uses consumer poll
        """
        pass

    @abstractmethod
    def publish(self, channel: str, message: bytes) -> None:
        """Publish message to channel."""
        pass
```

### 3.2 Backend Factory

```python
from typing import TypeVar, Generic

T = TypeVar('T', bound='BackendInterface')


class BackendFactory:
    """Factory for creating and caching backend instances."""

    _instances: dict[str, BackendInterface] = {}
    _lock = threading.Lock()

    @classmethod
    def get_backend(cls, config: BackendConfig | None = None) -> BackendInterface:
        """Get or create backend instance.

        Uses lazy initialization - connection is established on first use.
        Same config returns same instance (singleton per config).
        """
        pass

    @classmethod
    def clear_cache(cls) -> None:
        """Clear all cached backend instances."""
        pass
```

### 3.3 Scheduler

```python
from scrapy.core.scheduler import BaseScheduler
from scrapy.http import Request
import json


class DistributedScheduler(BaseScheduler):
    """Distributed scheduler using configurable backend."""

    def __init__(
            self,
            backend: BackendInterface,
            queue_key: str,
            dupefilter_key: str,
            idle_before_close: int = 0,
            stats: StatsCollector | None = None,
    ):
        self.backend = backend
        self.queue = PriorityQueue(backend, queue_key)
        self.dupefilter = RFPDupeFilter(backend, dupefilter_key)
        self.idle_before_close = idle_before_close
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> DistributedScheduler:
        """Factory method required by Scrapy."""
        settings = crawler.settings
        backend = BackendFactory.get_backend()
        return cls(
            backend=backend,
            queue_key=settings.get("SCHEDULER_QUEUE_KEY", "{spider}:requests"),
            dupefilter_key=settings.get("DUPEFILTER_KEY", "{spider}:dupefilter"),
            idle_before_close=settings.getint("SCHEDULER_IDLE_BEFORE_CLOSE", 0),
            stats=crawler.stats,
        )

    def open(self, spider: Spider) -> None:
        """Called when spider opens."""
        self.queue_key = self.queue_key.format(spider=spider.name)
        self.dupefilter_key = self.dupefilter_key.format(spider=spider.name)
        self.spider = spider

    def close(self, reason: str) -> None:
        """Called when spider closes."""
        pass

    def has_pending_requests(self) -> bool:
        return self.queue.len() > 0

    def enqueue_request(self, request: Request) -> bool:
        if not request.dont_filter and self.dupefilter.request_seen(request):
            self.dupefilter.log(request, self.spider)
            return False
        self.queue.push(self._encode_request(request), priority=request.priority)
        if self.stats:
            self.stats.inc_value("scheduler/enqueued", spider=self.spider)
        return True

    def next_request(self) -> Request | None:
        data = self.queue.pop(timeout=self.idle_before_close)
        if data:
            if self.stats:
                self.stats.inc_value("scheduler/dequeued", spider=self.spider)
            return self._decode_request(data)
        return None

    def _encode_request(self, request: Request) -> bytes:
        """Serialize request to JSON bytes."""
        return json.dumps(request_to_dict(request, spider=self.spider)).encode("utf-8")

    def _decode_request(self, data: bytes) -> Request:
        """Deserialize JSON bytes to request."""
        return request_from_dict(json.loads(data.decode("utf-8")), spider=self.spider)
```

### 3.4 Duplicate Filter

```python
from scrapy.dupefilters import BaseDupeFilter
from scrapy.http import Request
from scrapy.utils.request import request_fingerprint


class BackendDupeFilter(BaseDupeFilter):
    """Request duplicate filter using backend storage."""

    def __init__(
            self,
            backend: BackendInterface,
            key: str,
            debug: bool = False,
    ):
        self.backend = backend
        self.key = key
        self.debug = debug
        self.logdupes = True

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> BackendDupeFilter:
        settings = crawler.settings
        backend = BackendFactory.get_backend()
        return cls(
            backend=backend,
            key=settings.get("DUPEFILTER_KEY", "{spider}:dupefilter"),
            debug=settings.getbool("DUPEFILTER_DEBUG"),
        )

    def request_seen(self, request: Request) -> bool:
        """Return True if request has been seen before."""
        fp = request_fingerprint(request)
        return not self.backend.add_fingerprint(self.key, fp)

    def close(self, reason: str) -> None:
        """Clear fingerprints on spider close (optional)."""
        pass

    def log(self, request: Request, spider: Spider) -> None:
        """Log duplicate request."""
        if self.debug:
            msg = f"Filtered duplicate request: {request}"
            spider.logger.debug(msg)
```

### 3.5 Pipeline

```python
from scrapy import Item


class BackendPipeline:
    """Item pipeline using backend storage."""

    def __init__(
            self,
            backend: BackendInterface,
            collection: str,
            batch_size: int = 100,
    ):
        self.backend = backend
        self.collection = collection
        self.batch_size = batch_size
        self.batch: list[dict] = []

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> BackendPipeline:
        settings = crawler.settings
        backend = BackendFactory.get_backend()
        return cls(
            backend=backend,
            collection=settings.get("PIPELINE_COLLECTION", "{spider}:items"),
            batch_size=settings.getint("PIPELINE_BATCH_SIZE", 100),
        )

    def open_spider(self, spider: Spider) -> None:
        self.collection = self.collection.format(spider=spider.name)
        self.spider = spider

    def close_spider(self, spider: Spider) -> None:
        """Flush remaining items on close."""
        if self.batch:
            self._flush_batch()

    def process_item(self, item: Item, spider: Spider) -> Item:
        """Process and store item."""
        self.batch.append(dict(item))
        if len(self.batch) >= self.batch_size:
            self._flush_batch()
        return item

    def _flush_batch(self) -> None:
        self.backend.store_items_batch(self.collection, self.batch)
        self.batch = []
```

### 3.6 Queue

```python
class PriorityQueue:
    """Priority queue using backend storage."""

    def __init__(self, backend: BackendInterface, key: str):
        self.backend = backend
        self.key = key

    def push(self, item: bytes, priority: int = 0) -> None:
        """Push item with priority (higher = more important)."""
        self.backend.push(self.key, item, priority)

    def pop(self, timeout: int = 0) -> bytes | None:
        """Pop highest priority item. Block for timeout seconds if empty."""
        return self.backend.pop(self.key, timeout)

    def len(self) -> int:
        """Get queue length."""
        return self.backend.queue_len(self.key)

    def clear(self) -> None:
        """Clear all items."""
        self.backend.clear_queue(self.key)
```

### 3.7 Spider Mixin

```python
from scrapy import Spider, Request
from typing import Literal


class BackendSpiderMixin:
    """Mixin for spiders that read start URLs from backend."""

    start_urls_key: str = "{spider}:start_urls"
    start_urls_mode: Literal["poll", "pubsub", "both"] = "poll"
    poll_interval: int = 5  # seconds
    max_poll_empty: int = 10  # max empty polls before stopping

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._backend: BackendInterface | None = None
        self._empty_poll_count = 0

    @property
    def backend(self) -> BackendInterface:
        if self._backend is None:
            self._backend = BackendFactory.get_backend()
        return self._backend

    def start_requests(self):
        """Generate start requests from backend."""
        # First yield any static start_urls
        yield from super().start_requests()

        # Then get dynamic URLs from backend
        key = self.start_urls_key.format(spider=self.name)

        if self.start_urls_mode in ("poll", "both"):
            yield from self._poll_start_urls(key)

        if self.start_urls_mode in ("pubsub", "both"):
            yield from self._subscribe_start_urls(key)

    def _poll_start_urls(self, key: str):
        """Poll backend queue for new start URLs."""
        while self._empty_poll_count < self.max_poll_empty:
            data = self.backend.pop(key, timeout=self.poll_interval)
            if data:
                self._empty_poll_count = 0
                url = data.decode("utf-8")
                yield Request(url, callback=self.parse)
            else:
                self._empty_poll_count += 1
                if self.start_urls_mode == "poll":
                    break  # Only polling, exit when empty

    def _subscribe_start_urls(self, key: str):
        """Subscribe to backend channel for new URLs."""
        channel = f"{key}:channel"
        try:
            for message in self.backend.subscribe(channel):
                url = message.decode("utf-8")
                yield Request(url, callback=self.parse)
        except Exception as e:
            self.logger.error(f"Subscribe error: {e}")

    def add_start_url(self, url: str) -> None:
        """Add URL to backend queue (can be called externally)."""
        key = self.start_urls_key.format(spider=self.name)
        self.backend.push(key, url.encode("utf-8"))

        # Also publish if using pubsub
        if self.start_urls_mode in ("pubsub", "both"):
            channel = f"{key}:channel"
            self.backend.publish(channel, url.encode("utf-8"))
```

## 4. Backend Implementations

### 4.1 Redis Backend

Supports: standalone, sentinel, cluster

**Implementation Requirements:**

- Queue: Use Redis Sorted Sets (ZADD/ZRANGEBYSCORE) for priority queue
- DupeFilter: Use Redis Sets (SADD) - SADD returns 0 if member exists
- Pipeline: Use Redis Lists (LPUSH) or Hashes for item storage
- Pub/Sub: Use Redis PUBLISH/SUBSCRIBE commands

```python
from redis import Redis, Sentinel, RedisCluster
from redis.exceptions import RedisError


class RedisBackend(BackendInterface):
    """Redis backend implementation."""

    def __init__(self, config: RedisConfig):
        self.config = config
        self._client: Redis | Sentinel | RedisCluster | None = None
        self._pubsub = None

    def connect(self) -> None:
        if self.config.mode == "standalone":
            self._client = Redis(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=self.config.db,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                retry_on_timeout=self.config.retry_on_timeout,
            )
        elif self.config.mode == "sentinel":
            sentinel = Sentinel(
                [(n.host, n.port) for n in self.config.sentinels],
                password=self.config.password,
                socket_timeout=self.config.socket_timeout,
            )
            self._client = sentinel.master_for(self.config.master_name)
        elif self.config.mode == "cluster":
            from redis.cluster import RedisCluster
            self._client = RedisCluster(
                startup_nodes=[{"host": n.host, "port": n.port} for n in
                               self.config.startup_nodes],
                password=self.config.password,
                skip_full_coverage_check=self.config.skip_full_coverage_check,
            )

    @property
    def client(self) -> Redis:
        if self._client is None:
            self.connect()
        return self._client  # type: ignore

    def close(self) -> None:
        if self._pubsub:
            self._pubsub.close()
        if self._client:
            self._client.close()
            self._client = None

    @property
    def is_connected(self) -> bool:
        try:
            return self.client.ping() if self._client else False
        except RedisError:
            return False

    # Queue operations using Sorted Sets
    def push(self, queue_name: str, item: bytes, priority: int = 0) -> None:
        self.client.zadd(queue_name, {item: priority})

    def pop(self, queue_name: str, timeout: int = 0) -> bytes | None:
        if timeout > 0:
            # Use BZPOPMAX for blocking pop
            result = self.client.bzpopmax(queue_name, timeout=timeout)
            return result[1] if result else None
        else:
            result = self.client.zpopmax(queue_name)
            return result[0][0] if result else None

    def queue_len(self, queue_name: str) -> int:
        return self.client.zcard(queue_name)

    def clear_queue(self, queue_name: str) -> None:
        self.client.delete(queue_name)

    # Duplicate filter using Sets
    def add_fingerprint(self, key: str, fingerprint: str) -> bool:
        # SADD returns number of elements added (0 if already exists)
        return self.client.sadd(key, fingerprint) == 1

    def is_fingerprint_exists(self, key: str, fingerprint: str) -> bool:
        return self.client.sismember(key, fingerprint)

    def clear_fingerprints(self, key: str) -> None:
        self.client.delete(key)

    # Pipeline using Lists
    def store_item(self, collection: str, item: dict) -> None:
        self.client.lpush(collection, json.dumps(item))

    def store_items_batch(self, collection: str, items: list[dict]) -> None:
        pipe = self.client.pipeline()
        for item in items:
            pipe.lpush(collection, json.dumps(item))
        pipe.execute()

    # Pub/Sub operations
    def subscribe(self, channel: str) -> Iterator[bytes]:
        self._pubsub = self.client.pubsub()
        self._pubsub.subscribe(channel)
        try:
            for message in self._pubsub.listen():
                if message["type"] == "message":
                    yield message["data"]
                elif message["type"] == "unsubscribe":
                    break
        except RedisError as e:
            logger.error(f"Redis subscribe error: {e}")
            raise StopIteration

    def publish(self, channel: str, message: bytes) -> None:
        self.client.publish(channel, message)
```

### 4.2 MongoDB Backend

Supports: standalone, replica set, sharded

**Implementation Requirements:**

- Queue: Use capped collection with natural ordering, or standard collection with sort
  by priority
- DupeFilter: Use unique index on (key, fingerprint) compound field
- Pipeline: Use standard collection for items
- Pub/Sub: Use tailable cursor on capped collection (MongoDB change streams not
  available on all deployments)

```python
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, ConnectionFailure


class MongoBackend(BackendInterface):
    """MongoDB backend implementation."""

    def __init__(self, config: MongoConfig):
        self.config = config
        self._client: MongoClient | None = None
        self._db = None

    def connect(self) -> None:
        self._client = MongoClient(
            self.config.uri,
            maxPoolSize=self.config.max_pool_size,
            minPoolSize=self.config.min_pool_size,
            maxIdleTimeMS=self.config.max_idle_time_ms,
            connectTimeoutMS=self.config.connect_timeout_ms,
            serverSelectionTimeoutMS=self.config.server_selection_timeout_ms,
            socketTimeoutMS=self.config.socket_timeout_ms,
        )
        self._db = self._client[self.config.database]

    @property
    def client(self) -> MongoClient:
        if self._client is None:
            self.connect()
        return self._client  # type: ignore

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    @property
    def is_connected(self) -> bool:
        try:
            self.client.admin.command('ping')
            return True
        except ConnectionFailure:
            return False

    # Queue operations using standard collection with priority field
    def push(self, queue_name: str, item: bytes, priority: int = 0) -> None:
        collection = self._db[queue_name]
        collection.insert_one({
            'item': item,
            'priority': priority,
            'created_at': datetime.utcnow()
        })

    def pop(self, queue_name: str, timeout: int = 0) -> bytes | None:
        collection = self._db[queue_name]
        # Find and delete highest priority item
        result = collection.find_one_and_delete(
            {},
            sort=[('priority', DESCENDING), ('created_at', ASCENDING)]
        )
        return result['item'] if result else None

    def queue_len(self, queue_name: str) -> int:
        return self._db[queue_name].estimated_document_count()

    def clear_queue(self, queue_name: str) -> None:
        self._db[queue_name].drop()

    # Duplicate filter using unique index
    def add_fingerprint(self, key: str, fingerprint: str) -> bool:
        collection = self._db[key]
        try:
            collection.insert_one({
                'fingerprint': fingerprint,
                'created_at': datetime.utcnow()
            })
            return True
        except DuplicateKeyError:
            return False

    def is_fingerprint_exists(self, key: str, fingerprint: str) -> bool:
        return self._db[key].find_one({'fingerprint': fingerprint}) is not None

    def clear_fingerprints(self, key: str) -> None:
        self._db[key].drop()

    # Pipeline operations
    def store_item(self, collection: str, item: dict) -> None:
        self._db[collection].insert_one(item)

    def store_items_batch(self, collection: str, items: list[dict]) -> None:
        if items:
            self._db[collection].insert_many(items)

    # Pub/Sub using tailable cursor on capped collection
    def subscribe(self, channel: str) -> Iterator[bytes]:
        # Ensure capped collection exists
        if channel not in self._db.list_collection_names():
            self._db.create_collection(channel, capped=True, size=1000000, max=1000)

        collection = self._db[channel]
        last_id = None

        while True:
            query = {'_id': {'$gt': last_id}} if last_id else {}
            cursor = collection.find(query,
                                     cursor_type=pymongo.CursorType.TAILABLE_AWAIT)

            try:
                for doc in cursor:
                    last_id = doc['_id']
                    yield doc['message']
            except StopIteration:
                continue
            except pymongo.errors.PyMongoError as e:
                logger.error(f"MongoDB subscribe error: {e}")
                raise StopIteration

    def publish(self, channel: str, message: bytes) -> None:
        # Ensure capped collection exists
        if channel not in self._db.list_collection_names():
            self._db.create_collection(channel, capped=True, size=1000000, max=1000)
        self._db[channel].insert_one({'message': message})
```

### 4.3 Kafka Backend

**Implementation Requirements:**

- Queue: Use Kafka topics with priority-based partitioning
- DupeFilter: Use compacted topic or external dedup store (Kafka doesn't have native
  sets)
- Pipeline: Use Kafka topics for item streaming
- Pub/Sub: Native Kafka consumer groups

```python
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import json


class KafkaBackend(BackendInterface):
    """Kafka backend implementation."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self._producer: Producer | None = None
        self._consumers: dict[str, Consumer] = {}
        self._dedup_store: dict[str, set] = {}  # In-memory for dedup (not distributed)

    def connect(self) -> None:
        producer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'client.id': f"{self.config.client_id}-producer",
            'acks': self.config.producer_acks,
            'retries': self.config.producer_retries,
            'batch.size': self.config.producer_batch_size,
        }
        if self.config.security_protocol != 'PLAINTEXT':
            producer_config.update({
                'security.protocol': self.config.security_protocol,
                'sasl.mechanism': self.config.sasl_mechanism,
                'sasl.username': self.config.sasl_username,
                'sasl.password': self.config.sasl_password,
            })
        self._producer = Producer(producer_config)

    def _get_consumer(self, topic: str) -> Consumer:
        if topic not in self._consumers:
            config = {
                'bootstrap.servers': self.config.bootstrap_servers,
                'group.id': f"{self.config.consumer_group_id}-{topic}",
                'auto.offset.reset': self.config.consumer_auto_offset_reset,
                'enable.auto.commit': self.config.consumer_enable_auto_commit,
            }
            consumer = Consumer(config)
            consumer.subscribe([topic])
            self._consumers[topic] = consumer
        return self._consumers[topic]

    def close(self) -> None:
        if self._producer:
            self._producer.flush()
            self._producer = None
        for consumer in self._consumers.values():
            consumer.close()
        self._consumers.clear()

    @property
    def is_connected(self) -> bool:
        try:
            # Produce a test message to verify connection
            return self._producer is not None
        except Exception:
            return False

    # Queue operations using Kafka topics
    def push(self, queue_name: str, item: bytes, priority: int = 0) -> None:
        # Use partition key based on priority (higher priority = lower partition number)
        partition = max(0, 10 - priority) if priority < 10 else 0
        self._producer.produce(
            queue_name,
            value=item,
            partition=partition,
            callback=self._delivery_report
        )
        self._producer.poll(0)

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')

    def pop(self, queue_name: str, timeout: int = 0) -> bytes | None:
        consumer = self._get_consumer(queue_name)
        msg = consumer.poll(timeout=timeout * 1000 if timeout > 0 else 1000)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())
        return msg.value()

    def queue_len(self, queue_name: str) -> int:
        # Kafka doesn't expose queue length directly
        # Return 0 or estimate based on consumer lag
        return 0

    def clear_queue(self, queue_name: str) -> None:
        # Kafka topics cannot be cleared, only deleted
        logger.warning(f"Cannot clear Kafka topic {queue_name}")

    # Duplicate filter using in-memory set (limited, not distributed)
    def add_fingerprint(self, key: str, fingerprint: str) -> bool:
        if key not in self._dedup_store:
            self._dedup_store[key] = set()
        if fingerprint in self._dedup_store[key]:
            return False
        self._dedup_store[key].add(fingerprint)
        return True

    def is_fingerprint_exists(self, key: str, fingerprint: str) -> bool:
        return fingerprint in self._dedup_store.get(key, set())

    def clear_fingerprints(self, key: str) -> None:
        if key in self._dedup_store:
            del self._dedup_store[key]

    # Pipeline operations
    def store_item(self, collection: str, item: dict) -> None:
        self._producer.produce(
            collection,
            value=json.dumps(item).encode('utf-8'),
            callback=self._delivery_report
        )
        self._producer.poll(0)

    def store_items_batch(self, collection: str, items: list[dict]) -> None:
        for item in items:
            self._producer.produce(
                collection,
                value=json.dumps(item).encode('utf-8'),
                callback=self._delivery_report
            )
        self._producer.flush()

    # Pub/Sub using Kafka consumer groups
    def subscribe(self, channel: str) -> Iterator[bytes]:
        consumer = self._get_consumer(channel)
        try:
            while True:
                msg = consumer.poll(timeout=1000)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                yield msg.value()
        except Exception as e:
            logger.error(f"Kafka subscribe error: {e}")
            raise StopIteration

    def publish(self, channel: str, message: bytes) -> None:
        self._producer.produce(channel, value=message)
        self._producer.poll(0)
```

### 4.4 RabbitMQ Backend

**Implementation Requirements:**

- Queue: Use RabbitMQ priority queues (max-priority header)
- DupeFilter: Use in-memory dedup or external cache (RabbitMQ doesn't have native dedup)
- Pipeline: Use standard queues for item storage
- Pub/Sub: Use fanout exchanges

```python
import pika
from pika.adapters.blocking_connection import BlockingChannel
import json


class RabbitMQBackend(BackendInterface):
    """RabbitMQ backend implementation."""

    def __init__(self, config: RabbitMQConfig):
        self.config = config
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None
        self._dedup_store: dict[str, set] = {}  # In-memory dedup

    def connect(self) -> None:
        credentials = pika.PlainCredentials(
            self.config.username,
            self.config.password
        )
        parameters = pika.ConnectionParameters(
            host=self.config.host,
            port=self.config.port,
            virtual_host=self.config.virtual_host,
            credentials=credentials,
            connection_attempts=self.config.connection_attempts,
            retry_delay=self.config.retry_delay,
            socket_timeout=self.config.socket_timeout,
        )
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=self.config.prefetch_count)

    @property
    def channel(self) -> BlockingChannel:
        if self._channel is None:
            self.connect()
        return self._channel  # type: ignore

    def close(self) -> None:
        if self._channel and not self._channel.is_closed:
            self._channel.close()
        if self._connection and not self._connection.is_closed:
            self._connection.close()
        self._channel = None
        self._connection = None

    @property
    def is_connected(self) -> bool:
        return (
                self._connection is not None
                and self._connection.is_open
                and self._channel is not None
                and self._channel.is_open
        )

    # Queue operations using RabbitMQ priority queues
    def push(self, queue_name: str, item: bytes, priority: int = 0) -> None:
        # Declare queue with priority support
        self.channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={'x-max-priority': 10}
        )
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=item,
            properties=pika.BasicProperties(
                priority=min(priority, 10),
                delivery_mode=self.config.delivery_mode
            )
        )

    def pop(self, queue_name: str, timeout: int = 0) -> bytes | None:
        self.channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={'x-max-priority': 10}
        )

        if timeout > 0:
            # Use blocking consume with timeout
            method_frame, _, body = self.channel.basic_get(queue=queue_name,
                                                           auto_ack=True)
            if method_frame:
                return body
            # Wait for message
            start_time = time.time()
            while time.time() - start_time < timeout:
                method_frame, _, body = self.channel.basic_get(queue=queue_name,
                                                               auto_ack=True)
                if method_frame:
                    return body
                time.sleep(0.1)
            return None
        else:
            method_frame, _, body = self.channel.basic_get(queue=queue_name,
                                                           auto_ack=True)
            return body if method_frame else None

    def queue_len(self, queue_name: str) -> int:
        queue = self.channel.queue_declare(queue=queue_name, passive=True)
        return queue.method.message_count

    def clear_queue(self, queue_name: str) -> None:
        self.channel.queue_purge(queue=queue_name)

    # Duplicate filter using in-memory set
    def add_fingerprint(self, key: str, fingerprint: str) -> bool:
        if key not in self._dedup_store:
            self._dedup_store[key] = set()
        if fingerprint in self._dedup_store[key]:
            return False
        self._dedup_store[key].add(fingerprint)
        return True

    def is_fingerprint_exists(self, key: str, fingerprint: str) -> bool:
        return fingerprint in self._dedup_store.get(key, set())

    def clear_fingerprints(self, key: str) -> None:
        if key in self._dedup_store:
            del self._dedup_store[key]

    # Pipeline operations
    def store_item(self, collection: str, item: dict) -> None:
        self.channel.queue_declare(queue=collection, durable=True)
        self.channel.basic_publish(
            exchange='',
            routing_key=collection,
            body=json.dumps(item).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=self.config.delivery_mode
            )
        )

    def store_items_batch(self, collection: str, items: list[dict]) -> None:
        self.channel.queue_declare(queue=collection, durable=True)
        for item in items:
            self.channel.basic_publish(
                exchange='',
                routing_key=collection,
                body=json.dumps(item).encode('utf-8'),
                properties=pika.BasicProperties(
                    delivery_mode=self.config.delivery_mode
                )
            )

    # Pub/Sub using fanout exchange
    def subscribe(self, channel: str) -> Iterator[bytes]:
        # Create exclusive queue for this consumer
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        # Bind to fanout exchange
        self.channel.exchange_declare(exchange=channel, exchange_type='fanout')
        self.channel.queue_bind(queue=queue_name, exchange=channel)

        # Consume messages
        for method_frame, _, body in self.channel.consume(queue_name, auto_ack=True):
            yield body

    def publish(self, channel: str, message: bytes) -> None:
        self.channel.exchange_declare(exchange=channel, exchange_type='fanout')
        self.channel.basic_publish(exchange=channel, routing_key='', body=message)
```

## 5. Configuration

### 5.1 Base Configuration

```python
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import Literal, LiteralString


class BackendType(str, Enum):
    REDIS = "redis"
    MONGODB = "mongodb"
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"
```

### 5.2 Redis Configuration

```python
class RedisMode(str, Enum):
    STANDALONE = "standalone"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"


class SentinelNode(BaseModel):
    host: str
    port: int = 26379


class RedisConfig(BaseModel):
    backend_type: Literal[BackendType.REDIS] = BackendType.REDIS
    mode: RedisMode = RedisMode.STANDALONE

    # Standalone
    host: str = "localhost"
    port: int = 6379
    password: str | None = None
    db: int = 0

    # Sentinel
    sentinels: list[SentinelNode] = Field(default_factory=list)
    master_name: str = "mymaster"

    # Cluster
    startup_nodes: list[SentinelNode] = Field(default_factory=list)
    skip_full_coverage_check: bool = False

    # Common
    socket_timeout: int = 30
    socket_connect_timeout: int = 30
    retry_on_timeout: bool = True
```

### 5.3 MongoDB Configuration

```python
class MongoMode(str, Enum):
    STANDALONE = "standalone"
    REPLICA_SET = "replica_set"
    SHARDED = "sharded"


class MongoConfig(BaseModel):
    backend_type: Literal[BackendType.MONGODB] = BackendType.MONGODB
    mode: MongoMode = MongoMode.STANDALONE

    uri: str = "mongodb://localhost:27017"
    database: str = "scrapy"

    # Connection pool
    max_pool_size: int = 100
    min_pool_size: int = 10
    max_idle_time_ms: int = 60000

    # Timeouts
    connect_timeout_ms: int = 10000
    server_selection_timeout_ms: int = 30000
    socket_timeout_ms: int = 30000
```

### 5.4 Kafka Configuration

```python
class KafkaConfig(BaseModel):
    backend_type: Literal[BackendType.KAFKA] = BackendType.KAFKA

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "scrapy-extension"

    # Producer
    producer_acks: str = "all"
    producer_retries: int = 3
    producer_batch_size: int = 16384

    # Consumer
    consumer_group_id: str = "scrapy-group"
    consumer_auto_offset_reset: str = "earliest"
    consumer_enable_auto_commit: bool = True

    # Security
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
```

### 5.5 RabbitMQ Configuration

```python
class RabbitMQConfig(BaseModel):
    backend_type: Literal[BackendType.RABBITMQ] = BackendType.RABBITMQ

    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"

    # Connection
    connection_attempts: int = 3
    retry_delay: int = 5
    socket_timeout: int = 10

    # Channel
    prefetch_count: int = 1
    delivery_mode: int = 2  # persistent
```

### 5.6 Main Settings

```python
class ScrapyExtensionSettings(BaseSettings):
    """Main settings class for scrapy-extension."""

    backend: RedisConfig | MongoConfig | KafkaConfig | RabbitMQConfig = Field(
        ..., discriminator="backend_type"
    )

    # Scheduler settings
    scheduler_queue_key: str = "{spider}:requests"
    scheduler_dupefilter_key: str = "{spider}:dupefilter"
    scheduler_idle_before_close: int = 0

    # Pipeline settings
    pipeline_collection: str = "{spider}:items"
    pipeline_batch_size: int = 100

    # Spider mixin settings
    spider_start_urls_key: str = "{spider}:start_urls"
    spider_start_urls_mode: Literal["poll", "pubsub", "both"] = "poll"
    spider_poll_interval: int = 5
    spider_max_poll_empty: int = 10

    class Config:
        env_nested_delimiter = "__"
        env_prefix = "SCRAPY_EXT_"

    @property
    def backend_instance(self) -> BackendInterface:
        """Get configured backend instance."""
        return BackendFactory.get_backend(self.backend)
```

### 5.7 Configuration Loading

**Important:** Scrapy's settings system requires serializable values. Pydantic models
cannot be directly assigned to Scrapy settings.

**Approach 1: Environment Variables (Recommended)**

```python
# settings.py - Standard Scrapy settings file
import os

# Configure via env vars, parsed by pydantic-settings
os.environ["SCRAPY_EXT_BACKEND__BACKEND_TYPE"] = "redis"
os.environ["SCRAPY_EXT_BACKEND__HOST"] = "localhost"
```

**Approach 2: Python Instantiation in Extension Setup**

```python
# extensions.py or spider code
from scrapy_extension import BackendFactory, RedisConfig

# Direct Python instantiation (bypasses Scrapy settings)
config = RedisConfig(host="localhost", port=6379)
backend = BackendFactory.get_backend(config)
```

**Approach 3: Custom Settings Middleware**

```python
# Middleware that converts Scrapy dict settings to Pydantic models
class ExtensionSettingsMiddleware:
    def __init__(self, crawler):
        # Parse crawler.settings dict into Pydantic model
        settings_dict = crawler.settings.getdict("SCRAPY_EXT_BACKEND")
        self.config = parse_backend_config(settings_dict)
```

**Configuration Flow:**

1. User provides config via env vars or direct Python
2. `ScrapyExtensionSettings` parses and validates via pydantic-settings
3. `BackendFactory.get_backend()` creates singleton instance
4. Components receive backend instance via `from_crawler()`

## 6. Project Structure

```
src/scrapy_extension/
├── __init__.py                    # Public API exports
├── settings.py                    # Pydantic settings classes
├── backend/
│   ├── __init__.py
│   ├── interface.py               # BackendInterface ABC
│   ├── factory.py                 # BackendFactory (lazy singleton)
│   ├── redis/
│   │   ├── __init__.py
│   │   ├── backend.py             # RedisBackend
│   │   └── config.py              # RedisConfig
│   ├── mongo/
│   │   ├── __init__.py
│   │   ├── backend.py             # MongoBackend
│   │   └── config.py              # MongoConfig
│   ├── kafka/
│   │   ├── __init__.py
│   │   ├── backend.py             # KafkaBackend
│   │   └── config.py              # KafkaConfig
│   └── rabbitmq/
│       ├── __init__.py
│       ├── backend.py             # RabbitMQBackend
│       └── config.py              # RabbitMQConfig
├── components/
│   ├── __init__.py
│   ├── scheduler.py               # DistributedScheduler
│   ├── dupefilter.py              # BackendDupeFilter
│   ├── pipeline.py                # BackendPipeline
│   ├── queue.py                   # PriorityQueue
│   └── spider.py                  # BackendSpiderMixin
└── utils/
    ├── __init__.py
    ├── request.py                 # request_to_dict / request_from_dict
    └── fingerprint.py             # fingerprint helpers
```

## 7. Usage Examples

### 7.1 Basic Usage (Redis)

**Configuration via Environment Variables:**

```bash
export SCRAPY_EXT_BACKEND__BACKEND_TYPE="redis"
export SCRAPY_EXT_BACKEND__MODE="cluster"
export SCRAPY_EXT_BACKEND__STARTUP_NODES='[{"host": "redis1.example.com", "port": 6379}]'
export SCRAPY_EXT_BACKEND__PASSWORD="secret"
```

**Scrapy Settings:**

```python
# settings.py
EXTENSIONS = {
    "scrapy_extension.components.scheduler.DistributedScheduler": 500,
}

ITEM_PIPELINES = {
    "scrapy_extension.components.pipeline.BackendPipeline": 300,
}

DUPEFILTER_CLASS = "scrapy_extension.components.dupefilter.BackendDupeFilter"
SCHEDULER = "scrapy_extension.components.scheduler.DistributedScheduler"
```

```python
# spider.py
from scrapy import Spider
from scrapy_extension import BackendSpiderMixin


class MySpider(BackendSpiderMixin, Spider):
    name = "myspider"
    start_urls_mode = "both"

    def parse(self, response):
        yield {"url": response.url, "title": response.css("title::text").get()}
```

### 7.2 Environment Variables

```bash
# Redis standalone
export SCRAPY_EXT_BACKEND__BACKEND_TYPE="redis"
export SCRAPY_EXT_BACKEND__HOST="redis.example.com"
export SCRAPY_EXT_BACKEND__PORT="6379"
export SCRAPY_EXT_BACKEND__PASSWORD="secret"

# MongoDB replica set
export SCRAPY_EXT_BACKEND__BACKEND_TYPE="mongodb"
export SCRAPY_EXT_BACKEND__MODE="replica_set"
export SCRAPY_EXT_BACKEND__URI="mongodb://mongo1:27017,mongo2:27017/?replicaSet=rs0"

# Spider settings
export SCRAPY_EXT_SPIDER_START_URLS_MODE="pubsub"
export SCRAPY_EXT_SPIDER_POLL_INTERVAL="10"
```

### 7.3 Adding Start URLs

```python
# Add URLs to queue from external script
from scrapy_extension import BackendFactory, RedisConfig

config = RedisConfig(host="localhost")
backend = BackendFactory.get_backend(config)

# Push URLs
for url in urls:
    backend.push("myspider:start_urls", url.encode())

# Or publish for pubsub mode
backend.publish("myspider:start_urls:channel", url.encode())
```

## 8. Testing Strategy

### 8.1 Unit Tests

- Mock backend interface for component tests
- Test each backend implementation with mocked clients

### 8.2 Integration Tests

- Docker Compose setup for each backend
- Test full spider flow with real backends

### 8.3 Test Structure

```
tests/
├── unit/
│   ├── test_scheduler.py
│   ├── test_dupefilter.py
│   ├── test_pipeline.py
│   └── test_queue.py
├── integration/
│   ├── test_redis_backend.py
│   ├── test_mongo_backend.py
│   ├── test_kafka_backend.py
│   └── test_rabbitmq_backend.py
└── fixtures/
    └── docker-compose.yml
```

## 9. Dependencies

```toml
[project.dependencies]
scrapy = ">=2.14.2"
pydantic = ">=2.0"
pydantic-settings = ">=2.13.1"

[project.optional-dependencies]
# Backend-specific dependencies (choose what you need)
redis = ["redis>=7.3.0"]
mongo = ["pymongo>=4.6"]
kafka = ["confluent-kafka>=2.3"]
rabbitmq = ["pika>=1.3"]

# Install all backends
all = ["redis", "pymongo", "confluent-kafka", "pika"]

# Development
dev = ["pytest", "pytest-asyncio", "pytest-cov", "ruff", "mypy"]
```

**Installation Examples:**

```bash
# Install with specific backend
pip install scrapy-extension[redis]
pip install scrapy-extension[mongo]
pip install scrapy-extension[kafka]
pip install scrapy-extension[rabbitmq]

# Install with all backends
pip install scrapy-extension[all]

# Development install
pip install scrapy-extension[all,dev]
```

## 10. Error Handling

### 10.1 Backend Connection Errors

- Retry with exponential backoff on initial connection
- Clear error messages indicating which backend/mode failed
- Graceful degradation (log error, skip backend operations)

### 10.2 Serialization Errors

- Handle JSON errors for request serialization
- Log problematic requests without crashing spider

### 10.3 Timeout Handling

- Configurable timeouts for all backend operations
- Distinguish between temporary and permanent failures

## 11. Performance Considerations

### 11.1 Connection Pooling

- Each backend manages its own connection pool
- Configurable pool sizes per backend

### 11.2 Batching

- Pipeline uses batch inserts
- Queue operations can be batched when appropriate

### 11.3 Async Support

- Phase 2: Consider async backend implementations
- Current design compatible with async addition

## 12. Future Extensions

- Health check endpoint for monitoring
- Metrics integration (Prometheus)
- Automatic failover between backends
- Migration tools between backends
