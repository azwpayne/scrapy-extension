# Phase 1: Multi-Backend Implementation Design

## Overview

This document specifies the implementation of MongoDB, Kafka, and RabbitMQ backends for the `scrapy-extension` distributed crawling package.

## Goals

1. Implement three new backends following the existing protocol-based architecture
2. Maintain consistent API across all backends
3. Add appropriate configuration classes for each backend
4. Extend connection manager to support multi-backend selection
5. Ensure comprehensive test coverage

## Non-Goals

1. Kafka and RabbitMQ will not implement SetBackend and StorageBackend (message queues are not suitable for these use cases)
2. No migration tools between backends
3. No hybrid backend support (using different backends for different components)

## Architecture

### Protocol Compliance Matrix

| Backend | Backend | QueueBackend | SetBackend | StorageBackend |
|---------|---------|--------------|------------|----------------|
| Redis | ✅ | ✅ | ✅ | ✅ |
| MongoDB | ✅ | ✅ | ✅ | ✅ |
| Kafka | ✅ | ✅ | ❌ | ❌ |
| RabbitMQ | ✅ | ✅ | ❌ | ❌ |

### 1. MongoDB Backend

#### Data Model

**Queue Collection (`queues`)**
```javascript
{
    "_id": ObjectId,
    "queue_name": String,      // Indexed
    "item": Binary,
    "priority": Number,        // Negated priority for DESC sort
    "created_at": ISODate
}
// Indexes: {queue_name: 1, priority: 1, created_at: 1}
```

**Set Collection (`sets`)**
```javascript
{
    "_id": ObjectId,
    "set_name": String,        // Indexed
    "item_hash": String,       // sha256 of item, unique index
    "item": Binary,
    "created_at": ISODate
}
// Indexes: {set_name: 1, item_hash: 1} unique
```

**Storage Collection (`storage`)**
```javascript
{
    "_id": ObjectId,
    "key": String,             // Unique index
    "data": Binary,
    "expireAt": ISODate        // TTL index
}
// Indexes: {key: 1} unique, {expireAt: 1}: expireAfterSeconds: 0
```

#### Implementation Details

**QueueBackend.push()**
- Insert document with negated priority (for DESC sort)
- Use `w=1` write concern for durability

**QueueBackend.pop()**
- Use `find_one_and_delete()` with sort `{priority: 1, created_at: 1}`
- Atomic operation ensures no race conditions
- Blocking implementation using cursor with timeout

**QueueBackend.len()**
- `count_documents({queue_name: name})`

**QueueBackend.clear()**
- `delete_many({queue_name: name})`

**SetBackend.add()**
- Try insert with unique index
- DuplicateKeyError = already exists (return False)
- Success = added (return True)

**SetBackend.contains()**
- `find_one({set_name, item_hash})` is not None

**SetBackend.len()**
- `count_documents({set_name: name})`

**StorageBackend.store()**
- `replace_one({key}, doc, upsert=True)`
- Calculate `expireAt` from TTL if provided

**StorageBackend.retrieve()**
- `find_one({key})` return data field

**StorageBackend.ttl()**
- `find_one({key}, {expireAt: 1})`
- Calculate remaining seconds

#### Configuration

```python
class MongoDBSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SCRAPY_MONGO_")

    uri: str = "mongodb://localhost:27017"
    database: str = "scrapy_extension"
    queue_collection: str = "queues"
    set_collection: str = "sets"
    storage_collection: str = "storage"

    # Connection pool settings
    min_pool_size: int = 1
    max_pool_size: int = 10
    max_idle_time_ms: int = 60000
    wait_queue_timeout_ms: int = 5000

    # Write concern
    w: int | str = 1  # 1, "majority", or int
    journal: bool = True
    read_preference: str = "primary"  # primary, secondary, nearest
```

### 2. Kafka Backend

#### Architecture

Kafka is a distributed event streaming platform. It excels at queue semantics but does not support set operations or KV storage with TTL.

**Topic Naming Convention**
- Queue: `scrapy-{queue_name}`

**Priority Implementation**
- Use multiple partitions: priority 0 → partition 0, priority 1 → partition 1, etc.
- Consumers prioritize lower partition numbers
- Configurable max partitions (default: 10)

#### Implementation Details

**QueueBackend.push()**
```python
partition = min(int(priority), self.config.max_priority_partitions - 1)
self._producer.send(
    topic=queue_name,
    value=item,
    partition=partition
)
```

**QueueBackend.pop()**
- Non-blocking: poll all partitions, prioritize lower partition numbers
- Blocking: use consumer with timeout, round-robin through partitions

**QueueBackend.len()**
- Sum end offsets across all partitions minus current position
- Note: Eventually consistent

**QueueBackend.clear()**
- Delete and recreate topic (requires admin client)
- Or use compacted topic and tombstone records

**SetBackend & StorageBackend**
- Raise `NotImplementedError` with clear message
- Document that Kafka is queue-only backend

#### Configuration

```python
class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SCRAPY_KAFKA_")

    bootstrap_servers: str = "localhost:9092"
    max_priority_partitions: int = 10

    # Producer settings
    acks: str | int = "all"  # 0, 1, "all"
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 5
    compression_type: str | None = None  # gzip, snappy, lz4, zstd

    # Consumer settings
    group_id: str = "scrapy-extension"
    auto_offset_reset: str = "earliest"  # earliest, latest
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 500
    session_timeout_ms: int = 10000

    # Topic settings
    replication_factor: int = 1
    num_partitions: int = 10
    retention_ms: int = 604800000  # 7 days
```

### 3. RabbitMQ Backend

#### Architecture

RabbitMQ is an AMQP message broker with native priority queue support.

**Queue Declaration**
- Use `x-max-priority` argument (max 255 per AMQP spec)
- Priority 0-255, higher = more urgent

#### Implementation Details

**QueueBackend.push()**
```python
properties = pika.BasicProperties(
    priority=min(int(priority), 255),
    delivery_mode=2  # persistent
)
channel.basic_publish(
    exchange="",
    routing_key=queue_name,
    body=item,
    properties=properties
)
```

**QueueBackend.pop()**
```python
method, properties, body = channel.basic_get(queue=queue_name, auto_ack=False)
if method:
    channel.basic_ack(method.delivery_tag)
    return body
return None
```

**QueueBackend.len()**
- `queue_declare(passive=True)` returns message_count

**QueueBackend.clear()**
- `queue_purge()` removes all messages

**SetBackend & StorageBackend**
- Raise `NotImplementedError`
- Document that RabbitMQ is queue-only backend

#### Configuration

```python
class RabbitMQSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SCRAPY_RABBITMQ_")

    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"

    # Connection settings
    max_priority: int = 255
    heartbeat: int = 600
    blocked_connection_timeout: int = 300

    # Queue settings
    durable: bool = True
    auto_delete: bool = False
    delivery_mode: int = 2  # 1=transient, 2=persistent
```

### 4. Connection Manager Extension

Update `ConnectionManager` to support backend type selection:

```python
_BACKENDS: dict[BackendType, type[Backend]] = {
    BackendType.REDIS: RedisBackend,
    BackendType.MONGODB: MongoDBBackend,
    BackendType.KAFKA: KafkaBackend,
    BackendType.RABBITMQ: RabbitMQBackend,
}

_SETTINGS: dict[BackendType, type[BaseSettings]] = {
    BackendType.REDIS: RedisSettings,
    BackendType.MONGODB: MongoDBSettings,
    BackendType.KAFKA: KafkaSettings,
    BackendType.RABBITMQ: RabbitMQSettings,
}
```

### 5. Error Handling

Each backend should wrap client exceptions into `scrapy-extension` exceptions:

| Client Exception | scrapy-extension Exception |
|------------------|---------------------------|
| `pymongo.errors.ConnectionFailure` | `BackendConnectionError` |
| `pymongo.errors.DuplicateKeyError` | (SetBackend.add returns False) |
| `kafka.errors.KafkaError` | `BackendConnectionError` / `QueueError` |
| `pika.exceptions.AMQPError` | `BackendConnectionError` / `QueueError` |

### 6. Testing Strategy

Each backend requires:

1. **Unit tests** with mocked clients (no real services)
2. **Integration tests** marked with `pytest.mark.integration` (require real services)

Test coverage requirements:
- All protocol methods
- Connection/disconnection scenarios
- Error conditions
- Configuration validation

## Implementation Order

1. **MongoDB Backend** - Full feature support, serves as reference
2. **Kafka Backend** - Queue-only, complex configuration
3. **RabbitMQ Backend** - Queue-only, simpler configuration

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| MongoDB atomic pop race condition | Use `find_one_and_delete` which is atomic |
| Kafka eventual consistency for len() | Document behavior, use for monitoring only |
| RabbitMQ priority queue performance | Document that priorities > 5 impact performance |
| Configuration drift between backends | Shared base settings, clear env var prefixes |

## Migration Path

Existing Redis users are unaffected. To use new backends:

1. Install optional dependencies: `pip install scrapy-extension[mongodb]`
2. Set `SCRAPY_BACKEND_TYPE=mongodb`
3. Configure backend-specific settings via env vars

## Open Questions

1. Should we provide a `HybridBackend` that uses different backends for different components (e.g., Kafka for queue, MongoDB for set/storage)?
2. Should Kafka/RabbitMQ backends delegate Set/Storage to a secondary backend (e.g., Redis) rather than raising NotImplementedError?

## Success Criteria

- [ ] All three backends pass unit tests with mocked clients
- [ ] All three backends pass integration tests with real services
- [ ] 100% protocol method coverage for all backends
- [ ] Configuration classes with environment variable support
- [ ] Documentation with usage examples
- [ ] Performance benchmarks (optional but recommended)
