# Multi-Backend Scrapy Extension Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Scrapy extension with 4 backends (Redis, MongoDB, Kafka, RabbitMQ) providing distributed crawling components (Scheduler, DupeFilter, Pipeline, Queue, SpiderMixin).

**Architecture:** Unified `BackendInterface` that all backends implement. Components use the interface, not specific backends. Pydantic-settings for type-safe configuration with lazy singleton connection management via `BackendFactory`.

**Tech Stack:** Python 3.10+, Scrapy 2.14+, Pydantic v2, Pydantic-settings, uv (package manager), pytest

---

## File Structure Overview

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
│   │   ├── config.py              # RedisConfig
│   │   └── backend.py             # RedisBackend
│   ├── mongo/
│   │   ├── __init__.py
│   │   ├── config.py              # MongoConfig
│   │   └── backend.py             # MongoBackend
│   ├── kafka/
│   │   ├── __init__.py
│   │   ├── config.py              # KafkaConfig
│   │   └── backend.py             # KafkaBackend
│   └── rabbitmq/
│       ├── __init__.py
│       ├── config.py              # RabbitMQConfig
│       └── backend.py             # RabbitMQBackend
├── components/
│   ├── __init__.py
│   ├── queue.py                   # PriorityQueue
│   ├── scheduler.py               # DistributedScheduler
│   ├── dupefilter.py              # BackendDupeFilter
│   ├── pipeline.py                # BackendPipeline
│   └── spider.py                  # BackendSpiderMixin
└── utils/
    ├── __init__.py
    ├── request.py                 # request_to_dict / request_from_dict
    └── fingerprint.py             # fingerprint helpers

tests/
├── unit/
│   ├── backend/
│   │   ├── test_interface.py
│   │   ├── test_factory.py
│   │   ├── test_redis_backend.py
│   │   ├── test_mongo_backend.py
│   │   ├── test_kafka_backend.py
│   │   └── test_rabbitmq_backend.py
│   └── components/
│       ├── test_queue.py
│       ├── test_scheduler.py
│       ├── test_dupefilter.py
│       ├── test_pipeline.py
│       └── test_spider.py
└── integration/
    └── test_full_flow.py
```

---

## Phase 1: Foundation

### Task 1: Backend Interface

**Files:**
- Create: `src/scrapy_extension/backend/__init__.py`
- Create: `src/scrapy_extension/backend/interface.py`

**Prerequisites:** None

- [ ] **Step 1: Write failing test**

```python
# tests/unit/backend/test_interface.py
import pytest
from abc import ABC
from scrapy_extension.backend.interface import BackendInterface

def test_backend_interface_is_abstract():
    """BackendInterface should be an abstract base class."""
    assert issubclass(BackendInterface, ABC)

def test_backend_interface_cannot_be_instantiated():
    """BackendInterface cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BackendInterface()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/backend/test_interface.py -v
```
Expected: FAIL - ModuleNotFoundError

- [ ] **Step 3: Create interface module**

```python
# src/scrapy_extension/backend/__init__.py
"""Backend module for scrapy-extension."""

# src/scrapy_extension/backend/interface.py
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
        """Pop item from queue. Block for timeout seconds if empty."""
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
        """Add fingerprint to set. Return True if added, False if exists."""
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
        """
        pass

    @abstractmethod
    def publish(self, channel: str, message: bytes) -> None:
        """Publish message to channel."""
        pass
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/backend/test_interface.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/backend/__init__.py \
        src/scrapy_extension/backend/interface.py \
        tests/unit/backend/test_interface.py
git commit -m "feat: add BackendInterface abstract base class

- Define all required backend operations
- Connection lifecycle methods
- Queue, dupefilter, pipeline, pub/sub operations"
```

---

### Task 2: Backend Factory (Lazy Singleton)

**Files:**
- Create: `src/scrapy_extension/backend/factory.py`
- Create: `tests/unit/backend/test_factory.py`

**Prerequisites:** Task 1 (BackendInterface)

- [ ] **Step 1: Write failing test**

```python
# tests/unit/backend/test_factory.py
import pytest
from unittest.mock import Mock
from scrapy_extension.backend.factory import BackendFactory
from scrapy_extension.backend.interface import BackendInterface


def test_backend_factory_returns_same_instance():
    """Factory should return same instance for same config."""
    config = Mock()
    config.backend_type = "test"

    backend1 = BackendFactory.get_backend(config)
    backend2 = BackendFactory.get_backend(config)

    assert backend1 is backend2


def test_backend_factory_creates_different_instances_for_different_configs():
    """Factory should create different instances for different configs."""
    config1 = Mock()
    config1.backend_type = "test1"
    config2 = Mock()
    config2.backend_type = "test2"

    backend1 = BackendFactory.get_backend(config1)
    backend2 = BackendFactory.get_backend(config2)

    assert backend1 is not backend2


def test_backend_factory_clears_cache():
    """Clear cache should remove all instances."""
    config = Mock()
    config.backend_type = "test"

    backend1 = BackendFactory.get_backend(config)
    BackendFactory.clear_cache()
    backend2 = BackendFactory.get_backend(config)

    # After clearing cache, should get new instance
    # Note: In real implementation, we'd verify this differently
    assert BackendFactory._instances == {}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/backend/test_factory.py -v
```
Expected: FAIL - ModuleNotFoundError

- [ ] **Step 3: Implement BackendFactory**

```python
# src/scrapy_extension/backend/factory.py
import threading
from typing import TypeVar

from scrapy_extension.backend.interface import BackendInterface

T = TypeVar("T", bound=BackendInterface)


class BackendFactory:
    """Factory for creating and caching backend instances.

    Uses lazy initialization - connection is established on first use.
    Same config returns same instance (singleton per config).
    """

    _instances: dict[str, BackendInterface] = {}
    _lock = threading.Lock()

    @classmethod
    def get_backend(cls, config=None) -> BackendInterface:
        """Get or create backend instance.

        Args:
            config: Backend configuration (optional, uses default from settings if not provided)

        Returns:
            BackendInterface instance
        """
        # If no config provided, get from settings
        if config is None:
            from scrapy_extension.settings import ScrapyExtensionSettings
            settings = ScrapyExtensionSettings()
            config = settings.backend

        # Use backend_type as cache key
        cache_key = str(config.backend_type)

        with cls._lock:
            if cache_key not in cls._instances:
                backend = cls._create_backend(config)
                cls._instances[cache_key] = backend
            return cls._instances[cache_key]

    @classmethod
    def _create_backend(cls, config) -> BackendInterface:
        """Create backend instance based on config type.

        This method imports backend classes lazily to avoid
        importing unused dependencies.

        NOTE: These are forward references. The imports will succeed
        once the backend modules are implemented in Tasks 4-8.
        """
        backend_type = config.backend_type

        if backend_type == "redis":
            from scrapy_extension.backend.redis.backend import RedisBackend
            return RedisBackend(config)
        elif backend_type == "mongodb":
            from scrapy_extension.backend.mongo.backend import MongoBackend
            return MongoBackend(config)
        elif backend_type == "kafka":
            from scrapy_extension.backend.kafka.backend import KafkaBackend
            return KafkaBackend(config)
        elif backend_type == "rabbitmq":
            from scrapy_extension.backend.rabbitmq.backend import RabbitMQBackend
            return RabbitMQBackend(config)
        else:
            raise ValueError(f"Unknown backend type: {backend_type}")

    @classmethod
    def clear_cache(cls) -> None:
        """Clear all cached backend instances."""
        with cls._lock:
            # Close connections before clearing
            for backend in cls._instances.values():
                try:
                    backend.close()
                except Exception:
                    pass
            cls._instances.clear()
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/backend/test_factory.py -v
```
Expected: PASS (may need to adjust tests based on actual implementation)

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/backend/factory.py \
        tests/unit/backend/test_factory.py
git commit -m "feat: add BackendFactory for lazy singleton management

- Thread-safe singleton pattern
- Lazy backend class imports
- Cache management with connection cleanup"
```

---

### Task 3: Settings Module

**Files:**
- Create: `src/scrapy_extension/settings.py`
- Create: `tests/unit/test_settings.py`

**Prerequisites:** None

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_settings.py
import pytest
from pydantic import ValidationError
from scrapy_extension.settings import BackendType, ScrapyExtensionSettings
from scrapy_extension.backend.redis import RedisConfig
from scrapy_extension.backend.mongo import MongoConfig
from scrapy_extension.backend.kafka import KafkaConfig
from scrapy_extension.backend.rabbitmq import RabbitMQConfig


def test_backend_type_enum():
    """BackendType should have expected values."""
    assert BackendType.REDIS == "redis"
    assert BackendType.MONGODB == "mongodb"
    assert BackendType.KAFKA == "kafka"
    assert BackendType.RABBITMQ == "rabbitmq"


def test_redis_config_defaults():
    """RedisConfig should have sensible defaults."""
    config = RedisConfig()
    assert config.backend_type == "redis"
    assert config.mode == "standalone"
    assert config.host == "localhost"
    assert config.port == 6379


def test_redis_config_validation():
    """RedisConfig should validate port range."""
    with pytest.raises(ValidationError):
        RedisConfig(port=99999)


def test_scrapy_extension_settings_with_redis():
    """ScrapyExtensionSettings should accept RedisConfig."""
    settings = ScrapyExtensionSettings(
        backend=RedisConfig(host="redis.example.com")
    )
    assert settings.backend.backend_type == "redis"
    assert settings.backend.host == "redis.example.com"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/test_settings.py -v
```
Expected: FAIL - ModuleNotFoundError

- [ ] **Step 3: Implement settings module**

```python
# src/scrapy_extension/settings.py
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class BackendType(str, Enum):
    """Supported backend types."""
    REDIS = "redis"
    MONGODB = "mongodb"
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"


class RedisMode(str, Enum):
    """Redis deployment modes."""
    STANDALONE = "standalone"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"


class SentinelNode(BaseModel):
    """Redis sentinel node configuration."""
    host: str
    port: int = 26379


class RedisConfig(BaseModel):
    """Redis backend configuration."""
    backend_type: Literal[BackendType.REDIS] = BackendType.REDIS
    mode: RedisMode = RedisMode.STANDALONE

    # Standalone settings
    host: str = "localhost"
    port: int = Field(default=6379, ge=1, le=65535)
    password: str | None = None
    db: int = 0

    # Sentinel settings
    sentinels: list[SentinelNode] = Field(default_factory=list)
    master_name: str = "mymaster"

    # Cluster settings
    startup_nodes: list[SentinelNode] = Field(default_factory=list)
    skip_full_coverage_check: bool = False

    # Connection settings
    socket_timeout: int = 30
    socket_connect_timeout: int = 30
    retry_on_timeout: bool = True


class MongoMode(str, Enum):
    """MongoDB deployment modes."""
    STANDALONE = "standalone"
    REPLICA_SET = "replica_set"
    SHARDED = "sharded"


class MongoConfig(BaseModel):
    """MongoDB backend configuration."""
    backend_type: Literal[BackendType.MONGODB] = BackendType.MONGODB
    mode: MongoMode = MongoMode.STANDALONE

    uri: str = "mongodb://localhost:27017"
    database: str = "scrapy"

    # Connection pool settings
    max_pool_size: int = 100
    min_pool_size: int = 10
    max_idle_time_ms: int = 60000

    # Timeout settings
    connect_timeout_ms: int = 10000
    server_selection_timeout_ms: int = 30000
    socket_timeout_ms: int = 30000


class KafkaConfig(BaseModel):
    """Kafka backend configuration."""
    backend_type: Literal[BackendType.KAFKA] = BackendType.KAFKA

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "scrapy-extension"

    # Producer settings
    producer_acks: str = "all"
    producer_retries: int = 3
    producer_batch_size: int = 16384

    # Consumer settings
    consumer_group_id: str = "scrapy-group"
    consumer_auto_offset_reset: str = "earliest"
    consumer_enable_auto_commit: bool = True

    # Security settings
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None


class RabbitMQConfig(BaseModel):
    """RabbitMQ backend configuration."""
    backend_type: Literal[BackendType.RABBITMQ] = BackendType.RABBITMQ

    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"

    # Connection settings
    connection_attempts: int = 3
    retry_delay: int = 5
    socket_timeout: int = 10

    # Channel settings
    prefetch_count: int = 1
    delivery_mode: int = 2  # persistent


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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/test_settings.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/settings.py \
        tests/unit/test_settings.py
git commit -m "feat: add pydantic settings for all backends

- BackendType enum
- Config classes for Redis, MongoDB, Kafka, RabbitMQ
- ScrapyExtensionSettings with discriminator
- Environment variable support"
```

---

### Task 3b: Scrapy Settings Integration (Documentation)

**Important Context:**

Scrapy's settings system requires serializable values (strings, numbers, dicts, lists). Pydantic models cannot be directly assigned to Scrapy settings. The extension uses **environment variables** as the primary configuration mechanism.

**Configuration Flow:**
1. User sets environment variables (e.g., `SCRAPY_EXT_BACKEND__BACKEND_TYPE=redis`)
2. `ScrapyExtensionSettings` reads from environment via pydantic-settings
3. `BackendFactory.get_backend()` creates backend from settings
4. Components receive backend instance via `from_crawler()`

**No code changes required** - this is handled by pydantic-settings automatically. Just ensure environment variables are set before running Scrapy.

**Example:**
```bash
export SCRAPY_EXT_BACKEND__BACKEND_TYPE="redis"
export SCRAPY_EXT_BACKEND__HOST="localhost"
scrapy crawl myspider
```

---

## Phase 2: Redis Backend

### Task 4: Redis Config Module

**Files:**
- Create: `src/scrapy_extension/backend/redis/__init__.py`
- Create: `src/scrapy_extension/backend/redis/config.py`

**Prerequisites:** Task 3 (settings module)

- [ ] **Step 1: Move RedisConfig to dedicated module**

```python
# src/scrapy_extension/backend/redis/__init__.py
"""Redis backend module."""
from scrapy_extension.backend.redis.config import RedisConfig

__all__ = ["RedisConfig"]

# src/scrapy_extension/backend/redis/config.py
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from scrapy_extension.settings import BackendType


class RedisMode(str, Enum):
    """Redis deployment modes."""
    STANDALONE = "standalone"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"


class SentinelNode(BaseModel):
    """Redis sentinel node configuration."""
    host: str
    port: int = 26379


class RedisConfig(BaseModel):
    """Redis backend configuration."""
    backend_type: Literal[BackendType.REDIS] = BackendType.REDIS
    mode: RedisMode = RedisMode.STANDALONE

    # Standalone settings
    host: str = "localhost"
    port: int = Field(default=6379, ge=1, le=65535)
    password: str | None = None
    db: int = 0

    # Sentinel settings
    sentinels: list[SentinelNode] = Field(default_factory=list)
    master_name: str = "mymaster"

    # Cluster settings
    startup_nodes: list[SentinelNode] = Field(default_factory=list)
    skip_full_coverage_check: bool = False

    # Connection settings
    socket_timeout: int = 30
    socket_connect_timeout: int = 30
    retry_on_timeout: bool = True
```

- [ ] **Step 2: Update main settings to import from redis module**

Modify `src/scrapy_extension/settings.py` to import RedisConfig from the redis module instead of defining it inline.

- [ ] **Step 3: Commit**

```bash
git add src/scrapy_extension/backend/redis/
git commit -m "refactor: move RedisConfig to dedicated module

- Separate redis config into backend/redis/config.py
- Maintain backward compatibility in settings.py"
```

---

### Task 5: Redis Backend Implementation

**Files:**
- Create: `src/scrapy_extension/backend/redis/backend.py`
- Create: `tests/unit/backend/test_redis_backend.py`

**Prerequisites:** Task 1 (interface), Task 4 (redis config)

- [ ] **Step 1: Write failing test**

```python
# tests/unit/backend/test_redis_backend.py
import pytest
from unittest.mock import Mock, patch, MagicMock
from scrapy_extension.backend.redis.backend import RedisBackend
from scrapy_extension.backend.redis.config import RedisConfig


class TestRedisBackendStandalone:
    """Test RedisBackend in standalone mode."""

    @patch("scrapy_extension.backend.redis.backend.Redis")
    def test_connect_standalone(self, mock_redis_class):
        """Test connection to standalone Redis."""
        config = RedisConfig(
            host="localhost",
            port=6379,
            password="secret",
            db=0
        )
        backend = RedisBackend(config)
        backend.connect()

        mock_redis_class.assert_called_once_with(
            host="localhost",
            port=6379,
            password="secret",
            db=0,
            socket_timeout=30,
            socket_connect_timeout=30,
            retry_on_timeout=True,
        )

    @patch("scrapy_extension.backend.redis.backend.Redis")
    def test_push(self, mock_redis_class):
        """Test push operation."""
        mock_client = MagicMock()
        mock_redis_class.return_value = mock_client

        config = RedisConfig()
        backend = RedisBackend(config)
        backend.push("test_queue", b"test_item", priority=5)

        mock_client.zadd.assert_called_once_with("test_queue", {b"test_item": 5})

    @patch("scrapy_extension.backend.redis.backend.Redis")
    def test_pop(self, mock_redis_class):
        """Test pop operation."""
        mock_client = MagicMock()
        mock_client.zpopmax.return_value = [(b"test_item", 5)]
        mock_redis_class.return_value = mock_client

        config = RedisConfig()
        backend = RedisBackend(config)
        result = backend.pop("test_queue", timeout=0)

        assert result == b"test_item"
        mock_client.zpopmax.assert_called_once_with("test_queue")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/backend/test_redis_backend.py -v
```
Expected: FAIL - ModuleNotFoundError

- [ ] **Step 3: Implement RedisBackend**

```python
# src/scrapy_extension/backend/redis/backend.py
import json
import logging
from typing import Iterator

from redis import Redis, Sentinel
from redis.exceptions import RedisError

from scrapy_extension.backend.interface import BackendInterface
from scrapy_extension.backend.redis.config import RedisConfig

logger = logging.getLogger(__name__)


class RedisBackend(BackendInterface):
    """Redis backend implementation.

    Supports: standalone, sentinel, cluster modes.
    """

    def __init__(self, config: RedisConfig):
        self.config = config
        self._client: Redis | None = None
        self._pubsub = None

    def connect(self) -> None:
        """Establish connection to Redis based on config mode."""
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
                startup_nodes=[
                    {"host": n.host, "port": n.port}
                    for n in self.config.startup_nodes
                ],
                password=self.config.password,
                skip_full_coverage_check=self.config.skip_full_coverage_check,
            )
        else:
            raise ValueError(f"Unknown Redis mode: {self.config.mode}")

    @property
    def client(self) -> Redis:
        """Get or create Redis client."""
        if self._client is None:
            self.connect()
        return self._client

    def close(self) -> None:
        """Close Redis connection."""
        if self._pubsub:
            self._pubsub.close()
        if self._client:
            self._client.close()
            self._client = None

    @property
    def is_connected(self) -> bool:
        """Check if Redis is connected."""
        try:
            return self.client.ping() if self._client else False
        except RedisError:
            return False

    # Queue operations using Sorted Sets
    def push(self, queue_name: str, item: bytes, priority: int = 0) -> None:
        """Push item to priority queue."""
        self.client.zadd(queue_name, {item: priority})

    def pop(self, queue_name: str, timeout: int = 0) -> bytes | None:
        """Pop highest priority item from queue."""
        if timeout > 0:
            # Use BZPOPMAX for blocking pop
            result = self.client.bzpopmax(queue_name, timeout=timeout)
            return result[1] if result else None
        else:
            result = self.client.zpopmax(queue_name)
            return result[0][0] if result else None

    def queue_len(self, queue_name: str) -> int:
        """Get queue length."""
        return self.client.zcard(queue_name)

    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue."""
        self.client.delete(queue_name)

    # Duplicate filter using Sets
    def add_fingerprint(self, key: str, fingerprint: str) -> bool:
        """Add fingerprint to set. Returns True if added, False if exists."""
        # SADD returns number of elements added (0 if already exists)
        return self.client.sadd(key, fingerprint) == 1

    def is_fingerprint_exists(self, key: str, fingerprint: str) -> bool:
        """Check if fingerprint exists."""
        return self.client.sismember(key, fingerprint)

    def clear_fingerprints(self, key: str) -> None:
        """Clear all fingerprints."""
        self.client.delete(key)

    # Pipeline using Lists
    def store_item(self, collection: str, item: dict) -> None:
        """Store item in collection."""
        self.client.lpush(collection, json.dumps(item))

    def store_items_batch(self, collection: str, items: list[dict]) -> None:
        """Store multiple items in batch."""
        if not items:
            return
        pipe = self.client.pipeline()
        for item in items:
            pipe.lpush(collection, json.dumps(item))
        pipe.execute()

    # Pub/Sub operations
    def subscribe(self, channel: str) -> Iterator[bytes]:
        """Subscribe to channel."""
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
        """Publish message to channel."""
        self.client.publish(channel, message)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/backend/test_redis_backend.py -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/backend/redis/backend.py \
        tests/unit/backend/test_redis_backend.py
git commit -m "feat: implement RedisBackend with full feature set

- Support standalone, sentinel, cluster modes
- Queue operations using Sorted Sets
- DupeFilter using Sets
- Pipeline using Lists
- Pub/Sub support"
```

---

## Phase 3: Remaining Backends

### Task 6: MongoDB Backend

**Files:**
- Create: `src/scrapy_extension/backend/mongo/__init__.py`
- Create: `src/scrapy_extension/backend/mongo/config.py`
- Create: `src/scrapy_extension/backend/mongo/backend.py`
- Create: `tests/unit/backend/test_mongo_backend.py`

**Prerequisites:** Task 1 (interface)

**Reference:** See spec section 4.2 for full MongoBackend implementation

- [ ] **Step 1: Create MongoDB config module**

```python
# src/scrapy_extension/backend/mongo/__init__.py
"""MongoDB backend module."""
from scrapy_extension.backend.mongo.config import MongoConfig

__all__ = ["MongoConfig"]

# src/scrapy_extension/backend/mongo/config.py
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from scrapy_extension.settings import BackendType


class MongoMode(str, Enum):
    """MongoDB deployment modes."""
    STANDALONE = "standalone"
    REPLICA_SET = "replica_set"
    SHARDED = "sharded"


class MongoConfig(BaseModel):
    """MongoDB backend configuration."""
    backend_type: Literal[BackendType.MONGODB] = BackendType.MONGODB
    mode: MongoMode = MongoMode.STANDALONE

    uri: str = "mongodb://localhost:27017"
    database: str = "scrapy"

    # Connection pool settings
    max_pool_size: int = 100
    min_pool_size: int = 10
    max_idle_time_ms: int = 60000

    # Timeout settings
    connect_timeout_ms: int = 10000
    server_selection_timeout_ms: int = 30000
    socket_timeout_ms: int = 30000
```

- [ ] **Step 2: Implement MongoBackend**

```python
# src/scrapy_extension/backend/mongo/backend.py
import json
import logging
from datetime import datetime
from typing import Iterator

import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, ConnectionFailure

from scrapy_extension.backend.interface import BackendInterface
from scrapy_extension.backend.mongo.config import MongoConfig

logger = logging.getLogger(__name__)


class MongoBackend(BackendInterface):
    """MongoDB backend implementation."""

    def __init__(self, config: MongoConfig):
        self.config = config
        self._client: MongoClient | None = None
        self._db = None

    def connect(self) -> None:
        """Establish connection to MongoDB."""
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
        """Get or create MongoDB client."""
        if self._client is None:
            self.connect()
        return self._client

    def close(self) -> None:
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None

    @property
    def is_connected(self) -> bool:
        """Check if MongoDB is connected."""
        try:
            self.client.admin.command("ping")
            return True
        except ConnectionFailure:
            return False

    # Queue operations using standard collection
    def push(self, queue_name: str, item: bytes, priority: int = 0) -> None:
        """Push item to queue."""
        collection = self._db[queue_name]
        collection.insert_one({
            "item": item,
            "priority": priority,
            "created_at": datetime.utcnow()
        })

    def pop(self, queue_name: str, timeout: int = 0) -> bytes | None:
        """Pop highest priority item from queue."""
        collection = self._db[queue_name]
        result = collection.find_one_and_delete(
            {},
            sort=[("priority", DESCENDING), ("created_at", ASCENDING)]
        )
        return result["item"] if result else None

    def queue_len(self, queue_name: str) -> int:
        """Get queue length."""
        return self._db[queue_name].estimated_document_count()

    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue."""
        self._db[queue_name].drop()

    # Duplicate filter using unique index
    def add_fingerprint(self, key: str, fingerprint: str) -> bool:
        """Add fingerprint. Returns True if added, False if exists."""
        collection = self._db[key]
        try:
            collection.insert_one({
                "fingerprint": fingerprint,
                "created_at": datetime.utcnow()
            })
            return True
        except DuplicateKeyError:
            return False

    def is_fingerprint_exists(self, key: str, fingerprint: str) -> bool:
        """Check if fingerprint exists."""
        return self._db[key].find_one({"fingerprint": fingerprint}) is not None

    def clear_fingerprints(self, key: str) -> None:
        """Clear all fingerprints."""
        self._db[key].drop()

    # Pipeline operations
    def store_item(self, collection: str, item: dict) -> None:
        """Store item."""
        self._db[collection].insert_one(item)

    def store_items_batch(self, collection: str, items: list[dict]) -> None:
        """Store multiple items in batch."""
        if items:
            self._db[collection].insert_many(items)

    # Pub/Sub using capped collection
    def subscribe(self, channel: str) -> Iterator[bytes]:
        """Subscribe to channel using tailable cursor."""
        # Ensure capped collection exists
        if channel not in self._db.list_collection_names():
            self._db.create_collection(
                channel,
                capped=True,
                size=1000000,
                max=1000
            )

        collection = self._db[channel]
        last_id = None

        while True:
            query = {"_id": {"$gt": last_id}} if last_id else {}
            cursor = collection.find(
                query,
                cursor_type=pymongo.CursorType.TAILABLE_AWAIT
            )

            try:
                for doc in cursor:
                    last_id = doc["_id"]
                    yield doc["message"]
            except StopIteration:
                continue
            except pymongo.errors.PyMongoError as e:
                logger.error(f"MongoDB subscribe error: {e}")
                raise StopIteration

    def publish(self, channel: str, message: bytes) -> None:
        """Publish message to channel."""
        # Ensure capped collection exists
        if channel not in self._db.list_collection_names():
            self._db.create_collection(
                channel,
                capped=True,
                size=1000000,
                max=1000
            )
        self._db[channel].insert_one({"message": message})
```

- [ ] **Step 3: Write tests**

Write tests similar to Redis backend tests, mocking pymongo.

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/unit/backend/test_mongo_backend.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/backend/mongo/
git add tests/unit/backend/test_mongo_backend.py
git commit -m "feat: implement MongoBackend

- Support standalone, replica set, sharded modes
- Queue with priority using standard collections
- DupeFilter using unique index
- Pub/Sub using tailable cursor on capped collection"
```

---

### Task 7: Kafka Backend

**Files:**
- Create: `src/scrapy_extension/backend/kafka/__init__.py`
- Create: `src/scrapy_extension/backend/kafka/config.py`
- Create: `src/scrapy_extension/backend/kafka/backend.py`
- Create: `tests/unit/backend/test_kafka_backend.py`

**Pattern:** Same as MongoDB - config + backend + tests

**Reference:** See spec section 4.3 for full KafkaBackend implementation

**Key implementation notes:**
- Queue: Use Kafka topics with partition-based priority
- DupeFilter: In-memory set (not distributed, document this limitation)
- Pub/Sub: Native Kafka consumer groups

- [ ] **Step 1-5:** Follow same pattern as Task 6

---

### Task 8: RabbitMQ Backend

**Files:**
- Create: `src/scrapy_extension/backend/rabbitmq/__init__.py`
- Create: `src/scrapy_extension/backend/rabbitmq/config.py`
- Create: `src/scrapy_extension/backend/rabbitmq/backend.py`
- Create: `tests/unit/backend/test_rabbitmq_backend.py`

**Pattern:** Same as MongoDB - config + backend + tests

**Reference:** See spec section 4.4 for full RabbitMQBackend implementation

**Key implementation notes:**
- Queue: Priority queues with x-max-priority header
- DupeFilter: In-memory set (document limitation)
- Pub/Sub: Fanout exchanges

- [ ] **Step 1-5:** Follow same pattern as Task 6

---

## Phase 4: Components

### Task 9: PriorityQueue

**Files:**
- Create: `src/scrapy_extension/components/__init__.py`
- Create: `src/scrapy_extension/components/queue.py`
- Create: `tests/unit/components/test_queue.py`

**Prerequisites:** Task 1 (BackendInterface)

- [ ] **Step 1: Write failing test**

```python
# tests/unit/components/test_queue.py
import pytest
from unittest.mock import Mock
from scrapy_extension.components.queue import PriorityQueue


def test_priority_queue_push():
    """Test pushing item to queue."""
    mock_backend = Mock()
    queue = PriorityQueue(mock_backend, "test_queue")

    queue.push(b"item", priority=5)

    mock_backend.push.assert_called_once_with("test_queue", b"item", 5)


def test_priority_queue_pop():
    """Test popping item from queue."""
    mock_backend = Mock()
    mock_backend.pop.return_value = b"item"
    queue = PriorityQueue(mock_backend, "test_queue")

    result = queue.pop(timeout=10)

    assert result == b"item"
    mock_backend.pop.assert_called_once_with("test_queue", timeout=10)


def test_priority_queue_len():
    """Test getting queue length."""
    mock_backend = Mock()
    mock_backend.queue_len.return_value = 42
    queue = PriorityQueue(mock_backend, "test_queue")

    result = queue.len()

    assert result == 42
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement PriorityQueue**

```python
# src/scrapy_extension/components/__init__.py
"""Components module for scrapy-extension."""

# src/scrapy_extension/components/queue.py
from scrapy_extension.backend.interface import BackendInterface


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

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/components/queue.py \
        tests/unit/components/test_queue.py
git commit -m "feat: add PriorityQueue component

- Backend-agnostic priority queue
- Wrapper around BackendInterface queue operations"
```

---

### Task 10: DistributedScheduler

**Files:**
- Create: `src/scrapy_extension/components/scheduler.py`
- Create: `tests/unit/components/test_scheduler.py`

**Prerequisites:** Task 9 (PriorityQueue), Task 1 (BackendInterface)

- [ ] **Step 1: Write failing test**

```python
# tests/unit/components/test_scheduler.py
import pytest
import json
from unittest.mock import Mock, MagicMock, patch
from scrapy import Request
from scrapy.http import Response
from scrapy.settings import Settings

from scrapy_extension.components.scheduler import DistributedScheduler


class TestDistributedScheduler:
    """Test DistributedScheduler."""

    @patch("scrapy_extension.components.scheduler.BackendFactory")
    def test_from_crawler(self, mock_factory):
        """Test creating scheduler from crawler."""
        mock_backend = Mock()
        mock_factory.get_backend.return_value = mock_backend

        mock_crawler = Mock()
        mock_crawler.settings = Settings()

        scheduler = DistributedScheduler.from_crawler(mock_crawler)

        assert scheduler.backend is mock_backend
        mock_factory.get_backend.assert_called_once()

    @patch("scrapy_extension.components.scheduler.BackendFactory")
    def test_enqueue_request(self, mock_factory):
        """Test enqueuing request."""
        mock_backend = Mock()
        mock_factory.get_backend.return_value = mock_backend

        scheduler = DistributedScheduler.from_crawler(Mock())
        scheduler.open(Mock())

        request = Request("http://example.com")
        result = scheduler.enqueue_request(request)

        assert result is True
        mock_backend.push.assert_called_once()

    @patch("scrapy_extension.components.scheduler.BackendFactory")
    def test_next_request(self, mock_factory):
        """Test getting next request."""
        mock_backend = Mock()
        request_dict = {
            "url": "http://example.com",
            "method": "GET",
            "headers": {},
            "body": "",
            "cookies": {},
            "meta": {},
        }
        mock_backend.pop.return_value = json.dumps(request_dict).encode()
        mock_factory.get_backend.return_value = mock_backend

        scheduler = DistributedScheduler.from_crawler(Mock())
        mock_spider = Mock()
        mock_spider.name = "test"
        scheduler.open(mock_spider)

        result = scheduler.next_request()

        assert isinstance(result, Request)
        assert result.url == "http://example.com"
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement DistributedScheduler**

```python
# src/scrapy_extension/components/scheduler.py
import json
import logging
from typing import TYPE_CHECKING

from scrapy.core.scheduler import BaseScheduler
from scrapy.http import Request
from scrapy.utils.reqser import request_to_dict, request_from_dict

from scrapy_extension.backend.factory import BackendFactory
from scrapy_extension.backend.interface import BackendInterface
from scrapy_extension.components.queue import PriorityQueue

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from scrapy.spiders import Spider
    from scrapy.statscollectors import StatsCollector

logger = logging.getLogger(__name__)


class DistributedScheduler(BaseScheduler):
    """Distributed scheduler using configurable backend."""

    def __init__(
        self,
        backend: BackendInterface,
        queue_key: str,
        dupefilter_key: str,
        idle_before_close: int = 0,
        stats: "StatsCollector | None" = None,
    ):
        self.backend = backend
        self.queue = PriorityQueue(backend, queue_key)
        self.dupefilter = None  # Created in open()
        self.dupefilter_key = dupefilter_key
        self.idle_before_close = idle_before_close
        self.stats = stats
        self.queue_key_template = queue_key
        self.dupefilter_key_template = dupefilter_key
        self.spider: "Spider | None" = None

    @classmethod
    def from_crawler(cls, crawler: "Crawler") -> "DistributedScheduler":
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

    def open(self, spider: "Spider") -> None:
        """Called when spider opens."""
        from scrapy_extension.components.dupefilter import BackendDupeFilter

        self.queue_key = self.queue_key_template.format(spider=spider.name)
        self.dupefilter_key = self.dupefilter_key_template.format(spider=spider.name)

        # Update queue with formatted key
        self.queue.key = self.queue_key

        # Create dupefilter with formatted key
        self.dupefilter = BackendDupeFilter(
            backend=self.backend,
            key=self.dupefilter_key,
        )
        self.dupefilter.open(spider)

        self.spider = spider

    def close(self, reason: str) -> None:
        """Called when spider closes."""
        pass

    def has_pending_requests(self) -> bool:
        """Check if there are pending requests."""
        return self.queue.len() > 0

    def enqueue_request(self, request: Request) -> bool:
        """Enqueue a request."""
        # Use dupefilter if request should be filtered
        if not request.dont_filter and self.dupefilter.request_seen(request):
            logger.debug(f"Duplicate request filtered: {request}")
            return False

        self.queue.push(self._encode_request(request), priority=request.priority)
        if self.stats:
            self.stats.inc_value("scheduler/enqueued", spider=self.spider)
        return True

    def next_request(self) -> Request | None:
        """Get next request from queue."""
        data = self.queue.pop(timeout=self.idle_before_close)
        if data:
            if self.stats:
                self.stats.inc_value("scheduler/dequeued", spider=self.spider)
            return self._decode_request(data)
        return None

    def _encode_request(self, request: Request) -> bytes:
        """Serialize request to JSON bytes."""
        return json.dumps(
            request_to_dict(request, spider=self.spider)
        ).encode("utf-8")

    def _decode_request(self, data: bytes) -> Request:
        """Deserialize JSON bytes to request."""
        return request_from_dict(
            json.loads(data.decode("utf-8")),
            spider=self.spider
        )

    def _request_fingerprint(self, request: Request) -> str:
        """Generate fingerprint for request."""
        from scrapy.utils.request import request_fingerprint
        return request_fingerprint(request)
```

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/components/scheduler.py \
        tests/unit/components/test_scheduler.py
git commit -m "feat: add DistributedScheduler component

- Scrapy-compatible scheduler using backend
- Request serialization with JSON
- Duplicate filtering via backend"
```

---

### Task 11: BackendDupeFilter

**Files:**
- Create: `src/scrapy_extension/components/dupefilter.py`
- Create: `tests/unit/components/test_dupefilter.py`

**Prerequisites:** Task 1 (BackendInterface)

- [ ] **Step 1: Write failing test**

- [ ] **Step 2: Run test**

- [ ] **Step 3: Implement BackendDupeFilter**

```python
# src/scrapy_extension/components/dupefilter.py
import logging
from typing import TYPE_CHECKING

from scrapy.dupefilters import BaseDupeFilter
from scrapy.http import Request
from scrapy.utils.request import request_fingerprint

from scrapy_extension.backend.factory import BackendFactory
from scrapy_extension.backend.interface import BackendInterface

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from scrapy.spiders import Spider

logger = logging.getLogger(__name__)


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
        self.key_template = key

    @classmethod
    def from_crawler(cls, crawler: "Crawler") -> "BackendDupeFilter":
        """Create from crawler."""
        settings = crawler.settings
        backend = BackendFactory.get_backend()
        return cls(
            backend=backend,
            key=settings.get("DUPEFILTER_KEY", "{spider}:dupefilter"),
            debug=settings.getbool("DUPEFILTER_DEBUG"),
        )

    def open(self, spider: "Spider") -> None:
        """Called when spider opens."""
        self.key = self.key_template.format(spider=spider.name)
        self.spider = spider

    def request_seen(self, request: Request) -> bool:
        """Return True if request has been seen before."""
        fp = request_fingerprint(request)
        return not self.backend.add_fingerprint(self.key, fp)

    def close(self, reason: str) -> None:
        """Close dupefilter."""
        pass

    def log(self, request: Request, spider: "Spider") -> None:
        """Log duplicate request."""
        if self.debug:
            logger.debug(f"Filtered duplicate request: {request}")
```

- [ ] **Step 4: Run test**

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/components/dupefilter.py \
        tests/unit/components/test_dupefilter.py
git commit -m "feat: add BackendDupeFilter component

- Scrapy-compatible dupefilter
- Uses backend fingerprint storage"
```

---

### Task 12: BackendPipeline

**Files:**
- Create: `src/scrapy_extension/components/pipeline.py`
- Create: `tests/unit/components/test_pipeline.py`

**Prerequisites:** Task 1 (BackendInterface)

**Reference:** See spec section 3.5 for full BackendPipeline implementation

- [ ] **Step 1-5:** Follow same pattern as Tasks 10-11

---

### Task 13: BackendSpiderMixin

**Files:**
- Create: `src/scrapy_extension/components/spider.py`
- Create: `tests/unit/components/test_spider.py`

**Prerequisites:** Task 1 (BackendInterface)

**Reference:** See spec section 3.7 for full BackendSpiderMixin implementation

- [ ] **Step 1-5:** Follow same pattern

---

## Phase 5: Utils

### Task 14: Utils Module (Optional)

**Files:**
- Create: `src/scrapy_extension/utils/__init__.py`

**Note:** Scrapy already provides `request_to_dict` and `request_from_dict` in `scrapy.utils.reqser`.
The `utils/` directory in the file structure is reserved for future helper functions. For MVP,
components should import directly from `scrapy.utils.reqser`.

Update the file structure to remove `utils/request.py` and `utils/fingerprint.py` from MVP scope:

```
src/scrapy_extension/
├── utils/
│   ├── __init__.py              # Empty for now (future helpers)
```

- [ ] **Step 1: Create empty utils module**

```python
# src/scrapy_extension/utils/__init__.py
"""Utility functions for scrapy-extension."""
# Future: Add helper functions here
```

- [ ] **Step 2: Commit**

```bash
git add src/scrapy_extension/utils/__init__.py
git commit -m "chore: add utils module placeholder

- Empty module for future helper functions
- request_to_dict/request_from_dict imported from scrapy.utils.reqser"
```

---

## Phase 6: Integration

### Task 15: Public API Exports

**Files:**
- Modify: `src/scrapy_extension/__init__.py`

- [ ] **Step 1: Update __init__.py with public exports**

```python
# src/scrapy_extension/__init__.py
"""Scrapy Extension - Multi-backend distributed crawling."""

from scrapy_extension.backend.interface import BackendInterface
from scrapy_extension.backend.factory import BackendFactory
from scrapy_extension.backend.redis import RedisConfig
from scrapy_extension.backend.mongo import MongoConfig
from scrapy_extension.backend.kafka import KafkaConfig
from scrapy_extension.backend.rabbitmq import RabbitMQConfig
from scrapy_extension.settings import (
    BackendType,
    ScrapyExtensionSettings,
)
from scrapy_extension.components.scheduler import DistributedScheduler
from scrapy_extension.components.dupefilter import BackendDupeFilter
from scrapy_extension.components.pipeline import BackendPipeline
from scrapy_extension.components.spider import BackendSpiderMixin
from scrapy_extension.components.queue import PriorityQueue

__all__ = [
    # Backend
    "BackendInterface",
    "BackendFactory",
    # Settings
    "BackendType",
    "ScrapyExtensionSettings",
    "RedisConfig",
    "MongoConfig",
    "KafkaConfig",
    "RabbitMQConfig",
    # Components
    "DistributedScheduler",
    "BackendDupeFilter",
    "BackendPipeline",
    "BackendSpiderMixin",
    "PriorityQueue",
]
```

- [ ] **Step 2: Commit**

```bash
git add src/scrapy_extension/__init__.py
git commit -m "feat: add public API exports

- Export all public classes and functions
- Clean __all__ definition"
```

---

## Phase 7: Tests

### Task 16: Integration Tests

**Files:**
- Create: `tests/integration/test_full_flow.py`

- [ ] **Step 1: Write integration test with mocked backend**

```python
# tests/integration/test_full_flow.py
"""Integration tests for full scrapy flow."""
import pytest
from unittest.mock import Mock, MagicMock

from scrapy import Spider, Request
from scrapy.crawler import Crawler
from scrapy.settings import Settings

from scrapy_extension.components.scheduler import DistributedScheduler
from scrapy_extension.components.dupefilter import BackendDupeFilter
from scrapy_extension.components.pipeline import BackendPipeline
from scrapy_extension.components.spider import BackendSpiderMixin


class MockBackend:
    """Mock backend for integration testing."""

    def __init__(self):
        self.queues = {}
        self.sets = {}
        self.collections = {}

    def push(self, queue_name, item, priority=0):
        if queue_name not in self.queues:
            self.queues[queue_name] = []
        self.queues[queue_name].append((priority, item))
        self.queues[queue_name].sort(reverse=True)

    def pop(self, queue_name, timeout=0):
        if queue_name in self.queues and self.queues[queue_name]:
            return self.queues[queue_name].pop(0)[1]
        return None

    def queue_len(self, queue_name):
        return len(self.queues.get(queue_name, []))

    def add_fingerprint(self, key, fingerprint):
        if key not in self.sets:
            self.sets[key] = set()
        if fingerprint in self.sets[key]:
            return False
        self.sets[key].add(fingerprint)
        return True

    def is_fingerprint_exists(self, key, fingerprint):
        return fingerprint in self.sets.get(key, set())

    def store_items_batch(self, collection, items):
        if collection not in self.collections:
            self.collections[collection] = []
        self.collections[collection].extend(items)


def test_full_spider_flow():
    """Test full spider flow with all components."""
    backend = MockBackend()

    # Create scheduler
    scheduler = DistributedScheduler(
        backend=backend,
        queue_key="test:requests",
        dupefilter_key="test:dupefilter",
    )

    # Create dupefilter
    dupefilter = BackendDupeFilter(
        backend=backend,
        key="test:dupefilter",
    )

    # Test request flow
    request1 = Request("http://example.com/1")
    request2 = Request("http://example.com/1")  # Duplicate
    request3 = Request("http://example.com/2")

    # Enqueue requests
    assert scheduler.enqueue_request(request1) is True
    assert scheduler.enqueue_request(request2) is False  # Duplicate
    assert scheduler.enqueue_request(request3) is True

    # Verify queue state
    assert backend.queue_len("test:requests") == 2

    # Dequeue requests
    next1 = scheduler.next_request()
    assert next1 is not None
    assert next1.url in ["http://example.com/1", "http://example.com/2"]

    next2 = scheduler.next_request()
    assert next2 is not None

    next3 = scheduler.next_request()
    assert next3 is None  # Queue empty
```

- [ ] **Step 2: Run test**

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_full_flow.py
git commit -m "test: add integration tests

- Full spider flow test with mock backend
- Tests scheduler, dupefilter integration"
```

---

## Summary

This plan implements a complete multi-backend Scrapy extension with:

1. **4 Backends:** Redis (standalone/sentinel/cluster), MongoDB (standalone/replica/sharded), Kafka, RabbitMQ
2. **5 Components:** Scheduler, DupeFilter, Pipeline, Queue, SpiderMixin
3. **Full Test Coverage:** Unit tests for all backends and components, integration tests
4. **Type Safety:** Pydantic settings with discriminator pattern
5. **Clean Architecture:** Unified BackendInterface, lazy singleton connections

Each task follows TDD:
1. Write failing test
2. Run to verify failure
3. Implement minimal code
4. Run to verify pass
5. Commit

**Estimated Tasks:** 16
**Estimated Commits:** 16+
**Dependencies:** Managed per-task with clear prerequisites
