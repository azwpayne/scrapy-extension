# Multi-Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use @superpowers:subagent-driven-development (recommended) or @superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement MongoDB, Kafka, and RabbitMQ backends for the scrapy-extension distributed crawling package.

**Architecture:** Three new backend classes implementing the protocol-based abstraction with elif-based ConnectionManager. MongoDB implements all protocols; Kafka and RabbitMQ implement QueueBackend only.

**Tech Stack:** Python 3.10+, pymongo, kafka-python, pika, pydantic-settings, pytest

---

## File Structure

### New Files
| File | Responsibility |
|------|----------------|
| `src/scrapy_extension/backends/mongodb_backend.py` | MongoDB backend implementing all protocols |
| `src/scrapy_extension/backends/kafka_backend.py` | Kafka backend implementing QueueBackend only |
| `src/scrapy_extension/backends/rabbitmq_backend.py` | RabbitMQ backend implementing QueueBackend only |
| `tests/test_mongodb_backend.py` | MongoDB backend unit tests |
| `tests/test_kafka_backend.py` | Kafka backend unit tests |
| `tests/test_rabbitmq_backend.py` | RabbitMQ backend unit tests |

### Modified Files
| File | Changes |
|------|---------|
| `src/scrapy_extension/config/settings.py` | Add MongoDBSettings, KafkaSettings, RabbitMQSettings |
| `src/scrapy_extension/connection/manager.py` | Add backend registry, update _create_backend() |
| `pyproject.toml` | Add optional dependencies for mongodb, kafka, rabbitmq |
| `src/scrapy_extension/__init__.py` | Export new backends and settings |

---

## Task 1: MongoDB Configuration Settings

**Files:**
- Modify: `src/scrapy_extension/config/settings.py`
- Test: `tests/test_config.py` (add MongoDB settings tests)

- [ ] **Step 1: Write the failing test for MongoDBSettings**

```python
def test_mongodb_settings_defaults():
    from scrapy_extension.config.settings import MongoDBSettings
    settings = MongoDBSettings()
    assert settings.uri == "mongodb://localhost:27017"
    assert settings.database == "scrapy_extension"
    assert settings.queue_collection == "queues"
    assert settings.set_collection == "sets"
    assert settings.storage_collection == "storage"
    assert settings.min_pool_size == 1
    assert settings.max_pool_size == 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py::test_mongodb_settings_defaults -v`
Expected: FAIL with "MongoDBSettings not defined"

- [ ] **Step 3: Implement MongoDBSettings class**

Add to `src/scrapy_extension/config/settings.py` after RedisSettings:

```python
class MongoDBSettings(BaseSettings):
    """MongoDB-specific settings.

    These settings configure the MongoDB connection and can be set
    via environment variables with the SCRAPY_MONGO_ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_MONGO_",
        case_sensitive=False,
        extra="ignore",
    )

    uri: str = Field(
        default="mongodb://localhost:27017",
        description="MongoDB connection URI",
    )
    database: str = Field(
        default="scrapy_extension",
        description="MongoDB database name",
    )
    queue_collection: str = Field(
        default="queues",
        description="Collection name for queue storage",
    )
    set_collection: str = Field(
        default="sets",
        description="Collection name for set storage",
    )
    storage_collection: str = Field(
        default="storage",
        description="Collection name for key-value storage",
    )

    # Connection pool settings
    min_pool_size: int = Field(
        default=1,
        ge=0,
        description="Minimum connection pool size",
    )
    max_pool_size: int = Field(
        default=10,
        ge=1,
        description="Maximum connection pool size",
    )
    max_idle_time_ms: int = Field(
        default=60000,
        ge=0,
        description="Maximum connection idle time in milliseconds",
    )
    wait_queue_timeout_ms: int = Field(
        default=5000,
        ge=0,
        description="Maximum wait time for connection from pool",
    )

    # Write concern
    w: int | str = Field(
        default=1,
        description="Write concern (1, 'majority', or integer)",
    )
    journal: bool = Field(
        default=True,
        description="Wait for journal commit",
    )
    read_preference: str = Field(
        default="primary",
        description="Read preference (primary, secondary, nearest)",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py::test_mongodb_settings_defaults -v`
Expected: PASS

- [ ] **Step 5: Test environment variable loading**

```python
def test_mongodb_settings_from_env(monkeypatch):
    from scrapy_extension.config.settings import MongoDBSettings
    monkeypatch.setenv("SCRAPY_MONGO_URI", "mongodb://custom:27017")
    monkeypatch.setenv("SCRAPY_MONGO_DATABASE", "custom_db")
    settings = MongoDBSettings()
    assert settings.uri == "mongodb://custom:27017"
    assert settings.database == "custom_db"
```

- [ ] **Step 6: Run env test**

Run: `pytest tests/test_config.py::test_mongodb_settings_from_env -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/scrapy_extension/config/settings.py tests/test_config.py
git commit -m "feat: add MongoDBSettings configuration class"
```

---

## Task 2: MongoDB Backend Implementation - Connection

**Files:**
- Create: `src/scrapy_extension/backends/mongodb_backend.py` (initial structure)
- Test: `tests/test_mongodb_backend.py` (connection tests)

- [ ] **Step 1: Write failing test for MongoDBBackend connection**

```python
import pytest
from unittest.mock import MagicMock, patch
from scrapy_extension.backends.mongodb_backend import MongoDBBackend
from scrapy_extension.config.settings import MongoDBSettings


def test_mongodb_backend_connect():
    """Test MongoDB backend connection."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        backend.connect()

        mock_client.assert_called_once()
        mock_instance.admin.command.assert_called_once_with("ping")
        assert backend.is_connected()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_connect -v`
Expected: FAIL with "mongodb_backend not found"

- [ ] **Step 3: Create MongoDBBackend class with connection methods**

Create `src/scrapy_extension/backends/mongodb_backend.py`:

```python
"""MongoDB backend implementation.

This module provides a MongoDB-based implementation of the backend interfaces
for distributed crawling.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError, PyMongoError

from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.exceptions import BackendConnectionError

if TYPE_CHECKING:
    from scrapy_extension.config.settings import MongoDBSettings

logger = logging.getLogger(__name__)


class MongoDBBackend(Backend, QueueBackend, SetBackend, StorageBackend):
    """MongoDB backend implementation.

    Implements all backend interfaces using MongoDB collections:
    - Queue: Collection with priority and created_at fields
    - Set: Collection with unique index on (set_name, item_hash)
    - Storage: Collection with TTL index on expireAt

    Attributes:
        config: MongoDBSettings instance with connection parameters.
        _client: The MongoDB client instance (None until connected).
        _db: The MongoDB database instance.
    """

    def __init__(self, config: MongoDBSettings) -> None:
        """Initialize MongoDB backend.

        Args:
            config: Configuration for MongoDB connection.
        """
        self.config = config
        self._client: MongoClient | None = None
        self._db = None
        self._queue_collection = None
        self._set_collection = None
        self._storage_collection = None

    def connect(self) -> None:
        """Establish connection to MongoDB.

        Creates a MongoDB client and initializes collections with indexes.

        Raises:
            BackendConnectionError: If the connection cannot be established.
        """
        try:
            self._client = MongoClient(
                self.config.uri,
                minPoolSize=self.config.min_pool_size,
                maxPoolSize=self.config.max_pool_size,
                maxIdleTimeMS=self.config.max_idle_time_ms,
                waitQueueTimeoutMS=self.config.wait_queue_timeout_ms,
                w=self.config.w,
                journal=self.config.journal,
            )
            # Verify connection
            self._client.admin.command("ping")

            # Initialize database and collections
            self._db = self._client[self.config.database]
            self._queue_collection = self._db[self.config.queue_collection]
            self._set_collection = self._db[self.config.set_collection]
            self._storage_collection = self._db[self.config.storage_collection]

            # Create indexes
            self._create_indexes()

            logger.debug("Connected to MongoDB at %s", self.config.uri)
        except ConnectionFailure as e:
            msg = f"Failed to connect to MongoDB: {e}"
            raise BackendConnectionError(
                msg,
                backend_type="mongodb",
            ) from e

    def _create_indexes(self) -> None:
        """Create necessary indexes for collections."""
        # Queue indexes
        self._queue_collection.create_index(
            [("queue_name", ASCENDING), ("priority", ASCENDING), ("created_at", ASCENDING)]
        )

        # Set indexes
        self._set_collection.create_index(
            [("set_name", ASCENDING), ("item_hash", ASCENDING)],
            unique=True,
        )

        # Storage indexes
        self._storage_collection.create_index("key", unique=True)
        self._storage_collection.create_index(
            "expireAt",
            expireAfterSeconds=0,
        )

    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            self._queue_collection = None
            self._set_collection = None
            self._storage_collection = None

    def is_connected(self) -> bool:
        """Check if MongoDB is connected.

        Returns:
            True if connected and responding to ping.
        """
        try:
            if self._client is None:
                return False
            self._client.admin.command("ping")
            return True
        except PyMongoError:
            return False

    def ping(self) -> bool:
        """Check MongoDB health.

        Returns:
            True if MongoDB responds to ping.
        """
        return self.is_connected()

    @property
    def backend_type(self) -> BackendType:
        """Return backend type.

        Returns:
            BackendType.MONGODB
        """
        return BackendType.MONGODB
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_connect -v`
Expected: PASS

- [ ] **Step 5: Add disconnect test**

```python
def test_mongodb_backend_disconnect():
    """Test MongoDB backend disconnection."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        backend.connect()
        assert backend.is_connected()

        backend.disconnect()
        assert not backend.is_connected()
        mock_instance.close.assert_called_once()
```

- [ ] **Step 6: Run disconnect test**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_disconnect -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/scrapy_extension/backends/mongodb_backend.py tests/test_mongodb_backend.py
git commit -m "feat: add MongoDBBackend connection management"
```

---

## Task 3: MongoDB Backend - QueueBackend Implementation

**Files:**
- Modify: `src/scrapy_extension/backends/mongodb_backend.py` (add QueueBackend methods)
- Test: `tests/test_mongodb_backend.py` (queue tests)

- [ ] **Step 1: Write failing test for push/pop**

```python
def test_mongodb_backend_push_pop():
    """Test MongoDB backend push and pop operations."""
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBSettings

    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._queue_collection = mock_collection

        # Test push
        backend.push("test_queue", b"test_item", priority=1.0)
        mock_collection.insert_one.assert_called_once()
        call_args = mock_collection.insert_one.call_args[0][0]
        assert call_args["queue_name"] == "test_queue"
        assert call_args["item"] == b"test_item"
        assert call_args["priority"] == -1.0  # Negated

        # Test pop
        mock_collection.find_one_and_delete.return_value = {
            "queue_name": "test_queue",
            "item": b"test_item",
        }
        result = backend.pop("test_queue")
        assert result == b"test_item"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_push_pop -v`
Expected: FAIL with "push/pop not implemented"

- [ ] **Step 3: Implement QueueBackend methods**

Add to `src/scrapy_extension/backends/mongodb_backend.py` after the backend_type property:

```python
    # QueueBackend implementation
    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        """Push item to priority queue.

        Args:
            queue_name: Name of the queue.
            item: Item to push (bytes).
            priority: Priority value (higher = more urgent).
        """
        doc = {
            "queue_name": queue_name,
            "item": item,
            "priority": -priority,  # Negated for DESC sort
            "created_at": datetime.utcnow(),
        }
        self._queue_collection.insert_one(doc)

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop highest priority item from queue.

        Args:
            queue_name: Name of the queue.
            timeout: Seconds to wait (0 = non-blocking).

        Returns:
            The popped item, or None if queue is empty.
        """
        # MongoDB doesn't support blocking pop, so we ignore timeout
        result = self._queue_collection.find_one_and_delete(
            {"queue_name": queue_name},
            sort=[("priority", ASCENDING), ("created_at", ASCENDING)],
        )
        if result:
            return result["item"]
        return None

    def queue_len(self, queue_name: str) -> int:
        """Get queue length.

        Args:
            queue_name: Name of the queue.

        Returns:
            Number of items in the queue.
        """
        return self._queue_collection.count_documents({"queue_name": queue_name})

    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue.

        Args:
            queue_name: Name of the queue.
        """
        self._queue_collection.delete_many({"queue_name": queue_name})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_push_pop -v`
Expected: PASS

- [ ] **Step 5: Add queue_len and clear_queue tests**

```python
def test_mongodb_backend_queue_len():
    """Test MongoDB backend queue length."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._queue_collection = mock_collection
        mock_collection.count_documents.return_value = 5

        result = backend.queue_len("test_queue")
        assert result == 5
        mock_collection.count_documents.assert_called_once_with({"queue_name": "test_queue"})


def test_mongodb_backend_clear_queue():
    """Test MongoDB backend clear queue."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._queue_collection = mock_collection

        backend.clear_queue("test_queue")
        mock_collection.delete_many.assert_called_once_with({"queue_name": "test_queue"})
```

- [ ] **Step 6: Run queue_len and clear_queue tests**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_queue_len tests/test_mongodb_backend.py::test_mongodb_backend_clear_queue -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/scrapy_extension/backends/mongodb_backend.py tests/test_mongodb_backend.py
git commit -m "feat: implement MongoDBBackend QueueBackend methods"
```

---

## Task 4: MongoDB Backend - SetBackend Implementation

**Files:**
- Modify: `src/scrapy_extension/backends/mongodb_backend.py` (add SetBackend methods)
- Test: `tests/test_mongodb_backend.py` (set tests)

- [ ] **Step 1: Write failing test for SetBackend**

```python
def test_mongodb_backend_set_operations():
    """Test MongoDB backend set operations."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._set_collection = mock_collection

        # Test add
        mock_collection.insert_one.return_value = MagicMock()
        result = backend.add("test_set", b"test_item")
        assert result is True
        mock_collection.insert_one.assert_called_once()

        # Test contains (item exists)
        mock_collection.find_one.return_value = {"set_name": "test_set", "item_hash": "abc123"}
        result = backend.contains("test_set", b"test_item")
        assert result is True

        # Test contains (item not exists)
        mock_collection.find_one.return_value = None
        result = backend.contains("test_set", b"other_item")
        assert result is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_set_operations -v`
Expected: FAIL

- [ ] **Step 3: Implement SetBackend methods**

Add to `src/scrapy_extension/backends/mongodb_backend.py` after QueueBackend methods:

```python
    # SetBackend implementation
    def _hash_item(self, item: bytes) -> str:
        """Generate hash for item.

        Args:
            item: Item to hash.

        Returns:
            SHA256 hex digest of item.
        """
        return hashlib.sha256(item).hexdigest()

    def add(self, set_name: str, item: bytes) -> bool:
        """Add item to set.

        Args:
            set_name: Name of the set.
            item: Item to add (bytes).

        Returns:
            True if added, False if already existed.
        """
        doc = {
            "set_name": set_name,
            "item_hash": self._hash_item(item),
            "item": item,
            "created_at": datetime.utcnow(),
        }
        try:
            self._set_collection.insert_one(doc)
            return True
        except DuplicateKeyError:
            return False

    def remove(self, set_name: str, item: bytes) -> bool:
        """Remove item from set.

        Args:
            set_name: Name of the set.
            item: Item to remove.

        Returns:
            True if removed, False if didn't exist.
        """
        result = self._set_collection.delete_one({
            "set_name": set_name,
            "item_hash": self._hash_item(item),
        })
        return result.deleted_count > 0

    def contains(self, set_name: str, item: bytes) -> bool:
        """Check if item is in set.

        Args:
            set_name: Name of the set.
            item: Item to check.

        Returns:
            True if item exists in the set.
        """
        result = self._set_collection.find_one({
            "set_name": set_name,
            "item_hash": self._hash_item(item),
        })
        return result is not None

    def set_len(self, set_name: str) -> int:
        """Get set size.

        Args:
            set_name: Name of the set.

        Returns:
            Number of items in the set.
        """
        return self._set_collection.count_documents({"set_name": set_name})

    def clear_set(self, set_name: str) -> None:
        """Clear all items from set.

        Args:
            set_name: Name of the set.
        """
        self._set_collection.delete_many({"set_name": set_name})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_set_operations -v`
Expected: PASS

- [ ] **Step 5: Add remove and set_len tests**

```python
def test_mongodb_backend_set_remove():
    """Test MongoDB backend set remove."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._set_collection = mock_collection

        # Test remove success
        mock_delete_result = MagicMock()
        mock_delete_result.deleted_count = 1
        mock_collection.delete_one.return_value = mock_delete_result
        result = backend.remove("test_set", b"test_item")
        assert result is True

        # Test remove failure (not found)
        mock_delete_result.deleted_count = 0
        result = backend.remove("test_set", b"missing_item")
        assert result is False


def test_mongodb_backend_set_len():
    """Test MongoDB backend set length."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._set_collection = mock_collection
        mock_collection.count_documents.return_value = 3

        result = backend.set_len("test_set")
        assert result == 3
```

- [ ] **Step 6: Run tests**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_set_remove tests/test_mongodb_backend.py::test_mongodb_backend_set_len -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/scrapy_extension/backends/mongodb_backend.py tests/test_mongodb_backend.py
git commit -m "feat: implement MongoDBBackend SetBackend methods"
```

---

## Task 5: MongoDB Backend - StorageBackend Implementation

**Files:**
- Modify: `src/scrapy_extension/backends/mongodb_backend.py` (add StorageBackend methods)
- Test: `tests/test_mongodb_backend.py` (storage tests)

- [ ] **Step 1: Write failing test for StorageBackend**

```python
def test_mongodb_backend_storage_operations():
    """Test MongoDB backend storage operations."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._storage_collection = mock_collection

        # Test store
        backend.store("test_key", b"test_data")
        mock_collection.replace_one.assert_called_once()

        # Test retrieve
        mock_collection.find_one.return_value = {"key": "test_key", "data": b"test_data"}
        result = backend.retrieve("test_key")
        assert result == b"test_data"

        # Test exists
        result = backend.exists("test_key")
        assert result is True

        # Test delete
        mock_delete_result = MagicMock()
        mock_delete_result.deleted_count = 1
        mock_collection.delete_one.return_value = mock_delete_result
        result = backend.delete("test_key")
        assert result is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_storage_operations -v`
Expected: FAIL

- [ ] **Step 3: Implement StorageBackend methods**

Add to `src/scrapy_extension/backends/mongodb_backend.py` after SetBackend methods:

```python
    # StorageBackend implementation
    def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
        """Store data with key.

        Args:
            key: Storage key.
            data: Data to store (bytes).
            ttl: Optional time-to-live in seconds.
        """
        doc = {
            "key": key,
            "data": data,
        }
        if ttl is not None:
            doc["expireAt"] = datetime.utcnow() + timedelta(seconds=ttl)

        self._storage_collection.replace_one(
            {"key": key},
            doc,
            upsert=True,
        )

    def retrieve(self, key: str) -> bytes | None:
        """Retrieve data by key.

        Args:
            key: Storage key.

        Returns:
            Stored data, or None if not found.
        """
        result = self._storage_collection.find_one({"key": key})
        if result:
            return result.get("data")
        return None

    def delete(self, key: str) -> bool:
        """Delete data by key.

        Args:
            key: Storage key.

        Returns:
            True if deleted, False if didn't exist.
        """
        result = self._storage_collection.delete_one({"key": key})
        return result.deleted_count > 0

    def exists(self, key: str) -> bool:
        """Check if key exists.

        Args:
            key: Storage key.

        Returns:
            True if key exists.
        """
        result = self._storage_collection.find_one({"key": key}, {"_id": 1})
        return result is not None

    def ttl(self, key: str) -> int | None:
        """Get remaining time-to-live.

        Args:
            key: Storage key.

        Returns:
            Seconds remaining, None if no TTL, -1 if expired.
        """
        result = self._storage_collection.find_one({"key": key}, {"expireAt": 1})
        if result is None:
            return -1
        if "expireAt" not in result:
            return None

        expire_at = result["expireAt"]
        remaining = (expire_at - datetime.utcnow()).total_seconds()
        return max(0, int(remaining))

    def clear_storage(self, prefix: str | None = None) -> None:
        """Clear all stored data, optionally filtered by prefix.

        Args:
            prefix: If provided, only clear keys starting with this prefix.
                   If None, clear all storage data.
        """
        if prefix:
            import re
            pattern = re.escape(prefix)
            self._storage_collection.delete_many({"key": {"$regex": f"^{pattern}"}})
        else:
            self._storage_collection.delete_many({})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_storage_operations -v`
Expected: PASS

- [ ] **Step 5: Add TTL test**

```python
def test_mongodb_backend_storage_ttl():
    """Test MongoDB backend storage TTL."""
    from datetime import datetime, timedelta

    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("pymongo.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._storage_collection = mock_collection

        # Test with TTL
        future_time = datetime.utcnow() + timedelta(seconds=3600)
        mock_collection.find_one.return_value = {"key": "test_key", "expireAt": future_time}

        result = backend.ttl("test_key")
        assert result is not None
        assert 3590 <= result <= 3600  # Allow for execution time

        # Test without TTL
        mock_collection.find_one.return_value = {"key": "test_key"}
        result = backend.ttl("test_key")
        assert result is None

        # Test non-existent key
        mock_collection.find_one.return_value = None
        result = backend.ttl("missing_key")
        assert result == -1
```

- [ ] **Step 6: Run TTL test**

Run: `pytest tests/test_mongodb_backend.py::test_mongodb_backend_storage_ttl -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/scrapy_extension/backends/mongodb_backend.py tests/test_mongodb_backend.py
git commit -m "feat: implement MongoDBBackend StorageBackend methods"
```

---

## Task 6: Kafka Configuration Settings

**Files:**
- Modify: `src/scrapy_extension/config/settings.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write failing test for KafkaSettings**

```python
def test_kafka_settings_defaults():
    from scrapy_extension.config.settings import KafkaSettings
    settings = KafkaSettings()
    assert settings.bootstrap_servers == "localhost:9092"
    assert settings.max_priority_partitions == 10
    assert settings.acks == "all"
    assert settings.group_id == "scrapy-extension"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py::test_kafka_settings_defaults -v`
Expected: FAIL

- [ ] **Step 3: Implement KafkaSettings class**

Add to `src/scrapy_extension/config/settings.py` after MongoDBSettings:

```python
class KafkaSettings(BaseSettings):
    """Kafka-specific settings.

    These settings configure the Kafka connection and can be set
    via environment variables with the SCRAPY_KAFKA_ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_KAFKA_",
        case_sensitive=False,
        extra="ignore",
    )

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers",
    )
    max_priority_partitions: int = Field(
        default=10,
        ge=1,
        le=255,
        description="Number of partitions for priority support",
    )

    # Producer settings
    acks: str | int = Field(
        default="all",
        description="Producer acknowledgment level (0, 1, 'all')",
    )
    retries: int = Field(
        default=3,
        ge=0,
        description="Number of retries for failed sends",
    )
    batch_size: int = Field(
        default=16384,
        ge=0,
        description="Batch size in bytes",
    )
    linger_ms: int = Field(
        default=5,
        ge=0,
        description="Time to wait for batching",
    )
    compression_type: str | None = Field(
        default=None,
        description="Compression type (gzip, snappy, lz4, zstd)",
    )

    # Consumer settings
    group_id: str = Field(
        default="scrapy-extension",
        description="Consumer group ID",
    )
    auto_offset_reset: str = Field(
        default="earliest",
        description="Offset reset policy (earliest, latest)",
    )
    enable_auto_commit: bool = Field(
        default=True,
        description="Enable automatic offset commits",
    )
    auto_commit_interval_ms: int = Field(
        default=5000,
        ge=0,
        description="Auto commit interval in milliseconds",
    )
    max_poll_records: int = Field(
        default=500,
        ge=1,
        description="Maximum records per poll",
    )
    session_timeout_ms: int = Field(
        default=10000,
        ge=0,
        description="Session timeout in milliseconds",
    )

    # Topic settings
    replication_factor: int = Field(
        default=1,
        ge=1,
        description="Topic replication factor",
    )
    num_partitions: int = Field(
        default=10,
        ge=1,
        description="Number of partitions for new topics",
    )
    retention_ms: int = Field(
        default=604800000,  # 7 days
        ge=0,
        description="Topic retention time in milliseconds",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py::test_kafka_settings_defaults -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/config/settings.py tests/test_config.py
git commit -m "feat: add KafkaSettings configuration class"
```

---

## Task 7: Kafka Backend Implementation

**Files:**
- Create: `src/scrapy_extension/backends/kafka_backend.py`
- Test: `tests/test_kafka_backend.py`

- [ ] **Step 1: Write failing test for KafkaBackend connection**

```python
import pytest
from unittest.mock import MagicMock, patch
from scrapy_extension.backends.kafka_backend import KafkaBackend
from scrapy_extension.config.settings import KafkaSettings


def test_kafka_backend_connect():
    """Test Kafka backend connection."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    with patch("kafka.KafkaProducer") as mock_producer, \
         patch("kafka.KafkaConsumer") as mock_consumer:
        mock_producer.return_value = MagicMock()
        mock_consumer.return_value = MagicMock()

        backend.connect()

        mock_producer.assert_called_once()
        assert backend.is_connected()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_kafka_backend.py::test_kafka_backend_connect -v`
Expected: FAIL

- [ ] **Step 3: Create KafkaBackend class**

Create `src/scrapy_extension/backends/kafka_backend.py`:

```python
"""Kafka backend implementation.

This module provides a Kafka-based implementation of QueueBackend.
Note: Kafka does not support SetBackend or StorageBackend operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

from scrapy_extension.backends.base import Backend, BackendType, QueueBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError

if TYPE_CHECKING:
    from scrapy_extension.config.settings import KafkaSettings

logger = logging.getLogger(__name__)


class KafkaBackend(Backend, QueueBackend):
    """Kafka backend implementation.

    Implements QueueBackend using Kafka topics with partition-based priority.
    Does NOT implement SetBackend or StorageBackend.

    Attributes:
        config: KafkaSettings instance with connection parameters.
        _producer: The Kafka producer instance.
        _consumer: The Kafka consumer instance.
    """

    def __init__(self, config: KafkaSettings) -> None:
        """Initialize Kafka backend.

        Args:
            config: Configuration for Kafka connection.
        """
        self.config = config
        self._producer: KafkaProducer | None = None
        self._consumer: KafkaConsumer | None = None
        self._admin_client: KafkaAdminClient | None = None

    def connect(self) -> None:
        """Establish connection to Kafka.

        Creates Kafka producer and admin client.

        Raises:
            BackendConnectionError: If the connection cannot be established.
        """
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                acks=self.config.acks,
                retries=self.config.retries,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                compression_type=self.config.compression_type,
            )
            self._admin_client = KafkaAdminClient(
                bootstrap_servers=self.config.bootstrap_servers,
                client_id="scrapy-extension-admin",
            )
            logger.debug("Connected to Kafka at %s", self.config.bootstrap_servers)
        except KafkaError as e:
            msg = f"Failed to connect to Kafka: {e}"
            raise BackendConnectionError(
                msg,
                backend_type="kafka",
            ) from e

    def disconnect(self) -> None:
        """Close Kafka connection."""
        if self._producer:
            self._producer.close()
            self._producer = None
        if self._consumer:
            self._consumer.close()
            self._consumer = None
        if self._admin_client:
            self._admin_client.close()
            self._admin_client = None

    def is_connected(self) -> bool:
        """Check if Kafka is connected.

        Returns:
            True if producer is available.
        """
        return self._producer is not None

    def ping(self) -> bool:
        """Check Kafka health.

        Returns:
            True if Kafka brokers are reachable.
        """
        try:
            if self._admin_client:
                self._admin_client.list_topics()
                return True
            return False
        except KafkaError:
            return False

    @property
    def backend_type(self) -> BackendType:
        """Return backend type.

        Returns:
            BackendType.KAFKA
        """
        return BackendType.KAFKA

    def _ensure_topic_exists(self, queue_name: str) -> None:
        """Ensure Kafka topic exists for queue.

        Args:
            queue_name: Name of the queue/topic.
        """
        topic_name = f"scrapy-{queue_name}"
        try:
            topics = self._admin_client.list_topics()
            if topic_name not in topics:
                new_topic = NewTopic(
                    name=topic_name,
                    num_partitions=self.config.max_priority_partitions,
                    replication_factor=self.config.replication_factor,
                )
                self._admin_client.create_topics([new_topic])
        except KafkaError as e:
            logger.warning("Failed to create topic %s: %s", topic_name, e)

    # QueueBackend implementation
    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        """Push item to priority queue.

        Args:
            queue_name: Name of the queue.
            item: Item to push (bytes).
            priority: Priority value (higher = more urgent, max 255).

        Raises:
            QueueError: If the push operation fails.
        """
        try:
            self._ensure_topic_exists(queue_name)
            topic_name = f"scrapy-{queue_name}"
            partition = min(int(priority), self.config.max_priority_partitions - 1)

            future = self._producer.send(topic_name, value=item, partition=partition)
            # Wait for send to complete (synchronous for reliability)
            future.get(timeout=10)
        except KafkaError as e:
            msg = f"Failed to push to queue {queue_name}: {e}"
            raise QueueError(
                msg,
                queue_name=queue_name,
                operation="push",
            ) from e

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop highest priority item from queue.

        Args:
            queue_name: Name of the queue.
            timeout: Seconds to wait (0 = non-blocking).

        Returns:
            The popped item, or None if queue is empty.

        Raises:
            QueueError: If the pop operation fails.
        """
        try:
            topic_name = f"scrapy-{queue_name}"

            # Create consumer if not exists
            if self._consumer is None:
                self._consumer = KafkaConsumer(
                    bootstrap_servers=self.config.bootstrap_servers,
                    group_id=self.config.group_id,
                    auto_offset_reset=self.config.auto_offset_reset,
                    enable_auto_commit=self.config.enable_auto_commit,
                    auto_commit_interval_ms=self.config.auto_commit_interval_ms,
                    max_poll_records=self.config.max_poll_records,
                    session_timeout_ms=self.config.session_timeout_ms,
                )

            self._consumer.subscribe([topic_name])

            # Poll for messages
            timeout_ms = int(timeout * 1000)
            messages = self._consumer.poll(timeout_ms=timeout_ms, max_records=1)

            for topic_partition, records in messages.items():
                for record in records:
                    return record.value

            return None
        except KafkaError as e:
            msg = f"Failed to pop from queue {queue_name}: {e}"
            raise QueueError(
                msg,
                queue_name=queue_name,
                operation="pop",
            ) from e

    def queue_len(self, queue_name: str) -> int:
        """Get queue length.

        Args:
            queue_name: Name of the queue.

        Returns:
            Approximate number of items in the queue.

        Note:
            This is eventually consistent and should be used for monitoring only.
        """
        try:
            topic_name = f"scrapy-{queue_name}"
            partitions = self._admin_client.describe_topics([topic_name])

            total = 0
            for partition in partitions[0]["partitions"]:
                partition_id = partition["partition"]
                end_offset = self._admin_client.list_offsets(
                    topic_name, partition_id, "latest"
                )
                begin_offset = self._admin_client.list_offsets(
                    topic_name, partition_id, "earliest"
                )
                total += end_offset - begin_offset

            return total
        except KafkaError:
            return 0

    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue.

        Args:
            queue_name: Name of the queue.
        """
        try:
            topic_name = f"scrapy-{queue_name}"
            self._admin_client.delete_topics([topic_name])
            # Recreate topic
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=self.config.max_priority_partitions,
                replication_factor=self.config.replication_factor,
            )
            self._admin_client.create_topics([new_topic])
        except KafkaError as e:
            logger.warning("Failed to clear queue %s: %s", queue_name, e)

    # Note: KafkaBackend only implements QueueBackend
    # SetBackend and StorageBackend methods are not included
    # because Kafka only supports queue operations

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_kafka_backend.py::test_kafka_backend_connect -v`
Expected: PASS

- [ ] **Step 5: Add push/pop tests**

```python
def test_kafka_backend_push():
    """Test Kafka backend push."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    with patch("kafka.KafkaProducer") as mock_producer, \
         patch("kafka.admin.KafkaAdminClient"):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_future = MagicMock()
        mock_producer_instance.send.return_value = mock_future

        backend.connect()
        backend.push("test_queue", b"test_item", priority=1.0)

        mock_producer_instance.send.assert_called_once()
        call_kwargs = mock_producer_instance.send.call_args[1]
        assert call_kwargs["topic"] == "scrapy-test_queue"
        assert call_kwargs["value"] == b"test_item"
        assert call_kwargs["partition"] == 1


def test_kafka_backend_only_implements_queuebackend():
    """Test that KafkaBackend only implements QueueBackend protocol."""
    from scrapy_extension.backends.base import QueueBackend, Backend

    config = KafkaSettings()
    backend = KafkaBackend(config)

    # Should implement Backend and QueueBackend
    assert isinstance(backend, Backend)
    assert isinstance(backend, QueueBackend)
```

- [ ] **Step 6: Run tests**

Run: `pytest tests/test_kafka_backend.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/scrapy_extension/backends/kafka_backend.py tests/test_kafka_backend.py
git commit -m "feat: add KafkaBackend implementation"
```

---

## Task 8: RabbitMQ Configuration Settings

**Files:**
- Modify: `src/scrapy_extension/config/settings.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write failing test for RabbitMQSettings**

```python
def test_rabbitmq_settings_defaults():
    from scrapy_extension.config.settings import RabbitMQSettings
    settings = RabbitMQSettings()
    assert settings.host == "localhost"
    assert settings.port == 5672
    assert settings.username == "guest"
    assert settings.password == "guest"
    assert settings.max_priority == 255
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py::test_rabbitmq_settings_defaults -v`
Expected: FAIL

- [ ] **Step 3: Implement RabbitMQSettings class**

Add to `src/scrapy_extension/config/settings.py` after KafkaSettings:

```python
class RabbitMQSettings(BaseSettings):
    """RabbitMQ-specific settings.

    These settings configure the RabbitMQ connection and can be set
    via environment variables with the SCRAPY_RABBITMQ_ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_RABBITMQ_",
        case_sensitive=False,
        extra="ignore",
    )

    host: str = Field(
        default="localhost",
        description="RabbitMQ server hostname",
    )
    port: int = Field(
        default=5672,
        ge=1,
        le=65535,
        description="RabbitMQ server port",
    )
    username: str = Field(
        default="guest",
        description="RabbitMQ username",
    )
    password: str = Field(
        default="guest",
        description="RabbitMQ password",
    )
    virtual_host: str = Field(
        default="/",
        description="RabbitMQ virtual host",
    )

    # Connection settings
    max_priority: int = Field(
        default=255,
        ge=1,
        le=255,
        description="Maximum priority level (1-255)",
    )
    heartbeat: int = Field(
        default=600,
        ge=0,
        description="Heartbeat interval in seconds",
    )
    blocked_connection_timeout: int = Field(
        default=300,
        ge=0,
        description="Blocked connection timeout in seconds",
    )

    # Queue settings
    durable: bool = Field(
        default=True,
        description="Create durable queues",
    )
    auto_delete: bool = Field(
        default=False,
        description="Auto-delete queues when last consumer unsubscribes",
    )
    delivery_mode: int = Field(
        default=2,
        ge=1,
        le=2,
        description="Message delivery mode (1=transient, 2=persistent)",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py::test_rabbitmq_settings_defaults -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/scrapy_extension/config/settings.py tests/test_config.py
git commit -m "feat: add RabbitMQSettings configuration class"
```

---

## Task 9: RabbitMQ Backend Implementation

**Files:**
- Create: `src/scrapy_extension/backends/rabbitmq_backend.py`
- Test: `tests/test_rabbitmq_backend.py`

- [ ] **Step 1: Write failing test for RabbitMQBackend connection**

```python
import pytest
from unittest.mock import MagicMock, patch
from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
from scrapy_extension.config.settings import RabbitMQSettings


def test_rabbitmq_backend_connect():
    """Test RabbitMQ backend connection."""
    config = RabbitMQSettings()
    backend = RabbitMQBackend(config)

    with patch("pika.BlockingConnection") as mock_conn:
        mock_instance = MagicMock()
        mock_conn.return_value = mock_instance

        backend.connect()

        mock_conn.assert_called_once()
        assert backend.is_connected()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rabbitmq_backend.py::test_rabbitmq_backend_connect -v`
Expected: FAIL

- [ ] **Step 3: Create RabbitMQBackend class**

Create `src/scrapy_extension/backends/rabbitmq_backend.py`:

```python
"""RabbitMQ backend implementation.

This module provides a RabbitMQ-based implementation of QueueBackend.
Note: RabbitMQ does not support SetBackend or StorageBackend operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError

from scrapy_extension.backends.base import Backend, BackendType, QueueBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError

if TYPE_CHECKING:
    from scrapy_extension.config.settings import RabbitMQSettings

logger = logging.getLogger(__name__)


class RabbitMQBackend(Backend, QueueBackend):
    """RabbitMQ backend implementation.

    Implements QueueBackend using RabbitMQ priority queues.
    Does NOT implement SetBackend or StorageBackend.

    Attributes:
        config: RabbitMQSettings instance with connection parameters.
        _connection: The RabbitMQ connection instance.
        _channel: The RabbitMQ channel instance.
    """

    def __init__(self, config: RabbitMQSettings) -> None:
        """Initialize RabbitMQ backend.

        Args:
            config: Configuration for RabbitMQ connection.
        """
        self.config = config
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    def connect(self) -> None:
        """Establish connection to RabbitMQ.

        Creates RabbitMQ connection and channel.

        Raises:
            BackendConnectionError: If the connection cannot be established.
        """
        try:
            credentials = pika.PlainCredentials(
                self.config.username,
                self.config.password,
            )
            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                virtual_host=self.config.virtual_host,
                credentials=credentials,
                heartbeat=self.config.heartbeat,
                blocked_connection_timeout=self.config.blocked_connection_timeout,
            )
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            logger.debug("Connected to RabbitMQ at %s:%s", self.config.host, self.config.port)
        except AMQPError as e:
            msg = f"Failed to connect to RabbitMQ: {e}"
            raise BackendConnectionError(
                msg,
                backend_type="rabbitmq",
            ) from e

    def disconnect(self) -> None:
        """Close RabbitMQ connection."""
        if self._channel:
            try:
                self._channel.close()
            except AMQPError as e:
                logger.warning("Error closing channel: %s", e)
            finally:
                self._channel = None

        if self._connection:
            try:
                self._connection.close()
            except AMQPError as e:
                logger.warning("Error closing connection: %s", e)
            finally:
                self._connection = None

    def is_connected(self) -> bool:
        """Check if RabbitMQ is connected.

        Returns:
            True if connection is open.
        """
        return (
            self._connection is not None
            and self._connection.is_open
            and self._channel is not None
            and self._channel.is_open
        )

    def ping(self) -> bool:
        """Check RabbitMQ health.

        Returns:
            True if RabbitMQ responds.
        """
        try:
            if self._channel:
                # Try a basic operation
                self._channel.exchange_declare(
                    exchange="ping",
                    exchange_type="direct",
                    passive=True,
                )
                return True
            return False
        except AMQPError:
            return False

    @property
    def backend_type(self) -> BackendType:
        """Return backend type.

        Returns:
            BackendType.RABBITMQ
        """
        return BackendType.RABBITMQ

    def _ensure_queue_exists(self, queue_name: str) -> None:
        """Ensure RabbitMQ queue exists.

        Args:
            queue_name: Name of the queue.
        """
        args = {"x-max-priority": self.config.max_priority}
        self._channel.queue_declare(
            queue=queue_name,
            durable=self.config.durable,
            auto_delete=self.config.auto_delete,
            arguments=args,
        )

    # QueueBackend implementation
    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        """Push item to priority queue.

        Args:
            queue_name: Name of the queue.
            item: Item to push (bytes).
            priority: Priority value (higher = more urgent, max 255).

        Raises:
            QueueError: If the push operation fails.
        """
        try:
            self._ensure_queue_exists(queue_name)

            properties = pika.BasicProperties(
                priority=min(int(priority), self.config.max_priority),
                delivery_mode=self.config.delivery_mode,
            )

            self._channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=item,
                properties=properties,
            )
        except AMQPError as e:
            msg = f"Failed to push to queue {queue_name}: {e}"
            raise QueueError(
                msg,
                queue_name=queue_name,
                operation="push",
            ) from e

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop highest priority item from queue.

        Args:
            queue_name: Name of the queue.
            timeout: Seconds to wait (0 = non-blocking).

        Returns:
            The popped item, or None if queue is empty.

        Raises:
            QueueError: If the pop operation fails.
        """
        try:
            self._ensure_queue_exists(queue_name)

            if timeout > 0:
                # For blocking, we need to use basic_consume with a callback
                # For simplicity, we poll with short intervals
                import time
                start = time.time()
                while time.time() - start < timeout:
                    method, properties, body = self._channel.basic_get(
                        queue=queue_name,
                        auto_ack=False,
                    )
                    if method:
                        self._channel.basic_ack(method.delivery_tag)
                        return body
                    time.sleep(0.1)
                return None
            else:
                method, properties, body = self._channel.basic_get(
                    queue=queue_name,
                    auto_ack=False,
                )
                if method:
                    self._channel.basic_ack(method.delivery_tag)
                    return body
                return None
        except AMQPError as e:
            msg = f"Failed to pop from queue {queue_name}: {e}"
            raise QueueError(
                msg,
                queue_name=queue_name,
                operation="pop",
            ) from e

    def queue_len(self, queue_name: str) -> int:
        """Get queue length.

        Args:
            queue_name: Name of the queue.

        Returns:
            Number of items in the queue.
        """
        try:
            result = self._channel.queue_declare(
                queue=queue_name,
                passive=True,
            )
            return result.method.message_count
        except AMQPError:
            return 0

    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue.

        Args:
            queue_name: Name of the queue.
        """
        try:
            self._channel.queue_purge(queue=queue_name)
        except AMQPError as e:
            logger.warning("Failed to clear queue %s: %s", queue_name, e)

    # Note: RabbitMQBackend only implements QueueBackend
    # SetBackend and StorageBackend methods are not included
    # because RabbitMQ only supports queue operations
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rabbitmq_backend.py::test_rabbitmq_backend_connect -v`
Expected: PASS

- [ ] **Step 5: Add push/pop tests**

```python
def test_rabbitmq_backend_push():
    """Test RabbitMQ backend push."""
    config = RabbitMQSettings()
    backend = RabbitMQBackend(config)

    with patch("pika.BlockingConnection") as mock_conn:
        mock_instance = MagicMock()
        mock_channel = MagicMock()
        mock_instance.channel.return_value = mock_channel
        mock_conn.return_value = mock_instance

        backend.connect()
        backend.push("test_queue", b"test_item", priority=5)

        mock_channel.queue_declare.assert_called_once()
        mock_channel.basic_publish.assert_called_once()
        call_kwargs = mock_channel.basic_publish.call_args[1]
        assert call_kwargs["routing_key"] == "test_queue"
        assert call_kwargs["body"] == b"test_item"
```

- [ ] **Step 6: Run tests**

Run: `pytest tests/test_rabbitmq_backend.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/scrapy_extension/backends/rabbitmq_backend.py tests/test_rabbitmq_backend.py
git commit -m "feat: add RabbitMQBackend implementation"
```

---

## Task 10: Update Connection Manager

**Files:**
- Modify: `src/scrapy_extension/connection/manager.py`
- Create: `tests/test_connection_manager.py`

- [ ] **Step 0: Create test file**

Create `tests/test_connection_manager.py`:

```python
"""Tests for connection manager."""

from unittest.mock import MagicMock, patch

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.connection.manager import ConnectionManager


def test_connection_manager_get_manager_singleton():
    """Test that get_manager returns singleton for same params."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS)
    assert manager1 is manager2


def test_connection_manager_different_params():
    """Test that different params return different managers."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "localhost"})
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "other"})
    assert manager1 is not manager2
```

- [ ] **Step 1: Write failing test for multi-backend support**

```python
def test_connection_manager_create_mongodb_backend():
    """Test ConnectionManager creates MongoDB backend."""
    from scrapy_extension.connection.manager import ConnectionManager
    from scrapy_extension.backends.base import BackendType

    with patch("scrapy_extension.backends.mongodb_backend.MongoDBBackend") as mock_backend:
        mock_instance = MagicMock()
        mock_backend.return_value = mock_instance

        manager = ConnectionManager(BackendType.MONGODB)
        backend = manager._create_backend()

        mock_backend.assert_called_once()
        assert backend == mock_instance
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_connection_manager.py::test_connection_manager_create_mongodb_backend -v`
Expected: FAIL

- [ ] **Step 3: Update ConnectionManager _create_backend method**

Modify the `_create_backend` method in `src/scrapy_extension/connection/manager.py`:

```python
    def _create_backend(self) -> Backend:
        """Create a backend instance based on type.

        Returns:
            A new backend instance.

        Raises:
            ValueError: If the backend type is not supported.
        """
        if self.backend_type == BackendType.REDIS:
            from scrapy_extension.backends.redis_backend import RedisBackend
            from scrapy_extension.config.settings import RedisSettings

            config = RedisSettings(**self.settings)
            return RedisBackend(config)
        elif self.backend_type == BackendType.MONGODB:
            from scrapy_extension.backends.mongodb_backend import MongoDBBackend
            from scrapy_extension.config.settings import MongoDBSettings

            config = MongoDBSettings(**self.settings)
            return MongoDBBackend(config)
        elif self.backend_type == BackendType.KAFKA:
            from scrapy_extension.backends.kafka_backend import KafkaBackend
            from scrapy_extension.config.settings import KafkaSettings

            config = KafkaSettings(**self.settings)
            return KafkaBackend(config)
        elif self.backend_type == BackendType.RABBITMQ:
            from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
            from scrapy_extension.config.settings import RabbitMQSettings

            config = RabbitMQSettings(**self.settings)
            return RabbitMQBackend(config)
        else:
            raise ValueError(f"Unsupported backend type: {self.backend_type}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_connection_manager.py::test_connection_manager_create_mongodb_backend -v`
Expected: PASS

- [ ] **Step 5: Add tests for Kafka and RabbitMQ**

```python
def test_connection_manager_create_kafka_backend():
    """Test ConnectionManager creates Kafka backend."""
    from scrapy_extension.connection.manager import ConnectionManager
    from scrapy_extension.backends.base import BackendType

    with patch("scrapy_extension.backends.kafka_backend.KafkaBackend") as mock_backend:
        mock_instance = MagicMock()
        mock_backend.return_value = mock_instance

        manager = ConnectionManager(BackendType.KAFKA)
        backend = manager._create_backend()

        mock_backend.assert_called_once()


def test_connection_manager_create_rabbitmq_backend():
    """Test ConnectionManager creates RabbitMQ backend."""
    from scrapy_extension.connection.manager import ConnectionManager
    from scrapy_extension.backends.base import BackendType

    with patch("scrapy_extension.backends.rabbitmq_backend.RabbitMQBackend") as mock_backend:
        mock_instance = MagicMock()
        mock_backend.return_value = mock_instance

        manager = ConnectionManager(BackendType.RABBITMQ)
        backend = manager._create_backend()

        mock_backend.assert_called_once()
```

- [ ] **Step 6: Run all backend creation tests**

Run: `pytest tests/test_connection_manager.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/scrapy_extension/connection/manager.py tests/test_connection_manager.py
git commit -m "feat: update ConnectionManager with backend registry"
```

---

## Task 11: Update pyproject.toml with Optional Dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add optional dependencies**

Add to `pyproject.toml`:

```toml
[project.optional-dependencies]
mongodb = ["pymongo>=4.5.0"]
kafka = ["kafka-python>=2.0.2"]
rabbitmq = ["pika>=1.3.2"]
all = ["pymongo>=4.5.0", "kafka-python>=2.0.2", "pika>=1.3.2"]
```

- [ ] **Step 2: Verify TOML syntax**

Run: `python -c "import tomllib; tomllib.load(open('pyproject.toml', 'rb'))"`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "feat: add optional dependencies for mongodb, kafka, rabbitmq"
```

---

## Task 12: Update Package Exports

**Files:**
- Modify: `src/scrapy_extension/__init__.py`

- [ ] **Step 1: Add new exports**

Add to `src/scrapy_extension/__init__.py`:

```python
# Add to existing imports
from scrapy_extension.backends.mongodb_backend import MongoDBBackend
from scrapy_extension.backends.kafka_backend import KafkaBackend
from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
from scrapy_extension.config.settings import MongoDBSettings, KafkaSettings, RabbitMQSettings

# Add to __all__
__all__ = [
    # ... existing exports ...
    # New backends
    "MongoDBBackend",
    "KafkaBackend",
    "RabbitMQBackend",
    # New settings
    "MongoDBSettings",
    "KafkaSettings",
    "RabbitMQSettings",
]
```

- [ ] **Step 2: Verify imports work**

Run: `python -c "from scrapy_extension import MongoDBBackend, KafkaBackend, RabbitMQBackend; print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add src/scrapy_extension/__init__.py
git commit -m "feat: export new backends and settings"
```

---

## Task 13: Run All Tests

- [ ] **Step 1: Run unit tests**

Run: `pytest tests/ -v --ignore=tests/test_integration`
Expected: All tests pass

- [ ] **Step 2: Verify test coverage**

Run: `pytest tests/ --cov=scrapy_extension --cov-report=term-missing`
Expected: High coverage for all backend files

- [ ] **Step 3: Commit (if any fixes needed)**

```bash
git add -A
git commit -m "test: add comprehensive tests for all backends"
```

---

## Task 14: Final Verification

- [ ] **Step 1: Verify all backends are importable**

```python
python -c "
from scrapy_extension import (
    MongoDBBackend, KafkaBackend, RabbitMQBackend,
    MongoDBSettings, KafkaSettings, RabbitMQSettings
)
print('All imports successful')
"
```

- [ ] **Step 2: Verify protocol compliance**

```python
python -c "
from scrapy_extension.backends.base import Backend, QueueBackend, SetBackend, StorageBackend
from scrapy_extension.backends.mongodb_backend import MongoDBBackend

# Check MongoDB implements all protocols
assert issubclass(MongoDBBackend, Backend)
assert issubclass(MongoDBBackend, QueueBackend)
assert issubclass(MongoDBBackend, SetBackend)
assert issubclass(MongoDBBackend, StorageBackend)
print('Protocol compliance verified')
"
```

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "feat: complete multi-backend implementation (MongoDB, Kafka, RabbitMQ)"
```

---

## Summary

This plan implements three new backends for scrapy-extension:

1. **MongoDB Backend** - Full implementation (Queue, Set, Storage)
2. **Kafka Backend** - QueueBackend only
3. **RabbitMQ Backend** - QueueBackend only

All backends follow the protocol-based architecture with:
- Consistent configuration via pydantic-settings
- Environment variable support
- Comprehensive unit tests
- Clear error messages for unsupported operations

Total estimated tasks: 14
Estimated implementation time: 2-3 hours
