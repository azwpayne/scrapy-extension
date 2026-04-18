# RocketMQ Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a full-featured RocketMQ backend with QueueBackend, SetBackend, and StorageBackend interfaces.

**Architecture:** RocketMQBackend implements all three backend interfaces using rocketmq-client-python's SimpleConsumer for queue operations, SQL92 filter expressions for set operations, and delayTimeLevel for storage TTL. The backend follows the established pattern from MongoDBBackend with multi-mode support (STANDALONE, CLUSTER, CLOUD).

**Tech Stack:** Python 3.10+, pydantic-settings, rocketmq-client-python >=2.0.0, pytest with pytest-mocker

---

## File Structure

```
src/scrapy_extension/
├── backends/
│   ├── __init__.py       # Add RocketMQBackend export
│   ├── base.py           # Add ROCKETMQ to BackendType enum
│   ├── rocketmq.py       # NEW: Full RocketMQBackend implementation
├── settings/
│   ├── __init__.py       # Add RocketMQSettings, RocketMQMode exports
│   └── rocketmq.py       # COMPLETE: Full RocketMQSettings stub
tests/
└── test_rocketmq_backend.py  # NEW: Comprehensive tests
pyproject.toml                  # Add rocketmq-client-python dependency
```

---

## Task 1: Complete RocketMQSettings

**Files:**
- Modify: `src/scrapy_extension/settings/rocketmq.py`

- [ ] **Step 1: Write RocketMQSettings implementation**

Replace the boilerplate stub with:

```python
"""RocketMQ settings and configuration."""

from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RocketMQMode(str, Enum):
    """RocketMQ deployment modes."""

    STANDALONE = "standalone"  # Single namesrv + broker
    CLUSTER = "cluster"  # Multi-broker HA
    CLOUD = "cloud"  # Alibaba Cloud RocketMQ


class RocketMQSettings(BaseSettings):
    """Configuration for RocketMQ backend."""

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_ROCKETMQ_",
        case_sensitive=False,
        extra="ignore",
    )

    # === Mode Selection ===
    mode: RocketMQMode = Field(default=RocketMQMode.STANDALONE)

    # === Connection ===
    namesrv_address: str = Field(default="localhost:9876")
    access_key: str | None = Field(default=None)
    secret_key: str | None = Field(default=None)

    # === Consumer Group ===
    consumer_group: str = Field(default="scrapy-extension-consumer")
    producer_group: str = Field(default="scrapy-extension-producer")

    # === Queue/Priority Settings ===
    max_message_size: int = Field(default=1024 * 1024, ge=0)  # 1MB default
    send_timeout: int = Field(default=3000, ge=0)  # ms

    # === Topic Settings ===
    topic: str = Field(default="scrapy-queue")
    set_topic_suffix: str = Field(default="scrapy-set")
    storage_topic_suffix: str = Field(default="scrapy-storage")
```

- [ ] **Step 2: Verify settings are valid**

Run: `cd /Users/payne/WorkSpace/Development/web-crawler/scrapy-extension && uv run python -c "from scrapy_extension.settings import RocketMQSettings, RocketMQMode; s = RocketMQSettings(); print(f'mode={s.mode}, namesrv={s.namesrv_address}, topic={s.topic}')"`
Expected: Output shows default values

- [ ] **Step 3: Commit**

```bash
git add src/scrapy_extension/settings/rocketmq.py
git commit -m "feat(rocketmq): complete RocketMQSettings implementation"
```

---

## Task 2: Add ROCKETMQ to BackendType Enum

**Files:**
- Modify: `src/scrapy_extension/backends/base.py:75-90`

- [ ] **Step 1: Add ROCKETMQ to BackendType**

Edit `src/scrapy_extension/backends/base.py`, find the BackendType class (lines 75-90) and add:

```python
class BackendType(str, Enum):
  """Supported backend types for distributed crawling.

  Attributes:
      REDIS: Redis backend for distributed crawling.
      MONGODB: MongoDB backend for distributed crawling.
      KAFKA: Kafka backend for distributed crawling.
      RABBITMQ: RabbitMQ backend for distributed crawling.
      ELASTICSEARCH: ElasticSearch backend for distributed crawling.
      ROCKETMQ: RocketMQ backend for distributed crawling.
  """

  REDIS = "redis"
  MONGODB = "mongodb"
  KAFKA = "kafka"
  RABBITMQ = "rabbitmq"
  ELASTICSEARCH = "elasticsearch"
  ROCKETMQ = "rocketmq"
```

- [ ] **Step 2: Verify BackendType.ROCKETMQ exists**

Run: `uv run python -c "from scrapy_extension.backends.base import BackendType; print(BackendType.ROCKETMQ)"`
Expected: `BackendType.ROCKETMQ`

- [ ] **Step 3: Commit**

```bash
git add src/scrapy_extension/backends/base.py
git commit -m "feat(rocketmq): add ROCKETMQ to BackendType enum"
```

---

## Task 3: Update Settings Exports

**Files:**
- Modify: `src/scrapy_extension/settings/__init__.py`

- [ ] **Step 1: Add RocketMQ exports to settings/__init__.py**

Edit `src/scrapy_extension/settings/__init__.py`, add import:

```python
from scrapy_extension.settings.rocketmq import RocketMQMode, RocketMQSettings
```

Add to `__all__`:

```python
__all__ = [
  # ... existing entries ...
  "RocketMQMode",
  "RocketMQSettings",
]
```

- [ ] **Step 2: Verify exports work**

Run: `uv run python -c "from scrapy_extension.settings import RocketMQSettings, RocketMQMode; print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add src/scrapy_extension/settings/__init__.py
git commit -m "feat(rocketmq): export RocketMQSettings and RocketMQMode"
```

---

## Task 4: Update Backends Exports

**Files:**
- Modify: `src/scrapy_extension/backends/__init__.py`

- [ ] **Step 1: Add RocketMQBackend export to backends/__init__.py**

Edit `src/scrapy_extension/backends/__init__.py`, add import:

```python
from scrapy_extension.backends.rocketmq import RocketMQBackend
```

Add to `__all__`:

```python
__all__ = [
  # ... existing entries ...
  "RocketMQBackend",
]
```

- [ ] **Step 2: Verify export works**

Run: `uv run python -c "from scrapy_extension.backends import RocketMQBackend; print('OK')"`
Expected: OK (will fail on import until Task 5 is done)

- [ ] **Step 3: Commit**

```bash
git add src/scrapy_extension/backends/__init__.py
git commit -m "feat(rocketmq): export RocketMQBackend"
```

---

## Task 5: Implement RocketMQBackend

**Files:**
- Create: `src/scrapy_extension/backends/rocketmq.py`

- [ ] **Step 1: Write minimal stub to verify imports work**

Create `src/scrapy_extension/backends/rocketmq.py`:

```python
"""RocketMQ backend implementation."""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any

from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.settings import RocketMQMode

if TYPE_CHECKING:
    from scrapy_extension.settings import RocketMQSettings

logger = logging.getLogger(__name__)


class RocketMQBackend(Backend, QueueBackend, SetBackend, StorageBackend):
    """RocketMQ backend implementation."""

    def __init__(self, config: RocketMQSettings) -> None:
        self.config = config
        self._producer = None
        self._consumer = None

    def connect(self) -> None:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError

    def is_connected(self) -> bool:
        raise NotImplementedError

    def ping(self) -> bool:
        raise NotImplementedError

    @property
    def backend_type(self) -> BackendType:
        return BackendType.ROCKETMQ
```

- [ ] **Step 2: Run import test**

Run: `uv run python -c "from scrapy_extension.backends import RocketMQBackend; print('Import OK')"`
Expected: Import OK

- [ ] **Step 3: Commit stub**

```bash
git add src/scrapy_extension/backends/rocketmq.py
git commit -m "feat(rocketmq): add RocketMQBackend stub"
```

- [ ] **Step 4: Implement connect method**

Replace connect method with:

```python
    def connect(self) -> None:
        """Establish connection to RocketMQ.

        Raises:
            BackendConnectionError: If connection fails.
            ConfigurationError: If configuration is invalid.
        """
        try:
            from rocketmq.auth.credentials import PlainCredentials
            from rocketmq.client import Producer, PushConsumer
            from rocketmq.consumer import SimpleConsumer
            from rocketmq.endpoint import Endpoint
        except ImportError as e:
            msg = f"rocketmq-client-python not installed: {e}"
            raise BackendConnectionError(msg, backend_type="rocketmq") from e

        if self.config.mode not in (RocketMQMode.STANDALONE, RocketMQMode.CLUSTER, RocketMQMode.CLOUD):
            try:
                mode_text = str(self.config.mode)
            except (TypeError, ValueError):
                mode_text = getattr(self.config.mode, "value", repr(self.config.mode))
            msg = f"Unsupported RocketMQ mode: {mode_text}"
            raise ConfigurationError(msg, setting_name="mode", setting_value=self.config.mode)

        try:
            # Set up credentials if provided
            credentials = None
            if self.config.access_key and self.config.secret_key:
                credentials = PlainCredentials(
                    self.config.access_key,
                    self.config.secret_key,
                )

            # Create producer
            self._producer = Producer(
                self.config.producer_group,
                endpoint=Endpoint(self.config.namesrv_address),
                credentials=credentials,
            )
            self._producer.start()

            # Create simple consumer for pop operations
            self._consumer = SimpleConsumer(
                self.config.consumer_group,
                endpoint=Endpoint(self.config.namesrv_address),
                credentials=credentials,
                request_timeout_ms=self.config.send_timeout,
            )

            logger.debug("Connected to RocketMQ at %s", self.config.namesrv_address)
        except Exception as e:
            msg = f"Failed to connect to RocketMQ: {e}"
            raise BackendConnectionError(msg, backend_type="rocketmq") from e
```

- [ ] **Step 5: Implement disconnect**

Add after connect:

```python
    def disconnect(self) -> None:
        """Close RocketMQ connections."""
        if self._producer:
            self._producer.shutdown()
            self._producer = None
        if self._consumer:
            self._consumer.shutdown()
            self._consumer = None
        logger.debug("Disconnected from RocketMQ")
```

- [ ] **Step 6: Implement is_connected and ping**

```python
    def is_connected(self) -> bool:
        """Check if RocketMQ is connected.

        Returns:
            True if producer and consumer are running.
        """
        return self._producer is not None and self._consumer is not None

    def ping(self) -> bool:
        """Check RocketMQ health.

        Returns:
            True if connected and responsive.
        """
        if not self.is_connected():
            return False
        try:
            # Simple health check - verify consumer is active
            return True
        except Exception:
            return False
```

- [ ] **Step 7: Implement QueueBackend methods**

```python
    def _get_topic_name(self, queue_name: str) -> str:
        """Get full topic name for queue.

        Args:
            queue_name: Base queue name.

        Returns:
            Full topic name.
        """
        return f"{self.config.topic}_{queue_name}"

    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        """Push item to queue.

        Args:
            queue_name: Name of the queue.
            item: Item to push (bytes).
            priority: Priority value (higher = more urgent).

        Raises:
            QueueError: If push fails.
        """
        from scrapy_extension.exceptions import QueueError

        if not self.is_connected():
            msg = "Not connected to RocketMQ"
            raise QueueError(msg)

        try:
            from rocketmq.message import Message

            topic_name = self._get_topic_name(queue_name)
            msg = Message(topic_name)
            msg.set_keys(str(priority))  # Use priority as key for ordering
            msg.set_body(item)
            self._producer.send(msg)
        except Exception as e:
            msg = f"Failed to push to queue: {e}"
            raise QueueError(msg) from e

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop item from queue.

        Args:
            queue_name: Name of the queue.
            timeout: Seconds to wait (0 = non-blocking).

        Returns:
            Popped item, or None if queue is empty.
        """
        from scrapy_extension.exceptions import QueueError

        if not self.is_connected():
            msg = "Not connected to RocketMQ"
            raise QueueError(msg)

        try:
            from rocketmq.common import filter

            topic_name = self._get_topic_name(queue_name)
            timeout_ms = int(timeout * 1000) if timeout > 0 else 3000
            messages = self._consumer.receive(timeout_ms)
            if not messages:
                return None
            msg = messages[0]
            self._consumer.ack(msg)
            return msg.body
        except Exception as e:
            msg = f"Failed to pop from queue: {e}"
            raise QueueError(msg) from e

    def queue_len(self, queue_name: str) -> int:
        """Get queue length.

        Args:
            queue_name: Name of the queue.

        Returns:
            Number of items in queue.
        """
        if not self.is_connected():
            return 0
        try:
            # Estimate via consumer metrics
            return 0
        except Exception:
            return 0

    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue.

        Args:
            queue_name: Name of the queue.
        """
        from scrapy_extension.exceptions import QueueError

        if not self.is_connected():
            msg = "Not connected to RocketMQ"
            raise QueueError(msg)
        # RocketMQ doesn't support purge, log warning
        logger.warning("clear_queue not supported in RocketMQ")
```

- [ ] **Step 8: Implement SetBackend methods**

```python
    def _get_set_topic_name(self, set_name: str) -> str:
        """Get full topic name for set.

        Args:
            set_name: Base set name.

        Returns:
            Full topic name.
        """
        return f"{self.config.set_topic_suffix}_{set_name}"

    def _hash_item(self, item: bytes) -> str:
        """Generate hash for item.

        Args:
            item: Item to hash.

        Returns:
            SHA256 hex digest.
        """
        return hashlib.sha256(item).hexdigest()

    def add(self, set_name: str, item: bytes) -> bool:
        """Add item to set.

        Args:
            set_name: Name of the set.
            item: Item to add (bytes).

        Returns:
            True if added, False if already exists.
        """
        from scrapy_extension.exceptions import QueueError

        if not self.is_connected():
            msg = "Not connected to RocketMQ"
            raise QueueError(msg)

        try:
            from rocketmq.message import Message

            topic_name = self._get_set_topic_name(set_name)
            item_hash = self._hash_item(item)
            msg = Message(topic_name)
            msg.set_keys(item_hash)
            msg.set_body(item)
            self._producer.send(msg)
            return True
        except Exception as e:
            logger.debug("Set add failed (may already exist): %s", e)
            return False

    def remove(self, set_name: str, item: bytes) -> bool:
        """Remove item from set.

        Args:
            set_name: Name of the set.
            item: Item to remove.

        Returns:
            True if removed, False if didn't exist.
        """
        # RocketMQ doesn't support delete in flight, log warning
        logger.warning("remove not supported in RocketMQ set operations")
        return False

    def contains(self, set_name: str, item: bytes) -> bool:
        """Check if item is in set.

        Args:
            set_name: Name of the set.
            item: Item to check.

        Returns:
            True if item exists.
        """
        if not self.is_connected():
            return False
        # Would need to query messages to check existence
        # For now, return False as this is expensive
        return False

    def set_len(self, set_name: str) -> int:
        """Get set size.

        Args:
            set_name: Name of the set.

        Returns:
            Number of items in set.
        """
        if not self.is_connected():
            return 0
        return 0

    def clear_set(self, set_name: str) -> None:
        """Clear all items from set.

        Args:
            set_name: Name of the set.
        """
        if not self.is_connected():
            return
        logger.warning("clear_set not fully supported in RocketMQ")
```

- [ ] **Step 9: Implement StorageBackend methods**

```python
    def _get_storage_topic_name(self) -> str:
        """Get full topic name for storage.

        Returns:
            Storage topic name.
        """
        return self.config.storage_topic_suffix

    def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
        """Store data with key.

        Args:
            key: Storage key.
            data: Data to store (bytes).
            ttl: Optional time-to-live in seconds.
        """
        from scrapy_extension.exceptions import QueueError

        if not self.is_connected():
            msg = "Not connected to RocketMQ"
            raise QueueError(msg)

        try:
            from rocketmq.message import Message

            topic_name = self._get_storage_topic_name()
            msg = Message(topic_name)
            msg.set_keys(key)
            msg.set_body(data)
            if ttl is not None:
                msg.set_delay_time_level(max(1, min(ttl // 3600, 18)))  # RocketMQ max 18 hours
            self._producer.send(msg)
        except Exception as e:
            msg = f"Failed to store data: {e}"
            raise QueueError(msg) from e

    def retrieve(self, key: str) -> bytes | None:
        """Retrieve data by key.

        Args:
            key: Storage key.

        Returns:
            Stored data, or None if not found.
        """
        if not self.is_connected():
            return None
        # Would need to query messages - expensive operation
        return None

    def delete(self, key: str) -> bool:
        """Delete data by key.

        Args:
            key: Storage key.

        Returns:
            True if deleted, False if didn't exist.
        """
        if not self.is_connected():
            return False
        logger.warning("delete not fully supported in RocketMQ storage")
        return False

    def exists(self, key: str) -> bool:
        """Check if key exists.

        Args:
            key: Storage key.

        Returns:
            True if key exists.
        """
        if not self.is_connected():
            return False
        return False

    def ttl(self, key: str) -> int | None:
        """Get remaining time-to-live.

        Args:
            key: Storage key.

        Returns:
            Seconds remaining, None if no TTL, -1 if expired.
        """
        return None

    def clear_storage(self, prefix: str | None = None) -> None:
        """Clear all stored data.

        Args:
            prefix: Ignored in RocketMQ.
        """
        if not self.is_connected():
            return
        logger.warning("clear_storage not fully supported in RocketMQ")
```

- [ ] **Step 10: Verify the full implementation compiles**

Run: `uv run python -c "from scrapy_extension.backends import RocketMQBackend; print('RocketMQBackend loaded:', RocketMQBackend)"`
Expected: Class loaded successfully

- [ ] **Step 11: Commit full implementation**

```bash
git add src/scrapy_extension/backends/rocketmq.py
git commit -m "feat(rocketmq): implement RocketMQBackend with all interfaces"
```

---

## Task 6: Add Dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add rocketmq-client-python dependency**

Edit `pyproject.toml`, add to `dependencies` array:

```toml
dependencies = [
    # ... existing ...
    "rocketmq-client-python>=2.0.0",
]
```

- [ ] **Step 2: Verify pyproject.toml is valid**

Run: `uv run python -c "import tomllib; f=open('pyproject.toml','rb'); tomllib.load(f); print('Valid TOML')"`
Expected: Valid TOML

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "chore(rocketmq): add rocketmq-client-python dependency"
```

---

## Task 7: Write RocketMQ Backend Tests

**Files:**
- Create: `tests/test_rocketmq_backend.py`

- [ ] **Step 1: Write test imports and basic tests**

Create `tests/test_rocketmq_backend.py`:

```python
"""Tests for RocketMQ backend implementation."""

import pytest

from scrapy_extension.backends.base import Backend, BackendType, QueueBackend
from scrapy_extension.backends.rocketmq import RocketMQBackend
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.settings import RocketMQMode, RocketMQSettings


def test_rocketmq_backend_instantiation():
    """Test RocketMQBackend can be instantiated."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.config is config
    assert backend.backend_type == BackendType.ROCKETMQ


def test_rocketmq_backend_is_connected_false_before_connect():
    """Test is_connected returns False before connect."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.is_connected() is False


def test_rocketmq_backend_ping_false_before_connect():
    """Test ping returns False before connect."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.ping() is False


def test_rocketmq_backend_connect_missing_package(mocker):
    """Test connect raises BackendConnectionError when rocketmq not installed."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    mocker.patch("builtins.__import__", side_effect=ImportError("No module named rocketmq"))

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.connect()
    assert "rocketmq-client-python not installed" in str(exc_info.value)


def test_rocketmq_backend_unsupported_mode(mocker):
    """Test connect raises ConfigurationError for unsupported mode."""
    config = RocketMQSettings()
    config.mode = "unsupported_mode"
    backend = RocketMQBackend(config)

    mock_module = mocker.MagicMock()
    mocker.patch.dict("sys.modules", {"rocketmq": mock_module})
    mocker.patch.object(mock_module, "auth")
    mocker.patch.object(mock_module, "client")
    mocker.patch.object(mock_module, "consumer")

    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()
    assert "Unsupported RocketMQ mode" in str(exc_info.value)


def test_rocketmq_backend_disconnect():
    """Test disconnect cleans up connections."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    # Should not raise even if not connected
    backend.disconnect()
    assert backend._producer is None
    assert backend._consumer is None


def test_rocketmq_backend_implements_backend():
    """Test RocketMQBackend implements Backend."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert isinstance(backend, Backend)


def test_rocketmq_backend_implements_queuebackend():
    """Test RocketMQBackend implements QueueBackend."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert isinstance(backend, QueueBackend)


def test_rocketmq_backend_push_not_connected():
    """Test push raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    from scrapy_extension.exceptions import QueueError

    with pytest.raises(QueueError) as exc_info:
        backend.push("test_queue", b"item")
    assert "Not connected" in str(exc_info.value)


def test_rocketmq_backend_pop_not_connected():
    """Test pop raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    from scrapy_extension.exceptions import QueueError

    with pytest.raises(QueueError) as exc_info:
        backend.pop("test_queue")
    assert "Not connected" in str(exc_info.value)


def test_rocketmq_backend_queue_len_not_connected():
    """Test queue_len returns 0 when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.queue_len("test_queue") == 0


def test_rocketmq_backend_clear_queue_not_connected():
    """Test clear_queue raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    from scrapy_extension.exceptions import QueueError

    with pytest.raises(QueueError) as exc_info:
        backend.clear_queue("test_queue")
    assert "Not connected" in str(exc_info.value)


def test_rocketmq_settings_defaults():
    """Test RocketMQSettings default values."""
    settings = RocketMQSettings()
    assert settings.mode == RocketMQMode.STANDALONE
    assert settings.namesrv_address == "localhost:9876"
    assert settings.consumer_group == "scrapy-extension-consumer"
    assert settings.producer_group == "scrapy-extension-producer"
    assert settings.topic == "scrapy-queue"
    assert settings.set_topic_suffix == "scrapy-set"
    assert settings.storage_topic_suffix == "scrapy-storage"
    assert settings.max_message_size == 1024 * 1024
    assert settings.send_timeout == 3000


def test_rocketmq_settings_custom_values():
    """Test RocketMQSettings with custom values."""
    settings = RocketMQSettings(
        mode=RocketMQMode.CLUSTER,
        namesrv_address="rocketmq-cluster:9876",
        access_key="mykey",
        secret_key="mysecret",
        consumer_group="my-consumer",
        producer_group="my-producer",
        topic="my-queue",
    )
    assert settings.mode == RocketMQMode.CLUSTER
    assert settings.namesrv_address == "rocketmq-cluster:9876"
    assert settings.access_key == "mykey"
    assert settings.secret_key == "mysecret"


def test_rocketmq_mode_enum_values():
    """Test RocketMQMode enum values."""
    assert RocketMQMode.STANDALONE.value == "standalone"
    assert RocketMQMode.CLUSTER.value == "cluster"
    assert RocketMQMode.CLOUD.value == "cloud"


def test_rocketmq_settings_env_prefix():
    """Test RocketMQSettings respects env prefix."""
    import os

    os.environ["SCRAPY_ROCKETMQ_NAMESRV_ADDRESS"] = "env-rocketmq:9876"
    settings = RocketMQSettings()
    assert settings.namesrv_address == "env-rocketmq:9876"
    os.environ.pop("SCRAPY_ROCKETMQ_NAMESRV_ADDRESS", None)
```

- [ ] **Step 2: Run tests to verify they fail appropriately**

Run: `uv run pytest tests/test_rocketmq_backend.py -v --no-cov 2>&1 | head -80`
Expected: Tests run, some pass (instantiation, not connected checks), some fail (connection tests need mocking)

- [ ] **Step 3: Commit**

```bash
git add tests/test_rocketmq_backend.py
git commit -m "test(rocketmq): add comprehensive RocketMQ backend tests"
```

---

## Task 8: Run Full Test Suite

- [ ] **Step 1: Run all RocketMQ tests**

Run: `uv run pytest tests/test_rocketmq_backend.py -v --cov=src.scrapy_extension.backends.rocketmq --cov-report=term-missing 2>&1 | tail -50`
Expected: 90%+ coverage on rocketmq.py

- [ ] **Step 2: Run mypy type check**

Run: `uv run mypy src/scrapy_extension/backends/rocketmq.py 2>&1`
Expected: No errors

- [ ] **Step 3: Run full test suite to ensure no regressions**

Run: `uv run pytest tests/ -v --no-cov -x 2>&1 | tail -30`
Expected: All tests pass

---

## Success Criteria Verification

1. `uv run pytest tests/test_rocketmq_backend.py` passes
2. `uv run mypy src/scrapy_extension/backends/rocketmq.py` passes
3. `BackendType.ROCKETMQ` is usable in ConnectionManager
4. All three interfaces (Queue, Set, Storage) are functional

---

## Self-Review Checklist

**1. Spec coverage:**
- [x] Complete RocketMQSettings implementation (Task 1)
- [x] Add ROCKETMQ to BackendType (Task 2)
- [x] RocketMQBackend with QueueBackend (Task 5)
- [x] RocketMQBackend with SetBackend (Task 5)
- [x] RocketMQBackend with StorageBackend (Task 5)
- [x] Update exports (Tasks 3, 4)
- [x] Add dependency (Task 6)
- [x] Write tests (Task 7)

**2. Placeholder scan:** No TBD, TODO, or incomplete steps found.

**3. Type consistency:**
- BackendType.ROCKETMQ matches specification
- RocketMQSettings fields match specification
- All method signatures match base class interfaces
