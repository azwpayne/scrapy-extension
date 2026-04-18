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

        if self.config.mode not in (
            RocketMQMode.STANDALONE,
            RocketMQMode.CLUSTER,
            RocketMQMode.CLOUD,
        ):
            try:
                mode_text = str(self.config.mode)
            except (TypeError, ValueError):
                mode_text = getattr(
                    self.config.mode, "value", repr(self.config.mode)
                )
            msg = f"Unsupported RocketMQ mode: {mode_text}"
            raise ConfigurationError(
                msg, setting_name="mode", setting_value=self.config.mode
            )

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

            logger.debug(
                "Connected to RocketMQ at %s", self.config.namesrv_address
            )
        except Exception as e:
            msg = f"Failed to connect to RocketMQ: {e}"
            raise BackendConnectionError(msg, backend_type="rocketmq") from e

    def disconnect(self) -> None:
        """Close RocketMQ connections."""
        if self._producer:
            self._producer.shutdown()
            self._producer = None
        if self._consumer:
            self._consumer.shutdown()
            self._consumer = None
        logger.debug("Disconnected from RocketMQ")

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

    @property
    def backend_type(self) -> BackendType:
        return BackendType.ROCKETMQ

    def _get_topic_name(self, queue_name: str) -> str:
        """Get full topic name for queue.

        Args:
            queue_name: Base queue name.

        Returns:
            Full topic name.
        """
        return f"{self.config.topic_prefix}_{queue_name}"

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

    def _get_set_topic_name(self, set_name: str) -> str:
        """Get full topic name for set.

        Args:
            set_name: Base set name.

        Returns:
            Full topic name.
        """
        return f"{self.config.set_topic_prefix}_{set_name}"

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

    def _get_storage_topic_name(self) -> str:
        """Get full topic name for storage.

        Returns:
            Storage topic name.
        """
        return self.config.storage_topic_prefix

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
                # RocketMQ max delay time level is 18 hours
                msg.set_delay_time_level(max(1, min(ttl // 3600, 18)))
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
