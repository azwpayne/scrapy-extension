"""Redis backend implementation.

This module provides a Redis-based implementation of the backend interfaces
for distributed crawling.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from redis import Redis
from redis.exceptions import RedisError

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.exceptions import BackendConnectionError, QueueError

if TYPE_CHECKING:
  from scrapy_extension.config.settings import RedisSettings

logger = logging.getLogger(__name__)


class RedisBackend(Backend, QueueBackend, SetBackend, StorageBackend):
  """Redis backend implementation.

  Implements all backend interfaces using Redis data structures:
  - Queue: Redis Sorted Sets (ZADD/ZRANGEBYSCORE/ZREM)
  - Set: Redis Sets (SADD/SREM/SISMEMBER/SCARD/DEL)
  - Storage: Redis Strings with TTL (SET/GET/DEL/EXISTS/TTL)

  Supports standalone, sentinel, and cluster deployment modes.

  Attributes:
      config: RedisSettings instance with connection parameters.
      _client: The Redis client instance (None until connected).
  """

  def __init__(self, config: RedisSettings) -> None:
    """Initialize Redis backend.

    Args:
        config: Configuration for Redis connection.
    """
    self.config = config
    self._client: Redis | None = None

  def connect(self) -> None:
    """Establish connection to Redis.

    Creates a Redis client based on the configuration settings.
    Does not verify the connection until first use.

    Raises:
        ConnectionError: If the connection cannot be established.
    """
    try:
      self._client = Redis(
        host=self.config.host,
        port=self.config.port,
        db=self.config.db,
        password=self.config.password,
        decode_responses=False,  # Keep as bytes for consistency
      )
      # Verify connection
      self._client.ping()
    except RedisError as e:
      msg = f"Failed to connect to Redis: {e}"
      raise BackendConnectionError(
        msg,
        backend_type="redis",
      ) from e

  def disconnect(self) -> None:
    """Close Redis connection.

    Closes the connection pool and releases resources.
    """
    if self._client:
      self._client.close()
      self._client = None

  def is_connected(self) -> bool:
    """Check if Redis is connected.

    Returns:
        True if connected and responding to ping, False otherwise.
    """
    try:
      result = self._client.ping()
      return bool(result) if result is not None else False
    except RedisError:
      return False

  def ping(self) -> bool:
    """Check Redis health.

    Returns:
        True if Redis responds to ping.
    """
    try:
      result = self._client.ping() if self._client else False
      return bool(result) if result is not None else False
    except RedisError:
      return False

  @property
  def backend_type(self) -> BackendType:
    """Return backend type.

    Returns:
        BackendType.REDIS
    """
    return BackendType.REDIS

  @property
  def client(self) -> Redis:
    """Get Redis client, connecting if necessary.

    Returns:
        The Redis client instance.

    Raises:
        ConnectionError: If not connected and connection fails.
    """
    if self._client is None:
      self.connect()
    return self._client  # type: ignore[return-value]

  # QueueBackend implementation using Sorted Sets
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push item to priority queue.

    Uses Redis Sorted Set with priority as score.
    Lower priority values = higher priority (processed first).

    Args:
        queue_name: Name of the queue.
        item: Item to push (bytes).
        priority: Priority value (lower = more urgent).

    Raises:
        QueueError: If the push operation fails.
    """
    try:
      # Use negative priority so lower values (higher priority) have higher scores
      # This makes zpopmax return highest priority items first
      self.client.zadd(queue_name, {item: -priority})
    except RedisError as e:
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
      if timeout > 0:
        # Use BZPOPMAX for blocking pop
        result = self.client.bzpopmax(queue_name, timeout=timeout)
        if result:
          # result is (queue_name, item, score)
          return result[1]  # type: ignore[index, return-value]
        return None
      else:
        # Non-blocking pop - returns list of (item, score) tuples
        result = self.client.zpopmax(queue_name)  # type: ignore[assignment]
        if result and len(result) > 0:  # type: ignore[arg-type]
          return result[0][0]  # type: ignore[index, return-value]
        return None
    except RedisError as e:
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
      return int(self.client.zcard(queue_name))  # type: ignore[arg-type, return-value]
    except RedisError:
      return 0

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.
    """
    try:
      self.client.delete(queue_name)
    except RedisError as e:
      logger.warning("Failed to clear queue %s: %s", queue_name, e)

  # SetBackend implementation using Redis Sets
  def add(self, set_name: str, item: bytes) -> bool:
    """Add item to set.

    Args:
        set_name: Name of the set.
        item: Item to add (bytes).

    Returns:
        True if added, False if already existed.
    """
    try:
      return self.client.sadd(set_name, item) == 1
    except RedisError:
      return False

  def remove(self, set_name: str, item: bytes) -> bool:
    """Remove item from set.

    Args:
        set_name: Name of the set.
        item: Item to remove.

    Returns:
        True if removed, False if didn't exist.
    """
    try:
      return self.client.srem(set_name, item) == 1
    except RedisError:
      return False

  def contains(self, set_name: str, item: bytes) -> bool:
    """Check if item is in set.

    Args:
        set_name: Name of the set.
        item: Item to check.

    Returns:
        True if item exists in the set.
    """
    try:
      result = self.client.sismember(set_name, item)  # type: ignore[arg-type]
      return bool(result)  # type: ignore[arg-type]
    except RedisError:
      return False

  def set_len(self, set_name: str) -> int:
    """Get set size.

    Args:
        set_name: Name of the set.

    Returns:
        Number of items in the set.
    """
    try:
      return int(self.client.scard(set_name))  # type: ignore[arg-type, return-value]
    except RedisError:
      return 0

  def clear_set(self, set_name: str) -> None:
    """Clear all items from set.

    Args:
        set_name: Name of the set.
    """
    try:
      self.client.delete(set_name)
    except RedisError as e:
      logger.warning("Failed to clear set %s: %s", set_name, e)

  # StorageBackend implementation using Redis Strings
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store data with key.

    Args:
        key: Storage key.
        data: Data to store (bytes).
        ttl: Optional time-to-live in seconds.
    """
    try:
      if ttl:
        self.client.setex(key, ttl, data)
      else:
        self.client.set(key, data)
    except RedisError as e:
      logger.warning("Failed to store key %s: %s", key, e)

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key.

    Args:
        key: Storage key.

    Returns:
        Stored data, or None if not found.
    """
    try:
      return self.client.get(key)
    except RedisError:
      return None

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if deleted, False if didn't exist.
    """
    try:
      return self.client.delete(key) == 1
    except RedisError:
      return False

  def exists(self, key: str) -> bool:
    """Check if key exists.

    Args:
        key: Storage key.

    Returns:
        True if key exists.
    """
    try:
      return self.client.exists(key) == 1
    except RedisError:
      return False

  def ttl(self, key: str) -> int | None:
    """Get remaining time-to-live.

    Args:
        key: Storage key.

    Returns:
        Seconds remaining, None if no TTL, -1 if expired.
    """
    try:
      result = self.client.ttl(key)
      if result == -1:
        return None  # No TTL set
      if result == -2:
        return -1  # Key doesn't exist/expired
      return result
    except RedisError:
      return None

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    Args:
        prefix: If provided, only clear keys starting with this prefix.
               If None, clear all storage data.
    """
    try:
      if prefix:
        # Use scan + delete for prefixed keys
        pattern = f"{prefix}*"
        for key in self.client.scan_iter(match=pattern):
          self.client.delete(key)
      else:
        # Clear all keys in the current database
        self.client.flushdb()
    except RedisError as e:
      logger.warning("Failed to clear storage: %s", e)
