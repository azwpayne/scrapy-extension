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

  # QueueBackend methods (stubs for now)
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push an item to a queue."""
    raise NotImplementedError("push not implemented yet")

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop an item from a queue."""
    raise NotImplementedError("pop not implemented yet")

  def queue_len(self, queue_name: str) -> int:
    """Get the number of items in a queue."""
    raise NotImplementedError("queue_len not implemented yet")

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from a queue."""
    raise NotImplementedError("clear_queue not implemented yet")

  # SetBackend methods (stubs for now)
  def add(self, set_name: str, item: bytes) -> bool:
    """Add an item to a set."""
    raise NotImplementedError("add not implemented yet")

  def remove(self, set_name: str, item: bytes) -> bool:
    """Remove an item from a set."""
    raise NotImplementedError("remove not implemented yet")

  def contains(self, set_name: str, item: bytes) -> bool:
    """Check if an item is in a set."""
    raise NotImplementedError("contains not implemented yet")

  def set_len(self, set_name: str) -> int:
    """Get the number of items in a set."""
    raise NotImplementedError("set_len not implemented yet")

  def clear_set(self, set_name: str) -> None:
    """Clear all items from a set."""
    raise NotImplementedError("clear_set not implemented yet")

  # StorageBackend methods (stubs for now)
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store data with a key."""
    raise NotImplementedError("store not implemented yet")

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key."""
    raise NotImplementedError("retrieve not implemented yet")

  def delete(self, key: str) -> bool:
    """Delete data by key."""
    raise NotImplementedError("delete not implemented yet")

  def exists(self, key: str) -> bool:
    """Check if a key exists."""
    raise NotImplementedError("exists not implemented yet")

  def ttl(self, key: str) -> int | None:
    """Get the remaining time-to-live for a key."""
    raise NotImplementedError("ttl not implemented yet")

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix."""
    raise NotImplementedError("clear_storage not implemented yet")
