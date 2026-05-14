"""MongoDB backend implementation with multi-mode support.

This module provides a MongoDB-based implementation of the backend interfaces
for distributed crawling, supporting multiple deployment modes:
- Standalone: Single MongoDB instance
- Replica Set: High availability with automatic failover
- Sharded Cluster: Horizontal scaling with mongos routers
- Atlas: MongoDB Atlas cloud service
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

try:
    from pymongo import ASCENDING, MongoClient
    from pymongo.errors import ConnectionFailure, DuplicateKeyError, PyMongoError
except ImportError as e:
    raise ImportError(
        "MongoDB backend requires 'pymongo'. Install with: pip install scrapy-extension[mongodb]"
    ) from e

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
  _hash_item,
  _validate_key_name,
)
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError, QueueError
from scrapy_extension.settings import MongoDBMode

if TYPE_CHECKING:
  from pymongo.collection import Collection
  from pymongo.database import Database

  from scrapy_extension.settings import MongoDBSettings

logger = logging.getLogger(__name__)


class MongoDBBackend(Backend, QueueBackend, SetBackend, StorageBackend):
  """MongoDB backend implementation with multi-mode support.

  Implements all backend interfaces using MongoDB collections:
  - Queue: Collection with priority and created_at fields
  - Set: Collection with unique index on (set_name, item_hash)
  - Storage: Collection with TTL index on expireAt

  Supports standalone, replica_set, sharded_cluster, and atlas deployment modes.

  Attributes:
      config: MongoDBSettings instance with connection parameters.
      _client: The MongoDB client instance (None until connected).
      _db: The MongoDB database instance.
  """

  # Read preference mapping - defined as class constant to avoid recreating
  _READ_PREF_MAP: dict[str, str] = {
    "primary": "primary",
    "secondary": "secondary",
    "nearest": "nearest",
    "primarypreferred": "primaryPreferred",
    "secondarypreferred": "secondaryPreferred",
  }

  def __init__(self, config: MongoDBSettings) -> None:
    """Initialize MongoDB backend.

    Args:
        config: Configuration for MongoDB connection.
    """
    self.config = config
    self._client: MongoClient | None = None
    self._db: Database | None = None
    self._queue_collection: Collection | None = None
    self._set_collection: Collection | None = None
    self._storage_collection: Collection | None = None
    # Cache client kwargs to avoid rebuilding on reconnection
    self._client_kwargs: dict[str, Any] | None = None
    # Cache read preference to avoid string manipulation on every call
    self._read_preference: str | None = self._compute_read_preference()

  def connect(self) -> None:
    """Establish connection to MongoDB based on deployment mode.

    Creates the appropriate MongoDB client based on the configuration mode.
    Supports standalone, replica_set, sharded_cluster, and atlas modes.

    Raises:
        BackendConnectionError: If the connection cannot be established.
        ConfigurationError: If the configuration is invalid for the mode.
    """
    if self.config.mode not in (
      MongoDBMode.STANDALONE,
      MongoDBMode.REPLICA_SET,
      MongoDBMode.SHARDED_CLUSTER,
      MongoDBMode.ATLAS,
    ):
      try:
        mode_text = str(self.config.mode)
      except (TypeError, ValueError):
        mode_text = getattr(self.config.mode, "value", repr(self.config.mode))
      msg = f"Unsupported MongoDB mode: {mode_text}"
      raise ConfigurationError(
        msg,
        setting_name="mode",
        setting_value=self.config.mode,
      )
    try:
      if self.config.mode == MongoDBMode.STANDALONE:
        self._connect_standalone()
      elif self.config.mode == MongoDBMode.REPLICA_SET:
        self._connect_replica_set()
      elif self.config.mode == MongoDBMode.SHARDED_CLUSTER:
        self._connect_sharded_cluster()
      else:
        self._connect_atlas()
      logger.debug("Connected to MongoDB in %s mode", self.config.mode.value)
    except ConnectionFailure as e:
      msg = f"Failed to connect to MongoDB ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="mongodb",
      ) from e
    except Exception as e:
      # Unexpected errors (e.g., RuntimeError from mocking in tests)
      msg = f"Failed to connect to MongoDB ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="mongodb",
      ) from e

  def _build_client_kwargs(self) -> dict[str, Any]:
    """Build common MongoDB client kwargs.

    Returns:
        Dictionary of client configuration options.
    """
    # Return cached kwargs if available
    if self._client_kwargs is not None:
      return self._client_kwargs.copy()

    kwargs: dict[str, Any] = {
      "minPoolSize": self.config.min_pool_size,
      "maxPoolSize": self.config.max_pool_size,
      "maxIdleTimeMS": self.config.max_idle_time_ms,
      "waitQueueTimeoutMS": self.config.wait_queue_timeout_ms,
      "serverSelectionTimeoutMS": self.config.server_selection_timeout_ms,
      "heartbeatFrequencyMS": self.config.heartbeat_frequency_ms,
    }

    # Add write concern if specified
    if self.config.w is not None:
      kwargs["w"] = self.config.w
    if self.config.journal is not None:
      kwargs["journal"] = self.config.journal
    if self.config.w_timeout_ms is not None:
      kwargs["wtimeoutMS"] = self.config.w_timeout_ms

    # Add TLS/SSL settings
    if self.config.tls_enabled:
      kwargs["tls"] = True
      if self.config.tls_ca_file:
        kwargs["tlsCAFile"] = self.config.tls_ca_file
      if self.config.tls_cert_file:
        kwargs["tlsCertificateKeyFile"] = self.config.tls_cert_file
      if self.config.tls_key_file and not self.config.tls_cert_file:
        kwargs["tlsCertificateKeyFile"] = self.config.tls_key_file
      kwargs["tlsAllowInvalidCertificates"] = self.config.tls_allow_invalid_certificates

    # Add authentication
    if self.config.username and self.config.password:
      kwargs["username"] = self.config.username
      kwargs["password"] = self.config.password
      if self.config.auth_source:
        kwargs["authSource"] = self.config.auth_source
      if self.config.auth_mechanism:
        kwargs["authMechanism"] = self.config.auth_mechanism

    # Add read preference
    read_pref = self._get_read_preference()
    if read_pref:
      kwargs["readPreference"] = read_pref

    # Cache for future use
    self._client_kwargs = kwargs.copy()
    return kwargs

  def _compute_read_preference(self) -> str | None:
    """Compute read preference string for MongoDB.

    Returns:
        Read preference string or None for default.
    """
    read_pref = getattr(self.config, "read_preference", None)
    if read_pref is None:
      return None
    normalized = read_pref.lower().replace("_", "")
    return self._READ_PREF_MAP.get(normalized)

  def _get_read_preference(self) -> str | None:
    """Get cached read preference string for MongoDB.

    Returns:
        Read preference string or None for default.
    """
    return self._read_preference

  def _initialize_collections(self) -> None:
    """Initialize database and create indexes."""
    if self._client is None:
      msg = "MongoDB client not initialized"
      raise BackendConnectionError(msg, backend_type="mongodb")
    # Initialize database and collections
    self._db = self._client[self.config.database]
    self._queue_collection = self._db[self.config.queue_collection]
    self._set_collection = self._db[self.config.set_collection]
    self._storage_collection = self._db[self.config.storage_collection]

    # Create indexes
    self._create_indexes()

  def _connect_standalone(self) -> None:
    """Connect to standalone MongoDB instance."""
    kwargs = self._build_client_kwargs()
    self._client = MongoClient(self.config.uri, **kwargs)
    self._client.admin.command("ping")
    self._initialize_collections()

  def _connect_replica_set(self) -> None:
    """Connect to MongoDB replica set.

    Uses replica_set_name if provided, otherwise uses URI.
    """
    kwargs = self._build_client_kwargs()

    # Build connection URI for replica set
    if self.config.replica_set_members:
      # Build connection string with replica set members
      members = ",".join(self.config.replica_set_members)
      uri = f"mongodb://{members}/{self.config.database}"
      if self.config.replica_set_name:
        uri += f"?replicaSet={self.config.replica_set_name}"
    else:
      uri = self.config.uri

    if self.config.replica_set_name:
      kwargs["replicaSet"] = self.config.replica_set_name

    self._client = MongoClient(uri, **kwargs)
    self._client.admin.command("ping")
    self._initialize_collections()

  def _connect_sharded_cluster(self) -> None:
    """Connect to MongoDB sharded cluster.

    Connects via mongos routers.
    """
    kwargs = self._build_client_kwargs()

    if self.config.mongos_routers:
      # Use mongos routers as connection points
      routers = ",".join(self.config.mongos_routers)
      uri = f"mongodb://{routers}/{self.config.database}"
      self._client = MongoClient(uri, **kwargs)
    else:
      # Fall back to provided URI
      self._client = MongoClient(self.config.uri, **kwargs)

    self._client.admin.command("ping")
    self._initialize_collections()

  def _connect_atlas(self) -> None:
    """Connect to MongoDB Atlas.

    Uses standard Atlas connection string with TLS enabled.
    """
    kwargs = self._build_client_kwargs()

    # Atlas always requires TLS
    kwargs["tls"] = True

    self._client = MongoClient(self.config.uri, **kwargs)
    self._client.admin.command("ping")
    self._initialize_collections()

  def _create_indexes(self) -> None:
    """Create necessary indexes for collections.

    Raises:
        BackendConnectionError: If collections are not initialized.
    """
    if (
      self._queue_collection is None
      or self._set_collection is None
      or self._storage_collection is None
    ):
      msg = "Collections not initialized: call _initialize_collections() first"
      raise BackendConnectionError(msg, backend_type="mongodb")
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
    except PyMongoError:
      return False
    else:
      return True

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

  def _assert_connected(self) -> None:
    """Verify all collections are initialized.

    Raises:
        BackendConnectionError: If not connected.
    """
    if (
      self._queue_collection is None
      or self._set_collection is None
      or self._storage_collection is None
    ):
      msg = "Not connected: call connect() first"
      raise BackendConnectionError(msg, backend_type="mongodb")

  # QueueBackend implementation
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push item to priority queue.

    Args:
        queue_name: Name of the queue.
        item: Item to push (bytes).
        priority: Priority value (higher = more urgent).

    Raises:
        QueueError: If the push operation fails.
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    self._assert_connected()
    assert self._queue_collection is not None
    doc = {
      "queue_name": queue_name,
      "item": item,
      "priority": -priority,  # Negated for DESC sort
      "created_at": datetime.now(tz=timezone.utc),
    }
    try:
      self._queue_collection.insert_one(doc)
    except PyMongoError as e:
      msg = f"Failed to push to queue {queue_name}: {e}"
      raise QueueError(msg, queue_name=queue_name, operation="push") from e

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:  # noqa: ARG002
    """Pop highest priority item from queue.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (unused for MongoDB, blocking not supported).

    Returns:
        The popped item, or None if queue is empty.

    Raises:
        QueueError: If the pop operation fails.
    """
    self._assert_connected()
    assert self._queue_collection is not None
    try:
      # MongoDB doesn't support blocking pop, so we ignore timeout
      result = self._queue_collection.find_one_and_delete(
        {"queue_name": queue_name},
        sort=[("priority", ASCENDING), ("created_at", ASCENDING)],
      )
    except PyMongoError as e:
      msg = f"Failed to pop from queue {queue_name}: {e}"
      raise QueueError(msg, queue_name=queue_name, operation="pop") from e
    if result:
      return result["item"]
    return None

  def queue_len(self, queue_name: str) -> int:
    """Get queue length.

    Uses count_documents with limit to avoid O(n) full collection scans.
    The limit (100000) provides an upper bound; for queues exceeding this
    threshold, the returned value indicates "at least N" rather than exact count.

    Args:
        queue_name: Name of the queue.

    Returns:
        Number of items in the queue (capped at 100000).
    """
    self._assert_connected()
    assert self._queue_collection is not None
    return self._queue_collection.count_documents(
      {"queue_name": queue_name}, limit=100000
    )

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.
    """
    self._assert_connected()
    assert self._queue_collection is not None
    self._queue_collection.delete_many({"queue_name": queue_name})

  # SetBackend implementation
  def add(self, set_name: str, item: bytes) -> bool:
    """Add item to set.

    Args:
        set_name: Name of the set.
        item: Item to add (bytes).

    Returns:
        True if added, False if already existed.
    """
    self._assert_connected()
    assert self._set_collection is not None
    doc = {
      "set_name": set_name,
      "item_hash": _hash_item(item),
      "item": item,
      "created_at": datetime.now(tz=timezone.utc),
    }
    try:
      self._set_collection.insert_one(doc)
    except DuplicateKeyError:
      return False
    else:
      return True

  def remove(self, set_name: str, item: bytes) -> bool:
    """Remove item from set.

    Args:
        set_name: Name of the set.
        item: Item to remove.

    Returns:
        True if removed, False if didn't exist.
    """
    self._assert_connected()
    assert self._set_collection is not None
    result = self._set_collection.delete_one(
      {
        "set_name": set_name,
        "item_hash": _hash_item(item),
      }
    )
    return result.deleted_count > 0

  def contains(self, set_name: str, item: bytes) -> bool:
    """Check if item is in set.

    Args:
        set_name: Name of the set.
        item: Item to check.

    Returns:
        True if item exists in the set.
    """
    self._assert_connected()
    assert self._set_collection is not None
    result = self._set_collection.find_one(
      {
        "set_name": set_name,
        "item_hash": _hash_item(item),
      }
    )
    return result is not None

  def set_len(self, set_name: str) -> int:
    """Get set size.

    Uses count_documents with limit to avoid O(n) full collection scans.
    The limit (100000) provides an upper bound; for sets exceeding this
    threshold, the returned value indicates "at least N" rather than exact count.

    Args:
        set_name: Name of the set.

    Returns:
        Number of items in the set (capped at 100000).
    """
    self._assert_connected()
    assert self._set_collection is not None
    return self._set_collection.count_documents(
      {"set_name": set_name}, limit=100000
    )

  def clear_set(self, set_name: str) -> None:
    """Clear all items from set.

    Args:
        set_name: Name of the set.
    """
    self._assert_connected()
    assert self._set_collection is not None
    self._set_collection.delete_many({"set_name": set_name})

  # StorageBackend implementation
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store data with key.

    Args:
        key: Storage key.
        data: Data to store (bytes).
        ttl: Optional time-to-live in seconds.
    """
    self._assert_connected()
    assert self._storage_collection is not None
    doc: dict[str, Any] = {
      "key": key,
      "data": data,
    }
    if ttl is not None:
      doc["expireAt"] = datetime.now(tz=timezone.utc) + timedelta(seconds=ttl)

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
    self._assert_connected()
    assert self._storage_collection is not None
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
    self._assert_connected()
    assert self._storage_collection is not None
    result = self._storage_collection.delete_one({"key": key})
    return result.deleted_count > 0

  def exists(self, key: str) -> bool:
    """Check if key exists.

    Args:
        key: Storage key.

    Returns:
        True if key exists.
    """
    self._assert_connected()
    assert self._storage_collection is not None
    result = self._storage_collection.find_one({"key": key}, {"_id": 1})
    return result is not None

  def ttl(self, key: str) -> int | None:
    """Get remaining time-to-live.

    Args:
        key: Storage key.

    Returns:
        Seconds remaining, None if no TTL, -1 if expired.
    """
    self._assert_connected()
    assert self._storage_collection is not None
    result = self._storage_collection.find_one({"key": key}, {"expireAt": 1})
    if result is None:
      return -1
    if "expireAt" not in result:
      return None

    expire_at = result["expireAt"]
    remaining = (expire_at - datetime.now(tz=timezone.utc)).total_seconds()
    return max(0, int(remaining))

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    Args:
        prefix: If provided, only clear keys starting with this prefix.
               If None, clear all storage data.
    """
    self._assert_connected()
    assert self._storage_collection is not None
    if prefix:
      # Limit prefix length to prevent regex DoS attacks (ReDoS)
      prefix = prefix[:128]
      pattern = re.escape(prefix)
      self._storage_collection.delete_many({"key": {"$regex": f"^{pattern}"}})
    else:
      self._storage_collection.delete_many({})
