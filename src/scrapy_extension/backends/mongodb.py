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
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
  from pymongo import ASCENDING, MongoClient, ReadPreference
  from pymongo.errors import ConnectionFailure, DuplicateKeyError, PyMongoError
  from pymongo.read_concern import ReadConcern
  from pymongo.write_concern import WriteConcern
except ImportError as e:
  if not _is_missing_optional_dependency(e, "pymongo"):
    raise
  raise ImportError(
    "MongoDB backend requires 'pymongo'. Install with: pip install scrapy-extension[mongodb]"
  ) from e

from scrapy_extension.backends._redaction import _redact
from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
  _hash_item,
  _validate_key_name,
  _validate_ttl,
  secret_value,
)
from scrapy_extension.exceptions import (
  BackendConnectionError,
  ConfigurationError,
  QueueError,
)
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import MongoDBMode
from scrapy_extension.settings.mongodb import (
  validate_mongodb_collection_domains,
  validate_mongodb_write_concern,
)

if TYPE_CHECKING:
  from pymongo.collection import Collection
  from pymongo.database import Database

  from scrapy_extension.settings import MongoDBSettings

logger = logging.getLogger(__name__)

_CAPABILITY_DOMAIN_MARKER_ID = "scrapy-extension:capability-domain:v1"
_CAPABILITY_DOMAIN_MARKER_FIELD = "scrapy_extension_capability_domain"


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

  _push_is_durable = True

  # Read preference mapping - defined as class constant to avoid recreating
  _READ_PREF_MAP: ClassVar[dict[str, str]] = {
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
    # U8: parameterize pymongo generics — the document shape is backend-defined
    # (queue docs / set docs / storage docs all carry their own keys), so
    # ``dict[str, Any]`` is the honest element type rather than a leak.
    self._client: MongoClient[dict[str, Any]] | None = None
    self._db: Database[dict[str, Any]] | None = None
    self._queue_collection: Collection[dict[str, Any]] | None = None
    self._set_collection: Collection[dict[str, Any]] | None = None
    self._storage_collection: Collection[dict[str, Any]] | None = None
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
    mode = self.config.mode
    if mode not in (
      MongoDBMode.STANDALONE,
      MongoDBMode.REPLICA_SET,
      MongoDBMode.SHARDED_CLUSTER,
      MongoDBMode.ATLAS,
    ):
      try:
        mode_text = str(mode)
      except (TypeError, ValueError):
        mode_text = getattr(mode, "value", repr(mode))
      msg = f"Unsupported MongoDB mode: {mode_text}"
      raise ConfigurationError(
        msg,
        setting_name="mode",
        setting_value=mode,
      )
    # Settings models remain mutable after construction. Recheck before any
    # client construction so a live mutation cannot merge capability domains
    # or downgrade public writes to an unacknowledged socket handoff.
    collection_names = validate_mongodb_collection_domains(
      self.config.queue_collection,
      self.config.set_collection,
      self.config.storage_collection,
    )
    database = self.config.database
    _, w_timeout_ms = self._validated_write_concern()
    marker_options = self._marker_collection_options(
      mode,
      journal=self.config.journal,
      w_timeout_ms=w_timeout_ms,
    )
    try:
      if mode == MongoDBMode.STANDALONE:
        self._connect_standalone(database, collection_names, marker_options)
      elif mode == MongoDBMode.REPLICA_SET:
        self._connect_replica_set(database, collection_names, marker_options)
      elif mode == MongoDBMode.SHARDED_CLUSTER:
        self._connect_sharded_cluster(database, collection_names, marker_options)
      else:
        self._connect_atlas(database, collection_names, marker_options)
      logger.debug("Connected to MongoDB in %s mode", mode.value)
    except ConnectionFailure as e:
      self._discard_client(suppress_process_control=True)
      msg = f"Failed to connect to MongoDB ({mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="mongodb",
      ) from e
    except BackendConnectionError:
      self._discard_client(suppress_process_control=True)
      raise
    except ConfigurationError:
      self._discard_client(suppress_process_control=True)
      raise
    except Exception as e:
      self._discard_client(suppress_process_control=True)
      # Unexpected errors (e.g., RuntimeError from mocking in tests)
      msg = f"Failed to connect to MongoDB ({mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="mongodb",
      ) from e
    except BaseException:
      self._discard_client(suppress_process_control=True)
      raise

  def _discard_client(self, *, suppress_process_control: bool = False) -> None:
    """Clear all handles and best-effort close the current client."""
    client = self._client
    self._client = None
    self._db = None
    self._queue_collection = None
    self._set_collection = None
    self._storage_collection = None
    if client is not None:
      try:
        client.close()
      except Exception:
        try:
          logger.debug("Failed to close MongoDB client", exc_info=True)
        except BaseException:
          if not suppress_process_control:
            raise
      except BaseException:
        if not suppress_process_control:
          raise
        try:
          logger.debug(
            "Process-control interruption while closing failed MongoDB client",
            exc_info=True,
          )
        except BaseException:
          # Failed-connect cleanup must never replace the original exception.
          pass

  def _build_client_kwargs(self) -> dict[str, Any]:
    """Build common MongoDB client kwargs.

    Returns:
        Dictionary of client configuration options.
    """
    self._validated_write_concern()
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

    kwargs.update(self._write_concern_kwargs())
    kwargs.update(self._tls_kwargs())
    kwargs.update(self._auth_kwargs())

    # Add read preference
    read_pref = self._get_read_preference()
    if read_pref:
      kwargs["readPreference"] = read_pref

    # Cache for future use
    self._client_kwargs = kwargs.copy()
    return kwargs

  def _write_concern_kwargs(self) -> dict[str, Any]:
    """Build write-concern kwargs from config (w / journal / wtimeoutMS)."""
    w, w_timeout_ms = self._validated_write_concern()
    kwargs: dict[str, Any] = {"w": w}
    if self.config.journal is not None:
      kwargs["journal"] = self.config.journal
    if w_timeout_ms is not None:
      kwargs["wtimeoutMS"] = w_timeout_ms
    return kwargs

  def _validated_write_concern(self) -> tuple[int | str, int | None]:
    """Revalidate the acknowledged-write policy against mutable settings."""
    return validate_mongodb_write_concern(
      self.config.w,
      self.config.w_timeout_ms,
    )

  def _tls_kwargs(self) -> dict[str, Any]:
    """Build TLS/SSL kwargs from config (empty when tls disabled)."""
    kwargs: dict[str, Any] = {}
    if not self.config.tls_enabled:
      return kwargs
    kwargs["tls"] = True
    if self.config.tls_ca_file:
      kwargs["tlsCAFile"] = self.config.tls_ca_file
    if self.config.tls_cert_file:
      kwargs["tlsCertificateKeyFile"] = self.config.tls_cert_file
    if self.config.tls_key_file and not self.config.tls_cert_file:
      kwargs["tlsCertificateKeyFile"] = self.config.tls_key_file
    kwargs["tlsAllowInvalidCertificates"] = self.config.tls_allow_invalid_certificates
    return kwargs

  def _auth_kwargs(self) -> dict[str, Any]:
    """Build authentication kwargs from config (empty when no credentials)."""
    kwargs: dict[str, Any] = {}
    if not (self.config.username and self.config.password):
      return kwargs
    kwargs["username"] = self.config.username
    kwargs["password"] = _redact(secret_value(self.config.password))
    if self.config.auth_source:
      kwargs["authSource"] = self.config.auth_source
    if self.config.auth_mechanism:
      kwargs["authMechanism"] = self.config.auth_mechanism
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

  def _initialize_collections(
    self,
    database: str,
    collection_names: tuple[str, str, str],
    marker_options: Mapping[str, Any] | None = None,
  ) -> None:
    """Initialize database and create indexes."""
    if self._client is None:
      msg = "MongoDB client not initialized"
      raise BackendConnectionError(msg, backend_type="mongodb")
    # Initialize database and collections
    queue_collection, set_collection, storage_collection = collection_names
    self._db = self._client[database]
    self._queue_collection = self._db[queue_collection]
    self._set_collection = self._db[set_collection]
    self._storage_collection = self._db[storage_collection]

    if marker_options is None:
      _, w_timeout_ms = self._validated_write_concern()
      marker_options = self._marker_collection_options(
        self.config.mode,
        journal=self.config.journal,
        w_timeout_ms=w_timeout_ms,
      )
    self._claim_collection_domains(marker_options)

    # Create indexes
    self._create_indexes()

  def _claim_collection_domains(
    self,
    marker_options: Mapping[str, Any],
  ) -> None:
    """Claim each physical collection for exactly one capability domain."""
    if (
      self._queue_collection is None
      or self._set_collection is None
      or self._storage_collection is None
    ):
      msg = "Collections not initialized: cannot claim capability domains"
      raise BackendConnectionError(msg, backend_type="mongodb")
    claims = (
      (self._queue_collection, "queue"),
      (self._set_collection, "set"),
      (self._storage_collection, "storage"),
    )
    for collection, domain in claims:
      marker_collection = collection.with_options(**marker_options)
      self._claim_collection_domain(marker_collection, domain)

  @staticmethod
  def _marker_collection_options(
    mode: MongoDBMode,
    *,
    journal: bool | None,
    w_timeout_ms: int | None,
  ) -> dict[str, Any]:
    """Build an isolated durability policy for capability-domain markers."""
    options: dict[str, Any] = {"read_preference": ReadPreference.PRIMARY}
    if mode == MongoDBMode.STANDALONE:
      return options

    write_concern_kwargs: dict[str, Any] = {"w": "majority"}
    if journal is not None:
      write_concern_kwargs["j"] = journal
    if w_timeout_ms is not None:
      write_concern_kwargs["wtimeout"] = w_timeout_ms
    options["read_concern"] = ReadConcern("majority")
    options["write_concern"] = WriteConcern(**write_concern_kwargs)
    return options

  @staticmethod
  def _claim_collection_domain(
    collection: Collection[dict[str, Any]],
    domain: str,
  ) -> None:
    """Atomically install or confirm one collection's domain marker."""
    exists, existing_domain = MongoDBBackend._read_collection_domain_marker(
      collection
    )
    if exists:
      MongoDBBackend._require_matching_collection_domain(existing_domain, domain)
      return

    marker = {
      "_id": _CAPABILITY_DOMAIN_MARKER_ID,
      # Domain-dependent data is deliberately below an array boundary. A
      # sharded collection cannot use a multikey index as its shard-key index,
      # so a valid shard key either sees identical fixed/missing values for all
      # contenders (routing them together) or rejects this insert fail-closed.
      # This preserves the fixed-_id mutex even when _id is not the shard key.
      _CAPABILITY_DOMAIN_MARKER_FIELD: [{"domain": domain}],
    }
    try:
      collection.insert_one(marker)
    except DuplicateKeyError as conflict:
      exists, existing_domain = MongoDBBackend._read_collection_domain_marker(
        collection
      )
      if not exists:
        raise BackendConnectionError(
          "Failed to install the MongoDB capability-domain marker.",
          backend_type="mongodb",
        ) from conflict
      MongoDBBackend._require_matching_collection_domain(existing_domain, domain)

  @staticmethod
  def _read_collection_domain_marker(
    collection: Collection[dict[str, Any]],
  ) -> tuple[bool, object]:
    """Read at most two markers so a poisoned sharded state fails closed."""
    markers = list(
      collection.find(
        {"_id": _CAPABILITY_DOMAIN_MARKER_ID},
        {_CAPABILITY_DOMAIN_MARKER_FIELD: 1},
      ).limit(2)
    )
    if len(markers) > 1:
      raise ConfigurationError(
        "A MongoDB physical collection has conflicting domain markers.",
        setting_name="collection_names",
      )
    marker = markers[0] if markers else None
    # A real PyMongo Collection configured by this backend returns ``dict`` or
    # ``None``. Treat a non-mapping test double as inconclusive/absent: the
    # subsequent insert is still authoritative and a real existing marker
    # forces DuplicateKey plus a second primary read before acceptance.
    if marker is None or not isinstance(marker, Mapping):
      return False, None
    return True, marker.get(_CAPABILITY_DOMAIN_MARKER_FIELD)

  @staticmethod
  def _require_matching_collection_domain(
    existing_domain: object,
    requested_domain: str,
  ) -> None:
    """Accept only the exact one-element ownership envelope."""
    if (
      type(existing_domain) is list
      and len(existing_domain) == 1
      and type(existing_domain[0]) is dict
      and set(existing_domain[0]) == {"domain"}
      and type(existing_domain[0]["domain"]) is str
      and existing_domain[0]["domain"] == requested_domain
    ):
      return
    raise ConfigurationError(
      (
        "A MongoDB physical collection is already claimed by another or "
        "malformed scrapy-extension capability domain."
      ),
      setting_name="collection_names",
    )

  def _connect_standalone(
    self,
    database: str,
    collection_names: tuple[str, str, str],
    marker_options: Mapping[str, Any],
  ) -> None:
    """Connect to standalone MongoDB instance."""
    kwargs = self._build_client_kwargs()
    self._client = MongoClient(self.config.uri, **kwargs)
    self._client.admin.command("ping")
    self._initialize_collections(database, collection_names, marker_options)

  def _connect_replica_set(
    self,
    database: str,
    collection_names: tuple[str, str, str],
    marker_options: Mapping[str, Any],
  ) -> None:
    """Connect to MongoDB replica set.

    Uses replica_set_name if provided, otherwise uses URI.
    """
    kwargs = self._build_client_kwargs()

    # Build connection URI for replica set
    if self.config.replica_set_members:
      # Build connection string with replica set members
      members = ",".join(self.config.replica_set_members)
      uri = f"mongodb://{members}/{database}"
      if self.config.replica_set_name:
        uri += f"?replicaSet={self.config.replica_set_name}"
    else:
      uri = self.config.uri

    if self.config.replica_set_name:
      kwargs["replicaSet"] = self.config.replica_set_name

    self._client = MongoClient(uri, **kwargs)
    self._client.admin.command("ping")
    self._initialize_collections(database, collection_names, marker_options)

  def _connect_sharded_cluster(
    self,
    database: str,
    collection_names: tuple[str, str, str],
    marker_options: Mapping[str, Any],
  ) -> None:
    """Connect to MongoDB sharded cluster.

    Connects via mongos routers.
    """
    kwargs = self._build_client_kwargs()

    if self.config.mongos_routers:
      # Use mongos routers as connection points
      routers = ",".join(self.config.mongos_routers)
      uri = f"mongodb://{routers}/{database}"
      self._client = MongoClient(uri, **kwargs)
    else:
      # Fall back to provided URI
      self._client = MongoClient(self.config.uri, **kwargs)

    self._client.admin.command("ping")
    self._initialize_collections(database, collection_names, marker_options)

  def _connect_atlas(
    self,
    database: str,
    collection_names: tuple[str, str, str],
    marker_options: Mapping[str, Any],
  ) -> None:
    """Connect to MongoDB Atlas.

    Uses standard Atlas connection string with TLS enabled.
    """
    kwargs = self._build_client_kwargs()

    # Atlas always requires TLS
    kwargs["tls"] = True

    self._client = MongoClient(self.config.uri, **kwargs)
    self._client.admin.command("ping")
    self._initialize_collections(database, collection_names, marker_options)

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
      [
        ("queue_name", ASCENDING),
        ("priority", ASCENDING),
        ("created_at", ASCENDING),
      ]
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
    self._discard_client()

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
    if self._queue_collection is None:
      msg = "MongoDBBackend not connected: queue collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
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

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop highest priority item from queue.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (unused for MongoDB, blocking not supported).

    Returns:
        The popped item, or None if queue is empty.

    Raises:
        QueueError: If the pop operation fails.
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    self._assert_connected()
    if self._queue_collection is None:
      msg = "MongoDBBackend not connected: queue collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
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
      # find_one_and_delete returns Any; the queue doc stores ``item`` as bytes.
      return cast(bytes, result["item"])
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

    Raises:
        QueueError: If the count request fails.
    """
    _validate_key_name(queue_name, "queue_name")
    self._assert_connected()
    if self._queue_collection is None:
      msg = "MongoDBBackend not connected: queue collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      return self._queue_collection.count_documents(
        {"queue_name": queue_name}, limit=100000
      )
    except PyMongoError as e:
      msg = f"Failed to get queue length for {queue_name}: {e}"
      raise QueueError(msg, queue_name=queue_name, operation="queue_len") from e

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
        QueueError: If the delete request fails.
    """
    _validate_key_name(queue_name, "queue_name")
    self._assert_connected()
    if self._queue_collection is None:
      msg = "MongoDBBackend not connected: queue collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      self._queue_collection.delete_many({"queue_name": queue_name})
    except PyMongoError as e:
      msg = f"Failed to clear queue {queue_name}: {e}"
      raise QueueError(msg, queue_name=queue_name, operation="clear_queue") from e

  # SetBackend implementation
  def add(self, set_name: str, item: bytes) -> bool:
    """Add item to set.

    Args:
        set_name: Name of the set.
        item: Item to add (bytes).

    Returns:
        True if added, False if already existed.

    Raises:
        ValueError: If set_name contains invalid characters.
    """
    _validate_key_name(set_name, "set_name")
    self._assert_connected()
    if self._set_collection is None:
      msg = "MongoDBBackend not connected: set collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
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
    except PyMongoError as e:
      # R-dupe-1 (option b): wrap operational PyMongoError so BackendDupeFilter's
      # graceful-degradation arm catches it (degrade to not-seen) instead of
      # crashing the crawl. DuplicateKeyError (the "already existed" signal)
      # stays first so it still returns False.
      raise BackendConnectionError(
        f"MongoDB set add failed for {set_name!r}: {e}", backend_type="mongodb"
      ) from e
    else:
      return True

  def remove(self, set_name: str, item: bytes) -> bool:
    """Remove item from set.

    Args:
        set_name: Name of the set.
        item: Item to remove.

    Returns:
        True if removed, False if didn't exist.

    Raises:
        ValueError: If set_name contains invalid characters.
    """
    _validate_key_name(set_name, "set_name")
    self._assert_connected()
    if self._set_collection is None:
      msg = "MongoDBBackend not connected: set collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      result = self._set_collection.delete_one(
        {
          "set_name": set_name,
          "item_hash": _hash_item(item),
        }
      )
    except PyMongoError as e:
      raise BackendConnectionError(
        f"MongoDB set remove failed for {set_name!r}: {e}",
        backend_type="mongodb",
      ) from e
    return result.deleted_count > 0

  def contains(self, set_name: str, item: bytes) -> bool:
    """Check if item is in set.

    Args:
        set_name: Name of the set.
        item: Item to check.

    Returns:
        True if item exists in the set.

    Raises:
        ValueError: If set_name contains invalid characters.
    """
    _validate_key_name(set_name, "set_name")
    self._assert_connected()
    if self._set_collection is None:
      msg = "MongoDBBackend not connected: set collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      result = self._set_collection.find_one(
        {
          "set_name": set_name,
          "item_hash": _hash_item(item),
        }
      )
    except PyMongoError as e:
      raise BackendConnectionError(
        f"MongoDB set contains failed for {set_name!r}: {e}",
        backend_type="mongodb",
      ) from e
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
    _validate_key_name(set_name, "set_name")
    self._assert_connected()
    if self._set_collection is None:
      msg = "MongoDBBackend not connected: set collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      return self._set_collection.count_documents({"set_name": set_name}, limit=100000)
    except PyMongoError as e:
      raise BackendConnectionError(
        f"MongoDB set length failed for {set_name!r}: {e}",
        backend_type="mongodb",
      ) from e

  def clear_set(self, set_name: str) -> None:
    """Clear all items from set.

    Args:
        set_name: Name of the set.

    Raises:
        ValueError: If set_name contains invalid characters.
        BackendConnectionError: If not connected, or if the delete fails at the
            MongoDB layer (parity with add R-dupe-1 #38 + redis clear_set #71).
    """
    _validate_key_name(set_name, "set_name")
    self._assert_connected()
    if self._set_collection is None:
      msg = "MongoDBBackend not connected: set collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      self._set_collection.delete_many({"set_name": set_name})
    except PyMongoError as e:
      # R-rclears-mongo: wrap operational PyMongoError (parity with add
      # R-dupe-1 #38 + redis clear_set #71) so BackendDupeFilter's
      # graceful-degradation arm can fire; a raw leak crashes callers
      # expecting BackendError.
      raise BackendConnectionError(
        f"MongoDB set clear failed for {set_name!r}: {e}",
        backend_type="mongodb",
      ) from e

  # StorageBackend implementation
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store data with key.

    Args:
        key: Storage key.
        data: Data to store (bytes).
        ttl: Optional time-to-live in seconds.

    Raises:
        BackendConnectionError: If not connected.
        ValueError: If key contains invalid characters.
        StorageError: On PyMongoError (was previously unwrapped, leaking
            ``pymongo.errors.PyMongoError`` to callers expecting
            ``except BackendError``).
    """
    _validate_key_name(key, "key")
    _validate_ttl(ttl)
    self._assert_connected()
    if self._storage_collection is None:
      msg = "MongoDBBackend not connected: storage collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    doc: dict[str, Any] = {
      "key": key,
      "data": data,
    }
    if ttl is not None:
      doc["expireAt"] = datetime.now(tz=timezone.utc) + timedelta(seconds=ttl)

    try:
      self._storage_collection.replace_one(
        {"key": key},
        doc,
        upsert=True,
      )
    except PyMongoError as e:
      msg = f"Failed to store key {key!r} in MongoDB: {e}"
      raise StorageError(msg, operation="store", key=key) from e

  @staticmethod
  def _remaining_storage_ttl(document: dict[str, Any]) -> float | None:
    """Return seconds until ``document`` expires, or None without a TTL."""
    raw_expiry = document.get("expireAt")
    if raw_expiry is None:
      return None

    expire_at = cast(datetime, raw_expiry)
    # BSON datetimes are UTC, but PyMongo returns them without tzinfo unless
    # the client opts into tz-aware decoding.
    if expire_at.tzinfo is None:
      expire_at = expire_at.replace(tzinfo=timezone.utc)
    return (expire_at - datetime.now(tz=timezone.utc)).total_seconds()

  def _lazy_reap_expired_storage(self, document: dict[str, Any], key: str) -> bool:
    """Conditionally reap an expired snapshot and report whether it expired."""
    remaining = self._remaining_storage_ttl(document)
    if remaining is None or remaining > 0:
      return False

    if self._storage_collection is None:
      msg = "MongoDBBackend not connected: storage collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      # The read snapshot may be stale: a concurrent store() can replace the
      # same key before this delete runs. Matching the observed expireAt makes
      # the cleanup a CAS, so a fresh replacement is not removed.
      self._storage_collection.delete_one(
        {"key": key, "expireAt": document["expireAt"]}
      )
    except PyMongoError as e:
      logger.warning("Failed to reap expired MongoDB storage key %r: %s", key, e)
    return True

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve current data by key.

    Args:
        key: Storage key.

    Returns:
        Stored data, or None if not found or expired.

    Raises:
        BackendConnectionError: If not connected.
        StorageError: On PyMongoError (was previously unwrapped).
    """
    _validate_key_name(key, "key")
    self._assert_connected()
    if self._storage_collection is None:
      msg = "MongoDBBackend not connected: storage collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      result = self._storage_collection.find_one({"key": key})
      if result and self._lazy_reap_expired_storage(result, key):
        return None
    except PyMongoError as e:
      msg = f"Failed to retrieve key {key!r} from MongoDB: {e}"
      raise StorageError(msg, operation="retrieve", key=key) from e
    if result:
      # storage doc stores ``data`` as bytes; cast narrows the Any from pymongo.
      return cast(bytes, result.get("data"))
    return None

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if deleted, False if didn't exist.

    Raises:
        BackendConnectionError: If not connected.
        StorageError: On PyMongoError (was previously unwrapped).
    """
    _validate_key_name(key, "key")
    self._assert_connected()
    if self._storage_collection is None:
      msg = "MongoDBBackend not connected: storage collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      result = self._storage_collection.delete_one({"key": key})
    except PyMongoError as e:
      msg = f"Failed to delete key {key!r} in MongoDB: {e}"
      raise StorageError(msg, operation="delete", key=key) from e
    return result.deleted_count > 0

  def exists(self, key: str) -> bool:
    """Check if key exists and has not expired.

    Args:
        key: Storage key.

    Returns:
        True if key exists and is current.

    Raises:
        BackendConnectionError: If not connected.
        StorageError: On PyMongoError (was previously unwrapped).
    """
    _validate_key_name(key, "key")
    self._assert_connected()
    if self._storage_collection is None:
      msg = "MongoDBBackend not connected: storage collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      result = self._storage_collection.find_one(
        {"key": key}, {"_id": 1, "expireAt": 1}
      )
      if result and self._lazy_reap_expired_storage(result, key):
        return False
    except PyMongoError as e:
      msg = f"Failed to check existence of key {key!r} in MongoDB: {e}"
      raise StorageError(msg, operation="exists", key=key) from e
    return result is not None

  def ttl(self, key: str) -> int | None:
    """Get remaining time-to-live.

    Args:
        key: Storage key.

    Returns:
        Non-negative seconds remaining, or None if absent, permanent, or expired.

    Raises:
        BackendConnectionError: If not connected.
        StorageError: On PyMongoError (was previously unwrapped).
    """
    _validate_key_name(key, "key")
    self._assert_connected()
    if self._storage_collection is None:
      msg = "MongoDBBackend not connected: storage collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    try:
      result = self._storage_collection.find_one({"key": key}, {"expireAt": 1})
    except PyMongoError as e:
      msg = f"Failed to read TTL of key {key!r} in MongoDB: {e}"
      raise StorageError(msg, operation="ttl", key=key) from e
    if result is None:
      return None
    remaining = self._remaining_storage_ttl(result)
    if remaining is None:
      return None
    if remaining <= 0:
      self._lazy_reap_expired_storage(result, key)
      return None
    return max(0, int(remaining))

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    Args:
        prefix: If provided, only clear keys starting with this prefix.
               If None, clear all storage data.

    Raises:
        BackendConnectionError: If not connected.
        StorageError: On PyMongoError (was previously unwrapped).
    """
    if prefix is not None:
      _validate_key_name(prefix, "prefix")
    self._assert_connected()
    if self._storage_collection is None:
      msg = "MongoDBBackend not connected: storage collection is None"
      raise BackendConnectionError(msg, backend_type="mongodb")
    if prefix:
      pattern = re.escape(prefix)
      try:
        self._storage_collection.delete_many({"key": {"$regex": f"^{pattern}"}})
      except PyMongoError as e:
        msg = f"Failed to clear MongoDB storage (prefix={prefix!r}): {e}"
        raise StorageError(msg, operation="clear_storage", key=None) from e
    else:
      try:
        self._storage_collection.delete_many(
          {"_id": {"$ne": _CAPABILITY_DOMAIN_MARKER_ID}}
        )
      except PyMongoError as e:
        msg = f"Failed to clear MongoDB storage: {e}"
        raise StorageError(msg, operation="clear_storage", key=None) from e
