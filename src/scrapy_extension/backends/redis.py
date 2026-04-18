"""Redis backend implementation with multimode support.

This module provides a Redis-based implementation of the backend interfaces
for distributed crawling, supporting multiple deployment modes:
- Standalone: Single Redis instance
- Master-Slave: Read replicas with write to master
- Sentinel: High availability with automatic failover
- Cluster: Redis Cluster with automatic sharding
"""

from __future__ import annotations

import contextlib
import logging
import re
from typing import TYPE_CHECKING, Any, cast

# Key name validation pattern - only allow alphanumeric, dots, underscores, hyphens, colons
KEY_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._:-]+$")


def _validate_key_name(name: str, field_name: str = "name") -> None:
    """Validate key/queue/set name to prevent injection.

    Args:
        name: The name to validate.
        field_name: Field name for error messages.

    Raises:
        ValueError: If name contains invalid characters.
    """
    if not name or not KEY_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid {field_name}: {name!r}. "
            f"Only alphanumeric, dots, underscores, hyphens, and colons allowed."
        )

from redis import Redis
from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import RedisError
from redis.sentinel import Sentinel

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.exceptions import (
  BackendConnectionError,
  ConfigurationError,
  QueueError,
)
from scrapy_extension.settings import RedisMode

if TYPE_CHECKING:
  from scrapy_extension.settings import RedisSettings

logger = logging.getLogger(__name__)


class RedisBackend(Backend, QueueBackend, SetBackend, StorageBackend):
  """Redis backend implementation with multimode support.

  Implements all backend interfaces using Redis data structures:
  - Queue: Redis Sorted Sets (ZADD/ZRANGEBYSCORE/ZREM)
  - Set: Redis Sets (SADD/SREM/SISMEMBER/SCARD/DEL)
  - Storage: Redis Strings with TTL (SET/GET/DEL/EXISTS/TTL)

  Supports standalone, master-slave, sentinel, and cluster deployment modes.

  Attributes:
      config: RedisSettings instance with connection parameters.
      _client: The Redis client instance (None until connected).
      _master_client: The master Redis client for master-slave mode.
      _sentinel: Sentinel instance for sentinel mode.
  """

  def __init__(self, config: RedisSettings) -> None:
    """Initialize Redis backend.

    Args:
        config: Configuration for Redis connection.
    """
    self.config = config
    self._client: Redis | RedisCluster | None = None
    self._master_client: Redis | None = None
    self._sentinel: Sentinel | None = None

  def connect(self) -> None:
    """Establish connection to Redis based on deployment mode.

    Creates the appropriate Redis client based on the configuration mode.
    Verifies the connection after creation.

    Raises:
        BackendConnectionError: If the connection cannot be established.
        ConfigurationError: If the configuration is invalid for the mode.
    """
    try:
      if self.config.mode == RedisMode.STANDALONE:
        self._connect_standalone()
      elif self.config.mode == RedisMode.MASTER_SLAVE:
        self._connect_master_slave()
      elif self.config.mode == RedisMode.SENTINEL:
        self._connect_sentinel()
      elif self.config.mode == RedisMode.CLUSTER:
        self._connect_cluster()
      # else:
      #   msg = f"Unsupported Redis mode: {self.config.mode}"
      #   raise ConfigurationError(msg,setting_name="mode",setting_value=self.config.mode)
      logger.debug("Connected to Redis in %s mode", self.config.mode.value)
    except RedisError as e:
      msg = f"Failed to connect to Redis ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="redis",
      ) from e

  def _create_redis_client(self) -> Redis:
    """Create a Redis client with shared configuration.

    Returns:
        Configured Redis client instance.
    """
    return Redis(
      host=self.config.host,
      port=self.config.port,
      db=self.config.db,
      password=self.config.password,
      username=self.config.username,
      socket_timeout=self.config.socket_timeout,
      socket_connect_timeout=self.config.socket_connect_timeout,
      retry_on_timeout=self.config.retry_on_timeout,
      max_connections=self.config.max_connections,
      decode_responses=self.config.decode_responses,
      ssl=self.config.ssl_enabled,
      ssl_ca_certs=self.config.ssl_cafile,
      ssl_certfile=self.config.ssl_certfile,
      ssl_keyfile=self.config.ssl_keyfile,
      ssl_check_hostname=self.config.ssl_check_hostname,
    )

  def _connect_standalone(self) -> None:
    """Connect to standalone Redis instance."""
    self._client = self._create_redis_client()
    self._client.ping()

  def _connect_master_slave(self) -> None:
    """Connect to master-slave setup.

    Creates connection to master for writes. If replicas are configured,
    they can be used for read operations.

    Raises:
        ConfigurationError: If replicas are not configured.
    """
    self._master_client = self._create_redis_client()
    self._master_client.ping()
    self._client = self._master_client

    # Store replica info for potential read scaling (not yet implemented)
    if self.config.replicas:
      logger.debug("Configured %d replicas for read scaling", len(self.config.replicas))

  def _connect_sentinel(self) -> None:
    """Connect via Redis Sentinel for high availability.

    Uses Sentinel to discover the current master and handle failover.

    Raises:
        ConfigurationError: If sentinels are not configured.
    """
    if not self.config.sentinels:
      msg = "Sentinel mode requires 'sentinels' configuration"
      raise ConfigurationError(
        msg,
        setting_name="sentinels",
        setting_value=self.config.sentinels,
      )

    # Parse sentinel addresses
    sentinel_tuples = []
    for sentinel_str in self.config.sentinels:
      host, port_str = sentinel_str.rsplit(":", 1)
      sentinel_tuples.append((host, int(port_str)))

    # Create Sentinel connection
    sentinel_kwargs: dict[str, Any] = {}
    if self.config.sentinel_password:
      sentinel_kwargs["password"] = self.config.sentinel_password
    if self.config.sentinel_username:
      sentinel_kwargs["username"] = self.config.sentinel_username

    self._sentinel = Sentinel(
      sentinel_tuples,
      socket_timeout=self.config.socket_timeout,
      socket_connect_timeout=self.config.socket_connect_timeout,
      retry_on_timeout=self.config.sentinel_retry_on_timeout,
      min_other_sentinels=self.config.min_other_sentinels,
      sentinel_kwargs=sentinel_kwargs or None,
    )

    # Get master connection through sentinel
    self._master_client = self._sentinel.master_for(
      self.config.sentinel_master_name,
      db=self.config.db,
      password=self.config.password,
      username=self.config.username,
      socket_timeout=self.config.socket_timeout,
      socket_connect_timeout=self.config.socket_connect_timeout,
      retry_on_timeout=self.config.retry_on_timeout,
      decode_responses=self.config.decode_responses,
      ssl=self.config.ssl_enabled,
      ssl_ca_certs=self.config.ssl_cafile,
      ssl_certfile=self.config.ssl_certfile,
      ssl_keyfile=self.config.ssl_keyfile,
      ssl_check_hostname=self.config.ssl_check_hostname,
    )

    # Verify connection
    self._master_client.ping()
    self._client = self._master_client

    logger.debug(
      "Connected to master '%s' via Sentinel", self.config.sentinel_master_name
    )

  def _connect_cluster(self) -> None:
    """Connect to Redis Cluster.

    Uses RedisCluster client for automatic sharding and node discovery.

    Raises:
        ConfigurationError: If startup nodes are not configured.
    """
    # Determine startup nodes
    startup_nodes = self.config.cluster_startup_nodes
    if not startup_nodes:
      # Fall back to host:port if no startup nodes configured
      startup_nodes = [f"{self.config.host}:{self.config.port}"]

    # Parse startup nodes
    nodes: list[ClusterNode] = []
    for node_str in startup_nodes:
      host, port_str = node_str.rsplit(":", 1)
      nodes.append(ClusterNode(host=host, port=int(port_str)))

    self._client = RedisCluster(
      startup_nodes=nodes,
      password=self.config.password,
      username=self.config.username,
      socket_timeout=self.config.socket_timeout,
      socket_connect_timeout=self.config.socket_connect_timeout,
      retry_on_timeout=self.config.retry_on_timeout,
      max_connections=self.config.max_connections,
      decode_responses=self.config.decode_responses,
      skip_full_coverage_check=self.config.cluster_skip_full_coverage_check,
      max_redirects=self.config.cluster_max_redirects,
      ssl=self.config.ssl_enabled,
      ssl_ca_certs=self.config.ssl_cafile,
      ssl_certfile=self.config.ssl_certfile,
      ssl_keyfile=self.config.ssl_keyfile,
      ssl_check_hostname=self.config.ssl_check_hostname,
    )

    # Verify connection
    self._client.ping()
    logger.debug("Connected to Redis Cluster with %d startup nodes", len(nodes))

  def disconnect(self) -> None:
    """Close Redis connection.

    Closes the connection pool and releases resources.
    """
    if self._master_client and self._master_client is not self._client:
      with contextlib.suppress(RedisError):
        self._master_client.close()
      self._master_client = None

    if self._client:
      with contextlib.suppress(RedisError):
        self._client.close()
      self._client = None

    self._sentinel = None

  def is_connected(self) -> bool:
    """Check if Redis is connected.

    Returns:
        True if connected and responding to ping, False otherwise.
    """
    try:
      if (client := self._client) is None:
        return False
      result = client.ping()
      return bool(result) if result is not None else False
    except RedisError:
      return False

  def ping(self) -> bool:
    """Check Redis health.

    Returns:
        True if Redis responds to ping.
    """
    try:
      if (client := self._client) is None:
        return False
      result = client.ping()
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
  def client(self) -> Redis | RedisCluster:
    """Get Redis client, connecting if necessary.

    Returns:
        The Redis client instance.

    Raises:
        BackendConnectionError: If not connected and connection fails.
    """
    if self._client is None:
      self.connect()
    return cast("Redis | RedisCluster", self._client)

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
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
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
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    try:
      if timeout > 0:
        # Use BZPOPMAX for blocking pop
        bz_result = cast("tuple[str, bytes, float] | None", self.client.bzpopmax(queue_name, timeout=timeout))
        if bz_result is not None:
          item_bytes = bz_result[1]
          return item_bytes if isinstance(item_bytes, bytes) else None
        return None
      # Non-blocking pop - returns list of (item, score) tuples
      z_result = cast("list[tuple[bytes, float]]", self.client.zpopmax(queue_name))
      if z_result and len(z_result) > 0:
        item_bytes = z_result[0][0]
        return item_bytes if isinstance(item_bytes, bytes) else None
      return None  # noqa: TRY300
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

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    try:
      return cast("int", self.client.zcard(queue_name))
    except RedisError:
      return 0

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
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

    Raises:
        ValueError: If set_name contains invalid characters.
    """
    _validate_key_name(set_name, "set_name")
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

    Raises:
        ValueError: If set_name contains invalid characters.
    """
    _validate_key_name(set_name, "set_name")
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

    Raises:
        ValueError: If set_name contains invalid characters.
    """
    _validate_key_name(set_name, "set_name")
    try:
      result = cast("int", self.client.sismember(set_name, item.decode("utf-8")))
      return bool(result)
    except RedisError:
      return False

  def set_len(self, set_name: str) -> int:
    """Get set size.

    Args:
        set_name: Name of the set.

    Returns:
        Number of items in the set.

    Raises:
        ValueError: If set_name contains invalid characters.
    """
    _validate_key_name(set_name, "set_name")
    try:
      return cast("int", self.client.scard(set_name))
    except RedisError:
      return 0

  def clear_set(self, set_name: str) -> None:
    """Clear all items from set.

    Args:
        set_name: Name of the set.

    Raises:
        ValueError: If set_name contains invalid characters.
    """
    _validate_key_name(set_name, "set_name")
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

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      if ttl is not None:
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

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      result = self.client.get(key)
      if result is None:
        return None
      if isinstance(result, bytes):
        return result
      # redis-py may return str for string values in some modes
      return str(result).encode()
    except RedisError:
      return None

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if deleted, False if didn't exist.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
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

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
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

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      result = cast("int", self.client.ttl(key))
      # redis-py ttl() returns int: -2 = no key, -1 = no TTL, >= 0 = TTL seconds
      if result == -2:
        return -1  # Key doesn't exist (distinguish from no TTL)
      if result == -1:
        return None  # No TTL set
      return result  # noqa: TRY300
    except RedisError:
      return None

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    In cluster mode, this scans all nodes.

    Args:
        prefix: If provided, only clear keys starting with this prefix.
               If None, clear all storage data.

    Raises:
        ValueError: If prefix contains invalid characters.
    """
    if prefix:
        _validate_key_name(prefix, "prefix")
    try:
      if prefix:
        # Use scan + delete for prefixed keys
        pattern = f"{prefix}*"
        if isinstance(self._client, RedisCluster):
          # For cluster, use the cluster's scan_iter which handles all nodes
          for key in self._client.scan_iter(match=pattern):
            self.client.delete(key)
        else:
          for key in self.client.scan_iter(match=pattern):
            self.client.delete(key)
      # Clear all keys in the current database
      elif isinstance(self._client, RedisCluster):
        # For cluster, flush all nodes
        self._client.flushall()
      else:
        self.client.flushdb()
    except RedisError as e:
      logger.warning("Failed to clear storage: %s", e)
