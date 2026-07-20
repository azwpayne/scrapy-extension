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
import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
    from redis import Redis
    from redis.cluster import ClusterNode, RedisCluster
    from redis.exceptions import RedisError
    from redis.sentinel import Sentinel
except ImportError as e:
    if not _is_missing_optional_dependency(e, "redis"):
        raise
    raise ImportError(
        "Redis backend requires 'redis'. Install with: pip install scrapy-extension[redis]"
    ) from e

from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
    _validate_key_name,
    _validate_ttl,
    secret_value,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
    StorageError,
)
from scrapy_extension.settings import RedisMode

if TYPE_CHECKING:
  from scrapy_extension.settings import RedisSettings

logger = logging.getLogger(__name__)

_BLOCKING_POP_POLL_INTERVAL = 0.05

_POP_LUA = """
local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then return {0, false} end
local member = popped[1]
local payload = redis.call('HGET', KEYS[2], member)
redis.call('HDEL', KEYS[2], member)
if not payload then return {2, false} end
if type(payload) ~= 'string' then
  return {3, 'unexpected payload type: ' .. type(payload)}
end
return {1, payload}
"""

_PUSH_LUA = """
local counter = redis.call('INCR', KEYS[3])
local member = string.format('%020d:%s', counter, ARGV[1])
redis.call('ZADD', KEYS[1], ARGV[2], member)
redis.call('HSET', KEYS[2], member, ARGV[3])
return member
"""


class RedisBackend(Backend, QueueBackend, SetBackend, StorageBackend):
  """Redis backend implementation with multimode support.

  Implements all backend interfaces using Redis data structures:
  - Queue: Redis Sorted Sets (ZADD/ZRANGEBYSCORE/ZREM)
  - Set: Redis Sets (SADD/SREM/SISMEMBER/SCARD/DEL)
  - Storage: Redis Strings with TTL (SET/GET/DEL/EXISTS/TTL)

  Logical names are mapped into disjoint physical domains below the configured
  namespace. For example, logical name ``crawl`` maps to separate
  ``<namespace>:queue:*``, ``<namespace>:set:crawl``, and
  ``<namespace>:storage:crawl`` keys. This prevents Redis WRONGTYPE failures
  and cross-interface deletion when one backend instance serves all roles.

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
      logger.debug("Connected to Redis in %s mode", self.config.mode.value)
    except (BackendConnectionError, ConfigurationError):
      self.disconnect()
      raise
    except Exception as e:
      self.disconnect()
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
      password=secret_value(self.config.password),
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
    if not self._client.ping():
      raise BackendConnectionError(
        "Redis health check returned false during connect",
        backend_type="redis",
      )

  def _connect_master_slave(self) -> None:
    """Connect to master-slave setup.

    Creates connection to master for writes. If replicas are configured,
    they can be used for read operations.

    Raises:
        ConfigurationError: If replicas are not configured.
    """
    self._master_client = self._create_redis_client()
    if not self._master_client.ping():
      raise BackendConnectionError(
        "Redis master health check returned false during connect",
        backend_type="redis",
      )
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

    # Parse sentinel addresses. SEC-6: wrap malformed entries (raw ValueError
    # from ``int(port_str)`` or a missing ":port" suffix) as BackendConnectionError
    # so callers see a backend-typed error instead of an unhandled ValueError.
    sentinel_tuples: list[tuple[str, int]] = []
    try:
      for sentinel_str in self.config.sentinels:
        host, port_str = sentinel_str.rsplit(":", 1)
        sentinel_tuples.append((host, int(port_str)))

      # Create Sentinel connection
      sentinel_kwargs: dict[str, Any] = {}
      if self.config.sentinel_password:
        sentinel_kwargs["password"] = secret_value(self.config.sentinel_password)
      if self.config.sentinel_username:
        sentinel_kwargs["username"] = self.config.sentinel_username

      # redis.sentinel.Sentinel ships py.typed but no inline annotations on
      # __init__/master_for/ClusterNode — these are genuine untyped-third-party
      # call sites (verified: Sentinel.__init__ has no signature annotations).
      self._sentinel = Sentinel(  # type: ignore[no-untyped-call]
        sentinel_tuples,
        socket_timeout=self.config.socket_timeout,
        socket_connect_timeout=self.config.socket_connect_timeout,
        retry_on_timeout=self.config.sentinel_retry_on_timeout,
        min_other_sentinels=self.config.min_other_sentinels,
        sentinel_kwargs=sentinel_kwargs or None,
      )

      # Get master connection through sentinel
      self._master_client = self._sentinel.master_for(  # type: ignore[no-untyped-call]
        self.config.sentinel_master_name,
        db=self.config.db,
        password=secret_value(self.config.password),
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

      # Verify connection. SEC-6: a connection failure here (bad master name,
      # unreachable sentinels) must surface as BackendConnectionError, not the
      # raw redis-py exception type that varies across versions.
      if not self._master_client.ping():
        raise BackendConnectionError(
          "Redis Sentinel master health check returned false during connect",
          backend_type="redis",
        )
    except BackendConnectionError:
      raise
    except Exception as e:
      msg = f"Failed to connect via Redis Sentinel: {e}"
      raise BackendConnectionError(msg, backend_type="redis") from e
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

    # SEC-6: wrap malformed startup-node entries and cluster connection /
    # ping failures in BackendConnectionError (matches the Sentinel path).
    nodes: list[ClusterNode] = []
    try:
      for node_str in startup_nodes:
        host, port_str = node_str.rsplit(":", 1)
        nodes.append(ClusterNode(host=host, port=int(port_str)))  # type: ignore[no-untyped-call]

      self._client = RedisCluster(
        startup_nodes=nodes,
        password=secret_value(self.config.password),
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
      if not self._client.ping():
        raise BackendConnectionError(
          "Redis Cluster health check returned false during connect",
          backend_type="redis",
        )
    except BackendConnectionError:
      raise
    except Exception as e:
      msg = f"Failed to connect to Redis Cluster: {e}"
      raise BackendConnectionError(msg, backend_type="redis") from e
    logger.debug("Connected to Redis Cluster with %d startup nodes", len(nodes))

  def disconnect(self) -> None:
    """Close Redis connection.

    Closes the connection pool and releases resources.
    """
    clients = {
      id(client): client
      for client in (self._master_client, self._client)
      if client is not None
    }
    self._master_client = None
    self._client = None
    self._sentinel = None
    for client in clients.values():
      with contextlib.suppress(Exception):
        client.close()

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

  @property
  def _pop_script(self) -> Any:
    """Compiled Lua script for atomic non-blocking pop.

    Re-registered on every call (cheap: just object construction, no network
    I/O). The script body is cached server-side via EVALSHA after the first
    EVAL. Avoids stale references if ``self._client`` is replaced by a
    reconnect.
    """
    # Cast to ``Redis`` (not the runtime-accurate ``Redis | RedisCluster``)
    # because redis-py's stubs type ``register_script`` as a method on
    # ``Redis`` only, even though ``RedisCluster`` also exposes it at runtime.
    # Widening the cast to the union reintroduces mypy ``[misc]`` errors
    # (RedisCluster not in the stub's ``register_script`` signature) — the
    # narrower cast is the pragmatic, type-ignore-free choice here.
    return cast("Redis", self.client).register_script(_POP_LUA)

  @property
  def _push_script(self) -> Any:
    """Compiled Lua script for atomic FIFO-preserving push."""
    # See ``_pop_script`` for why this casts to ``Redis`` rather than the
    # ``Redis | RedisCluster`` union (redis-py stub limitation; the method
    # exists on RedisCluster at runtime).
    return cast("Redis", self.client).register_script(_PUSH_LUA)

  # QueueBackend implementation using Sorted Sets
  def _queue_key(self, queue_name: str) -> str:
    """Return the namespaced ZSET key for a logical queue.

    The complete queue identity is placed in a Redis Cluster hash tag. The
    item ZSET, payload hash, and FIFO counter therefore remain in one slot for
    Lua and transactional operations while other backend domains stay
    physically disjoint.
    """
    return f"{{{self.config.namespace}:queue:{queue_name}}}:items"

  def _payload_key(self, queue_name: str) -> str:
    """Return the hash key used to store payloads for a queue.

    The key uses a Redis Cluster hash tag containing namespace, domain, and
    logical queue name so the ZSET, payload hash, and counter all land in the
    same cluster slot — required for Lua scripts and DELETE across all keys.

    Args:
        queue_name: Name of the queue.

    Returns:
        The Redis key for the payload hash, with hash tag.
    """
    return f"{{{self.config.namespace}:queue:{queue_name}}}:payload"

  def _counter_key(self, queue_name: str) -> str:
    """Return the INCR counter key used to FIFO-order same-priority items.

    Args:
        queue_name: Name of the queue.

    Returns:
        The Redis key for the monotonic counter, with hash tag.
    """
    return f"{{{self.config.namespace}:queue:{queue_name}}}:counter"

  def _set_key(self, set_name: str) -> str:
    """Return the namespaced physical key for a logical set."""
    return f"{self.config.namespace}:set:{set_name}"

  def _storage_key(self, key: str) -> str:
    """Return the namespaced physical key for a logical storage key."""
    return f"{self.config.namespace}:storage:{key}"

  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push item to priority queue.

    Runs INCR + ZADD + HSET inside a single Lua ``EVAL``. The INCR counter
    prefixes the ZSET member (``{counter:020d}:{uuid}``) so that items with
    the same priority score pop in insertion order (FIFO) — without it,
    same-score items would pop in random uuid order. Hash-tagged keys
    ensure cluster slot affinity so the script is atomic on the owning
    shard.

    Args:
        queue_name: Name of the queue.
        item: Item to push (bytes).
        priority: Priority value (higher = more urgent).

    Raises:
        QueueError: If the push operation fails.
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    member_uuid = uuid.uuid4().hex
    queue_key = self._queue_key(queue_name)
    payload_key = self._payload_key(queue_name)
    counter_key = self._counter_key(queue_name)
    try:
      self._push_script(
        keys=[queue_key, payload_key, counter_key],
        args=[member_uuid, -priority, item],
      )
    except RedisError as e:
      msg = f"Failed to push to queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="push",
      ) from e

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop highest priority item from queue.

    Every attempt runs ZPOPMIN + HGET + HDEL inside a single Lua ``EVAL``.
    This is fully atomic, with no crash window between ZSET removal and
    payload consumption. For ``timeout>0``, the same atomic attempt is polled
    against a monotonic deadline; using ``BZPOPMIN`` would remove the member
    before a separate payload read and could make the message unreachable if
    the worker exited between those operations.

    Three outcomes are distinguished by the pop path:

    - **Empty queue**: the ZSET had no member. Returns ``None`` (no error).
    - **Orphaned member**: the ZSET referenced a missing sidecar payload.
      The atomic script removes the stale member and returns ``None`` so a
      blocking call can continue polling for a valid item.
    - **Structural corruption**: the Lua script surfaced a payload whose
      type cannot be normalized to bytes (an invariant violation).
      Raises ``QueueError`` so the caller can surface it.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (0 = non-blocking).

    Returns:
        The popped item, or None if the queue is empty or an orphaned member
        was discarded and no valid item was available before the deadline.

    Raises:
        QueueError: If the pop fails, or on structural corruption.
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    queue_key = self._queue_key(queue_name)
    payload_key = self._payload_key(queue_name)

    if timeout <= 0:
      return self._atomic_pop_once(queue_name, queue_key, payload_key)

    deadline = time.monotonic() + timeout
    while True:
      item = self._atomic_pop_once(queue_name, queue_key, payload_key)
      if item is not None:
        return item
      remaining = deadline - time.monotonic()
      if remaining <= 0:
        return None
      time.sleep(min(_BLOCKING_POP_POLL_INTERVAL, remaining))

  def _atomic_pop_once(
    self, queue_name: str, queue_key: str, payload_key: str
  ) -> bytes | None:
    """Atomically remove one queue member and its sidecar payload."""
    try:
      result = self._pop_script(keys=[queue_key, payload_key])
    except RedisError as e:
      msg = f"Failed to pop from queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      ) from e
    # The Lua script returns a 2-element table {status, payload_or_errmsg}
    # so a real payload can never collide with a control signal:
    #   {0, _}             empty queue          -> None
    #   {1, payload}       success              -> bytes(payload)
    #   {2, _}             orphaned member      -> DEBUG log, None
    #   {3, errmsg}        structural corruption -> QueueError
    # redis-py decodes Lua tables as Python lists. A legacy / unexpected
    # non-list result is treated as corruption (defensive — the script
    # always returns a list now, but a future edit could regress).
    if not isinstance(result, list) or len(result) < 2:
      msg = (
        f"Corrupt pop result from {queue_name}: expected a 2-element "
        f"[status, payload] list, got {type(result).__name__}: {result!r}"
      )
      raise QueueError(msg, queue_name=queue_name, operation="pop")
    status = result[0]
    detail = result[1]
    if status == 0:
      return None
    if status == 1:
      # decode_responses=True returns str; normalize to bytes for the queue contract.
      if isinstance(detail, str):
        return detail.encode("utf-8")
      if isinstance(detail, (bytes, bytearray)):
        return bytes(detail)
      msg = (
        f"Corrupt payload from {queue_name}: expected bytes/str but got "
        f"{type(detail).__name__}"
      )
      raise QueueError(msg, queue_name=queue_name, operation="pop")
    if status == 2:
      # A stale ZSET member without its sidecar payload can be discarded. The
      # script removed it atomically; the next blocking poll can make progress.
      logger.debug(
        "Orphaned member on %s: ZSET member had no sidecar payload. "
        "Discarding the stale member and returning None.",
        queue_name,
      )
      return None
    # status == 3 (or any unexpected status) is structural corruption.
    msg = (
      f"Structural corruption on queue {queue_name}: {detail}. The ZSET "
      f"and payload hash are in an unexpected state."
    )
    raise QueueError(msg, queue_name=queue_name, operation="pop")

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
      # redis-py's shared sync/async stubs type zcard() as ResponseT
      # (Awaitable[Any] | int); the sync client returns int at runtime.
      # The cast narrows for Pyright; harmless under mypy.
      return cast(  # type: ignore[redundant-cast]
        "int", self.client.zcard(self._queue_key(queue_name))
      )
    except RedisError as e:
      # R-qlen: do NOT swallow to 0 — that conflates an empty queue with a
      # backend failure. The scheduler trusts ``len(queue)`` for
      # ``has_pending_requests`` and the backpressure gate; a swallowed 0
      # during a Redis blip can trigger premature idle/CloseSpider and loses
      # the backpressure signal at the worst moment. Wrap as QueueError
      # (matching pop()'s contract); the scheduler's next_request already
      # handles QueueError from ``len(self._queue)`` (returns None safely).
      raise QueueError(
        str(e), queue_name=queue_name, operation="queue_len"
      ) from e

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Removes the ZSET (priority queue), its sidecar payload hash, and the
    FIFO counter. All three keys share a cluster slot via hash tags.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
        QueueError: If the delete fails at the Redis layer.
    """
    _validate_key_name(queue_name, "queue_name")
    queue_key = self._queue_key(queue_name)
    payload_key = self._payload_key(queue_name)
    counter_key = self._counter_key(queue_name)
    try:
      self.client.delete(queue_key, payload_key, counter_key)
    except RedisError as e:
      msg = f"Failed to clear queue {queue_name}: {e}"
      raise QueueError(msg, queue_name=queue_name, operation="clear_queue") from e

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
      return self.client.sadd(self._set_key(set_name), item) == 1
    except RedisError as e:
      # R-dupe-1 (option b): wrap the raw RedisError so BackendDupeFilter's
      # ``except BackendConnectionError`` graceful-degradation arm fires
      # (degrade to not-seen) instead of propagating and crashing the crawl.
      raise BackendConnectionError(
        f"Redis set add failed for {set_name!r}: {e}", backend_type="redis"
      ) from e

  def remove(self, set_name: str, item: bytes) -> bool:
    """Remove item from set.

    Args:
        set_name: Name of the set.
        item: Item to remove.

    Returns:
        True if removed, False if didn't exist.

    Raises:
        ValueError: If set_name contains invalid characters.
        BackendConnectionError: If Redis cannot remove the item.
    """
    _validate_key_name(set_name, "set_name")
    try:
      return self.client.srem(self._set_key(set_name), item) == 1
    except RedisError as e:
      raise BackendConnectionError(
        f"Redis set remove failed for {set_name!r}: {e}", backend_type="redis"
      ) from e

  def contains(self, set_name: str, item: bytes) -> bool:
    """Check if item is in set.

    Args:
        set_name: Name of the set.
        item: Item to check.

    Returns:
        True if item exists in the set.

    Raises:
        ValueError: If set_name contains invalid characters.
        BackendConnectionError: If Redis cannot check membership.
    """
    _validate_key_name(set_name, "set_name")
    try:
      result = self.client.sismember(self._set_key(set_name), cast("str", item))
    except RedisError as e:
      raise BackendConnectionError(
        f"Redis set contains failed for {set_name!r}: {e}", backend_type="redis"
      ) from e
    return bool(result)

  def set_len(self, set_name: str) -> int:
    """Get set size.

    Args:
        set_name: Name of the set.

    Returns:
        Number of items in the set.

    Raises:
        ValueError: If set_name contains invalid characters.
        BackendConnectionError: If Redis cannot read the set size.
    """
    _validate_key_name(set_name, "set_name")
    try:
      return cast(  # type: ignore[redundant-cast]
        "int", self.client.scard(self._set_key(set_name))
      )
    except RedisError as e:
      raise BackendConnectionError(
        f"Redis set length failed for {set_name!r}: {e}", backend_type="redis"
      ) from e

  def clear_set(self, set_name: str) -> None:
    """Clear all items from set.

    Args:
        set_name: Name of the set.

    Raises:
        ValueError: If set_name contains invalid characters.
        BackendConnectionError: If the delete fails at the Redis layer (parity
            with add, R-dupe-1 #38).
    """
    _validate_key_name(set_name, "set_name")
    try:
      self.client.delete(self._set_key(set_name))
    except RedisError as e:
      # R-rclears: wrap (parity with add R-dupe-1 #38) so BackendDupeFilter's
      # graceful-degradation arm fires; a swallowed clear hides a failed dedup
      # reset -> stale fingerprints -> duplicate requests.
      raise BackendConnectionError(
        f"Redis set clear failed for {set_name!r}: {e}", backend_type="redis"
      ) from e

  # StorageBackend implementation using Redis Strings
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store data with key.

    Args:
        key: Storage key.
        data: Data to store (bytes).
        ttl: Optional time-to-live in seconds.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the Redis write fails (R-store). Was previously
            silently swallowed (warn + normal return), masking data loss in
            the item pipeline; mirrors the mongodb/elasticsearch/memcached/
            dynamodb ``store()`` contracts (all raise ``StorageError``).
    """
    _validate_key_name(key, "key")
    _validate_ttl(ttl)
    storage_key = self._storage_key(key)
    stored: bool | str | bytes | None
    try:
      if ttl is not None:
        stored = self.client.setex(storage_key, ttl, data)
      else:
        stored = self.client.set(storage_key, data)
    except RedisError as e:
      # R-store: do NOT swallow. Pre-fix this logged a warning and returned
      # normally, so BackendPipeline.process_item's SUCCESS arm ran -- the
      # item was silently dropped, ``pipeline/storage_errors`` was never
      # incremented, and the C2 ``max_storage_errors`` escalation was neutered
      # (the success path resets the consecutive-error counter). Raising lets
      # the pipeline's ``except Exception`` arm count the failure and escalate.
      # Matches the other four storage backends; ``retrieve()`` already
      # propagates (R32-A1). See test_storage_store_error.
      raise StorageError(
        f"Failed to store key {key!r} in Redis: {e}",
        operation="store",
        key=key,
      ) from e
    if stored is False or stored is None:
      raise StorageError(
        f"Redis rejected the write for key {key!r}",
        operation="store",
        key=key,
      )

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key.

    Args:
        key: Storage key.

    Returns:
        Stored data, or None if not found.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the Redis read fails.
    """
    _validate_key_name(key, "key")
    try:
      result = self.client.get(self._storage_key(key))
    except RedisError as e:
      msg = f"Failed to retrieve key {key!r} from Redis: {e}"
      raise StorageError(msg, operation="retrieve", key=key) from e
    if result is None:
      return None
    if isinstance(result, bytes):
      return result
    # redis-py may return str for string values in some modes
    return str(result).encode()

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if deleted, False if didn't exist.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the Redis delete fails.
    """
    _validate_key_name(key, "key")
    try:
      return self.client.delete(self._storage_key(key)) == 1
    except RedisError as e:
      msg = f"Failed to delete key {key!r} from Redis: {e}"
      raise StorageError(msg, operation="delete", key=key) from e

  def exists(self, key: str) -> bool:
    """Check if key exists.

    Args:
        key: Storage key.

    Returns:
        True if key exists.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the Redis existence check fails.
    """
    _validate_key_name(key, "key")
    try:
      return self.client.exists(self._storage_key(key)) == 1
    except RedisError as e:
      msg = f"Failed to check existence of key {key!r} in Redis: {e}"
      raise StorageError(msg, operation="exists", key=key) from e

  def ttl(self, key: str) -> int | None:
    """Get remaining time-to-live.

    Args:
        key: Storage key.

    Returns:
        Non-negative seconds remaining, or None if absent, permanent, or expired.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: If the Redis TTL query fails.
    """
    _validate_key_name(key, "key")
    try:
      result = cast(  # type: ignore[redundant-cast]
        "int", self.client.ttl(self._storage_key(key))
      )
    except RedisError as e:
      msg = f"Failed to read TTL of key {key!r} in Redis: {e}"
      raise StorageError(msg, operation="ttl", key=key) from e
    # redis-py ttl() returns int: -2 = no key, -1 = no TTL, >= 0 = TTL seconds.
    # Per StorageBackend contract, both "missing key" and "no TTL" return None.
    if result < 0:
      return None
    return result

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    This always scans only the configured namespace's storage domain. It never
    uses ``FLUSHDB``/``FLUSHALL``, because a Redis database may be shared with
    queues, deduplication sets, or unrelated applications. In cluster mode,
    redis-py's cluster ``scan_iter`` scans all nodes.

    Args:
        prefix: If provided, only clear logical storage keys starting with this
               prefix. If None, clear all storage keys owned by this namespace.

    Raises:
        ValueError: If prefix contains invalid characters.
        StorageError: If the clear fails at the Redis layer (parity with store
            R-store #59 and mongodb/memcached/dynamodb clear_storage).
    """
    if prefix is not None:
      _validate_key_name(prefix, "prefix")
    logical_prefix = prefix or ""
    pattern = f"{self._storage_key(logical_prefix)}*"
    try:
      for physical_key in self.client.scan_iter(match=pattern):
        self.client.delete(physical_key)
    except RedisError as e:
      raise StorageError(
        f"Failed to clear Redis storage: {e}", operation="clear_storage"
      ) from e
