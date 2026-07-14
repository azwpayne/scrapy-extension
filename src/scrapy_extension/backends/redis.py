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
import uuid
from typing import TYPE_CHECKING, Any, cast

try:
    from redis import Redis
    from redis.cluster import ClusterNode, RedisCluster
    from redis.exceptions import RedisError
    from redis.sentinel import Sentinel
except ImportError as e:
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
      self._master_client.ping()
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
      self._client.ping()
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
  def _payload_key(self, queue_name: str) -> str:
    """Return the hash key used to store payloads for a queue.

    The key uses a Redis Cluster hash tag (`{queue_name}`) so that the
    ZSET, the payload hash, and the counter all land in the same cluster
    slot — required for Lua scripts and DELETE across all keys.

    Args:
        queue_name: Name of the queue.

    Returns:
        The Redis key for the payload hash, with hash tag.
    """
    return f"{{{queue_name}}}:payload"

  def _counter_key(self, queue_name: str) -> str:
    """Return the INCR counter key used to FIFO-order same-priority items.

    Args:
        queue_name: Name of the queue.

    Returns:
        The Redis key for the monotonic counter, with hash tag.
    """
    return f"{{{queue_name}}}:counter"

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
    payload_key = self._payload_key(queue_name)
    counter_key = self._counter_key(queue_name)
    try:
      self._push_script(
        keys=[queue_name, payload_key, counter_key],
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

    Non-blocking path (``timeout=0``) runs ZPOPMIN + HGET + HDEL inside a
    single Lua ``EVAL`` — fully atomic, no orphan window between ZSET
    removal and payload consumption. This is the path Scrapy's scheduler
    uses on every tick.

    Blocking path (``timeout>0``) cannot use Lua (Redis forbids blocking
    commands inside scripts); falls back to ``bzpopmin`` followed by an
    atomic MULTI/EXEC for HGET+HDEL.

    Three outcomes are distinguished by the pop path, so a recoverable
    race is never escalated to a hard error:

    - **Empty queue**: the ZSET had no member. Returns ``None`` (no error).
    - **Lost-payload race**: a ZSET member was popped but its payload was
      already consumed by a concurrent consumer (the loser of a
      ``ZPOPMIN`` race finds the payload ``HDEL``'d before its ``HGET``).
      This is an item-consumed-elsewhere race, not corruption — DEBUG-log
      and return ``None`` so the scheduler simply retries on the next tick.
    - **Structural corruption**: the Lua script surfaced a payload whose
      type cannot be normalized to bytes (an invariant violation).
      Raises ``QueueError`` so the caller can surface it.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (0 = non-blocking).

    Returns:
        The popped item, or None if the queue is empty or the item was
        consumed by a concurrent consumer.

    Raises:
        QueueError: If the pop fails, or on structural corruption.
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    payload_key = self._payload_key(queue_name)
    try:
      if timeout > 0:
        bz_result = self.client.bzpopmin(queue_name, timeout=timeout)
        if bz_result is None:
          return None
        member = cast("tuple[Any, Any]", bz_result)[1]
        return self._consume_payload(payload_key, member)
      result = self._pop_script(keys=[queue_name, payload_key])
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
    #   {2, _}             lost-payload race    -> DEBUG log, None
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
      # Recoverable concurrent-consumer race: another worker already
      # consumed this item's payload. Not corruption — retry next tick.
      logger.debug(
        "Lost-payload race on %s: ZSET member popped but payload already "
        "consumed by a concurrent consumer. Returning None (no error).",
        queue_name,
      )
      return None
    # status == 3 (or any unexpected status) is structural corruption.
    msg = (
      f"Structural corruption on queue {queue_name}: {detail}. The ZSET "
      f"and payload hash are in an unexpected state."
    )
    raise QueueError(msg, queue_name=queue_name, operation="pop")

  def _consume_payload(self, payload_key: str, member: str | bytes) -> bytes:
    """Atomically fetch and delete a payload from the sidecar hash.

    Uses MULTI/EXEC so the HGET and HDEL commit together. The hash tag
    on ``payload_key`` keeps this transaction valid under Redis Cluster.

    Args:
        payload_key: Redis key of the payload hash.
        member: The ZSET member (uuid str or bytes).

    Returns:
        The stored payload bytes.

    Raises:
        QueueError: If the payload is missing — the ZSET referenced a
            member that has no payload, indicating queue corruption.
    """
    try:
      pipe = self.client.pipeline(transaction=True)
      field = cast("str", member)
      pipe.hget(payload_key, field)
      pipe.hdel(payload_key, field)
      payload, _ = pipe.execute()
    except RedisError as e:
      msg = f"Failed to consume payload from {payload_key}: {e}"
      raise QueueError(
        msg,
        queue_name=payload_key,
        operation="pop",
      ) from e
    if payload is None:
      msg = (
        f"Queue corruption: ZSET member {member!r} has no payload in "
        f"{payload_key}. The ZSET and payload hash are out of sync."
      )
      raise QueueError(
        msg,
        queue_name=payload_key,
        operation="pop",
      )
    # decode_responses=True returns str; normalize to bytes for the queue contract.
    if isinstance(payload, str):
      return payload.encode("utf-8")
    if isinstance(payload, (bytes, bytearray)):
      return bytes(payload)
    msg = f"Unexpected payload type from HGET: {type(payload).__name__}"
    raise QueueError(msg, queue_name=payload_key, operation="pop")

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
      return cast("int", self.client.zcard(queue_name))  # type: ignore[redundant-cast]
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
    payload_key = self._payload_key(queue_name)
    counter_key = self._counter_key(queue_name)
    try:
      self.client.delete(queue_name, payload_key, counter_key)
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
      return self.client.sadd(set_name, item) == 1
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
    """
    _validate_key_name(set_name, "set_name")
    return self.client.srem(set_name, item) == 1

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
    result = self.client.sismember(set_name, cast("str", item))
    return bool(result)

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
      return cast("int", self.client.scard(set_name))  # type: ignore[redundant-cast]
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
        StorageError: If the Redis write fails (R-store). Was previously
            silently swallowed (warn + normal return), masking data loss in
            the item pipeline; mirrors the mongodb/elasticsearch/memcached/
            dynamodb ``store()`` contracts (all raise ``StorageError``).
    """
    _validate_key_name(key, "key")
    try:
      if ttl is not None:
        self.client.setex(key, ttl, data)
      else:
        self.client.set(key, data)
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
    result = self.client.get(key)
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
    """
    _validate_key_name(key, "key")
    return self.client.delete(key) == 1

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
    return self.client.exists(key) == 1

  def ttl(self, key: str) -> int | None:
    """Get remaining time-to-live.

    Args:
        key: Storage key.

    Returns:
        Seconds remaining, None if no TTL or key doesn't exist,
        -1 if expired (rare since Redis auto-evicts expired keys).

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    result = cast("int", self.client.ttl(key))  # type: ignore[redundant-cast]
    # redis-py ttl() returns int: -2 = no key, -1 = no TTL, >= 0 = TTL seconds.
    # Per StorageBackend contract, both "missing key" and "no TTL" return None.
    if result < 0:
      return None
    return result

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
