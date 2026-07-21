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
import math
import threading
import time
import uuid
import warnings
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, cast

from pydantic import TypeAdapter, ValidationError

from scrapy_extension.backends._optional import _is_missing_optional_dependency
from scrapy_extension.backends._redaction import _redact

try:
  from redis import Redis
  from redis.backoff import NoBackoff
  from redis.cluster import ClusterNode, RedisCluster
  from redis.exceptions import RedisError
  from redis.exceptions import TimeoutError as RedisTimeoutError
  from redis.retry import Retry
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
from scrapy_extension.settings.redis import RedisSettings

logger = logging.getLogger(__name__)

_REDIS_FIELD_ADAPTERS: dict[str, TypeAdapter[Any]] = {
  name: TypeAdapter(field.rebuild_annotation())
  for name, field in RedisSettings.model_fields.items()
}

_BLOCKING_POP_POLL_INTERVAL = 0.05
_SENTINEL_CONTROL_RETRIES = 1

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


def _normalize_pop_timeout(timeout: float) -> float:
  """Return a finite, non-negative timeout before any Redis I/O."""
  if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
    raise ValueError(f"timeout must be a finite non-negative number, got {timeout!r}")
  try:
    normalized = float(timeout)
  except (OverflowError, TypeError, ValueError) as exc:
    raise ValueError(
      f"timeout must be a finite non-negative number, got {timeout!r}"
    ) from exc
  if not math.isfinite(normalized) or normalized < 0:
    raise ValueError(f"timeout must be a finite non-negative number, got {timeout!r}")
  return normalized


def _new_no_replay_retry() -> Retry:
  """Return a fresh SDK policy that cleans up failures without replaying."""
  return Retry(backoff=NoBackoff(), retries=0)


def _new_sentinel_control_retry(enabled: bool) -> Retry:
  """Return the timeout-only retry policy for Sentinel control requests."""
  retries = _SENTINEL_CONTROL_RETRIES if enabled else 0
  return Retry(
    backoff=NoBackoff(),
    retries=retries,
    supported_errors=(RedisTimeoutError, TimeoutError),
  )


class _RedisConnectCancelled(Exception):
  """Internal signal for a candidate fenced by lifecycle teardown."""


@dataclass(frozen=True, slots=True)
class _RedisConnectionSnapshot:
  """Validated non-secret values fixed for one Redis generation."""

  mode: RedisMode
  namespace: str
  host: str
  port: int
  db: int
  username: str | None
  socket_timeout: float | None
  socket_connect_timeout: float | None
  max_connections: int | None
  decode_responses: bool
  replicas: tuple[str, ...]
  sentinel_nodes: tuple[tuple[str, int], ...]
  sentinel_master_name: str
  sentinel_username: str | None
  min_other_sentinels: int
  sentinel_retry_on_timeout: bool
  cluster_nodes: tuple[tuple[str, int], ...]
  cluster_skip_full_coverage_check: bool
  cluster_max_redirects: int
  ssl_enabled: bool
  ssl_cafile: str | None
  ssl_certfile: str | None
  ssl_keyfile: str | None
  ssl_check_hostname: bool


@dataclass(slots=True, eq=False)
class _RedisGeneration:
  """One atomically published Redis client set and immutable snapshot."""

  key: object
  client: Redis | RedisCluster
  master_client: Redis | None
  sentinel: Sentinel | None
  snapshot: _RedisConnectionSnapshot
  accepting: bool = True
  active_leases: int = 0
  retired: threading.Event = field(default_factory=threading.Event)


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
    if "retry_on_timeout" in config.model_fields_set:
      warnings.warn(
        (
          "RedisSettings.retry_on_timeout / "
          "SCRAPY_REDIS_RETRY_ON_TIMEOUT is deprecated and ignored; Redis "
          "data-plane SDK retries are disabled to prevent outcome-ambiguous "
          "command replay."
        ),
        FutureWarning,
        # Keep attribution on this static library line. Python's default
        # warning renderer prints the attributed source line; pointing at a
        # caller that constructs RedisSettings inline could disclose literals.
        stacklevel=1,
      )
    self._connect_lock = threading.Lock()
    self._connect_local = threading.local()
    self._generation_condition = threading.Condition()
    self._lease_local = threading.local()
    self._lifecycle_epoch = 0
    self._disconnecting = False
    self._disconnect_owner: int | None = None
    self._generation: _RedisGeneration | None = None
    # Compatibility/diagnostic mirrors. Internal operations use only the
    # authoritative generation captured by a lease.
    self._client: Redis | RedisCluster | None = None
    self._master_client: Redis | None = None
    self._sentinel: Sentinel | None = None

  @staticmethod
  def _parse_nodes(
    values: tuple[str, ...], *, setting_name: str
  ) -> tuple[tuple[str, int], ...]:
    """Parse configured ``host:port`` values without exposing raw endpoints."""
    nodes: list[tuple[str, int]] = []
    try:
      for value in values:
        host, port_text = value.rsplit(":", 1)
        port = int(port_text)
        if not host or not 1 <= port <= 65535:
          raise ValueError
        nodes.append((host, port))
    except (TypeError, ValueError):
      raise BackendConnectionError(
        f"Invalid Redis {setting_name} address.", backend_type="redis"
      ) from None
    return tuple(nodes)

  def _capture_connection_plan(
    self,
  ) -> tuple[_RedisConnectionSnapshot, Any, Any]:
    """Copy and revalidate every value consumed by one connect attempt."""
    raw_values = self.config.__dict__.copy()
    canonical_values: dict[str, Any] = {}
    for setting_name, adapter in _REDIS_FIELD_ADAPTERS.items():
      try:
        canonical_values[setting_name] = adapter.validate_python(
          raw_values.get(setting_name), strict=True
        )
      except ValidationError:
        raise ConfigurationError(
          f"Invalid Redis setting '{setting_name}'.",
          setting_name=setting_name,
        ) from None
    try:
      validated = RedisSettings.model_validate(
        canonical_values, strict=True
      )
    except ConfigurationError as exc:
      setting_name = exc.setting_name or "redis"
      raise ConfigurationError(
        f"Invalid Redis setting '{setting_name}'.",
        setting_name=setting_name,
      ) from None
    except ValidationError as exc:
      errors = exc.errors()
      location = errors[0].get("loc", ()) if errors else ()
      setting_name = str(location[0]) if location else "redis"
      raise ConfigurationError(
        f"Invalid Redis setting '{setting_name}'.",
        setting_name=setting_name,
      ) from None

    sentinel_nodes: tuple[tuple[str, int], ...] = ()
    if validated.mode == RedisMode.SENTINEL:
      sentinel_nodes = self._parse_nodes(
        tuple(validated.sentinels), setting_name="sentinel"
      )
    cluster_nodes: tuple[tuple[str, int], ...] = ()
    if validated.mode == RedisMode.CLUSTER:
      configured_nodes = tuple(validated.cluster_startup_nodes)
      if not configured_nodes:
        configured_nodes = (f"{validated.host}:{validated.port}",)
      cluster_nodes = self._parse_nodes(
        configured_nodes, setting_name="cluster startup-node"
      )

    snapshot = _RedisConnectionSnapshot(
      mode=validated.mode,
      namespace=validated.namespace,
      host=validated.host,
      port=validated.port,
      db=validated.db,
      username=validated.username,
      socket_timeout=validated.socket_timeout,
      socket_connect_timeout=validated.socket_connect_timeout,
      max_connections=validated.max_connections,
      decode_responses=validated.decode_responses,
      replicas=tuple(validated.replicas),
      sentinel_nodes=sentinel_nodes,
      sentinel_master_name=validated.sentinel_master_name,
      sentinel_username=validated.sentinel_username,
      min_other_sentinels=validated.min_other_sentinels,
      sentinel_retry_on_timeout=validated.sentinel_retry_on_timeout,
      cluster_nodes=cluster_nodes,
      cluster_skip_full_coverage_check=(validated.cluster_skip_full_coverage_check),
      cluster_max_redirects=validated.cluster_max_redirects,
      ssl_enabled=validated.ssl_enabled,
      ssl_cafile=validated.ssl_cafile,
      ssl_certfile=validated.ssl_certfile,
      ssl_keyfile=validated.ssl_keyfile,
      ssl_check_hostname=validated.ssl_check_hostname,
    )
    return (
      snapshot,
      _redact(secret_value(validated.password)),
      _redact(secret_value(validated.sentinel_password)),
    )

  def _raise_if_connect_cancelled(self, request_epoch: int) -> None:
    """Stop a stale candidate before its next externally visible SDK step."""
    with self._generation_condition:
      if (
        request_epoch != self._lifecycle_epoch
        or self._disconnecting
        or self._generation is not None
      ):
        raise _RedisConnectCancelled

  @staticmethod
  def _base_client_kwargs(
    snapshot: _RedisConnectionSnapshot, password: Any
  ) -> dict[str, Any]:
    """Return shared SDK kwargs for one validated candidate."""
    return {
      "host": snapshot.host,
      "port": snapshot.port,
      "db": snapshot.db,
      "password": password,
      "username": snapshot.username,
      "socket_timeout": snapshot.socket_timeout,
      "socket_connect_timeout": snapshot.socket_connect_timeout,
      "retry": _new_no_replay_retry(),
      "max_connections": snapshot.max_connections,
      "decode_responses": snapshot.decode_responses,
      "ssl": snapshot.ssl_enabled,
      "ssl_ca_certs": snapshot.ssl_cafile,
      "ssl_certfile": snapshot.ssl_certfile,
      "ssl_keyfile": snapshot.ssl_keyfile,
      "ssl_check_hostname": snapshot.ssl_check_hostname,
    }

  @staticmethod
  def _close_handles(
    client: Redis | RedisCluster | None,
    master_client: Redis | None,
    sentinel: Sentinel | None,
  ) -> None:
    """Best-effort close every distinct data/control-plane resource owner."""
    handles: list[Any] = [client, master_client]
    if sentinel is not None:
      sentinel_close = getattr(sentinel, "close", None)
      if callable(sentinel_close):
        handles.append(sentinel)
      else:
        controls = getattr(sentinel, "sentinels", ())
        if isinstance(controls, (list, tuple)):
          handles.extend(controls)
    unique = {id(handle): handle for handle in handles if handle is not None}
    pending_interrupt: BaseException | None = None
    for handle in unique.values():
      try:
        with contextlib.suppress(Exception):
          handle.close()
      except BaseException as exc:
        if pending_interrupt is None:
          pending_interrupt = exc
    if pending_interrupt is not None:
      raise pending_interrupt

  def _build_and_publish_generation(
    self,
    snapshot: _RedisConnectionSnapshot,
    password: Any,
    sentinel_password: Any,
    request_epoch: int,
  ) -> _RedisGeneration:
    """Build privately, health-check, then atomically publish a candidate."""
    client: Redis | RedisCluster | None = None
    master_client: Redis | None = None
    sentinel: Sentinel | None = None
    published = False
    try:
      self._raise_if_connect_cancelled(request_epoch)
      if snapshot.mode in (RedisMode.STANDALONE, RedisMode.MASTER_SLAVE):
        client = Redis(**self._base_client_kwargs(snapshot, password))
        if snapshot.mode == RedisMode.MASTER_SLAVE:
          master_client = client
        self._raise_if_connect_cancelled(request_epoch)
      elif snapshot.mode == RedisMode.SENTINEL:
        sentinel_kwargs: dict[str, Any] = {
          "socket_timeout": snapshot.socket_timeout,
          "socket_connect_timeout": snapshot.socket_connect_timeout,
          "retry": _new_sentinel_control_retry(
            snapshot.sentinel_retry_on_timeout
          ),
          "max_connections": snapshot.max_connections,
          "ssl": snapshot.ssl_enabled,
          "ssl_ca_certs": snapshot.ssl_cafile,
          "ssl_certfile": snapshot.ssl_certfile,
          "ssl_keyfile": snapshot.ssl_keyfile,
          "ssl_check_hostname": snapshot.ssl_check_hostname,
        }
        if sentinel_password is not None:
          sentinel_kwargs["password"] = sentinel_password
        if snapshot.sentinel_username is not None:
          sentinel_kwargs["username"] = snapshot.sentinel_username
        sentinel = Sentinel(  # type: ignore[no-untyped-call]
          list(snapshot.sentinel_nodes),
          socket_timeout=snapshot.socket_timeout,
          socket_connect_timeout=snapshot.socket_connect_timeout,
          retry=_new_no_replay_retry(),
          max_connections=snapshot.max_connections,
          min_other_sentinels=snapshot.min_other_sentinels,
          sentinel_kwargs=sentinel_kwargs,
        )
        self._raise_if_connect_cancelled(request_epoch)
        master_client = sentinel.master_for(  # type: ignore[no-untyped-call]
          snapshot.sentinel_master_name,
          db=snapshot.db,
          password=password,
          username=snapshot.username,
          socket_timeout=snapshot.socket_timeout,
          socket_connect_timeout=snapshot.socket_connect_timeout,
          retry=_new_no_replay_retry(),
          max_connections=snapshot.max_connections,
          decode_responses=snapshot.decode_responses,
          ssl=snapshot.ssl_enabled,
          ssl_ca_certs=snapshot.ssl_cafile,
          ssl_certfile=snapshot.ssl_certfile,
          ssl_keyfile=snapshot.ssl_keyfile,
          ssl_check_hostname=snapshot.ssl_check_hostname,
        )
        client = master_client
        self._raise_if_connect_cancelled(request_epoch)
      elif snapshot.mode == RedisMode.CLUSTER:
        nodes = [
          ClusterNode(host=host, port=port)  # type: ignore[no-untyped-call]
          for host, port in snapshot.cluster_nodes
        ]
        client = RedisCluster(
          startup_nodes=nodes,
          password=password,
          username=snapshot.username,
          socket_timeout=snapshot.socket_timeout,
          socket_connect_timeout=snapshot.socket_connect_timeout,
          retry=_new_no_replay_retry(),
          max_connections=snapshot.max_connections,
          decode_responses=snapshot.decode_responses,
          require_full_coverage=(not snapshot.cluster_skip_full_coverage_check),
          ssl=snapshot.ssl_enabled,
          ssl_ca_certs=snapshot.ssl_cafile,
          ssl_certfile=snapshot.ssl_certfile,
          ssl_keyfile=snapshot.ssl_keyfile,
          ssl_check_hostname=snapshot.ssl_check_hostname,
        )
        self._raise_if_connect_cancelled(request_epoch)
      else:  # pragma: no cover - RedisSettings validation owns this branch
        raise ConfigurationError(
          f"Unsupported Redis mode: {snapshot.mode}", setting_name="mode"
        )

      if client is None or not client.ping():
        raise BackendConnectionError(
          "Redis health check returned false during connect",
          backend_type="redis",
        )
      self._raise_if_connect_cancelled(request_epoch)
      candidate = _RedisGeneration(
        key=object(),
        client=client,
        master_client=master_client,
        sentinel=sentinel,
        snapshot=snapshot,
      )
      with self._generation_condition:
        if (
          request_epoch != self._lifecycle_epoch
          or self._disconnecting
          or self._generation is not None
        ):
          raise _RedisConnectCancelled
        try:
          self._generation = candidate
          self._client = candidate.client
          self._master_client = candidate.master_client
          self._sentinel = candidate.sentinel
          self._generation_condition.notify_all()
          published = True
        except BaseException:
          published = False
          if self._generation is candidate:
            self._generation = None
          if self._client is candidate.client:
            self._client = None
          if self._master_client is candidate.master_client:
            self._master_client = None
          if self._sentinel is candidate.sentinel:
            self._sentinel = None
          self._generation_condition.notify_all()
          raise
      return candidate
    except BaseException:
      if not published:
        self._close_handles(client, master_client, sentinel)
      raise

  def _connect_for_epoch(self, request_epoch: int) -> bool:
    """Single-flight one candidate tied to the caller's lifecycle epoch."""
    previous_depth = int(getattr(self._connect_local, "depth", 0))
    if previous_depth:
      raise BackendConnectionError(
        "Cannot connect to Redis re-entrantly while building a candidate.",
        backend_type="redis",
      )
    self._connect_local.depth = previous_depth + 1
    try:
      with self._connect_lock:
        with self._generation_condition:
          if request_epoch != self._lifecycle_epoch or self._disconnecting:
            return False
          if self._generation is not None:
            return True
        snapshot, password, sentinel_password = self._capture_connection_plan()
        try:
          self._build_and_publish_generation(
            snapshot, password, sentinel_password, request_epoch
          )
        except _RedisConnectCancelled:
          return False
        except (BackendConnectionError, ConfigurationError):
          with self._generation_condition:
            if request_epoch != self._lifecycle_epoch or self._disconnecting:
              return False
          raise
        except Exception:
          with self._generation_condition:
            if request_epoch != self._lifecycle_epoch or self._disconnecting:
              return False
          raise BackendConnectionError(
            f"Failed to connect to Redis ({snapshot.mode.value})",
            backend_type="redis",
          ) from None
        logger.debug("Connected to Redis in %s mode", snapshot.mode.value)
        if snapshot.mode == RedisMode.MASTER_SLAVE and snapshot.replicas:
          logger.debug(
            "Configured %d replicas; current backend routing remains primary-only",
            len(snapshot.replicas),
          )
        return True
    finally:
      self._connect_local.depth = previous_depth

  def connect(self) -> None:
    """Privately build and atomically publish one Redis generation."""
    current_thread = threading.get_ident()
    with self._generation_condition:
      if self._disconnect_owner == current_thread:
        raise BackendConnectionError(
          "Cannot connect to Redis re-entrantly during disconnect.",
          backend_type="redis",
        )
      while self._disconnecting:
        if int(getattr(self._lease_local, "depth", 0)):
          raise BackendConnectionError(
            "Cannot connect to Redis re-entrantly during disconnect.",
            backend_type="redis",
          )
        self._generation_condition.wait()
      request_epoch = self._lifecycle_epoch
      if self._generation is not None:
        return
    self._connect_for_epoch(request_epoch)

  @contextlib.contextmanager
  def _lease_generation(self, operation: str) -> Iterator[_RedisGeneration]:
    """Lease one generation, lazily connecting once within the same epoch."""
    with self._generation_condition:
      if self._disconnecting:
        raise BackendConnectionError(
          f"Cannot {operation} while Redis is disconnecting.",
          backend_type="redis",
        )
      generation = self._generation
      request_epoch = self._lifecycle_epoch
      if generation is not None and generation.accepting:
        generation.active_leases += 1
        leased = True
      else:
        leased = False

    if not leased:
      connected = self._connect_for_epoch(request_epoch)
      with self._generation_condition:
        generation = self._generation
        if (
          not connected
          or request_epoch != self._lifecycle_epoch
          or self._disconnecting
          or generation is None
          or not generation.accepting
        ):
          raise BackendConnectionError(
            f"Redis connection changed while starting {operation}.",
            backend_type="redis",
          )
        generation.active_leases += 1

    assert generation is not None  # narrowed by either successful lease path
    previous_depth = int(getattr(self._lease_local, "depth", 0))
    self._lease_local.depth = previous_depth + 1
    try:
      yield generation
    finally:
      try:
        with self._generation_condition:
          generation.active_leases -= 1
          if generation.active_leases == 0:
            self._generation_condition.notify_all()
      finally:
        self._lease_local.depth = previous_depth

  @contextlib.contextmanager
  def _lease_existing_generation(
    self,
  ) -> Iterator[_RedisGeneration | None]:
    """Lease the current generation for a non-connecting health probe."""
    with self._generation_condition:
      generation = self._generation
      if self._disconnecting or generation is None or not generation.accepting:
        generation = None
      else:
        generation.active_leases += 1
    previous_depth = int(getattr(self._lease_local, "depth", 0))
    if generation is not None:
      self._lease_local.depth = previous_depth + 1
    try:
      yield generation
    finally:
      if generation is not None:
        try:
          with self._generation_condition:
            generation.active_leases -= 1
            if generation.active_leases == 0:
              self._generation_condition.notify_all()
        finally:
          self._lease_local.depth = previous_depth

  def disconnect(self) -> None:
    """Detach, drain, and close one Redis client generation."""
    if int(getattr(self._lease_local, "depth", 0)):
      raise BackendConnectionError(
        "Cannot disconnect Redis re-entrantly from an active operation.",
        backend_type="redis",
      )

    current_thread = threading.get_ident()
    pending_interrupt: BaseException | None = None
    owns_barrier = False
    generation: _RedisGeneration | None = None
    legacy_client: Redis | RedisCluster | None = None
    legacy_master: Redis | None = None
    legacy_sentinel: Sentinel | None = None
    try:
      with self._generation_condition:
        if self._disconnect_owner == current_thread:
          return
        if self._disconnecting:
          while self._disconnecting:
            self._generation_condition.wait()
          return
        owns_barrier = True
        self._disconnect_owner = current_thread
        self._disconnecting = True
        self._lifecycle_epoch += 1
        generation = self._generation
        legacy_client = self._client
        legacy_master = self._master_client
        legacy_sentinel = self._sentinel
        if generation is not None:
          generation.accepting = False
        self._generation = None
        self._client = None
        self._master_client = None
        self._sentinel = None
        if generation is not None:
          try:
            generation.retired.set()
          except BaseException as exc:
            pending_interrupt = exc
            try:
              generation.retired.set()
            except BaseException:
              pass
        while generation is not None and generation.active_leases:
          try:
            self._generation_condition.wait()
          except BaseException as exc:
            if pending_interrupt is None:
              pending_interrupt = exc

      try:
        if generation is not None:
          self._close_handles(
            generation.client,
            generation.master_client,
            generation.sentinel,
          )
        else:
          # Preserve best-effort cleanup for legacy/test code that populated
          # compatibility mirrors without an authoritative generation.
          self._close_handles(legacy_client, legacy_master, legacy_sentinel)
      except BaseException as exc:
        if pending_interrupt is None:
          pending_interrupt = exc
    finally:
      if owns_barrier:
        with self._generation_condition:
          self._disconnecting = False
          self._disconnect_owner = None
          try:
            self._generation_condition.notify_all()
          except BaseException as exc:
            if pending_interrupt is None:
              pending_interrupt = exc
            try:
              self._generation_condition.notify_all()
            except BaseException:
              pass

    if pending_interrupt is not None:
      raise pending_interrupt

  def is_connected(self) -> bool:
    """Return whether the current published generation responds to PING."""
    return self.ping()

  def ping(self) -> bool:
    """Probe one existing generation without triggering lazy connection."""
    with self._lease_existing_generation() as generation:
      if generation is None:
        return False
      try:
        result = generation.client.ping()
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
    """Return a point-in-time raw client, connecting if necessary.

    The returned SDK object is not leased after this property returns and may
    therefore be closed by a concurrent :meth:`disconnect`. Backend internals
    never use this escape hatch; each bundled operation holds a generation
    lease for its complete logical transaction.

    Returns:
        The Redis client instance.

    Raises:
        BackendConnectionError: If not connected and connection fails.
    """
    with self._lease_generation("access the raw Redis client") as generation:
      return generation.client

  @staticmethod
  def _register_script(client: Redis | RedisCluster, source: str) -> Any:
    """Compile ``source`` against the explicitly leased SDK client.

    Registration is cheap object construction; the script body is cached
    server-side via EVALSHA. RedisCluster exposes ``register_script`` at
    runtime although redis-py's stubs currently place it only on ``Redis``.
    """
    return cast("Redis", client).register_script(source)

  # QueueBackend implementation using Sorted Sets
  def _queue_key(self, queue_name: str, *, namespace: str | None = None) -> str:
    """Return the namespaced ZSET key for a logical queue.

    The complete queue identity is placed in a Redis Cluster hash tag. The
    item ZSET, payload hash, and FIFO counter therefore remain in one slot for
    Lua and transactional operations while other backend domains stay
    physically disjoint.
    """
    resolved_namespace = self.config.namespace if namespace is None else namespace
    return f"{{{resolved_namespace}:queue:{queue_name}}}:items"

  def _payload_key(self, queue_name: str, *, namespace: str | None = None) -> str:
    """Return the hash key used to store payloads for a queue.

    The key uses a Redis Cluster hash tag containing namespace, domain, and
    logical queue name so the ZSET, payload hash, and counter all land in the
    same cluster slot — required for Lua scripts and DELETE across all keys.

    Args:
        queue_name: Name of the queue.

    Returns:
        The Redis key for the payload hash, with hash tag.
    """
    resolved_namespace = self.config.namespace if namespace is None else namespace
    return f"{{{resolved_namespace}:queue:{queue_name}}}:payload"

  def _counter_key(self, queue_name: str, *, namespace: str | None = None) -> str:
    """Return the INCR counter key used to FIFO-order same-priority items.

    Args:
        queue_name: Name of the queue.

    Returns:
        The Redis key for the monotonic counter, with hash tag.
    """
    resolved_namespace = self.config.namespace if namespace is None else namespace
    return f"{{{resolved_namespace}:queue:{queue_name}}}:counter"

  def _set_key(self, set_name: str, *, namespace: str | None = None) -> str:
    """Return the namespaced physical key for a logical set."""
    resolved_namespace = self.config.namespace if namespace is None else namespace
    return f"{resolved_namespace}:set:{set_name}"

  def _storage_key(self, key: str, *, namespace: str | None = None) -> str:
    """Return the namespaced physical key for a logical storage key."""
    resolved_namespace = self.config.namespace if namespace is None else namespace
    return f"{resolved_namespace}:storage:{key}"

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
    with self._lease_generation("push to a queue") as generation:
      namespace = generation.snapshot.namespace
      queue_key = self._queue_key(queue_name, namespace=namespace)
      payload_key = self._payload_key(queue_name, namespace=namespace)
      counter_key = self._counter_key(queue_name, namespace=namespace)
      try:
        push_script = self._register_script(generation.client, _PUSH_LUA)
        push_script(
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
        ValueError: If queue_name contains invalid characters, or timeout is
            not a finite, non-negative number.
    """
    _validate_key_name(queue_name, "queue_name")
    normalized_timeout = _normalize_pop_timeout(timeout)
    deadline = (
      time.monotonic() + normalized_timeout if normalized_timeout > 0 else None
    )
    with self._lease_generation("pop from a queue") as generation:
      namespace = generation.snapshot.namespace
      queue_key = self._queue_key(queue_name, namespace=namespace)
      payload_key = self._payload_key(queue_name, namespace=namespace)
      try:
        pop_script = self._register_script(generation.client, _POP_LUA)
      except RedisError as e:
        raise QueueError(
          f"Failed to pop from queue {queue_name}: {e}",
          queue_name=queue_name,
          operation="pop",
        ) from e

      if normalized_timeout == 0:
        return self._atomic_pop_once(queue_name, queue_key, payload_key, pop_script)

      assert deadline is not None
      while True:
        item = self._atomic_pop_once(queue_name, queue_key, payload_key, pop_script)
        if item is not None:
          return item
        if generation.retired.is_set():
          raise QueueError(
            f"Redis disconnected while waiting to pop from queue {queue_name}",
            queue_name=queue_name,
            operation="pop",
          )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
          return None
        if generation.retired.wait(min(_BLOCKING_POP_POLL_INTERVAL, remaining)):
          raise QueueError(
            f"Redis disconnected while waiting to pop from queue {queue_name}",
            queue_name=queue_name,
            operation="pop",
          )

  def _atomic_pop_once(
    self,
    queue_name: str,
    queue_key: str,
    payload_key: str,
    pop_script: Any,
  ) -> bytes | None:
    """Atomically remove one queue member and its sidecar payload."""
    try:
      result = pop_script(keys=[queue_key, payload_key])
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
    with self._lease_generation("read a queue length") as generation:
      queue_key = self._queue_key(queue_name, namespace=generation.snapshot.namespace)
      try:
        # redis-py's shared sync/async stubs type zcard() as ResponseT
        # (Awaitable[Any] | int); the sync client returns int at runtime.
        return cast(  # type: ignore[redundant-cast]
          "int", generation.client.zcard(queue_key)
        )
      except RedisError as e:
        # Do not conflate an empty queue with a backend failure. The scheduler
        # trusts this result for pending-work and backpressure decisions.
        raise QueueError(str(e), queue_name=queue_name, operation="queue_len") from e

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
    with self._lease_generation("clear a queue") as generation:
      namespace = generation.snapshot.namespace
      queue_key = self._queue_key(queue_name, namespace=namespace)
      payload_key = self._payload_key(queue_name, namespace=namespace)
      counter_key = self._counter_key(queue_name, namespace=namespace)
      try:
        generation.client.delete(queue_key, payload_key, counter_key)
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
    with self._lease_generation("add to a set") as generation:
      set_key = self._set_key(set_name, namespace=generation.snapshot.namespace)
      try:
        return generation.client.sadd(set_key, item) == 1
      except RedisError as e:
        # Wrap so BackendDupeFilter can degrade to not-seen during an outage.
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
    with self._lease_generation("remove from a set") as generation:
      set_key = self._set_key(set_name, namespace=generation.snapshot.namespace)
      try:
        return generation.client.srem(set_key, item) == 1
      except RedisError as e:
        raise BackendConnectionError(
          f"Redis set remove failed for {set_name!r}: {e}",
          backend_type="redis",
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
    with self._lease_generation("check set membership") as generation:
      set_key = self._set_key(set_name, namespace=generation.snapshot.namespace)
      try:
        result = generation.client.sismember(set_key, cast("str", item))
      except RedisError as e:
        raise BackendConnectionError(
          f"Redis set contains failed for {set_name!r}: {e}",
          backend_type="redis",
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
    with self._lease_generation("read a set length") as generation:
      set_key = self._set_key(set_name, namespace=generation.snapshot.namespace)
      try:
        return cast(  # type: ignore[redundant-cast]
          "int", generation.client.scard(set_key)
        )
      except RedisError as e:
        raise BackendConnectionError(
          f"Redis set length failed for {set_name!r}: {e}",
          backend_type="redis",
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
    with self._lease_generation("clear a set") as generation:
      set_key = self._set_key(set_name, namespace=generation.snapshot.namespace)
      try:
        generation.client.delete(set_key)
      except RedisError as e:
        # A swallowed clear hides a failed dedup reset and stale fingerprints.
        raise BackendConnectionError(
          f"Redis set clear failed for {set_name!r}: {e}",
          backend_type="redis",
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
    with self._lease_generation("store a value") as generation:
      storage_key = self._storage_key(key, namespace=generation.snapshot.namespace)
      stored: bool | str | bytes | None
      try:
        if ttl is not None:
          stored = generation.client.setex(storage_key, ttl, data)
        else:
          stored = generation.client.set(storage_key, data)
      except RedisError as e:
        # Do not swallow: the pipeline must count and escalate storage loss.
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
    with self._lease_generation("retrieve a value") as generation:
      storage_key = self._storage_key(key, namespace=generation.snapshot.namespace)
      try:
        result = generation.client.get(storage_key)
      except RedisError as e:
        msg = f"Failed to retrieve key {key!r} from Redis: {e}"
        raise StorageError(msg, operation="retrieve", key=key) from e
      if result is None:
        return None
      if isinstance(result, bytes):
        return result
      # redis-py may return str for string values in some modes.
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
    with self._lease_generation("delete a value") as generation:
      storage_key = self._storage_key(key, namespace=generation.snapshot.namespace)
      try:
        return generation.client.delete(storage_key) == 1
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
    with self._lease_generation("check value existence") as generation:
      storage_key = self._storage_key(key, namespace=generation.snapshot.namespace)
      try:
        return generation.client.exists(storage_key) == 1
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
    with self._lease_generation("read a value TTL") as generation:
      storage_key = self._storage_key(key, namespace=generation.snapshot.namespace)
      try:
        result = cast(  # type: ignore[redundant-cast]
          "int", generation.client.ttl(storage_key)
        )
      except RedisError as e:
        msg = f"Failed to read TTL of key {key!r} in Redis: {e}"
        raise StorageError(msg, operation="ttl", key=key) from e
      # -2 = no key, -1 = no TTL, >= 0 = remaining seconds.
      return None if result < 0 else result

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    This always scans only the configured namespace's storage domain. It never
    uses ``FLUSHDB``/``FLUSHALL``, because a Redis database may be shared with
    queues, deduplication sets, or unrelated applications. In cluster mode,
    redis-py's cluster ``scan_iter`` scans all nodes. SCAN is not a
    transactional snapshot; concurrent external writers can be missed, and a
    failure after one or more deletes is reported as possibly partial.

    Args:
        prefix: If provided, only clear logical storage keys starting with this
               prefix. If None, clear all storage keys owned by this namespace.

    Raises:
        ValueError: If prefix contains invalid characters.
        StorageError: If the clear fails at the Redis layer (parity with store
            R-store #59 and mongodb/memcached/dynamodb clear_storage). Earlier
            deletes are not rolled back, so a failure may be partial.
    """
    if prefix is not None:
      _validate_key_name(prefix, "prefix")
    logical_prefix = prefix or ""
    with self._lease_generation("clear storage") as generation:
      pattern = (
        f"{self._storage_key(logical_prefix, namespace=generation.snapshot.namespace)}*"
      )
      try:
        for physical_key in generation.client.scan_iter(match=pattern):
          generation.client.delete(physical_key)
      except RedisError as e:
        raise StorageError(
          "Redis storage clear failed and may be partially complete.",
          operation="clear_storage",
          key=None,
        ) from e
