"""Abstract queue-strategy interface for pluggable task-queue semantics (subsystem ②).

Defines :class:`QueueStrategy` — the strategy interface that
:class:`~scrapy_extension.queue.queue.BackendQueue` delegates bytes-level
push/pop to, so queueing semantics (passthrough, delay, ...) are pluggable
without changing the backend interface or the request-serialization layer.
"""

from __future__ import annotations

__all__ = ["QueueStrategy", "normalize_queue_timeout"]

import math
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from scrapy_extension.exceptions import QueueError

if TYPE_CHECKING:
  from scrapy_extension.backends.base import QueueBackend
  from scrapy_extension.backends.connectors import ConnectionManager


def normalize_queue_timeout(timeout: float) -> float:
  """Return a finite non-negative queue timeout.

  A non-finite timeout is not merely malformed input for polling backends:
  Redis' deadline loop never expires for ``NaN`` or ``inf``. Keep one strict
  contract for every strategy before any backend is touched.
  """
  if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
    raise ValueError(f"timeout must be a finite non-negative number, got {timeout!r}")
  try:
    normalized = float(timeout)
  except (OverflowError, TypeError, ValueError) as e:
    raise ValueError(
      f"timeout must be a finite non-negative number, got {timeout!r}"
    ) from e
  if not math.isfinite(normalized) or normalized < 0:
    raise ValueError(f"timeout must be a finite non-negative number, got {timeout!r}")
  return normalized


class _QueueAckToken(ABC):
  """Internal process-local token with explicit terminal operations."""

  @abstractmethod
  def ack(self) -> None:
    """Acknowledge the represented delivery transition."""

  @abstractmethod
  def nack(self) -> None:
    """Negatively acknowledge the represented delivery transition."""


class _BoundQueueAckToken(_QueueAckToken):
  """Bind a raw delivery token to its issuing backend incarnation.

  ``ConnectionManager`` may replace a disconnected backend while a Scrapy
  request remains in flight. Resolving the manager again at completion can
  hand the old raw token to the replacement backend, whose local generation
  counters and broker identifiers may have restarted. Keep the exact backend
  object and physical queue used by the pop so ack/nack cannot cross that
  incarnation boundary.

  The wrapper is process-local and intentionally private. ``BackendQueue``
  strips it at the serialization boundary just like every raw ack token.
  """

  __slots__ = ("_backend", "_queue_name", "_state", "_state_lock", "_token")

  def __init__(
    self,
    backend: QueueBackend,
    queue_name: str,
    token: Any,
  ) -> None:
    self._backend = backend
    self._queue_name = queue_name
    self._token = token
    self._state = "pending"
    self._state_lock = threading.Lock()

  @property
  def backend(self) -> QueueBackend:
    """The immutable backend proxy that issued the raw token."""
    return self._backend

  @property
  def queue_name(self) -> str:
    """The immutable physical queue used for the delivery."""
    return self._queue_name

  @property
  def token(self) -> Any:
    """The raw backend token; exposed only for internal compatibility tests."""
    return self._token

  @property
  def state(self) -> str:
    """Return ``pending``, ``acked``, or ``nacked`` for diagnostics."""
    with self._state_lock:
      return self._state

  def ack(self) -> None:
    """Acknowledge once through the backend that issued this token."""
    self._settle("acked")

  def nack(self) -> None:
    """Negatively acknowledge once through the issuing backend instance."""
    self._settle("nacked")

  def _settle(self, terminal_state: str) -> None:
    """Apply exactly one successful terminal transition.

    The lock spans the backend call so concurrent response/error signals cannot
    send conflicting terminal operations. A broker exception leaves the state
    pending, allowing the same operation to be retried safely.
    """
    with self._state_lock:
      if self._state != "pending":
        return
      if terminal_state == "acked":
        self._backend.ack(self._queue_name, token=self._token)
      else:
        self._backend.nack(self._queue_name, token=self._token)
      self._state = terminal_state

  def __repr__(self) -> str:
    """Return diagnostics without exposing the raw token's representation."""
    return (
      f"_BoundQueueAckToken(backend={type(self._backend).__name__}, "
      f"queue_name={self._queue_name!r}, token_type={type(self._token).__name__}, "
      f"state={self.state!r})"
    )


@dataclass(frozen=True, slots=True)
class _PreparedQueuePush:
  """One immutable strategy route whose commit returns exact durability."""

  backend_route: bool
  _commit: Callable[[bytes, bool], bool] = field(repr=False, compare=False)

  def commit(self, item: bytes, *, require_durable: bool = False) -> bool:
    """Commit ``item`` once; only literal ``True`` is durability evidence."""
    return self._commit(item, require_durable) is True

  @classmethod
  def local(
    cls,
    *,
    queue_name: str,
    strategy_name: str,
    publish: Callable[[bytes], None],
  ) -> _PreparedQueuePush:
    """Build a known-process-local route with a pre-mutation durability gate."""

    def commit(item: bytes, require_durable: bool) -> bool:
      if require_durable:
        raise QueueError(
          f"Selected queue route {strategy_name} is not worker-crash durable",
          queue_name=queue_name,
          operation="push",
        )
      publish(item)
      return False

    return cls(backend_route=False, _commit=commit)


class QueueStrategy(ABC):
  """Strategy interface for task-queue push/pop semantics.

  A strategy owns how serialized items are stored and retrieved: ordering
  (FIFO/LIFO/priority), holding (delay), fairness (round-robin), etc. It
  receives a connection manager so it can drive the underlying
  ``QueueBackend`` (and, where needed, ``StorageBackend``).

  Attributes:
      _connection_manager: Source of the QueueBackend / StorageBackend.
  """

  def __init__(self, connection_manager: ConnectionManager) -> None:
    """Initialize the strategy.

    Args:
        connection_manager: Connection manager providing the backends.
    """
    self._connection_manager = connection_manager
    self._queue_binding_lock = threading.Lock()
    self._bound_queue_name: str | None = None

  def bind(self, queue_name: str) -> None:  # noqa: B027
    """Bind a strategy to its owning logical queue when it requires one.

    Backend-delegating strategies remain shareable and use the default no-op.
    In-process strategies override this hook with
    :meth:`_bind_single_queue`, preventing state restored under one snapshot
    key from being popped through another logical queue.
    """

  def _bind_single_queue(self, queue_name: str) -> None:
    """Bind in-process state to exactly one logical queue name."""
    with self._queue_binding_lock:
      if self._bound_queue_name is None:
        self._bound_queue_name = queue_name
        return
      if self._bound_queue_name != queue_name:
        raise ValueError(
          f"{type(self).__name__} is already bound to logical queue "
          f"{self._bound_queue_name!r}; cannot reuse it for {queue_name!r}"
        )

  def is_push_durable(self, *, delay: float, source: str) -> bool:
    """Legacy route hint retained for source compatibility.

    The conservative default remains ``False``, but this pre-operation claim
    is no longer trusted as commit evidence: serialization callbacks can
    change a strategy route, and a backend generation can change before a
    later push. ``BackendQueue`` instead uses the private prepared-route
    operation, which binds the actual push to its durability receipt.
    """
    del delay, source
    return False

  def _prepare_push(
    self,
    queue_name: str,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> _PreparedQueuePush:
    """Freeze a conservative route for one later serialized-item commit.

    The default intentionally ignores the legacy ``is_push_durable`` claim.
    Existing custom strategies keep working for ordinary pushes, but they do
    not gain authority to acknowledge a broker source or publish a persistent
    dedup marker without adopting this private operation-bound protocol.
    """

    def publish(item: bytes) -> None:
      self.push(
        queue_name,
        item,
        priority=priority,
        delay=delay,
        source=source,
      )

    return _PreparedQueuePush.local(
      queue_name=queue_name,
      strategy_name=type(self).__name__,
      publish=publish,
    )

  def _prepare_backend_push(
    self,
    queue_name: str,
    *,
    priority: float = 0.0,
  ) -> _PreparedQueuePush:
    """Freeze a physical backend route and bind receipt to its exact push."""

    def commit(item: bytes, require_durable: bool) -> bool:
      return self._push_backend_prepared(
        queue_name,
        item,
        priority=priority,
        require_durable=require_durable,
      )

    return _PreparedQueuePush(backend_route=True, _commit=commit)

  def _push_backend_prepared(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    require_durable: bool = False,
  ) -> bool:
    """Execute one prepared backend push and normalize its private receipt."""
    receipt = self._connection_manager._push_queue_with_durability(
      queue_name,
      item,
      priority,
      require_durable=require_durable,
    )
    return receipt.worker_crash_durable is True

  @abstractmethod
  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Push a serialized item. Strategies define ordering/holding semantics.

    Args:
        queue_name: The queue name.
        item: Serialized item bytes.
        priority: Caller-supplied priority (semantics depend on strategy).
        delay: Optional delay in seconds before the item becomes poppable.
        source: Optional source tag (used by round-robin fairness strategies).
    """

  @abstractmethod
  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop the next ready item.

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        The next item, or None if empty.
    """

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop the next ready item plus a backend ack token (additive; #28).

    Default returns ``(self.pop(...), None)`` -- correct for strategies that
    hold no broker message to ack (in-process: round_robin / ring_buffer) and
    for strategies whose ack semantics don't map to a single backend message.
    Backend-using strategies override this to call
    ``QueueBackend.pop_with_ack`` and thread the token through (passthrough /
    delay / throttle / priority / time_wheel / work_stealing). Custom
    backend-delegating strategies should use ``_pop_backend_with_ack`` or
    ``_pop_backend_instance_with_ack``: those helpers bind a raw broker token
    to its issuing backend incarnation and physical queue.

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        ``(item, token)`` -- ``token`` is an opaque incarnation-bound token,
        or ``None`` when the strategy/backend has no per-message correlation.
    """
    return (self.pop(queue_name, timeout), None)

  def _pop_backend_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop from the backend, threading the per-message ack token when it provides one.

    Shared by backend-delegating strategies (passthrough / delay / throttle /
    time_wheel).
    MQ backends that override ``QueueBackend.pop_with_ack`` take the
    token-correlated path; atomic-pop backends keep the plain ``pop()`` path
    (byte-identical roundtrip for them). The breaker proxy is unwrapped
    before the class-level check (the proxy binds ``pop_with_ack`` as an
    instance attribute, so the proxy class resolves it to the ABC default
    via MRO).

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        ``(item, token)`` where a non-``None`` raw token is bound to the
        issuing backend and physical queue; ``None`` for atomic-pop backends.
    """
    backend = self._connection_manager.get_queue_backend()
    return self._pop_backend_instance_with_ack(backend, queue_name, timeout)

  @staticmethod
  def _pop_backend_instance_with_ack(
    backend: QueueBackend,
    queue_name: str,
    timeout: float = 0.0,
  ) -> tuple[bytes | None, Any | None]:
    """Pop from one backend and bind any token to that exact instance.

    Strategies that scan multiple physical queues must keep the backend and
    physical queue selected for each individual pop. This helper centralizes
    both the override detection and the incarnation-bound token wrapper.
    """
    from scrapy_extension.backends.base import QueueBackend

    wrapped_backend = getattr(backend, "_backend", None)
    unwrapped = (
      wrapped_backend if isinstance(wrapped_backend, QueueBackend) else backend
    )
    backend_cls = getattr(unwrapped, "__class__", None)
    is_interface_backend = isinstance(backend, QueueBackend)
    override = (
      backend_cls is not None
      and getattr(backend_cls, "pop_with_ack", None) is not None
      and backend_cls.pop_with_ack is not QueueBackend.pop_with_ack
    )
    if override:
      data, token = backend.pop_with_ack(queue_name, timeout)
    elif not is_interface_backend:
      # Lightweight ConnectionManager stubs are common for QueueStrategy
      # consumers. Honor an explicitly configured tuple result, but fall back
      # to ``pop`` when an unconfigured Mock returns another Mock.
      result = backend.pop_with_ack(queue_name, timeout)
      if isinstance(result, tuple) and len(result) == 2:
        data, token = result
      else:
        data = backend.pop(queue_name, timeout)
        token = None
    else:
      data = backend.pop(queue_name, timeout)
      token = None
    if token is None:
      return (data, None)
    if isinstance(token, _BoundQueueAckToken):
      return (data, token)
    # Preserve permissive duck-typed test/custom-manager behavior. Production
    # ConnectionManager enforces QueueBackend before a strategy can get here;
    # only interface-backed deliveries participate in the incarnation fence.
    if not is_interface_backend:
      return (data, token)
    return (data, _BoundQueueAckToken(backend, queue_name, token))

  @abstractmethod
  def queue_len(self, queue_name: str) -> int:
    """Return the number of pending (and held) items.

    Args:
        queue_name: The queue name.

    Returns:
        Approximate item count.
    """

  @abstractmethod
  def clear(self, queue_name: str) -> None:
    """Clear the queue and any held items.

    Args:
        queue_name: The queue name.
    """

  def open(self) -> None:  # noqa: B027
    """Lifecycle hook — prepare the strategy. Default no-op."""

  def begin_close(self) -> None:  # noqa: B027
    """Stop blocking operations without destroying snapshot state.

    Called after the owning queue stops admitting new operations but before it
    waits for already-admitted operations to finish. Strategies with blocking
    operations may override this hook to wake them. Destructive cleanup belongs
    in :meth:`close`, which runs only after the final snapshot is persisted.
    """

  def close(self) -> None:  # noqa: B027
    """Lifecycle hook — release resources. Default no-op."""

  def snapshot(self) -> bytes | None:
    """Serialize in-process state for crash/restart recovery (initiative #3).

    Returns a versioned, storage-storable bytes blob, or ``None`` when the
    strategy holds no persistable state (the default). Override to enable
    snapshot/restore for strategies with in-process held state (e.g.
    :class:`~scrapy_extension.queue.strategies.delay.DelayQueueStrategy`'s
    held-item heap — without this, delayed items are lost on close/restart).

    :class:`~scrapy_extension.queue.queue.BackendQueue` calls this on
    :meth:`close` and persists the result via the connection manager's
    storage backend (when storage-capable); ``None`` means "nothing to
    persist" and skips the store.

    Returns:
        Bytes blob consumed by :meth:`restore`, or ``None``.
    """
    return None

  def restore(self, state: bytes | None) -> None:
    """Re-populate in-process state from a prior :meth:`snapshot` (initiative #3).

    Default no-op. Called once on startup by
    :class:`~scrapy_extension.queue.queue.BackendQueue`. A ``None`` state
    (no prior snapshot) is a no-op. Corrupt / unknown-format state MUST be
    logged + skipped — restore never crashes the spider.

    Args:
        state: The bytes blob from a prior :meth:`snapshot`, or ``None``.
    """
    del state
