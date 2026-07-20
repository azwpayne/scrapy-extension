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
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
    delay / throttle / priority / time_wheel / work_stealing).

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        ``(item, token)`` -- ``token`` is ``None`` when the strategy/backend
        has no per-message ack correlation.
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
        ``(item, token)`` -- ``token`` is ``None`` for atomic-pop backends.
    """
    from scrapy_extension.backends.base import QueueBackend

    backend = self._connection_manager.get_queue_backend()
    unwrapped = getattr(backend, "_backend", backend)
    backend_cls = getattr(unwrapped, "__class__", None)
    override = (
      backend_cls is not None
      and getattr(backend_cls, "pop_with_ack", None) is not None
      and backend_cls.pop_with_ack is not QueueBackend.pop_with_ack
    )
    if override:
      return backend.pop_with_ack(queue_name, timeout)
    data = backend.pop(queue_name, timeout)
    return (data, None)

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
