"""Abstract queue-strategy interface for pluggable task-queue semantics (subsystem ②).

Defines :class:`QueueStrategy` — the strategy interface that
:class:`~scrapy_extension.queue.queue.BackendQueue` delegates bytes-level
push/pop to, so queueing semantics (passthrough, delay, ...) are pluggable
without changing the backend interface or the request-serialization layer.
"""

from __future__ import annotations

__all__ = ["QueueStrategy"]

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


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
    Backend-using strategies that CAN correlate a per-message token override
    this to call ``QueueBackend.pop_with_ack`` and thread the token through
    (passthrough / priority / work_stealing).

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

    Shared by backend-delegating strategies (passthrough / delay / throttle).
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
