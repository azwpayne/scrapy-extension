"""Abstract queue-strategy interface for pluggable task-queue semantics (subsystem ②).

Defines :class:`QueueStrategy` — the strategy interface that
:class:`~scrapy_extension.queue.queue.BackendQueue` delegates bytes-level
push/pop to, so queueing semantics (passthrough, delay, ...) are pluggable
without changing the backend interface or the request-serialization layer.
"""

from __future__ import annotations

__all__ = ["QueueStrategy"]

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

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
