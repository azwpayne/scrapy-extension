"""Passthrough queue strategy — delegates to QueueBackend unchanged (subsystem ② default).

The default strategy. Byte-identical to ``BackendQueue``'s pre-strategy
behavior, preserving backward compatibility. ``delay`` is ignored (this is
not a delay queue).
"""

from __future__ import annotations

__all__ = ["PassthroughQueueStrategy"]

from scrapy_extension.queue.strategies.base import QueueStrategy


class PassthroughQueueStrategy(QueueStrategy):
  """Push/pop/len/clear pass straight through to the QueueBackend.

  This preserves the exact pre-strategy ``BackendQueue`` behavior, so it is
  the default and is fully backward-compatible. ``delay`` is ignored.
  """

  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Push straight to the QueueBackend (delay/source ignored).

    Args:
        queue_name: The queue name.
        item: Serialized item bytes.
        priority: Priority passed through to the backend.
        delay: Ignored (passthrough is not a delay queue).
        source: Ignored (passthrough does no fairness routing).
    """
    del delay, source
    self._connection_manager.get_queue_backend().push(queue_name, item, priority)

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop from the QueueBackend.

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        The next item, or None if empty.
    """
    return self._connection_manager.get_queue_backend().pop(queue_name, timeout)

  def queue_len(self, queue_name: str) -> int:
    """Return the QueueBackend length.

    Args:
        queue_name: The queue name.

    Returns:
        Number of items in the backend queue.
    """
    return self._connection_manager.get_queue_backend().queue_len(queue_name)

  def clear(self, queue_name: str) -> None:
    """Clear the QueueBackend queue.

    Args:
        queue_name: The queue name.
    """
    self._connection_manager.get_queue_backend().clear_queue(queue_name)
