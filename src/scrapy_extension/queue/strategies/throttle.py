"""Throttle queue strategy — rate-limited pops (subsystem ②).

A task-queue type "beyond queue/stack/priority": caps the consumption rate by
enforcing a minimum interval between successful pops. Useful for polite
crawling. Unlike Delay/RoundRobin, items persist in the backend (this wraps
``pop`` with a rate gate rather than holding items in-process).
"""

from __future__ import annotations

__all__ = ["ThrottleQueueStrategy"]

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from scrapy_extension.queue.strategies.base import QueueStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


class ThrottleQueueStrategy(QueueStrategy):
  """Enforces a minimum interval between successful pops (max pop rate).

  ``push`` passes through to the backend; ``pop`` returns ``None`` if called
  within ``min_interval`` of the last successful pop. A throttled pop looks
  like an empty queue to the scheduler (it retries next tick), so the
  effective pop rate is at most ``1 / min_interval``.

  Attributes:
      _min_interval: Minimum seconds between successful pops.
      _clock: Monotonic clock callable (injectable for tests).
      _last_pop: Timestamp of the last successful pop, or None.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    *,
    min_interval: float = 0.0,
    clock: Callable[[], float] = time.monotonic,
  ) -> None:
    """Initialize the throttle strategy.

    Args:
        connection_manager: Connection manager providing the QueueBackend.
        min_interval: Minimum seconds between successful pops.
        clock: Monotonic clock callable returning seconds (injectable for tests).

    Raises:
        ValueError: If min_interval is negative.
    """
    super().__init__(connection_manager)
    if min_interval < 0:
      raise ValueError(f"min_interval must be >= 0, got {min_interval}")
    self._min_interval = min_interval
    self._clock = clock
    self._last_pop: float | None = None

  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Push straight through to the backend (delay/source ignored).

    Args:
        queue_name: The queue name.
        item: Serialized item bytes.
        priority: Priority passed through to the backend.
        delay: Ignored (throttle gates pops, not pushes).
        source: Ignored.
    """
    del delay, source
    self._connection_manager.get_queue_backend().push(queue_name, item, priority)

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop unless within ``min_interval`` of the last successful pop.

    Args:
        queue_name: The queue name.
        timeout: Seconds to block (0 = non-blocking).

    Returns:
        The next item, or None if throttled or empty.
    """
    now = self._clock()
    if self._last_pop is not None and (now - self._last_pop) < self._min_interval:
      return None
    item = self._connection_manager.get_queue_backend().pop(queue_name, timeout)
    if item is not None:
      self._last_pop = now
    return item

  def queue_len(self, queue_name: str) -> int:
    """Return the backend queue length.

    Args:
        queue_name: The queue name.

    Returns:
        Number of items in the backend queue.
    """
    return self._connection_manager.get_queue_backend().queue_len(queue_name)

  def clear(self, queue_name: str) -> None:
    """Clear the backend queue and reset the throttle timer.

    Args:
        queue_name: The queue name.
    """
    self._connection_manager.get_queue_backend().clear_queue(queue_name)
    self._last_pop = None
