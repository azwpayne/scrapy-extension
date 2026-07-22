"""Throttle queue strategy — rate-limited pops (subsystem ②).

A task-queue type "beyond queue/stack/priority": caps the consumption rate by
enforcing a minimum interval between successful pops. Useful for polite
crawling. Unlike Delay/RoundRobin, items persist in the backend (this wraps
``pop`` with a rate gate rather than holding items in-process).

Rate semantics (R14-F): the rate budget is **per-instance**. Two
:class:`ThrottleQueueStrategy` instances in the same process each enforce
their own ``min_interval`` — running two spiders (each with its own queue
strategy) in one process silently doubles the aggregate pop rate from the
backend's perspective. For a process-wide rate ceiling, share a single
strategy instance across queues or enforce the cap upstream (e.g. at the
backend connection).
"""

from __future__ import annotations

__all__ = ["ThrottleQueueStrategy"]

import math
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.queue.strategies.base import (
  QueueStrategy,
  normalize_queue_timeout,
)

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


# R14-F: ceiling on `min_interval` (seconds). A pathologically large value
# (e.g. ``1e9``) makes the queue look permanently empty for the process
# lifetime — a soft DoS via misconfig. 3600s = 1 hour is the documented
# upper bound; values above are rejected as ConfigurationError. Callers
# needing longer effective backoff should chain a delay strategy, not push
# the throttle ceiling into "queue never serves" territory.
THROTTLE_MAX_MIN_INTERVAL_S: float = 3600.0


class ThrottleQueueStrategy(QueueStrategy):
  """Enforces a minimum interval between successful pops (max pop rate).

  ``push`` passes through to the backend; ``pop`` returns ``None`` if called
  within ``min_interval`` of the last successful pop. A throttled pop looks
  like an empty queue to the scheduler (it retries next tick), so the
  effective pop rate is at most ``1 / min_interval``.

  Per-instance rate (R14-F): see module docstring — two instances in one
  process each enforce their own budget.

  Attributes:
      _min_interval: Minimum seconds between successful pops.
      _clock: Monotonic clock callable (injectable for tests).
      _last_pop: Timestamp of the last successful pop, or None.
      _gate_lock: Serializes the positive-interval check/pop/commit transaction.
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
        min_interval: Minimum seconds between successful pops. Must be in
            ``[0, THROTTLE_MAX_MIN_INTERVAL_S]`` (3600s = 1h). Values above
            the ceiling are rejected as ``ConfigurationError`` — a
            pathologically large value (e.g. ``1e9``) would make the queue
            look permanently empty for the process lifetime (R14-F: soft
            DoS via misconfig). Negative and NaN values raise ``ValueError``.
        clock: Monotonic clock callable returning seconds (injectable for tests).

    Raises:
        ValueError: If min_interval is negative or NaN.
        ConfigurationError: If min_interval exceeds the documented ceiling.
    """
    super().__init__(connection_manager)
    if isinstance(min_interval, bool) or not isinstance(min_interval, (int, float)):
      raise ValueError(f"min_interval must be finite, got {min_interval!r}")
    try:
      min_interval = float(min_interval)
    except (OverflowError, TypeError, ValueError) as e:
      raise ValueError(f"min_interval must be finite, got {min_interval!r}") from e
    if not math.isfinite(min_interval):
      raise ValueError(f"min_interval must be finite, got {min_interval}")
    if min_interval < 0:
      raise ValueError(f"min_interval must be >= 0, got {min_interval}")
    # R14-F MED: bound min_interval — a value of e.g. 1e9 makes the queue
    # look permanently empty for the process lifetime (soft DoS via
    # misconfig). Reject loudly with the stable setting_name so operators
    # grep their way to the fix.
    if min_interval > THROTTLE_MAX_MIN_INTERVAL_S:
      raise ConfigurationError(
        f"min_interval must be <= {THROTTLE_MAX_MIN_INTERVAL_S}s "
        f"(got {min_interval}s). A pathologically large min_interval makes "
        "the queue look permanently empty for the process lifetime.",
        setting_name="min_interval",
        setting_value=min_interval,
      )
    self._min_interval = min_interval
    self._clock = clock
    self._last_pop: float | None = None
    self._gate_lock = threading.Lock()

  def is_push_durable(self, *, delay: float, source: str) -> bool:
    """Report that throttling affects pops, never backend push durability."""
    del delay, source
    return True

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
    timeout = normalize_queue_timeout(timeout)
    backend = self._connection_manager.get_queue_backend()
    if self._min_interval == 0:
      return backend.pop(queue_name, timeout)
    with self._gate_lock:
      now = self._clock()
      if self._last_pop is not None and (now - self._last_pop) < self._min_interval:
        return None
      item = backend.pop(queue_name, timeout)
      if item is not None:
        self._last_pop = self._clock()
      return item

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Throttled pop, threading the per-message ack token (MQ backends, #28).

    Same throttle gate as :meth:`pop`: returns ``(None, None)`` within
    ``min_interval`` of the last successful pop (without touching the
    backend). Past the gate, delegates to
    :meth:`QueueStrategy._pop_backend_with_ack` so MQ backends keep their
    deferred-ack token instead of silently falling back to atomic ``pop()``
    (pre-fix the inherited base default dropped the token).
    """
    timeout = normalize_queue_timeout(timeout)
    if self._min_interval == 0:
      return self._pop_backend_with_ack(queue_name, timeout)
    with self._gate_lock:
      now = self._clock()
      if self._last_pop is not None and (now - self._last_pop) < self._min_interval:
        return (None, None)
      data, token = self._pop_backend_with_ack(queue_name, timeout)
      if data is not None:
        self._last_pop = self._clock()
      return data, token

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
    with self._gate_lock:
      self._connection_manager.get_queue_backend().clear_queue(queue_name)
      self._last_pop = None
