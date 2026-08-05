"""Scrapy-stats-backed monitor (Unit F — Tier-2).

:class:`ScrapyStatsMonitor` turns monitor hooks into namespaced Scrapy stats
so operators see queue / dedup / error telemetry on the standard Scrapy stats
dump. It is the concrete monitor wired by ``from_crawler`` factories whenever
``crawler.stats`` is available; otherwise components fall back to
:class:`~scrapy_extension.monitor.base.NullMonitor`.
"""

from __future__ import annotations

__all__ = ["ScrapyStatsMonitor"]

import functools
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, ParamSpec

from scrapy_extension.monitor.base import (
  DEFAULT_BACKPRESSURE_THRESHOLD,
  DEFAULT_POP_RATE_WINDOW_S,
  Monitor,
)

if TYPE_CHECKING:
  from scrapy.statscollectors import StatsCollector

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")


def _stats_safe(hook: Callable[_P, None]) -> Callable[_P, None]:
  """Decorator: a ScrapyStatsMonitor hook must never propagate stats failures.

  Observability must not crash the data path. If the wrapped
  :class:`~scrapy.statscollectors.StatsCollector` raises (a custom collector
  bug, a stats-backend outage), swallow the exception and log at ``debug`` so
  the spider keeps running and the operator can still diagnose via the log.
  Applied to every ``on_*`` hook (R5).

  The broad ``except Exception`` is deliberate (review feedback, R5): the
  decorated hook bodies are trivial -- the only callable that can raise is
  ``self._stats.inc_value`` / ``set_value`` (the rest is a string key), so a
  caught exception is in practice a StatsCollector failure, not a hidden
  programmer error. The wrapper also preserves the wrapped hook's return
  value on the success path (returns ``None`` only in the except branch), so
  future hooks that return meaningful values are not silently dropped.

  Directive: keep decorated hook bodies trivial (one stats call). If a hook
  grows non-trivial logic, move ONLY the ``self._stats.*`` call under the
  safe path -- don't let a buggy hook body hide behind this decorator.
  """

  @functools.wraps(hook)
  def _wrapper(*args: _P.args, **kwargs: _P.kwargs) -> None:
    hook_failed = False
    try:
      hook(*args, **kwargs)
    except Exception:
      hook_failed = True
    if hook_failed:
      try:
        logger.debug("ScrapyStatsMonitor hook raised; ignored")
      except BaseException:
        # The StatsCollector's ordinary failure is already intentionally
        # suppressed. A logger handler interruption is diagnostic-only and
        # must not make the monitor observable to the data path.
        pass

  return _wrapper


class ScrapyStatsMonitor(Monitor):
  """Monitor that increments namespaced Scrapy stats.

  Stat keys (all additive — existing component stats are untouched):

  - ``queue/push_count`` (counter) — per successful push.
  - ``queue/pop_attempt_count`` (counter) — per pop ATTEMPT (R14-D rename of
    ``queue/pop_count``). ``BackendQueue.pop`` fires :meth:`on_pop` on every
    call — including empty pops — because the consumer-liveness signal is
    "is the worker popping at all?", independent of whether an item was
    returned. The stat name now matches the per-attempt behavior.
  - ``dupefilter/hit_count`` (counter) — per duplicate request.
  - ``dupefilter/miss_count`` (counter) — per newly-seen request.
  - ``queue/depth`` (gauge) — last-sampled pending depth.
  - ``pipeline/store_count`` (counter) — per successful store.
  - ``errors/<operation>`` (counter) — per operation error. Wired (R14-D) at
    the ``BackendQueue`` push-except and deserialize-fail arms.
  - ``queue/backpressure`` (gauge) — set to the sampled ``depth`` when it
    exceeds ``backpressure_threshold``; reset to ``0`` once depth drops back
    under. Set to an int on EVERY depth sample (``0`` under threshold) — it
    is ``None`` only before the first sample, never "until the threshold is
    crossed" (the gauge follows depth, not threshold-crossing history).
  - ``queue/pop_rate_1m`` (gauge) — rolling pops/sec over the trailing 60s
    window (U2 operability). Sampled on the same cadence as the pop-path
    depth probe; falling-edge to ~0 = stalled consumer.
  - ``dupefilter/filter_saturation`` (gauge, 0.0-1.0) — filter fill ratio
    (U2 operability). Rises through 0.9 before ``dupefilter/filter_full``
    ever fires; leading indicator for raising filter capacity. ``0.0`` when
    the filter is unbounded or reports no capacity. Emitted by cuckoo and
    bloom filters (via :meth:`BackendDupeFilter.request_seen`) and by a bounded
    memory filter when it first reaches its cap and during later evictions.
  - ``backend/connect_count`` (counter) — per successful backend connect
    (R14-D connection-lifecycle). Wired from ``ConnectionManager.connect``.
  - ``backend/disconnect_count`` (counter) — per backend disconnect
    (R14-D connection-lifecycle). Wired from ``ConnectionManager.close``.
  - ``backend/disconnect_success_count`` (counter) — per successful final
    backend disconnect.
  - ``backend/disconnect_failure_count`` (counter) — per failed final backend
    disconnect; records no exception details.
  - ``backend/retry_count`` (counter) — per connection retry
    (R14-D connection-lifecycle). Wired from ``ConnectionManager.connect``.

  All hooks are ``@_stats_safe`` — a failing StatsCollector is swallowed +
  logged at debug, never propagated into the data path (R5).

  Attributes:
      _stats: The wrapped Scrapy StatsCollector.
      backpressure_threshold: Depth above which ``queue/backpressure``
          flips on. ``DEFAULT_BACKPRESSURE_THRESHOLD`` by default.
  """

  def __init__(
    self,
    stats: StatsCollector,
    *,
    backpressure_threshold: int = DEFAULT_BACKPRESSURE_THRESHOLD,
    pop_rate_window_s: float = DEFAULT_POP_RATE_WINDOW_S,
  ) -> None:
    """Initialize the monitor.

    Args:
        stats: Scrapy StatsCollector (e.g. ``crawler.stats``).
        backpressure_threshold: Depth above which ``queue/backpressure``
            is set. See :data:`DEFAULT_BACKPRESSURE_THRESHOLD`.
        pop_rate_window_s: Trailing window (seconds) the ``queue/pop_rate_1m``
            gauge is computed over. See :data:`DEFAULT_POP_RATE_WINDOW_S`.
            Round-14 R14-C: threaded via ``BackendScheduler.from_settings``
            so operators can tune the window without code changes (round-12
            U2 left it stuck at the constructor default).
    """
    self._stats = stats
    self.backpressure_threshold = backpressure_threshold
    self.pop_rate_window_s = pop_rate_window_s

  @_stats_safe
  def on_push(self, queue_name: str, priority: float) -> None:
    """Increment ``queue/push_count``."""
    self._stats.inc_value("queue/push_count")

  @_stats_safe
  def on_pop(self, queue_name: str) -> None:
    """Increment ``queue/pop_attempt_count`` (R14-D rename — per attempt).

    ``BackendQueue.pop`` fires this on every call — including empty pops —
    because the consumer-liveness signal is "is the worker popping at all?",
    independent of whether an item was returned. The stat key was renamed
    from ``queue/pop_count`` so the name matches the per-attempt behavior.

    Backward-compat: the legacy ``queue/pop_count`` key is ALSO incremented
    so existing dashboards and the out-of-scope test suite keep working
    during the rename window. The legacy key is documented as deprecated in
    favor of ``queue/pop_attempt_count`` and may be dropped at the next
    major.
    """
    self._stats.inc_value("queue/pop_attempt_count")
    # Legacy alias — preserved for backward compat with existing dashboards
    # and the pre-rename test suite. Deprecated in favor of the renamed key.
    self._stats.inc_value("queue/pop_count")

  @_stats_safe
  def on_dedup_hit(self, key: str) -> None:
    """Increment ``dupefilter/hit_count``."""
    self._stats.inc_value("dupefilter/hit_count")

  @_stats_safe
  def on_dedup_miss(self, key: str) -> None:
    """Increment ``dupefilter/miss_count``."""
    self._stats.inc_value("dupefilter/miss_count")

  @_stats_safe
  def on_queue_depth(self, queue_name: str, depth: int) -> None:
    """Set the ``queue/depth`` gauge and update ``queue/backpressure``.

    ``queue/depth`` is always set to the sampled depth (gauge semantics).

    ``queue/backpressure`` follows the depth: when ``depth >
    backpressure_threshold`` it's set to ``depth`` (the alert is on and
    shows the worst observed depth since it last cleared); when depth
    drops back to or below the threshold it's reset to ``0`` so the alert
    clears cleanly for operators. Observability only — no throttling
    action is taken here (a later tier may consume this stat to apply
    backpressure).
    """
    self._stats.set_value("queue/depth", depth)
    if depth > self.backpressure_threshold:
      self._stats.set_value("queue/backpressure", depth)
    else:
      self._stats.set_value("queue/backpressure", 0)

  @_stats_safe
  def on_store(self, key: str) -> None:
    """Increment ``pipeline/store_count``.

    The item pipeline itself is implemented in another lane; this hook is
    defined so the pipeline can emit through the same monitor interface.
    """
    self._stats.inc_value("pipeline/store_count")

  @_stats_safe
  def on_filter_full(self) -> None:
    """Increment ``dupefilter/filter_full``.

    The dupefilter emits this when a bounded-capacity membership filter
    (cuckoo) reports it is full and the dupefilter degrades by allowing the
    overflow request through. Counts every occurrence; the dupefilter
    additionally warns once per process via its own logger.
    """
    self._stats.inc_value("dupefilter/filter_full")

  @_stats_safe
  def on_pop_rate(self, window_s: float, rate: float) -> None:
    """Set the ``queue/pop_rate_1m`` gauge.

    The stat key is window-tagged: ``queue/pop_rate_1m`` at the default 60s
    window, or ``queue/pop_rate_{N}s`` when
    ``SCRAPY_MONITOR_POP_RATE_WINDOW_S`` is overridden. ``BackendQueue``
    forwards its configured ``pop_rate_window_s`` here, so the key always
    reflects the window an operator is looking at on the stats dump. ``rate``
    is pops per second over that trailing window.

    Args:
        window_s: Trailing window the rate was computed over (seconds).
            Recorded in the stat key (``1m`` for the default 60s).
        rate: Pops per second over ``window_s``.
    """
    tag = "1m" if window_s == DEFAULT_POP_RATE_WINDOW_S else f"{window_s:g}s"
    self._stats.set_value(f"queue/pop_rate_{tag}", rate)

  @_stats_safe
  def on_filter_saturation(self, used: int, capacity: int | None) -> None:
    """Set the ``dupefilter/filter_saturation`` gauge (0.0-1.0).

    Saturation is ``used / capacity`` clamped to ``[0.0, 1.0]``. An unbounded
    filter (``capacity is None``) reports ``0.0`` — it cannot be saturated, so
    the gauge stays at the floor and operators are not misled by a stale
    nonzero reading. Cuckoo and Bloom filters report after each deduplication
    decision. Bounded Memory filters report only when a successful insert
    reaches or remains at the capacity ceiling (including eviction); duplicate
    hits stay silent. Unsupported filters leave the gauge untouched.

    Args:
        used: Items currently recorded in the filter.
        capacity: Filter capacity in items, or ``None`` if unbounded.
    """
    if capacity is None or capacity <= 0:
      ratio = 0.0
    else:
      ratio = min(1.0, max(0.0, used / capacity))
    self._stats.set_value("dupefilter/filter_saturation", ratio)

  @_stats_safe
  def on_error(self, operation: str, error: BaseException) -> None:
    """Increment ``errors/<operation>``.

    Args:
        operation: Short operation tag (``"push"``, ``"pop"``, ...).
        error: The exception (recorded by count; the message is not
            emitted to stats — log it separately if needed).
    """
    self._stats.inc_value(f"errors/{operation}")

  @_stats_safe
  def on_connect(self, backend_type: str) -> None:
    """Increment ``backend/connect_count`` (R14-D connection-lifecycle).

    Args:
        backend_type: The backend-type registry string that connected.
    """
    self._stats.inc_value("backend/connect_count")

  @_stats_safe
  def on_disconnect(self, backend_type: str, reason: str | None) -> None:
    """Increment ``backend/disconnect_count`` (R14-D connection-lifecycle).

    Args:
        backend_type: The backend-type registry string that disconnected.
        reason: Scrapy engine close reason (or ``None``).
    """
    self._stats.inc_value("backend/disconnect_count")

  @_stats_safe
  def on_disconnect_result(self, backend_type: str, succeeded: bool) -> None:
    """Increment the fixed success or failure disconnect counter.

    Args:
        backend_type: The backend-type registry string that disconnected.
        succeeded: Whether ``backend.disconnect()`` returned successfully.
    """
    del backend_type
    stat_key = (
      "backend/disconnect_success_count"
      if succeeded
      else "backend/disconnect_failure_count"
    )
    self._stats.inc_value(stat_key)

  @_stats_safe
  def on_retry(self, backend_type: str, attempt: int) -> None:
    """Increment ``backend/retry_count`` (R14-D connection-lifecycle).

    Args:
        backend_type: The backend-type registry string being retried.
        attempt: 1-based retry index (1 = first retry).
    """
    self._stats.inc_value("backend/retry_count")

  @_stats_safe
  def on_buffer_depth(self, depth: int) -> None:
    """Set the ``pipeline/buffer_depth`` gauge (batched-storage operability).

    Lets operators alert before the crash-before-flush loss window grows.
    ``depth`` is the number of accepted-but-not-yet-persisted items in the
    :class:`BatchedStorageStrategy`, including any in-flight backend snapshot.

    Args:
        depth: Number of accepted-but-not-yet-persisted items.
    """
    self._stats.set_value("pipeline/buffer_depth", depth)

  @_stats_safe
  def on_delay_depth(self, depth: int) -> None:
    """Set the ``queue/delay_depth`` gauge (delay-strategy operability).

    Lets operators alert before the in-process delay heap grows unbounded
    (the held-delay state is in-process and lost on crash).

    Args:
        depth: Number of items currently held in the delay heap.
    """
    self._stats.set_value("queue/delay_depth", depth)
