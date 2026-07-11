"""Scrapy-stats-backed monitor (Unit F — Tier-2).

:class:`ScrapyStatsMonitor` turns monitor hooks into namespaced Scrapy stats
so operators see queue / dedup / error telemetry on the standard Scrapy stats
dump. It is the concrete monitor wired by ``from_crawler`` factories whenever
``crawler.stats`` is available; otherwise components fall back to
:class:`~scrapy_extension.monitor.base.NullMonitor`.
"""

from __future__ import annotations

__all__ = ["ScrapyStatsMonitor"]

from typing import TYPE_CHECKING

from scrapy_extension.monitor.base import (
  DEFAULT_BACKPRESSURE_THRESHOLD,
  DEFAULT_POP_RATE_WINDOW_S,
  Monitor,
)

if TYPE_CHECKING:
  from scrapy.statscollectors import StatsCollector


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
  - ``queue/backpressure`` (gauge) — current depth when it last exceeded
    ``backpressure_threshold``; reset to ``0`` once depth drops back under.
    ``None`` until the threshold has ever been crossed.
  - ``queue/pop_rate_1m`` (gauge) — rolling pops/sec over the trailing 60s
    window (U2 operability). Sampled on the same cadence as the pop-path
    depth probe; falling-edge to ~0 = stalled consumer.
  - ``dupefilter/filter_saturation`` (gauge, 0.0-1.0) — filter fill ratio
    (U2 operability). Rises through 0.9 before ``dupefilter/filter_full``
    ever fires; leading indicator for raising filter capacity. ``0.0`` when
    the filter is unbounded or reports no capacity. Emitted by cuckoo and
    bloom filters (via :meth:`BackendDupeFilter.request_seen`) and by the
    memory filter at LRU-eviction time.
  - ``backend/connect_count`` (counter) — per successful backend connect
    (R14-D connection-lifecycle). Wired from ``ConnectionManager.connect``.
  - ``backend/disconnect_count`` (counter) — per backend disconnect
    (R14-D connection-lifecycle). Wired from ``ConnectionManager.close``.
  - ``backend/retry_count`` (counter) — per connection retry
    (R14-D connection-lifecycle). Wired from ``ConnectionManager.connect``.

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
        pop_rate_window_s: Trailing window (seconds) the ``queue/pop_rate``
            gauge is computed over. See :data:`DEFAULT_POP_RATE_WINDOW_S`.
            Round-14 R14-C: threaded via ``BackendScheduler.from_settings``
            so operators can tune the window without code changes (round-12
            U2 left it stuck at the constructor default).
    """
    self._stats = stats
    self.backpressure_threshold = backpressure_threshold
    self.pop_rate_window_s = pop_rate_window_s

  def on_push(self, queue_name: str, priority: float) -> None:
    """Increment ``queue/push_count``."""
    self._stats.inc_value("queue/push_count")

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

  def on_dedup_hit(self, key: str) -> None:
    """Increment ``dupefilter/hit_count``."""
    self._stats.inc_value("dupefilter/hit_count")

  def on_dedup_miss(self, key: str) -> None:
    """Increment ``dupefilter/miss_count``."""
    self._stats.inc_value("dupefilter/miss_count")

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

  def on_store(self, key: str) -> None:
    """Increment ``pipeline/store_count``.

    The item pipeline itself is implemented in another lane; this hook is
    defined so the pipeline can emit through the same monitor interface.
    """
    self._stats.inc_value("pipeline/store_count")

  def on_filter_full(self) -> None:
    """Increment ``dupefilter/filter_full``.

    The dupefilter emits this when a bounded-capacity membership filter
    (cuckoo) reports it is full and the dupefilter degrades by allowing the
    overflow request through. Counts every occurrence; the dupefilter
    additionally warns once per process via its own logger.
    """
    self._stats.inc_value("dupefilter/filter_full")

  def on_pop_rate(self, window_s: float, rate: float) -> None:
    """Set the ``queue/pop_rate_1m`` gauge.

    The window tag is fixed at ``1m`` because :data:`DEFAULT_POP_RATE_WINDOW_S`
    is 60s and ``BackendQueue`` always passes that window; the stat key name
    documents the window an operator is looking at on the stats dump. ``rate``
    is pops per second over that trailing window.

    Args:
        window_s: Trailing window the rate was computed over (seconds).
            Recorded in the stat key (``1m`` for the default 60s).
        rate: Pops per second over ``window_s``.
    """
    tag = "1m" if window_s == DEFAULT_POP_RATE_WINDOW_S else f"{window_s:g}s"
    self._stats.set_value(f"queue/pop_rate_{tag}", rate)

  def on_filter_saturation(self, used: int, capacity: int | None) -> None:
    """Set the ``dupefilter/filter_saturation`` gauge (0.0-1.0).

    Saturation is ``used / capacity`` clamped to ``[0.0, 1.0]``. An unbounded
    filter (``capacity is None``) reports ``0.0`` — it cannot be saturated, so
    the gauge stays at the floor and operators are not misled by a stale
    nonzero reading. The dupefilter emits this after each add when the
    underlying filter exposes a ``saturation`` property (cuckoo only); other
    filters never emit, leaving the gauge at ``None`` (untouched).

    Args:
        used: Items currently recorded in the filter.
        capacity: Filter capacity in items, or ``None`` if unbounded.
    """
    if capacity is None or capacity <= 0:
      ratio = 0.0
    else:
      ratio = min(1.0, max(0.0, used / capacity))
    self._stats.set_value("dupefilter/filter_saturation", ratio)

  def on_error(self, operation: str, error: BaseException) -> None:
    """Increment ``errors/<operation>``.

    Args:
        operation: Short operation tag (``"push"``, ``"pop"``, ...).
        error: The exception (recorded by count; the message is not
            emitted to stats — log it separately if needed).
    """
    self._stats.inc_value(f"errors/{operation}")

  def on_connect(self, backend_type: str) -> None:
    """Increment ``backend/connect_count`` (R14-D connection-lifecycle).

    Args:
        backend_type: The backend-type registry string that connected.
    """
    self._stats.inc_value("backend/connect_count")

  def on_disconnect(self, backend_type: str, reason: str | None) -> None:
    """Increment ``backend/disconnect_count`` (R14-D connection-lifecycle).

    Args:
        backend_type: The backend-type registry string that disconnected.
        reason: Scrapy engine close reason (or ``None``).
    """
    self._stats.inc_value("backend/disconnect_count")

  def on_retry(self, backend_type: str, attempt: int) -> None:
    """Increment ``backend/retry_count`` (R14-D connection-lifecycle).

    Args:
        backend_type: The backend-type registry string being retried.
        attempt: 1-based retry index (1 = first retry).
    """
    self._stats.inc_value("backend/retry_count")

  def on_buffer_depth(self, depth: int) -> None:
    """Set the ``pipeline/buffer_depth`` gauge (batched-storage operability).

    Lets operators alert before the crash-before-flush loss window grows.
    ``depth`` is the number of items currently buffered in the
    :class:`BatchedStorageStrategy`, pending flush.

    Args:
        depth: Number of items currently buffered, pending flush.
    """
    self._stats.set_value("pipeline/buffer_depth", depth)

  def on_delay_depth(self, depth: int) -> None:
    """Set the ``queue/delay_depth`` gauge (delay-strategy operability).

    Lets operators alert before the in-process delay heap grows unbounded
    (the held-delay state is in-process and lost on crash).

    Args:
        depth: Number of items currently held in the delay heap.
    """
    self._stats.set_value("queue/delay_depth", depth)
