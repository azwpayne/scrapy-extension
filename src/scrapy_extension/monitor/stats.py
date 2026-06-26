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
  Monitor,
)

if TYPE_CHECKING:
  from scrapy.statscollectors import StatsCollector


class ScrapyStatsMonitor(Monitor):
  """Monitor that increments namespaced Scrapy stats.

  Stat keys (all additive — existing component stats are untouched):

  - ``queue/push_count`` (counter) — per successful push.
  - ``queue/pop_count`` (counter) — per successful pop.
  - ``dupefilter/hit_count`` (counter) — per duplicate request.
  - ``dupefilter/miss_count`` (counter) — per newly-seen request.
  - ``queue/depth`` (gauge) — last-sampled pending depth.
  - ``pipeline/store_count`` (counter) — per successful store.
  - ``errors/<operation>`` (counter) — per operation error.
  - ``queue/backpressure`` (gauge) — current depth when it last exceeded
    ``backpressure_threshold``; reset to ``0`` once depth drops back under.
    ``None`` until the threshold has ever been crossed.

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
  ) -> None:
    """Initialize the monitor.

    Args:
        stats: Scrapy StatsCollector (e.g. ``crawler.stats``).
        backpressure_threshold: Depth above which ``queue/backpressure``
            is set. See :data:`DEFAULT_BACKPRESSURE_THRESHOLD`.
    """
    self._stats = stats
    self.backpressure_threshold = backpressure_threshold

  def on_push(self, queue_name: str, priority: float) -> None:
    """Increment ``queue/push_count``."""
    self._stats.inc_value("queue/push_count")

  def on_pop(self, queue_name: str) -> None:
    """Increment ``queue/pop_count``."""
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

  def on_error(self, operation: str, error: BaseException) -> None:
    """Increment ``errors/<operation>``.

    Args:
        operation: Short operation tag (``"push"``, ``"pop"``, ...).
        error: The exception (recorded by count; the message is not
            emitted to stats — log it separately if needed).
    """
    self._stats.inc_value(f"errors/{operation}")
