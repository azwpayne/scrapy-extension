"""Tests for the observability namespace (Unit F — Tier-2).

Covers:
- ``Monitor`` protocol shape + ``NullMonitor`` no-op default.
- ``ScrapyStatsMonitor`` stat emission per hook.
- Additive wiring into ``BackendQueue`` and ``BackendDupeFilter``.
- Default-on resolution: ``from_crawler`` wires ScrapyStatsMonitor;
  construction without a crawler falls back to NullMonitor (no crash).
- Backpressure signal: ``on_queue_depth`` above threshold sets
  ``queue/backpressure``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from scrapy import Spider
from scrapy.http import Request
from scrapy.statscollectors import MemoryStatsCollector

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.monitor import (
  Monitor,
  NullMonitor,
  ScrapyStatsMonitor,
)
from scrapy_extension.monitor.base import DEFAULT_BACKPRESSURE_THRESHOLD
from scrapy_extension.queue.queue import BackendQueue


def _stats() -> MemoryStatsCollector:
  """Build a MemoryStatsCollector.

  Scrapy's StatsCollector requires a crawler in this version; a mock crawler
  is sufficient — the collector only stores the reference.
  """
  return MemoryStatsCollector(MagicMock())


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


class TestNullMonitor:
  """NullMonitor is the safe default — every hook is a no-op."""

  @pytest.mark.parametrize(
    "hook,kwargs",
    [
      ("on_push", {"queue_name": "q", "priority": 1.0}),
      ("on_pop", {"queue_name": "q"}),
      ("on_dedup_hit", {"key": "fp"}),
      ("on_dedup_miss", {"key": "fp"}),
      ("on_queue_depth", {"queue_name": "q", "depth": 5}),
      ("on_store", {"key": "k"}),
      ("on_filter_full", {}),
      ("on_error", {"operation": "push", "error": RuntimeError("x")}),
    ],
  )
  def test_hook_is_noop(self, hook, kwargs):
    """Each NullMonitor hook accepts its args and returns None."""
    monitor: Monitor = NullMonitor()
    result = getattr(monitor, hook)(**kwargs)
    assert result is None

  def test_null_monitor_satisfies_protocol(self):
    """NullMonitor is structurally a Monitor (duck-typed)."""
    monitor: Monitor = NullMonitor()
    for hook in (
      "on_push",
      "on_pop",
      "on_dedup_hit",
      "on_dedup_miss",
      "on_queue_depth",
      "on_store",
      "on_filter_full",
      "on_error",
    ):
      assert callable(getattr(monitor, hook))


# ---------------------------------------------------------------------------
# ScrapyStatsMonitor — per-hook stat emission
# ---------------------------------------------------------------------------


class TestScrapyStatsMonitor:
  """ScrapyStatsMonitor increments the right namespaced stat per hook."""

  def _monitor(self) -> tuple[ScrapyStatsMonitor, MemoryStatsCollector]:
    stats = _stats()
    return ScrapyStatsMonitor(stats), stats

  def test_on_push_increments_push_count(self):
    monitor, stats = self._monitor()
    monitor.on_push("q", priority=2.0)
    assert stats.get_value("queue/push_count") == 1
    monitor.on_push("q", priority=0.0)
    assert stats.get_value("queue/push_count") == 2

  def test_on_pop_increments_pop_count(self):
    monitor, stats = self._monitor()
    monitor.on_pop("q")
    assert stats.get_value("queue/pop_count") == 1

  def test_on_dedup_hit_increments_hit_count(self):
    monitor, stats = self._monitor()
    monitor.on_dedup_hit("fp1")
    assert stats.get_value("dupefilter/hit_count") == 1

  def test_on_dedup_miss_increments_miss_count(self):
    monitor, stats = self._monitor()
    monitor.on_dedup_miss("fp1")
    assert stats.get_value("dupefilter/miss_count") == 1

  def test_on_queue_depth_sets_gauge(self):
    """on_queue_depth is a gauge (set), not a counter."""
    monitor, stats = self._monitor()
    monitor.on_queue_depth("q", depth=42)
    assert stats.get_value("queue/depth") == 42
    monitor.on_queue_depth("q", depth=7)
    assert stats.get_value("queue/depth") == 7  # set, not incremented

  def test_on_store_increments_store_count(self):
    """Pipeline emits on_store; monitor records it (pipeline itself is another lane)."""
    monitor, stats = self._monitor()
    monitor.on_store("k")
    assert stats.get_value("pipeline/store_count") == 1

  def test_on_filter_full_increments_filter_full_stat(self):
    """Dupefilter emits on_filter_full when a cuckoo filter hits capacity."""
    monitor, stats = self._monitor()
    monitor.on_filter_full()
    assert stats.get_value("dupefilter/filter_full") == 1
    monitor.on_filter_full()
    assert stats.get_value("dupefilter/filter_full") == 2

  def test_on_error_increments_per_operation_stat(self):
    monitor, stats = self._monitor()
    monitor.on_error("push", RuntimeError("boom"))
    assert stats.get_value("errors/push") == 1
    monitor.on_error("pop", ValueError("x"))
    assert stats.get_value("errors/pop") == 1
    assert stats.get_value("errors/push") == 1  # unchanged


# ---------------------------------------------------------------------------
# Backpressure signal
# ---------------------------------------------------------------------------


class TestBackpressure:
  """on_queue_depth above threshold sets queue/backpressure gauge."""

  def test_depth_above_threshold_sets_backpressure(self):
    stats = _stats()
    monitor = ScrapyStatsMonitor(stats, backpressure_threshold=10)
    monitor.on_queue_depth("q", depth=11)
    assert stats.get_value("queue/backpressure") == 11

  def test_depth_at_threshold_does_not_set_backpressure(self):
    """Backpressure is strictly-greater-than (depth > threshold).

    At exactly the threshold the gauge reads ``0`` (no backpressure), not the
    depth — so an operator alerting on ``queue/backpressure > 0`` stays quiet.
    """
    stats = _stats()
    monitor = ScrapyStatsMonitor(stats, backpressure_threshold=10)
    monitor.on_queue_depth("q", depth=10)
    assert stats.get_value("queue/backpressure") == 0

  def test_backpressure_clears_when_depth_drops_below_threshold(self):
    """Backpressure is a gauge — once depth drops, the signal reflects it.

    Depth above threshold sets the gauge to the depth value. When depth
    later drops below threshold, the gauge is reset to 0 so operators
    see the alert clear (no stale backpressure flag).
    """
    stats = _stats()
    monitor = ScrapyStatsMonitor(stats, backpressure_threshold=10)
    monitor.on_queue_depth("q", depth=20)
    assert stats.get_value("queue/backpressure") == 20
    monitor.on_queue_depth("q", depth=3)
    assert stats.get_value("queue/backpressure") == 0

  def test_default_threshold_is_finite(self):
    """The default threshold is a sane finite number, not infinity.

    Guards against accidentally disabling backpressure by default.
    """
    assert DEFAULT_BACKPRESSURE_THRESHOLD > 0
    assert DEFAULT_BACKPRESSURE_THRESHOLD != float("inf")


# ---------------------------------------------------------------------------
# Wiring — BackendQueue emits on push/pop
# ---------------------------------------------------------------------------


class TestBackendQueueMonitorWiring:
  """BackendQueue emits monitor hooks additively (existing stats unchanged)."""

  def test_push_emits_on_push(self, mock_connection_manager):
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
    )
    queue.push(Request(url="https://example.com"))
    assert monitor._stats.get_value("queue/push_count") == 1  # type: ignore[attr-defined]

  def test_pop_emits_on_pop(self, mock_connection_manager):
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue.pop()
    assert monitor._stats.get_value("queue/pop_count") == 1  # type: ignore[attr-defined]

  def test_pop_emits_on_queue_depth(self, mock_connection_manager):
    """After pop, the monitor receives the current depth (backpressure signal)."""
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None
    mock_connection_manager.get_queue_backend().queue_len.return_value = 5
    queue.pop()
    assert monitor._stats.get_value("queue/depth") == 5

  def test_default_monitor_is_null_no_crash(self, mock_connection_manager):
    """No explicit monitor + no crawler → NullMonitor, push/pop still work."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
    )
    queue.push(Request(url="https://example.com"))
    mock_connection_manager.get_queue_backend().push.assert_called_once()

  def test_wiring_resolves_monitor_from_spider_crawler(self, mock_connection_manager, mocker):
    """Default-on: when spider.crawler.stats exists, BackendQueue auto-wires a
    ScrapyStatsMonitor (no explicit monitor= kwarg required).

    This is the path exercised in production — the scheduler constructs
    BackendQueue(spider=spider) without a monitor; we resolve one from
    spider.crawler.stats so observability is default-on.
    """
    stats = _stats()
    # Unspec'd Mock (not spec=Spider): Spider has no ``crawler`` attribute on
    # its class, so ``spec=Spider`` would block setting it. A real spider
    # instance gets ``crawler`` assigned by Scrapy at runtime, not via the
    # class. The Tier-1 oversize test uses the same unspec'd pattern.
    spider = mocker.Mock()
    spider.crawler.stats = stats
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      spider=spider,
    )
    queue.push(Request(url="https://example.com"))
    assert stats.get_value("queue/push_count") == 1

  def test_no_crawler_falls_back_to_null(self, mock_connection_manager, mocker):
    """spider without crawler → NullMonitor, no crash, no stats."""
    spider = mocker.MagicMock(spec=Spider)
    # Spider spec has no crawler attr by default; delattr-style via configure_mock
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      spider=spider,
    )
    # Must not raise; monitor is NullMonitor.
    assert isinstance(queue._monitor, NullMonitor)
    queue.push(Request(url="https://example.com"))

  def test_existing_stats_keys_unchanged(self, mock_connection_manager, mocker):
    """Additive wiring: Tier-1 oversize stat still fires alongside new stats."""
    stats = _stats()
    spider = mocker.Mock()
    spider.crawler.stats = stats
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      spider=spider,
      max_item_bytes=64,
    )
    with pytest.raises(Exception, match="exceeds.*max"):
      queue.push(Request(url="https://example.com", body=b"x" * 200))
    # Tier-1 stat preserved
    assert stats.get_value("scheduler/queue/oversize_dropped") == 1


# ---------------------------------------------------------------------------
# Wiring — BackendDupeFilter emits on hit/miss
# ---------------------------------------------------------------------------


class TestDupeFilterMonitorWiring:
  """BackendDupeFilter emits on_dedup_hit / on_dedup_miss."""

  def test_request_seen_emits_hit_on_duplicate(
    self, mock_connection_manager
  ):
    monitor = ScrapyStatsMonitor(_stats())
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    # SetBackend.add returns False → seen → on_dedup_hit
    mock_connection_manager.get_set_backend().add.return_value = False
    request = Request(url="https://example.com")
    seen = df.request_seen(request)
    assert seen is True
    assert monitor._stats.get_value("dupefilter/hit_count") == 1  # type: ignore[attr-defined]
    assert monitor._stats.get_value("dupefilter/miss_count") is None  # type: ignore[attr-defined]

  def test_request_seen_emits_miss_on_new(
    self, mock_connection_manager
  ):
    monitor = ScrapyStatsMonitor(_stats())
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    # SetBackend.add returns True → newly added → not seen → on_dedup_miss
    mock_connection_manager.get_set_backend().add.return_value = True
    request = Request(url="https://example.com")
    seen = df.request_seen(request)
    assert seen is False
    assert monitor._stats.get_value("dupefilter/miss_count") == 1  # type: ignore[attr-defined]
    assert monitor._stats.get_value("dupefilter/hit_count") is None  # type: ignore[attr-defined]

  def test_default_monitor_is_null_no_crash(self, mock_connection_manager):
    """No monitor → NullMonitor, request_seen still works."""
    df = BackendDupeFilter(connection_manager=mock_connection_manager)
    mock_connection_manager.get_set_backend().add.return_value = True
    df.request_seen(Request(url="https://example.com"))  # must not raise


# ---------------------------------------------------------------------------
# Default-on via from_crawler
# ---------------------------------------------------------------------------


class TestFromCrawlerWiring:
  """from_crawler wires ScrapyStatsMonitor when crawler.stats is present."""

  def test_dupefilter_from_crawler_wires_stats_monitor(self, mocker):
    """BackendDupeFilter.from_crawler attaches a ScrapyStatsMonitor."""
    from scrapy_extension.backends.connectors import ConnectionManager

    ConnectionManager.clear_registry()
    # Minimal settings dict for resolve_backend_config (Redis default).
    settings_dict = {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_BACKEND_SETTINGS": {"host": "localhost", "port": 6379},
    }
    from scrapy.settings import Settings as ScrapySettings

    settings = ScrapySettings()
    settings.setdict(settings_dict)

    crawler = mocker.MagicMock()
    crawler.settings = settings
    crawler.stats = _stats()
    crawler.request_fingerprinter = None

    df = BackendDupeFilter.from_crawler(crawler)
    assert isinstance(df._monitor, ScrapyStatsMonitor)
