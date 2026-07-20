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
from scrapy_extension.dupefilter.filters.cuckoo_filter import (
  CuckooMembershipFilter,
)
from scrapy_extension.exceptions import BackendConnectionError, SerializationError
from scrapy_extension.monitor import (
  Monitor,
  NullMonitor,
  ScrapyStatsMonitor,
)
from scrapy_extension.monitor.base import (
  DEFAULT_BACKPRESSURE_THRESHOLD,
  DEFAULT_POP_RATE_WINDOW_S,
)
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
      ("on_pop_rate", {"window_s": DEFAULT_POP_RATE_WINDOW_S, "rate": 1.5}),
      ("on_filter_saturation", {"used": 100, "capacity": 200}),
      ("on_error", {"operation": "push", "error": RuntimeError("x")}),
      ("on_connect", {"backend_type": "redis"}),
      ("on_disconnect", {"backend_type": "redis", "reason": "shutdown"}),
      ("on_retry", {"backend_type": "redis", "attempt": 1}),
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
      "on_pop_rate",
      "on_filter_saturation",
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
    # R14-D: renamed key (behavior-matching — per attempt).
    assert stats.get_value("queue/pop_attempt_count") == 1
    # Legacy alias preserved for backward compat (deprecated next major).
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

  def test_on_pop_rate_sets_1m_gauge(self):
    """on_pop_rate is a gauge (set), tagged ``1m`` for the default 60s window."""
    monitor, stats = self._monitor()
    monitor.on_pop_rate(DEFAULT_POP_RATE_WINDOW_S, 12.5)
    assert stats.get_value("queue/pop_rate_1m") == 12.5
    monitor.on_pop_rate(DEFAULT_POP_RATE_WINDOW_S, 3.0)
    assert stats.get_value("queue/pop_rate_1m") == 3.0  # set, not incremented

  def test_on_pop_rate_non_default_window_tags_value(self):
    """A non-default window length is reflected in the stat tag, not the key."""
    monitor, stats = self._monitor()
    monitor.on_pop_rate(30.0, 5.0)
    assert stats.get_value("queue/pop_rate_30s") == 5.0
    # default-window key must NOT be touched when a different window is passed
    assert stats.get_value("queue/pop_rate_1m") is None

  def test_on_filter_saturation_sets_ratio_gauge(self):
    """on_filter_saturation stores used/capacity clamped to [0, 1]."""
    monitor, stats = self._monitor()
    monitor.on_filter_saturation(used=90, capacity=100)
    assert stats.get_value("dupefilter/filter_saturation") == pytest.approx(0.9)

  def test_on_filter_saturation_clamps_above_one(self):
    """used > capacity (overflow before FilterFull) clamps to 1.0, not >1."""
    monitor, stats = self._monitor()
    monitor.on_filter_saturation(used=150, capacity=100)
    assert stats.get_value("dupefilter/filter_saturation") == 1.0

  def test_on_filter_saturation_zero_when_capacity_none(self):
    """Unbounded filter (capacity is None) reports 0.0 — it cannot saturate."""
    monitor, stats = self._monitor()
    monitor.on_filter_saturation(used=1_000_000, capacity=None)
    assert stats.get_value("dupefilter/filter_saturation") == 0.0

  def test_on_filter_saturation_zero_when_capacity_zero(self):
    """Defensive: capacity <= 0 reports 0.0 (no division-by-zero)."""
    monitor, stats = self._monitor()
    monitor.on_filter_saturation(used=5, capacity=0)
    assert stats.get_value("dupefilter/filter_saturation") == 0.0

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


# ---------------------------------------------------------------------------
# U2 — Operability signals (rolling pop rate + filter saturation)
# ---------------------------------------------------------------------------


class TestPopRateEmission:
  """BackendQueue.pop emits on_pop_rate on the depth-sample cadence (U2)."""

  def test_pop_rate_set_after_sampling_window(self, mock_connection_manager):
    """After ``depth_sample_every`` pops, the rolling rate is emitted once.

    Drives 100 pops (default ``depth_sample_every=100``) within a mocked
    sub-second window and asserts ``queue/pop_rate_1m`` is set to ~N/60.
    The rate reflects pop ATTEMPTS per second (matches ``on_pop`` semantics),
    independent of whether an item was returned.
    """
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None
    for _ in range(100):
      queue.pop()
    rate = monitor._stats.get_value("queue/pop_rate_1m")  # type: ignore[attr-defined]
    # 100 pops over a 60s window → ~1.667 pops/sec; allow a wide tolerance
    # since the window is monotonic and the test runs in well under 60s
    # (so all 100 timestamps are inside the window).
    assert rate is not None
    assert 1.0 <= rate <= 2.5

  def test_pop_rate_not_emitted_before_sampling_window(
    self, mock_connection_manager
  ):
    """Before the sampling cadence elapses, the rate gauge stays untouched.

    Guards against per-pop stat RPCs (the perf discipline from U4). With
    ``depth_sample_every=100`` and only 50 pops, no rate emission fires.
    """
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
      depth_sample_every=100,
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None
    for _ in range(50):
      queue.pop()
    assert monitor._stats.get_value("queue/pop_rate_1m") is None  # type: ignore[attr-defined]

  def test_pop_rate_emits_on_custom_cadence(self, mock_connection_manager):
    """A small ``depth_sample_every`` emits the rate on every pop (smoke)."""
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
      depth_sample_every=1,
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue.pop()
    rate = monitor._stats.get_value("queue/pop_rate_1m")  # type: ignore[attr-defined]
    assert rate is not None
    assert rate > 0.0

  def test_pop_rate_window_evicts_old_timestamps(
    self, mock_connection_manager, mocker
  ):
    """Aged timestamps leave the window so a stalled consumer falls to ~0.

    Uses a controllable monotonic clock: drive pops at t=0, then advance the
    clock past the window. After the next pop, only timestamps inside the
    trailing window remain in the deque — the stale t=0 entries are evicted.
    With only the current pop inside the window, the rate drops to ~1/60
    (one pop in 60s), demonstrating the falling-edge operability signal a
    stalled consumer produces once it resumes.
    """
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
      depth_sample_every=1,
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None

    fixed = [0.0]

    def fake_monotonic() -> float:
      return fixed[0]

    mocker.patch(
      "scrapy_extension.queue.queue.time.monotonic", side_effect=fake_monotonic
    )
    for _ in range(100):
      queue.pop()
    # Sanity: 100 timestamps accumulated at t=0.
    assert len(queue._pop_timestamps) == 100
    # Advance past the 60s window and pop once more — eviction runs on the
    # next record_pop_timestamp; only the new (t=70) entry survives.
    fixed[0] = DEFAULT_POP_RATE_WINDOW_S + 10.0
    queue.pop()
    assert len(queue._pop_timestamps) == 1
    # Rate reflects exactly one pop inside the trailing 60s window.
    assert monitor._stats.get_value("queue/pop_rate_1m") == pytest.approx(  # type: ignore[attr-defined]
      1.0 / DEFAULT_POP_RATE_WINDOW_S
    )


class TestFilterSaturationEmission:
  """BackendDupeFilter emits on_filter_saturation for cuckoo filters (U2)."""

  def test_cuckoo_saturation_property_rises_with_load(self):
    """CuckooMembershipFilter.saturation = len / capacity, in [0, ~1]."""
    cuckoo = CuckooMembershipFilter(capacity=1_000, error_rate=0.01)
    assert cuckoo.saturation == 0.0
    # Add a batch of distinct items; saturation must rise monotonically.
    prev = 0.0
    for i in range(500):
      cuckoo.add(f"item-{i}".encode())
      s = cuckoo.saturation
      assert s >= prev
      prev = s
    # ~500 items inserted; saturation should be nonzero and < 1.0
    assert 0.0 < cuckoo.saturation < 1.0

  def test_request_seen_emits_saturation_for_cuckoo(
    self, mock_connection_manager
  ):
    """request_seen on a cuckoo-backed dupefilter emits on_filter_saturation."""
    monitor = ScrapyStatsMonitor(_stats())
    cuckoo = CuckooMembershipFilter(capacity=1_000, error_rate=0.01)
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      monitor=monitor,
      membership_filter=cuckoo,
    )
    df.request_seen(Request(url="https://example.com/a"))
    sat = monitor._stats.get_value("dupefilter/filter_saturation")  # type: ignore[attr-defined]
    assert sat is not None
    assert sat > 0.0
    # Saturation rises as more distinct items are added.
    df.request_seen(Request(url="https://example.com/b"))
    sat2 = monitor._stats.get_value("dupefilter/filter_saturation")  # type: ignore[attr-defined]
    assert sat2 > sat

  def test_request_seen_silent_when_filter_has_no_saturation(
    self, mock_connection_manager
  ):
    """Non-cuckoo filters (set/memory/bloom) do not emit on_filter_saturation.

    The gauge stays at ``None`` (untouched), not misleadingly at 0.0 —
    a set filter cannot be saturated, so operators don't get a noisy
    flat-zero gauge alongside the cuckoo signal.
    """
    monitor = ScrapyStatsMonitor(_stats())
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    mock_connection_manager.get_set_backend().add.return_value = True
    df.request_seen(Request(url="https://example.com"))
    assert monitor._stats.get_value("dupefilter/filter_saturation") is None  # type: ignore[attr-defined]

  def test_saturation_leading_indicator_before_filter_full(
    self, mock_connection_manager
  ):
    """Saturation rises toward 1.0 BEFORE the FilterFull overflow fires.

    Builds a TINY cuckoo (capacity sized for ~85% load at n=4 → 4 buckets),
    drives it decisively past capacity, and asserts that
    ``dupefilter/filter_saturation`` was set to a high value (>0.9) before
    ``dupefilter/filter_full`` was first incremented. This is the U2
    leading-indicator contract: the gauge is the early warning, the counter
    is the overflow alarm.
    """
    monitor = ScrapyStatsMonitor(_stats())
    cuckoo = CuckooMembershipFilter(capacity=4, error_rate=0.01)
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      monitor=monitor,
      membership_filter=cuckoo,
    )
    seen_high_saturation_before_full = False
    saw_filter_full = False
    for i in range(200):
      df.request_seen(Request(url=f"https://example.com/prefix-{i}"))
      sat = monitor._stats.get_value("dupefilter/filter_saturation")  # type: ignore[attr-defined]
      full = monitor._stats.get_value("dupefilter/filter_full")  # type: ignore[attr-defined]
      if (sat or 0.0) > 0.9 and not full:
        seen_high_saturation_before_full = True
      if full:
        saw_filter_full = True
        break
    assert seen_high_saturation_before_full, (
      "saturation should cross 0.9 before the FilterFull overflow fires"
    )
    assert saw_filter_full, "the FilterFull arm should eventually fire"


# ---------------------------------------------------------------------------
# R14-D — Observability completeness
# ---------------------------------------------------------------------------


class TestR14DObservability:
  """R14-D: on_error wiring, Bloom saturation, Memory eviction saturation,
  connection-lifecycle hooks, on_pop stat rename.

  These tests pin the gaps the round-14 observability audit surfaced:
  ``on_error`` was dead (zero call sites), Bloom/Memory saturation was
  unobservable, and connection lifecycle was log-only.
  """

  def test_on_error_emitted_on_push_failure(self, mock_connection_manager):
    """queue.push serialize-fail emits on_error('push', e) → errors/push."""
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
    )
    # A non-serializable callback makes _request_to_dict raise (.__name__ on a
    # lambda-like object); pushing must emit on_error then re-raise.
    request = Request(url="https://example.com")
    request.callback = object()  # type: ignore[assignment]  # no __name__
    with pytest.raises(SerializationError):
      queue.push(request)
    assert monitor._stats.get_value("errors/push") == 1  # type: ignore[attr-defined]
    assert monitor._stats.get_value("errors/pop") is None  # type: ignore[attr-defined]

  def test_on_error_emitted_on_pop_deserialize_failure(
    self, mock_connection_manager
  ):
    """queue.pop deserialize-fail emits on_error('pop', e) → errors/pop."""
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
    )
    # Return bytes that fail JSON deserialization → deserialize-fail arm.
    mock_connection_manager.get_queue_backend().pop.return_value = b"not-json{"
    with pytest.raises(SerializationError):
      queue.pop()
    assert monitor._stats.get_value("errors/pop") == 1  # type: ignore[attr-defined]
    assert monitor._stats.get_value("errors/push") is None  # type: ignore[attr-defined]

  def test_bloom_saturation_property_rises_with_load(self):
    """BloomMembershipFilter.saturation = len / capacity (R14-D mirror of cuckoo)."""
    from scrapy_extension.dupefilter.filters.bloom_filter import (
      BloomMembershipFilter,
    )

    bloom = BloomMembershipFilter(capacity=1_000, error_rate=0.01)
    assert bloom.saturation == 0.0
    assert bloom.capacity == 1_000
    prev = 0.0
    for i in range(500):
      bloom.add(f"item-{i}".encode())
      s = bloom.saturation
      assert s >= prev
      prev = s
    assert 0.0 < bloom.saturation < 1.0

  def test_request_seen_emits_saturation_for_bloom(
    self, mock_connection_manager
  ):
    """request_seen on a bloom-backed dupefilter emits on_filter_saturation."""
    from scrapy_extension.dupefilter.filters.bloom_filter import (
      BloomMembershipFilter,
    )

    monitor = ScrapyStatsMonitor(_stats())
    bloom = BloomMembershipFilter(capacity=1_000, error_rate=0.01)
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      monitor=monitor,
      membership_filter=bloom,
    )
    df.request_seen(Request(url="https://example.com/a"))
    sat = monitor._stats.get_value("dupefilter/filter_saturation")  # type: ignore[attr-defined]
    assert sat is not None
    assert sat > 0.0

  def test_memory_filter_eviction_emits_saturation(
    self, mock_connection_manager
  ):
    """MemoryMembershipFilter LRU eviction emits on_filter_saturation (R14-D).

    Builds a tiny-cap memory filter, threads the dupefilter's monitor into
    it (the dupefilter does this in __init__), and drives it past cap. The
    eviction must fire ``on_filter_saturation(len, maxsize)`` → the gauge is
    set. Before R14-D eviction was log-warning only.
    """
    from scrapy_extension.dupefilter.filters.memory_filter import (
      MemoryMembershipFilter,
    )

    monitor = ScrapyStatsMonitor(_stats())
    memory = MemoryMembershipFilter(maxsize=3)
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      monitor=monitor,
      membership_filter=memory,
    )
    # Fill toward cap. The gauge is set once the filter reaches maxsize after
    # an add (the saturation ceiling the operator cares about) — so the 3rd
    # distinct add (len → 3 == maxsize) already emits.
    df.request_seen(Request(url="https://example.com/a"))
    df.request_seen(Request(url="https://example.com/b"))
    assert monitor._stats.get_value("dupefilter/filter_saturation") is None  # type: ignore[attr-defined]
    df.request_seen(Request(url="https://example.com/c"))
    # 3rd add fills to cap → first saturation emit at 1.0.
    assert monitor._stats.get_value("dupefilter/filter_saturation") == 1.0  # type: ignore[attr-defined]
    # 4th distinct request → eviction; gauge stays pinned at cap (1.0).
    df.request_seen(Request(url="https://example.com/d"))
    sat = monitor._stats.get_value("dupefilter/filter_saturation")  # type: ignore[attr-defined]
    assert sat == 1.0

  def test_memory_filter_eviction_silent_without_monitor(self):
    """A standalone MemoryMembershipFilter (no monitor threaded) evicts
    without crashing — the saturation hook is a no-op when _monitor is None.
    """
    from scrapy_extension.dupefilter.filters.memory_filter import (
      MemoryMembershipFilter,
    )

    memory = MemoryMembershipFilter(maxsize=2)
    memory.add(b"a")
    memory.add(b"b")
    # Third add triggers eviction; no monitor threaded → must not raise.
    assert memory.add(b"c") is True

  def test_on_connect_emitted_on_successful_connect(self, mocker):
    """ConnectionManager.connect emits on_connect → backend/connect_count."""
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = ConnectionManager("redis")
    # Stub _attempt_connection so connect() succeeds on first try.
    mocker.patch.object(manager, "_attempt_connection")
    stats = _stats()
    manager.set_monitor(ScrapyStatsMonitor(stats))
    manager.connect()
    assert stats.get_value("backend/connect_count") == 1
    assert stats.get_value("backend/disconnect_count") is None

  def test_on_retry_emitted_on_connect_failure(self, mocker):
    """ConnectionManager.connect emits on_retry before each backoff sleep."""
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = ConnectionManager("redis", {"retry_attempts": 3, "retry_delay": 0})
    # _attempt_connection always raises → all 3 configured retries fire.
    mocker.patch.object(
      manager, "_attempt_connection", side_effect=RuntimeError("boom")
    )
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")  # skip backoff
    stats = _stats()
    manager.set_monitor(ScrapyStatsMonitor(stats))
    with pytest.raises(BackendConnectionError):
      manager.connect()
    # 1 initial attempt + 3 retries → on_retry fires three times.
    assert stats.get_value("backend/retry_count") == 3
    # on_connect must NOT fire (never succeeded).
    assert stats.get_value("backend/connect_count") is None

  def test_on_disconnect_emitted_on_close(self, mocker):
    """ConnectionManager.close emits on_disconnect when the backend tears down."""
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = ConnectionManager.get_manager("redis", {"host": "disc-test"})
    # Simulate a connected backend so the disconnect arm runs.
    mock_backend = mocker.MagicMock()
    manager._backend = mock_backend
    stats = _stats()
    manager.set_monitor(ScrapyStatsMonitor(stats))
    manager.close()
    assert stats.get_value("backend/disconnect_count") == 1
    mock_backend.disconnect.assert_called_once()

  def test_connection_hooks_noop_on_default_null_monitor(self, mocker):
    """A ConnectionManager with no monitor attached still connects/closes
    without crashing — the default NullMonitor hooks are no-ops (R14-D
    non-regression: existing subclasses must stay green)."""
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = ConnectionManager("redis", {"retry_attempts": 1, "retry_delay": 0})
    mocker.patch.object(manager, "_attempt_connection")
    # No set_monitor call → default NullMonitor; must not raise.
    manager.connect()
    manager._backend = mocker.MagicMock()
    manager.close()

  def test_raising_on_connect_monitor_does_not_turn_success_into_failure(
    self, mocker
  ):
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = ConnectionManager("redis", {"retry_attempts": 0})
    backend = mocker.MagicMock(name="backend")
    mocker.patch.object(manager, "_create_backend", return_value=backend)
    monitor = mocker.MagicMock(name="monitor")
    monitor.on_connect.side_effect = RuntimeError("stats unavailable")
    manager.set_monitor(monitor)

    manager.connect()

    assert manager._backend is backend
    backend.connect.assert_called_once_with()

  def test_raising_on_retry_monitor_does_not_abort_retry(self, mocker):
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = ConnectionManager(
      "redis",
      {"retry_attempts": 1, "retry_delay": 0},
    )
    failed_backend = mocker.MagicMock(name="failed-backend")
    failed_backend.connect.side_effect = OSError("first attempt failed")
    recovered_backend = mocker.MagicMock(name="recovered-backend")
    mocker.patch.object(
      manager,
      "_create_backend",
      side_effect=[failed_backend, recovered_backend],
    )
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")
    monitor = mocker.MagicMock(name="monitor")
    monitor.on_retry.side_effect = RuntimeError("stats unavailable")
    manager.set_monitor(monitor)

    manager.connect()

    assert manager._backend is recovered_backend
    monitor.on_retry.assert_called_once_with("redis", 1)

  def test_raising_on_disconnect_monitor_does_not_skip_breaker_reset(
    self, mocker
  ):
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = ConnectionManager("redis")
    manager._backend = mocker.MagicMock(name="backend")
    manager._breaker = mocker.MagicMock(name="breaker")
    monitor = mocker.MagicMock(name="monitor")
    monitor.on_disconnect.side_effect = RuntimeError("stats unavailable")
    manager.set_monitor(monitor)

    manager.close()

    assert manager._backend is None
    manager._breaker.reset.assert_called_once_with()

  def test_on_pop_attempt_stat_name_matches_behavior(
    self, mock_connection_manager
  ):
    """R14-D: queue/pop_attempt_count is the renamed per-attempt key."""
    monitor = ScrapyStatsMonitor(_stats())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="q",
      monitor=monitor,
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue.pop()  # empty pop still counts as an attempt
    queue.pop()  # second attempt
    assert monitor._stats.get_value("queue/pop_attempt_count") == 2  # type: ignore[attr-defined]

  def test_connection_lifecycle_hooks_on_base_monitor_are_noop(self):
    """on_connect/on_disconnect/on_retry exist on Monitor base + NullMonitor
    as no-ops (R14-D: new hooks must not break existing subclasses)."""
    null_monitor: Monitor = NullMonitor()
    # All three new hooks must be callable + return None.
    assert null_monitor.on_connect("redis") is None
    assert null_monitor.on_disconnect("redis", "shutdown") is None
    assert null_monitor.on_retry("redis", 1) is None


class TestScrapyStatsMonitorResilience:
  """R5: every hook must swallow StatsCollector failures — observability must
  not crash the data path. A custom/buggy collector or stats-backend outage is
  logged at debug, never propagated into the push/pop/store hot paths."""

  @pytest.mark.parametrize(
    ("hook", "kwargs"),
    [
      ("on_push", {"queue_name": "q", "priority": 1.0}),
      ("on_pop", {"queue_name": "q"}),
      ("on_dedup_hit", {"key": "k"}),
      ("on_dedup_miss", {"key": "k"}),
      ("on_queue_depth", {"queue_name": "q", "depth": 5}),
      ("on_store", {"key": "k"}),
      ("on_filter_full", {}),
      ("on_pop_rate", {"window_s": DEFAULT_POP_RATE_WINDOW_S, "rate": 1.0}),
      ("on_filter_saturation", {"used": 1, "capacity": 10}),
      ("on_error", {"operation": "push", "error": RuntimeError("x")}),
      ("on_connect", {"backend_type": "redis"}),
      ("on_disconnect", {"backend_type": "redis", "reason": None}),
      ("on_retry", {"backend_type": "redis", "attempt": 1}),
      ("on_buffer_depth", {"depth": 3}),
      ("on_delay_depth", {"depth": 3}),
    ],
  )
  def test_hook_swallows_stats_failure(self, hook, kwargs, caplog) -> None:
    import logging

    caplog.set_level(logging.DEBUG, logger="scrapy_extension.monitor.stats")
    stats = MagicMock()
    stats.inc_value.side_effect = RuntimeError("stats backend down")
    stats.set_value.side_effect = RuntimeError("stats backend down")
    monitor = ScrapyStatsMonitor(stats)
    # Must NOT raise — the @_stats_safe wrapper swallows + logs at debug.
    result = getattr(monitor, hook)(**kwargs)
    assert result is None  # hooks return None on the swallowed path
    # Lock in the debug-log contract (review feedback): the failure is
    # surfaced for diagnosis, not silently dropped.
    debug_msgs = [
      r.getMessage()
      for r in caplog.records
      if r.levelno == logging.DEBUG and r.name == "scrapy_extension.monitor.stats"
    ]
    assert any(hook in m and "ignored" in m for m in debug_msgs), (
      f"expected debug log for {hook} failure; got: {debug_msgs}"
    )
