"""Tests for Scrapy components."""

from unittest.mock import ANY

import pytest
from scrapy import Field, Item
from scrapy.http import Request

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.exceptions import QueueError, SerializationError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler


class RecordingDupeFilter:
  """Test dupefilter that records crawler wiring and duplicate decisions."""

  def __init__(self, duplicates=None):
    self.duplicates = set() if duplicates is None else set(duplicates)
    self.seen_requests = []
    self.logged_requests = []
    self.crawler = None

  @classmethod
  def from_crawler(cls, crawler):
    dupefilter = cls()
    dupefilter.crawler = crawler
    return dupefilter

  def request_seen(self, request):
    self.seen_requests.append(request)
    return request.url in self.duplicates

  def log(self, request, spider):
    self.logged_requests.append((request, spider))


class SampleItem(Item):
  """Sample item for pipeline tests."""

  name = Field()
  value = Field()


class TestBackendQueue:
  """Test BackendQueue component."""

  def test_push_request(self, mock_connection_manager, mock_spider):
    """Test pushing a request to the queue."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(url="https://example.com")
    queue.push(request, priority=1.0)

    mock_connection_manager.get_queue_backend().push.assert_called_once()

  def test_pop_request(self, mock_connection_manager, mock_spider):
    """Test popping a request from the queue."""
    mock_connection_manager.get_queue_backend().pop.return_value = (
      b'{"url": "https://example.com", "callback": null}'
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    result = queue.pop()

    assert result is not None
    assert isinstance(result, Request)

  def test_pop_empty(self, mock_connection_manager, mock_spider):
    """Test popping from empty queue."""
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    result = queue.pop()

    assert result is None

  def test_len(self, mock_connection_manager, mock_spider):
    """Test queue length."""
    mock_connection_manager.get_queue_backend().queue_len.return_value = 5
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    assert len(queue) == 5


class TestBackendScheduler:
  """Test BackendScheduler component."""

  def test_enqueue_request_returns_false_for_duplicate(self, mock_connection_manager, mocker):
    """Test duplicate requests are rejected by the configured dupefilter."""
    dupefilter = RecordingDupeFilter(duplicates={"https://example.com"})
    spider = mocker.Mock()
    spider.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter=dupefilter,
    )
    scheduler.open(spider)

    request = Request(url="https://example.com")

    assert scheduler.enqueue_request(request) is False
    mock_connection_manager.get_queue_backend().push.assert_not_called()
    assert dupefilter.seen_requests == [request]
    assert dupefilter.logged_requests == [(request, spider)]

  def test_enqueue_request_pushes_new_request(self, mock_connection_manager, mocker):
    """Test new requests are enqueued after dupefilter check."""
    dupefilter = RecordingDupeFilter()
    spider = mocker.Mock()
    spider.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter=dupefilter,
    )
    scheduler.open(spider)

    request = Request(url="https://example.com")

    assert scheduler.enqueue_request(request) is True
    mock_connection_manager.get_queue_backend().push.assert_called_once()
    assert dupefilter.seen_requests == [request]

  def test_enqueue_request_bypasses_dupefilter_for_dont_filter(
    self, mock_connection_manager, mocker
  ):
    """Test dont_filter requests skip duplicate filtering and still enqueue."""
    dupefilter = RecordingDupeFilter(duplicates={"https://example.com"})
    spider = mocker.Mock()
    spider.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter=dupefilter,
    )
    scheduler.open(spider)

    request = Request(url="https://example.com", dont_filter=True)

    assert scheduler.enqueue_request(request) is True
    mock_connection_manager.get_queue_backend().push.assert_called_once()
    assert dupefilter.seen_requests == []

  def test_enqueue_request_serialization_error_returns_false_and_increments_stats(
    self, mock_connection_manager, mocker
  ):
    """Test enqueue_request handles serialization failures without bubbling."""
    stats = mocker.Mock()
    spider = mocker.Mock()
    spider.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      stats=stats,
    )
    scheduler.open(spider)
    mock_connection_manager.get_queue_backend().push.side_effect = SerializationError(
      "bad request payload"
    )

    request = Request(url="https://example.com")

    assert scheduler.enqueue_request(request) is False
    stats.inc_value.assert_called_once_with("scheduler/serialization_errors")

  def test_enqueue_request(self, mock_connection_manager, mock_spider):
    """Test enqueuing a request — scheduler pushes without dedup.

    Dedup is the dupefilter's responsibility (R1-P1-11 fix); the scheduler
    assumes the engine already filtered duplicates before calling.
    """
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is True
    mock_connection_manager.get_queue_backend().push.assert_called_once()
    # Scheduler must NOT touch set_backend — that's the dupefilter's job.
    mock_connection_manager.get_set_backend.assert_not_called()

  def test_next_request(self, mock_connection_manager, mock_spider):
    """Test getting next request."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    # Need to open the scheduler first
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_queue_backend = mock_connection_manager.get_queue_backend()
    mock_queue_backend.pop.return_value = (
      b'{"url": "https://example.com", "callback": null}'
    )

    result = scheduler.next_request()

    assert result is not None
    assert isinstance(result, Request)

  def test_enqueue_does_not_touch_set_backend(self, mock_connection_manager, mock_spider):
    """Scheduler must not touch set_backend — dedup is the dupefilter's job (R1-P1-11)."""
    mock_stats = mock_connection_manager.get_queue_backend()
    mock_stats.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      stats=mock_stats,
    )
    scheduler.open(mock_stats)

    set_backend = mock_connection_manager.get_set_backend()
    set_backend.add.return_value = True

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is True
    mock_stats.inc_value.assert_called_with("scheduler/enqueued")
    set_backend.add.assert_not_called()

  def test_enqueue_request_set_backend_not_implemented(self, mock_connection_manager, mock_spider):
    """Test enqueue_request skips dedup when backend does not support sets."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    # set_backend.add raises NotImplementedError - backend does not support sets
    mock_connection_manager.get_set_backend.side_effect = NotImplementedError

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is True
    mock_connection_manager.get_queue_backend().push.assert_called_once()

  def test_enqueue_request_queue_not_open(self, mock_connection_manager, mock_spider):
    """Test enqueue_request raises RuntimeError when scheduler not opened."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    request = Request(url="https://example.com")

    with pytest.raises(RuntimeError, match="Scheduler not opened"):
      scheduler.enqueue_request(request)

  def test_next_request_empty(self, mock_connection_manager, mock_spider):
    """Test next_request returns None when queue is empty."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().pop.return_value = None

    result = scheduler.next_request()

    assert result is None

  def test_next_request_queue_not_open(self, mock_connection_manager, mock_spider):
    """Test next_request raises RuntimeError when scheduler not opened."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    with pytest.raises(RuntimeError, match="Scheduler not opened"):
      scheduler.next_request()

  def test_next_request_queue_error(self, mock_connection_manager, mock_spider):
    """Test next_request returns None on QueueError."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    from scrapy_extension.exceptions import QueueError

    mock_connection_manager.get_queue_backend().pop.side_effect = QueueError(
      "test error"
    )

    result = scheduler.next_request()

    assert result is None

  def test_next_request_deserialization_error(self, mock_connection_manager):
    """Test next_request returns None on SerializationError."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().pop.side_effect = SerializationError(
      "bad request payload"
    )

    result = scheduler.next_request()

    assert result is None

  def test_next_request_deserialization_error_increments_stats(
    self, mock_connection_manager
  ):
    """Test next_request increments stats on SerializationError."""
    mock_stats = mock_connection_manager.get_queue_backend()
    mock_stats.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      stats=mock_stats,
    )
    scheduler.open(mock_stats)

    mock_connection_manager.get_queue_backend().pop.side_effect = SerializationError(
      "bad request payload"
    )

    result = scheduler.next_request()

    assert result is None
    mock_stats.inc_value.assert_called_once_with("scheduler/deserialization_errors")

  def test_next_request_dequeues_stats(self, mock_connection_manager, mock_spider):
    """Test next_request increments stats when dequeuing."""
    mock_stats = mock_connection_manager.get_queue_backend()
    mock_stats.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      stats=mock_stats,
    )
    scheduler.open(mock_stats)

    mock_queue_backend = mock_connection_manager.get_queue_backend()
    mock_queue_backend.pop.return_value = (
      b'{"url": "https://example.com", "callback": null}'
    )

    result = scheduler.next_request()

    assert result is not None
    mock_stats.inc_value.assert_called_with("scheduler/dequeued")

  def test_has_pending_requests(self, mock_connection_manager, mock_spider):
    """Test has_pending_requests returns True when queue has items."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().queue_len.return_value = 5

    assert scheduler.has_pending_requests() is True

  def test_has_pending_requests_empty(self, mock_connection_manager, mock_spider):
    """Test has_pending_requests returns False when queue is empty."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().queue_len.return_value = 0

    assert scheduler.has_pending_requests() is False

  def test_has_pending_requests_when_queue_len_not_implemented(
    self, mock_connection_manager
  ):
    """Test has_pending_requests is conservative when queue length is unsupported."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().queue_len.side_effect = (
      NotImplementedError
    )

    assert scheduler.has_pending_requests() is True

  def test_has_pending_requests_when_queue_len_raises_queue_error(
    self, mock_connection_manager
  ):
    """Test has_pending_requests is conservative when queue length lookup fails."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().queue_len.side_effect = QueueError(
      "length lookup failed"
    )

    assert scheduler.has_pending_requests() is True

  def test_len_with_queue(self, mock_connection_manager, mock_spider):
    """Test __len__ returns queue length."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().queue_len.return_value = 3

    assert len(scheduler) == 3

  def test_len_no_queue(self, mock_connection_manager, mock_spider):
    """Test __len__ returns 0 when scheduler not opened."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    assert len(scheduler) == 0

  def test_close(self, mock_connection_manager, mock_spider):
    """Test close clears scheduler state."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    assert scheduler._spider is not None
    assert scheduler._queue is not None

    scheduler.close("finished")

    assert scheduler._spider is None
    assert scheduler._queue is None

  def test_close_disconnects_ack_signals(self, mock_connection_manager, mocker):
    """Test close disconnects ack/nack signal handlers."""
    signals = mocker.Mock()
    crawler = mocker.Mock(signals=signals)
    spider = mocker.Mock(crawler=crawler)
    spider.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    scheduler.open(spider)
    scheduler.close("finished")

    assert signals.connect.call_count == 2
    assert signals.disconnect.call_count == 2
    signals.disconnect.assert_any_call(
      scheduler._on_response_received,
      signal=ANY,
    )
    signals.disconnect.assert_any_call(
      scheduler._on_spider_error,
      signal=ANY,
    )

  def test_open_uses_configured_queue_key(self, mock_connection_manager, mocker):
    """Test open creates BackendQueue with the configured queue_key."""
    queue_ctor = mocker.patch("scrapy_extension.schedule.scheduler.BackendQueue")
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="configured:queue",
    )
    spider = mocker.Mock()
    spider.name = "test_spider"

    scheduler.open(spider)

    queue_ctor.assert_called_once_with(
      connection_manager=mock_connection_manager,
      queue_name="configured:queue",
      spider=spider,
    )

  def test_close_calls_connection_manager_close(self, mock_connection_manager, mock_spider):
    """Test close shuts down the connection manager."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    scheduler.close("finished")

    mock_connection_manager.close.assert_called_once_with()

  def test_close_resets_signals_connected_for_reuse(self, mock_connection_manager, mocker):
    """R12-followup: close() must reset _signals_connected so reopen wires signals."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)
    assert scheduler._signals_connected is True

    scheduler.close("finished")
    assert scheduler._signals_connected is False

  def test_open_wires_response_received_to_ack(self, mock_connection_manager, mocker):
    """R12-2: scheduler.open connects response_received → queue.ack().

    Verifies the signal-driven ack path that replaces the auto-ack removed
    from KafkaBackend.pop / RabbitMQBackend.pop in Round 12.
    """
    from scrapy import signals

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)

    mock_spider.crawler.signals.connect.assert_any_call(
      scheduler._on_response_received,
      signal=signals.response_received,
    )

  def test_open_wires_spider_error_to_nack(self, mock_connection_manager, mocker):
    """R12-2: scheduler.open connects spider_error → queue.nack()."""
    from scrapy import signals

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)

    mock_spider.crawler.signals.connect.assert_any_call(
      scheduler._on_spider_error,
      signal=signals.spider_error,
    )

  def test_signal_handlers_call_queue_ack_nack(self, mock_connection_manager, mocker):
    """The wired signal handlers call queue.ack()/nack() on the right events."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)

    queue = scheduler._queue
    assert queue is not None
    queue_ack_spy = mocker.patch.object(queue, "ack")
    queue_nack_spy = mocker.patch.object(queue, "nack")

    # Fire the signal handlers directly
    scheduler._on_response_received(response=None, request=None, spider=mock_spider)
    queue_ack_spy.assert_called_once()

    scheduler._on_spider_error(failure=None, response=None, spider=mock_spider)
    queue_nack_spy.assert_called_once()

  def test_connect_ack_signals_is_idempotent(self, mock_connection_manager, mocker):
    """R12: _connect_ack_signals short-circuits when already wired (line 136)."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)

    connect_count = mock_spider.crawler.signals.connect.call_count
    # Second call must return early via the _signals_connected guard.
    scheduler._connect_ack_signals(mock_spider)
    assert mock_spider.crawler.signals.connect.call_count == connect_count

  def test_on_response_received_noop_when_queue_none(self, mock_connection_manager, mocker):
    """R12: _on_response_received returns early when _queue is None (line 161)."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)
    scheduler._queue = None  # not-yet-opened / already-closed

    # Must not raise (and must not attempt ack on a None queue).
    scheduler._on_response_received(response=None, request=None, spider=mock_spider)

  def test_on_response_received_swallows_queue_error(self, mock_connection_manager, mocker):
    """R12: a QueueError from ack() is swallowed — signal chain stays intact (164-165)."""
    from scrapy_extension.exceptions import QueueError

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)

    queue = scheduler._queue
    assert queue is not None
    ack_spy = mocker.patch.object(queue, "ack", side_effect=QueueError("ack failed"))

    # Must NOT propagate — the handler's try/except protects Scrapy's signal chain.
    scheduler._on_response_received(response=None, request=None, spider=mock_spider)
    ack_spy.assert_called_once()

  def test_on_spider_error_noop_when_queue_none(self, mock_connection_manager, mocker):
    """R12: _on_spider_error returns early when _queue is None (line 176)."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)
    scheduler._queue = None

    scheduler._on_spider_error(failure=None, response=None, spider=mock_spider)

  def test_on_spider_error_swallows_queue_error(self, mock_connection_manager, mocker):
    """R12: a QueueError from nack() is swallowed — signal chain stays intact (179-180)."""
    from scrapy_extension.exceptions import QueueError

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    mock_spider.crawler = mocker.MagicMock()
    scheduler.open(mock_spider)

    queue = scheduler._queue
    assert queue is not None
    nack_spy = mocker.patch.object(queue, "nack", side_effect=QueueError("nack failed"))

    scheduler._on_spider_error(failure=None, response=None, spider=mock_spider)
    nack_spy.assert_called_once()

  def test_enqueue_request_enqueues_stats(self, mock_connection_manager, mock_spider):
    """Test enqueue_request increments stats on successful enqueue."""
    mock_stats = mock_connection_manager.get_queue_backend()
    mock_stats.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      stats=mock_stats,
    )
    scheduler.open(mock_stats)

    set_backend = mock_connection_manager.get_set_backend()
    set_backend.add.return_value = True

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is True
    mock_stats.inc_value.assert_called_with("scheduler/enqueued")

  def test_from_settings(self, mocker):
    """Test from_settings class method creates scheduler from Scrapy settings."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_QUEUE_KEY": "scheduler:queue",
      "SCRAPY_DUPEFILTER_KEY": "scheduler:dupefilter",
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(mock_settings)

    assert scheduler.queue_key == "scheduler:queue"

  def test_from_crawler(self, mocker):
    """Test from_crawler class method creates scheduler from crawler."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_crawler = mocker.Mock()
    mock_crawler.settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_QUEUE_KEY": "scheduler:queue",
      "SCRAPY_DUPEFILTER_KEY": "scheduler:dupefilter",
    }.get(key, default)
    mock_crawler.settings.getdict.return_value = {}
    mock_crawler.stats = mocker.Mock()

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_crawler(mock_crawler)

    assert scheduler.stats is mock_crawler.stats

  def test_from_crawler_uses_configured_dupefilter_class(self, mocker):
    """Test from_crawler wires the configured dupefilter class."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_crawler = mocker.Mock()
    mock_crawler.settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_QUEUE_KEY": "scheduler:queue",
      "DUPEFILTER_CLASS": "tests.test_components.RecordingDupeFilter",
    }.get(key, default)
    mock_crawler.settings.getdict.return_value = {}
    mock_crawler.stats = mocker.Mock()

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_crawler(mock_crawler)

    assert isinstance(scheduler.dupefilter, RecordingDupeFilter)
    assert scheduler.dupefilter.crawler is mock_crawler

  def test_open_rejects_invalid_spider_name(self, mock_connection_manager, mocker):
    """R23-D2: spider.name with invalid chars must fail at open, not deep in push.

    Without this guard, `spider.name="my spider"` (space) propagates to
    queue_name="my spider:queue", which `_validate_key_name` rejects inside
    the first push. The error points at the queue name, hiding the root cause.
    """
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    invalid_spider = mocker.MagicMock()
    invalid_spider.name = "invalid name with spaces"

    with pytest.raises(ValueError, match="spider.name"):
      scheduler.open(invalid_spider)

  def test_open_accepts_valid_spider_name(self, mock_connection_manager, mocker):
    """R23-D2: valid spider.name (alphanumeric, dots, hyphens, colons, underscores) passes."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    valid_spider = mocker.MagicMock()
    valid_spider.name = "my-spider.v2:production"
    valid_spider.crawler = None  # avoid signal wiring side-effect

    scheduler.open(valid_spider)  # must not raise
    assert scheduler._spider is valid_spider


class TestBackendDupeFilter:
  """Test BackendDupeFilter component."""

  def test_request_seen_new(self, mock_connection_manager, mock_spider):
    """Test seeing a new request."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="test:dupefilter",
    )

    mock_set_backend = mock_connection_manager.get_set_backend()
    mock_set_backend.contains.return_value = False

    request = Request(url="https://example.com")
    result = dupefilter.request_seen(request)

    assert result is False  # Not a duplicate
    mock_set_backend.add.assert_called_once()

  def test_request_seen_duplicate(self, mock_connection_manager, mock_spider):
    """Test seeing a duplicate request."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="test:dupefilter",
    )

    mock_set_backend = mock_connection_manager.get_set_backend()
    mock_set_backend.contains.return_value = True
    mock_set_backend.add.return_value = False

    request = Request(url="https://example.com")
    result = dupefilter.request_seen(request)

    assert result is True  # Is a duplicate


class TestBackendPipeline:
  """Test BackendPipeline component."""

  def test_process_item(self, mock_connection_manager, mock_spider):
    """Test processing an item."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="items",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"

    item = SampleItem(name="Test", value=123)
    result = pipeline.process_item(item, mock_spider)

    assert result == item
    mock_connection_manager.get_storage_backend().store.assert_called_once()

  def test_process_item_with_ttl(self, mock_connection_manager, mock_spider):
    """Test processing an item with TTL."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="items",
      ttl=3600,
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"

    item = SampleItem(name="Test", value=123)
    pipeline.process_item(item, mock_spider)

    # Verify TTL was passed
    call_args = mock_connection_manager.get_storage_backend().store.call_args
    assert call_args[1].get("ttl") == 3600
