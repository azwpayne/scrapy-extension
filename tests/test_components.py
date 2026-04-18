"""Tests for Scrapy components."""

import pytest
from scrapy import Field, Item
from scrapy.http import Request
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler


class SampleItem(Item):
  """Sample item for pipeline tests."""

  name = Field()
  value = Field()


class TestBackendQueue:
  """Test BackendQueue component."""

  def test_push_request(self, mock_connection_manager):
    """Test pushing a request to the queue."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(url="https://example.com")
    queue.push(request, priority=1.0)

    mock_connection_manager.get_queue_backend().push.assert_called_once()

  def test_pop_request(self, mock_connection_manager):
    """Test popping a request from the queue."""
    mock_connection_manager.get_queue_backend().pop.return_value = (
      b'{"url": "https://example.com", "callback": null}'
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    result = queue.pop()

    assert result is not None
    assert isinstance(result, Request)

  def test_pop_empty(self, mock_connection_manager):
    """Test popping from empty queue."""
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    result = queue.pop()

    assert result is None

  def test_len(self, mock_connection_manager):
    """Test queue length."""
    mock_connection_manager.get_queue_backend().queue_len.return_value = 5
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    assert len(queue) == 5

  def test_peek_preserves_priority(self, mock_connection_manager):
    """Test that peek pushes back with the same priority."""
    # Return a request dict with explicit priority to preserve through push.
    mock_connection_manager.get_queue_backend().pop.return_value = b'{"url":"https://example.com","callback":null,"errback":null,"method":"GET","headers":{},"body":null,"cookies":{},"meta":{},"encoding":"utf-8","priority":42,"dont_filter":false,"flags":[]}'

    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    result = queue.peek()

    assert result is not None
    assert result.priority == 42

    call_args = mock_connection_manager.get_queue_backend().push.call_args
    assert call_args is not None
    assert call_args[0][0] == "test_queue"
    assert call_args[0][2] == 42


class TestBackendScheduler:
  """Test BackendScheduler component."""

  def test_enqueue_request(self, mock_connection_manager):
    """Test enqueuing a request."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    # Open the scheduler first
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_set_backend = mock_connection_manager.get_set_backend()
    mock_set_backend.contains.return_value = False

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is True
    mock_set_backend.add.assert_called_once()

  def test_enqueue_duplicate(self, mock_connection_manager):
    """Test enqueuing a duplicate request."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    mock_set_backend = mock_connection_manager.get_set_backend()
    mock_set_backend.add.return_value = False

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is False
    mock_set_backend.add.assert_called_once()

  def test_next_request(self, mock_connection_manager):
    """Test getting next request."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
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

  def test_enqueue_duplicate_uses_set_add(self, mock_connection_manager):
    """Test enqueue_request deduplication uses set.add result for atomicity."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    # Configure set.add to indicate duplicate (already exists)
    set_backend = mock_connection_manager.get_set_backend()
    set_backend.add.return_value = False

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is False
    set_backend.add.assert_called_once()
    set_backend.contains.assert_not_called()

  def test_enqueue_request_duplicate_increments_stats(self, mock_connection_manager):
    """Test enqueue_request increments stats when request is duplicate."""
    mock_stats = mock_connection_manager.get_queue_backend()
    mock_stats.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
      stats=mock_stats,
    )
    scheduler.open(mock_stats)

    set_backend = mock_connection_manager.get_set_backend()
    set_backend.add.return_value = False

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is False
    mock_stats.inc_value.assert_called_once_with("scheduler/dropped_duplicates")

  def test_enqueue_request_set_backend_not_implemented(self, mock_connection_manager):
    """Test enqueue_request skips dedup when backend does not support sets."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
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

  def test_enqueue_request_queue_not_open(self, mock_connection_manager):
    """Test enqueue_request raises RuntimeError when scheduler not opened."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    request = Request(url="https://example.com")

    with pytest.raises(RuntimeError, match="Scheduler not opened"):
      scheduler.enqueue_request(request)

  def test_next_request_empty(self, mock_connection_manager):
    """Test next_request returns None when queue is empty."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().pop.return_value = None

    result = scheduler.next_request()

    assert result is None

  def test_next_request_queue_not_open(self, mock_connection_manager):
    """Test next_request raises RuntimeError when scheduler not opened."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    with pytest.raises(RuntimeError, match="Scheduler not opened"):
      scheduler.next_request()

  def test_next_request_queue_error(self, mock_connection_manager):
    """Test next_request returns None on QueueError."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
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

  def test_next_request_dequeues_stats(self, mock_connection_manager):
    """Test next_request increments stats when dequeuing."""
    mock_stats = mock_connection_manager.get_queue_backend()
    mock_stats.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
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

  def test_has_pending_requests(self, mock_connection_manager):
    """Test has_pending_requests returns True when queue has items."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().queue_len.return_value = 5

    assert scheduler.has_pending_requests() is True

  def test_has_pending_requests_empty(self, mock_connection_manager):
    """Test has_pending_requests returns False when queue is empty."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().queue_len.return_value = 0

    assert scheduler.has_pending_requests() is False

  def test_len_with_queue(self, mock_connection_manager):
    """Test __len__ returns queue length."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_connection_manager.get_queue_backend().queue_len.return_value = 3

    assert len(scheduler) == 3

  def test_len_no_queue(self, mock_connection_manager):
    """Test __len__ returns 0 when scheduler not opened."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    assert len(scheduler) == 0

  def test_close(self, mock_connection_manager):
    """Test close clears scheduler state."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    assert scheduler._spider is not None
    assert scheduler._queue is not None

    scheduler.close("finished")

    assert scheduler._spider is None
    assert scheduler._queue is None

  def test_enqueue_request_enqueues_stats(self, mock_connection_manager):
    """Test enqueue_request increments stats on successful enqueue."""
    mock_stats = mock_connection_manager.get_queue_backend()
    mock_stats.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
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
    assert scheduler.dupefilter_key == "scheduler:dupefilter"

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


class TestBackendDupeFilter:
  """Test BackendDupeFilter component."""

  def test_request_seen_new(self, mock_connection_manager):
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

  def test_request_seen_duplicate(self, mock_connection_manager):
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

  def test_process_item(self, mock_connection_manager):
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

  def test_process_item_with_ttl(self, mock_connection_manager):
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
