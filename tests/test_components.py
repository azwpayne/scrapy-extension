"""Tests for Scrapy components."""

from unittest.mock import Mock, patch

import pytest
from scrapy.http import Request

from scrapy_extension.components.dupefilter import BackendDupeFilter
from scrapy_extension.components.pipeline import BackendPipeline
from scrapy_extension.components.queue import BackendQueue
from scrapy_extension.components.scheduler import BackendScheduler


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
    mock_connection_manager.get_queue_backend().len.return_value = 5
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    assert len(queue) == 5


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
    mock_spider = Mock()
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
    mock_set_backend.contains.return_value = True

    request = Request(url="https://example.com")
    result = scheduler.enqueue_request(request)

    assert result is False

  def test_next_request(self, mock_connection_manager):
    """Test getting next request."""
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
      dupefilter_key="test:dupefilter",
    )

    # Need to open the scheduler first
    mock_spider = Mock()
    mock_spider.name = "test_spider"
    scheduler.open(mock_spider)

    mock_queue_backend = mock_connection_manager.get_queue_backend()
    mock_queue_backend.pop.return_value = (
      b'{"url": "https://example.com", "callback": null}'
    )

    result = scheduler.next_request()

    assert result is not None
    assert isinstance(result, Request)


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

    mock_spider = Mock()
    mock_spider.name = "test_spider"

    item = {"name": "Test", "value": 123}
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

    mock_spider = Mock()
    mock_spider.name = "test_spider"

    item = {"name": "Test", "value": 123}
    pipeline.process_item(item, mock_spider)

    # Verify TTL was passed
    call_args = mock_connection_manager.get_storage_backend().store.call_args
    assert call_args[1].get("ttl") == 3600
