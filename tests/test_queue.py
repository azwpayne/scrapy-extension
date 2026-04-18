"""Tests for BackendQueue component."""

import pytest
from scrapy.http import Request

from scrapy_extension.exceptions import SerializationError
from scrapy_extension.queue.queue import BackendQueue


class TestBackendQueueInit:
  """Test BackendQueue initialization."""

  def test_init_sets_attributes(self, mock_connection_manager):
    """Test __init__ sets connection_manager and queue_name."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    assert queue.connection_manager is mock_connection_manager
    assert queue.queue_name == "test_queue"

  def test_serializer_lazy_initialized(self, mock_connection_manager):
    """Test serializer is lazily initialized via cached_property."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    # Access _serializer twice - should return same instance
    serializer1 = queue._serializer
    serializer2 = queue._serializer
    assert serializer1 is serializer2


class TestBackendQueueRequestToDict:
  """Test _request_to_dict method."""

  def test_basic_request_to_dict(self, mock_connection_manager):
    """Test converting a basic request to dict."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(url="https://example.com", method="GET")
    result = queue._request_to_dict(request)

    assert result["url"] == "https://example.com"
    assert result["method"] == "GET"
    assert result["callback"] is None
    assert result["errback"] is None
    assert result["headers"] == {}
    assert result["body"] is None
    assert result["cookies"] == {}
    assert result["meta"] == {}
    assert result["encoding"] == "utf-8"
    assert result["priority"] == 0
    assert result["dont_filter"] is False
    assert result["flags"] == []

  def test_request_to_dict_with_body_utf8(self, mock_connection_manager):
    """Test request with UTF-8 body decodes successfully."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      body=b"hello world",
      encoding="utf-8",
    )
    result = queue._request_to_dict(request)

    assert result["body"] == "hello world"
    assert result["encoding"] == "utf-8"

  def test_request_to_dict_with_body_latin1_fallback(self, mock_connection_manager):
    """Test request body falls back to latin-1 on UnicodeDecodeError."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    # Create a request with bytes that are valid latin-1 but not valid utf-8
    request = Request(
      url="https://example.com",
      body=b"\xe9",  # Latin-1 encoded character
      encoding="utf-8",
    )
    result = queue._request_to_dict(request)

    # Should fall back to latin-1
    assert result["body"] == "\xe9"

  def test_request_to_dict_with_headers(self, mock_connection_manager):
    """Test request with headers converts to dict."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      headers={"Content-Type": "application/json"},
    )
    result = queue._request_to_dict(request)

    assert result["headers"] == {"Content-Type": "application/json"}

  def test_request_to_dict_with_cookies(self, mock_connection_manager):
    """Test request with cookies."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      cookies={"name": "value"},
    )
    result = queue._request_to_dict(request)

    assert result["cookies"] == {"name": "value"}

  def test_request_to_dict_with_meta(self, mock_connection_manager):
    """Test request with meta."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      meta={"key": "value"},
    )
    result = queue._request_to_dict(request)

    assert result["meta"] == {"key": "value"}

  def test_request_to_dict_with_priority(self, mock_connection_manager):
    """Test request with priority."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      priority=10,
    )
    result = queue._request_to_dict(request)

    assert result["priority"] == 10

  def test_request_to_dict_dont_filter(self, mock_connection_manager):
    """Test request with dont_filter flag."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      dont_filter=True,
    )
    result = queue._request_to_dict(request)

    assert result["dont_filter"] is True

  def test_request_to_dict_with_flags(self, mock_connection_manager):
    """Test request with flags."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      flags=["flag1", "flag2"],
    )
    result = queue._request_to_dict(request)

    assert result["flags"] == ["flag1", "flag2"]

  def test_request_to_dict_with_callback(self, mock_connection_manager):
    """Test request with callback function name captured."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    def my_callback(response):
      pass

    request = Request(
      url="https://example.com",
      callback=my_callback,
    )
    result = queue._request_to_dict(request)

    assert result["callback"] == "my_callback"

  def test_request_to_dict_with_errback(self, mock_connection_manager):
    """Test request with errback function name captured."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    def my_errback(failure):
      pass

    request = Request(
      url="https://example.com",
      errback=my_errback,
    )
    result = queue._request_to_dict(request)

    assert result["errback"] == "my_errback"

  def test_request_to_dict_empty_body(self, mock_connection_manager):
    """Test request with empty body."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      body=b"",
    )
    result = queue._request_to_dict(request)

    assert result["body"] is None


class TestBackendQueuePush:
  """Test push method."""

  def test_push_serializes_and_calls_backend(self, mock_connection_manager):
    """Test push serializes request and calls queue backend."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(url="https://example.com")
    queue.push(request, priority=5.0)

    # Verify serializer was called
    mock_connection_manager.get_queue_backend().push.assert_called_once()
    call_args = mock_connection_manager.get_queue_backend().push.call_args
    assert call_args[0][0] == "test_queue"
    # Data should be bytes (serialized)
    assert isinstance(call_args[0][1], bytes)
    assert call_args[0][2] == 5.0

  def test_push_with_default_priority(self, mock_connection_manager):
    """Test push uses default priority of 0.0."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(url="https://example.com")
    queue.push(request)

    call_args = mock_connection_manager.get_queue_backend().push.call_args
    assert call_args[0][2] == 0.0

  def test_push_raises_serialization_error_on_exception(self, mock_connection_manager):
    """Test push raises SerializationError when serialization fails."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    # Make serializer raise an exception
    mock_connection_manager.get_queue_backend().push.side_effect = ValueError(
      "Serialize error"
    )

    request = Request(url="https://example.com")

    with pytest.raises(SerializationError) as exc_info:
      queue.push(request)

    assert "Failed to serialize request" in str(exc_info.value)
    assert exc_info.value.serializer == "json"
    assert exc_info.value.data is request


class TestBackendQueuePop:
  """Test pop method."""

  def test_pop_returns_none_when_empty(self, mock_connection_manager):
    """Test pop returns None when queue backend returns None."""
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    result = queue.pop()

    assert result is None

  def test_pop_deserializes_and_returns_request(self, mock_connection_manager):
    """Test pop deserializes data and returns Request object."""
    mock_connection_manager.get_queue_backend().pop.return_value = (
      b'{"url": "https://example.com", "callback": null, "errback": null, '
      b'"method": "GET", "headers": {}, "body": null, "cookies": {}, '
      b'"meta": {}, "encoding": "utf-8", "priority": 0, "dont_filter": false, "flags": []}'
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    result = queue.pop()

    assert result is not None
    assert isinstance(result, Request)
    assert result.url == "https://example.com"

  def test_pop_passes_timeout_to_backend(self, mock_connection_manager):
    """Test pop passes timeout to queue backend."""
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    queue.pop(timeout=2.5)

    mock_connection_manager.get_queue_backend().pop.assert_called_once_with(
      "test_queue", 2.5
    )

  def test_pop_raises_serialization_error_on_deserialize_failure(
    self, mock_connection_manager
  ):
    """Test pop raises SerializationError when deserialization fails."""
    mock_connection_manager.get_queue_backend().pop.return_value = b"invalid json"
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    with pytest.raises(SerializationError) as exc_info:
      queue.pop()

    assert "Failed to deserialize request" in str(exc_info.value)
    assert exc_info.value.serializer == "json"
    assert exc_info.value.data == b"invalid json"


class TestBackendQueuePeek:
  """Test peek method."""

  def test_peek_returns_request_without_removing(self, mock_connection_manager):
    """Test peek returns request but does not remove it from queue."""
    mock_connection_manager.get_queue_backend().pop.return_value = (
      b'{"url": "https://example.com", "callback": null, "errback": null, '
      b'"method": "GET", "headers": {}, "body": null, "cookies": {}, '
      b'"meta": {}, "encoding": "utf-8", "priority": 5, "dont_filter": false, "flags": []}'
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    result = queue.peek()

    assert result is not None
    assert result.url == "https://example.com"
    assert result.priority == 5
    # Should have called pop once and push once
    assert mock_connection_manager.get_queue_backend().pop.call_count == 1
    assert mock_connection_manager.get_queue_backend().push.call_count == 1

  def test_peek_with_empty_queue(self, mock_connection_manager):
    """Test peek with empty queue returns None."""
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    result = queue.peek()

    assert result is None
    # push should not be called when pop returns None
    mock_connection_manager.get_queue_backend().push.assert_not_called()

  def test_peek_pushes_back_with_same_priority(self, mock_connection_manager):
    """Test peek pushes request back with same priority."""
    mock_connection_manager.get_queue_backend().pop.return_value = (
      b'{"url": "https://example.com", "callback": null, "errback": null, '
      b'"method": "GET", "headers": {}, "body": null, "cookies": {}, '
      b'"meta": {}, "encoding": "utf-8", "priority": 42, "dont_filter": false, "flags": []}'
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    queue.peek()

    push_call = mock_connection_manager.get_queue_backend().push.call_args
    # First arg is queue name, second is data, third is priority
    assert push_call[0][2] == 42


class TestBackendQueueLen:
  """Test __len__ method."""

  def test_len_returns_queue_len(self, mock_connection_manager):
    """Test __len__ returns queue length from backend."""
    mock_connection_manager.get_queue_backend().queue_len.return_value = 10
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    result = len(queue)

    assert result == 10
    mock_connection_manager.get_queue_backend().queue_len.assert_called_once_with(
      "test_queue"
    )


class TestBackendQueueClear:
  """Test clear method."""

  def test_clear_calls_clear_queue(self, mock_connection_manager):
    """Test clear calls backend's clear_queue."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    queue.clear()

    mock_connection_manager.get_queue_backend().clear_queue.assert_called_once_with(
      "test_queue"
    )
