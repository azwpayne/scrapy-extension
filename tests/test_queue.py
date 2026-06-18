"""Tests for BackendQueue component."""

from typing import Any, cast

import pytest
from scrapy import Spider
from scrapy.http import Request

from scrapy_extension.exceptions import QueueError, SerializationError
from scrapy_extension.queue.queue import BackendQueue


class _QueueTestSpider(Spider):
  name = "queue-test-spider"

  def parse_item(self, response):
    return response

  def handle_failure(self, failure):
    return failure


class TestBackendQueueInit:
  """Test BackendQueue initialization."""

  def test_init_sets_attributes(self, mock_connection_manager, mock_spider):
    """Test __init__ sets connection_manager and queue_name."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    assert queue.connection_manager is mock_connection_manager
    assert queue.queue_name == "test_queue"

  def test_serializer_lazy_initialized(self, mock_connection_manager, mock_spider):
    """Test serializer is lazily initialized via cached_property."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    # Access _serializer twice - should return same instance
    serializer1 = queue._serializer
    serializer2 = queue._serializer
    assert serializer1 is serializer2


class TestBackendQueueRequestToDict:
  """Test _request_to_dict method."""

  def test_basic_request_to_dict(self, mock_connection_manager, mock_spider):
    """Test converting a basic request to dict."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
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

  def test_request_to_dict_with_body_utf8(self, mock_connection_manager, mock_spider):
    """Test UTF-8 body is base64-encoded for safe JSON round-trip."""
    import base64

    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      body=b"hello world",
      encoding="utf-8",
    )
    result = queue._request_to_dict(request)

    assert result["body"] == base64.b64encode(b"hello world").decode("ascii")
    assert result["encoding"] == "utf-8"

  def test_request_to_dict_with_binary_body_uses_base64(
    self, mock_connection_manager, mock_spider
  ):
    """Binary bodies (non-UTF-8) must round-trip via base64, not latin-1.

    R1-P2-18: the old latin-1 fallback corrupted binary bodies because
    Scrapy's request_from_dict re-encodes the string as UTF-8.
    """
    import base64

    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      body=b"\xe9\x00\xff",  # Non-UTF-8 bytes
      encoding="utf-8",
    )
    result = queue._request_to_dict(request)

    assert result["body"] == base64.b64encode(b"\xe9\x00\xff").decode("ascii")

  def test_binary_body_round_trips_through_pop(self, mock_connection_manager, mock_spider):
    """Binary body pushed then popped must equal the original bytes."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    original_body = b"\xe9\x00\xff\x42"
    request = Request(url="https://example.com", body=original_body)
    request_dict = queue._request_to_dict(request)
    serialized = queue._serializer.serialize(request_dict)

    # Simulate pop: deserialize + decode body
    deserialized = cast("dict[str, Any]", queue._serializer.deserialize(serialized))
    queue._decode_body(deserialized)
    assert deserialized["body"] == original_body

  def test_request_to_dict_with_headers(self, mock_connection_manager, mock_spider):
    """Test request with headers converts to dict."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      headers={"Content-Type": "application/json"},
    )
    result = queue._request_to_dict(request)

    assert result["headers"] == {"Content-Type": "application/json"}

  def test_request_to_dict_with_cookies(self, mock_connection_manager, mock_spider):
    """Test request with cookies."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      cookies={"name": "value"},
    )
    result = queue._request_to_dict(request)

    assert result["cookies"] == {"name": "value"}

  def test_request_to_dict_with_meta(self, mock_connection_manager, mock_spider):
    """Test request with meta."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      meta={"key": "value"},
    )
    result = queue._request_to_dict(request)

    assert result["meta"] == {"key": "value"}

  def test_request_to_dict_preserves_cb_kwargs(
    self, mock_connection_manager, mock_spider
  ):
    """cb_kwargs must be serialized — Scrapy 2.x recommends cb_kwargs over meta.

    Without this, Request(url, cb_kwargs={"item_id": 123}) loses item_id on
    push/pop, and the callback raises TypeError for missing required kwargs.
    """
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      cb_kwargs={"item_id": 123, "category": "books"},
    )
    result = queue._request_to_dict(request)

    assert result["cb_kwargs"] == {"item_id": 123, "category": "books"}

  def test_request_to_dict_default_cb_kwargs_is_empty_dict(
    self, mock_connection_manager, mock_spider
  ):
    """Default cb_kwargs (no kwargs passed) serializes as empty dict."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(url="https://example.com")
    result = queue._request_to_dict(request)

    assert result["cb_kwargs"] == {}

  def test_cb_kwargs_round_trips_through_serialize(
    self, mock_connection_manager, mock_spider
  ):
    """cb_kwargs must survive serialize -> deserialize -> request_from_dict.

    Validates the full push/pop path: any spider using cb_kwargs (the
    Scrapy 2.x recommended way to pass data to callbacks) gets the same
    cb_kwargs back after the queue round-trip.
    """
    from scrapy.utils.request import request_from_dict

    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      cb_kwargs={"item_id": 42, "tags": ["a", "b"], "nested": {"x": 1}},
    )
    request_dict = queue._request_to_dict(request)
    serialized = queue._serializer.serialize(request_dict)

    deserialized = cast("dict[str, Any]", queue._serializer.deserialize(serialized))
    queue._decode_body(deserialized)
    restored = request_from_dict(deserialized, spider=None)

    assert restored.cb_kwargs == {"item_id": 42, "tags": ["a", "b"], "nested": {"x": 1}}

  def test_request_to_dict_with_priority(self, mock_connection_manager, mock_spider):
    """Test request with priority."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      priority=10,
    )
    result = queue._request_to_dict(request)

    assert result["priority"] == 10

  def test_request_to_dict_dont_filter(self, mock_connection_manager, mock_spider):
    """Test request with dont_filter flag."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      dont_filter=True,
    )
    result = queue._request_to_dict(request)

    assert result["dont_filter"] is True

  def test_request_to_dict_with_flags(self, mock_connection_manager, mock_spider):
    """Test request with flags."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      flags=["flag1", "flag2"],
    )
    result = queue._request_to_dict(request)

    assert result["flags"] == ["flag1", "flag2"]

  def test_request_to_dict_with_callback(self, mock_connection_manager, mock_spider):
    """Test request with callback function name captured."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )

    def my_callback(response):
      pass

    request = Request(
      url="https://example.com",
      callback=my_callback,
    )
    result = queue._request_to_dict(request)

    assert result["callback"] == "my_callback"

  def test_request_to_dict_with_errback(self, mock_connection_manager, mock_spider):
    """Test request with errback function name captured."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )

    def my_errback(failure):
      pass

    request = Request(
      url="https://example.com",
      errback=my_errback,
    )
    result = queue._request_to_dict(request)

    assert result["errback"] == "my_errback"

  def test_request_to_dict_empty_body(self, mock_connection_manager, mock_spider):
    """Test request with empty body."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(
      url="https://example.com",
      body=b"",
    )
    result = queue._request_to_dict(request)

    assert result["body"] is None


class TestBackendQueuePush:
  """Test push method."""

  def test_push_serializes_and_calls_backend(self, mock_connection_manager, mock_spider):
    """Test push serializes request and calls queue backend."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
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

  def test_push_with_default_priority(self, mock_connection_manager, mock_spider):
    """Test push uses default priority of 0.0."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(url="https://example.com")
    queue.push(request)

    call_args = mock_connection_manager.get_queue_backend().push.call_args
    assert call_args[0][2] == 0.0

  def test_ack_delegates_to_queue_backend(self, mock_connection_manager, mock_spider):
    """R11: ack() delegates to the backend's ack with the queue name."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    queue.ack()
    mock_connection_manager.get_queue_backend().ack.assert_called_once_with("test_queue")

  def test_nack_delegates_to_queue_backend(self, mock_connection_manager, mock_spider):
    """R11: nack() delegates to the backend's nack with the queue name."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    queue.nack()
    mock_connection_manager.get_queue_backend().nack.assert_called_once_with("test_queue")

  def test_decode_body_raises_on_invalid_base64(self):
    """R17: _decode_body raises SerializationError on non-base64 body.

    Covers the corruption-detection path: a queued request whose body isn't
    valid base64 (queue tampering, version skew) surfaces as a loud
    SerializationError, not a silent wrong decode.
    """
    with pytest.raises(SerializationError, match="Invalid base64 body"):
      BackendQueue._decode_body({"body": "!!!not-base64!!!"})

  def test_push_raises_serialization_error_on_exception(self, mock_connection_manager, mock_spider):
    """Test push raises SerializationError when request serialization fails."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(url="https://example.com")

    def broken_request_to_dict(_: Request) -> dict[str, Any]:
      raise ValueError("Serialize error")

    queue._request_to_dict = broken_request_to_dict

    with pytest.raises(SerializationError) as exc_info:
      queue.push(request)

    assert "Failed to serialize request" in str(exc_info.value)
    assert exc_info.value.serializer == "json"
    assert exc_info.value.data is request

  def test_push_propagates_backend_queue_error(self, mock_connection_manager, mock_spider):
    """Backend queue failures must not be wrapped as SerializationError."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    backend_error = QueueError("Push failed", queue_name="test_queue", operation="push")
    mock_connection_manager.get_queue_backend().push.side_effect = backend_error

    request = Request(url="https://example.com")

    with pytest.raises(QueueError) as exc_info:
      queue.push(request)

    assert exc_info.value is backend_error


class TestBackendQueuePop:
  """Test pop method."""

  def test_pop_restores_callback_and_errback_with_spider(self, mock_connection_manager):
    """Test pop restores spider callback and errback callables."""
    spider = _QueueTestSpider()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=spider,
    )
    request = Request(
      url="https://example.com",
      callback=spider.parse_item,
      errback=spider.handle_failure,
    )
    queue.push(request)

    pushed_data = mock_connection_manager.get_queue_backend().push.call_args[0][1]
    mock_connection_manager.get_queue_backend().pop.return_value = pushed_data

    result = queue.pop()

    assert result is not None
    assert result.callback == spider.parse_item
    assert result.errback == spider.handle_failure

  def test_pop_roundtrips_binary_body_without_corruption(self, mock_connection_manager):
    """Test pop preserves arbitrary binary request bodies."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      method="POST",
      body=b"\x00\xff\x10payload",
    )
    queue.push(request)

    pushed_data = mock_connection_manager.get_queue_backend().push.call_args[0][1]
    mock_connection_manager.get_queue_backend().pop.return_value = pushed_data

    result = queue.pop()

    assert result is not None
    assert result.body == b"\x00\xff\x10payload"

  def test_pop_roundtrips_request_metadata(self, mock_connection_manager):
    """Test pop preserves request metadata fields."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      meta={"trace_id": "abc123", "retry_times": 2},
      priority=17,
      flags=["seed", "retry"],
      dont_filter=True,
    )
    queue.push(request)

    pushed_data = mock_connection_manager.get_queue_backend().push.call_args[0][1]
    mock_connection_manager.get_queue_backend().pop.return_value = pushed_data

    result = queue.pop()

    assert result is not None
    assert result.meta == {"trace_id": "abc123", "retry_times": 2}
    assert result.priority == 17
    assert result.flags == ["seed", "retry"]
    assert result.dont_filter is True

  def test_pop_returns_none_when_empty(self, mock_connection_manager, mock_spider):
    """Test pop returns None when queue backend returns None."""
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )

    result = queue.pop()

    assert result is None

  def test_pop_deserializes_and_returns_request(self, mock_connection_manager, mock_spider):
    """Test pop deserializes data and returns Request object."""
    mock_connection_manager.get_queue_backend().pop.return_value = (
      b'{"url": "https://example.com", "callback": null, "errback": null, '
      b'"method": "GET", "headers": {}, "body": null, "cookies": {}, '
      b'"meta": {}, "encoding": "utf-8", "priority": 0, "dont_filter": false, "flags": []}'
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )

    result = queue.pop()

    assert result is not None
    assert isinstance(result, Request)
    assert result.url == "https://example.com"

  def test_pop_passes_timeout_to_backend(self, mock_connection_manager, mock_spider):
    """Test pop passes timeout to queue backend."""
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    queue.pop(timeout=2.5)

    mock_connection_manager.get_queue_backend().pop.assert_called_once_with(
      "test_queue", 2.5
    )

  def test_pop_raises_serialization_error_on_deserialize_failure(
    self, mock_connection_manager, mock_spider
  ):
    """Test pop raises SerializationError when deserialization fails."""
    mock_connection_manager.get_queue_backend().pop.return_value = b"invalid json"
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )

    with pytest.raises(SerializationError) as exc_info:
      queue.pop()

    assert "Failed to deserialize request" in str(exc_info.value)
    assert exc_info.value.serializer == "json"
    assert exc_info.value.data == b"invalid json"


class TestBackendQueueLen:
  """Test __len__ method."""

  def test_len_returns_queue_len(self, mock_connection_manager, mock_spider):
    """Test __len__ returns queue length from backend."""
    mock_connection_manager.get_queue_backend().queue_len.return_value = 10
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )

    result = len(queue)

    assert result == 10
    mock_connection_manager.get_queue_backend().queue_len.assert_called_once_with(
      "test_queue"
    )


class TestBackendQueueClear:
  """Test clear method."""

  def test_clear_calls_clear_queue(self, mock_connection_manager, mock_spider):
    """Test clear calls backend's clear_queue."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )

    queue.clear()

    mock_connection_manager.get_queue_backend().clear_queue.assert_called_once_with(
      "test_queue"
    )
