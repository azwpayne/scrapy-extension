"""Tests for BackendQueue component."""

import math
from typing import Any, cast

import pytest
from scrapy import Spider
from scrapy.http import JsonRequest, Request
from scrapy.utils.request import request_from_dict

from scrapy_extension.backends.base import JSONSerializer
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

  def test_request_to_dict_strips_non_serializable_ack_token(
    self, mock_connection_manager, mock_spider
  ):
    """R-ack-serialize: the backend ack token is an opaque, non-JSON-serializable
    object (RocketMQ apache ``Message``; Pulsar ``MessageId``). Scrapy's retry
    middleware copies a failed request's meta (token included) and re-enqueues
    it via ``push`` → ``_request_to_dict``. If the token reaches the JSON
    serializer, serialization crashes (``SerializationError``) and the retry is
    DROPPED — broken retry on RocketMQ/Pulsar. The fix strips
    ``_backend_ack_token`` at the serialization boundary (non-mutating — the
    in-memory request keeps the token so the scheduler can still ack on
    ``response_received`` / ``spider_error`` after the download).
    """
    import json

    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    # Opaque non-serializable ack token (stands in for apache Message / Pulsar MessageId).
    request = Request(
      url="https://example.com",
      meta={"_backend_ack_token": object(), "keep": 1},
    )
    result = queue._request_to_dict(request)

    # The ack token must NOT survive into the serialized meta.
    assert "_backend_ack_token" not in result["meta"], (
      "ack token must be stripped at the serialization boundary"
    )
    # Other meta keys must survive.
    assert result["meta"]["keep"] == 1
    # The resulting dict MUST be JSON-serializable (the actual retry crash).
    json.dumps(result)  # must not raise
    # The in-memory request still carries the token (scheduler acks post-download).
    assert "_backend_ack_token" in request.meta

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

    assert result["headers"] == {"Content-Type": [b"application/json"]}

  def test_multiple_header_values_round_trip_without_being_joined(
    self, mock_connection_manager
  ):
    """Repeated headers are ordered values, not one comma-joined value."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    request = Request(
      url="https://example.com",
      headers={"Set-Cookie": [b"a=1", b"b=2"]},
    )

    request_dict = queue._request_to_dict(request)
    wire = queue._serializer.serialize(request_dict)
    recovered = cast("dict[str, Any]", queue._serializer.deserialize(wire))
    queue._decode_body(recovered)
    restored = request_from_dict(recovered)

    assert restored.headers.getlist("Set-Cookie") == [b"a=1", b"b=2"]

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

  def test_request_to_dict_with_callback(self, mock_connection_manager):
    """Test request with callback function name captured."""
    spider = _QueueTestSpider()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=spider,
    )

    request = Request(
      url="https://example.com",
      callback=spider.parse_item,
    )
    result = queue._request_to_dict(request)

    assert result["callback"] == "parse_item"

  def test_request_to_dict_with_errback(self, mock_connection_manager):
    """Test request with errback function name captured."""
    spider = _QueueTestSpider()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=spider,
    )

    request = Request(
      url="https://example.com",
      errback=spider.handle_failure,
    )
    result = queue._request_to_dict(request)

    assert result["errback"] == "handle_failure"

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

  def test_push_rejects_callback_that_cannot_be_restored(
    self, mock_connection_manager
  ):
    """A callback not bound to the queue spider must fail before enqueue."""
    spider = _QueueTestSpider()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=spider,
    )

    def foreign_callback(response):
      return response

    with pytest.raises(SerializationError, match="instance method"):
      queue.push(Request("https://example.com", callback=foreign_callback))

    mock_connection_manager.get_queue_backend().push.assert_not_called()

  def test_push_pop_preserves_request_subclass(
    self, mock_connection_manager, mocker
  ):
    """The serialized envelope retains Scrapy's request class discriminator."""
    strategy = mocker.MagicMock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      queue_strategy=strategy,
    )
    request = JsonRequest("https://example.com", data={"x": 1})

    queue.push(request)
    strategy.pop_with_ack.return_value = (strategy.push.call_args.args[1], None)

    restored = queue.pop()
    assert isinstance(restored, JsonRequest)

  def test_push_rejects_unallowlisted_request_subclass_before_enqueue(
    self, mock_connection_manager
  ):
    class CustomRequest(Request):
      pass

    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    with pytest.raises(SerializationError, match="request class"):
      queue.push(CustomRequest("https://example.com"))

    mock_connection_manager.get_queue_backend().push.assert_not_called()

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
    """R17/D1: _decode_body raises SerializationError on structurally corrupt body.

    After D1, valid-UTF-8 non-base64 bodies are migrated (legacy format). The
    corruption path that still raises is a body that is neither valid base64
    nor UTF-8-encodable — e.g. a lone surrogate, the only ``str`` content that
    ``encode("utf-8")`` rejects. Surfaces as a loud SerializationError, not a
    silent wrong decode.
    """
    # Lone surrogate: str.encode("utf-8") raises UnicodeEncodeError.
    corrupt_body = "\udcff"
    with pytest.raises(SerializationError, match="Invalid base64 body"):
      BackendQueue._decode_body({"body": corrupt_body})

  def test_decode_body_falls_back_for_legacy_utf8_body(self):
    """D1: legacy non-base64 UTF-8 body round-trips instead of being dropped.

    Pre-base64 package versions wrote raw UTF-8/latin-1 bodies to the queue.
    On rolling upgrade, those queued items would hit ``b64decode(validate=True)``
    and raise ``SerializationError`` → scheduler silently drops the request.

    Fix: detect a legacy body (not valid base64 but valid UTF-8) and fall
    back to ``body.encode("utf-8")`` + emit a one-time ``DeprecationWarning``.
    Structural corruption (not valid base64 AND not UTF-8-encodable) still raises.
    """
    import warnings

    legacy_body_str = "hello world"  # valid UTF-8, not valid base64-padding
    request_dict = {"body": legacy_body_str}

    with warnings.catch_warnings(record=True) as caught:
      warnings.simplefilter("always")
      BackendQueue._decode_body(request_dict)

    assert request_dict["body"] == b"hello world"
    assert any(
      issubclass(w.category, DeprecationWarning)
      and "legacy" in str(w.message).lower()
      for w in caught
    ), f"Expected a legacy-body DeprecationWarning, got: {caught}"

  def test_versioned_body_rejects_corrupt_base64_without_legacy_fallback(self):
    """A damaged current-format body must not be mistaken for legacy UTF-8."""
    request_dict = {
      "body": "YW!j",
      "_scrapy_extension_body_codec": "base64-v1",
    }

    with pytest.raises(SerializationError, match="Invalid base64 body"):
      BackendQueue._decode_body(request_dict)

  def test_decode_body_structural_corruption_still_raises(self):
    """D1: a lone surrogate (neither base64 nor UTF-8-encodable) still raises.

    The legacy fallback must not mask genuine corruption — only UTF-8-encodable
    bodies (the pre-base64 format) are migrated; a lone surrogate raises.
    """
    # Lone surrogate: str.encode("utf-8") raises UnicodeEncodeError.
    with pytest.raises(SerializationError, match="Invalid base64 body"):
      BackendQueue._decode_body({"body": "\udcff"})

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

  def test_push_replacement_acks_consumed_delivery_token_after_enqueue(
    self, mock_connection_manager, mock_spider, mocker
  ):
    """A retry/redirect replacement must terminate its original MQ delivery."""
    strategy = mocker.MagicMock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      queue_strategy=strategy,
    )
    request = Request(
      url="https://example.com/retry",
      meta={"_backend_ack_token": "old-token", "keep": True},
    )

    queue.push(request)

    strategy.push.assert_called_once()
    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="old-token"
    )
    assert "_backend_ack_token" not in request.meta

  def test_push_failure_keeps_original_delivery_unacked(
    self, mock_connection_manager, mock_spider, mocker
  ):
    """The original delivery remains recoverable until replacement enqueue commits."""
    strategy = mocker.MagicMock()
    strategy.push.side_effect = QueueError("push failed")
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      queue_strategy=strategy,
    )
    request = Request(
      url="https://example.com/retry",
      meta={"_backend_ack_token": "old-token"},
    )

    with pytest.raises(QueueError, match="push failed"):
      queue.push(request)

    mock_connection_manager.get_queue_backend().ack.assert_not_called()
    assert request.meta["_backend_ack_token"] == "old-token"

  @pytest.mark.parametrize(
    "delay",
    ["not-a-number", -1, float("nan"), float("inf"), float("-inf")],
  )
  def test_push_rejects_invalid_delay_without_mutating_meta(
    self, mock_connection_manager, mocker, delay
  ):
    strategy = mocker.MagicMock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      queue_strategy=strategy,
    )
    request = Request(
      "https://example.com",
      meta={"delay": delay, "source": "feed-a"},
    )

    with pytest.raises(QueueError, match="delay"):
      queue.push(request)

    if isinstance(delay, float) and math.isnan(delay):
      assert math.isnan(request.meta["delay"])
    else:
      assert request.meta["delay"] == delay
    assert request.meta["source"] == "feed-a"
    strategy.push.assert_not_called()

  def test_push_failure_preserves_routing_meta_for_retry(
    self, mock_connection_manager, mocker
  ):
    strategy = mocker.MagicMock()
    strategy.push.side_effect = QueueError("backend down")
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      queue_strategy=strategy,
    )
    request = Request(
      "https://example.com",
      meta={"delay": 5.0, "source": "feed-a"},
    )

    with pytest.raises(QueueError, match="backend down"):
      queue.push(request)

    assert request.meta["delay"] == 5.0
    assert request.meta["source"] == "feed-a"

  def test_invalid_replacement_serialization_terminates_old_delivery(
    self, mock_connection_manager
  ):
    request = Request(
      "https://example.com",
      meta={"_backend_ack_token": "old-token", "bad": object()},
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    with pytest.raises(SerializationError):
      queue.push(request)

    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="old-token"
    )
    assert "_backend_ack_token" not in request.meta

  def test_invalid_replacement_delay_terminates_old_delivery(
    self, mock_connection_manager
  ):
    request = Request(
      "https://example.com",
      meta={"_backend_ack_token": "old-token", "delay": "invalid"},
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )

    with pytest.raises(QueueError, match="delay"):
      queue.push(request)

    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="old-token"
    )
    assert "_backend_ack_token" not in request.meta

  def test_oversize_replacement_terminates_old_delivery(
    self, mock_connection_manager
  ):
    request = Request(
      "https://example.com",
      body=b"x" * 256,
      meta={"_backend_ack_token": "old-token"},
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      max_item_bytes=64,
    )

    with pytest.raises(SerializationError, match="exceeds max_item_bytes"):
      queue.push(request)

    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="old-token"
    )
    assert "_backend_ack_token" not in request.meta

  # ----- R14-F HIGH: retry + delay/source storm prevention -----

  def test_push_pops_delay_from_meta_so_retry_does_not_re_delay(
    self, mock_connection_manager, mock_spider
  ):
    """R14-F HIGH: ``push`` must consume ``delay`` from ``request.meta`` so a
    re-pushed retry does NOT re-apply the original delay indefinitely.

    Regression guard for the retry+delay storm: pre-fix ``push`` read
    ``request.meta['delay']`` and forwarded it to the strategy but never
    removed it, so when Scrapy's retry middleware re-queued the request
    (carrying the same meta), the delay re-applied — potentially forever.

    Breaking behavior change (documented in the push docstring): callers
    that push the same request object more than once and want the delay to
    apply each time must re-set ``request.meta['delay']`` between pushes.
    """
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(url="https://example.com", meta={"delay": 30.0})
    queue.push(request, priority=0.0)
    # The first push must have consumed `delay` from meta.
    assert "delay" not in request.meta, (
      "push did not pop 'delay' from request.meta — a retry re-push would "
      "re-apply the same delay (R14-F HIGH retry+delay storm)"
    )

  def test_push_pops_source_from_meta_so_retry_does_not_pin_source(
    self, mock_connection_manager, mock_spider
  ):
    """R14-F HIGH: ``push`` consumes ``source`` from meta alongside ``delay``,
    so a re-pushed retry is not pinned to its original source tag (which
    would defeat round-robin fairness on the retry path)."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    request = Request(url="https://example.com", meta={"source": "feed-A"})
    queue.push(request, priority=0.0)
    assert "source" not in request.meta

  def test_push_passes_delay_and_source_to_strategy_before_popping(
    self, mock_connection_manager, mock_spider, mocker
  ):
    """R14-F: the delay/source values ARE forwarded to the strategy on the
    first push (the pop-from-meta happens AFTER the read, so the strategy
    still observes the original values). Pins the read-then-pop order so a
    refactor doesn't accidentally drop the values before forwarding."""
    # Inject a mock strategy to observe the kwargs BackendQueue forwards.
    mock_strategy = mocker.MagicMock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      queue_strategy=mock_strategy,
    )
    request = Request(url="https://example.com", meta={"delay": 5.0, "source": "X"})
    queue.push(request, priority=2.0)

    mock_strategy.push.assert_called_once()
    call = mock_strategy.push.call_args
    assert call.args[0] == "test_queue"
    assert isinstance(call.args[1], bytes)
    assert call.kwargs["priority"] == 2.0
    assert call.kwargs["delay"] == 5.0  # forwarded on first push
    assert call.kwargs["source"] == "X"
    # And the meta was still consumed for the retry path.
    assert "delay" not in request.meta
    assert "source" not in request.meta

  def test_push_re_push_after_delay_pop_does_not_re_apply_delay(
    self, mock_connection_manager, mock_spider, mocker
  ):
    """R14-F HIGH end-to-end: simulate the retry path — push a delayed
    request (meta consumed); re-push the SAME request object (as Scrapy
    retry middleware would); the second push forwards delay=0.0 because
    the meta was popped on the first push. This is the storm-prevention
    contract: retries do not re-delay."""
    mock_strategy = mocker.MagicMock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      queue_strategy=mock_strategy,
    )
    request = Request(url="https://example.com", meta={"delay": 10.0})
    queue.push(request, priority=0.0)
    first_call = mock_strategy.push.call_args
    assert first_call.kwargs["delay"] == 10.0  # first push forwards it

    # Second push of the SAME request object — as if retried.
    queue.push(request, priority=0.0)
    second_call = mock_strategy.push.call_args
    assert second_call.kwargs["delay"] == 0.0, (
      "retry re-push forwarded delay again — retry+delay storm not prevented"
    )

  def test_push_pop_round_trip_does_not_restore_consumed_routing_meta(
    self, mock_connection_manager, mock_spider, mocker
  ):
    """Consumed routing controls must not survive in the persisted request."""
    mock_strategy = mocker.MagicMock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      queue_strategy=mock_strategy,
    )
    request = Request(
      url="https://example.com",
      meta={"delay": 10.0, "source": "feed-A", "keep": "value"},
    )

    queue.push(request)
    persisted = mock_strategy.push.call_args.args[1]
    mock_strategy.pop_with_ack.return_value = (persisted, None)

    restored = queue.pop()

    assert restored is not None
    assert restored.meta == {"keep": "value"}


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

  def test_pop_rejects_callable_attribute_that_is_not_bound_spider_method(
    self, mock_connection_manager, mocker
  ):
    spider = _QueueTestSpider()
    strategy = mocker.MagicMock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=spider,
      queue_strategy=strategy,
    )
    payload = queue._request_to_dict(Request("https://example.com"))
    payload["callback"] = "__class__"
    strategy.pop_with_ack.return_value = (
      JSONSerializer().serialize(payload),
      "token-1",
    )

    with pytest.raises(SerializationError, match="instance method"):
      queue.pop()

    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="token-1"
    )

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

  def test_pop_terminates_empty_payload_with_delivery_token(
    self, mock_connection_manager, mocker
  ):
    """Kafka tombstones and equivalent empty deliveries cannot pin in-flight state."""
    strategy = mocker.MagicMock()
    strategy.pop_with_ack.return_value = (None, "tombstone-token")
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      queue_strategy=strategy,
    )

    assert queue.pop() is None
    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="tombstone-token"
    )

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

  def test_pop_acks_and_drops_token_when_deserialization_fails(
    self, mock_connection_manager, mock_spider, mocker
  ):
    """An unrecoverable MQ payload is terminated instead of poison-looped."""
    strategy = mocker.MagicMock()
    strategy.pop_with_ack.return_value = (b"invalid json", "token-1")
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      queue_strategy=strategy,
    )

    with pytest.raises(SerializationError):
      queue.pop()

    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="token-1"
    )
    mock_connection_manager.get_queue_backend().nack.assert_not_called()

  def test_pop_records_poison_drop_stat(
    self, mock_connection_manager, mocker
  ):
    strategy = mocker.MagicMock()
    strategy.pop_with_ack.return_value = (b"invalid json", "token-1")
    spider = mocker.MagicMock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=spider,
      queue_strategy=strategy,
    )

    with pytest.raises(SerializationError):
      queue.pop()

    spider.crawler.stats.inc_value.assert_any_call(
      "scheduler/queue/poison_dropped"
    )

  def test_pop_rejects_oversize_backend_payload_before_deserializing(
    self, mock_connection_manager, mocker
  ):
    """The receive path enforces the same byte cap as enqueue."""
    strategy = mocker.MagicMock()
    strategy.pop_with_ack.return_value = (b"{" + b"x" * 128, "token-1")
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      queue_strategy=strategy,
      max_item_bytes=64,
    )
    deserialize = mocker.spy(queue._serializer, "deserialize")

    with pytest.raises(SerializationError, match="exceeds max_item_bytes"):
      queue.pop()

    deserialize.assert_not_called()
    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="token-1"
    )

  @pytest.mark.parametrize(
    ("field", "value"),
    [
      ("dont_filter", "false"),
      ("flags", "admin"),
      ("method", 123),
      ("meta", [["depth", 1]]),
    ],
  )
  def test_pop_rejects_wrong_typed_request_fields(
    self, mock_connection_manager, mocker, field, value
  ):
    strategy = mocker.MagicMock()
    payload = {
      "url": "https://example.com",
      "callback": None,
      "errback": None,
      "method": "GET",
      "headers": {},
      "body": None,
      "cookies": {},
      "meta": {},
      "cb_kwargs": {},
      "encoding": "utf-8",
      "priority": 0,
      "dont_filter": False,
      "flags": [],
    }
    payload[field] = value
    strategy.pop_with_ack.return_value = (
      JSONSerializer().serialize(payload),
      "token-1",
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      queue_strategy=strategy,
    )

    with pytest.raises(SerializationError, match=field):
      queue.pop()

    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="token-1"
    )

  def test_pop_rejects_forged_request_class_without_dynamic_loading(
    self, mock_connection_manager, mocker
  ):
    strategy = mocker.MagicMock()
    payload = {
      "url": "https://example.com",
      "callback": None,
      "errback": None,
      "method": "GET",
      "headers": {},
      "body": None,
      "cookies": {},
      "meta": {},
      "cb_kwargs": {},
      "encoding": "utf-8",
      "priority": 0,
      "dont_filter": False,
      "flags": [],
      "_class": "builtins.dict",
    }
    strategy.pop_with_ack.return_value = (
      JSONSerializer().serialize(payload),
      "token-1",
    )
    dynamic_loader = mocker.patch(
      "scrapy.utils.request.load_object",
      side_effect=AssertionError("untrusted dynamic load"),
    )
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      queue_strategy=strategy,
    )

    with pytest.raises(SerializationError, match="request class"):
      queue.pop()

    dynamic_loader.assert_not_called()
    mock_connection_manager.get_queue_backend().ack.assert_called_once_with(
      "test_queue", token="token-1"
    )


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

  def test_clear_invalidates_cached_nonzero_depth(
    self, mock_connection_manager, mock_spider
  ):
    backend = mock_connection_manager.get_queue_backend()
    backend.queue_len.return_value = 7
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      depth_sample_every=100,
    )
    assert len(queue) == 7

    backend.queue_len.return_value = 0
    queue.clear()

    assert len(queue) == 0
    assert backend.queue_len.call_count == 2


class TestBackendQueueMaxItemBytes:
  """D2: configurable per-item byte cap to prevent DoS via oversize payloads."""

  def test_push_oversize_payload_raises_and_increments_stat(
    self, mock_connection_manager, mocker
  ):
    """D2: an oversize serialized request raises SerializationError + bumps stat.

    A hostile target can push arbitrarily large request bodies; storage backends
    with caps (Memcached 1 MB, DynamoDB 400 KB) throw and the item is silently
    dropped. The cap surfaces the oversize condition loudly at push time with
    a stat increment so operators can see it on dashboards.
    """
    # Use an unspec'd mock so spider.crawler.stats is reachable (the production
    # code resolves stats defensively via getattr).
    spider = mocker.Mock()
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=spider,
      max_item_bytes=64,
    )
    request = Request(url="https://example.com", body=b"x" * 200)

    with pytest.raises(SerializationError, match="exceeds.*max"):
      queue.push(request)

    spider.crawler.stats.inc_value.assert_called_with(
      "scheduler/queue/oversize_dropped"
    )
    # Backend push never happened — rejected before strategy.push.
    mock_connection_manager.get_queue_backend().push.assert_not_called()

  def test_push_normal_size_payload_succeeds(
    self, mock_connection_manager, mock_spider
  ):
    """D2: a normal-size payload is unaffected by the cap."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      max_item_bytes=1_048_576,
    )
    request = Request(url="https://example.com")
    queue.push(request)

    mock_connection_manager.get_queue_backend().push.assert_called_once()

  def test_push_no_spider_stats_does_not_raise(self, mock_connection_manager):
    """D2: stat increment tolerates a queue without spider.crawler.stats.

    Mirrors the pipeline's defensive ``_inc_stat``: a missing crawler must not
    crash the push path — the SerializationError is still raised (the loud
    signal) but the stat is skipped.
    """
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      max_item_bytes=64,
    )
    request = Request(url="https://example.com", body=b"x" * 200)

    with pytest.raises(SerializationError, match="exceeds.*max"):
      queue.push(request)

  def test_default_max_item_bytes_allows_typical_request(
    self, mock_connection_manager, mock_spider
  ):
    """D2: default cap (1 MiB) allows typical request payloads."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
    )
    assert queue.max_item_bytes == 1_048_576
    request = Request(url="https://example.com", body=b"x" * 10_000)
    queue.push(request)
    mock_connection_manager.get_queue_backend().push.assert_called_once()


class TestBackendQueueMonitorWiring:
  """Unit F: BackendQueue emits monitor hooks on push/pop (additive)."""

  def test_push_with_explicit_monitor_emits_on_push(
    self, mock_connection_manager, mocker
  ):
    """Push with a ScrapyStatsMonitor increments queue/push_count."""
    from scrapy.statscollectors import MemoryStatsCollector

    from scrapy_extension.monitor import ScrapyStatsMonitor

    stats = MemoryStatsCollector(mocker.MagicMock())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      monitor=ScrapyStatsMonitor(stats),
    )
    queue.push(Request(url="https://example.com"))
    assert stats.get_value("queue/push_count") == 1

  def test_pop_with_explicit_monitor_emits_on_pop(
    self, mock_connection_manager, mocker
  ):
    """Pop with a ScrapyStatsMonitor increments queue/pop_count."""
    from scrapy.statscollectors import MemoryStatsCollector

    from scrapy_extension.monitor import ScrapyStatsMonitor

    stats = MemoryStatsCollector(mocker.MagicMock())
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      monitor=ScrapyStatsMonitor(stats),
    )
    mock_connection_manager.get_queue_backend().pop.return_value = None
    queue.pop()
    assert stats.get_value("queue/pop_count") == 1


class TestBackendQueueDepthSampling:
  """U4: queue_len sampling — cut ~25% off pop-path RTT.

  ``queue_len`` (e.g. ZCARD) fires on every pop via ``monitor.on_queue_depth``.
  +1 RTT/pop = +25% of pop-path RTT for a depth signal that changes slowly
  relative to pop rate. Sampling at 1/N keeps the backpressure signal fresh
  while amortizing the RPC cost.

  Emptiness-correctness invariant: when the underlying backend reports 0,
  every probe MUST return 0 immediately (no stale non-zero cache) so Scrapy
  idle detection still trips correctly.
  """

  def test_default_depth_sample_every_is_100(self, mock_connection_manager):
    """U4: default sampling window is 100 (spec-mandated default)."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
    )
    assert queue.depth_sample_every == 100

  def test_depth_sample_every_kwarg_is_opt_in(self, mock_connection_manager):
    """U4: caller can set the sampling window explicitly."""
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      depth_sample_every=5,
    )
    assert queue.depth_sample_every == 5

  def test_sample_every_1_calls_queue_len_every_pop(
    self, mock_connection_manager, mock_spider
  ):
    """U4: ``depth_sample_every=1`` preserves the pre-sampling behavior (backward-compat)."""
    backend = mock_connection_manager.get_queue_backend()
    backend.pop.return_value = None
    backend.queue_len.return_value = 0
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      depth_sample_every=1,
    )

    for _ in range(5):
      queue.pop()

    # Every pop probes depth when window=1 (legacy behavior).
    assert backend.queue_len.call_count == 5

  def test_sampling_reduces_queue_len_calls(self, mock_connection_manager, mock_spider):
    """U4 RED→GREEN: with window=5 and 20 pops, real queue_len calls <= 4.

    Today (no sampling) this would be 20 calls. With sampling at 1/5 it must
    be at most ceil(20/5) = 4 real RPCs — the rest return cached depth.
    """
    backend = mock_connection_manager.get_queue_backend()
    backend.pop.return_value = None
    backend.queue_len.return_value = 42
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      depth_sample_every=5,
    )

    for _ in range(20):
      queue.pop()

    # 20 pops / window-of-5 = at most 4 real depth probes (rounded).
    assert backend.queue_len.call_count <= 4
    # And at least one probe happened (the signal stays alive).
    assert backend.queue_len.call_count >= 1

  def test_empty_queue_never_caches_stale_nonzero_depth(
    self, mock_connection_manager, mock_spider
  ):
    """U4 emptiness-correctness invariant.

    Per the SPEC rule of thumb: "when the underlying backend reports 0, always
    return 0 immediately (no cache); sampling only applies to the non-zero
    depth probe." Concretely this codebase enforces it as: while the cache
    holds 0 (or is uninitialized) every call re-probes the backend — sampling
    only skips the RPC while the *cached* depth is non-zero. So:

    - An empty-from-the-start queue reports 0 on every probe (cache never goes
      stale-nonzero). This is the case Scrapy idle detection hits while a crawl
      winds down with the queue already empty.
    - Once a real probe returns 0, the cache holds 0 and stays fresh on every
      subsequent call (no stale masking) — the queue cannot appear non-empty
      after it has drained to a probe-confirmed 0.
    """
    backend = mock_connection_manager.get_queue_backend()
    backend.pop.return_value = None
    backend.queue_len.return_value = 0
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      depth_sample_every=5,
    )

    # Empty from the start: every probe returns 0 (no stale non-zero possible).
    for _ in range(20):
      assert len(queue) == 0
    # And the backend was actually probed every call (cache never short-circuits 0).
    assert backend.queue_len.call_count == 20

  def test_drained_queue_re_confirms_zero_within_one_window(
    self, mock_connection_manager, mock_spider
  ):
    """U4 drain semantics: a probe-confirmed 0 is reported on every subsequent
    call (no stale-nonzero masking once the real RPC has seen 0).

    The drain-detection latency is bounded by one sampling window: at most
    ``depth_sample_every`` calls after the backend goes to 0, the next real
    probe fires, and from that point on every call re-probes (zero is never
    served from a stale cache).
    """
    backend = mock_connection_manager.get_queue_backend()
    backend.pop.return_value = None
    # Prime a non-zero cache (active-crawl steady state).
    backend.queue_len.return_value = 42
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      depth_sample_every=5,
    )
    assert len(queue) == 42

    # Drain: within one window the real probe fires and returns 0.
    backend.queue_len.return_value = 0
    calls_before_zero = 0
    for _ in range(queue.depth_sample_every + 1):
      calls_before_zero += 1
      if len(queue) == 0:
        break
    assert calls_before_zero <= queue.depth_sample_every, (
      "drain took longer than one sampling window to surface"
    )

    # From the confirmed-0 probe onward, every call returns 0 and re-probes
    # (no stale-nonzero cache mask).
    assert queue._cached_depth == 0
    probe_count_at_confirm = backend.queue_len.call_count
    for _ in range(8):
      assert len(queue) == 0
    assert backend.queue_len.call_count == probe_count_at_confirm + 8

  def test_pop_with_empty_backend_never_masks_emptiness(
    self, mock_connection_manager, mock_spider
  ):
    """U4 pop-path: popping an empty backend with a non-zero cache still
    surfaces depth 0 to the monitor once the next real probe fires.

    Guards the pop() depth-emit path (not just __len__) so the backpressure
    monitor sees the drain the moment it happens.
    """
    backend = mock_connection_manager.get_queue_backend()
    backend.pop.return_value = None
    backend.queue_len.return_value = 100
    from scrapy_extension.monitor import NullMonitor

    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      monitor=NullMonitor(),
      depth_sample_every=3,
    )
    # Prime the non-zero cache.
    queue.pop()
    assert queue._cached_depth == 100

    # Drain: subsequent real probes return 0; pop must update the cache.
    backend.queue_len.return_value = 0
    for _ in range(queue.depth_sample_every):
      queue.pop()
    assert queue._cached_depth == 0

  def test_len_uses_sampled_depth(self, mock_connection_manager, mock_spider):
    """U4: __len__ also benefits from sampling — repeated len() probes cache.

    The cache is shared between the pop-path depth emit and __len__ so both
    hot paths amortize the same RPC.
    """
    backend = mock_connection_manager.get_queue_backend()
    backend.queue_len.return_value = 7
    queue = BackendQueue(
      connection_manager=mock_connection_manager,
      queue_name="test_queue",
      spider=mock_spider,
      depth_sample_every=5,
    )

    results = [len(queue) for _ in range(20)]

    # Every probe returns the correct depth (cache is consistent).
    assert all(r == 7 for r in results)
    # But the backend RPC only fired at most ceil(20/5) = 4 times.
    assert backend.queue_len.call_count <= 4
