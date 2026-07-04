"""Tests for RocketMQ backend implementation (apache ``rocketmq-python-client`` 5.1.1 gRPC).

Rewritten (#44) alongside the backend rewrite. The apache client is real-installed
(``rocketmq-python-client`` 5.1.1), so the prior ``builtins.__import__``
interception strategy is obsolete — these tests patch the top-level
``rocketmq.Producer`` / ``rocketmq.SimpleConsumer`` / etc. directly.
"""

from typing import cast
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from scrapy_extension.backends.base import Backend, BackendType, QueueBackend
from scrapy_extension.backends.connectors import (
  SET_CAPABLE_BACKENDS,
  STORAGE_CAPABLE_BACKENDS,
  resolve_backend_config,
)
from scrapy_extension.backends.rocketmq import RocketMQBackend
from scrapy_extension.exceptions import (
  BackendConnectionError,
  ConfigurationError,
  QueueError,
)
from scrapy_extension.settings import RocketMQMode, RocketMQSettings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_rocketmq(mocker):
  """Patch the apache 5.1.1 top-level rocketmq client surface.

  Returns ``(mock_producer_cls, mock_consumer_cls, mock_message_cls,
  mock_config_cls, mock_credentials_cls)`` so tests can assert on construction
  args. Instances: ``mock_producer_cls.return_value`` / ``mock_consumer_cls.return_value``.
  """
  mock_producer = mocker.MagicMock()
  mock_consumer = mocker.MagicMock()
  mock_producer_cls = mocker.patch("rocketmq.Producer", return_value=mock_producer)
  mock_consumer_cls = mocker.patch(
    "rocketmq.SimpleConsumer", return_value=mock_consumer
  )
  mock_message_cls = mocker.patch("rocketmq.Message")
  mock_config_cls = mocker.patch("rocketmq.ClientConfiguration")
  mock_credentials_cls = mocker.patch("rocketmq.Credentials")
  return (
    mock_producer_cls,
    mock_consumer_cls,
    mock_message_cls,
    mock_config_cls,
    mock_credentials_cls,
  )


def _make_connected_backend(mocker, *, access_key=None, secret_key=None, **kw):
  """Create a backend with patched rocketmq client and pre-connected state.

  Returns ``(backend, mock_producer, mock_consumer, mock_message_cls)``.
  """
  config = RocketMQSettings(access_key=access_key, secret_key=secret_key, **kw)
  backend = RocketMQBackend(config)
  (
    mock_producer_cls,
    mock_consumer_cls,
    mock_message_cls,
    _,
    _,
  ) = _patch_rocketmq(mocker)
  backend.connect()
  return (
      backend,
      mock_producer_cls.return_value,
      mock_consumer_cls.return_value,
      mock_message_cls,
  )


# ---------------------------------------------------------------------------
# Instantiation & interface
# ---------------------------------------------------------------------------


def test_rocketmq_backend_instantiation() -> None:
  """Test RocketMQBackend can be instantiated."""
  config = RocketMQSettings()
  backend = RocketMQBackend(config)
  assert backend.config is config
  assert backend.backend_type == BackendType.ROCKETMQ


def test_rocketmq_backend_is_connected_false_before_connect() -> None:
  """Test is_connected returns False before connect."""
  config = RocketMQSettings()
  backend = RocketMQBackend(config)
  assert backend.is_connected() is False


def test_rocketmq_backend_ping_false_before_connect() -> None:
  """Test ping returns False before connect."""
  config = RocketMQSettings()
  backend = RocketMQBackend(config)
  assert backend.ping() is False


def test_rocketmq_backend_implements_backend() -> None:
  """Test RocketMQBackend implements Backend."""
  config = RocketMQSettings()
  backend = RocketMQBackend(config)
  assert isinstance(backend, Backend)


def test_rocketmq_backend_implements_queuebackend() -> None:
  """Test RocketMQBackend implements QueueBackend."""
  config = RocketMQSettings()
  backend = RocketMQBackend(config)
  assert isinstance(backend, QueueBackend)


def test_rocketmq_backend_disconnect() -> None:
  """Test disconnect cleans up connections (no-op when never connected)."""
  config = RocketMQSettings()
  backend = RocketMQBackend(config)
  backend.disconnect()  # should not raise
  assert backend._producer is None
  assert backend._consumer is None


# ---------------------------------------------------------------------------
# connect — success paths
# ---------------------------------------------------------------------------


def test_connect_standalone_mode(mocker) -> None:
  """Standalone connect: ClientConfiguration built with the proxy endpoints."""
  config = RocketMQSettings(mode=RocketMQMode.STANDALONE)
  backend = RocketMQBackend(config)
  (
    mock_producer_cls,
    mock_consumer_cls,
    _,
    mock_config_cls,
    _,
  ) = _patch_rocketmq(mocker)

  backend.connect()

  assert backend._producer is mock_producer_cls.return_value
  assert backend._consumer is mock_consumer_cls.return_value
  mock_producer_cls.return_value.startup.assert_called_once()
  mock_consumer_cls.return_value.startup.assert_called_once()
  # ClientConfiguration receives endpoints=namesrv_address (now the gRPC proxy).
  assert mock_config_cls.call_args.kwargs["endpoints"] == config.namesrv_address


def test_connect_cluster_mode(mocker) -> None:
  """Cluster mode passes the configured proxy endpoints through."""
  config = RocketMQSettings(
    mode=RocketMQMode.CLUSTER, namesrv_address="rocketmq-cluster:8081"
  )
  backend = RocketMQBackend(config)
  (_, _, _, mock_config_cls, _) = _patch_rocketmq(mocker)

  backend.connect()

  assert mock_config_cls.call_args.kwargs["endpoints"] == "rocketmq-cluster:8081"


def test_connect_cloud_mode_with_credentials(mocker) -> None:
  """Cloud mode with access/secret builds Credentials(ak, sk)."""
  (_, _, _, _, mock_credentials_cls) = _patch_rocketmq(mocker)
  config = RocketMQSettings(
    mode=RocketMQMode.CLOUD,
    access_key=SecretStr("my-access-key"),
    secret_key=SecretStr("my-secret-key"),
  )
  backend = RocketMQBackend(config)

  backend.connect()

  mock_credentials_cls.assert_called_once_with("my-access-key", "my-secret-key")


def test_connect_standalone_without_credentials(mocker) -> None:
  """Standalone mode without keys builds an empty Credentials()."""
  (_, _, _, _, mock_credentials_cls) = _patch_rocketmq(mocker)
  config = RocketMQSettings()  # defaults: no keys
  backend = RocketMQBackend(config)

  backend.connect()

  # Credentials() is always constructed (empty for no-auth); it's the apache
  # no-auth pattern. Assert it was called with no positional args.
  mock_credentials_cls.assert_called_once_with()


# ---------------------------------------------------------------------------
# connect — failure paths
# ---------------------------------------------------------------------------


def test_connect_missing_package(mocker) -> None:
  """connect raises BackendConnectionError when the rocketmq import fails."""
  config = RocketMQSettings()
  backend = RocketMQBackend(config)
  mocker.patch("builtins.__import__", side_effect=ImportError("no rocketmq"))

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  assert "rocketmq-python-client not installed" in str(exc_info.value)


def test_connect_unsupported_mode(mocker) -> None:
  """connect raises ConfigurationError for unsupported mode."""
  _patch_rocketmq(mocker)
  config = RocketMQSettings()
  # Bypass the typed field (mode is RocketMQMode) to inject an invalid value
  # at runtime; cast keeps the static checkers happy without altering behavior.
  config.mode = cast("RocketMQMode", "invalid_mode")
  backend = RocketMQBackend(config)

  with pytest.raises(ConfigurationError) as exc_info:
    backend.connect()
  assert "Unsupported RocketMQ mode" in str(exc_info.value)
  assert exc_info.value.setting_name == "mode"


def test_connect_unsupported_mode_str_raises(mocker) -> None:
  """connect handles a mode whose str() raises TypeError (defensive repr)."""
  _patch_rocketmq(mocker)
  config = RocketMQSettings()

  class BadMode:
    def __str__(self) -> None:
      raise TypeError("bad")

  config.mode = cast("RocketMQMode", BadMode())
  backend = RocketMQBackend(config)

  with pytest.raises(ConfigurationError) as exc_info:
    backend.connect()
  assert "Unsupported RocketMQ mode" in str(exc_info.value)


def test_connect_producer_startup_failure(mocker) -> None:
  """connect wraps a producer startup failure in BackendConnectionError."""
  (
    mock_producer_cls,
    _,
    _,
    _,
    _,
  ) = _patch_rocketmq(mocker)
  mock_producer_cls.return_value.startup.side_effect = OSError("Connection refused")
  backend = RocketMQBackend(RocketMQSettings())

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  assert "Failed to connect to RocketMQ" in str(exc_info.value)
  assert exc_info.value.backend_type == "rocketmq"


def test_connect_unexpected_exception(mocker) -> None:
  """connect wraps any unexpected startup error in BackendConnectionError."""
  (
    mock_producer_cls,
    _,
    _,
    _,
    _,
  ) = _patch_rocketmq(mocker)
  mock_producer_cls.return_value.startup.side_effect = RuntimeError("unexpected")
  backend = RocketMQBackend(RocketMQSettings())

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  assert "Failed to connect to RocketMQ" in str(exc_info.value)


# ---------------------------------------------------------------------------
# disconnect — connected state
# ---------------------------------------------------------------------------


def test_disconnect_connected(mocker) -> None:
  """disconnect shuts down producer and consumer."""
  backend, mock_producer, mock_consumer, _ = _make_connected_backend(mocker)

  backend.disconnect()

  mock_producer.shutdown.assert_called_once()
  mock_consumer.shutdown.assert_called_once()
  assert backend._producer is None
  assert backend._consumer is None


def test_disconnect_best_effort_on_shutdown_failure(mocker) -> None:
  """disconnect swallows a per-client shutdown failure so the other still runs."""
  backend, mock_producer, mock_consumer, _ = _make_connected_backend(mocker)
  mock_producer.shutdown.side_effect = RuntimeError("boom")

  backend.disconnect()  # must not raise

  mock_producer.shutdown.assert_called_once()
  mock_consumer.shutdown.assert_called_once()  # still attempted
  assert backend._producer is None


# ---------------------------------------------------------------------------
# is_connected / ping — connected state
# ---------------------------------------------------------------------------


def test_is_connected_true(mocker) -> None:
  """is_connected returns True after successful connect."""
  backend, _, _, _ = _make_connected_backend(mocker)
  assert backend.is_connected() is True


def test_ping_true(mocker) -> None:
  """ping returns True when connected."""
  backend, _, _, _ = _make_connected_backend(mocker)
  assert backend.ping() is True


# ---------------------------------------------------------------------------
# _get_topic_name
# ---------------------------------------------------------------------------


def test_get_topic_name(mocker) -> None:
  """_get_topic_name returns prefixed queue name."""
  backend, _, _, _ = _make_connected_backend(mocker)
  result = backend._get_topic_name("my_queue")
  assert result == f"{backend.config.topic_prefix}_my_queue"


# ---------------------------------------------------------------------------
# push
# ---------------------------------------------------------------------------


def test_push_not_connected() -> None:
  """push raises QueueError when not connected."""
  backend = RocketMQBackend(RocketMQSettings())
  with pytest.raises(QueueError) as exc_info:
    backend.push("test_queue", b"item")
  assert "Not connected" in str(exc_info.value)


def test_push_success(mocker) -> None:
  """push builds a Message (topic/body/keys) and sends it via the producer."""
  backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)
  mock_msg = mocker.MagicMock()
  mock_message_cls.return_value = mock_msg

  backend.push("my_queue", b"test-data", priority=5.0)

  # apache Message: instantiate, then set .topic/.body/.keys attributes.
  mock_message_cls.assert_called_once_with()
  assert mock_msg.topic == f"{backend.config.topic_prefix}_my_queue"
  assert mock_msg.body == b"test-data"
  assert mock_msg.keys == "5.0"
  mock_producer.send.assert_called_once_with(mock_msg)


def test_push_default_priority(mocker) -> None:
  """push with default priority carries "0.0" as the message keys."""
  backend, _, _, mock_message_cls = _make_connected_backend(mocker)
  mock_msg = mocker.MagicMock()
  mock_message_cls.return_value = mock_msg

  backend.push("my_queue", b"data")

  assert mock_msg.keys == "0.0"


def test_push_send_failure(mocker) -> None:
  """push wraps a producer send failure in QueueError."""
  backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)
  mock_message_cls.return_value = mocker.MagicMock()
  mock_producer.send.side_effect = OSError("Network error")

  with pytest.raises(QueueError) as exc_info:
    backend.push("my_queue", b"data")
  assert "Failed to push to queue" in str(exc_info.value)


def test_push_unexpected_error(mocker) -> None:
  """push wraps any unexpected send error in QueueError."""
  backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)
  mock_message_cls.return_value = mocker.MagicMock()
  mock_producer.send.side_effect = RuntimeError("boom")

  with pytest.raises(QueueError) as exc_info:
    backend.push("my_queue", b"data")
  assert "Failed to push to queue" in str(exc_info.value)


# ---------------------------------------------------------------------------
# pop
# ---------------------------------------------------------------------------


def test_pop_not_connected() -> None:
  """pop raises QueueError when not connected."""
  backend = RocketMQBackend(RocketMQSettings())
  with pytest.raises(QueueError) as exc_info:
    backend.pop("test_queue")
  assert "Not connected" in str(exc_info.value)


def test_pop_returns_message(mocker) -> None:
  """pop returns message body when available (no inline ack — initiative #4)."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_msg = mocker.MagicMock()
  mock_msg.body = b"hello-world"
  mock_consumer.receive.return_value = [mock_msg]

  result = backend.pop("my_queue")

  assert result == b"hello-world"
  # apache receive(max_message_num, invisible_duration). Default invisible 15s.
  mock_consumer.receive.assert_called_once_with(1, 15)
  mock_consumer.ack.assert_not_called()  # deferred-ack: no inline ack


def test_pop_returns_none_when_empty(mocker) -> None:
  """pop returns None when no messages available."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.return_value = []

  assert backend.pop("my_queue") is None
  mock_consumer.ack.assert_not_called()


def test_pop_timeout_used_as_invisible_duration(mocker) -> None:
  """pop passes the timeout (seconds) as the invisible_duration arg."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.return_value = []

  backend.pop("my_queue", timeout=5.0)

  mock_consumer.receive.assert_called_once_with(1, 5)


def test_pop_zero_timeout_uses_default_invisible(mocker) -> None:
  """pop with timeout=0 uses the default 15s invisible-duration."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.return_value = []

  backend.pop("my_queue", timeout=0.0)

  mock_consumer.receive.assert_called_once_with(1, 15)


def test_pop_receive_failure(mocker) -> None:
  """pop wraps a receive failure in QueueError."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.side_effect = OSError("Network error")

  with pytest.raises(QueueError) as exc_info:
    backend.pop("my_queue")
  assert "Failed to pop from queue" in str(exc_info.value)


def test_pop_unexpected_error(mocker) -> None:
  """pop wraps any unexpected receive error in QueueError."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.side_effect = RuntimeError("unexpected")

  with pytest.raises(QueueError) as exc_info:
    backend.pop("my_queue")
  assert "Failed to pop from queue" in str(exc_info.value)


def test_pop_subscribes_to_topic_before_receive(mocker) -> None:
  """pop subscribes the consumer to the queue's topic before receiving."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_msg = mocker.MagicMock()
  mock_msg.body = b"data"
  mock_consumer.receive.return_value = [mock_msg]

  backend.pop("my_queue")

  mock_consumer.subscribe.assert_called_once_with("scrapy-queue_my_queue")


def test_pop_subscribes_only_once_per_topic(mocker) -> None:
  """Repeated pop calls for the same queue subscribe exactly once."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.return_value = []

  backend.pop("my_queue")
  backend.pop("my_queue")
  backend.pop("my_queue")

  assert mock_consumer.subscribe.call_count == 1


def test_pop_subscribes_distinct_topics_for_distinct_queues(mocker) -> None:
  """Different queue names subscribe to different topics."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.return_value = []

  backend.pop("queue_a")
  backend.pop("queue_b")

  subscribed = {call.args[0] for call in mock_consumer.subscribe.call_args_list}
  assert subscribed == {"scrapy-queue_queue_a", "scrapy-queue_queue_b"}


def test_connect_starts_consumer(mocker) -> None:
  """connect() must call consumer.startup() — without it receive() fails."""
  _, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.startup.assert_called_once()


def test_disconnect_clears_subscribed_topics(mocker) -> None:
  """disconnect() clears the subscription cache so reconnect re-subscribes."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.return_value = []
  backend.pop("my_queue")
  assert "scrapy-queue_my_queue" in backend._subscribed_topics

  backend.disconnect()
  assert len(backend._subscribed_topics) == 0


# ---------------------------------------------------------------------------
# pop/ack decouple (initiative #4 — at-least-once fix)
# ---------------------------------------------------------------------------


def test_pop_no_longer_inline_acks(mocker) -> None:
  """R2/regression: pop returns the body and does NOT ack inline.

  Pre-fix pop() acked inline, so an ack failure (broker down mid-ack) raised
  QueueError and the caller never got the data (at-most-once). Post-fix pop
  never acks — the body always reaches the caller; ack is the caller's
  explicit later responsibility.
  """
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_msg = mocker.MagicMock()
  mock_msg.body = b"payload"
  mock_consumer.receive.return_value = [mock_msg]
  mock_consumer.ack.side_effect = OSError("broker down")  # sabotage the OLD path

  result = backend.pop("my_queue")

  assert result == b"payload"  # body delivered
  mock_consumer.ack.assert_not_called()  # no inline ack attempted
  assert backend._last_msg is mock_msg  # tracked for legacy ack(token=None)


def test_pop_with_ack_returns_body_and_token_no_ack(mocker) -> None:
  """pop_with_ack returns (body, msg_token) and does NOT ack."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_msg = mocker.MagicMock()
  mock_msg.body = b"hello"
  mock_consumer.receive.return_value = [mock_msg]

  body, token = backend.pop_with_ack("my_queue")

  assert body == b"hello"
  assert token is mock_msg
  mock_consumer.ack.assert_not_called()


def test_pop_with_ack_empty_returns_none_none(mocker) -> None:
  """pop_with_ack on an empty queue returns (None, None)."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.receive.return_value = []

  body, token = backend.pop_with_ack("my_queue")

  assert body is None
  assert token is None


def test_ack_with_token_acks_specific_message(mocker) -> None:
  """ack(token=msg) acks the specific message (concurrent-ack correct)."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  msg_b = mocker.MagicMock(name="b")

  backend.ack("q", token=msg_b)

  mock_consumer.ack.assert_called_once_with(msg_b)


def test_ack_token_none_acks_last_msg_then_clears(mocker) -> None:
  """Legacy ack(token=None) acks the tracked _last_msg, then clears it."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_msg = mocker.MagicMock()
  mock_msg.body = b"x"
  mock_consumer.receive.return_value = [mock_msg]
  backend.pop("my_queue")
  assert backend._last_msg is mock_msg

  backend.ack("my_queue", token=None)

  mock_consumer.ack.assert_called_once_with(mock_msg)
  assert backend._last_msg is None


def test_ack_token_none_with_no_last_msg_is_noop(mocker) -> None:
  """ack(token=None) with no tracked message is a safe no-op."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)

  backend.ack("my_queue", token=None)

  mock_consumer.ack.assert_not_called()


def test_nack_does_not_ack(mocker) -> None:
  """nack is a no-op — RocketMQ redelivers via the invisible-duration window."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  msg = mocker.MagicMock()

  backend.nack("my_queue", token=msg)

  mock_consumer.ack.assert_not_called()


def test_rocketmq_requires_ack_class_attrs() -> None:
  """R6: RocketMQ declares the deferred-ack capability contract."""
  assert RocketMQBackend.requires_ack is True
  assert RocketMQBackend.supports_concurrent_ack is True


# ---------------------------------------------------------------------------
# _extract_body — defensive coercion of apache Message.body (any type → bytes)
# ---------------------------------------------------------------------------


def test_extract_body_none_returns_empty() -> None:
  """_extract_body returns b"" when the message has no body attr / body is None."""
  msg = MagicMock()
  del msg.body  # getattr(msg, "body", None) → None
  assert RocketMQBackend._extract_body(msg) == b""


def test_extract_body_bytearray() -> None:
  """_extract_body coerces bytearray to bytes."""
  msg = MagicMock()
  msg.body = bytearray(b"x")
  assert RocketMQBackend._extract_body(msg) == b"x"
  assert isinstance(RocketMQBackend._extract_body(msg), bytes)


def test_extract_body_memoryview() -> None:
  """_extract_body coerces memoryview to bytes."""
  msg = MagicMock()
  msg.body = memoryview(b"y")
  assert RocketMQBackend._extract_body(msg) == b"y"


def test_extract_body_str_encodes() -> None:
  """_extract_body utf-8-encodes a str body."""
  msg = MagicMock()
  msg.body = "héllo"
  assert RocketMQBackend._extract_body(msg) == "héllo".encode()


# ---------------------------------------------------------------------------
# is_connected / _ensure_subscribed — defensive exception branches
# ---------------------------------------------------------------------------


def test_is_connected_false_when_is_running_raises(mocker) -> None:
  """is_connected swallows a client is_running access failure and returns
  False (defensive — is_connected must never raise).

  ``is_running`` is a bool PROPERTY on the apache client; we swap in a tiny
  stand-in whose property raises (MagicMock's auto-child protocol defeats
  PropertyMock, so a real object is the clean way to exercise the branch).
  """

  class _RaisingProducer:
    @property
    def is_running(self) -> bool:
      raise RuntimeError("client closed")

    def shutdown(self) -> None:
      pass

  backend, _, _, _ = _make_connected_backend(mocker)
  backend._producer = _RaisingProducer()

  assert backend.is_connected() is False


def test_ensure_subscribed_swallows_subscribe_failure(mocker) -> None:
  """_ensure_subscribed swallows a subscribe() failure (best-effort); the
  subsequent receive() surfaces any real broker error."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  mock_consumer.subscribe.side_effect = RuntimeError("transient")
  mock_consumer.receive.return_value = []

  # pop must NOT raise despite the subscribe failure — _ensure_subscribed
  # caught it, and receive returned [].
  assert backend.pop("flaky_queue") is None


def test_ack_failure_raises_queue_error(mocker) -> None:
  """ack wraps a broker-side ack failure in QueueError(operation='ack')."""
  backend, _, mock_consumer, _ = _make_connected_backend(mocker)
  msg = mocker.MagicMock()
  mock_consumer.ack.side_effect = OSError("broker gone")

  with pytest.raises(QueueError, match="Failed to ack RocketMQ message") as exc_info:
    backend.ack("q", token=msg)
  assert exc_info.value.operation == "ack"


# ---------------------------------------------------------------------------
# queue_len
# ---------------------------------------------------------------------------


def test_queue_len_not_connected() -> None:
  """queue_len raises NotImplementedError when not connected."""
  backend = RocketMQBackend(RocketMQSettings())
  with pytest.raises(NotImplementedError):
    backend.queue_len("test_queue")


def test_queue_len_message() -> None:
  """queue_len error message."""
  backend = RocketMQBackend(RocketMQSettings())
  with pytest.raises(NotImplementedError) as exc_info:
    backend.queue_len("test_queue")
  assert "does not support queue_len" in str(exc_info.value)


# ---------------------------------------------------------------------------
# clear_queue
# ---------------------------------------------------------------------------


def test_clear_queue_not_connected() -> None:
  """clear_queue raises QueueError when not connected."""
  backend = RocketMQBackend(RocketMQSettings())
  with pytest.raises(QueueError) as exc_info:
    backend.clear_queue("test_queue")
  assert "Not connected" in str(exc_info.value)


def test_clear_queue_connected(mocker) -> None:
  """clear_queue logs a warning when connected (no-op)."""
  backend, _, _, _ = _make_connected_backend(mocker)
  backend.clear_queue("test_queue")  # should not raise


# ---------------------------------------------------------------------------
# Set / Storage — RocketMQBackend (queue) does NOT carry set/storage methods
# ---------------------------------------------------------------------------


def test_rocketmq_backend_has_no_set_methods() -> None:
  """E3: RocketMQBackend (queue) no longer carries SetBackend methods."""
  backend = RocketMQBackend(RocketMQSettings())
  for attr in ("add", "remove", "contains", "set_len", "clear_set"):
    assert not hasattr(backend, attr), (
      f"RocketMQBackend should not expose set method {attr!r} "
      f"(moved to guard class RocketMQSetBackend)"
    )


def test_rocketmq_backend_has_no_storage_methods() -> None:
  """E3: RocketMQBackend (queue) no longer carries StorageBackend methods."""
  backend = RocketMQBackend(RocketMQSettings())
  for attr in (
    "store",
    "retrieve",
    "delete",
    "exists",
    "ttl",
    "clear_storage",
  ):
    assert not hasattr(backend, attr), (
      f"RocketMQBackend should not expose storage method {attr!r} "
      f"(moved to guard class RocketMQStorageBackend)"
    )


# ---------------------------------------------------------------------------
# Settings tests
# ---------------------------------------------------------------------------


def test_rocketmq_settings_defaults() -> None:
  """Test RocketMQSettings default values."""
  settings = RocketMQSettings()
  assert settings.mode == RocketMQMode.STANDALONE
  assert settings.namesrv_address == "localhost:9876"
  assert settings.consumer_group == "scrapy-extension-consumer"
  assert settings.producer_group == "scrapy-extension-producer"
  assert settings.topic_prefix == "scrapy-queue"
  assert settings.set_topic_prefix == "scrapy-set"
  assert settings.storage_topic_prefix == "scrapy-storage"
  assert settings.max_message_size == 1024 * 1024
  assert settings.send_timeout == 3000


def test_rocketmq_settings_custom_values() -> None:
  """Test RocketMQSettings with custom values."""
  settings = RocketMQSettings(
    mode=RocketMQMode.CLUSTER,
    namesrv_address="rocketmq-cluster:9876",
    access_key=SecretStr("mykey"),
    secret_key=SecretStr("mysecret"),
    consumer_group="my-consumer",
    producer_group="my-producer",
    topic_prefix="my-queue",
  )
  assert settings.mode == RocketMQMode.CLUSTER
  assert settings.namesrv_address == "rocketmq-cluster:9876"
  assert settings.access_key is not None
  assert settings.access_key.get_secret_value() == "mykey"
  assert settings.secret_key is not None
  assert settings.secret_key.get_secret_value() == "mysecret"


def test_rocketmq_mode_enum_values() -> None:
  """Test RocketMQMode enum values."""
  assert RocketMQMode.STANDALONE.value == "standalone"
  assert RocketMQMode.CLUSTER.value == "cluster"
  assert RocketMQMode.CLOUD.value == "cloud"


def test_rocketmq_settings_env_prefix(monkeypatch) -> None:
  """Test RocketMQSettings respects env prefix."""
  monkeypatch.setenv("SCRAPY_ROCKETMQ_NAMESRV_ADDRESS", "env-rocketmq:9876")
  settings = RocketMQSettings()
  assert settings.namesrv_address == "env-rocketmq:9876"


def test_rocketmq_settings_cloud_mode() -> None:
  """Test RocketMQSettings cloud mode defaults."""
  settings = RocketMQSettings(mode=RocketMQMode.CLOUD)
  assert settings.mode == RocketMQMode.CLOUD


def test_rocketmq_settings_none_keys() -> None:
  """Test RocketMQSettings with explicit None keys."""
  settings = RocketMQSettings(access_key=None, secret_key=None)
  assert settings.access_key is None
  assert settings.secret_key is None


# ---------------------------------------------------------------------------
# Config-time capability guard — resolve_backend_config rejects RocketMQ
# ---------------------------------------------------------------------------


class _FakeSettings:
  """Minimal Scrapy-Settings-like object for resolve_backend_config tests."""

  def __init__(self, type_value: str) -> None:
    self._type_value = type_value

  def get(self, key, default=None):
    if key == "SCRAPY_BACKEND_TYPE":
      return self._type_value
    return default

  def getdict(self, key, default=None):  # noqa: ARG002 - Scrapy Settings.getdict signature
    del key
    if default is None:
      return {}
    return default


def test_resolve_backend_config_rejects_rocketmq_for_set() -> None:
  """Layer-2 guard: configuring RocketMQ for the set component fails at config time."""
  assert BackendType.ROCKETMQ not in SET_CAPABLE_BACKENDS

  settings = _FakeSettings(type_value="rocketmq")
  with pytest.raises(ConfigurationError) as exc_info:
    resolve_backend_config(
      settings,
      type_key="SCRAPY_SET_BACKEND_TYPE",
      settings_key="SCRAPY_SET_BACKEND_SETTINGS",
      required_capabilities={"set"},
      component_name="set",
    )
  msg = str(exc_info.value)
  assert "rocketmq" in msg
  assert "set" in msg
  assert "redis" in msg


def test_resolve_backend_config_rejects_rocketmq_for_storage() -> None:
  """Layer-2 guard: configuring RocketMQ for the storage component fails at config time."""
  assert BackendType.ROCKETMQ not in STORAGE_CAPABLE_BACKENDS

  settings = _FakeSettings(type_value="rocketmq")
  with pytest.raises(ConfigurationError) as exc_info:
    resolve_backend_config(
      settings,
      type_key="SCRAPY_STORAGE_BACKEND_TYPE",
      settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
      required_capabilities={"storage"},
      component_name="storage",
    )
  msg = str(exc_info.value)
  assert "rocketmq" in msg
  assert "storage" in msg


def test_resolve_backend_config_accepts_rocketmq_for_queue() -> None:
  """Sanity: RocketMQ IS queue-capable, so the queue config path succeeds."""
  from scrapy_extension.backends.connectors import QUEUE_CAPABLE_BACKENDS

  assert BackendType.ROCKETMQ in QUEUE_CAPABLE_BACKENDS

  settings = _FakeSettings(type_value="rocketmq")
  backend_type, backend_settings = resolve_backend_config(
    settings,
    type_key="SCRAPY_QUEUE_BACKEND_TYPE",
    settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
    required_capabilities={"queue"},
    component_name="queue",
  )
  assert backend_type == "rocketmq"
  assert backend_settings == {}


# ---------------------------------------------------------------------------
# E3: class-level guard — RocketMQSetBackend / RocketMQStorageBackend
# ---------------------------------------------------------------------------


def test_rocketmq_set_backend_construction_raises_configuration_error() -> None:
  """E3: instantiating RocketMQSetBackend fails fast with a typed error."""
  from scrapy_extension.backends.rocketmq import RocketMQSetBackend

  with pytest.raises(ConfigurationError) as exc_info:
    RocketMQSetBackend(RocketMQSettings())

  msg = str(exc_info.value)
  assert "RocketMQ" in msg
  assert "set" in msg.lower()
  assert "SCRAPY_SET_BACKEND_TYPE" in msg


def test_rocketmq_storage_backend_construction_raises_configuration_error() -> None:
  """E3: instantiating RocketMQStorageBackend fails fast with a typed error."""
  from scrapy_extension.backends.rocketmq import RocketMQStorageBackend

  with pytest.raises(ConfigurationError) as exc_info:
    RocketMQStorageBackend(RocketMQSettings())

  msg = str(exc_info.value)
  assert "RocketMQ" in msg
  assert "storage" in msg.lower()
  assert "SCRAPY_STORAGE_BACKEND_TYPE" in msg


def test_rocketmq_set_backend_class_is_importable() -> None:
  """E3: the guard classes remain importable (lazy-import architecture preserved)."""
  from scrapy_extension.backends.rocketmq import (
    RocketMQBackend,
    RocketMQSetBackend,
    RocketMQStorageBackend,
  )

  assert RocketMQBackend is not None
  assert RocketMQSetBackend is not None
  assert RocketMQStorageBackend is not None
