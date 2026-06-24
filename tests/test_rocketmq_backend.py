"""Tests for RocketMQ backend implementation."""

from unittest.mock import MagicMock

import pytest

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


def _make_connected_backend(mocker, *, access_key=None, secret_key=None):
    """Create a backend with mocked rocketmq imports and pre-connected state."""
    config = RocketMQSettings(
        access_key=access_key,
        secret_key=secret_key,
    )
    backend = RocketMQBackend(config)

    # Mock the rocketmq sub-module imports so connect() succeeds
    mock_producer_cls = mocker.MagicMock()
    mock_consumer_cls = mocker.MagicMock()
    mock_message_cls = mocker.MagicMock()
    mock_endpoint_cls = mocker.MagicMock()
    mock_credentials_cls = mocker.MagicMock()

    # Make producer.start() and consumer.shutdown() no-ops
    mock_producer = mocker.MagicMock()
    mock_consumer = mocker.MagicMock()
    mock_producer_cls.return_value = mock_producer
    mock_consumer_cls.return_value = mock_consumer

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mock_credentials_cls},
        "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mock_consumer_cls},
        "rocketmq.endpoint": {"Endpoint": mock_endpoint_cls},
        "rocketmq.message": {"Message": mock_message_cls},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    backend.connect()
    return backend, mock_producer, mock_consumer, mock_message_cls


# ---------------------------------------------------------------------------
# Instantiation & interface
# ---------------------------------------------------------------------------


def test_rocketmq_backend_instantiation():
    """Test RocketMQBackend can be instantiated."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.config is config
    assert backend.backend_type == BackendType.ROCKETMQ


def test_rocketmq_backend_is_connected_false_before_connect():
    """Test is_connected returns False before connect."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.is_connected() is False


def test_rocketmq_backend_ping_false_before_connect():
    """Test ping returns False before connect."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.ping() is False


def test_rocketmq_backend_connect_missing_package(mocker):
    """Test connect raises BackendConnectionError when rocketmq not installed."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    mocker.patch("builtins.__import__", side_effect=ImportError("No module named rocketmq"))

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.connect()
    assert "rocketmq-client-python not installed" in str(exc_info.value)


def test_rocketmq_backend_disconnect():
    """Test disconnect cleans up connections."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    # Should not raise even if not connected
    backend.disconnect()
    assert backend._producer is None
    assert backend._consumer is None


def test_rocketmq_backend_implements_backend():
    """Test RocketMQBackend implements Backend."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert isinstance(backend, Backend)


def test_rocketmq_backend_implements_queuebackend():
    """Test RocketMQBackend implements QueueBackend."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert isinstance(backend, QueueBackend)


# ---------------------------------------------------------------------------
# connect — success paths
# ---------------------------------------------------------------------------


def test_connect_standalone_mode(mocker):
    """Test successful connection in standalone mode."""
    config = RocketMQSettings(mode=RocketMQMode.STANDALONE)
    backend = RocketMQBackend(config)

    mock_producer_cls = mocker.MagicMock()
    mock_consumer_cls = mocker.MagicMock()
    mock_endpoint_cls = mocker.MagicMock()

    mock_producer = mocker.MagicMock()
    mock_consumer = mocker.MagicMock()
    mock_producer_cls.return_value = mock_producer
    mock_consumer_cls.return_value = mock_consumer

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mocker.MagicMock()},
        "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mock_consumer_cls},
        "rocketmq.endpoint": {"Endpoint": mock_endpoint_cls},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    backend.connect()

    assert backend._producer is mock_producer
    assert backend._consumer is mock_consumer
    mock_producer.start.assert_called_once()
    mock_endpoint_cls.assert_called_with(config.namesrv_address)


def test_connect_cluster_mode(mocker):
    """Test successful connection in cluster mode."""
    config = RocketMQSettings(
        mode=RocketMQMode.CLUSTER,
        namesrv_address="rocketmq-cluster:9876",
    )
    backend = RocketMQBackend(config)

    mock_producer_cls = mocker.MagicMock()
    mock_consumer_cls = mocker.MagicMock()
    mock_endpoint_cls = mocker.MagicMock()

    mock_producer = mocker.MagicMock()
    mock_consumer = mocker.MagicMock()
    mock_producer_cls.return_value = mock_producer
    mock_consumer_cls.return_value = mock_consumer

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mocker.MagicMock()},
        "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mock_consumer_cls},
        "rocketmq.endpoint": {"Endpoint": mock_endpoint_cls},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    backend.connect()

    assert backend._producer is mock_producer
    assert backend._consumer is mock_consumer
    mock_endpoint_cls.assert_called_with("rocketmq-cluster:9876")


def test_connect_cloud_mode_with_credentials(mocker):
    """Test connection in cloud mode with access_key and secret_key."""
    mock_credentials_cls = mocker.MagicMock()
    mock_producer_cls = mocker.MagicMock()
    mock_consumer_cls = mocker.MagicMock()
    mock_endpoint_cls = mocker.MagicMock()

    mock_producer = mocker.MagicMock()
    mock_consumer = mocker.MagicMock()
    mock_producer_cls.return_value = mock_producer
    mock_consumer_cls.return_value = mock_consumer

    config = RocketMQSettings(
        mode=RocketMQMode.CLOUD,
        access_key="my-access-key",
        secret_key="my-secret-key",
    )
    backend = RocketMQBackend(config)

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mock_credentials_cls},
        "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mock_consumer_cls},
        "rocketmq.endpoint": {"Endpoint": mock_endpoint_cls},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    backend.connect()

    # Credentials should have been created
    mock_credentials_cls.assert_called_once_with("my-access-key", "my-secret-key")
    # Producer should be created with credentials
    mock_producer_cls.assert_called_once()
    call_kwargs = mock_producer_cls.call_args
    assert call_kwargs[1]["credentials"] is not None
    # Consumer should use send_timeout as request_timeout_ms
    mock_consumer_cls.assert_called_once()
    consumer_kwargs = mock_consumer_cls.call_args
    assert consumer_kwargs[1]["request_timeout_ms"] == config.send_timeout


def test_connect_standalone_without_credentials(mocker):
    """Test standalone mode connection without credentials."""
    mock_credentials_cls = mocker.MagicMock()
    mock_producer_cls = mocker.MagicMock()
    mock_consumer_cls = mocker.MagicMock()
    mock_endpoint_cls = mocker.MagicMock()

    mock_producer = mocker.MagicMock()
    mock_consumer = mocker.MagicMock()
    mock_producer_cls.return_value = mock_producer
    mock_consumer_cls.return_value = mock_consumer

    config = RocketMQSettings()  # defaults: no keys
    backend = RocketMQBackend(config)

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mock_credentials_cls},
        "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mock_consumer_cls},
        "rocketmq.endpoint": {"Endpoint": mock_endpoint_cls},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    backend.connect()

    # Credentials should NOT have been created
    mock_credentials_cls.assert_not_called()


# ---------------------------------------------------------------------------
# connect — failure paths
# ---------------------------------------------------------------------------


def test_connect_unsupported_mode(mocker):
    """Test connect raises ConfigurationError for unsupported mode."""
    config = RocketMQSettings()
    # Monkey-patch an invalid mode value
    config.mode = "invalid_mode"
    backend = RocketMQBackend(config)

    mock_producer_cls = mocker.MagicMock()

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mocker.MagicMock()},
        "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mocker.MagicMock()},
        "rocketmq.endpoint": {"Endpoint": mocker.MagicMock()},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()
    assert "Unsupported RocketMQ mode" in str(exc_info.value)
    assert exc_info.value.setting_name == "mode"


def test_connect_oserror(mocker):
    """Test connect raises BackendConnectionError on OSError."""
    mock_producer_cls = mocker.MagicMock()
    mock_producer_cls.return_value.start.side_effect = OSError("Connection refused")

    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mocker.MagicMock()},
        "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mocker.MagicMock()},
        "rocketmq.endpoint": {"Endpoint": mocker.MagicMock()},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.connect()
    assert "Failed to connect to RocketMQ" in str(exc_info.value)
    assert exc_info.value.backend_type == "rocketmq"


def test_connect_unsupported_mode_str_raises(mocker):
    """Test connect handles mode where str() raises TypeError."""
    config = RocketMQSettings()
    # Set mode to an object whose __str__ raises TypeError
    class BadMode:
        def __str__(self):
            raise TypeError("bad")

    config.mode = BadMode()
    backend = RocketMQBackend(config)

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mocker.MagicMock()},
        "rocketmq.client": {"Producer": mocker.MagicMock(), "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mocker.MagicMock()},
        "rocketmq.endpoint": {"Endpoint": mocker.MagicMock()},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()
    assert "Unsupported RocketMQ mode" in str(exc_info.value)


def test_connect_unexpected_exception(mocker):
    """Test connect raises BackendConnectionError on unexpected exception."""
    mock_producer_cls = mocker.MagicMock()
    mock_producer_cls.return_value.start.side_effect = RuntimeError("unexpected")

    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    import_modules = {
        "rocketmq.auth.credentials": {"PlainCredentials": mocker.MagicMock()},
        "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
        "rocketmq.consumer": {"SimpleConsumer": mocker.MagicMock()},
        "rocketmq.endpoint": {"Endpoint": mocker.MagicMock()},
    }

    def _import_side_effect(name, *args, **kwargs):
        if name in import_modules:
            mod = MagicMock()
            for attr, val in import_modules[name].items():
                setattr(mod, attr, val)
            return mod
        return original_import(name, *args, **kwargs)

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__
    mocker.patch("builtins.__import__", side_effect=_import_side_effect)

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.connect()
    assert "Failed to connect to RocketMQ" in str(exc_info.value)


# ---------------------------------------------------------------------------
# disconnect — connected state
# ---------------------------------------------------------------------------


def test_disconnect_connected(mocker):
    """Test disconnect shuts down producer and consumer."""
    backend, mock_producer, mock_consumer, _ = _make_connected_backend(mocker)

    backend.disconnect()

    mock_producer.shutdown.assert_called_once()
    mock_consumer.shutdown.assert_called_once()
    assert backend._producer is None
    assert backend._consumer is None


# ---------------------------------------------------------------------------
# is_connected / ping — connected state
# ---------------------------------------------------------------------------


def test_is_connected_true(mocker):
    """Test is_connected returns True after successful connect."""
    backend, _, _, _ = _make_connected_backend(mocker)
    assert backend.is_connected() is True


def test_ping_true(mocker):
    """Test ping returns True when connected."""
    backend, _, _, _ = _make_connected_backend(mocker)
    assert backend.ping() is True


# ---------------------------------------------------------------------------
# _get_topic_name
# ---------------------------------------------------------------------------


def test_get_topic_name(mocker):
    """Test _get_topic_name returns prefixed queue name."""
    backend, _, _, _ = _make_connected_backend(mocker)
    result = backend._get_topic_name("my_queue")
    assert result == f"{backend.config.topic_prefix}_my_queue"


# ---------------------------------------------------------------------------
# push
# ---------------------------------------------------------------------------


def test_push_not_connected():
    """Test push raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    with pytest.raises(QueueError) as exc_info:
        backend.push("test_queue", b"item")
    assert "Not connected" in str(exc_info.value)


def test_push_success(mocker):
    """Test successful push creates message and sends via producer."""
    backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)

    mock_msg = mocker.MagicMock()
    mock_message_cls.return_value = mock_msg

    backend.push("my_queue", b"test-data", priority=5.0)

    mock_message_cls.assert_called_once_with(
        f"{backend.config.topic_prefix}_my_queue"
    )
    mock_msg.set_keys.assert_called_once_with("5.0")
    mock_msg.set_body.assert_called_once_with(b"test-data")
    mock_producer.send.assert_called_once_with(mock_msg)


def test_push_default_priority(mocker):
    """Test push with default priority 0.0."""
    backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)

    mock_msg = mocker.MagicMock()
    mock_message_cls.return_value = mock_msg

    backend.push("my_queue", b"data")

    mock_msg.set_keys.assert_called_once_with("0.0")


def test_push_oserror(mocker):
    """Test push raises QueueError on OSError."""
    backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)

    mock_msg = mocker.MagicMock()
    mock_message_cls.return_value = mock_msg
    mock_producer.send.side_effect = OSError("Network error")

    with pytest.raises(QueueError) as exc_info:
        backend.push("my_queue", b"data")
    assert "Failed to push to queue" in str(exc_info.value)


def test_push_unexpected_error(mocker):
    """Test push raises QueueError on unexpected exception."""
    backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)

    mock_msg = mocker.MagicMock()
    mock_message_cls.return_value = mock_msg
    mock_producer.send.side_effect = RuntimeError("boom")

    with pytest.raises(QueueError) as exc_info:
        backend.push("my_queue", b"data")
    assert "Failed to push to queue" in str(exc_info.value)


# ---------------------------------------------------------------------------
# pop
# ---------------------------------------------------------------------------


def test_pop_not_connected():
    """Test pop raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    with pytest.raises(QueueError) as exc_info:
        backend.pop("test_queue")
    assert "Not connected" in str(exc_info.value)


def test_pop_returns_message(mocker):
    """Test pop returns message body when available."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_msg = mocker.MagicMock()
    mock_msg.body = b"hello-world"
    mock_consumer.receive.return_value = [mock_msg]

    result = backend.pop("my_queue")

    assert result == b"hello-world"
    mock_consumer.receive.assert_called_once_with(3000)
    mock_consumer.ack.assert_called_once_with(mock_msg)


def test_pop_returns_none_when_empty(mocker):
    """Test pop returns None when no messages available."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_consumer.receive.return_value = []

    result = backend.pop("my_queue")

    assert result is None
    mock_consumer.ack.assert_not_called()


def test_pop_with_timeout(mocker):
    """Test pop passes timeout correctly to consumer.receive."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_consumer.receive.return_value = []

    backend.pop("my_queue", timeout=5.0)

    mock_consumer.receive.assert_called_once_with(5000)


def test_pop_zero_timeout(mocker):
    """Test pop with timeout=0 uses default 3000ms."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_consumer.receive.return_value = []

    backend.pop("my_queue", timeout=0.0)

    mock_consumer.receive.assert_called_once_with(3000)


def test_pop_oserror(mocker):
    """Test pop raises QueueError on OSError."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_consumer.receive.side_effect = OSError("Network error")

    with pytest.raises(QueueError) as exc_info:
        backend.pop("my_queue")
    assert "Failed to pop from queue" in str(exc_info.value)


def test_pop_unexpected_error(mocker):
    """Test pop raises QueueError on unexpected exception."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_consumer.receive.side_effect = RuntimeError("unexpected")

    with pytest.raises(QueueError) as exc_info:
        backend.pop("my_queue")
    assert "Failed to pop from queue" in str(exc_info.value)


def test_pop_subscribes_to_topic_before_receive(mocker):
    """Pop must subscribe the consumer to the queue's topic before receiving.

    Regression for R1-P1-12: pre-fix, pop computed topic_name but never used
    it. The consumer received from nothing (or whatever default subscription
    existed), so messages pushed to the topic were never delivered.
    """
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_msg = mocker.MagicMock()
    mock_msg.body = b"data"
    mock_consumer.receive.return_value = [mock_msg]

    backend.pop("my_queue")

    mock_consumer.subscribe.assert_called_once_with("scrapy-queue_my_queue")


def test_pop_subscribes_only_once_per_topic(mocker):
    """Repeated pop calls for the same queue subscribe exactly once."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_consumer.receive.return_value = []

    backend.pop("my_queue")
    backend.pop("my_queue")
    backend.pop("my_queue")

    assert mock_consumer.subscribe.call_count == 1


def test_pop_subscribes_distinct_topics_for_distinct_queues(mocker):
    """Different queue names subscribe to different topics."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_consumer.receive.return_value = []

    backend.pop("queue_a")
    backend.pop("queue_b")

    subscribed = {call.args[0] for call in mock_consumer.subscribe.call_args_list}
    assert subscribed == {"scrapy-queue_queue_a", "scrapy-queue_queue_b"}


def test_connect_starts_consumer(mocker):
    """connect() must call consumer.start() — without it receive() fails."""
    _, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.start.assert_called_once()


def test_disconnect_clears_subscribed_topics(mocker):
    """disconnect() clears the subscription cache so reconnect re-subscribes."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    mock_consumer.receive.return_value = []
    backend.pop("my_queue")
    assert "scrapy-queue_my_queue" in backend._subscribed_topics

    backend.disconnect()
    assert len(backend._subscribed_topics) == 0


# ---------------------------------------------------------------------------
# queue_len
# ---------------------------------------------------------------------------


def test_queue_len_not_connected():
    """Test queue_len raises NotImplementedError when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    with pytest.raises(NotImplementedError):
        backend.queue_len("test_queue")


def test_queue_len_message():
    """Test queue_len error message."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    with pytest.raises(NotImplementedError) as exc_info:
        backend.queue_len("test_queue")
    assert "does not support queue_len" in str(exc_info.value)


# ---------------------------------------------------------------------------
# clear_queue
# ---------------------------------------------------------------------------


def test_clear_queue_not_connected():
    """Test clear_queue raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    with pytest.raises(QueueError) as exc_info:
        backend.clear_queue("test_queue")
    assert "Not connected" in str(exc_info.value)


def test_clear_queue_connected(mocker):
    """Test clear_queue logs warning when connected (no-op)."""
    backend, _, _, _ = _make_connected_backend(mocker)

    # Should not raise, just log a warning
    backend.clear_queue("test_queue")


# ---------------------------------------------------------------------------
# Set / Storage — RocketMQBackend (queue) does NOT carry set/storage methods
# ---------------------------------------------------------------------------
#
# E3: the former per-method NotImplementedError stubs on RocketMQBackend were
# unreachable dead code (connector capability gating excludes RocketMQ from
# SET_CAPABLE_BACKENDS / STORAGE_CAPABLE_BACKENDS). They are replaced by the
# dedicated guard classes RocketMQSetBackend / RocketMQStorageBackend, which
# raise ConfigurationError at construction. These tests pin the new contract:
# the queue backend is queue-only; the set/storage surface lives on the guard
# classes (see the E3 section at the end of this file).


def test_rocketmq_backend_has_no_set_methods():
    """E3: RocketMQBackend (queue) no longer carries SetBackend methods.

    The set stubs were removed; set semantics are rejected at config time
    (SET_CAPABLE_BACKENDS) and, if gating is bypassed, by RocketMQSetBackend.
    """
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    for attr in ("add", "remove", "contains", "set_len", "clear_set"):
        assert not hasattr(backend, attr), (
            f"RocketMQBackend should not expose set method {attr!r} "
            f"(moved to guard class RocketMQSetBackend)"
        )


def test_rocketmq_backend_has_no_storage_methods():
    """E3: RocketMQBackend (queue) no longer carries StorageBackend methods."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
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


def test_rocketmq_settings_defaults():
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


def test_rocketmq_settings_custom_values():
    """Test RocketMQSettings with custom values."""
    settings = RocketMQSettings(
        mode=RocketMQMode.CLUSTER,
        namesrv_address="rocketmq-cluster:9876",
        access_key="mykey",
        secret_key="mysecret",
        consumer_group="my-consumer",
        producer_group="my-producer",
        topic_prefix="my-queue",
    )
    assert settings.mode == RocketMQMode.CLUSTER
    assert settings.namesrv_address == "rocketmq-cluster:9876"
    assert settings.access_key.get_secret_value() == "mykey"
    assert settings.secret_key.get_secret_value() == "mysecret"


def test_rocketmq_mode_enum_values():
    """Test RocketMQMode enum values."""
    assert RocketMQMode.STANDALONE.value == "standalone"
    assert RocketMQMode.CLUSTER.value == "cluster"
    assert RocketMQMode.CLOUD.value == "cloud"


def test_rocketmq_settings_env_prefix(monkeypatch):
    """Test RocketMQSettings respects env prefix."""
    monkeypatch.setenv("SCRAPY_ROCKETMQ_NAMESRV_ADDRESS", "env-rocketmq:9876")
    settings = RocketMQSettings()
    assert settings.namesrv_address == "env-rocketmq:9876"


def test_rocketmq_settings_cloud_mode():
    """Test RocketMQSettings cloud mode defaults."""
    settings = RocketMQSettings(mode=RocketMQMode.CLOUD)
    assert settings.mode == RocketMQMode.CLOUD


def test_rocketmq_settings_none_keys():
    """Test RocketMQSettings with explicit None keys."""
    settings = RocketMQSettings(access_key=None, secret_key=None)
    assert settings.access_key is None
    assert settings.secret_key is None


# ---------------------------------------------------------------------------
# Config-time capability guard — resolve_backend_config rejects RocketMQ
# ---------------------------------------------------------------------------


class _FakeSettings:
  """Minimal Scrapy-Settings-like object for resolve_backend_config tests."""

  def __init__(self, type_value: str):
    self._type_value = type_value

  def get(self, key, default=None):
    if key == "SCRAPY_BACKEND_TYPE":
      return self._type_value
    return default

  def getdict(self, key, default=None):
    if default is None:
      return {}
    return default


def test_resolve_backend_config_rejects_rocketmq_for_set():
  """Layer-2 guard: configuring RocketMQ for the set component fails at config time.

  RocketMQ is intentionally excluded from SET_CAPABLE_BACKENDS. The factory
  must raise ConfigurationError before any backend is constructed — surfacing
  the misconfiguration at startup rather than as a NotImplementedError on
  the first request_seen() call mid-crawl.
  """
  assert BackendType.ROCKETMQ not in SET_CAPABLE_BACKENDS

  settings = _FakeSettings(type_value="rocketmq")
  with pytest.raises(ConfigurationError) as exc_info:
    resolve_backend_config(
      settings,
      type_key="SCRAPY_SET_BACKEND_TYPE",
      settings_key="SCRAPY_SET_BACKEND_SETTINGS",
      required_capabilities=SET_CAPABLE_BACKENDS,
      component_name="set",
    )
  msg = str(exc_info.value)
  assert "rocketmq" in msg
  assert "set" in msg
  assert "redis" in msg  # the suggested capable backends list


def test_resolve_backend_config_rejects_rocketmq_for_storage():
  """Layer-2 guard: configuring RocketMQ for the storage component fails at config time.

  RocketMQ is intentionally excluded from STORAGE_CAPABLE_BACKENDS.
  """
  assert BackendType.ROCKETMQ not in STORAGE_CAPABLE_BACKENDS

  settings = _FakeSettings(type_value="rocketmq")
  with pytest.raises(ConfigurationError) as exc_info:
    resolve_backend_config(
      settings,
      type_key="SCRAPY_STORAGE_BACKEND_TYPE",
      settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
      required_capabilities=STORAGE_CAPABLE_BACKENDS,
      component_name="storage",
    )
  msg = str(exc_info.value)
  assert "rocketmq" in msg
  assert "storage" in msg


def test_resolve_backend_config_accepts_rocketmq_for_queue():
  """Sanity: RocketMQ IS queue-capable, so the queue config path succeeds.

  Guards against an over-broad capability set that would block the only
  supported RocketMQ interface.
  """
  from scrapy_extension.backends.connectors import QUEUE_CAPABLE_BACKENDS

  assert BackendType.ROCKETMQ in QUEUE_CAPABLE_BACKENDS

  settings = _FakeSettings(type_value="rocketmq")
  backend_type, backend_settings = resolve_backend_config(
    settings,
    type_key="SCRAPY_QUEUE_BACKEND_TYPE",
    settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
    required_capabilities=QUEUE_CAPABLE_BACKENDS,
    component_name="queue",
  )
  assert backend_type is BackendType.ROCKETMQ
  assert backend_settings == {}


# ---------------------------------------------------------------------------
# E3: class-level guard — RocketMQSetBackend / RocketMQStorageBackend
# ---------------------------------------------------------------------------


def test_rocketmq_set_backend_construction_raises_configuration_error():
  """E3: instantiating RocketMQSetBackend fails fast with a typed error.

  Replaces the unreachable per-method ``NotImplementedError`` stubs. The
  connector layer already excludes RocketMQ from SET_CAPABLE_BACKENDS, so
  the stubs were misleading dead code. A class-level ``__init__`` guard
  surfaces the misconfiguration immediately and with a typed error if
  someone bypasses the connector gating.
  """
  from scrapy_extension.backends.rocketmq import RocketMQSetBackend

  with pytest.raises(ConfigurationError) as exc_info:
    RocketMQSetBackend(RocketMQSettings())

  msg = str(exc_info.value)
  assert "RocketMQ" in msg
  assert "set" in msg.lower()
  assert "SCRAPY_SET_BACKEND_TYPE" in msg


def test_rocketmq_storage_backend_construction_raises_configuration_error():
  """E3: instantiating RocketMQStorageBackend fails fast with a typed error."""
  from scrapy_extension.backends.rocketmq import RocketMQStorageBackend

  with pytest.raises(ConfigurationError) as exc_info:
    RocketMQStorageBackend(RocketMQSettings())

  msg = str(exc_info.value)
  assert "RocketMQ" in msg
  assert "storage" in msg.lower()
  assert "SCRAPY_STORAGE_BACKEND_TYPE" in msg


def test_rocketmq_set_backend_class_is_importable():
  """E3: the guard classes remain importable (lazy-import architecture preserved)."""
  from scrapy_extension.backends.rocketmq import (
    RocketMQBackend,
    RocketMQSetBackend,
    RocketMQStorageBackend,
  )

  assert RocketMQBackend is not None
  assert RocketMQSetBackend is not None
  assert RocketMQStorageBackend is not None
