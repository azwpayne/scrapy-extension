"""Tests for RocketMQ backend implementation."""

import pytest

from scrapy_extension.backends.base import Backend, BackendType, QueueBackend
from scrapy_extension.backends.rocketmq import RocketMQBackend
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.settings import RocketMQMode, RocketMQSettings


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


def test_rocketmq_backend_push_not_connected():
    """Test push raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    from scrapy_extension.exceptions import QueueError

    with pytest.raises(QueueError) as exc_info:
        backend.push("test_queue", b"item")
    assert "Not connected" in str(exc_info.value)


def test_rocketmq_backend_pop_not_connected():
    """Test pop raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    from scrapy_extension.exceptions import QueueError

    with pytest.raises(QueueError) as exc_info:
        backend.pop("test_queue")
    assert "Not connected" in str(exc_info.value)


def test_rocketmq_backend_queue_len_not_connected():
    """Test queue_len raises NotImplementedError when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    with pytest.raises(NotImplementedError):
        backend.queue_len("test_queue")


def test_rocketmq_backend_clear_queue_not_connected():
    """Test clear_queue raises error when not connected."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)

    from scrapy_extension.exceptions import QueueError

    with pytest.raises(QueueError) as exc_info:
        backend.clear_queue("test_queue")
    assert "Not connected" in str(exc_info.value)


def test_rocketmq_settings_defaults():
    """Test RocketMQSettings default values."""
    settings = RocketMQSettings()
    assert settings.mode == RocketMQMode.STANDALONE
    assert settings.namesrv_address == "localhost:9876"
    assert settings.consumer_group == "scrapy-extension-consumer"
    assert settings.producer_group == "scrapy-extension-producer"
    assert settings.topic == "scrapy-queue"
    assert settings.set_topic_suffix == "scrapy-set"
    assert settings.storage_topic_suffix == "scrapy-storage"
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
        topic="my-queue",
    )
    assert settings.mode == RocketMQMode.CLUSTER
    assert settings.namesrv_address == "rocketmq-cluster:9876"
    assert settings.access_key == "mykey"
    assert settings.secret_key == "mysecret"  # noqa: S105


def test_rocketmq_mode_enum_values():
    """Test RocketMQMode enum values."""
    assert RocketMQMode.STANDALONE.value == "standalone"
    assert RocketMQMode.CLUSTER.value == "cluster"
    assert RocketMQMode.CLOUD.value == "cloud"


def test_rocketmq_settings_env_prefix():
    """Test RocketMQSettings respects env prefix."""
    import os

    os.environ["SCRAPY_ROCKETMQ_NAMESRV_ADDRESS"] = "env-rocketmq:9876"
    settings = RocketMQSettings()
    assert settings.namesrv_address == "env-rocketmq:9876"
    os.environ.pop("SCRAPY_ROCKETMQ_NAMESRV_ADDRESS", None)
