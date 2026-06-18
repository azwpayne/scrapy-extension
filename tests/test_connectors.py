"""Tests for scrapy_extension/backends/connectors.py."""

from __future__ import annotations

import pytest

from scrapy_extension.backends.base import (
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import BackendConnectionError


class TestConnectionManagerCreateBackend:
  """Tests for _create_backend method."""

  def test_create_backend_redis(self, mocker):
    """Test _create_backend creates RedisBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_redis_backend = mocker.patch("scrapy_extension.backends.redis.RedisBackend")
    mock_redis_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.REDIS)
    backend = manager._create_backend()

    mock_redis_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_mongodb(self, mocker):
    """Test _create_backend creates MongoDBBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_mongo_backend = mocker.patch(
      "scrapy_extension.backends.mongodb.MongoDBBackend"
    )
    mock_mongo_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.MONGODB)
    backend = manager._create_backend()

    mock_mongo_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_kafka(self, mocker):
    """Test _create_backend creates KafkaBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_kafka_backend = mocker.patch("scrapy_extension.backends.kafka.KafkaBackend")
    mock_kafka_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.KAFKA)
    backend = manager._create_backend()

    mock_kafka_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_rabbitmq(self, mocker):
    """Test _create_backend creates RabbitMQBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_rabbitmq_backend = mocker.patch(
      "scrapy_extension.backends.rabbitmq.RabbitMQBackend"
    )
    mock_rabbitmq_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.RABBITMQ)
    backend = manager._create_backend()

    mock_rabbitmq_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_elasticsearch(self, mocker):
    """Test _create_backend creates ElasticSearchBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_es_backend = mocker.patch(
      "scrapy_extension.backends.elasticsearch.ElasticSearchBackend"
    )
    mock_es_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.ELASTICSEARCH)
    backend = manager._create_backend()

    mock_es_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_unsupported_type(self):
    """Test _create_backend raises ValueError for unsupported backend type."""
    manager = ConnectionManager(BackendType.REDIS)
    # Deliberately set invalid type to test error handling
    manager.backend_type = "INVALID"  # type: ignore[assignment]

    with pytest.raises(ValueError, match="Unsupported backend type"):
      manager._create_backend()

  def test_create_backend_rocketmq(self, mocker):
    """Test _create_backend creates RocketMQBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_rocketmq_backend = mocker.patch(
      "scrapy_extension.backends.rocketmq.RocketMQBackend"
    )
    mock_rocketmq_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.ROCKETMQ)
    backend = manager._create_backend()

    mock_rocketmq_backend.assert_called_once()
    assert backend == mock_backend


class TestConnectionManagerSettingsKey:
  """Tests for settings key generation in get_manager."""

  def test_settings_key_json_fallback(self):
    """Test JSON serialization falls back to string sorting for non-serializable settings."""

    # Create a settings object that cannot be JSON serialized
    # Use an object with __slots__ and no __dict__ so json.dumps can't serialize it
    class NonSerializable:
      __slots__ = ()

    settings = {"func": NonSerializable()}
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings)
    assert manager1 is manager2

  def test_settings_key_json_fallback_with_value_error(self, mocker):
    """Test JSON serialization falls back to str() sorting when json.dumps raises ValueError."""
    # Mock json.dumps to raise ValueError
    mock_json = mocker.patch("scrapy_extension.backends.connectors.json")
    mock_json.dumps.side_effect = ValueError("Object is not JSON serializable")

    settings = {"key": "value"}
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings)

    assert manager1 is manager2
    assert mock_json.dumps.call_count >= 1

  def test_settings_key_json_fallback_with_type_error(self, mocker):
    """Test JSON serialization falls back to str() sorting when json.dumps raises TypeError."""
    # Mock json.dumps to raise TypeError
    mock_json = mocker.patch("scrapy_extension.backends.connectors.json")
    mock_json.dumps.side_effect = TypeError("Object of type is not JSON serializable")

    settings = {"key": "value"}
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings)

    assert manager1 is manager2
    assert mock_json.dumps.call_count >= 1


class TestConnectionManagerRetryLogic:
  """Tests for retry logic with exponential backoff."""

  def test_connect_retry_exhausted(self, mocker):
    """Test connect raises BackendConnectionError after max retries."""
    mock_create_backend = mocker.patch.object(
      ConnectionManager,
      "_create_backend",
      side_effect=ConnectionError("Connection failed"),
    )
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")

    manager = ConnectionManager(
      BackendType.REDIS, {"retry_attempts": 3, "retry_delay": 0.1}
    )

    with pytest.raises(BackendConnectionError) as exc_info:
      manager.connect()

    assert "Failed to connect after 3 attempts" in str(exc_info.value)
    assert mock_create_backend.call_count == 3

  def test_connect_retry_success_on_first_attempt(self, mocker):
    """Test connect succeeds on first attempt without retries."""
    mock_create_backend = mocker.patch.object(ConnectionManager, "_create_backend")
    mock_backend = mocker.MagicMock()
    mock_create_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.REDIS)
    manager.connect()

    assert manager._backend == mock_backend
    assert mock_create_backend.call_count == 1

  def test_connect_retry_success_on_second_attempt(self, mocker):
    """Test connect succeeds on second attempt after initial failure."""
    mock_create_backend = mocker.patch.object(ConnectionManager, "_create_backend")
    mock_backend = mocker.MagicMock()

    # First call raises, second succeeds
    mock_create_backend.side_effect = [ConnectionError("Failed"), mock_backend]
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")

    manager = ConnectionManager(
      BackendType.REDIS, {"retry_attempts": 3, "retry_delay": 0.1}
    )
    manager.connect()

    assert manager._backend == mock_backend
    assert mock_create_backend.call_count == 2

  def test_connect_keyboard_interrupt_not_caught(self, mocker):
    """Test that KeyboardInterrupt is re-raised immediately."""
    mocker.patch.object(
      ConnectionManager, "_create_backend", side_effect=KeyboardInterrupt
    )
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")

    manager = ConnectionManager(BackendType.REDIS)

    with pytest.raises(KeyboardInterrupt):
      manager.connect()

  def test_connect_system_exit_not_caught(self, mocker):
    """Test that SystemExit is re-raised immediately."""
    mocker.patch.object(ConnectionManager, "_create_backend", side_effect=SystemExit)
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")

    manager = ConnectionManager(BackendType.REDIS)

    with pytest.raises(SystemExit):
      manager.connect()


class TestConnectionManagerClose:
  """Tests for close method."""

  def test_close_disconnects_backend(self, mocker):
    """Test close calls disconnect on backend."""
    mock_backend = mocker.MagicMock()
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    manager.close()

    mock_backend.disconnect.assert_called_once()
    assert manager._backend is None

  def test_close_handles_disconnect_error(self, mocker):
    """Test close handles errors during disconnect gracefully."""
    mock_backend = mocker.MagicMock()
    mock_backend.disconnect.side_effect = RuntimeError("Disconnect error")
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    # Should not raise
    manager.close()

    assert manager._backend is None

  def test_close_when_backend_is_none(self):
    """Test close does nothing when backend is None."""
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    # Should not raise
    manager.close()


class TestConnectionManagerBackendProperty:
  """Tests for backend property with double-checked locking."""

  def test_backend_returns_existing_backend(self, mocker):
    """Test backend property returns existing backend without connecting."""
    mock_connect = mocker.patch.object(ConnectionManager, "connect")
    mock_backend = mocker.MagicMock()
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    result = manager.backend

    assert result == mock_backend
    mock_connect.assert_not_called()

  def test_backend_calls_connect_when_none(self, mocker):
    """Test backend property calls connect when _backend is None."""
    mock_connect = mocker.patch.object(ConnectionManager, "connect")
    mock_backend = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    # Simulate what connect() would do by setting _backend after connect is called
    def setup_backend():
      manager._backend = mock_backend

    mock_connect.side_effect = setup_backend

    result = manager.backend

    assert result == mock_backend
    mock_connect.assert_called_once()

  def test_backend_double_checked_locking_sets_backend(self, mocker):
    """Test backend property double-checked locking sets _backend via connect."""
    mock_backend = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    # Access backend property - should trigger connect which sets _backend
    result = manager.backend

    assert result is mock_backend
    assert manager._backend is mock_backend

  def test_backend_double_checked_locking_assertion(self, mocker):
    """Test backend property assert passes when connect sets _backend."""
    mock_backend = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    # This should not raise AssertionError
    result = manager.backend

    assert result is mock_backend


class TestConnectionManagerIsConnected:
  """Tests for is_connected method."""

  def test_is_connected_returns_false_when_backend_none(self):
    """Test is_connected returns False when _backend is None."""
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    assert manager.is_connected() is False

  def test_is_connected_returns_backend_status(self, mocker):
    """Test is_connected returns result from backend.is_connected()."""
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    assert manager.is_connected() is True
    mock_backend.is_connected.assert_called_once()


class TestConnectionManagerGetBackendInterface:
  """Tests for get_queue_backend, get_set_backend, get_storage_backend."""

  def test_get_queue_backend_not_implemented(self, mocker):
    """Test get_queue_backend raises NotImplementedError for non-QueueBackend."""
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True

    manager = ConnectionManager(BackendType.KAFKA)
    manager._backend = mock_backend

    with pytest.raises(NotImplementedError) as exc_info:
      manager.get_queue_backend()

    assert "does not support queue operations" in str(exc_info.value)

  def test_get_queue_backend_returns_backend(self):
    """Test get_queue_backend returns backend when it implements QueueBackend."""
    from scrapy_extension.backends.base import Backend

    class MockQueueBackend(Backend, QueueBackend):
      """Mock backend implementing QueueBackend."""

      def __init__(self):
        self._is_connected = True

      def connect(self):
        pass

      def disconnect(self):
        pass

      def is_connected(self):
        return self._is_connected

      def ping(self):
        return True

      @property
      def backend_type(self):
        return BackendType.REDIS

      def push(self, queue_name, item, priority=0.0):
        pass

      def pop(self, queue_name, timeout=0.0):
        return None

      def queue_len(self, queue_name):
        return 0

      def clear_queue(self, queue_name):
        pass

    mock_backend = MockQueueBackend()

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    result = manager.get_queue_backend()

    assert result is mock_backend

  def test_get_set_backend_not_implemented(self, mocker):
    """Test get_set_backend raises NotImplementedError for non-SetBackend."""
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True

    manager = ConnectionManager(BackendType.KAFKA)
    manager._backend = mock_backend

    with pytest.raises(NotImplementedError) as exc_info:
      manager.get_set_backend()

    assert "does not support set operations" in str(exc_info.value)

  def test_get_set_backend_returns_backend(self):
    """Test get_set_backend returns backend when it implements SetBackend."""
    from scrapy_extension.backends.base import Backend

    class MockSetBackend(Backend, SetBackend):
      """Mock backend implementing SetBackend."""

      def __init__(self):
        self._is_connected = True

      def connect(self):
        pass

      def disconnect(self):
        pass

      def is_connected(self):
        return self._is_connected

      def ping(self):
        return True

      @property
      def backend_type(self):
        return BackendType.REDIS

      def add(self, set_name, item):
        return True

      def remove(self, set_name, item):
        return True

      def contains(self, set_name, item):
        return True

      def set_len(self, set_name):
        return 0

      def clear_set(self, set_name):
        pass

    mock_backend = MockSetBackend()

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    result = manager.get_set_backend()

    assert result is mock_backend

  def test_get_storage_backend_not_implemented(self, mocker):
    """Test get_storage_backend raises NotImplementedError for non-StorageBackend."""
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True

    manager = ConnectionManager(BackendType.KAFKA)
    manager._backend = mock_backend

    with pytest.raises(NotImplementedError) as exc_info:
      manager.get_storage_backend()

    assert "does not support storage operations" in str(exc_info.value)

  def test_get_storage_backend_returns_backend(self):
    """Test get_storage_backend returns backend when it implements StorageBackend."""
    from scrapy_extension.backends.base import Backend

    class MockStorageBackend(Backend, StorageBackend):
      """Mock backend implementing StorageBackend."""

      def __init__(self):
        self._is_connected = True

      def connect(self):
        pass

      def disconnect(self):
        pass

      def is_connected(self):
        return self._is_connected

      def ping(self):
        return True

      @property
      def backend_type(self):
        return BackendType.REDIS

      def store(self, key, data, ttl=None):
        pass

      def retrieve(self, key):
        return None

      def delete(self, key):
        return True

      def exists(self, key):
        return True

      def ttl(self, key):
        return None

      def clear_storage(self, prefix=None):
        pass

    mock_backend = MockStorageBackend()

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    result = manager.get_storage_backend()

    assert result is mock_backend


class TestConnectionManagerSingleton:
  """Tests for singleton pattern."""

  def test_get_manager_same_instance_same_params(self):
    """Test get_manager returns same instance for same backend_type and settings."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS)

    assert manager1 is manager2

  def test_get_manager_different_instance_different_backend_type(self):
    """Test get_manager returns different instance for different backend types."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS)
    manager2 = ConnectionManager.get_manager(BackendType.MONGODB)

    assert manager1 is not manager2

  def test_get_manager_different_instance_different_settings(self):
    """Test get_manager returns different instance for different settings."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "localhost"})
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "otherhost"})

    assert manager1 is not manager2
