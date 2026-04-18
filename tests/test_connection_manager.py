"""Tests for connection manager."""

import pytest
from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager


def test_connection_manager_get_manager_singleton():
  """Test that get_manager returns singleton for same params."""
  manager1 = ConnectionManager.get_manager(BackendType.REDIS)
  manager2 = ConnectionManager.get_manager(BackendType.REDIS)
  assert manager1 is manager2


def test_connection_manager_different_params():
  """Test that different params return different managers."""
  manager1 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "localhost"})
  manager2 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "other"})
  assert manager1 is not manager2


def test_connection_manager_create_mongodb_backend(mocker):
  """Test ConnectionManager creates MongoDB backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.mongodb.MongoDBBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.MONGODB)
  backend = manager._create_backend()  # noqa: SLF001

  mock_backend.assert_called_once()
  assert backend == mock_instance


def test_connection_manager_create_kafka_backend(mocker):
  """Test ConnectionManager creates Kafka backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.kafka.KafkaBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.KAFKA)
  backend = manager._create_backend()  # noqa: SLF001

  mock_backend.assert_called_once()


def test_connection_manager_create_rabbitmq_backend(mocker):
  """Test ConnectionManager creates RabbitMQ backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.rabbitmq.RabbitMQBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.RABBITMQ)
  backend = manager._create_backend()  # noqa: SLF001

  mock_backend.assert_called_once()


def test_connection_manager_get_manager_same_settings_order():
  """Same settings with different key order should resolve to same manager."""
  settings_a = {"a": 1, "b": 2}
  settings_b = {"b": 2, "a": 1}

  manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings_a)
  manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings_b)

  assert manager1 is manager2


def test_connection_manager_get_set_backend_not_supported(mocker):
  """get_set_backend should raise NotImplementedError for unsupported backend."""
  manager = ConnectionManager(BackendType.KAFKA)
  # We need to set _backend to something that is not a SetBackend but is a Backend subclass
  mock_backend = mocker.MagicMock()
  mock_backend.is_connected.return_value = True
  manager._backend = mock_backend

  with pytest.raises(NotImplementedError):
    manager.get_set_backend()
