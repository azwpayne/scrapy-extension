"""Tests for connection manager."""

from unittest.mock import MagicMock, patch

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.connection.manager import ConnectionManager


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


def test_connection_manager_create_mongodb_backend():
  """Test ConnectionManager creates MongoDB backend."""
  with patch(
    "scrapy_extension.backends.mongodb_backend.MongoDBBackend"
  ) as mock_backend:
    mock_instance = MagicMock()
    mock_backend.return_value = mock_instance

    manager = ConnectionManager(BackendType.MONGODB)
    backend = manager._create_backend()  # noqa: SLF001

    mock_backend.assert_called_once()
    assert backend == mock_instance


def test_connection_manager_create_kafka_backend():
  """Test ConnectionManager creates Kafka backend."""
  with patch("scrapy_extension.backends.kafka_backend.KafkaBackend") as mock_backend:
    mock_instance = MagicMock()
    mock_backend.return_value = mock_instance

    manager = ConnectionManager(BackendType.KAFKA)
    backend = manager._create_backend()  # noqa: SLF001

    mock_backend.assert_called_once()


def test_connection_manager_create_rabbitmq_backend():
  """Test ConnectionManager creates RabbitMQ backend."""
  with patch(
    "scrapy_extension.backends.rabbitmq_backend.RabbitMQBackend"
  ) as mock_backend:
    mock_instance = MagicMock()
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


def test_connection_manager_get_set_backend_not_supported():
  """get_set_backend should raise NotImplementedError for unsupported backend."""
  manager = ConnectionManager(BackendType.KAFKA)
  manager._backend = object()

  with pytest.raises(NotImplementedError):
    manager.get_set_backend()
