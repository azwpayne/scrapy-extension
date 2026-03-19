"""Tests for RabbitMQ backend implementation."""

from unittest.mock import MagicMock, patch

import pytest

from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
from scrapy_extension.config.settings import RabbitMQSettings


def test_rabbitmq_backend_connect():
  """Test RabbitMQ backend connection."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_conn.return_value = mock_instance

    backend.connect()

    mock_conn.assert_called_once()
    assert backend.is_connected()


def test_rabbitmq_backend_push():
  """Test RabbitMQ backend push."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_channel = MagicMock()
    mock_instance.channel.return_value = mock_channel
    mock_conn.return_value = mock_instance

    backend.connect()
    backend.push("test_queue", b"test_item", priority=5)

    mock_channel.queue_declare.assert_called_once()
    mock_channel.basic_publish.assert_called_once()
    call_kwargs = mock_channel.basic_publish.call_args[1]
    assert call_kwargs["routing_key"] == "test_queue"
    assert call_kwargs["body"] == b"test_item"


def test_rabbitmq_backend_pop():
  """Test RabbitMQ backend pop."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_channel = MagicMock()
    mock_instance.channel.return_value = mock_channel
    mock_conn.return_value = mock_instance

    # Mock message return
    mock_method = MagicMock()
    mock_method.delivery_tag = "tag123"
    mock_channel.basic_get.return_value = (mock_method, None, b"test_item")

    backend.connect()
    result = backend.pop("test_queue")

    assert result == b"test_item"
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="tag123")


def test_rabbitmq_backend_pop_empty():
  """Test RabbitMQ backend pop with empty queue."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_channel = MagicMock()
    mock_instance.channel.return_value = mock_channel
    mock_conn.return_value = mock_instance

    # Mock empty queue (None returned)
    mock_channel.basic_get.return_value = (None, None, None)

    backend.connect()
    result = backend.pop("test_queue")

    assert result is None


def test_rabbitmq_backend_queue_len():
  """Test RabbitMQ backend queue length."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_channel = MagicMock()
    mock_instance.channel.return_value = mock_channel
    mock_conn.return_value = mock_instance

    # Mock queue_declare return with message_count
    mock_method = MagicMock()
    mock_method.method.message_count = 5
    mock_channel.queue_declare.return_value = mock_method

    backend.connect()
    length = backend.queue_len("test_queue")

    assert length == 5


def test_rabbitmq_backend_clear_queue():
  """Test RabbitMQ backend clear queue."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_channel = MagicMock()
    mock_instance.channel.return_value = mock_channel
    mock_conn.return_value = mock_instance

    backend.connect()
    backend.clear_queue("test_queue")

    mock_channel.queue_purge.assert_called_once_with(queue="test_queue")


def test_rabbitmq_backend_disconnect():
  """Test RabbitMQ backend disconnect."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_conn.return_value = mock_instance

    backend.connect()
    assert backend.is_connected()

    backend.disconnect()
    assert not backend.is_connected()
    mock_instance.close.assert_called_once()


def test_rabbitmq_backend_ping():
  """Test RabbitMQ backend ping."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_channel = MagicMock()
    mock_instance.channel.return_value = mock_channel
    mock_instance.is_open = True
    mock_conn.return_value = mock_instance

    backend.connect()
    assert backend.ping()


def test_rabbitmq_backend_only_implements_queuebackend():
  """Test that RabbitMQBackend only implements QueueBackend protocol."""
  from scrapy_extension.backends.base import Backend, QueueBackend

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  # Should implement Backend and QueueBackend
  assert isinstance(backend, Backend)
  assert isinstance(backend, QueueBackend)


def test_rabbitmq_backend_connection_error():
  """Test RabbitMQ backend connection error handling."""
  import pika.exceptions

  from scrapy_extension.exceptions import BackendConnectionError

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_conn.side_effect = pika.exceptions.AMQPError("Connection failed")

    with pytest.raises(BackendConnectionError):
      backend.connect()


def test_rabbitmq_backend_push_error():
  """Test RabbitMQ backend push error handling."""
  import pika.exceptions

  from scrapy_extension.exceptions import QueueError

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_channel = MagicMock()
    mock_instance.channel.return_value = mock_channel
    mock_conn.return_value = mock_instance

    mock_channel.basic_publish.side_effect = pika.exceptions.AMQPError("Publish failed")

    backend.connect()
    with pytest.raises(QueueError):
      backend.push("test_queue", b"test_item")


def test_rabbitmq_backend_pop_error():
  """Test RabbitMQ backend pop error handling."""
  import pika.exceptions

  from scrapy_extension.exceptions import QueueError

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with patch("pika.BlockingConnection") as mock_conn:
    mock_instance = MagicMock()
    mock_channel = MagicMock()
    mock_instance.channel.return_value = mock_channel
    mock_conn.return_value = mock_instance

    mock_channel.basic_get.side_effect = pika.exceptions.AMQPError("Get failed")

    backend.connect()
    with pytest.raises(QueueError):
      backend.pop("test_queue")
