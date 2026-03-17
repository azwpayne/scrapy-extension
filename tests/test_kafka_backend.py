import pytest
from unittest.mock import MagicMock, patch
from scrapy_extension.config.settings import KafkaSettings


def test_kafka_backend_connect():
    """Test Kafka backend connection."""
    from scrapy_extension.backends.kafka_backend import KafkaBackend

    config = KafkaSettings()
    backend = KafkaBackend(config)

    with patch("scrapy_extension.backends.kafka_backend.KafkaProducer") as mock_producer, \
         patch("scrapy_extension.backends.kafka_backend.KafkaConsumer") as mock_consumer, \
         patch("scrapy_extension.backends.kafka_backend.KafkaAdminClient") as mock_admin_client:
        mock_producer.return_value = MagicMock()
        mock_consumer.return_value = MagicMock()
        mock_admin_client.return_value = MagicMock()

        backend.connect()

        mock_producer.assert_called_once()
        mock_admin_client.assert_called_once()
        assert backend.is_connected()


def test_kafka_backend_push():
    """Test Kafka backend push."""
    from scrapy_extension.backends.kafka_backend import KafkaBackend

    config = KafkaSettings()
    backend = KafkaBackend(config)

    with patch("scrapy_extension.backends.kafka_backend.KafkaProducer") as mock_producer, \
         patch("scrapy_extension.backends.kafka_backend.KafkaAdminClient") as mock_admin_client:
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_admin_client_instance = MagicMock()
        mock_admin_client.return_value = mock_admin_client_instance
        mock_admin_client_instance.list_topics.return_value = []
        mock_future = MagicMock()
        mock_producer_instance.send.return_value = mock_future

        backend.connect()
        backend.push("test_queue", b"test_item", priority=1.0)

        mock_producer_instance.send.assert_called_once()
        # Check positional args: send(topic, value=value, partition=partition)
        call_args = mock_producer_instance.send.call_args
        assert call_args[0][0] == "scrapy-test_queue"  # First positional arg is topic
        assert call_args[1]["value"] == b"test_item"
        assert call_args[1]["partition"] == 1


def test_kafka_backend_only_implements_queuebackend():
    """Test that KafkaBackend only implements QueueBackend protocol."""
    from scrapy_extension.backends.kafka_backend import KafkaBackend
    from scrapy_extension.backends.base import QueueBackend, Backend

    config = KafkaSettings()
    backend = KafkaBackend(config)

    # Should implement Backend and QueueBackend
    assert isinstance(backend, Backend)
    assert isinstance(backend, QueueBackend)
