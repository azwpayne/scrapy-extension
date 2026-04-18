"""Tests for KafkaBackend."""

from __future__ import annotations

import pytest
from kafka import TopicPartition
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from scrapy_extension.backends.kafka import KafkaBackend
from scrapy_extension.exceptions import (
  BackendConnectionError,
  ConfigurationError,
  QueueError,
)
from scrapy_extension.settings import KafkaMode, KafkaSettings


class TestKafkaBackendConnect:
  """Tests for connect() method and its helper methods."""

  def test_connect_unsupported_mode(self):
    """Test connect raises ConfigurationError for unsupported mode."""
    from unittest.mock import MagicMock

    # Create mock config with invalid mode
    mock_config = MagicMock()
    mock_config.mode = "invalid_mode"
    mock_config.sasl_mechanism = "PLAIN"
    mock_config.security_protocol = "PLAINTEXT"

    backend = KafkaBackend(mock_config)
    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()
    assert "Unsupported Kafka mode" in str(exc_info.value)
    assert exc_info.value.setting_name == "mode"

  def test_connect_standalone_success(self, mocker):
    """Test successful connection in standalone mode."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_producer_instance = mocker.MagicMock()
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaProducer",
      return_value=mock_producer_instance,
    )
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaAdminClient",
      return_value=mocker.MagicMock(),
    )

    backend.connect()

    assert backend.is_connected()
    mock_producer_instance.send.assert_not_called()

  def test_connect_standalone_kafka_error(self, mocker):
    """Test connect raises BackendConnectionError on KafkaError."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaProducer",
      side_effect=KafkaError("Connection failed"),
    )

    with pytest.raises(BackendConnectionError) as exc_info:
      backend.connect()
    assert "Failed to connect to Kafka" in str(exc_info.value)
    assert exc_info.value.backend_type == "kafka"

  def test_connect_standalone_generic_error(self, mocker):
    """Test connect raises BackendConnectionError on generic Exception."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaProducer",
      side_effect=RuntimeError("Unexpected error"),
    )

    with pytest.raises(BackendConnectionError) as exc_info:
      backend.connect()
    assert "Failed to connect to Kafka" in str(exc_info.value)


class TestKafkaBackendBuildCommonConfig:
  """Tests for _build_common_config method."""

  def test_build_common_config_basic(self):
    """Test basic config building without security."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    result = backend._build_common_config()

    assert result["acks"] == config.acks
    assert result["retries"] == config.retries
    assert result["batch_size"] == config.batch_size
    assert result["linger_ms"] == config.linger_ms
    assert result["compression_type"] == config.compression_type
    assert (
      result["max_in_flight_requests_per_connection"]
      == config.max_in_flight_requests_per_connection
    )
    assert result["request_timeout_ms"] == config.request_timeout_ms

  def test_build_common_config_with_sasl_ssl(self):
    """Test config building with SASL/SSL settings."""
    config = KafkaSettings(
      security_protocol="SASL_SSL",
      sasl_mechanism="PLAIN",
      sasl_username="myuser",
      sasl_password="mypass",
      ssl_cafile="/path/to/cafile",
      ssl_certfile="/path/to/certfile",
      ssl_keyfile="/path/to/keyfile",
      ssl_check_hostname=True,
    )
    backend = KafkaBackend(config)

    result = backend._build_common_config()

    assert result["security_protocol"] == "SASL_SSL"
    assert result["sasl_mechanism"] == "PLAIN"
    assert result["sasl_plain_username"] == "myuser"
    assert result["sasl_plain_password"] == "mypass"  # noqa: S105
    assert result["ssl_cafile"] == "/path/to/cafile"
    assert result["ssl_certfile"] == "/path/to/certfile"
    assert result["ssl_keyfile"] == "/path/to/keyfile"
    assert result["ssl_check_hostname"] is True

  def test_build_common_config_with_sasl_ssl_partial(self):
    """Test config building with SASL/SSL but missing some credentials."""
    config = KafkaSettings(
      security_protocol="SASL_SSL",
      sasl_mechanism="PLAIN",
      # sasl_username and sasl_password not set
    )
    backend = KafkaBackend(config)

    result = backend._build_common_config()

    assert result["security_protocol"] == "SASL_SSL"
    assert "sasl_mechanism" not in result
    assert "sasl_plain_username" not in result
    assert "sasl_plain_password" not in result

  def test_build_common_config_ssl_no_cafile(self):
    """Test config building with SSL but no CA file."""
    config = KafkaSettings(
      security_protocol="SSL",
      ssl_cafile=None,
      ssl_certfile=None,
      ssl_keyfile=None,
      ssl_check_hostname=False,
    )
    backend = KafkaBackend(config)

    result = backend._build_common_config()

    assert result["security_protocol"] == "SSL"
    assert "ssl_cafile" not in result
    assert "ssl_certfile" not in result
    assert "ssl_keyfile" not in result
    assert result["ssl_check_hostname"] is False


class TestKafkaBackendClusterMode:
  """Tests for _connect_cluster method."""

  def test_connect_cluster_with_cluster_brokers(self, mocker):
    """Test _connect_cluster uses cluster_brokers when available."""
    config = KafkaSettings(
      mode=KafkaMode.CLUSTER,
      cluster_brokers=["broker1:9092", "broker2:9092"],
    )
    backend = KafkaBackend(config)

    mock_producer_instance = mocker.MagicMock()
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaProducer",
      return_value=mock_producer_instance,
    )
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaAdminClient",
      return_value=mocker.MagicMock(),
    )

    backend.connect()

    # Verify KafkaProducer was called - the mock returns mock_producer_instance
    # which gets stored in backend._producer
    assert backend._producer is mock_producer_instance

  def test_connect_cluster_without_cluster_brokers(self, mocker):
    """Test _connect_cluster falls back to bootstrap_servers."""
    config = KafkaSettings(
      mode=KafkaMode.CLUSTER,
      cluster_brokers=[],  # empty
      bootstrap_servers="fallback:9092",
    )
    backend = KafkaBackend(config)

    mock_producer_instance = mocker.MagicMock()
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaProducer",
      return_value=mock_producer_instance,
    )
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaAdminClient",
      return_value=mocker.MagicMock(),
    )

    backend.connect()

    # Should use bootstrap_servers as fallback
    assert backend.is_connected()


class TestKafkaBackendConfluentMode:
  """Tests for _connect_confluent method."""

  def test_connect_confluent_with_api_key_secret(self, mocker):
    """Test _connect_confluent uses API key/secret when provided."""
    config = KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      confluent_api_key="api_key_123",
      confluent_api_secret="api_secret_456",
      confluent_bootstrap_servers="pulsar://abc.xyz.confluent.cloud:9092",
    )
    backend = KafkaBackend(config)

    mock_producer_instance = mocker.MagicMock()
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaProducer",
      return_value=mock_producer_instance,
    )
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaAdminClient",
      return_value=mocker.MagicMock(),
    )

    backend.connect()

    assert backend.is_connected()

  def test_connect_confluent_without_api_key_falls_back_to_sasl(self, mocker):
    """Test _connect_confluent falls back to SASL config when no API key."""
    config = KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      confluent_api_key=None,
      confluent_api_secret=None,
      confluent_bootstrap_servers="custom:9092",
      security_protocol="SASL_SSL",
      sasl_mechanism="PLAIN",
      sasl_username="user",
      sasl_password="pass",  # noqa: S105
    )
    backend = KafkaBackend(config)

    mock_producer_instance = mocker.MagicMock()
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaProducer",
      return_value=mock_producer_instance,
    )
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaAdminClient",
      return_value=mocker.MagicMock(),
    )

    backend.connect()

    assert backend.is_connected()


class TestKafkaBackendDisconnect:
  """Tests for disconnect method."""

  def test_disconnect_closes_all_clients(self, mocker):
    """Test disconnect closes producer, consumer, and admin_client."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_producer = mocker.MagicMock()
    mock_consumer = mocker.MagicMock()
    mock_admin = mocker.MagicMock()

    backend._producer = mock_producer
    backend._consumer = mock_consumer
    backend._admin_client = mock_admin

    backend.disconnect()

    mock_producer.close.assert_called_once()
    mock_consumer.close.assert_called_once()
    mock_admin.close.assert_called_once()
    assert backend._producer is None
    assert backend._consumer is None
    assert backend._admin_client is None

  def test_disconnect_handles_none_clients(self):
    """Test disconnect handles None clients gracefully."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    # All are None initially
    backend.disconnect()  # Should not raise


class TestKafkaBackendPing:
  """Tests for ping method."""

  def test_ping_returns_true_when_admin_client_available(self, mocker):
    """Test ping returns True when admin_client can list topics."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.list_topics.return_value = ["topic1", "topic2"]
    backend._admin_client = mock_admin

    result = backend.ping()

    assert result is True
    mock_admin.list_topics.assert_called_once()

  def test_ping_returns_false_on_kafka_error(self, mocker):
    """Test ping returns False when admin_client raises KafkaError."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.list_topics.side_effect = KafkaError("Network error")
    backend._admin_client = mock_admin

    result = backend.ping()

    assert result is False

  def test_ping_returns_false_when_admin_client_is_none(self):
    """Test ping returns False when admin_client is None."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    backend._admin_client = None

    result = backend.ping()

    assert result is False


class TestKafkaBackendEnsureTopicExists:
  """Tests for _ensure_topic_exists method."""

  def test_ensure_topic_exists_skips_known_topic(self, mocker):
    """Test _ensure_topic_exists skips topics in cache."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    # Pre-populate known topics
    backend._known_topics.add("scrapy-myqueue")

    # Should not call admin client
    mock_admin = mocker.MagicMock()
    backend._admin_client = mock_admin

    backend._ensure_topic_exists("myqueue")

    mock_admin.create_topics.assert_not_called()

  def test_ensure_topic_exists_creates_new_topic(self, mocker):
    """Test _ensure_topic_exists creates topic when not known."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    backend._admin_client = mock_admin

    backend._ensure_topic_exists("newqueue")

    mock_admin.create_topics.assert_called_once()
    call_args = mock_admin.create_topics.call_args
    new_topic = call_args[0][0][0]
    assert isinstance(new_topic, NewTopic)
    assert new_topic.name == "scrapy-newqueue"
    assert "scrapy-newqueue" in backend._known_topics

  def test_ensure_topic_exists_handles_topic_already_exists(self, mocker):
    """Test _ensure_topic_exists handles TopicAlreadyExistsError gracefully."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.create_topics.side_effect = TopicAlreadyExistsError(
      "Topic already exists"
    )
    backend._admin_client = mock_admin

    backend._ensure_topic_exists("existingqueue")

    # Should still add to known topics
    assert "scrapy-existingqueue" in backend._known_topics

  def test_ensure_topic_exists_handles_kafka_error_on_create(self, mocker):
    """Test _ensure_topic_exists logs warning on KafkaError."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.create_topics.side_effect = KafkaError("Create failed")
    backend._admin_client = mock_admin

    # Should not raise, just log warning
    backend._ensure_topic_exists("failedqueue")


class TestKafkaBackendPush:
  """Tests for push method."""

  def test_push_success(self, mocker):
    """Test successful push to queue."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_producer = mocker.MagicMock()
    mock_future = mocker.MagicMock()
    mock_producer.send.return_value = mock_future
    backend._producer = mock_producer

    mock_admin = mocker.MagicMock()
    backend._admin_client = mock_admin

    backend.push("testq", b"item_data", priority=5.0)

    call_args = mock_producer.send.call_args
    assert call_args[0][0] == "scrapy-testq"
    assert call_args[1]["value"] == b"item_data"
    assert call_args[1]["partition"] == 5
    mock_future.get.assert_called_once_with(timeout=10)

  def test_push_with_priority_clamped_to_max(self, mocker):
    """Test priority is clamped to max_priority_partitions - 1."""
    config = KafkaSettings(max_priority_partitions=10)
    backend = KafkaBackend(config)

    mock_producer = mocker.MagicMock()
    mock_future = mocker.MagicMock()
    mock_producer.send.return_value = mock_future
    backend._producer = mock_producer

    mock_admin = mocker.MagicMock()
    backend._admin_client = mock_admin

    # Priority 255 should be clamped to 9 (max_priority_partitions - 1)
    backend.push("testq", b"item", priority=255.0)

    call_args = mock_producer.send.call_args
    assert call_args[1]["partition"] == 9

  def test_push_raises_queue_error_on_kafka_error(self, mocker):
    """Test push raises QueueError on KafkaError."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_producer = mocker.MagicMock()
    mock_producer.send.side_effect = KafkaError("Send failed")
    backend._producer = mock_producer

    mock_admin = mocker.MagicMock()
    backend._admin_client = mock_admin

    with pytest.raises(QueueError) as exc_info:
      backend.push("testq", b"item")
    assert "Failed to push to queue" in str(exc_info.value)
    assert exc_info.value.queue_name == "testq"
    assert exc_info.value.operation == "push"


class TestKafkaBackendPop:
  """Tests for pop method."""

  def test_pop_returns_message(self, mocker):
    """Test pop returns message value."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_consumer = mocker.MagicMock()
    mock_record = mocker.MagicMock()
    mock_record.value = b"popped_data"
    mock_consumer.poll.return_value = {
      TopicPartition("scrapy-testq", 0): [mock_record],
    }
    backend._consumer = mock_consumer

    result = backend.pop("testq", timeout=1.0)

    assert result == b"popped_data"

  def test_pop_returns_none_when_queue_empty(self, mocker):
    """Test pop returns None when no messages."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_consumer = mocker.MagicMock()
    mock_consumer.poll.return_value = {}  # No messages
    backend._consumer = mock_consumer

    result = backend.pop("testq", timeout=1.0)

    assert result is None

  def test_pop_creates_consumer_if_none(self, mocker):
    """Test pop creates consumer if not already created."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_consumer = mocker.MagicMock()
    mock_record = mocker.MagicMock()
    mock_record.value = b"data"
    mock_consumer.poll.return_value = {
      TopicPartition("scrapy-testq", 0): [mock_record],
    }
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      return_value=mock_consumer,
    )

    result = backend.pop("testq", timeout=0.0)

    assert result == b"data"

  def test_pop_raises_queue_error_on_kafka_error(self, mocker):
    """Test pop raises QueueError on KafkaError."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_consumer = mocker.MagicMock()
    mock_consumer.poll.side_effect = KafkaError("Poll failed")
    backend._consumer = mock_consumer

    with pytest.raises(QueueError) as exc_info:
      backend.pop("testq")
    assert "Failed to pop from queue" in str(exc_info.value)
    assert exc_info.value.queue_name == "testq"
    assert exc_info.value.operation == "pop"


class TestKafkaBackendQueueLen:
  """Tests for queue_len method."""

  def test_queue_len_success(self, mocker):
    """Test queue_len returns correct count."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.describe_topics.return_value = [
      {
        "partition": 0,
        "partitions": [
          {"partition": 0},
          {"partition": 1},
        ],
      },
    ]
    backend._admin_client = mock_admin

    mock_temp_consumer = mocker.MagicMock()
    mock_temp_consumer.beginning_offsets.return_value = {
      TopicPartition("scrapy-testq", 0): 0,
      TopicPartition("scrapy-testq", 1): 0,
    }
    mock_temp_consumer.end_offsets.return_value = {
      TopicPartition("scrapy-testq", 0): 10,
      TopicPartition("scrapy-testq", 1): 5,
    }
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      return_value=mock_temp_consumer,
    )

    result = backend.queue_len("testq")

    assert result == 15  # (10-0) + (5-0) = 15

  def test_queue_len_returns_zero_on_kafka_error(self, mocker):
    """Test queue_len returns 0 on KafkaError."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.describe_topics.side_effect = KafkaError("Describe failed")
    backend._admin_client = mock_admin

    result = backend.queue_len("testq")

    assert result == 0


class TestKafkaBackendClearQueue:
  """Tests for clear_queue method."""

  def test_clear_queue_success(self, mocker):
    """Test clear_queue deletes and recreates topic."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    backend._admin_client = mock_admin

    backend.clear_queue("testq")

    mock_admin.delete_topics.assert_called_once_with(["scrapy-testq"])
    mock_admin.create_topics.assert_called_once()
    call_args = mock_admin.create_topics.call_args
    new_topic = call_args[0][0][0]
    assert isinstance(new_topic, NewTopic)
    assert new_topic.name == "scrapy-testq"

  def test_clear_queue_handles_kafka_error(self, mocker):
    """Test clear_queue logs warning on KafkaError."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.delete_topics.side_effect = KafkaError("Delete failed")
    backend._admin_client = mock_admin

    # Should not raise
    backend.clear_queue("testq")


class TestKafkaBackendBackendType:
  """Tests for backend_type property."""

  def test_backend_type_returns_kafka(self):
    """Test backend_type returns BackendType.KAFKA."""
    from scrapy_extension.backends.base import BackendType

    config = KafkaSettings()
    backend = KafkaBackend(config)

    assert backend.backend_type == BackendType.KAFKA


# ============================================================================
# Re-use existing tests to ensure they still pass
# ============================================================================


def test_kafka_backend_connect(mocker):
  """Test Kafka backend connection."""
  config = KafkaSettings()
  backend = KafkaBackend(config)

  mock_producer = mocker.patch("scrapy_extension.backends.kafka.KafkaProducer")
  mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer")
  mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient")

  mock_producer.return_value = mocker.MagicMock()

  backend.connect()

  mock_producer.assert_called_once()
  assert backend.is_connected()


def test_kafka_backend_push(mocker):
  """Test Kafka backend push."""
  config = KafkaSettings()
  backend = KafkaBackend(config)

  mock_producer_instance = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.kafka.KafkaProducer",
    return_value=mock_producer_instance,
  )
  mock_admin_client_instance = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.kafka.KafkaAdminClient",
    return_value=mock_admin_client_instance,
  )
  mock_admin_client_instance.list_topics.return_value = []
  mock_future = mocker.MagicMock()
  mock_producer_instance.send.return_value = mock_future

  backend.connect()
  backend.push("test_queue", b"test_item", priority=1.0)

  mock_producer_instance.send.assert_called_once()
  call_args = mock_producer_instance.send.call_args
  assert call_args[0][0] == "scrapy-test_queue"
  assert call_args[1]["value"] == b"test_item"
  assert call_args[1]["partition"] == 1


def test_kafka_backend_only_implements_queuebackend():
  """Test that KafkaBackend only implements QueueBackend protocol."""
  from scrapy_extension.backends.base import Backend, QueueBackend

  config = KafkaSettings()
  backend = KafkaBackend(config)

  assert isinstance(backend, Backend)
  assert isinstance(backend, QueueBackend)
