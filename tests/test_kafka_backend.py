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
    assert result["sasl_plain_password"] == "mypass"
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
      sasl_password="pass",
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


class TestKafkaBackendPushPriorityMapping:
  """Tests for push() priority mapping."""

  def test_push_clamps_negative_priority_to_partition_zero(self, mocker):
    """Negative priorities must map to the lowest valid partition."""
    config = KafkaSettings(max_priority_partitions=10)
    backend = KafkaBackend(config)

    mock_future = mocker.MagicMock()
    mock_producer = mocker.MagicMock()
    mock_producer.send.return_value = mock_future
    backend._producer = mock_producer
    backend._admin_client = mocker.MagicMock()

    backend.push("test-queue", b"item", priority=-3)

    mock_producer.send.assert_called_once_with(
      "scrapy-test-queue",
      value=b"item",
      partition=0,
    )
    mock_future.get.assert_called_once_with(timeout=10)


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

  def test_pop_subscribes_once_per_topic_not_every_call(self, mocker):
    """R57: pop() re-subscribes only when the topic changes, not every call.

    Mirrors RocketMQ's ``_ensure_subscribed`` (R7). Pre-R57, pop() called
    ``subscribe([topic])`` unconditionally on every pop — wasteful on
    Scrapy's hot path (``next_request`` pops the same queue every tick).
    """
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_consumer = mocker.MagicMock()
    mock_consumer.poll.return_value = {}  # empty → pop returns None cleanly
    backend._consumer = mock_consumer

    backend.pop("queue_a", timeout=0.0)
    backend.pop("queue_a", timeout=0.0)  # same topic → no re-subscribe
    backend.pop("queue_b", timeout=0.0)  # different topic → re-subscribe

    # subscribe called exactly twice (once per distinct topic), not 3 times.
    assert mock_consumer.subscribe.call_count == 2
    mock_consumer.subscribe.assert_any_call(["scrapy-queue_a"])
    mock_consumer.subscribe.assert_any_call(["scrapy-queue_b"])

  def test_pop_does_not_warn_on_concurrent_pops(self, mocker, caplog):
    """Tier-2 Unit H: the single-slot defect warning is GONE.

    Previously pop() warned about CONCURRENT_REQUESTS>1 because the single
    _last_record slot would be overwritten. With the in-flight-set fix
    (pop_with_ack tracks every popped offset), concurrent pops no longer
    warn — they're correct. This test pins the warning's absence so a
    regression to the single-slot path is caught.
    """
    import logging

    from kafka import TopicPartition

    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_consumer = mocker.MagicMock()
    mock_record = mocker.MagicMock()
    mock_record.value = b"data"
    # Two polls, each yielding a record (second pop happens before any ack).
    mock_consumer.poll.side_effect = [
      {TopicPartition("scrapy-testq", 0): [mock_record]},
      {TopicPartition("scrapy-testq", 0): [mock_record]},
    ]
    backend._consumer = mock_consumer

    caplog.clear()
    with caplog.at_level(logging.WARNING):
      backend.pop("testq", timeout=0.0)
      backend.pop("testq", timeout=0.0)  # concurrent pop — no longer a defect

    assert "pop() called while previous message is unacked" not in caplog.text
    assert "CONCURRENT_REQUESTS>1" not in caplog.text

  def test_nack_is_in_session_noop_that_clears_record(self, mocker):
    """R11/R12: nack() is an in-session no-op — clears the tracked record, does NOT commit.

    The offset stays uncommitted so the message re-delivers on the next
    consumer restart (at-least-once retry). Within a running session nack
    can't re-deliver, so it just drops the tracked record (so a subsequent
    pop doesn't spuriously warn).
    """
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    backend._consumer = mock_consumer
    backend._last_record = mocker.MagicMock()

    backend.nack("testq")

    assert backend._last_record is None
    # The whole point of nack: do NOT commit — the offset must stay uncommitted.
    mock_consumer.commit.assert_not_called()

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

  def test_pop_cluster_mode_uses_cluster_bootstrap_servers(self, mocker):
    """Test pop creates consumer with cluster brokers in cluster mode."""
    config = KafkaSettings(
      mode=KafkaMode.CLUSTER,
      bootstrap_servers="fallback:9092",
      cluster_brokers=["broker1:9092", "broker2:9092"],
    )
    backend = KafkaBackend(config)

    mock_consumer = mocker.MagicMock()
    mock_record = mocker.MagicMock()
    mock_record.value = b"data"
    mock_consumer.poll.return_value = {
      TopicPartition("scrapy-testq", 0): [mock_record],
    }
    consumer_cls = mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      return_value=mock_consumer,
    )

    result = backend.pop("testq", timeout=0.0)

    assert result == b"data"
    assert consumer_cls.call_args.kwargs["bootstrap_servers"] == "broker1:9092,broker2:9092"

  def test_pop_confluent_mode_uses_security_config(self, mocker):
    """Test pop creates consumer with Confluent SASL/SSL settings."""
    config = KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      bootstrap_servers="fallback:9092",
      confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
      confluent_api_key="test_key",
      confluent_api_secret="test_secret",
    )
    backend = KafkaBackend(config)

    mock_consumer = mocker.MagicMock()
    mock_record = mocker.MagicMock()
    mock_record.value = b"data"
    mock_consumer.poll.return_value = {
      TopicPartition("scrapy-testq", 0): [mock_record],
    }
    consumer_cls = mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      return_value=mock_consumer,
    )

    result = backend.pop("testq", timeout=0.0)

    assert result == b"data"
    assert consumer_cls.call_args.kwargs["bootstrap_servers"] == (
      "pkc-xxx.us-east-1.aws.confluent.cloud:9092"
    )
    assert consumer_cls.call_args.kwargs["security_protocol"] == "SASL_SSL"
    assert consumer_cls.call_args.kwargs["sasl_mechanism"] == "PLAIN"
    assert consumer_cls.call_args.kwargs["sasl_plain_username"] == "test_key"
    assert consumer_cls.call_args.kwargs["sasl_plain_password"] == "test_secret"

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

  def test_pop_does_not_auto_ack_after_round_12(self, mocker):
    """Round 12: pop no longer auto-commits — ack is driven by Scrapy signals.

    This preserves at-least-once semantics: if the worker crashes before
    the signal fires, the offset isn't committed and the message
    re-delivers on consumer restart.
    """
    from kafka.structs import TopicPartition

    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    mock_record = mocker.MagicMock()
    mock_record.value = b"payload"
    tp = TopicPartition("test_topic", 0)
    mock_consumer.poll.return_value = {tp: [mock_record]}
    backend._consumer = mock_consumer

    result = backend.pop("testq")

    assert result == b"payload"
    # The record is tracked for signal-driven ack, but NOT committed yet.
    assert backend._last_record is mock_record
    mock_consumer.commit.assert_not_called()

  def test_ack_commits_tracked_record(self, mocker):
    """ack() commits the offset after the signal fires."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    backend._consumer = mock_consumer
    backend._last_record = mocker.MagicMock()

    backend.ack("testq")

    mock_consumer.commit.assert_called_once()
    assert backend._last_record is None

  def test_ack_is_idempotent(self, mocker):
    """Calling ack twice is safe — second call is a no-op (no tracked record)."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    backend._consumer = mock_consumer

    backend.ack("testq")
    backend.ack("testq")

    assert mock_consumer.commit.call_count == 0

  def test_ack_raises_on_commit_failure(self, mocker):
    """ack() wraps commit errors as QueueError."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    mock_consumer.commit.side_effect = KafkaError("commit failed")
    backend._consumer = mock_consumer
    backend._last_record = mocker.MagicMock()

    with pytest.raises(QueueError, match="ack"):
      backend.ack("testq")


class TestKafkaBackendQueueLen:
  """Tests for queue_len method.

  R3-G4: queue_len now reuses the existing consumer instead of creating a
  temporary one per call. Uses end_offsets - position for lag calculation.
  """

  def test_queue_len_returns_lag_from_consumer(self, mocker):
    """queue_len returns sum(end_offset - position) across assigned partitions."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    tp0 = TopicPartition("scrapy-testq", 0)
    tp1 = TopicPartition("scrapy-testq", 1)
    mock_consumer = mocker.MagicMock()
    mock_consumer.assignment.return_value = {tp0, tp1}
    mock_consumer.end_offsets.return_value = {tp0: 10, tp1: 5}
    mock_consumer.position.side_effect = lambda tp: {tp0: 3, tp1: 1}[tp]
    backend._consumer = mock_consumer

    result = backend.queue_len("testq")

    assert result == 11  # (10-3) + (5-1) = 11

  def test_queue_len_creates_temp_consumer_with_confluent_security_config(self, mocker):
    """queue_len temporary consumer reuses Confluent bootstrap and security settings."""
    config = KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      bootstrap_servers="fallback:9092",
      confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
      confluent_api_key="test_key",
      confluent_api_secret="test_secret",
      group_id="lag-checker",
    )
    backend = KafkaBackend(config)

    tp = TopicPartition("scrapy-testq", 0)
    mock_consumer = mocker.MagicMock()
    mock_consumer.partitions_for_topic.return_value = {0}
    mock_consumer.end_offsets.return_value = {tp: 8}
    mock_consumer.position.return_value = 3
    consumer_cls = mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      return_value=mock_consumer,
    )

    result = backend.queue_len("testq")

    assert result == 5
    assert consumer_cls.call_args.kwargs["bootstrap_servers"] == (
      "pkc-xxx.us-east-1.aws.confluent.cloud:9092"
    )
    assert consumer_cls.call_args.kwargs["security_protocol"] == "SASL_SSL"
    assert consumer_cls.call_args.kwargs["sasl_mechanism"] == "PLAIN"
    assert consumer_cls.call_args.kwargs["sasl_plain_username"] == "test_key"
    assert consumer_cls.call_args.kwargs["sasl_plain_password"] == "test_secret"
    mock_consumer.close.assert_called_once()

  def test_queue_len_returns_zero_when_no_consumer(self, mocker):
    """queue_len returns 0 before consumer is created (no pop called yet)."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    backend._consumer = None
    mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer")

    assert backend.queue_len("testq") == 0

  def test_queue_len_returns_zero_when_no_assignment(self, mocker):
    """queue_len returns 0 when consumer hasn't been assigned partitions yet."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    mock_consumer.assignment.return_value = set()
    backend._consumer = mock_consumer

    assert backend.queue_len("testq") == 0

  def test_queue_len_returns_zero_on_kafka_error(self, mocker):
    """queue_len returns 0 on KafkaError from end_offsets/position."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    mock_consumer.assignment.return_value = {TopicPartition("t", 0)}
    mock_consumer.end_offsets.side_effect = KafkaError("Broker unavailable")
    backend._consumer = mock_consumer

    assert backend.queue_len("testq") == 0


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


def test_kafka_sasl_password_repr_does_not_leak():
  """R2-B2: SASL password in producer config must be redacted in repr().

  Without _RedactedStr wrapping, ``repr(config)`` (e.g., in a Sentry
  traceback capturing locals) would show the raw password. The wrapper
  keeps the value usable as a str for kafka-python while hiding it from
  repr-based introspection.
  """
  from scrapy_extension.backends.kafka import _RedactedStr

  secret = _RedactedStr("hunter2-secret-password")
  assert str(secret) == "hunter2-secret-password"  # value intact for client lib
  assert "hunter2" not in repr(secret)
  assert "<redacted>" in repr(secret)


def test_kafka_build_common_config_redacts_sasl_password(mocker):
  """R2-B2: _build_common_config returns dict whose repr doesn't leak SASL password."""
  from scrapy_extension.backends.kafka import KafkaBackend
  from scrapy_extension.settings.kafka import KafkaSettings

  config = KafkaSettings(
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_username="alice",
    sasl_password="super-secret-pwd",
  )
  backend = KafkaBackend(config)
  built = backend._build_common_config()

  assert built["sasl_plain_username"] == "alice"
  # Value is usable as a normal string
  assert str(built["sasl_plain_password"]) == "super-secret-pwd"
  # But repr of the dict (the leak vector for Sentry / debug logs) hides it
  assert "super-secret-pwd" not in repr(built)
  assert "<redacted>" in repr(built)


def test_kafka_build_client_security_config_redacts_confluent_credentials():
  """E2: Confluent api_key/secret must be redacted in repr of the client config.

  SASL password is already wrapped in ``_RedactedStr``, but Confluent Cloud
  credentials (``confluent_api_key`` / ``confluent_api_secret``) are plumbed
  into ``sasl_plain_username`` / ``sasl_plain_password`` without redaction,
  so ``repr(config)`` and traceback dumps of locals leak them.
  """
  from scrapy_extension.backends.kafka import KafkaBackend
  from scrapy_extension.settings.kafka import KafkaSettings

  config = KafkaSettings(
    mode=KafkaMode.CONFLUENT,
    confluent_bootstrap_servers="pkc-xxx.confluent.cloud:9092",
    confluent_api_key="CKEY_TOP_SECRET_123",
    confluent_api_secret="CSECRET_TOP_SECRET_456",
  )
  backend = KafkaBackend(config)
  client_config = backend._build_client_security_config()

  # Values remain usable as normal strings for kafka-python.
  assert str(client_config["sasl_plain_username"]) == "CKEY_TOP_SECRET_123"
  assert str(client_config["sasl_plain_password"]) == "CSECRET_TOP_SECRET_456"
  # But repr of the config dict (Sentry / debug-log leak vector) hides both.
  assert "CKEY_TOP_SECRET_123" not in repr(client_config)
  assert "CSECRET_TOP_SECRET_456" not in repr(client_config)
  assert "<redacted>" in repr(client_config)



class TestKafkaBackendPopWithAckConcurrency:
  """Tier-2 Unit H: pop_with_ack + ack(token) correctness under CONCURRENT_REQUESTS>1.

  These tests prove no message is lost or skipped when N messages are popped
  before any is acked, and that acking out of order commits only the contiguous
  low-watermark (no unprocessed record ever skipped).
  """

  @staticmethod
  def _make_backend_with_records(mocker, records):
    """Build a KafkaBackend whose consumer.poll yields the given records in order.

    Each record is a MagicMock with .value/.partition/.offset/.topic set.
    """
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    mock_consumer.poll.side_effect = [
      {TopicPartition("scrapy-testq", r.partition): [r]} for r in records
    ] + [{}] * 5  # subsequent polls return empty
    mock_consumer.position.return_value = 0  # watermark base for partition 0
    backend._consumer = mock_consumer
    return backend, mock_consumer

  @staticmethod
  def _record(mocker, partition, offset, value=b"x", topic="scrapy-testq"):
    r = mocker.MagicMock()
    r.partition = partition
    r.offset = offset
    r.value = value
    r.topic = topic
    return r

  def test_concurrent_pops_return_distinct_tokens(self, mocker):
    """(a) N concurrent pop_with_ack calls return N distinct (partition, offset) tokens."""
    records = [
      self._record(mocker, 0, 0, b"a"),
      self._record(mocker, 0, 1, b"b"),
      self._record(mocker, 0, 2, b"c"),
    ]
    backend, _consumer = self._make_backend_with_records(mocker, records)

    tokens = []
    for _ in range(3):
      _value, token = backend.pop_with_ack("testq", timeout=0.0)
      tokens.append(token)

    # Three distinct tokens, each correlating to its specific offset.
    assert all(t is not None for t in tokens)
    assert len({(t.partition, t.offset) for t in tokens}) == 3
    assert [t.offset for t in tokens] == [0, 1, 2]

  def test_reverse_order_ack_commits_only_contiguous_watermark(self, mocker):
    """(b) ack offsets 0,1,2 in REVERSE order; watermark advances only contiguously.

    pop offsets 0,1,2 ; ack 2 then 1 → no commit yet (gap at 0);
    ack 0 → commit advances to watermark 3 (all three processed).
    """
    records = [
      self._record(mocker, 0, 0),
      self._record(mocker, 0, 1),
      self._record(mocker, 0, 2),
    ]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)

    _v0, t0 = backend.pop_with_ack("testq")
    _v1, t1 = backend.pop_with_ack("testq")
    _v2, t2 = backend.pop_with_ack("testq")

    # ack offset 2 → no contiguous run from base 0 (0,1 still in-flight)
    backend.ack("testq", token=t2)
    mock_consumer.commit.assert_not_called()

    # ack offset 1 → still gap at 0, no commit
    backend.ack("testq", token=t1)
    mock_consumer.commit.assert_not_called()

    # ack offset 0 → contiguous run complete, commit advances to 3
    backend.ack("testq", token=t0)
    mock_consumer.commit.assert_called_once()
    committed_map = mock_consumer.commit.call_args.args[0]
    tp, oam = next(iter(committed_map.items()))
    assert tp == TopicPartition("scrapy-testq", 0)
    assert oam.offset == 3

  def test_no_offset_skipped_under_concurrency(self, mocker):
    """(c) Interleaved pop/ack never skips an unprocessed offset.

    pop 0, pop 1, ack 1 (no commit — 0 unacked), pop 2, ack 0 → commit to 2.
    Offset 1 was acked out of order but the watermark only advances to 2
    (offset 2 still in-flight). Then ack 2 → commit to 3.
    """
    records = [
      self._record(mocker, 0, 0),
      self._record(mocker, 0, 1),
      self._record(mocker, 0, 2),
    ]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)

    _v0, t0 = backend.pop_with_ack("testq")
    _v1, t1 = backend.pop_with_ack("testq")
    backend.ack("testq", token=t1)  # ack 1 — gap at 0
    mock_consumer.commit.assert_not_called()
    _v2, t2 = backend.pop_with_ack("testq")
    backend.ack("testq", token=t0)  # ack 0 — contiguous 0,1 done → commit to 2

    committed_map = mock_consumer.commit.call_args.args[0]
    _tp, oam = next(iter(committed_map.items()))
    assert oam.offset == 2  # offset 2 not yet acked — NOT skipped

    backend.ack("testq", token=t2)  # ack 2 → commit to 3
    final_map = mock_consumer.commit.call_args.args[0]
    _tp2, oam2 = next(iter(final_map.items()))
    assert oam2.offset == 3

  def test_ack_token_none_legacy_fallback_commits_last_record(self, mocker):
    """(d) ack(token=None) legacy fallback commits the last-popped record wholesale."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    backend._consumer = mock_consumer
    backend._last_record = mocker.MagicMock()

    backend.ack("testq", token=None)  # legacy path

    mock_consumer.commit.assert_called_once_with()  # bare commit, no offset map

  def test_nack_does_not_commit_redeliver_semantics(self, mocker):
    """(e) nack(token) does NOT commit — offset stays uncommitted → re-delivered on restart."""
    records = [self._record(mocker, 0, 0)]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)
    _value, token = backend.pop_with_ack("testq")

    backend.nack("testq", token=token)

    mock_consumer.commit.assert_not_called()
    # The offset stays in-flight so the watermark can never advance past it.
    assert 0 in backend._in_flight[0]

  def test_ack_idempotent_on_duplicate_token(self, mocker):
    """Acking the same token twice does not double-commit or advance past the run."""
    records = [self._record(mocker, 0, 0), self._record(mocker, 0, 1)]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)
    _v0, t0 = backend.pop_with_ack("testq")
    _v1, t1 = backend.pop_with_ack("testq")

    backend.ack("testq", token=t0)  # commit to 1 (0 done, 1 in-flight)
    first_commit_count = mock_consumer.commit.call_count
    backend.ack("testq", token=t0)  # duplicate — no-op (already discarded)

    assert mock_consumer.commit.call_count == first_commit_count
