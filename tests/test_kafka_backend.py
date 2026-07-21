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

  def test_backend_rejects_auto_commit_with_explicit_ack_contract(self):
    """KafkaBackend requires application-level ack, so auto-commit is unsafe."""
    config = KafkaSettings(enable_auto_commit=True)

    with pytest.raises(ConfigurationError, match="enable_auto_commit") as exc_info:
      KafkaBackend(config)

    assert exc_info.value.setting_name == "enable_auto_commit"

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

  def test_connect_admin_client_failure_nulls_producer_no_wedge(self, mocker):
    """R-kacc: admin-client failure AFTER producer assigned must not wedge.

    ``KafkaAdminClient`` construction runs after ``self._producer`` is
    assigned in each ``_connect_*`` path. If admin construction raises, the
    producer must be closed and nulled so ``is_connected()`` reports False
    truthfully (no silent wedge) and the producer is not leaked under the
    ConnectionManager retry loop. Mirrors the R-mcc memcached connect-cleanup
    (PR #60).
    """
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_producer = mocker.MagicMock()
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaProducer",
      return_value=mock_producer,
    )
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaAdminClient",
      side_effect=KafkaError("admin init failed"),
    )

    with pytest.raises(BackendConnectionError):
      backend.connect()

    # No wedge: is_connected() must be truthful False, not lie True.
    assert backend.is_connected() is False
    assert backend._producer is None
    # No leak: the partially-assigned producer was closed before nulling.
    mock_producer.close.assert_called_once()

  def test_connect_rejects_mutated_unconfirmed_acks_before_sdk_io(self, mocker):
    config = KafkaSettings()
    backend = KafkaBackend(config)
    config.acks = 0
    producer = mocker.patch("scrapy_extension.backends.kafka.KafkaProducer")
    admin = mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient")

    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()

    assert exc_info.value.setting_name == "acks"
    producer.assert_not_called()
    admin.assert_not_called()


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

  def test_build_common_config_with_gssapi(self):
    """Ambient Kerberos auth still emits the selected SDK mechanism."""
    config = KafkaSettings(
      security_protocol="SASL_SSL",
      sasl_mechanism="GSSAPI",
    )
    backend = KafkaBackend(config)

    result = backend._build_common_config()

    assert result["security_protocol"] == "SASL_SSL"
    assert result["sasl_mechanism"] == "GSSAPI"
    assert "sasl_plain_username" not in result
    assert "sasl_plain_password" not in result

  def test_connect_revalidates_mutated_sasl_password_before_sdk_io(self, mocker):
    """A valid model cannot be downgraded after backend construction."""
    config = KafkaSettings(
      security_protocol="SASL_SSL",
      sasl_mechanism="PLAIN",
      sasl_username="user",
      sasl_password="secret",
    )
    backend = KafkaBackend(config)
    config.sasl_password = " "  # type: ignore[assignment]
    producer = mocker.patch("scrapy_extension.backends.kafka.KafkaProducer")
    admin = mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient")

    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()

    assert exc_info.value.setting_name == "sasl_password"
    producer.assert_not_called()
    admin.assert_not_called()

  def test_connect_revalidates_mutated_confluent_secret_before_sdk_io(
    self, mocker
  ):
    """A blank post-construction cloud secret cannot select PLAINTEXT."""
    config = KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      confluent_api_key="key",
      confluent_api_secret="secret",
    )
    backend = KafkaBackend(config)
    config.confluent_api_secret = ""  # type: ignore[assignment]
    producer = mocker.patch("scrapy_extension.backends.kafka.KafkaProducer")
    admin = mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient")

    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()

    assert exc_info.value.setting_name == "confluent_api_secret"
    producer.assert_not_called()
    admin.assert_not_called()

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
    """SASL config without Confluent API key uses STANDALONE mode (R9-b SV2).

    R9-b SV2: ``KafkaMode.CONFLUENT`` now requires ``confluent_api_key`` +
    ``confluent_api_secret`` (the silent PLAINTEXT-localhost fallback was a
    HIGH footgun). The legitimate "SASL against a custom broker" path is
    exercised under STANDALONE mode — the backend's ``_build_client_security_
    config`` applies SASL settings regardless of mode. This test was renamed
    in intent but kept in name to preserve coverage; it now pins the
    correct (SASL-via-STANDALONE) path.
    """
    config = KafkaSettings(
      mode=KafkaMode.STANDALONE,
      confluent_api_key=None,
      confluent_api_secret=None,
      bootstrap_servers="custom:9092",
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
    backend._admin_client.create_topics.return_value.topic_errors = [
      ("scrapy-test-queue", 0, None)
    ]

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

  def test_disconnect_releases_all_clients_when_one_close_fails(self, mocker):
    """A producer close failure must not leak the consumer or admin client."""
    backend = KafkaBackend(KafkaSettings())
    producer = mocker.MagicMock()
    consumer = mocker.MagicMock()
    admin = mocker.MagicMock()
    producer.close.side_effect = RuntimeError("producer close failed")
    backend._producer = producer
    backend._consumer = consumer
    backend._admin_client = admin

    backend.disconnect()

    producer.close.assert_called_once()
    consumer.close.assert_called_once()
    admin.close.assert_called_once()
    assert backend._producer is None
    assert backend._consumer is None
    assert backend._admin_client is None


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
    mock_admin.create_topics.return_value.topic_errors = [
      ("scrapy-newqueue", 0, None)
    ]
    backend._admin_client = mock_admin

    backend._ensure_topic_exists("newqueue")

    mock_admin.create_topics.assert_called_once()
    call_args = mock_admin.create_topics.call_args
    new_topic = call_args[0][0][0]
    assert isinstance(new_topic, NewTopic)
    assert new_topic.name == "scrapy-newqueue"
    assert "scrapy-newqueue" in backend._known_topics

  def test_ensure_topic_exists_applies_configured_durability(self, mocker):
    config = KafkaSettings(
      max_priority_partitions=7,
      num_partitions=7,
      replication_factor=3,
      min_insync_replicas=2,
      retention_ms=123456,
    )
    backend = KafkaBackend(config)
    admin = mocker.MagicMock()
    admin.create_topics.return_value.topic_errors = [("scrapy-durable", 0, None)]
    backend._admin_client = admin

    backend._ensure_topic_exists("durable")

    new_topic = admin.create_topics.call_args.args[0][0]
    assert new_topic.num_partitions == 7
    assert new_topic.replication_factor == 3
    assert new_topic.topic_configs == {
      "min.insync.replicas": "2",
      "retention.ms": "123456",
    }

  def test_known_topic_still_revalidates_mutated_policy(self, mocker):
    config = KafkaSettings()
    backend = KafkaBackend(config)
    backend._known_topics.add("scrapy-known")
    config.min_insync_replicas = 2
    admin = mocker.MagicMock()
    backend._admin_client = admin

    with pytest.raises(ConfigurationError) as exc_info:
      backend._ensure_topic_exists("known")

    assert exc_info.value.setting_name == "min_insync_replicas"
    admin.create_topics.assert_not_called()

  def test_known_topic_rechecks_valid_policy_change(self, mocker):
    config = KafkaSettings()
    backend = KafkaBackend(config)
    admin = mocker.MagicMock()
    admin.create_topics.return_value.topic_errors = [("scrapy-known", 0, None)]
    backend._admin_client = admin
    backend._ensure_topic_exists("known")

    config.retention_ms = 123
    admin.describe_topics.return_value = [
      {
        "error_code": 0,
        "topic": "scrapy-known",
        "partitions": [
          {"partition": partition, "replicas": [0]} for partition in range(10)
        ],
      }
    ]
    config_response = mocker.MagicMock()
    config_response.resources = [
      (
        0,
        None,
        2,
        "scrapy-known",
        [
          ("retention.ms", "123", False, False, False),
          ("min.insync.replicas", "1", False, False, False),
        ],
      )
    ]
    admin.describe_configs.return_value = [config_response]

    backend._ensure_topic_exists("known")

    admin.create_topics.assert_called_once()
    admin.describe_topics.assert_called_once_with(["scrapy-known"])

  def test_ensure_topic_exists_handles_topic_already_exists(self, mocker):
    """Test _ensure_topic_exists handles TopicAlreadyExistsError gracefully."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.create_topics.side_effect = TopicAlreadyExistsError(
      "Topic already exists"
    )
    backend._admin_client = mock_admin
    validate_policy = mocker.patch.object(
      backend, "_validate_existing_topic_policy"
    )

    backend._ensure_topic_exists("existingqueue")

    # Should still add to known topics
    assert "scrapy-existingqueue" in backend._known_topics
    validate_policy.assert_called_once()

  def test_ensure_topic_exists_surfaces_kafka_error_on_create(self, mocker):
    """A thrown admin failure must prevent a success-shaped push path."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    mock_admin.create_topics.side_effect = KafkaError("Create failed")
    backend._admin_client = mock_admin

    with pytest.raises(QueueError) as exc_info:
      backend._ensure_topic_exists("failedqueue")

    assert exc_info.value.operation == "push"
    assert "scrapy-failedqueue" not in backend._known_topics

  def test_ensure_topic_exists_rejects_per_topic_error_response(self, mocker):
    backend = KafkaBackend(KafkaSettings())
    admin = mocker.MagicMock()
    admin.create_topics.return_value.topic_errors = [
      ("scrapy-denied", 29, "authorization failed")
    ]
    backend._admin_client = admin

    with pytest.raises(QueueError) as exc_info:
      backend._ensure_topic_exists("denied")

    assert exc_info.value.operation == "push"
    assert "scrapy-denied" not in backend._known_topics

  def test_ensure_topic_exists_accepts_already_exists_response(self, mocker):
    backend = KafkaBackend(KafkaSettings())
    admin = mocker.MagicMock()
    admin.create_topics.return_value.topic_errors = [
      ("scrapy-existing", 36, "already exists")
    ]
    backend._admin_client = admin
    validate_policy = mocker.patch.object(
      backend, "_validate_existing_topic_policy"
    )

    backend._ensure_topic_exists("existing")

    assert "scrapy-existing" in backend._known_topics
    validate_policy.assert_called_once()

  def test_existing_topic_matching_policy_is_accepted(self, mocker):
    backend = KafkaBackend(KafkaSettings())
    admin = mocker.MagicMock()
    admin.create_topics.return_value.topic_errors = [
      ("scrapy-existing", 36, "already exists")
    ]
    admin.describe_topics.return_value = [
      {
        "error_code": 0,
        "topic": "scrapy-existing",
        "partitions": [
          {"partition": partition, "replicas": [0]} for partition in range(10)
        ],
      }
    ]
    config_response = mocker.MagicMock()
    config_response.resources = [
      (
        0,
        None,
        2,
        "scrapy-existing",
        [
          ("retention.ms", "604800000", False, False, False),
          ("min.insync.replicas", "1", False, False, False),
        ],
      )
    ]
    admin.describe_configs.return_value = [config_response]
    backend._admin_client = admin

    backend._ensure_topic_exists("existing")

    assert "scrapy-existing" in backend._known_topics
    admin.describe_topics.assert_called_once_with(["scrapy-existing"])
    admin.describe_configs.assert_called_once()

  def test_existing_topic_policy_mismatch_fails_before_cache(self, mocker):
    backend = KafkaBackend(KafkaSettings())
    admin = mocker.MagicMock()
    admin.create_topics.return_value.topic_errors = [
      ("scrapy-drifted", 36, "already exists")
    ]
    admin.describe_topics.return_value = [
      {
        "error_code": 0,
        "topic": "scrapy-drifted",
        "partitions": [
          {"partition": partition, "replicas": [0]} for partition in range(10)
        ],
      }
    ]
    config_response = mocker.MagicMock()
    config_response.resources = [
      (
        0,
        None,
        2,
        "scrapy-drifted",
        [
          ("retention.ms", "604800000", False, False, False),
          ("min.insync.replicas", "2", False, False, False),
        ],
      )
    ]
    admin.describe_configs.return_value = [config_response]
    backend._admin_client = admin

    with pytest.raises(QueueError, match="policy"):
      backend._ensure_topic_exists("drifted")

    assert "scrapy-drifted" not in backend._known_topics

  def test_ensure_topic_exists_rejects_malformed_response(self, mocker):
    backend = KafkaBackend(KafkaSettings())
    admin = mocker.MagicMock()
    admin.create_topics.return_value.topic_errors = []
    backend._admin_client = admin

    with pytest.raises(QueueError, match="response"):
      backend._ensure_topic_exists("missing")

    assert "scrapy-missing" not in backend._known_topics


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
    backend._known_topics.add("scrapy-testq")

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
    backend._known_topics.add("scrapy-testq")

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
    backend._known_topics.add("scrapy-testq")

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
    mock_consumer.subscribe.assert_any_call(
      ["scrapy-queue_a"], listener=backend._rebalance_listener
    )
    mock_consumer.subscribe.assert_any_call(
      ["scrapy-queue_b"], listener=backend._rebalance_listener
    )

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

  def test_legacy_ack_commit_failure_keeps_record_retryable(self, mocker):
    """A failed legacy commit must not turn a retry into an idempotent no-op."""
    backend = KafkaBackend(KafkaSettings())
    consumer = mocker.MagicMock()
    consumer.commit.side_effect = [KafkaError("commit failed"), None]
    record = mocker.MagicMock()
    backend._consumer = consumer
    backend._last_record = record

    with pytest.raises(QueueError, match="ack"):
      backend.ack("testq")

    assert backend._last_record is record
    backend.ack("testq")
    assert consumer.commit.call_count == 2
    assert backend._last_record is None

  def test_ack_with_foreign_token_is_idempotent_noop(self, mocker):
    """The public Any token boundary rejects foreign token shapes safely."""
    backend = KafkaBackend(KafkaSettings())
    backend._consumer = mocker.MagicMock()

    backend.ack("testq", token=object())

    backend._consumer.commit.assert_not_called()


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

  def test_queue_len_filters_foreign_topics_from_current_assignment(self, mocker):
    """A rebalance overlap must not add another topic's lag to this queue."""
    backend = KafkaBackend(KafkaSettings())
    requested = TopicPartition("scrapy-testq", 0)
    foreign = TopicPartition("scrapy-other", 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {requested, foreign}
    consumer.end_offsets.return_value = {requested: 10}
    consumer.position.return_value = 4
    backend._consumer = consumer

    assert backend.queue_len("testq") == 6
    consumer.end_offsets.assert_called_once_with({requested})
    consumer.position.assert_called_once_with(requested)

  def test_queue_len_uses_requested_topic_when_consumer_is_on_other_topic(
    self, mocker
  ):
    """queue_len(B) must not report the currently subscribed topic A's lag."""
    backend = KafkaBackend(KafkaSettings(group_id="lag-checker"))
    current = TopicPartition("scrapy-current", 0)
    requested = TopicPartition("scrapy-requested", 0)
    active_consumer = mocker.MagicMock()
    active_consumer.assignment.return_value = {current}
    backend._consumer = active_consumer

    temp_consumer = mocker.MagicMock()
    temp_consumer.partitions_for_topic.return_value = {0}
    temp_consumer.end_offsets.return_value = {requested: 12}
    temp_consumer.position.return_value = 5
    consumer_cls = mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      return_value=temp_consumer,
    )

    assert backend.queue_len("requested") == 7
    active_consumer.end_offsets.assert_not_called()
    consumer_cls.assert_called_once()
    temp_consumer.close.assert_called_once()

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
    """A rebalance gap must fall back to a metadata consumer, not fake empty."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    active_consumer = mocker.MagicMock()
    active_consumer.assignment.return_value = set()
    backend._consumer = active_consumer
    tp = TopicPartition("scrapy-testq", 0)
    probe_consumer = mocker.MagicMock()
    probe_consumer.partitions_for_topic.return_value = {0}
    probe_consumer.end_offsets.return_value = {tp: 12}
    probe_consumer.position.return_value = 5
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      return_value=probe_consumer,
    )

    assert backend.queue_len("testq") == 7
    probe_consumer.close.assert_called_once()

  def test_queue_len_raises_on_kafka_error(self, mocker):
    """R-kqlen: queue_len must raise QueueError on KafkaError (not swallow to 0).

    A broker failure during end_offsets/position must NOT look like an empty
    queue — otherwise the scheduler mistakes outage for idle and drops the
    backpressure signal (parity with R-sqs-qlen #62 / R-es-qlen #65 / redis).
    """
    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    mock_consumer.assignment.return_value = {TopicPartition("scrapy-testq", 0)}
    mock_consumer.end_offsets.side_effect = KafkaError("Broker unavailable")
    backend._consumer = mock_consumer

    with pytest.raises(QueueError) as exc_info:
      backend.queue_len("testq")
    assert exc_info.value.queue_name == "testq"
    assert exc_info.value.operation == "queue_len"

  def test_queue_len_raises_on_kafka_error_temp_consumer(self, mocker):
    """R-kqlen: temp-consumer branch must also raise (and still close it)."""
    config = KafkaSettings()
    backend = KafkaBackend(config)
    backend._consumer = None  # force the temp-consumer branch

    mock_temp_consumer = mocker.MagicMock()
    mock_temp_consumer.partitions_for_topic.return_value = {0}
    mock_temp_consumer.end_offsets.side_effect = KafkaError("Broker unavailable")
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      return_value=mock_temp_consumer,
    )

    with pytest.raises(QueueError) as exc_info:
      backend.queue_len("testq")
    assert exc_info.value.operation == "queue_len"
    # finally arm must still close the temp consumer (no leak).
    mock_temp_consumer.close.assert_called_once()


class TestKafkaBackendClearQueue:
  """Tests for clear_queue method."""

  def test_clear_queue_is_explicitly_unsupported_without_admin_io(self, mocker):
    """Delete/recreate cannot satisfy a linearizable distributed clear."""
    config = KafkaSettings()
    backend = KafkaBackend(config)

    mock_admin = mocker.MagicMock()
    backend._admin_client = mock_admin

    with pytest.raises(NotImplementedError, match="Kafka"):
      backend.clear_queue("testq")

    mock_admin.delete_topics.assert_not_called()
    mock_admin.create_topics.assert_not_called()

  def test_clear_queue_still_validates_name_before_unsupported(self):
    backend = KafkaBackend(KafkaSettings())

    with pytest.raises(ValueError, match="Invalid topic"):
      backend.clear_queue("bad queue")


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
  mock_admin_client_instance.create_topics.return_value.topic_errors = [
    ("scrapy-test_queue", 0, None)
  ]
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
      {TopicPartition(r.topic, r.partition): [r]} for r in records
    ] + [{}] * 5  # subsequent polls return empty
    # Kafka position() is the NEXT offset to fetch, not the first in-flight
    # record. The backend must seed its ack watermark from records as they are
    # popped; using this realistic position would skip/no-op every pending ack.
    mock_consumer.position.return_value = max(r.offset for r in records) + 1
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
    mock_consumer.position.assert_not_called()

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
    """(e) nack(token) rewinds an assigned partition for in-session retry."""
    records = [self._record(mocker, 0, 0)]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)
    topic_partition = TopicPartition("scrapy-testq", 0)
    mock_consumer.assignment.return_value = {topic_partition}
    _value, token = backend.pop_with_ack("testq")

    backend.nack("testq", token=token)

    mock_consumer.commit.assert_not_called()
    mock_consumer.seek.assert_called_once_with(topic_partition, 0)
    # The offset stays in-flight so the watermark can never advance past it.
    assert 0 in backend._in_flight[("scrapy-testq", 0)]

  def test_nack_keeps_offset_uncommitted_when_partition_is_not_assigned(self, mocker):
    """A revoked partition cannot be sought; restart redelivery remains the fallback."""
    records = [self._record(mocker, 0, 7)]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)
    mock_consumer.assignment.return_value = set()
    _value, token = backend.pop_with_ack("testq")

    backend.nack("testq", token=token)

    mock_consumer.seek.assert_not_called()
    assert 7 in backend._in_flight[("scrapy-testq", 0)]

  def test_nack_seek_failure_is_retryable(self, mocker):
    """A broker/client seek failure surfaces while preserving in-flight state."""
    records = [self._record(mocker, 0, 7)]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)
    topic_partition = TopicPartition("scrapy-testq", 0)
    mock_consumer.assignment.return_value = {topic_partition}
    mock_consumer.seek.side_effect = KafkaError("seek failed")
    _value, token = backend.pop_with_ack("testq")

    with pytest.raises(QueueError, match="Failed to nack Kafka message"):
      backend.nack("testq", token=token)

    assert 7 in backend._in_flight[("scrapy-testq", 0)]

  def test_stale_token_cannot_ack_redelivery_after_reconnect(self, mocker):
    """An old consumer generation cannot commit a same-offset redelivery."""
    old_record = self._record(mocker, 0, 0)
    new_record = self._record(mocker, 0, 0)
    old_consumer = mocker.MagicMock()
    old_consumer.poll.return_value = {
      TopicPartition("scrapy-testq", 0): [old_record]
    }
    new_consumer = mocker.MagicMock()
    new_consumer.poll.return_value = {
      TopicPartition("scrapy-testq", 0): [new_record]
    }
    mocker.patch(
      "scrapy_extension.backends.kafka.KafkaConsumer",
      side_effect=[old_consumer, new_consumer],
    )
    backend = KafkaBackend(KafkaSettings())

    _old_value, old_token = backend.pop_with_ack("testq")
    backend.disconnect()
    _new_value, new_token = backend.pop_with_ack("testq")

    backend.ack("testq", token=old_token)

    new_consumer.commit.assert_not_called()
    assert old_token != new_token
    assert 0 in backend._in_flight[("scrapy-testq", 0)]

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

  def test_subscription_switch_fences_prior_topic_token(self, mocker):
    """A single consumer cannot settle a token from its revoked topic."""
    records = [
      self._record(mocker, 0, 0, topic="scrapy-first"),
      self._record(mocker, 0, 0, topic="scrapy-second"),
    ]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)

    _first_value, first_token = backend.pop_with_ack("first")
    _second_value, second_token = backend.pop_with_ack("second")

    backend.ack("first", token=first_token)
    backend.ack("second", token=second_token)

    assert mock_consumer.commit.call_count == 1
    committed_partitions = [
      next(iter(call.args[0])) for call in mock_consumer.commit.call_args_list
    ]
    assert committed_partitions == [TopicPartition("scrapy-second", 0)]
    assert [
      next(iter(call.args[0].values())).offset
      for call in mock_consumer.commit.call_args_list
    ] == [1]

  def test_ack_retries_same_token_after_commit_failure(self, mocker):
    """A failed broker commit must leave the token retryable."""
    records = [self._record(mocker, 0, 0)]
    backend, mock_consumer = self._make_backend_with_records(mocker, records)
    mock_consumer.commit.side_effect = [KafkaError("commit failed"), None]
    _value, token = backend.pop_with_ack("testq")

    with pytest.raises(QueueError, match="Failed to ack Kafka message"):
      backend.ack("testq", token=token)

    backend.ack("testq", token=token)
    backend.ack("testq", token=token)

    assert mock_consumer.commit.call_count == 2
    first_commit = mock_consumer.commit.call_args_list[0].args[0]
    retry_commit = mock_consumer.commit.call_args_list[1].args[0]
    assert first_commit == retry_commit
    topic_partition = ("scrapy-testq", 0)
    assert topic_partition not in backend._in_flight
    assert topic_partition not in backend._watermarks
    assert topic_partition not in backend._high_water


# ---------------------------------------------------------------------------
# SEC-1 (round-6): shared _redaction helper parity.
# ---------------------------------------------------------------------------


def test_redaction_module_is_shared_helper():
  """SEC-1: _RedactedStr is now defined once in backends/_redaction.py and
  re-imported by kafka. The kafka module re-exports it for backward compat.
  """
  from scrapy_extension.backends._redaction import _RedactedStr as SharedRedacted
  from scrapy_extension.backends.kafka import _RedactedStr as KafkaRedacted

  assert SharedRedacted is KafkaRedacted  # same class object (re-exported)
  # str-subclass semantics preserved: client libs get the real value.
  s = SharedRedacted("hunter2")
  assert str(s) == "hunter2"
  assert s == "hunter2"  # equality works
  assert "hunter2" not in repr(s)
  assert repr(s) == "<redacted>"


# ===========================================================================
# R14-E — Lifecycle bounds: Kafka partition-dict pruning
# ===========================================================================


class TestKafkaBackendPartitionPruning:
  """R14-E MED: ack bookkeeping prunes per-topic-partition keys.

  These dicts grow one key per topic-partition ever popped; without pruning,
  topic or partition churn grows them unbounded. When a topic-partition's
  in-flight set empties (its watermark has caught up to the popped frontier),
  its keys are stale and safe to drop. A fresh pop on the same topic-partition
  re-seeds them lazily.
  """

  @staticmethod
  def _make_backend(mocker):
    from scrapy_extension.settings import KafkaSettings

    config = KafkaSettings()
    backend = KafkaBackend(config)
    mock_consumer = mocker.MagicMock()
    mock_consumer.position.return_value = 0
    backend._consumer = mock_consumer
    return backend, mock_consumer

  def test_prunes_empty_partition_keys_on_ack(self, mocker):
    """When the last in-flight offset for a partition is acked, its keys are pruned."""
    backend, mock_consumer = self._make_backend(mocker)
    topic_partition = ("scrapy-testq", 5)
    # Simulate one pop on partition 5 (a non-default partition to prove the
    # key is genuinely removed, not just the default 0).
    backend._in_flight[topic_partition].add(100)
    backend._high_water[topic_partition] = 101
    backend._watermarks[topic_partition] = 100  # base == popped offset

    from scrapy_extension.backends.kafka import _KafkaAckToken

    token = _KafkaAckToken(partition=5, offset=100, topic="scrapy-testq")
    backend.ack("testq", token=token)

    # The watermark advanced to 101 (high_water), so commit fired.
    mock_consumer.commit.assert_called_once()
    # All three per-topic-partition keys are now pruned.
    assert topic_partition not in backend._in_flight, (
      "topic-partition not pruned after drain — partition-churn leak"
    )
    assert topic_partition not in backend._watermarks
    assert topic_partition not in backend._high_water

  def test_keeps_keys_when_partition_still_has_in_flight(self, mocker):
    """If a partition still has unacked offsets, its keys are retained."""
    backend, _mock_consumer = self._make_backend(mocker)
    topic_partition = ("scrapy-testq", 5)
    backend._in_flight[topic_partition].update({100, 101})
    backend._high_water[topic_partition] = 102
    backend._watermarks[topic_partition] = 100

    from scrapy_extension.backends.kafka import _KafkaAckToken

    token = _KafkaAckToken(partition=5, offset=100, topic="scrapy-testq")
    backend.ack("testq", token=token)

    # Partition 5 still has offset 101 in-flight → keys retained.
    assert topic_partition in backend._in_flight
    assert 101 in backend._in_flight[topic_partition]
    assert topic_partition in backend._watermarks
    assert topic_partition in backend._high_water

  def test_multiple_partitions_prune_independently(self, mocker):
    """Partition churn across many partitions prunes each independently."""
    from scrapy_extension.backends.kafka import _KafkaAckToken

    backend, _mock_consumer = self._make_backend(mocker)
    # Seed 8 partitions, each with one in-flight offset, all at the same base.
    for p in range(8):
      topic_partition = ("scrapy-testq", p)
      backend._in_flight[topic_partition].add(10)
      backend._high_water[topic_partition] = 11
      backend._watermarks[topic_partition] = 10

    # Ack each — each partition drains and prunes.
    for p in range(8):
      token = _KafkaAckToken(partition=p, offset=10, topic="scrapy-testq")
      backend.ack("testq", token=token)

    # All 8 partitions pruned — no unbounded growth under partition churn.
    assert len(backend._in_flight) == 0
    assert len(backend._watermarks) == 0
    assert len(backend._high_water) == 0
