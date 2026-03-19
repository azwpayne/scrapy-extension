"""Tests for multi-mode backend implementations.

This module tests the multi-mode support for Kafka, MongoDB, and RabbitMQ backends.
"""

from unittest.mock import MagicMock, patch

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.exceptions import ConfigurationError


class TestKafkaMode:
  """Test KafkaMode enum."""

  def test_standalone_value(self):
    from scrapy_extension.config.settings import KafkaMode

    assert KafkaMode.STANDALONE.value == "standalone"

  def test_cluster_value(self):
    from scrapy_extension.config.settings import KafkaMode

    assert KafkaMode.CLUSTER.value == "cluster"

  def test_confluent_value(self):
    from scrapy_extension.config.settings import KafkaMode

    assert KafkaMode.CONFLUENT.value == "confluent"


class TestMongoDBMode:
  """Test MongoDBMode enum."""

  def test_standalone_value(self):
    from scrapy_extension.config.settings import MongoDBMode

    assert MongoDBMode.STANDALONE.value == "standalone"

  def test_replica_set_value(self):
    from scrapy_extension.config.settings import MongoDBMode

    assert MongoDBMode.REPLICA_SET.value == "replica_set"

  def test_sharded_cluster_value(self):
    from scrapy_extension.config.settings import MongoDBMode

    assert MongoDBMode.SHARDED_CLUSTER.value == "sharded_cluster"

  def test_atlas_value(self):
    from scrapy_extension.config.settings import MongoDBMode

    assert MongoDBMode.ATLAS.value == "atlas"


class TestRabbitMQMode:
  """Test RabbitMQMode enum."""

  def test_standalone_value(self):
    from scrapy_extension.config.settings import RabbitMQMode

    assert RabbitMQMode.STANDALONE.value == "standalone"

  def test_cluster_value(self):
    from scrapy_extension.config.settings import RabbitMQMode

    assert RabbitMQMode.CLUSTER.value == "cluster"

  def test_mirrored_queues_value(self):
    from scrapy_extension.config.settings import RabbitMQMode

    assert RabbitMQMode.MIRRORED_QUEUES.value == "mirrored_queues"


class TestKafkaBackendModes:
  """Test KafkaBackend with different deployment modes."""

  @pytest.fixture
  def mock_kafka_producer(self):
    """Create mock Kafka producer."""
    return MagicMock()

  @pytest.fixture
  def mock_kafka_admin(self):
    """Create mock Kafka admin client."""
    return MagicMock()

  def test_standalone_mode_default(self, mock_kafka_producer, mock_kafka_admin):
    """Test standalone mode is default."""
    from scrapy_extension.backends.kafka_backend import KafkaBackend
    from scrapy_extension.config.settings import KafkaMode, KafkaSettings

    settings = KafkaSettings(bootstrap_servers="localhost:9092")
    assert settings.mode == KafkaMode.STANDALONE

    with (
      patch(
        "scrapy_extension.backends.kafka_backend.KafkaProducer",
        return_value=mock_kafka_producer,
      ),
      patch(
        "scrapy_extension.backends.kafka_backend.KafkaAdminClient",
        return_value=mock_kafka_admin,
      ),
    ):
      backend = KafkaBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_cluster_mode_success(self, mock_kafka_producer, mock_kafka_admin):
    """Test cluster mode connection."""
    from scrapy_extension.backends.kafka_backend import KafkaBackend
    from scrapy_extension.config.settings import KafkaMode, KafkaSettings

    settings = KafkaSettings(
      mode=KafkaMode.CLUSTER,
      bootstrap_servers="broker1:9092",
      cluster_brokers=["broker1:9092", "broker2:9092", "broker3:9092"],
    )

    with (
      patch(
        "scrapy_extension.backends.kafka_backend.KafkaProducer",
        return_value=mock_kafka_producer,
      ) as mock_producer_class,
      patch(
        "scrapy_extension.backends.kafka_backend.KafkaAdminClient",
        return_value=mock_kafka_admin,
      ),
    ):
      backend = KafkaBackend(settings)
      backend.connect()
      assert backend.is_connected()
      # Verify cluster brokers were used
      call_kwargs = mock_producer_class.call_args.kwargs
      assert "broker1:9092" in call_kwargs["bootstrap_servers"]
      assert "broker2:9092" in call_kwargs["bootstrap_servers"]

  def test_confluent_mode_success(self, mock_kafka_producer, mock_kafka_admin):
    """Test Confluent Cloud mode connection."""
    from scrapy_extension.backends.kafka_backend import KafkaBackend
    from scrapy_extension.config.settings import KafkaMode, KafkaSettings

    settings = KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
      confluent_api_key="test_key",
      confluent_api_secret="test_secret",
    )

    with (
      patch(
        "scrapy_extension.backends.kafka_backend.KafkaProducer",
        return_value=mock_kafka_producer,
      ) as mock_producer_class,
      patch(
        "scrapy_extension.backends.kafka_backend.KafkaAdminClient",
        return_value=mock_kafka_admin,
      ),
    ):
      backend = KafkaBackend(settings)
      backend.connect()
      assert backend.is_connected()
      # Verify SASL_SSL configuration
      call_kwargs = mock_producer_class.call_args.kwargs
      assert call_kwargs["security_protocol"] == "SASL_SSL"
      assert call_kwargs["sasl_mechanism"] == "PLAIN"
      assert call_kwargs["sasl_plain_username"] == "test_key"
      assert call_kwargs["sasl_plain_password"] == "test_secret"  # noqa: S105

  def test_confluent_mode_fallback_to_sasl(self, mock_kafka_producer, mock_kafka_admin):
    """Test Confluent mode falls back to configured SASL if no API key."""
    from scrapy_extension.backends.kafka_backend import KafkaBackend
    from scrapy_extension.config.settings import KafkaMode, KafkaSettings

    settings = KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      bootstrap_servers="kafka.example.com:9092",
      security_protocol="SASL_SSL",
      sasl_mechanism="PLAIN",
      sasl_username="user",
      sasl_password="pass",
    )

    with (
      patch(
        "scrapy_extension.backends.kafka_backend.KafkaProducer",
        return_value=mock_kafka_producer,
      ),
      patch(
        "scrapy_extension.backends.kafka_backend.KafkaAdminClient",
        return_value=mock_kafka_admin,
      ),
    ):
      backend = KafkaBackend(settings)
      backend.connect()
      assert backend.is_connected()


class TestMongoDBBackendModes:
  """Test MongoDBBackend with different deployment modes."""

  @pytest.fixture
  def mock_mongo_client(self):
    """Create mock MongoDB client."""
    mock = MagicMock()
    mock.admin.command.return_value = {"ok": 1}
    return mock

  def test_standalone_mode_default(self, mock_mongo_client):
    """Test standalone mode is default."""
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBMode, MongoDBSettings

    settings = MongoDBSettings(uri="mongodb://localhost:27017")
    assert settings.mode == MongoDBMode.STANDALONE

    with patch(
      "scrapy_extension.backends.mongodb_backend.MongoClient",
      return_value=mock_mongo_client,
    ):
      backend = MongoDBBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_replica_set_mode_success(self, mock_mongo_client):
    """Test replica set mode connection."""
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBMode, MongoDBSettings

    settings = MongoDBSettings(
      mode=MongoDBMode.REPLICA_SET,
      database="testdb",
      replica_set_name="myReplicaSet",
      replica_set_members=["host1:27017", "host2:27017", "host3:27017"],
    )

    with patch(
      "scrapy_extension.backends.mongodb_backend.MongoClient",
      return_value=mock_mongo_client,
    ) as mock_client_class:
      backend = MongoDBBackend(settings)
      backend.connect()
      assert backend.is_connected()
      # Verify replicaSet was passed
      call_kwargs = mock_client_class.call_args.kwargs
      assert call_kwargs["replicaSet"] == "myReplicaSet"

  def test_sharded_cluster_mode_success(self, mock_mongo_client):
    """Test sharded cluster mode connection."""
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBMode, MongoDBSettings

    settings = MongoDBSettings(
      mode=MongoDBMode.SHARDED_CLUSTER,
      database="testdb",
      mongos_routers=["router1:27017", "router2:27017"],
    )

    with patch(
      "scrapy_extension.backends.mongodb_backend.MongoClient",
      return_value=mock_mongo_client,
    ) as mock_client_class:
      backend = MongoDBBackend(settings)
      backend.connect()
      assert backend.is_connected()
      # Verify connection string uses mongos routers
      call_args = mock_client_class.call_args
      assert "router1:27017" in call_args[0][0]
      assert "router2:27017" in call_args[0][0]

  def test_atlas_mode_success(self, mock_mongo_client):
    """Test Atlas mode connection."""
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBMode, MongoDBSettings

    settings = MongoDBSettings(
      mode=MongoDBMode.ATLAS,
      uri="mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/mydb?retryWrites=true&w=majority",
    )

    with patch(
      "scrapy_extension.backends.mongodb_backend.MongoClient",
      return_value=mock_mongo_client,
    ) as mock_client_class:
      backend = MongoDBBackend(settings)
      backend.connect()
      assert backend.is_connected()
      # Verify TLS is enabled for Atlas
      call_kwargs = mock_client_class.call_args.kwargs
      assert call_kwargs["tls"] is True

  def test_replica_set_mode_with_tls(self, mock_mongo_client):
    """Test replica set mode with TLS configuration."""
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBMode, MongoDBSettings

    settings = MongoDBSettings(
      mode=MongoDBMode.REPLICA_SET,
      database="testdb",
      replica_set_name="myReplicaSet",
      replica_set_members=["host1:27017", "host2:27017"],
      tls_enabled=True,
      tls_ca_file="/path/to/ca.pem",
      tls_cert_file="/path/to/cert.pem",
    )

    with patch(
      "scrapy_extension.backends.mongodb_backend.MongoClient",
      return_value=mock_mongo_client,
    ) as mock_client_class:
      backend = MongoDBBackend(settings)
      backend.connect()
      call_kwargs = mock_client_class.call_args.kwargs
      assert call_kwargs["tls"] is True
      assert call_kwargs["tlsCAFile"] == "/path/to/ca.pem"
      assert call_kwargs["tlsCertificateKeyFile"] == "/path/to/cert.pem"


class TestRabbitMQBackendModes:
  """Test RabbitMQBackend with different deployment modes."""

  @pytest.fixture
  def mock_pika_connection(self):
    """Create mock pika connection."""
    mock_conn = MagicMock()
    mock_channel = MagicMock()
    mock_conn.channel.return_value = mock_channel
    mock_conn.is_open = True
    return mock_conn, mock_channel

  def test_standalone_mode_default(self, mock_pika_connection):
    """Test standalone mode is default."""
    from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
    from scrapy_extension.config.settings import RabbitMQMode, RabbitMQSettings

    settings = RabbitMQSettings(host="localhost", port=5672)
    assert settings.mode == RabbitMQMode.STANDALONE

    mock_conn, _ = mock_pika_connection
    with patch("pika.BlockingConnection", return_value=mock_conn):
      backend = RabbitMQBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_cluster_mode_success(self, mock_pika_connection):
    """Test cluster mode connection."""
    from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
    from scrapy_extension.config.settings import RabbitMQMode, RabbitMQSettings

    settings = RabbitMQSettings(
      mode=RabbitMQMode.CLUSTER,
      host="node1",
      port=5672,
      cluster_nodes=["node2:5672", "node3:5672"],
    )

    mock_conn, _ = mock_pika_connection
    with patch("pika.BlockingConnection", return_value=mock_conn):
      backend = RabbitMQBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_mirrored_queues_mode_success(self, mock_pika_connection):
    """Test mirrored queues mode connection."""
    from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
    from scrapy_extension.config.settings import RabbitMQMode, RabbitMQSettings

    settings = RabbitMQSettings(
      mode=RabbitMQMode.MIRRORED_QUEUES,
      host="node1",
      port=5672,
      ha_mode="all",
      ha_sync_mode="automatic",
    )

    mock_conn, mock_channel = mock_pika_connection
    with patch("pika.BlockingConnection", return_value=mock_conn):
      backend = RabbitMQBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_mirrored_queues_mode_with_ha_params(self, mock_pika_connection):
    """Test mirrored queues mode with HA params."""
    from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
    from scrapy_extension.config.settings import RabbitMQMode, RabbitMQSettings

    settings = RabbitMQSettings(
      mode=RabbitMQMode.MIRRORED_QUEUES,
      host="node1",
      port=5672,
      ha_mode="exactly",
      ha_params="2",
      ha_sync_mode="manual",
    )

    mock_conn, mock_channel = mock_pika_connection
    with patch("pika.BlockingConnection", return_value=mock_conn):
      backend = RabbitMQBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_ssl_configuration(self, mock_pika_connection):
    """Test SSL/TLS configuration."""
    import ssl as ssl_module

    from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
    from scrapy_extension.config.settings import RabbitMQSettings

    settings = RabbitMQSettings(
      host="localhost",
      port=5671,
      ssl_enabled=True,
      ssl_cafile="/path/to/ca.pem",
      ssl_certfile="/path/to/cert.pem",
      ssl_keyfile="/path/to/key.pem",
    )

    mock_conn, _ = mock_pika_connection
    mock_ssl_context = MagicMock(spec=ssl_module.SSLContext)

    # Need to patch both ConnectionParameters and SSLOptions since pika validates types
    with (
      patch("pika.BlockingConnection", return_value=mock_conn),
      patch.object(ssl_module, "create_default_context", return_value=mock_ssl_context),
      patch(
        "scrapy_extension.backends.rabbitmq_backend.pika.SSLOptions"
      ) as mock_ssl_opts_class,
      patch(
        "scrapy_extension.backends.rabbitmq_backend.pika.ConnectionParameters"
      ) as mock_params_class,
    ):
      mock_params_instance = MagicMock()
      mock_params_class.return_value = mock_params_instance
      mock_ssl_options_instance = MagicMock()
      mock_ssl_opts_class.return_value = mock_ssl_options_instance

      backend = RabbitMQBackend(settings)
      backend.connect()

      # Verify SSL context was created
      ssl_module.create_default_context.assert_called_once_with(
        cafile="/path/to/ca.pem"
      )
      # Verify cert chain was loaded
      mock_ssl_context.load_cert_chain.assert_called_once_with(
        certfile="/path/to/cert.pem",
        keyfile="/path/to/key.pem",
      )
      # Verify SSLOptions was created with the context
      mock_ssl_opts_class.assert_called_once_with(mock_ssl_context)
      # Verify ConnectionParameters was called with SSL options
      call_kwargs = mock_params_class.call_args.kwargs
      assert call_kwargs["ssl_options"] == mock_ssl_options_instance


class TestBackendTypeDetection:
  """Test backend type property for all backends."""

  def test_redis_backend_type(self):
    from scrapy_extension.backends.redis_backend import RedisBackend
    from scrapy_extension.config.settings import RedisSettings

    settings = RedisSettings()
    backend = RedisBackend(settings)
    assert backend.backend_type == BackendType.REDIS

  def test_mongodb_backend_type(self):
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBSettings

    settings = MongoDBSettings()
    backend = MongoDBBackend(settings)
    assert backend.backend_type == BackendType.MONGODB

  def test_kafka_backend_type(self):
    from scrapy_extension.backends.kafka_backend import KafkaBackend
    from scrapy_extension.config.settings import KafkaSettings

    settings = KafkaSettings()
    backend = KafkaBackend(settings)
    assert backend.backend_type == BackendType.KAFKA

  def test_rabbitmq_backend_type(self):
    from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
    from scrapy_extension.config.settings import RabbitMQSettings

    settings = RabbitMQSettings()
    backend = RabbitMQBackend(settings)
    assert backend.backend_type == BackendType.RABBITMQ


class TestModeConfigurationErrors:
  """Test error handling for invalid mode configurations."""

  def test_kafka_unsupported_mode(self):
    """Test Kafka backend with unsupported mode."""
    from scrapy_extension.backends.kafka_backend import KafkaBackend
    from scrapy_extension.config.settings import KafkaSettings

    settings = MagicMock(spec=KafkaSettings)
    settings.mode = MagicMock()
    settings.mode.value = "invalid_mode"
    settings.mode.__str__ = lambda: "invalid_mode"

    backend = KafkaBackend(settings)
    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()
    assert "invalid_mode" in str(exc_info.value)

  def test_mongodb_unsupported_mode(self):
    """Test MongoDB backend with unsupported mode."""
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBSettings

    settings = MagicMock(spec=MongoDBSettings)
    settings.mode = MagicMock()
    settings.mode.value = "invalid_mode"
    settings.mode.__str__ = lambda: "invalid_mode"

    backend = MongoDBBackend(settings)
    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()
    assert "invalid_mode" in str(exc_info.value)

  def test_rabbitmq_unsupported_mode(self):
    """Test RabbitMQ backend with unsupported mode."""
    from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
    from scrapy_extension.config.settings import RabbitMQSettings

    settings = MagicMock(spec=RabbitMQSettings)
    settings.mode = MagicMock()
    settings.mode.value = "invalid_mode"
    settings.mode.__str__ = lambda: "invalid_mode"

    backend = RabbitMQBackend(settings)
    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()
    assert "invalid_mode" in str(exc_info.value)
