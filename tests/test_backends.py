"""Tests for backend implementations."""

from unittest.mock import Mock, patch

import pytest

from scrapy_extension.backends.base import (
  BackendType,
  JSONSerializer,
)
from scrapy_extension.exceptions import BackendConnectionError


class TestRedisMode:
  """Test RedisMode enum."""

  def test_standalone_value(self):
    from scrapy_extension.config.settings import RedisMode

    assert RedisMode.STANDALONE.value == "standalone"

  def test_master_slave_value(self):
    from scrapy_extension.config.settings import RedisMode

    assert RedisMode.MASTER_SLAVE.value == "master_slave"

  def test_sentinel_value(self):
    from scrapy_extension.config.settings import RedisMode

    assert RedisMode.SENTINEL.value == "sentinel"

  def test_cluster_value(self):
    from scrapy_extension.config.settings import RedisMode

    assert RedisMode.CLUSTER.value == "cluster"


class TestBackendType:
  """Test BackendType enum."""

  def test_redis_value(self):
    assert BackendType.REDIS.value == "redis"

  def test_mongodb_value(self):
    assert BackendType.MONGODB.value == "mongodb"

  def test_kafka_value(self):
    assert BackendType.KAFKA.value == "kafka"

  def test_rabbitmq_value(self):
    assert BackendType.RABBITMQ.value == "rabbitmq"


class TestJSONSerializer:
  """Test JSONSerializer."""

  def test_serialize_dict(self):
    serializer = JSONSerializer()
    data = {"key": "value"}
    result = serializer.serialize(data)
    assert result == b'{"key": "value"}'

  def test_deserialize_dict(self):
    serializer = JSONSerializer()
    data = b'{"key": "value"}'
    result = serializer.deserialize(data)
    assert result == {"key": "value"}

  def test_serialize_list(self):
    serializer = JSONSerializer()
    data = [1, 2, 3]
    result = serializer.serialize(data)
    assert result == b"[1, 2, 3]"

  def test_round_trip(self):
    serializer = JSONSerializer()
    data = {"nested": {"key": "value"}, "list": [1, 2, 3]}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized == data


class TestRedisBackend:
  """Test RedisBackend implementation."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.config.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self):
    """Create mock Redis client."""
    return Mock()

  def test_backend_type(self, redis_settings):
    """Test backend type is REDIS."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    backend = RedisBackend(redis_settings)
    assert backend.backend_type == BackendType.REDIS

  def test_connect_success(self, redis_settings, mock_redis):
    """Test successful connection."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(redis_settings)
      backend.connect()
      assert backend.is_connected()
      # ping is called in connect() and is_connected(), so at least once
      assert mock_redis.ping.call_count >= 1

  def test_connect_failure(self, redis_settings):
    """Test connection failure raises ConnectionError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis_backend import RedisBackend

    with patch("scrapy_extension.backends.redis_backend.Redis") as mock:
      mock.return_value.ping.side_effect = RedisError("Connection refused")
      backend = RedisBackend(redis_settings)
      with pytest.raises(BackendConnectionError):
        backend.connect()

  def test_queue_push(self, redis_settings, mock_redis):
    """Test queue push operation."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(redis_settings)
      backend.push("test_queue", b"test_data", priority=1.0)
      mock_redis.zadd.assert_called_once()

  def test_queue_pop(self, redis_settings, mock_redis):
    """Test queue pop operation."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    mock_redis.zpopmax.return_value = [(b"test_data", 1.0)]
    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(redis_settings)
      result = backend.pop("test_queue")
      assert result == b"test_data"

  def test_queue_pop_empty(self, redis_settings, mock_redis):
    """Test queue pop with empty queue."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    mock_redis.zpopmax.return_value = []
    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(redis_settings)
      result = backend.pop("test_queue")
      assert result is None

  def test_set_add(self, redis_settings, mock_redis):
    """Test set add operation."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    mock_redis.sadd.return_value = 1
    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(redis_settings)
      result = backend.add("test_set", b"test_item")
      assert result is True

  def test_set_contains(self, redis_settings, mock_redis):
    """Test set contains operation."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    mock_redis.sismember.return_value = True
    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(redis_settings)
      result = backend.contains("test_set", b"test_item")
      assert result is True

  def test_storage_store(self, redis_settings, mock_redis):
    """Test storage store operation."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(redis_settings)
      backend.store("test_key", b"test_data")
      mock_redis.set.assert_called_once_with("test_key", b"test_data")

  def test_storage_retrieve(self, redis_settings, mock_redis):
    """Test storage retrieve operation."""
    from scrapy_extension.backends.redis_backend import RedisBackend

    mock_redis.get.return_value = b"test_data"
    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(redis_settings)
      result = backend.retrieve("test_key")
      assert result == b"test_data"


class TestRedisBackendModes:
  """Test RedisBackend with different deployment modes."""

  @pytest.fixture
  def mock_redis(self):
    """Create mock Redis client."""
    return Mock()

  def test_standalone_mode_default(self, mock_redis):
    """Test standalone mode is default."""
    from scrapy_extension.backends.redis_backend import RedisBackend
    from scrapy_extension.config.settings import RedisMode, RedisSettings

    settings = RedisSettings(host="localhost", port=6379)
    assert settings.mode == RedisMode.STANDALONE

    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_sentinel_mode_success(self, mock_redis):
    """Test sentinel mode connection."""
    from scrapy_extension.backends.redis_backend import RedisBackend
    from scrapy_extension.config.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379", "sentinel2:26379"],
      sentinel_master_name="mymaster",
      password="secret",
    )

    mock_sentinel = Mock()
    mock_sentinel.master_for.return_value = mock_redis

    with patch(
      "scrapy_extension.backends.redis_backend.Sentinel", return_value=mock_sentinel
    ):
      backend = RedisBackend(settings)
      backend.connect()
      assert backend.is_connected()
      mock_sentinel.master_for.assert_called_once()

  def test_sentinel_mode_missing_sentinels(self):
    """Test sentinel mode requires sentinels configuration."""
    from scrapy_extension.backends.redis_backend import RedisBackend
    from scrapy_extension.config.settings import RedisMode, RedisSettings
    from scrapy_extension.exceptions import BackendConnectionError

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinel_master_name="mymaster",
    )

    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.connect()
    assert "sentinels" in str(exc_info.value).lower()

  def test_cluster_mode_success(self, mock_redis):
    """Test cluster mode connection."""
    from scrapy_extension.backends.redis_backend import RedisBackend
    from scrapy_extension.config.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["node1:7000", "node2:7000", "node3:7000"],
      password="secret",
    )

    with patch(
      "scrapy_extension.backends.redis_backend.RedisCluster", return_value=mock_redis
    ):
      backend = RedisBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_master_slave_mode_success(self, mock_redis):
    """Test master-slave mode connection."""
    from scrapy_extension.backends.redis_backend import RedisBackend
    from scrapy_extension.config.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=["replica1.redis.com:6379", "replica2.redis.com:6379"],
    )

    with patch(
      "scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis
    ):
      backend = RedisBackend(settings)
      backend.connect()
      assert backend.is_connected()

  def test_cluster_mode_uses_startup_nodes(self, mock_redis):
    """Test cluster mode uses startup nodes configuration."""
    from scrapy_extension.backends.redis_backend import RedisBackend
    from scrapy_extension.config.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["node1:7000", "node2:7000"],
    )

    with patch(
      "scrapy_extension.backends.redis_backend.RedisCluster", return_value=mock_redis
    ) as mock_cluster_class:
      backend = RedisBackend(settings)
      backend.connect()
      mock_cluster_class.assert_called_once()
      call_kwargs = mock_cluster_class.call_args.kwargs
      assert "startup_nodes" in call_kwargs
      assert len(call_kwargs["startup_nodes"]) == 2

  def test_sentinel_mode_configuration(self, mock_redis):
    """Test sentinel mode configuration options."""
    from scrapy_extension.backends.redis_backend import RedisBackend
    from scrapy_extension.config.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"],
      sentinel_master_name="myredis",
      sentinel_password="sentinel_pass",
      password="redis_pass",
      db=0,
    )

    mock_sentinel = Mock()
    mock_sentinel.master_for.return_value = mock_redis

    with patch(
      "scrapy_extension.backends.redis_backend.Sentinel", return_value=mock_sentinel
    ) as mock_sentinel_class:
      backend = RedisBackend(settings)
      backend.connect()
      mock_sentinel_class.assert_called_once()
      # Verify sentinels were passed correctly
      call_args = mock_sentinel_class.call_args
      assert len(call_args.args[0]) == 3  # Three sentinel tuples
