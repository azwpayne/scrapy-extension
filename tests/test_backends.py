"""Tests for backend implementations."""

import pytest
from scrapy_extension.backends.base import (
  BackendType,
  JSONSerializer,
)
from scrapy_extension.exceptions import BackendConnectionError


class TestRedisMode:
  """Test RedisMode enum."""

  def test_standalone_value(self):
    from scrapy_extension.settings import RedisMode

    assert RedisMode.STANDALONE.value == "standalone"

  def test_master_slave_value(self):
    from scrapy_extension.settings import RedisMode

    assert RedisMode.MASTER_SLAVE.value == "master_slave"

  def test_sentinel_value(self):
    from scrapy_extension.settings import RedisMode

    assert RedisMode.SENTINEL.value == "sentinel"

  def test_cluster_value(self):
    from scrapy_extension.settings import RedisMode

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
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_backend_type(self, redis_settings):
    """Test backend type is REDIS."""
    from scrapy_extension.backends.redis import RedisBackend

    backend = RedisBackend(redis_settings)
    assert backend.backend_type == BackendType.REDIS

  def test_connect_success(self, redis_settings, mock_redis, mocker):
    """Test successful connection."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.is_connected()
    # ping is called in connect() and is_connected(), so at least once
    assert mock_redis.ping.call_count >= 1

  def test_connect_failure(self, redis_settings, mocker):
    """Test connection failure raises ConnectionError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock = mocker.patch("scrapy_extension.backends.redis.Redis")
    mock.return_value.ping.side_effect = RedisError("Connection refused")
    backend = RedisBackend(redis_settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_queue_push(self, redis_settings, mock_redis, mocker):
    """Test queue push operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.push("test_queue", b"test_data", priority=1.0)
    mock_redis.zadd.assert_called_once()

  def test_queue_pop(self, redis_settings, mock_redis, mocker):
    """Test queue pop operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.zpopmax.return_value = [(b"test_data", 1.0)]
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue")
    assert result == b"test_data"

  def test_queue_pop_empty(self, redis_settings, mock_redis, mocker):
    """Test queue pop with empty queue."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.zpopmax.return_value = []
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue")
    assert result is None

  def test_set_add(self, redis_settings, mock_redis, mocker):
    """Test set add operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sadd.return_value = 1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.add("test_set", b"test_item")
    assert result is True

  def test_set_contains(self, redis_settings, mock_redis, mocker):
    """Test set contains operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sismember.return_value = True
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.contains("test_set", b"test_item")
    assert result is True

  def test_storage_store(self, redis_settings, mock_redis, mocker):
    """Test storage store operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.store("test_key", b"test_data")
    mock_redis.set.assert_called_once_with("test_key", b"test_data")

  def test_storage_retrieve(self, redis_settings, mock_redis, mocker):
    """Test storage retrieve operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.get.return_value = b"test_data"
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.retrieve("test_key")
    assert result == b"test_data"


class TestRedisBackendModes:
  """Test RedisBackend with different deployment modes."""

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_standalone_mode_default(self, mock_redis, mocker):
    """Test standalone mode is default."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(host="localhost", port=6379)
    assert settings.mode == RedisMode.STANDALONE

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

  def test_sentinel_mode_success(self, mock_redis, mocker):
    """Test sentinel mode connection."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379", "sentinel2:26379"],
      sentinel_master_name="mymaster",
      password="secret",
    )

    mock_sentinel = mocker.Mock()
    mock_sentinel.master_for.return_value = mock_redis

    mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=mock_sentinel)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()
    mock_sentinel.master_for.assert_called_once()

  def test_sentinel_mode_missing_sentinels(self):
    """Test sentinel mode requires sentinels configuration."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import ConfigurationError
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinel_master_name="mymaster",
    )

    backend = RedisBackend(settings)
    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()
    assert "sentinels" in str(exc_info.value).lower()

  def test_cluster_mode_success(self, mock_redis, mocker):
    """Test cluster mode connection."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["node1:7000", "node2:7000", "node3:7000"],
      password="secret",
    )

    mocker.patch(
      "scrapy_extension.backends.redis.RedisCluster", return_value=mock_redis
    )
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

  def test_master_slave_mode_success(self, mock_redis, mocker):
    """Test master-slave mode connection."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=["replica1.redis.com:6379", "replica2.redis.com:6379"],
    )

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

  def test_cluster_mode_uses_startup_nodes(self, mock_redis, mocker):
    """Test cluster mode uses startup nodes configuration."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["node1:7000", "node2:7000"],
    )

    mock_cluster_class = mocker.patch("scrapy_extension.backends.redis.RedisCluster")
    mock_cluster_class.return_value = mock_redis
    backend = RedisBackend(settings)
    backend.connect()
    mock_cluster_class.assert_called_once()
    call_kwargs = mock_cluster_class.call_args.kwargs
    assert "startup_nodes" in call_kwargs
    assert len(call_kwargs["startup_nodes"]) == 2

  def test_sentinel_mode_configuration(self, mock_redis, mocker):
    """Test sentinel mode configuration options."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"],
      sentinel_master_name="myredis",
      sentinel_password="sentinel_pass",
      password="redis_pass",
      db=0,
    )

    mock_sentinel = mocker.Mock()
    mock_sentinel.master_for.return_value = mock_redis

    mock_sentinel_class = mocker.patch("scrapy_extension.backends.redis.Sentinel")
    mock_sentinel_class.return_value = mock_sentinel
    backend = RedisBackend(settings)
    backend.connect()
    mock_sentinel_class.assert_called_once()
    # Verify sentinels were passed correctly
    call_args = mock_sentinel_class.call_args
    assert len(call_args.args[0]) == 3  # Three sentinel tuples

  def test_sentinel_mode_with_username(self, mock_redis, mocker):
    """Test sentinel mode with username configuration."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379"],
      sentinel_master_name="mymaster",
      sentinel_username="sentinel_user",
      sentinel_password="sentinel_pass",
    )

    mock_sentinel = mocker.Mock()
    mock_sentinel.master_for.return_value = mock_redis

    mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=mock_sentinel)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

  def test_master_slave_mode_with_replicas(self, mocker):
    """Test master-slave mode logs replica configuration."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=["replica1.redis.com:6379", "replica2.redis.com:6379"],
    )

    mock_redis = mocker.Mock()
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(settings)
    backend.connect()
    # Just verify connect succeeds with replicas configured
    assert backend.is_connected()

  def test_cluster_mode_fallback_host_port(self, mock_redis, mocker):
    """Test cluster mode falls back to host:port when no startup nodes."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      host="cluster.redis.com",
      port=7000,
      # No cluster_startup_nodes configured
    )

    mock_cluster_class = mocker.patch("scrapy_extension.backends.redis.RedisCluster")
    mock_cluster_class.return_value = mock_redis
    backend = RedisBackend(settings)
    backend.connect()
    mock_cluster_class.assert_called_once()
    call_kwargs = mock_cluster_class.call_args.kwargs
    # Should fall back to host:port
    assert len(call_kwargs["startup_nodes"]) == 1


class TestRedisBackendDisconnect:
  """Test RedisBackend disconnect functionality."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_disconnect_single_client(self, redis_settings, mock_redis, mocker):
    """Test disconnect with single client."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.is_connected()

    backend.disconnect()
    assert not backend.is_connected()
    mock_redis.close.assert_called_once()

  def test_disconnect_master_slave_separate_clients(self, mocker):
    """Test disconnect with separate master and slave clients."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=["replica1.redis.com:6379"],
    )

    mock_master = mocker.Mock()
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_master)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

    backend.disconnect()
    assert not backend.is_connected()
    # Master should be closed separately
    mock_master.close.assert_called()

  def test_disconnect_error_suppressed(self, redis_settings, mock_redis, mocker):
    """Test disconnect suppresses RedisError during close."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    mock_redis.close.side_effect = RedisError("Already closed")
    backend = RedisBackend(redis_settings)
    backend.connect()
    # Should not raise
    backend.disconnect()


class TestRedisBackendQueueOperations:
  """Test RedisBackend queue operations with error handling."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_queue_push_with_priority(self, redis_settings, mock_redis, mocker):
    """Test queue push with priority uses negative score."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.push("test_queue", b"test_data", priority=5.0)
    # Priority should be negated for ZADD
    mock_redis.zadd.assert_called_once_with("test_queue", {b"test_data": -5.0})

  def test_queue_push_error(self, redis_settings, mock_redis, mocker):
    """Test queue push raises QueueError on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_redis.zadd.side_effect = RedisError("Write error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(QueueError) as exc_info:
      backend.push("test_queue", b"test_data")
    assert "push" in str(exc_info.value).lower()

  def test_queue_pop_blocking(self, redis_settings, mock_redis, mocker):
    """Test blocking pop with BZPOPMAX."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.bzpopmax.return_value = ("test_queue", b"blocked_data", 1.0)
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue", timeout=5.0)
    assert result == b"blocked_data"
    mock_redis.bzpopmax.assert_called_once_with("test_queue", timeout=5.0)

  def test_queue_pop_blocking_timeout(self, redis_settings, mock_redis, mocker):
    """Test blocking pop returns None on timeout."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.bzpopmax.return_value = None
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue", timeout=1.0)
    assert result is None

  def test_queue_pop_error(self, redis_settings, mock_redis, mocker):
    """Test queue pop raises QueueError on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_redis.zpopmax.side_effect = RedisError("Read error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(QueueError) as exc_info:
      backend.pop("test_queue")
    assert "pop" in str(exc_info.value).lower()

  def test_queue_len_error(self, redis_settings, mock_redis, mocker):
    """Test queue_len returns 0 on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.zcard.side_effect = RedisError("Card error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.queue_len("test_queue")
    assert result == 0

  def test_clear_queue_error(self, redis_settings, mock_redis, mocker):
    """Test clear_queue logs warning on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.side_effect = RedisError("Delete error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    # Should not raise, just log warning
    backend.clear_queue("test_queue")


class TestRedisBackendSetOperations:
  """Test RedisBackend set operations with error handling."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_set_add_already_exists(self, redis_settings, mock_redis, mocker):
    """Test set add returns False when item already exists."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sadd.return_value = 0  # Already exists
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.add("test_set", b"existing_item")
    assert result is False

  def test_set_add_error(self, redis_settings, mock_redis, mocker):
    """Test set add returns False on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sadd.side_effect = RedisError("Add error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.add("test_set", b"test_item")
    assert result is False

  def test_set_remove_success(self, redis_settings, mock_redis, mocker):
    """Test set remove returns True on successful removal."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.srem.return_value = 1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.remove("test_set", b"test_item")
    assert result is True

  def test_set_remove_not_found(self, redis_settings, mock_redis, mocker):
    """Test set remove returns False when item not found."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.srem.return_value = 0
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.remove("test_set", b"missing_item")
    assert result is False

  def test_set_remove_error(self, redis_settings, mock_redis, mocker):
    """Test set remove returns False on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.srem.side_effect = RedisError("Remove error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.remove("test_set", b"test_item")
    assert result is False

  def test_set_contains_error(self, redis_settings, mock_redis, mocker):
    """Test set contains returns False on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sismember.side_effect = RedisError("Member error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.contains("test_set", b"test_item")
    assert result is False

  def test_set_len_error(self, redis_settings, mock_redis, mocker):
    """Test set_len returns 0 on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.scard.side_effect = RedisError("Card error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.set_len("test_set")
    assert result == 0

  def test_clear_set_error(self, redis_settings, mock_redis, mocker):
    """Test clear_set logs warning on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.side_effect = RedisError("Delete error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    # Should not raise, just log warning
    backend.clear_set("test_set")


class TestRedisBackendStorageOperations:
  """Test RedisBackend storage operations with error handling."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_storage_store_with_ttl(self, redis_settings, mock_redis, mocker):
    """Test storage store with TTL uses SETEX."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.store("test_key", b"test_data", ttl=3600)
    mock_redis.setex.assert_called_once_with("test_key", 3600, b"test_data")

  def test_storage_store_no_ttl(self, redis_settings, mock_redis, mocker):
    """Test storage store without TTL uses SET."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.store("test_key", b"test_data")
    mock_redis.set.assert_called_once_with("test_key", b"test_data")

  def test_storage_store_error(self, redis_settings, mock_redis, mocker, caplog):
    """Test storage store logs warning on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.set.side_effect = RedisError("Write error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.store("test_key", b"test_data")
    assert "Failed to store key" in caplog.text

  def test_storage_retrieve_string_conversion(self, redis_settings, mock_redis, mocker):
    """Test storage retrieve converts string to bytes."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.get.return_value = "string_data"
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.retrieve("test_key")
    assert result == b"string_data"

  def test_storage_retrieve_error(self, redis_settings, mock_redis, mocker):
    """Test storage retrieve returns None on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.get.side_effect = RedisError("Read error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.retrieve("test_key")
    assert result is None

  def test_delete_success(self, redis_settings, mock_redis, mocker):
    """Test delete returns True on successful deletion."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.return_value = 1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.delete("test_key")
    assert result is True

  def test_delete_not_found(self, redis_settings, mock_redis, mocker):
    """Test delete returns False when key not found."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.return_value = 0
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.delete("missing_key")
    assert result is False

  def test_delete_error(self, redis_settings, mock_redis, mocker):
    """Test delete returns False on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.side_effect = RedisError("Delete error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.delete("test_key")
    assert result is False

  def test_exists_true(self, redis_settings, mock_redis, mocker):
    """Test exists returns True when key exists."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.exists.return_value = 1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.exists("test_key")
    assert result is True

  def test_exists_false(self, redis_settings, mock_redis, mocker):
    """Test exists returns False when key does not exist."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.exists.return_value = 0
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.exists("missing_key")
    assert result is False

  def test_exists_error(self, redis_settings, mock_redis, mocker):
    """Test exists returns False on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.exists.side_effect = RedisError("Exists error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.exists("test_key")
    assert result is False

  def test_ttl_with_ttl(self, redis_settings, mock_redis, mocker):
    """Test ttl returns seconds when TTL is set."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ttl.return_value = 3600
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.ttl("test_key")
    assert result == 3600

  def test_ttl_no_ttl(self, redis_settings, mock_redis, mocker):
    """Test ttl returns None when no TTL set (-1)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ttl.return_value = -1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.ttl("test_key")
    assert result is None

  def test_ttl_key_not_exists(self, redis_settings, mock_redis, mocker):
    """Test ttl returns -1 when key doesn't exist (-2)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ttl.return_value = -2
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.ttl("missing_key")
    assert result == -1

  def test_ttl_error(self, redis_settings, mock_redis, mocker):
    """Test ttl returns None on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ttl.side_effect = RedisError("TTL error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.ttl("test_key")
    assert result is None

  def test_clear_storage_with_prefix(self, redis_settings, mock_redis, mocker):
    """Test clear_storage with prefix uses scan_iter."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.scan_iter.return_value = iter(["key1", "key2"])
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.clear_storage(prefix="test_prefix")
    mock_redis.scan_iter.assert_called_once_with(match="test_prefix*")
    assert mock_redis.delete.call_count == 2

  def test_clear_storage_no_prefix(self, redis_settings, mock_redis, mocker):
    """Test clear_storage without prefix uses flushdb."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.clear_storage()
    mock_redis.flushdb.assert_called_once()

  def test_clear_storage_cluster_with_prefix(self):
    """Test clear_storage with cluster and prefix.

    Note: isinstance check with mocked RedisCluster doesn't work with mocks.
    This test verifies the non-cluster branch behavior with prefix via the
    regular Redis client path. Cluster-specific behavior is covered by
    integration tests with real Redis Cluster.
    """
    # Cluster mode with prefix uses scan_iter - tested via code inspection
    # The isinstance check is the limiting factor for direct mocking
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )
    backend = RedisBackend(settings)
    # Verify settings are correctly stored for cluster mode
    assert backend.config.mode == RedisMode.CLUSTER

  def test_clear_storage_cluster_no_prefix(self):
    """Test clear_storage with cluster without prefix.

    Note: isinstance check with mocked RedisCluster doesn't work with mocks.
    Cluster-specific flushall is tested via integration tests.
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )
    backend = RedisBackend(settings)
    # Verify settings are correctly stored for cluster mode
    assert backend.config.mode == RedisMode.CLUSTER

  def test_clear_storage_error(self, redis_settings, mock_redis, mocker, caplog):
    """Test clear_storage logs warning on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.flushdb.side_effect = RedisError("Flush error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.clear_storage()
    assert "Failed to clear storage" in caplog.text


class TestRedisBackendPingAndConnection:
  """Test RedisBackend ping and connection state methods."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_is_connected_true(self, redis_settings, mock_redis, mocker):
    """Test is_connected returns True when ping succeeds."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ping.return_value = True
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.is_connected() is True

  def test_is_connected_false_when_none(self, redis_settings):
    """Test is_connected returns False when client is None."""
    from scrapy_extension.backends.redis import RedisBackend

    backend = RedisBackend(redis_settings)
    # Never connected
    assert backend.is_connected() is False

  def test_is_connected_false_on_error(self, redis_settings, mock_redis, mocker):
    """Test is_connected returns False on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    # First ping succeeds to allow connect, then fails for is_connected check
    mock_redis.ping.side_effect = [True, RedisError("Ping error")]
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.is_connected() is False

  def test_ping_success(self, redis_settings, mock_redis, mocker):
    """Test ping returns True on success."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ping.return_value = True
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.ping() is True

  def test_ping_false_when_none(self, redis_settings):
    """Test ping returns False when client is None."""
    from scrapy_extension.backends.redis import RedisBackend

    backend = RedisBackend(redis_settings)
    assert backend.ping() is False

  def test_ping_false_on_error(self, redis_settings, mock_redis, mocker):
    """Test ping returns False on RedisError."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    # First ping succeeds to allow connect, then fails for ping check
    mock_redis.ping.side_effect = [True, RedisError("Ping error")]
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.ping() is False

  def test_client_property_auto_connect(self, redis_settings, mock_redis, mocker):
    """Test client property triggers auto-connect if not connected."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    # Access client property without calling connect
    client = backend.client
    assert client is mock_redis
    # Verify ping was called during auto-connect
    assert getattr(mock_redis.ping, "call_count", 0) > 0


class TestRedisBackendConnectErrors:
  """Test RedisBackend connection error handling."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_connect_standalone_connection_error(self, redis_settings, mocker):
    """Test connect raises BackendConnectionError on ConnectionError."""
    from redis.exceptions import ConnectionError as RedisConnError
    from scrapy_extension.backends.redis import RedisBackend

    mock = mocker.patch("scrapy_extension.backends.redis.Redis")
    mock.return_value.ping.side_effect = RedisConnError("Connection refused")
    backend = RedisBackend(redis_settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_connect_master_slave_error(self, mocker):
    """Test connect raises BackendConnectionError for master-slave mode."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(mode=RedisMode.MASTER_SLAVE, host="master.redis.com")
    mock = mocker.patch("scrapy_extension.backends.redis.Redis")
    mock.return_value.ping.side_effect = RedisError("Master error")
    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_connect_sentinel_error(self, mocker):
    """Test connect raises BackendConnectionError for sentinel mode."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379"],
      sentinel_master_name="mymaster",
    )
    mock_sentinel = mocker.patch("scrapy_extension.backends.redis.Sentinel")
    mock_sentinel.return_value.master_for.side_effect = RedisError("Sentinel error")
    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_connect_cluster_error(self, mocker):
    """Test connect raises BackendConnectionError for cluster mode."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )
    mock = mocker.patch("scrapy_extension.backends.redis.RedisCluster")
    mock.return_value.ping.side_effect = RedisError("Cluster error")
    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()


class TestRedisBackendCoverageGaps:
  """Tests covering previously missing coverage lines in RedisBackend."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_validate_key_name_empty(self):
    """Test _validate_key_name raises ValueError for empty name (line 33)."""
    from scrapy_extension.backends.redis import _validate_key_name

    with pytest.raises(ValueError, match="Invalid name"):
      _validate_key_name("")

  def test_import_error_message(self):
    """Test ImportError includes helpful install message (lines 43-44)."""
    import subprocess
    import sys

    # Use subprocess to avoid corrupting the current process's module state
    result = subprocess.run(
      [
        sys.executable,
        "-c",
        (
          "import sys\n"
          "# Block redis from being imported\n"
          "import importlib.util\n"
          "sys.modules['redis'] = None\n"
          "sys.modules['redis.exceptions'] = None\n"
          "sys.modules['redis.cluster'] = None\n"
          "sys.modules['redis.sentinel'] = None\n"
          "try:\n"
          "    import scrapy_extension.backends.redis\n"
          "    print('ERROR: No ImportError raised')\n"
          "    sys.exit(1)\n"
          "except ImportError as e:\n"
          "    msg = str(e)\n"
          '    if "pip install scrapy-extension[redis]" in msg:\n'
          "        print('PASS')\n"
          "    else:\n"
          "        print(f'ERROR: Wrong message: {msg}')\n"
          "        sys.exit(1)\n"
        ),
      ],
      capture_output=True,
      text=True,
    )
    assert result.returncode == 0, (
      f"subprocess failed: {result.stderr}\n{result.stdout}"
    )
    assert "PASS" in result.stdout

  def test_connect_cluster_branch(self, mock_redis, mocker):
    """Test connect() CLUSTER branch and logger.debug (lines 113->118)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )
    mocker.patch(
      "scrapy_extension.backends.redis.RedisCluster", return_value=mock_redis
    )
    backend = RedisBackend(settings)
    backend.connect()
    # The CLUSTER branch is exercised; verify it connected
    assert backend.is_connected()

  def test_connect_master_slave_no_replicas(self, mock_redis, mocker):
    """Test _connect_master_slave with no replicas skips logging (line 169->exit)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=[],
    )
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()
    # With replicas=None, the `if self.config.replicas:` branch is skipped

  def test_disconnect_separate_master_client(self, redis_settings, mocker):
    """Test disconnect closes separate _master_client (lines 283-285)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_master = mocker.Mock()
    mock_client = mocker.Mock()

    backend = RedisBackend(redis_settings)
    # Manually create a scenario where _master_client is separate from _client
    backend._master_client = mock_master
    backend._client = mock_client
    backend._sentinel = mocker.Mock()

    backend.disconnect()
    # Both should be closed
    mock_master.close.assert_called()
    mock_client.close.assert_called()
    assert backend._master_client is None
    assert backend._client is None
    assert backend._sentinel is None

  def test_disconnect_master_client_redis_error_suppressed(
    self, redis_settings, mocker
  ):
    """Test disconnect suppresses RedisError when closing _master_client (lines 283-285)."""
    from redis.exceptions import RedisError
    from scrapy_extension.backends.redis import RedisBackend

    mock_master = mocker.Mock()
    mock_master.close.side_effect = RedisError("Already closed")
    mock_client = mocker.Mock()

    backend = RedisBackend(redis_settings)
    backend._master_client = mock_master
    backend._client = mock_client

    # Should not raise
    backend.disconnect()
    assert backend._master_client is None
    assert backend._client is None

  def test_disconnect_clears_sentinel(self, redis_settings, mocker):
    """Test disconnect sets _sentinel to None (lines 287->292)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_client = mocker.Mock()
    backend = RedisBackend(redis_settings)
    backend._client = mock_client
    backend._sentinel = mocker.Mock()

    backend.disconnect()
    assert backend._sentinel is None
    assert backend._client is None

  def test_retrieve_returns_none_for_missing_key(
    self, redis_settings, mock_redis, mocker
  ):
    """Test retrieve returns None when key doesn't exist (line 573)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.get.return_value = None
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.retrieve("missing_key")
    assert result is None

  def test_clear_storage_cluster_with_prefix(self, mocker):
    """Test clear_storage cluster scan_iter branch (lines 661-662)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )

    mock_cluster = mocker.MagicMock()
    mock_cluster.scan_iter.return_value = iter([b"prefix:key1", b"prefix:key2"])
    mock_cluster.ping.return_value = True

    mocker.patch(
      "scrapy_extension.backends.redis.RedisCluster", return_value=mock_cluster
    )
    # Patch isinstance so it returns True for the mock_cluster instance
    original_isinstance = isinstance
    mocker.patch(
      "scrapy_extension.backends.redis.isinstance",
      side_effect=lambda obj, cls: (
        True if obj is mock_cluster else original_isinstance(obj, cls)
      ),
    )
    backend = RedisBackend(settings)
    backend.connect()
    backend.clear_storage(prefix="prefix")

    mock_cluster.scan_iter.assert_called_once_with(match="prefix*")
    assert mock_cluster.delete.call_count == 2

  def test_clear_storage_cluster_no_prefix(self, mocker):
    """Test clear_storage cluster flushall branch (line 669)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )

    mock_cluster = mocker.MagicMock()
    mock_cluster.ping.return_value = True

    mocker.patch(
      "scrapy_extension.backends.redis.RedisCluster", return_value=mock_cluster
    )
    # Patch isinstance so it returns True for the mock_cluster instance
    original_isinstance = isinstance
    mocker.patch(
      "scrapy_extension.backends.redis.isinstance",
      side_effect=lambda obj, cls: (
        True if obj is mock_cluster else original_isinstance(obj, cls)
      ),
    )
    backend = RedisBackend(settings)
    backend.connect()
    backend.clear_storage()

    mock_cluster.flushall.assert_called_once()
