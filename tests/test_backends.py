"""Tests for backend implementations."""

from unittest.mock import Mock, patch

import pytest

from scrapy_extension.backends.base import (
    BackendType,
    JSONSerializer,
)
from scrapy_extension.exceptions import ConnectionError, QueueError


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
        with patch("scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis):
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
            with pytest.raises(ConnectionError):
                backend.connect()

    def test_queue_push(self, redis_settings, mock_redis):
        """Test queue push operation."""
        from scrapy_extension.backends.redis_backend import RedisBackend
        with patch("scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis):
            backend = RedisBackend(redis_settings)
            backend.push("test_queue", b"test_data", priority=1.0)
            mock_redis.zadd.assert_called_once()

    def test_queue_pop(self, redis_settings, mock_redis):
        """Test queue pop operation."""
        from scrapy_extension.backends.redis_backend import RedisBackend
        mock_redis.zpopmax.return_value = [(b"test_data", 1.0)]
        with patch("scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis):
            backend = RedisBackend(redis_settings)
            result = backend.pop("test_queue")
            assert result == b"test_data"

    def test_queue_pop_empty(self, redis_settings, mock_redis):
        """Test queue pop with empty queue."""
        from scrapy_extension.backends.redis_backend import RedisBackend
        mock_redis.zpopmax.return_value = []
        with patch("scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis):
            backend = RedisBackend(redis_settings)
            result = backend.pop("test_queue")
            assert result is None

    def test_set_add(self, redis_settings, mock_redis):
        """Test set add operation."""
        from scrapy_extension.backends.redis_backend import RedisBackend
        mock_redis.sadd.return_value = 1
        with patch("scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis):
            backend = RedisBackend(redis_settings)
            result = backend.add("test_set", b"test_item")
            assert result is True

    def test_set_contains(self, redis_settings, mock_redis):
        """Test set contains operation."""
        from scrapy_extension.backends.redis_backend import RedisBackend
        mock_redis.sismember.return_value = True
        with patch("scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis):
            backend = RedisBackend(redis_settings)
            result = backend.contains("test_set", b"test_item")
            assert result is True

    def test_storage_store(self, redis_settings, mock_redis):
        """Test storage store operation."""
        from scrapy_extension.backends.redis_backend import RedisBackend
        with patch("scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis):
            backend = RedisBackend(redis_settings)
            backend.store("test_key", b"test_data")
            mock_redis.set.assert_called_once_with("test_key", b"test_data")

    def test_storage_retrieve(self, redis_settings, mock_redis):
        """Test storage retrieve operation."""
        from scrapy_extension.backends.redis_backend import RedisBackend
        mock_redis.get.return_value = b"test_data"
        with patch("scrapy_extension.backends.redis_backend.Redis", return_value=mock_redis):
            backend = RedisBackend(redis_settings)
            result = backend.retrieve("test_key")
            assert result == b"test_data"
