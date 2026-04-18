"""Tests for configuration module."""

import pytest
from pydantic import ValidationError
from scrapy_extension.backends.base import BackendType
from scrapy_extension.settings import RedisSettings, Settings


class TestSettings:
  """Test base Settings."""

  def test_default_backend_type(self):
    """Test default backend type is REDIS."""
    settings = Settings()
    assert settings.backend_type == BackendType.REDIS

  def test_default_serializer(self):
    """Test default serializer is json."""
    settings = Settings()
    assert settings.serializer == "json"

  def test_default_retry_attempts(self):
    """Test default retry attempts."""
    settings = Settings()
    assert settings.retry_attempts == 3

  def test_default_retry_delay(self):
    """Test default retry delay."""
    settings = Settings()
    assert settings.retry_delay == 1.0

  def test_backend_type_from_str(self):
    """Test backend type from string."""
    settings = Settings(backend_type=BackendType.MONGODB)
    assert settings.backend_type == BackendType.MONGODB


class TestRedisSettings:
  """Test RedisSettings."""

  def test_default_host(self):
    """Test default host."""
    settings = RedisSettings()
    assert settings.host == "localhost"

  def test_default_port(self):
    """Test default port."""
    settings = RedisSettings()
    assert settings.port == 6379

  def test_default_db(self):
    """Test default db."""
    settings = RedisSettings()
    assert settings.db == 0

  def test_custom_host(self):
    """Test custom host."""
    settings = RedisSettings(host="redis.example.com")
    assert settings.host == "redis.example.com"

  def test_custom_port(self):
    """Test custom port."""
    settings = RedisSettings(port=6380)
    assert settings.port == 6380

  def test_port_validation(self):
    """Test port validation."""
    with pytest.raises(ValidationError):
      RedisSettings(port=0)

    with pytest.raises(ValidationError):
      RedisSettings(port=70000)

  def test_password_optional(self):
    """Test password is optional."""
    settings = RedisSettings()
    assert settings.password is None

    settings = RedisSettings(password="secret")
    assert settings.password == "secret"  # noqa: S105

  def test_from_env_vars(self, monkeypatch):
    """Test loading from environment variables."""
    monkeypatch.setenv("SCRAPY_REDIS_HOST", "redis.example.com")
    monkeypatch.setenv("SCRAPY_REDIS_PORT", "6380")

    settings = RedisSettings()
    assert settings.host == "redis.example.com"
    assert settings.port == 6380


class TestMongoDBSettings:
  """Test MongoDBSettings."""

  def test_default_values(self):
    """Test all default values."""
    from scrapy_extension.settings import MongoDBSettings

    settings = MongoDBSettings()
    assert settings.uri == "mongodb://localhost:27017"
    assert settings.database == "scrapy_extension"
    assert settings.queue_collection == "queues"
    assert settings.set_collection == "sets"
    assert settings.storage_collection == "storage"
    assert settings.min_pool_size == 1
    assert settings.max_pool_size == 10
    assert settings.max_idle_time_ms == 60000
    assert settings.wait_queue_timeout_ms == 5000
    assert settings.w == 1
    assert settings.journal is True
    assert settings.read_preference == "primary"

  def test_from_env_vars(self, monkeypatch):
    """Test loading from environment variables."""
    from scrapy_extension.settings import MongoDBSettings

    monkeypatch.setenv("SCRAPY_MONGO_URI", "mongodb://custom:27017")
    monkeypatch.setenv("SCRAPY_MONGO_DATABASE", "custom_db")
    settings = MongoDBSettings()
    assert settings.uri == "mongodb://custom:27017"
    assert settings.database == "custom_db"


def test_kafka_settings_defaults():
  from scrapy_extension.settings import KafkaSettings

  settings = KafkaSettings()
  assert settings.bootstrap_servers == "localhost:9092"
  assert settings.max_priority_partitions == 10
  assert settings.acks == "all"
  assert settings.group_id == "scrapy-extension"


def test_kafka_settings_from_env(monkeypatch):
  from scrapy_extension.settings import KafkaSettings

  monkeypatch.setenv("SCRAPY_KAFKA_BOOTSTRAP_SERVERS", "kafka.example.com:9092")
  monkeypatch.setenv("SCRAPY_KAFKA_GROUP_ID", "my-group")
  settings = KafkaSettings()
  assert settings.bootstrap_servers == "kafka.example.com:9092"
  assert settings.group_id == "my-group"


def test_rabbitmq_settings_defaults():
  from scrapy_extension.settings import RabbitMQSettings

  settings = RabbitMQSettings()
  assert settings.host == "localhost"
  assert settings.port == 5672
  assert settings.username == "guest"
  assert settings.password == "guest"  # noqa: S105
  assert settings.max_priority == 255
