"""Tests for configuration module."""

import pytest
from pydantic import ValidationError

from scrapy_extension.backends.base import BackendType
from scrapy_extension.config.settings import RedisSettings, Settings


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
    settings = Settings(backend_type="mongodb")
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
    assert settings.password == "secret"

  def test_from_env_vars(self, monkeypatch):
    """Test loading from environment variables."""
    monkeypatch.setenv("SCRAPY_REDIS_HOST", "redis.example.com")
    monkeypatch.setenv("SCRAPY_REDIS_PORT", "6380")

    settings = RedisSettings()
    assert settings.host == "redis.example.com"
    assert settings.port == 6380
