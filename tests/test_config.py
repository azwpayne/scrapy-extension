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
    assert settings.password.get_secret_value() == "secret"

  def test_from_env_vars(self, monkeypatch):
    """Test loading from environment variables."""
    monkeypatch.setenv("SCRAPY_REDIS_HOST", "redis.example.com")
    monkeypatch.setenv("SCRAPY_REDIS_PORT", "6380")

    settings = RedisSettings()
    assert settings.host == "redis.example.com"
    assert settings.port == 6380

  def test_ssl_check_hostname_defaults_to_true(self):
    """R2-C1: TLS hostname verification must be ON by default.

    A misconfigured env that flips ``ssl_enabled=True`` must NOT silently
    accept any valid-CA cert for an unrelated domain (MITM). Operators who
    need IP-only service discovery must opt out explicitly.
    """
    settings = RedisSettings()
    assert settings.ssl_check_hostname is True


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
  """R2-C2: username/password are required (no silent guest/guest fallback).

  Construction must fail fast when creds are missing. Tests that exercise
  non-auth fields pass explicit test credentials.
  """
  from scrapy_extension.settings import RabbitMQSettings

  settings = RabbitMQSettings(username="test-user", password="test-pass")
  assert settings.host == "localhost"
  assert settings.port == 5672
  assert settings.username == "test-user"
  assert settings.password.get_secret_value() == "test-pass"
  assert settings.max_priority == 255


def test_rabbitmq_settings_requires_username_and_password(monkeypatch):
  """R2-C2: missing creds must raise ValidationError (no guest/guest default)."""
  from scrapy_extension.settings import RabbitMQSettings

  # The ``_rabbitmq_test_credentials`` autouse fixture (conftest) sets these so
  # bare ``RabbitMQSettings()`` works elsewhere; this test asserts the
  # required-creds contract, so they must be absent here.
  monkeypatch.delenv("SCRAPY_RABBITMQ_USERNAME", raising=False)
  monkeypatch.delenv("SCRAPY_RABBITMQ_PASSWORD", raising=False)

  with pytest.raises(ValidationError):
    RabbitMQSettings()

  with pytest.raises(ValidationError):
    RabbitMQSettings(password="p")

  with pytest.raises(ValidationError):
    RabbitMQSettings(username="u")


class TestConfigurationErrorRedaction:
  """R2-B6 / R26-C1: ConfigurationError must not retain secrets.

  Defensive design — current backend code only passes non-sensitive
  ``setting_value`` (mode, sentinels, defaults), but future contributors
  may pass credentials. The redaction at ``__init__`` time ensures the
  raw value never lives on the exception object, so ``repr(exc)`` and
  debug-logging the exception cannot leak.
  """

  def test_secretstr_setting_value_is_redacted(self):
    """A SecretStr value is masked regardless of setting_name."""
    from pydantic import SecretStr

    from scrapy_extension.exceptions import ConfigurationError

    exc = ConfigurationError(
      "invalid",
      setting_name="uri",
      setting_value=SecretStr("hunter2"),
    )
    assert exc.setting_value == "***REDACTED***"
    assert "hunter2" not in repr(exc)

  def test_sensitive_setting_name_redacts_any_value(self):
    """Names containing 'password', 'secret', 'api_key', 'token' trigger redaction."""
    from scrapy_extension.exceptions import ConfigurationError

    for sensitive_name in (
      "password",
      "rabbitmq_password",
      "API_KEY",
      "auth_token",
      "confluent_api_secret",
    ):
      exc = ConfigurationError(
        "invalid", setting_name=sensitive_name, setting_value="plain-string-secret"
      )
      assert exc.setting_value == "***REDACTED***", sensitive_name

  def test_non_sensitive_value_is_preserved(self):
    """Non-sensitive names + non-secret values pass through unchanged (for debugging)."""
    from scrapy_extension.exceptions import ConfigurationError

    exc = ConfigurationError(
      "invalid mode",
      setting_name="mode",
      setting_value="INVALID_MODE",
    )
    assert exc.setting_value == "INVALID_MODE"

  def test_no_setting_passed_preserves_none(self):
    """Default (no name/value) leaves setting_value as None."""
    from scrapy_extension.exceptions import ConfigurationError

    exc = ConfigurationError("just a message")
    assert exc.setting_name is None
    assert exc.setting_value is None


class TestBackpressureSettings:
  """Round-4 BP-1: backpressure pause/resume depth settings.

  Two additive, default-``None`` fields (zero compat break) configuring the
  scheduler's depth-gated pull-rate throttle. ``pause_at`` is the queue depth
  at/above which ``next_request`` returns None (Scrapy's contract-correct
  "slow down" signal); ``resume_at`` is the depth at/below which it resumes
  (hysteresis, prevents flapping). When only ``pause_at`` is set the scheduler
  defaults ``resume_at := pause_at`` at consume time, so no cross-check fires.
  """

  def test_both_unset_defaults_to_none(self):
    """Default-off: both None → feature disabled (byte-identical to pre-BP)."""
    settings = Settings()
    assert settings.backpressure_pause_at is None
    assert settings.backpressure_resume_at is None

  def test_only_pause_at_set_accepted(self):
    """Only pause_at set → cross-check skipped (resume defaults to pause later)."""
    settings = Settings(backpressure_pause_at=10)
    assert settings.backpressure_pause_at == 10
    assert settings.backpressure_resume_at is None

  def test_resume_below_pause_accepted(self):
    """resume_at=5, pause_at=10 → valid hysteresis band."""
    settings = Settings(backpressure_pause_at=10, backpressure_resume_at=5)
    assert settings.backpressure_pause_at == 10
    assert settings.backpressure_resume_at == 5

  def test_resume_above_pause_rejected(self):
    """resume_at > pause_at → ConfigurationError (would never resume)."""
    from scrapy_extension.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError) as exc_info:
      Settings(backpressure_pause_at=10, backpressure_resume_at=15)
    assert exc_info.value.setting_name == "backpressure_resume_at"

  def test_negative_pause_at_rejected(self):
    """pause_at < 0 → ConfigurationError (depth cannot be negative)."""
    from scrapy_extension.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError):
      Settings(backpressure_pause_at=-1)

  def test_negative_resume_at_rejected(self):
    """resume_at < 0 → ConfigurationError."""
    from scrapy_extension.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError):
      Settings(backpressure_pause_at=10, backpressure_resume_at=-1)

  def test_resume_equals_pause_accepted(self):
    """resume_at == pause_at → valid (no hysteresis, but not invalid)."""
    settings = Settings(backpressure_pause_at=10, backpressure_resume_at=10)
    assert settings.backpressure_pause_at == 10
    assert settings.backpressure_resume_at == 10

  def test_only_resume_at_set_accepted(self):
    """Only resume_at set → accepted (pause_at None at settings layer; scheduler treats feature off)."""
    settings = Settings(backpressure_resume_at=5)
    assert settings.backpressure_pause_at is None
    assert settings.backpressure_resume_at == 5

  def test_pause_at_from_env(self, monkeypatch):
    """Loads from SCRAPY_BACKPRESSURE_PAUSE_AT env var."""
    monkeypatch.setenv("SCRAPY_BACKPRESSURE_PAUSE_AT", "10")
    settings = Settings()
    assert settings.backpressure_pause_at == 10

  def test_resume_at_from_env(self, monkeypatch):
    """Loads from SCRAPY_BACKPRESSURE_RESUME_AT env var."""
    monkeypatch.setenv("SCRAPY_BACKPRESSURE_RESUME_AT", "5")
    settings = Settings()
    assert settings.backpressure_resume_at == 5


