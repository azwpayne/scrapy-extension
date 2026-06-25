"""Tests for scrapy_extension/backends/connectors.py."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.base import (
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import BackendConnectionError


# --- SDK stubs for backends whose optional deps are absent in the test env ---
# Mirrors tests/test_memcached_backend.py: inject a stub package into sys.modules
# so the backend's module-level ``import pulsar`` / ``import boto3`` /
# ``from pymemcache.client.base import Client`` succeeds without the dep installed.
def _ensure_sdk_stub(module_dotted: str, attrs: dict[str, object] | None = None) -> None:
  """Inject (or extend) a stub package at ``module_dotted`` in sys.modules.

  Creates each parent package so attribute access like ``pulsar.Client`` works
  once the leaf module is imported.
  """
  parts = module_dotted.split(".")
  attrs = attrs or {}
  for i in range(1, len(parts) + 1):
    name = ".".join(parts[:i])
    if name not in sys.modules:
      mod = types.ModuleType(name)
      sys.modules[name] = mod
  leaf = sys.modules[module_dotted]
  for k, v in attrs.items():
    setattr(leaf, k, v)


_ensure_sdk_stub("pulsar", {"Client": MagicMock(name="PulsarClient")})
_ensure_sdk_stub("boto3")
_ensure_sdk_stub("pymemcache")
_ensure_sdk_stub("pymemcache.client")
_ensure_sdk_stub("pymemcache.client.base", {"Client": MagicMock(name="MemcachedClient")})
# rocketmq-client-python is also absent in the test env.
_ensure_sdk_stub("rocketmq")
_ensure_sdk_stub("rocketmq.client", {"Producer": MagicMock, "PushConsumer": MagicMock})


# Expected concrete class name per BackendType. Asserting ``type(backend).__name__``
# (in addition to ``backend.backend_type``) catches a case block wiring the WRONG
# Backend/Settings pair — e.g. ``PulsarBackend(SqsSettings(...))`` would still report
# ``backend_type is PULSAR`` (a hardcoded property) but the constructed class name
# would mismatch. This pins the per-case wiring, not just dispatch success.
_EXPECTED_BACKEND_CLASS: dict[BackendType, str] = {
  BackendType.REDIS: "RedisBackend",
  BackendType.MONGODB: "MongoDBBackend",
  BackendType.KAFKA: "KafkaBackend",
  BackendType.RABBITMQ: "RabbitMQBackend",
  BackendType.ELASTICSEARCH: "ElasticSearchBackend",
  BackendType.ROCKETMQ: "RocketMQBackend",
  BackendType.PULSAR: "PulsarBackend",
  BackendType.SQS: "SqsBackend",
  BackendType.MEMCACHED: "MemcachedBackend",
  BackendType.DYNAMODB: "DynamoDBBackend",
}



class TestConnectionManagerCreateBackend:
  """Tests for _create_backend method."""

  def test_create_backend_redis(self, mocker):
    """Test _create_backend creates RedisBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_redis_backend = mocker.patch("scrapy_extension.backends.redis.RedisBackend")
    mock_redis_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.REDIS)
    backend = manager._create_backend()

    mock_redis_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_mongodb(self, mocker):
    """Test _create_backend creates MongoDBBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_mongo_backend = mocker.patch(
      "scrapy_extension.backends.mongodb.MongoDBBackend"
    )
    mock_mongo_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.MONGODB)
    backend = manager._create_backend()

    mock_mongo_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_kafka(self, mocker):
    """Test _create_backend creates KafkaBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_kafka_backend = mocker.patch("scrapy_extension.backends.kafka.KafkaBackend")
    mock_kafka_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.KAFKA)
    backend = manager._create_backend()

    mock_kafka_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_rabbitmq(self, mocker):
    """Test _create_backend creates RabbitMQBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_rabbitmq_backend = mocker.patch(
      "scrapy_extension.backends.rabbitmq.RabbitMQBackend"
    )
    mock_rabbitmq_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.RABBITMQ)
    backend = manager._create_backend()

    mock_rabbitmq_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_elasticsearch(self, mocker):
    """Test _create_backend creates ElasticSearchBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_es_backend = mocker.patch(
      "scrapy_extension.backends.elasticsearch.ElasticSearchBackend"
    )
    mock_es_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.ELASTICSEARCH)
    backend = manager._create_backend()

    mock_es_backend.assert_called_once()
    assert backend == mock_backend

  def test_create_backend_unsupported_type(self):
    """Test _create_backend raises ConfigurationError for unregistered type.

    Round-5 R5-1: dispatch now routes through the registry's
    ``get_descriptor``; an unregistered backend type raises
    ``ConfigurationError`` (was ``ValueError``) — typed + carries the
    setting name, surfaceable by ``from_settings`` error handling.
    """
    from scrapy_extension.exceptions import ConfigurationError

    manager = ConnectionManager(BackendType.REDIS)
    # Deliberately set invalid type to test error handling
    manager.backend_type = "INVALID"  # type: ignore[assignment]

    with pytest.raises(ConfigurationError, match="not a registered backend"):
      manager._create_backend()

  def test_create_backend_rocketmq(self, mocker):
    """Test _create_backend creates RocketMQBackend correctly."""
    mock_backend = mocker.MagicMock()
    mock_rocketmq_backend = mocker.patch(
      "scrapy_extension.backends.rocketmq.RocketMQBackend"
    )
    mock_rocketmq_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.ROCKETMQ)
    backend = manager._create_backend()

    mock_rocketmq_backend.assert_called_once()
    assert backend == mock_backend


class TestConnectionManagerSettingsKey:
  """Tests for settings key generation in get_manager."""

  def test_settings_key_json_fallback(self):
    """Test JSON serialization falls back to string sorting for non-serializable settings."""

    # Create a settings object that cannot be JSON serialized
    # Use an object with __slots__ and no __dict__ so json.dumps can't serialize it
    class NonSerializable:
      __slots__ = ()

    settings = {"func": NonSerializable()}
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings)
    assert manager1 is manager2

  def test_settings_key_json_fallback_with_value_error(self, mocker):
    """Test JSON serialization falls back to str() sorting when json.dumps raises ValueError."""
    # Mock json.dumps to raise ValueError
    mock_json = mocker.patch("scrapy_extension.backends.connectors.json")
    mock_json.dumps.side_effect = ValueError("Object is not JSON serializable")

    settings = {"key": "value"}
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings)

    assert manager1 is manager2
    assert mock_json.dumps.call_count >= 1

  def test_settings_key_json_fallback_with_type_error(self, mocker):
    """Test JSON serialization falls back to str() sorting when json.dumps raises TypeError."""
    # Mock json.dumps to raise TypeError
    mock_json = mocker.patch("scrapy_extension.backends.connectors.json")
    mock_json.dumps.side_effect = TypeError("Object of type is not JSON serializable")

    settings = {"key": "value"}
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings)

    assert manager1 is manager2
    assert mock_json.dumps.call_count >= 1


class TestConnectionManagerRetryLogic:
  """Tests for retry logic with exponential backoff."""

  def test_connect_retry_exhausted(self, mocker):
    """Test connect raises BackendConnectionError after max retries."""
    mock_create_backend = mocker.patch.object(
      ConnectionManager,
      "_create_backend",
      side_effect=ConnectionError("Connection failed"),
    )
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")

    manager = ConnectionManager(
      BackendType.REDIS, {"retry_attempts": 3, "retry_delay": 0.1}
    )

    with pytest.raises(BackendConnectionError) as exc_info:
      manager.connect()

    assert "Failed to connect after 3 attempts" in str(exc_info.value)
    assert mock_create_backend.call_count == 3

  def test_connect_retry_success_on_first_attempt(self, mocker):
    """Test connect succeeds on first attempt without retries."""
    mock_create_backend = mocker.patch.object(ConnectionManager, "_create_backend")
    mock_backend = mocker.MagicMock()
    mock_create_backend.return_value = mock_backend

    manager = ConnectionManager(BackendType.REDIS)
    manager.connect()

    assert manager._backend == mock_backend
    assert mock_create_backend.call_count == 1

  def test_connect_retry_success_on_second_attempt(self, mocker):
    """Test connect succeeds on second attempt after initial failure."""
    mock_create_backend = mocker.patch.object(ConnectionManager, "_create_backend")
    mock_backend = mocker.MagicMock()

    # First call raises, second succeeds
    mock_create_backend.side_effect = [ConnectionError("Failed"), mock_backend]
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")

    manager = ConnectionManager(
      BackendType.REDIS, {"retry_attempts": 3, "retry_delay": 0.1}
    )
    manager.connect()

    assert manager._backend == mock_backend
    assert mock_create_backend.call_count == 2

  def test_connect_keyboard_interrupt_not_caught(self, mocker):
    """Test that KeyboardInterrupt is re-raised immediately."""
    mocker.patch.object(
      ConnectionManager, "_create_backend", side_effect=KeyboardInterrupt
    )
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")

    manager = ConnectionManager(BackendType.REDIS)

    with pytest.raises(KeyboardInterrupt):
      manager.connect()

  def test_connect_system_exit_not_caught(self, mocker):
    """Test that SystemExit is re-raised immediately."""
    mocker.patch.object(ConnectionManager, "_create_backend", side_effect=SystemExit)
    mocker.patch("scrapy_extension.backends.connectors.time.sleep")

    manager = ConnectionManager(BackendType.REDIS)

    with pytest.raises(SystemExit):
      manager.connect()


class TestConnectionManagerClose:
  """Tests for close method."""

  def test_close_disconnects_backend(self, mocker):
    """Test close calls disconnect on backend."""
    mock_backend = mocker.MagicMock()
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    manager.close()

    mock_backend.disconnect.assert_called_once()
    assert manager._backend is None

  def test_close_handles_disconnect_error(self, mocker):
    """Test close handles errors during disconnect gracefully."""
    mock_backend = mocker.MagicMock()
    mock_backend.disconnect.side_effect = RuntimeError("Disconnect error")
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    # Should not raise
    manager.close()

    assert manager._backend is None

  def test_close_when_backend_is_none(self):
    """Test close does nothing when backend is None."""
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    # Should not raise
    manager.close()


class TestConnectionManagerBackendProperty:
  """Tests for backend property with double-checked locking."""

  def test_backend_returns_existing_backend(self, mocker):
    """Test backend property returns existing backend without connecting."""
    mock_connect = mocker.patch.object(ConnectionManager, "connect")
    mock_backend = mocker.MagicMock()
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    result = manager.backend

    assert result == mock_backend
    mock_connect.assert_not_called()

  def test_backend_calls_connect_when_none(self, mocker):
    """Test backend property calls connect when _backend is None."""
    mock_connect = mocker.patch.object(ConnectionManager, "connect")
    mock_backend = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    # Simulate what connect() would do by setting _backend after connect is called
    def setup_backend():
      manager._backend = mock_backend

    mock_connect.side_effect = setup_backend

    result = manager.backend

    assert result == mock_backend
    mock_connect.assert_called_once()

  def test_backend_double_checked_locking_sets_backend(self, mocker):
    """Test backend property double-checked locking sets _backend via connect."""
    mock_backend = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    # Access backend property - should trigger connect which sets _backend
    result = manager.backend

    assert result is mock_backend
    assert manager._backend is mock_backend

  def test_backend_double_checked_locking_assertion(self, mocker):
    """Test backend property assert passes when connect sets _backend."""
    mock_backend = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    # This should not raise AssertionError
    result = manager.backend

    assert result is mock_backend

  def test_backend_raises_when_connect_returns_without_setting_backend(self, mocker):
    """Defensive guard: if ``connect()`` returns without raising AND without
    setting ``self._backend`` (a contract violation), the ``backend`` property
    must raise ``BackendConnectionError`` rather than returning ``None``.

    Exercises the ``if self._backend is None: raise BackendConnectionError``
    branch in the property (connectors.py ``backend`` getter). The mock makes
    ``connect()`` a no-op that never assigns ``_backend``, simulating the
    violation. The guard is the load-bearing safety net — ``assert`` would be
    stripped under ``python -O``, and returning ``None`` would crash callers
    downstream with a confusing ``AttributeError`` instead of a typed error.
    """
    # connect() returns normally but does NOT set self._backend — the
    # contract violation the defensive guard exists to catch.
    mocker.patch.object(ConnectionManager, "connect", return_value=None)

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None  # explicit: nothing wired the backend

    with pytest.raises(BackendConnectionError) as exc_info:
      _ = manager.backend  # property access triggers the defensive guard

    msg = str(exc_info.value)
    assert "connect()" in msg
    assert "backend" in msg

    # Registry hygiene: this test constructed a bare manager (not via
    # get_manager), but clear anyway to match the file's isolation pattern.
    ConnectionManager.clear_registry()


class TestConnectionManagerIsConnected:
  """Tests for is_connected method."""

  def test_is_connected_returns_false_when_backend_none(self):
    """Test is_connected returns False when _backend is None."""
    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = None

    assert manager.is_connected() is False

  def test_is_connected_returns_backend_status(self, mocker):
    """Test is_connected returns result from backend.is_connected()."""
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    assert manager.is_connected() is True
    mock_backend.is_connected.assert_called_once()


class TestConnectionManagerGetBackendInterface:
  """Tests for get_queue_backend, get_set_backend, get_storage_backend."""

  def test_get_queue_backend_not_implemented(self, mocker):
    """Test get_queue_backend raises NotImplementedError for non-QueueBackend."""
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True

    manager = ConnectionManager(BackendType.KAFKA)
    manager._backend = mock_backend

    with pytest.raises(NotImplementedError) as exc_info:
      manager.get_queue_backend()

    assert "does not support queue operations" in str(exc_info.value)

  def test_get_queue_backend_returns_backend(self):
    """Test get_queue_backend returns backend when it implements QueueBackend."""
    from scrapy_extension.backends.base import Backend

    class MockQueueBackend(Backend, QueueBackend):
      """Mock backend implementing QueueBackend."""

      def __init__(self):
        self._is_connected = True

      def connect(self):
        pass

      def disconnect(self):
        pass

      def is_connected(self):
        return self._is_connected

      def ping(self):
        return True

      @property
      def backend_type(self):
        return BackendType.REDIS

      def push(self, queue_name, item, priority=0.0):
        pass

      def pop(self, queue_name, timeout=0.0):
        return None

      def queue_len(self, queue_name):
        return 0

      def clear_queue(self, queue_name):
        pass

    mock_backend = MockQueueBackend()

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    result = manager.get_queue_backend()

    assert result is mock_backend

  def test_get_set_backend_not_implemented(self, mocker):
    """Test get_set_backend raises NotImplementedError for non-SetBackend."""
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True

    manager = ConnectionManager(BackendType.KAFKA)
    manager._backend = mock_backend

    with pytest.raises(NotImplementedError) as exc_info:
      manager.get_set_backend()

    assert "does not support set operations" in str(exc_info.value)

  def test_get_set_backend_returns_backend(self):
    """Test get_set_backend returns backend when it implements SetBackend."""
    from scrapy_extension.backends.base import Backend

    class MockSetBackend(Backend, SetBackend):
      """Mock backend implementing SetBackend."""

      def __init__(self):
        self._is_connected = True

      def connect(self):
        pass

      def disconnect(self):
        pass

      def is_connected(self):
        return self._is_connected

      def ping(self):
        return True

      @property
      def backend_type(self):
        return BackendType.REDIS

      def add(self, set_name, item):
        return True

      def remove(self, set_name, item):
        return True

      def contains(self, set_name, item):
        return True

      def set_len(self, set_name):
        return 0

      def clear_set(self, set_name):
        pass

    mock_backend = MockSetBackend()

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    result = manager.get_set_backend()

    assert result is mock_backend

  def test_get_storage_backend_not_implemented(self, mocker):
    """Test get_storage_backend raises NotImplementedError for non-StorageBackend."""
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True

    manager = ConnectionManager(BackendType.KAFKA)
    manager._backend = mock_backend

    with pytest.raises(NotImplementedError) as exc_info:
      manager.get_storage_backend()

    assert "does not support storage operations" in str(exc_info.value)

  def test_get_storage_backend_returns_backend(self):
    """Test get_storage_backend returns backend when it implements StorageBackend."""
    from scrapy_extension.backends.base import Backend

    class MockStorageBackend(Backend, StorageBackend):
      """Mock backend implementing StorageBackend."""

      def __init__(self):
        self._is_connected = True

      def connect(self):
        pass

      def disconnect(self):
        pass

      def is_connected(self):
        return self._is_connected

      def ping(self):
        return True

      @property
      def backend_type(self):
        return BackendType.REDIS

      def store(self, key, data, ttl=None):
        pass

      def retrieve(self, key):
        return None

      def delete(self, key):
        return True

      def exists(self, key):
        return True

      def ttl(self, key):
        return None

      def clear_storage(self, prefix=None):
        pass

    mock_backend = MockStorageBackend()

    manager = ConnectionManager(BackendType.REDIS)
    manager._backend = mock_backend

    result = manager.get_storage_backend()

    assert result is mock_backend


class TestConnectionManagerSingleton:
  """Tests for singleton pattern."""

  def test_get_manager_same_instance_same_params(self):
    """Test get_manager returns same instance for same backend_type and settings."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS)

    assert manager1 is manager2

  def test_get_manager_different_instance_different_backend_type(self):
    """Test get_manager returns different instance for different backend types."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS)
    manager2 = ConnectionManager.get_manager(BackendType.MONGODB)

    assert manager1 is not manager2

  def test_get_manager_different_instance_different_settings(self):
    """Test get_manager returns different instance for different settings."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "localhost"})
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "otherhost"})

    assert manager1 is not manager2


class TestConnectionManagerCreateBackendAllTypes:
  """Regression: _create_backend must support ALL 10 BackendType values.

  Previously only 6 (REDIS, MONGODB, KAFKA, RABBITMQ, ELASTICSEARCH, ROCKETMQ)
  were handled; PULSAR, SQS, MEMCACHED, DYNAMODB fell through to
  ``case _: raise ValueError("Unsupported backend type")`` despite being
  listed in the QUEUE_CAPABLE / STORAGE_CAPABLE sets and the README's
  "10 Backends" claim. Configuring them via standard Scrapy settings routed
  through ConnectionManager and crashed.
  """

  @pytest.fixture(autouse=True)
  def _clear_registry_between_tests(self):
    """Ensure managers do not leak across parametrized invocations."""
    ConnectionManager.clear_registry()
    yield
    ConnectionManager.clear_registry()

  @pytest.mark.parametrize(
    "backend_type",
    list(BackendType),
    ids=[bt.name for bt in BackendType],
  )
  def test_create_backend_supports_all_backend_types(self, backend_type):
    """Every BackendType must build via _create_backend without ValueError.

    SDKs absent from the test env are stubbed at module top. We only exercise
    construction — connect() is never called (no real services).
    """
    manager = ConnectionManager.get_manager(backend_type=backend_type, settings={})
    backend = manager._create_backend()

    assert backend.backend_type is backend_type
    # Pin the per-case wiring: the right concrete class must be constructed,
    # not merely one whose backend_type property matches (guards against a
    # Backend/Settings swap inside a match arm).
    assert type(backend).__name__ == _EXPECTED_BACKEND_CLASS[backend_type]

  @pytest.mark.parametrize(
    "backend_type",
    [
      BackendType.PULSAR,
      BackendType.SQS,
      BackendType.MEMCACHED,
      BackendType.DYNAMODB,
    ],
    ids=["pulsar", "sqs", "memcached", "dynamodb"],
  )
  def test_create_backend_regression_for_four_new_backends(self, backend_type):
    """Direct regression for the P0 bug: each of the 4 newly-added backends
    must build via _create_backend (previously raised ValueError)."""
    manager = ConnectionManager.get_manager(backend_type=backend_type, settings={})
    backend = manager._create_backend()

    assert backend.backend_type is backend_type
    # Pin the per-case wiring: the right concrete class must be constructed,
    # not merely one whose backend_type property matches (guards against a
    # Backend/Settings swap inside a match arm).
    assert type(backend).__name__ == _EXPECTED_BACKEND_CLASS[backend_type]


class TestResolveBackendConfigEnumNormalization:
  """A3: ``resolve_backend_config`` must not crash on programmatic enum values.

  Previously ``BackendType(per_component_type)`` raised ``ValueError`` if the
  value was an invalid string OR a value that ``BackendType.__call__`` could
  not coerce (e.g. an int passed by a programmatic caller). The crash
  surfaced as an untyped ``ValueError`` deep in ``from_settings`` instead of
  a ``ConfigurationError`` with the offending setting name + value attached.
  """

  @staticmethod
  def _make_settings(values: dict[str, object]) -> MagicMock:
    """Build a Scrapy-settings-like stub returning per-key values.

    ``resolve_backend_config`` calls ``settings.get(key)`` and
    ``settings.getdict(key, default)``; a ``MagicMock`` spec'd to a dict-like
    gives predictable per-key behavior without dragging in scrapy.Settings.
    """
    settings = MagicMock()

    def _get(key, default=None):
      return values.get(key, default)

    def _getdict(key, default=None):
      v = values.get(key, default)
      if v is None:
        return {}
      return dict(v)

    settings.get.side_effect = _get
    settings.getdict.side_effect = _getdict
    return settings

  def test_enum_instance_passthrough_per_component(self):
    """A BackendType instance passed as the per-component value must resolve
    to its registry-key string (no ValueError, no re-coercion crash).

    Round-5 R5-1: ``resolve_backend_config`` now returns the backend-type
    STRING (was the ``BackendType`` member). ``BackendType.MONGODB`` →
    ``"mongodb"`` — the same registry key the descriptor table uses.
    """
    from scrapy_extension.backends.connectors import resolve_backend_config

    settings = self._make_settings(
      {"SCRAPY_QUEUE_BACKEND_TYPE": BackendType.MONGODB}
    )
    backend_type, _ = resolve_backend_config(
      settings, "SCRAPY_QUEUE_BACKEND_TYPE", "SCRAPY_QUEUE_BACKEND_SETTINGS"
    )
    assert backend_type == "mongodb"

  def test_string_value_resolves_per_component(self):
    """A plain string (the typical Scrapy settings path) must still resolve
    to the registry-key string unchanged."""
    from scrapy_extension.backends.connectors import resolve_backend_config

    settings = self._make_settings({"SCRAPY_QUEUE_BACKEND_TYPE": "kafka"})
    backend_type, _ = resolve_backend_config(
      settings, "SCRAPY_QUEUE_BACKEND_TYPE", "SCRAPY_QUEUE_BACKEND_SETTINGS"
    )
    assert backend_type == "kafka"

  def test_invalid_string_raises_configuration_error(self):
    """An invalid backend type string must raise ``ConfigurationError``
    (not bare ``ValueError``) so the caller sees a typed error with the
    offending setting name + value attached."""
    from scrapy_extension.backends.connectors import resolve_backend_config
    from scrapy_extension.exceptions import ConfigurationError

    settings = self._make_settings({"SCRAPY_QUEUE_BACKEND_TYPE": "not-a-backend"})
    with pytest.raises(ConfigurationError) as exc_info:
      resolve_backend_config(
        settings, "SCRAPY_QUEUE_BACKEND_TYPE", "SCRAPY_QUEUE_BACKEND_SETTINGS"
      )
    assert exc_info.value.setting_name == "SCRAPY_QUEUE_BACKEND_TYPE"
    assert exc_info.value.setting_value == "not-a-backend"

  def test_invalid_global_raises_configuration_error(self):
    """Same normalization on the GLOBAL fallback path
    (``SCRAPY_BACKEND_TYPE``)."""
    from scrapy_extension.backends.connectors import resolve_backend_config
    from scrapy_extension.exceptions import ConfigurationError

    settings = self._make_settings({"SCRAPY_BACKEND_TYPE": "bogus"})
    with pytest.raises(ConfigurationError) as exc_info:
      resolve_backend_config(
        settings, "SCRAPY_QUEUE_BACKEND_TYPE", "SCRAPY_QUEUE_BACKEND_SETTINGS"
      )
    assert exc_info.value.setting_name == "SCRAPY_BACKEND_TYPE"
    assert exc_info.value.setting_value == "bogus"

  def test_non_string_value_raises_configuration_error(self):
    """A non-string, non-enum value (e.g. an int from a programmatic caller)
    must raise ``ConfigurationError`` rather than the raw ``ValueError``
    that ``BackendType(123)`` produces internally."""
    from scrapy_extension.backends.connectors import resolve_backend_config
    from scrapy_extension.exceptions import ConfigurationError

    settings = self._make_settings({"SCRAPY_QUEUE_BACKEND_TYPE": 123})
    with pytest.raises(ConfigurationError) as exc_info:
      resolve_backend_config(
        settings, "SCRAPY_QUEUE_BACKEND_TYPE", "SCRAPY_QUEUE_BACKEND_SETTINGS"
      )
    assert exc_info.value.setting_name == "SCRAPY_QUEUE_BACKEND_TYPE"


class TestConnectionManagerRefcount:
  """A1: shared ConnectionManager.close() must refcount co-located holders.

  When two components (scheduler queue + dupefilter) resolve to the same
  ``backend_type:settings_hash`` registry key, ``get_manager()`` returns the
  SAME instance. The old ``close()`` unconditionally disconnected + evicted,
  so the first component to close tore the connection out from under the
  other during shutdown.
  """

  def test_get_manager_acquires_refcount(self):
    """Each ``get_manager()`` call for the same key must increment the
    shared manager's refcount (one acquire per get)."""
    manager = ConnectionManager.get_manager(BackendType.REDIS)
    assert manager._users == 1
    manager2 = ConnectionManager.get_manager(BackendType.REDIS)
    assert manager is manager2
    assert manager._users == 2

  def test_close_last_holder_evicts_and_reconnects_fresh(self, mocker):
    """Closing the LAST holder evicts the registry entry; a subsequent
    ``get_manager(same key)`` returns a FRESH manager (different ``id``)
    that reconnects from scratch."""
    mocker.patch.object(ConnectionManager, "_create_backend")
    mock_backend = mocker.MagicMock()
    ConnectionManager._create_backend.return_value = mock_backend

    manager = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "reconnect-test"}
    )
    manager._backend = mock_backend
    first_id = id(manager)

    manager.close()

    # Backend was disconnected and _backend cleared.
    mock_backend.disconnect.assert_called_once()
    assert manager._backend is None

    # Registry evicted → fresh manager on next get.
    manager_after = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "reconnect-test"}
    )
    assert id(manager_after) != first_id
    assert manager_after is not manager

  def test_colocated_close_one_keeps_backend_alive(self, mocker):
    """Two holders share one manager. Closing ONE must NOT disconnect the
    backend or evict the registry — the other holder still needs it."""
    mocker.patch.object(ConnectionManager, "_create_backend")
    mock_backend = mocker.MagicMock()
    ConnectionManager._create_backend.return_value = mock_backend

    holder_a = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "colocated"}
    )
    holder_b = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "colocated"}
    )
    assert holder_a is holder_b
    assert holder_a._users == 2

    holder_a._backend = mock_backend

    # First close — must NOT tear down.
    holder_a.close()

    mock_backend.disconnect.assert_not_called()
    assert holder_a._backend is mock_backend  # backend still wired
    # Registry still holds the manager.
    key = ConnectionManager._registry_key(
      BackendType.REDIS, {"host": "colocated"}
    )
    assert key in ConnectionManager._managers
    # Refcount decremented to the remaining holder.
    assert holder_b._users == 1

  def test_colocated_close_both_disconnects_and_evicts(self, mocker):
    """Closing BOTH holders (last one out) disconnects the backend AND
    evicts the registry entry."""
    mocker.patch.object(ConnectionManager, "_create_backend")
    mock_backend = mocker.MagicMock()
    ConnectionManager._create_backend.return_value = mock_backend

    holder_a = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "colocated-both"}
    )
    holder_b = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "colocated-both"}
    )
    holder_a._backend = mock_backend

    holder_a.close()
    holder_b.close()

    mock_backend.disconnect.assert_called_once()
    key = ConnectionManager._registry_key(
      BackendType.REDIS, {"host": "colocated-both"}
    )
    assert key not in ConnectionManager._managers

  def test_concurrent_get_manager_same_key_one_shared_instance(self):
    """Under concurrency, N threads hitting the same registry key must
    resolve to exactly ONE shared manager (no registry race creating
    duplicates) with refcount == N."""
    import threading

    results: list[ConnectionManager] = []
    errors: list[BaseException] = []
    n = 20
    barrier = threading.Barrier(n)

    def worker():
      try:
        barrier.wait()
        m = ConnectionManager.get_manager(
          BackendType.REDIS, {"host": "concurrent-shared"}
        )
        results.append(m)
      except BaseException as e:  # noqa: BLE001 - surface any failure
        errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert errors == []
    assert len(results) == n
    first = results[0]
    assert all(r is first for r in results)
    assert first._users == n

  def test_concurrent_get_manager_distinct_keys_n_instances(self):
    """N threads with distinct keys → N distinct managers, each refcount 1."""
    import threading

    results: list[ConnectionManager] = []
    errors: list[BaseException] = []
    n = 10
    barrier = threading.Barrier(n)

    def worker(i: int):
      try:
        barrier.wait()
        m = ConnectionManager.get_manager(
          BackendType.REDIS, {"host": f"concurrent-distinct-{i}"}
        )
        results.append(m)
      except BaseException as e:  # noqa: BLE001
        errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert errors == []
    assert len(results) == n
    assert len({id(r) for r in results}) == n
    assert all(r._users == 1 for r in results)


# ---------------------------------------------------------------------------
# Circuit-breaker wiring (Unit J): default-off is byte-identical, opt-in
# wraps hot-path ops and fail-fasts with BackendError when OPEN.
# ---------------------------------------------------------------------------


class TestCircuitBreakerWiringDefaultOff:
  """When ``SCRAPY_CIRCUIT_BREAKER_ENABLED`` is unset, backends are unwrapped.

  The default path must return the raw backend instance with zero overhead
  and byte-identical behavior — no proxy, no breaker, no behavior change.
  """

  def test_default_returns_raw_backend_unwrapped(self, monkeypatch):
    # Ensure the env var is unset so the lazy builder resolves to disabled.
    monkeypatch.delenv("SCRAPY_CIRCUIT_BREAKER_ENABLED", raising=False)
    ConnectionManager.clear_registry()

    manager = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "breaker-default-off"}
    )
    try:
      # Force the lazy breaker resolution before touching .backend so we
      # don't depend on a real Redis connection for this assertion.
      assert manager._get_breaker() is None
      # _breaker_configured is sticky-True after first resolution; the value
      # is what matters — None means disabled.
      assert manager._breaker is None
    finally:
      manager.close()
      ConnectionManager.clear_registry()

  def test_disabled_get_queue_backend_returns_identity(self, monkeypatch):
    """Disabled path returns the SAME object the backend property yields.

    We bypass the real connect by stubbing ``backend`` to return a fake and
    asserting ``get_queue_backend()`` returns it unchanged (``is``).
    """
    monkeypatch.delenv("SCRAPY_CIRCUIT_BREAKER_ENABLED", raising=False)
    ConnectionManager.clear_registry()

    manager = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "breaker-identity"}
    )
    try:
      fake_qb = _FakeRedisQueueBackend()
      # Bypass connect: assign the backend directly.
      manager._backend = fake_qb
      assert manager.get_queue_backend() is fake_qb
    finally:
      manager.close()
      ConnectionManager.clear_registry()


class TestCircuitBreakerWiringEnabled:
  """When the breaker is enabled, hot-path ops wrap + OPEN fail-fast."""

  def test_enabled_wraps_queue_hot_path_and_open_failfast(self, monkeypatch):
    monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_ENABLED", "true")
    monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
    ConnectionManager.clear_registry()

    manager = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "breaker-on"}
    )
    try:
      fake_qb = _FailingRedisQueueBackend()
      manager._backend = fake_qb
      wrapped = manager.get_queue_backend()
      # Wrapped is a proxy, NOT the raw backend.
      assert wrapped is not fake_qb

      # Trip the breaker via the wrapped hot-path op.
      with pytest.raises(RuntimeError):
        wrapped.push("q", b"x")

      from scrapy_extension.backends.circuit_breaker import (
        CircuitBreakerOpenError,
      )

      # Now OPEN — a subsequent push must fail-fast with BackendError subclass.
      with pytest.raises(CircuitBreakerOpenError):
        wrapped.push("q", b"x")
    finally:
      manager.close()
      ConnectionManager.clear_registry()

  def test_enabled_non_network_methods_not_blocked(self, monkeypatch):
    monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_ENABLED", "true")
    monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
    ConnectionManager.clear_registry()

    manager = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "breaker-on-nonblock"}
    )
    try:
      fake_qb = _FailingRedisQueueBackend()
      manager._backend = fake_qb
      wrapped = manager.get_queue_backend()

      # Trip via push.
      with pytest.raises(RuntimeError):
        wrapped.push("q", b"x")

      # Non-network methods still work — they're forwarded, not breaker-wrapped.
      wrapped.clear_queue("q")
      assert fake_qb.clear_calls == 1
      # is_connected forwards too.
      assert wrapped.is_connected() is True
    finally:
      manager.close()
      ConnectionManager.clear_registry()

  def test_single_breaker_shared_across_interfaces(self, monkeypatch):
    """Queue+set+storage on one manager share a single breaker instance.

    A failure on the queue hot-path trips the shared breaker so the storage
    interface also rejects — they share the failure signal.
    """
    monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_ENABLED", "true")
    monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
    ConnectionManager.clear_registry()

    manager = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "breaker-shared"}
    )
    try:
      fake = _FakeRedisAllBackend()
      manager._backend = fake
      qb = manager.get_queue_backend()
      # Trip via queue.
      fake.push = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x"))
      # Re-wrap to capture the failing push (proxy snapshots at construction).
      qb = manager.get_queue_backend()
      with pytest.raises(RuntimeError):
        qb.push("q", b"x")
      # The shared breaker is now OPEN.
      assert manager._breaker is not None
      assert manager._breaker.state.value == "open"
    finally:
      manager.close()
      ConnectionManager.clear_registry()


# ---------------------------------------------------------------------------
# Fakes for breaker-wiring tests — real backends need a live service; these
# stubs satisfy the interface ABCs so we can drive the proxy without a broker.
# ---------------------------------------------------------------------------


class _FakeRedisQueueBackend(QueueBackend):
  def __init__(self) -> None:
    self.clear_calls = 0
    self.ack_calls = 0

  def connect(self) -> None: ...
  def disconnect(self) -> None: ...
  def is_connected(self) -> bool:
    return True

  def ping(self) -> bool:
    return True

  @property
  def backend_type(self):
    return BackendType.REDIS

  def push(self, queue_name, item, priority=0.0) -> None: ...
  def pop(self, queue_name, timeout=0.0):
    return None

  def queue_len(self, queue_name) -> int:
    return 0

  def clear_queue(self, queue_name) -> None:
    self.clear_calls += 1

  def ack(self, queue_name) -> None:
    self.ack_calls += 1


class _FailingRedisQueueBackend(_FakeRedisQueueBackend):
  def push(self, queue_name, item, priority=0.0) -> None:
    raise RuntimeError("backend on fire")


class _FakeRedisAllBackend(QueueBackend, SetBackend, StorageBackend):
  def __init__(self) -> None:
    self.clear_calls = 0

  def connect(self) -> None: ...
  def disconnect(self) -> None: ...
  def is_connected(self) -> bool:
    return True

  def ping(self) -> bool:
    return True

  @property
  def backend_type(self):
    return BackendType.REDIS

  # Queue
  def push(self, queue_name, item, priority=0.0) -> None: ...
  def pop(self, queue_name, timeout=0.0):
    return None

  def queue_len(self, queue_name) -> int:
    return 0

  def clear_queue(self, queue_name) -> None:
    self.clear_calls += 1

  def ack(self, queue_name) -> None: ...
  def nack(self, queue_name) -> None: ...
  # Set
  def add(self, set_name, item) -> bool:
    return True

  def remove(self, set_name, item) -> bool:
    return False

  def contains(self, set_name, item) -> bool:
    return False

  def set_len(self, set_name) -> int:
    return 0

  def clear_set(self, set_name) -> None: ...
  # Storage
  def store(self, key, data, ttl=None) -> None: ...
  def retrieve(self, key):
    return None

  def delete(self, key) -> bool:
    return False

  def exists(self, key) -> bool:
    return False

  def ttl(self, key):
    return None

  def clear_storage(self, prefix=None) -> None: ...

