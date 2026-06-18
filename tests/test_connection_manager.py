"""Tests for connection manager."""

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager


def test_connection_manager_get_manager_singleton():
  """Test that get_manager returns singleton for same params."""
  manager1 = ConnectionManager.get_manager(BackendType.REDIS)
  manager2 = ConnectionManager.get_manager(BackendType.REDIS)
  assert manager1 is manager2


def test_connection_manager_close_evicts_from_registry():
  """R1-P1-8: close() must remove the manager from the class-level registry.

  Without eviction, get_manager returns the closed instance on the next call
  — masking state across reconnect cycles and across tests.
  """
  manager = ConnectionManager.get_manager(
    BackendType.REDIS, {"host": "close-test-host"}
  )
  assert manager.settings == {"host": "close-test-host"}

  manager.close()

  # Registry no longer contains the key; a new get_manager creates a fresh instance.
  manager_after = ConnectionManager.get_manager(
    BackendType.REDIS, {"host": "close-test-host"}
  )
  assert manager_after is not manager


def test_connection_manager_clear_registry():
  """R1-P1-8: clear_registry() wipes all managers — for test isolation."""
  ConnectionManager.get_manager(BackendType.REDIS, {"host": "h1"})
  ConnectionManager.get_manager(BackendType.REDIS, {"host": "h2"})
  assert len(ConnectionManager._managers) >= 2

  ConnectionManager.clear_registry()

  assert ConnectionManager._managers == {}


def test_connection_manager_different_params():
  """Test that different params return different managers."""
  manager1 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "localhost"})
  manager2 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "other"})
  assert manager1 is not manager2


def test_connection_manager_create_mongodb_backend(mocker):
  """Test ConnectionManager creates MongoDB backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.mongodb.MongoDBBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.MONGODB)
  backend = manager._create_backend()

  mock_backend.assert_called_once()
  assert backend == mock_instance


def test_connection_manager_create_kafka_backend(mocker):
  """Test ConnectionManager creates Kafka backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.kafka.KafkaBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.KAFKA)
  backend = manager._create_backend()

  mock_backend.assert_called_once()


def test_connection_manager_create_rabbitmq_backend(mocker):
  """Test ConnectionManager creates RabbitMQ backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.rabbitmq.RabbitMQBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.RABBITMQ)
  backend = manager._create_backend()

  mock_backend.assert_called_once()


def test_connection_manager_get_manager_same_settings_order():
  """Same settings with different key order should resolve to same manager."""
  settings_a = {"a": 1, "b": 2}
  settings_b = {"b": 2, "a": 1}

  manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings_a)
  manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings_b)

  assert manager1 is manager2


def test_connection_manager_get_set_backend_not_supported(mocker):
  """get_set_backend should raise NotImplementedError for unsupported backend."""
  manager = ConnectionManager(BackendType.KAFKA)
  # We need to set _backend to something that is not a SetBackend but is a Backend subclass
  mock_backend = mocker.MagicMock()
  mock_backend.is_connected.return_value = True
  manager._backend = mock_backend

  with pytest.raises(NotImplementedError):
    manager.get_set_backend()


def test_attempt_connection_calls_disconnect_on_failure(mocker):
  """R25-A1: failed connect() must release backend resources (pools, sockets).

  Without this guard, each retry leaks one Redis/MongoDB connection pool.
  RedisBackend.connect() allocates the client (and its pool) at line 150,
  then pings at line 151. A ping failure leaves ``self._client`` holding
  an orphaned pool. On retry, ConnectionManager creates a NEW backend
  with a NEW pool; the old one is garbage-collected without ``close()``,
  leaking the pool until the GC finalizer runs (which redis-py doesn't
  guarantee promptly).
  """
  manager = ConnectionManager(BackendType.REDIS)

  mock_backend = mocker.MagicMock()
  mock_backend.connect.side_effect = ConnectionError("ping failed")
  mocker.patch.object(manager, "_create_backend", return_value=mock_backend)

  with pytest.raises(ConnectionError):
    manager._attempt_connection()

  mock_backend.connect.assert_called_once()
  mock_backend.disconnect.assert_called_once()


def test_attempt_connection_disconnect_failure_is_swallowed(mocker):
  """R25-A1: cleanup failures during connect-failure path must not mask the original error.

  If backend.disconnect() itself raises (e.g., broken pipe on attempted
  close), we should still propagate the original connect error, not the
  cleanup error. The operator needs to know the connect failed, not that
  cleanup also failed.
  """
  manager = ConnectionManager(BackendType.REDIS)

  mock_backend = mocker.MagicMock()
  mock_backend.connect.side_effect = ConnectionError("original connect failure")
  mock_backend.disconnect.side_effect = RuntimeError("cleanup also failed")
  mocker.patch.object(manager, "_create_backend", return_value=mock_backend)

  with pytest.raises(ConnectionError, match="original connect failure"):
    manager._attempt_connection()


def test_close_swallows_backend_disconnect_error_and_still_evicts(mocker):
  """R44-A1: close() must not propagate a backend-specific disconnect error.

  R25-A1 hardened the connect-path's disconnect cleanup with
  ``contextlib.suppress(Exception)`` because disconnecting a possibly-broken
  backend can raise anything (OSError from the socket layer, a
  backend-specific error the backend's own disconnect didn't swallow).
  ``close()`` faced the identical scenario but caught only
  ``(RuntimeError, ValueError, AttributeError)``. An ``OSError`` (or any
  backend exception outside that tuple) propagated out of close(), skipped
  the registry-eviction code that runs after the try/finally, and broke the
  caller's close chain (scheduler.close, _on_spider_closed). Now catches
  ``Exception`` so close() always completes cleanup — matching R25-A1.
  """
  # Register via get_manager so the eviction branch is exercisable. Unique
  # host isolates this test's registry key from other tests.
  manager = ConnectionManager.get_manager(
    BackendType.REDIS, {"host": "r44-close-error-test"}
  )

  mock_backend = mocker.MagicMock()
  # OSError is NOT a subclass of (RuntimeError, ValueError, AttributeError),
  # so the old narrow tuple would let it propagate out of close().
  mock_backend.disconnect.side_effect = OSError("broken pipe during close")
  manager._backend = mock_backend

  # Must not raise.
  manager.close()

  # Cleanup completed despite the disconnect error.
  assert manager._backend is None
  # Registry evicted even though disconnect raised (the code path after the
  # try/finally — the part the old bug skipped).
  key = ConnectionManager._registry_key(
    BackendType.REDIS, {"host": "r44-close-error-test"}
  )
  assert key not in ConnectionManager._managers

