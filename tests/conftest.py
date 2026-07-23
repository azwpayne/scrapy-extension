"""Pytest fixtures for scrapy-extension tests."""

import pytest


@pytest.fixture
def mock_redis_client(mocker):
  """Create a mock Redis client."""
  return mocker.Mock()


@pytest.fixture
def mock_connection_manager(mocker):
  """Create a mock connection manager.

  The ``push_queue_with_durability`` double mirrors the real
  ``ConnectionManager._push_queue_with_durability`` contract: it classifies a
  push as durable iff ``manager.push_is_durable`` is True (default True — these
  mocks stand in for real durable backends) and raises ``_DurablePushRequired``
  when a durable push is required of a non-durable backend. Without this the
  fixture silently returned ``worker_crash_durable=True`` regardless of
  ``require_durable``, masking any test trying to exercise the volatile
  (non-durable) push path. Set ``manager.push_is_durable = False`` to opt in.
  """
  from scrapy_extension.backends.base import (
    _DurablePushRequired,
    _QueuePushReceipt,
  )

  manager = mocker.MagicMock()
  queue_backend = mocker.Mock()
  manager.get_queue_backend.return_value = queue_backend
  manager.get_set_backend.return_value = mocker.Mock()
  manager.get_storage_backend.return_value = mocker.Mock()
  manager.push_is_durable = True

  def push_queue_with_durability(
    queue_name,
    item,
    priority=0.0,
    *,
    require_durable=False,
  ):
    durable = manager.push_is_durable is True
    if require_durable and not durable:
      raise _DurablePushRequired
    queue_backend.push(queue_name, item, priority)
    return _QueuePushReceipt(worker_crash_durable=durable)

  manager._push_queue_with_durability.side_effect = push_queue_with_durability
  return manager


@pytest.fixture
def mock_spider(mocker):
  """Create a mock Scrapy spider for BackendQueue callback resolution."""
  from scrapy import Spider

  return mocker.MagicMock(spec=Spider)


@pytest.fixture
def sample_request():
  """Create a sample Scrapy request."""
  from scrapy.http import Request

  return Request(url="https://example.com")


@pytest.fixture
def sample_item():
  """Create a sample Scrapy item."""
  return {"name": "Test Item", "value": 123}


@pytest.fixture(autouse=True)
def _isolate_connection_manager_registry():
  """Auto-clear the ConnectionManager class-level registry before each test.

  ``ConnectionManager._managers`` is a process-global dict; tests that reach
  ``get_manager()`` (via the ``from_settings`` / ``from_crawler`` factories on
  the scheduler / pipeline / dupefilter) populate it. Without clearing,
  managers leak across tests — the cross-test pollution R1-P1-8/R8 warned
  about. ``clear_registry()`` existed but was only invoked inside its own
  self-test, so the isolation it was built for wasn't actually applied.
  """
  from scrapy_extension.backends.connectors import ConnectionManager

  ConnectionManager.clear_registry()
  yield


@pytest.fixture(autouse=True)
def _rabbitmq_test_credentials(monkeypatch):
  """Provide RabbitMQ creds via env so bare ``RabbitMQSettings()`` constructs in tests.

  Production still requires explicit credentials (``settings/rabbitmq.py`` has no
  default for ``username``/``password`` — the C2 security fix). This fixture only
  restores pre-C2 test convenience for the ~60 backend tests that mock pika and
  never cared about creds. Tests asserting the required-creds contract must
  ``monkeypatch.delenv`` these two variables.
  """
  monkeypatch.setenv("SCRAPY_RABBITMQ_USERNAME", "guest")
  monkeypatch.setenv("SCRAPY_RABBITMQ_PASSWORD", "guest")
