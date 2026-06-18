"""Pytest fixtures for scrapy-extension tests."""

import pytest


@pytest.fixture
def mock_redis_client(mocker):
  """Create a mock Redis client."""
  return mocker.Mock()


@pytest.fixture
def mock_connection_manager(mocker):
  """Create a mock connection manager."""
  manager = mocker.MagicMock()
  manager.get_queue_backend.return_value = mocker.Mock()
  manager.get_set_backend.return_value = mocker.Mock()
  manager.get_storage_backend.return_value = mocker.Mock()
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
