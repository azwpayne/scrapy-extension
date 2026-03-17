"""Pytest fixtures for scrapy-extension tests."""

from unittest.mock import MagicMock, Mock

import pytest


@pytest.fixture
def mock_redis_client():
  """Create a mock Redis client."""
  return Mock()


@pytest.fixture
def mock_connection_manager():
  """Create a mock connection manager."""
  manager = MagicMock()
  manager.get_queue_backend.return_value = Mock()
  manager.get_set_backend.return_value = Mock()
  manager.get_storage_backend.return_value = Mock()
  return manager


@pytest.fixture
def sample_request():
  """Create a sample Scrapy request."""
  from scrapy.http import Request

  return Request(url="https://example.com")


@pytest.fixture
def sample_item():
  """Create a sample Scrapy item."""
  return {"name": "Test Item", "value": 123}
