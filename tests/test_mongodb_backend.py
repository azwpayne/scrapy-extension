import pytest
from unittest.mock import MagicMock, patch
from scrapy_extension.backends.mongodb_backend import MongoDBBackend
from scrapy_extension.config.settings import MongoDBSettings


def test_mongodb_backend_connect():
    """Test MongoDB backend connection."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        backend.connect()

        mock_client.assert_called_once()
        mock_instance.admin.command.assert_called_once_with("ping")
        assert backend.is_connected()


def test_mongodb_backend_disconnect():
    """Test MongoDB backend disconnection."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        backend.connect()
        assert backend.is_connected()

        backend.disconnect()
        assert not backend.is_connected()
        mock_instance.close.assert_called_once()
