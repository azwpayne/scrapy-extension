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


def test_mongodb_backend_push_pop():
    """Test MongoDB backend push and pop operations."""
    from scrapy_extension.backends.mongodb_backend import MongoDBBackend
    from scrapy_extension.config.settings import MongoDBSettings

    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._queue_collection = mock_collection

        # Test push
        backend.push("test_queue", b"test_item", priority=1.0)
        mock_collection.insert_one.assert_called_once()
        call_args = mock_collection.insert_one.call_args[0][0]
        assert call_args["queue_name"] == "test_queue"
        assert call_args["item"] == b"test_item"
        assert call_args["priority"] == -1.0  # Negated

        # Test pop
        mock_collection.find_one_and_delete.return_value = {
            "queue_name": "test_queue",
            "item": b"test_item",
        }
        result = backend.pop("test_queue")
        assert result == b"test_item"


def test_mongodb_backend_queue_len():
    """Test MongoDB backend queue length."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._queue_collection = mock_collection
        mock_collection.count_documents.return_value = 5

        result = backend.queue_len("test_queue")
        assert result == 5
        mock_collection.count_documents.assert_called_once_with({"queue_name": "test_queue"})


def test_mongodb_backend_clear_queue():
    """Test MongoDB backend clear queue."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._queue_collection = mock_collection

        backend.clear_queue("test_queue")
        mock_collection.delete_many.assert_called_once_with({"queue_name": "test_queue"})


def test_mongodb_backend_set_operations():
    """Test MongoDB backend set operations."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._set_collection = mock_collection

        # Test add
        mock_collection.insert_one.return_value = MagicMock()
        result = backend.add("test_set", b"test_item")
        assert result is True
        mock_collection.insert_one.assert_called_once()

        # Test contains (item exists)
        mock_collection.find_one.return_value = {"set_name": "test_set", "item_hash": "abc123"}
        result = backend.contains("test_set", b"test_item")
        assert result is True

        # Test contains (item not exists)
        mock_collection.find_one.return_value = None
        result = backend.contains("test_set", b"other_item")
        assert result is False


def test_mongodb_backend_set_remove():
    """Test MongoDB backend set remove."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._set_collection = mock_collection

        # Test remove success
        mock_delete_result = MagicMock()
        mock_delete_result.deleted_count = 1
        mock_collection.delete_one.return_value = mock_delete_result
        result = backend.remove("test_set", b"test_item")
        assert result is True

        # Test remove failure (not found)
        mock_delete_result.deleted_count = 0
        result = backend.remove("test_set", b"missing_item")
        assert result is False


def test_mongodb_backend_set_len():
    """Test MongoDB backend set length."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._set_collection = mock_collection
        mock_collection.count_documents.return_value = 3

        result = backend.set_len("test_set")
        assert result == 3


def test_mongodb_backend_clear_set():
    """Test MongoDB backend clear set."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)

    with patch("scrapy_extension.backends.mongodb_backend.MongoClient"):
        backend.connect()
        mock_collection = MagicMock()
        backend._set_collection = mock_collection

        backend.clear_set("test_set")
        mock_collection.delete_many.assert_called_once_with({"set_name": "test_set"})
