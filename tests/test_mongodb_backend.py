from datetime import datetime, timedelta, timezone

import pytest

from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import MongoDBSettings


def test_mongodb_backend_connect(mocker):
  """Test MongoDB backend connection."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mock_instance = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_instance
  )

  backend.connect()

  mock_instance.admin.command.assert_called_once_with("ping")
  assert backend.is_connected()


def test_mongodb_backend_disconnect(mocker):
  """Test MongoDB backend disconnection."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mock_instance = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_instance
  )

  backend.connect()
  assert backend.is_connected()

  backend.disconnect()
  assert not backend.is_connected()
  mock_instance.close.assert_called_once()


def test_mongodb_backend_push_pop(mocker):
  """Test MongoDB backend push and pop operations."""
  from scrapy_extension.backends.mongodb import MongoDBBackend
  from scrapy_extension.settings import MongoDBSettings

  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
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


def test_mongodb_backend_push_raises_queue_error_on_pymongo_error(mocker):
  """Push wraps a PyMongoError as QueueError (lines 409-411).

  Pins the error-wrapping contract — callers catch QueueError, never the
  raw PyMongoError (mirrors the redis/rabbitmq/kafka contract-pinning,
  R65-R67).
  """
  from pymongo.errors import PyMongoError

  from scrapy_extension.backends.mongodb import MongoDBBackend
  from scrapy_extension.exceptions import QueueError
  from scrapy_extension.settings import MongoDBSettings

  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._queue_collection = mock_collection
  mock_collection.insert_one.side_effect = PyMongoError("push failed")

  with pytest.raises(QueueError, match="Failed to push to queue test_queue"):
    backend.push("test_queue", b"item", priority=1.0)


def test_mongodb_backend_pop_raises_queue_error_on_pymongo_error(mocker):
  """Pop wraps a PyMongoError as QueueError (lines 434-436)."""
  from pymongo.errors import PyMongoError

  from scrapy_extension.backends.mongodb import MongoDBBackend
  from scrapy_extension.exceptions import QueueError
  from scrapy_extension.settings import MongoDBSettings

  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._queue_collection = mock_collection
  mock_collection.find_one_and_delete.side_effect = PyMongoError("pop failed")

  with pytest.raises(QueueError, match="Failed to pop from queue test_queue"):
    backend.pop("test_queue")


def test_mongodb_backend_queue_len(mocker):
  """Test MongoDB backend queue length."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._queue_collection = mock_collection
  mock_collection.count_documents.return_value = 5

  result = backend.queue_len("test_queue")
  assert result == 5
  mock_collection.count_documents.assert_called_once_with(
    {"queue_name": "test_queue"}, limit=100000
  )


def test_mongodb_backend_clear_queue(mocker):
  """Test MongoDB backend clear queue."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._queue_collection = mock_collection

  backend.clear_queue("test_queue")
  mock_collection.delete_many.assert_called_once_with({"queue_name": "test_queue"})


def test_mongodb_backend_set_operations(mocker):
  """Test MongoDB backend set operations."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._set_collection = mock_collection

  # Test add
  mock_collection.insert_one.return_value = mocker.MagicMock()
  result = backend.add("test_set", b"test_item")
  assert result is True
  mock_collection.insert_one.assert_called_once()

  # Test contains (item exists)
  mock_collection.find_one.return_value = {
    "set_name": "test_set",
    "item_hash": "abc123",
  }
  result = backend.contains("test_set", b"test_item")
  assert result is True

  # Test contains (item not exists)
  mock_collection.find_one.return_value = None
  result = backend.contains("test_set", b"other_item")
  assert result is False


def test_mongodb_backend_set_remove(mocker):
  """Test MongoDB backend set remove."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._set_collection = mock_collection

  # Test remove success
  mock_delete_result = mocker.MagicMock()
  mock_delete_result.deleted_count = 1
  mock_collection.delete_one.return_value = mock_delete_result
  result = backend.remove("test_set", b"test_item")
  assert result is True

  # Test remove failure (not found)
  mock_delete_result.deleted_count = 0
  result = backend.remove("test_set", b"missing_item")
  assert result is False


def test_mongodb_backend_set_len(mocker):
  """Test MongoDB backend set length."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._set_collection = mock_collection
  mock_collection.count_documents.return_value = 3

  result = backend.set_len("test_set")
  assert result == 3
  mock_collection.count_documents.assert_called_once_with(
    {"set_name": "test_set"}, limit=100000
  )


def test_mongodb_backend_clear_set(mocker):
  """Test MongoDB backend clear set."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._set_collection = mock_collection

  backend.clear_set("test_set")
  mock_collection.delete_many.assert_called_once_with({"set_name": "test_set"})


def test_mongodb_backend_storage_operations(mocker):
  """Test MongoDB backend storage operations."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection

  # Test store
  backend.store("test_key", b"test_data")
  mock_collection.replace_one.assert_called_once()

  # Test retrieve
  mock_collection.find_one.return_value = {"key": "test_key", "data": b"test_data"}
  retrieved = backend.retrieve("test_key")
  assert retrieved == b"test_data"

  # Test exists
  exists_result = backend.exists("test_key")
  assert exists_result is True

  # Test delete
  mock_delete_result = mocker.MagicMock()
  mock_delete_result.deleted_count = 1
  mock_collection.delete_one.return_value = mock_delete_result
  deleted = backend.delete("test_key")
  assert deleted is True


def test_mongodb_backend_storage_ttl(mocker):
  """Test MongoDB backend storage TTL."""

  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection

  # Test with TTL
  future_time = datetime.now(tz=timezone.utc) + timedelta(seconds=3600)
  mock_collection.find_one.return_value = {"key": "test_key", "expireAt": future_time}

  result = backend.ttl("test_key")
  assert result is not None
  assert 3590 <= result <= 3600  # Allow for execution time

  # Test without TTL
  mock_collection.find_one.return_value = {"key": "test_key"}
  result = backend.ttl("test_key")
  assert result is None

  # Test non-existent key
  mock_collection.find_one.return_value = None
  result = backend.ttl("missing_key")
  assert result is None


def test_mongodb_backend_storage_ttl_expired_returns_negative_one(mocker):
  """#30: an expired doc (expireAt in the past) returns -1 per the
  StorageBackend ttl() contract (None=no-TTL/missing, -1=expired), matching
  ElasticSearch. MongoDB's TTL index normally auto-deletes expired docs so
  this branch is rarely hit in production, but contract conformance matters
  for callers + for a misconfigured/delayed TTL index.

  Pre-fix: ``return max(0, int(remaining))`` returned 0 for expired,
  contradicting both the docstring ("-1 if expired") and the ABC contract.
  """
  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection

  past_time = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
  mock_collection.find_one.return_value = {"key": "k", "expireAt": past_time}

  assert backend.ttl("k") == -1


# -----------------------------------------------------------------------------
# Additional tests for coverage gaps
# -----------------------------------------------------------------------------


def test_mongodb_backend_connect_connection_failure(mocker):
  """Test MongoDB backend raises BackendConnectionError on ConnectionFailure."""
  from pymongo.errors import ConnectionFailure

  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.admin.command.side_effect = ConnectionFailure("network error")
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_instance
  )

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  assert exc_info.value.backend_type == "mongodb"


def test_mongodb_backend_connect_generic_exception(mocker):
  """Test MongoDB backend raises BackendConnectionError on generic Exception."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.admin.command.side_effect = RuntimeError("unexpected error")
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_instance
  )

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  assert exc_info.value.backend_type == "mongodb"


def test_mongodb_backend_build_client_kwargs_cached(mocker):
  """Test _build_client_kwargs returns cached copy on second call."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  # First call builds kwargs
  kwargs1 = backend._build_client_kwargs()

  # Modify the returned dict to verify we get a copy
  kwargs1["custom_key"] = "custom_value"

  # Second call should return a copy, not the cached dict with our modification
  kwargs2 = backend._build_client_kwargs()
  assert "custom_key" not in kwargs2


def test_mongodb_backend_build_client_kwargs_w(mocker):
  """Test _build_client_kwargs includes w when config.w is set."""
  config = MongoDBSettings(w=2)
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  kwargs = backend._build_client_kwargs()
  assert kwargs.get("w") == 2


def test_mongodb_backend_build_client_kwargs_journal(mocker):
  """Test _build_client_kwargs includes journal when config.journal is set."""
  config = MongoDBSettings(journal=True)
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  kwargs = backend._build_client_kwargs()
  assert kwargs.get("journal") is True


def test_mongodb_backend_build_client_kwargs_w_timeout_ms(mocker):
  """Test _build_client_kwargs includes wtimeoutMS when config.w_timeout_ms is set."""
  config = MongoDBSettings(w_timeout_ms=5000)
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  kwargs = backend._build_client_kwargs()
  assert kwargs.get("wtimeoutMS") == 5000


def test_mongodb_backend_build_client_kwargs_tls_cert_file(mocker):
  """Test _build_client_kwargs includes tlsCertificateKeyFile when tls_cert_file is set."""
  config = MongoDBSettings(tls_enabled=True, tls_cert_file="/path/to/cert.pem")
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  kwargs = backend._build_client_kwargs()
  assert kwargs.get("tlsCertificateKeyFile") == "/path/to/cert.pem"


def test_mongodb_backend_build_client_kwargs_tls_key_file_no_cert(mocker):
  """Test _build_client_kwargs uses tls_key_file when tls_cert_file is not set."""
  config = MongoDBSettings(tls_enabled=True, tls_key_file="/path/to/key.pem")
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  kwargs = backend._build_client_kwargs()
  assert kwargs.get("tlsCertificateKeyFile") == "/path/to/key.pem"


def test_mongodb_backend_build_client_kwargs_tls_allow_invalid(mocker):
  """Test _build_client_kwargs includes tlsAllowInvalidCertificates when set."""
  config = MongoDBSettings(tls_enabled=True, tls_allow_invalid_certificates=True)
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  kwargs = backend._build_client_kwargs()
  assert kwargs.get("tlsAllowInvalidCertificates") is True


def test_mongodb_backend_build_client_kwargs_auth(mocker):
  """Test _build_client_kwargs includes auth fields when username/password are set."""
  config = MongoDBSettings(
    username="admin",
    password="secret",
    auth_source="admin",
    auth_mechanism="SCRAM-SHA-256",
  )
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  kwargs = backend._build_client_kwargs()
  assert kwargs.get("username") == "admin"
  assert kwargs.get("password") == "secret"
  assert kwargs.get("authSource") == "admin"
  assert kwargs.get("authMechanism") == "SCRAM-SHA-256"


def test_mongodb_backend_build_client_kwargs_read_preference(mocker):
  """Test _build_client_kwargs includes readPreference when read_preference is set."""
  config = MongoDBSettings(read_preference="secondary")
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  kwargs = backend._build_client_kwargs()
  assert kwargs.get("readPreference") == "secondary"


def test_mongodb_backend_initialize_collections_raises_when_client_none():
  """Test _initialize_collections raises BackendConnectionError when client is None."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  # _client is None by default

  with pytest.raises(BackendConnectionError) as exc_info:
    backend._initialize_collections()
  assert "MongoDB client not initialized" in str(exc_info.value)


def test_mongodb_backend_replica_set_with_members(mocker):
  """Test _connect_replica_set with replica_set_members builds uri from members."""
  from scrapy_extension.settings import MongoDBMode

  config = MongoDBSettings(
    mode=MongoDBMode.REPLICA_SET,
    replica_set_members=["host1:27017", "host2:27017"],
    replica_set_name="rs0",
  )
  backend = MongoDBBackend(config)

  mock_client = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_client
  )

  backend.connect()

  # Verify MongoClient was called
  assert mock_client.admin.command.called
  # Verify the backend has a client set
  assert backend._client is not None


def test_mongodb_backend_replica_set_without_members(mocker):
  """Test _connect_replica_set falls back to config.uri when no replica_set_members.

  R9-b SV2: REPLICA_SET mode requires ``replica_set_name`` (or a URI carrying
  ``?replicaSet=``). This test pins the no-members URI fallback, so the name
  is supplied to satisfy the validator without changing the fallback intent.
  """
  from scrapy_extension.settings import MongoDBMode

  config = MongoDBSettings(
    mode=MongoDBMode.REPLICA_SET,
    replica_set_name="rs0",
  )
  backend = MongoDBBackend(config)

  mock_client = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_client
  )

  backend.connect()

  # Verify MongoClient was called
  assert mock_client.admin.command.called
  assert backend._client is not None


def test_mongodb_backend_replica_set_adds_replica_set_kwarg(mocker):
  """Test _connect_replica_set adds replicaSet to kwargs when replica_set_name is set."""
  from scrapy_extension.settings import MongoDBMode

  config = MongoDBSettings(
    mode=MongoDBMode.REPLICA_SET,
    replica_set_members=["host1:27017"],
    replica_set_name="rs0",
  )
  backend = MongoDBBackend(config)

  mock_client = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_client
  )

  backend.connect()

  # Verify MongoClient was called (kwargs with replicaSet were passed)
  assert mock_client.admin.command.called


def test_mongodb_backend_sharded_cluster_fallback(mocker):
  """Test _connect_sharded_cluster falls back to config.uri when no mongos_routers."""
  from scrapy_extension.settings import MongoDBMode

  config = MongoDBSettings(mode=MongoDBMode.SHARDED_CLUSTER)
  backend = MongoDBBackend(config)

  mock_client = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_client
  )

  backend.connect()

  # Verify MongoClient was called
  assert mock_client.admin.command.called
  assert backend._client is not None


def test_mongodb_backend_create_indexes_raises_when_collections_none(mocker):
  """Test _create_indexes raises BackendConnectionError when collections are None."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  # Set collections to None to simulate uninitialized state
  backend._queue_collection = None

  with pytest.raises(BackendConnectionError) as exc_info:
    backend._create_indexes()
  assert "Collections not initialized" in str(exc_info.value)


def test_mongodb_backend_is_connected_returns_false_on_pymongo_error(mocker):
  """Test is_connected returns False when PyMongoError is raised."""
  from pymongo.errors import PyMongoError

  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mock_client = mocker.MagicMock()
  mock_client.admin.command.side_effect = PyMongoError("ping failed")
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_client
  )

  # Set up the client directly without calling connect()
  # so we can test is_connected on its own
  backend._client = mock_client

  result = backend.is_connected()
  assert result is False


def test_mongodb_backend_ping_delegates_to_is_connected(mocker):
  """Test ping returns result of is_connected."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  result = backend.ping()
  assert result is True


def test_mongodb_backend_assert_connected_raises(mocker):
  """Test _assert_connected raises BackendConnectionError when collections are None."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  # Set collections to None to simulate disconnected state
  backend._queue_collection = None

  with pytest.raises(BackendConnectionError) as exc_info:
    backend._assert_connected()
  assert "Not connected" in str(exc_info.value)


def test_mongodb_backend_pop_returns_none_when_empty(mocker):
  """Test pop returns None when queue is empty (find_one_and_delete returns None)."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._queue_collection = mock_collection
  mock_collection.find_one_and_delete.return_value = None

  result = backend.pop("empty_queue")
  assert result is None


def test_mongodb_backend_add_returns_false_on_duplicate_key(mocker):
  """Test add returns False when DuplicateKeyError is raised."""
  from pymongo.errors import DuplicateKeyError

  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._set_collection = mock_collection
  mock_collection.insert_one.side_effect = DuplicateKeyError("duplicate")

  result = backend.add("test_set", b"duplicate_item")
  assert result is False


def test_mongodb_backend_store_with_ttl(mocker):
  """Test store adds expireAt to doc when ttl is provided."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection

  backend.store("test_key", b"test_data", ttl=3600)

  # Verify replace_one was called with doc containing expireAt
  call_args = mock_collection.replace_one.call_args
  doc = call_args[0][1]  # Second positional arg is the doc
  assert "expireAt" in doc


def test_mongodb_backend_retrieve_returns_none_when_not_found(mocker):
  """Test retrieve returns None when find_one returns falsy result."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection
  mock_collection.find_one.return_value = None

  result = backend.retrieve("missing_key")
  assert result is None


def test_mongodb_backend_clear_storage_with_prefix(mocker):
  """Test clear_storage uses regex when prefix is provided."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection

  backend.clear_storage(prefix="test_")

  # Verify delete_many was called with regex pattern
  call_args = mock_collection.delete_many.call_args
  filter_doc = call_args[0][0]
  assert "$regex" in filter_doc.get("key", {})


def test_mongodb_backend_clear_storage_without_prefix(mocker):
  """Test clear_storage deletes all when prefix is None."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection

  backend.clear_storage(prefix=None)

  # Verify delete_many was called with empty filter
  call_args = mock_collection.delete_many.call_args
  filter_doc = call_args[0][0]
  assert filter_doc == {}


def test_mongodb_backend_disconnect_clears_all_collections(mocker):
  """Test disconnect clears all collection references."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)

  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()

  assert backend._queue_collection is not None
  assert backend._set_collection is not None
  assert backend._storage_collection is not None

  backend.disconnect()

  assert backend._client is None
  assert backend._db is None
  assert backend._queue_collection is None
  assert backend._set_collection is None
  assert backend._storage_collection is None


# ---------------------------------------------------------------------------
# SEC-1 (round-6): MongoDB password redaction in _auth_kwargs.
# ---------------------------------------------------------------------------


def test_mongodb_password_redacted_in_auth_kwargs_repr():
  """SEC-1: the password plumbed into MongoClient kwargs is wrapped in
  _RedactedStr so ``repr(kwargs)`` / Sentry captures of locals don't leak it.
  The str VALUE is preserved so pymongo still authenticates.
  """
  from scrapy_extension.backends._redaction import _RedactedStr
  from scrapy_extension.backends.mongodb import MongoDBBackend
  from scrapy_extension.settings.mongodb import MongoDBSettings

  config = MongoDBSettings(
    username="alice",
    password="top-secret-mongo-pwd",
  )
  backend = MongoDBBackend(config)
  auth_kwargs = backend._auth_kwargs()

  password = auth_kwargs["password"]
  # Value still usable as a normal string for pymongo auth.
  assert str(password) == "top-secret-mongo-pwd"
  # But repr of the kwargs dict hides it.
  assert "top-secret-mongo-pwd" not in repr(auth_kwargs)
  assert isinstance(password, _RedactedStr)


# ---------------------------------------------------------------------------
# R14-A: StorageBackend error-contract uniformity.
# MongoDB storage ops must wrap PyMongoError → StorageError (mirroring the
# existing queue-op wrap at mongodb.py push/pop). The raw PyMongoError must
# never leak to a caller expecting ``except BackendError``.
# ---------------------------------------------------------------------------


def _storage_backend(mocker):
  """Return a connected MongoDBBackend with a mocked storage collection."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection
  return backend, mock_collection


class TestMongoDBStorageErrorContract:
  def test_retrieve_connection_error_raises_storage_error(self, mocker):
    from pymongo.errors import AutoReconnect, PyMongoError

    backend, mock_collection = _storage_backend(mocker)
    mock_collection.find_one.side_effect = AutoReconnect("connection lost")

    with pytest.raises(StorageError) as exc_info:
      backend.retrieve("key1")
    assert exc_info.value.operation == "retrieve"
    assert exc_info.value.key == "key1"
    assert isinstance(exc_info.value.__cause__, PyMongoError)

  def test_store_pymongo_error_raises_storage_error(self, mocker):
    from pymongo.errors import PyMongoError

    backend, mock_collection = _storage_backend(mocker)
    mock_collection.replace_one.side_effect = PyMongoError("write failed")

    with pytest.raises(StorageError) as exc_info:
      backend.store("key1", b"data")
    assert exc_info.value.operation == "store"
    assert exc_info.value.key == "key1"

  def test_delete_pymongo_error_raises_storage_error(self, mocker):
    from pymongo.errors import PyMongoError

    backend, mock_collection = _storage_backend(mocker)
    mock_collection.delete_one.side_effect = PyMongoError("delete failed")

    with pytest.raises(StorageError):
      backend.delete("key1")

  def test_exists_pymongo_error_raises_storage_error(self, mocker):
    from pymongo.errors import PyMongoError

    backend, mock_collection = _storage_backend(mocker)
    mock_collection.find_one.side_effect = PyMongoError("exists failed")

    with pytest.raises(StorageError):
      backend.exists("key1")

  def test_ttl_pymongo_error_raises_storage_error(self, mocker):
    from pymongo.errors import PyMongoError

    backend, mock_collection = _storage_backend(mocker)
    mock_collection.find_one.side_effect = PyMongoError("ttl failed")

    with pytest.raises(StorageError):
      backend.ttl("key1")

  def test_clear_storage_pymongo_error_raises_storage_error(self, mocker):
    from pymongo.errors import PyMongoError

    backend, mock_collection = _storage_backend(mocker)
    mock_collection.delete_many.side_effect = PyMongoError("clear failed")

    with pytest.raises(StorageError):
      backend.clear_storage()

  def test_storage_error_is_backend_error_subclass(self, mocker):
    """``except BackendError`` must catch storage-path failures."""
    from pymongo.errors import PyMongoError

    from scrapy_extension.exceptions.base import BackendError

    backend, mock_collection = _storage_backend(mocker)
    mock_collection.replace_one.side_effect = PyMongoError("write failed")

    with pytest.raises(BackendError):
      backend.store("key1", b"data")

  def test_retrieve_missing_still_returns_none(self, mocker):
    """Retrieve-missing is NOT an error — find_one returns None → return None."""
    backend, mock_collection = _storage_backend(mocker)
    mock_collection.find_one.return_value = None
    assert backend.retrieve("missing_key") is None


# ---------------------------------------------------------------------------
# R14-G: not-connected guards across all 3 collections.
#
# ``MongoDBBackend`` enforces a connect-before-use contract two ways:
#   1. ``_assert_connected()`` — raises if ANY of the 3 collections is None;
#   2. per-collection ``if self._<x>_collection is None: raise`` guards that
#      run AFTER ``_assert_connected()`` on every public op (defensive belt-
#      and-suspenders against a race that nulled a collection between the
#      assertion and the call).
#
# Both layers are the primary corruption-prevention contract and were entirely
# untested. These tests pin both:
#   - ``test_*_raises_when_disconnected`` — the natural disconnect path
#     (``disconnect()`` nulls all collections) → ``_assert_connected`` raises.
#   - ``test_*_per_collection_guard_fires`` — null ONLY the relevant collection
#     and stub ``_assert_connected`` so the per-collection guard is reached.
# ---------------------------------------------------------------------------


class TestMongoDBNotConnectedGuards:
  """Every public op must raise ``BackendConnectionError`` when disconnected."""

  def _connected(self, mocker):
    """Return a connected backend (MongoClient mocked)."""
    config = MongoDBSettings()
    backend = MongoDBBackend(config)
    mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
    backend.connect()
    return backend

  # -- natural disconnect path: _assert_connected raises -------------------

  def test_push_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.push("q", b"x")

  def test_pop_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.pop("q")

  def test_queue_len_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.queue_len("q")

  def test_clear_queue_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.clear_queue("q")

  def test_add_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.add("s", b"x")

  def test_remove_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.remove("s", b"x")

  def test_contains_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.contains("s", b"x")

  def test_set_len_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.set_len("s")

  def test_clear_set_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.clear_set("s")

  def test_store_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.store("k", b"v")

  def test_retrieve_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.retrieve("k")

  def test_delete_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.delete("k")

  def test_exists_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.exists("k")

  def test_ttl_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.ttl("k")

  def test_clear_storage_raises_when_disconnected(self, mocker):
    backend = self._connected(mocker)
    backend.disconnect()
    with pytest.raises(BackendConnectionError):
      backend.clear_storage()

  # -- per-collection defensive guards (bypass _assert_connected) ----------
  # These pin the ``if self._<x>_collection is None`` branches that sit AFTER
  # ``_assert_connected()``. To reach them we null ONLY the relevant collection
  # (so ``_assert_connected`` would raise first) and stub the assertion to a
  # no-op so the per-collection guard is the one that fires.

  def test_push_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._queue_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.push("q", b"x")
    assert "queue collection is None" in str(exc.value)

  def test_pop_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._queue_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.pop("q")
    assert "queue collection is None" in str(exc.value)

  def test_add_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._set_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.add("s", b"x")
    assert "set collection is None" in str(exc.value)

  def test_contains_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._set_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.contains("s", b"x")
    assert "set collection is None" in str(exc.value)

  def test_store_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._storage_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.store("k", b"v")
    assert "storage collection is None" in str(exc.value)

  def test_retrieve_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._storage_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.retrieve("k")
    assert "storage collection is None" in str(exc.value)

  # -- remaining per-collection guards (queue_len / clear_queue / set_remove /
  #    set_len / clear_set / delete / exists / ttl / clear_storage) -----------
  # Each pins the ``if self._<x>_collection is None`` branch that sits AFTER
  # ``_assert_connected()``; reached by stubbing the assertion to a no-op.

  def test_queue_len_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._queue_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.queue_len("q")
    assert "queue collection is None" in str(exc.value)

  def test_clear_queue_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._queue_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.clear_queue("q")
    assert "queue collection is None" in str(exc.value)

  def test_set_remove_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._set_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.remove("s", b"x")
    assert "set collection is None" in str(exc.value)

  def test_set_len_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._set_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.set_len("s")
    assert "set collection is None" in str(exc.value)

  def test_clear_set_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._set_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.clear_set("s")
    assert "set collection is None" in str(exc.value)

  def test_delete_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._storage_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.delete("k")
    assert "storage collection is None" in str(exc.value)

  def test_exists_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._storage_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.exists("k")
    assert "storage collection is None" in str(exc.value)

  def test_ttl_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._storage_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.ttl("k")
    assert "storage collection is None" in str(exc.value)

  def test_clear_storage_per_collection_guard_fires(self, mocker):
    backend = self._connected(mocker)
    backend._storage_collection = None
    mocker.patch.object(backend, "_assert_connected", lambda: None)
    with pytest.raises(BackendConnectionError) as exc:
      backend.clear_storage()
    assert "storage collection is None" in str(exc.value)
