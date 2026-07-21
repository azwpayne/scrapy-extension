from datetime import datetime, timedelta, timezone

import pytest

from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
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


def test_mongodb_connect_rejects_mutated_unacknowledged_w_before_sdk_io(mocker):
  config = MongoDBSettings(username="crawler", password="do-not-leak")
  backend = MongoDBBackend(config)
  config.w = 0
  client = mocker.patch("scrapy_extension.backends.mongodb.MongoClient")

  with pytest.raises(ConfigurationError) as exc_info:
    backend.connect()

  assert exc_info.value.setting_name == "w"
  assert "do-not-leak" not in str(exc_info.value)
  client.assert_not_called()


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


def test_mongodb_backend_queue_len_wraps_pymongo_error(mocker):
  from pymongo.errors import PyMongoError

  from scrapy_extension.exceptions import QueueError

  backend = MongoDBBackend(MongoDBSettings())
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  backend._queue_collection = mocker.MagicMock()
  backend._queue_collection.count_documents.side_effect = PyMongoError("count failed")

  with pytest.raises(QueueError) as exc_info:
    backend.queue_len("test_queue")

  assert exc_info.value.queue_name == "test_queue"
  assert exc_info.value.operation == "queue_len"
  assert isinstance(exc_info.value.__cause__, PyMongoError)


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


def test_mongodb_backend_clear_queue_wraps_pymongo_error(mocker):
  from pymongo.errors import PyMongoError

  from scrapy_extension.exceptions import QueueError

  backend = MongoDBBackend(MongoDBSettings())
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  backend._queue_collection = mocker.MagicMock()
  backend._queue_collection.delete_many.side_effect = PyMongoError("clear failed")

  with pytest.raises(QueueError) as exc_info:
    backend.clear_queue("test_queue")

  assert exc_info.value.queue_name == "test_queue"
  assert exc_info.value.operation == "clear_queue"
  assert isinstance(exc_info.value.__cause__, PyMongoError)


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


def test_mongodb_set_add_wraps_pymongo_error(mocker):
  """R-dupe-1 (option b): a transient PyMongoError during set add is wrapped as
  BackendConnectionError so BackendDupeFilter degrades instead of crashing.
  DuplicateKeyError (the 'already existed' signal) still returns False -- it's a
  success signal, NOT wrapped."""
  from pymongo.errors import DuplicateKeyError, PyMongoError

  from scrapy_extension.exceptions import BackendConnectionError

  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._set_collection = mock_collection

  # Operational PyMongoError → wrapped as BackendConnectionError.
  mock_collection.insert_one.side_effect = PyMongoError("set add failed")
  with pytest.raises(BackendConnectionError) as exc_info:
    backend.add("test_set", b"item")
  assert exc_info.value.backend_type == "mongodb"

  # DuplicateKeyError (the 'already existed' signal) still returns False,
  # NOT wrapped — it is a success signal, not a transient error.
  mock_collection.insert_one.side_effect = DuplicateKeyError("dup")
  assert backend.add("test_set", b"item") is False


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


@pytest.mark.parametrize(
  ("method", "driver_method"),
  [
    ("remove", "delete_one"),
    ("contains", "find_one"),
    ("set_len", "count_documents"),
  ],
)
def test_mongodb_set_reads_wrap_pymongo_errors(mocker, method, driver_method):
  from pymongo.errors import PyMongoError

  backend = MongoDBBackend(MongoDBSettings())
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  collection = mocker.MagicMock()
  backend._set_collection = collection
  error = PyMongoError(f"{driver_method} failed")
  getattr(collection, driver_method).side_effect = error

  args = ("test_set", b"item") if method != "set_len" else ("test_set",)
  with pytest.raises(BackendConnectionError) as exc_info:
    getattr(backend, method)(*args)

  assert exc_info.value.backend_type == "mongodb"
  assert exc_info.value.__cause__ is error


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


def test_mongodb_backend_storage_ttl_null_expiry_is_permanent(mocker):
  """A persisted null expiry is the same permanent-value sentinel as absence."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection
  mock_collection.find_one.return_value = {"key": "k", "expireAt": None}

  assert backend.ttl("k") is None
  mock_collection.delete_one.assert_not_called()


def test_mongodb_backend_storage_ttl_expired_returns_none_and_reaps(mocker):
  """Expired storage is absent after the backend's conditional lazy reap."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection

  past_time = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
  mock_collection.find_one.return_value = {"key": "k", "expireAt": past_time}

  assert backend.ttl("k") is None
  mock_collection.delete_one.assert_called_once_with(
    {"key": "k", "expireAt": past_time}
  )


def test_mongodb_backend_storage_ttl_reap_failure_still_returns_none(mocker):
  """Cleanup is best effort; an expired value remains logically absent."""
  from pymongo.errors import PyMongoError

  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection
  past_time = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
  mock_collection.find_one.return_value = {"key": "k", "expireAt": past_time}
  mock_collection.delete_one.side_effect = PyMongoError("cleanup unavailable")

  assert backend.ttl("k") is None
  mock_collection.delete_one.assert_called_once_with(
    {"key": "k", "expireAt": past_time}
  )


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
  assert backend._client is None
  assert backend._db is None
  assert backend._queue_collection is None
  assert backend._set_collection is None
  assert backend._storage_collection is None
  mock_instance.close.assert_called_once()


def test_mongodb_backend_reconnects_after_failed_connect(mocker):
  """A failed client must be discarded so a later connect can recover."""
  from pymongo.errors import ConnectionFailure

  backend = MongoDBBackend(MongoDBSettings())
  failed_client = mocker.MagicMock()
  failed_client.admin.command.side_effect = ConnectionFailure("network error")
  healthy_client = mocker.MagicMock()
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient",
    side_effect=[failed_client, healthy_client],
  )

  with pytest.raises(BackendConnectionError):
    backend.connect()
  backend.connect()

  assert backend._client is healthy_client
  assert backend._queue_collection is not None
  assert backend._set_collection is not None
  assert backend._storage_collection is not None
  assert backend.is_connected() is True


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


def test_mongodb_backend_retrieve_treats_expired_document_as_missing(mocker):
  """A delayed MongoDB TTL sweep must not expose expired storage data."""
  backend, mock_collection = _storage_backend(mocker)
  expired_at = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(
    seconds=60
  )
  mock_collection.find_one.return_value = {
    "key": "expired_key",
    "data": b"stale",
    "expireAt": expired_at,
  }

  assert backend.retrieve("expired_key") is None


def test_mongodb_backend_exists_treats_expired_document_as_missing(mocker):
  """Existence follows the Storage contract, not TTL monitor timing."""
  backend, mock_collection = _storage_backend(mocker)
  expired_at = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
  mock_collection.find_one.return_value = {
    "_id": "expired-id",
    "expireAt": expired_at,
  }

  assert backend.exists("expired_key") is False


def test_mongodb_expired_reap_does_not_delete_a_concurrent_fresh_write(mocker):
  """Lazy cleanup must compare the stale snapshot's expiry before deleting."""
  backend, mock_collection = _storage_backend(mocker)
  expired_at = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
  fresh_expire_at = datetime.now(tz=timezone.utc) + timedelta(hours=1)
  current_document = {
    "key": "race_key",
    "data": b"stale",
    "expireAt": expired_at,
  }
  read_count = 0

  def find_one(_query, _projection=None):
    nonlocal current_document, read_count
    snapshot = dict(current_document) if current_document is not None else None
    read_count += 1
    if read_count == 1:
      current_document = {
        "key": "race_key",
        "data": b"fresh",
        "expireAt": fresh_expire_at,
      }
    return snapshot

  def delete_one(query):
    nonlocal current_document
    if current_document is not None and all(
      current_document.get(field) == value for field, value in query.items()
    ):
      current_document = None
    return mocker.MagicMock(deleted_count=0)

  mock_collection.find_one.side_effect = find_one
  mock_collection.delete_one.side_effect = delete_one

  assert backend.retrieve("race_key") is None
  assert backend.retrieve("race_key") == b"fresh"
  delete_filter = mock_collection.delete_one.call_args.args[0]
  assert delete_filter["key"] == "race_key"
  assert delete_filter["expireAt"] == expired_at


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


def test_mongodb_clear_storage_prefix_wraps_pymongo_error(mocker):
  """Coverage: clear_storage(prefix=...) wraps a PyMongoError as StorageError
  (the #30 StorageError-family contract). Locks mongodb.py:818-820."""
  from pymongo.errors import PyMongoError

  from scrapy_extension.exceptions.base import StorageError

  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection
  mock_collection.delete_many.side_effect = PyMongoError("delete failed")

  with pytest.raises(StorageError, match="clear MongoDB storage") as ei:
    backend.clear_storage(prefix="test_")
  assert isinstance(ei.value.__cause__, PyMongoError)


def test_mongodb_ttl_handles_naive_datetime(mocker):
  """Coverage: PyMongo returns naive UTC datetimes by default (tz_aware=False);
  ttl() must normalize to aware before subtraction or raise TypeError. Locks
  mongodb.py:788-789 (the naive→aware replace branch)."""
  config = MongoDBSettings()
  backend = MongoDBBackend(config)
  mocker.patch("scrapy_extension.backends.mongodb.MongoClient")
  backend.connect()
  mock_collection = mocker.MagicMock()
  backend._storage_collection = mock_collection
  # Naive future datetime (what PyMongo returns by default).
  naive_future = datetime.now() + timedelta(seconds=3600)
  mock_collection.find_one.return_value = {"expireAt": naive_future}

  result = backend.ttl("k")
  # Must not raise TypeError (naive - aware); returns a positive remaining.
  assert result is not None and result > 0


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

  def test_clear_set_wraps_pymongo_error(self, mocker):
    """R-rclears-mongo: a PyMongoError during clear_set is wrapped as
    BackendConnectionError (parity with add R-dupe-1 #38 + redis clear_set #71),
    not leaked raw."""
    from pymongo.errors import PyMongoError

    backend = self._connected(mocker)
    backend._set_collection.delete_many.side_effect = PyMongoError("delete boom")
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.clear_set("s")
    assert exc_info.value.backend_type == "mongodb"
    assert "clear failed" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, PyMongoError)  # `from e` chaining

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


def test_mongodb_invalid_queue_name_rejected_before_backend_call():
  """R-mongo-validate: invalid queue names raise ValueError before any backend
  interaction — parity with Redis (which validates in all methods). Defense-in-
  depth vs NoSQL operator-injection (``$ne``/``$gt``) via a special-char name.
  Validation fires before ``_assert_connected`` so this needs no connection.
  """
  backend = MongoDBBackend(MongoDBSettings())
  with pytest.raises(ValueError, match="queue_name"):
    backend.push("bad queue name!", b"x")  # space + ! are outside KEY_NAME_PATTERN


def test_mongodb_invalid_set_name_rejected_before_backend_call():
  """R-mongo-validate: set methods also validate (parity with Redis)."""
  backend = MongoDBBackend(MongoDBSettings())
  with pytest.raises(ValueError, match="set_name"):
    backend.add("bad/set", b"x")  # slash is outside KEY_NAME_PATTERN


def test_mongodb_invalid_storage_key_rejected_before_backend_call():
  """R-mongo-validate: storage methods also validate (parity with Redis)."""
  backend = MongoDBBackend(MongoDBSettings())
  with pytest.raises(ValueError, match="key"):
    backend.store("bad key", b"x")  # space is outside KEY_NAME_PATTERN


def test_mongodb_clear_storage_invalid_prefix_rejected():
  """R-mongo-validate: clear_storage validates a provided prefix (None still
  clears all — the optional-prefix contract is preserved)."""
  backend = MongoDBBackend(MongoDBSettings())
  with pytest.raises(ValueError, match="prefix"):
    backend.clear_storage("bad prefix!")  # space + ! outside KEY_NAME_PATTERN


def test_mongodb_valid_names_pass_validation(mocker):
  """R-mongo-validate guard: pattern-valid names (alnum, dots, underscores,
  hyphens, colons) are NOT rejected — the default queue/storage naming
  (``scheduler:queue``, ``k1``, ``prefix:spider``) keeps working."""
  backend = MongoDBBackend(MongoDBSettings())
  mocker.patch.object(backend, "_assert_connected", lambda: None)
  backend._queue_collection = mocker.MagicMock()
  backend._set_collection = mocker.MagicMock()
  backend._storage_collection = mocker.MagicMock()
  backend._storage_collection.find_one.return_value = None
  # None of these should raise ValueError.
  backend.queue_len("scheduler:queue")
  backend.set_len("dedup:spider.name")
  backend.exists("items:a-b_c.1")
