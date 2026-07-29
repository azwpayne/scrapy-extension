"""Terminal privacy contracts for direct MongoDB public operations."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from typing import Any

import pytest
from pymongo.errors import DuplicateKeyError, PyMongoError

from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError, StorageError
from scrapy_extension.settings import MongoDBSettings

_MARKER = "round44-mongodb-private-marker"


class _PluginConnectionError(BackendConnectionError):
  """A plugin-owned exception subclass that must retain its own contract."""


class _PluginStorageError(StorageError):
  """A plugin-owned storage subclass that must not be reconstructed."""


def _assert_value_graph_is_redacted(
  value: object, marker: str, seen: set[int] | None = None
) -> None:
  """Walk a bounded public graph without trusting a redacted ``repr``."""
  if seen is None:
    seen = set()
  value_id = id(value)
  if value_id in seen:
    return
  seen.add(value_id)
  if isinstance(value, str):
    assert marker not in value
    return
  if isinstance(value, bytes):
    assert marker.encode() not in value
    return
  if isinstance(value, dict):
    for key, item in value.items():
      _assert_value_graph_is_redacted(key, marker, seen)
      _assert_value_graph_is_redacted(item, marker, seen)
    return
  if isinstance(value, (tuple, list, set, frozenset)):
    for item in value:
      _assert_value_graph_is_redacted(item, marker, seen)
    return
  try:
    attributes = vars(value)
  except TypeError:
    return
  _assert_value_graph_is_redacted(attributes, marker, seen)


def _assert_terminal_error_is_redacted(error: BaseException, marker: str) -> None:
  """Assert public error metadata and package-frame locals omit ``marker``."""
  assert marker not in str(error)
  assert marker not in repr(error.args)
  assert marker not in repr(error.__dict__)
  assert error.__cause__ is None
  assert error.__context__ is None
  _assert_value_graph_is_redacted(error, marker)
  assert marker not in "".join(traceback.format_exception(error))

  trace = error.__traceback__
  while trace is not None:
    frame = trace.tb_frame
    if "/src/scrapy_extension/" in frame.f_code.co_filename:
      assert marker not in repr(frame.f_locals)
      for value in frame.f_locals.values():
        _assert_value_graph_is_redacted(value, marker)
    trace = trace.tb_next


def _backend(mocker: Any) -> tuple[MongoDBBackend, Any, Any, Any]:
  """Build a direct backend retaining deliberately private mutable settings."""
  config = MongoDBSettings()
  config.uri = f"mongodb://{_MARKER}.example:27017"
  backend = MongoDBBackend(config)
  queue_collection = mocker.MagicMock()
  set_collection = mocker.MagicMock()
  storage_collection = mocker.MagicMock()
  backend._queue_collection = queue_collection
  backend._set_collection = set_collection
  backend._storage_collection = storage_collection
  return backend, queue_collection, set_collection, storage_collection


def _failing_queue_operation(
  mocker: Any, method_name: str
) -> Callable[[], object]:
  backend, queue_collection, _set_collection, _storage_collection = _backend(mocker)
  failure = PyMongoError(_MARKER)
  if method_name == "push":
    queue_collection.insert_one.side_effect = failure
    return lambda: backend.push(_MARKER, _MARKER.encode())
  if method_name == "pop":
    queue_collection.find_one_and_delete.side_effect = failure
    return lambda: backend.pop(_MARKER)
  if method_name == "pop_with_ack":
    queue_collection.find_one_and_delete.side_effect = failure
    return lambda: backend.pop_with_ack(_MARKER)
  if method_name == "queue_len":
    queue_collection.count_documents.side_effect = failure
    return lambda: backend.queue_len(_MARKER)
  if method_name == "clear_queue":
    queue_collection.delete_many.side_effect = failure
    return lambda: backend.clear_queue(_MARKER)
  raise AssertionError(f"Unexpected queue operation: {method_name}")


@pytest.mark.parametrize(
  ("method_name", "expected_operation", "expected_message"),
  (
    ("push", "push", "MongoDB queue push failed."),
    ("pop", "pop", "MongoDB queue pop failed."),
    ("pop_with_ack", "pop", "MongoDB queue pop failed."),
    ("queue_len", "queue_len", "MongoDB queue length read failed."),
    ("clear_queue", "clear_queue", "MongoDB queue clear failed."),
  ),
)
def test_direct_mongodb_queue_operation_rebuilds_private_error_graph(
  mocker: Any,
  method_name: str,
  expected_operation: str,
  expected_message: str,
) -> None:
  operation = _failing_queue_operation(mocker, method_name)

  with pytest.raises(QueueError) as exc_info:
    operation()

  error = exc_info.value
  assert str(error) == expected_message
  assert error.operation == expected_operation
  assert error.queue_name is None
  _assert_terminal_error_is_redacted(error, _MARKER)


def _failing_set_operation(mocker: Any, method_name: str) -> Callable[[], object]:
  backend, _queue_collection, set_collection, _storage_collection = _backend(mocker)
  failure = PyMongoError(_MARKER)
  if method_name == "add":
    set_collection.insert_one.side_effect = failure
    return lambda: backend.add(_MARKER, _MARKER.encode())
  if method_name == "remove":
    set_collection.delete_one.side_effect = failure
    return lambda: backend.remove(_MARKER, _MARKER.encode())
  if method_name == "contains":
    set_collection.find_one.side_effect = failure
    return lambda: backend.contains(_MARKER, _MARKER.encode())
  if method_name == "set_len":
    set_collection.count_documents.side_effect = failure
    return lambda: backend.set_len(_MARKER)
  if method_name == "clear_set":
    set_collection.delete_many.side_effect = failure
    return lambda: backend.clear_set(_MARKER)
  raise AssertionError(f"Unexpected set operation: {method_name}")


@pytest.mark.parametrize(
  ("method_name", "expected_message"),
  (
    ("add", "MongoDB set add failed."),
    ("remove", "MongoDB set remove failed."),
    ("contains", "MongoDB set membership check failed."),
    ("set_len", "MongoDB set length read failed."),
    ("clear_set", "MongoDB set clear failed."),
  ),
)
def test_direct_mongodb_set_operation_rebuilds_private_error_graph(
  mocker: Any, method_name: str, expected_message: str
) -> None:
  operation = _failing_set_operation(mocker, method_name)

  with pytest.raises(BackendConnectionError) as exc_info:
    operation()

  error = exc_info.value
  assert str(error) == expected_message
  assert error.backend_type == "mongodb"
  _assert_terminal_error_is_redacted(error, _MARKER)


def _failing_storage_operation(
  mocker: Any, method_name: str
) -> Callable[[], object]:
  backend, _queue_collection, _set_collection, storage_collection = _backend(mocker)
  failure = PyMongoError(_MARKER)
  if method_name == "store":
    storage_collection.replace_one.side_effect = failure
    return lambda: backend.store(_MARKER, _MARKER.encode())
  if method_name == "retrieve":
    storage_collection.find_one.side_effect = failure
    return lambda: backend.retrieve(_MARKER)
  if method_name == "delete":
    storage_collection.delete_one.side_effect = failure
    return lambda: backend.delete(_MARKER)
  if method_name == "exists":
    storage_collection.find_one.side_effect = failure
    return lambda: backend.exists(_MARKER)
  if method_name == "ttl":
    storage_collection.find_one.side_effect = failure
    return lambda: backend.ttl(_MARKER)
  if method_name == "clear_storage":
    storage_collection.delete_many.side_effect = failure
    return lambda: backend.clear_storage(_MARKER)
  raise AssertionError(f"Unexpected storage operation: {method_name}")


@pytest.mark.parametrize(
  ("method_name", "expected_operation", "expected_message"),
  (
    ("store", "store", "MongoDB storage store failed."),
    ("retrieve", "retrieve", "MongoDB storage retrieve failed."),
    ("delete", "delete", "MongoDB storage delete failed."),
    ("exists", "exists", "MongoDB storage existence check failed."),
    ("ttl", "ttl", "MongoDB storage TTL read failed."),
    ("clear_storage", "clear_storage", "MongoDB storage clear failed."),
  ),
)
def test_direct_mongodb_storage_operation_rebuilds_private_error_graph(
  mocker: Any,
  method_name: str,
  expected_operation: str,
  expected_message: str,
) -> None:
  operation = _failing_storage_operation(mocker, method_name)

  with pytest.raises(StorageError) as exc_info:
    operation()

  error = exc_info.value
  assert str(error) == expected_message
  assert error.operation == expected_operation
  assert error.key is None
  _assert_terminal_error_is_redacted(error, _MARKER)


@pytest.mark.parametrize(
  ("operation_kind", "expected_type", "expected_operation"),
  (
    ("queue", QueueError, "push"),
    ("set", BackendConnectionError, None),
    ("storage", BackendConnectionError, None),
  ),
)
def test_direct_mongodb_disconnected_operation_rebuilds_private_error_graph(
  operation_kind: str,
  expected_type: type[Exception],
  expected_operation: str | None,
) -> None:
  config = MongoDBSettings()
  config.uri = f"mongodb://{_MARKER}.example:27017"
  backend = MongoDBBackend(config)
  if operation_kind == "queue":
    operation = lambda: backend.push(_MARKER, _MARKER.encode())
  elif operation_kind == "set":
    operation = lambda: backend.add(_MARKER, _MARKER.encode())
  else:
    operation = lambda: backend.store(_MARKER, _MARKER.encode())

  with pytest.raises(expected_type) as exc_info:
    operation()

  error = exc_info.value
  if isinstance(error, QueueError):
    assert error.operation == expected_operation
    assert error.queue_name is None
  else:
    assert isinstance(error, BackendConnectionError)
    assert error.backend_type == "mongodb"
  _assert_terminal_error_is_redacted(error, _MARKER)


def test_direct_mongodb_set_duplicate_still_returns_false(mocker: Any) -> None:
  backend, _queue_collection, set_collection, _storage_collection = _backend(mocker)
  set_collection.insert_one.side_effect = DuplicateKeyError(_MARKER)

  assert backend.add(_MARKER, _MARKER.encode()) is False


@pytest.mark.parametrize("operation_kind", ("queue", "set", "storage"))
def test_mongodb_terminal_boundaries_preserve_control_flow(
  mocker: Any, operation_kind: str
) -> None:
  backend, queue_collection, set_collection, storage_collection = _backend(mocker)
  interruption = KeyboardInterrupt(_MARKER)
  if operation_kind == "queue":
    queue_collection.insert_one.side_effect = interruption
    operation = lambda: backend.push(_MARKER, _MARKER.encode())
  elif operation_kind == "set":
    set_collection.insert_one.side_effect = interruption
    operation = lambda: backend.add(_MARKER, _MARKER.encode())
  else:
    storage_collection.replace_one.side_effect = interruption
    operation = lambda: backend.store(_MARKER, _MARKER.encode())

  with pytest.raises(KeyboardInterrupt) as exc_info:
    operation()

  assert exc_info.value is interruption


@pytest.mark.parametrize("operation_kind", ("queue", "set", "storage"))
def test_mongodb_terminal_boundaries_preserve_unknown_exception_contract(
  mocker: Any, operation_kind: str
) -> None:
  backend, queue_collection, set_collection, storage_collection = _backend(mocker)
  unknown = RuntimeError(_MARKER)
  if operation_kind == "queue":
    queue_collection.insert_one.side_effect = unknown
    operation = lambda: backend.push(_MARKER, _MARKER.encode())
  elif operation_kind == "set":
    set_collection.insert_one.side_effect = unknown
    operation = lambda: backend.add(_MARKER, _MARKER.encode())
  else:
    storage_collection.replace_one.side_effect = unknown
    operation = lambda: backend.store(_MARKER, _MARKER.encode())

  with pytest.raises(RuntimeError) as exc_info:
    operation()

  assert exc_info.value is unknown


@pytest.mark.parametrize("operation_kind", ("set", "storage"))
def test_mongodb_terminal_boundaries_preserve_plugin_connection_subclass(
  mocker: Any, operation_kind: str
) -> None:
  backend, _queue_collection, _set_collection, _storage_collection = _backend(mocker)
  plugin_error = _PluginConnectionError(_MARKER, backend_type="plugin")
  mocker.patch.object(backend, "_assert_connected", side_effect=plugin_error)
  operation = (
    (lambda: backend.add(_MARKER, _MARKER.encode()))
    if operation_kind == "set"
    else (lambda: backend.store(_MARKER, _MARKER.encode()))
  )

  with pytest.raises(_PluginConnectionError) as exc_info:
    operation()

  assert exc_info.value is plugin_error


def test_mongodb_storage_boundary_preserves_plugin_storage_subclass(
  mocker: Any,
) -> None:
  backend, _queue_collection, _set_collection, _storage_collection = _backend(mocker)
  plugin_error = _PluginStorageError(_MARKER, operation="plugin", key=_MARKER)
  mocker.patch.object(backend, "_assert_connected", side_effect=plugin_error)

  with pytest.raises(_PluginStorageError) as exc_info:
    backend.store(_MARKER, _MARKER.encode())

  assert exc_info.value is plugin_error


def test_mongodb_boundaries_validate_inputs_before_backend_work(mocker: Any) -> None:
  backend, queue_collection, set_collection, storage_collection = _backend(mocker)

  with pytest.raises(ValueError, match="queue_name"):
    backend.push("invalid queue", b"payload")
  with pytest.raises(ValueError, match="set_name"):
    backend.add("invalid/set", b"payload")
  with pytest.raises(ValueError, match="positive integer"):
    backend.store("valid", b"payload", ttl=0)
  with pytest.raises(ValueError, match="prefix"):
    backend.clear_storage("invalid prefix")

  queue_collection.insert_one.assert_not_called()
  set_collection.insert_one.assert_not_called()
  storage_collection.replace_one.assert_not_called()
  storage_collection.delete_many.assert_not_called()


def test_mongodb_storage_boundary_accepts_public_keyword_data(mocker: Any) -> None:
  backend, _queue_collection, _set_collection, storage_collection = _backend(mocker)

  backend.store("keyword", data=b"payload")

  storage_collection.replace_one.assert_called_once()
