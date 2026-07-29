"""Terminal privacy contracts for direct DynamoDB storage operations."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from typing import Any

import boto3
import pytest

from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.exceptions import BackendConnectionError, StorageError
from scrapy_extension.settings import DynamoDBSettings

_MARKER = "round44-dynamodb-private-marker"


class _PluginStorageError(StorageError):
  """A plugin-owned storage subclass that must retain its own contract."""


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


def _backend(mocker: Any) -> tuple[DynamoDBBackend, Any]:
  """Build a direct backend retaining deliberately private mutable settings."""
  backend = DynamoDBBackend(DynamoDBSettings())
  resource = mocker.MagicMock()
  table = mocker.MagicMock()
  table.load.return_value = None
  table.table_status = "ACTIVE"
  resource.Table.return_value = table
  table.meta.client = resource.meta.client
  resource.meta.client.batch_write_item.return_value = {"UnprocessedItems": {}}
  session = mocker.MagicMock()
  session.resource.return_value = resource
  mocker.patch.object(boto3.session, "Session", return_value=session)
  backend.connect()
  backend.config.endpoint_url = f"http://{_MARKER}.example:4566"
  return backend, table


def _failing_storage_operation(
  mocker: Any, method_name: str
) -> tuple[Callable[[], object], str, str]:
  backend, table = _backend(mocker)
  failure = RuntimeError(_MARKER)
  if method_name == "store":
    table.put_item.side_effect = failure
    return lambda: backend.store(_MARKER, _MARKER.encode()), "store", (
      "DynamoDB storage store failed."
    )
  if method_name == "retrieve":
    table.get_item.side_effect = failure
    return lambda: backend.retrieve(_MARKER), "retrieve", (
      "DynamoDB storage retrieve failed."
    )
  if method_name == "delete":
    table.delete_item.side_effect = failure
    return lambda: backend.delete(_MARKER), "delete", (
      "DynamoDB storage delete failed."
    )
  if method_name == "exists":
    table.get_item.side_effect = failure
    return lambda: backend.exists(_MARKER), "exists", (
      "DynamoDB storage existence check failed."
    )
  if method_name == "ttl":
    table.get_item.side_effect = failure
    return lambda: backend.ttl(_MARKER), "ttl", (
      "DynamoDB storage TTL read failed."
    )
  if method_name == "clear_storage":
    table.scan.return_value = {"Items": [{"pk": _MARKER}]}
    table.meta.client.batch_write_item.side_effect = failure
    return lambda: backend.clear_storage(_MARKER), "clear_storage", (
      "DynamoDB storage clear failed."
    )
  raise AssertionError(f"Unexpected storage operation: {method_name}")


@pytest.mark.parametrize(
  "method_name",
  ("store", "retrieve", "delete", "exists", "ttl", "clear_storage"),
)
def test_direct_dynamodb_storage_operation_rebuilds_private_error_graph(
  mocker: Any, method_name: str
) -> None:
  operation, expected_operation, expected_message = _failing_storage_operation(
    mocker, method_name
  )

  with pytest.raises(StorageError) as exc_info:
    operation()

  error = exc_info.value
  assert type(error) is StorageError
  assert str(error) == expected_message
  assert error.operation == expected_operation
  assert error.key is None
  _assert_terminal_error_is_redacted(error, _MARKER)


def test_direct_dynamodb_clear_rebuilds_nested_batch_failure_graph(
  mocker: Any,
) -> None:
  backend, table = _backend(mocker)
  table.scan.return_value = {"Items": [{"pk": _MARKER}]}
  table.meta.client.batch_write_item.return_value = {
    "UnprocessedItems": {
      "scrapy-extension": [{"unexpected": _MARKER}],
    }
  }

  with pytest.raises(StorageError) as exc_info:
    backend.clear_storage(_MARKER)

  error = exc_info.value
  assert str(error) == "DynamoDB storage clear failed."
  assert error.operation == "clear_storage"
  assert error.key is None
  _assert_terminal_error_is_redacted(error, _MARKER)


def test_direct_dynamodb_disconnected_store_rebuilds_private_error_graph() -> None:
  backend = DynamoDBBackend(DynamoDBSettings())
  backend.config.endpoint_url = f"http://{_MARKER}.example:4566"

  with pytest.raises(StorageError) as exc_info:
    backend.store(_MARKER, _MARKER.encode())

  error = exc_info.value
  assert str(error) == "DynamoDB storage store failed."
  assert error.operation == "store"
  assert error.key is None
  _assert_terminal_error_is_redacted(error, _MARKER)


def test_dynamodb_storage_boundary_rebuilds_exact_connection_error(
  mocker: Any,
) -> None:
  backend, _table = _backend(mocker)
  error = BackendConnectionError(_MARKER, backend_type="plugin")
  mocker.patch.object(
    backend, "_table_for_operation_locked", side_effect=error
  )

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.store(_MARKER, _MARKER.encode())

  terminal_error = exc_info.value
  assert type(terminal_error) is BackendConnectionError
  assert str(terminal_error) == "DynamoDB storage store failed."
  assert terminal_error.backend_type == "dynamodb"
  _assert_terminal_error_is_redacted(terminal_error, _MARKER)


def test_dynamodb_storage_boundary_preserves_plugin_storage_subclass(
  mocker: Any,
) -> None:
  backend, _table = _backend(mocker)
  plugin_error = _PluginStorageError(
    _MARKER, operation="plugin", key=_MARKER
  )
  mocker.patch.object(
    backend, "_table_for_operation_locked", side_effect=plugin_error
  )

  with pytest.raises(_PluginStorageError) as exc_info:
    backend.store(_MARKER, _MARKER.encode())

  assert exc_info.value is plugin_error


@pytest.mark.parametrize("method_name", ("store", "clear_storage"))
def test_dynamodb_terminal_boundary_preserves_control_flow(
  mocker: Any, method_name: str
) -> None:
  backend, table = _backend(mocker)
  interruption = KeyboardInterrupt(_MARKER)
  if method_name == "store":
    table.put_item.side_effect = interruption
    operation = lambda: backend.store(_MARKER, _MARKER.encode())
  else:
    table.scan.return_value = {"Items": [{"pk": _MARKER}]}
    table.meta.client.batch_write_item.side_effect = interruption
    operation = lambda: backend.clear_storage(_MARKER)

  with pytest.raises(KeyboardInterrupt) as exc_info:
    operation()

  assert exc_info.value is interruption


def test_dynamodb_terminal_boundary_validates_inputs_before_backend_work(
  mocker: Any,
) -> None:
  backend, table = _backend(mocker)

  with pytest.raises(ValueError, match="key"):
    backend.store("invalid key", b"payload")
  with pytest.raises(ValueError, match="positive integer"):
    backend.store("valid", b"payload", ttl=0)
  with pytest.raises(ValueError, match="prefix"):
    backend.clear_storage("invalid prefix")

  table.put_item.assert_not_called()
  table.scan.assert_not_called()
  table.meta.client.batch_write_item.assert_not_called()


def test_dynamodb_storage_boundary_accepts_public_keyword_data(mocker: Any) -> None:
  backend, table = _backend(mocker)

  backend.store("keyword", data=b"payload")

  table.put_item.assert_called_once()
