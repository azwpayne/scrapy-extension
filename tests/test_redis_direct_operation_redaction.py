"""Terminal privacy contracts for direct Redis set and storage operations."""

from __future__ import annotations

import traceback
from typing import Any

import pytest
from redis.exceptions import RedisError

from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.exceptions import BackendConnectionError, StorageError
from scrapy_extension.settings import RedisSettings

_MARKER = "round44-redis-direct-private-marker"


def _connected_backend(mocker: Any) -> tuple[RedisBackend, Any]:
  """Build a live Redis generation with private mutable configuration."""
  client = mocker.MagicMock()
  client.ping.return_value = True
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(
    RedisSettings(host=f"{_MARKER}.example", namespace=_MARKER)
  )
  backend.connect()
  return backend, client


def _assert_value_graph_is_redacted(
  value: object, marker: str, seen: set[int] | None = None
) -> None:
  """Walk a bounded object graph without relying on a redacted repr."""
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
  """Assert public metadata and package traceback locals omit ``marker``."""
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


@pytest.mark.parametrize(
  ("method_name", "expected_message"),
  (
    ("add", "Redis set add failed."),
    ("remove", "Redis set remove failed."),
    ("contains", "Redis set membership check failed."),
    ("set_len", "Redis set length read failed."),
    ("clear_set", "Redis set clear failed."),
  ),
)
def test_direct_redis_set_operations_rebuild_private_error_graphs(
  mocker: Any, method_name: str, expected_message: str
) -> None:
  backend, client = _connected_backend(mocker)
  failure = RedisError(_MARKER)
  if method_name == "add":
    client.sadd.side_effect = failure
    operation = lambda: backend.add(_MARKER, _MARKER.encode())
  elif method_name == "remove":
    client.srem.side_effect = failure
    operation = lambda: backend.remove(_MARKER, _MARKER.encode())
  elif method_name == "contains":
    client.sismember.side_effect = failure
    operation = lambda: backend.contains(_MARKER, _MARKER.encode())
  elif method_name == "set_len":
    client.scard.side_effect = failure
    operation = lambda: backend.set_len(_MARKER)
  else:
    client.delete.side_effect = failure
    operation = lambda: backend.clear_set(_MARKER)

  with pytest.raises(BackendConnectionError) as exc_info:
    operation()

  error = exc_info.value
  assert str(error) == expected_message
  assert error.backend_type == "redis"
  _assert_terminal_error_is_redacted(error, _MARKER)


@pytest.mark.parametrize(
  ("method_name", "expected_operation", "expected_message"),
  (
    ("store", "store", "Redis storage write failed."),
    ("store_rejected", "store", "Redis rejected a storage write."),
    ("retrieve", "retrieve", "Redis storage read failed."),
    (
      "retrieve_invalid",
      "retrieve",
      "Redis storage read returned an invalid response type.",
    ),
    ("delete", "delete", "Redis storage delete failed."),
    ("exists", "exists", "Redis storage existence check failed."),
    ("ttl", "ttl", "Redis storage TTL read failed."),
    (
      "clear_storage",
      "clear_storage",
      "Redis storage clear failed and may be partially complete.",
    ),
  ),
)
def test_direct_redis_storage_operations_rebuild_private_error_graphs(
  mocker: Any,
  method_name: str,
  expected_operation: str,
  expected_message: str,
) -> None:
  backend, client = _connected_backend(mocker)
  failure = RedisError(_MARKER)
  if method_name == "store":
    client.set.side_effect = failure
    operation = lambda: backend.store(_MARKER, _MARKER.encode())
  elif method_name == "store_rejected":
    client.set.return_value = False
    operation = lambda: backend.store(_MARKER, _MARKER.encode())
  elif method_name == "retrieve":
    client.get.side_effect = failure
    operation = lambda: backend.retrieve(_MARKER)
  elif method_name == "retrieve_invalid":
    client.get.return_value = object()
    operation = lambda: backend.retrieve(_MARKER)
  elif method_name == "delete":
    client.delete.side_effect = failure
    operation = lambda: backend.delete(_MARKER)
  elif method_name == "exists":
    client.exists.side_effect = failure
    operation = lambda: backend.exists(_MARKER)
  elif method_name == "ttl":
    client.ttl.side_effect = failure
    operation = lambda: backend.ttl(_MARKER)
  else:
    client.scan_iter.side_effect = failure
    operation = lambda: backend.clear_storage(_MARKER)

  with pytest.raises(StorageError) as exc_info:
    operation()

  error = exc_info.value
  assert str(error) == expected_message
  assert error.operation == expected_operation
  assert error.key is None
  _assert_terminal_error_is_redacted(error, _MARKER)


def test_redis_direct_set_and_storage_validators_run_before_io(mocker: Any) -> None:
  backend, client = _connected_backend(mocker)

  with pytest.raises(ValueError, match="set_name"):
    backend.add("invalid set name", _MARKER.encode())
  with pytest.raises(ValueError, match="key"):
    backend.store("invalid storage key", _MARKER.encode())
  with pytest.raises(ValueError, match="ttl"):
    backend.store("item", _MARKER.encode(), ttl=-1)

  client.sadd.assert_not_called()
  client.set.assert_not_called()


def test_redis_storage_boundary_accepts_public_keyword_data(mocker: Any) -> None:
  backend, client = _connected_backend(mocker)

  backend.store("keyword", data=b"payload")

  client.set.assert_called_once()


def test_redis_direct_set_and_storage_boundaries_preserve_base_exception_identity(
  mocker: Any,
) -> None:
  backend, client = _connected_backend(mocker)
  set_interrupt = KeyboardInterrupt(_MARKER)
  storage_interrupt = KeyboardInterrupt(_MARKER)
  client.sadd.side_effect = set_interrupt

  with pytest.raises(KeyboardInterrupt) as set_exc_info:
    backend.add(_MARKER, _MARKER.encode())

  client.get.side_effect = storage_interrupt
  with pytest.raises(KeyboardInterrupt) as storage_exc_info:
    backend.retrieve(_MARKER)

  assert set_exc_info.value is set_interrupt
  assert storage_exc_info.value is storage_interrupt
