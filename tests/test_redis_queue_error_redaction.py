"""Direct Redis queue errors must not retain backend operation state."""

from __future__ import annotations

import traceback
from typing import Any

import pytest
from redis.exceptions import RedisError

from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import RedisSettings

_MARKER = "round44-redis-queue-private-marker"


def _connected_backend(mocker: Any) -> tuple[RedisBackend, Any]:
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
  """Walk the bounded public graph without relying on redacted repr output."""
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


def _assert_terminal_queue_error_is_redacted(
  error: QueueError, marker: str
) -> None:
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
      for value in frame.f_locals.values():
        _assert_value_graph_is_redacted(value, marker)
    trace = trace.tb_next


@pytest.mark.parametrize(
  ("method", "operation"),
  [
    ("push", "push"),
    ("pop", "pop"),
    ("pop_with_ack", "pop"),
    ("queue_len", "queue_len"),
    ("clear_queue", "clear_queue"),
  ],
)
def test_direct_redis_queue_operations_rebuild_private_error_graphs(
  mocker: Any, method: str, operation: str
) -> None:
  backend, client = _connected_backend(mocker)
  failure = RedisError(_MARKER)

  if method in {"push", "pop", "pop_with_ack"}:
    client.register_script.return_value.side_effect = failure
  elif method == "queue_len":
    client.zcard.side_effect = failure
  else:
    client.delete.side_effect = failure

  with pytest.raises(QueueError) as exc_info:
    if method == "push":
      backend.push(_MARKER, _MARKER.encode())
    elif method == "pop":
      backend.pop(_MARKER)
    elif method == "pop_with_ack":
      backend.pop_with_ack(_MARKER)
    elif method == "queue_len":
      backend.queue_len(_MARKER)
    else:
      backend.clear_queue(_MARKER)

  error = exc_info.value
  assert error.operation == operation
  assert error.queue_name is None
  _assert_terminal_queue_error_is_redacted(error, _MARKER)


@pytest.mark.parametrize("method", ["pop", "pop_with_ack"])
def test_redis_pop_timeout_validation_happens_before_boundary_io(
  mocker: Any, method: str
) -> None:
  backend, client = _connected_backend(mocker)

  with pytest.raises(ValueError, match="finite non-negative"):
    getattr(backend, method)("jobs", timeout=-1)

  client.register_script.assert_not_called()


def test_redis_queue_boundary_preserves_base_exception_identity(mocker: Any) -> None:
  backend, client = _connected_backend(mocker)
  interrupt = KeyboardInterrupt(_MARKER)
  client.zcard.side_effect = interrupt

  with pytest.raises(KeyboardInterrupt) as exc_info:
    backend.queue_len(_MARKER)

  assert exc_info.value is interrupt
