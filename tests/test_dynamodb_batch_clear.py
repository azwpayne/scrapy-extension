"""Bounded DynamoDB clear batching and retry contracts."""

from __future__ import annotations

import logging
import subprocess
import sys
import threading
from typing import Any

import pytest

from scrapy_extension.backends import dynamodb as dynamodb_module
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import DynamoDBSettings

_TABLE_NAME = "scrapy-extension"
_MAX_ATTEMPTS = 8
_BASE_DELAY = 0.05


def _connected(mocker) -> tuple[DynamoDBBackend, Any, Any, Any]:
  backend = DynamoDBBackend(DynamoDBSettings())
  session = mocker.MagicMock()
  resource = mocker.MagicMock()
  table = mocker.MagicMock()
  client = resource.meta.client
  table.load.return_value = None
  table.table_status = "ACTIVE"
  resource.Table.return_value = table
  table.meta.client = client
  session.resource.return_value = resource
  mocker.patch.object(
    dynamodb_module.boto3.session,
    "Session",
    return_value=session,
  )
  backend.connect()
  return backend, table, client, resource


def _delete_request(key: str) -> dict[str, Any]:
  # The Resource client owns AttributeValue transforms; pre-serializing this
  # would double-encode the key as a DynamoDB Map instead of a String.
  return {"DeleteRequest": {"Key": {"pk": key}}}


def _join(thread: threading.Thread) -> None:
  thread.join(timeout=5)
  assert not thread.is_alive()


def test_clear_chunks_low_level_batch_writes_at_25_items(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  keys = [f"key-{index}" for index in range(60)]
  table.scan.return_value = {"Items": [{"pk": key} for key in keys]}
  client.batch_write_item.return_value = {"UnprocessedItems": {}}

  backend.clear_storage()

  assert client.batch_write_item.call_count == 3
  batches = [
    call.kwargs["RequestItems"][_TABLE_NAME]
    for call in client.batch_write_item.call_args_list
  ]
  assert [len(batch) for batch in batches] == [25, 25, 10]
  assert [request for batch in batches for request in batch] == [
    _delete_request(key) for key in keys
  ]
  table.batch_writer.assert_not_called()


def test_real_resource_client_round_trips_native_delete_requests() -> None:
  """Verify the locked boto3 transformer seam in an unpolluted process."""
  script = "\n".join(
    (
      "from unittest.mock import patch",
      "import boto3",
      "from botocore.stub import Stubber",
      "from scrapy_extension.backends.dynamodb import DynamoDBBackend",
      "resource = boto3.session.Session().resource(",
      "  'dynamodb', region_name='us-east-1',",
      "  endpoint_url='http://localhost:4566',",
      "  aws_access_key_id='x', aws_secret_access_key='y',",
      ")",
      "client = resource.meta.client",
      "request = {'DeleteRequest': {'Key': {'pk': 'k'}}}",
      "expected = {'RequestItems': {'table-a': [request]}}",
      "wire_pending = {'UnprocessedItems': {'table-a': [",
      "  {'DeleteRequest': {'Key': {'pk': {'S': 'k'}}}}",
      "]}}",
      "with Stubber(client) as stubber:",
      "  stubber.add_response('batch_write_item', wire_pending, expected)",
      "  stubber.add_response(",
      "    'batch_write_item', {'UnprocessedItems': {}}, expected",
      "  )",
      "  with patch(",
      "    'scrapy_extension.backends.dynamodb.compute_full_jitter_backoff',",
      "    return_value=0.0,",
      "  ), patch('scrapy_extension.backends.dynamodb.time.sleep'):",
      "    DynamoDBBackend._delete_batch_with_backoff(",
      "      client, 'table-a', [request]",
      "    )",
      "  stubber.assert_no_pending_responses()",
      "assert request == {'DeleteRequest': {'Key': {'pk': 'k'}}}",
    )
  )

  result = subprocess.run(
    [sys.executable, "-c", script],
    capture_output=True,
    text=True,
    check=False,
    timeout=10,
  )

  assert result.returncode == 0, result.stderr


def test_clear_retries_only_unprocessed_items_with_full_jitter(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  first = _delete_request("first")
  second = _delete_request("second")
  timeline: list[str] = []
  table.scan.return_value = {"Items": [{"pk": "first"}, {"pk": "second"}]}
  responses = iter(
    [
      {"UnprocessedItems": {_TABLE_NAME: [second]}},
      {"UnprocessedItems": {}},
    ]
  )

  def batch_write(**_kwargs: Any) -> dict[str, Any]:
    timeline.append("write")
    return next(responses)

  def jitter(attempt: int, base_delay: float) -> float:
    timeline.append(f"jitter:{attempt}:{base_delay}")
    return 0.0125

  def record_sleep(delay: float) -> None:
    timeline.append(f"sleep:{delay}")

  client.batch_write_item.side_effect = batch_write
  backoff = mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    side_effect=jitter,
    create=True,
  )
  sleep = mocker.patch.object(dynamodb_module.time, "sleep", side_effect=record_sleep)

  backend.clear_storage()

  assert client.batch_write_item.call_args_list[0].kwargs["RequestItems"] == {
    _TABLE_NAME: [first, second]
  }
  assert client.batch_write_item.call_args_list[1].kwargs["RequestItems"] == {
    _TABLE_NAME: [second]
  }
  backoff.assert_called_once_with(0, _BASE_DELAY)
  sleep.assert_called_once_with(0.0125)
  assert timeline == [
    "write",
    "jitter:0:0.05",
    "sleep:0.0125",
    "write",
  ]


def test_clear_exhausts_unprocessed_items_with_typed_partial_failure(
  mocker, caplog,
) -> None:
  backend, table, client, _resource = _connected(mocker)
  request = _delete_request("stuck")
  table.scan.return_value = {"Items": [{"pk": "stuck"}]}
  client.batch_write_item.side_effect = [
    {"UnprocessedItems": {_TABLE_NAME: [request]}} for _ in range(_MAX_ATTEMPTS)
  ]
  backoff = mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    side_effect=[0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07],
    create=True,
  )
  sleep = mocker.patch.object(dynamodb_module.time, "sleep")
  caplog.set_level(
    logging.DEBUG, logger="scrapy_extension.backends.dynamodb"
  )

  with pytest.raises(StorageError, match="partially complete") as exc_info:
    backend.clear_storage()

  assert exc_info.value.operation == "clear_storage"
  assert exc_info.value.key is None
  assert client.batch_write_item.call_count == _MAX_ATTEMPTS
  assert backoff.call_args_list == [
    mocker.call(attempt, _BASE_DELAY) for attempt in range(_MAX_ATTEMPTS - 1)
  ]
  assert sleep.call_args_list == [
    mocker.call(0.01),
    mocker.call(0.02),
    mocker.call(0.03),
    mocker.call(0.04),
    mocker.call(0.05),
    mocker.call(0.06),
    mocker.call(0.07),
  ]
  assert "stuck" not in caplog.text


@pytest.mark.parametrize(
  "response",
  [
    None,
    [],
    {},
    {"UnprocessedItems": []},
    {"UnprocessedItems": {_TABLE_NAME: {}}},
    {"UnprocessedItems": {_TABLE_NAME: []}},
    {"UnprocessedItems": {"foreign-table": []}},
    {"UnprocessedItems": {_TABLE_NAME: [{"PutRequest": {"Item": {"pk": "key"}}}]}},
    {"UnprocessedItems": {_TABLE_NAME: [_delete_request("not-submitted")]}},
    {
      "UnprocessedItems": {
        _TABLE_NAME: [_delete_request("key"), _delete_request("key")]
      }
    },
  ],
)
def test_clear_rejects_malformed_batch_responses(mocker, response: Any) -> None:
  backend, table, client, _resource = _connected(mocker)
  table.scan.return_value = {"Items": [{"pk": "key"}]}
  client.batch_write_item.return_value = response
  backoff = mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    create=True,
  )
  sleep = mocker.patch.object(dynamodb_module.time, "sleep")

  with pytest.raises(StorageError, match="malformed") as exc_info:
    backend.clear_storage()

  assert exc_info.value.operation == "clear_storage"
  assert exc_info.value.key is None
  assert "partially complete" in str(exc_info.value)
  client.batch_write_item.assert_called_once()
  backoff.assert_not_called()
  sleep.assert_not_called()


def test_retry_attempt_index_resets_for_each_physical_batch(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  keys = [f"key-{index}" for index in range(26)]
  first_pending = _delete_request(keys[0])
  second_pending = _delete_request(keys[-1])
  table.scan.return_value = {"Items": [{"pk": key} for key in keys]}
  client.batch_write_item.side_effect = [
    {"UnprocessedItems": {_TABLE_NAME: [first_pending]}},
    {"UnprocessedItems": {}},
    {"UnprocessedItems": {_TABLE_NAME: [second_pending]}},
    {"UnprocessedItems": {}},
  ]
  backoff = mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    return_value=0.0,
    create=True,
  )
  mocker.patch.object(dynamodb_module.time, "sleep")

  backend.clear_storage()

  assert backoff.call_args_list == [
    mocker.call(0, _BASE_DELAY),
    mocker.call(0, _BASE_DELAY),
  ]


def test_clear_uses_generation_table_name_after_config_mutation(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  table.scan.return_value = {"Items": [{"pk": "key"}]}
  client.batch_write_item.return_value = {"UnprocessedItems": {}}
  backend.config.table_name = "mutated-after-connect"

  backend.clear_storage()

  assert client.batch_write_item.call_args.kwargs["RequestItems"] == {
    _TABLE_NAME: [_delete_request("key")]
  }


def test_clear_wraps_batch_write_transport_failure(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  table.scan.return_value = {"Items": [{"pk": "key"}]}
  failure = RuntimeError(
    "do-not-leak-key tenant-a: https://user:secret@example.test"
  )
  client.batch_write_item.side_effect = failure
  backoff = mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    create=True,
  )
  sleep = mocker.patch.object(dynamodb_module.time, "sleep")

  with pytest.raises(StorageError) as exc_info:
    backend.clear_storage()

  assert exc_info.value.operation == "clear_storage"
  assert exc_info.value.__cause__ is failure
  assert "secret" not in str(exc_info.value)
  assert "example.test" not in str(exc_info.value)
  assert "do-not-leak-key" not in str(exc_info.value)
  assert "tenant-a:" not in str(exc_info.value)
  client.batch_write_item.assert_called_once()
  backoff.assert_not_called()
  sleep.assert_not_called()


def test_partial_batch_failure_stops_before_later_batches(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  keys = [f"key-{index}" for index in range(51)]
  second_batch = [_delete_request(key) for key in keys[25:50]]
  table.scan.return_value = {
    "Items": [{"pk": key} for key in keys],
    "LastEvaluatedKey": {"pk": "next-page"},
  }
  client.batch_write_item.side_effect = [
    {"UnprocessedItems": {}},
    *[{"UnprocessedItems": {_TABLE_NAME: second_batch}} for _ in range(_MAX_ATTEMPTS)],
  ]
  mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    return_value=0.0,
    create=True,
  )
  mocker.patch.object(dynamodb_module.time, "sleep")

  with pytest.raises(StorageError, match="partially complete"):
    backend.clear_storage()

  assert client.batch_write_item.call_count == _MAX_ATTEMPTS + 1
  assert all(
    call.kwargs["RequestItems"] == {_TABLE_NAME: second_batch}
    for call in client.batch_write_item.call_args_list[1:]
  )
  assert all(
    _delete_request(keys[-1]) not in call.kwargs["RequestItems"][_TABLE_NAME]
    for call in client.batch_write_item.call_args_list
  )
  assert table.scan.call_count == 1


@pytest.mark.parametrize(
  "response",
  [
    None,
    [],
    {},
    {"Items": {}},
    {"Items": [{"value": b"missing-key"}]},
    {"Items": [], "LastEvaluatedKey": "not-a-key-map"},
  ],
)
def test_clear_rejects_malformed_scan_responses(mocker, response: Any) -> None:
  backend, table, client, _resource = _connected(mocker)
  table.scan.return_value = response

  with pytest.raises(StorageError, match="malformed") as exc_info:
    backend.clear_storage()

  assert exc_info.value.operation == "clear_storage"
  assert exc_info.value.key is None
  client.batch_write_item.assert_not_called()


def test_clear_rejects_non_adjacent_scan_cursor_cycle(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  cursor_a = {"pk": "cursor-a"}
  cursor_b = {"pk": "cursor-b"}
  table.scan.side_effect = [
    {"Items": [], "LastEvaluatedKey": cursor_a},
    {"Items": [], "LastEvaluatedKey": cursor_b},
    {"Items": [], "LastEvaluatedKey": cursor_a},
  ]

  with pytest.raises(StorageError, match="partially complete") as exc_info:
    backend.clear_storage()

  assert exc_info.value.operation == "clear_storage"
  assert table.scan.call_count == 3
  client.batch_write_item.assert_not_called()


def test_prefix_clear_rejects_out_of_scope_scan_item(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  table.scan.return_value = {"Items": [{"pk": "tenant-b:victim"}]}
  sleep = mocker.patch.object(dynamodb_module.time, "sleep")

  with pytest.raises(StorageError, match="out-of-scope") as exc_info:
    backend.clear_storage(prefix="tenant-a:")

  assert exc_info.value.operation == "clear_storage"
  assert exc_info.value.key is None
  client.batch_write_item.assert_not_called()
  sleep.assert_not_called()


def test_prefix_clear_allows_out_of_scope_last_evaluated_key(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  cursor = {"pk": "tenant-b:cursor"}
  table.scan.side_effect = [
    {
      "Items": [{"pk": "tenant-a:first"}],
      "LastEvaluatedKey": cursor,
    },
    {"Items": [{"pk": "tenant-a:second"}]},
  ]
  client.batch_write_item.return_value = {"UnprocessedItems": {}}

  backend.clear_storage(prefix="tenant-a:")

  assert table.scan.call_count == 2
  assert table.scan.call_args_list[1].kwargs["ExclusiveStartKey"] == cursor
  assert client.batch_write_item.call_count == 2


def test_clear_skips_batch_api_for_empty_scan_pages(mocker) -> None:
  backend, table, client, _resource = _connected(mocker)
  cursor = {"pk": "cursor"}
  table.scan.side_effect = [
    {"Items": [], "LastEvaluatedKey": cursor},
    {"Items": []},
  ]

  backend.clear_storage()

  assert table.scan.call_count == 2
  client.batch_write_item.assert_not_called()
  table.batch_writer.assert_not_called()


def test_disconnect_proceeds_during_clear_retry_backoff(mocker) -> None:
  # U2: clear_storage releases _operation_lock around each batch_write + its
  # full-jitter backoff sleep, so a concurrent disconnect is NOT blocked -- it
  # retires the generation and closes the client mid-clear. The clear then
  # re-validates the lifecycle epoch before the next page/batch and aborts
  # gracefully (no StorageError, no further scan) rather than touch the closed
  # client. Pre-fix the lock was held across the whole loop, freezing shutdown.
  backend, table, client, resource = _connected(mocker)
  request = _delete_request("key")
  timeline: list[str] = []
  # Two pages: page 1's batch retries through one backoff; disconnect retires
  # the generation during that sleep, so the clear aborts before scanning p2.
  table.scan.side_effect = [
    {"Items": [{"pk": "key"}], "LastEvaluatedKey": {"pk": "page2"}},
    {"Items": [{"pk": "second"}]},
  ]
  responses = iter([
    {"UnprocessedItems": {_TABLE_NAME: [request]}},
    {"UnprocessedItems": {}},
  ])

  def batch_write(**_kwargs: Any) -> dict[str, Any]:
    timeline.append("batch")
    return next(responses)

  client.batch_write_item.side_effect = batch_write
  resource.meta.client.close.side_effect = lambda: timeline.append("close")
  mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    return_value=0.5,
    create=True,
  )
  sleep_entered = threading.Event()
  sleep_release = threading.Event()

  def blocked_sleep(_delay: float) -> None:
    timeline.append("sleep-enter")
    sleep_entered.set()
    assert sleep_release.wait(timeout=5)
    timeline.append("sleep-exit")

  mocker.patch.object(dynamodb_module.time, "sleep", side_effect=blocked_sleep)
  errors: list[BaseException] = []

  def run(target) -> None:
    try:
      target()
    except BaseException as exc:
      errors.append(exc)

  clear_thread = threading.Thread(
    target=lambda: run(backend.clear_storage), name="clear"
  )
  clear_thread.start()
  assert sleep_entered.wait(timeout=5)
  disconnect_returned = threading.Event()

  def disconnect() -> None:
    backend.disconnect()
    disconnect_returned.set()

  disconnect_thread = threading.Thread(
    target=lambda: run(disconnect), name="disconnect"
  )
  disconnect_thread.start()
  disconnect_thread.join(timeout=2.0)
  disconnect_blocked = disconnect_thread.is_alive()
  close_calls_while_backing_off = resource.meta.client.close.call_count
  sleep_release.set()
  _join(clear_thread)
  _join(disconnect_thread)

  assert not disconnect_blocked, (
    "disconnect() was blocked while clear_storage was parked in backoff sleep"
  )
  # disconnect retired the generation and closed the client during the backoff.
  assert close_calls_while_backing_off == 1
  # The clear completed page 1's batch retry, then re-validated the epoch before
  # page 2 and aborted gracefully -- page 2 was never scanned, no error raised.
  assert errors == []
  assert table.scan.call_count == 1
  assert client.batch_write_item.call_count == 2
  resource.meta.client.close.assert_called_once_with()
  assert backend.is_connected() is False
  assert timeline == [
    "batch",
    "sleep-enter",
    "close",
    "sleep-exit",
    "batch",
  ]


def test_store_proceeds_during_clear_retry_backoff(mocker) -> None:
  # U2: clear_storage releases _operation_lock around each batch_write + its
  # full-jitter backoff sleep, so a concurrent store is NOT blocked while the
  # clear is parked retrying UnprocessedItems. Pre-fix the lock was held across
  # the whole paginated scan + batch loop, freezing the storage pipeline.
  backend, table, client, _resource = _connected(mocker)
  request = _delete_request("clear-key")
  timeline: list[str] = []
  table.scan.return_value = {"Items": [{"pk": "clear-key"}]}
  responses = iter([
    {"UnprocessedItems": {_TABLE_NAME: [request]}},
    {"UnprocessedItems": {}},
  ])

  def batch_write(**_kwargs: Any) -> dict[str, Any]:
    timeline.append("batch")
    return next(responses)

  client.batch_write_item.side_effect = batch_write
  table.put_item.side_effect = lambda **_kwargs: timeline.append("store")
  mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    return_value=0.5,
    create=True,
  )
  sleep_entered = threading.Event()
  sleep_release = threading.Event()

  def blocked_sleep(_delay: float) -> None:
    timeline.append("sleep-enter")
    sleep_entered.set()
    assert sleep_release.wait(timeout=5)
    timeline.append("sleep-exit")

  mocker.patch.object(dynamodb_module.time, "sleep", side_effect=blocked_sleep)
  errors: list[BaseException] = []

  def run(target) -> None:
    try:
      target()
    except BaseException as exc:
      errors.append(exc)

  clear_thread = threading.Thread(
    target=lambda: run(backend.clear_storage), name="clear"
  )
  clear_thread.start()
  assert sleep_entered.wait(timeout=5)
  # The clear is parked in its backoff sleep with the lock released; a concurrent
  # store must slip through and complete immediately (not wait for sleep-exit).
  store_thread = threading.Thread(
    target=lambda: run(lambda: backend.store("stored-after", b"value")),
    name="store",
  )
  store_thread.start()
  store_thread.join(timeout=2.0)
  store_blocked = store_thread.is_alive()
  put_calls_while_backing_off = table.put_item.call_count
  sleep_release.set()
  _join(clear_thread)
  _join(store_thread)

  assert not store_blocked, (
    "store() was blocked while clear_storage was parked in backoff sleep"
  )
  assert put_calls_while_backing_off == 1
  assert errors == []
  assert timeline == [
    "batch",
    "sleep-enter",
    "store",
    "sleep-exit",
    "batch",
  ]
  table.put_item.assert_called_once_with(
    Item={"pk": "stored-after", "value": b"value"}
  )


@pytest.mark.parametrize("injection_point", ["batch", "jitter", "sleep"])
def test_clear_propagates_base_exception_and_releases_lock(
  mocker, injection_point: str
) -> None:
  backend, table, client, _resource = _connected(mocker)
  request = _delete_request("key")
  table.scan.return_value = {"Items": [{"pk": "key"}]}
  if injection_point == "batch":
    client.batch_write_item.side_effect = KeyboardInterrupt
  else:
    client.batch_write_item.side_effect = [
      {"UnprocessedItems": {_TABLE_NAME: [request]}},
      {"UnprocessedItems": {}},
    ]
  mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    side_effect=(
      KeyboardInterrupt if injection_point == "jitter" else None
    ),
    return_value=0.0,
    create=True,
  )
  mocker.patch.object(
    dynamodb_module.time,
    "sleep",
    side_effect=(KeyboardInterrupt if injection_point == "sleep" else None),
  )

  with pytest.raises(KeyboardInterrupt):
    backend.clear_storage()

  assert client.batch_write_item.call_count == 1
  backend.store("after-interrupt", b"value")
  table.put_item.assert_called_once_with(
    Item={"pk": "after-interrupt", "value": b"value"}
  )
