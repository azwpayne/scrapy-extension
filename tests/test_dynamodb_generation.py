"""Concurrency and connection-generation contracts for DynamoDB."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest

from scrapy_extension.backends import dynamodb as dynamodb_module
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import DynamoDBSettings


def _backend(**overrides: Any) -> DynamoDBBackend:
  return DynamoDBBackend(DynamoDBSettings(**overrides))


def _resource(mocker: Any, table: Any | None = None) -> tuple[Any, Any]:
  resource = mocker.MagicMock()
  if table is None:
    table = mocker.MagicMock()
  table.load.return_value = None
  table.table_status = "ACTIVE"
  resource.Table.return_value = table
  table.meta.client = resource.meta.client
  resource.meta.client.batch_write_item.return_value = {"UnprocessedItems": {}}
  return resource, table


def _patch_resource(mocker, *, return_value=None, side_effect=None):
  """Patch a private candidate Session and return Session/resource mocks."""
  session = mocker.MagicMock()
  if side_effect is not None:
    session.resource.side_effect = side_effect
  else:
    session.resource.return_value = return_value
  session_factory = mocker.patch.object(
    dynamodb_module.boto3.session,
    "Session",
    return_value=session,
  )
  return session_factory, session.resource


def _client_error(code: str) -> Exception:
  error = Exception(code)
  error.response = {"Error": {"Code": code}}  # type: ignore[attr-defined]
  return error


def _thread_call(
  target: Callable[[], None], errors: list[BaseException], *, name: str
) -> threading.Thread:
  def run() -> None:
    try:
      target()
    except BaseException as exc:  # surfaced in the parent thread below
      errors.append(exc)

  thread = threading.Thread(target=run, name=name)
  thread.start()
  return thread


def _join(thread: threading.Thread) -> None:
  thread.join(timeout=5)
  assert not thread.is_alive()


class _ObservedRLock:
  """RLock wrapper exposing when selected threads reach lock admission."""

  def __init__(self) -> None:
    self._lock = threading.RLock()
    self.attempts: dict[str, threading.Event] = {}

  def observe(self, thread_name: str) -> threading.Event:
    event = threading.Event()
    self.attempts[thread_name] = event
    return event

  def __enter__(self) -> _ObservedRLock:
    event = self.attempts.get(threading.current_thread().name)
    if event is not None:
      event.set()
    self._lock.acquire()
    return self

  def __exit__(self, *_args: object) -> None:
    self._lock.release()


def test_candidate_is_private_until_ready_and_failure_closes_it(mocker) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  entered = threading.Event()
  release = threading.Event()
  failure = RuntimeError("load failed")

  def blocked_load() -> None:
    entered.set()
    assert release.wait(timeout=5)
    raise failure

  table.load.side_effect = blocked_load
  _patch_resource(mocker, return_value=resource)
  errors: list[BaseException] = []
  thread = _thread_call(backend.connect, errors, name="connect")

  assert entered.wait(timeout=5)
  connected_while_preparing = backend.is_connected()
  resource_while_preparing = backend._resource
  table_while_preparing = backend._table
  release.set()
  _join(thread)

  assert connected_while_preparing is False
  assert resource_while_preparing is None
  assert table_while_preparing is None
  assert len(errors) == 1
  assert isinstance(errors[0], BackendConnectionError)
  assert errors[0].__cause__ is failure
  resource.meta.client.close.assert_called_once_with()


def test_concurrent_connect_is_single_flight_and_live_connect_is_idempotent(
  mocker,
) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  entered = threading.Event()
  release = threading.Event()

  def blocked_factory(*_args: object, **_kwargs: object) -> Any:
    entered.set()
    assert release.wait(timeout=5)
    return resource

  _, factory = _patch_resource(mocker, side_effect=blocked_factory)
  errors: list[BaseException] = []
  first = _thread_call(backend.connect, errors, name="connect-1")
  assert entered.wait(timeout=5)
  second_started = threading.Event()

  def second_connect() -> None:
    second_started.set()
    backend.connect()

  second = _thread_call(second_connect, errors, name="connect-2")
  assert second_started.wait(timeout=5)
  release.set()
  _join(first)
  _join(second)
  backend.connect()

  assert errors == []
  factory.assert_called_once()
  resource.Table.assert_called_once()
  table.load.assert_called_once()


def test_disconnect_fences_in_progress_candidate(mocker) -> None:
  backend = _backend()
  resource, _table = _resource(mocker)
  entered = threading.Event()
  release = threading.Event()

  def blocked_factory(*_args: object, **_kwargs: object) -> Any:
    entered.set()
    assert release.wait(timeout=5)
    return resource

  _patch_resource(mocker, side_effect=blocked_factory)
  errors: list[BaseException] = []
  thread = _thread_call(backend.connect, errors, name="connect")
  assert entered.wait(timeout=5)

  backend.disconnect()
  release.set()
  _join(thread)

  assert errors == []
  assert backend.is_connected() is False
  resource.Table.assert_not_called()
  resource.meta.client.close.assert_called_once_with()


def test_disconnect_prevents_late_candidate_table_creation(mocker) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  load_entered = threading.Event()
  load_release = threading.Event()

  def missing_after_disconnect() -> None:
    load_entered.set()
    assert load_release.wait(timeout=5)
    raise _client_error("ResourceNotFoundException")

  table.load.side_effect = missing_after_disconnect
  _patch_resource(mocker, return_value=resource)
  errors: list[BaseException] = []
  thread = _thread_call(backend.connect, errors, name="connect")
  assert load_entered.wait(timeout=5)

  backend.disconnect()
  load_release.set()
  _join(thread)

  assert errors == []
  resource.create_table.assert_not_called()
  resource.meta.client.close.assert_called_once_with()
  assert backend.is_connected() is False


def test_disconnect_drains_an_admitted_table_creation(mocker) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  created_table = mocker.MagicMock()
  table.load.side_effect = _client_error("ResourceNotFoundException")
  create_entered = threading.Event()
  create_release = threading.Event()

  def blocked_create(**_kwargs: object) -> Any:
    create_entered.set()
    assert create_release.wait(timeout=5)
    return created_table

  resource.create_table.side_effect = blocked_create
  _patch_resource(mocker, return_value=resource)
  observed_lock = _ObservedRLock()
  disconnect_attempted = observed_lock.observe("disconnect")
  backend._operation_lock = observed_lock
  errors: list[BaseException] = []
  connect_thread = _thread_call(backend.connect, errors, name="connect")
  assert create_entered.wait(timeout=5)
  disconnect_returned = threading.Event()

  def disconnect() -> None:
    backend.disconnect()
    disconnect_returned.set()

  disconnect_thread = _thread_call(disconnect, errors, name="disconnect")
  attempted = disconnect_attempted.wait(timeout=5)
  returned_while_create_blocked = disconnect_returned.is_set()
  close_calls_while_create_blocked = resource.meta.client.close.call_count
  create_release.set()
  _join(connect_thread)
  _join(disconnect_thread)

  assert attempted
  assert returned_while_create_blocked is False
  assert close_calls_while_create_blocked == 0
  assert errors == []
  resource.create_table.assert_called_once()
  resource.meta.client.close.assert_called_once_with()
  assert backend.is_connected() is False


def test_disconnect_fences_queued_connect_intent(mocker) -> None:
  backend = _backend()
  resource, _table = _resource(mocker)
  factory_entered = threading.Event()
  factory_release = threading.Event()
  second_intent = threading.Event()
  original_capture = backend._capture_connect_intent
  capture_count = 0
  capture_count_lock = threading.Lock()

  def observed_capture() -> tuple[int, bool]:
    nonlocal capture_count
    result = original_capture()
    with capture_count_lock:
      capture_count += 1
      if capture_count == 2:
        second_intent.set()
    return result

  mocker.patch.object(backend, "_capture_connect_intent", observed_capture)

  def blocked_factory(*_args: object, **_kwargs: object) -> Any:
    factory_entered.set()
    assert factory_release.wait(timeout=5)
    return resource

  _, factory = _patch_resource(mocker, side_effect=blocked_factory)
  errors: list[BaseException] = []
  first = _thread_call(backend.connect, errors, name="connect-1")
  assert factory_entered.wait(timeout=5)
  second = _thread_call(backend.connect, errors, name="connect-2")
  assert second_intent.wait(timeout=5)

  backend.disconnect()
  factory_release.set()
  _join(first)
  _join(second)

  assert errors == []
  factory.assert_called_once()
  assert backend.is_connected() is False
  resource.meta.client.close.assert_called_once_with()


def test_connected_settings_are_frozen_until_explicit_reconnect(mocker) -> None:
  backend = _backend(table_name="table-a", region_name="us-east-1")
  resource_a, table_a = _resource(mocker)
  resource_b, table_b = _resource(mocker)
  _, factory = _patch_resource(
    mocker, side_effect=[resource_a, resource_b]
  )

  backend.connect()
  backend.config.table_name = "table-b"
  backend.config.region_name = "eu-west-1"
  backend.connect()
  backend.store("key", b"old-generation")

  factory.assert_called_once()
  resource_a.Table.assert_called_once_with("table-a")
  table_a.put_item.assert_called_once()
  table_b.put_item.assert_not_called()

  backend.disconnect()
  backend.connect()
  backend.store("key", b"new-generation")

  assert factory.call_count == 2
  assert factory.call_args_list[1].kwargs["region_name"] == "eu-west-1"
  resource_b.Table.assert_called_once_with("table-b")
  table_b.put_item.assert_called_once()
  resource_a.meta.client.close.assert_called_once_with()


def test_connect_revalidates_mutated_region_before_boto3(mocker) -> None:
  backend = _backend()
  backend.config.region_name = "not-a-region"
  session_factory, factory = _patch_resource(mocker)

  with pytest.raises(ConfigurationError) as exc_info:
    backend.connect()

  assert exc_info.value.setting_name == "region_name"
  session_factory.assert_not_called()
  factory.assert_not_called()


def test_each_candidate_uses_a_private_boto3_session(mocker) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  private_session = mocker.MagicMock()
  private_session.resource.return_value = resource
  session_factory = mocker.patch.object(
    dynamodb_module.boto3.session,
    "Session",
    return_value=private_session,
  )
  default_resource = mocker.patch.object(
    dynamodb_module.boto3,
    "resource",
    side_effect=AssertionError("shared default Session must not be used"),
  )

  backend.connect()

  session_factory.assert_called_once_with()
  private_session.resource.assert_called_once()
  table.load.assert_called_once()
  default_resource.assert_not_called()


def test_existing_transitional_table_is_private_until_waiter_succeeds(
  mocker,
) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  table.table_status = "CREATING"
  waiter_entered = threading.Event()
  waiter_release = threading.Event()
  failure = RuntimeError("table never became active")

  def failed_waiter() -> None:
    waiter_entered.set()
    assert waiter_release.wait(timeout=5)
    raise failure

  table.wait_until_exists.side_effect = failed_waiter
  _patch_resource(mocker, return_value=resource)
  errors: list[BaseException] = []
  thread = _thread_call(backend.connect, errors, name="connect")
  assert waiter_entered.wait(timeout=5)
  connected_while_waiting = backend.is_connected()
  waiter_release.set()
  _join(thread)

  assert connected_while_waiting is False
  assert len(errors) == 1
  assert isinstance(errors[0], BackendConnectionError)
  assert errors[0].__cause__ is failure
  resource.meta.client.close.assert_called_once_with()


@pytest.mark.parametrize(
  ("table_status", "expected"),
  [
    ("ACTIVE", True),
    ("UPDATING", True),
    ("CREATING", False),
    ("DELETING", False),
  ],
)
def test_ping_requires_a_data_plane_usable_table_status(
  mocker, table_status: str, expected: bool
) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  _patch_resource(mocker, return_value=resource)
  backend.connect()
  table.table_status = table_status

  assert backend.ping() is expected


@pytest.mark.parametrize(
  ("method_name", "table_method"),
  [
    ("retrieve", "get_item"),
    ("delete", "delete_item"),
    ("exists", "get_item"),
    ("ttl", "get_item"),
    ("ping", "load"),
    ("clear_storage", "scan"),
  ],
)
def test_every_sdk_operation_is_serialized_behind_store(
  mocker, method_name: str, table_method: str
) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  _patch_resource(mocker, return_value=resource)
  backend.connect()
  table.load.reset_mock()
  table.get_item.reset_mock()
  table.delete_item.reset_mock()
  table.scan.reset_mock()
  table.get_item.return_value = {}
  table.delete_item.return_value = {}
  table.scan.return_value = {"Items": []}
  observed_lock = _ObservedRLock()
  contender_attempted = observed_lock.observe(method_name)
  backend._operation_lock = observed_lock
  put_entered = threading.Event()
  put_release = threading.Event()

  def blocked_put(**_kwargs: object) -> None:
    put_entered.set()
    assert put_release.wait(timeout=5)

  def contend() -> None:
    method = getattr(backend, method_name)
    if method_name in {"ping", "clear_storage"}:
      method()
    else:
      method("key")

  table.put_item.side_effect = blocked_put
  errors: list[BaseException] = []
  store_thread = _thread_call(
    lambda: backend.store("key", b"value"), errors, name="store"
  )
  assert put_entered.wait(timeout=5)
  contender_thread = _thread_call(contend, errors, name=method_name)
  attempted = contender_attempted.wait(timeout=5)
  calls_while_store_blocked = getattr(table, table_method).call_count
  put_release.set()
  _join(store_thread)
  _join(contender_thread)

  assert attempted
  assert calls_while_store_blocked == 0
  assert errors == []
  getattr(table, table_method).assert_called_once()


@pytest.mark.parametrize(
  ("method_name", "operation", "expected_key"),
  [
    ("store", "store", "key"),
    ("retrieve", "retrieve", "key"),
    ("delete", "delete", "key"),
    ("exists", "exists", "key"),
    ("ttl", "ttl", "key"),
    ("clear_storage", "clear_storage", None),
  ],
)
def test_storage_operations_after_disconnect_fail_without_stale_table_io(
  mocker, method_name: str, operation: str, expected_key: str | None
) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  _patch_resource(mocker, return_value=resource)
  backend.connect()
  backend.disconnect()
  table.reset_mock()

  with pytest.raises(StorageError) as exc_info:
    if method_name == "store":
      backend.store("key", b"value")
    elif method_name == "clear_storage":
      backend.clear_storage()
    else:
      getattr(backend, method_name)("key")

  assert exc_info.value.operation == operation
  assert exc_info.value.key == expected_key
  assert table.method_calls == []


def test_sdk_operations_are_serialized(mocker) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  _patch_resource(mocker, return_value=resource)
  backend.connect()
  observed_lock = _ObservedRLock()
  contender_attempted = observed_lock.observe("retrieve")
  backend._operation_lock = observed_lock
  put_entered = threading.Event()
  put_release = threading.Event()

  def blocked_put(**_kwargs: object) -> None:
    put_entered.set()
    assert put_release.wait(timeout=5)

  table.put_item.side_effect = blocked_put
  table.get_item.return_value = {}
  errors: list[BaseException] = []
  store_thread = _thread_call(
    lambda: backend.store("key", b"value"), errors, name="store"
  )
  assert put_entered.wait(timeout=5)
  retrieve_thread = _thread_call(
    lambda: backend.retrieve("key"), errors, name="retrieve"
  )

  attempted = contender_attempted.wait(timeout=5)
  get_calls_while_store_blocked = table.get_item.call_count
  put_release.set()
  _join(store_thread)
  _join(retrieve_thread)

  assert attempted
  assert get_calls_while_store_blocked == 0
  assert errors == []
  table.get_item.assert_called_once()


def test_disconnect_drains_inflight_store_before_closing_resource(mocker) -> None:
  backend = _backend()
  resource, table = _resource(mocker)
  _patch_resource(mocker, return_value=resource)
  backend.connect()
  observed_lock = _ObservedRLock()
  disconnect_attempted = observed_lock.observe("disconnect")
  backend._operation_lock = observed_lock
  put_entered = threading.Event()
  put_release = threading.Event()

  def blocked_put(**_kwargs: object) -> None:
    put_entered.set()
    assert put_release.wait(timeout=5)

  table.put_item.side_effect = blocked_put
  errors: list[BaseException] = []
  store_thread = _thread_call(
    lambda: backend.store("key", b"value"), errors, name="store"
  )
  assert put_entered.wait(timeout=5)
  disconnect_thread = _thread_call(
    backend.disconnect, errors, name="disconnect"
  )

  attempted = disconnect_attempted.wait(timeout=5)
  close_calls_while_store_blocked = resource.meta.client.close.call_count
  put_release.set()
  _join(store_thread)
  _join(disconnect_thread)

  assert attempted
  assert close_calls_while_store_blocked == 0
  assert errors == []
  resource.meta.client.close.assert_called_once_with()
  assert backend.is_connected() is False


@pytest.mark.parametrize(
  ("method_name", "missing_result"),
  [("retrieve", None), ("exists", False), ("ttl", None)],
)
def test_lazy_ttl_reap_stays_on_issuing_generation(
  mocker, method_name: str, missing_result: object
) -> None:
  backend = _backend(table_name="table-a")
  resource_a, table_a = _resource(mocker)
  resource_b, table_b = _resource(mocker)
  _, factory = _patch_resource(
    mocker, side_effect=[resource_a, resource_b]
  )
  backend.connect()
  observed_lock = _ObservedRLock()
  rollover_attempted = observed_lock.observe("rollover")
  backend._operation_lock = observed_lock
  expiry_entered = threading.Event()
  expiry_release = threading.Event()

  class BlockingExpiredItem(dict[str, Any]):
    def get(self, key: str, default: Any = None) -> Any:
      if key == "expire_at":
        expiry_entered.set()
        assert expiry_release.wait(timeout=5)
      return super().get(key, default)

  table_a.get_item.return_value = {
    "Item": BlockingExpiredItem(
      pk="key", value=b"value", expire_at=1.0
    )
  }
  errors: list[BaseException] = []
  results: list[object] = []
  read_thread = _thread_call(
    lambda: results.append(getattr(backend, method_name)("key")),
    errors,
    name="read",
  )
  assert expiry_entered.wait(timeout=5)

  def rollover() -> None:
    backend.disconnect()
    backend.config.table_name = "table-b"
    backend.connect()

  rollover_thread = _thread_call(rollover, errors, name="rollover")
  attempted = rollover_attempted.wait(timeout=5)
  factory_calls_while_read_blocked = factory.call_count
  expiry_release.set()
  _join(read_thread)
  _join(rollover_thread)

  assert attempted
  assert factory_calls_while_read_blocked == 1
  assert errors == []
  assert results == [missing_result]
  table_a.delete_item.assert_called_once()
  table_b.delete_item.assert_not_called()
  assert factory.call_count == 2


def test_paginated_clear_stays_on_issuing_generation(mocker) -> None:
  backend = _backend(table_name="table-a")
  resource_a, table_a = _resource(mocker)
  resource_b, table_b = _resource(mocker)
  _, factory = _patch_resource(
    mocker, side_effect=[resource_a, resource_b]
  )
  backend.connect()
  observed_lock = _ObservedRLock()
  rollover_attempted = observed_lock.observe("rollover")
  backend._operation_lock = observed_lock
  scan_entered = threading.Event()
  scan_release = threading.Event()
  cursor = {"pk": "first"}

  scan_count = 0

  def scan_pages(**_kwargs: object) -> dict[str, Any]:
    nonlocal scan_count
    scan_count += 1
    if scan_count == 1:
      scan_entered.set()
      assert scan_release.wait(timeout=5)
      return {"Items": [{"pk": "first"}], "LastEvaluatedKey": cursor}
    return {"Items": [{"pk": "second"}]}

  table_a.scan.side_effect = scan_pages
  errors: list[BaseException] = []
  clear_thread = _thread_call(backend.clear_storage, errors, name="clear")
  assert scan_entered.wait(timeout=5)

  def rollover() -> None:
    backend.disconnect()
    backend.config.table_name = "table-b"
    backend.connect()

  rollover_thread = _thread_call(rollover, errors, name="rollover")
  attempted = rollover_attempted.wait(timeout=5)
  table_b_scans_while_clear_blocked = table_b.scan.call_count
  factory_calls_while_clear_blocked = factory.call_count
  close_calls_while_clear_blocked = resource_a.meta.client.close.call_count
  scan_release.set()
  _join(clear_thread)
  _join(rollover_thread)

  assert attempted
  assert table_b_scans_while_clear_blocked == 0
  assert factory_calls_while_clear_blocked == 1
  assert close_calls_while_clear_blocked == 0
  assert errors == []
  assert table_a.scan.call_count == 2
  assert [
    call.kwargs["RequestItems"]
    for call in resource_a.meta.client.batch_write_item.call_args_list
  ] == [
    {"table-a": [{"DeleteRequest": {"Key": {"pk": "first"}}}]},
    {"table-a": [{"DeleteRequest": {"Key": {"pk": "second"}}}]},
  ]
  table_b.scan.assert_not_called()
  resource_b.meta.client.batch_write_item.assert_not_called()
  table_b.batch_writer.assert_not_called()
  assert factory.call_count == 2
  resource_a.meta.client.close.assert_called_once_with()
