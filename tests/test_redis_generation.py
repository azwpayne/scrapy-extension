"""Deterministic lifecycle contracts for Redis client generations."""

from __future__ import annotations

import threading
import warnings
from collections.abc import Callable
from typing import Any

import pytest

from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.exceptions import (
  BackendConnectionError,
  ConfigurationError,
  QueueError,
)
from scrapy_extension.settings import RedisMode, RedisSettings


def _client(mocker, name: str):
  client = mocker.MagicMock(name=name)
  client.ping.return_value = True
  client.set.return_value = True
  return client


def _run_in_thread(call: Callable[[], Any]):
  values: list[Any] = []
  errors: list[BaseException] = []

  def target() -> None:
    try:
      values.append(call())
    except BaseException as exc:  # pragma: no cover - asserted by callers
      errors.append(exc)

  thread = threading.Thread(target=target, daemon=True)
  thread.start()
  return thread, values, errors


def _join(thread: threading.Thread) -> None:
  thread.join(timeout=2)
  assert not thread.is_alive(), "Redis lifecycle test thread did not terminate"


def test_repeated_connect_is_idempotent_and_keeps_one_client(mocker) -> None:
  first = _client(mocker, "first")
  second = _client(mocker, "second")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[first, second]
  )
  backend = RedisBackend(RedisSettings())

  backend.connect()
  backend.connect()

  constructor.assert_called_once()
  assert backend.client is first
  first.close.assert_not_called()
  second.close.assert_not_called()


def test_lazy_operation_cannot_observe_candidate_before_ping_completes(
  mocker,
) -> None:
  ping_started = threading.Event()
  release_ping = threading.Event()
  operation_connect_entered = threading.Event()
  candidate = _client(mocker, "candidate")

  def blocked_ping() -> bool:
    ping_started.set()
    assert release_ping.wait(timeout=2)
    return True

  candidate.ping.side_effect = blocked_ping
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=candidate)
  backend = RedisBackend(RedisSettings())
  original_connect_for_epoch = backend._connect_for_epoch
  connect_calls = 0
  connect_calls_lock = threading.Lock()

  def tracked_connect_for_epoch(epoch: int) -> bool:
    nonlocal connect_calls
    with connect_calls_lock:
      connect_calls += 1
      if connect_calls == 2:
        operation_connect_entered.set()
    return original_connect_for_epoch(epoch)

  mocker.patch.object(
    backend, "_connect_for_epoch", side_effect=tracked_connect_for_epoch
  )

  connect_thread, _connect_values, connect_errors = _run_in_thread(backend.connect)
  assert ping_started.wait(timeout=2)
  store_thread, _store_values, store_errors = _run_in_thread(
    lambda: backend.store("key", b"value")
  )

  assert operation_connect_entered.wait(timeout=2)
  candidate.set.assert_not_called()
  release_ping.set()
  _join(connect_thread)
  _join(store_thread)

  assert connect_errors == []
  assert store_errors == []
  candidate.set.assert_called_once_with("scrapy-extension:storage:key", b"value")


def test_disconnect_fences_and_closes_candidate_still_in_health_check(
  mocker,
) -> None:
  ping_started = threading.Event()
  release_ping = threading.Event()
  candidate = _client(mocker, "candidate")

  def blocked_ping() -> bool:
    ping_started.set()
    assert release_ping.wait(timeout=2)
    return True

  candidate.ping.side_effect = blocked_ping
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=candidate
  )
  backend = RedisBackend(RedisSettings())

  connect_thread, _values, connect_errors = _run_in_thread(backend.connect)
  assert ping_started.wait(timeout=2)
  backend.disconnect()
  release_ping.set()
  _join(connect_thread)

  assert connect_errors == []
  assert backend.is_connected() is False
  constructor.assert_called_once()
  candidate.close.assert_called_once()


def test_failed_candidate_closes_locally_and_retry_publishes_fresh_client(
  mocker,
) -> None:
  failed = _client(mocker, "failed")
  failed.ping.return_value = False
  healthy = _client(mocker, "healthy")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[failed, healthy]
  )
  backend = RedisBackend(RedisSettings())

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()

  assert exc_info.value.backend_type == "redis"
  assert backend._generation is None
  assert backend._client is None
  assert backend._master_client is None
  assert backend._sentinel is None
  failed.close.assert_called_once()

  backend.connect()
  assert backend.client is healthy
  assert constructor.call_count == 2
  healthy.close.assert_not_called()


def test_interrupted_candidate_publication_rolls_back_and_closes(mocker) -> None:
  interrupted = _client(mocker, "interrupted")
  healthy = _client(mocker, "healthy")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[interrupted, healthy]
  )
  backend = RedisBackend(RedisSettings())
  notify = mocker.patch.object(
    backend._generation_condition,
    "notify_all",
    side_effect=[KeyboardInterrupt, None],
  )

  with pytest.raises(KeyboardInterrupt):
    backend.connect()

  assert backend._generation is None
  assert backend._client is None
  assert backend._master_client is None
  assert backend._sentinel is None
  interrupted.close.assert_called_once()

  notify.side_effect = None
  backend.connect()
  assert backend.client is healthy
  assert constructor.call_count == 2


def test_published_generation_connect_is_idempotent_even_after_failed_ping(
  mocker,
) -> None:
  client = _client(mocker, "published")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )
  backend = RedisBackend(RedisSettings())
  backend.connect()
  client.ping.return_value = False
  assert backend.ping() is False

  backend.connect()

  constructor.assert_called_once()
  assert backend.client is client


def test_mutated_wrong_type_cannot_leak_through_pydantic_warning(mocker) -> None:
  marker = "USER:PASSWORD@internal:6379"
  settings = RedisSettings(
    mode=RedisMode.CLUSTER, cluster_startup_nodes=["safe:6379"]
  )
  settings.cluster_startup_nodes = [marker.encode()]
  constructor = mocker.patch("scrapy_extension.backends.redis.RedisCluster")

  with warnings.catch_warnings():
    warnings.simplefilter("error", UserWarning)
    with pytest.raises(ConfigurationError) as exc_info:
      RedisBackend(settings).connect()

  assert marker not in str(exc_info.value)
  assert marker not in repr(exc_info.value.setting_value)
  assert exc_info.value.__cause__ is None
  constructor.assert_not_called()


def test_connect_sanitizes_mutated_sentinel_validator_context(mocker) -> None:
  marker = "USER:PASSWORD@internal:26379"
  settings = RedisSettings()
  settings.mode = RedisMode.SENTINEL
  settings.sentinels = [marker]
  settings.sentinel_master_name = ""
  constructor = mocker.patch("scrapy_extension.backends.redis.Sentinel")

  with pytest.raises(ConfigurationError) as exc_info:
    RedisBackend(settings).connect()

  assert exc_info.value.setting_name == "sentinel_master_name"
  assert marker not in str(exc_info.value)
  assert marker not in repr(exc_info.value.setting_value)
  assert exc_info.value.__cause__ is None
  constructor.assert_not_called()


def test_connect_intent_queued_before_disconnect_cannot_publish_after_it(
  mocker,
) -> None:
  ping_started = threading.Event()
  release_ping = threading.Event()
  second_intent_entered = threading.Event()
  first = _client(mocker, "first")
  second = _client(mocker, "second")

  def blocked_ping() -> bool:
    ping_started.set()
    assert release_ping.wait(timeout=2)
    return True

  first.ping.side_effect = blocked_ping
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[first, second]
  )
  backend = RedisBackend(RedisSettings())
  original_connect_for_epoch = backend._connect_for_epoch
  call_count = 0
  call_count_lock = threading.Lock()

  def tracked_connect_for_epoch(epoch: int) -> bool:
    nonlocal call_count
    with call_count_lock:
      call_count += 1
      if call_count == 2:
        second_intent_entered.set()
    return original_connect_for_epoch(epoch)

  mocker.patch.object(
    backend, "_connect_for_epoch", side_effect=tracked_connect_for_epoch
  )
  first_thread, _first_values, first_errors = _run_in_thread(backend.connect)
  assert ping_started.wait(timeout=2)
  queued_thread, _queued_values, queued_errors = _run_in_thread(backend.connect)
  assert second_intent_entered.wait(timeout=2)

  backend.disconnect()
  release_ping.set()
  _join(first_thread)
  _join(queued_thread)

  assert first_errors == []
  assert queued_errors == []
  constructor.assert_called_once()
  first.close.assert_called_once()
  assert backend.is_connected() is False

  backend.connect()
  assert backend.client is second
  assert constructor.call_count == 2


def test_disconnect_drains_admitted_operation_before_close(mocker) -> None:
  set_started = threading.Event()
  release_set = threading.Event()
  closed = threading.Event()
  client = _client(mocker, "leased")

  def blocked_set(*_args, **_kwargs) -> bool:
    set_started.set()
    assert release_set.wait(timeout=2)
    return True

  client.set.side_effect = blocked_set
  client.close.side_effect = closed.set
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  generation = backend._generation
  assert generation is not None

  store_thread, _values, store_errors = _run_in_thread(
    lambda: backend.store("key", b"value")
  )
  assert set_started.wait(timeout=2)
  disconnect_thread, _disconnect_values, disconnect_errors = _run_in_thread(
    backend.disconnect
  )
  assert generation.retired.wait(timeout=2)

  with pytest.raises(BackendConnectionError, match="disconnecting") as exc_info:
    backend.store("new-key", b"new-value")
  assert exc_info.value.backend_type == "redis"

  assert closed.is_set() is False
  release_set.set()
  _join(store_thread)
  _join(disconnect_thread)

  assert store_errors == []
  assert disconnect_errors == []
  client.close.assert_called_once()


def test_interrupted_disconnect_finishes_drain_and_cleanup_before_reraising(
  mocker,
) -> None:
  set_started = threading.Event()
  release_set = threading.Event()
  interrupt_observed = threading.Event()
  first = _client(mocker, "first")
  second = _client(mocker, "second")

  def blocked_set(*_args, **_kwargs) -> bool:
    set_started.set()
    assert release_set.wait(timeout=2)
    return True

  first.set.side_effect = blocked_set
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[first, second]
  )
  backend = RedisBackend(RedisSettings())
  backend.connect()
  store_thread, _store_values, store_errors = _run_in_thread(
    lambda: backend.store("key", b"value")
  )
  assert set_started.wait(timeout=2)

  original_wait = backend._generation_condition.wait

  def interrupt_once(timeout=None):
    if not interrupt_observed.is_set():
      interrupt_observed.set()
      raise KeyboardInterrupt
    return original_wait(timeout)

  mocker.patch.object(
    backend._generation_condition, "wait", side_effect=interrupt_once
  )
  disconnect_thread, _values, disconnect_errors = _run_in_thread(
    backend.disconnect
  )
  assert interrupt_observed.wait(timeout=2)
  assert disconnect_thread.is_alive()
  release_set.set()
  _join(store_thread)
  _join(disconnect_thread)

  assert store_errors == []
  assert len(disconnect_errors) == 1
  assert isinstance(disconnect_errors[0], KeyboardInterrupt)
  assert backend._disconnecting is False
  assert backend._disconnect_owner is None
  assert backend._generation is None
  first.close.assert_called_once()

  backend.connect()
  assert backend.client is second
  assert constructor.call_count == 2


def test_interrupted_retirement_signal_still_detaches_and_closes(mocker) -> None:
  client = _client(mocker, "retirement-interrupted")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  generation = backend._generation
  assert generation is not None
  original_set = generation.retired.set
  set_calls = 0

  def interrupt_once() -> None:
    nonlocal set_calls
    set_calls += 1
    if set_calls == 1:
      raise KeyboardInterrupt
    original_set()

  mocker.patch.object(generation.retired, "set", side_effect=interrupt_once)

  with pytest.raises(KeyboardInterrupt):
    backend.disconnect()

  assert set_calls == 2
  assert generation.retired.is_set()
  assert backend._generation is None
  assert backend._disconnecting is False
  assert backend._disconnect_owner is None
  client.close.assert_called_once()


def test_lease_count_is_released_before_thread_reentry_guard(mocker) -> None:
  client = _client(mocker, "lease-release-order")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  generation = backend._generation
  assert generation is not None

  class DisconnectWhenDepthClears:
    def __init__(self) -> None:
      object.__setattr__(self, "depth", 0)
      object.__setattr__(self, "armed", True)

    def __setattr__(self, name: str, value: Any) -> None:
      object.__setattr__(self, name, value)
      if name == "depth" and value == 0 and self.armed:
        object.__setattr__(self, "armed", False)
        backend.disconnect()

  backend._lease_local = DisconnectWhenDepthClears()  # type: ignore[assignment]

  thread, values, errors = _run_in_thread(
    lambda: backend.store("key", b"value")
  )
  _join(thread)

  assert values == [None]
  assert errors == []
  assert generation.active_leases == 0
  assert backend._generation is None
  client.close.assert_called_once()


def test_connect_reentry_from_candidate_ping_fails_fast_and_resets_guard(
  mocker,
) -> None:
  reentrant = _client(mocker, "reentrant")
  healthy = _client(mocker, "healthy")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[reentrant, healthy]
  )
  backend = RedisBackend(RedisSettings())
  reentrant.ping.side_effect = backend.connect

  thread, values, errors = _run_in_thread(backend.connect)
  _join(thread)

  assert values == []
  assert len(errors) == 1
  assert isinstance(errors[0], BackendConnectionError)
  assert "re-entrantly" in str(errors[0])
  assert errors[0].backend_type == "redis"
  assert backend._generation is None
  reentrant.close.assert_called_once()

  backend.connect()
  assert backend.client is healthy
  assert constructor.call_count == 2


def test_disconnect_reentry_from_leased_sdk_call_fails_fast(mocker) -> None:
  client = _client(mocker, "leased")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  client.set.side_effect = lambda *_args, **_kwargs: backend.disconnect()

  thread, values, errors = _run_in_thread(
    lambda: backend.store("key", b"value")
  )
  _join(thread)

  assert values == []
  assert len(errors) == 1
  assert isinstance(errors[0], BackendConnectionError)
  assert "re-entrantly" in str(errors[0])
  assert errors[0].backend_type == "redis"
  assert backend._generation is not None
  assert backend._disconnecting is False
  client.close.assert_not_called()


def test_disconnect_reentry_from_close_callback_is_idempotent(mocker) -> None:
  client = _client(mocker, "closing")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  client.close.side_effect = backend.disconnect

  thread, values, errors = _run_in_thread(backend.disconnect)
  _join(thread)

  assert values == [None]
  assert errors == []
  client.close.assert_called_once()
  assert backend._generation is None
  assert backend._disconnecting is False
  assert backend._disconnect_owner is None


def test_disconnect_reentry_from_health_probe_fails_fast(mocker) -> None:
  client = _client(mocker, "health")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  client.ping.side_effect = backend.disconnect

  thread, values, errors = _run_in_thread(backend.ping)
  _join(thread)

  assert values == []
  assert len(errors) == 1
  assert isinstance(errors[0], BackendConnectionError)
  assert "re-entrantly" in str(errors[0])
  assert backend._generation is not None
  assert backend._disconnecting is False
  client.close.assert_not_called()


def test_connect_reentry_from_operation_during_peer_disconnect_fails_fast(
  mocker,
) -> None:
  set_started = threading.Event()
  client = _client(mocker, "leased")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  generation = backend._generation
  assert generation is not None

  def reconnect_from_set(*_args, **_kwargs) -> bool:
    set_started.set()
    assert generation.retired.wait(timeout=2)
    backend.connect()
    return True

  client.set.side_effect = reconnect_from_set
  store_thread, store_values, store_errors = _run_in_thread(
    lambda: backend.store("key", b"value")
  )
  assert set_started.wait(timeout=2)
  disconnect_thread, disconnect_values, disconnect_errors = _run_in_thread(
    backend.disconnect
  )
  _join(store_thread)
  _join(disconnect_thread)

  assert store_values == []
  assert len(store_errors) == 1
  assert isinstance(store_errors[0], BackendConnectionError)
  assert "re-entrantly" in str(store_errors[0])
  assert disconnect_values == [None]
  assert disconnect_errors == []
  client.close.assert_called_once()


def test_operations_share_a_generation_without_global_io_serialization(
  mocker,
) -> None:
  calls_met = threading.Barrier(2)
  client = _client(mocker, "shared")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()

  def overlapping_set(*_args, **_kwargs) -> bool:
    calls_met.wait(timeout=2)
    return True

  client.set.side_effect = overlapping_set
  first_thread, _first_values, first_errors = _run_in_thread(
    lambda: backend.store("first", b"1")
  )
  second_thread, _second_values, second_errors = _run_in_thread(
    lambda: backend.store("second", b"2")
  )
  _join(first_thread)
  _join(second_thread)

  assert first_errors == []
  assert second_errors == []
  assert client.set.call_count == 2


def test_new_operation_after_completed_disconnect_may_lazy_reconnect(
  mocker,
) -> None:
  first = _client(mocker, "first")
  second = _client(mocker, "second")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[first, second]
  )
  backend = RedisBackend(RedisSettings())
  backend.connect()
  backend.disconnect()

  backend.store("key", b"value")

  first.close.assert_called_once()
  second.set.assert_called_once_with("scrapy-extension:storage:key", b"value")
  assert constructor.call_count == 2


def test_disconnect_drains_existing_health_probe_before_close(mocker) -> None:
  ping_started = threading.Event()
  release_ping = threading.Event()
  disconnect_done = threading.Event()
  client = _client(mocker, "leased")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  generation = backend._generation
  assert generation is not None

  def blocked_ping() -> bool:
    ping_started.set()
    assert release_ping.wait(timeout=2)
    return True

  client.ping.side_effect = blocked_ping
  ping_thread, ping_values, ping_errors = _run_in_thread(backend.ping)
  assert ping_started.wait(timeout=2)

  def disconnect() -> None:
    backend.disconnect()
    disconnect_done.set()

  disconnect_thread, _values, disconnect_errors = _run_in_thread(disconnect)
  assert generation.retired.wait(timeout=2)
  assert disconnect_done.is_set() is False
  client.close.assert_not_called()
  release_ping.set()
  _join(ping_thread)
  _join(disconnect_thread)

  assert ping_values == [True]
  assert ping_errors == []
  assert disconnect_errors == []
  client.close.assert_called_once()


def test_namespace_is_frozen_until_reconnect(mocker) -> None:
  first = _client(mocker, "first")
  second = _client(mocker, "second")
  mocker.patch("scrapy_extension.backends.redis.Redis", side_effect=[first, second])
  settings = RedisSettings(namespace="tenant-a")
  backend = RedisBackend(settings)
  backend.connect()

  settings.namespace = "tenant-b"
  backend.store("key", b"first")

  first.set.assert_called_once_with("tenant-a:storage:key", b"first")
  backend.disconnect()
  backend.connect()
  backend.store("key", b"second")
  second.set.assert_called_once_with("tenant-b:storage:key", b"second")


def test_invalid_mutated_namespace_is_rejected_only_at_next_generation(
  mocker,
) -> None:
  first = _client(mocker, "first")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=first
  )
  settings = RedisSettings(namespace="tenant-a")
  backend = RedisBackend(settings)
  backend.connect()

  settings.namespace = "*"
  backend.store("key", b"still-safe")
  first.set.assert_called_once_with("tenant-a:storage:key", b"still-safe")
  backend.disconnect()

  with pytest.raises(ConfigurationError) as exc_info:
    backend.connect()

  assert exc_info.value.setting_name == "namespace"
  constructor.assert_called_once()


@pytest.mark.parametrize(
  "timeout",
  [
    True,
    -1.0,
    float("nan"),
    float("inf"),
    float("-inf"),
    10**400,
    "1",
  ],
)
def test_pop_rejects_invalid_timeout_before_connect(mocker, timeout) -> None:
  constructor = mocker.patch("scrapy_extension.backends.redis.Redis")
  backend = RedisBackend(RedisSettings())

  with pytest.raises(ValueError, match="finite non-negative"):
    backend.pop("jobs", timeout=timeout)

  constructor.assert_not_called()


def test_pop_timeout_budget_starts_before_lazy_connection_and_script_setup(
  mocker,
) -> None:
  from types import SimpleNamespace

  from scrapy_extension.backends import redis as redis_module

  now = [10.0]
  client = _client(mocker, "slow-connect")
  pop_script = mocker.MagicMock(return_value=[0, None])

  def delayed_ping() -> bool:
    now[0] += 0.75
    return True

  def delayed_registration(_source):
    now[0] += 0.5
    return pop_script

  client.ping.side_effect = delayed_ping
  client.register_script.side_effect = delayed_registration
  mocker.patch.object(
    redis_module, "time", SimpleNamespace(monotonic=lambda: now[0])
  )
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())

  assert backend.pop("jobs", timeout=1.0) is None

  pop_script.assert_called_once()
  assert now[0] == pytest.approx(11.25)
  assert backend._generation is not None
  assert backend._generation.retired.is_set() is False


def test_clear_storage_stays_on_issuing_generation(mocker) -> None:
  scan_started = threading.Event()
  release_scan = threading.Event()
  disconnect_done = threading.Event()
  first = _client(mocker, "first")
  second = _client(mocker, "second")

  def blocked_scan(*_args, **_kwargs):
    scan_started.set()
    assert release_scan.wait(timeout=2)
    yield b"tenant:storage:victim"

  first.scan_iter.side_effect = blocked_scan
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[first, second]
  )
  backend = RedisBackend(RedisSettings(namespace="tenant"))
  backend.connect()
  generation = backend._generation
  assert generation is not None

  clear_thread, _clear_values, clear_errors = _run_in_thread(backend.clear_storage)
  assert scan_started.wait(timeout=2)

  def disconnect() -> None:
    backend.disconnect()
    disconnect_done.set()

  disconnect_thread, _disconnect_values, disconnect_errors = _run_in_thread(disconnect)
  assert generation.retired.wait(timeout=2)
  assert disconnect_done.is_set() is False

  original_wait = backend._generation_condition.wait
  reconnect_waiting = threading.Event()

  def track_reconnect_wait(timeout=None):
    if threading.current_thread().name == "redis-reconnect-test":
      reconnect_waiting.set()
    return original_wait(timeout)

  mocker.patch.object(
    backend._generation_condition, "wait", side_effect=track_reconnect_wait
  )

  # A connect intent inside the continuous teardown barrier must not publish B.
  def reconnect() -> None:
    threading.current_thread().name = "redis-reconnect-test"
    backend.connect()

  reconnect_thread, _reconnect_values, reconnect_errors = _run_in_thread(
    reconnect
  )
  assert reconnect_waiting.wait(timeout=2)
  assert reconnect_thread.is_alive()
  release_scan.set()
  _join(clear_thread)
  _join(disconnect_thread)
  _join(reconnect_thread)

  assert clear_errors == []
  assert disconnect_errors == []
  assert reconnect_errors == []
  first.delete.assert_called_once_with(b"tenant:storage:victim")
  second.delete.assert_not_called()
  assert constructor.call_count == 2


def test_clear_storage_failure_reports_possible_partial_completion(
  mocker,
) -> None:
  from redis.exceptions import RedisError

  from scrapy_extension.exceptions import StorageError

  client = _client(mocker, "partial-clear")
  client.scan_iter.return_value = iter(
    [b"tenant:storage:first", b"tenant:storage:second"]
  )
  failure = RedisError("private driver detail")
  client.delete.side_effect = [1, failure]
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings(namespace="tenant"))

  with pytest.raises(StorageError, match="partially complete") as exc_info:
    backend.clear_storage()

  assert exc_info.value.operation == "clear_storage"
  assert exc_info.value.key is None
  assert exc_info.value.__cause__ is failure
  assert "private driver detail" not in str(exc_info.value)
  assert client.delete.call_args_list == [
    mocker.call(b"tenant:storage:first"),
    mocker.call(b"tenant:storage:second"),
  ]


def test_blocking_pop_never_crosses_or_resurrects_generation(mocker) -> None:
  first_poll_started = threading.Event()
  release_first_poll = threading.Event()
  disconnect_done = threading.Event()
  first = _client(mocker, "first")
  second = _client(mocker, "second")
  first_script = mocker.MagicMock(name="first-script")
  second_script = mocker.MagicMock(name="second-script", return_value=[1, b"from-b"])

  def empty_poll(*_args, **_kwargs):
    if not first_poll_started.is_set():
      first_poll_started.set()
      assert release_first_poll.wait(timeout=2)
    return [0, None]

  first_script.side_effect = empty_poll
  first.register_script.return_value = first_script
  second.register_script.return_value = second_script
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[first, second]
  )
  backend = RedisBackend(RedisSettings())
  backend.connect()
  generation = backend._generation
  assert generation is not None

  pop_thread, pop_values, pop_errors = _run_in_thread(
    lambda: backend.pop("jobs", timeout=0.12)
  )
  assert first_poll_started.wait(timeout=2)

  def disconnect() -> None:
    backend.disconnect()
    disconnect_done.set()

  disconnect_thread, _disconnect_values, disconnect_errors = _run_in_thread(disconnect)
  assert generation.retired.wait(timeout=2)
  assert disconnect_done.is_set() is False
  release_first_poll.set()
  _join(pop_thread)
  _join(disconnect_thread)

  assert pop_values == []
  assert len(pop_errors) == 1
  assert isinstance(pop_errors[0], QueueError)
  assert pop_errors[0].operation == "pop"
  assert pop_errors[0].queue_name == "jobs"
  assert disconnect_errors == []
  constructor.assert_called_once()
  second_script.assert_not_called()


@pytest.mark.parametrize(
  ("skip_full_coverage_check", "require_full_coverage"),
  [(False, True), (True, False)],
)
def test_cluster_coverage_setting_maps_to_supported_sdk_keyword(
  mocker,
  skip_full_coverage_check: bool,
  require_full_coverage: bool,
) -> None:
  client = _client(mocker, "cluster")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster", return_value=client
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["cluster-a:6379"],
      cluster_skip_full_coverage_check=skip_full_coverage_check,
    )
  )

  backend.connect()

  kwargs = constructor.call_args.kwargs
  assert kwargs["require_full_coverage"] is require_full_coverage
  assert "skip_full_coverage_check" not in kwargs
  assert "max_redirects" not in kwargs


def test_sentinel_connection_cap_reaches_control_and_master_pools(
  mocker,
) -> None:
  master = _client(mocker, "master")
  sentinel = mocker.MagicMock(name="sentinel")
  sentinel.master_for.return_value = master
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Sentinel", return_value=sentinel
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel-a:26379"],
      max_connections=17,
    )
  )

  backend.connect()

  constructor_kwargs = constructor.call_args.kwargs
  assert constructor_kwargs["max_connections"] == 17
  assert constructor_kwargs["sentinel_kwargs"]["max_connections"] == 17
  assert sentinel.master_for.call_args.kwargs["max_connections"] == 17


class _SentinelControlClient:
  def __init__(self) -> None:
    self.close_calls = 0

  def close(self) -> None:
    self.close_calls += 1


class _SentinelWithoutClose:
  def __init__(self, master: Any, controls: list[_SentinelControlClient]) -> None:
    self.master = master
    self.sentinels = controls

  def master_for(self, *_args, **_kwargs):
    return self.master


class _SentinelWithClose(_SentinelWithoutClose):
  def __init__(self, master: Any, controls: list[_SentinelControlClient]) -> None:
    super().__init__(master, controls)
    self.close_calls = 0

  def close(self) -> None:
    self.close_calls += 1


def test_sentinel_generation_closes_master_and_control_plane_once(mocker) -> None:
  master = _client(mocker, "master")
  controls = [_SentinelControlClient(), _SentinelControlClient()]
  sentinel = _SentinelWithoutClose(master, controls)
  mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=sentinel)
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel-a:26379", "sentinel-b:26380"],
    )
  )

  backend.connect()
  backend.disconnect()

  master.close.assert_called_once()
  assert [control.close_calls for control in controls] == [1, 1]


def test_sentinel_disconnect_closes_controls_after_master_interrupt(mocker) -> None:
  master = _client(mocker, "interrupting-master")
  master.close.side_effect = KeyboardInterrupt
  controls = [_SentinelControlClient(), _SentinelControlClient()]
  sentinel = _SentinelWithoutClose(master, controls)
  mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=sentinel)
  backend = RedisBackend(
    RedisSettings(mode=RedisMode.SENTINEL, sentinels=["sentinel-a:26379"])
  )
  backend.connect()

  with pytest.raises(KeyboardInterrupt):
    backend.disconnect()

  master.close.assert_called_once()
  assert [control.close_calls for control in controls] == [1, 1]
  assert backend._generation is None
  assert backend._disconnecting is False
  assert backend._disconnect_owner is None


def test_failed_sentinel_health_check_closes_master_and_controls(mocker) -> None:
  master = _client(mocker, "master")
  master.ping.return_value = False
  controls = [_SentinelControlClient(), _SentinelControlClient()]
  sentinel = _SentinelWithoutClose(master, controls)
  mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=sentinel)
  backend = RedisBackend(
    RedisSettings(mode=RedisMode.SENTINEL, sentinels=["sentinel-a:26379"])
  )

  with pytest.raises(BackendConnectionError):
    backend.connect()

  master.close.assert_called_once()
  assert [control.close_calls for control in controls] == [1, 1]
  assert backend._generation is None


def test_future_sentinel_close_is_feature_detected_without_double_close(
  mocker,
) -> None:
  master = _client(mocker, "master")
  controls = [_SentinelControlClient(), _SentinelControlClient()]
  sentinel = _SentinelWithClose(master, controls)
  mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=sentinel)
  backend = RedisBackend(
    RedisSettings(mode=RedisMode.SENTINEL, sentinels=["sentinel-a:26379"])
  )

  backend.connect()
  backend.disconnect()

  master.close.assert_called_once()
  assert sentinel.close_calls == 1
  assert [control.close_calls for control in controls] == [0, 0]
