"""Concurrency regressions for pooled ConnectionManager construction."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any
from unittest.mock import Mock

import pytest

import scrapy_extension.backends.connectors as connectors
from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import ConfigurationError

_RECURSION_MESSAGE = (
    "Recursive pooled connection manager construction is not supported."
)


def _join_all(threads: list[threading.Thread]) -> None:
    for thread in threads:
        thread.join(timeout=3)
    assert not [thread.name for thread in threads if thread.is_alive()]


def _wait_for_attempt(
    predicate: Callable[[connectors._ManagerConstructionAttempt], bool],
) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        with ConnectionManager._registry_lock:
            attempts = list(ConnectionManager._manager_constructions.values())
            if len(attempts) == 1 and predicate(attempts[0]):
                return
        time.sleep(0.001)
    pytest.fail("manager construction attempt did not reach expected state")


@pytest.mark.parametrize("recursive_host", ["outer", "different"])
def test_discovery_callback_recursion_fails_without_registry_lock(
    monkeypatch: pytest.MonkeyPatch,
    recursive_host: str,
) -> None:
    original_get_descriptor = connectors.get_descriptor
    callback_errors: list[ConfigurationError] = []
    lock_was_free: list[bool] = []
    preexisting = None
    if recursive_host == "different":
        # Recursion is forbidden even when the different key is already pooled.
        preexisting = ConnectionManager.get_manager(
            BackendType.REDIS, {"host": recursive_host}
        )

    def callback(backend_type: str):
        acquired = ConnectionManager._registry_lock.acquire(blocking=False)
        lock_was_free.append(acquired)
        if acquired:
            ConnectionManager._registry_lock.release()
        try:
            ConnectionManager.get_manager(BackendType.REDIS, {"host": recursive_host})
        except ConfigurationError as error:
            callback_errors.append(error)
        return original_get_descriptor(backend_type)

    monkeypatch.setattr(connectors._manager, "get_descriptor", callback)

    manager = ConnectionManager.get_manager(BackendType.REDIS, {"host": "outer"})

    assert lock_was_free == [True]
    assert len(callback_errors) == 1
    assert str(callback_errors[0]) == _RECURSION_MESSAGE
    assert callback_errors[0].setting_name == "backend_settings"
    assert manager._users == 1
    if preexisting is not None:
        assert preexisting._users == 1


def test_same_key_32_way_single_flight_constructs_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_init = ConnectionManager.__init__
    construction_entered = threading.Event()
    release_construction = threading.Event()
    count_lock = threading.Lock()
    construction_count = 0

    def blocking_init(self: ConnectionManager, *args: Any, **kwargs: Any) -> None:
        nonlocal construction_count
        with count_lock:
            construction_count += 1
        construction_entered.set()
        assert release_construction.wait(timeout=3)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(ConnectionManager, "__init__", blocking_init)
    barrier = threading.Barrier(32)
    results: list[ConnectionManager] = []
    errors: list[BaseException] = []

    def acquire() -> None:
        try:
            barrier.wait(timeout=3)
            results.append(
                ConnectionManager.get_manager(
                    BackendType.REDIS, {"host": "single-flight"}
                )
            )
        except BaseException as error:  # noqa: BLE001 - thread errors are asserted
            errors.append(error)

    threads = [threading.Thread(target=acquire) for _ in range(32)]
    for thread in threads:
        thread.start()
    assert construction_entered.wait(timeout=3)
    _wait_for_attempt(lambda attempt: attempt.waiters == 31)
    release_construction.set()
    _join_all(threads)

    assert errors == []
    assert construction_count == 1
    assert len(results) == 32
    assert len({id(manager) for manager in results}) == 1
    assert results[0]._users == 32


@pytest.mark.parametrize("failure", [RuntimeError("failed"), KeyboardInterrupt()])
def test_constructor_base_exception_clears_gate_and_wakes_waiter(
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    original_init = ConnectionManager.__init__
    first_entered = threading.Event()
    release_first = threading.Event()
    count_lock = threading.Lock()
    construction_count = 0

    def fail_once(self: ConnectionManager, *args: Any, **kwargs: Any) -> None:
        nonlocal construction_count
        with count_lock:
            construction_count += 1
            call_number = construction_count
        if call_number == 1:
            first_entered.set()
            assert release_first.wait(timeout=3)
            raise failure
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(ConnectionManager, "__init__", fail_once)
    results: list[ConnectionManager] = []
    errors: list[BaseException] = []

    def acquire() -> None:
        try:
            results.append(
                ConnectionManager.get_manager(
                    BackendType.REDIS, {"host": "failure-wakeup"}
                )
            )
        except BaseException as error:  # noqa: BLE001 - includes KeyboardInterrupt
            errors.append(error)

    owner = threading.Thread(target=acquire)
    owner.start()
    assert first_entered.wait(timeout=3)
    waiter = threading.Thread(target=acquire)
    waiter.start()
    _wait_for_attempt(lambda attempt: attempt.waiters == 1)
    release_first.set()
    _join_all([owner, waiter])

    assert len(errors) == 1
    assert errors[0] is failure
    assert construction_count == 2
    assert len(results) == 1
    assert results[0]._users == 1
    assert ConnectionManager._manager_constructions == {}
    assert ConnectionManager._manager_construction_owners == set()


def test_clear_epoch_discards_candidate_and_reuses_post_clear_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_init = ConnectionManager.__init__
    first_entered = threading.Event()
    release_first = threading.Event()
    count_lock = threading.Lock()
    construction_count = 0
    candidate_backends = [Mock(), Mock()]

    def controlled_init(self: ConnectionManager, *args: Any, **kwargs: Any) -> None:
        nonlocal construction_count
        with count_lock:
            call_number = construction_count
            construction_count += 1
        original_init(self, *args, **kwargs)
        self._backend = candidate_backends[call_number]
        if call_number == 0:
            first_entered.set()
            assert release_first.wait(timeout=3)

    monkeypatch.setattr(ConnectionManager, "__init__", controlled_init)
    results: list[ConnectionManager] = []
    errors: list[BaseException] = []

    def acquire() -> None:
        try:
            results.append(
                ConnectionManager.get_manager(
                    BackendType.REDIS, {"host": "clear-fence"}
                )
            )
        except BaseException as error:  # noqa: BLE001 - thread errors are asserted
            errors.append(error)

    owner = threading.Thread(target=acquire)
    owner.start()
    assert first_entered.wait(timeout=3)
    waiter = threading.Thread(target=acquire)
    waiter.start()
    _wait_for_attempt(lambda attempt: attempt.waiters == 1)

    ConnectionManager.clear_registry()
    waiter.join(timeout=3)
    assert not waiter.is_alive()
    release_first.set()
    _join_all([owner])

    assert errors == []
    assert construction_count == 2
    assert len(results) == 2
    assert results[0] is results[1]
    assert results[0]._users == 2
    candidate_backends[0].disconnect.assert_called_once_with()
    candidate_backends[1].disconnect.assert_not_called()
    assert ConnectionManager._manager_constructions == {}
    assert ConnectionManager._manager_construction_owners == set()

    results[0].close()
    results[1].close()
    candidate_backends[1].disconnect.assert_called_once_with()


def test_concurrent_clear_and_close_disconnect_once_without_thread_leak() -> None:
    manager = ConnectionManager.get_manager(BackendType.REDIS, {"host": "clear-close"})
    backend = Mock()
    manager._backend = backend
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def run(action: Callable[[], None]) -> None:
        try:
            barrier.wait(timeout=3)
            action()
        except BaseException as error:  # noqa: BLE001 - thread errors are asserted
            errors.append(error)

    threads = [
        threading.Thread(target=run, args=(ConnectionManager.clear_registry,)),
        threading.Thread(target=run, args=(manager.close,)),
    ]
    for thread in threads:
        thread.start()
    _join_all(threads)

    assert errors == []
    backend.disconnect.assert_called_once_with()
    assert manager._backend is None
    assert manager._retired is True
    assert ConnectionManager._managers == {}
