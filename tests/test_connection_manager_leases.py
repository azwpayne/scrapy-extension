"""Acquire-specific ConnectionManager ownership regressions."""

from __future__ import annotations

import threading
from unittest.mock import Mock

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager


def test_leases_release_only_their_exact_acquire() -> None:
    first = ConnectionManager.acquire_lease(BackendType.REDIS, {"host": "leases"})
    second = ConnectionManager.acquire_lease(BackendType.REDIS, {"host": "leases"})
    unrelated = ConnectionManager.acquire_lease(BackendType.REDIS, {"host": "leases"})
    manager = first.manager
    backend = Mock()
    manager._backend = backend

    assert second.manager is manager
    assert unrelated.manager is manager
    assert manager._users == 3

    first.release()
    first.release()
    assert first.released is True
    assert second.released is False
    assert unrelated.released is False
    assert manager._users == 2
    backend.disconnect.assert_not_called()

    second.release()
    assert manager._users == 1
    assert unrelated.released is False
    backend.disconnect.assert_not_called()

    unrelated.release()
    unrelated.release()
    assert manager._users == 0
    assert manager._retirement_complete is True
    backend.disconnect.assert_called_once_with()


def test_concurrent_legacy_close_claims_distinct_tokens_atomically() -> None:
    manager = ConnectionManager.get_manager(BackendType.REDIS, {"host": "legacy-race"})
    for _ in range(15):
        assert (
            ConnectionManager.get_manager(BackendType.REDIS, {"host": "legacy-race"})
            is manager
        )
    backend = Mock()
    manager._backend = backend
    barrier = threading.Barrier(16)
    errors: list[BaseException] = []

    def release() -> None:
        try:
            barrier.wait(timeout=3)
            manager.close()
        except BaseException as error:  # noqa: BLE001 - asserted below
            errors.append(error)

    threads = [threading.Thread(target=release) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert errors == []
    assert manager._users == 0
    assert manager._legacy_acquires == []
    assert manager._retirement_complete is True
    backend.disconnect.assert_called_once_with()


def test_legacy_close_never_consumes_an_acquire_specific_lease() -> None:
    manager = ConnectionManager.get_manager(BackendType.REDIS, {"host": "mixed"})
    lease = ConnectionManager.acquire_lease(BackendType.REDIS, {"host": "mixed"})
    backend = Mock()
    manager._backend = backend

    manager.close()
    manager.close()

    assert manager._users == 1
    assert lease.released is False
    backend.disconnect.assert_not_called()

    lease.release()
    backend.disconnect.assert_called_once_with()


@pytest.mark.parametrize("raise_after_effect", [False, True])
def test_release_retry_repairs_interruption_around_retirement(
    monkeypatch: pytest.MonkeyPatch,
    raise_after_effect: bool,
) -> None:
    lease = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": f"interrupt-{raise_after_effect}"}
    )
    manager = lease.manager
    backend = Mock()
    manager._backend = backend
    original = manager._finalize_retirement
    attempts = 0

    def interrupted() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            if raise_after_effect:
                original()
            raise KeyboardInterrupt
        original()

    monkeypatch.setattr(manager, "_finalize_retirement", interrupted)

    with pytest.raises(KeyboardInterrupt):
        lease.release()

    assert lease.released is True
    lease.release()
    assert manager._retirement_complete is True
    assert manager._users == 0
    backend.disconnect.assert_called_once_with()


def test_one_interrupted_retirement_publication_repairs_event_and_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": "publication-interruption"}
    )
    manager = lease.manager
    backend = Mock()
    manager._backend = backend
    original = manager._publish_retirement_complete
    interruption = KeyboardInterrupt("publish")
    calls = 0

    def interrupted_publication() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            # Model interruption after one package-state assignment. The bounded
            # repair pass must finish the event and finalizer ownership fields.
            with manager._lock:
                manager._retirement_complete = True
            raise interruption
        original()

    monkeypatch.setattr(
        manager, "_publish_retirement_complete", interrupted_publication
    )

    with pytest.raises(KeyboardInterrupt) as exc_info:
        lease.release()

    assert exc_info.value is interruption
    assert manager._retirement_complete is True
    assert manager._retirement_finalizing is False
    assert manager._retirement_finalizer_token is None
    assert manager._retirement_finalization_event.is_set()
    lease.release()
    backend.disconnect.assert_called_once_with()


def test_duplicate_concurrent_release_waits_for_retirement_completion() -> None:
    lease = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": "concurrent-release"}
    )
    manager = lease.manager
    disconnect_entered = threading.Event()
    allow_disconnect = threading.Event()
    backend = Mock()

    def disconnect() -> None:
        disconnect_entered.set()
        assert allow_disconnect.wait(timeout=3)

    backend.disconnect.side_effect = disconnect
    manager._backend = backend
    completed: list[str] = []

    first = threading.Thread(
        target=lambda: (lease.release(), completed.append("first"))
    )
    second = threading.Thread(
        target=lambda: (lease.release(), completed.append("second"))
    )
    first.start()
    assert disconnect_entered.wait(timeout=3)
    second.start()
    second.join(timeout=0.05)
    assert second.is_alive()

    allow_disconnect.set()
    first.join(timeout=3)
    second.join(timeout=3)

    assert sorted(completed) == ["first", "second"]
    assert manager._retirement_complete is True
    backend.disconnect.assert_called_once_with()


def test_single_flight_lease_callers_receive_unique_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_init = ConnectionManager.__init__
    entered = threading.Event()
    proceed = threading.Event()
    construction_count = 0
    count_lock = threading.Lock()

    def blocking_init(self: ConnectionManager, *args: object, **kwargs: object) -> None:
        nonlocal construction_count
        with count_lock:
            construction_count += 1
        entered.set()
        assert proceed.wait(timeout=3)
        original_init(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(ConnectionManager, "__init__", blocking_init)
    leases = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(8)

    def acquire() -> None:
        try:
            barrier.wait(timeout=3)
            leases.append(
                ConnectionManager.acquire_lease(
                    BackendType.REDIS, {"host": "lease-single-flight"}
                )
            )
        except BaseException as error:  # noqa: BLE001 - asserted below
            errors.append(error)

    threads = [threading.Thread(target=acquire) for _ in range(8)]
    for thread in threads:
        thread.start()
    assert entered.wait(timeout=3)
    proceed.set()
    for thread in threads:
        thread.join(timeout=3)

    assert errors == []
    assert construction_count == 1
    assert len(leases) == 8
    assert len({id(lease.manager) for lease in leases}) == 1
    assert len({id(lease._token) for lease in leases}) == 8
    assert leases[0].manager._users == 8

    for lease in leases:
        lease.release()
