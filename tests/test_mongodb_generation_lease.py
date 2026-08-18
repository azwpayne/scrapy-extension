"""Deterministic barriers for MongoDB operation-generation leases."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

import pytest

from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.exceptions import BackendConnectionError, StorageError
from scrapy_extension.settings import MongoDBSettings


def _thread(target: Any) -> threading.Thread:
    thread = threading.Thread(target=target)
    thread.start()
    return thread


def _injected_backend(mocker: Any) -> tuple[MongoDBBackend, Any, Any, Any]:
    backend = MongoDBBackend(MongoDBSettings())
    client = mocker.MagicMock()
    backend._client = client
    backend._db = mocker.MagicMock()
    queue_collection = mocker.MagicMock()
    set_collection = mocker.MagicMock()
    storage_collection = mocker.MagicMock()
    backend._queue_collection = queue_collection
    backend._set_collection = set_collection
    backend._storage_collection = storage_collection
    return backend, client, queue_collection, storage_collection


def _wait_for_disconnect_entry(backend: MongoDBBackend) -> None:
    with backend._generation_condition:
        assert backend._generation_condition.wait_for(
            lambda: backend._disconnecting, timeout=2
        )


def _assert_lifecycle_lock_free(backend: MongoDBBackend) -> None:
    """Probe ownership locally and acquisition from a competing thread."""
    assert not backend._connection_lock._is_owned()
    acquired = threading.Event()

    def acquire_lock() -> None:
        with backend._connection_lock:
            acquired.set()

    contender = _thread(acquire_lock)
    assert acquired.wait(timeout=2)
    contender.join(timeout=2)
    assert not contender.is_alive()


def test_pop_delete_lease_keeps_pool_open_until_delete_finishes(mocker: Any) -> None:
    backend, client, queue_collection, _storage_collection = _injected_backend(mocker)
    delete_entered = threading.Event()
    release_delete = threading.Event()
    disconnected = threading.Event()
    result: list[bytes | None] = []

    def find_one_and_delete(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        delete_entered.set()
        assert release_delete.wait(timeout=2)
        return {
            "queue_name": "jobs",
            "item": b"payload",
            "priority": 0.0,
            "created_at": datetime.now(tz=timezone.utc),
        }

    queue_collection.find_one_and_delete.side_effect = find_one_and_delete
    popping = _thread(lambda: result.append(backend.pop("jobs")))
    assert delete_entered.wait(timeout=2)
    disconnecting = _thread(lambda: (backend.disconnect(), disconnected.set()))
    _wait_for_disconnect_entry(backend)

    assert not disconnected.wait(timeout=0.1)
    client.close.assert_not_called()
    release_delete.set()
    popping.join(timeout=2)
    disconnecting.join(timeout=2)

    assert not popping.is_alive()
    assert not disconnecting.is_alive()
    assert result == [b"payload"]
    client.close.assert_called_once_with()


def test_disconnect_rejects_new_sibling_operation_while_pop_drains(mocker: Any) -> None:
    backend, client, queue_collection, storage_collection = _injected_backend(mocker)
    delete_entered = threading.Event()
    release_delete = threading.Event()

    def find_one_and_delete(*_args: Any, **_kwargs: Any) -> None:
        delete_entered.set()
        assert release_delete.wait(timeout=2)
        return None

    queue_collection.find_one_and_delete.side_effect = find_one_and_delete
    popping = _thread(lambda: backend.pop("jobs"))
    assert delete_entered.wait(timeout=2)
    disconnecting = _thread(backend.disconnect)
    _wait_for_disconnect_entry(backend)

    with pytest.raises((BackendConnectionError, StorageError)):
        backend.store("key", b"new")
    storage_collection.replace_one.assert_not_called()
    client.close.assert_not_called()

    release_delete.set()
    popping.join(timeout=2)
    disconnecting.join(timeout=2)
    assert not popping.is_alive()
    assert not disconnecting.is_alive()
    client.close.assert_called_once_with()


def test_normal_operations_remain_concurrent(mocker: Any) -> None:
    backend, _client, queue_collection, storage_collection = _injected_backend(mocker)
    both_entered = threading.Barrier(3)

    def insert_one(*_args: Any, **_kwargs: Any) -> None:
        both_entered.wait(timeout=2)

    def replace_one(*_args: Any, **_kwargs: Any) -> None:
        both_entered.wait(timeout=2)

    queue_collection.insert_one.side_effect = insert_one
    storage_collection.replace_one.side_effect = replace_one
    pushing = _thread(lambda: backend.push("jobs", b"payload"))
    storing = _thread(lambda: backend.store("key", b"value"))
    both_entered.wait(timeout=2)
    pushing.join(timeout=2)
    storing.join(timeout=2)

    assert not pushing.is_alive()
    assert not storing.is_alive()
    with backend._generation_condition:
        assert backend._active_leases == 0


def test_reconnect_waits_for_retired_generation_and_uses_new_collections(
    mocker: Any,
) -> None:
    first_client = mocker.MagicMock()
    second_client = mocker.MagicMock()
    first_database = mocker.MagicMock()
    second_database = mocker.MagicMock()
    first_client.__getitem__.return_value = first_database
    second_client.__getitem__.return_value = second_database
    first_collections = {
        "queues": mocker.MagicMock(),
        "sets": mocker.MagicMock(),
        "storage": mocker.MagicMock(),
    }
    second_collections = {
        "queues_next": mocker.MagicMock(),
        "sets_next": mocker.MagicMock(),
        "storage_next": mocker.MagicMock(),
    }
    first_database.__getitem__.side_effect = first_collections.__getitem__
    second_database.__getitem__.side_effect = second_collections.__getitem__
    factory = mocker.patch(
        "scrapy_extension.backends.mongodb.MongoClient",
        side_effect=[first_client, second_client],
    )
    config = MongoDBSettings()
    backend = MongoDBBackend(config)
    backend.connect()
    pop_entered = threading.Event()
    release_pop = threading.Event()
    connected = threading.Event()

    def find_one_and_delete(*_args: Any, **_kwargs: Any) -> None:
        pop_entered.set()
        assert release_pop.wait(timeout=2)
        return None

    first_collections["queues"].find_one_and_delete.side_effect = find_one_and_delete
    popping = _thread(lambda: backend.pop("jobs"))
    assert pop_entered.wait(timeout=2)
    disconnecting = _thread(backend.disconnect)
    _wait_for_disconnect_entry(backend)

    config.queue_collection = "queues_next"
    config.set_collection = "sets_next"
    config.storage_collection = "storage_next"
    connecting = _thread(lambda: (backend.connect(), connected.set()))
    assert not connected.wait(timeout=0.1)
    first_client.close.assert_not_called()
    assert factory.call_count == 1

    release_pop.set()
    popping.join(timeout=2)
    disconnecting.join(timeout=2)
    connecting.join(timeout=2)
    backend.push("jobs", b"next")

    assert not popping.is_alive()
    assert not disconnecting.is_alive()
    assert not connecting.is_alive()
    assert factory.call_count == 2
    first_client.close.assert_called_once_with()
    second_collections["queues_next"].insert_one.assert_called_once()
    first_collections["queues"].insert_one.assert_not_called()


def test_reentrant_disconnect_from_operation_is_rejected_without_deadlock(
    mocker: Any,
) -> None:
    backend, client, queue_collection, _storage_collection = _injected_backend(mocker)
    rejection: list[BackendConnectionError] = []

    def find_one_and_delete(*_args: Any, **_kwargs: Any) -> None:
        with pytest.raises(BackendConnectionError) as exc_info:
            backend.disconnect()
        rejection.append(exc_info.value)
        return None

    queue_collection.find_one_and_delete.side_effect = find_one_and_delete
    assert backend.pop("jobs") is None

    assert len(rejection) == 1
    assert "re-entrantly" in str(rejection[0])
    client.close.assert_not_called()


def test_disconnect_close_callback_connect_fails_immediately_without_deadlock(
    mocker: Any,
) -> None:
    backend, client, _queue_collection, _storage_collection = _injected_backend(mocker)
    rejection: list[BackendConnectionError] = []

    def close() -> None:
        with pytest.raises(BackendConnectionError) as exc_info:
            backend.connect()
        rejection.append(exc_info.value)

    client.close.side_effect = close
    disconnected = _thread(backend.disconnect)
    disconnected.join(timeout=2)

    assert not disconnected.is_alive()
    assert len(rejection) == 1
    assert rejection[0].backend_type == "mongodb"
    client.close.assert_called_once_with()


@pytest.mark.parametrize(
    "phase",
    ["construction", "ping", "domain", "index", "success-log", "failed-close"],
)
def test_connect_sdk_and_callbacks_run_with_lifecycle_lock_free(
    mocker: Any,
    phase: str,
) -> None:
    from pymongo.errors import ConnectionFailure

    backend = MongoDBBackend(MongoDBSettings())
    client = mocker.MagicMock()
    database = mocker.MagicMock()
    queue_collection = mocker.MagicMock()
    set_collection = mocker.MagicMock()
    storage_collection = mocker.MagicMock()
    collections = {
        "queues": queue_collection,
        "sets": set_collection,
        "storage": storage_collection,
    }
    client.__getitem__.return_value = database
    database.__getitem__.side_effect = collections.__getitem__
    for collection in collections.values():
        collection.with_options.return_value = collection

    probes: list[str] = []

    def probe() -> None:
        _assert_lifecycle_lock_free(backend)
        probes.append(phase)

    def construct(*_args: Any, **_kwargs: Any) -> Any:
        probe()
        return client

    factory = mocker.patch(
        "scrapy_extension.backends.mongodb.MongoClient",
        side_effect=construct if phase == "construction" else None,
        return_value=None if phase == "construction" else client,
    )
    if phase == "ping":
        client.admin.command.side_effect = lambda _command: probe()
    elif phase == "domain":
        queue_collection.with_options.side_effect = lambda **_kwargs: (
            probe(),
            queue_collection,
        )[1]
    elif phase == "index":
        queue_collection.create_index.side_effect = lambda *_args, **_kwargs: probe()
    elif phase == "success-log":
        mocker.patch(
            "scrapy_extension.backends.mongodb.logger.debug",
            side_effect=lambda *_args: probe(),
        )
    elif phase == "failed-close":
        client.admin.command.side_effect = ConnectionFailure("startup failed")
        client.close.side_effect = probe

    if phase == "failed-close":
        with pytest.raises(BackendConnectionError):
            backend.connect()
    else:
        backend.connect()

    assert probes == [phase]
    factory.assert_called_once()
