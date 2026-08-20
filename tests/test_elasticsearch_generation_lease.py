"""Deterministic barriers for Elasticsearch immutable generation leases."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

_MUTATION_SHARDS = {"total": 2, "successful": 1, "failed": 0}
_READ_SHARDS = {"total": 1, "successful": 1, "failed": 0}
_INDEX_RESPONSE = {"result": "created", "_shards": _MUTATION_SHARDS}
_DELETE_RESPONSE = {"result": "deleted", "_shards": _MUTATION_SHARDS}


def _index_response(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **_INDEX_RESPONSE,
        "_index": kwargs["index"],
        "_id": kwargs["id"],
    }


def _delete_response(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        **_DELETE_RESPONSE,
        "_index": kwargs["index"],
        "_id": kwargs["id"],
    }


def _adapt_elasticsearch_client_mock(client: Any) -> Any:
    client.options.return_value = client
    if client.indices.create.side_effect is None:
        client.indices.create.side_effect = lambda **kwargs: {
            "acknowledged": True,
            "shards_acknowledged": True,
            "index": kwargs["index"],
        }
    return client


def _successful_client(mocker: Any) -> Any:
    client = _adapt_elasticsearch_client_mock(
        mocker.MagicMock(ping=mocker.MagicMock(return_value=True))
    )
    client.index.side_effect = lambda **kwargs: _index_response(kwargs)
    client.delete.side_effect = lambda **kwargs: _delete_response(kwargs)
    return client


def _injected_backend(mocker: Any, **settings: Any) -> tuple[ElasticSearchBackend, Any]:
    backend = ElasticSearchBackend(ElasticSearchSettings(**settings))
    client = _adapt_elasticsearch_client_mock(mocker.MagicMock())
    client.index.side_effect = lambda **kwargs: _index_response(kwargs)
    client.delete.side_effect = lambda **kwargs: _delete_response(kwargs)
    client.indices.refresh.return_value = {"_shards": _MUTATION_SHARDS}
    backend._client = client
    backend._connection_snapshot = backend._capture_connection_snapshot()
    return backend, client


def _thread(target: Any) -> threading.Thread:
    thread = threading.Thread(target=target)
    thread.start()
    return thread


def _wait_for_disconnect_entry(backend: ElasticSearchBackend) -> None:
    with backend._generation_condition:
        assert backend._generation_condition.wait_for(
            lambda: backend._disconnecting, timeout=2
        )


def test_push_lease_keeps_client_open_until_sdk_call_finishes(mocker: Any) -> None:
    backend, client = _injected_backend(mocker, queue_index="queue-a")
    entered = threading.Event()
    release = threading.Event()
    disconnected = threading.Event()

    def index(**_kwargs: Any) -> dict[str, Any]:
        entered.set()
        assert release.wait(timeout=2)
        return _index_response(_kwargs)

    client.index.side_effect = index
    pushing = _thread(lambda: backend.push("jobs", b"payload"))
    assert entered.wait(timeout=2)
    disconnecting = _thread(lambda: (backend.disconnect(), disconnected.set()))

    assert not disconnected.wait(timeout=0.1)
    client.close.assert_not_called()
    release.set()
    pushing.join(timeout=2)
    disconnecting.join(timeout=2)

    assert not pushing.is_alive()
    assert not disconnecting.is_alive()
    client.close.assert_called_once_with()


def test_pop_search_delete_uses_one_generation_lease(mocker: Any) -> None:
    backend, client = _injected_backend(mocker, queue_index="queue-a")
    search_entered = threading.Event()
    release_search = threading.Event()
    disconnected = threading.Event()
    calls: list[str] = []

    def search(**_kwargs: Any) -> dict[str, Any]:
        calls.append("search")
        search_entered.set()
        assert release_search.wait(timeout=2)
        return {
            "timed_out": False,
            "_shards": _READ_SHARDS,
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [
                    {
                        "_id": "doc-1",
                        "_seq_no": 3,
                        "_primary_term": 2,
                        "_source": {"item": "cGF5bG9hZA=="},
                    }
                ],
            },
        }

    client.search.side_effect = search

    def delete(**_kwargs: Any) -> dict[str, Any]:
        calls.append("delete")
        return _delete_response(_kwargs)

    client.delete.side_effect = delete
    client.close.side_effect = lambda: calls.append("close")

    result: list[bytes | None] = []
    popping = _thread(lambda: result.append(backend.pop("jobs")))
    assert search_entered.wait(timeout=2)
    disconnecting = _thread(lambda: (backend.disconnect(), disconnected.set()))
    assert not disconnected.wait(timeout=0.1)

    release_search.set()
    popping.join(timeout=2)
    disconnecting.join(timeout=2)

    assert result == [b"payload"]
    assert calls == ["search", "delete", "close"]
    assert client.delete.call_args.kwargs["index"] == "queue-a"


def test_expired_storage_reap_uses_get_generation_for_delete(mocker: Any) -> None:
    backend, client = _injected_backend(mocker, storage_index="storage-a")
    reap_entered = threading.Event()
    release_reap = threading.Event()
    disconnected = threading.Event()
    expired = (datetime.now(tz=timezone.utc) - timedelta(seconds=10)).isoformat()
    client.get.return_value = {
        "_source": {"data": "cGF5bG9hZA==", "expireAt": expired},
        "_seq_no": 8,
        "_primary_term": 4,
    }

    def delete(**_kwargs: Any) -> dict[str, Any]:
        reap_entered.set()
        assert release_reap.wait(timeout=2)
        return _delete_response(_kwargs)

    client.delete.side_effect = delete
    result: list[bytes | None] = []
    retrieving = _thread(lambda: result.append(backend.retrieve("key")))
    assert reap_entered.wait(timeout=2)
    disconnecting = _thread(lambda: (backend.disconnect(), disconnected.set()))

    assert not disconnected.wait(timeout=0.1)
    client.close.assert_not_called()
    release_reap.set()
    retrieving.join(timeout=2)
    disconnecting.join(timeout=2)

    assert result == [None]
    assert client.get.call_args.kwargs["index"] == "storage-a"
    assert client.delete.call_args.kwargs["index"] == "storage-a"
    client.close.assert_called_once_with()


def test_disconnect_drains_all_peer_operation_leases(mocker: Any) -> None:
    backend, client = _injected_backend(mocker)
    both_entered = threading.Barrier(3)
    release = threading.Event()
    disconnected = threading.Event()

    def index(**_kwargs: Any) -> dict[str, Any]:
        both_entered.wait(timeout=2)
        assert release.wait(timeout=2)
        return _index_response(_kwargs)

    client.index.side_effect = index
    first = _thread(lambda: backend.push("jobs", b"one"))
    second = _thread(lambda: backend.store("key", b"two"))
    both_entered.wait(timeout=2)
    disconnecting = _thread(lambda: (backend.disconnect(), disconnected.set()))

    assert not disconnected.wait(timeout=0.1)
    client.close.assert_not_called()
    release.set()
    first.join(timeout=2)
    second.join(timeout=2)
    disconnecting.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    client.close.assert_called_once_with()


def test_reconnect_fences_retired_client_and_snapshot(mocker: Any) -> None:
    first = _successful_client(mocker)
    second = _successful_client(mocker)
    factory = mocker.patch(
        "scrapy_extension.backends.elasticsearch.Elasticsearch",
        side_effect=[first, second],
    )
    config = ElasticSearchSettings(queue_index="queue-a")
    backend = ElasticSearchBackend(config)
    backend.connect()
    entered = threading.Event()
    release = threading.Event()

    def old_index(**_kwargs: Any) -> dict[str, Any]:
        entered.set()
        assert release.wait(timeout=2)
        return _index_response(_kwargs)

    first.index.side_effect = old_index
    pushing = _thread(lambda: backend.push("jobs", b"old"))
    assert entered.wait(timeout=2)
    disconnecting = _thread(backend.disconnect)
    _wait_for_disconnect_entry(backend)
    first.close.assert_not_called()
    release.set()
    pushing.join(timeout=2)
    disconnecting.join(timeout=2)

    config.queue_index = "queue-b"
    backend.connect()
    backend.push("jobs", b"new")

    assert factory.call_count == 2
    assert first.index.call_args.kwargs["index"] == "queue-a"
    assert second.index.call_args.kwargs["index"] == "queue-b"
    first.close.assert_called_once_with()
    second.close.assert_not_called()


def test_connect_during_drain_rejects_lease_owner_and_replaces_for_peer(
    mocker: Any,
) -> None:
    first = _successful_client(mocker)
    second = _successful_client(mocker)
    factory = mocker.patch(
        "scrapy_extension.backends.elasticsearch.Elasticsearch",
        side_effect=[first, second],
    )
    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    operation_entered = threading.Barrier(2)
    try_nested_connect = threading.Event()
    nested_connect_done = threading.Event()
    release_operation = threading.Event()
    nested_errors: list[BaseException] = []
    push_errors: list[BaseException] = []
    disconnect_errors: list[BaseException] = []
    peer_errors: list[BaseException] = []
    peer_connected = threading.Event()

    def hold_lease(**_kwargs: Any) -> dict[str, Any]:
        operation_entered.wait(timeout=2)
        assert try_nested_connect.wait(timeout=2)
        try:
            backend.connect()
        except BaseException as error:
            nested_errors.append(error)
        finally:
            nested_connect_done.set()
        assert release_operation.wait(timeout=2)
        return _index_response(_kwargs)

    def push() -> None:
        try:
            backend.push("jobs", b"payload")
        except BaseException as error:
            push_errors.append(error)

    def disconnect() -> None:
        try:
            backend.disconnect()
        except BaseException as error:
            disconnect_errors.append(error)

    def peer_connect() -> None:
        try:
            backend.connect()
        except BaseException as error:
            peer_errors.append(error)
        finally:
            peer_connected.set()

    first.index.side_effect = hold_lease
    pushing = _thread(push)
    operation_entered.wait(timeout=2)
    disconnecting = _thread(disconnect)
    _wait_for_disconnect_entry(backend)

    try_nested_connect.set()
    assert nested_connect_done.wait(timeout=2)
    assert len(nested_errors) == 1
    assert isinstance(nested_errors[0], BackendConnectionError)
    assert nested_errors[0].backend_type == "elasticsearch"

    connecting = _thread(peer_connect)
    assert not peer_connected.wait(timeout=0.1)
    assert factory.call_count == 1
    first.close.assert_not_called()

    release_operation.set()
    pushing.join(timeout=2)
    disconnecting.join(timeout=2)
    connecting.join(timeout=2)

    assert not pushing.is_alive()
    assert not disconnecting.is_alive()
    assert not connecting.is_alive()
    assert push_errors == []
    assert disconnect_errors == []
    assert peer_errors == []
    assert factory.call_count == 2
    first.close.assert_called_once_with()
    second.close.assert_not_called()
    with backend._generation_condition:
        assert backend._active_leases == 0
        assert backend._generation is not None
        assert backend._generation.client is second


def test_post_publication_interrupt_preserves_leasable_generation(
    mocker: Any,
) -> None:
    candidate = _successful_client(mocker)
    factory = mocker.patch(
        "scrapy_extension.backends.elasticsearch.Elasticsearch",
        return_value=_adapt_elasticsearch_client_mock(candidate),
    )
    backend = ElasticSearchBackend(ElasticSearchSettings())
    original_notify_all = backend._generation_condition.notify_all
    publication_entered = threading.Event()
    allow_interrupt = threading.Event()
    operation_entered = threading.Barrier(2)
    release_operation = threading.Event()
    interrupt = KeyboardInterrupt()
    notify_calls = 0
    connect_errors: list[BaseException] = []
    push_errors: list[BaseException] = []

    def interrupt_first_publication_notification() -> None:
        nonlocal notify_calls
        notify_calls += 1
        if notify_calls == 1:
            publication_entered.set()
            assert allow_interrupt.wait(timeout=2)
            raise interrupt
        original_notify_all()

    mocker.patch.object(
        backend._generation_condition,
        "notify_all",
        side_effect=interrupt_first_publication_notification,
    )

    def connect() -> None:
        try:
            backend.connect()
        except BaseException as error:
            connect_errors.append(error)

    def hold_lease(**_kwargs: Any) -> dict[str, Any]:
        operation_entered.wait(timeout=2)
        assert release_operation.wait(timeout=2)
        return _index_response(_kwargs)

    def push() -> None:
        try:
            backend.push("jobs", b"payload")
        except BaseException as error:
            push_errors.append(error)

    candidate.index.side_effect = hold_lease
    connecting = _thread(connect)
    assert publication_entered.wait(timeout=2)
    pushing = _thread(push)
    assert pushing.is_alive()
    allow_interrupt.set()
    operation_entered.wait(timeout=2)
    connecting.join(timeout=2)

    assert not connecting.is_alive()
    assert connect_errors == [interrupt]
    factory.assert_called_once()
    candidate.close.assert_not_called()
    with backend._generation_condition:
        generation = backend._generation
        assert generation is not None
        assert generation.client is candidate
        assert backend._client is candidate
        assert backend._connection_snapshot is generation.snapshot
        assert backend._active_leases == 1

    release_operation.set()
    pushing.join(timeout=2)
    assert not pushing.is_alive()
    assert push_errors == []
    with backend._generation_condition:
        assert backend._active_leases == 0


def test_health_probe_lease_delays_disconnect(mocker: Any) -> None:
    backend, client = _injected_backend(mocker)
    ping_entered = threading.Event()
    release_ping = threading.Event()
    disconnected = threading.Event()

    def ping() -> bool:
        ping_entered.set()
        assert release_ping.wait(timeout=2)
        return True

    client.ping.side_effect = ping
    result: list[bool] = []
    probing = _thread(lambda: result.append(backend.is_connected()))
    assert ping_entered.wait(timeout=2)
    disconnecting = _thread(lambda: (backend.disconnect(), disconnected.set()))

    assert not disconnected.wait(timeout=0.1)
    release_ping.set()
    probing.join(timeout=2)
    disconnecting.join(timeout=2)

    assert result == [True]
    client.close.assert_called_once_with()


def test_reentrant_disconnect_is_rejected_without_deadlock(mocker: Any) -> None:
    backend, client = _injected_backend(mocker)
    rejection: list[BackendConnectionError] = []

    def index(**_kwargs: Any) -> dict[str, Any]:
        with pytest.raises(BackendConnectionError) as exc_info:
            backend.disconnect()
        rejection.append(exc_info.value)
        return _index_response(_kwargs)

    client.index.side_effect = index
    backend.push("jobs", b"payload")

    assert len(rejection) == 1
    assert "re-entrantly" in str(rejection[0])
    client.close.assert_not_called()


@pytest.mark.parametrize("startup_callback", ["ping", "ensure_indices", "close"])
@pytest.mark.parametrize("nested_action", ["connect", "disconnect", "lazy_push"])
def test_private_candidate_rejects_same_thread_reentrant_lifecycle(
    mocker: Any, startup_callback: str, nested_action: str
) -> None:
    backend = ElasticSearchBackend(ElasticSearchSettings())
    candidate = mocker.MagicMock()
    rejections: list[Exception] = []
    callback_count = 0

    def invoke_nested_action() -> None:
        nonlocal callback_count
        callback_count += 1
        expected_error = (
            QueueError if nested_action == "lazy_push" else BackendConnectionError
        )
        with pytest.raises(expected_error) as exc_info:
            if nested_action == "connect":
                backend.connect()
            elif nested_action == "disconnect":
                backend.disconnect()
            else:
                backend.push("jobs", b"nested")
        rejections.append(exc_info.value)

    if startup_callback == "ping":

        def ping() -> bool:
            invoke_nested_action()
            return False

        candidate.ping.side_effect = ping
    elif startup_callback == "ensure_indices":
        candidate.ping.return_value = True

        def create_index(**_kwargs: Any) -> None:
            invoke_nested_action()
            raise RuntimeError("index setup failed")

        candidate.indices.create.side_effect = create_index
    else:
        candidate.ping.return_value = False
        candidate.close.side_effect = invoke_nested_action

    factory = mocker.patch(
        "scrapy_extension.backends.elasticsearch.Elasticsearch",
        return_value=_adapt_elasticsearch_client_mock(candidate),
    )

    with pytest.raises(BackendConnectionError):
        backend.connect()

    assert callback_count == 1
    assert len(rejections) == 1
    factory.assert_called_once()
    candidate.close.assert_called_once_with()
    assert backend._generation is None
    assert backend._client is None
    assert backend._connection_snapshot is None
    assert backend._connecting is False
    assert backend._connect_owner is None
