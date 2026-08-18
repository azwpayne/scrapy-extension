"""Deterministic barriers for Elasticsearch immutable generation leases."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings


def _injected_backend(mocker: Any, **settings: Any) -> tuple[ElasticSearchBackend, Any]:
    backend = ElasticSearchBackend(ElasticSearchSettings(**settings))
    client = mocker.MagicMock()
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

    def index(**_kwargs: Any) -> None:
        entered.set()
        assert release.wait(timeout=2)

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
            "hits": {
                "hits": [
                    {
                        "_id": "doc-1",
                        "_seq_no": 3,
                        "_primary_term": 2,
                        "_source": {"item": "cGF5bG9hZA=="},
                    }
                ]
            }
        }

    client.search.side_effect = search
    client.delete.side_effect = lambda **_kwargs: calls.append("delete")
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

    def delete(**_kwargs: Any) -> None:
        reap_entered.set()
        assert release_reap.wait(timeout=2)

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

    def index(**_kwargs: Any) -> None:
        both_entered.wait(timeout=2)
        assert release.wait(timeout=2)

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
    first = mocker.MagicMock(ping=mocker.MagicMock(return_value=True))
    second = mocker.MagicMock(ping=mocker.MagicMock(return_value=True))
    factory = mocker.patch(
        "scrapy_extension.backends.elasticsearch.Elasticsearch",
        side_effect=[first, second],
    )
    config = ElasticSearchSettings(queue_index="queue-a")
    backend = ElasticSearchBackend(config)
    backend.connect()
    entered = threading.Event()
    release = threading.Event()

    def old_index(**_kwargs: Any) -> None:
        entered.set()
        assert release.wait(timeout=2)

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

    def index(**_kwargs: Any) -> None:
        with pytest.raises(BackendConnectionError) as exc_info:
            backend.disconnect()
        rejection.append(exc_info.value)

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
        return_value=candidate,
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
