"""Regression contracts for MongoDB's durable in-place poison quarantine."""

from __future__ import annotations

import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier, Lock
from typing import Any

import pytest
from bson.binary import Binary

from scrapy_extension.backends.mongodb import (
    MongoDBBackend,
    _active_queue_filter,
    _is_valid_queue_result,
)
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import MongoDBSettings

_QUEUE = "jobs"
_MARKER = "mongodb-private-poison-marker"
_BASE_TIME = datetime(2026, 8, 18, tzinfo=timezone.utc)


def _valid_document(
    identity: str, payload: bytes, *, priority: float = 0.0, age: int = 0
) -> dict[str, Any]:
    return {
        "_id": identity,
        "queue_name": _QUEUE,
        "item": payload,
        "priority": priority,
        "created_at": _BASE_TIME + timedelta(microseconds=age),
    }


class _AtomicQueueCollection:
    """Small deterministic model of the one-command MongoDB pop contract."""

    def __init__(self, documents: list[dict[str, Any]]) -> None:
        self.documents = list(documents)
        self.pop_calls = 0
        self.count_calls = 0
        self.clear_calls = 0
        self._lock = Lock()

    def find_one_and_delete(
        self, query: dict[str, Any], *, sort: list[tuple[str, int]]
    ) -> dict[str, Any] | None:
        assert query == _active_queue_filter(_QUEUE)
        assert sort == [("priority", 1), ("created_at", 1)]
        with self._lock:
            self.pop_calls += 1
            candidates = [
                document
                for document in self.documents
                if _is_valid_queue_result(document, _QUEUE)
            ]
            if not candidates:
                return None
            winner = min(
                candidates,
                key=lambda document: (document["priority"], document["created_at"]),
            )
            self.documents.remove(winner)
            return winner

    def count_documents(self, query: dict[str, Any], *, limit: int) -> int:
        assert query == _active_queue_filter(_QUEUE)
        with self._lock:
            self.count_calls += 1
            return min(
                limit,
                sum(
                    _is_valid_queue_result(document, _QUEUE)
                    for document in self.documents
                ),
            )

    def delete_many(self, query: dict[str, Any]) -> None:
        assert query == {"queue_name": _QUEUE}
        with self._lock:
            self.clear_calls += 1
            self.documents = [
                document
                for document in self.documents
                if document.get("queue_name") != _QUEUE
            ]


def _backend(mocker: Any, collection: object) -> MongoDBBackend:
    backend = MongoDBBackend(MongoDBSettings())
    backend._queue_collection = collection  # type: ignore[assignment]
    backend._set_collection = mocker.MagicMock()
    backend._storage_collection = mocker.MagicMock()
    return backend


@pytest.mark.parametrize(
    "poison",
    [
        {
            "_id": "missing-item",
            "queue_name": _QUEUE,
            "priority": -100.0,
            "created_at": _BASE_TIME,
        },
        {
            "_id": "text-item",
            "queue_name": _QUEUE,
            "item": _MARKER,
            "priority": -100.0,
            "created_at": _BASE_TIME,
        },
        {
            "_id": "missing-priority",
            "queue_name": _QUEUE,
            "item": b"poison",
            "created_at": _BASE_TIME,
        },
        {
            "_id": "bool-priority",
            "queue_name": _QUEUE,
            "item": b"poison",
            "priority": False,
            "created_at": _BASE_TIME,
        },
        {
            "_id": "nan-priority",
            "queue_name": _QUEUE,
            "item": b"poison",
            "priority": float("nan"),
            "created_at": _BASE_TIME,
        },
        {
            "_id": "infinite-priority",
            "queue_name": _QUEUE,
            "item": b"poison",
            "priority": float("-inf"),
            "created_at": _BASE_TIME,
        },
        {
            "_id": "missing-created-at",
            "queue_name": _QUEUE,
            "item": b"poison",
            "priority": -100.0,
        },
        {
            "_id": "text-created-at",
            "queue_name": _QUEUE,
            "item": b"poison",
            "priority": -100.0,
            "created_at": _MARKER,
        },
    ],
    ids=lambda poison: str(poison["_id"]),
)
def test_malformed_head_is_durable_but_does_not_starve_valid_record(
    mocker: Any, poison: dict[str, Any]
) -> None:
    valid = _valid_document("valid", b"deliverable", priority=0.0, age=1)
    collection = _AtomicQueueCollection([poison, valid])
    backend = _backend(mocker, collection)

    assert backend.queue_len(_QUEUE) == 1
    assert backend.pop(_QUEUE) == b"deliverable"
    assert backend.queue_len(_QUEUE) == 0
    assert collection.documents == [poison]

    backend.clear_queue(_QUEUE)
    assert collection.documents == []


def test_atomic_valid_selection_preserves_priority_and_fifo_behind_poison(
    mocker: Any,
) -> None:
    poison = {
        "_id": "poison-head",
        "queue_name": _QUEUE,
        "item": _MARKER,
        "priority": -1000.0,
        "created_at": _BASE_TIME,
    }
    documents = [
        poison,
        _valid_document("low", b"low", priority=0.0, age=0),
        _valid_document("high-second", b"high-second", priority=-10.0, age=2),
        _valid_document("high-first", b"high-first", priority=-10.0, age=1),
    ]
    collection = _AtomicQueueCollection(documents)
    backend = _backend(mocker, collection)

    assert [backend.pop(_QUEUE) for _ in range(3)] == [
        b"high-first",
        b"high-second",
        b"low",
    ]
    assert collection.documents == [poison]


def test_concurrent_contention_uses_one_atomic_command_per_pop(
    mocker: Any,
) -> None:
    valid_count = 24
    caller_count = 32
    poison = {
        "_id": "poison-head",
        "queue_name": _QUEUE,
        "item": b"poison",
        "priority": float("-inf"),
        "created_at": _BASE_TIME,
    }
    documents = [poison] + [
        _valid_document(f"valid-{index}", f"item-{index}".encode(), age=index)
        for index in range(valid_count)
    ]
    collection = _AtomicQueueCollection(documents)
    backend = _backend(mocker, collection)
    start = Barrier(caller_count)

    def pop_once() -> bytes | None:
        start.wait(timeout=5)
        return backend.pop(_QUEUE)

    with ThreadPoolExecutor(max_workers=caller_count) as executor:
        results = list(executor.map(lambda _index: pop_once(), range(caller_count)))

    delivered = [result for result in results if result is not None]
    assert len(delivered) == valid_count
    assert len(set(delivered)) == valid_count
    assert results.count(None) == caller_count - valid_count
    assert collection.pop_calls == caller_count
    assert collection.documents == [poison]


def test_pop_query_is_strict_and_accepts_bson_binary_result(mocker: Any) -> None:
    collection = mocker.MagicMock()
    document = _valid_document("binary", Binary(b"payload", subtype=128))
    collection.find_one_and_delete.return_value = document
    backend = _backend(mocker, collection)

    assert backend.pop(_QUEUE) == b"payload"

    query = collection.find_one_and_delete.call_args.args[0]
    assert query["queue_name"] == {"$eq": _QUEUE}
    assert query["item"] == {"$type": "binData"}
    assert query["priority"]["$type"] == "number"
    assert set(query["priority"]) == {"$type", "$gte", "$lte"}
    assert query["created_at"] == {"$type": "date"}
    collection.find_one_and_delete.assert_called_once()


def test_nonconforming_driver_result_raises_static_redacted_error(mocker: Any) -> None:
    collection = mocker.MagicMock()
    collection.find_one_and_delete.return_value = {
        "queue_name": _QUEUE,
        "item": _MARKER,
        "priority": -1.0,
        "created_at": _BASE_TIME,
    }
    backend = _backend(mocker, collection)

    with pytest.raises(QueueError) as exc_info:
        backend.pop(_QUEUE)

    error = exc_info.value
    assert str(error) == "MongoDB queue pop failed."
    assert error.operation == "pop"
    assert error.queue_name is None
    assert error.__cause__ is None
    assert error.__context__ is None
    assert _MARKER not in "".join(traceback.format_exception(error))
