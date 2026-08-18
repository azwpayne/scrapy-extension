"""Snapshot namespace, backend-limit, and exception-isolation regressions."""

from __future__ import annotations

import logging
import sys
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest

import scrapy_extension.backends.memcached as memcached_mod
from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.backends.memcached import MemcachedBackend
from scrapy_extension.exceptions import QueueError
from scrapy_extension.monitor import NullMonitor
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.snapshot import (
    MAX_SNAPSHOT_CHUNK_BYTES,
    SnapshotRepository,
    SnapshotRepositoryError,
)
from scrapy_extension.settings import (
    DynamoDBSettings,
    ElasticSearchSettings,
    MemcachedSettings,
)

_KEY = "queue:snapshot:v3:0::1:q"
_MARKER = "snapshot_private_backend_marker"


class _ExceptionContextProbe(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.contexts: list[tuple[object | None, object | None, object | None]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.contexts.append(sys.exc_info())


@contextmanager
def _queue_log_probe() -> Iterator[_ExceptionContextProbe]:
    target = logging.getLogger("scrapy_extension.queue.queue")
    old_level = target.level
    probe = _ExceptionContextProbe()
    target.setLevel(logging.DEBUG)
    target.addHandler(probe)
    try:
        yield probe
    finally:
        target.removeHandler(probe)
        target.setLevel(old_level)


def _assert_callback_isolated(probe: _ExceptionContextProbe) -> None:
    assert probe.records
    assert probe.contexts == [(None, None, None)] * len(probe.records)
    for record in probe.records:
        assert _MARKER not in record.getMessage()
        assert _MARKER not in repr(record.args)
        assert record.exc_info is None
        assert record.exc_text is None


def _assert_public_error_graph_isolated(error: BaseException) -> None:
    assert error.__cause__ is None
    assert error.__context__ is None
    assert _MARKER not in "".join(traceback.format_exception(error))
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        assert _MARKER not in str(current)
        assert _MARKER not in repr(getattr(current, "__dict__", {}))
        for linked in (current.__cause__, current.__context__):
            if linked is not None:
                pending.append(linked)
        trace = current.__traceback__
        while trace is not None:
            frame = trace.tb_frame
            if "/src/scrapy_extension/" in frame.f_code.co_filename:
                assert _MARKER not in repr(frame.f_locals)
            trace = trace.tb_next


class _Storage:
    def __init__(
        self,
        values: dict[str, bytes] | None = None,
        *,
        fail_retrieve: str | None = None,
        fail_store: bool = False,
        fail_delete: str | None = None,
    ) -> None:
        self.values = dict(values or {})
        self.fail_retrieve = fail_retrieve
        self.fail_store = fail_store
        self.fail_delete = fail_delete

    def retrieve(self, key: str) -> bytes | None:
        if key == self.fail_retrieve:
            raise RuntimeError(_MARKER)
        return self.values.get(key)

    def store(self, key: str, value: bytes) -> None:
        if self.fail_store:
            raise RuntimeError(_MARKER)
        self.values[key] = value

    def delete(self, key: str) -> bool:
        if key == self.fail_delete:
            raise RuntimeError(_MARKER)
        return self.values.pop(key, None) is not None


class _Manager:
    def __init__(self, storage: _Storage) -> None:
        self.storage = storage

    def get_storage_backend(self) -> _Storage:
        return self.storage

    def get_queue_backend(self) -> MagicMock:
        return MagicMock(name="QueueBackend")


def _queue(storage: _Storage, strategy: MagicMock | None = None) -> BackendQueue:
    selected = strategy or MagicMock(name="QueueStrategy")
    selected.snapshot.return_value = b"state"
    return BackendQueue(
        _Manager(storage),  # type: ignore[arg-type]
        "q",
        queue_strategy=selected,
        monitor=NullMonitor(),
        snapshot_max_bytes=64,
        snapshot_chunk_bytes=4,
    )


def test_commit_queue_error_and_log_have_no_backend_exception_graph() -> None:
    queue = _queue(_Storage(fail_store=True))

    with _queue_log_probe() as probe:
        with pytest.raises(QueueError, match="snapshot commit") as exc_info:
            queue.close()

    _assert_callback_isolated(probe)
    _assert_public_error_graph_isolated(exc_info.value)


def test_commit_queue_error_drops_the_complete_snapshot_payload() -> None:
    strategy = MagicMock(name="QueueStrategy")
    strategy.snapshot.return_value = _MARKER.encode()
    queue = _queue(_Storage(fail_store=True), strategy)

    with pytest.raises(QueueError, match="snapshot commit") as exc_info:
        queue.close()

    _assert_public_error_graph_isolated(exc_info.value)


def test_snapshot_creation_queue_error_has_no_strategy_exception_graph() -> None:
    strategy = MagicMock(name="QueueStrategy")
    queue = _queue(_Storage(), strategy)
    strategy.snapshot.side_effect = RuntimeError(_MARKER)

    with _queue_log_probe() as probe:
        with pytest.raises(QueueError, match="snapshot creation") as exc_info:
            queue.close()

    _assert_callback_isolated(probe)
    _assert_public_error_graph_isolated(exc_info.value)


def test_read_log_has_no_backend_exception_context() -> None:
    with _queue_log_probe() as probe:
        queue = _queue(_Storage(fail_retrieve=_KEY))

    _assert_callback_isolated(probe)
    queue._strategy.restore.assert_not_called()  # type: ignore[attr-defined]


def test_legacy_cleanup_log_has_no_backend_exception_context() -> None:
    legacy_key = "queue:snapshot:q"
    storage = _Storage({legacy_key: b"old-state"}, fail_delete=legacy_key)
    queue = _queue(storage)

    with _queue_log_probe() as probe:
        queue.close()

    _assert_callback_isolated(probe)
    assert _KEY in storage.values


def test_restore_log_has_no_strategy_exception_context() -> None:
    strategy = MagicMock(name="QueueStrategy")
    strategy.restore.side_effect = RuntimeError(_MARKER)

    with _queue_log_probe() as probe:
        _queue(_Storage({_KEY: b"state"}), strategy)

    _assert_callback_isolated(probe)


@pytest.mark.parametrize(
    "failing_key, expected_message",
    [
        (
            "queue:snapshot-tombstone:v3:0::1:q",
            "Failed to retrieve empty strategy snapshot tombstone; starting clean",
        ),
        ("queue:snapshot:q", "Failed to read legacy strategy snapshot; starting clean"),
    ],
)
def test_legacy_read_logs_have_no_backend_exception_context(
    failing_key: str, expected_message: str
) -> None:
    with _queue_log_probe() as probe:
        _queue(_Storage(fail_retrieve=failing_key))

    _assert_callback_isolated(probe)
    assert [record.getMessage() for record in probe.records] == [expected_message]


@pytest.mark.parametrize(
    ("backend_type", "maximum_key_bytes"),
    [
        (BackendType.MEMCACHED, 250),
        (BackendType.ELASTICSEARCH, 512),
        (BackendType.DYNAMODB, 2_048),
    ],
)
def test_manifest_key_backend_limit_is_rejected_before_any_chunk_write(
    backend_type: BackendType, maximum_key_bytes: int
) -> None:
    storage = MagicMock()
    storage.backend_type = backend_type
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=4)

    with pytest.raises(SnapshotRepositoryError, match="storage backend limit"):
        repository.commit("k" * (maximum_key_bytes + 1), b"state")

    storage.store.assert_not_called()


def test_unknown_custom_backend_does_not_inherit_a_bundled_key_limit() -> None:
    storage = MagicMock()
    storage.backend_type = "third-party-storage"
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=4)

    repository.commit("k" * 3_000, b"state")

    assert storage.store.call_count == 3


def test_memcached_contract_accepts_maximum_snapshot_chunk(mocker: Any) -> None:
    backend = MemcachedBackend(MemcachedSettings())
    client = MagicMock()
    client.stats.return_value = {}

    def checked_set(key: str, value: bytes, *, expire: int) -> bool:
        assert len(key.encode("ascii")) <= 250
        assert len(value) <= 1024 * 1024
        assert expire == 0
        return True

    client.set.side_effect = checked_set
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    backend.connect()

    SnapshotRepository(backend).commit(_KEY, b"x" * MAX_SNAPSHOT_CHUNK_BYTES)

    assert client.set.call_count == 2


def test_dynamodb_contract_accepts_maximum_snapshot_chunk(mocker: Any) -> None:
    backend = DynamoDBBackend(DynamoDBSettings())
    resource = MagicMock()
    table = MagicMock()
    table.load.return_value = None
    table.table_status = "ACTIVE"
    resource.Table.return_value = table
    table.meta.client = resource.meta.client
    session = MagicMock()
    session.resource.return_value = resource
    mocker.patch.object(boto3.session, "Session", return_value=session)
    backend.connect()

    SnapshotRepository(backend).commit(_KEY, b"x" * MAX_SNAPSHOT_CHUNK_BYTES)

    items = [call.kwargs["Item"] for call in table.put_item.call_args_list]
    assert len(items) == 2
    assert max(len(item["value"]) for item in items) == MAX_SNAPSHOT_CHUNK_BYTES
    assert all(len(item["pk"].encode("utf-8")) <= 2_048 for item in items)


def test_elasticsearch_contract_accepts_maximum_snapshot_chunk() -> None:
    backend = ElasticSearchBackend(ElasticSearchSettings())
    client = MagicMock()
    backend._client = client
    backend._connection_snapshot = backend._capture_connection_snapshot()

    SnapshotRepository(backend).commit(_KEY, b"x" * MAX_SNAPSHOT_CHUNK_BYTES)

    assert client.index.call_count == 2
    ids = [call.kwargs["id"] for call in client.index.call_args_list]
    assert all(len(document_id.encode("utf-8")) <= 512 for document_id in ids)
