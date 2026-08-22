"""Focused failure-injection coverage for durable state and lifecycle seams."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import threading
from typing import Any
from unittest.mock import MagicMock

import pytest
from scrapy import Request, Spider

import scrapy_extension.queue.snapshot as snapshot_module
from scrapy_extension.backends.elasticsearch import (
    ElasticSearchBackend,
    _api_error_has_type,
    _ElasticSearchResponseError,
    _validate_delete_response,
    _validate_index_response,
    _validate_search_response,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    QueueError,
    SerializationError,
    StorageError,
)
from scrapy_extension.queue.snapshot import SnapshotRepository, SnapshotRepositoryError
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.time_wheel import (
    TimeWheelQueueStrategy,
    _finite_number,
)
from scrapy_extension.schedule import scheduler as scheduler_module
from scrapy_extension.schedule.scheduler import (
    _LIFECYCLE_CLOSED,
    _LIFECYCLE_OPENING,
    BackendScheduler,
)
from scrapy_extension.settings._transport_security import (
    is_loopback_host,
    normalize_allow_remote_plaintext,
)
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings
from scrapy_extension.spider import spider_mixin as spider_mixin_module
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin

_SNAPSHOT_KEY = "queue:snapshot:v3:0::1:coverage"
_GENERATION = "a" * 32


class _AtomicNoVolatile:
    """Atomic filter double that deliberately has no volatile commit hook."""

    def __init__(
        self,
        *,
        decision_error: BaseException | None = None,
        process_error: BaseException | None = None,
        reservation_none: bool = False,
    ) -> None:
        self.decision_error = decision_error
        self.process_error = process_error
        self.reservation_none = reservation_none
        self.rollbacks: list[object] = []
        self.intent_rollbacks: list[object] = []

    def request_seen_with_reservation(self, _request: Request, _owner: object) -> Any:
        if self.decision_error is not None:
            raise self.decision_error
        reservation = None if self.reservation_none else object()
        return type("Decision", (), {"seen": False, "reservation": reservation})()

    def commit_reservation(self, _reservation: object) -> None:
        return None

    def rollback_reservation(self, reservation: object) -> None:
        self.rollbacks.append(reservation)
        if self.process_error is not None:
            raise self.process_error

    def rollback_reservation_intent(self, owner: object) -> None:
        self.intent_rollbacks.append(owner)
        if self.process_error is not None:
            raise self.process_error


class _CoverageSpider(BackendSpiderMixin):
    name = "coverage-spider"


class _MemoryStorage:
    """Small storage double whose values can be corrupted per test."""

    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.fail_keys: set[str] = set()

    def retrieve(self, key: str) -> object:
        if key in self.fail_keys:
            raise RuntimeError("injected retrieve failure")
        return self.values.get(key)

    def store(self, key: str, value: bytes) -> None:
        self.values[key] = value


def _repository_with_manifest(
    state: bytes = b"abcd", *, length: int | None = None
) -> tuple[SnapshotRepository, _MemoryStorage, str]:
    storage = _MemoryStorage()
    repository = SnapshotRepository(storage, max_bytes=16, chunk_bytes=4)
    actual_length = len(state) if length is None else length
    chunk_key = repository._chunk_key(_SNAPSHOT_KEY, _GENERATION, 0)
    storage.values[chunk_key] = state
    manifest = {
        "schema": "scrapy-extension.queue-strategy-snapshot",
        "version": 7,
        "generation": _GENERATION,
        "length": actual_length,
        "chunk_bytes": 4,
        "chunks": 1,
        "sha256": hashlib.sha256(state).hexdigest(),
        "state": "bytes",
    }
    storage.values[_SNAPSHOT_KEY] = json.dumps(manifest).encode()
    return repository, storage, chunk_key


@pytest.mark.parametrize(
    ("chunk", "message"),
    [
        (None, "invalid"),
        (memoryview(b"abcdefgh")[::2], "not contiguous"),
        (bytearray(b"abcd"), "mutable"),
        (b"abc", "length validation"),
    ],
)
def test_snapshot_read_rejects_corrupt_chunk_without_clean_start(
    chunk: object, message: str
) -> None:
    """Each corrupt chunk shape remains an explicit read failure."""
    repository, storage, chunk_key = _repository_with_manifest()
    storage.values[chunk_key] = chunk

    with pytest.raises(SnapshotRepositoryError, match=message):
        repository.read(_SNAPSHOT_KEY)


def test_snapshot_read_rejects_chunk_retrieval_failure() -> None:
    repository, storage, chunk_key = _repository_with_manifest()
    storage.fail_keys.add(chunk_key)

    with pytest.raises(SnapshotRepositoryError, match="chunk retrieval"):
        repository.read(_SNAPSHOT_KEY)


def test_snapshot_read_rejects_chunk_conversion_failure(mocker: Any) -> None:
    repository, storage, _chunk_key = _repository_with_manifest()
    manifest = storage.values[_SNAPSHOT_KEY]
    repository._copy_buffer = mocker.Mock(  # type: ignore[method-assign]
        side_effect=[
            (manifest, None),
            (None, snapshot_module._BUFFER_CONVERSION_FAILED),
        ]
    )

    with pytest.raises(SnapshotRepositoryError, match="conversion"):
        repository.read(_SNAPSHOT_KEY)


def test_snapshot_read_rejects_assembled_length_mismatch(mocker: Any) -> None:
    repository, _storage, _chunk_key = _repository_with_manifest()
    manifest = snapshot_module._Manifest(
        version=7,
        generation=_GENERATION,
        length=5,
        chunk_bytes=4,
        chunks=1,
        checksum=hashlib.sha256(b"abcd").hexdigest(),
        state_present=True,
    )
    mocker.patch.object(repository, "_decode_manifest", return_value=(manifest, None))

    with pytest.raises(SnapshotRepositoryError, match="length validation"):
        repository.read(_SNAPSHOT_KEY)


@pytest.mark.parametrize(
    ("state", "copy_error", "message"),
    [
        (b"abcd", snapshot_module._BUFFER_NONCONTIGUOUS, "not contiguous"),
        (b"abcd", snapshot_module._BUFFER_MUTABLE, "mutable"),
        (b"abcd", snapshot_module._BUFFER_CONVERSION_FAILED, "conversion"),
    ],
)
def test_snapshot_commit_preserves_buffer_failure_contract(
    mocker: Any, state: bytes, copy_error: str, message: str
) -> None:
    storage = _MemoryStorage()
    repository = SnapshotRepository(storage, max_bytes=4, chunk_bytes=1)
    mocker.patch.object(repository, "_copy_buffer", return_value=(None, copy_error))

    with pytest.raises(SnapshotRepositoryError, match=message):
        repository.commit(_SNAPSHOT_KEY, state)

    assert storage.values == {}


def test_snapshot_commit_rejects_oversized_and_invalid_state() -> None:
    repository = SnapshotRepository(_MemoryStorage(), max_bytes=4, chunk_bytes=1)

    with pytest.raises(SnapshotRepositoryError, match="invalid type"):
        repository.commit(_SNAPSHOT_KEY, bytearray(b"abcd"))
    with pytest.raises(SnapshotRepositoryError, match="size limit"):
        repository.commit(_SNAPSHOT_KEY, b"12345")


def test_snapshot_commit_rejects_post_copy_size_increase(mocker: Any) -> None:
    repository = SnapshotRepository(_MemoryStorage(), max_bytes=4, chunk_bytes=1)
    mocker.patch.object(repository, "_copy_buffer", return_value=(b"12345", None))

    with pytest.raises(SnapshotRepositoryError, match="size limit"):
        repository.commit(_SNAPSHOT_KEY, b"1234")


@pytest.mark.parametrize("copy_mode", ["nested", "copy-error", "none", "short", "long"])
def test_snapshot_buffer_copy_revalidates_untrusted_view_results(
    monkeypatch: pytest.MonkeyPatch, copy_mode: str
) -> None:
    class _FakeView:
        def __init__(self, value: object) -> None:
            self.nbytes = 5 if copy_mode == "long" else 4
            self.c_contiguous = True
            self.readonly = True
            self.obj = value

        def tobytes(self) -> bytes | None:
            if copy_mode == "copy-error":
                raise RuntimeError("copy failed")
            if copy_mode == "none":
                return None
            if copy_mode == "short":
                return b"abc"
            if copy_mode == "long":
                return b"abcde"
            return b"abcd"

        def release(self) -> None:
            return None

    class _FakeMemoryView(_FakeView):
        def __init__(self, value: object) -> None:
            super().__init__(value)
            if copy_mode == "nested":
                nested = object.__new__(_FakeMemoryView)
                nested.obj = b"abcd"
                self.obj = nested

    monkeypatch.setattr(snapshot_module, "memoryview", _FakeMemoryView, raising=False)
    copied, error = SnapshotRepository._copy_buffer(b"abcd", 4)

    if copy_mode == "nested":
        assert copied == b"abcd" and error is None
    else:
        if copy_mode == "long":
            assert copied is None and error == snapshot_module._BUFFER_OVERSIZED
        else:
            assert copied is None and error == snapshot_module._BUFFER_CONVERSION_FAILED


def test_snapshot_manifest_oversize_is_treated_as_legacy_data() -> None:
    manifest, error = SnapshotRepository._decode_manifest(
        b"x" * (snapshot_module._MAX_MANIFEST_BYTES + 1)
    )
    assert manifest is None and error is None


@pytest.mark.parametrize(
    "response",
    [
        object(),
        {"timed_out": True},
        {"timed_out": False, "_shards": {"total": 1, "successful": 1, "failed": 0}},
    ],
)
def test_elasticsearch_search_acknowledgement_failures_are_typed(
    response: object,
) -> None:
    with pytest.raises(_ElasticSearchResponseError):
        _validate_search_response(response)


@pytest.mark.parametrize(
    "response",
    [
        {
            "_index": "wrong",
            "_id": "id",
            "result": "created",
            "_shards": {"total": 1, "successful": 1, "failed": 0},
        },
        {
            "_index": "index",
            "_id": "id",
            "result": "noop",
            "_shards": {"total": 1, "successful": 1, "failed": 0},
        },
    ],
)
def test_elasticsearch_mutation_acknowledgement_identity_and_result_are_strict(
    response: object,
) -> None:
    with pytest.raises(_ElasticSearchResponseError):
        _validate_index_response(
            response,
            frozenset({"created"}),
            expected_index="index",
            expected_id="id",
        )


@pytest.mark.parametrize(
    ("result", "expected"),
    [("deleted", True), ("not_found", False)],
)
def test_elasticsearch_delete_acknowledgements_distinguish_absence(
    result: str, expected: bool
) -> None:
    response = {
        "_index": "index",
        "_id": "id",
        "result": result,
        "_shards": {"total": 1, "successful": 1, "failed": 0},
    }
    assert (
        _validate_delete_response(response, expected_index="index", expected_id="id")
        is expected
    )


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (None, "missing object"),
        ([], "missing object"),
    ],
)
def test_elasticsearch_storage_schema_rejects_corrupt_source(
    source: object, message: str
) -> None:
    with pytest.raises(StorageError, match=message):
        ElasticSearchBackend._storage_source({"_source": source}, "key", "retrieve")


@pytest.mark.parametrize("value", [None, 3, "not-base64"])
def test_elasticsearch_storage_data_rejects_corrupt_payload(value: object) -> None:
    with pytest.raises(StorageError):
        ElasticSearchBackend._storage_data({"data": value}, "key")


@pytest.mark.parametrize("value", [3, "not-an-iso-date"])
def test_elasticsearch_storage_expiry_rejects_corrupt_values(value: object) -> None:
    with pytest.raises(StorageError):
        ElasticSearchBackend._storage_expiry({"expireAt": value}, "key", "retrieve")


class _LifecycleSpider(Spider):
    name = "coverage-lifecycle"


def test_elasticsearch_response_helpers_reject_untrusted_shapes() -> None:
    assert _api_error_has_type(MagicMock(body=None), "wanted") is False
    assert _api_error_has_type(MagicMock(body={"error": None}), "wanted") is False
    assert (
        _api_error_has_type(
            MagicMock(body={"error": {"root_cause": [{"type": "wanted"}]}}),
            "wanted",
        )
        is True
    )

    with pytest.raises(_ElasticSearchResponseError):
        _validate_search_response(
            {
                "timed_out": False,
                "_shards": {"total": 1, "successful": 1, "failed": 0},
                "hits": {
                    "total": {"value": 2, "relation": "eq"},
                    "hits": [{}, {}],
                },
            }
        )
    with pytest.raises(_ElasticSearchResponseError):
        _validate_index_response(
            {
                "_index": "index",
                "_id": "id",
                "result": "created",
                "_shards": {"total": 1, "successful": 0, "failed": 0},
            },
            frozenset({"created"}),
            expected_index="index",
            expected_id="id",
        )
    with pytest.raises(_ElasticSearchResponseError):
        _validate_delete_response(
            {
                "_index": "index",
                "_id": "id",
                "result": "unexpected",
                "_shards": {"total": 1, "successful": 1, "failed": 0},
            },
            expected_index="index",
            expected_id="id",
        )


def test_timewheel_rejects_nonfinite_clock_and_ready_times() -> None:
    with pytest.raises(ValueError):
        _finite_number(10**1000, "value")
    strategy = TimeWheelQueueStrategy(
        MagicMock(), clock=lambda: 1.0, wall_clock=lambda: 1.0
    )
    with pytest.raises(ValueError):
        strategy._tick_at(math.inf)
    with pytest.raises(ValueError):
        strategy._slot_at(math.inf)

    overflowing = TimeWheelQueueStrategy(
        MagicMock(), default_delay=1e308, clock=lambda: 1e308
    )
    with pytest.raises(ValueError, match="ready time"):
        overflowing.push("q", b"item")


def test_timewheel_prepared_routes_validate_and_preserve_durability() -> None:
    manager = MagicMock()
    direct = TimeWheelQueueStrategy(manager, default_delay=0.0)
    assert direct.is_push_durable(delay=0.0, source="test") is True
    with pytest.raises(ValueError, match="delay"):
        route = TimeWheelQueueStrategy(manager)._prepare_push("q", delay=-1.0)
        route.commit(b"item")

    delayed = TimeWheelQueueStrategy(manager, default_delay=1.0)
    assert delayed.is_push_durable(delay=1.0, source="test") is False
    with pytest.raises(ValueError, match="delay"):
        route = delayed._prepare_push("q", delay=-1.0)
        route.commit(b"item")
    overflow_route = TimeWheelQueueStrategy(manager, default_delay=100.0)._prepare_push(
        "q", delay=100.0
    )
    assert overflow_route.commit(b"item") is False


def test_timewheel_prepared_publish_rejects_overflowing_ready_time() -> None:
    strategy = TimeWheelQueueStrategy(
        MagicMock(), default_delay=1e308, clock=lambda: 1e308
    )
    route = strategy._prepare_push("q", delay=1e308)
    with pytest.raises(ValueError, match="ready time"):
        route.commit(b"item")


def test_timewheel_restore_rejects_invalid_timing_metadata() -> None:
    manager = MagicMock()
    strategy = TimeWheelQueueStrategy(
        manager, clock=lambda: 1e308, wall_clock=lambda: 1.0
    )
    version_two = {
        "strategy": "time_wheel",
        "version": 2,
        "snapshot_wall_time": 1.0,
        "slots_flat": [
            {
                "remaining": 1e308,
                "item_b64": base64.b64encode(b"x").decode(),
                "priority": 0.0,
            },
            {
                "remaining": 1.0,
                "item_b64": base64.b64encode(b"x").decode(),
                "priority": float("inf"),
            },
        ],
        "overflow": [
            {
                "remaining": 1.0,
                "item_b64": base64.b64encode(b"x").decode(),
                "priority": float("inf"),
            }
        ],
    }
    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(json.dumps(version_two).encode())

    version_one = {
        "strategy": "time_wheel",
        "version": 1,
        "slots_flat": [
            {
                "ready_at": float("inf"),
                "item_b64": base64.b64encode(b"x").decode(),
                "priority": 0.0,
            }
        ],
        "overflow": [],
    }
    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(json.dumps(version_one).encode())
    assert all(not slot for slot in strategy._wheel)


def test_timewheel_restore_rejects_nonfinite_current_wall_clock() -> None:
    strategy = TimeWheelQueueStrategy(
        MagicMock(), clock=lambda: 1.0, wall_clock=lambda: math.inf
    )
    state = {
        "strategy": "time_wheel",
        "version": 2,
        "snapshot_wall_time": 1.0,
        "slots_flat": [],
        "overflow": [],
    }
    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(json.dumps(state).encode())
    assert all(not slot for slot in strategy._wheel)


def test_timewheel_snapshot_and_restore_reject_wall_and_clock_metadata() -> None:
    strategy = TimeWheelQueueStrategy(
        MagicMock(), default_delay=1.0, clock=lambda: 1.0, wall_clock=lambda: math.inf
    )
    strategy.push("q", b"item")
    with pytest.raises(ValueError, match="wall_clock"):
        strategy.snapshot()

    restored_manager = MagicMock()
    restored_manager.get_queue_backend.return_value.queue_len.return_value = 0
    restored = TimeWheelQueueStrategy(
        restored_manager, clock=lambda: 1.0, wall_clock=lambda: 1.0
    )
    state = {
        "strategy": "time_wheel",
        "version": 2,
        "snapshot_wall_time": 1.0,
        "slots_flat": [
            {
                "remaining": float("nan"),
                "item_b64": base64.b64encode(b"x").decode(),
                "priority": 0.0,
            }
        ],
        "overflow": [{"remaining": 1.0, "item_b64": "!", "priority": 0.0}],
    }
    with pytest.raises(QueueError, match="snapshot restore failed"):
        restored.restore(json.dumps(state).encode())
    assert restored.queue_len("q") == 0


def test_delay_restore_rejects_bad_clock_metadata_and_keeps_state_empty() -> None:
    strategy = DelayQueueStrategy(MagicMock(), clock=lambda: math.inf)
    state = json.dumps({"strategy": "delay", "version": 1, "items": []}).encode()
    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(state)
    assert strategy._holding == []


def test_delay_prepared_routes_reject_negative_delay_and_bad_wall_clock() -> None:
    manager = MagicMock()
    direct = DelayQueueStrategy(manager)
    with pytest.raises(ValueError, match="delay"):
        direct._prepare_push("q", delay=-1.0).commit(b"item")

    delayed = DelayQueueStrategy(manager, default_delay=1.0)
    with pytest.raises(ValueError, match="delay"):
        delayed._prepare_push("q", delay=-1.0).commit(b"item")

    state = {
        "strategy": "delay",
        "version": 2,
        "snapshot_wall_time": 1.0,
        "items": [
            {
                "remaining": float("inf"),
                "item_b64": base64.b64encode(b"item").decode(),
                "priority": 0.0,
            }
        ],
    }
    invalid_wall = DelayQueueStrategy(
        manager, clock=lambda: 1.0, wall_clock=lambda: math.inf
    )
    with pytest.raises(QueueError, match="snapshot restore failed"):
        invalid_wall.restore(json.dumps(state).encode())
    assert invalid_wall._holding == []

    valid_wall = DelayQueueStrategy(manager, clock=lambda: 1.0, wall_clock=lambda: 2.0)
    with pytest.raises(QueueError, match="snapshot restore failed"):
        valid_wall.restore(json.dumps(state).encode())
    assert valid_wall._holding == []


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        (object(), False),
        ("localhost", True),
        ("localhost.", True),
        ("fe80::1%lo0", False),
        ("[::1", False),
        ("[127.0.0.1]", False),
        ("::ffff:127.0.0.1", False),
        ("::1", True),
    ],
)
def test_loopback_security_classifier_rejects_ambiguous_hosts(
    host: object, expected: bool
) -> None:
    assert is_loopback_host(host) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [(True, True), (False, False), ("true", True), (" FALSE ", False)],
)
def test_remote_plaintext_boolean_parser_accepts_only_canonical_values(
    value: object, expected: bool
) -> None:
    assert normalize_allow_remote_plaintext(value) is expected


@pytest.mark.parametrize("value", [1, None, "yes", ""])
def test_remote_plaintext_boolean_parser_rejects_truthy_lookalikes(
    value: object,
) -> None:
    with pytest.raises(Exception, match="must be a boolean"):
        normalize_allow_remote_plaintext(value)


def test_elasticsearch_lifecycle_fences_reentrant_connect_and_operations() -> None:
    backend = ElasticSearchBackend(ElasticSearchSettings())
    current = threading.get_ident()
    backend._connect_owner = current
    with pytest.raises(BackendConnectionError):
        backend.connect()

    backend._connect_owner = None
    backend._disconnect_owner = current
    with pytest.raises(BackendConnectionError):
        backend.connect()

    backend._disconnect_owner = None
    backend._disconnecting = True
    backend._lease_local.depth = 1
    with pytest.raises(BackendConnectionError):
        backend.connect()
    with pytest.raises(BackendConnectionError):
        with backend._lease_generation("coverage"):
            pass


def test_elasticsearch_active_snapshot_and_failed_candidate_cleanup() -> None:
    backend = ElasticSearchBackend(ElasticSearchSettings())
    snapshot = backend._active_snapshot()
    assert backend._active_snapshot() is snapshot
    backend._generation = MagicMock(snapshot=snapshot)
    assert backend._active_snapshot() is snapshot

    raced = ElasticSearchBackend(ElasticSearchSettings())
    sentinel = raced._capture_connection_snapshot()
    captured = raced._capture_connection_snapshot()

    def capture_and_publish():
        raced._connection_snapshot = sentinel
        return captured

    raced._capture_connection_snapshot = capture_and_publish  # type: ignore[method-assign]
    assert raced._active_snapshot() is sentinel

    candidate = MagicMock()
    candidate.close.side_effect = RuntimeError("close failed")
    backend._client = candidate
    assert backend._abort_failed_connect(candidate) is True
    assert backend._client is None

    backend._client = object()  # type: ignore[assignment]
    candidate.close.side_effect = None
    assert backend._abort_failed_connect(candidate) is False


def test_spider_connect_signals_recovers_when_previous_dispatcher_is_missing() -> None:
    spider = _CoverageSpider()
    spider.crawler = MagicMock()
    spider._signals_connected = True
    spider._connected_signals = None

    spider._connect_signals()

    assert spider._connected_signals is spider.crawler.signals
    assert spider._signals_connected is True


def test_spider_signal_failure_with_existing_owner_does_not_close_peer_manager(
    mocker,
) -> None:
    spider = _CoverageSpider()
    manager = MagicMock()
    spider._connection_manager = manager
    spider._connected_signals = object()
    mocker.patch.object(spider, "_connect_signals", side_effect=RuntimeError("wire"))

    with pytest.raises(RuntimeError, match="wire"):
        spider.setup_backend()
    manager.close.assert_not_called()


def test_spider_setup_releases_manager_when_signal_wiring_and_cleanup_fail(
    mocker,
) -> None:
    spider = _CoverageSpider()
    spider.backend_type = "redis"
    manager = MagicMock()
    manager.close.side_effect = RuntimeError("cleanup failed")
    from scrapy_extension.backends.connectors import ConnectionManager

    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    mocker.patch.object(spider, "_connect_signals", side_effect=RuntimeError("wire"))

    with pytest.raises(RuntimeError, match="wire"):
        spider.setup_backend()
    manager.close.assert_called_once_with()


def _queue_constructor_failure_spider(mocker) -> tuple[Any, Any, Any]:
    from scrapy.settings import Settings

    from scrapy_extension.backends.base import BackendType
    from scrapy_extension.backends.connectors import ConnectionManager

    spider = _CoverageSpider()
    spider.backend_type = BackendType.KAFKA
    spider._connection_manager = MagicMock()
    crawler = MagicMock()
    crawler.settings = Settings(
        {
            "SCRAPY_QUEUE_STRATEGY": "delay",
            "SCRAPY_STORAGE_BACKEND_TYPE": "redis",
        }
    )
    spider.crawler = crawler
    lease = MagicMock()
    lease.manager = MagicMock()
    mocker.patch.object(ConnectionManager, "acquire_lease", return_value=lease)
    mocker.patch(
        "scrapy_extension.queue.queue.BackendQueue",
        side_effect=RuntimeError("queue construction failed"),
    )
    return spider, lease, spider._connection_manager


def test_spider_queue_construction_retains_failed_snapshot_lease(mocker) -> None:
    spider, lease, manager = _queue_constructor_failure_spider(mocker)
    lease.release.side_effect = [RuntimeError("lease release failed"), None]

    with pytest.raises(RuntimeError, match="queue construction failed"):
        spider.get_queue()
    lease.release.assert_called_once_with()
    assert spider._snapshot_connection_lease is lease
    manager.close.assert_not_called()

    spider.close_backend()
    assert lease.release.call_count == 2
    manager.close.assert_called_once_with()


def test_spider_queue_construction_releases_snapshot_lease_after_constructor_failure(
    mocker,
) -> None:
    spider, lease, manager = _queue_constructor_failure_spider(mocker)

    with pytest.raises(RuntimeError, match="queue construction failed"):
        spider.get_queue()
    lease.release.assert_called_once_with()
    manager.close.assert_called_once_with()


def test_spider_close_logs_ordinary_component_failure(mocker) -> None:
    spider = _CoverageSpider()
    component = MagicMock()
    component.close.side_effect = RuntimeError("component failed")
    spider._queue = component
    logger_error = mocker.patch.object(spider_mixin_module.logger, "error")

    with pytest.raises(RuntimeError, match="component failed"):
        spider.close_backend()

    logger_error.assert_called_once_with("Failed to close backend component")
    assert spider._queue is component


def test_spider_close_component_control_error_does_not_replace_signal_error(
    mocker,
) -> None:
    spider = _CoverageSpider()
    spider._connected_signals = MagicMock()
    spider._disconnect_lifecycle_signals = MagicMock(
        side_effect=KeyboardInterrupt("signal interrupted")
    )
    component = MagicMock()
    component.close.side_effect = KeyboardInterrupt("component interrupted")
    spider._queue = component

    with pytest.raises(KeyboardInterrupt, match="signal interrupted"):
        spider.close_backend()


def test_spider_close_preserves_process_control_from_component() -> None:
    spider = _CoverageSpider()
    component = MagicMock()
    signal = KeyboardInterrupt("component interrupted")
    component.close.side_effect = signal
    spider._queue = component

    with pytest.raises(KeyboardInterrupt) as raised:
        spider.close_backend()
    assert raised.value is signal


def test_spider_close_does_not_replace_an_existing_control_error() -> None:
    spider = _CoverageSpider()
    component = MagicMock()
    component.close.side_effect = KeyboardInterrupt("component interrupted")
    lease = MagicMock()
    lease.release.side_effect = KeyboardInterrupt("lease interrupted")
    spider._queue = component
    spider._snapshot_connection_lease = lease

    with pytest.raises(KeyboardInterrupt, match="component interrupted"):
        spider.close_backend()
    assert component.close.call_count == 1
    # The queue failed before the snapshot durability barrier, so its lease is
    # retained for a later close attempt rather than released against a closed
    # manager.
    assert lease.release.call_count == 0


def test_spider_close_snapshot_release_success_is_nonfatal() -> None:
    spider = _CoverageSpider()
    lease = MagicMock()
    spider._snapshot_connection_lease = lease
    spider.close_backend()
    lease.release.assert_called_once_with()


def test_spider_close_preserves_process_control_from_snapshot_release() -> None:
    spider = _CoverageSpider()
    lease = MagicMock()
    signal = KeyboardInterrupt("lease interrupted")
    lease.release.side_effect = signal
    spider._snapshot_connection_lease = lease

    with pytest.raises(KeyboardInterrupt) as raised:
        spider.close_backend()
    assert raised.value is signal


def test_spider_close_logs_snapshot_lease_failure(mocker) -> None:
    spider = _CoverageSpider()
    lease = MagicMock()
    lease.release.side_effect = RuntimeError("lease failed")
    spider._snapshot_connection_lease = lease
    logger_error = mocker.patch.object(spider_mixin_module.logger, "error")

    with pytest.raises(RuntimeError, match="lease failed"):
        spider.close_backend()

    logger_error.assert_called_once_with("Failed to release snapshot connection lease")
    assert spider._snapshot_connection_lease is lease


def test_spider_close_manager_failure_is_logged_without_losing_teardown(mocker) -> None:
    spider = _CoverageSpider()
    manager = MagicMock()
    manager.close.side_effect = RuntimeError("manager close failed")
    spider._connection_manager = manager
    logger_error = mocker.patch.object(spider_mixin_module.logger, "error")

    with pytest.raises(RuntimeError, match="manager close failed"):
        spider.close_backend()

    manager.close.assert_called_once_with()
    logger_error.assert_called_once_with("Failed to close backend connection manager")
    assert spider._connection_manager is manager


def test_scheduler_warning_resolves_capabilities_from_a_manager(mocker) -> None:
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = ConnectionManager("kafka", {})
    warning = mocker.patch.object(scheduler_module.logger, "warning")
    scheduler_module.BackendScheduler._warn_strategy_mq_ack_bypass(object(), manager)

    warning.assert_called_once()


def test_scheduler_atomic_volatile_push_without_shadow_rolls_back() -> None:
    dupefilter = _AtomicNoVolatile()
    scheduler = BackendScheduler(
        connection_manager=MagicMock(),
        dupefilter=dupefilter,
    )
    queue = MagicMock()
    scheduler._queue = queue

    assert scheduler.enqueue_request(Request("https://example.test/volatile")) is True
    assert len(dupefilter.rollbacks) == 1


def test_scheduler_dedup_decision_failure_rolls_back_intent_and_retries() -> None:
    signal = QueueError("dedup unavailable")
    dupefilter = _AtomicNoVolatile(decision_error=signal)
    scheduler = BackendScheduler(connection_manager=MagicMock(), dupefilter=dupefilter)
    scheduler._queue = MagicMock()

    assert scheduler.enqueue_request(Request("https://example.test/dedup")) is True
    assert len(dupefilter.intent_rollbacks) == 1
    scheduler._queue.push.assert_called_once()


def test_scheduler_push_failure_with_only_owner_intent_rolls_back_intent() -> None:
    dupefilter = _AtomicNoVolatile(reservation_none=True)
    scheduler = BackendScheduler(connection_manager=MagicMock(), dupefilter=dupefilter)
    scheduler._queue = MagicMock()
    scheduler._queue.push.side_effect = QueueError("push unavailable")

    with pytest.raises(QueueError, match="not dropped"):
        scheduler.enqueue_request(Request("https://example.test/intent-push"))
    assert len(dupefilter.intent_rollbacks) == 1


def test_scheduler_push_failure_rolls_back_atomic_receipt() -> None:
    dupefilter = _AtomicNoVolatile()
    scheduler = BackendScheduler(connection_manager=MagicMock(), dupefilter=dupefilter)
    scheduler._queue = MagicMock()
    scheduler._queue.push.side_effect = QueueError("push unavailable")

    with pytest.raises(QueueError, match="not dropped"):
        scheduler.enqueue_request(Request("https://example.test/push"))
    assert len(dupefilter.rollbacks) == 1


def test_scheduler_legacy_serialization_failure_forgets_marker() -> None:
    dupefilter = MagicMock(spec=["request_seen", "forget", "log"])
    dupefilter.request_seen.return_value = False
    scheduler = BackendScheduler(connection_manager=MagicMock(), dupefilter=dupefilter)
    scheduler._queue = MagicMock()
    scheduler._queue.push.side_effect = SerializationError("cannot encode")

    assert scheduler.enqueue_request(Request("https://example.test/legacy")) is False
    dupefilter.forget.assert_called_once()


def test_scheduler_process_control_preserves_atomic_rollback_signal() -> None:
    signal = KeyboardInterrupt("interrupted")
    dupefilter = _AtomicNoVolatile(process_error=RuntimeError("rollback failed"))
    scheduler = BackendScheduler(connection_manager=MagicMock(), dupefilter=dupefilter)
    scheduler._queue = MagicMock()
    scheduler._queue.push.side_effect = signal

    with pytest.raises(KeyboardInterrupt) as raised:
        scheduler.enqueue_request(Request("https://example.test/control"))
    assert raised.value is signal
    assert len(dupefilter.intent_rollbacks) == 2


def test_scheduler_legacy_process_control_forgets_marker() -> None:
    signal = KeyboardInterrupt("push interrupted")
    dupefilter = MagicMock(spec=["request_seen", "forget", "log"])
    dupefilter.request_seen.return_value = False
    scheduler = BackendScheduler(connection_manager=MagicMock(), dupefilter=dupefilter)
    scheduler._queue = MagicMock()
    scheduler._queue.push.side_effect = signal

    with pytest.raises(KeyboardInterrupt) as raised:
        scheduler.enqueue_request(Request("https://example.test/legacy-control"))
    assert raised.value is signal
    dupefilter.forget.assert_called_once()


def test_scheduler_open_publication_cleanup_failure_preserves_primary(mocker) -> None:
    class _PublicationFailure(BaseException):
        pass

    class _Lock:
        def __init__(self) -> None:
            self._lock = threading.RLock()
            self.enters = 0

        def __enter__(self):
            self.enters += 1
            if self.enters == 3:
                raise _PublicationFailure("publication")
            self._lock.acquire()
            return self

        def __exit__(self, *_args: object) -> None:
            self._lock.release()

    scheduler = BackendScheduler(connection_manager=MagicMock())
    scheduler._lifecycle_lock = _Lock()  # type: ignore[assignment]
    scheduler._close_attempt = MagicMock(side_effect=RuntimeError("cleanup"))  # type: ignore[method-assign]
    mocker.patch.object(scheduler_module, "BackendQueue", return_value=MagicMock())

    with pytest.raises(_PublicationFailure):
        scheduler.open(Spider(name="publication"))


def test_scheduler_process_control_cleanup_failure_without_intent_preserves_signal() -> (
    None
):
    push_signal = KeyboardInterrupt("push interrupted")
    cleanup_signal = KeyboardInterrupt("cleanup interrupted")
    dupefilter = MagicMock(spec=["request_seen", "forget", "log"])
    dupefilter.request_seen.return_value = False
    dupefilter.forget.side_effect = cleanup_signal
    scheduler = BackendScheduler(connection_manager=MagicMock(), dupefilter=dupefilter)
    scheduler._queue = MagicMock()
    scheduler._queue.push.side_effect = push_signal

    with pytest.raises(KeyboardInterrupt) as raised:
        scheduler.enqueue_request(Request("https://example.test/cleanup"))
    assert raised.value is push_signal


def test_scheduler_process_control_without_dedup_still_propagates() -> None:
    signal = KeyboardInterrupt("push interrupted")
    scheduler = BackendScheduler(connection_manager=MagicMock(), dupefilter=None)
    scheduler._queue = MagicMock()
    scheduler._queue.push.side_effect = signal

    with pytest.raises(KeyboardInterrupt) as raised:
        scheduler.enqueue_request(Request("https://example.test/no-dedup"))
    assert raised.value is signal


def test_scheduler_ack_and_nack_requests_fence_missing_tokens() -> None:
    scheduler = BackendScheduler(connection_manager=MagicMock())
    scheduler._queue = MagicMock()

    request = Request("https://example.test/no-token")
    scheduler._ack_request_token(request, log_message="ack")
    scheduler._nack_request_token(request, log_message="nack")
    scheduler._queue = None
    assert scheduler._ack_token("token", log_message="ack") is False
    assert scheduler._nack_token("token", log_message="nack") is False
    scheduler._on_spider_error(MagicMock(), MagicMock(request=None), _LifecycleSpider())


def test_scheduler_queue_failure_is_indeterminate_and_retryable() -> None:
    queue = MagicMock()
    queue.close.side_effect = QueueError("checkpoint incomplete")
    scheduler = BackendScheduler(connection_manager=MagicMock())
    scheduler._queue = queue

    with pytest.raises(QueueError):
        scheduler.close("coverage")

    assert scheduler._lifecycle_state != "closed"


def test_deferred_ack_groups_keep_source_unsettled_on_ack_failure() -> None:
    scheduler = BackendScheduler(connection_manager=MagicMock())
    scheduler._ack_token = MagicMock(return_value=False)  # type: ignore[method-assign]
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")

    group.seal()
    child = group.new_child()
    assert child is not None
    child.ack()
    assert group._terminal is False
    group.abort()
    assert group._terminal is True


def test_errback_wrapper_handles_empty_and_failed_async_replacements() -> None:
    scheduler = BackendScheduler(connection_manager=MagicMock())
    queue = MagicMock()
    scheduler._queue = queue
    wrapper = scheduler_module._BackendDownloadFailureErrback(scheduler, None)

    request = Request("https://example.test/async", meta={"_backend_ack_token": "ack"})

    async def empty():
        if False:
            yield Request("https://example.test/unreachable")

    empty_iterator = wrapper._transfer_async_iterable(request, empty())
    with pytest.raises(StopAsyncIteration):
        empty_iterator.__anext__().send(None)
    queue.ack.assert_called_once_with(token="ack")

    queue.reset_mock()
    failed = Request("https://example.test/failed", meta={"_backend_ack_token": "nack"})

    async def raises():
        raise RuntimeError("replacement failed")
        yield Request("https://example.test/unreachable")

    failed_iterator = wrapper._transfer_async_iterable(failed, raises())
    with pytest.raises(RuntimeError, match="replacement failed"):
        failed_iterator.__anext__().send(None)
    queue.nack.assert_called_once_with(token="nack")

    assert wrapper._finish_failure(None, "failure") == "failure"

    sync_request = Request("https://example.test/sync")
    sync_iterator = wrapper._transfer_iterable(
        sync_request, [Request("https://example.test/replacement")]
    )
    assert next(iter(sync_iterator)).url.endswith("replacement")

    async_request = Request("https://example.test/async-no-token")

    async def one_replacement():
        yield Request("https://example.test/replacement")

    async_iterator = wrapper._transfer_async_iterable(async_request, one_replacement())
    with pytest.raises(StopIteration) as result:
        async_iterator.__anext__().send(None)
    assert result.value.value.url.endswith("replacement")


def test_errback_attach_after_group_abort_does_not_create_child() -> None:
    scheduler = BackendScheduler(connection_manager=MagicMock())
    scheduler._queue = MagicMock()
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    group.abort()
    replacement = Request("https://example.test/replacement")
    wrapper = scheduler_module._BackendDownloadFailureErrback(scheduler, None)

    wrapper._attach_replacement(group, "source", replacement)
    assert "_backend_ack_token" not in replacement.meta
    scheduler._queue.nack.assert_called_once_with(token="source")


def test_errback_conflict_without_stats_still_nacks_source() -> None:
    scheduler = BackendScheduler(connection_manager=MagicMock(), stats=None)
    scheduler._queue = MagicMock()
    source = Request(
        "https://example.test/source", meta={"_backend_ack_token": "source"}
    )
    replacement = Request(
        "https://example.test/replacement",
        meta={"_backend_ack_token": "other"},
    )
    wrapper = scheduler_module._BackendDownloadFailureErrback(scheduler, None)

    wrapper._attach_replacement(
        scheduler_module._DeferredReplacementAckGroup(scheduler, "source"),
        "source",
        replacement,
    )
    scheduler._queue.nack.assert_called_once_with(token="source")


def test_scheduler_signal_lease_replacement_is_not_removed_by_stale_disconnect() -> (
    None
):
    scheduler = BackendScheduler(connection_manager=MagicMock())

    class _Manager:
        def __init__(self) -> None:
            self.calls = 0

        def disconnect(self, _receiver: object, *, signal: object) -> None:
            del signal
            self.calls += 1
            if self.calls == 1:
                scheduler._signal_leases[0] = replacement

    lease = type("Lease", (), {})()
    replacement = type("Lease", (), {})()
    manager = _Manager()
    for item in (lease, replacement):
        item.manager = manager
        item.receiver = object()
        item.signal = object()
    scheduler._signal_leases = [lease]

    scheduler._disconnect_signal_leases()
    assert scheduler._signal_leases == []
    assert manager.calls == 2


def test_scheduler_signal_lease_disconnect_removes_exact_registration() -> None:
    scheduler = BackendScheduler(connection_manager=MagicMock())

    class _Manager:
        def __init__(self) -> None:
            self.calls: list[tuple[object, object]] = []

        def disconnect(self, receiver: object, *, signal: object) -> None:
            self.calls.append((receiver, signal))

    lease = type("Lease", (), {})()
    lease.manager = _Manager()
    lease.receiver = object()
    lease.signal = object()
    scheduler._signal_leases = [lease]
    scheduler._disconnect_signal_leases()
    assert lease.manager.calls == [(lease.receiver, lease.signal)]
    assert scheduler._signal_leases == []


def test_scheduler_close_owner_is_not_cleared_by_a_stale_attempt() -> None:
    scheduler = BackendScheduler(connection_manager=MagicMock())
    scheduler._close_locked = lambda _reason, lossy=False: setattr(  # type: ignore[method-assign]
        scheduler, "_close_attempt_owner", None
    )
    scheduler._close_attempt("stale")
    assert scheduler._close_attempt_owner is None


def test_scheduler_lifecycle_guards_and_compatibility_disconnect_are_fail_closed() -> (
    None
):
    scheduler = BackendScheduler(connection_manager=MagicMock())
    scheduler._lifecycle_state = _LIFECYCLE_OPENING
    with pytest.raises(RuntimeError, match="already in progress"):
        scheduler.close("opening")
    with pytest.raises(RuntimeError, match="already in progress"):
        scheduler.open(Spider(name="opening"))

    scheduler._lifecycle_state = _LIFECYCLE_CLOSED
    scheduler._close_locked("already-closed")

    scheduler._lifecycle_state = "closing"
    queue = MagicMock()
    queue.close.side_effect = RuntimeError("ordinary close failure")
    scheduler._queue = queue
    scheduler._connected_signals = MagicMock()
    scheduler._connected_signals.disconnect.side_effect = RuntimeError("gone")
    scheduler._close_locked("compatibility")
    assert scheduler._lifecycle_state == _LIFECYCLE_CLOSED


def test_elasticsearch_unconnected_client_property_has_typed_failure() -> None:
    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(Exception, match="client is None"):
        _ = backend.client

    assert not backend.is_connected()
