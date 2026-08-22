"""Deterministic closure tests for queue snapshots and strategy ownership.

These tests exercise the semantic edges that are easy to miss in broad queue
coverage: manifest authority, bounded generation cleanup, lease publication,
and item-preserving strategy retries.
"""

from __future__ import annotations

import base64
import hashlib
import json
import threading
from collections import OrderedDict, deque
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
from scrapy.http import Request
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

import scrapy_extension.queue.queue as queue_module
import scrapy_extension.queue.snapshot as snapshot_module
import scrapy_extension.queue.strategies.round_robin as round_robin_module
import scrapy_extension.queue.strategies.time_wheel as time_wheel_module
from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BACKEND_ACK_TOKEN_META_KEY, BackendQueue
from scrapy_extension.queue.snapshot import SnapshotRepository, SnapshotRepositoryError
from scrapy_extension.queue.strategies.base import _PreparedQueuePush
from scrapy_extension.queue.strategies.priority import PriorityQueueStrategy
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy
from scrapy_extension.queue.strategies.time_wheel import TimeWheelQueueStrategy
from scrapy_extension.queue.strategies.work_stealing import WorkStealingQueueStrategy

_KEY = "queue:snapshot:v3:0::1:q"
_SCHEMA = "scrapy-extension.queue-strategy-snapshot"
_GENERATION = "a" * 32


def _storage(
    initial: dict[str, object] | None = None,
) -> tuple[MagicMock, dict[str, object]]:
    values = dict(initial or {})
    storage = MagicMock(name="snapshot-storage")
    storage.retrieve.side_effect = lambda key: values.get(key)
    storage.store.side_effect = lambda key, value: values.__setitem__(key, value)
    storage.delete.side_effect = lambda key: values.pop(key, None)
    return storage, values


def _manifest(
    *,
    version: int = 7,
    generation: str = _GENERATION,
    length: int = 4,
    chunk_bytes: int = 4,
    chunks: int = 1,
    checksum: str | None = None,
    state: str = "bytes",
) -> bytes:
    return json.dumps(
        {
            "schema": _SCHEMA,
            "version": version,
            "generation": generation,
            "length": length,
            "chunk_bytes": chunk_bytes,
            "chunks": chunks,
            "sha256": checksum or hashlib.sha256(b"data").hexdigest(),
            "state": state,
        },
        separators=(",", ":"),
    ).encode()


def _repo_with_state(
    state: bytes = b"data",
) -> tuple[SnapshotRepository, MagicMock, dict[str, object]]:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    repository.commit(_KEY, state)
    return repository, storage, values


# ---------------------------------------------------------------------------
# Snapshot manifest marker and shape semantics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"plain legacy bytes", False),
        (b'{"schema":"' + _SCHEMA.encode(), True),
        (b'{"schema" \t : \n "' + _SCHEMA.encode()[:12], True),
        (b'{"schema":' + _SCHEMA.encode(), True),
        (b'{"schema":"not-the-current-schema"}', False),
        (b'{"payload":"schema text ' + _SCHEMA.encode() + b'"}', False),
        (b'{"payload":{"nested":true}}', False),
        (b'{"payload":"escaped\\"quote"}', False),
        (
            b'{"payload":"escaped\\\\slash","schema":"' + _SCHEMA.encode() + b'"}',
            True,
        ),
        (b'{"schema":"' + _SCHEMA.encode() + b'-suffix"}', False),
    ],
    ids=[
        "raw",
        "schema-prefix",
        "schema-whitespace",
        "unquoted-prefix",
        "wrong-schema",
        "nested-text",
        "nested-object",
        "escaped-quote",
        "escaped-value-before-schema",
        "schema-prefix-in-string",
    ],
)
def test_current_manifest_marker_scans_only_a_top_level_schema_value(
    raw: bytes, expected: bool
) -> None:
    assert SnapshotRepository._has_current_manifest_marker(raw) is expected


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(version=True),
        lambda value: value.update(version=3),
        lambda value: value.update(state="unknown"),
        lambda value: value.update(state="none", length=4),
        lambda value: value.update(generation="short"),
        lambda value: value.update(length=True),
        lambda value: value.update(length=-1),
        lambda value: value.update(chunk_bytes=True),
        lambda value: value.update(chunk_bytes=0),
        lambda value: value.update(chunks=True),
        lambda value: value.update(chunks=2),
        lambda value: value.update(sha256="short"),
        lambda value: value.update(extra=True),
    ],
    ids=[
        "boolean-version",
        "unsupported-version",
        "unknown-state",
        "none-with-payload",
        "bad-generation",
        "boolean-length",
        "negative-length",
        "boolean-chunk-size",
        "zero-chunk-size",
        "boolean-chunks",
        "geometry-mismatch",
        "bad-checksum",
        "unknown-field",
    ],
)
def test_v7_manifest_shape_is_rejected_before_any_chunk_read(mutation) -> None:
    value = json.loads(_manifest())
    mutation(value)
    storage, _values = _storage({_KEY: json.dumps(value).encode()})
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)

    with pytest.raises(SnapshotRepositoryError, match="schema"):
        repository.read(_KEY)

    storage.retrieve.assert_called_once_with(_KEY)


def test_manifest_parser_exception_is_a_static_shape_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage, _values = _storage({_KEY: b"manifest"})
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    monkeypatch.setattr(
        snapshot_module.json,
        "loads",
        lambda _value: {"schema": _SCHEMA, "version": []},
    )

    with pytest.raises(SnapshotRepositoryError, match="schema") as error:
        repository.read(_KEY)

    assert error.value.__cause__ is None
    assert error.value.__context__ is None


def test_post_copy_buffer_growth_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    class FlakyLengthBytes(bytes):
        calls = 0

        def __len__(self) -> int:
            self.calls += 1
            return 4 if self.calls == 1 else 5

    class View:
        nbytes = 4
        c_contiguous = True
        readonly = True
        obj = b"data"

        def __init__(self, _value: object) -> None:
            return None

        def tobytes(self) -> bytes:
            return FlakyLengthBytes(b"data")

        def release(self) -> None:
            return None

    monkeypatch.setattr(snapshot_module, "memoryview", View, raising=False)
    copied, error = SnapshotRepository._copy_buffer(b"data", 4)
    assert copied is None
    assert error == snapshot_module._BUFFER_OVERSIZED


# ---------------------------------------------------------------------------
# Chunk geometry, checksums, listing, deletion, and readback ambiguity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("length", 3, "schema"),
        ("chunk_bytes", 2, "schema"),
        ("chunks", 1, "schema"),
        ("sha256", "0" * 64, "checksum"),
    ],
)
def test_chunk_geometry_and_checksum_remain_authoritative(
    field: str, value: object, message: str
) -> None:
    repository, storage, values = _repo_with_state(b"abcdefgh")
    manifest = json.loads(values[_KEY])
    manifest[field] = value
    values[_KEY] = json.dumps(manifest).encode()

    with pytest.raises(SnapshotRepositoryError, match=message):
        repository.read(_KEY)

    # A malformed manifest still keeps the logical key as the first read.
    assert storage.retrieve.call_args_list[0].args == (_KEY,)


def test_gc_returns_zero_when_the_authoritative_manifest_is_absent() -> None:
    storage, _values = _storage()
    storage.list_storage_keys = MagicMock(return_value=[])
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)

    assert repository.gc(_KEY, quiescent=True) == 0
    storage.list_storage_keys.assert_not_called()


@pytest.mark.parametrize(
    "attribute",
    [
        "missing",
        "noncallable",
    ],
)
def test_gc_requires_a_callable_listing_capability(attribute: str) -> None:
    repository, storage, _values = _repo_with_state()
    if attribute == "missing":
        del storage.list_storage_keys
    else:
        storage.list_storage_keys = object()

    with pytest.raises(SnapshotRepositoryError, match="unavailable"):
        repository.gc(_KEY, quiescent=True)


def test_gc_ignores_non_string_and_noncanonical_candidates_and_honors_limit() -> None:
    repository, storage, values = _repo_with_state(b"old-state")
    old_manifest = json.loads(values[_KEY])
    old_generation = old_manifest["generation"]
    old_key = repository._chunk_key(_KEY, old_generation, 0)
    repository.commit(_KEY, b"current-state")
    current_generation = json.loads(values[_KEY])["generation"]
    current_key = repository._chunk_key(_KEY, current_generation, 0)
    storage.list_storage_keys = MagicMock(
        return_value=[b"not-a-key", "wrong-prefix", old_key, old_key, current_key]
    )

    assert repository.gc(_KEY, quiescent=True, max_deletions=1) == 1
    assert old_key not in values
    assert current_key in values


def test_gc_delete_readback_accepts_legacy_none_result() -> None:
    repository, storage, values = _repo_with_state(b"old")
    old_generation = json.loads(values[_KEY])["generation"]
    old_key = repository._chunk_key(_KEY, old_generation, 0)
    repository.commit(_KEY, b"current")
    storage.list_storage_keys = MagicMock(return_value=[old_key])
    storage.delete.side_effect = lambda key: values.pop(key, None) and None

    assert repository.gc(_KEY, quiescent=True) == 1


def test_gc_rejects_a_delete_that_leaves_the_chunk_present() -> None:
    repository, storage, values = _repo_with_state(b"old")
    old_generation = json.loads(values[_KEY])["generation"]
    old_key = repository._chunk_key(_KEY, old_generation, 0)
    repository.commit(_KEY, b"current")
    storage.list_storage_keys = MagicMock(return_value=[old_key])
    storage.delete.side_effect = lambda _key: True

    with pytest.raises(SnapshotRepositoryError, match="not confirmed") as error:
        repository.gc(_KEY, quiescent=True)

    assert error.value.confirmed_deletions == 0
    assert old_key in values


def test_maintenance_alias_has_the_same_bounded_delete_contract() -> None:
    repository, storage, values = _repo_with_state(b"old")
    old_generation = json.loads(values[_KEY])["generation"]
    old_key = repository._chunk_key(_KEY, old_generation, 0)
    repository.commit(_KEY, b"current")
    storage.list_storage_keys = MagicMock(return_value=[old_key])

    assert repository.maintenance(_KEY, quiescent=True) == 1


@pytest.mark.parametrize(
    ("candidate", "expected"),
    [
        ("other-prefix", None),
        (f"queue:snapshot-chunk:v2:{'f' * 64}:0", None),
        (f"queue:snapshot-chunk:v2:{_GENERATION}:x", None),
        (f"queue:snapshot-chunk:v2:{_GENERATION}:01", None),
        (f"queue:snapshot-chunk:v2:{_GENERATION}:4096", None),
    ],
)
def test_current_chunk_key_parser_fails_closed(candidate: str, expected) -> None:
    repository = SnapshotRepository(MagicMock(), max_bytes=32, chunk_bytes=4)
    prefix = "queue:snapshot-chunk:v2:" + ("b" * 64) + ":"
    assert repository._parse_current_chunk_key(candidate, prefix) is expected


# ---------------------------------------------------------------------------
# Repository lease publication and cleanup rollback
# ---------------------------------------------------------------------------


class _SetThenInterruptDict(dict):
    armed = False

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if self.armed:
            self.armed = False
            raise KeyboardInterrupt("lease publication")


@pytest.mark.parametrize("lease_name", ["reader", "commit"])
def test_lease_acquire_rolls_back_after_thread_registration_interrupt(
    lease_name: str,
) -> None:
    repository = SnapshotRepository(MagicMock(), max_bytes=32, chunk_bytes=4)
    mapping = _SetThenInterruptDict()
    mapping.armed = True
    if lease_name == "reader":
        repository._reader_threads = mapping
        lease = repository._reader_lease()
        active_name = "_active_readers"
    else:
        repository._commit_threads = mapping
        lease = repository._commit_lease()
        active_name = "_active_commits"

    with pytest.raises(KeyboardInterrupt):
        with lease:
            raise AssertionError("lease must not publish")

    assert mapping == {}
    assert getattr(repository, active_name) == 0


@pytest.mark.parametrize("lease_name", ["reader", "commit"])
def test_lease_release_reconciles_after_publication_interrupt(lease_name: str) -> None:
    repository = SnapshotRepository(MagicMock(), max_bytes=32, chunk_bytes=4)
    mapping = _SetThenInterruptDict()
    if lease_name == "reader":
        repository._reader_threads = mapping
        lease_factory = repository._reader_lease
        active_name = "_active_readers"
    else:
        repository._commit_threads = mapping
        lease_factory = repository._commit_lease
        active_name = "_active_commits"

    with lease_factory():
        mapping.armed = True
        with pytest.raises(KeyboardInterrupt):
            with lease_factory():
                pass

    assert mapping == {}
    assert getattr(repository, active_name) == 0


def test_reader_waits_for_foreign_maintenance_owner_without_sleep() -> None:
    repository = SnapshotRepository(MagicMock(), max_bytes=32, chunk_bytes=4)
    repository._maintenance_active = True
    repository._maintenance_owner = threading.get_ident() + 1

    def release_foreign_owner(*_args: object, **_kwargs: object) -> None:
        repository._maintenance_active = False
        repository._maintenance_owner = None

    repository._lease_condition.wait = MagicMock(side_effect=release_foreign_owner)
    with repository._reader_lease():
        assert repository._active_readers == 1
    assert repository._reader_threads == {}


def test_maintenance_waits_for_foreign_owner_without_sleep() -> None:
    repository = SnapshotRepository(MagicMock(), max_bytes=32, chunk_bytes=4)
    repository._maintenance_active = True
    repository._maintenance_owner = threading.get_ident() + 1

    def release_foreign_owner(*_args: object, **_kwargs: object) -> None:
        repository._maintenance_active = False
        repository._maintenance_owner = None

    repository._lease_condition.wait = MagicMock(side_effect=release_foreign_owner)
    with repository._maintenance_lease():
        assert repository._maintenance_owner == threading.get_ident()
    assert repository._maintenance_active is False


def test_maintenance_cleanup_retries_after_notify_interruption() -> None:
    repository = SnapshotRepository(MagicMock(), max_bytes=32, chunk_bytes=4)
    notify = MagicMock(side_effect=[KeyboardInterrupt("notify"), None])
    repository._lease_condition.notify_all = notify

    with pytest.raises(KeyboardInterrupt):
        with repository._maintenance_lease():
            pass

    assert repository._maintenance_active is False
    assert repository._maintenance_owner is None
    assert notify.call_count == 2


# ---------------------------------------------------------------------------
# Queue lifecycle fences and replacement settlement
# ---------------------------------------------------------------------------


class _FrameWithBrokenLocals:
    f_back = None

    @property
    def f_locals(self):
        raise RuntimeError("locals unavailable")


def test_close_owner_liveness_fails_closed_on_frame_introspection_errors(monkeypatch):
    token = queue_module._CloseOwnerToken()
    monkeypatch.setattr(
        queue_module.sys,
        "current_frames",
        lambda: (_ for _ in ()).throw(RuntimeError("frames")),
        raising=False,
    )
    assert token.active is False

    monkeypatch.setattr(
        queue_module.sys,
        "current_frames",
        lambda: {token.thread_id: _FrameWithBrokenLocals()},
        raising=False,
    )
    assert token.active is False


def _queue_for_lifecycle(
    strategy: MagicMock | None = None, storage: MagicMock | None = None
) -> tuple[BackendQueue, MagicMock, MagicMock]:
    manager = MagicMock(name="queue-manager")
    storage = storage or _storage()[0]
    manager.get_storage_backend.return_value = storage
    manager.get_queue_backend.return_value = MagicMock(name="queue-backend")
    strategy = strategy or MagicMock(name="strategy")
    strategy.snapshot.return_value = None
    queue = BackendQueue(manager, "q", queue_strategy=strategy, monitor=MagicMock())
    return queue, strategy, storage


def test_queue_set_monitor_forwards_only_monitor_aware_strategies() -> None:
    queue, strategy, _storage_value = _queue_for_lifecycle()
    monitor = MagicMock(name="new-monitor")
    queue.set_monitor(monitor)
    strategy.set_monitor.assert_called_with(monitor)

    strategy.set_monitor = None
    queue.set_monitor(MagicMock(name="ignored-monitor"))


def test_push_preserves_interrupted_post_commit_marker_when_requested() -> None:
    queue, _strategy, _storage_value = _queue_for_lifecycle()

    def committed(_request: Request, _priority: float) -> bool:
        queue._operation_context.push_commits[-1] = True
        return True

    queue._push = committed  # type: ignore[method-assign]
    queue._emit_push_monitor = MagicMock(side_effect=KeyboardInterrupt("monitor"))  # type: ignore[method-assign]
    with pytest.raises(KeyboardInterrupt):
        queue._push_with_durability(
            Request("https://example.com/committed"),
            _preserve_post_commit_marker=True,
        )

    assert queue._consume_post_commit_push() is True
    assert queue._consume_post_commit_push() is False


def test_volatile_replacement_route_is_rejected_without_acknowledgement() -> None:
    token = object()
    request = Request("https://example.com/replacement")

    # A concrete backend strategy is required for the durable-route seam.
    from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy

    strategy = PassthroughQueueStrategy(MagicMock())
    strategy._prepare_push = MagicMock(  # type: ignore[method-assign]
        return_value=_PreparedQueuePush(
            backend_route=True,
            _commit=lambda _item, _require_durable: False,
        )
    )
    strategy.snapshot = MagicMock(return_value=None)
    queue, _strategy, _storage_value = _queue_for_lifecycle(strategy)
    request.meta[BACKEND_ACK_TOKEN_META_KEY] = token
    with pytest.raises(QueueError, match="durability receipt"):
        queue._push_with_durability(request)

    assert request.meta[BACKEND_ACK_TOKEN_META_KEY] is token


def test_replacement_ack_settlement_uses_authoritative_operation(monkeypatch):
    queue, _strategy, _storage_value = _queue_for_lifecycle()
    operation: Deferred[None] = Deferred()
    bounded: Deferred[None] = Deferred()
    monkeypatch.setattr(
        queue_module,
        "defer_to_thread_ordered",
        lambda *_args, **_kwargs: (operation, bounded),
    )
    token = object()
    request = Request("https://example.com/replacement")
    request.meta[BACKEND_ACK_TOKEN_META_KEY] = token

    queue._schedule_replacement_ack(request, token)
    assert operation in queue._pending_replacement_settlements
    operation.callback(None)
    assert request.meta.get(BACKEND_ACK_TOKEN_META_KEY) is None
    assert operation not in queue._pending_replacement_settlements

    bounded.errback(Failure(RuntimeError("ack failed")))
    assert queue._pending_replacement_settlements == set()


def test_process_pop_rejects_oversized_and_foreign_payloads_without_loss():
    queue, strategy, _storage_value = _queue_for_lifecycle()
    queue.max_item_bytes = 4
    strategy.pop_with_ack.return_value = (b"12345", None)
    with pytest.raises(Exception, match="Failed to deserialize request"):
        queue.pop()

    class Spider:
        name = "owner"

    queue, strategy, _storage_value = _queue_for_lifecycle()
    queue._spider = Spider()
    payload = queue._request_to_dict(Request("https://example.com/foreign"))
    payload["_scrapy_extension_spider"] = "other"
    payload["_scrapy_extension_project"] = "other-project"
    foreign = json.dumps(payload).encode()
    strategy.pop_with_ack.return_value = (foreign, None)
    strategy.push.side_effect = RuntimeError("return failed")
    with pytest.raises(QueueError, match="return"):
        queue.pop()


def test_queue_wire_validation_rejects_codec_identity_priority_and_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, _strategy, _storage_value = _queue_for_lifecycle()
    with pytest.raises(Exception, match="Unsupported queued request body codec"):
        queue._decode_body({"_scrapy_extension_body_codec": "future"})
    queue._spider = type("Spider", (), {"name": "owner"})()
    with pytest.raises(Exception, match="malformed"):
        queue._validate_envelope_identity({"_scrapy_extension_spider": 1})

    monkeypatch.setattr(
        queue_module.math, "isfinite", MagicMock(side_effect=OverflowError)
    )
    with pytest.raises(TypeError, match="priority"):
        queue._validate_serialized_priority({"priority": 1})

    for value in ([1], {1: "ok"}, {"x": object()}, {"x": [object()]}):
        with pytest.raises(TypeError):
            queue._validate_request_dict(
                {"flags": value} if isinstance(value, list) else {"headers": value}
            )


def test_request_callback_lookup_failure_is_a_clean_value_error():
    class Spider:
        name = "owner"

    queue, _strategy, _storage_value = _queue_for_lifecycle()
    queue._spider = Spider()
    with pytest.raises(ValueError, match="not found"):
        queue._request_from_dict({"url": "https://example.com", "callback": "missing"})


# ---------------------------------------------------------------------------
# Strategy scan and in-process item preservation
# ---------------------------------------------------------------------------


def _backend_strategy(strategy_type, **kwargs):
    manager = MagicMock(name="strategy-manager")
    backend = MagicMock(name="queue-backend")
    backend.pop.return_value = None
    backend.pop_with_ack.return_value = (None, None)
    manager.get_queue_backend.return_value = backend
    manager._push_queue_with_durability.return_value = SimpleNamespace(
        worker_crash_durable=True
    )
    backend.queue_len.return_value = 0
    return strategy_type(manager, **kwargs), backend


def test_priority_fraction_validation_prepare_route_and_empty_scan():
    strategy, backend = _backend_strategy(PriorityQueueStrategy, levels=2)
    with pytest.raises(ValueError, match="finite"):
        strategy._level_for(10**1000)
    with pytest.raises(ValueError):
        strategy._level_for(float("inf"))
    prepared = strategy._prepare_push("q", priority=1.0)
    assert prepared.commit(b"item", require_durable=True) is True
    assert strategy.is_push_durable(delay=0.0, source="default") is True
    assert strategy.pop_with_ack("q", timeout=0.0) == (None, None)
    assert backend.pop_with_ack.call_count == 2


def test_priority_fractional_nonfinite_conversion_is_rejected(monkeypatch):
    strategy, _backend = _backend_strategy(PriorityQueueStrategy, levels=3)
    isfinite = MagicMock(side_effect=[True, False])
    monkeypatch.setattr(
        strategy_module := __import__(
            "scrapy_extension.queue.strategies.priority", fromlist=["math"]
        ).math,
        "isfinite",
        isfinite,
    )
    with pytest.raises(ValueError, match="finite"):
        strategy._level_for(1.5)


def test_round_robin_restore_and_pop_rollback_preserve_the_head():
    strategy = RoundRobinQueueStrategy(MagicMock())
    strategy.push("q", b"head", source="source")

    class FailingSources(OrderedDict):
        failed = False

        def __delitem__(self, key):
            super().__delitem__(key)
            if not self.failed:
                self.failed = True
                raise KeyboardInterrupt("rotation")

    strategy._sources = FailingSources(strategy._sources)
    with pytest.raises(KeyboardInterrupt):
        strategy.pop("q")
    assert strategy.pop("q") == b"head"

    invalid = json.dumps(
        {
            "version": 1,
            "strategy": "round_robin",
            "sources": [{"source": 1, "items": []}],
        }
    ).encode()
    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(invalid)


def test_round_robin_durability_and_restore_logger_failure_are_safe(monkeypatch):
    strategy = RoundRobinQueueStrategy(MagicMock())
    assert strategy.is_push_durable(delay=0.0, source="x") is False
    strategy.push("q", b"item", source="x")
    monkeypatch.setattr(
        round_robin_module.logger, "info", MagicMock(side_effect=KeyboardInterrupt)
    )
    restored = RoundRobinQueueStrategy(MagicMock())
    restored.restore(strategy.snapshot())
    assert restored.pop("q") == b"item"


def test_time_wheel_append_drain_and_clear_rollback_preserve_state(monkeypatch):
    strategy, backend = _backend_strategy(
        TimeWheelQueueStrategy,
        wheel_size=4,
        clock=lambda: 0.0,
        wall_clock=lambda: 0.0,
    )
    strategy.bind("q")

    class FailingDeque(deque):
        def append(self, value):
            super().append(value)
            raise KeyboardInterrupt("append")

    strategy._wheel[1] = FailingDeque()
    with pytest.raises(KeyboardInterrupt):
        strategy._append_wheel_entry(1, (1.0, b"item", 0.0))
    assert list(strategy._wheel[1]) == []

    # A wheel candidate which vanished between scan and settlement is skipped.
    strategy._wheel[0] = deque([(0.0, b"gone", 0.0)])
    strategy._wheel_sequences[0] = type(
        "MissingSequence",
        (deque,),
        {"index": lambda self, _value: (_ for _ in ()).throw(ValueError())},
    )([0])
    strategy._last_tick = -1
    strategy._drain_ready("q")
    assert backend.push.call_count == 0

    # Backend success followed by local bookkeeping interruption rolls both
    # parallel containers back, preserving the item and sequence.
    class DeleteOnce(list):
        failed = False

        def __delitem__(self, index):
            super().__delitem__(index)
            if not self.failed:
                self.failed = True
                raise KeyboardInterrupt("delete")

    strategy._wheel_sequences[0] = DeleteOnce([0])
    strategy._wheel[0] = deque([(0.0, b"retry", 0.0)])
    strategy._last_tick = -1
    with pytest.raises(KeyboardInterrupt):
        strategy._drain_ready("q")
    assert list(strategy._wheel[0]) == [(0.0, b"retry", 0.0)]
    assert list(strategy._wheel_sequences[0]) == [0]

    # A malformed heap ordering cannot make a non-root overflow item disappear.
    strategy._overflow = [(2.0, 2, b"root", 0.0), (1.0, 1, b"tail", 0.0)]
    strategy._clock = lambda: 2.0
    strategy._drain_ready("q")
    assert backend.push.call_args_list[-2:] == [
        call("q", b"tail", 0.0),
        call("q", b"root", 0.0),
    ]

    previous = strategy._wheel
    original_deque = time_wheel_module.deque
    monkeypatch.setattr(
        time_wheel_module, "deque", lambda: (_ for _ in ()).throw(MemoryError("clear"))
    )
    with pytest.raises(MemoryError):
        strategy.clear("q")
    assert strategy._wheel is previous
    monkeypatch.setattr(time_wheel_module, "deque", original_deque)


def test_time_wheel_restore_rejects_invalid_entry_and_priority_and_keeps_item():
    strategy, backend = _backend_strategy(
        TimeWheelQueueStrategy,
        wheel_size=4,
        clock=lambda: 10.0,
        wall_clock=lambda: 100.0,
    )
    strategy.push("q", b"keep", delay=1.0)
    for entry in [
        {
            "remaining": 1.0,
            "item_b64": base64.b64encode(b"x").decode(),
            "priority": float("inf"),
        },
        42,
    ]:
        state = json.dumps(
            {
                "version": 2,
                "strategy": "time_wheel",
                "snapshot_wall_time": 100.0,
                "slots_flat": [entry],
                "overflow": [],
            },
            allow_nan=True,
        ).encode()
        with pytest.raises(QueueError, match="snapshot restore failed"):
            strategy.restore(state)
    backend.queue_len.return_value = 0
    assert strategy.queue_len("q") == 1


def test_time_wheel_restore_logger_failure_does_not_change_restored_item(monkeypatch):
    source, _backend = _backend_strategy(
        TimeWheelQueueStrategy,
        wheel_size=4,
        clock=lambda: 0.0,
        wall_clock=lambda: 100.0,
    )
    source.push("q", b"held", delay=1.0)
    state = source.snapshot()
    assert state is not None
    monkeypatch.setattr(
        time_wheel_module.logger, "info", MagicMock(side_effect=KeyboardInterrupt)
    )
    target, _target_backend = _backend_strategy(
        TimeWheelQueueStrategy,
        wheel_size=4,
        clock=lambda: 0.0,
        wall_clock=lambda: 100.0,
    )
    target.restore(state)
    assert target.queue_len("q") == 1


def test_work_stealing_prepare_and_final_scans_preserve_peer_items():
    strategy, backend = _backend_strategy(
        WorkStealingQueueStrategy,
        worker_id="w1",
        peer_ids=(),
    )
    assert strategy.is_push_durable(delay=0.0, source="default") is True
    prepared = strategy._prepare_push("q", priority=3.0)
    assert prepared.commit(b"item", require_durable=True) is True
    backend.pop_with_ack.side_effect = [
        (None, None),
        (None, None),
        (b"peer", "token"),
    ]
    # With no peers the blocking own queue is the only fallback; a token is
    # still authoritative when it arrives there.
    assert strategy.pop_with_ack("q", timeout=1.0) == (b"peer", "token")

    strategy, backend = _backend_strategy(
        WorkStealingQueueStrategy,
        worker_id="w1",
        peer_ids=("w2",),
    )
    backend.pop.side_effect = [None, None, None, b"own"]
    assert strategy.pop("q", timeout=1.0) == b"own"


def test_work_stealing_empty_ack_scan_returns_empty_without_replay():
    strategy, backend = _backend_strategy(
        WorkStealingQueueStrategy,
        worker_id="w1",
        peer_ids=(),
    )
    backend.pop_with_ack.return_value = (None, None)
    assert strategy.pop_with_ack("q", timeout=0.0) == (None, None)
    assert backend.pop_with_ack.call_count == 1
