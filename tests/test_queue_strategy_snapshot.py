"""Tests for QueueStrategy snapshot/restore (initiative #3) + BackendQueue wiring.

Covers:
- Delay snapshot serializes the held heap (versioned JSON, seq excluded)
- Delay restore round-trips, skips corrupt/unknown-format/malformed entries
- Restored past-ready items drain on the next pop
- ABC defaults (passthrough snapshot -> None, restore no-op)
- BackendQueue.close() persists BEFORE strategy.close() clears the heap
- BackendQueue.__init__ restores on construction
- Storage-incapable backends skip gracefully (no crash)
"""

from __future__ import annotations

import base64
import json
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
from scrapy.http import Request

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.snapshot import SnapshotRepository
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy
from scrapy_extension.queue.strategies.ring_buffer import RingBufferQueueStrategy

_SNAPSHOT_KEY = "queue:snapshot:v3:0::1:q"
_SNAPSHOT_TOMBSTONE_KEY = "queue:snapshot-tombstone:v3:0::1:q"
_LEGACY_SNAPSHOT_KEY = "queue:snapshot:q"


def _delay(
    *,
    clock_value: float = 100.0,
    wall_clock_value: float = 1_000.0,
    default_delay: float = 0.0,
) -> DelayQueueStrategy:
    """DelayQueueStrategy with a frozen clock (deterministic ready_at)."""
    return DelayQueueStrategy(
        MagicMock(name="ConnectionManager"),
        default_delay=default_delay,
        clock=lambda: clock_value,
        wall_clock=lambda: wall_clock_value,
    )


def _strip_seq(heap: list) -> list:
    """Compare heaps modulo the seq tie-breaker (re-sequenced on restore)."""
    return sorted((ready_at, item, priority) for ready_at, _s, item, priority in heap)


# ---------------------------------------------------------------------------
# Delay snapshot
# ---------------------------------------------------------------------------


def test_delay_snapshot_empty_returns_none():
    """An empty held heap snapshots to None (nothing to persist)."""
    assert _delay().snapshot() is None


def test_delay_snapshot_serializes_held_items():
    """A non-empty heap serializes to a versioned JSON bytes blob."""
    strategy = _delay(clock_value=100.0)
    strategy.push("q", b"item-1", delay=10.0, priority=2.0)
    strategy.push("q", b"item-2", delay=20.0, priority=1.0)

    blob = strategy.snapshot()
    assert blob is not None
    data = json.loads(blob.decode("utf-8"))
    assert data["version"] == 2
    assert data["strategy"] == "delay"
    assert len(data["items"]) == 2
    assert data["snapshot_wall_time"] == 1_000.0
    by_remaining = {item["remaining"]: item for item in data["items"]}
    assert set(by_remaining) == {10.0, 20.0}
    assert base64.b64decode(by_remaining[10.0]["item_b64"]) == b"item-1"
    assert base64.b64decode(by_remaining[20.0]["item_b64"]) == b"item-2"
    assert by_remaining[10.0]["priority"] == 2.0
    assert by_remaining[20.0]["priority"] == 1.0


def test_delay_snapshot_excludes_seq():
    """The seq tie-breaker is NOT persisted (re-sequenced fresh on restore)."""
    strategy = _delay()
    strategy.push("q", b"x", delay=5.0)

    data = json.loads(strategy.snapshot().decode("utf-8"))
    assert "seq" not in data["items"][0]


# ---------------------------------------------------------------------------
# Delay restore
# ---------------------------------------------------------------------------


def test_delay_restore_round_trip_preserves_state():
    """snapshot -> restore on a fresh strategy reproduces the heap (modulo seq)."""
    src = _delay(clock_value=100.0)
    src.push("q", b"a", delay=10.0, priority=1.0)
    src.push("q", b"b", delay=20.0, priority=2.0)

    dst = _delay(clock_value=100.0)
    dst.restore(src.snapshot())

    assert _strip_seq(dst._holding) == [(110.0, b"a", 1.0), (120.0, b"b", 2.0)]


def test_delay_restore_rebases_monotonic_deadline_and_counts_downtime():
    """Persisted deadlines must survive a different monotonic clock epoch."""
    source_clock = [5_000.0]
    source_wall = [10_000.0]
    source = DelayQueueStrategy(
        MagicMock(),
        clock=lambda: source_clock[0],
        wall_clock=lambda: source_wall[0],
    )
    source.push("q", b"later", delay=60.0)
    blob = source.snapshot()

    destination_clock = [7.0]
    destination_wall = [10_030.0]
    manager = MagicMock()
    backend = manager.get_queue_backend.return_value
    destination = DelayQueueStrategy(
        manager,
        clock=lambda: destination_clock[0],
        wall_clock=lambda: destination_wall[0],
    )
    destination.restore(blob)

    # 30 seconds elapsed while stopped, leaving 30 seconds from the new
    # monotonic epoch: 7 + 30 = 37, never the old absolute 5060.
    assert destination._holding[0][0] == 37.0
    destination_clock[0] = 36.9
    destination.pop("q")
    backend.push.assert_not_called()
    destination_clock[0] = 37.0
    destination.pop("q")
    backend.push.assert_called_once_with("q", b"later", 0.0)


def test_delay_restore_v1_deadline_is_due_instead_of_cross_boot_stall():
    """Legacy absolute-monotonic snapshots cannot be rebased reliably."""
    blob = json.dumps(
        {
            "version": 1,
            "strategy": "delay",
            "items": [
                {
                    "ready_at": 999_999.0,
                    "item_b64": base64.b64encode(b"legacy").decode(),
                    "priority": 0.0,
                }
            ],
        }
    ).encode()
    strategy = _delay(clock_value=3.0)

    strategy.restore(blob)

    assert strategy._holding[0][0] == 3.0


def test_delay_restore_none_is_noop():
    """restore(None) and restore(b'') are silent no-ops."""
    strategy = _delay()
    strategy.restore(None)
    strategy.restore(b"")
    assert strategy._holding == []


def test_delay_restore_corrupt_json_skipped():
    """restore of non-JSON bytes logs + skips (no crash, heap stays empty)."""
    strategy = _delay()
    strategy.restore(b"\x00 not json \x00")
    assert strategy._holding == []


def test_delay_restore_unknown_format_skipped():
    """restore of valid JSON with the wrong strategy/version is skipped."""
    strategy = _delay()
    blob = json.dumps({"version": 99, "strategy": "other", "items": []}).encode()
    strategy.restore(blob)
    assert strategy._holding == []


def test_delay_restore_skips_malformed_entries():
    """A snapshot with one good + one bad entry recovers the good one only."""
    strategy = _delay()
    good = {
        "ready_at": 110.0,
        "item_b64": base64.b64encode(b"ok").decode(),
        "priority": 1.0,
    }
    bad = {"ready_at": "not-a-float", "item_b64": "!!!"}
    blob = json.dumps(
        {"version": 1, "strategy": "delay", "items": [good, bad]}
    ).encode()

    strategy.restore(blob)

    assert len(strategy._holding) == 1
    assert strategy._holding[0][2] == b"ok"  # item bytes recovered


def test_delay_restore_past_ready_drains_on_pop():
    """Restored items with past ready_at drain into the live queue on the next pop."""
    src = _delay(clock_value=100.0)
    src.push("q", b"due", delay=10.0)  # ready_at = 110
    blob = src.snapshot()

    dst = _delay(
        clock_value=200.0,
        wall_clock_value=1_100.0,
    )  # 100s downtime -> item is due
    dst.restore(blob)

    dst.pop("q")  # drain fires

    qb = dst._connection_manager.get_queue_backend()
    qb.push.assert_called_once_with("q", b"due", 0.0)


# ---------------------------------------------------------------------------
# ABC defaults
# ---------------------------------------------------------------------------


def test_passthrough_snapshot_returns_none_default():
    """The ABC default snapshot() returns None (passthrough has no state)."""
    assert PassthroughQueueStrategy(MagicMock()).snapshot() is None


def test_abc_restore_default_is_noop():
    """The ABC default restore() accepts any state without crashing."""
    strategy = PassthroughQueueStrategy(MagicMock())
    strategy.restore(b"anything")
    strategy.restore(None)


# ---------------------------------------------------------------------------
# BackendQueue wiring (close persists, init restores, storage-incapable skips)
# ---------------------------------------------------------------------------


def _storage_mock(retrieve_return=None):
    storage = MagicMock(name="StorageBackend")
    storage.retrieve.return_value = retrieve_return
    return storage


def _stateful_storage(initial: dict[str, bytes]):
    """Storage mock whose retrieve/store/delete calls mutate shared state."""
    state = dict(initial)
    storage = MagicMock(name="StatefulStorageBackend")
    storage.retrieve.side_effect = lambda key: state.get(key)
    storage.store.side_effect = lambda key, value: state.__setitem__(key, value)
    storage.delete.side_effect = lambda key: state.pop(key, None)
    return storage, state


def _legacy_delay_snapshot() -> bytes:
    source = _delay(clock_value=100.0)
    source.push("q", b"legacy-recovered", delay=10.0)
    state = source.snapshot()
    assert state is not None
    return state


def _stored_snapshot_payload(storage, key: str = _SNAPSHOT_KEY) -> bytes | None:
    """Reassemble the v5 payload from captured mock store calls."""
    values = {args.args[0]: args.args[1] for args in storage.store.call_args_list}
    reader = MagicMock()
    reader.retrieve.side_effect = lambda stored_key: values.get(stored_key)
    return SnapshotRepository(reader).read(key).state


def _wired_cm(storage=None, queue_backend=None):
    cm = MagicMock(name="ConnectionManager")
    cm.get_storage_backend.return_value = (
        storage if storage is not None else _storage_mock()
    )
    cm.get_queue_backend.return_value = queue_backend or MagicMock(name="QueueBackend")
    return cm


def test_backends_queue_close_persists_snapshot_before_clearing():
    """close() snapshots the held heap THEN strategy.close() clears it.

    Regression guard: the snapshot must capture state BEFORE Delay.close()
    clears _holding, or the persisted blob is empty.
    """
    storage = _storage_mock()
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)
    strategy.push("q", b"x", delay=10.0)
    bq = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    bq.close()

    payload = _stored_snapshot_payload(storage)
    assert payload is not None
    assert json.loads(payload.decode())["items"][0]["item_b64"]


@pytest.mark.parametrize("operation", ["push", "pop", "ack", "nack", "len", "clear"])
def test_close_rejects_operations_after_snapshot_store_starts(operation):
    storage = _storage_mock()
    store_started = threading.Event()
    release_store = threading.Event()
    close_done = threading.Event()
    close_errors: list[Exception] = []

    def blocking_store(key, state):
        del key, state
        store_started.set()
        if not release_store.wait(timeout=2.0):
            raise AssertionError("snapshot store was not released")

    storage.store.side_effect = blocking_store
    cm = _wired_cm(storage=storage)
    strategy = RingBufferQueueStrategy(cm, capacity=2)
    queue = BackendQueue(cm, "q", queue_strategy=strategy, monitor=MagicMock())
    queue.push(Request("https://example.com/first"))

    def close_queue():
        try:
            queue.close()
        except Exception as exc:
            close_errors.append(exc)
        finally:
            close_done.set()

    thread = threading.Thread(target=close_queue, daemon=True)
    thread.start()
    assert store_started.wait(timeout=2.0), "close should reach snapshot storage"

    try:
        with pytest.raises(QueueError, match="clos"):
            if operation == "push":
                queue.push(Request("https://example.com/late"))
            elif operation == "pop":
                queue.pop()
            elif operation == "ack":
                queue.ack(token="late-token")
            elif operation == "nack":
                queue.nack(token="late-token")
            elif operation == "len":
                len(queue)
            else:
                queue.clear()
    finally:
        release_store.set()
        thread.join(timeout=2.0)

    assert close_done.is_set()
    assert not thread.is_alive()
    assert close_errors == []
    payload = _stored_snapshot_payload(storage)
    assert payload is not None
    assert len(json.loads(payload)["items"]) == 1


def test_close_waits_for_entered_push_before_snapshot():
    storage = _storage_mock()
    cm = _wired_cm(storage=storage)
    strategy = MagicMock(name="QueueStrategy")
    push_entered = threading.Event()
    release_push = threading.Event()
    pushed_items: list[bytes] = []
    push_errors: list[Exception] = []
    close_errors: list[Exception] = []

    def blocking_push(_queue_name, item, **_kwargs):
        push_entered.set()
        if not release_push.wait(timeout=2.0):
            raise AssertionError("entered push was not released")
        pushed_items.append(item)

    strategy.push.side_effect = blocking_push
    strategy.begin_close.side_effect = release_push.set
    strategy.close.side_effect = release_push.set
    strategy.snapshot.side_effect = lambda: str(len(pushed_items)).encode()
    queue = BackendQueue(cm, "q", queue_strategy=strategy, monitor=MagicMock())

    def push_request():
        try:
            queue.push(Request("https://example.com/in-flight"))
        except Exception as exc:
            push_errors.append(exc)

    def close_queue():
        try:
            queue.close()
        except Exception as exc:
            close_errors.append(exc)

    push_thread = threading.Thread(target=push_request, daemon=True)
    close_thread = threading.Thread(target=close_queue, daemon=True)
    push_thread.start()
    assert push_entered.wait(timeout=2.0)
    close_thread.start()
    close_thread.join(timeout=2.0)
    release_push.set()
    push_thread.join(timeout=2.0)

    assert not push_thread.is_alive()
    assert not close_thread.is_alive()
    assert push_errors == []
    assert close_errors == []
    assert _stored_snapshot_payload(storage) == b"1"


@pytest.mark.parametrize("operation", ["ack", "nack"])
def test_close_waits_for_entered_terminal_operation(operation):
    storage = _storage_mock()
    backend = MagicMock(name="QueueBackend")
    cm = _wired_cm(storage=storage, queue_backend=backend)
    strategy = MagicMock(name="QueueStrategy")
    strategy.snapshot.return_value = None
    operation_entered = threading.Event()
    release_operation = threading.Event()
    begin_close_called = threading.Event()
    operation_errors: list[Exception] = []
    close_errors: list[Exception] = []

    def blocking_terminal(*_args, **_kwargs):
        operation_entered.set()
        if not release_operation.wait(timeout=2.0):
            raise AssertionError("terminal operation was not released")

    getattr(backend, operation).side_effect = blocking_terminal
    strategy.begin_close.side_effect = begin_close_called.set
    queue = BackendQueue(cm, "q", queue_strategy=strategy, monitor=MagicMock())

    def run_operation():
        try:
            getattr(queue, operation)(token="delivery-token")
        except Exception as exc:
            operation_errors.append(exc)

    def close_queue():
        try:
            queue.close()
        except Exception as exc:
            close_errors.append(exc)

    operation_thread = threading.Thread(target=run_operation, daemon=True)
    close_thread = threading.Thread(target=close_queue, daemon=True)
    operation_thread.start()
    assert operation_entered.wait(timeout=2.0)
    close_thread.start()
    assert begin_close_called.wait(timeout=2.0)

    try:
        assert close_thread.is_alive(), "close returned before ack/nack completed"
        storage.delete.assert_not_called()
    finally:
        release_operation.set()
        operation_thread.join(timeout=2.0)
        close_thread.join(timeout=2.0)

    assert not operation_thread.is_alive()
    assert not close_thread.is_alive()
    assert operation_errors == []
    assert close_errors == []
    getattr(backend, operation).assert_called_once_with("q", token="delivery-token")


def test_blocked_pop_does_not_hold_queue_lifecycle_lock():
    storage = _storage_mock()
    cm = _wired_cm(storage=storage)
    strategy = MagicMock(name="QueueStrategy")
    pop_entered = threading.Event()
    release_pop = threading.Event()
    begin_close_called = threading.Event()
    pop_errors: list[Exception] = []
    close_errors: list[Exception] = []

    def blocking_pop(_queue_name, _timeout):
        pop_entered.set()
        if not release_pop.wait(timeout=2.0):
            raise AssertionError("blocked pop was not released")
        return (None, None)

    def begin_close():
        begin_close_called.set()
        release_pop.set()

    strategy.pop_with_ack.side_effect = blocking_pop
    strategy.begin_close.side_effect = begin_close
    strategy.close.side_effect = release_pop.set
    strategy.snapshot.return_value = None
    queue = BackendQueue(cm, "q", queue_strategy=strategy, monitor=MagicMock())

    def pop_request():
        try:
            queue.pop(timeout=30.0)
        except Exception as exc:
            pop_errors.append(exc)

    def close_queue():
        try:
            queue.close()
        except Exception as exc:
            close_errors.append(exc)

    pop_thread = threading.Thread(target=pop_request, daemon=True)
    close_thread = threading.Thread(target=close_queue, daemon=True)
    pop_thread.start()
    assert pop_entered.wait(timeout=2.0)
    close_thread.start()

    try:
        assert begin_close_called.wait(timeout=2.0), (
            "close should run begin_close while broker pop is blocked"
        )
    finally:
        release_pop.set()
        pop_thread.join(timeout=2.0)
        close_thread.join(timeout=2.0)

    assert not pop_thread.is_alive()
    assert not close_thread.is_alive()
    assert pop_errors == []
    assert close_errors == []


def test_backends_queue_init_restores_snapshot():
    """BackendQueue.__init__ retrieves the snapshot + strategy.restore runs."""
    src = _delay(clock_value=100.0)
    src.push("q", b"recovered", delay=10.0)
    blob = src.snapshot()

    storage = _storage_mock(retrieve_return=blob)
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)

    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert len(strategy._holding) == 1
    assert strategy._holding[0][2] == b"recovered"
    storage.retrieve.assert_called_once_with(_SNAPSHOT_KEY)
    storage.delete.assert_not_called()


def test_backends_queue_restores_marker_like_v3_snapshot_bytes():
    """A strategy's arbitrary snapshot bytes are never interpreted as a marker."""
    marker_like_state = b"\x00scrapy-extension:empty-snapshot:v3"
    storage = _storage_mock(retrieve_return=marker_like_state)
    cm = _wired_cm(storage=storage)
    strategy = MagicMock(name="OpaqueBytesStrategy")

    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    strategy.restore.assert_called_once_with(marker_like_state)


def test_backends_queue_marker_retrieval_failure_skips_legacy_fallback():
    """A marker read error must not resurrect the legacy checkpoint."""
    storage, state = _stateful_storage({_LEGACY_SNAPSHOT_KEY: _legacy_delay_snapshot()})

    def retrieve(key: str):
        if key == _SNAPSHOT_TOMBSTONE_KEY:
            raise RuntimeError("marker storage unavailable")
        return state.get(key)

    storage.retrieve.side_effect = retrieve
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)

    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert strategy._holding == []
    assert storage.retrieve.call_args_list == [
        call(_SNAPSHOT_KEY),
        call(_SNAPSHOT_TOMBSTONE_KEY),
    ]


def test_backends_queue_restores_legacy_snapshot_when_v3_is_missing():
    """An upgrade restores and retires an existing legacy checkpoint."""
    legacy_state = _legacy_delay_snapshot()
    legacy_key = _LEGACY_SNAPSHOT_KEY
    storage, state = _stateful_storage({legacy_key: legacy_state})
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)

    queue = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert len(strategy._holding) == 1
    assert strategy._holding[0][2] == b"legacy-recovered"
    storage.retrieve.assert_has_calls(
        [
            call(queue._snapshot_key()),
            call(_SNAPSHOT_TOMBSTONE_KEY),
            call(legacy_key),
        ]
    )
    queue.close()
    assert queue._snapshot_key() in state
    assert legacy_key not in state


def test_backends_queue_prefers_v3_snapshot_over_legacy_snapshot():
    """A current checkpoint must prevent a stale legacy checkpoint from replaying."""
    current = _delay(clock_value=100.0)
    current.push("q", b"v3", delay=10.0)
    current_state = current.snapshot()
    assert current_state is not None
    storage, _state = _stateful_storage(
        {
            _SNAPSHOT_KEY: current_state,
            _LEGACY_SNAPSHOT_KEY: _legacy_delay_snapshot(),
        }
    )
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)

    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert len(strategy._holding) == 1
    assert strategy._holding[0][2] == b"v3"
    storage.retrieve.assert_called_once_with(_SNAPSHOT_KEY)


@pytest.mark.parametrize(
    ("spider", "queue_name", "legacy_key"),
    [
        (SimpleNamespace(name="a"), "b", "queue:snapshot:a:b"),
        (SimpleNamespace(name="a:b"), "c", "queue:snapshot:a:b:c"),
        (SimpleNamespace(name="a"), "b:c", "queue:snapshot:a:b:c"),
        (None, "a:b:c", "queue:snapshot:a:b:c"),
    ],
    ids=[
        "scoped-collides-with-unscoped",
        "colon-in-spider",
        "colon-in-queue",
        "colon-in-unscoped-queue",
    ],
)
def test_backends_queue_skips_non_unique_legacy_snapshot(
    spider, queue_name: str, legacy_key: str
):
    """A non-unique legacy identity must not restore or delete another queue."""
    storage, state = _stateful_storage({legacy_key: _legacy_delay_snapshot()})
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)
    queue = BackendQueue(
        connection_manager=cm,
        queue_name=queue_name,
        spider=spider,
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert strategy._holding == []
    storage.retrieve.assert_called_once_with(queue._snapshot_key())

    queue.close()

    assert legacy_key in state
    storage.delete.assert_not_called()


def test_backends_queue_empty_close_retires_restored_legacy_snapshot():
    """A clean empty checkpoint deletes both v3 and the restored legacy key."""
    storage, state = _stateful_storage({_LEGACY_SNAPSHOT_KEY: _legacy_delay_snapshot()})
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)
    queue = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    queue.clear()
    queue.close()

    assert SnapshotRepository(storage).read(_SNAPSHOT_KEY).state is None
    assert _LEGACY_SNAPSHOT_KEY not in state
    assert storage.delete.call_args_list == [
        call(_LEGACY_SNAPSHOT_KEY),
        call(queue._empty_snapshot_tombstone_key()),
    ]


@pytest.mark.parametrize(
    "clear_before_close",
    [False, True],
    ids=["stateful-checkpoint", "empty-tombstone"],
)
def test_backends_queue_keeps_legacy_snapshot_when_v3_checkpoint_store_fails(
    clear_before_close: bool,
):
    """A failed v3 update leaves the only successful legacy checkpoint intact."""
    legacy_state = _legacy_delay_snapshot()
    storage, state = _stateful_storage({_LEGACY_SNAPSHOT_KEY: legacy_state})
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)
    queue = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    if clear_before_close:
        queue.clear()
    storage.store.side_effect = RuntimeError("v3 store failed")

    with pytest.raises(QueueError, match="snapshot commit"):
        queue.close()

    assert state[_LEGACY_SNAPSHOT_KEY] == legacy_state
    assert _SNAPSHOT_KEY not in state
    assert bool(strategy._holding) is (not clear_before_close)


def test_backends_queue_retries_legacy_cleanup_after_v3_restart():
    """A stale legacy key cannot replay after its first cleanup attempt fails."""
    legacy_state = _legacy_delay_snapshot()
    storage, state = _stateful_storage({_LEGACY_SNAPSHOT_KEY: legacy_state})
    legacy_delete_failed_once = False

    def delete(key: str) -> None:
        nonlocal legacy_delete_failed_once
        if key == _LEGACY_SNAPSHOT_KEY and not legacy_delete_failed_once:
            legacy_delete_failed_once = True
            raise RuntimeError("legacy delete failed")
        state.pop(key, None)

    storage.delete.side_effect = delete
    cm = _wired_cm(storage=storage)

    first = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=_delay(clock_value=100.0),
        monitor=MagicMock(),
    )
    first.close()

    assert _SNAPSHOT_KEY in state
    assert _LEGACY_SNAPSHOT_KEY in state

    second_strategy = _delay(clock_value=100.0)
    second = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=second_strategy,
        monitor=MagicMock(),
    )

    assert len(second_strategy._holding) == 1
    assert storage.retrieve.call_args_list[:4] == [
        call(_SNAPSHOT_KEY),
        call(_SNAPSHOT_TOMBSTONE_KEY),
        call(_LEGACY_SNAPSHOT_KEY),
        call(_SNAPSHOT_KEY),
    ]

    second.clear()
    second.close()

    assert SnapshotRepository(storage).read(_SNAPSHOT_KEY).state is None
    assert _LEGACY_SNAPSHOT_KEY not in state

    third_strategy = _delay(clock_value=100.0)
    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=third_strategy,
        monitor=MagicMock(),
    )

    assert third_strategy._holding == []


def test_empty_checkpoint_tombstone_blocks_legacy_replay_after_retirement_failure():
    """A failed legacy delete after an empty close cannot resurrect old work."""
    storage, state = _stateful_storage({_LEGACY_SNAPSHOT_KEY: _legacy_delay_snapshot()})

    def delete(key: str) -> None:
        if key == _LEGACY_SNAPSHOT_KEY:
            raise RuntimeError("legacy delete failed")
        state.pop(key, None)

    storage.delete.side_effect = delete
    cm = _wired_cm(storage=storage)
    first = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=_delay(clock_value=100.0),
        monitor=MagicMock(),
    )

    first.clear()
    first.close()

    tombstone_key = first._empty_snapshot_tombstone_key()
    assert SnapshotRepository(storage).read(_SNAPSHOT_KEY).state is None
    assert tombstone_key not in state
    assert _LEGACY_SNAPSHOT_KEY in state

    second_strategy = _delay(clock_value=100.0)
    second = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=second_strategy,
        monitor=MagicMock(),
    )

    assert second_strategy._holding == []
    assert call(_SNAPSHOT_KEY) in storage.retrieve.call_args_list
    assert call(_LEGACY_SNAPSHOT_KEY) in storage.retrieve.call_args_list

    storage.delete.side_effect = lambda key: state.pop(key, None)
    second.close()

    assert SnapshotRepository(storage).read(_SNAPSHOT_KEY).state is None
    assert _LEGACY_SNAPSHOT_KEY not in state

    third_strategy = _delay(clock_value=100.0)
    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=third_strategy,
        monitor=MagicMock(),
    )

    assert third_strategy._holding == []


def test_backends_queue_uses_injected_snapshot_manager_only_for_snapshot_io():
    """An external snapshot manager must not replace the queue manager.

    Queue operations continue to use ``connection_manager``; only the two
    snapshot checkpoints resolve storage through the injected manager.  The
    queue does not own either manager's acquire, so this test deliberately
    leaves lifecycle release to the scheduler-level tests.
    """
    snapshot_storage = _storage_mock()
    queue_manager = MagicMock(name="QueueConnectionManager")
    queue_manager.get_storage_backend.side_effect = AssertionError(
        "queue manager must not resolve snapshot storage"
    )
    queue_manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")
    snapshot_manager = _wired_cm(storage=snapshot_storage)
    strategy = _delay(clock_value=100.0)
    strategy.push("q", b"persist-me", delay=10.0)
    expected_snapshot = strategy.snapshot()

    queue = BackendQueue(
        connection_manager=queue_manager,
        snapshot_connection_manager=snapshot_manager,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )
    queue.close()

    queue_manager.get_storage_backend.assert_not_called()
    snapshot_storage.retrieve.assert_has_calls(
        [
            call(_SNAPSHOT_KEY),
            call(_SNAPSHOT_TOMBSTONE_KEY),
            call(_LEGACY_SNAPSHOT_KEY),
        ]
    )
    assert _stored_snapshot_payload(snapshot_storage) == expected_snapshot
    snapshot_manager.close.assert_not_called()


def test_restored_snapshot_survives_crash_until_clean_checkpoint():
    source = _delay(clock_value=100.0)
    source.push("q", b"recovered", delay=10.0)
    blob = source.snapshot()
    assert blob is not None
    storage, state = _stateful_storage({_SNAPSHOT_KEY: blob})
    cm = _wired_cm(storage=storage)

    first_strategy = _delay(clock_value=100.0)
    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=first_strategy,
        monitor=MagicMock(),
    )

    assert len(first_strategy._holding) == 1
    assert state[_SNAPSHOT_KEY] == blob

    # Simulate a hard crash by constructing a new queue without closing the
    # first. The same recovery checkpoint must still be available and replayable.
    second_strategy = _delay(clock_value=100.0)
    second = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=second_strategy,
        monitor=MagicMock(),
    )
    assert len(second_strategy._holding) == 1

    # A later clean checkpoint with no remaining state retires the old snapshot.
    second.clear()
    second.close()
    assert SnapshotRepository(storage).read(_SNAPSHOT_KEY).state is None


def test_backends_queue_close_deletes_stale_snapshot_when_strategy_is_empty():
    """An empty clean close invalidates the current and eligible legacy keys."""
    storage = _storage_mock()
    cm = _wired_cm(storage=storage)
    bq = BackendQueue(connection_manager=cm, queue_name="q", monitor=MagicMock())

    bq.close()

    assert _stored_snapshot_payload(storage) is None
    assert storage.delete.call_args_list == [
        call(_LEGACY_SNAPSHOT_KEY),
        call(bq._empty_snapshot_tombstone_key()),
    ]


def test_backends_queue_storage_incapable_skips_cleanly():
    """A storage-incapable backend (NotImplementedError) skips snapshot, no crash."""
    cm = MagicMock(name="ConnectionManager")
    cm.get_storage_backend.side_effect = NotImplementedError("no storage")
    cm.get_queue_backend.return_value = MagicMock()
    strategy = _delay(clock_value=100.0)
    strategy.push("q", b"x", delay=10.0)

    bq = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )
    with pytest.raises(QueueError, match="requires snapshot storage"):
        bq.close()
    bq.close(lossy=True)


def test_backends_queue_init_skips_when_cm_has_no_storage_attr():
    """A connection manager without ``get_storage_backend`` skips snapshot, no crash.

    Regression: test stubs (e.g. ``_NullConnectionManager``) may not expose the
    storage interface at all. ``getattr(..., None)`` must short-circuit rather
    than ``AttributeError``.
    """

    class _NoStorageCM:
        pass

    strategy = _delay(clock_value=100.0)
    strategy.push("q", b"x", delay=10.0)
    # Init skips restore, but nonempty close requires an explicit lossy abort:
    bq = BackendQueue(
        connection_manager=_NoStorageCM(),  # type: ignore[arg-type]
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )
    with pytest.raises(QueueError, match="requires snapshot storage"):
        bq.close()
    bq.close(lossy=True)


def test_backends_queue_restore_skips_non_bytes_state():
    """A non-bytes retrieve result (e.g. a mock) is skipped, not passed to restore.

    Regression: an auto-mocked connection manager's ``retrieve`` returns a Mock,
    not bytes — ``isinstance(state, (bytes, bytearray))`` must guard before
    ``strategy.restore()`` or ``json.loads`` raises TypeError.
    """
    cm = MagicMock(name="ConnectionManager")
    # retrieve returns a Mock (not None, not bytes) — simulating an auto-mock CM:
    cm.get_storage_backend.return_value.retrieve.return_value = MagicMock(
        name="not-bytes"
    )
    cm.get_queue_backend.return_value = MagicMock()
    strategy = _delay(clock_value=100.0)

    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )
    # No crash, and strategy.restore was never handed the non-bytes value (held heap empty):
    assert strategy._holding == []


def test_backends_queue_init_restore_crash_does_not_break_startup():
    """Regression: a strategy.restore() that raises (buggy third-party strategy,
    or a snapshot from an incompatible version) must NOT crash BackendQueue.__init__.

    _restore_snapshot's docstring promises 'never crashes startup'; pre-fix the
    restore() call was the one operation in the method NOT wrapped in try/except
    (snapshot/get_storage/retrieve/store all were). The bundled strategies
    (delay/time_wheel/ring_buffer) already catch their own decode errors, so the
    live risk is a third-party strategy with an un-hardened restore().
    """
    storage = _storage_mock(retrieve_return=b'{"some":"prior"}')
    cm = _wired_cm(storage=storage)
    strategy = _delay(clock_value=100.0)

    def _raise(_state):
        raise RuntimeError("third-party restore bug")

    strategy.restore = _raise  # type: ignore[assignment]

    # Must NOT raise — the restore() crash is logged + swallowed.
    BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Spider-scoped snapshot key (initiative #16)
# ---------------------------------------------------------------------------


def _make_queue_for_key(spider=None, queue_name="jobs", snapshot_owner=None):
    """Minimal BackendQueue for snapshot-key unit tests.

    ``connection_manager`` is a spec-empty Mock so the queue can be constructed
    without touching a real backend; only ``_snapshot_key()`` is exercised (a
    pure derivation over ``self._spider`` / ``self.queue_name``).
    """
    return BackendQueue(
        connection_manager=MagicMock(spec=[]),
        queue_name=queue_name,
        spider=spider,
        snapshot_owner=snapshot_owner,
    )


def test_snapshot_key_includes_spider_name():
    """Two queues with the same queue_name but different spiders MUST produce
    different snapshot keys (initiative #16: cross-spider snapshot isolation).

    Regression: prior to #16 the key was ``<prefix><queue_name>`` only, so two
    spiders sharing a storage backend (multi-spider in one process, or
    multi-worker with shared Redis/Mongo/ES) overwrote each other's strategy
    snapshot on close — and on restart the survivor restored the wrong spider's
    Delay heap.
    """
    spider_a = SimpleNamespace(name="spiderA")
    spider_b = SimpleNamespace(name="spiderB")

    key_a = _make_queue_for_key(spider=spider_a, queue_name="jobs")._snapshot_key()
    key_b = _make_queue_for_key(spider=spider_b, queue_name="jobs")._snapshot_key()

    assert key_a != key_b
    assert "spiderA" in key_a
    assert "spiderB" in key_b
    assert key_a.endswith(":jobs")
    assert key_b.endswith(":jobs")


def test_default_snapshot_key_distinguishes_colon_delimited_components():
    """Distinct spider/queue pairs must never share a recovery checkpoint."""
    first = _make_queue_for_key(
        spider=SimpleNamespace(name="a:b"),
        queue_name="c",
    )
    second = _make_queue_for_key(
        spider=SimpleNamespace(name="a"),
        queue_name="b:c",
    )

    assert first._snapshot_key() != second._snapshot_key()


def test_snapshot_key_without_spider_uses_v3_identity():
    """A queue without a spider still uses an injective v3 checkpoint key."""
    key = _make_queue_for_key(spider=None, queue_name="jobs")._snapshot_key()
    assert key == "queue:snapshot:v3:0::4:jobs"


def test_empty_snapshot_tombstone_uses_a_separate_namespace():
    """The migration marker must never collide with a legacy snapshot key."""
    queue = _make_queue_for_key(spider=None, queue_name="q")

    assert queue._empty_snapshot_tombstone_key() == _SNAPSHOT_TOMBSTONE_KEY
    assert not queue._empty_snapshot_tombstone_key().startswith("queue:snapshot:")


def test_snapshot_key_spider_without_name_attr_falls_back():
    """A spider-like object without a ``name`` attribute uses an empty v3 part.

    Mirrors the defensive ``getattr`` chaining already used at
    ``queue.py:561`` (``getattr(self._spider, "crawler", None)``).
    """
    key = _make_queue_for_key(
        spider=SimpleNamespace(), queue_name="jobs"
    )._snapshot_key()
    assert key == "queue:snapshot:v3:0::4:jobs"


def test_snapshot_key_isolates_workers_with_stable_owner():
    spider = SimpleNamespace(name="shared-spider")

    key_a = _make_queue_for_key(
        spider=spider,
        snapshot_owner="worker-a",
    )._snapshot_key()
    key_b = _make_queue_for_key(
        spider=spider,
        snapshot_owner="worker-b",
    )._snapshot_key()

    assert key_a == "queue:snapshot:v2:8:worker-a:13:shared-spider:jobs"
    assert key_b == "queue:snapshot:v2:8:worker-b:13:shared-spider:jobs"
    assert key_a != key_b


def test_snapshot_owner_rejects_unsafe_storage_key_characters():
    with pytest.raises(ValueError, match="snapshot_owner"):
        _make_queue_for_key(snapshot_owner="worker with spaces")
