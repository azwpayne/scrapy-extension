"""Transactional close regressions for stateful backend queues."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler


def _queue_with_storage(strategy: MagicMock, storage: MagicMock) -> BackendQueue:
    manager = MagicMock(name="queue-manager")
    manager.get_storage_backend.return_value = storage
    manager.get_queue_backend.return_value = MagicMock()
    return BackendQueue(
        manager,
        "q",
        queue_strategy=strategy,
        snapshot_max_bytes=64,
        snapshot_chunk_bytes=4,
    )


def test_checkpoint_failure_is_retryable_and_never_runs_destructive_close() -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    storage.store.side_effect = RuntimeError("secret backend failure")
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)

    with pytest.raises(QueueError, match="snapshot commit") as exc_info:
        queue.close()

    assert exc_info.value.__cause__ is None
    strategy.close.assert_not_called()
    storage.store.side_effect = None

    queue.close()

    strategy.begin_close.assert_called_once_with()
    strategy.close.assert_called_once_with()


def test_concurrent_close_callers_observe_the_same_failed_attempt() -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    store_entered = threading.Event()
    release_store = threading.Event()

    def fail_store(_key: str, _value: bytes) -> None:
        store_entered.set()
        assert release_store.wait(timeout=2.0)
        raise RuntimeError("secret backend failure")

    storage.store.side_effect = fail_store
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)
    errors: list[BaseException] = []

    def close_queue() -> None:
        try:
            queue.close()
        except BaseException as exc:  # capture thread outcome for assertion
            errors.append(exc)

    first = threading.Thread(target=close_queue, daemon=True)
    second = threading.Thread(target=close_queue, daemon=True)
    first.start()
    assert store_entered.wait(timeout=2.0)
    second.start()
    release_store.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert len(errors) == 2
    assert all(isinstance(error, QueueError) for error in errors)
    strategy.close.assert_not_called()

    storage.store.side_effect = None
    queue.close()
    strategy.close.assert_called_once_with()


@pytest.mark.timeout(10)
def test_failed_close_waiter_is_not_overtaken_by_successful_retry() -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    first_store_entered = threading.Event()
    release_first_store = threading.Event()
    waiter_is_waiting = threading.Event()
    retry_may_start = threading.Barrier(2)
    retry_completed = threading.Barrier(2)

    class _DelayedWaiterCondition(threading.Condition):
        """Let caller C complete its retry before caller B resumes from wait."""

        def wait(self, timeout: float | None = None) -> bool:
            if threading.current_thread().name == "close-waiter-b":
                waiter_is_waiting.set()
            notified = super().wait(timeout)
            if threading.current_thread().name == "close-waiter-b":
                self.release()
                try:
                    retry_may_start.wait(timeout=2.0)
                    retry_completed.wait(timeout=2.0)
                finally:
                    self.acquire()
            return notified

    store_calls = 0

    def store(_key: str, _value: bytes) -> None:
        nonlocal store_calls
        store_calls += 1
        if store_calls == 1:
            first_store_entered.set()
            assert release_first_store.wait(timeout=2.0)
            raise RuntimeError("secret backend failure")

    storage.store.side_effect = store
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)
    queue._operation_gate = _DelayedWaiterCondition()
    outcomes: dict[str, BaseException | None] = {}

    def close_queue(caller: str) -> None:
        try:
            if caller == "c":
                retry_may_start.wait(timeout=2.0)
            queue.close()
        except BaseException as exc:  # capture each thread outcome for assertion
            outcomes[caller] = exc
        else:
            outcomes[caller] = None
        finally:
            if caller == "c":
                retry_completed.wait(timeout=2.0)

    caller_a = threading.Thread(
        target=close_queue, args=("a",), name="close-owner-a", daemon=True
    )
    caller_b = threading.Thread(
        target=close_queue, args=("b",), name="close-waiter-b", daemon=True
    )
    caller_c = threading.Thread(
        target=close_queue, args=("c",), name="close-retry-c", daemon=True
    )
    caller_a.start()
    assert first_store_entered.wait(timeout=2.0)
    caller_b.start()
    assert waiter_is_waiting.wait(timeout=2.0)
    caller_c.start()
    release_first_store.set()

    for caller in (caller_a, caller_b, caller_c):
        caller.join(timeout=2.0)
        assert not caller.is_alive()

    assert isinstance(outcomes["a"], QueueError)
    assert isinstance(outcomes["b"], QueueError)
    assert outcomes["c"] is None
    assert store_calls == 4  # failed generation chunk, then successful retry
    strategy.close.assert_called_once_with()


def test_nonempty_state_without_storage_requires_explicit_lossy_abort() -> None:
    manager = MagicMock()
    manager.get_storage_backend.side_effect = NotImplementedError("queue only")
    strategy = MagicMock()
    strategy.snapshot.return_value = b"held-state"
    queue = BackendQueue(manager, "q", queue_strategy=strategy)

    with pytest.raises(QueueError, match="requires snapshot storage"):
        queue.close()
    strategy.close.assert_not_called()

    queue.close(lossy=True)
    strategy.close.assert_called_once_with()


def test_oversize_checkpoint_can_be_retried_after_state_is_reduced() -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.snapshot.return_value = b"12345"
    queue = BackendQueue(
        MagicMock(get_storage_backend=MagicMock(return_value=storage)),
        "q",
        queue_strategy=strategy,
        snapshot_max_bytes=4,
        snapshot_chunk_bytes=2,
    )

    with pytest.raises(QueueError, match="snapshot commit"):
        queue.close()
    storage.store.assert_not_called()
    strategy.close.assert_not_called()

    strategy.snapshot.return_value = b"1234"
    queue.close()
    strategy.close.assert_called_once_with()


def test_scheduler_retains_queue_and_managers_until_checkpoint_retry_succeeds() -> None:
    queue_manager = MagicMock(name="queue-manager")
    snapshot_manager = MagicMock(name="snapshot-manager")
    queue = MagicMock(name="queue")
    queue.close.side_effect = [QueueError("checkpoint unavailable"), None]
    scheduler = BackendScheduler(
        queue_manager,
        snapshot_connection_manager=snapshot_manager,
        owns_snapshot_connection_manager=True,
    )
    scheduler._queue = queue

    with pytest.raises(QueueError, match="checkpoint unavailable"):
        scheduler.close("first-attempt")

    assert scheduler._queue is queue
    queue_manager.close.assert_not_called()
    snapshot_manager.close.assert_not_called()

    scheduler.close("retry")

    assert scheduler._queue is None
    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_called_once_with()


def test_scheduler_abort_is_an_explicit_lossy_teardown() -> None:
    queue_manager = MagicMock()
    queue = MagicMock()
    scheduler = BackendScheduler(queue_manager)
    scheduler._queue = queue

    scheduler.abort("operator-selected-lossy-abort")

    queue.close.assert_any_call(lossy=True)
    queue_manager.close.assert_called_once_with()
