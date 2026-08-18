"""Transactional close regressions for stateful backend queues."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, call

import pytest

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler


class _CustomControlFlow(BaseException):
    pass


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


class _InterruptingWaiterSet(set[object]):
    def __init__(self, operation: str, exception: BaseException) -> None:
        super().__init__()
        self._operation = operation
        self._exception = exception
        self._raised = False

    def add(self, element: object) -> None:
        super().add(element)
        if self._operation == "add" and not self._raised:
            self._raised = True
            raise self._exception

    def discard(self, element: object) -> None:
        super().discard(element)
        if self._operation == "discard" and not self._raised:
            self._raised = True
            raise self._exception


class _InterruptingWaiterMap(dict[int, set[object]]):
    def __init__(self, operation: str, exception: BaseException) -> None:
        super().__init__()
        self._waiters = _InterruptingWaiterSet(operation, exception)
        self.registration_attempted = threading.Event()

    def setdefault(self, key: int, default: set[object] | None = None) -> set[object]:
        self.registration_attempted.set()
        return super().setdefault(key, self._waiters)


@pytest.mark.timeout(10)
@pytest.mark.parametrize(
    ("operation", "interruption"),
    [
        ("add", KeyboardInterrupt("waiter registration interrupted")),
        ("discard", SystemExit("waiter cleanup interrupted")),
    ],
)
def test_interrupted_waiter_registration_and_cleanup_are_reclaimed(
    operation: str, interruption: BaseException
) -> None:
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
    waiter_map = _InterruptingWaiterMap(operation, interruption)
    queue._close_attempt_waiters = waiter_map
    outcomes: dict[str, BaseException | None] = {}

    def close_queue(caller: str) -> None:
        try:
            queue.close()
        except BaseException as exc:
            outcomes[caller] = exc
        else:
            outcomes[caller] = None

    owner = threading.Thread(
        target=close_queue, args=("owner",), name="close-owner", daemon=True
    )
    waiter = threading.Thread(
        target=close_queue, args=("waiter",), name="close-waiter", daemon=True
    )
    owner.start()
    assert store_entered.wait(timeout=2.0)
    waiter.start()
    assert waiter_map.registration_attempted.wait(timeout=2.0)
    if operation == "add":
        waiter.join(timeout=2.0)
        assert not waiter.is_alive()
    release_store.set()
    owner.join(timeout=2.0)
    waiter.join(timeout=2.0)
    assert not owner.is_alive()
    assert not waiter.is_alive()

    assert isinstance(outcomes["owner"], QueueError)
    assert isinstance(outcomes["waiter"], type(interruption))
    assert queue._close_in_progress is False
    assert queue._close_attempt_waiters == {}
    assert queue._close_attempt_outcomes == {}

    storage.store.side_effect = None
    queue.close()
    assert queue._close_in_progress is False
    assert queue._close_attempt_waiters == {}
    assert queue._close_attempt_outcomes == {}
    strategy.close.assert_called_once_with()


@pytest.mark.timeout(10)
def test_interrupted_owner_publication_repairs_waiters_and_allows_retry() -> None:
    waiter_is_waiting = threading.Event()
    publication_is_armed = threading.Event()

    class _InterruptPublicationCondition(threading.Condition):
        def __init__(self) -> None:
            super().__init__()
            self.interrupted = False

        def __enter__(self) -> threading.Condition:
            if publication_is_armed.is_set() and not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt("owner publication interrupted")
            return super().__enter__()

        def wait(self, timeout: float | None = None) -> bool:
            if threading.current_thread().name == "close-waiter":
                waiter_is_waiting.set()
            return super().wait(timeout)

    storage = MagicMock()
    storage.retrieve.return_value = None

    def fail_store(_key: str, _value: bytes) -> None:
        assert waiter_is_waiting.wait(timeout=2.0)
        publication_is_armed.set()
        raise RuntimeError("secret backend failure")

    storage.store.side_effect = fail_store
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)
    queue._operation_gate = _InterruptPublicationCondition()
    outcomes: dict[str, BaseException | None] = {}

    def close_queue(caller: str) -> None:
        try:
            queue.close()
        except BaseException as exc:
            outcomes[caller] = exc
        else:
            outcomes[caller] = None

    owner = threading.Thread(
        target=close_queue, args=("owner",), name="close-owner", daemon=True
    )
    waiter = threading.Thread(
        target=close_queue, args=("waiter",), name="close-waiter", daemon=True
    )
    owner.start()
    waiter.start()
    owner.join(timeout=2.0)
    waiter.join(timeout=2.0)
    assert not owner.is_alive()
    assert not waiter.is_alive()

    assert isinstance(outcomes["owner"], KeyboardInterrupt)
    assert isinstance(outcomes["waiter"], QueueError)
    assert str(outcomes["waiter"]) == "Queue close failed; checkpoint can be retried."
    assert queue._close_in_progress is False
    assert queue._close_attempt_waiters == {}
    assert queue._close_attempt_outcomes == {}

    storage.store.side_effect = None
    queue.close()
    assert queue._close_in_progress is False
    assert queue._close_attempt_waiters == {}
    assert queue._close_attempt_outcomes == {}
    strategy.close.assert_called_once_with()


@pytest.mark.parametrize(
    "control_error",
    [GeneratorExit("generator closing"), _CustomControlFlow("custom control flow")],
)
def test_close_control_error_is_not_replaced_by_publication_failure(
    control_error: BaseException,
) -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.begin_close.side_effect = control_error
    queue = _queue_with_storage(strategy, storage)
    publication_error = RuntimeError("publication failed")
    queue._publish_close_attempt = MagicMock(return_value=publication_error)  # type: ignore[method-assign]

    with pytest.raises(type(control_error)) as exc_info:
        queue.close()

    assert exc_info.value is control_error


def test_initial_close_control_error_is_not_replaced_by_publication_failure() -> None:
    control_error = _CustomControlFlow("condition exit interrupted")

    class _InterruptingExitCondition(threading.Condition):
        def __exit__(self, *args: object) -> None:
            super().__exit__(*args)
            raise control_error

    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    queue = _queue_with_storage(strategy, storage)
    queue._operation_gate = _InterruptingExitCondition()
    publication_error = RuntimeError("publication failed")
    queue._publish_close_attempt = MagicMock(return_value=publication_error)  # type: ignore[method-assign]

    with pytest.raises(_CustomControlFlow) as exc_info:
        queue.close()

    assert exc_info.value is control_error


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

    assert queue.close.call_count == 2
    assert scheduler._queue is None
    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_called_once_with()


def test_scheduler_retries_real_backend_queue_only_after_checkpoint_failure() -> None:
    storage = MagicMock(name="snapshot-storage")
    storage.retrieve.return_value = None
    storage.store.side_effect = RuntimeError("checkpoint unavailable")
    strategy = MagicMock(name="strategy")
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)
    queue_manager = MagicMock(name="queue-manager")
    snapshot_manager = MagicMock(name="snapshot-manager")
    scheduler = BackendScheduler(
        queue_manager,
        snapshot_connection_manager=snapshot_manager,
        owns_snapshot_connection_manager=True,
    )
    scheduler._queue = queue

    with pytest.raises(QueueError, match="snapshot commit"):
        scheduler.close("checkpoint-failed")

    assert queue._checkpoint_complete is False
    assert scheduler._queue is queue
    strategy.close.assert_not_called()
    queue_manager.close.assert_not_called()
    snapshot_manager.close.assert_not_called()

    storage.store.side_effect = None
    scheduler.close("checkpoint-retry")

    assert queue._checkpoint_complete is True
    strategy.close.assert_called_once_with()
    assert scheduler._queue is None
    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_called_once_with()


def test_scheduler_cleans_up_after_real_strategy_close_queue_error() -> None:
    storage = MagicMock(name="snapshot-storage")
    storage.retrieve.return_value = None
    strategy = MagicMock(name="strategy")
    strategy.snapshot.return_value = b"state"
    strategy.close.side_effect = QueueError("strategy cleanup unavailable")
    queue = _queue_with_storage(strategy, storage)
    queue_manager = MagicMock(name="queue-manager")
    snapshot_manager = MagicMock(name="snapshot-manager")
    scheduler = BackendScheduler(
        queue_manager,
        snapshot_connection_manager=snapshot_manager,
        owns_snapshot_connection_manager=True,
    )
    scheduler._queue = queue

    scheduler.close("cleanup-failed")

    assert queue._checkpoint_complete is True
    strategy.close.assert_called_once_with()
    assert scheduler._queue is None
    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_called_once_with()

    # Cleanup failures follow the terminal ordinary policy: neither retry nor
    # abort can revisit a queue whose managers have already been released.
    scheduler.close("duplicate-close")
    scheduler.abort("late-abort")
    strategy.close.assert_called_once_with()
    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_called_once_with()


def test_scheduler_abort_is_an_explicit_lossy_teardown() -> None:
    queue_manager = MagicMock()
    queue = MagicMock()
    scheduler = BackendScheduler(queue_manager)
    scheduler._queue = queue

    scheduler.abort("operator-selected-lossy-abort")

    assert queue.close.call_args_list == [call(lossy=True)]
    queue_manager.close.assert_called_once_with()

    scheduler.abort("duplicate-abort")
    assert queue.close.call_args_list == [call(lossy=True)]
    queue_manager.close.assert_called_once_with()


@pytest.mark.parametrize(
    "queue_failure",
    [RuntimeError("lossy close failed"), KeyboardInterrupt("lossy close interrupted")],
)
def test_scheduler_lossy_abort_failure_still_runs_terminal_teardown(
    queue_failure: BaseException,
) -> None:
    queue_manager = MagicMock(name="queue-manager")
    snapshot_manager = MagicMock(name="snapshot-manager")
    signal_manager = MagicMock(name="signal-manager")
    dupefilter = MagicMock(name="dupefilter")
    queue = MagicMock(name="queue")
    queue.close.side_effect = queue_failure
    scheduler = BackendScheduler(
        queue_manager,
        snapshot_connection_manager=snapshot_manager,
        owns_snapshot_connection_manager=True,
    )
    scheduler._queue = queue
    scheduler._connected_signals = signal_manager
    scheduler._signals_connected = True
    scheduler.dupefilter = dupefilter
    scheduler._owns_dupefilter = True

    if isinstance(queue_failure, Exception):
        scheduler.abort("lossy-abort")
    else:
        with pytest.raises(type(queue_failure)) as exc_info:
            scheduler.abort("lossy-abort")
        assert exc_info.value is queue_failure

    queue.close.assert_called_once_with(lossy=True)
    assert signal_manager.disconnect.call_count == 2
    dupefilter.close.assert_called_once_with("lossy-abort")
    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_called_once_with()
    assert scheduler._queue is None
    assert scheduler._connected_signals is None
    assert scheduler._signals_connected is False
