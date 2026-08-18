"""Transactional close regressions for stateful backend queues."""

from __future__ import annotations

import dis
import sys
import threading
from unittest.mock import MagicMock, call

import pytest

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler


class _CustomControlFlow(BaseException):
    pass


def _instruction_after_in(function: object, opname: str, argval: object) -> int:
    instructions = list(dis.get_instructions(function))
    for index in range(len(instructions) - 2, -1, -1):
        instruction = instructions[index]
        if instruction.opname == opname and instruction.argval == argval:
            return instructions[index + 1].offset
    raise AssertionError(f"Missing {opname} {argval!r} in {function!r}")


def _instruction_after(opname: str, argval: object) -> int:
    return _instruction_after_in(BackendQueue.close, opname, argval)


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


class _PublicationBoundaryInterruption(BaseException):
    pass


class _MutationBoundaryQueue(BackendQueue):
    """Inject once immediately after a publication-state mutation."""

    _TRACKED_ATTRIBUTES = {
        "_close_complete": "close-complete",
        "_close_in_progress": "close-in-progress",
        "_close_owner_token": "owner-token",
    }

    def __init__(self, *args: object, **kwargs: object) -> None:
        object.__setattr__(self, "_boundary_tracking", False)
        object.__setattr__(self, "_boundary_target", None)
        object.__setattr__(self, "_boundary_interruption", None)
        object.__setattr__(self, "_boundary_raised", False)
        object.__setattr__(self, "_boundary_observed", [])
        super().__init__(*args, **kwargs)

    def __setattr__(self, name: str, value: object) -> None:
        object.__setattr__(self, name, value)
        label = self._TRACKED_ATTRIBUTES.get(name)
        if label is not None and getattr(self, "_boundary_tracking", False):
            self._after_mutation(label)

    def _after_mutation(self, label: str) -> None:
        self._boundary_observed.append(label)
        if self._boundary_target == label and not self._boundary_raised:
            object.__setattr__(self, "_boundary_raised", True)
            interruption = self._boundary_interruption
            assert interruption is not None
            raise interruption

    def arm_boundary(self, label: str, interruption: BaseException) -> None:
        object.__setattr__(self, "_boundary_target", label)
        object.__setattr__(self, "_boundary_interruption", interruption)
        object.__setattr__(self, "_boundary_raised", False)
        self._boundary_observed.clear()
        object.__setattr__(self, "_boundary_tracking", True)


class _MutationBoundaryMap(dict[int, object]):
    def __init__(
        self,
        queue: _MutationBoundaryQueue,
        *,
        set_label: str,
        pop_label: str,
        initial: dict[int, object],
    ) -> None:
        super().__init__(initial)
        self._queue = queue
        self._set_label = set_label
        self._pop_label = pop_label

    def __setitem__(self, key: int, value: object) -> None:
        super().__setitem__(key, value)
        self._queue._after_mutation(self._set_label)

    def pop(self, key: int, default: object = None) -> object:
        value = super().pop(key, default)
        self._queue._after_mutation(self._pop_label)
        return value


class _MutationBoundaryCondition(threading.Condition):
    def __init__(self, queue: _MutationBoundaryQueue) -> None:
        super().__init__()
        self._queue = queue

    def notify_all(self) -> None:
        super().notify_all()
        self._queue._after_mutation("notify-all")


_PUBLICATION_MODELS = {
    (True, True): [
        "close-complete",
        "outcome-set",
        "notify-all",
        "close-in-progress",
        "owner-token",
    ],
    (True, False): [
        "close-complete",
        "waiters-pop",
        "outcomes-pop",
        "notify-all",
        "close-in-progress",
        "owner-token",
    ],
    (False, True): [
        "outcome-set",
        "notify-all",
        "close-in-progress",
        "owner-token",
    ],
    (False, False): [
        "waiters-pop",
        "outcomes-pop",
        "notify-all",
        "close-in-progress",
        "owner-token",
    ],
}


@pytest.mark.parametrize(
    ("succeeded", "has_waiter", "boundary"),
    [
        (succeeded, has_waiter, boundary)
        for (succeeded, has_waiter), boundaries in _PUBLICATION_MODELS.items()
        for boundary in boundaries
    ],
)
def test_every_close_publication_mutation_boundary_repairs_to_terminal_state(
    succeeded: bool, has_waiter: bool, boundary: str
) -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    queue = _MutationBoundaryQueue(
        MagicMock(
            get_storage_backend=MagicMock(return_value=storage),
            get_queue_backend=MagicMock(return_value=MagicMock()),
        ),
        "q",
        queue_strategy=strategy,
    )
    attempt = 1
    owner_token = object()
    waiter_token = object()
    queue._close_owner_token = owner_token
    queue._close_in_progress = True
    queue._close_complete = False
    waiter_values: dict[int, object] = {
        attempt: {waiter_token} if has_waiter else set()
    }
    outcome_values: dict[int, object] = {attempt: not succeeded}
    queue._close_attempt_waiters = _MutationBoundaryMap(
        queue,
        set_label="waiters-set",
        pop_label="waiters-pop",
        initial=waiter_values,
    )  # type: ignore[assignment]
    queue._close_attempt_outcomes = _MutationBoundaryMap(
        queue,
        set_label="outcome-set",
        pop_label="outcomes-pop",
        initial=outcome_values,
    )  # type: ignore[assignment]
    queue._operation_gate = _MutationBoundaryCondition(queue)
    interruption = _PublicationBoundaryInterruption(boundary)
    queue.arm_boundary(boundary, interruption)

    publication_failure = queue._publish_close_attempt(
        attempt, owner_token, succeeded=succeeded
    )

    assert publication_failure is interruption
    expected_order = _PUBLICATION_MODELS[succeeded, has_waiter]
    assert list(dict.fromkeys(queue._boundary_observed)) == expected_order
    assert queue._close_complete is succeeded
    assert queue._close_in_progress is False
    assert queue._close_owner_token is None
    if has_waiter:
        assert queue._close_attempt_outcomes == {attempt: succeeded}
        with queue._operation_gate:
            assert queue._cleanup_close_waiter_locked(attempt, waiter_token) is None
    assert queue._close_attempt_waiters == {}
    assert queue._close_attempt_outcomes == {}


@pytest.mark.timeout(10)
def test_opcode_interruption_at_former_owner_clear_boundary_is_terminal() -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)
    interruption = _PublicationBoundaryInterruption(
        "interrupted immediately after owner-token clearing"
    )
    target_offset = _instruction_after_in(
        BackendQueue._publish_close_attempt, "STORE_ATTR", "_close_owner_token"
    )

    def inject(frame: object, event: str, _arg: object) -> object:
        if (
            getattr(frame, "f_code", None)
            is BackendQueue._publish_close_attempt.__code__
        ):
            frame.f_trace_opcodes = True  # type: ignore[attr-defined]
            if event == "opcode" and frame.f_lasti == target_offset:  # type: ignore[attr-defined]
                raise interruption
        return inject

    sys.settrace(inject)
    try:
        with pytest.raises(_PublicationBoundaryInterruption) as exc_info:
            queue.close()
    finally:
        sys.settrace(None)

    assert exc_info.value is interruption
    assert queue._close_complete is True
    assert queue._close_in_progress is False
    assert queue._close_owner_token is None
    assert queue._close_attempt_waiters == {}
    assert queue._close_attempt_outcomes == {}

    queue.close()

    strategy.close.assert_called_once_with()
    assert queue._close_owner_token is None


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


@pytest.mark.timeout(10)
@pytest.mark.parametrize(
    ("opname", "argval", "fail_snapshot"),
    [
        ("STORE_ATTR", "_close_owner_token", False),
        ("STORE_FAST", "failure", True),
    ],
    ids=["after-owner-assignment", "after-failure-assignment"],
)
def test_trace_interruption_after_ownership_always_publishes_and_allows_retry(
    opname: str, argval: str, fail_snapshot: bool
) -> None:
    snapshot_entered = threading.Event()
    release_snapshot = threading.Event()
    waiter_is_waiting = threading.Event()

    class _ObservedWaitCondition(threading.Condition):
        def wait(self, timeout: float | None = None) -> bool:
            if threading.current_thread().name == "close-trace-waiter":
                waiter_is_waiting.set()
            return super().wait(timeout)

    def snapshot() -> bytes:
        if fail_snapshot:
            snapshot_entered.set()
            assert release_snapshot.wait(timeout=2.0)
            raise RuntimeError("ordinary snapshot failure")
        return b"state"

    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.snapshot.side_effect = snapshot
    queue = _queue_with_storage(strategy, storage)
    queue._operation_gate = _ObservedWaitCondition()
    interruption = _CustomControlFlow("trace boundary interruption")
    target_offset = _instruction_after(opname, argval)
    outcomes: dict[str, BaseException | None] = {}

    def close_owner() -> None:
        def inject(frame: object, event: str, _arg: object) -> object:
            if getattr(frame, "f_code", None) is BackendQueue.close.__code__:
                frame.f_trace_opcodes = True  # type: ignore[attr-defined]
                if event == "opcode" and frame.f_lasti == target_offset:  # type: ignore[attr-defined]
                    raise interruption
            return inject

        sys.settrace(inject)
        try:
            queue.close()
        except BaseException as exc:
            outcomes["owner"] = exc
        else:
            outcomes["owner"] = None
        finally:
            sys.settrace(None)

    def close_waiter() -> None:
        try:
            queue.close()
        except BaseException as exc:
            outcomes["waiter"] = exc
        else:
            outcomes["waiter"] = None

    owner = threading.Thread(target=close_owner, name="close-trace-owner", daemon=True)
    owner.start()
    waiter: threading.Thread | None = None
    if fail_snapshot:
        assert snapshot_entered.wait(timeout=2.0)
        waiter = threading.Thread(
            target=close_waiter, name="close-trace-waiter", daemon=True
        )
        waiter.start()
        assert waiter_is_waiting.wait(timeout=2.0)
        release_snapshot.set()

    owner.join(timeout=2.0)
    assert not owner.is_alive()
    if waiter is not None:
        waiter.join(timeout=2.0)
        assert not waiter.is_alive()
        assert isinstance(outcomes["waiter"], QueueError)

    assert outcomes["owner"] is interruption
    assert queue._close_in_progress is False
    assert queue._close_owner_token is None
    assert queue._close_attempt_waiters == {}
    assert queue._close_attempt_outcomes == {}

    strategy.snapshot.side_effect = None
    strategy.snapshot.return_value = b"state"
    queue.close()

    assert queue._close_complete is True
    assert queue._close_in_progress is False
    assert queue._close_owner_token is None
    assert queue._close_attempt_waiters == {}
    assert queue._close_attempt_outcomes == {}


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
