"""Transactional close regressions for stateful backend queues."""

from __future__ import annotations

import dis
import sys
import threading
from unittest.mock import MagicMock, call

import pytest

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import (
    _STRATEGY_CLEANUP_FAILED,
    _STRATEGY_CLEANUP_INDETERMINATE,
    _STRATEGY_CLEANUP_NOT_STARTED,
    BackendQueue,
)
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


def _instruction_after_strategy_close_call() -> int:
    instructions = list(dis.get_instructions(BackendQueue.close))
    for index, instruction in enumerate(instructions):
        if (
            instruction.opname in {"LOAD_METHOD", "LOAD_ATTR"}
            and instruction.argval == "close"
        ):
            if not any(
                candidate.opname == "LOAD_ATTR" and candidate.argval == "_strategy"
                for candidate in instructions[max(0, index - 3) : index]
            ):
                continue
            for call_index in range(index + 1, len(instructions) - 1):
                if instructions[call_index].opname.startswith("CALL"):
                    return instructions[call_index + 1].offset
    raise AssertionError("Missing strategy.close() call in BackendQueue.close")


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


class _PublicationBoundaryInterruption(BaseException):
    pass


@pytest.mark.timeout(10)
def test_interrupted_close_publication_is_bounded_and_retryable() -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)
    original = queue._publish_close_attempt
    interruption = _PublicationBoundaryInterruption("publish")
    calls = 0

    def interrupt_once(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise interruption
        original(*args, **kwargs)  # type: ignore[arg-type]

    queue._publish_close_attempt = interrupt_once  # type: ignore[method-assign]

    with pytest.raises(_PublicationBoundaryInterruption) as exc_info:
        queue.close()

    assert exc_info.value is interruption
    assert queue._close_in_progress is True
    queue.close()
    assert queue._close_complete is True
    assert queue._close_in_progress is False
    assert queue._close_owner_token is None
    strategy.close.assert_called_once_with()


@pytest.mark.timeout(10)
def test_interrupted_cleanup_publication_is_terminal_without_replay() -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)
    original = queue._publish_strategy_cleanup_outcome
    interruption = _PublicationBoundaryInterruption("cleanup publication")
    calls = 0

    def interrupt_once(outcome: str) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise interruption
        original(outcome)

    queue._publish_strategy_cleanup_outcome = interrupt_once  # type: ignore[method-assign]

    with pytest.raises(_PublicationBoundaryInterruption):
        queue.close()

    queue.close()
    assert queue._close_complete is True
    strategy.close.assert_called_once_with()


def test_close_finalization_has_no_unbounded_retry_loop() -> None:
    instructions = list(dis.get_instructions(BackendQueue._repair_close_finalization))
    assert all(instruction.opname != "JUMP_BACKWARD" for instruction in instructions)


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


@pytest.mark.parametrize(
    ("cleanup_error", "expected_state"),
    [
        (RuntimeError("ordinary cleanup failure"), _STRATEGY_CLEANUP_FAILED),
        (
            _CustomControlFlow("cleanup control interruption"),
            _STRATEGY_CLEANUP_INDETERMINATE,
        ),
    ],
)
def test_cleanup_error_is_terminal_for_direct_and_lossy_retry(
    cleanup_error: BaseException, expected_state: str
) -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    strategy.close.side_effect = cleanup_error
    queue = _queue_with_storage(strategy, storage)

    with pytest.raises(type(cleanup_error)) as exc_info:
        queue.close()

    assert exc_info.value is cleanup_error
    assert queue._strategy_cleanup_state == expected_state
    assert queue._checkpoint_complete is True
    assert queue._close_complete is True

    queue.close()
    queue.close(lossy=True)
    strategy.close.assert_called_once_with()


@pytest.mark.timeout(10)
def test_opcode_interruption_immediately_after_cleanup_return_is_indeterminate() -> (
    None
):
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    queue = _queue_with_storage(strategy, storage)
    interruption = _CustomControlFlow("interrupted after cleanup returned")
    target_offset = _instruction_after_strategy_close_call()

    def inject(frame: object, event: str, _arg: object) -> object:
        if getattr(frame, "f_code", None) is BackendQueue.close.__code__:
            frame.f_trace_opcodes = True  # type: ignore[attr-defined]
            if event == "opcode" and frame.f_lasti == target_offset:  # type: ignore[attr-defined]
                raise interruption
        return inject

    sys.settrace(inject)
    try:
        with pytest.raises(_CustomControlFlow) as exc_info:
            queue.close()
    finally:
        sys.settrace(None)

    assert exc_info.value is interruption
    assert queue._strategy_cleanup_state == _STRATEGY_CLEANUP_INDETERMINATE
    assert queue._close_complete is True

    queue.close()
    queue.close(lossy=True)
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


@pytest.mark.parametrize("terminal_action", ["close", "abort"])
def test_scheduler_retains_real_queue_interrupted_after_checkpoint(
    terminal_action: str,
) -> None:
    storage = MagicMock(name="snapshot-storage")
    storage.retrieve.return_value = None
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
    interruption = _CustomControlFlow("interrupted after checkpoint commit")
    target_offset = _instruction_after("STORE_ATTR", "_checkpoint_complete")

    def inject(frame: object, event: str, _arg: object) -> object:
        if getattr(frame, "f_code", None) is BackendQueue.close.__code__:
            frame.f_trace_opcodes = True  # type: ignore[attr-defined]
            if event == "opcode" and frame.f_lasti == target_offset:  # type: ignore[attr-defined]
                raise interruption
        return inject

    sys.settrace(inject)
    try:
        with pytest.raises(_CustomControlFlow) as exc_info:
            scheduler.close("checkpoint-committed")
    finally:
        sys.settrace(None)

    assert exc_info.value is interruption
    assert queue._checkpoint_complete is True
    assert queue._strategy_cleanup_state == _STRATEGY_CLEANUP_NOT_STARTED
    assert queue._close_complete is False
    assert scheduler._queue is queue
    strategy.close.assert_not_called()
    queue_manager.close.assert_not_called()
    snapshot_manager.close.assert_not_called()

    if terminal_action == "close":
        scheduler.close("retry-close")
    else:
        scheduler.abort("explicit-abort")

    assert queue._close_complete is True
    strategy.close.assert_called_once_with()
    assert scheduler._queue is None
    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_called_once_with()

    scheduler.close("duplicate-close")
    scheduler.abort("duplicate-abort")
    strategy.close.assert_called_once_with()
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


def test_scheduler_abort_does_not_reenter_interrupted_real_queue_cleanup() -> None:
    storage = MagicMock()
    storage.retrieve.return_value = None
    strategy = MagicMock()
    strategy.snapshot.return_value = b"state"
    interruption = _CustomControlFlow("lossy cleanup interrupted")
    strategy.close.side_effect = interruption
    queue = _queue_with_storage(strategy, storage)
    queue_manager = MagicMock(name="queue-manager")
    snapshot_manager = MagicMock(name="snapshot-manager")
    scheduler = BackendScheduler(
        queue_manager,
        snapshot_connection_manager=snapshot_manager,
        owns_snapshot_connection_manager=True,
    )
    scheduler._queue = queue

    with pytest.raises(_CustomControlFlow) as exc_info:
        scheduler.abort("first-lossy-abort")

    assert exc_info.value is interruption
    strategy.close.assert_called_once_with()
    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_called_once_with()

    scheduler.abort("duplicate-lossy-abort")
    strategy.close.assert_called_once_with()


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
