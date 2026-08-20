"""Unit coverage for bounded Deferred adapters and lifecycle offloading."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from scrapy import Field, Item
from scrapy.settings import Settings
from twisted.internet.defer import Deferred, fail, succeed
from twisted.python.failure import Failure

import scrapy_extension.pipeline.pipeline as pipeline_module
import scrapy_extension.schedule.scheduler as scheduler_module
import scrapy_extension.utils.reactor as reactor_module
from scrapy_extension.exceptions import BackendError, QueueError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.schedule.scheduler import BackendScheduler


class _Item(Item):
    value = Field()


class _FakeDelayedCall:
    def __init__(self, callback):
        self.callback = callback
        self.cancelled = False

    def active(self) -> bool:
        return not self.cancelled

    def cancel(self) -> None:
        self.cancelled = True

    def fire(self) -> None:
        if self.active():
            self.callback()


class _FakeReactor:
    def __init__(self) -> None:
        self.calls: list[_FakeDelayedCall] = []

    def callLater(self, _delay: float, callback):
        call = _FakeDelayedCall(callback)
        self.calls.append(call)
        return call


def test_ordered_deferred_success_failure_and_timeout(monkeypatch) -> None:
    fake_reactor = _FakeReactor()
    workers: list[Deferred[object]] = []
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(
        reactor_module,
        "deferToThread",
        lambda _function, *_args, **_kwargs: workers[-1],
    )

    worker: Deferred[object] = Deferred()
    workers.append(worker)
    operation, bounded = reactor_module.defer_to_thread_ordered(
        lambda: "ok",
        timeout=1.0,
        operation="test-success",
    )
    results: list[object] = []
    bounded.addCallback(results.append)
    worker.callback("ok")
    assert results == ["ok"]
    assert operation.called
    assert fake_reactor.calls[-1].cancelled

    worker = Deferred()
    workers.append(worker)
    _operation, bounded = reactor_module.defer_to_thread_ordered(
        lambda: None,
        timeout=1.0,
        operation="test-failure",
    )
    failures: list[BaseException] = []
    bounded.addErrback(lambda failure: failures.append(failure.value))
    worker.errback(RuntimeError("backend"))
    assert isinstance(failures[0], RuntimeError)

    worker = Deferred()
    workers.append(worker)
    _operation, bounded = reactor_module.defer_to_thread_ordered(
        lambda: None,
        timeout=1.0,
        operation="test-timeout",
    )
    timeout_errors: list[BaseException] = []
    bounded.addErrback(lambda failure: timeout_errors.append(failure.value))
    fake_reactor.calls[-1].fire()
    assert timeout_errors[0].operation == "test-timeout"
    worker.callback("late")
    worker = Deferred()
    workers.append(worker)
    _operation, bounded = reactor_module.defer_to_thread_ordered(
        lambda: None,
        timeout=1.0,
        operation="test-late-failure",
    )
    fake_reactor.calls[-1].fire()
    worker.errback(RuntimeError("late failure"))
    assert bounded.called

    worker = Deferred()
    workers.append(worker)
    _operation, bounded = reactor_module.defer_to_thread_ordered(
        lambda: None,
        timeout=1.0,
        operation="test-cancel-race",
    )
    fake_reactor.calls[-1].cancelled = True
    worker.callback("ok")
    assert bounded.called

    worker = Deferred()
    workers.append(worker)
    _operation, bounded = reactor_module.defer_to_thread_ordered(
        lambda: None,
        timeout=1.0,
        operation="test-expire-after-settle",
    )
    worker.callback("ok")
    fake_reactor.calls[-1].callback()
    assert bounded.called


def test_bounded_deferred_success_failure_and_timeout(monkeypatch) -> None:
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    assert reactor_module.reactor_is_running()

    success_source: Deferred[object] = Deferred()
    success_bounded = reactor_module.bounded_deferred(
        success_source,
        timeout=1.0,
        operation="bounded-success",
    )
    values: list[object] = []
    success_bounded.addCallback(values.append)
    fake_reactor.calls[-1].cancelled = True
    success_source.callback("ok")
    assert values == ["ok"]
    fake_reactor.calls[-1].callback()

    source = Deferred()
    bounded = reactor_module.bounded_deferred(
        source,
        timeout=1.0,
        operation="bounded-late-success",
    )
    fake_reactor.calls[-1].fire()
    source.callback("late")

    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)

    source: Deferred[object] = Deferred()
    bounded = reactor_module.bounded_deferred(
        source,
        timeout=1.0,
        operation="bounded-failure",
    )
    failures: list[BaseException] = []
    bounded.addErrback(lambda failure: failures.append(failure.value))
    source.errback(ValueError("failed"))
    assert isinstance(failures[0], ValueError)
    fake_reactor.calls[-1].callback()

    source = Deferred()
    bounded = reactor_module.bounded_deferred(
        source,
        timeout=1.0,
        operation="bounded-timeout",
    )
    timeout_errors: list[BaseException] = []
    bounded.addErrback(lambda failure: timeout_errors.append(failure.value))
    fake_reactor.calls[-1].fire()
    assert timeout_errors[0].operation == "bounded-timeout"
    source.errback(RuntimeError("late failure"))

    worker: Deferred[object] = Deferred()
    monkeypatch.setattr(reactor_module, "deferToThread", lambda *_a, **_k: worker)
    bounded = reactor_module.defer_to_thread_bounded(
        lambda: "ok",
        timeout=1.0,
        operation="bounded-wrapper",
    )
    worker.callback("ok")
    assert bounded.called


def test_scheduler_warms_queue_and_snapshot_managers() -> None:
    queue_manager = MagicMock()
    snapshot_manager = MagicMock()
    scheduler = BackendScheduler(
        queue_manager,
        snapshot_connection_manager=snapshot_manager,
    )

    scheduler._warm_connections()

    queue_manager.get_queue_backend.assert_called_once_with()
    snapshot_manager.get_storage_backend.assert_called_once_with()


def test_signal_handlers_return_none_when_async_queue_is_unavailable(
    monkeypatch,
) -> None:
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    scheduler = BackendScheduler(MagicMock())
    scheduler._queue = None
    request = SimpleNamespace(meta={scheduler_module.BACKEND_ACK_TOKEN_META_KEY: "tok"})
    response = SimpleNamespace(request=request)

    assert scheduler._on_response_received(None, request, None) is None
    assert scheduler._on_spider_error(None, response, None) is None


def test_scheduler_close_exception_releases_attempt_flag(monkeypatch) -> None:
    scheduler = BackendScheduler(MagicMock())
    scheduler._close_retain_authoritative_failure = True

    def fail_close(_reason, *, lossy=False):
        del lossy
        raise RuntimeError("close setup failed")

    monkeypatch.setattr(scheduler, "_close_locked", fail_close)

    with pytest.raises(RuntimeError, match="close setup failed"):
        scheduler.close("failed")

    assert scheduler._close_retain_authoritative_failure is False
    assert scheduler._close_attempt_owner is None


@pytest.mark.parametrize("callable_descriptor", [False, True])
def test_scheduler_factory_resolves_injected_manager_descriptor(
    mocker,
    callable_descriptor: bool,
) -> None:
    manager = mocker.MagicMock()
    if callable_descriptor:
        manager._backend_type_for_operations = lambda: None
        manager.backend_type = "redis"
    else:
        manager._backend_type_for_operations = "redis"
    scheduler = BackendScheduler.from_settings(
        Settings(),
        connection_manager=manager,
    )

    assert scheduler.connection_manager is manager


def test_scheduler_factory_accepts_injected_snapshot_manager(mocker) -> None:
    manager = mocker.MagicMock()
    manager._backend_type_for_operations = lambda: "redis"
    snapshot_manager = mocker.MagicMock()
    lease = mocker.MagicMock()

    scheduler = BackendScheduler.from_settings(
        Settings(),
        connection_manager=manager,
        snapshot_connection_manager=snapshot_manager,
        snapshot_connection_manager_lease=lease,
    )

    assert scheduler._snapshot_connection_manager is snapshot_manager
    assert scheduler._snapshot_connection_manager_lease is lease


def test_signal_ack_handlers_return_pending_deferred_and_settle_failure(
    monkeypatch,
) -> None:
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    workers: list[Deferred[object]] = []
    operations: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    def fake_thread(function, *args, **kwargs):
        operations.append((function, args, kwargs))
        return workers[-1]

    monkeypatch.setattr(reactor_module, "deferToThread", fake_thread)

    scheduler = BackendScheduler(MagicMock())
    queue = MagicMock()
    scheduler._queue = queue
    request = SimpleNamespace(meta={scheduler_module.BACKEND_ACK_TOKEN_META_KEY: "ack"})

    ack_worker: Deferred[object] = Deferred()
    workers.append(ack_worker)
    ack_result = scheduler._on_response_received(None, request, None)
    assert isinstance(ack_result, Deferred)
    assert not ack_result.called
    function, args, kwargs = operations[0]
    function(*args, **kwargs)
    ack_worker.callback(None)
    assert ack_result.called
    queue.ack.assert_called_once_with(token="ack")
    assert request.meta == {}

    nack_worker: Deferred[object] = Deferred()
    workers.append(nack_worker)
    queue.nack.side_effect = QueueError("nack failed")
    failed_request = SimpleNamespace(
        meta={scheduler_module.BACKEND_ACK_TOKEN_META_KEY: "nack"}
    )
    failed_response = SimpleNamespace(request=failed_request)
    nack_result = scheduler._on_spider_error(None, failed_response, None)
    assert isinstance(nack_result, Deferred)
    assert not nack_result.called
    function, args, kwargs = operations[1]
    with pytest.raises(QueueError):
        function(*args, **kwargs)
    nack_worker.errback(QueueError("worker failed"))
    assert nack_result.called
    assert failed_request.meta == {scheduler_module.BACKEND_ACK_TOKEN_META_KEY: "nack"}


def test_scheduler_close_accepts_direct_authoritative_deferred(monkeypatch) -> None:
    scheduler = BackendScheduler(MagicMock())
    pending: Deferred[None] = Deferred()
    monkeypatch.setattr(
        scheduler,
        "_close_locked",
        lambda _reason, *, lossy=False: pending,
    )

    result = scheduler.close("direct-deferred")

    assert result is pending
    assert scheduler._close_attempt_owner is not None
    pending.callback(None)
    assert scheduler._close_attempt_owner is None


def test_scheduler_close_completion_ignores_replaced_attempt_owner(monkeypatch) -> None:
    scheduler = BackendScheduler(MagicMock())
    pending: Deferred[None] = Deferred()
    monkeypatch.setattr(
        scheduler,
        "_close_locked",
        lambda _reason, *, lossy=False: pending,
    )

    scheduler.close("replaced-owner")
    scheduler._close_attempt_owner = None
    scheduler._close_completion_deferred = None
    pending.callback(None)

    assert scheduler._close_attempt_owner is None


def test_scheduler_open_failure_preserves_failure_after_deferred_cleanup(
    mocker,
) -> None:
    scheduler = BackendScheduler(MagicMock())
    original = Failure(RuntimeError("open failed"))
    scheduler._cleanup_after_open_failure = mocker.MagicMock(return_value=succeed(None))

    result = scheduler._handle_open_failure(original, "open-failed")
    failures: list[Failure] = []
    assert isinstance(result, Deferred)
    result.addErrback(lambda failure: failures.append(failure))

    assert failures and failures[0].value is original.value


def test_scheduler_open_failure_returns_original_without_cleanup(mocker) -> None:
    scheduler = BackendScheduler(MagicMock())
    original = Failure(RuntimeError("open failed"))
    scheduler._cleanup_after_open_failure = mocker.MagicMock(return_value=None)

    result = scheduler._handle_open_failure(original, "open-failed")

    assert result is original


def test_scheduler_close_offloads_queue_and_retries_after_late_failure(
    monkeypatch,
    mocker,
) -> None:
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    workers: list[Deferred[object]] = []
    operations: list[object] = []

    def fake_thread(function, *_args, **_kwargs):
        operations.append(function)
        worker: Deferred[object] = Deferred()
        workers.append(worker)
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", fake_thread)
    queue = MagicMock()
    queue.close.side_effect = [QueueError("temporary"), None]
    manager = MagicMock()
    scheduler = BackendScheduler(manager, reactor_io_timeout=1.0)
    scheduler._queue = queue
    scheduler._lifecycle_state = "open"

    first = scheduler.close("first")
    assert isinstance(first, Deferred)
    first.addErrback(lambda _failure: None)
    assert queue.close.call_count == 0
    assert scheduler._lifecycle_state == "closing"
    with pytest.raises(QueueError):
        operations[0]()
    workers[0].errback(QueueError("temporary"))
    assert first.called
    assert scheduler._queue is queue
    assert scheduler._lifecycle_state == "closing"
    manager.close.assert_not_called()

    second = scheduler.close("retry")
    assert isinstance(second, Deferred)
    operations[1]()
    workers[1].callback(None)
    operations[2]()
    workers[2].callback(None)
    assert scheduler._lifecycle_state == "closed"
    queue.close.assert_has_calls([mocker.call(), mocker.call()])
    manager.close.assert_called_once_with()


def test_scheduler_close_queue_timeout_keeps_ownership_until_late_success(
    monkeypatch,
) -> None:
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    queue_worker: Deferred[object] = Deferred()
    manager_worker: Deferred[object] = Deferred()
    workers = iter((queue_worker, manager_worker))
    operations: list[object] = []

    def fake_thread(function, *_args, **_kwargs):
        operations.append(function)
        return next(workers)

    monkeypatch.setattr(scheduler_module, "deferToThread", fake_thread)
    queue = MagicMock()
    manager = MagicMock()
    scheduler = BackendScheduler(manager, reactor_io_timeout=1.0)
    scheduler._queue = queue
    scheduler._lifecycle_state = "open"

    closing = scheduler.close("slow")
    failures: list[BaseException] = []
    assert isinstance(closing, Deferred)
    closing.addErrback(lambda failure: failures.append(failure.value))
    fake_reactor.calls[-1].fire()
    assert failures and failures[0].operation == "scheduler queue close"
    assert queue.close.call_count == 0
    assert manager.close.call_count == 0
    assert (
        scheduler.close("duplicate") is not None
        or scheduler._lifecycle_state == "closing"
    )

    operations[0]()
    queue_worker.callback(None)
    assert queue.close.call_count == 1
    assert manager.close.call_count == 0
    operations[1]()
    manager_worker.callback(None)
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()


def test_scheduler_close_queue_late_failure_is_retryable(monkeypatch) -> None:
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    queue_worker: Deferred[object] = Deferred()
    monkeypatch.setattr(
        scheduler_module, "deferToThread", lambda *_a, **_k: queue_worker
    )
    queue = MagicMock()
    manager = MagicMock()
    scheduler = BackendScheduler(manager, reactor_io_timeout=1.0)
    scheduler._queue = queue
    scheduler._lifecycle_state = "open"

    closing = scheduler.close("late-failure")
    closing.addErrback(lambda _failure: None)
    fake_reactor.calls[-1].fire()
    queue_worker.errback(QueueError("late queue failure"))
    assert scheduler._queue is queue
    assert scheduler._queue_terminal is False
    assert scheduler._lifecycle_state == "closing"
    manager.close.assert_not_called()


def test_scheduler_close_timeout_keeps_heartbeat_running() -> None:
    """A blocking queue close must not stop reactor callbacks."""
    script = """
import time
from types import SimpleNamespace
from scrapy_extension.schedule.scheduler import BackendScheduler
from twisted.internet import reactor
class Queue:
    def close(self):
        time.sleep(0.15)
class Manager:
    def close(self):
        return None
scheduler = BackendScheduler(Manager(), reactor_io_timeout=1.0)
scheduler._queue = Queue()
scheduler._lifecycle_state = 'open'
beats = []
def tick():
    beats.append(time.monotonic())
    if len(beats) < 3:
        reactor.callLater(0.02, tick)
def start():
    result = scheduler.close('heartbeat')
    result.addBoth(lambda _: reactor.stop())
    reactor.callLater(0.02, tick)
reactor.callWhenRunning(start)
reactor.run(installSignalHandlers=False)
assert len(beats) >= 3
"""
    import subprocess
    import sys

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr


def test_scheduler_close_timeout_retains_authoritative_ownership(monkeypatch) -> None:
    """A close timeout must not permit a second manager release or close race."""
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    worker: Deferred[object] = Deferred()
    operations: list[object] = []

    def fake_thread(function, *_args, **_kwargs):
        operations.append(function)
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", fake_thread)

    manager = MagicMock()
    scheduler = BackendScheduler(manager, reactor_io_timeout=1.0)
    scheduler._lifecycle_state = "open"

    closing = scheduler.close("slow-release")
    assert isinstance(closing, Deferred)
    failures: list[BaseException] = []
    closing.addErrback(lambda failure: failures.append(failure.value))
    fake_reactor.calls[-1].fire()

    assert failures and failures[0].operation == "scheduler connection close"
    assert scheduler._lifecycle_state == "closing"
    assert scheduler.close("duplicate-close") is None
    manager.close.assert_not_called()

    assert operations
    operations[0]()
    worker.callback(None)
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()


def test_scheduler_open_timeout_keeps_late_worker_authoritative(monkeypatch) -> None:
    """A warm-up timeout must not publish OPEN until the worker really settles."""
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    worker: Deferred[object] = Deferred()
    operations: list[object] = []

    def fake_thread(function, *_args, **_kwargs):
        operations.append(function)
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", fake_thread)
    queue = MagicMock()
    monkeypatch.setattr(scheduler_module, "BackendQueue", lambda **_kwargs: queue)

    scheduler = BackendScheduler(MagicMock(), reactor_io_timeout=1.0)
    spider = SimpleNamespace(name="slow-open", crawler=None)
    opened = scheduler.open(spider)
    assert isinstance(opened, Deferred)
    failures: list[BaseException] = []
    opened.addErrback(lambda failure: failures.append(failure.value))
    fake_reactor.calls[-1].fire()

    assert failures and failures[0].operation == "scheduler connection warm-up"
    assert scheduler._lifecycle_state == "opening"
    assert operations
    operations[0]()
    worker.callback(None)
    assert scheduler._lifecycle_state == "open"


def test_scheduler_open_worker_failure_runs_authoritative_cleanup(monkeypatch) -> None:
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    worker: Deferred[object] = Deferred()
    operations: list[object] = []

    def fake_thread(function, *_args, **_kwargs):
        operations.append(function)
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", fake_thread)
    scheduler = BackendScheduler(MagicMock(), reactor_io_timeout=1.0)
    scheduler._cleanup_after_open_failure = MagicMock(return_value=succeed(None))
    opened = scheduler.open(SimpleNamespace(name="failed-open", crawler=None))
    failures: list[BaseException] = []
    assert isinstance(opened, Deferred)
    opened.addErrback(lambda failure: failures.append(failure.value))
    operations[0]()
    worker.errback(RuntimeError("warm-up failed"))

    assert failures and str(failures[0]) == "warm-up failed"
    scheduler._cleanup_after_open_failure.assert_called_once_with("open-failed")


def test_scheduler_open_publication_failure_preserves_worker_error(monkeypatch) -> None:
    fake_reactor = _FakeReactor()
    fake_reactor.running = True
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    worker: Deferred[object] = Deferred()
    operations: list[object] = []

    def fake_thread(function, *_args, **_kwargs):
        operations.append(function)
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", fake_thread)
    scheduler = BackendScheduler(MagicMock(), reactor_io_timeout=1.0)
    scheduler._finish_open = MagicMock(side_effect=RuntimeError("publish failed"))
    scheduler._cleanup_after_open_failure = MagicMock(return_value=None)
    opened = scheduler.open(SimpleNamespace(name="publication-failure", crawler=None))
    failures: list[BaseException] = []
    assert isinstance(opened, Deferred)
    opened.addErrback(lambda failure: failures.append(failure.value))
    operations[0]()
    worker.callback(None)

    assert failures and str(failures[0]) == "publish failed"
    scheduler._cleanup_after_open_failure.assert_called_once_with("open-failed")


def test_pipeline_lifecycle_and_store_use_deferred_adapters(monkeypatch) -> None:
    manager = MagicMock()
    manager.get_storage_backend.return_value.store.return_value = None
    spider = SimpleNamespace(name="adapter", crawler=None)
    pipeline = BackendPipeline(manager, reactor_io_timeout=1.0)
    pipeline._storage_supported = True
    pending: list[tuple[Deferred[object], object, tuple[object, ...]]] = []

    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(pipeline_module, "bounded_deferred", lambda source, **_: source)

    def fake_thread(function, *args, **_kwargs):
        deferred: Deferred[object] = Deferred()
        pending.append((deferred, function, args))
        return deferred

    def fake_ordered(function, *args, **_kwargs):
        deferred: Deferred[object] = Deferred()
        pending.append((deferred, function, args))
        return deferred, deferred

    monkeypatch.setattr(pipeline_module, "deferToThread", fake_thread)
    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", fake_ordered)

    opening = pipeline.open_spider(spider)
    assert isinstance(opening, Deferred)
    opening_operation, opening_function, opening_args = pending.pop(0)
    opening_function(*opening_args)
    opening_operation.callback(None)
    assert pipeline._opened

    stored = pipeline.process_item(_Item(value="ok"), spider)
    assert isinstance(stored, Deferred)
    # The fake ordered adapter returns the worker Deferred; execute its queued
    # operation and settle it to advance the FIFO tail.
    store_operation, store_function, store_args = pending.pop(0)
    result = store_function(*store_args)
    store_operation.callback(result)
    assert stored.called

    closing = pipeline.close_spider(spider)
    assert isinstance(closing, Deferred)
    close_operation, close_function, close_args = pending.pop(0)
    close_result = close_function(*close_args)
    close_operation.callback(close_result)
    assert closing.called
    manager.close.assert_called_once_with()


def test_pipeline_async_admission_and_lifecycle_fences(monkeypatch) -> None:
    """Closed/opening/closing states remain fail-fast on the Deferred path."""
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    spider = SimpleNamespace(name="fence", crawler=None)

    closed = BackendPipeline(MagicMock())
    closed._closed = True
    with pytest.raises(RuntimeError, match="pipeline is closed"):
        closed.open_spider(spider)
    with pytest.raises(RuntimeError, match="pipeline is closed"):
        closed.process_item(_Item(value="closed"), spider)
    assert closed.close_spider(spider).called

    opening = BackendPipeline(MagicMock())
    opening._opening = True
    with pytest.raises(RuntimeError, match="lifecycle transition"):
        opening.open_spider(spider)
    with pytest.raises(RuntimeError, match="open is still"):
        opening.process_item(_Item(value="opening"), spider)
    with pytest.raises(RuntimeError, match="open is still"):
        opening.close_spider(spider)

    closing = BackendPipeline(MagicMock())
    closing._closing = True
    with pytest.raises(RuntimeError, match="lifecycle transition"):
        closing.open_spider(spider)
    with pytest.raises(RuntimeError, match="close must"):
        closing.process_item(_Item(value="closing"), spider)


def test_scheduler_ack_and_lifecycle_deferred_paths(monkeypatch) -> None:
    manager = MagicMock()
    queue = MagicMock()
    scheduler = BackendScheduler(manager, reactor_io_timeout=1.0)
    scheduler._queue = queue
    scheduler._lifecycle_state = "open"
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)

    def immediate_ordered(function, *args, **kwargs):
        kwargs.pop("timeout")
        kwargs.pop("operation")
        return succeed(function(*args, **kwargs)), succeed(None)

    monkeypatch.setattr(scheduler_module, "defer_to_thread_ordered", immediate_ordered)
    monkeypatch.setattr(
        scheduler_module,
        "deferToThread",
        lambda function, *args, **kwargs: succeed(function(*args, **kwargs)),
    )
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    request = SimpleNamespace(meta={"_backend_ack_token": "token"})
    result = scheduler._ack_request_token(request, log_message="ack failed")
    assert isinstance(result, Deferred)
    assert result.called
    assert request.meta == {}
    queue.ack.assert_called_once_with(token="token")

    nack_request = SimpleNamespace(meta={"_backend_ack_token": "nack-token"})
    nack_result = scheduler._nack_request_token(
        nack_request,
        log_message="nack failed",
    )
    assert isinstance(nack_result, Deferred)
    assert nack_request.meta == {}

    def failed_ordered(_function, *args, **kwargs):
        del args, kwargs
        return succeed(None), fail(BackendError("ack unavailable"))

    monkeypatch.setattr(scheduler_module, "defer_to_thread_ordered", failed_ordered)
    failed_request = SimpleNamespace(meta={"_backend_ack_token": "failed-token"})
    failed_result = scheduler._ack_request_token(
        failed_request,
        log_message="ack failed",
    )
    assert isinstance(failed_result, Deferred)
    assert failed_result.called
    assert failed_request.meta["_backend_ack_token"] == "failed-token"
    monkeypatch.setattr(scheduler_module, "defer_to_thread_ordered", immediate_ordered)

    # Open warms the manager off-reactor before publishing the queue.
    scheduler._queue = None
    scheduler._lifecycle_state = "new"
    spider = SimpleNamespace(name="scheduler", crawler=None)
    monkeypatch.setattr(scheduler_module, "BackendQueue", lambda **_: queue)
    opened = scheduler.open(spider)
    assert isinstance(opened, Deferred)
    assert opened.called
    assert scheduler._queue is queue

    closed = scheduler.close("finished")
    assert isinstance(closed, Deferred)
    assert closed.called
    manager.close.assert_called_once_with()
