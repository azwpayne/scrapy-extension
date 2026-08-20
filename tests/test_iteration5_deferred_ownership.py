"""Deterministic deferred-ownership regressions for iteration five."""

from __future__ import annotations

import gc
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from scrapy import Field, Item
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure as TwistedFailure

import scrapy_extension.pipeline.pipeline as pipeline_module
import scrapy_extension.schedule.scheduler as scheduler_module
import scrapy_extension.utils.reactor as reactor_module
from scrapy_extension.exceptions import BackendError, BackendOperationTimeout
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.schedule.scheduler import _chain_lifecycle_result
from scrapy_extension.utils.reactor import defer_to_thread_bounded


class _Item(Item):
    value = Field()


class _DelayedCall:
    def __init__(self, callback):
        self._callback = callback
        self.cancelled = False

    def active(self) -> bool:
        return not self.cancelled

    def cancel(self) -> None:
        self.cancelled = True

    def fire(self) -> None:
        if self.active():
            self.cancelled = True
            self._callback()


class _FakeReactor:
    running = True

    def __init__(self) -> None:
        self.calls: list[_DelayedCall] = []

    def callLater(self, _delay: float, callback):
        call = _DelayedCall(callback)
        self.calls.append(call)
        return call


def _open_scheduler(monkeypatch, dupefilter):
    fake_reactor = _FakeReactor()
    workers: list[tuple[Deferred[object], object, tuple, dict]] = []

    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function, args, kwargs))
        return worker

    queue = MagicMock()
    manager = MagicMock()
    monkeypatch.setattr(scheduler_module, "deferToThread", thread)
    monkeypatch.setattr(scheduler_module, "BackendQueue", lambda **_kwargs: queue)
    scheduler = scheduler_module.BackendScheduler(
        manager,
        dupefilter=dupefilter,
        reactor_io_timeout=1.0,
    )
    opening = scheduler.open(SimpleNamespace(name="iteration5", crawler=None))
    return fake_reactor, workers, queue, manager, scheduler, opening


def test_lifecycle_chain_observes_nested_failure_and_raises_safely():
    source: Deferred[object] = Deferred()
    nested: Deferred[object] = Deferred()
    chained = _chain_lifecycle_result(source, lambda _value: nested)
    failures: list[BaseException] = []
    chained.addErrback(lambda failure: failures.append(failure.value))
    source.callback(None)
    nested.errback(RuntimeError("nested failure"))
    assert str(failures[0]) == "nested failure"

    source = Deferred()
    chained = _chain_lifecycle_result(
        source, lambda _value: (_ for _ in ()).throw(KeyboardInterrupt("continuation"))
    )
    failures = []
    chained.addErrback(lambda failure: failures.append(failure.value))
    source.callback(None)
    assert isinstance(failures[0], KeyboardInterrupt)


def test_lifecycle_chain_preserves_failure_value_from_nested_success():
    source: Deferred[object] = Deferred()
    nested: Deferred[object] = Deferred()
    chained = _chain_lifecycle_result(source, lambda _value: nested)
    failures: list[BaseException] = []
    chained.addErrback(lambda failure: failures.append(failure.value) or None)

    source.callback(None)
    nested.callback(TwistedFailure(RuntimeError("failure value")))

    assert len(failures) == 1
    assert str(failures[0]) == "failure value"


def test_bounded_thread_timeout_consumes_late_failure(monkeypatch):
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
    public = defer_to_thread_bounded(
        lambda: None,
        timeout=1.0,
        operation="iteration5 worker",
    )
    failures: list[BaseException] = []
    public.addErrback(lambda failure: failures.append(failure.value))
    fake_reactor.calls[-1].fire()
    worker.errback(RuntimeError("late worker failure"))
    assert isinstance(failures[0], BackendOperationTimeout)

    worker = Deferred()
    workers.append(worker)
    public = defer_to_thread_bounded(
        lambda: None,
        timeout=1.0,
        operation="iteration5 in-time worker",
    )
    public.addErrback(lambda failure: failures.append(failure.value))
    worker.errback(BackendError("in-time worker failure"))
    assert isinstance(failures[-1], BackendError)


@pytest.mark.parametrize("settle_before_open", [False, True])
def test_generic_dupefilter_failure_is_public_once_without_unhandled_deferred(
    monkeypatch,
    settle_before_open: bool,
):
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: False)
    dupefilter = MagicMock()
    source: Deferred[object] = Deferred()
    dupefilter.open.return_value = source
    manager = MagicMock()
    scheduler = scheduler_module.BackendScheduler(manager, dupefilter=dupefilter)
    if settle_before_open:
        source.errback(RuntimeError("generic source failure"))

    opening = scheduler.open(SimpleNamespace(name="iteration5", crawler=None))
    assert opening is source
    failures: list[BaseException] = []
    opening.addErrback(lambda failure: failures.append(failure.value) or None)
    if not settle_before_open:
        source.errback(RuntimeError("generic source failure"))

    assert len(failures) == 1
    assert str(failures[0]) == "generic source failure"
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()
    del opening, source, scheduler
    gc.collect()


def test_generic_dupefilter_timeout_then_failure_updates_close_authority(monkeypatch):
    dupefilter = MagicMock()
    dupefilter_open: Deferred[object] = Deferred()
    dupefilter.open.return_value = dupefilter_open
    reactor, workers, _queue, manager, scheduler, opening = _open_scheduler(
        monkeypatch, dupefilter
    )
    failures: list[BaseException] = []
    opening.addErrback(lambda failure: failures.append(failure.value))
    reactor.calls[0].fire()
    dupefilter_open.errback(RuntimeError("late generic failure"))
    for worker, function, args, kwargs in workers:
        function(*args, **kwargs)
        if not worker.called:
            worker.callback(None)
    assert isinstance(failures[0], BackendOperationTimeout)
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()


def test_generic_dupefilter_success_publishes_in_time_and_closes_cleanly(monkeypatch):
    dupefilter = MagicMock()
    dupefilter_open: Deferred[object] = Deferred()
    dupefilter.open.return_value = dupefilter_open
    reactor, workers, queue, manager, scheduler, opening = _open_scheduler(
        monkeypatch, dupefilter
    )
    values: list[object] = []
    opening.addCallback(values.append)

    dupefilter_open.callback(None)
    for worker, function, args, kwargs in list(workers):
        function(*args, **kwargs)
        if not worker.called:
            worker.callback(None)

    assert values == [None]
    assert scheduler._lifecycle_state == "open"
    closing = scheduler.close("iteration5-success")
    assert isinstance(closing, Deferred)
    index = 0
    while index < len(workers):
        worker, function, args, kwargs = workers[index]
        if not worker.called:
            function(*args, **kwargs)
            worker.callback(None)
        index += 1
    assert scheduler._lifecycle_state == "closed"
    queue.close.assert_called_once_with()
    manager.close.assert_called_once_with()
    del reactor


def test_generic_dupefilter_running_failure_publishes_before_gc(monkeypatch):
    dupefilter = MagicMock()
    dupefilter_open: Deferred[object] = Deferred()
    dupefilter.open.return_value = dupefilter_open
    _reactor, workers, _queue, manager, scheduler, opening = _open_scheduler(
        monkeypatch, dupefilter
    )
    failures: list[BaseException] = []
    opening.addErrback(lambda failure: failures.append(failure.value) or None)

    dupefilter_open.errback(RuntimeError("running generic failure"))
    for worker, function, args, kwargs in list(workers):
        function(*args, **kwargs)
        if not worker.called:
            worker.callback(None)

    assert len(failures) == 1
    assert str(failures[0]) == "running generic failure"
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()
    gc.collect()


def test_non_generic_open_failure_runs_sync_cleanup(monkeypatch):
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: False)
    dupefilter = MagicMock()
    dupefilter.open.return_value = None
    scheduler = scheduler_module.BackendScheduler(
        MagicMock(),
        dupefilter=dupefilter,
    )

    def fail_queue(**_kwargs):
        raise RuntimeError("sync queue construction failed")

    monkeypatch.setattr(scheduler_module, "BackendQueue", fail_queue)

    with pytest.raises(RuntimeError, match="sync queue construction failed"):
        scheduler.open(SimpleNamespace(name="iteration5", crawler=None))

    assert scheduler._lifecycle_state == "closed"
    dupefilter.close.assert_called_once_with("open-failed")


def test_close_requested_during_open_fences_late_warmup_success(monkeypatch):
    fake_reactor = _FakeReactor()
    workers: list[tuple[Deferred[object], object, tuple, dict]] = []
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function, args, kwargs))
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", thread)
    manager = MagicMock()
    scheduler = scheduler_module.BackendScheduler(manager, reactor_io_timeout=1.0)
    opening = scheduler.open(SimpleNamespace(name="iteration5", crawler=None))
    opening.addErrback(lambda _failure: None)
    closing = scheduler.close("close-during-open")
    closing.addErrback(lambda _failure: None)

    worker, function, args, kwargs = workers[0]
    function(*args, **kwargs)
    worker.callback(None)
    index = 1
    while index < len(workers):
        worker, function, args, kwargs = workers[index]
        function(*args, **kwargs)
        if not worker.called:
            worker.callback(None)
        index += 1

    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()


def test_generic_dupefilter_timeout_keeps_late_success_authoritative(monkeypatch):
    dupefilter = MagicMock()
    dupefilter_open: Deferred[object] = Deferred()
    dupefilter.open.return_value = dupefilter_open
    reactor, workers, queue, _manager, scheduler, opening = _open_scheduler(
        monkeypatch, dupefilter
    )
    failures: list[BaseException] = []
    opening.addErrback(lambda failure: failures.append(failure.value))

    reactor.calls[0].fire()
    assert isinstance(failures[0], BackendOperationTimeout)
    assert scheduler._lifecycle_state == "opening"
    assert scheduler._queue is None
    assert not queue.close.called

    dupefilter_open.callback(None)
    assert scheduler._lifecycle_state == "opening"
    worker, function, args, kwargs = workers[0]
    function(*args, **kwargs)
    worker.callback(None)
    assert scheduler._lifecycle_state == "open"


def test_pipeline_store_public_timeout_preserves_fifo_authority(monkeypatch):
    pending: list[tuple[Deferred[object], Deferred[object]]] = []

    def ordered(_function, *_args, **_kwargs):
        operation: Deferred[object] = Deferred()
        bounded: Deferred[object] = Deferred()
        pending.append((operation, bounded))
        return operation, bounded

    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", ordered)
    pipeline = BackendPipeline(MagicMock(), reactor_io_timeout=1.0)
    pipeline._storage_supported = True
    spider = SimpleNamespace(name="iteration5", crawler=None)

    first = pipeline.process_item(_Item(value="first"), spider)
    second = pipeline.process_item(_Item(value="second"), spider)
    first_failures: list[BaseException] = []
    first.addErrback(lambda failure: first_failures.append(failure.value))
    assert len(pending) == 1

    operation, bounded = pending[0]
    bounded.errback(BackendOperationTimeout("pipeline store", 1.0))
    assert isinstance(first_failures[0], BackendOperationTimeout)
    operation.errback(BackendError("late store failure"))

    # The late authoritative failure is consumed, but it still advances the FIFO
    # tail and starts the second operation only after the first has settled.
    assert len(pending) == 2
    second_operation, second_bounded = pending[1]
    second_bounded.callback("second-item")
    second_values: list[object] = []
    second.addCallback(second_values.append)
    assert second_values == ["second-item"]
    second_operation.callback("second-item")


def test_pipeline_close_reentry_is_bounded_and_idempotent(monkeypatch):
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: False)
    manager = MagicMock()
    strategy = MagicMock()
    spider = SimpleNamespace(name="iteration5", crawler=None)
    pipeline = BackendPipeline(manager, storage_strategy=strategy)
    pipeline.open_spider(spider)

    recursive_result: list[object] = []

    def close_strategy():
        recursive_result.append(pipeline.close_spider(spider))

    strategy.close.side_effect = close_strategy
    pipeline.close_spider(spider)

    assert recursive_result == [None]
    assert strategy.close.call_count == 1
    manager.close.assert_called_once_with()
    assert pipeline._closed is True


def test_pipeline_async_close_reentry_and_concurrency_are_bounded(monkeypatch):
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    pipeline = BackendPipeline(MagicMock())
    pipeline._closing = True
    pipeline._close_owner_thread_id = threading.get_ident()
    assert pipeline.close_spider().called

    pipeline._close_owner_thread_id = threading.get_ident() + 1
    try:
        pipeline.close_spider()
    except RuntimeError as exc:
        assert str(exc) == "pipeline close is already in progress"
    else:
        raise AssertionError("concurrent close was not rejected")


def test_pipeline_public_store_views_do_not_double_publish(monkeypatch):
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    calls: list[tuple[Deferred[object], Deferred[object]]] = []

    def ordered(_function, *_args, **_kwargs):
        operation: Deferred[object] = Deferred()
        bounded: Deferred[object] = Deferred()
        calls.append((operation, bounded))
        return operation, bounded

    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", ordered)
    spider = SimpleNamespace(name="iteration5", crawler=None)
    first_pipeline = BackendPipeline(MagicMock())
    first_pipeline._storage_supported = True
    first = first_pipeline.process_item(_Item(value="first"), spider)
    first.callback("already published")
    calls[0][1].callback("late success")

    second_pipeline = BackendPipeline(MagicMock())
    second_pipeline._storage_supported = True
    second = second_pipeline.process_item(_Item(value="second"), spider)
    second.callback("already published")
    calls[1][1].errback(RuntimeError("late failure"))
    calls[0][0].callback(None)
    calls[1][0].callback(None)


def test_pipeline_close_rejects_unrelated_concurrent_attempts():
    pipeline = BackendPipeline(MagicMock())
    pipeline._closing = True
    pipeline._close_async_pending = True
    pipeline._close_owner_thread_id = threading.get_ident() + 1
    try:
        pipeline._close_locked()
    except RuntimeError as exc:
        assert str(exc) == "pipeline close is already in progress"
    else:
        raise AssertionError("concurrent close was not rejected")

    pipeline._close_async_pending = False
    try:
        pipeline._close_locked()
    except RuntimeError as exc:
        assert str(exc) == "pipeline close is already in progress"
    else:
        raise AssertionError("owned close was not rejected")

    pipeline._close_owner_thread_id = threading.get_ident()
    pipeline._close_locked()
    assert pipeline._closed is False


def test_pipeline_close_waits_for_late_open_success_without_failure(monkeypatch):
    fake_reactor = _FakeReactor()
    workers: list[tuple[Deferred[object], object, tuple, dict]] = []
    close_calls: list[tuple[Deferred[object], Deferred[object], object, tuple]] = []
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function, args, kwargs))
        return worker

    def ordered(function, *args, **kwargs):
        del kwargs
        operation: Deferred[object] = Deferred()
        bounded: Deferred[object] = Deferred()
        close_calls.append((operation, bounded, function, args))
        return operation, bounded

    monkeypatch.setattr(pipeline_module, "deferToThread", thread)
    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", ordered)
    manager = MagicMock()
    pipeline = BackendPipeline(manager, reactor_io_timeout=1.0)
    spider = SimpleNamespace(name="iteration5", crawler=None)
    pipeline.open_spider(spider)
    closing = pipeline.close_spider(spider)
    worker, function, args, kwargs = workers[0]
    function(*args, **kwargs)
    worker.callback(None)
    operation, bounded, close_function, close_args = close_calls[0]
    close_function(*close_args)
    operation.callback(None)
    bounded.callback(None)
    assert closing.called
    assert pipeline._closed


def test_pipeline_close_after_late_open_success_uses_close_result(monkeypatch):
    fake_reactor = _FakeReactor()
    open_workers: list[tuple[Deferred[object], object, tuple, dict]] = []
    close_calls: list[tuple[Deferred[object], Deferred[object], object, tuple]] = []
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        open_workers.append((worker, function, args, kwargs))
        return worker

    def ordered(function, *args, **kwargs):
        del kwargs
        operation: Deferred[object] = Deferred()
        bounded: Deferred[object] = Deferred()
        close_calls.append((operation, bounded, function, args))
        return operation, bounded

    monkeypatch.setattr(pipeline_module, "deferToThread", thread)
    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", ordered)
    manager = MagicMock()
    pipeline = BackendPipeline(manager, reactor_io_timeout=1.0)
    spider = SimpleNamespace(name="iteration5", crawler=None)
    pipeline.open_spider(spider)
    worker, function, args, kwargs = open_workers[0]
    function(*args, **kwargs)
    worker.callback(None)
    assert pipeline._opened

    closing = pipeline.close_spider(spider)
    operation, bounded, close_function, close_args = close_calls[0]
    close_function(*close_args)
    operation.callback(None)
    bounded.callback(None)
    assert closing.called
    assert pipeline._closed


def test_pipeline_async_close_failure_is_public_and_consumed(monkeypatch):
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    calls: list[tuple[Deferred[object], Deferred[object]]] = []

    def ordered(_function, *_args, **_kwargs):
        operation: Deferred[object] = Deferred()
        bounded: Deferred[object] = Deferred()
        calls.append((operation, bounded))
        return operation, bounded

    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", ordered)
    pipeline = BackendPipeline(MagicMock(), reactor_io_timeout=1.0)
    pipeline._opened = True
    pipeline._opened_spider = SimpleNamespace(name="iteration5", crawler=None)
    closing = pipeline.close_spider(pipeline._opened_spider)
    failures: list[BaseException] = []
    closing.addErrback(lambda failure: failures.append(failure.value))
    operation, bounded = calls[0]
    bounded.errback(RuntimeError("close worker failure"))
    operation.errback(RuntimeError("late close worker failure"))
    assert str(failures[0]) == "close worker failure"

    second_pipeline = BackendPipeline(MagicMock(), reactor_io_timeout=1.0)
    second_pipeline._opened = True
    second_pipeline._opened_spider = SimpleNamespace(name="iteration5", crawler=None)
    second_closing = second_pipeline.close_spider(second_pipeline._opened_spider)
    second_closing.callback(None)
    second_operation, second_bounded = calls[1]
    second_bounded.callback(None)

    third_pipeline = BackendPipeline(MagicMock(), reactor_io_timeout=1.0)
    third_pipeline._opened = True
    third_pipeline._opened_spider = SimpleNamespace(name="iteration5", crawler=None)
    third_closing = third_pipeline.close_spider(third_pipeline._opened_spider)
    third_closing.callback(None)
    third_operation, third_bounded = calls[2]
    third_bounded.errback(RuntimeError("ignored late close failure"))
    third_operation.errback(RuntimeError("ignored authoritative failure"))
    second_operation.errback(RuntimeError("ignored authoritative failure"))


def test_pipeline_open_late_failure_reaches_active_close_owner(monkeypatch):
    fake_reactor = _FakeReactor()
    workers: list[tuple[Deferred[object], object, tuple, dict]] = []
    close_calls: list[
        tuple[Deferred[object], Deferred[object], object, tuple, dict]
    ] = []

    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function, args, kwargs))
        return worker

    def ordered(function, *args, **kwargs):
        operation: Deferred[object] = Deferred()
        bounded: Deferred[object] = Deferred()
        close_calls.append((operation, bounded, function, args, kwargs))
        return operation, bounded

    monkeypatch.setattr(pipeline_module, "deferToThread", thread)
    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", ordered)
    manager = MagicMock()
    pipeline = BackendPipeline(manager, reactor_io_timeout=1.0)
    spider = SimpleNamespace(name="iteration5", crawler=None)

    opening = pipeline.open_spider(spider)
    opening_failures: list[BaseException] = []
    opening.addErrback(lambda failure: opening_failures.append(failure.value))
    fake_reactor.calls[0].fire()

    closing = pipeline.close_spider(spider)
    close_failures: list[BaseException] = []
    closing.addErrback(lambda failure: close_failures.append(failure.value))
    workers[0][0].errback(RuntimeError("late pipeline open failure"))

    close_operation, close_bounded, close_function, close_args, close_kwargs = (
        close_calls[0]
    )
    del close_kwargs
    close_function(*close_args)
    close_operation.callback(None)
    close_bounded.callback(None)
    assert str(opening_failures[0]) == "Backend operation timed out: pipeline open."
    assert str(close_failures[0]) == "late pipeline open failure"
    assert pipeline._closed is True
    manager.close.assert_called_once_with()
