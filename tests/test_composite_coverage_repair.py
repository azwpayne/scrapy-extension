"""Deterministic lifecycle coverage using controlled Deferred ownership."""

from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pydispatch.errors import DispatcherKeyError
from scrapy import Spider
from scrapy.http import Request
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

import scrapy_extension.pipeline.pipeline as pipeline_module
import scrapy_extension.schedule.scheduler as scheduler_module
import scrapy_extension.spider.spider_mixin as spider_module
import scrapy_extension.utils.reactor as reactor_module
from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions import BackendError, SerializationError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin


class _AttachFailureDeferred(Deferred):
    def __init__(self, failures: tuple[str, ...] = ()) -> None:
        super().__init__()
        self._failures = set(failures)

    def _reject_once(self, name: str) -> None:
        if name in self._failures:
            self._failures.remove(name)
            raise RuntimeError(f"{name} attachment rejected")

    def addBoth(self, callback, *args, **kwargs):  # type: ignore[no-untyped-def]
        self._reject_once("addBoth")
        return super().addBoth(callback, *args, **kwargs)

    def addCallbacks(self, callback, errback, *args, **kwargs):  # type: ignore[no-untyped-def]
        self._reject_once("addCallbacks")
        return super().addCallbacks(callback, errback, *args, **kwargs)

    def addErrback(self, errback, *args, **kwargs):  # type: ignore[no-untyped-def]
        self._reject_once("addErrback")
        return super().addErrback(errback, *args, **kwargs)


class _AlwaysRejectingDeferred(Deferred):
    def addCallback(self, callback, *args, **kwargs):  # type: ignore[no-untyped-def]
        del callback, args, kwargs
        raise KeyboardInterrupt("observer attachment interrupted")

    def addBoth(self, callback, *args, **kwargs):  # type: ignore[no-untyped-def]
        del callback, args, kwargs
        raise KeyboardInterrupt("observer attachment interrupted")

    def addCallbacks(self, callback, errback, *args, **kwargs):  # type: ignore[no-untyped-def]
        del callback, errback, args, kwargs
        raise KeyboardInterrupt("observer attachment interrupted")


class _BothFailureDeferred(Deferred):
    def addBoth(self, callback, *args, **kwargs):  # type: ignore[no-untyped-def]
        del callback, args, kwargs
        raise KeyboardInterrupt("both attachment interrupted")


class _ErrbackFailureDeferred(Deferred):
    def addErrback(self, errback, *args, **kwargs):  # type: ignore[no-untyped-def]
        del errback, args, kwargs
        raise KeyboardInterrupt("errback attachment interrupted")


class _ObserverRejectingDeferred(Deferred):
    def addCallbacks(self, callback, errback, *args, **kwargs):  # type: ignore[no-untyped-def]
        del callback, errback, args, kwargs
        raise RuntimeError("observer attachment rejected")


class _SecondAddBothRejectingDeferred(Deferred):
    created = 0

    def __init__(self) -> None:
        super().__init__()
        type(self).created += 1
        self._reject_add_both = type(self).created == 2

    def addBoth(self, callback, *args, **kwargs):  # type: ignore[no-untyped-def]
        if self._reject_add_both:
            del callback, args, kwargs
            raise KeyboardInterrupt("bounded observer interrupted")
        return super().addBoth(callback, *args, **kwargs)


class _ControlledTimer:
    def __init__(self) -> None:
        self.cancelled = False

    def active(self) -> bool:
        return not self.cancelled

    def cancel(self) -> None:
        self.cancelled = True


class _ControlledReactor:
    def __init__(self) -> None:
        self.timers: list[_ControlledTimer] = []

    def callLater(self, _delay: float, _callback):  # type: ignore[no-untyped-def]
        timer = _ControlledTimer()
        self.timers.append(timer)
        return timer


class _Spider(BackendSpiderMixin, Spider):
    name = "coverage-repair"
    backend_type = BackendType.REDIS


def _mixin(mocker) -> BackendSpiderMixin:
    spider = _Spider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)
    return spider


def _errors(deferred: Deferred[object]) -> list[BaseException]:
    errors: list[BaseException] = []
    deferred.addErrback(lambda failure: errors.append(failure.value))
    return errors


# Reactor adapters ---------------------------------------------------------


def test_reactor_notification_and_timer_boundaries_are_nonfatal(monkeypatch):
    class Rejecting(Deferred):
        def callback(self, value):  # type: ignore[no-untyped-def]
            del value
            raise KeyboardInterrupt("callback rejected")

        def errback(self, error=None):  # type: ignore[no-untyped-def]
            del error
            raise KeyboardInterrupt("errback rejected")

    reactor_module._safe_callback(Rejecting(), None)
    reactor_module._safe_errback(Rejecting(), RuntimeError("ignored"))

    worker: Deferred[object] = Deferred()

    class NoTimer:
        def callLater(self, _delay, _callback):
            raise RuntimeError("reactor is shutting down")

    monkeypatch.setattr(reactor_module, "deferToThread", lambda *_a, **_k: worker)
    monkeypatch.setattr(reactor_module, "_reactor", lambda: NoTimer())
    operation, bounded = reactor_module.defer_to_thread_ordered(
        lambda: None, timeout=1, operation="timer-rejection"
    )
    errors = _errors(bounded)
    assert errors and str(errors[0]) == "reactor is shutting down"
    assert not operation.called
    worker.callback(None)
    assert operation.called


def test_reactor_bounded_view_attachment_and_timer_cancellation_failures(monkeypatch):
    source: Deferred[object] = Deferred()
    timer = MagicMock()
    timer.active.return_value = True

    class Reactor:
        def callLater(self, _delay, _callback):
            return timer

    monkeypatch.setattr(reactor_module, "_reactor", lambda: Reactor())
    monkeypatch.setattr(reactor_module, "Deferred", _BothFailureDeferred)
    bounded = reactor_module.bounded_deferred(
        source, timeout=1, operation="observer-rejection"
    )
    errors = _errors(bounded)
    assert errors and isinstance(errors[0], KeyboardInterrupt)
    source.callback(None)

    worker: Deferred[object] = Deferred()
    timer.cancel.side_effect = KeyboardInterrupt("cancel rejected")
    monkeypatch.setattr(reactor_module, "Deferred", Deferred)
    monkeypatch.setattr(reactor_module, "deferToThread", lambda *_a, **_k: worker)
    _operation, bounded = reactor_module.defer_to_thread_ordered(
        lambda: None, timeout=1, operation="cancel-cleanup"
    )
    worker.callback(None)
    assert bounded.called


def test_reactor_bounded_wrapper_observer_failure_does_not_replace_worker(
    monkeypatch,
):
    worker: Deferred[object] = _ErrbackFailureDeferred()
    controlled_reactor = _ControlledReactor()
    monkeypatch.setattr(reactor_module, "_reactor", lambda: controlled_reactor)
    monkeypatch.setattr(
        reactor_module,
        "deferToThread",
        lambda *_a, **_k: worker,
    )
    public = reactor_module.defer_to_thread_bounded(
        lambda: None, timeout=1, operation="observer-failure"
    )
    assert public is not worker
    assert not worker.called
    worker.callback(None)
    assert public.called


# Pipeline adapters/lifecycle ---------------------------------------------


def test_pipeline_submission_and_batched_attachment_roll_back(mocker):
    def reject(*_args, **_kwargs):
        raise KeyboardInterrupt("thread pool closed")

    mocker.patch.object(pipeline_module, "deferToThread", reject)
    returned = pipeline_module._submit_thread(lambda: None)
    errors = _errors(returned)
    assert errors and isinstance(errors[0], KeyboardInterrupt)

    from scrapy_extension.storage.strategies import BatchedStorageStrategy

    strategy = BatchedStorageStrategy(threshold=2)
    strategy.attach_owner = MagicMock(side_effect=RuntimeError("attach failed"))  # type: ignore[method-assign]
    lease = MagicMock()
    release = mocker.patch(
        "scrapy_extension.backends.connectors.release_manager_acquire",
        side_effect=KeyboardInterrupt("release interrupted"),
    )
    with pytest.raises(RuntimeError, match="attach failed"):
        BackendPipeline(
            MagicMock(), storage_strategy=strategy, connection_manager_lease=lease
        )
    release.assert_called_once_with(lease, exact=True)


def test_pipeline_async_open_and_close_provider_failures_keep_retry_fences(
    monkeypatch,
):
    pipeline = BackendPipeline(MagicMock())
    spider = SimpleNamespace(name="coverage-repair", crawler=None)
    worker = _AlwaysRejectingDeferred()
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(pipeline_module, "deferToThread", lambda *_a, **_k: worker)
    returned = pipeline.open_spider(spider)
    errors = _errors(returned)
    assert errors and isinstance(errors[0], KeyboardInterrupt)
    assert pipeline._opening is True
    assert not pipeline._closed

    pipeline = BackendPipeline(MagicMock())
    pipeline._opened = True

    def reject_submission(*_args, **_kwargs):
        raise RuntimeError("close adapter rejected")

    monkeypatch.setattr(reactor_module, "deferToThread", reject_submission)
    returned = pipeline.close_spider(spider)
    errors = _errors(returned)
    assert errors and str(errors[0]) == "close adapter rejected"
    assert not pipeline._closing

    pipeline = BackendPipeline(MagicMock())
    pipeline._opened = True
    pipeline._async_tail = _AlwaysRejectingDeferred()
    returned = pipeline.close_spider(spider)
    errors = _errors(returned)
    assert errors and isinstance(errors[0], KeyboardInterrupt)
    assert not pipeline._closing


def test_pipeline_async_close_observer_and_store_finally_paths(mocker, monkeypatch):
    pipeline = BackendPipeline(MagicMock())
    pipeline._opened = True
    spider = SimpleNamespace(name="coverage-repair", crawler=None)
    operation = _ErrbackFailureDeferred()
    jobs: list[tuple[object, tuple[object, ...], dict[str, object]]] = []
    controlled_reactor = _ControlledReactor()

    def submit(function, *args, **kwargs):
        jobs.append((function, args, kwargs))
        return operation

    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(reactor_module, "_reactor", lambda: controlled_reactor)
    monkeypatch.setattr(reactor_module, "deferToThread", submit)
    returned = pipeline.close_spider(spider)
    assert not returned.called
    assert pipeline._closing
    function, args, kwargs = jobs.pop()
    function(*args, **kwargs)
    operation.callback(None)
    assert returned.called

    failure = KeyboardInterrupt("close retry interrupted")
    pipeline = BackendPipeline(MagicMock(), max_storage_errors=None)
    pipeline._closing = True
    pipeline._close_waiting_for_stores = True
    thread_id = threading.get_ident()
    pipeline._active_store_count = 1
    pipeline._active_store_threads[thread_id] = 1
    mocker.patch.object(pipeline, "_close_locked", side_effect=failure)
    pipeline._leave_store(thread_id)
    assert pipeline._pending_close_error is failure
    assert not pipeline._closing


def test_pipeline_store_serialization_and_oversize_state_are_observable(mocker):
    stats = MagicMock()
    spider = SimpleNamespace(
        name="coverage-repair", crawler=SimpleNamespace(stats=stats)
    )
    pipeline = BackendPipeline(MagicMock(), max_item_bytes=2)
    pipeline._storage_supported = True
    serialization = SerializationError("already normalized", serializer="json")
    mocker.patch.object(pipeline, "_serialize_item", side_effect=serialization)
    with pytest.raises(SerializationError) as raised:
        pipeline._process_item_unlocked({"x": 1}, spider)
    assert raised.value is serialization

    mocker.patch.object(pipeline, "_serialize_item", return_value=b"123")
    with pytest.raises(SerializationError, match="Failed to serialize item"):
        pipeline._process_item_unlocked({"x": 1}, spider)
    stats.inc_value.assert_any_call("pipeline/oversize_dropped")
    stats.inc_value.assert_any_call("pipeline/oversize_rejected")


# Scheduler open/close -----------------------------------------------------


def _scheduler(mocker):
    manager = mocker.MagicMock()
    scheduler = BackendScheduler(manager, queue_key="queue:{spider}")
    return scheduler, manager, SimpleNamespace(name="coverage-repair", crawler=None)


def test_scheduler_async_open_warmup_queue_and_close_are_ordered(monkeypatch, mocker):
    scheduler, manager, spider = _scheduler(mocker)
    workers: list[tuple[Deferred[object], object]] = []

    class Queue:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def close(self, **_kwargs):
            return None

    def submit(function, *_args, **_kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function))
        return worker

    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "deferToThread", submit)
    monkeypatch.setattr(scheduler_module, "BackendQueue", Queue)
    manager.get_queue_backend.return_value = object()
    returned = scheduler.open(spider)
    warmup, warmup_fn = workers.pop(0)
    warmup_fn()
    warmup.callback(None)
    construct, construct_fn = workers.pop(0)
    queue = construct_fn()
    construct.callback(queue)
    assert returned.called
    assert scheduler._lifecycle_state == scheduler_module._LIFECYCLE_OPEN
    assert scheduler._queue is queue

    closed = scheduler.close("done")
    queue_worker, queue_fn = workers.pop(0)
    queue_fn()
    queue_worker.callback(None)
    release_worker, release_fn = workers.pop(0)
    release_fn()
    release_worker.callback(None)
    assert closed.called
    manager.close.assert_called_once_with()


def test_scheduler_async_open_failure_rolls_back_manager_and_preserves_error(
    monkeypatch, mocker
):
    scheduler, manager, spider = _scheduler(mocker)
    workers: list[tuple[Deferred[object], object]] = []

    def submit(function, *_args, **_kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function))
        return worker

    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "deferToThread", submit)
    manager.get_queue_backend.return_value = object()
    mocker.patch.object(
        scheduler_module, "BackendQueue", side_effect=RuntimeError("queue build")
    )
    returned = scheduler.open(spider)
    warmup, warmup_fn = workers.pop(0)
    warmup_fn()
    warmup.callback(None)
    queue_worker, _queue_fn = workers.pop(0)
    queue_worker.errback(RuntimeError("queue build"))
    release_worker, release_fn = workers.pop(0)
    release_fn()
    release_worker.callback(None)
    errors = _errors(returned)
    assert errors and str(errors[0]) == "queue build"
    assert scheduler._lifecycle_state == scheduler_module._LIFECYCLE_CLOSED
    manager.close.assert_called_once_with()


def test_scheduler_open_submission_and_callback_attachment_failures_reset_state(
    monkeypatch, mocker
):
    scheduler, manager, spider = _scheduler(mocker)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(
        scheduler_module,
        "deferToThread",
        lambda *_a, **_k: (_ for _ in ()).throw(KeyboardInterrupt("pool closed")),
    )
    returned = scheduler.open(spider)
    errors = _errors(returned)
    assert errors and isinstance(errors[0], KeyboardInterrupt)
    assert scheduler._lifecycle_state == scheduler_module._LIFECYCLE_NEW
    assert scheduler._spider is None
    manager.close.assert_not_called()

    scheduler, manager, spider = _scheduler(mocker)
    scheduler._lifecycle_state = scheduler_module._LIFECYCLE_OPENING
    scheduler._spider = spider
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "BackendQueue", lambda **_k: object())
    worker = _AlwaysRejectingDeferred()
    monkeypatch.setattr(scheduler_module, "deferToThread", lambda *_a, **_k: worker)
    result = scheduler._finish_open(spider)
    assert isinstance(result, Deferred)
    errors = _errors(result)
    assert errors and isinstance(errors[0], KeyboardInterrupt)


def test_scheduler_real_dupefilter_adapter_failure_stays_below_open_authority(
    monkeypatch, mocker
):
    scheduler, manager, spider = _scheduler(mocker)
    scheduler.dupefilter = BackendDupeFilter(
        None,
        membership_filter=MemoryMembershipFilter(),
    )
    scheduler._owns_dupefilter = True
    manager.get_queue_backend.return_value = object()
    adapter_jobs: list[tuple[Deferred[object], object]] = []
    scheduler_jobs: list[tuple[Deferred[object], object]] = []

    def adapter_submit(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        adapter_jobs.append((worker, lambda: function(*args, **kwargs)))
        return worker

    def scheduler_submit(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        scheduler_jobs.append((worker, lambda: function(*args, **kwargs)))
        return worker

    class Queue:
        def __init__(self, **_kwargs):
            pass

    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "deferToThread", scheduler_submit)
    monkeypatch.setattr(scheduler_module, "BackendQueue", Queue)
    monkeypatch.setattr(reactor_module, "_reactor", _ControlledReactor)
    monkeypatch.setattr(reactor_module, "deferToThread", adapter_submit)
    _SecondAddBothRejectingDeferred.created = 0
    monkeypatch.setattr(reactor_module, "Deferred", _SecondAddBothRejectingDeferred)

    returned = scheduler.open(spider)
    errors = _errors(returned)
    assert not returned.called
    assert not errors
    assert len(adapter_jobs) == 1

    adapter_worker, adapter_function = adapter_jobs.pop()
    adapter_function()
    adapter_worker.callback(None)
    warmup_worker, warmup_function = scheduler_jobs.pop()
    warmup_function()
    warmup_worker.callback(None)
    queue_worker, queue_function = scheduler_jobs.pop()
    queue = queue_function()
    queue_worker.callback(queue)

    assert returned.called
    assert not errors
    assert scheduler._lifecycle_state == scheduler_module._LIFECYCLE_OPEN
    assert scheduler._queue is queue


def test_scheduler_real_dupefilter_close_failure_retains_authority(monkeypatch, mocker):
    scheduler, manager, spider = _scheduler(mocker)
    dupefilter = BackendDupeFilter(
        None,
        membership_filter=MemoryMembershipFilter(),
    )
    dupefilter.close = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("filter close")
    )
    scheduler.dupefilter = dupefilter
    scheduler._owns_dupefilter = True
    scheduler._spider = spider
    scheduler._queue_terminal = True
    scheduler._lifecycle_state = scheduler_module._LIFECYCLE_OPEN
    adapter_jobs: list[tuple[Deferred[object], object]] = []
    release_jobs: list[tuple[Deferred[object], object]] = []

    def adapter_submit(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        adapter_jobs.append((worker, lambda: function(*args, **kwargs)))
        return worker

    def release_submit(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        release_jobs.append((worker, lambda: function(*args, **kwargs)))
        return worker

    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "deferToThread", release_submit)
    monkeypatch.setattr(reactor_module, "_reactor", _ControlledReactor)
    monkeypatch.setattr(reactor_module, "deferToThread", adapter_submit)

    returned = scheduler.close("filter-close")
    errors = _errors(returned)
    assert not returned.called
    assert len(adapter_jobs) == 1
    worker, function = adapter_jobs.pop()
    with pytest.raises(RuntimeError, match="filter close"):
        function()
    worker.errback(RuntimeError("filter close"))
    assert errors and str(errors[0]) == "filter close"
    assert scheduler.dupefilter is dupefilter
    assert scheduler._lifecycle_state == scheduler_module._LIFECYCLE_CLOSING
    manager.close.assert_not_called()

    dupefilter.close.side_effect = None
    retry = scheduler.close("filter-retry")
    retry_errors = _errors(retry)
    worker, function = adapter_jobs.pop() if adapter_jobs else (None, None)
    assert worker is not None and function is not None
    function()
    worker.callback(None)
    release_worker, release_function = release_jobs.pop()
    release_function()
    release_worker.callback(None)
    assert retry_errors == []
    assert scheduler._lifecycle_state == scheduler_module._LIFECYCLE_CLOSED
    manager.close.assert_called_once_with()


def test_scheduler_close_queue_dupefilter_and_baseexception_outcomes(
    monkeypatch, mocker
):
    scheduler, manager, spider = _scheduler(mocker)
    queue = MagicMock()
    scheduler._queue = queue
    scheduler._spider = spider
    scheduler._lifecycle_state = scheduler_module._LIFECYCLE_OPEN
    workers: list[tuple[Deferred[object], object]] = []

    def submit(function, *_args, **_kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function))
        return worker

    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "deferToThread", submit)
    returned = scheduler.close("queue-failure")
    queue_worker, _queue_fn = workers.pop(0)
    queue_worker.errback(RuntimeError("queue failure"))
    release_worker, release_fn = workers.pop(0)
    release_fn()
    release_worker.callback(None)
    assert returned.called
    assert scheduler._lifecycle_state == scheduler_module._LIFECYCLE_CLOSED
    manager.close.assert_called_once_with()

    scheduler, manager, spider = _scheduler(mocker)
    dupefilter = BackendDupeFilter(MagicMock())
    close_worker: Deferred[object] = Deferred()
    dupefilter._release_authoritative_async = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("adapter rejected")
    )
    scheduler.dupefilter = dupefilter
    scheduler._owns_dupefilter = True
    scheduler._spider = spider
    scheduler._lifecycle_state = scheduler_module._LIFECYCLE_OPEN
    scheduler._queue_terminal = True
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    returned = scheduler.close("filter-failure")
    errors = _errors(returned)
    assert errors and str(errors[0]) == "adapter rejected"
    assert scheduler.dupefilter is dupefilter
    assert not manager.close.called
    del close_worker


def test_scheduler_close_callback_attachment_failure_releases_attempt_owner(
    monkeypatch, mocker
):
    scheduler, _manager, _spider = _scheduler(mocker)
    operation = _AlwaysRejectingDeferred()
    bounded: Deferred[object] = Deferred()
    scheduler._close_locked = MagicMock(  # type: ignore[method-assign]
        return_value=scheduler_module._DeferredLifecycleResult(operation, bounded)
    )
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    returned = scheduler.close("attachment")
    assert isinstance(returned, Deferred)
    errors = _errors(returned)
    assert errors and isinstance(errors[0], RuntimeError)
    assert scheduler._close_attempt_owner is None


# Spider composite ownership ----------------------------------------------


def test_mixin_signal_leases_and_dispatcher_retry_are_exact(mocker):
    spider = _mixin(mocker)

    class Signals:
        def __init__(self):
            self.connect_results = [None, RuntimeError("second connect")]
            self.disconnect_results = [
                KeyboardInterrupt("rollback"),
                KeyboardInterrupt("rollback"),
            ]

        def connect(self, _handler, signal=None):
            del signal
            result = self.connect_results.pop(0)
            if isinstance(result, BaseException):
                raise result

        def disconnect(self, _handler, signal=None):
            del signal
            result = self.disconnect_results.pop(0)
            if isinstance(result, BaseException):
                raise result

    signals = Signals()
    spider.crawler = SimpleNamespace(signals=signals)
    with pytest.raises(RuntimeError, match="second connect"):
        spider._connect_signals()
    assert len(spider._signal_leases) == 2
    signals.disconnect_results = [KeyboardInterrupt("disconnect"), None, None]
    with pytest.raises(KeyboardInterrupt):
        spider._disconnect_lifecycle_signals(signals, strict=True)
    spider._disconnect_lifecycle_signals(signals, strict=True)
    assert spider._signal_leases == []


def test_mixin_component_and_owner_deferred_cleanup_retain_leases(mocker, monkeypatch):
    spider = _mixin(mocker)
    candidate = MagicMock()
    candidate.close.return_value = Deferred()
    lease = MagicMock()
    lease.release.return_value = Deferred()
    candidate._connection_manager_lease = lease
    spider._orphan_candidates = [("scheduler", candidate)]
    spider._orphan_leases = [lease]
    result = spider._cleanup_orphan_candidates("retry")
    assert isinstance(result, Deferred)
    candidate_close = candidate.close.return_value
    candidate_close.callback(None)
    lease.release.return_value.callback(None)
    assert result.called
    assert spider._orphan_candidates == []
    assert spider._orphan_leases == []

    class Owner(ConnectionManager):
        pass

    owner = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    owner.close = MagicMock(return_value=None)  # type: ignore[method-assign]
    operation = Deferred()
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(spider_module, "deferToThread", lambda *_a, **_k: operation)
    started, immediate, succeeded = spider._start_owner_operation(owner, "close")
    assert started is operation and immediate is None and not succeeded
    worker_result = started
    worker_result.callback((True, None))
    del Owner


def test_mixin_orphan_and_candidate_baseexception_cleanup_is_retryable(mocker):
    spider = _mixin(mocker)
    candidate = MagicMock()
    candidate.close.side_effect = KeyboardInterrupt("candidate close")
    lease = MagicMock()
    lease.release.side_effect = KeyboardInterrupt("lease release")
    manager = MagicMock()
    manager.close.side_effect = KeyboardInterrupt("manager close")
    spider._orphan_candidates = [("queue", candidate)]
    spider._orphan_leases = [lease]
    spider._orphan_managers = [manager]
    error = spider._cleanup_orphan_candidates("retry")
    assert isinstance(error, KeyboardInterrupt)
    assert (
        spider._orphan_candidates and spider._orphan_leases and spider._orphan_managers
    )


def test_mixin_async_close_waits_for_all_sibling_authorities(mocker, monkeypatch):
    spider = _mixin(mocker)
    manager = spider._connection_manager
    scheduler = MagicMock()
    scheduler._reactor_io_timeout = 1
    scheduler_close: Deferred[object] = Deferred()
    scheduler.close.return_value = scheduler_close
    scheduler.dupefilter = object()
    queue = MagicMock()
    queue_close: Deferred[object] = Deferred()
    queue.close.return_value = queue_close
    dupefilter = BackendDupeFilter(MagicMock())
    dupe_close: Deferred[object] = Deferred()
    dupefilter._close_authoritative_async = MagicMock(  # type: ignore[method-assign]
        return_value=(dupe_close, Deferred())
    )
    snapshot = MagicMock()
    snapshot_release: Deferred[object] = Deferred()
    snapshot.release.return_value = snapshot_release
    queue_lease = MagicMock()
    queue_release: Deferred[object] = Deferred()
    queue_lease.release.return_value = queue_release
    spider._scheduler = scheduler
    spider._queue = queue
    spider._dupefilter = dupefilter
    spider._snapshot_connection_manager = snapshot
    spider._snapshot_connection_lease = snapshot
    spider._queue_connection_manager = MagicMock()
    spider._queue_connection_lease = queue_lease
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(reactor_module, "_reactor", _ControlledReactor)
    returned = spider.close_backend()
    scheduler_close.callback(None)
    queue_close.callback(None)
    dupe_close.callback(None)
    snapshot_release.callback(None)
    queue_release.callback(None)
    assert returned.called
    manager.close.assert_called_once_with()
    assert spider._scheduler is None and spider._queue is None
    assert spider._dupefilter is None


def test_mixin_dupefilter_adapter_rejection_keeps_manager_fenced(mocker, monkeypatch):
    manager = MagicMock()
    spider = _mixin(mocker)
    spider._connection_manager = manager
    dupefilter = BackendDupeFilter(
        None,
        membership_filter=MemoryMembershipFilter(),
    )
    spider._dupefilter = dupefilter
    workers: list[tuple[Deferred[object], object]] = []

    def submit(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, lambda: function(*args, **kwargs)))
        return worker

    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(reactor_module, "_reactor", _ControlledReactor)
    monkeypatch.setattr(reactor_module, "deferToThread", submit)
    monkeypatch.setattr(reactor_module, "Deferred", _ObserverRejectingDeferred)

    returned = spider.close_backend()
    assert isinstance(returned, Deferred)
    assert not returned.called
    assert not manager.close.called
    assert spider._dupefilter is dupefilter
    assert len(workers) == 1

    worker, function = workers.pop()
    function()
    worker.callback(None)
    assert returned.called
    assert manager.close.call_count == 1
    assert spider._dupefilter is None
    assert spider._connection_manager is None


def test_mixin_close_adapter_baseexception_preserves_dupefilter_and_manager(
    mocker, monkeypatch
):
    spider = _mixin(mocker)
    dupefilter = BackendDupeFilter(MagicMock())
    interrupt = KeyboardInterrupt("adapter rejected")
    dupefilter._close_authoritative_async = MagicMock(  # type: ignore[method-assign]
        side_effect=interrupt
    )
    spider._dupefilter = dupefilter
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    with pytest.raises(KeyboardInterrupt) as raised:
        spider.close_backend()
    assert raised.value is interrupt
    assert spider._dupefilter is dupefilter
    assert not spider._connection_manager.close.called


def test_mixin_ack_signal_dispatcher_absence_and_async_component_tracking(
    mocker, monkeypatch
):
    spider = _mixin(mocker)
    signal_manager = MagicMock()
    signal_manager.disconnect.side_effect = DispatcherKeyError("gone")
    spider._signal_leases = []
    spider._connected_signals = signal_manager
    spider._signals_connected = True
    spider._disconnect_lifecycle_signals(signal_manager, strict=True)
    assert spider._signals_connected is False

    operation = _AttachFailureDeferred(("addBoth",))
    spider._track_async_operation_locked("op", operation)
    operation.callback(None)
    assert not spider._async_component_operations
    del monkeypatch


def test_reactor_already_settled_and_bounded_timer_failure_paths(monkeypatch):
    settled = Deferred()
    settled.callback(None)
    reactor_module._safe_callback(settled, None)
    reactor_module._safe_errback(settled, RuntimeError("already settled"))

    class NoTimer:
        def callLater(self, _delay, _callback):
            raise RuntimeError("timer unavailable")

    source = Deferred()
    monkeypatch.setattr(reactor_module, "_reactor", lambda: NoTimer())
    bounded = reactor_module.bounded_deferred(
        source, timeout=1, operation="bounded-timer"
    )
    errors = _errors(bounded)
    assert errors and str(errors[0]) == "timer unavailable"

    source = Deferred()
    timer = MagicMock()
    timer.active.return_value = True
    timer.cancel.side_effect = KeyboardInterrupt("cancel rejected")

    class WorkingTimer:
        def callLater(self, _delay, _callback):
            return timer

    monkeypatch.setattr(reactor_module, "_reactor", lambda: WorkingTimer())
    bounded = reactor_module.bounded_deferred(
        source, timeout=1, operation="bounded-cancel"
    )
    source.callback(None)
    assert bounded.called


def test_scheduler_submission_and_factory_rollback_observers_are_retryable(
    mocker,
):
    def reject(*_args, **_kwargs):
        raise KeyboardInterrupt("scheduler pool closed")

    mocker.patch.object(scheduler_module, "deferToThread", reject)
    failed = scheduler_module._submit_thread(lambda: None)
    errors = _errors(failed)
    assert errors and isinstance(errors[0], KeyboardInterrupt)

    scheduler, manager, _spider = _scheduler(mocker)
    cleanup = Deferred()
    scheduler._observe_factory_cleanup(
        cleanup,
        on_failure=lambda _failure: (_ for _ in ()).throw(
            KeyboardInterrupt("observer failed")
        ),
    )
    cleanup.errback(RuntimeError("cleanup"))
    manager.close.assert_called_once_with()


def test_pipeline_item_observer_rejection_keeps_worker_ahead_of_close(
    monkeypatch, mocker
):
    """A rejected public observer must not release the item's manager early."""
    workers: list[tuple[Deferred[object], object]] = []

    def submit(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, lambda: function(*args, **kwargs)))
        return worker

    controlled_reactor = _ControlledReactor()
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(reactor_module, "_reactor", lambda: controlled_reactor)
    monkeypatch.setattr(reactor_module, "deferToThread", submit)
    monkeypatch.setattr(reactor_module, "Deferred", _ObserverRejectingDeferred)

    manager = MagicMock()
    pipeline = BackendPipeline(manager)
    pipeline._opened = True
    spider = SimpleNamespace(name="coverage-repair", crawler=None)
    mocker.patch.object(
        pipeline,
        "_process_item_sync",
        side_effect=lambda item, _spider: item,
    )

    item_result = pipeline.process_item({"value": 1}, spider)
    item_errors = _errors(item_result)
    assert item_errors and isinstance(item_errors[0], RuntimeError)
    assert len(workers) == 1
    assert not workers[0][0].called

    close_result = pipeline.close_spider(spider)
    close_errors = _errors(close_result)
    assert not close_errors
    assert not manager.close.called
    assert len(workers) == 1

    store_worker, store_function = workers.pop(0)
    store_function()
    store_worker.callback({"value": 1})
    assert len(workers) == 1
    assert not manager.close.called

    close_worker, close_function = workers.pop(0)
    close_function()
    close_worker.callback(None)
    assert manager.close.call_count == 1
    assert pipeline._closed


def test_pipeline_open_worker_already_settled_and_close_observer_retries(
    monkeypatch,
):
    pipeline = BackendPipeline(MagicMock())
    spider = SimpleNamespace(name="coverage-repair", crawler=None)
    worker = _AlwaysRejectingDeferred()
    worker.callback(None)
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(pipeline_module, "deferToThread", lambda *_a, **_k: worker)
    returned = pipeline.open_spider(spider)
    assert returned.called
    assert not pipeline._opening

    pipeline = BackendPipeline(MagicMock())
    pipeline._opened = True
    operation = _ErrbackFailureDeferred()
    bounded = _AttachFailureDeferred(("addCallbacks",))
    monkeypatch.setattr(
        pipeline_module,
        "defer_to_thread_ordered",
        lambda *_a, **_k: (operation, bounded),
    )
    returned = pipeline.close_spider(spider)
    operation.callback(None)
    bounded.callback(None)
    assert returned.called


def test_scheduler_open_generic_cleanup_and_close_authority_failure(
    mocker, monkeypatch
):
    scheduler, manager, spider = _scheduler(mocker)

    class GenericFilter:
        def __init__(self):
            self.open_result = Deferred()

        def open(self):
            return self.open_result

        def close(self, _reason):
            return None

    dupefilter = GenericFilter()
    scheduler.dupefilter = dupefilter
    scheduler._owns_dupefilter = True
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: False)
    mocker.patch.object(
        scheduler, "_finish_open", side_effect=RuntimeError("open finish")
    )
    returned = scheduler.open(spider)
    dupefilter.open_result.callback(None)
    errors = _errors(returned)
    assert errors and str(errors[0]) == "open finish"
    assert scheduler._queue is None

    scheduler, manager, spider = _scheduler(mocker)
    scheduler._queue_terminal = True
    scheduler._spider = spider
    scheduler._lifecycle_state = scheduler_module._LIFECYCLE_OPEN
    dupefilter = MagicMock()
    close_result = _AttachFailureDeferred(("addCallbacks",))
    dupefilter.close.return_value = close_result
    scheduler.dupefilter = dupefilter
    scheduler._owns_dupefilter = True
    returned = scheduler.close("generic-close")
    close_result.callback(None)
    assert returned is not None
    assert manager.close.call_count == 1


def test_mixin_construction_wait_and_candidate_failure_keep_manager_owned(
    mocker, monkeypatch
):
    spider = _mixin(mocker)
    construction = threading.Event()
    operation = Deferred()
    workers: list[tuple[Deferred[object], object]] = []

    def submit(function, *_args, **_kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function))
        return worker

    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(spider_module, "deferToThread", submit)
    public = spider._close_after_construction_wait(
        (construction,), (operation,), threading.get_ident()
    )
    assert isinstance(public, Deferred)
    construction.set()
    wait_worker, wait_fn = workers.pop(0)
    wait_fn()
    wait_worker.callback(None)
    operation.callback(None)
    assert public.called
    assert spider._close_in_progress is False

    candidate = MagicMock()
    candidate.close.side_effect = RuntimeError("candidate")
    spider._orphan_candidates = [("scheduler", candidate)]
    error = spider._cleanup_orphan_candidates("retry")
    assert isinstance(error, RuntimeError)
    assert spider._orphan_candidates == [("scheduler", candidate)]


def test_mixin_async_close_queue_failure_preserves_component_and_manager_lease(
    mocker, monkeypatch
):
    spider = _mixin(mocker)
    scheduler = MagicMock()
    scheduler._reactor_io_timeout = 1
    scheduler_close = Deferred()
    scheduler.close.return_value = scheduler_close
    queue = MagicMock()
    queue_close = Deferred()
    queue.close.return_value = queue_close
    spider._scheduler = scheduler
    spider._queue = queue
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(reactor_module, "_reactor", _ControlledReactor)
    returned = spider.close_backend()
    scheduler_close.callback(None)
    queue_close.errback(RuntimeError("queue sibling"))
    errors = _errors(returned)
    assert errors and str(errors[0]) == "queue sibling"
    assert spider._queue is queue
    assert spider._connection_manager is not None
    spider._queue = None
    spider._scheduler = None
    spider.close_backend()


def test_mixin_async_close_dupefilter_snapshot_and_queue_lease_failures_retry(
    mocker, monkeypatch
):
    spider = _mixin(mocker)
    scheduler = MagicMock()
    scheduler._reactor_io_timeout = 1
    scheduler_close = Deferred()
    scheduler.close.return_value = scheduler_close
    dupefilter = BackendDupeFilter(MagicMock())
    dupe_close = Deferred()
    dupefilter._close_authoritative_async = MagicMock(  # type: ignore[method-assign]
        return_value=(dupe_close, Deferred())
    )
    spider._scheduler = scheduler
    spider._dupefilter = dupefilter
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(reactor_module, "_reactor", _ControlledReactor)
    returned = spider.close_backend()
    scheduler_close.callback(None)
    dupe_close.errback(RuntimeError("dupefilter close"))
    errors = _errors(returned)
    assert errors and str(errors[0]) == "dupefilter close"
    assert spider._dupefilter is dupefilter

    # A separate retry exercises the exact snapshot-acquire failure fence.
    spider = _mixin(mocker)
    snapshot = MagicMock()
    snapshot_release = Deferred()
    snapshot.release.return_value = snapshot_release
    spider._snapshot_connection_manager = snapshot
    spider._snapshot_connection_lease = snapshot
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    returned = spider.close_backend()
    snapshot_release.errback(RuntimeError("snapshot release"))
    errors = _errors(returned)
    assert errors and str(errors[0]) == "snapshot release"
    assert spider._snapshot_connection_lease is snapshot


def test_mixin_parent_release_failure_is_authoritative_and_retryable(
    mocker, monkeypatch
):
    spider = _mixin(mocker)
    manager = spider._connection_manager
    parent_release = Deferred()
    manager.close.return_value = parent_release
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(reactor_module, "_reactor", _ControlledReactor)
    returned = spider.close_backend()
    parent_release.errback(RuntimeError("parent release"))
    errors = _errors(returned)
    assert errors and str(errors[0]) == "parent release"
    assert spider._connection_manager is manager
    manager.close.return_value = None
    spider.close_backend()
    assert spider._connection_manager is None


def test_mixin_scheduler_control_failure_keeps_scheduler_for_retry(mocker):
    spider = _mixin(mocker)
    scheduler = MagicMock()
    first = Deferred()
    second = Deferred()
    scheduler.close.side_effect = [first, second]
    spider._scheduler = scheduler
    returned = spider.close_backend()
    first.errback(KeyboardInterrupt("scheduler interrupt"))
    errors = _errors(returned)
    assert errors and isinstance(errors[0], KeyboardInterrupt)
    assert spider._scheduler is scheduler
    retry = spider.close_backend()
    second.callback(None)
    assert retry.called
    assert spider._scheduler is None


def test_scheduler_settlement_observer_rejection_keeps_worker_authority(
    monkeypatch, mocker
):
    queue = MagicMock()
    scheduler = BackendScheduler(mocker.MagicMock(), reactor_io_timeout=1)
    scheduler._queue = queue
    request = Request("https://example.test")
    token = object()
    request.meta[scheduler_module.BACKEND_ACK_TOKEN_META_KEY] = token
    worker: Deferred[object] = Deferred()
    jobs: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    def submit(function, *args, **kwargs):
        jobs.append((function, args, kwargs))
        return worker

    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(reactor_module, "_reactor", _ControlledReactor)
    monkeypatch.setattr(reactor_module, "deferToThread", submit)
    monkeypatch.setattr(reactor_module, "Deferred", _AlwaysRejectingDeferred)

    result = scheduler._on_response_received(None, request, None)
    assert isinstance(result, Deferred)
    errors = _errors(result)
    assert errors and isinstance(errors[0], KeyboardInterrupt)
    assert request.meta[scheduler_module.BACKEND_ACK_TOKEN_META_KEY] is token
    assert not queue.ack.called

    function, args, kwargs = jobs.pop()
    function(*args, **kwargs)
    worker.callback(None)
    queue.ack.assert_called_once_with(token=token)
    assert scheduler_module.BACKEND_ACK_TOKEN_META_KEY not in request.meta


# Exact scheduler token outcomes are still part of the lifecycle contract.
def test_scheduler_ack_and_nack_keep_token_on_backend_failure(mocker):
    scheduler = BackendScheduler(mocker.MagicMock())
    queue = MagicMock()
    scheduler._queue = queue
    request = Request("https://example.test")
    token = object()
    request.meta[scheduler_module.BACKEND_ACK_TOKEN_META_KEY] = token
    queue.ack.side_effect = BackendError("ack")
    scheduler._on_response_received(None, request, None)
    assert request.meta[scheduler_module.BACKEND_ACK_TOKEN_META_KEY] is token
    queue.nack.side_effect = BackendError("nack")
    scheduler._on_spider_error(
        Failure(RuntimeError("failure")), SimpleNamespace(request=request), None
    )
    assert request.meta[scheduler_module.BACKEND_ACK_TOKEN_META_KEY] is token
