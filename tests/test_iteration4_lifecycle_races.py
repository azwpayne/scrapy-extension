"""Iteration-4 lifecycle-race regressions."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from scrapy import Request
from twisted.internet.defer import Deferred, succeed
from twisted.python.failure import Failure

import scrapy_extension.queue.queue as queue_module
import scrapy_extension.schedule.scheduler as scheduler_module
import scrapy_extension.utils.reactor as reactor_module
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.exceptions import (
    BackendError,
    BackendOperationTimeout,
    QueueError,
)
from scrapy_extension.queue.queue import BACKEND_ACK_TOKEN_META_KEY, BackendQueue
from scrapy_extension.queue.strategies.base import (
    QueueStrategy,
    _PreparedQueuePush,
)
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings import DynamoDBSettings


def _raise_continuation(_value):
    raise RuntimeError("continuation failed")


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


class _DurableStrategy(QueueStrategy):
    def _prepare_push(
        self,
        queue_name: str,
        *,
        priority: float = 0.0,
        delay: float = 0.0,
        source: str = "default",
    ) -> _PreparedQueuePush:
        del queue_name, priority, delay, source
        return _PreparedQueuePush(
            backend_route=True,
            _commit=lambda _item, _require_durable: True,
        )

    def push(self, queue_name, item, *, priority=0.0, delay=0.0, source="default"):
        del queue_name, item, priority, delay, source

    def pop(self, queue_name, timeout=0.0):
        del queue_name, timeout
        return None

    def queue_len(self, queue_name):
        del queue_name
        return 0

    def clear(self, queue_name):
        del queue_name


def _patch_ordered(monkeypatch, *, reactor: _FakeReactor | None = None):
    calls: list[tuple[Deferred[object], Deferred[object], object, tuple, dict]] = []

    def ordered(function, *args, **kwargs):
        call_kwargs = dict(kwargs)
        call_kwargs.pop("timeout", None)
        call_kwargs.pop("operation", None)
        operation: Deferred[object] = Deferred()
        bounded: Deferred[object] = Deferred()
        calls.append((operation, bounded, function, args, call_kwargs))
        return operation, bounded

    if reactor is not None:
        monkeypatch.setattr(reactor_module, "_reactor", lambda: reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(queue_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "defer_to_thread_ordered", ordered)
    monkeypatch.setattr(queue_module, "defer_to_thread_ordered", ordered)
    return calls


@pytest.mark.parametrize("settlement", ["ack", "nack"])
def test_scheduler_timeout_late_success_removes_only_same_request_token(
    monkeypatch,
    settlement: str,
) -> None:
    calls = _patch_ordered(monkeypatch)
    scheduler = BackendScheduler(MagicMock())
    scheduler._queue = MagicMock()
    token = object()
    request = SimpleNamespace(meta={BACKEND_ACK_TOKEN_META_KEY: token})

    result = (
        scheduler._ack_request_token(request, log_message="ack")
        if settlement == "ack"
        else scheduler._nack_request_token(request, log_message="nack")
    )
    assert isinstance(result, Deferred)
    calls[0][1].errback(BackendOperationTimeout(settlement, 1.0))
    assert request.meta[BACKEND_ACK_TOKEN_META_KEY] is token

    # A retry/replacement may have taken ownership while the worker was late.
    replacement = object()
    request.meta[BACKEND_ACK_TOKEN_META_KEY] = replacement
    calls[0][2](*calls[0][3], **calls[0][4])
    calls[0][0].callback(None)
    assert request.meta[BACKEND_ACK_TOKEN_META_KEY] is replacement


def test_lifecycle_chain_flattens_deferred_wrappers_and_preserves_failures() -> None:
    source: Deferred[object] = Deferred()
    nested: Deferred[object] = Deferred()
    chained = scheduler_module._chain_lifecycle_result(
        source,
        lambda _value: scheduler_module._DeferredLifecycleResult(nested, succeed(None)),
    )
    source.callback(None)
    assert not nested.called
    nested.callback(None)
    assert chained.called

    direct = scheduler_module._chain_lifecycle_result(
        succeed(None), lambda _value: succeed(None)
    )
    assert direct.called
    same_view = Deferred()
    flattened_same_view = scheduler_module._chain_lifecycle_result(
        succeed(None),
        lambda _value: scheduler_module._DeferredLifecycleResult(same_view, same_view),
    )
    same_view.callback(None)
    assert flattened_same_view.called
    immediate = scheduler_module._chain_lifecycle_result(
        succeed(None), lambda _value: None
    )
    assert immediate.called

    original = Failure(ValueError("original"))
    preserved: list[BaseException] = []
    source = Deferred()
    nested = Deferred()
    chained = scheduler_module._chain_lifecycle_result(
        source,
        lambda _value: scheduler_module._DeferredLifecycleResult(nested, succeed(None)),
        preserve_failure=original,
    )
    chained.addErrback(lambda failure: preserved.append(failure.value))
    source.callback(None)
    nested.callback(None)
    assert preserved == [original.value]

    preserved = []
    immediate = scheduler_module._chain_lifecycle_result(
        succeed(None), lambda _value: None, preserve_failure=original
    )
    immediate.addErrback(lambda failure: preserved.append(failure.value))
    assert preserved == [original.value]

    preserved = []
    raised = scheduler_module._chain_lifecycle_result(
        succeed(None), _raise_continuation, preserve_failure=original
    )
    raised.addErrback(lambda failure: preserved.append(failure.value))
    assert preserved == [original.value]

    failures: list[BaseException] = []
    raised = scheduler_module._chain_lifecycle_result(
        succeed(None), _raise_continuation
    )
    raised.addErrback(lambda failure: failures.append(failure.value))
    assert str(failures[0]) == "continuation failed"


def test_dynamodb_stale_candidate_closes_without_publication(monkeypatch) -> None:
    backend = DynamoDBBackend(DynamoDBSettings())
    resource = MagicMock()
    candidate = SimpleNamespace(resource=resource)

    def build_candidate(*_args, **_kwargs):
        backend._generation = object()
        return candidate

    monkeypatch.setattr(backend, "_build_candidate", build_candidate)
    backend.connect()

    resource.meta.client.close.assert_called_once_with()


def test_scheduler_async_settlement_without_request_callback(monkeypatch) -> None:
    calls = _patch_ordered(monkeypatch)
    scheduler = BackendScheduler(MagicMock())
    scheduler._queue = MagicMock()

    result = scheduler._settle_token_async(object(), negative=False, log_message="ack")
    assert isinstance(result, Deferred)
    calls[0][0].callback(None)
    calls[0][1].callback(None)
    assert result.called


def test_scheduler_timeout_late_success_removes_original_token(monkeypatch) -> None:
    calls = _patch_ordered(monkeypatch)
    scheduler = BackendScheduler(MagicMock())
    scheduler._queue = MagicMock()
    token = object()
    request = SimpleNamespace(meta={BACKEND_ACK_TOKEN_META_KEY: token})

    scheduler._ack_request_token(request, log_message="ack")
    calls[0][1].errback(BackendOperationTimeout("scheduler ack", 1.0))
    calls[0][0].callback(None)

    assert BACKEND_ACK_TOKEN_META_KEY not in request.meta


def test_scheduler_late_failure_keeps_token_and_reports_once(monkeypatch) -> None:
    calls = _patch_ordered(monkeypatch)
    scheduler = BackendScheduler(MagicMock())
    scheduler._queue = MagicMock()
    scheduler.stats = MagicMock()
    token = object()
    request = SimpleNamespace(meta={BACKEND_ACK_TOKEN_META_KEY: token})

    scheduler._ack_request_token(request, log_message="ack")
    calls[0][1].errback(BackendOperationTimeout("scheduler ack", 1.0))
    calls[0][0].errback(BackendError("late failure"))

    assert request.meta[BACKEND_ACK_TOKEN_META_KEY] is token
    scheduler.stats.inc_value.assert_called_once_with("scheduler/ack_error")


def test_committed_replacement_timeout_late_success_removes_exact_token(
    monkeypatch,
) -> None:
    calls = _patch_ordered(monkeypatch)
    manager = MagicMock()
    queue = BackendQueue(manager, "q", queue_strategy=_DurableStrategy(manager))
    token = object()
    request = Request(
        "https://example.test/replacement", meta={BACKEND_ACK_TOKEN_META_KEY: token}
    )

    queue.push(request)
    calls[0][1].errback(BackendOperationTimeout("queue replacement ack", 1.0))
    calls[0][2](*calls[0][3], **calls[0][4])
    calls[0][0].callback(None)

    assert BACKEND_ACK_TOKEN_META_KEY not in request.meta


def test_committed_replacement_late_failure_keeps_token_and_reports_once(
    monkeypatch,
) -> None:
    calls = _patch_ordered(monkeypatch)
    manager = MagicMock()
    queue = BackendQueue(manager, "q", queue_strategy=_DurableStrategy(manager))
    queue._record_replacement_ack_failure = MagicMock()
    token = object()
    request = Request(
        "https://example.test/replacement", meta={BACKEND_ACK_TOKEN_META_KEY: token}
    )

    queue.push(request)
    calls[0][1].errback(BackendOperationTimeout("queue replacement ack", 1.0))
    calls[0][0].errback(BackendError("late failure"))

    assert request.meta[BACKEND_ACK_TOKEN_META_KEY] is token
    queue._record_replacement_ack_failure.assert_called_once_with()


def _open_scheduler(monkeypatch, *, dupefilter=None):
    fake_reactor = _FakeReactor()
    monkeypatch.setattr(reactor_module, "_reactor", lambda: fake_reactor)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    workers: list[tuple[Deferred[object], object, tuple, dict]] = []

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function, args, kwargs))
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", thread)
    queue = MagicMock()
    monkeypatch.setattr(scheduler_module, "BackendQueue", lambda **_kwargs: queue)
    manager = MagicMock()
    scheduler = BackendScheduler(
        manager,
        dupefilter=dupefilter,
        reactor_io_timeout=1.0,
    )
    opened = scheduler.open(SimpleNamespace(name="open-race", crawler=None))
    assert isinstance(opened, Deferred)
    return fake_reactor, workers, queue, manager, scheduler, opened


def _settle_workers(workers, start: int = 0) -> None:
    index = start
    while index < len(workers):
        worker, function, args, kwargs = workers[index]
        function(*args, **kwargs)
        worker.callback(None)
        index += 1


def test_async_dupefilter_failure_bridges_authoritative_cleanup(monkeypatch) -> None:
    dupefilter = MagicMock()
    dupefilter_open: Deferred[object] = Deferred()
    dupefilter.open.return_value = dupefilter_open
    _fake_reactor, workers, _queue, manager, scheduler, opened = _open_scheduler(
        monkeypatch, dupefilter=dupefilter
    )
    failures: list[BaseException] = []
    opened.addErrback(lambda failure: failures.append(failure.value))
    dupefilter_open.errback(RuntimeError("dupefilter failed"))
    _settle_workers(workers)

    assert failures and str(failures[0]) == "dupefilter failed"
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()


def test_async_dupefilter_success_bridges_authority_without_close(monkeypatch) -> None:
    dupefilter = MagicMock()
    dupefilter_open: Deferred[object] = Deferred()
    dupefilter.open.return_value = dupefilter_open
    _fake_reactor, workers, _queue, manager, scheduler, opened = _open_scheduler(
        monkeypatch, dupefilter=dupefilter
    )
    opened_values: list[object] = []
    opened.addCallback(opened_values.append)
    dupefilter_open.callback(None)
    _settle_workers(workers)

    assert opened_values == [None]
    assert scheduler._lifecycle_state == "open"
    settled_workers = len(workers)
    closing = scheduler.close("dupefilter-success")
    assert isinstance(closing, Deferred)
    _settle_workers(workers, start=settled_workers)
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()


def test_async_dupefilter_timeout_does_not_replace_public_open_timeout(
    monkeypatch,
) -> None:
    dupefilter = MagicMock()
    dupefilter_open: Deferred[object] = Deferred()
    dupefilter.open.return_value = dupefilter_open
    fake_reactor, workers, _queue, manager, scheduler, opened = _open_scheduler(
        monkeypatch, dupefilter=dupefilter
    )
    open_failures: list[BaseException] = []
    opened.addErrback(lambda failure: open_failures.append(failure.value))
    closing = scheduler.close("dupefilter-open-timeout")
    dupefilter_open.callback(None)
    assert len(workers) == 1
    # call 0 bounds close's whole authoritative chain; call 1 bounds warm-up.
    fake_reactor.calls[1].fire()
    assert open_failures
    _settle_workers(workers)
    assert closing.called
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()


def test_sync_open_close_request_suppresses_dupefilter_publication(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: False)
    dupefilter = MagicMock()
    manager = MagicMock()
    scheduler = BackendScheduler(manager, dupefilter=dupefilter)

    def request_close(_spider):
        scheduler._open_close_requested = True

    dupefilter.open.side_effect = request_close
    scheduler.open(SimpleNamespace(name="sync-cancel", crawler=None))
    assert scheduler._lifecycle_state == "opening"
    scheduler._cleanup_after_open_failure("sync-cancel")
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()


def test_open_timeout_then_close_waits_for_warmup_and_owns_manager(monkeypatch) -> None:
    fake_reactor, workers, queue, manager, scheduler, opened = _open_scheduler(
        monkeypatch
    )
    open_failures: list[BaseException] = []
    opened.addErrback(lambda failure: open_failures.append(failure.value))
    fake_reactor.calls[-1].fire()

    closing = scheduler.close("open-timeout")
    assert isinstance(closing, Deferred)
    assert scheduler._lifecycle_state == "closing"
    assert not closing.called
    assert not queue.close.called
    assert not manager.close.called

    _settle_workers(workers)
    assert scheduler._lifecycle_state == "closed"
    assert closing.called
    assert manager.close.call_count == 1
    assert len(open_failures) == 1

    assert scheduler.close("repeated") is None
    assert manager.close.call_count == 1


def test_close_before_late_open_success_suppresses_open_publication(
    monkeypatch,
) -> None:
    _fake_reactor, workers, queue, manager, scheduler, opened = _open_scheduler(
        monkeypatch
    )
    open_failures: list[BaseException] = []
    opened.addErrback(lambda failure: open_failures.append(failure.value))

    closing = scheduler.close("cancel-open")
    _settle_workers(workers)

    assert closing.called
    assert (
        open_failures and str(open_failures[0]) == "Scheduler open cancelled by close"
    )
    assert scheduler._lifecycle_state == "closed"
    assert scheduler._queue is None
    assert not queue.close.called
    manager.close.assert_called_once_with()


def test_open_publication_failure_restores_after_async_cleanup(monkeypatch) -> None:
    _fake_reactor, workers, _queue, _manager, scheduler, opened = _open_scheduler(
        monkeypatch
    )
    scheduler._finish_open = MagicMock(side_effect=RuntimeError("publication failed"))
    scheduler._cleanup_after_open_failure = MagicMock(return_value=succeed(None))
    failures: list[BaseException] = []
    opened.addErrback(lambda failure: failures.append(failure.value))

    worker, function, args, kwargs = workers[0]
    function(*args, **kwargs)
    worker.callback(None)

    assert failures and str(failures[0]) == "publication failed"
    scheduler._cleanup_after_open_failure.assert_called_once_with("open-failed")


def test_open_late_failure_remains_close_failure_precedence(monkeypatch) -> None:
    fake_reactor, workers, _queue, manager, scheduler, opened = _open_scheduler(
        monkeypatch
    )
    opened.addErrback(lambda failure: None)
    fake_reactor.calls[-1].fire()
    closing = scheduler.close("late-open-failure")
    close_failures: list[BaseException] = []
    closing.addErrback(lambda failure: close_failures.append(failure.value))

    workers[0][0].errback(RuntimeError("warm-up failed late"))
    _settle_workers(workers, start=1)

    assert scheduler._lifecycle_state == "closed"
    assert close_failures and str(close_failures[0]) == "warm-up failed late"
    manager.close.assert_called_once_with()


def test_pending_settlement_flattens_queue_close_and_release(monkeypatch) -> None:
    fake_reactor = _FakeReactor()
    calls = _patch_ordered(monkeypatch, reactor=fake_reactor)
    scheduler = BackendScheduler(MagicMock(), owns_connection_manager=True)
    queue = MagicMock()
    scheduler._queue = queue
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    child = group.new_child()
    assert child is not None
    group.seal()
    child.ack()

    workers: list[tuple[Deferred[object], object, tuple, dict]] = []

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function, args, kwargs))
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", thread)
    closing = scheduler.close("flatten")
    assert isinstance(closing, Deferred)
    assert not queue.close.called

    calls[0][0].callback(None)
    assert len(workers) == 1
    assert not closing.called

    worker, function, args, kwargs = workers[0]
    function(*args, **kwargs)
    worker.callback(None)
    assert len(workers) == 2
    assert queue.close.call_count == 1
    assert not closing.called

    release_worker, release_function, release_args, release_kwargs = workers[1]
    release_function(*release_args, **release_kwargs)
    release_worker.callback(None)
    assert closing.called
    assert scheduler._lifecycle_state == "closed"


def test_pending_settlement_queue_close_failure_is_retryable(monkeypatch) -> None:
    fake_reactor = _FakeReactor()
    calls = _patch_ordered(monkeypatch, reactor=fake_reactor)
    scheduler = BackendScheduler(MagicMock(), owns_connection_manager=True)
    queue = MagicMock()
    scheduler._queue = queue
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    child = group.new_child()
    assert child is not None
    group.seal()
    child.ack()
    workers: list[tuple[Deferred[object], object, tuple, dict]] = []

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function, args, kwargs))
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", thread)
    closing = scheduler.close("queue-failure")
    assert isinstance(closing, Deferred)
    closing.addErrback(lambda failure: None)
    calls[0][0].callback(None)
    queue.close.side_effect = QueueError("queue close failed")
    worker, function, args, kwargs = workers[0]
    with pytest.raises(QueueError):
        function(*args, **kwargs)
    worker.errback(QueueError("queue close failed"))
    assert scheduler._lifecycle_state == "closing"
    assert not scheduler.connection_manager.close.called

    queue.close.side_effect = None
    retry = scheduler.close("queue-retry")
    assert isinstance(retry, Deferred)
    retry_worker, retry_function, retry_args, retry_kwargs = workers[1]
    retry_function(*retry_args, **retry_kwargs)
    retry_worker.callback(None)
    release_worker, release_function, release_args, release_kwargs = workers[2]
    release_function(*release_args, **release_kwargs)
    release_worker.callback(None)

    assert closing.called
    assert retry.called
    assert scheduler._lifecycle_state == "closed"
    assert queue.close.call_count == 2
    assert scheduler.connection_manager.close.call_count == 1


def test_pending_settlement_queue_close_timeout_keeps_authority(monkeypatch) -> None:
    fake_reactor = _FakeReactor()
    calls = _patch_ordered(monkeypatch, reactor=fake_reactor)
    scheduler = BackendScheduler(MagicMock(), owns_connection_manager=True)
    queue = MagicMock()
    scheduler._queue = queue
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    child = group.new_child()
    assert child is not None
    group.seal()
    child.ack()
    workers: list[tuple[Deferred[object], object, tuple, dict]] = []

    def thread(function, *args, **kwargs):
        worker: Deferred[object] = Deferred()
        workers.append((worker, function, args, kwargs))
        return worker

    monkeypatch.setattr(scheduler_module, "deferToThread", thread)
    closing = scheduler.close("queue-timeout")
    assert isinstance(closing, Deferred)
    closing.addErrback(lambda failure: None)
    calls[0][0].callback(None)
    worker, function, args, kwargs = workers[0]
    function(*args, **kwargs)
    fake_reactor.calls[0].fire()
    assert closing.called
    assert scheduler._lifecycle_state == "closing"
    assert not scheduler.connection_manager.close.called

    worker.callback(None)
    release_worker, release_function, release_args, release_kwargs = workers[1]
    release_function(*release_args, **release_kwargs)
    release_worker.callback(None)
    assert scheduler._lifecycle_state == "closed"
    assert scheduler.connection_manager.close.call_count == 1
