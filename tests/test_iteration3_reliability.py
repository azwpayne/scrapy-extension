"""Deterministic regression coverage for iteration-3 settlement reliability."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from scrapy import Request
from twisted.internet.defer import Deferred, succeed

import scrapy_extension.queue.queue as queue_module
import scrapy_extension.schedule.scheduler as scheduler_module
from scrapy_extension.exceptions import BackendError, BackendOperationTimeout
from scrapy_extension.queue.queue import BACKEND_ACK_TOKEN_META_KEY, BackendQueue
from scrapy_extension.queue.strategies.base import (
    QueueStrategy,
    _BoundQueueAckToken,
    _PreparedQueuePush,
)
from scrapy_extension.schedule.scheduler import BackendScheduler


class _PendingSettlement:
    def __init__(self, function, args, kwargs) -> None:
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.operation: Deferred[object] = Deferred()
        self.bounded: Deferred[object] = Deferred()


def _patch_ordered_adapter(monkeypatch, *, running: bool = True):
    calls: list[_PendingSettlement] = []

    def ordered(function, *args, **kwargs):
        call_kwargs = dict(kwargs)
        call_kwargs.pop("timeout", None)
        call_kwargs.pop("operation", None)
        call = _PendingSettlement(function, args, call_kwargs)
        calls.append(call)
        return call.operation, call.bounded

    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: running)
    monkeypatch.setattr(queue_module, "reactor_is_running", lambda: running)
    monkeypatch.setattr(scheduler_module, "defer_to_thread_ordered", ordered)
    monkeypatch.setattr(queue_module, "defer_to_thread_ordered", ordered)
    return calls


def _durable_strategy(mocker) -> MagicMock:
    strategy = MagicMock(spec=QueueStrategy)

    def prepare(
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

    strategy._prepare_push.side_effect = prepare
    return strategy


def test_queue_replacement_ack_is_off_reactor_and_close_fenced(monkeypatch, mocker):
    calls = _patch_ordered_adapter(monkeypatch)
    manager = MagicMock(name="ConnectionManager")
    strategy = _durable_strategy(mocker)
    queue = BackendQueue(manager, "q", queue_strategy=strategy)
    token = _BoundQueueAckToken(
        manager.get_queue_backend.return_value,
        "q",
        "source-token",
    )
    request = Request(
        "https://example.test/replacement",
        meta={BACKEND_ACK_TOKEN_META_KEY: token},
    )

    queue.push(request)
    assert len(calls) == 1
    manager.get_queue_backend.return_value.ack.assert_not_called()
    assert BACKEND_ACK_TOKEN_META_KEY in request.meta
    calls[0].function(*calls[0].args, **calls[0].kwargs)
    manager.get_queue_backend.return_value.ack.assert_called_once_with(
        "q", token="source-token"
    )

    close_done = threading.Event()

    def close() -> None:
        queue.close(lossy=True)
        close_done.set()

    closer = threading.Thread(target=close)
    closer.start()
    assert not close_done.wait(0.05)

    calls[0].operation.callback(None)
    calls[0].bounded.callback(None)
    closer.join(timeout=2.0)
    assert close_done.is_set()
    assert BACKEND_ACK_TOKEN_META_KEY not in request.meta


def test_replacement_ack_timeout_late_success_preserves_authority(monkeypatch):
    calls = _patch_ordered_adapter(monkeypatch)
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(manager, owns_connection_manager=False)
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    child = group.new_child()
    assert child is not None
    group.seal()

    child.ack()
    assert len(calls) == 1
    heartbeat = []
    heartbeat.append("reactor-alive")
    calls[0].bounded.errback(BackendOperationTimeout("scheduler ack", 1.0))
    assert group._terminal is False
    assert heartbeat == ["reactor-alive"]

    calls[0].function(*calls[0].args, **calls[0].kwargs)
    calls[0].operation.callback(None)
    assert group._terminal is True
    assert group._pending == set()
    queue.ack.assert_called_once_with(token="source")


def test_queue_replacement_ack_timeout_keeps_source_token(monkeypatch, mocker):
    calls = _patch_ordered_adapter(monkeypatch)
    manager = MagicMock(name="ConnectionManager")
    strategy = _durable_strategy(mocker)
    queue = BackendQueue(manager, "q", queue_strategy=strategy)
    token = _BoundQueueAckToken(
        manager.get_queue_backend.return_value,
        "q",
        "source-token",
    )
    request = Request(
        "https://example.test/replacement-failure",
        meta={BACKEND_ACK_TOKEN_META_KEY: token},
    )

    queue.push(request)
    calls[0].bounded.errback(BackendOperationTimeout("queue replacement ack", 1.0))
    calls[0].operation.errback(BackendError("late failure"))

    assert BACKEND_ACK_TOKEN_META_KEY in request.meta
    manager.get_queue_backend.return_value.ack.assert_not_called()


def test_group_without_queue_keeps_source_unsettled(monkeypatch):
    calls = _patch_ordered_adapter(monkeypatch)
    scheduler = BackendScheduler(MagicMock())
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    child = group.new_child()
    assert child is not None
    group.seal()
    child.ack()

    assert calls == []
    assert group._terminal is False
    assert group._pending == {0}
    group.abort()
    assert group._terminal is True


def test_group_abort_does_not_double_settle_an_inflight_ack(monkeypatch):
    calls = _patch_ordered_adapter(monkeypatch)
    scheduler = BackendScheduler(MagicMock())
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    child = group.new_child()
    assert child is not None
    group.seal()
    child.ack()
    group.abort()

    assert len(calls) == 1
    calls[0].function(*calls[0].args, **calls[0].kwargs)
    calls[0].operation.callback(None)
    assert queue.ack.call_count == 1
    queue.nack.assert_not_called()
    assert group._terminal is True


def test_replacement_ack_late_failure_is_retryable_without_double_settlement(
    monkeypatch,
):
    calls = _patch_ordered_adapter(monkeypatch)
    scheduler = BackendScheduler(MagicMock())
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    child = group.new_child()
    assert child is not None
    group.seal()

    child.ack()
    calls[0].bounded.errback(BackendOperationTimeout("scheduler ack", 1.0))
    calls[0].operation.errback(BackendError("late failure"))
    assert group._terminal is False
    assert group._pending == {0}

    child.ack()
    assert len(calls) == 2
    assert queue.ack.call_count == 0
    calls[1].function(*calls[1].args, **calls[1].kwargs)
    calls[1].operation.callback(None)
    assert queue.ack.call_count == 1
    assert group._terminal is True


def test_replacement_group_concurrent_children_settles_source_exactly_once(
    monkeypatch,
):
    calls = _patch_ordered_adapter(monkeypatch)
    scheduler = BackendScheduler(MagicMock())
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    first = group.new_child()
    second = group.new_child()
    assert first is not None and second is not None
    group.seal()

    first.ack()
    second.ack()
    second.ack()
    assert group._pending == {1}
    assert len(calls) == 1

    calls[0].function(*calls[0].args, **calls[0].kwargs)
    calls[0].operation.callback(None)
    assert queue.ack.call_count == 1
    assert group._terminal is True


def test_scheduler_close_waits_for_group_source_settlement(monkeypatch):
    calls = _patch_ordered_adapter(monkeypatch)
    scheduler = BackendScheduler(MagicMock(), owns_connection_manager=False)
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue
    group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
    child = group.new_child()
    assert child is not None
    group.seal()
    child.ack()
    assert len(calls) == 1

    monkeypatch.setattr(
        scheduler_module,
        "deferToThread",
        lambda function, *args, **kwargs: succeed(function(*args, **kwargs)),
    )
    closing = scheduler.close("settlement-fence")
    assert isinstance(closing, Deferred)
    queue.close.assert_not_called()

    calls[0].function(*calls[0].args, **calls[0].kwargs)
    calls[0].operation.callback(None)
    assert closing.called
    queue.close.assert_called_once()
