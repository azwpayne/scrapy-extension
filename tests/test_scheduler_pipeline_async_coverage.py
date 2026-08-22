"""Deterministic lifecycle and ownership contracts for scheduler and pipeline."""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from scrapy.http import Request
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

import scrapy_extension.pipeline.pipeline as pipeline_module
import scrapy_extension.schedule.scheduler as scheduler_module
from scrapy_extension.exceptions import (
    BackendError,
)
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.schedule.scheduler import BackendScheduler


class _RejectingDeferred(Deferred[Any]):
    """A worker Deferred whose first observer registration can be rejected."""

    def __init__(self, *methods: str, reject_count: int = 1) -> None:
        super().__init__()
        self._rejected_methods = set(methods)
        self._reject_count = reject_count

    def _maybe_reject(self, method: str) -> None:
        if method in self._rejected_methods and self._reject_count:
            self._reject_count -= 1
            raise RuntimeError(f"{method} registration rejected")

    def addCallbacks(self, *args: Any, **kwargs: Any) -> Deferred[Any]:
        self._maybe_reject("addCallbacks")
        return super().addCallbacks(*args, **kwargs)

    def addBoth(self, *args: Any, **kwargs: Any) -> Deferred[Any]:
        self._maybe_reject("addBoth")
        return super().addBoth(*args, **kwargs)

    def addCallback(self, *args: Any, **kwargs: Any) -> Deferred[Any]:
        self._maybe_reject("addCallback")
        return super().addCallback(*args, **kwargs)

    def addErrback(self, *args: Any, **kwargs: Any) -> Deferred[Any]:
        self._maybe_reject("addErrback")
        return super().addErrback(*args, **kwargs)


class _Workers:
    """Explicit worker authority used instead of timing or sleeping.

    ``preset`` holds pre-built worker Deferreds handed out first-in-first-out
    by :meth:`submit`, so one submission can deliver a prepared object (for
    example a rejecting Deferred) to the component under test.
    """

    def __init__(self) -> None:
        self.calls: list[
            tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]
        ] = []
        self.deferreds: list[Deferred[Any]] = []
        self.preset: list[Deferred[Any]] = []

    def submit(
        self, function: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Deferred[Any]:
        worker: Deferred[Any] = self.preset.pop(0) if self.preset else Deferred()
        self.calls.append((function, args, kwargs))
        self.deferreds.append(worker)
        return worker


class _Spider:
    name = "ownership-spider"

    def __init__(self, signals: Any | None = None) -> None:
        self.crawler = None if signals is None else SimpleNamespace(signals=signals)


def _consume(deferred: Deferred[Any]) -> None:
    deferred.addErrback(lambda failure: None)


def _complete_worker(workers: _Workers, index: int) -> None:
    """Run the submitted callable, then settle its authoritative Deferred."""
    function, args, kwargs = workers.calls[index]
    result = function(*args, **kwargs)
    workers.deferreds[index].callback(result)


def test_scheduler_open_constructs_and_publishes_only_after_authoritative_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Warm-up and queue construction workers fence OPEN publication."""
    workers = _Workers()
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "_submit_thread", workers.submit)
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    manager = Mock(name="queue-manager")
    queue = Mock(name="queue")
    monkeypatch.setattr(scheduler_module, "BackendQueue", lambda **_kwargs: queue)
    scheduler = BackendScheduler(manager, queue_key="queue:{spider}")

    opening = scheduler.open(_Spider())
    assert scheduler._lifecycle_state == "opening"
    assert len(workers.calls) == 1

    workers.deferreds[0].callback(None)
    assert len(workers.calls) == 2
    assert scheduler._queue is None
    assert scheduler._lifecycle_state == "opening"

    workers.deferreds[1].callback(queue)
    assert opening.called
    assert scheduler._queue is queue
    assert scheduler.queue_key == "queue:ownership-spider"
    assert scheduler._lifecycle_state == "open"
    assert manager.set_monitor.call_count == 1

    closing = scheduler.close("finished")
    assert isinstance(closing, Deferred)
    assert len(workers.calls) == 3
    _complete_worker(workers, 2)
    assert len(workers.calls) == 4
    _complete_worker(workers, 3)
    queue.close.assert_called_once_with()
    manager.close.assert_called_once_with()


def test_scheduler_close_request_cancels_open_without_constructing_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A close request wins over a late warm-up success and still releases ownership."""
    workers = _Workers()
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "_submit_thread", workers.submit)
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    manager = Mock(name="manager")
    scheduler = BackendScheduler(manager)
    opening = scheduler.open(_Spider())
    scheduler._open_close_requested = True

    workers.deferreds[0].callback(None)
    _consume(opening)
    assert scheduler._queue is None
    assert scheduler._lifecycle_state == "opening"
    assert len(workers.calls) == 1

    with pytest.raises(RuntimeError, match="open is already in progress"):
        scheduler.close("cancelled")
    cleanup = scheduler._close_attempt("cancelled", allow_opening=True)
    assert isinstance(cleanup, Deferred)
    _complete_worker(workers, 1)
    assert scheduler._lifecycle_state == "closed"
    assert manager.close.call_count == 1


def test_scheduler_generic_backend_dupefilter_open_and_release_use_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generic lifecycle fallback keeps exact filter and manager ownership fenced."""
    from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter

    workers = _Workers()
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "_submit_thread", workers.submit)
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    manager = Mock(name="manager")
    membership = Mock(name="membership-filter")
    dupefilter = BackendDupeFilter(None, membership_filter=membership)
    dupefilter._open_authoritative_async = None  # type: ignore[assignment]
    dupefilter._release_authoritative_async = None  # type: ignore[assignment]
    dupefilter.open = Mock(return_value=None)  # type: ignore[method-assign]
    dupefilter.release = Mock()  # type: ignore[method-assign]
    queue = Mock(name="queue")
    monkeypatch.setattr(scheduler_module, "BackendQueue", lambda **_kwargs: queue)
    scheduler = BackendScheduler(manager, dupefilter=dupefilter)

    opening = scheduler.open(_Spider())
    assert len(workers.calls) == 1
    assert workers.calls[0][0] is scheduler_module._call_dupefilter_open
    _complete_worker(workers, 0)
    assert len(workers.calls) == 2
    _complete_worker(workers, 1)
    assert len(workers.calls) == 3
    _complete_worker(workers, 2)
    _consume(opening)
    assert scheduler._lifecycle_state == "open"
    dupefilter.open.assert_called_once()

    closing = scheduler.close("finished")
    assert isinstance(closing, Deferred)
    assert len(workers.calls) == 4
    _complete_worker(workers, 3)
    assert len(workers.calls) == 5
    _complete_worker(workers, 4)

    dupefilter.release.assert_called_once_with(
        scheduler._dupefilter_release_owner, "finished"
    )
    queue.close.assert_called_once_with()
    manager.close.assert_called_once_with()
    assert scheduler._lifecycle_state == "closed"


def test_scheduler_open_failure_waits_for_authoritative_cleanup_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue construction failure preserves the original error until release completes."""
    workers = _Workers()
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "_submit_thread", workers.submit)
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    manager = Mock(name="manager")
    scheduler = BackendScheduler(manager)
    opening = scheduler.open(_Spider())
    _complete_worker(workers, 0)
    assert len(workers.deferreds) == 2

    construction_error = RuntimeError("queue construction failed")
    workers.deferreds[1].errback(construction_error)
    _consume(opening)
    assert len(workers.deferreds) == 3
    assert scheduler._lifecycle_state == "closing"
    assert manager.close.call_count == 0

    _complete_worker(workers, 2)
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()
    assert scheduler._queue is None


def test_scheduler_factory_cleanup_force_releases_after_callback_failure() -> None:
    """Factory rollback observes cleanup failures without leaking its manager handles."""
    scheduler = BackendScheduler(Mock(name="manager"))
    cleanup = Deferred()
    force_release = Mock()
    scheduler._force_factory_manager_release = force_release  # type: ignore[method-assign]

    def fail_cleanup(_failure: Any) -> None:
        raise KeyboardInterrupt("cleanup observer interrupted")

    scheduler._observe_factory_cleanup(cleanup, on_failure=fail_cleanup)
    cleanup.errback(RuntimeError("close failed"))
    assert force_release.call_count == 1


def test_scheduler_rollback_factory_failure_retries_as_lossy_abort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A synchronous close rejection invokes one abort and retains cleanup authority."""
    scheduler = BackendScheduler(Mock(name="manager"))
    close = Mock(side_effect=RuntimeError("close rejected"))
    abort = Mock(return_value=None)
    force_release = Mock()
    scheduler.close = close  # type: ignore[method-assign]
    scheduler.abort = abort  # type: ignore[method-assign]
    scheduler._force_factory_manager_release = force_release  # type: ignore[method-assign]

    scheduler._rollback_factory_failure()
    close.assert_called_once_with("crawler-factory-failed")
    abort.assert_called_once_with("crawler-factory-failed")
    force_release.assert_not_called()


def test_scheduler_signal_workers_ack_and_nack_exact_delivery_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Signal paths retain the exact token until the authoritative worker settles."""
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    manager = Mock(name="manager")
    queue = Mock(name="queue")
    scheduler = BackendScheduler(manager)
    scheduler._queue = queue
    ack_worker: Deferred[Any] = Deferred()
    ack_public: Deferred[Any] = Deferred()
    nack_worker: Deferred[Any] = Deferred()
    nack_public: Deferred[Any] = Deferred()
    calls: list[tuple[Callable[..., Any], dict[str, Any]]] = []

    def ordered(
        function: Callable[..., Any], **kwargs: Any
    ) -> tuple[Deferred[Any], Deferred[Any]]:
        calls.append((function, kwargs))
        return (
            (ack_worker, ack_public) if len(calls) == 1 else (nack_worker, nack_public)
        )

    monkeypatch.setattr(scheduler_module, "defer_to_thread_ordered", ordered)
    ack_token = object()
    nack_token = object()
    ack_request = Request("https://ack.example", meta={"_backend_ack_token": ack_token})
    nack_request = Request(
        "https://nack.example", meta={"_backend_ack_token": nack_token}
    )

    ack_result = scheduler._on_response_received(None, ack_request, None)
    nack_result = scheduler._on_spider_error(
        None, SimpleNamespace(request=nack_request), None
    )
    assert ack_result is ack_public
    assert nack_result is nack_public
    assert ack_request.meta["_backend_ack_token"] is ack_token
    assert nack_request.meta["_backend_ack_token"] is nack_token

    calls[0][0](**calls[0][1])
    queue.ack.assert_called_once_with(
        token=ack_token,
        timeout=5.0,
        operation="scheduler ack",
    )
    ack_worker.callback(None)
    assert "_backend_ack_token" not in ack_request.meta

    calls[1][0](**calls[1][1])
    queue.nack.assert_called_once_with(
        token=nack_token,
        timeout=5.0,
        operation="scheduler nack",
    )
    nack_worker.callback(None)
    assert "_backend_ack_token" not in nack_request.meta


def test_scheduler_settlement_worker_failure_is_reported_and_token_is_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed public settlement never claims ownership of an unacknowledged token."""
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    scheduler = BackendScheduler(Mock(name="manager"))
    scheduler._queue = Mock(name="queue")
    worker: Deferred[Any] = Deferred()
    public: Deferred[Any] = Deferred()
    monkeypatch.setattr(
        scheduler_module,
        "defer_to_thread_ordered",
        lambda *_args, **_kwargs: (worker, public),
    )
    stats = Mock()
    scheduler.stats = stats
    request = Request("https://failed.example", meta={"_backend_ack_token": "token-f"})

    result = scheduler._ack_request_token(request, log_message="ack failed")
    assert result is public
    failure = Failure(BackendError("backend failure"))
    public.errback(failure)
    assert request.meta["_backend_ack_token"] == "token-f"
    stats.inc_value.assert_called_once_with("scheduler/ack_error")
    worker.callback(None)
    assert not scheduler._pending_settlements


def test_scheduler_settlement_observer_rejections_do_not_release_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider observer failures leave the accepted worker in the pending barrier."""
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    scheduler = BackendScheduler(Mock(name="manager"))
    scheduler._queue = Mock(name="queue")
    worker = _RejectingDeferred("addBoth", reject_count=2)
    bounded = Deferred()
    monkeypatch.setattr(
        scheduler_module,
        "defer_to_thread_ordered",
        lambda *_args, **_kwargs: (worker, bounded),
    )

    result = scheduler._settle_token_async_ordered(
        "token-x", negative=False, log_message="ack failed"
    )
    assert result is not None
    assert worker in scheduler._pending_settlements
    worker.callback(None)
    # Both observer registrations were rejected. Retaining this operation is
    # conservative: close must not release the manager without proof that the
    # accepted worker was observed.
    assert worker in scheduler._pending_settlements
    scheduler._pending_settlements.remove(worker)


def test_scheduler_close_queue_worker_retries_observer_and_releases_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue close remains authoritative when the first callback attachment is rejected."""
    workers = _Workers()
    queue_worker = _RejectingDeferred("addCallbacks")
    workers.preset.append(queue_worker)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "_submit_thread", workers.submit)
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    manager = Mock(name="manager")
    queue = Mock(name="queue")
    scheduler = BackendScheduler(manager)
    scheduler._queue = queue

    closing = scheduler.close("finished")
    assert isinstance(closing, Deferred)
    # The pre-seeded rejecting worker reached the scheduler itself: the first
    # addCallbacks was refused, the retry arm re-attached the observers, and
    # the authoritative Deferred was returned to the caller untouched.
    assert closing is queue_worker
    assert len(workers.calls) == 1
    assert workers.calls[0][0] is queue.close
    assert queue_worker._reject_count == 0
    assert not queue_worker.called
    assert queue.close.call_count == 0

    _complete_worker(workers, 0)
    queue.close.assert_called_once_with()
    assert len(workers.calls) == 2
    assert workers.calls[1][0] == scheduler._release_managers

    _complete_worker(workers, 1)
    manager.close.assert_called_once_with()
    assert scheduler._lifecycle_state == "closed"


def test_scheduler_close_queue_worker_double_rejection_preserves_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A doubly-rejected queue observer surfaces the failure and keeps the queue.

    Both ``addCallbacks`` attachments are refused by the provider Deferred, so
    the close pass must hand the caller the bound view of the synthesized
    failure while the queue generation and manager ownership stay intact for a
    retry (mirroring the ``submission_failed`` semantics of ``queue_failure``).
    """
    workers = _Workers()
    queue_worker = _RejectingDeferred("addCallbacks", reject_count=2)
    workers.preset.append(queue_worker)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "_submit_thread", workers.submit)
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    manager = Mock(name="manager")
    queue = Mock(name="queue")
    scheduler = BackendScheduler(manager)
    scheduler._queue = queue

    closing = scheduler.close("finished")
    assert isinstance(closing, Deferred)
    assert closing is not queue_worker
    failures: list[Failure] = []
    closing.addErrback(failures.append)
    assert len(failures) == 1
    assert isinstance(failures[0].value, RuntimeError)
    assert str(failures[0].value) == "addCallbacks registration rejected"
    assert queue_worker._reject_count == 0
    assert not queue_worker.called
    # No queue close observer was accepted: the queue generation and manager
    # ownership remain available for the next close pass.
    assert scheduler._queue is queue
    assert not scheduler._queue_terminal
    assert queue.close.call_count == 0
    assert manager.close.call_count == 0
    assert scheduler._lifecycle_state == "closing"

    second = scheduler.close("finished")
    assert isinstance(second, Deferred)
    assert len(workers.calls) == 2
    assert workers.calls[1][0] == queue.close
    _complete_worker(workers, 1)
    queue.close.assert_called_once_with()
    assert len(workers.calls) == 3
    assert workers.calls[2][0] == scheduler._release_managers
    _complete_worker(workers, 2)
    manager.close.assert_called_once_with()
    assert scheduler._lifecycle_state == "closed"


def test_scheduler_close_finish_attachment_double_rejection_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A doubly-rejected finish observer returns a stable error and keeps ownership."""
    workers = _Workers()
    queue_worker = _RejectingDeferred("addBoth", reject_count=2)
    workers.preset.append(queue_worker)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "_submit_thread", workers.submit)
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    manager = Mock(name="manager")
    queue = Mock(name="queue")
    scheduler = BackendScheduler(manager)
    scheduler._queue = queue

    closing = scheduler.close("finished")
    assert isinstance(closing, Deferred)
    assert closing is not queue_worker
    failures: list[Failure] = []
    closing.addErrback(failures.append)
    assert len(failures) == 1
    assert isinstance(failures[0].value, RuntimeError)
    assert str(failures[0].value) == "scheduler close callback attachment failed"
    # The accepted queue worker keeps its queue observers while the attempt
    # unwinds: the queue generation and manager ownership stay intact.
    assert scheduler._queue is queue
    assert not scheduler._queue_terminal
    assert not queue_worker.called
    assert queue.close.call_count == 0
    assert manager.close.call_count == 0
    assert scheduler._lifecycle_state == "closing"

    second = scheduler.close("finished")
    assert isinstance(second, Deferred)
    assert len(workers.calls) == 2
    assert workers.calls[1][0] == queue.close
    _complete_worker(workers, 1)
    queue.close.assert_called_once_with()
    assert len(workers.calls) == 3
    assert workers.calls[2][0] == scheduler._release_managers
    _complete_worker(workers, 2)
    manager.close.assert_called_once_with()
    assert scheduler._lifecycle_state == "closed"


def test_scheduler_untyped_queue_close_failure_is_terminal_but_releases_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-checkpoint queue failure is diagnostic-only and cannot leak the manager."""
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: False)
    manager = Mock(name="manager")
    queue = Mock(name="queue")
    queue.close.side_effect = RuntimeError("queue cleanup failed")
    scheduler = BackendScheduler(manager)
    scheduler._queue = queue

    scheduler.close("finished")
    queue.close.assert_called_once_with()
    manager.close.assert_called_once_with()
    assert scheduler._lifecycle_state == "closed"


def test_scheduler_async_dupefilter_close_callback_and_manager_release_are_ordered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delayed dupefilter close publishes CLOSED only after manager release."""
    workers = _Workers()
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(scheduler_module, "_submit_thread", workers.submit)
    manager = Mock(name="manager")
    close_worker = Deferred()

    class GenericDupeFilter:
        def close(self, reason: str) -> Deferred[Any]:
            assert reason == "finished"
            return close_worker

    dupefilter = GenericDupeFilter()
    scheduler = BackendScheduler(manager, dupefilter=dupefilter)
    scheduler._dupefilter_open = True
    monkeypatch.setattr(
        scheduler_module, "bounded_deferred", lambda source, **_kwargs: source
    )

    result = scheduler._close_after_queue("finished", lossy=False)
    assert result is close_worker
    assert len(workers.calls) == 0
    close_worker.callback(None)
    assert len(workers.calls) == 1
    _complete_worker(workers, 0)
    assert scheduler._lifecycle_state == "closed"
    assert manager.close.call_count == 1


def test_scheduler_enqueue_process_control_after_durable_push_settles_legacy_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-commit monitor interruption settles, rather than forgets, a legacy receipt."""

    class Queue:
        def push(self, request: Request, *, priority: float = 0.0) -> None:
            del request, priority
            raise KeyboardInterrupt("monitor interrupted after commit")

        def _consume_post_commit_push(self) -> bool:
            return True

    dupefilter = Mock(name="dupefilter")
    dupefilter.request_seen.return_value = False
    dupefilter.consume_reservation.return_value = True
    settled: list[Request] = []
    dupefilter.settle_reservation.side_effect = lambda request: settled.append(request)
    scheduler = BackendScheduler(Mock(name="manager"), dupefilter=dupefilter)
    scheduler._queue = Queue()  # type: ignore[assignment]

    request = Request("https://durable.example")
    with pytest.raises(KeyboardInterrupt, match="monitor interrupted"):
        scheduler.enqueue_request(request)

    dupefilter.request_seen.assert_called_once_with(request)
    dupefilter.consume_reservation.assert_called_once_with(request)
    assert settled == [request]
    dupefilter.forget.assert_not_called()


def test_pipeline_open_submission_observer_rejection_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An already-settled accepted open worker clears admission after observer rejection."""
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    worker = _RejectingDeferred("addBoth", reject_count=2)
    worker.callback(None)
    monkeypatch.setattr(
        pipeline_module, "_submit_thread", lambda *_args, **_kwargs: worker
    )
    monkeypatch.setattr(
        pipeline_module, "bounded_deferred", lambda source, **_kwargs: source
    )
    manager = Mock(name="manager")
    pipeline = BackendPipeline(manager)
    spider = _Spider()

    result = pipeline.open_spider(spider)
    assert isinstance(result, Deferred)
    assert pipeline._opening is False
    assert pipeline._opening_operation is None
    assert manager.close.call_count == 0


def test_pipeline_open_failure_releases_manager_when_strategy_cleanup_also_fails(
    mocker: Any,
) -> None:
    """Open's primary failure survives a failed rollback diagnostic and release."""
    manager = Mock(name="manager")
    strategy = Mock(name="strategy")
    open_error = RuntimeError("open failed")
    strategy.open.side_effect = open_error
    strategy.close.side_effect = RuntimeError("cleanup failed")
    pipeline = BackendPipeline(manager, storage_strategy=strategy)
    pipeline._storage_supported = None
    spider = _Spider()
    logger_error = mocker.patch("scrapy_extension.pipeline.pipeline.logger.error")

    with pytest.raises(RuntimeError) as exc_info:
        pipeline.open_spider(spider)
    assert exc_info.value is open_error
    strategy.close.assert_called_once_with()
    manager.close.assert_called_once_with()
    logger_error.assert_called_once()
    assert pipeline._closed is True


def test_pipeline_close_adapter_rejection_leaves_close_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected close adapter does not strand the pipeline in CLOSING."""
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    manager = Mock(name="manager")
    pipeline = BackendPipeline(manager)
    worker_error = RuntimeError("close adapter unavailable")
    monkeypatch.setattr(
        pipeline_module,
        "defer_to_thread_ordered",
        Mock(side_effect=worker_error),
    )

    result = pipeline.close_spider(_Spider())
    assert isinstance(result, Deferred)
    _consume(result)
    assert pipeline._closing is False
    assert pipeline._close_async_pending is False
    assert manager.close.call_count == 0


def test_pipeline_close_observer_failure_is_reported_without_releasing_worker_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The authoritative close worker remains the only owner after a public-view failure."""
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    manager = Mock(name="manager")
    pipeline = BackendPipeline(manager)
    close_worker = Deferred()
    bounded = _RejectingDeferred("addCallbacks", reject_count=2)
    close_calls: list[Callable[..., Any]] = []

    def close_adapter(
        function: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> tuple[Deferred[Any], Deferred[Any]]:
        del args, kwargs
        close_calls.append(function)
        return close_worker, bounded

    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", close_adapter)

    result = pipeline.close_spider(_Spider())
    assert isinstance(result, Deferred)
    _consume(result)
    assert pipeline._closing is True
    assert manager.close.call_count == 0
    close_worker.callback(close_calls[0]())
    assert pipeline._closed is True
    assert manager.close.call_count == 1


def test_pipeline_store_worker_observer_rejection_preserves_item_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A store worker is ordered and authoritative even if both public observers reject."""
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: True)
    manager = Mock(name="manager")
    pipeline = BackendPipeline(manager)
    spider = _Spider()
    worker = Deferred()
    bounded = _RejectingDeferred("addCallbacks", reject_count=2)
    calls: list[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]] = []

    def ordered(
        function: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> tuple[Deferred[Any], Deferred[Any]]:
        calls.append((function, args, kwargs))
        return worker, bounded

    monkeypatch.setattr(pipeline_module, "defer_to_thread_ordered", ordered)
    pipeline._opened = True
    pipeline._opened_spider = spider

    item = {"name": "ordered"}
    result = pipeline.process_item(item, spider)
    assert isinstance(result, Deferred)
    _consume(result)
    assert calls[0][1] == (item, spider)
    assert pipeline._async_tail is not worker
    assert pipeline._async_tail.paused == 1
    assert not pipeline._closed

    worker.callback(item)
    assert worker.called
    monkeypatch.setattr(pipeline_module, "reactor_is_running", lambda: False)
    pipeline.close_spider(spider)
    assert pipeline._closed is True
    manager.close.assert_called_once_with()


def test_pipeline_from_settings_maps_strategy_type_and_getpriority_fallback(
    mocker: Any,
) -> None:
    """Factory parsing reports the public setting while tolerating old settings doubles."""
    from scrapy_extension.backends.connectors import ConnectionManager

    settings = Mock()
    values = {
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_STORAGE_STRATEGY": object(),
    }
    settings.get.side_effect = lambda key, default=None: values.get(key, default)
    settings.getpriority.side_effect = TypeError("legacy settings")
    manager = Mock(name="manager")
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    mocker.patch(
        "scrapy_extension.backends.connectors.resolve_backend_config",
        return_value=("redis", {}),
    )

    with pytest.raises(Exception) as exc_info:
        BackendPipeline.from_settings(settings)
    assert exc_info.value.setting_name == "SCRAPY_STORAGE_STRATEGY"
    manager.close.assert_called_once_with()


def test_pipeline_process_success_and_failure_keep_monitor_and_counter_contract(
    mocker: Any,
) -> None:
    """Worker completion resets the error counter; a later failure is counted anew."""
    manager = Mock(name="manager")
    monitor = Mock(name="monitor")
    pipeline = BackendPipeline(manager, monitor=monitor, max_storage_errors=2)
    pipeline._storage_supported = True
    spider = _Spider()
    backend = manager.get_storage_backend.return_value
    backend.store.side_effect = [RuntimeError("first"), None]

    item = {"name": "counter"}
    assert pipeline.process_item(item, spider) is item
    assert pipeline._consecutive_storage_errors == 1
    assert pipeline.process_item(item, spider) is item
    assert pipeline._consecutive_storage_errors == 0
    assert monitor.on_error.call_count == 1
    assert monitor.on_store.call_count == 1
    assert backend.store.call_count == 2


def test_pipeline_close_runs_strategy_before_exact_manager_release(
    mocker: Any,
) -> None:
    """Close ownership is durable: strategy completion precedes manager release."""
    manager = Mock(name="manager")
    strategy = Mock(name="strategy")
    calls: list[str] = []
    strategy.close.side_effect = lambda: calls.append("strategy.close")
    manager.close.side_effect = lambda: calls.append("manager.close")
    pipeline = BackendPipeline(manager, storage_strategy=strategy)

    pipeline.close_spider(_Spider())
    assert calls == ["strategy.close", "manager.close"]
    assert pipeline._closed is True
    assert pipeline._manager_released is True
