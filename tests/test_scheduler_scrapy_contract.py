"""Scrapy 2.17 scheduler/dupefilter lifecycle contracts."""

from __future__ import annotations

from threading import Event, Thread, get_ident
from types import SimpleNamespace
from unittest.mock import MagicMock

from scrapy.dupefilters import RFPDupeFilter
from twisted.internet.defer import Deferred

import scrapy_extension.schedule.scheduler as scheduler_module
from scrapy_extension.schedule.scheduler import BackendScheduler


def _spider() -> SimpleNamespace:
    return SimpleNamespace(name="contract", crawler=None)


def test_scheduler_opens_standard_rfp_dupefilter_without_spider_argument(
    mocker,
) -> None:
    """Scrapy's generic RFPDupeFilter uses the no-argument open() contract."""
    manager = MagicMock(name="ConnectionManager")
    queue = MagicMock(name="BackendQueue")
    mocker.patch.object(scheduler_module, "BackendQueue", return_value=queue)
    dupefilter = RFPDupeFilter()
    scheduler = BackendScheduler(manager, dupefilter=dupefilter)

    assert scheduler.open(_spider()) is None
    scheduler.close("finished")

    queue.close.assert_called_once_with()
    manager.close.assert_called_once_with()


class _DelayedDupeFilter:
    def __init__(self) -> None:
        self.open_deferred: Deferred[None] = Deferred()
        self.close_deferred: Deferred[None] = Deferred()
        self.open_calls = 0
        self.close_reasons: list[str] = []

    def open(self) -> Deferred[None]:
        self.open_calls += 1
        return self.open_deferred

    def close(self, reason: str) -> Deferred[None]:
        self.close_reasons.append(reason)
        return self.close_deferred

    def request_seen(self, request: object) -> bool:
        del request
        return False

    def log(self, request: object, spider: object) -> None:
        del request, spider


def test_scheduler_builds_backend_queue_off_reactor_thread(mocker, monkeypatch) -> None:
    """Queue snapshot restore must not block Scrapy's reactor thread."""
    manager = MagicMock(name="ConnectionManager")
    queue = MagicMock(name="BackendQueue")
    reactor_thread_id = get_ident()
    constructor_thread_ids: list[int] = []
    constructed = Event()

    def construct_queue(*args, **kwargs):
        del args, kwargs
        constructor_thread_ids.append(get_ident())
        constructed.set()
        return queue

    mocker.patch.object(scheduler_module, "BackendQueue", side_effect=construct_queue)
    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(
        scheduler_module,
        "bounded_deferred",
        lambda source, **_kwargs: source,
    )
    warmup = Deferred()
    deferred_calls: list[tuple[object, tuple[object, ...]]] = []

    def defer_to_thread(function, *args, **kwargs):
        del kwargs
        deferred_calls.append((function, args))
        if len(deferred_calls) == 1:
            return warmup
        operation = Deferred()

        def run() -> None:
            try:
                operation.callback(function(*args))
            except BaseException as exc:
                operation.errback(exc)

        Thread(target=run, daemon=True).start()
        return operation

    monkeypatch.setattr(scheduler_module, "deferToThread", defer_to_thread)
    scheduler = BackendScheduler(manager)

    opening = scheduler.open(_spider())
    assert opening is warmup

    # This callback represents the reactor resuming after the off-reactor warm-up.
    # The queue constructor itself must be the second, worker-bound operation.
    warmup.callback(None)
    assert constructed.wait(timeout=2.0)
    assert len(constructor_thread_ids) == 1
    assert constructor_thread_ids[0] != reactor_thread_id


def test_scheduler_waits_for_delayed_cleanup_after_open_failure(mocker) -> None:
    """A failed queue setup waits for the generic dupefilter's close Deferred."""
    manager = MagicMock(name="ConnectionManager")
    close_deferred: Deferred[None] = Deferred()

    class SyncOpenDelayedClose:
        def open(self) -> None:
            return None

        def close(self, reason: str) -> Deferred[None]:
            del reason
            return close_deferred

    mocker.patch.object(
        scheduler_module,
        "BackendQueue",
        side_effect=RuntimeError("queue construction failed"),
    )
    scheduler = BackendScheduler(manager, dupefilter=SyncOpenDelayedClose())

    opening = scheduler.open(_spider())
    assert isinstance(opening, Deferred)
    opening.addErrback(lambda _failure: None)
    assert scheduler._lifecycle_state == "closing"
    assert manager.close.call_count == 0

    close_deferred.callback(None)
    assert manager.close.call_count == 1
    assert scheduler._lifecycle_state == "closed"


def test_scheduler_retries_after_delayed_dupefilter_close_failure(mocker) -> None:
    """A failed Deferred close keeps ownership for the next Scrapy close call."""
    manager = MagicMock(name="ConnectionManager")
    queue = MagicMock(name="BackendQueue")
    mocker.patch.object(scheduler_module, "BackendQueue", return_value=queue)

    class RetryDupeFilter(_DelayedDupeFilter):
        def __init__(self) -> None:
            super().__init__()
            self.retry_close: Deferred[None] = Deferred()

        def close(self, reason: str) -> Deferred[None] | None:
            self.close_reasons.append(reason)
            if len(self.close_reasons) == 1:
                return self.close_deferred
            return self.retry_close

    dupefilter = RetryDupeFilter()
    scheduler = BackendScheduler(manager, dupefilter=dupefilter)
    opening = scheduler.open(_spider())
    assert opening is dupefilter.open_deferred
    dupefilter.open_deferred.callback(None)

    closing = scheduler.close("first-close")
    assert closing is dupefilter.close_deferred
    dupefilter.close_deferred.errback(RuntimeError("delayed close failed"))
    assert scheduler._lifecycle_state == "closing"
    assert manager.close.call_count == 0

    retry = scheduler.close("retry-close")
    assert retry is dupefilter.retry_close
    dupefilter.retry_close.callback(None)
    assert scheduler._lifecycle_state == "closed"
    manager.close.assert_called_once_with()
    assert dupefilter.close_reasons == ["first-close", "retry-close"]


def test_scheduler_waits_for_delayed_dupefilter_lifecycle_hooks(mocker) -> None:
    """Queue publication and manager release follow Deferred hook completion."""
    manager = MagicMock(name="ConnectionManager")
    queue = MagicMock(name="BackendQueue")
    mocker.patch.object(scheduler_module, "BackendQueue", return_value=queue)
    dupefilter = _DelayedDupeFilter()
    scheduler = BackendScheduler(manager, dupefilter=dupefilter)

    opening = scheduler.open(_spider())
    assert opening is dupefilter.open_deferred
    assert scheduler._queue is None
    assert scheduler._lifecycle_state == "opening"

    dupefilter.open_deferred.callback(None)
    assert dupefilter.open_calls == 1
    assert scheduler._queue is queue
    assert scheduler._lifecycle_state == "open"

    closing = scheduler.close("delayed-close")
    assert closing is dupefilter.close_deferred
    owner = scheduler._close_attempt_owner
    assert owner is not None and owner.active
    assert dupefilter.close_reasons == ["delayed-close"]
    assert manager.close.call_count == 0
    assert scheduler._lifecycle_state == "closing"

    dupefilter.close_deferred.callback(None)
    assert manager.close.call_count == 1
    assert scheduler._lifecycle_state == "closed"
    queue.close.assert_called_once_with()
