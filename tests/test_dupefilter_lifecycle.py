"""Deterministic admission/barrier contracts for BackendDupeFilter."""

from __future__ import annotations

import gc
from threading import Barrier, Event, Thread
from typing import Any, get_type_hints
from unittest.mock import MagicMock
from weakref import ref

import pytest
from scrapy import Spider
from scrapy.http import Request
from twisted.internet.defer import Deferred, succeed

import scrapy_extension.schedule.scheduler as scheduler_module
import scrapy_extension.utils.reactor as reactor_module
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions.base import QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin


def _dupefilter(membership_filter: MembershipFilter) -> BackendDupeFilter:
    return BackendDupeFilter(
        connection_manager=MagicMock(),
        membership_filter=membership_filter,
    )


class _TrackingMembershipFilter(MembershipFilter):
    """Small exact filter that exposes marker ownership to lifecycle tests."""

    def __init__(self) -> None:
        self.items: set[bytes] = set()
        self.add_calls: list[bytes] = []
        self.remove_calls: list[bytes] = []
        self.clear_calls = 0

    def add(self, item: bytes) -> bool:
        self.add_calls.append(item)
        if item in self.items:
            return False
        self.items.add(item)
        return True

    def __contains__(self, item: bytes) -> bool:
        return item in self.items

    def __len__(self) -> int:
        return len(self.items)

    def clear(self) -> None:
        self.clear_calls += 1
        self.items.clear()

    def remove(self, item: bytes) -> bool:
        self.remove_calls.append(item)
        if item not in self.items:
            return False
        self.items.remove(item)
        return True


class _AlwaysNewMembershipFilter(_TrackingMembershipFilter):
    """Return a new admission for every call to create multiple weak receipts."""

    def add(self, item: bytes) -> bool:
        self.add_calls.append(item)
        self.items.add(item)
        return True


class _WriteThenInterruptFilter(_TrackingMembershipFilter):
    """Exact filter whose ``add`` writes the marker, then raises a control signal."""

    def __init__(self) -> None:
        super().__init__()
        self.interrupt_next_add = False

    def add(self, item: bytes) -> bool:
        was_new = super().add(item)
        if self.interrupt_next_add:
            self.interrupt_next_add = False
            raise KeyboardInterrupt("interrupted after marker write")
        return was_new


class _BlockingLifecycleFilter(MemoryMembershipFilter):
    """Real filter with event barriers around each lifecycle callback."""

    def __init__(self) -> None:
        super().__init__(maxsize=None)
        self.open_entered = Event()
        self.open_release = Event()
        self.clear_entered = Event()
        self.clear_release = Event()
        self.close_entered = Event()
        self.close_release = Event()

    def open(self) -> None:
        self.open_entered.set()
        self.open_release.wait(timeout=2.0)

    def clear(self) -> None:
        self.clear_entered.set()
        self.clear_release.wait(timeout=2.0)
        super().clear()

    def close(self) -> None:
        self.close_entered.set()
        self.close_release.wait(timeout=2.0)


class _BlockingAddFilter(MemoryMembershipFilter):
    """Hold one admitted membership call until the transition is waiting."""

    def __init__(self) -> None:
        super().__init__(maxsize=None)
        self.add_entered = Event()
        self.add_release = Event()
        self.clear_entered = Event()

    def add(self, item: bytes) -> bool:
        self.add_entered.set()
        self.add_release.wait(timeout=2.0)
        return super().add(item)

    def clear(self) -> None:
        self.clear_entered.set()
        super().clear()


class _ManualDelayedCall:
    def __init__(self, callback: object, *, fail_cancel: bool = False) -> None:
        self.callback = callback
        self.cancelled = False
        self.fail_cancel = fail_cancel

    def active(self) -> bool:
        return not self.cancelled

    def cancel(self) -> None:
        if self.fail_cancel:
            raise KeyboardInterrupt("timer cancellation failed")
        self.cancelled = True


class _AsyncAdapterHarness:
    """Submit real Deferred workers while keeping execution under test control."""

    def __init__(self, *, fail_cancel: bool = False) -> None:
        self.jobs: list[
            tuple[object, tuple[object, ...], dict[str, object], Deferred[Any]]
        ] = []
        self.calls: list[_ManualDelayedCall] = []
        self.fail_cancel = fail_cancel

    def submit(
        self,
        function: object,
        *args: object,
        **kwargs: object,
    ) -> Deferred[Any]:
        worker: Deferred[Any] = Deferred()
        self.jobs.append((function, args, kwargs, worker))
        return worker

    def callLater(self, _delay: float, callback: object) -> _ManualDelayedCall:
        call = _ManualDelayedCall(callback, fail_cancel=self.fail_cancel)
        self.calls.append(call)
        return call


class _AlwaysRejectingObserverDeferred(Deferred[Any]):
    """A real accepted worker whose first observer registrations are unavailable."""

    def __init__(self) -> None:
        super().__init__()
        self.rejections = 2

    def addCallbacks(self, *args: object, **kwargs: object) -> object:
        if self.rejections:
            self.rejections -= 1
            del args, kwargs
            raise RuntimeError("callback observer rejected")
        return super().addCallbacks(*args, **kwargs)

    def addErrback(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RuntimeError("errback observer rejected")


class _BlockingBackendDupeFilter(BackendDupeFilter):
    def __init__(self, manager: MagicMock) -> None:
        super().__init__(manager)
        self.close_calls: list[str] = []
        self.close_result: Deferred[object] = Deferred()

    def close(self, reason: str) -> Deferred[object]:
        self.close_calls.append(reason)
        return self.close_result


def test_backend_dupefilter_subclass_close_is_off_reactor_and_awaited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = MagicMock()
    dupefilter = _BlockingBackendDupeFilter(manager)
    operation: Deferred[object] = Deferred()
    bounded: Deferred[object] = Deferred()
    workers: list[object] = []

    def ordered(function: object, *args: object, **_kwargs: object):
        def run() -> None:
            assert callable(function)
            returned = function(*args)
            assert isinstance(returned, Deferred)
            returned.addCallbacks(operation.callback, operation.errback)

        workers.append(run)
        return operation, bounded

    monkeypatch.setattr(scheduler_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(
        scheduler_module, "bounded_deferred", lambda source, **_: source
    )
    monkeypatch.setattr(
        scheduler_module,
        "deferToThread",
        lambda function, *args, **kwargs: succeed(function(*args, **kwargs)),
    )
    monkeypatch.setattr(reactor_module, "defer_to_thread_ordered", ordered)
    scheduler = BackendScheduler(manager, dupefilter=dupefilter)
    scheduler._queue = None
    scheduler._lifecycle_state = "open"

    closing = scheduler.close("subclass-close")
    assert isinstance(closing, Deferred)
    assert dupefilter.close_calls == []
    assert workers

    workers[0]()
    assert dupefilter.close_calls == ["subclass-close"]
    assert not closing.called
    dupefilter.close_result.callback(None)

    assert closing.called
    manager.close.assert_called_once_with()


def test_same_request_legacy_receipts_are_invocation_specific() -> None:
    membership = MagicMock(spec=MembershipFilter)
    membership.add.side_effect = [True, True]
    dupefilter = _dupefilter(membership)
    request = Request("https://example.test/repeated-legacy")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.request_seen(request) is False
    assert len(dupefilter._legacy_reservations[id(request)]) == 2

    assert dupefilter.consume_reservation(request) is True
    assert dupefilter.consume_reservation(request) is True
    # Repeated consumption cannot settle either receipt a second time.
    assert dupefilter.consume_reservation(request) is False
    dupefilter.forget(request)
    dupefilter.forget(request)
    dupefilter.forget(request)

    assert not dupefilter._legacy_reservations
    assert not dupefilter._legacy_handoffs
    assert membership.remove.call_count == 2


def test_same_request_concurrent_legacy_receipts_do_not_cross_settle() -> None:
    membership = MagicMock(spec=MembershipFilter)
    membership.add.side_effect = lambda _fingerprint: True
    dupefilter = _dupefilter(membership)
    request = Request("https://example.test/concurrent-legacy")
    barrier = Barrier(2)
    outcomes: list[tuple[bool, bool]] = []

    def enqueue_and_settle() -> None:
        barrier.wait()
        seen = dupefilter.request_seen(request)
        consumed = dupefilter.consume_reservation(request)
        assert dupefilter.settle_reservation(request) is True
        outcomes.append((seen, consumed))

    workers = [Thread(target=enqueue_and_settle) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=2.0)
    assert all(not worker.is_alive() for worker in workers)
    assert outcomes == [(False, True), (False, True)]
    assert not dupefilter._legacy_handoffs
    assert not dupefilter._legacy_reservations


def test_request_seen_is_rejected_while_open_is_in_flight() -> None:
    membership = MagicMock(spec=MembershipFilter)
    entered = Event()
    release = Event()

    def blocked_open() -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    membership.open.side_effect = blocked_open
    dupefilter = _dupefilter(membership)
    errors: list[BaseException] = []

    opener = Thread(
        target=lambda: _capture_error(errors, dupefilter.open),
        name="dupefilter-open",
    )
    opener.start()
    assert entered.wait(timeout=2.0)

    with pytest.raises(RuntimeError, match="opening"):
        dupefilter.request_seen(Request("https://example.test/opening"))
    membership.add.assert_not_called()

    release.set()
    opener.join(timeout=2.0)
    assert not opener.is_alive()
    assert errors == []


def test_request_seen_is_rejected_while_clear_is_in_flight() -> None:
    membership = MagicMock(spec=MembershipFilter)
    entered = Event()
    release = Event()

    def blocked_clear() -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    membership.clear.side_effect = blocked_clear
    dupefilter = _dupefilter(membership)
    dupefilter.open()

    clearer = Thread(target=dupefilter.clear, name="dupefilter-clear")
    clearer.start()
    assert entered.wait(timeout=2.0)

    with pytest.raises(RuntimeError, match="clearing"):
        dupefilter.request_seen(Request("https://example.test/clearing"))
    membership.add.assert_not_called()

    release.set()
    clearer.join(timeout=2.0)
    assert not clearer.is_alive()


def test_lifecycle_state_transitions_reject_each_inflight_boundary() -> None:
    membership = _BlockingLifecycleFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)
    spider = Spider(name="lifecycle-boundary")

    assert dupefilter._lifecycle_state == "new"
    opener = Thread(target=dupefilter.open, args=(spider,), name="boundary-open")
    opener.start()
    assert membership.open_entered.wait(timeout=2.0)
    assert dupefilter._lifecycle_state == "opening"
    with pytest.raises(RuntimeError, match="open is already in progress"):
        dupefilter.open(spider)
    with pytest.raises(RuntimeError, match="already in progress"):
        dupefilter.clear()
    with pytest.raises(RuntimeError, match="opening"):
        dupefilter.request_seen(Request("https://example.test/opening-boundary"))

    membership.open_release.set()
    opener.join(timeout=2.0)
    assert not opener.is_alive()
    assert dupefilter._lifecycle_state == "open"

    clearer = Thread(target=dupefilter.clear, name="boundary-clear")
    clearer.start()
    assert membership.clear_entered.wait(timeout=2.0)
    assert dupefilter._lifecycle_state == "clearing"
    with pytest.raises(RuntimeError, match="already in progress"):
        dupefilter.open(spider)
    with pytest.raises(RuntimeError, match="already in progress"):
        dupefilter.clear()
    with pytest.raises(RuntimeError, match="clearing"):
        dupefilter.request_seen(Request("https://example.test/clearing-boundary"))

    membership.clear_release.set()
    clearer.join(timeout=2.0)
    assert not clearer.is_alive()
    assert dupefilter._lifecycle_state == "open"

    closer = Thread(target=dupefilter.close, args=("boundary",), name="boundary-close")
    closer.start()
    assert membership.close_entered.wait(timeout=2.0)
    assert dupefilter._lifecycle_state == "closing"
    with pytest.raises(RuntimeError, match="closing or closed"):
        dupefilter.open(spider)
    with pytest.raises(RuntimeError, match="closing or closed"):
        dupefilter.clear()
    with pytest.raises(RuntimeError, match="closing"):
        dupefilter.request_seen(Request("https://example.test/closing-boundary"))

    membership.close_release.set()
    closer.join(timeout=2.0)
    assert not closer.is_alive()
    assert dupefilter._lifecycle_state == "closed"
    with pytest.raises(RuntimeError, match="closed"):
        dupefilter.request_seen(Request("https://example.test/closed-boundary"))


def test_clear_drains_admitted_call_before_resetting_marker_generation() -> None:
    membership = _BlockingAddFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)
    request = Request("https://example.test/admitted-drain")
    request_errors: list[BaseException] = []

    def request_seen() -> None:
        _capture_error(request_errors, dupefilter.request_seen, request)

    request_thread = Thread(target=request_seen, name="admitted-request")
    request_thread.start()
    assert membership.add_entered.wait(timeout=2.0)

    clear_thread = Thread(target=dupefilter.clear, name="admitted-clear")
    clear_thread.start()
    with dupefilter._lifecycle_condition:
        assert dupefilter._lifecycle_condition.wait_for(
            lambda: dupefilter._lifecycle_state == "clearing",
            timeout=2.0,
        )
    assert not membership.clear_entered.is_set()

    membership.add_release.set()
    request_thread.join(timeout=2.0)
    clear_thread.join(timeout=2.0)
    assert not request_thread.is_alive()
    assert not clear_thread.is_alive()
    assert request_errors == []
    assert membership.clear_entered.is_set()
    assert len(membership) == 0
    # The old request's receipt was retired with the generation; it cannot be
    # handed to a caller after the transition completed.
    assert dupefilter.consume_reservation(request) is False


def test_filter_callback_same_request_reentry_is_observational_without_deadlock() -> (
    None
):
    class ReentrantFilter(MemoryMembershipFilter):
        def __init__(self) -> None:
            super().__init__(maxsize=None)
            self.dupefilter: BackendDupeFilter | None = None
            self.request = Request("https://example.test/filter-callback-reentry")
            self.worker: Thread | None = None
            self.nested_results: list[bool] = []

        def add(self, item: bytes) -> bool:
            if self.worker is None:
                assert self.dupefilter is not None
                self.worker = Thread(
                    target=lambda: self.nested_results.append(
                        self.dupefilter.request_seen(self.request)
                    )
                )
                self.worker.start()
                self.worker.join(timeout=2.0)
                assert not self.worker.is_alive()
            return super().add(item)

    membership = ReentrantFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)
    membership.dupefilter = dupefilter

    assert dupefilter.request_seen(membership.request) is False
    assert membership.nested_results == [True]
    assert dupefilter._active_filter_requests == {}
    assert dupefilter.consume_reservation(membership.request) is True
    assert dupefilter.settle_reservation(membership.request) is True


def test_reentrant_admitted_calls_keep_epoch_and_thread_counts_balanced() -> None:
    class ReentrantFilter(MemoryMembershipFilter):
        def __init__(self) -> None:
            super().__init__(maxsize=None)
            self.dupefilter: BackendDupeFilter | None = None
            self.nested_request = Request("https://example.test/nested-admitted")
            self.nested_results: list[bool] = []
            self.in_nested = False

        def add(self, item: bytes) -> bool:
            if not self.in_nested:
                self.in_nested = True
                assert self.dupefilter is not None
                self.nested_results.append(
                    self.dupefilter.request_seen(self.nested_request)
                )
                self.in_nested = False
            return super().add(item)

    membership = ReentrantFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)
    membership.dupefilter = dupefilter
    outer_request = Request("https://example.test/outer-admitted")

    assert dupefilter.request_seen(outer_request) is False
    assert membership.nested_results == [False]
    assert dupefilter._active_operations == 0
    assert dupefilter.consume_reservation(membership.nested_request) is True
    assert dupefilter.consume_reservation(outer_request) is True
    assert dupefilter.settle_reservation(membership.nested_request) is True
    assert dupefilter.settle_reservation(outer_request) is True
    assert len(membership) == 2


def test_clear_reentry_from_an_admitted_filter_call_restores_new_state() -> None:
    class ClearReenterFilter(MemoryMembershipFilter):
        def __init__(self) -> None:
            super().__init__(maxsize=None)
            self.dupefilter: BackendDupeFilter | None = None
            self.reentry_error: RuntimeError | None = None

        def add(self, item: bytes) -> bool:
            assert self.dupefilter is not None
            try:
                self.dupefilter.clear()
            except RuntimeError as error:
                self.reentry_error = error
            return super().add(item)

    membership = ClearReenterFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)
    membership.dupefilter = dupefilter
    request = Request("https://example.test/clear-reentry")

    assert dupefilter.request_seen(request) is False
    assert membership.reentry_error is not None
    assert "active operation" in str(membership.reentry_error)
    assert dupefilter._lifecycle_state == "new"
    assert dupefilter.consume_reservation(request) is True
    assert dupefilter.settle_reservation(request) is True
    assert len(membership) == 1


def test_failed_clear_can_be_retried_without_retiring_the_generation() -> None:
    membership = MagicMock(spec=MembershipFilter)
    membership.clear.side_effect = [RuntimeError("transient clear"), None]
    dupefilter = _dupefilter(membership)

    with pytest.raises(RuntimeError, match="transient clear"):
        dupefilter.clear()
    assert dupefilter._lifecycle_state == "new"

    dupefilter.clear()
    assert membership.clear.call_count == 2
    assert dupefilter._lifecycle_state == "new"


def test_clear_waits_for_an_admitted_request_before_filter_clear() -> None:
    membership = MagicMock(spec=MembershipFilter)
    request_entered = Event()
    release_request = Event()

    def blocked_add(_fingerprint: bytes) -> bool:
        request_entered.set()
        assert release_request.wait(timeout=2.0)
        return True

    membership.add.side_effect = blocked_add
    dupefilter = _dupefilter(membership)
    request_errors: list[BaseException] = []

    request = Request("https://example.test/in-flight")

    def admit_and_handoff() -> None:
        _capture_error(request_errors, dupefilter.request_seen, request)
        dupefilter.consume_reservation(request)

    request_thread = Thread(
        target=admit_and_handoff,
        name="dupefilter-request",
    )
    request_thread.start()
    assert request_entered.wait(timeout=2.0)

    clear_done = Event()

    def clear() -> None:
        dupefilter.clear()
        clear_done.set()

    clear_thread = Thread(target=clear, name="dupefilter-clear")
    clear_thread.start()
    assert not clear_done.wait(timeout=0.05)
    membership.clear.assert_not_called()

    release_request.set()
    request_thread.join(timeout=2.0)
    clear_thread.join(timeout=2.0)
    assert not request_thread.is_alive()
    assert not clear_thread.is_alive()
    assert request_errors == []
    membership.clear.assert_called_once_with()


def test_clear_waits_for_an_admitted_commit_before_filter_clear() -> None:
    membership = MemoryMembershipFilter()
    dupefilter = _dupefilter(membership)
    request = Request("https://example.test/commit-barrier")
    decision = dupefilter.request_seen_with_reservation(request)
    assert decision.reservation is not None

    commit_entered = Event()
    release_commit = Event()
    original_add = membership.add

    def blocked_commit(fingerprint: bytes) -> bool:
        commit_entered.set()
        assert release_commit.wait(timeout=2.0)
        return original_add(fingerprint)

    membership.add = blocked_commit  # type: ignore[method-assign]
    commit_thread = Thread(
        target=lambda: dupefilter.commit_reservation(decision.reservation),
        name="dupefilter-commit",
    )
    commit_thread.start()
    assert commit_entered.wait(timeout=2.0)

    clear_thread = Thread(target=dupefilter.clear, name="dupefilter-clear")
    clear_thread.start()
    assert clear_thread.is_alive()
    assert len(membership) == 0

    release_commit.set()
    commit_thread.join(timeout=2.0)
    clear_thread.join(timeout=2.0)
    assert not commit_thread.is_alive()
    assert not clear_thread.is_alive()
    assert len(membership) == 0


def test_commit_arriving_after_clear_stops_admission_is_a_noop() -> None:
    membership = MemoryMembershipFilter()
    dupefilter = _dupefilter(membership)
    decision = dupefilter.request_seen_with_reservation(
        Request("https://example.test/late-commit")
    )
    assert decision.reservation is not None

    entered = Event()
    release = Event()
    original_clear = membership.clear

    def blocked_clear() -> None:
        entered.set()
        assert release.wait(timeout=2.0)
        original_clear()

    membership.clear = blocked_clear  # type: ignore[method-assign]
    add = MagicMock(side_effect=membership.add)
    membership.add = add  # type: ignore[method-assign]
    clear_thread = Thread(target=dupefilter.clear, name="dupefilter-clear")
    clear_thread.start()
    assert entered.wait(timeout=2.0)

    dupefilter.commit_reservation(decision.reservation)
    add.assert_not_called()

    release.set()
    clear_thread.join(timeout=2.0)
    assert not clear_thread.is_alive()
    assert dupefilter.request_seen(Request("https://example.test/late-commit")) is False


def test_close_waits_for_a_blocked_clear_before_filter_close() -> None:
    membership = MagicMock(spec=MembershipFilter)
    entered = Event()
    release = Event()

    def blocked_clear() -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    membership.clear.side_effect = blocked_clear
    dupefilter = _dupefilter(membership)
    dupefilter.open()

    clear_thread = Thread(target=dupefilter.clear, name="dupefilter-clear")
    clear_thread.start()
    assert entered.wait(timeout=2.0)

    close_done = Event()
    close_errors: list[BaseException] = []

    def close() -> None:
        _capture_error(close_errors, dupefilter.close, "close")
        close_done.set()

    close_thread = Thread(target=close, name="dupefilter-close")
    close_thread.start()
    assert not close_done.wait(timeout=0.05)
    membership.close.assert_not_called()

    release.set()
    clear_thread.join(timeout=2.0)
    close_thread.join(timeout=2.0)
    assert not clear_thread.is_alive()
    assert not close_thread.is_alive()
    assert close_errors == []
    membership.close.assert_called_once_with()


def test_stale_forget_cannot_remove_a_marker_from_a_new_epoch() -> None:
    membership = _TrackingMembershipFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)
    old_request = Request("https://example.test/stale-forget")
    assert dupefilter.request_seen(old_request) is False
    assert dupefilter.consume_reservation(old_request) is True

    dupefilter.clear()
    new_request = old_request.replace()
    assert dupefilter.request_seen(new_request) is False
    dupefilter.forget(old_request)

    assert membership.remove_calls == []
    assert dupefilter.request_seen(new_request.replace()) is True


def test_stale_commit_and_rollback_cannot_mutate_replacement_generation() -> None:
    membership = _TrackingMembershipFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)
    old = dupefilter.request_seen_with_reservation(
        Request("https://example.test/stale-transaction")
    )
    assert old.reservation is not None

    dupefilter.clear()
    replacement = dupefilter.request_seen_with_reservation(
        Request("https://example.test/stale-transaction")
    )
    assert replacement.reservation is not None
    add_count_before_stale_settlements = len(membership.add_calls)

    dupefilter.commit_reservation(old.reservation)
    dupefilter.rollback_reservation(old.reservation)
    assert len(membership.add_calls) == add_count_before_stale_settlements
    assert membership.items == set()

    dupefilter.commit_reservation(replacement.reservation)
    assert len(membership.add_calls) == add_count_before_stale_settlements + 1
    assert (
        dupefilter.request_seen(Request("https://example.test/stale-transaction"))
        is True
    )


def test_legacy_receipts_release_request_references_via_weakref_cleanup() -> None:
    membership = _AlwaysNewMembershipFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)

    def submit_receipts() -> Any:
        request = Request("https://example.test/weak-legacy")
        marker = ref(request)
        assert dupefilter.request_seen(request) is False
        assert dupefilter.request_seen(request) is False
        return marker

    marker = submit_receipts()
    gc.collect()
    assert marker() is None
    assert not dupefilter._legacy_reservations
    assert not dupefilter._legacy_handoffs
    assert not dupefilter._pending_reservations


def test_late_forget_cannot_remove_same_request_replacement_generation() -> None:
    membership = MemoryMembershipFilter()
    dupefilter = _dupefilter(membership)
    request = Request("https://example.test/same-request-generation")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    dupefilter.clear()
    assert dupefilter.request_seen(request) is False

    # The old receipt was cancelled by clear(); its late forget must not settle
    # the new pending receipt carried by the same long-lived Request object.
    dupefilter.forget(request)
    assert dupefilter.request_seen(request.replace()) is True


def test_open_retires_legacy_receipt_before_the_next_generation() -> None:
    membership = MemoryMembershipFilter()
    dupefilter = _dupefilter(membership)
    old_request = Request("https://example.test/open-boundary")

    assert dupefilter.request_seen(old_request) is False
    dupefilter.open()

    # The marker was written before open, but its old receipt must not be able to
    # compensate the newly opened generation.
    dupefilter.forget(old_request)
    assert dupefilter.request_seen(old_request.replace()) is True


def test_baseexception_filter_close_is_retryable_without_manager_release() -> None:
    class CloseOnceFilter(MemoryMembershipFilter):
        def __init__(self) -> None:
            super().__init__(maxsize=None)
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise KeyboardInterrupt("close interrupted")

    membership = CloseOnceFilter()
    dupefilter = BackendDupeFilter(None, membership_filter=membership)

    with pytest.raises(KeyboardInterrupt, match="close interrupted"):
        dupefilter.close("first")
    assert dupefilter._lifecycle_state == "closing"
    assert dupefilter._closed is False

    dupefilter.close("retry")
    assert membership.close_calls == 2
    assert dupefilter._lifecycle_state == "closed"


def _run_async_job(
    harness: _AsyncAdapterHarness,
    index: int,
    *,
    error: BaseException | None = None,
) -> None:
    function, args, kwargs, worker = harness.jobs[index]
    assert callable(function)
    function(*args, **kwargs)
    if error is None:
        worker.callback(None)
    else:
        worker.errback(error)


def test_async_lifecycle_adapters_submit_real_workers_and_close_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scrapy_extension.utils.reactor as reactor_module

    harness = _AsyncAdapterHarness()
    monkeypatch.setattr(reactor_module, "deferToThread", harness.submit)
    monkeypatch.setattr(reactor_module, "_reactor", lambda: harness)
    dupefilter = BackendDupeFilter(None, membership_filter=MemoryMembershipFilter())
    spider = Spider(name="async-lifecycle")

    opened = dupefilter.open_async(spider, timeout=2.0)
    assert isinstance(opened, Deferred)
    assert not opened.called
    _run_async_job(harness, 0)
    assert opened.called
    assert dupefilter._lifecycle_state == "open"

    cleared = dupefilter.clear_async(timeout=2.0)
    _run_async_job(harness, 1)
    assert cleared.called
    assert dupefilter._lifecycle_state == "open"

    released = dupefilter.release_async(
        dupefilter._direct_release_owner,
        "async-finished",
        timeout=2.0,
    )
    _run_async_job(harness, 2)
    assert released.called
    assert dupefilter._lifecycle_state == "closed"
    assert len(harness.calls) == 3
    assert all(call.cancelled for call in harness.calls)


def test_async_submission_failure_leaves_new_state_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scrapy_extension.utils.reactor as reactor_module

    harness = _AsyncAdapterHarness()
    attempts = 0

    def submit(function: object, *args: object, **kwargs: object) -> Deferred[Any]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("thread submission failed")
        return harness.submit(function, *args, **kwargs)

    monkeypatch.setattr(reactor_module, "deferToThread", submit)
    monkeypatch.setattr(reactor_module, "_reactor", lambda: harness)
    dupefilter = BackendDupeFilter(None, membership_filter=MemoryMembershipFilter())

    failed = dupefilter.open_async(timeout=2.0)
    failed.addErrback(lambda _failure: None)
    assert failed.called
    assert dupefilter._lifecycle_state == "new"

    retried = dupefilter.open_async(timeout=2.0)
    _run_async_job(harness, 0)
    assert retried.called
    assert dupefilter._lifecycle_state == "open"


def test_async_callback_observer_and_timer_cancellation_failures_are_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scrapy_extension.utils.reactor as reactor_module

    harness = _AsyncAdapterHarness(fail_cancel=True)
    worker = _AlwaysRejectingObserverDeferred()
    harness.jobs.append((lambda: None, (), {}, worker))
    monkeypatch.setattr(
        reactor_module,
        "deferToThread",
        lambda _function, *_args, **_kwargs: worker,
    )
    monkeypatch.setattr(reactor_module, "_reactor", lambda: harness)
    dupefilter = BackendDupeFilter(None, membership_filter=MemoryMembershipFilter())

    # The adapter accepted the worker but could not install its callback. The
    # lifecycle wrapper must not turn observer failure into a synchronous raise.
    failed = dupefilter.open_async(timeout=2.0)
    failed.addErrback(lambda _failure: None)
    assert failed.called
    assert dupefilter._lifecycle_state == "new"
    Deferred.addErrback(worker, lambda _failure: None)

    # A normal accepted worker still reaches the real lifecycle method, and a
    # timer cancellation BaseException remains advisory after success.
    normal = _AsyncAdapterHarness(fail_cancel=True)
    monkeypatch.setattr(reactor_module, "deferToThread", normal.submit)
    monkeypatch.setattr(reactor_module, "_reactor", lambda: normal)
    opened = dupefilter.open_async(timeout=2.0)
    _run_async_job(normal, 0)
    assert opened.called
    assert dupefilter._lifecycle_state == "open"
    assert normal.calls[0].cancelled is False


def test_async_observer_failures_are_ignored_for_clear_and_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scrapy_extension.utils.reactor as reactor_module

    dupefilter = BackendDupeFilter(None, membership_filter=MemoryMembershipFilter())
    dupefilter.open()
    workers: list[_AlwaysRejectingObserverDeferred] = []

    def reject_submission(
        _function: object,
        *_args: object,
        **_kwargs: object,
    ) -> _AlwaysRejectingObserverDeferred:
        worker = _AlwaysRejectingObserverDeferred()
        workers.append(worker)
        return worker

    monkeypatch.setattr(reactor_module, "deferToThread", reject_submission)
    monkeypatch.setattr(reactor_module, "_reactor", lambda: _AsyncAdapterHarness())

    cleared = dupefilter.clear_async(timeout=2.0)
    cleared.addErrback(lambda _failure: None)
    released = dupefilter.release_async(
        dupefilter._direct_release_owner,
        "observer-failure",
        timeout=2.0,
    )
    released.addErrback(lambda _failure: None)

    assert dupefilter._lifecycle_state == "open"
    assert len(workers) == 2
    for worker in workers:
        Deferred.addErrback(worker, lambda _failure: None)
    dupefilter.close("cleanup")


def test_mixin_waits_for_async_dupefilter_before_releasing_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TestSpider(BackendSpiderMixin, Spider):
        name = "dupefilter-parent-barrier"

    spider = TestSpider()
    manager = MagicMock()
    spider._connection_manager = manager
    scheduler_close: Deferred[object] = Deferred()
    scheduler = MagicMock()
    scheduler.close.return_value = scheduler_close
    scheduler._close_completion_deferred = None
    scheduler.dupefilter = None
    spider._scheduler = scheduler

    dupefilter = _dupefilter(MemoryMembershipFilter())
    dupefilter_operation: Deferred[object] = Deferred()
    dupefilter_bounded: Deferred[object] = Deferred()
    dupefilter._close_authoritative_async = MagicMock(  # type: ignore[method-assign]
        return_value=(dupefilter_operation, dupefilter_bounded)
    )
    spider._dupefilter = dupefilter

    monkeypatch.setattr(
        "scrapy_extension.spider.spider_mixin.reactor_is_running", lambda: True
    )
    monkeypatch.setattr(
        "scrapy_extension.spider.spider_mixin.bounded_deferred",
        lambda source, **_kwargs: source,
    )

    closing = spider.close_backend()
    scheduler_close.callback(None)
    assert isinstance(closing, Deferred)
    assert not closing.called
    manager.close.assert_not_called()

    dupefilter_operation.callback(None)
    assert closing.called
    manager.close.assert_called_once_with()


def test_mixin_offloads_direct_dupefilter_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TestSpider(BackendSpiderMixin, Spider):
        name = "dupefilter-close"

    spider = TestSpider()
    dupefilter = _dupefilter(MemoryMembershipFilter())
    worker: Deferred[object] = Deferred()
    bounded: Deferred[object] = Deferred()
    dupefilter._close_authoritative_async = MagicMock(  # type: ignore[method-assign]
        return_value=(worker, bounded)
    )
    spider._dupefilter = dupefilter
    monkeypatch.setattr(
        "scrapy_extension.spider.spider_mixin.reactor_is_running", lambda: True
    )
    monkeypatch.setattr(
        "scrapy_extension.spider.spider_mixin.bounded_deferred",
        lambda source, **_kwargs: source,
    )

    closing = spider.close_backend()
    assert isinstance(closing, Deferred)
    dupefilter._close_authoritative_async.assert_called_once()
    assert spider._dupefilter is dupefilter

    worker.callback(None)
    assert closing.called
    assert spider._dupefilter is None


def test_nonremovable_failed_push_backpressures_without_evicting_recovery() -> None:
    class LegacyDupeFilter(BackendDupeFilter):
        def request_seen(self, request: Request) -> bool:
            return super().request_seen(request)

    dupefilter = LegacyDupeFilter(
        connection_manager=MagicMock(),
        membership_filter=BloomMembershipFilter(capacity=100, error_rate=0.01),
    )
    dupefilter._retry_allowance_limit = 1
    queue = MagicMock(spec=BackendQueue)
    queue._push_with_durability.side_effect = QueueError(
        "queue unavailable", operation="push"
    )
    scheduler = BackendScheduler(
        connection_manager=MagicMock(),
        dupefilter=dupefilter,
        owns_connection_manager=False,
    )
    scheduler._queue = queue
    first = Request("https://example.test/retry-ledger/first")
    second = Request("https://example.test/retry-ledger/second")

    with pytest.raises(QueueError):
        scheduler.enqueue_request(first)
    with pytest.raises(QueueError):
        scheduler.enqueue_request(second)

    assert len(dupefilter._retry_allowances) == 1
    # The first marker's recovery remains reachable; the second request was
    # admitted without a marker while the bounded ledger was full.
    assert dupefilter.request_seen(first) is False
    assert dupefilter.consume_reservation(first) is True
    assert dupefilter.settle_reservation(first) is True
    assert dupefilter.request_seen(first) is True


def test_scheduler_settles_successful_legacy_handoff_immediately() -> None:
    class LegacyDupeFilter(BackendDupeFilter):
        def request_seen(self, request: Request) -> bool:
            return super().request_seen(request)

    dupefilter = LegacyDupeFilter(
        connection_manager=MagicMock(),
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    queue = MagicMock(spec=BackendQueue)
    scheduler = BackendScheduler(
        connection_manager=MagicMock(),
        dupefilter=dupefilter,
        owns_connection_manager=False,
    )
    scheduler._queue = queue

    request = Request("https://example.test/legacy-handoff")
    assert scheduler.enqueue_request(request) is True
    assert not dupefilter._legacy_reservations
    assert not dupefilter._legacy_handoffs
    assert len(dupefilter._pending_reservations) == 0


def test_scheduler_preserves_request_when_dupefilter_admission_is_unavailable() -> None:
    membership = MagicMock(spec=MembershipFilter)
    entered = Event()
    release = Event()

    def blocked_clear() -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    membership.clear.side_effect = blocked_clear
    dupefilter = _dupefilter(membership)
    dupefilter.open()
    clear_thread = Thread(target=dupefilter.clear, name="dupefilter-clear")
    clear_thread.start()
    assert entered.wait(timeout=2.0)

    queue = MagicMock(spec=BackendQueue)
    queue._push_with_durability.return_value = True
    scheduler = BackendScheduler(
        connection_manager=MagicMock(),
        dupefilter=dupefilter,
        owns_connection_manager=False,
    )
    scheduler._queue = queue

    request = Request("https://example.test/scheduler-preserve")
    assert scheduler.enqueue_request(request) is True
    queue.push.assert_called_once_with(request, priority=0)
    queue._push_with_durability.assert_not_called()

    release.set()
    clear_thread.join(timeout=2.0)
    assert not clear_thread.is_alive()


def test_filter_callbacks_run_without_the_lifecycle_lock() -> None:
    membership = MagicMock(spec=MembershipFilter)
    dupefilter = _dupefilter(membership)
    lock_owned: list[bool] = []

    def observe(_fingerprint: bytes) -> bool:
        lock_owned.append(dupefilter._lifecycle_lock._is_owned())  # type: ignore[attr-defined]
        return True

    membership.add.side_effect = observe
    dupefilter.request_seen(Request("https://example.test/unlocked"))

    assert lock_owned == [False]


def test_process_control_after_marker_write_grants_one_retry_allowance() -> None:
    membership = _WriteThenInterruptFilter()
    membership.interrupt_next_add = True
    dupefilter = _dupefilter(membership)
    request = Request("https://example.test/ghost-window")

    with pytest.raises(KeyboardInterrupt, match="interrupted after marker write"):
        dupefilter.request_seen(request)

    # The marker may already be written while no receipt was published. The
    # compensation must leave a recovery path instead of a permanent ghost.
    assert membership.items != set()
    assert dupefilter._legacy_reservations == {}
    assert dupefilter._retry_allowance_slots_reserved == 0
    assert len(dupefilter._retry_allowances) == 1

    # The one-shot allowance admits the same fingerprint exactly once ...
    assert dupefilter.request_seen(request.replace()) is False
    # ... while the retained marker keeps every later caller a duplicate.
    assert dupefilter.request_seen(request.replace()) is True


def test_async_lifecycle_annotations_resolve_runtime_deferred_hints() -> None:
    """The public async lifecycle contracts resolve via typing.get_type_hints.

    ``Deferred`` and ``Spider`` must be runtime imports of the dupefilter
    module: keeping either ``TYPE_CHECKING``-only makes ``get_type_hints``
    fail with ``NameError`` for the ``Deferred[None]`` return annotations
    and the ``open_async`` spider parameter.
    """
    assert get_type_hints(BackendDupeFilter.clear_async)["return"] == Deferred[None]
    assert get_type_hints(BackendDupeFilter.release_async)["return"] == Deferred[None]
    open_async_hints = get_type_hints(BackendDupeFilter.open_async)
    assert open_async_hints["return"] == Deferred[None]
    assert open_async_hints["spider"] == Spider | None
    assert open_async_hints["spider"].__args__ == (Spider, type(None))


def _capture_error(
    errors: list[BaseException],
    callback: object,
    *args: object,
) -> None:
    try:
        assert callable(callback)
        callback(*args)
    except BaseException as error:
        errors.append(error)
