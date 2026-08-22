"""Focused lifecycle, receipt, and re-entry contracts for the duplicate filter."""

from __future__ import annotations

import gc
import logging
import threading
from collections import deque
from typing import Any
from unittest.mock import Mock

import pytest
from scrapy.http import Request
from twisted.internet.defer import Deferred

import scrapy_extension.dupefilter.dupefilter as dupefilter_module
from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter, _MonitorFenceToken
from scrapy_extension.dupefilter.filters.base import FilterFull, MembershipFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.monitor.base import Monitor


class _StateFilter(MembershipFilter):
    """Small exact filter whose callbacks can be made to fail or re-enter."""

    def __init__(self) -> None:
        self.items: set[bytes] = set()
        self.add_calls = 0
        self.contains_calls = 0
        self.remove_calls = 0
        self.add_error: BaseException | None = None
        self.contains_error: BaseException | None = None
        self.remove_error: BaseException | None = None
        self.on_add: Any | None = None

    def add(self, item: bytes) -> bool:
        self.add_calls += 1
        if self.on_add is not None:
            callback = self.on_add
            self.on_add = None
            callback()
        if self.add_error is not None:
            raise self.add_error
        was_new = item not in self.items
        self.items.add(item)
        return was_new

    def __contains__(self, item: bytes) -> bool:
        self.contains_calls += 1
        if self.contains_error is not None:
            raise self.contains_error
        return item in self.items

    def __len__(self) -> int:
        return len(self.items)

    def clear(self) -> None:
        self.items.clear()

    def remove(self, item: bytes) -> bool:
        self.remove_calls += 1
        if self.remove_error is not None:
            raise self.remove_error
        if item in self.items:
            self.items.remove(item)
            return True
        return False


class _RecordingMonitor(Monitor):
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []
        self.dupefilter: BackendDupeFilter | None = None
        self.request: Request | None = None

    def on_dedup_miss(self, key: str) -> None:
        self.events.append(("miss", key))
        if self.dupefilter is not None and self.request is not None:
            decision = self.dupefilter.request_seen_with_reservation(self.request)
            assert decision.seen is True
            assert decision.observational is True
            assert decision.reservation is None

    def on_error(self, operation: str, error: BaseException) -> None:
        self.events.append(("error", (operation, error)))


class _AsyncHarness:
    """Deterministic worker/clock seam for the real async adapter methods."""

    def __init__(self) -> None:
        self.jobs: deque[tuple[Any, tuple[Any, ...], dict[str, Any], Deferred[Any]]] = (
            deque()
        )
        self.timers: list[Mock] = []

    def submit(self, function: Any, *args: Any, **kwargs: Any) -> Deferred[Any]:
        worker: Deferred[Any] = Deferred()
        self.jobs.append((function, args, kwargs, worker))
        return worker

    def callLater(self, _delay: float, _callback: Any) -> Mock:
        timer = Mock()
        timer.active.return_value = True
        self.timers.append(timer)
        return timer

    def run_next(self) -> None:
        function, args, kwargs, worker = self.jobs.popleft()
        try:
            value = function(*args, **kwargs)
        except BaseException as error:  # noqa: BLE001 - worker mirrors a Deferred
            worker.errback(error)
        else:
            worker.callback(value)


@pytest.fixture(autouse=True)
def _reset_diagnostic_latches() -> Any:
    old_backend = dupefilter_module._backend_error_warned
    old_forget = dupefilter_module._forget_backend_error_warned
    old_full = dupefilter_module._filter_full_warned
    yield
    dupefilter_module._backend_error_warned = old_backend
    dupefilter_module._forget_backend_error_warned = old_forget
    dupefilter_module._filter_full_warned = old_full


def _filter_dupefilter(
    membership: MembershipFilter,
    *,
    monitor: Monitor | None = None,
) -> BackendDupeFilter:
    return BackendDupeFilter(
        connection_manager=None,
        membership_filter=membership,
        monitor=monitor,
        owns_connection_manager=False,
    )


def test_legacy_handoff_is_thread_affine_and_stale_receipts_are_not_reused() -> None:
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)
    request = Request("https://example.test/thread-owned-receipt")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True

    other_thread_result: list[bool] = []

    def settle_from_peer() -> None:
        other_thread_result.append(dupefilter.settle_reservation(request))

    peer = threading.Thread(target=settle_from_peer)
    peer.start()
    peer.join(timeout=2.0)
    assert not peer.is_alive()
    assert other_thread_result == [False]
    assert dupefilter.settle_reservation(request) is True
    # settle is pure bookkeeping for the successful handoff (no filter I/O): the
    # published marker stays so the durably enqueued URL remains deduplicated.
    assert membership.remove_calls == 0
    assert dupefilter.request_seen(request.replace()) is True

    # A second settlement cannot consume the already retired handoff.
    assert dupefilter.settle_reservation(request) is False


def test_weakref_fallback_keeps_a_legacy_receipt_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)
    # The fallback is only for the callable receipt; use a normal set for this
    # focused hostile-ref seam so the weak membership index does not mask it.
    dupefilter._pending_reservations = set()  # type: ignore[assignment]

    def reject_weakref(*_args: object, **_kwargs: object) -> object:
        raise TypeError("weak references disabled")

    monkeypatch.setattr(dupefilter_module, "ref", reject_weakref)
    request = Request("https://example.test/strong-receipt")
    with dupefilter._lifecycle_condition:
        receipt = dupefilter._new_legacy_reservation_locked(
            request,
            b"strong-fingerprint",
            "strong-fingerprint",
            allowance_slot_reserved=True,
        )
        assert receipt.request_ref() is request
        dupefilter._remove_legacy_reservation_locked(receipt)
    assert dupefilter._legacy_reservations == {}
    assert dupefilter._retry_allowance_slots_reserved == 0


def test_weakref_collection_releases_reserved_retry_slot_and_tables() -> None:
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)

    def issue() -> tuple[int, Any]:
        request = Request("https://example.test/collected-receipt")
        marker = dupefilter_module.ref(request)
        with dupefilter._lifecycle_condition:
            dupefilter._new_legacy_reservation_locked(
                request,
                b"collected",
                "collected",
                allowance_slot_reserved=True,
            )
        return id(request), marker

    request_id, marker = issue()
    gc.collect()
    assert marker() is None
    assert request_id not in dupefilter._legacy_reservations
    assert request_id not in dupefilter._legacy_handoffs
    assert dupefilter._retry_allowance_slots_reserved == 0


def test_filter_reentry_is_observational_only_for_the_originating_request() -> None:
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)
    request = Request("https://example.test/filter-reentry")
    observations: list[tuple[bool, bool]] = []

    def reenter() -> None:
        decision = dupefilter.request_seen_with_reservation(request)
        observations.append((decision.seen, decision.observational))

    membership.on_add = reenter
    decision = dupefilter.request_seen_with_reservation(request)
    assert decision.reservation is not None
    # The marker publication (not the read-only decision) owns the ``add``
    # callback boundary, so the exact-request re-entry fires at commit time.
    dupefilter.commit_reservation(decision.reservation)
    assert observations == [(True, True)]
    assert membership.add_calls == 1
    assert dupefilter._active_filter_requests == {}


def test_monitor_reentry_does_not_create_a_receipt_or_raw_error_context() -> None:
    membership = _StateFilter()
    monitor = _RecordingMonitor()
    dupefilter = _filter_dupefilter(membership, monitor=monitor)
    request = Request("https://example.test/monitor-reentry")
    monitor.dupefilter = dupefilter
    monitor.request = request

    decision = dupefilter.request_seen_with_reservation(request)
    assert decision.reservation is not None
    dupefilter.commit_reservation(decision.reservation)
    assert monitor.events and monitor.events[0][0] == "miss"
    assert dupefilter._monitor_drain_token is None
    assert dupefilter._active_monitor_requests == {}


def test_stale_monitor_drain_cannot_consume_a_new_event_batch() -> None:
    dupefilter = _filter_dupefilter(_StateFilter())
    request = Request("https://example.test/stale-drainer")
    stale = _MonitorFenceToken(threading.get_ident(), "not-a-live-local")
    dupefilter._monitor_drain_token = stale
    dupefilter._monitor_events.append(("on_dedup_miss", ("fp",), request))

    dupefilter._drain_monitor_events(_MonitorFenceToken(threading.get_ident(), "other"))
    assert len(dupefilter._monitor_events) == 1
    assert dupefilter._monitor_drain_token is stale


def test_async_open_clear_and_close_use_real_lifecycle_adapters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scrapy_extension.utils.reactor as reactor_module

    harness = _AsyncHarness()
    monkeypatch.setattr(reactor_module, "deferToThread", harness.submit)
    monkeypatch.setattr(reactor_module, "_reactor", lambda: harness)
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)

    opened = dupefilter.open_async(timeout=2.0)
    assert isinstance(opened, Deferred)
    assert not opened.called
    harness.run_next()
    assert opened.called
    assert dupefilter._lifecycle_state == "open"

    cleared = dupefilter.clear_async(timeout=2.0)
    harness.run_next()
    assert cleared.called
    assert dupefilter._lifecycle_state == "open"

    closed = dupefilter.release_async(
        dupefilter._direct_release_owner,
        "async-close",
        timeout=2.0,
    )
    harness.run_next()
    assert closed.called
    assert dupefilter._lifecycle_state == "closed"
    assert len(harness.jobs) == 0
    assert all(timer.cancel.called for timer in harness.timers)


def test_retry_allowance_consumption_race_has_one_linearized_miss() -> None:
    membership = BloomMembershipFilter(capacity=128, error_rate=0.01)
    dupefilter = _filter_dupefilter(membership)
    dupefilter._retry_allowance_limit = 1
    request = Request("https://example.test/allowance-race")
    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    dupefilter.forget(request)
    assert len(dupefilter._retry_allowances) == 1

    barrier = threading.Barrier(2)
    results: list[bool] = []
    result_lock = threading.Lock()

    def consume() -> None:
        barrier.wait(timeout=2.0)
        value = dupefilter.request_seen(request.replace())
        with result_lock:
            results.append(value)

    workers = [threading.Thread(target=consume) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=2.0)
    assert all(not worker.is_alive() for worker in workers)
    assert sorted(results) == [False, True]
    assert dupefilter._retry_allowances == {}


def test_backpressure_probes_membership_without_creating_a_ghost() -> None:
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)
    dupefilter._retry_allowance_limit = 0
    request = Request("https://example.test/backpressure")

    assert dupefilter.request_seen(request) is False
    assert membership.add_calls == 0
    assert dupefilter.request_seen(request.replace()) is False
    assert membership.contains_calls == 2
    assert dupefilter._legacy_reservations == {}


def test_backpressure_preserves_backend_error_degradation() -> None:
    membership = _StateFilter()
    membership.contains_error = CircuitBreakerOpenError("real-secret")
    dupefilter = _filter_dupefilter(membership)
    dupefilter._retry_allowance_limit = 0

    assert (
        dupefilter.request_seen(Request("https://example.test/backpressure-error"))
        is False
    )
    assert membership.add_calls == 0
    assert dupefilter._legacy_reservations == {}


def test_process_control_during_add_releases_reserved_slot() -> None:
    membership = _StateFilter()
    membership.add_error = KeyboardInterrupt("add interrupted")
    dupefilter = _filter_dupefilter(membership)

    with pytest.raises(KeyboardInterrupt):
        dupefilter.request_seen(Request("https://example.test/add-interrupt"))
    assert dupefilter._retry_allowance_slots_reserved == 0
    assert dupefilter._legacy_reservations == {}


def test_commit_filter_full_discards_receipt_without_leaking_context() -> None:
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)
    membership.add_error = FilterFull("plugin detail")
    decision = dupefilter.request_seen_with_reservation(
        Request("https://example.test/commit-full")
    )
    # contains() is used for the transactional decision; only commit raises.
    assert decision.reservation is not None
    dupefilter.commit_reservation(decision.reservation)
    assert dupefilter._active_reservations == {}
    assert dupefilter._reservations_by_owner == {}


def test_late_legacy_settlement_after_clear_is_a_noop() -> None:
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)
    request = Request("https://example.test/late-settlement")
    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    dupefilter.clear()
    assert dupefilter.settle_reservation(request) is False
    assert membership.remove_calls == 0


def test_forget_control_interruption_grants_one_retry_path() -> None:
    membership = _StateFilter()
    membership.remove_error = KeyboardInterrupt("remove interrupted")
    dupefilter = _filter_dupefilter(membership)
    request = Request("https://example.test/forget-interrupt")
    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True

    with pytest.raises(KeyboardInterrupt):
        dupefilter.forget(request)
    assert len(dupefilter._retry_allowances) == 1
    assert dupefilter.request_seen(request.replace()) is False


def test_clear_retire_marks_legacy_receipt_and_clears_volatile_state() -> None:
    membership = _StateFilter()
    dupefilter = _filter_dupefilter(membership)
    request = Request("https://example.test/clear-receipt")
    assert dupefilter.request_seen(request) is False
    # ``replace()`` keeps the URL (and therefore the fingerprint), so a same-URL
    # transactional probe is a cross-protocol seen-hit that issues no
    # reservation; use a distinct URL to take a genuine transactional miss.
    decision = dupefilter.request_seen_with_reservation(
        Request("https://example.test/clear-receipt-volatile")
    )
    assert decision.reservation is not None
    dupefilter.commit_volatile_reservation(decision.reservation)
    dupefilter.clear()

    assert request in dupefilter._legacy_retired_requests
    assert dupefilter._legacy_reservations == {}
    assert dupefilter._volatile_fingerprints == {}


def test_filter_backend_failure_is_static_and_does_not_expose_driver_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    membership = _StateFilter()
    membership.add_error = BackendConnectionError("password=not-public")
    monitor = _RecordingMonitor()
    dupefilter = _filter_dupefilter(membership, monitor=monitor)
    # The warn-once latch is process-global; reset it so this focused case
    # deterministically observes the degradation warning.
    dupefilter_module._backend_error_warned = False

    with caplog.at_level(
        logging.WARNING, logger="scrapy_extension.dupefilter.dupefilter"
    ):
        assert (
            dupefilter.request_seen(Request("https://example.test/static-error"))
            is False
        )
    # Transient-outage degradation deliberately admits without writing a
    # receipt: nothing was recorded, so no settle/forget compensation is owed.
    assert dupefilter._legacy_reservations == {}
    assert list(dupefilter._monitor_events) == []
    errors = [event for name, event in monitor.events if name == "error"]
    assert len(errors) == 1
    operation, reported = errors[0]
    assert operation == "dedup"
    assert isinstance(reported, BackendConnectionError)
    assert str(reported) == "Dedup backend is unavailable."
    assert "password" not in repr(reported)
    warnings = [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING
        and "transiently unavailable" in record.getMessage()
    ]
    assert len(warnings) == 1
    assert "password=not-public" not in warnings[0].getMessage()


def test_filter_full_and_backend_errors_are_not_raised_by_monitor_logging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    membership = _StateFilter()
    membership.add_error = FilterFull("full")
    dupefilter = _filter_dupefilter(membership)
    monkeypatch.setattr(
        dupefilter_module.logger, "warning", Mock(side_effect=KeyboardInterrupt())
    )

    assert (
        dupefilter.request_seen(Request("https://example.test/full-warning")) is False
    )
    assert dupefilter._retry_allowance_slots_reserved == 0


class _InvalidContainmentFilter(_StateFilter):
    def __contains__(self, item: bytes) -> bool:
        del item
        raise NotImplementedError


def test_transactional_unsupported_filter_reports_a_typed_runtime_error() -> None:
    dupefilter = _filter_dupefilter(_InvalidContainmentFilter())
    with pytest.raises(RuntimeError, match="does not support set/duplicate filtering"):
        dupefilter.request_seen_with_reservation(
            Request("https://example.test/unsupported-contains")
        )
    assert dupefilter._active_reservations == {}
    assert dupefilter._reservations_by_owner == {}
