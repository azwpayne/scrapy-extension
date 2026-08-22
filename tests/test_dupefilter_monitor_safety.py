"""Telemetry failures must not change duplicate-filter control flow."""

from __future__ import annotations

import gc
import logging
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from weakref import ref

import pytest
from pytest_mock import MockerFixture
from scrapy.http import Request

import scrapy_extension.dupefilter.dupefilter as dupefilter_module
from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.dupefilter.dupefilter import (
    BackendDupeFilter,
    _MonitorFenceToken,
)
from scrapy_extension.dupefilter.filters.base import FilterFull, MembershipFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.monitor.base import Monitor


class _ExceptionContextProbeHandler(logging.Handler):
    """Capture the interpreter state exposed to a custom logging handler."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []
        self.active_errors: list[BaseException | None] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.active_errors.append(sys.exc_info()[1])


@contextmanager
def _capture_diagnostics(
    logger_name: str,
    *,
    level: int,
) -> Iterator[_ExceptionContextProbeHandler]:
    """Attach a handler while preserving the logger's previous level."""
    source_logger = logging.getLogger(logger_name)
    previous_level = source_logger.level
    handler = _ExceptionContextProbeHandler()
    source_logger.addHandler(handler)
    source_logger.setLevel(level)
    try:
        yield handler
    finally:
        source_logger.removeHandler(handler)
        source_logger.setLevel(previous_level)


def _assert_handler_records_are_redacted(
    handler: _ExceptionContextProbeHandler,
    marker: str,
) -> None:
    """Assert fixed continuation diagnostics did not expose a caught failure."""
    assert handler.records
    assert handler.active_errors == [None] * len(handler.records)
    assert all(marker not in record.getMessage() for record in handler.records)
    assert all(not record.args for record in handler.records)
    assert all(record.exc_info is None for record in handler.records)
    assert all(record.exc_text is None for record in handler.records)


class _SelectiveRaisingMonitor(Monitor):
    """Monitor double that fails only the selected telemetry hooks."""

    def __init__(self, *hooks: str) -> None:
        self._hooks = set(hooks)

    def _raise_for(self, hook: str) -> None:
        if hook in self._hooks:
            raise RuntimeError(f"{hook} telemetry unavailable")

    def fail(self, hook: str) -> None:
        """Make a later invocation of ``hook`` fail."""
        self._hooks.add(hook)

    def on_dedup_hit(self, key: str) -> None:
        del key
        self._raise_for("on_dedup_hit")

    def on_dedup_miss(self, key: str) -> None:
        del key
        self._raise_for("on_dedup_miss")

    def on_filter_full(self) -> None:
        self._raise_for("on_filter_full")

    def on_filter_saturation(self, used: int, capacity: int | None) -> None:
        del used, capacity
        self._raise_for("on_filter_saturation")

    def on_error(self, operation: str, error: BaseException) -> None:
        del operation, error
        self._raise_for("on_error")


class _RecordingErrorMonitor(Monitor):
    """Capture fail-open monitor errors and their active exception state."""

    def __init__(self) -> None:
        self.events: list[tuple[str, BaseException]] = []
        self.active_errors: list[BaseException | None] = []

    def on_error(self, operation: str, error: BaseException) -> None:
        self.events.append((operation, error))
        self.active_errors.append(sys.exc_info()[1])


class _LifecycleLockProbeMonitor(Monitor):
    """Probe whether a callback runs after the dupefilter releases its lock."""

    def __init__(self, hook: str) -> None:
        self._hook = hook
        self._lock: Any = None
        self.completed_during_hook: list[bool] = []
        self._threads: list[threading.Thread] = []

    def bind(self, lock: Any) -> None:
        self._lock = lock

    def _probe(self, hook: str) -> None:
        if hook != self._hook:
            return
        completed = threading.Event()
        started = threading.Event()

        def acquire_lifecycle_lock() -> None:
            started.set()
            with self._lock:
                completed.set()

        thread = threading.Thread(target=acquire_lifecycle_lock, daemon=True)
        self._threads.append(thread)
        thread.start()
        worker_started = started.wait(2.0)
        self.completed_during_hook.append(worker_started and completed.wait(2.0))

    def on_dedup_miss(self, key: str) -> None:
        del key
        self._probe("on_dedup_miss")

    def on_filter_saturation(self, used: int, capacity: int | None) -> None:
        del used, capacity
        self._probe("on_filter_saturation")

    def join(self) -> None:
        for thread in self._threads:
            thread.join(timeout=1)


class _AttributeRaisingMonitor(Monitor):
    """Fail while resolving a hook, before its method body can run."""

    def __getattribute__(self, name: str) -> Any:
        if name == "on_dedup_miss":
            raise RuntimeError("monitor hook descriptor unavailable")
        return super().__getattribute__(name)


class _MonitorStop(BaseException):
    """Process-control sentinel that telemetry isolation must not swallow."""


class _ProcessControlMonitor(Monitor):
    def on_dedup_miss(self, key: str) -> None:
        del key
        raise _MonitorStop

    def on_filter_saturation(self, used: int, capacity: int | None) -> None:
        del used, capacity
        raise _MonitorStop


class _OneShotProcessControlMonitor(Monitor):
    """Raise one exact process-control object, then become inert."""

    def __init__(self, signal: BaseException) -> None:
        self._signal = signal
        self._raised = False

    def on_dedup_miss(self, key: str) -> None:
        del key
        if not self._raised:
            self._raised = True
            raise self._signal


class _RecordingMonitor(Monitor):
    """Capture the observable order and cadence of dedup telemetry."""

    def __init__(self) -> None:
        self.events: list[str] = []

    def on_dedup_hit(self, key: str) -> None:
        del key
        self.events.append("hit")

    def on_dedup_miss(self, key: str) -> None:
        del key
        self.events.append("miss")

    def on_filter_saturation(self, used: int, capacity: int | None) -> None:
        del used, capacity
        self.events.append("saturation")

    def on_error(self, operation: str, error: BaseException) -> None:
        del operation, error
        self.events.append("error")


class _FifoProbeMonitor(Monitor):
    """Block the first callback and detect overlap or completion reordering."""

    def __init__(self) -> None:
        self.first_entered = threading.Event()
        self.release_first = threading.Event()
        self._lock = threading.Lock()
        self.call_order: list[str] = []
        self.completion_order: list[str] = []
        self.active = 0
        self.max_active = 0

    def on_dedup_miss(self, key: str) -> None:
        with self._lock:
            first = not self.call_order
            self.call_order.append(key)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        if first:
            self.first_entered.set()
            self.release_first.wait(5.0)
        with self._lock:
            self.completion_order.append(key)
            self.active -= 1


class _DelayedSameRequestMonitor(Monitor):
    """Hold one drainer while a later miss is decided and rolled back."""

    def __init__(self) -> None:
        self.first_entered = threading.Event()
        self.release_first = threading.Event()
        self._first = True
        self._dupefilter: BackendDupeFilter | None = None
        self._request: Request | None = None
        self._target_key: str | None = None
        self.nested_results: list[bool] = []

    def bind(self, dupefilter: BackendDupeFilter, request: Request) -> None:
        self._dupefilter = dupefilter
        self._request = request
        self._target_key = dupefilter.request_fingerprint(request)

    def on_dedup_miss(self, key: str) -> None:
        if self._first:
            self._first = False
            self.first_entered.set()
            self.release_first.wait(5.0)
            return
        if key != self._target_key:
            return
        if self._dupefilter is None or self._request is None:
            raise RuntimeError("delayed monitor is not bound")
        self.nested_results.append(self._dupefilter.request_seen(self._request))


class _ThreadedSameRequestMonitor(Monitor):
    """Delegate one observer re-entry to a raw thread using the same Request."""

    def __init__(self) -> None:
        self._dupefilter: BackendDupeFilter | None = None
        self._request: Request | None = None
        self._used = False
        self.nested_results: list[bool] = []

    def bind(self, dupefilter: BackendDupeFilter, request: Request) -> None:
        self._dupefilter = dupefilter
        self._request = request

    def on_dedup_miss(self, key: str) -> None:
        del key
        if self._used:
            return
        self._used = True
        if self._dupefilter is None or self._request is None:
            raise RuntimeError("threaded monitor is not bound")
        dupefilter = self._dupefilter
        request = self._request
        thread = threading.Thread(
            target=lambda: self.nested_results.append(dupefilter.request_seen(request)),
            daemon=True,
        )
        thread.start()
        thread.join(timeout=2.0)
        if thread.is_alive():
            raise RuntimeError("threaded monitor re-entry deadlocked")


class _SameThreadSameRequestMonitor(Monitor):
    """Re-enter directly to verify the invocation-local frame fence."""

    def __init__(self) -> None:
        self.dupefilter: BackendDupeFilter | None = None
        self.request: Request | None = None
        self.observations: list[tuple[bool, bool]] = []

    def on_dedup_miss(self, key: str) -> None:
        del key
        if self.dupefilter is None or self.request is None:
            raise RuntimeError("same-thread monitor is not bound")
        decision = self.dupefilter.request_seen_with_reservation(self.request)
        self.observations.append((decision.seen, decision.observational))
        assert decision.reservation is None


class _NoSaturationCapacityPoisonFilter(MembershipFilter):
    """Expose no saturation and fail if an irrelevant capacity is inspected."""

    def __init__(self) -> None:
        self._items: set[bytes] = set()

    @property
    def saturation(self) -> None:
        return None

    @property
    def capacity(self) -> int:
        raise RuntimeError("capacity must remain lazy without saturation")

    def add(self, item: bytes) -> bool:
        before = len(self._items)
        self._items.add(item)
        return len(self._items) != before

    def __contains__(self, item: bytes) -> bool:
        return item in self._items

    def __len__(self) -> int:
        return len(self._items)

    def clear(self) -> None:
        self._items.clear()


class _FailingClearMemoryFilter(MemoryMembershipFilter):
    def clear(self) -> None:
        raise BackendConnectionError("clear unavailable", backend_type="redis")


def test_miss_monitor_failure_preserves_new_reservation(
    mock_connection_manager: Any,
) -> None:
    membership = MemoryMembershipFilter(maxsize=10)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=_SelectiveRaisingMonitor("on_dedup_miss"),
    )
    request = Request("https://example.test/new")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    assert dupefilter.request_seen(request) is True


def test_hit_monitor_failure_preserves_duplicate_result(
    mock_connection_manager: Any,
) -> None:
    membership = MemoryMembershipFilter(maxsize=10)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=_SelectiveRaisingMonitor("on_dedup_hit"),
    )
    request = Request("https://example.test/duplicate")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.request_seen(request) is True


def test_retry_allowance_monitor_failure_preserves_one_shot_miss(
    mock_connection_manager: Any,
) -> None:
    membership = BloomMembershipFilter(capacity=1_000, error_rate=0.01)
    monitor = _SelectiveRaisingMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )
    request = Request("https://example.test/retry")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    dupefilter.forget(request)
    monitor.fail("on_dedup_miss")
    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    assert dupefilter.request_seen(request) is True


@pytest.mark.parametrize("hook", ["on_filter_full", "on_dedup_miss"])
def test_filter_full_monitor_failure_preserves_degraded_miss(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    hook: str,
) -> None:
    membership = mocker.MagicMock(spec=MembershipFilter)
    membership.add.side_effect = FilterFull("capacity exhausted")
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=_SelectiveRaisingMonitor(hook),
    )
    request = Request(f"https://example.test/full/{hook}")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is False


@pytest.mark.parametrize("hook", ["on_error", "on_dedup_miss"])
def test_backend_error_monitor_failure_preserves_degraded_miss(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    hook: str,
) -> None:
    membership = mocker.MagicMock(spec=MembershipFilter)
    membership.add.side_effect = BackendConnectionError(
        "backend unavailable", backend_type="redis"
    )
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=_SelectiveRaisingMonitor(hook),
    )
    request = Request(f"https://example.test/backend/{hook}")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is False


@pytest.mark.parametrize(
    ("source_kind", "expected_type", "expected_message"),
    [
        ("connection", BackendConnectionError, "Dedup backend is unavailable."),
        ("circuit", CircuitBreakerOpenError, "Circuit breaker open for 'dedup'"),
    ],
)
def test_fail_open_monitor_error_is_fresh_static_and_redacted(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    source_kind: str,
    expected_type: type[BaseException],
    expected_message: str,
) -> None:
    """Fail-open telemetry exposes neither the source graph nor an active error."""
    marker = f"round45-dupefilter-{source_kind}-private-marker"
    raw_failures: list[BaseException] = []
    membership = mocker.MagicMock(spec=MembershipFilter)

    def fail_add(_fingerprint: bytes) -> None:
        private_frame_state = {"marker": marker}
        try:
            raise RuntimeError(marker)
        except RuntimeError as cause:
            if source_kind == "connection":
                failure: BaseException = BackendConnectionError(
                    marker, backend_type=marker
                )
            else:
                failure = CircuitBreakerOpenError(marker)
            failure.private_state = private_frame_state
            raw_failures.append(failure)
            raise failure from cause

    membership.add.side_effect = fail_add
    monitor = _RecordingErrorMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )
    request = Request(f"https://example.test/{marker}")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is False
    assert len(monitor.events) == 1
    operation, reported_error = monitor.events[0]

    assert operation == "dedup"
    assert type(reported_error) is expected_type
    assert reported_error is not raw_failures[0]
    assert str(reported_error) == expected_message
    assert reported_error.__cause__ is None
    assert reported_error.__context__ is None
    assert reported_error.__traceback__ is None
    assert marker not in repr(reported_error.args)
    assert marker not in repr(reported_error.__dict__)
    assert monitor.active_errors == [None]
    if type(reported_error) is BackendConnectionError:
        assert reported_error.backend_type is None
    else:
        assert isinstance(reported_error, CircuitBreakerOpenError)
        assert reported_error.name == "dedup"


def test_saturation_monitor_failure_preserves_new_reservation(
    mock_connection_manager: Any,
) -> None:
    membership = CuckooMembershipFilter(capacity=100, error_rate=0.01)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=_SelectiveRaisingMonitor("on_filter_saturation"),
    )
    request = Request("https://example.test/saturation")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    assert dupefilter.request_seen(request) is True


def test_memory_saturation_monitor_failure_does_not_reject_insert(
    mock_connection_manager: Any,
) -> None:
    membership = MemoryMembershipFilter(maxsize=1)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=_SelectiveRaisingMonitor("on_filter_saturation"),
    )
    request = Request("https://example.test/memory-saturation")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    assert len(membership) == 1

    replacement = Request("https://example.test/memory-eviction")
    assert dupefilter.request_seen(replacement) is False
    assert dupefilter.consume_reservation(replacement) is True
    assert len(membership) == 1


def test_standalone_memory_monitor_failure_does_not_reject_insert() -> None:
    membership = MemoryMembershipFilter(maxsize=1)
    membership.set_monitor(_SelectiveRaisingMonitor("on_filter_saturation"))

    assert membership.add(b"new") is True
    assert b"new" in membership


def test_standalone_memory_monitor_fallback_handler_has_no_active_exception(
    mocker: MockerFixture,
) -> None:
    """A saturation fallback logs after the monitor exception has unwound."""
    marker = "round47-memory-monitor-private-marker"
    monitor = mocker.Mock()
    monitor.on_filter_saturation.side_effect = RuntimeError(marker)
    membership = MemoryMembershipFilter(maxsize=1)
    membership.set_monitor(monitor)

    with _capture_diagnostics(
        "scrapy_extension.dupefilter.filters.memory_filter",
        level=logging.DEBUG,
    ) as handler:
        assert membership.add(b"new") is True

    _assert_handler_records_are_redacted(handler, marker)


@pytest.mark.parametrize(
    "diagnostic_error",
    [
        RuntimeError("logging unavailable"),
        KeyboardInterrupt(),
        SystemExit(),
    ],
)
def test_monitor_diagnostic_failure_does_not_reject_reservation(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    diagnostic_error: BaseException,
) -> None:
    """A logger failure after a normal monitor failure remains advisory."""
    mocker.patch(
        "scrapy_extension.dupefilter.dupefilter.logger.debug",
        side_effect=diagnostic_error,
    )
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
        monitor=_SelectiveRaisingMonitor("on_dedup_miss"),
    )
    request = Request("https://example.test/diagnostic-failure")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True


def test_monitor_fallback_handler_has_no_active_exception(
    mock_connection_manager: Any,
    mocker: MockerFixture,
) -> None:
    """Deferred monitor delivery exits its exception handler before logging."""
    marker = "round47-dupefilter-monitor-private-marker"
    monitor = mocker.Mock()
    monitor.on_dedup_miss.side_effect = RuntimeError(marker)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
        monitor=monitor,
    )

    with _capture_diagnostics(
        "scrapy_extension.dupefilter.dupefilter",
        level=logging.DEBUG,
    ) as handler:
        assert dupefilter.request_seen(Request("https://example.test/monitor")) is False

    _assert_handler_records_are_redacted(handler, marker)


def test_monitor_fence_cleanup_diagnostic_releases_raw_error_context(
    mock_connection_manager: Any,
) -> None:
    """A preserved primary signal cannot expose a failed fence cleanup."""
    cleanup_marker = "round50-dupefilter-fence-cleanup-marker"
    primary = _MonitorStop("round50-dupefilter-fence-primary-marker")

    class _CleanupFailingRegistry(dict[int, tuple[object, set[Any]]]):
        def __init__(self) -> None:
            super().__init__()
            self._get_calls = 0

        def get(self, key: int, default: Any = None) -> Any:
            self._get_calls += 1
            if self._get_calls == 2:
                raise RuntimeError(cleanup_marker)
            return super().get(key, default)

    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
    )
    request = Request("https://example.test/fence-cleanup-context")
    registry = _CleanupFailingRegistry()
    dupefilter._active_monitor_requests = registry  # type: ignore[assignment]
    dupefilter._monitor = _OneShotProcessControlMonitor(primary)

    with _capture_diagnostics(
        "scrapy_extension.dupefilter.dupefilter",
        level=logging.DEBUG,
    ) as handler:
        with pytest.raises(_MonitorStop) as raised:
            dupefilter._emit_monitor(("on_dedup_miss", ("fingerprint",), request))

    assert raised.value is primary
    assert id(request) not in registry
    _assert_handler_records_are_redacted(handler, cleanup_marker)
    assert all(str(primary) not in record.getMessage() for record in handler.records)
    assert all(str(primary) not in repr(record.args) for record in handler.records)


def test_interrupted_decision_compensation_is_silent_with_active_primary(
    mock_connection_manager: Any,
) -> None:
    """Best-effort compensation must not leak an active caller exception."""
    cleanup_marker = "round50-dupefilter-compensation-cleanup-marker"
    primary = _MonitorStop("round50-dupefilter-compensation-primary-marker")

    class _FailingReservationRegistry(dict[int, object]):
        def get(self, key: int, default: Any = None) -> Any:
            del key, default
            raise RuntimeError(cleanup_marker)

    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
    )
    decision = dupefilter.request_seen_with_reservation(
        Request("https://example.test/interrupted-compensation")
    )
    assert decision.reservation is not None
    dupefilter._active_reservations = _FailingReservationRegistry()  # type: ignore[assignment]

    with _capture_diagnostics(
        "scrapy_extension.dupefilter.dupefilter",
        level=logging.DEBUG,
    ) as handler:
        with pytest.raises(_MonitorStop) as raised:
            try:
                raise primary
            except _MonitorStop:
                dupefilter._compensate_interrupted_decision(
                    decision.reservation,
                    decision.reservation.owner,
                )
                raise

    assert raised.value is primary
    assert handler.records == []
    assert handler.active_errors == []


@pytest.mark.parametrize(
    "diagnostic_error",
    [
        RuntimeError("logging unavailable"),
        KeyboardInterrupt(),
        SystemExit(),
    ],
)
@pytest.mark.parametrize("degraded_path", ["filter_full", "backend_error"])
def test_graceful_miss_warning_failure_preserves_degraded_decision(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    diagnostic_error: BaseException,
    degraded_path: str,
) -> None:
    """Warn-once reporting cannot replace either intentional graceful miss."""
    from scrapy_extension.dupefilter import dupefilter as dupefilter_module

    membership = mocker.MagicMock(spec=MembershipFilter)
    if degraded_path == "filter_full":
        mocker.patch.object(dupefilter_module, "_filter_full_warned", False)
        membership.add.side_effect = FilterFull("capacity exhausted")
    else:
        mocker.patch.object(dupefilter_module, "_backend_error_warned", False)
        membership.add.side_effect = BackendConnectionError(
            "backend unavailable", backend_type="redis"
        )
    mocker.patch(
        "scrapy_extension.dupefilter.dupefilter.logger.warning",
        side_effect=diagnostic_error,
    )
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
    )
    request = Request(f"https://example.test/graceful-miss/{degraded_path}")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is False


@pytest.mark.parametrize("degraded_path", ["filter_full", "backend_error"])
def test_graceful_miss_warning_handler_has_no_active_exception(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    degraded_path: str,
) -> None:
    """Warn-once degradation logs only after the filter failure is released."""
    from scrapy_extension.dupefilter import dupefilter as dupefilter_module

    marker = f"round47-dupefilter-{degraded_path}-private-marker"
    membership = mocker.MagicMock(spec=MembershipFilter)
    if degraded_path == "filter_full":
        mocker.patch.object(dupefilter_module, "_filter_full_warned", False)
        membership.add.side_effect = FilterFull(marker)
    else:
        mocker.patch.object(dupefilter_module, "_backend_error_warned", False)
        membership.add.side_effect = BackendConnectionError(marker, backend_type=marker)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
    )

    with _capture_diagnostics(
        "scrapy_extension.dupefilter.dupefilter",
        level=logging.WARNING,
    ) as handler:
        assert (
            dupefilter.request_seen(Request("https://example.test/degraded")) is False
        )

    _assert_handler_records_are_redacted(handler, marker)


def test_retry_allowance_warning_handler_has_no_active_exception(
    mock_connection_manager: Any,
    mocker: MockerFixture,
) -> None:
    """A helper invoked by ``forget`` logs after its caught fallback unwinds."""
    marker = "round47-dupefilter-allowance-private-marker"
    membership = mocker.MagicMock(spec=MembershipFilter)
    membership.add.return_value = True
    membership.saturation = None
    membership.remove.side_effect = NotImplementedError(marker)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
    )
    dupefilter._retry_allowance_limit = 1
    first = Request("https://example.test/allowance/first")
    second = Request("https://example.test/allowance/second")

    assert dupefilter.request_seen(first) is False
    dupefilter.forget(first)
    with _capture_diagnostics(
        "scrapy_extension.dupefilter.dupefilter",
        level=logging.WARNING,
    ) as handler:
        # The full recovery ledger backpressures this new marker; it does not
        # admit work whose failed push could not be represented exactly.
        assert dupefilter.request_seen(second) is False

    _assert_handler_records_are_redacted(handler, marker)


@pytest.mark.parametrize(
    "diagnostic_error",
    [
        RuntimeError("logging unavailable"),
        KeyboardInterrupt(),
        SystemExit(),
    ],
)
def test_volatile_marker_overflow_warning_preserves_published_marker(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    diagnostic_error: BaseException,
) -> None:
    """A post-publication volatile-shadow warning cannot undo the new marker."""
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    dupefilter._volatile_fingerprint_limit = 1
    first_request = Request("https://example.test/volatile-warning/first")
    second_request = Request("https://example.test/volatile-warning/second")
    first = dupefilter.request_seen_with_reservation(first_request)
    assert first.reservation is not None
    dupefilter.commit_volatile_reservation(first.reservation)
    mocker.patch(
        "scrapy_extension.dupefilter.dupefilter.logger.warning",
        side_effect=diagnostic_error,
    )
    second = dupefilter.request_seen_with_reservation(second_request)
    assert second.reservation is not None

    dupefilter.commit_volatile_reservation(second.reservation)

    assert len(dupefilter._volatile_fingerprints) == 1
    assert dupefilter.request_seen_with_reservation(second_request.replace()).seen


@pytest.mark.parametrize(
    "diagnostic_error",
    [
        RuntimeError("logging unavailable"),
        KeyboardInterrupt(),
        SystemExit(),
    ],
)
def test_retry_allowance_overflow_warning_preserves_existing_allowance(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    diagnostic_error: BaseException,
) -> None:
    """Backpressure preserves the existing failed-work allowance."""
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=BloomMembershipFilter(capacity=1_000, error_rate=0.01),
    )
    dupefilter._retry_allowance_limit = 1
    first_request = Request("https://example.test/retry-warning/first")
    second_request = Request("https://example.test/retry-warning/second")
    assert dupefilter.request_seen(first_request) is False
    dupefilter.forget(first_request)
    mocker.patch(
        "scrapy_extension.dupefilter.dupefilter.logger.warning",
        side_effect=diagnostic_error,
    )
    assert dupefilter.request_seen(second_request) is False

    # The first allowance remains reachable; the second fingerprint was not
    # inserted while the bounded ledger was full and therefore has no ghost.
    assert len(dupefilter._retry_allowances) == 1
    assert dupefilter.request_seen(second_request) is False
    assert dupefilter.request_seen(first_request) is False
    assert dupefilter.request_seen(first_request) is True


@pytest.mark.parametrize(
    "diagnostic_error",
    [
        RuntimeError("logging unavailable"),
        KeyboardInterrupt(),
        SystemExit(),
    ],
)
def test_monitor_backlog_warning_preserves_reservation(
    mock_connection_manager: Any,
    mocker: MockerFixture,
    diagnostic_error: BaseException,
) -> None:
    """Backlog-drop reporting is outside the already-linearized miss result."""
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    dupefilter._monitor_event_limit = 0
    mocker.patch(
        "scrapy_extension.dupefilter.dupefilter.logger.warning",
        side_effect=diagnostic_error,
    )
    request = Request("https://example.test/monitor-backlog-warning")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True
    assert dupefilter._monitor_overflow_warned is True


@pytest.mark.parametrize(
    "diagnostic_error",
    [
        RuntimeError("logging unavailable"),
        KeyboardInterrupt(),
        SystemExit(),
    ],
)
def test_standalone_memory_diagnostic_failure_does_not_reject_insert(
    mocker: MockerFixture,
    diagnostic_error: BaseException,
) -> None:
    mocker.patch(
        "scrapy_extension.dupefilter.filters.memory_filter.logger.debug",
        side_effect=diagnostic_error,
    )
    membership = MemoryMembershipFilter(maxsize=1)
    membership.set_monitor(_SelectiveRaisingMonitor("on_filter_saturation"))

    assert membership.add(b"new") is True
    assert b"new" in membership


def test_memory_saturation_preserves_insert_only_cadence_and_order(
    mock_connection_manager: Any,
) -> None:
    monitor = _RecordingMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=1),
        monitor=monitor,
    )
    request = Request("https://example.test/memory-cadence")

    assert dupefilter.request_seen(request) is False
    assert monitor.events == ["saturation", "miss"]

    monitor.events.clear()
    assert dupefilter.request_seen(request) is True
    assert monitor.events == ["hit"]


def test_atomic_decision_returns_invocation_reservation_and_preserves_order(
    mock_connection_manager: Any,
) -> None:
    monitor = _RecordingMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=1),
        monitor=monitor,
    )
    request = Request("https://example.test/atomic-decision")

    first = dupefilter.request_seen_with_reservation(request)
    assert first.seen is False
    assert first.reservation is not None
    assert first.observational is False
    assert dupefilter.consume_reservation(request) is False
    assert monitor.events == []
    dupefilter.commit_reservation(first.reservation)
    assert monitor.events == ["saturation", "miss"]

    monitor.events.clear()
    second = dupefilter.request_seen_with_reservation(request)
    assert second.seen is True
    assert second.reservation is None
    assert second.observational is False
    assert monitor.events == ["hit"]


def test_atomic_bloom_commit_preserves_miss_and_hit_telemetry_order(
    mock_connection_manager: Any,
) -> None:
    monitor = _RecordingMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=BloomMembershipFilter(
            capacity=1_000,
            error_rate=0.01,
        ),
        monitor=monitor,
    )
    request = Request("https://example.test/atomic-bloom-cadence")

    first = dupefilter.request_seen_with_reservation(request)
    assert first.reservation is not None
    assert monitor.events == []
    dupefilter.commit_reservation(first.reservation)
    assert monitor.events == ["miss", "saturation"]

    monitor.events.clear()
    duplicate = dupefilter.request_seen_with_reservation(request.replace())
    assert duplicate.seen is True
    assert duplicate.reservation is None
    assert monitor.events == ["hit", "saturation"]


def test_atomic_contains_outage_and_push_rollback_count_one_miss(
    mock_connection_manager: Any,
    mocker: MockerFixture,
) -> None:
    membership = mocker.MagicMock(spec=MembershipFilter)
    membership.__contains__.side_effect = BackendConnectionError(
        "contains unavailable",
        backend_type="redis",
    )
    monitor = _RecordingMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )

    decision = dupefilter.request_seen_with_reservation(
        Request("https://example.test/outage-rollback")
    )
    assert decision.reservation is not None
    assert monitor.events == ["error"]
    dupefilter.rollback_reservation(decision.reservation)
    assert monitor.events == ["error", "miss"]


def test_atomic_contains_outage_and_recovered_commit_count_one_miss(
    mock_connection_manager: Any,
    mocker: MockerFixture,
) -> None:
    membership = mocker.MagicMock(spec=MembershipFilter)
    membership.__contains__.side_effect = BackendConnectionError(
        "contains unavailable",
        backend_type="redis",
    )
    membership.add.return_value = True
    membership.saturation = None
    monitor = _RecordingMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )

    decision = dupefilter.request_seen_with_reservation(
        Request("https://example.test/outage-recovered")
    )
    assert decision.reservation is not None
    dupefilter.commit_reservation(decision.reservation)

    assert monitor.events == ["error", "miss"]


def test_atomic_contains_and_commit_outages_count_each_error_but_one_miss(
    mock_connection_manager: Any,
    mocker: MockerFixture,
) -> None:
    membership = mocker.MagicMock(spec=MembershipFilter)
    membership.__contains__.side_effect = BackendConnectionError(
        "contains unavailable",
        backend_type="redis",
    )
    membership.add.side_effect = BackendConnectionError(
        "add unavailable",
        backend_type="redis",
    )
    monitor = _RecordingMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )

    decision = dupefilter.request_seen_with_reservation(
        Request("https://example.test/outage-twice")
    )
    assert decision.reservation is not None
    dupefilter.commit_reservation(decision.reservation)

    assert monitor.events == ["error", "error", "miss"]


def test_atomic_decision_same_object_concurrency_keeps_first_receipt(
    mock_connection_manager: Any,
) -> None:
    monitor = _FifoProbeMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
        monitor=monitor,
    )
    request = Request("https://example.test/atomic-concurrency")
    first = dupefilter.request_seen_with_reservation(request)
    assert first.reservation is not None
    rollback_thread = threading.Thread(
        target=lambda: dupefilter.rollback_reservation(first.reservation),
        daemon=True,
    )
    rollback_thread.start()
    try:
        assert monitor.first_entered.wait(2.0)
        second = dupefilter.request_seen_with_reservation(request)
    finally:
        monitor.release_first.set()
    rollback_thread.join(timeout=2.0)

    assert not rollback_thread.is_alive()
    assert second.seen is True
    assert second.reservation is None
    assert second.observational is True
    retry = dupefilter.request_seen_with_reservation(request.replace())
    assert retry.reservation is not None
    dupefilter.commit_reservation(retry.reservation)


def test_monitor_fence_does_not_suppress_distinct_request_same_fingerprint(
    mock_connection_manager: Any,
) -> None:
    monitor = _FifoProbeMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
        monitor=monitor,
    )
    first = Request("https://example.test/identity-fence")
    first_decision = dupefilter.request_seen_with_reservation(first)
    assert first_decision.reservation is not None
    thread = threading.Thread(
        target=lambda: dupefilter.rollback_reservation(first_decision.reservation),
        daemon=True,
    )
    thread.start()
    try:
        assert monitor.first_entered.wait(2.0)
        independent = dupefilter.request_seen_with_reservation(first.replace())
    finally:
        monitor.release_first.set()
    thread.join(timeout=2.0)

    assert independent.seen is False
    assert independent.observational is False
    assert independent.reservation is not None
    dupefilter.commit_reservation(independent.reservation)


@pytest.mark.parametrize(
    "membership",
    [
        pytest.param(MemoryMembershipFilter(maxsize=None), id="memory"),
        pytest.param(
            BloomMembershipFilter(capacity=1_000, error_rate=0.01),
            id="bloom",
        ),
    ],
)
def test_monitor_raw_thread_same_request_is_observational(
    mock_connection_manager: Any,
    membership: MembershipFilter,
) -> None:
    monitor = _ThreadedSameRequestMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )
    request = Request("https://example.test/threaded-monitor")
    monitor.bind(dupefilter, request)

    decision = dupefilter.request_seen_with_reservation(request)

    assert decision.reservation is not None
    dupefilter.rollback_reservation(decision.reservation)
    assert monitor.nested_results == [True]
    retry = dupefilter.request_seen_with_reservation(request.replace())
    assert retry.seen is False
    assert retry.reservation is not None
    dupefilter.commit_reservation(retry.reservation)


def test_monitor_same_thread_reentry_is_observational_and_cleans_frame_token(
    mock_connection_manager: Any,
) -> None:
    monitor = _SameThreadSameRequestMonitor()
    membership = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )
    request = Request("https://example.test/same-thread-monitor")
    monitor.dupefilter = dupefilter
    monitor.request = request

    decision = dupefilter.request_seen_with_reservation(request)
    assert decision.reservation is not None
    dupefilter.rollback_reservation(decision.reservation)

    assert monitor.observations == [(True, True)]
    assert dupefilter._monitor_drain_token is None
    retry = dupefilter.request_seen_with_reservation(request.replace())
    assert retry.reservation is not None
    dupefilter.commit_reservation(retry.reservation)


@pytest.mark.parametrize(
    "membership",
    [
        pytest.param(MemoryMembershipFilter(maxsize=None), id="memory"),
        pytest.param(
            BloomMembershipFilter(capacity=1_000, error_rate=0.01),
            id="bloom",
        ),
    ],
)
def test_delayed_monitor_reentry_cannot_recreate_rolled_back_state(
    mock_connection_manager: Any,
    membership: MembershipFilter,
) -> None:
    monitor = _DelayedSameRequestMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )
    first_request = Request("https://example.test/delayed/first")
    rolled_back_request = Request("https://example.test/delayed/rollback")
    monitor.bind(dupefilter, rolled_back_request)
    results: dict[str, Any] = {}

    def commit_first() -> None:
        decision = dupefilter.request_seen_with_reservation(first_request)
        results["first"] = decision
        assert decision.reservation is not None
        dupefilter.commit_reservation(decision.reservation)

    first_thread = threading.Thread(target=commit_first, daemon=True)
    first_thread.start()
    try:
        assert monitor.first_entered.wait(2.0)
        rolled_back = dupefilter.request_seen_with_reservation(rolled_back_request)
        assert rolled_back.reservation is not None
        dupefilter.rollback_reservation(rolled_back.reservation)
    finally:
        monitor.release_first.set()
    first_thread.join(timeout=2.0)

    assert not first_thread.is_alive()
    assert monitor.nested_results == [True]
    retry = dupefilter.request_seen_with_reservation(rolled_back_request)
    assert retry.seen is False
    assert retry.reservation is not None
    dupefilter.commit_reservation(retry.reservation)
    assert results["first"].reservation is not None


def test_duplicate_owner_error_does_not_discard_prior_receipt(
    mock_connection_manager: Any,
) -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    owner = object()
    request = Request("https://example.test/duplicate-owner")
    first = dupefilter.request_seen_with_reservation(request, owner)
    assert first.reservation is not None

    with pytest.raises(RuntimeError, match="owner intent is already active"):
        dupefilter.request_seen_with_reservation(request.replace(), owner)

    dupefilter.commit_reservation(first.reservation)
    duplicate = dupefilter.request_seen_with_reservation(request.replace())
    assert duplicate.seen is True


def test_interrupted_owner_intent_cleanup_is_silent_and_retryable(
    mock_connection_manager: Any,
) -> None:
    monitor = _ProcessControlMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
        monitor=monitor,
    )
    owner = object()
    request = Request("https://example.test/interrupted-owner")
    decision = dupefilter.request_seen_with_reservation(request, owner)
    assert decision.reservation is not None

    dupefilter.rollback_reservation_intent(owner)

    retry = dupefilter.request_seen_with_reservation(request.replace())
    assert retry.seen is False
    assert retry.reservation is not None


def test_empty_monitor_fence_entry_does_not_suppress_real_decision(
    mock_connection_manager: Any,
) -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    request = Request("https://example.test/empty-monitor-fence")
    dupefilter._active_monitor_requests[id(request)] = (ref(request), set())

    decision = dupefilter.request_seen_with_reservation(request)

    assert decision.seen is False
    assert decision.observational is False
    assert decision.reservation is not None
    dupefilter.rollback_reservation(decision.reservation)


def test_interrupted_monitor_fence_cleanup_fails_open_for_exact_request(
    mock_connection_manager: Any,
) -> None:
    signal = _MonitorStop()

    class _CleanupInterruptingRegistry(dict[int, tuple[object, set[Any]]]):
        def __init__(self) -> None:
            super().__init__()
            self.get_calls = 0
            self.item_failed = False

        def get(self, key: int, default: Any = None) -> Any:
            self.get_calls += 1
            if self.get_calls == 3:
                raise signal
            return super().get(key, default)

        def __getitem__(self, key: int) -> tuple[object, set[Any]]:
            if self.get_calls == 3 and not self.item_failed:
                self.item_failed = True
                raise _MonitorStop
            return super().__getitem__(key)

    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    registry = _CleanupInterruptingRegistry()
    dupefilter._active_monitor_requests = registry  # type: ignore[assignment]
    request = Request("https://example.test/interrupted-monitor-cleanup")
    decision = dupefilter.request_seen_with_reservation(request)
    assert decision.reservation is not None

    with pytest.raises(_MonitorStop) as raised:
        dupefilter.rollback_reservation(decision.reservation)
    assert raised.value is signal
    retained = registry[id(request)]
    assert retained[0]() is request
    assert retained[1]
    assert not any(token.active for token in retained[1])

    retry = dupefilter.request_seen_with_reservation(request)
    assert retry.seen is False
    assert retry.observational is False
    assert retry.reservation is not None
    assert id(request) not in registry


def test_monitor_fence_uses_invocation_identity_not_reusable_frame_id(
    mock_connection_manager: Any,
) -> None:
    ready = threading.Event()
    release = threading.Event()
    tokens: dict[str, _MonitorFenceToken] = {}

    def hold_new_invocation() -> None:
        event_token = _MonitorFenceToken(threading.get_ident(), "event_token")
        stale_token = _MonitorFenceToken(threading.get_ident(), "event_token")
        tokens.update(live=event_token, stale=stale_token)
        ready.set()
        release.wait(2.0)

    thread = threading.Thread(target=hold_new_invocation, daemon=True)
    thread.start()
    assert ready.wait(1.0)
    request = Request("https://example.test/frame-aba")
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    dupefilter._active_monitor_requests[id(request)] = (
        ref(request),
        {tokens["stale"]},
    )
    try:
        assert tokens["live"].active is True
        assert tokens["stale"].active is False
        decision = dupefilter.request_seen_with_reservation(request)
    finally:
        release.set()
        thread.join(timeout=1.0)

    assert decision.seen is False
    assert decision.observational is False
    assert decision.reservation is not None
    dupefilter.rollback_reservation(decision.reservation)


def test_monitor_fence_liveness_audit_failure_admits_real_decision(
    mock_connection_manager: Any,
    mocker: MockerFixture,
) -> None:
    request = Request("https://example.test/audit-denied")
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    dupefilter._active_monitor_requests[id(request)] = (
        ref(request),
        {_MonitorFenceToken(threading.get_ident(), "event_token")},
    )
    mocker.patch.object(
        sys,
        "_current_frames",
        side_effect=RuntimeError("audit denied"),
    )

    decision = dupefilter.request_seen_with_reservation(request)

    assert decision.seen is False
    assert decision.observational is False
    assert decision.reservation is not None
    dupefilter.rollback_reservation(decision.reservation)


def test_monitor_fence_returns_false_when_frame_locals_are_unavailable(
    mock_connection_manager: Any,
    mocker: MockerFixture,
) -> None:
    class LocalsFailingFrame:
        f_back = None

        @property
        def f_locals(self) -> dict[str, object]:
            raise RuntimeError("frame audit denied")

    mocker.patch.object(
        sys,
        "_current_frames",
        return_value={threading.get_ident(): LocalsFailingFrame()},
    )
    token = _MonitorFenceToken(threading.get_ident(), "event_token")

    assert token.active is False


def test_monitor_fence_weakref_failure_fails_open(
    mocker: MockerFixture,
) -> None:
    def hostile_ref(*_args: object, **_kwargs: object) -> object:
        raise TypeError("weak references unavailable")

    mocker.patch.object(dupefilter_module, "ref", hostile_ref)
    dupefilter = BackendDupeFilter(
        connection_manager=None,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    request = Request("https://example.test/hostile-monitor-weakref")

    decision = dupefilter.request_seen_with_reservation(request)
    assert decision.reservation is not None
    dupefilter.rollback_reservation(decision.reservation)
    assert dupefilter._active_monitor_requests == {}


def test_monitor_fence_returns_false_when_frame_chain_is_hostile(
    mocker: MockerFixture,
) -> None:
    class FrameChainFailingFrame:
        f_locals: dict[str, object] = {}

        @property
        def f_back(self) -> object:
            raise RuntimeError("frame chain audit denied")

    mocker.patch.object(
        sys,
        "_current_frames",
        return_value={threading.get_ident(): FrameChainFailingFrame()},
    )
    token = _MonitorFenceToken(threading.get_ident(), "event_token")

    assert token.active is False


def test_monitor_origin_weakref_callback_removes_stale_entry() -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=None,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )

    def retain_origin() -> tuple[int, Any]:
        request = Request("https://example.test/weak-monitor-origin")
        origin = dupefilter._monitor_origin_ref(request)
        request_id = id(request)
        dupefilter._active_monitor_requests[request_id] = (origin, set())
        return request_id, origin

    request_id, origin = retain_origin()
    gc.collect()
    assert origin() is None
    assert request_id not in dupefilter._active_monitor_requests


def test_monitor_origin_weakref_cleanup_tolerates_owner_collection() -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=None,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    request = Request("https://example.test/weak-monitor-owner")
    origin = dupefilter._monitor_origin_ref(request)
    del dupefilter
    del request
    gc.collect()
    assert origin() is None


def test_reservation_repr_does_not_expose_request_or_owner_secrets(
    mock_connection_manager: Any,
) -> None:
    owner = {"api_key": "owner-secret"}
    request = Request("https://user:password@example.test/path?token=request-secret")
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )

    decision = dupefilter.request_seen_with_reservation(request, owner)

    assert decision.reservation is not None
    rendered = repr(decision)
    for secret in ("owner-secret", "password", "request-secret"):
        assert secret not in rendered
    dupefilter.rollback_reservation(decision.reservation)


def test_volatile_marker_shadow_is_bounded_and_evicts_safely(
    mock_connection_manager: Any,
) -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    dupefilter._volatile_fingerprint_limit = 1
    first_request = Request("https://example.test/volatile-shadow/first")
    second_request = Request("https://example.test/volatile-shadow/second")

    first = dupefilter.request_seen_with_reservation(first_request)
    assert first.reservation is not None
    dupefilter.commit_volatile_reservation(first.reservation)
    second = dupefilter.request_seen_with_reservation(second_request)
    assert second.reservation is not None
    dupefilter.commit_volatile_reservation(second.reservation)

    assert len(dupefilter._volatile_fingerprints) == 1
    assert dupefilter._volatile_fingerprint_overflow_warned is True
    assert dupefilter.request_seen_with_reservation(second_request.replace()).seen
    replay = dupefilter.request_seen_with_reservation(first_request.replace())
    assert replay.seen is False
    assert replay.reservation is not None


def test_stale_receipt_cannot_roll_back_post_clear_reservation(
    mock_connection_manager: Any,
) -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    request = Request("https://example.test/receipt-aba")
    stale = dupefilter.request_seen_with_reservation(request)
    assert stale.reservation is not None

    dupefilter.clear()
    current = dupefilter.request_seen_with_reservation(request.replace())
    assert current.reservation is not None
    dupefilter.rollback_reservation(stale.reservation)
    dupefilter.commit_reservation(current.reservation)

    duplicate = dupefilter.request_seen_with_reservation(request.replace())
    assert duplicate.seen is True
    assert duplicate.observational is False


def test_failed_clear_retains_receipt_rollback_ownership(
    mock_connection_manager: Any,
) -> None:
    membership = _FailingClearMemoryFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
    )
    request = Request("https://example.test/failed-clear")
    decision = dupefilter.request_seen_with_reservation(request)
    assert decision.reservation is not None

    with pytest.raises(BackendConnectionError):
        dupefilter.clear()
    dupefilter.rollback_reservation(decision.reservation)

    retry = dupefilter.request_seen_with_reservation(request.replace())
    assert retry.seen is False
    assert retry.reservation is not None
    dupefilter.commit_reservation(retry.reservation)


def test_close_compensates_uncommitted_receipt(
    mock_connection_manager: Any,
) -> None:
    membership = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        owns_connection_manager=False,
    )
    decision = dupefilter.request_seen_with_reservation(
        Request("https://example.test/close-receipt")
    )
    assert decision.reservation is not None

    dupefilter.close("test")

    assert len(membership) == 0


def test_capacity_is_not_read_when_custom_filter_has_no_saturation(
    mock_connection_manager: Any,
) -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=_NoSaturationCapacityPoisonFilter(),
    )
    request = Request("https://example.test/lazy-capacity")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True


def test_concurrent_monitor_dispatch_is_fifo_non_overlapping_and_non_waiting(
    mock_connection_manager: Any,
) -> None:
    monitor = _FifoProbeMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
        monitor=monitor,
    )
    first_request = Request("https://example.test/fifo/first")
    second_request = Request("https://example.test/fifo/second")
    first_key = dupefilter.request_fingerprint(first_request)
    second_key = dupefilter.request_fingerprint(second_request)
    results: dict[str, bool] = {}
    second_done = threading.Event()

    first_thread = threading.Thread(
        target=lambda: results.__setitem__(
            "first", dupefilter.request_seen(first_request)
        ),
        daemon=True,
    )

    def submit_second() -> None:
        results["second"] = dupefilter.request_seen(second_request)
        second_done.set()

    second_thread = threading.Thread(target=submit_second, daemon=True)
    first_thread.start()
    try:
        assert monitor.first_entered.wait(2.0)
        second_thread.start()
        # A peer request must not wait for the elected monitor drainer. Its event
        # remains in the shared FIFO and is delivered by that drainer in order.
        assert second_done.wait(2.0)
    finally:
        monitor.release_first.set()
    first_thread.join(timeout=2.0)
    second_thread.join(timeout=2.0)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert results == {"first": False, "second": False}
    assert monitor.call_order == [first_key, second_key]
    assert monitor.completion_order == [first_key, second_key]
    assert monitor.max_active == 1


def test_blocked_monitor_backlog_is_bounded_by_complete_event_batches(
    mock_connection_manager: Any,
) -> None:
    monitor = _FifoProbeMonitor()
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=BloomMembershipFilter(capacity=1_000, error_rate=0.01),
        monitor=monitor,
    )
    # The first miss is active while its saturation event remains queued. One
    # more two-event [miss, saturation] batch fits exactly; the next has only one
    # slot available and must therefore be dropped whole.
    dupefilter._monitor_event_limit = 4
    requests = [
        Request(f"https://example.test/bounded-fifo/{index}") for index in range(3)
    ]
    keys = [dupefilter.request_fingerprint(request) for request in requests]

    first_thread = threading.Thread(
        target=dupefilter.request_seen,
        args=(requests[0],),
        daemon=True,
    )
    first_thread.start()
    try:
        assert monitor.first_entered.wait(2.0)
        assert dupefilter.request_seen(requests[1]) is False
        assert dupefilter.request_seen(requests[2]) is False
        assert len(dupefilter._monitor_events) == 3
        assert dupefilter._monitor_overflow_warned is True
    finally:
        monitor.release_first.set()
    first_thread.join(timeout=2.0)

    assert not first_thread.is_alive()
    assert monitor.call_order == keys[:2]
    assert monitor.completion_order == keys[:2]
    assert monitor.max_active == 1


def test_hook_attribute_failure_is_isolated_after_reservation(
    mock_connection_manager: Any,
) -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
        monitor=_AttributeRaisingMonitor(),
    )
    request = Request("https://example.test/descriptor")

    assert dupefilter.request_seen(request) is False
    assert dupefilter.consume_reservation(request) is True


@pytest.mark.parametrize(
    ("hook", "maxsize"),
    [
        ("on_dedup_miss", 10),
        ("on_filter_saturation", 1),
    ],
)
def test_monitor_hooks_run_after_lifecycle_lock_release(
    mock_connection_manager: Any,
    hook: str,
    maxsize: int,
) -> None:
    monitor = _LifecycleLockProbeMonitor(hook)
    membership = MemoryMembershipFilter(maxsize=maxsize)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=monitor,
    )
    monitor.bind(dupefilter._lifecycle_lock)

    assert (
        dupefilter.request_seen(Request(f"https://example.test/lock/{hook}")) is False
    )
    monitor.join()
    assert monitor.completed_during_hook == [True]


def test_dupefilter_does_not_swallow_process_control_exception(
    mock_connection_manager: Any,
) -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
        monitor=_ProcessControlMonitor(),
    )
    request = Request("https://example.test/process-control")

    with pytest.raises(_MonitorStop):
        dupefilter.request_seen(request)
    assert dupefilter.consume_reservation(request) is True
    assert dupefilter._monitor_drain_token is not None
    assert dupefilter._monitor_drain_token.active is False


def test_atomic_rollback_discards_before_propagating_process_control(
    mock_connection_manager: Any,
) -> None:
    signal = _MonitorStop()
    membership = MemoryMembershipFilter(maxsize=10)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=membership,
        monitor=_OneShotProcessControlMonitor(signal),
    )
    request = Request("https://example.test/atomic-process-control")

    decision = dupefilter.request_seen_with_reservation(request)
    assert decision.reservation is not None

    with pytest.raises(_MonitorStop) as raised:
        dupefilter.rollback_reservation(decision.reservation)
    assert raised.value is signal
    assert len(membership) == 0
    assert dupefilter.consume_reservation(request) is False
    assert dupefilter._monitor_drain_token is not None
    assert dupefilter._monitor_drain_token.active is False

    retry = dupefilter.request_seen_with_reservation(request)
    assert retry.seen is False
    assert retry.reservation is not None
    dupefilter.commit_reservation(retry.reservation)
    assert dupefilter._monitor_drain_token is None


def test_process_control_before_drain_releases_election(
    mock_connection_manager: Any,
    mocker: MockerFixture,
) -> None:
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
    )
    real_drain = dupefilter._drain_monitor_events
    drain = mocker.patch.object(
        dupefilter,
        "_drain_monitor_events",
        side_effect=_MonitorStop,
    )
    first = Request("https://example.test/pre-drain-stop")

    with pytest.raises(_MonitorStop):
        dupefilter.request_seen(first)
    assert dupefilter.consume_reservation(first) is True
    assert dupefilter._monitor_drain_token is not None
    assert dupefilter._monitor_drain_token.active is False
    assert len(dupefilter._monitor_events) == 1

    # A later decision can win a fresh election and drain both the retained
    # event and its own; the aborted owner never poisons the FIFO permanently.
    drain.side_effect = real_drain
    second = Request("https://example.test/post-drain-stop")
    assert dupefilter.request_seen(second) is False
    assert dupefilter._monitor_drain_token is None
    assert not dupefilter._monitor_events


def test_memory_filter_does_not_swallow_process_control_exception() -> None:
    membership = MemoryMembershipFilter(maxsize=1)
    membership.set_monitor(_ProcessControlMonitor())

    with pytest.raises(_MonitorStop):
        membership.add(b"process-control")
    assert b"process-control" in membership
