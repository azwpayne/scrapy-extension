"""fix-dupe teardown hygiene: close() must not retain telemetry state.

L-d9: after a successful close, the bounded monitor-event FIFO (up to 1,024
entries, each holding a strong ``Request`` reference) and the drain-election
token used to be left in place until the dupefilter object itself was
collected. Nothing dispatches after close, so close() now clears both.

L-s3: the secondary manager-close failure inside ``_close_locked`` rendered the
driving exception into the log via an explicit ``exc_info`` tuple, which can
publish backend endpoint details through custom handlers. It now logs a static
message only (mirrors scheduler.py's teardown diagnostics).
"""

from __future__ import annotations

import gc
import logging
import sys
import weakref
from typing import Any

import pytest
from scrapy.http import Request
from scrapy.settings import Settings

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter


class _DrainInterrupted(Exception):
    """Sentinel raised by a patched drainer to retain queued telemetry."""


class _ExceptionContextProbe(logging.Handler):
    """Capture implicit exception state visible to custom logging handlers."""

    def __init__(self) -> None:
        super().__init__()
        self.exceptions: list[BaseException | None] = []

    def emit(self, record: logging.LogRecord) -> None:
        del record
        self.exceptions.append(sys.exc_info()[1])


def _dupefilter_with_retained_events(
    mocker: Any,
    mock_connection_manager: Any,
) -> BackendDupeFilter:
    """Build a dupefilter whose next decision leaves one event queued.

    Patching ``_drain_monitor_events`` to raise reproduces the retained-FIFO
    state an interrupted/aborted drainer leaves behind (same mechanism as
    ``test_process_control_before_drain_releases_election``).
    """
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=MemoryMembershipFilter(maxsize=10),
    )
    mocker.patch.object(
        dupefilter,
        "_drain_monitor_events",
        side_effect=_DrainInterrupted,
    )
    return dupefilter


class TestCloseReleasesTelemetryState:
    """L-d9: closing drops the queued event FIFO and the drain token."""

    def test_close_clears_queued_events_and_drain_token(
        self, mocker: Any, mock_connection_manager: Any
    ) -> None:
        dupefilter = _dupefilter_with_retained_events(mocker, mock_connection_manager)
        request = Request("https://example.test/retained")

        with pytest.raises(_DrainInterrupted):
            dupefilter.request_seen(request)
        assert len(dupefilter._monitor_events) == 1
        assert dupefilter._monitor_drain_token is not None

        dupefilter.close("teardown")

        assert not dupefilter._monitor_events
        assert dupefilter._monitor_drain_token is None

    def test_close_releases_queued_request_references(
        self, mocker: Any, mock_connection_manager: Any
    ) -> None:
        """The queued telemetry batch is the last strong Request reference."""
        dupefilter = _dupefilter_with_retained_events(mocker, mock_connection_manager)
        request = Request("https://example.test/gc")
        marker = weakref.ref(request)

        try:
            dupefilter.request_seen(request)
        except _DrainInterrupted:
            pass  # clear the traceback frames referencing the request
        del request
        gc.collect()
        assert marker() is not None  # still pinned by the queued batch

        dupefilter.close("teardown")
        gc.collect()
        assert marker() is None  # close dropped the reference

    def test_close_does_not_dispatch_retained_events(
        self, mocker: Any, mock_connection_manager: Any
    ) -> None:
        """Closing discards retained telemetry instead of flushing it late."""
        monitor = mocker.Mock(name="monitor")
        dupefilter = BackendDupeFilter(
            connection_manager=mock_connection_manager,
            membership_filter=MemoryMembershipFilter(maxsize=10),
            monitor=monitor,
        )
        mocker.patch.object(
            dupefilter,
            "_drain_monitor_events",
            side_effect=_DrainInterrupted,
        )
        request = Request("https://example.test/no-late-dispatch")
        with pytest.raises(_DrainInterrupted):
            dupefilter.request_seen(request)

        dupefilter.close("teardown")

        monitor.on_dedup_miss.assert_not_called()

    def test_double_close_stays_idempotent(
        self, mocker: Any, mock_connection_manager: Any
    ) -> None:
        dupefilter = _dupefilter_with_retained_events(mocker, mock_connection_manager)
        request = Request("https://example.test/idempotent")
        with pytest.raises(_DrainInterrupted):
            dupefilter.request_seen(request)

        dupefilter.close("first")
        dupefilter.close("second")  # already closed: no-op, no error

        assert not dupefilter._monitor_events
        assert dupefilter._monitor_drain_token is None


class TestCloseSecondaryFailureStaticDiagnostic:
    """L-s3: secondary close failures log static messages without ``exc_info``."""

    def test_lease_release_failure_logs_static_error_without_exc_info(
        self,
        mocker: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        manager = mocker.MagicMock(name="ConnectionManager")
        lease = mocker.MagicMock(name="ConnectionManagerLease")
        membership = mocker.MagicMock(spec=MembershipFilter)
        dupefilter = BackendDupeFilter(
            connection_manager=manager,
            membership_filter=membership,
            connection_manager_lease=lease,
        )
        primary = _DrainInterrupted("filter close failed")

        def release_then_fail() -> None:
            # Mark the filter released before failing so teardown proceeds to
            # lease release and hits the secondary-failure diagnostic.
            dupefilter._filter_released = True
            raise primary

        membership.close.side_effect = release_then_fail
        lease.release.side_effect = RuntimeError(
            "manager release failed: redis://secret@example.invalid"
        )

        logger_under_test = logging.getLogger("scrapy_extension.dupefilter.dupefilter")
        probe = _ExceptionContextProbe()
        logger_under_test.addHandler(probe)
        try:
            with caplog.at_level(
                logging.ERROR, logger="scrapy_extension.dupefilter.dupefilter"
            ):
                with pytest.raises(_DrainInterrupted) as raised:
                    dupefilter.close("coverage")
        finally:
            logger_under_test.removeHandler(probe)
        assert raised.value is primary
        lease.release.assert_called_once_with()
        manager.close.assert_not_called()

        records = [
            record
            for record in caplog.records
            if record.levelno == logging.ERROR
            and "ConnectionManager close failed while preserving filter close error"
            in record.getMessage()
        ]
        assert len(records) == 1
        assert records[0].exc_info is None
        assert records[0].exc_text is None
        assert probe.exceptions == [None]
        assert "secret" not in records[0].getMessage()


def test_close_without_manager_releases_filter_only(mocker: Any) -> None:
    """L-d5 companion: a manager-less dupefilter closes cleanly.

    In-process strategies from ``from_settings`` own no ConnectionManager; the
    close path must treat that as "nothing to release" rather than skipping the
    closed transition.
    """
    settings = Settings({"SCRAPY_DEDUP_STRATEGY": "memory"})
    dupefilter = BackendDupeFilter.from_settings(settings)

    assert dupefilter.connection_manager is None

    dupefilter.close("done")

    assert dupefilter._closed is True
    assert not dupefilter._monitor_events
    assert dupefilter._monitor_drain_token is None
