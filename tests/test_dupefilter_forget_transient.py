"""fix-dupe M-1: transient backend failures during ``forget()`` compensation.

Pre-fix (RED): ``forget`` caught only ``NotImplementedError``. When the
backend-backed set raised ``BackendConnectionError`` /
``CircuitBreakerOpenError`` during ``remove`` (network outage, tripped breaker),
the error propagated to the scheduler's best-effort rollback — which swallowed
it — while the remote marker survived as a ghost that judged the URL seen for
the rest of the crawl. Post-fix (GREEN): the transient arm grants the same
one-shot retry allowance the non-removable (Bloom) path uses, warns once per
process, and counts the failure via ``monitor.on_error("dedup", ...)`` so the
URL can be re-crawled exactly once more.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest
from scrapy.http import Request

from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.exceptions.base import BackendConnectionError


class _FlakyRemoveFilter(MembershipFilter):
    """Exact set-like filter whose ``remove`` cannot reach its backend.

    Models a backend-backed set during an outage: membership operations still
    work locally, but the deletion call fails, so the marker survives remotely.
    """

    def __init__(self, remove_error: BaseException | None = None) -> None:
        self._items: set[bytes] = set()
        self.remove_error = remove_error

    def add(self, item: bytes) -> bool:
        if item in self._items:
            return False
        self._items.add(item)
        return True

    def __contains__(self, item: object) -> bool:
        return item in self._items

    def __len__(self) -> int:
        return len(self._items)

    def clear(self) -> None:
        self._items.clear()

    def remove(self, item: bytes) -> bool:
        if self.remove_error is not None:
            raise self.remove_error
        if item in self._items:
            self._items.discard(item)
            return True
        return False


@pytest.fixture(autouse=True)
def _reset_warn_once_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate the process-global warn-once latches per test."""
    from scrapy_extension.dupefilter import dupefilter as dupefilter_module

    monkeypatch.setattr(dupefilter_module, "_forget_backend_error_warned", False)
    monkeypatch.setattr(dupefilter_module, "_backend_error_warned", False)


def _make_dupefilter(
    mock_connection_manager: Any,
    remove_error: BaseException | None,
    monitor: Mock | None = None,
) -> tuple[BackendDupeFilter, _FlakyRemoveFilter]:
    flt = _FlakyRemoveFilter(remove_error=remove_error)
    dupefilter = BackendDupeFilter(
        connection_manager=mock_connection_manager,
        membership_filter=flt,
        monitor=monitor,
    )
    return dupefilter, flt


class TestForgetTransientBackendError:
    """M-1: a transient ``remove`` failure degrades instead of ghosting a URL."""

    def test_connection_error_grants_one_shot_retry_allowance(
        self, mock_connection_manager: Any
    ) -> None:
        """``remove`` raising BackendConnectionError → allowance, no raise.

        The surviving remote marker would judge the URL seen for the rest of the
        crawl; the allowance admits exactly one re-crawl (and only one).
        """
        dupefilter, flt = _make_dupefilter(
            mock_connection_manager,
            BackendConnectionError("connection lost: redis-primary:6379"),
        )
        request = Request("https://example.com/ghost-marker")

        assert dupefilter.request_seen(request) is False  # marker published
        dupefilter.forget(request)  # must not raise despite the outage
        assert len(flt) == 1  # the marker survived the failed removal

        # One allowance-consumed miss, then the surviving marker resumes dedup.
        assert dupefilter.request_seen(request) is False
        assert dupefilter.request_seen(request) is True

    def test_circuit_breaker_rejection_grants_one_shot_retry_allowance(
        self, mock_connection_manager: Any
    ) -> None:
        """A tripped breaker follows the same transient-outage envelope."""
        dupefilter, _flt = _make_dupefilter(
            mock_connection_manager,
            CircuitBreakerOpenError("redis-set"),
        )
        request = Request("https://example.com/open-circuit")

        assert dupefilter.request_seen(request) is False
        dupefilter.forget(request)
        assert dupefilter.request_seen(request) is False
        assert dupefilter.request_seen(request) is True

    def test_error_emits_static_package_error_on_dedup_scope(
        self, mock_connection_manager: Any
    ) -> None:
        """``monitor.on_error("dedup", safe_error)`` fires with a static message.

        The driving exception may carry endpoint details; the reported error
        must be the package's fixed-string error instead.
        """
        monitor = Mock(name="monitor")
        dupefilter, _flt = _make_dupefilter(
            mock_connection_manager,
            BackendConnectionError("connection lost: redis-primary:6379"),
            monitor=monitor,
        )
        request = Request("https://example.com/static-error")
        dupefilter.request_seen(request)

        dupefilter.forget(request)

        monitor.on_error.assert_called_once()
        scope, reported = monitor.on_error.call_args.args
        assert scope == "dedup"
        assert isinstance(reported, BackendConnectionError)
        assert str(reported) == "Dedup backend is unavailable."

    def test_circuit_rejection_reports_dedup_breaker_name(
        self, mock_connection_manager: Any
    ) -> None:
        monitor = Mock(name="monitor")
        dupefilter, _flt = _make_dupefilter(
            mock_connection_manager,
            CircuitBreakerOpenError("redis-set-primary"),
            monitor=monitor,
        )
        request = Request("https://example.com/breaker-name")
        dupefilter.request_seen(request)

        dupefilter.forget(request)

        _scope, reported = monitor.on_error.call_args.args
        assert isinstance(reported, CircuitBreakerOpenError)
        assert reported.name == "dedup"

    def test_monitor_dispatch_runs_outside_lifecycle_lock(
        self, mock_connection_manager: Any
    ) -> None:
        observations: list[bool] = []
        monitor = Mock(name="monitor")
        dupefilter, _flt = _make_dupefilter(
            mock_connection_manager,
            BackendConnectionError("outage"),
            monitor=monitor,
        )
        monitor.on_error.side_effect = lambda *_args: observations.append(
            dupefilter._lifecycle_lock._is_owned()  # type: ignore[attr-defined]
        )
        request = Request("https://example.com/outside-lock")
        dupefilter.request_seen(request)

        dupefilter.forget(request)

        assert observations == [False]

    def test_compensation_emits_no_dedup_decision_hooks(
        self, mock_connection_manager: Any
    ) -> None:
        """``forget`` compensates a prior decision; it makes no new one."""
        monitor = Mock(name="monitor")
        dupefilter, _flt = _make_dupefilter(
            mock_connection_manager,
            BackendConnectionError("outage"),
            monitor=monitor,
        )
        request = Request("https://example.com/no-miss-hook")
        dupefilter.request_seen(request)
        monitor.on_dedup_miss.reset_mock()

        dupefilter.forget(request)

        monitor.on_dedup_miss.assert_not_called()
        monitor.on_dedup_hit.assert_not_called()

    def test_warns_once_per_process(
        self, mock_connection_manager: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Two transient ``remove`` failures log exactly one WARNING."""
        import logging

        dupefilter, _flt = _make_dupefilter(
            mock_connection_manager,
            BackendConnectionError("outage"),
        )
        requests = [
            Request("https://example.com/warn-once/1"),
            Request("https://example.com/warn-once/2"),
        ]
        for request in requests:
            dupefilter.request_seen(request)

        with caplog.at_level(
            logging.WARNING, logger="scrapy_extension.dupefilter.dupefilter"
        ):
            for request in requests:
                dupefilter.forget(request)

        warnings = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING
            and "while compensating a failed queue push" in r.getMessage()
        ]
        assert len(warnings) == 1

    def test_every_occurrence_counts_via_monitor(
        self, mock_connection_manager: Any
    ) -> None:
        """Beyond the warn-once latch, each failed ``remove`` is counted."""
        monitor = Mock(name="monitor")
        dupefilter, _flt = _make_dupefilter(
            mock_connection_manager,
            BackendConnectionError("outage"),
            monitor=monitor,
        )
        requests = [
            Request("https://example.com/counted/1"),
            Request("https://example.com/counted/2"),
        ]
        for request in requests:
            dupefilter.request_seen(request)
        for request in requests:
            dupefilter.forget(request)

        assert monitor.on_error.call_count == 2

    def test_successful_removal_still_grants_no_allowance(
        self, mock_connection_manager: Any
    ) -> None:
        """The healthy path is unchanged: removal succeeds, nothing is armed."""
        dupefilter, flt = _make_dupefilter(mock_connection_manager, None)
        request = Request("https://example.com/healthy")

        assert dupefilter.request_seen(request) is False
        dupefilter.forget(request)

        assert len(flt) == 0
        assert not dupefilter._retry_allowances
        # The marker is genuinely gone — a re-crawl is a fresh miss.
        assert dupefilter.request_seen(request) is False
