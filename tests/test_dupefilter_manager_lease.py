"""Token-aware BackendDupeFilter teardown regressions."""

from __future__ import annotations

import threading
from unittest.mock import Mock

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.base import MembershipFilter


def _dupefilter_with_lease(
    membership_filter: MembershipFilter,
    *,
    host: str,
) -> tuple[BackendDupeFilter, object, Mock]:
    lease = ConnectionManager.acquire_lease(BackendType.REDIS, {"host": host})
    backend = Mock()
    lease.manager._backend = backend
    dupefilter = BackendDupeFilter(
        connection_manager=lease.manager,
        membership_filter=membership_filter,
        connection_manager_lease=lease,
    )
    return dupefilter, lease, backend


def test_filter_and_manager_lifecycle_callbacks_run_outside_lock() -> None:
    manager = Mock()
    membership_filter = Mock(spec=MembershipFilter)
    dupefilter = BackendDupeFilter(
        connection_manager=manager,
        membership_filter=membership_filter,
        clear_on_open=True,
    )
    observations: list[bool] = []

    def observe(*_args: object, **_kwargs: object) -> None:
        observations.append(dupefilter._lifecycle_lock._is_owned())  # type: ignore[attr-defined]

    membership_filter.open.side_effect = observe
    membership_filter.clear.side_effect = observe
    membership_filter.close.side_effect = lambda: (
        dupefilter.close("reentrant"),
        observe(),
    )
    manager.close.side_effect = observe

    dupefilter.open()
    dupefilter.clear()
    dupefilter.close("finished")

    assert observations == [False, False, False, False, False]
    assert dupefilter._closed is True


def test_filter_failure_retains_manager_lease_for_retry() -> None:
    membership_filter = Mock(spec=MembershipFilter)
    membership_filter.close.side_effect = [RuntimeError("filter failed"), None]
    dupefilter, lease, backend = _dupefilter_with_lease(
        membership_filter,
        host="dupefilter-filter-retry",
    )
    owner = object()

    with pytest.raises(RuntimeError, match="filter failed"):
        dupefilter.release(owner, "first")

    assert lease.released is False
    backend.disconnect.assert_not_called()

    dupefilter.release(owner, "retry")
    assert lease.released is True
    assert membership_filter.close.call_count == 2
    backend.disconnect.assert_called_once_with()


def test_manager_effect_then_control_error_retries_same_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    membership_filter = Mock(spec=MembershipFilter)
    dupefilter, lease, backend = _dupefilter_with_lease(
        membership_filter,
        host="dupefilter-manager-retry",
    )
    owner = object()
    original_release = type(lease).release
    calls = 0

    def release_then_raise(current_lease: object) -> None:
        nonlocal calls
        calls += 1
        original_release(current_lease)  # type: ignore[arg-type]
        if calls == 1:
            raise KeyboardInterrupt

    monkeypatch.setattr(type(lease), "release", release_then_raise)

    with pytest.raises(KeyboardInterrupt):
        dupefilter.release(owner, "first")

    assert lease.released is True
    dupefilter.release(owner, "retry")
    assert calls == 2
    assert membership_filter.close.call_count == 1
    backend.disconnect.assert_called_once_with()


def test_persistent_filter_failure_never_releases_manager() -> None:
    membership_filter = Mock(spec=MembershipFilter)
    membership_filter.close.side_effect = RuntimeError("persistent")
    dupefilter, lease, backend = _dupefilter_with_lease(
        membership_filter,
        host="dupefilter-persistent-filter",
    )
    owner = object()

    for _ in range(2):
        with pytest.raises(RuntimeError, match="persistent"):
            dupefilter.release(owner, "retry")

    assert lease.released is False
    assert dupefilter._closed is False
    backend.disconnect.assert_not_called()


def test_concurrent_release_cannot_overtake_active_open() -> None:
    manager = Mock()
    membership_filter = Mock(spec=MembershipFilter)
    entered = threading.Event()
    continue_open = threading.Event()

    def blocking_open() -> None:
        entered.set()
        assert continue_open.wait(timeout=3)

    membership_filter.open.side_effect = blocking_open
    dupefilter = BackendDupeFilter(
        connection_manager=manager,
        membership_filter=membership_filter,
    )
    errors: list[BaseException] = []

    def run_open() -> None:
        try:
            dupefilter.open()
        except BaseException as exc:
            errors.append(exc)

    opener = threading.Thread(target=run_open, name="dupefilter-open-owner")
    opener.start()
    assert entered.wait(timeout=3)
    try:
        with pytest.raises(RuntimeError, match="open is already in progress"):
            dupefilter.close("concurrent")
        assert dupefilter._closing is False
        membership_filter.close.assert_not_called()
    finally:
        continue_open.set()
    opener.join(timeout=3)

    assert not opener.is_alive()
    assert errors == []
    assert dupefilter._opened is True
    assert dupefilter._closed is False

    dupefilter.close("finished")
    membership_filter.close.assert_called_once_with()
    manager.close.assert_called_once_with()
