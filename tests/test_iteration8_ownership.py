"""Iteration-eight ownership and rollback regressions."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest
from scrapy import Spider
from scrapy.settings import Settings
from twisted.internet.defer import Deferred

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import (
    ConnectionManager,
    release_manager_acquire,
)
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin


class _OwnershipSpider(BackendSpiderMixin, Spider):
    name = "iteration-eight"
    backend_type = BackendType.REDIS


def test_setup_monitor_recursion_releases_only_this_exact_acquire() -> None:
    peer = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "iteration8-monitor"}
    )
    spider = _OwnershipSpider()
    spider.redis_host = "iteration8-monitor"
    manager = None

    try:
        manager = peer

        def recurse(_monitor: object) -> None:
            assert spider.close_backend() is None

        manager.set_monitor = recurse  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="completed after close"):
            spider.setup_backend()

        assert spider._connection_manager is None
        assert manager._users == 1
        peer.close()
        assert manager._users == 0
    finally:
        ConnectionManager.clear_registry()


def test_setup_signal_recursion_releases_only_this_exact_acquire() -> None:
    peer = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "iteration8-signal"}
    )
    spider = _OwnershipSpider()
    spider.redis_host = "iteration8-signal"
    signal_manager = MagicMock()
    spider.crawler = MagicMock(signals=signal_manager, settings=Settings())

    def connect(*_args: object, **_kwargs: object) -> None:
        spider.close_backend()

    signal_manager.connect.side_effect = connect
    try:
        with pytest.raises(RuntimeError, match="completed after close"):
            spider.setup_backend()
        assert peer._users == 1
        peer.close()
        assert peer._users == 0
    finally:
        ConnectionManager.clear_registry()


def test_setup_effect_then_raise_retains_retryable_exact_release(monkeypatch) -> None:
    spider = _OwnershipSpider()
    spider.redis_host = "iteration8-effect-raise"
    manager = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "iteration8-effect-raise"}
    )
    original_finalize = manager._finalize_retirement
    calls = 0

    def finalize_then_raise() -> None:
        nonlocal calls
        calls += 1
        original_finalize()
        if calls == 1:
            raise RuntimeError("release effect then raise")

    monkeypatch.setattr(manager, "_finalize_retirement", finalize_then_raise)
    monkeypatch.setattr(manager, "set_monitor", lambda _monitor: spider.close_backend())
    monkeypatch.setattr(
        ConnectionManager,
        "get_manager",
        classmethod(lambda cls, **_kwargs: manager),
    )
    try:
        with pytest.raises(RuntimeError, match="completed after close"):
            spider.setup_backend()
        assert manager._users == 0
        spider.close_backend()
        assert not spider._orphan_leases
    finally:
        ConnectionManager.clear_registry()


def test_unpublished_candidate_and_leases_are_retryable_before_parent_release(
    mocker,
) -> None:
    spider = _OwnershipSpider()
    parent_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = parent_manager
    candidate = MagicMock()
    candidate.close.side_effect = [RuntimeError("candidate"), None]
    lease = MagicMock()
    lease.release.side_effect = [RuntimeError("lease"), None]
    candidate._connection_manager_lease = lease

    def construct(*_args: object, **_kwargs: object) -> object:
        spider.close_backend()
        return candidate

    mocker.patch.object(BackendDupeFilter, "from_settings", side_effect=construct)
    with pytest.raises(RuntimeError, match="construction completed"):
        spider.get_dupefilter()

    assert spider._orphan_candidates == [("dupefilter", candidate)]
    assert parent_manager.close.call_count == 0
    spider.close_backend()
    assert spider._orphan_candidates == []
    assert spider._orphan_leases == []
    parent_manager.close.assert_called_once_with()


@pytest.mark.parametrize(
    "control", [RuntimeError("ordinary"), KeyboardInterrupt("control")]
)
def test_pipeline_factory_rollback_retries_failed_manager_release(
    mocker,
    control: BaseException,
) -> None:
    manager = mocker.MagicMock()
    manager.close.side_effect = [RuntimeError("release"), None]
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    mocker.patch(
        "scrapy_extension.pipeline.pipeline.create_storage_strategy",
        side_effect=control,
    )

    with pytest.raises(type(control)):
        BackendPipeline.from_settings(Settings({"SCRAPY_BACKEND_TYPE": "redis"}))
    assert manager.close.call_count == 2


def test_dupefilter_factory_retries_failed_exact_lease_release(mocker) -> None:
    lease = mocker.MagicMock()
    lease.manager = mocker.MagicMock()
    lease.release.side_effect = [RuntimeError("release"), None]
    mocker.patch.object(ConnectionManager, "acquire_lease", return_value=lease)
    mocker.patch(
        "scrapy_extension.dupefilter.filters.factory.build_membership_filter",
        side_effect=RuntimeError("factory"),
    )

    with pytest.raises(RuntimeError, match="factory"):
        BackendDupeFilter.from_settings(Settings({"SCRAPY_BACKEND_TYPE": "redis"}))
    assert lease.release.call_count == 2


def test_scheduler_factory_retries_failed_exact_lease_release(mocker) -> None:
    lease = mocker.MagicMock()
    lease.manager = mocker.MagicMock()
    lease.release.side_effect = [KeyboardInterrupt("release"), None]
    mocker.patch.object(ConnectionManager, "acquire_lease", return_value=lease)
    mocker.patch.object(
        BackendScheduler,
        "_enforce_ack_concurrency_gate",
        side_effect=RuntimeError("factory"),
    )

    with pytest.raises(RuntimeError, match="factory"):
        BackendScheduler.from_settings(Settings({"SCRAPY_BACKEND_TYPE": "redis"}))
    assert lease.release.call_count == 2


def test_release_rollback_preserves_first_error_when_both_attempts_fail() -> None:
    owner = MagicMock()
    owner.close.side_effect = KeyboardInterrupt("first")
    with pytest.raises(KeyboardInterrupt, match="first"):
        release_manager_acquire(owner)
    assert owner.close.call_count == 2


def test_pending_release_retry_reentrancy_is_bounded() -> None:
    thread_id = threading.get_ident()
    pending_lease = ConnectionManager.acquire_lease(
        BackendType.REDIS,
        {"host": "iteration8-reentrant-lease"},
    )
    pending_manager = ConnectionManager.get_manager(
        BackendType.REDIS,
        {"host": "iteration8-reentrant-manager"},
    )
    original_leases = ConnectionManager._pending_release_leases
    original_managers = ConnectionManager._pending_release_managers
    ConnectionManager._pending_release_leases = [pending_lease]
    ConnectionManager._pending_release_managers = [pending_manager]
    ConnectionManager._pending_release_retry_threads.add(thread_id)
    try:
        ConnectionManager.retry_pending_releases()
        assert pending_lease.released is False
        assert pending_lease.manager._users == 1
        assert pending_manager._users == 1
        assert ConnectionManager._pending_release_leases == [pending_lease]
        assert ConnectionManager._pending_release_managers == [pending_manager]
        assert thread_id in ConnectionManager._pending_release_retry_threads
    finally:
        ConnectionManager._pending_release_leases = original_leases
        ConnectionManager._pending_release_managers = original_managers
        ConnectionManager._pending_release_retry_threads.discard(thread_id)
        pending_lease.release()
        pending_manager.close()


def test_legacy_handoff_is_consumed_only_once() -> None:
    manager = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "iteration8-handoff"}
    )
    lease = ConnectionManager._adopt_latest_legacy_lease(manager)
    assert lease is not None
    assert ConnectionManager._adopt_latest_legacy_lease(manager) is None
    lease.release()
    ConnectionManager.clear_registry()


def test_registry_retains_and_retries_failed_exact_release(monkeypatch) -> None:
    lease = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": "iteration8-pending-lease"}
    )
    manager = lease.manager
    original_release = manager._release_acquire
    monkeypatch.setattr(
        manager,
        "_release_acquire",
        lambda _token: (_ for _ in ()).throw(RuntimeError("release")),
    )
    with pytest.raises(RuntimeError, match="release"):
        lease.release()
    manager._retain_failed_lease(lease)
    monkeypatch.setattr(manager, "_release_acquire", original_release)
    ConnectionManager.retry_pending_releases()
    assert lease.released
    assert manager._users == 0
    ConnectionManager.clear_registry()


def test_registry_retries_pending_release_failures(monkeypatch) -> None:
    lease = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": "iteration8-pending-retry"}
    )
    manager = lease.manager
    original_release = manager._release_acquire
    monkeypatch.setattr(
        manager,
        "_release_acquire",
        lambda _token: (_ for _ in ()).throw(RuntimeError("release")),
    )
    with pytest.raises(RuntimeError):
        lease.release()
    ConnectionManager.retry_pending_releases()
    monkeypatch.setattr(manager, "_release_acquire", original_release)
    ConnectionManager.retry_pending_releases()
    assert manager._users == 0
    ConnectionManager.clear_registry()


def test_registry_retries_pending_legacy_release_failures(monkeypatch) -> None:
    manager = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "iteration8-pending-manager-retry"}
    )
    original_release = manager._release_acquire_under_lock
    monkeypatch.setattr(
        manager,
        "_release_acquire_under_lock",
        lambda _token: (_ for _ in ()).throw(RuntimeError("release")),
    )
    with pytest.raises(ConfigurationError):
        manager.close()
    ConnectionManager.retry_pending_releases()
    monkeypatch.setattr(manager, "_release_acquire_under_lock", original_release)
    ConnectionManager.retry_pending_releases()
    assert manager._users == 0
    ConnectionManager.clear_registry()


def test_registry_retains_and_retries_failed_legacy_release(monkeypatch) -> None:
    manager = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "iteration8-pending-manager"}
    )
    original_release = manager._release_acquire_under_lock
    monkeypatch.setattr(
        manager,
        "_release_acquire_under_lock",
        lambda _token: (_ for _ in ()).throw(RuntimeError("release")),
    )
    with pytest.raises(ConfigurationError):
        manager.close()
    monkeypatch.setattr(manager, "_release_acquire_under_lock", original_release)
    ConnectionManager.retry_pending_releases()
    assert manager._users == 0
    ConnectionManager.clear_registry()


def test_orphan_manager_cleanup_succeeds_on_retry(mocker) -> None:
    spider = _OwnershipSpider()
    manager = mocker.MagicMock()
    manager.close.side_effect = [RuntimeError("manager"), None]
    spider._orphan_managers = [manager]
    assert isinstance(spider._cleanup_orphan_candidates("retry"), RuntimeError)
    assert spider._cleanup_orphan_candidates("retry") is None
    assert spider._orphan_managers == []


def test_orphan_manager_cleanup_preserves_first_failure(mocker) -> None:
    spider = _OwnershipSpider()
    manager = mocker.MagicMock()
    manager.close.side_effect = KeyboardInterrupt("manager")
    spider._orphan_managers = [manager]
    error = spider._cleanup_orphan_candidates("retry")
    assert isinstance(error, KeyboardInterrupt)
    assert spider._orphan_managers == [manager]


def test_pipeline_factory_rollback_survives_diagnostic_failure(mocker) -> None:
    manager = mocker.MagicMock()
    manager.close.side_effect = RuntimeError("release")
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    mocker.patch(
        "scrapy_extension.pipeline.pipeline.create_storage_strategy",
        side_effect=RuntimeError("factory"),
    )
    mocker.patch(
        "scrapy_extension.pipeline.pipeline.logger.exception",
        side_effect=KeyboardInterrupt("diagnostic"),
    )
    with pytest.raises(RuntimeError, match="factory"):
        BackendPipeline.from_settings(Settings({"SCRAPY_BACKEND_TYPE": "redis"}))
    assert manager.close.call_count == 2


def test_scheduler_factory_rollback_survives_diagnostic_failure(mocker) -> None:
    lease = mocker.MagicMock()
    lease.manager = mocker.MagicMock()
    lease.release.side_effect = RuntimeError("release")
    mocker.patch.object(ConnectionManager, "acquire_lease", return_value=lease)
    mocker.patch.object(
        BackendScheduler,
        "_enforce_ack_concurrency_gate",
        side_effect=RuntimeError("factory"),
    )
    mocker.patch(
        "scrapy_extension.schedule.scheduler.logger.exception",
        side_effect=KeyboardInterrupt("diagnostic"),
    )
    with pytest.raises(RuntimeError, match="factory"):
        BackendScheduler.from_settings(Settings({"SCRAPY_BACKEND_TYPE": "redis"}))
    assert lease.release.call_count == 2


def test_pipeline_factory_adopts_exact_legacy_handoff_and_releases_it(
    mocker,
) -> None:
    settings = Settings(
        {
            "SCRAPY_BACKEND_TYPE": "redis",
            "SCRAPY_REDIS_HOST": "iteration8-pipeline-exact",
            "SCRAPY_STORAGE_STRATEGY": "invalid",
        }
    )
    acquire = mocker.patch.object(
        ConnectionManager,
        "get_manager",
        wraps=ConnectionManager.get_manager,
    )
    with pytest.raises(ConfigurationError):
        BackendPipeline.from_settings(settings)
    acquire.assert_called_once()
    ConnectionManager.clear_registry()


def test_scheduler_factory_releases_both_leases_when_constructor_fails(mocker) -> None:
    queue_lease = mocker.MagicMock()
    queue_lease.manager = mocker.MagicMock()
    queue_lease.release.side_effect = [RuntimeError("queue"), None]
    snapshot_lease = mocker.MagicMock()
    snapshot_lease.manager = mocker.MagicMock()
    snapshot_lease.release.side_effect = [RuntimeError("snapshot"), None]
    mocker.patch.object(
        ConnectionManager,
        "acquire_lease",
        side_effect=[queue_lease, snapshot_lease],
    )
    mocker.patch.object(
        BackendScheduler,
        "__init__",
        side_effect=RuntimeError("constructor"),
    )
    settings = Settings(
        {
            "SCRAPY_QUEUE_BACKEND_TYPE": "kafka",
            "SCRAPY_STORAGE_BACKEND_TYPE": "redis",
            "SCRAPY_QUEUE_STRATEGY": "delay",
        }
    )
    with pytest.raises(RuntimeError, match="constructor"):
        BackendScheduler.from_settings(settings)
    assert queue_lease.release.call_count == 2
    assert snapshot_lease.release.call_count == 2


def test_composite_close_consumes_late_authoritative_failure_after_timeout(
    mocker,
    monkeypatch,
) -> None:
    import scrapy_extension.spider.spider_mixin as mixin_module

    spider = _OwnershipSpider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)
    scheduler = mocker.MagicMock()
    public = Deferred()
    authoritative = Deferred()
    bounded = Deferred()
    scheduler.close.return_value = public
    scheduler._close_completion_deferred = authoritative
    scheduler._reactor_io_timeout = 1.0
    spider._scheduler = scheduler
    monkeypatch.setattr(mixin_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(
        mixin_module,
        "bounded_deferred",
        lambda *_args, _bounded=bounded, **_kwargs: _bounded,
    )

    result = spider.close_backend()
    failures: list[BaseException] = []
    result.addErrback(lambda failure: failures.append(failure.value) or None)
    bounded.errback(RuntimeError("public timeout"))
    authoritative.errback(RuntimeError("late authoritative"))

    assert failures and str(failures[0]) == "public timeout"
    assert spider._scheduler is scheduler
    assert spider._connection_manager.close.call_count == 0
    del public, authoritative, bounded
