"""Iteration-nine construction rollback and exact lifecycle ownership tests."""

from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
from pydispatch.errors import DispatcherKeyError
from scrapy import Spider
from scrapy.settings import Settings
from twisted.internet.defer import Deferred

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.dupefilter.dupefilter import (
    BackendDupeFilter,
    _cleanup_factory_filter_and_manager,
)
from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.exceptions import QueueError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.queue.queue import (
    _STRATEGY_CLEANUP_FAILED,
    _STRATEGY_CLEANUP_SUCCEEDED,
    BackendQueue,
)
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin


class _IterationNineSpider(BackendSpiderMixin, Spider):
    name = "iteration-nine"
    backend_type = BackendType.REDIS


class _SignalBus:
    def __init__(self, *, connect_effect_then_raise: bool = False) -> None:
        self.registrations: set[tuple[int, int]] = set()
        self.connect_calls = 0
        self.disconnect_failures = 0
        self.connect_effect_then_raise = connect_effect_then_raise

    def connect(self, handler: object, signal: object) -> None:
        self.connect_calls += 1
        self.registrations.add((id(handler), id(signal)))
        if self.connect_calls == 2:
            raise RuntimeError("registration failed")

    def disconnect(self, handler: object, signal: object) -> None:
        key = (id(handler), id(signal))
        if key not in self.registrations:
            raise DispatcherKeyError("already absent")
        if self.disconnect_failures:
            self.disconnect_failures -= 1
            raise RuntimeError("rollback disconnect failed")
        self.registrations.remove(key)


class _ClosableFilter(MembershipFilter):
    def add(self, item: bytes) -> bool:
        del item
        return True

    def __contains__(self, item: bytes) -> bool:
        del item
        return False

    def __len__(self) -> int:
        return 0

    def clear(self) -> None:
        return None


def test_spider_signal_rollback_retains_exact_leases_until_close_retry() -> None:
    bus = _SignalBus()
    # The internal rollback and the setup-level retry each fail both handlers.
    bus.disconnect_failures = 4
    spider = _IterationNineSpider()
    spider.crawler = MagicMock(signals=bus, settings=Settings(), stats=None)

    with pytest.raises(RuntimeError, match="registration failed"):
        spider.setup_backend()

    manager = spider._connection_manager
    assert manager is not None
    assert len(spider._signal_leases) == 2
    assert bus.registrations
    assert manager._users == 1

    spider.close_backend()

    assert spider._signal_leases == []
    assert bus.registrations == set()
    assert manager._users == 0


def test_dupefilter_constructor_abort_closes_filter_before_real_manager_release(
    mocker,
) -> None:
    membership_filter = _ClosableFilter()
    membership_filter.close = mocker.Mock(  # type: ignore[method-assign]
        side_effect=[RuntimeError("filter close"), RuntimeError("filter close")]
    )
    factory_error = RuntimeError("constructor failed")
    mocker.patch(
        "scrapy_extension.dupefilter.filters.factory.build_membership_filter",
        return_value=membership_filter,
    )
    mocker.patch.object(BackendDupeFilter, "__init__", side_effect=factory_error)

    with pytest.raises(RuntimeError, match="constructor failed"):
        BackendDupeFilter.from_settings(
            Settings(
                {
                    "SCRAPY_BACKEND_TYPE": "redis",
                    "SCRAPY_BACKEND_SETTINGS": {"host": "iteration-nine-dupe"},
                }
            )
        )

    assert membership_filter.close.call_count == 2  # type: ignore[attr-defined]
    assert ConnectionManager._managers == {}


def test_dupefilter_lossy_abort_releases_legacy_manager_without_lease(mocker) -> None:
    manager = MagicMock()
    membership_filter = _ClosableFilter()

    assert (
        _cleanup_factory_filter_and_manager(
            membership_filter,
            manager,
            None,
            owns_manager=True,
        )
        is None
    )
    manager.close.assert_called_once_with()


def test_dupefilter_lossy_abort_preserves_filter_error_over_manager_error(
    mocker,
) -> None:
    manager = MagicMock()
    manager.close.side_effect = [RuntimeError("manager close"), None]
    membership_filter = _ClosableFilter()
    membership_filter.close = mocker.Mock(  # type: ignore[method-assign]
        side_effect=[RuntimeError("filter close"), RuntimeError("filter close")]
    )

    error = _cleanup_factory_filter_and_manager(
        membership_filter,
        manager,
        None,
        owns_manager=True,
    )

    assert isinstance(error, RuntimeError)
    assert str(error) == "filter close"
    assert manager.close.call_count == 2


def test_dupefilter_crawler_wiring_abort_releases_real_lease_after_filter_retry(
    mocker,
) -> None:
    lease = ConnectionManager.acquire_lease(
        BackendType.REDIS,
        {"host": "iteration-nine-crawler"},
    )
    membership_filter = _ClosableFilter()
    membership_filter.close = mocker.Mock(  # type: ignore[method-assign]
        side_effect=[RuntimeError("filter close"), None]
    )
    candidate = BackendDupeFilter(
        lease.manager,
        membership_filter=membership_filter,
        connection_manager_lease=lease,
    )
    crawler = SimpleNamespace(
        settings=Settings(),
        stats=object(),
        request_fingerprinter=None,
    )
    mocker.patch.object(BackendDupeFilter, "from_settings", return_value=candidate)
    mocker.patch(
        "scrapy_extension.dupefilter.dupefilter.ScrapyStatsMonitor",
        side_effect=RuntimeError("monitor wiring failed"),
    )

    with pytest.raises(RuntimeError, match="monitor wiring failed"):
        BackendDupeFilter.from_crawler(crawler)

    assert membership_filter.close.call_count == 2  # type: ignore[attr-defined]
    assert lease.released
    assert ConnectionManager._managers == {}


def test_scheduler_factory_uses_lossy_abort_after_checkpoint_failure(mocker) -> None:
    queue = MagicMock()
    queue.close.side_effect = lambda **kwargs: (
        (_ for _ in ()).throw(QueueError("checkpoint failed"))
        if not kwargs.get("lossy")
        else None
    )
    settings = Settings(
        {
            "SCRAPY_QUEUE_BACKEND_TYPE": "kafka",
            "SCRAPY_STORAGE_BACKEND_TYPE": "redis",
            "SCRAPY_QUEUE_STRATEGY": "delay",
            "DUPEFILTER_CLASS": "example.BrokenDupeFilter",
        }
    )
    real_factory = BackendScheduler.from_settings

    def build_scheduler(
        factory_settings: Settings, **kwargs: object
    ) -> BackendScheduler:
        scheduler = real_factory(factory_settings, **kwargs)
        scheduler._queue = queue
        return scheduler

    mocker.patch.object(BackendScheduler, "from_settings", side_effect=build_scheduler)
    mocker.patch(
        "scrapy_extension.schedule.scheduler.load_object",
        side_effect=RuntimeError("crawler factory failed"),
    )
    crawler = MagicMock(settings=settings)

    with pytest.raises(RuntimeError, match="crawler factory failed"):
        BackendScheduler.from_crawler(crawler)

    assert queue.close.call_args_list == [call(), call(lossy=True)]
    assert ConnectionManager._managers == {}


def test_scheduler_factory_rollback_failure_keeps_primary_error(mocker) -> None:
    scheduler = BackendScheduler(MagicMock(), owns_connection_manager=False)
    primary = RuntimeError("crawler failure")
    mocker.patch.object(BackendScheduler, "from_settings", return_value=scheduler)
    mocker.patch(
        "scrapy_extension.schedule.scheduler.load_object",
        side_effect=primary,
    )
    scheduler._rollback_factory_failure = mocker.Mock(  # type: ignore[method-assign]
        side_effect=KeyboardInterrupt("rollback interrupted")
    )

    with pytest.raises(RuntimeError, match="crawler failure"):
        BackendScheduler.from_crawler(
            MagicMock(settings=Settings({"SCRAPY_BACKEND_TYPE": "redis"}))
        )


def test_scheduler_force_factory_release_attempts_snapshot_and_queue(mocker) -> None:
    queue_manager = MagicMock()
    snapshot_manager = MagicMock()
    queue_manager.close.side_effect = [
        RuntimeError("queue release"),
        None,
        None,
        None,
    ]
    snapshot_manager.close.side_effect = [
        RuntimeError("snapshot release"),
        None,
        None,
        None,
    ]
    scheduler = BackendScheduler(
        queue_manager,
        snapshot_connection_manager=snapshot_manager,
        owns_snapshot_connection_manager=True,
    )

    with pytest.raises(RuntimeError, match="snapshot release"):
        scheduler._force_factory_manager_release()

    assert snapshot_manager.close.call_count == 2
    assert queue_manager.close.call_count == 2
    scheduler._force_factory_manager_release()
    assert scheduler._snapshot_manager_released is True
    assert scheduler._manager_released is True


def test_scheduler_force_factory_release_retries_exact_queue_lease(mocker) -> None:
    manager = MagicMock()
    lease = MagicMock()
    lease.manager = manager
    lease.release.side_effect = [RuntimeError("lease release"), None]
    scheduler = BackendScheduler(
        manager,
        connection_manager_lease=lease,
    )

    with pytest.raises(RuntimeError, match="lease release"):
        scheduler._force_factory_manager_release()

    assert lease.release.call_count == 2


def test_pipeline_close_releases_its_exact_manager_lease(mocker) -> None:
    manager = mocker.MagicMock(name="pipeline-manager")
    lease = mocker.MagicMock(name="pipeline-lease", manager=manager)
    pipeline = BackendPipeline(manager, connection_manager_lease=lease)

    pipeline.close_spider()

    lease.release.assert_called_once_with()
    manager.close.assert_not_called()
    assert pipeline._manager_released is True


@pytest.mark.parametrize(
    ("cleanup_state", "raises"),
    [
        (_STRATEGY_CLEANUP_SUCCEEDED, False),
        (_STRATEGY_CLEANUP_FAILED, True),
    ],
)
def test_queue_reclaims_stale_close_owner_without_replaying_cleanup(
    cleanup_state: str,
    raises: bool,
) -> None:
    manager = MagicMock(name="queue-manager")
    manager.get_storage_backend.side_effect = NotImplementedError
    queue = BackendQueue(manager, "iteration-nine-stale", queue_strategy=MagicMock())
    queue._close_in_progress = True
    queue._close_owner_token = SimpleNamespace(active=False, thread_id=-1)
    queue._strategy_cleanup_state = cleanup_state

    if raises:
        with pytest.raises(QueueError, match="close is terminal"):
            queue.close()
    else:
        queue.close()

    assert queue._close_complete is True
    queue._strategy.close.assert_not_called()


def test_queue_reentrant_close_is_bounded_to_the_outer_owner() -> None:
    manager = MagicMock(name="queue-manager")
    manager.get_storage_backend.side_effect = NotImplementedError
    queue = BackendQueue(
        manager, "iteration-nine-reentrant", queue_strategy=MagicMock()
    )
    queue._close_in_progress = True
    queue._close_owner_token = SimpleNamespace(
        active=True,
        thread_id=threading.get_ident(),
    )

    with pytest.raises(QueueError, match="already in progress"):
        queue.close()

    queue._strategy.close.assert_not_called()


def test_scheduler_factory_cleanup_none_is_observed(mocker) -> None:
    scheduler = BackendScheduler(MagicMock(), owns_connection_manager=False)
    scheduler._observe_factory_cleanup(
        None,
        on_failure=mocker.Mock(),
    )


def test_scheduler_flattens_public_and_authoritative_factory_cleanup(mocker) -> None:
    scheduler = BackendScheduler(MagicMock(), owns_connection_manager=False)
    public_close: Deferred[None] = Deferred()
    authoritative_close: Deferred[None] = Deferred()
    public_abort: Deferred[None] = Deferred()
    authoritative_abort: Deferred[None] = Deferred()
    scheduler.close = mocker.MagicMock(return_value=public_close)  # type: ignore[method-assign]
    scheduler._close_completion_deferred = authoritative_close

    def abort() -> Deferred[None]:
        scheduler._close_completion_deferred = authoritative_abort
        return public_abort

    scheduler.abort = mocker.MagicMock(side_effect=abort)  # type: ignore[method-assign]
    scheduler._rollback_factory_failure()
    public_close.errback(RuntimeError("bounded close"))
    authoritative_close.errback(RuntimeError("authoritative close"))
    assert scheduler.abort.call_count == 1
    public_abort.addErrback(lambda _failure: None)
    authoritative_abort.addErrback(lambda _failure: None)
    public_abort.errback(RuntimeError("bounded abort"))
    authoritative_abort.errback(RuntimeError("authoritative abort"))


def test_scheduler_factory_observes_async_abort_failure_without_unhandled_branch(
    mocker,
) -> None:
    scheduler = BackendScheduler(MagicMock(), owns_connection_manager=False)
    close_result: Deferred[None] = Deferred()
    abort_result: Deferred[None] = Deferred()
    scheduler.close = mocker.MagicMock(return_value=close_result)  # type: ignore[method-assign]
    scheduler.abort = mocker.MagicMock(return_value=abort_result)  # type: ignore[method-assign]

    scheduler._rollback_factory_failure()
    close_result.errback(RuntimeError("normal close failed"))
    assert scheduler.abort.call_count == 1

    # The factory has no caller to observe this Deferred. The rollback observer
    # consumes the failure and does not leave Twisted's unhandled-failure hook armed.
    abort_result.errback(RuntimeError("lossy abort failed"))
    assert abort_result.called
