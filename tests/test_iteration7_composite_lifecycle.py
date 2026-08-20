"""Iteration-seven composite lifecycle ownership regressions."""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from scrapy import Field, Item, Spider
from twisted.internet.defer import Deferred

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin


class _Item(Item):
    value = Field()


class _Spider(BackendSpiderMixin, Spider):
    name = "iteration-seven"
    backend_type = BackendType.REDIS


def _mixin_with_manager(mocker):
    spider = _Spider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)
    return spider


def test_invalidated_dupefilter_releases_lease_when_candidate_close_fails(mocker):
    spider = _mixin_with_manager(mocker)
    manager = spider._connection_manager
    lease = mocker.MagicMock(name="candidate-lease")
    candidate = MagicMock(name="candidate")
    candidate._connection_manager_lease = lease
    candidate.close.side_effect = RuntimeError("candidate close")

    def construct(*_args, **_kwargs):
        spider.close_backend()
        return candidate

    mocker.patch.object(BackendDupeFilter, "from_settings", side_effect=construct)

    with pytest.raises(RuntimeError, match="construction completed"):
        spider.get_dupefilter()

    candidate.close.assert_called_once_with("mixin-dupefilter-factory-failed")
    lease.release.assert_called_once_with()
    manager.close.assert_not_called()
    assert spider._orphan_candidates == [("dupefilter", candidate)]


def test_failed_candidate_lease_release_is_retryable_and_fences_parent_manager(
    mocker,
):
    spider = _mixin_with_manager(mocker)
    manager = spider._connection_manager
    lease = mocker.MagicMock(name="candidate-lease")
    lease.release.side_effect = [RuntimeError("lease release"), None]
    candidate = MagicMock(name="candidate")
    candidate._connection_manager_lease = lease
    candidate.close.side_effect = RuntimeError("candidate close")

    def construct(*_args, **_kwargs):
        spider.close_backend()
        return candidate

    mocker.patch.object(BackendDupeFilter, "from_settings", side_effect=construct)

    with pytest.raises(RuntimeError, match="construction completed"):
        spider.get_dupefilter()

    lease.release.assert_called_once_with()
    manager.close.assert_not_called()
    with pytest.raises(RuntimeError, match="candidate close"):
        spider.close_backend()
    assert lease.release.call_count == 2
    manager.close.assert_not_called()
    assert spider._orphan_candidates == [("dupefilter", candidate)]


def test_setup_rejects_incompatible_live_reconfiguration_before_acquire(mocker):
    manager = mocker.MagicMock(spec=ConnectionManager)
    acquire = mocker.patch.object(
        ConnectionManager, "get_manager", return_value=manager
    )
    spider = _Spider()
    spider.setup_backend()
    spider.backend_type = BackendType.MONGODB

    with pytest.raises(RuntimeError, match="cannot reconfigure"):
        spider.setup_backend()

    acquire.assert_called_once()
    spider.close_backend()


def test_setup_rejects_while_close_is_owned(mocker):
    spider = _Spider()
    spider._close_in_progress = True
    with pytest.raises(RuntimeError, match="close is already in progress"):
        spider.setup_backend()


def test_setup_retires_closed_manager_before_reacquiring(mocker):
    old = mocker.MagicMock(spec=ConnectionManager)
    old._retired = True
    fresh = mocker.MagicMock(spec=ConnectionManager)
    acquire = mocker.patch.object(ConnectionManager, "get_manager", return_value=fresh)
    spider = _Spider()
    spider._connection_manager = old
    spider.setup_backend()
    acquire.assert_called_once()
    spider.close_backend()


def test_setup_rejects_same_thread_recursive_attempt(mocker):
    from scrapy_extension.spider.spider_mixin import _BackendSetupAttempt

    spider = _Spider()
    spider._setup_attempt = _BackendSetupAttempt(1, threading.get_ident())
    with pytest.raises(RuntimeError, match="already in progress"):
        spider.setup_backend()
    spider._setup_attempt = None


def test_setup_invalidated_signal_registration_is_compensated(mocker):
    manager = mocker.MagicMock(spec=ConnectionManager)
    spider = _Spider()
    signal_manager = mocker.MagicMock()

    def wire():
        spider._connected_signals = signal_manager
        spider.close_backend()

    mocker.patch.object(spider, "_connect_signals", side_effect=wire)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    with pytest.raises(RuntimeError, match="completed after close"):
        spider.setup_backend()
    signal_manager.disconnect.assert_called()
    manager.close.assert_called_once_with()


def test_setup_callback_recursive_close_compensates_unpublished_manager(mocker):
    manager = mocker.MagicMock(spec=ConnectionManager)
    spider = _Spider()

    def close_during_monitor(_monitor):
        assert spider.close_backend() is None

    manager.set_monitor.side_effect = close_during_monitor
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)

    with pytest.raises(RuntimeError, match="completed after close"):
        spider.setup_backend()

    manager.close.assert_called_once_with()
    assert spider._connection_manager is None


def test_pipeline_reentrant_close_cannot_reopen_after_open_callback(mocker):
    manager = mocker.MagicMock()
    pipeline = BackendPipeline(manager)
    spider = SimpleNamespace(name="iteration-seven", crawler=None)

    def open_strategy():
        pipeline.close_spider(spider)

    strategy = mocker.MagicMock()
    strategy.open.side_effect = open_strategy
    pipeline.storage_strategy = strategy

    pipeline.open_spider(spider)

    assert pipeline._closed is True
    assert pipeline._opened is False
    manager.close.assert_called_once_with()


def test_pipeline_close_waits_for_admitted_store_before_manager_release(mocker):
    manager = mocker.MagicMock()
    pipeline = BackendPipeline(manager, max_storage_errors=None)
    pipeline._opened = True
    spider = SimpleNamespace(name="iteration-seven", crawler=None)
    entered = threading.Event()
    release = threading.Event()

    def store(_item, _spider):
        entered.set()
        assert release.wait(2)
        return _item

    pipeline._process_item_unlocked = store  # type: ignore[method-assign]
    worker = threading.Thread(
        target=pipeline._process_item_sync,
        args=(_Item(value=1), spider),
    )
    worker.start()
    assert entered.wait(2)

    closer = threading.Thread(target=pipeline.close_spider, args=(spider,))
    closer.start()
    time.sleep(0.02)
    assert closer.is_alive()
    manager.close.assert_not_called()

    release.set()
    worker.join(2)
    closer.join(2)
    assert not worker.is_alive()
    assert not closer.is_alive()
    manager.close.assert_called_once_with()


def test_stale_disconnect_is_unlocked_and_tolerates_recursive_connect(mocker):
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    stale = mocker.MagicMock(name="stale")
    replacement = mocker.MagicMock(name="replacement")
    stale.is_connected.return_value = False
    replacement.is_connected.return_value = True
    recursive = False

    def disconnect_stale():
        nonlocal recursive
        if not recursive:
            recursive = True
            manager.connect()

    stale.disconnect.side_effect = disconnect_stale
    manager._backend = stale
    mocker.patch.object(manager, "_create_backend", return_value=replacement)

    manager.connect()

    stale.disconnect.assert_called_once_with()
    assert manager._backend is replacement


def test_mixin_async_close_public_view_waits_for_siblings(mocker, monkeypatch):
    from scrapy_extension.spider import spider_mixin as mixin_module

    spider = _mixin_with_manager(mocker)
    manager = spider._connection_manager
    scheduler = mocker.MagicMock()
    scheduler._reactor_io_timeout = 1.0
    scheduler_close = Deferred()
    scheduler.close.return_value = scheduler_close
    queue = mocker.MagicMock()
    spider._scheduler = scheduler
    spider._queue = queue
    monkeypatch.setattr(mixin_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(mixin_module, "bounded_deferred", lambda source, **_: source)

    result = spider.close_backend()
    assert isinstance(result, Deferred)
    assert not result.called
    scheduler_close.callback(None)
    assert result.called
    queue.close.assert_called_once_with()
    manager.close.assert_called_once_with()


def test_mixin_async_close_surfaces_sibling_failure(mocker, monkeypatch):
    from scrapy_extension.spider import spider_mixin as mixin_module

    spider = _mixin_with_manager(mocker)
    scheduler = mocker.MagicMock()
    scheduler._reactor_io_timeout = 1.0
    scheduler_close = Deferred()
    scheduler.close.return_value = scheduler_close
    queue = mocker.MagicMock()
    queue.close.side_effect = RuntimeError("queue sibling")
    spider._scheduler = scheduler
    spider._queue = queue
    monkeypatch.setattr(mixin_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(mixin_module, "bounded_deferred", lambda source, **_: source)

    result = spider.close_backend()
    failures: list[BaseException] = []
    result.addErrback(lambda failure: failures.append(failure.value))
    scheduler_close.callback(None)
    assert failures and str(failures[0]) == "queue sibling"
    spider._connection_manager.close.assert_not_called()


def test_pipeline_rejects_new_store_after_close(mocker):
    manager = mocker.MagicMock()
    pipeline = BackendPipeline(manager)
    spider = SimpleNamespace(name="iteration-seven", crawler=None)
    pipeline.close_spider(spider)

    with pytest.raises(RuntimeError, match="closed"):
        pipeline._process_item_sync(_Item(value=1), spider)


def test_pipeline_open_observer_fences_late_failure_and_clears_token():
    from twisted.python.failure import Failure

    pipeline = BackendPipeline(MagicMock())
    pipeline._opening = True
    pipeline._opening_generation = 4
    pipeline._opening_operation = Deferred()
    result = pipeline._finish_async_open(Failure(RuntimeError("late open")))

    assert isinstance(result, Failure)
    assert pipeline._opening is False
    assert pipeline._opening_generation is None
    assert pipeline._opening_operation is None


def test_pipeline_open_observer_clears_completed_operation():
    pipeline = BackendPipeline(MagicMock())
    pipeline._opening_operation = Deferred()
    pipeline._finish_async_open(None)
    assert pipeline._opening_operation is None


@pytest.mark.parametrize("state", ["closed", "closing", "stale-generation"])
def test_pipeline_open_admission_rejects_stale_lifecycle_states(state):
    pipeline = BackendPipeline(MagicMock())
    spider = SimpleNamespace(name="iteration-seven", crawler=None)
    if state == "closed":
        pipeline._closed = True
    elif state == "closing":
        pipeline._closing = True
    else:
        pipeline._opening_generation = 9

    with pytest.raises(RuntimeError):
        pipeline._open_spider_sync(spider, 8 if state == "stale-generation" else None)


def test_pipeline_async_close_returns_idempotent_result_when_already_closed(
    monkeypatch,
):
    monkeypatch.setattr(
        "scrapy_extension.pipeline.pipeline.reactor_is_running",
        lambda: True,
    )
    pipeline = BackendPipeline(MagicMock())
    pipeline._closed = True
    result = pipeline.close_spider(SimpleNamespace(name="closed", crawler=None))
    assert isinstance(result, Deferred)
    assert result.called


def test_pipeline_store_exit_resumes_deferred_reentrant_close(mocker):
    pipeline = BackendPipeline(mocker.MagicMock())
    thread_id = threading.get_ident()
    pipeline._closing = True
    pipeline._close_waiting_for_stores = True
    pipeline._close_owner_thread_id = thread_id
    pipeline._active_store_count = 1
    pipeline._active_store_threads[thread_id] = 1
    pipeline._leave_store(thread_id)
    assert pipeline._closed


def test_mixin_candidate_lease_capture_deduplicates_lease(mocker):
    spider = _mixin_with_manager(mocker)
    lease = MagicMock()
    candidate = MagicMock()
    candidate._connection_manager_lease = lease
    spider._capture_candidate_leases(candidate)
    candidate._connection_manager_lease = lease
    spider._capture_candidate_leases(candidate)
    assert spider._orphan_leases == [lease]


def test_mixin_candidate_lease_capture_handles_slots_and_assignment_failure(mocker):
    spider = _mixin_with_manager(mocker)

    class SlotsOnly:
        __slots__ = ()

    assert spider._capture_candidate_leases(SlotsOnly()) is False
    lease = MagicMock()

    class AssignmentFailure:
        def __init__(self) -> None:
            object.__setattr__(self, "_connection_manager_lease", lease)

        def __setattr__(self, name: str, value: object) -> None:
            if name == "_connection_manager_lease":
                raise RuntimeError("set")
            object.__setattr__(self, name, value)

    candidate = AssignmentFailure()
    assert spider._capture_candidate_leases(candidate) is True
    lease.release.assert_not_called()
    spider._release_orphan_leases()
    lease.release.assert_called_once_with()


def test_mixin_orphan_cleanup_retries_queue_and_scheduler_candidates(mocker):
    spider = _mixin_with_manager(mocker)
    queue = MagicMock()
    scheduler = MagicMock()
    spider._orphan_candidates = [("queue", queue), ("scheduler", scheduler)]
    spider._cleanup_orphan_candidates("retry")
    queue.close.assert_called_once_with()
    scheduler.close.assert_called_once_with("retry")
    assert spider._orphan_candidates == []


def test_mixin_orphan_cleanup_retains_failed_candidate_and_lease(mocker):
    spider = _mixin_with_manager(mocker)
    candidate = MagicMock()
    candidate.close.side_effect = KeyboardInterrupt("candidate")
    lease = MagicMock()
    lease.release.side_effect = KeyboardInterrupt("lease")
    spider._orphan_candidates = [("scheduler", candidate)]
    spider._orphan_leases = [lease]

    error = spider._cleanup_orphan_candidates("retry")

    assert isinstance(error, KeyboardInterrupt)
    assert spider._orphan_candidates == [("scheduler", candidate)]
    assert spider._orphan_leases == [lease]


def test_connection_manager_stale_probe_without_backend_is_a_noop():
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    assert manager._detach_stale_backend() == (None, None)


def test_connection_manager_stale_probe_rejects_retired_manager():
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    manager._retired = True
    with pytest.raises(BackendConnectionError):
        manager._detach_stale_backend()


def test_connection_manager_stale_probe_reconciles_replacement(mocker):
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    old = MagicMock()
    replacement = MagicMock()
    old.is_connected.side_effect = lambda: (
        setattr(manager, "_backend", replacement) or False
    )
    manager._backend = old

    stale, generation = manager._detach_stale_backend()

    assert stale is None
    assert generation is None
    assert manager._backend is replacement


def test_connection_manager_stale_probe_failure_is_retried(mocker):
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    old = MagicMock()
    old.is_connected.side_effect = RuntimeError("probe")
    replacement = MagicMock()
    replacement.is_connected.return_value = True
    manager._backend = old
    mocker.patch.object(manager, "_create_backend", return_value=replacement)

    manager.connect()

    old.disconnect.assert_called_once_with()
    assert manager._backend is replacement


def test_pipeline_factory_preserves_other_strategy_configuration_error(mocker):
    from scrapy.settings import Settings

    from scrapy_extension.exceptions import ConfigurationError

    manager = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    mocker.patch(
        "scrapy_extension.pipeline.pipeline.create_storage_strategy",
        side_effect=ConfigurationError("other", setting_name="other"),
    )
    with pytest.raises(ConfigurationError, match="other"):
        BackendPipeline.from_settings(Settings({"SCRAPY_BACKEND_TYPE": "redis"}))
    manager.close.assert_called_once_with()


def test_pipeline_factory_attributes_max_pending_error(mocker):
    from scrapy.settings import Settings

    from scrapy_extension.exceptions import ConfigurationError

    manager = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    settings = Settings(
        {
            "SCRAPY_BACKEND_TYPE": "redis",
            "SCRAPY_STORAGE_STRATEGY": "batched",
            "SCRAPY_STORAGE_BUFFER_MAX_PENDING": 1,
        }
    )
    with pytest.raises(ConfigurationError) as error:
        BackendPipeline.from_settings(settings)
    assert error.value.setting_name == "SCRAPY_STORAGE_BUFFER_MAX_PENDING"
    manager.close.assert_called_once_with()


def test_pipeline_factory_rejects_non_string_prefix(mocker):
    from scrapy.settings import Settings

    from scrapy_extension.exceptions import ConfigurationError

    manager = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    settings = Settings(
        {
            "SCRAPY_BACKEND_TYPE": "redis",
            "SCRAPY_PIPELINE_KEY_PREFIX": 7,
        }
    )
    with pytest.raises(ConfigurationError, match="must be a string"):
        BackendPipeline.from_settings(settings)
    manager.close.assert_called_once_with()


def test_pipeline_open_async_assignment_can_lose_its_generation(monkeypatch):
    pipeline = BackendPipeline(MagicMock())
    spider = SimpleNamespace(name="iteration-seven", crawler=None)
    monkeypatch.setattr(
        "scrapy_extension.pipeline.pipeline.reactor_is_running", lambda: True
    )

    def thread(_function, *_args, **_kwargs):
        pipeline._opening_generation = 99
        return Deferred()

    monkeypatch.setattr("scrapy_extension.pipeline.pipeline.deferToThread", thread)
    pipeline._open_spider_async(spider)
    assert pipeline._opening_operation is None


def test_pipeline_open_failure_observer_after_token_was_cleared():
    from twisted.python.failure import Failure

    pipeline = BackendPipeline(MagicMock())
    pipeline._opening = False
    pipeline._opening_operation = Deferred()
    pipeline._finish_async_open(Failure(RuntimeError("late")))
    assert pipeline._opening_operation is not None


def test_pipeline_open_generation_replaced_during_callback(mocker):
    pipeline = BackendPipeline(mocker.MagicMock())
    spider = SimpleNamespace(name="iteration-seven", crawler=None)

    def replace_generation():
        pipeline._opening_generation = 99

    pipeline.storage_strategy.open = replace_generation
    pipeline._open_spider_sync(spider)
    assert pipeline._closed


def test_pipeline_open_failure_clears_matching_generation(mocker):
    pipeline = BackendPipeline(mocker.MagicMock())
    spider = SimpleNamespace(name="iteration-seven", crawler=None)
    pipeline._opening_generation = 8
    pipeline.storage_strategy = mocker.MagicMock()
    pipeline.storage_strategy.open.side_effect = RuntimeError("open")
    with pytest.raises(RuntimeError, match="open"):
        pipeline._open_spider_sync(spider, 8)
    assert pipeline._closed


def test_pipeline_open_failure_after_generation_replacement(mocker):
    pipeline = BackendPipeline(mocker.MagicMock())
    spider = SimpleNamespace(name="iteration-seven", crawler=None)

    def fail_open():
        pipeline._opening_generation = 99
        raise RuntimeError("open")

    pipeline.storage_strategy.open = fail_open
    with pytest.raises(RuntimeError, match="open"):
        pipeline._open_spider_sync(spider)
    assert pipeline._closed


def test_pipeline_close_waits_for_another_open_owner(mocker):
    pipeline = BackendPipeline(mocker.MagicMock())
    pipeline._opening = True
    pipeline._opening_owner_thread_id = threading.get_ident() + 1
    done = threading.Event()

    def close():
        pipeline._close_spider_sync(SimpleNamespace(name="x", crawler=None))
        done.set()

    worker = threading.Thread(target=close)
    worker.start()
    time.sleep(0.02)
    with pipeline._lifecycle_lock:
        pipeline._opening = False
        pipeline._store_condition.notify_all()
    worker.join(2)
    assert done.is_set()


def test_pipeline_close_locked_allows_reserved_owner_after_store_wait(mocker):
    pipeline = BackendPipeline(mocker.MagicMock())
    pipeline._closing = True
    pipeline._close_owner_thread_id = threading.get_ident()
    pipeline._close_waiting_for_stores = True
    pipeline._close_locked()
    assert pipeline._closed


def test_pipeline_leave_store_keeps_nested_thread_admission(mocker):
    pipeline = BackendPipeline(mocker.MagicMock())
    thread_id = threading.get_ident()
    pipeline._active_store_count = 2
    pipeline._active_store_threads[thread_id] = 2
    pipeline._leave_store(thread_id)
    assert pipeline._active_store_threads[thread_id] == 1


def test_mixin_dispose_invalidated_queue_candidate(mocker):
    spider = _mixin_with_manager(mocker)
    candidate = MagicMock()
    spider._dispose_invalidated_candidate("queue", candidate, "retry")
    candidate.close.assert_called_once_with()


def test_mixin_orphan_cleanup_collects_all_secondary_failures(mocker):
    spider = _mixin_with_manager(mocker)
    first = MagicMock()
    second = MagicMock()
    first.close.side_effect = RuntimeError("first")
    second.close.side_effect = RuntimeError("second")
    spider._orphan_candidates = [("scheduler", first), ("scheduler", second)]
    error = spider._cleanup_orphan_candidates("retry")
    assert str(error) == "first"


def test_mixin_release_orphan_leases_attempts_all_failures(mocker):
    spider = _mixin_with_manager(mocker)
    first = MagicMock()
    second = MagicMock()
    first.release.side_effect = RuntimeError("first")
    second.release.side_effect = RuntimeError("second")
    spider._orphan_leases = [first, second]
    error = spider._release_orphan_leases()
    assert str(error) == "first"


def test_mixin_orphan_cleanup_reports_first_lease_failure(mocker):
    spider = _mixin_with_manager(mocker)
    lease = MagicMock()
    lease.release.side_effect = RuntimeError("lease")
    spider._orphan_leases = [lease]
    error = spider._cleanup_orphan_candidates("retry")
    assert str(error) == "lease"


def test_mixin_orphan_cleanup_collects_lease_after_candidate_failure(mocker):
    spider = _mixin_with_manager(mocker)
    candidate = MagicMock()
    candidate.close.side_effect = RuntimeError("candidate")
    first = MagicMock()
    second = MagicMock()
    first.release.side_effect = RuntimeError("first lease")
    second.release.side_effect = RuntimeError("second lease")
    spider._orphan_candidates = [("scheduler", candidate)]
    spider._orphan_leases = [first, second]
    error = spider._cleanup_orphan_candidates("retry")
    assert str(error) == "candidate"


def test_setup_normalizes_backend_object_with_value(mocker):
    manager = mocker.MagicMock(spec=ConnectionManager)
    acquire = mocker.patch.object(
        ConnectionManager, "get_manager", return_value=manager
    )
    spider = _Spider()
    spider.backend_type = SimpleNamespace(value="redis")
    spider.setup_backend()
    acquire.assert_called_once()
    spider.close_backend()


def test_setup_invalidated_before_publication_does_not_publish_manager(mocker):
    manager = mocker.MagicMock(spec=ConnectionManager)
    spider = _Spider()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    manager.set_monitor.side_effect = lambda _monitor: spider.close_backend()
    with pytest.raises(RuntimeError, match="completed after close"):
        spider.setup_backend()
    assert spider._connection_manager is None


def test_pipeline_reentrant_store_close_drains_after_store_returns(mocker):
    manager = mocker.MagicMock()
    pipeline = BackendPipeline(manager, max_storage_errors=None)
    pipeline._opened = True
    spider = SimpleNamespace(name="iteration-seven", crawler=None)
    calls: list[object] = []

    def store(item, _spider):
        calls.append(pipeline.close_spider(spider))
        return item

    pipeline._process_item_unlocked = store  # type: ignore[method-assign]
    assert pipeline._process_item_sync(_Item(value=2), spider).get("value") == 2
    assert calls == [None]
    assert pipeline._closed
    manager.close.assert_called_once_with()
