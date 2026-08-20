"""Focused lifecycle ownership and factory-parity contracts for the spider mixin."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest
from scrapy import Spider
from scrapy.settings import Settings
from twisted.internet.defer import Deferred

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import QueueError
from scrapy_extension.spider.spider_mixin import (
    BackendSpiderMixin,
    _ComponentConstruction,
)
from scrapy_extension.utils.reactor import DEFAULT_REACTOR_IO_TIMEOUT_S


class _Spider(BackendSpiderMixin, Spider):
    name = "mixin-reliability"
    backend_type = BackendType.REDIS


def _spider_with_manager(mocker):
    spider = _Spider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)
    return spider


def test_spider_closed_signal_returns_async_close_deferred(mocker) -> None:
    spider = _Spider()
    close_result: Deferred[None] = Deferred()
    spider.close_backend = mocker.MagicMock(return_value=close_result)  # type: ignore[method-assign]

    returned = spider._on_spider_closed(spider, reason="finished")

    assert returned is close_result
    assert not returned.called
    close_result.callback(None)
    assert returned.called


def test_spider_closed_signal_preserves_async_control_failure(mocker) -> None:
    spider = _Spider()
    close_result: Deferred[None] = Deferred()
    spider.close_backend = mocker.MagicMock(return_value=close_result)  # type: ignore[method-assign]

    returned = spider._on_spider_closed(spider, reason="interrupted")
    failures: list[BaseException] = []
    assert returned is close_result
    returned.addErrback(lambda failure: failures.append(failure.value))
    close_result.errback(KeyboardInterrupt("close interrupted"))

    assert failures and isinstance(failures[0], KeyboardInterrupt)


def test_spider_closed_signal_swallows_async_close_failure(mocker) -> None:
    spider = _Spider()
    close_result: Deferred[None] = Deferred()
    spider.close_backend = mocker.MagicMock(return_value=close_result)  # type: ignore[method-assign]

    returned = spider._on_spider_closed(spider, reason="failed")
    assert returned is close_result
    close_result.errback(RuntimeError("async close failed"))
    assert returned.called


def test_close_backend_in_progress_from_other_thread_returns_existing_deferred(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    pending: Deferred[None] = Deferred()
    spider._close_in_progress = True
    spider._close_owner_thread_id = threading.get_ident() + 1
    spider._close_deferred = pending

    assert spider.close_backend() is pending


def test_close_backend_async_scheduler_control_failure_is_preserved(mocker) -> None:
    spider = _spider_with_manager(mocker)
    scheduler = mocker.MagicMock(name="scheduler")
    scheduler_close: Deferred[None] = Deferred()
    scheduler.close.return_value = scheduler_close
    spider._scheduler = scheduler

    returned = spider.close_backend()
    failures: list[BaseException] = []
    assert isinstance(returned, Deferred)
    returned.addErrback(lambda failure: failures.append(failure.value))
    scheduler_close.errback(KeyboardInterrupt("scheduler interrupted"))

    assert failures and isinstance(failures[0], KeyboardInterrupt)
    assert spider._scheduler is scheduler
    assert spider._connection_manager is not None


def test_close_backend_async_scheduler_does_not_clear_replacement(mocker) -> None:
    spider = _spider_with_manager(mocker)
    scheduler = mocker.MagicMock(name="scheduler")
    replacement = mocker.MagicMock(name="replacement")
    scheduler_close: Deferred[None] = Deferred()

    def close_scheduler(_reason: str) -> Deferred[None]:
        scheduler.close.return_value = scheduler_close
        spider._scheduler = replacement
        return scheduler_close

    scheduler.close.side_effect = close_scheduler
    spider._scheduler = scheduler

    returned = spider.close_backend()
    assert isinstance(returned, Deferred)
    scheduler_close.callback(None)

    assert spider._scheduler is replacement
    replacement.close = MagicMock()
    spider.close_backend()


def test_close_backend_async_success_preserves_existing_signal_error(mocker) -> None:
    spider = _spider_with_manager(mocker)
    signal_manager = mocker.MagicMock(name="signals")
    signal_manager.disconnect.side_effect = RuntimeError("signal failed")
    spider._connected_signals = signal_manager
    spider._signals_connected = True
    scheduler = mocker.MagicMock(name="scheduler")
    scheduler_close: Deferred[None] = Deferred()
    scheduler.close.return_value = scheduler_close
    spider._scheduler = scheduler

    returned = spider.close_backend()
    failures: list[BaseException] = []
    assert isinstance(returned, Deferred)
    returned.addErrback(lambda failure: failures.append(failure.value))
    scheduler_close.callback(None)

    assert failures and str(failures[0]) == "signal failed"
    assert spider._scheduler is None
    assert spider._connection_manager is not None


def test_close_backend_async_failure_preserves_existing_signal_error(mocker) -> None:
    spider = _spider_with_manager(mocker)
    signal_manager = mocker.MagicMock(name="signals")
    signal_manager.disconnect.side_effect = RuntimeError("signal failed")
    spider._connected_signals = signal_manager
    spider._signals_connected = True
    scheduler = mocker.MagicMock(name="scheduler")
    scheduler_close: Deferred[None] = Deferred()
    scheduler.close.return_value = scheduler_close
    spider._scheduler = scheduler

    returned = spider.close_backend()
    failures: list[BaseException] = []
    assert isinstance(returned, Deferred)
    returned.addErrback(lambda failure: failures.append(failure.value))
    scheduler_close.errback(RuntimeError("scheduler failed"))

    assert failures and str(failures[0]) == "signal failed"
    assert spider._scheduler is scheduler
    assert spider._connection_manager is not None


def test_close_backend_waits_for_async_scheduler_before_releasing_components(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    scheduler = mocker.MagicMock(name="scheduler")
    scheduler.dupefilter = spider._dupefilter
    scheduler_close: Deferred[None] = Deferred()
    scheduler.close.return_value = scheduler_close
    queue = mocker.MagicMock(name="queue")
    spider._scheduler = scheduler
    spider._queue = queue

    result = spider.close_backend()

    assert result is scheduler_close
    assert spider._scheduler is scheduler
    assert spider._queue is queue
    assert manager is not None
    manager.close.assert_not_called()
    queue.close.assert_not_called()

    scheduler_close.callback(None)
    assert scheduler.close.call_count == 1
    assert queue.close.call_count == 1
    manager.close.assert_called_once_with()
    assert spider._scheduler is None
    assert spider._queue is None
    assert spider._connection_manager is None


def test_close_backend_async_scheduler_failure_retains_retry_ownership(mocker) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    scheduler = mocker.MagicMock(name="scheduler")
    first: Deferred[None] = Deferred()
    second: Deferred[None] = Deferred()
    scheduler.close.side_effect = [first, second]
    queue = mocker.MagicMock(name="queue")
    spider._scheduler = scheduler
    spider._queue = queue

    result = spider.close_backend()
    failures: list[BaseException] = []
    assert isinstance(result, Deferred)
    result.addErrback(lambda failure: failures.append(failure.value))
    first.errback(RuntimeError("scheduler close failed"))

    assert failures and str(failures[0]) == "scheduler close failed"
    assert spider._scheduler is scheduler
    assert manager is not None
    manager.close.assert_not_called()

    retry = spider.close_backend()
    assert retry is second
    second.callback(None)
    assert spider._scheduler is None
    assert manager.close.call_count == 1


def test_close_backend_async_scheduler_timeout_keeps_late_completion_authoritative(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    scheduler = mocker.MagicMock(name="scheduler")
    public: Deferred[None] = Deferred()
    authoritative: Deferred[None] = Deferred()
    scheduler.close.return_value = public
    scheduler._close_completion_deferred = authoritative
    queue = mocker.MagicMock(name="queue")
    spider._scheduler = scheduler
    spider._queue = queue

    result = spider.close_backend()
    assert result is public
    public.addErrback(lambda failure: None)
    public.errback(RuntimeError("bounded close timeout"))
    assert spider._scheduler is scheduler
    assert spider._queue is queue
    assert manager is not None
    manager.close.assert_not_called()

    authoritative.callback(None)
    assert queue.close.call_count == 1
    manager.close.assert_called_once_with()
    assert spider._scheduler is None


def test_close_rejects_an_unrelated_concurrent_attempt(mocker) -> None:
    spider = _spider_with_manager(mocker)
    spider._close_in_progress = True
    spider._close_owner_thread_id = threading.get_ident() + 1

    with pytest.raises(RuntimeError, match="already in progress"):
        spider.close_backend()


def test_close_retains_failed_component_and_manager_for_retry(mocker) -> None:
    spider = _spider_with_manager(mocker)
    queue = mocker.MagicMock(name="queue")
    manager = spider._connection_manager
    queue.close.side_effect = [QueueError("temporary queue failure"), None]
    spider._queue = queue

    with pytest.raises(QueueError, match="temporary queue failure"):
        spider.close_backend()

    assert spider._queue is queue
    assert spider._connection_manager is manager
    manager.close.assert_not_called()

    spider.close_backend()

    queue.close.assert_has_calls([mocker.call(), mocker.call()])
    manager.close.assert_called_once_with()
    assert spider._queue is None
    assert spider._connection_manager is None


def test_close_retains_failed_snapshot_lease_until_queue_is_closed(mocker) -> None:
    spider = _spider_with_manager(mocker)
    queue = mocker.MagicMock(name="queue")
    lease = mocker.MagicMock(name="snapshot-lease")
    lease.release.side_effect = [RuntimeError("temporary lease failure"), None]
    spider._queue = queue
    spider._snapshot_connection_lease = lease
    spider._snapshot_connection_manager = mocker.MagicMock(name="snapshot-manager")

    with pytest.raises(RuntimeError, match="temporary lease failure"):
        spider.close_backend()

    manager = spider._connection_manager
    assert spider._queue is None
    assert spider._snapshot_connection_lease is lease
    assert manager is not None
    manager.close.assert_not_called()

    spider.close_backend()

    assert lease.release.call_count == 2
    manager.close.assert_called_once_with()
    assert spider._snapshot_connection_lease is None
    assert spider._connection_manager is None


def test_close_retries_manager_release_after_all_components_succeed(mocker) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    manager.close.side_effect = [RuntimeError("manager unavailable"), None]

    with pytest.raises(RuntimeError, match="manager unavailable"):
        spider.close_backend()

    assert spider._connection_manager is manager
    manager.close.assert_called_once_with()

    spider.close_backend()

    assert manager.close.call_count == 2
    assert spider._connection_manager is None


@pytest.mark.parametrize("allow_cross_spider", [False, True])
def test_get_queue_forwards_cross_spider_setting(
    mocker,
    allow_cross_spider: bool,
) -> None:
    spider = _spider_with_manager(mocker)
    crawler = mocker.MagicMock()
    crawler.settings = Settings({"SCRAPY_QUEUE_ALLOW_CROSS_SPIDER": allow_cross_spider})
    spider.crawler = crawler

    queue = spider.get_queue()

    assert queue._allow_cross_spider is allow_cross_spider
    queue.close = MagicMock()
    spider.close_backend()


@pytest.mark.parametrize("timeout", [1.25, None])
def test_get_queue_forwards_reactor_io_timeout(
    mocker,
    timeout: float | None,
) -> None:
    spider = _spider_with_manager(mocker)
    crawler = mocker.MagicMock()
    values = {} if timeout is None else {"SCRAPY_REACTOR_IO_TIMEOUT": timeout}
    crawler.settings = Settings(values)
    spider.crawler = crawler

    queue = spider.get_queue()

    expected = DEFAULT_REACTOR_IO_TIMEOUT_S if timeout is None else timeout
    assert queue._reactor_io_timeout == expected
    queue.close = MagicMock()
    spider.close_backend()


def test_get_queue_uses_scheduler_factory_limits_monitor_and_snapshot_owner(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    crawler = mocker.MagicMock()
    crawler.stats = mocker.MagicMock()
    crawler.settings = Settings(
        {
            "SCRAPY_QUEUE_STRATEGY": "delay",
            "SCRAPY_QUEUE_DELAY_DEFAULT": 2.5,
            "SCRAPY_QUEUE_DELAY_MAX_HELD": 7,
            "SCRAPY_QUEUE_MAX_ITEM_BYTES": 1234,
            "SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY": 3,
            "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD": 9,
            "SCRAPY_MONITOR_POP_RATE_WINDOW_S": 11.0,
            "SCRAPY_QUEUE_SNAPSHOT_OWNER": "worker-a",
            "SCRAPY_QUEUE_SNAPSHOT_MAX_BYTES": 1024,
            "SCRAPY_QUEUE_SNAPSHOT_CHUNK_BYTES": 256,
        }
    )
    spider.crawler = crawler

    queue = spider.get_queue()

    assert queue.max_item_bytes == 1234
    assert queue.depth_sample_every == 3
    assert queue._pop_rate_window_s == 11.0
    assert queue._snapshot_owner == "worker-a"
    assert queue._snapshot_max_bytes == 1024
    assert queue._snapshot_chunk_bytes == 256
    assert queue._strategy._default_delay == 2.5
    assert queue._strategy._max_held == 7
    assert queue._monitor.backpressure_threshold == 9

    # Avoid asking an auto-mocked storage backend to persist a test snapshot.
    queue.close = MagicMock()
    spider.close_backend()


def test_close_retains_failed_queue_lease_for_retry(mocker) -> None:
    spider = _spider_with_manager(mocker)
    lease = mocker.MagicMock(name="queue-lease")
    lease.release.side_effect = [RuntimeError("temporary queue lease failure"), None]
    spider._queue_connection_manager = mocker.MagicMock(name="queue-manager")
    spider._queue_connection_lease = lease

    with pytest.raises(RuntimeError, match="temporary queue lease failure"):
        spider.close_backend()

    assert spider._queue_connection_lease is lease
    main_manager = spider._connection_manager
    assert main_manager is not None
    main_manager.close.assert_not_called()
    spider.close_backend()
    assert lease.release.call_count == 2
    main_manager.close.assert_called_once_with()


def test_close_surfaces_dupefilter_failure_and_retains_it(mocker) -> None:
    spider = _spider_with_manager(mocker)
    dupefilter = mocker.MagicMock(name="dupefilter")
    dupefilter.close.side_effect = RuntimeError("dupefilter unavailable")
    spider._dupefilter = dupefilter

    with pytest.raises(RuntimeError, match="dupefilter unavailable"):
        spider.close_backend()

    assert spider._dupefilter is dupefilter
    spider._connection_manager.close.assert_not_called()


def test_close_handles_signal_effect_then_error_without_releasing_manager(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    signal_manager = mocker.MagicMock(name="signals")
    spider._connected_signals = signal_manager
    spider._signals_connected = True

    def disconnect(_handler, _signal):
        spider._connected_signals = None
        raise RuntimeError("signal effect then error")

    signal_manager.disconnect.side_effect = disconnect
    manager = spider._connection_manager

    with pytest.raises(RuntimeError, match="signal effect then error"):
        spider.close_backend()

    assert manager is not None
    manager.close.assert_not_called()
    signal_manager.disconnect.side_effect = None
    spider.close_backend()


def test_close_preserves_primary_signal_error_when_manager_also_fails(mocker) -> None:
    spider = _spider_with_manager(mocker)
    signal_manager = mocker.MagicMock(name="signals")
    spider._connected_signals = signal_manager
    spider._signals_connected = True

    def disconnect(_handler, _signal):
        spider._connected_signals = None
        raise RuntimeError("signal primary")

    signal_manager.disconnect.side_effect = disconnect
    manager = spider._connection_manager
    manager.close.side_effect = RuntimeError("manager secondary")

    with pytest.raises(RuntimeError, match="signal primary"):
        spider.close_backend()


def test_close_does_not_clear_replaced_queue_reference(mocker) -> None:
    spider = _spider_with_manager(mocker)
    queue = mocker.MagicMock(name="queue")

    def replace_before_return():
        spider._queue = mocker.MagicMock(name="replacement")

    queue.close.side_effect = replace_before_return
    spider._queue = queue

    spider.close_backend()

    assert spider._queue is not queue
    spider._queue.close = MagicMock()
    spider.close_backend()


def test_close_does_not_clear_replaced_signal_reference(mocker) -> None:
    spider = _spider_with_manager(mocker)
    signal_manager = mocker.MagicMock(name="signals")
    spider._connected_signals = signal_manager
    spider._signals_connected = True

    def replace_before_return(_handler, _signal):
        spider._connected_signals = mocker.MagicMock(name="replacement-signals")

    signal_manager.disconnect.side_effect = replace_before_return
    spider.close_backend()

    assert spider._connected_signals is not None
    spider._connected_signals = None
    spider.close_backend()


def test_close_preserves_process_control_from_dupefilter(mocker) -> None:
    spider = _spider_with_manager(mocker)
    dupefilter = mocker.MagicMock(name="dupefilter")
    dupefilter.close.side_effect = KeyboardInterrupt("dupefilter interrupted")
    spider._dupefilter = dupefilter

    with pytest.raises(KeyboardInterrupt, match="dupefilter interrupted"):
        spider.close_backend()

    assert spider._dupefilter is dupefilter


def test_close_clears_snapshot_lease_only_when_release_keeps_identity(mocker) -> None:
    spider = _spider_with_manager(mocker)
    lease = mocker.MagicMock(name="snapshot-lease")

    def replace_lease():
        spider._snapshot_connection_lease = mocker.MagicMock(name="replacement")

    lease.release.side_effect = replace_lease
    spider._snapshot_connection_lease = lease
    spider.close_backend()

    assert spider._snapshot_connection_lease is not lease
    spider._snapshot_connection_lease = None
    spider.close_backend()


def test_close_preserves_process_control_from_queue_lease(mocker) -> None:
    spider = _spider_with_manager(mocker)
    lease = mocker.MagicMock(name="queue-lease")
    lease.release.side_effect = KeyboardInterrupt("queue lease interrupted")
    spider._queue_connection_lease = lease

    with pytest.raises(KeyboardInterrupt, match="queue lease interrupted"):
        spider.close_backend()

    assert spider._queue_connection_lease is lease


def test_close_manager_error_does_not_replace_component_error(mocker) -> None:
    spider = _spider_with_manager(mocker)
    scheduler = mocker.MagicMock(name="scheduler")

    def fail_after_detach(_reason):
        spider._scheduler = None
        raise RuntimeError("scheduler primary")

    scheduler.close.side_effect = fail_after_detach
    spider._scheduler = scheduler
    spider._connection_manager.close.side_effect = RuntimeError("manager secondary")

    with pytest.raises(RuntimeError, match="scheduler primary"):
        spider.close_backend()


def test_close_does_not_clear_replaced_component_reference(mocker) -> None:
    spider = _spider_with_manager(mocker)
    dupefilter = mocker.MagicMock(name="dupefilter")

    def replace_before_return(_reason):
        spider._dupefilter = mocker.MagicMock(name="replacement")

    dupefilter.close.side_effect = replace_before_return
    spider._dupefilter = dupefilter

    spider.close_backend()

    assert spider._dupefilter is not None
    spider._dupefilter.close = MagicMock()
    spider.close_backend()


def test_close_does_not_clear_replaced_scheduler_reference(mocker) -> None:
    spider = _spider_with_manager(mocker)
    scheduler = mocker.MagicMock(name="scheduler")

    def replace_before_return(_reason):
        spider._scheduler = mocker.MagicMock(name="replacement")

    scheduler.close.side_effect = replace_before_return
    spider._scheduler = scheduler

    spider.close_backend()

    assert spider._scheduler is not None
    spider._scheduler.close = MagicMock()
    spider.close_backend()


def test_close_does_not_clear_replaced_manager_reference(mocker) -> None:
    spider = _Spider()
    manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = manager

    def replace_before_return():
        spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)

    manager.close.side_effect = replace_before_return
    spider.close_backend()

    assert spider._connection_manager is not manager
    spider._connection_manager.close = MagicMock()
    spider.close_backend()


def test_get_scheduler_with_no_dupefilter_path_uses_backend_filter(mocker) -> None:
    spider = _spider_with_manager(mocker)
    crawler = mocker.MagicMock()
    crawler.settings = Settings({"DUPEFILTER_CLASS": ""})
    spider.crawler = crawler

    scheduler = spider.get_scheduler()

    assert scheduler.dupefilter is spider._dupefilter
    spider.close_backend()


def test_get_queue_explicit_backend_override_releases_its_own_lease(mocker) -> None:
    spider = _spider_with_manager(mocker)
    crawler = mocker.MagicMock()
    crawler.settings = Settings(
        {
            "SCRAPY_QUEUE_BACKEND_TYPE": "kafka",
        }
    )
    spider.crawler = crawler
    queue_manager = mocker.MagicMock(name="queue-manager")
    queue_lease = mocker.MagicMock(name="queue-lease")
    queue_lease.manager = queue_manager
    mocker.patch.object(ConnectionManager, "acquire_lease", return_value=queue_lease)
    # The mixin imports BackendQueue lazily from its defining module.
    queue_module = mocker.patch(
        "scrapy_extension.queue.queue.BackendQueue", return_value=MagicMock()
    )

    result = spider.get_queue()

    assert result is queue_module.return_value
    assert queue_module.call_args.kwargs["connection_manager"] is queue_manager
    main_manager = spider._connection_manager
    spider.close_backend()
    queue_lease.release.assert_called_once_with()
    main_manager.close.assert_called_once_with()


def test_get_queue_constructor_failure_closes_factory_without_extra_leases(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    mocker.patch(
        "scrapy_extension.queue.queue.BackendQueue",
        side_effect=RuntimeError("queue construction failed"),
    )

    with pytest.raises(RuntimeError, match="queue construction failed"):
        spider.get_queue()

    spider._connection_manager.close.assert_not_called()


def test_get_dupefilter_honors_a_set_backend_override(mocker) -> None:
    class KafkaSpider(BackendSpiderMixin, Spider):
        name = "kafka-dedupe"
        backend_type = BackendType.KAFKA

    spider = KafkaSpider()
    main_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = main_manager
    crawler = mocker.MagicMock()
    crawler.settings = Settings({"SCRAPY_SET_BACKEND_TYPE": "redis"})
    spider.crawler = crawler
    set_manager = mocker.MagicMock(name="set-manager")
    set_lease = mocker.MagicMock(name="set-lease")
    set_lease.manager = set_manager
    mocker.patch.object(ConnectionManager, "acquire_lease", return_value=set_lease)

    dupefilter = spider.get_dupefilter()

    assert dupefilter.connection_manager is set_manager
    assert dupefilter._owns_connection_manager is True
    spider.close_backend()
    set_lease.release.assert_called_once_with()
    main_manager.close.assert_called_once_with()


def test_get_scheduler_honors_a_queue_backend_override(mocker) -> None:
    spider = _spider_with_manager(mocker)
    crawler = mocker.MagicMock()
    crawler.settings = Settings({"SCRAPY_QUEUE_BACKEND_TYPE": "kafka"})
    spider.crawler = crawler
    queue_manager = mocker.MagicMock(name="queue-manager")
    queue_lease = mocker.MagicMock(name="queue-lease")
    queue_lease.manager = queue_manager
    mocker.patch.object(ConnectionManager, "acquire_lease", return_value=queue_lease)

    scheduler = spider.get_scheduler()

    assert scheduler.connection_manager is queue_manager
    assert scheduler._owns_connection_manager is True
    main_manager = spider._connection_manager
    spider.close_backend()
    queue_lease.release.assert_called_once_with()
    main_manager.close.assert_called_once_with()


def test_get_scheduler_reuses_dupefilter_and_factory_ack_gate(mocker) -> None:
    spider = _spider_with_manager(mocker)
    crawler = mocker.MagicMock()
    crawler.stats = mocker.MagicMock()
    crawler.settings = Settings(
        {
            "SCRAPY_QUEUE_STRATEGY": "delay",
            "SCRAPY_QUEUE_MAX_ITEM_BYTES": 4321,
            "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD": 13,
            "SCRAPY_MONITOR_POP_RATE_WINDOW_S": 17.0,
            "SCRAPY_DEDUP_STRATEGY": "memory",
            "SCRAPY_DEDUP_MEMORY_MAXSIZE": 19,
            "DUPEFILTER_CLASS": (
                "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"
            ),
            "CONCURRENT_REQUESTS": 1,
        }
    )
    spider.crawler = crawler
    gate = mocker.patch(
        "scrapy_extension.schedule.scheduler.BackendScheduler._enforce_ack_concurrency_gate"
    )

    scheduler = spider.get_scheduler()

    assert scheduler.stats is crawler.stats
    assert scheduler._queue_max_item_bytes == 4321
    assert scheduler._monitor_backpressure_threshold == 13
    assert scheduler._monitor_pop_rate_window_s == 17.0
    assert scheduler.dupefilter is spider._dupefilter
    assert scheduler.dupefilter._filter._maxsize == 19
    gate.assert_called_once()

    spider.close_backend()
    assert spider._connection_manager is None


def test_get_queue_recursive_close_discards_candidate_once(mocker) -> None:
    spider = _spider_with_manager(mocker)
    candidate = MagicMock(name="candidate-queue")
    recursive_results: list[object] = []

    def construct_queue(**_kwargs):
        recursive_results.append(spider.close_backend())
        return candidate

    mocker.patch(
        "scrapy_extension.queue.queue.BackendQueue",
        side_effect=construct_queue,
    )

    with pytest.raises(RuntimeError, match="queue construction completed after close"):
        spider.get_queue()

    assert recursive_results == [None]
    candidate.close.assert_called_once_with()
    assert spider._queue is None
    assert spider._connection_manager is None


def test_get_dupefilter_recursive_close_discards_candidate_once(mocker) -> None:
    spider = _spider_with_manager(mocker)
    candidate = MagicMock(name="candidate-dupefilter")
    recursive_results: list[object] = []

    def construct_dupefilter(*_args, **_kwargs):
        recursive_results.append(spider.close_backend())
        return candidate

    mocker.patch.object(
        __import__(
            "scrapy_extension.dupefilter.dupefilter",
            fromlist=["BackendDupeFilter"],
        ).BackendDupeFilter,
        "from_settings",
        side_effect=construct_dupefilter,
    )

    with pytest.raises(
        RuntimeError, match="dupefilter construction completed after close"
    ):
        spider.get_dupefilter()

    assert recursive_results == [None]
    candidate.close.assert_called_once_with("mixin-dupefilter-factory-failed")
    assert spider._dupefilter is None
    assert spider._connection_manager is None


def test_get_scheduler_recursive_close_discards_candidate_once(mocker) -> None:
    spider = _spider_with_manager(mocker)
    spider._dupefilter = MagicMock(name="existing-dupefilter")
    candidate = MagicMock(name="candidate-scheduler")
    candidate.connection_manager = spider._connection_manager
    candidate._snapshot_connection_manager = None
    recursive_results: list[object] = []

    def construct_scheduler(*_args, **_kwargs):
        recursive_results.append(spider.close_backend())
        return candidate

    mocker.patch(
        "scrapy_extension.schedule.scheduler.BackendScheduler.from_settings",
        side_effect=construct_scheduler,
    )

    with pytest.raises(
        RuntimeError, match="scheduler construction completed after close"
    ):
        spider.get_scheduler()

    assert recursive_results == [None]
    candidate.close.assert_called_once_with("mixin-scheduler-factory-failed")
    assert spider._scheduler is None
    assert spider._connection_manager is None


def test_get_queue_peer_close_waits_for_construction_and_closes_published_candidate(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    assert manager is not None
    build_started = threading.Event()
    release_build = threading.Event()
    candidate = MagicMock(name="candidate-queue")
    getter_result: list[object] = []
    getter_errors: list[BaseException] = []

    def construct_queue(**_kwargs):
        build_started.set()
        assert release_build.wait(2.0)
        return candidate

    mocker.patch(
        "scrapy_extension.queue.queue.BackendQueue",
        side_effect=construct_queue,
    )

    getter = threading.Thread(
        target=lambda: _capture_thread_result(
            lambda: spider.get_queue(), getter_result, getter_errors
        )
    )
    getter.start()
    assert build_started.wait(2.0)

    close_result: list[object] = []
    closer = threading.Thread(
        target=lambda: _capture_thread_result(
            spider.close_backend,
            close_result,
            [],
        )
    )
    closer.start()
    # A peer close must wait on the construction reservation, not release the
    # manager while the factory callback is still building the candidate.
    assert closer.is_alive()
    assert not manager.close.called

    release_build.set()
    getter.join(2.0)
    closer.join(2.0)
    assert not getter.is_alive()
    assert not closer.is_alive()
    assert getter_result == [candidate]
    assert close_result == [None]
    candidate.close.assert_called_once_with()
    manager.close.assert_called_once_with()
    assert spider._queue is None


def _capture_thread_result(
    function, results: list[object], errors: list[BaseException]
) -> None:
    try:
        results.append(function())
    except BaseException as exc:
        errors.append(exc)


def test_get_queue_failed_async_cleanup_adopts_every_factory_resource(mocker) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    assert manager is not None
    candidate = MagicMock(name="candidate-queue")
    snapshot_manager = MagicMock(name="snapshot-manager")
    snapshot_lease = MagicMock(name="snapshot-lease")
    queue_lease = MagicMock(name="queue-lease")
    cleanup: Deferred[None] = Deferred()
    factory = MagicMock(name="factory-scheduler")
    factory.connection_manager = manager
    factory._queue_strategy = None
    factory._queue_snapshot_owner = None
    factory._queue_snapshot_max_bytes = 128
    factory._queue_snapshot_chunk_bytes = 64
    factory._queue_depth_sample_every = 1
    factory._monitor_pop_rate_window_s = 60.0
    factory._queue_max_item_bytes = 1024
    factory._allow_cross_spider = False
    factory._reactor_io_timeout = 5.0
    factory._queue = candidate
    factory._snapshot_connection_manager = snapshot_manager
    factory._snapshot_connection_manager_lease = snapshot_lease
    factory._connection_manager_lease = queue_lease
    factory._close_completion_deferred = None
    factory.close.return_value = cleanup

    def construct_queue(**_kwargs):
        raise RuntimeError("queue construction failed")

    mocker.patch(
        "scrapy_extension.schedule.scheduler.BackendScheduler.from_settings",
        return_value=factory,
    )
    mocker.patch(
        "scrapy_extension.queue.queue.BackendQueue",
        side_effect=construct_queue,
    )

    with pytest.raises(RuntimeError, match="queue construction failed"):
        spider.get_queue()

    cleanup.errback(RuntimeError("factory cleanup failed"))

    assert spider._queue is candidate
    assert spider._snapshot_connection_lease is snapshot_lease
    assert spider._queue_connection_lease is queue_lease
    manager.close.assert_not_called()

    spider.close_backend()
    candidate.close.assert_called_once_with()
    snapshot_lease.release.assert_called_once_with()
    queue_lease.release.assert_called_once_with()
    manager.close.assert_called_once_with()


def test_get_queue_successful_async_cleanup_releases_failed_factory_parent(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    assert manager is not None
    snapshot_manager = MagicMock(name="snapshot-manager")
    cleanup: Deferred[None] = Deferred()
    factory = MagicMock(name="factory-scheduler")
    factory.connection_manager = manager
    factory._queue_strategy = None
    factory._queue_snapshot_owner = None
    factory._queue_snapshot_max_bytes = 128
    factory._queue_snapshot_chunk_bytes = 64
    factory._queue_depth_sample_every = 1
    factory._monitor_pop_rate_window_s = 60.0
    factory._queue_max_item_bytes = 1024
    factory._allow_cross_spider = False
    factory._reactor_io_timeout = 5.0
    factory._snapshot_connection_manager = snapshot_manager
    factory._snapshot_connection_manager_lease = MagicMock()
    factory._connection_manager_lease = None
    factory._close_completion_deferred = None
    factory.close.return_value = cleanup

    mocker.patch(
        "scrapy_extension.schedule.scheduler.BackendScheduler.from_settings",
        return_value=factory,
    )

    def fail_queue(**_kwargs):
        raise RuntimeError("queue construction failed")

    mocker.patch("scrapy_extension.queue.queue.BackendQueue", side_effect=fail_queue)

    with pytest.raises(RuntimeError, match="queue construction failed"):
        spider.get_queue()

    cleanup.callback(None)
    manager.close.assert_called_once_with()
    assert spider._connection_manager is None


def test_get_queue_failure_restores_existing_consumer_claim(mocker) -> None:
    class KafkaSpider(BackendSpiderMixin, Spider):
        name = "claim-restore"
        backend_type = BackendType.KAFKA

    spider = KafkaSpider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._consumer_queue_name = "claimed"
    mocker.patch(
        "scrapy_extension.queue.queue.BackendQueue",
        side_effect=RuntimeError("queue construction failed"),
    )

    with pytest.raises(RuntimeError, match="queue construction failed"):
        spider.get_queue("claimed")

    assert spider._consumer_queue_name == "claimed"


def test_get_scheduler_failure_restores_existing_consumer_claim(mocker) -> None:
    class KafkaSpider(BackendSpiderMixin, Spider):
        name = "scheduler-claim-restore"
        backend_type = BackendType.KAFKA

    spider = KafkaSpider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)
    queue_name = spider._mixin_queue_key()
    spider._consumer_queue_name = queue_name
    mocker.patch(
        "scrapy_extension.schedule.scheduler.BackendScheduler.from_settings",
        side_effect=RuntimeError("scheduler construction failed"),
    )

    with pytest.raises(RuntimeError, match="scheduler construction failed"):
        spider.get_scheduler()

    assert spider._consumer_queue_name == queue_name


def test_close_backend_async_success_clears_scheduler_owned_dupefilter(mocker) -> None:
    spider = _spider_with_manager(mocker)
    scheduler = mocker.MagicMock(name="scheduler")
    dupefilter = mocker.MagicMock(name="dupefilter")
    close_result: Deferred[None] = Deferred()
    scheduler.dupefilter = dupefilter
    scheduler.close.return_value = close_result
    scheduler._close_completion_deferred = None
    spider._scheduler = scheduler
    spider._dupefilter = dupefilter

    result = spider.close_backend()
    assert result is close_result
    close_result.callback(None)

    assert spider._scheduler is None
    assert spider._dupefilter is None
    assert spider._connection_manager is None


def test_get_queue_failed_cleanup_adopts_queue_lease_without_snapshot(mocker) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    assert manager is not None
    candidate = MagicMock(name="orphan-queue")
    queue_lease = MagicMock(name="queue-lease")
    cleanup: Deferred[None] = Deferred()
    factory = MagicMock(name="factory-scheduler")
    factory.connection_manager = manager
    factory._queue = candidate
    factory._snapshot_connection_manager = None
    factory._snapshot_connection_manager_lease = None
    factory._connection_manager_lease = queue_lease
    factory.close.return_value = cleanup
    mocker.patch(
        "scrapy_extension.schedule.scheduler.BackendScheduler.from_settings",
        return_value=factory,
    )
    mocker.patch(
        "scrapy_extension.queue.queue.BackendQueue",
        side_effect=RuntimeError("queue construction failed"),
    )

    with pytest.raises(RuntimeError, match="queue construction failed"):
        spider.get_queue()
    cleanup.errback(RuntimeError("queue cleanup failed"))

    assert spider._queue is candidate
    assert spider._queue_connection_lease is queue_lease
    spider.close_backend()
    candidate.close.assert_called_once_with()
    queue_lease.release.assert_called_once_with()
    manager.close.assert_called_once_with()


def test_get_scheduler_waits_for_authoritative_failed_cleanup(mocker) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    assert manager is not None
    spider._dupefilter = MagicMock(name="existing-dupefilter")
    candidate = MagicMock(name="candidate-scheduler")
    candidate.connection_manager = manager
    candidate._snapshot_connection_manager = None
    public: Deferred[None] = Deferred()
    authoritative: Deferred[None] = Deferred()
    candidate.close.return_value = public
    candidate._close_completion_deferred = authoritative
    recursive_results: list[object] = []

    def construct_scheduler(*_args, **_kwargs):
        recursive_results.append(spider.close_backend())
        return candidate

    mocker.patch(
        "scrapy_extension.schedule.scheduler.BackendScheduler.from_settings",
        side_effect=construct_scheduler,
    )

    with pytest.raises(
        RuntimeError, match="scheduler construction completed after close"
    ):
        spider.get_scheduler()

    public.errback(RuntimeError("bounded cleanup timeout"))
    authoritative.errback(RuntimeError("authoritative cleanup failure"))

    assert recursive_results == [None]
    candidate.close.assert_called_once_with("mixin-scheduler-factory-failed")
    assert spider._scheduler is None
    manager.close.assert_not_called()
    assert spider._orphan_candidates == [("scheduler", candidate)]


def test_get_queue_aborts_before_callback_when_reservation_is_invalidated(
    mocker,
) -> None:
    spider = _spider_with_manager(mocker)
    manager = spider._connection_manager
    assert manager is not None
    construction = _ComponentConstruction(
        kind="queue",
        generation=spider._component_generation,
        owner_thread_id=threading.get_ident(),
        invalidated=True,
    )
    mocker.patch.object(
        spider,
        "_reserve_component_construction",
        return_value=(manager, construction),
    )

    with pytest.raises(RuntimeError, match="queue construction was invalidated"):
        spider.get_queue()


def test_get_dupefilter_candidate_close_failure_still_releases_manager(mocker) -> None:
    spider = _spider_with_manager(mocker)
    candidate = MagicMock(name="candidate-dupefilter")
    candidate.close.side_effect = RuntimeError("candidate close failed")

    def construct_dupefilter(*_args, **_kwargs):
        spider.close_backend()
        return candidate

    from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter

    mocker.patch.object(
        BackendDupeFilter,
        "from_settings",
        side_effect=construct_dupefilter,
    )

    with pytest.raises(
        RuntimeError, match="dupefilter construction completed after close"
    ):
        spider.get_dupefilter()

    candidate.close.assert_called_once_with("mixin-dupefilter-factory-failed")
    assert spider._connection_manager is not None
    assert spider._orphan_candidates == [("dupefilter", candidate)]


def test_get_scheduler_candidate_close_failure_still_releases_manager(mocker) -> None:
    spider = _spider_with_manager(mocker)
    spider._dupefilter = MagicMock(name="existing-dupefilter")
    candidate = MagicMock(name="candidate-scheduler")
    candidate.connection_manager = spider._connection_manager
    candidate._snapshot_connection_manager = None
    candidate.close.side_effect = RuntimeError("candidate close failed")

    def construct_scheduler(*_args, **_kwargs):
        spider.close_backend()
        return candidate

    mocker.patch(
        "scrapy_extension.schedule.scheduler.BackendScheduler.from_settings",
        side_effect=construct_scheduler,
    )

    with pytest.raises(
        RuntimeError, match="scheduler construction completed after close"
    ):
        spider.get_scheduler()

    candidate.close.assert_called_once_with("mixin-scheduler-factory-failed")
    assert spider._connection_manager is not None
    assert spider._orphan_candidates == [("scheduler", candidate)]


def test_getter_rejects_construction_while_close_is_in_progress(mocker) -> None:
    spider = _spider_with_manager(mocker)
    spider._close_in_progress = True

    with pytest.raises(RuntimeError, match="close is already in progress"):
        spider.get_queue()


def test_getter_rejects_same_thread_recursive_construction(mocker) -> None:
    spider = _spider_with_manager(mocker)
    construction = _ComponentConstruction(
        kind="queue",
        generation=spider._component_generation,
        owner_thread_id=threading.get_ident(),
    )
    spider._component_constructions["queue"] = construction

    with pytest.raises(RuntimeError, match="queue construction is already in progress"):
        spider.get_queue()

    construction.done.set()
    spider._component_constructions.clear()


def test_invalidated_generation_cannot_publish_candidate(mocker) -> None:
    spider = _spider_with_manager(mocker)
    construction = _ComponentConstruction(
        kind="queue",
        generation=spider._component_generation,
        owner_thread_id=threading.get_ident(),
        invalidated=True,
    )

    assert not spider._construction_is_current_locked(
        construction,
        spider._connection_manager,
    )


def test_close_invalidation_is_empty_without_active_construction(mocker) -> None:
    spider = _spider_with_manager(mocker)

    assert spider._invalidate_component_constructions_locked() == ()


def test_finish_open_race_does_not_publish_after_close_request() -> None:
    spider = _Spider()

    # Exercise the real scheduler guard at the publication checkpoint while its
    # close request is already authoritative.
    from scrapy_extension.schedule.scheduler import BackendScheduler

    real_scheduler = BackendScheduler(MagicMock(name="manager"))
    real_scheduler._lifecycle_state = "opening"
    real_scheduler._open_close_requested = True
    real_scheduler._finish_open(spider)
    assert real_scheduler._queue is None


def test_close_releases_successful_queue_override_lease_before_manager(mocker) -> None:
    spider = _spider_with_manager(mocker)
    lease = mocker.MagicMock(name="queue-lease")
    spider._queue_connection_lease = lease

    spider.close_backend()

    lease.release.assert_called_once_with()
    assert spider._connection_manager is None


def test_close_retains_queue_lease_after_release_failure(mocker) -> None:
    spider = _spider_with_manager(mocker)
    lease = mocker.MagicMock(name="queue-lease")
    lease.release.side_effect = RuntimeError("queue lease release failed")
    spider._queue_connection_lease = lease

    with pytest.raises(RuntimeError, match="queue lease release failed"):
        spider.close_backend()

    assert spider._queue_connection_lease is lease
    manager = spider._connection_manager
    assert manager is not None
    manager.close.assert_not_called()


def test_close_retains_snapshot_lease_after_release_failure(mocker) -> None:
    spider = _spider_with_manager(mocker)
    lease = mocker.MagicMock(name="snapshot-lease")
    lease.release.side_effect = RuntimeError("snapshot release failed")
    spider._snapshot_connection_lease = lease

    with pytest.raises(RuntimeError, match="snapshot release failed"):
        spider.close_backend()

    assert spider._snapshot_connection_lease is lease
    manager = spider._connection_manager
    assert manager is not None
    manager.close.assert_not_called()


def test_close_clears_scheduler_owned_dupefilter_after_sync_success(mocker) -> None:
    spider = _spider_with_manager(mocker)
    scheduler = mocker.MagicMock(name="scheduler")
    dupefilter = mocker.MagicMock(name="dupefilter")
    scheduler.dupefilter = dupefilter
    scheduler.close.return_value = None
    spider._scheduler = scheduler
    spider._dupefilter = dupefilter

    spider.close_backend()

    scheduler.close.assert_called_once_with("spider-mixin-close")
    dupefilter.close.assert_not_called()
    assert spider._scheduler is None
    assert spider._dupefilter is None
    assert spider._connection_manager is None
