"""Scheduler CLOSING ownership-state regressions."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler


def _real_queue(*, failure: BaseException | None = None) -> BackendQueue:
    queue = object.__new__(BackendQueue)
    queue._close_complete = failure is None
    queue.close = Mock(side_effect=failure)  # type: ignore[method-assign]
    return queue


def test_shared_manager_releases_only_scheduler_leases() -> None:
    queue_lease = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": "scheduler-shared"}
    )
    snapshot_lease = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": "scheduler-shared"}
    )
    unrelated = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": "scheduler-shared"}
    )
    manager = queue_lease.manager
    scheduler = BackendScheduler(
        manager,
        snapshot_connection_manager=manager,
        owns_snapshot_connection_manager=True,
        connection_manager_lease=queue_lease,
        snapshot_connection_manager_lease=snapshot_lease,
    )
    scheduler._queue = _real_queue()

    scheduler.close("finished")
    scheduler.close("duplicate")

    assert queue_lease.released is True
    assert snapshot_lease.released is True
    assert unrelated.released is False
    assert manager._users == 1
    assert scheduler._lifecycle_state == "closed"
    unrelated.release()


def test_checkpoint_failure_retains_every_downstream_handle() -> None:
    queue_lease = ConnectionManager.acquire_lease(
        BackendType.REDIS, {"host": "scheduler-checkpoint"}
    )
    manager = queue_lease.manager
    dupefilter = Mock()
    scheduler = BackendScheduler(
        manager,
        dupefilter=dupefilter,
        connection_manager_lease=queue_lease,
    )
    queue = _real_queue(failure=QueueError("checkpoint failed"))
    scheduler._queue = queue

    with pytest.raises(QueueError, match="checkpoint failed"):
        scheduler.close("first")

    assert scheduler._lifecycle_state == "closing"
    assert scheduler._queue is queue
    assert scheduler._dupefilter_released is False
    assert queue_lease.released is False

    queue._close_complete = True
    queue.close.side_effect = None  # type: ignore[attr-defined]
    scheduler.close("retry")
    dupefilter.close.assert_called_once_with("retry")
    assert queue_lease.released is True


def test_persistent_signal_failure_prevents_closed_and_handle_clearing() -> None:
    manager = Mock()
    scheduler = BackendScheduler(manager)
    signal_manager = Mock()
    spider = Mock()
    spider.crawler.signals = signal_manager
    scheduler._connect_ack_signals(spider)
    signal_manager.disconnect.side_effect = RuntimeError("persistent signal failure")

    with pytest.raises(RuntimeError, match="persistent signal failure"):
        scheduler.close("first")

    assert scheduler._lifecycle_state == "closing"
    assert len(scheduler._signal_leases) == 2
    manager.close.assert_not_called()

    signal_manager.disconnect.side_effect = None
    scheduler.close("retry")
    manager.close.assert_called_once_with()
    assert scheduler._lifecycle_state == "closed"


class _InterruptClosedPublicationScheduler(BackendScheduler):
    interrupt_closed_once = False

    def __setattr__(self, name: str, value: object) -> None:
        if (
            name == "_lifecycle_state"
            and value == "closed"
            and self.interrupt_closed_once
        ):
            object.__setattr__(self, "interrupt_closed_once", False)
            raise KeyboardInterrupt
        super().__setattr__(name, value)


def test_interruption_before_closed_publication_retries_without_releases() -> None:
    manager = Mock()
    scheduler = _InterruptClosedPublicationScheduler(manager)
    scheduler._queue = _real_queue()
    scheduler.interrupt_closed_once = True

    with pytest.raises(KeyboardInterrupt):
        scheduler.close("first")

    assert scheduler._lifecycle_state == "closing"
    manager.close.assert_called_once_with()

    scheduler.close("retry")
    manager.close.assert_called_once_with()
    assert scheduler._lifecycle_state == "closed"
