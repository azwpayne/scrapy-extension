"""Regression coverage for fixed-text, graph-free fallback diagnostics."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from scrapy import Request, Spider

from scrapy_extension.exceptions import QueueError
from scrapy_extension.monitor import NullMonitor
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin


def _assert_records_are_redacted(caplog, marker: str) -> None:
    """Ensure custom logging handlers cannot recover an swallowed error graph."""
    assert caplog.records
    for record in caplog.records:
        assert marker not in record.getMessage()
        assert marker not in repr(record.args)
        assert record.exc_info is None
        assert record.exc_text is None


def test_queue_monitor_fallback_log_is_fixed_and_graph_free(caplog) -> None:
    marker = "round45_queue_monitor_private_marker"
    manager = MagicMock(name="ConnectionManager")
    strategy = MagicMock(name="QueueStrategy")
    monitor = MagicMock(name="Monitor")
    monitor.on_push.side_effect = RuntimeError(marker)
    queue = BackendQueue(
        connection_manager=manager,
        queue_name=marker,
        queue_strategy=strategy,
        monitor=monitor,
    )

    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="scrapy_extension.queue.queue"):
        queue.push(Request(f"https://example.invalid/{marker}"))

    assert [record.getMessage() for record in caplog.records] == [
        "monitor.on_push raised; ignored"
    ]
    _assert_records_are_redacted(caplog, marker)


def test_queue_snapshot_fallback_log_is_fixed_and_graph_free(caplog) -> None:
    marker = "round45_snapshot_private_marker"
    manager = MagicMock(name="ConnectionManager")
    strategy = MagicMock(name="QueueStrategy")
    strategy.snapshot.side_effect = RuntimeError(marker)
    queue = BackendQueue(
        connection_manager=manager,
        queue_name=marker,
        queue_strategy=strategy,
        monitor=NullMonitor(),
    )

    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="scrapy_extension.queue.queue"):
        with pytest.raises(QueueError, match="snapshot creation"):
            queue.close()

    assert [record.getMessage() for record in caplog.records] == [
        "Strategy snapshot creation failed"
    ]
    assert queue._close_complete is False
    _assert_records_are_redacted(caplog, marker)


def test_scheduler_degradation_logs_are_fixed_and_graph_free(caplog) -> None:
    marker = "round45_scheduler_private_marker"
    scheduler = BackendScheduler(connection_manager=MagicMock(name="ConnectionManager"))
    queue = MagicMock(name="BackendQueue")
    queue.pop.side_effect = QueueError(marker)
    scheduler._queue = queue

    with caplog.at_level(logging.ERROR, logger="scrapy_extension.schedule.scheduler"):
        assert scheduler.next_request() is None

    assert [record.getMessage() for record in caplog.records] == [
        "Failed to get next request"
    ]
    _assert_records_are_redacted(caplog, marker)


def test_scheduler_settlement_fallback_log_is_fixed_and_graph_free(caplog) -> None:
    marker = "round45_settlement_private_marker"
    scheduler = BackendScheduler(connection_manager=MagicMock(name="ConnectionManager"))
    queue = MagicMock(name="BackendQueue")
    queue.ack.side_effect = QueueError(marker)
    scheduler._queue = queue

    with caplog.at_level(logging.ERROR, logger="scrapy_extension.schedule.scheduler"):
        assert (
            scheduler._ack_token(
                marker,
                log_message="Failed to acknowledge queued request",
            )
            is False
        )

    assert [record.getMessage() for record in caplog.records] == [
        "Failed to acknowledge queued request"
    ]
    _assert_records_are_redacted(caplog, marker)


def test_scheduler_shutdown_fallback_log_is_fixed_and_graph_free(caplog) -> None:
    marker = "round45_shutdown_private_marker"
    scheduler = BackendScheduler(connection_manager=MagicMock(name="ConnectionManager"))
    queue = MagicMock(name="BackendQueue")
    queue.close.side_effect = RuntimeError(marker)
    scheduler._queue = queue

    with caplog.at_level(logging.ERROR, logger="scrapy_extension.schedule.scheduler"):
        scheduler.close("test")

    assert [record.getMessage() for record in caplog.records] == [
        "Failed to close queue strategy during shutdown"
    ]
    _assert_records_are_redacted(caplog, marker)


def test_spider_signal_fallback_log_is_fixed_and_graph_free(caplog) -> None:
    marker = "round45_spider_private_marker"

    class TestSpider(BackendSpiderMixin, Spider):
        name = "diagnostic-redaction-spider"

    spider = TestSpider()
    spider.close_backend = MagicMock(side_effect=RuntimeError(marker))  # type: ignore[method-assign]

    with caplog.at_level(logging.ERROR, logger="scrapy_extension.spider.spider_mixin"):
        spider._on_spider_closed(spider, reason="finished")

    assert [record.getMessage() for record in caplog.records] == [
        "close_backend() failed during spider_closed signal"
    ]
    _assert_records_are_redacted(caplog, marker)
