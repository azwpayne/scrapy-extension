"""Regression tests for continuation diagnostics outside active exceptions."""

from __future__ import annotations

import json
import logging
import sys
import warnings
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from scrapy import Request

from scrapy_extension.exceptions import QueueError
from scrapy_extension.monitor import NullMonitor
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.ring_buffer import RingBufferQueueStrategy
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy
from scrapy_extension.queue.strategies.time_wheel import TimeWheelQueueStrategy


class _ExceptionContextProbe(logging.Handler):
    """Capture what a synchronous handler can inspect during ``emit``."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.contexts: list[tuple[object | None, object | None, object | None]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.contexts.append(sys.exc_info())


@contextmanager
def _probe_logger(name: str) -> Iterator[_ExceptionContextProbe]:
    logger = logging.getLogger(name)
    old_level = logger.level
    probe = _ExceptionContextProbe()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(probe)
    try:
        yield probe
    finally:
        logger.removeHandler(probe)
        logger.setLevel(old_level)


def _assert_isolated(probe: _ExceptionContextProbe, marker: str) -> None:
    assert probe.records
    assert probe.contexts == [(None, None, None)] * len(probe.records)
    for record in probe.records:
        assert marker not in record.getMessage()
        assert marker not in repr(record.args)
        assert record.exc_info is None
        assert record.exc_text is None


def _connection_manager() -> MagicMock:
    manager = MagicMock(name="ConnectionManager")
    manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")
    return manager


@pytest.mark.parametrize(
    ("logger_name", "factory"),
    [
        (
            "scrapy_extension.queue.strategies.delay",
            lambda: DelayQueueStrategy(_connection_manager(), clock=lambda: 0.0),
        ),
        (
            "scrapy_extension.queue.strategies.ring_buffer",
            lambda: RingBufferQueueStrategy(_connection_manager()),
        ),
        (
            "scrapy_extension.queue.strategies.round_robin",
            lambda: RoundRobinQueueStrategy(_connection_manager()),
        ),
        (
            "scrapy_extension.queue.strategies.time_wheel",
            lambda: TimeWheelQueueStrategy(
                _connection_manager(),
                clock=lambda: 0.0,
                wall_clock=lambda: 0.0,
            ),
        ),
    ],
)
def test_snapshot_parse_fallback_handler_cannot_recover_error_context(
    logger_name: str,
    factory: Callable[[], object],
) -> None:
    marker = "round47_snapshot_parse_marker"
    strategy = factory()

    with _probe_logger(logger_name) as probe:
        strategy.restore(f'{{"{marker}":'.encode())  # type: ignore[attr-defined]

    _assert_isolated(probe, marker)


def test_round_robin_malformed_item_diagnostic_redacts_source_and_context() -> None:
    marker = "round47_round_robin_source_marker"
    strategy = RoundRobinQueueStrategy(_connection_manager())
    state = json.dumps(
        {
            "version": 1,
            "strategy": "round_robin",
            "sources": [{"source": marker, "items": ["!!!"]}],
        }
    ).encode()

    with _probe_logger("scrapy_extension.queue.strategies.round_robin") as probe:
        strategy.restore(state)

    _assert_isolated(probe, marker)


def test_delay_monitor_fallback_handler_cannot_recover_error_context() -> None:
    marker = "round47_delay_monitor_marker"
    monitor = MagicMock(name="Monitor")
    monitor.on_delay_depth.side_effect = RuntimeError(marker)
    strategy = DelayQueueStrategy(
        _connection_manager(),
        default_delay=1.0,
        clock=lambda: 0.0,
        monitor=monitor,
    )

    with _probe_logger("scrapy_extension.queue.strategies.delay") as probe:
        strategy.push("queue", b"payload")

    _assert_isolated(probe, marker)


def test_queue_snapshot_fallback_handler_cannot_recover_error_context() -> None:
    marker = "round47_queue_snapshot_marker"
    strategy = MagicMock(name="QueueStrategy")
    strategy.snapshot.side_effect = RuntimeError(marker)
    queue = BackendQueue(
        connection_manager=_connection_manager(),
        queue_name=marker,
        queue_strategy=strategy,
        monitor=NullMonitor(),
    )

    with _probe_logger("scrapy_extension.queue.queue") as probe:
        with pytest.raises(QueueError, match="snapshot creation"):
            queue.close()

    assert queue._close_complete is False
    _assert_isolated(probe, marker)


def test_queue_monitor_fallback_handler_cannot_recover_error_context() -> None:
    marker = "round47_queue_monitor_marker"
    strategy = MagicMock(name="QueueStrategy")
    monitor = MagicMock(name="Monitor")
    monitor.on_push.side_effect = RuntimeError(marker)
    queue = BackendQueue(
        connection_manager=_connection_manager(),
        queue_name=marker,
        queue_strategy=strategy,
        monitor=monitor,
    )

    with _probe_logger("scrapy_extension.queue.queue") as probe:
        queue.push(Request("https://example.invalid/"))

    _assert_isolated(probe, marker)


def test_queue_storage_capability_fallback_redacts_queue_name_and_context() -> None:
    marker = "round47_queue_name_marker"
    manager = _connection_manager()
    manager.get_storage_backend.side_effect = NotImplementedError(marker)
    strategy = MagicMock(name="QueueStrategy")
    strategy.snapshot.return_value = None
    queue = BackendQueue(
        connection_manager=manager,
        queue_name=marker,
        queue_strategy=strategy,
        monitor=NullMonitor(),
    )

    with _probe_logger("scrapy_extension.queue.queue") as probe:
        queue.close()

    _assert_isolated(probe, marker)


def test_legacy_body_warning_handler_cannot_recover_decode_error_context() -> None:
    marker = "round47_legacy_body_marker"
    contexts: list[tuple[object | None, object | None, object | None]] = []
    request_dict = {"body": marker}

    with warnings.catch_warnings():
        warnings.simplefilter("always")
        original_showwarning = warnings.showwarning

        def capture_warning(*_args: object, **_kwargs: object) -> None:
            contexts.append(sys.exc_info())

        warnings.showwarning = capture_warning
        try:
            BackendQueue._decode_body(request_dict)
        finally:
            warnings.showwarning = original_showwarning

    assert request_dict["body"] == marker.encode("utf-8")
    assert contexts == [(None, None, None)]
