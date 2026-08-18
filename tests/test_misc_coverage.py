"""Misc coverage gaps: monitor module and throttle length."""

from __future__ import annotations

from scrapy_extension.queue.strategies.throttle import ThrottleQueueStrategy


def test_monitor_module_importable() -> None:
    """Cover the empty monitor namespace module (TYPE_CHECKING stub)."""
    import scrapy_extension.monitor

    assert scrapy_extension.monitor is not None


def test_throttle_queue_len(mock_connection_manager) -> None:
    """Cover ThrottleQueueStrategy.queue_len delegation."""
    strat = ThrottleQueueStrategy(mock_connection_manager)
    mock_connection_manager.get_queue_backend().queue_len.return_value = 5
    assert strat.queue_len("q") == 5
