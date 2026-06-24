"""Wiring tests: the queue-strategy ``close()`` lifecycle must reach production.

``DelayQueueStrategy.close()`` warns about held items lost on shutdown (P1 fix),
but that warning is inert unless something calls it. These tests pin the two
links in the chain so the non-silent-loss guarantee actually holds in a real
Scrapy crawl:

1. ``BackendQueue.close()`` forwards to ``self._strategy.close()``.
2. ``BackendScheduler.close()`` calls ``self._queue.close()`` BEFORE
   ``connection_manager.close()`` (order matters: the strategy may want the
   backend still connected, and the warning must fire before teardown).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler


class TestBackendQueueCloseDelegatesToStrategy:
  """BackendQueue.close() must forward to the strategy's close()."""

  def test_close_calls_strategy_close(self) -> None:
    strategy = MagicMock(name="QueueStrategy")
    bq = BackendQueue(
      connection_manager=MagicMock(name="ConnectionManager"),
      queue_name="q",
      queue_strategy=strategy,
    )

    bq.close()

    strategy.close.assert_called_once_with()

  def test_close_on_default_passthrough_strategy_is_safe(self) -> None:
    """No strategy supplied → PassthroughQueueStrategy.close() (a no-op) runs
    without raising. Guards against the common case regressing."""
    bq = BackendQueue(
      connection_manager=MagicMock(name="ConnectionManager"),
      queue_name="q",
    )

    bq.close()  # must not raise


class TestBackendSchedulerCloseInvokesQueueClose:
  """BackendScheduler.close() must call self._queue.close() before the
  connection manager closes — otherwise DelayQueueStrategy.close() (the P1
  non-silent-loss warning) never fires in production."""

  def test_close_calls_queue_close_then_manager_close_in_order(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(connection_manager=manager)
    mock_queue = MagicMock(name="BackendQueue")
    scheduler._queue = mock_queue

    # Record call order across the two collaborators.
    call_order: list[str] = []
    mock_queue.close.side_effect = lambda: call_order.append("queue.close")
    manager.close.side_effect = lambda: call_order.append("manager.close")

    scheduler.close(reason="test-done")

    mock_queue.close.assert_called_once_with()
    manager.close.assert_called_once_with()
    # Strategy close MUST precede connection teardown.
    assert call_order == ["queue.close", "manager.close"], (
      f"Expected queue.close before manager.close, got {call_order}"
    )

  def test_close_swallows_strategy_close_exception(self) -> None:
    """A failing strategy.close() must not prevent connection teardown or
    crash shutdown — the scheduler logs and continues (defense-in-depth)."""
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(connection_manager=manager)
    mock_queue = MagicMock(name="BackendQueue")
    mock_queue.close.side_effect = RuntimeError("strategy boom")
    scheduler._queue = mock_queue

    scheduler.close(reason="test-done")  # must not raise

    mock_queue.close.assert_called_once_with()
    manager.close.assert_called_once_with()

  def test_close_with_no_open_queue_is_safe(self) -> None:
    """If open() was never called (_queue is None), close() must not raise."""
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(connection_manager=manager)
    # _queue defaults to None per __init__

    scheduler.close(reason="never-opened")  # must not raise

    manager.close.assert_called_once_with()
