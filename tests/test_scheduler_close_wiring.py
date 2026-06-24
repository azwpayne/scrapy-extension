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

import logging
from unittest.mock import MagicMock

from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
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


class TestSchedulerCloseFiresDelayStrategyWarningE2E:
  """End-to-end: scheduler.close() → BackendQueue.close() →
  DelayQueueStrategy.close() must fire the non-silent-loss WARNING when held
  delayed items exist at shutdown.

  The two halves of this chain are pinned in isolation above (scheduler
  calls ``_queue.close()``; BackendQueue.close forwards to
  ``_strategy.close()``). This test proves them together with REAL objects —
  a real ``DelayQueueStrategy`` holding a delayed item, wrapped in a real
  ``BackendQueue``, set as a real ``BackendScheduler``'s ``_queue`` — so the
  P1 "non-silent loss" guarantee is shown to actually fire in the integrated
  shutdown lifecycle, not just in isolation.

  The connection_manager is a MagicMock with a no-op ``.close()`` (scheduler
  also tears it down during close) and a ``get_queue_backend()`` returning a
  MagicMock — the delay strategy's holding path never reaches the backend on
  close (only ``_drain_ready`` does, and close does not drain).
  """

  def test_delay_strategy_warning_fires_through_scheduler_close(
    self, caplog
  ) -> None:
    # Real delay strategy seeded with one held item.
    fake_manager = MagicMock(name="ConnectionManager")
    fake_manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")
    strategy = DelayQueueStrategy(connection_manager=fake_manager)

    # Push a delayed item. effective delay > 0 parks it in the in-process
    # holding heap; the backend's push is never touched.
    strategy.push("q", b"delayed-payload", delay=3600.0)
    assert len(strategy._holding) == 1

    # Real BackendQueue wrapping the real strategy.
    queue = BackendQueue(
      connection_manager=fake_manager,
      queue_name="q",
      queue_strategy=strategy,
    )

    # Real scheduler with the queue wired in (as open() would do).
    # fake_manager.close() must be a no-op so scheduler teardown completes.
    scheduler = BackendScheduler(connection_manager=fake_manager)
    scheduler._queue = queue

    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      scheduler.close(reason="done")

    # The warning must have fired through the integrated close chain.
    warnings = [
      r
      for r in caplog.records
      if r.levelno == logging.WARNING
      and "discarding" in r.getMessage()
      and "held delayed item" in r.getMessage()
    ]
    assert len(warnings) == 1, (
      f"Expected exactly one DelayQueueStrategy discard warning through the "
      f"scheduler close path; got {len(warnings)}: "
      f"{[r.getMessage() for r in caplog.records]}"
    )
    # Held count is surfaced in the message (non-silent loss is quantitative).
    assert "1 held" in warnings[0].getMessage()

    # Holding heap cleared after warning, and scheduler teardown completed.
    assert strategy._holding == []
    fake_manager.close.assert_called_once_with()
