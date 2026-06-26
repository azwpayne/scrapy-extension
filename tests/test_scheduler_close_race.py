"""Round-8 forward coverage: scheduler close-path + close/idle ordering.

Closes F7 (``schedule/scheduler.py`` 92.99% → higher). The close path
(``BackendScheduler.close``, scheduler.py:430-455) has FOUR observable
behaviors that this module pins:

1. **Signal disconnect.** ``close()`` disconnects ``_on_response_received``
   and ``_on_spider_error`` from the crawler signal manager (so a stale
   scheduler doesn't keep acking/nacking for a new one).
2. **Queue-strategy-close BEFORE connection-manager-close.** The queue
   strategy's ``close()`` runs FIRST (so e.g. ``DelayQueueStrategy`` can
   warn about held items) while the backend is still connected; only THEN
   does ``connection_manager.close()`` run. This ordering is load-bearing
   — reversing it would race the strategy's final flush against a closed
   backend.
3. **Strategy-close failure is non-fatal.** A ``close()`` raising on the
   strategy is caught + logged; the connection manager STILL closes and
   scheduler state is STILL reset. One bad strategy can't leak the
   connection.
4. **State reset.** After close, ``_queue``/``_spider``/``_connected_signals``
   are ``None`` and ``_signals_connected`` is ``False`` — so the scheduler
   is reusable (``open()`` again wires signals cleanly).

HONESTY NOTE — no close-race exists in the code:
``close()`` does NOT touch any in-flight / unacked tracker. The at-least-once
guarantee under close is provided by the BACKEND (unacked messages
re-deliver on reconnect — Kafka offset / RabbitMQ redelivery / SQS visibility
timeout) — NOT by the scheduler. The scheduler's only close-time data-loss
surface is the in-process ``DelayQueueStrategy`` holding heap, and that's
covered by ``close()`` → ``strategy.close()`` (behavior #2 above). This
module tests the REAL close-path behaviors; it does NOT invent a
close-race that the code doesn't have.
"""

from __future__ import annotations

from unittest.mock import ANY

import pytest
from scrapy.http import Request

from scrapy_extension.schedule.scheduler import BackendScheduler


def _make_scheduler_with_queue(
  mock_connection_manager, mocker, *, queue_strategy=None
) -> tuple[BackendScheduler, object]:
  """Build an opened scheduler with an injected mock queue strategy.

  Returns (scheduler, queue_strategy_mock). The scheduler's ``_queue`` is a
  real ``BackendQueue`` wrapping the mock strategy — so ``close()`` exercises
  the real ``BackendQueue.close()`` → ``strategy.close()`` path.
  """
  spider = mock_connection_manager.get_queue_backend()
  spider.name = "test_spider"
  spider.crawler = mocker.MagicMock()

  scheduler = BackendScheduler(
    connection_manager=mock_connection_manager,
    queue_key="test:queue",
    queue_strategy=queue_strategy,
  )
  scheduler.open(spider)
  return scheduler, queue_strategy


class TestCloseDisconnectsAckSignals:
  """Behavior #1: close() disconnects the two ack/nack signal handlers."""

  def test_close_disconnects_both_handlers(self, mock_connection_manager, mocker):
    """close() calls ``signals.disconnect`` for both handlers.

    Re-asserts the existing test_components contract from a close-race angle:
    after close, the crawler's signal manager has TWO disconnect calls — one
    for ``_on_response_received`` (response_received) and one for
    ``_on_spider_error`` (spider_error). A regression that drops one
    disconnect leaves a stale handler acking for a dead scheduler.
    """
    signals_mock = mocker.Mock()
    crawler = mocker.Mock(signals=signals_mock)
    spider = mocker.Mock(crawler=crawler)
    spider.name = "test_spider"

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    scheduler.open(spider)

    signals_mock.disconnect.reset_mock()
    scheduler.close("finished")

    assert signals_mock.disconnect.call_count == 2
    signals_mock.disconnect.assert_any_call(
      scheduler._on_response_received,
      signal=ANY,
    )
    signals_mock.disconnect.assert_any_call(
      scheduler._on_spider_error,
      signal=ANY,
    )


class TestCloseStrategyBeforeConnectionManager:
  """Behavior #2: queue-strategy close runs BEFORE connection-manager close.

  Load-bearing ordering: the strategy's ``close()`` may need to flush /
  warn about in-process state (e.g. ``DelayQueueStrategy``'s held-item
  warning) while the backend is still reachable. Only AFTER that does
  ``connection_manager.close()`` tear down the backend.
  """

  def test_strategy_close_called_before_connection_manager_close(
    self, mock_connection_manager, mocker
  ):
    """The strategy's close() invocation precedes connection_manager.close().

    Uses a recording mock strategy; asserts the call-order via a shared list.
    """
    from scrapy_extension.queue.strategies.base import QueueStrategy

    call_log: list[str] = []

    class _RecordingStrategy(QueueStrategy):
      """Minimal strategy that logs close() into the shared list."""

      def push(self, queue_name, item, *, priority=0.0, delay=0.0, source="default"):  # noqa: ARG002
        pass

      def pop(self, queue_name, timeout=0.0):  # noqa: ARG002
        return None

      def queue_len(self, queue_name):  # noqa: ARG002
        return 0

      def clear(self, queue_name):  # noqa: ARG002
        pass

      def close(self) -> None:
        call_log.append("strategy_close")

    def _cm_close_side_effect():
      call_log.append("cm_close")

    mock_connection_manager.close.side_effect = _cm_close_side_effect

    scheduler, _ = _make_scheduler_with_queue(
      mock_connection_manager, mocker, queue_strategy=_RecordingStrategy(
        mock_connection_manager
      )
    )

    scheduler.close("finished")

    # The strategy close MUST come before the connection manager close.
    assert call_log == ["strategy_close", "cm_close"], (
      f"Expected strategy_close before cm_close, got {call_log}"
    )
    mock_connection_manager.close.assert_called_once_with()


class TestStrategyCloseFailureIsNonFatal:
  """Behavior #3: a strategy close() raising does NOT block CM close.

  The scheduler's close path catches any Exception from ``self._queue.close()``
  (scheduler.py:446-449) and STILL proceeds to close the connection manager
  and reset state. One bad strategy can't leak the backend connection.
  """

  def test_strategy_close_raising_still_closes_connection_manager(
    self, mock_connection_manager, mocker
  ):
    """A strategy close() that raises is swallowed; CM close still runs."""
    from scrapy_extension.queue.strategies.base import QueueStrategy

    class _ExplodingStrategy(QueueStrategy):
      def push(self, queue_name, item, *, priority=0.0, delay=0.0, source="default"):  # noqa: ARG002
        pass

      def pop(self, queue_name, timeout=0.0):  # noqa: ARG002
        return None

      def queue_len(self, queue_name):  # noqa: ARG002
        return 0

      def clear(self, queue_name):  # noqa: ARG002
        pass

      def close(self) -> None:
        msg = "simulated strategy close failure"
        raise RuntimeError(msg)

    scheduler, _ = _make_scheduler_with_queue(
      mock_connection_manager, mocker,
      queue_strategy=_ExplodingStrategy(mock_connection_manager),
    )

    # Must NOT raise — the explosion is caught + logged inside close().
    scheduler.close("finished")

    # CM close STILL ran despite the strategy explosion.
    mock_connection_manager.close.assert_called_once_with()
    # State was STILL reset.
    assert scheduler._queue is None
    assert scheduler._spider is None


class TestCloseResetsStateForReuse:
  """Behavior #4: after close, scheduler is reusable (state cleared)."""

  def test_close_clears_queue_spider_and_signals_flag(
    self, mock_connection_manager, mocker
  ):
    """close() sets _queue=None, _spider=None, _signals_connected=False."""
    scheduler, _ = _make_scheduler_with_queue(mock_connection_manager, mocker)

    # Pre-condition: open() populated these.
    assert scheduler._queue is not None
    assert scheduler._spider is not None
    assert scheduler._signals_connected is True

    scheduler.close("finished")

    assert scheduler._queue is None
    assert scheduler._spider is None
    assert scheduler._connected_signals is None
    assert scheduler._signals_connected is False

  def test_scheduler_is_reusable_after_close(self, mock_connection_manager, mocker):
    """After close, open() again wires signals cleanly (no double-register).

    Pins the R12-followup contract: ``_signals_connected`` reset enables
    signal re-wiring on the second open. This is the close→reopen race the
    fix targets — without the reset, ``_connect_ack_signals`` would
    short-circuit on the stale True flag and the second spider's signals
    would never wire.
    """
    spider1 = mock_connection_manager.get_queue_backend()
    spider1.name = "spider_one"
    spider1.crawler = mocker.MagicMock()

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    scheduler.open(spider1)
    assert scheduler._signals_connected is True
    first_connect_count = spider1.crawler.signals.connect.call_count

    scheduler.close("finished")
    assert scheduler._signals_connected is False

    # Re-open with a second spider — signals must wire again.
    spider2 = mocker.MagicMock()
    spider2.name = "spider_two"
    spider2.crawler = mocker.MagicMock()
    scheduler.open(spider2)

    assert scheduler._signals_connected is True
    # Second spider's crawler got the two connect calls (idempotency reset
    # worked — the False from close() let _connect_ack_signals proceed).
    assert spider2.crawler.signals.connect.call_count == 2
    # And the first spider's crawler was NOT touched again.
    assert spider1.crawler.signals.connect.call_count == first_connect_count


class TestClosePopsThenClosesCleanly:
  """Close after pops: no crash, dedup guarantee unaffected.

  Honest scope: the scheduler's close-path has no in-flight tracker to lose.
  The at-least-once guarantee for popped-but-unacked items is a BACKEND
  concern (Kafka offset, RabbitMQ delivery tag, SQS visibility timeout) —
  it does not live in ``BackendScheduler.close``. So this test asserts the
  OBSERVABLE contract: pop-then-close leaves no exception, the queue/CM
  close exactly once, and subsequent ``enqueue_request`` raises the typed
  RuntimeError ("Scheduler not opened") rather than silently no-op'ing.
  """

  def test_pop_then_close_does_not_crash_and_cm_closes_once(
    self, mock_connection_manager, mocker
  ):
    """Pop an item, then close — no exception, CM closed exactly once."""
    mock_queue_backend = mock_connection_manager.get_queue_backend()
    mock_queue_backend.pop.return_value = (
      b'{"url": "https://example.com", "callback": null}'
    )
    mock_queue_backend.name = "test_spider"
    mock_queue_backend.crawler = mocker.MagicMock()

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    scheduler.open(mock_queue_backend)

    popped = scheduler.next_request()
    assert popped is not None
    assert isinstance(popped, Request)

    # close() after a pop must not raise (the popped item's ack lifecycle
    # is the backend's responsibility, not the scheduler's close-path).
    scheduler.close("finished")

    mock_connection_manager.close.assert_called_once_with()
    # And the scheduler is now in the closed state.
    assert scheduler._queue is None

  def test_enqueue_after_close_raises_runtime_error(
    self, mock_connection_manager, mocker
  ):
    """After close, ``enqueue_request`` raises RuntimeError ("not opened").

    Pins the typed-error contract: close puts the scheduler in a state where
    enqueue fails loudly (not silently) — so a buggy caller that enqueues
    after close can't silently drop the request.
    """
    mock_queue_backend = mock_connection_manager.get_queue_backend()
    mock_queue_backend.name = "test_spider"
    mock_queue_backend.crawler = mocker.MagicMock()

    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    scheduler.open(mock_queue_backend)
    scheduler.close("finished")

    with pytest.raises(RuntimeError, match="Scheduler not opened"):
      scheduler.enqueue_request(Request(url="https://example.com"))


class TestCloseOnNeverOpenedScheduler:
  """Edge: close() on a scheduler that was never opened must not crash.

  Pins the defensive branch: ``self._queue is None`` and
  ``self._connected_signals is None`` at close-time. The close path guards
  both (``if self._queue is not None`` and ``if self._connected_signals
  is not None``), so closing a never-opened scheduler is a safe no-op +
  CM close.
  """

  def test_close_without_open_still_closes_connection_manager(
    self, mock_connection_manager
  ):
    scheduler = BackendScheduler(
      connection_manager=mock_connection_manager,
      queue_key="test:queue",
    )
    # Never opened — _queue and _connected_signals are both None.
    assert scheduler._queue is None

    scheduler.close("finished")  # must NOT raise

    mock_connection_manager.close.assert_called_once_with()
    assert scheduler._spider is None
