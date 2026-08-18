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
4. **Terminal state.** After close, ``_queue``/``_spider``/``_connected_signals``
   are ``None`` and ``_signals_connected`` is ``False``. The scheduler cannot
   be reopened because its single ConnectionManager acquire was released.

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

import pytest
from scrapy.http import Request

from scrapy_extension.schedule import scheduler as scheduler_module
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


def _make_from_crawler_scheduler_with_dupefilter(mocker):
    """Build a scheduler that owns the dupefilter created by ``from_crawler``."""
    manager = mocker.MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(
        connection_manager=manager,
        queue_key="test:queue",
    )
    mocker.patch.object(
        BackendScheduler,
        "from_settings",
        return_value=scheduler,
    )
    dupefilter = mocker.MagicMock(name="OwnedDupeFilter")
    dupefilter_cls = mocker.Mock(name="DupeFilterClass")
    dupefilter_cls.from_crawler.return_value = dupefilter
    mocker.patch(
        "scrapy_extension.schedule.scheduler.load_object",
        return_value=dupefilter_cls,
    )
    crawler = mocker.Mock()
    crawler.settings.get.return_value = "example.OwnedDupeFilter"
    crawler.stats = mocker.Mock()
    return BackendScheduler.from_crawler(crawler), dupefilter, manager


class TestOwnedDupeFilterLifecycle:
    """A scheduler-created dupefilter follows the scheduler's lifecycle."""

    @pytest.mark.parametrize(
        "diagnostic_error",
        [
            RuntimeError("logger unavailable"),
            KeyboardInterrupt("logger interrupted"),
            SystemExit("logger exited"),
        ],
    )
    def test_open_success_diagnostic_interruption_preserves_open_state(
        self, mocker, diagnostic_error: BaseException
    ) -> None:
        """R117: post-publication success logging cannot roll back OPEN state."""
        manager = mocker.MagicMock(name="ConnectionManager")
        scheduler = BackendScheduler(connection_manager=manager, queue_key="test:queue")
        spider = mocker.MagicMock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.MagicMock()
        mocker.patch(
            "scrapy_extension.schedule.scheduler.logger.info",
            side_effect=diagnostic_error,
        )

        assert scheduler.open(spider) is None

        assert scheduler._lifecycle_state == scheduler_module._LIFECYCLE_OPEN
        assert scheduler._queue is not None
        assert scheduler.open(spider) is None
        scheduler.close("finished")
        manager.close.assert_called_once_with()

    def test_open_opens_owned_dupefilter_with_spider(self, mocker):
        scheduler, dupefilter, _ = _make_from_crawler_scheduler_with_dupefilter(mocker)
        spider = mocker.Mock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.Mock()

        scheduler.open(spider)

        dupefilter.open.assert_called_once_with(spider)

    def test_close_closes_owned_dupefilter_with_reason(self, mocker):
        scheduler, dupefilter, _ = _make_from_crawler_scheduler_with_dupefilter(mocker)
        spider = mocker.Mock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.Mock()
        scheduler.open(spider)

        scheduler.close("finished")

        dupefilter.close.assert_called_once_with("finished")

    def test_close_before_open_releases_owned_dupefilter(self, mocker):
        scheduler, dupefilter, manager = _make_from_crawler_scheduler_with_dupefilter(
            mocker
        )

        scheduler.close("startup-failed")

        dupefilter.open.assert_not_called()
        dupefilter.close.assert_called_once_with("startup-failed")
        manager.close.assert_called_once_with()

    def test_repeated_open_and_close_do_not_repeat_releases(self, mocker):
        scheduler, dupefilter, manager = _make_from_crawler_scheduler_with_dupefilter(
            mocker
        )
        spider = mocker.Mock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.Mock()

        scheduler.open(spider)
        first_queue = scheduler._queue
        scheduler.open(spider)
        assert scheduler._queue is first_queue
        scheduler.close("finished")
        scheduler.close("finished-again")

        dupefilter.open.assert_called_once_with(spider)
        dupefilter.close.assert_called_once_with("finished")
        manager.close.assert_called_once_with()
        assert first_queue is not None

    def test_signal_registration_failure_rolls_back_all_owned_resources(self, mocker):
        scheduler, dupefilter, manager = _make_from_crawler_scheduler_with_dupefilter(
            mocker
        )
        signal_manager = mocker.Mock(name="SignalManager")
        signal_manager.connect.side_effect = [
            None,
            RuntimeError("second signal registration failed"),
        ]
        spider = mocker.Mock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.Mock(signals=signal_manager)

        with pytest.raises(RuntimeError, match="second signal registration failed"):
            scheduler.open(spider)

        disconnected = [
            item.args[0].handler for item in signal_manager.disconnect.call_args_list
        ]
        assert disconnected == [
            scheduler._on_response_received,
            scheduler._on_spider_error,
        ]
        dupefilter.open.assert_called_once_with(spider)
        dupefilter.close.assert_called_once_with("open-failed")
        manager.close.assert_called_once_with()
        assert scheduler._queue is None

        with pytest.raises(RuntimeError, match="closed"):
            scheduler.open(spider)

    def test_signal_rollback_baseexception_retries_during_terminal_cleanup(
        self, mocker
    ):
        """A rollback interrupt cannot orphan an already-connected handler."""
        scheduler, dupefilter, manager = _make_from_crawler_scheduler_with_dupefilter(
            mocker
        )
        registration_error = RuntimeError("second signal registration failed")
        rollback_error = KeyboardInterrupt("rollback interrupted")
        signal_manager = mocker.Mock(name="SignalManager")
        signal_manager.connect.side_effect = [None, registration_error]
        signal_manager.disconnect.side_effect = [rollback_error, None, None]
        spider = mocker.Mock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.Mock(signals=signal_manager)

        with pytest.raises(RuntimeError) as raised:
            scheduler.open(spider)

        # The registration failure remains primary. The interrupted cleanup keeps
        # its exact receiver leases and downstream ownership for a later retry.
        assert raised.value is registration_error
        assert signal_manager.disconnect.call_count == 1
        dupefilter.close.assert_not_called()
        manager.close.assert_not_called()

        signal_manager.disconnect.side_effect = None
        scheduler.close("open-failed-retry")
        assert signal_manager.disconnect.call_count == 3
        dupefilter.close.assert_called_once_with("open-failed-retry")
        manager.close.assert_called_once_with()
        assert scheduler._queue is None
        assert scheduler._spider is None
        assert scheduler._connected_signals is None
        assert scheduler._connected_ack_signal_handlers is None
        assert scheduler._signals_connected is False


class TestConstructorSuppliedDupeFilterLifecycle:
    """R44-F1: a dupefilter passed to the public constructor is owned.

    The constructor documents ``dupefilter`` as an optional scheduler dependency
    and provides no caller-managed lifecycle switch. It must therefore receive
    the same open/close lifecycle as a dupefilter created by ``from_crawler``.
    This matters because ``BackendDupeFilter.open(spider)`` resolves ``{spider}``
    key templates and applies clear-on-open before the first fingerprint check.
    """

    def test_constructor_supplied_dupefilter_is_opened_and_closed(
        self, mock_connection_manager, mocker
    ):
        dupefilter = mocker.MagicMock(name="ConstructorDupeFilter")
        scheduler = BackendScheduler(
            connection_manager=mock_connection_manager,
            queue_key="test:queue",
            dupefilter=dupefilter,
        )
        spider = mocker.Mock(name="Spider")
        spider.name = "constructor_spider"
        spider.crawler = mocker.MagicMock()

        scheduler.open(spider)
        dupefilter.open.assert_called_once_with(spider)

        scheduler.close("finished")
        dupefilter.close.assert_called_once_with("finished")

    def test_constructor_without_dupefilter_preserves_noop_lifecycle(
        self, mock_connection_manager, mocker
    ):
        scheduler = BackendScheduler(
            connection_manager=mock_connection_manager,
            queue_key="test:queue",
        )
        spider = mocker.Mock(name="Spider")
        spider.name = "no_dupefilter"
        spider.crawler = mocker.MagicMock()

        scheduler.open(spider)
        scheduler.close("finished")

        mock_connection_manager.close.assert_called_once_with()


class TestOperationCloseRaces:
    """Operations retain the queue selected before a concurrent close."""

    def test_enqueue_uses_captured_queue_after_close_clears_attribute(self, mocker):
        manager = mocker.MagicMock(name="ConnectionManager")
        scheduler = BackendScheduler(manager)
        queue = mocker.MagicMock(name="BackendQueue")
        scheduler._queue = queue
        request = Request("https://example.test")

        def close_after_initial_queue_read(_request):
            scheduler._queue = None

        mocker.patch.object(
            scheduler,
            "_restore_original_errback",
            side_effect=close_after_initial_queue_read,
        )

        assert scheduler.enqueue_request(request) is True
        queue.push.assert_called_once_with(request, priority=0)

    def test_next_request_uses_captured_queue_after_depth_probe(self, mocker):
        manager = mocker.MagicMock(name="ConnectionManager")
        scheduler = BackendScheduler(manager, backpressure_pause_at=10)
        request = Request("https://example.test")

        class ClosingQueue:
            def __len__(self):
                scheduler._queue = None
                return 0

            def pop(self, timeout=0):
                assert timeout == 0
                return request

        scheduler._queue = ClosingQueue()

        assert scheduler.next_request() is request


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
        disconnected = [
            item.args[0].handler for item in signals_mock.disconnect.call_args_list
        ]
        assert scheduler._on_response_received in disconnected
        assert scheduler._on_spider_error in disconnected


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

            def push(
                self, queue_name, item, *, priority=0.0, delay=0.0, source="default"
            ):  # noqa: ARG002
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
            mock_connection_manager,
            mocker,
            queue_strategy=_RecordingStrategy(mock_connection_manager),
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
            def push(
                self, queue_name, item, *, priority=0.0, delay=0.0, source="default"
            ):  # noqa: ARG002
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
            mock_connection_manager,
            mocker,
            queue_strategy=_ExplodingStrategy(mock_connection_manager),
        )

        # Must NOT raise — the explosion is caught + logged inside close().
        scheduler.close("finished")

        # CM close STILL ran despite the strategy explosion.
        mock_connection_manager.close.assert_called_once_with()
        # State was STILL reset.
        assert scheduler._queue is None
        assert scheduler._spider is None


class TestStrategyCloseBaseExceptionStillReleasesManager:
    """R26-G: a ``BaseException`` (Ctrl+C / SystemExit) during teardown must NOT
    skip the ``connection_manager.close()`` release.

    The pre-R26-G close path guarded the 3 teardown steps (signal disconnect,
    queue.close, dupefilter.close) with ``except Exception`` — which does NOT
    cover ``KeyboardInterrupt`` / ``SystemExit``. A BaseException escaping any of
    those skipped ``connection_manager.close()`` entirely, pinning the manager
    (its ``_users`` never decrements → socket/fd leak, registry pin toward
    ``MAX_MANAGERS = 32``). R13/PR#54's close guard was complete on the Exception
    axis but incomplete on the BaseException axis. R26-G mirrors pipeline R20-B:
    capture the first BaseException into ``primary_error``, run the CM release in
    a ``finally``, re-raise ``primary_error`` last.
    """

    def test_strategy_close_base_exception_still_releases_manager(
        self, mock_connection_manager, mocker
    ):
        """A ``KeyboardInterrupt`` during strategy close is re-raised, but the
        connection manager is STILL released (the BaseException axis of the close
        guard). Pre-fix, the CM release was skipped."""
        from scrapy_extension.queue.strategies.base import QueueStrategy

        class _KeyboardInterruptingStrategy(QueueStrategy):
            def push(
                self, queue_name, item, *, priority=0.0, delay=0.0, source="default"
            ):  # noqa: ARG002
                pass

            def pop(self, queue_name, timeout=0.0):  # noqa: ARG002
                return None

            def queue_len(self, queue_name):  # noqa: ARG002
                return 0

            def clear(self, queue_name):  # noqa: ARG002
                pass

            def close(self) -> None:
                raise KeyboardInterrupt("simulated Ctrl+C during strategy close")

        scheduler, _ = _make_scheduler_with_queue(
            mock_connection_manager,
            mocker,
            queue_strategy=_KeyboardInterruptingStrategy(mock_connection_manager),
        )

        # The KeyboardInterrupt is re-raised (primary_error), NOT swallowed.
        with pytest.raises(KeyboardInterrupt):
            scheduler.close("finished")

        # CRITICAL: the connection manager was STILL released despite the
        # BaseException during strategy close. Pre-fix this was skipped.
        mock_connection_manager.close.assert_called_once_with()


class TestEnqueueBaseExceptionDuringConsumeReservationForgetsFingerprint:
    """R34 / SCHED-EXC-CATCH-1: a ``BaseException`` (Ctrl+C / SystemExit) during
    a legacy dupefilter's ``consume_reservation`` must STILL call ``forget()`` so
    the fingerprint ``request_seen`` just recorded does not become permanent.

    WHY THIS MATTERS (intent, not just behavior): the bundled ``BackendDupeFilter``
    is an add-on-check filter — ``request_seen`` records the fingerprint into the
    membership set at the moment of the check (``_request_seen_unlocked``:
    ``self._filter.add(encoded_fingerprint)``). The scheduler then calls
    ``consume_reservation`` to learn whether a reservation was actually written;
    ``dedup_reserved`` is the gate every cleanup arm uses to decide whether to
    call ``forget``. Pre-R34, ``dedup_reserved`` was assigned AFTER the
    interruptible ``consume_reservation(request)`` call, so a BaseException during
    that call left it ``False`` and the ``except BaseException`` rollback gate
    never fired. Result: the recorded fingerprint was never forgotten (ghost) and,
    because the push never happened either, the URL was permanently lost with a
    marker blocking redelivery — a direct violation of the documented at-least-
    once policy ("accept possible replay rather than leave a permanent ghost
    fingerprint").

    This test would fail to encode anything meaningful if ``consume_reservation``
    raising were indistinguishable from ``request_seen`` raising — so the custom
    filter records the fingerprint (returns not-seen) BEFORE the interrupting call.
    """

    def test_keyboard_interrupt_during_consume_reservation_calls_forget(
        self, mock_connection_manager, mocker
    ):
        """Pre-fix: ``forget`` is NOT called (gate inactive). Post-fix: called once."""

        class _AddOnCheckInterruptingDupeFilter:
            """Legacy add-on-check dupefilter whose consume_reservation is interrupted.

            Mirrors the SADD-style custom filter (and the bundled BackendDupeFilter's
            own legacy arm): ``request_seen`` records the fingerprint and returns
            not-seen; ``consume_reservation`` would normally report the reservation,
            but here it is interrupted by a process-control signal.
            """

            def __init__(self) -> None:
                self.request_seen_count = 0
                self.forget = mocker.MagicMock(name="forget")

            # No request_seen_with_reservation / commit_reservation /
            # rollback_reservation → _atomic_dupefilter_methods returns None → the
            # scheduler takes the legacy (non-atomic) arm under test.
            def request_seen(self, request):  # noqa: ARG002
                # Add-on-check: records the fingerprint (simulated), returns not-seen.
                self.request_seen_count += 1
                return False

            def consume_reservation(self, request):  # noqa: ARG002
                raise KeyboardInterrupt("simulated Ctrl+C during consume_reservation")

        dupefilter = _AddOnCheckInterruptingDupeFilter()
        scheduler, _ = _make_scheduler_with_queue(mock_connection_manager, mocker)
        # Inject the legacy dupefilter after open() (the fixture leaves it None).
        scheduler.dupefilter = dupefilter

        request = Request(url="https://example.com/r34-ghost")

        # The KeyboardInterrupt is re-raised (primary signal preserved), NOT
        # swallowed — consistent with the at-least-once cleanup contract.
        with pytest.raises(KeyboardInterrupt):
            scheduler.enqueue_request(request)

        # The fingerprint was recorded by request_seen exactly once.
        assert dupefilter.request_seen_count == 1
        # CRITICAL: forget() was called despite the BaseException landing during
        # consume_reservation — pre-fix, dedup_reserved was still False here and
        # this assertion failed (the ghost-fingerprint window).
        dupefilter.forget.assert_called_once_with(request)


class TestSignalDisconnectFailureIsNonFatal:
    """Behavior #5 (exception-safety symmetry): a signal ``disconnect()`` raising
    does NOT block ``queue.close()`` (snapshot persist) or ``connection_manager.close()``.

    Symmetric with ``TestStrategyCloseFailureIsNonFatal`` (#3). The close path
    already guarded ``self._queue.close()`` with try/except (lines 654-658), but
    the signal-disconnect block that PRECEDES it (lines 642-650) was unguarded —
    so a raise from ``disconnect`` (realistic via pydispatch ``DispatcherKeyError``
    on a stale/already-disconnected tuple, e.g. double-close after a partial engine
    teardown, or a signal manager already torn down) skipped the queue snapshot
    persist AND leaked the backend connection. This pins the symmetry fix: the
    disconnect block is now guarded, so the queue/CM close + state-reset tail
    always runs.
    """

    def test_signal_disconnect_raising_still_closes_queue_and_cm(
        self, mock_connection_manager, mocker
    ):
        """A ``disconnect()`` that raises is swallowed; queue.close() + CM close still run."""
        from scrapy_extension.queue.strategies.base import QueueStrategy

        call_log: list[str] = []

        class _RecordingStrategy(QueueStrategy):
            def push(
                self, queue_name, item, *, priority=0.0, delay=0.0, source="default"
            ):  # noqa: ARG002
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
            mock_connection_manager,
            mocker,
            queue_strategy=_RecordingStrategy(mock_connection_manager),
        )

        # After open(), _connected_signals is the crawler's signal manager. Make its
        # disconnect raise as pydispatch does for a stale/already-disconnected tuple
        # (DispatcherKeyError). The fix catches Exception, so any raise exercises it.
        assert scheduler._connected_signals is not None
        scheduler._connected_signals.disconnect.side_effect = RuntimeError(
            "stale tuple (already disconnected)"
        )

        # A non-absence failure remains owned and prevents terminal publication.
        with pytest.raises(RuntimeError, match="stale tuple"):
            scheduler.close("finished")
        assert call_log == ["strategy_close"]
        assert scheduler._lifecycle_state == "closing"

        scheduler._connected_signals.disconnect.side_effect = None
        scheduler.close("retry")
        assert call_log == ["strategy_close", "cm_close"]
        # State was STILL reset — no re-entry poison left for a second open().
        assert scheduler._queue is None
        assert scheduler._spider is None
        assert scheduler._connected_signals is None
        assert scheduler._signals_connected is False

    def test_first_disconnect_failure_does_not_skip_second_handler(
        self, mock_connection_manager, mocker
    ):
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
        signals_mock.disconnect.side_effect = RuntimeError(
            "response handler already disconnected"
        )

        with pytest.raises(RuntimeError, match="response handler"):
            scheduler.close("finished")
        assert signals_mock.disconnect.call_count == 1

        signals_mock.disconnect.side_effect = None
        scheduler.close("retry")
        assert signals_mock.disconnect.call_count == 3
        disconnected = [
            item.args[0].handler for item in signals_mock.disconnect.call_args_list
        ]
        assert scheduler._on_response_received in disconnected
        assert scheduler._on_spider_error in disconnected


class TestTerminalLifecycle:
    """Behavior #4: close is terminal after releasing the manager acquire."""

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

    def test_scheduler_rejects_reopen_after_close(
        self, mock_connection_manager, mocker
    ):
        """A closed scheduler cannot use its already-released manager again."""
        spider1 = mock_connection_manager.get_queue_backend()
        spider1.name = "spider_one"
        spider1.crawler = mocker.MagicMock()

        scheduler = BackendScheduler(
            connection_manager=mock_connection_manager,
            queue_key="test:queue",
        )
        scheduler.open(spider1)
        assert scheduler._signals_connected is True
        scheduler.close("finished")
        assert scheduler._signals_connected is False

        spider2 = mocker.MagicMock()
        spider2.name = "spider_two"
        spider2.crawler = mocker.MagicMock()

        with pytest.raises(RuntimeError, match="closed"):
            scheduler.open(spider2)

        spider2.crawler.signals.connect.assert_not_called()
        mock_connection_manager.close.assert_called_once_with()

    def test_open_scheduler_rejects_a_different_spider(
        self, mock_connection_manager, mocker
    ):
        spider1 = mocker.MagicMock(name="FirstSpider")
        spider1.name = "spider_one"
        spider1.crawler = mocker.MagicMock()
        spider2 = mocker.MagicMock(name="SecondSpider")
        spider2.name = "spider_two"
        spider2.crawler = mocker.MagicMock()
        scheduler = BackendScheduler(
            connection_manager=mock_connection_manager,
            queue_key="test:queue",
        )
        scheduler.open(spider1)
        first_queue = scheduler._queue

        with pytest.raises(RuntimeError, match="different spider"):
            scheduler.open(spider2)

        assert scheduler._queue is first_queue
        assert scheduler._spider is spider1
        spider2.crawler.signals.connect.assert_not_called()
        mock_connection_manager.close.assert_not_called()
        scheduler.close("finished")


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


class TestStateResetsAfterBaseExceptionTeardown:
    """R35-F7: ``_close_locked`` state-reset tail must run even when a
    ``BaseException`` (Ctrl+C / SystemExit) escapes teardown mid-``try``.

    R26-G captures the ``connection_manager.close()`` release via a
    ``finally`` block, but the state-reset tail (``_queue = None``,
    ``_spider = None``, ``_connected_signals = None``,
    ``_signals_connected = False``, ``_backpressure_paused = False``,
    ``_backpressure_probe_due = False``) lives INSIDE the guarded ``try``
    at lines 1510-1515 — so a ``BaseException`` landing during signal
    disconnect, ``queue.close()``, or owned ``dupefilter.close()`` skips
    the tail entirely. The ``finally`` at 1521 only handles manager
    release; re-entry via ``close()`` short-circuits at 1464 because
    ``_lifecycle_state`` is set to ``_LIFECYCLE_CLOSED`` before the
    ``try``. Stale ``_queue``/``_spider``/``_connected_signals`` and a
    dirty ``_signals_connected=True`` survive until GC, and any signal
    handler not yet iterated in the disconnect loop remains registered.

    Triggering surfaces (all realistic in production):

    - per-handler ``signal.disconnect`` raising ``BaseException``
      (custom signal_manager subclass);
    - ``queue.close()`` raising ``BaseException`` via a custom
      queue/strategy override;
    - owned ``dupefilter.close()`` raising ``BaseException`` via a
      custom filter override.

    These tests pin the post-fix invariant: the state-reset tail runs
    on every code path, idempotent, regardless of how teardown aborts.
    """

    def test_state_resets_when_strategy_close_raises_keyboardinterrupt(
        self, mock_connection_manager, mocker
    ):
        """``KeyboardInterrupt`` from strategy.close() must not leave stale refs.

        Pre-fix: state-reset tail lives in the guarded try; BaseException
        capture at line 1516 skips it. Post-fix: tail in finally runs on
        every path.
        """
        from scrapy_extension.queue.strategies.base import QueueStrategy

        class _KeyboardInterruptingStrategy(QueueStrategy):
            def push(
                self, queue_name, item, *, priority=0.0, delay=0.0, source="default"
            ):  # noqa: ARG002
                pass

            def pop(self, queue_name, timeout=0.0):  # noqa: ARG002
                return None

            def queue_len(self, queue_name):  # noqa: ARG002
                return 0

            def clear(self, queue_name):  # noqa: ARG002
                pass

            def close(self) -> None:
                raise KeyboardInterrupt("simulated Ctrl+C during strategy close")

        scheduler, _ = _make_scheduler_with_queue(
            mock_connection_manager,
            mocker,
            queue_strategy=_KeyboardInterruptingStrategy(mock_connection_manager),
        )

        # Pre-condition: open() populated state.
        assert scheduler._queue is not None
        assert scheduler._spider is not None
        assert scheduler._signals_connected is True

        # Primary signal is re-raised (R26-G).
        with pytest.raises(KeyboardInterrupt):
            scheduler.close("test-done")

        # R26-G: connection_manager still released exactly once.
        mock_connection_manager.close.assert_called_once_with()

        # R35-F7: state-reset tail runs even when the BaseException aborts
        # teardown. Pre-fix these assertions fail because the tail lives
        # inside the guarded try.
        assert scheduler._queue is None
        assert scheduler._spider is None
        assert scheduler._connected_signals is None
        assert scheduler._signals_connected is False
        assert scheduler._backpressure_paused is False
        assert scheduler._backpressure_probe_due is False

    def test_state_resets_when_signal_disconnect_raises_baseexception(
        self, mock_connection_manager, mocker
    ):
        """A ``BaseException`` from ``signal_manager.disconnect`` must not leave
        stale ``_connected_signals`` / ``_signals_connected`` / ``_queue`` /
        ``_spider`` behind."""
        scheduler, _ = _make_scheduler_with_queue(mock_connection_manager, mocker)

        assert scheduler._connected_signals is not None
        # The disconnect loop is the FIRST teardown step; BaseException here
        # raises out of the try at line 1485 before queue/dupefilter close.
        scheduler._connected_signals.disconnect.side_effect = KeyboardInterrupt(
            "simulated Ctrl+C during signal disconnect"
        )

        with pytest.raises(KeyboardInterrupt):
            scheduler.close("test-done")

        mock_connection_manager.close.assert_not_called()
        assert scheduler._queue is not None
        assert scheduler._spider is not None
        assert scheduler._connected_signals is not None
        assert scheduler._signals_connected is True
        assert scheduler._lifecycle_state == "closing"

        scheduler._connected_signals.disconnect.side_effect = None
        scheduler.close("retry")
        mock_connection_manager.close.assert_called_once_with()
        assert scheduler._queue is None
        assert scheduler._spider is None
        assert scheduler._connected_signals is None
        assert scheduler._signals_connected is False

    def test_signal_interrupt_still_closes_queue_and_owned_dupefilter(self, mocker):
        scheduler, dupefilter, manager = _make_from_crawler_scheduler_with_dupefilter(
            mocker
        )
        spider = mocker.MagicMock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.MagicMock()
        scheduler.open(spider)
        assert scheduler._queue is not None
        queue_close = mocker.patch.object(scheduler._queue, "close")
        first = KeyboardInterrupt()
        assert scheduler._connected_signals is not None
        connected_signals = scheduler._connected_signals
        connected_signals.disconnect.side_effect = [first, None]

        with pytest.raises(KeyboardInterrupt) as raised:
            scheduler.close("test-done")

        assert raised.value is first
        assert connected_signals.disconnect.call_count == 1
        queue_close.assert_called_once_with()
        dupefilter.close.assert_not_called()
        manager.close.assert_not_called()

        connected_signals.disconnect.side_effect = None
        scheduler.close("retry")
        assert connected_signals.disconnect.call_count == 3
        queue_close.assert_called_once_with()
        dupefilter.close.assert_called_once_with("retry")
        manager.close.assert_called_once_with()

    def test_queue_checkpoint_interrupt_retains_owned_dupefilter(self, mocker):
        scheduler, dupefilter, manager = _make_from_crawler_scheduler_with_dupefilter(
            mocker
        )
        spider = mocker.MagicMock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.MagicMock()
        scheduler.open(spider)
        assert scheduler._queue is not None
        first = KeyboardInterrupt()
        mocker.patch.object(scheduler._queue, "close", side_effect=first)

        with pytest.raises(KeyboardInterrupt) as raised:
            scheduler.close("test-done")

        assert raised.value is first
        dupefilter.close.assert_not_called()
        manager.close.assert_not_called()
        assert scheduler._queue is not None

    def test_state_resets_when_owned_dupefilter_close_raises_keyboardinterrupt(
        self, mocker
    ):
        """A ``BaseException`` from an OWNED dupefilter.close() must not leave
        stale refs. This goes through ``from_crawler`` so the scheduler owns
        the dupefilter (the trigger surface for the owned arm at line 1505).
        """
        manager = mocker.MagicMock(name="ConnectionManager")
        scheduler = BackendScheduler(
            connection_manager=manager,
            queue_key="test:queue",
        )
        mocker.patch.object(
            BackendScheduler,
            "from_settings",
            return_value=scheduler,
        )
        dupefilter = mocker.MagicMock(name="OwnedDupeFilter")
        dupefilter.close.side_effect = KeyboardInterrupt(
            "simulated Ctrl+C during owned dupefilter close"
        )
        dupefilter_cls = mocker.Mock(name="DupeFilterClass")
        dupefilter_cls.from_crawler.return_value = dupefilter
        mocker.patch(
            "scrapy_extension.schedule.scheduler.load_object",
            return_value=dupefilter_cls,
        )
        crawler = mocker.Mock()
        crawler.settings.get.return_value = "example.OwnedDupeFilter"
        crawler.stats = mocker.Mock()
        scheduler_with_df = BackendScheduler.from_crawler(crawler)

        # Wire a fake queue + spider + signals so the close path reaches the
        # owned dupefilter arm. Mirrors what open() would have done.
        fake_queue = mocker.MagicMock(name="BackendQueue")
        fake_queue.close.return_value = None
        scheduler_with_df._queue = fake_queue
        fake_spider = mocker.MagicMock(name="Spider")
        fake_spider.name = "test_spider"
        fake_spider.crawler = mocker.MagicMock()
        scheduler_with_df._spider = fake_spider
        scheduler_with_df._connected_signals = mocker.MagicMock(name="SignalManager")
        scheduler_with_df._signals_connected = True
        scheduler_with_df._owns_dupefilter = True
        scheduler_with_df._dupefilter_released = False

        with pytest.raises(KeyboardInterrupt):
            scheduler_with_df.close("test-done")

        manager.close.assert_not_called()
        assert scheduler_with_df._queue is fake_queue
        assert scheduler_with_df._spider is fake_spider
        assert scheduler_with_df._lifecycle_state == "closing"

        dupefilter.close.side_effect = None
        scheduler_with_df.close("retry")
        manager.close.assert_called_once_with()
        assert scheduler_with_df._queue is None
        assert scheduler_with_df._spider is None
        assert scheduler_with_df._connected_signals is None
        assert scheduler_with_df._signals_connected is False


class TestCloseDiagnosticsAreNonFatal:
    """Shutdown logging is observational and cannot own resource teardown."""

    def test_initial_closed_log_keyboardinterrupt_still_releases_every_resource(
        self, mocker
    ):
        """The lifecycle ``info`` call runs after CLOSED is recorded, so a
        control exception from a logging handler must not strand this terminal
        scheduler before signals, queue, dupefilter, and manager are released.
        """
        scheduler, dupefilter, manager = _make_from_crawler_scheduler_with_dupefilter(
            mocker
        )
        spider = mocker.MagicMock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.MagicMock()
        scheduler.open(spider)
        assert scheduler._queue is not None
        queue_close = mocker.patch.object(scheduler._queue, "close")
        assert scheduler._connected_signals is not None
        connected_signals = scheduler._connected_signals
        mocker.patch(
            "scrapy_extension.schedule.scheduler.logger.info",
            side_effect=KeyboardInterrupt("interrupted logging handler"),
        )

        scheduler.close("test-done")

        assert connected_signals.disconnect.call_count == 2
        queue_close.assert_called_once_with()
        dupefilter.close.assert_called_once_with("test-done")
        manager.close.assert_called_once_with()

    def test_exception_log_keyboardinterrupt_does_not_skip_later_cleanup(self, mocker):
        """A normal signal-disconnect failure is only diagnostic; even if its
        exception logger is interrupted, the sibling handler and later teardown
        phases still run.
        """
        scheduler, dupefilter, manager = _make_from_crawler_scheduler_with_dupefilter(
            mocker
        )
        spider = mocker.MagicMock(name="Spider")
        spider.name = "test_spider"
        spider.crawler = mocker.MagicMock()
        scheduler.open(spider)
        assert scheduler._queue is not None
        queue_close = mocker.patch.object(scheduler._queue, "close")
        assert scheduler._connected_signals is not None
        connected_signals = scheduler._connected_signals
        connected_signals.disconnect.side_effect = RuntimeError("stale")

        with pytest.raises(RuntimeError, match="stale"):
            scheduler.close("test-done")
        assert connected_signals.disconnect.call_count == 1
        queue_close.assert_called_once_with()
        dupefilter.close.assert_not_called()
        manager.close.assert_not_called()

        connected_signals.disconnect.side_effect = None
        scheduler.close("retry")
        assert connected_signals.disconnect.call_count == 3
        dupefilter.close.assert_called_once_with("retry")
        manager.close.assert_called_once_with()

    def test_state_resets_when_reentrant_close_after_baseexception(
        self, mock_connection_manager, mocker
    ):
        """A second ``close()`` after a BaseException teardown short-circuits
        (no re-release), and the first ``close()`` must already have run the
        state-reset tail. Re-entry cannot re-run idempotent assignments
        (no-op on already-None), so this is a stability guard for the fix.
        """
        from scrapy_extension.queue.strategies.base import QueueStrategy

        class _BoomStrategy(QueueStrategy):
            def push(
                self, queue_name, item, *, priority=0.0, delay=0.0, source="default"
            ):  # noqa: ARG002
                pass

            def pop(self, queue_name, timeout=0.0):  # noqa: ARG002
                return None

            def queue_len(self, queue_name):  # noqa: ARG002
                return 0

            def clear(self, queue_name):  # noqa: ARG002
                pass

            def close(self) -> None:
                raise KeyboardInterrupt("simulated Ctrl+C")

        scheduler, _ = _make_scheduler_with_queue(
            mock_connection_manager,
            mocker,
            queue_strategy=_BoomStrategy(mock_connection_manager),
        )

        with pytest.raises(KeyboardInterrupt):
            scheduler.close("first")

        # First close released CM exactly once.
        assert mock_connection_manager.close.call_count == 1
        # State already reset.
        assert scheduler._queue is None
        assert scheduler._spider is None
        assert scheduler._connected_signals is None
        assert scheduler._signals_connected is False

        # Second close is a no-op (lifecycle_state == CLOSED short-circuit).
        scheduler.close("second")
        assert mock_connection_manager.close.call_count == 1  # no double release
        # State idempotently None.
        assert scheduler._queue is None
        assert scheduler._spider is None
