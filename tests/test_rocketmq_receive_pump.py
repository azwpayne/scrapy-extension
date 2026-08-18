"""Deterministic concurrency contracts for the RocketMQ receive pump."""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.rocketmq import RocketMQBackend, _RocketMQAckToken
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings import RocketMQSettings

pytestmark = pytest.mark.usefixtures("cleanup_rocketmq_backends")


def _connected_backend() -> tuple[RocketMQBackend, MagicMock, MagicMock]:
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    return backend, producer, consumer


def test_start_failure_is_redacted_and_allows_clean_disconnect_then_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend, producer, consumer = _connected_backend()
    original_start = threading.Thread.start
    receive_start_attempts = 0

    def fail_first_receive_start(worker: threading.Thread) -> None:
        nonlocal receive_start_attempts
        if worker.name.startswith("rocketmq-receive-"):
            receive_start_attempts += 1
            if receive_start_attempts == 1:
                raise RuntimeError("private thread startup detail")
        original_start(worker)

    monkeypatch.setattr(threading.Thread, "start", fail_first_receive_start)

    with pytest.raises(QueueError) as exc_info:
        backend.pop("jobs", timeout=1)

    assert str(exc_info.value) == "Failed to pop RocketMQ message."
    assert exc_info.value.__cause__ is None
    assert "private thread startup detail" not in str(exc_info.value)
    assert backend._receive_worker is None

    started = time.monotonic()
    backend.disconnect()
    assert time.monotonic() - started < 1
    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()

    message = MagicMock(body=b"recovered")
    second_consumer = MagicMock(is_running=True)
    second_consumer.receive.return_value = [message]
    backend._producer = MagicMock(is_running=True)
    backend._consumer = second_consumer
    backend._consumer_generation += 1

    assert backend.pop("replacement", timeout=1) == b"recovered"
    assert receive_start_attempts == 2
    backend.disconnect()


def test_zero_timeout_never_waits_for_blocked_broker_receive() -> None:
    backend, _, consumer = _connected_backend()
    receive_entered = threading.Event()
    release_receive = threading.Event()

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        assert release_receive.wait(timeout=2)
        return []

    consumer.receive.side_effect = receive

    assert backend.pop("jobs", timeout=0) is None
    assert receive_entered.wait(timeout=1)
    for _ in range(10):
        assert backend.pop("jobs", timeout=0) is None
    assert backend._receive_demand == 1

    release_receive.set()
    backend.disconnect()


def test_positive_timeout_is_woken_by_pumped_delivery() -> None:
    backend, _, consumer = _connected_backend()
    receive_entered = threading.Event()
    release_receive = threading.Event()
    message = MagicMock(body=b"payload")
    result: list[tuple[bytes | None, Any | None]] = []

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        assert release_receive.wait(timeout=2)
        return [message]

    consumer.receive.side_effect = receive
    waiter = threading.Thread(
        target=lambda: result.append(backend.pop_with_ack("jobs", timeout=1))
    )
    waiter.start()
    assert receive_entered.wait(timeout=1)
    assert result == []

    release_receive.set()
    waiter.join(timeout=2)

    assert not waiter.is_alive()
    assert result[0][0] == b"payload"
    token = result[0][1]
    assert isinstance(token, _RocketMQAckToken)
    assert token.consumer is consumer
    assert token.generation == 1
    assert token.message is message
    backend.disconnect()


def test_second_pop_wakes_idle_pump_for_a_second_broker_cycle() -> None:
    backend, _, consumer = _connected_backend()
    first_receive_entered = threading.Event()
    release_first_receive = threading.Event()
    second_receive_entered = threading.Event()
    release_second_receive = threading.Event()
    message = MagicMock(body=b"second-cycle")
    call_count = 0

    def receive(_maximum: int, _lease: int) -> list[object]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            first_receive_entered.set()
            assert release_first_receive.wait(timeout=2)
            return []
        if call_count == 2:
            second_receive_entered.set()
            assert release_second_receive.wait(timeout=2)
            return [message]
        return []

    consumer.receive.side_effect = receive
    assert backend.pop("jobs", timeout=0) is None
    assert first_receive_entered.wait(timeout=1)
    release_first_receive.set()
    with backend._receive_condition:
        assert backend._receive_condition.wait_for(
            lambda: backend._receive_cycle == 1 and backend._receive_demand == 0,
            timeout=1,
        )

    result: list[bytes | None] = []
    waiter = threading.Thread(
        target=lambda: result.append(backend.pop("jobs", timeout=1))
    )
    waiter.start()
    assert second_receive_entered.wait(timeout=1)
    release_second_receive.set()
    waiter.join(timeout=2)

    assert not waiter.is_alive()
    assert result == [b"second-cycle"]
    assert consumer.receive.call_count == 2
    backend.disconnect()


def test_generation_rejects_second_topic_before_broker_work() -> None:
    backend, _, consumer = _connected_backend()
    subscribe_entered = threading.Event()
    release_subscribe = threading.Event()

    def subscribe(_topic: str) -> None:
        subscribe_entered.set()
        assert release_subscribe.wait(timeout=2)

    consumer.subscribe.side_effect = subscribe
    assert backend.pop("queue_a", timeout=0) is None
    assert subscribe_entered.wait(timeout=1)

    with pytest.raises(QueueError, match="different queue"):
        backend.pop("queue_b", timeout=0)

    consumer.subscribe.assert_called_once_with("scrapy-queue_queue_a")
    consumer.receive.assert_not_called()
    release_subscribe.set()
    backend.disconnect()


def test_reconnect_may_select_a_new_topic() -> None:
    backend, _, first_consumer = _connected_backend()
    first_receive = threading.Event()

    def receive_first(_maximum: int, _lease: int) -> list[object]:
        first_receive.set()
        return []

    first_consumer.receive.side_effect = receive_first
    assert backend.pop("queue_a", timeout=0) is None
    assert first_receive.wait(timeout=1)
    backend.disconnect()

    second_consumer = MagicMock(is_running=True)
    backend._producer = MagicMock(is_running=True)
    backend._consumer = second_consumer
    backend._consumer_generation += 1
    second_receive = threading.Event()

    def receive_second(_maximum: int, _lease: int) -> list[object]:
        second_receive.set()
        return []

    second_consumer.receive.side_effect = receive_second
    assert backend.pop("queue_b", timeout=0) is None
    assert second_receive.wait(timeout=1)
    second_consumer.subscribe.assert_called_once_with("scrapy-queue_queue_b")
    backend.disconnect()


def test_disconnect_interrupts_and_joins_receive_worker() -> None:
    backend, _, consumer = _connected_backend()
    receive_entered = threading.Event()
    interrupted = threading.Event()

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        assert interrupted.wait(timeout=2)
        return []

    consumer.receive.side_effect = receive
    consumer.shutdown.side_effect = interrupted.set

    assert backend.pop("jobs", timeout=0) is None
    assert receive_entered.wait(timeout=1)
    worker = backend._receive_worker
    assert worker is not None and worker.is_alive()

    backend.disconnect()

    assert not worker.is_alive()
    consumer.shutdown.assert_called_once_with()
    consumer.ack.assert_not_called()
    assert backend._receive_worker is None
    assert list(backend._receive_buffer) == []


def test_pump_failure_is_sticky_for_every_concurrent_waiter() -> None:
    backend, _, consumer = _connected_backend()
    receive_entered = threading.Event()
    release_receive = threading.Event()
    all_waiters_blocked = threading.Event()
    waiter_count = 4
    blocked_waiters = 0
    blocked_lock = threading.Lock()
    original_wait = backend._receive_condition.wait

    def tracked_wait(timeout: float | None = None) -> bool:
        nonlocal blocked_waiters
        if threading.current_thread().name.startswith("rocketmq-waiter-"):
            with blocked_lock:
                blocked_waiters += 1
                if blocked_waiters == waiter_count:
                    all_waiters_blocked.set()
        return original_wait(timeout)

    backend._receive_condition.wait = tracked_wait

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        assert release_receive.wait(timeout=2)
        raise RuntimeError("private broker failure")

    outcomes: list[str] = []

    def pop() -> None:
        try:
            backend.pop("jobs", timeout=2)
        except QueueError:
            outcomes.append("error")
        else:  # pragma: no cover - explicit false-empty regression signal
            outcomes.append("empty")

    consumer.receive.side_effect = receive
    waiters = [
        threading.Thread(target=pop, name=f"rocketmq-waiter-{index}")
        for index in range(waiter_count)
    ]
    for waiter in waiters:
        waiter.start()

    assert receive_entered.wait(timeout=1)
    assert all_waiters_blocked.wait(timeout=1)
    release_receive.set()
    for waiter in waiters:
        waiter.join(timeout=2)

    assert all(not waiter.is_alive() for waiter in waiters)
    assert outcomes == ["error"] * waiter_count
    with pytest.raises(QueueError):
        backend.pop("jobs", timeout=0)
    consumer.receive.assert_called_once_with(1, 300)
    backend.disconnect()


def test_reconnect_restarts_after_terminal_pump_failure() -> None:
    backend, _, first_consumer = _connected_backend()
    first_consumer.receive.side_effect = RuntimeError("first generation failed")

    with pytest.raises(QueueError):
        backend.pop("jobs", timeout=1)
    with pytest.raises(QueueError):
        backend.pop("jobs", timeout=0)
    first_consumer.receive.assert_called_once_with(1, 300)
    backend.disconnect()

    message = MagicMock(body=b"replacement")
    second_consumer = MagicMock(is_running=True)
    second_consumer.receive.return_value = [message]
    backend._producer = MagicMock(is_running=True)
    backend._consumer = second_consumer
    backend._consumer_generation += 1

    assert backend.pop("replacement", timeout=1) == b"replacement"
    second_consumer.subscribe.assert_called_once_with("scrapy-queue_replacement")
    backend.disconnect()


def test_disconnect_bounds_stuck_pump_and_fences_late_publication() -> None:
    backend, _, consumer = _connected_backend()
    receive_entered = threading.Event()
    release_receive = threading.Event()
    stale_message = MagicMock(body=b"stale")

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        release_receive.wait()
        return [stale_message]

    consumer.receive.side_effect = receive
    consumer.shutdown.side_effect = RuntimeError("shutdown failed")
    assert backend.pop("jobs", timeout=0) is None
    assert receive_entered.wait(timeout=1)
    stale_worker = backend._receive_worker
    assert stale_worker is not None

    started = time.monotonic()
    with pytest.raises(BackendConnectionError, match="Failed to disconnect"):
        backend.disconnect()
    assert time.monotonic() - started < 2
    assert stale_worker.is_alive()
    assert backend._receive_worker is None

    replacement = MagicMock(body=b"fresh")
    second_consumer = MagicMock(is_running=True)
    second_consumer.receive.return_value = [replacement]
    backend._producer = MagicMock(is_running=True)
    backend._consumer = second_consumer
    backend._consumer_generation += 1
    assert backend.pop("fresh", timeout=1) == b"fresh"

    release_receive.set()
    stale_worker.join(timeout=1)
    assert not stale_worker.is_alive()
    assert all(item[0] is not stale_message for item in backend._receive_buffer)
    backend.disconnect()


def test_disconnect_bounds_blocked_shutdown_and_receive_and_fences_both() -> None:
    backend, producer, consumer = _connected_backend()
    receive_entered = threading.Event()
    shutdown_entered = threading.Event()
    release_receive = threading.Event()
    release_shutdown = threading.Event()
    stale_message = MagicMock(body=b"stale")

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        assert release_receive.wait(timeout=5)
        return [stale_message]

    def shutdown() -> None:
        shutdown_entered.set()
        assert release_shutdown.wait(timeout=5)

    consumer.receive.side_effect = receive
    consumer.shutdown.side_effect = shutdown
    assert backend.pop("jobs", timeout=0) is None
    assert receive_entered.wait(timeout=1)
    stale_worker = backend._receive_worker
    assert stale_worker is not None

    started = time.monotonic()
    with pytest.raises(BackendConnectionError, match="Failed to disconnect"):
        backend.disconnect()
    assert time.monotonic() - started < 3
    assert shutdown_entered.is_set()
    assert producer.shutdown.call_count == 1
    assert stale_worker.is_alive()

    replacement = MagicMock(body=b"fresh")
    second_consumer = MagicMock(is_running=True)
    second_consumer.receive.return_value = [replacement]
    backend._producer = MagicMock(is_running=True)
    backend._consumer = second_consumer
    backend._consumer_generation += 1
    assert backend.pop("fresh", timeout=1) == b"fresh"

    release_shutdown.set()
    release_receive.set()
    stale_worker.join(timeout=1)
    assert not stale_worker.is_alive()
    assert all(item[0] is not stale_message for item in backend._receive_buffer)
    backend.disconnect()


def test_disconnect_propagates_control_error_after_bounded_pump_join() -> None:
    backend, producer, consumer = _connected_backend()
    receive_entered = threading.Event()
    release_receive = threading.Event()
    interrupt = KeyboardInterrupt()

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        release_receive.wait()
        return []

    consumer.receive.side_effect = receive
    consumer.shutdown.side_effect = interrupt
    assert backend.pop("jobs", timeout=0) is None
    assert receive_entered.wait(timeout=1)
    worker = backend._receive_worker
    assert worker is not None

    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt) as raised:
        backend.disconnect()
    assert raised.value is interrupt
    assert time.monotonic() - started < 2
    producer.shutdown.assert_called_once_with()

    release_receive.set()
    worker.join(timeout=1)
    assert not worker.is_alive()


def test_live_error_observer_sees_driver_error_without_public_retention() -> None:
    backend, _, consumer = _connected_backend()
    driver_error = RuntimeError("broker detail")
    observed: list[BaseException] = []
    backend._receive_error_observer = observed.append
    consumer.receive.side_effect = driver_error

    with pytest.raises(QueueError) as exc_info:
        backend.pop("jobs", timeout=1)

    assert observed == [driver_error]
    assert exc_info.value.__cause__ is None
    assert "broker detail" not in str(exc_info.value)
    backend.disconnect()


def test_retired_pump_exception_cannot_contaminate_replacement_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "scrapy_extension.backends.rocketmq._RECEIVE_PUMP_JOIN_TIMEOUT_S", 0.01
    )
    backend, _, first_consumer = _connected_backend()
    receive_entered = threading.Event()
    release_receive = threading.Event()
    stale_error = RuntimeError("retired pump failed late")
    observed: list[BaseException] = []
    backend._receive_error_observer = observed.append

    def stale_receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        assert release_receive.wait(timeout=2)
        raise stale_error

    first_consumer.receive.side_effect = stale_receive
    assert backend.pop("jobs", timeout=0) is None
    assert receive_entered.wait(timeout=1)
    stale_worker = backend._receive_worker
    assert stale_worker is not None

    with pytest.raises(BackendConnectionError, match="Failed to disconnect"):
        backend.disconnect()
    assert stale_worker.is_alive()

    fresh_message = MagicMock(body=b"fresh")
    second_consumer = MagicMock(is_running=True)
    second_consumer.receive.return_value = [fresh_message]
    backend._producer = MagicMock(is_running=True)
    backend._consumer = second_consumer
    backend._consumer_generation += 1

    assert backend.pop("fresh", timeout=1) == b"fresh"
    assert backend._receive_failed is False

    release_receive.set()
    stale_worker.join(timeout=1)

    assert not stale_worker.is_alive()
    assert observed == []
    assert backend._receive_failed is False
    backend.disconnect()


def test_disconnect_drops_buffered_delivery_without_ack() -> None:
    backend, _, consumer = _connected_backend()
    message = MagicMock(body=b"undelivered")
    receive_entered = threading.Event()
    release_receive = threading.Event()

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_entered.set()
        assert release_receive.wait(timeout=2)
        return [message]

    consumer.receive.side_effect = receive
    assert backend.pop("jobs", timeout=0) is None
    assert receive_entered.wait(timeout=1)
    release_receive.set()
    with backend._receive_condition:
        assert backend._receive_condition.wait_for(
            lambda: bool(backend._receive_buffer), timeout=1
        )

    backend.disconnect()

    consumer.ack.assert_not_called()
    assert list(backend._receive_buffer) == []
