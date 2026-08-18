"""Deterministic concurrency contracts for the RocketMQ receive pump."""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.rocketmq import RocketMQBackend, _RocketMQAckToken
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import RocketMQSettings


def _connected_backend() -> tuple[RocketMQBackend, MagicMock, MagicMock]:
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    return backend, producer, consumer


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
