"""Deterministic regression tests for Pulsar's off-reactor receive pumps."""

from __future__ import annotations

from collections import deque
from threading import Condition, Event, Thread
from time import monotonic
from typing import Any
from unittest.mock import MagicMock

import pulsar

from scrapy_extension.backends.pulsar import PulsarBackend, _PulsarAckToken
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings import PulsarSettings


class _ControllableConsumer:
    """Sync consumer double whose receive stays blocked until delivery or close."""

    def __init__(self) -> None:
        self.receive_started = Event()
        self.closed = Event()
        self._condition = Condition()
        self._deliveries: deque[Any] = deque()
        self._receive_calls = 0
        self.acknowledged: list[Any] = []
        self.nacked: list[Any] = []

    @property
    def receive_calls(self) -> int:
        with self._condition:
            return self._receive_calls

    def receive(self, *, timeout_millis: int) -> Any:
        del timeout_millis
        with self._condition:
            self._receive_calls += 1
            self.receive_started.set()
            while not self._deliveries and not self.closed.is_set():
                self._condition.wait()
            if self.closed.is_set():
                raise RuntimeError("consumer closed")
            return self._deliveries.popleft()

    def deliver(self, message: Any) -> None:
        with self._condition:
            self._deliveries.append(message)
            self._condition.notify_all()

    def acknowledge(self, message_id: Any) -> None:
        self.acknowledged.append(message_id)

    def negative_acknowledge(self, message_id: Any) -> None:
        self.nacked.append(message_id)

    def close(self) -> None:
        with self._condition:
            self.closed.set()
            self._condition.notify_all()


def _message(payload: bytes, message_id: Any) -> MagicMock:
    message = MagicMock(name=f"message-{payload!r}")
    message.data.return_value = payload
    message.message_id.return_value = message_id
    return message


def _connected_backend(mocker: Any, consumers: list[_ControllableConsumer]) -> PulsarBackend:
    client = MagicMock(name="pulsar-client")
    client.subscribe.side_effect = consumers
    mocker.patch.object(pulsar, "Client", return_value=client)
    backend = PulsarBackend(PulsarSettings())
    backend.connect()
    return backend


def _wait_until(predicate: Any, timeout: float = 1.0) -> bool:
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return True
        Event().wait(0.005)
    return bool(predicate())


def test_scheduler_zero_poll_returns_while_sync_receive_is_blocked(mocker: Any) -> None:
    consumer = _ControllableConsumer()
    backend = _connected_backend(mocker, [consumer])
    queue = MagicMock(name="BackendQueue")
    queue.pop.side_effect = lambda timeout: backend.pop("scheduler", timeout=timeout)
    scheduler = BackendScheduler(connection_manager=MagicMock(name="manager"))
    scheduler._queue = queue
    finished = Event()
    result: list[Any] = []

    def poll_scheduler() -> None:
        result.append(scheduler.next_request())
        finished.set()

    poll = Thread(target=poll_scheduler)
    try:
        poll.start()
        assert finished.wait(timeout=0.5)
        assert consumer.receive_started.wait(timeout=0.5)
        assert result == [None]
        assert not consumer.closed.is_set()
        queue.pop.assert_called_once_with(timeout=0)
    finally:
        backend.disconnect()
        poll.join(timeout=1.0)


def test_positive_poll_obeys_budget_while_receive_remains_blocked(mocker: Any) -> None:
    consumer = _ControllableConsumer()
    backend = _connected_backend(mocker, [consumer])
    try:
        started = monotonic()
        assert backend.pop("budget", timeout=0.05) is None
        elapsed = monotonic() - started
        assert elapsed >= 0.04
        assert elapsed < 0.5
        assert consumer.receive_started.is_set()
    finally:
        backend.disconnect()


def test_delivery_keeps_topic_consumer_and_message_id_for_reverse_settlement(
    mocker: Any,
) -> None:
    consumer_a = _ControllableConsumer()
    consumer_b = _ControllableConsumer()
    backend = _connected_backend(mocker, [consumer_a, consumer_b])
    id_a = object()
    id_b = object()
    try:
        assert backend.pop_with_ack("a", timeout=0) == (None, None)
        assert backend.pop_with_ack("b", timeout=0) == (None, None)
        assert consumer_a.receive_started.wait(timeout=0.5)
        assert consumer_b.receive_started.wait(timeout=0.5)

        consumer_a.deliver(_message(b"a", id_a))
        consumer_b.deliver(_message(b"b", id_b))
        value_a, token_a = backend.pop_with_ack("a", timeout=1.0)
        value_b, token_b = backend.pop_with_ack("b", timeout=1.0)

        assert value_a == b"a"
        assert value_b == b"b"
        assert isinstance(token_a, _PulsarAckToken)
        assert isinstance(token_b, _PulsarAckToken)
        assert token_a.consumer is consumer_a and token_a.message_id is id_a
        assert token_b.consumer is consumer_b and token_b.message_id is id_b

        backend.nack("b", token=token_b)
        backend.ack("a", token=token_a)
        assert consumer_b.nacked == [id_b]
        assert consumer_a.acknowledged == [id_a]
        assert consumer_a.nacked == []
        assert consumer_b.acknowledged == []
    finally:
        backend.disconnect()


def test_receive_buffer_is_bounded_per_topic(mocker: Any) -> None:
    consumer = _ControllableConsumer()
    backend = _connected_backend(mocker, [consumer])
    backend._receive_buffer_size = 2
    try:
        assert backend.pop("bounded", timeout=0) is None
        for index in range(3):
            consumer.deliver(_message(str(index).encode(), object()))
        pump = backend._receive_pumps["scrapy-bounded"]
        assert _wait_until(lambda: len(pump.records) == 2)
        assert consumer.receive_calls == 2
        assert len(pump.records) == pump.capacity == 2
    finally:
        backend.disconnect()


def test_disconnect_drops_unreturned_buffer_and_reconnect_fences_generation(
    mocker: Any,
) -> None:
    old_consumer = _ControllableConsumer()
    new_consumer = _ControllableConsumer()
    old_client = MagicMock(name="old-client")
    old_client.subscribe.return_value = old_consumer
    new_client = MagicMock(name="new-client")
    new_client.subscribe.return_value = new_consumer
    client_factory = mocker.patch.object(
        pulsar, "Client", side_effect=[old_client, new_client]
    )
    backend = PulsarBackend(PulsarSettings())
    backend.connect()

    assert backend.pop("queue", timeout=0) is None
    old_pump = backend._receive_pumps["scrapy-queue"]
    old_consumer.deliver(_message(b"old", object()))
    assert old_pump.buffered.wait(timeout=0.5)

    backend.disconnect()

    assert old_consumer.closed.is_set()
    assert old_pump.stopped.is_set()
    assert len(old_pump.records) == 0
    assert old_consumer.acknowledged == []
    assert old_consumer.nacked == []
    assert backend._receive_pumps == {}

    backend.connect()
    try:
        assert backend.pop("queue", timeout=0) is None
        new_pump = backend._receive_pumps["scrapy-queue"]
        assert new_pump is not old_pump
        new_consumer.deliver(_message(b"new", object()))
        assert backend.pop("queue", timeout=1.0) == b"new"
        assert client_factory.call_count == 2
    finally:
        backend.disconnect()
