"""Deterministic regression tests for Pulsar's off-reactor receive pumps."""

from __future__ import annotations

from collections import deque
from threading import Condition, Event, Thread, current_thread
from threading import enumerate as enumerate_threads
from time import monotonic
from typing import Any
from unittest.mock import MagicMock, call

import pulsar
import pytest

import scrapy_extension.backends.pulsar as pulsar_backend_module
from scrapy_extension.backends.pulsar import PulsarBackend, _PulsarAckToken
from scrapy_extension.exceptions import QueueError
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings import PulsarSettings

_CONNECTED_BACKENDS: list[PulsarBackend] = []


@pytest.fixture(autouse=True)
def _disconnect_receive_pumps_after_test() -> Any:
    """Keep every receive/close daemon owned by a backend inside its test."""
    yield
    while _CONNECTED_BACKENDS:
        backend = _CONNECTED_BACKENDS.pop()
        try:
            backend.disconnect()
        except BaseException:
            pass


class _LifecycleGate:
    """Lock wrapper that lets teardown win before a poll's extraction fence."""

    def __init__(self, lock: Any, target_name: str) -> None:
        self._lock = lock
        self._target_name = target_name
        self._target_entries = 0
        self.extraction_attempted = Event()
        self.allow_extraction = Event()

    def acquire(self, *args: Any, **kwargs: Any) -> bool:
        if current_thread().name == self._target_name:
            self._target_entries += 1
            if self._target_entries == 2:
                self.extraction_attempted.set()
                self.allow_extraction.wait(timeout=2.0)
        return self._lock.acquire(*args, **kwargs)

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> _LifecycleGate:
        self.acquire()
        return self

    def __exit__(self, *_args: Any) -> None:
        self.release()


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


def _connected_backend(mocker: Any, consumers: list[Any]) -> PulsarBackend:
    client = MagicMock(name="pulsar-client")
    client.subscribe.side_effect = consumers
    mocker.patch.object(pulsar, "Client", return_value=client)
    backend = PulsarBackend(PulsarSettings())
    backend.connect()
    _CONNECTED_BACKENDS.append(backend)
    return backend


def _wait_until(predicate: Any, timeout: float = 1.0) -> bool:
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return True
        Event().wait(0.005)
    return bool(predicate())


def test_first_poll_never_waits_outside_budget_for_blocked_subscribe(
    mocker: Any,
) -> None:
    subscribe_started = Event()
    release_subscribe = Event()
    consumer = _ControllableConsumer()
    client = MagicMock(name="blocked-subscribe-client")

    def subscribe(*_args: Any, **_kwargs: Any) -> _ControllableConsumer:
        subscribe_started.set()
        release_subscribe.wait(timeout=2.0)
        return consumer

    client.subscribe.side_effect = subscribe
    mocker.patch.object(pulsar, "Client", return_value=client)
    backend = PulsarBackend(PulsarSettings())
    backend.connect()
    _CONNECTED_BACKENDS.append(backend)
    try:
        started = monotonic()
        assert backend.pop("bootstrap", timeout=0) is None
        assert monotonic() - started < 0.2
        assert subscribe_started.wait(timeout=0.5)

        started = monotonic()
        assert backend.pop("bootstrap", timeout=0.05) is None
        elapsed = monotonic() - started
        assert elapsed >= 0.04
        assert elapsed < 0.5
        assert not consumer.receive_started.is_set()
    finally:
        release_subscribe.set()
        backend.disconnect()


def test_transient_subscribe_failure_is_retired_for_next_poll(mocker: Any) -> None:
    subscribe_started = Event()
    release_failure = Event()
    recovered_consumer = _ControllableConsumer()
    client = MagicMock(name="transient-subscribe-client")
    subscribe_calls = 0

    def subscribe(*_args: Any, **_kwargs: Any) -> _ControllableConsumer:
        nonlocal subscribe_calls
        subscribe_calls += 1
        if subscribe_calls == 1:
            subscribe_started.set()
            release_failure.wait(timeout=2.0)
            raise RuntimeError("transient subscribe failure")
        return recovered_consumer

    client.subscribe.side_effect = subscribe
    mocker.patch.object(pulsar, "Client", return_value=client)
    backend = PulsarBackend(PulsarSettings())
    backend.connect()
    _CONNECTED_BACKENDS.append(backend)
    topic = "scrapy-subscribe-recovery"
    try:
        assert backend.pop("subscribe-recovery", timeout=0) is None
        failed_pump = backend._receive_pumps[topic]
        assert subscribe_started.wait(timeout=0.5)
        release_failure.set()
        assert failed_pump.stopped.wait(timeout=0.5)

        with pytest.raises(QueueError):
            backend.pop("subscribe-recovery", timeout=0.1)
        assert topic not in backend._receive_pumps
        assert topic not in backend._consumers

        assert backend.pop("subscribe-recovery", timeout=0) is None
        recovered_pump = backend._receive_pumps[topic]
        assert recovered_pump is not failed_pump
        assert recovered_consumer.receive_started.wait(timeout=0.5)
        recovered_consumer.deliver(_message(b"recovered", object()))
        assert backend.pop("subscribe-recovery", timeout=1.0) == b"recovered"
        assert client.subscribe.call_count == 2
    finally:
        release_failure.set()
        backend.disconnect()


def test_transient_receive_failure_is_retired_for_next_poll(mocker: Any) -> None:
    receive_started = Event()
    release_failure = Event()
    failed_consumer = MagicMock(name="failed-consumer")
    recovered_consumer = _ControllableConsumer()

    def receive(*, timeout_millis: int) -> Any:
        del timeout_millis
        receive_started.set()
        release_failure.wait(timeout=2.0)
        raise RuntimeError("transient receive failure")

    failed_consumer.receive.side_effect = receive
    backend = _connected_backend(mocker, [failed_consumer, recovered_consumer])
    topic = "scrapy-receive-recovery"
    try:
        assert backend.pop("receive-recovery", timeout=0) is None
        failed_pump = backend._receive_pumps[topic]
        assert receive_started.wait(timeout=0.5)
        release_failure.set()
        assert failed_pump.stopped.wait(timeout=0.5)

        with pytest.raises(QueueError):
            backend.pop("receive-recovery", timeout=0.1)
        failed_consumer.close.assert_called_once_with()
        assert topic not in backend._receive_pumps
        assert topic not in backend._consumers

        assert backend.pop("receive-recovery", timeout=0) is None
        recovered_pump = backend._receive_pumps[topic]
        assert recovered_pump is not failed_pump
        assert recovered_consumer.receive_started.wait(timeout=0.5)
        recovered_consumer.deliver(_message(b"recovered", object()))
        assert backend.pop("receive-recovery", timeout=1.0) == b"recovered"
    finally:
        release_failure.set()
        backend.disconnect()


@pytest.mark.parametrize("timeout", [0.0, 0.05], ids=["zero", "positive"])
def test_terminal_retirement_never_exceeds_caller_budget(
    mocker: Any, timeout: float
) -> None:
    """A failed close starts, but terminal delivery honors the poll deadline."""
    close_started = Event()
    release_close = Event()
    failed_consumer = MagicMock(name="budgeted-failed-consumer")
    failed_consumer.receive.side_effect = RuntimeError("budgeted receive failure")

    def blocked_close() -> None:
        close_started.set()
        release_close.wait(timeout=2.0)

    failed_consumer.close.side_effect = blocked_close
    backend = _connected_backend(mocker, [failed_consumer])
    backend._receive_shutdown_timeout = 0.5
    topic = "scrapy-budgeted-retirement"
    try:
        assert backend.pop("budgeted-retirement", timeout=0) is None
        failed_pump = backend._receive_pumps[topic]
        assert failed_pump.stopped.wait(timeout=0.5)

        started = monotonic()
        with pytest.raises(QueueError, match="Failed to pop Pulsar message"):
            backend.pop("budgeted-retirement", timeout=timeout)
        elapsed = monotonic() - started

        assert close_started.is_set()
        assert list(backend._consumer_retirements) == [topic]
        if timeout == 0:
            assert elapsed < 0.1
        else:
            assert elapsed >= 0.04
            assert elapsed < 0.2
    finally:
        release_close.set()


def test_retirement_start_that_launches_then_raises_preserves_terminal_error(
    mocker: Any,
) -> None:
    """An ambiguous Thread.start failure keeps its fence until close completes."""
    close_started = Event()
    release_close = Event()
    pump_error = KeyboardInterrupt("pump control marker")
    start_error = SystemExit("retirement start marker")
    failed_consumer = MagicMock(name="ambiguous-start-consumer")
    recovered_consumer = _ControllableConsumer()
    failed_consumer.receive.side_effect = pump_error

    def blocked_close() -> None:
        close_started.set()
        release_close.wait(timeout=2.0)

    failed_consumer.close.side_effect = blocked_close
    backend = _connected_backend(mocker, [failed_consumer, recovered_consumer])
    topic = "scrapy-ambiguous-start"
    assert backend.pop("ambiguous-start", timeout=0) is None
    failed_pump = backend._receive_pumps[topic]
    assert failed_pump.stopped.wait(timeout=0.5)

    real_start = Thread.start

    def launch_then_raise(worker: Thread) -> None:
        real_start(worker)
        if worker.name == "pulsar-failed-consumer-retirement":
            assert close_started.wait(timeout=0.5)
            raise start_error

    mocker.patch.object(pulsar_backend_module.Thread, "start", launch_then_raise)
    try:
        with pytest.raises(KeyboardInterrupt) as captured:
            backend.pop("ambiguous-start", timeout=0)
        assert captured.value is pump_error
        assert list(backend._consumer_retirements) == [topic]
        assert topic not in backend._receive_pumps
        assert topic not in backend._consumers
        assert failed_consumer.close.call_count == 1

        release_close.set()
        assert _wait_until(lambda: backend._consumer_retirements == {})
        assert backend.pop("ambiguous-start", timeout=0) is None
        assert recovered_consumer.receive_started.wait(timeout=0.5)
        assert failed_consumer.close.call_count == 1
    finally:
        release_close.set()


@pytest.mark.parametrize("consumer_type", ["Exclusive", "Failover"])
@pytest.mark.parametrize(
    "receive_error",
    [RuntimeError("failed receive"), KeyboardInterrupt("cancelled receive")],
    ids=["exception", "base-exception"],
)
def test_failed_consumer_close_fences_concurrent_replacement_across_disconnect(
    mocker: Any, consumer_type: str, receive_error: BaseException
) -> None:
    """A blocked failed close conserves one subscription until it truly exits."""
    receive_started = Event()
    close_started = Event()
    release_close = Event()
    failed_consumer = MagicMock(name="failed-exclusive-consumer")
    recovered_consumer = _ControllableConsumer()

    def failed_receive(*, timeout_millis: int) -> Any:
        del timeout_millis
        receive_started.set()
        raise receive_error

    def blocked_close() -> None:
        close_started.set()
        release_close.wait(timeout=2.0)

    failed_consumer.receive.side_effect = failed_receive
    failed_consumer.close.side_effect = blocked_close
    old_client = MagicMock(name="old-client")
    old_client.subscribe.return_value = failed_consumer
    new_client = MagicMock(name="new-client")
    new_client.subscribe.return_value = recovered_consumer
    client_factory = mocker.patch.object(
        pulsar, "Client", side_effect=[old_client, new_client]
    )
    backend = PulsarBackend(PulsarSettings(consumer_type=consumer_type))
    backend._receive_shutdown_timeout = 0.05
    backend.connect()
    _CONNECTED_BACKENDS.append(backend)
    topic = "scrapy-serialized-retirement"
    observed_errors: list[BaseException] = []

    assert backend.pop("serialized-retirement", timeout=0) is None
    failed_pump = backend._receive_pumps[topic]
    assert receive_started.wait(timeout=0.5)
    assert failed_pump.stopped.wait(timeout=0.5)

    def observe_failure() -> None:
        try:
            backend.pop("serialized-retirement", timeout=0.5)
        except BaseException as error:
            observed_errors.append(error)

    observer = Thread(target=observe_failure, name="failed-pump-observer")
    observer.start()
    assert close_started.wait(timeout=0.5)

    # This poll races the bounded failed-close wait. It must not publish a
    # replacement subscribe while an Exclusive/Failover consumer may remain live.
    assert backend.pop("serialized-retirement", timeout=0) is None
    assert old_client.subscribe.call_count == 1
    assert new_client.subscribe.call_count == 0
    assert list(backend._consumer_retirements) == [topic]

    observer.join(timeout=0.5)
    assert not observer.is_alive()
    assert len(observed_errors) == 1
    if isinstance(receive_error, Exception):
        assert isinstance(observed_errors[0], QueueError)
    else:
        assert observed_errors[0] is receive_error

    # A disconnect/reconnect changes the client generation but cannot erase the
    # topic retirement fence or admit another broker subscription.
    backend.disconnect()
    backend.connect()
    assert backend.pop("serialized-retirement", timeout=0) is None
    assert old_client.subscribe.call_count == 1
    assert new_client.subscribe.call_count == 0
    assert list(backend._consumer_retirements) == [topic]

    release_close.set()
    assert _wait_until(lambda: backend._consumer_retirements == {})
    assert backend.pop("serialized-retirement", timeout=0) is None
    assert recovered_consumer.receive_started.wait(timeout=0.5)
    recovered_consumer.deliver(_message(b"replacement", object()))
    assert backend.pop("serialized-retirement", timeout=1.0) == b"replacement"

    # Conservation: one failed close, one replacement subscribe, one active pump,
    # and no leaked retirement after the old handle has actually finished.
    assert failed_consumer.close.call_count == 1
    assert old_client.subscribe.call_count + new_client.subscribe.call_count == 2
    assert client_factory.call_count == 2
    assert list(backend._receive_pumps) == [topic]
    assert backend._consumers == {topic: recovered_consumer}
    assert backend._consumer_retirements == {}


@pytest.mark.parametrize("consumer_type", ["Exclusive", "Failover"])
def test_disconnect_retires_unobserved_terminal_consumer_across_reconnect(
    mocker: Any, consumer_type: str
) -> None:
    """Teardown fences a failed consumer even before a poll observes failure."""
    close_started = Event()
    release_close = Event()
    failed_consumer = MagicMock(name="unobserved-failed-consumer")
    recovered_consumer = _ControllableConsumer()
    failed_consumer.receive.side_effect = RuntimeError("unobserved receive failure")

    def blocked_close() -> None:
        close_started.set()
        release_close.wait(timeout=2.0)

    failed_consumer.close.side_effect = blocked_close
    old_client = MagicMock(name="unobserved-old-client")
    old_client.subscribe.return_value = failed_consumer
    new_client = MagicMock(name="unobserved-new-client")
    new_client.subscribe.return_value = recovered_consumer
    mocker.patch.object(pulsar, "Client", side_effect=[old_client, new_client])
    backend = PulsarBackend(PulsarSettings(consumer_type=consumer_type))
    backend._receive_shutdown_timeout = 0.05
    backend.connect()
    _CONNECTED_BACKENDS.append(backend)
    topic = "scrapy-unobserved-retirement"

    assert backend.pop("unobserved-retirement", timeout=0) is None
    failed_pump = backend._receive_pumps[topic]
    assert failed_pump.stopped.wait(timeout=0.5)

    backend.disconnect()
    assert close_started.is_set()
    assert list(backend._consumer_retirements) == [topic]
    backend.connect()
    assert backend.pop("unobserved-retirement", timeout=0) is None
    assert new_client.subscribe.call_count == 0

    release_close.set()
    assert _wait_until(lambda: backend._consumer_retirements == {})
    assert backend.pop("unobserved-retirement", timeout=0) is None
    assert recovered_consumer.receive_started.wait(timeout=0.5)
    assert failed_consumer.close.call_count == 1
    assert new_client.subscribe.call_count == 1


@pytest.mark.parametrize("consumer_type", ["Exclusive", "Failover"])
def test_disconnect_fences_stale_blocked_bootstrap_return_across_reconnect(
    mocker: Any, consumer_type: str
) -> None:
    """A subscribe result returning after teardown closes before replacement."""
    subscribe_started = Event()
    release_subscribe = Event()
    close_started = Event()
    release_close = Event()
    stale_consumer = MagicMock(name="stale-bootstrap-consumer")
    recovered_consumer = _ControllableConsumer()

    def blocked_subscribe(*_args: Any, **_kwargs: Any) -> Any:
        subscribe_started.set()
        release_subscribe.wait(timeout=2.0)
        return stale_consumer

    def blocked_close() -> None:
        close_started.set()
        release_close.wait(timeout=2.0)

    stale_consumer.close.side_effect = blocked_close
    old_client = MagicMock(name="bootstrap-old-client")
    old_client.subscribe.side_effect = blocked_subscribe
    new_client = MagicMock(name="bootstrap-new-client")
    new_client.subscribe.return_value = recovered_consumer
    mocker.patch.object(pulsar, "Client", side_effect=[old_client, new_client])
    backend = PulsarBackend(PulsarSettings(consumer_type=consumer_type))
    backend._receive_shutdown_timeout = 0.05
    backend.connect()
    _CONNECTED_BACKENDS.append(backend)
    topic = "scrapy-stale-bootstrap"

    assert backend.pop("stale-bootstrap", timeout=0) is None
    assert subscribe_started.wait(timeout=0.5)
    backend.disconnect()
    assert list(backend._consumer_retirements) == [topic]
    backend.connect()
    assert backend.pop("stale-bootstrap", timeout=0) is None
    assert new_client.subscribe.call_count == 0

    release_subscribe.set()
    assert close_started.wait(timeout=0.5)
    assert backend.pop("stale-bootstrap", timeout=0) is None
    assert new_client.subscribe.call_count == 0

    release_close.set()
    assert _wait_until(lambda: backend._consumer_retirements == {})
    assert backend.pop("stale-bootstrap", timeout=0) is None
    assert recovered_consumer.receive_started.wait(timeout=0.5)
    stale_consumer.close.assert_called_once_with()
    assert new_client.subscribe.call_count == 1


def test_receive_worker_name_does_not_expose_private_topic(mocker: Any) -> None:
    private_marker = "private-thread-topic-marker"
    consumer = _ControllableConsumer()
    backend = _connected_backend(mocker, [consumer])
    try:
        assert backend.pop(private_marker, timeout=0) is None
        pump = backend._receive_pumps[f"scrapy-{private_marker}"]
        assert consumer.receive_started.wait(timeout=0.5)
        assert pump.worker is not None and pump.worker.is_alive()
        assert all(private_marker not in thread.name for thread in enumerate_threads())
    finally:
        backend.disconnect()


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
        pump = backend._receive_pumps["scrapy-bounded"]
        assert consumer.receive_started.wait(timeout=0.5)
        for index in range(3):
            consumer.deliver(_message(str(index).encode(), object()))
        assert _wait_until(lambda: len(pump.records) == 2)
        assert consumer.receive_calls == 2
        assert len(pump.records) == pump.capacity == 2
    finally:
        backend.disconnect()


@pytest.mark.parametrize("with_ack", [False, True])
def test_disconnect_wins_before_atomic_buffer_extraction(
    mocker: Any, with_ack: bool
) -> None:
    consumer = _ControllableConsumer()
    backend = _connected_backend(mocker, [consumer])
    old_message = _message(b"old", object())
    results: list[Any] = []
    poll_name = f"fenced-poll-{with_ack}"
    try:
        assert backend.pop("race", timeout=0) is None
        pump = backend._receive_pumps["scrapy-race"]
        assert consumer.receive_started.wait(timeout=0.5)
        consumer.deliver(old_message)
        assert pump.buffered.wait(timeout=0.5)

        gate = _LifecycleGate(backend._lifecycle_lock, poll_name)
        backend._lifecycle_lock = gate  # type: ignore[assignment]

        def poll() -> None:
            if with_ack:
                results.append(backend.pop_with_ack("race", timeout=1.0))
            else:
                results.append(backend.pop("race", timeout=1.0))

        thread = Thread(target=poll, name=poll_name)
        thread.start()
        assert gate.extraction_attempted.wait(timeout=0.5)

        # The poll has found its pump but has not entered the lifecycle-fenced
        # record check. Teardown linearizes first and must make that record stale.
        backend.disconnect()
        gate.allow_extraction.set()
        thread.join(timeout=1.0)

        assert not thread.is_alive()
        assert results == ([(None, None)] if with_ack else [None])
        assert backend._last_msg is None
        assert backend._last_delivery is None
        assert backend._in_flight == set()
        assert len(pump.records) == 0
    finally:
        if isinstance(backend._lifecycle_lock, _LifecycleGate):
            backend._lifecycle_lock.allow_extraction.set()
        backend.disconnect()


def test_disconnect_is_bounded_when_close_does_not_interrupt_receive(
    mocker: Any,
) -> None:
    release_receive = Event()
    receive_started = Event()
    close_called = Event()
    message = _message(b"late", object())
    consumer = MagicMock(name="uninterruptible-consumer")

    def receive(*, timeout_millis: int) -> Any:
        del timeout_millis
        receive_started.set()
        release_receive.wait(timeout=2.0)
        return message

    consumer.receive.side_effect = receive
    consumer.close.side_effect = close_called.set
    backend = _connected_backend(mocker, [consumer])
    backend._receive_shutdown_timeout = 0.05
    warning = mocker.patch("scrapy_extension.backends.pulsar.logger.warning")

    assert backend.pop("stuck", timeout=0) is None
    pump = backend._receive_pumps["scrapy-stuck"]
    assert receive_started.wait(timeout=0.5)

    started = monotonic()
    backend.disconnect()
    elapsed = monotonic() - started

    assert elapsed < 0.5
    assert close_called.is_set()
    warning.assert_called_once_with(
        "Pulsar receive worker did not stop within the shutdown timeout."
    )
    assert not pump.stopped.is_set()

    release_receive.set()
    assert pump.stopped.wait(timeout=0.5)
    assert len(pump.records) == 0
    assert backend._last_msg is None
    assert backend._in_flight == set()


def test_disconnect_is_bounded_when_sdk_close_never_returns(mocker: Any) -> None:
    release_close = Event()
    close_started = Event()
    close_finished = Event()
    consumer = _ControllableConsumer()
    real_close = consumer.close

    def close_that_never_returns() -> None:
        close_started.set()
        try:
            release_close.wait()
            real_close()
        finally:
            close_finished.set()

    consumer.close = close_that_never_returns  # type: ignore[method-assign]
    backend = _connected_backend(mocker, [consumer])
    backend._receive_shutdown_timeout = 0.05
    warning = mocker.patch("scrapy_extension.backends.pulsar.logger.warning")

    try:
        assert backend.pop("stuck-close", timeout=0) is None
        pump = backend._receive_pumps["scrapy-stuck-close"]
        assert consumer.receive_started.wait(timeout=0.5)

        started = monotonic()
        backend.disconnect()
        elapsed = monotonic() - started

        assert elapsed < 0.5
        assert close_started.is_set()
        assert backend._client is None
        assert backend._consumers == {}
        assert not pump.stopped.is_set()
        assert warning.call_args_list == [
            call("Pulsar SDK handle close did not finish within the shutdown timeout."),
            call("Pulsar receive worker did not stop within the shutdown timeout."),
        ]
    finally:
        release_close.set()

    assert close_finished.wait(timeout=0.5)
    assert pump.stopped.wait(timeout=0.5)
    assert backend._client is None
    assert backend._consumers == {}


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
    _CONNECTED_BACKENDS.append(backend)

    assert backend.pop("queue", timeout=0) is None
    old_pump = backend._receive_pumps["scrapy-queue"]
    assert old_consumer.receive_started.wait(timeout=0.5)
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
        assert new_consumer.receive_started.wait(timeout=0.5)
        new_consumer.deliver(_message(b"new", object()))
        assert backend.pop("queue", timeout=1.0) == b"new"
        assert client_factory.call_count == 2
    finally:
        backend.disconnect()
