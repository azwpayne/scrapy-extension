"""Deterministic generation admission/retirement contracts for MQ backends."""

from __future__ import annotations

from threading import Event, Thread
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends._generation import (
    GenerationLeaseGate,
    GenerationUnavailable,
)
from scrapy_extension.backends.kafka import KafkaBackend, _KafkaAckToken
from scrapy_extension.backends.pulsar import PulsarBackend, _PulsarAckToken
from scrapy_extension.backends.rocketmq import (
    RocketMQBackend,
    _RocketMQAckToken,
)
from scrapy_extension.exceptions import QueueError, QueueOutcomeIndeterminateError
from scrapy_extension.settings import KafkaSettings, PulsarSettings, RocketMQSettings


def test_publish_is_idempotent_and_retired_finalizer_runs_once() -> None:
    gate: GenerationLeaseGate[object] = GenerationLeaseGate()
    first = gate.publish(object())
    assert gate.publish(object()) is first
    retired = gate.retire()
    assert retired is first
    assert gate.retire() is None

    finalized: list[str] = []
    gate.drain(retired, lambda: finalized.append("closed"))
    gate.drain(retired, lambda: finalized.append("duplicate"))
    assert finalized == ["closed"]


def test_nested_leases_defer_failing_finalizer_until_outer_operation_returns() -> None:
    gate: GenerationLeaseGate[object] = GenerationLeaseGate()
    record = gate.publish(object())
    finalizer_started: list[str] = []
    cleanup_error = KeyboardInterrupt("cleanup interrupted")

    def finalizer() -> None:
        finalizer_started.append("after-drain")
        raise cleanup_error

    with gate.lease("outer"):
        with gate.lease("reentrant"):
            retired = gate.retire()
            assert retired is record
            assert gate.drain(retired, finalizer) is None
            assert record.active_leases == 2
            assert finalizer_started == []
        assert record.active_leases == 1
        assert finalizer_started == []

    assert record.active_leases == 0
    assert finalizer_started == ["after-drain"]
    assert record.finalization_errors == [cleanup_error]


def test_interrupted_drain_returns_exact_signal_after_other_owner_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate: GenerationLeaseGate[object] = GenerationLeaseGate()
    record = gate.publish(object())
    entered = Event()
    release = Event()

    def operation() -> None:
        with gate.lease("blocked"):
            entered.set()
            assert release.wait(timeout=2.0)

    worker = Thread(target=operation)
    worker.start()
    assert entered.wait(timeout=2.0)
    retired = gate.retire()
    original_wait = gate.condition.wait
    interrupted = KeyboardInterrupt("drain cancelled")
    interrupted_once = Event()

    def wait_with_one_interrupt(*args: object, **kwargs: object) -> bool:
        if not interrupted_once.is_set():
            interrupted_once.set()
            raise interrupted
        return original_wait(*args, **kwargs)

    monkeypatch.setattr(gate.condition, "wait", wait_with_one_interrupt)
    finalizers: list[str] = []
    result: list[BaseException | None] = []

    def drain() -> None:
        result.append(gate.drain(retired, lambda: finalizers.append("closed")))

    drainer = Thread(target=drain)
    drainer.start()
    assert interrupted_once.wait(timeout=2.0)
    release.set()
    worker.join(timeout=2.0)
    drainer.join(timeout=2.0)

    assert not worker.is_alive()
    assert not drainer.is_alive()
    assert result == [interrupted]
    assert interrupted.__traceback__ is None
    assert interrupted.__cause__ is None
    assert interrupted.__context__ is None
    assert interrupted.__suppress_context__ is True
    assert finalizers == ["closed"]


def test_retirement_waits_for_admitted_operation_and_rejects_new_admission() -> None:
    gate: GenerationLeaseGate[object] = GenerationLeaseGate()
    handle = object()
    record = gate.publish(handle)
    entered = Event()
    release = Event()
    operation_done = Event()

    def operation() -> None:
        with gate.lease("push") as admitted:
            assert admitted.value is handle
            entered.set()
            assert release.wait(timeout=2.0)
        operation_done.set()

    worker = Thread(target=operation)
    worker.start()
    assert entered.wait(timeout=2.0)

    retired = gate.retire()
    assert retired is record
    with pytest.raises(GenerationUnavailable):
        with gate.lease("push"):
            pass

    drained = Event()
    drain_started = Event()

    def wait_for_drain() -> None:
        drain_started.set()
        assert gate.drain(retired) is None
        drained.set()

    drainer = Thread(target=wait_for_drain)
    drainer.start()
    assert drain_started.wait(timeout=2.0)
    # ``release`` remains unset, so the admitted lease is still authoritative;
    # the barrier proves the drain is blocked without scheduler timing.
    assert not drained.is_set()
    release.set()
    worker.join(timeout=2.0)
    drainer.join(timeout=2.0)
    assert operation_done.is_set()
    assert drained.is_set()


def test_release_is_authoritative_after_caller_timeout() -> None:
    gate: GenerationLeaseGate[object] = GenerationLeaseGate()
    record = gate.publish(object())
    entered = Event()
    release = Event()

    def operation() -> None:
        with gate.lease("send"):
            entered.set()
            release.wait(timeout=2.0)

    worker = Thread(target=operation)
    worker.start()
    assert entered.wait(timeout=2.0)
    retired = gate.retire()
    assert retired is record
    # A caller-side timeout cannot make drain believe the SDK call completed.
    assert retired.active_leases == 1
    release.set()
    worker.join(timeout=2.0)
    assert retired.active_leases == 0


def test_same_thread_retirement_defers_handle_finalizer_until_callback_returns() -> (
    None
):
    gate: GenerationLeaseGate[object] = GenerationLeaseGate()
    record = gate.publish(object())
    callback_returned = Event()
    finalized = Event()

    with gate.lease("sdk-callback"):
        retired = gate.retire()
        assert retired is record
        assert gate.drain(retired, finalized.set) is None
        assert not finalized.is_set()
        assert not callback_returned.is_set()
        callback_returned.set()

    # The deferred finalizer runs from lease release, after the SDK callback's
    # with-body has returned; it is never a mid-callback close.
    assert callback_returned.is_set()
    assert finalized.is_set()


def test_kafka_reentrant_disconnect_finalizes_after_callback_returns() -> None:
    config = KafkaSettings()
    backend = KafkaBackend(config)
    producer = MagicMock()
    admin = MagicMock()
    future = MagicMock()
    callback_returned = Event()

    def send_result(*, timeout: float) -> None:
        backend.disconnect()
        assert not producer.close.called
        callback_returned.set()

    future.get.side_effect = send_result
    producer.send.return_value = future
    admin.create_topics.return_value.topic_errors = [("scrapy-jobs", 0, None)]
    backend._producer = producer
    backend._admin_client = admin

    with pytest.raises(QueueError, match="connection changed"):
        backend.push("jobs", b"payload")

    assert callback_returned.is_set()
    producer.close.assert_called_once_with()
    admin.close.assert_called_once_with()


def test_pulsar_reentrant_disconnect_finalizes_after_callback_returns() -> None:
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock()
    producer = MagicMock()
    callback_returned = Event()
    backend._client = client
    backend._lifecycle_generation = 1
    backend._producers["scrapy-jobs"] = producer

    def send(_item: bytes) -> None:
        backend.disconnect()
        assert not producer.close.called
        callback_returned.set()

    producer.send.side_effect = send

    backend.push("jobs", b"payload")

    assert callback_returned.is_set()
    producer.close.assert_called_once_with()
    client.close.assert_called_once_with()


def test_rocketmq_reentrant_disconnect_finalizes_after_callback_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rocketmq

    message = MagicMock()
    monkeypatch.setattr(rocketmq, "Message", MagicMock(return_value=message))
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    callback_returned = Event()
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1

    def send(_message: object) -> None:
        backend.disconnect()
        assert not producer.shutdown.called
        callback_returned.set()

    producer.send.side_effect = send

    with pytest.raises(QueueError, match="connection changed"):
        backend.push("jobs", b"payload")

    assert callback_returned.is_set()
    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()


@pytest.mark.parametrize(
    ("operation", "sdk_method"),
    [("ack", "ack"), ("nack", "change_invisible_duration")],
)
@pytest.mark.parametrize(
    "failure",
    [RuntimeError("settlement failed"), KeyboardInterrupt("settlement interrupted")],
    ids=["exception", "base-exception"],
)
def test_rocketmq_settlement_failure_restores_token_for_retry(
    operation: str, sdk_method: str, failure: BaseException
) -> None:
    """Both ordinary and control failures leave an admitted token retryable."""
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    with backend._connection_lock:
        backend._publish_generation_locked()

    message = object()
    token = _RocketMQAckToken(message, consumer, backend._consumer_generation)
    getattr(consumer, sdk_method).side_effect = [failure, None]

    if isinstance(failure, Exception):
        with pytest.raises(QueueError):
            getattr(backend, operation)("jobs", token=token)
    else:
        with pytest.raises(type(failure)) as raised:
            getattr(backend, operation)("jobs", token=token)
        assert raised.value is failure
    assert token._settlement_state == "pending"

    getattr(backend, operation)("jobs", token=token)
    getattr(backend, operation)("jobs", token=token)
    assert getattr(consumer, sdk_method).call_count == 2
    assert token._settlement_state in {"acked", "nacked"}
    backend.disconnect()


def test_kafka_disconnected_backend_does_not_publish_empty_generation() -> None:
    backend = KafkaBackend(KafkaSettings())

    with pytest.raises(QueueError):
        backend.push("jobs", b"payload")
    assert backend._generation_gate.current is None

    backend._producer = MagicMock()
    backend._admin_client = MagicMock()
    backend.connect()
    assert backend._generation_gate.current is not None
    assert backend.is_connected() is True
    backend.disconnect()


def test_kafka_disconnect_waits_for_admitted_send_before_closing_handle() -> None:
    """Disconnect cannot close a producer while its send outcome is unresolved."""
    config = KafkaSettings(request_timeout_ms=1_250)
    backend = KafkaBackend(config)
    producer = MagicMock()
    admin = MagicMock()
    future = MagicMock()
    entered = Event()
    release = Event()

    def blocked_get(*, timeout: float) -> None:
        assert timeout == 1.25
        entered.set()
        assert release.wait(timeout=2.0)

    future.get.side_effect = blocked_get
    producer.send.return_value = future
    admin.create_topics.return_value.topic_errors = [("scrapy-jobs", 0, None)]
    backend._producer = producer
    backend._admin_client = admin

    send_errors: list[BaseException] = []

    def send() -> None:
        try:
            backend.push("jobs", b"payload")
        except BaseException as error:
            send_errors.append(error)

    sender = Thread(target=send)
    sender.start()
    assert entered.wait(timeout=2.0)

    disconnect_done = Event()

    def disconnect() -> None:
        backend.disconnect()
        disconnect_done.set()

    drain_started = Event()
    original_drain = backend._generation_gate.drain

    def drain(record, finalizer=None):
        drain_started.set()
        return original_drain(record, finalizer)

    backend._generation_gate.drain = drain  # type: ignore[method-assign]
    teardown = Thread(target=disconnect)
    teardown.start()
    assert drain_started.wait(timeout=2.0)
    assert not disconnect_done.is_set()
    assert not producer.close.called

    release.set()
    sender.join(timeout=2.0)
    teardown.join(timeout=2.0)
    assert not sender.is_alive()
    assert disconnect_done.is_set()
    assert len(send_errors) == 1
    assert isinstance(send_errors[0], QueueError)
    producer.close.assert_called_once_with()


def test_kafka_send_timeout_is_indeterminate_and_uses_configured_deadline() -> None:
    config = KafkaSettings(request_timeout_ms=1_250)
    backend = KafkaBackend(config)
    producer = MagicMock()
    admin = MagicMock()
    future = MagicMock()
    future.get.side_effect = TimeoutError("response lost")
    producer.send.return_value = future
    admin.create_topics.return_value.topic_errors = [("scrapy-jobs", 0, None)]
    backend._producer = producer
    backend._admin_client = admin

    with pytest.raises(QueueOutcomeIndeterminateError):
        backend.push("jobs", b"payload")

    future.get.assert_called_once_with(timeout=1.25)


def test_kafka_admitted_ack_commits_before_disconnect_clears_delivery_state() -> None:
    backend = KafkaBackend(KafkaSettings())
    producer = MagicMock()
    admin = MagicMock()
    consumer = MagicMock()
    backend._producer = producer
    backend._admin_client = admin
    backend._consumer = consumer
    token = _KafkaAckToken(
        partition=0,
        offset=0,
        topic="scrapy-jobs",
        consumer_generation=0,
        assignment_epoch=0,
        delivery_attempt=1,
    )
    topic_partition = (token.topic, token.partition)
    backend._in_flight[topic_partition].add(token.offset)
    backend._watermarks[topic_partition] = token.offset
    backend._high_water[topic_partition] = token.offset + 1
    backend._active_attempts[(token.topic, token.partition, token.offset)] = 1

    ack_entered = Event()
    release_ack = Event()
    original_ack = backend._ack_unleased

    def blocked_ack(queue_name: str, *, token: object | None = None) -> None:
        ack_entered.set()
        assert release_ack.wait(timeout=2.0)
        original_ack(queue_name, token=token)

    backend._ack_unleased = blocked_ack  # type: ignore[method-assign]
    retired = Event()
    original_retire = backend._generation_gate.retire

    def retire():
        record = original_retire()
        retired.set()
        return record

    backend._generation_gate.retire = retire  # type: ignore[method-assign]
    settlement_errors: list[BaseException] = []

    def settle() -> None:
        try:
            backend.ack("jobs", token=token)
        except BaseException as error:
            settlement_errors.append(error)

    settlement = Thread(target=settle)
    settlement.start()
    assert ack_entered.wait(timeout=2.0)

    teardown = Thread(target=backend.disconnect)
    teardown.start()
    assert retired.wait(timeout=2.0)
    assert teardown.is_alive()

    release_ack.set()
    settlement.join(timeout=2.0)
    teardown.join(timeout=2.0)
    assert not settlement.is_alive()
    assert not teardown.is_alive()
    assert len(settlement_errors) == 1
    assert isinstance(settlement_errors[0], QueueError)
    consumer.commit.assert_called_once()
    consumer.close.assert_called_once_with()


def test_kafka_reentrant_disconnect_defers_close_until_send_returns() -> None:
    backend = KafkaBackend(KafkaSettings())
    producer = MagicMock()
    admin = MagicMock()
    future = MagicMock()
    backend._producer = producer
    backend._admin_client = admin
    backend._known_topics.add("scrapy-jobs")

    def send_and_disconnect(*, timeout: float) -> None:
        del timeout
        backend.disconnect()
        assert not producer.close.called
        assert not admin.close.called

    future.get.side_effect = send_and_disconnect
    producer.send.return_value = future

    with pytest.raises(QueueError, match="connection changed"):
        backend.push("jobs", b"payload")

    producer.close.assert_called_once_with()
    admin.close.assert_called_once_with()


def test_pulsar_reentrant_disconnect_defers_close_until_send_returns() -> None:
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock()
    producer = MagicMock()
    backend._client = client
    backend._lifecycle_generation = 1
    backend._producers["scrapy-jobs"] = producer

    def send_and_disconnect(_item: bytes) -> None:
        backend.disconnect()
        assert not producer.close.called
        assert not client.close.called

    producer.send.side_effect = send_and_disconnect

    backend.push("jobs", b"payload")

    producer.close.assert_called_once_with()
    client.close.assert_called_once_with()


def test_rocketmq_reentrant_disconnect_defers_shutdown_until_send_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rocketmq

    message = MagicMock()
    monkeypatch.setattr(rocketmq, "Message", MagicMock(return_value=message))
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1

    def send_and_disconnect(_message: object) -> None:
        backend.disconnect()
        assert not producer.shutdown.called
        assert not consumer.shutdown.called

    producer.send.side_effect = send_and_disconnect

    with pytest.raises(QueueError, match="connection changed"):
        backend.push("jobs", b"payload")

    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()


def test_pulsar_reentrant_disconnect_defers_receive_close_until_receive_returns() -> (
    None
):
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock()
    consumer = MagicMock()
    backend._client = client
    backend._lifecycle_generation = 1
    client.subscribe.return_value = consumer
    close_done = Event()
    consumer.close.side_effect = lambda: close_done.set()

    def receive_and_disconnect(**_kwargs: object) -> list[object]:
        backend.disconnect()
        assert not consumer.close.called
        assert not client.close.called
        return []

    consumer.receive.side_effect = receive_and_disconnect

    assert backend.pop("jobs", timeout=1.0) is None
    assert close_done.wait(timeout=2.0)

    consumer.close.assert_called_once_with()
    client.close.assert_called_once_with()


def test_pulsar_disconnect_waits_for_admitted_send_before_closing_handle() -> None:
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock()
    producer = MagicMock()
    entered = Event()
    release = Event()

    def blocked_send(_item: bytes) -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    producer.send.side_effect = blocked_send
    backend._client = client
    backend._lifecycle_generation = 1
    backend._producers["scrapy-jobs"] = producer

    sender = Thread(target=lambda: backend.push("jobs", b"payload"))
    sender.start()
    assert entered.wait(timeout=2.0)
    teardown = Thread(target=backend.disconnect)
    teardown.start()
    assert teardown.is_alive()
    assert not producer.close.called

    release.set()
    sender.join(timeout=2.0)
    teardown.join(timeout=2.0)
    assert not sender.is_alive()
    assert not teardown.is_alive()
    producer.close.assert_called_once_with()
    client.close.assert_called_once_with()


def test_pulsar_admitted_ack_uses_retired_consumer_before_close() -> None:
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock()
    consumer = MagicMock()
    entered = Event()
    release = Event()

    def blocked_ack(_message_id: object) -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    consumer.acknowledge.side_effect = blocked_ack
    backend._client = client
    backend._lifecycle_generation = 1
    backend._consumers["scrapy-jobs"] = consumer
    backend._consumer = consumer
    token = _PulsarAckToken("message-id", "scrapy-jobs", consumer)

    settlement = Thread(target=lambda: backend.ack("jobs", token=token))
    settlement.start()
    assert entered.wait(timeout=2.0)
    teardown = Thread(target=backend.disconnect)
    teardown.start()
    assert teardown.is_alive()
    assert not consumer.close.called

    release.set()
    settlement.join(timeout=2.0)
    teardown.join(timeout=2.0)
    assert not settlement.is_alive()
    assert not teardown.is_alive()
    consumer.acknowledge.assert_called_once_with("message-id")
    consumer.close.assert_called_once_with()


def test_pulsar_admitted_legacy_ack_uses_retired_consumer_before_close() -> None:
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock()
    consumer = MagicMock()
    message = object()
    entered = Event()
    release = Event()

    def blocked_ack(_message: object) -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    consumer.acknowledge.side_effect = blocked_ack
    backend._client = client
    backend._lifecycle_generation = 1
    backend._consumers["scrapy-jobs"] = consumer
    backend._consumer = consumer
    backend._last_msg = message
    backend._last_delivery = (consumer, message)

    settlement = Thread(target=lambda: backend.ack("jobs"))
    settlement.start()
    assert entered.wait(timeout=2.0)
    teardown = Thread(target=backend.disconnect)
    teardown.start()
    assert teardown.is_alive()
    assert not consumer.close.called

    release.set()
    settlement.join(timeout=2.0)
    teardown.join(timeout=2.0)
    assert not settlement.is_alive()
    assert not teardown.is_alive()
    consumer.acknowledge.assert_called_once_with(message)
    consumer.close.assert_called_once_with()


def test_rocketmq_reentrant_disconnect_defers_receive_shutdown_until_receive_returns() -> (
    None
):
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    shutdown_done = Event()
    consumer.shutdown.side_effect = lambda: shutdown_done.set()

    def receive_and_disconnect(_count: int, _invisible: int) -> list[object]:
        backend.disconnect()
        assert not consumer.shutdown.called
        return []

    consumer.receive.side_effect = receive_and_disconnect

    with pytest.raises(QueueError, match="Not connected"):
        backend.pop("jobs", timeout=1.0)
    assert shutdown_done.wait(timeout=2.0)
    consumer.shutdown.assert_called_once_with()


def test_rocketmq_admitted_legacy_ack_uses_retired_consumer_before_shutdown() -> None:
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    message = object()
    entered = Event()
    release = Event()

    def blocked_ack(_message: object) -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    consumer.ack.side_effect = blocked_ack
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    backend._last_msg = message
    backend._last_delivery = (consumer, 1, message)

    settlement_errors: list[BaseException] = []

    def settle() -> None:
        try:
            backend.ack("jobs")
        except BaseException as error:
            settlement_errors.append(error)

    settlement = Thread(target=settle)
    settlement.start()
    assert entered.wait(timeout=2.0)
    teardown = Thread(target=backend.disconnect)
    teardown.start()
    assert teardown.is_alive()
    assert not consumer.shutdown.called

    release.set()
    settlement.join(timeout=2.0)
    teardown.join(timeout=2.0)
    assert not settlement.is_alive()
    assert not teardown.is_alive()
    assert len(settlement_errors) == 1
    assert isinstance(settlement_errors[0], QueueError)
    consumer.ack.assert_called_once_with(message)
    consumer.shutdown.assert_called_once_with()


def test_rocketmq_disconnect_waits_for_admitted_send_before_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rocketmq

    message = MagicMock()
    monkeypatch.setattr(rocketmq, "Message", MagicMock(return_value=message))
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock()
    consumer = MagicMock()
    producer.is_running = True
    consumer.is_running = True
    entered = Event()
    release = Event()

    def blocked_send(_message: object) -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    producer.send.side_effect = blocked_send
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1

    send_errors: list[BaseException] = []

    def send() -> None:
        try:
            backend.push("jobs", b"payload")
        except BaseException as error:
            send_errors.append(error)

    sender = Thread(target=send)
    sender.start()
    assert entered.wait(timeout=2.0)
    teardown = Thread(target=backend.disconnect)
    teardown.start()
    assert teardown.is_alive()
    assert not producer.shutdown.called

    release.set()
    sender.join(timeout=2.0)
    teardown.join(timeout=2.0)
    assert not sender.is_alive()
    assert not teardown.is_alive()
    assert len(send_errors) == 1
    assert isinstance(send_errors[0], QueueError)
    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()
