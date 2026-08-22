"""Deterministic MQ lifecycle-fence and reentrant-operation regressions."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.kafka import KafkaBackend, _KafkaAckToken
from scrapy_extension.backends.rabbitmq import RabbitMQBackend, _RabbitMQAckToken
from scrapy_extension.backends.rocketmq import (
    RocketMQBackend,
    _RocketMQAckToken,
)
from scrapy_extension.exceptions import ConfigurationError, QueueError
from scrapy_extension.settings import KafkaSettings, RabbitMQSettings, RocketMQSettings


def test_kafka_disconnect_fences_constructor_candidate_without_publish(mocker) -> None:
    backend = KafkaBackend(KafkaSettings())
    entered = threading.Event()
    release = threading.Event()
    producer = MagicMock()
    admin = MagicMock()

    def construct_producer(**_kwargs: object) -> object:
        entered.set()
        assert release.wait(timeout=2)
        return producer

    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaProducer",
        side_effect=construct_producer,
    )
    mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient", return_value=admin)

    connect_errors: list[BaseException] = []
    disconnect_errors: list[BaseException] = []

    def connect() -> None:
        try:
            backend.connect()
        except BaseException as error:  # pragma: no cover - assertion aid
            connect_errors.append(error)

    def disconnect() -> None:
        try:
            backend.disconnect()
        except BaseException as error:  # pragma: no cover - assertion aid
            disconnect_errors.append(error)

    connect_thread = threading.Thread(target=connect)
    disconnect_thread = threading.Thread(target=disconnect)
    connect_thread.start()
    assert entered.wait(timeout=2)
    disconnect_thread.start()
    # The teardown epoch is advanced before it waits for the single-flight lock.
    release.set()
    connect_thread.join(timeout=2)
    disconnect_thread.join(timeout=2)

    assert connect_errors == []
    assert disconnect_errors == []
    assert not connect_thread.is_alive()
    assert not disconnect_thread.is_alive()
    assert backend._generation_gate.current is None
    assert backend.is_connected() is False
    producer.close.assert_called_once_with()
    admin.close.assert_called_once_with()


def test_rocketmq_disconnect_fences_startup_candidate_without_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rocketmq

    backend = RocketMQBackend(RocketMQSettings())
    entered = threading.Event()
    release = threading.Event()
    producer = MagicMock()
    consumer = MagicMock()

    def startup() -> None:
        entered.set()
        assert release.wait(timeout=2)

    producer.startup.side_effect = startup
    monkeypatch.setattr(rocketmq, "Producer", MagicMock(return_value=producer))
    monkeypatch.setattr(rocketmq, "SimpleConsumer", MagicMock(return_value=consumer))

    connect_errors: list[BaseException] = []
    disconnect_errors: list[BaseException] = []

    def connect() -> None:
        try:
            backend.connect()
        except BaseException as error:  # pragma: no cover - assertion aid
            connect_errors.append(error)

    def disconnect() -> None:
        try:
            backend.disconnect()
        except BaseException as error:  # pragma: no cover - assertion aid
            disconnect_errors.append(error)

    connect_thread = threading.Thread(target=connect)
    disconnect_thread = threading.Thread(target=disconnect)
    connect_thread.start()
    assert entered.wait(timeout=2)
    disconnect_thread.start()
    release.set()
    connect_thread.join(timeout=2)
    disconnect_thread.join(timeout=2)

    assert connect_errors == []
    assert disconnect_errors == []
    assert not connect_thread.is_alive()
    assert not disconnect_thread.is_alive()
    assert backend._generation_gate.current is None
    assert backend.is_connected() is False
    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()


def test_rocketmq_revalidates_topic_prefix_at_connection_snapshot_boundary() -> None:
    settings = RocketMQSettings()
    settings.topic_prefix = "invalid:rocket-topic"
    backend = RocketMQBackend(settings)

    with pytest.raises(ConfigurationError) as error:
        backend.connect()

    assert error.value.setting_name == "topic_prefix"
    assert str(error.value) == "RocketMQ topic_prefix is invalid."


def test_rocketmq_push_uses_leased_producer_after_mirror_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rocketmq

    message = MagicMock()
    monkeypatch.setattr(rocketmq, "Message", MagicMock(return_value=message))
    backend = RocketMQBackend(RocketMQSettings())
    leased_producer = MagicMock()
    consumer = MagicMock(is_running=True)
    backend._producer = leased_producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    with backend._connection_lock:
        backend._publish_generation_locked()

    # This is a compatibility mirror only; the admitted operation owns the
    # producer retained in its generation record.
    backend._producer = None
    backend.push("jobs", b"payload")

    leased_producer.send.assert_called_once_with(message)


@pytest.mark.parametrize(
    ("operation", "sdk_method"),
    [("ack", "ack"), ("nack", "change_invisible_duration")],
)
def test_rocketmq_stale_token_cannot_settle_reused_consumer(
    operation: str, sdk_method: str
) -> None:
    """A replacement generation fences tokens even when the handle is reused."""
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    with backend._connection_lock:
        backend._publish_generation_locked()

    token = _RocketMQAckToken(object(), consumer, backend._consumer_generation)
    backend.disconnect()

    # Reuse the same SDK object in a fresh, real gate generation. Identity alone
    # is not a sufficient fence because test doubles and reconnect adapters may
    # retain a handle while replacing the lifecycle generation.
    consumer.reset_mock()
    backend._producer = MagicMock(is_running=True)
    backend._consumer = consumer
    backend._consumer_generation += 1
    with backend._connection_lock:
        backend._publish_generation_locked()

    getattr(backend, operation)("jobs", token=token)

    getattr(consumer, sdk_method).assert_not_called()


def test_kafka_reentrant_disconnect_does_not_report_send_success() -> None:
    backend = KafkaBackend(KafkaSettings())
    producer = MagicMock()
    admin = MagicMock()
    future = MagicMock()
    backend._producer = producer
    backend._admin_client = admin
    backend._known_topics.add("scrapy-jobs")

    def send_and_disconnect(*_args: object, **_kwargs: object) -> object:
        del _args, _kwargs
        backend.disconnect()
        return future

    producer.send.side_effect = send_and_disconnect

    with pytest.raises(QueueError, match="connection changed"):
        backend.push("jobs", b"payload")

    producer.close.assert_called_once_with()
    admin.close.assert_called_once_with()


def test_kafka_ack_work_is_partition_local_at_scale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # kafka-python changed OffsetAndMetadata's constructor shape across
    # releases; this test measures local bookkeeping, not that dependency API.
    monkeypatch.setattr(
        "scrapy_extension.backends.kafka.OffsetAndMetadata", lambda *args: args
    )
    backend = KafkaBackend(KafkaSettings())
    producer = MagicMock()
    admin = MagicMock()
    consumer = MagicMock()
    backend._producer = producer
    backend._admin_client = admin
    backend._consumer = consumer

    partition_count = 512
    for partition in range(partition_count):
        key = ("scrapy-jobs", partition)
        backend._in_flight[key].add(0)
        backend._watermarks[key] = 0
        backend._high_water[key] = 1
        backend._active_attempts[(key[0], partition, 0)] = 1
        backend._active_attempts_by_partition[key] = {1}

    token = _KafkaAckToken(
        partition=0,
        offset=0,
        topic="scrapy-jobs",
        consumer_generation=0,
        assignment_epoch=0,
        delivery_attempt=1,
    )
    backend.ack("jobs", token=token)

    consumer.commit.assert_called_once()
    committed = consumer.commit.call_args.args[0]
    assert len(committed) == 1
    assert ("scrapy-jobs", 1) in backend._in_flight
    assert len(backend._in_flight) == partition_count - 1


def test_rabbitmq_candidate_close_is_not_repeated_after_control_failure(mocker) -> None:
    backend = RabbitMQBackend(RabbitMQSettings())
    connection = MagicMock()
    channel = MagicMock()
    connection.channel.return_value = channel

    def construct(_parameters: object) -> object:
        backend.disconnect()
        return connection

    channel.close.side_effect = KeyboardInterrupt()
    mocker.patch(
        "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
        side_effect=construct,
    )

    with pytest.raises(KeyboardInterrupt):
        backend.connect()

    channel.close.assert_called_once_with()
    connection.close.assert_called_once_with()


def test_rabbitmq_reentrant_disconnect_does_not_report_publish_success() -> None:
    backend = RabbitMQBackend(RabbitMQSettings())
    connection = MagicMock(is_open=True)
    channel = MagicMock(is_open=True)
    backend._activate_channel(connection, channel)
    callback_returned = threading.Event()

    def publish(**_kwargs: object) -> None:
        backend.disconnect()
        assert not channel.close.called
        callback_returned.set()

    channel.basic_publish.side_effect = publish

    with pytest.raises(QueueError, match="connection changed"):
        backend.push("jobs", b"payload")

    assert callback_returned.is_set()
    channel.close.assert_called_once_with()
    connection.close.assert_called_once_with()
    assert backend.is_connected() is False


@pytest.mark.parametrize(
    ("operation", "sdk_method"),
    [("ack", "basic_ack"), ("nack", "basic_nack")],
)
def test_rabbitmq_settlement_reentrant_disconnect_is_ambiguous(
    operation: str, sdk_method: str
) -> None:
    backend = RabbitMQBackend(RabbitMQSettings())
    connection = MagicMock(is_open=True)
    channel = MagicMock(is_open=True)
    backend._activate_channel(connection, channel)
    token = _RabbitMQAckToken(7, backend._channel_generation, "jobs")
    backend._in_flight_tags.add(token)
    backend._pending_deliveries["jobs"] = 1

    def settle(**_kwargs: object) -> None:
        backend.disconnect()
        assert not channel.close.called

    getattr(channel, sdk_method).side_effect = settle

    with pytest.raises(QueueError, match="connection changed during publish"):
        getattr(backend, operation)("jobs", token=token)

    getattr(channel, sdk_method).assert_called_once()
    channel.close.assert_called_once_with()
    connection.close.assert_called_once_with()
    assert backend.is_connected() is False


def test_rabbitmq_disconnect_waits_for_leased_publish_and_closes_once() -> None:
    backend = RabbitMQBackend(RabbitMQSettings())
    connection = MagicMock(is_open=True)
    channel = MagicMock(is_open=True)
    backend._activate_channel(connection, channel)
    entered = threading.Event()
    release = threading.Event()

    def blocked_publish(**_kwargs: object) -> bool:
        entered.set()
        assert release.wait(timeout=2.0)
        return True

    channel.basic_publish.side_effect = blocked_publish
    push_errors: list[BaseException] = []

    def push() -> None:
        try:
            backend.push("jobs", b"payload")
        except BaseException as error:
            push_errors.append(error)

    publisher = threading.Thread(target=push)
    publisher.start()
    assert entered.wait(timeout=2.0)
    teardown = threading.Thread(target=backend.disconnect)
    teardown.start()
    assert teardown.is_alive()
    assert not channel.close.called

    release.set()
    publisher.join(timeout=2.0)
    teardown.join(timeout=2.0)
    assert not publisher.is_alive()
    assert not teardown.is_alive()
    # RabbitMQ serializes the channel SDK call with the delivery lock. Teardown
    # cannot linearize until that call returns, so this confirmed publish remains
    # a success while cleanup still waits for the exact operation to finish.
    assert push_errors == []
    channel.close.assert_called_once_with()
    connection.close.assert_called_once_with()
    assert backend.is_connected() is False


@pytest.mark.parametrize("cycles", [1, 25])
def test_generation_fence_stress_loop_has_no_timing_dependency(cycles: int) -> None:
    """Repeated reentrant retire/drain cycles never close inside the callback."""
    from scrapy_extension.backends._generation import GenerationLeaseGate

    gate: GenerationLeaseGate[object] = GenerationLeaseGate()
    for _ in range(cycles):
        handle = MagicMock()
        record = gate.publish(handle)
        state = {"callback_returned": False}
        with gate.lease("sdk"):
            retired = gate.retire()
            assert retired is record
            finalized: list[bool] = []
            assert (
                gate.drain(
                    retired,
                    lambda finalized=finalized, state=state: finalized.append(
                        state["callback_returned"]
                    ),
                )
                is None
            )
            assert finalized == []
            state["callback_returned"] = True
        assert finalized == [True]
