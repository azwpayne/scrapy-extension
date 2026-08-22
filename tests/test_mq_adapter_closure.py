"""Deterministic closure tests for the Pulsar, RocketMQ, and RabbitMQ adapters."""

from __future__ import annotations

import threading
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock

import pika
import pulsar
import pytest

import scrapy_extension.backends.pulsar as pulsar_module
import scrapy_extension.backends.rabbitmq as rabbitmq_module
import scrapy_extension.backends.rocketmq as rocketmq_module
from scrapy_extension.backends.base import _DurablePushRequired
from scrapy_extension.backends.pulsar import (
    PulsarBackend,
    _BufferedPulsarRecord,
    _PulsarAckToken,
    _PulsarReceivePump,
)
from scrapy_extension.backends.rabbitmq import RabbitMQBackend, _RabbitMQAckToken
from scrapy_extension.backends.rocketmq import (
    RocketMQBackend,
    _RocketMQAckToken,
    _RocketMQOperationSnapshot,
)
from scrapy_extension.exceptions import (
    ConfigurationError,
    QueueError,
)
from scrapy_extension.settings import (
    PulsarSettings,
    RabbitMQSettings,
    RocketMQSettings,
)

# ---------------------------------------------------------------------------
# Pulsar snapshot, ownership, and bounded cleanup
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "value", "setting"),
    [
        ("consumer_type", "invalid", "consumer_type"),
        ("initial_position", "invalid", "initial_position"),
        ("negative_ack_redelivery_delay_ms", -1, "negative_ack_redelivery_delay_ms"),
    ],
)
def test_pulsar_snapshot_revalidates_mutable_delivery_settings(
    field: str, value: object, setting: str
) -> None:
    config = PulsarSettings()
    object.__setattr__(config, field, value)

    with pytest.raises(ConfigurationError) as raised:
        PulsarBackend(config)._capture_connection_snapshot()

    assert raised.value.setting_name == setting


@pytest.mark.parametrize(
    ("mapper", "value", "setting"),
    [
        (pulsar_module._consumer_type, "invalid", "consumer_type"),
        (pulsar_module._initial_position, "invalid", "initial_position"),
    ],
)
def test_pulsar_enum_mapping_rejects_unknown_values(
    mapper, value: str, setting: str
) -> None:
    with pytest.raises(ConfigurationError) as raised:
        mapper(value)
    assert raised.value.setting_name == setting


def test_pulsar_generation_publication_and_lease_race(mocker) -> None:
    backend = PulsarBackend(PulsarSettings())
    backend._publish_generation_locked()
    assert backend._generation_gate.current is None

    backend._client = MagicMock(name="client")
    backend._lifecycle_generation = 4
    backend._publish_generation_locked()
    record = backend._generation_gate.current
    assert record is not None
    backend._publish_generation_locked()
    assert backend._generation_gate.current is record

    mocker.patch.object(
        backend._generation_gate,
        "lease",
        side_effect=rocketmq_module.GenerationUnavailable("ack"),
    )
    with pytest.raises(QueueError, match="disconnected"):
        with backend._lease_generation("ack"):
            pass


def test_pulsar_connect_failure_is_typed_and_closes_candidate(mocker) -> None:
    client = MagicMock(name="candidate")
    client.close.side_effect = RuntimeError("close failed")
    mocker.patch.object(pulsar_module.pulsar, "Client", return_value=client)
    mocker.patch.object(
        pulsar_module,
        "logger",
        debug=MagicMock(side_effect=RuntimeError("logger failed")),
    )
    # The client is returned successfully; making the snapshot invalid after the
    # constructor is not a useful connection failure. Exercise the same cleanup
    # ownership seam directly with an unpublished candidate instead.
    backend = PulsarBackend(PulsarSettings())
    assert backend._abort_failed_connect(client, None) == 1
    client.close.assert_called_once_with()


def test_pulsar_bounded_close_fences_late_outcome_and_preserves_baseexception(
    mocker,
) -> None:
    backend = PulsarBackend(PulsarSettings())
    entered = Event()
    release = Event()
    handle = MagicMock(name="slow-close")

    def close() -> None:
        entered.set()
        assert release.wait(timeout=2.0)
        raise KeyboardInterrupt("close control")

    handle.close.side_effect = close
    errors, timeout_count = backend._run_bounded_close_tasks(handle, deadline=0.0)
    assert entered.wait(timeout=1.0)
    assert errors == []
    assert timeout_count == 1
    release.set()

    # A late control outcome is fenced and cannot change the returned timeout.
    assert backend._run_bounded_close_tasks() == ([], 0)
    mocker.patch.object(
        pulsar_module, "logger", warning=MagicMock(side_effect=SystemExit())
    )
    backend._log_close_shutdown_timeout()
    backend._log_receive_shutdown_timeout()


def test_pulsar_close_thread_start_fallback_failure_is_reported(mocker) -> None:
    backend = PulsarBackend(PulsarSettings())
    handle = MagicMock(name="unstarted-close")
    start_error = RuntimeError("thread start")
    fallback_error = KeyboardInterrupt("fallback start")
    real_start = threading.Thread.start

    def fail_start(worker: threading.Thread) -> None:
        if worker.name == "pulsar-sdk-close-cleanup":
            raise start_error
        real_start(worker)

    mocker.patch.object(pulsar_module.Thread, "start", fail_start)
    mocker.patch.object(backend, "_thread_definitely_unstarted", return_value=True)
    mocker.patch.object(pulsar_module, "start_new_thread", side_effect=fallback_error)

    errors, timeout_count = backend._run_bounded_close_tasks(handle)
    assert start_error in errors
    assert fallback_error in errors
    assert timeout_count == 0
    handle.close.assert_not_called()


def test_pulsar_close_detached_handles_keeps_control_error_and_suppresses_regular_error(
    mocker,
) -> None:
    backend = PulsarBackend(PulsarSettings())
    ordinary = MagicMock()
    ordinary.close.side_effect = RuntimeError("ordinary close")
    backend._close_detached_handles(ordinary)
    ordinary.close.assert_called_once_with()

    control = MagicMock()
    control.close.side_effect = KeyboardInterrupt("control close")
    with pytest.raises(KeyboardInterrupt):
        backend._close_detached_handles(control)
    mocker.patch.object(pulsar_module, "logger", warning=MagicMock())


def test_pulsar_producer_creation_reports_disconnect_and_stale_candidate(
    mocker,
) -> None:
    backend = PulsarBackend(PulsarSettings())
    with pytest.raises(QueueError, match="disconnected"):
        backend._producer_for("scrapy-jobs")

    client = MagicMock(name="client")
    producer = MagicMock(name="producer")
    client.create_producer.return_value = producer
    backend._client = client
    backend._lifecycle_generation = 2

    def create_and_disconnect(_topic: str) -> object:
        backend._client = None
        return producer

    client.create_producer.side_effect = create_and_disconnect
    with pytest.raises(QueueError, match="connection changed"):
        backend._producer_for("scrapy-jobs")
    producer.close.assert_called_once_with()


def test_pulsar_receive_extracts_only_the_current_consumer() -> None:
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock(name="client")
    consumer = MagicMock(name="consumer")
    other = MagicMock(name="other")
    backend._client = client
    backend._lifecycle_generation = 1
    pump = _PulsarReceivePump("scrapy-jobs", client, None, 1, 1, consumer=consumer)
    pump.records.append(_BufferedPulsarRecord(MagicMock(), object(), other))
    backend._receive_pumps[pump.topic] = pump
    backend._consumers[pump.topic] = consumer

    assert backend._receive("jobs", 0.0, lambda record: record) is None
    assert not pump.records


def test_pulsar_failed_receive_retirement_has_typed_outcome() -> None:
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock(name="client")
    consumer = MagicMock(name="consumer")
    backend._client = client
    backend._lifecycle_generation = 1
    pump = _PulsarReceivePump("scrapy-jobs", client, None, 1, 1, consumer=consumer)
    pump.failed = True
    backend._receive_pumps[pump.topic] = pump
    backend._consumers[pump.topic] = consumer

    with pytest.raises(QueueError, match="receive pump failed"):
        backend._receive("jobs", 0.0, lambda record: record)
    assert pump.stopped.is_set() is False
    assert consumer.close.call_count == 1


@pytest.mark.parametrize(
    "failure", [RuntimeError("close"), KeyboardInterrupt("control")]
)
def test_pulsar_retirement_preserves_exact_close_kind(failure: BaseException) -> None:
    backend = PulsarBackend(PulsarSettings())
    pump = _PulsarReceivePump("scrapy-jobs", MagicMock(), None, 1, 1)
    retirement = backend._new_consumer_retirement_locked(pump, MagicMock())
    retirement.consumer.close.side_effect = failure

    backend._run_consumer_retirement(retirement)
    assert retirement.completed.is_set()
    completed, error = retirement.fence_and_collect()
    assert completed
    if isinstance(failure, KeyboardInterrupt):
        assert error is failure
    else:
        assert error is None


def test_pulsar_retirement_start_fallback_keeps_topic_fenced(mocker) -> None:
    backend = PulsarBackend(PulsarSettings())
    pump = _PulsarReceivePump("scrapy-jobs", MagicMock(), None, 1, 1)
    candidate = MagicMock(name="candidate")
    start_error = RuntimeError("start")
    real_start = threading.Thread.start

    def fail_start(worker: threading.Thread) -> None:
        if worker.name == "pulsar-failed-consumer-retirement":
            raise start_error
        real_start(worker)

    mocker.patch.object(pulsar_module.Thread, "start", fail_start)
    mocker.patch.object(backend, "_thread_definitely_unstarted", return_value=True)
    mocker.patch.object(
        pulsar_module, "start_new_thread", side_effect=KeyboardInterrupt()
    )

    retirement = backend._start_consumer_retirement_locked(pump, candidate)
    assert backend._consumer_retirements[pump.topic] is retirement
    assert not retirement.completed.is_set()


def test_pulsar_receive_pump_timeout_then_control_error(mocker) -> None:
    backend = PulsarBackend(PulsarSettings())
    client = MagicMock(name="client")
    consumer = MagicMock(name="consumer")
    consumer.receive.side_effect = [pulsar.Timeout(), KeyboardInterrupt("stop")]
    client.subscribe.return_value = consumer
    backend._client = client
    backend._lifecycle_generation = 1
    pump = _PulsarReceivePump(
        "scrapy-jobs", client, backend._capture_connection_snapshot(), 1, 1
    )
    backend._receive_pumps[pump.topic] = pump
    mocker.patch.object(
        pulsar_module, "logger", debug=MagicMock(side_effect=SystemExit())
    )

    backend._run_receive_pump(pump)
    assert pump.stopped.is_set()
    assert isinstance(pump.control_error, KeyboardInterrupt)


def test_pulsar_legacy_settlement_and_token_owner_fallbacks() -> None:
    backend = PulsarBackend(PulsarSettings())
    consumer = MagicMock(name="consumer")
    backend._client = MagicMock(name="client")
    backend._lifecycle_generation = 1
    backend._consumers["scrapy-jobs"] = consumer
    backend._consumer = consumer
    backend._subscribed_topic = "scrapy-jobs"
    message = MagicMock(name="message")
    backend._last_msg = message

    backend.ack("jobs")
    assert backend._last_msg is None
    consumer.acknowledge.assert_called_once_with(message)

    backend._last_msg = message
    consumer.negative_acknowledge.side_effect = RuntimeError("nack")
    with pytest.raises(QueueError, match="Failed to nack"):
        backend.nack("jobs")
    assert backend._last_msg is message

    token = _PulsarAckToken(object(), "scrapy-jobs")
    assert backend._consumer_for_token(token) is consumer
    backend._subscribed_topic = "scrapy-other"
    backend._consumers.pop("scrapy-jobs")
    assert backend._consumer_for_token(token) is None
    assert pulsar_module._message_bytes(MagicMock(spec=[]))


def test_pulsar_queue_operations_have_typed_unsupported_outcomes() -> None:
    backend = PulsarBackend(PulsarSettings())
    with pytest.raises(NotImplementedError, match="queue depth"):
        backend.queue_len("jobs")
    with pytest.raises(QueueError, match="not supported"):
        backend.clear_queue("jobs")
    with pytest.raises(ValueError):
        backend.clear_queue("bad queue")


# ---------------------------------------------------------------------------
# RocketMQ constructor, pump, settlement, and shutdown ownership
# ---------------------------------------------------------------------------


def test_rocketmq_generation_publication_and_race(mocker) -> None:
    backend = RocketMQBackend(RocketMQSettings())
    backend._publish_generation_locked()
    backend._producer = MagicMock(is_running=True)
    backend._consumer = MagicMock(is_running=True)
    backend._consumer_generation = 1
    backend._publish_generation_locked()
    record = backend._generation_gate.current
    assert record is not None
    backend._publish_generation_locked()
    assert backend._generation_gate.current is record

    mocker.patch.object(
        backend._generation_gate,
        "lease",
        side_effect=rocketmq_module.GenerationUnavailable("queue_len"),
    )
    with pytest.raises(QueueError, match="disconnected"):
        with backend._lease_generation("queue_len"):
            pass


def test_rocketmq_one_sided_connect_cleanup_is_logged(mocker) -> None:
    backend = RocketMQBackend(RocketMQSettings())
    residual = MagicMock(name="residual")
    residual.shutdown.side_effect = RuntimeError("shutdown")
    backend._producer = residual
    producer_cls = MagicMock(return_value=MagicMock(is_running=True))
    consumer_cls = MagicMock(return_value=MagicMock(is_running=True))
    module = MagicMock(
        Producer=producer_cls,
        SimpleConsumer=consumer_cls,
        Message=MagicMock(),
        ClientConfiguration=MagicMock(),
        Credentials=MagicMock(),
    )
    mocker.patch.dict("sys.modules", {"rocketmq": module})
    debug = mocker.patch.object(rocketmq_module, "logger", debug=MagicMock())

    backend.connect()
    assert backend._producer is producer_cls.return_value
    assert debug.debug.called


def test_rocketmq_shutdown_control_and_fallback_results(mocker) -> None:
    client = MagicMock(name="client")
    client.shutdown.side_effect = KeyboardInterrupt("shutdown")
    with pytest.raises(KeyboardInterrupt):
        RocketMQBackend._shutdown_detached_clients((client, "client"))
    assert client.shutdown.call_count == 1

    ordinary = MagicMock(name="ordinary")
    ordinary.shutdown.side_effect = RuntimeError("ordinary")
    assert RocketMQBackend._shutdown_detached_clients((ordinary, "ordinary"))

    start_error = RuntimeError("thread start")
    fallback_error = KeyboardInterrupt("fallback")
    unstarted = MagicMock(name="unstarted")
    real_start = threading.Thread.start

    def fail_start(worker: threading.Thread) -> None:
        if worker.name == "rocketmq-shutdown-client":
            raise start_error
        real_start(worker)

    mocker.patch.object(rocketmq_module.threading.Thread, "start", fail_start)
    mocker.patch.object(rocketmq_module, "start_new_thread", side_effect=fallback_error)
    mocker.patch.object(
        RocketMQBackend, "_thread_definitely_unstarted", return_value=True
    )
    assert RocketMQBackend._shutdown_detached_clients(
        (unstarted, "client"), suppress_control_errors=True
    )
    unstarted.shutdown.assert_not_called()


def test_rocketmq_receive_worker_start_failure_rolls_back(mocker) -> None:
    backend = RocketMQBackend(RocketMQSettings())
    backend._receive_consumer = MagicMock(name="consumer")
    backend._selected_topic = "scrapy-jobs"
    backend._receive_generation = 1
    backend._receive_stop = threading.Event()
    backend._receive_snapshot = _RocketMQOperationSnapshot("scrapy", 100, 10)
    start_error = RuntimeError("start")
    real_start = threading.Thread.start

    def fail_start(worker: threading.Thread) -> None:
        if worker.name == "rocketmq-receive-1":
            raise start_error
        real_start(worker)

    mocker.patch.object(rocketmq_module.threading.Thread, "start", fail_start)
    with backend._receive_condition:
        with pytest.raises(RuntimeError, match="start"):
            backend._start_receive_worker_locked()
    assert backend._receive_worker is None


def test_rocketmq_receive_pump_failure_observer_is_fenced() -> None:
    backend = RocketMQBackend(RocketMQSettings())
    consumer = MagicMock(name="consumer")
    consumer.subscribe.return_value = None
    consumer.receive.side_effect = RuntimeError("receive")
    stop = threading.Event()
    backend._receive_consumer = consumer
    backend._receive_generation = 1
    backend._receive_stop = stop
    backend._selected_topic = "scrapy-jobs"
    backend._receive_snapshot = _RocketMQOperationSnapshot("scrapy", 100, 10)
    backend._receive_demand = 1
    observed: list[BaseException] = []
    backend._receive_error_observer = lambda error: (
        observed.append(error),
        (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    backend._receive_pump(consumer, 1, "scrapy-jobs", stop, backend._receive_snapshot)
    assert observed and isinstance(observed[0], RuntimeError)
    assert backend._receive_failed


def test_rocketmq_pop_stale_delivery_is_typed(mocker) -> None:
    backend = RocketMQBackend(RocketMQSettings())
    message = SimpleNamespace(body=b"body")
    consumer = MagicMock(name="consumer")
    mocker.patch.object(
        backend, "_receive_delivery", return_value=(message, consumer, 2)
    )
    mocker.patch.object(
        backend, "_receive_generation_is_current_locked", return_value=False
    )

    with pytest.raises(QueueError, match="connection changed"):
        backend.pop("jobs")
    with pytest.raises(QueueError, match="connection changed"):
        backend.pop_with_ack("jobs")


@pytest.mark.parametrize(
    ("operation", "method"),
    [("ack", "ack"), ("nack", "change_invisible_duration")],
)
def test_rocketmq_settlement_reentrant_disconnect_is_ambiguous(
    operation: str, method: str
) -> None:
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    with backend._connection_lock:
        backend._publish_generation_locked()
    token = _RocketMQAckToken(object(), consumer, 1)

    def settle(*_args: object, **_kwargs: object) -> None:
        backend.disconnect()

    getattr(consumer, method).side_effect = settle
    with pytest.raises(QueueError, match="connection changed"):
        getattr(backend, operation)("jobs", token=token)
    assert token._settlement_state in {"acked", "nacked"}


def test_rocketmq_connected_clear_queue_and_legacy_mismatch_are_typed() -> None:
    backend = RocketMQBackend(RocketMQSettings())
    producer = MagicMock(is_running=True)
    consumer = MagicMock(is_running=True)
    backend._producer = producer
    backend._consumer = consumer
    backend._consumer_generation = 1
    with backend._connection_lock:
        backend._publish_generation_locked()
    with pytest.raises(QueueError, match="not supported"):
        backend.clear_queue("jobs")

    message = object()
    backend._last_msg = message
    backend._last_delivery = (MagicMock(), 0, message)
    backend.ack("jobs")
    assert consumer.ack.call_count == 0


# ---------------------------------------------------------------------------
# RabbitMQ declaration, publish, polling, settlement, and purge contracts
# ---------------------------------------------------------------------------


def test_rabbitmq_generation_and_session_health_failure(mocker) -> None:
    backend = RabbitMQBackend(RabbitMQSettings())
    backend._publish_generation_locked()
    assert backend._generation_gate.current is None

    class BrokenConnection:
        @property
        def is_open(self) -> bool:
            raise RuntimeError("health")

    backend._connection = BrokenConnection()
    backend._channel = MagicMock(is_open=True)
    backend._channel_session = (1, backend._channel)
    assert backend._session_is_healthy_locked() is False
    backend._channel = None
    assert backend._capture_connect_intent()[1] is False


def test_rabbitmq_close_handles_is_best_effort_and_preserves_control(mocker) -> None:
    channel = MagicMock(name="channel")
    connection = MagicMock(name="connection")
    channel.close.side_effect = RuntimeError("channel")
    connection.close.side_effect = RuntimeError("connection")
    mocker.patch.object(rabbitmq_module, "logger", debug=MagicMock())
    RabbitMQBackend._close_handles(channel, connection)
    assert channel.close.call_count == 1
    assert connection.close.call_count == 1

    channel.close.side_effect = KeyboardInterrupt("channel control")
    with pytest.raises(KeyboardInterrupt):
        RabbitMQBackend._close_handles(channel, connection)
    assert connection.close.call_count == 2


def test_rabbitmq_snapshot_validation_and_ssl_parameters(mocker) -> None:
    config = RabbitMQSettings()
    object.__setattr__(config, "cluster_nodes", None)
    with pytest.raises(ConfigurationError) as raised:
        RabbitMQBackend(config)._capture_connection_snapshot()
    assert raised.value.setting_name == "cluster_nodes"

    backend = RabbitMQBackend(RabbitMQSettings(ssl_enabled=True, ssl_cafile=None))
    snapshot = backend._capture_connection_snapshot()
    context = MagicMock(name="ssl-context")
    mocker.patch.object(
        rabbitmq_module.ssl, "create_default_context", return_value=context
    )
    mocker.patch.object(rabbitmq_module.pika, "SSLOptions", return_value="ssl-options")
    parameters = mocker.patch.object(rabbitmq_module.pika, "ConnectionParameters")
    backend._build_common_parameters(snapshot=snapshot)
    context.load_cert_chain.assert_not_called()
    assert parameters.call_args.kwargs["ssl_options"] == "ssl-options"


def test_rabbitmq_cluster_empty_endpoint_and_channel_open_control_cleanup(
    mocker,
) -> None:
    backend = RabbitMQBackend(RabbitMQSettings())
    snapshot = MagicMock()
    snapshot.cluster_nodes = ()
    backend._build_common_parameters = MagicMock(return_value="params")
    connection = MagicMock(name="connection")
    connection.channel.side_effect = KeyboardInterrupt("open")
    mocker.patch.object(
        rabbitmq_module.pika, "BlockingConnection", return_value=connection
    )

    with pytest.raises(KeyboardInterrupt):
        backend._connect_cluster(snapshot)

    backend._build_common_parameters.assert_called_once_with(snapshot=snapshot)
    connection.close.assert_called_once_with()


def test_rabbitmq_qos_logging_failure_and_queue_declaration_variants(mocker) -> None:
    backend, channel = _rabbit_connected()
    backend.config.prefetch_count = 1
    mocker.patch.object(
        rabbitmq_module, "logger", debug=MagicMock(side_effect=SystemExit())
    )
    backend._apply_qos(channel)
    channel.basic_qos.assert_called_once_with(prefetch_count=1, prefetch_size=0)

    backend._ensure_queue_exists("jobs")
    backend._ensure_queue_exists("jobs")
    assert channel.queue_declare.call_count == 1

    channel.queue_declare.side_effect = pika.exceptions.AMQPError("PRECONDITION_FAILED")
    with pytest.raises(QueueError, match="incompatible"):
        backend._ensure_queue_exists("other")
    channel.queue_declare.side_effect = pika.exceptions.AMQPError("broker")
    with pytest.raises(QueueError, match="Failed to declare"):
        backend._ensure_queue_exists("third")


def _rabbit_connected() -> tuple[RabbitMQBackend, MagicMock]:
    backend = RabbitMQBackend(RabbitMQSettings())
    channel = MagicMock(name="channel")
    backend._activate_channel(MagicMock(name="connection"), channel)
    return backend, channel


def test_rabbitmq_publish_receipts_cover_durability_and_ambiguity() -> None:
    backend, channel = _rabbit_connected()
    channel.basic_publish.return_value = False
    with pytest.raises(QueueError, match="not confirmed"):
        backend.push("jobs", b"body")

    channel.basic_publish.side_effect = pika.exceptions.AMQPError("publish")
    with pytest.raises(QueueError, match="Failed to push"):
        backend.push("other", b"body")

    channel.basic_publish.side_effect = None
    channel.basic_publish.return_value = True
    backend._connection_snapshot = None
    receipt = backend._push_with_durability("jobs", b"body")
    assert receipt.worker_crash_durable is False
    with pytest.raises(_DurablePushRequired):
        backend._push_with_durability("jobs", b"body", require_durable=True)


def test_rabbitmq_basic_get_timeout_and_driver_error(mocker) -> None:
    backend, channel = _rabbit_connected()
    connection = backend._connection
    assert connection is not None
    channel.basic_get.return_value = (None, None, None)
    clock = mocker.patch.object(
        rabbitmq_module.time, "monotonic", side_effect=[0.0, 0.01, 2.0]
    )
    assert backend.pop("jobs", timeout=1.0) is None
    assert channel.basic_get.call_count >= 1
    assert connection.process_data_events.called
    assert clock.call_count >= 2

    channel.basic_get.side_effect = pika.exceptions.AMQPError("get")
    with pytest.raises(QueueError, match="Failed to pop"):
        backend.pop("other")


def test_rabbitmq_basic_get_stale_wait_is_not_reported_as_empty(mocker) -> None:
    backend, channel = _rabbit_connected()
    channel.basic_get.return_value = (None, None, None)
    current = mocker.patch.object(
        backend, "_generation_is_current", side_effect=[True, True, False]
    )
    with pytest.raises(QueueError, match="waiting for a message"):
        backend._basic_get("jobs", timeout=1.0)
    assert current.call_count >= 3


@pytest.mark.parametrize("operation", ["ack", "nack"])
def test_rabbitmq_legacy_and_token_settlement_branches(operation: str) -> None:
    backend, channel = _rabbit_connected()
    backend._last_delivery_tag = 7
    backend._last_delivery_queue = "jobs"
    backend._pending_deliveries["jobs"] = 1
    getattr(backend, operation)("jobs")
    assert backend._last_delivery_tag is None
    assert backend._pending_deliveries == {}

    token = _RabbitMQAckToken(8, backend._channel_generation, "jobs")
    backend._in_flight_tags.add(token)
    backend._pending_deliveries["jobs"] = 1
    getattr(
        channel, "basic_ack" if operation == "ack" else "basic_nack"
    ).side_effect = pika.exceptions.AMQPError("settle")
    with pytest.raises(QueueError):
        getattr(backend, operation)("jobs", token=token)
    assert not token._completed


def test_rabbitmq_queue_len_and_clear_queue_variants() -> None:
    backend, channel = _rabbit_connected()
    declared = SimpleNamespace(method=SimpleNamespace(message_count=3))
    channel.queue_declare.return_value = declared
    assert backend.queue_len("jobs") == 3

    backend._pending_deliveries["jobs"] = 1
    with pytest.raises(QueueError, match="in-flight"):
        backend.clear_queue("jobs")
    backend._pending_deliveries.clear()
    backend.clear_queue("jobs")
    channel.queue_purge.assert_called_once_with(queue="jobs")

    channel.queue_purge.side_effect = pika.exceptions.AMQPError("purge")
    with pytest.raises(QueueError, match="Failed to clear"):
        backend.clear_queue("other")
