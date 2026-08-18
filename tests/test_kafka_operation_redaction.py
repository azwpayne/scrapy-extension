"""Direct Kafka queue-operation terminal privacy contracts."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from kafka import TopicPartition
from kafka.errors import KafkaError

from scrapy_extension.backends.kafka import (
    _KAFKA_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
    KafkaBackend,
)
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import KafkaSettings

_MARKER = "round44-kafka-private-marker"


def _assert_value_is_redacted(
    value: object, marker: str, seen: set[int] | None = None
) -> None:
    """Walk a bounded object graph without trusting redacted ``repr`` output."""
    if seen is None:
        seen = set()
    value_id = id(value)
    if value_id in seen:
        return
    seen.add(value_id)
    if isinstance(value, str):
        assert marker not in value
        return
    if isinstance(value, bytes):
        assert marker.encode() not in value
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_value_is_redacted(key, marker, seen)
            _assert_value_is_redacted(item, marker, seen)
        return
    if isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_value_is_redacted(item, marker, seen)
        return
    try:
        attributes = vars(value)
    except TypeError:
        return
    _assert_value_is_redacted(attributes, marker, seen)


def _assert_terminal_error_is_redacted(error: BaseException, marker: str) -> None:
    """Assert no public exception surface retains the private operation marker."""
    assert marker not in str(error)
    assert marker not in repr(error.args)
    assert marker not in repr(error.__dict__)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in "".join(traceback.format_exception(error))

    trace = error.__traceback__
    while trace is not None:
        frame = trace.tb_frame
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            assert marker not in repr(frame.f_locals)
            for value in frame.f_locals.values():
                _assert_value_is_redacted(value, marker)
        trace = trace.tb_next


def _backend() -> KafkaBackend:
    """Build a direct backend whose retained configuration is deliberately private."""
    return KafkaBackend(
        KafkaSettings(
            bootstrap_servers=f"{_MARKER}.example:9092", allow_remote_plaintext=True
        )
    )


def _failing_operation(mocker: Any, method_name: str) -> Callable[[], object]:
    """Return one direct public Kafka operation backed by a marker-bearing error."""
    backend = _backend()
    topic = f"scrapy-{_MARKER}"

    if method_name == "push":
        producer = mocker.MagicMock()
        producer.send.side_effect = KafkaError(_MARKER)
        backend._producer = producer
        backend._known_topics.add(topic)
        return lambda: backend.push(_MARKER, _MARKER.encode())

    if method_name in {"pop", "pop_with_ack"}:
        consumer = mocker.MagicMock()
        consumer.poll.side_effect = KafkaError(_MARKER)
        backend._consumer = consumer
        backend._subscribed_topic = topic
        if method_name == "pop":
            return lambda: backend.pop(_MARKER)
        return lambda: backend.pop_with_ack(_MARKER)

    if method_name == "ack":
        consumer = mocker.MagicMock()
        consumer.commit.side_effect = KafkaError(_MARKER)
        backend._consumer = consumer
        backend._last_record = SimpleNamespace(topic=topic, partition=0, offset=1)
        return lambda: backend.ack(_MARKER)

    if method_name == "nack":
        topic_partition = TopicPartition(topic, 0)
        consumer = mocker.MagicMock()
        consumer.assignment.return_value = {topic_partition}
        consumer.seek.side_effect = KafkaError(_MARKER)
        backend._consumer = consumer
        backend._last_record = SimpleNamespace(topic=topic, partition=0, offset=1)
        return lambda: backend.nack(_MARKER)

    if method_name == "queue_len":
        topic_partition = TopicPartition(topic, 0)
        consumer = mocker.MagicMock()
        consumer.assignment.return_value = {topic_partition}
        consumer.end_offsets.side_effect = KafkaError(_MARKER)
        backend._consumer = consumer
        return lambda: backend.queue_len(_MARKER)

    if method_name == "clear_queue":
        return lambda: backend.clear_queue(_MARKER)

    raise AssertionError(f"Unexpected Kafka operation: {method_name}")


@pytest.mark.parametrize(
    ("method_name", "expected_operation", "expected_message"),
    (
        ("push", "push", "Failed to push Kafka message."),
        ("pop", "pop", "Failed to pop Kafka message."),
        ("pop_with_ack", "pop", "Failed to pop Kafka message."),
        ("ack", "ack", "Failed to ack Kafka message."),
        ("nack", "nack", "Failed to nack Kafka message."),
        ("queue_len", "queue_len", "Failed to inspect Kafka queue."),
        (
            "clear_queue",
            "clear_queue",
            _KAFKA_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
        ),
    ),
)
def test_direct_kafka_queue_operation_rebuilds_private_error_graph(
    mocker: Any,
    method_name: str,
    expected_operation: str,
    expected_message: str,
) -> None:
    """Every direct public operation removes input, config, and driver graphs."""
    operation = _failing_operation(mocker, method_name)

    with pytest.raises(QueueError) as exc_info:
        operation()

    error = exc_info.value
    assert str(error) == expected_message
    assert error.operation == expected_operation
    assert error.queue_name is None
    _assert_terminal_error_is_redacted(error, _MARKER)


def test_kafka_boundary_validates_name_before_protected_operation(mocker: Any) -> None:
    """Caller-facing validation remains visible and prevents backend work."""
    backend = _backend()
    ensure_topic = mocker.patch.object(backend, "_ensure_topic_exists")

    with pytest.raises(ValueError, match="Invalid topic/queue name"):
        backend.push("invalid queue", b"payload")

    ensure_topic.assert_not_called()


def test_kafka_boundary_preserves_control_flow_exception(mocker: Any) -> None:
    """A public terminal boundary must never convert process-control flow."""
    backend = _backend()
    producer = mocker.MagicMock()
    interrupt = KeyboardInterrupt(_MARKER)
    producer.send.side_effect = interrupt
    backend._producer = producer
    backend._known_topics.add(f"scrapy-{_MARKER}")

    with pytest.raises(KeyboardInterrupt) as exc_info:
        backend.push(_MARKER, _MARKER.encode())

    assert exc_info.value is interrupt
