"""Direct RocketMQ queue-operation terminal privacy contracts."""

from __future__ import annotations

import sys
import traceback
from collections.abc import Callable
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from scrapy_extension.backends.rocketmq import (
    _ROCKETMQ_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
    _ROCKETMQ_MAX_MESSAGE_SIZE_ERROR,
    RocketMQBackend,
    _RocketMQAckToken,
)
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import RocketMQSettings

_MARKER = "round44-rocketmq-private-marker"

pytestmark = pytest.mark.usefixtures("cleanup_rocketmq_backends")


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
    """Assert no public error surface retains private operation state."""
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


def _backend(mocker: Any, *, max_message_size: int = 1024 * 1024) -> RocketMQBackend:
    """Build a direct backend whose retained configuration is deliberately private."""
    backend = RocketMQBackend(
        RocketMQSettings(
            namesrv_address=f"{_MARKER}.example:8081",
            max_message_size=max_message_size,
        )
    )
    mocker.patch.object(backend, "is_connected", return_value=True)
    return backend


def _install_message_module(mocker: Any) -> None:
    """Install the minimal lazy-import surface used by ``push``."""
    rocketmq_module = ModuleType("rocketmq")
    rocketmq_module.Message = mocker.MagicMock()
    mocker.patch.dict(sys.modules, {"rocketmq": rocketmq_module})


def _failing_operation(mocker: Any, method_name: str) -> Callable[[], object]:
    """Return one direct public operation backed by a marker-bearing failure."""
    backend = _backend(mocker)

    if method_name == "push":
        _install_message_module(mocker)
        producer = mocker.MagicMock()
        producer.send.side_effect = RuntimeError(_MARKER)
        backend._producer = producer
        return lambda: backend.push(_MARKER, _MARKER.encode())

    if method_name in {"pop", "pop_with_ack"}:
        consumer = mocker.MagicMock()
        consumer.receive.side_effect = RuntimeError(_MARKER)
        backend._consumer = consumer
        if method_name == "pop":
            return lambda: backend.pop(_MARKER, timeout=1)
        return lambda: backend.pop_with_ack(_MARKER, timeout=1)

    if method_name in {"ack", "nack"}:
        consumer = mocker.MagicMock()
        message = SimpleNamespace(body=_MARKER.encode())
        token = _RocketMQAckToken(message, consumer, backend._consumer_generation)
        backend._consumer = consumer
        if method_name == "ack":
            consumer.ack.side_effect = RuntimeError(_MARKER)
            return lambda: backend.ack(_MARKER, token=token)
        consumer.change_invisible_duration.side_effect = RuntimeError(_MARKER)
        return lambda: backend.nack(_MARKER, token=token)

    if method_name == "clear_queue":
        return lambda: backend.clear_queue(_MARKER)

    raise AssertionError(f"Unexpected RocketMQ operation: {method_name}")


@pytest.mark.parametrize(
    ("method_name", "expected_operation", "expected_message"),
    (
        ("push", "push", "Failed to push RocketMQ message."),
        ("pop", "pop", "Failed to pop RocketMQ message."),
        ("pop_with_ack", "pop", "Failed to pop RocketMQ message."),
        ("ack", "ack", "Failed to ack RocketMQ message."),
        ("nack", "nack", "Failed to nack RocketMQ message."),
        (
            "clear_queue",
            "clear_queue",
            _ROCKETMQ_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
        ),
    ),
)
def test_direct_rocketmq_queue_operation_rebuilds_private_error_graph(
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


def test_rocketmq_max_message_size_error_is_static_and_terminal(mocker: Any) -> None:
    """The fail-fast size contract cannot expose its item or configured limit."""
    backend = _backend(mocker, max_message_size=1)

    with pytest.raises(QueueError) as exc_info:
        backend.push("jobs", _MARKER.encode())

    error = exc_info.value
    assert str(error) == _ROCKETMQ_MAX_MESSAGE_SIZE_ERROR
    assert error.operation == "push"
    assert error.queue_name is None
    _assert_terminal_error_is_redacted(error, _MARKER)


def test_rocketmq_boundary_validates_name_before_protected_operation(
    mocker: Any,
) -> None:
    """Caller-facing queue validation remains visible and prevents driver work."""
    backend = _backend(mocker)
    producer = mocker.MagicMock()
    backend._producer = producer

    with pytest.raises(ValueError, match="Invalid queue_name"):
        backend.push("invalid queue", b"payload")

    producer.send.assert_not_called()


def test_rocketmq_token_failure_is_redacted_and_remains_retryable(
    mocker: Any,
) -> None:
    """Terminal rebuilding does not consume a token when its broker ack fails."""
    backend = _backend(mocker)
    consumer = mocker.MagicMock()
    message = SimpleNamespace(body=_MARKER.encode())
    token = _RocketMQAckToken(message, consumer, backend._consumer_generation)
    backend._consumer = consumer
    consumer.ack.side_effect = [RuntimeError(_MARKER), None]

    with pytest.raises(QueueError) as exc_info:
        backend.ack(_MARKER, token=token)

    error = exc_info.value
    assert str(error) == "Failed to ack RocketMQ message."
    assert error.operation == "ack"
    assert error.queue_name is None
    _assert_terminal_error_is_redacted(error, _MARKER)
    assert token._settlement_state == "pending"

    backend.ack(_MARKER, token=token)

    assert consumer.ack.call_count == 2
    assert token._settlement_state == "acked"


def test_rocketmq_queue_boundary_preserves_control_flow_exception(mocker: Any) -> None:
    """A terminal boundary must never convert process-control flow."""
    backend = _backend(mocker)
    consumer = mocker.MagicMock()
    interrupt = KeyboardInterrupt(_MARKER)
    consumer.receive.side_effect = interrupt
    backend._consumer = consumer

    with pytest.raises(KeyboardInterrupt) as exc_info:
        backend.pop(_MARKER, timeout=1)

    assert exc_info.value is interrupt
