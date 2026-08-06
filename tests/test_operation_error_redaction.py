"""Regression tests for terminal public queue-operation error boundaries."""

from __future__ import annotations

import base64
import gc
import traceback
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import boto3
import pulsar
import pytest

from scrapy_extension.backends.base import (
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.backends.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    CircuitBreakerOpenError,
    _ProtectedForwardedOperation,
    wrap_queue_backend,
    wrap_set_backend,
    wrap_storage_backend,
)
from scrapy_extension.backends.pulsar import PulsarBackend
from scrapy_extension.backends.sqs import SqsBackend
from scrapy_extension.exceptions import BackendError, QueueError, StorageError
from scrapy_extension.settings import PulsarSettings, SqsSettings

_MARKER = "round42a-private-marker"
_FORWARDED_MARKER = "round43b-forwarded-private-marker"


def _assert_value_is_redacted(
    value: object, marker: str, seen: set[int] | None = None
) -> None:
    """Inspect a bounded graph without relying on redacting ``repr`` methods."""
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


def _assert_operation_error_is_redacted(error: BaseException, marker: str) -> None:
    """Assert no public exception surface retains a sensitive operation marker."""
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


def _connected_pulsar_backend(mocker: Any) -> tuple[PulsarBackend, MagicMock]:
    backend = PulsarBackend(
        PulsarSettings(service_url=f"pulsar://{_MARKER}.example:6650")
    )
    client = mocker.MagicMock()
    mocker.patch.object(pulsar, "Client", return_value=client)
    backend.connect()
    return backend, client


def _connected_sqs_backend(mocker: Any) -> tuple[SqsBackend, MagicMock]:
    backend = SqsBackend(SqsSettings(endpoint_url=f"http://{_MARKER}.example:4566"))
    client = mocker.MagicMock()
    session = mocker.MagicMock()
    session.client.return_value = client
    mocker.patch.object(boto3.session, "Session", return_value=session)
    backend.connect()
    return backend, client


def test_pulsar_pop_rebuilds_driver_error_without_topic_or_config_state(
    mocker: Any,
) -> None:
    backend, client = _connected_pulsar_backend(mocker)
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError(_MARKER)
    client.subscribe.return_value = consumer

    with pytest.raises(QueueError) as exc_info:
        backend.pop(_MARKER)

    error = exc_info.value
    assert error.operation == "pop"
    assert error.queue_name is None
    _assert_operation_error_is_redacted(error, _MARKER)


def test_sqs_token_ack_rebuilds_receipt_and_driver_failure(mocker: Any) -> None:
    backend, client = _connected_sqs_backend(mocker)
    client.get_queue_url.return_value = {"QueueUrl": f"https://{_MARKER}/queue"}
    client.receive_message.return_value = {
        "Messages": [
            {
                "Body": base64.b64encode(b"payload").decode("ascii"),
                "ReceiptHandle": f"{_MARKER}-receipt",
            }
        ]
    }
    _body, token = backend.pop_with_ack(_MARKER)
    assert token is not None
    client.delete_message.side_effect = RuntimeError(_MARKER)

    with pytest.raises(QueueError) as exc_info:
        backend.ack(_MARKER, token=token)

    error = exc_info.value
    assert error.operation == "ack"
    assert error.queue_name is None
    _assert_operation_error_is_redacted(error, _MARKER)


def test_operation_boundary_leaves_input_validation_and_base_exception_untouched(
    mocker: Any,
) -> None:
    backend, client = _connected_pulsar_backend(mocker)
    consumer = mocker.MagicMock()
    interrupt = KeyboardInterrupt(_MARKER)
    consumer.receive.side_effect = interrupt
    client.subscribe.return_value = consumer

    with pytest.raises(ValueError, match="queue_name"):
        backend.pop("invalid queue name")
    with pytest.raises(KeyboardInterrupt) as exc_info:
        backend.pop(_MARKER)

    assert exc_info.value is interrupt


class _SensitiveQueueBackend(QueueBackend):
    """Minimal backend whose error graph intentionally holds private state."""

    def __init__(self, marker: str) -> None:
        self.config = SimpleNamespace(endpoint_url=f"https://{marker}.example")
        self.marker = marker

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def is_connected(self) -> bool:
        return True

    def ping(self) -> bool:
        return True

    @property
    def backend_type(self) -> BackendType:
        return BackendType.REDIS

    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        del item
        del priority
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise QueueError(
                self.marker,
                queue_name=queue_name,
                operation="push",
            ) from error

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        del queue_name
        del timeout
        return None

    def queue_len(self, queue_name: str) -> int:
        del queue_name
        return 0

    def clear_queue(self, queue_name: str) -> None:
        del queue_name


def test_breaker_proxy_rebuilds_backend_errors_and_open_error_without_bound_method() -> (
    None
):
    backend = _SensitiveQueueBackend(_MARKER)
    breaker = CircuitBreaker(f"breaker-{_MARKER}", failure_threshold=1)
    proxy = wrap_queue_backend(backend, breaker)

    with pytest.raises(QueueError) as first_error:
        proxy.push(_MARKER, _MARKER.encode())

    assert first_error.value.operation == "push"
    assert first_error.value.queue_name is None
    _assert_operation_error_is_redacted(first_error.value, _MARKER)

    with pytest.raises(CircuitBreakerOpenError) as open_error:
        proxy.push(_MARKER, _MARKER.encode())

    assert open_error.value.name == "backend-operation"
    _assert_operation_error_is_redacted(open_error.value, _MARKER)


class _SensitiveStorageBackend(StorageBackend):
    """Storage equivalent used to lock the proxy's class-preservation contract."""

    def __init__(self, marker: str) -> None:
        self.config = SimpleNamespace(endpoint_url=f"https://{marker}.example")
        self.marker = marker

    def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
        del data
        del ttl
        raise StorageError(self.marker, operation="store", key=key)

    def retrieve(self, key: str) -> bytes | None:
        del key
        return None

    def delete(self, key: str) -> bool:
        del key
        return False

    def exists(self, key: str) -> bool:
        del key
        return False

    def ttl(self, key: str) -> int | None:
        del key
        return None

    def clear_storage(self, prefix: str | None = None) -> None:
        del prefix


def test_breaker_proxy_preserves_storage_error_class_and_safe_operation() -> None:
    backend = _SensitiveStorageBackend(_MARKER)
    proxy = wrap_storage_backend(backend, CircuitBreaker(f"breaker-{_MARKER}"))

    with pytest.raises(StorageError) as exc_info:
        proxy.store(_MARKER, _MARKER.encode())

    error = exc_info.value
    assert type(error) is StorageError
    assert error.operation == "store"
    assert error.key is None
    _assert_operation_error_is_redacted(error, _MARKER)


class _SensitiveForwardedQueueBackend(_SensitiveQueueBackend):
    """Queue admin methods that retain private backend state before wrapping."""

    def queue_len(self, queue_name: str) -> int:
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise QueueError(
                self.marker,
                queue_name=queue_name,
                operation="push",
            ) from error

    def clear_queue(self, queue_name: str) -> None:
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise QueueError(
                self.marker,
                queue_name=queue_name,
                operation="pop",
            ) from error


@pytest.mark.parametrize(
    ("method_name", "expected_operation"),
    (("queue_len", "queue_len"), ("clear_queue", "clear_queue")),
)
def test_breaker_proxy_redacts_non_counting_queue_admin_errors(
    method_name: str,
    expected_operation: str,
) -> None:
    backend = _SensitiveForwardedQueueBackend(_FORWARDED_MARKER)
    breaker = CircuitBreaker(f"breaker-{_FORWARDED_MARKER}", failure_threshold=1)
    proxy = wrap_queue_backend(backend, breaker)

    with pytest.raises(QueueError) as exc_info:
        getattr(proxy, method_name)(_FORWARDED_MARKER)

    error = exc_info.value
    assert str(error) == "Backend operation failed."
    assert error.operation == expected_operation
    assert error.queue_name is None
    assert breaker.state is BreakerState.CLOSED
    assert breaker.failure_count == 0
    _assert_operation_error_is_redacted(error, _FORWARDED_MARKER)


class _SensitiveForwardedSetBackend(SetBackend):
    """Set admin methods with a deliberately unsafe generic backend error."""

    def __init__(self, marker: str) -> None:
        self.config = SimpleNamespace(endpoint_url=f"https://{marker}.example")
        self.marker = marker

    def add(self, set_name: str, item: bytes) -> bool:
        del set_name
        del item
        return False

    def remove(self, set_name: str, item: bytes) -> bool:
        del set_name
        del item
        return False

    def contains(self, set_name: str, item: bytes) -> bool:
        del set_name
        del item
        return False

    def set_len(self, set_name: str) -> int:
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise BackendError(self.marker) from error

    def clear_set(self, set_name: str) -> None:
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise BackendError(self.marker) from error


@pytest.mark.parametrize("method_name", ("set_len", "clear_set"))
def test_breaker_proxy_redacts_non_counting_set_admin_errors(method_name: str) -> None:
    backend = _SensitiveForwardedSetBackend(_FORWARDED_MARKER)
    breaker = CircuitBreaker(f"breaker-{_FORWARDED_MARKER}", failure_threshold=1)
    proxy = wrap_set_backend(backend, breaker)

    with pytest.raises(BackendError) as exc_info:
        getattr(proxy, method_name)(_FORWARDED_MARKER)

    error = exc_info.value
    assert type(error) is BackendError
    assert str(error) == "Backend operation failed."
    assert breaker.state is BreakerState.CLOSED
    assert breaker.failure_count == 0
    _assert_operation_error_is_redacted(error, _FORWARDED_MARKER)


class _SensitiveForwardedStorageBackend(_SensitiveStorageBackend):
    """Storage admin methods that expose keys before terminal reconstruction."""

    def exists(self, key: str) -> bool:
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise StorageError(self.marker, operation="delete", key=key) from error

    def ttl(self, key: str) -> int | None:
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise StorageError(self.marker, operation="retrieve", key=key) from error

    def clear_storage(self, prefix: str | None = None) -> None:
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise StorageError(self.marker, operation="store", key=prefix) from error


@pytest.mark.parametrize(
    ("method_name", "expected_operation"),
    (("exists", "exists"), ("ttl", "ttl"), ("clear_storage", "clear_storage")),
)
def test_breaker_proxy_redacts_non_counting_storage_admin_errors(
    method_name: str,
    expected_operation: str,
) -> None:
    backend = _SensitiveForwardedStorageBackend(_FORWARDED_MARKER)
    breaker = CircuitBreaker(f"breaker-{_FORWARDED_MARKER}", failure_threshold=1)
    proxy = wrap_storage_backend(backend, breaker)

    with pytest.raises(StorageError) as exc_info:
        getattr(proxy, method_name)(_FORWARDED_MARKER)

    error = exc_info.value
    assert type(error) is StorageError
    assert str(error) == "Backend operation failed."
    assert error.operation == expected_operation
    assert error.key is None
    assert breaker.state is BreakerState.CLOSED
    assert breaker.failure_count == 0
    _assert_operation_error_is_redacted(error, _FORWARDED_MARKER)


class _NonWeakrefableQueueLength:
    """Plugin-style callable that forces the protected lookup fallback."""

    __slots__ = ("marker",)

    def __init__(self, marker: str) -> None:
        self.marker = marker

    def __call__(self, queue_name: str) -> int:
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise QueueError(
                self.marker,
                queue_name=queue_name,
                operation="push",
            ) from error


def test_forwarded_proxy_uses_lookup_fallback_for_nonweakrefable_plugin_callable() -> (
    None
):
    backend = _SensitiveQueueBackend(_FORWARDED_MARKER)
    backend.queue_len = _NonWeakrefableQueueLength(_FORWARDED_MARKER)  # type: ignore[method-assign]
    breaker = CircuitBreaker(f"breaker-{_FORWARDED_MARKER}", failure_threshold=1)
    proxy = wrap_queue_backend(backend, breaker)

    with pytest.raises(QueueError) as exc_info:
        proxy.queue_len(_FORWARDED_MARKER)

    error = exc_info.value
    assert error.operation == "queue_len"
    assert error.queue_name is None
    assert breaker.state is BreakerState.CLOSED
    _assert_operation_error_is_redacted(error, _FORWARDED_MARKER)


def _make_unavailable_forwarded_operation(marker: str) -> _ProtectedForwardedOperation:
    class _EphemeralBackend:
        def __init__(self) -> None:
            self.config = SimpleNamespace(endpoint_url=f"https://{marker}.example")

        def queue_len(self, queue_name: str) -> int:
            del queue_name
            return 0

    backend = _EphemeralBackend()
    return _ProtectedForwardedOperation(backend, "queue_len", backend.queue_len)


def test_forwarded_wrapper_returns_static_error_when_weak_backend_is_unavailable() -> (
    None
):
    operation = _make_unavailable_forwarded_operation(_FORWARDED_MARKER)
    gc.collect()

    with pytest.raises(BackendError) as exc_info:
        operation(_FORWARDED_MARKER)

    error = exc_info.value
    assert type(error) is BackendError
    assert str(error) == "Backend operation is unavailable."
    _assert_operation_error_is_redacted(error, _FORWARDED_MARKER)
