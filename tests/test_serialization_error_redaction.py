"""Terminal privacy contracts for public queue and pipeline serialization."""

from __future__ import annotations

import sys
import traceback
from typing import Any

import pytest
from scrapy.http import Request

from scrapy_extension.exceptions import SerializationError
from scrapy_extension.exceptions._redaction import serialization_error_boundary
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.queue.queue import BackendQueue

_MARKER = "round45-serialization-private-marker"


def _assert_value_graph_is_redacted(
    value: object, marker: str, seen: set[int] | None = None
) -> None:
    """Walk a bounded object graph without relying on a redacted repr."""
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
            _assert_value_graph_is_redacted(key, marker, seen)
            _assert_value_graph_is_redacted(item, marker, seen)
        return
    if isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_value_graph_is_redacted(item, marker, seen)
        return
    try:
        attributes = vars(value)
    except TypeError:
        return
    _assert_value_graph_is_redacted(attributes, marker, seen)


def _assert_terminal_error_is_redacted(error: BaseException, marker: str) -> None:
    """Assert an exception, its graph, and package-frame locals omit ``marker``."""
    assert marker not in str(error)
    assert marker not in repr(error.args)
    assert marker not in repr(error.__dict__)
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_value_graph_is_redacted(error, marker)
    assert marker not in "".join(traceback.format_exception(error))

    trace = error.__traceback__
    while trace is not None:
        frame = trace.tb_frame
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            _assert_value_graph_is_redacted(frame.f_locals, marker)
        trace = trace.tb_next


def _capture_monitor_errors(
    mocker: Any,
) -> tuple[Any, list[tuple[str, BaseException]], list[object]]:
    """Return a monitor spy that records event objects and active exceptions."""
    monitor = mocker.MagicMock()
    events: list[tuple[str, BaseException]] = []
    active_errors: list[object] = []

    def on_error(operation: str, error: BaseException) -> None:
        active_errors.append(sys.exc_info()[1])
        events.append((operation, error))

    monitor.on_error.side_effect = on_error
    return monitor, events, active_errors


@pytest.mark.parametrize("entrypoint", ("push", "_push_with_durability"))
def test_queue_push_serialization_terminals_redact_input_and_monitor_event(
    mocker: Any,
    mock_connection_manager: Any,
    entrypoint: str,
) -> None:
    """Both scheduler and public push paths release request/error graphs first."""
    monitor, events, active_errors = _capture_monitor_errors(mocker)
    queue = BackendQueue(
        connection_manager=mock_connection_manager,
        queue_name=_MARKER,
        monitor=monitor,
    )
    request = Request(
        url=f"https://{_MARKER}.example/path",
        headers={"X-Private": _MARKER},
        body=_MARKER.encode(),
    )

    def fail_request_conversion(_: Request) -> dict[str, object]:
        private_state = {"marker": _MARKER}
        raise RuntimeError(private_state["marker"])

    queue._request_to_dict = fail_request_conversion

    with pytest.raises(SerializationError) as exc_info:
        getattr(queue, entrypoint)(request)

    error = exc_info.value
    assert type(error) is SerializationError
    assert str(error) == "Failed to serialize request."
    assert error.data is None
    assert error.serializer == "json"
    _assert_terminal_error_is_redacted(error, _MARKER)

    assert active_errors == [None]
    assert len(events) == 1
    operation, event_error = events[0]
    assert operation == "push"
    assert type(event_error) is SerializationError
    assert str(event_error) == "Failed to serialize request."
    assert event_error.data is None
    assert event_error.serializer == "json"
    _assert_terminal_error_is_redacted(event_error, _MARKER)


def test_queue_pop_serialization_terminal_redacts_payload_and_monitor_event(
    mocker: Any,
    mock_connection_manager: Any,
) -> None:
    """A malformed broker payload cannot escape via the pop exception or monitor."""
    monitor, events, active_errors = _capture_monitor_errors(mocker)
    queue = BackendQueue(
        connection_manager=mock_connection_manager,
        queue_name=_MARKER,
        monitor=monitor,
    )
    mock_connection_manager.get_queue_backend().pop.return_value = _MARKER.encode()

    def fail_deserialization(_: bytes) -> object:
        private_state = {"marker": _MARKER}
        raise RuntimeError(private_state["marker"])

    queue._serializer.deserialize = fail_deserialization

    with pytest.raises(SerializationError) as exc_info:
        queue.pop()

    error = exc_info.value
    assert type(error) is SerializationError
    assert str(error) == "Failed to deserialize request."
    assert error.data is None
    assert error.serializer == "json"
    _assert_terminal_error_is_redacted(error, _MARKER)

    assert active_errors == [None]
    assert len(events) == 1
    operation, event_error = events[0]
    assert operation == "pop"
    assert type(event_error) is SerializationError
    assert str(event_error) == "Failed to deserialize request."
    assert event_error.data is None
    _assert_terminal_error_is_redacted(event_error, _MARKER)


def test_pipeline_serialization_terminal_redacts_item_and_monitor_event(
    mocker: Any,
    mock_connection_manager: Any,
) -> None:
    """A pipeline serializer error releases the item before reporting or raising."""
    monitor, events, active_errors = _capture_monitor_errors(mocker)
    pipeline = BackendPipeline(
        connection_manager=mock_connection_manager,
        key_prefix=_MARKER,
        monitor=monitor,
    )
    pipeline._storage_supported = True
    spider = mocker.MagicMock()
    spider.name = _MARKER
    item = {"private": _MARKER}

    def fail_item_serialization(_: object) -> bytes:
        private_state = {"marker": _MARKER}
        raise RuntimeError(private_state["marker"])

    pipeline._serialize_item = fail_item_serialization

    with pytest.raises(SerializationError) as exc_info:
        pipeline.process_item(item, spider)

    error = exc_info.value
    assert type(error) is SerializationError
    assert str(error) == "Failed to serialize item."
    assert error.data is None
    assert error.serializer == "json"
    _assert_terminal_error_is_redacted(error, _MARKER)
    mock_connection_manager.get_storage_backend().store.assert_not_called()

    assert active_errors == [None]
    assert len(events) == 1
    operation, event_error = events[0]
    assert operation == "store"
    assert type(event_error) is SerializationError
    assert str(event_error) == "Failed to serialize item."
    assert event_error.data is None
    _assert_terminal_error_is_redacted(event_error, _MARKER)


class _PluginSerializationError(SerializationError):
    """A plugin-owned subclass whose public exception contract is untouched."""


class _SerializationBoundaryHarness:
    _monitor = None

    @serialization_error_boundary("Static serialization failure.", serializer="json")
    def raise_plugin_error(self, error: BaseException) -> None:
        raise error


def test_serialization_boundary_preserves_plugin_subclass_and_control_flow() -> None:
    """Only exact package errors are rebuilt; extension/control contracts survive."""
    harness = _SerializationBoundaryHarness()
    plugin_error = _PluginSerializationError(_MARKER, data=_MARKER)
    interruption = KeyboardInterrupt(_MARKER)

    with pytest.raises(_PluginSerializationError) as plugin_exc_info:
        harness.raise_plugin_error(plugin_error)
    with pytest.raises(KeyboardInterrupt) as interrupt_exc_info:
        harness.raise_plugin_error(interruption)

    assert plugin_exc_info.value is plugin_error
    assert interrupt_exc_info.value is interruption
