"""Terminal privacy contracts for direct ElasticSearch public operations."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from elasticsearch import ApiError, ConflictError, RequestError, TransportError

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
    StorageError,
)
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

_MARKER = "round44-elasticsearch-private-marker"


class _PluginConnectionError(BackendConnectionError):
    """A plugin-owned connection subclass that must retain its contract."""


class _PluginStorageError(StorageError):
    """A plugin-owned storage subclass that must retain its contract."""


def _assert_value_graph_is_redacted(
    value: object, marker: str, seen: set[int] | None = None
) -> None:
    """Walk a bounded public graph without trusting a redacted ``repr``."""
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
    """Assert public metadata and package traceback frames omit ``marker``."""
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
            assert marker not in repr(frame.f_locals)
            for value in frame.f_locals.values():
                _assert_value_graph_is_redacted(value, marker)
        trace = trace.tb_next


def _backend(mocker: Any) -> tuple[ElasticSearchBackend, Any]:
    """Build a direct backend retaining deliberately private mutable settings."""
    config = ElasticSearchSettings(hosts=[f"http://{_MARKER}.example:9200"])
    backend = ElasticSearchBackend(config)
    client = mocker.MagicMock()
    backend._client = client
    backend._connection_snapshot = backend._capture_connection_snapshot()
    return backend, client


def _failing_queue_operation(mocker: Any, method_name: str) -> Callable[[], object]:
    backend, client = _backend(mocker)
    failure = TransportError(_MARKER)
    if method_name == "push":
        client.index.side_effect = failure
        return lambda: backend.push(_MARKER, _MARKER.encode())
    if method_name in {"pop", "pop_with_ack"}:
        client.indices.refresh.side_effect = failure
        if method_name == "pop":
            return lambda: backend.pop(_MARKER)
        return lambda: backend.pop_with_ack(_MARKER)
    if method_name == "queue_len":
        client.count.side_effect = failure
        return lambda: backend.queue_len(_MARKER)
    if method_name == "clear_queue":
        client.delete_by_query.side_effect = failure
        return lambda: backend.clear_queue(_MARKER)
    raise AssertionError(f"Unexpected queue operation: {method_name}")


@pytest.mark.parametrize(
    ("method_name", "expected_operation", "expected_message"),
    (
        ("push", "push", "ElasticSearch queue push failed."),
        ("pop", "pop", "ElasticSearch queue pop failed."),
        ("pop_with_ack", "pop", "ElasticSearch queue pop failed."),
        ("queue_len", "queue_len", "ElasticSearch queue length read failed."),
        ("clear_queue", "clear_queue", "ElasticSearch queue clear failed."),
    ),
)
def test_direct_elasticsearch_queue_operation_rebuilds_private_error_graph(
    mocker: Any,
    method_name: str,
    expected_operation: str,
    expected_message: str,
) -> None:
    operation = _failing_queue_operation(mocker, method_name)

    with pytest.raises(QueueError) as exc_info:
        operation()

    error = exc_info.value
    assert str(error) == expected_message
    assert error.operation == expected_operation
    assert error.queue_name is None
    _assert_terminal_error_is_redacted(error, _MARKER)


def _failing_set_operation(mocker: Any, method_name: str) -> Callable[[], object]:
    backend, client = _backend(mocker)
    failure = TransportError(_MARKER)
    if method_name == "add":
        client.index.side_effect = failure
        return lambda: backend.add(_MARKER, _MARKER.encode())
    if method_name == "remove":
        client.delete.side_effect = failure
        return lambda: backend.remove(_MARKER, _MARKER.encode())
    if method_name == "contains":
        client.exists.side_effect = failure
        return lambda: backend.contains(_MARKER, _MARKER.encode())
    if method_name == "set_len":
        client.count.side_effect = failure
        return lambda: backend.set_len(_MARKER)
    if method_name == "clear_set":
        client.delete_by_query.side_effect = failure
        return lambda: backend.clear_set(_MARKER)
    raise AssertionError(f"Unexpected set operation: {method_name}")


@pytest.mark.parametrize(
    ("method_name", "expected_message"),
    (
        ("add", "ElasticSearch set add failed."),
        ("remove", "ElasticSearch set remove failed."),
        ("contains", "ElasticSearch set membership check failed."),
        ("set_len", "ElasticSearch set length read failed."),
        ("clear_set", "ElasticSearch set clear failed."),
    ),
)
def test_direct_elasticsearch_set_operation_rebuilds_private_error_graph(
    mocker: Any, method_name: str, expected_message: str
) -> None:
    operation = _failing_set_operation(mocker, method_name)

    with pytest.raises(BackendConnectionError) as exc_info:
        operation()

    error = exc_info.value
    assert str(error) == expected_message
    assert error.backend_type == "elasticsearch"
    _assert_terminal_error_is_redacted(error, _MARKER)


def _failing_storage_operation(mocker: Any, method_name: str) -> Callable[[], object]:
    backend, client = _backend(mocker)
    failure = TransportError(_MARKER)
    if method_name == "store":
        client.index.side_effect = failure
        return lambda: backend.store(_MARKER, _MARKER.encode())
    if method_name == "retrieve":
        client.get.side_effect = failure
        return lambda: backend.retrieve(_MARKER)
    if method_name == "delete":
        client.delete.side_effect = failure
        return lambda: backend.delete(_MARKER)
    if method_name == "exists":
        client.get.side_effect = failure
        return lambda: backend.exists(_MARKER)
    if method_name == "ttl":
        client.get.side_effect = failure
        return lambda: backend.ttl(_MARKER)
    if method_name == "clear_storage":
        client.delete_by_query.side_effect = failure
        return lambda: backend.clear_storage(_MARKER)
    raise AssertionError(f"Unexpected storage operation: {method_name}")


@pytest.mark.parametrize(
    ("method_name", "expected_operation", "expected_message"),
    (
        ("store", "store", "ElasticSearch storage store failed."),
        ("retrieve", "retrieve", "ElasticSearch storage retrieve failed."),
        ("delete", "delete", "ElasticSearch storage delete failed."),
        ("exists", "exists", "ElasticSearch storage existence check failed."),
        ("ttl", "ttl", "ElasticSearch storage TTL read failed."),
        ("clear_storage", "clear_storage", "ElasticSearch storage clear failed."),
    ),
)
def test_direct_elasticsearch_storage_operation_rebuilds_private_error_graph(
    mocker: Any,
    method_name: str,
    expected_operation: str,
    expected_message: str,
) -> None:
    operation = _failing_storage_operation(mocker, method_name)

    with pytest.raises(StorageError) as exc_info:
        operation()

    error = exc_info.value
    assert str(error) == expected_message
    assert error.operation == expected_operation
    assert error.key is None
    _assert_terminal_error_is_redacted(error, _MARKER)


def _failing_set_api_operation(mocker: Any, method_name: str) -> Callable[[], object]:
    backend, client = _backend(mocker)
    failure = ApiError(_MARKER, mocker.MagicMock(), {})
    if method_name == "add":
        client.index.side_effect = failure
        return lambda: backend.add(_MARKER, _MARKER.encode())
    if method_name == "remove":
        client.delete.side_effect = failure
        return lambda: backend.remove(_MARKER, _MARKER.encode())
    if method_name == "contains":
        client.exists.side_effect = failure
        return lambda: backend.contains(_MARKER, _MARKER.encode())
    if method_name == "set_len":
        client.count.side_effect = failure
        return lambda: backend.set_len(_MARKER)
    if method_name == "clear_set":
        client.delete_by_query.side_effect = failure
        return lambda: backend.clear_set(_MARKER)
    raise AssertionError(f"Unexpected set operation: {method_name}")


@pytest.mark.parametrize(
    "method_name", ("add", "remove", "contains", "set_len", "clear_set")
)
def test_elasticsearch_set_api_rejection_is_non_transient_and_redacted(
    mocker: Any, method_name: str
) -> None:
    operation = _failing_set_api_operation(mocker, method_name)

    with pytest.raises(ConfigurationError) as exc_info:
        operation()

    error = exc_info.value
    assert str(error) == "ElasticSearch set request was rejected."
    assert error.setting_name == "operation"
    _assert_terminal_error_is_redacted(error, _MARKER)


def test_elasticsearch_set_non_conflict_request_error_is_non_transient_and_redacted(
    mocker: Any,
) -> None:
    backend, client = _backend(mocker)
    client.index.side_effect = RequestError(
        "400", mocker.MagicMock(), {"error": _MARKER}
    )

    with pytest.raises(ConfigurationError) as exc_info:
        backend.add(_MARKER, _MARKER.encode())

    error = exc_info.value
    assert str(error) == "ElasticSearch set request was rejected."
    assert error.setting_name == "operation"
    _assert_terminal_error_is_redacted(error, _MARKER)


@pytest.mark.parametrize("error_type", (ConflictError, RequestError))
def test_elasticsearch_set_duplicate_conflict_still_returns_false(
    mocker: Any, error_type: type[ApiError]
) -> None:
    backend, client = _backend(mocker)
    if error_type is ConflictError:
        failure = ConflictError(_MARKER, mocker.MagicMock(), {})
    else:
        failure = RequestError(
            "409", mocker.MagicMock(), {"error": "version_conflict_engine_exception"}
        )
    client.index.side_effect = failure

    assert backend.add(_MARKER, _MARKER.encode()) is False


@pytest.mark.parametrize(
    ("operation_kind", "expected_type", "expected_operation"),
    (
        ("queue", QueueError, "push"),
        ("set", BackendConnectionError, None),
        ("storage", BackendConnectionError, None),
    ),
)
def test_elasticsearch_disconnected_operation_rebuilds_private_error_graph(
    mocker: Any,
    operation_kind: str,
    expected_type: type[Exception],
    expected_operation: str | None,
) -> None:
    config = ElasticSearchSettings(hosts=[f"http://{_MARKER}.example:9200"])
    backend = ElasticSearchBackend(config)
    mocker.patch.object(
        backend,
        "connect",
        side_effect=BackendConnectionError(_MARKER, backend_type="plugin"),
    )
    if operation_kind == "queue":
        operation = lambda: backend.push(_MARKER, _MARKER.encode())
    elif operation_kind == "set":
        operation = lambda: backend.add(_MARKER, _MARKER.encode())
    else:
        operation = lambda: backend.store(_MARKER, _MARKER.encode())

    with pytest.raises(expected_type) as exc_info:
        operation()

    error = exc_info.value
    if isinstance(error, QueueError):
        assert error.operation == expected_operation
        assert error.queue_name is None
    else:
        assert isinstance(error, BackendConnectionError)
        assert error.backend_type == "elasticsearch"
    _assert_terminal_error_is_redacted(error, _MARKER)


@pytest.mark.parametrize("operation_kind", ("queue", "set", "storage"))
def test_elasticsearch_terminal_boundaries_preserve_control_flow(
    mocker: Any, operation_kind: str
) -> None:
    backend, client = _backend(mocker)
    interruption = KeyboardInterrupt(_MARKER)
    if operation_kind == "queue":
        client.index.side_effect = interruption
        operation = lambda: backend.push(_MARKER, _MARKER.encode())
    elif operation_kind == "set":
        client.index.side_effect = interruption
        operation = lambda: backend.add(_MARKER, _MARKER.encode())
    else:
        client.index.side_effect = interruption
        operation = lambda: backend.store(_MARKER, _MARKER.encode())

    with pytest.raises(KeyboardInterrupt) as exc_info:
        operation()

    assert exc_info.value is interruption


@pytest.mark.parametrize("operation_kind", ("queue", "set", "storage"))
def test_elasticsearch_terminal_boundaries_preserve_unknown_exception_contract(
    mocker: Any, operation_kind: str
) -> None:
    backend, client = _backend(mocker)
    unknown = RuntimeError(_MARKER)
    client.index.side_effect = unknown
    if operation_kind == "queue":
        operation = lambda: backend.push(_MARKER, _MARKER.encode())
    elif operation_kind == "set":
        operation = lambda: backend.add(_MARKER, _MARKER.encode())
    else:
        operation = lambda: backend.store(_MARKER, _MARKER.encode())

    with pytest.raises(RuntimeError) as exc_info:
        operation()

    assert exc_info.value is unknown


def test_elasticsearch_set_boundary_preserves_plugin_connection_subclass(
    mocker: Any,
) -> None:
    backend, _client = _backend(mocker)
    plugin_error = _PluginConnectionError(_MARKER, backend_type="plugin")
    mocker.patch.object(backend, "connect", side_effect=plugin_error)
    backend._client = None

    with pytest.raises(_PluginConnectionError) as exc_info:
        backend.add(_MARKER, _MARKER.encode())

    assert exc_info.value is plugin_error


def test_elasticsearch_storage_boundary_preserves_plugin_storage_subclass(
    mocker: Any,
) -> None:
    backend, client = _backend(mocker)
    plugin_error = _PluginStorageError(_MARKER, operation="plugin", key=_MARKER)
    client.index.side_effect = plugin_error

    with pytest.raises(_PluginStorageError) as exc_info:
        backend.store(_MARKER, _MARKER.encode())

    assert exc_info.value is plugin_error


def test_elasticsearch_boundaries_validate_inputs_before_backend_work(
    mocker: Any,
) -> None:
    backend, client = _backend(mocker)

    with pytest.raises(ValueError, match="queue_name"):
        backend.push("invalid queue", b"payload")
    with pytest.raises(ValueError, match="set_name"):
        backend.add("invalid/set", b"payload")
    with pytest.raises(ValueError, match="positive integer"):
        backend.store("valid", b"payload", ttl=0)
    with pytest.raises(ValueError, match="prefix"):
        backend.clear_storage("invalid prefix")

    client.index.assert_not_called()
    client.delete_by_query.assert_not_called()


def test_elasticsearch_store_accepts_keyword_data_argument(mocker: Any) -> None:
    """The validator must retain the public ``data=`` keyword parameter name."""
    backend, client = _backend(mocker)

    backend.store("keyword-data", data=b"payload")

    assert client.index.call_args.kwargs["document"]["data"] == "cGF5bG9hZA=="


def test_elasticsearch_expired_document_warning_omits_storage_key(mocker: Any) -> None:
    backend, client = _backend(mocker)
    client.get.return_value = {
        "_source": {
            "data": "ZGF0YQ==",
            "expireAt": (
                datetime.now(tz=timezone.utc) - timedelta(seconds=1)
            ).isoformat(),
        }
    }
    warning = mocker.patch("scrapy_extension.backends.elasticsearch.logger.warning")

    assert backend.retrieve(_MARKER) is None

    warning.assert_called_once_with(
        "Skipping unsafe reap of expired ES storage document: response omitted "
        "_seq_no/_primary_term"
    )
    assert _MARKER not in repr(warning.call_args)
