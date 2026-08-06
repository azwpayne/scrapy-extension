"""Terminal privacy contracts for direct Memcached storage operations."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

import scrapy_extension.backends.memcached as memcached_mod
from scrapy_extension.backends.memcached import (
    MemcachedBackend,
    _clear_storage_capability_error_boundary,
)
from scrapy_extension.exceptions import StorageError
from scrapy_extension.settings import MemcachedSettings

_MARKER = "round44-memcached-private-marker"


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
    """Assert public error metadata and package-frame locals omit ``marker``."""
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


def _backend(
    mocker: Any, *, allow_flush_all: bool = True
) -> tuple[MemcachedBackend, MagicMock]:
    """Return a connected backend whose configuration retains ``_MARKER``."""
    backend = MemcachedBackend(
        MemcachedSettings(
            host=f"{_MARKER}.example",
            allow_remote_plaintext=True,
            allow_flush_all=allow_flush_all,
        )
    )
    client = mocker.MagicMock()
    client.set.return_value = True
    client.flush_all.return_value = True
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    backend.connect()
    return backend, client


def _failing_storage_operation(mocker: Any, method_name: str) -> Callable[[], object]:
    backend, client = _backend(mocker)
    failure = RuntimeError(_MARKER)
    if method_name == "store":
        client.set.side_effect = failure
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
    if method_name == "clear_storage":
        client.flush_all.side_effect = failure
        return lambda: backend.clear_storage()
    raise AssertionError(f"Unexpected storage operation: {method_name}")


@pytest.mark.parametrize(
    ("method_name", "expected_operation", "expected_message"),
    (
        ("store", "store", "Memcached storage store failed."),
        ("retrieve", "retrieve", "Memcached storage retrieve failed."),
        ("delete", "delete", "Memcached storage delete failed."),
        ("exists", "exists", "Memcached storage existence check failed."),
        ("clear_storage", "clear_storage", "Memcached storage clear failed."),
    ),
)
def test_direct_memcached_storage_operation_rebuilds_private_error_graph(
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


def test_disconnected_memcached_storage_rebuilds_private_error_graph() -> None:
    backend = MemcachedBackend(
        MemcachedSettings(
            host=f"{_MARKER}.example",
            allow_remote_plaintext=True,
        )
    )

    with pytest.raises(StorageError) as exc_info:
        backend.store(_MARKER, _MARKER.encode())

    error = exc_info.value
    assert str(error) == "Memcached storage store failed."
    assert error.operation == "store"
    assert error.key is None
    _assert_terminal_error_is_redacted(error, _MARKER)


def test_memcached_storage_boundaries_validate_before_backend_work(mocker: Any) -> None:
    backend, client = _backend(mocker)

    with pytest.raises(ValueError, match="Invalid key"):
        backend.store("invalid key", b"payload")
    with pytest.raises(ValueError, match="positive integer"):
        backend.store("valid", b"payload", ttl=0)
    with pytest.raises(ValueError, match="Invalid key"):
        backend.retrieve("invalid key")
    with pytest.raises(ValueError, match="Invalid prefix"):
        backend.clear_storage("invalid prefix")

    client.set.assert_not_called()
    client.get.assert_not_called()
    client.flush_all.assert_not_called()


def test_memcached_storage_boundary_accepts_public_keyword_data(mocker: Any) -> None:
    backend, client = _backend(mocker)

    backend.store("keyword", data=b"payload")

    client.set.assert_called_once()


@pytest.mark.parametrize(
    ("allow_flush_all", "prefix", "expected_message"),
    (
        (
            False,
            None,
            "Memcached clear_storage would flush every key on the server. Set "
            "SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=true (allow_flush_all=True) only "
            "for a dedicated cache where that destructive scope is intended.",
        ),
        (
            True,
            _MARKER,
            "Memcached flush_all does not support prefix scoping; pass "
            "prefix=None only when a server-wide flush is explicitly acceptable.",
        ),
    ),
)
def test_memcached_clear_capability_error_rebuilds_private_error_graph(
    mocker: Any,
    allow_flush_all: bool,
    prefix: str | None,
    expected_message: str,
) -> None:
    backend, client = _backend(mocker, allow_flush_all=allow_flush_all)

    with pytest.raises(NotImplementedError) as exc_info:
        backend.clear_storage(prefix)

    error = exc_info.value
    assert str(error) == expected_message
    client.flush_all.assert_not_called()
    _assert_terminal_error_is_redacted(error, _MARKER)


def test_memcached_storage_boundary_preserves_control_flow(mocker: Any) -> None:
    backend, client = _backend(mocker)
    interruption = KeyboardInterrupt(_MARKER)
    client.set.side_effect = interruption

    with pytest.raises(KeyboardInterrupt) as exc_info:
        backend.store(_MARKER, _MARKER.encode())

    assert exc_info.value is interruption


def test_memcached_capability_boundary_preserves_plugin_subclass() -> None:
    class PluginCapabilityError(NotImplementedError):
        pass

    error = PluginCapabilityError(_MARKER)

    @_clear_storage_capability_error_boundary
    def operation() -> None:
        raise error

    with pytest.raises(PluginCapabilityError) as exc_info:
        operation()

    assert exc_info.value is error


def test_memcached_capability_boundary_rebuilds_unexpected_builtin_error() -> None:
    @_clear_storage_capability_error_boundary
    def operation() -> None:
        raise NotImplementedError(_MARKER)

    with pytest.raises(NotImplementedError) as exc_info:
        operation()

    error = exc_info.value
    assert str(error) == (
        "Memcached clear_storage would flush every key on the server. Set "
        "SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=true (allow_flush_all=True) only "
        "for a dedicated cache where that destructive scope is intended."
    )
    _assert_terminal_error_is_redacted(error, _MARKER)


def test_memcached_capability_boundary_preserves_control_flow() -> None:
    interruption = KeyboardInterrupt(_MARKER)

    @_clear_storage_capability_error_boundary
    def operation() -> None:
        raise interruption

    with pytest.raises(KeyboardInterrupt) as exc_info:
        operation()

    assert exc_info.value is interruption
