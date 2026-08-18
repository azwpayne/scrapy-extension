"""Conformance tests for third-party deferred acknowledgement plugins."""

from __future__ import annotations

import gc
import sys
import traceback
import weakref
from collections import defaultdict, deque
from types import ModuleType
from typing import Any

import pytest

from scrapy_extension.backends.base import Backend, QueueBackend
from scrapy_extension.backends.circuit_breaker import CircuitBreaker, wrap_queue_backend
from scrapy_extension.backends.connectors import (
    ConnectionManager,
    _DeferredAckPluginQueueBackend,
)
from scrapy_extension.backends.registry import BackendDescriptor, _reset_registry_cache
from scrapy_extension.exceptions import ConfigurationError, QueueError
from scrapy_extension.queue.strategies.base import QueueStrategy, _BoundQueueAckToken


class _Settings:
    def __init__(self, **values: Any) -> None:
        self.values = values


class _QueuePlugin(Backend, QueueBackend):
    requires_ack = False

    def __init__(self, settings: _Settings | None = None) -> None:
        self.settings = settings
        self.pop_results: dict[str, deque[tuple[bytes | None, object | None]]] = (
            defaultdict(deque)
        )
        self.ack_calls: list[tuple[str, object]] = []
        self.nack_calls: list[tuple[str, object]] = []

    @property
    def backend_type(self) -> str:
        return "ackplugin"

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def ping(self) -> bool:
        return True

    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        del queue_name, item, priority

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        del queue_name, timeout
        return None

    def queue_len(self, queue_name: str) -> int:
        del queue_name
        return 0

    def clear_queue(self, queue_name: str) -> None:
        del queue_name


class _DeferredPlugin(_QueuePlugin):
    requires_ack = True
    supports_concurrent_ack = True

    def pop_with_ack(
        self, queue_name: str, timeout: float = 0.0
    ) -> tuple[bytes | None, object | None]:
        del timeout
        return self.pop_results[queue_name].popleft()

    def ack(self, queue_name: str, *, token: Any | None = None) -> None:
        self.ack_calls.append((queue_name, token))

    def nack(self, queue_name: str, *, token: Any | None = None) -> None:
        self.nack_calls.append((queue_name, token))


class _SingleConcurrencyDeferredPlugin(_DeferredPlugin):
    supports_concurrent_ack = False


class _MissingDeferredMethods(_QueuePlugin):
    requires_ack = True
    supports_concurrent_ack = True


class _NonBooleanMetadata(_DeferredPlugin):
    requires_ack = 1  # type: ignore[assignment]


class _CoroutineSettlementPlugin(_DeferredPlugin):
    async def ack(self, queue_name: str, *, token: Any | None = None) -> None:
        raise AssertionError("async ack body must not be invoked")

    async def nack(self, queue_name: str, *, token: Any | None = None) -> None:
        raise AssertionError("async nack body must not be invoked")


class _CustomAwaitable:
    def __await__(self) -> Any:
        yield


class _IdentityToken:
    pass


async def _unstarted_settlement_coroutine() -> None:
    raise AssertionError("settlement coroutine body must not be invoked")


class _AwaitableSettlementPlugin(_DeferredPlugin):
    settlement_result: object | None = None

    def ack(self, queue_name: str, *, token: Any | None = None) -> None:
        self.ack_calls.append((queue_name, token))
        return self.settlement_result  # type: ignore[return-value]

    def nack(self, queue_name: str, *, token: Any | None = None) -> None:
        self.nack_calls.append((queue_name, token))
        return self.settlement_result  # type: ignore[return-value]


class _HostileMethodDescriptor:
    def __get__(self, instance: object, owner: type[object]) -> object:
        del instance, owner
        raise AssertionError("plugin method descriptor was invoked")


_PLUGIN_CLASS: type[_QueuePlugin] = _QueuePlugin


def _register_ackplugin() -> BackendDescriptor:
    return BackendDescriptor(
        backend_type="ackplugin",
        backend_cls_path=f"{__name__}.{_PLUGIN_CLASS.__name__}",
        settings_cls_path=f"{__name__}._Settings",
        capabilities=frozenset({"queue"}),
    )


class _EntryPoint:
    name = "ackplugin"

    @staticmethod
    def load() -> Any:
        return _register_ackplugin


def _install_plugin(
    monkeypatch: pytest.MonkeyPatch, plugin_class: type[_QueuePlugin]
) -> None:
    global _PLUGIN_CLASS
    _PLUGIN_CLASS = plugin_class
    monkeypatch.setattr(
        "scrapy_extension.backends.registry.importlib.metadata.entry_points",
        lambda *, group: [_EntryPoint()],
    )
    _reset_registry_cache()


def _install_descriptor(
    monkeypatch: pytest.MonkeyPatch,
    descriptor: BackendDescriptor,
) -> None:
    class _DescriptorEntryPoint:
        name = descriptor.backend_type

        @staticmethod
        def load() -> Any:
            return lambda: descriptor

    monkeypatch.setattr(
        "scrapy_extension.backends.registry.importlib.metadata.entry_points",
        lambda *, group: [_DescriptorEntryPoint()],
    )
    _reset_registry_cache()


@pytest.mark.parametrize(
    ("plugin_class", "accepted", "deferred"),
    [
        (_QueuePlugin, True, False),
        (_DeferredPlugin, True, True),
        (_SingleConcurrencyDeferredPlugin, True, True),
        (_MissingDeferredMethods, False, False),
        (_NonBooleanMetadata, False, False),
    ],
)
def test_manager_conformance_matrix_fails_before_construction_or_broker_io(
    monkeypatch: pytest.MonkeyPatch,
    plugin_class: type[_QueuePlugin],
    accepted: bool,
    deferred: bool,
) -> None:
    _install_plugin(monkeypatch, plugin_class)
    if not accepted:
        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionManager("ackplugin")
        assert str(exc_info.value) == (
            "Selected third-party queue backend has an invalid acknowledgement contract."
        )
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
        return

    manager = ConnectionManager("ackplugin")
    assert manager._deferred_ack_plugin is deferred
    assert manager._backend is None


def test_manager_constructs_the_exact_class_returned_by_dynamic_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "dynamic_ack_backend_for_test"
    dynamic_module = ModuleType(module_name)
    classes = iter((_DeferredPlugin, _QueuePlugin))
    lookups: list[str] = []

    def dynamic_getattr(name: str) -> object:
        lookups.append(name)
        return next(classes)

    dynamic_module.__getattr__ = dynamic_getattr  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module_name, dynamic_module)
    _install_descriptor(
        monkeypatch,
        BackendDescriptor(
            backend_type="ackplugin",
            backend_cls_path=f"{module_name}.Backend",
            settings_cls_path=f"{__name__}._Settings",
            capabilities=frozenset({"queue"}),
        ),
    )

    manager = ConnectionManager("ackplugin")
    backend = manager._create_backend()

    assert type(backend) is _DeferredPlugin
    assert manager._deferred_ack_plugin is True
    assert manager._plugin_supports_concurrent_ack is True
    assert manager._static_ack_capabilities() == (True, True)
    assert lookups == ["Backend"]


def test_manager_ignores_backend_module_mutation_after_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "mutable_ack_backend_for_test"
    mutable_module = ModuleType(module_name)
    mutable_module.Backend = _DeferredPlugin  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module_name, mutable_module)
    _install_descriptor(
        monkeypatch,
        BackendDescriptor(
            backend_type="ackplugin",
            backend_cls_path=f"{module_name}.Backend",
            settings_cls_path=f"{__name__}._Settings",
            capabilities=frozenset({"queue"}),
        ),
    )

    manager = ConnectionManager("ackplugin")
    mutable_module.Backend = _QueuePlugin  # type: ignore[attr-defined]

    assert type(manager._create_backend()) is _DeferredPlugin
    assert manager._static_ack_capabilities() == (True, True)


def test_manager_statically_rejects_coroutine_settlement_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _CoroutineSettlementPlugin)

    with pytest.raises(ConfigurationError, match="acknowledgement contract"):
        ConnectionManager("ackplugin")


@pytest.mark.parametrize("method_name", ["pop_with_ack", "ack", "nack"])
def test_manager_statically_rejects_signature_incompatible_overrides(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    if method_name == "pop_with_ack":

        def incompatible(self: object, queue_name: str) -> None:
            raise AssertionError("incompatible hook must not be invoked")

    else:

        def incompatible(self: object, queue_name: str) -> None:
            raise AssertionError("incompatible hook must not be invoked")

    monkeypatch.setattr(_DeferredPlugin, method_name, incompatible)
    _install_plugin(monkeypatch, _DeferredPlugin)

    with pytest.raises(ConfigurationError, match="acknowledgement contract"):
        ConnectionManager("ackplugin")


@pytest.mark.parametrize("method_name", ["pop_with_ack", "ack", "nack"])
def test_manager_statically_rejects_noncallable_method_descriptors(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    monkeypatch.setattr(
        _DeferredPlugin,
        method_name,
        _HostileMethodDescriptor(),
    )
    _install_plugin(monkeypatch, _DeferredPlugin)

    with pytest.raises(ConfigurationError, match="acknowledgement contract"):
        ConnectionManager("ackplugin")


@pytest.mark.parametrize("operation", ["ack", "nack"])
@pytest.mark.parametrize("awaitable_kind", ["coroutine", "custom"])
def test_awaitable_settlement_is_redacted_and_keeps_token_retryable(
    operation: str,
    awaitable_kind: str,
    recwarn: pytest.WarningsRecorder,
) -> None:
    marker = f"awaitable-settlement-private-{operation}-{awaitable_kind}"
    backend = _AwaitableSettlementPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    backend.pop_results["q"].append((b"item", marker))
    contract.pop_with_ack("q")
    backend.settlement_result = (
        _unstarted_settlement_coroutine()
        if awaitable_kind == "coroutine"
        else _CustomAwaitable()
    )

    with pytest.raises(QueueError) as exc_info:
        getattr(contract, operation)("q", token=marker)

    _assert_terminal_queue_error_is_redacted(exc_info.value, marker)
    assert "awaitable settlement result" in str(exc_info.value)
    backend.settlement_result = None
    getattr(contract, operation)("q", token=marker)
    calls = backend.ack_calls if operation == "ack" else backend.nack_calls
    assert calls == [("q", marker), ("q", marker)]
    assert not [
        warning for warning in recwarn if "never awaited" in str(warning.message)
    ]


def test_non_value_tokens_are_owned_and_settled_by_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    monkeypatch.setattr(
        "scrapy_extension.backends.connectors._ack_token_key",
        lambda token: ("identity", 1),
    )
    issued = _IdentityToken()
    issued_ref = weakref.ref(issued)
    backend.pop_results["q"].append((b"item", issued))
    item, returned = contract.pop_with_ack("q")
    assert item == b"item"
    assert returned is issued

    del issued
    del returned
    gc.collect()
    owned = issued_ref()
    assert owned is not None

    forged = _IdentityToken()
    with pytest.raises(QueueError, match="unknown"):
        contract.ack("q", token=forged)
    assert backend.ack_calls == []

    contract.ack("q", token=owned)
    backend.ack_calls.clear()
    del owned
    gc.collect()
    assert issued_ref() is None


def test_non_value_token_reference_is_released_on_adapter_teardown() -> None:
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    issued = _IdentityToken()
    issued_ref = weakref.ref(issued)
    backend.pop_results["q"].append((b"item", issued))
    _item, returned = contract.pop_with_ack("q")
    assert returned is issued

    del issued
    del returned
    gc.collect()
    assert issued_ref() is not None

    del contract
    gc.collect()
    assert issued_ref() is None


def test_deferred_plugin_rejects_empty_and_missing_tokens() -> None:
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    empty_tokens: tuple[Any, ...] = (
        None,
        "",
        b"",
        bytearray(),
        memoryview(b""),
        (),
        [],
        {},
        set(),
        frozenset(),
    )
    for token in empty_tokens:
        backend.pop_results["q"].append((b"item", token))
        with pytest.raises(QueueError, match="token"):
            contract.pop_with_ack("q")


def _assert_value_graph_is_redacted(
    value: object,
    marker: str,
    seen: set[int] | None = None,
) -> None:
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


def _assert_terminal_queue_error_is_redacted(error: QueueError, marker: str) -> None:
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
            for value in frame.f_locals.values():
                _assert_value_graph_is_redacted(value, marker)
        trace = trace.tb_next


@pytest.mark.parametrize("case", ["missing", "empty", "duplicate", "forged", "cross"])
def test_deferred_ack_contract_errors_drop_recursive_private_markers(
    case: str,
) -> None:
    marker = f"deferred-ack-private-{case}"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )

    with pytest.raises(QueueError) as exc_info:
        if case == "missing":
            backend.pop_results[marker].append((marker.encode(), None))
            contract.pop_with_ack(marker)
        elif case == "empty":
            backend.pop_results[marker].append((marker.encode(), memoryview(b"")))
            contract.pop_with_ack(marker)
        elif case == "duplicate":
            backend.pop_results[marker].extend(
                [(marker.encode(), marker), (marker.encode(), marker)]
            )
            contract.pop_with_ack(marker)
            contract.pop_with_ack(marker)
        elif case == "forged":
            contract.ack(marker, token=marker)
        else:
            backend.pop_results["issued-queue"].append((marker.encode(), marker))
            contract.pop_with_ack("issued-queue")
            contract.ack(marker, token=marker)

    _assert_terminal_queue_error_is_redacted(exc_info.value, marker)


def test_deferred_plugin_rejects_forged_and_cross_queue_tokens() -> None:
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    backend.pop_results["q-a"].append((b"item", "issued"))
    assert contract.pop_with_ack("q-a") == (b"item", "issued")

    with pytest.raises(QueueError, match="unknown"):
        contract.ack("q-a", token="forged")
    with pytest.raises(QueueError, match="unknown"):
        contract.ack("q-b", token="issued")
    assert backend.ack_calls == []

    contract.ack("q-a", token="issued")
    assert backend.ack_calls == [("q-a", "issued")]


def test_deferred_plugin_token_contract_survives_circuit_breaker() -> None:
    backend = _DeferredPlugin()
    backend.pop_results["q"].append((b"item", "issued"))
    published = wrap_queue_backend(backend, CircuitBreaker("plugin-test"))
    contract = _DeferredAckPluginQueueBackend(
        published,
        supports_concurrent_ack=True,
    )

    data, token = QueueStrategy._pop_backend_instance_with_ack(contract, "q")

    assert data == b"item"
    assert isinstance(token, _BoundQueueAckToken)
    assert token.backend is contract
    token.ack()
    assert backend.ack_calls == [("q", "issued")]


def test_deferred_plugin_overlap_is_scoped_per_queue() -> None:
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    backend.pop_results["q-a"].extend(
        [(b"first", "shared"), (b"second", "shared"), (b"third", "distinct")]
    )
    backend.pop_results["q-b"].append((b"other", "shared"))

    assert contract.pop_with_ack("q-a") == (b"first", "shared")
    with pytest.raises(QueueError, match="reused"):
        contract.pop_with_ack("q-a")
    assert contract.pop_with_ack("q-a") == (b"third", "distinct")
    assert contract.pop_with_ack("q-b") == (b"other", "shared")

    contract.nack("q-a", token="shared")
    contract.ack("q-a", token="distinct")
    contract.ack("q-b", token="shared")
    assert backend.nack_calls == [("q-a", "shared")]
    assert backend.ack_calls == [("q-a", "distinct"), ("q-b", "shared")]
