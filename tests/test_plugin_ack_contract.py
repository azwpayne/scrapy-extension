"""Conformance tests for third-party deferred acknowledgement plugins."""

from __future__ import annotations

import gc
import sys
import threading
import traceback
import weakref
from collections import defaultdict, deque
from types import ModuleType
from typing import Any

import pytest
from scrapy.settings import Settings as ScrapySettings

from scrapy_extension.backends.base import Backend, QueueBackend
from scrapy_extension.backends.circuit_breaker import CircuitBreaker, wrap_queue_backend
from scrapy_extension.backends.connectors import (
    ConnectionManager,
    _DeferredAckPluginQueueBackend,
)
from scrapy_extension.backends.registry import BackendDescriptor, _reset_registry_cache
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.queue.strategies.base import QueueStrategy, _BoundQueueAckToken
from scrapy_extension.schedule.scheduler import BackendScheduler


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


class _CustomAwaitable:
    def __init__(self, broker_events: list[str] | None = None) -> None:
        self._broker_events = broker_events

    def __await__(self) -> Any:
        if self._broker_events is not None:
            self._broker_events.append("custom-awaitable-advanced")
        yield


class _IdentityToken:
    pass


_QUEUE_FACTORY_CALLS: list[str] = []


def _queue_backend_factory(settings: _Settings | None = None) -> _QueuePlugin:
    del settings
    _QUEUE_FACTORY_CALLS.append("factory-called")
    return _QueuePlugin()


class _CallableQueueBackendInstance(_QueuePlugin):
    def __call__(self, settings: _Settings | None = None) -> _QueuePlugin:
        del settings
        _QUEUE_FACTORY_CALLS.append("instance-called")
        return _QueuePlugin()


_CALLABLE_QUEUE_BACKEND_INSTANCE = _CallableQueueBackendInstance()


class _ManagerLockProbeToken:
    """Identity token whose destructor probes and optionally re-enters a manager."""

    def __init__(
        self,
        manager: ConnectionManager,
        events: list[str],
        reenter: Any | None = None,
    ) -> None:
        self.manager = manager
        self.events = events
        self.reenter = reenter

    def __del__(self) -> None:
        acquired = self.manager._lock.acquire(blocking=False)
        self.events.append("lock-free" if acquired else "lock-held")
        if not acquired:
            return
        self.manager._lock.release()
        if self.reenter is not None:
            self.reenter(self.manager, self.events)


async def _tracked_delivery_coroutine(broker_events: list[str]) -> None:
    broker_events.append("coroutine-advanced")


async def _tracked_delivery_async_generator(broker_events: list[str]) -> Any:
    broker_events.append("async-generator-advanced")
    yield None


def _tracked_delivery_generator(broker_events: list[str]) -> Any:
    broker_events.append("generator-advanced")
    yield None


class _TrackedIterator:
    def __init__(self, broker_events: list[str]) -> None:
        self._broker_events = broker_events

    def __iter__(self) -> _TrackedIterator:
        return self

    def __next__(self) -> object:
        self._broker_events.append("iterator-advanced")
        raise StopIteration


def _wrapped_lazy_result(
    result_kind: str,
    broker_events: list[str],
) -> object:
    if result_kind == "coroutine":
        return _tracked_delivery_coroutine(broker_events)
    if result_kind == "awaitable":
        return _CustomAwaitable(broker_events)
    if result_kind == "async-generator":
        return _tracked_delivery_async_generator(broker_events)
    if result_kind == "generator":
        return _tracked_delivery_generator(broker_events)
    return _TrackedIterator(broker_events)


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


def _pause_after_next_generation_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    manager: ConnectionManager,
) -> tuple[threading.Barrier, threading.Barrier]:
    """Pause one accessor after its coherent snapshot but before publication."""
    snapshot_taken = threading.Barrier(2)
    resume_publication = threading.Barrier(2)
    original_snapshot = manager._get_backend_breaker_snapshot
    pause_next = True

    def paused_snapshot() -> tuple[Backend, CircuitBreaker | None]:
        nonlocal pause_next
        snapshot = original_snapshot()
        if pause_next:
            pause_next = False
            snapshot_taken.wait(timeout=5)
            resume_publication.wait(timeout=5)
        return snapshot

    monkeypatch.setattr(manager, "_get_backend_breaker_snapshot", paused_snapshot)
    return snapshot_taken, resume_publication


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


@pytest.mark.parametrize(
    "backend_object_name",
    ["_queue_backend_factory", "_CALLABLE_QUEUE_BACKEND_INSTANCE"],
)
@pytest.mark.parametrize("requires_ack", [False, True])
def test_queue_descriptor_rejects_callable_non_classes_before_invocation(
    monkeypatch: pytest.MonkeyPatch,
    backend_object_name: str,
    requires_ack: bool,
) -> None:
    backend_object = globals()[backend_object_name]
    monkeypatch.setattr(backend_object, "requires_ack", requires_ack, raising=False)
    _QUEUE_FACTORY_CALLS.clear()
    _install_descriptor(
        monkeypatch,
        BackendDescriptor(
            backend_type="ackplugin",
            backend_cls_path=f"{__name__}.{backend_object_name}",
            settings_cls_path=f"{__name__}._Settings",
            capabilities=frozenset({"queue"}),
        ),
    )

    with pytest.raises(ConfigurationError) as exc_info:
        ConnectionManager("ackplugin")

    error = exc_info.value
    assert str(error) == (
        "Selected third-party queue backend has an invalid acknowledgement contract."
    )
    assert error.setting_name == "SCRAPY_BACKEND_TYPE"
    assert error.__cause__ is None
    assert error.__context__ is None
    assert _QUEUE_FACTORY_CALLS == []


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


def test_manager_pins_mutated_plugin_flags_for_gate_and_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _SingleConcurrencyDeferredPlugin)
    manager = ConnectionManager("ackplugin")
    assert manager._static_ack_capabilities() == (True, False)

    monkeypatch.setattr(_SingleConcurrencyDeferredPlugin, "requires_ack", False)
    monkeypatch.setattr(
        _SingleConcurrencyDeferredPlugin,
        "supports_concurrent_ack",
        True,
    )

    settings = ScrapySettings({"CONCURRENT_REQUESTS": 2})
    with pytest.raises(ConfigurationError, match="single-slot ack"):
        BackendScheduler._enforce_ack_concurrency_gate(settings, manager)

    backend = manager._create_backend()
    manager._backend = backend
    manager._breaker_configured = True
    adapter = manager.get_queue_backend()
    assert isinstance(adapter, _DeferredAckPluginQueueBackend)
    assert adapter.requires_ack is True
    assert adapter.supports_concurrent_ack is False

    later_manager = ConnectionManager("ackplugin")
    assert later_manager._static_ack_capabilities() == (False, True)
    later_backend = later_manager._create_backend()
    later_manager._backend = later_backend
    later_manager._breaker_configured = True
    assert later_manager.get_queue_backend() is later_backend


def test_later_manager_revalidates_mutated_flags_with_static_redaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "private-mutated-ack-capability"
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin")

    monkeypatch.setattr(_DeferredPlugin, "requires_ack", marker)
    monkeypatch.setattr(_DeferredPlugin, "supports_concurrent_ack", marker)

    assert manager._static_ack_capabilities() == (True, True)
    with pytest.raises(ConfigurationError) as exc_info:
        ConnectionManager("ackplugin")

    error = exc_info.value
    assert str(error) == (
        "Selected third-party queue backend has an invalid acknowledgement contract."
    )
    assert error.setting_name == "SCRAPY_BACKEND_TYPE"
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in "".join(traceback.format_exception(error))
    trace = error.__traceback__
    while trace is not None:
        if "/src/scrapy_extension/" in trace.tb_frame.f_code.co_filename:
            for value in trace.tb_frame.f_locals.values():
                _assert_value_graph_is_redacted(value, marker)
        trace = trace.tb_next


@pytest.mark.filterwarnings("error")
@pytest.mark.parametrize("method_name", ["pop_with_ack", "ack", "nack"])
@pytest.mark.parametrize("function_kind", ["coroutine", "async-generator", "generator"])
def test_manager_statically_rejects_lazy_delivery_hooks_without_io(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    function_kind: str,
) -> None:
    constructed = False
    broker_events: list[str] = []

    if function_kind == "coroutine":

        async def lazy_hook(
            self: object,
            *args: object,
            **kwargs: object,
        ) -> None:
            del self, args, kwargs
            broker_events.append("coroutine-advanced")

    elif function_kind == "async-generator":

        async def lazy_hook(  # type: ignore[misc]
            self: object,
            *args: object,
            **kwargs: object,
        ) -> Any:
            del self, args, kwargs
            broker_events.append("async-generator-advanced")
            yield None

    else:

        def lazy_hook(  # type: ignore[misc]
            self: object,
            *args: object,
            **kwargs: object,
        ) -> Any:
            del self, args, kwargs
            broker_events.append("generator-advanced")
            yield None

    def reject_construction(self: object, settings: object | None = None) -> None:
        del self, settings
        nonlocal constructed
        constructed = True
        raise AssertionError("plugin must not be constructed")

    monkeypatch.setattr(_DeferredPlugin, method_name, lazy_hook)
    monkeypatch.setattr(_DeferredPlugin, "__init__", reject_construction)
    _install_plugin(monkeypatch, _DeferredPlugin)

    with pytest.raises(ConfigurationError) as exc_info:
        ConnectionManager("ackplugin")

    gc.collect()
    assert str(exc_info.value) == (
        "Selected third-party queue backend has an invalid acknowledgement contract."
    )
    assert exc_info.value.setting_name == "SCRAPY_BACKEND_TYPE"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert constructed is False
    assert broker_events == []


@pytest.mark.parametrize(
    ("method_name", "signature_case"),
    [
        ("pop_with_ack", "missing_argument"),
        ("pop_with_ack", "required_extra"),
        ("ack", "missing_argument"),
        ("ack", "positional_only_token"),
        ("ack", "required_extra"),
        ("nack", "missing_argument"),
        ("nack", "positional_only_token"),
        ("nack", "required_extra"),
    ],
)
def test_manager_statically_rejects_signature_incompatible_overrides(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    signature_case: str,
) -> None:
    if signature_case == "missing_argument":

        def incompatible(self: object, queue_name: str) -> None:
            raise AssertionError("incompatible hook must not be invoked")

    elif signature_case == "positional_only_token":

        def incompatible(  # type: ignore[misc]
            self: object,
            queue_name: str,
            token: object,
            /,
        ) -> None:
            raise AssertionError("incompatible hook must not be invoked")

    else:

        def incompatible(  # type: ignore[misc]
            self: object,
            queue_name: str,
            timeout_or_token: object,
            required: object,
        ) -> None:
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


@pytest.mark.filterwarnings("error")
@pytest.mark.parametrize("method_name", ["pop_with_ack", "ack", "nack"])
@pytest.mark.parametrize(
    "result_kind",
    ["coroutine", "awaitable", "async-generator", "generator", "iterator"],
)
def test_wrapped_lazy_results_fail_without_advancing_or_broker_settlement(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    result_kind: str,
) -> None:
    marker = f"async-result-private-{method_name}-{result_kind}"
    broker_events: list[str] = []
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    original_hook = getattr(backend, method_name)

    if method_name != "pop_with_ack":
        backend.pop_results["q"].append((b"item", marker))
        contract.pop_with_ack("q")

    def wrapped_hook(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return _wrapped_lazy_result(result_kind, broker_events)

    monkeypatch.setattr(backend, method_name, wrapped_hook)
    with pytest.raises(QueueError) as exc_info:
        if method_name == "pop_with_ack":
            contract.pop_with_ack("q")
        else:
            getattr(contract, method_name)("q", token=marker)

    gc.collect()
    _assert_terminal_queue_error_is_redacted(exc_info.value, marker)
    assert broker_events == []
    assert backend.ack_calls == []
    assert backend.nack_calls == []

    monkeypatch.setattr(backend, method_name, original_hook)
    if method_name == "pop_with_ack":
        assert contract._active_ack_tokens == {}
        backend.pop_results["q"].append((b"item", marker))
        assert contract.pop_with_ack("q") == (b"item", marker)
        contract.ack("q", token=marker)
        assert backend.ack_calls == [("q", marker)]
    else:
        # The failed lazy settlement must leave ownership with the adapter so
        # the same delivery can be synchronously retried.
        getattr(contract, method_name)("q", token=marker)
        expected = [("q", marker)]
        assert backend.ack_calls == (expected if method_name == "ack" else [])
        assert backend.nack_calls == (expected if method_name == "nack" else [])
        assert contract._active_ack_tokens == {}


@pytest.mark.parametrize("method_name", ["ack", "nack"])
def test_non_none_settlement_result_is_terminal_redacted_and_retryable(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    marker = f"private-non-none-{method_name}"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    backend.pop_results["q"].append((b"item", marker))
    contract.pop_with_ack("q")
    original_hook = getattr(backend, method_name)
    invalid_calls = 0

    def non_none_hook(*args: object, **kwargs: object) -> object:
        del args, kwargs
        nonlocal invalid_calls
        invalid_calls += 1
        return marker

    monkeypatch.setattr(backend, method_name, non_none_hook)
    with pytest.raises(QueueError, match="non-None") as exc_info:
        getattr(contract, method_name)("q", token=marker)

    _assert_terminal_queue_error_is_redacted(exc_info.value, marker)
    assert invalid_calls == 1
    assert contract._active_ack_tokens

    monkeypatch.setattr(backend, method_name, original_hook)
    getattr(contract, method_name)("q", token=marker)
    expected = [("q", marker)]
    assert backend.ack_calls == (expected if method_name == "ack" else [])
    assert backend.nack_calls == (expected if method_name == "nack" else [])
    assert contract._active_ack_tokens == {}
    assert contract._settling_ack_tokens == set()


@pytest.mark.parametrize("method_name", ["ack", "nack"])
def test_settlement_descriptor_and_hook_run_outside_contract_lock_and_reenter(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    marker = f"private-reentrant-{method_name}"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    backend.pop_results["q"].append((b"item", marker))
    contract.pop_with_ack("q")
    opposite_name = "nack" if method_name == "ack" else "ack"
    original_hook = getattr(_DeferredPlugin, method_name)
    lock_checks: list[bool] = []
    reentrant_errors: list[QueueError] = []

    class LockCheckingDescriptor:
        def __get__(self, instance: object, owner: type[object]) -> object:
            del owner
            acquired = contract._ack_contract_lock.acquire(blocking=False)
            lock_checks.append(acquired)
            if acquired:
                contract._ack_contract_lock.release()

            def settle(queue_name: str, *, token: object | None = None) -> None:
                acquired = contract._ack_contract_lock.acquire(blocking=False)
                lock_checks.append(acquired)
                if acquired:
                    contract._ack_contract_lock.release()
                try:
                    getattr(contract, opposite_name)(queue_name, token=token)
                except QueueError as error:
                    reentrant_errors.append(error)
                original_hook(instance, queue_name, token=token)

            return settle

    monkeypatch.setattr(type(backend), method_name, LockCheckingDescriptor())

    getattr(contract, method_name)("q", token=marker)

    assert lock_checks == [True, True]
    assert len(reentrant_errors) == 1
    _assert_terminal_queue_error_is_redacted(reentrant_errors[0], marker)
    expected = [("q", marker)]
    assert backend.ack_calls == (expected if method_name == "ack" else [])
    assert backend.nack_calls == (expected if method_name == "nack" else [])
    assert contract._active_ack_tokens == {}
    assert contract._settling_ack_tokens == set()


def test_delivery_rejects_reentrant_hash_queue_subclass() -> None:
    marker = "private-reentrant-delivery-queue"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    hook_calls: list[str] = []

    class ReentrantQueueName(str):
        def __hash__(self) -> int:
            hook_calls.append("hash")
            contract.pop_with_ack("reentrant-safe-queue")
            return super().__hash__()

        def __str__(self) -> str:
            hook_calls.append("str")
            return super().__str__()

    errors: list[BaseException] = []

    def deliver() -> None:
        try:
            contract.pop_with_ack(ReentrantQueueName(marker))
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=deliver, daemon=True)
    worker.start()
    worker.join(timeout=1.0)

    assert not worker.is_alive(), "hostile queue hash deadlocked delivery"
    assert len(errors) == 1
    assert isinstance(errors[0], QueueError)
    assert "exact built-in string" in str(errors[0])
    _assert_terminal_queue_error_is_redacted(errors[0], marker)
    assert hook_calls == []
    assert backend.pop_results == {}
    assert contract._active_ack_tokens == {}


@pytest.mark.parametrize("method_name", ["ack", "nack"])
def test_settlement_rejects_reentrant_hash_queue_subclass_and_keeps_token(
    method_name: str,
) -> None:
    marker = f"private-reentrant-{method_name}-queue"
    token = f"issued-{method_name}-token"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    backend.pop_results["q"].append((b"item", token))
    contract.pop_with_ack("q")
    hook_calls: list[str] = []

    class ReentrantQueueName(str):
        def __hash__(self) -> int:
            hook_calls.append("hash")
            getattr(contract, method_name)("q", token=token)
            return super().__hash__()

        def __str__(self) -> str:
            hook_calls.append("str")
            return super().__str__()

    errors: list[BaseException] = []

    def settle() -> None:
        try:
            getattr(contract, method_name)(ReentrantQueueName(marker), token=token)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=settle, daemon=True)
    worker.start()
    worker.join(timeout=1.0)

    assert not worker.is_alive(), "hostile queue hash deadlocked settlement"
    assert len(errors) == 1
    assert isinstance(errors[0], QueueError)
    assert "exact built-in string" in str(errors[0])
    _assert_terminal_queue_error_is_redacted(errors[0], marker)
    assert hook_calls == []
    assert backend.ack_calls == []
    assert backend.nack_calls == []
    assert contract._active_ack_tokens
    assert contract._settling_ack_tokens == set()

    getattr(contract, method_name)("q", token=token)
    expected = [("q", token)]
    assert backend.ack_calls == (expected if method_name == "ack" else [])
    assert backend.nack_calls == (expected if method_name == "nack" else [])
    assert contract._active_ack_tokens == {}


@pytest.mark.parametrize("method_name", ["ack", "nack"])
def test_blocked_settlement_fences_racer_then_failure_restores_exact_retry(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    marker = f"private-race-{method_name}"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    backend.pop_results["q"].append((b"item", marker))
    contract.pop_with_ack("q")
    original_hook = getattr(backend, method_name)
    entered = threading.Event()
    release = threading.Event()
    broker_calls = 0
    worker_errors: list[BaseException] = []

    def fail_once_then_settle(queue_name: str, *, token: object | None = None) -> None:
        nonlocal broker_calls
        broker_calls += 1
        if broker_calls == 1:
            entered.set()
            assert release.wait(timeout=2.0)
            raise QueueError(marker)
        original_hook(queue_name, token=token)

    monkeypatch.setattr(backend, method_name, fail_once_then_settle)

    def settle_in_worker() -> None:
        try:
            getattr(contract, method_name)("q", token=marker)
        except BaseException as error:
            worker_errors.append(error)

    worker = threading.Thread(target=settle_in_worker, daemon=True)
    worker.start()
    assert entered.wait(timeout=1.0)

    opposite_name = "nack" if method_name == "ack" else "ack"
    with pytest.raises(QueueError) as exc_info:
        getattr(contract, opposite_name)("q", token=marker)
    _assert_terminal_queue_error_is_redacted(exc_info.value, marker)
    assert broker_calls == 1
    assert backend.ack_calls == []
    assert backend.nack_calls == []

    release.set()
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert len(worker_errors) == 1
    assert isinstance(worker_errors[0], QueueError)
    _assert_terminal_queue_error_is_redacted(worker_errors[0], marker)
    assert contract._active_ack_tokens
    assert contract._settling_ack_tokens == set()

    getattr(contract, method_name)("q", token=marker)
    assert broker_calls == 2
    expected = [("q", marker)]
    assert backend.ack_calls == (expected if method_name == "ack" else [])
    assert backend.nack_calls == (expected if method_name == "nack" else [])
    assert contract._active_ack_tokens == {}
    assert contract._settling_ack_tokens == set()


@pytest.mark.parametrize(
    "result_kind",
    ["list", "tuple-subclass", "short-tuple", "long-tuple"],
)
def test_pop_with_ack_requires_an_exact_two_tuple(
    monkeypatch: pytest.MonkeyPatch,
    result_kind: str,
) -> None:
    marker = f"private-delivery-shape-{result_kind}"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )

    class TupleSubclass(tuple[object, ...]):
        pass

    results: dict[str, object] = {
        "list": [b"item", marker],
        "tuple-subclass": TupleSubclass((b"item", marker)),
        "short-tuple": (marker,),
        "long-tuple": (b"item", marker, None),
    }
    monkeypatch.setattr(backend, "pop_with_ack", lambda *args: results[result_kind])

    with pytest.raises(QueueError, match="invalid delivery") as exc_info:
        contract.pop_with_ack("q")

    _assert_terminal_queue_error_is_redacted(exc_info.value, marker)
    assert contract._active_ack_tokens == {}


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


def _cache_manager_lock_probe_token(
    manager: ConnectionManager,
    backend: _DeferredPlugin,
    events: list[str],
    reenter: Any | None = None,
) -> tuple[
    weakref.ReferenceType[_DeferredAckPluginQueueBackend],
    weakref.ReferenceType[_ManagerLockProbeToken],
]:
    adapter = manager.get_queue_backend()
    assert isinstance(adapter, _DeferredAckPluginQueueBackend)
    token = _ManagerLockProbeToken(manager, events, reenter)
    adapter_ref = weakref.ref(adapter)
    token_ref = weakref.ref(token)
    backend.pop_results["q"].append((b"item", token))
    _item, returned = adapter.pop_with_ack("q")
    assert returned is token
    del returned
    del token
    del adapter
    gc.collect()
    assert adapter_ref() is manager._plugin_queue_backend
    assert token_ref() is not None
    return adapter_ref, token_ref


def _reenter_retired_manager(
    manager: ConnectionManager,
    events: list[str],
) -> None:
    try:
        manager.get_queue_backend()
    except BackendConnectionError:
        events.append("retired")


def _reenter_current_queue_cache(
    manager: ConnectionManager,
    events: list[str],
) -> None:
    events.append(
        "current-cache"
        if manager.get_queue_backend() is manager._plugin_queue_backend
        else "wrong-cache"
    )


def test_final_close_releases_manager_adapter_without_erasing_active_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin")
    backend = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    manager._backend = backend
    manager._breaker_configured = True
    adapter = manager.get_queue_backend()
    assert isinstance(adapter, _DeferredAckPluginQueueBackend)
    adapter_ref = weakref.ref(adapter)
    backend_ref = weakref.ref(backend)
    settings_ref = weakref.ref(backend.settings)
    token = _IdentityToken()
    backend.pop_results["q"].append((b"item", token))
    adapter.pop_with_ack("q")

    manager.close()

    assert manager._plugin_queue_backend is None
    assert manager._plugin_queue_backend_source is None
    assert adapter._active_ack_tokens
    adapter.ack("q", token=token)
    backend.ack_calls.clear()
    del token
    del adapter
    del backend
    gc.collect()
    assert adapter_ref() is None
    assert backend_ref() is None
    assert settings_ref() is None


@pytest.mark.parametrize("teardown", ["close", "clear_registry"])
def test_retirement_releases_identity_token_with_manager_lock_free(
    monkeypatch: pytest.MonkeyPatch,
    teardown: str,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = (
        ConnectionManager.get_manager("ackplugin")
        if teardown == "clear_registry"
        else ConnectionManager("ackplugin")
    )
    backend = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    manager._backend = backend
    manager._breaker_configured = True
    events: list[str] = []
    adapter_ref, token_ref = _cache_manager_lock_probe_token(
        manager,
        backend,
        events,
        _reenter_retired_manager,
    )

    if teardown == "close":
        manager.close()
    else:
        ConnectionManager.clear_registry()

    gc.collect()
    assert events == ["lock-free", "retired"]
    assert adapter_ref() is None
    assert token_ref() is None
    assert manager._plugin_queue_backend is None
    assert manager._plugin_queue_backend_source is None


def test_reconnect_releases_identity_token_with_manager_lock_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin", {"retry_attempts": 0})
    backend = manager._create_backend()
    replacement = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    backend.is_connected = lambda: False  # type: ignore[method-assign]
    manager._backend = backend
    manager._breaker_configured = True
    events: list[str] = []

    def reenter(manager: ConnectionManager, events: list[str]) -> None:
        events.append("disconnected" if not manager.is_connected() else "connected")

    adapter_ref, token_ref = _cache_manager_lock_probe_token(
        manager, backend, events, reenter
    )
    monkeypatch.setattr(manager, "_create_backend", lambda: replacement)

    manager._connect_with_retries([])

    gc.collect()
    assert events == ["lock-free", "disconnected"]
    assert adapter_ref() is None
    assert token_ref() is None
    assert manager._backend is replacement
    assert manager._plugin_queue_backend is None
    assert manager._plugin_queue_backend_source is None


def test_failed_reconnect_releases_identity_token_with_manager_lock_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin", {"retry_attempts": 0})
    backend = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    backend.is_connected = lambda: False  # type: ignore[method-assign]
    manager._backend = backend
    manager._breaker_configured = True
    events: list[str] = []
    adapter_ref, token_ref = _cache_manager_lock_probe_token(manager, backend, events)
    monkeypatch.setattr(
        manager,
        "_create_backend",
        lambda: (_ for _ in ()).throw(RuntimeError("replacement failed")),
    )

    with pytest.raises(BackendConnectionError, match="Failed to connect"):
        manager._connect_with_retries([])

    gc.collect()
    assert events == ["lock-free"]
    assert adapter_ref() is None
    assert token_ref() is None
    assert manager._plugin_queue_backend is None
    assert manager._plugin_queue_backend_source is None


def test_cache_replacement_releases_identity_token_with_manager_lock_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin")
    backend = manager._create_backend()
    replacement = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    manager._backend = backend
    manager._breaker_configured = True
    events: list[str] = []
    adapter_ref, token_ref = _cache_manager_lock_probe_token(
        manager,
        backend,
        events,
        _reenter_current_queue_cache,
    )
    with manager._lock:
        manager._backend = replacement

    replacement_adapter = manager.get_queue_backend()

    gc.collect()
    assert events == ["lock-free", "current-cache"]
    assert adapter_ref() is None
    assert token_ref() is None
    assert replacement_adapter is manager._plugin_queue_backend
    assert manager._plugin_queue_backend_source == (replacement, None)


def test_generation_race_releases_identity_token_with_manager_lock_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin")
    backend = manager._create_backend()
    replacement = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    manager._backend = backend
    manager._breaker_configured = True
    events: list[str] = []
    adapter_ref, token_ref = _cache_manager_lock_probe_token(
        manager,
        backend,
        events,
        _reenter_current_queue_cache,
    )
    snapshot_taken, resume_publication = _pause_after_next_generation_snapshot(
        monkeypatch, manager
    )
    adapters: list[QueueBackend] = []
    accessor = threading.Thread(
        target=lambda: adapters.append(manager.get_queue_backend())
    )
    accessor.start()
    snapshot_taken.wait(timeout=5)

    with manager._lock:
        manager._backend = replacement
        retired_adapter, retired_source = (
            manager._detach_plugin_queue_backend_under_lock()
        )
    del retired_adapter, retired_source

    assert events == ["lock-free", "current-cache"]
    assert adapter_ref() is None
    assert token_ref() is None
    resume_publication.wait(timeout=5)
    accessor.join(timeout=5)
    assert not accessor.is_alive()
    assert adapters == [manager._plugin_queue_backend]
    assert manager._plugin_queue_backend_source == (replacement, None)


def test_forced_teardown_releases_manager_adapter_and_backend_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager.get_manager("ackplugin")
    backend = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    manager._backend = backend
    manager._breaker_configured = True
    adapter = manager.get_queue_backend()
    adapter_ref = weakref.ref(adapter)
    backend_ref = weakref.ref(backend)
    settings_ref = weakref.ref(backend.settings)

    ConnectionManager.clear_registry()

    assert manager._plugin_queue_backend is None
    assert manager._plugin_queue_backend_source is None
    del adapter
    del backend
    gc.collect()
    assert adapter_ref() is None
    assert backend_ref() is None
    assert settings_ref() is None


@pytest.mark.parametrize("teardown", ["close", "clear_registry"])
def test_teardown_race_cannot_publish_retired_plugin_adapter(
    monkeypatch: pytest.MonkeyPatch,
    teardown: str,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager.get_manager("ackplugin")
    backend = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    manager._backend = backend
    manager._breaker_configured = True
    backend_ref = weakref.ref(backend)
    settings_ref = weakref.ref(backend.settings)
    snapshot_taken, resume_publication = _pause_after_next_generation_snapshot(
        monkeypatch,
        manager,
    )
    outcomes: list[tuple[str, str]] = []

    def access_queue_backend() -> None:
        try:
            manager.get_queue_backend()
        except BaseException as error:
            outcomes.append((type(error).__name__, str(error)))
        else:
            outcomes.append(("returned", ""))

    accessor = threading.Thread(target=access_queue_backend)
    accessor.start()
    snapshot_taken.wait(timeout=5)

    if teardown == "close":
        manager.close()
    else:
        ConnectionManager.clear_registry()

    assert manager._retired is True
    assert manager._plugin_queue_backend is None
    assert manager._plugin_queue_backend_source is None
    resume_publication.wait(timeout=5)
    accessor.join(timeout=5)

    assert not accessor.is_alive()
    assert outcomes == [
        ("BackendConnectionError", "Cannot access a released ConnectionManager")
    ]
    assert manager._plugin_queue_backend is None
    assert manager._plugin_queue_backend_source is None
    del backend
    del accessor
    gc.collect()
    assert backend_ref() is None
    assert settings_ref() is None


def test_backend_replacement_race_publishes_only_replacement_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin")
    retired_backend = manager._create_backend()
    assert isinstance(retired_backend, _DeferredPlugin)
    manager._backend = retired_backend
    manager._breaker_configured = True
    retired_ref = weakref.ref(retired_backend)
    settings_ref = weakref.ref(retired_backend.settings)
    snapshot_taken, resume_publication = _pause_after_next_generation_snapshot(
        monkeypatch,
        manager,
    )
    adapters: list[QueueBackend] = []

    accessor = threading.Thread(
        target=lambda: adapters.append(manager.get_queue_backend())
    )
    accessor.start()
    snapshot_taken.wait(timeout=5)
    replacement = manager._create_backend()
    with manager._lock:
        manager._backend = replacement
        retired_adapter, retired_source = (
            manager._detach_plugin_queue_backend_under_lock()
        )
    del retired_adapter, retired_source
    resume_publication.wait(timeout=5)
    accessor.join(timeout=5)

    assert not accessor.is_alive()
    assert len(adapters) == 1
    adapter = adapters[0]
    assert isinstance(adapter, _DeferredAckPluginQueueBackend)
    assert adapter._delegate is replacement
    assert manager._plugin_queue_backend is adapter
    assert manager._plugin_queue_backend_source == (replacement, None)
    del retired_backend
    del accessor
    gc.collect()
    assert retired_ref() is None
    assert settings_ref() is None


def test_breaker_replacement_race_publishes_only_replacement_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin")
    backend = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    retired_breaker = CircuitBreaker("retired-plugin-generation")
    manager._backend = backend
    manager._breaker = retired_breaker
    manager._breaker_configured = True
    retired_ref = weakref.ref(retired_breaker)
    snapshot_taken, resume_publication = _pause_after_next_generation_snapshot(
        monkeypatch,
        manager,
    )
    adapters: list[QueueBackend] = []

    accessor = threading.Thread(
        target=lambda: adapters.append(manager.get_queue_backend())
    )
    accessor.start()
    snapshot_taken.wait(timeout=5)
    replacement = retired_breaker.new_generation()
    with manager._lock:
        manager._breaker = replacement
        retired_adapter, retired_source = (
            manager._detach_plugin_queue_backend_under_lock()
        )
    del retired_adapter, retired_source
    resume_publication.wait(timeout=5)
    accessor.join(timeout=5)

    assert not accessor.is_alive()
    assert len(adapters) == 1
    adapter = adapters[0]
    assert isinstance(adapter, _DeferredAckPluginQueueBackend)
    assert adapter._delegate._breaker is replacement  # type: ignore[attr-defined]
    assert manager._plugin_queue_backend is adapter
    assert manager._plugin_queue_backend_source == (backend, replacement)
    del retired_breaker
    del accessor
    gc.collect()
    assert retired_ref() is None


def test_failed_generation_replacement_releases_retired_plugin_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_plugin(monkeypatch, _DeferredPlugin)
    manager = ConnectionManager("ackplugin", {"retry_attempts": 0})
    backend = manager._create_backend()
    assert isinstance(backend, _DeferredPlugin)
    backend.is_connected = lambda: False  # type: ignore[method-assign]
    manager._backend = backend
    manager._breaker_configured = True
    adapter = manager.get_queue_backend()
    adapter_ref = weakref.ref(adapter)
    backend_ref = weakref.ref(backend)
    settings_ref = weakref.ref(backend.settings)

    def fail_create() -> Backend:
        raise RuntimeError("replacement failed")

    monkeypatch.setattr(manager, "_create_backend", fail_create)
    with pytest.raises(BackendConnectionError, match="Failed to connect"):
        manager._connect_with_retries([])

    assert manager._plugin_queue_backend is None
    assert manager._plugin_queue_backend_source is None
    del adapter
    del backend
    gc.collect()
    assert adapter_ref() is None
    assert backend_ref() is None
    assert settings_ref() is None


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


def test_deferred_plugin_rejects_released_memoryview_delivery_token() -> None:
    marker = "private-released-delivery-token"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    payload = bytearray(marker.encode())
    token = memoryview(payload)
    token.release()
    backend.pop_results["q"].append((b"item", token))

    with pytest.raises(QueueError, match="unusable") as exc_info:
        contract.pop_with_ack("q")

    _assert_terminal_queue_error_is_redacted(exc_info.value, marker)
    assert contract._active_ack_tokens == {}
    assert contract._settling_ack_tokens == set()
    assert backend.ack_calls == []
    assert backend.nack_calls == []


@pytest.mark.parametrize("method_name", ["ack", "nack"])
def test_deferred_plugin_rejects_released_memoryview_settlement_token_safely(
    method_name: str,
) -> None:
    marker = f"private-released-{method_name}-token"
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    payload = bytearray(marker.encode())
    token = memoryview(payload)
    backend.pop_results["q"].append((b"item", token))
    assert contract.pop_with_ack("q") == (b"item", token)
    token.release()

    with pytest.raises(QueueError, match="unusable") as exc_info:
        getattr(contract, method_name)("q", token=token)

    _assert_terminal_queue_error_is_redacted(exc_info.value, marker)
    assert backend.ack_calls == []
    assert backend.nack_calls == []
    assert contract._settling_ack_tokens == set()
    active = contract._active_ack_tokens["q"]
    assert list(active.values()) == [token]


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
