"""Focused manager and plugin-boundary contracts left outside the broad suites."""

from __future__ import annotations

import sys
from datetime import date, datetime
from datetime import time as datetime_time
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock
from uuid import UUID

import pytest
from pydantic import SecretBytes, SecretStr

import scrapy_extension.backends.connectors._manager as manager_module
import scrapy_extension.backends.connectors._plugin_contract as plugin_module
from scrapy_extension.backends.base import Backend, QueueBackend, SetBackend
from scrapy_extension.backends.connectors import (
    ConnectionManager,
    _DeferredAckPluginQueueBackend,
)
from scrapy_extension.backends.registry import BackendDescriptor
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)


class _Backend(Backend):
    def __init__(self, _settings: object | None = None) -> None:
        self.connected = False
        self.connect_calls = 0
        self.disconnect_calls = 0

    @property
    def backend_type(self) -> str:
        return "redis"

    def connect(self) -> None:
        self.connect_calls += 1
        self.connected = True

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    def ping(self) -> bool:
        return self.connected


class _QueueBackend(_Backend, QueueBackend):
    def __init__(self, _settings: object | None = None) -> None:
        super().__init__(_settings)
        self.pop_result: tuple[bytes | None, object | None] = (None, None)
        self.ack_calls: list[tuple[str, object]] = []
        self.nack_calls: list[tuple[str, object]] = []

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

    def pop_with_ack(
        self, queue_name: str, timeout: float = 0.0
    ) -> tuple[bytes | None, object | None]:
        del queue_name, timeout
        return self.pop_result

    def ack(self, queue_name: str, *, token: object | None = None) -> None:
        assert token is not None
        self.ack_calls.append((queue_name, token))

    def nack(self, queue_name: str, *, token: object | None = None) -> None:
        assert token is not None
        self.nack_calls.append((queue_name, token))


class _SetOnlyBackend(_Backend, SetBackend):
    def add(self, set_name: str, item: bytes) -> bool:
        del set_name, item
        return True

    def remove(self, set_name: str, item: bytes) -> bool:
        del set_name, item
        return True

    def contains(self, set_name: str, item: bytes) -> bool:
        del set_name, item
        return False

    def set_len(self, set_name: str) -> int:
        del set_name
        return 0

    def clear_set(self, set_name: str) -> None:
        del set_name


class _Settings:
    model_fields = {"host": object()}

    def __init__(self, **_values: object) -> None:
        self.values = _values


@pytest.fixture(autouse=True)
def _clear_manager_registry() -> None:
    ConnectionManager.clear_registry()
    yield
    ConnectionManager.clear_registry()


def _manager_with_backend(
    backend: Backend,
    *,
    retry_attempts: int = 0,
    retry_delay: float = 0.0,
) -> ConnectionManager:
    manager = ConnectionManager(
        "redis",
        {"retry_attempts": retry_attempts, "retry_delay": retry_delay},
    )
    manager._create_backend = lambda: backend  # type: ignore[method-assign]
    return manager


def test_connect_retry_stops_when_close_wins_and_redacts_driver_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager: ConnectionManager
    backend = _Backend()

    def close_wins() -> None:
        manager._retired = True
        manager._retirement_event.set()
        raise RuntimeError("driver-secret")

    backend.connect = close_wins  # type: ignore[method-assign]
    manager = _manager_with_backend(backend, retry_attempts=3)
    monkeypatch.setattr(
        manager_module, "_wait_for_retry_backoff", Mock(return_value=False)
    )

    with pytest.raises(BackendConnectionError) as exc_info:
        manager.connect()
    assert str(exc_info.value) == "Connection manager was released while connecting"
    assert exc_info.value.__context__ is None
    assert backend.disconnect_calls == 1


def test_retry_deadline_and_event_interruption_are_bounded_without_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failing = _Backend()
    failing.connect = Mock(side_effect=RuntimeError("temporary"))  # type: ignore[method-assign]
    manager = _manager_with_backend(failing, retry_attempts=4, retry_delay=10.0)
    monkeypatch.setattr(manager, "_reactor_io_timeout", lambda: 0.0)
    waits = Mock(return_value=False)
    monkeypatch.setattr(manager_module, "_wait_for_retry_backoff", waits)

    with pytest.raises(BackendConnectionError, match="after 5 attempts"):
        manager.connect()
    # A zero remaining budget ends the retry transaction before the next attempt.
    assert failing.connect_calls == 0
    assert failing.connect.call_count == 1  # type: ignore[attr-defined]
    assert waits.call_count == 0

    interrupted = _Backend()
    interrupted.connect = Mock(side_effect=RuntimeError("temporary"))  # type: ignore[method-assign]
    manager = _manager_with_backend(interrupted, retry_attempts=4, retry_delay=1.0)
    monkeypatch.setattr(manager, "_reactor_io_timeout", lambda: 5.0)
    monkeypatch.setattr(
        manager_module, "_wait_for_retry_backoff", Mock(return_value=True)
    )
    with pytest.raises(BackendConnectionError, match="after 5 attempts"):
        manager.connect()
    assert interrupted.connect.call_count == 1  # type: ignore[attr-defined]


def test_failed_candidate_disconnect_error_is_not_the_public_error() -> None:
    backend = _Backend()
    backend.connect = Mock(side_effect=RuntimeError("connect-secret"))  # type: ignore[method-assign]
    backend.disconnect = Mock(side_effect=RuntimeError("cleanup-secret"))  # type: ignore[method-assign]
    manager = _manager_with_backend(backend)

    with pytest.raises(BackendConnectionError) as exc_info:
        manager.connect()
    assert str(exc_info.value) == "Failed to connect after 1 attempt."
    assert "secret" not in str(exc_info.value)
    assert exc_info.value.__context__ is None
    assert backend.disconnect.call_count == 1  # type: ignore[attr-defined]


def test_attempt_connection_rejects_a_retired_manager_before_candidate_creation() -> (
    None
):
    manager = ConnectionManager("redis")
    manager._retired = True
    with pytest.raises(BackendConnectionError, match="released"):
        manager._attempt_connection()


def test_direct_close_preserves_active_exact_ownership() -> None:
    manager = ConnectionManager("redis")
    manager._backend = _Backend()
    active_token = object()
    manager._active_acquires.add(active_token)
    manager._users = 1

    manager.close()
    assert manager._retired is False
    assert manager._backend is not None
    assert manager._active_acquires == {active_token}


def test_interrupted_direct_retirement_is_reachable_for_a_later_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ConnectionManager("redis")
    backend = _Backend()
    manager._backend = backend
    calls = 0
    original = manager._finalize_retirement

    def interrupt_once() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyboardInterrupt("teardown interrupted")
        original()

    monkeypatch.setattr(manager, "_finalize_retirement", interrupt_once)
    with pytest.raises(KeyboardInterrupt):
        manager.close()
    assert manager in ConnectionManager._pending_release_managers
    manager.close()
    assert manager._retirement_complete is True
    assert backend.disconnect_calls == 1


def test_finalizer_complete_state_repairs_owner_fields_and_waiters() -> None:
    manager = ConnectionManager("redis")
    manager._retirement_complete = True
    manager._retirement_finalizing = True
    manager._retirement_finalizer_thread_id = 123
    manager._retirement_finalizer_token = object()  # type: ignore[assignment]
    manager._finalize_retirement()
    assert manager._retirement_complete is True
    assert manager._retirement_finalizing is False
    assert manager._retirement_finalizer_thread_id is None
    assert manager._retirement_finalizer_token is None
    assert manager._retirement_finalization_event.is_set()


def test_finalizer_waits_for_another_live_owner_without_releasing_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ConnectionManager("redis")
    owner = manager_module._RetirementFinalizerToken()
    owner.thread_id += 1
    manager._retirement_finalizing = True
    manager._retirement_finalizer_token = owner
    manager._retirement_finalization_event.set()
    monkeypatch.setattr(
        manager_module._RetirementFinalizerToken, "active", property(lambda _self: True)
    )

    manager._finalize_retirement()
    assert manager._retirement_complete is False
    assert manager._retirement_finalization_event.is_set()


def test_finalizer_publishes_state_before_preserving_disconnect_control_flow() -> None:
    manager = ConnectionManager("redis")
    backend = _Backend()
    backend.disconnect = Mock(side_effect=KeyboardInterrupt("disconnect-stop"))  # type: ignore[method-assign]
    manager._backend = backend
    events: list[tuple[str, object]] = []

    class Monitor:
        def on_disconnect(self, backend_type: str, reason: object) -> None:
            events.append((backend_type, reason))

        def on_disconnect_result(self, backend_type: str, succeeded: bool) -> None:
            events.append((backend_type, succeeded))

    manager.set_monitor(Monitor())  # type: ignore[arg-type]
    with pytest.raises(KeyboardInterrupt):
        manager._finalize_retirement()
    assert manager._retirement_complete is True
    assert events == [("redis", None), ("redis", False)]


def test_registry_cap_evicts_oldest_orphan_and_disconnects_outside_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ConnectionManager, "MAX_MANAGERS", 2)
    first = ConnectionManager.get_manager("redis", {"host": "cap-first"})
    second = ConnectionManager.get_manager("redis", {"host": "cap-second"})
    first_backend = _Backend()
    first._backend = first_backend
    first.close()
    assert first._users == 0

    third = ConnectionManager.get_manager("redis", {"host": "cap-third"})
    assert third is not first
    assert first_backend.disconnect_calls == 1
    assert first._retired is True
    assert len(ConnectionManager._managers) == 2
    # The still-held second manager survived the cap pass.
    assert second in ConnectionManager._managers.values()


def test_finalizer_token_fails_open_when_frame_audit_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = manager_module._RetirementFinalizerToken()
    monkeypatch.setattr(sys, "_current_frames", Mock(side_effect=RuntimeError("audit")))
    assert token.active is False


def test_load_descriptor_error_is_static_for_a_plugin() -> None:
    descriptor = BackendDescriptor(
        "external",
        "missing.module.Backend",
        "missing.module.Settings",
        frozenset({"storage"}),
    )
    with pytest.raises(ConfigurationError) as exc_info:
        plugin_module._load_descriptor_object(descriptor, descriptor.backend_cls_path)
    assert str(exc_info.value) == "Selected backend has an invalid plugin class path."
    assert exc_info.value.__cause__ is None


def test_plugin_capability_snapshot_accepts_non_queue_and_rejects_bad_ack_flags() -> (
    None
):
    non_queue = BackendDescriptor(
        "external", "x.Backend", "x.Settings", frozenset({"storage"})
    )
    assert plugin_module._validate_plugin_ack_class(non_queue, _Backend) == (
        False,
        False,
        False,
    )

    class BadConcurrent(_QueueBackend):
        requires_ack = False
        supports_concurrent_ack = "yes"  # type: ignore[assignment]

    with pytest.raises(ConfigurationError, match="invalid acknowledgement contract"):
        plugin_module._validate_plugin_ack_class(
            BackendDescriptor(
                "external", "x.Backend", "x.Settings", frozenset({"queue"})
            ),
            BadConcurrent,
        )

    class MissingMethods(_QueueBackend):
        requires_ack = True
        supports_concurrent_ack = True
        pop_with_ack = QueueBackend.pop_with_ack
        ack = QueueBackend.ack
        nack = QueueBackend.nack

    with pytest.raises(ConfigurationError, match="invalid acknowledgement contract"):
        plugin_module._validate_plugin_ack_class(
            BackendDescriptor(
                "external", "x.Backend", "x.Settings", frozenset({"queue"})
            ),
            MissingMethods,
        )


def test_plugin_runtime_contract_reports_each_missing_interface_safely() -> None:
    descriptor = BackendDescriptor(
        "external",
        "x.Backend",
        "x.Settings",
        frozenset({"queue", "set"}),
    )
    with pytest.raises(ConfigurationError) as exc_info:
        plugin_module._validate_backend_contract(_Backend(), descriptor)
    assert str(exc_info.value) == (
        "Selected third-party backend does not implement its declared contract: "
        "missing QueueBackend, SetBackend."
    )
    assert exc_info.value.setting_name == "SCRAPY_BACKEND_TYPE"

    valid = BackendDescriptor(
        "external", "x.Backend", "x.Settings", frozenset({"queue"})
    )
    backend = _QueueBackend()
    assert plugin_module._validate_backend_contract(backend, valid) is backend


def test_static_ack_loader_and_model_fields_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = BackendDescriptor("external", "x.Backend", "x.Settings", frozenset())

    class Flags:
        requires_ack = True
        supports_concurrent_ack = False

    monkeypatch.setattr(plugin_module, "_load_descriptor_object", lambda *_args: Flags)
    assert plugin_module._load_static_ack_capabilities(descriptor) == (True, False)

    class BadFlags:
        requires_ack = True
        supports_concurrent_ack = 1

    monkeypatch.setattr(
        plugin_module, "_load_descriptor_object", lambda *_args: BadFlags
    )
    with pytest.raises(ConfigurationError, match="invalid acknowledgement contract"):
        plugin_module._load_static_ack_capabilities(descriptor)

    class BrokenFields:
        @property
        def model_fields(self) -> object:
            raise RuntimeError("plugin metadata")

    assert plugin_module._model_field_names(BrokenFields()) == frozenset()
    assert plugin_module._model_field_names(type("NoFields", (), {})) == frozenset()


def test_deferred_adapter_forwards_basic_queue_operations_and_empty_delivery() -> None:
    delegate = _QueueBackend()
    adapter = _DeferredAckPluginQueueBackend(delegate, supports_concurrent_ack=True)
    adapter.push("q", b"item", 2.0)
    assert adapter.pop("q") is None
    assert adapter.queue_len("q") == 0
    adapter.clear_queue("q")
    assert delegate.pop_result == (None, None)
    assert adapter.pop_with_ack("q") == (None, None)

    # A string subclass is rejected before plugin dispatch.
    class QueueName(str):
        pass

    with pytest.raises(QueueError, match="exact built-in string"):
        adapter.pop(QueueName("q"))


def test_deferred_settlement_reentry_is_fenced_and_token_is_cleaned() -> None:
    delegate = _QueueBackend()
    token = object()
    delegate.pop_result = (b"item", token)
    adapter = _DeferredAckPluginQueueBackend(delegate, supports_concurrent_ack=True)
    assert adapter.pop_with_ack("q") == (b"item", token)
    reentry_errors: list[QueueError] = []

    def reenter(_queue_name: str, *, token: object | None = None) -> None:
        assert token is not None
        try:
            adapter.ack("q", token=token)
        except QueueError as error:
            reentry_errors.append(error)

    delegate.ack = reenter  # type: ignore[method-assign]
    adapter.ack("q", token=token)
    assert len(reentry_errors) == 1
    assert "unknown acknowledgement token" in str(reentry_errors[0])
    assert adapter._active_ack_tokens == {}
    assert adapter._settling_ack_tokens == set()


def test_deferred_adapter_rejects_missing_settlement_token_without_plugin_call() -> (
    None
):
    delegate = _QueueBackend()
    adapter = _DeferredAckPluginQueueBackend(delegate, supports_concurrent_ack=False)
    with pytest.raises(QueueError) as exc_info:
        adapter.nack("q")
    assert (
        str(exc_info.value)
        == "Deferred-ack settlement requires an issued acknowledgement token"
    )
    assert delegate.nack_calls == []


def test_manager_create_backend_sanitizes_plugin_settings_and_constructor_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = BackendDescriptor(
        "external",
        "plugin.Backend",
        "plugin.Settings",
        frozenset({"storage"}),
    )
    monkeypatch.setattr(manager_module, "get_descriptor", lambda _name: descriptor)

    class BadSettings:
        model_fields = {"host": object()}

        def __init__(self, **_values: object) -> None:
            raise ValueError("password=plugin-secret")

    monkeypatch.setattr(
        manager_module,
        "_load_descriptor_object",
        lambda _descriptor, path: (
            BadSettings if path.endswith("Settings") else _Backend
        ),
    )
    manager = ConnectionManager("external", {"host": "plugin-secret"})
    with pytest.raises(ConfigurationError) as exc_info:
        manager._create_backend()
    assert str(exc_info.value) == "Invalid backend setting 'backend_settings'."
    assert "plugin-secret" not in repr(exc_info.value)
    assert exc_info.value.__context__ is None

    class GoodSettings:
        model_fields = {}

        def __init__(self, **_values: object) -> None:
            pass

    class BadBackend(_Backend):
        def __init__(self, _settings: object | None = None) -> None:
            raise RuntimeError("constructor-secret")

    monkeypatch.setattr(
        manager_module,
        "_load_descriptor_object",
        lambda _descriptor, path: (
            GoodSettings if path.endswith("Settings") else BadBackend
        ),
    )
    manager = ConnectionManager("external", {})
    with pytest.raises(ConfigurationError) as exc_info:
        manager._create_backend()
    assert str(exc_info.value) == "Selected backend could not be constructed."
    assert "constructor-secret" not in repr(exc_info.value)


def test_registry_key_normalization_is_type_tagged_and_secret_free() -> None:
    values: dict[str, object] = {
        "secret": SecretStr("credential-value"),
        "bytes": SecretBytes(b"binary-credential"),
        "date": date(2024, 1, 2),
        "datetime": datetime(2024, 1, 2, 3, 4),
        "time": datetime_time(3, 4),
        "path": Path("/tmp/path"),
        "uuid": UUID("12345678-1234-5678-1234-567812345678"),
        "module": ModuleType("module-name"),
        "nested": {"values", "more"},
    }
    key = ConnectionManager._registry_key("redis", values)
    assert "credential-value" not in key
    assert "binary-credential" not in key
    assert key == ConnectionManager._registry_key("redis", dict(values))
