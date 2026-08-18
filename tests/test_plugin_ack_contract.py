"""Conformance tests for third-party deferred acknowledgement plugins."""

from __future__ import annotations

from collections import defaultdict, deque
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


def test_deferred_plugin_rejects_empty_and_missing_tokens() -> None:
    backend = _DeferredPlugin()
    contract = _DeferredAckPluginQueueBackend(
        backend,
        supports_concurrent_ack=True,
    )
    empty_tokens: tuple[Any, ...] = (None, "", b"", (), [], {}, set(), frozenset())
    for token in empty_tokens:
        backend.pop_results["q"].append((b"item", token))
        with pytest.raises(QueueError, match="token"):
            contract.pop_with_ack("q")


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
