"""Regression tests for issuer-bound deferred-ack settlement."""

from __future__ import annotations

import threading
from unittest.mock import Mock

import pytest

from scrapy_extension.backends.base import QueueBackend
from scrapy_extension.backends.connectors import _DeferredAckPluginQueueBackend
from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.strategies.base import _BoundQueueAckToken


@pytest.mark.parametrize("operation", ["ack", "nack"])
def test_failed_settlement_stays_pending_and_success_becomes_terminal(operation):
    """A broker failure is retryable, but only one successful terminal action runs."""
    backend = Mock(spec=QueueBackend)
    selected = getattr(backend, operation)
    opposite = backend.nack if operation == "ack" else backend.ack
    selected.side_effect = [QueueError("temporary"), None]
    token = _BoundQueueAckToken(backend, "physical-q", "raw-token")

    with pytest.raises(QueueError, match="temporary"):
        getattr(token, operation)()
    assert token.state == "pending"

    getattr(token, operation)()
    assert token.state == f"{operation}ed"

    getattr(token, operation)()
    (token.nack if operation == "ack" else token.ack)()
    assert selected.call_count == 2
    opposite.assert_not_called()


def test_binding_is_read_only_and_repr_hides_raw_token():
    """Diagnostics expose routing metadata without logging broker credentials/handles."""

    class _SensitiveToken:
        def __repr__(self) -> str:
            return "do-not-log"

    backend = Mock(spec=QueueBackend)
    token = _BoundQueueAckToken(backend, "physical-q", _SensitiveToken())

    with pytest.raises(AttributeError):
        token.backend = backend  # type: ignore[misc]
    with pytest.raises(AttributeError):
        token.queue_name = "other"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        token.token = "other"  # type: ignore[misc]

    rendered = repr(token)
    assert "do-not-log" not in rendered
    assert "token_type=_SensitiveToken" in rendered
    assert "state='pending'" in rendered


@pytest.mark.parametrize("operation", ["ack", "nack"])
def test_settlement_hook_runs_outside_state_lock_and_reentry_is_a_noop(operation):
    """A callback can inspect token state and reenter without deadlock or duplication."""
    backend = Mock(spec=QueueBackend)
    lock_checks: list[bool] = []
    token = _BoundQueueAckToken(backend, "physical-q", "raw-token")
    selected = getattr(backend, operation)
    opposite = backend.nack if operation == "ack" else backend.ack

    def reentrant_hook(queue_name: str, *, token: object | None = None) -> None:
        del queue_name, token
        acquired = bound._state_lock.acquire(blocking=False)
        lock_checks.append(acquired)
        if acquired:
            bound._state_lock.release()
        (bound.nack if operation == "ack" else bound.ack)()

    bound = token
    selected.side_effect = reentrant_hook

    getattr(bound, operation)()

    assert lock_checks == [True]
    assert bound.state == f"{operation}ed"
    selected.assert_called_once_with("physical-q", token="raw-token")
    opposite.assert_not_called()


def test_bound_plugin_hook_can_acquire_adapter_and_token_locks() -> None:
    """The complete settlement chain invokes plugin code outside both project locks."""
    delegate = Mock(spec=QueueBackend)
    delegate.pop_with_ack.return_value = (b"item", "private-token")
    adapter = _DeferredAckPluginQueueBackend(
        delegate,
        supports_concurrent_ack=True,
    )
    _, raw_token = adapter.pop_with_ack("physical-q")
    bound = _BoundQueueAckToken(adapter, "physical-q", raw_token)
    lock_checks: list[tuple[bool, bool]] = []

    def checking_ack(queue_name: str, *, token: object | None = None) -> None:
        del queue_name, token
        state_acquired = bound._state_lock.acquire(blocking=False)
        adapter_acquired = adapter._ack_contract_lock.acquire(blocking=False)
        lock_checks.append((state_acquired, adapter_acquired))
        if adapter_acquired:
            adapter._ack_contract_lock.release()
        if state_acquired:
            bound._state_lock.release()
        bound.nack()

    delegate.ack.side_effect = checking_ack

    bound.ack()

    assert lock_checks == [(True, True)]
    assert bound.state == "acked"
    delegate.ack.assert_called_once_with("physical-q", token="private-token")
    delegate.nack.assert_not_called()
    assert adapter._active_ack_tokens == {}
    assert adapter._settling_ack_tokens == set()


def test_concurrent_ack_and_nack_emit_only_one_terminal_broker_call():
    """Concurrent completion paths serialize around one terminal transition."""
    backend = Mock(spec=QueueBackend)
    ack_entered = threading.Event()
    nack_entered = threading.Event()
    release_ack = threading.Event()
    nack_started = threading.Event()

    def blocking_ack(queue_name: str, *, token: object | None = None) -> None:
        del queue_name, token
        ack_entered.set()
        release_ack.wait(timeout=2.0)

    def record_nack(queue_name: str, *, token: object | None = None) -> None:
        del queue_name, token
        nack_entered.set()

    backend.ack.side_effect = blocking_ack
    backend.nack.side_effect = record_nack
    token = _BoundQueueAckToken(backend, "physical-q", "raw-token")

    ack_thread = threading.Thread(target=token.ack, daemon=True)

    def nack_after_start() -> None:
        nack_started.set()
        token.nack()

    nack_thread = threading.Thread(target=nack_after_start, daemon=True)
    try:
        ack_thread.start()
        assert ack_entered.wait(timeout=1.0)
        nack_thread.start()
        assert nack_started.wait(timeout=1.0)
        assert not nack_entered.wait(timeout=0.05)
    finally:
        release_ack.set()

    ack_thread.join(timeout=1.0)
    nack_thread.join(timeout=1.0)
    assert not ack_thread.is_alive()
    assert not nack_thread.is_alive()
    assert token.state == "acked"
    backend.ack.assert_called_once_with("physical-q", token="raw-token")
    backend.nack.assert_not_called()


def test_blocked_failure_fences_concurrent_nack_then_restores_ack_retry():
    """An in-flight attempt excludes racers but a broker failure is not terminal."""
    backend = Mock(spec=QueueBackend)
    entered = threading.Event()
    release = threading.Event()
    attempts = 0

    def fail_once(queue_name: str, *, token: object | None = None) -> None:
        del queue_name, token
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            entered.set()
            assert release.wait(timeout=2.0)
            raise QueueError("temporary")

    backend.ack.side_effect = fail_once
    token = _BoundQueueAckToken(backend, "physical-q", "raw-token")
    errors: list[BaseException] = []

    def ack_in_worker() -> None:
        try:
            token.ack()
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=ack_in_worker, daemon=True)
    worker.start()
    assert entered.wait(timeout=1.0)

    token.nack()
    backend.nack.assert_not_called()
    assert backend.ack.call_count == 1

    release.set()
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], QueueError)
    assert token.state == "pending"

    token.ack()
    assert token.state == "acked"
    assert attempts == 2
    assert backend.ack.call_count == 2
    backend.nack.assert_not_called()
