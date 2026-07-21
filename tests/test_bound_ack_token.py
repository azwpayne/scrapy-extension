"""Regression tests for issuer-bound deferred-ack settlement."""

from __future__ import annotations

import threading
from unittest.mock import Mock

import pytest

from scrapy_extension.backends.base import QueueBackend
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
