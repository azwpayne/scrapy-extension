"""R16-C — pin the mock_connection_manager fixture's durability contract.

These tests pin the **fixture's** reimplementation of the durability contract
(the ``push_queue_with_durability`` closure installed on the MagicMock at
``conftest.py``), NOT the production ``ConnectionManager``. They guard the
fixture itself: when a durable push is required of a non-durable mock, the
fixture must raise the public ``QueueError`` (``queue_name`` + ``operation``)
so tests exercising the volatile push path get a faithful signal.

Production parity — the real ``ConnectionManager._push_queue_with_durability``
translation of the internal ``_DurablePushRequired`` into the public
``QueueError`` — is asserted in
``test_connectors.py::TestOperationBoundQueueDurability`` AND by
``test_push_durability_translation_uses_real_connection_manager`` below, which
exercises the real ``ConnectionManager`` directly (including the translated
exception's ``queue_name``/``operation`` attributes, not just its message).
"""
from __future__ import annotations

import pytest

from scrapy_extension.backends.base import BackendType, QueueBackend
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions.base import QueueError

# --------------------------------------------------------------------------- #
# Fixture-parity tests — pin the conftest mock (NOT production code).
# --------------------------------------------------------------------------- #


def test_durable_default_returns_durable_receipt(mock_connection_manager) -> None:
  receipt = mock_connection_manager._push_queue_with_durability(
    "q", b"x", require_durable=True
  )
  assert receipt.worker_crash_durable is True


def test_volatile_knob_raises_queue_error(mock_connection_manager) -> None:
  mock_connection_manager.push_is_durable = False
  with pytest.raises(QueueError) as exc_info:
    mock_connection_manager._push_queue_with_durability("q", b"x", require_durable=True)
  assert exc_info.value.queue_name == "q"
  assert exc_info.value.operation == "push"


def test_volatile_knob_non_required_is_volatile(mock_connection_manager) -> None:
  mock_connection_manager.push_is_durable = False
  receipt = mock_connection_manager._push_queue_with_durability(
    "q", b"x", require_durable=False
  )
  assert receipt.worker_crash_durable is False


# --------------------------------------------------------------------------- #
# Production-parity test — exercise the REAL ConnectionManager translation.
# --------------------------------------------------------------------------- #


class _VolatileFakeQueueBackend(QueueBackend):
  """Minimal volatile queue backend (no ``_push_is_durable``) for the real-CM test.

  Inherits ``_push_with_durability`` from the base (which raises the internal
  ``_DurablePushRequired`` when ``require_durable=True`` on a volatile backend),
  so the real ``ConnectionManager`` translation path is genuinely exercised.
  """

  def connect(self) -> None: ...

  def disconnect(self) -> None: ...

  def is_connected(self) -> bool:
    return True

  def ping(self) -> bool:
    return True

  @property
  def backend_type(self):
    return BackendType.REDIS

  def push(self, queue_name, item, priority=0.0) -> None: ...

  def pop(self, queue_name, timeout=0.0):
    return None

  def queue_len(self, queue_name) -> int:
    return 0

  def clear_queue(self, queue_name) -> None: ...

  def ack(self, queue_name) -> None: ...


def test_push_durability_translation_uses_real_connection_manager() -> None:
  """R16-C/D: the real CM translates ``_DurablePushRequired`` → ``QueueError``.

  Builds a real ``ConnectionManager`` (not the fixture mirror), assigns a
  volatile backend, and asserts the production translation (connectors.py) both
  returns a volatile receipt for ordinary pushes AND raises a ``QueueError``
  carrying ``queue_name`` + ``operation="push"`` for a required-durable push.
  Co-located with the fixture tests above so they cannot be mistaken for the
  production coverage.
  """
  manager = ConnectionManager(BackendType.REDIS)
  manager._backend = _VolatileFakeQueueBackend()  # type: ignore[assignment]
  manager._breaker_configured = True
  manager._breaker = None
  try:
    receipt = manager._push_queue_with_durability("orders", b"payload")
    assert receipt.worker_crash_durable is False

    with pytest.raises(QueueError) as exc_info:
      manager._push_queue_with_durability(
        "orders", b"payload", require_durable=True
      )
    assert exc_info.value.queue_name == "orders"
    assert exc_info.value.operation == "push"
  finally:
    manager.close()
