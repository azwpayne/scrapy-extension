"""R16-C — pin the mock_connection_manager fixture's durability contract.

The fixture's ``manager.push_is_durable`` knob lets a test exercise the volatile
(non-durable) push path. When a durable push is required of a non-durable mock,
the fixture must raise the public ``QueueError`` (as the real ConnectionManager
does — connectors.py catches the internal ``_DurablePushRequired`` and re-raises
``QueueError``), not the internal exception.
"""
from __future__ import annotations

import pytest

from scrapy_extension.exceptions.base import QueueError


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
