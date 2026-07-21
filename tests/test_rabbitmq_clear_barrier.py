"""Lifecycle barrier contracts for RabbitMQ queue clearing."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest
from pika.exceptions import AMQPError

from scrapy_extension.backends.rabbitmq import (
  _MAX_IN_FLIGHT,
  RabbitMQBackend,
  _RabbitMQAckToken,
)
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import RabbitMQSettings


def _connected_backend() -> tuple[RabbitMQBackend, MagicMock]:
  backend = RabbitMQBackend(RabbitMQSettings())
  channel = MagicMock(name="channel")
  backend._activate_channel(MagicMock(name="connection"), channel)
  return backend, channel


def _deliver(channel: MagicMock, tag: int, body: bytes = b"body") -> None:
  channel.basic_get.return_value = (MagicMock(delivery_tag=tag), None, body)


def test_clear_rejects_same_queue_token_delivery_before_purge() -> None:
  backend, channel = _connected_backend()
  _deliver(channel, 1)
  _body, token = backend.pop_with_ack("queue-a")

  with pytest.raises(QueueError, match="in-flight") as exc_info:
    backend.clear_queue("queue-a")

  assert token is not None
  assert exc_info.value.operation == "clear_queue"
  channel.queue_purge.assert_not_called()


def test_in_flight_delivery_does_not_block_an_unrelated_queue() -> None:
  backend, channel = _connected_backend()
  _deliver(channel, 1)
  backend.pop_with_ack("queue-a")

  backend.clear_queue("queue-b")

  channel.queue_purge.assert_called_once_with(queue="queue-b")


@pytest.mark.parametrize("operation", ["ack", "nack"])
def test_successful_settlement_releases_clear_barrier(operation: str) -> None:
  backend, channel = _connected_backend()
  _deliver(channel, 1)
  _body, token = backend.pop_with_ack("queue-a")

  getattr(backend, operation)("queue-a", token=token)
  backend.clear_queue("queue-a")

  channel.queue_purge.assert_called_once_with(queue="queue-a")


def test_failed_settlement_keeps_clear_barrier_closed() -> None:
  backend, channel = _connected_backend()
  _deliver(channel, 1)
  _body, token = backend.pop_with_ack("queue-a")
  channel.basic_ack.side_effect = [AMQPError("ambiguous ack"), None]

  with pytest.raises(QueueError, match="Failed to ack"):
    backend.ack("queue-a", token=token)
  with pytest.raises(QueueError, match="in-flight"):
    backend.clear_queue("queue-a")

  channel.queue_purge.assert_not_called()

  backend.ack("queue-a", token=token)
  backend.clear_queue("queue-a")

  assert channel.basic_ack.call_count == 2
  channel.queue_purge.assert_called_once_with(queue="queue-a")


def test_token_pop_does_not_populate_legacy_ack_slot() -> None:
  backend, channel = _connected_backend()
  _deliver(channel, 1)

  _body, token = backend.pop_with_ack("queue-a")
  backend.ack("queue-a")

  assert token is not None
  assert backend._last_delivery_tag is None
  channel.basic_ack.assert_not_called()
  with pytest.raises(QueueError, match="in-flight"):
    backend.clear_queue("queue-a")


def test_legacy_pop_blocks_clear_until_legacy_ack_succeeds() -> None:
  backend, channel = _connected_backend()
  _deliver(channel, 1)

  backend.pop("queue-a")
  with pytest.raises(QueueError, match="in-flight"):
    backend.clear_queue("queue-a")

  backend.ack("queue-a")
  backend.clear_queue("queue-a")

  channel.basic_ack.assert_called_once_with(delivery_tag=1)
  channel.queue_purge.assert_called_once_with(queue="queue-a")


def test_disconnect_resets_local_barrier_for_broker_requeue_and_reconnect() -> None:
  backend, old_channel = _connected_backend()
  _deliver(old_channel, 1)
  backend.pop_with_ack("queue-a")

  backend.disconnect()
  new_channel = MagicMock(name="new_channel")
  backend._activate_channel(MagicMock(name="new_connection"), new_channel)
  backend.clear_queue("queue-a")

  new_channel.queue_purge.assert_called_once_with(queue="queue-a")


def test_clear_barrier_remains_exact_after_diagnostic_token_cap() -> None:
  backend, channel = _connected_backend()
  for tag in range(_MAX_IN_FLIGHT):
    backend._in_flight_tags.add(
      _RabbitMQAckToken(tag, backend._channel_generation)
    )
  # The diagnostic set's values are irrelevant; only its size triggers the cap.
  assert len(backend._in_flight_tags) == _MAX_IN_FLIGHT
  _deliver(channel, _MAX_IN_FLIGHT + 1)

  _body, token = backend.pop_with_ack("queue-a")

  assert token is not None
  assert token not in backend._in_flight_tags
  with pytest.raises(QueueError, match="in-flight"):
    backend.clear_queue("queue-a")


def test_clear_and_pop_are_linearized_around_broker_purge() -> None:
  backend, channel = _connected_backend()
  purge_started = threading.Event()
  allow_purge = threading.Event()
  pop_finished = threading.Event()
  failures: list[BaseException] = []

  def blocking_purge(*, queue: str) -> None:
    assert queue == "queue-a"
    purge_started.set()
    assert allow_purge.wait(timeout=2)

  def run_clear() -> None:
    try:
      backend.clear_queue("queue-a")
    except BaseException as exc:  # pragma: no cover - assertion reports the value
      failures.append(exc)

  def run_pop() -> None:
    try:
      backend.pop_with_ack("queue-a")
    except BaseException as exc:  # pragma: no cover - assertion reports the value
      failures.append(exc)
    finally:
      pop_finished.set()

  channel.queue_purge.side_effect = blocking_purge
  _deliver(channel, 1)
  clear_thread = threading.Thread(target=run_clear)
  pop_thread = threading.Thread(target=run_pop)

  clear_thread.start()
  assert purge_started.wait(timeout=2)
  pop_thread.start()
  assert pop_finished.wait(timeout=0.05) is False
  channel.basic_get.assert_not_called()

  allow_purge.set()
  clear_thread.join(timeout=2)
  pop_thread.join(timeout=2)

  assert not clear_thread.is_alive()
  assert not pop_thread.is_alive()
  assert failures == []
  channel.queue_purge.assert_called_once_with(queue="queue-a")
  channel.basic_get.assert_called_once_with(queue="queue-a", auto_ack=False)


def test_pop_issued_first_closes_clear_barrier_before_purge() -> None:
  backend, channel = _connected_backend()
  get_started = threading.Event()
  allow_get = threading.Event()
  clear_finished = threading.Event()
  failures: list[BaseException] = []

  def blocking_get(*, queue: str, auto_ack: bool):
    assert queue == "queue-a"
    assert auto_ack is False
    get_started.set()
    assert allow_get.wait(timeout=2)
    return (MagicMock(delivery_tag=1), None, b"body")

  def run_pop() -> None:
    backend.pop_with_ack("queue-a")

  def run_clear() -> None:
    try:
      backend.clear_queue("queue-a")
    except BaseException as exc:
      failures.append(exc)
    finally:
      clear_finished.set()

  channel.basic_get.side_effect = blocking_get
  pop_thread = threading.Thread(target=run_pop)
  clear_thread = threading.Thread(target=run_clear)

  pop_thread.start()
  assert get_started.wait(timeout=2)
  clear_thread.start()
  assert clear_finished.wait(timeout=0.05) is False
  channel.queue_purge.assert_not_called()

  allow_get.set()
  pop_thread.join(timeout=2)
  clear_thread.join(timeout=2)

  assert not pop_thread.is_alive()
  assert not clear_thread.is_alive()
  assert len(failures) == 1
  assert isinstance(failures[0], QueueError)
  assert "in-flight" in str(failures[0])
  channel.queue_purge.assert_not_called()
