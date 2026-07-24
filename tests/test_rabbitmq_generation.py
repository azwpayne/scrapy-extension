"""Deterministic connection-generation regressions for RabbitMQ."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pika.exceptions
import pytest

from scrapy_extension.backends.rabbitmq import RabbitMQBackend, _RabbitMQCandidate
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings import RabbitMQSettings


def _backend() -> RabbitMQBackend:
  return RabbitMQBackend(RabbitMQSettings())


def _handles(name: str) -> tuple[MagicMock, MagicMock]:
  connection = MagicMock(name=f"{name}-connection", is_open=True)
  channel = MagicMock(name=f"{name}-channel", is_open=True)
  connection.channel.return_value = channel
  return connection, channel


def test_live_connect_is_idempotent_and_keeps_token_settleable(mocker) -> None:
  backend = _backend()
  connection_a, channel_a = _handles("a")
  connection_b, channel_b = _handles("b")
  factory = mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    side_effect=[connection_a, connection_b],
  )
  channel_a.basic_get.return_value = (
    MagicMock(delivery_tag=17),
    None,
    b"payload",
  )

  backend.connect()
  _body, token = backend.pop_with_ack("q")
  backend.connect()
  backend.ack("q", token=token)

  factory.assert_called_once()
  assert backend._connection is connection_a
  assert backend._channel is channel_a
  channel_a.basic_ack.assert_called_once_with(delivery_tag=17, multiple=False)
  channel_b.basic_ack.assert_not_called()
  connection_a.close.assert_not_called()


def test_failed_candidate_cannot_erase_peer_generation(mocker) -> None:
  backend = _backend()
  candidate_connection, candidate_channel = _handles("candidate")
  peer_connection, peer_channel = _handles("peer")
  prepare_entered = threading.Event()
  release_prepare = threading.Event()
  errors: list[BaseException] = []

  def fail_prepare() -> None:
    prepare_entered.set()
    assert release_prepare.wait(timeout=2.0)
    raise pika.exceptions.AMQPError("candidate prepare failed")

  def connect() -> None:
    try:
      backend.connect()
    except BaseException as error:  # pragma: no cover - assertion aid
      errors.append(error)

  candidate_channel.confirm_delivery.side_effect = fail_prepare
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    return_value=candidate_connection,
  )
  connect_thread = threading.Thread(target=connect)
  connect_thread.start()
  assert prepare_entered.wait(timeout=2.0)

  # Simulate a peer connect publishing a healthy session while this private
  # candidate is still preparing. Candidate cleanup must remain local.
  backend._activate_channel(peer_connection, peer_channel)
  peer_channel.basic_get.return_value = (
    MagicMock(delivery_tag=23),
    None,
    b"peer-payload",
  )
  _body, peer_token = backend.pop_with_ack("q")
  release_prepare.set()
  connect_thread.join(timeout=2.0)

  assert not connect_thread.is_alive()
  assert len(errors) == 1
  assert isinstance(errors[0], BackendConnectionError)
  assert backend._connection is peer_connection
  assert backend._channel is peer_channel
  assert backend.is_connected() is True
  backend.ack("q", token=peer_token)
  peer_channel.basic_ack.assert_called_once_with(
    delivery_tag=23,
    multiple=False,
  )
  candidate_channel.close.assert_called_once()
  candidate_connection.close.assert_called_once()
  peer_channel.close.assert_not_called()
  peer_connection.close.assert_not_called()


def test_connect_closes_connection_on_baseexception_during_channel_open(mocker) -> None:
  """R17-B: a Ctrl+C during channel-open must not leak the built connection.

  ``_open_prepared_channel`` builds the ``pika.BlockingConnection`` first, then
  opens/prepares the channel (real AMQP I/O — the natural landing point for an
  operator Ctrl+C during a slow handshake). Its cleanup arm was
  ``except Exception``, which cannot catch ``BaseException`` — so a
  ``KeyboardInterrupt`` raised by ``connection.channel()`` escaped before
  ``connection.close()`` ran, leaking the connection's background heartbeat/I/O
  thread + TCP FD. The arm must clean up on ``BaseException`` too (R16-A
  parity; mirror kafka/rocketmq/dynamodb connect arms).
  """
  backend = _backend()
  candidate_connection, _candidate_channel = _handles("candidate")
  candidate_connection.channel.side_effect = KeyboardInterrupt
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    return_value=candidate_connection,
  )

  with pytest.raises(KeyboardInterrupt):
    backend.connect()

  # The connection built before the interrupt is closed — no FD/thread leak.
  candidate_connection.close.assert_called_once()
  assert backend._connection is None
  assert backend.is_connected() is False


def test_connect_closes_candidate_on_baseexception_in_publish_window(mocker) -> None:
  """R17-B: a Ctrl+C in the candidate→publish window must close the candidate.

  ``connect()`` returns a fully-prepared candidate (live ``BlockingConnection``
  + open channel) off-instance, then publishes it under lock. Neither the
  build try nor the publish window carried an ``except BaseException`` arm, so
  a ``Ctrl+C`` landing between candidate creation and publication bypassed the
  ``except Exception`` arm and never reached the ``if not published`` close —
  leaking the candidate. A ``BaseException`` arm must close the candidate when
  it was not yet published (resource leak, not wedge: the candidate never
  reaches instance state, so ``is_connected()`` stays truthful).
  """
  backend = _backend()
  candidate_connection, candidate_channel = _handles("candidate")
  snapshot = backend._capture_connection_snapshot()
  candidate = _RabbitMQCandidate(
    connection=candidate_connection,
    channel=candidate_channel,
    snapshot=snapshot,
  )
  mocker.patch.object(backend, "_connect_standalone", return_value=candidate)
  mocker.patch.object(backend, "_publish_handles_locked", side_effect=KeyboardInterrupt)

  with pytest.raises(KeyboardInterrupt):
    backend.connect()

  candidate_channel.close.assert_called_once()
  candidate_connection.close.assert_called_once()
  assert backend._connection is None
  assert backend._channel is None
  assert backend.is_connected() is False


def test_disconnect_fences_in_progress_candidate(mocker) -> None:
  backend = _backend()
  candidate_connection, candidate_channel = _handles("candidate")
  construction_entered = threading.Event()
  release_construction = threading.Event()
  disconnect_finished = threading.Event()
  errors: list[BaseException] = []

  def construct(_parameters):
    construction_entered.set()
    assert release_construction.wait(timeout=2.0)
    return candidate_connection

  def connect() -> None:
    try:
      backend.connect()
    except BaseException as error:  # pragma: no cover - assertion aid
      errors.append(error)

  def disconnect() -> None:
    try:
      backend.disconnect()
    finally:
      disconnect_finished.set()

  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    side_effect=construct,
  )
  connect_thread = threading.Thread(target=connect)
  disconnect_thread = threading.Thread(target=disconnect)
  connect_thread.start()
  assert construction_entered.wait(timeout=2.0)
  disconnect_thread.start()
  returned_while_candidate_was_blocked = disconnect_finished.wait(timeout=0.5)
  release_construction.set()
  connect_thread.join(timeout=2.0)
  disconnect_thread.join(timeout=2.0)

  assert returned_while_candidate_was_blocked is True
  assert not connect_thread.is_alive()
  assert not disconnect_thread.is_alive()
  assert errors == []
  assert backend.is_connected() is False
  candidate_channel.close.assert_called_once()
  candidate_connection.close.assert_called_once()


def test_replacing_closed_session_closes_old_handles(mocker) -> None:
  backend = _backend()
  old_connection, old_channel = _handles("old")
  old_connection.is_open = False
  old_channel.is_open = False
  new_connection, new_channel = _handles("new")
  backend._activate_channel(old_connection, old_channel)
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    return_value=new_connection,
  )

  backend.connect()

  assert backend._connection is new_connection
  assert backend._channel is new_channel
  old_channel.close.assert_called_once()
  old_connection.close.assert_called_once()


def test_replacement_retires_old_generation_before_building_candidate(
  mocker,
) -> None:
  backend = _backend()
  old_connection, old_channel = _handles("old")
  new_connection, _new_channel = _handles("new")
  old_channel.is_open = False
  close_entered = threading.Event()
  release_close = threading.Event()
  factory_called = threading.Event()

  def close_old_channel() -> None:
    close_entered.set()
    assert release_close.wait(timeout=2.0)

  def construct(_parameters):
    factory_called.set()
    return new_connection

  old_channel.close.side_effect = close_old_channel
  backend._activate_channel(old_connection, old_channel)
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    side_effect=construct,
  )
  connect_thread = threading.Thread(target=backend.connect)
  connect_thread.start()
  assert close_entered.wait(timeout=2.0)

  assert factory_called.is_set() is False
  release_close.set()
  connect_thread.join(timeout=2.0)

  assert not connect_thread.is_alive()
  assert factory_called.is_set() is True
  assert backend._connection is new_connection


def test_connect_waits_for_disconnect_to_retire_old_unacked_generation(
  mocker,
) -> None:
  backend = _backend()
  old_connection, old_channel = _handles("old")
  new_connection, _new_channel = _handles("new")
  close_entered = threading.Event()
  release_close = threading.Event()
  factory_called = threading.Event()
  connect_waiting_for_retirement = threading.Event()
  errors: list[BaseException] = []
  old_channel.basic_get.return_value = (
    MagicMock(delivery_tag=41),
    None,
    b"old-unacked",
  )

  def close_old_channel() -> None:
    close_entered.set()
    assert release_close.wait(timeout=2.0)

  def construct(_parameters):
    factory_called.set()
    return new_connection

  def run(operation) -> None:
    try:
      operation()
    except BaseException as error:  # pragma: no cover - assertion aid
      errors.append(error)

  old_channel.close.side_effect = close_old_channel
  backend._activate_channel(old_connection, old_channel)
  _body, token = backend.pop_with_ack("q")
  assert token is not None

  class _ObservedRetirementLock:
    def __init__(self, lock) -> None:
      self._lock = lock

    def __enter__(self):
      if threading.current_thread().name == "rabbit-connect":
        connect_waiting_for_retirement.set()
      self._lock.acquire()
      return self

    def __exit__(self, *_args) -> None:
      self._lock.release()

  backend._retirement_lock = _ObservedRetirementLock(backend._retirement_lock)
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    side_effect=construct,
  )
  disconnect_thread = threading.Thread(target=run, args=(backend.disconnect,))
  connect_thread = threading.Thread(
    target=run,
    args=(backend.connect,),
    name="rabbit-connect",
  )
  disconnect_thread.start()
  assert close_entered.wait(timeout=2.0)
  connect_thread.start()
  assert connect_waiting_for_retirement.wait(timeout=2.0)

  built_before_old_delivery_was_requeued = factory_called.is_set()
  release_close.set()
  disconnect_thread.join(timeout=2.0)
  connect_thread.join(timeout=2.0)

  assert built_before_old_delivery_was_requeued is False
  assert not disconnect_thread.is_alive()
  assert not connect_thread.is_alive()
  assert errors == []
  assert factory_called.is_set() is True
  assert backend._connection is new_connection


def test_disconnect_fences_queued_connect_intent(mocker) -> None:
  backend = _backend()
  candidate_connection, candidate_channel = _handles("candidate")
  construction_entered = threading.Event()
  release_construction = threading.Event()
  second_intent_captured = threading.Event()
  capture_lock = threading.Lock()
  capture_count = 0
  errors: list[BaseException] = []
  original_capture = backend._capture_connect_intent

  def capture_intent() -> tuple[int, bool]:
    nonlocal capture_count
    intent = original_capture()
    with capture_lock:
      capture_count += 1
      if capture_count == 2:
        second_intent_captured.set()
    return intent

  def construct(_parameters):
    construction_entered.set()
    assert release_construction.wait(timeout=2.0)
    return candidate_connection

  def connect() -> None:
    try:
      backend.connect()
    except BaseException as error:  # pragma: no cover - assertion aid
      errors.append(error)

  mocker.patch.object(
    backend,
    "_capture_connect_intent",
    side_effect=capture_intent,
  )
  factory = mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    side_effect=construct,
  )
  first = threading.Thread(target=connect)
  second = threading.Thread(target=connect)
  first.start()
  assert construction_entered.wait(timeout=2.0)
  second.start()
  assert second_intent_captured.wait(timeout=2.0)

  backend.disconnect()
  release_construction.set()
  first.join(timeout=2.0)
  second.join(timeout=2.0)

  assert not first.is_alive()
  assert not second.is_alive()
  assert errors == []
  factory.assert_called_once()
  assert backend.is_connected() is False
  candidate_channel.close.assert_called_once()
  candidate_connection.close.assert_called_once()


def test_queue_policy_is_frozen_until_explicit_reconnect(mocker) -> None:
  backend = _backend()
  connection_a, channel_a = _handles("a")
  connection_b, channel_b = _handles("b")
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    side_effect=[connection_a, connection_b],
  )

  backend.config.exclusive = True
  backend.connect()
  backend.config.durable = False
  backend.config.auto_delete = True
  backend.config.exclusive = False
  backend.config.max_priority = 7
  backend.config.delivery_mode = 1
  backend.push("before-reconnect", b"a", priority=99)

  channel_a.queue_declare.assert_called_once_with(
    queue="before-reconnect",
    durable=True,
    auto_delete=False,
    exclusive=True,
    arguments={"x-max-priority": 255},
  )
  first_properties = channel_a.basic_publish.call_args.kwargs["properties"]
  assert first_properties.priority == 99
  assert first_properties.delivery_mode == 2

  backend.disconnect()
  backend.connect()
  backend.push("after-reconnect", b"b", priority=99)

  channel_b.queue_declare.assert_called_once_with(
    queue="after-reconnect",
    durable=False,
    auto_delete=True,
    exclusive=False,
    arguments={"x-max-priority": 7},
  )
  second_properties = channel_b.basic_publish.call_args.kwargs["properties"]
  assert second_properties.priority == 7
  assert second_properties.delivery_mode == 1


def test_timeout_pop_does_not_cross_connection_generation() -> None:
  backend = _backend()
  connection_a, channel_a = _handles("a")
  connection_b, channel_b = _handles("b")
  backend._activate_channel(connection_a, channel_a)
  channel_a.basic_get.return_value = (None, None, None)

  def replace_generation(*, time_limit: float) -> None:
    assert time_limit > 0
    backend._activate_channel(connection_b, channel_b)

  connection_a.process_data_events.side_effect = replace_generation

  with pytest.raises(QueueError, match="connection changed"):
    backend.pop("q", timeout=0.2)

  channel_a.basic_get.assert_called_once()
  channel_b.basic_get.assert_not_called()
