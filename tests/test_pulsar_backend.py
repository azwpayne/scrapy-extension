"""Tests for PulsarBackend (subsystem ③) — mocked pulsar-client.

The real ``pulsar-client`` is a heavy C++ binding; we inject a mock into
``sys.modules`` before importing the backend so the module-level
``import pulsar`` succeeds without the dependency installed, and assert call
patterns against the mock.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("pulsar", MagicMock())
import pulsar  # noqa: E402 — the mocked module actually in sys.modules

from scrapy_extension.backends.base import (  # noqa: E402
  BackendType,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.pulsar import PulsarBackend  # noqa: E402
from scrapy_extension.exceptions import BackendConnectionError, QueueError  # noqa: E402
from scrapy_extension.settings import PulsarMode, PulsarSettings  # noqa: E402


def _make_backend(**overrides) -> PulsarBackend:
  return PulsarBackend(PulsarSettings(**overrides))


def _connected(mocker, **client_children):
  """Build a connected backend; ``client_children`` pre-stubs client attrs."""
  b = _make_backend()
  client = mocker.MagicMock()
  for attr, val in client_children.items():
    getattr(client, attr).return_value = val
  mocker.patch.object(pulsar, "Client", return_value=client)
  b.connect()
  return b, client


class TestPulsarBackendType:
  def test_backend_type_is_pulsar(self) -> None:
    assert _make_backend().backend_type is BackendType.PULSAR

  def test_queue_only_no_set_no_storage(self) -> None:
    b = _make_backend()
    assert not isinstance(b, SetBackend)
    assert not isinstance(b, StorageBackend)

  def test_settings_defaults(self) -> None:
    s = PulsarSettings()
    assert s.mode is PulsarMode.STANDALONE
    assert s.service_url == "pulsar://localhost:6650"
    assert s.consumer_type == "Shared"


class TestPulsarConnect:
  def test_connect_creates_client(self, mocker) -> None:
    b = _make_backend()
    client = mocker.MagicMock()
    mocker.patch.object(pulsar, "Client", return_value=client)
    b.connect()
    pulsar.Client.assert_called_once_with("pulsar://localhost:6650")
    assert b.is_connected() is True

  def test_connect_failure_raises_connection_error(self, mocker) -> None:
    b = _make_backend()
    mocker.patch.object(pulsar, "Client", side_effect=RuntimeError("boom"))
    with pytest.raises(BackendConnectionError):
      b.connect()
    assert b.is_connected() is False

  def test_connect_with_auth_token(self, mocker) -> None:
    b = _make_backend(auth_token="secret-token")
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    auth_mock = mocker.patch.object(pulsar, "AuthenticationToken")
    b.connect()
    auth_mock.assert_called_once_with("secret-token")

  def test_disconnect_closes_client(self, mocker) -> None:
    b, client = _connected(mocker)
    b.disconnect()
    client.close.assert_called_once()
    assert b.is_connected() is False


class TestPulsarPush:
  def test_push_creates_producer_and_sends(self, mocker) -> None:
    producer = mocker.MagicMock()
    b, client = _connected(mocker, create_producer=producer)
    b.push("queue1", b"payload")
    client.create_producer.assert_called_once_with("scrapy-queue1")
    producer.send.assert_called_once_with(b"payload")

  def test_push_reuses_cached_producer(self, mocker) -> None:
    b, client = _connected(mocker)
    b.push("queue1", b"a")
    b.push("queue1", b"b")
    client.create_producer.assert_called_once_with("scrapy-queue1")

  def test_push_ignores_priority(self, mocker) -> None:
    producer = mocker.MagicMock()
    b, _ = _connected(mocker, create_producer=producer)
    b.push("queue1", b"x", priority=99.0)
    producer.send.assert_called_once_with(b"x")

  def test_push_failure_raises_queue_error(self, mocker) -> None:
    producer = mocker.MagicMock()
    producer.send.side_effect = RuntimeError("send failed")
    b, _ = _connected(mocker, create_producer=producer)
    with pytest.raises(QueueError):
      b.push("queue1", b"x")

  def test_push_invalid_name_raises(self, mocker) -> None:
    b, _ = _connected(mocker)
    with pytest.raises(ValueError):
      b.push("bad name!", b"x")


class TestPulsarPop:
  def _msg(self, mocker, payload=b"hello"):
    msg = mocker.MagicMock()
    msg.data.return_value = payload
    return msg

  def test_pop_subscribes_and_returns_bytes(self, mocker) -> None:
    msg = self._msg(mocker, b"hello")
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, client = _connected(mocker, subscribe=consumer)
    assert b.pop("queue1", timeout=1.0) == b"hello"
    client.subscribe.assert_called_once()
    assert b._last_msg is msg

  def test_pop_returns_none_on_empty(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError("timed out")
    b, _ = _connected(mocker, subscribe=consumer)
    assert b.pop("queue1") is None
    assert b._last_msg is None

  def test_pop_reuses_consumer_for_same_topic(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError("none")
    b, client = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.pop("queue1")
    client.subscribe.assert_called_once()

  def test_pop_resubscribes_on_topic_change(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError("none")
    b, client = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.pop("queue2")
    assert client.subscribe.call_count == 2


class TestPulsarAckNack:
  def test_ack_calls_acknowledge(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.ack("queue1")
    consumer.acknowledge.assert_called_once_with(msg)
    assert b._last_msg is None

  def test_ack_noop_without_message(self, mocker) -> None:
    b, _ = _connected(mocker)
    b.ack("queue1")  # no prior pop -> no error, no call

  def test_nack_calls_negative_acknowledge(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.nack("queue1")
    consumer.negative_acknowledge.assert_called_once_with(msg)
    assert b._last_msg is None


class TestPulsarLenClear:
  def test_queue_len_returns_zero(self, mocker) -> None:
    b, _ = _connected(mocker)
    assert b.queue_len("queue1") == 0

  def test_clear_queue_drops_cached_handles(self, mocker) -> None:
    producer = mocker.MagicMock()
    b, _ = _connected(mocker, create_producer=producer)
    b.push("queue1", b"x")
    b.clear_queue("queue1")
    producer.close.assert_called_once()
    assert "scrapy-queue1" not in b._producers
