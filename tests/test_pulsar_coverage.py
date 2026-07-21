"""Error-path coverage for PulsarBackend (≥98% coverage goal)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pulsar
import pytest

from scrapy_extension.backends.pulsar import (
  PulsarBackend,
  _consumer_type,
  _initial_position,
  _message_bytes,
)
from scrapy_extension.exceptions import ConfigurationError, QueueError
from scrapy_extension.settings import PulsarSettings


def _connected(mocker, **overrides):
  b = PulsarBackend(PulsarSettings(**overrides))
  client = mocker.MagicMock()
  mocker.patch.object(pulsar, "Client", return_value=client)
  b.connect()
  return b, client


def _msg(mocker, payload=b"x"):
  msg = mocker.MagicMock()
  msg.data.return_value = payload
  return msg


class TestPulsarHelpers:
  def test_invalid_consumer_type_raises(self) -> None:
    with pytest.raises(ConfigurationError):
      _consumer_type("Bogus")

  def test_invalid_initial_position_raises(self) -> None:
    with pytest.raises(ConfigurationError):
      _initial_position("Bogus")

  def test_message_bytes_value_fallback(self) -> None:
    msg = MagicMock(spec=["value"])
    msg.value.return_value = b"v"
    assert _message_bytes(msg) == b"v"

  def test_message_bytes_str_fallback(self) -> None:
    msg = MagicMock(spec=[])
    assert isinstance(_message_bytes(msg), bytes)


class TestPulsarErrorPaths:
  def test_connect_with_tls_certs(self, mocker) -> None:
    b, _ = _connected(
      mocker, tls_trust_certs_file="/tmp/ca.pem", allow_insecure_connection=True
    )
    assert b.is_connected()

  def test_ping_true_when_connected(self, mocker) -> None:
    b, _ = _connected(mocker)
    assert b.ping() is True

  def test_ack_failure_raises_queue_error(self, mocker) -> None:
    b, client = _connected(mocker)
    consumer = mocker.MagicMock()
    consumer.receive.return_value = _msg(mocker)
    consumer.acknowledge.side_effect = [RuntimeError("ack fail"), None]
    client.subscribe.return_value = consumer
    b.pop("q")
    with pytest.raises(QueueError) as exc_info:
      b.ack("q")
    assert exc_info.value.operation == "ack"
    assert b._last_msg is not None
    b.ack("q")
    assert consumer.acknowledge.call_count == 2
    assert b._last_msg is None

  def test_nack_failure_raises_queue_error_and_is_retryable(self, mocker) -> None:
    b, client = _connected(mocker)
    consumer = mocker.MagicMock()
    consumer.receive.return_value = _msg(mocker)
    consumer.negative_acknowledge.side_effect = [RuntimeError("nack fail"), None]
    client.subscribe.return_value = consumer
    b.pop("q")
    with pytest.raises(QueueError) as exc_info:
      b.nack("q")
    assert exc_info.value.operation == "nack"
    assert b._last_msg is not None
    b.nack("q")
    assert consumer.negative_acknowledge.call_count == 2
    assert b._last_msg is None

  def test_disconnect_suppresses_close_errors(self, mocker) -> None:
    b, client = _connected(mocker)
    consumer = mocker.MagicMock()
    consumer.receive.return_value = _msg(mocker)
    consumer.close.side_effect = RuntimeError("close fail")
    client.subscribe.return_value = consumer
    b.pop("q")
    b.disconnect()  # _suppress catches consumer.close failure

  def test_clear_queue_reports_unsupported(self, mocker) -> None:
    b, client = _connected(mocker)
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = pulsar.Timeout("none")
    client.subscribe.return_value = consumer
    b.pop("q")  # creates consumer for scrapy-q
    with pytest.raises(QueueError, match="not supported"):
      b.clear_queue("q")
