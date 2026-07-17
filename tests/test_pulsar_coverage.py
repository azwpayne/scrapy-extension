"""Error-path coverage for PulsarBackend (≥98% coverage goal)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

sys.modules.setdefault("pulsar", MagicMock())
import pulsar  # noqa: E402 — the mocked module actually in sys.modules
import pytest  # noqa: E402


class _PulsarTimeout(Exception):
  """Test double matching ``pulsar.Timeout`` from the C++ client binding."""


pulsar.Timeout = _PulsarTimeout


@pytest.fixture(scope="module", autouse=True)
def _cleanup_sys_modules_mock_pulsar():
  """Pop the module-level ``pulsar`` mock after this module's tests finish.

  R14-G flake fix: module-top-level ``sys.modules.setdefault`` pollutes the
  session for later modules; pop at module teardown.
  """
  yield
  sys.modules.pop("pulsar", None)

from scrapy_extension.backends.pulsar import (  # noqa: E402
  PulsarBackend,
  _consumer_type,
  _initial_position,
  _message_bytes,
)
from scrapy_extension.exceptions import ConfigurationError, QueueError  # noqa: E402
from scrapy_extension.settings import PulsarSettings  # noqa: E402


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
    consumer.acknowledge.side_effect = RuntimeError("ack fail")
    client.subscribe.return_value = consumer
    b.pop("q")
    with pytest.raises(QueueError):
      b.ack("q")

  def test_nack_swallows_failure(self, mocker) -> None:
    b, client = _connected(mocker)
    consumer = mocker.MagicMock()
    consumer.receive.return_value = _msg(mocker)
    consumer.negative_acknowledge.side_effect = RuntimeError("nack fail")
    client.subscribe.return_value = consumer
    b.pop("q")
    b.nack("q")  # must not raise

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
