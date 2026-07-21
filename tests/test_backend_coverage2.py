"""Remaining coverage gaps: subsystem-③ backends — invalid-mode paths + Pulsar edges."""

from __future__ import annotations

import pulsar
import pytest

from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.backends.memcached import MemcachedBackend
from scrapy_extension.backends.pulsar import PulsarBackend
from scrapy_extension.backends.sqs import SqsBackend
from scrapy_extension.exceptions import (
  ConfigurationError,
  QueueError,
)
from scrapy_extension.settings import (
  DynamoDBSettings,
  MemcachedSettings,
  PulsarSettings,
  SqsSettings,
)


class TestInvalidModeGuards:
  def test_pulsar_invalid_mode(self) -> None:
    b = PulsarBackend(PulsarSettings())
    b.config.mode = "bogus"  # type: ignore[assignment]
    with pytest.raises(ConfigurationError):
      b.connect()

  def test_dynamodb_invalid_mode(self) -> None:
    b = DynamoDBBackend(DynamoDBSettings())
    b.config.mode = "bogus"  # type: ignore[assignment]
    with pytest.raises(ConfigurationError):
      b.connect()

  def test_memcached_invalid_mode(self) -> None:
    b = MemcachedBackend(MemcachedSettings())
    b.config.mode = "bogus"  # type: ignore[assignment]
    with pytest.raises(ConfigurationError):
      b.connect()

  def test_sqs_invalid_mode(self, mocker) -> None:
    b = SqsBackend(SqsSettings())
    b.config.mode = "bogus"  # type: ignore[assignment]
    session_factory = mocker.patch(
      "scrapy_extension.backends.sqs.boto3.session.Session",
      side_effect=AssertionError("validation must precede Session construction"),
    )
    default_client = mocker.patch(
      "scrapy_extension.backends.sqs.boto3.client",
      side_effect=AssertionError("shared default Session must not be used"),
    )
    with pytest.raises(ConfigurationError):
      b.connect()
    session_factory.assert_not_called()
    default_client.assert_not_called()


def _pulsar(mocker):
  b = PulsarBackend(PulsarSettings())
  client = mocker.MagicMock()
  mocker.patch.object(pulsar, "Client", return_value=client)
  b.connect()
  return b, client


class TestPulsarRemaining:
  def test_push_producer_creation_failure(self, mocker) -> None:
    b, client = _pulsar(mocker)
    client.create_producer.side_effect = RuntimeError("nope")
    with pytest.raises(QueueError):
      b.push("q", b"x")

  def test_pop_receive_returns_none(self, mocker) -> None:
    b, client = _pulsar(mocker)
    consumer = mocker.MagicMock()
    consumer.receive.return_value = None
    client.subscribe.return_value = consumer
    assert b.pop("q") is None

  def test_nack_without_message_is_noop(self, mocker) -> None:
    b, _ = _pulsar(mocker)
    b.nack("q")  # no consumer/msg -> early return

  def test_ack_without_message_is_noop(self, mocker) -> None:
    b, _ = _pulsar(mocker)
    b.ack("q")  # no consumer/msg -> early return

  def test_disconnect_closes_cached_producer(self, mocker) -> None:
    b, client = _pulsar(mocker)
    producer = mocker.MagicMock()
    client.create_producer.return_value = producer
    b.push("q", b"x")  # caches a producer
    b.disconnect()
    producer.close.assert_called_once()

  def test_queue_len_reports_unsupported(self, mocker) -> None:
    b, _ = _pulsar(mocker)
    with pytest.raises(NotImplementedError, match="admin API"):
      b.queue_len("q")
