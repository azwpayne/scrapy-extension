"""Remaining coverage gaps: subsystem-③ backends — invalid-mode paths + Pulsar edges."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

# Canonical mocks for all backend deps.
sys.modules.setdefault("pulsar", MagicMock())
sys.modules.setdefault("boto3", MagicMock())
if "pymemcache" not in sys.modules:
  _pkg = types.ModuleType("pymemcache")
  _client_mod = types.ModuleType("pymemcache.client")
  _base = types.ModuleType("pymemcache.client.base")
  _base.Client = MagicMock(name="MemcachedClient")
  sys.modules["pymemcache"] = _pkg
  sys.modules["pymemcache.client"] = _client_mod
  sys.modules["pymemcache.client.base"] = _base

import pulsar  # noqa: E402
import pytest  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _cleanup_sys_modules_mocks():
  """Pop the module-level mocks after this module's tests finish.

  R14-G flake fix: this module injects ``pulsar``, ``boto3``, and the
  ``pymemcache`` mock tree at module top-level (runs at collection, persists
  for the whole session). Popping all injected keys at module teardown
  restores a clean ``sys.modules`` for subsequent modules.
  """
  yield
  for key in (
    "pulsar",
    "boto3",
    "pymemcache",
    "pymemcache.client",
    "pymemcache.client.base",
  ):
    sys.modules.pop(key, None)

from scrapy_extension.backends.dynamodb import DynamoDBBackend  # noqa: E402
from scrapy_extension.backends.memcached import MemcachedBackend  # noqa: E402
from scrapy_extension.backends.pulsar import PulsarBackend  # noqa: E402
from scrapy_extension.backends.sqs import SqsBackend  # noqa: E402
from scrapy_extension.exceptions import (  # noqa: E402
  ConfigurationError,
  QueueError,
)
from scrapy_extension.settings import (  # noqa: E402
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

  def test_sqs_invalid_mode(self) -> None:
    b = SqsBackend(SqsSettings())
    b.config.mode = "bogus"  # type: ignore[assignment]
    with pytest.raises(ConfigurationError):
      b.connect()


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
