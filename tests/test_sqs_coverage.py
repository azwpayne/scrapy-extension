"""Error-path coverage for SqsBackend (≥98% coverage goal)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

sys.modules.setdefault("boto3", MagicMock())
import boto3  # noqa: E402
import pytest  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _cleanup_sys_modules_mock_boto3():
  """Pop the module-level ``boto3`` mock after this module's tests finish.

  R14-G flake fix: module-top-level ``sys.modules.setdefault`` pollutes the
  session for later modules; pop at module teardown.
  """
  yield
  sys.modules.pop("boto3", None)

from scrapy_extension.backends.sqs import SqsBackend  # noqa: E402
from scrapy_extension.exceptions import QueueError  # noqa: E402
from scrapy_extension.settings import SqsSettings  # noqa: E402


def _connected(mocker, **overrides):
  b = SqsBackend(SqsSettings(**overrides))
  client = mocker.MagicMock()
  client.get_queue_url.return_value = {"QueueUrl": "https://sqs/test"}
  mocker.patch.object(boto3, "client", return_value=client)
  b.connect()
  return b, client


class TestSqsErrorPaths:
  def test_connect_with_credentials(self, mocker) -> None:
    b, _ = _connected(mocker, aws_access_key_id="k", aws_secret_access_key="s")
    assert b.is_connected()

  def test_queue_url_create_fallback(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get_queue_url.side_effect = RuntimeError("missing")
    client.create_queue.return_value = {"QueueUrl": "https://sqs/new"}
    # _queue_url falls back to create_queue on get failure
    b.push("qnew", b"x")
    client.create_queue.assert_called_once_with(QueueName="scrapy-qnew")

  def test_queue_url_total_failure_raises(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get_queue_url.side_effect = RuntimeError("missing")
    client.create_queue.side_effect = RuntimeError("create fail")
    with pytest.raises(QueueError):
      b.push("qbad", b"x")

  def test_pop_failure_raises_queue_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.receive_message.side_effect = RuntimeError("recv fail")
    with pytest.raises(QueueError):
      b.pop("q")

  def test_ack_failure_raises_queue_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [{"Body": "eA==", "ReceiptHandle": "rh"}]
    }
    client.delete_message.side_effect = RuntimeError("del fail")
    b.pop("q")
    with pytest.raises(QueueError):
      b.ack("q")

  def test_clear_swallows_failure(self, mocker) -> None:
    b, client = _connected(mocker)
    client.purge_queue.side_effect = RuntimeError("purge fail")
    b.clear_queue("q")  # must not raise

  def test_disconnect_and_ping(self, mocker) -> None:
    b, _ = _connected(mocker)
    assert b.ping() is True
    b.disconnect()
    assert b.is_connected() is False

  def test_push_invalid_name_raises(self, mocker) -> None:
    b, _ = _connected(mocker)
    with pytest.raises(ValueError):
      b.push("bad name!", b"x")
