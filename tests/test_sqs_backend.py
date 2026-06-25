"""Tests for SqsBackend (subsystem ③) — mocked boto3.

Injects a mock ``boto3`` into ``sys.modules`` and patches ``boto3.client``
(the module-attribute pattern) to assert call patterns.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("boto3", MagicMock())
import boto3  # noqa: E402 — the mocked module actually in sys.modules

from scrapy_extension.backends.base import (  # noqa: E402
  BackendType,
  QueueBackend,
  SetBackend,
)
from scrapy_extension.backends.sqs import SqsBackend  # noqa: E402
from scrapy_extension.exceptions import BackendConnectionError, QueueError  # noqa: E402
from scrapy_extension.settings import SqsMode, SqsSettings  # noqa: E402


def _make_backend(**overrides) -> SqsBackend:
  return SqsBackend(SqsSettings(**overrides))


def _connected(mocker, **client_children):
  b = _make_backend()
  client = mocker.MagicMock()
  client.get_queue_url.return_value = {"QueueUrl": "https://sqs/test"}
  for attr, val in client_children.items():
    getattr(client, attr).return_value = val
  mocker.patch.object(boto3, "client", return_value=client)
  b.connect()
  return b, client


class TestSqsBackendType:
  def test_backend_type_is_sqs(self) -> None:
    assert _make_backend().backend_type is BackendType.SQS

  def test_queue_only_no_set_storage(self) -> None:
    b = _make_backend()
    assert isinstance(b, QueueBackend)
    assert not isinstance(b, SetBackend)

  def test_settings_defaults(self) -> None:
    s = SqsSettings()
    assert s.mode is SqsMode.STANDALONE
    assert s.region_name == "us-east-1"
    assert s.queue_name_prefix == "scrapy-"


class TestSqsConnect:
  def test_connect_creates_client(self, mocker) -> None:
    b = _make_backend()
    client = mocker.MagicMock()
    mocker.patch.object(boto3, "client", return_value=client)
    b.connect()
    boto3.client.assert_called_once()
    args, kwargs = boto3.client.call_args
    assert args == ("sqs",)
    assert kwargs["region_name"] == "us-east-1"
    assert b.is_connected() is True

  def test_connect_failure_raises(self, mocker) -> None:
    b = _make_backend()
    mocker.patch.object(boto3, "client", side_effect=RuntimeError("boom"))
    with pytest.raises(BackendConnectionError):
      b.connect()

  def test_disconnect_closes_client(self, mocker) -> None:
    b, client = _connected(mocker)
    b.disconnect()
    client.close.assert_called_once()
    assert b.is_connected() is False


class TestSqsPushPop:
  def test_push_resolves_url_and_sends_b64(self, mocker) -> None:
    b, client = _connected(mocker)
    b.push("queue1", b"payload")
    client.get_queue_url.assert_called_once_with(QueueName="scrapy-queue1")
    args, kwargs = client.send_message.call_args
    assert kwargs["QueueUrl"] == "https://sqs/test"
    # MessageBody is base64 of the payload
    import base64

    assert base64.b64decode(kwargs["MessageBody"]) == b"payload"

  def test_push_caches_queue_url(self, mocker) -> None:
    b, client = _connected(mocker)
    b.push("queue1", b"a")
    b.push("queue1", b"b")
    client.get_queue_url.assert_called_once_with(QueueName="scrapy-queue1")

  def test_push_ignores_priority(self, mocker) -> None:
    b, _ = _connected(mocker)
    b.push("queue1", b"x", priority=99.0)
    # send_message has no priority arg
    assert b._client.send_message.call_args.kwargs.keys() >= {"QueueUrl", "MessageBody"}

  def test_push_failure_raises_queue_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.send_message.side_effect = RuntimeError("send failed")
    with pytest.raises(QueueError):
      b.push("queue1", b"x")

  def test_pop_returns_decoded_bytes(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [{"Body": base64.b64encode(b"hello").decode(), "ReceiptHandle": "rh"}]
    }
    assert b.pop("queue1") == b"hello"
    # _last_receipt is a (queue_url, receipt_handle) tuple (round-2 C3 fix).
    assert b._last_receipt == ("https://sqs/test", "rh")

  def test_pop_returns_none_when_empty(self, mocker) -> None:
    b, client = _connected(mocker)
    client.receive_message.return_value = {}
    assert b.pop("queue1") is None

  def test_pop_caps_wait_at_20(self, mocker) -> None:
    b, client = _connected(mocker)
    client.receive_message.return_value = {}
    b.pop("queue1", timeout=99.0)
    assert client.receive_message.call_args.kwargs["WaitTimeSeconds"] == 20


class TestSqsAckNack:
  def test_ack_deletes_message(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [{"Body": base64.b64encode(b"x").decode(), "ReceiptHandle": "rh"}]
    }
    b.pop("queue1")
    b.ack("queue1")
    client.delete_message.assert_called_once_with(
      QueueUrl="https://sqs/test", ReceiptHandle="rh"
    )
    assert b._last_receipt is None

  def test_ack_noop_without_message(self, mocker) -> None:
    b, client = _connected(mocker)
    b.ack("queue1")
    client.delete_message.assert_not_called()

  def test_nack_is_noop_and_clears_receipt(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [{"Body": base64.b64encode(b"x").decode(), "ReceiptHandle": "rh"}]
    }
    b.pop("queue1")
    b.nack("queue1")
    client.delete_message.assert_not_called()
    assert b._last_receipt is None


class TestSqsLenClear:
  def test_queue_len_reads_attributes(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get_queue_attributes.return_value = {
      "Attributes": {"ApproximateNumberOfMessages": "42"}
    }
    assert b.queue_len("queue1") == 42

  def test_queue_len_zero_on_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get_queue_attributes.side_effect = RuntimeError("oops")
    assert b.queue_len("queue1") == 0

  def test_clear_purges_queue(self, mocker) -> None:
    b, client = _connected(mocker)
    b.clear_queue("queue1")
    client.purge_queue.assert_called_once_with(QueueUrl="https://sqs/test")


def _connected_multi_queue(mocker, urls: dict[str, str]):
  """Connect a backend whose queue-URL cache resolves multiple queues.

  ``urls`` maps queue_name -> QueueUrl. ``get_queue_url`` is patched to
  return the right URL per call, mirroring real SQS resolution.
  """
  b = _make_backend()
  client = mocker.MagicMock()

  def _get_queue_url(*, QueueName):  # noqa: N803 — boto3 kwarg name
    for qname, url in urls.items():
      if QueueName.endswith(qname):
        return {"QueueUrl": url}
    raise RuntimeError(f"unexpected QueueName={QueueName!r}")

  client.get_queue_url.side_effect = _get_queue_url
  mocker.patch.object(boto3, "client", return_value=client)
  b.connect()
  return b, client


class TestSqsAckCorrectQueueUrl:
  """Unit A (A4 / C3 HIGH): ack deletes against the queue the msg was popped from.

  Pre-fix SQS ``ack`` resolved the QueueUrl via
  ``next(iter(self._queue_urls.values()))`` — the first-cached queue, NOT
  necessarily the source queue of the popped message. With >=2 queues, a
  message popped from queue B could be ``delete_message``d against queue A's
  URL, never actually deleting the message (silent redeliver) and possibly
  erroring on AWS. The fix tracks ``(queue_url, receipt_handle)`` per pop.

  This test is HYPOTHESIS-gated: it MUST fail pre-fix (C3 RED).
  """

  def test_ack_after_pop_from_queue_b_targets_queue_b_url(self, mocker) -> None:
    """pop from qB -> ack deletes with QueueUrl=qB_url, NOT qA_url."""
    import base64

    urls = {"qA": "https://sqs/qA-url", "qB": "https://sqs/qB-url"}
    b, client = _connected_multi_queue(mocker, urls)

    # Seed the URL cache in a known order by touching qA first, so the
    # pre-fix `next(iter(...))` resolves to qA (proving the bug is real,
    # not masked by dict ordering).
    b._queue_url("qA")
    b._queue_url("qB")
    # Sanity: the cache is populated and iteration order is qA, qB.
    assert list(b._queue_urls.values()) == [
      "https://sqs/qA-url",
      "https://sqs/qB-url",
    ]

    # Pop a message FROM qB with a distinct receipt handle.
    client.receive_message.return_value = {
      "Messages": [
        {
          "Body": base64.b64encode(b"from-b").decode(),
          "ReceiptHandle": "rh-from-b",
        }
      ]
    }
    popped = b.pop("qB")
    assert popped == b"from-b"

    # Ack it.
    b.ack("qB")

    # CRITICAL: delete_message MUST target qB's URL, not qA's.
    client.delete_message.assert_called_once_with(
      QueueUrl="https://sqs/qB-url", ReceiptHandle="rh-from-b"
    )

  def test_ack_records_correct_url_across_queues(self, mocker) -> None:
    """Round-trip pop+ack across two queues acks each against its own URL."""
    import base64

    urls = {"qA": "https://sqs/qA-url", "qB": "https://sqs/qB-url"}
    b, client = _connected_multi_queue(mocker, urls)

    # Pop from qA then qB (single-slot, so qA's receipt is overwritten —
    # that's the documented limitation; here we just check ack-after-pop
    # targets the LAST popped queue's URL).
    client.receive_message.side_effect = [
      {"Messages": [{"Body": base64.b64encode(b"a").decode(), "ReceiptHandle": "rh-a"}]},
      {"Messages": [{"Body": base64.b64encode(b"b").decode(), "ReceiptHandle": "rh-b"}]},
    ]
    b.pop("qA")
    b.pop("qB")
    b.ack("qB")  # acks the last-popped (qB)

    client.delete_message.assert_called_once_with(
      QueueUrl="https://sqs/qB-url", ReceiptHandle="rh-b"
    )
