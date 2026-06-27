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
from scrapy_extension.backends.sqs import SqsBackend, _SqsAckToken  # noqa: E402
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


class TestSqsAckToken:
  """The internal ``_SqsAckToken`` carries both per-message ReceiptHandle and
  the queue URL it was popped from (preserving the round-2 C3 multi-queue
  correctness). It is the SQS analog of Kafka's ``_KafkaAckToken``."""

  def test_token_is_hashable_and_equality_compares_fields(self) -> None:
    t1 = _SqsAckToken(queue_url="https://sqs/qA", receipt_handle="rh-1")
    t2 = _SqsAckToken(queue_url="https://sqs/qA", receipt_handle="rh-1")
    t3 = _SqsAckToken(queue_url="https://sqs/qB", receipt_handle="rh-1")
    t4 = _SqsAckToken(queue_url="https://sqs/qA", receipt_handle="rh-2")
    assert t1 == t2
    assert t1 != t3  # different queue_url
    assert t1 != t4  # different receipt_handle
    assert hash(t1) == hash(t2)
    assert {t1, t2, t3, t4} == {t1, t3, t4}  # dedup via __hash__

  def test_token_repr_is_useful(self) -> None:
    t = _SqsAckToken(queue_url="https://sqs/qA", receipt_handle="rh-1")
    r = repr(t)
    assert "https://sqs/qA" in r
    assert "rh-1" in r

  def test_token_uses_slots(self) -> None:
    t = _SqsAckToken(queue_url="u", receipt_handle="r")
    with pytest.raises(AttributeError):
      t.something_else = 1  # type: ignore[attr-defined]  # noqa: PGH003


class TestSqsPopWithAck:
  def test_pop_with_ack_returns_body_and_token(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [
        {"Body": base64.b64encode(b"payload").decode(), "ReceiptHandle": "rh-1"}
      ]
    }
    body, token = b.pop_with_ack("queue1")
    assert body == b"payload"
    assert isinstance(token, _SqsAckToken)
    assert token.queue_url == "https://sqs/test"
    assert token.receipt_handle == "rh-1"
    # Token tracked in the diagnostic in-flight set.
    assert token in b._in_flight

  def test_pop_with_ack_empty_returns_none_none(self, mocker) -> None:
    b, client = _connected(mocker)
    client.receive_message.return_value = {}
    body, token = b.pop_with_ack("queue1")
    assert body is None
    assert token is None
    assert b._in_flight == set()

  def test_pop_with_ack_failure_raises_queue_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.receive_message.side_effect = RuntimeError("receive failed")
    with pytest.raises(QueueError):
      b.pop_with_ack("queue1")


class TestSqsRealInFlightAck:
  """The core at-least-once-under-concurrency test: N pops before any ack
  MUST be able to ack each by its OWN token. Pre-fix (single-slot
  ``_last_receipt``) this is impossible — the first two acks no-op or hit
  the wrong handle."""

  def test_three_pops_then_three_distinct_acks(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    # Three sequential pops, each returning a distinct ReceiptHandle.
    # (SQS receive_message returns one message per call.)
    client.receive_message.side_effect = [
      {"Messages": [{"Body": base64.b64encode(b"m1").decode(), "ReceiptHandle": "rh-1"}]},
      {"Messages": [{"Body": base64.b64encode(b"m2").decode(), "ReceiptHandle": "rh-2"}]},
      {"Messages": [{"Body": base64.b64encode(b"m3").decode(), "ReceiptHandle": "rh-3"}]},
    ]

    body1, tok1 = b.pop_with_ack("q")
    body2, tok2 = b.pop_with_ack("q")
    body3, tok3 = b.pop_with_ack("q")
    assert (body1, body2, body3) == (b"m1", b"m2", b"m3")
    # 3 distinct tokens — the single-slot pre-fix path cannot produce these.
    assert len({tok1, tok2, tok3}) == 3
    assert len(b._in_flight) == 3

    # Ack each by its OWN token — 3 DISTINCT delete_message calls, each with
    # the correct ReceiptHandle for that token.
    b.ack("q", token=tok1)
    b.ack("q", token=tok2)
    b.ack("q", token=tok3)

    calls = client.delete_message.call_args_list
    assert len(calls) == 3
    # Each call targets the CORRECT (QueueUrl, ReceiptHandle) for that token.
    assert calls[0].kwargs == {"QueueUrl": "https://sqs/test", "ReceiptHandle": "rh-1"}
    assert calls[1].kwargs == {"QueueUrl": "https://sqs/test", "ReceiptHandle": "rh-2"}
    assert calls[2].kwargs == {"QueueUrl": "https://sqs/test", "ReceiptHandle": "rh-3"}
    # In-flight set fully drains after all acks.
    assert b._in_flight == set()

  def test_ack_by_token_is_order_independent(self, mocker) -> None:
    """Ack in reverse-pop-order to prove it isn't single-slot last-wins."""
    import base64

    b, client = _connected(mocker)
    client.receive_message.side_effect = [
      {"Messages": [{"Body": base64.b64encode(b"a").decode(), "ReceiptHandle": "rh-a"}]},
      {"Messages": [{"Body": base64.b64encode(b"b").decode(), "ReceiptHandle": "rh-b"}]},
    ]
    _, tok_a = b.pop_with_ack("q")
    _, tok_b = b.pop_with_ack("q")
    # Ack B FIRST (out of pop order) — single-slot pre-fix would have lost A.
    b.ack("q", token=tok_b)
    b.ack("q", token=tok_a)
    calls = client.delete_message.call_args_list
    assert {c.kwargs["ReceiptHandle"] for c in calls} == {"rh-a", "rh-b"}

  def test_ack_token_failure_raises_queue_error(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [{"Body": base64.b64encode(b"x").decode(), "ReceiptHandle": "rh"}]
    }
    _, tok = b.pop_with_ack("q")
    client.delete_message.side_effect = RuntimeError("delete failed")
    with pytest.raises(QueueError):
      b.ack("q", token=tok)


class TestSqsRealNack:
  def test_nack_with_token_is_noop_and_discards_from_in_flight(
    self, mocker
  ) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [{"Body": base64.b64encode(b"x").decode(), "ReceiptHandle": "rh-1"}]
    }
    _, tok = b.pop_with_ack("q")
    assert tok in b._in_flight
    b.nack("q", token=tok)
    # No delete_message call — SQS re-delivers on visibility timeout.
    client.delete_message.assert_not_called()
    # Token removed from the in-flight set.
    assert tok not in b._in_flight


class TestSqsCrashMidAck:
  """Crash-mid-ack semantics: popped-but-unacked messages stay in
  ``_in_flight`` so the leak is observable. SQS re-delivers them on
  visibility-timeout expiry — at-least-once is preserved by SQS itself."""

  def test_pop_without_ack_keeps_tokens_in_flight(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.side_effect = [
      {"Messages": [{"Body": base64.b64encode(b"m1").decode(), "ReceiptHandle": "rh-1"}]},
      {"Messages": [{"Body": base64.b64encode(b"m2").decode(), "ReceiptHandle": "rh-2"}]},
    ]
    _, tok1 = b.pop_with_ack("q")
    _, tok2 = b.pop_with_ack("q")
    # Crash mid-batch — ack NEITHER.
    assert len(b._in_flight) == 2
    assert {tok1, tok2} == b._in_flight
    # No delete_message fired — SQS will re-deliver both after their
    # visibility timeouts expire (at-least-once).
    client.delete_message.assert_not_called()


class TestSqsMultiQueueRealAck:
  """C3 multi-queue correctness preserved under the real-ack path: a token
  popped from qB acks against qB's QueueUrl, never qA's."""

  def test_multi_queue_pop_from_qB_ack_targets_qB(self, mocker) -> None:
    import base64

    urls = {"qA": "https://sqs/qA-url", "qB": "https://sqs/qB-url"}
    b, client = _connected_multi_queue(mocker, urls)
    # Seed the URL cache with qA first so any dict-iteration bug surfaces.
    b._queue_url("qA")
    b._queue_url("qB")

    client.receive_message.return_value = {
      "Messages": [
        {
          "Body": base64.b64encode(b"from-b").decode(),
          "ReceiptHandle": "rh-from-b",
        }
      ]
    }
    _, tok = b.pop_with_ack("qB")
    assert tok.queue_url == "https://sqs/qB-url"
    b.ack("qB", token=tok)
    client.delete_message.assert_called_once_with(
      QueueUrl="https://sqs/qB-url", ReceiptHandle="rh-from-b"
    )


class TestSqsSupportsConcurrentAck:
  def test_supports_concurrent_ack_is_true(self) -> None:
    # The round-2 gate reads this flag; flipping it True retires the gate
    # for SQS (real per-message ack is concurrency-safe).
    assert SqsBackend.supports_concurrent_ack is True
    assert SqsBackend.requires_ack is True


class TestSqsLegacyAckCompat:
  """The ``pop()`` + ``ack(token=None)`` legacy path still works via the
  ``_last_receipt`` single-slot — mirrors Kafka keeping ``_last_record``."""

  def test_legacy_pop_then_ack_no_token(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [{"Body": base64.b64encode(b"x").decode(), "ReceiptHandle": "rh"}]
    }
    assert b.pop("queue1") == b"x"
    # ack with no token — falls back to _last_receipt.
    b.ack("queue1")
    client.delete_message.assert_called_once_with(
      QueueUrl="https://sqs/test", ReceiptHandle="rh"
    )
    assert b._last_receipt is None

  def test_legacy_nack_then_clears_receipt(self, mocker) -> None:
    import base64

    b, client = _connected(mocker)
    client.receive_message.return_value = {
      "Messages": [{"Body": base64.b64encode(b"x").decode(), "ReceiptHandle": "rh"}]
    }
    b.pop("queue1")
    b.nack("queue1")  # no token
    client.delete_message.assert_not_called()
    assert b._last_receipt is None


# ---------------------------------------------------------------------------
# SEC-1 (round-6): SQS AWS creds redaction in boto3.client kwargs.
# SEC-7: AWS credentials must be both-or-neither (XOR validation).
# ---------------------------------------------------------------------------


def test_sqs_credentials_redacted_in_client_kwargs(mocker):
  """SEC-1: aws_access_key_id / aws_secret_access_key handed to boto3.client
  are wrapped in _RedactedStr so ``repr(call_args)`` doesn't leak them. The
  str values are preserved so boto3 still authenticates.
  """
  from scrapy_extension.backends._redaction import _RedactedStr
  from scrapy_extension.settings import SqsSettings

  config = SqsSettings(
    aws_access_key_id="AKIAEXAMPLEKEY",
    aws_secret_access_key="top-secret-sqs-secret",
  )
  backend = SqsBackend(config)

  captured = {}
  mocker.patch.object(
    boto3,
    "client",
    side_effect=lambda service, **kw: captured.update(kw) or mocker.MagicMock(),
  )
  backend.connect()
  key = captured["aws_access_key_id"]
  secret = captured["aws_secret_access_key"]
  # Values preserved for boto3 auth.
  assert str(key) == "AKIAEXAMPLEKEY"
  assert str(secret) == "top-secret-sqs-secret"
  # But repr of the captured kwargs hides both.
  assert "AKIAEXAMPLEKEY" not in repr(captured)
  assert "top-secret-sqs-secret" not in repr(captured)
  assert isinstance(key, _RedactedStr)
  assert isinstance(secret, _RedactedStr)


class TestSqsHalfCredentialGuard:
  """SEC-7: AWS credentials must be both-or-neither.

  Exactly one of (aws_access_key_id, aws_secret_access_key) set used to fall
  through silently to boto3's default credential chain — masking a
  misconfiguration. Now it raises ConfigurationError naming the missing half.
  """

  def test_key_without_secret_raises(self):
    from scrapy_extension.exceptions import ConfigurationError

    # SV3-6: half-cred guard now fires at config (SqsSettings construction),
    # ahead of the connect-path SEC-7 defense-in-depth guard.
    with pytest.raises(ConfigurationError) as exc_info:
      _make_backend(
        aws_access_key_id="AKIAEXAMPLEKEY",
        aws_secret_access_key=None,
      )
    assert "aws_secret_access_key" in str(exc_info.value)
    assert exc_info.value.setting_name == "aws_secret_access_key"

  def test_secret_without_key_raises(self):
    from scrapy_extension.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError) as exc_info:
      _make_backend(
        aws_access_key_id=None,
        aws_secret_access_key="orphan-secret",
      )
    assert "aws_access_key_id" in str(exc_info.value)
    assert exc_info.value.setting_name == "aws_access_key_id"

  def test_both_set_proceeds(self, mocker):
    """Both set → no ConfigurationError; boto3.client called with both."""
    backend = _make_backend(
      aws_access_key_id="AKIAEXAMPLEKEY",
      aws_secret_access_key="top-secret",
    )
    mocker.patch.object(boto3, "client", return_value=mocker.MagicMock())
    backend.connect()  # must not raise
    boto3.client.assert_called_once()

  def test_neither_set_proceeds(self, mocker):
    """Neither set → no ConfigurationError; boto3 default credential chain."""
    backend = _make_backend()  # defaults: both None
    mocker.patch.object(boto3, "client", return_value=mocker.MagicMock())
    backend.connect()  # must not raise
    _, kwargs = boto3.client.call_args.args, boto3.client.call_args.kwargs
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs


# ===========================================================================
# R14-E — Lifecycle bounds: SQS diagnostic in-flight set cap
# ===========================================================================


class TestSqsInFlightCap:
  """R14-E MED: the diagnostic ``_in_flight`` set is capped at ``_MAX_IN_FLIGHT``."""

  def test_pop_with_ack_caps_in_flight_set(self, mocker, caplog) -> None:
    """When the set is saturated, the pop still succeeds but the set stops growing."""
    import base64
    import logging

    from scrapy_extension.backends.sqs import _MAX_IN_FLIGHT

    body = b"hello-sqs"
    client = mocker.MagicMock()
    client.get_queue_url.return_value = {"QueueUrl": "https://sqs/test"}
    client.receive_message.return_value = {
      "Messages": [{"Body": base64.b64encode(body).decode("ascii"), "ReceiptHandle": "rh-new"}]
    }
    b, _ = _connected(mocker)
    b._client = client

    # Pre-saturate the set so the next pop trips the cap.
    b._in_flight = {
      _SqsAckToken(queue_url=f"https://sqs/t{i}", receipt_handle=f"rh{i}")
      for i in range(_MAX_IN_FLIGHT)
    }
    assert not b._in_flight_overflow_warned

    with caplog.at_level(logging.WARNING):
      value, token = b.pop_with_ack("queue1")

    # The pop succeeded — message returned, NOT dropped.
    assert value == body
    assert isinstance(token, _SqsAckToken)
    # The set stayed at the cap (the new token was not added).
    assert len(b._in_flight) == _MAX_IN_FLIGHT
    # The one-shot warning fired.
    assert b._in_flight_overflow_warned is True
    assert any("at cap" in r.message for r in caplog.records)
