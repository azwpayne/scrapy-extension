"""Resilience / contract tests for KafkaBackend (initiative #29).

kafka.py was 90.51% (24 uncovered lines + 11 partial branches) — the
largest coverage gap and below the 95% floor. This pins the clearest
contract clusters (validation, ack-token semantics, TOCTOU None-guards,
the watermark-commit error path, queue_len's temp-consumer fallback).
Each test maps to a documented contract, not a line-hit.
"""

from __future__ import annotations

from collections import defaultdict

import pytest
from kafka.errors import KafkaError

from scrapy_extension.backends.kafka import (
  KafkaBackend,
  _KafkaAckToken,
  _validate_topic_name,
)
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings import KafkaMode, KafkaSettings


def _backend() -> KafkaBackend:
  """Constructed-but-not-connected backend (clients are None)."""
  return KafkaBackend(KafkaSettings())


# ---------------------------------------------------------------------------
# _validate_topic_name (line 59)
# ---------------------------------------------------------------------------


def test_validate_topic_name_raises_on_invalid_name() -> None:
  """Line 59: a name with characters outside [A-Za-z0-9._-] raises
  ValueError — the input-validation contract at the system boundary
  (prevents malformed topic names reaching the broker)."""
  with pytest.raises(ValueError, match="Invalid topic/queue name"):
    _validate_topic_name("bad name with spaces!")
  # Sanity: a valid name passes.
  _validate_topic_name("valid.topic-name_1")


# ---------------------------------------------------------------------------
# _KafkaAckToken semantics (lines 102-104, 111)
# ---------------------------------------------------------------------------


def test_ack_token_eq_not_implemented_for_other_types() -> None:
  """Lines 102-103: __eq__ vs a non-_KafkaAckToken returns NotImplemented
  (Python's __eq__ protocol — dict/set membership for tokens survives a
  refactor that would silently return False)."""
  token = _KafkaAckToken(partition=0, offset=1, topic="t")
  assert token.__eq__("not-a-token") is NotImplemented


def test_ack_token_eq_true_for_equal_tokens_and_hash_stable() -> None:
  """Line 104 + 111: two tokens with identical (partition, offset, topic)
  compare equal AND hash equal — the contract that lets the watermark
  algorithm's in-flight set correctly dedup acks."""
  a = _KafkaAckToken(partition=0, offset=1, topic="t")
  b = _KafkaAckToken(partition=0, offset=1, topic="t")
  assert a == b
  assert hash(a) == hash(b)
  assert _KafkaAckToken(partition=0, offset=2, topic="t") != a


# ---------------------------------------------------------------------------
# TOCTOU None-guards (lines 454-455 push, 570-571 _poll_record, 657
# _ack_token, 804-805 clear_queue)
# ---------------------------------------------------------------------------


def test_push_raises_when_producer_becomes_none(mocker) -> None:
  """Lines 454-455: ``is_connected()`` passed but the producer became None
  before ``send()`` (concurrent disconnect) → BackendConnectionError
  rather than ``AttributeError`` on ``None.send()``. ``_ensure_topic_exists``
  is stubbed so the topic-check (which would itself raise on a None admin
  client) doesn't preempt the producer-None guard under test."""
  backend = _backend()
  backend._producer = None
  mocker.patch.object(backend, "is_connected", return_value=True)
  mocker.patch.object(backend, "_ensure_topic_exists")  # don't let topic-check preempt
  with pytest.raises(BackendConnectionError, match="producer is None"):
    backend.push("q", b"x")


def test_ensure_topic_exists_raises_when_admin_client_is_none() -> None:
  """Lines 424-425: ``_ensure_topic_exists`` with no admin client (never
  connected / concurrent disconnect) → BackendConnectionError rather than
  ``None.create_topics()`` — distinct from clear_queue's own guard (804-805)
  but the same TOCTOU shape on the admin client."""
  backend = _backend()
  backend._admin_client = None
  with pytest.raises(BackendConnectionError, match="admin client is None"):
    backend._ensure_topic_exists("q")


def test_poll_record_raises_when_consumer_construction_returns_none(mocker) -> None:
  """Lines 570-571: ``KafkaConsumer(...)`` returning None (client-lib
  contract violation) fails fast with BackendConnectionError rather than
  dispatching ``.subscribe()`` / ``.poll()`` on None."""
  backend = _backend()
  backend._consumer = None
  mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer", return_value=None)
  with pytest.raises(BackendConnectionError, match="consumer is None"):
    backend._poll_record("q", 0.0)


def test_ack_token_is_noop_when_consumer_is_none() -> None:
  """Line 657: ``_ack_token`` with no consumer silently returns — ack is
  best-effort, and a vanished consumer (post-disconnect) must not crash
  the caller (the message re-delivers via the broker's at-least-once)."""
  backend = _backend()
  backend._consumer = None
  backend._ack_token(_KafkaAckToken(partition=0, offset=1, topic="t"))  # must not raise


def test_clear_queue_raises_when_admin_client_is_none() -> None:
  """Lines 804-805: clear_queue with no admin client (never connected /
  concurrent disconnect) → BackendConnectionError rather than
  ``None.delete_topics()``."""
  backend = _backend()
  backend._admin_client = None
  with pytest.raises(BackendConnectionError, match="admin client is None"):
    backend.clear_queue("q")


# ---------------------------------------------------------------------------
# _ack_token error paths (lines 668-670 position, 685-687 commit)
# ---------------------------------------------------------------------------


def test_ack_token_raises_when_consumer_position_fails(mocker) -> None:
  """Lines 668-670: ``_consumer.position()`` raising KafkaError surfaces as
  a QueueError — the watermark base can't be seeded, so ack fails loudly
  (the offset stays uncommitted → at-least-once re-delivery on restart)."""
  backend = _backend()
  backend._consumer = mocker.MagicMock()
  backend._consumer.position.side_effect = KafkaError("position boom")
  backend._in_flight = defaultdict(set)
  backend._watermarks = {}  # unseeded -> position() will be called
  with pytest.raises(QueueError, match="Failed to read consumer position"):
    backend._ack_token(_KafkaAckToken(partition=0, offset=1, topic="t"))


def test_ack_token_raises_when_commit_fails(mocker) -> None:
  """Lines 685-687: ``_consumer.commit()`` raising KafkaError (after the
  watermark advanced past the base) surfaces as a QueueError — the
  contiguous-run commit failed, so the caller sees the failure rather than
  a silent data loss. Setup: pre-seed the watermark base + high-water so
  the walk advances and triggers commit."""
  backend = _backend()
  backend._consumer = mocker.MagicMock()
  backend._consumer.commit.side_effect = KafkaError("commit boom")
  backend._in_flight = defaultdict(set)
  # Pre-seeded base (0) so position() is NOT called; high-water (5) lets the
  # watermark walk advance past the base -> commit path taken.
  backend._watermarks = {0: 0}
  backend._high_water = {0: 5}
  with pytest.raises(QueueError, match="Failed to ack Kafka message"):
    backend._ack_token(_KafkaAckToken(partition=0, offset=1, topic="t"))


# ---------------------------------------------------------------------------
# nack-with-token re-adds the offset (line 732->734)
# ---------------------------------------------------------------------------


def test_nack_with_token_re_adds_offset_to_in_flight() -> None:
  """Line 732 (true branch): nack(token=_KafkaAckToken) re-adds the offset
  to the partition's in-flight set so the watermark never advances past it →
  uncommitted → re-delivered on consumer restart (at-least-once)."""
  backend = _backend()
  backend._in_flight = defaultdict(set)
  token = _KafkaAckToken(partition=0, offset=7, topic="t")
  backend.nack("q", token=token)
  assert 7 in backend._in_flight[0]


def test_nack_with_non_kafka_token_is_a_silent_noop() -> None:
  """Line 732->734 (false branch): nack(token=<non-_KafkaAckToken>) skips
  the in-flight re-add (can't partition/offset a foreign token type) and
  returns — defensive against a caller passing a legacy/external token
  shape, must not raise."""
  backend = _backend()
  backend._in_flight = defaultdict(set)
  backend.nack("q", token="some-legacy-opaque-token")  # must not raise
  assert backend._in_flight[0] == set()  # nothing added


# ---------------------------------------------------------------------------
# queue_len temp-consumer fallback (lines 777, 785-786)
# ---------------------------------------------------------------------------


def test_queue_len_returns_zero_when_topic_has_no_partitions(mocker) -> None:
  """Line 777: with no connected consumer, queue_len builds a temp consumer;
  if the topic has no partitions (doesn't exist) → 0 (eventual-consistency
  monitoring signal, not an error)."""
  backend = _backend()
  backend._consumer = None
  mock_temp = mocker.MagicMock()
  mock_temp.partitions_for_topic.return_value = None  # topic missing
  mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer", return_value=mock_temp)
  assert backend.queue_len("q") == 0


def test_queue_len_returns_zero_on_temp_consumer_kafka_error(mocker) -> None:
  """Lines 785-786: a KafkaError during the temp-consumer depth query
  (end_offsets / position) → 0 (eventual-consistency monitoring must not
  crash the caller; the broker error is surfaced elsewhere)."""
  backend = _backend()
  backend._consumer = None
  mock_temp = mocker.MagicMock()
  mock_temp.partitions_for_topic.return_value = {0}  # topic exists
  mock_temp.end_offsets.side_effect = KafkaError("end_offsets boom")
  mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer", return_value=mock_temp)
  assert backend.queue_len("q") == 0


# ---------------------------------------------------------------------------
# Niche edge cases (initiative #33 — completing kafka to 100%)
# ---------------------------------------------------------------------------


def test_build_client_security_config_confluent_without_keys_falls_through(mocker) -> None:
  """Line 268->280: CONFLUENT mode WITHOUT both confluent_api_key +
  confluent_api_secret falls through the SASL_SSL branch to the
  common-config subset (a half-configured CONFLUENT backend must not
  hand the consumer a partial SASL dict)."""
  backend = _backend()
  backend.config.mode = KafkaMode.CONFLUENT  # confluent keys stay None (default)
  mocker.patch.object(
    backend, "_build_common_config", return_value={"security_protocol": "PLAINTEXT"}
  )
  result = backend._build_client_security_config()
  # Fell through to the common-config subset, NOT the SASL_SSL dict:
  assert result == {"security_protocol": "PLAINTEXT"}
  assert "sasl_plain_password" not in result


def test_pop_with_ack_returns_none_tuple_when_queue_empty(mocker) -> None:
  """Line 517: pop_with_ack on an empty queue (``_poll_record`` returned
  None) returns ``(None, None)`` — the empty-queue sentinel before any
  token / in-flight bookkeeping."""
  backend = _backend()
  mocker.patch.object(backend, "_poll_record", return_value=None)
  assert backend.pop_with_ack("q", 0.0) == (None, None)


def test_poll_record_returns_none_when_poll_yields_empty_records(mocker) -> None:
  """Line 584->583: when ``consumer.poll`` returns a non-empty messages
  dict whose record list IS empty (a TP key with no records — common
  during long-poll idle), the inner loop doesn't iterate and _poll_record
  falls through to return None rather than mishandling the empty batch."""
  backend = _backend()
  backend._consumer = mocker.MagicMock()
  # Non-empty dict (one TopicPartition) but an EMPTY record list:
  backend._consumer.poll.return_value = {mocker.MagicMock(name="tp"): []}
  assert backend._poll_record("q", 0.0) is None
