"""Resilience / contract tests for PulsarBackend (initiative #32).

pulsar.py was the LAST module below the 95% floor (94.81%, 9 uncovered
lines + 6 partial branches). 8 tests pin the documented contracts:

- disconnect() client-None branch (line 280->284): idempotent when the
  client was never created / already torn down.
- _track_in_flight warn-once overflow (line 449->exit): bounded
  diagnostic set; the pop is never dropped, broker tracks message_ids.
- _ack_token TOCTOU (line 537) + acknowledge error (lines 540-541).
- nack legacy path when the consumer lacks negative_acknowledge
  (line 570->576 false branch): best-effort, message re-delivers on
  timeout/restart.
- _nack_token error logging (lines 585-586): best-effort, must not raise.
- _ensure_consumer subscribe failure (lines 648-649).
- _message_bytes non-bytes payload fallbacks (lines 673, 679): a producer
  whose data()/value() returns a non-bytes payload is str-encoded rather
  than crashing the pop path.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.pulsar import (
  _MAX_IN_FLIGHT,
  PulsarBackend,
  _message_bytes,
  _PulsarAckToken,
)
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import PulsarSettings


def _backend() -> PulsarBackend:
  """Constructed-but-not-connected backend (client / consumer are None)."""
  return PulsarBackend(PulsarSettings())


def test_suppress_pulsar_errors_does_not_suppress_base_exception() -> None:
  """R-swallow: _suppress_pulsar_errors must NOT suppress BaseException.

  Pre-fix ``__exit__`` returned True for any non-None ``exc_type``, so a
  KeyboardInterrupt raised inside a ``with _suppress_pulsar_errors():`` cleanup
  block was trapped -- the operator's shutdown signal disappeared into a debug
  log. Now only regular Exceptions are suppressed; BaseException propagates.
  """
  from scrapy_extension.backends.pulsar import _suppress_pulsar_errors

  sw = _suppress_pulsar_errors()
  sw.__enter__()
  # Regular Exception is suppressed (returns True).
  assert sw.__exit__(RuntimeError, RuntimeError("cleanup"), None) is True
  # BaseException (KeyboardInterrupt) is NOT suppressed (returns False).
  assert sw.__exit__(KeyboardInterrupt, KeyboardInterrupt(), None) is False
  # No exception (exc_type None) -> False (normal exit, propagate nothing).
  assert sw.__exit__(None, None, None) is False


# ---------------------------------------------------------------------------
# disconnect() client-None branch (line 280->284)
# ---------------------------------------------------------------------------


def test_disconnect_is_noop_when_client_already_none() -> None:
  """Line 280->284 (false branch): disconnect() with no client must skip
  ``client.close()`` and not raise — idempotent teardown for callers that
  never connected or already disconnected."""
  backend = _backend()
  backend._client = None
  backend._consumer = None
  backend._producers = {}
  backend.disconnect()  # must not raise
  assert backend._client is None


# ---------------------------------------------------------------------------
# _track_in_flight warn-once overflow (line 449->exit)
# ---------------------------------------------------------------------------


def test_track_in_flight_warns_once_on_overflow(caplog) -> None:
  """Line 449->exit: once the diagnostic in-flight set reaches
  ``_MAX_IN_FLIGHT``, further unacked pops emit a single warning (not one
  per pop) — the broker still tracks message_ids, so ack correctness is
  unaffected."""
  backend = _backend()
  for i in range(_MAX_IN_FLIGHT):
    backend._track_in_flight(_PulsarAckToken(message_id=f"id-{i}", topic="t"))
  assert len(backend._in_flight) == _MAX_IN_FLIGHT

  with caplog.at_level(logging.WARNING):
    backend._track_in_flight(_PulsarAckToken(message_id="overflow-1", topic="t"))
    backend._track_in_flight(_PulsarAckToken(message_id="overflow-2", topic="t"))

  overflow_warnings = [r for r in caplog.records if "at cap" in r.message]
  assert len(overflow_warnings) == 1  # warn-once
  assert backend._in_flight_overflow_warned is True
  assert len(backend._in_flight) == _MAX_IN_FLIGHT  # cap held


# ---------------------------------------------------------------------------
# _ack_token TOCTOU + acknowledge error (lines 537, 540-541)
# ---------------------------------------------------------------------------


def test_ack_token_is_noop_when_consumer_is_none() -> None:
  """Line 537: _ack_token with no consumer (concurrent disconnect / never
  connected) silently returns — ack is best-effort, and a vanished consumer
  must not crash the caller (the broker re-delivers via at-least-once)."""
  backend = _backend()
  backend._consumer = None
  backend._ack_token(_PulsarAckToken(message_id="id", topic="t"))  # must not raise


def test_ack_token_raises_when_acknowledge_fails(mocker) -> None:
  """Lines 540-541: ``consumer.acknowledge`` raising surfaces as a
  QueueError (operation='ack') — raise-on-failure so the caller sees the
  broker error (at-least-once redelivery follows from the unacked state)."""
  backend = _backend()
  backend._consumer = mocker.MagicMock()
  backend._consumer.acknowledge.side_effect = RuntimeError("ack boom")
  with pytest.raises(QueueError) as exc:
    backend._ack_token(_PulsarAckToken(message_id="id", topic="t"))
  assert exc.value.operation == "ack"


# ---------------------------------------------------------------------------
# nack legacy: consumer lacks negative_acknowledge (570->576 false branch)
# ---------------------------------------------------------------------------


def test_nack_legacy_noop_when_consumer_lacks_negative_acknowledge() -> None:
  """Line 570->576 (false branch): the legacy nack path (token=None) on a
  consumer whose client-lib version doesn't expose ``negative_acknowledge``
  is a silent no-op — the message stays unacked and is re-delivered on the
  unacked-timeout / consumer restart (at-least-once)."""
  backend = _backend()
  # spec restricts to "acknowledge" only -> getattr(negative_acknowledge) is None
  backend._consumer = MagicMock(spec=["acknowledge"])
  backend._last_msg = MagicMock(name="last-msg")
  backend.nack("q")  # token=None -> legacy path; must not raise
  assert backend._last_msg is None  # cleared by finally


# ---------------------------------------------------------------------------
# _nack_token error logging (lines 585-586)
# ---------------------------------------------------------------------------


def test_nack_token_logs_and_swallows_when_negative_acknowledge_fails(caplog) -> None:
  """Lines 585-586: a failure inside ``_nack_token`` (negative_acknowledge
  raises) is logged and swallowed — nack is best-effort, the message
  re-delivers on restart regardless. Must NOT raise (would break the
  caller's error path)."""
  backend = _backend()
  backend._consumer = MagicMock()
  backend._consumer.negative_acknowledge.side_effect = RuntimeError("nack boom")
  token = _PulsarAckToken(message_id="id", topic="t")
  with caplog.at_level(logging.WARNING):
    backend.nack("q", token=token)  # must not raise
  assert any("redeliver on restart" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _ensure_consumer subscribe failure (lines 648-649)
# ---------------------------------------------------------------------------


def test_ensure_consumer_raises_when_subscribe_fails(mocker) -> None:
  """Lines 648-649: ``client.subscribe`` raising surfaces as a QueueError
  (operation='pop') so the scheduler's pop path sees a clean error rather
  than the raw client-lib exception."""
  backend = _backend()
  backend._client = mocker.MagicMock()
  backend._client.subscribe.side_effect = RuntimeError("subscribe boom")
  with pytest.raises(QueueError) as exc:
    backend._ensure_consumer("persistent://public/default/q")
  assert exc.value.operation == "pop"


# ---------------------------------------------------------------------------
# _message_bytes non-bytes payload fallbacks (lines 673, 679)
# ---------------------------------------------------------------------------


def test_message_bytes_str_encodes_when_data_returns_non_bytes() -> None:
  """Line 673: when ``msg.data()`` returns a non-bytes payload (a misbehaving
  producer / schema mismatch), it is str-encoded rather than crashing the
  pop path with an AssertionError."""
  msg = MagicMock()
  msg.data.return_value = "string-not-bytes"  # str, not bytes/bytearray
  assert _message_bytes(msg) == b"string-not-bytes"


def test_message_bytes_falls_back_to_value_when_data_absent() -> None:
  """Line 679: when ``msg.data`` is absent and ``msg.value()`` returns a
  non-bytes payload, it is str-encoded — defensive multi-accessor fallback
  so the pop path tolerates schema-less / schema-full message variants."""
  msg = MagicMock(spec=["value"])  # no data method
  msg.value.return_value = 42  # int, not bytes
  assert _message_bytes(msg) == b"42"
