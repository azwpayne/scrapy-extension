"""Resilience / contract tests for SqsBackend (initiative #27).

sqs.py was 91.96% (9 uncovered lines + partial branches), below the 95%
floor. Every gap was a real documented contract with no direct test:

- ``_SqsAckToken.__eq__`` returning ``NotImplemented`` for non-token types
  (Python's __eq__ protocol).
- connect() SEC-7 credential XOR-validation: exactly one of
  (access_key_id, secret_access_key) set → ConfigurationError naming the
  missing counterpart (prevents running under an unintended AWS identity).
- connect() wiring ``endpoint_url`` into the boto3 client (LocalStack path).
- disconnect() idempotency when never connected.
- ``_track_in_flight`` warn-once overflow (bounded diagnostic set).
- ``_receive`` tolerating a malformed message (no ReceiptHandle).
- ack/nack-with-token failures when the client is gone remain retryable.
- nack-with-token clearing the legacy ``_last_receipt`` slot when it matches.
- ``_swallow`` actually suppressing + logging a cleanup exception.
"""

from __future__ import annotations

import base64
import logging
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.sqs import (
  _MAX_IN_FLIGHT,
  SqsBackend,
  _SqsAckToken,
  _swallow,
)
from scrapy_extension.exceptions import ConfigurationError, QueueError
from scrapy_extension.settings import SqsSettings


def _backend() -> SqsBackend:
  """A constructed-but-not-connected backend (client is None)."""
  return SqsBackend(SqsSettings())


# ---------------------------------------------------------------------------
# _SqsAckToken.__eq__ protocol (line 96)
# ---------------------------------------------------------------------------


def test_ack_token_eq_returns_not_implemented_for_other_types() -> None:
  """Line 96: comparing to a non-_SqsAckToken returns NotImplemented (Python's
  __eq__ protocol — lets the other side's __eq__ be tried, falls back to
  identity). Pinned so a future refactor doesn't silently return False and
  break dict/set membership semantics for tokens."""
  token = _SqsAckToken("u", "r")
  assert token.__eq__("not-a-token") is NotImplemented
  assert token.__eq__(42) is NotImplemented
  # Token equality still works for real tokens:
  assert token == _SqsAckToken("u", "r")


# ---------------------------------------------------------------------------
# connect() SEC-7 credential XOR-validation (lines 172-174)
# ---------------------------------------------------------------------------


def test_connect_rejects_partial_aws_credentials_access_key_only() -> None:
  """Lines 172-174 (SEC-7, defense-in-depth): connect() re-checks the
  both-or-neither credential invariant even though ``SqsSettings`` already
  validates it at construction — so a backend whose config is mutated
  post-construction (bypassing settings validation) still fails fast rather
  than silently running under boto3's default credential chain (an
  unintended identity). Reached by mutating the config after construction."""
  from pydantic import SecretStr

  backend = _backend()
  backend.config.aws_access_key_id = SecretStr("ak")  # bypass settings validation
  with pytest.raises(ConfigurationError) as exc:
    backend.connect()
  assert "aws_secret_access_key" in str(exc.value)


def test_connect_rejects_partial_aws_credentials_secret_only() -> None:
  """Lines 172-174 (SEC-7, defense-in-depth): the reverse mismatch — secret
  set, key missing — also caught at connect() when settings validation is
  bypassed by post-construction mutation."""
  from pydantic import SecretStr

  backend = _backend()
  backend.config.aws_secret_access_key = SecretStr("sk")  # bypass settings validation
  with pytest.raises(ConfigurationError) as exc:
    backend.connect()
  assert "aws_access_key_id" in str(exc.value)


# ---------------------------------------------------------------------------
# connect() endpoint_url wiring (line 184)
# ---------------------------------------------------------------------------


def test_connect_passes_endpoint_url_into_boto3_client(mocker) -> None:
  """Line 184: when ``endpoint_url`` is set (LocalStack), it is forwarded to
  ``boto3.client`` so LocalStack tests route correctly."""
  mock_boto3 = mocker.patch("scrapy_extension.backends.sqs.boto3")
  backend = SqsBackend(SqsSettings(endpoint_url="http://localhost:4566"))
  backend.connect()
  _, kwargs = mock_boto3.client.call_args
  assert kwargs["endpoint_url"] == "http://localhost:4566"


# ---------------------------------------------------------------------------
# disconnect() idempotency (line 197->201 false branch)
# ---------------------------------------------------------------------------


def test_disconnect_before_connect_is_a_silent_noop() -> None:
  """Line 197->201 (false branch): disconnect() with no client must not raise
  (no ``None.close()``) — idempotent teardown for callers that didn't connect."""
  backend = _backend()
  backend.disconnect()  # _client is None — must not raise
  assert backend._client is None
  assert backend._in_flight == set()


# ---------------------------------------------------------------------------
# _track_in_flight warn-once overflow (line 357->exit)
# ---------------------------------------------------------------------------


def test_track_in_flight_warns_once_on_overflow(caplog) -> None:
  """Line 357->exit: once the diagnostic in-flight set reaches ``_MAX_IN_FLIGHT``,
  further unacked pops emit a single warning (not one per pop) — the broker
  still tracks receipt handles, so ack correctness is unaffected."""
  backend = _backend()
  for i in range(_MAX_IN_FLIGHT):
    backend._track_in_flight(_SqsAckToken(f"u{i}", f"r{i}"))
  assert len(backend._in_flight) == _MAX_IN_FLIGHT

  with caplog.at_level(logging.WARNING):
    backend._track_in_flight(_SqsAckToken("overflow-1", "ro-1"))
    backend._track_in_flight(_SqsAckToken("overflow-2", "ro-2"))

  overflow_warnings = [r for r in caplog.records if "at cap" in r.message]
  assert len(overflow_warnings) == 1  # warn-once
  assert backend._in_flight_overflow_warned is True
  # Cap held — overflow tokens NOT added (the pop itself isn't dropped; only
  # the diagnostic set is bounded):
  assert len(backend._in_flight) == _MAX_IN_FLIGHT


# ---------------------------------------------------------------------------
# _receive tolerates a malformed message (line 407)
# ---------------------------------------------------------------------------


def test_receive_raises_for_message_without_receipt_handle(mocker) -> None:
  """A malformed delivery must not masquerade as an empty queue."""
  backend = _backend()
  mocker.patch.object(backend, "_queue_url", return_value="http://q-url")
  backend._client = mocker.MagicMock()
  backend._client.receive_message.return_value = {
    "Messages": [{"Body": base64.b64encode(b"x").decode()}]  # no ReceiptHandle
  }
  with pytest.raises(QueueError) as exc_info:
    backend._receive("q", 0.0)

  assert exc_info.value.operation == "pop"
  assert "ReceiptHandle" in str(exc_info.value)


# ---------------------------------------------------------------------------
# ack-with-token TOCTOU: client gone (line 443)
# ---------------------------------------------------------------------------


def test_ack_with_token_client_none_raises_and_remains_retryable() -> None:
  """A disconnect is transient, not proof that broker settlement succeeded."""
  backend = _backend()
  backend._client = None
  token = _SqsAckToken("u", "r")
  backend._track_in_flight(token)

  with pytest.raises(QueueError) as exc_info:
    backend.ack("q", token=token)

  assert exc_info.value.operation == "ack"
  assert token in backend._in_flight


def test_nack_with_token_client_none_raises_and_remains_retryable() -> None:
  backend = _backend()
  token = _SqsAckToken("u", "r")
  backend._track_in_flight(token)

  with pytest.raises(QueueError) as exc_info:
    backend.nack("q", token=token)

  assert exc_info.value.operation == "nack"
  assert token in backend._in_flight


# ---------------------------------------------------------------------------
# nack-with-token clears the matching legacy slot (line 483->485)
# ---------------------------------------------------------------------------


def test_nack_with_token_clears_matching_legacy_last_receipt() -> None:
  """Line 483 (true branch): nack(token=...) clears ``_last_receipt`` when it
  points at the same handle — keeps the legacy single-pop slot coherent with
  the per-message token path (single-process sanity)."""
  backend = _backend()
  backend._client = MagicMock()
  token = _SqsAckToken("u-1", "rh-1")
  backend._last_receipt = ("u-1", "rh-1")  # legacy slot matches the token
  backend.nack("q", token=token)
  assert backend._last_receipt is None  # cleared
  assert token not in backend._in_flight


def test_nack_with_token_keeps_nonmatching_legacy_last_receipt() -> None:
  """Line 483->485 (false branch): nack(token=...) where ``_last_receipt``
  points at a DIFFERENT handle leaves it intact — only the matching case
  clears the legacy slot (the token path and legacy path are independent
  except for the single-process coherence optimization)."""
  backend = _backend()
  backend._client = MagicMock()
  token = _SqsAckToken("u-new", "rh-new")
  backend._last_receipt = ("u-other", "rh-other")  # different handle
  backend.nack("q", token=token)
  assert backend._last_receipt == ("u-other", "rh-other")  # unchanged
  assert token not in backend._in_flight  # still discarded from in-flight


# ---------------------------------------------------------------------------
# _swallow suppresses + logs a cleanup exception (lines 534-535)
# ---------------------------------------------------------------------------


def test_swallow_suppresses_and_logs_cleanup_exception(caplog) -> None:
  """Lines 534-535: ``_swallow`` catches an exception raised inside the
  context, logs it at debug, and suppresses propagation (returns True) —
  the contract disconnect() relies on so a flaky ``client.close()`` never
  crashes teardown."""
  with caplog.at_level(logging.DEBUG):
    with _swallow():
      raise RuntimeError("cleanup boom")
  # Propagation suppressed (we reached this assert without the raise escaping):
  assert any("Suppressed SQS cleanup error" in r.message for r in caplog.records)


def test_swallow_does_not_suppress_base_exception() -> None:
  """R-swallow: _swallow must NOT suppress BaseException (Ctrl+C / SystemExit).

  Pre-fix ``__exit__`` returned True for any non-None ``exc_type``, so a
  KeyboardInterrupt raised inside a ``with _swallow():`` cleanup block was
  trapped -- the operator's shutdown signal disappeared into a debug log. Now
  only regular Exceptions are suppressed; BaseException propagates.
  """
  sw = _swallow()
  sw.__enter__()
  # Regular Exception is suppressed (returns True).
  assert sw.__exit__(RuntimeError, RuntimeError("cleanup"), None) is True
  # BaseException (KeyboardInterrupt) is NOT suppressed (returns False).
  assert sw.__exit__(KeyboardInterrupt, KeyboardInterrupt(), None) is False
  # No exception (exc_type None) -> False (normal exit, propagate nothing).
  assert sw.__exit__(None, None, None) is False
