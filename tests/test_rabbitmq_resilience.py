"""Resilience / contract tests for RabbitMQBackend (initiative #30).

rabbitmq.py was 91.85% (21 uncovered lines + 6 partial branches), the
last sizeable coverage gap and below the 95% floor. This pins the clear
contract clusters (ack/nack TOCTOU + AMQP-error paths, _setup_qos
no-op, in-flight overflow warn-once, _connect_cluster null-on-failure
cleanup). Each test maps to a documented contract, not a line-hit.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from pika.exceptions import AMQPError

from scrapy_extension.backends.rabbitmq import (
  _MAX_IN_FLIGHT,
  RabbitMQBackend,
  _RabbitMQAckToken,
)
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import RabbitMQSettings


def _backend() -> RabbitMQBackend:
  """Constructed-but-not-connected backend (channel / connection are None)."""
  return RabbitMQBackend(RabbitMQSettings())


def _connected_backend() -> tuple[RabbitMQBackend, MagicMock]:
  """Construct a backend with one active mock channel generation."""
  backend = _backend()
  channel = MagicMock(name="channel")
  backend._activate_channel(MagicMock(name="connection"), channel)
  return backend, channel


def _token(backend: RabbitMQBackend, delivery_tag: int) -> _RabbitMQAckToken:
  """Build a token belonging to ``backend``'s current channel generation."""
  return _RabbitMQAckToken(delivery_tag, backend._channel_generation)


# ---------------------------------------------------------------------------
# _connect_cluster null-on-failure cleanup (lines 304-311, R14-E)
# ---------------------------------------------------------------------------


def test_connect_cluster_cleans_up_on_qos_failure(mocker) -> None:
  """Lines 304-311 (R14-E): if ``_apply_qos`` raises after the channel is
  open, the channel + connection are closed and ``_channel``/``_connection``
  are left None so :meth:`is_connected` stays truthful (no half-connected
  state). The AMQPError re-raises so connect()'s retry loop sees it."""
  mock_conn = MagicMock(name="connection")
  mock_channel = MagicMock(name="channel")
  mock_conn.channel.return_value = mock_channel
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    return_value=mock_conn,
  )
  mocker.patch.object(
    RabbitMQBackend, "_build_common_parameters", return_value=MagicMock(name="params")
  )
  backend = _backend()
  mocker.patch.object(backend, "_apply_qos", side_effect=AMQPError("qos fail"))
  with pytest.raises(AMQPError):
    backend._connect_cluster()
  # Cleanup: both closed, instance state stays None (truthful is_connected):
  mock_channel.close.assert_called_once()
  mock_conn.close.assert_called_once()
  assert backend._channel is None
  assert backend._connection is None


# ---------------------------------------------------------------------------
# _setup_qos no-op when no channel (lines 353-354)
# ---------------------------------------------------------------------------


def test_setup_qos_is_noop_when_channel_is_none() -> None:
  """Lines 353-354: ``_setup_qos`` with no channel is a silent no-op (no
  ``_apply_qos(None)``) — backward-compat shim for external callers that
  invoke it before connect."""
  backend = _backend()
  backend._channel = None
  backend._setup_qos()  # must not raise


# ---------------------------------------------------------------------------
# _track_in_flight warn-once overflow (line 582->exit)
# ---------------------------------------------------------------------------


def test_track_in_flight_warns_once_on_overflow(caplog) -> None:
  """Line 582->exit: once the diagnostic in-flight set reaches
  ``_MAX_IN_FLIGHT``, further unacked pops emit a single warning (not one
  per pop) — the broker still tracks delivery tags, so ack correctness is
  unaffected."""
  backend = _backend()
  for i in range(_MAX_IN_FLIGHT):
    backend._track_in_flight(_token(backend, i))
  assert len(backend._in_flight_tags) == _MAX_IN_FLIGHT

  with caplog.at_level(logging.WARNING):
    backend._track_in_flight(_token(backend, _MAX_IN_FLIGHT + 1))
    backend._track_in_flight(_token(backend, _MAX_IN_FLIGHT + 2))

  overflow_warnings = [r for r in caplog.records if "at cap" in r.message]
  assert len(overflow_warnings) == 1  # warn-once
  assert backend._in_flight_overflow_warned is True
  assert len(backend._in_flight_tags) == _MAX_IN_FLIGHT  # cap held


# ---------------------------------------------------------------------------
# ack(token=...) TOCTOU + AMQP error (lines 672, 675-677)
# ---------------------------------------------------------------------------


def test_ack_with_token_is_noop_when_channel_is_none() -> None:
  """Line 672: ack(token=...) when the channel is None (concurrent
  disconnect / never connected) silently returns rather than
  ``AttributeError``-ing on ``None.basic_ack()``."""
  backend = _backend()
  backend._channel = None
  backend.ack("q", token=_token(backend, 5))  # must not raise


def test_ack_with_token_raises_on_amqp_error() -> None:
  """Lines 675-677: a ``basic_ack`` AMQP failure surfaces as a QueueError —
  the caller must see the ack failure (at-least-once redelivery follows
  from the broker's visibility-timeout, not a silent swallow)."""
  backend, channel = _connected_backend()
  channel.basic_ack.side_effect = AMQPError("ack boom")
  with pytest.raises(QueueError, match="Failed to ack RabbitMQ message"):
    backend.ack("q", token=_token(backend, 5))


# ---------------------------------------------------------------------------
# nack(token=...) TOCTOU + AMQP error + last-tag coherence (711-712,
# 715-717, 720->722) + legacy no-channel (724-725)
# ---------------------------------------------------------------------------


def test_nack_with_token_is_noop_when_channel_is_none() -> None:
  """Lines 711-712: nack(token=...) with no channel silently returns — the
  broker re-delivers on visibility-timeout regardless (at-least-once)."""
  backend = _backend()
  backend._channel = None
  backend.nack("q", token=_token(backend, 5))  # must not raise


def test_nack_with_token_raises_on_amqp_error() -> None:
  """Lines 715-717: a ``basic_nack`` AMQP failure surfaces as a QueueError
  (operation='nack') — matches ack's raise-on-failure contract."""
  backend, channel = _connected_backend()
  channel.basic_nack.side_effect = AMQPError("nack boom")
  with pytest.raises(QueueError, match="Failed to nack RabbitMQ message"):
    backend.nack("q", token=_token(backend, 5))


def test_nack_with_token_does_not_settle_matching_legacy_slot() -> None:
  """Token and legacy settlement paths remain independent.

  A token-aware pop no longer populates the legacy slot, so a token must not
  clear a separately tracked legacy delivery even if a synthetic test gives
  both the same numeric tag.
  """
  backend, _channel = _connected_backend()
  backend._last_delivery_tag = 5
  backend.nack("q", token=_token(backend, 5))
  assert backend._last_delivery_tag == 5


def test_nack_legacy_is_noop_when_channel_or_tag_absent() -> None:
  """Lines 724-725: nack(token=None) with no channel (or no tracked
  last-delivery-tag) is a silent no-op — idempotent teardown."""
  backend = _backend()
  backend._channel = None
  backend._last_delivery_tag = None
  backend.nack("q")  # must not raise


# ---------------------------------------------------------------------------
# branch-completion: _setup_qos apply path (line 354) + nack non-matching (720->722)
# ---------------------------------------------------------------------------


def test_setup_qos_applies_qos_when_channel_present(mocker) -> None:
  """Line 354 (true branch): ``_setup_qos`` with a channel delegates to
  ``_apply_qos`` (the backward-compat shim's active path). _apply_qos is
  stubbed so the test doesn't depend on pika's basic_qos signature."""
  backend = _backend()
  backend._channel = mocker.MagicMock()
  mocker.patch.object(backend, "_apply_qos")
  backend._setup_qos()
  backend._apply_qos.assert_called_once_with(backend._channel)


def test_nack_with_token_keeps_nonmatching_last_delivery_tag() -> None:
  """A token settlement leaves an unrelated legacy delivery intact."""
  backend, _channel = _connected_backend()
  backend._last_delivery_tag = 99  # different from the token below
  backend.nack("q", token=_token(backend, 5))
  assert backend._last_delivery_tag == 99  # unchanged


def test_ack_token_identity_includes_channel_generation() -> None:
  """The same numeric tag in two channel generations is not the same token."""
  old = _RabbitMQAckToken(delivery_tag=1, channel_generation=3)
  same = _RabbitMQAckToken(delivery_tag=1, channel_generation=3)
  reconnected = _RabbitMQAckToken(delivery_tag=1, channel_generation=4)

  assert old == same
  assert hash(old) == hash(same)
  assert old != reconnected
  assert old != 1


def test_raw_delivery_tag_token_is_safely_ignored() -> None:
  """A bare int has no channel identity and must never reach the broker."""
  backend, channel = _connected_backend()

  backend.ack("q", token=1)
  backend.nack("q", token=1)

  channel.basic_ack.assert_not_called()
  channel.basic_nack.assert_not_called()


@pytest.mark.parametrize(
  ("operation", "channel_method"),
  [("ack", "basic_ack"), ("nack", "basic_nack")],
)
def test_stale_token_cannot_ack_same_delivery_tag_after_reconnect(
  mocker, operation: str, channel_method: str
) -> None:
  """Delivery tags are channel-scoped and restart on a fresh channel.

  A completion from the previous channel must never acknowledge the new
  channel's message when both deliveries happen to have the same numeric tag.
  The current-generation token must remain usable after the stale completion.
  """
  old_connection = MagicMock(name="old_connection")
  old_channel = MagicMock(name="old_channel")
  old_connection.channel.return_value = old_channel
  old_channel.basic_get.return_value = (
    MagicMock(delivery_tag=1),
    None,
    b"old-body",
  )
  new_connection = MagicMock(name="new_connection")
  new_channel = MagicMock(name="new_channel")
  new_connection.channel.return_value = new_channel
  new_channel.basic_get.return_value = (
    MagicMock(delivery_tag=1),
    None,
    b"new-body",
  )
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    side_effect=[old_connection, new_connection],
  )

  backend = _backend()
  backend.connect()
  _old_body, stale_token = backend.pop_with_ack("q")
  backend.disconnect()
  backend.connect()
  _new_body, current_token = backend.pop_with_ack("q")

  getattr(backend, operation)("q", token=stale_token)

  channel_call = getattr(new_channel, channel_method)
  channel_call.assert_not_called()

  getattr(backend, operation)("q", token=current_token)
  if operation == "ack":
    channel_call.assert_called_once_with(delivery_tag=1, multiple=False)
  else:
    channel_call.assert_called_once_with(delivery_tag=1, requeue=True)


# ---------------------------------------------------------------------------
# _connect_mirrored_queues HA-policy honesty (initiative #34 — functional fix)
# ---------------------------------------------------------------------------


def test_connect_mirrored_queues_warns_ha_policy_not_applied(mocker, caplog) -> None:
  """#34: when ``ha_mode`` is configured, connect emits a WARNING that the
  HA policy is NOT applied via AMQP (must be set out-of-band via
  rabbitmqctl/management) — so an operator doesn't operate under the false
  impression this client applied it. Previously the dict was built into a
  local ``definition`` and only logged at DEBUG as 'Configured', which was
  misleading and left the policy silently unset."""
  mock_conn = MagicMock(name="connection")
  mock_channel = MagicMock(name="channel")
  mock_conn.channel.return_value = mock_channel
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    return_value=mock_conn,
  )
  mocker.patch.object(
    RabbitMQBackend, "_build_common_parameters", return_value=MagicMock(name="params")
  )
  mocker.patch.object(RabbitMQBackend, "_apply_qos")  # succeeds -> no cleanup path
  backend = _backend()
  backend.config.ha_mode = "all"
  with caplog.at_level(logging.WARNING):
    backend._connect_mirrored_queues()
  assert any("NOT applied via AMQP" in r.message for r in caplog.records)
  assert backend._channel is mock_channel  # connect committed


def test_connect_mirrored_queues_no_warning_when_ha_mode_unset(mocker, caplog) -> None:
  """Early-return branch: when ``ha_mode`` is unset (default), the HA block
  is skipped cleanly after ``_connect_cluster`` — no warning emitted."""
  mock_conn = MagicMock(name="connection")
  mock_conn.channel.return_value = MagicMock(name="channel")
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    return_value=mock_conn,
  )
  mocker.patch.object(
    RabbitMQBackend, "_build_common_parameters", return_value=MagicMock(name="params")
  )
  mocker.patch.object(RabbitMQBackend, "_apply_qos")
  backend = _backend()  # ha_mode stays unset (default)
  with caplog.at_level(logging.WARNING):
    backend._connect_mirrored_queues()
  assert not any("NOT applied via AMQP" in r.message for r in caplog.records)
