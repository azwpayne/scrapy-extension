"""Tests for PulsarBackend (subsystem ③) — mocked pulsar-client.

The real ``pulsar-client`` is a heavy C++ binding; we inject a mock into
``sys.modules`` before importing the backend so the module-level
``import pulsar`` succeeds without the dependency installed, and assert call
patterns against the mock.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("pulsar", MagicMock())
import pulsar  # noqa: E402 — the mocked module actually in sys.modules


@pytest.fixture(scope="module", autouse=True)
def _cleanup_sys_modules_mock_pulsar():
  """Pop the module-level ``pulsar`` mock after this module's tests finish.

  R14-G flake fix: module-top-level ``sys.modules.setdefault`` pollutes the
  session for later modules; pop at module teardown.
  """
  yield
  sys.modules.pop("pulsar", None)

from scrapy_extension.backends.base import (  # noqa: E402
  BackendType,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.pulsar import (  # noqa: E402
  PulsarBackend,
  _PulsarAckToken,
)
from scrapy_extension.exceptions import BackendConnectionError, QueueError  # noqa: E402
from scrapy_extension.settings import PulsarMode, PulsarSettings  # noqa: E402


def _make_backend(**overrides) -> PulsarBackend:
  return PulsarBackend(PulsarSettings(**overrides))


def _connected(mocker, **client_children):
  """Build a connected backend; ``client_children`` pre-stubs client attrs."""
  b = _make_backend()
  client = mocker.MagicMock()
  for attr, val in client_children.items():
    getattr(client, attr).return_value = val
  mocker.patch.object(pulsar, "Client", return_value=client)
  b.connect()
  return b, client


class TestPulsarBackendType:
  def test_backend_type_is_pulsar(self) -> None:
    assert _make_backend().backend_type is BackendType.PULSAR

  def test_queue_only_no_set_no_storage(self) -> None:
    b = _make_backend()
    assert not isinstance(b, SetBackend)
    assert not isinstance(b, StorageBackend)

  def test_settings_defaults(self) -> None:
    s = PulsarSettings()
    assert s.mode is PulsarMode.STANDALONE
    assert s.service_url == "pulsar://localhost:6650"
    assert s.consumer_type == "Shared"


class TestPulsarConnect:
  def test_connect_creates_client(self, mocker) -> None:
    b = _make_backend()
    client = mocker.MagicMock()
    mocker.patch.object(pulsar, "Client", return_value=client)
    b.connect()
    pulsar.Client.assert_called_once_with("pulsar://localhost:6650")
    assert b.is_connected() is True

  def test_connect_failure_raises_connection_error(self, mocker) -> None:
    b = _make_backend()
    mocker.patch.object(pulsar, "Client", side_effect=RuntimeError("boom"))
    with pytest.raises(BackendConnectionError):
      b.connect()
    assert b.is_connected() is False

  def test_connect_with_auth_token(self, mocker) -> None:
    # SV3-2: auth_token requires pulsar+ssl:// (cleartext-token guard).
    b = _make_backend(
      service_url="pulsar+ssl://localhost:6651",
      auth_token="secret-token",
    )
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    auth_mock = mocker.patch.object(pulsar, "AuthenticationToken")
    b.connect()
    auth_mock.assert_called_once_with("secret-token")

  def test_disconnect_closes_client(self, mocker) -> None:
    b, client = _connected(mocker)
    b.disconnect()
    client.close.assert_called_once()
    assert b.is_connected() is False


class TestPulsarPush:
  def test_push_creates_producer_and_sends(self, mocker) -> None:
    producer = mocker.MagicMock()
    b, client = _connected(mocker, create_producer=producer)
    b.push("queue1", b"payload")
    client.create_producer.assert_called_once_with("scrapy-queue1")
    producer.send.assert_called_once_with(b"payload")

  def test_push_reuses_cached_producer(self, mocker) -> None:
    b, client = _connected(mocker)
    b.push("queue1", b"a")
    b.push("queue1", b"b")
    client.create_producer.assert_called_once_with("scrapy-queue1")

  def test_push_ignores_priority(self, mocker) -> None:
    producer = mocker.MagicMock()
    b, _ = _connected(mocker, create_producer=producer)
    b.push("queue1", b"x", priority=99.0)
    producer.send.assert_called_once_with(b"x")

  def test_push_failure_raises_queue_error(self, mocker) -> None:
    producer = mocker.MagicMock()
    producer.send.side_effect = RuntimeError("send failed")
    b, _ = _connected(mocker, create_producer=producer)
    with pytest.raises(QueueError):
      b.push("queue1", b"x")

  def test_push_invalid_name_raises(self, mocker) -> None:
    b, _ = _connected(mocker)
    with pytest.raises(ValueError):
      b.push("bad name!", b"x")


class TestPulsarPop:
  def _msg(self, mocker, payload=b"hello"):
    msg = mocker.MagicMock()
    msg.data.return_value = payload
    return msg

  def test_pop_subscribes_and_returns_bytes(self, mocker) -> None:
    msg = self._msg(mocker, b"hello")
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, client = _connected(mocker, subscribe=consumer)
    assert b.pop("queue1", timeout=1.0) == b"hello"
    client.subscribe.assert_called_once()
    assert b._last_msg is msg

  def test_pop_returns_none_on_empty(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError("timed out")
    b, _ = _connected(mocker, subscribe=consumer)
    assert b.pop("queue1") is None
    assert b._last_msg is None

  def test_pop_reuses_consumer_for_same_topic(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError("none")
    b, client = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.pop("queue1")
    client.subscribe.assert_called_once()

  def test_pop_resubscribes_on_topic_change(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError("none")
    b, client = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.pop("queue2")
    assert client.subscribe.call_count == 2

  def test_pop_topic_change_subscribe_failure_nulls_state_no_wedge(self, mocker) -> None:
    """R-pulsar-ensure: a failed re-subscribe on topic change must null the
    closed consumer + stale topic. Otherwise a later pop of the OLD topic hits
    the _ensure_consumer fast-path (``_subscribed_topic == topic``) and reuses
    the dead consumer -> silent consumption wedge."""
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError("none")
    b, client = _connected(mocker, subscribe=consumer)
    b.pop("queue1")  # subscribes to topic1; _consumer set, _subscribed_topic=topic1
    # topic change to queue2 -> re-subscribe fails
    client.subscribe.side_effect = RuntimeError("subscribe failed")
    with pytest.raises(QueueError):
      b.pop("queue2")
    # FIX: failed re-subscribe must null the closed consumer + stale topic
    assert b._consumer is None
    assert b._subscribed_topic is None


class TestPulsarAckNack:
  def test_ack_calls_acknowledge(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.ack("queue1")
    consumer.acknowledge.assert_called_once_with(msg)
    assert b._last_msg is None

  def test_ack_noop_without_message(self, mocker) -> None:
    b, _ = _connected(mocker)
    b.ack("queue1")  # no prior pop -> no error, no call

  def test_nack_calls_negative_acknowledge(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.nack("queue1")
    consumer.negative_acknowledge.assert_called_once_with(msg)
    assert b._last_msg is None


class TestPulsarRealAck:
  """Real per-message ack (round-3): in-flight set + _PulsarAckToken.

  Pulsar's Shared subscription is natively per-message —
  ``consumer.acknowledge(msg_id)`` targets one specific message. These
  tests prove the in-flight-set ack is correct under
  ``CONCURRENT_REQUESTS > 1`` (N pops before any ack no longer overwrite
  a single slot).
  """

  def test_supports_concurrent_ack_is_true(self) -> None:
    b = _make_backend()
    assert b.requires_ack is True
    assert b.supports_concurrent_ack is True

  def test_pop_with_ack_returns_bytes_and_token(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"hello"
    msg_id = mocker.MagicMock(name="msg_id_a")
    msg.message_id.return_value = msg_id
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    value, token = b.pop_with_ack("queue1", timeout=1.0)
    assert value == b"hello"
    assert isinstance(token, _PulsarAckToken)
    assert token.message_id is msg_id
    assert token in b._in_flight

  def test_pop_with_ack_empty_returns_none_none(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = RuntimeError("timed out")
    b, _ = _connected(mocker, subscribe=consumer)
    value, token = b.pop_with_ack("queue1")
    assert value is None
    assert token is None
    assert b._in_flight == set()

  def test_multi_pop_then_ack_each_by_own_token(self, mocker) -> None:
    """Three pops with no acks between, then ack each by its OWN token.

    RED pre-fix: single-slot _last_msg only holds the 3rd message, so the
    first two acks would no-op (or only the 3rd message gets acked).
    GREEN post-fix: each ack hits the right message_id and the in-flight
    set empties.
    """
    msg_ids = [mocker.MagicMock(name=f"id_{i}") for i in range(3)]
    msgs = []
    for i, mid in enumerate(msg_ids):
      m = mocker.MagicMock(name=f"msg_{i}")
      m.data.return_value = f"payload-{i}".encode()
      m.message_id.return_value = mid
      msgs.append(m)
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = msgs
    b, _ = _connected(mocker, subscribe=consumer)

    # Pop 3 without acking between — each builds its own token.
    tokens = []
    for _ in range(3):
      value, token = b.pop_with_ack("queue1", timeout=1.0)
      assert token is not None
      tokens.append(token)
    assert len(b._in_flight) == 3

    # Ack each by its OWN token — distinct message_ids, correct each.
    consumer.acknowledge.reset_mock()
    for token in tokens:
      b.ack("queue1", token=token)
    # Three distinct acknowledge(message_id) calls, in token order.
    assert consumer.acknowledge.call_count == 3
    actual_ids = [call.args[0] for call in consumer.acknowledge.call_args_list]
    assert actual_ids == [t.message_id for t in tokens]
    assert len(set(id(x) for x in actual_ids)) == 3  # 3 distinct objects
    assert b._in_flight == set()

  def test_ack_with_token_discards_from_in_flight(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg_id = mocker.MagicMock()
    msg.message_id.return_value = msg_id
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")
    assert token is not None
    assert len(b._in_flight) == 1
    b.ack("q", token=token)
    consumer.acknowledge.assert_called_once_with(msg_id)
    assert b._in_flight == set()

  def test_ack_with_token_idempotent_re_ack(self, mocker) -> None:
    """Re-acking a discarded token is a safe no-op on the in-flight set."""
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")
    b.ack("q", token=token)
    # Second ack on the same token — acknowledge fires again (Pulsar ack is
    # idempotent at the broker) but the in-flight set stays empty.
    b.ack("q", token=token)
    assert consumer.acknowledge.call_count == 2
    assert b._in_flight == set()

  def test_nack_with_token_calls_negative_acknowledge(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg_id = mocker.MagicMock()
    msg.message_id.return_value = msg_id
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")
    b.nack("q", token=token)
    consumer.negative_acknowledge.assert_called_once_with(msg_id)
    assert b._in_flight == set()

  def test_nack_with_token_no_op_when_client_lacks_method(self, mocker) -> None:
    """Client without negative_acknowledge: nack(token) is a safe no-op."""
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    # Remove negative_acknowledge to simulate older client.
    del consumer.negative_acknowledge
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")
    b.nack("q", token=token)  # must not raise
    assert b._in_flight == set()

  def test_crash_mid_ack_leaves_messages_in_flight(self, mocker) -> None:
    """Pop 2, ack neither → both stay in _in_flight (re-delivered on restart).

    At-least-once: an unacked message is redelivered by Pulsar on consumer
    restart, so a crash mid-batch never loses work.
    """
    msg_ids = [mocker.MagicMock(name=f"id_{i}") for i in range(2)]
    msgs = []
    for i, mid in enumerate(msg_ids):
      m = mocker.MagicMock()
      m.data.return_value = f"p-{i}".encode()
      m.message_id.return_value = mid
      msgs.append(m)
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = msgs
    b, _ = _connected(mocker, subscribe=consumer)
    b.pop_with_ack("q")
    b.pop_with_ack("q")
    # No acks — both remain in-flight.
    assert len(b._in_flight) == 2
    consumer.acknowledge.assert_not_called()

  def test_legacy_pop_then_ack_without_token(self, mocker) -> None:
    """Legacy path: pop() then ack(token=None) via _last_msg still works."""
    msg = mocker.MagicMock()
    msg.data.return_value = b"legacy"
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    value = b.pop("q")
    assert value == b"legacy"
    assert b._last_msg is msg
    b.ack("q")  # no token — legacy path
    consumer.acknowledge.assert_called_once_with(msg)
    assert b._last_msg is None

  def test_ack_token_equality_and_repr(self) -> None:
    """_PulsarAckToken equality is by message_id identity; repr is informative."""
    mid1 = object()
    mid2 = object()
    t1a = _PulsarAckToken(message_id=mid1, topic="t")
    t1b = _PulsarAckToken(message_id=mid1, topic="t")
    t2 = _PulsarAckToken(message_id=mid2, topic="t")
    assert t1a == t1b
    assert t1a != t2
    assert t1a != "not-a-token"
    # Hashable (set membership).
    assert len({t1a, t1b, t2}) == 2
    r = repr(t1a)
    assert "_PulsarAckToken" in r
    assert "topic='t'" in r


class TestPulsarLenClear:
  def test_queue_len_returns_zero(self, mocker) -> None:
    b, _ = _connected(mocker)
    assert b.queue_len("queue1") == 0

  def test_clear_queue_drops_cached_handles(self, mocker) -> None:
    producer = mocker.MagicMock()
    b, _ = _connected(mocker, create_producer=producer)
    b.push("queue1", b"x")
    b.clear_queue("queue1")
    producer.close.assert_called_once()
    assert "scrapy-queue1" not in b._producers


# ---------------------------------------------------------------------------
# SEC-5 (round-6): Pulsar TLS decouple — allow_insecure_connection is passed
# for pulsar+ssl:// URLs even when tls_trust_certs_file is unset.
# SEC-1: auth_token is wrapped in _RedactedStr.
# ---------------------------------------------------------------------------


class TestPulsarTlsDecouple:
  """SEC-5: ``allow_insecure_connection`` and ``tls_trust_certs_file`` are
  independent TLS controls. Pre-fix, ``allow_insecure_connection`` was only
  passed inside ``if tls_trust_certs_file``, silently dropping the user's
  intent (and reverting ``True`` to Pulsar's stricter default) when no trust
  certs file was configured.
  """

  def test_ssl_passes_allow_insecure_without_trust_certs(self, mocker) -> None:
    """pulsar+ssl:// + allow_insecure_connection=False + no trust_certs:
    Client kwargs include allow_insecure_connection WITHOUT trust certs."""
    b = _make_backend(
      service_url="pulsar+ssl://broker:6651",
      allow_insecure_connection=False,
    )
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    b.connect()
    _, kwargs = pulsar.Client.call_args.args, pulsar.Client.call_args.kwargs
    assert "allow_insecure_connection" in kwargs
    assert kwargs["allow_insecure_connection"] is False
    # trust_certs is NOT passed when unset (the bug was gating on this).
    assert "tls_trust_certs_file" not in kwargs

  def test_ssl_passes_allow_insecure_true_without_trust_certs(self, mocker) -> None:
    """SEC-5 reverse: allow_insecure_connection=True is forwarded (not silently
    dropped) even when tls_trust_certs_file is unset."""
    b = _make_backend(
      service_url="pulsar+ssl://broker:6651",
      allow_insecure_connection=True,
    )
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    b.connect()
    kwargs = pulsar.Client.call_args.kwargs
    assert kwargs.get("allow_insecure_connection") is True
    assert "tls_trust_certs_file" not in kwargs

  def test_ssl_passes_both_when_both_set(self, mocker) -> None:
    """Both set → both passed (backward compat with the original path)."""
    b = _make_backend(
      service_url="pulsar+ssl://broker:6651",
      allow_insecure_connection=True,
      tls_trust_certs_file="/etc/ssl/ca.pem",
    )
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    b.connect()
    kwargs = pulsar.Client.call_args.kwargs
    assert kwargs.get("allow_insecure_connection") is True
    assert kwargs.get("tls_trust_certs_file") == "/etc/ssl/ca.pem"

  def test_non_ssl_url_omits_tls_kwargs(self, mocker) -> None:
    """pulsar:// (plaintext) doesn't pass either TLS field."""
    b = _make_backend(
      service_url="pulsar://broker:6650",
      allow_insecure_connection=True,  # ignored — not an ssl url
    )
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    b.connect()
    kwargs = pulsar.Client.call_args.kwargs
    assert "allow_insecure_connection" not in kwargs
    assert "tls_trust_certs_file" not in kwargs


def test_pulsar_auth_token_is_redacted_str(mocker) -> None:
  """SEC-1: the auth_token handed to AuthenticationToken is wrapped in
  _RedactedStr so Sentry / repr captures don't leak it. str value preserved."""
  from scrapy_extension.backends._redaction import _RedactedStr

  b = _make_backend(
    service_url="pulsar+ssl://broker:6651",
    auth_token="top-secret-pulsar-token",
  )
  mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
  auth_mock = mocker.patch.object(pulsar, "AuthenticationToken")
  b.connect()
  auth_mock.assert_called_once()
  token_arg = auth_mock.call_args.args[0]
  # Value preserved for the pulsar client (str semantics).
  assert str(token_arg) == "top-secret-pulsar-token"
  # But repr is masked.
  assert "top-secret-pulsar-token" not in repr(token_arg)
  assert isinstance(token_arg, _RedactedStr)


# ===========================================================================
# R14-E — Lifecycle bounds: Pulsar diagnostic in-flight set cap
# ===========================================================================


class TestPulsarInFlightCap:
  """R14-E MED: the diagnostic ``_in_flight`` set is capped at ``_MAX_IN_FLIGHT``."""

  def test_pop_with_ack_caps_in_flight_set(self, mocker, caplog) -> None:
    """When the set is saturated, the pop still succeeds but the set stops growing."""
    import logging

    from scrapy_extension.backends.pulsar import _MAX_IN_FLIGHT

    msg = mocker.MagicMock()
    msg.data.return_value = b"hello"
    msg_id = mocker.MagicMock(name="msg_id_overflow")
    msg.message_id.return_value = msg_id
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _client = _connected(mocker, subscribe=consumer)

    # Pre-saturate the set so the next pop trips the cap.
    b._in_flight = {
      _PulsarAckToken(message_id=object(), topic=f"t{i}") for i in range(_MAX_IN_FLIGHT)
    }
    assert not b._in_flight_overflow_warned

    with caplog.at_level(logging.WARNING):
      value, token = b.pop_with_ack("queue1", timeout=1.0)

    # The pop succeeded — message returned, NOT dropped.
    assert value == b"hello"
    assert isinstance(token, _PulsarAckToken)
    # The set stayed at the cap (the new token was not added).
    assert len(b._in_flight) == _MAX_IN_FLIGHT
    # The one-shot warning fired.
    assert b._in_flight_overflow_warned is True
    assert any("at cap" in r.message for r in caplog.records)
