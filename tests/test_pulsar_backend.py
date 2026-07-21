"""Tests for PulsarBackend (subsystem ③) — mocked pulsar-client.

The real ``pulsar-client`` is a heavy C++ binding; we inject a mock into
``sys.modules`` before importing the backend so the module-level
``import pulsar`` succeeds without the dependency installed, and assert call
patterns against the mock.
"""

from __future__ import annotations

import subprocess
import sys
import traceback
from threading import Event, Lock, Thread
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("pulsar", MagicMock())
import pulsar  # noqa: E402 — the mocked module actually in sys.modules


class _PulsarTimeout(Exception):
  """Test double matching ``pulsar.Timeout`` from the C++ client binding."""


pulsar.Timeout = _PulsarTimeout


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
from scrapy_extension.exceptions import (  # noqa: E402
  BackendConnectionError,
  ConfigurationError,
  QueueError,
)
from scrapy_extension.schedule.scheduler import BackendScheduler  # noqa: E402
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

  def test_connect_revalidates_mutated_authenticated_tls_before_sdk_io(
    self, mocker
  ) -> None:
    settings = PulsarSettings(
      service_url="pulsar+ssl://broker:6651",
      auth_token="secret-token",  # type: ignore[arg-type]
    )
    settings.allow_insecure_connection = True
    client = mocker.patch.object(pulsar, "Client")

    with pytest.raises(ConfigurationError) as exc_info:
      PulsarBackend(settings).connect()

    assert exc_info.value.setting_name == "allow_insecure_connection"
    client.assert_not_called()

  def test_connect_rejects_mutated_blank_token_before_sdk_io(self, mocker) -> None:
    settings = PulsarSettings(service_url="pulsar+ssl://broker:6651")
    settings.auth_token = "   "  # type: ignore[assignment]
    client = mocker.patch.object(pulsar, "Client")

    with pytest.raises(ConfigurationError) as exc_info:
      PulsarBackend(settings).connect()

    assert exc_info.value.setting_name == "auth_token"
    client.assert_not_called()

  def test_connect_uses_one_validated_settings_snapshot(self, mocker) -> None:
    settings = PulsarSettings(
      mode=PulsarMode.CLUSTER,
      service_url="pulsar+ssl://one:6651,two:6651",
      subscription_name="original-subscription",
      consumer_type="Shared",
      initial_position="Earliest",
      negative_ack_redelivery_delay_ms=7_000,
      auth_token="original-secret",  # type: ignore[arg-type]
      tls_trust_certs_file="/tls/original-ca.pem",
    )
    backend = PulsarBackend(settings)
    client = mocker.MagicMock(name="client")
    consumer = mocker.MagicMock(name="consumer")
    consumer.receive.side_effect = pulsar.Timeout("empty")
    client.subscribe.return_value = consumer
    client_factory = mocker.patch.object(pulsar, "Client", return_value=client)
    auth_object = mocker.MagicMock(name="authentication")

    def mutate_after_authentication(_token):
      settings.mode = PulsarMode.STANDALONE
      settings.service_url = "pulsar://attacker:6650"
      settings.subscription_name = "attacker-subscription"
      settings.consumer_type = "Exclusive"
      settings.initial_position = "Latest"
      settings.negative_ack_redelivery_delay_ms = 1
      settings.auth_token = None
      settings.tls_trust_certs_file = "/tls/attacker-ca.pem"
      settings.allow_insecure_connection = True
      settings.tls_validate_hostname = False
      return auth_object

    mocker.patch.object(
      pulsar,
      "AuthenticationToken",
      side_effect=mutate_after_authentication,
    )

    backend.connect()
    assert backend.pop("queue") is None

    client_factory.assert_called_once_with(
      "pulsar+ssl://one:6651,two:6651",
      authentication=auth_object,
      tls_allow_insecure_connection=False,
      tls_trust_certs_file_path="/tls/original-ca.pem",
      tls_validate_hostname=True,
    )
    client.subscribe.assert_called_once_with(
      "scrapy-queue",
      "original-subscription",
      consumer_type=pulsar.ConsumerType.Shared,
      initial_position=pulsar.InitialPosition.Earliest,
      negative_ack_redelivery_delay_ms=7_000,
    )

  def test_connection_snapshot_repr_redacts_auth_token(self) -> None:
    secret = "snapshot-secret"
    settings = PulsarSettings(
      service_url="pulsar+ssl://broker:6651",
      auth_token=secret,  # type: ignore[arg-type]
    )

    snapshot = PulsarBackend(settings)._capture_connection_snapshot()

    assert secret not in repr(snapshot)

  def test_startup_error_traceback_does_not_echo_driver_secrets(
    self, mocker
  ) -> None:
    secret = "pulsar-driver-secret"
    settings = PulsarSettings(
      service_url="pulsar+ssl://broker:6651",
      auth_token=secret,  # type: ignore[arg-type]
    )
    mocker.patch.object(
      pulsar,
      "Client",
      side_effect=RuntimeError(f"driver dump included {secret}"),
    )

    with pytest.raises(BackendConnectionError) as exc_info:
      PulsarBackend(settings).connect()

    rendered = "".join(traceback.format_exception(exc_info.value))
    assert secret not in str(exc_info.value)
    assert secret not in rendered
    assert exc_info.value.__cause__ is None

  def test_connect_is_idempotent_while_connected(self, mocker) -> None:
    consumer = mocker.MagicMock(name="consumer")
    consumer.receive.side_effect = pulsar.Timeout("empty")
    b, old_client = _connected(mocker, subscribe=consumer)
    b.pop("queue")
    new_client = mocker.MagicMock(name="new_client")
    pulsar.Client.return_value = new_client

    b.connect()

    assert pulsar.Client.call_count == 1
    assert b._client is old_client
    assert b._consumers == {"scrapy-queue": consumer}
    new_client.close.assert_not_called()

  def test_disconnect_closes_client(self, mocker) -> None:
    b, client = _connected(mocker)
    b.disconnect()
    client.close.assert_called_once()
    assert b.is_connected() is False

  def test_disconnect_closes_all_topic_consumers(self, mocker) -> None:
    consumer_a = mocker.MagicMock(name="consumer_a")
    consumer_a.receive.side_effect = pulsar.Timeout("empty a")
    consumer_b = mocker.MagicMock(name="consumer_b")
    consumer_b.receive.side_effect = pulsar.Timeout("empty b")
    b, client = _connected(mocker)
    client.subscribe.side_effect = [consumer_a, consumer_b]
    b.pop("queue_a")
    b.pop("queue_b")

    b.disconnect()

    consumer_a.close.assert_called_once_with()
    consumer_b.close.assert_called_once_with()
    assert b._consumers == {}
    assert b._consumer is None
    assert b._subscribed_topic is None

  def test_reconnect_during_disconnect_does_not_close_new_client(self, mocker) -> None:
    close_started = Event()
    release_close = Event()
    old_consumer = mocker.MagicMock(name="old_consumer")
    old_consumer.receive.side_effect = pulsar.Timeout("empty")

    def blocking_close() -> None:
      close_started.set()
      release_close.wait(timeout=3.0)

    old_consumer.close.side_effect = blocking_close
    b, old_client = _connected(mocker, subscribe=old_consumer)
    b.pop("queue")
    disconnect_thread = Thread(target=b.disconnect)
    disconnect_thread.start()
    assert close_started.wait(timeout=2.0)

    new_client = mocker.MagicMock(name="new_client")
    pulsar.Client.return_value = new_client
    b.connect()
    release_close.set()
    disconnect_thread.join(timeout=2.0)

    assert not disconnect_thread.is_alive()
    old_client.close.assert_called_once_with()
    new_client.close.assert_not_called()
    assert b._client is new_client

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

  def test_disconnect_during_producer_creation_closes_loser(self, mocker) -> None:
    creation_started = Event()
    release_creation = Event()
    producer = mocker.MagicMock(name="loser_producer")

    def create_producer(*_args, **_kwargs):
      creation_started.set()
      release_creation.wait(timeout=3.0)
      return producer

    b, client = _connected(mocker)
    client.create_producer.side_effect = create_producer
    errors: list[Exception] = []

    def push_one() -> None:
      try:
        b.push("queue", b"payload")
      except Exception as error:
        errors.append(error)

    push_thread = Thread(target=push_one)
    push_thread.start()
    assert creation_started.wait(timeout=2.0)
    b.disconnect()
    release_creation.set()
    push_thread.join(timeout=2.0)

    assert not push_thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], QueueError)
    assert errors[0].operation == "push"
    producer.close.assert_called_once_with()
    producer.send.assert_not_called()
    assert b._producers == {}


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
    consumer.receive.side_effect = pulsar.Timeout("timed out")
    b, _ = _connected(mocker, subscribe=consumer)
    assert b.pop("queue1") is None
    assert b._last_msg is None

  def test_pop_wraps_non_timeout_receive_failure(self, mocker) -> None:
    consumer = mocker.MagicMock()
    failure = RuntimeError("broker disconnected")
    consumer.receive.side_effect = failure
    b, _ = _connected(mocker, subscribe=consumer)

    with pytest.raises(QueueError) as exc_info:
      b.pop("queue1")

    assert exc_info.value.queue_name == "queue1"
    assert exc_info.value.operation == "pop"
    assert exc_info.value.__cause__ is failure

  def test_pop_reuses_consumer_for_same_topic(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = pulsar.Timeout("none")
    b, client = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.pop("queue1")
    client.subscribe.assert_called_once()

  def test_pop_resubscribes_on_topic_change(self, mocker) -> None:
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = pulsar.Timeout("none")
    b, client = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    b.pop("queue2")
    assert client.subscribe.call_count == 2

  def test_pop_topic_subscribe_failure_preserves_cached_consumer(self, mocker) -> None:
    """A failed subscription for one topic must not break another topic."""
    consumer = mocker.MagicMock()
    consumer.receive.side_effect = pulsar.Timeout("none")
    b, client = _connected(mocker, subscribe=consumer)
    b.pop("queue1")
    client.subscribe.side_effect = RuntimeError("subscribe failed")
    with pytest.raises(QueueError):
      b.pop("queue2")

    assert b._consumer is consumer
    assert b._subscribed_topic == "scrapy-queue1"
    assert b._consumers == {"scrapy-queue1": consumer}
    assert b.pop("queue1") is None
    assert consumer.receive.call_count == 2

  def test_concurrent_first_pop_creates_one_consumer_per_topic(self, mocker) -> None:
    first_subscribe_started = Event()
    second_pop_started = Event()
    second_subscribe_started = Event()
    release_first_subscribe = Event()
    call_lock = Lock()
    subscribe_index = 0

    consumers = [mocker.MagicMock(name=f"consumer_{i}") for i in range(2)]
    for i, consumer in enumerate(consumers):
      msg = mocker.MagicMock(name=f"msg_{i}")
      msg.data.return_value = f"payload-{i}".encode()
      msg.message_id.return_value = mocker.MagicMock(name=f"msg_id_{i}")
      consumer.receive.return_value = msg

    def subscribe(*_args, **_kwargs):
      nonlocal subscribe_index
      with call_lock:
        index = subscribe_index
        subscribe_index += 1
      if index == 0:
        first_subscribe_started.set()
        release_first_subscribe.wait(timeout=3.0)
      else:
        second_subscribe_started.set()
      return consumers[index]

    b, client = _connected(mocker)
    client.subscribe.side_effect = subscribe
    errors: list[Exception] = []

    def pop_one(started: Event | None = None) -> None:
      if started is not None:
        started.set()
      try:
        b.pop_with_ack("queue")
      except Exception as error:
        errors.append(error)

    first = Thread(target=pop_one)
    second = Thread(target=pop_one, args=(second_pop_started,))
    first.start()
    assert first_subscribe_started.wait(timeout=2.0)
    second.start()
    assert second_pop_started.wait(timeout=2.0)
    duplicate_created = second_subscribe_started.wait(timeout=1.0)
    release_first_subscribe.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert duplicate_created is False
    assert errors == []
    assert client.subscribe.call_count == 1
    assert b._consumers == {"scrapy-queue": consumers[0]}

  def test_disconnect_during_consumer_creation_closes_loser(self, mocker) -> None:
    subscribe_started = Event()
    release_subscribe = Event()
    consumer = mocker.MagicMock(name="loser_consumer")

    def subscribe(*_args, **_kwargs):
      subscribe_started.set()
      release_subscribe.wait(timeout=3.0)
      return consumer

    b, client = _connected(mocker)
    client.subscribe.side_effect = subscribe
    errors: list[Exception] = []

    def pop_one() -> None:
      try:
        b.pop("queue")
      except Exception as error:
        errors.append(error)

    pop_thread = Thread(target=pop_one)
    pop_thread.start()
    assert subscribe_started.wait(timeout=2.0)
    b.disconnect()
    release_subscribe.set()
    pop_thread.join(timeout=2.0)

    assert not pop_thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], QueueError)
    assert errors[0].operation == "pop"
    consumer.close.assert_called_once_with()
    assert b._consumers == {}
    assert b._consumer is None

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

  def test_ack_unknown_token_does_not_ack_legacy_message(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    b.pop("queue1")

    b.ack("queue1", token=object())

    consumer.acknowledge.assert_not_called()
    assert b._last_msg is msg

  def test_legacy_ack_uses_consumer_that_delivered_last_message(self, mocker) -> None:
    msg = mocker.MagicMock(name="msg_a")
    msg.data.return_value = b"a"
    consumer_a = mocker.MagicMock(name="consumer_a")
    consumer_a.receive.return_value = msg
    consumer_b = mocker.MagicMock(name="consumer_b")
    consumer_b.receive.side_effect = pulsar.Timeout("empty b")
    b, client = _connected(mocker)
    client.subscribe.side_effect = [consumer_a, consumer_b]
    assert b.pop("queue_a") == b"a"
    assert b.pop("queue_b") is None

    b.ack("queue_a")

    consumer_a.acknowledge.assert_called_once_with(msg)
    consumer_b.acknowledge.assert_not_called()

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

  def test_nack_unknown_token_does_not_nack_legacy_message(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    b.pop("queue1")

    b.nack("queue1", token=object())

    consumer.negative_acknowledge.assert_not_called()
    assert b._last_msg is msg

  def test_legacy_nack_uses_consumer_that_delivered_last_message(self, mocker) -> None:
    msg = mocker.MagicMock(name="msg_a")
    msg.data.return_value = b"a"
    consumer_a = mocker.MagicMock(name="consumer_a")
    consumer_a.receive.return_value = msg
    consumer_b = mocker.MagicMock(name="consumer_b")
    consumer_b.receive.side_effect = pulsar.Timeout("empty b")
    b, client = _connected(mocker)
    client.subscribe.side_effect = [consumer_a, consumer_b]
    assert b.pop("queue_a") == b"a"
    assert b.pop("queue_b") is None

    b.nack("queue_a")

    consumer_a.negative_acknowledge.assert_called_once_with(msg)
    consumer_b.negative_acknowledge.assert_not_called()
    assert b._last_delivery is None


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
    consumer.receive.side_effect = pulsar.Timeout("timed out")
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

  def test_cross_topic_ack_uses_consumer_that_popped_message(self, mocker) -> None:
    msg_a = mocker.MagicMock(name="msg_a")
    msg_a.data.return_value = b"a"
    msg_id_a = mocker.MagicMock(name="msg_id_a")
    msg_a.message_id.return_value = msg_id_a
    consumer_a = mocker.MagicMock(name="consumer_a")
    consumer_a.receive.return_value = msg_a

    msg_b = mocker.MagicMock(name="msg_b")
    msg_b.data.return_value = b"b"
    msg_b.message_id.return_value = mocker.MagicMock(name="msg_id_b")
    consumer_b = mocker.MagicMock(name="consumer_b")
    consumer_b.receive.return_value = msg_b

    b, client = _connected(mocker)
    client.subscribe.side_effect = [consumer_a, consumer_b]

    _, token_a = b.pop_with_ack("queue_a")
    b.pop_with_ack("queue_b")
    b.ack("queue_a", token=token_a)

    consumer_a.close.assert_not_called()
    consumer_a.acknowledge.assert_called_once_with(msg_id_a)
    consumer_b.acknowledge.assert_not_called()

  def test_cross_topic_nack_uses_consumer_that_popped_message(self, mocker) -> None:
    msg_a = mocker.MagicMock(name="msg_a")
    msg_a.data.return_value = b"a"
    msg_id_a = mocker.MagicMock(name="msg_id_a")
    msg_a.message_id.return_value = msg_id_a
    consumer_a = mocker.MagicMock(name="consumer_a")
    consumer_a.receive.return_value = msg_a

    msg_b = mocker.MagicMock(name="msg_b")
    msg_b.data.return_value = b"b"
    msg_b.message_id.return_value = mocker.MagicMock(name="msg_id_b")
    consumer_b = mocker.MagicMock(name="consumer_b")
    consumer_b.receive.return_value = msg_b

    b, client = _connected(mocker)
    client.subscribe.side_effect = [consumer_a, consumer_b]

    _, token_a = b.pop_with_ack("queue_a")
    b.pop_with_ack("queue_b")
    b.nack("queue_a", token=token_a)

    consumer_a.negative_acknowledge.assert_called_once_with(msg_id_a)
    consumer_b.negative_acknowledge.assert_not_called()

  def test_stale_token_does_not_ack_reconnected_topic_consumer(self, mocker) -> None:
    old_msg = mocker.MagicMock(name="old_msg")
    old_msg.data.return_value = b"old"
    old_msg.message_id.return_value = mocker.MagicMock(name="old_msg_id")
    old_consumer = mocker.MagicMock(name="old_consumer")
    old_consumer.receive.return_value = old_msg
    b, _ = _connected(mocker, subscribe=old_consumer)
    _, old_token = b.pop_with_ack("queue")
    b.disconnect()

    new_msg = mocker.MagicMock(name="new_msg")
    new_msg.data.return_value = b"new"
    new_msg.message_id.return_value = mocker.MagicMock(name="new_msg_id")
    new_consumer = mocker.MagicMock(name="new_consumer")
    new_consumer.receive.return_value = new_msg
    new_client = mocker.MagicMock(name="new_client")
    new_client.subscribe.return_value = new_consumer
    pulsar.Client.return_value = new_client
    b.connect()
    b.pop_with_ack("queue")

    b.ack("queue", token=old_token)

    old_consumer.acknowledge.assert_not_called()
    new_consumer.acknowledge.assert_not_called()

  def test_disconnect_during_receive_returns_stale_token_safely(self, mocker) -> None:
    msg = mocker.MagicMock(name="msg")
    msg.data.return_value = b"payload"
    msg.message_id.return_value = mocker.MagicMock(name="msg_id")
    consumer = mocker.MagicMock(name="consumer")
    b, _ = _connected(mocker, subscribe=consumer)

    def receive_and_disconnect(**_kwargs):
      b.disconnect()
      return msg

    consumer.receive.side_effect = receive_and_disconnect

    value, token = b.pop_with_ack("queue")
    b.ack("queue", token=token)

    assert value == b"payload"
    assert isinstance(token, _PulsarAckToken)
    assert token.consumer is consumer
    consumer.acknowledge.assert_not_called()
    assert token not in b._in_flight

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

  def test_ack_with_token_is_one_shot(self, mocker) -> None:
    """A successful token ack performs exactly one broker operation."""
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")
    b.ack("q", token=token)
    b.ack("q", token=token)
    assert consumer.acknowledge.call_count == 1
    assert b._in_flight == set()

  def test_ack_then_nack_has_one_terminal_broker_call(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")

    b.ack("q", token=token)
    b.nack("q", token=token)

    consumer.acknowledge.assert_called_once_with(token.message_id)
    consumer.negative_acknowledge.assert_not_called()

  def test_nack_then_ack_has_one_terminal_broker_call(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")

    b.nack("q", token=token)
    b.ack("q", token=token)

    consumer.negative_acknowledge.assert_called_once_with(token.message_id)
    consumer.acknowledge.assert_not_called()

  def test_failed_ack_is_retryable_then_terminal(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    consumer.acknowledge.side_effect = [RuntimeError("ack failed"), None]
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")

    with pytest.raises(QueueError, match="Failed to ack Pulsar message"):
      b.ack("q", token=token)
    assert token in b._in_flight

    b.ack("q", token=token)
    b.nack("q", token=token)

    assert consumer.acknowledge.call_count == 2
    consumer.negative_acknowledge.assert_not_called()
    assert token not in b._in_flight

  def test_failed_nack_is_retryable_then_terminal(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    consumer.negative_acknowledge.side_effect = [RuntimeError("nack failed"), None]
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")

    with pytest.raises(QueueError, match="Failed to nack Pulsar message"):
      b.nack("q", token=token)
    assert token in b._in_flight

    b.nack("q", token=token)
    b.ack("q", token=token)

    assert consumer.negative_acknowledge.call_count == 2
    consumer.acknowledge.assert_not_called()
    assert token not in b._in_flight

  def test_concurrent_ack_and_nack_claim_one_terminal_action(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    ack_entered = Event()
    release_ack = Event()

    def blocking_ack(_message_id) -> None:
      ack_entered.set()
      assert release_ack.wait(timeout=2.0)

    consumer.acknowledge.side_effect = blocking_ack
    b, _ = _connected(mocker, subscribe=consumer)
    _, token = b.pop_with_ack("q")
    errors: list[BaseException] = []

    def settle(action) -> None:
      try:
        action("q", token=token)
      except BaseException as error:  # pragma: no cover - assertion aid
        errors.append(error)

    ack_thread = Thread(target=settle, args=(b.ack,))
    nack_thread = Thread(target=settle, args=(b.nack,))
    ack_thread.start()
    assert ack_entered.wait(timeout=2.0)
    nack_thread.start()
    nack_thread.join(timeout=0.2)
    settlement_was_serialized = nack_thread.is_alive()
    release_ack.set()
    ack_thread.join(timeout=2.0)
    nack_thread.join(timeout=2.0)

    assert settlement_was_serialized
    assert not ack_thread.is_alive()
    assert not nack_thread.is_alive()
    assert errors == []
    consumer.acknowledge.assert_called_once_with(token.message_id)
    consumer.negative_acknowledge.assert_not_called()

  def test_token_pop_does_not_populate_legacy_settlement_slot(self, mocker) -> None:
    msg = mocker.MagicMock()
    msg.data.return_value = b"x"
    msg.message_id.return_value = mocker.MagicMock()
    consumer = mocker.MagicMock()
    consumer.receive.return_value = msg
    b, _ = _connected(mocker, subscribe=consumer)

    _, token = b.pop_with_ack("q")

    assert b._last_msg is None
    assert b._last_delivery is None
    b.ack("q", token=token)
    b.nack("q")
    consumer.acknowledge.assert_called_once_with(token.message_id)
    consumer.negative_acknowledge.assert_not_called()

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
    b.ack("q", token=token)
    consumer.acknowledge.assert_not_called()
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
  def test_queue_len_reports_unsupported(self, mocker) -> None:
    b, _ = _connected(mocker)
    with pytest.raises(NotImplementedError, match="admin API"):
      b.queue_len("queue1")

  def test_unsupported_depth_keeps_scheduler_conservative(self) -> None:
    backend = _make_backend()
    queue = MagicMock(name="BackendQueue")
    queue.__len__.side_effect = lambda: backend.queue_len("queue1")
    queue.pop.return_value = None
    scheduler = BackendScheduler(
      connection_manager=MagicMock(name="ConnectionManager"),
      backpressure_pause_at=1,
    )
    scheduler._queue = queue

    assert scheduler.has_pending_requests() is True
    assert scheduler.next_request() is None
    queue.pop.assert_called_once_with(timeout=0)

  def test_clear_queue_reports_unsupported(self, mocker) -> None:
    b, _ = _connected(mocker)

    with pytest.raises(QueueError) as exc_info:
      b.clear_queue("queue1")

    assert exc_info.value.queue_name == "queue1"
    assert exc_info.value.operation == "clear_queue"
    assert "not supported" in str(exc_info.value)


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
    Client kwargs include the SDK's TLS-prefixed insecure flag."""
    b = _make_backend(
      service_url="pulsar+ssl://broker:6651",
      allow_insecure_connection=False,
    )
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    b.connect()
    _, kwargs = pulsar.Client.call_args.args, pulsar.Client.call_args.kwargs
    assert kwargs["tls_allow_insecure_connection"] is False
    assert kwargs["tls_validate_hostname"] is True
    # trust_certs is NOT passed when unset (the bug was gating on this).
    assert "tls_trust_certs_file_path" not in kwargs

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
    assert kwargs.get("tls_allow_insecure_connection") is True
    assert kwargs.get("tls_validate_hostname") is True
    assert "tls_trust_certs_file_path" not in kwargs

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
    assert kwargs.get("tls_allow_insecure_connection") is True
    assert kwargs.get("tls_validate_hostname") is True
    assert kwargs.get("tls_trust_certs_file_path") == "/etc/ssl/ca.pem"

  def test_ssl_forwards_explicit_hostname_validation_opt_out(self, mocker) -> None:
    """The public compatibility setting controls the real SDK keyword."""
    b = _make_backend(
      service_url="pulsar+ssl://broker:6651",
      tls_validate_hostname=False,
    )
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    b.connect()

    assert pulsar.Client.call_args.kwargs["tls_validate_hostname"] is False

  def test_non_ssl_url_omits_tls_kwargs(self, mocker) -> None:
    """pulsar:// (plaintext) doesn't pass either TLS field."""
    b = _make_backend(
      service_url="pulsar://broker:6650",
      allow_insecure_connection=True,  # ignored — not an ssl url
      tls_trust_certs_file="/tmp/plaintext-ignored.pem",
      tls_validate_hostname=False,
    )
    mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
    b.connect()
    kwargs = pulsar.Client.call_args.kwargs
    assert "tls_allow_insecure_connection" not in kwargs
    assert "tls_validate_hostname" not in kwargs
    assert "tls_trust_certs_file_path" not in kwargs


def test_locked_pulsar_sdk_tls_keyword_contract() -> None:
  """The installed real client must expose every TLS keyword we forward."""
  script = "\n".join(
    (
      "import inspect",
      "import pulsar",
      "names = inspect.signature(pulsar.Client).parameters",
      "assert 'tls_allow_insecure_connection' in names",
      "assert 'tls_trust_certs_file_path' in names",
      "assert 'tls_validate_hostname' in names",
      "assert 'allow_insecure_connection' not in names",
      "assert 'tls_trust_certs_file' not in names",
    )
  )

  result = subprocess.run(
    [sys.executable, "-c", script],
    capture_output=True,
    text=True,
    check=False,
  )

  assert result.returncode == 0, result.stderr


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
