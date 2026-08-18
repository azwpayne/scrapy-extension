"""Tests for PulsarBackend (subsystem ③) with mocked network seams.

The test dependency group supplies the real binding so enums, exceptions, and
constructor signatures stay faithful; individual tests patch ``Client`` or
``AuthenticationToken`` without replacing the SDK module.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import traceback
from threading import Event, Lock, Thread, current_thread
from types import TracebackType
from typing import Any
from unittest.mock import MagicMock

import pulsar
import pytest

import scrapy_extension.backends.pulsar as pulsar_mod
from scrapy_extension.backends.base import (
    BackendType,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.backends.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    wrap_queue_backend,
)
from scrapy_extension.backends.pulsar import (
    PulsarBackend,
    _PulsarAckToken,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.exceptions._redaction import not_implemented_error_boundary
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings import PulsarMode, PulsarSettings


def _assert_value_is_redacted(
    value: object, marker: str, seen: set[int] | None = None
) -> None:
    """Walk a bounded public exception graph without trusting ``repr``."""
    if seen is None:
        seen = set()
    value_id = id(value)
    if value_id in seen:
        return
    seen.add(value_id)
    if isinstance(value, str):
        assert marker not in value
        return
    if isinstance(value, bytes):
        assert marker.encode() not in value
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_value_is_redacted(key, marker, seen)
            _assert_value_is_redacted(item, marker, seen)
        return
    if isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_value_is_redacted(item, marker, seen)
        return
    try:
        attributes = vars(value)
    except TypeError:
        return
    _assert_value_is_redacted(attributes, marker, seen)


def _assert_capability_error_is_redacted(error: BaseException, marker: str) -> None:
    """Assert that a public static capability error has no backend graph."""
    assert marker not in str(error)
    assert marker not in repr(error.args)
    assert marker not in repr(error.__dict__)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in "".join(traceback.format_exception(error))

    trace = error.__traceback__
    while trace is not None:
        frame = trace.tb_frame
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            assert marker not in repr(frame.f_locals)
            for value in frame.f_locals.values():
                _assert_value_is_redacted(value, marker)
        trace = trace.tb_next


_CONNECTED_BACKENDS: list[PulsarBackend] = []


def _make_backend(**overrides) -> PulsarBackend:
    return PulsarBackend(PulsarSettings(**overrides))


@pytest.fixture(autouse=True)
def _disconnect_receive_pumps_after_test():
    """Keep receive-worker lifetime inside the unit test that created it."""
    yield
    while _CONNECTED_BACKENDS:
        backend = _CONNECTED_BACKENDS.pop()
        try:
            backend.disconnect()
        except BaseException:
            pass


def _connected(mocker, **client_children):
    """Build a connected backend; ``client_children`` pre-stubs client attrs."""
    b = _make_backend()
    client = mocker.MagicMock()
    for attr, val in client_children.items():
        getattr(client, attr).return_value = val
    mocker.patch.object(pulsar, "Client", return_value=client)
    b.connect()
    _CONNECTED_BACKENDS.append(b)
    return b, client


def _wait_for_pump_subscription(backend: PulsarBackend, queue_name: str) -> Any:
    """Wait until a zero-time poll's consumer is published and receiving."""
    pump = backend._receive_pumps[f"scrapy-{queue_name}"]
    assert pump.receive_started.wait(timeout=0.5)
    return pump


class _ExceptionContextProbe(logging.Handler):
    """Capture exception state available to synchronous backend handlers."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.contexts: list[tuple[object | None, object | None, object | None]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.contexts.append(sys.exc_info())


class _InterruptOnLifecycleEntry:
    """Lock proxy that raises at a deterministic lifecycle-lock entry."""

    def __init__(
        self,
        lock: Any,
        interrupt_on_entry: int,
        thread_name_prefix: str | None = None,
    ) -> None:
        self._lock = lock
        self._interrupt_on_entry = interrupt_on_entry
        self._thread_name_prefix = thread_name_prefix
        self._entries = 0
        self._interrupted = False

    def acquire(self) -> bool:
        is_target = (
            self._thread_name_prefix is None
            or current_thread().name.startswith(self._thread_name_prefix)
        )
        if is_target:
            self._entries += 1
        if (
            is_target
            and not self._interrupted
            and self._entries == self._interrupt_on_entry
        ):
            self._interrupted = True
            raise KeyboardInterrupt("lifecycle publication interrupted")
        return self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> _InterruptOnLifecycleEntry:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.release()


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

    def test_connect_diagnostic_hides_service_url(self, mocker) -> None:
        marker = "pulsar-service-url-log-marker"
        b = _make_backend(
            service_url=f"pulsar://{marker}.example:6650",
            allow_remote_plaintext=True,
        )
        client = mocker.MagicMock()
        mocker.patch.object(pulsar, "Client", return_value=client)
        logger_debug = mocker.patch("scrapy_extension.backends.pulsar.logger.debug")

        b.connect()

        logger_debug.assert_called_once_with(
            "Connected to Pulsar in %s mode", PulsarMode.STANDALONE.value
        )
        assert marker not in repr(logger_debug.call_args_list)

    def test_connect_failure_raises_connection_error(self, mocker) -> None:
        b = _make_backend()
        mocker.patch.object(pulsar, "Client", side_effect=RuntimeError("boom"))
        with pytest.raises(BackendConnectionError):
            b.connect()
        assert b.is_connected() is False

    def test_failed_connect_cleanup_log_has_no_active_driver_exception(
        self, mocker
    ) -> None:
        """Candidate cleanup telemetry runs after the causal driver suite exits."""

        class _FailOnIncrement:
            def __iadd__(self, _other: object) -> object:
                raise RuntimeError("round48-pulsar-connect-marker")

        marker = "round48-pulsar-close-marker"
        b = _make_backend()
        b._lifecycle_generation = _FailOnIncrement()  # type: ignore[assignment]
        client = mocker.MagicMock(name="candidate")
        client.close.side_effect = RuntimeError(marker)
        mocker.patch.object(pulsar, "Client", return_value=client)
        probe = _ExceptionContextProbe()
        logger = pulsar_mod.logger
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(probe)
        try:
            with pytest.raises(BackendConnectionError):
                b.connect()
        finally:
            logger.removeHandler(probe)
            logger.setLevel(old_level)

        assert [record.getMessage() for record in probe.records] == [
            "Failed to close Pulsar connect candidate"
        ]
        assert probe.contexts == [(None, None, None)]
        assert marker not in repr(probe.records)

    @pytest.mark.parametrize(
        "diagnostic_error",
        [RuntimeError("logger extension failed"), KeyboardInterrupt(), SystemExit()],
    )
    def test_post_publish_logger_failure_keeps_live_client(
        self, mocker, diagnostic_error: BaseException
    ) -> None:
        """R106: post-publication diagnostics cannot abort a live generation."""
        b = _make_backend()
        client = mocker.MagicMock(name="candidate")
        mocker.patch.object(pulsar, "Client", return_value=client)
        mocker.patch(
            "scrapy_extension.backends.pulsar.logger.debug",
            side_effect=diagnostic_error,
        )

        b.connect()

        assert b._client is client
        assert b._connection_snapshot is not None
        assert b._lifecycle_generation == 1
        client.close.assert_not_called()

        b.disconnect()
        client.close.assert_called_once_with()

    def test_connect_baseexception_after_client_build_closes_client(
        self, mocker
    ) -> None:
        """R18-B: a Ctrl+C after pulsar.Client(...) but before publish closes the client.

        ``pulsar.Client`` starts C++ background IO/service threads in its constructor.
        A Ctrl+C delivered between the constructor return (line 430) and the publish
        (line 432) escapes the ``except Exception`` arm without closing the client;
        it was never published to ``self._client`` so ``disconnect()`` cannot reach
        it -> the C++ bg threads + lazy broker FD leak to interpreter shutdown. The
        increment at line 431 sits in that window, so we make it raise KeyboardInterrupt
        after the client is built. Mirror the R16-A/R17 connect() BaseException contract
        (pulsar is the last connect()-capable backend to gain the arm).
        """

        class _InterruptOnIncrement:
            def __add__(self, other):
                raise KeyboardInterrupt

            def __iadd__(self, other):
                raise KeyboardInterrupt

        b = _make_backend()
        client = mocker.MagicMock()
        mocker.patch.object(pulsar, "Client", return_value=client)
        # Line 431 (`self._lifecycle_generation += 1`) is the post-build, pre-publish window.
        b._lifecycle_generation = _InterruptOnIncrement()  # type: ignore[assignment]

        with pytest.raises(KeyboardInterrupt):
            b.connect()

        # The built-but-un-published client is closed (no thread/FD leak).
        client.close.assert_called_once()
        assert b._client is None
        assert b.is_connected() is False

    def test_connect_baseexception_during_kwargs_setup_reraises_original(
        self, mocker
    ) -> None:
        """R19-B: a Ctrl+C during kwargs-setup (before the client hoist) re-raises
        the original BaseException, not UnboundLocalError.

        R18-B hoisted ``client: Any = None`` INSIDE the try, AFTER the kwargs block —
        which calls ``pulsar.AuthenticationToken()`` (a C++-backed constructor). A
        Ctrl+C during that call reached the ``except BaseException`` arm before the
        hoist ran, so the arm referenced an unbound ``client`` -> ``UnboundLocalError``,
        masking the original ``KeyboardInterrupt``. The hoist must sit BEFORE the try
        (mirror rabbitmq ``_open_prepared_channel``, which hoists ``channel`` before
        its try). Requires auth_token so the AuthenticationToken call executes.
        """
        b = _make_backend(
            service_url="pulsar+ssl://localhost:6651", auth_token="secret-token"
        )
        mocker.patch.object(
            pulsar, "AuthenticationToken", side_effect=KeyboardInterrupt
        )

        # The original KeyboardInterrupt must propagate — NOT UnboundLocalError from
        # the arm referencing an unbound `client`.
        with pytest.raises(KeyboardInterrupt):
            b.connect()

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
        try:
            assert backend.pop("queue") is None
            _wait_for_pump_subscription(backend, "queue")

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
        finally:
            backend.disconnect()

    def test_connection_snapshot_repr_redacts_auth_token(self) -> None:
        secret = "snapshot-secret"
        settings = PulsarSettings(
            service_url="pulsar+ssl://broker:6651",
            auth_token=secret,  # type: ignore[arg-type]
        )

        snapshot = PulsarBackend(settings)._capture_connection_snapshot()

        assert secret not in repr(snapshot)

    def test_startup_error_traceback_does_not_echo_driver_secrets(self, mocker) -> None:
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
        assert exc_info.value.__context__ is None

    def test_connect_is_idempotent_while_connected(self, mocker) -> None:
        consumer = mocker.MagicMock(name="consumer")
        consumer.receive.side_effect = pulsar.Timeout("empty")
        b, old_client = _connected(mocker, subscribe=consumer)
        b.pop("queue")
        _wait_for_pump_subscription(b, "queue")
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

    def test_in_flight_overflow_warning_flag_resets_on_disconnect(self, mocker) -> None:
        """R90: the one-shot in-flight-overflow warning flag must reset on disconnect,
        mirroring R89/rabbitmq (d2269be). _in_flight is cleared on disconnect (room for a
        chronic ack-leak to recur), so the flag must reset alongside it -- a reconnect then
        re-warns on the next overflow, not stay latched for the process lifetime."""
        b, _ = _connected(mocker)
        b._in_flight_overflow_warned = True  # a prior overflow warned

        b.disconnect()

        assert b._in_flight_overflow_warned is False

    def test_in_flight_overflow_warning_flag_resets_on_abort_failed_connect(
        self, mocker
    ) -> None:
        """R90: the connect-failure rollback (_abort_failed_connect) also resets the flag,
        so a reconnect attempt after a failed connect re-enables the warning (distinct from
        the disconnect path)."""
        b = _make_backend()
        client = mocker.MagicMock()
        b._client = client
        b._lifecycle_generation = 5
        b._in_flight_overflow_warned = True

        b._abort_failed_connect(client, published_generation=5)

        assert b._in_flight_overflow_warned is False

    def test_disconnect_closes_all_topic_consumers(self, mocker) -> None:
        consumer_a = mocker.MagicMock(name="consumer_a")
        consumer_a.receive.side_effect = pulsar.Timeout("empty a")
        consumer_b = mocker.MagicMock(name="consumer_b")
        consumer_b.receive.side_effect = pulsar.Timeout("empty b")
        b, client = _connected(mocker)
        client.subscribe.side_effect = [consumer_a, consumer_b]
        b.pop("queue_a")
        b.pop("queue_b")
        _wait_for_pump_subscription(b, "queue_a")
        _wait_for_pump_subscription(b, "queue_b")

        b.disconnect()

        consumer_a.close.assert_called_once_with()
        consumer_b.close.assert_called_once_with()
        assert b._consumers == {}
        assert b._consumer is None
        assert b._subscribed_topic is None

    def test_disconnect_closes_every_handle_after_baseexception(self, mocker) -> None:
        b, client = _connected(mocker)
        consumer_a = mocker.MagicMock(name="consumer_a")
        consumer_b = mocker.MagicMock(name="consumer_b")
        producer_a = mocker.MagicMock(name="producer_a")
        producer_b = mocker.MagicMock(name="producer_b")
        first = KeyboardInterrupt()
        consumer_a.close.side_effect = first
        producer_a.close.side_effect = SystemExit(2)
        b._consumers = {"topic_a": consumer_a, "topic_b": consumer_b}
        b._producers = {"topic_a": producer_a, "topic_b": producer_b}

        with pytest.raises(KeyboardInterrupt) as raised:
            b.disconnect()

        assert raised.value is first
        for handle in (consumer_a, consumer_b, producer_a, producer_b, client):
            handle.close.assert_called_once_with()
        assert b._consumers == {}
        assert b._producers == {}
        assert b._client is None

    def test_disconnect_preserves_foreign_direct_close_control_graph(
        self, mocker
    ) -> None:
        marker = "caller-owned-direct-close-context-marker"
        control_error = SystemExit("direct close exit")
        cause_error = ValueError("caller-owned direct close cause")
        contexts: list[RuntimeError] = []
        direct_consumer = mocker.MagicMock(name="direct_consumer")

        def close_with_foreign_graph() -> None:
            try:
                raise RuntimeError(marker)
            except RuntimeError as context_error:
                contexts.append(context_error)
                raise control_error from cause_error

        direct_consumer.close.side_effect = close_with_foreign_graph
        b, client = _connected(mocker)
        b._consumer = direct_consumer

        with pytest.raises(SystemExit) as captured:
            b.disconnect()

        assert captured.value is control_error
        assert control_error.__cause__ is cause_error
        assert control_error.__context__ is contexts[0]
        assert control_error.__suppress_context__ is True
        assert any(
            frame.f_code.co_name == "close_with_foreign_graph"
            for frame, _line_number in traceback.walk_tb(control_error.__traceback__)
        )
        assert b._consumer is None
        assert b._client is None
        direct_consumer.close.assert_called_once_with()
        client.close.assert_called_once_with()

    def test_disconnect_ignores_diagnostic_control_error_after_close_error(
        self, mocker
    ) -> None:
        """R96: logger failures must not stop suppressed-close sibling cleanup."""
        b, client = _connected(mocker)
        consumer_a = mocker.MagicMock(name="consumer_a")
        consumer_b = mocker.MagicMock(name="consumer_b")
        consumer_a.close.side_effect = RuntimeError("ordinary close failure")
        b._consumers = {"topic_a": consumer_a, "topic_b": consumer_b}
        mocker.patch(
            "scrapy_extension.backends.pulsar.logger.debug",
            side_effect=KeyboardInterrupt("diagnostic interrupted"),
        )

        b.disconnect()

        for handle in (consumer_a, consumer_b, client):
            handle.close.assert_called_once_with()

    def test_disconnect_cleanup_log_has_no_active_close_exception(self, mocker) -> None:
        marker = "round48-pulsar-close-marker"
        b, client = _connected(mocker)
        client.close.side_effect = RuntimeError(marker)
        probe = _ExceptionContextProbe()
        logger = pulsar_mod.logger
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(probe)
        try:
            b.disconnect()
        finally:
            logger.removeHandler(probe)
            logger.setLevel(old_level)

        assert [record.getMessage() for record in probe.records] == [
            "Suppressed pulsar cleanup error"
        ]
        assert probe.contexts == [(None, None, None)]
        assert marker not in repr(probe.records)

    def test_stale_producer_close_keeps_connection_changed_queue_error(
        self, mocker
    ) -> None:
        """R96: a diagnostic interruption cannot replace stale-candidate QueueError."""
        b, client = _connected(mocker)
        producer = mocker.MagicMock(name="stale_producer")
        producer.close.side_effect = RuntimeError("ordinary close failure")

        def create_stale_producer(_topic: str):
            with b._lifecycle_lock:
                b._client = None
                b._lifecycle_generation += 1
            return producer

        client.create_producer.side_effect = create_stale_producer
        mocker.patch(
            "scrapy_extension.backends.pulsar.logger.debug",
            side_effect=KeyboardInterrupt("diagnostic interrupted"),
        )

        with pytest.raises(QueueError, match="connection changed"):
            b._producer_for("topic")

        producer.close.assert_called_once_with()

    def test_reconnect_during_disconnect_does_not_close_new_client(
        self, mocker
    ) -> None:
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
        _wait_for_pump_subscription(b, "queue")
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

    def test_push_lifecycle_interrupt_after_creation_closes_candidate(
        self, mocker
    ) -> None:
        """R74: a private producer is closed when publication is interrupted."""
        producer = mocker.MagicMock(name="unpublished_producer")
        producer.close.side_effect = SystemExit("cleanup must not mask interrupt")
        b, _ = _connected(mocker, create_producer=producer)
        b._lifecycle_lock = _InterruptOnLifecycleEntry(b._lifecycle_lock, 2)

        with pytest.raises(KeyboardInterrupt, match="publication interrupted"):
            b.push("queue", b"payload")

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
        _wait_for_pump_subscription(b, "queue1")
        assert b._last_msg is None

    def test_empty_timeout_diagnostic_hides_queue_and_driver_text(self, mocker) -> None:
        marker = "pulsar-timeout-log-marker"
        consumer = mocker.MagicMock()
        consumer.receive.side_effect = pulsar.Timeout(marker)
        b, _ = _connected(mocker, subscribe=consumer)
        logger_debug = mocker.patch("scrapy_extension.backends.pulsar.logger.debug")

        assert b.pop(f"{marker}-queue") is None
        pump = b._receive_pumps[f"scrapy-{marker}-queue"]
        assert pump.receive_started.wait(timeout=0.5)
        assert b.pop(f"{marker}-queue", timeout=0.02) is None

        assert logger_debug.call_count >= 1
        assert all(
            call.args == ("Pulsar receive returned no message.",)
            for call in logger_debug.call_args_list
        )
        assert marker not in repr(logger_debug.call_args_list)

    def test_empty_timeout_log_has_no_active_driver_exception(self, mocker) -> None:
        marker = "round48-pulsar-timeout-marker"
        consumer = mocker.MagicMock()
        consumer.receive.side_effect = pulsar.Timeout(marker)
        b, _ = _connected(mocker, subscribe=consumer)
        probe = _ExceptionContextProbe()
        logger = pulsar_mod.logger
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(probe)
        try:
            assert b.pop("queue1") is None
            pump = b._receive_pumps["scrapy-queue1"]
            assert pump.receive_started.wait(timeout=0.5)
            assert b.pop("queue1", timeout=0.02) is None
        finally:
            logger.removeHandler(probe)
            logger.setLevel(old_level)

        assert probe.records
        assert all(
            record.getMessage() == "Pulsar receive returned no message."
            for record in probe.records
        )
        assert all(context == (None, None, None) for context in probe.contexts)
        assert marker not in repr(probe.records)

    @pytest.mark.parametrize(
        "diagnostic_error",
        [RuntimeError("logger extension failed"), KeyboardInterrupt(), SystemExit()],
    )
    def test_empty_timeout_logger_failure_preserves_empty_result(
        self, mocker, diagnostic_error: BaseException
    ) -> None:
        """R118: timeout diagnostics cannot turn an empty poll into a failure."""
        consumer = mocker.MagicMock()
        consumer.receive.side_effect = pulsar.Timeout("timed out")
        b, _ = _connected(mocker, subscribe=consumer)
        mocker.patch(
            "scrapy_extension.backends.pulsar.logger.debug",
            side_effect=diagnostic_error,
        )

        assert b.pop("queue1") is None
        _wait_for_pump_subscription(b, "queue1")
        assert b._last_msg is None

    def test_pop_wraps_non_timeout_receive_failure(self, mocker) -> None:
        consumer = mocker.MagicMock()
        failure = RuntimeError("broker disconnected")
        consumer.receive.side_effect = failure
        b, _ = _connected(mocker, subscribe=consumer)

        with pytest.raises(QueueError) as exc_info:
            b.pop("queue1", timeout=1.0)

        assert exc_info.value.queue_name is None
        assert exc_info.value.operation == "pop"
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None

    def test_pop_reuses_consumer_for_same_topic(self, mocker) -> None:
        consumer = mocker.MagicMock()
        consumer.receive.side_effect = pulsar.Timeout("none")
        b, client = _connected(mocker, subscribe=consumer)
        b.pop("queue1")
        _wait_for_pump_subscription(b, "queue1")
        b.pop("queue1")
        client.subscribe.assert_called_once()

    def test_pop_resubscribes_on_topic_change(self, mocker) -> None:
        consumer = mocker.MagicMock()
        consumer.receive.side_effect = pulsar.Timeout("none")
        b, client = _connected(mocker, subscribe=consumer)
        b.pop("queue1")
        b.pop("queue2")
        _wait_for_pump_subscription(b, "queue1")
        _wait_for_pump_subscription(b, "queue2")
        assert client.subscribe.call_count == 2

    def test_pop_topic_subscribe_failure_preserves_cached_consumer(
        self, mocker
    ) -> None:
        """A failed subscription for one topic must not break another topic."""
        consumer = mocker.MagicMock()
        consumer.receive.side_effect = pulsar.Timeout("none")
        b, client = _connected(mocker, subscribe=consumer)
        b.pop("queue1", timeout=1.0)
        client.subscribe.side_effect = RuntimeError("subscribe failed")
        with pytest.raises(QueueError):
            b.pop("queue2", timeout=1.0)

        assert b._consumer is consumer
        assert b._subscribed_topic == "scrapy-queue1"
        assert b._consumers == {"scrapy-queue1": consumer}
        assert b.pop("queue1") is None
        assert consumer.receive.call_count >= 1

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
        pump = b._receive_pumps["scrapy-queue"]
        assert pump.receive_started.wait(timeout=0.5)
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
        b._receive_shutdown_timeout = 0.05
        results: list[bytes | None] = []

        pop_thread = Thread(target=lambda: results.append(b.pop("queue")))
        pop_thread.start()
        assert subscribe_started.wait(timeout=2.0)
        pop_thread.join(timeout=0.5)
        assert results == [None]

        pump = b._receive_pumps["scrapy-queue"]
        b.disconnect()
        release_subscribe.set()
        assert pump.stopped.wait(timeout=0.5)

        consumer.close.assert_called_once_with()
        assert b._consumers == {}
        assert b._consumer is None

    def test_pop_lifecycle_interrupt_after_creation_closes_candidate(
        self, mocker
    ) -> None:
        """R74: a private consumer is closed when publication is interrupted."""
        consumer = mocker.MagicMock(name="unpublished_consumer")
        consumer.close.side_effect = SystemExit("cleanup must not mask interrupt")
        b, _ = _connected(mocker, subscribe=consumer)
        b._lifecycle_lock = _InterruptOnLifecycleEntry(
            b._lifecycle_lock,
            1,
            thread_name_prefix="scrapy-pulsar-receive-",
        )

        with pytest.raises(KeyboardInterrupt, match="publication interrupted"):
            b.pop("queue", timeout=1.0)

        consumer.close.assert_called_once_with()
        consumer.receive.assert_not_called()
        assert b._consumers == {}
        assert b._consumer is None


class TestPulsarAckNack:
    def test_ack_calls_acknowledge(self, mocker) -> None:
        msg = mocker.MagicMock()
        msg.data.return_value = b"x"
        consumer = mocker.MagicMock()
        consumer.receive.return_value = msg
        b, _ = _connected(mocker, subscribe=consumer)
        b.pop("queue1", timeout=1.0)
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
        b.pop("queue1", timeout=1.0)

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
        assert b.pop("queue_a", timeout=1.0) == b"a"
        assert b.pop("queue_b") is None
        _wait_for_pump_subscription(b, "queue_b")

        b.ack("queue_a")

        consumer_a.acknowledge.assert_called_once_with(msg)
        consumer_b.acknowledge.assert_not_called()

    def test_nack_calls_negative_acknowledge(self, mocker) -> None:
        msg = mocker.MagicMock()
        msg.data.return_value = b"x"
        consumer = mocker.MagicMock()
        consumer.receive.return_value = msg
        b, _ = _connected(mocker, subscribe=consumer)
        b.pop("queue1", timeout=1.0)
        b.nack("queue1")
        consumer.negative_acknowledge.assert_called_once_with(msg)
        assert b._last_msg is None

    def test_nack_unknown_token_does_not_nack_legacy_message(self, mocker) -> None:
        msg = mocker.MagicMock()
        msg.data.return_value = b"x"
        consumer = mocker.MagicMock()
        consumer.receive.return_value = msg
        b, _ = _connected(mocker, subscribe=consumer)
        b.pop("queue1", timeout=1.0)

        b.nack("queue1", token=object())

        consumer.negative_acknowledge.assert_not_called()
        assert b._last_msg is msg

    def test_legacy_nack_uses_consumer_that_delivered_last_message(
        self, mocker
    ) -> None:
        msg = mocker.MagicMock(name="msg_a")
        msg.data.return_value = b"a"
        consumer_a = mocker.MagicMock(name="consumer_a")
        consumer_a.receive.return_value = msg
        consumer_b = mocker.MagicMock(name="consumer_b")
        consumer_b.receive.side_effect = pulsar.Timeout("empty b")
        b, client = _connected(mocker)
        client.subscribe.side_effect = [consumer_a, consumer_b]
        assert b.pop("queue_a", timeout=1.0) == b"a"
        assert b.pop("queue_b") is None
        _wait_for_pump_subscription(b, "queue_b")

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
        _wait_for_pump_subscription(b, "queue1")
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

        _, token_a = b.pop_with_ack("queue_a", timeout=1.0)
        b.pop_with_ack("queue_b", timeout=1.0)
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

        _, token_a = b.pop_with_ack("queue_a", timeout=1.0)
        b.pop_with_ack("queue_b", timeout=1.0)
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
        _, old_token = b.pop_with_ack("queue", timeout=1.0)
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
        b.pop_with_ack("queue", timeout=1.0)

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

        value, token = b.pop_with_ack("queue", timeout=1.0)
        b.ack("queue", token=token)

        assert value is None
        assert token is None
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
        _, token = b.pop_with_ack("q", timeout=1.0)
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
        _, token = b.pop_with_ack("q", timeout=1.0)
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
        _, token = b.pop_with_ack("q", timeout=1.0)

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
        _, token = b.pop_with_ack("q", timeout=1.0)

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
        _, token = b.pop_with_ack("q", timeout=1.0)

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
        _, token = b.pop_with_ack("q", timeout=1.0)

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
        _, token = b.pop_with_ack("q", timeout=1.0)
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

        _, token = b.pop_with_ack("q", timeout=1.0)

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
        _, token = b.pop_with_ack("q", timeout=1.0)
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
        _, token = b.pop_with_ack("q", timeout=1.0)
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
        b.pop_with_ack("q", timeout=1.0)
        b.pop_with_ack("q", timeout=1.0)
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
        value = b.pop("q", timeout=1.0)
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

    def test_queue_len_rebuilds_static_capability_error_without_backend_graph(
        self,
    ) -> None:
        marker = "round44-pulsar-depth-private-marker"
        backend = _make_backend(
            service_url=f"pulsar://{marker}.example:6650",
            allow_remote_plaintext=True,
        )

        with pytest.raises(NotImplementedError) as exc_info:
            backend.queue_len(marker)

        error = exc_info.value
        assert type(error) is NotImplementedError
        assert str(error) == (
            "Pulsar queue depth requires the admin API, which is not configured"
        )
        _assert_capability_error_is_redacted(error, marker)

    def test_queue_len_proxy_keeps_static_capability_error_and_breaker_neutral(
        self,
    ) -> None:
        marker = "round44-pulsar-depth-proxy-marker"
        backend = _make_backend(
            service_url=f"pulsar://{marker}.example:6650",
            allow_remote_plaintext=True,
        )
        breaker = CircuitBreaker("pulsar-capability-depth", failure_threshold=1)
        proxy = wrap_queue_backend(backend, breaker)

        with pytest.raises(NotImplementedError) as exc_info:
            proxy.queue_len(marker)

        assert str(exc_info.value) == (
            "Pulsar queue depth requires the admin API, which is not configured"
        )
        assert breaker.state is BreakerState.CLOSED
        assert breaker.failure_count == 0
        _assert_capability_error_is_redacted(exc_info.value, marker)

    def test_queue_len_keeps_invalid_name_validation_outside_capability_boundary(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="Invalid queue_name"):
            _make_backend().queue_len("invalid queue name")

    @pytest.mark.parametrize(
        "error",
        (
            RuntimeError("unknown capability marker"),
            KeyboardInterrupt("control-flow capability marker"),
        ),
    )
    def test_capability_boundary_preserves_unknown_and_control_flow_errors(
        self, error: BaseException
    ) -> None:
        @not_implemented_error_boundary("safe capability error")
        def operation(_queue_name: str) -> None:
            raise error

        with pytest.raises(type(error)) as exc_info:
            operation("queue")

        assert exc_info.value is error

    def test_capability_boundary_preserves_not_implemented_subclasses(self) -> None:
        class PluginCapabilityError(NotImplementedError):
            pass

        error = PluginCapabilityError("plugin capability marker")

        @not_implemented_error_boundary("safe capability error")
        def operation(_queue_name: str) -> None:
            raise error

        with pytest.raises(PluginCapabilityError) as exc_info:
            operation("queue")

        assert exc_info.value is error

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

        assert exc_info.value.queue_name is None
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
        """The explicit insecure compatibility option remains available on loopback."""
        b = _make_backend(
            service_url="pulsar+ssl://localhost:6651",
            allow_insecure_connection=True,
        )
        mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
        b.connect()
        kwargs = pulsar.Client.call_args.kwargs
        assert kwargs.get("tls_allow_insecure_connection") is True
        assert kwargs.get("tls_validate_hostname") is True
        assert "tls_trust_certs_file_path" not in kwargs

    def test_ssl_passes_both_when_both_set(self, mocker) -> None:
        """Loopback TLS forwards both explicit compatibility settings."""
        b = _make_backend(
            service_url="pulsar+ssl://localhost:6651",
            allow_insecure_connection=True,
            tls_trust_certs_file="/etc/ssl/ca.pem",
        )
        mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
        b.connect()
        kwargs = pulsar.Client.call_args.kwargs
        assert kwargs.get("tls_allow_insecure_connection") is True
        assert kwargs.get("tls_validate_hostname") is True
        assert kwargs.get("tls_trust_certs_file_path") == "/etc/ssl/ca.pem"

    def test_ssl_forwards_loopback_hostname_validation_opt_out(self, mocker) -> None:
        """The explicit compatibility setting remains available only on loopback."""
        b = _make_backend(
            service_url="pulsar+ssl://localhost:6651",
            tls_validate_hostname=False,
        )
        mocker.patch.object(pulsar, "Client", return_value=mocker.MagicMock())
        b.connect()

        assert pulsar.Client.call_args.kwargs["tls_validate_hostname"] is False

    @pytest.mark.parametrize(
        ("setting_name", "value"),
        [
            ("allow_insecure_connection", True),
            ("tls_trust_certs_file", "/tmp/plaintext-ignored.pem"),
            ("tls_validate_hostname", False),
        ],
    )
    def test_non_ssl_url_rejects_ignored_tls_intent(
        self, mocker, setting_name: str, value: object
    ) -> None:
        """TLS-only settings cannot be silently discarded on pulsar://."""
        client = mocker.patch.object(pulsar, "Client")

        with pytest.raises(ConfigurationError) as exc_info:
            _make_backend(**{setting_name: value})

        assert exc_info.value.setting_name == setting_name
        client.assert_not_called()


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


def test_locked_pulsar_sdk_consumer_type_contract() -> None:
    """The public Key_Shared setting maps to the real SDK's KeyShared member."""
    script = "\n".join(
        (
            "import pulsar",
            "from scrapy_extension.backends.pulsar import _consumer_type",
            "assert hasattr(pulsar.ConsumerType, 'KeyShared')",
            "assert not hasattr(pulsar.ConsumerType, 'Key_Shared')",
            "assert _consumer_type('Key_Shared') is pulsar.ConsumerType.KeyShared",
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
            _PulsarAckToken(message_id=object(), topic=f"t{i}")
            for i in range(_MAX_IN_FLIGHT)
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

    @pytest.mark.parametrize(
        "diagnostic_error",
        [
            RuntimeError("diagnostic failure"),
            KeyboardInterrupt("diagnostic interruption"),
            SystemExit("diagnostic exit"),
        ],
    )
    def test_overflow_warning_failure_preserves_delivery_and_ack(
        self, mocker, diagnostic_error
    ) -> None:
        """R124: post-delivery diagnostics cannot prevent the token's broker ack."""
        from scrapy_extension.backends.pulsar import _MAX_IN_FLIGHT

        msg = mocker.MagicMock(name="delivered_message")
        msg.data.return_value = b"hello"
        msg_id = mocker.MagicMock(name="delivered_message_id")
        msg.message_id.return_value = msg_id
        consumer = mocker.MagicMock(name="delivering_consumer")
        consumer.receive.return_value = msg
        b, _client = _connected(mocker, subscribe=consumer)
        b._in_flight = {
            _PulsarAckToken(message_id=object(), topic=f"t{i}")
            for i in range(_MAX_IN_FLIGHT)
        }
        mocker.patch(
            "scrapy_extension.backends.pulsar.logger.warning",
            side_effect=diagnostic_error,
        )

        value, token = b.pop_with_ack("queue1", timeout=1.0)
        b.ack("queue1", token=token)

        assert value == b"hello"
        assert isinstance(token, _PulsarAckToken)
        assert token.message_id is msg_id
        assert token not in b._in_flight
        assert len(b._in_flight) == _MAX_IN_FLIGHT
        assert b._in_flight_overflow_warned is True
        consumer.acknowledge.assert_called_once_with(msg_id)
