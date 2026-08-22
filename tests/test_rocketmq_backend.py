"""Tests for RocketMQ backend implementation (apache ``rocketmq-python-client`` 5.1.1 gRPC).

Rewritten (#44) alongside the backend rewrite. The apache client is installed for
one isolated compatibility smoke, while behavioural unit tests install a module
stub so importing the SDK cannot configure its process-global file logger.
"""

import os
import subprocess
import sys
import threading
import traceback
from pathlib import Path
from types import ModuleType
from typing import cast
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr, ValidationError

from scrapy_extension.backends.base import Backend, BackendType, QueueBackend
from scrapy_extension.backends.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    wrap_queue_backend,
)
from scrapy_extension.backends.connectors import (
    SET_CAPABLE_BACKENDS,
    STORAGE_CAPABLE_BACKENDS,
    resolve_backend_config,
)
from scrapy_extension.backends.rocketmq import RocketMQBackend
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings import RocketMQMode, RocketMQSettings

pytestmark = pytest.mark.usefixtures("cleanup_rocketmq_backends")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


class _FailingShutdownClient:
    """Keep nested private state while reporting a fixed shutdown failure."""

    def __init__(self, private_state: object) -> None:
        self.state = {"nested": [private_state]}

    def shutdown(self) -> None:
        raise RuntimeError("fixed shutdown failure")


class _ShutdownControlSignal(BaseException):
    """Custom process-control signal used to verify exact type preservation."""


class _ControlShutdownClient:
    """Expose nested client state from a shutdown control-exception frame."""

    def __init__(
        self, private_state: object, control_error: BaseException | None = None
    ) -> None:
        self.state = {"nested": [private_state]}
        self.control_error = control_error
        self.shutdown_calls = 0

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        if self.control_error is not None:
            raise self.control_error


def _patch_rocketmq(mocker):
    """Install a stub of the apache 5.1.1 top-level client surface.

    Returns ``(mock_producer_cls, mock_consumer_cls, mock_message_cls,
    mock_config_cls, mock_credentials_cls)`` so tests can assert on construction
    args. Instances: ``mock_producer_cls.return_value`` / ``mock_consumer_cls.return_value``.
    """
    rocketmq_module = ModuleType("rocketmq")
    mock_producer = mocker.MagicMock()
    mock_consumer = mocker.MagicMock()
    mock_producer_cls = mocker.MagicMock(return_value=mock_producer)
    mock_consumer_cls = mocker.MagicMock(return_value=mock_consumer)
    mock_message_cls = mocker.MagicMock()
    mock_config_cls = mocker.MagicMock()
    mock_credentials_cls = mocker.MagicMock()
    rocketmq_module.Producer = mock_producer_cls
    rocketmq_module.SimpleConsumer = mock_consumer_cls
    rocketmq_module.Message = mock_message_cls
    rocketmq_module.ClientConfiguration = mock_config_cls
    rocketmq_module.Credentials = mock_credentials_cls
    mocker.patch.dict(sys.modules, {"rocketmq": rocketmq_module})
    return (
        mock_producer_cls,
        mock_consumer_cls,
        mock_message_cls,
        mock_config_cls,
        mock_credentials_cls,
    )


def _make_connected_backend(mocker, *, access_key=None, secret_key=None, **kw):
    """Create a backend with patched rocketmq client and pre-connected state.

    Returns ``(backend, mock_producer, mock_consumer, mock_message_cls)``.
    """
    config = RocketMQSettings(access_key=access_key, secret_key=secret_key, **kw)
    backend = RocketMQBackend(config)
    (
        mock_producer_cls,
        mock_consumer_cls,
        mock_message_cls,
        _,
        _,
    ) = _patch_rocketmq(mocker)
    backend.connect()
    return (
        backend,
        mock_producer_cls.return_value,
        mock_consumer_cls.return_value,
        mock_message_cls,
    )


# ---------------------------------------------------------------------------
# Instantiation & interface
# ---------------------------------------------------------------------------


def test_rocketmq_backend_instantiation() -> None:
    """Test RocketMQBackend can be instantiated."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.config is config
    assert backend.backend_type == BackendType.ROCKETMQ


def test_rocketmq_backend_is_connected_false_before_connect() -> None:
    """Test is_connected returns False before connect."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.is_connected() is False


def test_rocketmq_backend_ping_false_before_connect() -> None:
    """Test ping returns False before connect."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert backend.ping() is False


def test_rocketmq_backend_implements_backend() -> None:
    """Test RocketMQBackend implements Backend."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert isinstance(backend, Backend)


def test_rocketmq_backend_implements_queuebackend() -> None:
    """Test RocketMQBackend implements QueueBackend."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    assert isinstance(backend, QueueBackend)


def test_rocketmq_backend_disconnect() -> None:
    """Test disconnect cleans up connections (no-op when never connected)."""
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    backend.disconnect()  # should not raise
    assert backend._producer is None
    assert backend._consumer is None


def test_locked_sdk_exposes_wait_and_lease_mutation_contracts(tmp_path: Path) -> None:
    """Check the real SDK surface without importing its file logger in pytest."""
    script = "\n".join(
        (
            "from inspect import signature",
            "from rocketmq import Producer, SimpleConsumer",
            "assert 'tls_enable' in signature(Producer).parameters",
            "assert 'tls_enable' in signature(SimpleConsumer).parameters",
            "consumer = object.__new__(SimpleConsumer)",
            "consumer.await_duration = 0",
            "assert consumer.await_duration == 0",
            "assert callable(SimpleConsumer.change_invisible_duration)",
        )
    )
    env = os.environ.copy()
    env["HOME"] = str(tmp_path)

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# connect — success paths
# ---------------------------------------------------------------------------


def test_connect_standalone_mode(mocker) -> None:
    """Standalone connect: ClientConfiguration built with the proxy endpoints."""
    config = RocketMQSettings(mode=RocketMQMode.STANDALONE)
    backend = RocketMQBackend(config)
    (
        mock_producer_cls,
        mock_consumer_cls,
        _,
        mock_config_cls,
        _,
    ) = _patch_rocketmq(mocker)

    backend.connect()

    assert backend._producer is mock_producer_cls.return_value
    assert backend._consumer is mock_consumer_cls.return_value
    mock_producer_cls.return_value.startup.assert_called_once()
    mock_consumer_cls.return_value.startup.assert_called_once()
    mock_consumer_cls.assert_called_once_with(
        mock_config_cls.return_value,
        config.consumer_group,
        await_duration=0,
        tls_enable=False,
    )
    mock_producer_cls.assert_called_once_with(
        mock_config_cls.return_value,
        tls_enable=False,
    )
    # ClientConfiguration receives endpoints=namesrv_address (now the gRPC proxy).
    assert mock_config_cls.call_args.kwargs["endpoints"] == config.namesrv_address


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("logger extension failed"), KeyboardInterrupt(), SystemExit()],
)
def test_connect_post_publish_logger_failure_keeps_live_generation(
    mocker, diagnostic_error: BaseException
) -> None:
    """Published clients survive ordinary and control diagnostic failures."""
    backend = RocketMQBackend(RocketMQSettings())
    mock_producer_cls, mock_consumer_cls, *_ = _patch_rocketmq(mocker)
    producer = mock_producer_cls.return_value
    consumer = mock_consumer_cls.return_value
    debug_log = mocker.patch(
        "scrapy_extension.backends.rocketmq.logger.debug",
        side_effect=diagnostic_error,
    )

    backend.connect()

    assert backend._producer is producer
    assert backend._consumer is consumer
    assert backend._consumer_generation == 1
    producer.shutdown.assert_not_called()
    consumer.shutdown.assert_not_called()

    debug_log.side_effect = None
    backend.disconnect()
    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()


def test_repeated_connect_keeps_the_existing_client_generation(mocker) -> None:
    """A complete live producer/consumer pair makes connect idempotent."""
    backend = RocketMQBackend(RocketMQSettings())
    mock_producer_cls, mock_consumer_cls, _, _, _ = _patch_rocketmq(mocker)

    backend.connect()
    producer = backend._producer
    consumer = backend._consumer
    backend.connect()

    assert backend._producer is producer
    assert backend._consumer is consumer
    mock_producer_cls.assert_called_once()
    mock_consumer_cls.assert_called_once()
    producer.startup.assert_called_once_with()
    consumer.startup.assert_called_once_with()


def test_overlapping_connects_share_the_same_client_generation(mocker) -> None:
    """A second direct connect waits for and reuses the first generation."""
    backend = RocketMQBackend(RocketMQSettings())
    mock_producer_cls, mock_consumer_cls, _, _, _ = _patch_rocketmq(mocker)
    startup_started = threading.Event()
    allow_startup = threading.Event()
    errors: list[BaseException] = []

    def block_producer_startup() -> None:
        startup_started.set()
        assert allow_startup.wait(timeout=2)

    mock_producer_cls.return_value.startup.side_effect = block_producer_startup

    def connect() -> None:
        try:
            backend.connect()
        except BaseException as error:  # pragma: no cover - surfaced below
            errors.append(error)

    first = threading.Thread(target=connect)
    second = threading.Thread(target=connect)
    first.start()
    assert startup_started.wait(timeout=1)
    second.start()
    assert mock_producer_cls.call_count == 1

    allow_startup.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    mock_producer_cls.assert_called_once()
    mock_consumer_cls.assert_called_once()


def test_connect_retires_a_one_sided_residual_before_fresh_generation(mocker) -> None:
    """An interrupted old generation cannot survive a later fresh connect."""
    backend = RocketMQBackend(RocketMQSettings())
    orphan_producer = MagicMock()
    backend._producer = orphan_producer
    mock_producer_cls, mock_consumer_cls, _, _, _ = _patch_rocketmq(mocker)

    backend.connect()

    orphan_producer.shutdown.assert_called_once_with()
    assert backend._producer is mock_producer_cls.return_value
    assert backend._consumer is mock_consumer_cls.return_value


def test_disconnect_waits_for_the_full_connect_generation(mocker) -> None:
    """Concurrent disconnect cannot detach the producer while connect starts it."""
    backend = RocketMQBackend(RocketMQSettings())
    mock_producer_cls, mock_consumer_cls, _, _, _ = _patch_rocketmq(mocker)
    startup_started = threading.Event()
    allow_startup = threading.Event()
    disconnect_finished = threading.Event()
    errors: list[BaseException] = []

    def block_producer_startup() -> None:
        startup_started.set()
        assert allow_startup.wait(timeout=2)

    mock_producer_cls.return_value.startup.side_effect = block_producer_startup

    def connect() -> None:
        try:
            backend.connect()
        except BaseException as error:  # pragma: no cover - surfaced below
            errors.append(error)

    def disconnect() -> None:
        try:
            backend.disconnect()
        except BaseException as error:  # pragma: no cover - surfaced below
            errors.append(error)
        finally:
            disconnect_finished.set()

    connect_thread = threading.Thread(target=connect)
    connect_thread.start()
    assert startup_started.wait(timeout=1)

    disconnect_thread = threading.Thread(target=disconnect)
    disconnect_thread.start()
    assert not disconnect_finished.wait(timeout=0.1)

    allow_startup.set()
    connect_thread.join(timeout=2)
    disconnect_thread.join(timeout=2)

    assert not connect_thread.is_alive()
    assert not disconnect_thread.is_alive()
    assert errors == []
    mock_producer_cls.return_value.shutdown.assert_called_once_with()
    mock_consumer_cls.return_value.shutdown.assert_called_once_with()
    assert backend._producer is None
    assert backend._consumer is None


def test_connect_cluster_mode(mocker) -> None:
    """Cluster mode passes the configured proxy endpoints through."""
    config = RocketMQSettings(
        mode=RocketMQMode.CLUSTER,
        namesrv_address="rocketmq-cluster:8081",
        allow_remote_plaintext=True,
    )
    backend = RocketMQBackend(config)
    (_, _, _, mock_config_cls, _) = _patch_rocketmq(mocker)

    backend.connect()

    assert mock_config_cls.call_args.kwargs["endpoints"] == "rocketmq-cluster:8081"


def test_connect_cloud_mode_with_credentials(mocker) -> None:
    """Cloud mode with access/secret builds Credentials(ak, sk)."""
    (_, _, _, _, mock_credentials_cls) = _patch_rocketmq(mocker)
    config = RocketMQSettings(
        mode=RocketMQMode.CLOUD,
        access_key=SecretStr("my-access-key"),
        secret_key=SecretStr("my-secret-key"),
        tls_enabled=True,
    )
    backend = RocketMQBackend(config)

    backend.connect()

    mock_credentials_cls.assert_called_once_with("my-access-key", "my-secret-key")


def test_connect_redacts_credentials_at_sdk_boundary(mocker) -> None:
    (_, _, _, _, mock_credentials_cls) = _patch_rocketmq(mocker)
    backend = RocketMQBackend(
        RocketMQSettings(
            access_key=SecretStr("my-access-key"),
            secret_key=SecretStr("my-secret-key"),
            tls_enabled=True,
        )
    )

    backend.connect()

    sdk_access_key, sdk_secret_key = mock_credentials_cls.call_args.args
    assert str(sdk_access_key) == "my-access-key"
    assert str(sdk_secret_key) == "my-secret-key"
    assert repr(sdk_access_key) == "<redacted>"
    assert repr(sdk_secret_key) == "<redacted>"


def test_connect_standalone_without_credentials(mocker) -> None:
    """Standalone mode without keys builds an empty Credentials()."""
    (_, _, _, _, mock_credentials_cls) = _patch_rocketmq(mocker)
    config = RocketMQSettings()  # defaults: no keys
    backend = RocketMQBackend(config)

    backend.connect()

    # Credentials() is always constructed (empty for no-auth); it's the apache
    # no-auth pattern. Assert it was called with no positional args.
    mock_credentials_cls.assert_called_once_with()


@pytest.mark.parametrize(
    ("access_key", "secret_key", "setting_name"),
    (
        (SecretStr(""), SecretStr("secret"), "access_key"),
        (SecretStr("key"), SecretStr(" "), "secret_key"),
        (SecretStr("do-not-leak"), None, "secret_key"),
        (None, SecretStr("do-not-leak"), "access_key"),
    ),
)
def test_settings_reject_empty_or_partial_credentials_without_leaking(
    access_key,
    secret_key,
    setting_name,
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RocketMQSettings(access_key=access_key, secret_key=secret_key)

    assert exc_info.value.setting_name == setting_name
    assert "do-not-leak" not in str(exc_info.value)
    assert "do-not-leak" not in repr(exc_info.value.setting_value)


def test_cloud_mode_requires_credentials() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RocketMQSettings(mode=RocketMQMode.CLOUD, tls_enabled=True)

    assert exc_info.value.setting_name == "access_key"


def test_cloud_mode_requires_tls() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RocketMQSettings(
            mode=RocketMQMode.CLOUD,
            access_key=SecretStr("key"),
            secret_key=SecretStr("secret"),
            tls_enabled=False,
        )

    assert exc_info.value.setting_name == "tls_enabled"


@pytest.mark.parametrize("mode", (RocketMQMode.STANDALONE, RocketMQMode.CLUSTER))
def test_credentials_require_tls_in_every_mode(mode: RocketMQMode) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        RocketMQSettings(
            mode=mode,
            access_key=SecretStr("key"),
            secret_key=SecretStr("do-not-leak"),
            tls_enabled=False,
        )

    assert exc_info.value.setting_name == "tls_enabled"
    assert "do-not-leak" not in str(exc_info.value)


def test_connect_propagates_tls_to_both_sdk_clients(mocker) -> None:
    mock_producer_cls, mock_consumer_cls, _, mock_config_cls, _ = _patch_rocketmq(
        mocker
    )
    config = RocketMQSettings(tls_enabled=True)
    backend = RocketMQBackend(config)

    backend.connect()

    mock_producer_cls.assert_called_once_with(
        mock_config_cls.return_value,
        tls_enable=True,
    )
    mock_consumer_cls.assert_called_once_with(
        mock_config_cls.return_value,
        config.consumer_group,
        await_duration=0,
        tls_enable=True,
    )


def test_connect_revalidates_mutated_cloud_security_before_sdk_io(mocker) -> None:
    mock_producer_cls, mock_consumer_cls, _, mock_config_cls, _ = _patch_rocketmq(
        mocker
    )
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    config.mode = RocketMQMode.CLOUD

    with pytest.raises(ConfigurationError, match="access_key"):
        backend.connect()

    mock_config_cls.assert_not_called()
    mock_producer_cls.assert_not_called()
    mock_consumer_cls.assert_not_called()


@pytest.mark.parametrize("mutated_timeout", [True, 1.5, "1000", -1, 300_001])
def test_connect_revalidates_mutated_send_timeout_before_sdk_io(
    mocker, mutated_timeout: object
) -> None:
    (
        mock_producer_cls,
        mock_consumer_cls,
        _,
        mock_config_cls,
        mock_credentials_cls,
    ) = _patch_rocketmq(mocker)
    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    config.send_timeout = cast("int", mutated_timeout)

    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()

    error = exc_info.value
    assert str(error) == (
        "RocketMQ send_timeout must be an integer between 0 and 300000 milliseconds."
    )
    assert error.setting_name == "send_timeout"
    assert error.setting_value is None
    assert error.__cause__ is None
    assert error.__context__ is None
    mock_config_cls.assert_not_called()
    mock_credentials_cls.assert_not_called()
    mock_producer_cls.assert_not_called()
    mock_consumer_cls.assert_not_called()


def test_connect_uses_one_validated_settings_snapshot(mocker) -> None:
    (
        mock_producer_cls,
        mock_consumer_cls,
        _,
        mock_config_cls,
        mock_credentials_cls,
    ) = _patch_rocketmq(mocker)
    config = RocketMQSettings(
        namesrv_address="original:8081",
        access_key=SecretStr("original-key"),
        secret_key=SecretStr("original-secret"),
        consumer_group="original-consumer",
        send_timeout=5_000,
        tls_enabled=True,
    )
    backend = RocketMQBackend(config)

    credentials_obj = MagicMock(name="Credentials")

    def mutate_after_credentials(*_args):
        config.namesrv_address = "mutated:8081"
        config.consumer_group = "mutated-consumer"
        config.send_timeout = 1_000
        config.tls_enabled = False
        return credentials_obj

    mock_credentials_cls.side_effect = mutate_after_credentials

    backend.connect()

    assert mock_config_cls.call_args.kwargs == {
        "endpoints": "original:8081",
        "credentials": credentials_obj,
        "request_timeout": 5,
    }
    mock_producer_cls.assert_called_once_with(
        mock_config_cls.return_value,
        tls_enable=True,
    )
    mock_consumer_cls.assert_called_once_with(
        mock_config_cls.return_value,
        "original-consumer",
        await_duration=0,
        tls_enable=True,
    )


def test_operations_use_immutable_settings_from_connected_generation(mocker) -> None:
    """Post-connect setting mutation cannot retarget or reject old clients."""
    backend, producer, _consumer, message_cls = _make_connected_backend(
        mocker,
        topic_prefix="original",
        max_message_size=10,
    )
    message = MagicMock()
    message_cls.return_value = message
    config = backend.config
    config.topic_prefix = "mutated"
    config.max_message_size = 1
    config.invisible_duration = 1

    backend.push("jobs", b"payload")

    assert message.topic == "original_jobs"
    producer.send.assert_called_once_with(message)


def test_send_timeout_field_rejects_above_ceiling():
    """R22-A: ``send_timeout`` has a 5 min (300_000 ms) ceiling so a stray-zero
    typo (e.g. a microseconds copy-paste) cannot wedge the gRPC per-RPC deadline
    for hours. Mirrors the cap discipline on ``invisible_duration`` (le=12*60*60)
    and the R21 timeout caps (circuit_breaker/backoff/throttle)."""
    with pytest.raises(ValidationError):
        RocketMQSettings(send_timeout=300_001)


def test_send_timeout_conversion_capped_at_max_request_timeout(mocker):
    """R22-A: the backend conversion ``min(max(3, send_timeout // 1000),
    _MAX_REQUEST_TIMEOUT_S)`` applies the ceiling defense-in-depth. A field-max
    ``send_timeout`` (300_000 ms) converts to at most ``_MAX_REQUEST_TIMEOUT_S``
    seconds; lowering the const via monkeypatch proves the ``min(...)`` arm caps
    rather than passing the raw seconds through."""
    from scrapy_extension.backends import rocketmq as rmq_mod

    (
        mock_producer_cls,
        _mock_consumer_cls,
        _mock_message_cls,
        mock_config_cls,
        _mock_credentials_cls,
    ) = _patch_rocketmq(mocker)

    # Field-max valid value (300_000 ms == 300 s); default const is 300 → 300.
    config = RocketMQSettings(namesrv_address="localhost:8081", send_timeout=300_000)
    RocketMQBackend(config).connect()
    assert mock_config_cls.call_args.kwargs["request_timeout"] == 300

    # Lower the ceiling: the same field-max value must now clamp to the const,
    # not pass 300 through — proves the ``min(..., _MAX_REQUEST_TIMEOUT_S)`` arm.
    mocker.patch.object(rmq_mod, "_MAX_REQUEST_TIMEOUT_S", 60)
    mock_config_cls.reset_mock()
    RocketMQBackend(config).connect()
    assert mock_config_cls.call_args.kwargs["request_timeout"] == 60


# ---------------------------------------------------------------------------
# connect — failure paths
# ---------------------------------------------------------------------------


def test_connect_missing_package(mocker) -> None:
    """connect raises BackendConnectionError when the rocketmq import fails."""
    import builtins

    config = RocketMQSettings()
    backend = RocketMQBackend(config)
    real_import = builtins.__import__

    def import_without_rocketmq(name, *args, **kwargs):
        if name == "rocketmq":
            raise ModuleNotFoundError("no rocketmq", name="rocketmq")
        return real_import(name, *args, **kwargs)

    mocker.patch("builtins.__import__", side_effect=import_without_rocketmq)

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.connect()
    assert "rocketmq-python-client not installed" in str(exc_info.value)


def test_connect_unsupported_mode(mocker) -> None:
    """connect raises ConfigurationError for unsupported mode."""
    _patch_rocketmq(mocker)
    config = RocketMQSettings()
    # Bypass the typed field (mode is RocketMQMode) to inject an invalid value
    # at runtime; cast keeps the static checkers happy without altering behavior.
    config.mode = cast("RocketMQMode", "invalid_mode")
    backend = RocketMQBackend(config)

    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()
    assert "Unsupported RocketMQ mode" in str(exc_info.value)
    assert exc_info.value.setting_name == "mode"


def test_connect_unsupported_mode_str_raises(mocker) -> None:
    """connect handles a mode whose str() raises TypeError (defensive repr)."""
    _patch_rocketmq(mocker)
    config = RocketMQSettings()

    class BadMode:
        def __str__(self) -> None:
            raise TypeError("bad")

    config.mode = cast("RocketMQMode", BadMode())
    backend = RocketMQBackend(config)

    with pytest.raises(ConfigurationError) as exc_info:
        backend.connect()
    assert "Unsupported RocketMQ mode" in str(exc_info.value)


def test_connect_producer_startup_failure(mocker) -> None:
    """connect wraps a producer startup failure in BackendConnectionError."""
    (
        mock_producer_cls,
        _,
        _,
        _,
        _,
    ) = _patch_rocketmq(mocker)
    mock_producer_cls.return_value.startup.side_effect = OSError("Connection refused")
    backend = RocketMQBackend(RocketMQSettings())

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.connect()
    assert "Failed to connect to RocketMQ" in str(exc_info.value)
    assert exc_info.value.backend_type == "rocketmq"
    mock_producer_cls.return_value.shutdown.assert_called_once()
    assert backend._producer is None
    assert backend._consumer is None


def test_connect_consumer_startup_failure_cleans_both_clients(mocker) -> None:
    mock_producer_cls, mock_consumer_cls, *_ = _patch_rocketmq(mocker)
    mock_consumer_cls.return_value.startup.side_effect = RuntimeError("consumer down")
    backend = RocketMQBackend(RocketMQSettings())

    with pytest.raises(BackendConnectionError):
        backend.connect()

    mock_consumer_cls.return_value.shutdown.assert_called_once()
    mock_producer_cls.return_value.shutdown.assert_called_once()
    assert backend._producer is None
    assert backend._consumer is None


def test_connect_failure_keeps_primary_when_abort_logging_is_interrupted(
    mocker,
) -> None:
    """An abort diagnostic must not mask startup failure or skip sibling cleanup."""
    mock_producer_cls, mock_consumer_cls, *_ = _patch_rocketmq(mocker)
    startup_error = RuntimeError("consumer down")
    mock_consumer_cls.return_value.startup.side_effect = startup_error
    mock_consumer_cls.return_value.shutdown.side_effect = RuntimeError("close down")
    mocker.patch(
        "scrapy_extension.backends.rocketmq.logger.debug",
        side_effect=KeyboardInterrupt(),
    )
    backend = RocketMQBackend(RocketMQSettings())

    with pytest.raises(BackendConnectionError) as raised:
        backend.connect()

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    mock_consumer_cls.return_value.shutdown.assert_called_once_with()
    mock_producer_cls.return_value.shutdown.assert_called_once_with()
    assert backend._producer is None
    assert backend._consumer is None


def test_connect_consumer_startup_baseexception_cleans_both_clients(mocker) -> None:
    """R16-A: a BaseException mid-construction must not skip the abort arm.

    A ``KeyboardInterrupt`` / ``SystemExit`` raised by ``consumer.startup()``
    (after the producer is assigned and started) is NOT an ``Exception``
    subclass, so the ``except Exception`` arm cannot catch it. Without an
    ``except BaseException`` arm both clients
    leak and the backend wedges. Mirrors the kafka BaseException arm and the
    mongodb/es ``except BaseException`` cleanup pattern.
    """
    mock_producer_cls, mock_consumer_cls, *_ = _patch_rocketmq(mocker)
    startup_error = KeyboardInterrupt()
    mock_consumer_cls.return_value.startup.side_effect = startup_error
    mock_consumer_cls.return_value.shutdown.side_effect = SystemExit(2)
    backend = RocketMQBackend(RocketMQSettings())

    # The BaseException must propagate, not be swallowed into BackendConnectionError.
    with pytest.raises(KeyboardInterrupt) as raised:
        backend.connect()

    assert raised.value is startup_error
    # No wedge/leak: abort ran, so both clients are detached and best-effort stopped.
    mock_consumer_cls.return_value.shutdown.assert_called_once()
    mock_producer_cls.return_value.shutdown.assert_called_once()
    assert backend._producer is None
    assert backend._consumer is None


def test_connect_unexpected_exception(mocker) -> None:
    """connect wraps any unexpected startup error in BackendConnectionError."""
    (
        mock_producer_cls,
        _,
        _,
        _,
        _,
    ) = _patch_rocketmq(mocker)
    marker = "rocketmq-driver-secret"
    mock_producer_cls.return_value.startup.side_effect = RuntimeError(
        f"driver dump included {marker}"
    )
    backend = RocketMQBackend(RocketMQSettings())

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.connect()
    assert "Failed to connect to RocketMQ" in str(exc_info.value)
    assert marker not in str(exc_info.value)
    assert marker not in repr(exc_info.value.__dict__)
    assert marker not in "".join(traceback.format_exception(exc_info.value))
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_connect_backend_error_does_not_retain_driver_diagnostics(mocker) -> None:
    mock_producer_cls, _, _, _, _ = _patch_rocketmq(mocker)
    marker = "rocketmq-backend-secret"
    mock_producer_cls.return_value.startup.side_effect = BackendConnectionError(
        f"custom client diagnostic included {marker}", backend_type="custom"
    )
    backend = RocketMQBackend(RocketMQSettings())

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.connect()

    assert marker not in str(exc_info.value)
    assert marker not in repr(exc_info.value.__dict__)
    assert marker not in "".join(traceback.format_exception(exc_info.value))
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    mock_producer_cls.return_value.shutdown.assert_called_once()
    assert backend._producer is None
    assert backend._consumer is None


# ---------------------------------------------------------------------------
# disconnect — connected state
# ---------------------------------------------------------------------------


def test_disconnect_connected(mocker) -> None:
    """disconnect shuts down producer and consumer."""
    backend, mock_producer, mock_consumer, _ = _make_connected_backend(mocker)

    backend.disconnect()

    mock_producer.shutdown.assert_called_once()
    mock_consumer.shutdown.assert_called_once()
    assert backend._producer is None
    assert backend._consumer is None


def test_shutdown_fallback_offers_candidate_exactly_once_after_thread_start_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    real_start = threading.Thread.start

    def fail_shutdown_thread_start(worker: threading.Thread) -> None:
        if worker.name.startswith("rocketmq-shutdown-"):
            raise RuntimeError("thread start interrupted")
        real_start(worker)

    monkeypatch.setattr(threading.Thread, "start", fail_shutdown_thread_start)

    cleanup_failed = RocketMQBackend._shutdown_detached_clients((client, "producer"))

    assert cleanup_failed is True
    client.shutdown.assert_called_once_with()


def test_disconnect_reports_typed_shutdown_failure_after_closing_peers(mocker) -> None:
    """disconnect reports a failed close after still attempting every client."""
    backend, mock_producer, mock_consumer, _ = _make_connected_backend(mocker)
    mock_producer.shutdown.side_effect = RuntimeError("boom")

    with pytest.raises(BackendConnectionError, match="Failed to disconnect") as raised:
        backend.disconnect()

    assert raised.value.backend_type == "rocketmq"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    mock_producer.shutdown.assert_called_once()
    mock_consumer.shutdown.assert_called_once()
    assert backend._producer is None


@pytest.mark.parametrize("private_location", ("producer", "consumer", "config"))
def test_disconnect_failure_traceback_recursively_redacts_teardown_state(
    private_location: str,
) -> None:
    """The terminal error omits nested client and retained configuration state."""
    marker = f"rocketmq-disconnect-{private_location}-private-marker"
    settings = RocketMQSettings(
        namesrv_address=(
            f"{marker}.example:8081"
            if private_location == "config"
            else "localhost:8081"
        ),
        allow_remote_plaintext=True,
    )
    backend = RocketMQBackend(settings)
    backend._producer = _FailingShutdownClient(
        marker if private_location == "producer" else "public producer state"
    )
    backend._consumer = _FailingShutdownClient(
        marker if private_location == "consumer" else "public consumer state"
    )

    with pytest.raises(BackendConnectionError) as exc_info:
        backend.disconnect()

    error = exc_info.value
    assert str(error) == "Failed to disconnect from RocketMQ."
    assert error.backend_type == "rocketmq"
    _assert_capability_error_is_redacted(error, marker)


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("debug failed"), KeyboardInterrupt(), SystemExit(1)],
)
def test_disconnect_preserves_completed_state_when_debug_is_interrupted(
    mocker, diagnostic_error: BaseException
) -> None:
    """Post-disconnect diagnostics cannot turn a completed close into failure."""
    backend, producer, consumer, _ = _make_connected_backend(mocker)
    mocker.patch(
        "scrapy_extension.backends.rocketmq.logger.debug",
        side_effect=diagnostic_error,
    )

    backend.disconnect()

    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()
    assert backend._producer is None
    assert backend._consumer is None


def test_disconnect_diagnostic_cannot_mask_typed_shutdown_failure(mocker) -> None:
    """A cleanup diagnostic must not stop peers or mask the typed error."""
    backend, producer, consumer, _ = _make_connected_backend(mocker)
    producer.shutdown.side_effect = RuntimeError("boom")
    mocker.patch(
        "scrapy_extension.backends.rocketmq.logger.debug",
        side_effect=KeyboardInterrupt(),
    )

    with pytest.raises(BackendConnectionError, match="Failed to disconnect"):
        backend.disconnect()

    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()


def test_disconnect_closes_all_clients_after_baseexception(mocker) -> None:
    backend, producer, consumer, _ = _make_connected_backend(mocker)
    producer_error = KeyboardInterrupt()
    consumer_error = SystemExit(2)
    producer.shutdown.side_effect = producer_error
    consumer.shutdown.side_effect = consumer_error

    with pytest.raises(SystemExit) as raised:
        backend.disconnect()

    assert raised.value is consumer_error
    producer.shutdown.assert_called_once_with()
    consumer.shutdown.assert_called_once_with()
    assert backend._producer is None
    assert backend._consumer is None


@pytest.mark.parametrize("private_location", ("producer", "consumer", "config"))
@pytest.mark.parametrize(
    ("control_error_type", "control_args"),
    (
        (KeyboardInterrupt, ()),
        (SystemExit, (17,)),
        (_ShutdownControlSignal, ("stop",)),
    ),
    ids=("keyboard-interrupt", "system-exit", "custom-baseexception"),
)
def test_disconnect_control_exception_recursively_redacts_teardown_state(
    private_location: str,
    control_error_type: type[BaseException],
    control_args: tuple[object, ...],
) -> None:
    """The exact shutdown signal escapes without old client/config frames."""
    marker = f"rocketmq-control-{private_location}-private-marker"
    settings = RocketMQSettings(
        namesrv_address=(
            f"{marker}.example:8081"
            if private_location == "config"
            else "localhost:8081"
        ),
        allow_remote_plaintext=True,
    )
    control_error = control_error_type(*control_args)
    control_error.__cause__ = RuntimeError("obsolete shutdown cause")
    control_error.__context__ = RuntimeError("obsolete shutdown context")
    producer = _ControlShutdownClient(
        marker if private_location == "producer" else "public producer state",
        control_error if private_location != "consumer" else None,
    )
    consumer = _ControlShutdownClient(
        marker if private_location == "consumer" else "public consumer state",
        control_error if private_location == "consumer" else None,
    )
    backend = RocketMQBackend(settings)
    backend._producer = producer
    backend._consumer = consumer

    with pytest.raises(BaseException) as exc_info:
        backend.disconnect()

    error = exc_info.value
    assert error is control_error
    assert type(error) is control_error_type
    assert producer.shutdown_calls == 1
    assert consumer.shutdown_calls == 1
    assert backend._producer is None
    assert backend._consumer is None
    _assert_capability_error_is_redacted(error, marker)


# ---------------------------------------------------------------------------
# is_connected / ping — connected state
# ---------------------------------------------------------------------------


def test_is_connected_true(mocker) -> None:
    """is_connected returns True after successful connect."""
    backend, _, _, _ = _make_connected_backend(mocker)
    assert backend.is_connected() is True


def test_ping_true(mocker) -> None:
    """ping returns True when connected."""
    backend, _, _, _ = _make_connected_backend(mocker)
    assert backend.ping() is True


# ---------------------------------------------------------------------------
# _get_topic_name
# ---------------------------------------------------------------------------


def test_get_topic_name(mocker) -> None:
    """_get_topic_name returns prefixed queue name."""
    backend, _, _, _ = _make_connected_backend(mocker)
    result = backend._get_topic_name("my_queue")
    assert result == f"{backend.config.topic_prefix}_my_queue"


# ---------------------------------------------------------------------------
# push
# ---------------------------------------------------------------------------


def test_push_not_connected() -> None:
    """push raises QueueError when not connected."""
    backend = RocketMQBackend(RocketMQSettings())
    with pytest.raises(QueueError) as exc_info:
        backend.push("test_queue", b"item")
    assert "Not connected" in str(exc_info.value)


def test_push_success(mocker) -> None:
    """push builds a Message (topic/body/keys) and sends it via the producer."""
    backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)
    mock_msg = mocker.MagicMock()
    mock_message_cls.return_value = mock_msg

    backend.push("my_queue", b"test-data", priority=5.0)

    # apache Message: instantiate, then set .topic/.body/.keys attributes.
    mock_message_cls.assert_called_once_with()
    assert mock_msg.topic == f"{backend.config.topic_prefix}_my_queue"
    assert mock_msg.body == b"test-data"
    assert mock_msg.keys == "5.0"
    mock_producer.send.assert_called_once_with(mock_msg)


def test_push_default_priority(mocker) -> None:
    """push with default priority carries "0.0" as the message keys."""
    backend, _, _, mock_message_cls = _make_connected_backend(mocker)
    mock_msg = mocker.MagicMock()
    mock_message_cls.return_value = mock_msg

    backend.push("my_queue", b"data")

    assert mock_msg.keys == "0.0"


def test_push_send_failure(mocker) -> None:
    """push wraps a producer send failure in QueueError."""
    backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)
    mock_message_cls.return_value = mocker.MagicMock()
    mock_producer.send.side_effect = OSError("Network error")

    with pytest.raises(QueueError) as exc_info:
        backend.push("my_queue", b"data")
    assert str(exc_info.value) == "Failed to push RocketMQ message."


def test_push_unexpected_error(mocker) -> None:
    """push wraps any unexpected send error in QueueError."""
    backend, mock_producer, _, mock_message_cls = _make_connected_backend(mocker)
    mock_message_cls.return_value = mocker.MagicMock()
    mock_producer.send.side_effect = RuntimeError("boom")

    with pytest.raises(QueueError) as exc_info:
        backend.push("my_queue", b"data")
    assert str(exc_info.value) == "Failed to push RocketMQ message."


def test_push_rejects_item_above_max_message_size(mocker) -> None:
    """R22-C: push enforces RocketMQSettings.max_message_size client-side (fail-
    fast) so the documented size cap is not silently ignored. An oversized item
    raises QueueError BEFORE producer.send is called, mirroring BackendQueue's
    queue_max_item_bytes gate. Pre-fix the Field was dead config (never read)."""
    backend, mock_producer, _, _mock_message_cls = _make_connected_backend(
        mocker, max_message_size=8
    )

    with pytest.raises(QueueError, match="max_message_size") as exc_info:
        backend.push("my_queue", b"x" * 16)  # 16 > 8-byte cap
    assert (
        str(exc_info.value) == "RocketMQ message exceeds configured max_message_size."
    )
    assert exc_info.value.operation == "push"
    mock_producer.send.assert_not_called()


# ---------------------------------------------------------------------------
# pop
# ---------------------------------------------------------------------------


def test_pop_not_connected() -> None:
    """pop raises QueueError when not connected."""
    backend = RocketMQBackend(RocketMQSettings())
    with pytest.raises(QueueError) as exc_info:
        backend.pop("test_queue")
    assert "Not connected" in str(exc_info.value)


def test_pop_returns_message(mocker) -> None:
    """A positive wait takes a pumped message without acknowledging inline."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_msg = mocker.MagicMock(body=b"hello-world")
    mock_consumer.receive.return_value = [mock_msg]

    result = backend.pop("my_queue", timeout=1)

    assert result == b"hello-world"
    assert mock_consumer.await_duration == 5
    mock_consumer.receive.assert_called_once_with(1, 300)
    mock_consumer.ack.assert_not_called()


def test_pop_returns_none_when_empty(mocker) -> None:
    """A completed empty pump cycle wakes a positive waiter."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.return_value = []

    assert backend.pop("my_queue", timeout=1) is None
    mock_consumer.ack.assert_not_called()


def test_pop_timeout_does_not_change_bounded_broker_poll(mocker) -> None:
    """Public wait duration is local; the broker poll remains bounded."""
    backend, _, mock_consumer, _ = _make_connected_backend(
        mocker, invisible_duration=90
    )
    mock_consumer.receive.return_value = []

    backend.pop("my_queue", timeout=20.0)

    assert mock_consumer.await_duration == 5
    mock_consumer.receive.assert_called_once_with(1, 90)


def test_pop_fractional_timeout_uses_bounded_broker_poll(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.return_value = []

    backend.pop("my_queue", timeout=0.1)

    assert mock_consumer.await_duration == 5
    mock_consumer.receive.assert_called_once_with(1, 300)


def test_pop_zero_timeout_starts_bounded_pump(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    receive_called = threading.Event()

    def receive(_maximum: int, _lease: int) -> list[object]:
        receive_called.set()
        return []

    mock_consumer.receive.side_effect = receive
    assert backend.pop("my_queue", timeout=0.0) is None
    assert receive_called.wait(timeout=1)
    assert mock_consumer.await_duration == 5


def test_pop_receive_failure_is_reported_without_driver_graph(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.side_effect = OSError("Network error")

    with pytest.raises(QueueError) as exc_info:
        backend.pop("my_queue", timeout=1)
    assert str(exc_info.value) == "Failed to pop RocketMQ message."
    assert exc_info.value.__cause__ is None


def test_pop_unexpected_error(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.side_effect = RuntimeError("unexpected")

    with pytest.raises(QueueError) as exc_info:
        backend.pop("my_queue", timeout=1)
    assert str(exc_info.value) == "Failed to pop RocketMQ message."


def test_pop_subscribes_to_topic_before_receive(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.return_value = []

    backend.pop("my_queue", timeout=1)

    mock_consumer.subscribe.assert_called_once_with("scrapy-queue_my_queue")


def test_pop_subscribes_only_once_per_generation(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.return_value = []

    backend.pop("my_queue", timeout=1)
    backend.pop("my_queue", timeout=1)
    backend.pop("my_queue", timeout=1)

    assert mock_consumer.subscribe.call_count == 1


def test_pop_rejects_distinct_topic_on_same_generation(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.return_value = []

    backend.pop("queue_a", timeout=1)
    with pytest.raises(QueueError, match="different queue"):
        backend.pop("queue_b", timeout=0)

    mock_consumer.subscribe.assert_called_once_with("scrapy-queue_queue_a")


def test_connect_starts_consumer(mocker) -> None:
    """connect() must call consumer.startup() — without it receive() fails."""
    _, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.startup.assert_called_once()


def test_disconnect_clears_subscribed_topics(mocker) -> None:
    """disconnect() clears the subscription cache so reconnect re-subscribes."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.return_value = []
    backend.pop("my_queue", timeout=1)
    assert "scrapy-queue_my_queue" in backend._subscribed_topics

    backend.disconnect()
    assert len(backend._subscribed_topics) == 0


# ---------------------------------------------------------------------------
# pop/ack decouple (initiative #4 — at-least-once fix)
# ---------------------------------------------------------------------------


def test_pop_no_longer_inline_acks(mocker) -> None:
    """R2/regression: pop returns the body and does NOT ack inline.

    Pre-fix pop() acked inline, so an ack failure (broker down mid-ack) raised
    QueueError and the caller never got the data (at-most-once). Post-fix pop
    never acks — the body always reaches the caller; ack is the caller's
    explicit later responsibility.
    """
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_msg = mocker.MagicMock()
    mock_msg.body = b"payload"
    mock_consumer.receive.return_value = [mock_msg]
    mock_consumer.ack.side_effect = OSError("broker down")  # sabotage the OLD path

    result = backend.pop("my_queue", timeout=1)

    assert result == b"payload"  # body delivered
    mock_consumer.ack.assert_not_called()  # no inline ack attempted
    assert backend._last_msg is mock_msg  # tracked for legacy ack(token=None)


def test_pop_with_ack_returns_body_and_token_no_ack(mocker) -> None:
    """pop_with_ack returns an opaque generation token and does NOT ack."""
    from scrapy_extension.backends.rocketmq import _RocketMQAckToken

    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_msg = mocker.MagicMock()
    mock_msg.body = b"hello"
    mock_consumer.receive.return_value = [mock_msg]

    body, token = backend.pop_with_ack("my_queue", timeout=1)

    assert body == b"hello"
    assert isinstance(token, _RocketMQAckToken)
    assert token.message is mock_msg
    assert token.consumer is mock_consumer
    mock_consumer.ack.assert_not_called()


def test_token_pop_cannot_be_settled_through_legacy_slot(mocker) -> None:
    backend, _, consumer, _ = _make_connected_backend(mocker)
    message = mocker.MagicMock(body=b"x")
    consumer.receive.return_value = [message]

    _body, token = backend.pop_with_ack("q", timeout=1)
    backend.ack("q", token=None)
    backend.nack("q", token=token)

    assert backend._last_msg is None
    assert backend._last_delivery is None
    consumer.ack.assert_not_called()
    consumer.change_invisible_duration.assert_called_once_with(message, 10)


def test_pop_with_ack_empty_returns_none_none(mocker) -> None:
    """pop_with_ack on an empty queue returns (None, None)."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.receive.return_value = []

    body, token = backend.pop_with_ack("my_queue", timeout=1)

    assert body is None
    assert token is None


def test_ack_with_token_acks_specific_message(mocker) -> None:
    """ack(token) acks the specific message (concurrent-ack correct)."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    msg_b = mocker.MagicMock(name="b")
    msg_b.body = b"b"
    mock_consumer.receive.return_value = [msg_b]
    _body, token = backend.pop_with_ack("q", timeout=1)

    backend.ack("q", token=token)

    mock_consumer.ack.assert_called_once_with(msg_b)


def test_ack_token_none_acks_last_msg_then_clears(mocker) -> None:
    """Legacy ack(token=None) acks the tracked _last_msg, then clears it."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_msg = mocker.MagicMock()
    mock_msg.body = b"x"
    mock_consumer.receive.return_value = [mock_msg]
    backend.pop("my_queue", timeout=1)
    assert backend._last_msg is mock_msg

    backend.ack("my_queue", token=None)

    mock_consumer.ack.assert_called_once_with(mock_msg)
    assert backend._last_msg is None


def test_ack_token_none_with_no_last_msg_is_noop(mocker) -> None:
    """ack(token=None) with no tracked message is a safe no-op."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)

    backend.ack("my_queue", token=None)

    mock_consumer.ack.assert_not_called()


def test_ack_token_is_idempotent(mocker) -> None:
    backend, _, consumer, _ = _make_connected_backend(mocker)
    message = mocker.MagicMock(body=b"x")
    consumer.receive.return_value = [message]
    _body, token = backend.pop_with_ack("q", timeout=1)

    backend.ack("q", token=token)
    backend.ack("q", token=token)
    backend.nack("q", token=token)

    consumer.ack.assert_called_once_with(message)
    consumer.change_invisible_duration.assert_not_called()


def test_ack_and_nack_for_same_token_are_serialized(mocker) -> None:
    backend, _, consumer, _ = _make_connected_backend(mocker)
    message = mocker.MagicMock(body=b"x")
    consumer.receive.return_value = [message]
    _body, token = backend.pop_with_ack("q", timeout=1)
    ack_entered = threading.Event()
    release_ack = threading.Event()
    nack_started = threading.Event()
    errors: list[BaseException] = []

    def blocking_ack(_message) -> None:
        ack_entered.set()
        assert release_ack.wait(timeout=2.0)

    def settle(operation, started=None) -> None:
        if started is not None:
            started.set()
        try:
            operation("q", token=token)
        except BaseException as error:  # pragma: no cover - assertion aid
            errors.append(error)

    consumer.ack.side_effect = blocking_ack
    ack_thread = threading.Thread(target=settle, args=(backend.ack, None))
    nack_thread = threading.Thread(target=settle, args=(backend.nack, nack_started))
    ack_thread.start()
    assert ack_entered.wait(timeout=2.0)
    nack_thread.start()
    assert nack_started.wait(timeout=2.0)
    nack_thread.join(timeout=0.2)
    settlement_was_serialized = nack_thread.is_alive()
    release_ack.set()
    ack_thread.join(timeout=2.0)
    nack_thread.join(timeout=2.0)

    assert settlement_was_serialized
    assert not ack_thread.is_alive()
    assert not nack_thread.is_alive()
    assert errors == []
    consumer.ack.assert_called_once_with(message)
    consumer.change_invisible_duration.assert_not_called()


def test_failed_token_settlement_remains_retryable(mocker) -> None:
    backend, _, consumer, _ = _make_connected_backend(mocker)
    message = mocker.MagicMock(body=b"x")
    consumer.receive.return_value = [message]
    consumer.ack.side_effect = [RuntimeError("ack failed"), None]
    _body, token = backend.pop_with_ack("q", timeout=1)

    with pytest.raises(QueueError):
        backend.ack("q", token=token)
    backend.ack("q", token=token)
    backend.nack("q", token=token)

    assert consumer.ack.call_count == 2
    consumer.change_invisible_duration.assert_not_called()


def test_stale_token_does_not_ack_replacement_consumer(mocker) -> None:
    backend, _, old_consumer, _ = _make_connected_backend(mocker)
    message = mocker.MagicMock(body=b"x")
    old_consumer.receive.return_value = [message]
    _body, token = backend.pop_with_ack("q", timeout=1)
    backend.disconnect()
    new_consumer = mocker.MagicMock(is_running=True)
    backend._producer = mocker.MagicMock(is_running=True)
    backend._consumer = new_consumer
    backend._consumer_generation += 1

    backend.ack("q", token=token)
    backend.nack("q", token=token)

    new_consumer.ack.assert_not_called()
    new_consumer.change_invisible_duration.assert_not_called()


def test_legacy_ack_failure_keeps_delivery_for_retry(mocker) -> None:
    backend, _, consumer, _ = _make_connected_backend(mocker)
    message = mocker.MagicMock(body=b"x")
    consumer.receive.return_value = [message]
    consumer.ack.side_effect = [RuntimeError("ack failed"), None]
    backend.pop("q", timeout=1)

    with pytest.raises(QueueError):
        backend.ack("q")
    assert backend._last_msg is message

    backend.ack("q")

    assert consumer.ack.call_count == 2
    assert backend._last_msg is None


def test_nack_shortens_processing_lease_to_broker_floor(mocker) -> None:
    """RocketMQ has no zero-delay nack; 10s is the broker minimum."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    msg = mocker.MagicMock()
    msg.body = b"x"
    mock_consumer.receive.return_value = [msg]
    _body, token = backend.pop_with_ack("my_queue", timeout=1)

    backend.nack("my_queue", token=token)

    mock_consumer.ack.assert_not_called()
    mock_consumer.change_invisible_duration.assert_called_once_with(msg, 10)


def test_legacy_nack_shortens_lease_and_clears_last_message(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    msg = mocker.MagicMock()
    msg.body = b"payload"
    mock_consumer.receive.return_value = [msg]
    backend.pop("my_queue", timeout=1)

    backend.nack("my_queue")

    mock_consumer.change_invisible_duration.assert_called_once_with(msg, 10)
    assert backend._last_msg is None


def test_legacy_nack_failure_keeps_delivery_for_retry(mocker) -> None:
    backend, _, consumer, _ = _make_connected_backend(mocker)
    message = mocker.MagicMock(body=b"x")
    consumer.receive.return_value = [message]
    consumer.change_invisible_duration.side_effect = [RuntimeError("nack failed"), None]
    backend.pop("q", timeout=1)

    with pytest.raises(QueueError):
        backend.nack("q")
    assert backend._last_msg is message

    backend.nack("q")

    assert consumer.change_invisible_duration.call_count == 2
    assert backend._last_msg is None


def test_nack_failure_raises_queue_error(mocker) -> None:
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    msg = mocker.MagicMock()
    msg.body = b"x"
    mock_consumer.receive.return_value = [msg]
    _body, token = backend.pop_with_ack("my_queue", timeout=1)
    mock_consumer.change_invisible_duration.side_effect = RuntimeError("renew failed")

    with pytest.raises(QueueError) as exc_info:
        backend.nack("my_queue", token=token)

    assert exc_info.value.operation == "nack"


def test_rocketmq_requires_ack_class_attrs() -> None:
    """R6: RocketMQ declares the deferred-ack capability contract."""
    assert RocketMQBackend.requires_ack is True
    assert RocketMQBackend.supports_concurrent_ack is True


# ---------------------------------------------------------------------------
# _extract_body — defensive coercion of apache Message.body (any type → bytes)
# ---------------------------------------------------------------------------


def test_extract_body_none_returns_empty() -> None:
    """_extract_body returns b"" when the message has no body attr / body is None."""
    msg = MagicMock()
    del msg.body  # getattr(msg, "body", None) → None
    assert RocketMQBackend._extract_body(msg) == b""


def test_extract_body_bytearray() -> None:
    """_extract_body coerces bytearray to bytes."""
    msg = MagicMock()
    msg.body = bytearray(b"x")
    assert RocketMQBackend._extract_body(msg) == b"x"
    assert isinstance(RocketMQBackend._extract_body(msg), bytes)


def test_extract_body_memoryview() -> None:
    """_extract_body coerces memoryview to bytes."""
    msg = MagicMock()
    msg.body = memoryview(b"y")
    assert RocketMQBackend._extract_body(msg) == b"y"


def test_extract_body_str_encodes() -> None:
    """_extract_body utf-8-encodes a str body."""
    msg = MagicMock()
    msg.body = "héllo"
    assert RocketMQBackend._extract_body(msg) == "héllo".encode()


# ---------------------------------------------------------------------------
# is_connected / pump subscription — defensive exception branches
# ---------------------------------------------------------------------------


def test_is_connected_false_when_is_running_raises(mocker) -> None:
    """is_connected swallows a client is_running access failure and returns
    False (defensive — is_connected must never raise).

    ``is_running`` is a bool PROPERTY on the apache client; we swap in a tiny
    stand-in whose property raises (MagicMock's auto-child protocol defeats
    PropertyMock, so a real object is the clean way to exercise the branch).
    """

    class _RaisingProducer:
        @property
        def is_running(self) -> bool:
            raise RuntimeError("client closed")

        def shutdown(self) -> None:
            pass

    backend, _, _, _ = _make_connected_backend(mocker)
    backend._producer = _RaisingProducer()

    assert backend.is_connected() is False


def test_receive_pump_surfaces_subscribe_failure(mocker) -> None:
    """A pump subscription failure must not masquerade as an empty queue."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    mock_consumer.subscribe.side_effect = RuntimeError("transient")
    mock_consumer.receive.return_value = []

    with pytest.raises(QueueError) as exc_info:
        backend.pop("flaky_queue", timeout=1)

    assert exc_info.value.queue_name is None
    assert exc_info.value.operation == "pop"
    mock_consumer.receive.assert_not_called()


def test_ack_failure_raises_queue_error(mocker) -> None:
    """ack wraps a broker-side ack failure in QueueError(operation='ack')."""
    backend, _, mock_consumer, _ = _make_connected_backend(mocker)
    msg = mocker.MagicMock()
    msg.body = b"x"
    mock_consumer.receive.return_value = [msg]
    _body, token = backend.pop_with_ack("q", timeout=1)
    mock_consumer.ack.side_effect = OSError("broker gone")

    with pytest.raises(QueueError, match="Failed to ack RocketMQ message") as exc_info:
        backend.ack("q", token=token)
    assert exc_info.value.operation == "ack"


@pytest.mark.parametrize(
    "method", ["push", "pop", "pop_with_ack", "queue_len", "clear_queue"]
)
def test_queue_methods_validate_names_before_driver_call(mocker, method) -> None:
    backend, producer, consumer, _ = _make_connected_backend(mocker)
    args = ("bad name!", b"x") if method == "push" else ("bad name!",)

    with pytest.raises(ValueError):
        getattr(backend, method)(*args)

    producer.send.assert_not_called()
    consumer.subscribe.assert_not_called()
    consumer.receive.assert_not_called()


# ---------------------------------------------------------------------------
# queue_len
# ---------------------------------------------------------------------------


def _reset_queue_len_warned() -> None:
    """Reset the module-level warn-once flag (Risk 1) for test isolation."""
    from scrapy_extension.backends import rocketmq as rocketmq_module

    rocketmq_module._queue_len_warned = False


def test_queue_len_reports_unsupported_risk1() -> None:
    """Unsupported depth must not masquerade as a confirmed empty queue."""
    _reset_queue_len_warned()
    backend = RocketMQBackend(RocketMQSettings())
    with pytest.raises(NotImplementedError, match="broker-side depth RPC"):
        backend.queue_len("test_queue")


def test_queue_len_rebuilds_static_capability_error_without_backend_graph() -> None:
    marker = "round44-rocketmq-depth-private-marker"
    backend = RocketMQBackend(
        RocketMQSettings(
            namesrv_address=f"{marker}.example:8081", allow_remote_plaintext=True
        )
    )
    _reset_queue_len_warned()

    with pytest.raises(NotImplementedError) as exc_info:
        backend.queue_len(marker)

    error = exc_info.value
    assert type(error) is NotImplementedError
    assert str(error) == "RocketMQ queue depth is unsupported: no broker-side depth RPC"
    _assert_capability_error_is_redacted(error, marker)


def test_queue_len_proxy_keeps_static_capability_error_and_breaker_neutral() -> None:
    marker = "round44-rocketmq-depth-proxy-marker"
    backend = RocketMQBackend(
        RocketMQSettings(
            namesrv_address=f"{marker}.example:8081", allow_remote_plaintext=True
        )
    )
    breaker = CircuitBreaker("rocketmq-capability-depth", failure_threshold=1)
    proxy = wrap_queue_backend(backend, breaker)
    _reset_queue_len_warned()

    with pytest.raises(NotImplementedError) as exc_info:
        proxy.queue_len(marker)

    assert str(exc_info.value) == (
        "RocketMQ queue depth is unsupported: no broker-side depth RPC"
    )
    assert breaker.state is BreakerState.CLOSED
    assert breaker.failure_count == 0
    _assert_capability_error_is_redacted(exc_info.value, marker)


def test_queue_len_warns_once_risk1(caplog) -> None:
    """Risk 1: queue_len emits a one-time WARNING explaining depth unsupported."""
    import logging

    _reset_queue_len_warned()
    backend = RocketMQBackend(RocketMQSettings())
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.backends.rocketmq"):
        for _ in range(2):
            with pytest.raises(NotImplementedError):
                backend.queue_len("test_queue")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "unsupported" in warnings[0].message


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("warning failed"), KeyboardInterrupt(), SystemExit(1)],
)
def test_queue_len_preserves_unsupported_contract_when_warning_is_interrupted(
    mocker, diagnostic_error: BaseException
) -> None:
    """The pure warning cannot replace RocketMQ's required capability signal."""
    _reset_queue_len_warned()
    backend = RocketMQBackend(RocketMQSettings())
    mocker.patch(
        "scrapy_extension.backends.rocketmq.logger.warning",
        side_effect=diagnostic_error,
    )

    with pytest.raises(NotImplementedError, match="broker-side depth RPC"):
        backend.queue_len("test_queue")


def test_unsupported_depth_keeps_scheduler_conservative() -> None:
    _reset_queue_len_warned()
    backend = RocketMQBackend(RocketMQSettings())
    queue = MagicMock(name="BackendQueue")
    queue.__len__.side_effect = lambda: backend.queue_len("test_queue")
    queue.pop.return_value = None
    scheduler = BackendScheduler(
        connection_manager=MagicMock(name="ConnectionManager"),
        backpressure_pause_at=1,
    )
    scheduler._queue = queue

    assert scheduler.has_pending_requests() is True
    assert scheduler.next_request() is None
    queue.pop.assert_called_once_with(timeout=0)


# ---------------------------------------------------------------------------
# clear_queue
# ---------------------------------------------------------------------------


def test_clear_queue_not_connected() -> None:
    """clear_queue raises QueueError when not connected."""
    backend = RocketMQBackend(RocketMQSettings())
    with pytest.raises(QueueError) as exc_info:
        backend.clear_queue("test_queue")
    assert "Not connected" in str(exc_info.value)
    assert exc_info.value.queue_name is None
    assert exc_info.value.operation == "clear_queue"


def test_clear_queue_connected(mocker) -> None:
    """clear_queue reports that broker-side purge is unsupported."""
    backend, _, _, _ = _make_connected_backend(mocker)
    with pytest.raises(QueueError) as exc_info:
        backend.clear_queue("test_queue")
    assert "not supported" in str(exc_info.value)
    assert exc_info.value.queue_name is None
    assert exc_info.value.operation == "clear_queue"


# ---------------------------------------------------------------------------
# Set / Storage — RocketMQBackend (queue) does NOT carry set/storage methods
# ---------------------------------------------------------------------------


def test_rocketmq_backend_has_no_set_methods() -> None:
    """E3: RocketMQBackend (queue) no longer carries SetBackend methods."""
    backend = RocketMQBackend(RocketMQSettings())
    for attr in ("add", "remove", "contains", "set_len", "clear_set"):
        assert not hasattr(backend, attr), (
            f"RocketMQBackend should not expose set method {attr!r} "
            f"(moved to guard class RocketMQSetBackend)"
        )


def test_rocketmq_backend_has_no_storage_methods() -> None:
    """E3: RocketMQBackend (queue) no longer carries StorageBackend methods."""
    backend = RocketMQBackend(RocketMQSettings())
    for attr in (
        "store",
        "retrieve",
        "delete",
        "exists",
        "ttl",
        "clear_storage",
    ):
        assert not hasattr(backend, attr), (
            f"RocketMQBackend should not expose storage method {attr!r} "
            f"(moved to guard class RocketMQStorageBackend)"
        )


# ---------------------------------------------------------------------------
# Settings tests
# ---------------------------------------------------------------------------


def test_rocketmq_settings_defaults() -> None:
    """Test RocketMQSettings default values."""
    settings = RocketMQSettings()
    assert settings.mode == RocketMQMode.STANDALONE
    assert settings.namesrv_address == "localhost:8081"
    assert settings.consumer_group == "scrapy-extension-consumer"
    assert settings.topic_prefix == "scrapy-queue"
    assert settings.max_message_size == 1024 * 1024
    assert settings.send_timeout == 3000
    assert settings.invisible_duration == 300
    assert settings.tls_enabled is False


def test_rocketmq_settings_custom_values() -> None:
    """Test RocketMQSettings with custom values."""
    settings = RocketMQSettings(
        mode=RocketMQMode.CLUSTER,
        namesrv_address="rocketmq-cluster:9876",
        access_key=SecretStr("mykey"),
        secret_key=SecretStr("mysecret"),
        tls_enabled=True,
        consumer_group="my-consumer",
        topic_prefix="my-queue",
        invisible_duration=600,
    )
    assert settings.mode == RocketMQMode.CLUSTER
    assert settings.namesrv_address == "rocketmq-cluster:9876"
    assert settings.access_key is not None
    assert settings.access_key.get_secret_value() == "mykey"
    assert settings.secret_key is not None
    assert settings.secret_key.get_secret_value() == "mysecret"
    assert settings.invisible_duration == 600


@pytest.mark.parametrize("duration", [0, 1, 9, 43_201])
def test_rocketmq_invisible_duration_rejects_outside_broker_range(
    duration: int,
) -> None:
    with pytest.raises(ValueError, match="invisible_duration"):
        RocketMQSettings(invisible_duration=duration)


@pytest.mark.parametrize("duration", [10, 43_200])
def test_rocketmq_invisible_duration_accepts_broker_boundaries(
    duration: int,
) -> None:
    assert RocketMQSettings(invisible_duration=duration).invisible_duration == duration


def test_rocketmq_mode_enum_values() -> None:
    """Test RocketMQMode enum values."""
    assert RocketMQMode.STANDALONE.value == "standalone"
    assert RocketMQMode.CLUSTER.value == "cluster"
    assert RocketMQMode.CLOUD.value == "cloud"


def test_rocketmq_settings_env_prefix(monkeypatch) -> None:
    """Test RocketMQSettings respects env prefix."""
    monkeypatch.setenv("SCRAPY_ROCKETMQ_NAMESRV_ADDRESS", "env-rocketmq:9876")
    monkeypatch.setenv("SCRAPY_ROCKETMQ_ALLOW_REMOTE_PLAINTEXT", "true")
    settings = RocketMQSettings()
    assert settings.namesrv_address == "env-rocketmq:9876"


def test_rocketmq_settings_cloud_mode() -> None:
    """Test RocketMQSettings cloud mode defaults."""
    settings = RocketMQSettings(
        mode=RocketMQMode.CLOUD,
        access_key=SecretStr("key"),
        secret_key=SecretStr("secret"),
        tls_enabled=True,
    )
    assert settings.mode == RocketMQMode.CLOUD
    assert settings.tls_enabled is True


def test_rocketmq_settings_none_keys() -> None:
    """Test RocketMQSettings with explicit None keys."""
    settings = RocketMQSettings(access_key=None, secret_key=None)
    assert settings.access_key is None
    assert settings.secret_key is None


# ---------------------------------------------------------------------------
# Config-time capability guard — resolve_backend_config rejects RocketMQ
# ---------------------------------------------------------------------------


class _FakeSettings:
    """Minimal Scrapy-Settings-like object for resolve_backend_config tests."""

    def __init__(self, type_value: str) -> None:
        self._type_value = type_value

    def get(self, key, default=None):
        if key == "SCRAPY_BACKEND_TYPE":
            return self._type_value
        return default

    def getdict(self, key, default=None):  # noqa: ARG002 - Scrapy Settings.getdict signature
        del key
        if default is None:
            return {}
        return default


def test_resolve_backend_config_rejects_rocketmq_for_set() -> None:
    """Layer-2 guard: configuring RocketMQ for the set component fails at config time."""
    assert BackendType.ROCKETMQ not in SET_CAPABLE_BACKENDS

    settings = _FakeSettings(type_value="rocketmq")
    with pytest.raises(ConfigurationError) as exc_info:
        resolve_backend_config(
            settings,
            type_key="SCRAPY_SET_BACKEND_TYPE",
            settings_key="SCRAPY_SET_BACKEND_SETTINGS",
            required_capabilities={"set"},
            component_name="set",
        )
    msg = str(exc_info.value)
    assert "rocketmq" in msg
    assert "set" in msg
    assert "redis" in msg


def test_resolve_backend_config_rejects_rocketmq_for_storage() -> None:
    """Layer-2 guard: configuring RocketMQ for the storage component fails at config time."""
    assert BackendType.ROCKETMQ not in STORAGE_CAPABLE_BACKENDS

    settings = _FakeSettings(type_value="rocketmq")
    with pytest.raises(ConfigurationError) as exc_info:
        resolve_backend_config(
            settings,
            type_key="SCRAPY_STORAGE_BACKEND_TYPE",
            settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
            required_capabilities={"storage"},
            component_name="storage",
        )
    msg = str(exc_info.value)
    assert "rocketmq" in msg
    assert "storage" in msg


def test_resolve_backend_config_accepts_rocketmq_for_queue() -> None:
    """Sanity: RocketMQ IS queue-capable, so the queue config path succeeds."""
    from scrapy_extension.backends.connectors import QUEUE_CAPABLE_BACKENDS

    assert BackendType.ROCKETMQ in QUEUE_CAPABLE_BACKENDS

    settings = _FakeSettings(type_value="rocketmq")
    backend_type, backend_settings = resolve_backend_config(
        settings,
        type_key="SCRAPY_QUEUE_BACKEND_TYPE",
        settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
        required_capabilities={"queue"},
        component_name="queue",
    )
    assert backend_type == "rocketmq"
    assert backend_settings == {}


# ---------------------------------------------------------------------------
# E3: class-level guard — RocketMQSetBackend / RocketMQStorageBackend
# ---------------------------------------------------------------------------


def test_rocketmq_set_backend_construction_raises_configuration_error() -> None:
    """E3: instantiating RocketMQSetBackend fails fast with a typed error."""
    from scrapy_extension.backends.rocketmq import RocketMQSetBackend

    with pytest.raises(ConfigurationError) as exc_info:
        RocketMQSetBackend(RocketMQSettings())

    msg = str(exc_info.value)
    assert "RocketMQ" in msg
    assert "set" in msg.lower()
    assert "SCRAPY_SET_BACKEND_TYPE" in msg


def test_rocketmq_storage_backend_construction_raises_configuration_error() -> None:
    """E3: instantiating RocketMQStorageBackend fails fast with a typed error."""
    from scrapy_extension.backends.rocketmq import RocketMQStorageBackend

    with pytest.raises(ConfigurationError) as exc_info:
        RocketMQStorageBackend(RocketMQSettings())

    msg = str(exc_info.value)
    assert "RocketMQ" in msg
    assert "storage" in msg.lower()
    assert "SCRAPY_STORAGE_BACKEND_TYPE" in msg


def test_rocketmq_set_backend_class_is_importable() -> None:
    """E3: the guard classes remain importable (lazy-import architecture preserved)."""
    from scrapy_extension.backends.rocketmq import (
        RocketMQBackend,
        RocketMQSetBackend,
        RocketMQStorageBackend,
    )

    assert RocketMQBackend is not None
    assert RocketMQSetBackend is not None
    assert RocketMQStorageBackend is not None
