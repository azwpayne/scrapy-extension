"""Tests for connection manager."""

import logging
import sys
import traceback
from decimal import Decimal
from fractions import Fraction
from types import SimpleNamespace

import pytest

from scrapy_extension.backends import connectors as connectors_module
from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    _DurablePushRequired,
    _QueuePushReceipt,
)
from scrapy_extension.backends.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    CircuitBreakerOpenError,
)
from scrapy_extension.backends.connectors import (
    ConnectionManager,
    _rebuild_connect_attempt_error,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    BackendError,
    ConfigurationError,
    QueueError,
)


def _assert_package_traceback_locals_are_redacted(
    error: BaseException,
    marker: str,
) -> None:
    """Check package frames cannot expose a rejected configuration input."""
    trace = error.__traceback__
    while trace is not None:
        frame = trace.tb_frame
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            locals_snapshot = frame.f_locals
            assert marker not in repr(locals_snapshot)
            for local in locals_snapshot.values():
                if type(local) is ConnectionManager:
                    settings = vars(local).get("settings")
                    if settings is not None:
                        assert marker not in repr(settings)
                if type(local) is tuple:
                    for argument in local:
                        if type(argument) is ConnectionManager:
                            settings = vars(argument).get("settings")
                            if settings is not None:
                                assert marker not in repr(settings)
        trace = trace.tb_next


def test_connection_manager_rejects_hostile_outer_inputs_without_dispatch():
    marker = "manager-hostile-outer-input-marker"
    calls: list[str] = []

    class _HostileBackendType(str):
        def __hash__(self) -> int:
            calls.append("hash")
            raise RuntimeError(marker)

        def __format__(self, format_spec: str) -> str:
            del format_spec
            calls.append("format")
            raise RuntimeError(marker)

    class _HostileSettings(dict[str, object]):
        def __bool__(self) -> bool:
            calls.append("bool")
            raise RuntimeError(marker)

        def __deepcopy__(self, memo: object) -> object:
            del memo
            calls.append("deepcopy")
            raise RuntimeError(marker)

    for factory in (
        lambda: ConnectionManager(_HostileBackendType("redis")),  # type: ignore[arg-type]
        lambda: ConnectionManager("redis", _HostileSettings()),  # type: ignore[arg-type]
        lambda: ConnectionManager.get_manager(
            _HostileBackendType("redis")  # type: ignore[arg-type]
        ),
        lambda: ConnectionManager.get_manager(
            "redis",
            _HostileSettings(),  # type: ignore[arg-type]
        ),
    ):
        with pytest.raises(ConfigurationError) as exc_info:
            factory()
        error = exc_info.value
        assert error.setting_name == "backend_settings"
        assert marker not in str(error)
        assert marker not in repr(error.__dict__)
        _assert_package_traceback_locals_are_redacted(error, marker)

    assert calls == []


def test_rebuild_lazy_attempt_errors_never_reuses_raw_failure_graph():
    """Shared lazy-attempt errors are new, static objects for every safe type."""
    marker = "lazy-attempt-error-rebuild-secret-marker"

    import_error = _rebuild_connect_attempt_error(ImportError(marker))
    assert isinstance(import_error, ImportError)
    assert str(import_error) == (
        "Selected backend could not be initialized because an import failed."
    )
    assert marker not in repr(import_error)

    connection_error = _rebuild_connect_attempt_error(
        BackendConnectionError(marker, backend_type=marker)
    )
    assert isinstance(connection_error, BackendConnectionError)
    assert str(connection_error) == (
        "Connection manager failed to connect to the selected backend."
    )
    assert connection_error.backend_type == "connection-manager"
    assert marker not in repr(connection_error.__dict__)

    safe_configuration_error = _rebuild_connect_attempt_error(
        ConfigurationError(
            "Invalid backend setting 'retry_attempts'.",
            setting_name="retry_attempts",
        )
    )
    assert isinstance(safe_configuration_error, ConfigurationError)
    assert str(safe_configuration_error) == "Invalid backend setting 'retry_attempts'."
    assert safe_configuration_error.setting_name == "retry_attempts"

    unsafe_configuration_error = _rebuild_connect_attempt_error(
        ConfigurationError(marker, setting_name=marker, setting_value=marker)
    )
    assert isinstance(unsafe_configuration_error, ConfigurationError)
    assert (
        str(unsafe_configuration_error)
        == "Connection manager configuration is invalid."
    )
    assert unsafe_configuration_error.setting_name == "configuration"
    assert unsafe_configuration_error.setting_value is None
    assert marker not in repr(unsafe_configuration_error.__dict__)

    unexpected_error = _rebuild_connect_attempt_error(RuntimeError(marker))
    assert isinstance(unexpected_error, ConfigurationError)
    assert str(unexpected_error) == "Connection manager configuration is invalid."
    assert unexpected_error.setting_name == "configuration"
    assert marker not in repr(unexpected_error.__dict__)

    interrupt = KeyboardInterrupt(marker)
    assert _rebuild_connect_attempt_error(interrupt) is interrupt


def test_get_manager_discards_failed_deepcopy_snapshot():
    marker = "manager-deepcopy-snapshot-marker"
    before = dict(ConnectionManager._managers)

    class _ExplosiveSnapshotValue:
        def __deepcopy__(self, memo: object) -> object:
            del memo
            raise RuntimeError(marker)

    with pytest.raises(ConfigurationError) as exc_info:
        ConnectionManager.get_manager("redis", {"value": _ExplosiveSnapshotValue()})

    error = exc_info.value
    assert error.setting_name == "backend_settings"
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)
    assert dict(ConnectionManager._managers) == before


def test_close_sanitizes_failed_registry_key_normalization():
    marker = "manager-close-normalization-marker"

    class _ExplosiveRegistryValue:
        def __getattribute__(self, name: str) -> object:
            if name == "__dict__":
                raise RuntimeError(marker)
            return super().__getattribute__(name)

    manager = ConnectionManager("redis", {"value": _ExplosiveRegistryValue()})

    with pytest.raises(ConfigurationError) as exc_info:
        manager.close()

    error = exc_info.value
    assert str(error) == "Connection manager configuration is invalid."
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)


def test_connection_manager_get_manager_singleton():
    """Test that get_manager returns singleton for same params."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS)
    assert manager1 is manager2


def test_connection_manager_close_evicts_from_registry():
    """R1-P1-8: close() must remove the manager from the class-level registry.

    Without eviction, get_manager returns the closed instance on the next call
    — masking state across reconnect cycles and across tests.
    """
    manager = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "close-test-host"}
    )
    assert manager.settings == {"host": "close-test-host"}

    manager.close()

    # Registry no longer contains the key; a new get_manager creates a fresh instance.
    manager_after = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "close-test-host"}
    )
    assert manager_after is not manager


def test_close_bare_instance_does_not_evict_registered_peer():
    """A bare ``ConnectionManager(...)`` (not inserted via get_manager) sharing a
    registry key with a registered peer must NOT evict that peer on close().

    ``close()`` evicts by registry key (``cls._managers.pop(key, None)``). Without
    an identity check, a bare instance constructed in tests — whose key collides
    with a registered manager — silently evicts the peer on close(): the peer
    disappears from the registry while still held by its caller, so the next
    ``get_manager(same key)`` creates a second live manager (split-brain /
    connection leak). The fix guards the pop with ``is self``.
    """
    registered = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "bare-peer-host"}
    )
    key = ConnectionManager._registry_key(BackendType.REDIS, {"host": "bare-peer-host"})
    assert ConnectionManager._managers.get(key) is registered

    # Bare instance — NOT inserted via get_manager — sharing the same key.
    bare = ConnectionManager(BackendType.REDIS, {"host": "bare-peer-host"})
    assert bare._users == 0
    bare.close()  # must not raise, must not evict the registered peer

    # The registered peer is STILL in the registry (bare close did not evict it).
    assert ConnectionManager._managers.get(key) is registered, (
        "bare ConnectionManager.close() evicted a registered peer sharing the key"
    )

    # Cleanup.
    registered.close()


def test_pooled_manager_pins_top_level_settings_and_registry_token(mocker):
    """Public setting mutation cannot retarget or strand a pooled manager."""
    original_settings = {"host": "acquired-host"}
    manager = ConnectionManager.get_manager(BackendType.REDIS, original_settings)
    original_key = ConnectionManager._registry_key(BackendType.REDIS, original_settings)

    manager.settings["host"] = "mutated-host"

    settings_cls = mocker.patch("scrapy_extension.settings.RedisSettings")
    settings_cls.model_fields = {"host": object()}
    settings_obj = mocker.MagicMock(name="RedisSettings")
    settings_cls.return_value = settings_obj
    backend_cls = mocker.patch("scrapy_extension.backends.redis.RedisBackend")

    manager._create_backend()
    reacquired = ConnectionManager.get_manager(BackendType.REDIS, original_settings)

    settings_cls.assert_called_once_with(host="acquired-host")
    backend_cls.assert_called_once_with(settings_obj)
    assert reacquired is manager
    assert manager._users == 2

    manager.close()
    assert ConnectionManager._managers.get(original_key) is manager
    reacquired.close()
    assert original_key not in ConnectionManager._managers


def test_pooled_manager_pins_nested_settings_snapshot(mocker):
    """Nested public mutations cannot alter pooled backend construction."""
    original_settings = {"sentinels": ["127.0.0.1:26379"]}
    manager = ConnectionManager.get_manager(BackendType.REDIS, original_settings)

    manager.settings["sentinels"].append("mutated.invalid:26379")

    settings_cls = mocker.patch("scrapy_extension.settings.RedisSettings")
    settings_cls.model_fields = {"sentinels": object()}
    settings_obj = mocker.MagicMock(name="RedisSettings")
    settings_cls.return_value = settings_obj
    backend_cls = mocker.patch("scrapy_extension.backends.redis.RedisBackend")

    manager._create_backend()

    settings_cls.assert_called_once_with(sentinels=["127.0.0.1:26379"])
    backend_cls.assert_called_once_with(settings_obj)
    assert manager.settings == {
        "sentinels": ["127.0.0.1:26379", "mutated.invalid:26379"]
    }


def test_pooled_manager_pins_backend_type_for_registry_and_operations(mocker):
    """Public backend-type mutation cannot retarget a pooled manager."""
    settings = {"host": "backend-type-pin-host"}
    manager = ConnectionManager.get_manager(BackendType.REDIS, settings)
    original_key = ConnectionManager._registry_key(BackendType.REDIS, settings)

    manager.backend_type = BackendType.MONGODB

    settings_cls = mocker.patch("scrapy_extension.settings.RedisSettings")
    settings_cls.model_fields = {"host": object()}
    settings_obj = mocker.MagicMock(name="RedisSettings")
    settings_cls.return_value = settings_obj
    redis_backend_cls = mocker.patch("scrapy_extension.backends.redis.RedisBackend")
    mongodb_backend_cls = mocker.patch(
        "scrapy_extension.backends.mongodb.MongoDBBackend"
    )

    manager._create_backend()
    reacquired = ConnectionManager.get_manager(BackendType.REDIS, settings)

    settings_cls.assert_called_once_with(host="backend-type-pin-host")
    redis_backend_cls.assert_called_once_with(settings_obj)
    mongodb_backend_cls.assert_not_called()
    assert reacquired is manager

    manager.close()
    reacquired.close()
    assert original_key not in ConnectionManager._managers


def test_get_manager_replaces_retired_stale_registry_entry(mocker):
    """A retired entry left in the registry is never reacquired."""
    settings = {"host": "retired-stale-host"}
    stale = ConnectionManager.get_manager(BackendType.REDIS, settings)
    stale_backend = mocker.MagicMock(name="stale-backend")
    stale._backend = stale_backend
    stale._retired = True
    key = ConnectionManager._registry_key(BackendType.REDIS, settings)
    assert ConnectionManager._managers.get(key) is stale

    fresh = ConnectionManager.get_manager(BackendType.REDIS, settings)

    assert fresh is not stale
    assert fresh._retired is False
    assert fresh._users == 1
    assert ConnectionManager._managers.get(key) is fresh
    stale_backend.disconnect.assert_called_once_with()

    # The stale holder still releases its own refcount, but its saved token and
    # the identity guard prevent it from evicting the fresh replacement.
    stale.close()
    assert ConnectionManager._managers.get(key) is fresh


def test_connection_manager_clear_registry():
    """R1-P1-8: clear_registry() wipes all managers — for test isolation."""
    ConnectionManager.get_manager(BackendType.REDIS, {"host": "h1"})
    ConnectionManager.get_manager(BackendType.REDIS, {"host": "h2"})
    assert len(ConnectionManager._managers) >= 2

    ConnectionManager.clear_registry()

    assert ConnectionManager._managers == {}


def test_connection_manager_different_params():
    """Test that different params return different managers."""
    manager1 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "localhost"})
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "other"})
    assert manager1 is not manager2


def test_connection_manager_create_mongodb_backend(mocker):
    """Test ConnectionManager creates MongoDB backend."""
    mock_backend = mocker.patch("scrapy_extension.backends.mongodb.MongoDBBackend")
    mock_instance = mocker.MagicMock()
    mock_backend.return_value = mock_instance

    manager = ConnectionManager(BackendType.MONGODB)
    backend = manager._create_backend()

    mock_backend.assert_called_once()
    assert backend == mock_instance


def test_connection_manager_create_kafka_backend(mocker):
    """Test ConnectionManager creates Kafka backend."""
    mock_backend = mocker.patch("scrapy_extension.backends.kafka.KafkaBackend")
    mock_instance = mocker.MagicMock()
    mock_backend.return_value = mock_instance

    manager = ConnectionManager(BackendType.KAFKA)
    backend = manager._create_backend()

    mock_backend.assert_called_once()


def test_connection_manager_create_rabbitmq_backend(mocker):
    """Test ConnectionManager creates RabbitMQ backend."""
    mock_backend = mocker.patch("scrapy_extension.backends.rabbitmq.RabbitMQBackend")
    mock_instance = mocker.MagicMock()
    mock_backend.return_value = mock_instance

    manager = ConnectionManager(BackendType.RABBITMQ)
    backend = manager._create_backend()

    mock_backend.assert_called_once()


def test_connection_manager_get_manager_same_settings_order():
    """Same settings with different key order should resolve to same manager."""
    settings_a = {"a": 1, "b": 2}
    settings_b = {"b": 2, "a": 1}

    manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings_a)
    manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings_b)

    assert manager1 is manager2


def test_connection_manager_get_set_backend_not_supported(mocker):
    """get_set_backend should raise NotImplementedError for unsupported backend."""
    manager = ConnectionManager(BackendType.KAFKA)
    # We need to set _backend to something that is not a SetBackend but is a Backend subclass
    mock_backend = mocker.MagicMock()
    mock_backend.is_connected.return_value = True
    manager._backend = mock_backend

    with pytest.raises(NotImplementedError):
        manager.get_set_backend()


def test_attempt_connection_calls_disconnect_on_failure(mocker):
    """R25-A1: failed connect() must release backend resources (pools, sockets).

    Without this guard, each retry leaks one Redis/MongoDB connection pool.
    RedisBackend.connect() allocates the client (and its pool) at line 150,
    then pings at line 151. A ping failure leaves ``self._client`` holding
    an orphaned pool. On retry, ConnectionManager creates a NEW backend
    with a NEW pool; the old one is garbage-collected without ``close()``,
    leaking the pool until the GC finalizer runs (which redis-py doesn't
    guarantee promptly).
    """
    manager = ConnectionManager(BackendType.REDIS)

    mock_backend = mocker.MagicMock()
    mock_backend.connect.side_effect = ConnectionError("ping failed")
    mocker.patch.object(manager, "_create_backend", return_value=mock_backend)

    with pytest.raises(ConnectionError):
        manager._attempt_connection()

    mock_backend.connect.assert_called_once()
    mock_backend.disconnect.assert_called_once()


def test_attempt_connection_disconnect_failure_is_swallowed(mocker):
    """R25-A1: cleanup failures during connect-failure path must not mask the original error.

    If backend.disconnect() itself raises (e.g., broken pipe on attempted
    close), we should still propagate the original connect error, not the
    cleanup error. The operator needs to know the connect failed, not that
    cleanup also failed.
    """
    manager = ConnectionManager(BackendType.REDIS)

    mock_backend = mocker.MagicMock()
    mock_backend.connect.side_effect = ConnectionError("original connect failure")
    mock_backend.disconnect.side_effect = RuntimeError("cleanup also failed")
    mocker.patch.object(manager, "_create_backend", return_value=mock_backend)

    with pytest.raises(ConnectionError, match="original connect failure"):
        manager._attempt_connection()


def test_attempt_connection_cleans_up_after_baseexception(mocker):
    """A control-flow interruption still releases a half-built backend."""
    manager = ConnectionManager(BackendType.REDIS)
    mock_backend = mocker.MagicMock()
    original = KeyboardInterrupt()
    mock_backend.connect.side_effect = original
    mock_backend.disconnect.side_effect = SystemExit(2)
    mocker.patch.object(manager, "_create_backend", return_value=mock_backend)

    with pytest.raises(KeyboardInterrupt) as raised:
        manager._attempt_connection()

    assert raised.value is original
    mock_backend.disconnect.assert_called_once_with()
    assert manager._backend is None


def test_attempt_connection_close_wins_preserves_discard_error(mocker):
    """A close during connect keeps its causal terminal error despite cleanup.

    ``backend.connect()`` runs without the manager state lock.  The final holder
    can therefore retire the manager after the backend has connected but before
    ``_attempt_connection()`` publishes it.  The newly successful handle must
    be disconnected, but even a control-flow exception from that best-effort
    cleanup must not replace the typed ``BackendConnectionError`` explaining
    that close won the race.
    """
    import threading

    manager = ConnectionManager(BackendType.REDIS)
    connect_entered = threading.Event()
    allow_connect_return = threading.Event()

    def _connect() -> None:
        connect_entered.set()
        assert allow_connect_return.wait(timeout=5), "test did not release connect"

    backend = mocker.MagicMock()
    backend.connect.side_effect = _connect
    cleanup_signal = KeyboardInterrupt("cleanup interruption")
    backend.disconnect.side_effect = cleanup_signal
    mocker.patch.object(manager, "_create_backend", return_value=backend)

    outcome: list[BaseException] = []

    def _attempt() -> None:
        try:
            manager._attempt_connection()
        except BaseException as exc:  # noqa: BLE001 - capture the thread outcome
            outcome.append(exc)

    worker = threading.Thread(target=_attempt)
    worker.start()
    assert connect_entered.wait(timeout=5), "backend.connect() was not entered"

    # ``close()`` takes the terminal-state lock while the backend is still in
    # flight, so the worker deterministically sees a retired manager once it is
    # allowed to return from ``connect()``.
    manager.close()
    allow_connect_return.set()
    worker.join(timeout=5)

    assert not worker.is_alive(), "connection attempt did not finish"
    assert len(outcome) == 1
    assert isinstance(outcome[0], BackendConnectionError)
    assert "backend discarded" in str(outcome[0])
    backend.disconnect.assert_called_once_with()
    assert manager._backend is None


def test_attempt_connection_close_wins_preserves_discard_error_when_warning_interrupts(
    mocker,
):
    """Close-winning error survives a control exception from cleanup diagnostics."""
    import threading

    manager = ConnectionManager(BackendType.REDIS)
    connect_entered = threading.Event()
    allow_connect_return = threading.Event()

    def _connect() -> None:
        connect_entered.set()
        assert allow_connect_return.wait(timeout=5), "test did not release connect"

    backend = mocker.MagicMock()
    backend.connect.side_effect = _connect
    backend.disconnect.side_effect = RuntimeError("cleanup failure")
    mocker.patch.object(manager, "_create_backend", return_value=backend)
    mocker.patch(
        "scrapy_extension.backends.connectors.logger.warning",
        side_effect=SystemExit("diagnostic interruption"),
    )

    outcome: list[BaseException] = []

    def _attempt() -> None:
        try:
            manager._attempt_connection()
        except BaseException as exc:  # noqa: BLE001 - capture the thread outcome
            outcome.append(exc)

    worker = threading.Thread(target=_attempt)
    worker.start()
    assert connect_entered.wait(timeout=5), "backend.connect() was not entered"

    manager.close()
    allow_connect_return.set()
    worker.join(timeout=5)

    assert not worker.is_alive(), "connection attempt did not finish"
    assert len(outcome) == 1
    assert isinstance(outcome[0], BackendConnectionError)
    assert "backend discarded" in str(outcome[0])
    backend.disconnect.assert_called_once_with()
    assert manager._backend is None


def test_connect_with_retries_preserves_release_error_when_close_wins(mocker):
    """The retry loop keeps the typed release error when close wins the race.

    ``_connect_with_retries`` broad-catches every ordinary connection failure
    and reports the generic attempt-count tail.  A concurrent ``close()`` that
    retires the manager mid-attempt is not an ordinary failure: the typed
    ``BackendConnectionError`` from ``_attempt_connection()`` explains the
    actionable cause and must surface verbatim instead of being re-wrapped
    (with a miscounted attempt number) by the generic tail.
    """
    import threading

    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 1, "retry_delay": 0},
    )
    connect_entered = threading.Event()
    allow_connect_return = threading.Event()

    def _connect() -> None:
        connect_entered.set()
        assert allow_connect_return.wait(timeout=5), "test did not release connect"

    backend = mocker.MagicMock()
    backend.connect.side_effect = _connect
    mocker.patch.object(manager, "_create_backend", return_value=backend)

    outcome: list[BaseException] = []

    def _connect_with_retries() -> None:
        try:
            manager._connect_with_retries([])
        except BaseException as exc:  # noqa: BLE001 - capture the thread outcome
            outcome.append(exc)

    worker = threading.Thread(target=_connect_with_retries)
    worker.start()
    assert connect_entered.wait(timeout=5), "backend.connect() was not entered"

    # ``close()`` retires the manager while ``backend.connect()`` is still in
    # flight, so the retry loop deterministically observes a released manager
    # when the attempt resolves with the typed discard error.
    manager.close()
    allow_connect_return.set()
    worker.join(timeout=5)

    assert not worker.is_alive(), "connection attempt did not finish"
    assert len(outcome) == 1
    assert isinstance(outcome[0], BackendConnectionError)
    assert "backend discarded" in str(outcome[0])
    assert "Failed to connect after" not in str(outcome[0])
    backend.disconnect.assert_called_once_with()
    assert manager._backend is None


def test_attempt_connection_close_wins_logs_after_disconnect_context_unwinds(
    mocker,
) -> None:
    """A release-race diagnostic cannot expose the detached close exception."""
    import threading

    class _ExceptionContextProbe(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.contexts: list[tuple[object, object, object]] = []

        def emit(self, _record: logging.LogRecord) -> None:
            self.contexts.append(sys.exc_info())

    manager = ConnectionManager(BackendType.REDIS)
    connect_entered = threading.Event()
    allow_connect_return = threading.Event()

    def _connect() -> None:
        connect_entered.set()
        assert allow_connect_return.wait(timeout=5), "test did not release connect"

    backend = mocker.MagicMock()
    backend.connect.side_effect = _connect
    backend.disconnect.side_effect = RuntimeError("detached close failed")
    mocker.patch.object(manager, "_create_backend", return_value=backend)

    probe = _ExceptionContextProbe()
    previous_level = connectors_module.logger.level
    connectors_module.logger.setLevel(logging.WARNING)
    connectors_module.logger.addHandler(probe)
    outcome: list[BaseException] = []

    def _attempt() -> None:
        try:
            manager._attempt_connection()
        except BaseException as exc:  # noqa: BLE001 - capture worker terminal signal
            outcome.append(exc)

    try:
        worker = threading.Thread(target=_attempt)
        worker.start()
        assert connect_entered.wait(timeout=5), "backend.connect() was not entered"
        manager.close()
        allow_connect_return.set()
        worker.join(timeout=5)
    finally:
        connectors_module.logger.removeHandler(probe)
        connectors_module.logger.setLevel(previous_level)

    assert not worker.is_alive(), "connection attempt did not finish"
    assert len(outcome) == 1
    assert isinstance(outcome[0], BackendConnectionError)
    assert probe.contexts == [(None, None, None)]


def test_close_swallows_backend_disconnect_error_and_still_evicts(mocker):
    """R44-A1: close() must not propagate a backend-specific disconnect error.

    R25-A1 hardened the connect-path's disconnect cleanup with
    ``contextlib.suppress(Exception)`` because disconnecting a possibly-broken
    backend can raise anything (OSError from the socket layer, a
    backend-specific error the backend's own disconnect didn't swallow).
    ``close()`` faced the identical scenario but caught only
    ``(RuntimeError, ValueError, AttributeError)``. An ``OSError`` (or any
    backend exception outside that tuple) propagated out of close(), skipped
    the registry-eviction code that runs after the try/finally, and broke the
    caller's close chain (scheduler.close, _on_spider_closed). Now catches
    ``Exception`` so close() always completes cleanup — matching R25-A1.
    """
    # Register via get_manager so the eviction branch is exercisable. Unique
    # host isolates this test's registry key from other tests.
    manager = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "r44-close-error-test"}
    )

    mock_backend = mocker.MagicMock()
    # OSError is NOT a subclass of (RuntimeError, ValueError, AttributeError),
    # so the old narrow tuple would let it propagate out of close().
    mock_backend.disconnect.side_effect = OSError("broken pipe during close")
    manager._backend = mock_backend

    # Must not raise.
    manager.close()

    # Cleanup completed despite the disconnect error.
    assert manager._backend is None
    # Registry evicted even though disconnect raised (the code path after the
    # try/finally — the part the old bug skipped).
    key = ConnectionManager._registry_key(
        BackendType.REDIS, {"host": "r44-close-error-test"}
    )
    assert key not in ConnectionManager._managers


def test_connect_retry_backoff_outside_backend_lock(mocker):
    """A2: the interruptible retry wait must not hold ``_lock``.

    A slow retry delay must not block peer threads sharing the manager. This
    observes the wait seam and verifies the manager state lock remains available
    throughout every backoff.
    """

    manager = ConnectionManager(
        BackendType.REDIS, {"retry_attempts": 3, "retry_delay": 0.01}
    )

    # Force _create_backend to keep failing so all retry waits fire.
    mocker.patch.object(
        ConnectionManager,
        "_create_backend",
        side_effect=ConnectionError("transient"),
    )
    wait = mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )

    lock_held_during_wait: list[bool] = []

    def wait_observer(_retirement_event, _delay):
        acquired = manager._lock.acquire(blocking=False)
        lock_held_during_wait.append(not acquired)
        if acquired:
            manager._lock.release()
        return False

    wait.side_effect = wait_observer

    with pytest.raises(Exception, match="Failed to connect"):  # noqa: B017 - testing retry exhaustion
        manager.connect()

    assert wait.call_count == 3  # 1 initial attempt + 3 retries
    assert lock_held_during_wait, "retry wait was never observed"
    assert not any(lock_held_during_wait), (
        "_lock was held across retry backoff — this blocks peer threads sharing "
        "the manager. connect() must run its retry loop without holding _lock."
    )


@pytest.mark.parametrize(
    ("settings", "setting_name"),
    [
        ({"retry_attempts": -1}, "retry_attempts"),
        ({"retry_attempts": 21}, "retry_attempts"),
        ({"retry_attempts": True}, "retry_attempts"),
        ({"retry_attempts": "many"}, "retry_attempts"),
        ({"retry_delay": -0.1}, "retry_delay"),
        ({"retry_delay": float("inf")}, "retry_delay"),
        ({"retry_delay": True}, "retry_delay"),
        ({"manager_retry_attempts": -1}, "retry_attempts"),
        ({"manager_retry_delay": float("nan")}, "retry_delay"),
    ],
)
def test_connect_rejects_invalid_retry_policy_before_backend_creation(
    mocker, settings, setting_name
):
    manager = ConnectionManager(BackendType.REDIS, settings)
    create_backend = mocker.patch.object(manager, "_create_backend")

    with pytest.raises(ConfigurationError) as exc_info:
        manager.connect()

    assert exc_info.value.setting_name == setting_name
    create_backend.assert_not_called()


@pytest.mark.parametrize(
    "raw_attempts",
    [
        0.0,
        1.0,
        Decimal("1"),
        Fraction(1, 1),
        "",
        "00",
        "01",
        "+1",
        "-0",
        " 1",
        "1 ",
        "1.0",
        "1e0",
        "\uff11",
    ],
)
def test_connect_rejects_noncanonical_retry_attempts(mocker, raw_attempts):
    """Retry counts never truncate or accept alternate integer spellings."""
    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": raw_attempts},
    )
    create_backend = mocker.patch.object(manager, "_create_backend")

    with pytest.raises(ConfigurationError) as exc_info:
        manager.connect()

    assert exc_info.value.setting_name == "retry_attempts"
    create_backend.assert_not_called()


def test_connect_rejects_custom_retry_attempt_integer_conversion(mocker):
    """Arbitrary ``__int__`` hooks are not part of the retry-count wire format."""
    conversions: list[str] = []

    class _IntegerLike:
        def __int__(self) -> int:
            conversions.append("__int__")
            return 1

    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": _IntegerLike()},
    )
    create_backend = mocker.patch.object(manager, "_create_backend")

    with pytest.raises(ConfigurationError) as exc_info:
        manager.connect()

    assert exc_info.value.setting_name == "retry_attempts"
    assert conversions == []
    create_backend.assert_not_called()


@pytest.mark.parametrize(
    ("raw_attempts", "expected"),
    [(0, 0), (20, 20), ("0", 0), ("20", 20)],
)
def test_retry_policy_accepts_canonical_retry_attempt_boundaries(
    raw_attempts, expected
):
    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": raw_attempts},
    )

    assert manager._retry_policy()[0] == expected


@pytest.mark.parametrize("field", ["retry_attempts", "retry_delay"])
def test_connect_retry_policy_does_not_retain_raw_configuration_value(field):
    marker = f"manager-{field}-secret-marker"
    manager = ConnectionManager(BackendType.REDIS, {field: marker})

    with pytest.raises(ConfigurationError) as exc_info:
        manager.connect()

    error = exc_info.value
    assert error.setting_name == field
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.setting_value is None
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    ("field", "coercion_method"),
    [("retry_attempts", "__int__"), ("retry_delay", "__float__")],
)
def test_connect_retry_policy_sanitizes_custom_numeric_coercion(
    mocker, field, coercion_method
):
    """Custom conversion diagnostics are configuration errors, never retries."""
    marker = f"manager-{field}-coercion-secret-marker"

    class _ExplosiveNumber:
        def __int__(self) -> int:
            raise RuntimeError(marker)

        def __float__(self) -> float:
            raise RuntimeError(marker)

    value = _ExplosiveNumber()
    manager = ConnectionManager(BackendType.REDIS, {field: value})
    create_backend = mocker.patch.object(manager, "_create_backend")
    sleep = mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )

    with pytest.raises(ConfigurationError) as exc_info:
        manager.connect()

    error = exc_info.value
    assert coercion_method in {"__int__", "__float__"}
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    create_backend.assert_not_called()
    sleep.assert_not_called()


def test_connect_settings_validation_does_not_retain_secret_input():
    marker = "manager-settings-secret-marker"
    manager = ConnectionManager(BackendType.ELASTICSEARCH, {"api_key": [marker]})

    with pytest.raises(ConfigurationError) as exc_info:
        manager.connect()

    error = exc_info.value
    assert error.setting_name == "api_key"
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    assert manager._backend is None


def test_connect_normalizes_string_retry_policy(mocker):
    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": "0", "retry_delay": "0.25"},
    )
    backend = mocker.MagicMock()
    create_backend = mocker.patch.object(
        manager,
        "_create_backend",
        return_value=backend,
    )

    manager.connect()

    create_backend.assert_called_once_with()
    assert manager._backend is backend


def test_connect_does_not_retry_configuration_errors(mocker):
    """Static configuration cannot recover through network retry backoff."""
    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 3, "retry_delay": 0.25},
    )
    attempt = mocker.patch.object(
        manager,
        "_attempt_connection",
        side_effect=ConfigurationError(
            "invalid backend setting",
            setting_name="host",
        ),
    )
    sleep = mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )

    with pytest.raises(
        ConfigurationError,
        match="Connection manager configuration is invalid.",
    ):
        manager.connect()

    attempt.assert_called_once_with()
    sleep.assert_not_called()


def test_connect_sanitizes_plugin_validation_errors_without_retry(mocker):
    """A plugin's Pydantic error is configuration, never a raw public failure."""
    from pydantic import BaseModel, ValidationError

    marker = "manager-plugin-validation-error-marker"

    class _PluginModel(BaseModel):
        retries: int

    class _PluginValidationError(ValidationError):
        """Third-party validation subclasses must follow the same redaction path."""

    try:
        _PluginModel(retries=marker)
    except ValidationError as error:
        validation_error = _PluginValidationError.from_exception_data(
            "PluginSettings", error.errors()
        )
    else:  # pragma: no cover - documents the test fixture invariant
        raise AssertionError("fixture must create a ValidationError")

    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 3, "retry_delay": 0.25},
    )
    attempt = mocker.patch.object(
        manager,
        "_attempt_connection",
        side_effect=validation_error,
    )
    sleep = mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )

    with pytest.raises(ConfigurationError) as exc_info:
        manager.connect()

    sanitized_error = exc_info.value
    assert str(sanitized_error) == "Connection manager configuration is invalid."
    assert marker not in repr(sanitized_error.__dict__)
    assert marker not in "".join(traceback.format_exception(sanitized_error))
    assert sanitized_error.__cause__ is None
    assert sanitized_error.__context__ is None
    _assert_package_traceback_locals_are_redacted(sanitized_error, marker)
    attempt.assert_called_once_with()
    sleep.assert_not_called()


@pytest.mark.parametrize(
    "accessor",
    ["connect", "backend", "queue", "set", "storage", "durable_push"],
)
def test_manager_public_startup_boundaries_drop_settings_from_tracebacks(
    mocker, accessor
):
    """Every public startup route rebuilds failures after manager frames exit."""
    marker = f"manager-{accessor}-traceback-secret-marker"
    manager = ConnectionManager(
        BackendType.REDIS,
        {"api_key": marker, "retry_attempts": 0, "retry_delay": 0},
    )
    mocker.patch.object(
        manager, "_attempt_connection", side_effect=RuntimeError(marker)
    )

    with pytest.raises(BackendConnectionError) as exc_info:
        if accessor == "connect":
            manager.connect()
        elif accessor == "backend":
            _ = manager.backend
        elif accessor == "queue":
            manager.get_queue_backend()
        elif accessor == "set":
            manager.get_set_backend()
        elif accessor == "storage":
            manager.get_storage_backend()
        else:
            manager._push_queue_with_durability("jobs", b"payload")

    error = exc_info.value
    assert str(error) == "Failed to connect after 1 attempt."
    assert error.backend_type == "redis"
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)


@pytest.mark.parametrize("accessor", ["backend", "queue", "set", "storage"])
def test_manager_accessors_rebuild_configuration_errors_without_manager_frames(
    accessor,
):
    marker = f"manager-{accessor}-configuration-secret-marker"
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": marker})

    with pytest.raises(ConfigurationError) as exc_info:
        if accessor == "backend":
            _ = manager.backend
        elif accessor == "queue":
            manager.get_queue_backend()
        elif accessor == "set":
            manager.get_set_backend()
        else:
            manager.get_storage_backend()

    error = exc_info.value
    assert str(error) == "Connection manager configuration is invalid."
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)


@pytest.mark.parametrize("error_kind", ["connection", "configuration"])
def test_is_connected_rebuilds_plugin_failures_without_manager_frames(error_kind):
    marker = f"manager-is-connected-{error_kind}-secret-marker"
    manager = ConnectionManager(BackendType.REDIS, {"api_key": marker})

    class _Backend:
        def is_connected(self) -> bool:
            if error_kind == "connection":
                raise BackendConnectionError(marker, backend_type="redis")
            raise ConfigurationError(marker, setting_name="api_key")

    manager._backend = _Backend()  # type: ignore[assignment]

    expected_type = (
        BackendConnectionError if error_kind == "connection" else ConfigurationError
    )
    with pytest.raises(expected_type) as exc_info:
        manager.is_connected()

    error = exc_info.value
    assert marker not in str(error)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)


def test_connect_replaces_existing_disconnected_backend(mocker):
    """An existing object is not evidence of a live backend connection.

    A backend can lose connectivity after its initial successful connection.
    Explicit ``connect()`` is the recovery API, so it must health-check the
    published backend and replace a disconnected instance instead of returning
    early solely because ``_backend`` is non-None.
    """
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    stale_backend = mocker.MagicMock(name="stale-backend")
    stale_backend.is_connected.return_value = False
    replacement = mocker.MagicMock(name="replacement-backend")
    manager._backend = stale_backend
    create_backend = mocker.patch.object(
        manager,
        "_create_backend",
        return_value=replacement,
    )

    manager.connect()

    stale_backend.is_connected.assert_called_once_with()
    stale_backend.disconnect.assert_called_once_with()
    create_backend.assert_called_once_with()
    replacement.connect.assert_called_once_with()
    assert manager._backend is replacement


def test_health_check_diagnostic_interrupt_does_not_block_reconnect(mocker):
    """A logger handler cannot abort recovery after an ordinary probe failure."""
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    stale = mocker.MagicMock(name="stale-backend")
    stale.is_connected.side_effect = RuntimeError("probe failed")
    replacement = mocker.MagicMock(name="replacement-backend")
    monitor = mocker.MagicMock()
    manager._backend = stale
    manager.set_monitor(monitor)
    mocker.patch.object(manager, "_create_backend", return_value=replacement)
    mocker.patch(
        "scrapy_extension.backends.connectors.logger.debug",
        side_effect=KeyboardInterrupt("diagnostic interruption"),
    )

    manager.connect()

    stale.disconnect.assert_called_once_with()
    replacement.connect.assert_called_once_with()
    assert manager._backend is replacement
    monitor.on_disconnect.assert_called_once_with("BackendType.REDIS", None)
    monitor.on_connect.assert_called_once_with("BackendType.REDIS")


def test_stale_disconnect_diagnostic_interrupt_does_not_block_reconnect(mocker):
    """A logger handler cannot abort recovery after stale cleanup fails."""
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    stale = mocker.MagicMock(name="stale-backend")
    stale.is_connected.return_value = False
    stale.disconnect.side_effect = RuntimeError("disconnect failed")
    replacement = mocker.MagicMock(name="replacement-backend")
    monitor = mocker.MagicMock()
    manager._backend = stale
    manager.set_monitor(monitor)
    mocker.patch.object(manager, "_create_backend", return_value=replacement)
    mocker.patch(
        "scrapy_extension.backends.connectors.logger.warning",
        side_effect=SystemExit("diagnostic interruption"),
    )

    manager.connect()

    stale.disconnect.assert_called_once_with()
    replacement.connect.assert_called_once_with()
    assert manager._backend is replacement
    monitor.on_disconnect.assert_called_once_with("BackendType.REDIS", None)
    monitor.on_connect.assert_called_once_with("BackendType.REDIS")


def test_retry_diagnostic_interrupt_does_not_block_retry(mocker):
    """A logger handler cannot replace a retryable backend failure."""
    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 1, "retry_delay": 0},
    )
    monitor = mocker.MagicMock()
    manager.set_monitor(monitor)
    attempt = mocker.patch.object(
        manager,
        "_attempt_connection",
        side_effect=[ConnectionError("first attempt failed"), None],
    )
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )
    mocker.patch(
        "scrapy_extension.backends.connectors.logger.warning",
        side_effect=KeyboardInterrupt("diagnostic interruption"),
    )

    manager.connect()

    assert attempt.call_count == 2
    monitor.on_retry.assert_called_once_with("BackendType.REDIS", 1)
    monitor.on_connect.assert_called_once_with("BackendType.REDIS")


def test_direct_connect_dispatches_retry_monitor_outside_terminal_error(mocker):
    """Direct retry callbacks cannot inspect the active terminal failure."""
    import sys

    marker = "direct-connect-monitor-terminal-secret-marker"
    manager = ConnectionManager(
        BackendType.REDIS,
        {"api_key": marker, "retry_attempts": 1, "retry_delay": 0},
    )
    observed_exception_states: list[tuple[object, object, object]] = []
    monitor_events: list[tuple[str, int]] = []

    class _Monitor:
        def on_connect(self, backend_type: str) -> None:
            del backend_type

        def on_disconnect(self, backend_type: str, reason: object) -> None:
            del backend_type, reason

        def on_retry(self, backend_type: str, attempt: int) -> None:
            monitor_events.append((backend_type, attempt))
            observed_exception_states.append(sys.exc_info())

    manager.set_monitor(_Monitor())  # type: ignore[arg-type]
    mocker.patch.object(
        manager,
        "_attempt_connection",
        side_effect=RuntimeError(marker),
    )
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )

    with pytest.raises(BackendConnectionError) as exc_info:
        manager.connect()

    error = exc_info.value
    assert monitor_events == [("BackendType.REDIS", 1)]
    assert observed_exception_states == [(None, None, None)]
    assert str(error) == "Failed to connect after 2 attempts."
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)


def test_direct_connect_preserves_base_exception_retry_monitor_dispatch(mocker):
    """Buffered retry callbacks run after a control-flow failure has unwound."""
    import sys

    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 1, "retry_delay": 0},
    )
    monitor_events: list[tuple[str, int]] = []
    observed_exception_states: list[tuple[object, object, object]] = []
    marker = "direct-connect-monitor-control-flow-marker"
    interrupt = KeyboardInterrupt(marker)

    class _Monitor:
        def on_connect(self, backend_type: str) -> None:
            del backend_type

        def on_disconnect(self, backend_type: str, reason: object) -> None:
            del backend_type, reason

        def on_retry(self, backend_type: str, attempt: int) -> None:
            monitor_events.append((backend_type, attempt))
            observed_exception_states.append(sys.exc_info())

    manager.set_monitor(_Monitor())  # type: ignore[arg-type]
    mocker.patch.object(
        manager,
        "_attempt_connection",
        side_effect=[RuntimeError("retryable failure"), interrupt],
    )
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )

    with pytest.raises(KeyboardInterrupt) as exc_info:
        manager.connect()

    assert exc_info.value is interrupt
    assert monitor_events == [("BackendType.REDIS", 1)]
    assert observed_exception_states == [(None, None, None)]


def test_direct_connect_base_exception_monitor_callback_keeps_precedence(mocker):
    """A callback's control-flow signal still wins over a pending backend one."""
    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 1, "retry_delay": 0},
    )
    monitor_events: list[tuple[str, int]] = []
    backend_interrupt = KeyboardInterrupt("backend interrupted")
    monitor_interrupt = SystemExit("monitor interrupted")

    class _Monitor:
        def on_connect(self, backend_type: str) -> None:
            del backend_type

        def on_disconnect(self, backend_type: str, reason: object) -> None:
            del backend_type, reason

        def on_retry(self, backend_type: str, attempt: int) -> None:
            monitor_events.append((backend_type, attempt))
            raise monitor_interrupt

    manager.set_monitor(_Monitor())  # type: ignore[arg-type]
    mocker.patch.object(
        manager,
        "_attempt_connection",
        side_effect=[RuntimeError("retryable failure"), backend_interrupt],
    )
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )

    with pytest.raises(SystemExit) as exc_info:
        manager.connect()

    assert exc_info.value is monitor_interrupt
    assert monitor_events == [("BackendType.REDIS", 1)]


def test_connect_success_diagnostic_interrupt_still_dispatches_monitor(mocker):
    """A success log handler cannot suppress the committed connect event."""
    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    monitor = mocker.MagicMock()
    manager.set_monitor(monitor)
    mocker.patch.object(manager, "_attempt_connection")
    mocker.patch(
        "scrapy_extension.backends.connectors.logger.debug",
        side_effect=KeyboardInterrupt("diagnostic interruption"),
    )

    manager.connect()

    monitor.on_connect.assert_called_once_with("BackendType.REDIS")


def test_close_success_diagnostic_interrupt_still_dispatches_monitor(mocker):
    """A normal close log handler cannot skip the terminal monitor event."""
    manager = ConnectionManager(BackendType.REDIS)
    backend = mocker.MagicMock()
    monitor = mocker.MagicMock()
    manager._backend = backend
    manager.set_monitor(monitor)
    mocker.patch(
        "scrapy_extension.backends.connectors.logger.debug",
        side_effect=KeyboardInterrupt("diagnostic interruption"),
    )

    manager.close()

    backend.disconnect.assert_called_once_with()
    monitor.on_disconnect.assert_called_once_with("BackendType.REDIS", None)


def test_close_failure_diagnostic_interrupt_still_dispatches_monitor(mocker):
    """A failed close log handler cannot skip the terminal monitor event."""
    manager = ConnectionManager(BackendType.REDIS)
    backend = mocker.MagicMock()
    backend.disconnect.side_effect = RuntimeError("disconnect failed")
    monitor = mocker.MagicMock()
    manager._backend = backend
    manager.set_monitor(monitor)
    mocker.patch(
        "scrapy_extension.backends.connectors.logger.warning",
        side_effect=SystemExit("diagnostic interruption"),
    )

    manager.close()

    backend.disconnect.assert_called_once_with()
    monitor.on_disconnect.assert_called_once_with("BackendType.REDIS", None)


def test_monitor_failure_diagnostic_interrupt_stays_best_effort(mocker):
    """A logger handler cannot turn an already-ignored monitor error fatal."""
    manager = ConnectionManager(BackendType.REDIS)
    monitor = mocker.MagicMock()
    monitor.on_connect.side_effect = RuntimeError("monitor failed")
    manager.set_monitor(monitor)
    mocker.patch(
        "scrapy_extension.backends.connectors.logger.debug",
        side_effect=KeyboardInterrupt("diagnostic interruption"),
    )

    manager._notify_monitor("on_connect", "BackendType.REDIS")

    monitor.on_connect.assert_called_once_with("BackendType.REDIS")


def test_backend_and_monitor_control_exceptions_retain_their_semantics(mocker):
    """Only logging handlers are insulated from control-flow exceptions."""
    manager = ConnectionManager(BackendType.REDIS)
    stale = mocker.MagicMock()
    backend_interrupt = KeyboardInterrupt("backend interruption")
    stale.is_connected.side_effect = backend_interrupt
    manager._backend = stale

    with pytest.raises(KeyboardInterrupt) as backend_raised:
        manager.connect()

    assert backend_raised.value is backend_interrupt

    monitor = mocker.MagicMock()
    monitor_interrupt = SystemExit("monitor interruption")
    monitor.on_connect.side_effect = monitor_interrupt
    manager.set_monitor(monitor)

    with pytest.raises(SystemExit) as monitor_raised:
        manager._notify_monitor("on_connect", "BackendType.REDIS")

    assert monitor_raised.value is monitor_interrupt


def test_reconnect_isolates_breaker_from_retired_backend_failures(mocker):
    """A late old-generation failure cannot trip the replacement generation."""
    from scrapy_extension.backends.base import Backend, QueueBackend
    from scrapy_extension.backends.circuit_breaker import (
        BreakerState,
        CircuitBreaker,
    )
    from scrapy_extension.exceptions import BackendError, QueueError

    class _QueueBackend(Backend, QueueBackend):
        def __init__(self, name: str) -> None:
            self.name = name
            self.connected = True
            self.connect_mock = mocker.Mock(name=f"{name}.connect")
            self.disconnect_mock = mocker.Mock(name=f"{name}.disconnect")
            self.pop_mock = mocker.Mock(name=f"{name}.pop", return_value=None)

        @property
        def backend_type(self) -> BackendType:
            return BackendType.REDIS

        def connect(self) -> None:
            self.connect_mock()
            self.connected = True

        def disconnect(self) -> None:
            self.disconnect_mock()
            self.connected = False

        def is_connected(self) -> bool:
            return self.connected

        def ping(self) -> bool:
            return self.connected

        def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
            del queue_name, item, priority

        def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
            return self.pop_mock(queue_name, timeout)

        def queue_len(self, queue_name: str) -> int:
            del queue_name
            return 0

        def clear_queue(self, queue_name: str) -> None:
            del queue_name

    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    retired = _QueueBackend("retired-backend")
    manager._backend = retired
    retired_breaker = CircuitBreaker(
        "redis-backend",
        failure_threshold=1,
        failure_exceptions=(BackendError,),
    )
    manager._breaker = retired_breaker
    manager._breaker_configured = True
    retired_proxy = manager.get_queue_backend()
    breaker_during_disconnect: list[CircuitBreaker | None] = []
    retired.disconnect_mock.side_effect = lambda: breaker_during_disconnect.append(
        manager._breaker
    )

    replacement = _QueueBackend("replacement-backend")
    replacement.pop_mock.return_value = b"fresh"
    retired.connected = False
    mocker.patch.object(manager, "_create_backend", return_value=replacement)

    manager.connect()
    replacement_proxy = manager.get_queue_backend()

    assert manager._breaker is not retired_breaker
    assert breaker_during_disconnect == [manager._breaker]
    retired.pop_mock.side_effect = QueueError("late retired failure")
    with pytest.raises(QueueError) as exc_info:
        retired_proxy.pop("queue")

    assert str(exc_info.value) == "Backend operation failed."
    assert exc_info.value.operation == "pop"
    assert exc_info.value.queue_name is None
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert retired_breaker.state is BreakerState.OPEN
    assert manager._breaker is not None
    assert manager._breaker.state is BreakerState.CLOSED
    assert replacement_proxy.pop("queue") == b"fresh"
    replacement.pop_mock.assert_called_once_with("queue", 0.0)


def test_get_queue_backend_retries_mixed_reconnect_generation(mocker):
    """An accessor must never pair an old backend with a new breaker.

    Reconnect replaces ``_backend`` and starts a fresh breaker generation.  If
    that replacement lands between the accessor's backend read and breaker
    read, independently reading the two fields constructs a mixed proxy.  A
    late failure from the retired backend would then trip the live generation's
    breaker.  Force that exact interleaving and require the accessor to retry
    until both objects come from one coherent manager snapshot.
    """
    from scrapy_extension.backends.base import Backend, QueueBackend
    from scrapy_extension.backends.circuit_breaker import CircuitBreaker
    from scrapy_extension.exceptions import BackendError

    class _QueueBackend(Backend, QueueBackend):
        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            self.pop_mock = mocker.Mock(return_value=payload)

        @property
        def backend_type(self) -> BackendType:
            return BackendType.REDIS

        def connect(self) -> None:
            return None

        def disconnect(self) -> None:
            return None

        def is_connected(self) -> bool:
            return True

        def ping(self) -> bool:
            return True

        def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
            del queue_name, item, priority

        def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
            return self.pop_mock(queue_name, timeout)

        def queue_len(self, queue_name: str) -> int:
            del queue_name
            return 0

        def clear_queue(self, queue_name: str) -> None:
            del queue_name

    manager = ConnectionManager(BackendType.REDIS)
    retired = _QueueBackend(b"retired")
    replacement = _QueueBackend(b"fresh")
    retired_breaker = CircuitBreaker(
        "retired-generation",
        failure_exceptions=(BackendError,),
    )
    replacement_breaker = CircuitBreaker(
        "replacement-generation",
        failure_exceptions=(BackendError,),
    )
    manager._backend = retired
    manager._breaker = retired_breaker
    manager._breaker_configured = True

    swapped = False

    def swap_generation_during_breaker_read() -> CircuitBreaker:
        nonlocal swapped
        if not swapped:
            with manager._lock:
                manager._backend = replacement
                manager._breaker = replacement_breaker
            swapped = True
        assert manager._breaker is not None
        return manager._breaker

    mocker.patch.object(
        manager,
        "_get_breaker",
        side_effect=swap_generation_during_breaker_read,
    )

    proxy = manager.get_queue_backend()

    assert proxy.pop("queue") == b"fresh"
    retired.pop_mock.assert_not_called()
    replacement.pop_mock.assert_called_once_with("queue", 0.0)


def test_backend_property_concurrent_first_connect_single_connect(mocker):
    """A2 + thread-safety: when N threads hit the ``backend`` property
    concurrently on a fresh manager, exactly ONE ``connect()`` runs and all
    threads see the same connected backend. The fast lock-free read must not
    let two threads both enter the slow path.

    This pins both the double-checked-locking invariant AND that the lock is
    released between the connect path's retry sleeps (so peers aren't
    serialized on the backoff).
    """
    import threading

    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 1})
    mock_backend = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )

    n = 15
    barrier = threading.Barrier(n)
    results: list[object] = []
    errors: list[BaseException] = []

    def worker():
        try:
            barrier.wait()
            results.append(manager.backend)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(results) == n
    # Every thread observed the same connected backend.
    assert all(r is mock_backend for r in results)
    # Exactly one _create_backend call (single connect).
    assert ConnectionManager._create_backend.call_count == 1


def test_lazy_owner_failure_publishes_before_retry_monitor_reentry(mocker):
    """A failed lazy owner must wake peers before ``on_retry`` can re-enter.

    Before Round 43A, the callback below waited on the owner's attempt event
    while the owner waited for the callback to return.  Run it in a daemon
    thread so a regression fails promptly instead of stalling the test process.
    """
    import threading

    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 1, "retry_delay": 0},
    )
    failed = mocker.MagicMock(name="failed-backend")
    failed.connect.side_effect = OSError("temporary failure")
    mocker.patch.object(manager, "_create_backend", return_value=failed)
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )
    monitor_states: list[tuple[bool, bool]] = []
    reentrant_errors: list[BackendConnectionError] = []

    class _Monitor:
        def on_connect(self, backend_type: str) -> None:
            del backend_type

        def on_disconnect(self, backend_type: str, reason: object) -> None:
            del backend_type, reason

        def on_retry(self, backend_type: str, attempt: int) -> None:
            del backend_type, attempt
            monitor_states.append(
                (manager._connecting, manager._connected_event.is_set())
            )
            try:
                _ = manager.backend
            except BackendConnectionError as error:
                reentrant_errors.append(error)

    manager.set_monitor(_Monitor())  # type: ignore[arg-type]
    outcomes: list[BaseException] = []

    def materialize() -> None:
        try:
            _ = manager.backend
        except BaseException as error:  # noqa: BLE001 - capture owner outcome
            outcomes.append(error)

    owner = threading.Thread(target=materialize, daemon=True)
    owner.start()
    owner.join(timeout=2.0)

    assert not owner.is_alive(), "lazy owner deadlocked in its retry monitor"
    assert failed.connect.call_count == 2
    assert monitor_states == [(False, True)]
    assert len(outcomes) == len(reentrant_errors) == 1
    assert isinstance(outcomes[0], BackendConnectionError)
    assert str(reentrant_errors[0]) == str(outcomes[0])
    assert manager._connecting is False


def test_lazy_owner_publishes_sanitized_mutated_config_error_to_peer(mocker):
    """Peer and retry monitor never receive a mutated-settings failure graph."""
    import threading

    marker = "lazy-owner-mutated-config-secret-marker"
    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 1, "retry_delay": 0},
    )
    first_attempt_entered = threading.Event()
    allow_first_failure = threading.Event()
    attempts = 0

    def attempt_connection() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            first_attempt_entered.set()
            assert allow_first_failure.wait(timeout=2.0), "test did not release owner"
            raise OSError("temporary failure")
        raw_attempts = manager.settings["retry_attempts"]
        raise ConfigurationError(
            f"retry_attempts rejected: {raw_attempts}",
            setting_name="retry_attempts",
            setting_value=raw_attempts,
        )

    mocker.patch.object(manager, "_attempt_connection", side_effect=attempt_connection)
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )
    outcomes: dict[str, BaseException] = {}
    monitor_states: list[tuple[bool, bool]] = []
    reentrant_errors: list[ConfigurationError] = []

    class _Monitor:
        def on_connect(self, backend_type: str) -> None:
            del backend_type

        def on_disconnect(self, backend_type: str, reason: object) -> None:
            del backend_type, reason

        def on_retry(self, backend_type: str, attempt: int) -> None:
            del backend_type, attempt
            monitor_states.append(
                (manager._connecting, manager._connected_event.is_set())
            )
            try:
                _ = manager.backend
            except ConfigurationError as error:
                reentrant_errors.append(error)

    manager.set_monitor(_Monitor())  # type: ignore[arg-type]

    def materialize(label: str) -> None:
        try:
            _ = manager.backend
        except BaseException as error:  # noqa: BLE001 - inspect owner and peer
            outcomes[label] = error

    owner = threading.Thread(target=materialize, args=("owner",), daemon=True)
    owner.start()
    assert first_attempt_entered.wait(timeout=2.0), "owner did not start attempt"
    attempt = manager._connect_attempt
    assert attempt is not None
    peer_waiting = threading.Event()
    original_wait = attempt.event.wait

    def observed_wait(timeout: float | None = None) -> bool:
        peer_waiting.set()
        return original_wait(timeout)

    mocker.patch.object(attempt.event, "wait", side_effect=observed_wait)
    peer = threading.Thread(target=materialize, args=("peer",), daemon=True)
    peer.start()
    assert peer_waiting.wait(timeout=2.0), "peer did not join owner's attempt"

    manager.settings["retry_attempts"] = marker
    allow_first_failure.set()
    owner.join(timeout=2.0)
    peer.join(timeout=2.0)

    assert not owner.is_alive()
    assert not peer.is_alive()
    assert set(outcomes) == {"owner", "peer"}
    assert attempts == 2
    assert monitor_states == [(False, True)]
    assert len(reentrant_errors) == 1
    for error in outcomes.values():
        assert isinstance(error, ConfigurationError)
        assert str(error) == "Connection manager configuration is invalid."
        assert error.setting_name == "retry_attempts"
        assert error.setting_value is None
        assert marker not in repr(error.__dict__)
        assert marker not in "".join(traceback.format_exception(error))
        assert error.__cause__ is None
        assert error.__context__ is None
        _assert_package_traceback_locals_are_redacted(error, marker)

    reentrant_error = reentrant_errors[0]
    assert str(reentrant_error) == "Connection manager configuration is invalid."
    assert reentrant_error.setting_name == "retry_attempts"
    assert reentrant_error.setting_value is None
    assert marker not in repr(reentrant_error.__dict__)
    assert marker not in "".join(traceback.format_exception(reentrant_error))
    assert reentrant_error.__cause__ is None
    assert reentrant_error.__context__ is None
    _assert_package_traceback_locals_are_redacted(reentrant_error, marker)

    published_error = attempt.error
    assert isinstance(published_error, ConfigurationError)
    assert str(published_error) == "Connection manager configuration is invalid."
    assert published_error.setting_name == "retry_attempts"
    assert published_error.setting_value is None
    assert published_error.__traceback__ is None
    assert published_error.__cause__ is None
    assert published_error.__context__ is None
    assert marker not in repr(published_error.__dict__)


def test_lazy_owner_retry_success_publishes_before_monitor_reentry(mocker):
    """Retry and connect hooks see the completed lazy result, not its owner."""
    import threading

    manager = ConnectionManager(
        BackendType.REDIS,
        {"retry_attempts": 1, "retry_delay": 0},
    )
    failed = mocker.MagicMock(name="failed-backend")
    failed.connect.side_effect = OSError("temporary failure")
    recovered = mocker.MagicMock(name="recovered-backend")
    mocker.patch.object(
        manager,
        "_create_backend",
        side_effect=[failed, recovered],
    )
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        return_value=False,
    )
    monitor_states: list[tuple[str, bool, bool]] = []
    reentries: list[object] = []
    connect_reentries: list[str] = []

    class _Monitor:
        def on_connect(self, backend_type: str) -> None:
            del backend_type
            monitor_states.append(
                ("connect", manager._connecting, manager._connected_event.is_set())
            )
            reentries.append(manager.backend)
            manager.connect()
            connect_reentries.append("connect")

        def on_disconnect(self, backend_type: str, reason: object) -> None:
            del backend_type, reason

        def on_retry(self, backend_type: str, attempt: int) -> None:
            del backend_type, attempt
            monitor_states.append(
                ("retry", manager._connecting, manager._connected_event.is_set())
            )
            reentries.append(manager.backend)
            manager.connect()
            connect_reentries.append("retry")

    manager.set_monitor(_Monitor())  # type: ignore[arg-type]
    outcomes: list[BaseException | object] = []

    def materialize() -> None:
        try:
            outcomes.append(manager.backend)
        except BaseException as error:  # noqa: BLE001 - capture owner outcome
            outcomes.append(error)

    owner = threading.Thread(target=materialize, daemon=True)
    owner.start()
    owner.join(timeout=2.0)

    assert not owner.is_alive(), "lazy owner deadlocked in its lifecycle monitor"
    assert outcomes == [recovered]
    assert monitor_states == [("retry", False, True), ("connect", False, True)]
    assert reentries == [recovered, recovered]
    assert connect_reentries == ["retry", "connect"]
    assert failed.connect.call_count == recovered.connect.call_count == 1
    assert manager._connecting is False


def test_direct_concurrent_connect_calls_create_one_backend(mocker):
    """Public ``connect()`` calls share one connection attempt.

    ``BackendSpiderMixin`` invokes ``ConnectionManager.connect()`` directly from
    the ``spider_opened`` signal, so the single-connect guarantee cannot live
    only in the lazy ``backend`` property. Block the first caller inside the
    backend factory: a racing direct caller must not enter the factory and build
    a second connection that would overwrite (and leak) the first one.
    """
    import threading

    manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
    first_factory_entered = threading.Event()
    release_first_factory = threading.Event()
    second_factory_entered = threading.Event()
    factory_lock = threading.Lock()
    factory_calls = 0
    backends = [
        mocker.MagicMock(name="backend-one"),
        mocker.MagicMock(name="backend-two"),
    ]

    def create_backend():
        nonlocal factory_calls
        with factory_lock:
            factory_calls += 1
            call_number = factory_calls
        if call_number == 1:
            first_factory_entered.set()
            assert release_first_factory.wait(timeout=2.0)
        else:
            second_factory_entered.set()
        return backends[call_number - 1]

    mocker.patch.object(manager, "_create_backend", side_effect=create_backend)
    errors: list[BaseException] = []

    def connect() -> None:
        try:
            manager.connect()
        except BaseException as exc:  # noqa: BLE001 - surface thread failures
            errors.append(exc)

    first = threading.Thread(target=connect, daemon=True)
    second = threading.Thread(target=connect, daemon=True)
    first.start()
    assert first_factory_entered.wait(timeout=2.0)
    second.start()
    try:
        assert not second_factory_entered.wait(timeout=0.2)
    finally:
        release_first_factory.set()
        first.join(timeout=2.0)
        second.join(timeout=2.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert factory_calls == 1
    assert manager._backend is backends[0]


# ===========================================================================
# R14-E — Lifecycle bounds (long-run leak prevention)
# ===========================================================================


def test_managers_registry_capped_under_settings_churn(mocker):
    """R14-E HIGH: settings churn must not grow ``_managers`` unbounded.

    A crawler with rotating per-spider credentials / unique ``group_id``
    produces a fresh registry entry per distinct settings dict. Without a
    cap, prior entries (each holding a live ``Backend`` + open sockets)
    linger forever. The registry is now an LRU ``OrderedDict`` capped at
    ``MAX_MANAGERS``; on overflow the oldest *genuinely-orphaned* entry
    (``_users <= 0``) is evicted and its backend disconnected.
    """
    ConnectionManager.clear_registry()
    cap = ConnectionManager.MAX_MANAGERS
    # Patch _create_backend so we don't touch the network; track disconnect
    # calls so we can assert the victim was torn down.
    mock_backends: list = []
    disconnected: list = []

    class _FakeBackend:
        def __init__(self) -> None:
            mock_backends.append(self)

        def connect(self) -> None:
            pass

        def disconnect(self) -> None:
            disconnected.append(self)

        def is_connected(self) -> bool:
            return True

    mocker.patch.object(
        ConnectionManager, "_create_backend", side_effect=lambda: _FakeBackend()
    )

    n = 64  # double the cap
    try:
        for i in range(n):
            # Each iteration: acquire a manager with distinct settings, force
            # its backend to materialize (so disconnect has something to tear
            # down), then release. The entry becomes orphaned (``_users <= 0``)
            # and is eligible for LRU eviction on the NEXT insert.
            mgr = ConnectionManager.get_manager(
                BackendType.REDIS, {"host": f"churn-{i}"}
            )
            _ = mgr.backend  # materialize the backend
            mgr.close()
        # Registry must be at-or-under the cap after the churn.
        assert len(ConnectionManager._managers) <= cap, (
            f"registry grew to {len(ConnectionManager._managers)} > cap {cap} "
            "under settings churn — LRU eviction did not fire"
        )
        # At least one victim's backend was disconnected (many more, in fact).
        assert len(disconnected) > 0, "no orphaned manager was disconnected"
    finally:
        ConnectionManager.clear_registry()
        mocker.stopall()


def test_managers_registry_does_not_evict_actively_held_manager(mocker):
    """R14-E CRITICAL: an actively-held manager (``_users > 0``) is never evicted.

    Force-eviction would corrupt the holder's connection. When the cap is
    reached with ALL entries live, we stop evicting and warn-once instead.
    """
    ConnectionManager.clear_registry()
    cap = ConnectionManager.MAX_MANAGERS
    mocker.patch.object(
        ConnectionManager,
        "_create_backend",
        return_value=mocker.MagicMock(),
    )
    held_managers: list = []
    try:
        # Acquire ``cap`` distinct managers WITHOUT closing them — all live.
        for i in range(cap):
            held_managers.append(
                ConnectionManager.get_manager(BackendType.REDIS, {"host": f"live-{i}"})
            )
        assert len(ConnectionManager._managers) == cap
        # All are actively held.
        assert all(m._users > 0 for m in ConnectionManager._managers.values())

        # Acquire one MORE — would normally trigger eviction, but every entry
        # is live, so the new one is added without evicting any holder.
        extra = ConnectionManager.get_manager(BackendType.REDIS, {"host": "extra-live"})
        held_managers.append(extra)
        # Registry is now over cap (cap + 1) — but NO held manager was evicted.
        assert len(ConnectionManager._managers) == cap + 1
        # Every originally-held manager is still in the registry.
        for m in held_managers[:-1]:
            assert m._users > 0
            assert m in ConnectionManager._managers.values()
    finally:
        # Release every holder so teardown is clean.
        for m in held_managers:
            m.close()
        ConnectionManager.clear_registry()
        mocker.stopall()


def test_registry_over_cap_diagnostic_interrupt_does_not_block_acquire(mocker):
    """A warning handler cannot abort an acquire after the cap state is marked."""
    ConnectionManager.clear_registry()
    cap = ConnectionManager.MAX_MANAGERS
    held_managers: list[ConnectionManager] = []
    try:
        for i in range(cap):
            held_managers.append(
                ConnectionManager.get_manager(BackendType.REDIS, {"host": f"live-{i}"})
            )
        mocker.patch(
            "scrapy_extension.backends.connectors.logger.warning",
            side_effect=KeyboardInterrupt("diagnostic interruption"),
        )

        extra = ConnectionManager.get_manager(
            BackendType.REDIS,
            {"host": "extra-after-diagnostic-interruption"},
        )
        held_managers.append(extra)

        assert ConnectionManager._over_cap_warned
        assert len(ConnectionManager._managers) == cap + 1
        assert extra in ConnectionManager._managers.values()
    finally:
        for manager in held_managers:
            manager.close()
        ConnectionManager.clear_registry()
        mocker.stopall()


def test_close_resets_circuit_breaker(mocker):
    """R14-E MED: ``close()`` resets the breaker so a reconnect doesn't inherit stale OPEN state.

    Without reset, an orphan-evicted or torn-down manager whose breaker had
    tripped OPEN would leave the breaker stuck OPEN — and since the breaker
    is per-manager, a fresh manager created from the same settings inherits
    nothing (good), but a manager that reconnects after teardown (kept alive
    by an external ref) would stay OPEN forever.
    """
    ConnectionManager.clear_registry()
    mock_backend = mocker.MagicMock()
    mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)

    mgr = ConnectionManager.get_manager(BackendType.REDIS, {"host": "breaker-test"})
    _ = mgr.backend  # materialize
    # Manually construct + trip the breaker to simulate a failure run.
    from scrapy_extension.backends.circuit_breaker import (
        BreakerState,
        CircuitBreaker,
    )

    breaker = CircuitBreaker(name="test", failure_threshold=1)
    # Trip it: one failure crosses the threshold.
    try:
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    except RuntimeError:
        pass
    assert breaker.state is BreakerState.OPEN
    mgr._breaker = breaker
    mgr._breaker_configured = True

    mgr.close()

    assert breaker.state is BreakerState.CLOSED, (
        "close() did not reset the circuit breaker — a reconnecting manager "
        "would inherit a stale OPEN state"
    )
    ConnectionManager.clear_registry()
    mocker.stopall()


# ---------------------------------------------------------------------------
# R14-G: A2 single-connect-owner error-signal threading test.
#
# ``_get_backend`` splits fast/slow paths: the first thread to enter the slow
# path takes ownership of connecting; peers wait on ``_connected_event``
# (released by the owner in a ``finally``). The load-bearing invariant: if the
# owner's ``connect()`` raises, ALL peer waiters must (a) receive the same
# exception and (b) NOT hang — ``_connected_event.set()`` must run in the
# ``finally`` block so peers wake up.
# ---------------------------------------------------------------------------


def test_owner_connect_failure_signals_all_peer_waiters(mocker):
    """Owner's ``connect()`` raises → every peer waiter re-raises + event is set.

    Constructs a manager whose backend ``connect()`` always raises, then calls
    ``get_queue_backend()`` from multiple threads simultaneously. Without the
    A2 finally-set, peers would block on ``_connected_event.wait()`` forever
    (the test would hang). With it, every peer re-raises the owner's exception.
    """
    import threading

    # Unique settings so this manager is isolated in the class-level registry.
    settings = {"retry_attempts": 1, "retry_delay": 0, "host": "owner-fail-test"}
    manager = ConnectionManager.get_manager(BackendType.REDIS, settings)
    ConnectionManager.clear_registry()  # evict the just-created empty shell

    # Re-create so we control it directly, then patch its backend factory.
    manager = ConnectionManager.get_manager(BackendType.REDIS, settings)

    connect_error = BackendConnectionError(
        "simulated owner connect failure", backend_type="redis"
    )

    class _FailingBackend:
        def __init__(self, *_args, **_kwargs) -> None:
            self.backend_type = "redis"

        def connect(self) -> None:
            raise connect_error

        def disconnect(self) -> None:
            pass

        def is_connected(self) -> bool:
            return False

        def ping(self) -> bool:
            return False

    mocker.patch.object(manager, "_create_backend", return_value=_FailingBackend())

    results: dict[str, object] = {}
    start_gate = threading.Event()
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def _worker(worker_id: str) -> None:
        # Wait for the green light so all workers race into _get_backend together.
        start_gate.wait(timeout=5.0)
        try:
            manager.get_queue_backend()
            results[worker_id] = "no-error"
        except BaseException as exc:  # noqa: BLE001 — capture every peer's outcome
            with errors_lock:
                errors.append(exc)
            results[worker_id] = type(exc).__name__

    threads = [threading.Thread(target=_worker, args=(f"w{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    # Release all workers at once to maximize owner/peer contention.
    start_gate.set()
    for t in threads:
        t.join(timeout=10.0)

    # No thread may still be alive (would mean _connected_event was never set).
    assert not any(t.is_alive() for t in threads), (
        f"a peer waiter hung — _connected_event.set() did not fire in the owner "
        f"finally block. results={results}"
    )

    # Every worker must receive one typed, sanitized retry-exhaustion error —
    # never a silent success, a different exception type, or the owner's raw
    # backend diagnostic in an exception chain.
    assert len(errors) == 4, (
        f"expected 4 errors (one per worker), got {len(errors)}; results={results}"
    )
    for exc in errors:
        assert isinstance(exc, BackendConnectionError), (
            f"peer received a non-BackendConnectionError: got {exc!r}"
        )
        assert "simulated owner connect failure" not in str(exc)
        assert exc.__cause__ is None
        assert exc.__context__ is None

    # The event must be set (the finally ran) — otherwise a later waiter would
    # hang on the next ``get_queue_backend()`` call.
    assert manager._connected_event.is_set(), (
        "_connected_event not set after owner failure — peers on the next call "
        "would hang (permanent stall after one connect failure)"
    )

    manager.close()
    ConnectionManager.clear_registry()
    mocker.stopall()


def test_last_close_during_connect_cannot_publish_orphan_backend(mocker):
    """A manager evicted while connect is slow must never resurrect afterwards."""
    import threading

    manager = ConnectionManager.get_manager(
        BackendType.REDIS,
        {"host": "close-during-connect", "retry_attempts": 0},
    )
    connect_entered = threading.Event()
    release_connect = threading.Event()
    backend = mocker.MagicMock(name="slow-backend")

    def slow_connect() -> None:
        connect_entered.set()
        assert release_connect.wait(timeout=2.0)

    backend.connect.side_effect = slow_connect
    mocker.patch.object(manager, "_create_backend", return_value=backend)
    outcomes: list[BaseException | object] = []

    def materialize() -> None:
        try:
            outcomes.append(manager.backend)
        except BaseException as exc:  # noqa: BLE001 - capture thread outcome
            outcomes.append(exc)

    thread = threading.Thread(target=materialize, daemon=True)
    thread.start()
    assert connect_entered.wait(timeout=2.0)

    manager.close()
    release_connect.set()
    thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], BackendConnectionError)
    assert manager._backend is None
    backend.disconnect.assert_called_once_with()
    key = ConnectionManager._registry_key(manager.backend_type, manager.settings)
    assert key not in ConnectionManager._managers


@pytest.mark.parametrize("teardown", ["close", "clear_registry"])
def test_retirement_interrupts_retry_backoff_before_second_backend(mocker, teardown):
    """Final and forced teardown wake a retry owner before another factory call."""
    import threading

    manager = ConnectionManager.get_manager(
        BackendType.REDIS,
        {
            "host": f"interrupt-backoff-{teardown}",
            "retry_attempts": 3,
            "retry_delay": 60,
        },
    )
    backend = mocker.MagicMock(name="failed-backend")
    backend.connect.side_effect = OSError("temporary failure")
    create_backend = mocker.patch.object(
        manager,
        "_create_backend",
        return_value=backend,
    )
    wait_entered = threading.Event()

    def observed_wait(retirement_event, delay):
        # Reactor-facing manager retries cap each wait to the configured
        # SCRAPY_REACTOR_IO_TIMEOUT default (5s), even when retry_delay is 60s.
        assert 0 < delay <= 5.0
        wait_entered.set()
        return retirement_event.wait(delay)

    mocker.patch(
        "scrapy_extension.backends.connectors.compute_full_jitter_backoff",
        return_value=60,
    )
    mocker.patch(
        "scrapy_extension.backends.connectors._wait_for_retry_backoff",
        side_effect=observed_wait,
    )
    outcomes: list[BaseException] = []

    def connect() -> None:
        try:
            manager.connect()
        except BaseException as error:  # noqa: BLE001 - inspect owner outcome
            outcomes.append(error)

    owner = threading.Thread(target=connect, daemon=True)
    owner.start()
    assert wait_entered.wait(timeout=2.0), "connect owner did not enter backoff"

    if teardown == "close":
        manager.close()
        assert manager._users == 0
    else:
        ConnectionManager.clear_registry()
        # Force teardown invalidates every outstanding ownership token. A stale
        # holder can still call close(), but it cannot affect a replacement.
        assert manager._users == 0

    owner.join(timeout=1.0)

    assert not owner.is_alive(), "retired manager waited for the full retry delay"
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], BackendConnectionError)
    assert manager._retirement_event.is_set()
    assert manager._retired is True
    assert create_backend.call_count == 1
    assert manager not in ConnectionManager._managers.values()


def test_backend_property_rejects_released_manager(mocker):
    """A warmed manager is terminal after its final holder releases it."""
    manager = ConnectionManager.get_manager(
        BackendType.REDIS,
        {"host": "access-after-close", "retry_attempts": 0},
    )
    backend = mocker.MagicMock(name="connected-backend")
    create_backend = mocker.patch.object(
        manager,
        "_create_backend",
        return_value=backend,
    )

    assert manager.backend is backend
    manager.close()

    with pytest.raises(BackendConnectionError, match="released"):
        _ = manager.backend

    backend.disconnect.assert_called_once_with()
    create_backend.assert_called_once_with()


# ---------------------------------------------------------------------------
# Round 43B — durable-push terminal error boundary
# ---------------------------------------------------------------------------


def _assert_durable_value_is_redacted(
    value: object,
    marker: str,
    seen: set[int] | None = None,
) -> None:
    """Walk a bounded object graph without trusting custom ``repr`` methods."""
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
            _assert_durable_value_is_redacted(key, marker, seen)
            _assert_durable_value_is_redacted(item, marker, seen)
        return
    if isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_durable_value_is_redacted(item, marker, seen)
        return
    try:
        attributes = vars(value)
    except TypeError:
        return
    _assert_durable_value_is_redacted(attributes, marker, seen)


def _assert_durable_push_error_is_redacted(error: BaseException, marker: str) -> None:
    """Assert every public durable-push error surface drops private markers."""
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
                _assert_durable_value_is_redacted(value, marker)
        trace = trace.tb_next


class _SensitiveDurableQueueBackend(Backend, QueueBackend):
    """Queue backend that deliberately retains request/config state on failure."""

    def __init__(self, marker: str, *, policy_rejection: bool) -> None:
        self.config = SimpleNamespace(
            endpoint_url=f"https://{marker}.example",
            api_key=marker,
        )
        self.marker = marker
        self.policy_rejection = policy_rejection
        self.push_attempts = 0

    @property
    def backend_type(self) -> BackendType:
        return BackendType.REDIS

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def is_connected(self) -> bool:
        return True

    def ping(self) -> bool:
        return True

    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        self._push_with_durability(queue_name, item, priority)

    def _push_with_durability(
        self,
        queue_name: str,
        item: bytes,
        priority: float = 0.0,
        *,
        require_durable: bool = False,
    ) -> _QueuePushReceipt:
        self.push_attempts += 1
        if self.policy_rejection:
            raise _DurablePushRequired
        try:
            raise RuntimeError(self.marker)
        except RuntimeError as error:
            raise QueueError(
                self.marker,
                queue_name=queue_name,
                operation="push",
            ) from error

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        del queue_name
        del timeout
        return None

    def queue_len(self, queue_name: str) -> int:
        del queue_name
        return 0

    def clear_queue(self, queue_name: str) -> None:
        del queue_name


def _durable_push_manager(
    backend: QueueBackend,
    marker: str,
    breaker: CircuitBreaker | None = None,
) -> ConnectionManager:
    manager = ConnectionManager(BackendType.REDIS, {"api_key": marker})
    manager._backend = backend  # type: ignore[assignment]
    manager._breaker_configured = True
    manager._breaker = breaker
    return manager


def test_durable_policy_error_is_terminally_redacted_without_tripping_breaker() -> None:
    marker = "round43b-durable-policy-private-marker"
    backend = _SensitiveDurableQueueBackend(marker, policy_rejection=True)
    breaker = CircuitBreaker(
        f"breaker-{marker}",
        failure_threshold=1,
        failure_exceptions=(BackendError,),
    )
    manager = _durable_push_manager(backend, marker, breaker)
    try:
        with pytest.raises(QueueError) as exc_info:
            manager._push_queue_with_durability(
                f"{marker}-queue",
                f"{marker}-item".encode(),
                require_durable=True,
            )

        error = exc_info.value
        assert (
            str(error)
            == "Selected queue backend generation is not worker-crash durable"
        )
        assert error.operation == "push"
        assert error.queue_name is None
        assert backend.push_attempts == 1
        assert breaker.state is BreakerState.CLOSED
        assert breaker.failure_count == 0
        _assert_durable_push_error_is_redacted(error, marker)
    finally:
        manager.close()


def test_durable_backend_queue_error_is_terminally_redacted_without_breaker() -> None:
    marker = "round43b-durable-backend-private-marker"
    backend = _SensitiveDurableQueueBackend(marker, policy_rejection=False)
    manager = _durable_push_manager(backend, marker)
    try:
        with pytest.raises(QueueError) as exc_info:
            manager._push_queue_with_durability(
                f"{marker}-queue",
                f"{marker}-item".encode(),
            )

        error = exc_info.value
        assert str(error) == "Queue backend push failed."
        assert error.operation == "push"
        assert error.queue_name is None
        assert backend.push_attempts == 1
        _assert_durable_push_error_is_redacted(error, marker)
    finally:
        manager.close()


def test_durable_backend_queue_error_preserves_breaker_open_semantics() -> None:
    marker = "round43b-durable-breaker-private-marker"
    backend = _SensitiveDurableQueueBackend(marker, policy_rejection=False)
    breaker = CircuitBreaker(
        f"breaker-{marker}",
        failure_threshold=1,
        failure_exceptions=(BackendError,),
    )
    manager = _durable_push_manager(backend, marker, breaker)
    try:
        with pytest.raises(QueueError) as first_exc_info:
            manager._push_queue_with_durability(
                f"{marker}-queue",
                f"{marker}-item".encode(),
            )

        first_error = first_exc_info.value
        assert str(first_error) == "Backend operation failed."
        assert first_error.operation == "push"
        assert first_error.queue_name is None
        assert breaker.state is BreakerState.OPEN
        assert breaker.failure_count == 1
        assert backend.push_attempts == 1
        _assert_durable_push_error_is_redacted(first_error, marker)

        with pytest.raises(CircuitBreakerOpenError) as open_exc_info:
            manager._push_queue_with_durability(
                f"{marker}-queue",
                f"{marker}-item".encode(),
            )

        open_error = open_exc_info.value
        assert open_error.name == "backend-operation"
        assert breaker.state is BreakerState.OPEN
        assert breaker.failure_count == 1
        assert backend.push_attempts == 1
        _assert_durable_push_error_is_redacted(open_error, marker)
    finally:
        manager.close()


@pytest.mark.parametrize(
    ("raised_error", "expected_type", "preserves_identity"),
    (
        (
            BackendConnectionError("private", backend_type="private"),
            BackendConnectionError,
            False,
        ),
        (
            ConfigurationError("private", setting_name="api_key"),
            ConfigurationError,
            False,
        ),
        (ImportError("private"), ImportError, False),
        (ValueError("input contract"), ValueError, True),
        (NotImplementedError("unsupported"), NotImplementedError, False),
        (KeyboardInterrupt("control flow"), KeyboardInterrupt, True),
    ),
)
def test_durable_push_preserves_non_queue_exception_contracts(
    mocker,
    raised_error: BaseException,
    expected_type: type[BaseException],
    preserves_identity: bool,
) -> None:
    """Only QueueError is rebuilt; established sibling contracts stay intact."""
    manager = ConnectionManager(BackendType.REDIS, {"api_key": "private"})
    mocker.patch.object(
        manager,
        "_get_backend_breaker_snapshot",
        side_effect=raised_error,
    )

    with pytest.raises(expected_type) as exc_info:
        manager._push_queue_with_durability("queue", b"item")

    error = exc_info.value
    if preserves_identity:
        assert error is raised_error
    else:
        assert error is not raised_error


def test_apply_scrapy_breaker_policy_keeps_registry_key_stable(monkeypatch):
    """R137-F1: apply_scrapy_breaker_policy must never mutate manager.settings.

    close() recomputes the registry key from self.settings at release time and
    evicts by identity. A post-registration settings mutation changes the
    recomputed key, the identity check fails, and the retired entry stays in
    the registry — the next get_manager with the ORIGINAL settings returns the
    retired manager, on which every backend access raises permanently.
    """
    for key in (
        "SCRAPY_CIRCUIT_BREAKER_ENABLED",
        "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT",
    ):
        monkeypatch.delenv(key, raising=False)

    from scrapy.settings import Settings as ScrapySettings

    settings = {"host": "r137-key-stability-host"}
    manager = ConnectionManager.get_manager(BackendType.REDIS, settings)
    try:
        manager.apply_scrapy_breaker_policy(
            ScrapySettings(
                {
                    "SCRAPY_CIRCUIT_BREAKER_ENABLED": True,
                    "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD": 4,
                    "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT": 9.0,
                }
            )
        )
        assert manager._get_breaker() is not None
    finally:
        manager.close()

    # The retired entry must have been evicted: a fresh get_manager with the
    # ORIGINAL (policy-less) settings returns a brand-new usable manager.
    manager_after = ConnectionManager.get_manager(BackendType.REDIS, settings)
    try:
        assert manager_after is not manager
        assert manager_after._retired is False
    finally:
        manager_after.close()


def test_apply_scrapy_breaker_policy_overrides_env_fallback_resolution(
    monkeypatch,
):
    """R137-F2: a pre-crawler first use caches the disabled env fallback
    (_breaker_configured=True, _breaker=None). An explicit Scrapy policy
    arriving afterwards must OVERRIDE that fallback — otherwise the breaker
    the user configured silently never engages (the exact R136-F1 symptom
    surviving through used-early, not just acquired-early)."""
    for key in (
        "SCRAPY_CIRCUIT_BREAKER_ENABLED",
        "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT",
    ):
        monkeypatch.delenv(key, raising=False)

    from scrapy.settings import Settings as ScrapySettings

    manager = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "r137-env-fallback-host"}
    )
    try:
        assert manager._get_breaker() is None  # env fallback cached + latched

        manager.apply_scrapy_breaker_policy(
            ScrapySettings(
                {
                    "SCRAPY_CIRCUIT_BREAKER_ENABLED": True,
                    "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD": 5,
                    "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT": 30.0,
                }
            )
        )

        breaker = manager._get_breaker()
        assert breaker is not None
        assert breaker.failure_threshold == 5
        assert breaker.reset_timeout == 30.0
    finally:
        manager.close()


def test_apply_scrapy_breaker_policy_warns_on_dropped_differing_policy(
    monkeypatch, caplog
):
    """R137-F3: two spiders sharing one early-acquired manager cannot both
    own its single breaker. First explicit resolution wins; a later DIFFERING
    explicit policy is dropped with a one-shot warning instead of silently
    overwriting (last-write-wins) the first spider's configuration. Re-applying
    the SAME policy never warns."""
    from scrapy.settings import Settings as ScrapySettings

    def _policy(threshold: int) -> ScrapySettings:
        return ScrapySettings(
            {
                "SCRAPY_CIRCUIT_BREAKER_ENABLED": True,
                "SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD": threshold,
                "SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT": 7.5,
            }
        )

    manager = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": "r137-dropped-policy-host"}
    )
    try:
        with caplog.at_level(logging.WARNING, logger=connectors_module.__name__):
            manager.apply_scrapy_breaker_policy(_policy(3))
            manager.apply_scrapy_breaker_policy(_policy(3))  # same → no warn
            manager.apply_scrapy_breaker_policy(_policy(9))  # differing → warn
            manager.apply_scrapy_breaker_policy(_policy(9))  # again → once only

        warnings = [
            r for r in caplog.records if "circuit breaker policy" in r.getMessage()
        ]
        assert len(warnings) == 1
        breaker = manager._get_breaker()
        assert breaker is not None
        assert breaker.failure_threshold == 3  # first explicit policy wins
    finally:
        manager.close()
