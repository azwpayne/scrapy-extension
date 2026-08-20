"""Tests for MemcachedBackend (subsystem ③) with mocked client seams."""

from __future__ import annotations

import socket
import subprocess
import sys
import traceback
from threading import Event, Thread
from time import monotonic

import pytest

import scrapy_extension.backends.memcached as memcached_mod
from scrapy_extension.backends.base import (
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.backends.memcached import MemcachedBackend
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
)
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import MemcachedMode, MemcachedSettings


def _make_backend(**overrides) -> MemcachedBackend:
    return MemcachedBackend(MemcachedSettings(**overrides))


def _connected(mocker):
    b = _make_backend()
    client = mocker.MagicMock()
    client.stats.return_value = {}
    client.set.return_value = True
    # Patch the backend's captured MemcachedClient name (bound at import).
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    b.connect()
    return b, client


class TestMemcachedBackendType:
    def test_backend_type_is_memcached(self) -> None:
        assert _make_backend().backend_type is BackendType.MEMCACHED

    def test_storage_only_no_queue_no_set(self) -> None:
        b = _make_backend()
        assert isinstance(b, StorageBackend)
        assert not isinstance(b, QueueBackend)
        assert not isinstance(b, SetBackend)

    def test_settings_defaults(self) -> None:
        s = MemcachedSettings()
        assert s.mode is MemcachedMode.STANDALONE
        assert s.host == "127.0.0.1"
        assert s.port == 11211
        assert s.allow_remote_plaintext is False
        assert s.connect_timeout == 5.0
        assert s.socket_timeout == 30.0
        assert s.allow_flush_all is False

    @pytest.mark.parametrize(
        "timeout",
        [True, False, float("nan"), float("inf"), -float("inf"), 0, -1, 86_400.1],
    )
    @pytest.mark.parametrize("setting_name", ["connect_timeout", "socket_timeout"])
    def test_timeouts_require_finite_positive_bounded_numbers(
        self, timeout: object, setting_name: str
    ) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedSettings(**{setting_name: timeout})

        assert exc_info.value.setting_name == setting_name
        assert str(exc_info.value) == (
            "Memcached timeout must be finite, greater than 0, and at most 86400 "
            "seconds."
        )

    @pytest.mark.parametrize(
        ("env_name", "expected"),
        [
            ("SCRAPY_MEMCACHED_CONNECT_TIMEOUT", 1.25),
            ("SCRAPY_MEMCACHED_SOCKET_TIMEOUT", 2.5),
        ],
    )
    def test_timeouts_accept_environment_numbers(
        self, monkeypatch, env_name: str, expected: float
    ) -> None:
        monkeypatch.setenv(env_name, str(expected))
        settings = MemcachedSettings()
        field_name = env_name.removeprefix("SCRAPY_MEMCACHED_").lower()

        assert getattr(settings, field_name) == expected

    @pytest.mark.parametrize("allow_flush_all", [1, 0, "yes", None])
    def test_allow_flush_all_requires_exact_boolean(self, allow_flush_all) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedSettings(allow_flush_all=allow_flush_all)
        assert exc_info.value.setting_name == "allow_flush_all"

    @pytest.mark.parametrize(
        ("env_value", "expected"), [("true", True), ("false", False)]
    )
    def test_allow_flush_all_accepts_canonical_environment_boolean(
        self, monkeypatch, env_value: str, expected: bool
    ) -> None:
        monkeypatch.setenv("SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL", env_value)
        assert MemcachedSettings().allow_flush_all is expected


class TestMemcachedConnect:
    def test_unsupported_mode_is_configuration_error(self) -> None:
        b = _make_backend()
        b.config.mode = "unsupported"  # type: ignore[assignment]

        with pytest.raises(ConfigurationError) as exc_info:
            b.connect()

        assert exc_info.value.setting_name == "mode"

    def test_connect_creates_client_and_stats(self, mocker) -> None:
        b, client = _connected(mocker)
        memcached_mod.MemcachedClient.assert_called_once_with(
            ("127.0.0.1", 11211),
            connect_timeout=5.0,
            timeout=30.0,
            default_noreply=False,
        )
        client.stats.assert_called_once()
        assert b.is_connected() is True

    def test_connect_diagnostics_hide_remote_endpoint(self, mocker) -> None:
        marker = "memcached-endpoint-log-marker"
        backend = _make_backend(host=f"{marker}.example", allow_remote_plaintext=True)
        client = mocker.MagicMock(name="client")
        client.stats.return_value = {}
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        logger_warning = mocker.patch.object(memcached_mod.logger, "warning")
        logger_debug = mocker.patch.object(memcached_mod.logger, "debug")

        backend.connect()

        logger_warning.assert_called_once_with(
            "Remote Memcached plaintext was explicitly enabled; use only an "
            "isolated trusted network."
        )
        logger_debug.assert_called_once_with("Connected to Memcached.")
        assert marker not in repr(
            (logger_warning.call_args_list, logger_debug.call_args_list)
        )

    def test_connect_keeps_remote_client_live_when_warning_interrupts(
        self, mocker
    ) -> None:
        backend = _make_backend(host="cache.internal", allow_remote_plaintext=True)
        client = mocker.MagicMock(name="client")
        client.stats.return_value = {}
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        mocker.patch.object(
            memcached_mod.logger, "warning", side_effect=KeyboardInterrupt
        )

        backend.connect()

        assert backend.is_connected() is True
        assert backend._client is client
        client.close.assert_not_called()

    def test_connect_keeps_loopback_client_live_when_debug_interrupts(
        self, mocker
    ) -> None:
        backend = _make_backend()
        client = mocker.MagicMock(name="client")
        client.stats.return_value = {}
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        mocker.patch.object(
            memcached_mod.logger, "debug", side_effect=KeyboardInterrupt
        )

        backend.connect()

        assert backend.is_connected() is True
        assert backend._client is client
        client.close.assert_not_called()

    def test_connect_is_idempotent_while_connected(self, mocker) -> None:
        b, client = _connected(mocker)

        b.connect()

        memcached_mod.MemcachedClient.assert_called_once_with(
            ("127.0.0.1", 11211),
            connect_timeout=5.0,
            timeout=30.0,
            default_noreply=False,
        )
        client.stats.assert_called_once_with()

    def test_connect_does_not_publish_client_before_probe_succeeds(
        self, mocker
    ) -> None:
        stats_entered = Event()
        release_stats = Event()
        client = mocker.MagicMock(name="client")

        def blocking_stats():
            stats_entered.set()
            assert release_stats.wait(timeout=2.0)
            return {}

        client.stats.side_effect = blocking_stats
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        backend = _make_backend()
        errors: list[BaseException] = []

        def connect() -> None:
            try:
                backend.connect()
            except BaseException as error:  # pragma: no cover - assertion aid
                errors.append(error)

        thread = Thread(target=connect)
        thread.start()
        assert stats_entered.wait(timeout=2.0)
        was_private_during_probe = not backend.is_connected()
        release_stats.set()
        thread.join(timeout=2.0)

        assert was_private_during_probe
        assert not thread.is_alive()
        assert errors == []
        assert backend.is_connected() is True

    def test_connect_revalidates_mutated_remote_host_before_sdk_io(
        self, mocker
    ) -> None:
        settings = MemcachedSettings()
        settings.host = "cache.internal"
        client = mocker.patch.object(memcached_mod, "MemcachedClient")

        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedBackend(settings).connect()

        assert exc_info.value.setting_name == "allow_remote_plaintext"
        client.assert_not_called()

    def test_lookalike_localhost_requires_explicit_remote_plaintext_opt_in(
        self,
    ) -> None:
        """A suffix hostname is DNS-controlled and not a local Memcached boundary."""
        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedSettings(host="attacker.localhost")

        assert exc_info.value.setting_name == "allow_remote_plaintext"

    def test_connect_revalidates_mutated_lookalike_localhost_before_sdk_io(
        self, mocker
    ) -> None:
        settings = MemcachedSettings()
        settings.host = "attacker.localhost"
        client = mocker.patch.object(memcached_mod, "MemcachedClient")

        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedBackend(settings).connect()

        assert exc_info.value.setting_name == "allow_remote_plaintext"
        client.assert_not_called()

    def test_connect_revalidates_mutated_port_before_sdk_io(self, mocker) -> None:
        settings = MemcachedSettings()
        settings.port = 0
        client = mocker.patch.object(memcached_mod, "MemcachedClient")

        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedBackend(settings).connect()

        assert exc_info.value.setting_name == "port"
        client.assert_not_called()

    @pytest.mark.parametrize("setting_name", ["connect_timeout", "socket_timeout"])
    @pytest.mark.parametrize("raw_timeout", [" 1 ", "+1", "01", "1e2", "1."])
    def test_connect_revalidates_mutated_timeout_before_sdk_io(
        self, mocker, setting_name: str, raw_timeout: str
    ) -> None:
        settings = MemcachedSettings()
        setattr(settings, setting_name, raw_timeout)
        client = mocker.patch.object(memcached_mod, "MemcachedClient")

        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedBackend(settings).connect()

        assert exc_info.value.setting_name == setting_name
        assert exc_info.value.setting_value is None
        assert str(exc_info.value) == "Memcached configuration is invalid."
        client.assert_not_called()

    def test_connect_revalidates_mutated_flush_permission_before_sdk_io(
        self, mocker
    ) -> None:
        settings = MemcachedSettings()
        settings.allow_flush_all = "yes"  # type: ignore[assignment]
        client = mocker.patch.object(memcached_mod, "MemcachedClient")

        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedBackend(settings).connect()

        assert exc_info.value.setting_name == "allow_flush_all"
        client.assert_not_called()

    def test_connect_retains_one_preconstruction_snapshot(self, mocker) -> None:
        settings = MemcachedSettings(host="cache.internal", allow_remote_plaintext=True)
        client = mocker.MagicMock(name="client")
        client.stats.return_value = {}

        def mutate_after_construction(_endpoint, **_kwargs):
            settings.host = "attacker.internal"
            settings.port = 22122
            settings.allow_remote_plaintext = False
            settings.connect_timeout = 45.0
            settings.socket_timeout = 60.0
            return client

        client_factory = mocker.patch.object(
            memcached_mod,
            "MemcachedClient",
            side_effect=mutate_after_construction,
        )
        backend = MemcachedBackend(settings)

        backend.connect()

        client_factory.assert_called_once_with(
            ("cache.internal", 11211),
            connect_timeout=5.0,
            timeout=30.0,
            default_noreply=False,
        )
        assert backend._connection_snapshot is not None
        assert backend._connection_snapshot.host == "cache.internal"
        assert backend._connection_snapshot.port == 11211
        assert backend._connection_snapshot.allow_remote_plaintext is True
        assert backend._connection_snapshot.connect_timeout == 5.0
        assert backend._connection_snapshot.socket_timeout == 30.0

    def test_blocked_stats_probe_is_bounded_by_socket_timeout(
        self, socket_enabled
    ) -> None:
        """A server that accepts but never replies cannot wedge ``connect()``."""
        del socket_enabled
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        _host, port = server.getsockname()
        accepted = Event()
        release_server = Event()

        def stall_after_accept() -> None:
            connection, _address = server.accept()
            with connection:
                accepted.set()
                release_server.wait(timeout=2.0)

        server_thread = Thread(target=stall_after_accept)
        server_thread.start()
        backend = _make_backend(
            host="127.0.0.1",
            port=port,
            connect_timeout=0.05,
            socket_timeout=0.05,
        )

        started = monotonic()
        try:
            with pytest.raises(BackendConnectionError):
                backend.connect()
            elapsed = monotonic() - started
            assert accepted.wait(timeout=0.5)
            assert elapsed < 1.0
            assert backend.is_connected() is False
        finally:
            release_server.set()
            server.close()
            server_thread.join(timeout=2.0)

        assert not server_thread.is_alive()

    def test_disconnect_returns_and_fences_in_progress_connect_probe(
        self, mocker
    ) -> None:
        stats_entered = Event()
        release_stats = Event()
        disconnect_returned = Event()
        client = mocker.MagicMock(name="client")

        def blocking_stats():
            stats_entered.set()
            assert release_stats.wait(timeout=2.0)
            return {}

        client.stats.side_effect = blocking_stats
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        backend = _make_backend()

        connect_thread = Thread(target=backend.connect)
        connect_thread.start()
        assert stats_entered.wait(timeout=2.0)

        def disconnect() -> None:
            backend.disconnect()
            disconnect_returned.set()

        disconnect_thread = Thread(target=disconnect)
        disconnect_thread.start()
        returned_during_probe = disconnect_returned.wait(timeout=2.0)
        release_stats.set()
        connect_thread.join(timeout=2.0)
        disconnect_thread.join(timeout=2.0)

        assert returned_during_probe is True
        assert not connect_thread.is_alive()
        assert not disconnect_thread.is_alive()
        assert backend.is_connected() is False
        client.close.assert_called_once()

    def test_startup_error_traceback_does_not_echo_driver_text(self, mocker) -> None:
        secret = "memcached-driver-secret"
        mocker.patch.object(
            memcached_mod,
            "MemcachedClient",
            side_effect=RuntimeError(f"driver dump included {secret}"),
        )

        with pytest.raises(BackendConnectionError) as exc_info:
            _make_backend().connect()

        rendered = "".join(traceback.format_exception(exc_info.value))
        assert secret not in str(exc_info.value)
        assert secret not in rendered
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None

    def test_connect_failure_raises(self, mocker) -> None:
        b = _make_backend()
        mocker.patch.object(
            memcached_mod, "MemcachedClient", side_effect=RuntimeError("nope")
        )
        with pytest.raises(BackendConnectionError):
            b.connect()
        assert b.is_connected() is False

    def test_malformed_stats_is_closed_and_a_later_generation_can_connect(
        self, mocker
    ) -> None:
        marker = "malformed-stats-private-marker"
        malformed = mocker.MagicMock(name="malformed")
        malformed.stats.return_value = marker
        healthy = mocker.MagicMock(name="healthy")
        healthy.stats.return_value = {}
        mocker.patch.object(
            memcached_mod, "MemcachedClient", side_effect=[malformed, healthy]
        )
        backend = _make_backend()

        with pytest.raises(BackendConnectionError) as exc_info:
            backend.connect()

        rendered = "".join(traceback.format_exception(exc_info.value))
        assert marker not in rendered
        assert backend.is_connected() is False
        malformed.close.assert_called_once_with()

        backend.connect()

        assert backend.is_connected() is True
        assert backend._client is healthy
        healthy.close.assert_not_called()

    def test_connect_stats_failure_nulls_client(self, mocker) -> None:
        """R-mcc: stats() failure must null the half-created client.

        pymemcache's Client ctor is lazy (no network I/O); ``stats()`` is the real
        probe. Pre-fix, a failed ``stats()`` left ``_client`` pointing at a
        never-connected client, so ``is_connected()`` returned True after a
        ``connect()`` that already raised ``BackendConnectionError`` -- wedging the
        backend "connected-but-dead" (``ConnectionManager.is_connected()`` delegates
        here, so external health checks saw the lying True and skipped reconnect).
        Mirrors RabbitMQ R25-A1 null-on-failure. The ctor-raises path
        (``test_connect_failure_raises``) is unaffected -- the ``is not None`` guard
        skips close when ``_client`` was never assigned.
        """
        b = _make_backend()
        client = mocker.MagicMock()
        client.stats.side_effect = RuntimeError("stats probe failed")
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        with pytest.raises(BackendConnectionError):
            b.connect()
        assert b.is_connected() is False
        client.close.assert_called_once()

    def test_connect_stats_failure_preserves_backend_error_when_close_interrupts(
        self, mocker
    ) -> None:
        backend = _make_backend()
        client = mocker.MagicMock()
        client.stats.side_effect = RuntimeError("stats probe failed")
        client.close.side_effect = KeyboardInterrupt
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)

        with pytest.raises(BackendConnectionError) as raised:
            backend.connect()

        assert raised.value.__cause__ is None
        client.close.assert_called_once()
        assert backend.is_connected() is False

    def test_connect_stats_baseexception_closes_candidate(self, mocker) -> None:
        """R17-C: a Ctrl+C during the stats() probe must close the candidate socket.

        ``stats()`` is the first command to open the TCP socket (pymemcache is
        lazy). The cleanup arm was ``except Exception``, which cannot catch
        ``BaseException``, so a ``KeyboardInterrupt`` raised by ``stats()``
        escaped before ``candidate.close()`` ran, leaking the open socket on the
        local candidate. The candidate is never published (generation-fenced), so
        ``is_connected()`` stays truthful — bounded to a single FD per occurrence.
        R16-A parity: kafka/rocketmq/dynamodb carry the ``except BaseException``
        connect arm; memcached was missed.
        """
        b = _make_backend()
        client = mocker.MagicMock()
        primary = KeyboardInterrupt()
        client.stats.side_effect = primary
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        with pytest.raises(KeyboardInterrupt) as raised:
            b.connect()
        assert raised.value is primary
        assert b.is_connected() is False
        client.close.assert_called_once()

    def test_connect_stats_control_signal_survives_close_control_signal(
        self, mocker
    ) -> None:
        backend = _make_backend()
        client = mocker.MagicMock()
        primary = KeyboardInterrupt()
        client.stats.side_effect = primary
        client.close.side_effect = SystemExit
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)

        with pytest.raises(KeyboardInterrupt) as raised:
            backend.connect()

        assert raised.value is primary
        client.close.assert_called_once()
        assert backend.is_connected() is False

    def test_disconnect_closes_client(self, mocker) -> None:
        b, client = _connected(mocker)
        b.disconnect()
        client.close.assert_called_once()
        assert b.is_connected() is False
        assert b._connection_snapshot is None


def test_locked_pymemcache_requires_explicit_reply_confirmation() -> None:
    """Pin the SDK default that makes backend-side opt-out load-bearing."""
    script = "\n".join(
        (
            "import inspect",
            "from pymemcache.client.base import Client",
            "parameter = inspect.signature(Client).parameters['default_noreply']",
            "assert parameter.default is True",
        )
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


class TestMemcachedStorageOps:
    def test_single_socket_operations_do_not_overlap(self, mocker) -> None:
        backend, client = _connected(mocker)
        get_entered = Event()
        release_get = Event()
        store_attempted = Event()
        set_entered = Event()
        errors: list[BaseException] = []

        def blocking_get(_key):
            get_entered.set()
            assert release_get.wait(timeout=2.0)
            return b"value"

        def observed_set(*_args, **_kwargs):
            set_entered.set()
            return True

        client.get.side_effect = blocking_get
        client.set.side_effect = observed_set

        def retrieve() -> None:
            try:
                backend.retrieve("read-key")
            except BaseException as error:  # pragma: no cover - assertion aid
                errors.append(error)

        def store() -> None:
            store_attempted.set()
            try:
                backend.store("write-key", b"value")
            except BaseException as error:  # pragma: no cover - assertion aid
                errors.append(error)

        retrieve_thread = Thread(target=retrieve)
        store_thread = Thread(target=store)
        retrieve_thread.start()
        assert get_entered.wait(timeout=2.0)
        store_thread.start()
        assert store_attempted.wait(timeout=2.0)
        overlapped = set_entered.wait(timeout=0.2)
        release_get.set()
        retrieve_thread.join(timeout=2.0)
        store_thread.join(timeout=2.0)

        assert overlapped is False
        assert set_entered.is_set()
        assert errors == []

    @pytest.mark.parametrize("stats_response", [None, b"stats", [], True])
    def test_ping_rejects_malformed_stats_response(
        self, mocker, stats_response: object
    ) -> None:
        backend, client = _connected(mocker)
        client.stats.return_value = stats_response

        assert backend.ping() is False

    def test_ping_accepts_mapping_stats_response(self, mocker) -> None:
        backend, client = _connected(mocker)
        client.stats.return_value = {b"version": b"1.6"}

        assert backend.ping() is True

    def test_ping_does_not_overlap_storage_operation(self, mocker) -> None:
        backend, client = _connected(mocker)
        stats_entered = Event()
        release_stats = Event()
        retrieve_attempted = Event()
        get_entered = Event()

        def blocking_stats():
            stats_entered.set()
            assert release_stats.wait(timeout=2.0)
            return {}

        def observed_get(_key):
            get_entered.set()
            return b"value"

        client.stats.side_effect = blocking_stats
        client.get.side_effect = observed_get
        ping_thread = Thread(target=backend.ping)

        def retrieve() -> None:
            retrieve_attempted.set()
            backend.retrieve("key")

        retrieve_thread = Thread(target=retrieve)
        ping_thread.start()
        assert stats_entered.wait(timeout=2.0)
        retrieve_thread.start()
        assert retrieve_attempted.wait(timeout=2.0)
        overlapped = get_entered.wait(timeout=0.2)
        release_stats.set()
        ping_thread.join(timeout=2.0)
        retrieve_thread.join(timeout=2.0)

        assert overlapped is False
        assert get_entered.is_set()

    def test_disconnect_waits_for_active_storage_operation(self, mocker) -> None:
        backend, client = _connected(mocker)
        get_entered = Event()
        release_get = Event()
        disconnect_returned = Event()

        def blocking_get(_key):
            get_entered.set()
            assert release_get.wait(timeout=2.0)
            return b"value"

        client.get.side_effect = blocking_get
        retrieve_thread = Thread(target=lambda: backend.retrieve("key"))

        def disconnect() -> None:
            backend.disconnect()
            disconnect_returned.set()

        disconnect_thread = Thread(target=disconnect)
        retrieve_thread.start()
        assert get_entered.wait(timeout=2.0)
        disconnect_thread.start()
        returned_during_operation = disconnect_returned.wait(timeout=0.2)
        release_get.set()
        retrieve_thread.join(timeout=2.0)
        disconnect_thread.join(timeout=2.0)

        assert returned_during_operation is False
        assert backend.is_connected() is False
        client.close.assert_called_once()

    def test_store_sets_with_ttl(self, mocker) -> None:
        b, client = _connected(mocker)
        b.store("key1", b"value", ttl=60)
        client.set.assert_called_once_with("key1", b"value", expire=60)

    def test_store_without_ttl(self, mocker) -> None:
        b, client = _connected(mocker)
        b.store("key1", b"value")
        client.set.assert_called_once_with("key1", b"value", expire=0)

    def test_store_with_none_ttl_uses_memcached_no_expiry_sentinel(
        self, mocker
    ) -> None:
        b, client = _connected(mocker)

        b.store("key1", b"value", ttl=None)

        client.set.assert_called_once_with("key1", b"value", expire=0)

    def test_store_ttl_over_30_days_converts_to_absolute_timestamp(
        self, mocker
    ) -> None:
        # Memcached treats exptime > 60*60*24*30 (2_592_000) as an ABSOLUTE
        # Unix epoch, not relative seconds. A 31-day TTL must therefore be
        # converted to (now + ttl) so the server does not read it as a past
        # timestamp (1970-01-31) and silently expire the item on write.
        fixed_now = 1_700_000_000
        mocker.patch("time.time", return_value=fixed_now)

        b, client = _connected(mocker)
        b.store("key1", b"value", ttl=2_592_001)

        client.set.assert_called_once_with(
            "key1", b"value", expire=fixed_now + 2_592_001
        )

    def test_retrieve_gets(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get.return_value = b"payload"
        assert b.retrieve("key1") == b"payload"
        client.get.assert_called_once_with("key1")

    def test_retrieve_missing_returns_none(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get.return_value = None
        assert b.retrieve("key1") is None

    def test_retrieve_normalizes_bytearray(self, mocker) -> None:
        backend, client = _connected(mocker)
        client.get.return_value = bytearray(b"payload")

        assert backend.retrieve("key1") == b"payload"

    @pytest.mark.parametrize("response", [True, 1, "payload", [], memoryview(b"x")])
    def test_retrieve_rejects_malformed_response(
        self, mocker, response: object
    ) -> None:
        backend, client = _connected(mocker)
        client.get.return_value = response

        with pytest.raises(StorageError) as exc_info:
            backend.retrieve("key1")

        assert exc_info.value.operation == "retrieve"

    @pytest.mark.parametrize("response", [True, False])
    def test_delete_returns_exact_bool(self, mocker, response: bool) -> None:
        backend, client = _connected(mocker)
        client.delete.return_value = response

        assert backend.delete("key1") is response
        client.delete.assert_called_once_with("key1")

    @pytest.mark.parametrize("response", [None, 0, 1, "deleted", [], b"deleted"])
    def test_delete_rejects_non_boolean_response(
        self, mocker, response: object
    ) -> None:
        backend, client = _connected(mocker)
        client.delete.return_value = response

        with pytest.raises(StorageError) as exc_info:
            backend.delete("key1")

        assert exc_info.value.operation == "delete"

    def test_exists_uses_get(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get.return_value = b"x"
        assert b.exists("key1") is True
        client.get.assert_called_once_with("key1")

    def test_exists_missing(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get.return_value = None
        assert b.exists("key1") is False

    @pytest.mark.parametrize("response", [True, 1, "payload", [], memoryview(b"x")])
    def test_exists_reuses_get_response_validation(
        self, mocker, response: object
    ) -> None:
        backend, client = _connected(mocker)
        client.get.return_value = response

        with pytest.raises(StorageError) as exc_info:
            backend.exists("key1")

        assert exc_info.value.operation == "exists"

    def test_ttl_returns_none(self, mocker) -> None:
        b, _ = _connected(mocker)
        assert b.ttl("key1") is None

    def test_clear_storage_flushes_all_when_explicitly_enabled(self, mocker) -> None:
        b = _make_backend(allow_flush_all=True)
        client = mocker.MagicMock()
        client.stats.return_value = {}
        client.flush_all.return_value = True
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        b.connect()
        b.clear_storage()
        client.flush_all.assert_called_once()

    def test_clear_storage_uses_connected_generation_permission(self, mocker) -> None:
        settings = MemcachedSettings(allow_flush_all=True)
        backend = MemcachedBackend(settings)
        client = mocker.MagicMock()
        client.stats.return_value = {}
        client.flush_all.return_value = True
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        backend.connect()
        settings.allow_flush_all = False

        backend.clear_storage()

        client.flush_all.assert_called_once()

    def test_mutation_cannot_enable_flush_for_connected_generation(
        self, mocker
    ) -> None:
        settings = MemcachedSettings()
        backend = MemcachedBackend(settings)
        client = mocker.MagicMock()
        client.stats.return_value = {}
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        backend.connect()
        settings.allow_flush_all = "yes"  # type: ignore[assignment]

        with pytest.raises(NotImplementedError, match="allow_flush_all"):
            backend.clear_storage()

        client.flush_all.assert_not_called()

    @pytest.mark.parametrize("response", [False, None, 1])
    def test_clear_storage_rejected_reply_raises_storage_error(
        self, mocker, response: object
    ) -> None:
        backend = _make_backend(allow_flush_all=True)
        client = mocker.MagicMock()
        client.stats.return_value = {}
        client.flush_all.return_value = response
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        backend.connect()

        with pytest.raises(StorageError) as exc_info:
            backend.clear_storage()

        assert exc_info.value.operation == "clear_storage"

    def test_clear_storage_rejects_global_flush_by_default(self, mocker) -> None:
        b, client = _connected(mocker)

        with pytest.raises(NotImplementedError, match="allow_flush_all"):
            b.clear_storage()

        client.flush_all.assert_not_called()

    def test_clear_storage_rejects_prefix(self, mocker) -> None:
        # R3: prefix-based clear is unsupported on Memcached (flush_all is global).
        # Calling clear_storage(prefix=...) must raise NotImplementedError and must
        # NOT call flush_all — silently flushing a shared cache would cross-tenant
        # destroy data.
        b, client = _connected(mocker)
        with pytest.raises(NotImplementedError):
            b.clear_storage(prefix="foo")
        client.flush_all.assert_not_called()

    def test_invalid_key_raises(self, mocker) -> None:
        b, _ = _connected(mocker)
        with pytest.raises(ValueError):
            b.store("bad key!", b"x")


# ---------------------------------------------------------------------------
# R14-A: StorageBackend error-contract uniformity.
# Storage ops must raise StorageError on failure (not silently swallow to
# None/False — that masked data loss in the item pipeline).
# ---------------------------------------------------------------------------


class TestMemcachedStorageErrorContract:
    """R14-A: each storage op raises StorageError on client-lib failure."""

    @pytest.mark.parametrize("result", [False, None, 1])
    def test_store_rejected_result_raises_storage_error(self, mocker, result) -> None:
        """A rejected write must not be reported as a successful store."""
        b, client = _connected(mocker)
        client.set.return_value = result

        with pytest.raises(StorageError) as exc_info:
            b.store("key1", b"value")

        assert exc_info.value.operation == "store"
        assert exc_info.value.key is None

    def test_store_failure_raises_storage_error(self, mocker) -> None:
        b, client = _connected(mocker)
        client.set.side_effect = RuntimeError("memcached unreachable")
        with pytest.raises(StorageError) as exc_info:
            b.store("key1", b"value")
        assert exc_info.value.operation == "store"
        assert exc_info.value.key is None
        assert exc_info.value.__cause__ is None

    def test_retrieve_failure_raises_storage_error(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get.side_effect = RuntimeError("memcached unreachable")
        with pytest.raises(StorageError) as exc_info:
            b.retrieve("key1")
        assert exc_info.value.operation == "retrieve"
        assert exc_info.value.key is None

    def test_delete_failure_raises_storage_error(self, mocker) -> None:
        b, client = _connected(mocker)
        client.delete.side_effect = RuntimeError("memcached unreachable")
        with pytest.raises(StorageError) as exc_info:
            b.delete("key1")
        assert exc_info.value.operation == "delete"
        assert exc_info.value.key is None

    def test_exists_failure_raises_storage_error(self, mocker) -> None:
        b, client = _connected(mocker)
        client.get.side_effect = RuntimeError("memcached unreachable")
        with pytest.raises(StorageError) as exc_info:
            b.exists("key1")
        assert exc_info.value.operation == "exists"
        assert exc_info.value.key is None

    def test_clear_storage_failure_raises_storage_error(self, mocker) -> None:
        b = _make_backend(allow_flush_all=True)
        client = mocker.MagicMock()
        client.stats.return_value = {}
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        b.connect()
        client.flush_all.side_effect = RuntimeError("memcached unreachable")
        with pytest.raises(StorageError) as exc_info:
            b.clear_storage()
        assert exc_info.value.operation == "clear_storage"
        assert exc_info.value.key is None
        assert exc_info.value.__cause__ is None

    def test_storage_error_is_backend_error_subclass(self, mocker) -> None:
        """``except BackendError`` must catch storage-path failures."""
        from scrapy_extension.exceptions.base import BackendError

        b, client = _connected(mocker)
        client.set.side_effect = RuntimeError("boom")
        with pytest.raises(BackendError):
            b.store("key1", b"value")


# ---------------------------------------------------------------------------
# R138-F2: clear_storage lifecycle classification.
# A None connection snapshot (never-connected or disconnected) is a lifecycle
# state, not a capability gap — it must surface the static storage contract,
# never the allow_flush_all advisory for a flag the operator already enabled.
# ---------------------------------------------------------------------------


class TestMemcachedClearStorageLifecycleContract:
    def test_never_connected_clear_storage_is_storage_error_not_advisory(
        self,
    ) -> None:
        backend = _make_backend(allow_flush_all=True)
        assert backend._connection_snapshot is None

        with pytest.raises(StorageError) as exc_info:
            backend.clear_storage()

        error = exc_info.value
        assert type(error) is StorageError
        assert str(error) == "Memcached storage clear failed."
        assert error.operation == "clear_storage"
        assert error.key is None
        assert "SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL" not in str(error)

    def test_disconnected_clear_storage_is_storage_error_not_advisory(
        self, mocker
    ) -> None:
        backend = _make_backend(allow_flush_all=True)
        client = mocker.MagicMock()
        client.stats.return_value = {}
        client.flush_all.return_value = True
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        backend.connect()
        backend.disconnect()
        assert backend._connection_snapshot is None

        with pytest.raises(StorageError) as exc_info:
            backend.clear_storage()

        error = exc_info.value
        assert type(error) is StorageError
        assert str(error) == "Memcached storage clear failed."
        assert error.operation == "clear_storage"
        assert error.key is None
        assert "SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL" not in str(error)
        client.flush_all.assert_not_called()

    def test_connected_capability_disabled_keeps_not_implemented_error(
        self, mocker
    ) -> None:
        backend = _make_backend()
        client = mocker.MagicMock()
        client.stats.return_value = {}
        mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
        backend.connect()
        assert backend._connection_snapshot is not None

        with pytest.raises(NotImplementedError) as exc_info:
            backend.clear_storage()

        assert type(exc_info.value) is NotImplementedError
        assert (
            str(exc_info.value)
            == memcached_mod._MEMCACHED_CLEAR_STORAGE_DISABLED_MESSAGE
        )
        client.flush_all.assert_not_called()
