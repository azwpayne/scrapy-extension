"""Tests for MemcachedBackend (subsystem ③) with mocked client seams."""

from __future__ import annotations

import subprocess
import sys
import traceback
from threading import Event, Thread

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
    assert s.host == "localhost"
    assert s.port == 11211
    assert s.allow_remote_plaintext is False
    assert s.allow_flush_all is False

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
      ("localhost", 11211), default_noreply=False
    )
    client.stats.assert_called_once()
    assert b.is_connected() is True

  def test_connect_is_idempotent_while_connected(self, mocker) -> None:
    b, client = _connected(mocker)

    b.connect()

    memcached_mod.MemcachedClient.assert_called_once_with(
      ("localhost", 11211), default_noreply=False
    )
    client.stats.assert_called_once_with()

  def test_connect_does_not_publish_client_before_probe_succeeds(self, mocker) -> None:
    stats_entered = Event()
    release_stats = Event()
    client = mocker.MagicMock(name="client")

    def blocking_stats():
      stats_entered.set()
      assert release_stats.wait(timeout=2.0)

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

  def test_connect_revalidates_mutated_port_before_sdk_io(self, mocker) -> None:
    settings = MemcachedSettings()
    settings.port = 0
    client = mocker.patch.object(memcached_mod, "MemcachedClient")

    with pytest.raises(ConfigurationError) as exc_info:
      MemcachedBackend(settings).connect()

    assert exc_info.value.setting_name == "port"
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
    settings = MemcachedSettings(
      host="cache.internal", allow_remote_plaintext=True
    )
    client = mocker.MagicMock(name="client")

    def mutate_after_construction(_endpoint, **_kwargs):
      settings.host = "attacker.internal"
      settings.port = 22122
      settings.allow_remote_plaintext = False
      return client

    client_factory = mocker.patch.object(
      memcached_mod,
      "MemcachedClient",
      side_effect=mutate_after_construction,
    )
    backend = MemcachedBackend(settings)

    backend.connect()

    client_factory.assert_called_once_with(
      ("cache.internal", 11211), default_noreply=False
    )
    assert backend._connection_snapshot is not None
    assert backend._connection_snapshot.host == "cache.internal"
    assert backend._connection_snapshot.port == 11211
    assert backend._connection_snapshot.allow_remote_plaintext is True

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

  def test_connect_failure_raises(self, mocker) -> None:
    b = _make_backend()
    mocker.patch.object(
      memcached_mod, "MemcachedClient", side_effect=RuntimeError("nope")
    )
    with pytest.raises(BackendConnectionError):
      b.connect()
    assert b.is_connected() is False

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

  def test_store_with_none_ttl_uses_memcached_no_expiry_sentinel(self, mocker) -> None:
    b, client = _connected(mocker)

    b.store("key1", b"value", ttl=None)

    client.set.assert_called_once_with("key1", b"value", expire=0)

  def test_retrieve_gets(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.return_value = b"payload"
    assert b.retrieve("key1") == b"payload"
    client.get.assert_called_once_with("key1")

  def test_retrieve_missing_returns_none(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.return_value = None
    assert b.retrieve("key1") is None

  def test_delete_returns_bool(self, mocker) -> None:
    b, client = _connected(mocker)
    client.delete.return_value = True
    assert b.delete("key1") is True
    client.delete.assert_called_once_with("key1")

  def test_exists_uses_get(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.return_value = b"x"
    assert b.exists("key1") is True
    client.get.assert_called_once_with("key1")

  def test_exists_missing(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.return_value = None
    assert b.exists("key1") is False

  def test_ttl_returns_none(self, mocker) -> None:
    b, _ = _connected(mocker)
    assert b.ttl("key1") is None

  def test_clear_storage_flushes_all_when_explicitly_enabled(self, mocker) -> None:
    b = _make_backend(allow_flush_all=True)
    client = mocker.MagicMock()
    client.flush_all.return_value = True
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    b.connect()
    b.clear_storage()
    client.flush_all.assert_called_once()

  def test_clear_storage_uses_connected_generation_permission(self, mocker) -> None:
    settings = MemcachedSettings(allow_flush_all=True)
    backend = MemcachedBackend(settings)
    client = mocker.MagicMock()
    client.flush_all.return_value = True
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    backend.connect()
    settings.allow_flush_all = False

    backend.clear_storage()

    client.flush_all.assert_called_once()

  def test_mutation_cannot_enable_flush_for_connected_generation(self, mocker) -> None:
    settings = MemcachedSettings()
    backend = MemcachedBackend(settings)
    client = mocker.MagicMock()
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    backend.connect()
    settings.allow_flush_all = "yes"  # type: ignore[assignment]

    with pytest.raises(NotImplementedError, match="allow_flush_all"):
      backend.clear_storage()

    client.flush_all.assert_not_called()

  def test_clear_storage_rejected_reply_raises_storage_error(self, mocker) -> None:
    backend = _make_backend(allow_flush_all=True)
    client = mocker.MagicMock()
    client.flush_all.return_value = False
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

  @pytest.mark.parametrize("result", [False, None])
  def test_store_rejected_result_raises_storage_error(self, mocker, result) -> None:
    """A rejected write must not be reported as a successful store."""
    b, client = _connected(mocker)
    client.set.return_value = result

    with pytest.raises(StorageError) as exc_info:
      b.store("key1", b"value")

    assert exc_info.value.operation == "store"
    assert exc_info.value.key == "key1"

  def test_store_failure_raises_storage_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.set.side_effect = RuntimeError("memcached unreachable")
    with pytest.raises(StorageError) as exc_info:
      b.store("key1", b"value")
    assert exc_info.value.operation == "store"
    assert exc_info.value.key == "key1"
    assert isinstance(exc_info.value.__cause__, RuntimeError)

  def test_retrieve_failure_raises_storage_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.side_effect = RuntimeError("memcached unreachable")
    with pytest.raises(StorageError) as exc_info:
      b.retrieve("key1")
    assert exc_info.value.operation == "retrieve"
    assert exc_info.value.key == "key1"

  def test_delete_failure_raises_storage_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.delete.side_effect = RuntimeError("memcached unreachable")
    with pytest.raises(StorageError) as exc_info:
      b.delete("key1")
    assert exc_info.value.operation == "delete"
    assert exc_info.value.key == "key1"

  def test_exists_failure_raises_storage_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.side_effect = RuntimeError("memcached unreachable")
    with pytest.raises(StorageError) as exc_info:
      b.exists("key1")
    assert exc_info.value.operation == "exists"
    assert exc_info.value.key == "key1"

  def test_clear_storage_failure_raises_storage_error(self, mocker) -> None:
    b = _make_backend(allow_flush_all=True)
    client = mocker.MagicMock()
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    b.connect()
    client.flush_all.side_effect = RuntimeError("memcached unreachable")
    with pytest.raises(StorageError) as exc_info:
      b.clear_storage()
    assert exc_info.value.operation == "clear_storage"

  def test_storage_error_is_backend_error_subclass(self, mocker) -> None:
    """``except BackendError`` must catch storage-path failures."""
    from scrapy_extension.exceptions.base import BackendError

    b, client = _connected(mocker)
    client.set.side_effect = RuntimeError("boom")
    with pytest.raises(BackendError):
      b.store("key1", b"value")
