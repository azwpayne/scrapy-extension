"""Tests for MemcachedBackend (subsystem ③) — mocked pymemcache.

Injects a stub ``pymemcache`` package into ``sys.modules`` so the backend's
module-level ``from pymemcache.client.base import Client`` succeeds without
the dependency installed, then patches the backend's captured
``MemcachedClient`` name to assert call patterns.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

# Stub the pymemcache package so the backend imports cleanly.
if "pymemcache" not in sys.modules:
  _pkg = types.ModuleType("pymemcache")
  _pkg_client = types.ModuleType("pymemcache.client")
  _pkg_base = types.ModuleType("pymemcache.client.base")
  _pkg_base.Client = MagicMock(name="MemcachedClient")
  sys.modules["pymemcache"] = _pkg
  sys.modules["pymemcache.client"] = _pkg_client
  sys.modules["pymemcache.client.base"] = _pkg_base


@pytest.fixture(scope="module", autouse=True)
def _cleanup_sys_modules_mock_pymemcache():
  """Pop the module-level ``pymemcache`` mock tree after this module's tests.

  R14-G flake fix: module-top-level ``sys.modules`` injection pollutes the
  session for later modules; pop all three injected keys at module teardown.
  """
  yield
  for key in ("pymemcache", "pymemcache.client", "pymemcache.client.base"):
    sys.modules.pop(key, None)

import scrapy_extension.backends.memcached as memcached_mod  # noqa: E402
from scrapy_extension.backends.base import (  # noqa: E402
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.memcached import MemcachedBackend  # noqa: E402
from scrapy_extension.exceptions import (  # noqa: E402
  BackendConnectionError,
  ConfigurationError,
)
from scrapy_extension.exceptions.base import StorageError  # noqa: E402
from scrapy_extension.settings import MemcachedMode, MemcachedSettings  # noqa: E402


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
    assert s.allow_flush_all is False


class TestMemcachedConnect:
  def test_unsupported_mode_is_configuration_error(self) -> None:
    b = _make_backend()
    b.config.mode = "unsupported"  # type: ignore[assignment]

    with pytest.raises(ConfigurationError) as exc_info:
      b.connect()

    assert exc_info.value.setting_name == "mode"

  def test_connect_creates_client_and_stats(self, mocker) -> None:
    b, client = _connected(mocker)
    memcached_mod.MemcachedClient.assert_called_once_with(("localhost", 11211))
    client.stats.assert_called_once()
    assert b.is_connected() is True

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


class TestMemcachedStorageOps:
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
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    b.connect()
    b.clear_storage()
    client.flush_all.assert_called_once()

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
