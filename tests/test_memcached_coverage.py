"""Error-path coverage for MemcachedBackend (≥98% coverage goal)."""

from __future__ import annotations

import pytest

import scrapy_extension.backends.memcached as memcached_mod
from scrapy_extension.backends.memcached import MemcachedBackend
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import MemcachedSettings


def _connected(mocker):
  b = MemcachedBackend(MemcachedSettings())
  client = mocker.MagicMock()
  client.set.return_value = True
  mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
  b.connect()
  return b, client


class TestMemcachedErrorPaths:
  def test_ping_failure(self, mocker) -> None:
    b, client = _connected(mocker)
    client.stats.side_effect = RuntimeError("down")
    assert b.ping() is False

  def test_ping_false_when_not_connected(self) -> None:
    """ping() returns False before connect() (client is None)."""
    from scrapy_extension.settings.memcached import MemcachedSettings

    b = memcached_mod.MemcachedBackend(MemcachedSettings())
    assert b.ping() is False

  def test_ping_true_when_stats_succeeds(self, mocker) -> None:
    """ping() returns True when stats() succeeds (the happy path)."""
    b, client = _connected(mocker)
    client.stats.return_value = {"stat_key": "stat_val"}
    assert b.ping() is True

  def test_disconnect_before_connect_is_noop(self) -> None:
    """disconnect() before connect() (client is None) is a safe no-op.

    Covers the False branch of ``disconnect``'s ``if self._client is not None``.
    """
    from scrapy_extension.settings.memcached import MemcachedSettings

    b = memcached_mod.MemcachedBackend(MemcachedSettings())
    b.disconnect()  # client is None — must not raise
    assert b._client is None

  def test_disconnect(self, mocker) -> None:
    b, client = _connected(mocker)
    b.disconnect()
    client.close.assert_called_once()

  def test_disconnect_swallows_close_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.close.side_effect = RuntimeError("close failed")
    b.disconnect()  # _swallow catches; must not raise

  def test_swallow_does_not_suppress_base_exception(self) -> None:
    """R-swallow: _swallow must NOT suppress BaseException (Ctrl+C / SystemExit).

    Pre-fix ``__exit__`` returned True for any non-None ``exc_type``, so a
    ``KeyboardInterrupt`` raised inside a ``with _swallow():`` cleanup block was
    trapped -- the operator's shutdown signal disappeared into a debug log. Now
    only regular Exceptions are suppressed; BaseException propagates.
    """
    from scrapy_extension.backends.memcached import _swallow

    sw = _swallow()
    sw.__enter__()
    # Regular Exception is suppressed (returns True).
    assert sw.__exit__(RuntimeError, RuntimeError("cleanup"), None) is True
    # BaseException (KeyboardInterrupt) is NOT suppressed (returns False).
    assert sw.__exit__(KeyboardInterrupt, KeyboardInterrupt(), None) is False
    # No exception (exc_type None) -> False (normal exit, propagate nothing).
    assert sw.__exit__(None, None, None) is False

  def test_store_raises_storage_error(self, mocker) -> None:
    # R14-A: storage ops raise StorageError instead of silently swallowing.
    b, client = _connected(mocker)
    client.set.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError) as exc_info:
      b.store("k", b"v")
    assert exc_info.value.operation == "store"
    assert exc_info.value.key == "k"

  def test_retrieve_raises_storage_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError) as exc_info:
      b.retrieve("k")
    assert exc_info.value.operation == "retrieve"

  def test_delete_raises_storage_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.delete.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError):
      b.delete("k")

  def test_exists_raises_storage_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError):
      b.exists("k")

  def test_clear_raises_storage_error(self, mocker) -> None:
    b = MemcachedBackend(MemcachedSettings(allow_flush_all=True))
    client = mocker.MagicMock()
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    b.connect()
    client.flush_all.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError):
      b.clear_storage()

  def test_retrieve_without_client_raises_storage_error(self, mocker) -> None:
    """Disconnected storage ops raise StorageError (no silent None)."""
    b = MemcachedBackend(MemcachedSettings())
    with pytest.raises(StorageError):
      b.retrieve("k")
