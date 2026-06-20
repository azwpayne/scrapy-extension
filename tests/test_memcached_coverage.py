"""Error-path coverage for MemcachedBackend (≥98% coverage goal)."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

if "pymemcache" not in sys.modules:
  _pkg = types.ModuleType("pymemcache")
  _pkg_client = types.ModuleType("pymemcache.client")
  _pkg_base = types.ModuleType("pymemcache.client.base")
  _pkg_base.Client = MagicMock(name="MemcachedClient")
  sys.modules["pymemcache"] = _pkg
  sys.modules["pymemcache.client"] = _pkg_client
  sys.modules["pymemcache.client.base"] = _pkg_base

import scrapy_extension.backends.memcached as memcached_mod  # noqa: E402
from scrapy_extension.backends.memcached import MemcachedBackend  # noqa: E402
from scrapy_extension.settings import MemcachedSettings  # noqa: E402


def _connected(mocker):
  b = MemcachedBackend(MemcachedSettings())
  client = mocker.MagicMock()
  mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
  b.connect()
  return b, client


class TestMemcachedErrorPaths:
  def test_ping_failure(self, mocker) -> None:
    b, client = _connected(mocker)
    client.stats.side_effect = RuntimeError("down")
    assert b.ping() is False

  def test_disconnect(self, mocker) -> None:
    b, client = _connected(mocker)
    b.disconnect()
    client.close.assert_called_once()

  def test_disconnect_swallows_close_error(self, mocker) -> None:
    b, client = _connected(mocker)
    client.close.side_effect = RuntimeError("close failed")
    b.disconnect()  # _swallow catches; must not raise

  def test_store_swallows_exception(self, mocker) -> None:
    b, client = _connected(mocker)
    client.set.side_effect = RuntimeError("boom")
    b.store("k", b"v")

  def test_retrieve_swallows_exception(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.side_effect = RuntimeError("boom")
    assert b.retrieve("k") is None

  def test_delete_swallows_exception(self, mocker) -> None:
    b, client = _connected(mocker)
    client.delete.side_effect = RuntimeError("boom")
    assert b.delete("k") is False

  def test_exists_swallows_exception(self, mocker) -> None:
    b, client = _connected(mocker)
    client.get.side_effect = RuntimeError("boom")
    assert b.exists("k") is False

  def test_clear_swallows_exception(self, mocker) -> None:
    b, client = _connected(mocker)
    client.flush_all.side_effect = RuntimeError("boom")
    b.clear_storage()

  def test_retrieve_returns_none_without_client(self, mocker) -> None:
    """is_connected False path: ops without connect return safely."""
    b = MemcachedBackend(MemcachedSettings())
    assert b.retrieve("k") is None
