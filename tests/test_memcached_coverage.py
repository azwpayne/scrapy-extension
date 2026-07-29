"""Error-path coverage for MemcachedBackend (≥98% coverage goal)."""

from __future__ import annotations

import logging
import sys

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


class _ExceptionContextProbe(logging.Handler):
  """Capture active exception state visible to synchronous log handlers."""

  def __init__(self) -> None:
    super().__init__(logging.DEBUG)
    self.records: list[logging.LogRecord] = []
    self.contexts: list[tuple[object | None, object | None, object | None]] = []

  def emit(self, record: logging.LogRecord) -> None:
    self.records.append(record)
    self.contexts.append(sys.exc_info())


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

  def test_disconnect_logs_suppressed_cleanup_after_exception_unwinds(
    self, mocker
  ) -> None:
    """A handler cannot inspect the ordinary close error via ``sys.exc_info``."""
    marker = "round48-memcached-close-marker"
    b, client = _connected(mocker)
    client.close.side_effect = RuntimeError(marker)
    probe = _ExceptionContextProbe()
    logger = memcached_mod.logger
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(probe)
    try:
      b.disconnect()
    finally:
      logger.removeHandler(probe)
      logger.setLevel(old_level)

    assert [record.getMessage() for record in probe.records] == [
      "Suppressed memcached cleanup error"
    ]
    assert probe.contexts == [(None, None, None)]
    assert marker not in repr(probe.records)

  def test_disconnect_ignores_diagnostic_interrupt_after_close_error(
    self, mocker
  ) -> None:
    """R105: normal best-effort disconnect survives failed diagnostics."""
    b, client = _connected(mocker)
    client.close.side_effect = RuntimeError("close failed")
    mocker.patch.object(memcached_mod.logger, "debug", side_effect=KeyboardInterrupt)

    b.disconnect()

    client.close.assert_called_once()
    assert b.is_connected() is False

  def test_disconnect_propagates_direct_close_control_exception(self, mocker) -> None:
    """R105: an actual close control exception remains observable."""
    b, client = _connected(mocker)
    primary = KeyboardInterrupt()
    client.close.side_effect = primary

    with pytest.raises(KeyboardInterrupt) as raised:
      b.disconnect()

    assert raised.value is primary
    assert b.is_connected() is False

  def test_stale_candidate_ignores_diagnostic_interrupt_after_close_error(
    self, mocker
  ) -> None:
    """R105: stale private-candidate cleanup stays best effort."""
    b = MemcachedBackend(MemcachedSettings())
    client = mocker.MagicMock()
    client.close.side_effect = RuntimeError("close failed")

    def stale_probe() -> dict[str, str]:
      b.disconnect()
      return {}

    client.stats.side_effect = stale_probe
    mocker.patch.object(memcached_mod, "MemcachedClient", return_value=client)
    mocker.patch.object(memcached_mod.logger, "debug", side_effect=KeyboardInterrupt)

    b.connect()

    client.close.assert_called_once()
    assert b.is_connected() is False

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
    assert sw.did_suppress is True
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
    assert exc_info.value.key is None

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
