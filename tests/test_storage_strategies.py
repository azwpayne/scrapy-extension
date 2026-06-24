"""Tests for storage-semantics strategies + factory (subsystem ③ Tier-2)."""

from __future__ import annotations

import threading

import pytest

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.storage.strategies import (
  BatchedStorageStrategy,
  PassthroughStorageStrategy,
  StorageStrategy,
  create_storage_strategy,
)


class TestPassthroughStorageStrategy:
  """Default strategy delegates straight through to the StorageBackend."""

  def test_store_delegates_one_to_one(self, mocker) -> None:
    backend = mocker.Mock()
    strat = PassthroughStorageStrategy()
    strat.store(backend, "k", b"v", ttl=10)
    backend.store.assert_called_once_with("k", b"v", ttl=10)

  def test_store_default_ttl_is_none(self, mocker) -> None:
    backend = mocker.Mock()
    strat = PassthroughStorageStrategy()
    strat.store(backend, "k", b"v")
    backend.store.assert_called_once_with("k", b"v", ttl=None)

  def test_store_byte_identical_to_direct_call(self, mocker) -> None:
    """Passthrough must pass the exact same (key, value, ttl) as a direct call."""
    backend = mocker.Mock()
    strat = PassthroughStorageStrategy()
    strat.store(backend, "items:a", b"\x00\x01\x02", ttl=300)
    direct = mocker.Mock()
    direct.store("items:a", b"\x00\x01\x02", ttl=300)
    assert backend.store.call_args == direct.store.call_args

  def test_flush_is_noop(self, mocker) -> None:
    backend = mocker.Mock()
    strat = PassthroughStorageStrategy()
    strat.flush()  # must not raise / must not touch backend
    backend.store.assert_not_called()

  def test_close_is_noop(self, mocker) -> None:
    backend = mocker.Mock()
    strat = PassthroughStorageStrategy()
    strat.close()
    backend.store.assert_not_called()


class TestBatchedStorageStrategy:
  """Buffers items, flushes at threshold, drains on close."""

  def test_under_threshold_no_store(self, mocker) -> None:
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=100)
    strat.store(backend, "k1", b"v1")
    strat.store(backend, "k2", b"v2")
    backend.store.assert_not_called()
    assert strat.pending == 2

  def test_flushes_when_threshold_reached(self, mocker) -> None:
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=3)
    strat.store(backend, "k1", b"v1")
    strat.store(backend, "k2", b"v2")
    strat.store(backend, "k3", b"v3")  # hits threshold -> auto-flush
    assert backend.store.call_count == 3
    assert strat.pending == 0

  def test_flush_preserves_order(self, mocker) -> None:
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=2)
    strat.store(backend, "k1", b"v1")
    strat.store(backend, "k2", b"v2")  # flush
    keys = [c.args[0] for c in backend.store.call_args_list]
    assert keys == ["k1", "k2"]

  def test_flush_passes_ttl(self, mocker) -> None:
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=1)
    strat.store(backend, "k", b"v", ttl=42)
    backend.store.assert_called_once_with("k", b"v", ttl=42)

  def test_manual_flush_writes_all_buffered(self, mocker) -> None:
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=100)
    strat.store(backend, "k1", b"v1")
    strat.store(backend, "k2", b"v2")
    strat.flush()
    assert backend.store.call_count == 2
    assert strat.pending == 0

  def test_close_flushes_remaining(self, mocker) -> None:
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=100)
    strat.store(backend, "k1", b"v1")
    strat.store(backend, "k2", b"v2")
    strat.close()
    assert backend.store.call_count == 2

  def test_close_after_auto_flush_no_extra_writes(self, mocker) -> None:
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=1)
    strat.store(backend, "k1", b"v1")  # flush
    strat.close()
    assert backend.store.call_count == 1

  def test_default_threshold_is_100(self) -> None:
    strat = BatchedStorageStrategy()
    assert strat.threshold == 100

  def test_invalid_threshold_raises(self) -> None:
    with pytest.raises(ValueError, match="threshold"):
      BatchedStorageStrategy(threshold=0)
    with pytest.raises(ValueError, match="threshold"):
      BatchedStorageStrategy(threshold=-5)

  def test_thread_safety_no_corruption(self, mocker) -> None:
    """Concurrent stores + flushes don't lose or duplicate items."""
    backend = mocker.Mock()

    # Make backend.store sleep briefly to widen the race window.
    def slow_store(key, data, ttl=None):  # noqa: ARG001
      pass

    backend.store.side_effect = slow_store

    strat = BatchedStorageStrategy(threshold=50)
    n_threads = 8
    per_thread = 20
    total = n_threads * per_thread

    def worker(tid: int) -> None:
      for i in range(per_thread):
        strat.store(backend, f"t{tid}-{i}", b"x")

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()
    strat.close()  # flush any remainder

    # Every item must be stored exactly once — no drops, no duplicates.
    assert backend.store.call_count == total
    keys = {c.args[0] for c in backend.store.call_args_list}
    assert len(keys) == total


class TestStorageStrategyFactory:
  def test_passthrough(self) -> None:
    strat = create_storage_strategy("passthrough")
    assert isinstance(strat, PassthroughStorageStrategy)

  def test_batched(self) -> None:
    strat = create_storage_strategy("batched", threshold=50)
    assert isinstance(strat, BatchedStorageStrategy)
    assert strat.threshold == 50

  def test_returns_strategy_subclass(self) -> None:
    assert isinstance(create_storage_strategy("passthrough"), StorageStrategy)
    assert isinstance(create_storage_strategy("batched"), StorageStrategy)

  def test_invalid_name_raises_configuration_error(self) -> None:
    with pytest.raises(ConfigurationError, match="Unknown storage strategy"):
      create_storage_strategy("bogus")

  def test_invalid_name_redacts_value(self) -> None:
    """ConfigurationError on an unknown strategy must not echo the raw value
    if the name were sensitive — and must surface a clear message regardless."""
    with pytest.raises(ConfigurationError) as exc_info:
      create_storage_strategy("bogus")
    assert exc_info.value.setting_name == "storage_strategy"
