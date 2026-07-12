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

  def test_close_joins_age_flusher(self, mocker) -> None:
    # close() must join the age-flusher thread so BackendPipeline.close_spider
    # cannot tear down the backend connection while the flusher is mid-store().
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=100, max_buffer_age_s=0.01)
    strat.store(backend, "k1", b"v1")  # triggers _ensure_flusher
    flusher = strat._flusher
    assert flusher is not None and flusher.is_alive()
    strat.close()
    assert not flusher.is_alive()

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


class TestBatchedStoragePartialFailure:
  """C4 — at-least-once flush: a mid-batch store failure must not silently
  drop the un-written tail (insight round-2, HIGH). The un-written items must
  remain buffered for the next flush, and the error must surface to the caller.
  """

  def test_partial_failure_keeps_tail_buffered_no_raise(self, mocker) -> None:
    """R-pipe-1 (option A): a mid-batch store failure must not silently drop
    the un-written tail (C4 at-least-once) AND must not propagate to the
    caller — the item is safely buffered. Pre-R-pipe-1: ``store()`` re-raised
    (C4 "surface the error"), which stormed the pipeline's ``max_storage_errors``
    breaker per item during sustained outages. Post-R-pipe-1: ``store()``
    swallows the threshold-flush exception; the un-written tail stays buffered.
    """
    backend = mocker.Mock()
    call_state = {"n": 0}

    def flaky_store(key, value, ttl=None):  # noqa: ARG001
      call_state["n"] += 1
      if call_state["n"] == 2:
        raise RuntimeError("backend down on item 2")

    backend.store.side_effect = flaky_store

    strat = BatchedStorageStrategy(threshold=3)
    strat.store(backend, "k1", b"v1")
    strat.store(backend, "k2", b"v2")
    # 3rd store hits threshold → triggers _flush_to → item-2 store raises
    # INSIDE _flush_to, which re-enqueues the tail; store() swallows it.
    strat.store(backend, "k3", b"v3")  # MUST NOT raise (R-pipe-1 option A)

    # Items 2 (raised mid-flush) + 3 (never attempted) stay buffered for retry
    # — NOT silently dropped (C4 at-least-once preserved).
    buffered_keys = [k for k, _v, _t in strat._buffer]
    assert "k2" in buffered_keys, (
      f"regression: item k2 lost on partial flush; buffer={buffered_keys}"
    )
    assert "k3" in buffered_keys, (
      f"regression: item k3 lost on partial flush; buffer={buffered_keys}"
    )
    # Item 1 was written; item 2 raised; backend.store called exactly twice.
    assert backend.store.call_count == 2

  def test_green_path_leaves_buffer_empty(self, mocker) -> None:
    """All stores succeed → buffer drained, no exception (regression guard)."""
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=3)
    strat.store(backend, "k1", b"v1")
    strat.store(backend, "k2", b"v2")
    strat.store(backend, "k3", b"v3")  # threshold → flush

    assert backend.store.call_count == 3
    assert strat.pending == 0
    assert strat._buffer == []

  def test_partial_failure_then_retry_flushes_tail(self, mocker) -> None:
    """After a partial flush, the buffered tail must be flushable on retry
    once the backend recovers (at-least-once is observable end-to-end).
    """
    backend = mocker.Mock()
    state = {"n": 0}

    def recover_store(key, value, ttl=None):  # noqa: ARG001
      state["n"] += 1
      # Raise on the FIRST flush (item 2 of the first batch); succeed after.
      if state["n"] == 2 and not getattr(recover_store, "_recovered", False):
        recover_store._recovered = True  # type: ignore[attr-defined]
        raise RuntimeError("transient item-2 failure")

    backend.store.side_effect = recover_store

    strat = BatchedStorageStrategy(threshold=3)
    strat.store(backend, "k1", b"v1")
    strat.store(backend, "k2", b"v2")
    # 3rd store hits threshold → flush fails mid-batch → tail re-enqueued.
    # R-pipe-1 option A: store() MUST NOT raise (item safely buffered).
    strat.store(backend, "k3", b"v3")

    # k3 buffered. Recover: a manual flush drains it.
    strat.flush()
    # k3 (and only k3 — k1/k2 already attempted) now written.
    written_keys = [c.args[0] for c in backend.store.call_args_list]
    assert "k3" in written_keys
    assert strat.pending == 0

  def test_threshold_flush_failure_does_not_storm_caller(self, mocker) -> None:
    """R-pipe-1 (option A): a threshold-flush failure must NOT propagate from
    ``store()``. The item is safely buffered (at-least-once; ``_flush_to``
    re-enqueued the tail and already logged the partial), so the caller must
    see ``store()`` succeed. Pre-fix: ``store()`` re-raised, so once the
    buffer saturated at threshold+ during a sustained outage, EVERY incoming
    item raised → ``BackendPipeline.process_item``'s ``max_storage_errors``
    counter stormed per item → ``BackendError`` killed the spider → buffered
    tail LOST (crash-before-flush) = data loss. The at-least-once buffer was
    defeated by the breaker meant to surface outages. Post-fix: ``store()``
    swallows the threshold-flush exception; escalation moves to the
    ``on_buffer_depth`` monitor hook.
    """
    backend = mocker.Mock()
    backend.store.side_effect = RuntimeError("backend down")
    strat = BatchedStorageStrategy(threshold=2)
    # Saturate: each store past the 2nd triggers a failing threshold-flush.
    # NONE may raise — the items are all safely buffered for retry.
    for i in range(10):
      strat.store(backend, f"k{i}", b"v")
    # All 10 items preserved in the buffer (at-least-once held); no storm.
    assert strat.pending == 10

  def test_explicit_flush_still_propagates_failure(self, mocker) -> None:
    """R-pipe-1 (option A): ``store()`` swallows threshold-flush failures (item
    buffered), BUT explicit :meth:`flush` still propagates — a caller explicitly
    draining must know the final flush failed (the items are buffered but the
    caller is tearing down). Preserves the C4 'surface the error' intent for
    the explicit-drain path while removing the per-item storm on the store path.
    """
    backend = mocker.Mock()
    backend.store.side_effect = RuntimeError("backend down")
    strat = BatchedStorageStrategy(threshold=100)
    strat.store(backend, "k1", b"v1")  # buffered; depth 1 < threshold; no raise
    with pytest.raises(RuntimeError, match="backend down"):
      strat.flush()  # explicit drain → MUST still propagate


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


class TestBatchedStorageRisk2:
  """Risk 2: monitor hook + age-based flusher + set_monitor wiring."""

  def test_on_buffer_depth_emits_after_store(self, mocker) -> None:
    """store() emits on_buffer_depth(depth) so operators can alert pre-flush."""
    from scrapy_extension.monitor.base import Monitor

    monitor = mocker.Mock(spec=Monitor)
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=10, monitor=monitor)
    strat.store(backend, "k", b"v")
    monitor.on_buffer_depth.assert_called_once_with(1)

  def test_set_monitor_injects_after_construction(self, mocker) -> None:
    """from_crawler wires the monitor post-construction via set_monitor."""
    from scrapy_extension.monitor.base import Monitor, NullMonitor

    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=10)  # NullMonitor default
    assert isinstance(strat._monitor, NullMonitor)
    monitor = mocker.Mock(spec=Monitor)
    strat.set_monitor(monitor)
    strat.store(backend, "k", b"v")
    monitor.on_buffer_depth.assert_called_once_with(1)

  def test_max_buffer_age_s_none_starts_no_flusher(self, mocker) -> None:
    """Disabled (None) → no background flusher thread (byte-identical to old)."""
    backend = mocker.Mock()
    strat = BatchedStorageStrategy(threshold=10)  # max_buffer_age_s=None
    strat.store(backend, "k", b"v")
    assert strat._flusher is None

  def test_max_buffer_age_s_starts_and_flushes(self, mocker) -> None:
    """Enabled → daemon thread flushes once the oldest item exceeds the age cap."""
    import time

    backend = mocker.Mock()
    # threshold high so only the age-flusher can fire; tiny age so the test
    # is fast. The daemon thread flushes once the oldest item exceeds age.
    strat = BatchedStorageStrategy(threshold=1000, max_buffer_age_s=0.01)
    strat.store(backend, "k", b"v")
    assert strat._flusher is not None  # age-flusher started
    # Give the daemon thread a window to wake + flush (15x the age cap).
    time.sleep(0.15)
    backend.store.assert_called_with("k", b"v", ttl=None)
    strat.close()  # stops the flusher cleanly
