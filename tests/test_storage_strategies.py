"""Tests for storage-semantics strategies + factory (subsystem ③ Tier-2)."""

from __future__ import annotations

import logging
import sys
import threading
import time

import pytest

from scrapy_extension.exceptions import (
    ConfigurationError,
    StorageBackpressureError,
    StorageError,
)
from scrapy_extension.storage.strategies import (
    BatchedStorageStrategy,
    PassthroughStorageStrategy,
    StorageStrategy,
    create_storage_strategy,
)
from scrapy_extension.storage.strategies import batched as batched_module


class _ExceptionContextHandler(logging.Handler):
    """Capture the interpreter exception state visible to a log handler."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []
        self.active_exceptions: list[tuple[object, object, object]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.active_exceptions.append(sys.exc_info())


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

    @pytest.mark.parametrize(
        "diagnostic_error",
        [
            RuntimeError("logging failed"),
            KeyboardInterrupt("logging interrupted"),
            SystemExit("logging exited"),
        ],
    )
    def test_flush_lock_timeout_diagnostic_cannot_raise(
        self, mocker, diagnostic_error: BaseException
    ) -> None:
        """R120: a lock-timeout warning cannot change the skip-and-return result."""
        strat = BatchedStorageStrategy()
        strat._flush_lock = mocker.Mock()
        strat._flush_lock.acquire.return_value = False
        warning = mocker.patch.object(
            batched_module.logger,
            "warning",
            side_effect=diagnostic_error,
        )

        strat.flush()

        strat._flush_lock.acquire.assert_called_once_with(
            timeout=batched_module._FLUSH_LOCK_TIMEOUT_S
        )
        warning.assert_called_once()

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

    def test_close_join_keyboardinterrupt_still_drains_then_reraises(
        self, mocker
    ) -> None:
        """R75: an interrupted age-worker join cannot skip the final drain."""
        backend = mocker.Mock()
        strat = BatchedStorageStrategy(threshold=100)
        strat.store(backend, "k1", b"v1")

        class _InterruptedFlusher:
            def is_alive(self) -> bool:
                return True

            def join(self, timeout: float | None = None) -> None:
                raise KeyboardInterrupt("interrupted join")

        strat._flusher = _InterruptedFlusher()

        with pytest.raises(KeyboardInterrupt, match="interrupted join"):
            strat.close()

        backend.store.assert_called_once_with("k1", b"v1", ttl=None)
        assert strat.pending == 0

    def test_close_retries_requeued_tail_after_transient_store_failure(self, mocker):
        """R74: close() must retry the re-enqueued tail once after a transient
        mid-drain store Exception, so at-least-once holds at the final drain.
        Pre-fix: _flush_serialized re-enqueues items 24..49 then re-raises;
        close() captures+re-raises WITHOUT retrying, so the pipeline closing the
        backend strands ~25 buffered items (silent loss)."""

        backend = mocker.Mock()
        calls = {"n": 0}

        def transient_store(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 25:
                raise RuntimeError("transient blip")

        backend.store.side_effect = transient_store
        strat = BatchedStorageStrategy(threshold=100)
        for i in range(50):
            strat.store(backend, f"k{i}", b"v")

        strat.close()  # pre-fix: raises RuntimeError after 25 calls; post-fix: recovers

        stored_keys = {c.args[0] for c in backend.store.call_args_list}
        assert stored_keys == {f"k{i}" for i in range(50)}
        assert strat.pending == 0

    def test_close_does_not_retry_control_signal(self, mocker) -> None:
        """R74 no-regression: a control BaseException (KeyboardInterrupt) from
        the drain is NOT retried (only ordinary store Exceptions retry) and IS
        re-raised -- the operator's signal is preserved."""

        backend = mocker.Mock()
        calls = {"n": 0}

        def interrupting_store(*args, **kwargs):
            calls["n"] += 1
            raise KeyboardInterrupt("ctrl-c during store")

        backend.store.side_effect = interrupting_store
        strat = BatchedStorageStrategy(threshold=100)
        strat.store(backend, "k1", b"v1")
        strat.store(backend, "k2", b"v2")

        with pytest.raises(KeyboardInterrupt, match="ctrl-c during store"):
            strat.close()

        # one drain only (no retry): the KI bypassed the inner except Exception
        assert calls["n"] == 1

    def test_close_waits_for_inflight_threshold_flush(self, mocker) -> None:
        backend = mocker.Mock()
        store_entered = threading.Event()
        release_store = threading.Event()
        store_errors: list[Exception] = []
        close_errors: list[Exception] = []

        def blocking_store(*_args, **_kwargs):
            store_entered.set()
            if not release_store.wait(timeout=2.0):
                raise AssertionError("backend store was not released")

        backend.store.side_effect = blocking_store
        strat = BatchedStorageStrategy(threshold=1)

        def run_store() -> None:
            try:
                strat.store(backend, "k", b"v")
            except Exception as exc:
                store_errors.append(exc)

        def run_close() -> None:
            try:
                strat.close()
            except Exception as exc:
                close_errors.append(exc)

        store_thread = threading.Thread(target=run_store, daemon=True)
        close_thread = threading.Thread(target=run_close, daemon=True)
        store_thread.start()
        assert store_entered.wait(timeout=2.0)
        close_thread.start()

        try:
            close_thread.join(timeout=0.1)
            assert close_thread.is_alive(), (
                "close returned while threshold flush was active"
            )
        finally:
            release_store.set()
            store_thread.join(timeout=2.0)
            close_thread.join(timeout=2.0)

        assert not store_thread.is_alive()
        assert not close_thread.is_alive()
        assert store_errors == []
        assert close_errors == []

    def test_close_drains_tail_after_slow_threshold_flush_releases(
        self, monkeypatch, mocker
    ) -> None:
        """R75: close waits for a non-flusher holder before its final drain.

        The first threshold flush owns ``_flush_lock`` while writing ``k1``. A
        concurrent store appends ``k2`` after that snapshot, then close begins.
        Public ``flush()`` would give up at its short lock timeout and leave ``k2``
        buffered; close must wait through its shutdown deadline and drain the tail
        immediately after the holder releases.
        """
        monkeypatch.setattr(batched_module, "_FLUSH_LOCK_TIMEOUT_S", 0.05)
        monkeypatch.setattr(batched_module, "_CLOSE_DRAIN_DEADLINE_S", 1.0)

        backend = mocker.Mock()
        first_store_entered = threading.Event()
        release_first_store = threading.Event()

        def slow_first_store(key: str, *_args, **_kwargs) -> None:
            if key == "k1":
                first_store_entered.set()
                assert release_first_store.wait(timeout=2.0), (
                    "first flush was not released"
                )

        backend.store.side_effect = slow_first_store
        strat = BatchedStorageStrategy(threshold=1)
        flush_thread = threading.Thread(
            target=strat.store,
            args=(backend, "k1", b"v1"),
            daemon=True,
        )
        flush_thread.start()
        assert first_store_entered.wait(timeout=2.0), "threshold flush did not start"

        # The first flush detached k1, so k2 remains the close-only tail.
        strat.store(backend, "k2", b"v2")
        assert strat.pending == 2
        assert strat._in_flight_count == 1
        close_done = threading.Event()
        close_errors: list[BaseException] = []

        def run_close() -> None:
            try:
                strat.close()
            except BaseException as error:  # noqa: BLE001 - capture control failures
                close_errors.append(error)
            finally:
                close_done.set()

        close_thread = threading.Thread(target=run_close, daemon=True)
        close_thread.start()
        # Prove close is waiting for the general flush holder, not only a flusher.
        time.sleep(0.15)
        assert not close_done.is_set(), "close used public flush's short timeout"

        release_first_store.set()
        flush_thread.join(timeout=2.0)
        assert not flush_thread.is_alive()
        assert close_done.wait(timeout=2.0), "close did not drain after flush release"
        close_thread.join(timeout=2.0)

        assert close_errors == []
        assert [call.args[0] for call in backend.store.call_args_list] == ["k1", "k2"]
        assert strat.pending == 0
        assert strat._in_flight_count == 0

    def test_close_drains_buffer_when_flusher_completes_after_old_join_window(
        self, monkeypatch, mocker
    ) -> None:
        """R23-A: close() must drain buffered items when the age-flusher is
        mid-flush (holding _flush_lock) and completes shortly AFTER the pre-R23
        fixed join window would have expired.

        Pre-R23 close() did a single ``join(5.0)`` then ``flush()`` with a bounded
        ``_flush_lock`` acquire; if the flusher still held the lock when that
        acquire fired, the flush SKIPPED and items still in ``_buffer`` were
        abandoned (a data-loss regression vs the pre-R22-B blocking acquire, real
        for slow-but-healthy cross-region backends whose flush exceeds 10s). R23
        loops the join to a hard ``_CLOSE_DRAIN_DEADLINE_S`` so a progressing flush
        completes and releases ``_flush_lock`` before the final drain.

        The original >10s timing is impractical to reproduce in a unit test, so
        this guard uses a stub flusher (``join`` returns instantly; ``is_alive``
        flips on an event) + a manually-held ``_flush_lock``. Against the pre-R23
        close() the stub's instant ``join`` + held lock make the bounded acquire
        skip immediately -> both buffered items are lost (call_count == 0). Against
        R23's drain loop, close() waits; once the stub "completes" (lock released +
        is_alive False) the final flush() drains both items (call_count == 2).
        """
        monkeypatch.setattr(batched_module, "_FLUSH_LOCK_TIMEOUT_S", 0.05)
        monkeypatch.setattr(batched_module, "_CLOSE_DRAIN_DEADLINE_S", 1.0)

        backend = mocker.Mock()
        # threshold=100 + no max_buffer_age_s -> no real age-flusher is started;
        # we install a stub below to stand in for a flusher mid-store().
        strat = BatchedStorageStrategy(threshold=100)
        strat.store(backend, "k1", b"v1")
        strat.store(backend, "k2", b"v2")

        flusher_completed = threading.Event()

        class _StubFlusher:
            """Stands in for the age-flusher blocked mid-store() holding _flush_lock."""

            def is_alive(self) -> bool:
                return not flusher_completed.is_set()

            def join(self, timeout: float | None = None) -> None:
                # Faithfully model Thread.join blocking semantics: block up to timeout
                # for the flusher to complete, so the drain loop iterates on is_alive()
                # rather than busy-spinning on an instant-return stub.
                flusher_completed.wait(timeout=timeout)

        strat._flusher = _StubFlusher()
        # Simulate the flusher mid-store(): it holds _flush_lock.
        strat._flush_lock.acquire()

        close_done = threading.Event()
        close_errors: list[Exception] = []

        def run_close() -> None:
            try:
                strat.close()
            except Exception as exc:  # noqa: BLE001 - record any failure
                close_errors.append(exc)
            finally:
                close_done.set()

        close_thread = threading.Thread(target=run_close, daemon=True)
        close_thread.start()

        # Let close() reach its drain wait. (Pre-R23 close() returns here almost
        # instantly: stub join() returns, the bounded _flush_lock acquire times
        # out at 0.05s and skips.)
        time.sleep(0.15)
        # The flusher now "completes": release the lock and mark it dead. R23's
        # drain loop is still waiting (deadline 1.0s) -> it sees the flusher dead,
        # exits the loop, and the final flush() acquires the now-free lock and
        # drains both buffered items.
        strat._flush_lock.release()
        flusher_completed.set()

        assert close_done.wait(timeout=2.0), "close() did not return"
        close_thread.join(timeout=2.0)
        assert close_errors == []
        assert backend.store.call_count == 2, (
            f"expected both buffered items drained, got {backend.store.call_count}"
        )

    def test_store_after_close_is_rejected(self, mocker) -> None:
        backend = mocker.Mock()
        strat = BatchedStorageStrategy(threshold=10)
        strat.close()

        with pytest.raises(RuntimeError, match="closed"):
            strat.store(backend, "late", b"value")

        backend.store.assert_not_called()
        assert strat.pending == 0

    def test_default_threshold_is_100(self) -> None:
        strat = BatchedStorageStrategy()
        assert strat.threshold == 100

    def test_default_max_pending_is_twice_the_threshold(self) -> None:
        strat = BatchedStorageStrategy(threshold=7)
        assert strat.max_pending == 14

    def test_invalid_threshold_raises(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            BatchedStorageStrategy(threshold=0)
        with pytest.raises(ValueError, match="threshold"):
            BatchedStorageStrategy(threshold=-5)

    @pytest.mark.parametrize("age", [0.0, -0.1])
    def test_nonpositive_max_buffer_age_raises(self, age: float) -> None:
        with pytest.raises(ValueError, match="max_buffer_age_s must be > 0"):
            BatchedStorageStrategy(max_buffer_age_s=age)

    def test_nan_max_buffer_age_raises(self) -> None:
        """R21-D: NaN bypasses the '<= 0' guard (nan <= 0 is False) and would make
        the age-flusher never fire + hot-spin on wait(timeout=nan)."""
        with pytest.raises(ValueError, match="max_buffer_age_s"):
            BatchedStorageStrategy(max_buffer_age_s=float("nan"))

    def test_nan_threshold_raises(self) -> None:
        """R21-D: NaN bypasses the '< 1' guard (nan < 1 is False) — same isfinite
        discipline for consistency."""
        with pytest.raises(ValueError, match="threshold"):
            BatchedStorageStrategy(threshold=float("nan"))

    @pytest.mark.parametrize("max_pending", [True, 1.5, "2", 0, -1])
    def test_invalid_max_pending_raises(self, max_pending: object) -> None:
        with pytest.raises(ValueError, match="max_pending"):
            BatchedStorageStrategy(threshold=2, max_pending=max_pending)  # type: ignore[arg-type]

    def test_max_pending_must_cover_one_complete_batch(self) -> None:
        with pytest.raises(ValueError, match="max_pending must be >= threshold"):
            BatchedStorageStrategy(threshold=3, max_pending=2)

    def test_hard_backpressure_counts_inflight_and_rejects_without_lock_wait(
        self, monkeypatch, mocker
    ) -> None:
        """A blocked flush cannot hide its detached snapshot from admission.

        The second item is buffered while the first is blocked in backend I/O.
        The third reaches the total cap and must return immediately even after the
        normal flush-lock timeout is made deliberately large.
        """
        from scrapy_extension.monitor.base import Monitor

        monkeypatch.setattr(batched_module, "_FLUSH_LOCK_TIMEOUT_S", 0.01)
        backend = mocker.Mock()
        monitor = mocker.Mock(spec=Monitor)
        first_store_entered = threading.Event()
        release_first_store = threading.Event()
        first_errors: list[BaseException] = []

        def blocking_store(key: str, *_args, **_kwargs) -> None:
            if key == "first":
                first_store_entered.set()
                assert release_first_store.wait(timeout=2.0), (
                    "blocked write was not released"
                )

        backend.store.side_effect = blocking_store
        strat = BatchedStorageStrategy(threshold=1, max_pending=2, monitor=monitor)

        def store_first() -> None:
            try:
                strat.store(backend, "first", b"one")
            except BaseException as error:  # noqa: BLE001 - assert after release
                first_errors.append(error)

        first_thread = threading.Thread(target=store_first, daemon=True)
        first_thread.start()
        assert first_store_entered.wait(timeout=2.0), "first flush did not start"
        assert strat.pending == 1

        # This accepted item may make a bounded, unsuccessful flush attempt while
        # the first snapshot owns the serialization lock.
        strat.store(backend, "second", b"two")
        assert strat.pending == 2
        assert monitor.on_buffer_depth.call_args_list[-1].args == (2,)

        # A full strategy must not wait for this intentionally huge lock timeout.
        monkeypatch.setattr(batched_module, "_FLUSH_LOCK_TIMEOUT_S", 10.0)
        rejected_done = threading.Event()
        rejected: list[StorageBackpressureError] = []

        def reject_third() -> None:
            try:
                strat.store(backend, "sensitive-third-key", b"sensitive-third-value")
            except StorageBackpressureError as error:
                rejected.append(error)
            finally:
                rejected_done.set()

        rejected_thread = threading.Thread(target=reject_third, daemon=True)
        rejected_thread.start()
        try:
            assert rejected_done.wait(timeout=0.5), (
                "full admission waited on flush lock"
            )
            assert len(rejected) == 1
            error = rejected[0]
            assert str(error) == "Batched storage is at capacity."
            assert error.operation == "store"
            assert error.key is None
            assert "sensitive-third" not in str(error)
            assert strat.pending == 2
        finally:
            release_first_store.set()
            first_thread.join(timeout=2.0)
            rejected_thread.join(timeout=2.0)

        assert not first_thread.is_alive()
        assert first_errors == []
        # The durable first record frees one slot; the second remains buffered.
        assert strat.pending == 1
        strat.flush()
        assert [call.args[0] for call in backend.store.call_args_list] == [
            "first",
            "second",
        ]
        assert strat.pending == 0

    def test_failed_snapshot_tail_requeues_without_leaking_inflight_capacity(
        self, mocker
    ) -> None:
        attempts: list[str] = []
        failed = False
        backend = mocker.Mock()

        def fail_once(key: str, *_args, **_kwargs) -> None:
            nonlocal failed
            attempts.append(key)
            if key == "second" and not failed:
                failed = True
                raise RuntimeError("backend down")

        backend.store.side_effect = fail_once
        strat = BatchedStorageStrategy(threshold=2, max_pending=2)
        strat.store(backend, "first", b"one")
        with pytest.raises(RuntimeError, match="backend down"):
            strat.store(backend, "second", b"two")

        assert strat.pending == 1
        assert strat._in_flight_count == 0

        # The retry tail consumes one slot, not two: the replacement admission is
        # accepted and flushes both records in FIFO order after recovery.
        strat.store(backend, "third", b"three")
        assert attempts == ["first", "second", "second", "third"]
        assert strat.pending == 0

    def test_thread_safety_no_corruption(self, mocker) -> None:
        """Concurrent stores + flushes don't lose or duplicate items."""
        backend = mocker.Mock()

        # Make backend.store sleep briefly to widen the race window.
        def slow_store(key, data, ttl=None):  # noqa: ARG001
            pass

        backend.store.side_effect = slow_store

        n_threads = 8
        per_thread = 20
        total = n_threads * per_thread
        strat = BatchedStorageStrategy(threshold=50, max_pending=total)

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


class TestBatchedStorageBackendAffinity:
    """Every buffered item remains bound to the backend accepted with it."""

    @pytest.mark.parametrize("drain", ["threshold", "flush", "close"])
    def test_mixed_backends_keep_global_order_across_drains(
        self, mocker, drain
    ) -> None:
        trace: list[tuple[str, str, bytes, int | None]] = []
        backend_a = mocker.Mock()
        backend_b = mocker.Mock()
        backend_a.store.side_effect = lambda key, value, ttl=None: trace.append(
            ("a", key, value, ttl)
        )
        backend_b.store.side_effect = lambda key, value, ttl=None: trace.append(
            ("b", key, value, ttl)
        )
        threshold = 2 if drain == "threshold" else 100
        strat = BatchedStorageStrategy(threshold=threshold)

        strat.store(backend_a, "a1", b"A", ttl=11)
        strat.store(backend_b, "b1", b"B", ttl=22)
        if drain == "flush":
            strat.flush()
        elif drain == "close":
            strat.close()

        assert trace == [
            ("a", "a1", b"A", 11),
            ("b", "b1", b"B", 22),
        ]
        assert strat.pending == 0

    def test_age_worker_flushes_each_item_to_its_backend(self, mocker) -> None:
        trace: list[tuple[str, str]] = []
        trace_lock = threading.Lock()
        drained = threading.Event()
        clock = {"now": 0.0}

        def record(owner: str):
            def store(key, _value, ttl=None):  # noqa: ARG001
                with trace_lock:
                    trace.append((owner, key))
                    if len(trace) == 2:
                        drained.set()

            return store

        backend_a = mocker.Mock()
        backend_b = mocker.Mock()
        backend_a.store.side_effect = record("a")
        backend_b.store.side_effect = record("b")
        mocker.patch(
            "scrapy_extension.storage.strategies.batched.time.monotonic",
            side_effect=lambda: clock["now"],
        )
        strat = BatchedStorageStrategy(threshold=100, max_buffer_age_s=0.01)

        try:
            strat.store(backend_a, "a1", b"A")
            strat.store(backend_b, "b1", b"B")
            clock["now"] = 1.0
            assert drained.wait(timeout=1.0), "age worker did not drain both entries"
        finally:
            strat.close()

        assert trace == [("a", "a1"), ("b", "b1")]

    def test_equal_distinct_backends_are_not_grouped_or_reordered(self, mocker) -> None:
        trace: list[tuple[str, str]] = []
        backend_a = mocker.MagicMock()
        backend_b = mocker.MagicMock()
        backend_a.__eq__.return_value = True
        backend_b.__eq__.return_value = True
        backend_a.store.side_effect = lambda key, _value, ttl=None: trace.append(
            ("a", key)
        )
        backend_b.store.side_effect = lambda key, _value, ttl=None: trace.append(
            ("b", key)
        )
        strat = BatchedStorageStrategy(threshold=3)

        assert backend_a is not backend_b
        assert backend_a == backend_b
        strat.store(backend_a, "a1", b"A")
        strat.store(backend_b, "b1", b"B")
        strat.store(backend_a, "a2", b"A")

        assert trace == [("a", "a1"), ("b", "b1"), ("a", "a2")]
        assert strat.pending == 0

    def test_partial_failure_retries_tail_via_original_backends(self, mocker) -> None:
        trace: list[tuple[str, str]] = []
        failed = False

        def record(owner: str):
            def store(key, _value, ttl=None):  # noqa: ARG001
                nonlocal failed
                trace.append((owner, key))
                if key == "b1" and not failed:
                    failed = True
                    raise RuntimeError("transient b1 failure")

            return store

        backend_a = mocker.Mock()
        backend_b = mocker.Mock()
        backend_c = mocker.Mock()
        backend_d = mocker.Mock()
        backend_a.store.side_effect = record("a")
        backend_b.store.side_effect = record("b")
        backend_c.store.side_effect = record("c")
        backend_d.store.side_effect = record("d")
        strat = BatchedStorageStrategy(threshold=3)

        strat.store(backend_a, "a1", b"A")
        strat.store(backend_b, "b1", b"B")
        with pytest.raises(RuntimeError, match="transient b1 failure"):
            strat.store(backend_c, "c1", b"C")

        assert trace == [("a", "a1"), ("b", "b1")]
        assert strat.pending == 2

        # A third caller reaches the threshold while the retry tail is pending. It
        # may trigger the drain, but it must not take ownership of the older items.
        strat.store(backend_d, "d1", b"D")

        assert trace == [
            ("a", "a1"),
            ("b", "b1"),
            ("b", "b1"),
            ("c", "c1"),
            ("d", "d1"),
        ]
        assert strat.pending == 0

    def test_failed_tail_stays_ahead_of_concurrent_new_backend(self, mocker) -> None:
        trace: list[tuple[str, str]] = []
        first_store_entered = threading.Event()
        release_first_store = threading.Event()
        failed = False
        flush_errors: list[Exception] = []

        def record(owner: str):
            def store(key, _value, ttl=None):  # noqa: ARG001
                nonlocal failed
                trace.append((owner, key))
                if key == "a1":
                    first_store_entered.set()
                    if not release_first_store.wait(timeout=2.0):
                        raise AssertionError("blocked backend store was not released")
                if key == "b1" and not failed:
                    failed = True
                    raise RuntimeError("transient b1 failure")

            return store

        backend_a = mocker.Mock()
        backend_b = mocker.Mock()
        backend_c = mocker.Mock()
        backend_a.store.side_effect = record("a")
        backend_b.store.side_effect = record("b")
        backend_c.store.side_effect = record("c")
        strat = BatchedStorageStrategy(threshold=2)
        strat.store(backend_a, "a1", b"A")

        def trigger_flush() -> None:
            try:
                strat.store(backend_b, "b1", b"B")
            except Exception as exc:
                flush_errors.append(exc)

        flush_thread = threading.Thread(target=trigger_flush, daemon=True)
        flush_thread.start()
        assert first_store_entered.wait(timeout=2.0)
        strat.store(backend_c, "c1", b"C")
        release_first_store.set()
        flush_thread.join(timeout=2.0)

        assert not flush_thread.is_alive()
        assert len(flush_errors) == 1
        assert isinstance(flush_errors[0], RuntimeError)
        assert strat.pending == 2

        strat.flush()

        assert trace == [
            ("a", "a1"),
            ("b", "b1"),
            ("b", "b1"),
            ("c", "c1"),
        ]
        assert strat.pending == 0


class TestBatchedStoragePartialFailure:
    """C4 — at-least-once flush: a mid-batch store failure must not silently
    drop the un-written tail (insight round-2, HIGH). The un-written items must
    remain buffered for the next flush, and the error must surface to the caller.
    """

    def test_partial_failure_keeps_tail_buffered_and_raises(self, mocker) -> None:
        """A threshold flush retains its retry tail and reports persistence failure."""
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
        # 3rd store hits threshold; item-2 fails and the unwritten tail is retained.
        with pytest.raises(RuntimeError, match="backend down on item 2"):
            strat.store(backend, "k3", b"v3")

        # Item 2 (outcome unknown after the exception) + item 3 (never attempted)
        # stay buffered for retry — NOT silently dropped (C4 at-least-once).
        assert strat.pending == 2
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
        # 3rd store hits threshold; the failure surfaces after tail re-enqueue.
        with pytest.raises(RuntimeError, match="transient item-2 failure"):
            strat.store(backend, "k3", b"v3")

        # k2 (unknown outcome) and k3 (unattempted) are buffered. Recover: a manual
        # flush retries both in their original order.
        assert strat.pending == 2
        strat.flush()
        written_keys = [c.args[0] for c in backend.store.call_args_list]
        assert written_keys == ["k1", "k2", "k2", "k3"]
        assert strat.pending == 0

    def test_keyboard_interrupt_requeues_unwritten_tail_before_reraising(
        self, mocker
    ) -> None:
        """A control signal must preserve the same at-least-once retry tail."""
        backend = mocker.Mock()
        attempts: list[str] = []
        interrupted = False

        def interrupt_second_store(key, value, ttl=None):  # noqa: ARG001
            nonlocal interrupted
            attempts.append(key)
            if key == "k2" and not interrupted:
                interrupted = True
                raise KeyboardInterrupt("stop after item 1")

        backend.store.side_effect = interrupt_second_store
        strat = BatchedStorageStrategy(threshold=3)
        strat.store(backend, "k1", b"v1")
        strat.store(backend, "k2", b"v2")

        with pytest.raises(KeyboardInterrupt, match="stop after item 1"):
            strat.store(backend, "k3", b"v3")

        assert strat.pending == 2

        strat.flush()

        assert attempts == ["k1", "k2", "k2", "k3"]
        assert strat.pending == 0

    def test_on_store_keyboard_interrupt_requeues_only_unreported_tail(
        self, mocker
    ) -> None:
        """A post-write control signal must retain the exact unreported tail.

        ``k2`` has already reached durable storage when its monitor callback is
        interrupted, so retrying it would create an avoidable duplicate.  ``k3``
        has not been attempted and must remain retryable with its original backend
        capability.
        """
        backend = mocker.Mock()
        monitor = mocker.Mock()

        def interrupt_second_notification(key: str) -> None:
            if key == "k2":
                raise KeyboardInterrupt("stop after k2 persisted")

        monitor.on_store.side_effect = interrupt_second_notification
        strat = BatchedStorageStrategy(threshold=3, monitor=monitor)
        strat.store(backend, "k1", b"v1")
        strat.store(backend, "k2", b"v2")

        with pytest.raises(KeyboardInterrupt, match="stop after k2 persisted"):
            strat.store(backend, "k3", b"v3")

        assert strat.pending == 1
        assert strat._in_flight_count == 0
        tail_backend, tail_key, tail_value, tail_ttl = strat._buffer[0]
        assert tail_backend is backend
        assert (tail_key, tail_value, tail_ttl) == ("k3", b"v3", None)

        monitor.on_store.side_effect = None
        strat.flush()

        assert [call.args[0] for call in backend.store.call_args_list] == [
            "k1",
            "k2",
            "k3",
        ]
        assert strat.pending == 0

    def test_on_store_error_and_debug_control_error_leave_batch_durable(
        self, mocker
    ) -> None:
        """An ordinary monitor failure cannot stop later writes, even if logging fails."""
        backend = mocker.Mock()
        monitor = mocker.Mock()
        monitor.on_store.side_effect = [RuntimeError("monitor down"), None]
        debug = mocker.patch.object(
            batched_module.logger,
            "debug",
            side_effect=KeyboardInterrupt("debug handler interrupted"),
        )
        strat = BatchedStorageStrategy(threshold=2, monitor=monitor)

        strat.store(backend, "k1", b"v1")
        strat.store(backend, "k2", b"v2")

        assert [call.args[0] for call in backend.store.call_args_list] == ["k1", "k2"]
        assert [call.args[0] for call in monitor.on_store.call_args_list] == [
            "k1",
            "k2",
        ]
        debug.assert_called_once_with("on_store hook raised")
        assert strat.pending == 0
        assert strat._in_flight_count == 0

    def test_final_on_store_control_error_releases_accounting_without_retry(
        self, mocker
    ) -> None:
        """A control signal after the final durable item must not invent a retry tail."""
        backend = mocker.Mock()
        monitor = mocker.Mock()

        def stop_after_final_store(key: str) -> None:
            if key == "k2":
                raise SystemExit("stop after final durable write")

        monitor.on_store.side_effect = stop_after_final_store
        strat = BatchedStorageStrategy(threshold=2, monitor=monitor)
        strat.store(backend, "k1", b"v1")

        with pytest.raises(SystemExit, match="stop after final durable write"):
            strat.store(backend, "k2", b"v2")

        assert [call.args[0] for call in backend.store.call_args_list] == ["k1", "k2"]
        assert strat._buffer == []
        assert strat.pending == 0
        assert strat._in_flight_count == 0

    def test_empty_retry_tail_is_an_accounting_noop(self) -> None:
        """An empty recovery tail cannot alter an active snapshot's accounting."""
        strat = BatchedStorageStrategy(threshold=2)
        with strat._lock:
            strat._in_flight_count = 1

        assert strat._requeue_tail([]) == 1
        assert strat._buffer == []
        assert strat._oldest_ts is None
        assert strat.pending == 1
        assert strat._in_flight_count == 1

    def test_backend_primary_survives_warning_and_depth_control_errors(
        self, mocker
    ) -> None:
        """Recovery diagnostics must not replace the causal backend exception."""
        backend = mocker.Mock()
        backend.store.side_effect = RuntimeError("backend down")
        monitor = mocker.Mock()

        def interrupt_requeue_depth(depth: int) -> None:
            # The second threshold store emits depth=2 before persistence.  Raise
            # only during the recovery depth emission, after backend.store failed.
            if depth == 2 and backend.store.call_count:
                raise SystemExit("monitor must not mask backend failure")

        monitor.on_buffer_depth.side_effect = interrupt_requeue_depth
        strat = BatchedStorageStrategy(threshold=2, monitor=monitor)
        strat.store(backend, "k1", b"v1")
        mocker.patch.object(
            batched_module.logger,
            "warning",
            side_effect=KeyboardInterrupt("warning must not mask backend failure"),
        )

        with pytest.raises(RuntimeError, match="backend down"):
            strat.store(backend, "k2", b"v2")

        assert strat.pending == 2
        assert all(entry[0] is backend for entry in strat._buffer)

    def test_threshold_flush_failure_propagates_without_losing_buffer(
        self, mocker
    ) -> None:
        """Sustained failure stays retryable while remaining visible to callers."""
        backend = mocker.Mock()
        backend.store.side_effect = RuntimeError("backend down")
        strat = BatchedStorageStrategy(threshold=2, max_pending=10)
        strat.store(backend, "k0", b"v")
        for i in range(1, 10):
            with pytest.raises(RuntimeError, match="backend down"):
                strat.store(backend, f"k{i}", b"v")
        # All 10 items remain available for a later successful flush.
        assert strat.pending == 10

    def test_explicit_flush_still_propagates_failure(self, mocker) -> None:
        """Explicit drains propagate using the same retry-tail contract."""
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
        strat = create_storage_strategy("batched", threshold=50, max_pending=75)
        assert isinstance(strat, BatchedStorageStrategy)
        assert strat.threshold == 50
        assert strat.max_pending == 75

    def test_batched_rejects_max_pending_below_threshold(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            create_storage_strategy("batched", threshold=50, max_pending=49)

        assert exc_info.value.setting_name == "max_pending"

    def test_batched_rejects_float_threshold(self) -> None:
        """R25-C: a float threshold must raise ConfigurationError, not silently
        truncate (which subverted BatchedStorageStrategy.__init__'s R21-D
        strict-int guard — 50.9 used to become 50 with no warning)."""
        with pytest.raises(ConfigurationError):
            create_storage_strategy("batched", threshold=50.9)

    def test_batched_rejects_bad_threshold_type(self) -> None:
        """R25-C: a non-numeric threshold raises the codebase-standard
        ConfigurationError, not a bare TypeError/ValueError."""
        with pytest.raises(ConfigurationError):
            create_storage_strategy("batched", threshold="abc")
        with pytest.raises(ConfigurationError):
            create_storage_strategy("batched", threshold=None)

    def test_batched_rejects_threshold_below_minimum(self) -> None:
        """R25-C: threshold < 1 raises ConfigurationError (minimum=1)."""
        with pytest.raises(ConfigurationError):
            create_storage_strategy("batched", threshold=0)

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

    def test_on_buffer_depth_resets_after_threshold_flush(self, mocker) -> None:
        from scrapy_extension.monitor.base import Monitor

        monitor = mocker.Mock(spec=Monitor)
        backend = mocker.Mock()
        strat = BatchedStorageStrategy(threshold=2, monitor=monitor)

        strat.store(backend, "k1", b"v1")
        strat.store(backend, "k2", b"v2")

        assert [call.args[0] for call in monitor.on_buffer_depth.call_args_list] == [
            1,
            2,
            0,
        ]

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

    def test_buffer_depth_monitor_control_exception_still_propagates(
        self, mocker
    ) -> None:
        """R100: direct monitor control exceptions retain their public meaning."""
        from scrapy_extension.monitor.base import Monitor

        backend = mocker.Mock()
        monitor = mocker.Mock(spec=Monitor)
        monitor.on_buffer_depth.side_effect = KeyboardInterrupt("stop store")
        strat = BatchedStorageStrategy(threshold=10, monitor=monitor)

        with pytest.raises(KeyboardInterrupt, match="stop store"):
            strat.store(backend, "k", b"v")

        # The item was buffered before telemetry ran, so a caller that handles the
        # control signal can retry the persistence lifecycle without losing it.
        assert strat.pending == 1

    def test_monitor_fallback_handlers_see_no_active_exception(self, mocker) -> None:
        """R47: ignored monitor errors must unwind before fallback logging."""
        marker = "round47-batched-private-marker"
        handler = _ExceptionContextHandler()
        monitor = mocker.Mock()
        strat = BatchedStorageStrategy(threshold=1, monitor=monitor)
        backend = mocker.Mock()
        previous_level = batched_module.logger.level
        batched_module.logger.setLevel(logging.DEBUG)
        batched_module.logger.addHandler(handler)
        try:
            monitor.on_buffer_depth.side_effect = RuntimeError(marker)
            strat._emit_buffer_depth(1)

            monitor.on_error.side_effect = RuntimeError(marker)
            strat._emit_error("store", RuntimeError(marker))

            monitor.on_buffer_depth.side_effect = None
            monitor.on_store.side_effect = RuntimeError(marker)
            strat.store(backend, "key", b"value")
        finally:
            batched_module.logger.removeHandler(handler)
            batched_module.logger.setLevel(previous_level)

        assert handler.active_exceptions
        assert all(state == (None, None, None) for state in handler.active_exceptions)
        for record in handler.records:
            assert marker not in record.getMessage()
            assert marker not in repr(record.args)
            assert record.exc_info is None
            assert record.exc_text is None

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

    def test_age_flush_failure_is_reported_to_monitor(self, mocker) -> None:
        """Background failures publish one fresh, key-free monitor error."""
        from scrapy_extension.monitor.base import Monitor

        attempted = threading.Event()
        failure = RuntimeError("backend down")
        backend = mocker.Mock()

        def fail_store(*_args, **_kwargs):
            attempted.set()
            raise failure

        backend.store.side_effect = fail_store
        monitor = mocker.Mock(spec=Monitor)
        strat = BatchedStorageStrategy(
            threshold=1000,
            max_buffer_age_s=0.01,
            monitor=monitor,
        )
        strat.store(backend, "k", b"v")

        assert attempted.wait(timeout=1.0)
        strat._stop.set()
        assert strat._flusher is not None
        strat._flusher.join(timeout=1.0)

        monitor.on_error.assert_called_once()
        operation, reported_error = monitor.on_error.call_args.args
        assert operation == "store"
        assert type(reported_error) is StorageError
        assert reported_error is not failure
        assert str(reported_error) == "Batched storage flush failed."
        assert reported_error.operation == "store"
        assert reported_error.key is None
        assert reported_error.__cause__ is None
        assert reported_error.__context__ is None
        assert reported_error.__traceback__ is None

    def test_age_flush_monitor_error_is_redacted_after_raw_failure_unwinds(
        self, monkeypatch, mocker
    ) -> None:
        """A monitor cannot recover a failed key/value/error graph from age flush."""
        from scrapy_extension.monitor.base import Monitor

        marker = "round45-batched-monitor-private-marker"
        raw_failures: list[StorageError] = []
        active_errors: list[BaseException | None] = []
        backend = mocker.Mock()

        def fail_store(*_args, **_kwargs):
            private_frame_state = {"marker": marker}
            try:
                raise RuntimeError(marker)
            except RuntimeError as cause:
                failure = StorageError(marker, operation=marker, key=marker)
                failure.private_state = private_frame_state
                raw_failures.append(failure)
                raise failure from cause

        backend.store.side_effect = fail_store
        monitor = mocker.Mock(spec=Monitor)

        def capture_error(_operation, _error) -> None:
            active_errors.append(sys.exc_info()[1])

        monitor.on_error.side_effect = capture_error
        strat = BatchedStorageStrategy(
            threshold=1000,
            max_buffer_age_s=0.1,
            monitor=monitor,
        )
        with strat._lock:
            strat._buffer.append((backend, marker, marker.encode(), None))
            strat._oldest_ts = 0.0
        stop = mocker.Mock()
        stop.wait.side_effect = [False, True]
        strat._stop = stop
        monkeypatch.setattr(
            batched_module.time, "monotonic", mocker.Mock(return_value=1.0)
        )

        strat._age_flush_loop()

        monitor.on_error.assert_called_once()
        operation, reported_error = monitor.on_error.call_args.args
        assert operation == "store"
        assert type(reported_error) is StorageError
        assert reported_error is not raw_failures[0]
        assert str(reported_error) == "Batched storage flush failed."
        assert reported_error.operation == "store"
        assert reported_error.key is None
        assert reported_error.__cause__ is None
        assert reported_error.__context__ is None
        assert reported_error.__traceback__ is None
        assert marker not in repr(reported_error.args)
        assert marker not in repr(reported_error.__dict__)
        assert active_errors == [None]
        assert strat.pending == 1

    def test_age_flush_warning_keyboardinterrupt_does_not_stop_retry_cycle(
        self, monkeypatch, mocker
    ) -> None:
        """R90: warning diagnostics cannot strand a recovered age-flush tail."""
        failure = RuntimeError("backend down")
        backend = mocker.Mock()
        backend.store.side_effect = [failure, None]
        strat = BatchedStorageStrategy(threshold=1000, max_buffer_age_s=0.1)
        with strat._lock:
            strat._buffer.append((backend, "k", b"v", None))
            strat._oldest_ts = 0.0
        stop = mocker.Mock()
        stop.wait.side_effect = [False, False, True]
        strat._stop = stop
        monotonic = mocker.Mock(side_effect=[1.0, 0.0, 1.0])
        monkeypatch.setattr(batched_module.time, "monotonic", monotonic)
        mocker.patch.object(
            batched_module.logger,
            "warning",
            side_effect=KeyboardInterrupt("diagnostic interrupted"),
        )

        strat._age_flush_loop()

        assert backend.store.call_count == 2
        assert strat.pending == 0

    def test_age_flush_monitor_fallback_debug_interrupt_does_not_stop_retry_cycle(
        self, monkeypatch, mocker
    ) -> None:
        """R90: a monitor's ordinary error plus debug interruption is non-fatal."""
        from scrapy_extension.monitor.base import Monitor

        failure = RuntimeError("backend down")
        backend = mocker.Mock()
        backend.store.side_effect = [failure, None]
        monitor = mocker.Mock(spec=Monitor)
        monitor.on_error.side_effect = RuntimeError("monitor down")
        strat = BatchedStorageStrategy(
            threshold=1000,
            max_buffer_age_s=0.1,
            monitor=monitor,
        )
        with strat._lock:
            strat._buffer.append((backend, "k", b"v", None))
            strat._oldest_ts = 0.0
        stop = mocker.Mock()
        stop.wait.side_effect = [False, False, True]
        strat._stop = stop
        monotonic = mocker.Mock(side_effect=[1.0, 0.0, 1.0])
        monkeypatch.setattr(batched_module.time, "monotonic", monotonic)
        mocker.patch.object(
            batched_module.logger,
            "debug",
            side_effect=KeyboardInterrupt("diagnostic interrupted"),
        )

        strat._age_flush_loop()

        monitor.on_error.assert_called_once()
        operation, reported_error = monitor.on_error.call_args.args
        assert operation == "store"
        assert type(reported_error) is StorageError
        assert reported_error is not failure
        assert str(reported_error) == "Batched storage flush failed."
        assert reported_error.operation == "store"
        assert reported_error.key is None
        assert backend.store.call_count == 2
        assert strat.pending == 0

    def test_age_flush_buffer_depth_fallback_debug_interrupt_keeps_two_cycles_alive(
        self, monkeypatch, mocker
    ) -> None:
        """R100: fallback depth diagnostics cannot terminate the age-flusher."""
        from scrapy_extension.monitor.base import Monitor

        backend = mocker.Mock()
        monitor = mocker.Mock(spec=Monitor)
        monitor.on_buffer_depth.side_effect = RuntimeError("monitor down")
        strat = BatchedStorageStrategy(
            threshold=1000,
            max_buffer_age_s=0.1,
            monitor=monitor,
        )
        with strat._lock:
            strat._buffer.append((backend, "k1", b"v1", None))
            strat._oldest_ts = 0.0

        def store_second_batch(key, _value, *, ttl=None):  # noqa: ARG001
            if key == "k1":
                with strat._lock:
                    strat._buffer.append((backend, "k2", b"v2", None))
                    strat._oldest_ts = 0.0

        backend.store.side_effect = store_second_batch
        stop = mocker.Mock()
        stop.wait.side_effect = [False, False, True]
        strat._stop = stop
        monkeypatch.setattr(
            batched_module.time,
            "monotonic",
            mocker.Mock(side_effect=[1.0, 1.0]),
        )
        mocker.patch.object(
            batched_module.logger,
            "debug",
            side_effect=KeyboardInterrupt("fallback diagnostic interrupted"),
        )

        strat._age_flush_loop()

        assert [call.args[0] for call in backend.store.call_args_list] == ["k1", "k2"]
        assert strat.pending == 0

    def test_age_flush_buffer_depth_monitor_control_exception_propagates(
        self, monkeypatch, mocker
    ) -> None:
        """R100: direct buffer-depth control exceptions are not swallowed."""
        from scrapy_extension.monitor.base import Monitor

        backend = mocker.Mock()
        monitor = mocker.Mock(spec=Monitor)
        monitor.on_buffer_depth.side_effect = SystemExit("stop flusher")
        strat = BatchedStorageStrategy(
            threshold=1000,
            max_buffer_age_s=0.1,
            monitor=monitor,
        )
        with strat._lock:
            strat._buffer.append((backend, "k", b"v", None))
            strat._oldest_ts = 0.0
        stop = mocker.Mock()
        stop.wait.side_effect = [False]
        strat._stop = stop
        monkeypatch.setattr(
            batched_module.time, "monotonic", mocker.Mock(return_value=1.0)
        )

        with pytest.raises(SystemExit, match="stop flusher"):
            strat._age_flush_loop()

        backend.store.assert_called_once_with("k", b"v", ttl=None)


class TestBatchedStorageFlusherTOCTOU:
    """R-flusher-1: ``_ensure_flusher``'s guard + create + start must be ATOMIC
    (under ``self._lock``) so concurrent stores can't each spawn a daemon flusher.

    Pre-fix, the guard checked ``self._flusher is not None`` OUTSIDE the lock, so
    N threads racing the first ``store()`` each observed ``_flusher is None``,
    each constructed a ``Thread``, each called ``start()`` → N orphaned daemon
    flushers. The code comment claiming "idempotent guard guarantees no
    double-start" was a false claim; this test pins the corrected atomic
    behavior. Race-window widening (the patched ``threading.Thread`` sleeps) makes
    the TOCTOU deterministic both pre-fix (N flushers) and post-fix (1 flusher).
    """

    def test_concurrent_stores_start_exactly_one_flusher(self, mocker) -> None:
        import time

        real_thread = threading.Thread

        def slow_thread_ctor(*args, **kwargs):
            # Widen the window between the `_flusher is not None` guard and the
            # `self._flusher = flusher` assignment so the TOCTOU is observable
            # deterministically rather than via scheduler timing.
            time.sleep(0.02)
            return real_thread(*args, **kwargs)

        # Patch the Thread constructor the strategy resolves (``import threading``
        # then ``threading.Thread(...)`` in batched.py). Global patch is fine —
        # only the racer + flusher constructions happen during this test.
        mocker.patch(
            "scrapy_extension.storage.strategies.batched.threading.Thread",
            side_effect=slow_thread_ctor,
        )

        backend = mocker.Mock()
        # threshold huge so no threshold-flush interferes; max_buffer_age_s set so
        # _ensure_flusher actually fires.
        strat = BatchedStorageStrategy(threshold=10**9, max_buffer_age_s=1.0)

        n = 8
        barrier = threading.Barrier(n)

        def racer(i: int) -> None:
            barrier.wait()  # release all racers into store() simultaneously
            strat.store(backend, f"k{i}", b"v")

        threads = [threading.Thread(target=racer, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        try:
            flushers = [
                t
                for t in threading.enumerate()
                if t.name == "batched-storage-age-flush"
            ]
            assert len(flushers) == 1, (
                f"expected exactly 1 age-flush thread (atomic guard), found {len(flushers)}"
            )
        finally:
            strat.close()

    def test_start_failure_rolls_back_flusher_and_allows_retry(self, mocker) -> None:
        """R49: a failed Thread.start() must not permanently disable age flushing."""
        strat = BatchedStorageStrategy(threshold=100, max_buffer_age_s=1.0)
        failed_thread = mocker.MagicMock()
        failed_thread.start.side_effect = RuntimeError("thread start failed")

        thread_constructor = mocker.patch(
            "scrapy_extension.storage.strategies.batched.threading.Thread",
            return_value=failed_thread,
        )
        try:
            with pytest.raises(RuntimeError, match="thread start failed"):
                strat._ensure_flusher()
        finally:
            mocker.stop(thread_constructor)

        assert strat._flusher is None
        strat._ensure_flusher()
        assert strat._flusher is not None and strat._flusher.is_alive()
        strat.close()

    def test_close_does_not_hang_when_age_flusher_wedges_on_flush_lock(
        self, mocker
    ) -> None:
        """R22-B: ``close()`` must not block forever on ``_flush_lock`` when the
        age-flusher is wedged mid-``store()`` against an unresponsive backend
        (redis-py ``socket_timeout=None`` / pymongo ``socketTimeoutMS=None``).

        Pre-fix, the post-join ``self.flush()`` re-entered ``with
        self._flush_lock:`` and blocked indefinitely — the 5s join timeout was
        theater and ``close_spider`` hung until SIGKILL. The durable fix (option a)
        bounds the ``_flush_lock`` acquisition itself, so both ``close()`` and the
        public ``flush()`` skip-and-log instead of hanging. This pins that close()
        returns within a bounded window while the flusher still holds the lock.
        """
        from scrapy_extension.storage.strategies import batched as batched_mod

        backend = mocker.Mock()
        store_entered = threading.Event()
        release_store = threading.Event()

        def wedged_store(*_args, **_kwargs):
            store_entered.set()
            # Hold _flush_lock until the test releases us — simulates a backend that
            # blocks without raising (no socket timeout configured).
            release_store.wait(timeout=30.0)

        backend.store.side_effect = wedged_store
        strat = BatchedStorageStrategy(threshold=10**9, max_buffer_age_s=0.01)
        # Shrink both bounds so the wedge case is fast: the per-acquire timeout
        # (_FLUSH_LOCK_TIMEOUT_S) and R23-A's close drain deadline
        # (_CLOSE_DRAIN_DEADLINE_S, which replaced the old fixed 5s join).
        if hasattr(batched_mod, "_FLUSH_LOCK_TIMEOUT_S"):
            mocker.patch.object(batched_mod, "_FLUSH_LOCK_TIMEOUT_S", 0.3)
        if hasattr(batched_mod, "_CLOSE_DRAIN_DEADLINE_S"):
            mocker.patch.object(batched_mod, "_CLOSE_DRAIN_DEADLINE_S", 1.0)

        strat.store(backend, "k", b"v")  # append + spawn the age-flusher
        # Wait until the flusher is mid-store(), holding _flush_lock.
        assert store_entered.wait(timeout=2.0), "age-flusher never reached store()"

        close_done = threading.Event()
        close_errors: list[Exception] = []

        def run_close() -> None:
            try:
                strat.close()
            except Exception as exc:  # noqa: BLE001 — capture any close() failure
                close_errors.append(exc)
            finally:
                close_done.set()

        close_thread = threading.Thread(target=run_close, daemon=True)
        close_thread.start()
        try:
            # drain deadline (1.0s) + acquire (0.3s) + slack; under the hang this
            # never completes.
            close_done.wait(timeout=7.0)
            assert close_done.is_set(), (
                "close() hung: the post-drain flush() blocked on _flush_lock held by "
                "the wedged age-flusher instead of bounding the acquisition (R22-B/R23-A)"
            )
            assert close_errors == []
        finally:
            release_store.set()
            close_thread.join(timeout=2.0)
