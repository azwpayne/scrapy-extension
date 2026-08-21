"""Batched storage strategy — buffers backend-bound items before flushing.

Each record retains the exact backend capability received by its ``store``
call. Records are written in global insertion order when the buffer reaches a
configurable threshold or on ``flush()`` / ``close()``. Persistence is delayed
(items are lost on crash before flush — a distinct failure mode from a store
*exception*, which is handled with at-least-once re-enqueueing; see
:meth:`_flush`).
Thread-safe via an internal lock — Scrapy pipelines are single-threaded per
spider, but the guard makes the strategy safe under concurrent stores (e.g.
concurrent item-processing callers feeding one shared strategy). Callers that
share an instance must coordinate its single ``open`` / ``close`` lifecycle.

Risk 2 (crash-before-flush loss window): when ``max_buffer_age_s`` is set, a
daemon thread flushes once the oldest buffered item is older than the cap,
bounding the documented crash-loss of the in-flight batch to roughly that
value. ``None`` (default) is byte-identical to the pre-Risk-2 behavior.
"""

from __future__ import annotations

__all__ = ["BatchedStorageStrategy"]

import logging
import math
import threading
import time
from typing import TYPE_CHECKING

from scrapy_extension.exceptions import StorageBackpressureError, StorageError
from scrapy_extension.exceptions._redaction import storage_operation_error_boundary
from scrapy_extension.monitor.base import Monitor, NullMonitor
from scrapy_extension.storage.strategies.base import StorageStrategy

if TYPE_CHECKING:
    from scrapy_extension.backends.base import StorageBackend

#: Default flush threshold (items) — chosen to match the common "100 items per
#: batch" rule of thumb and to keep the docstring / factory default in sync.
DEFAULT_BATCH_THRESHOLD = 100

#: R22-B: ceiling (seconds) for acquiring ``_flush_lock`` in :meth:`_flush`.
#: Bounds the public-flush path so a wedged backend (redis-py default
#: ``socket_timeout=None``, pymongo default ``socketTimeoutMS=None``) cannot hang
#: a synchronous caller forever: another flush may hold ``_flush_lock`` across
#: its ``storage_backend.store()`` network-I/O loop. This bound only fires when a flush
#: exceeds it (e.g. a large batch against a high-latency cross-region backend);
#: healthy ``store()`` is milliseconds, so in normal operation the lock is free
#: and the acquire returns instantly.
_FLUSH_LOCK_TIMEOUT_S: float = 5.0

#: R23-A/R75: hard ceiling (seconds) for :meth:`close` to wait for *any*
#: ``_flush_lock`` holder before the final drain. Pre-R23 :meth:`close` did
#: a single fixed ``join(5.0)``; a slow-but-healthy cross-region flush (e.g.
#: Mongo Atlas at ~150ms/store x threshold=100 = ~15s) left the flusher alive
#: past that window, so the subsequent ``flush()`` lost the bounded
#: ``_flush_lock`` race and SKIPPED, abandoning items still in ``_buffer``.
#: R75 extends the deadline to threshold and explicit flushes too: close
#: acquires the serialization lock directly, rather than taking public
#: ``flush()``'s short anti-hang timeout. A genuinely-wedged backend remains
#: bounded (at a 30s ceiling instead of the old 10s).
_CLOSE_DRAIN_DEADLINE_S: float = 30.0

#: R139-F7: floor (seconds) for a single age-flusher wait (see
#: :meth:`BatchedStorageStrategy._next_age_wait_interval`). A nearly-due oldest
#: item has a remaining age budget at or below zero; without a floor the
#: deadline-driven loop would wake in a tight spin until the flush comparison
#: ticks over. 10ms bounds that spin while keeping worst-case flush latency at
#: the configured cap plus one wakeup epsilon.
_AGE_WAIT_FLOOR_S: float = 0.01

_BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE = "Batched storage flush failed."

logger = logging.getLogger(__name__)

_BufferedEntry = tuple["StorageBackend", str, bytes, int | None]


class BatchedStorageStrategy(StorageStrategy):
    """Buffer items and flush to the backend in batches.

    Attributes:
        threshold: Number of buffered items that triggers an automatic flush.
        max_buffer_age_s: Risk 2 — age cap (seconds) beyond which a background
            flush fires, bounding the crash-before-flush loss window. ``None``
            disables it (flush only on threshold / close).
        max_pending: Maximum number of accepted-but-not-yet-persisted records,
            including both buffered and currently flushing entries. ``None``
            uses ``2 * threshold``.
        pending: Count of accepted-but-not-yet-persisted records, including the
            in-flight snapshot currently being written.
    """

    emits_store_events = True

    def __init__(
        self,
        threshold: int = DEFAULT_BATCH_THRESHOLD,
        *,
        max_buffer_age_s: float | None = None,
        max_pending: int | None = None,
        monitor: Monitor | None = None,
    ) -> None:
        """Initialize the batched strategy.

        Args:
            threshold: Buffer size that triggers an automatic flush. Must be >= 1.
            max_buffer_age_s: Risk 2 — caps the crash-before-flush loss window.
                When set, a daemon thread flushes once the oldest buffered item is
                older than this many seconds, bounding the documented crash-loss
                of the in-flight batch to roughly this value. ``None`` (default)
                disables the age-based flush — byte-identical to the pre-Risk-2
                behavior (flush only on threshold / close).
            max_pending: Maximum accepted-but-not-yet-persisted item count across
                the pending buffer and the snapshot currently being flushed. When
                full, :meth:`store` rejects the new item immediately with
                :class:`~scrapy_extension.exceptions.StorageBackpressureError`.
                ``None`` (default) uses ``2 * threshold``.
            monitor: Optional observability monitor. When ``None`` (default)
                :class:`~scrapy_extension.monitor.base.NullMonitor`. Emits
                ``on_buffer_depth(buffer + in_flight)`` after each accepted item
                and flush outcome so a wired collector sees the true outstanding
                backlog.

        Raises:
            ValueError: If ``threshold`` is less than 1, ``max_pending`` is not a
                non-boolean integer at least ``threshold``, or a configured
                ``max_buffer_age_s`` is not positive.
        """
        # R21-D: NaN bypasses plain comparison guards (nan < 1 and nan <= 0 are both
        # False) — reject non-finite values explicitly. Mirrors delay.py
        # _require_finite / time_wheel.py _finite_number.
        if (
            isinstance(threshold, bool)
            or not isinstance(threshold, int)
            or not math.isfinite(threshold)
        ):
            msg = f"threshold must be a finite int >= 1, got {threshold!r}"
            raise ValueError(msg)
        if threshold < 1:
            msg = f"threshold must be >= 1, got {threshold}"
            raise ValueError(msg)
        if max_pending is None:
            max_pending = 2 * threshold
        if isinstance(max_pending, bool) or not isinstance(max_pending, int):
            msg = f"max_pending must be an int >= threshold, got {max_pending!r}"
            raise ValueError(msg)
        if max_pending < threshold:
            msg = f"max_pending must be >= threshold ({threshold}), got {max_pending}"
            raise ValueError(msg)
        if max_buffer_age_s is not None and (
            not math.isfinite(max_buffer_age_s) or max_buffer_age_s <= 0
        ):
            msg = f"max_buffer_age_s must be > 0 (and finite), got {max_buffer_age_s!r}"
            raise ValueError(msg)
        self.threshold = threshold
        self.max_buffer_age_s = max_buffer_age_s
        self.max_pending = max_pending
        self._buffer: list[_BufferedEntry] = []
        # Entries are removed from ``_buffer`` before their backend call so other
        # callers can enqueue while network I/O runs.  Keep their count separately
        # so admission, ``pending``, and monitor depth retain a hard total bound.
        self._in_flight_count = 0
        self._lock = threading.Lock()
        # Serializes the complete snapshot/write/requeue transaction. The buffer
        # lock alone cannot make close wait for a threshold flush after that flush
        # has detached its batch and started writing outside _lock.
        self._flush_lock = threading.Lock()
        self._closed = False
        self._owner: object | None = None
        self._monitor: Monitor = monitor if monitor is not None else NullMonitor()
        # Risk 2: oldest-buffered-item timestamp (monotonic) + flusher lifecycle.
        # ``_oldest_ts`` is None whenever the buffer is empty; set on first append
        # after a drain, reset to None under the lock whenever the buffer empties.
        self._oldest_ts: float | None = None
        self._flusher: threading.Thread | None = None
        self._stop = threading.Event()

    def attach_owner(self, owner: object) -> None:
        """Attach exactly one lifecycle owner to the strategy.

        A batched strategy has one close boundary for its buffer and age flusher.
        Reattaching the same owner is harmless, while a distinct pipeline is
        rejected rather than silently sharing that close boundary.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("batched storage strategy is closed")
            if self._owner is None:
                self._owner = owner
                return
            if self._owner is owner:
                return
            raise RuntimeError("batched storage strategy already has an owner")

    @property
    def pending(self) -> int:
        """Number of accepted-but-not-yet-persisted items (thread-safe)."""
        with self._lock:
            return self._pending_locked()

    def _pending_locked(self) -> int:
        """Return the outstanding count while ``_lock`` is held."""
        return len(self._buffer) + self._in_flight_count

    def set_monitor(self, monitor: Monitor) -> None:
        """Inject a monitor after construction (Risk 2 wiring).

        Lets ``BackendPipeline.from_crawler`` share its (possibly late-wired)
        :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` with the strategy so
        ``on_buffer_depth`` emits through the same collector. Safe to call any
        time; the strategy defaults to :class:`NullMonitor` until called.

        Args:
            monitor: The monitor to use for ``on_buffer_depth`` emissions.
        """
        self._monitor = monitor

    def _emit_buffer_depth(self, depth: int) -> None:
        """Publish the buffer gauge without allowing telemetry into the data path."""
        monitor_failed = False
        try:
            self._monitor.on_buffer_depth(depth)
        except Exception:  # noqa: BLE001 - monitor must never crash storage
            monitor_failed = True

        if monitor_failed:
            # Invoke the fallback only after the monitor exception suite has ended.
            # A custom logging handler is extension code and must not inherit the
            # monitor's raw failure through ``sys.exc_info()``.
            try:
                logger.debug("on_buffer_depth hook raised")
            except BaseException:
                # This is only fallback diagnostics after an ordinary monitor failure;
                # it must not interrupt a synchronous store or the age-flush daemon.
                pass

    def _emit_error(self, operation: str, error: Exception) -> None:
        """Publish an error without allowing telemetry to stop retry processing."""
        monitor_failed = False
        try:
            self._monitor.on_error(operation, error)
        except Exception:  # noqa: BLE001 - monitor must never crash storage
            monitor_failed = True

        if monitor_failed:
            # The monitor's ordinary failure is no longer active here, so a logging
            # handler cannot inspect it through ``sys.exc_info()``.
            try:
                logger.debug("on_error hook raised")
            except BaseException:
                # This fallback must not terminate the age-flush retry daemon and
                # strand its re-enqueued batch.
                pass

    def store(
        self,
        storage_backend: StorageBackend,
        key: str,
        value: bytes,
        ttl: int | None = None,
    ) -> None:
        """Buffer one item; auto-flush when the buffer reaches the threshold.

        Buffering is at-least-once for backend exceptions: ``_flush`` restores
        the unwritten tail before re-raising. Threshold-triggered failures propagate
        so ``BackendPipeline`` records a real persistence failure and its
        ``max_storage_errors`` guard remains effective; returning success here would
        emit ``on_store`` for data that exists only in volatile memory.

        Args:
            storage_backend: The exact caller-owned backend capability retained for
                this entry until it is written or discarded with the strategy.
            key: The storage key.
            value: The serialized item bytes.
            ttl: Optional time-to-live in seconds.
        """
        flush_now = False
        with self._lock:
            if self._closed:
                raise RuntimeError("batched storage strategy is closed")
            if self._pending_locked() >= self.max_pending:
                # Admission must fail before deciding whether a threshold flush should
                # run. In particular, a full strategy never waits for ``_flush_lock``
                # merely to discover that it cannot accept another record.
                raise StorageBackpressureError(operation="store")
            self._buffer.append((storage_backend, key, value, ttl))
            if self._oldest_ts is None:
                self._oldest_ts = time.monotonic()
            depth = self._pending_locked()
            if len(self._buffer) >= self.threshold:
                flush_now = True
        # on_buffer_depth is a no-op under NullMonitor; emit outside the lock and
        # guard it so a misbehaving monitor can never crash the store path
        # (matches the BLE001-guard convention used across the codebase).
        self._emit_buffer_depth(depth)
        self._ensure_flusher()
        if flush_now:
            self._flush()

    @storage_operation_error_boundary(
        "store",
        _BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,
        "storage-strategy",
        safe_messages=(_BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,),
    )
    def flush(self) -> None:
        """Flush buffered items to their per-entry backends in insertion order.

        The exact backend from every ``store`` call travels with that entry through
        explicit, threshold, age, close, and retry drains. No-op when empty.
        """
        self._flush(raise_on_lock_timeout=True)

    @storage_operation_error_boundary(
        "store",
        _BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,
        "storage-strategy",
        safe_messages=(_BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,),
    )
    def close(self) -> None:
        """Flush remaining buffered items, then release resources.

        Stops the age-based flusher (Risk 2) and waits, bounded, for every active
        flush transaction before draining. This prevents
        ``BackendPipeline.close_spider`` from tearing down the backend connection
        while a daemon, threshold, or explicit flush is mid-``store``.

        A control exception during the age-flusher join must not bypass the final
        drain. Close records the first such exception, completes its bounded drain
        attempt, then re-raises that original control exception.
        """
        with self._lock:
            self._closed = True
        self._stop.set()
        primary_error: BaseException | None = None
        deadline = time.monotonic() + _CLOSE_DRAIN_DEADLINE_S
        flusher = self._flusher
        if flusher is not None and flusher.is_alive():
            # The join lets the daemon exit promptly after its active store returns.
            # The direct lock acquisition below is still required: threshold/manual
            # callers can hold _flush_lock without an age flusher.
            while flusher.is_alive() and time.monotonic() < deadline:
                try:
                    flusher.join(
                        timeout=min(1.0, max(0.0, deadline - time.monotonic()))
                    )
                except BaseException as error:
                    # Do not let KeyboardInterrupt/SystemExit skip the final drain. The
                    # lock phase below waits through the same shared deadline and drains
                    # if the current flush holder releases.
                    primary_error = error
                    break
            if flusher.is_alive():
                try:
                    logger.warning(
                        "batched-storage-age-flush did not exit within %.1fs; "
                        "close remains retryable",
                        _CLOSE_DRAIN_DEADLINE_S,
                    )
                except BaseException:
                    pass

        # R75: public flush deliberately gives up after its short per-acquire
        # timeout to keep synchronous callers responsive. Close has a stronger
        # durability obligation: wait through the remaining shutdown deadline for
        # *any* holder, then serialize the final snapshot/write/requeue operation.
        # Retry an interrupted acquire until the deadline, retaining the first
        # control exception for re-signal only after the drain attempt.
        acquired = False
        while not acquired:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # A join can consume the final fraction of the deadline just as its
                # holder releases the lock. Preserve one final drain opportunity
                # without exceeding the bounded close contract.
                try:
                    acquired = self._flush_lock.acquire(blocking=False)
                except BaseException as error:
                    if primary_error is None:
                        primary_error = error
                break
            try:
                acquired = self._flush_lock.acquire(timeout=remaining)
            except BaseException as error:
                if primary_error is None:
                    primary_error = error

        if not acquired:
            try:
                logger.warning(
                    "batched-storage close drain lock not acquired within %.1fs; "
                    "close remains retryable",
                    _CLOSE_DRAIN_DEADLINE_S,
                )
            except BaseException:
                pass
            if primary_error is None:
                primary_error = StorageError(
                    _BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,
                    operation="store",
                    key=None,
                )
        else:
            try:
                try:
                    self._flush_serialized()
                except Exception as first_error:
                    # At-least-once at the final drain: _flush_serialized has
                    # already re-enqueued the un-written tail into _buffer. Retry
                    # the drain once so a transient store exception does not strand
                    # the requeued items when the caller (BackendPipeline) closes
                    # the backend right after close() returns/raises. Only ordinary
                    # Exceptions retry -- control BaseExceptions bypass this
                    # handler and propagate to the outer block (no retry, matching
                    # the pre-fix control-signal contract).
                    if not self._buffer:
                        raise first_error
                    self._flush_serialized()
            except BaseException as error:
                if primary_error is None:
                    primary_error = error
            finally:
                self._flush_lock.release()

        # Normal return is a durability barrier, not a best-effort attempt. Keep
        # ``_closed`` and ``_stop`` asserted after failure to reject new admissions,
        # while a later close() retries the unchanged tail and worker termination.
        flusher = self._flusher
        flusher_alive = flusher is not None and flusher.is_alive()
        if primary_error is None and (self.pending != 0 or flusher_alive):
            primary_error = StorageError(
                _BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,
                operation="store",
                key=None,
            )

        if primary_error is None:
            with self._lock:
                self._owner = None
        if primary_error is not None:
            raise primary_error

    def _flush(self, *, raise_on_lock_timeout: bool = False) -> None:
        """Drain the buffer to each entry's backend in insertion order.

        At-least-once under partial failure: the buffer is snapshotted and cleared
        under the lock, then each item is written through the exact backend
        capability retained with it. If ``backend.store`` raises on item N, the
        un-written backend-bound tail (items N..end) is prepended back into
        ``_buffer`` under the lock and the exception is re-raised so the caller
        knows the flush was partial. The previously snapshotted items (already
        written) are not re-added — the tail carries only what was not yet
        attempted.

        Note: this protects against store failures, including process-control
        signals such as ``KeyboardInterrupt``; a process *crash* before
        the flush completes still loses the in-flight batch (documented at module
        level) — that is a separate failure mode requiring durable buffering.
        Risk 2's ``max_buffer_age_s`` bounds (but does not eliminate) that window.

        R22-B: the ``_flush_lock`` acquisition is *bounded* by
        ``_FLUSH_LOCK_TIMEOUT_S``. If the age-flusher (or any concurrent holder) is
        wedged mid-``store()`` against an unresponsive backend, public
        :meth:`flush` raises a fixed :class:`StorageError` instead of falsely
        reporting success or blocking forever; pending work remains owned by the
        strategy. Internal threshold/age attempts remain best-effort. The close path
        has a distinct, longer ``_CLOSE_DRAIN_DEADLINE_S`` because it must drain any
        tail left by a slow-but-healthy transaction before releasing backend
        resources; it serializes that final drain directly.
        """
        acquired = self._flush_lock.acquire(timeout=_FLUSH_LOCK_TIMEOUT_S)
        if not acquired:
            # Logging is diagnostic only: a handler failure cannot replace the fixed,
            # typed public result or expose buffered key/value/backend details.
            try:
                logger.warning(
                    "batched-storage flush lock not acquired within %.1fs; "
                    "pending work retained",
                    _FLUSH_LOCK_TIMEOUT_S,
                )
            except BaseException:
                pass
            if raise_on_lock_timeout:
                raise StorageError(
                    _BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,
                    operation="store",
                    key=None,
                )
            return
        try:
            self._flush_serialized()
        finally:
            self._flush_lock.release()

    def _flush_serialized(self) -> None:
        """Run one flush while the caller owns ``_flush_lock``."""
        with self._lock:
            batch = list(self._buffer)
            self._buffer = []
            self._oldest_ts = None  # buffer drained; age resets on next append
            self._in_flight_count += len(batch)
        for i, (storage_backend, key, value, ttl) in enumerate(batch):
            try:
                storage_backend.store(key, value, ttl=ttl)
            except BaseException:
                # Re-enqueue the un-written tail (this item + remaining) so the next
                # flush retries them. This must include BaseException: after a
                # KeyboardInterrupt/SystemExit the caller still receives the control
                # signal, but the unattempted tail must not have been silently lost.
                # At-least-once: no silent loss.
                requeued_depth = self._requeue_tail(batch[i:])
                try:
                    logger.warning(
                        "batched flush partial: %d/%d items written, %d re-enqueued",
                        i,
                        len(batch),
                        len(batch) - i,
                    )
                except BaseException:
                    # Recovery diagnostics must not replace the backend failure after
                    # its retry tail is safely restored.
                    pass
                try:
                    self._emit_buffer_depth(requeued_depth)
                except BaseException:
                    # This telemetry follows safe requeue while the backend failure is
                    # active; neither monitor nor diagnostic code may replace it.
                    pass
                raise
            # This entry is known durable before observability runs. Release exactly
            # one admission slot under the same lock that protects the buffer so a
            # concurrent caller can use it without violating the total cap.
            self._release_persisted_entry()
            store_monitor_failed = False
            try:
                self._monitor.on_store(key)
            except Exception:  # noqa: BLE001 - persistence already succeeded
                store_monitor_failed = True
            except BaseException:
                # This item is already durable, but its remaining snapshot tail has
                # not yet been attempted.  Preserve precisely that tail before
                # honoring a control exception from the monitor.  Do not requeue the
                # current item: retrying it would convert a successful write into an
                # avoidable duplicate.
                if i + 1 < len(batch):
                    self._requeue_tail(batch[i + 1 :])
                raise
            if store_monitor_failed:
                # The ordinary monitor failure has unwound before diagnostics run;
                # logging handlers therefore cannot recover it through
                # ``sys.exc_info()``.
                try:
                    logger.debug("on_store hook raised")
                except BaseException:
                    # A diagnostic handler cannot be allowed to interrupt the remaining
                    # snapshot after the monitor's ordinary failure was intentionally
                    # ignored.
                    pass
        if batch:
            with self._lock:
                remaining_depth = self._pending_locked()
            self._emit_buffer_depth(remaining_depth)

    def _release_persisted_entry(self) -> None:
        """Release one in-flight admission slot after a durable backend write."""
        with self._lock:
            if self._in_flight_count <= 0:  # pragma: no cover - invariant guard
                raise RuntimeError("batched storage in-flight accounting underflow")
            self._in_flight_count -= 1

    def _requeue_tail(self, tail: list[_BufferedEntry]) -> int:
        """Prepend an unattempted snapshot tail without changing entry identity."""
        with self._lock:
            # New items may have been appended between the snapshot and a failure —
            # preserve them after the earlier snapshot tail.  ``tail`` contains the
            # original entry tuples, so each carries its exact backend capability.
            tail_size = len(tail)
            if tail_size > self._in_flight_count:  # pragma: no cover - invariant guard
                raise RuntimeError("batched storage in-flight accounting underflow")
            tail.extend(self._buffer)
            self._buffer = tail
            # The tail remains outstanding, but changes ownership from the snapshot
            # to the retry buffer. Decrementing in-flight before returning preserves
            # the total (buffer + in-flight) count under the lock.
            self._in_flight_count -= tail_size
            # Re-enqueued tail's oldest is approximately now (per-item timestamps
            # aren't tracked), giving retries a fresh conservative age budget.
            if self._buffer:
                self._oldest_ts = time.monotonic()
            return self._pending_locked()

    def _ensure_flusher(self) -> None:
        """Start the age-based background flusher (Risk 2), exactly once.

        Lazy + atomic: the daemon thread is started on the first ``store`` after
        which a non-None ``max_buffer_age_s`` is configured. It runs until
        :meth:`close` sets ``_stop``. Pipelines are single-threaded per spider;
        the flusher is the only background thread and serializes flushes via
        ``_lock`` + ``_flush``.

        R-flusher-1: the guard + create + start are performed UNDER ``self._lock``
        so concurrent stores (a documented-supported scenario — see module
        docstring) cannot each observe ``_flusher is None`` and each spawn a daemon
        flusher. The pre-fix guard checked ``_flusher is not None`` outside the lock
        (a TOCTOU); the first racer now holds the lock through Thread construction
        + assignment + start, and the rest see ``_flusher`` non-None on entry and
        return without constructing. ``max_buffer_age_s is None`` is checked outside
        the lock (immutable after ``__init__`` — never changes, so it's a safe
        fast-path that avoids acquiring the lock when the flusher is disabled).
        """
        if self.max_buffer_age_s is None:
            return
        with self._lock:
            if self._closed or self._flusher is not None:
                return
            flusher = threading.Thread(
                target=self._age_flush_loop,
                name="batched-storage-age-flush",
                daemon=True,
            )
            # Assign + start inside the lock so the guard (``_flusher is not None``)
            # check above is atomic with the assignment — concurrent stores can't
            # each pass the guard and each start a flusher.
            self._flusher = flusher
            try:
                flusher.start()
            except BaseException:
                # A failed start never creates a usable worker. Roll back the
                # provisional reference while the guard lock is held so a later store
                # can safely retry with a fresh thread.
                self._flusher = None
                raise

    def _next_age_wait_interval(self, age: float) -> float:
        """Return the deadline-driven wait until the next age-flush check.

        R139-F7: the pre-fix loop waited a fixed ``age`` between checks, so an
        item accepted just after a wake missed the next check and spent up to
        ``2 * age`` in volatile memory — twice what ``max_buffer_age_s`` is
        documented to bound. This computes each wait from the oldest item's own
        deadline instead: an empty buffer (or a missing timestamp) waits a full
        ``age``; a live oldest item waits exactly its remaining budget, clamped
        to ``_AGE_WAIT_FLOOR_S`` so a nearly-due item cannot busy-loop the
        daemon.

        Acquires ``self._lock`` to read a consistent
        (``_buffer``, ``_oldest_ts``) pair and reads the clock under it so the
        budget cannot go stale mid-computation; the caller must wait OUTSIDE
        the lock (the loop's existing discipline).

        Args:
            age: The already-None-checked ``max_buffer_age_s`` cap.

        Returns:
            Seconds the age-flush loop should wait before its next check.
        """
        with self._lock:
            oldest_ts = self._oldest_ts
            if not self._buffer or oldest_ts is None:
                return age
            return max(age - (time.monotonic() - oldest_ts), _AGE_WAIT_FLOOR_S)

    def _age_flush_loop(self) -> None:
        """Flush when the oldest buffered item reaches the age cap (deadline-driven).

        Bounds the crash-before-flush loss window to roughly ``max_buffer_age_s``:
        each cycle sleeps the oldest item's REMAINING age budget (a full ``age``
        when the buffer is empty — see :meth:`_next_age_wait_interval`), so an
        item accepted just after a wake is re-checked at its own deadline rather
        than one full interval later. Worst-case time-to-flush is the cap plus
        one wakeup epsilon instead of up to ``2 * age``. The flush condition
        itself is unchanged: flush once the oldest item is at least ``age`` old.
        Uses ``_stop.wait(timeout=...)`` so :meth:`close` unblocks it immediately
        on shutdown. All flush work goes through ``_flush`` (lock-guarded) so it
        composes safely with the store-path threshold flush. A transient flush
        failure is logged and the loop continues so a temporary outage does not
        permanently disable the flusher.
        """
        age = self.max_buffer_age_s
        if age is None:  # defensive — _ensure_flusher should have checked
            return
        while not self._stop.wait(timeout=self._next_age_wait_interval(age)):
            with self._lock:
                need_flush = (
                    bool(self._buffer)
                    and self._oldest_ts is not None
                    and (time.monotonic() - self._oldest_ts) >= age
                )
            if need_flush:
                reported_error: StorageError | None = None
                try:
                    self._flush()
                except Exception:  # noqa: BLE001 — keep retry loop alive
                    # Keep the loop alive so a transient outage doesn't disable the
                    # flusher, and report a static failure with no synchronous caller.
                    # ``_flush`` has already restored its retry tail; do not retain its
                    # raw backend graph in an extension-facing monitor event.
                    reported_error = StorageError(
                        _BATCHED_STORAGE_FLUSH_FAILURE_MESSAGE,
                        operation="store",
                        key=None,
                    )
                if reported_error is not None:
                    # Dispatch after the ``except`` has unwound so listener code cannot
                    # recover the raw backend error through the active exception state.
                    self._emit_error("store", reported_error)
                    try:
                        logger.warning(
                            "age-based flush failed; will retry next cycle (loss window "
                            "may grow until the backend recovers)"
                        )
                    except BaseException:
                        # The backend failure was already recovered into the retry buffer.
                        # Diagnostics must not kill the daemon before its next cycle.
                        pass
