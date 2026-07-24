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

from scrapy_extension.monitor.base import Monitor, NullMonitor
from scrapy_extension.storage.strategies.base import StorageStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.base import StorageBackend

#: Default flush threshold (items) — chosen to match the common "100 items per
#: batch" rule of thumb and to keep the docstring / factory default in sync.
DEFAULT_BATCH_THRESHOLD = 100

logger = logging.getLogger(__name__)

_BufferedEntry = tuple["StorageBackend", str, bytes, int | None]


class BatchedStorageStrategy(StorageStrategy):
  """Buffer items and flush to the backend in batches.

  Attributes:
      threshold: Number of buffered items that triggers an automatic flush.
      max_buffer_age_s: Risk 2 — age cap (seconds) beyond which a background
          flush fires, bounding the crash-before-flush loss window. ``None``
          disables it (flush only on threshold / close).
      pending: Count of items currently buffered (not yet flushed).
  """

  emits_store_events = True

  def __init__(
    self,
    threshold: int = DEFAULT_BATCH_THRESHOLD,
    *,
    max_buffer_age_s: float | None = None,
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
        monitor: Optional observability monitor. When ``None`` (default)
            :class:`~scrapy_extension.monitor.base.NullMonitor`. Emits
            ``on_buffer_depth(len(buffer))`` after each buffered item so a
            wired collector can alert before the loss window grows.

    Raises:
        ValueError: If ``threshold`` is less than 1, or a configured
            ``max_buffer_age_s`` is not positive.
    """
    # R21-D: NaN bypasses plain comparison guards (nan < 1 and nan <= 0 are both
    # False) — reject non-finite values explicitly. Mirrors delay.py
    # _require_finite / time_wheel.py _finite_number.
    if isinstance(threshold, bool) or not isinstance(threshold, int) or not math.isfinite(threshold):
      msg = f"threshold must be a finite int >= 1, got {threshold!r}"
      raise ValueError(msg)
    if threshold < 1:
      msg = f"threshold must be >= 1, got {threshold}"
      raise ValueError(msg)
    if max_buffer_age_s is not None and (
      not math.isfinite(max_buffer_age_s) or max_buffer_age_s <= 0
    ):
      msg = f"max_buffer_age_s must be > 0 (and finite), got {max_buffer_age_s!r}"
      raise ValueError(msg)
    self.threshold = threshold
    self.max_buffer_age_s = max_buffer_age_s
    self._buffer: list[_BufferedEntry] = []
    self._lock = threading.Lock()
    # Serializes the complete snapshot/write/requeue transaction. The buffer
    # lock alone cannot make close wait for a threshold flush after that flush
    # has detached its batch and started writing outside _lock.
    self._flush_lock = threading.Lock()
    self._closed = False
    self._monitor: Monitor = monitor if monitor is not None else NullMonitor()
    # Risk 2: oldest-buffered-item timestamp (monotonic) + flusher lifecycle.
    # ``_oldest_ts`` is None whenever the buffer is empty; set on first append
    # after a drain, reset to None under the lock whenever the buffer empties.
    self._oldest_ts: float | None = None
    self._flusher: threading.Thread | None = None
    self._stop = threading.Event()

  @property
  def pending(self) -> int:
    """Number of items currently buffered (thread-safe snapshot)."""
    with self._lock:
      return len(self._buffer)

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
    try:
      self._monitor.on_buffer_depth(depth)
    except Exception:  # noqa: BLE001 - monitor must never crash storage
      logger.debug("on_buffer_depth hook raised", exc_info=True)

  def _emit_error(self, operation: str, error: Exception) -> None:
    """Publish an error without allowing telemetry to stop retry processing."""
    try:
      self._monitor.on_error(operation, error)
    except Exception:  # noqa: BLE001 - monitor must never crash storage
      logger.debug("on_error hook raised", exc_info=True)

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
      self._buffer.append((storage_backend, key, value, ttl))
      if self._oldest_ts is None:
        self._oldest_ts = time.monotonic()
      depth = len(self._buffer)
      if depth >= self.threshold:
        flush_now = True
    # on_buffer_depth is a no-op under NullMonitor; emit outside the lock and
    # guard it so a misbehaving monitor can never crash the store path
    # (matches the BLE001-guard convention used across the codebase).
    self._emit_buffer_depth(depth)
    self._ensure_flusher()
    if flush_now:
      self._flush()

  def flush(self) -> None:
    """Flush buffered items to their per-entry backends in insertion order.

    The exact backend from every ``store`` call travels with that entry through
    explicit, threshold, age, close, and retry drains. No-op when empty.
    """
    self._flush()

  def close(self) -> None:
    """Flush remaining buffered items, then release resources.

    Stops the age-based flusher (Risk 2) and joins it (bounded) before
    draining, so ``BackendPipeline.close_spider`` does not tear down the
    backend connection while the daemon flusher is mid-``store``.
    """
    with self._lock:
      self._closed = True
    self._stop.set()
    flusher = self._flusher
    if flusher is not None and flusher.is_alive():
      flusher.join(timeout=5.0)
      if flusher.is_alive():
        logger.warning(
          "batched-storage-age-flush did not exit within 5.0s; "
          "in-flight items may be lost"
        )
    self.flush()

  def _flush(self) -> None:
    """Drain the buffer to each entry's backend in insertion order.

    At-least-once under partial failure: the buffer is snapshotted and cleared
    under the lock, then each item is written through the exact backend
    capability retained with it. If ``backend.store`` raises on item N, the
    un-written backend-bound tail (items N..end) is prepended back into
    ``_buffer`` under the lock and the exception is re-raised so the caller
    knows the flush was partial. The previously snapshotted items (already
    written) are not re-added — the tail carries only what was not yet
    attempted.

    Note: this protects against store *exceptions*; a process *crash* before
    the flush completes still loses the in-flight batch (documented at module
    level) — that is a separate failure mode requiring durable buffering.
    Risk 2's ``max_buffer_age_s`` bounds (but does not eliminate) that window.
    """
    with self._flush_lock:
      self._flush_serialized()

  def _flush_serialized(self) -> None:
    """Run one flush while the caller owns ``_flush_lock``."""
    with self._lock:
      batch = list(self._buffer)
      self._buffer = []
      self._oldest_ts = None  # buffer drained; age resets on next append
    for i, (storage_backend, key, value, ttl) in enumerate(batch):
      try:
        storage_backend.store(key, value, ttl=ttl)
      except Exception:
        # Re-enqueue the un-written tail (this item + remaining) so the next
        # flush retries them. At-least-once: no silent loss.
        tail = batch[i:]
        with self._lock:
          # New items may have been appended between the snapshot and the
          # failure — preserve them by extending the tail with current buffer.
          tail.extend(self._buffer)
          self._buffer = tail
          # Re-enqueued tail's oldest is approximately now (per-item
          # timestamps aren't tracked) — conservative for the age-flusher so
          # it gives the retried tail a fresh age budget.
          if self._buffer:
            self._oldest_ts = time.monotonic()
          requeued_depth = len(self._buffer)
        logger.warning(
          "batched flush partial: %d/%d items written, %d re-enqueued",
          i,
          len(batch),
          len(batch) - i,
        )
        self._emit_buffer_depth(requeued_depth)
        raise
      try:
        self._monitor.on_store(key)
      except Exception:  # noqa: BLE001 - persistence already succeeded
        logger.debug("on_store hook raised", exc_info=True)
    if batch:
      with self._lock:
        remaining_depth = len(self._buffer)
      self._emit_buffer_depth(remaining_depth)

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
      flusher.start()

  def _age_flush_loop(self) -> None:
    """Periodically flush when the oldest buffered item exceeds the age cap.

    Bounds the crash-before-flush loss window to roughly ``max_buffer_age_s``:
    the loop wakes on the age interval and flushes if the oldest item is older
    than the cap. Uses ``_stop.wait(timeout=...)`` so :meth:`close` unblocks it
    immediately on shutdown. All flush work goes through ``_flush``
    (lock-guarded) so it composes safely with the store-path threshold flush.
    A transient flush failure is logged and the loop continues so a temporary
    outage does not permanently disable the flusher.
    """
    age = self.max_buffer_age_s
    if age is None:  # defensive — _ensure_flusher should have checked
      return
    while not self._stop.wait(timeout=age):
      with self._lock:
        need_flush = (
          bool(self._buffer)
          and self._oldest_ts is not None
          and (time.monotonic() - self._oldest_ts) >= age
        )
      if need_flush:
        try:
          self._flush()
        except Exception as exc:  # noqa: BLE001 — keep retry loop alive
          # the loop alive so a transient outage doesn't disable the flusher
          # and report a failure that has no synchronous pipeline caller.
          self._emit_error("store", exc)
          logger.warning(
            "age-based flush failed; will retry next cycle (loss window "
            "may grow until the backend recovers)"
          )
