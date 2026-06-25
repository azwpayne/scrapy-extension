"""Batched storage strategy — buffers items and flushes in bulk.

Buffers ``(key, value, ttl)`` triples and writes them to the backend when the
buffer reaches a configurable threshold or on ``close()``. Reduces per-item
backend round-trips at the cost of delayed persistence (items are lost on
crash before flush — a distinct failure mode from a store *exception*, which
is handled with at-least-once re-enqueueing; see :meth:`_flush_to`).
Thread-safe via an internal lock — Scrapy pipelines are single-threaded per
spider, but the guard makes the strategy safe under concurrent stores (e.g.
concurrent item-processing pipelines feeding one shared strategy).
"""

from __future__ import annotations

__all__ = ["BatchedStorageStrategy"]

import logging
import threading
from typing import TYPE_CHECKING

from scrapy_extension.storage.strategies.base import StorageStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.base import StorageBackend

#: Default flush threshold (items) — chosen to match the common "100 items per
#: batch" rule of thumb and to keep the docstring / factory default in sync.
DEFAULT_BATCH_THRESHOLD = 100

logger = logging.getLogger(__name__)


class BatchedStorageStrategy(StorageStrategy):
  """Buffer items and flush to the backend in batches.

  Attributes:
      threshold: Number of buffered items that triggers an automatic flush.
      pending: Count of items currently buffered (not yet flushed).
  """

  def __init__(self, threshold: int = DEFAULT_BATCH_THRESHOLD) -> None:
    """Initialize the batched strategy.

    Args:
        threshold: Buffer size that triggers an automatic flush. Must be >= 1.

    Raises:
        ValueError: If ``threshold`` is less than 1.
    """
    if threshold < 1:
      msg = f"threshold must be >= 1, got {threshold}"
      raise ValueError(msg)
    self.threshold = threshold
    self._buffer: list[tuple[str, bytes, int | None]] = []
    self._lock = threading.Lock()
    self._last_backend: StorageBackend | None = None

  @property
  def pending(self) -> int:
    """Number of items currently buffered (thread-safe snapshot)."""
    with self._lock:
      return len(self._buffer)

  def store(
    self,
    storage_backend: StorageBackend,
    key: str,
    value: bytes,
    ttl: int | None = None,
  ) -> None:
    """Buffer one item; auto-flush when the buffer reaches the threshold.

    Args:
        storage_backend: The StorageBackend to flush to.
        key: The storage key.
        value: The serialized item bytes.
        ttl: Optional time-to-live in seconds.
    """
    flush_now = False
    with self._lock:
      self._buffer.append((key, value, ttl))
      self._last_backend = storage_backend
      if len(self._buffer) >= self.threshold:
        flush_now = True
    if flush_now:
      self._flush_to(storage_backend)

  def flush(self) -> None:
    """Flush any buffered items to the last-seen backend.

    The batched strategy records the backend from each ``store`` call so
    ``flush`` and ``close`` can drain without an explicit backend argument.
    No-op if no backend has been seen yet or the buffer is empty.
    """
    backend = self._last_backend
    if backend is not None:
      self._flush_to(backend)

  def close(self) -> None:
    """Flush any remaining buffered items, then release resources."""
    self.flush()

  def _flush_to(self, storage_backend: StorageBackend) -> None:
    """Drain the buffer, writing each item to the backend in insertion order.

    At-least-once under partial failure: the buffer is snapshotted and cleared
    under the lock, then each item is written outside the lock. If
    ``backend.store`` raises on item N, the un-written tail (items N..end) is
    prepended back into ``_buffer`` under the lock and the exception is
    re-raised so the caller knows the flush was partial. The previously
    snapshotted items (already written) are not re-added — the tail carries
    only what was not yet attempted.

    Note: this protects against store *exceptions*; a process *crash* before
    the flush completes still loses the in-flight batch (documented at module
    level) — that is a separate failure mode requiring durable buffering.
    """
    with self._lock:
      batch = list(self._buffer)
      self._buffer = []
      self._last_backend = storage_backend
    for i, (key, value, ttl) in enumerate(batch):
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
        logger.warning(
          "batched flush partial: %d/%d items written, %d re-enqueued",
          i,
          len(batch),
          len(batch) - i,
        )
        raise
