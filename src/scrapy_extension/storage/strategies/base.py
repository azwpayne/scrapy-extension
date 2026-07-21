"""Abstract storage-strategy interface for pluggable item-persistence semantics.

Defines :class:`StorageStrategy` — the strategy interface that
:class:`~scrapy_extension.pipeline.pipeline.BackendPipeline` delegates item
storage to, so persistence semantics (passthrough, batched, ...) are pluggable
without changing the backend interface or the item-serialization layer.

Mirrors the shape of :class:`~scrapy_extension.queue.strategies.base.QueueStrategy`.
The strategy is backend-agnostic: it receives the ``StorageBackend`` on each
``store`` call (the pipeline owns the backend / connection manager). A
buffering strategy must preserve that per-entry backend affinity until the
entry is written; retaining a capability does not transfer lifecycle ownership.
"""

from __future__ import annotations

__all__ = ["StorageStrategy"]

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from scrapy_extension.backends.base import StorageBackend


class StorageStrategy(ABC):
  """Strategy interface for item-storage semantics.

  A strategy owns how serialized items are persisted: direct writes
  (passthrough), buffered batched writes, etc. It is backend-agnostic — each
  ``store`` call receives the ``StorageBackend`` to delegate to, so the
  pipeline retains ownership of the backend lifecycle. Buffered entries remain
  bound to the exact backend capability supplied with their call.
  """

  #: True when the strategy emits ``Monitor.on_store`` at its actual durable
  #: write boundary. Buffering strategies override this so the pipeline does
  #: not report volatile acceptance as persistence.
  emits_store_events = False

  @abstractmethod
  def store(
    self,
    storage_backend: StorageBackend,
    key: str,
    value: bytes,
    ttl: int | None = None,
  ) -> None:
    """Persist one serialized item via the given backend.

    Args:
        storage_backend: The exact StorageBackend capability to delegate this
            item to. It remains owned by the caller.
        key: The storage key.
        value: The serialized item bytes.
        ttl: Optional time-to-live in seconds.
    """

  @abstractmethod
  def flush(self) -> None:
    """Flush any buffered items to their backend. No-op for non-buffering strategies."""

  @abstractmethod
  def close(self) -> None:
    """Release resources and flush any remaining buffered items."""

  def open(self) -> None:  # noqa: B027
    """Lifecycle hook — prepare the strategy. Default no-op."""
