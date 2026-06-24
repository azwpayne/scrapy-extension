"""Passthrough storage strategy — delegates to StorageBackend unchanged.

The default strategy. Byte-identical to ``BackendPipeline``'s pre-strategy
behavior (same ``store`` call, same key, same TTL), preserving backward
compatibility.
"""

from __future__ import annotations

__all__ = ["PassthroughStorageStrategy"]

from typing import TYPE_CHECKING

from scrapy_extension.storage.strategies.base import StorageStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.base import StorageBackend


class PassthroughStorageStrategy(StorageStrategy):
  """Each ``store`` call passes straight through to the StorageBackend.

  This preserves the exact pre-strategy ``BackendPipeline`` behavior, so it is
  the default and is fully backward-compatible.
  """

  def store(
    self,
    storage_backend: StorageBackend,
    key: str,
    value: bytes,
    ttl: int | None = None,
  ) -> None:
    """Store straight to the backend.

    Args:
        storage_backend: The StorageBackend to delegate to.
        key: The storage key.
        value: The serialized item bytes.
        ttl: Optional time-to-live in seconds.
    """
    storage_backend.store(key, value, ttl=ttl)

  def flush(self) -> None:
    """No-op — passthrough never buffers."""

  def close(self) -> None:
    """No-op — passthrough holds no resources."""
