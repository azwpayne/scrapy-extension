"""Storage strategy layer for scrapy-extension (subsystem ③ Tier-2).

Re-exports the storage-strategy ABC, concrete strategies, and the factory so
the pipeline can pick a persistence strategy via ``SCRAPY_STORAGE_STRATEGY``.
This closes the dedup/queue/storage strategy asymmetry: each subsystem now has
its own pluggable strategy layer.
"""

from __future__ import annotations

__all__ = [
  "BatchedStorageStrategy",
  "PassthroughStorageStrategy",
  "StorageStrategy",
  "StorageStrategyType",
  "create_storage_strategy",
]

from scrapy_extension.storage.strategies import (
  BatchedStorageStrategy,
  PassthroughStorageStrategy,
  StorageStrategy,
  StorageStrategyType,
  create_storage_strategy,
)
