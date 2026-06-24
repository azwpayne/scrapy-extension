"""Storage-semantics strategies for pluggable item-persistence types (subsystem ③).

Exports the strategy interface and concrete strategies. The
:class:`~scrapy_extension.storage.strategies.factory.StorageStrategyType` enum
and ``create_storage_strategy`` factory live in the ``factory`` submodule.
"""

from __future__ import annotations

__all__ = [
  "BatchedStorageStrategy",
  "PassthroughStorageStrategy",
  "StorageStrategy",
  "StorageStrategyType",
  "create_storage_strategy",
]

from scrapy_extension.storage.strategies.base import StorageStrategy
from scrapy_extension.storage.strategies.batched import BatchedStorageStrategy
from scrapy_extension.storage.strategies.factory import (
  StorageStrategyType,
  create_storage_strategy,
)
from scrapy_extension.storage.strategies.passthrough import (
  PassthroughStorageStrategy,
)
