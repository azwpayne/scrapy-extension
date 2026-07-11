"""Factory selecting a storage strategy from a name (subsystem ③ Tier-2).

Mirrors :mod:`scrapy_extension.queue.strategies.factory`: maps a
:class:`StorageStrategyType` to a concrete :class:`StorageStrategy`. Used by
``BackendPipeline.from_settings`` so item-persistence semantics are chosen via
``SCRAPY_STORAGE_STRATEGY`` with no code change.
"""

from __future__ import annotations

__all__ = ["StorageStrategyType", "create_storage_strategy"]

from enum import Enum
from typing import Any

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.storage.strategies.base import StorageStrategy
from scrapy_extension.storage.strategies.batched import (
  DEFAULT_BATCH_THRESHOLD,
  BatchedStorageStrategy,
)
from scrapy_extension.storage.strategies.passthrough import (
  PassthroughStorageStrategy,
)


class StorageStrategyType(str, Enum):
  """Selectable storage strategies.

  Attributes:
      PASSTHROUGH: Default — delegates to StorageBackend unchanged (byte-identical
          to the pre-strategy BackendPipeline behavior).
      BATCHED: Buffers items and flushes in bulk at a threshold / on close.
  """

  PASSTHROUGH = "passthrough"
  BATCHED = "batched"

  @classmethod
  def _missing_(cls, value: object) -> StorageStrategyType:
    valid = ", ".join(repr(m.value) for m in cls)
    raise ValueError(f"{value!r} is not a valid {cls.__name__}. Valid: {valid}.")


def create_storage_strategy(name: str, **opts: Any) -> StorageStrategy:
  """Build the storage strategy for ``name``.

  Args:
      name: Strategy name (``"passthrough"`` or ``"batched"``). Case-insensitive
          via :class:`StorageStrategyType` lookup.
      **opts: Strategy-specific options. ``BatchedStorageStrategy`` accepts
          ``threshold`` (int, default 100); passthrough accepts none.

  Returns:
      A concrete StorageStrategy instance.

  Raises:
      ConfigurationError: If ``name`` is not a known storage strategy.
  """
  try:
    strategy_type = StorageStrategyType(name)
  except ValueError as e:
    msg = (
      f"Unknown storage strategy: {name!r}. Valid: "
      f"{', '.join(repr(m.value) for m in StorageStrategyType)}."
    )
    raise ConfigurationError(
      msg, setting_name="storage_strategy", setting_value=name
    ) from e

  if strategy_type is StorageStrategyType.PASSTHROUGH:
    return PassthroughStorageStrategy()
  if strategy_type is StorageStrategyType.BATCHED:
    threshold_raw = opts.get("threshold", DEFAULT_BATCH_THRESHOLD)
    threshold = threshold_raw if isinstance(threshold_raw, int) else int(threshold_raw)
    # Risk 2: thread max_buffer_age_s + monitor through to the strategy so
    # the crash-before-flush loss window can be bounded from settings.
    kwargs: dict[str, Any] = {"threshold": threshold}
    max_buffer_age_s = opts.get("max_buffer_age_s")
    if max_buffer_age_s is not None:
      kwargs["max_buffer_age_s"] = max_buffer_age_s
    monitor = opts.get("monitor")
    if monitor is not None:
      kwargs["monitor"] = monitor
    return BatchedStorageStrategy(**kwargs)
  raise ConfigurationError(  # pragma: no cover
    f"Unknown storage strategy: {strategy_type!r}"
  )
