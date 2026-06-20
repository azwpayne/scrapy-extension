"""Factory selecting a membership-filter strategy from settings (subsystem ①).

Maps a :class:`DedupeStrategy` (plus strategy-specific options) to a concrete
:class:`MembershipFilter`. Used by ``BackendDupeFilter.from_settings`` so the
dedup backend is chosen via ``SCRAPY_DEDUP_STRATEGY`` with no code change.
"""

from __future__ import annotations

__all__ = ["DedupeStrategy", "build_membership_filter"]

from enum import Enum
from typing import TYPE_CHECKING

from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter
from scrapy_extension.exceptions import ConfigurationError

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


class DedupeStrategy(str, Enum):
  """Selectable dedup strategies.

  Attributes:
      SET: Exact, cross-worker, backend-backed (default).
      MEMORY: Exact, in-process, optional LRU cap.
      BLOOM: Probabilistic, in-process, no deletion.
      CUCKOO: Probabilistic, in-process, supports deletion.
  """

  SET = "set"
  MEMORY = "memory"
  BLOOM = "bloom"
  CUCKOO = "cuckoo"

  @classmethod
  def _missing_(cls, value: object) -> DedupeStrategy:
    valid = ", ".join(repr(m.value) for m in cls)
    raise ValueError(f"{value!r} is not a valid {cls.__name__}. Valid: {valid}.")


def build_membership_filter(
  strategy: DedupeStrategy,
  connection_manager: ConnectionManager,
  *,
  key: str = "dupefilter",
  memory_maxsize: int | None = None,
  bloom_capacity: int = 1_000_000,
  bloom_error_rate: float = 0.001,
  cuckoo_capacity: int = 1_000_000,
  cuckoo_error_rate: float = 0.001,
) -> MembershipFilter:
  """Build the membership filter for ``strategy``.

  Args:
      strategy: Which dedup strategy to instantiate.
      connection_manager: Connection manager (used only by the ``set``
          strategy; in-memory strategies ignore it).
      key: Backend set name for the ``set`` strategy.
      memory_maxsize: Optional LRU cap for the ``memory`` strategy.
      bloom_capacity: Expected item count for the ``bloom`` strategy.
      bloom_error_rate: Target false-positive rate for ``bloom``.
      cuckoo_capacity: Expected item count for the ``cuckoo`` strategy.
      cuckoo_error_rate: Target false-positive rate for ``cuckoo``.

  Returns:
      A concrete MembershipFilter instance.

  Raises:
      ConfigurationError: If ``strategy`` is not a known DedupeStrategy.
  """
  if strategy is DedupeStrategy.SET:
    return SetMembershipFilter(connection_manager, key)
  if strategy is DedupeStrategy.MEMORY:
    return MemoryMembershipFilter(maxsize=memory_maxsize)
  if strategy is DedupeStrategy.BLOOM:
    return BloomMembershipFilter(
      capacity=bloom_capacity, error_rate=bloom_error_rate
    )
  if strategy is DedupeStrategy.CUCKOO:
    return CuckooMembershipFilter(
      capacity=cuckoo_capacity, error_rate=cuckoo_error_rate
    )
  raise ConfigurationError(f"Unknown dedup strategy: {strategy!r}")  # pragma: no cover
