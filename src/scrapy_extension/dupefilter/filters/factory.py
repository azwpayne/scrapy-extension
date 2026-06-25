"""Factory selecting a membership-filter strategy from settings (subsystem ①).

Maps a :class:`DedupeStrategy` (plus strategy-specific options) to a concrete
:class:`MembershipFilter`. Used by ``BackendDupeFilter.from_settings`` so the
dedup backend is chosen via ``SCRAPY_DEDUP_STRATEGY`` with no code change.
"""

from __future__ import annotations

__all__ = ["DedupeStrategy", "build_membership_filter"]

import logging
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

logger = logging.getLogger(__name__)

# Module-level cache so the warning fires once per process per strategy even
# when many dupefilters are constructed (multi-spider process). Tests reset
# this to verify the warn-once contract from a clean slate.
_warned: set[DedupeStrategy] = set()


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


# Per-process strategies whose state is invisible to other workers. The library
# markets itself as distributed, so selecting one of these silently degrades the
# cross-worker dedup contract. The factory warns once per process per strategy
# (idempotent) to surface the limitation at selection time — class docstrings
# alone were not enough (INSIGHTS-2026-06-25 Theme C).
_PER_PROCESS_STRATEGIES: frozenset[DedupeStrategy] = frozenset(
  {DedupeStrategy.MEMORY, DedupeStrategy.BLOOM, DedupeStrategy.CUCKOO}
)


def _warn_per_process_scope(strategy: DedupeStrategy) -> None:
  """Emit a one-time per-process warning when ``strategy`` is per-process.

  Bloom / cuckoo / memory filters live in-process: cross-worker duplicates
  pass silently. Operators assuming distributed dedup need a loud signal at
  selection time. Idempotent via the module-level ``_warned`` set so a
  multi-spider process does not spam the log.

  Args:
      strategy: The selected dedup strategy.
  """
  if strategy not in _PER_PROCESS_STRATEGIES:
    return
  if strategy in _warned:
    return
  _warned.add(strategy)
  logger.warning(
    "Dedup strategy %r is per-process — its state is not shared across "
    "workers, so cross-worker duplicate requests will pass undetected. "
    "For cross-worker dedup, use the default 'set' strategy (or a "
    "MemoryMembershipFilter backed by a shared backend).",
    strategy.value,
  )


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
  _warn_per_process_scope(strategy)
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
