"""Membership-filter strategies for pluggable dedup (subsystem ①).

Exports the strategy interface and all concrete filters. The
:class:`~scrapy_extension.dupefilter.filters.factory.DedupeStrategy` enum and
``build_membership_filter`` factory live in the ``factory`` submodule.
"""

from __future__ import annotations

__all__ = [
  "BloomMembershipFilter",
  "CuckooMembershipFilter",
  "MembershipFilter",
  "MemoryMembershipFilter",
  "SetMembershipFilter",
]

from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter
