"""Scale checks that execute the production ``MemoryMembershipFilter`` class.

Unlike the neighboring reference-model files, these tests import production
membership-filter code. They provide evidence for the in-process exact-memory
strategy only: no shared set backend, broker, queue, or multi-process behavior
is exercised.
"""

from __future__ import annotations

from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter

PRODUCTION_MEMORY_ITEM_COUNT = 10_000


class TestMemoryMembershipFilterScale:
    """Exercise production exact-memory membership behavior at 10k entries."""

    def test_tracks_10k_unique_fingerprints(self) -> None:
        """Track all unique values and reject every duplicate without count drift."""
        membership = MemoryMembershipFilter()
        fingerprints = [f"fp-{i}".encode() for i in range(PRODUCTION_MEMORY_ITEM_COUNT)]

        new_count = sum(
            1 for fingerprint in fingerprints if membership.add(fingerprint)
        )

        assert new_count == PRODUCTION_MEMORY_ITEM_COUNT
        assert len(membership) == PRODUCTION_MEMORY_ITEM_COUNT
        for fingerprint in fingerprints:
            assert membership.add(fingerprint) is False
            assert fingerprint in membership
        assert len(membership) == PRODUCTION_MEMORY_ITEM_COUNT

    def test_clear_resets_10k_entries(self) -> None:
        """Clear production in-process state and permit a former value again."""
        membership = MemoryMembershipFilter()
        for i in range(PRODUCTION_MEMORY_ITEM_COUNT):
            membership.add(f"fp-{i}".encode())
        assert len(membership) == PRODUCTION_MEMORY_ITEM_COUNT

        membership.clear()

        assert len(membership) == 0
        assert membership.add(b"fp-0") is True
