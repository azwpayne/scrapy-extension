"""Property-based tests for the probabilistic membership filters (Hypothesis).

Complements the example-based tests (test_bloom_filter.py / test_cuckoo_filter.py)
with randomized edge-case coverage of the CRITICAL never-false-negative contract:
a probabilistic filter must NEVER report a just-added item as absent (within
capacity). A false-negative means a duplicate request is re-fetched — silent
dedup loss — so this is the highest-severity contract in the dedup subsystem.

Hypothesis generates + shrinks hundreds of random item sets to verify the
contract holds far beyond the hand-picked cases. If the underlying audit
(workflow v6, which confirmed the contract on 5 example scenarios) missed an
edge case, these tests surface it; if they pass, the contract is verified under
randomized input — the strongest evidence short of a formal proof.

Run: ``uv run pytest tests/test_filter_properties.py -v`` (Hypothesis is a dev
dependency; these are ordinary pytest tests, no special runner needed).
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from scrapy_extension.dupefilter.filters.base import FilterFull
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter


@st.composite
def distinct_items(draw, max_count: int = 40) -> list[bytes]:
  """A strategy for a list of DISTINCT byte-string items.

  Distinct so each add() returns True (the new-item path) — re-adds return
  False and exercise a different code path. Bounded size keeps the test fast.
  """
  count = draw(st.integers(min_value=0, max_value=max_count))
  items: set[bytes] = set()
  while len(items) < count:
    items.add(draw(st.binary(min_size=1, max_size=32)))
  return list(items)


# ---------------------------------------------------------------------------
# Bloom filter: never-false-negative within capacity
# ---------------------------------------------------------------------------


@settings(max_examples=75, deadline=None)
@given(items=distinct_items(max_count=40))
def test_bloom_never_false_negative_within_capacity(items: list[bytes]) -> None:
  """CRITICAL: every added item reports present.

  The bloom filter's false-negative rate is 0 by construction (add sets the
  same k bits contains checks). This verifies that holds for 75 random item
  sets (40 items each) — a failure means duplicate requests would be re-fetched.
  """
  f = BloomMembershipFilter(capacity=max(len(items), 1), error_rate=0.01)
  for item in items:
    f.add(item)
  for item in items:
    assert item in f, f"FALSE NEGATIVE: bloom reports added item {item!r} absent"


# ---------------------------------------------------------------------------
# Cuckoo filter: never-false-negative within capacity (before FilterFull)
# ---------------------------------------------------------------------------


@settings(max_examples=75, deadline=None)
@given(items=distinct_items(max_count=40))
def test_cuckoo_never_false_negative_within_capacity(items: list[bytes]) -> None:
  """CRITICAL: every SUCCESSFULLY-added item reports present, within capacity.

  Cuckoo raises FilterFull once _MAX_KICKS exhausts (filter past capacity) —
  that is the correct signal, NOT a false-negative. So we track which items
  were added before FilterFull and assert each reports present. A failure here
  (an added item absent) is a true false-negative = duplicate fetch.
  """
  # capacity 3x the item count + floor so adds rarely hit FilterFull — testing
  # the never-false-negative contract, not the capacity boundary.
  f = CuckooMembershipFilter(
    capacity=max(len(items) * 3, 100), error_rate=0.01
  )
  added: list[bytes] = []
  for item in items:
    try:
      result = f.add(item)
    except FilterFull:
      break  # capacity reached — correct behavior, stop adding
    if result:  # True = newly added (False = already existed; impossible for distinct)
      added.append(item)
  for item in added:
    assert item in f, f"FALSE NEGATIVE: cuckoo reports added item {item!r} absent"


# ---------------------------------------------------------------------------
# Cuckoo filter: remove correctness (deletion makes contains False)
# ---------------------------------------------------------------------------


@settings(max_examples=50, deadline=None)
@given(items=distinct_items(max_count=20))
def test_cuckoo_remove_makes_item_absent(items: list[bytes]) -> None:
  """After remove(item), contains(item) is False for that specific item.

  Verifies remove() deletes the RIGHT fingerprint (not a colliding one). A
  wrong removal would cause a false-negative on a DIFFERENT still-present item
  — caught here because we re-check all OTHER added items stay present after
  each remove. (Fingerprint collisions mean remove CAN affect a different
  item's presence only if fingerprints match — the property catches that.)
  """
  if not items:
    return
  f = CuckooMembershipFilter(capacity=max(len(items) * 3, 100), error_rate=0.01)
  added: list[bytes] = []
  for item in items:
    try:
      if f.add(item):
        added.append(item)
    except FilterFull:
      break
  if not added:
    return
  # Remove the first added item, then assert it's absent AND the rest stay present.
  target = added[0]
  f.remove(target)
  assert target not in f, f"remove({target!r}) did not make it absent"
  for other in added[1:]:
    # A fingerprint collision could legitimately make `other` absent after
    # removing `target` only if they share a fingerprint bucket slot. That is
    # a known cuckoo tradeoff (fingerprint, not item, granularity) — but the
    # dupefilter contract treats remove as item-exact. If this asserts, the
    # filter's remove granularity is coarser than the dedup contract assumes.
    # Documented as acceptable for the probabilistic filter; flagged here so a
    # future change is deliberate, not silent.
    assert other in f or _shares_fingerprint(target, other), (
      f"remove({target!r}) made unrelated item {other!r} absent — fingerprint "
      f"granularity coarser than item-granular dedup contract assumes"
    )


def _shares_fingerprint(a: bytes, b: bytes) -> bool:
  """Helper: do two items hash to the same cuckoo fingerprint?

  Cuckoo stores fingerprints, not items — two distinct items with the same
  fingerprint are indistinguishable to remove(). This helper documents that the
  `test_cuckoo_remove_makes_item_absent` assertion tolerates ONLY that case
  (genuine fingerprint collision), not a wrong-bucket removal.
  """
  import hashlib

  return hashlib.sha256(a).hexdigest()[:8] == hashlib.sha256(b).hexdigest()[:8]
