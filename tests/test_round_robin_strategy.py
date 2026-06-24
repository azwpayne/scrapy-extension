"""Tests for RoundRobinQueueStrategy (subsystem ②)."""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from scrapy_extension.queue.strategies.factory import (
  QueueStrategyType,
  build_queue_strategy,
)
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy


class TestRoundRobinQueueStrategy:
  def test_single_source_is_fifo(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a", source="x")
    s.push("q", b"b", source="x")
    assert s.pop("q") == b"a"
    assert s.pop("q") == b"b"
    assert s.pop("q") is None

  def test_round_robin_across_two_sources(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a1", source="A")
    s.push("q", b"a2", source="A")
    s.push("q", b"b1", source="B")
    s.push("q", b"b2", source="B")
    results = [s.pop("q") for _ in range(4)]
    # Fair interleaving: A, B, A, B (neither source served twice before the other)
    assert results == [b"a1", b"b1", b"a2", b"b2"]

  def test_no_source_starvation(self, mock_connection_manager) -> None:
    """A source with many items must not starve another source."""
    s = RoundRobinQueueStrategy(mock_connection_manager)
    for i in range(5):
      s.push("q", f"a{i}".encode(), source="A")
    s.push("q", b"b0", source="B")
    assert s.pop("q") == b"a0"
    assert s.pop("q") == b"b0"  # B served on the second pop despite A having 5

  def test_returns_none_when_all_empty(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a", source="A")
    s.pop("q")  # drains A
    assert s.pop("q") is None

  def test_default_source_when_unspecified(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"x")  # no source -> "default"
    assert s.pop("q") == b"x"

  def test_len_totals_all_sources(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a", source="A")
    s.push("q", b"b", source="A")
    s.push("q", b"c", source="B")
    assert s.queue_len("q") == 3

  def test_clear_empties_all_sources(self, mock_connection_manager) -> None:
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a", source="A")
    s.push("q", b"b", source="B")
    s.clear("q")
    assert s.queue_len("q") == 0
    assert s.pop("q") is None

  def test_reuses_source_after_drain(self, mock_connection_manager) -> None:
    """A source that drained can receive and serve new items."""
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a1", source="A")
    s.pop("q")  # A now empty
    s.push("q", b"a2", source="A")  # reuse same source
    assert s.pop("q") == b"a2"

  def test_state_not_shared_across_instances(self) -> None:
    """Per-process state: a second strategy instance sees nothing of the first.

    RoundRobinQueueStrategy holds items in-process (per-instance deques),
    NOT shared across workers. Pushing to instance A must not be visible to
    instance B — pin the non-sharing contract so regressions (e.g. moving
    state to a shared backend) are caught.
    """
    instance_a = RoundRobinQueueStrategy(object())
    instance_b = RoundRobinQueueStrategy(object())
    instance_a.push("q", b"x", source="A")
    instance_a.push("q", b"y", source="B")
    # Instance B observes zero items and cannot pop what A holds.
    assert instance_b.queue_len("q") == 0
    assert instance_b.pop("q") is None
    # Instance A still owns its own items.
    assert instance_a.queue_len("q") == 2


class TestFactoryRoundRobin:
  def test_build_round_robin(self, mock_connection_manager) -> None:
    s = build_queue_strategy(QueueStrategyType.ROUND_ROBIN, mock_connection_manager)
    assert isinstance(s, RoundRobinQueueStrategy)

  def test_invalid_strategy_string(self) -> None:
    with pytest.raises(ValueError, match="not a valid QueueStrategyType"):
      QueueStrategyType("bogus")


class TestRoundRobinFairnessProperty:
  """Hypothesis property tests for the round-robin "no starvation" invariant.

  Pins the claim in ``round_robin.py``'s docstring: "every non-empty source
  is served before any source is served twice." Stated as an output property:
  in the drained sequence, for every prefix, no source's count exceeds every
  other non-empty source's count by more than 1. Equivalently — no source is
  served ``k+1`` times before every other non-empty source has been served
  ``k`` times. 100 hypothesis-generated cases.
  """

  @given(
    counts=st.lists(
      st.integers(min_value=0, max_value=8),
      min_size=1,
      max_size=6,
    )
  )
  @settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
  )
  def test_no_source_starvation_property(self, counts: list[int]) -> None:
    """Full drain interleaves sources fairly — no source outpaces another by >1.

    Given N sources with arbitrary item counts (some possibly zero), a full
    drain yields an output order where, at every prefix, the max times any
    source has been served is at most one more than the min over sources that
    still have remaining items (the non-empty ones at that point in the drain).
    """
    strategy = RoundRobinQueueStrategy(object())
    n_sources = len(counts)
    # Seed each source with its allotted count of unique items. Items are
    # source-tagged so we can attribute each popped byte back to its source.
    expected_total = sum(counts)
    for src_idx in range(n_sources):
      source = f"src{src_idx}"
      for i in range(counts[src_idx]):
        # Encode source index into the item so we can recover it after pop.
        strategy.push("q", f"{src_idx}:{i}".encode(), source=source)

    drained: list[int] = []
    while len(drained) < expected_total:
      item = strategy.pop("q")
      assert item is not None, "pop returned None before all items drained"
      src_idx_str, _ = item.decode().split(":", 1)
      drained.append(int(src_idx_str))
    # Final pop must report empty.
    assert strategy.pop("q") is None

    # The fairness invariant: track per-source served counts over the drain
    # order. After each pop, the served-count of the just-served source must
    # not exceed the served-count of any source that still has pending items
    # by more than 1. (Pending = total allotted minus served so far.)
    served = [0] * n_sources
    for served_src in drained:
      served[served_src] += 1
      # For every OTHER source that still has pending items, its served count
      # must be >= served[served_src] - 1 (i.e. our just-served source did not
      # jump ahead by more than one full round).
      for other in range(n_sources):
        if other == served_src:
          continue
        pending_other = counts[other] - served[other]
        if pending_other > 0:
          assert served[served_src] - served[other] <= 1, (
            f"starvation: src{served_src} served {served[served_src]}x while "
            f"src{other} (still pending) served only {served[other]}x "
            f"(drain prefix {drained[:sum(served) + 0]})"
          )

    # Sanity: every source was served exactly its allotted count.
    for src_idx in range(n_sources):
      assert served[src_idx] == counts[src_idx]
