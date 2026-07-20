"""Tests for RoundRobinQueueStrategy (subsystem ②)."""

from __future__ import annotations

import base64
import json
import threading
from collections import OrderedDict, deque

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from scrapy_extension.queue.strategies.factory import (
  QueueStrategyType,
  build_queue_strategy,
)
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy


class _ObservedLock:
  """Lock that signals when one named competitor attempts acquisition."""

  def __init__(self, attempted: threading.Event, thread_name: str) -> None:
    self._lock = threading.Lock()
    self._attempted = attempted
    self._thread_name = thread_name

  def __enter__(self):  # type: ignore[no-untyped-def]
    if threading.current_thread().name == self._thread_name:
      self._attempted.set()
    self._lock.acquire()
    return self

  def __exit__(self, *args):  # type: ignore[no-untyped-def]
    self._lock.release()


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

  # ----- R14-F: drained sources must be evicted, not leaked -----

  def test_drained_source_is_evicted_from_sources(self) -> None:
    """R14-F HIGH: a fully-drained source key MUST be removed from
    ``_sources`` (not left as an empty deque).

    Regression guard for the empty-source leak: pre-fix the strategy kept
    drained-source keys around forever (``_idx`` rotated through empty
    slots), making ``_sources`` unbounded on a long crawl with transient
    sources AND making every pop O(n) in the total number of sources ever
    seen. Eviction on drain caps the state at the live source set.
    """
    s = RoundRobinQueueStrategy(object())
    s.push("q", b"a", source="ephemeral")
    assert "ephemeral" in s._sources
    assert s.pop("q") == b"a"
    # Drain complete → key must be evicted, not left as an empty deque.
    assert "ephemeral" not in s._sources, (
      "drained source 'ephemeral' leaked into _sources "
      "(R14-F HIGH regression — unbounded growth under source churn)"
    )

  def test_drain_then_repush_recreates_source(self) -> None:
    """R14-F: after eviction, a new push to the same source name recreates
    it cleanly (no stale state, rotation index still sound)."""
    s = RoundRobinQueueStrategy(object())
    s.push("q", b"a", source="A")
    assert s.pop("q") == b"a"
    assert "A" not in s._sources  # evicted on drain
    # Repush recreates the source — must work without any stale-residue bug.
    s.push("q", b"a2", source="A")
    assert "A" in s._sources
    assert s.pop("q") == b"a2"

  def test_sources_bounded_under_source_churn(self) -> None:
    """R14-F HIGH: under sustained source churn (new source, push, drain,
    repeat), ``_sources`` must stay bounded at the *live* source count —
    not accumulate one entry per source name ever seen.

    This is the operational form of the leak: a long crawl with transient
    per-batch sources would otherwise grow ``_sources`` without limit and
    slow every pop to O(n) in historical-source count.
    """
    s = RoundRobinQueueStrategy(object())
    for i in range(1000):
      src = f"batch-{i}"
      s.push("q", b"x", source=src)
      s.pop("q")  # drain immediately → source should evict
    # All 1000 sources drained → none should remain.
    assert len(s._sources) == 0, (
      f"_sources leaked {len(s._sources)} drained entries under churn "
      "(R14-F HIGH — unbounded growth under transient sources)"
    )

  def test_rotation_continues_after_mid_rotation_drain(
    self, mock_connection_manager
  ) -> None:
    """R14-F: when a source drains mid-rotation, the next pop serves the
    next source in rotation (not the same one twice, not a KeyError).

    Drains an "A" entry while B and C still have items, then verifies B
    and C are both served before any further A would be (A is empty now).
    """
    s = RoundRobinQueueStrategy(mock_connection_manager)
    s.push("q", b"a1", source="A")
    s.push("q", b"b1", source="B")
    s.push("q", b"c1", source="C")
    first = s.pop("q")  # serves A → A drains + evicts
    assert first == b"a1"
    # Next two pops must serve B and C (in rotation order), not error.
    second = s.pop("q")
    third = s.pop("q")
    assert {second, third} == {b"b1", b"c1"}
    assert s.pop("q") is None  # all drained

  def test_pop_does_not_materialize_all_source_keys(self) -> None:
    """A pop must stay O(1) in live-source count rather than copying keys."""

    class NoKeyIterationOrderedDict(OrderedDict[str, deque[bytes]]):
      def __iter__(self):  # type: ignore[no-untyped-def]
        raise AssertionError("pop materialized every source key")

    s = RoundRobinQueueStrategy(object())
    guarded = NoKeyIterationOrderedDict()
    guarded["A"] = deque([b"a1", b"a2"])
    guarded["B"] = deque([b"b1"])
    s._sources = guarded

    assert s.pop("q") == b"a1"

  def test_snapshot_round_trip_preserves_items_and_next_source(self) -> None:
    """Restart recovery must retain FIFO contents and the fairness cursor."""
    source = RoundRobinQueueStrategy(object())
    source.push("q", b"a1", source="A")
    source.push("q", b"a2", source="A")
    source.push("q", b"b1", source="B")
    source.push("q", b"b2", source="B")
    assert source.pop("q") == b"a1"  # next source must now be B

    state = source.snapshot()
    assert state is not None
    restored = RoundRobinQueueStrategy(object())
    restored.restore(state)

    assert [restored.pop("q") for _ in range(3)] == [b"b1", b"a2", b"b2"]
    assert restored.pop("q") is None

  def test_empty_snapshot_and_empty_restore_are_noops(self) -> None:
    strategy = RoundRobinQueueStrategy(object())

    assert strategy.snapshot() is None
    strategy.restore(None)
    strategy.restore(b"")
    assert strategy.queue_len("q") == 0

  @pytest.mark.parametrize(
    "state",
    [
      b"\xff",
      b"{not-json",
      json.dumps({"version": 2, "strategy": "round_robin", "sources": []}).encode(),
      json.dumps({"version": 1, "strategy": "other", "sources": []}).encode(),
      json.dumps({"version": 1, "strategy": "round_robin", "sources": {}}).encode(),
    ],
  )
  def test_invalid_snapshot_preserves_live_state(self, state: bytes) -> None:
    strategy = RoundRobinQueueStrategy(object())
    strategy.push("q", b"live", source="live")

    strategy.restore(state)

    assert strategy.pop("q") == b"live"

  def test_restore_skips_malformed_duplicate_and_empty_entries(self) -> None:
    valid = base64.b64encode(b"valid").decode("ascii")
    duplicate = base64.b64encode(b"duplicate").decode("ascii")
    state = json.dumps(
      {
        "version": 1,
        "strategy": "round_robin",
        "sources": [
          None,
          {"source": 7, "items": []},
          {"source": "A", "items": ["not-base64!", valid]},
          {"source": "A", "items": [duplicate]},
          {"source": "empty", "items": []},
        ],
      }
    ).encode()

    strategy = RoundRobinQueueStrategy(object())
    strategy.restore(state)

    assert strategy.pop("q") == b"valid"
    assert strategy.pop("q") is None


class TestRoundRobinConcurrency:
  def test_concurrent_pushes_to_new_source_do_not_lose_an_item(self) -> None:
    first_get_entered = threading.Event()
    second_push_reached = threading.Event()

    class CoordinatedGetDict(OrderedDict[str, deque[bytes]]):
      def __init__(self) -> None:
        super().__init__()
        self._get_calls = 0
        self._coordination_lock = threading.Lock()

      def get(
        self, key: str, default: deque[bytes] | None = None
      ) -> deque[bytes] | None:
        value = super().get(key, default)
        with self._coordination_lock:
          self._get_calls += 1
          call_number = self._get_calls
        if call_number == 1:
          first_get_entered.set()
          if not second_push_reached.wait(timeout=2.0):
            raise AssertionError("second push did not reach the shared source")
        elif call_number == 2:
          second_push_reached.set()
        return value

    strategy = RoundRobinQueueStrategy(object())
    strategy._sources = CoordinatedGetDict()
    strategy._lock = _ObservedLock(second_push_reached, "second-push")  # type: ignore[assignment]
    errors: list[Exception] = []

    def push(item: bytes) -> None:
      try:
        strategy.push("q", item, source="shared")
      except Exception as exc:
        errors.append(exc)

    first = threading.Thread(target=push, args=(b"first",), daemon=True)
    second = threading.Thread(
      target=push,
      args=(b"second",),
      name="second-push",
      daemon=True,
    )
    first.start()
    assert first_get_entered.wait(timeout=2.0)
    second.start()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert strategy.queue_len("q") == 2
    assert {strategy.pop("q"), strategy.pop("q")} == {b"first", b"second"}

  def test_pop_and_clear_are_atomic_against_each_other(self) -> None:
    pop_entered = threading.Event()
    release_pop = threading.Event()
    clear_called = threading.Event()

    class BlockingPopDeque(deque[bytes]):
      def popleft(self) -> bytes:
        pop_entered.set()
        if not release_pop.wait(timeout=2.0):
          raise AssertionError("pop was not released")
        return super().popleft()

    class ObservableClearDict(OrderedDict[str, deque[bytes]]):
      def clear(self) -> None:
        clear_called.set()
        super().clear()

    strategy = RoundRobinQueueStrategy(object())
    sources = ObservableClearDict()
    sources["A"] = BlockingPopDeque([b"item"])
    strategy._sources = sources
    strategy._lock = _ObservedLock(clear_called, "clear-thread")  # type: ignore[assignment]
    pop_results: list[bytes | None] = []
    errors: list[Exception] = []

    def pop() -> None:
      try:
        pop_results.append(strategy.pop("q"))
      except Exception as exc:
        errors.append(exc)

    def clear() -> None:
      try:
        strategy.clear("q")
      except Exception as exc:
        errors.append(exc)

    pop_thread = threading.Thread(target=pop, daemon=True)
    clear_thread = threading.Thread(target=clear, name="clear-thread", daemon=True)
    pop_thread.start()
    assert pop_entered.wait(timeout=2.0)
    clear_thread.start()
    assert clear_called.wait(timeout=2.0)
    release_pop.set()
    pop_thread.join(timeout=2.0)
    clear_thread.join(timeout=2.0)

    assert not pop_thread.is_alive()
    assert not clear_thread.is_alive()
    assert errors == []
    assert pop_results == [b"item"]
    assert clear_called.is_set()
    assert strategy.queue_len("q") == 0

  def test_queue_len_is_consistent_during_new_source_push(self) -> None:
    len_entered = threading.Event()
    release_len = threading.Event()
    push_reached = threading.Event()

    class ObservableGetDict(OrderedDict[str, deque[bytes]]):
      def get(
        self, key: str, default: deque[bytes] | None = None
      ) -> deque[bytes] | None:
        if key == "C":
          push_reached.set()
        return super().get(key, default)

    class BlockingLenDeque(deque[bytes]):
      def __len__(self) -> int:
        len_entered.set()
        if not release_len.wait(timeout=2.0):
          raise AssertionError("queue_len was not released")
        return super().__len__()

    strategy = RoundRobinQueueStrategy(object())
    sources = ObservableGetDict()
    sources["A"] = BlockingLenDeque([b"a"])
    sources["B"] = deque([b"b"])
    strategy._sources = sources
    strategy._lock = _ObservedLock(push_reached, "push-thread")  # type: ignore[assignment]
    lengths: list[int] = []
    errors: list[Exception] = []

    def read_len() -> None:
      try:
        lengths.append(strategy.queue_len("q"))
      except Exception as exc:
        errors.append(exc)

    def push() -> None:
      try:
        strategy.push("q", b"c", source="C")
      except Exception as exc:
        errors.append(exc)

    len_thread = threading.Thread(target=read_len, daemon=True)
    push_thread = threading.Thread(target=push, name="push-thread", daemon=True)
    len_thread.start()
    assert len_entered.wait(timeout=2.0)
    push_thread.start()
    assert push_reached.wait(timeout=2.0)
    release_len.set()
    len_thread.join(timeout=2.0)
    push_thread.join(timeout=2.0)

    assert not len_thread.is_alive()
    assert not push_thread.is_alive()
    assert errors == []
    assert lengths == [2]
    assert strategy.queue_len("q") == 3

  def test_snapshot_is_consistent_during_pop(self) -> None:
    snapshot_iterating = threading.Event()
    release_snapshot = threading.Event()
    pop_reached = threading.Event()

    class BlockingIterDeque(deque[bytes]):
      def __iter__(self):  # type: ignore[no-untyped-def]
        iterator = super().__iter__()
        first = True
        for item in iterator:
          if first:
            first = False
            snapshot_iterating.set()
            if not release_snapshot.wait(timeout=2.0):
              raise AssertionError("snapshot was not released")
          yield item

      def popleft(self) -> bytes:
        pop_reached.set()
        return super().popleft()

    strategy = RoundRobinQueueStrategy(object())
    strategy._sources["A"] = BlockingIterDeque([b"a1", b"a2"])
    strategy._sources["B"] = deque([b"b1"])
    strategy._lock = _ObservedLock(pop_reached, "pop-thread")  # type: ignore[assignment]
    snapshots: list[bytes | None] = []
    popped: list[bytes | None] = []
    errors: list[Exception] = []

    def snapshot() -> None:
      try:
        snapshots.append(strategy.snapshot())
      except Exception as exc:
        errors.append(exc)

    def pop() -> None:
      try:
        popped.append(strategy.pop("q"))
      except Exception as exc:
        errors.append(exc)

    snapshot_thread = threading.Thread(target=snapshot, daemon=True)
    pop_thread = threading.Thread(target=pop, name="pop-thread", daemon=True)
    snapshot_thread.start()
    assert snapshot_iterating.wait(timeout=2.0)
    pop_thread.start()
    assert pop_reached.wait(timeout=2.0)
    release_snapshot.set()
    snapshot_thread.join(timeout=2.0)
    pop_thread.join(timeout=2.0)

    assert not snapshot_thread.is_alive()
    assert not pop_thread.is_alive()
    assert errors == []
    assert popped == [b"a1"]
    assert len(snapshots) == 1
    assert snapshots[0] is not None
    data = json.loads(snapshots[0].decode())
    assert [entry["source"] for entry in data["sources"]] == ["A", "B"]

  def test_snapshot_encodes_outside_state_lock(self, monkeypatch) -> None:
    strategy = RoundRobinQueueStrategy(object())
    strategy.push("q", b"a", source="A")
    encode_entered = threading.Event()
    release_encode = threading.Event()
    push_done = threading.Event()
    original_encode = base64.b64encode

    def blocking_encode(item: bytes) -> bytes:
      encode_entered.set()
      if not release_encode.wait(timeout=2.0):
        raise AssertionError("snapshot encoding was not released")
      return original_encode(item)

    monkeypatch.setattr(
      "scrapy_extension.queue.strategies.round_robin.base64.b64encode",
      blocking_encode,
    )
    snapshot_thread = threading.Thread(target=strategy.snapshot, daemon=True)
    push_thread = threading.Thread(
      target=lambda: (strategy.push("q", b"b", source="B"), push_done.set()),
      daemon=True,
    )
    snapshot_thread.start()
    assert encode_entered.wait(timeout=2.0)
    push_thread.start()
    try:
      assert push_done.wait(timeout=2.0), "base64 encoding held the state lock"
    finally:
      release_encode.set()
      snapshot_thread.join(timeout=2.0)
      push_thread.join(timeout=2.0)

    assert not snapshot_thread.is_alive()
    assert not push_thread.is_alive()

  def test_restore_decodes_before_atomic_state_replacement(self, monkeypatch) -> None:
    strategy = RoundRobinQueueStrategy(object())
    strategy.push("q", b"old", source="old")
    encoded = base64.b64encode(b"restored").decode("ascii")
    state = json.dumps(
      {
        "version": 1,
        "strategy": "round_robin",
        "sources": [{"source": "new", "items": [encoded]}],
      }
    ).encode()
    decode_entered = threading.Event()
    release_decode = threading.Event()
    push_done = threading.Event()
    original_decode = base64.b64decode

    def blocking_decode(item, *, validate=False):  # type: ignore[no-untyped-def]
      decode_entered.set()
      if not release_decode.wait(timeout=2.0):
        raise AssertionError("restore decoding was not released")
      return original_decode(item, validate=validate)

    monkeypatch.setattr(
      "scrapy_extension.queue.strategies.round_robin.base64.b64decode",
      blocking_decode,
    )
    restore_thread = threading.Thread(
      target=strategy.restore, args=(state,), daemon=True
    )
    push_thread = threading.Thread(
      target=lambda: (
        strategy.push("q", b"during-parse", source="old"),
        push_done.set(),
      ),
      daemon=True,
    )
    restore_thread.start()
    assert decode_entered.wait(timeout=2.0)
    push_thread.start()
    try:
      assert push_done.wait(timeout=2.0), "base64 decoding held the state lock"
    finally:
      release_decode.set()
      restore_thread.join(timeout=2.0)
      push_thread.join(timeout=2.0)

    assert not restore_thread.is_alive()
    assert not push_thread.is_alive()
    assert strategy.pop("q") == b"restored"
    assert strategy.pop("q") is None


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
            f"(drain prefix {drained[: sum(served) + 0]})"
          )

    # Sanity: every source was served exactly its allotted count.
    for src_idx in range(n_sources):
      assert served[src_idx] == counts[src_idx]


def test_pop_evicts_lingering_empty_deque_defensive(mock_connection_manager):
  """Safety-net characterization: the ``pop`` loop's defensive branch
  evicts a deque that is empty at the cursor — a state the
  eviction-on-drain invariant says should never exist, but the branch
  guards against a regression in that invariant (so ``pop`` terminates
  instead of spinning on an empty slot).

  Tested by directly injecting the invariant-violating state (an empty
  deque placed at the cursor ahead of a live source). This is the
  standard way to exercise defensive code: simulate the failure it
  guards against and assert the net catches it. Not a normal-path test."""
  s = RoundRobinQueueStrategy(mock_connection_manager)
  # Insert the impossible state FIRST so the cursor (idx=0) lands on it.
  s._sources["ghost"] = deque()
  s.push("q", b"alive", source="alive")
  # pop: rotation = ["ghost", "alive"], idx=0 -> "ghost" is empty ->
  #   defensive branch evicts "ghost", retries -> "alive" -> returns b"alive".
  assert s.pop("q") == b"alive"
  assert "ghost" not in s._sources
  assert "alive" not in s._sources  # drained on pop -> evicted by invariant
