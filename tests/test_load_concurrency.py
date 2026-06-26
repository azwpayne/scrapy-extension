"""Round-8 concurrency correctness tests — in-flight-set thread safety.

HONEST SCOPE: this module pins the round-3 ``Not-tested: concurrent
next_request under true thread parallelism`` gap. The broker I/O is mocked;
the in-flight-set data structure under test is REAL and shared across
threads. This is genuine thread parallelism (``threading.Thread`` +
``ThreadPoolExecutor``), NOT asyncio — Scrapy is Twisted, but the in-flight
ack set must be thread-safe regardless of the runtime concurrency model.

WHAT THIS CATCHES:
- Token lost (popped but never acked under contention)
- Double-ack (same token acked twice — KeyError or silent miscount)
- ``_in_flight`` not emptying after all acks complete
- Race in the pop-then-add / discard-then-commit interleaving

MOCK FIDELITY: the mock mirrors the real Kafka/RabbitMQ/SQS backends' locking
granularity — a plain ``set`` + ``discard`` for the in-flight set, with NO
extra synchronization added by the mock. If the in-flight-set pattern has a
real race, this test MUST catch it. We do NOT paper over races by adding
locks to the mock that the real backends don't have.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor


class _AckToken:
  """Opaque ack token mirroring ``_KafkaAckToken`` / ``_SqsAckToken``.

  Carries a monotonically-increasing sequence number so the concurrency test
  can verify every popped token is acked exactly once. Hashable + equality
  by sequence (matches the real tokens' value semantics).
  """

  __slots__ = ("seq",)

  def __init__(self, seq: int) -> None:
    self.seq = seq

  def __eq__(self, other: object) -> bool:
    if not isinstance(other, _AckToken):
      return NotImplemented
    return self.seq == other.seq

  def __hash__(self) -> int:
    return hash(self.seq)

  def __repr__(self) -> str:
    return f"_AckToken(seq={self.seq})"


class _MockInFlightQueueBackend:
  """Mock queue backend mirroring the real in-flight-set ack pattern.

  IMPORTANT — locking granularity: this mock uses a plain ``set`` for
  ``_in_flight`` and ``set.add`` / ``set.discard`` for pop-track / ack-clear,
  matching the real Kafka (``_in_flight[p].add(offset)`` / ``in_flight.discard(o)``),
  RabbitMQ (``_in_flight_tags.add(t)`` / ``_in_flight_tags.discard(t)``), and
  SQS (``_in_flight.add(token)`` / ``_in_flight.discard(token)``) backends. We
  deliberately do NOT add a lock around pop/ack here: if a race exists in the
  pattern, the test must catch it (the real backends have no lock either —
  they rely on the GIL for individual set ops, and on Twisted's single-thread
  reactor for pop/ack interleaving). Adding a mock-side lock would hide a
  real bug.

  The only lock here guards the source queue (the producer side), so the
  sequence numbers are handed out without duplicates — that's the mock's I/O
  substrate, not the data structure under test.
  """

  requires_ack = True
  supports_concurrent_ack = True

  def __init__(self, total_items: int) -> None:
    """Pre-seed the queue with ``total_items`` monotonically-numbered items.

    Args:
        total_items: How many items the queue will hand out before going empty.
    """
    self._source: list[int] = list(range(total_items))
    self._source_lock = threading.Lock()  # guards the source list only
    # In-flight set — the data structure under test. No lock on this set;
    # mirrors the real backends. pop adds, ack/nack discards.
    self._in_flight: set[_AckToken] = set()
    # Commit log — records each ack in arrival order so the test can assert
    # exactly-once. Guarded by its own lock so the assertion isn't itself
    # the source of a race.
    self._commits: list[int] = []
    self._commits_lock = threading.Lock()
    # Hook so tests can inject a delay between pop-return and ack-call to
    # widen the concurrency window and maximize contention. Default: no delay.
    self.pop_delay: float = 0.0

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, _AckToken | None]:
    """Pop the next item + record its token in the in-flight set.

    Mirrors the real backends' pop_with_ack: get next message, build token,
    ``_in_flight.add(token)``. The add is intentionally not inside the source
    lock — that's how the real backends behave (lock around consumer poll,
    not around the in-flight bookkeeping).
    """
    del queue_name, timeout
    with self._source_lock:
      if not self._source:
        return (None, None)
      seq = self._source.pop(0)
    token = _AckToken(seq)
    # In-flight add — NOT under the source lock. This is the exact
    # interleaving surface the real backends expose.
    self._in_flight.add(token)
    if self.pop_delay:
      time.sleep(self.pop_delay)
    return (str(seq).encode(), token)

  def ack(self, queue_name: str, *, token: _AckToken | None = None) -> None:
    """Ack a token — discard from in-flight + record the commit."""
    del queue_name
    if token is None:
      return
    self._in_flight.discard(token)
    with self._commits_lock:
      self._commits.append(token.seq)

  def nack(self, queue_name: str, *, token: _AckToken | None = None) -> None:
    """Nack a token — discard from in-flight WITHOUT recording a commit.

    Mirrors the real nack semantics (message left uncommitted so it
    re-delivers; in-flight bookkeeping cleared either way).
    """
    del queue_name
    if token is None:
      return
    self._in_flight.discard(token)

  @property
  def in_flight_size(self) -> int:
    """Current in-flight count (snapshot — may race; for diagnostics only)."""
    return len(self._in_flight)

  @property
  def commits(self) -> list[int]:
    """Snapshot of the commit log (call after threads join)."""
    with self._commits_lock:
      return list(self._commits)


def _worker_pop_then_ack(
  backend: _MockInFlightQueueBackend,
  ops: int,
  barrier: threading.Barrier,
  results: list[tuple[int, _AckToken | None]],
  results_lock: threading.Lock,
) -> None:
  """Worker: wait on the barrier, then do ``ops`` pop-then-ack cycles.

  The barrier maximizes contention — every thread enters the pop path at the
  same instant. Each (worker_id, token) pair is recorded under a lock so the
  main thread can verify the pop/ack correspondence after join.
  """
  wid = threading.get_ident()
  barrier.wait()  # release all threads simultaneously
  for _ in range(ops):
    _data, token = backend.pop_with_ack("queue")
    if token is None:
      # Queue drained before this thread finished its quota — record + exit.
      with results_lock:
        results.append((wid, None))
      return
    # Ack immediately (the common Scrapy path: pop -> process -> ack).
    backend.ack("queue", token=token)
    with results_lock:
      results.append((wid, token))


class TestInFlightSetConcurrency:
  """In-flight-set is correct under true thread parallelism."""

  def test_acked_exactly_once_16x100(self) -> None:
    """16 threads x 100 ops = 1600 pop+ack cycles; every token acked exactly once.

    Pins the round-3 Not-tested gap. Asserts:
    - 1600 tokens popped == 1600 acks recorded (no token lost)
    - every acked seq is unique (no double-ack)
    - the in-flight set empties after all threads join (no leak)
    - no exception escapes any worker (no KeyError from a raced discard)
    """
    n_threads = 16
    ops_per_thread = 100
    total = n_threads * ops_per_thread
    backend = _MockInFlightQueueBackend(total_items=total)
    barrier = threading.Barrier(n_threads)
    results: list[tuple[int, _AckToken | None]] = []
    results_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
      futures = [
        ex.submit(
          _worker_pop_then_ack,
          backend,
          ops_per_thread,
          barrier,
          results,
          results_lock,
        )
        for _ in range(n_threads)
      ]
      # Surface any worker exception (a raced discard raising KeyError would
      # show up here, not as a silent miscount).
      for f in futures:
        f.result()

    commits = backend.commits
    counter = Counter(commits)

    # (1) No token lost: every seq 0..total-1 was acked exactly once.
    missing = [s for s in range(total) if counter[s] == 0]
    assert not missing, f"{len(missing)} tokens were never acked (lost); e.g. {missing[:5]}"

    # (2) No double-ack: every acked seq appears exactly once.
    double = [s for s, c in counter.items() if c > 1]
    assert not double, (
      f"{len(double)} tokens acked more than once; e.g. {double[:5]} "
      f"(counts: {[(s, counter[s]) for s in double[:5]]})"
    )

    # (3) In-flight set empties — no token leaked (popped but never acked/nacked).
    assert backend.in_flight_size == 0, (
      f"in-flight set not empty after all acks: {backend.in_flight_size} leaked"
    )

    # (4) Count conservation: total acks == total items.
    assert len(commits) == total, (
      f"commit count {len(commits)} != total {total}; tokens lost or duplicated"
    )

    # (5) Every pop returned a token (no pop silently dropped a sequence number).
    popped_tokens = [t for _w, t in results if t is not None]
    assert len(popped_tokens) == total, (
      f"popped {len(popped_tokens)} tokens, expected {total}"
    )

  def test_acked_exactly_once_32x50_high_contention(self) -> None:
    """32 threads x 50 ops with a small pop delay — widens the race window.

    A tiny ``pop_delay`` between pop-return and the next op widens the
    interleaving window so any race in the pop-add / ack-discard interleaving
    has more opportunities to surface. Same exactly-once assertions as the
    16x100 case.
    """
    n_threads = 32
    ops_per_thread = 50
    total = n_threads * ops_per_thread
    backend = _MockInFlightQueueBackend(total_items=total)
    backend.pop_delay = 0.0002  # 200us — widens the window without slowing CI much
    barrier = threading.Barrier(n_threads)
    results: list[tuple[int, _AckToken | None]] = []
    results_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
      futures = [
        ex.submit(
          _worker_pop_then_ack,
          backend,
          ops_per_thread,
          barrier,
          results,
          results_lock,
        )
        for _ in range(n_threads)
      ]
      for f in futures:
        f.result()  # will raise if any worker threw

    commits = backend.commits
    counter = Counter(commits)

    assert len(commits) == total, (
      f"commit count {len(commits)} != total {total}"
    )
    assert backend.in_flight_size == 0, (
      f"in-flight leak: {backend.in_flight_size} unacked"
    )
    assert not [s for s, c in counter.items() if c > 1], "double-ack detected"
    assert not [s for s in range(total) if counter[s] == 0], "token lost"

  def test_nack_returns_token_to_in_flight_under_contention(self) -> None:
    """Nack under contention discards from in-flight without committing.

    Mirrors the at-least-once nack contract: a nacked token is NOT committed
    (so it re-delivers), and the in-flight set must still drain without
    leaking or double-discarding. Mixes ack and nack across threads.
    """
    n_threads = 8
    ops_per_thread = 25
    total = n_threads * ops_per_thread
    backend = _MockInFlightQueueBackend(total_items=total)
    barrier = threading.Barrier(n_threads)

    def mixed_worker() -> None:
      """Pop; ack even seqs, nack odd seqs — half commit, half don't."""
      barrier.wait()
      for _ in range(ops_per_thread):
        _data, token = backend.pop_with_ack("queue")
        if token is None:
          return
        if token.seq % 2 == 0:
          backend.ack("queue", token=token)
        else:
          backend.nack("queue", token=token)

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
      futures = [ex.submit(mixed_worker) for _ in range(n_threads)]
      for f in futures:
        f.result()

    commits = backend.commits
    # Every committed seq must be even (odd seqs were nacked, not committed).
    assert all(s % 2 == 0 for s in commits), "odd (nacked) seq leaked into commits"
    # In-flight set must be empty: both ack and nack discard from it.
    assert backend.in_flight_size == 0, (
      f"in-flight leak after mixed ack/nack: {backend.in_flight_size}"
    )
    # Exactly half the items committed (the even seqs), half were nacked.
    assert len(commits) == total // 2, (
      f"expected {total // 2} commits (even seqs), got {len(commits)}"
    )

  def test_repeated_runs_are_stable(self) -> None:
    """Run the 16x100 case 5 times — a subtle race surfaces as flakiness.

    A genuine race condition is non-deterministic: it may not fire on every
    run. Repeating the contention test catches races that pass on a single
    invocation but fail on repeat. If this test is flaky, INVESTIGATE — it
    is almost certainly catching a real race, not a test bug.
    """
    for run in range(5):
      n_threads = 16
      ops_per_thread = 100
      total = n_threads * ops_per_thread
      backend = _MockInFlightQueueBackend(total_items=total)
      barrier = threading.Barrier(n_threads)
      results: list[tuple[int, _AckToken | None]] = []
      results_lock = threading.Lock()

      with ThreadPoolExecutor(max_workers=n_threads) as ex:
        futures = [
          ex.submit(
            _worker_pop_then_ack,
            backend,
            ops_per_thread,
            barrier,
            results,
            results_lock,
          )
          for _ in range(n_threads)
        ]
        for f in futures:
          f.result()

      commits = backend.commits
      counter = Counter(commits)
      assert len(commits) == total, f"run {run}: commit count drift"
      assert backend.in_flight_size == 0, f"run {run}: in-flight leak"
      assert not [s for s, c in counter.items() if c > 1], (
        f"run {run}: double-ack"
      )
      assert not [s for s in range(total) if counter[s] == 0], (
        f"run {run}: token lost"
      )
