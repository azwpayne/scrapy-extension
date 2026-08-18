"""Threaded reference-model tests for token/in-flight bookkeeping.

These tests exercise only local classes defined in this module. They provide
**no production backend evidence** for Kafka, RabbitMQ, SQS, their SDK tokens,
or ``BackendQueue``. The model is inspired by their add/discard bookkeeping,
but it does not import or execute any production backend code and cannot prove
production locking, broker, retry, ack, or redelivery behavior.

Within that deliberately narrow model, real ``ThreadPoolExecutor`` contention
checks count conservation, duplicate commits, and cleanup of a shared Python
``set``. Failures diagnose the reference model; production concurrency claims
require backend-specific tests or live-broker integration evidence.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest

pytestmark = pytest.mark.reference_model


class _ReferenceAckToken:
    """Local sequence token used only by the reference model."""

    __slots__ = ("seq",)

    def __init__(self, seq: int) -> None:
        self.seq = seq

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _ReferenceAckToken):
            return NotImplemented
        return self.seq == other.seq

    def __hash__(self) -> int:
        return hash(self.seq)

    def __repr__(self) -> str:
        return f"_ReferenceAckToken(seq={self.seq})"


class _ReferenceInFlightQueue:
    """Local list/set token model; not a ``QueueBackend`` implementation.

    A source-list lock makes sequence allocation deterministic. The separate
    in-flight set intentionally uses bare ``add``/``discard`` so the tests can
    characterize that local Python model under thread contention. Production
    backend classes, SDK calls, and their synchronization are not exercised.
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
        # In-flight set under reference-model test. Pop adds; ack/nack discards.
        self._in_flight: set[_ReferenceAckToken] = set()
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
    ) -> tuple[bytes | None, _ReferenceAckToken | None]:
        """Pop one local item and record its reference token as in flight."""
        del queue_name, timeout
        with self._source_lock:
            if not self._source:
                return (None, None)
            seq = self._source.pop(0)
        token = _ReferenceAckToken(seq)
        # Deliberately outside the source lock to exercise the model's interleaving.
        self._in_flight.add(token)
        if self.pop_delay:
            time.sleep(self.pop_delay)
        return (str(seq).encode(), token)

    def ack(self, queue_name: str, *, token: _ReferenceAckToken | None = None) -> None:
        """Ack a token — discard from in-flight + record the commit."""
        del queue_name
        if token is None:
            return
        self._in_flight.discard(token)
        with self._commits_lock:
            self._commits.append(token.seq)

    def nack(self, queue_name: str, *, token: _ReferenceAckToken | None = None) -> None:
        """Discard a reference token without recording a model commit."""
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


def _reference_worker_pop_then_ack(
    model: _ReferenceInFlightQueue,
    ops: int,
    barrier: threading.Barrier,
    results: list[tuple[int, _ReferenceAckToken | None]],
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
        _data, token = model.pop_with_ack("queue")
        if token is None:
            # Queue drained before this thread finished its quota — record + exit.
            with results_lock:
                results.append((wid, None))
            return
        # Ack immediately (the common Scrapy path: pop -> process -> ack).
        model.ack("queue", token=token)
        with results_lock:
            results.append((wid, token))


class TestReferenceInFlightSetConcurrency:
    """Characterize the local in-flight set under true thread parallelism."""

    def test_reference_model_acked_exactly_once_16x100(self) -> None:
        """16 threads x 100 ops = 1600 pop+ack cycles; every token acked exactly once.

        Characterizes only the local model. Asserts:
        - 1600 tokens popped == 1600 acks recorded (no token lost)
        - every acked seq is unique (no double-ack)
        - the in-flight set empties after all threads join (no leak)
        - no exception escapes any worker (no KeyError from a raced discard)
        """
        n_threads = 16
        ops_per_thread = 100
        total = n_threads * ops_per_thread
        model = _ReferenceInFlightQueue(total_items=total)
        barrier = threading.Barrier(n_threads)
        results: list[tuple[int, _ReferenceAckToken | None]] = []
        results_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            futures = [
                ex.submit(
                    _reference_worker_pop_then_ack,
                    model,
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

        commits = model.commits
        counter = Counter(commits)

        # (1) No token lost: every seq 0..total-1 was acked exactly once.
        missing = [s for s in range(total) if counter[s] == 0]
        assert not missing, (
            f"{len(missing)} tokens were never acked (lost); e.g. {missing[:5]}"
        )

        # (2) No double-ack: every acked seq appears exactly once.
        double = [s for s, c in counter.items() if c > 1]
        assert not double, (
            f"{len(double)} tokens acked more than once; e.g. {double[:5]} "
            f"(counts: {[(s, counter[s]) for s in double[:5]]})"
        )

        # (3) In-flight set empties — no token leaked (popped but never acked/nacked).
        assert model.in_flight_size == 0, (
            f"in-flight set not empty after all acks: {model.in_flight_size} leaked"
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

    def test_reference_model_acked_exactly_once_32x50_high_contention(self) -> None:
        """32 threads x 50 ops with a small pop delay — widens the race window.

        A tiny ``pop_delay`` between pop-return and the next op widens the
        interleaving window so any race in the pop-add / ack-discard interleaving
        has more opportunities to surface. Same exactly-once assertions as the
        16x100 case.
        """
        n_threads = 32
        ops_per_thread = 50
        total = n_threads * ops_per_thread
        model = _ReferenceInFlightQueue(total_items=total)
        model.pop_delay = 0.0002  # 200us — widens the window without slowing CI much
        barrier = threading.Barrier(n_threads)
        results: list[tuple[int, _ReferenceAckToken | None]] = []
        results_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            futures = [
                ex.submit(
                    _reference_worker_pop_then_ack,
                    model,
                    ops_per_thread,
                    barrier,
                    results,
                    results_lock,
                )
                for _ in range(n_threads)
            ]
            for f in futures:
                f.result()  # will raise if any worker threw

        commits = model.commits
        counter = Counter(commits)

        assert len(commits) == total, f"commit count {len(commits)} != total {total}"
        assert model.in_flight_size == 0, (
            f"in-flight leak: {model.in_flight_size} unacked"
        )
        assert not [s for s, c in counter.items() if c > 1], "double-ack detected"
        assert not [s for s in range(total) if counter[s] == 0], "token lost"

    def test_reference_model_nack_clears_in_flight_under_contention(self) -> None:
        """Nack under contention discards from in-flight without committing.

        In this model a nacked token is not appended to the commit log, while
        both ack and nack discard in-flight bookkeeping. No redelivery occurs or
        is proved here. The test mixes model ack/nack operations across threads.
        """
        n_threads = 8
        ops_per_thread = 25
        total = n_threads * ops_per_thread
        model = _ReferenceInFlightQueue(total_items=total)
        barrier = threading.Barrier(n_threads)

        def mixed_worker() -> None:
            """Pop; ack even seqs, nack odd seqs — half commit, half don't."""
            barrier.wait()
            for _ in range(ops_per_thread):
                _data, token = model.pop_with_ack("queue")
                if token is None:
                    return
                if token.seq % 2 == 0:
                    model.ack("queue", token=token)
                else:
                    model.nack("queue", token=token)

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            futures = [ex.submit(mixed_worker) for _ in range(n_threads)]
            for f in futures:
                f.result()

        commits = model.commits
        # Every committed seq must be even (odd seqs were nacked, not committed).
        assert all(s % 2 == 0 for s in commits), "odd (nacked) seq leaked into commits"
        # In-flight set must be empty: both ack and nack discard from it.
        assert model.in_flight_size == 0, (
            f"in-flight leak after mixed ack/nack: {model.in_flight_size}"
        )
        # Exactly half the items committed (the even seqs), half were nacked.
        assert len(commits) == total // 2, (
            f"expected {total // 2} commits (even seqs), got {len(commits)}"
        )

    def test_reference_model_repeated_runs_are_stable(self) -> None:
        """Run the 16x100 case 5 times — a subtle race surfaces as flakiness.

        A race in this local model is non-deterministic and may not fire on every
        run. Repetition increases the chance of detecting reference-model drift;
        it still supplies no production backend concurrency evidence.
        """
        for run in range(5):
            n_threads = 16
            ops_per_thread = 100
            total = n_threads * ops_per_thread
            model = _ReferenceInFlightQueue(total_items=total)
            barrier = threading.Barrier(n_threads)
            results: list[tuple[int, _ReferenceAckToken | None]] = []
            results_lock = threading.Lock()

            with ThreadPoolExecutor(max_workers=n_threads) as ex:
                futures = [
                    ex.submit(
                        _reference_worker_pop_then_ack,
                        model,
                        ops_per_thread,
                        barrier,
                        results,
                        results_lock,
                    )
                    for _ in range(n_threads)
                ]
                for f in futures:
                    f.result()

            commits = model.commits
            counter = Counter(commits)
            assert len(commits) == total, f"run {run}: commit count drift"
            assert model.in_flight_size == 0, f"run {run}: in-flight leak"
            assert not [s for s, c in counter.items() if c > 1], (
                f"run {run}: double-ack"
            )
            assert not [s for s in range(total) if counter[s] == 0], (
                f"run {run}: token lost"
            )
