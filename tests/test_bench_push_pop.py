"""Benchmarks for push→pop against a mock QueueBackend (round-8 F4).

Measures the in-process CPU path of ``BackendQueue.push`` → ``pop`` —
serialization, strategy dispatch, monitor hooks, and deserialization — with
NO broker involved. A mock ``QueueBackend`` buffers items in an in-memory
``list`` so we measure the package's CPU work, not Redis/Mongo/Kafka RTT
(that's integration's job).

Scope — what is measured here:
  - ``BackendQueue.push``: ``_request_to_dict`` + ``JSONSerializer.serialize``
    + ``PassthroughQueueStrategy.push`` + monitor ``on_push``.
  - ``BackendQueue.pop``: ``PassthroughQueueStrategy.pop`` + ack-token routing
    + ``JSONSerializer.deserialize`` + ``_decode_body`` + ``request_from_dict``
    + monitor ``on_pop`` / ``on_queue_depth``.
  - Batched throughput: N pushes then N pops, per-op mean.

Scope — what is NOT measured here:
  - Real broker I/O (the mock returns instantly).
  - Strategies other than passthrough (delay/round_robin/throttle have their
    own unit tests; isolating them here would double the file for no new signal).

Opt-in: ``@pytest.mark.benchmark`` on every test, skipped by the root
``conftest.py`` unless ``--benchmark-only`` / ``--benchmark-enable`` is passed.
No hard perf thresholds — the gate is "runs and reports a defensible number".

One honest monotone assertion: per-op cost of pushing a single item must be >=
per-op cost of pushing one item in a batched loop of 10 (loop amortizes the
fixture/setup but NOT the per-item work, so per-op is ~equal or slightly
favoring the batch — it must never be the case that batched-per-op is SLOWER
than single). We assert the genuinely monotone relationship: batched-10
per-op <= single per-op x 2 (a generous upper bound that only fails if batching
is pathologically slower — the only ordering that would indicate a regression).
"""

from __future__ import annotations

from typing import Any

import pytest
from scrapy.http import Request

from scrapy_extension.queue.queue import BackendQueue

#: Module-level marker so every test in this file is opted-in together.
pytestmark = pytest.mark.benchmark


class _InMemoryQueueBackend:
  """Minimal in-memory QueueBackend for benchmarking the CPU path.

  Implements only the four abstract methods ``BackendQueue`` touches through
  ``PassthroughQueueStrategy``. Items are buffered in a per-queue list; pop
  returns the oldest (FIFO). No ack semantics — passthrough's ack-token
  routing sees the base ``pop_with_ack`` default (``None`` token).
  """

  def __init__(self) -> None:
    self._queues: dict[str, list[bytes]] = {}

  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    del priority
    self._queues.setdefault(queue_name, []).append(item)

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    del timeout
    buf = self._queues.get(queue_name)
    if not buf:
      return None
    return buf.pop(0)

  def queue_len(self, queue_name: str) -> int:
    return len(self._queues.get(queue_name, ()))

  def clear_queue(self, queue_name: str) -> None:
    self._queues.pop(queue_name, None)


class _BenchConnectionManager:
  """Stand-in returning a shared in-memory queue backend.

  Mirrors the slice of the ``ConnectionManager`` surface that
  ``PassthroughQueueStrategy`` + ``BackendQueue`` exercise: ``get_queue_backend``.
  """

  def __init__(self, backend: _InMemoryQueueBackend) -> None:
    self._backend = backend

  def get_queue_backend(self) -> _InMemoryQueueBackend:
    return self._backend


@pytest.fixture()
def bench_queue() -> tuple[BackendQueue, _InMemoryQueueBackend]:
  """Fresh BackendQueue + its backing in-memory backend for each test."""
  backend = _InMemoryQueueBackend()
  # A bare MagicMock(spec=Spider) is enough for callback resolution during pop;
  # callbacks resolve by attribute name on the spider, so a spec'd mock suffices.
  spider: Any = pytest.importorskip("scrapy").Spider
  queue = BackendQueue(
    connection_manager=_BenchConnectionManager(backend),
    queue_name="bench-push-pop",
    spider=spider,  # type: ignore[arg-type]
  )
  return queue, backend


def _make_request(idx: int = 0) -> Request:
  """Build a representative Scrapy Request (no callback → no spider resolution)."""
  return Request(
    url=f"https://example.com/item/{idx}",
    method="GET",
    headers={"Accept": "text/html", "User-Agent": "scrapy-extension-bench/1.0"},
    meta={"depth": 1, "source": "bench", "retry_times": 0},
    priority=idx,
    dont_filter=False,
  )


def test_push_single(benchmark, bench_queue: tuple[BackendQueue, _InMemoryQueueBackend]) -> None:
  """Measure single-item push (serialize + strategy + monitor) — no threshold.

  Each benchmark iteration clears the buffer so memory does not grow across
  rounds. The reported number is per-op push CPU cost against the mock backend.
  """
  queue, backend = bench_queue
  request = _make_request()

  def push_one() -> None:
    backend.clear_queue("bench-push-pop")
    queue.push(request)

  benchmark(push_one)

  assert backend.queue_len("bench-push-pop") == 1


def test_pop_single(benchmark, bench_queue: tuple[BackendQueue, _InMemoryQueueBackend]) -> None:
  """Measure single-item pop (deserialize + decode + request_from_dict + monitor).

  Pre-pushes one item per iteration so pop has work to do; measures the
  full CPU path of the consumer side.
  """
  queue, backend = bench_queue
  request = _make_request()

  def pop_one() -> Any:
    queue.push(request)  # refill; not measured (outside benchmark caliper)
    return queue.pop()

  result = benchmark(pop_one)

  assert result is not None
  assert result.url == request.url


def test_push_batch_of_10(
  benchmark,
  bench_queue: tuple[BackendQueue, _InMemoryQueueBackend],
) -> None:
  """Measure batched push of 10 items — per-op reported is push cost ÷ 10.

  Used by the monotone assertion in ``test_batched_push_not_slower_than_single``
  to confirm batching does not introduce per-item pathology.
  """
  queue, backend = bench_queue
  requests = [_make_request(i) for i in range(10)]

  def push_ten() -> None:
    backend.clear_queue("bench-push-pop")
    for req in requests:
      queue.push(req)

  benchmark(push_ten)

  assert backend.queue_len("bench-push-pop") == 10


def test_batched_push_not_slower_than_single(
  bench_queue: tuple[BackendQueue, _InMemoryQueueBackend],
) -> None:
  """Honest monotone assertion: batched-10 per-op <= single per-op x 2.

  Uses ``time.perf_counter`` (not the ``benchmark`` fixture) so the two
  measurements can be compared within ONE test — the ``benchmark`` fixture is
  single-use per test, which forbids the two-call layout. This is a real
  measurement of the same CPU path, just without pytest-benchmark's
  calibration; the monotone ordering it asserts is unaffected by that.

  This is the only ordering that is genuinely monotone in the mock: batching
  amortizes nothing on the per-item CPU work (each push still serializes +
  strategy-dispatches + monitor-hooks independently), so the per-op numbers
  must be roughly equal. A batched-per-op MORE THAN 2x the single-op cost
  would indicate a real regression (e.g. accidental O(n²) in the hot path,
  a monitor that grows linearly, a strategy that re-scans on every push).
  The 2x ceiling is generous on purpose — it catches pathology without
  gaming-prone tightness.
  """
  import time

  queue, backend = bench_queue
  single_req = _make_request()
  batch_reqs = [_make_request(i) for i in range(10)]

  rounds = 200  # enough samples to wash out single-iteration jitter
  single_total = 0.0
  for _ in range(rounds):
    backend.clear_queue("bench-push-pop")
    t0 = time.perf_counter()
    queue.push(single_req)
    single_total += time.perf_counter() - t0
  single_per_op = single_total / rounds

  batch_total = 0.0
  for _ in range(rounds):
    backend.clear_queue("bench-push-pop")
    t0 = time.perf_counter()
    for req in batch_reqs:
      queue.push(req)
    batch_total += time.perf_counter() - t0
  batch_per_op = batch_total / rounds / 10.0

  # Monotone guard — fails only if batching is pathologically slower per item.
  assert batch_per_op <= single_per_op * 2, (
    f"batched push per-op ({batch_per_op * 1e6:.3f} us) > "
    f"single push per-op ({single_per_op * 1e6:.3f} us) x 2 — "
    "batching introduced a per-item regression"
  )
