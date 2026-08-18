"""Benchmarks for the in-process ``BackendQueue`` push/pop path.

The in-memory backend excludes broker latency while retaining request
serialization, passthrough strategy dispatch, monitoring, deserialization, and
Scrapy request reconstruction. Benchmark setup is deliberately performed by
``benchmark.pedantic`` so queue clearing and pop refills are outside the timed
caliper.

Only the three measurements carry the ``benchmark`` marker. The deterministic
push/pop round-trip remains part of the ordinary suite. No performance
thresholds are asserted; reported values are baseline evidence only.
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from scrapy import Spider
from scrapy.http import Request

from scrapy_extension.backends.base import QueueBackend, _QueuePushReceipt
from scrapy_extension.queue.queue import BackendQueue

_QUEUE_NAME = "bench-push-pop"
_BENCHMARK_ROUNDS = 5
_BENCHMARK_SOURCE = "bench"


class _InMemoryQueueBackend(QueueBackend):
    """Minimal FIFO ``QueueBackend`` test double with atomic-pop semantics."""

    def __init__(self) -> None:
        self._queues: dict[str, list[bytes]] = {}

    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        del priority
        self._queues.setdefault(queue_name, []).append(item)

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        del timeout
        queue = self._queues.get(queue_name)
        if not queue:
            return None
        return queue.pop(0)

    def queue_len(self, queue_name: str) -> int:
        return len(self._queues.get(queue_name, ()))

    def clear_queue(self, queue_name: str) -> None:
        self._queues.pop(queue_name, None)


class _BenchConnectionManager:
    """Expose the manager operations used by the passthrough strategy."""

    def __init__(self, backend: _InMemoryQueueBackend) -> None:
        self._backend = backend

    def get_queue_backend(self) -> _InMemoryQueueBackend:
        return self._backend

    def _push_queue_with_durability(
        self,
        queue_name: str,
        item: bytes,
        priority: float = 0.0,
        *,
        require_durable: bool = False,
    ) -> _QueuePushReceipt:
        return self._backend._push_with_durability(
            queue_name,
            item,
            priority,
            require_durable=require_durable,
        )


@pytest.fixture()
def bench_queue() -> tuple[BackendQueue, _InMemoryQueueBackend]:
    """Return a fresh queue and its in-memory backend."""
    backend = _InMemoryQueueBackend()
    queue = BackendQueue(
        connection_manager=_BenchConnectionManager(backend),  # type: ignore[arg-type]
        queue_name=_QUEUE_NAME,
        spider=Spider(name="benchmark-push-pop"),
    )
    return queue, backend


def _make_request(idx: int = 0) -> Request:
    """Build a representative callback-free Scrapy request."""
    return Request(
        url=f"https://example.com/item/{idx}",
        method="GET",
        headers={"Accept": "text/html", "User-Agent": "scrapy-extension-bench/1.0"},
        body=f"item={idx}".encode(),
        meta={"depth": 1, "source": _BENCHMARK_SOURCE, "retry_times": 0},
        priority=idx,
        dont_filter=False,
    )


def _prepare_push_round(
    backend: _InMemoryQueueBackend,
    requests: Sequence[Request],
) -> None:
    """Clear prior output and restore mutable routing input outside the caliper."""
    backend.clear_queue(_QUEUE_NAME)
    for request in requests:
        request.meta["source"] = _BENCHMARK_SOURCE


@pytest.mark.benchmark
def test_push_single(
    benchmark, bench_queue: tuple[BackendQueue, _InMemoryQueueBackend]
) -> None:
    """Measure one push; queue clearing occurs outside every timed round."""
    queue, backend = bench_queue
    request = _make_request()

    benchmark.pedantic(
        queue.push,
        args=(request,),
        setup=lambda: _prepare_push_round(backend, (request,)),
        rounds=_BENCHMARK_ROUNDS,
    )

    assert backend.queue_len(_QUEUE_NAME) == 1


@pytest.mark.benchmark
def test_pop_single(
    benchmark, bench_queue: tuple[BackendQueue, _InMemoryQueueBackend]
) -> None:
    """Measure one pop; clearing and refill occur outside every timed round."""
    queue, backend = bench_queue
    request = _make_request()

    def prepare_pop() -> None:
        _prepare_push_round(backend, (request,))
        queue.push(request)

    result = benchmark.pedantic(
        queue.pop,
        setup=prepare_pop,
        rounds=_BENCHMARK_ROUNDS,
    )

    assert result is not None
    assert result.url == request.url


@pytest.mark.benchmark
def test_push_batch_of_10_latency(
    benchmark,
    bench_queue: tuple[BackendQueue, _InMemoryQueueBackend],
) -> None:
    """Measure total latency for one batch containing ten sequential pushes."""
    queue, backend = bench_queue
    requests = [_make_request(i) for i in range(10)]

    def push_ten() -> None:
        for request in requests:
            queue.push(request)

    benchmark.pedantic(
        push_ten,
        setup=lambda: _prepare_push_round(backend, requests),
        rounds=_BENCHMARK_ROUNDS,
    )

    assert backend.queue_len(_QUEUE_NAME) == 10


def test_push_round_setup_restores_source_for_every_batch_member(
    bench_queue: tuple[BackendQueue, _InMemoryQueueBackend],
) -> None:
    """Prove all five push rounds start with equal source-routing state."""
    queue, backend = bench_queue
    requests = [_make_request(i) for i in range(10)]
    source_state_by_round: list[tuple[object, ...]] = []

    for _ in range(_BENCHMARK_ROUNDS):
        _prepare_push_round(backend, requests)
        source_state_by_round.append(
            tuple(request.meta.get("source") for request in requests)
        )
        for request in requests:
            queue.push(request)

    expected_state = (_BENCHMARK_SOURCE,) * len(requests)
    assert source_state_by_round == [expected_state] * _BENCHMARK_ROUNDS
    assert all("source" not in request.meta for request in requests)


def test_push_pop_roundtrip_is_lossless(
    bench_queue: tuple[BackendQueue, _InMemoryQueueBackend],
) -> None:
    """Keep queue round-trip correctness deterministic in the ordinary suite."""
    queue, backend = bench_queue
    request = _make_request(7)

    queue.push(request)
    restored = queue.pop()

    assert restored is not None
    assert restored.url == request.url
    assert restored.method == request.method
    assert restored.body == request.body
    assert restored.priority == request.priority
    assert "source" not in restored.meta
    assert backend.queue_len(_QUEUE_NAME) == 0
