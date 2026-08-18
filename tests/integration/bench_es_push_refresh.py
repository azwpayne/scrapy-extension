#!/usr/bin/env python
"""Measure the production ElasticSearch queue push path.

This script calls :meth:`ElasticSearchBackend.push` end to end against a live
broker. It reports sequential call latency, including backend validation and
serialization, elasticsearch-py, network, and server indexing work. It does not
isolate refresh cost, compare raw ``Elasticsearch.index`` modes, or prove why a
latency change occurred; the integration regression gate owns the production
per-push budget.

Run (requires the compose ES broker up):
    SCRAPY_TEST_INTEGRATION=1 \
    SCRAPY_TEST_ES_HOSTS=http://localhost:9200 uv run python \
        tests/integration/bench_es_push_refresh.py
"""

from __future__ import annotations

import math
import os
import statistics
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

N = 50


@dataclass(frozen=True)
class BenchmarkResult:
    """Summary of one sequential production-backend push run."""

    total: float
    mean: float
    p95: float
    maximum: float


def configured_hosts(environment: Mapping[str, str] | None = None) -> list[str]:
    """Return explicitly opted-in broker hosts or stop without broker I/O."""
    environment = os.environ if environment is None else environment
    if environment.get("SCRAPY_TEST_INTEGRATION") != "1":
        raise SystemExit("Set SCRAPY_TEST_INTEGRATION=1 to enable live-broker I/O.")

    configured = environment.get("SCRAPY_TEST_ES_HOSTS")
    if not configured:
        raise SystemExit(
            "Set SCRAPY_TEST_ES_HOSTS (for example, http://localhost:9200)."
        )
    hosts = [host.strip() for host in configured.split(",") if host.strip()]
    if not hosts:
        raise SystemExit("SCRAPY_TEST_ES_HOSTS must contain at least one host.")
    return hosts


def bench_push(
    backend: ElasticSearchBackend,
    queue_name: str,
    *,
    sample_count: int = N,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[float, list[float]]:
    """Push queue items through the production backend surface."""
    if sample_count < 1:
        raise ValueError("sample_count must be positive")

    per_push: list[float] = []
    for index in range(sample_count):
        started = clock()
        backend.push(queue_name, f"item-{index:03d}".encode(), priority=1.0)
        per_push.append(clock() - started)
    return sum(per_push), per_push


def run_benchmark(
    backend: ElasticSearchBackend,
    queue_name: str,
    *,
    sample_count: int = N,
    clock: Callable[[], float] = time.monotonic,
) -> BenchmarkResult:
    """Connect, measure production pushes, and always clear and disconnect."""
    backend.connect()
    try:
        total, per_push = bench_push(
            backend,
            queue_name,
            sample_count=sample_count,
            clock=clock,
        )
        ordered = sorted(per_push)
        return BenchmarkResult(
            total=total,
            mean=statistics.mean(per_push),
            p95=ordered[math.ceil(0.95 * len(ordered)) - 1],
            maximum=max(per_push),
        )
    finally:
        try:
            backend.clear_queue(queue_name)
        finally:
            backend.disconnect()


def main() -> None:
    hosts = configured_hosts()
    backend = ElasticSearchBackend(
        ElasticSearchSettings(hosts=hosts, request_timeout=10.0, max_retries=1)
    )
    result = run_benchmark(backend, f"bench-push-{uuid.uuid4().hex}")

    print(f"ElasticSearchBackend.push benchmark: N={N}, broker={hosts[0]}")
    print(f"total: {result.total:.2f}s")
    print(f"mean: {result.mean * 1000:.1f}ms")
    print(f"p95: {result.p95 * 1000:.1f}ms")
    print(f"max: {result.maximum * 1000:.1f}ms")
    print(
        "Scope: sequential end-to-end backend.push calls; this measurement "
        "does not isolate refresh behavior or compare raw client modes."
    )


if __name__ == "__main__":
    main()
