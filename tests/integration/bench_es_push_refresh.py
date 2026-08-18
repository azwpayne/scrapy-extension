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

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

N = 50


def _configured_hosts() -> list[str]:
    """Return explicitly opted-in broker hosts or stop without broker I/O."""
    if os.environ.get("SCRAPY_TEST_INTEGRATION") != "1":
        raise SystemExit("Set SCRAPY_TEST_INTEGRATION=1 to enable live-broker I/O.")

    configured = os.environ.get("SCRAPY_TEST_ES_HOSTS")
    if not configured:
        raise SystemExit(
            "Set SCRAPY_TEST_ES_HOSTS (for example, http://localhost:9200)."
        )
    hosts = [host.strip() for host in configured.split(",") if host.strip()]
    if not hosts:
        raise SystemExit("SCRAPY_TEST_ES_HOSTS must contain at least one host.")
    return hosts


def _bench_push(
    backend: ElasticSearchBackend, queue_name: str
) -> tuple[float, list[float]]:
    """Push N queue items through the production backend surface."""
    per_push: list[float] = []
    for index in range(N):
        started = time.monotonic()
        backend.push(queue_name, f"item-{index:03d}".encode(), priority=1.0)
        per_push.append(time.monotonic() - started)
    return sum(per_push), per_push


def main() -> None:
    hosts = _configured_hosts()
    backend = ElasticSearchBackend(
        ElasticSearchSettings(hosts=hosts, request_timeout=10.0, max_retries=1)
    )
    queue_name = f"bench-push-{uuid.uuid4().hex}"
    backend.connect()
    try:
        total, per_push = _bench_push(backend, queue_name)
        ordered = sorted(per_push)
        p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
        mean = statistics.mean(per_push)
        maximum = max(per_push)

        print(f"ElasticSearchBackend.push benchmark: N={N}, broker={hosts[0]}")
        print(f"total: {total:.2f}s")
        print(f"mean: {mean * 1000:.1f}ms")
        print(f"p95: {p95 * 1000:.1f}ms")
        print(f"max: {maximum * 1000:.1f}ms")
        print(
            "Scope: sequential end-to-end backend.push calls; this measurement "
            "does not isolate refresh behavior or compare raw client modes."
        )
    finally:
        try:
            backend.clear_queue(queue_name)
        finally:
            backend.disconnect()


if __name__ == "__main__":
    main()
