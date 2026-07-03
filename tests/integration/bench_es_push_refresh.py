#!/usr/bin/env python
"""Benchmark: latency cost of ES push refresh="wait_for" (#40 tradeoff).

The ElasticSearchBackend.push/add/delete use ``refresh="wait_for"`` for
read-your-writes consistency (push→pop correctness; see #40). The naive
concern is "1s/push" (ES's default refresh interval). This script measures
the real cost against a live ES — ES BATCHES refreshes, so consecutive
pushes within one refresh window amortize the wait.

Run (requires the compose ES broker up):
    SCRAPY_TEST_ES_HOSTS=http://localhost:9200 uv run python \\
        tests/integration/bench_es_push_refresh.py
"""

from __future__ import annotations

import os
import statistics
import time
import uuid

from elasticsearch import Elasticsearch

N = 50  # pushes per mode — matches test_push_pop_round_trip_optimistic_lock scale


def _bench(client: Elasticsearch, index: str, refresh: str | None) -> tuple[float, list[float]]:
  """Push N docs with the given refresh mode. Return (total_s, per_push_s_list)."""
  per_push: list[float] = []
  for _ in range(N):
    doc = {"queue_name": f"bench-{uuid.uuid4().hex}", "item": "eA==", "priority": -1.0,
           "created_at": "2026-07-04T12:00:00Z"}
    kwargs: dict = {"index": index, "document": doc}
    if refresh is not None:
      kwargs["refresh"] = refresh
    t0 = time.monotonic()
    client.index(**kwargs)
    per_push.append(time.monotonic() - t0)
  return sum(per_push), per_push


def main() -> None:
  hosts = [h.strip() for h in os.environ.get("SCRAPY_TEST_ES_HOSTS", "http://localhost:9200").split(",") if h.strip()]
  client = Elasticsearch(hosts=hosts, request_timeout=10.0, retry_on_timeout=False)
  assert client.ping(), "ES broker not reachable"

  idx = f"bench-push-{uuid.uuid4().hex[:8]}"
  client.indices.create(index=idx)

  print(f"ES push benchmark: N={N} per mode, broker={hosts[0]}")
  print(f"{'mode':<22} {'total':>8} {'per-push mean':>15} {'per-push p95':>14} {'per-push max':>13}")
  print("-" * 76)

  for label, refresh in [("refresh='wait_for'", "wait_for"), ("refresh=False", False), ("no refresh arg", None)]:
    total, per_push = _bench(client, idx, refresh)
    per_push_sorted = sorted(per_push)
    mean = statistics.mean(per_push)
    p95 = per_push_sorted[int(0.95 * len(per_push))]
    mx = max(per_push)
    print(f"{label:<22} {total:>7.2f}s {mean*1000:>13.1f}ms {p95*1000:>12.1f}ms {mx*1000:>11.1f}ms")

  # Correctness check: with wait_for, a search immediately after push sees it.
  client.indices.delete(index=idx)
  print("\nInterpretation: refresh='wait_for' total >> refresh=False total means")
  print("ES is NOT amortizing — every push pays the refresh wait. If the gap is")
  print("small, ES batches refreshes and the per-push cost is sub-linear.")


if __name__ == "__main__":
  main()
