"""Perf regression gate for the ElasticSearch backend push path.

Guards against the #43 regression: ``refresh="wait_for"`` on every push was a
~250x slowdown (1010ms/push vs 4ms without — ES does not batch ``wait_for``
across consecutive pushes). The fix moved refresh to the read side (one forced
``indices.refresh`` before pop/count). This test fails if anyone re-adds a
per-push ``wait_for`` (or otherwise regresses the push path to ~1s/push).

Threshold is RELATIVE-to-the-regression, not absolute-speed: 200ms/push gives
~6x headroom over the observed fast path (~3-4ms) and ~5x margin below the
~1000ms wait_for regression — robust to runner variance (a slow runner at 8ms
fast-path is still 25x under threshold; the regression at 1000ms is 5x over).

Skipped by default — runs in the CI integration job (live ES required).
"""

from __future__ import annotations

import os
import statistics
import time
import uuid

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_INTEGRATION"),
    reason="integration opt-in: set SCRAPY_TEST_INTEGRATION=1",
  ),
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_ES_HOSTS"),
    reason="Set SCRAPY_TEST_ES_HOSTS (e.g. http://localhost:9200) to run ES perf gate.",
  ),
]

# Per-push budget. See module docstring for the threshold rationale.
# If this fires, the push path has regressed back toward per-push refresh wait.
_PER_PUSH_BUDGET_MS = 200.0
_N = 20


@pytest.fixture(scope="module")
def es_backend():  # type: ignore[no-untyped-def]
  """Connect an ElasticSearchBackend once per module; disconnect on teardown.

  Mirrors the fixture in test_elasticsearch_integration.py — kept local (not
  in a conftest) to match the per-file integration-test pattern.
  """
  from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
  from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

  hosts = [h.strip() for h in os.environ["SCRAPY_TEST_ES_HOSTS"].split(",") if h.strip()]
  backend = ElasticSearchBackend(
    ElasticSearchSettings(hosts=hosts, request_timeout=10.0, max_retries=1)
  )
  backend.connect()
  yield backend
  backend.disconnect()


def test_push_does_not_regress_to_wait_for_latency(es_backend):  # type: ignore[no-untyped-def]
  """Each push must stay well under the refresh='wait_for' regression cost.

  The #43 fix removed ``refresh="wait_for"`` from push (it was ~1010ms/push;
  ES doesn't amortize ``wait_for`` across consecutive pushes). If a future
  change re-adds it (or otherwise makes push block on the ES refresh cycle),
  mean per-push latency jumps ~250x and this gate fires.
  """
  queue = f"perf-{uuid.uuid4().hex}"
  per_push: list[float] = []
  for i in range(_N):
    t0 = time.monotonic()
    es_backend.push(queue, f"item-{i:03d}".encode(), priority=1.0)
    per_push.append(time.monotonic() - t0)

  mean_ms = statistics.mean(per_push) * 1000
  max_ms = max(per_push) * 1000
  # Assert on BOTH mean (steady-state) and max (no single push stalls).
  # CI runners vary, so the headroom is intentionally wide (see module
  # docstring) — the regression is ~5x over budget, the fast path ~50x under.
  assert mean_ms < _PER_PUSH_BUDGET_MS, (
    f"ES push mean latency {mean_ms:.1f}ms/push exceeds "
    f"{_PER_PUSH_BUDGET_MS}ms budget — likely a refresh='wait_for' "
    "regression on the push path (see #43). "
    f"(max={max_ms:.1f}ms, n={_N})"
  )
  assert max_ms < _PER_PUSH_BUDGET_MS * 2, (
    f"ES push max latency {max_ms:.1f}ms exceeds "
    f"{_PER_PUSH_BUDGET_MS * 2}ms — a single push stalled near the "
    "refresh-interval (1s), suggesting per-push refresh. (see #43)"
  )
