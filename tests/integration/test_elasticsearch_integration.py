"""ElasticSearch integration tests (R2-A4 foundation, part 3).

Completes the trio of fully-implemented backends (Redis R46, MongoDB R47,
ElasticSearch here). ES is the highest-value of the three because its pop
uses **optimistic locking** (``if_seq_no`` / ``if_primary_term``,
``ConflictError`` retry — R10), the single most mock-opaque contract in the
project: a mock cannot reproduce a real HTTP 409 version conflict.

These tests pin:
- R10 — optimistic-locking pop: push N → pop N, no double-consume, no loss.
  The delete-with-``if_seq_no`` race only resolves correctly against a real
  ES index.
- Priority + same-priority FIFO ordering (``sort=[{priority:asc},
  {created_at:asc}]``, priority negated on push).
- R31 — ``add`` returns False on duplicate via ``op_type="create"`` +
  ``ConflictError`` on the deterministic ``{set_name}:{sha256}`` doc id.
- R32/R33 — retrieve/exists None/False means absent, not errored.
- R48 — ``ttl()`` returns None for a missing key (the R5-sweep miss this
  suite exists to keep honest).

ElasticSearch is near-real-time
-------------------------------
A freshly indexed doc is NOT visible to search/get until the next refresh
(default 1s interval). Every read-after-write test calls the ``refresh``
fixture first. This is not a backend bug — it's ES's documented consistency
model. Without the refresh, these tests would be racy.

Running
-------
Skipped by default. Point at an ES you don't mind ``inttest:*`` docs landing
in::

    SCRAPY_TEST_ES_HOSTS=http://localhost:9200 uv run pytest tests/integration -q

Each test uses a UUID-prefixed logical-name namespace (queue_name / set_name
/ key) so concurrent runs and leftover data can't interfere.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.skipif(
  not os.environ.get("SCRAPY_TEST_ES_HOSTS"),
  reason=(
    "Set SCRAPY_TEST_ES_HOSTS (comma-separated, e.g. http://localhost:9200) "
    "to run ElasticSearch integration tests against a live instance."
  ),
)


@pytest.fixture(scope="module")
def es_backend():  # type: ignore[no-untyped-def]
  """Connect an ElasticSearchBackend once per module; disconnect on teardown."""
  from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
  from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

  hosts = [h.strip() for h in os.environ["SCRAPY_TEST_ES_HOSTS"].split(",") if h.strip()]
  backend = ElasticSearchBackend(
    ElasticSearchSettings(hosts=hosts, request_timeout=5.0, max_retries=1)
  )
  backend.connect()  # _ensure_indices creates the queue/set/storage indices
  yield backend
  backend.disconnect()


@pytest.fixture
def refresh(es_backend):  # type: ignore[no-untyped-def]
  """Force ES to flush pending writes so they're visible to search/get.

  ES is near-real-time: a doc indexed this instant isn't searchable until the
  next refresh. Call this before any read that must observe a prior write.
  """

  def _refresh() -> None:
    for index in (
      es_backend.config.queue_index,
      es_backend.config.set_index,
      es_backend.config.storage_index,
    ):
      es_backend.client.indices.refresh(index=index)

  return _refresh


@pytest.fixture
def unique_prefix() -> str:
  """UUID-prefixed namespace so tests can't collide with each other or stale data."""
  return f"inttest:{uuid.uuid4().hex}"


def test_push_pop_round_trip_optimistic_lock(es_backend, unique_prefix, refresh):
  """R10: optimistic-locking pop is correct — no double-consume, no loss.

  The pop search-then-delete with ``if_seq_no``/``if_primary_term`` only
  resolves correctly against a real ES index. A mock can't reproduce a real
  version-conflict path. 50 distinct items in → 50 out.
  """
  queue = f"{unique_prefix}:queue"
  n = 50
  for i in range(n):
    es_backend.push(queue, f"item-{i:03d}".encode(), priority=1.0)

  refresh()  # make all 50 searchable before popping
  assert es_backend.queue_len(queue) == n
  popped = [es_backend.pop(queue, timeout=0.0) for _ in range(n)]
  assert len(popped) == n
  assert all(item is not None for item in popped)
  refresh()  # let the deletes settle
  assert es_backend.pop(queue, timeout=0.0) is None  # drained


def test_priority_ordering(es_backend, unique_prefix, refresh):
  """Higher priority pops first (priority negated on push, asc sort on pop)."""
  queue = f"{unique_prefix}:prio"
  es_backend.push(queue, b"low", priority=1.0)
  es_backend.push(queue, b"high", priority=10.0)
  es_backend.push(queue, b"mid", priority=5.0)

  refresh()
  assert es_backend.pop(queue, timeout=0.0) == b"high"


def test_same_priority_fifo(es_backend, unique_prefix, refresh):
  """Same-priority items pop in insertion order (created_at asc tiebreak)."""
  queue = f"{unique_prefix}:fifo"
  items = [b"first", b"second", b"third"]
  for item in items:
    es_backend.push(queue, item, priority=5.0)

  refresh()
  popped = [es_backend.pop(queue, timeout=0.0) for _ in items]
  assert popped == items


def test_set_add_duplicate_contract(es_backend, unique_prefix, refresh):
  """R31: add() True (new) then False (duplicate) via op_type=create + ConflictError.

  The deterministic doc id ``{set_name}:{sha256(item)}`` plus
  ``op_type="create"`` makes a second add hit a 409 → False. A mock can't
  exercise a real ES version conflict.
  """
  key = f"{unique_prefix}:set"
  fingerprint = b"request-fingerprint-abc"

  assert es_backend.add(key, fingerprint) is True
  refresh()
  assert es_backend.contains(key, fingerprint) is True
  assert es_backend.add(key, fingerprint) is False  # duplicate
  refresh()
  assert es_backend.set_len(key) == 1


def test_storage_contract(es_backend, unique_prefix, refresh):
  """R32/R33: store/retrieve round-trip; exists; delete; retrieve-after-delete None."""
  key = f"{unique_prefix}:kv"
  payload = b'{"item": 1, "ts": "2026-06-18"}'

  assert es_backend.exists(key) is False
  es_backend.store(key, payload)
  refresh()
  assert es_backend.exists(key) is True
  assert es_backend.retrieve(key) == payload

  assert es_backend.delete(key) is True
  refresh()
  assert es_backend.retrieve(key) is None
  assert es_backend.exists(key) is False


def test_ttl_contract(es_backend, unique_prefix, refresh):
  """R5/R48: ttl() returns a positive int with TTL, None without / missing.

  This is the integration verification of R48 — the R5 sweep had missed ES,
  which returned -1 for a missing key. ES now returns None for both missing
  and no-TTL, matching Redis and MongoDB.
  """
  with_ttl = f"{unique_prefix}:ttl"
  no_ttl = f"{unique_prefix}:persistent"
  missing = f"{unique_prefix}:missing"

  es_backend.store(with_ttl, b"x", ttl=300)
  es_backend.store(no_ttl, b"y")  # no expireAt → persisted
  refresh()

  remaining = es_backend.ttl(with_ttl)
  assert isinstance(remaining, int)
  assert 0 < remaining <= 300
  assert es_backend.ttl(no_ttl) is None  # no TTL → None
  assert es_backend.ttl(missing) is None  # R48: missing → None, not -1
