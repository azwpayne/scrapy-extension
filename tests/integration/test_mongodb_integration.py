"""MongoDB integration tests (R2-A4 foundation, part 2).

Companion to ``test_redis_integration.py``. MongoDB is the second fully-
implemented backend (Queue + Set + Storage). These tests pin the contracts
mocks cannot verify:

- ``find_one_and_delete`` pop atomicity and strict valid-record selection. Real
  round-trips confirm concurrent pops cannot double-consume and malformed queue
  documents remain durable without starving deliverable records.
- Priority + same-priority FIFO ordering — pop sorts
  ``priority ASC, created_at ASC`` (priority negated on push, line 404/432).
- R31 — ``add`` returns False on duplicate via the unique ``(set_name,
  item_hash)`` index created in ``connect()._create_indexes`` (lines
  317-319). A mock can't exercise a real ``DuplicateKeyError``.
- R32/R33 — retrieve/exists None/False means absent, not errored.
- R1-P0-4 / R5 — ttl() semantics (None for missing/no-TTL, int with TTL).

Running
-------
Skipped by default. Point at a MongoDB you don't mind a few throwaway
``inttest:*`` logical keys landing in::

    SCRAPY_TEST_INTEGRATION=1 SCRAPY_TEST_MONGODB_URI=mongodb://localhost:27017 \
      uv run pytest tests/integration -q --force-enable-socket

Optional ``SCRAPY_TEST_MONGODB_DB`` overrides the database (default
``scrapy_extension``). Each test uses a UUID-prefixed logical-name namespace
so concurrent runs and leftover data can't interfere.
"""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier

import pytest
from bson.decimal128 import Decimal128
from bson.int64 import Int64

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("SCRAPY_TEST_MONGODB_URI"),
        reason=(
            "Set SCRAPY_TEST_MONGODB_URI (e.g. mongodb://localhost:27017) to run "
            "MongoDB integration tests against a live instance."
        ),
    ),
]


@pytest.fixture(scope="module")
def mongo_backend():  # type: ignore[no-untyped-def]
    """Connect a MongoDBBackend once per module; disconnect on teardown."""
    from scrapy_extension.backends.mongodb import MongoDBBackend
    from scrapy_extension.settings.mongodb import MongoDBSettings

    config = MongoDBSettings(
        uri=os.environ["SCRAPY_TEST_MONGODB_URI"],
        server_selection_timeout_ms=5000,
    )
    db = os.environ.get("SCRAPY_TEST_MONGODB_DB")
    if db:
        config = config.model_copy(update={"database": db})

    backend = MongoDBBackend(config)
    backend.connect()
    yield backend
    backend.disconnect()


@pytest.fixture
def unique_prefix() -> str:
    """UUID-prefixed namespace so tests can't collide with each other or stale data."""
    return f"inttest:{uuid.uuid4().hex}"


def test_push_pop_round_trip_atomic(mongo_backend, unique_prefix):
    """R1-P0-3 (withdrawn): find_one_and_delete pop is atomic — no double-consume.

    The Round-1 critique was withdrawn on the assumption that
    ``find_one_and_delete`` is document-level atomic. This verifies that
    assumption against a real MongoDB: 50 distinct items pushed, 50 popped,
    none lost or duplicated. (Mock tests can't catch a non-atomic pop.)
    """
    queue = f"{unique_prefix}:queue"
    n = 50
    for i in range(n):
        mongo_backend.push(queue, f"item-{i:03d}".encode(), priority=1.0)

    assert mongo_backend.queue_len(queue) == n
    popped = [mongo_backend.pop(queue, timeout=0.0) for _ in range(n)]
    assert len(popped) == n
    assert all(item is not None for item in popped)
    assert mongo_backend.pop(queue, timeout=0.0) is None  # drained
    assert mongo_backend.queue_len(queue) == 0


def test_concurrent_live_pop_has_one_winner_and_conserves_the_item(
    mongo_backend, unique_prefix
):
    """Concurrent live-server deletes deliver one document exactly once."""
    queue = f"{unique_prefix}:contended"
    payload = b"single-winner"
    caller_count = 24
    mongo_backend.push(queue, payload)
    start = Barrier(caller_count)

    def pop_once(_index):  # type: ignore[no-untyped-def]
        start.wait(timeout=10)
        return mongo_backend.pop(queue, timeout=0.0)

    try:
        with ThreadPoolExecutor(max_workers=caller_count) as executor:
            results = list(executor.map(pop_once, range(caller_count)))

        delivered = [result for result in results if result is not None]
        remaining = mongo_backend.queue_len(queue)
        assert delivered == [payload]
        assert results.count(None) == caller_count - 1
        assert len(delivered) + remaining == 1
        assert remaining == 0
    finally:
        mongo_backend.clear_queue(queue)


def test_priority_ordering(mongo_backend, unique_prefix):
    """Higher priority pops first (priority negated on push, ASC sort on pop)."""
    queue = f"{unique_prefix}:prio"
    mongo_backend.push(queue, b"low", priority=1.0)
    mongo_backend.push(queue, b"high", priority=10.0)
    mongo_backend.push(queue, b"mid", priority=5.0)

    assert mongo_backend.pop(queue, timeout=0.0) == b"high"


def test_same_priority_fifo(mongo_backend, unique_prefix):
    """Same-priority items pop in insertion order (created_at ASC tiebreak)."""
    queue = f"{unique_prefix}:fifo"
    items = [b"first", b"second", b"third"]
    for item in items:
        mongo_backend.push(queue, item, priority=5.0)

    popped = [mongo_backend.pop(queue, timeout=0.0) for _ in items]
    assert popped == items


def test_poison_records_are_durable_and_excluded_from_active_length(
    mongo_backend, unique_prefix
):
    """A real server enforces the strict active schema before atomic deletion."""
    queue = f"{unique_prefix}:poison"
    collection = mongo_backend._queue_collection
    assert collection is not None
    now = datetime.now(tz=timezone.utc)
    poison_documents = [
        {
            "queue_name": queue,
            "item": "not-binary",
            "priority": -100.0,
            "created_at": now,
        },
        {
            "queue_name": queue,
            "item": b"bool-priority",
            "priority": False,
            "created_at": now,
        },
        {
            "queue_name": queue,
            "item": b"nan-priority",
            "priority": float("nan"),
            "created_at": now,
        },
        {
            "queue_name": queue,
            "item": b"infinite-priority",
            "priority": float("-inf"),
            "created_at": now,
        },
        {
            "queue_name": queue,
            "item": b"decimal-nan-priority",
            "priority": Decimal128("NaN"),
            "created_at": now,
        },
        {
            "queue_name": queue,
            "item": b"decimal-positive-infinity-priority",
            "priority": Decimal128("Infinity"),
            "created_at": now,
        },
        {
            "queue_name": queue,
            "item": b"decimal-negative-infinity-priority",
            "priority": Decimal128("-Infinity"),
            "created_at": now,
        },
        {
            "queue_name": queue,
            "item": b"bad-created-at",
            "priority": -100.0,
            "created_at": "not-a-date",
        },
    ]
    collection.insert_many(poison_documents)
    mongo_backend.push(queue, b"deliverable", priority=1.0)

    assert mongo_backend.queue_len(queue) == 1
    assert mongo_backend.pop(queue) == b"deliverable"
    assert mongo_backend.queue_len(queue) == 0
    assert collection.count_documents({"queue_name": queue}) == len(poison_documents)

    mongo_backend.clear_queue(queue)
    assert collection.count_documents({"queue_name": queue}) == 0


def test_active_filter_accepts_all_finite_bson_numeric_types(
    mongo_backend, unique_prefix
):
    """Double, int32, int64, and Decimal128 priorities remain deliverable."""
    queue = f"{unique_prefix}:numeric-types"
    collection = mongo_backend._queue_collection
    assert collection is not None
    now = datetime.now(tz=timezone.utc)
    documents = [
        {
            "queue_name": queue,
            "item": b"double",
            "priority": 1.5,
            "created_at": now,
        },
        {
            "queue_name": queue,
            "item": b"int32",
            "priority": 1,
            "created_at": now + timedelta(microseconds=1),
        },
        {
            "queue_name": queue,
            "item": b"int64",
            "priority": Int64(2**40),
            "created_at": now + timedelta(microseconds=2),
        },
        {
            "queue_name": queue,
            "item": b"decimal128",
            "priority": Decimal128("2.5"),
            "created_at": now + timedelta(microseconds=3),
        },
    ]
    collection.insert_many(documents)

    try:
        assert mongo_backend.queue_len(queue) == len(documents)
        delivered = [mongo_backend.pop(queue) for _document in documents]
        assert set(delivered) == {b"double", b"int32", b"int64", b"decimal128"}
        assert mongo_backend.queue_len(queue) == 0
    finally:
        mongo_backend.clear_queue(queue)


def test_set_add_duplicate_contract(mongo_backend, unique_prefix):
    """R31: add() True (new) then False (duplicate) via the unique (set_name, item_hash) index.

    The unique index is created in ``connect()._create_indexes`` (lines
    317-319). Without it, ``DuplicateKeyError`` never fires and duplicates
    would silently accumulate. This pins that the index exists and ``add``
    honors the SetBackend contract.
    """
    key = f"{unique_prefix}:set"
    fingerprint = b"request-fingerprint-abc"

    assert mongo_backend.add(key, fingerprint) is True
    assert mongo_backend.contains(key, fingerprint) is True
    assert mongo_backend.add(key, fingerprint) is False  # duplicate
    assert mongo_backend.set_len(key) == 1


def test_storage_contract(mongo_backend, unique_prefix):
    """R32/R33: store/retrieve round-trip; exists; delete; retrieve-after-delete None."""
    key = f"{unique_prefix}:kv"
    payload = b'{"item": 1, "ts": "2026-06-18"}'

    assert mongo_backend.exists(key) is False
    mongo_backend.store(key, payload)
    assert mongo_backend.exists(key) is True
    assert mongo_backend.retrieve(key) == payload

    assert mongo_backend.delete(key) is True
    assert mongo_backend.retrieve(key) is None
    assert mongo_backend.exists(key) is False


def test_ttl_contract(mongo_backend, unique_prefix):
    """R1-P0-4 / R5: ttl() returns a positive int with TTL, None without / missing.

    MongoDB stores ``expireAt``; a TTL index (``expireAfterSeconds=0``,
    line 324-326) auto-deletes expired docs. ``ttl()`` computes remaining
    seconds manually — None when there's no ``expireAt`` or the doc is absent.
    """
    with_ttl = f"{unique_prefix}:ttl"
    no_ttl = f"{unique_prefix}:persistent"

    mongo_backend.store(with_ttl, b"x", ttl=300)
    mongo_backend.store(no_ttl, b"y")  # no expireAt → persisted

    remaining = mongo_backend.ttl(with_ttl)
    assert isinstance(remaining, int)
    assert 0 < remaining <= 300
    assert mongo_backend.ttl(no_ttl) is None  # no TTL → None
