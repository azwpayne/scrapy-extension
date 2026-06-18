"""Redis integration tests (R2-A4 foundation).

These tests exercise ``RedisBackend`` against a **real** Redis instance. They
exist because mock-based tests provably cannot verify the contracts that
matter most on this backend:

- R1-P0-1 / R5 / R6 — ZSET member collision, Lua push/pop atomicity, FIFO
  within a priority bucket. Mocks return whatever the test sets up; a
  regression in the Lua script or the hash-tagged key layout is invisible
  to them (the R31-R34 arc showed mocks can even codify the *wrong*
  contract).
- R31 — ``SetBackend.add`` returns False for "already existed" vs raises on
  error. A mock can't distinguish a real ``sadd`` result from a stub.
- R32 / R33 — ``retrieve``/``exists`` returning None/False means "absent",
  not "errored". Only a real Redis round-trip pins that.
- R5 — ``ttl()`` semantics (None for missing / no-TTL, positive int with TTL).

Running
-------
Skipped by default. To run, point at a Redis you don't mind a few throwaway
``inttest:*`` keys landing in::

    SCRAPY_TEST_REDIS_URL=redis://localhost:6379/0 uv run pytest tests/integration -q

Each test uses a UUID-prefixed key namespace so concurrent runs and leftover
data don't interfere.
"""

from __future__ import annotations

import os
import uuid
from urllib.parse import urlparse

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_REDIS_URL"),
    reason=(
      "Set SCRAPY_TEST_REDIS_URL (e.g. redis://localhost:6379/0) to run "
      "Redis integration tests against a live instance."
    ),
  ),
]


def _settings_from_url(url: str):  # type: ignore[no-untyped-def]
  """Build a RedisSettings from a redis:// URL via stdlib urlparse.

  Kept dependency-free (no redis-py ``parse_url``) so the module imports
  even when the redis extra isn't installed — it skips before any redis
  call is made.
  """
  from pydantic import SecretStr

  from scrapy_extension.settings.redis import RedisSettings

  parsed = urlparse(url)
  db_raw = parsed.path.lstrip("/") or "0"
  return RedisSettings(
    host=parsed.hostname or "localhost",
    port=parsed.port or 6379,
    db=int(db_raw),
    # urlparse returns '' (not None) for a password-only URL like
    # redis://:secret@host — coerce to None so redis-py doesn't AUTH with
    # an empty username (treated differently from no-username).
    username=parsed.username or None,
    password=SecretStr(parsed.password) if parsed.password else None,
    socket_timeout=5.0,
    socket_connect_timeout=5.0,
  )


@pytest.fixture(scope="module")
def redis_backend():  # type: ignore[no-untyped-def]
  """Connect a RedisBackend once per module; disconnect on teardown."""
  from scrapy_extension.backends.redis import RedisBackend

  backend = RedisBackend(_settings_from_url(os.environ["SCRAPY_TEST_REDIS_URL"]))
  backend.connect()
  yield backend
  backend.disconnect()


@pytest.fixture
def unique_prefix() -> str:
  """UUID-prefixed namespace so tests can't collide with each other or stale data."""
  return f"inttest:{uuid.uuid4().hex}"


def test_push_pop_round_trip_no_collision(redis_backend, unique_prefix):
  """R1-P0-1 / R5 / R6: identical-byte items must NOT collide in the ZSET.

  The pre-R1 bug used raw item bytes as the ZSET member, so two identical
  payloads silently deduped to one (data loss). The Lua push (R6) uses an
  INCR counter + uuid member + sidecar hash so every push is distinct.
  Only a real ZADD/HGET round-trip can catch a regression here.
  """
  queue = f"{unique_prefix}:queue"
  payload = b"x" * 32  # identical bytes — would have collided pre-R1
  n = 50

  for _ in range(n):
    redis_backend.push(queue, payload, priority=1.0)

  assert redis_backend.queue_len(queue) == n
  popped = [redis_backend.pop(queue, timeout=0.0) for _ in range(n)]
  assert all(item == payload for item in popped)
  assert redis_backend.pop(queue, timeout=0.0) is None  # fully drained
  assert redis_backend.queue_len(queue) == 0


def test_same_priority_fifo(redis_backend, unique_prefix):
  """R6: same-priority items pop in insertion order (INCR counter tiebreak).

  Pre-R6 the member was a random uuid, so same-score items came out in
  lexicographic-uuid order (effectively random) — FIFO within a priority
  bucket was violated.
  """
  queue = f"{unique_prefix}:fifo"
  items = [b"first", b"second", b"third"]
  for item in items:
    redis_backend.push(queue, item, priority=5.0)

  popped = [redis_backend.pop(queue, timeout=0.0) for _ in items]
  assert popped == items


def test_priority_ordering(redis_backend, unique_prefix):
  """Higher priority pops first (ZPOPMAX on score)."""
  queue = f"{unique_prefix}:prio"
  redis_backend.push(queue, b"low", priority=1.0)
  redis_backend.push(queue, b"high", priority=10.0)
  redis_backend.push(queue, b"mid", priority=5.0)

  assert redis_backend.pop(queue, timeout=0.0) == b"high"


def test_set_add_duplicate_contract(redis_backend, unique_prefix):
  """R31: add() returns True (new), then False (duplicate); contains honored.

  R31 fixed Redis's ``except RedisError: return False`` which had conflated
  network errors with "already existed". A real ``sadd`` pins the contract:
  first add returns 1 (→ True), second returns 0 (→ False), no exception.
  """
  key = f"{unique_prefix}:set"
  fingerprint = b"request-fingerprint-abc"

  assert redis_backend.add(key, fingerprint) is True
  assert redis_backend.contains(key, fingerprint) is True
  assert redis_backend.add(key, fingerprint) is False  # duplicate
  assert redis_backend.set_len(key) == 1


def test_storage_contract(redis_backend, unique_prefix):
  """R32/R33: store/retrieve round-trip; exists; delete; retrieve-after-delete None.

  R32/R33 fixed retrieve()/exists() returning None/False on backend errors
  (which made callers overwrite existing keys during network blips). A real
  round-trip pins "None means absent, not errored".
  """
  key = f"{unique_prefix}:kv"
  payload = b'{"item": 1, "ts": "2026-06-18"}'

  assert redis_backend.exists(key) is False
  redis_backend.store(key, payload)
  assert redis_backend.exists(key) is True
  assert redis_backend.retrieve(key) == payload

  assert redis_backend.delete(key) is True
  assert redis_backend.retrieve(key) is None
  assert redis_backend.exists(key) is False


def test_ttl_contract(redis_backend, unique_prefix):
  """R5: ttl() returns a positive int with a TTL, None without one.

  R5 fixed Redis returning -1 for both "expired" and "missing" — the
  contract is None for missing/no-TTL, positive seconds remaining with a TTL.
  """
  with_ttl = f"{unique_prefix}:ttl"
  no_ttl = f"{unique_prefix}:persistent"

  redis_backend.store(with_ttl, b"x", ttl=300)
  redis_backend.store(no_ttl, b"y")  # persisted, no expiry

  remaining = redis_backend.ttl(with_ttl)
  assert isinstance(remaining, int)
  assert 0 < remaining <= 300
  assert redis_backend.ttl(no_ttl) is None  # no TTL set → None, not -1
