"""Memcached integration tests (Risk 7 scaffolding).

Mock-based tests provably cannot verify the storage contract that matters most
on this backend:

- Memcached's 1 MB value cap + LRU eviction vs TTL — a mock can't surface a
  real server-side silent drop or eviction.
- ``store``/``retrieve``/``delete`` round-trip fidelity through pymemcache's
  serialization boundary.
- ``ttl()`` semantics — Memcached does NOT expose remaining TTL, so this
  backend returns ``None`` (a documented divergence mocks can't pin).

Running
-------
Skipped by default. To run, point at a memcached you don't mind throwaway
``inttest:*`` keys landing in::

    SCRAPY_TEST_MEMCACHED_HOST=localhost uv run pytest tests/integration/test_memcached_integration.py -q

For a non-loopback test server, also set
``SCRAPY_TEST_MEMCACHED_ALLOW_REMOTE_PLAINTEXT=1`` to acknowledge that the
Memcached protocol is unauthenticated and unencrypted.

The test uses a UUID-prefixed key namespace so concurrent runs and leftover
data don't interfere.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_MEMCACHED_HOST"),
    reason=(
      "Set SCRAPY_TEST_MEMCACHED_HOST (e.g. localhost) to run Memcached "
      "integration tests against a live instance."
    ),
  ),
]


def test_store_retrieve_round_trip() -> None:
  """Real-broker round-trip: store → retrieve → delete (storage ABC contract)."""
  from scrapy_extension.backends.memcached import MemcachedBackend
  from scrapy_extension.settings.memcached import MemcachedMode, MemcachedSettings

  settings = MemcachedSettings(
    mode=MemcachedMode.STANDALONE,
    host=os.environ["SCRAPY_TEST_MEMCACHED_HOST"],
    allow_remote_plaintext=(
      os.environ.get("SCRAPY_TEST_MEMCACHED_ALLOW_REMOTE_PLAINTEXT") == "1"
    ),
  )
  backend = MemcachedBackend(settings)
  backend.connect()
  try:
    key = f"inttest:{uuid.uuid4().hex}"
    payload = b'{"v":1}'
    backend.store(key, payload)
    assert backend.retrieve(key) == payload
    backend.delete(key)
    assert backend.retrieve(key) is None
  finally:
    backend.disconnect()
