"""DynamoDB integration tests (Risk 7 scaffolding).

Mock-based tests provably cannot verify the storage contract that matters most
on this backend:

- The auto-created single-table layout (hash key ``pk``) + app-level TTL
  (``expire_at`` checked on READ) — a mock can't surface a real
  eventually-consistent read or a stale-item-after-TTL regression.
- ``store``/``retrieve``/``exists`` round-trip fidelity through boto3.
- ``ttl()`` semantics (``expire_at`` - now) computed app-side, not via AWS TTL.

Running
-------
Skipped by default. Point at LocalStack (or real AWS) via endpoint_url::

    SCRAPY_TEST_DYNAMODB_ENDPOINT=http://localhost:4566 \
      uv run pytest tests/integration/test_dynamodb_integration.py -q

The test uses a UUID-prefixed key + table namespace so concurrent runs and
leftover data don't interfere.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_DYNAMODB_ENDPOINT"),
    reason=(
      "Set SCRAPY_TEST_DYNAMODB_ENDPOINT (e.g. http://localhost:4566 for "
      "LocalStack) to run DynamoDB integration tests against a live instance."
    ),
  ),
]


def test_store_retrieve_exists_round_trip() -> None:
  """Real-broker round-trip: store → retrieve → exists → delete (storage ABC)."""
  from scrapy_extension.backends.dynamodb import DynamoDBBackend
  from scrapy_extension.settings.dynamodb import DynamoDBMode, DynamoDBSettings

  settings = DynamoDBSettings(
    mode=DynamoDBMode.STANDALONE,
    endpoint_url=os.environ["SCRAPY_TEST_DYNAMODB_ENDPOINT"],
    table_name=f"inttest-{uuid.uuid4().hex[:8]}",
  )
  backend = DynamoDBBackend(settings)
  backend.connect()
  try:
    key = f"inttest:{uuid.uuid4().hex}"
    payload = b'{"v":1}'
    backend.store(key, payload)
    assert backend.retrieve(key) == payload
    assert backend.exists(key) is True
    backend.delete(key)
    assert backend.exists(key) is False
  finally:
    backend.disconnect()
