"""Pulsar integration tests (Risk 7 scaffolding).

Mock-based tests provably cannot verify the queue contract that matters most
on this backend:

- Shared-subscription competing-consumers semantics — a mock can't surface a
  real redelivery-after-ack-timeout or a MessageId-idempotent-ack regression.
- ``push``/``pop`` round-trip + the per-message ``_PulsarAckToken`` (MessageId)
  contract under ``CONCURRENT_REQUESTS > 1``.
- ``queue_len()`` is unsupported (the client exposes no broker-side depth) —
  documented as always-0; only a real broker confirms the documented
  limitation, not a silent regression to a wrong value.

Running
-------
Skipped by default. Point at a Pulsar broker via service_url::

    SCRAPY_TEST_PULSAR_URL=pulsar://localhost:6650 \
      uv run pytest tests/integration/test_pulsar_integration.py -q

The test uses a UUID-prefixed topic so concurrent runs don't interfere.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_PULSAR_URL"),
    reason=(
      "Set SCRAPY_TEST_PULSAR_URL (e.g. pulsar://localhost:6650) to run "
      "Pulsar integration tests against a live broker."
    ),
  ),
]


def test_push_pop_round_trip() -> None:
  """Real-broker round-trip: push → pop (queue ABC, Shared subscription)."""
  from scrapy_extension.backends.pulsar import PulsarBackend
  from scrapy_extension.settings.pulsar import PulsarMode, PulsarSettings

  settings = PulsarSettings(
    mode=PulsarMode.STANDALONE,
    service_url=os.environ["SCRAPY_TEST_PULSAR_URL"],
    subscription_name=f"inttest-{uuid.uuid4().hex[:8]}",
  )
  backend = PulsarBackend(settings)
  backend.connect()
  try:
    topic = f"inttest-{uuid.uuid4().hex[:8]}"
    payload = b'{"v":1}'
    backend.push(topic, payload)
    # pop blocks on the timeout param for up to 10s for the message to arrive.
    popped = backend.pop(topic, timeout=10.0)
    assert popped == payload
  finally:
    backend.disconnect()
