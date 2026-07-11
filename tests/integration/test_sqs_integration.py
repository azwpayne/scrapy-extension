"""SQS integration tests (Risk 7 scaffolding).

Mock-based tests provably cannot verify the queue contract that matters most
on this backend:

- Standard-queue visibility-timeout + delete-on-ack semantics — a mock can't
  surface a real redelivery-after-visibility-timeout or an
  ``ApproximateNumberOfMessages`` depth drift (SQS depth is eventually
  consistent).
- ``push``/``pop`` round-trip + the per-message ``_SqsAckToken`` (ReceiptHandle)
  idempotent-ack contract under ``CONCURRENT_REQUESTS > 1``.
- MessageBody = base64 of item bytes through boto3's serialization boundary.

Running
-------
Skipped by default. Point at LocalStack (or real AWS) via endpoint_url::

    SCRAPY_TEST_SQS_ENDPOINT=http://localhost:4566 \
      uv run pytest tests/integration/test_sqs_integration.py -q

The test uses a UUID-prefixed queue name so concurrent runs don't interfere.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_SQS_ENDPOINT"),
    reason=(
      "Set SCRAPY_TEST_SQS_ENDPOINT (e.g. http://localhost:4566 for "
      "LocalStack) to run SQS integration tests against a live instance."
    ),
  ),
]


def test_push_pop_round_trip() -> None:
  """Real-broker round-trip: push → pop (queue ABC contract, base64 MessageBody)."""
  from scrapy_extension.backends.sqs import SqsBackend
  from scrapy_extension.settings.sqs import SqsMode, SqsSettings

  settings = SqsSettings(
    mode=SqsMode.STANDALONE,
    endpoint_url=os.environ["SCRAPY_TEST_SQS_ENDPOINT"],
  )
  backend = SqsBackend(settings)
  backend.connect()
  try:
    queue_name = f"inttest-{uuid.uuid4().hex[:8]}"
    payload = b'{"v":1}'
    backend.push(queue_name, payload)
    # SQS visibility/propagation is eventually consistent — pop blocks on the
    # timeout param for up to 10s for the message to become visible.
    popped = backend.pop(queue_name, timeout=10.0)
    assert popped == payload
  finally:
    backend.disconnect()
