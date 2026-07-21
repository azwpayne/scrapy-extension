"""Kafka integration tests (R2-A4 foundation, part 5).

Kafka is the hardest backend to integration-test (and the most mock-opaque):
its "queue" is a partitioned log consumed via a consumer group, and its
ack/nack contract (R11/R12) is built on offset commits — exactly what a
mock cannot reproduce.

Key Kafka semantics these tests respect (verified against kafka.py before
writing):
- ``pop`` lazily creates a consumer, subscribes, and polls. The **first
  poll(s) after subscribe return empty** until the consumer-group join +
  partition assignment completes — so a naive ``push; pop`` gets ``None``.
  The round-trip test uses a poll-loop with a deadline.
- ``ack`` = ``consumer.commit()`` — commits the offset so the message
  isn't re-delivered on the next consumer restart (R11/R12).
- ``nack`` is an **in-session no-op** (clears the tracked record); the
  uncommitted offset means re-delivery on restart (R11/R12).
- Priority = **partition selection** (``min(priority, max_priority_partitions-1)``).
  Kafka gives NO cross-partition ordering guarantee, so — unlike
  Redis/MongoDB/ES/RabbitMQ — priority *ordering* is not asserted here.

What's pinned
-------------
- ``test_push_pop_round_trip_with_ack`` — N in → N out, no loss, each acked
  (commit). The poll-loop handles the consumer-group join latency.
- ``test_cold_start_depth_sees_existing_backlog`` — a fresh group with the
  default earliest reset reports pre-existing records before its first poll.
- ``test_ack_idempotent_when_no_pending`` — R11: ack/nack with no tracked
  record is a safe no-op.

Deferred (need multi-consumer / restart orchestration — too fiddly to write
blind reliably; recommend adding against a live Kafka):
- offset-commit durability: ack on consumer A → a fresh consumer B with the
  same group_id sees nothing to re-deliver.
- nack-restart: pop without ack → fresh consumer with the same group_id
  re-delivers the uncommitted message.

Running
-------
Skipped by default. Point at a Kafka broker you don't mind ``scrapy-inttest-*``
topics landing in::

    SCRAPY_TEST_KAFKA_BOOTSTRAP=localhost:9092 uv run pytest tests/integration -q

Each test uses a UUID-prefixed topic name (Kafka topics are
``scrapy-{queue_name}``) so concurrent runs and leftover data can't interfere.
The consumer group is also unique per module run to avoid offset cross-talk.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_KAFKA_BOOTSTRAP"),
    reason=(
      "Set SCRAPY_TEST_KAFKA_BOOTSTRAP (e.g. localhost:9092) to run Kafka "
      "integration tests against a live broker."
    ),
  ),
]


def _drain(backend, queue: str, n: int, deadline_s: float = 15.0) -> list:  # type: ignore[no-untyped-def]
  """Poll until ``n`` records consumed or deadline; ack each.

  Handles Kafka consumer-group join + partition-assignment latency: the
  first poll(s) after subscribe routinely return empty until assignment
  completes, so a single ``pop`` would spuriously return None.
  """
  received: list[bytes] = []
  deadline = time.time() + deadline_s
  while len(received) < n and time.time() < deadline:
    item = backend.pop(queue, timeout=1.0)  # 1s poll gives the broker time
    if item is not None:
      received.append(item)
      backend.ack(queue)  # commit the offset (pop does NOT auto-ack, R12)
  return received


@pytest.fixture(scope="module")
def kafka_backend():  # type: ignore[no-untyped-def]
  """Connect a KafkaBackend once per module; disconnect on teardown.

  Uses a unique consumer ``group_id`` per module run so test offsets don't
  cross-talk with any real ``scrapy-extension`` group or prior runs.
  """
  from scrapy_extension.backends.kafka import KafkaBackend
  from scrapy_extension.settings.kafka import KafkaSettings

  config = KafkaSettings(
    bootstrap_servers=os.environ["SCRAPY_TEST_KAFKA_BOOTSTRAP"],
    group_id=f"inttest-{uuid.uuid4().hex[:8]}",
    session_timeout_ms=10000,
    request_timeout_ms=10000,
  )
  backend = KafkaBackend(config)
  backend.connect()
  yield backend
  backend.disconnect()


@pytest.fixture
def unique_prefix() -> str:
  """UUID-suffixed namespace → unique topic (scrapy-{prefix}-...) per test.

  Hyphen-delimited (NOT colon): Kafka topic names allow only ``[a-zA-Z0-9._-]``
  (``_validate_topic_name``), so the topic ``scrapy-{queue_name}`` rejects the
  ``inttest:`` colon style the other suites use. Matches R56's RocketMQ fix.
  """
  return f"inttest-{uuid.uuid4().hex}"


def test_push_pop_round_trip_with_ack(kafka_backend, unique_prefix):
  """N in → N out, no loss. ack commits each offset (pop does NOT auto-ack, R12).

  The poll-loop (``_drain``) handles the consumer-group join + partition-
  assignment latency that makes a single pop spuriously return None. Kafka
  preserves order within a partition; with priority=0 all messages land in
  partition 0, but we compare as sets to stay robust to poll batching.
  """
  queue = f"{unique_prefix}-rt"
  n = 5
  sent = [f"item-{i:03d}".encode() for i in range(n)]
  for item in sent:
    kafka_backend.push(queue, item, priority=0.0)

  received = _drain(kafka_backend, queue, n)

  assert len(received) == n
  assert set(received) == set(sent)


def test_cold_start_depth_sees_existing_backlog(kafka_backend, unique_prefix):
  """Fresh earliest groups must not turn a pre-existing backlog into false 0."""
  queue = f"{unique_prefix}-cold-depth"
  sent = [b"cold-0", b"cold-1", b"cold-2"]
  for item in sent:
    kafka_backend.push(queue, item, priority=0.0)

  assert kafka_backend.queue_len(queue) == len(sent)


def test_ack_idempotent_when_no_pending(kafka_backend, unique_prefix):
  """R11: ack()/nack() with no tracked record are safe no-ops.

  Guards against committing with no consumed offset (the backend
  short-circuits when ``_last_record is None``). Kafka's nack is always an
  in-session no-op; ack with nothing pending must not raise.
  """
  queue = f"{unique_prefix}-ackidem"
  # No record polled on this topic → no tracked record → must not raise.
  kafka_backend.ack(queue)
  kafka_backend.nack(queue)
