"""RocketMQ integration tests (R2-A4 foundation, part 6 — sextet complete).

Completes the integration sextet (Redis R46, MongoDB R47, ElasticSearch R49,
RabbitMQ R54, Kafka R55, RocketMQ here). This suite is the **verification of
the R7 fix**: pre-R7, ``connect()`` never called ``consumer.start()`` and
``pop()`` never subscribed, so pop *always* returned None — the entire
RocketMQ backend was silently broken, invisible because every test mocked
the consumer. A round-trip against a real broker is the only thing that
proves R7 actually fixed it.

RocketMQ semantics these tests respect (verified against rocketmq.py,
apache rocketmq-python-client 5.1.1 gRPC):
- **Deferred-ack** (initiative #4): ``pop`` / ``pop_with_ack`` return the
  body WITHOUT acking; the caller acks via ``ack(token=msg)``. A crash before
  ack → the broker's invisible-duration window redelivers (at-least-once).
  ``_drain`` acks each message as it arrives.
- ``pop(timeout=t)`` passes ``t`` as the invisible-duration (seconds; default
  15s when t=0). ``receive`` long-polls up to the consumer's await_duration.
- ``queue_len`` raises ``NotImplementedError`` — RocketMQ has no count API;
  counts are verified by popping, not queue_len.
- Topic name is ``{topic_prefix}_{queue_name}``. **RocketMQ topic names
  reject colons**, so this suite uses hyphen-delimited queue names (not the
  ``inttest:`` colon style of the other suites) or pushes fail.

What's pinned
-------------
- ``test_push_pop_round_trip`` — N in → N out, no loss. This is the R7
  verification: pre-R7 this returned 0 (pop always None).
- ``test_pop_empty_returns_none`` — pop on a topic with no messages returns
  None after the receive timeout (no spurious hang/raise).
- ``test_queue_len_raises_not_implemented`` — RocketMQ correctly reports
  queue_len as unsupported (contract honesty).

Running
-------
Skipped by default. Point at a RocketMQ gRPC PROXY (the broker must run
with ``--enable-proxy``, which serves gRPC on 8081). The apache
``rocketmq-python-client`` 5.1.1 client speaks gRPC to the proxy, NOT the
legacy remoting port (10911)::

    SCRAPY_TEST_ROCKETMQ_NAMESRV=localhost:8081 uv run pytest tests/integration -q

Each test uses a UUID-suffixed topic so concurrent runs and leftover data
can't interfere. Consumer/producer groups are unique per module run.

The client is pure-Python (gRPC + protobuf) — no native ``librocketmq`` is
needed (the old ctypes wrapper is gone). The suite skips before any import
when ``SCRAPY_TEST_ROCKETMQ_NAMESRV`` is unset.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_ROCKETMQ_NAMESRV"),
    reason=(
      "Set SCRAPY_TEST_ROCKETMQ_NAMESRV to the gRPC proxy endpoint "
      "(e.g. localhost:8081) to run RocketMQ integration tests."
    ),
  ),
]


def _drain(backend, queue: str, n: int, deadline_s: float = 15.0) -> list:  # type: ignore[no-untyped-def]
  """Poll until ``n`` records consumed or deadline.

  Initiative #4 (at-least-once): the apache ``SimpleConsumer`` uses a
  deferred-ack model — ``pop_with_ack`` returns ``(body, token)`` WITHOUT
  acking; the caller acks via ``ack(token=msg)`` after receiving. This loop
  acks each message as it arrives so the broker doesn't redeliver within the
  invisible-duration window. The loop also absorbs subscription-propagation
  latency: the first receive(s) after ``subscribe`` can return empty until
  the subscription takes effect.
  """
  received: list[bytes] = []
  deadline = time.time() + deadline_s
  while len(received) < n and time.time() < deadline:
    body, token = backend.pop_with_ack(queue, timeout=1.0)  # 1s receive window
    if body is not None:
      backend.ack(queue, token=token)
      received.append(body)
  return received


@pytest.fixture(scope="module")
def rocketmq_backend():  # type: ignore[no-untyped-def]
  """Connect a RocketMQBackend once per module; disconnect on teardown.

  Unique consumer/producer groups per run avoid cross-talk with any real
  ``scrapy-extension-*`` groups or prior runs.
  """
  from scrapy_extension.backends.rocketmq import RocketMQBackend
  from scrapy_extension.settings.rocketmq import RocketMQSettings

  suffix = uuid.uuid4().hex[:8]
  config = RocketMQSettings(
    namesrv_address=os.environ["SCRAPY_TEST_ROCKETMQ_NAMESRV"],
    consumer_group=f"inttest-cg-{suffix}",
    producer_group=f"inttest-pg-{suffix}",
  )
  backend = RocketMQBackend(config)
  backend.connect()  # R7: starts producer AND consumer
  yield backend
  backend.disconnect()


@pytest.fixture
def unique_prefix() -> str:
  """UUID-suffixed namespace → unique topic per test.

  Hyphen-delimited (NOT colon) because RocketMQ topic names reject colons:
  the topic is ``scrapy-queue_{queue_name}``.
  """
  return f"inttest-{uuid.uuid4().hex}"


def test_push_pop_round_trip(rocketmq_backend, unique_prefix):
  """R7 verification: N in → N out, no loss.

  Pre-R7 this returned 0 — ``connect()`` never started the consumer and
  ``pop()`` never subscribed, so receive() always came back empty. Only a
  real broker round-trip proves the subscribe+start fix actually works.
  Deferred-ack: ``_drain`` acks each received message (initiative #4).
  """
  queue = f"{unique_prefix}-rt"
  n = 5
  sent = [f"item-{i:03d}".encode() for i in range(n)]
  for item in sent:
    rocketmq_backend.push(queue, item, priority=0.0)

  received = _drain(rocketmq_backend, queue, n)

  assert len(received) == n
  assert set(received) == set(sent)


def test_pop_empty_returns_none(rocketmq_backend, unique_prefix):
  """pop on a topic with no messages returns None (receive times out)."""
  queue = f"{unique_prefix}-empty"
  # No push. Subscribe + receive should time out → None (not hang, not raise).
  assert rocketmq_backend.pop(queue, timeout=1.0) is None


def test_queue_len_raises_not_implemented(rocketmq_backend, unique_prefix):
  """RocketMQ correctly reports queue_len as unsupported (contract honesty).

  ``queue_len`` raises NotImplementedError rather than returning a misleading
  0 — callers (e.g. ``BackendScheduler.has_pending_requests``) catch this and
  assume pending requests exist.
  """
  with pytest.raises(NotImplementedError, match="queue_len"):
    rocketmq_backend.queue_len(f"{unique_prefix}-qlen")
