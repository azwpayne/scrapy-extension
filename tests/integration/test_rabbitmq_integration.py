"""RabbitMQ integration tests (R2-A4 foundation, part 4 — first queue-only backend).

Extends integration coverage past the three storage-capable backends
(Redis R46, MongoDB R47, ElasticSearch R49) to the queue-only backends,
whose delivery semantics are the most mock-opaque in the project.

RabbitMQ is the clearest queue-only target because its ack/nack contract
(R11/R12) is exercised through real AMQP delivery tags — exactly what a
mock cannot reproduce:

- R12 — ``pop`` uses ``basic_get(auto_ack=False)``: the message is **not**
  acked on pop. Ack is signal-driven (``response_received`` → ``ack``). A
  mock can't verify that an unacked message is re-delivered on disconnect.
- R11 — ``nack(requeue=True)`` returns the message to the ready queue for
  another consumer. A mock can't reproduce a real AMQP requeue.
- Priority queue delivery order (``x-max-priority``).

These tests pin:
- ``test_push_pop_round_trip_with_ack`` — N in → N out, no loss; each pop
  acked (pop doesn't auto-ack).
- ``test_priority_ordering`` — higher priority delivered first.
- ``test_nack_requeues_for_retry`` — R11/R12: a nacked message is
  re-delivered (at-least-once).
- ``test_ack_idempotent_when_no_pending`` — R11: ack/nack with no tracked
  tag is a safe no-op.

Running
-------
Skipped by default. Point at a RabbitMQ you don't mind ``inttest:*`` queues
landing in::

    SCRAPY_TEST_RABBITMQ_URL=amqp://localhost:5672/ \
      uv run pytest tests/integration -q

Each test uses a UUID-prefixed queue name so concurrent runs and leftover
queues don't interfere. The helper defaults to ``guest`` only for loopback;
set ``SCRAPY_TEST_RABBITMQ_USERNAME`` and
``SCRAPY_TEST_RABBITMQ_PASSWORD`` for another account. Remote brokers require
an ``amqps://`` URL.
"""

from __future__ import annotations

import os
import time
import uuid
from urllib.parse import urlparse

import pytest

pytestmark = [
  pytest.mark.integration,
  pytest.mark.skipif(
    not os.environ.get("SCRAPY_TEST_RABBITMQ_URL"),
    reason=(
      "Set SCRAPY_TEST_RABBITMQ_URL (e.g. amqp://localhost:5672/) "
      "to run RabbitMQ integration tests against a live instance."
    ),
  ),
]


def _settings_from_url(url: str):  # type: ignore[no-untyped-def]
  """Build RabbitSettings from an AMQP test URL via stdlib urlparse.

  The test-only URL is decomposed so credentials can come from separate
  environment variables and the production URL-userinfo guard remains covered.
  Legacy test URLs containing userinfo remain accepted by this helper only.
  """
  from pydantic import SecretStr

  from scrapy_extension.settings.rabbitmq import RabbitMQSettings

  parsed = urlparse(url)
  tls_enabled = parsed.scheme == "amqps"
  username = os.environ.get("SCRAPY_TEST_RABBITMQ_USERNAME") or parsed.username
  password = os.environ.get("SCRAPY_TEST_RABBITMQ_PASSWORD") or parsed.password
  # amqp://host:port/  → path "/" → vhost "/"; amqp://host:port/vh → "vh"
  vhost = parsed.path.lstrip("/") or "/"
  return RabbitMQSettings(
    host=parsed.hostname or "localhost",
    port=parsed.port or (5671 if tls_enabled else 5672),
    username=username or "guest",
    password=SecretStr(password or "guest"),
    virtual_host=vhost,
    ssl_enabled=tls_enabled,
    ssl_cafile=os.environ.get("SCRAPY_TEST_RABBITMQ_SSL_CAFILE"),
    connection_attempts=1,
    heartbeat=60,
  )


@pytest.fixture(scope="module")
def rabbitmq_backend():  # type: ignore[no-untyped-def]
  """Connect a RabbitMQBackend once per module; disconnect on teardown."""
  from scrapy_extension.backends.rabbitmq import RabbitMQBackend

  backend = RabbitMQBackend(_settings_from_url(os.environ["SCRAPY_TEST_RABBITMQ_URL"]))
  backend.connect()
  yield backend
  backend.disconnect()


@pytest.fixture
def unique_prefix() -> str:
  """UUID-prefixed namespace so tests can't collide with each other or stale queues."""
  return f"inttest:{uuid.uuid4().hex}"


def _wait_for_queue_len(backend, queue: str, expected: int, timeout: float = 5.0) -> int:
  """Poll ``queue_len`` until it reaches ``expected`` or ``timeout`` elapses.

  AMQP ``basic_publish`` is asynchronous at the broker level even on pika's
  ``BlockingConnection`` (the client flushes its send buffer, but the broker
  enqueues the frames on its own schedule — slower on priority queues with
  ``x-max-priority``, which is why a strict ``queue_len == n`` immediately
  after N publishes sees only the settled subset). The queue contract under
  test is "N in → N out, no loss"; that is verified by the pop round-trip
  below. This helper lets the intermediate count check wait for broker
  settle so the test is deterministic without weakening the no-loss claim.
  """
  import time

  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    n = backend.queue_len(queue)
    if n >= expected:
      return n
    time.sleep(0.02)
  return backend.queue_len(queue)


def test_push_pop_round_trip_with_ack(rabbitmq_backend, unique_prefix):
  """N in → N out, no loss. pop uses auto_ack=False (R12), so each pop is acked."""
  queue = f"{unique_prefix}:rt"
  n = 10
  for i in range(n):
    rabbitmq_backend.push(queue, f"item-{i:03d}".encode(), priority=1.0)

  # AMQP publish-settle is broker-async (see _wait_for_queue_len); the strict
  # immediate count is non-deterministic on priority queues. The no-loss
  # contract is proven by the pop round-trip below — this just waits for settle.
  assert _wait_for_queue_len(rabbitmq_backend, queue, n) == n

  popped = []
  for _ in range(n):
    item = rabbitmq_backend.pop(queue, timeout=0.0)
    assert item is not None
    popped.append(item)
    rabbitmq_backend.ack(queue)  # pop does NOT auto-ack (R12)

  assert len(popped) == n
  assert rabbitmq_backend.pop(queue, timeout=0.0) is None  # drained
  assert rabbitmq_backend.queue_len(queue) == 0


def test_priority_ordering(rabbitmq_backend, unique_prefix):
  """Higher priority is delivered first (x-max-priority queue)."""
  queue = f"{unique_prefix}:prio"
  rabbitmq_backend.push(queue, b"low", priority=1.0)
  rabbitmq_backend.push(queue, b"high", priority=10.0)

  first = rabbitmq_backend.pop(queue, timeout=0.0)
  assert first == b"high"
  rabbitmq_backend.ack(queue)

  # drain the remaining message so the queue is clean
  rabbitmq_backend.pop(queue, timeout=0.0)
  rabbitmq_backend.ack(queue)


def test_nack_requeues_for_retry(rabbitmq_backend, unique_prefix):
  """R11/R12: nack(requeue=True) returns the message for re-delivery (at-least-once).

  This is the delivery contract a mock fundamentally cannot reproduce: a
  real AMQP broker tracks the delivery tag and re-queues on nack. The same
  payload must come back.
  """
  queue = f"{unique_prefix}:nack"
  payload = b"retry-me"
  rabbitmq_backend.push(queue, payload, priority=1.0)

  first = rabbitmq_backend.pop(queue, timeout=0.0)
  assert first == payload
  # Message is unacked (auto_ack=False). Nack → requeue for another attempt.
  rabbitmq_backend.nack(queue)

  # Requeue is a broker op; allow a brief settle before re-fetching.
  time.sleep(0.1)
  second = rabbitmq_backend.pop(queue, timeout=0.0)
  assert second == payload  # re-delivered — at-least-once
  rabbitmq_backend.ack(queue)  # clean up


def test_ack_idempotent_when_no_pending(rabbitmq_backend, unique_prefix):
  """R11: ack()/nack() with no tracked delivery tag are safe no-ops.

  Guards against "calling basic_ack with no tag raises a channel error" —
  the backend short-circuits when ``_last_delivery_tag is None``.
  """
  queue = f"{unique_prefix}:ackidem"
  # No message popped on this queue → no tracked tag → must not raise.
  rabbitmq_backend.ack(queue)
  rabbitmq_backend.nack(queue)


def test_clear_rejects_unacked_delivery_then_purges_requeue(
  rabbitmq_backend, unique_prefix
):
  """Purge must not report a clear boundary while an old nack can resurrect work."""
  from scrapy_extension.exceptions import QueueError

  queue = f"{unique_prefix}:clear"
  rabbitmq_backend.push(queue, b"old-work")
  body, token = rabbitmq_backend.pop_with_ack(queue)
  assert body == b"old-work"
  assert token is not None

  with pytest.raises(QueueError, match="in-flight"):
    rabbitmq_backend.clear_queue(queue)

  rabbitmq_backend.nack(queue, token=token)
  rabbitmq_backend.clear_queue(queue)
  assert rabbitmq_backend.queue_len(queue) == 0
