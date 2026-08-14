# R132 SPEC — concurrent-disconnect handle races (rocketmq subscription set + mongodb collection handles)

> Plan: [R132-concurrent-disconnect-handle-races-PLAN.md](R132-concurrent-disconnect-handle-races-PLAN.md)
> Tasks: [R132-concurrent-disconnect-handle-races-TASK.md](R132-concurrent-disconnect-handle-races-TASK.md)

## Context

R132 scan (5 opus finders + adversarial verify, rotated surfaces: 8 backends +
circuit_breaker/registry/connectors + components + utils). 4 dimensions empty;
the concurrency dimension produced 2 confirmed findings, both hand-verified
against the quoted source at HEAD `e606f1c`. Neither is registered in
`LEDGER.md` (checked R23-R131); no R132 row exists.

Both findings are the same root class — **check-then-use on a mutable backend
handle without the lock that guards the mutation** — and both have a
same-repo precedent contract: rocketmq's TOCTOU race guards
(`tests/test_rocketmq_resilience.py`) and its generation checks in the ack
path (`rocketmq.py:759-790`).

## Finding A (HIGH) — rocketmq `_subscribed_topics` cross-generation poisoning

`_receive_delivery` captures `consumer = self._consumer` without
`_connection_lock` (rocketmq.py:606) and calls `_ensure_subscribed`
(rocketmq.py:611), which performs an **unlocked check-then-act** on the shared
`_subscribed_topics` set (rocketmq.py:532-542):

1. membership check → absent;
2. `consumer.subscribe(topic_name)` — blocking gRPC route retrieval
   (verified in rocketmq-python-client 5.1.1 `v5/consumer/consumer.py:63-82`);
3. `self._subscribed_topics.add(topic_name)`.

`disconnect()` (rocketmq.py:430-440) and `_abort_partial_connect()`
(rocketmq.py:412-426) both clear the set and replace/null the consumer under
`_connection_lock` — a lock the pop path never takes. If a disconnect+reconnect
completes while step 2 is in flight on the old consumer, step 3 writes the
topic into the **new** generation's cleared set. Every subsequent pop then
skips `subscribe()` on the live consumer:

- fresh `SimpleConsumer` has no subscription → `receive` raises
  `IllegalArgumentException` on every pop → persistent `QueueError` that never
  self-heals; or
- if another topic was subscribed meanwhile → `__select_topic_for_receive`
  round-robins only over actually-subscribed topics → **silent starvation**:
  pop returns None forever despite pending messages.

Narrow window (requires subscribe in-flight during teardown) but permanent
impact when hit. Not intentional asymmetry: the ack path already validates
`token.generation` / consumer identity for exactly this class of race.

**Fix**: after `subscribe()` returns, take `_connection_lock` and re-check the
consumer identity; only add the topic when `self._consumer is consumer`.
On mismatch raise a clean typed `QueueError` (retry-able) instead of
proceeding on a detached consumer. The blocking network I/O stays OUTSIDE the
lock, so in-flight subscribes never block disconnect.

## Finding B (LOW) — mongodb collection-handle TOCTOU leaks raw `AttributeError`

Every MongoDB operation method does
`if self._queue_collection is None: raise BackendConnectionError(...)` and then
**re-reads the same attribute** for the call
(e.g. mongodb.py:1090 guard, mongodb.py:1095 use). `_discard_client()`
(mongodb.py:576-582) None-izes all three collection handles under
`_connection_lock` — which only `connect()` / `_discard_client()` ever take,
never the operation methods. A concurrent disconnect (ConnectionManager
health-probe stale-backend disconnect at connectors.py:1933, registry-victim
eviction at connectors.py:1461, or close racing the BatchedStorageStrategy
flusher thread) landing in the check→use window makes the call raise a raw
`AttributeError: 'NoneType' object has no attribute ...`.

The boundary's `handled_exception_types=(QueueError, BackendConnectionError)`
re-raises `AttributeError` raw (`exceptions/_redaction.py:120-127`), and
`BackendScheduler.next_request`'s degradation arm catches only
`QueueError`/`BackendConnectionError`/`CircuitBreakerOpenError`
(scheduler.py:2391-2401) — so the crawl dies with an unhandled, misleading
error instead of degrading gracefully.

Same contract RocketMQ already enforces for the identical race
(`tests/test_rocketmq_resilience.py:94-110`: "push must raise a clean
QueueError rather than AttributeError on None.send()").

**Fix**: capture the handle into a local before the guard
(`collection = self._queue_collection`) and use the local for the call, in all
16 operation-method guard sites (mongodb.py:1053, 1090, 1143, 1172, 1202,
1246, 1283, 1320, 1351, 1391, 1431, 1478, 1514, 1545, 1580, 1618). A discard
landing mid-call then leaves the local pointing at a closed-but-typed handle,
whose operations raise `PyMongoError` subclasses — caught by the existing
`except PyMongoError` arms and normalized to typed errors. Setup paths
(`_create_indexes`, `_assert_collections`) run under `_connection_lock` during
connect and are out of scope.

## Non-goals

- No signature changes to public methods; `_ensure_subscribed` keeps its
  current parameters (the identity re-check needs only the `consumer` it
  already receives).
- No new locks around operation bodies; no lock held across network I/O.
- Dirty user files (queue.py, kafka.py, delay.py, time_wheel.py, connectors.py
  dirty region) untouched.

## Acceptance

- RED tests reproduce both windows deterministically (see PLAN).
- Full gate green: `ruff check`, `ruff format --check src tests conftest.py`,
  `uv run --frozen pytest`, `mypy --strict src`.
- Two atomic commits, ff-merged to main, pushed; `LEDGER.md` rows added.
