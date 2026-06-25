# PLAN — Round 3: SQS/Pulsar Real In-Flight-Set Ack (Tier-2)

The biggest Tier-2 item from [`PLAN-2026-06-25.md`](./PLAN-2026-06-25.md): replace the
single-slot ack (the C1 stopgap gate) with real per-message ack, mirroring the Kafka/RabbitMQ
in-flight-set pattern. Companion to round-2 [`INSIGHTS-2026-06-25.md`](./INSIGHTS-2026-06-25.md).

## Why

Round-2's C1 fix was a **gate** (fail-fast `ConfigurationError` for SQS/Pulsar under
`CONCURRENT_REQUESTS>1`). It prevents the silent at-least-once violation but forces
`CONCURRENT_REQUESTS=1` on those backends — no concurrency. Both backends have **native
per-message ack** (SQS `ReceiptHandle` + `delete_message`; Pulsar `message_id` + `acknowledge`/
`negative_acknowledge`), so the single-slot `_last_receipt`/`_last_msg` was an artificial
limitation, not a real constraint. Real ack unlocks safe concurrency and retires the gate for
these two backends (the gate stays as a backstop for any future single-slot backend).

## Design (canonical pattern, from Kafka `backends/kafka.py`)

1. **Ack-token class** (per backend): opaque, `__slots__`, `__eq__`, `__hash__`, `__repr__`
   (Kafka: `_KafkaAckToken(partition, offset, topic)`).
2. **`pop_with_ack(queue_name, timeout) -> (bytes|None, token|None)`**: pop, build token, add to
   in-flight tracker, return `(value, token)`; empty → `(None, None)`.
3. **`ack(queue_name, *, token)`**: if `token`, ack the *specific* message; else legacy
   last-slot fallback (mirror Kafka keeping `_last_record` for `ack(token=None)` callers).
4. **`nack(queue_name, *, token)`**: if `token`, nack the specific message.
5. **In-flight set** (diagnostic — SQS/Pulsar ack each msg independently, unlike Kafka's
   watermark commit, so the set is for leak detection / monitoring, mirroring RabbitMQ's
   `_in_flight_tags`).
6. **`supports_concurrent_ack = True`** → the round-2 gate auto-allows them.

### SQS — `_SqsAckToken(queue_url, receipt_handle)`
- `pop_with_ack`: `receive_message` → token carries the URL it came from (C3 multi-queue
  correctness preserved) + the ReceiptHandle. Add token to `_in_flight: set[_SqsAckToken]`.
- `ack(token)`: `delete_message(QueueUrl=token.queue_url, ReceiptHandle=token.receipt_handle)`;
  `discard` from set.
- `nack(token)`: no-op (SQS re-delivers on visibility timeout) OR
  `change_message_visibility(VisibilityTimeout=0)` for immediate re-delivery — **pick no-op** to
  match the current contract; `discard` from set.
- Stale handle (visibility timeout expired → AWS error on delete): raise `QueueError` (matches
  current + Kafka's raise-on-commit-failure) — at-least-once is preserved by SQS re-delivery.

### Pulsar — `_PulsarAckToken(message_id)`
- `pop_with_ack`: `consumer.receive()` → token carries `msg.message_id()`. Add to
  `_in_flight: set[_PulsarAckToken]`. Keep `_last_msg = msg` for legacy.
- `ack(token)`: `consumer.acknowledge(token.message_id)`; `discard` from set.
- `nack(token)`: `consumer.negative_acknowledge(token.message_id)` (fall back to no-op if the
  client lacks the method — current code already does this); `discard` from set.
- `_PulsarAckToken.__hash__`: identity-based (`id(self)` or store a stable repr) since pulsar
  `MessageId` hashability varies by client version — the set is diagnostic only.

## Units (parallel fan-out; disjoint source files)

### Unit S — `backends/sqs.py` + `tests/test_sqs_backend.py`
- Add `_SqsAckToken`; implement `pop_with_ack`; rewrite `ack`/`nack` to take the token; add
  `_in_flight` set; flip `supports_concurrent_ack = True`. Keep legacy `ack(token=None)` →
  `_last_receipt` fallback + keep `pop()` setting `_last_receipt`.
- **Files**: `src/scrapy_extension/backends/sqs.py`, `tests/test_sqs_backend.py` ONLY.
  **Do NOT touch** `test_scheduler_ack_gate.py` or `test_components.py` (orchestrator owns the
  contract-flip there post-fan-out).

### Unit P — `backends/pulsar.py` + `tests/test_pulsar_backend.py` (+ `test_pulsar_coverage.py`)
- Add `_PulsarAckToken`; implement `pop_with_ack`; rewrite `ack`/`nack`; add `_in_flight` set;
  flip `supports_concurrent_ack = True`. Keep legacy `ack(token=None)` → `_last_msg`.
- **Files**: `src/scrapy_extension/backends/pulsar.py`, `tests/test_pulsar_backend.py`,
  `tests/test_pulsar_coverage.py` ONLY. **Do NOT touch** `test_scheduler_ack_gate.py` /
  `test_components.py`.

### Orchestrator (post-fan-out) — `tests/test_scheduler_ack_gate.py` + `test_components.py`
- Both backends are now `supports_concurrent_ack=True`: remove/invert the "SQS/Pulsar raise under
  CONCURRENT_REQUESTS>1" assertions; the declaration tests now assert `True`. Re-confirm the gate
  still fires for a hypothetical single-slot backend (add a synthetic `requires_ack=True,
  supports_concurrent_ack=False` stub class test so the gate mechanism stays covered).

## Tests (TDD — RED first, then GREEN)

**Unit S** (`test_sqs_backend.py`):
- `pop_with_ack` returns `(body, _SqsAckToken(url, rh))`; empty → `(None, None)`.
- pop 3 under concurrency, ack each by its OWN token → 3 distinct `delete_message` calls with the
  right (QueueUrl, ReceiptHandle) each; `_in_flight` empties. (RED pre-fix: single-slot overwrites.)
- `nack(token)` no-op + discards from set.
- crash-mid-ack: pop 2, ack neither → both stay in `_in_flight` (SQS re-delivers on visibility
  timeout — at-least-once).
- multi-queue: pop from qB, ack(token-from-qB) → deletes qB (C3 correctness preserved).

**Unit P** (`test_pulsar_backend.py`):
- `pop_with_ack` returns `(bytes, _PulsarAckToken(message_id))`; empty → `(None, None)`.
- pop 3, ack each by token → 3 distinct `acknowledge(message_id)` calls; `_in_flight` empties.
- `nack(token)` → `negative_acknowledge(message_id)` (or no-op fallback); discards.
- pop 2, ack neither → both in `_in_flight` (re-deliver on consumer restart — at-least-once).

**Orchestrator** (post-fan-out, `test_scheduler_ack_gate.py`):
- SQS/Pulsar `supports_concurrent_ack is True`; SQS+`CONCURRENT_REQUESTS=16` does NOT raise.
- Synthetic single-slot stub backend + concurrency → gate raises (gate mechanism still covered).

## Acceptance
- `uv run pytest -q --tb=line -p no:randomly` green (existing 1251 + new real-ack tests);
  `-p no:randomly` stable.
- `uv run ruff check src tests` clean; `uv run mypy src/scrapy_extension` clean.
- Each new real-ack test RED pre-fix, GREEN post-fix.
- Independent verifier + code-reviewer approval lane: APPROVE, 0 CRITICAL/HIGH.
- Public API stable: `ack`/`nack`/`pop` signatures unchanged (`token` kwarg already present from
  round-2); only behavior changes (token now USED). New token classes are internal (`_`-prefixed).

## Non-goals (remain Tier-2/3)
- Distributed Delay/Throttle/Bloom; backpressure action hook; entry-point plugin registration;
  `_RedactedStr` parity; Sentinel failover re-discovery; rocketmq-client replacement; B5
  reconnect in-flight-survival test.
