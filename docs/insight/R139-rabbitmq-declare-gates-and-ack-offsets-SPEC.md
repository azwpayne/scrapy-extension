# R139 SPEC — RabbitMQ declare gates + Kafka legacy-ack offsets + cadence/doc repairs

> Plan: [R139-rabbitmq-declare-gates-and-ack-offsets-PLAN.md](R139-rabbitmq-declare-gates-and-ack-offsets-PLAN.md)
> Tasks: [R139-rabbitmq-declare-gates-and-ack-offsets-TASK.md](R139-rabbitmq-declare-gates-and-ack-offsets-TASK.md)

## Context

R139 rotated onto the never-deep-scanned full-file surfaces of kafka, sqs,
rabbitmq, pulsar, and batched storage (diagnostics theme exhausted long ago;
everything else fresh), plus a self-scan of the landed R138 diff
(`677239b..437f9c4`) and the standing ndiff sweep. 7 opus finders → 7
findings → **7 adversarially CONFIRMED** (verifiers checked library sources in
the venv — kafka-python-ng and pulsar-client — not memory). The R138 self-scan,
sqs, and ndiff surfaces returned **zero findings** (R138's fixes introduced no
regressions; the self-review streak ends at two). All seven findings live in
files OUTSIDE the user's dirty tree — nothing is DIRTY-BLOCKED this round.

## Findings

### F1 (MED, semantics): Kafka legacy `ack(token=None)` bare commit sweeps every partition's position

`kafka.py:1432-1442` — the legacy path calls `self._consumer.commit()` with no
offsets. In kafka-python-ng (pinned >=2.2.2,<3), `commit(offsets=None)`
substitutes `all_consumed_offsets()` = the CURRENT FETCH POSITION of EVERY
assigned partition (venv `kafka/consumer/group.py:511,533`;
`subscription_state.py:313-319`). `poll()` advances position past every record
it returns, so those positions include `pop_with_ack` records still sitting
un-acked in `_in_flight`. The existing fence (pop_with_ack nulling
`_last_record`, :1321-1324, "Token and legacy settlement modes must not share a
bare-commit slot") only covers the immediate nack-then-bare-ack adjacency; any
later legacy `pop()` re-arms `_last_record` (:1260) and the bare commit then
advances the committed offsets PAST concurrently in-flight token records.
Crash/rebalance afterwards → those records are never redelivered: silent data
loss violating the class-docstring at-least-once contract (:298-304). The
in-code comment "commit the last-popped record wholesale" mischaracterizes the
full-position sweep. Verifier confirmed no in-repo caller mixes the modes (all
queue strategies thread tokens) — the trigger is documented-compat external
legacy pop/ack on the ConnectionManager-shared instance.

### F2 (HIGH, semantics): RabbitMQ `queue_len` passive-declares without the declare gate

`rabbitmq.py:1506-1510` — `queue_len` calls `channel.queue_declare(passive=True)`
without the `_ensure_queue_exists` gate that `push` (:1122) and `_basic_get`
(:1310) use. On a queue that does not exist, the broker answers the passive
declare with a 404 channel-closing exception: `queue_len` raises QueueError
instead of returning 0 per the shared `QueueBackend` contract (base.py:667-675;
sibling Redis `zcard` returns 0 for a missing key), the channel dies, and every
subsequent push/pop/ack on the session fails with "Not connected to RabbitMQ"
until a full reconnect. Realistic trigger: cold-start depth probe
(`BackendQueue._probe_depth`, first uninitialized probe) before any push has
declared the queue.

### F3 (MED, semantics): RabbitMQ `clear_queue` purges without the gate

`rabbitmq.py:1556-1564` — same root cause, distinct method and contract:
`queue_purge` of a never-created queue 404s (raising QueueError where Redis
`DEL`/Mongo `delete_many` are silent no-ops) and kills the channel. Trigger:
teardown/runbook "clear-queue" on a fresh environment before any push.

### F4 (MED, semantics): RabbitMQ QoS knobs are inert — and prefetch_size actively breaks the channel

`rabbitmq.py:911-921` — `_apply_qos` sends `basic_qos(prefetch_count,
prefetch_size)` and `RabbitMQSettings` advertises both knobs
(settings/rabbitmq.py:371-381), but every pop goes through `basic_get`
(:1312-1315): RabbitMQ prefetch governs only `basic_consume` push deliveries
(no code path uses it), so the advertised prefetch bound never applies.
Worse: RabbitMQ does NOT implement byte-based prefetch — a nonzero
`prefetch_size` against a real broker closes the just-opened channel with
NOT_IMPLEMENTED/PRECONDITION_FAILED (unit-test mocks mask this). The
`confirm_delivery` half of `_prepare_channel` is effective; only the QoS half
is inert.

### F5 (LOW, semantics): RabbitMQ pop timeout docstring says "unused" but timeout is honored

`rabbitmq.py:1174,1203` — "timeout: Seconds to wait (unused for RabbitMQ,
blocking not supported)" contradicts the implementation (:1290 deadline,
:1331-1348 50ms-slice polling until the deadline) and the ABC contract
(base.py:657). A caller believing the docstring passes a large timeout and
gets a blocking call that stalls the thread (the engine loop in a crawl) for
up to that window. Docs-only; a test already pins the real behavior.

### F6 (MED, semantics): Pulsar docstring promises unacked-timeout redelivery that is never configured

`pulsar.py:1030-1031,1061,1083-1084` — nack's docstring and token comments
promise redelivery "on the unacked-timeout / consumer restart", but the
subscribe call (:1181-1189) passes only consumer_type, initial_position, and
negative_ack_redelivery_delay_ms; `unacked_messages_timeout_ms` is never set
anywhere and PulsarSettings has no such field (the pinned client's subscribe
supports it, venv pulsar 3.13.0 `__init__.py:1223,1392-1393`, opt-in only, must
be >10000 ms). A live consumer that abandons an ack accumulates unacked
messages with NO timeout redelivery — stuck-until-restart, degrading the
documented at-least-once liveness and misleading operators.

### F7 (LOW, semantics): Batched age-flusher worst-case latency is ~2x the configured cap

`batched.py:584-589` — the flusher wakes at `self._stop.wait(timeout=age)`:
the check interval EQUALS the cap, and the flush condition requires the oldest
item's age to already be >= age. An item accepted just after a wake misses the
next check, so time-in-volatile-memory is uniformly distributed in (age, 2*age)
— the runbook's own example (`SCRAPY_STORAGE_BUFFER_MAX_AGE_S=60.0`) really
bounds crash-loss at ~119s, ~2x what the operator configured. No store-side
wakeup exists (`_stop` is set only by close()). Tests never observe the
cadence math (tiny ages + single-cycle direct calls).

## Fix design

**Fix A (F1, kafka.py)** — make the legacy bare ack honest and bounded:
- If the legacy record's topic-partition has ANY un-acked in-flight token
  offsets, raise a typed QueueError refusing the mixed-mode bare commit
  (extends the existing "must not share a bare-commit slot" invariant from
  adjacency to concurrency; a purely-legacy caller is unaffected).
- Otherwise commit an EXPLICIT offset map for `_last_record`'s
  topic-partition only (`{tp: OffsetAndMetadata(record.offset + 1, ...)}`,
  matching the shape `_ack_token` already uses) instead of the bare
  `commit()` — other partitions' positions are never swept.
- Correct the :1433 comment to describe the explicit-offset semantics.

**Fix B (F2+F3, rabbitmq.py)** — add `self._ensure_queue_exists(queue_name)`
before the passive declare in `queue_len` and before `queue_purge` in
`clear_queue`, mirroring push/_basic_get. `queue_len` of a fresh queue then
returns 0 (declare creates it empty); `clear_queue` of a fresh queue purges a
just-declared empty queue (no-op, no error), and neither path can 404-kill
the session channel.

**Fix C (F4, settings/rabbitmq.py + rabbitmq.py)** — fail fast on the
actively-harmful knob and stop advertising the inert one as effective:
- `RabbitMQSettings` gains a validator rejecting `prefetch_size != 0`
  (R45-style configuration error: RabbitMQ does not implement byte-based
  prefetch; a nonzero value closes the channel at connect).
- `_apply_qos` keeps sending `basic_qos(prefetch_count, prefetch_size=0)`
  when prefetch_count > 0 (harmless), and the settings docstrings note that
  prefetch governs push deliveries only — this backend consumes via
  `basic_get`, which prefetch does not bound.

**Fix D (F5, rabbitmq.py)** — correct the two "unused for RabbitMQ" timeout
docstring entries to the implemented deadline-honoring semantics.

**Fix E (F6, pulsar.py)** — correct the nack docstring and the two token
comments to drop the never-configured "unacked-timeout" claim: redelivery of
an abandoned ack happens on consumer restart/disconnect (stale-token consumer
replacement included). The optional `unacked_messages_timeout_ms` setting is
recorded in the LEDGER as DEFERRED (a real configuration-surface addition,
design-gated like R137-F6).

**Fix F (F7, batched.py)** — deadline-driven cadence: before each wait,
compute the interval from the current `_oldest_ts` under the lock — empty
buffer sleeps a full `age`; otherwise sleeps exactly the remaining budget
(clamped to a small floor). Worst-case time-to-flush becomes `age` + one
wait-wakeup epsilon instead of (age, 2*age). Docstring notes the cadence.

## Acceptance

RED tests for F1-F7 on the current tree; GREEN after; full gate green (ruff
check, ruff format, `uv run --frozen pytest`, `mypy --strict src`); atomic
commits (one per backend/surface + docs); LEDGER rows (6 LANDED + 1
DEFERRED-part for F6's optional setting); memory round entry; R138 self-scan
CLEAN noted.
