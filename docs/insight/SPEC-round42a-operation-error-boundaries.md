# Round 42A — SPEC / PLAN / TASK: terminal operational-error privacy

**Base:** `main` after `3d8aa69` (Round 41B startup boundaries and diagnostics).

## Audit conclusion

The startup boundaries added in Round 41B deliberately stop at connection and
configuration entry points.  Three independent audits found a separate public
failure path: runtime queue operations can keep SDK exceptions, queue URLs,
topics, payloads, receipt handles, endpoint configuration, and bound backend
methods alive through exception chains and traceback locals.

The immediately reproducible instances are Pulsar receive/producer/settlement,
SQS queue lookup/receive/settlement, and the circuit-breaker proxy wrapper.
The latter can leak a wrapped backend configuration even for an OPEN
fail-fast path, because its closure holds the bound backend method.

## Specification

1. A public queue-operation failure in the Round 42A scope is a terminal
   privacy boundary.  After it crosses the boundary, its `str`, `args`,
   `__dict__`, cause/context, formatted traceback, and package-frame locals
   contain no driver text, topic/queue/URL, endpoint, credential, receipt
   handle, payload, token, or bound backend method.
2. The boundary catches `Exception` only.  `KeyboardInterrupt`, `SystemExit`,
   and all other `BaseException` control flow remain untouched.
3. Input validation stays outside the boundary, so existing `ValueError`
   contracts remain unchanged.  Operational failures preserve `QueueError`
   and its fixed operation (`push`, `pop`, `ack`, `nack`, or `clear_queue`),
   but use static allowlisted messages and do not retain a logical queue ID.
4. Pulsar scope: `push`, `pop`, `pop_with_ack`, `ack`, `nack`, and
   `clear_queue`, including their private receive/producer/consumer/token
   seams once their error reaches a public operation.
5. SQS scope: `push`, `pop`, `pop_with_ack`, `ack`, `nack`, `queue_len`, and
   `clear_queue`, including queue URL lookup and both legacy and token
   settlement paths once their error reaches a public operation.
6. Circuit-breaker proxy calls must reconstruct escaped `BackendError`
   subclasses after `CircuitBreaker.call()` unwinds.  The proxy must not use a
   closure which retains a bound backend method in the published traceback.
   It preserves the error class and safe operation metadata; an OPEN error
   exposes a fixed internal-safe name rather than an arbitrary caller name.
7. Successful results, no-message/timeout behavior, retryability of failed
   acknowledgement tokens, and breaker state transitions remain unchanged.

## Plan

1. Add reusable terminal reconstruction helpers in `exceptions/_redaction.py`.
2. Apply the queue boundary to the public Pulsar and SQS methods, preserving
   validation before I/O and only a finite set of static messages.
3. Replace the proxy closure with a protected callable object that clears its
   reference graph before raising a reconstructed backend error.
4. Add marker tests that inspect exception text, attributes, chains, formatted
   traceback, and package-frame locals, including the OPEN breaker path.
5. Run focused backend/breaker tests, then static checks and the full
   non-integration suite; create one atomic commit.

## Task checklist

- [ ] Shared queue/backend operation reconstruction helpers.
- [ ] Pulsar public operation boundary and tests.
- [ ] SQS public operation boundary and tests.
- [ ] Protected circuit-breaker bound-operation wrapper and tests.
- [ ] Focused and full verification; atomic commit.

## Explicit follow-up

The same audit identifies analogous operation-boundary work for Redis, Kafka,
RabbitMQ, RocketMQ, MongoDB, Elasticsearch, DynamoDB, and Memcached.  It is
intentionally a separate atomic follow-up: Round 42A first establishes and
tests the shared primitive on the highest-evidence runtime paths rather than
mixing unrelated backend behavior changes into one review unit.
