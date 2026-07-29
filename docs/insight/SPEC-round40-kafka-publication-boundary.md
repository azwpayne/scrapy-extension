# Round 40A — SPEC / PLAN / TASK: Kafka candidate-publication boundary

**Base:** `main` after Round 39C's frozen Kafka connection snapshot.
**Scope:** close the fresh-audit race in which public operations could read a
connection snapshot before producer/admin construction had succeeded.

## Audit conclusion

Round 39C retained an instance-level `_connecting_snapshot` so private
connection helpers could consume the same plan. A concurrent `queue_len()` or
lazy `pop()` could read that unpublished candidate while `KafkaProducer` was
still blocked. If the candidate later failed, a consumer had already been
constructed from a generation that never became connected.

## Specification

1. A candidate snapshot belongs only to its `connect()` call until both the
   producer and admin client have been constructed successfully.
2. `connect()` passes that local immutable snapshot explicitly to its private
   mode-specific builders; no instance-level pending snapshot is published.
3. Public and lazy paths serialize with a concurrent `connect()` before
   reading generation state. They use only a successfully published snapshot;
   without any connection attempt, they capture and strictly validate a fresh
   direct-use snapshot before SDK I/O.
4. A concurrent configuration downgrade cannot create a mixed generation: the
   operation waits for the already-validated candidate to publish, then uses
   that immutable generation, or it sees the failed connection boundary.
5. Success diagnostics must not interpolate broker endpoints, which are
   configuration input and may contain malformed secret-bearing authority text.

## Plan and tasks

1. Remove the shared pending-snapshot field and route the local candidate
   explicitly through each connect builder.
2. Restrict snapshot fallback to published-or-fresh capture and remove unused
   live-settings helper readers that could bypass the generation boundary.
3. Make connection success diagnostics static.
4. Add a deterministic blocked-producer race: mutate TLS verification while
   connection is incomplete, call `queue_len()`, assert no consumer is built
   until the original connection publishes, then assert the consumer receives
   the captured verified TLS configuration.
5. Run the Kafka regression suite, lint, strict typing, and the full final
   verification gate before the atomic commit.

## Acceptance evidence

- A candidate cannot be observed by concurrent public work.
- The original connection can still complete from its captured verified plan.
- An overlapping consumer waits for publication and receives the verified
  generation rather than a mutable candidate or live setting.
- Successful Kafka logs do not echo broker endpoint input.
