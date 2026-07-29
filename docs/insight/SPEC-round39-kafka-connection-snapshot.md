# Round 39C — SPEC / PLAN / TASK: Kafka immutable connection generation

**Base:** `main` after Round 39B's authenticated Redis transport boundary.
**Scope:** the Kafka TLS time-of-check/time-of-use P1 identified by the fresh
architecture/security swarm.

## Audit conclusion

Kafka revalidated `ssl_check_hostname` and then reread the mutable settings
object while constructing producer, admin, persistent consumer, and temporary
depth-query consumer clients. A concurrent or post-validation mutation could
therefore send `ssl_check_hostname=False` to kafka-python despite a successful
security check.

## Specification

1. Every Kafka client construction uses one frozen, strictly revalidated
   `_KafkaConnectionSnapshot`. It includes resolved bootstrap servers,
   transport/authentication, producer, consumer, and topic-policy inputs.
2. Snapshot capture copies the settings field mapping, validates it with
   `KafkaSettings.model_validate(..., strict=True)`, and turns invalid mutable
   state into a named, value-free `ConfigurationError`.
3. Passwords and Confluent credentials retained for SDK construction are
   repr-redacted. Errors and snapshot representations must not expose them.
4. A successful `connect()` publishes its snapshot only after producer and
   admin construction succeed. During construction an internal pending snapshot
   keeps all helper calls coherent; failure and disconnect clear both states.
5. Producer, admin, normal consumer, and temporary `queue_len()` consumer use
   the published snapshot. Direct lazy-consumer use without a connected
   producer/admin captures one validated snapshot immediately before SDK I/O.
6. TLS verification remains mandatory for TLS/Confluent modes. Confluent SDK
   configs explicitly include `ssl_check_hostname=True`, rather than relying
   on a client-library default.
7. Connection-related configuration changes become generation-scoped: callers
   must `disconnect()` and then `connect()` to apply them to a new generation.

## Plan and tasks

1. Add a frozen snapshot type and one strict capture/revalidation function.
2. Refactor bootstrap, producer, admin, and security builders to consume the
   supplied snapshot rather than `self.config`.
3. Route lazy and temporary consumers through the published snapshot; preserve
   direct private-helper compatibility with an on-demand capture fallback.
4. Freeze producer acknowledgement and topic policy inputs with the generation
   so post-connect mutation cannot create a mixed client/policy generation.
5. Add deterministic race, runtime downgrade, Confluent, consumer, temporary
   consumer, and redaction regressions.
6. Run focused Kafka suites, static/security checks, package build, the full
   non-integration test suite, then GitHub Actions before an atomic commit.

## Acceptance evidence

- A mutation immediately after validation cannot change the TLS kwargs that
  reach a client builder.
- Mutating `ssl_check_hostname=False` before `connect()` fails before producer
  or admin SDK I/O and never exposes credentials.
- Mutating it after a successful connect leaves late persistent and temporary
  consumers pinned to the verified snapshot.
- Confluent producer and admin configs explicitly carry `SASL_SSL` and hostname
  verification.
