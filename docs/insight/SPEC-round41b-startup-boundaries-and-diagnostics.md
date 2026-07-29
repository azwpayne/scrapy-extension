# Round 41B — SPEC / PLAN / TASK: startup boundaries and diagnostic privacy

**Base:** `main` after `172347c` (Round 41A configuration/plugin redaction).

## Audit conclusion

Three independent audits found that the remaining direct backend startup APIs
were not protected by a terminal error boundary.  A caller can mutate a valid
settings object after construction, then call a backend's public `connect()`;
the resulting public `ConfigurationError` retains the backend instance and its
full configuration in traceback-frame locals.  Kafka, RabbitMQ, Pulsar,
DynamoDB, and SQS also rendered an arbitrary mutated `mode` value directly.

The same audit found successful and failure diagnostics that include endpoint
or driver-controlled data: RabbitMQ cluster hosts and HA values, Pulsar service
URLs/timeouts, Memcached host/port, and SQS's `logger.exception` traceback.

## Specification

1. Direct `connect()` for Kafka, Redis, RabbitMQ, Pulsar, Memcached, DynamoDB,
   SQS, and RocketMQ must be a terminal configuration boundary.  It rebuilds a
   `ConfigurationError` after backend frames unwind and only retains a verified
   bundled setting name.  Existing `BackendConnectionError` remains typed but
   is rebuilt after its source frames unwind.
   A non-missing RocketMQ dependency `ImportError` keeps its established object
   identity and message, but must have its accumulated backend traceback and
   exception chain removed before publication.
2. Direct configuration snapshots use the same boundary so their supported
   private/test seam cannot retain raw inputs either.
3. Mutable mode rejection is static (`Unsupported <Backend> mode.`) with no
   user value in `message`, `setting_value`, exception chain, or traceback.
   The fixed policy text emitted by the shared AWS validators remains
   actionable, but only through an explicit allowlist; endpoint and credential
   values themselves never survive the boundary.
4. Constructor-only Kafka auto-commit rejection and RocketMQ unsupported
   capability guards must drop the supplied configuration before publishing
   their static error.
5. Startup diagnostics must never render endpoint addresses, arbitrary policy
   values, queue names, driver exceptions, or driver tracebacks.  Safe mode
   labels and fixed explanatory messages remain available.

## Plan and tasks

1. Add one shared-boundary composition to each direct backend startup path;
   retain only model-declared field names and static mode messages.
2. Replace mutable mode interpolation and direct constructor-frame retention.
3. Replace RabbitMQ/Pulsar/Memcached/SQS diagnostic rendering with static or
   verified-enum diagnostics.
4. Add marker regressions for public `connect()`, direct snapshot seams, and
   log records; assert no marker in error text, attributes, chains, package
   traceback locals, `SecretStr` internals, or logging `exc_info`.
5. Run targeted backend suites, full non-integration coverage, static checks,
   package build, and make one atomic commit.

## Deferred, separately atomic follow-up

Pulsar `_receive()` currently interpolates a queue name and driver exception
into a public `QueueError`.  It is an operational-error contract, so it will
be fixed in the next redaction slice rather than mixing queue operation
semantics with this startup/configuration commit.
