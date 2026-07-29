# Round 40B — SPEC / PLAN / TASK: startup error redaction boundary

**Base:** `main` after the Round 40A Kafka candidate-publication repair.
**Scope:** remove raw driver and plugin diagnostics from public connection
errors, their exception graphs, and ordinary lifecycle logs.

## Audit conclusion

Several startup paths interpolated a caught driver exception into a public
`BackendConnectionError`, used `raise ... from error`, or logged it through
`%s`/`exc_info=True`. Other paths used `from None` inside an `except` block;
that suppresses default traceback rendering but still retains the raw exception
in `__context__`. A driver error can include a URI, endpoint, or credential.

Affected public startup boundaries were ConnectionManager plus MongoDB, Kafka,
Elasticsearch, SQS, DynamoDB, RocketMQ, RabbitMQ, Memcached, and Pulsar.

## Specification

1. A public connection failure exposes only a static, backend-specific error
   message and trusted metadata such as a validated mode or retry count.
2. The public error is raised after its `except` suite has finished, so both
   `__cause__` and `__context__` are `None`.
3. Configuration errors and process-control exceptions retain their existing
   control flow. Candidate cleanup and publication ordering do not change.
4. Retry/stale/released/disconnect/monitor diagnostics never pass an arbitrary
   exception object or traceback to logging handlers.
5. Driver cleanup and best-effort expiry cleanup diagnostics are static as
   well; closing one failed candidate must not disclose its error payload.
6. Runtime operation errors remain outside this narrowly scoped startup
   contract and keep their existing documented causality semantics.

## Plan and tasks

1. Replace inline chained startup errors with a local static
   `startup_error`, then raise it after the handler.
2. Replace ConnectionManager's stored `last_exception` with a failure flag and
   make exhausted retries raise a static error after the loop.
3. Remove raw exception values and `exc_info=True` from lifecycle cleanup
   diagnostics.
4. Add secret-marker regressions across direct startup paths and the manager;
   assert message, `__dict__`, formatted traceback, `__cause__`,
   `__context__`, logging records, retry behavior, and unpublished cleanup.
5. Run focused backend suites, static checks, security checks, full
   non-integration tests, packaging verification, and CI before the atomic
   commit.

## Acceptance evidence

- A synthetic driver message containing a secret marker is absent from the
  public error, exception graph, formatted traceback, and ordinary logs.
- Failed candidates still close and remain unpublished.
- Retry counts, cancellation behavior, configuration failures, and
  `KeyboardInterrupt`/`SystemExit` behavior remain unchanged.
