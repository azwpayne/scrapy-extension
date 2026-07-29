# Round 48 — SPEC / PLAN / TASK: terminal exception-context completion

## Context and new audit evidence

Round 47 eliminated the first source-wide cohort of caught-and-continued
diagnostic leaks.  Its independent post-change audit found a remaining,
well-bounded cohort where a raw error can still be observed by a custom logger
or stats handler through `sys.exc_info()` before code returns, continues, or
rebuilds a sanitized terminal error.

Confirmed cases are grouped by implementation boundary:

1. **Queue and pipeline:** serialization failure paths invoke a stats/logging
   helper while the original conversion or acknowledgement error is active.
2. **Backend suppressors:** Memcached, SQS, and Pulsar suppress an ordinary
   cleanup error from a context manager/utility while emitting its diagnostic
   from the active `__exit__`/exception suite.  Pulsar timeout and SQS poison
   cleanup have equivalent continuation paths.
3. **Connection and candidate cleanup:** a losing ConnectionManager candidate
   and DynamoDB, ElasticSearch, MongoDB, and Pulsar failed-connect cleanup
   paths log during an outer error handler before publishing their fixed public
   error.

The review also classified a small set of true raw-preserving primary-rethrow
paths.  They remain outside this round unless implementation evidence shows a
returned/continued or sanitized-terminal branch.

## Specification

- Before any logger, warning hook, stats collector, monitor, or cleanup
  diagnostic is invoked, the caught ordinary exception that prompted it must
  have completely unwound from every calling frame.
- The rule applies equally to graceful continuation and to rebuilding a fixed,
  sanitized terminal package error; an extension must never observe the raw
  driver/parser error while the package is preparing that boundary.
- Suppressor/context-manager helpers must report only a boolean/static outcome
  to an outer caller.  The caller emits fixed diagnostic text only after the
  context manager has returned and `sys.exc_info()` is clear.
- Cleanup and lifecycle state semantics remain unchanged: all detached
  candidates are still released, control-flow `BaseException` behavior is
  preserved, and fixed public error boundaries retain their types/messages.

## Plan and independently verifiable tasks

- [ ] **R48-1 — Queue/pipeline terminal handoff:** defer invalid replacement
      acknowledgement, malformed-payload cleanup, and serialization stats
      until the raw parser/conversion exception has left its suite.
- [ ] **R48-2 — Suppressor backend cohort:** refactor Memcached, SQS, and
      Pulsar suppressors plus Pulsar timeout/SQS poison cleanup to return
      status flags; emit only fixed diagnostics after unwind.
- [ ] **R48-3 — Connection/candidate cleanup cohort:** defer losing-candidate,
      failed-connect, and candidate-close diagnostics in ConnectionManager,
      DynamoDB, ElasticSearch, MongoDB, and Pulsar until the outer handler is
      complete.
- [ ] **R48-4 — Dynamic regressions:** attach real logging handlers and custom
      stats probes per family.  Assert `sys.exc_info() == (None, None, None)`
      at delivery time and no marker in message/args/`exc_info`/`exc_text`.
- [ ] **R48-5 — Verify and re-audit:** run focused suites, static source audit,
      lint/type/security checks, artifact checks, and the full Python 3.10–3.14
      non-integration matrix.  Begin a further numbered round for any P0/P1.

## Acceptance criteria

1. Every confirmed Round 48 path invokes external diagnostics only after its
   raw caught exception has fully unwound.
2. Queue and pipeline preserve their existing public fixed error contract and
   durable-ack/poison semantics.
3. Backend suppressors retain cleanup, continuation, and process-control
   behavior while no longer logging from active exception context.
4. Custom handlers and stats probes observe no active exception and cannot
   recover sentinel data from any log record field.
5. Independent static and dynamic post-change audits find no remaining P0/P1
   path in the inspected terminal-context boundary.
