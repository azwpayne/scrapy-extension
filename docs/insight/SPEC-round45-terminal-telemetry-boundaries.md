# Round 45 — SPEC / PLAN / TASK: terminal telemetry and serialization boundaries

**Base:** `main` after Round 44 direct-operation verification.

## Audit conclusion

The Round 44 direct-operation boundaries close the public driver-call surface,
but three independent observer paths can still see an unsanitized error before
the public boundary replaces it.  In addition, the documented
`SerializationError.data` surface keeps complete requests, payload bytes, and
exception chains on public Queue/Pipeline failures.  The same review found a
small boolean-health contract gap and a package-artifact CI coverage gap.

The actionable findings are deliberately split into narrow commits:

1. **P0:** direct `ConnectionManager.connect()` dispatches retry monitor
   callbacks while its original terminal error is active, exposing the raw
   traceback and manager settings through `sys.exc_info()`.
2. **P1:** public queue/pipeline serialization failures retain input data,
   dynamic exception text, cause/context, and implementation frames; raw forms
   of those failures are also passed to Monitor implementations.
3. **P1:** background batched storage and fail-open dupefilter paths forward
   original backend error graphs to custom monitor listeners.
4. **P1:** selected diagnostics intentionally swallow expected operational
   errors but record `exc_info` or raw error text, providing a log/structured
   handler disclosure path.
5. **P2:** four bundled health probes let an unexpected ordinary exception
   escape a documented boolean API.
6. **P2:** artifact smoke coverage installs only the base distribution and
   does not exercise the supported `[all]` extras/lazy backend exports.

## Specification

### 1. Direct connection lifecycle dispatch

For the direct `ConnectionManager.connect()` path, capture a terminal ordinary
failure, finish the implementation stack, dispatch the already-buffered
monitor events with no active exception, then re-raise so the existing outer
terminal boundary rebuilds the public error.  Successful calls and retry event
ordering remain byte-for-byte compatible.  `BaseException` control flow is
never captured or converted.

### 2. Serialization terminal contract and migration

Queue `push`, queue `_push_with_durability`, queue `pop`, and pipeline
`process_item` are public terminal boundaries.  On an exact
`SerializationError`, each must raise a newly constructed
`SerializationError` after implementation frames unwind:

- fixed operation-specific text;
- `data is None`;
- `serializer == "json"`;
- no cause, context, original traceback graph, input object, request bytes, or
  dynamic serializer/driver text.

Unknown `SerializationError` subclasses and every `BaseException` preserve
their existing behavior.  Queue input validation, acknowledgement/poison-drop
semantics, storage admission/backpressure behavior, and private helper
debugging contracts are not changed.

This intentionally supersedes the stable `SerializationError.data` diagnostic
context contract for these public terminal paths.  README, stability policy,
and changelog/migration documentation must state the security change.  Private
helpers such as `_decode_body()` may retain their local debugging behavior when
called directly.

### 3. Monitor event contract

Any error passed to a Monitor is an extension-facing public object.  For
serialization, background batched-store failure, and fail-open dupefilter
events it must be a fresh static package error with no raw error graph.  The
event's operation/type remains useful for metrics (`"push"`, `"pop"`,
`"store"`, or `"dedup"`), but listener code cannot recover a payload, key,
settings, cause/context, or frame local.  Local requeue/retry/fail-open logic
continues to use the original error privately.

### 4. Best-effort diagnostics

When the program deliberately swallows a monitor/stats failure or expected
queue/serialization operational failure and continues, its diagnostic record
must use fixed text and no `exc_info`, exception text, or error object.
Logger-handler interruptions remain isolated exactly as before.  This task is
limited to the identified observer/statistics and scheduler continuation paths;
it does not rewrite unrelated cleanup diagnostics whose failure is surfaced.

### 5. Bundled health probes

Kafka, MongoDB, Redis, and Elasticsearch `ping()` / I/O-backed
`is_connected()` methods return `False` for any ordinary `Exception`.  They
continue to propagate `KeyboardInterrupt`, `SystemExit`, and other
`BaseException` control flow.  This rule applies only to bundled implementations
and does not alter plugin proxy or `ConnectionManager` error policy.

### 6. Artifact extras smoke

For each built wheel and sdist, CI must additionally install `[all]` into an
isolated venv and import all supported optional backend packages/root exports.
The base smoke remains in place.  The regression test pins the workflow intent
without depending on networked package installation during the source suite.

## Plan and task checklist

- [ ] `docs: specify Round45 terminal telemetry boundaries` (this document).
- [ ] `fix(connectors): dispatch direct lifecycle monitors outside raw failure`.
- [ ] `fix(serialization): redact public queue and pipeline failures`.
- [ ] `fix(monitors): redact background storage and dupefilter error events`.
- [ ] `fix(diagnostics): remove raw expected-failure telemetry`.
- [ ] `fix(health): make bundled health probes total boolean operations`.
- [ ] `ci(packaging): smoke-test all extras from wheel and sdist`.
- [ ] `docs: document serialization-error privacy migration`.
- [ ] Repeat a full independent audit and run the exact local/remote CI matrix
  against the final pushed SHA.

## Acceptance criteria

- A synthetic marker injected into an input, backend error, callback, stats
  collector, or Monitor cannot be recovered from public terminal exceptions,
  Monitor event graphs, formatted diagnostics, `LogRecord.exc_info`, or
  package-frame locals after the affected operation returns.
- Existing retry/event order, request acknowledgement, failure counters,
  fail-open dedup behavior, and `BaseException` behavior remain unchanged.
- Health APIs return a bool for all ordinary failures.
- Built wheel and sdist both pass base and `[all]` isolated install/import
  smoke checks.
- Focused tests, full non-integration tests, static/security checks, build
  smoke, and all GitHub Actions jobs pass for the exact final commit.
