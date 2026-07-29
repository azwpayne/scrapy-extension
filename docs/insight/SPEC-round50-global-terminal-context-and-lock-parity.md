# Round 50 — SPEC / PLAN / TASK: global terminal-context closure

## Context and audit evidence

The independent Round 49 source audit found four remaining, reproducible
contracts that were outside the Round 48 cohort:

1. `ConnectionManager.connect()` dispatches buffered monitor events from a
   `finally` while a non-lazy `BaseException` is active.
2. Dupefilter observer-fence cleanup emits a raw secondary exception through
   `LogRecord.exc_info` while preserving a primary signal, and interrupted
   decision compensation logs while suppressing its own cleanup failure.
3. Pipeline shutdown logs a manager-close secondary exception from inside its
   handler while a storage-close primary is being preserved.
4. The Round 49 Elasticsearch fixture test compares two hard-coded image
   strings but does not derive the expected version from `uv.lock`; a future
   lock-only client upgrade could silently create a version mismatch.

Dynamic custom handler probes reproduced every P0: handlers observed the raw
exception in `sys.exc_info()` and, for the fence path, in `LogRecord.exc_info`.

## Specification

- External monitor, logging, stats, warning, and callback code must run only
  after each caught ordinary or control-flow exception has unwound from every
  relevant caller frame.
- A true raw-primary rethrow may remain diagnostic-free; a preserved primary
  must never be exposed merely to retain secondary cleanup telemetry.
- Fixed continuation diagnostics may communicate only static context and must
  omit raw `exc_info`, exception objects, and traceback text.
- CI and local Elasticsearch fixture images must be derived from and equal to
  the exact locked Python client version, not a duplicated literal.

## Plan and independently verifiable tasks

- [ ] **R50-1 — Connection monitor dispatch:** capture non-lazy terminal
      `BaseException` values, leave their handlers, then dispatch monitor
      events while preserving existing propagation and callback precedence.
- [ ] **R50-2 — Dupefilter cleanup:** defer observer-fence diagnostics as a
      static outcome and make interrupted-decision compensation truly silent.
- [ ] **R50-3 — Pipeline shutdown:** retain strategy/manager teardown order
      and primary-error precedence while deferring the manager-secondary
      diagnostic past its exception handler.
- [ ] **R50-4 — Locked Elasticsearch parity:** parse `uv.lock` in the CI
      regression test and assert its exact client version drives both images.
- [ ] **R50-5 — Verify and re-audit:** run dynamic handler probes, focused
      lifecycle suites, global static audit, CI-equivalent quality gates, and
      exact-SHA GitHub Actions.

## Acceptance criteria

1. All reproduced handlers observe `(None, None, None)` and no record field
   carries the sentinel/raw exception.
2. Connection, dupefilter, and pipeline lifecycle behavior preserves primary
   error type, state cleanup, ordering, and control-flow semantics.
3. The compensation path emits no diagnostics while an outer primary can be
   active.
4. The CI regression test fails if `uv.lock`, workflow, or Compose select
   different Elasticsearch versions.
5. A fresh independent audit finds no P0/P1 in the inspected terminal-context
   and CI fixture contracts.
