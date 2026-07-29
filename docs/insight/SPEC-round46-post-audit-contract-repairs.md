# Round 46 — SPEC / PLAN / TASK: post-audit contract repairs

## Context

Round 45 closed the terminal telemetry and release-artifact boundaries.  Its
post-implementation audit and a clean-room Python-version matrix found that
the intended contracts are not yet total:

1. `RedisBackend.ping()` / `is_connected()` turn every `Exception` into
   `False`.  That includes the package's `BackendConnectionError` raised when
   a health callback attempts a re-entrant disconnect while holding a
   generation lease.  The lifecycle guard is therefore silently bypassed.
2. The `[all]` release-artifact smoke test checks `find_spec("rocketmq")`,
   rather than importing the installed RocketMQ SDK.  The root export is lazy,
   so that check cannot observe an import-time SDK failure.
3. A continuation path must never retain raw operational exceptions, dynamic
   exception class names, or persisted snapshot payloads in `LogRecord`
   fields.  The fresh audit has identified candidates in the pipeline,
   dupefilter, and queue-state recovery paths; each candidate will be either
   fixed with a regression test or explicitly shown to re-raise without
   continuation.

## Specification

### Redis health and lifecycle contract

- `ping()` and `is_connected()` return a boolean for ordinary driver/runtime
  failures, including `RedisError`, `RuntimeError`, and `ValueError`.
- They must re-raise `BackendConnectionError` that represents a package
  lifecycle/lease invariant, including a re-entrant disconnect.
- `BaseException` control flow remains unmodified.
- No connection, generation, or active-lease state may change merely because
  the probe reports an ordinary failure.

### Release artifact optional-dependency contract

- Both the wheel and sdist installed with `[all]` must execute a real import
  of every optional SDK and resolve every public optional root export.
- RocketMQ's import-time log directory side effect must be contained inside
  the isolated package-smoke virtual environment by a process-local
  `os.path.expanduser` redirection.  The workflow must not mutate `HOME` or
  any user-owned path.
- The smoke test remains isolated (`-I`) and proves that
  `scrapy_extension` was imported from the newly installed artifact rather
  than the source checkout.

### Terminal diagnostic contract

- Any path that catches an operational failure and then returns, continues,
  opens a component, or otherwise degrades gracefully must emit only a fixed,
  non-sensitive diagnostic message.  Its `LogRecord.args`, `exc_info`, and
  `exc_text` must not retain the original exception graph or snapshot data.
- Primary failure, rollback, and `BaseException` paths that immediately
  re-raise remain outside this continuation rule; the audit must document
  that classification.

## Plan and independently verifiable tasks

- [ ] **R46-1 — Redis guard:** reproduce the generation re-entry failure,
      re-raise `BackendConnectionError` before the boolean fallback, and test
      the lifecycle guard alongside ordinary driver failures.
- [ ] **R46-2 — Artifact SDK import:** replace RocketMQ's spec lookup with a
      contained real import in the wheel/sdist smoke block; pin that behavior
      with the CI coverage regression test and execute it against both local
      artifacts.
- [ ] **R46-3 — Continuation diagnostics:** resolve every confirmed
      pipeline/dupefilter/queue-recovery continuation candidate with static
      diagnostics and marker-redaction regression tests.  Do not expand
      primary re-throw boundaries without evidence of a returned/continued
      path.
- [ ] **R46-4 — Verify:** run focused red/green tests, lint, strict typing,
      security/dependency checks, artifact smoke, and the full non-integration
      matrix for Python 3.10–3.14.
- [ ] **R46-5 — Re-audit:** fan out independent reviewers over the changed
      health, diagnostics, and packaging boundaries.  Start a new numbered
      round if any P0/P1 gap remains; otherwise mark this round complete.

## Acceptance criteria

1. A re-entrant Redis disconnect invoked from a health probe raises the
   established `BackendConnectionError`; ordinary unexpected driver failures
   still return `False`.
2. The artifact smoke test imports `rocketmq` successfully from both a wheel
   and an sdist `[all]` installation without touching user-owned paths.
3. Every fixed continuation regression test proves that a sentinel marker is
   absent from the rendered message, `args`, `exc_info`, and `exc_text`.
4. The lock is valid; source quality gates and the complete supported Python
   test matrix pass.
5. A final independent post-change audit finds no unresolved P0/P1 issue in
   the changed contract boundaries.
