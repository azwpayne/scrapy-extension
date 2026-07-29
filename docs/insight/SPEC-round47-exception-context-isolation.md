# Round 47 — SPEC / PLAN / TASK: exception-context isolation

## Context and audit result

Round 46's continuation-diagnostic audit found a stronger leak than a
formatted `LogRecord` field.  Python keeps the caught exception active for
the complete `except` suite.  A custom logging handler invoked from that
suite can call `sys.exc_info()` and recover the raw exception and traceback,
even if the log call has fixed text, no arguments, and `exc_info=False`.

This is a P0 violation of the terminal telemetry boundary whenever a path
catches an operational failure and then returns, continues, falls back, or
otherwise keeps the crawler running.  Initial evidence covers pipeline and
dupefilter degradation plus `BackendQueue` and all snapshot-capable queue
strategy recovery paths.  Immediate primary-error rethrows and
`BaseException` propagation are intentionally not part of this change.

## Specification

### Exception-context isolation contract

- A continuation diagnostic must be emitted only after control has left the
  `except Exception` suite that caught the operational failure.
- Therefore, during a logger handler's `emit()`, `sys.exc_info()` must be
  `(None, None, None)` for every fixed continuation diagnostic.
- The resulting `LogRecord` must also have no raw exception in `args`,
  `exc_info`, or `exc_text`; messages must not contain operational identifiers
  or serialized snapshot data.
- Existing recovery semantics (return values, restored state, ordering,
  retry/fail-open behavior, and safe numeric observability) must remain
  unchanged.
- Do not move logging out of a primary-failure/rollback path that immediately
  re-raises unless it also has a true continuation branch.

## Plan and independently verifiable tasks

- [ ] **R47-1 — Inventory:** make a source-wide, evidence-backed inventory
      of log/diagnostic calls lexically inside caught-exception suites and
      classify each as continuation or immediate primary failure.
- [ ] **R47-2 — Pipeline and dupefilter:** move confirmed fallback
      diagnostics out of active exception contexts, use fixed messages, and
      protect them with sentinel-marker plus custom-handler regressions.
- [ ] **R47-3 — Queue and restore paths:** apply the same control-flow
      pattern to `BackendQueue` and every snapshot strategy continuation;
      retain recovery behavior and safe counters without logging queue names,
      snapshot fields, or caught exceptions.
- [ ] **R47-4 — Regression harness:** add a focused handler which records
      `sys.exc_info()` at `emit()` time.  Cover each corrected family and
      assert no marker survives through rendered messages, `LogRecord`
      fields, or active interpreter exception context.
- [ ] **R47-5 — Verify and re-audit:** run the diagnostic suite, full unit
      matrix, quality/security gates, then independent post-change audits.
      A new numbered round is required for any remaining P0/P1 boundary.

## Acceptance criteria

1. Every confirmed caught-and-continued source path emits diagnostics outside
   its `except` suite.
2. A custom handler observing each representative fallback receives no active
   exception from `sys.exc_info()`.
3. Sentinel markers cannot be recovered from log text, arguments,
   `exc_info`, `exc_text`, or the handler-visible exception context.
4. Snapshot restore and queue/pipeline/dupefilter fallback behavior retains
   its prior safe result and state semantics.
5. Static source audit plus independent review finds no remaining P0/P1
   caught-and-continued diagnostic emission in the changed boundary.
