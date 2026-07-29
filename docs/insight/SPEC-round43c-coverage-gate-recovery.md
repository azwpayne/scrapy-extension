# Round 43C — SPEC / PLAN / TASK: CI coverage gate recovery

**Base:** post-Round 42 full test run.

## Audit conclusion

All non-integration tests pass, but the new endpoint/operation/backpressure
branches lower aggregate coverage to 94.82%, below CI's enforced 95.00% gate.
This is a release blocker, not an informational metric.

## Specification

1. Restore total coverage to at least 95.00% using behavior-focused tests,
   not excludes or a reduced threshold.
2. Cover newly introduced fallback/error paths: custom backend-error
   reconstruction, safe QueueError message preservation, proxy weak-reference
   fallback, monitor-on-store failure, and new lazy-owner event ordering.
3. Every added test asserts an externally meaningful invariant in addition to
   executing the branch.

## Plan and task checklist

- [ ] Add focused branch regressions in the owning test modules.
- [ ] Run coverage-gated non-integration suite.
- [ ] Keep coverage work in the relevant atomic behavior commits where
      possible; use a standalone test-only commit only for untouched paths.
