# Round 37 — SPEC / PLAN / TASK: total queue-routing validation

**Base:** `main` @ `78cfa53`. **Scope:** final independent audit of R36's
routing-value boundary.

## Specification

An omitted delay defaults to `0.0`; an explicitly supplied delay must be a
finite, non-boolean `int` or `float` and must be non-negative. Falsy values
such as `None`, `""`, `[]`, and `{}` are inputs, not absence, and must fail
through the same `QueueError`/replacement-token termination path.

Error construction itself must be total. It must not call unbounded or
user-controlled `repr()` while reporting invalid routing values: a huge integer
can cause CPython's digit guard to raise a second `ValueError`, otherwise
bypassing queue error translation and inherited-delivery settlement.

## Plan and tasks

1. Add RED tests for explicit falsy delay values and `10**5000` delay/priority
   replacement deliveries.
2. Make queue ingress distinguish absence from supplied false-y input and use
   safe type-only diagnostics for invalid values.
3. Run focused and full non-integration regression, Ruff, and strict mypy;
   create one atomic commit and one final change-boundary audit.
