# Round 36 — SPEC / PLAN / TASK: queue ingress totality and integration commands

**Base:** `main` @ `2749fdc`. **Scope:** findings from the independent
post-R35 audit, limited to the queue routing-value boundary and executable
integration-test instructions.

## Specification

`BackendQueue` accepts a public `priority: float` and a `meta["delay"]` routing
value. Both must be total validation boundaries: only finite, non-boolean
numeric values may reach a strategy or backend, and conversion overflow must
raise the documented `QueueError`. Every invalid value, including a large
integer whose float conversion overflows, must use the established replacement
delivery termination path.

`PriorityQueueStrategy` must enforce the same finite non-boolean numeric
contract for direct callers, before it selects a bucket.

Every current integration-test command must include the three necessary
conditions: `SCRAPY_TEST_INTEGRATION=1`, the applicable endpoint variables,
and `--force-enable-socket`. README and runbook must state the same contract.
Historical audit records remain historical and are out of scope.

## Plan and tasks

1. Add RED tests for overflow delay, malformed priority, direct strategy input,
   and replacement-token settlement.
2. Extend the two ingress guards without normalizing valid integer priority
   semantics; run targeted tests, Ruff, mypy, and the full non-integration suite.
3. Normalize live integration commands across active module docstrings and
   operator docs; review the command inventory with a repository search.
4. Create one atomic commit, then perform a final independent rescan.
