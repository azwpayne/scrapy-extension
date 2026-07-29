# Round 43A — SPEC / PLAN / TASK: lazy-connect monitor reentrancy

**Base:** `main` after Round 42 verification.

## Audit conclusion

When `ConnectionManager.backend` owns a first lazy connection attempt, it does
not publish the attempt result or signal peer waiters until `connect()` returns.
`connect()` dispatches retry monitor events before returning.  A monitor whose
`on_retry()` reads `manager.backend` or `get_*_backend()` then waits for the
owner's event while the owner waits for the monitor: a deterministic deadlock.

## Specification

1. A lazy-connect owner must resolve its attempt, clear `_connecting`, and
   signal all waiters before any retry/disconnect/connect monitor callback can
   re-enter a manager accessor.
2. This ordering applies on terminal failure and retry-then-success paths; an
   exception from diagnostics must never prevent the owner-state cleanup.
3. Direct `connect()` keeps its established monitor ordering and continues to
   call monitoring outside the connection lock.
4. Reentrant monitor access must finish within the normal bounded connection
   operation, return the already-published backend on success, and surface the
   same typed `BackendConnectionError` to owner/peers on failure.

## Plan and task checklist

- [ ] Separate lazy attempt publication/signaling from monitor dispatch.
- [ ] Add terminal-failure and retry-then-success reentrant-monitor tests.
- [ ] Preserve direct-connect behavior and cover the new branches.
- [ ] Run focused/full CI checks and make one atomic commit.
