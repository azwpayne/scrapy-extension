# R67 PLAN

Confirm partial signal registration is held only in local state and a control
exception can abort rollback, publish per-handler ownership before each
registration attempt, retain failed rollback entries for terminal cleanup, add
a deterministic retry regression, verify, and atomically commit.
