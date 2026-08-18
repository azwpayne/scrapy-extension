# R136 SPEC — early-setup breaker rescue + snapshot monitor parity

> Tasks: [R136-early-setup-breaker-rescue-TASK.md](R136-early-setup-breaker-rescue-TASK.md) — two fixes shipped inline (round is small; no separate PLAN doc needed).

## Context

R136 scan (5 dims over the R135-landed diff + two explicit hypotheses): both
hypotheses CONFIRMED (2-3 independent finders each, adversarial verify
airtight, 0 refuted). Both defects were introduced by R135 itself.

## Finding A (HIGH-ish MED) — R135-B breaker fold misses the documented early-setup path

The fold sits inside `if manager is None:` (spider_mixin.py:179-181). The
class docstring's own example calls `setup_backend()` in `__init__` — before
Scrapy attaches the crawler — so the fold is skipped, the idempotent
`from_crawler` re-run skips the whole acquisition block, and
`_get_breaker`'s env-only fallback is cached forever. A breaker configured in
Scrapy settings silently never engages for every backend the mixin hands out.
The repo's R14-D monitor wiring was deliberately hoisted outside this guard
for exactly this reason (spider_mixin.py:204-212).

**Fix**: (1) keep the acquisition-time fold (registry-key parity with the
factory path); (2) add a small public `ConnectionManager.
apply_scrapy_breaker_policy(settings)` — resolves the policy (no-op when no
source) and merges the internal keys into `manager.settings` under `_lock`,
only while the breaker is unresolved (`_breaker_configured` False); (3) the
mixin calls it on EVERY `setup_backend` invocation (mirroring the
`set_monitor` placement), rescuing early-acquired managers.

## Finding B (MED/LOW) — R135-C snapshot manager never receives a monitor

`_resolve_snapshot_connection_manager` (spider_mixin.py:608-611) returns the
storage manager bare; the mixin wires only the primary manager
(spider_mixin.py:212). The snapshot backend's `backend/{connect,disconnect,
retry}_count` stats are dead on the get_queue-direct path — the exact gap
scheduler.py:1507-1513 fixed in R55.

**Fix**: `snapshot_manager.set_monitor(BackendQueue._resolve_monitor(self))`
at acquisition, mirroring the scheduler pairing.

## Acceptance

RED tests for both on current code; GREEN after; full gate green; atomic
commits; LEDGER rows; memory round entry.
