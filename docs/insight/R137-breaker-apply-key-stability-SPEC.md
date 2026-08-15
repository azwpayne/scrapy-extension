# R137 SPEC — breaker-apply key stability + mixin queue-monitor knobs

> Plan: [R137-breaker-apply-key-stability-PLAN.md](R137-breaker-apply-key-stability-PLAN.md)
> Tasks: [R137-breaker-apply-key-stability-TASK.md](R137-breaker-apply-key-stability-TASK.md)

## Context

R137 scanned the R136-landed diff (`11115d7..864de74`) plus
shared-registry / monitor-propagation seams (5 dims, 12 agents). **6
confirmed, 1 refuted.** Five of the six are defects in or adjacent to
R136-F1's own `apply_scrapy_breaker_policy` — the self-review-first rotation
paid out again.

## Findings

### F1 (HIGH, empirically reproduced by the verifier)

`apply_scrapy_breaker_policy` is the only code that mutates a manager's
`settings` dict after construction. `close()` (connectors.py:2286) recomputes
the registry key from `self.settings` at release time and evicts only
`if cls._managers.get(key) is self` (:2302) — after the policy fold the
recomputed key differs from the registration key, so the entry is never
evicted even on last-holder release. The stale entry (`_retired=True`,
disconnected) is returned by the next `get_manager` with the original
settings; every backend op on it raises `BackendConnectionError("Cannot
access a released ConnectionManager")` for the life of the process. Trigger:
documented early-setup pattern + any breaker source + more than one crawl per
process. Breaks the key-hash immutability invariant that R135-B's own comment
states ("the fold must precede get_manager so the registry key hashes the
policy").

### F2 (MED)

The latch cannot distinguish "resolved from an explicit policy" from "cached
the disabled env fallback before the policy arrived": any backend op in
`__init__` (seed push, queue clear, config read) drives `_get_breaker` →
caches env fallback (`_breaker_configured=True`, `_breaker=None`) → the later
`apply` no-ops silently. Byte-for-byte the R136-F1 symptom ("a
Scrapy-configured breaker silently never engages") surviving through
used-early, not just acquired-early.

### F3 (MED)

Two early-setup spiders on one shared backend (only kafka/rocketmq get
per-spider scope keys) share one policy-less-key manager; both applies pass
the latch → second `settings.update` silently overwrites the first spider's
policy (or is dropped if traffic resolved the breaker first). The factory
path isolates differing policies via the key hash; the early path cannot
(retroactively), so the divergence must at least be OBSERVABLE.

### F4 (MED)

Mixin `get_queue`-direct builds `BackendQueue` with no `monitor=` /
`pop_rate_window_s=` — the runbook knobs `SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD`
/ `SCRAPY_MONITOR_POP_RATE_WINDOW_S` are only read by
`BackendScheduler.from_settings` (scheduler.py:331-341, threaded at open
:1514-1529). The R14-C comment documents exactly this gap as fixed for the
scheduler path; the mixin direct path still has it.

### F5 (MED)

`get_queue()` in the early-setup window (no crawler) bakes `NullMonitor` into
the cached `BackendQueue` forever — no re-resolution on later calls, so
queue-level hooks (on_push/on_pop/on_queue_depth + strategy gauges) stay dead
for the whole crawl.

### F6 (DEFERRED — observability-only)

`set_monitor` last-writer-wins on registry-shared managers: in a multi-spider
`CrawlerProcess` on a shared backend, lifecycle counts attribute to the last
writer's crawler only. Matches the multi-spider overwrite semantics the R55
SPEC already accepted for the queue manager; a real fix needs monitor fan-out
(a design change). Recorded as DEFERRED in the LEDGER, not fixed this round.

### Refuted

`_get_breaker` unlocked settings read racing `apply` — no realistic execution
order (single-threaded Scrapy flows serialize the two paths per spider).

## Fix design

**Fix A (F1+F2+F3, connectors.py)** — `apply_scrapy_breaker_policy` v2:

- NEVER mutates `self.settings` (registry key stays stable → F1 root-caused
  away).
- Parses the policy and installs the breaker directly under `_lock`
  (mirroring `_get_breaker`'s construction), setting `_breaker_configured`.
- New `_breaker_resolved_from_env_fallback` flag, set True only by
  `_get_breaker`'s env branch: an explicit Scrapy policy OVERRIDES a
  fallback-cached resolution (F2), but never an explicitly-resolved one.
- Differing explicit policy dropped on an already-explicitly-resolved shared
  manager → one-shot static `logger.warning` (F3 observable;
  first-resolution-wins, documented: one shared manager = one breaker).

**Fix B (F4+F5, queue.py + spider_mixin.py)**:

- `BackendQueue.set_monitor(monitor)`: replaces `self._monitor` and forwards
  to the strategy per the R21-B convention (delay.py already has the hook).
- Mixin `get_queue`: resolve a knobs-aware monitor at construction (reuse
  `BackendScheduler._resolve_monitor_for_spider` + the same parse helpers and
  defaults) and pass `monitor=` / `pop_rate_window_s=`; on cached re-entry,
  upgrade ONLY a `NullMonitor` (never overwrite a real monitor — protects
  any externally-tuned wiring).

## Acceptance

RED tests for F1-F5 on the current tree; GREEN after; full gate green;
atomic commits (Fix A, Fix B, docs); LEDGER rows (5 LANDED + 1 DEFERRED +
1 REFUTED); memory round entry.
