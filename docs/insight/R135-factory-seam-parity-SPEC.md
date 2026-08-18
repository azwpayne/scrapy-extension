# R135 SPEC — factory-seam parity (pipeline ownership flag, mixin breaker policy, mixin snapshot pairing)

> Plan: [R135-factory-seam-parity-PLAN.md](R135-factory-seam-parity-PLAN.md)
> Tasks: [R135-factory-seam-parity-TASK.md](R135-factory-seam-parity-TASK.md)

## Context

R135 dedicated deep scan: connectors.py in full (4 dims EMPTY — the file
itself is clean) + a factory-seams dimension over the three component
factories and the spider mixin. 4 raw findings, 3 confirmed by adversarial
verification (citation-confirmed), 1 refuted (manager_retry_* nested alias —
unreachable-from-Scrapy observation was mechanically right but the alias is
documented as backend-settings-dict usage, not Scrapy-config usage). All
three confirmed findings are parity gaps at the same seam family; all
hand-verified at HEAD `14cc5c3`.

## Finding A (MEDIUM) — BackendPipeline lacks `owns_connection_manager`

`BackendScheduler` (scheduler.py:902/1869) and `BackendDupeFilter`
(dupefilter.py:183/797) both support the composite ownership contract: one
shared `get_manager()` acquire lent to several components, each constructed
with `owns_connection_manager=False` so only the composite owner releases.
`BackendPipeline.__init__` (pipeline.py:149-159) has no such kwarg and
`_close_locked` (pipeline.py:546-572) releases unconditionally: a composite
owner who lends the single acquire to a pipeline gets the shared backend
torn down (`_users` → 0 → retired + disconnected + registry-evicted) while
scheduler/dupefilter still hold it. Default factory paths self-acquire and
are unaffected; the defect is scoped to direct-construction composite wiring
that the siblings' public contract invites.

**Fix**: add keyword-only `owns_connection_manager: bool = True` to
`BackendPipeline.__init__`; in `_close_locked`, keep the `_manager_released`
idempotency latch but only call `connection_manager.close()` when owning
(mirror the dupefilter gate shape and docstring wording). Factories keep the
default (True).

## Finding B (LOW) — spider_mixin bypasses the Scrapy breaker-policy resolution

The three component factories acquire managers through
`resolve_backend_config`, whose `_merge_connection_manager_settings` folds
Scrapy-level `SCRAPY_CIRCUIT_BREAKER_{ENABLED,FAILURE_THRESHOLD,RESET_TIMEOUT}`
into manager settings (connectors.py:773, `_resolve_circuit_breaker_policy`
at :794). `BackendSpiderMixin.setup_backend` builds settings from
`backend_settings` + shortcut attrs only and calls `ConnectionManager.get_manager`
directly (spider_mixin.py:163-181, :317-333), so a breaker configured in
Scrapy settings never reaches the mixin's manager — `_get_breaker` falls
back to the env-only `Settings()` and the mixin path silently runs
unwrapped. `docs/codebase-deep-insight.md:15` lists this exact gap as an
open issue; the R14-D comment in setup_backend records parity as the
intended direction.

**Fix**: promote the policy resolver to a public
`resolve_circuit_breaker_policy(settings)` in connectors (keep the private
name as an alias for internal callers), and merge its result into the
mixin's manager settings whenever crawler settings are available. Behavior
unchanged when no source is set (resolver returns `{}`).

## Finding C (MEDIUM) — mixin get_queue never pairs a snapshot ConnectionManager

`BackendScheduler.from_settings` (scheduler.py:1157-1188) detects a stateful
strategy (delay/round_robin/time_wheel/ring_buffer) on a queue-only backend
and separately acquires the configured storage component as a
`snapshot_connection_manager` so `BackendQueue` can persist/restore
in-process state. The mixin's `get_queue` (spider_mixin.py:590-597) builds
those same strategies from `SCRAPY_QUEUE_STRATEGY` but never passes a
snapshot manager — with a queue-only backend, items held in the delay heap /
pending set / ring buffer are lost on every shutdown, even when
`SCRAPY_STORAGE_BACKEND_TYPE` is explicitly configured, and queue.py's skip
diagnostic recommends exactly the setting this path ignores.

**Fix**: in the mixin's queue construction, when the built strategy is one
of the four stateful types and the queue backend lacks the storage
capability, resolve the storage component exactly as the scheduler does
(explicit override fail-fast; no-explicit-override ConfigurationError →
best-effort skip), acquire a snapshot manager, pass it to `BackendQueue`,
and release it in the mixin's teardown ordering after the queue closes.
`get_scheduler` delegates to the scheduler factory path and needs no change
if it already receives snapshot pairing there — verify and document in the
test.

## Non-goals

- No change to default factory behavior (all three fixes are additive
  opt-in / parity paths; defaults byte-identical).
- The refuted manager_retry_* alias finding is closed, not fixed.

## Acceptance

- RED tests per finding on current code; GREEN after fixes; full gate green
  (`ruff check`, `ruff format --check src tests conftest.py`,
  `uv run --frozen pytest`, `mypy --strict src`).
- Three atomic commits; LEDGER rows; memory round entry.
