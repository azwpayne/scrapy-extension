# R138 SPEC — worker-queue colon ambiguity + disconnected clear + redis doc drift

> Plan: [R138-worker-queue-colon-ambiguity-PLAN.md](R138-worker-queue-colon-ambiguity-PLAN.md)
> Tasks: [R138-worker-queue-colon-ambiguity-TASK.md](R138-worker-queue-colon-ambiguity-TASK.md)

## Context

R138 rotated off the exhausted surfaces (dupefilter, elasticsearch, R134-clean
infra layer, diagnostic theme, overflow-latch pattern) onto: circuit_breaker
deep scan (first since R34), redis/mongodb full-file scans, storage-only
backends, the breaker↔connectors seam, a self-scan of the landed R137 diff
(`864de74..a23243b`), and the always-on ndiff sweep. 7 opus finders → 6
findings → 6 adversarially CONFIRMED (citations verified; three verifiers ran
mechanical reproductions). 3 of the 6 sit inside the user's in-flight WIP
rewrite of `spider_mixin.py`/`connectors.py` (the primary tree has drifted
structurally at those sites) → DIRTY-BLOCKED per the standing rule; this round
ships the other 3.

## Findings

### F1 (MED, ndiff — SHIPPED): colon-ambiguous work-stealing worker-queue identity

`physical_strategy_queue_name` (`queue/strategies/_names.py:59-85`) returns
`legacy_name = f"{queue_name}:{discriminator}"` verbatim for the five
legacy-colon backends. `KEY_NAME_PATTERN` (`backends/base.py:382`) explicitly
allows colons in both components, so `WorkStealingQueueStrategy(worker_id="a:b")
._own_queue("jobs")` and `(worker_id="b")._own_queue("jobs:a")` both resolve to
the physical queue `jobs:a:b` on redis/mongodb/elasticsearch/pulsar/rabbitmq.
One worker then pops and executes the other logical queue's requests (wrong
spider callbacks/pipelines; `clear()` drains the other's backlog). The project
already fixed this exact delimiter-ambiguity class for snapshot keys (commit
`92f7882`, migration-guide "colon-bearing components add more possible splits")
but left the physical queue-name path ambiguous. Verifier reproduced the
collision by execution and confirmed no downstream guard.

### F2 (LOW, semantics — SHIPPED): disconnected memcached `clear_storage` misclassified as capability-disabled

`memcached.py:558-559` folds `snapshot is None` (never-connected or
disconnected) into the flush-capability branch, so an operator who already set
`allow_flush_all=True` gets `NotImplementedError` advising them to enable the
flag they already enabled. Sibling contract: DynamoDB's disconnected storage
ops raise the stable `StorageError("DynamoDB backend is not connected",
operation=..., key=...)` (`dynamodb.py:494-499`). The
`_clear_storage_capability_error_boundary` rebuilds only exact-type
`NotImplementedError`, so the misclassification survives both error boundaries
verbatim. Verifier reproduced both disconnected paths with a stubbed client.

### F3 (LOW, semantics — SHIPPED): RedisBackend class docstring misdocuments the queue mechanism

`redis.py:328` says the queue uses `ZADD/ZRANGEBYSCORE/ZREM` — the latter two
commands appear nowhere in the implementation (repo-wide grep: the docstring is
the only hit). The actual ops are the atomic Lua scripts `_PUSH_LUA`
(INCR+ZADD+HSET) and `_POP_LUA` (ZPOPMIN+HGET+HDEL), and the method-level pop
docstring explicitly advertises the no-crash-window atomicity the class
summary's command list contradicts. Unenforced by any test.

### F4 (MED, semantics — DIRTY-BLOCKED): early-setup registry-key divergence

Early `setup_backend()` (no crawler yet) skips the acquisition-time breaker
fold (`spider_mixin.py:183-184`, gated on `if crawler is not None`), so the
mixin's manager registers under a key that does not hash the breaker policy
while the factory path (`_merge_connection_manager_settings`,
`connectors.py:773`) does. With any `SCRAPY_CIRCUIT_BREAKER_*` source this
yields two live managers for one backend config and split breaker state —
contradicting the parity comment at `spider_mixin.py:181-182`. R137-F1's
key-stable `apply` (never mutate `self.settings` post-registration) makes
convergence structurally impossible without a redesign. Verifier reproduced
with controls (crawler-attached → 1 manager; no breaker source → 1 manager).
BLOCKED: `spider_mixin.py` is inside the user's in-flight WIP rewrite.

### F5 (MED, semantics — DIRTY-BLOCKED): R137-F5 monitor upgrade drops `pop_rate_window_s`

`spider_mixin.py:763` does `monitor, _ = self._resolve_queue_monitor()` and
`BackendQueue.set_monitor` replaces only the monitor — the queue's
`_pop_rate_window_s` stays frozen at the construction-time 60.0 default, so
`SCRAPY_MONITOR_POP_RATE_WINDOW_S` remains dead on exactly the upgrade path
R137-F5 built (the monitor's own `pop_rate_window_s` attribute has no readers).
BLOCKED: same dirty file.

### F6 (MED, lifecycle — DIRTY-BLOCKED): `apply_scrapy_breaker_policy` reinstalls an identical policy over live breaker state

When `_breaker_resolved_from_env_fallback` is True, apply skips the
equal-values early return that the explicit branch has
(`connectors.py:2719-2729`) and unconditionally calls
`_install_breaker_locked` (:2730-2735) — replacing a live breaker (possibly
OPEN, with failure accounting and HALF_OPEN fencing) with a fresh CLOSED one on
a pure provenance re-label, with no generation change. Verifier reproduced the
silent un-trip by execution and confirmed the explicit-path asymmetry.
BLOCKED: `connectors.py` is inside the user's in-flight WIP rewrite.

## Fix design

**Fix A (F1, `_names.py`)** — `physical_strategy_queue_name` falls back to the
injective `strategy_queue_name` hash when the **discriminator** contains `':'`.

Completeness argument (why discriminator-only is sufficient): if two identities
`(q1,w1)`/`(q2,w2)` with colon-free discriminators produced equal legacy
strings, each string contains exactly one colon, forcing `q1==q2` and
`w1==w2` — the same identity. Any real collision pair therefore has at least
one colon-bearing discriminator; that side now hashes, and a hash name
(`scrapyext-<namespace>-<hex>`, colon-free) can never equal a legacy name
(exactly one colon). Consequences:

- priority is untouched (discriminator is `str(level)`, digits only);
- colon-free work-stealing ids keep the legacy name (backlog compat);
- a colon-bearing queue name with a colon-free worker id ALSO keeps the legacy
  name — its string is unambiguous once the other side of any would-be
  collision hashes away. This is the minimal behavior change.
- Known limitation (pre-existing, unchanged): legacy names carry no namespace,
  so a worker queue `f"{queue}:{worker}"` can equal an unrelated logical queue
  name with colon-free parts. Recorded, not fixed here.

Migration note: deployments running a colon-bearing `SCRAPY_QUEUE_WORKER_ID`
change physical own-queue name on upgrade (the previously-published name was
ambiguous); the #31 sticky-worker-id advisory already covers stranded-backlog
awareness for identity changes.

**Fix B (F2, `memcached.py`)** — split the guard: `snapshot is None` raises
`StorageError("Memcached backend is not connected", operation="clear_storage",
key=None)` (verbatim sibling style of `dynamodb.py:494-499`);
`not snapshot.allow_flush_all` keeps the exact `NotImplementedError`
capability contract. The capability boundary only rebuilds exact-type
`NotImplementedError`, so the new error passes through like every other
storage op's.

**Fix C (F3, `redis.py`)** — one docstring line: replace the
`ZADD/ZRANGEBYSCORE/ZREM` parenthetical with the atomic-Lua truth
(`ZADD` push / `ZPOPMIN` pop via atomic Lua scripts), plus a doc-contract pin
so the drift cannot return silently.

## Acceptance

RED tests for F1/F2/F3 on the current tree; GREEN after; full gate green
(ruff check, ruff format, `uv run --frozen pytest`, `mypy --strict src`);
atomic commits (Fix A, Fix B, Fix C, docs); LEDGER rows (3 LANDED + 3
DIRTY-BLOCKED); memory round entry.
