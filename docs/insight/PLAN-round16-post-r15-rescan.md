# PLAN — Round 16: Post-Round-15 Re-scan Closeout

> **Companion.** [`SPEC-round16-post-r15-rescan.md`](./SPEC-round16-post-r15-rescan.md), [`TASK-round16-post-r15-rescan.md`](./TASK-round16-post-r15-rescan.md). Source: 2026-07-24 ultracode re-scan `wf_3c9efaf9-23c`.

**Strategy.** Round 15 is clean; this is a small bounded batch (1 real defect + self-corrections + polish). Execute via parallel subagents where file-disjoint, TDD, atomic commits, then merge to main (main-only). `/goal` is not wired in this environment (omc CLI absent — see memory) → execute the plan inline via fanned-out Claude subagents.

## Round sequence

### Round 16.1 — R16-A solo (only real defect)  🔴
- Kafka + rocketmq `connect()` `except BaseException` arm. TDD (KeyboardInterrupt mid-construction). Mirror `elasticsearch.py:132-136`.
- Executor: opus subagent (or main-loop). Files: `kafka.py`, `rocketmq.py` + tests.

### Round 16.2 — R16-B / R16-C / R16-D parallel (file-disjoint)
| Unit | File | Disjoint? |
|---|---|---|
| R16-B README | `README.md` | ✓ |
| R16-C conftest | `tests/conftest.py` | ✓ |
| R16-D dynamodb comment | `backends/dynamodb.py` | ✓ |
3 executors, no shared files. R16-C (conftest) is the only test-infra change — run full suite at the gate to validate its ~14 consumers.

### Round 16.3 — R16-E docs/operability bundle
- runbook + STABILITY (docs, disjoint) ∥ pulsar/rabbitmq/memcached `from None`→`from e` (src, disjoint). TDD the `from e` change (assert `__cause__` preserved).

### Round 16.4 — gate 🔧
1. `uv run ruff check src/ tests/` → clean
2. `uv run mypy --strict src/` → 0 errors
3. `uv run pytest -q` → all green; coverage ≥ 95%

### Round 16.5 — merge to main (main-only)
1. Exit worktree (keep).
2. On main: `git merge --ff-only worktree-round15-frontier` → `git push origin main`.
3. Delete the worktree branch + remove the worktree dir.
4. Future rounds commit directly to main (no feature branches).

## Dependency graph
```
R16-A ──┐
R16-B ──┤
R16-C ──┼──► gate ──► merge-to-main (main-only)
R16-D ──┤
R16-E ──┘
```
R16-B/C/D file-disjoint (parallel). R16-E docs disjoint from B/C/D.

## Risk register
- **R16-A caller-trace.** Before adding the arm, confirm no caller depends on BaseException propagating unhandled past connect() (connectors.py `_attempt_connection` `except Exception` already skips BaseException by design — the arm just adds cleanup before re-raise; semantics preserved).
- **R16-C blast radius.** conftest fixture change → validate all ~14 consumers at the gate.
- **R16-E(iii) behavior.** `from None`→`from e` only enriches tracebacks (no exception-type change); still add a test asserting `__cause__` is the broker error.
- **Thrash guard.** Large files (kafka.py 1359, dynamodb.py 938) — if a subagent autocompacts, fall back to main-loop TDD with targeted reads (lesson: opus survives, sonnet thrashes; Claude-only regardless).

## Out of scope
R16-F deferred items; the 2 unverified/thrashed re-scan findings (re-scan with tighter caps if pursued).
