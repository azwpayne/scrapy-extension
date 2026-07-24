# PLAN — Round 18: connect()-BaseException final parity + R17-B TOCTOU + docs drift

> Spec: [SPEC-round18-parity-tocou.md](SPEC-round18-parity-tocou.md)
> Workflow: worktree `round18-parity-tocou` → execute A→D (atomic commits) → gate → ff-merge to `main` → delete branch.

## Key design lesson (from the scan)
**Identity guards beat lagging flags for BaseException cleanup arms.** R17-B used a
`published` boolean set after the publish side-effect → TOCTOU (finding C). R18 fixes it
with `self._connection is not candidate.connection` (reads actual post-publish state). The
pulsar arm (finding B) uses the same identity pattern (`self._client is not client`) from
the start. **Principle: in a BaseException cleanup arm, decide what to close by inspecting
current instance state, not a local flag that can lag the side-effect.**

## Phases

### Phase 1 — TDD RED (B + C)
- **C** (`tests/test_rabbitmq_generation.py`): patch `_publish_handles_locked` to call the
  REAL publish (installs candidate as `self._connection`) THEN raise `KeyboardInterrupt`
  (simulates the post-publish / pre-`published=True` window). Assert the candidate's
  `connection.close()`/`channel.close()` are NOT called (it's live) and
  `backend._connection is candidate.connection`. Run → RED (current code closes it).
- **B** (`tests/test_pulsar_backend.py` or sibling): make `pulsar.Client(...)` return a mock
  whose presence is tracked, then raise `KeyboardInterrupt` after construction (before
  publish). Assert the mock client's `close()` is called. Run → RED.

### Phase 2 — Implement (GREEN)
- **A** (docs): runbook.md:602 + :630 `dupefilter/filtered` → `dupefilter/hit_count`
  (add `miss_count` as the newly-seen complement where the prose discusses dedup saturation).
- **B**: pulsar `connect()` — hoist `client: Any = None` before the construction; add
  `except BaseException:` arm: `if client is not None and self._client is not client:
  with _suppress_pulsar_errors(): client.close()` then `raise`. Leave the `from None`
  redaction at :444 untouched.
- **C**: rabbitmq except-arm guard — replace `if not published and candidate is not None`
  (line 549) with `if candidate is not None and self._connection is not candidate.connection:`.
  Keep `published` for the normal-flow `if not published:` at :529 (no race in normal flow).
  Update the arm's comment to note the identity guard supersedes the flag (TOCTOU fix).
- **D** (docs): CHANGELOG.md:115 Kafka clear_queue bullet → "raises `QueueError`" (parity
  with README:527 + migration-guide:449).

### Phase 3 — Gate (3 hard gates, unsandboxed for pytest)
```bash
uv run ruff check src/ tests/
uv run mypy --strict src/
uv run pytest -q        # sandbox OFF — engine-e2e probe spawns a subprocess
```

### Phase 4 — Merge to main (main-only)
```bash
git checkout main && git pull --ff-only origin main
git merge --ff-only worktree-round18-parity-tocou
git push origin main          # sandbox-off
git worktree remove --force .claude/worktrees/round18-parity-tocou
git branch -d worktree-round18-parity-tocou
```

## Fan-out strategy (Claude-only)
All 4 units are small + mechanical + precisely specified (exact lines + fix shapes). Main-loop
sequential execution (proven anti-thrash for this repo; avoids git-commit/pytest races from
parallel subagents in a shared worktree). The ultracode 11-agent scan was the multi-agent
fan-out for insight; execution is main-loop. A final parallel code-review fan-out validates.

## Risk notes
- **C test fidelity**: the TOCTOU is a ~1-bytecode window; the test simulates it by patching
  `_publish_handles_locked` to publish-then-raise. Must verify the patched call actually
  invokes the real publish (so `self._connection` becomes `candidate.connection`) before raising.
- **B concurrency**: the identity check `self._client is not client` is read outside the
  lifecycle_lock in the except arm — safe because the close is best-effort (under
  `_suppress_pulsar_errors`); a concurrent disconnect double-close is swallowed.
