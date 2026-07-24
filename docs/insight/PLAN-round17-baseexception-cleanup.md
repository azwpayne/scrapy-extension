# PLAN — Round 17: BaseException cleanup-parity + abort null-ordering

> Spec: [SPEC-round17-baseexception-cleanup.md](SPEC-round17-baseexception-cleanup.md)
> Workflow: worktree `round17-baseexception` → execute units A→D (atomic commits) → gate → ff-merge to `main` → delete branch.

## Phases

### Phase 1 — Pin the regressions (TDD RED)
Write the failing tests FIRST, run them, confirm RED:
- **A**: `tests/test_kafka_connect_cleanup.py` — assert `backend._producer is None` AND
  `backend._admin_client is None` when `mock_producer.close.side_effect = KeyboardInterrupt`
  (and a `SystemExit` variant). Today's R16-A test does not assert null-after-second-interrupt → RED.
- **B**: `tests/test_rabbitmq_connect_cleanup.py` (or sibling) — simulate `Ctrl+C`
  (`KeyboardInterrupt`) raised by `_publish_handles_locked` / in the candidate→publish window; assert
  the candidate's `connection.close()` + `channel.close()` were invoked (no leak).
- **C**: `tests/test_memcached_connect_cleanup.py` — `KeyboardInterrupt` raised by `candidate.stats()`;
  assert `candidate.close()` was invoked.
- **D**: real-CM test in `test_mock_connection_manager_contract.py` exercising connectors.py:1654-1659.

### Phase 2 — Implement (GREEN)
- **A**: rewrite `_abort_partial_connect` null-first (mirror mongodb `_discard_client` 195-220 +
  rocketmq `_abort_partial_connect` 253-268). Capture `producer = self._producer; admin = self._admin_client`;
  set `self._producer = None; self._admin_client = None` FIRST; then `for closer in (producer, admin):
  if closer: try: closer.close() except Exception: logger.debug(...)`. Update the docstring claim
  ("mirrors mongodb") to be TRUE.
- **B**: wrap build+publish (491-534) so a `BaseException` closes the candidate iff `not published`.
  Hoist `candidate: _RabbitMQCandidate | None = None` + `published = False` before the try; add
  `except BaseException:` → `if not published and candidate is not None: self._close_handles(
  candidate.channel, candidate.connection); raise`. Keep `_close_handles` idempotency (double-close safe).
- **C**: add `except BaseException:` arm to the build try (mirroring the existing `except Exception`
  at 147-153): `if candidate is not None: with _swallow(): candidate.close(); raise`.
- **D**: retitle existing test module/class as fixture-parity (docstring: "pins the fixture closure;
  production parity asserted in test_connectors.py::TestOperationBoundQueueDurability"); add
  `test_push_durability_translation_uses_real_connection_manager` building a real
  `ConnectionManager(BackendType.REDIS)` with a fake `_push_with_durability` raising
  `_DurablePushRequired`, asserting `_push_queue_with_durability(..., require_durable=True)` raises
  `QueueError(queue_name=..., operation="push")`.

### Phase 3 — Gate (3 hard gates)
```bash
uv run ruff check src/ tests/
uv run mypy --strict src/
UV_CACHE_DIR=$TMPDIR/uv-cache uv run pytest -q   # expect ≥3757 passed
```
If sandbox blocks: `UV_CACHE_DIR=$TMPDIR/uv-cache` for pytest; sandbox-off for git push / gh.

### Phase 4 — Merge to main (main-only)
```bash
git checkout main && git pull --ff-only origin main
git merge --ff-only worktree-round17-baseexception
git push origin main          # sandbox-off
git branch -d worktree-round17-baseexception   # after ExitWorktree(remove) or from main
```
Open NO PR (user's main-only + atomic-merge constraint); push the ff-merge directly.

## Fan-out strategy (Claude-only)
- Opus subagents for A + B (large files; opus survives, sonnet thrashes — proven fire-9/10/15 lesson).
  Tight per-unit scope; TDD RED→GREEN; one atomic commit each.
- Main-loop for C + D (small surfaces; cheaper than spawning).
- Sequential merge to main (one ff-merge carrying all 4 commits) to honor "all branches merge to main".

## Risk notes
- **B subtlety**: the `except BaseException` must NOT close a candidate that was already published
  (it's then the live `self._connection`). The `if not published` guard is load-bearing — the test
  must also assert a published candidate is NOT closed when a later BaseException occurs.
- **A is an R16-A regression**: the existing R16-A test stays green (it asserts the abort runs); the
  new assertion layers the null-after-second-interrupt guarantee on top.
