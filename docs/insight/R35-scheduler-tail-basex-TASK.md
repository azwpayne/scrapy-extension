# R35 — TASK: BackendScheduler._close_locked state-reset tail fix

> Back-navigation: [SPEC](./R35-scheduler-tail-basex-SPEC.md) ·
> [PLAN](./R35-scheduler-tail-basex-PLAN.md) ·Driven by durable cron `038706c4`.

## Pre-flight (gate 0)

- [ ] `git status --porcelain` clean
- [ ] `git branch --show-current` = `R35-insight-round`
- [ ] `git rev-list --left-right --count R35-insight-round...main` shows only forward
- [ ] `gh pr list --state open --json number` shows no F7 conflicts
- [ ] `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv --version` exit 0
- [ ] `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run ruff --version` exit 0
- [ ] `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run mypy --version` exit 0
- [ ] `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run pytest --version` exit 0
- [ ] Read `docs/insight/LEDGER.md` (initialized at `5f3a3a6`) — confirm F7 not LANDED/REFUTED/DUPLICATE
- [ ] Read `docs/insight/SPEC-round34-sched-exc-catch.md` — confirm no overlap (F7 is the state-tail AFTER consume_reservation, not the consume_reservation ghost window itself)

Any failure → abort with `BLOCKED` reason. Do NOT stash, reset, or amend.

## Branch setup (gate 1)

- [ ] From `R35-insight-round`: `git checkout -b R35-scheduler-tail-basex`
- [ ] Confirm `git branch --show-current` = `R35-scheduler-tail-basex`
- [ ] Confirm `git status` clean

## RED tests (gate 2)

Write `tests/test_scheduler_close_basex_tail.py` with these tests
(detailed in PLAN.md §"Test changes"):

- [ ] `test_scheduler_close_resets_state_tail_after_dupefilter_keyboardinterrupt`
- [ ] `test_scheduler_close_resets_state_tail_after_signal_disconnect_baseexception`
- [ ] `test_scheduler_close_resets_state_tail_after_queue_close_baseexception`
- [ ] `test_scheduler_close_normal_path_unaffected`
- [ ] `test_scheduler_close_reentrant_short_circuit`

Run with: `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run pytest tests/test_scheduler_close_basex_tail.py -v`

Verify RED:

- [ ] All five tests FAIL with assertion errors on the pre-fix tree.
- [ ] The failures reference the correct attributes (`_queue is not None`,
  `_connected_signals is not None`, etc.) — confirms the RED tests target
  the actual bug surface, not a helper bypass.
- [ ] No existing tests in `tests/test_scheduler.py` /
  `tests/test_scheduler_lifecycle.py` / `tests/test_scheduler_resilience.py`
  break (sanity: the RED tests must be the only failures).

If RED tests pass on the pre-fix tree → STOP, the finding is REFUTED,
re-classify as `REFUTED`, do NOT proceed to GREEN.

If existing tests break → STOP, re-investigate (the fix shape may be wrong).

## GREEN (gate 3)

Apply the single-block edit in `src/scrapy_extension/schedule/scheduler.py`
described in PLAN.md §"Changes". Specifically: move the six-line state-reset
tail from inside the `try` (lines 1510-1515) to the `finally` block (1521),
BEFORE the manager-release block. Keep all per-handler `except Exception`
guards intact.

Run the RED test file again:

- [ ] All five new tests PASS.
- [ ] No existing test in `tests/` fails (target ≥ R34 3833 + 5 new = 3838).

## Verification gate (gate 4)

In order:

- [ ] `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run ruff check .` exit 0
- [ ] `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run mypy --strict src/scrapy_extension` exit 0
- [ ] `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run pytest -q` exit 0, count ≥ 3838

Any non-zero exit or unexpected skip → STOP, re-classify as `BLOCKED`,
do NOT commit.

## Commit (gate 5)

- [ ] `git diff R35-insight-round...HEAD` shows ONLY:
  - `src/scrapy_extension/schedule/scheduler.py` (the single block move)
  - `tests/test_scheduler_close_basex_tail.py` (new file)
  - `docs/insight/R35-scheduler-tail-basex-SPEC.md`, `PLAN.md`, `TASK.md`
- [ ] `git add src/scrapy_extension/schedule/scheduler.py tests/test_scheduler_close_basex_tail.py docs/insight/R35-scheduler-tail-basex-*.md`
- [ ] `git commit -m "fix(scheduler): R35-F7 reset state-detach tail on BaseException teardown"`
- [ ] Commit body references finding id `R35-F7` and SPEC/PLAN/TASK paths.
- [ ] No co-authored-by trailers; no force-push markers.

## Merge (gate 6)

- [ ] `git push -u origin R35-scheduler-tail-basex` (no force, no tags)
- [ ] `gh pr create --base main --head R35-scheduler-tail-basex --title "fix(scheduler): R35-F7 reset state-detach tail on BaseException teardown" --body-file <(cat docs/insight/R35-scheduler-tail-basex-SPEC.md docs/insight/R35-scheduler-tail-basex-PLAN.md) --draft`
- [ ] Wait for project CI to go green (no self-approval, no auto-merge).
- [ ] If CI red → STOP, fix, push again; do NOT bypass.
- [ ] If CI green → `gh pr merge --squash --delete-branch` (ff-merge preferred).

## Post-merge (gate 7)

- [ ] `git checkout main && git pull --ff-only`
- [ ] `git rev-parse HEAD` matches the merged SHA
- [ ] `git status --porcelain` clean
- [ ] `git branch -d R35-scheduler-tail-basex` (local; remote already deleted by gh)
- [ ] `git push origin --delete R35-scheduler-tail-basex` (only if not deleted by gh)
- [ ] Re-run on main:
  - `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run ruff check .`
  - `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run mypy --strict src/scrapy_extension`
  - `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run pytest -q`

## Ledger (gate 8)

- [ ] Edit `docs/insight/LEDGER.md`: append row
  `R35 | F7 | schedule/scheduler.py:1462-1541 | scheduler-tail-after-basex | LANDED | <commit SHA> + PR #`
- [ ] Commit the LEDGER update as a separate atomic commit:
  `docs(insight): R35-F7 ledger entry`
- [ ] Push; do NOT merge yet (let the ledger commit ride the same PR, or
  a follow-up PR; whichever the project conventions prefer — confirm
  before pushing).

## Memory record (gate 9)

After successful merge:

- [ ] Append a per-round note to MEMORY.md (in
  `~/.claude/projects/.../memory/MEMORY.md`) summarizing F7: trigger
  surface, fix shape (R26-G precedent), pytest count delta, lessons
  (state-tail must be in `finally`, not `try`; cron-driven R-round
  pipeline functional).

## Out of scope (do NOT touch in this round)

- F3 (Monitor.on_disconnect reason) — separate subsystem (monitor), R36 candidate.
- F4 (set_monitor race) — separate subsystem, R36 candidate.
- F5 (monitor KeyboardInterrupt) — separate subsystem, R36 candidate.
- F6 (mixin close_backend BaseException) — caller-side complement; ship in R36 after F7.
- F8 (batched flusher start failure) — separate subsystem (storage strategies), R36 candidate.
- STORAGE-02/03/04 (already deferred/iterative).
- R33 dupefilter `_close_locked` retry design — DO NOT RE-FLAG.
- R26-G scheduler manager release — DO NOT RE-FLAG.
- R34 scheduler consume_reservation ghost window — DO NOT RE-FLAG.

## Stop states (one of these only)

- `SHIPPED` — all gates green, merged, ledger updated, memory recorded.
- `DOCS_ONLY` — gates failed at RED; finding REFUTED or DEFERRED.
- `BLOCKED` — any gate failed; reason recorded in the round log.
- `QUOTA_EXHAUSTED` — Claude rate limits; do NOT push, do NOT merge,
  record ETA, retry on next cron.