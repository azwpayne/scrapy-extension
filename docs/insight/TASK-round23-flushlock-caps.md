# Round 23 — TASK: atomic commit sequence

> Plan: [PLAN-round23-flushlock-caps.md](./PLAN-round23-flushlock-caps.md).
> Each unit = ONE atomic conventional commit. TDD: RED → GREEN → refactor.
> Gate green before merge. Branch `worktree-round23`, ff-merge to `main`.

## Commit 0 — `docs(insight): Round-23 SPEC/PLAN/TASK — flushlock regression + validation caps`

- [x] Write `docs/insight/SPEC-round23-flushlock-caps.md`
- [x] Write `docs/insight/PLAN-round23-flushlock-caps.md`
- [x] Write `docs/insight/TASK-round23-flushlock-caps.md` (this file)
- [ ] `git add docs/insight/*round23* && git commit`

## Commit 1 — `fix(storage): drain buffered items in BatchedStorageStrategy.close() so a slow age-flusher cannot abandon them (R23-A)`

- [ ] RED: `tests/test_storage_strategies.py` — new test
  `test_close_drains_buffer_appended_after_age_flusher_snapshot`: configure
  `max_buffer_age_s` to spawn the flusher; mock `storage_backend.store` to
  block ~50ms each (slow-but-healthy); append 1 item, let flusher snapshot +
  enter its store loop; append a 2nd item to `_buffer`; call `close()`;
  assert BOTH items were stored (the post-snapshot item is NOT lost).
  Confirm it FAILS at HEAD (the post-snapshot item is lost).
- [ ] GREEN: add `_CLOSE_DRAIN_DEADLINE_S = 30.0` const; loop the flusher
  `join` to the deadline in `close()`; then `self.flush()`.
- [ ] Correct docstrings L46 / L219-222 / L248-249 per PLAN.
- [ ] Re-run the unit; confirm GREEN. Keep the existing no-hang test GREEN.

## Commit 2 — `fix(redis): reject non-finite socket timeouts and cap at 86400s (R23-B)`

- [ ] RED: `tests/test_config.py` (or settings test) —
  `RedisSettings(socket_timeout=float('inf'))` and `socket_connect_timeout=1e10`
  raise `ValidationError`; `socket_timeout=86400.0` accepted.
- [ ] GREEN: `field_validator(..., mode="after")` isfinite guard + `le=86400`
  on both Fields.

## Commit 3 — `fix(elasticsearch): reject non-finite request_timeout and cap at 86400s (R23-C)`

- [ ] RED: `ElasticSearchSettings(request_timeout=float('inf'))` raises.
- [ ] GREEN: isfinite `field_validator` + `le=86400` on `request_timeout`.

## Commit 4 — `fix(rabbitmq): cap heartbeat at 65535 to match the AMQP Tune-Ok unsigned-short bound (R23-D)`

- [ ] RED: `RabbitMQSettings(heartbeat=70000)` raises; `65535` accepted.
- [ ] GREEN: `le=65535` on the `heartbeat` Field.

## Commit 5 — `fix(monitor): warn when pop-rate window is pathologically large (R23-E)`

- [ ] RED: `tests/test_queue.py` — constructing `BackendQueue` with a huge
  `monitor_pop_rate_window_s` emits a warning (use `pytest.warns`); document
  the memory cost in the Field description.
- [ ] GREEN: warn-once in `BackendQueue.__init__` when `window_s` exceeds the
  threshold (mirror `queue_delay_max_held`); update Field description.

## Commit 6 — `docs(changelog): record R17-R22 hardening behavior changes (R23-F)`

- [ ] Append R17-R22 entries to `.github/CHANGELOG.md` `[Unreleased] ### Fixed`.

## Commit 7 — `fix(dynamodb): close the candidate HTTP client if publish is interrupted (R23-G)`

- [ ] RED: `tests/test_dynamodb_backend.py` (or resilience) — simulate a
  BaseException during the publish step while a candidate is built-but-not-
  published; assert `_close_resource` called on the candidate and the live
  generation unchanged.
- [ ] GREEN: wrap publish (L528-538) in `try/except BaseException`; identity
  guard `self._generation is not candidate` → `_close_resource(candidate.resource)`;
  re-raise.

## Gate

- [ ] `uv run ruff check` (FIRST — CI gate order)
- [ ] `uv run mypy --strict src/scrapy_extension`
- [ ] `uv run pytest` (≥3782 pass / ≥95% cov; sandbox off +
    `UV_CACHE_DIR=$TMPDIR/uv-cache`)

## Merge

- [ ] code-reviewer (opus) fan-out on `git diff main...HEAD`
- [ ] `git checkout main && git merge --ff-only worktree-round23`
- [ ] `git push origin main`
- [ ] `git branch -d worktree-round23` (+ worktree remove)

## Record

- [ ] Append R23 outcome to `deep-insight-2026-07-23-ultracode.md`
- [ ] Update `MEMORY.md` index line (pytest count, cov, next-fire surface)
- [ ] `docs(memory): Round-23 outcome note`
