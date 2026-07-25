# Round 25 — TASK: atomic commit sequence

> Plan: [PLAN-round25-untouched-surfaces.md](./PLAN-round25-untouched-surfaces.md).
> Each unit = ONE atomic conventional commit. TDD. Branch `worktree-round25`.

## Commit 0 — `docs(insight): Round-25 SPEC/PLAN/TASK — untouched-surfaces (frontier not empty)`

- [x] SPEC / PLAN / TASK written
- [ ] commit

## Commit 1 — `fix(queue): reject dunder callback/errback names so a crafted payload cannot re-init the spider (R25-A)`

- [ ] RED+GREEN: queue.py `_request_from_dict` reject `__`-prefixed; test.

## Commit 2 — `fix(queue): cap snapshot restore size so a corrupt blob cannot OOM startup (R25-B)`

- [ ] RED+GREEN: queue.py `_restore_snapshot` `_MAX_SNAPSHOT_BYTES` guard; test.

## Commit 3 — `fix(storage): validate factory threshold via parse_int_setting so floats don't silently truncate (R25-C)`

- [ ] RED+GREEN: factory.py parse_int_setting(minimum=1); test.

## Commit 4 — `fix(queue): emit delay_depth on drain+clear so the gauge can fall (R25-D)`

- [ ] RED+GREEN: delay.py `_drain_ready` + `clear()` emit; test.

## Commit 5 — `fix(observability): wire monitor into dupefilter/pipeline ConnectionManagers so lifecycle counters cover multi-backend (R25-F)`

- [ ] RED+GREEN: dupefilter + pipeline from_crawler set_monitor; test.

## Commit 6 — `fix(rocketmq): remove dead producer_group + set/storage_topic_prefix config (R25-G/H)`

- [ ] remove 3 Fields + test refs + integration kwarg; docstring note; CHANGELOG Removed entry.

## Gate / Merge / Record

- [ ] ruff → mypy --strict → pytest (≥3787/≥95%)
- [ ] code-reviewer fan-out
- [ ] ff-merge → push → delete branch
- [ ] memory record (R25 outcome + R25-E deferred + frontier-replenishes observation)
