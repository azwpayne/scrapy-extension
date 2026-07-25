# Round 26 — TASK: atomic commit sequence

> Plan: [PLAN-round26-r25-regression.md](./PLAN-round26-r25-regression.md).
> Each unit = ONE atomic conventional commit. TDD. Branch `worktree-round26`.

## Commit 0 — `docs(insight): Round-26 SPEC/PLAN/TASK — r25-regression catch + cross-cutting hardening`

- [x] SPEC / PLAN / TASK written
- [ ] commit

## Commit 1 — `fix(queue): raise snapshot cap to 128MiB + warn at persist so a legit large heap isn't silently dropped on restart (R26-A)`

- [ ] RED+GREEN: queue.py `_MAX_SNAPSHOT_BYTES` 16→128MiB; `_persist_snapshot` warn-on-over-cap; runbook note.

## Commit 2 — `fix(scheduler): make _close_locked BaseException-safe so a Ctrl+C during teardown cannot pin the connection manager (R26-G)`

- [ ] RED+GREEN: scheduler.py `_close_locked` primary_error pattern (mirror pipeline R20-B); test.

## Commit 3 — `fix(elasticsearch): require auth in CLOUD mode so a no-auth config fails fast, not as an opaque health-check 401 (R26-F)`

- [ ] RED+GREEN: elasticsearch.py `validate_mode_requirements` ≥1 auth method; update no-auth fixture.

## Commit 4 — `fix(queue): validate dumps_kwargs so a crafted JsonRequest payload gives a clean TypeError (R26-D)`

- [ ] RED+GREEN: queue.py `_validate_request_dict` `require_type("dumps_kwargs", dict)`; test.

## Commit 5 — `fix(kafka): reject CONFLUENT mode still pointing at the localhost default (R26-E)`

- [ ] RED+GREEN: kafka.py `_validate_authentication` localhost-default guard; update fixture.

## Commit 6 — `test(queue,dupefilter,pipeline): cover the ismethod callback arm + assert R25-F wired ScrapyStatsMonitor (R26-B/C)`

- [ ] test_queue.py rename + new ismethod-arm test; test_dupefilter.py + test_pipeline.py isinstance assert.

## Gate / Merge / Record

- [ ] ruff → mypy --strict → pytest (≥3796/≥95%)
- [ ] code-reviewer fan-out (if rate-limit reset)
- [ ] ff-merge → push → delete branch
- [ ] memory record
