# Round 27 — TASK: atomic commit sequence

> Plan: [PLAN-round27-r26-empty-auth.md](./PLAN-round27-r26-empty-auth.md).
> Each unit = ONE atomic conventional commit. TDD. Branch `worktree-r27-es-empty-auth`.

## Commit 0 — `docs(insight): Round-27 SPEC/PLAN/TASK — r26-empty-auth gap (self-caught via diff-regression)`

- [x] SPEC / PLAN / TASK written
- [ ] commit

## Commit 1 — `fix(elasticsearch): treat empty-string auth as absent so CLOUD fail-fast + cleartext guard match _build_kwargs truthiness (R27-A)`

- [ ] RED: 3 tests in test_elasticsearch_backend.py (empty api_key CLOUD; empty basic_auth CLOUD; empty api_key STANDALONE-http not blocked)
- [ ] GREEN: settings/elasticsearch.py `:192` `:193` `:225` → `bool(...)` (3 one-token edits)

## Gate / Merge / Record

- [ ] ruff → mypy --strict → pytest (≥3802/≥95%)
- [ ] reviewer (general-purpose+opus or inline; NOT agent-skills:code-reviewer)
- [ ] ff-merge → push → delete branch
- [ ] memory record
