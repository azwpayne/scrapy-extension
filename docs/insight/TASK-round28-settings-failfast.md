# Round 28 — TASK: atomic commit sequence

> Plan: [PLAN-round28-settings-failfast.md](./PLAN-round28-settings-failfast.md).
> Each unit = ONE atomic conventional commit. TDD. Branch `worktree-r28-settings-validation`.

## Commit 0 — `docs(insight): Round-28 SPEC/PLAN/TASK — r27-empty-auth incompleteness + settings fail-fast gaps`

- [x] SPEC / PLAN / TASK written
- [ ] commit

## Commit 1 — `fix(elasticsearch): complete R27-A's truthiness fix on the auth-exclusivity validator so empty-secret + basic_auth is no longer falsely rejected (R28-A)`

- [ ] RED: 2 tests in test_elasticsearch_backend.py (empty api_key + basic_auth accepted; real api_key + empty password accepted) + keep both-real exclusivity test green
- [ ] GREEN: settings/elasticsearch.py `:268` `:270` `:277` `:279` truthiness

## Commit 2 — `fix(elasticsearch): reject empty STANDALONE hosts list so it fails fast, not as an opaque client error at connect (R28-B)`

- [ ] RED: test_standalone_empty_hosts_rejected (CLOUD+[] still passes)
- [ ] GREEN: settings/elasticsearch.py `_validate_hosts_scheme` non-empty guard

## Commit 3 — `fix(kafka): reject empty/whitespace CONFLUENT endpoints so they fail fast, not as an opaque kafka error at connect (R28-C)`

- [ ] RED: 2 tests (empty bootstrap_servers; whitespace bootstrap_servers) + keep R26-E localhost test green
- [ ] GREEN: settings/kafka.py `:462-463` effective-value `.strip()` + `in ("", "localhost:9092")`

## Gate / Merge / Record

- [ ] ruff → mypy --strict → pytest (≥3808/≥95%)
- [ ] reviewer (inline or general-purpose+opus; NOT agent-skills:code-reviewer)
- [ ] ff-merge → push → delete branch
- [ ] memory record
