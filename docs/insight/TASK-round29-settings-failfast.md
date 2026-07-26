# Round 29 — TASK: atomic commit sequence

> Plan: [PLAN-round29-settings-failfast.md](./PLAN-round29-settings-failfast.md).
> Each unit = ONE atomic conventional commit. TDD. Branch `worktree-r29-settings-failfast`.

## Commit 0 — `docs(insight): Round-29 SPEC/PLAN/TASK — kafka runtime strip + mongodb/rocketmq name fail-fast`

- [x] SPEC / PLAN / TASK written
- [ ] commit

## Commit 1 — `fix(kafka): make _bootstrap_servers strip-aware so whitespace confluent falls back to bootstrap_servers, matching the R28-C validator (R28-C-1)`

- [ ] RED: test_bootstrap_servers_strips_whitespace_confluent (kafka backend test)
- [ ] GREEN: backends/kafka.py:496 `(confluent_bootstrap_servers or "").strip() or bootstrap_servers`

## Commit 2 — `fix(mongodb): reject empty/whitespace collection names, database, replica_set members, and replica_set_name so they fail fast, not as opaque pymongo errors at connect (R29-A/B/C/D)`

- [ ] RED: 4 tests (collection empty; replica_set_members empty element; database empty; replica_set_name whitespace)
- [ ] GREEN: settings/mongodb.py helper non-empty (A) + field_validators (B/C) + REPLICA_SET strip (D)

## Commit 3 — `fix(rocketmq): reject zero max_message_size and empty/whitespace consumer_group so they fail fast (R27-RMQ-1/2)`

- [ ] RED: 2 tests (max_message_size=0 rejected; consumer_group empty/whitespace rejected)
- [ ] GREEN: settings/rocketmq.py max_message_size gt=0 + consumer_group min_length + whitespace validator

## Gate / Merge / Record

- [ ] ruff → mypy --strict → pytest (≥3818/≥95%)
- [ ] reviewer (inline or general-purpose+opus; NOT agent-skills:code-reviewer)
- [ ] ff-merge → push → delete branch
- [ ] memory record
