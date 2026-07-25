# Round 24 — TASK: atomic commit sequence

> Plan: [PLAN-round24-docs-hygiene.md](./PLAN-round24-docs-hygiene.md).
> Docs-only round. Branch `worktree-round24`, ff-merge to `main`.

## Commit 0 — `docs(insight): Round-24 SPEC/PLAN/TASK — docs-hygiene (code surface clean)`

- [x] Write `docs/insight/SPEC-round24-docs-hygiene.md`
- [x] Write `docs/insight/PLAN-round24-docs-hygiene.md`
- [x] Write `docs/insight/TASK-round24-docs-hygiene.md` (this file)
- [ ] `git add docs/insight/*round24* && git commit`

## Commit 1 — `docs(migration): warn that redis/es timeouts reject non-finite + are capped at 86400s (R24-A)`

- [ ] `docs/migration-guide.md` Configuration Changes section: add R23-B/C
      timeout-cap paragraph (SCRAPY_REDIS_SOCKET_TIMEOUT / SOCKET_CONNECT_TIMEOUT
      / SCRAPY_ES_REQUEST_TIMEOUT).

## Commit 2 — `docs(runbook): document the pop_rate_window 24h cap + RocketMQ send_timeout/max_message_size caps (R24-B/C)`

- [ ] `docs/runbook.md:586` — append 86400s cap to SCRAPY_MONITOR_POP_RATE_WINDOW_S row.
- [ ] `docs/runbook.md` per-item-byte-cap section — add R22-A send_timeout 5min
      cap + R22-C max_message_size push-time QueueError note.

## Gate

- [ ] `uv run ruff check` (confirm no stray .py edit)
- [ ] `uv run mypy --strict src/scrapy_extension`
- [ ] `git diff --stat` (confirm docs-only; pytest skipped — no .py touched)

## Merge

- [ ] `git checkout main && git merge --ff-only worktree-round24`
- [ ] `git push origin main`
- [ ] `git branch -d worktree-round24` (+ worktree remove)

## Record

- [ ] Append R24 outcome to `deep-insight-2026-07-23-ultracode.md`
      (frontier-thinning observation: first round with zero code defects)
- [ ] Update `MEMORY.md` index line
