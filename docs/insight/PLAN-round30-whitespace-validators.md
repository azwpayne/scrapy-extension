# Round 30 — PLAN + TASK: whitespace-validator gaps (rabbitmq + redis)

> Spec: [SPEC-round30-whitespace-validators.md](./SPEC-round30-whitespace-validators.md).
> TDD (RED → GREEN), each unit = one atomic commit. Claude-Code-only, main-loop
> (429 cap blocks subagents — degraded mode per memory).

## Commit 0 — `docs(insight): Round-30 SPEC/PLAN/TASK — rabbitmq+redis whitespace-validator gaps`
- [x] written  [ ] commit

## Commit 1 — `fix(rabbitmq): strip-aware virtual_host + ha_mode checks so whitespace no longer bypasses fail-fast (R30-A/B)`
- **A** `backends/rabbitmq.py:377` `not virtual_host` → `not virtual_host.strip()`
- **B** `settings/rabbitmq.py:471` `and not self.ha_mode` → `and not self.ha_mode.strip()`; `backends/rabbitmq.py:372` `and not ha_mode` → `and not ha_mode.strip()`
- RED: `test_virtual_host_whitespace_rejected`; `test_ha_mode_whitespace_rejected` (MIRRORED_QUEUES)

## Commit 2 — `fix(redis): strip-aware sentinel_master_name so whitespace no longer bypasses the SENTINEL missing-field check (R30-C)`
- `settings/redis.py:537` strip-aware (`name = self.sentinel_master_name; if not name or not name.strip(): missing.append(...)`)
- RED: `test_sentinel_master_name_whitespace_rejected`

## Gate / Merge / Record
- [ ] ruff → mypy --strict → pytest (≥3824/≥95%; UV_CACHE_DIR=$TMPDIR/uv-cache + sandbox off)
- [ ] ff-merge → push → delete branch
- [ ] memory record (note: 429-degraded round; R31 deferred candidates)
