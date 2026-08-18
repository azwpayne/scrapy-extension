# R137 TASKS — breaker-apply key stability + mixin queue-monitor knobs

> Spec: [R137-breaker-apply-key-stability-SPEC.md](R137-breaker-apply-key-stability-SPEC.md)
> Plan: [R137-breaker-apply-key-stability-PLAN.md](R137-breaker-apply-key-stability-PLAN.md)

## Task A — apply_scrapy_breaker_policy v2 (F1+F2+F3, connectors.py)

- [ ] RED A1 registry-key stability (retired manager must not resurface)
- [ ] RED A2 used-early env-fallback override
- [ ] RED A3 differing-policy one-shot warning, first-resolution-wins
- [ ] GREEN: direct install, no settings mutation, fallback-override flag,
      policy-values compare + one-shot warn
- [ ] Focused: `uv run --frozen pytest tests/test_connection_manager.py tests/test_spider_mixin.py -q`

## Task B — mixin queue monitor knobs (F4+F5, queue.py + spider_mixin.py)

- [ ] RED B1 SCRAPY_MONITOR_* knobs reach get_queue-direct BackendQueue
- [ ] RED B2 NullMonitor upgrade on cached re-entry (guard: non-Null never
      re-wired)
- [ ] GREEN: BackendQueue.set_monitor + mixin _resolve_queue_monitor +
      construction threading + NullMonitor-only upgrade
- [ ] Focused: `uv run --frozen pytest tests/test_spider_mixin.py tests/test_queue.py -q`

## Task C — Gate + ship

- [ ] ruff / format / pytest / mypy --strict (plain commands)
- [ ] Commits: Fix A, Fix B, docs; LEDGER rows (5 LANDED + F6 DEFERRED +
      race REFUTED); push HEAD:main; memory + MEMORY.md
