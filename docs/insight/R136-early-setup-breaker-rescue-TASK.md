# R136 TASKS — early-setup breaker rescue + snapshot monitor parity

> Spec: [R136-early-setup-breaker-rescue-SPEC.md](R136-early-setup-breaker-rescue-SPEC.md)

## Task A — rescue breaker policy for early-acquired managers (F1)

- [ ] RED: spider calls `setup_backend()` with NO crawler (early-setup
      docstring pattern), THEN a crawler with Scrapy breaker settings is
      attached and `setup_backend()` re-runs (the `from_crawler` idempotent
      path) → `manager._get_breaker()` must return a breaker with the
      Scrapy-configured threshold. Currently returns `None` (env-only
      fallback cached, no env source) → RED.
- [ ] GREEN: `ConnectionManager.apply_scrapy_breaker_policy(settings)` public
      method (resolve → no-op if empty → under `_lock`, skip when
      `_breaker_configured`, else merge internal keys into `self.settings`);
      mixin calls it on EVERY `setup_backend` invocation, placed beside the
      R14-D `set_monitor` hoist; acquisition-time fold kept (registry-key
      parity).
- [ ] Focused: `uv run --frozen pytest tests/test_spider_mixin.py tests/test_connection_manager.py -q`

## Task B — snapshot manager monitor parity (F2)

- [ ] RED: snapshot-pairing scenario (queue-only backend × stateful strategy ×
      explicit storage) with a crawler carrying stats → the acquired snapshot
      `ConnectionManager` must have received `set_monitor` (its
      `_monitor`/monitor is the resolved `ScrapyStatsMonitor`, not the
      default). Currently never called → RED.
- [ ] GREEN: `_resolve_snapshot_connection_manager` calls
      `snapshot_manager.set_monitor(BackendQueue._resolve_monitor(self))`
      right after `get_manager(...)` (mirrors scheduler.py R55 pairing).
- [ ] Focused: `uv run --frozen pytest tests/test_spider_mixin.py -q`

## Task C — Gate + ship

- [ ] ruff / format / pytest / mypy --strict (plain commands)
- [ ] Atomic commits (A, B, docs), LEDGER rows R136, push HEAD:main,
      memory round entry + MEMORY.md
