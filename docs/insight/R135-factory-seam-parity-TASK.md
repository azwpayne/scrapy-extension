# R135 TASKS — factory-seam parity

> Spec: [R135-factory-seam-parity-SPEC.md](R135-factory-seam-parity-SPEC.md)
> Plan: [R135-factory-seam-parity-PLAN.md](R135-factory-seam-parity-PLAN.md)

## Task A — pipeline owns_connection_manager

- [ ] RED: composite test (owns=False does not close; default closes)
- [ ] GREEN: keyword-only flag + gated release in `_close_locked`
- [ ] Focused: `uv run --frozen pytest tests/test_pipeline.py -q`

## Task B — mixin breaker-policy parity

- [ ] RED: Scrapy-configured breaker reaches the mixin manager (env cleared)
- [ ] GREEN: public `resolve_circuit_breaker_policy` (private alias kept) +
      mixin merge when crawler settings available
- [ ] Focused: `uv run --frozen pytest tests/test_spider_mixin.py tests/test_backend_config_adapter.py -q`

## Task C — mixin snapshot-manager pairing

- [ ] RED: stateful strategy + queue-only backend + explicit storage →
      snapshot manager passed and released; no-storage case unchanged
- [ ] GREEN: mirror scheduler pairing + teardown release
- [ ] Focused: `uv run --frozen pytest tests/test_spider_mixin.py tests/test_scheduler_snapshot_storage_pairing.py -q`

## Task D — Gate + ship

- [ ] ruff / format / pytest / mypy --strict
- [ ] 3 atomic commits; LEDGER rows; push HEAD:main; memory + MEMORY.md
