# R135 PLAN — factory-seam parity

> Spec: [R135-factory-seam-parity-SPEC.md](R135-factory-seam-parity-SPEC.md)

## Phase 1 — TDD RED (three findings, independent files)

- A (`tests/test_pipeline.py`): composite test — acquire one manager, build
  `BackendPipeline(manager, owns_connection_manager=False)`, run
  `close_spider`; assert the manager was NOT closed (still usable /
  `_users` unchanged) and that the default (no kwarg) still closes.
- B (`tests/test_spider_mixin.py`): with crawler settings carrying
  `SCRAPY_CIRCUIT_BREAKER_ENABLED=True` + threshold/reset (env vars
  cleared), the mixin's manager `_get_breaker()` returns a live breaker
  with the Scrapy-configured threshold; without any source, behavior
  unchanged (no internal keys, env fallback intact).
- C (`tests/test_spider_mixin.py` or a sibling): queue-only backend +
  `SCRAPY_QUEUE_STRATEGY=delay` + `SCRAPY_STORAGE_BACKEND_TYPE=redis`
  (mocked managers per the file's conventions) → the mixin's
  `get_queue()` BackendQueue receives a snapshot connection manager and
  close_backend releases it after the queue; no-explicit-storage case stays
  best-effort (no crash).

## Phase 2 — Implement (GREEN)

- A: pipeline.py — keyword-only `owns_connection_manager: bool = True`;
  store flag; `_close_locked` releases only when owning (latch still always
  set once); docstring mirrors the siblings' composite wording.
- B: connectors.py — rename `_resolve_circuit_breaker_policy` → public
  `resolve_circuit_breaker_policy` (private name kept as alias; internal
  call sites may use either); spider_mixin.py — in setup_backend, when
  crawler settings are available, merge the resolved policy into the
  manager settings dict before `get_manager` (empty dict = no-op).
- C: spider_mixin.py — in the queue-construction path, mirror
  scheduler.py:1157-1188 (stateful strategy × queue-only backend →
  resolve storage component; explicit override fail-fast, absent override
  ConfigurationError → best-effort skip), pass
  `snapshot_connection_manager=` to BackendQueue, release it in
  close_backend teardown after the queue closes (match scheduler ordering).

## Phase 3 — Gate (plain commands)

```bash
uv run --frozen ruff check src tests conftest.py
uv run --frozen ruff format --check src tests conftest.py
uv run --frozen pytest
uv run --frozen mypy --strict src
```

## Phase 4 — Ship

Three atomic commits (A/B/C), LEDGER rows, push HEAD:main, memory round
entry.
