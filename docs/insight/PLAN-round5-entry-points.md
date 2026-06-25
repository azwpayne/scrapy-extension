# PLAN — Round 5: Entry-Point Plugin Registration (Tier-2)

Architect-selected round-5 pick. Kills the round-2 MEDIUM "4 hand-synced backend registries"
coupling debt and lets 3rd-party packages register a backend via
`[project.entry-points."scrapy_extension.backends"]` without forking. Companion to
[`INSIGHTS-2026-06-25.md`](./INSIGHTS-2026-06-25.md) (Theme F) and
[`PLAN-round4-backpressure.md`](./PLAN-round4-backpressure.md).

## Decision (architect, option b)

- **Unify the 4 registries** (`_BACKEND_FACTORIES` + 3 capability sets in `connectors.py`) into ONE
  `BackendDescriptor` table in a NEW `backends/registry.py`, keyed by backend-type STRING.
- **`BackendType` enum stays** as a convenience enum for the 10 bundled backends; 3rd-party backends
  are plain strings. `_missing_` still raises (fail-fast UX preserved).
- **`resolve_backend_config` stops forcing `BackendType(...)`** — treats `backend_type` as an opaque
  string validated against the descriptor table. `required_capabilities: set[str]` (was `set[BackendType]`).
- **Lazy-import PRESERVED (critical)**: `_BUNDLED_DESCRIPTORS` is a STATIC dict of dotted-path strings
  (never imported at registry-build time). Bundled backends are NOT registered as entry-points (that
  would eager-import their optional deps, breaking "core works without any backend dep"). Entry-points
  are strictly the 3rd-party path.
- **Rejected**: `BackendType._missing_` synthesizing members (mutable enum, breaks fail-fast); a 5th
  entry-point loader map alongside `_BACKEND_FACTORIES` (leaves the debt).

## 3rd-party contract

- **Group**: `scrapy_extension.backends`.
- **Name**: backend-type string (`^[a-z][a-z0-9_]*$`), e.g. `"mybackend"` — the `SCRAPY_BACKEND_TYPE` value.
- **Value**: dotted path to a registration CALLABLE (no args) returning a `BackendDescriptor`:
  ```python
  @dataclass(frozen=True)
  class BackendDescriptor:
    backend_type: str
    backend_cls_path: str          # "mypkg.backends.MyBackend"
    settings_cls_path: str         # "mypkg.settings.MySettings"
    capabilities: frozenset[str]   # subset of {"queue","set","storage"}
  ```
  ONE registration declares the backend class, settings class, AND capability matrix — no editing
  other registries. The callable returns PATHS only (must NOT import the backend module).
- **Precedence**: bundled-wins + `UserWarning` on name conflict (deterministic, safe; the project's
  `error::UserWarning` pytest filter makes this load-bearing in tests).

## Units (fan-out; coherent refactor + parallel docs)

### Unit R5-1 — refactor (one executor; the chain is tightly coupled)
**Files**: NEW `src/scrapy_extension/backends/registry.py`; `src/scrapy_extension/backends/connectors.py`
(delete 4 registries, dispatch via `get_descriptor`); `schedule/scheduler.py` + `dupefilter/dupefilter.py`
+ `pipeline/pipeline.py` (pass `{"queue"}`/`{"set"}`/`{"storage"}` to `resolve_backend_config`);
NEW `tests/test_registry.py`; extend `tests/test_connectors.py`. Also widen `backends/base.py`
`Backend.backend_type` annotation to `BackendType | str` (additive).

### Unit R5-2 — docs (parallel, independent)
**Files**: `CLAUDE.md` (Backend Implementation Matrix note: 3rd-party via entry-points);
NEW `docs/backend-plugins.md` (the 3rd-party contract: group, descriptor dataclass, capability
frozenset, bundled-wins precedence, lazy-import rule, a worked example).

## TDD contract (the acceptance gate — `tests/test_registry.py` + `test_connectors.py`)
1. **bundled_still_work**: `SCRAPY_BACKEND_TYPE=redis` → resolves + builds `RedisBackend`. Byte-identical.
2. **third_party_discovered**: mock entry-point → registry returns its descriptor → resolves + instantiates stub.
3. **capability_gated**: 3rd-party descriptor `{"queue"}` only → selecting for set/storage → `ConfigurationError` w/ `setting_name` + capable-backend list.
4. **name_conflict_bundled_wins**: entry-point named `"redis"` → bundled descriptor wins + `UserWarning`.
5. **import_error_graceful_skip**: entry-point callable raising `ImportError` → skipped + warned, bundled 10 intact.
6. **lazy_import_preserved**: `import scrapy_extension` + `get_registry()` with NO optional dep → returns 10 descriptors; assert `"redis"` NOT in `sys.modules` after.
7. **py310_py312_entry_point_api**: `_discover_entry_points` works on both `entry_points(group=...)` (3.12+) and `entry_points()[group]` (3.10/3.11) shapes.

Plus: registry-cache isolation helper `_reset_registry_cache()` (test-only, mirrors `ConnectionManager.clear_registry`).

## Acceptance
- 7 TDD tests green; `uv run pytest` full suite green (no regression in test_connectors/test_connection_manager/test_components).
- `ruff check` / `mypy` / `bandit` clean; coverage ≥ 95% on `registry.py` + refactored `connectors.py`.
- Backward-compat: `SCRAPY_BACKEND_TYPE=redis` (all 10 bundled) byte-identical; `BackendType` still exported + usable.
- Independent verifier + code-reviewer approval lane: APPROVE, 0 CRITICAL/HIGH — especially that lazy-import holds (Test #6) and one broken plugin never breaks the bundled set (Test #5).

## Non-goals (remain Tier-2/3)
- Security-parity cluster (round-6). Distributed Delay/Throttle. Sentinel failover re-discovery.
- rocketmq-client replacement. B5 reconnect in-flight-survival test.
