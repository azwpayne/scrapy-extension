# SPEC: connectors/scheduler module split (pure move)

**Date**: 2026-08-22
**Commits**: `012a268` (backends) · `dc96788` (schedule)
**Kind**: structural refactor — zero behavior change, zero public-API change

## Why

Bug history concentrates where file size concentrates: `backends/connectors.py`
(4192 lines; 5× the 800-line ceiling) was the birthplace of R137-F1 (registry-key
mutation landmine), R132 (concurrent-disconnect handle races), and R71 (pipeline
teardown). `schedule/scheduler.py` (3729 lines) had the same shape. The logic was
already clean (117 rounds of adversarial scanning); only the module mass was the
problem. `queue/queue.py` was deliberately **not** split: it is one cohesive
~1900-line class whose pure-move yield is <10% (decision recorded here so future
rounds don't re-litigate it).

## What moved

### `backends/connectors.py` → `backends/connectors/` package

| Submodule | Responsibility | Lines |
|---|---|---|
| `_constants.py` | settings-key maps, safe-message allow-lists, bundled-type snapshot | 157 |
| `_diagnostics.py` | `_wait_for_retry_backoff`, `_log_diagnostic` | 38 |
| `_capabilities.py` | capability gates + `_load_object` | 83 |
| `_plugin_contract.py` | 3rd-party plugin validation + `_DeferredAckPluginQueueBackend` | 639 |
| `_config.py` | `resolve_backend_config`, breaker policy parsing | 504 |
| `_manager.py` | `ConnectionManager` + lease/registry helpers + `release_manager_acquire` | ~2380 |
| `__init__.py` | facade re-exporting the entire historical module surface | 256 |

Dependency DAG (verified acyclic before the move):
`_constants`/`_diagnostics`/`_capabilities` (leaves) ← `_plugin_contract` ←
`_config` ← `_manager`. Submodules never import the package root or
`backends` package root at module level.

### `schedule/scheduler.py` → three helper modules

- `_lifecycle.py` — deferred lifecycle results, `_LifecycleContinuation`,
  attempt tokens, signal receiver/lease hierarchy
- `_queue_config.py` — `_QueueComponentConfig`
- `_dupefilter_compat.py` — `_MISSING_STATIC_ATTRIBUTE` + dupefilter
  open/declaration compatibility helpers

**Seam consumers stayed in scheduler.py** (tests patch them on the scheduler
module object): `_push_queue_with_durability`, `_DeferredReplacementAckGroup`,
`_BackendDownloadFailureErrback`, `BackendScheduler`. scheduler.py re-exports
every moved name via redundant aliases.

## Seam rules (load-bearing for future rounds)

1. **Patch target = the consuming submodule.** After a split, patching the
   package facade rebinds only the facade attribute; the consumer's own
   from-import binding is untouched. 72 patch sites were retargeted:
   `_wait_for_retry_backoff`/`compute_full_jitter_backoff`/`json`/
   `CircuitBreaker`/`_is_safe_manager_configuration_message` → `_manager`;
   `_load_object`/`_load_descriptor_object`/`_ack_token_key` →
   `_plugin_contract`; `get_descriptor` → `_config` or `_manager` **per driven
   path**. `get_descriptor` is dual-consumer — a test exercising both
   `resolve_backend_config` and `_create_backend` must patch BOTH submodules
   (see `test_plugin_retry_fields_remain_backend_owned_and_manager_aliases_win`).
2. **Logger object identity via the historical name.** `_manager.py` uses
   `logging.getLogger("scrapy_extension.backends.connectors")` (explicit string,
   not `__name__`), and the facade re-exports `logger`. `getLogger` returns a
   process-wide singleton, so the 9 object-level `connectors.logger.*` patches
   and caplog-by-name assertions survive unchanged.
3. **Moved schedule modules must not log.** 9 caplog/logger sites pin the
   `scrapy_extension.schedule.scheduler` logger name; a log call in a moved
   helper would silently vanish from those captures. Each module carries a
   docstring note pinning the historical name if logging is ever needed.
4. **mypy `--strict` (no-implicit-reexport)**: facade re-exports use redundant
   aliases (`from ._manager import _load_object as _load_object`); public names
   go in `__all__`; seam names imported from elsewhere (`json`,
   `CircuitBreaker`, `get_descriptor`, `compute_full_jitter_backoff`) are
   re-exported from their ORIGINAL sources, not through `_manager`.

## Verification

- Gate ×3 (after each phase + after old-file removal): `ruff check` +
  `ruff format --check src tests conftest.py` + `mypy --strict src` +
  `pytest` — **7038 passed / 55 skipped every time, identical to baseline**.
- Pure-move audit: every original top-level symbol's `ast.get_source_segment`
  text is byte-identical in its target module — 82/82 (connectors) and 25/25
  (scheduler). Only intentional textual change: the `_manager` logger's
  `getLogger` argument (documented above).
- Import smoke: all 36 externally-referenced names resolve on the package
  facade; `scrapy_extension.ConnectionManager/BackendScheduler/BackendQueue`
  unchanged; git detected `connectors.py → connectors/_manager.py` as a 71%
  rename.

## Follow-ups / notes

- `queue/queue.py` stays whole by decision (see Why).
- The repo-local untracked `.claude/CLAUDE.md` architecture tree (user's
  working copy, not committed) still describes `connectors.py` as a single
  file — update opportunistically.
- R138 F4/F5/F6 backlog remains queued and untouched (not mixed into a
  structural diff).
