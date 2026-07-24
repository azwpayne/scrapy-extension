# PLAN — Round 20: ES health-probe ApiError leak + pipeline close BaseException-swallow + on_pop_rate docstring

> Spec: [SPEC-round20-healthprobe-close.md](SPEC-round20-healthprobe-close.md)
> Workflow: worktree `round20-healthprobe-close` → execute A→C (atomic commits) → gate → ff-merge to `main` → delete branch.

## Design notes
- **A**: identical shape to R19-A but on the bool-returning probe. `ApiError` imported at :16. Change
  `except TransportError: return False` → `except (ApiError, TransportError): return False`. `ping()`
  delegates to `is_connected()` so both are covered by the one edit.
- **B**: mirror dupefilter.py:730-768. Track `primary_error: BaseException | None`; capture from
  `storage_strategy.close()`; in the manager arm, set `primary_error` if None (else log preserving the
  primary); re-raise `primary_error` at the end. Keep `self._manager_released = True` before the
  manager try (preserves current release-semantics — `_close_locked` is guarded by `if self._closed` so
  no re-entry). Wrap the secondary logger call in `try/except BaseException: pass` (dupefilter parity)
  so logging can't mask the primary.
- **C**: rewrite the `on_pop_rate` prose (stats.py:216-218) to describe the dynamic tag + the
  operator-configurable window, matching the runbook:585 wording R19-C landed.

## Phases
1. **TDD RED (A + B)** — see TASK.
2. **Implement (GREEN)** — A (broaden), B (primary_error pattern), C (docstring rewrite).
3. **Gate** — `uv run ruff check src/ tests/`; `uv run mypy --strict src/`; `uv run pytest -q` (sandbox OFF).
4. **Merge to main** — ff-only, push, `worktree remove --force` + `branch -d` (sandbox-off).

## Fan-out (Claude-only)
3 small, mechanical units → main-loop sequential. Ultracode 9-agent scan was the insight fan-out.

## Risk notes
- **B**: the test must simulate a BaseException from `connection_manager.close()` while
  `storage_strategy.close()` succeeds, and assert it propagates (not swallowed). Use a fake
  storage_strategy whose `close()` is a no-op + a connection_manager whose `close()` raises
  `KeyboardInterrupt`.
- **A**: confirm `is_connected()` returns `False` (not raises) when `ping()` raises a non-TransportError
  `ApiError`.
