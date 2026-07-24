# TASK — Round 20: ES health-probe ApiError leak + pipeline close BaseException-swallow + on_pop_rate docstring

> Spec: [SPEC-round20-healthprobe-close.md](SPEC-round20-healthprobe-close.md)
> Plan:  [PLAN-round20-healthprobe-close.md](PLAN-round20-healthprobe-close.md)
> Constraints: one atomic commit per unit · all → main · Claude-only.

## Unit A — ES is_connected/ping broaden to (ApiError, TransportError) (LOW, probe)
**File:** `src/scrapy_extension/backends/elasticsearch.py:189`; test `tests/test_elasticsearch_backend_coverage.py`. `ApiError` imported at :16.
1. [RED] Test: a connected backend whose `self._client.ping()` raises a non-TransportError `ApiError`
   (use the existing `_make_api_error()` helper from R19-A) → `is_connected()` returns `False` (not
   raises). Run → RED (currently raises raw ApiError).
2. [GREEN] Change line 189 `except TransportError:` → `except (ApiError, TransportError):` (keep
   `return False`). `ping()` delegates to `is_connected()` so both covered.
3. Run gate; commit `fix(elasticsearch): broaden is_connected/ping to (ApiError, TransportError) so an API-layer rejection returns False, not raw (R20-A, R19-A health-probe analog)`.

## Unit B — pipeline _close_locked re-raises BaseException when no primary error (LOW, swallow)
**File:** `src/scrapy_extension/pipeline/pipeline.py:411-432`; test `tests/test_pipeline.py` (or sibling). Ref `dupefilter.py:730-768`.
1. [RED] Test: a pipeline whose `storage_strategy.close()` succeeds but `connection_manager.close()`
   raises `KeyboardInterrupt` → `_close_locked()` (or `close()`) re-raises `KeyboardInterrupt` (not
   swallows). Run → RED (currently swallows + returns normally).
2. [GREEN] Mirror the dupefilter `primary_error` pattern:
   ```python
   primary_error: BaseException | None = None
   try:
     self.storage_strategy.close()
   except BaseException as exc:
     primary_error = exc
   finally:
     if not self._manager_released:
       self._manager_released = True
       try:
         self.connection_manager.close()
       except BaseException as exc:
         if primary_error is None:
           primary_error = exc
         else:
           try:
             logger.exception("connection_manager.close() failed during teardown")
           except BaseException:
             pass
   if primary_error is not None:
     raise primary_error
   ```
   Keep the `_manager_released = True` placement (before the manager try) to preserve release-semantics.
3. Run gate; commit `fix(pipeline): re-raise BaseException during _close_locked when there is no primary error (R20-B, R-swallow sibling)`.

## Unit C — on_pop_rate docstring dynamic tag (LOW, docs)
**File:** `src/scrapy_extension/monitor/stats.py:216-218`. Ref emitter stats.py:226 + runbook:585.
1. Rewrite the prose: "The stat key is window-tagged: ``queue/pop_rate_1m`` at the default 60s window,
   or ``queue/pop_rate_{N}s`` when ``SCRAPY_MONITOR_POP_RATE_WINDOW_S`` is overridden. ``BackendQueue``
   forwards its configured ``pop_rate_window_s`` here, so the key always reflects the window an operator
   is looking at." (Keep the ``rate`` sentence + Args section as-is.)
2. Commit `docs(monitor): on_pop_rate docstring — dynamic window tag, not 'fixed at 1m' (R20-C)`.

## Definition of done
- [ ] ruff clean · mypy --strict 0 issues · pytest ≥3767 passed (unsandboxed) · coverage ≥95%
- [ ] 3 atomic commits on `worktree-round20-healthprobe-close`
- [ ] ff-merged to `main`, pushed, worktree branch deleted
- [ ] memory updated (R20 close-out note in `deep-insight-2026-07-23-ultracode.md`)
