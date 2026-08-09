# SPEC-round63 — close_spider skips teardown when _resolve_spider raises

## Context and audit evidence

Found via the R71 deep-insight scan (dim `pipeline`), confirmed REAL by an
opus adversarial verifier (low severity, medium confidence), and
**independently re-verified by hand** against the current tree.

`BackendPipeline.close_spider` (`src/scrapy_extension/pipeline/pipeline.py:513`)
resolves the spider via `_resolve_spider(spider)` at **line 526 — unguarded —
before** invoking `_close_locked()` at line 534. The resolved spider is used
*only* to format the diagnostic log on line 528:

```python
        with self._lifecycle_lock:
            if self._closed:
                return
            spider = self._resolve_spider(spider)          # 526  ← unguarded; can raise
            try:
                logger.info("Pipeline closed for spider %s", spider.name)
            except BaseException:                           # 529  ← the LOG is guarded...
                # This is diagnostic-only and runs before _close_locked() establishes
                # the resource-release invariant.  A logging handler must therefore
                # not be able to skip strategy draining or manager teardown.
                pass
            self._close_locked()                            # 534  ← ...but _resolve_spider is not
```

`_resolve_spider` (`pipeline.py:436`) raises `RuntimeError("BackendPipeline
has no spider; ...")` when `spider is None` AND `_opened_spider is None` AND
`_crawler is None` (or `crawler.spider is None`) — reachable for a pipeline
built via `from_settings()` (no crawler) whose `open_spider` was never called
or failed before setting `_opened_spider`, then `close_spider()` invoked
argument-less. That `RuntimeError` exits the `with self._lifecycle_lock:`
block, so `_close_locked()` (line 534) is **never reached**: the storage
strategy is not closed and `connection_manager.close()` is not called
(`_manager_released` stays `False`) — a ConnectionManager registry leak until
LRU eviction.

The file's **own** teardown invariant (comment lines 530-532 — "A logging
handler must therefore not be able to skip strategy draining or manager
teardown") is directly violated: the unguarded `_resolve_spider` feeding that
same log line can skip teardown. The log *handler* is guarded; the resolution
that feeds it is not — an asymmetry.

**Severity: low.** No data corruption; the leak is a registry entry (the
manager is typically not yet connected in this path, so no socket leak).
Reachability is narrow (standard `from_crawler` always sets `_crawler`, so
`_resolve_spider` succeeds in production) — but the invariant violation is
concrete and the fix is minimal.

## Goal

Guarantee `_close_locked()` runs regardless of spider resolution, mirroring
the existing log-handler guard — so a resolution failure cannot skip strategy
draining or manager teardown.

## Specification

Wrap `_resolve_spider` in a `try/except` (mirroring the existing log-handler
guard at 527-533). On resolution failure, set `spider = None`, skip the
diagnostic log, and proceed to `_close_locked()`:

```python
        with self._lifecycle_lock:
            if self._closed:
                return
            try:
                spider = self._resolve_spider(spider)
            except Exception:
                # _resolve_spider raises when there is no opened spider and no
                # crawler (direct/from_settings use, or after an open_spider
                # failure). Teardown must still run -- _close_locked() releases
                # the storage strategy and the connection manager. The spider
                # name only feeds the diagnostic log below; mirror the
                # log-handler guard so a resolution failure cannot skip it.
                spider = None
            try:
                if spider is not None:
                    logger.info("Pipeline closed for spider %s", spider.name)
            except BaseException:
                # This is diagnostic-only and runs before _close_locked() establishes
                # the resource-release invariant.  A logging handler must therefore
                # not be able to skip strategy draining or manager teardown.
                pass
            self._close_locked()
```

`except Exception` (not `BaseException`): `_resolve_spider` is controlled code
that raises only `RuntimeError`; `KeyboardInterrupt`/`SystemExit` should still
propagate (the existing `_close_locked` already handles interrupts via its
`primary_error` pattern). No public-API change.

## Plan and independently verifiable tasks

- **R63-1 — RED test.** Add `test_close_spider_runs_teardown_when_spider_resolution_fails`
  to `TestBackendPipelineCloseSpider` (`tests/test_pipeline.py`), mirroring
  `test_close_spider_releases_resources_when_close_log_interrupts` (line 602)
  but with NO spider and NO crawler: construct
  `BackendPipeline(connection_manager=mock_connection_manager)`, call
  `close_spider()` argument-less (so `_resolve_spider` raises), then assert
  `pipeline._closed is True`, `mock_connection_manager.close.assert_called_once_with()`,
  and `pipeline._manager_released is True`. → verify: FAILS on current code
  (`_resolve_spider`'s `RuntimeError` propagates out of `close_spider` before
  `_close_locked`).
- **R63-2 — GREEN fix.** Wrap `_resolve_spider` in `try/except Exception` →
  `spider = None`, guard the log with `if spider is not None:`. → verify: the
  R63-1 test PASSES.
- **R63-3 — no-regression.** The existing close_spider tests
  (`test_close_spider_logs_message`, `test_close_spider_calls_connection_manager_close`,
  `test_close_spider_releases_resources_when_close_log_interrupts`, etc.) still
  pass — they pass a spider or use `from_crawler`. Full `test_pipeline.py` green;
  `ruff check` + `mypy --strict` green.

## Acceptance criteria

1. `close_spider()` runs `_close_locked()` (releases the connection manager,
   sets `_closed`) even when `_resolve_spider` raises.
2. Existing close_spider behavior (with a spider / via from_crawler) is
   unchanged (existing tests green).
3. Gate green: `uv run ruff check .` + `uv run pytest` + `uv run mypy --strict
   src/scrapy_extension`.
4. One atomic commit, ff-merged to `main`; CI green.
