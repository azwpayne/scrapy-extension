# SPEC-round59 — dupefilter open() cleanup leaks primary exception context (6b28166 sibling)

## Context and audit evidence

Found via the R65 deep-insight scan (ndiff dim), confirmed REAL by an opus
adversarial verifier (high confidence), and **independently re-verified by hand**
against the current tree before implementation.

Commit `6b28166` ("fix: isolate lifecycle cleanup exception contexts") established
an invariant — when a component's `open`/`setup` fails, the rollback cleanup and
its diagnostic log must run with **no active exception context**, so logging
handlers and monitor callbacks cannot observe the primary failure's
`exc_info`. It applied a mechanical transform to the instance lifecycle methods
of three components: `pipeline.open_spider`, `schedule/scheduler.py` `open()`,
and `spider/spider_mixin.py` `setup_backend` — plus a sibling test
(`tests/test_components.py:882`, `test_open_rollback_clears_primary_context_before_cleanup_and_logging`).

`git show --stat 6b28166` confirms `dupefilter/dupefilter.py` was **not** in the
touched-files list. The instance lifecycle analog — `BackendDupeFilter.open()`
(`src/scrapy_extension/dupefilter/dupefilter.py:713-731`) — still uses the
**pre-6b28166** pattern:

```python
            try:
                if spider is not None:
                    _validate_key_name(spider.name, field_name="spider.name")
                    self._resolve_spider_key(spider)
                self._clear_retry_allowances()
                self._filter.open()
                if self.clear_on_open:
                    self.clear()
            except BaseException:                     # line 721
                try:
                    self._close_locked()               # cleanup INSIDE the except
                except BaseException:
                    try:
                        logger.exception(              # logger.exception attaches active exc_info
                            "Failed to clean up dupefilter after open failure"
                        )
                    except BaseException:
                        pass
                raise
```

**The bug (the 6b28166 invariant is violated):** because `self._close_locked()`
runs *inside* the `except BaseException:` block, `sys.exc_info()` during cleanup
returns the primary failure (not `(None, None, None)`). And `logger.exception()`
populates `record.exc_info` from the active exception, so any handler on the
`scrapy_extension.dupefilter.dupefilter` logger observes the primary failure's
`exc_info` by default. This is exactly the leak `6b28166`'s test
(`cleanup_contexts == [(None,None,None)]`, `records[0].exc_info is None`)
prohibits for the scheduler/pipeline/spider_mixin.

**Severity: low** (defensive-hardening consistency gap, not data loss). The
default `ScrapyStatsMonitor.on_disconnect` does not call `sys.exc_info()`, so the
monitor-callback path is opt-in; but the `logger.exception()` exc_info leak is
reachable by default via any logging handler. This is the same threat model
`6b28166` used to justify its transform.

**Scope:** the instance-lifecycle `open()` site only — the direct analog of
`scheduler.open` (`schedule/scheduler.py:1447-1516`), which `6b28166` transformed.
The two factory classmethods (`from_settings`, `from_crawler`) share the bug
*shape* but are a **different structural pattern** (classmethod factories with a
nested `try` + `return cls(...)` inside the try); `6b28166` transformed **no**
factory classmethod anywhere in the codebase, so they are out of scope for this
SPEC (noted as a separate follow-up, not forced here).

## Goal

Bring `BackendDupeFilter.open()`'s failure-rollback into compliance with the
`6b28166` invariant: capture the primary failure, run `_close_locked()` and the
cleanup-failed diagnostic in a context-free block, and re-raise the original —
mirroring `scheduler.open` exactly.

## Specification

In `BackendDupeFilter.open()` (`src/scrapy_extension/dupefilter/dupefilter.py`,
the `try:` at line 713 / `except` at 721), apply the `6b28166` transform. The
block becomes (matching `scheduler.py:1447-1516`):

```python
            open_failure: BaseException | None = None
            try:
                if spider is not None:
                    _validate_key_name(spider.name, field_name="spider.name")
                    self._resolve_spider_key(spider)
                self._clear_retry_allowances()
                self._filter.open()
                if self.clear_on_open:
                    self.clear()
            except BaseException as exc:
                open_failure = exc
            if open_failure is not None:
                cleanup_failed = False
                try:
                    self._close_locked()
                except BaseException:
                    cleanup_failed = True
                if cleanup_failed:
                    try:
                        logger.error("Failed to clean up dupefilter after open failure")
                    except BaseException:
                        pass
                raise open_failure
            self._opened = True
            self._opened_spider = spider
```

Key properties (identical to `scheduler.open`): (1) `open_failure` declared
before the `try`; (2) `except ... as exc: open_failure = exc` captures without
re-raising inside the handler; (3) cleanup runs in `if open_failure is not None:`
**outside** the except, so `sys.exc_info()` is `(None, None, None)` during
`_close_locked()`; (4) diagnostic uses `logger.error` (no `exc_info` attached);
(5) `raise open_failure` re-raises the original primary. On the success path the
block falls through to `self._opened = True` exactly as before. No public-API
change; the existing `test_open_failure_closes_filter_and_manager` (which asserts
the primary is re-raised AND `_close_locked` runs) continues to pass.

## Plan and independently verifiable tasks

- **R59-1 — RED test.** Add
  `test_open_rollback_clears_primary_context_before_cleanup_and_logging` to
  `TestBackendDupeFilterOpenClose` (`tests/test_dupefilter.py`), mirroring
  `test_components.py:882`: monkeypatch `membership_filter.open` to raise
  `KeyboardInterrupt`, monkeypatch `_close_locked` to record `sys.exc_info()`
  and raise (to trip the cleanup-failed log), attach a logging `Handler` that
  asserts `sys.exc_info() == (None,None,None)` on emit. Assert: primary
  `KeyboardInterrupt` re-raised; `cleanup_contexts == [(None,None,None)]`; one
  log record with `exc_info is None` and `exc_text is None`. Add `import sys` to
  the test module. → verify: FAILS on current code (`cleanup_contexts` holds the
  active `KeyboardInterrupt`; `record.exc_info` is the `KeyboardInterrupt`).
- **R59-2 — GREEN fix.** Apply the `6b28166` transform above at
  `dupefilter.py:713-733`. → verify: the R59-1 test now PASSES.
- **R59-3 — no-regression.** Confirm the existing
  `test_open_failure_closes_filter_and_manager` and the rest of
  `TestBackendDupeFilterOpenClose` still pass (primary still re-raised; cleanup
  still runs). → verify: full dupefilter test file green; `ruff check` +
  `ruff format --check` + `mypy --strict` green.

## Acceptance criteria

1. `BackendDupeFilter.open()` rollback: `_close_locked` observes
   `sys.exc_info() == (None,None,None)`; the cleanup-failed log record has
   `exc_info is None`.
2. The primary failure is still re-raised verbatim (existing
   `test_open_failure_closes_filter_and_manager` passes).
3. Gate green: `uv run ruff check .` + `uv run ruff format --check src tests
   conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension`.
4. One atomic commit, ff-merged to `main`; CI green.
