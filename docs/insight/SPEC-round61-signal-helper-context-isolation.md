# SPEC-round61 — signal-registration helper rollbacks leak primary exc_info (6b28166 sibling cluster)

> **Scope this fire:** implements **Site 1 only** (`spider_mixin._connect_signals`,
> low/high). **Site 2** (`scheduler._connect_ack_signals`, low/medium) is **deferred**
> — its adversarial verifier flagged it as "narrow observable effect... a reviewer
> could reasonably dismiss it as not worth a ship on its own" (default
> `SignalManager.disconnect` does not inspect `sys.exc_info()`); forcing it would be
> shipping-to-ship. It remains a queued candidate. Both sites are documented below
> for context; only Site 1 is implemented here.

## Context and audit evidence

Found via the R68 deep-insight scan (ndiff dim), both confirmed REAL by opus
adversarial verifiers, and **independently re-verified by hand** against the
current tree. This is a consolidation of two structurally-identical
**6b28166 sibling misses** — the same invariant commit `6b28166` ("isolate
lifecycle cleanup exception contexts") established for `scheduler.open`,
`pipeline.open_spider`, `spider_mixin.setup_backend` (and R67/`1a229d0` extended
to `BackendDupeFilter.open`). 6b28166 transformed each component's OUTER
open/setup rollback but missed the **nested signal-registration helper** each one
calls — both of which have their own inline-except rollback.

**Site 1 — `BackendSpiderMixin._connect_signals`** (`src/scrapy_extension/spider/spider_mixin.py:374-392`):
```python
        try:
            for handler, signal in handlers:
                signal_manager.connect(handler, signal)
                connected.append((handler, signal))
        except BaseException:                       # line 378 — caught the registration failure
            try:
                self._disconnect_lifecycle_signals(  # rollback INSIDE the except
                    signal_manager,
                    handlers=tuple(reversed(connected)),
                )
            except BaseException:
                try:
                    logger.exception("Failed to roll back backend lifecycle signals")  # attaches active exc_info
                except BaseException:
                    pass
            raise
```

**Site 2 — `BackendScheduler._connect_ack_signals`** (`src/scrapy_extension/schedule/scheduler.py:1559-1594`):
```python
        try:
            for handler, signal in signal_handlers:
                ...
                sig.connect(handler, signal=signal)
        except BaseException:                       # line 1573 — caught the registration failure
            for handler, signal in reversed(tuple(connected)):
                try:
                    sig.disconnect(handler, signal=signal)   # rollback INSIDE the except
                except BaseException:
                    try:
                        logger.exception("Failed to roll back %s after signal registration failure", signal)
                    except BaseException:
                        pass
                else:
                    connected.remove((handler, signal))
            ...
            raise
```

**The bug (6b28166 invariant violated at both sites):** the rollback
(`_disconnect_lifecycle_signals` / `sig.disconnect`) runs INSIDE the
`except BaseException:` that caught the registration failure, so
`sys.exc_info()` during rollback returns the primary registration error. The
`logger.exception(...)` calls attach that active `exc_info` to the log record.
For site 1 the leak is concretely observable inside `_disconnect_lifecycle_signals`
(spider_mixin.py:~427, whose own logging emits while the registration failure is
still active). This is exactly the leak 6b28166's test
(`test_components.py:882`) prohibits: `cleanup_contexts == [(None,None,None)]`,
`records[0].exc_info is None`.

**Severity: low.** The default `scrapy.signalmanager.SignalManager.disconnect()`
does not inspect `sys.exc_info()`, so there is no observable effect on the
default path; the leak matters only for a third-party signal manager whose
`disconnect`/`connect` observes exc_info, or for logging handlers reading
`record.exc_info`. This is invariant-consistency hardening (completing the
6b28166 cluster), not a crash/data fix.

## Goal

Bring both signal-registration helpers' failure rollbacks into compliance with
the 6b28166 invariant: capture the primary registration failure, run the
disconnect rollback + its diagnostic in a context-free block, use `logger.error`
(no `exc_info`), and re-raise the original — mirroring `scheduler.open`
(`schedule/scheduler.py:1503-1516`) and `BackendDupeFilter.open` (R67).

## Specification

Apply the 6b28166 transform to BOTH sites. For each: declare
`registration_failure: BaseException | None = None` before the `try`; change
`except BaseException:` to `except BaseException as exc: registration_failure = exc`;
move the rollback block under `if registration_failure is not None:` (OUTSIDE the
except, so `sys.exc_info()` is `(None, None, None)` during the disconnect calls);
change `logger.exception(...)` → `logger.error(...)`; change the trailing bare
`raise` → `raise registration_failure`. Existing re-raise semantics (the primary
registration failure propagates) are preserved exactly.

Site 1 (`spider_mixin.py`) becomes:
```python
        registration_failure: BaseException | None = None
        try:
            for handler, signal in handlers:
                signal_manager.connect(handler, signal)
                connected.append((handler, signal))
        except BaseException as exc:
            registration_failure = exc
        if registration_failure is not None:
            cleanup_failed = False
            try:
                self._disconnect_lifecycle_signals(
                    signal_manager,
                    handlers=tuple(reversed(connected)),
                )
            except BaseException:
                cleanup_failed = True
            if cleanup_failed:
                try:
                    logger.error("Failed to roll back backend lifecycle signals")
                except BaseException:
                    pass
            raise registration_failure
        self._connected_signals = signal_manager
        self._signals_connected = True
```

Site 2 (`scheduler.py`) applies the same shape to its loop + post-loop state
clear (move the `for ... sig.disconnect` loop and the `if not connected:` state
reset under `if registration_failure is not None:`; `logger.exception` →
`logger.error`; `raise` → `raise registration_failure`).

No public-API change; the primary registration failure is still the propagated
exception at both sites.

## Plan and independently verifiable tasks

- **R61-1 — RED tests (both sites).** Add a test per site mirroring
  `test_open_rollback_clears_primary_context_before_cleanup_and_logging`
  (`test_components.py:882`, and R67's dupefilter variant): monkeypatch
  `signal_manager.connect` (site 1) / `sig.connect` (site 2) to raise
  `KeyboardInterrupt` mid-loop; monkeypatch the disconnect path to record
  `sys.exc_info()` and raise (to trip the cleanup-failed log); attach a logging
  `Handler` asserting `sys.exc_info() == (None,None,None)` on emit. Assert:
  primary re-raised; cleanup saw `(None,None,None)`; cleanup-failed log record
  `exc_info is None`. **Save+restore the logger level in `finally`** (R67 lesson
  — `test_components:882` leaks `setLevel(ERROR)`). → verify: both FAIL on
  current code (cleanup sees the active registration failure; record.exc_info set).
- **R61-2 — GREEN fix.** Apply the 6b28166 transform to both sites. → verify:
  both R61-1 tests PASS; existing signal tests still pass.
- **R61-3 — no-regression.** Full spider_mixin + components/scheduler test files
  green; `ruff check` + `ruff format --check` + `mypy --strict` green.

## Acceptance criteria

1. Both helpers: during the disconnect rollback `sys.exc_info() ==
   (None,None,None)`; the cleanup-failed log record has `exc_info is None`.
2. The primary registration failure is still the propagated exception at both
   sites (existing signal-failure tests unchanged).
3. Gate green: `uv run ruff check .` + `uv run ruff format --check src tests
   conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension`.
4. One atomic commit, ff-merged to `main`; CI green.
