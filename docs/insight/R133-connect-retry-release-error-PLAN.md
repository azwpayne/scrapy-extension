# R133 PLAN — connect-retry release-error preservation

> Spec: [R133-connect-retry-release-error-SPEC.md](R133-connect-retry-release-error-SPEC.md)

## Phase 1 — TDD RED (Finding A)

In `tests/test_connection_manager.py`, mirror the existing
`test_attempt_connection_close_wins_preserves_discard_error` (threading
Events, ~line 400) but drive the RETRY LOOP instead of `_attempt_connection`
directly:

`test_connect_with_retries_preserves_release_error_when_close_wins`:
- Wire a manager whose backend `connect()` blocks on an Event; a second
  thread calls `manager.close()` (setting `_retired`) while the attempt is
  in flight; release the Event.
- Call `manager._connect_with_retries([])` (or the public connect path if
  the helper is awkward) and capture the outcome.
- Assert: the raised error message contains the release reason
  ("backend discarded" or "Cannot connect a released ConnectionManager") AND
  does NOT contain "Failed to connect after".
- Current code: raises the generic count message → RED.

## Phase 2 — Implement (GREEN)

`src/scrapy_extension/backends/connectors.py`, `_connect_with_retries`
retry loop:

- Inside `except Exception:` (after `failed_attempt = True;
  attempt_failed = True`), add:

```python
                with self._lock:
                    retired = self._retired
                if retired:
                    # A concurrent close() won the race and detached this
                    # manager mid-attempt; its typed release error is the
                    # actionable reason. Re-raise it instead of falling
                    # through to the generic attempt-count failure.
                    raise
```

- Remove the post-except retired check (`with self._lock: retired =
  self._retired` / `if retired: break` at ~:2071-2074).
- Keep the "Connection attempt failed." diagnostic, `on_retry` event, and
  backoff sleep exactly as they are for ordinary failures. A bare `raise`
  inside the except suite needs no survivor variable (the `as` target is
  cleared at suite exit — the R71-verified `UnboundLocalError` trap).
- The `if failed_attempt:` generic tail stays for ordinary exhaustion.

## Phase 3 — Gate (plain commands; pytest unsandboxed if uv cache blocked)

```bash
uv run --frozen ruff check src tests conftest.py
uv run --frozen ruff format --check src tests conftest.py
uv run --frozen pytest tests/test_connection_manager.py -q   # focused first
uv run --frozen pytest
uv run --frozen mypy --strict src
```

## Phase 4 — Ship

1. Commit: `fix(backends): preserve the release error when close wins a connect retry`
   (connectors.py + test).
2. LEDGER rows: R133-A LANDED; R66 closure REFUTED (safety-net reasoning).
3. Round docs trio committed; push `HEAD:main`.
4. Scan results (parallel workflow over the landed WIP) ship this round if
   confirmed, else queue; memory round entry + MEMORY.md.
