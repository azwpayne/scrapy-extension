# R35 — PLAN: BackendScheduler._close_locked state-reset tail fix

> Back-navigation: [SPEC](./R35-scheduler-tail-basex-SPEC.md) ·Driven by
> durable cron `038706c4`. Base: `main` @ `f8553f0` (post-R34 `14bac6c`).

## Branch

`R35-scheduler-tail-basex` (off `R35-insight-round`, off `main`).

## Changes (minimal)

### File: `src/scrapy_extension/schedule/scheduler.py`

**Single block edit in `_close_locked` (lines 1476-1541).** Move the
six-line state-reset tail from inside the `try` (1510-1515) to the
`finally` block (currently 1521-1539), BEFORE the manager-release block.

**Before (1476-1541):**

```python
primary_error: BaseException | None = None
try:
  if self._connected_signals is not None:
    # ... signal disconnect loop with per-handler except Exception
  if self._queue is not None:
    try:
      self._queue.close()
    except Exception:
      logger.exception(...)
  if (
    self._owns_dupefilter
    and self.dupefilter is not None
    and not self._dupefilter_released
  ):
    self._dupefilter_released = True
    try:
      self.dupefilter.close(reason)
    except Exception:
      logger.exception(...)
    finally:
      self._dupefilter_open = False
  self._queue = None
  self._spider = None
  self._connected_signals = None
  self._signals_connected = False
  self._backpressure_paused = False
  self._backpressure_probe_due = False
except BaseException as exc:
  primary_error = exc
finally:
  if self._owns_connection_manager and not self._manager_released:
    self._manager_released = True
    try:
      self.connection_manager.close()
    except BaseException as exc:
      if primary_error is None:
        primary_error = exc
      else:
        try:
          logger.exception(...)
        except BaseException:
          pass
if primary_error is not None:
  raise primary_error
return None
```

**After:**

```python
primary_error: BaseException | None = None
try:
  if self._connected_signals is not None:
    # ... signal disconnect loop with per-handler except Exception
  if self._queue is not None:
    try:
      self._queue.close()
    except Exception:
      logger.exception(...)
  if (
    self._owns_dupefilter
    and self.dupefilter is not None
    and not self._dupefilter_released
  ):
    self._dupefilter_released = True
    try:
      self.dupefilter.close(reason)
    except Exception:
      logger.exception(...)
    finally:
      self._dupefilter_open = False
except BaseException as exc:
  primary_error = exc
finally:
  # R35-F7: state-reset tail moved here so it runs even if BaseException
  # aborts teardown mid-try. Idempotent: assigning None/False on already-None
  # is a no-op; a re-entrant close() short-circuits at the lifecycle_state
  # guard before reaching here, so no double-reset risk.
  self._queue = None
  self._spider = None
  self._connected_signals = None
  self._signals_connected = False
  self._backpressure_paused = False
  self._backpressure_probe_due = False
  # R26-G manager release (unchanged shape).
  if self._owns_connection_manager and not self._manager_released:
    self._manager_released = True
    try:
      self.connection_manager.close()
    except BaseException as exc:
      if primary_error is None:
        primary_error = exc
      else:
        try:
          logger.exception(...)
        except BaseException:
          pass
if primary_error is not None:
  raise primary_error
return None
```

### Net diff

- **Add:** 7-line state-reset tail inside the existing `finally` (idempotent
  assignment of `_queue`, `_spider`, `_connected_signals`, `_signals_connected`,
  `_backpressure_paused`, `_backpressure_probe_due`).
- **Remove:** the same 6-line tail from inside the `try`.
- **No change** to: signal disconnect loop, queue close, dupefilter close,
  per-handler except blocks, outer `except BaseException`, manager release,
  primary error re-raise.

## Test changes

### File: `tests/test_scheduler_close_basex_tail.py` (new)

```python
"""Regression tests for R35-F7: BackendScheduler._close_locked state-reset
tail survives BaseException teardown.

Pre-fix: the six state-reset lines (`_queue = None`, `_spider = None`,
`_connected_signals = None`, `_signals_connected = False`,
`_backpressure_paused = False`, `_backpressure_probe_due = False`) live
INSIDE the guarded try-block and are skipped when a BaseException aborts
teardown mid-try. Post-fix: tail moves into the finally block and runs on
every code path.
"""

import pytest
from scrapy_extension.schedule.scheduler import BackendScheduler, _LIFECYCLE_CLOSED


def _build_scheduler_with_custom_owned_dupefilter(monkeypatch, close_side_effect):
    """Build a scheduler whose owned dupefilter.close raises the given exception."""
    # ... (helpers copied/adapted from existing from_settings test fixtures)
    pass


def test_scheduler_close_resets_state_tail_after_dupefilter_keyboardinterrupt(monkeypatch):
    """F7-A: KeyboardInterrupt from owned dupefilter.close must still reset
    state-reset tail and re-raise."""
    scheduler, cm = _build_scheduler_with_custom_owned_dupefilter(
        monkeypatch, lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt())
    )
    with pytest.raises(KeyboardInterrupt):
        scheduler.close("test")
    assert scheduler._lifecycle_state == _LIFECYCLE_CLOSED
    assert scheduler._queue is None
    assert scheduler._spider is None
    assert scheduler._connected_signals is None
    assert scheduler._signals_connected is False
    assert scheduler._backpressure_paused is False
    assert scheduler._backpressure_probe_due is False
    assert cm.close.call_count == 1


def test_scheduler_close_resets_state_tail_after_signal_disconnect_baseexception(monkeypatch):
    """F7-B: BaseException from signal disconnect loop must still reset tail."""
    # Variant: monkeypatch scheduler._connected_signals with a fake whose
    # disconnect() raises BaseException on the second handler (so the first
    # was already disconnected before the BaseException fires).
    pass


def test_scheduler_close_resets_state_tail_after_queue_close_baseexception(monkeypatch):
    """F7-C: BaseException from queue.close() override must still reset tail."""
    # Variant: monkeypatch self._queue with a fake whose close() raises.
    pass


def test_scheduler_close_normal_path_unaffected():
    """Sanity: when no exception fires, behavior is identical to pre-fix.
    The existing R26-G regression test must continue to pass."""
    # ... assert queue closed, manager released, no exceptions
    pass


def test_scheduler_close_reentrant_short_circuit():
    """A second close() after a successful close() short-circuits at
    `_lifecycle_state == _LIFECYCLE_CLOSED` (1464) without re-running
    the tail. Pre- and post-fix behavior must match."""
    pass
```

## Validation gate

In order:

1. `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run ruff check .`
2. `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run mypy --strict src/`
3. `UV_CACHE_DIR=$CLAUDE_JOB_DIR/tmp/uv-cache-r35 uv run pytest -q`

Target pass count: ≥ R34's 3833 + 5 new tests = 3838.

## DO-NOT-RE-FLAG after R35

- `BackendScheduler._close_locked` state-reset tail runs in the
  `finally` block (this fix).
- Do NOT re-flag the dupefilter `_close_locked` retry design (R33).
- Do NOT re-flag R26-G manager release.
- Do NOT re-flag R34 scheduler consume_reservation ghost window.

## Risk register

- **Behavior delta:** none observable in the success path (idempotent
  assignments; tail runs in `finally` either way). All current tests
  must pass.
- **Test fidelity:** the new regression tests use monkeypatched fakes
  for the BaseException source (dupefilter / signal / queue). The
  actual production wiring (R26-G, R34 consume_reservation) keeps
  existing behavior; only the BaseException arm gains a side effect
  (state-reset tail runs).
- **Coverage:** the new tests target `scheduler.py` directly. No
  ancillary tests (queue, dupefilter, spider mixin) require updates.

## Gate / Merge / Record

ruff → mypy --strict → pytest (R34 3833 + 5 new = 3838 expected).
ff-merge → push → delete branch → memory record.

DO NOT delete branches outside this round. DO NOT touch LEDGER entries
without recording the round number.