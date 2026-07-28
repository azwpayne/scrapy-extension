# R35 — SPEC: BackendScheduler._close_locked state-reset tail skipped after BaseException

> Back-navigation: [../insight](./) ·Driven by durable cron `038706c4`.
> Scan: R35 ultracode workflow on main `f8553f0` (post-R34 `14bac6c`).
> Branch: `R35-insight-round` (isolated worktree, off main).

## Headline

**Candidate F7 (MED):** `BackendScheduler._close_locked`
(`src/scrapy_extension/schedule/scheduler.py:1462-1541`) is **partially**
BaseException-safe. R26-G captures `connection_manager.close()` via a
`finally`, but the **state-reset tail** (`_queue = None`, `_spider = None`,
`_connected_signals = None`, `_signals_connected = False`,
`_backpressure_paused = False`, `_backpressure_probe_due = False`) at
lines 1510-1515 lives INSIDE the guarded `try` block. A `BaseException`
(`KeyboardInterrupt` / `SystemExit`) escaping any of the three teardown
steps (signal disconnect at 1485, `self._queue.close()` at 1495, owned
`dupefilter.close()` at 1505) is captured at 1516 (`primary_error = exc`),
then the manager-release `finally` (1521) runs, and 1540-1541 re-raises.
**The state-reset tail is never executed.**

Net effect after a BaseException teardown:

1. `_lifecycle_state == _LIFECYCLE_CLOSED` (set at 1466 BEFORE the try).
2. `connection_manager` released exactly once.
3. **`_queue`** still references the (now closed) `BackendQueue`.
4. **`_spider`** still references the (now tearing-down) spider.
5. **`_connected_signals`** still references the SignalManager — any
   signal handlers that were already disconnected remain disconnected,
   but any handler that hadn't yet been iterated in the disconnect loop
   (1483-1489) is **still registered**. Subsequent `response_received`
   or `spider_error` events reach `_on_response_received` /
   `_on_spider_error`, which call `_ack_token(...)` against the
   (now stale) `_queue` and may try to use `_spider`.
6. `_signals_connected = True` (not reset).
7. `_backpressure_paused` / `_backpressure_probe_due` survive across
   the lifecycle mark, so the backpressure probe slot is dirtied.

A re-entrant `close()` short-circuits at 1464 (`_lifecycle_state ==
_LIFECYCLE_CLOSED`), so the stale references are not cleaned by retry
either. They live until garbage collection.

## Triggering state sequence (concrete)

1. Engine calls `scheduler.close("finished")` → `_close_locked("finished")`.
2. `_lifecycle_state` is set to `_LIFECYCLE_CLOSED` (1466) — closure
   flag flipped BEFORE teardown begins.
3. `_connected_signals.disconnect(handler, signal)` at 1485 raises
   `KeyboardInterrupt` (custom signal_manager subclass). Per-handler
   `except Exception` (1486) does not catch BaseException; the loop
   aborts. (Alternatively, `self._queue.close()` at 1495 raises via a
   custom queue override, or owned `dupefilter.close(reason)` at 1505
   raises via a custom filter override. All three are realistic.)
4. The outer `except BaseException as exc` at 1516 captures
   `primary_error = exc` and skips the 1510-1515 state-reset tail.
5. `finally` at 1521 runs `connection_manager.close()` (R26-G path).
6. 1540 `if primary_error is not None: raise primary_error` re-raises
   the BaseException to the caller.

## Why this is NOT R26-G (and not R33 refuted)

- **R26-G** captured `connection_manager.close()` via `finally`. The
  state-reset tail was already in place when R26-G shipped; R26-G did
  not move the tail into the `finally`. The two concerns are
  orthogonal: manager release must run; state-reset must run;
  BaseException-safe teardown needs both.
- **R33** refuted the dupefilter `_close_locked` retry design — that
  finding's BaseException residual was specifically accepted as a
  theoretical, low-impact cost of the intentional retry mechanism.
  This finding is on the **scheduler** (a different file/surface),
  and the residual here is operator-visible (stale refs + signal
  handlers) and out of scope of R33's retry design.

## Expected vs actual

- **Expected:** after `close(reason)` returns (or re-raises a captured
  BaseException), the scheduler's lifecycle is fully detached — no
  stale references to queue/spider/signal-manager, no surviving signal
  handlers, backpressure flags cleared.
- **Actual:** after a BaseException teardown, the manager is released
  but the scheduler retains stale references and possibly-registered
  signal handlers. The lifecycle flag says CLOSED so no retry can
  clean up.

## Affected callers

- `BackendSpiderMixin.close_backend` (spider_mixin.py:619-630) calls
  `scheduler.close("spider-mixin-close",)`. If a sibling `dupefilter.close()`
  or signal disconnect in the mixin's loop aborts, the scheduler
  BaseException-survivor tail leaks through. The mixin's own BaseException
  safety (F6) is the complementary half; the scheduler side is F7.
- `from_crawler` / `from_settings` engines that swallow close errors.
- Any code path that triggers a `KeyboardInterrupt` during teardown
  (operator Ctrl+C, `SystemExit` from signal-aware container shutdown).

## Minimal executable regression test

`tests/test_scheduler_close_basex_tail.py`:

1. **`test_scheduler_close_resets_state_tail_after_baseexception`**:
   Construct a `BackendScheduler` with `crawler` + `from_settings()`,
   plus a custom `dupefilter.close` that raises `KeyboardInterrupt`.
   Call `scheduler.close("test")` under `pytest.raises(KeyboardInterrupt)`,
   then assert:

   ```python
   assert scheduler._lifecycle_state == _LIFECYCLE_CLOSED
   assert scheduler._queue is None
   assert scheduler._spider is None
   assert scheduler._connected_signals is None
   assert scheduler._signals_connected is False
   assert scheduler._backpressure_paused is False
   assert scheduler._backpressure_probe_due is False
   # Manager released exactly once
   assert cm.close.call_count == 1
   ```

2. **`test_scheduler_close_resets_state_tail_after_signal_disconnect_baseexception`**:
   Same assertions, but inject a custom `signal_manager.disconnect`
   that raises `BaseException`. The signal disconnect loop must
   tolerate the BaseException (currently does not — note: signals
   already disconnected before the BaseException escape only if the
   loop had iterated past them; the fix must ensure any *remaining*
   handlers are disconnected on the BaseException path).

3. **`test_scheduler_close_resets_state_tail_after_queue_close_baseexception`**:
   Same assertions, but inject a custom queue whose `close()` raises
   `BaseException`.

## Fix shape (R26-G precedent)

Move the state-reset tail OUTSIDE the guarded `try` block, into a
`finally` BEFORE the manager release, mirroring the R26-G pattern:

```python
primary_error: BaseException | None = None
try:
  # ... existing signal disconnect + queue.close + dupefilter.close
  # ... existing per-handler `except Exception:` blocks stay as-is
except BaseException as exc:
  primary_error = exc
finally:
  # R35-F7: state-reset tail moved here so it runs even if
  # BaseException aborts teardown mid-try. The tail is idempotent
  # (assigning None / False is safe to repeat on a re-entered close).
  self._queue = None
  self._spider = None
  self._connected_signals = None
  self._signals_connected = False
  self._backpressure_paused = False
  self._backpressure_probe_due = False
  # R26-G manager release runs after the state tail so any caller
  # observing the manager state during teardown sees detached refs.
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

### Why this is safe

- **Success path unchanged:** when no exception fires, the tail runs
  once in the `finally`, identical semantics.
- **Exception path fixed:** BaseException aborting teardown now leaves
  the scheduler fully detached; only the manager release (and primary
  error capture) run after.
- **Idempotency:** `self._queue = None` on a `None` reference is a
  no-op; same for the boolean flags. A re-entrant `close()` (which
  short-circuits at 1464) never re-runs the tail, so there is no
  double-free risk.

## Verification gate

`uv run ruff check` → `uv run mypy --strict src/scrapy_extension` →
`uv run pytest -q` (target ≥ R34's 3833 pass + 3 new).

## Out of scope (deferred to R36, NOT in this round)

- **F3** (MED): `Monitor.on_disconnect(reason=)` always receives `None`.
- **F4** (LOW): `ConnectionManager.set_monitor()` race with dispatch.
- **F5** (LOW): User `Monitor.on_disconnect` raising `BaseException`.
- **F6** (MED): `BackendSpiderMixin.close_backend` BaseException
  windows — **note:** F6 is the complementary half on the caller side;
  if F7 ships, F6 follows in R36 with the same fix shape.
- **F8** (LOW): `BatchedStorageStrategy._ensure_flusher` start failure.

## DO-NOT-RE-FLAG additions after R35

- `BackendScheduler._close_locked` state-reset tail runs in the
  `finally` block (this fix), independent of R26-G manager release.
- Do NOT re-flag the dupefilter `_close_locked` retry design (R33).
- Do NOT re-flag R26-G manager release.
- Do NOT re-flag R34 scheduler consume_reservation ghost window.