# Round 54 — SPEC / PLAN / TASK: release ConnectionManager when replacement-crawler signal rewiring fails

## Context and audit evidence

`BackendSpiderMixin.setup_backend` (src/scrapy_extension/spider/spider_mixin.py:135)
gates the signal-wiring-failure cleanup on `acquired_here` only (line 191):

```python
if signal_wiring_failure is not None:
  cleanup_failed = False
  if acquired_here:          # <-- only the fresh-acquire path releases
    self._connection_manager = None
    try:
      manager.close()
    except BaseException:
      cleanup_failed = True
  ...
  raise signal_wiring_failure
```

`_connect_signals` (line 339), on the **replacement-crawler** branch (lines 356-360),
detaches the OLD crawler's `spider_opened`/`spider_closed` handlers and sets
`_connected_signals = None` **before** attempting to connect the new crawler's
handlers:

```python
previous_signal_manager = self._connected_signals
self._connected_signals = None
self._signals_connected = False
if previous_signal_manager is not None:
  self._disconnect_lifecycle_signals(previous_signal_manager)
```

If the NEW crawler's `signal_manager.connect(...)` then raises (lines 367-385),
the partial new handlers are rolled back and the exception propagates — but
`_connected_signals` stays `None` (line 386 is never reached), and the old
handlers are already gone.

Back in `setup_backend`, because the manager pre-existed this call,
`acquired_here` is `False`, so the cleanup block is **skipped**: the manager is
left live, `_connection_manager` still set, yet NO `spider_closed` handler is
wired to either crawler. Scrapy's later `spider_closed` therefore cannot trigger
`close_backend()`, so `ConnectionManager.close()` is never invoked and the
registry refcount is never decremented — the backend connection is held open
until process exit (connectors.py guarantees `_users > 0` entries are never
LRU-evicted).

This is the un-tested intersection of two existing hardenings: R47 hardened
`close_backend`'s `BaseException` teardown, and R73 added signal-wiring rollback
— but only the **fresh-acquire** failure path (`acquired_here=True`, tested at
test_spider_mixin.py:288) and the replacement-crawler **success** path (line 460)
are covered. The replacement-crawler **failure** path is neither handled nor
tested.

## Goal

If `setup_backend` is re-invoked on a replacement crawler and the new signal
wiring fails, release the now-orphaned `ConnectionManager` so the registry
refcount is decremented and the backend connection is not leaked for the life of
the process.

## Specification

- Broaden the cleanup gate in `setup_backend` from `if acquired_here:` to
  `if acquired_here or self._connected_signals is None:`. When the manager
  pre-existed (`acquired_here=False`) but `_connected_signals is None` at the
  failure point, the manager has no live lifecycle signals (old detached inside
  `_connect_signals`, new rolled back) — `spider_closed` can never fire
  `close_backend()`, so the manager must be released here.
- `manager.close()` on a shared singleton only decrements `_users`; it stays
  alive if other spiders still reference it. No change to sharing semantics.
- The fresh-acquire failure path (`acquired_here=True`) is unchanged — the
  `or` short-circuits to `True`.
- The same-crawler idempotent re-call never reaches this block: `_connect_signals`
  returns early at line 352 (`_connected_signals is signal_manager`), so
  `signal_wiring_failure` stays `None`.
- Success paths are unaffected (block not entered).
- Keep the change surgical: do not also clear `_queue`/`_dupefilter`/`_scheduler`
  (stale-component handling after a failed re-setup is out of scope; the leak is
  the finding). `close_backend()` remains the full-teardown path for the
  success-and-then-close lifecycle.

## Plan and independently verifiable tasks

- [ ] **R54-1 — RED: replacement-crawler signal-failure test.** Add
      `test_setup_backend_replacement_signal_failure_releases_manager` mirroring
      the fresh-acquire failure test (line 288) + the replacement-success test
      (line 460): first `setup_backend()` on crawler_A succeeds; reassign
      `spider.crawler` to crawler_B whose `signals.connect` raises on the 2nd
      call; assert `setup_backend()` re-raises, `manager.close` called once,
      `spider._connection_manager is None`, old signals disconnected. Run → FAIL
      (manager.close not called, `_connection_manager` still set).
- [ ] **R54-2 — GREEN: broaden the cleanup gate.** Change
      `if acquired_here:` → `if acquired_here or self._connected_signals is None:`
      with a comment explaining the orphaned-manager case. Run the new test →
      PASS; confirm the existing fresh-acquire/idempotent/success tests still
      PASS.
- [ ] **R54-3 — Verify.** `uv run ruff check .` then `uv run pytest` then
      `uv run mypy --strict src/scrapy_extension`. All green, no regressions.

## Acceptance criteria

1. On replacement-crawler signal-wiring failure, `manager.close()` is called
   exactly once and `spider._connection_manager is None` after the re-raise.
2. The fresh-acquire signal-failure path (line 288 test) is unchanged — still
   closes the manager.
3. The same-crawler idempotent re-call (line 269 test) and replacement-crawler
   success path (line 460 test) are unchanged — manager retained, no spurious
   close.
4. `ruff check`, `pytest`, and `mypy --strict` are all clean; no other test
   regresses.
