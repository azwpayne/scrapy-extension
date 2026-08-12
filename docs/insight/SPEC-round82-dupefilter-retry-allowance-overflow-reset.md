# SPEC — Round 82: dupefilter retry-allowance overflow latch not reset on clear

## Context

R-round fire 2026-08-12 (R91). Cap-scaled ultracode Workflow scan, 3 dims:
**ndiff** (always) + **elasticsearch** core + **queue-strategies concurrency**
(round_robin / ring_buffer / throttle — the three strategies not covered by
R84/R86 rescan-after-block).

- **ndiff: 1 confirmed** (clean-file, shippable — this SPEC).
- **elasticsearch: EMPTY** (core logic clean; the only ES findings remain the
  exhausted R74/R75 safe-list surface).
- **queue-strategies: EMPTY** (round_robin / ring_buffer / throttle clean of the
  rescan-after-block bug and ack/token mismatches).

## Goal

Ship the one confirmed ndiff finding: `_retry_allowance_overflow_warned` (the
one-shot advisory latch on dupefilter's failed-push retry-allowance LRU) is set
True on first overflow and **never reset**. `_clear_retry_allowances()` clears
the `_retry_allowances` LRU at every lifecycle boundary (open / close / clear)
but leaves the latch True, so in a multi-spider / long-running process the
advisory overflow warning fires once per process lifetime and is never
re-emitted for subsequent spiders — masking a chronically-too-low bound. This
is a direct ndiff sibling of R89 (`d2269be`, rabbitmq `_in_flight_overflow_warned`)
and R90 (`4d0bd9a`, pulsar + sqs), and an in-method inconsistency with the
adjacent `_volatile_fingerprint_overflow_warned = False` reset at line 1439
(six lines below the cleared LRU).

## Specification

**Defect (dupefilter.py:1431-1440):**

```python
def _clear_retry_allowances(self) -> None:
    """Discard transient failed-push allowances at lifecycle boundaries."""
    with self._retry_allowance_lock:
        self._retry_allowances.clear()          # line 1434 — LRU cleared
    self._pending_reservations.clear()
    self._active_reservations.clear()
    self._reservations_by_owner.clear()
    self._volatile_fingerprints.clear()
    self._volatile_fingerprint_overflow_warned = False   # line 1439 — sibling reset
    self._reservation_epoch += 1
```

The latch is initialized at line 235 and read/set ONLY at lines 1408-1409
(inside `with self._retry_allowance_lock:` in `_grant_retry_allowance`); it is
never reset anywhere (grep-verified: references are exactly lines 235, 1408,
1409). The sibling `_volatile_fingerprint_overflow_warned` IS reset at line
1439 in this same method, even though BOTH LRUs are cleared here — a local
inconsistency that strongly indicates the reset was intended but missed.

**Impact:** observability-only (severity low). The LRU eviction itself is
by-design bounded behavior and is unchanged by the fix; only the one-shot
advisory warning (code comments at lines 1419-1421 explicitly call it
advisory) fails to re-fire across a close→open cycle in the same process.
`_clear_retry_allowances` is called at open (718), close (786), and clear
(837). No correctness / data-loss path — the warning IS the safety signal
that's lost. Mirrors R89/R90 exactly (diagnostic-only latch; broker /
backend tracks the authoritative state).

**Fix (one line, inside the existing `_retry_allowance_lock` block — the flag
is read/written under that lock at 1408-1409, so its reset must be too):**

```python
def _clear_retry_allowances(self) -> None:
    """Discard transient failed-push allowances at lifecycle boundaries."""
    with self._retry_allowance_lock:
        self._retry_allowances.clear()
        self._retry_allowance_overflow_warned = False   # NEW — mirror line 1439
    self._pending_reservations.clear()
    ...
```

## Plan R82-seq

1. **RED** — `tests/test_dupefilter.py`: new test asserting the latch resets on
   `_clear_retry_allowances()`. Construct a dupefilter, force
   `_retry_allowance_overflow_warned = True`, call `_clear_retry_allowances()`,
   assert the flag is `False`. Expect FAIL (flag stays True).
2. **GREEN** — add `self._retry_allowance_overflow_warned = False` inside the
   `with self._retry_allowance_lock:` block at dupefilter.py:1434. Re-run → PASS.
3. **HARDEN** — add a second test covering the close→open re-fire path: grant a
   real allowance overflow (drive `_grant_retry_allowance` past
   `_retry_allowance_limit`), assert the warning fires (caplog) and the latch is
   True; call `_clear_retry_allowances()`; assert latch is False and a second
   overflow re-fires the warning. (Thematic pair, mirrors the R89/R90
   reset-on-reconnect test shape.)
4. **GATE** — `uv run --frozen ruff check .` + `uv run --frozen pytest` + `uv run
   --frozen mypy --strict src/scrapy_extension` + `uv run --frozen ruff format
   --check src tests conftest.py` (all green, worktree @ 4d0bd9a + fix, py3.10).
5. **COMMIT** — `fix(dupefilter): reset retry-allowance overflow warning latch
   on clear` + git trailers (Constraint / Rejected / Confidence / Scope-risk /
   Directive / Not-tested). `git push origin HEAD:main` (ff; fetch+rebase if
   moved). Watch CI green via `gh run list`. ExitWorktree(remove,
   discard_changes=true). Memory update.

## Acceptance criteria

- [ ] RED test fails before the fix (flag stays True after clear).
- [ ] GREEN: one-line reset added inside `_retry_allowance_lock` block.
- [ ] HARDEN test passes (close→open re-fire confirmed).
- [ ] All 4 gate commands green (ruff check + pytest + mypy --strict + ruff
      format --check).
- [ ] Conventional commit + trailers; ff-push to main; CI green.
- [ ] dupefilter.py is the ONLY non-test file touched; no dirty-file edits.
