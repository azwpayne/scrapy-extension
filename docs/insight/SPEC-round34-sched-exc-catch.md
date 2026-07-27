# SPEC — Round 34: SCHED-EXC-CATCH-1 (legacy ghost-fingerprint window)

**Base:** `main` @ `268c56f` (post-R33). **Surface:** `schedule/scheduler.py`
`enqueue_request` legacy (non-atomic) dupefilter path.

## Origin

`SCHED-EXC-CATCH-1` — a finding deferred **4 times** (R30 → R31 → R32 → R33) by
persistent 429 cap-throttling of its subagent verifier. R34 inline-verifies it
(read the cited location directly, per the R33 degraded-mode precedent) instead
of deferring a 5th time.

## The defect (inline-verified REAL, LOW-MED)

The legacy dupefilter arm of `BackendScheduler.enqueue_request`
(`schedule/scheduler.py:1605-1622`) consults an add-on-check dupefilter and
**then** assigns the rollback gate:

```python
seen = self.dupefilter.request_seen(request)   # :1607  — RECORDS fingerprint
                                                #         (bundled BackendDupeFilter
                                                #         _request_seen_unlocked:982
                                                #         self._filter.add(...))
if not seen:
    consume_reservation = getattr(self.dupefilter, "consume_reservation", None)
    dedup_reserved = (                            # :1618 — assigned AFTER the call
        bool(consume_reservation(request))        # :1619 — INTERRUPTIBLE
        if callable(consume_reservation)
        else True
    )
```

`dedup_reserved` is the gate every cleanup arm uses to decide whether to call
`_rollback_dupefilter_reservation` → `dupefilter.forget(request)`:

- `except SerializationError`  (`:1697`) — `elif dedup_reserved:`
- `except (QueueError, BackendError)` push-phase (`:1743`) — `elif dedup_reserved:`
- `except BaseException` (`:1769`) — `elif dedup_reserved:`

A `BaseException` (Ctrl+C / SystemExit) — **or any regular `Exception` that is
not `SerializationError`/`QueueError`/`BackendError`** — landing during
`consume_reservation(request)` (`:1619`) leaves `dedup_reserved` at its init
value `False` (`:1577`). The `except BaseException` arm at `:1749` then sees
`dedup_reserved=False` (and `reservation`/`reservation_intent` are `None` on the
legacy path) → **none of the rollback branches fire** → `forget()` is never
called → the fingerprint that `request_seen` recorded at dupefilter `:982`
stays in the membership set **permanently**.

### Consequence

1. **Ghost fingerprint** — the URL is wrongly deduped-out on every future
   re-yield (re-scrape, redirect retry, scheduler reschedule).
2. **Lost URL (BaseException case)** — the push never happened, so unlike the
   dedup-outage degrade-to-enqueue arm, the request is gone entirely. Permanent
   data loss with a marker that blocks redelivery.

### Documented intent (why this is a defect, not accepted)

The `except BaseException` comment (`:1750-1753`) explicitly states the intent:

> Process-control interruption after receipt handoff but before a confirmed push
> follows the package's at-least-once policy: compensate best-effort, preserve
> the original signal, and **accept possible replay rather than leave a permanent
> ghost fingerprint.**

The current code **violates** this intent for the consume_reservation window.
The `dedup_reserved` gate is the mechanism intended to prevent the ghost — it is
simply assigned one statement too late.

## Why this is NOT an R33-style false positive

R33 refuted a "lone holdout" finding where the gate (`_filter_released`) WAS the
retry mechanism and the BaseException window was an accepted cost of that design.
Here:

1. The documented intent explicitly says "no ghost fingerprint."
2. The gate (`dedup_reserved`) is not a retry mechanism — it is a one-shot
   rollback flag, and it is **demonstrably assigned too late** (after the
   interruptible call whose result it captures).
3. The bundled `BackendDupeFilter` hits this exact path (`request_seen`
   add-on-checks at `:982`, and implements `consume_reservation` at `:1279`).

The TDD gate is the second adversarial verifier: if the RED test is not RED, or
if the fix breaks existing retry/rollback tests, the finding is refuted (as R33
was). The fix is sized to be minimal enough that breakage would signal the
finding is wrong, not that the fix is incomplete.

## The fix (minimal, surgical)

Pre-arm `dedup_reserved` to `True` **before** the interruptible
`consume_reservation(request)` call, then let the call's precise return value
refine it on success:

```python
dedup_reserved = True  # pre-arm: request_seen may have recorded a fingerprint
                       # (add-on-check filters). A BaseException during
                       # consume_reservation would otherwise leave this False
                       # and leak that fingerprint. The call's return refines
                       # it on success; _rollback_dupefilter_reservation→forget
                       # is guarded (try/except) for the filter-full-miss case
                       # where request_seen recorded nothing.
dedup_reserved = (
    bool(consume_reservation(request))
    if callable(consume_reservation)
    else True
)
```

### Why this is safe

- **Success path unchanged:** when `consume_reservation` completes,
  `dedup_reserved = bool(result)` — identical to today. The SerializationError
  (`:1697`) and QueueError/BackendError push (`:1743`) arms behave identically.
- **Interrupted path fixed:** a BaseException/Exception during the call leaves
  `dedup_reserved=True` (pre-armed) → `:1769` fires → `forget()` called.
- **Over-trigger is bounded and guarded:** the only over-trigger case is
  `request_seen` returning not-seen WITHOUT recording (filter-full miss at
  dupefilter `:988`, or retry-allowance consumption at `:977`). In that case
  `consume_reservation` returns False normally (refines `dedup_reserved` to
  False) — so over-trigger only happens if BaseException interrupts a
  filter-full-miss `consume_reservation`, and `forget()` is then wrapped in
  `_rollback_dupefilter_reservation`'s `try/except Exception` (`:1873-1886`) →
  at worst a `scheduler/dupefilter_rollback_error` stat + log. No silent harm.
  This is strictly better than a permanent ghost fingerprint.

### Out of scope (accepted residuals, mirroring R33)

- **`except (QueueError, BackendError)` dedup-phase arm** (`:1703-1718`) does
  not call `_rollback_dupefilter_reservation` even with `dedup_reserved=True`
  — it degrades to enqueue. A `QueueError`/`BackendError` during a custom
  filter's `consume_reservation` therefore still leaks a fingerprint (request
  is enqueued once, then ghost on re-yield). Lower impact than the BaseException
  case (URL is not lost), and changing the dedup-outage arm's contract is a
  larger behavior change. Documented as accepted residual.

## TDD plan

1. **RED:** custom legacy dupefilter whose `request_seen` returns not-seen
   (add-on-check) and whose `consume_reservation` raises `KeyboardInterrupt`.
   Assert `scheduler.enqueue_request(request)` re-raises `KeyboardInterrupt`
   AND `dupefilter.forget` is called with `request`. → pre-fix, `forget` is
   NOT called (gate inactive) → assertion fails.
2. **GREEN:** apply the 1-line pre-arm. `forget` is called → assertion passes.
3. **no-regression:** full `tests/` gate stays green (no existing rollback /
   retry / dedup test breaks — the success path is unchanged).

## Verification gate

`uv run ruff check` → `uv run mypy --strict src/scrapy_extension` →
`uv run pytest` (target ≥ R33's 3830 pass + the 1 new test).
