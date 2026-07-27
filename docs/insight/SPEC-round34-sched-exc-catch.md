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

---

# Addendum — R34-B + R34-C (circuit_breaker.py focused scan)

After R34-A shipped, a focused ultracode scan of the previously-un-audited
`backends/circuit_breaker.py` (3 finder lenses + adversarial verify, opus, 5
agents / 0 errors / ~917k tokens) surfaced 2 more confirmed findings. Both
shipped in the same round.

## R34-C (MED) — `_QueueBackendProxy` forwards ack/nack outside the breaker

`_QueueBackendProxy._FORWARDED` included `ack`/`nack` (lines 490-491),
justified by a comment calling them "no-ops on atomic backends." That is true
for Redis/MongoDB/ES (they inherit the ABC no-op ack) but **FALSE for the 5 MQ
backends** — `ack()` is a real network op there (`kafka consumer.commit`,
`rabbitmq basic_ack`, `sqs delete_message`, `rocketmq`/`pulsar` ack) that
raises `QueueError` (a `BackendError`). Two failure modes:

- **(a) ack-path-only degradation invisible.** A broker degradation isolated
  to the ack path (e.g. Kafka group-coordinator / `__consumer_offsets` broker
  partitioned while partition leaders keep serving `push`/`pop_with_ack`)
  raised `QueueError` that never reached `breaker.call` → `_failure_count`
  never incremented → breaker never tripped → **zero operator signal**.
- **(b) no fail-fast when OPEN.** Once tripped via a later `push`/`pop`,
  forwarded `ack()` still hit the dead broker and blocked on the commit
  timeout (librdkafka ~60s) instead of failing fast → tied up
  `CONCURRENT_REQUESTS` workers, defeating the breaker's contract on the ack
  path.

The 2026-07-10 fix already wrapped `pop_with_ack` for the **identical
rationle** ("a broker degradation on the MQ ack-pop path trips the breaker");
R34-C extends it to `ack`/`nack`. **Fix:** move `ack`/`nack` from `_FORWARDED`
to `_HOT_PATH`. Safe because (1) atomic-pop backends' ack is an ABC no-op →
wrapping is a no-op for their breaker state, and (2) `CircuitBreakerOpenError`
subclasses `BackendError` → the scheduler's existing
`(QueueError, BackendError)` ack-error handling covers the fail-fast path
unchanged. Updated `test_non_hot_path_methods_forwarded_unchanged` (it used a
fake no-op ack that encoded the wrong assumption) and added
`test_ack_failure_trips_breaker_and_fail_fast_when_open`.

## R34-B (LOW) — `reset_timeout` accepts bool silently

`reset_timeout` validation (line 132) used `math.isfinite(x) or x < 0`, which
does NOT reject bool (`math.isfinite(True)` is True, `True < 0` is False) →
`reset_timeout=True` was silently stored as boolean `True` (the OPEN breaker
then waited ~1s). Asymmetric with the R21-A `failure_threshold` bool guard
(line 126). **Production-unreachable** (pydantic float-coerces the settings
field `True→1.0`), but direct `CircuitBreaker(...)` construction (third-party
plugins, YAML `on`/`off` parsed as bool, test doubles) must still reject it.
**Fix:** one-line `isinstance(x, bool)` mirror of the R21-A guard +
`test_bool_reset_timeout_raises`.

## Round-34 totals

3 findings (R34-A LOW-MED scheduler + R34-B LOW cb-bool + R34-C MED cb-ack),
all TDD RED→GREEN, all 3 gates green (ruff / mypy --strict 76 files / pytest
**3833 passed** = R33's 3830 + 3). The 4×-deferred SCHED-EXC-CATCH-1 debt is
cleared, and the previously-un-audited `circuit_breaker.py` is now covered.

