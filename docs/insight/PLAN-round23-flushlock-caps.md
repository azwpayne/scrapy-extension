# Round 23 — PLAN: implementation approach per unit

> Spec: [SPEC-round23-flushlock-caps.md](./SPEC-round23-flushlock-caps.md).
> Method: TDD (RED → GREEN → refactor), each unit = one atomic conventional
> commit. Claude-Code-only (Opus main-loop + code-reviewer fan-out). No
> Gemini/Codex/GPT.

## R23-A — `storage/strategies/batched.py` close-drain regression (MED, headline)

**Root cause.** R22-B (9fe364d) bounded `_flush()`'s `_flush_lock.acquire` to
`_FLUSH_LOCK_TIMEOUT_S=5.0` with skip-and-return on miss (L251-259).
`close()` (L205-223) does `flusher.join(timeout=5.0)` then `self.flush()` →
`_flush()` → `acquire(timeout=5.0)`. For a slow-but-healthy cross-region
backend (e.g. Mongo Atlas ~150ms/store × threshold=100 = 15s flush), the
flusher is still alive at t=5s; the subsequent `flush()`'s acquire times out
at t=10s and **skips**, abandoning items appended to `_buffer` between the
flusher's snapshot and `close()`.

**Fix (option (b) from the verifier, not (a)).** Loop the flusher `join` up to
a hard `_CLOSE_DRAIN_DEADLINE_S = 30.0` so a progressing flush completes and
releases `_flush_lock` *before* the final `self.flush()` runs. Once the
flusher is dead, the final `flush()` acquires the lock immediately and drains
the remaining buffer. A genuinely-wedged backend is bounded by the deadline
(not infinite — R22-B's anti-hang guarantee preserved, just at a 30s ceiling
vs the prior 10s).

```python
# module const
_CLOSE_DRAIN_DEADLINE_S: float = 30.0

# close():
flusher = self._flusher
if flusher is not None and flusher.is_alive():
    deadline = time.monotonic() + _CLOSE_DRAIN_DEADLINE_S
    while flusher.is_alive() and time.monotonic() < deadline:
        flusher.join(timeout=min(1.0, max(0.0, deadline - time.monotonic())))
    if flusher.is_alive():
        logger.warning(
            "batched-storage-age-flush did not exit within %.1fs; "
            "buffered items may be lost", _CLOSE_DRAIN_DEADLINE_S,
        )
self.flush()
```

**Docs corrections.**
- L46 "this timeout never fires in normal operation" → qualify: fires only
  when a flush exceeds the per-acquire bound (cross-region backend + large
  batch); ms-latency healthy stores never hit it.
- L219-222 close() warning → "buffered items may be lost" (not "in-flight");
  the in-flight batch is the flusher's, which the drain loop now waits for.
- L248-249 docstring → drop "no at-least-once guarantee is lost that the wedge
  had not already forfeited"; replace with: the skip concedes items only when
  the flusher is genuinely wedged past `_CLOSE_DRAIN_DEADLINE_S`; slow-but-
  progressing flushes are drained by the close loop.

**Regression test.** Populate `_buffer` AFTER the age-flusher snapshots its
batch (flusher mid-`store()` against a slow mock), trigger `close()`, assert
the post-snapshot buffered items ARE flushed (not lost). The existing
`test_close_does_not_hang_when_age_flusher_wedges_on_flush_lock` (threshold=1e9,
empty buffer at close) does not cover this — it only asserts no-hang.

## R23-B — `settings/redis.py` socket timeouts isfinite+cap (MED)

Add `field_validator(socket_timeout, socket_connect_timeout, mode="after")`
rejecting `not math.isfinite(v)`, plus `le=86400` on each Field. Mirrors
`_normalize_pop_timeout` isfinite (`backends/redis.py:110`) and the R21/R22
cap discipline (1-day ceiling). Test: `RedisSettings(socket_timeout=inf)`
raises `ValidationError`; valid 30.0 accepted.

## R23-C — `settings/elasticsearch.py` request_timeout isfinite+cap (MED)

Same pattern as R23-B on `request_timeout` (L105). Test:
`ElasticSearchSettings(request_timeout=float('inf'))` raises.

## R23-D — `settings/rabbitmq.py` heartbeat le=65535 (MED)

Add `le=65535` to the `heartbeat` Field (L329), mirroring the sibling
`max_priority` `le=255` (L323-327) which already enforces its AMQP protocol
bound. Test: `RabbitMQSettings(heartbeat=70000)` raises; 65535 accepted.

## R23-E — `settings/base.py` monitor_pop_rate_window_s bound (LOW)

The verifier offered three options (hard `maximum=`, warn-once, document).
**Chosen: warn-once** in `BackendQueue.__init__` when `window_s` exceeds a
threshold, mirroring `queue_delay_max_held`'s warn-once pattern
(`settings/base.py:271-282`). Rationale: a hard cap would reject a legitimate
24h rolling-rate window; warn-once flags pathological misconfig (soft-OOM)
without breaking intentional large windows. Document the memory cost
(`entries ≈ pop_rate × window_s`) in the Field description.

## R23-F — `.github/CHANGELOG.md` R17-R22 entries (LOW/docs)

Append to `[Unreleased] ### Fixed`: R22-C max_message_size push enforcement,
R22-A send_timeout cap, R21-C backoff cap, R21-A breaker reset_timeout cap,
R21-D NaN threshold/max_buffer_age rejection, R22-B flush_lock bound,
R21-B delay_depth gauge. No code change.

## R23-G — `backends/dynamodb.py` publish BaseException arm (LOW)

Wrap the publish step (L528-538) in `try/except BaseException`; on
BaseException, if not published (`self._generation is not candidate`),
call `self._close_resource(candidate.resource)` before re-raising. Mirror
`rabbitmq.py:540-558` identity-guarded pattern. Test: simulate BaseException
during publish, assert candidate resource closed exactly once and the live
generation untouched.

## Gate

`uv run ruff check` → `uv run mypy --strict src/scrapy_extension` →
`uv run pytest` (target ≥3782 pass / ≥95% cov). Sandbox OFF +
`UV_CACHE_DIR=$TMPDIR/uv-cache` (loopback/socket/file-logging artifacts
otherwise false-fail — see memory `dep-merge-2026-07-22`,
`v1-hardening-release-line-in-flight`).

## Ship

code-reviewer (opus) fan-out on the full R23 diff → ff-merge
`worktree-round23` → `main` → push → delete branch (main-only). Memory record.
