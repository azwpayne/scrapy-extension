# PLAN — Round 22: RocketMQ timeout cap + flush-lock hang + docs drift

> Back-nav: [SPEC](SPEC-round22-rocketmq-caps-flushlock-docs.md) · [TASK](TASK-round22-rocketmq-caps-flushlock-docs.md)

## Design notes

### R22-A — RocketMQ `send_timeout` upper bound (defense-in-depth, two layers)

The codebase's R21 cap discipline is: **pydantic Field `le=`** (config-time
reject) + **module const** (runtime defense-in-depth). Both layers for timeouts
that can wedge a network call. `send_timeout` is the last timeout Field without
either.

- **Layer 1 (settings/rocketmq.py:143):** `send_timeout: int = Field(default=3000, ge=0, le=300_000)` — 5 min ceiling in ms. Generous for a slow broker send, but a stray-zero typo (e.g. `36000000`) is rejected at config load.
- **Layer 2 (backends/rocketmq.py:~202):** module const `_MAX_REQUEST_TIMEOUT_S: int = 300` (5 min in seconds — same ceiling, different unit) + `request_timeout = min(max(3, send_timeout // 1000), _MAX_REQUEST_TIMEOUT_S)`. Preserves the existing `max(3, …)` floor; adds the ceiling. Exported in `__all__` for parity with `CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S` / `_MAX_BACKOFF_S`.

The two layers express the **same 5-min ceiling** in their native units (ms /
s) so they cannot disagree.

### R22-B — Bound `_flush_lock` acquisition (option a, the durable fix)

`_flush_lock = threading.Lock()` (plain Lock → `acquire(timeout=)` returns bool).
Refactor `_flush()`:

```
acquired = self._flush_lock.acquire(timeout=_FLUSH_LOCK_TIMEOUT_S)
try:
  if not acquired:
    logger.warning("flush lock not acquired within %.1fs; skipping …", _FLUSH_LOCK_TIMEOUT_S)
    return
  self._flush_serialized()
finally:
  if acquired:
    self._flush_lock.release()
```

- `_FLUSH_LOCK_TIMEOUT_S: float = 5.0` — matches the close()-join timeout. Total
  close() wall-clock under a wedge is now ~10 s (5 s join + 5 s acquire) instead
  of ∞.
- **Why option (a) over (b):** the verifier explicitly stated option (b)
  (`if flusher is None or not flusher.is_alive(): self.flush()`) "does NOT
  remediate the public-flush() vector." Option (a) bounds both close() and
  public `flush()` because both go through `_flush()`.
- **Why zero hot-path risk:** healthy `store()` is ms; the timeout never fires
  in normal operation. The flusher is the only background thread (pipelines are
  single-threaded per spider, per the `_ensure_flusher` docstring), so lock
  contention only arises under the exact wedge the bound targets.
- **close() needs no separate guard** — once `_flush()` bounds, close()'s
  `self.flush()` skips-and-logs instead of hanging; the daemon flusher is killed
  on process exit (it's `daemon=True`). Minimal diff.

### R22-C — Wire `max_message_size` as a client-side fail-fast push gate

`RocketMQBackend.push` gains, before `self._producer.send(msg)`:

```
if len(item) > self.config.max_message_size:
  raise QueueError(
    f"item size {len(item)} exceeds RocketMQ max_message_size {self.config.max_message_size}",
    queue_name=queue_name, operation="push",
  )
```

- Mirrors `BackendQueue`'s `queue_max_item_bytes` gate (fail-fast at push,
  `QueueError`). Placed AFTER the existing `is_connected()` check, inside the
  `try` so the broad `except Exception → QueueError` arm is not disturbed —
  actually raise it BEFORE the `from rocketmq import Message` to avoid the
  import on the reject path; simplest is to raise right after the
  `is_connected()` guard, before the `try`.
- In default config the check never fires (item already ≤ `queue_max_item_bytes`
  default 1 MiB, and `max_message_size` default is also 1 MiB). It only fires
  when an operator tightens `max_message_size` below `queue_max_item_bytes`
  (e.g. a 512 KB broker limit) — the documented intent.

### R22-D / R22-E — runbook doc edits (R21 self-regressions)

Two one-line edits to `docs/runbook.md`:
- **:436** — append the 3600 s ceiling to the `SCRAPY_RETRY_DELAY` contract row.
- **:569 + :585** — cite the live `queue/delay_depth` gauge as the alert target
  alongside the `SCRAPY_QUEUE_DELAY_MAX_HELD` soft cap, and add it to the
  operability-monitor knobs table as the missing third gauge.

## Phases / fan-out strategy

**Main-loop sequential execution** (not parallel subagents). Rationale (memory
[[deep-insight-2026-07-23-ultracode]]): the ultracode scan IS the multi-agent
fan-out for insight; execution is atomic-commit-per-unit in a shared worktree
where parallel git commits + pytest races corrupt each other. OMC executor
subagents also fail "Prompt too long" on [1m] sessions. Direct TDD is the
proven-working path for small fixes on this repo.

Each unit: RED test first (asserts the defect) → GREEN (minimal fix) → ruff +
mypy on the touched file → atomic git commit. Round gate at the end: full ruff +
mypy --strict + pytest unsandboxed + coverage.

## Ordering

1. **R22-A** (MED, isolated to rocketmq settings + backend conversion).
2. **R22-B** (MED, isolated to batched `_flush()`).
3. **R22-C** (LOW, rocketmq push — same file region as A but different method).
4. **R22-D** + **R22-E** (LOW docs, batched into one commit — both are runbook
   drift from R21; a single `docs(runbook)` commit is cleaner than two).

5 atomic commits total (one per code unit + one combined docs commit).
