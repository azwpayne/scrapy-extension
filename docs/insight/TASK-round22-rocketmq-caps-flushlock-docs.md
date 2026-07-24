# TASK — Round 22: RocketMQ timeout cap + flush-lock hang + docs drift

> Back-nav: [SPEC](SPEC-round22-rocketmq-caps-flushlock-docs.md) · [PLAN](PLAN-round22-rocketmq-caps-flushlock-docs.md)

## R22-A — RocketMQ `send_timeout` upper bound

**Files:** `src/scrapy_extension/settings/rocketmq.py`, `src/scrapy_extension/backends/rocketmq.py`, `tests/test_config.py` (or `tests/test_rocketmq_backend.py`).

1. **RED:** add a test asserting a huge `send_timeout` is rejected by pydantic
   (`ValidationError`) AND a test asserting the backend conversion caps at
   `_MAX_REQUEST_TIMEOUT_S`. Run → fail.
2. **GREEN:**
   - `settings/rocketmq.py:143` → `send_timeout: int = Field(default=3000, ge=0, le=300_000)`.
   - `backends/rocketmq.py`: add `_MAX_REQUEST_TIMEOUT_S: int = 300` (export in
     `__all__` if one exists, else module-level const + docstring); change the
     conversion to
     `request_timeout = min(max(3, send_timeout // 1000), _MAX_REQUEST_TIMEOUT_S)`.
3. `ruff check` + `mypy --strict` on both files.
4. **Commit:** `fix(rocketmq): cap send_timeout at 5min so a typo cannot wedge the gRPC deadline (R22-A)`.

**DoD:** huge `send_timeout` rejected at config; conversion never exceeds 300 s;
default 3000 ms → 3 s unchanged.

## R22-B — Bound `_flush_lock` acquisition (durable, option a)

**Files:** `src/scrapy_extension/storage/strategies/batched.py`, `tests/test_storage_batched*.py`.

1. **RED:** add a test that constructs a `BatchedStorageStrategy` with
   `max_buffer_age_s` set (so a flusher spawns), installs a backend whose
   `store()` blocks (e.g. an Event it waits on), appends an item, waits for the
   flusher to enter `store()` (hold the lock), then calls `close()` and asserts
   it returns within a bounded window (e.g. < 15 s) instead of hanging. Run with
   a pytest timeout guard → fail (hang/timeout). NOTE: use a *short* artificial
   `_FLUSH_LOCK_TIMEOUT_S` via monkeypatch or a tight join so the test is fast;
   the real const is 5.0 s.
2. **GREEN:** add module const `_FLUSH_LOCK_TIMEOUT_S: float = 5.0`; refactor
   `_flush()` to `acquired = self._flush_lock.acquire(timeout=…)` + try/finally
   + skip-and-log on `not acquired`. Do NOT touch `close()`.
3. `ruff` + `mypy --strict`.
4. **Commit:** `fix(storage): bound BatchedStorageStrategy _flush_lock acquire so close()/flush() cannot hang on a wedged backend (R22-B)`.

**DoD:** close() returns within ~10 s under a wedged flusher (was ∞); healthy
flush (ms store) unchanged; public `flush()` also bounded.

## R22-C — Wire `max_message_size` as a push-time fail-fast gate

**Files:** `src/scrapy_extension/backends/rocketmq.py`, `tests/test_rocketmq_backend.py`.

1. **RED:** add a test that calls `push()` with an item larger than
   `config.max_message_size` (set it small, e.g. 8) and asserts `QueueError`
   with `operation="push"` is raised BEFORE `producer.send` is called (mock the
   producer; assert `send` not called). Run → fail.
2. **GREEN:** in `push()`, after the `is_connected()` guard and before the
   `try: from rocketmq import Message`, add
   `if len(item) > self.config.max_message_size: raise QueueError(...)`.
3. `ruff` + `mypy --strict`.
4. **Commit:** `fix(rocketmq): enforce max_message_size at push so the documented client-side cap is not silently ignored (R22-C)`.

**DoD:** oversized push raises `QueueError` client-side; default 1 MiB unchanged
(doesn't fire when item ≤ `queue_max_item_bytes`).

## R22-D + R22-E — runbook doc drift (one combined commit)

**File:** `docs/runbook.md`.

1. **:436 (R22-D):** append to the `SCRAPY_RETRY_DELAY` contract row — note the
   `min(base * 2**n, 3600s)` ceiling (R21-C).
2. **:569 + :585 (R22-E):** add the live `queue/delay_depth` gauge — cite it as
   the alert target next to `SCRAPY_QUEUE_DELAY_MAX_HELD`, and add a row to the
   operability-monitor knobs table (the missing third gauge).
3. **Commit:** `docs(runbook): note R21-C retry 3600s cap + the live queue/delay_depth gauge (R22-D/E)`.

**DoD:** runbook formula matches code; `queue/delay_depth` discoverable by an
operator reading the memory-cap + monitor sections.

## Round gate (after all 4 commits)

- `UV_CACHE_DIR=$TMPDIR/uv-cache uv run ruff check src/ tests/`
- `uv run mypy --strict src/`
- `uv run pytest` (unsandboxed — engine-e2e + uv cache are sandbox artifacts)
- coverage ≥ 95 %
- All green → ExitWorktree(keep) → ff-merge main → push → worktree remove --force + branch -d.
