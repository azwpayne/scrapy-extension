# SPEC — Round 22: RocketMQ timeout cap + flush-lock hang + docs drift

> Back-nav: [PLAN](PLAN-round22-rocketmq-caps-flushlock-docs.md) · [TASK](TASK-round22-rocketmq-caps-flushlock-docs.md)
> Scan: `wf_8e60fdd2-306` (post-R21 tree, main `1f46470`); 5 raw → 5 confirmed, 0 refuted, 11 agents, 0 errors.

## Problem statement

Round-22 introduced a **NEW thread-safety / memory-visibility dimension** (never
audited before). It immediately surfaced one real **MED shutdown hang**
(`BatchedStorageStrategy.close()` re-blocks on `_flush_lock` after its 5 s join
times out) plus a continuation of the input-validation cluster (one uncapped
RocketMQ timeout). Two **LOW docs-drift** regressions were introduced by R21-C /
R21-B themselves (runbook not updated for the new caps/gauges), and one **LOW**
dead-config Field surfaced in the RocketMQ settings module.

The frontier is genuinely thinning — race-correctness, api-contract, and the
r21-diff-regression dimensions all returned **0 findings** — so R22 ships the 5
remaining confirmed defects and is expected to be near the tail of this line.

## Confirmed findings → units

| Unit | Sev | Dimension | Finding | Fix |
|---|---|---|---|---|
| **R22-A** | MED | input-validation | `RocketMQSettings.send_timeout` (`Field(default=3000, ge=0)`) has no upper bound; conversion `request_timeout = send_timeout // 1000 if >= 1000 else 3` floors at 3 s but applies **no ceiling** → a stray-zero typo (e.g. `36000000` ms) yields a 10-hour gRPC per-RPC deadline that wedges the producer/consumer on a stalled broker. Sibling `invisible_duration` IS capped (`le=12*60*60`). | `le=300_000` on the Field (5 min in ms) + backend `_MAX_REQUEST_TIMEOUT_S = 300` module const + `request_timeout = min(max(3, send_timeout // 1000), _MAX_REQUEST_TIMEOUT_S)`. Mirrors the R21 throttle/circuit-breaker/backoff cap discipline. |
| **R22-B** | MED | thread-safety | `BatchedStorageStrategy.close()` (line 214) unconditionally calls `self.flush()` after the 5 s join times out. `flush()` → `_flush()` → `with self._flush_lock:` (line 233) **blocks forever** because the timed-out flusher still holds `_flush_lock` mid-`storage_backend.store()` against a wedged backend (redis-py `socket_timeout=None` / pymongo `socketTimeoutMS=None`). The join timeout is theater. Hits even with an empty live buffer. | **Option (a) — durable:** bound the `_flush_lock` acquisition itself in `_flush()` (`acquire(timeout=_FLUSH_LOCK_TIMEOUT_S)`; skip-and-log on timeout). Bounds BOTH the close() vector AND the public `flush()` vector (verifier: option b only fixes close()). Plain `threading.Lock` → `acquire(timeout=)` returns bool; zero behavioral change for healthy backends (store is ms). |
| **R22-C** | LOW | input-validation | `RocketMQSettings.max_message_size` (`Field(default=1MiB, ge=0)`) is a **dead config** — declared but never consumed by `RocketMQBackend` (0 refs in `backends/rocketmq.py`). An operator who sets it expecting a client-side fail-fast cap (mirroring `queue_max_item_bytes`) gets the value silently ignored; oversized payloads surface only as opaque broker errors. | Wire it: in `RocketMQBackend.push`, before `self._producer.send(msg)`, `if len(item) > self.config.max_message_size: raise QueueError(..., operation="push")`. Mirrors `BackendQueue`'s `queue_max_item_bytes` gate. (Does NOT add `le=` — byte count, not a wedge-prone timeout; broker enforces its own hard limit.) |
| **R22-D** | LOW | docs-vs-behavior (R21-C regression) | `docs/runbook.md:436` documents `SCRAPY_RETRY_DELAY` as "retry `n` sleeps uniformly between 0 and `base * 2**n`" — but R21-C capped `compute_full_jitter_backoff` at `_MAX_BACKOFF_S = 3600`. The formula overstates the backoff range for any `base*2**n > 3600` (e.g. `SCRAPY_RETRY_DELAY=300`, retry #4: documented 4800 s, actual 3600 s). The `_retry.py` docstring was updated; the operator-facing runbook was not. | One-line runbook edit: note the 3600 s ceiling. Code fix at `_retry.py:61` is correct and stays. |
| **R22-E** | LOW | docs-vs-behavior (R21-B regression) | `queue/delay_depth` is now a **live** Scrapy stats gauge (R21-B wired `BackendQueue` → `DelayQueueStrategy.set_monitor`; `stats.py:307-316` emits `set_value("queue/delay_depth", depth)`) but appears in **zero** operator docs. The runbook memory-cap row (`:569`) + operability-monitor table (`:584-585`) omit it, so operators conclude log-parsing the warn-once message is the only alerting option. | One-line runbook note: cite the live `queue/delay_depth` gauge as the alert target. |

## Non-goals

- **Public `flush()` API contract change** — R22-B bounds acquisition but keeps
  skip-and-log semantics (no new raised exception) so the at-least-once contract
  wording is unchanged; the wedge case is already documented as "in-flight items
  may be lost".
- **`max_message_size` upper bound** — byte count, not a timeout; broker enforces
  its own hard limit. Out of scope (avoids arbitrary ceiling).
- **Re-audit of closed clusters** — durability-bound push, connect-BaseException,
  exception-catch-breadth, bloom/cuckoo never-FN, dynamodb clear_storage TOCTOU,
  `_push_is_durable` pin: all on the DO-NOT-RE-FLAG list; untouched.
- **race-correctness / api-contract / r21-diff-regression** — all 3 returned 0
  findings this scan; no units derived.

## Gates (per unit, then round)

- `uv run ruff check src/ tests/` — clean (CI runs this BEFORE pytest).
- `uv run mypy --strict src/` — clean.
- `uv run pytest` — green, **unsandboxed** (engine-e2e subprocess probe + uv cache
  are sandbox artifacts; ~10 errors in-sandbox, 0 unsandboxed).
- coverage ≥ 95 % (project floor).
