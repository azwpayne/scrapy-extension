# Round 25 — PLAN: untouched-surfaces fixes

> Spec: [SPEC-round25-untouched-surfaces.md](./SPEC-round25-untouched-surfaces.md).
> TDD (RED → GREEN), each unit = one atomic conventional commit. Claude-Code-only.

## R25-A — `queue/queue.py:774` reject dunder callback/errback (LOW)

In `_request_from_dict`'s callback/errback loop, before `getattr`, reject
dunder names: `if method_name.startswith('__'): raise ValueError(...)`. Scope
is **dunders only** (not single-underscore) — blocks `__init__`/`__reduce__`/
`__setstate__`/`__del__` (the state-corruption vector) while preserving
legitimate `_private` and public callbacks. Test: a crafted payload with
`callback='__init__'` raises; `_private_cb` and `parse` accepted.

## R25-B — `queue/queue.py:1198` bound snapshot restore (LOW)

Add module const `_MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024` (16 MiB — orders above
any legit in-process strategy state, below the OOM danger zone). After
`state = storage.retrieve(snapshot_key)` and the bytes-type check, before
`bytes(state)`: `if len(state) > _MAX_SNAPSHOT_BYTES: logger.warning(...) +
return` (start clean). Mirrors the `len(data) > self.max_item_bytes` guard at
`queue.py:578`. Test: a >16 MiB retrieve returns without calling
`strategy.restore` and logs.

## R25-C — `storage/strategies/factory.py:75` parse_int_setting (LOW)

Replace the ad-hoc `int(threshold_raw)` coercion with
`parse_int_setting(threshold_raw, "threshold", minimum=1)` so float thresholds
are NOT silently truncated (they raise `ConfigurationError`, preserving R21-D's
strict-int contract at the factory entry) and bad types raise the codebase-
standard `ConfigurationError` instead of bare `TypeError`/`ValueError`. Test:
`create_storage_strategy('batched', threshold=50.9)` raises
`ConfigurationError`; `threshold='abc'` raises `ConfigurationError`; int 100
accepted.

## R25-D — `queue/strategies/delay.py:333` delay_depth drain+clear emit (MED)

Emit `self._monitor.on_delay_depth(len(self._holding))` at the end of
`_drain_ready` (after the drain loop, under `_state_lock`) and in `clear()`
(after `self._holding.clear()`, emit 0). No new thread — these are existing
single-threaded (reactor-thread) paths, so no new concurrency surface. Test:
push N items (gauge reads N), advance time + pop to drain, assert gauge falls
to the post-drain count; clear() asserts gauge reads 0.

## R25-F — `backends/connectors.py:752` multi-backend monitor wiring (LOW)

In `BackendDupeFilter.from_crawler` (after `dupefilter._monitor` is set) and
`BackendPipeline.from_crawler` (after `pipeline._monitor` is set), add
`self.connection_manager.set_monitor(self._monitor)`. All component monitors
wrap the same `crawler.stats`, so counters aggregate correctly across managers.
Test: a multi-backend deployment (queue≠dedup≠storage) — assert
`backend/connect_count` increments for each backend's connect, not just the
queue's.

## R25-G/H — `settings/rocketmq.py` remove dead config (LOW)

Remove `producer_group` (L139), `set_topic_prefix` (L163),
`storage_topic_prefix` (L164). Remove the default-value test assertions
(`test_rocketmq_backend.py:1415,1417,1418,1434`) and the integration kwarg
(`test_rocketmq_integration.py:165`). Add a docstring note at the Consumer
Group section that the gRPC Producer is group-less + RocketMQ is queue-only.
Add a CHANGELOG `[Unreleased] ### Removed` entry (removing dead settings =
fail-fast vs the prior silent-ignore; operators who set them now get a clear
`ValidationError`).

## Deferred — R25-E (`queue/pop_rate_1m` heartbeat)

Document the limitation in `monitor/base.py` + `monitor/stats.py` docstrings
(the gauge freezes on a fully-stopped consumer; correlate with process
liveness / scheduler dequeued counter) and a runbook note. Ship the heartbeat
thread in a dedicated observability round. (One-line doc edit bundled with
R25-D's commit or the round docs — no separate ship unit.)

## Gate

`uv run ruff check` → `uv run mypy --strict src/scrapy_extension` →
`uv run pytest` (≥3787 pass / ≥95% cov; sandbox off +
`UV_CACHE_DIR=$TMPDIR/uv-cache`).

## Ship

code-reviewer (opus) fan-out on the full R25 diff → ff-merge `worktree-round25`
→ `main` → push → delete branch (main-only). Memory record.
