# SPEC — R72: Memcached store() silently drops data when ttl > 30 days

> Back-navigation: [../insight/](.) · Round log in [MEMORY index](../../../../.claude/projects/-Users-payne-WorkSpace-project-individual-dev-web-crawler-scrapy-extension/memory/MEMORY.md).
> R81 fire (2026-08-11), 2-dim scan on the fresh backend implementations (pulsar/sqs + memcached/dynamodb). 1 confirmed (this), 1 refuted (Pulsar sub-ms timeout — C++ `wait_for(0)` returns immediately, does not block).

## Goal

Stop silent data loss in the Memcached storage backend: when a caller passes a relative TTL greater than 30 days, `store()` currently reports success while Memcached discards the item immediately. Make oversized relative TTLs reach the server in the form Memcached actually expects.

## Problem (root cause)

`MemcachedBackend.store` passes the caller's relative TTL straight through to pymemcache:

```python
# src/scrapy_extension/backends/memcached.py:396
stored = client.set(key, data, expire=0 if ttl is None else ttl)
```

The Memcached wire protocol defines the `exptime` field as:

- **`exptime <= 60*60*24*30` (2,592,000, ~30 days)** → relative seconds from now.
- **`exptime > 2,592,000`** → an **absolute Unix epoch timestamp** (UTC seconds).

pymemcache forwards `expire` to the server verbatim (no 30-day conversion). The shared `_validate_ttl` (`backends/base.py:398`) only requires `ttl` to be a positive `int` (rejecting bool / non-int / `<= 0`), so any `ttl` in `(2,592_000, +inf)` passes validation and reaches the driver unmodified.

### Concrete failure scenario

`backend.store("user_session", b"<payload>", ttl=2_592_001)` (31 days — a normal long-lived cache TTL):

1. `_validate_ttl(2_592_001)` → passes (positive int).
2. `client.set(key, data, expire=2_592_001)` → pymemcache sends `exptime=2592001`.
3. Memcached sees `2592001 > 2592000` → reads it as the absolute timestamp `1970-01-31 14:40:01 UTC` (decades in the past) → item is instantly treated as expired.
4. Server returns `STORED` → `client.set` returns `True` → the `stored is not True` guard at line 400 does **not** fire → `store()` returns `None` (success).
5. `backend.retrieve("user_session")` → `None`; `backend.exists(...)` → `False`.

**Net effect:** `store()` reports success while the data is silently discarded — undetectable data loss in the storage/KV path (e.g. a `BackendPipeline` that believes it durably persisted an item).

Any `ttl` in `(2_592_001, ~1.7e9)` (current epoch) lands in the past and is lost immediately; larger TTLs land at a wrong future instant.

### Reachability

`ttl` is not bounded upstream:

- `BackendPipeline.ttl` ← `SCRAPY_PIPELINE_TTL` (`pipeline/pipeline.py:318`), parsed via `parse_int_setting`; `ttl=2592001` flows unmodified to `storage_backend.store(key, data, ttl=2592001)` through both `passthrough` (`storage/strategies/passthrough.py:42`) and `batched` (`storage/strategies/batched.py:427`) strategies.
- `store()` is part of the public `StorageBackend` ABC — any direct caller can pass an oversized TTL.

### Asymmetry (confirms this is a Memcached-only gap)

DynamoDB is unaffected: `dynamodb.py` computes `expire_at = math.ceil(time.time() + ttl)` — an explicit absolute epoch. Only Memcached forwards the raw relative value.

## Solution

Convert only oversized relative TTLs to the absolute timestamp Memcached expects; values ≤ 30 days and the `None` sentinel behave exactly as before. Touch **only** `memcached.py` (a clean file).

1. Add `import time` to the imports.
2. Add a module constant near the other `_MEMCACHED_*` storage constants:
   ```python
   _MEMCACHED_MAX_RELATIVE_TTL_SECONDS = 60 * 60 * 24 * 30  # 2_592_000
   ```
3. In `MemcachedBackend.store`, replace the single `client.set` call's `expire` with a 3-way computation:
   ```python
   if ttl is None:
       expire = 0
   elif ttl > _MEMCACHED_MAX_RELATIVE_TTL_SECONDS:
       expire = int(time.time()) + ttl
   else:
       expire = ttl
   stored = client.set(key, data, expire=expire)
   ```

For `ttl > 2_592_000`, `int(time.time()) + ttl` is always a large future timestamp (well within Memcached's 32-bit exptime range for any realistic TTL), so the server treats it as the intended absolute expiry. The `None` → `0` and small-positive pass-through paths are byte-equivalent to today.

## Why this is NEW (not an exhausted/shipped theme)

- Not the raw-error-wrapping theme — `store()` already raises `StorageError` on driver failure and on a non-`True` set result; no exception path is taken here because `set` returns `True`.
- Not settings whitespace/strip/fail-fast — `_validate_ttl` is the shared runtime-input contract and is out of scope; `ttl=2_592_001` legitimately passes it.
- Not a mongodb-URI / DynamoDB / RocketMQ / Kafka theme.
- No 30-day handling exists anywhere in `src/` or `tests/` (grep for `2592000` / `60*60*24` / `_MAX_RELATIVE_TTL` → empty). The existing `test_store_sets_with_ttl` only covers `ttl=60`.

## Tasks

- [ ] TDD RED: `test_store_ttl_over_30_days_converts_to_absolute_timestamp` — patch `time.time` to a fixed value; `b.store("k", b"v", ttl=2_592_001)`; assert `client.set.assert_called_once_with("k", b"v", expire=fixed_now + 2_592_001)`. Fails before fix (`expire=2592001`).
- [ ] TDD GREEN: implement the 3-way `expire` computation + `import time` + `_MEMCACHED_MAX_RELATIVE_TTL_SECONDS` constant in `memcached.py`.
- [ ] Regression guard: keep `ttl=60` → `expire=60`, `ttl=None` → `expire=0`, no-ttl → `expire=0` byte-equivalent (existing tests stay green).
- [ ] GATE: `ruff check .` + `ruff format --check src tests conftest.py` + `pytest` + `mypy --strict src/scrapy_extension`.
- [ ] Atomic commit + ff-push `HEAD:main`; verify CI green.

## Verification (hand-confirmed before TDD)

- `memcached.py:396` passes `ttl` straight through as `expire`. ✓
- `_validate_ttl` (base.py:398) rejects only bool/non-int/`<=0`; `ttl=2_592_001` passes. ✓
- `SCRAPY_PIPELINE_TTL` → `BackendPipeline.ttl` → `store(...)` unmodified. ✓
- Adversarial verifier (opus) could not refute: constructed the full `store→set→STORED→retrieve→None` chain and confirmed the dynamodb asymmetry. ✓

## Confidence / risk

- **Confidence:** high — textbook Memcached protocol gotcha, source-confirmed pass-through, production-reachable.
- **Scope-risk:** narrow — one `store()` block in a clean file; small-TTL and None paths byte-equivalent.
- **Constraint:** touch only `memcached.py` + the new test (none in the dirty tree).
- **Directive:** Memcached `exptime > 2_592_000` means absolute epoch. Any backend method that forwards a relative duration to `client.set(expire=...)` MUST convert oversized values to absolute; do not pass a raw relative `ttl` through.
