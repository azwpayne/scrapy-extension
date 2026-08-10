# SPEC-round71 — Attribute snapshot_owner key-name failures to the real source setting

> Back-nav: [../insight index](LEDGER.md). Related: R79 scan (this finding was
> confirmed there; queue-shipped cap-free this fire — ndiff sibling of R79, the
> volatile-shadow LRU, is documented FIFO, not a bug). Fire: R80.

## Context and audit evidence

In `_QueueComponentConfig.with_runtime_settings` (`schedule/scheduler.py:343-361`), when
`SCRAPY_QUEUE_SNAPSHOT_OWNER` is unset, `queue_snapshot_owner` defaults to `self.worker_id`
(L344-346). `worker_id` is parsed in `with_strategy_settings` (L243-253) by `.strip()` only —
it is **not** validated with `_validate_key_name` there (and the `delay`/`time_wheel`
strategies ignore `worker_id` entirely, so a key-unsafe value survives `build_queue_strategy`
unvalidated).

When that defaulted `worker_id` is key-unsafe (e.g. `SCRAPY_QUEUE_WORKER_ID='worker/1'`;
`KEY_NAME_PATTERN=^[a-zA-Z0-9._:-]+$` at `base.py:378` rejects `/`), `_validate_key_name` at
L355 raises `ValueError`, and the except block (L356-361) raises:

```python
ConfigurationError(str(exc), setting_name="SCRAPY_QUEUE_SNAPSHOT_OWNER", setting_value=snapshot_owner_raw)
```

`snapshot_owner_raw` is `None` (the operator never set `SNAPSHOT_OWNER`). So the structured
error points at a setting the operator **never touched** (`SNAPSHOT_OWNER`, `setting_value=None`),
while the real culprit `SCRAPY_QUEUE_WORKER_ID` is unnamed in the structured fields (the
invalid `worker/1` appears only in the message text). The operator is sent to debug the wrong
setting. (R79 verifier-confirmed, walked end-to-end.)

## Goal

A key-name failure for the snapshot owner is attributed to the setting the operator actually
configured — `SCRAPY_QUEUE_SNAPSHOT_OWNER` when set, `SCRAPY_QUEUE_WORKER_ID` when defaulted
from it — with the actual invalid value in `setting_value`.

## Specification

In `with_runtime_settings` (`scheduler.py:347-361`), compute the owning setting name from the
source before validating:

- `SCRAPY_QUEUE_SNAPSHOT_OWNER` when `snapshot_owner_raw is not None`
- `SCRAPY_QUEUE_WORKER_ID` when the owner defaulted from `self.worker_id`

Pass that name as the `field_name` to `_validate_key_name` (so the message text matches) and
use it as the `ConfigurationError.setting_name` in the except. Set `setting_value` to
`queue_snapshot_owner` (the actual invalid value) instead of `snapshot_owner_raw` (which is
`None` when defaulted). The `isinstance(str)` branch above (non-str `snapshot_owner_raw`) is
unchanged — a non-str can only arrive via `SCRAPY_QUEUE_SNAPSHOT_OWNER`, so its attribution is
already correct.

No change to validation behavior (the key-unsafe value is still rejected identically); only
the structured attribution + the field_name in the message text are corrected.

## Plan and independently verifiable tasks

- **R71-1 (RED)**: Add `test_snapshot_owner_defaulted_from_key_unsafe_worker_id_names_worker_id`
  — minimal `_QueueComponentConfig` with `SCRAPY_QUEUE_STRATEGY='delay'` +
  `SCRAPY_QUEUE_WORKER_ID='worker/1'` and no `SNAPSHOT_OWNER`; assert `with_runtime_settings`
  raises `ConfigurationError` with `setting_name == 'SCRAPY_QUEUE_WORKER_ID'`. Run — **FAILS**
  (currently `setting_name == 'SCRAPY_QUEUE_SNAPSHOT_OWNER'`).
- **R71-2 (GREEN)**: branch `owner_setting` by source; use it in `_validate_key_name` +
  `ConfigurationError(setting_name=..., setting_value=queue_snapshot_owner)`. Re-run — **PASSES**.
- **R71-3 (gate)**: `ruff check .`, `ruff format --check src tests conftest.py`, `pytest`,
  `mypy --strict src/scrapy_extension` green.
- **R71-4 (ship)**: atomic commit + ff-merge to `main`; CI green.

## Acceptance criteria

- `with_runtime_settings` with a key-unsafe `worker_id` + unset `SNAPSHOT_OWNER` raises
  `ConfigurationError(setting_name='SCRAPY_QUEUE_WORKER_ID', setting_value=<the worker_id>)`.
- With `SCRAPY_QUEUE_SNAPSHOT_OWNER` explicitly set to a key-unsafe value, attribution stays
  `SCRAPY_QUEUE_SNAPSHOT_OWNER` (unchanged).
- `_validate_key_name` message text names the correct source setting.
- `ruff check`, `ruff format --check`, `pytest`, `mypy --strict` green; CI on `main` green.
- No dirty file touched (`scheduler.py` + test are clean — not in the dirty list).
