# SPEC-round79 — Attribute work_stealing worker_id failure to `SCRAPY_QUEUE_WORKER_ID`

**Round log:** R88 (SPEC counter round 79; offset R−9 from the round log).
**Source:** ndiff dimension of the R88 cap-scaled ultracode scan (opus find + adversarial verify), confirmed real + new + TDD-able.
**Surface:** `schedule/scheduler.py::BackendScheduler.from_settings` strategy-constructor catch block. The unfixed sibling of R80 (`fe72f30` snapshot_owner attribution).

## Context and audit evidence

R80 (`fe72f30`) fixed **snapshot_owner** misattribution: when a key-unsafe
`SCRAPY_QUEUE_WORKER_ID` (e.g. `'worker/1'` — `'/'` is rejected by `KEY_NAME_PATTERN`)
defaults into `snapshot_owner`, the raised `ConfigurationError` now names
`SCRAPY_QUEUE_WORKER_ID`. R80's commit **Directive** explicitly demands source-setting
attribution, and its **Rejected** line deliberately declined to *pre-validate*
`worker_id` in `with_strategy_settings` — leaving the `work_stealing` constructor path
as a known unfixed sibling. No work_stealing-equivalent regression test exists (the R80
test `test_snapshot_owner_defaulted_from_key_unsafe_worker_id_names_worker_id` covers the
snapshot path only).

The sibling lives in `BackendScheduler.from_settings` (`scheduler.py:1123-1138`):

```python
except (TypeError, ValueError, OverflowError) as exc:
    constructor_setting = {
        ...
        QueueStrategyType.WORK_STEALING: "SCRAPY_QUEUE_PEER_IDS",
        ...
    }.get(queue_config.strategy_type, "SCRAPY_QUEUE_STRATEGY")
    raise ConfigurationError(
        f"Invalid {constructor_setting}: {exc}",
        setting_name=constructor_setting,
        setting_value=settings.get(constructor_setting),
    ) from exc
```

End-to-end mechanism (verified by reading the actual code):
1. `scheduler.py:243-253` parses `SCRAPY_QUEUE_WORKER_ID` via `.strip()` only — **no
   `_validate_key_name`** — so `'worker/1'` survives into `queue_config.worker_id`.
2. `WorkStealingQueueStrategy.__init__` calls `_validate_key_name(worker_id, "worker_id")`
   at `work_stealing.py:108-109` — **before** peer validation at `:136` — so a key-unsafe
   worker_id raises `ValueError` here (and a peer-invalid value would not reach this branch
   unless worker_id were also valid).
3. The catch block hard-codes WORK_STEALING → `"SCRAPY_QUEUE_PEER_IDS"`, so the raised
   `ConfigurationError.setting_name == "SCRAPY_QUEUE_PEER_IDS"` and
   `setting_value == settings.get("SCRAPY_QUEUE_PEER_IDS")` (likely `None`/unrelated) —
   sending the operator to debug a setting they never set, while the real culprit
   `SCRAPY_QUEUE_WORKER_ID` appears only inside the message text.

Identical shape to the R80 snapshot_owner misattribution.

**Severity: low.** The error IS still raised correctly — no correctness, data-loss, or
security impact, only diagnostic attribution. A human reading the full message can still
find the culprit; only programmatic consumers of `ConfigurationError.setting_name` /
`.setting_value` are misled. Shipped because it closes R80's explicit Directive gap and is
the highest-ROI ndiff-regression pattern (sibling of a recent fix).

## Goal

When `work_stealing` construction fails because `SCRAPY_QUEUE_WORKER_ID` is key-unsafe,
the `ConfigurationError` must name `SCRAPY_QUEUE_WORKER_ID` (the setting the operator
configured), not the unrelated `SCRAPY_QUEUE_PEER_IDS`.

## Specification

In the catch block at `scheduler.py:1123-1138`, after the `constructor_setting` dict
`.get(...)` and before the `raise`, add a targeted re-check for the work_stealing case:

- Only when `queue_config.strategy_type is QueueStrategyType.WORK_STEALING`.
- Re-run `_validate_key_name(queue_config.worker_id, "worker_id")` (already imported at
  `scheduler.py:24`); on `ValueError`, set `constructor_setting = "SCRAPY_QUEUE_WORKER_ID"`.
- The existing `setting_value=settings.get(constructor_setting)` then resolves the correct
  worker_id value.
- No other strategy's attribution changes. No pre-validation added to
  `with_strategy_settings` (R80's Rejected stands). No signature change.

**Why this is correct (no false redirect):** `WorkStealingQueueStrategy.__init__` validates
`worker_id` (line 109) **before** `peer_ids` (line 136). So if the constructor raised, either
worker_id was the offender (redirect fires → WORKER_ID, correct) or worker_id was valid and a
peer was the offender (the re-check passes → constructor_setting stays PEER_IDS, correct).

## Plan and independently verifiable tasks

- **R88-1 (RED):** add `test_workstealing_key_unsafe_worker_id_names_worker_id` to
  `tests/test_scheduler_config_parsing.py` (next to the R80 sibling): real ScrapySettings
  with `SCRAPY_QUEUE_STRATEGY=work_stealing`, `SCRAPY_QUEUE_WORKER_ID="worker/1"`, a
  queue backend; mock `ConnectionManager.get_manager`; call `BackendScheduler.from_settings`;
  assert `ConfigurationError.setting_name == "SCRAPY_QUEUE_WORKER_ID"` and
  `setting_value == "worker/1"`. Run → fails (today `setting_name == "SCRAPY_QUEUE_PEER_IDS"`).
- **R88-2 (GREEN):** add the targeted re-check in the catch block. Run → passes.
- **R88-3 (HARDEN):** add a regression guard that a key-unsafe **peer_id** under work_stealing
  still attributes to `SCRAPY_QUEUE_PEER_IDS` (proves the re-check doesn't over-redirect).

## Acceptance criteria

- [ ] `pytest tests/test_scheduler_config_parsing.py` green, including the new tests.
- [ ] `uv run ruff check .` clean.
- [ ] `uv run ruff format --check src tests conftest.py` clean (CI enforces it).
- [ ] `uv run mypy --strict src/scrapy_extension` clean.
- [ ] `uv run pytest` full suite green (no regression).
- [ ] The R80 snapshot-owner test still green (sibling untouched).
- [ ] One atomic commit; ff-merge to `main`; CI green.
