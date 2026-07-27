# Round 32 — SPEC/PLAN/TASK: mongodb username/password whitespace + pipeline storage on_error

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_1c034644-1bd` (5-dim find + adversarial verify;
> 8 agents, 7 done / 1 verifier 429'd [cap resets 12:51:11]). Base: `main` @ `4569fea`.

## Headline

**Rotated away from the exhausted settings-fail-fast theme** to scheduler/queue-
strategy/spider-mixin/pipeline surfaces. **2 confirmed findings** (both verifiers
succeeded); 1 scheduler candidate (`SCHED-EXC-CATCH-1`) **deferred — its verifier
429'd** (unverified). The r31-diff-regression dim caught the headline (R31-A's
sweep missed the `username`/`password` siblings), and the NEW pipeline-store-path
dim paid off (storage on_error observability gap).

## Scan result

2 confirmed. EMPTY: `queue-strategies-deep` (round_robin/throttle/work_stealing/
priority/factory all sound), `spider-mixin` (setup_backend/from_crawler/component
access sound). DEFERRED (429-blocked verifier): `SCHED-EXC-CATCH-1` (scheduler
exception-catch finding — verify at R33).

## Ship set (2 units)

| ID | Sev | Surface | Defect |
|----|-----|---------|--------|
| **A** | MED | `settings/mongodb.py:265-272` (+ backend `mongodb.py:290`) | R31-A added an `auth_source` validator but the sibling `username` (str\|None) + `password` (SecretStr\|None) fields have none. Backend `_auth_kwargs` gates on bare truthiness (`if not (username and password)`) → whitespace `"   "` is truthy → passed verbatim to MongoClient → opaque auth failure. Same pattern as R31-A; **11th consecutive round diff-regression caught a self-shipped gap** (R31-A's sweep incomplete). |
| **B** | LOW | `pipeline/pipeline.py:541` | Storage-error swallow arm increments `pipeline/storage_errors` but does NOT call `self._monitor.on_error("store", e)` — inconsistent with the sibling serialization arm (line 494), the batched age-flusher, queue, and dupefilter. An operator alerting on the documented `errors/store` counter would miss the most common storage failure (synchronous store errors). Observability gap only (the `pipeline/storage_errors` stat still captures it). |

## Fixes (minimal, TDD)

### A — mongodb username/password whitespace (mirror R31-A)
Two `@field_validator`s (Optional fields → None-guard; password uses `get_secret_value().strip()`):
```python
@field_validator("username", mode="after")
def _reject_blank_username(cls, value):  # str | None
    if value is not None and not value.strip():
        raise ConfigurationError("MongoDB 'username' must be non-empty.", setting_name="username", setting_value=value)
    return value

@field_validator("password", mode="after")
def _reject_blank_password(cls, value):  # SecretStr | None
    if value is not None and not value.get_secret_value().strip():
        raise ConfigurationError("MongoDB 'password' must be non-empty.", setting_name="password")  # no setting_value (secret)
    return value
```

### B — pipeline storage on_error (mirror serialization arm line 493-496)
After line 541 (`self._inc_stat(spider, "pipeline/storage_errors")`), insert:
```python
try:
    self._monitor.on_error("store", e)
except Exception:  # noqa: BLE001 - telemetry cannot mask storage
    logger.debug("monitor.on_error(store) raised; ignored", exc_info=True)
```

## RED tests
- A: `test_mongodb_username_whitespace_rejected`, `test_mongodb_password_whitespace_rejected` (in test_config.py near R31's auth_source tests).
- B: `test_storage_error_emits_monitor_on_error` (in test_pipeline.py — assert `monitor.on_error("store", ...)` called on the storage swallow path).

## Gate / Merge / Record
ruff → mypy --strict → pytest (R31 3827 + 3 new = 3830 expected). ff-merge → push → delete branch → memory.

## DO-NOT-RE-FLAG after R32
mongodb username/password reject whitespace (R32-A) — R31-A sweep now fully covers
all mongodb auth fields (username/password/auth_source). pipeline storage arm emits
on_error (R32-B). DEFERRED R33: SCHED-EXC-CATCH-1 (scheduler exception-catch,
unverified). queue-strategies + spider-mixin sound (EMPTY).
