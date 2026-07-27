# Round 31 — SPEC/PLAN/TASK: mongodb auth_source whitespace gap

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_0fad54a0-30d` (5-dim find + adversarial verify;
> 6 agents, 0 errors, ~1.1M tokens — cap reset, full run). Base: `main` @ `ff7df09`.

## Headline

**1 raw → 1 confirmed, 0 refuted. 4/5 dims EMPTY.** The `deferred-candidates-verify`
dimension resolved the R30-deferred grep candidate: **mongodb `auth_source`** —
R29-A/B/C/D's whitespace-truthiness sweep covered `database`/`collection_names`/
`replica_set_members`/`mongos_routers`/`replica_set_name` but **missed `auth_source`**.
Same defect class as R29-C; the deferred-candidates dimension paid off (R30 noted
`auth_source` as a grep candidate, R31 verified it).

## Scan result

**1 confirmed.** EMPTY: `r30-diff-regression` (R30 fixes complete, no missed siblings),
`utils-redaction-serialization` (utils + redaction sound — secret-leak/serialization
crash paths all handled), `pulsar-deep` (pulsar sound beyond PS-1), `connectors-registry-deep`
(ConnectionManager singleton sound — per-key lock, settings_hash, disconnect race,
MAX_MANAGERS eviction all correct).

Deferred-candidates resolution: **PS-1** (pulsar subscription_name) and **mongodb
atlas_cluster_name** NOT flagged → sound (backend guards / no opaque-failure path).
**mongodb auth_source** flagged → the 1 finding.

## Ship set (1 unit)

| ID | Sev | Surface | Defect |
|----|-----|---------|--------|
| **A** | MED | `settings/mongodb.py:273` (+ backend `mongodb.py:294`) | `auth_source: str = Field(default="admin")` has no settings-layer validator; backend `_auth_kwargs` uses bare truthiness (`if self.config.auth_source:`) → whitespace `"   "` is truthy and passed verbatim as `authSource='   '` to MongoClient → opaque authentication failure. Empty-string is benign (falsy → skipped → pymongo default), whitespace is the live footgun. Same class as R29-C `database`. |

## Fix (mirrors R29-C exactly)

Add `@field_validator("auth_source", mode="after")` rejecting empty/whitespace —
identical to the shipped R29-C `database` validator (settings/mongodb.py:369-381):

```python
@field_validator("auth_source", mode="after")
@classmethod
def _reject_blank_auth_source(cls, value: str) -> str:
    if not value or not value.strip():
        raise ConfigurationError(
            "MongoDB 'auth_source' must be non-empty.",
            setting_name="auth_source", setting_value=value,
        )
    return value
```

### RED
- `test_mongodb_auth_source_empty_rejected` — `MongoDBSettings(auth_source="")` raises.
- `test_mongodb_auth_source_whitespace_rejected` — `MongoDBSettings(auth_source="   ")` raises (the live footgun).

## DO-NOT-RE-FLAG after R31

mongodb `auth_source` rejects empty/whitespace (R31-A). R29's whitespace sweep now
fully covers the mongodb name/auth fields. utils + redaction sound (EMPTY). pulsar
beyond PS-1 sound (EMPTY). ConnectionManager registry sound (EMPTY). PS-1 +
atlas_cluster_name confirmed sound (not flagged).

## Gate / Merge / Record

ruff → mypy --strict → pytest (R30 3825 + 2 new = 3827 expected; UV_CACHE_DIR + sandbox off).
ff-merge → push → delete branch → memory record.
