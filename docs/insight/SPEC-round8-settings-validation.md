# SPEC — Round 8 Settings-Validation Footgun Hunt (for the next /goal)

Third `/loop` incremental fire. NEW insight dimension (settings validation
completeness — rounds 1-8 only added spot TLS guards in round-6). A focused
code-reviewer audit found **34 settings footguns**: 8 HIGH + ~18 MEDIUM. Every
one is a user-supplied invalid value silently accepted at config time that
surfaces as an opaque stack trace later (wrong cluster, silent auth bypass,
cleartext credential leak, etc.). This is a genuine correctness/UX cluster,
not a rehash — it warrants its own SPEC.

Companion: [`PLAN-round8-forward.md`](./PLAN-round8-forward.md) (F10 settings-
reference is the DOCS side; this is the VALIDATION side — they pair).

## Findings summary (34 total, by pattern)

| Pattern | Count | Sev mix | Unit |
|---|---|---|---|
| Enum-shaped `str` fields that should be `Literal` (typos accepted) | 10 | 4H, 6M | SV1 |
| Mode-conditional requirements missing (required-when-mode) | 8 | 3H, 5M | SV2 |
| Cross-field auth/transport coherence missing | 6 | 3H, 3M | SV3 |
| URL/scheme format unguarded (beyond round-6 SQS/Dynamo/ES) | 5 | 1H, 4M | SV4 |
| Empty-string + unbounded-int gaps | 5 | 1H, 4L-M | SV5 |

**Validated well (no action — dimension characterized):** all `mode` enums
reject typos at parse (pydantic); all int settings EXCEPT `MemcachedSettings.port`
are bounded; Redis SENTINEL + ES CLOUD required-when-mode enforced; round-6
SEC-1..SEC-7 TLS/scheme guards hold. So the gaps below are the REMAINING surface.

---

## SV1 — `Literal` enum types `[10 footguns, top leverage]`

**Rationale:** 10 free-form `str` fields hold values from a closed set but accept
any typo, failing at first backend RPC with an opaque client-lib error. `Literal`
converts these to parse-time rejections (the contract `mode` enums already enjoy).

**Files + fields:**
- `settings/kafka.py`: `security_protocol` (66, **H** — `"SAS_SSL"`/`"ssl"` typo), `sasl_mechanism` (70, **H** — lowercase `"plain"` silent auth fail), `compression_type` (141, M), `auto_offset_reset` (156, M).
- `settings/pulsar.py`: `consumer_type` (55, **H** — `"shared"` vs required `"Shared"`), `initial_position` (59, M).
- `settings/rabbitmq.py`: `ssl_verify_mode` (122, M — `"CERT_REQ"`), `cluster_node_type` (86, L — `"disk"` vs `"disc"`).
- `settings/mongodb.py`: `read_preference` (94, M), `auth_mechanism` (146, M).

**Fix:** `str` → `Literal[...]` with the closed set (values pulled from each
client lib's docs). Backward-compat: values currently in any valid config stay valid.

**TDD:** for each field, RED = construct with a typo (e.g. `KafkaSettings(security_protocol="SAS_SSL")`) succeeds today; GREEN = raises `ValidationError`. Honest pinning.

**Acceptance:** `uv run pytest tests/test_config.py` (+ new `test_settings_validation.py`) green; all 10 fields reject typos at parse.

**Leverage H · Effort S** (10 single-line type changes + tests; kills 10 footguns at once).

---

## SV2 — Mode-conditional `model_validator`s `[8 footguns, required-when-mode]`

**Rationale:** Only Redis SENTINEL + ES CLOUD enforce "mode X requires field Y".
Mongo REPLICA_SET/ATLAS, Redis CLUSTER/MASTER_SLAVE, Kafka CONFLUENT, RabbitMQ
CLUSTER/MIRRORED all silently fall through to a wrong default (standalone host,
PLAINTEXT localhost, no HA) instead of failing fast.

**Files + rules (mirror the existing Redis SENTINEL validator pattern):**
- `settings/mongodb.py`: REPLICA_SET → require `replica_set_name` (86, **H**); ATLAS → require `uri` has `mongodb+srv://` OR `atlas_cluster_name` (106, M).
- `settings/redis.py`: CLUSTER → require non-empty `cluster_startup_nodes` (164, M); MASTER_SLAVE → warn/require `replicas` (127, L).
- `settings/kafka.py`: CONFLUENT → require `confluent_api_key` + `confluent_secret` + `bootstrap_servers` (99, **H** — else silent PLAINTEXT-localhost fallback).
- `settings/rabbitmq.py`: CLUSTER/MIRRORED → require `cluster_nodes` (82, M); MIRRORED → require `ha_mode` (92, M).

**Fix:** `@model_validator(mode="after")` per settings class, raising `ConfigurationError(setting_name=..., ...)` with an actionable message (point at the missing field).

**TDD:** RED = construct `MongoDBSettings(mode=REPLICA_SET)` succeeds today; GREEN = `ConfigurationError`. Pin the message names the missing field.

**Acceptance:** each mode-conditional gap raises a named `ConfigurationError` at config time, not an opaque runtime error.

**Leverage H · Effort M** (~5 validators across 4 files + tests).

---

## SV3 — Cross-field auth/transport coherence `[6 footguns, 3H security]`

**Rationale:** Fields that must co-occur (or must NOT) have no cross-validation —
silent auth bypass or credential leak.

**Files + rules:**
- `settings/kafka.py`: SASL fields set → require `security_protocol` startswith `SASL_` (70-81, **H** — else credentials silently ignored, auth never attempted).
- `settings/pulsar.py`: `auth_token` set → require `service_url` is `pulsar+ssl://` (78/45, **H** — else token sent in cleartext).
- `settings/redis.py`: `ssl_enabled=True` → require `ssl_cafile` OR document self-signed path (179, M).
- `settings/mongodb.py`: `min_pool_size <= max_pool_size` (112/117, M — else deadlocks under load).
- `settings/elasticsearch.py`: `api_key` + (`username`,`password`) mutually exclusive (79-90, L — silent auth bypass).
- `settings/sqs.py` + `settings/dynamodb.py`: AWS creds both-or-neither (51/54, 54/57, M — round-6 SEC-7 did this for the connect path; **lift the same XOR into the settings validator** so it fires at config, not connect).

**Fix:** `@model_validator(mode="after")` cross-field checks; raise `ConfigurationError`.

**TDD:** RED = `PulsarSettings(auth_token="x", service_url="pulsar://...")` accepted; GREEN = `ConfigurationError`.

**Leverage H (security) · Effort M** (~6 cross-validators; the Pulsar-token-cleartext + Kafka-SASL are real credential-leak/ignore bugs).

---

## SV4 — URL/scheme format guards `[5 footguns, beyond round-6]`

**Rationale:** Round-6 SEC-4 guarded SQS/DynamoDB `endpoint_url` + SEC-3 ES
http+creds. The same shape is missing elsewhere — opaque `InvalidURI`/
`ValueError` at connect instead of a config hint.

**Files + rules:**
- `settings/mongodb.py`: `uri` must startswith `mongodb://` or `mongodb+srv://`; reject empty (62, **H**).
- `settings/pulsar.py`: `service_url` must startswith `pulsar://` or `pulsar+ssl://` (45, **H** — pairs with SV3 token rule).
- `settings/rocketmq.py`: `namesrv_address` must match `host:port` regex (30, M).
- `settings/elasticsearch.py`: each `hosts` entry must match `^(http|https)://` (67, M).
- `settings/sqs.py` + `settings/dynamodb.py`: `region_name` regex `^[a-z]{2}-[a-z]+-\d+$` (46/50, M — `"us-eat-1"` typo).

**Fix:** `@field_validator` (or `model_validator`) with the scheme/pattern check; raise `ConfigurationError`.

**TDD:** RED = `MongoDBSettings(uri="localhost:27017")` accepted; GREEN = `ConfigurationError`.

**Leverage M · Effort S-M** (~5 validators; mostly regex/startswith).

---

## SV5 — Empty-string + unbounded-int gaps `[5 footguns]`

**Rationale:** `host: str = Field(default="localhost")` accepts `""` → opaque DNS
failure. One unbounded int (`MemcachedSettings.port`) accepts `-1`/`99999`.

**Files + rules:**
- `settings/memcached.py`: `port` → `Field(default=11211, ge=1, le=65535)` (42, **H** — the ONLY unbounded int in the project).
- `settings/memcached.py`: `host` → `min_length=1` (41, M).
- `settings/redis.py` + `settings/rabbitmq.py`: `host` → `min_length=1` (redis 75, rabbitmq 54, L).
- `settings/base.py`: `retry_attempts` → add `le=20` sane cap (46, L — `999999` DoS); document `0` = no retries.

**Fix:** pydantic `Field` constraints (`min_length`, `ge`/`le`).

**TDD:** RED = `MemcachedSettings(port=-1)` accepted; GREEN = `ValidationError`.

**Leverage M · Effort S** (Field-constraint one-liners).

---

## Recommended next `/goal` execution (batching)

- **Round 9a (one executor, S effort, biggest leverage):** SV1 (`Literal` types) +
  SV5 (Field bounds). ~15 single-line changes, kills 15 footguns, no API break
  (values in valid configs unchanged). One test file `test_settings_validation.py`.
- **Round 9b (one executor, M effort):** SV2 (mode-conditional validators) +
  SV4 (URL/scheme guards). The "fail-fast at config" tier.
- **Round 9c (security lead, M effort):** SV3 (cross-field auth/transport) — the
  3 H-severity credential-leak/ignore bugs. Independent **security-reviewer**
  approval lane (this is the credential-safety cluster).

After 9a/9b/9c: the settings dimension is CLOSED (every field validated at config
time, no opaque runtime failures from bad input). This is a v1.0-readiness
contribution (F10 settings-reference doc + this validation layer = "users can
configure confidently without reading source").

## Non-goals (this SPEC)

- Executing SV1-SV5 (next `/goal`).
- The settings-reference DOCS (F10/U7 in SPEC-round8-tier1) — that's the discovery
  side; this is the validation side. Pair them in execution.
- Re-running round-8 agents (incremental contract; this dimension was genuinely new).
