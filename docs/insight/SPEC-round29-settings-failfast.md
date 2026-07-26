# Round 29 — SPEC: kafka runtime strip + mongodb/rocketmq name fail-fast

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_389a4695-579` (5-dim find + adversarial verify;
> 14 agents, 0 errors, ~2.4M tokens, ~12 min). Base: `main` @ `e5596a8` (post-R28).

## Headline

**7 raw → 7 confirmed, 0 refuted.** Highest yield since R25/R26 — rotating to the
under-audited mongodb/sqs/dynamodb/rocketmq/pulsar settings surfaces paid off. The
`r28-diff-regression` dimension caught the headline MED: **R28-C fixed the validator
side but missed the runtime side** — `backends/kafka.py:496` `_bootstrap_servers`
uses bare `or`, so a whitespace `confluent_bootstrap_servers` passes the R28-C
validator's `.strip()` semantics (with a real `bootstrap_servers`) but the runtime
returns the whitespace string → the exact opaque kafka-python connect error R28-C
exists to prevent. **9th consecutive round** the diff-regression dimension caught a
self-shipped gap — this time a gap IN R28-C itself (validator fixed, runtime missed).

## Scan result

**7 raw → 7 confirmed.** Per-dimension: `r28-diff-regression` 1→1, `mongodb-settings-deep`
4→4, `rocketmq-pulsar-settings` 2→2, `sqs-dynamodb-settings` EMPTY, `monitor-subsystem`
EMPTY.

The 2 EMPTY dims add to DO-NOT-RE-FLAG: sqs+dynamodb settings are sound (endpoint_url /
queue_name / aws-creds / region all validated); the monitor subsystem is sound
(thread-safety, sampling windows, hook robustness all correct).

## Ship set (3 logical units, 7 findings)

| ID | Sev | Surface | Defect (one line) |
|----|-----|---------|-------------------|
| **R28-C-1** | MED | `backends/kafka.py:496` | **R28-C self-gap (self-caught):** the runtime `_bootstrap_servers` resolver uses bare `or` (`confluent_bootstrap_servers or bootstrap_servers`), so a whitespace `confluent_bootstrap_servers` + real `bootstrap_servers` passes the R28-C validator (which `.strip()`s) but the runtime returns the whitespace string → opaque kafka-python connect error. Validator-runtime mismatch. |
| **R29-A** | MED | `settings/mongodb.py:39` | `validate_mongodb_collection_domains` checks type + distinctness but NOT non-empty → `('', 'sets', 'storage')` passes (`SCRAPY_MONGO_QUEUE_COLLECTION=` empty) → opaque pymongo `InvalidName` at connect. Covers settings + backend (both call the helper). |
| **R29-B** | MED | `settings/mongodb.py:202` | `replica_set_members` / `mongos_routers` lists have no element-level validator → empty/whitespace elements build a malformed `mongodb://` URI → opaque `InvalidURI` at connect. |
| **R29-C** | MED | `settings/mongodb.py:178` | `database` field has no validator / `min_length` → empty/whitespace passes → opaque pymongo `InvalidName` at `_initialize_collections`. |
| **R29-D** | LOW | `settings/mongodb.py:448` | REPLICA_SET validator uses bare truthiness (`not self.replica_set_name`) → whitespace `"  "` bypasses it (`not "  "` is False) → opaque discovery error with `replicaSet='  '`. |
| **R27-RMQ-1** | LOW | `settings/rocketmq.py:147` | `max_message_size` uses `ge=0` → zero accepted at config time → every non-empty push fails (backend unusable). |
| **R27-RMQ-2** | LOW | `settings/rocketmq.py:143` | `consumer_group` has no non-empty validator → empty string accepted → opaque client error inside `SimpleConsumer` at connect. |

## Root cause (common thread)

All 7 are the same meta-defect the R26–R28 settings-fail-fast theme has been closing:
**a name/endpoint field accepts a set-but-empty or set-but-whitespace value at config
time, surfacing as an opaque client-lib error at connect().** R28-C-1 is the one
runtime-side instance (validator already `.strip()`-aware; runtime not). The mongodb
cluster (A/B/C/D) is the mongodb instance of the pattern ES/kafka closed earlier. The
rocketmq pair (RMQ-1/2) extends it to numeric-floor + name fields.

## Fixes (minimal, TDD)

- **R28-C-1:** `backends/kafka.py:496` → `return (self.config.confluent_bootstrap_servers or "").strip() or self.config.bootstrap_servers`. One line; realigns runtime with the R28-C validator's `.strip()` semantics.
- **R29-A:** `settings/mongodb.py:39` add `if not all(name and name.strip() for name in validated_names): raise ConfigurationError(...)` after the type check. Covers backend too (shared helper).
- **R29-B:** add `@field_validator("replica_set_members", "mongos_routers")` rejecting empty/whitespace elements.
- **R29-C:** add `@field_validator("database")` rejecting empty/whitespace.
- **R29-D:** `settings/mongodb.py:448` strip before truthiness: `name_set = bool(self.replica_set_name) and bool(self.replica_set_name.strip())`.
- **R27-RMQ-1:** `settings/rocketmq.py:147` `ge=0` → `gt=0`.
- **R27-RMQ-2:** `consumer_group` add `min_length=1` (or field_validator rejecting whitespace).

## DO-NOT-RE-FLAG additions after R29

- kafka `_bootstrap_servers` runtime is `.strip()`-aware, matching the R28-C validator (R28-C-1).
- mongodb collection names / database / replica_set_members / mongos_routers / replica_set_name reject empty/whitespace (R29-A/B/C/D).
- rocketmq `max_message_size` is `gt=0`; `consumer_group` non-empty (R27-RMQ-1/2).
- sqs+dynamodb settings sound (EMPTY). monitor subsystem sound (EMPTY) — thread-safety, sampling, hooks all correct.

## Frontier note

R24=0, R27=1, R28=3, **R29=7**. The frontier is NOT empty — rotating to under-audited
backends (mongodb/rocketmq) yielded a 4-finding mongodb cluster + 2 rocketmq edges +
the diff-regression catch. The settings-fail-fast theme still has legs; cadence
justified. Next surfaces to rotate: pulsar service_url/auth (rocketmq-pulsar dim only
partially mined), rabbitmq vhost/exchange-name, redis key-prefix validators.
