# SPEC-round68 — Complete the R14-B settings-message safe-list (static messages)

> Back-nav: [../insight index](LEDGER.md). Related: SPEC-round66 (R74 ES CLOUD),
> SPEC-round67 (R75 ES blank-password). Fire: R76.

## Context and audit evidence

**R14-B invariant** (established R74 `130fb3c`, R75 `cf47a10`): every
`ConfigurationError` raised during construction of a trusted `RedactedBaseSettings`
subclass must carry an **exact static message string** that is a member of
`_SAFE_SETTINGS_CONFIGURATION_MESSAGES` (`src/scrapy_extension/settings/_redacted.py`).
Otherwise `RedactedBaseSettings.__init__` (`_redacted.py:180-200`) substitutes the
generic `"Settings contain an invalid configuration value."` and the operator loses the
specific diagnostic — forced to source-grep to diagnose a misconfig.

R74/R75 closed the **ElasticSearch** gaps only (2 CLOUD-mode messages + the blank-password
message). R76's ndiff-regression scan (5-dim ultracode Workflow, 26 opus agents, 15
adversarially-confirmed findings) found the **same invariant violated across base + Redis +
MongoDB + Kafka + RabbitMQ + RocketMQ + ElasticSearch + Memcached + Pulsar**.

An AST enumeration of every `raise ConfigurationError(...)` in
`src/scrapy_extension/settings/*.py` (resolving the first positional arg to a static
string literal; excluding `_redacted.py` sanitizer internals; deduplicating) found
**119 unique static messages currently sanitized to generic**.

Examples (each empirically reproduced by adversarial verifiers):

| Input | Now (sanitized) | Should be (preserved) |
|---|---|---|
| `Settings(backend_type=123)` | generic | `Selected backend type is not a registered backend type.` |
| `RedisSettings(mode=CLUSTER, db=1)` | generic | `Redis Cluster supports only database 0; use namespace for isolation.` |
| `MongoDBSettings(username=" ")` | generic | `MongoDB 'username' must be non-empty.` |
| `RabbitMQSettings(url="amqp://u:p@h", username="u", password="p")` | generic | `RabbitMQ URL userinfo is not allowed; use explicit credential settings.` |
| `KafkaSettings(acks=True)` | generic | `Kafka acks must be 1 or 'all', not a boolean.` |

## Goal

Close the R14-B invariant for **all static** settings messages in one atomic change, and
make it **self-enforcing** so no future validator message can silently regress to generic.

## Specification

1. **Add the 119 exact static strings** (AST-enumerated, deduplicated, sorted) to
   `_SAFE_SETTINGS_CONFIGURATION_MESSAGES` in `src/scrapy_extension/settings/_redacted.py`.
   Purely additive — **no validator behavior changes** (the messages are already raised
   identically; only their preservation through the sanitization boundary is restored).
2. **Add a contract test** (`tests/test_settings_message_safelist_contract.py`) that
   AST-scans `src/scrapy_extension/settings/*.py` (excluding `_redacted.py`), resolves
   every `raise ConfigurationError(...)` first positional arg to a static literal, and
   asserts each is a member of `_SAFE_SETTINGS_CONFIGURATION_MESSAGES`. This is the
   RED→GREEN driver **and** the permanent enforcer.
3. **Add representative functional tests** proving the runtime survival mechanism across
   backends not previously covered for message-survival (base, redis, mongodb, kafka,
   rabbitmq — ES was R74/R75).
4. **DEFER (out of scope this round, documented)**: 19 f-string message sites (interpolate
   field/setting names — e.g. `redis.py` `f"Redis setting '{setting_name}' requires
   mode='sentinel'."`, `_aws.py` region/profile interpolations). These cannot be
   exact-safe-listed without enumerating each interpolated output or refactoring to a static
   message + `setting_name`. Tracked as a follow-up round.

## Plan and independently verifiable tasks

- **R68-1 (RED)**: Write `tests/test_settings_message_safelist_contract.py` with the AST
  completeness test. Run it — it **FAILS**, listing the 119 missing strings.
- **R68-2 (GREEN)**: Add the 119 exact static strings to `_SAFE_SETTINGS_CONFIGURATION_MESSAGES`.
  Re-run the contract test — **PASSES**.
- **R68-3 (hardening)**: Add functional survival tests for base/redis/mongodb/kafka/rabbitmq
  representative cases (assert precise message, not generic).
- **R68-4 (gate)**: `ruff check .`, `pytest`, `mypy --strict src/scrapy_extension` all green.
- **R68-5 (ship)**: atomic commit + ff-merge to `main`; CI green.

## Acceptance criteria

- The contract test passes: every static `ConfigurationError` first-arg in
  `settings/*.py` (excl. `_redacted.py`) is in `_SAFE_SETTINGS_CONFIGURATION_MESSAGES`.
- A representative functional test per backend asserts the precise message survives
  sanitization (not the generic).
- `ruff check`, `pytest`, `mypy --strict` all green; CI on `main` green.
- No change to validator behavior (messages raised identically; only preservation restored).
- No dirty file touched (`_redacted.py` + new test file are clean — not in the dirty list).
