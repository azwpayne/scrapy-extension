# SPEC-round64 — KafkaSettings does not validate ssl_certfile/ssl_keyfile as a pair

## Context and audit evidence

Found via the R72 deep-insight scan (dim `settings-validators`), confirmed REAL
by an opus adversarial verifier (medium severity, **high** confidence), and
**independently re-verified by hand** against the current tree.

`KafkaSettings` exposes `ssl_certfile` and `ssl_keyfile` as independent
`Optional` fields (`src/scrapy_extension/settings/kafka.py:336` and `:340`) with
**no validator enforcing mutual presence**. A user can set
`security_protocol="SSL"` (or `"SASL_SSL"`, or `mode=CONFLUENT`) with
`ssl_certfile` but no `ssl_keyfile`, or vice-versa, and the settings construct
successfully — surfacing later as an opaque connect-time failure.

This is an asymmetry with the two sibling queue backends that use TLS client
auth, both of which enforce the identical XOR check:

- `validate_redis_transport_security` (`settings/redis.py:217`):
  `if (ssl_certfile is None) != (ssl_keyfile is None):` →
  `"Redis TLS client authentication requires both certificate and key files."`
- the RabbitMQ equivalent (`settings/rabbitmq.py:187`): same check, same message
  shape (`"RabbitMQ TLS client authentication requires both certificate and key files."`).

The key-without-cert case is genuinely silent: when `ssl_certfile is None`,
kafka-python skips `load_cert_chain` entirely, so no client certificate is
presented and mTLS auth fails at the broker with no local signal. Redis and
RabbitMQ reject this at construction; Kafka does not.

## Goal

Reject an incomplete Kafka mTLS client-cert/key pair at settings construction,
mirroring Redis and RabbitMQ — so the misconfiguration surfaces with a precise,
actionable error instead of an opaque broker-side mTLS failure.

## Specification

Add the XOR check to `KafkaSettings._validate_authentication`
(`settings/kafka.py:498`), the `@model_validator(mode="after")` that already
groups TLS concerns (it calls `validate_kafka_transport_security`). Gate it on
`uses_tls` (the same expression `validate_kafka_transport_security` uses at
`:191`) so server-auth-only TLS (both fields `None`) and non-TLS configs are
unaffected:

```python
        validate_kafka_transport_security(
            self.mode, self.security_protocol, self.ssl_check_hostname
        )
        uses_tls = (
            self.security_protocol in {"SSL", "SASL_SSL"}
            or self.mode == KafkaMode.CONFLUENT
        )
        if uses_tls and (self.ssl_certfile is None) != (self.ssl_keyfile is None):
            missing_name = (
                "ssl_keyfile" if self.ssl_certfile is not None else "ssl_certfile"
            )
            raise ConfigurationError(
                "Kafka TLS client authentication requires both certificate and "
                "key files.",
                setting_name=missing_name,
            )
        return self
```

**Why inline and not in `validate_kafka_transport_security`:** that function is
also called from `src/scrapy_extension/backends/kafka.py:659` (runtime
revalidation before SDK I/O). `backends/kafka.py` is in the user's uncommitted
dirty tree, so changing the function's *signature* (to accept cert/key) is not
shippable without touching the dirty file. The inline check in the
settings-only model_validator adds the validation at construction with no
signature change and no touch to `backends/kafka.py`. (The one-line `uses_tls`
duplication is accepted as the lesser evil vs. a signature change.)

The message mirrors Redis/RabbitMQ verbatim ("Kafka TLS client authentication
requires both certificate and key files.") for grep-consistency. No public-API
change.

Additionally, add that exact message string to `_SAFE_SETTINGS_CONFIGURATION_MESSAGES`
in `src/scrapy_extension/settings/_redacted.py` so the precise text survives the
`RedactedBaseSettings` sanitization layer — otherwise the operator sees the
generic "Settings contain an invalid configuration value." instead. (Redis and
RabbitMQ's identical messages are NOT currently safe-listed — a pre-existing
latent gap where their operators also see the generic message; out of scope for
this finding, which fixes Kafka only.) There is precedent: the safe-list already
carries four Kafka messages (CONFLUENT creds, ssl_check_hostname, broker
endpoints, SASL).

## Plan and independently verifiable tasks

- **R64-1 — RED test.** Create `tests/test_kafka_transport_security.py`
  (mirroring `tests/test_rabbitmq_transport_security.py:164`
  `test_tls_client_certificate_must_be_a_pair`): a parametrized test asserting
  `KafkaSettings(security_protocol="SSL", ssl_certfile=X, ssl_keyfile=None)` and
  the mirror raise `ConfigurationError` with `setting_name` pointing at the
  missing field. → verify: FAILS on current code (settings construct
  successfully; no exception).
- **R64-2 — GREEN fix.** Add the inline XOR check to `_validate_authentication`.
  → verify: the R64-1 test PASSES.
- **R64-3 — no-regression.** Add a positive test: `KafkaSettings(security_protocol="SSL", ssl_certfile=X, ssl_keyfile=Y)` (both set — mTLS) constructs OK, and
  `KafkaSettings(security_protocol="SSL")` (both None — server-auth-only TLS)
  still constructs OK. Existing Kafka TLS tests
  (`test_kafka_connection_snapshot.py` constructs `KafkaSettings(security_protocol="SSL")` with no cert/key) stay green. Full suite + `ruff check`
  + `mypy --strict` green.

## Acceptance criteria

1. Under TLS (`SSL`/`SASL_SSL`/`CONFLUENT`), setting exactly one of
   `ssl_certfile`/`ssl_keyfile` raises `ConfigurationError` at construction with
   `setting_name` = the missing field.
2. Both set (mTLS) and both None (server-auth-only TLS) still construct
   successfully (no regression).
3. Non-TLS configs (`PLAINTEXT`) are unaffected regardless of cert/key presence.
4. `validate_kafka_transport_security`'s signature is unchanged (no touch to the
   dirty `backends/kafka.py`).
5. Gate green: `uv run ruff check .` + `uv run pytest` + `uv run mypy --strict
   src/scrapy_extension`.
6. One atomic commit, ff-merged to `main`; CI green.
