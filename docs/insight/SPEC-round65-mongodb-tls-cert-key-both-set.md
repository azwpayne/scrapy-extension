# SPEC-round65 — MongoDBSettings does not reject both tls_cert_file and tls_key_file

## Context and audit evidence

Found via the R72 deep-insight scan (dim `settings-validators`), confirmed REAL
by an opus adversarial verifier (medium severity, **high** confidence), and
**independently re-verified by hand** against the current tree.

`MongoDBSettings` exposes `tls_cert_file` and `tls_key_file` as independent
`Optional` fields (`src/scrapy_extension/settings/mongodb.py:947` and `:951`)
with **no validator rejecting the both-set case**. PyMongo's client takes a
SINGLE `tlsCertificateKeyFile` kwarg (a combined cert+key PEM), so the backend
treats the two fields as **alternatives, not a pair**
(`src/scrapy_extension/backends/mongodb.py:662-665`):

```python
        if snapshot.tls_cert_file:
            kwargs["tlsCertificateKeyFile"] = snapshot.tls_cert_file
        if snapshot.tls_key_file and not snapshot.tls_cert_file:
            kwargs["tlsCertificateKeyFile"] = snapshot.tls_key_file
```

When BOTH are set, `tls_cert_file` wins (line 662-663) and `tls_key_file` is
**silently dropped** (the second `if` is False). A user who has separate
`cert.pem` + `key.pem` files and sets both fields (mirroring Redis/Kafka's
cert+key PAIR model) gets `tlsCertificateKeyFile = cert.pem` only — if that
file does not also contain the key, mTLS auth fails at the broker with no local
signal. This is the OPPOSITE of Kafka's pair requirement (R64): Kafka requires
cert+key as a pair (both or neither); MongoDB requires them as alternatives
(exactly one, because PyMongo uses one combined PEM).

## Goal

Reject the both-set misconfiguration at settings construction, so it surfaces
with a precise, actionable error explaining MongoDB's combined-file model —
instead of silently dropping the key and failing opaquely at the broker.

## Specification

Add the both-set rejection to `_validate_authentication_and_transport_security`
(`settings/mongodb.py:1085`), the `@model_validator(mode="after")` that already
groups TLS concerns. Gate it on the same TLS-active condition the backend uses
to consume the fields (`backends/mongodb.py:657`:
`tls_enabled or mode is ATLAS`) so the check fires only when the silent-drop
would actually occur:

```python
        validate_mongodb_transport_security(
            mode=self.mode,
            uri=self.uri,
            replica_set_members=self.replica_set_members,
            mongos_routers=self.mongos_routers,
            tls_enabled=self.tls_enabled,
            tls_allow_invalid_certificates=self.tls_allow_invalid_certificates,
            username=self.username,
            password=self.password,
            auth_mechanism=self.auth_mechanism,
            auth_source=self.auth_source,
            allow_remote_plaintext=self.allow_remote_plaintext,
        )
        if (
            self.tls_enabled or self.mode is MongoDBMode.ATLAS
        ) and self.tls_cert_file is not None and self.tls_key_file is not None:
            raise ConfigurationError(
                "MongoDB TLS uses a single combined certificate+key file "
                "(tlsCertificateKeyFile); set tls_cert_file OR tls_key_file, "
                "not both -- setting both silently drops the key.",
                setting_name="tls_key_file",
            )
        return self
```

Additionally, safe-list that exact message string in
`_SAFE_SETTINGS_CONFIGURATION_MESSAGES` (`src/scrapy_extension/settings/_redacted.py`)
so it survives the `RedactedBaseSettings` sanitization layer (R64 lesson) —
otherwise the operator sees the generic "Settings contain an invalid
configuration value."

The backend (`backends/mongodb.py`) is NOT touched: the silent-drop behavior
stays as the runtime fallback; the validator rejects the misconfig at
construction before the backend ever sees both fields. No public-API change.

## Plan and independently verifiable tasks

- **R65-1 — RED test.** Add a test (in a clean mongodb settings test location,
  mirroring `tests/test_kafka_transport_security.py` from R64) asserting
  `MongoDBSettings(tls_enabled=True, tls_cert_file=X, tls_key_file=Y)` raises
  `ConfigurationError` with `setting_name == "tls_key_file"` and the precise
  message. → verify: FAILS on current code (settings construct successfully;
  no exception).
- **R65-2 — GREEN fix.** Add the both-set rejection to
  `_validate_authentication_and_transport_security` + safe-list the message.
  → verify: the R65-1 test PASSES.
- **R65-3 — no-regression.** Positive tests: cert-only (`tls_cert_file` set,
  `tls_key_file` None), key-only (the mirror), and neither — all under
  `tls_enabled=True` — still construct OK. Existing mongodb settings/backend
  tests (which exercise cert-only and key-only paths) stay green. Full suite +
  `ruff check` + `mypy --strict` green.

## Acceptance criteria

1. Under TLS (`tls_enabled=True` or `mode=ATLAS`), setting BOTH `tls_cert_file`
   and `tls_key_file` raises `ConfigurationError` at construction with
   `setting_name="tls_key_file"` and the precise message surviving sanitization.
2. Cert-only, key-only, and neither (under TLS) still construct successfully
   (no regression).
3. Without TLS (`tls_enabled=False`, not ATLAS), both-set does NOT raise (the
   fields are ignored by the backend anyway) — gated precisely on TLS-active.
4. `backends/mongodb.py` is not touched.
5. Gate green: `uv run ruff check .` + `uv run pytest` + `uv run mypy --strict
   src/scrapy_extension`.
6. One atomic commit, ff-merged to `main`; CI green.
