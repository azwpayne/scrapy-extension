# Security Policy

## Supported versions

`scrapy-extension` is pre-1.0 (`0.1.x`). Only the **latest release** receives
security fixes.

| Version | Supported |
|---|---|
| latest `0.1.x` | ✅ |
| older `0.1.x` | ❌ |

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue.

- **GitHub Security Advisory** (preferred): use
  [Report a vulnerability](https://github.com/azwpayne/scrapy-extension/security/advisories/new)
  (private disclosure to the maintainer).
- **Email**: paynewu0719@gmail.com

Include: the affected version, a minimal reproduction, and the impact. You'll
receive an acknowledgement and a fix/coordination plan.

## Scope

This policy covers the `scrapy-extension` package itself — its backend
abstractions, connection management, request/item serialization, and Scrapy
component integration.

It does **not** cover the underlying backend client libraries
(`redis`, `pymongo`, `kafka-python`, `pika`, `elasticsearch`,
`rocketmq-python-client`, `pulsar-client`, `boto3`, `pymemcache`) or Scrapy
itself — report those to their respective projects. `scrapy-extension`
handles secrets via pydantic `SecretStr` and redacts them in exceptions (see
`docs/code-review-2026-06-15.md`, Rounds 13, 26–28); credential-handling
bugs in this package are in scope.

## Response SLA

- **Acknowledgement:** within 3 business days.
- **Initial assessment + severity:** within 7 business days.
- **Fix or mitigation:** target 30 days for `High`/`Critical`, 90 days for
  `Medium`/`Low`. Coordinated disclosure timing is honored on request.

A fix is released on the earliest affected minor or patch line; a public
advisory is published alongside the release.

## Built-in security controls

`scrapy-extension` ships with several layers of defense that downstream users
should be aware of. Each is enforced in code; this list exists so operators
can audit and rely on it.

### Credential redaction

Every password / SASL token / API key that flows through a backend's config
builder is wrapped in `_RedactedStr`
(`src/scrapy_extension/backends/_redaction.py`). `_RedactedStr.__repr__` and
`__str__` return `***` instead of the raw value, so credentials do not leak
via:

- `repr(backend.config)`
- Logs emitted by the backend or its caller
- Tracebacks printed when a backend operation fails

This is defense-in-depth; it does **not** relieve callers of the duty to
keep their own logs free of raw credentials.

### TLS / scheme guards (round-6 SEC-1..7 + round-9 SV3/SV4)

The settings layer rejects insecure transport configurations at startup,
before any network call. Each guard raises `ConfigurationError` with
`setting_name` and `setting_value` context attributes.

| Guard | Where | What it catches |
|---|---|---|
| SEC-1 | `backends/_redaction.py` | credential leakage in repr/str/logs |
| SEC-2 | `settings/mongodb.py` `_validate_tls_insecure_not_in_production_mode` | `tls_allow_invalid_certificates=True` in production modes (disables cert verification) |
| SEC-3 | `settings/elasticsearch.py` | credentials sent over cleartext `http://` |
| SEC-4 | `settings/{sqs,dynamodb}.py` `_validate_endpoint_url_scheme` | `endpoint_url` without `http://` or `https://` scheme |
| SV4 | `settings/{mongodb,pulsar,rocketmq,elasticsearch,sqs,dynamodb}.py` | malformed URLs / missing scheme on host fields |
| SV3 | `settings/{kafka,pulsar,redis,mongodb,elasticsearch,sqs,dynamodb}.py` | cross-field auth/transport incoherence — e.g. SASL username without password, TLS cert without key, mismatched auth mode |
| SEC-7 | `backends/connectors.py` connect path | AWS credential XOR — half-configured `aws_access_key_id` / `aws_secret_access_key` caught before connect |
| round-9 | `settings/{sqs,dynamodb}.py` `_validate_aws_credentials_both_or_neither` | both-or-neither AWS credential enforcement at config time (config-path sibling of SEC-7's connect-path check) |

### Input validation

- Queue / set / index names validated against `^[a-zA-Z0-9._:-]+$`
  (`backends/base.py:170`).
- Topic names (Kafka) validated against a stricter subset.
- Injection-shaped names are rejected before use.

### No code execution on the data path

Serialization is JSON only via `JSONSerializer` (`backends/base.py:131`).
Unknown types raise `TypeError` with a clear message rather than being
silently `str()`-ed. There is no `pickle`, `eval`, `exec`, or `marshal` on
the request / item path. Callers who supply a custom `Serializer`
implementation are responsible for its safety.

### Ack safety under concurrency

For message-queue backends (Kafka, RabbitMQ), the scheduler's
`from_settings` gate refuses to start under `CONCURRENT_REQUESTS > 1` with a
backend that does not support concurrent ack
(`supports_concurrent_ack=False` — SQS, Pulsar single-slot ack), unless the
explicit `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS` opt-out is set. This
prevents the "pop N, ack last" footgun where N-1 messages are silently
unacked.

## Supply-chain notes

One bundled backend currently carries a known supply-chain caveat:

- **RocketMQ** — uses Apache `rocketmq-python-client>=5.1.1,<6`, the
  maintained pure-Python gRPC client. The old unmaintained
  `rocketmq-client-python`/native-client risk is no longer part of the
  supported dependency path. RocketMQ is still queue-only: Set/Storage are
  rejected at config time (`ConfigurationError`) and guard classes fail fast if
  the capability gate is bypassed.
- **Memcached** — depends on `pymemcache==4.0.0`, unmaintained (last release
  2022-10-17). Marked Experimental in [`STABILITY.md`](STABILITY.md);
  tracked as U20.

See [`STABILITY.md`](STABILITY.md) for the per-backend maturity tiers.
