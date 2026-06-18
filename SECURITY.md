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
`rocketmq-client-python`) or Scrapy itself — report those to their respective
projects. `scrapy-extension` handles secrets via pydantic `SecretStr` and
redacts them in exceptions (see `docs/code-review-2026-06-15.md`, Rounds 13,
26–28); credential-handling bugs in this package are in scope.
