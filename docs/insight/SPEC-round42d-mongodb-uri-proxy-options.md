# Round 42D — SPEC / PLAN / TASK: MongoDB URI proxy policy

**Base:** `main` after `3d8aa69`.

## Audit conclusion

MongoDB URI parsing already rejects authentication and TLS query options but
currently accepts `proxyHost`, `proxyPort`, `proxyUsername`, and
`proxyPassword`.  PyMongo 4.17 rejects them today, so this is a fail-fast and
future-policy gap rather than a demonstrated active traffic bypass.

## Specification

1. Reject those four query option names case-insensitively, including
   semicolon-separated URI options and empty values.
2. Publish a fixed `ConfigurationError` for `uri`, never echoing a URI or
   option value.
3. Do not silently strip options or introduce typed proxy settings.
4. The existing connect-time revalidation must reject a mutated URI before
   `MongoClient` I/O.

## Plan and task checklist

- [ ] Add the proxy-option policy to the existing URI parser.
- [ ] Add construction and mutation-before-I/O redaction regressions.
- [ ] Run focused/full validation and make a standalone atomic commit.
