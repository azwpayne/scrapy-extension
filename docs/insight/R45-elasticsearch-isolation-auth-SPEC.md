# R45 SPEC — Elasticsearch capability isolation and authentication boundary

## Finding

`ElasticSearchSettings` accepts blank or colliding `queue_index`,
`set_index`, and `storage_index` values. `ElasticSearchBackend.clear_storage()`
executes a match-all delete against `storage_index`; if that index is also the
queue or set index, an ordinary storage cleanup deletes unrelated durable
state.

The same settings model accepts host URLs with URL userinfo and accepts a
single or blank basic-auth field. The backend drops incomplete basic auth in
`_build_kwargs()`, so an operator can unintentionally connect anonymously.
It also permits `verify_certs=False` for authenticated standalone HTTPS
connections, exposing credentials to an active network attacker.

## Required behavior

1. Queue, set, and storage index names must each be non-blank and pairwise
   distinct at settings construction time.
2. Standalone hosts must be absolute `http` or `https` endpoints with a host,
   and must reject whitespace/control characters, URL userinfo, query strings,
   and fragments. Validation errors must not echo userinfo.
3. An API key, username, and password, when supplied, must be non-blank.
   Basic authentication requires both username and password; it remains
   mutually exclusive with API-key authentication.
4. Authenticated standalone connections require certificate verification.
   Anonymous HTTP remains valid for local development; cloud mode keeps its
   existing `cloud_id` + authentication contract.

## Acceptance criteria

- Every pairwise index collision and every blank index is rejected before the
  Elasticsearch client is constructed.
- Storage clear remains directed only to the configured, isolated storage
  index.
- Host userinfo, malformed authority, whitespace/control characters, query,
  and fragment are rejected without exposing a secret in the error.
- Partial/blank basic auth and blank API keys are rejected.
- Authenticated standalone TLS with `verify_certs=False` is rejected; valid
  authenticated verified HTTPS and anonymous local HTTP configurations stay
  valid.

## Non-goals

- No backend protocol, index mapping, retry, or Elasticsearch-client upgrade.
- No change to cloud connection parameter wiring, which does not pass the
  standalone `verify_certs` setting.
