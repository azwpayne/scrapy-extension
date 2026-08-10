# SPEC-round67 — ElasticSearch blank-password error sanitized to generic (safe-list gap)

## Context and audit evidence

Found via the R75 deep-insight scan (dim `ndiff-regression`), confirmed REAL by
an opus adversarial verifier (low severity, **high** confidence), and
**independently re-verified by hand** against the current tree.

`ElasticSearchSettings._validate_auth_completeness`
(`src/scrapy_extension/settings/elasticsearch.py:219`) rejects a blank password
with `ConfigurationError("password must not be blank when supplied.",
setting_name="password")` (line 234-237). The two sibling fields in the **same
validator** raise analogous messages that ARE safe-listed:

- `"api_key must not be blank when supplied."` (line 227) → safe-listed at `_redacted.py:30`
- `"username must not be blank when supplied."` (line 231) → safe-listed at `_redacted.py:51`
- `"password must not be blank when supplied."` (line 235) → **NOT safe-listed** ❌

`RedactedBaseSettings.__init__` only preserves a `ConfigurationError` message
when it is an exact member of `_SAFE_SETTINGS_CONFIGURATION_MESSAGES`;
otherwise it substitutes the generic `"Settings contain an invalid
configuration value."`. So an operator who blanks `api_key` or `username` (e.g.
`SCRAPY_ELASTICSEARCH_USERNAME=`) sees the precise message, but one who blanks
`password` (e.g. `SCRAPY_ELASTICSEARCH_PASSWORD=`) sees only the generic message
and must grep source to diagnose.

This is the **same bug shape** (unsanitized ConfigurationError message) that
commit `130fb3c` (R74) established the R14-B invariant against for ES CLOUD
mode — but R74 safe-listed the two CLOUD-mode messages and overlooked the
password-blank message in the adjacent `_validate_auth_completeness` validator.
The ndiff-regression dim caught the gap (highest-ROI dim, as expected).

## Goal

Safe-list the password-blank message so it survives sanitization — restoring
message parity with its api_key/username siblings and honoring the R14-B
invariant across the whole `_validate_auth_completeness` validator.

## Specification

Add the exact static string `"password must not be blank when supplied."` to
`_SAFE_SETTINGS_CONFIGURATION_MESSAGES` in
`src/scrapy_extension/settings/_redacted.py`, adjacent to the existing
`"api_key must not be blank when supplied."` entry.

One line. No validation behavior change — the blank password is already
correctly rejected; only the operator-facing message precision is restored. No
public-API change. The message is static (no interpolation), so it is
exactly safe-listable (R64/R74 lesson: safe-list messages must be static).

## Plan and independently verifiable tasks

- **R67-1 — RED test.** Add a test to `tests/test_elasticsearch_backend.py`
  asserting `ElasticSearchSettings(username="user", password="   ")` raises
  `ConfigurationError` matching `"password must not be blank"` with
  `setting_name == "password"`. → verify: FAILS on current code (the message is
  sanitized to the generic "Settings contain an invalid configuration value.",
  so the `match=` fails).
- **R67-2 — GREEN fix.** Add `"password must not be blank when supplied."` to
  `_SAFE_SETTINGS_CONFIGURATION_MESSAGES`. → verify: the R67-1 test PASSES.
- **R67-3 — no-regression.** The existing blank-credential tests
  (`test_cloud_mode_empty_api_key_fails_at_construction` matching "api_key",
  `test_cloud_mode_empty_basic_auth_fails_at_construction` matching "username")
  stay green. Full suite + `ruff check` + `mypy --strict` green.

## Acceptance criteria

1. `ElasticSearchSettings(username="user", password="   ")` raises
   `ConfigurationError` whose precise message "password must not be blank when
   supplied." survives sanitization (not the generic message).
2. `setting_name == "password"` on that error.
3. The api_key/username sibling messages remain safe-listed (no regression).
4. Gate green: `uv run ruff check .` + `uv run pytest` + `uv run mypy --strict
   src/scrapy_extension`.
5. One atomic commit, ff-merged to `main`; CI green.
