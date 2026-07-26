# Round 27 — PLAN: r26-empty-auth gap fix

> Spec: [SPEC-round27-r26-empty-auth.md](./SPEC-round27-r26-empty-auth.md).
> TDD (RED → GREEN), one atomic conventional commit. Claude-Code-only.

## R27-A — `settings/elasticsearch.py` empty-secret truthiness (LOW)

Replace the two `is not None` auth-presence checks with `bool(...)` so the
validators agree with `backends/elasticsearch.py:_build_kwargs` (which uses
truthiness) by construction. Three one-token edits, one defect class:

1. **`:192`** — `has_api_key = self.api_key is not None` → `bool(self.api_key)`.
   Closes the R26-F gap: `api_key=SecretStr("")` (env var set but unpopulated)
   no longer passes CLOUD fail-fast only to 401 anonymously at connect.

2. **`:193`** — `has_basic_auth = self.username is not None and self.password
   is not None` → `bool(self.username) and bool(self.password)`. Same gap for
   the basic-auth arm (`username=""` / `password=SecretStr("")`).

3. **`:225`** — `has_credential = self.api_key is not None or self.password is
   not None` → `bool(self.api_key) or bool(self.password)`. Sibling consistency
   (rule 7): `_validate_no_cleartext_credentials` currently false-positives on
   `http://` + `api_key=SecretStr("")`, blocking the legit no-auth-http dev
   config its own docstring (`:211`) permits.

### RED (write first, must fail before fix)

In `tests/test_elasticsearch_backend.py`, after the existing
`test_cloud_mode_without_auth_fails_at_construction` (R26-F):

- `test_cloud_mode_empty_api_key_fails_at_construction` — CLOUD + cloud_id +
  `api_key=""` → must raise `ValidationError` (currently passes validation →
  bug). Encodes WHY: an empty env-var value is the same operator error as an
  unset one, and must surface at the same fail-fast point.
- `test_cloud_mode_empty_basic_auth_fails_at_construction` — CLOUD + cloud_id
  + `username=""` + `password=""` → must raise `ValidationError`.
- `test_standalone_empty_api_key_http_not_blocked` — STANDALONE + `http://`
  host + `api_key=""` → must NOT raise the cleartext-credentials error (the
  docstring-permitted no-auth-http dev config). Asserts the false-positive is
  gone without weakening the real-cred http guard (covered by an existing
  SEC-3 test with a non-empty key).

### GREEN

Apply the three `bool(...)` edits. All three RED tests pass; no existing test
regresses (the R26-F no-auth test still raises — `None` is still falsy).

### Gates

`uv run ruff check` → `uv run mypy --strict src/scrapy_extension` →
`uv run pytest` (expect ≥3802 pass, ≥95% cov — R26 baseline 3802/95.03%).

### Reviewer

Claude-Code-only. Use `general-purpose`+opus or inline review (NOT
`agent-skills:code-reviewer` — it is GLM, not Claude; see memory
[[agent-skills-code-reviewer-is-glm-non-claude]]). Green gate = ruff+mypy+pytest
all clean; reviewer APPROVED or skipped-on-429.
