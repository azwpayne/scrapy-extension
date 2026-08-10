# SPEC-round66 — ElasticSearch CLOUD validator raises ValueError, not ConfigurationError

## Context and audit evidence

Found via the R72 deep-insight scan (dim `settings-validators`), confirmed REAL
by an opus adversarial verifier (medium severity, **high** confidence), and
**independently re-verified by hand** against the current tree.

`ElasticSearchSettings.validate_mode_requirements`
(`src/scrapy_extension/settings/elasticsearch.py:277`, a `@model_validator(mode="after")`)
raises `ValueError` for the two CLOUD-mode misconfigs (lines 303 and 313):

```python
        if self.mode == ElasticSearchMode.CLOUD:
            if not self.cloud_id:
                msg = (
                    "ElasticSearch CLOUD mode requires 'cloud_id' to be set. "
                    f"Got cloud_id={self.cloud_id!r}."
                )
                raise ValueError(msg)                       # 303
            ...
            if not (has_api_key or has_basic_auth):
                msg = ("ElasticSearch CLOUD mode requires an auth method: ...")
                raise ValueError(msg)                       # 313
```

Every other backend's mode validator raises `ConfigurationError`, whose specific
message is preserved by the `RedactedBaseSettings` safe-list. `ValueError`,
however, is wrapped by pydantic into a `ValidationError`, which
`RedactedBaseSettings.__init__` catches on a separate branch and rebuilds via
`_redacted_validation_error` — **discarding the original message** and
substituting the generic `"Settings contain an invalid configuration value."`

So an operator misconfiguring ES CLOUD mode (missing `cloud_id`, or no auth)
sees a generic "Invalid configuration value." instead of the specific
"ElasticSearch CLOUD mode requires 'cloud_id'..." / "...requires an auth
method..." — forcing them to grep source to diagnose. This breaks the R14-B
invariant ("exception family is uniform across every settings-validation path").
Two existing tests (`test_elasticsearch_backend.py:179` and `:195`) lock in the
buggy symptom (`pytest.raises(ValidationError)`); newer CLOUD tests (`:209`,
`:221`) already assert `ConfigurationError`.

## Goal

Make ES CLOUD-mode validation raise `ConfigurationError` with a precise,
sanitization-surviving message — matching every other backend and honoring the
R14-B uniform-exception-family invariant.

## Specification

In `validate_mode_requirements` (`settings/elasticsearch.py:277`):

1. Change both `raise ValueError(msg)` (303, 313) to
   `raise ConfigurationError(msg, setting_name=...)` — `setting_name="cloud_id"`
   for the first, `"api_key"` for the second (the primary ES CLOUD auth method;
   the message lists both api_key and basic-auth options).
2. Drop the `f"Got cloud_id={self.cloud_id!r}."` interpolation from message 1
   (line 301-302) so it is a static, exactly-safe-listable string. The
   interpolated value is `None`/empty when the error fires (that *is* the error),
   so it adds no diagnostic value; making the message static is what lets it
   survive sanitization.
3. Update the docstring `Raises: ValueError` → `Raises: ConfigurationError`.

Safe-list both resulting static message strings in
`_SAFE_SETTINGS_CONFIGURATION_MESSAGES` (`src/scrapy_extension/settings/_redacted.py`)
so they survive the `RedactedBaseSettings` sanitization layer (R64/R65 lesson).

`validate_mode_requirements` is a `@model_validator(mode="after")` with no
backend caller (confirmed: only the model_validator invocation; the backend
revalidates transport security via separate helpers), so changing the exception
type affects only construction-time behavior. No public-API change.

## Plan and independently verifiable tasks

- **R66-1 — RED (test rewrite).** Update
  `test_cloud_mode_missing_id_fails_at_construction` (test_elasticsearch_backend.py:169)
  and `test_cloud_mode_without_auth_fails_at_construction` (:182) to assert
  `pytest.raises(ConfigurationError, match=...)` with the precise message
  ("requires 'cloud_id'" / "requires an auth method"), and drop the
  `from pydantic import ValidationError` imports. → verify: FAIL on current code
  (raises ValidationError with a generic sanitized message; wrong type + no match).
- **R66-2 — GREEN fix.** Change both `raise ValueError` → `raise ConfigurationError`
  (+ setting_name), make message 1 static (drop interpolation), update the
  docstring, and safe-list both messages. → verify: the R66-1 tests PASS.
- **R66-3 — no-regression.** The newer CLOUD tests (`:209`, `:221` — already
  ConfigurationError) stay green. `test_connect_cloud` (:150, a valid CLOUD
  config) still constructs. Full suite + `ruff check` + `mypy --strict` green.

## Acceptance criteria

1. CLOUD mode without `cloud_id` raises `ConfigurationError` (setting_name
   `cloud_id`) whose precise message survives sanitization.
2. CLOUD mode without auth raises `ConfigurationError` (setting_name `api_key`)
   whose precise message survives sanitization.
3. Valid CLOUD configs (`cloud_id` + auth) still construct (no regression).
4. No backend caller is affected (`validate_mode_requirements` is settings-only).
5. Gate green: `uv run ruff check .` + `uv run pytest` + `uv run mypy --strict
   src/scrapy_extension`.
6. One atomic commit, ff-merged to `main`; CI green.
