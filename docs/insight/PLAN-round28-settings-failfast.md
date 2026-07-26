# Round 28 — PLAN: settings fail-fast gaps (3 units)

> Spec: [SPEC-round28-settings-failfast.md](./SPEC-round28-settings-failfast.md).
> TDD (RED → GREEN), each unit = one atomic conventional commit. Claude-Code-only.

## R28-A — ES auth-exclusivity truthiness (MED, headline, self-caught)

`settings/elasticsearch.py:268-283` `_validate_auth_method_exclusivity` still
uses `is not None` — R27-A's stated "all three sites" claim was wrong (only 2/3
patched). Mirror R27-A exactly:

- `:268` `if self.api_key is None:` → `if not self.api_key:`
- `:270` `if self.username is not None or self.password is not None:` → `if bool(self.username) or bool(self.password):`
- `:277`/`:279` display f-string `is not None` → truthiness (display accuracy).

### RED
- `test_exclusivity_empty_api_key_with_basic_auth_accepted` — CLOUD + cloud_id +
  `api_key=""` + valid `username`/`password` → must NOT raise (currently raises
  "mutually exclusive"). Encodes WHY: `_build_kwargs` drops the empty key and
  uses basic_auth, so the config is valid.
- `test_exclusivity_real_api_key_with_empty_password_accepted` — real `api_key` +
  `password=""` → must NOT raise (`_build_kwargs` uses the api_key; empty password
  is harmless). Verifier confirmed this symmetric case.
- Keep the existing both-real `test_*_mutually_exclusive` test asserting it still
  raises (regression guard for the truthiness change).

## R28-B — ES STANDALONE empty hosts rejected (LOW)

`settings/elasticsearch.py:_validate_hosts_scheme` add a non-empty guard at the
top for STANDALONE mode (CLOUD uses cloud_id, not hosts):

```python
if self.mode == ElasticSearchMode.STANDALONE and not self.hosts:
  raise ConfigurationError("STANDALONE mode requires at least one hosts entry ...")
```

### RED
- `test_standalone_empty_hosts_rejected` — `mode=STANDALONE, hosts=[]` → must
  raise ConfigurationError (currently passes → opaque client error at connect).
- CLOUD + `hosts=[]` must still pass (hosts unused in CLOUD).

## R28-C — kafka CONFLUENT empty/whitespace endpoint rejected (LOW)

`settings/kafka.py:462-471` tighten both effective-value checks:

```python
if self.mode == KafkaMode.CONFLUENT and not (self.confluent_bootstrap_servers or "").strip():
  if (self.bootstrap_servers or "").strip() in ("", "localhost:9092"):
    raise ConfigurationError(...)  # extend message to name empty/whitespace too
```

### RED
- `test_confluent_empty_bootstrap_servers_rejected` — CONFLUENT + unset
  `confluent_bootstrap_servers` + `bootstrap_servers=""` → must raise.
- `test_confluent_whitespace_bootstrap_servers_rejected` — `bootstrap_servers="   "`
  → must raise.
- Keep R26-E's existing `test_confluent_localhost_default_rejected` passing
  (localhost:9092 still rejected — `in ("", "localhost:9092")`).

## Gates

`uv run ruff check` → `uv run mypy --strict src/scrapy_extension` → `uv run pytest`
(target ≥3808 pass / ≥95% — R27 baseline 3805 + ~5 new tests; use
`UV_CACHE_DIR=$TMPDIR/uv-cache` + sandbox off for the loopback/socket artifacts).

## Reviewer

Claude-Code-only. Inline review or `general-purpose`+opus (NOT `agent-skills:code-reviewer`
— GLM, see [[agent-skills-code-reviewer-is-glm-non-claude]]). Green gate =
ruff+mypy+pytest clean. R28-A is the headline (self-caught); verify the truthiness
change doesn't weaken the existing both-real exclusivity test.
