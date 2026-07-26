# Round 27 — SPEC: r26-empty-auth gap (self-caught via diff-regression)

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_b4a74703-e70` (5-dim find + adversarial verify;
> 6 agents, 0 errors, ~1.25M tokens, ~12 min). Base: `main` @ `6cf3066` (post-R26).

## Headline

**1 raw → 1 confirmed, 0 refuted. 4/5 dims EMPTY.** The frontier-thinning trend
noted in R24/R25 continues — code-defect ROI is dropping. The single survivor is
a genuine gap in my **own R26-F work**, caught by the `r26-diff-regression`
safety-net dimension — the dimension that has now caught a self-shipped
regression / incompleteness in **7 consecutive rounds** (R17-B, R18-C, R19-B,
R23-A, R25-A's test shift, R26-A's snapshot cap, now R26-F's auth check). It
remains the single highest-ROI dimension and must never be dropped.

The headline is LOW severity (requires a specific operator misconfiguration:
env var set but unpopulated, e.g. `SCRAPY_ELASTICSEARCH_API_KEY=""` in CI), but
it is a real fail-fast promise gap with a trivial two-token fix, and it closes a
real inconsistency between the validator and the backend's `_build_kwargs`.

## Scan result

**1 raw → 1 confirmed.** Per-dimension: `r26-diff-regression` 1→1 confirmed,
`connectors-concurrency` EMPTY, `registry-entrypoint` EMPTY,
`circuit-breaker-statemachine` EMPTY, `batched-storage-edge` EMPTY.

The 4 EMPTY dimensions confirm (no re-flag): the 2-layer generation fence in
`connectors.py` is sound (R13/R18), the registry entry-point loading +
descriptor shape validation is correct, the circuit-breaker state machine
beyond the R-fire3 `_record_success` fix is correct, and the batched-storage
edge cases beyond R22-B/R23-A are correct.

## Ship set (1 unit)

| ID | Sev | Surface | Defect (one line) |
|----|-----|---------|-------------------|
| **A** | LOW | `settings/elasticsearch.py:192-193` (+ sibling `:225`) | **R26-F incompleteness (self-caught):** the CLOUD-mode auth check uses `is not None`, so a set-but-empty `api_key=SecretStr("")` / `username=""` / `password=SecretStr("")` passes fail-fast validation — but `_build_kwargs` (`backends/elasticsearch.py:83-85`) uses truthiness, so the empty secret is treated as "no auth", the client is constructed anonymously, and Elastic Cloud 401s the ping → the exact opaque `BackendConnectionError('health check returned false')` R26-F was added to surface at config time. Sibling: `_validate_no_cleartext_credentials` (`:225`) has the same `is not None` pattern and false-positives on `http://` + empty key (its own docstring at `:211` allows `http://` with no creds). |

## Root cause (verified end-to-end)

- `settings/elasticsearch.py:192-193` — `has_api_key = self.api_key is not None`;
  `has_basic_auth = self.username is not None and self.password is not None`.
  `SecretStr("")` / `""` are non-None → both checks True → validation passes.
- `backends/elasticsearch.py:83` — `if self.config.api_key:` — pydantic v2
  `SecretStr` defines `__len__`, so `SecretStr("")` is **falsy** → api_key arm
  skipped; `:85` `elif self.config.username and self.config.password:` — empty
  `username` is falsy → basic_auth arm skipped → **no auth added to kwargs**.
- `backends/elasticsearch.py:111` — anonymous client → Elastic Cloud 401 →
  `ping()` returns False → `BackendConnectionError('health check returned false
  during connect')`. The opaque failure R26-F exists to prevent.

The verifier corrected one scanner mis-statement: the scanner claimed empty
creds are *forwarded* to the client; empirically `SecretStr.__len__` makes
`if SecretStr("")` False, so no auth is forwarded at all (anonymous client).
The end conclusion (validation passes → 401 at connect) is unchanged.

## Fix (minimal, consistency per Karpathy rule 7)

Make the validators agree with `_build_kwargs` by construction — truthiness,
not `is not None`:

```python
# settings/elasticsearch.py:192-193
has_api_key = bool(self.api_key)
has_basic_auth = bool(self.username) and bool(self.password)
# settings/elasticsearch.py:225
has_credential = bool(self.api_key) or bool(self.password)
```

`None` / `""` / `SecretStr("")` are all falsy; populated values are truthy via
`SecretStr.__len__`. The validator and `_build_kwargs` then cannot drift.

## DO-NOT-RE-FLAG additions after R27

- ES CLOUD auth check + cleartext guard use truthiness, matching `_build_kwargs`
  (R27-A); empty-string secrets are treated as absent at validation time.

## Frontier note

R24 shipped docs-only (5/6 EMPTY). R25 found 8 by rotating surfaces. R26 found 7
via diff-regression + config-validation. **R27 finds 1** — the lowest code-defect
yield since the loop began. The cron cadence (`25 0-13,19-23 * * *` = 19
fires/day) is now likely over-provisioned relative to defect-discovery rate.
This is the second explicit signal (after R24) to consider winding down or
re-scoping the loop. No action taken this round; flagging for the operator.
