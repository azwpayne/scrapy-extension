# Round 28 — SPEC: r27-empty-auth incompleteness + settings fail-fast gaps

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_afeab0cc-041` (5-dim find + adversarial verify;
> 10 agents, 0 errors, ~1.85M tokens, ~20 min). Base: `main` @ `97c76a0` (post-R27).

## Headline

**4 raw → 4 confirmed, 0 refuted → 3 units after dedup.** The `r27-diff-regression`
dimension caught the headline MED: **my own R27-A commit was incomplete** — its
message claimed "`bool(...)` at all three sites" but only patched TWO of the
three ElasticSearch settings validators. The third, `_validate_auth_method_exclusivity`,
still uses `is not None` and falsely rejects a valid config (`api_key=SecretStr("")`
env-var drift + real basic_auth) as "mutually exclusive" — the exact
inconsistency R27-A exists to purge. **8th consecutive round** the diff-regression
safety-net caught a self-shipped gap; this round it caught a gap *in the prior
round's own fix*. The dimension remains the highest-ROI and must never drop.

Two independent dimensions (`r27-diff-regression` + `settings-coercion-validator-edges`)
flagged the same auth-exclusivity defect — high cross-confidence. The
`settings-coercion` dimension also surfaced two related fail-fast gaps in the
same family (empty `hosts=[]`, empty/whitespace kafka `bootstrap_servers`).

## Scan result

**4 raw → 4 confirmed.** Per-dimension: `r27-diff-regression` 1→1, `cross-backend-secret-truthiness`
EMPTY (no other backend has the ES pattern — the sibling audit cleared kafka/rabbitmq/mongodb/rocketmq/sqs/dynamodb),
`type-safety-none-propagation` EMPTY, `api-contract-exception-consistency` EMPTY,
`settings-coercion-validator-edges` 3→3.

The 3 EMPTY dims add to the DO-NOT-RE-FLAG set: cross-backend secret-truthiness is
clean (ES was the only instance); component None-propagation is sound; the
exception-type contract is uniform across the swallow→raise cluster (already shipped).

## Ship set (3 units)

| ID | Sev | Surface | Defect (one line) |
|----|-----|---------|-------------------|
| **A** | MED | `settings/elasticsearch.py:268,270` | **R27-A self-gap (self-caught):** `_validate_auth_method_exclusivity` still uses `is not None` — R27-A fixed `validate_mode_requirements` (197-198) + `_validate_no_cleartext_credentials` (234) but missed this third validator. `api_key=SecretStr("")` + valid basic_auth is falsely rejected as "mutually exclusive" even though `_build_kwargs` would drop the empty key and use basic_auth. Same root as R27-A; completes the prior round's stated intent. |
| **B** | LOW | `settings/elasticsearch.py:148` | `_validate_hosts_scheme` filters each entry but not the empty list itself — STANDALONE `hosts=[]` (e.g. `SCRAPY_ELASTICSEARCH_HOSTS=`) trivially passes and surfaces as an opaque elasticsearch-py client error at connect(). The docstring claims "Empty strings are rejected" but an empty LIST is not. |
| **C** | LOW | `settings/kafka.py:463` | R26-E's CONFLUENT guard only rejects the literal `localhost:9092` — empty/whitespace `bootstrap_servers` (and whitespace `confluent_bootstrap_servers`) slips through and surfaces as an opaque kafka-python error at connect. Same fail-fast promise as R26-E, gap on the empty/whitespace axis. |

## Root causes (verified end-to-end)

### A — ES auth-exclusivity (`elasticsearch.py:268-283`)
```python
if self.api_key is None:                                    # 268 — identity, not truthiness
  return self
if self.username is not None or self.password is not None:  # 270 — identity
  raise ConfigurationError("...mutually exclusive...")
```
`SecretStr("") is None` → False → no early-return; `username is not None` → True →
raises. But `backends/elasticsearch.py:_build_kwargs` uses truthiness (`if
self.config.api_key:` → empty key falsy → skips to `elif username and password:`
→ uses basic_auth). So the config the backend would handle correctly is rejected
at construction with a misleading "mutually exclusive" error. Verified empirically
by both scanner and verifier.

### B — ES empty hosts (`elasticsearch.py:148-162`)
```python
bad = [host for host in self.hosts if not host or not host.lower().startswith(_VALID_ES_SCHEMES)]
if bad: raise ...
```
`hosts=[]` → comprehension yields `[]` → `bad=[]` → no raise. Empty list reaches
`Elasticsearch(hosts=[])` → opaque client error. CLOUD is unaffected (uses cloud_id).

### C — kafka CONFLUENT empty endpoint (`kafka.py:462-471`)
```python
if self.mode == KafkaMode.CONFLUENT and not self.confluent_bootstrap_servers:
  if self.bootstrap_servers == "localhost:9092":   # literal match only
    raise ...
```
`bootstrap_servers=""` / `"  "` → `== "localhost:9092"` is False → no raise.
Whitespace `confluent_bootstrap_servers="  "` → `not "  "` is False → outer
guard skipped entirely. Either way an unusable endpoint reaches connect.

## Fixes (minimal, TDD)

- **A:** `if self.api_key is None:` → `if not self.api_key:`; `if self.username
  is not None or self.password is not None:` → `if bool(self.username) or
  bool(self.password):`. Update display f-string (277/279) `is not None` →
  truthiness for accuracy. Mirrors R27-A exactly.
- **B:** Add a non-empty-list guard at the top of `_validate_hosts_scheme` for
  STANDALONE mode: `if self.mode == ElasticSearchMode.STANDALONE and not
  self.hosts: raise ConfigurationError(...)`. CLOUD uses cloud_id, not hosts.
- **C:** Tighten both effective-value checks: `if self.mode == CONFLUENT and not
  (self.confluent_bootstrap_servers or "").strip():` then `if (self.bootstrap_servers
  or "").strip() in ("", "localhost:9092"):`. Closes empty + whitespace on both fields.

## DO-NOT-RE-FLAG additions after R28

- ES auth-exclusivity validator uses truthiness, matching R27-A's two other sites
  (R28-A); ES STANDALONE rejects empty hosts list (R28-B); kafka CONFLUENT rejects
  empty/whitespace endpoints on both fields (R28-C).
- Cross-backend secret-truthiness is CLEAN outside ES (R28 cross-backend dim EMPTY);
  component None-propagation sound; exception-type contract uniform.

## Frontier note

R27 = 1 finding (lowest). R28 = 3 (the diff-regression catch + 2 sibling fail-fast
gaps in the same family). The frontier is thin but not empty — the
diff-regression dimension continues to pay (it found a gap *in the prior round's
own fix*), and rotating within the settings-validation surface yielded 2 more.
Cadence still justified this round; re-evaluate next.
