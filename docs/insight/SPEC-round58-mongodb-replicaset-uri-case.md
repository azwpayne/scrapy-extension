# SPEC-round58 — MongoDB REPLICA_SET replicaSet-URI hint is case-sensitive substring match

## Context and audit evidence

Found via the R65 deep-insight fire (ultracode scan), dimension
`settings-validators`, and **independently re-reproduced by hand** before
implementation (reproduction below).

`MongoDBSettings._validate_mode_requirements` (SV2) guards the genuine REPLICA_SET
footgun: a replica set declared *neither* via `replica_set_name` *nor* in the URI
would reach PyMongo and surface as an opaque driver error at `connect()`. The
fail-fast validator exists to turn that into a friendly `ConfigurationError`.

The URI-side detection is a **case-sensitive substring test**:

- `src/scrapy_extension/settings/mongodb.py:1140`
  ```python
  uri_has_rs = "replicaSet=" in self.uri
  ```

PyMongo's `uri_parser.parse_uri` normalizes option keys **case-insensitively**
(lowercase `?replicaset=`, camelCase `?replicaSet=`, and UPPER `?REPLICASET=`
all parse to `{'options': {'replicaSet': ...}}`). The backend
(`backends/mongodb.py`) passes `uri` verbatim to `MongoClient(uri, **kwargs)`, so
a lowercase URI genuinely declares the RS to the driver. The project's validator
therefore disagrees with the driver it is configuring.

**Two failure paths (both reproduced):**

1. **FALSE-NEGATIVE — valid config rejected.** `MongoDBSettings(mode=REPLICA_SET,
   uri='mongodb://localhost:27017/?replicaset=rs0')` is REJECTED with
   `ConfigurationError`, even though PyMongo would honor it. An operator using
   the lowercase form (permitted by MongoDB's own docs / common in
   env-var-driven configs) is wrongly told the URI is missing the RS hint.
2. **FALSE-POSITIVE — fail-fast bypassed.**
   `uri='mongodb://localhost:27017/?appname=replicaSet=x'` is ACCEPTED because
   the literal `replicaSet=` substring is present, but `parse_qsl` yields
   `[('appname', 'replicaSet=x')]` — no real `replicaSet` option. The fail-fast
   validator is bypassed and PyMongo raises `InvalidURI` at connect instead of
   the friendly `ConfigurationError` the validator exists to produce.

**Isolated reproduction** of the exact check (`python3`, stdlib only):

```
case            OLD    NEW    CORRECT
lowercase       False  True   True  <-- OLD WRONG
camelCase       True   True   True
UPPER           False  True   True  <-- OLD WRONG
semicolon       True   True   True
nested-fp       True   False  False <-- OLD WRONG   (?appname=replicaSet=x)
no-rs           False  False  False
empty           False  False  False
```

`OLD` = `"replicaSet=" in uri`; `NEW` = the proposed parsed lookup. `OLD` is wrong
on lowercase / UPPER (rejects valid) and on the nested-substring payload (accepts
an RS-less URI). `camelCase` — the only form covered by existing tests — is
unchanged, so no existing test pins the case-sensitive behavior.

**Why this is not an exhausted theme.** The exhausted list's "mongodb URI proxy
options" is a different concern, and the adjacent R29-D comment at
`mongodb.py:1141-1143` addresses whitespace in the `replica_set_name` *field*, not
URI-option case. The module already lowercases URI option names via the helper
`_mongodb_uri_option_pairs` (`mongodb.py:539`) used at lines 296 and 687 — this
site simply was not converted.

**Severity: medium.** It blocks a class of valid configurations (usability) *and*
defeats the fail-fast invariant the validator exists to enforce. Trivial,
zero-risk fix for the camelCase happy path.

## Goal

Make the REPLICA_SET URI-hint detection agree with PyMongo: a URI carries a
replica-set declaration iff a `replicaSet` option (any case) is present in the
query string. Accept lowercase/camelCase/UPPER; reject nested-substring
false-positives.

## Specification

In `_validate_mode_requirements` (`src/scrapy_extension/settings/mongodb.py:1140`),
replace the case-sensitive substring test with a parsed, case-insensitive option
lookup via the module's existing helper (already used at lines 296 and 687;
`urlsplit` already imported at line 13):

```python
# before (mongodb.py:1140)
            uri_has_rs = "replicaSet=" in self.uri
# after
            uri_has_rs = any(
                name == "replicaset"
                for name, _value in _mongodb_uri_option_pairs(
                    urlsplit(self.uri).query
                )
            )
```

`_mongodb_uri_option_pairs` (line 539) lowercases option names and honors both
`&` and `;` separators, so the lookup matches PyMongo's case-insensitive parsing
and never matches a substring nested inside another option's value. The error
message and `setting_value` are unchanged, so the existing
`_SAFE_SETTINGS_CONFIGURATION_MESSAGES` entry (`_redacted.py`) stays valid.
Behavior is identical for every camelCase/semicolon URI (the only currently
tested forms) and unchanged when the URI has no query.

## Plan and independently verifiable tasks

- **R58-1 — RED test.** Add tests asserting (a) lowercase and UPPER
  `?replicaSet=`/`?replicaset=`/`?REPLICASET=` URIs are ACCEPTED in REPLICA_SET
  mode without `replica_set_name`, and (b) the nested-substring URI
  `?appname=replicaSet=x` is REJECTED with `ConfigurationError`. → verify:
  `uv run pytest <file>::<new>` FAILS on current code (lowercase/UPPER raise;
  nested-substring is wrongly accepted).
- **R58-2 — GREEN fix.** Apply the substring→parsed-lookup change at
  `mongodb.py:1140`. → verify: the R58-1 tests now PASS.
- **R58-3 — no-regression.** Confirm the existing camelCase REPLICA_SET URI
  acceptance test and the no-name-no-URI rejection test still pass. → verify:
  full settings test files green; `ruff check .` + `ruff format --check` + `mypy
  --strict src/scrapy_extension` green.

## Acceptance criteria

1. `MongoDBSettings(mode=REPLICA_SET, uri='mongodb://h/?replicaset=rs0')` and
   `?REPLICASET=rs0` construct WITHOUT error (no `replica_set_name` set).
2. `MongoDBSettings(mode=REPLICA_SET, uri='mongodb://h/?appname=replicaSet=x')`
   raises `ConfigurationError` (no real `replicaSet` option).
3. Existing camelCase REPLICA_SET URI behavior unchanged; the no-name-no-URI
   rejection still raises.
4. Gate green: `uv run ruff check .` + `uv run ruff format --check src tests
   conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension`.
5. One atomic commit, ff-merged to `main`; CI green.
