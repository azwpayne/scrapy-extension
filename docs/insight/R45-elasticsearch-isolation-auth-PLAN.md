# R45 PLAN — Elasticsearch capability isolation and authentication boundary

1. Add settings-level regression tests for index isolation, endpoint structure,
   authentication completeness, and authenticated TLS verification.
2. Make the smallest settings-only change: structural host validation and
   model-level cross-field guards. Do not alter backend data operations because
   correct settings make the existing `clear_storage()` target safe.
3. Run focused Elasticsearch/settings tests, lint, strict type checking, and
   the full test suite.
4. Record the result in the insight ledger and create one atomic conventional
   commit containing code, tests, and this R45 specification set.

## Risk control

These are deliberately fail-fast compatibility changes. Any formerly accepted
ambiguous or insecure configuration will now fail at construction, before SDK
construction or a destructive command. Anonymous HTTP is retained solely for
local-development compatibility.
