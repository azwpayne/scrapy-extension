# Round 56 — SPEC / PLAN / TASK: lock the two dep-module tables to the bundled registry (contract-test completion)

## Context and audit evidence

`5c2f7c5` added `tests/test_backend_metadata_contract.py` whose docstring states
it "catches a missed hand-maintained mapping without resolving any dotted path"
when adding a bundled backend. It locks `_OPTIONAL_IMPORTS`, `_BACKEND_MODULES`,
`_BACKEND_EXTRAS`, `__all__`, the pyproject extras, and the descriptor fields —
but it **never reads the two hand-synced dep-module tables** that drive the
parallel lazy `__getattr__` install-hint paths:

- `_BACKEND_DEP_MODULES` (`backends/__init__.py:59-70`) — used by
  `backends/__init__.py:_is_missing_optional_dep` for the
  `from scrapy_extension.backends import X` surface.
- `_OPTIONAL_DEP_MODULES` (`scrapy_extension/__init__.py:176-189`) — used by
  `scrapy_extension/__init__.py:_is_missing_optional_dep` for the
  `from scrapy_extension import X` surface.

The two tables are **byte-identical 10-entry duplicates** today (same backend
module-path keys, same dep `frozenset` values — including `rocketmq`'s empty
frozenset and the `boto3` double-use for sqs/dynamodb). Nothing locks them to
each other or to `_BUNDLED_DESCRIPTORS`, so the contract's single-source-of-truth
guarantee has a hole exactly where R14-H placed the install-hint logic.

Verifier-candor note: the user-visible *failure scenario* of a drift is muted
today because every bundled backend self-wraps its module-level `ImportError`
with its own install hint (e.g. `redis.py:41-45`), so `_is_missing_optional_dep`
returns False for the wrapped error regardless of table state. The **gap**
(two hand-synced tables outside the contract test) and the **preventive value**
of the lock are real and unambiguous — the lock exists to catch the *next*
backend that deviates from the self-wrap convention, or any value-desync
between the two tables.

## Goal

Extend the existing contract test with metadata-only assertions that fail loud
the moment either dep-module table drifts from the bundled registry or from
each other — closing the hole in the contract test's own stated mandate.

## Specification

- Add three assertions to `test_bundled_registry_metadata_matches_lazy_exports_and_extras`
  (metadata-only, no dotted-path resolution, no SDK import):
  1. `set(public_backends._BACKEND_DEP_MODULES)` == the set of backend module
     paths derived from `_BUNDLED_DESCRIPTORS`.
  2. `set(scrapy_extension._OPTIONAL_DEP_MODULES)` == the same set.
  3. `public_backends._BACKEND_DEP_MODULES == scrapy_extension._OPTIONAL_DEP_MODULES`
     (full-dict equality — catches value desync, not just missing keys).
- Test-only change. No production code touched. Stays metadata-only (consistent
  with the test's "never resolves a backend class or imports an optional SDK"
  contract).
- Touches only `tests/test_backend_metadata_contract.py` (not in the user-dirty
  list → shippable, no conflict).

## Plan and independently verifiable tasks

- [ ] **R56-1 — Add the lock.** Append the 3 assertions (after the existing
      set-equality locks / the per-descriptor loop), deriving the expected
      module-path set from `_BUNDLED_DESCRIPTORS` via the existing
      `_split_dotted_path` helper.
- [ ] **R56-2 — Verify.** `uv run ruff check .` then `uv run pytest` (the new
      assertions pass today — tables are 10-for-10 in sync — confirming the lock
      is correctly calibrated; drift would surface as set- or dict-inequality)
      then `uv run mypy --strict src/scrapy_extension`. All green, no regressions.

## Acceptance criteria

1. The contract test passes today with the new assertions (tables in sync).
2. The lock is calibrated so a missing entry in either table → set-inequality
   failure, and a value desync between the tables → dict-inequality failure.
3. No production code changed; no dotted path resolved; no optional SDK imported.
4. `ruff check`, `pytest`, and `mypy --strict` are all clean; no other test
   regresses.
