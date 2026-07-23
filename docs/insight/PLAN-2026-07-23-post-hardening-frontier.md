# PLAN — Round 15: Post-v1-Hardening Frontier Closeout

> **Companion.** [`SPEC-2026-07-23-post-hardening-frontier.md`](./SPEC-2026-07-23-post-hardening-frontier.md).
> Operationalizes the 2026-07-23 ultracode deep-insight (`wf_53cd8e6f-54c`).
> Maintainer planning, not a public roadmap.

**Strategy.** v1.0 is already shippable; the hardening line is sound. This round
is **not** a bug-hunt — it is closing 6 bounded, file-disjoint units (1 test-pin +
2 contract/availability parity fixes + 1 hygiene fix + 1 docs pass) so the v1.0
guarantees are test-pinned and contract-uniform. Execution is TDD, file-disjoint
parallel fan-out where possible, conventional commits per unit.

---

## Round sequence

### Round 1 — U1 durability-flag regression pin  🔴 (solo, highest leverage)

**Why first.** Guards the package's core distributed-dedup value proposition and
currently has **zero regression signal**. Cheapest unit, biggest risk-mover.

- Executor: solo.
- TDD: write `tests/test_push_durability_contract.py` parametrized over the 7 real
  backends → **RED** (delete the ClassVar from one backend, confirm the test fails) →
  restore → **GREEN**.
- Template: `tests/test_rabbitmq_backend.py:183-207` (the only real-backend file
  that currently asserts the flag).
- Files touched: `tests/test_push_durability_contract.py` (NEW only).

### Round 2 — U2 / U3 / U4 parallel  🟠🟠🟢 (3 executors, file-disjoint)

All three are `src/scrapy_extension/backends/*.py` on **different files** → safe
parallel fan-out, no merge conflict.

| Executor | Unit | File | Pattern mirrored |
|---|---|---|---|
| A | U2 dynamodb `clear_storage` lock release | `dynamodb.py` | `connectors.py:1078-1090` (connect releases lock before backoff) |
| B | U3 kafka `clear_queue` → `QueueError` | `kafka.py` | `pulsar.py:888` / `rocketmq.py:685` |
| C | U4 ES `connect()` broad-except arm | `elasticsearch.py` | `mongodb.py:181-191` / `kafka.py:369-376` |

- **Caller-trace before error-propagation changes** (U3): confirm the scheduler's
  `clear`/`QueueError` handling is conservative (fire-10 #68 template).
- TDD per unit: RED (prove the test catches the defect) → GREEN.

### Round 3 — U5 test hygiene  🟢 (solo)

- Fix `conftest.py` fixture to honor `require_durable` (or split + docstring warn).
- Replace `test_connection_manager_coverage.py:403` fixed `sleep(0.05)` with a
  deterministic waiter-count gate (mirror the `while fake.connect_calls < 1` poll above it).
- Files touched: `tests/conftest.py`, `tests/test_connection_manager_coverage.py`.

### Round 4 — U6 docs  🟢 (solo, any time — disjoint from src/tests)

- Add the Pulsar/RocketMQ `queue_len`→`NotImplementedError` before/after subsection to
  `docs/migration-guide.md`.
- Refresh or retire `docs/codebase-deep-insight.md` (stale 2026-07-11, pre-hardening).
- Files touched: `docs/migration-guide.md`, `docs/codebase-deep-insight.md`.

### Round 5 — U7 verification gate  🔧 (depends on U1–U6)

Three hard gates, run in this order (CI order — ruff before pytest):

1. `uv run ruff check src/ tests/` → clean
2. `uv run mypy --strict src/` → 0 errors
3. `uv run pytest -q` → all green, no new skips; coverage ≥ 95%

### Round 6 — ship

- One conventional commit per unit: `test:`, `fix(dynamodb):`, `fix(kafka):`,
  `fix(elasticsearch):`, `test:`, `docs:`.
- Push branch `-u`; open **draft PR** linking the SPEC + PLAN.
- PR body: summary table (unit → file → commit), test plan, the 3-gate result.

---

## Dependency graph

```
U1 ──┐
U2 ──┤
U3 ──┼──► U7 (gate) ──► ship
U4 ──┤
U5 ──┤
U6 ──┘
```

U2/U3/U4 file-disjoint (parallel). U6 (docs) has no code dependency — slot in any
round. U7 strictly after U1–U6.

## Risk register (execution)

- **U2 over-fix risk.** Releasing the lock around batch deletes changes the
  serialization contract for `clear_storage`. Mitigation: snapshot the generation
  inside the lock and re-validate the epoch per batch — a retired generation aborts
  the clear (correctness preserved). Add the "concurrent retrieve not blocked" test
  to lock the new behavior.
- **U3 caller-trace.** Before widening `NotImplementedError` → `QueueError`, grep
  callers of `clear_queue` / `BackendQueue.clear` to confirm no `except NotImplementedError`
  path relies on the current type (the two such handlers at `queue.py:1133/1179` are
  for `get_storage_backend`, a different contract — verify).
- **U5 fixture blast radius.** The `mock_connection_manager` fixture is consumed by
  ~14 test files. Changing its return shape could ripple — run the full suite, fix any
  test that implicitly depended on the unconditional-`True` lie.
- **Thrash guard.** `dynamodb.py` (938 LOC) and `connectors.py` (1716 LOC) are large;
  if an executor subagent autocompacts, fall back to direct main-loop TDD with tight
  read caps (per the workflow thrash lessons — `MEMORY.md`).

## Out of scope (deferred)

- The 2 deep-insight findings left **unverified** (rocketmq race + base.py test-quality —
  verify agents thrashed on large files). Re-scan with tighter read caps if pursued;
  not blocking v1.0.
- Post-1.0 Tier-2/3 items already in `EXECUTION-INDEX.md` (U10–U17, U19 module splits).
