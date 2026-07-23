# SPEC — Round 15: Post-v1-Hardening Frontier Closeout

> **Provenance.** Operationalizes the 2026-07-23 ultracode deep-insight (workflow
> `wf_53cd8e6f-54c`: 4 opus architecture maps + 5-dimension find + adversarial
> verify; **7 findings confirmed, 0 refuted** — DO-NOT-RE-FLAG guardrails held).
> Baseline map: [`ITERATIVE-HARDENING-2026-07-21.md`](./ITERATIVE-HARDENING-2026-07-21.md).
> Companion plan: [`PLAN-2026-07-23-post-hardening-frontier.md`](./PLAN-2026-07-23-post-hardening-frontier.md).
> This is maintainer planning, not a public roadmap — for current public
> behavior see [`../../README.md`](../../README.md); for maturity guarantees
> see [`../../.github/STABILITY.md`](../../.github/STABILITY.md).

**Status legend:** 🔴 top-risk · 🟠 medium · 🟢 low · 🔧 executable

---

## Goal

Close the 7 verified findings from the 2026-07-23 deep-insight so the v1.0
tag's core guarantees are **test-pinned and contract-uniform**. Every unit is a
parity / hygiene fix — **zero architectural changes** — each mirroring an
established codebase pattern.

## Non-goals

- Re-auditing closed clusters: `clear_*` / swallow (PRs #59–#72), bloom/cuckoo
  never-false-negative (audited clean), pulsar `_RedactedStr` (false positive).
- Touching documented / contained low-severity theoretical risks unless they
  become public contract:
  - redis `.client` escape hatch (`backends/redis.py:841` — unused by any bundled component),
  - dynamodb `clear_storage` availability tradeoff (correctness sound; documented),
  - unlocked ack-fence reads in pulsar/rocketmq (benign — single-outcome settlement contains them).

## Baseline

post-v1-hardening main `8fdae05`; CI green; v1.0 tag-defensible. The 3 hardening
pillars are verified sound by the 4 architecture maps:

1. **Two-layer generation fence** — L1 ConnectionManager backend/breaker
   identity-coherence (`connectors.py:1553-1591`); L2 per-backend client generation
   (lease+drain for redis/sqs; operation-lock for dynamodb/memcached/rabbitmq).
2. **Durability-bound push** — `_push_queue_with_durability` → `_QueuePushReceipt(worker_crash_durable)`;
   dedup marker published only AFTER durable push (`scheduler.py:1607-1647`).
3. **Single-outcome token settlement** + 3-layer capability gating.

The maps surfaced **0 HIGH / CRITICAL** defects.

---

## Work units

### U1 — Durability-flag regression pin  🔴 TOP-RISK (MED)

- **What.** `_push_is_durable = True` is set on exactly 7 real backends
  (`redis.py:211`, `mongodb.py:85`, `elasticsearch.py:61`, `kafka.py:217`,
  `pulsar.py:282`, `rocketmq.py:125`, `sqs.py:334`) but **no real-backend test
  pins it** — every durability assertion runs against test fakes that self-set
  the flag. If the ClassVar regresses, every push classifies `volatile` → the
  scheduler records only a process-local dedup shadow → **cross-worker dedup
  silently breaks with zero test signal**, defeating the package's core value prop.
- **Files.** `tests/test_push_durability_contract.py` (NEW — file-disjoint).
- **Acceptance.** Parametrized over the 7 real backends (mocked SDK clients):
  `backend._push_with_durability('q', b'x', priority=0.0, require_durable=True)`
  returns `.worker_crash_durable is True` AND does NOT raise `_DurablePushRequired`.
  **RED-proven**: deleting the ClassVar from any one backend fails the test.
  Template: `tests/test_rabbitmq_backend.py:183-207`.
- **Why.** Same "mock the helper, not the real client" anti-pattern as R-es-qlen
  #65 / R-kqlen #68. Cheapest unit, highest leverage — guards the distributed-dedup promise.

### U2 — DynamoDB `clear_storage` lock discipline  🟠 (MED)

- **What.** `dynamodb.py:862` `clear_storage` holds `self._operation_lock` across
  the entire paginated scan + `_delete_batch_with_backoff` (which `time.sleep`s
  full-jitter backoff at `:490`). That same lock serializes `store`/`retrieve`/
  `delete`/`exists`/`ttl` + `disconnect` → a throttled clear stalls **all** concurrent
  storage ops and shutdown. In-file `connect()` (`:505-510`) already follows
  release-lock-before-slow-work discipline; `clear_storage` violates it.
- **Files.** `src/scrapy_extension/backends/dynamodb.py`.
- **Acceptance.** Snapshot `generation.table`/`client`/`table_name` inside the lock
  (already captured `:863-866`), release `_operation_lock` around each
  `_delete_batch_with_backoff` call, re-acquire per batch + re-validate the generation
  epoch. Mirrors `connectors.py:1078-1090`. Existing clear tests stay green; NEW test
  asserts a concurrent `retrieve()` is NOT blocked while `batch_write_item` returns
  `UnprocessedItems` + backoff sleeps. No data-loss / correctness change.
- **Why.** Availability — a throttled clear currently freezes the whole storage pipeline.

### U3 — Kafka `clear_queue` exception-family normalization  🟠 (MED)

- **What.** `kafka.py:1354` `clear_queue` raises `NotImplementedError`, while
  sibling Pulsar (`pulsar.py:888`) and RocketMQ (`rocketmq.py:685`) raise
  `QueueError` for the identical "unsupported clear" condition. A caller using
  `except QueueError` handles Pulsar/RocketMQ cleanly but lets Kafka's
  `NotImplementedError` escape uncaught.
- **Files.** `src/scrapy_extension/backends/kafka.py`.
- **Acceptance.** `raise QueueError("Kafka clear_queue is unsupported: ...", queue_name=queue_name, operation="clear_queue")`;
  update the docstring (currently says `NotImplementedError`). New test: `except QueueError`
  catches Kafka clear. Caller-trace first (scheduler handles QueueError conservatively).
  Keep `NotImplementedError` for capability gates (`get_storage_backend`) only.
- **Why.** Contract uniformity across MQ backends.

### U4 — ElasticSearch `connect()` broad-except arm  🟢 (LOW)

- **What.** `elasticsearch.py:121` `connect()` has only two except arms
  (`BackendConnectionError`; `(ApiError, TransportError)`). No fallback → a Ctrl-C /
  unexpected non-`(ApiError, TransportError)` after `self._client = Elasticsearch(...)`
  (`:110`) propagates raw without `self._discard_client()` → leaked half-init ES transport.
  Every peer (mongodb `:181-191` incl. `BaseException`, kafka `:369-376`, rocketmq,
  memcached, pulsar) has the fallback.
- **Files.** `src/scrapy_extension/backends/elasticsearch.py`.
- **Acceptance.** Add `except Exception as e: self._discard_client(); raise BackendConnectionError(...) from e`
  (optionally `except BaseException: self._discard_client(); raise` for Ctrl-C parity with
  mongodb) after the existing `(ApiError, TransportError)` arm. Parity / defense-in-depth.
- **Why.** Cross-backend consistency on the wedge/leak surface.

### U5 — Test hygiene: conftest fixture + CI flake  🟢 (LOW)

- **What.** (a) `tests/conftest.py:25` `mock_connection_manager` hardcodes
  `return _QueuePushReceipt(worker_crash_durable=True)` + `del require_durable` → latent
  false-green across ~14 test files; diverges from real `connectors.py:1615-1673`.
  (b) `tests/test_connection_manager_coverage.py:403` fixed `time.sleep(0.05)` in the
  single-flight connect cohort test T10 → timing-dependent CI flake under xdist/randomly
  (sibling T9 uses a deterministic poll).
- **Files.** `tests/conftest.py`, `tests/test_connection_manager_coverage.py`.
- **Acceptance.** (a) fixture honors `require_durable` (raise `QueueError` when
  `require_durable=True` and push raises, or a configurable durable flag) OR split into a
  faithful double + docstring warning. (b) replace the fixed sleep with a deterministic
  waiter-count gate (poll until N peers reached `_connected_event.wait()`, 5s deadline).
  No current test breaks.
- **Why.** Prevent latent false-greens + CI flake.

### U6 — Docs accuracy  🟢 (LOW)

- **What.** (a) `docs/migration-guide.md` documents 6/7 v1 breaks but MISSES
  Pulsar/RocketMQ `queue_len`→`NotImplementedError` before/after (it IS in
  `.github/CHANGELOG.md:102-105`, `.github/STABILITY.md:160-162`, `docs/runbook.md:547-551` —
  only the migration guide lacks it). (b) `docs/codebase-deep-insight.md` is stale
  (2026-07-11, pre-v1-hardening) — its §2 layered model predates the generation-fence /
  durability-bound-push pillars.
- **Files.** `docs/migration-guide.md`, `docs/codebase-deep-insight.md`.
- **Acceptance.** (a) add a subsection after the Kafka `queue_len` paragraph (`:479`):
  BEFORE returned a number; AFTER raises `NotImplementedError` (`pulsar.py:884`,
  `rocketmq.py:681`); operator impact = `queue/depth` stat stops emitting + depth-based
  backpressure (`SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD`) skipped per-poll. (b) refresh
  deep-insight §2 to the 3-pillar model OR mark it historical with a pointer to
  `docs/insight/ITERATIVE-HARDENING-2026-07-21.md`.
- **Why.** The migration guide is the canonical operator upgrade doc; the deep-insight
  doc must not contradict current behavior.

### U7 — Verification gate  🔧

Run AFTER all code units land (fixes final state). Three hard gates
(per the CI-ruff-gate lesson — CI runs `ruff` BEFORE `pytest`; local pytest ≠ CI green):

- `uv run pytest -q` — all green, no new skips
- `uv run ruff check src/ tests/` — clean
- `uv run mypy --strict src/` — 0 errors; coverage ≥ 95% floor

Then commit per unit (conventional commits), push branch, open draft PR.

---

## Sequencing (summary — full detail in the PLAN)

1. **U1** solo (highest leverage) → 2. **U2 / U3 / U4** parallel (file-disjoint) →
3. **U5** → 4. **U6** (docs, any time) → 5. **U7** gate → 6. ship.

Total estimate: ~1–1.5 days; U1 alone ~2 hours.
