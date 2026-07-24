# SPEC — Round 16: Post-Round-15 Re-scan Closeout

> **Provenance.** 2026-07-24 ultracode re-scan `wf_3c9efaf9-23c` (6-dimension find + adversarial verify, **incl. a Round-15 diff-regression dimension**): 14 raw → **10 confirmed/plausible, 3 refuted, 1 thrashed**. Headline: **Round 15 held up under adversarial review — 0 critical/high regressions.** Companion: [`PLAN-round16-post-r15-rescan.md`](./PLAN-round16-post-r15-rescan.md), [`TASK-round16-post-r15-rescan.md`](./TASK-round16-post-r15-rescan.md). Maintainer planning, not a public roadmap.

**Status legend:** 🔴 real defect · 🟠 medium · 🟢 low · 🔧 executable · ⏸ deferred

---

## Goal

Close the bounded next batch surfaced by the post-Round-15 re-scan: **1 real MED defect** (predates R15) + **2 R15 self-introduced gaps** (honest follow-up) + **1 R15 TOCTOU comment** + a **docs/operability polish bundle**. All units are small, mirroring existing patterns. **Zero architectural changes.**

## Non-goals

- Re-auditing closed clusters (clear_*/swallow PRs #59–#72; bloom/cuckoo never-FN; pulsar `_RedactedStr`; Round-15 U1–U6).
- The 3 refuted findings (dynamodb silent-return-on-retirement is intentional; U1 class-flag pin cannot disagree with the receipt).

## Baseline

Post-Round-15 worktree `worktree-round15-frontier` (9 commits ahead of main `8fdae05`). Gates green: ruff clean, mypy --strict 0/76, 3752 passed, coverage 95.08%.

---

## Units

### R16-A — kafka + rocketmq connect() `except BaseException` arm  🔴 MED (only real defect)
- **What.** `KafkaBackend.connect()` (`kafka.py:354-376`) and `RocketMQBackend.connect()` (`rocketmq.py:235-241`) lack an `except BaseException` arm. A Ctrl+C / SystemExit in the window between `self._producer = ...` and `self._admin_client = ...` (kafka) / `consumer.startup()` (rocketmq) skips `_abort_partial_connect()` → orphaned producer (TCP socket + bg thread) + kafka `is_connected()` lies True. **mongodb/es/dynamodb/redis ALL have the BaseException arm** — kafka+rocketmq are outliers. This is the R-mcc#60 / R-kacc#67 wedge class extended to the BaseException variant. (Predates R15; `is_round15_regression=false`.)
- **Files.** `src/scrapy_extension/backends/kafka.py`, `src/scrapy_extension/backends/rocketmq.py` + tests.
- **Acceptance.** Add `except BaseException: self._abort_partial_connect(); raise` (kafka) / the equivalent discard+raise (rocketmq) mirroring `elasticsearch.py:132-136` / `mongodb.py:189-191`. TDD: simulate KeyboardInterrupt mid-construction → assert cleanup ran + state not wedged.

### R16-B — README kafka clear_queue exception type  🟠 MED (R15 U6 miss)
- **What.** `README.md:527` still says Kafka `clear_queue()` raises `NotImplementedError`; Round-15 U3 changed the code to `QueueError`. The migration-guide was fixed (`4e6a344`) but README was missed.
- **Files.** `README.md`.
- **Acceptance.** One-line: `NotImplementedError` → `QueueError` at :527. Optionally name `QueueError` in `.github/CHANGELOG.md` kafka-clear entry.

### R16-C — conftest `push_is_durable` knob  🟢 LOW (R15 U5 self-correction)
- **What.** The U5 knob is dead config (no test sets it False) AND contract-divergent: it raises the raw internal `_DurablePushRequired`, while the real `ConnectionManager` catches it and re-raises as public `QueueError` (`connectors.py:1654-1659`).
- **Files.** `tests/conftest.py`.
- **Acceptance.** Either translate `_DurablePushRequired` → `QueueError` in the fixture side-effect (mirror real CM), or delete the dead knob. Prefer translate (keeps the volatile-path opt-in usable + faithful).

### R16-D — dynamodb clear_storage TOCTOU comment  🟢 LOW (R15 U2 regression — comment correctness)
- **What.** U2's lock-release admits a narrow TOCTOU: `disconnect()` can close the client in the gap between the under-lock epoch check and the outside-lock `batch_write_item`. The comment at `dynamodb.py:922-925` claims it "aborts before touching the closed client" — **false** (best-effort, not a hard boundary). Impact is low (1 in-flight batch, surfaces as the documented partial-clear `StorageError`).
- **Files.** `src/scrapy_extension/backends/dynamodb.py`.
- **Acceptance.** Correct the comment to state the fence is best-effort. (Optional harden: adopt redis/sqs `active_leases` pattern for a true boundary — defer unless wanted.)

### R16-E — docs / operability polish bundle  🟢 LOW
- **(i)** `docs/runbook.md:593` — "stuck crawl" says `on_pop_rate`/`on_filter_saturation` "not yet landed"; both ARE implemented (`monitor/stats.py:213,230`). Rewrite to present them as the primary landed signals.
- **(ii)** `.github/STABILITY.md:65` — "Fresh hooks/settings" table omits 5 wired settings: `SCRAPY_CIRCUIT_BREAKER_*` (3), `SCRAPY_DEDUP_STRICT`, `SCRAPY_PIPELINE_MAX_STORAGE_ERRORS`, `SCRAPY_STORAGE_BUFFER_MAX_AGE_S`, `SCRAPY_PIPELINE_MAX_ITEM_BYTES`. Add rows.
- **(iii)** `connect()` `from None` → `from e` in pulsar (`pulsar.py:444`) / rabbitmq (`rabbitmq.py:508`) / memcached (`memcached.py:153`) to stop hiding broker-auth/TLS/DNS errors (kafka/mongo/es use `from e`).
- **Files.** `docs/runbook.md`, `.github/STABILITY.md`, `src/scrapy_extension/backends/{pulsar,rabbitmq,memcached}.py` + tests for the `from e` change.

### ⏸ Deferred (R16-F, not blocking v1.0)
- memcached `clear_storage` `NotImplementedError`→`StorageError` parity.
- `dupefilter.clear()` graceful-degradation arm (fire-13 backlog).
- U2 thread tests `@pytest.mark.timeout` guards.

---

## Constraints (from the /loop)

- **Atomic commit per unit** (conventional commits).
- **Merge to main, main-only** — after the gate, the worktree branch fast-forwards into main and is deleted; future rounds commit directly to main.
- **Claude Code only** — no gemini / codex / GPT agents.
