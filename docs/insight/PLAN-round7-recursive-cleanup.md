# PLAN — Round 7: Recursive Cleanup (zero-out open-items)

Round 7 closes the hardening arc. The goal was recursive: *every* known issue from
rounds 1-6 must reach a terminal disposition — FIXED, or ACCEPTED with a defensible
file:line-anchored rationale (both count as "processed"). No item may remain in an
un-triaged "open / defer-to-later" state.

This document is the **RESOLUTION table**. It supersedes the "Tier-2 / Tier-3 /
Non-goals" deferral lists of [`PLAN-2026-06-25.md`](./PLAN-2026-06-25.md) and the
per-round PLAN-round{3,4,5,6} docs. Companion: [`INSIGHTS-2026-06-25.md`](./INSIGHTS-2026-06-25.md).

## Method

Orchestrator enumerated every open / Not-tested / Non-goal item from rounds 1-6
(INSIGHTS Themes A-F + every commit's `Not-tested:` trailer), then **verified the
current code state** rather than trusting the doc labels. The verify sweep found
that 5 items marked "open" in INSIGHTS had in fact been fixed incidentally in
rounds 2-4 without the tracker being updated (doc-lag, not debt). One item
(Cuckoo-filter-full) was a genuine remaining bug and is fixed in this round.
The remaining items received architect verdicts (round-7 R7-B) applying the same
"opt-in + documented + warned → not a silent-default correctness gap" lens that
round-4 used for distributed Bloom.

## The one code fix — R7-A: Cuckoo-filter-full graceful degradation

**Bug.** `dupefilter/filters/cuckoo_filter.py:153` raises `RuntimeError` once
insertion exhausts `_MAX_KICKS` (filter past capacity). `dupefilter/dupefilter.py`
caught only `NotImplementedError`, so the `RuntimeError` propagated through
`scheduler.enqueue_request` → **crashed the spider mid-crawl** the first time the
filter filled. No graceful degradation (INSIGHTS Theme C, MEDIUM, open through
rounds 2-6).

**Fix** (layered, type-based — no string-matching). A new `FilterFull(RuntimeError)`
exception on the `MembershipFilter` interface (`dupefilter/filters/base.py`) is
raised by the cuckoo filter at capacity (`cuckoo_filter.py`) and caught **by
type** in the dupefilter (`dupefilter/dupefilter.py:303-322`) — a dedicated
`except FilterFull` arm (kept separate from `NotImplementedError` — different
meaning: full vs. unsupported). Catching by type (not string-matching the
message) means the cuckoo layer can reword its message without silently
disabling the guard; this addresses the code-reviewer MEDIUM on the initial
string-match approach. On filter-full: warn **once per process** (module-level
`_filter_full_warned`, mirroring `factory.py:31`'s `_warned`), emit
`monitor.on_filter_full()` (a new `Monitor` hook; `ScrapyStatsMonitor`
translates it to `dupefilter/filter_full`) via the monitor contract — no
private-attribute reach — and treat the overflow item as **NOT-seen** (allow
enqueue).

**Trade-off rationale.** For a crawler, a dead spider is worse than a duplicate
fetch — Scrapy + the downstream pipeline handle occasional duplicates, but a
crashed long-running crawl loses all in-flight progress. Dedup stays effective
within capacity; overflow items are allowed through (may re-fetch, recoverable).
Cuckoo's never-false-negative-*within-capacity* contract is preserved.

**TDD evidence.** 4 RED tests genuinely reproduced the pre-fix `RuntimeError`
crash; all 5 cuckoo-degradation tests pass GREEN post-fix
(`tests/test_dupefilter.py::TestBackendDupeFilterCuckooFilterFullDegradation`),
plus a new `test_on_filter_full_increments_filter_full_stat` in
`test_monitor.py` covering the monitor-contract translation. No skip / xfail /
weakened assertion. An autouse fixture in the test class resets the warn-once
flag per test for isolation. Central verify: **1353 passed / 27 skipped** (was
1346 at round-6 end → +7).

---

## RESOLUTION table — every previously-open item, terminal disposition

### CLOSED-DONE (fixed incidentally in rounds 2-4; INSIGHTS tracker was stale)

| ID (INSIGHTS) | Was marked | Actual state (verified) | Evidence |
|---|---|---|---|
| B4 — `has_pending_requests` returns True on `NotImplementedError`/`QueueError` (silent stall) | MEDIUM open | **DONE** — wrapped in try/except `(NotImplementedError, QueueError)` → logs warning + returns True (conservative) | `schedule/scheduler.py:584-596`; tests `test_components.py:429,448` |
| Theme E HIGH — ack-handler body tests (only `signals.connect` asserted, never `ack(token=T)`) | HIGH gap | **DONE** — tests assert `_on_response_received` forwards token to `ack(token=…)` and `_on_spider_error` forwards to `nack(token=…)` | `tests/test_components.py:1018,1033` |
| Theme E HIGH — crash-mid-ack / no-ack-no-commit | HIGH gap | **DONE** — covered for all 3 ack-using MQ backends | `tests/test_sqs_backend.py:434` (TestSqsCrashMidAck), `tests/test_pulsar_backend.py:327`, `tests/test_rabbitmq_backend.py:1095` |
| Theme C / D3 — per-process dedup strategy factory warning | MEDIUM | **DONE** — `_warn_per_process_scope` warns once per process per strategy | `dupefilter/filters/factory.py:65-87` (idempotent via `_warned` set) |
| B5 — reconnect-after-close round-trip test | MEDIUM HYPOTHESIS | **DONE (round-trip)** — closing last holder evicts registry; next `get_manager()` reconnects fresh | `tests/test_connectors.py:845`, `tests/test_connection_manager.py:16` |
| B3 — Circuit-breaker half-open "single probe" not enforced (N threads flip to HALF_OPEN and call the backend concurrently) | HIGH HYPOTHESIS open (INSIGHTS:55) | **DONE** — `_probe_in_flight` flag claims the single-probe slot under the lock; concurrent callers block while a probe is in flight | `backends/circuit_breaker.py:127,186-189`; `tests/test_circuit_breaker.py:588` (`test_concurrent_probes_issue_exactly_one_func_call`, RED reproduced 8 probes → 1 GREEN) — round-2 E-E4 |
| SEC-6 — Sentinel/Cluster malformed-entry raw `ValueError` from `int(port_str)` (round-1 residual, INSIGHTS:81 "confirmed OPEN") | LOW open | **DONE** — Sentinel + Cluster parse errors (`int(port_str)`, missing `:port`, ping failures) surface as `BackendConnectionError(backend_type="redis")`, not raw `ValueError` | `backends/redis.py:189-195` (Sentinel), `backends/redis.py:260-265` (Cluster) — round-6 SEC-6 |

### CLOSED-FIX (this round)

| ID | Fix | Evidence |
|---|---|---|
| Cuckoo-filter-full → crashes spider | `FilterFull` exception on `MembershipFilter` ABC, caught by type in dupefilter; new `on_filter_full` monitor hook; warn-once + degrade to not-seen | `dupefilter/filters/base.py` (`FilterFull`), `cuckoo_filter.py` (raise), `dupefilter.py:303-322` (catch), `monitor/{base,stats}.py` (`on_filter_full`); `tests/test_dupefilter.py` + `test_monitor.py` (7 new) |

### ACCEPT-as-non-gap (opt-in strategy, per-process scope documented + warned; not a silent-default correctness gap)

| Item | Disposition rationale |
|---|---|
| **Distributed Delay** (durable heap) | Default queue strategy is `passthrough`; `delay` is explicit opt-in via `SCRAPY_QUEUE_STRATEGY=delay`. Per-process scope is documented in the class docstring (`queue/strategies/delay.py:30-38`) and `close()` emits a non-silent WARNING with discarded-item count (`delay.py:151-168`). Failure mode is work-loss-on-crash (delayed items vanish), not silent corruption — already-popped items are unaffected, no duplicates emitted. Durable-delay-via-backend-ZSET is a feature, not a hardening fix. Same lens as round-4 distributed-Bloom ruling. |
| **Distributed Throttle** (shared token bucket) | Opt-in via `SCRAPY_QUEUE_STRATEGY=throttle`. Under N workers the effective rate is `N×(1/min_interval)` — a politeness-contract caveat, not durability/correctness (items persist in the backend). Per-instance scope documented in docstring (`queue/strategies/throttle.py:23-35`). Shared token-bucket via backend INCR+TTL is a feature; per-process Scrapy deployments (the dominant shape) get the correct rate today. |
| **Bloom / Cuckoo / Memory dedup per-process** | NOT a correctness gap — the `set` strategy is distributed-exact and is the default. Per-process filters are opt-in; the factory warns once per process per strategy at selection time (`factory.py:65-87`). A space-optimization, not a silent bug. (Architect round-4 verdict, re-confirmed.) |
| **RoundRobin `_idx` per-worker** | LOW. Per-worker index advances past empty sources → non-work-conserving under skew. Throughput consideration, not correctness — items are not lost or duplicated. |

### ACCEPT-as-feature-request (niche, opt-in, eventually-recovered)

| Item | Disposition rationale |
|---|---|
| **Sentinel failover re-discovery** (round-2 residual) | The round-2 framing ("no re-discovery path exists") was **imprecise**. A full `ConnectionManager` reconnect *does* re-discover the master via a fresh `_sentinel.master_for()` call (`backends/redis.py:204` + `connectors.py:367`). The residual gap is narrower: a *single transient* `ConnectionError` against the cached master client during `push`/`pop` is not auto-re-resolved inline — the circuit-breaker or operator retry must trip to trigger the reconnect cycle. SENTINEL is an explicit opt-in mode; `retry_on_timeout=True` is already wired (`redis.py:208,221`); eventual recovery holds. An inline `except ConnectionError → _connect_sentinel()` wrapper (~40 LOC + a sentinel-fixture-gated integration test) is a real feature (HA resilience for one mode), not a default-settings correctness gap. |

### ACCEPT-infra-gated (requires live infra / product decision; not a code hardening fix)

| Item | Disposition rationale |
|---|---|
| **`rocketmq-client-python==2.0.0` replacement** (unmaintained 6y) | Genuine supply-chain concern but the blast radius is gated behind the explicit `[rocketmq]`/`[all]` extra — not in the default install or test group (`pyproject.toml:64-91`; test group deliberately excludes rocketmq). Secrets use `SecretStr` + `secret_value()` (`rocketmq.py:84-87`); round-1 `ConfigurationError` gating holds. No clear maintained Python drop-in exists as of mid-2026 (both `rocketmq-client-python` and `rocketmq-python` wrap the same C++ core / are stale); migration is a maintainer tier-1-backend product decision, not a hardening fix. Caveat: a 30-min maintained-replacement audit could flip this to a code fix. |
| **SQS / Pulsar / Kafka / RabbitMQ real-broker integration tests** | Integration CI job exists (`tests/integration/`, revived round-3) but is env-gated — no live brokers in this environment. Mock-based unit coverage is honest and complete (verified: ack handlers, crash-mid-ack, multi-queue, real in-flight-set for all 4 MQ backends). Running real brokers needs CI infra, not code. |
| **B5 — in-flight-set survival across reconnect** (HYPOTHESIS) | The reconnect round-trip is tested (`test_connectors.py:845`); whether Kafka `_in_flight` / RabbitMQ `_in_flight_tags` *instance state* survives a backend swap is not pinned. Requires a real-broker reconnect fixture to falsify honestly. Mock-only pinning would not add evidence. |
| **SEC-3 schemeless ES hosts** (e.g. `"localhost:9200"` → ES-py treats as http) | Accepted scope — the round-6 guard matches explicit `http://`; schemeless hosts are an ES-py normalization quirk. Documented in the round-6 commit `Not-tested:` trailer. |
| **SEC-5 case-sensitive Pulsar scheme** (`"Pulsar+SSL://"`) | **Superseded 2026-07-21:** a real pulsar-client 3.12.0 probe disproved this historical assumption. The SDK rejects mixed-case schemes; current settings canonicalize the scheme and enforce its single-prefix cluster syntax before construction. |
| **Redis MASTER_SLAVE read-scaling** (`redis.py:168` comment) | Documented "not yet implemented" — a feature request (read replica scaling), not a bug. The MASTER_SLAVE mode connects correctly; only the optional read-offload is unimplemented. |

---

## Acceptance (this round)

- `uv run pytest -q -p no:randomly` → **1353 passed / 27 skipped / 0 failed** (was 1346 → +7: cuckoo degradation + monitor hook).
- `uv run ruff check src tests` → All checks passed.
- `uv run mypy src/scrapy_extension` → Success, no issues in 67 source files (+2 vs round-6).
- TDD honored: 4 RED tests reproduced the pre-fix crash; all 5 GREEN post-fix.
- Public API stable: the RuntimeError arm is internal; no dupefilter signature change.
- Every previously-open item from rounds 1-6 has a terminal disposition in this table (FIXED, DONE, or ACCEPT with rationale + evidence). **Open-items list: 0 un-triaged.**

## What is NOT in scope (and now explicitly will not be re-litigated)

- Distributed Delay / Throttle / Bloom — accepted scope limitations (opt-in, documented, warned). A future feature major could add durable variants; this is not a hardening concern.
- RocketMQ dependency replacement — infra-gated on a maintainer product decision.
- Real-broker integration tests — infra-gated on CI broker fixtures.
- Sentinel inline re-resolve — feature-request (HA resilience for one opt-in mode).

The hardening arc (rounds 1-7) is complete: 1 CRITICAL + 6 HIGH (rounds 2-3) +
all MEDIUM correctness/architecture items closed; remaining items are documented
feature/scope limitations, not latent bugs.
