# EXECUTION-INDEX — Maintainer Execution History

> This directory is maintainer planning/history, not the user-facing source of truth. For current public behavior use [`../../README.md`](../../README.md); for API/maturity guarantees use [`../../STABILITY.md`](../../STABILITY.md); for operations use [`../runbook.md`](../runbook.md); for plugin authors use [`../backend-plugins.md`](../backend-plugins.md). Treat the round tables below as historical context unless a current issue/PR explicitly revives an item.

Single-page consolidated, prioritized, dependency-ordered index of every executable
unit discovered across rounds 1-8. Supersedes cross-reading 5 docs. Source SPECs:
[`PLAN-round8-forward.md`](./PLAN-round8-forward.md) (strategic) ·
[`SPEC-round8-tier1.md`](./SPEC-round8-tier1.md) ·
[`SPEC-round8-v1readiness.md`](./SPEC-round8-v1readiness.md) ·
[`SPEC-round8-settings-validation.md`](./SPEC-round8-settings-validation.md).

**Status legend:** ✅ DONE · 🔧 executable (spec'd) · ⏸ deferred (Tier-2/3) · 🔭 new-lens candidate

---

## ⚡ Round 14 menu — six-dimension hardening (✅ ALL LANDED)

Rounds 9-13 closed the round-8 execution menu (✅). A fresh **full-coverage**
six-dimension insight fan-out (error-handling / lifecycle / strategy / test /
observability / API-stability) surfaced **~40 new findings → 8 units (R14-A…H)** — **all executed & merged**.

👉 **SPEC:** [`SPEC-round14-six-dimension-hardening.md`](./SPEC-round14-six-dimension-hardening.md)
👉 **PLAN:** [`PLAN-round14-six-dimension-hardening.md`](./PLAN-round14-six-dimension-hardening.md)

| ID | Title | Status | Commit |
|---|---|:-:|---|
| **R14-A** | StorageBackend error-contract uniformity (3 data-loss/leak bugs) | ✅ | `7a0d4d0` |
| **R14-F** | Queue-strategy correctness (delay priority, RR cleanup, retry+delay storm) | ✅ | `c966c2d` |
| **R14-H** | Lazy-import hygiene + polish (misleading install hint, rocketmq dead branch) | ✅ | `a5e73d9` |
| **R14-B** | v1.0 breaking-change disclosure + ConfigurationError contract freeze | ✅ | `4003f35` |
| **R14-E** | Lifecycle bounds (cap `_managers` registry, Kafka partition pruning, RabbitMQ partial-state) | ✅ | `0237070` |
| **R14-C** | Operability configurability (thread U4/U5/U2 knobs via Scrapy settings) | ✅ | `da458b5` |
| **R14-D** | Observability completeness (dead `on_error`, Bloom/Memory saturation, connection hooks) | ✅ | `dbfdc4a` + scheduler follow-up |
| **R14-G** | Test-coverage hardening (backend layer → 95%+, property tests, integration-tier gate, flake fix) | ✅ | `1960169` |

**Verification (3 hard gates, all green):** `uv run pytest -q` → **1688 passed, 36 skipped** (stable across random seeds — the order-dependent flake was root-caused to a `test_lazy_imports.py` sys.modules pop-without-restore and fixed); `uv run ruff check src/ tests/` → clean; `uv run mypy --strict src/` → 0 errors; coverage **95.31%**.

> ✅ **v1.0 re-assessment: tag defensible again.** R14-B (breaking changes now
> documented in CHANGELOG) + R14-C (runbook-promised knobs now thread via
> `SCRAPY_*` settings) — the two pre-tag blockers — are landed. All 3 v1.0
> non-negotiables met. Remaining backend files <95% coverage (connectors 90.9%,
> dynamodb 90.5%, kafka 90.5%) are error-path/mode branches, not the primary
> corruption-prevention contract (mongodb guards now covered) — a future
> coverage round can close them.

The round-8 menu below is retained for history (all ✅ DONE).

---

## Master unit table (sorted by leverage ÷ effort)

| ID | Title | Leverage | Effort | Files (owner scope) | Source | Depends on |
|---|---|:-:|:-:|---|---|---|
| **SV1** | `Literal` enum types (10 settings footguns) | H | S | settings/{kafka,pulsar,rabbitmq,mongodb}.py | settings-validation | — |
| **SV5** | empty-string + unbounded-int gaps (5 footguns) | M | S | settings/{memcached,redis,rabbitmq,base}.py | settings-validation | — |
| **U4** | queue_len sampling (cut +25% pop RTT) | H | S | queue/queue.py | tier1 | — |
| **U5** | memory default cap (OOM prevention) | M | S | dupefilter/filters/memory_filter.py, queue/strategies/delay.py | tier1 | — |
| **SV3** | cross-field auth/transport coherence (3H credential bugs) | H | M | settings/{kafka,pulsar,redis,mongodb,elasticsearch,sqs,dynamodb}.py | settings-validation | — (security-reviewer lane) |
| **SV2** | mode-conditional `model_validator`s (8 footguns) | H | M | settings/{mongodb,redis,kafka,rabbitmq}.py | settings-validation | — |
| **SV4** | URL/scheme format guards (5 footguns) | M | S-M | settings/{mongodb,pulsar,rocketmq,elasticsearch,sqs,dynamodb}.py | settings-validation | — |
| **U1** | README Guarantees table (v1.0 non-neg #1) | H | S | README.md | tier1 | — |
| **U9** | stability artifacts (STABILITY/SECURITY/CHANGELOG/runbook) | H | S | new docs root | v1readiness | U20 (RocketMQ/Memcached Experimental labeling) |
| **U8** | `mypy --strict` clean (25 errors) | M | S | pyproject.toml + ~11 source files | tier1 | land AFTER functional changes (fixes final code state) |
| **U2** | operability signals — `on_pop_rate` + `on_filter_saturation` (v1.0 #2) | H | M | monitor/{base,stats}.py, dupefilter/filters/cuckoo_filter.py, queue/queue.py | v1readiness | — |
| **U20** | pymemcache unmaintained — document-as-experimental or migrate | H | S-M | pyproject.toml + STABILITY.md | v1readiness | pairs with U9 |
| **U21** | bump redis + elasticsearch caps, validate | M | M | pyproject.toml + uv.lock; retest test_{redis,elasticsearch}_backend*.py | v1readiness | — |
| **U19** | module splits — redis.py (844) + kafka.py (801) over 800-LOC cap | M | L | backends/redis.py → redis_scripts.py + redis_connection.py; backends/kafka.py → kafka_helpers.py | tier1 (structural) | post-1.0 (non-blocking) |
| **U3** | multi-backend e2e integration test | H | L | ✅ DONE `3cef50c` (round-8 Tier-I) | v1readiness | — |

---

## Recommended `/goal` sequencing (file-disjoint batches, v1.0-oriented)

### Round 9 — cheap-wins cluster (all S, no API break) — **highest leverage-per-effort**
**Parallel fan-out (3 executors, file-disjoint):**
- **Executor A (settings owner):** SV1 + SV2 + SV4 + SV5 (all settings/*.py). The 34-footgun cluster minus SV3.
- **Executor B:** U4 (queue/queue.py — `depth_sample_every=100` constructor kwarg).
- **Executor C:** U5 (memory_filter.py default maxsize=1M + delay.py soft-cap+warn).
**Then solo:** Executor A also lands SV3 (security cluster, 3H credential bugs) — or split to its own security-reviewer-led round. ~1-2 days. Kills ~32 footguns + perf win + OOM cap.

### Round 10 — type promise (solo, after R9 code stable)
- **U8 mypy --strict.** Single executor (touches ~11 files; additive type annotations). Run AFTER R9 so it fixes the final code. ~half-day.

### Round 11 — v1.0 tag-defensibility (docs cluster)
- **U1 (README Guarantees) + U9 (stability artifacts) + U20 (pymemcache Experimental label).** All docs/labeling, file-disjoint from code. After this round: v1.0 non-negotiables #1 (U1) met, #3 (U3) already done, tag artifacts (U9) exist. ~1 day.

### Round 12 — v1.0 non-negotiable #2 (operability)
- **U2 operability signals.** New monitor hooks + rolling pop-rate + cuckoo saturation. One executor, M effort. After this: all 3 v1.0 non-negotiables met → **v1.0 tag defensible.**

### Round 13 — supply-chain + dep freshness
- **U21 (redis/es cap bump + validate).** Retest 2 backends against bumped client libs. ~1 day.

### Post-1.0 (deferred Tier-2/3)
- **U19** module splits (refactor, L effort, non-blocking).
- **U10** distributed strategies · **U11** batch API · **U12** OTel · **U13** alt serializers · **U14** async · **U15** capability-richness · **U16** RocketMQ resolution · **U17** property/bench expansion. (See PLAN-round8-forward Tier-2/3.)

---

## "If you only run one `/goal`" → **Round 9 (cheap-wins)**

Biggest bang-for-buck in the entire backlog:
- **~32 settings footguns killed** (SV1/SV2/SV4/SV5) — every user-supplied invalid value now rejected at config time instead of an opaque runtime stack trace.
- **+25% pop-path RTT reclaimed** (U4) — the scientist-quantified default-config perf ceiling.
- **Silent OOM prevented** (U5) — MemoryMembershipFilter + DelayQueueStrategy ship sane caps.
- All **S effort**, **no API break** (defaults/opt-outs preserve current behavior), **3 file-disjoint executors** = clean parallel fan-out + verify.

Round 9 alone moves the library materially toward v1.0 without touching any architectural seem.

---

## Open new-lens candidates (if `/loop` continues past execution)

Untried insight dimensions (a future `/loop` fire could audit, finding newer issues):
- 🔭 **Error-handling consistency** across 10 backends — do they uniformly wrap client-lib exceptions in `BackendError`-family, or do some leak raw `redis.exceptions.*`/`pymongo.errors.*`? (Uniform-catch contract.)
- 🔭 **Concurrency beyond in-flight-set** — ConnectionManager singleton under multi-thread crawler construction; circuit-breaker under sustained OPEN.
- 🔭 **README/doc accuracy vs actual behavior** — does the README describe what the code actually does (esp. after rounds 1-8 changes)?
- 🔭 **kafka.py queue_len None-arithmetic** (surfaced R10/R13) — `backends/kafka.py:746,768` compute `max(0, end_offsets[tp] - position(tp))` where kafka-python stubs type both operands `int | None`; a `None` would raise `TypeError` (not caught by the `except KafkaError`). mypy passes (kafka-python treated as Any); Pyright flags it. Guard with `(x or 0)` or narrow before subtracting.
- 🔭 **test_connectors test_create_backend_redis ordering flake** (surfaced R13) — fails ~1/N full-suite runs (passes in isolation + on rerun). The test mocks `RedisBackend` while `_create_backend` dispatches via the registry descriptor path; mock state leaks under certain `pytest-randomly` seeds. Needs test-isolation fix (fixture scoping), not a code bug.

These are insight-only (no spec yet); run when the execution backlog is drained.

---

## Progress log

| Round | Landed | Commit |
|---|---|---|
| 8 | forward insight + 4-tier test infra | `3cef50c` |
| 8b | Tier-1 executable SPEC + structural sweep | `4462cdd` |
| 8c | v1.0-readiness SPEC + dep audit (pymemcache H) | `cbc924d` |
| 8d | settings-validation SPEC (34 footguns → 5 units) | `a81139c` |
| 9a | SV1 + SV5 — Literal enums + Field bounds (15 footguns) | `f4fd1f3` |
| 9b | SV2 + SV4 — mode-conditional validators + URL/scheme guards (19 footguns) | `d25d27d` |
| 9c | SV3 — cross-field auth/transport coherence (6 footguns, 3H credential) | `97355b9` |
| 9 | U4 — queue_len depth sampling (+25% pop RTT) | `8e91183` |
| 9 | U5 — memory default cap + delay soft-cap (OOM prevention) | `42366b4` |
| 10 | U8 — mypy --strict clean on src/ (py.typed promise) | `fda3b16` |
| 11 | U1 README Guarantees + U9 stability artifacts (STABILITY/SECURITY/CHANGELOG/runbook) | `432f991` |
| 12 | U2 operability — on_pop_rate + on_filter_saturation (v1.0 non-neg #2) | `7d7401a` |
| 13 | U21 redis+elasticsearch cap bump + U20 pymemcache Experimental label | `c48062b` |
| 14 | six-dimension hardening — R14-A…H (storage contract, v1.0 disclosure, operability settings, observability, lifecycle bounds, strategy correctness, coverage+flake-fix, import hygiene) | `7a0d4d0`…`1960169` |

**Rounds 9-14 are CLOSED.** Settings dimension fully validated (SV1-5 + R14-B),
perf/OOM caps shipped (U4/U5) + threaded via settings (R14-C), type promise
kept (U8), v1.0 non-negotiables #1 (U1) + #2 (U2 + R14-D) + #3 (U3) all met,
storage/strategy/lifecycle correctness hardened (R14-A/E/F), test flake
root-caused + fixed (R14-G) → **v1.0 tag defensible.** Remaining work is
Post-1.0 Tier-2/3 (U19 splits, U10-U17) + a future coverage round for the
backend error-path branches still <95%.
