# SPEC — Round 8 Tier-1: Executable Quick-Wins (for the next /goal)

Incremental refinement of [`PLAN-round8-forward.md`](./PLAN-round8-forward.md) Tier-1
units. The PLAN listed direction; this SPEC makes each unit **directly executable**
by a future `/goal` fan-out (file:line anchors, TDD shape, acceptance, exact change).
Produced by the first incremental `/loop` fire after the round-8 full pass.

## STATUS tracker (delta vs PLAN-round8)

| Unit | PLAN-round8 status | This fire's update |
|---|---|---|
| U3 — Multi-backend e2e integration test | H/L, v1.0 #3 | **✅ DONE** — landed in round-8 testing Tier-I (`tests/integration/test_multi_backend_e2e.py` + `docker-compose.yml`, commit `3cef50c`). v1.0 non-negotiable #3 closed. |
| U1 — README Guarantees table | H/S, v1.0 #1 | executable spec below |
| U4 — queue_len sampling | H/S, 1-line perf | executable spec below |
| U5 — Memory default cap | M/S, OOM | executable spec below |
| U8 — mypy --strict clean | M/S | executable spec below |

---

## U1 — README Guarantees table `[v1.0 non-negotiable #1]`

**Rationale (critic A1/V1, 3-way corroborated):** README sells "Distributed crawling"
but 3/4 queue strategies + 3/4 dedup filters are per-process-with-a-warning. First
prod incident = user re-crawls the entire site because Bloom/Cuckoo don't share
across workers. A Guarantees table kills the surprise in one stroke.

**Files:** `README.md` only (new "## Guarantees" section after the capabilities
matrix; cross-link from the top intro).

**Content (exact):** a per-feature table with column "Distributed? (cross-worker)":

| Layer | Strategy | Cross-worker? | Notes |
|---|---|---|---|
| Queue | Passthrough (default) | ✅ Yes | items live in the backend queue |
| Queue | Delay | ⚠️ Per-process | in-process heapq; lost on crash; `close()` warns |
| Queue | RoundRobin | ⚠️ Per-process | per-worker index |
| Queue | Throttle | ⚠️ Per-process | effective rate = N × (1/min_interval) under N workers |
| Dedup | set (default) | ✅ Yes — exact | backend SADD/SISMEMBER |
| Dedup | Memory / Bloom / Cuckoo | ⚠️ Per-process | cross-worker duplicates pass; factory warns once at selection |
| Storage | all backends | ✅ Yes | via backend KV+TTL |

Plus one paragraph: "Default `set` dedup + `passthrough` queue are distributed-exact.
Delay/Throttle/RoundRobin/Bloom/Cuckoo/Memory are per-process opt-in — safe for
single-worker crawls; for multi-worker politeness/dedup, see the distributed
strategies roadmap (U10)."

**TDD:** N/A (docs). **Acceptance:** a new user answers "is feature X cross-worker
safe?" from the README Guarantees table alone, without reading source.

**Leverage H · Effort S** (half-day, docs-only).

---

## U4 — `queue_len` sampling `[perf, H/S, scientist F2-DEPTH]`

**Rationale (scientist, measured):** `queue_len` (ZCARD) fires on EVERY pop via
`monitor.on_queue_depth` → `strategy.queue_len` → backend. +1 RTT/pop = **+25% of
the pop-path RTT budget** for a depth signal that changes slowly vs pop rate. At
10k pop/s = 10k extra ZCARD/s.

**Files:** `src/scrapy_extension/queue/queue.py:197-205` (the `on_queue_depth` emit
in the pop path); `src/scrapy_extension/settings/base.py` (new opt-in setting
`SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY: int = 100`, default 100 = sample 1/100 pops).

**Change:** wrap the `on_queue_depth` emission in a counter — only emit (and only
call `strategy.queue_len`) every Nth pop. Keep the backpressure signal fresh (depth
changes slowly relative to pop rate; sampling at 1/100 keeps it within ~1% variance).

**TDD:**
- RED: today `queue_len` called every pop → test asserts call_count == N after N
  pops. Post-fix: call_count == N/100 (rounding).
- GREEN: with sampling, `on_queue_depth` still emitted within the window; backpressure
  gate (round-4 `scheduler/backpressure_pause`) still trips at the right depth.
- Edge: `SAMPLE_EVERY=1` preserves current behavior (backward-compat default-off path
  if set to 1; default 100 is the new opt-in speedup).

**Acceptance:** `uv run pytest tests/test_queue*.py tests/test_scheduler_backpressure.py`
green; benchmark (round-8 Tier-B `test_pop_single`) shows measurable pop-path improvement
at default sampling.

**Leverage H · Effort S** (1-line + setting + 2 tests).

---

## U5 — Memory default cap `[OOM prevention, M/S, scientist F5-MEM]`

**Rationale (scientist, measured):** `MemoryMembershipFilter(maxsize=None)` default
silently grows — **~366 MB @ 1M entries · ~3.58 GB @ 10M** (measured ~481 B/entry).
Long crawls with high URL cardinality → silent OOM in prod. The LRU `maxsize`
mechanism ALREADY EXISTS — just ship a sane default. `DelayQueueStrategy` heap
(`queue/strategies/delay.py:69`) has the same unbounded-growth property.

**Files:**
- `src/scrapy_extension/dupefilter/filters/memory_filter.py:32` — change default
  `maxsize: int | None = None` → `maxsize: int | None = 1_000_000` (LRU eviction
  already implemented; None still allowed as explicit opt-out for advanced users).
- `src/scrapy_extension/queue/strategies/delay.py:69` — add soft-cap + warn: when
  `_holding` heap exceeds a configurable threshold (default 100k items), emit a
  one-time warning (mirror factory.py `_warned` pattern) pointing at the
  distributed-delay roadmap (U10).
- `src/scrapy_extension/settings/base.py` — `SCRAPY_MEMORY_FILTER_MAXSIZE: int = 1_000_000`
  + `SCRAPY_DELAY_MAX_HELD: int = 100_000` (threaded into the strategy/factory).

**TDD:**
- `test_memory_filter.py`: insert 1.5M items with default maxsize → assert LRU evicts
  (set_len stays ~1M), not grow; warn fires at threshold.
- `test_delay_strategy.py`: hold >100k items → warn-once fires.
- Backward-compat: explicit `maxsize=None` still unbounded (advanced opt-out).

**Acceptance:** no unbounded growth by default; `uv run pytest tests/test_memory_filter.py tests/test_delay_strategy.py` green.

**Leverage M · Effort S** (mechanism exists; ship defaults + warns).

---

## U8 — `mypy --strict` clean `[type-hint promise, M/S, code-reviewer DX-07]`

**Rationale (code-reviewer):** `py.typed` is a public promise. `mypy` (default) is
clean, but `mypy --strict` reports **25 errors in 11 files** — mostly `Collection`
missing type-args, `Any`-return leaks. Downstream users running strict typing hit
these. Cheapest high-credibility DX win.

**Files:**
- `pyproject.toml` `[tool.mypy]` — add `disallow_any_generics = true`,
  `warn_return_any = true` (incremental; do NOT flip the full `strict = true` flag
  in one step — enable the two cheapest flags, fix, then expand).
- Fix the 25 errors. Anchors (code-reviewer DX-07): `backends/mongodb.py:88,89`
  (`Collection` → `Collection[str, dict[str, Any]]`), `mongodb.py:458,640`
  (`Any`-return → concrete return type or `cast`), `backends/elasticsearch.py:493,521`
  (untyped `dict` → `dict[str, Any]`); plus ~19 more of the same shape.

**TDD:** N/A (types). **Acceptance:** `uv run mypy --strict src/scrapy_extension` →
"Success, no issues" (then consider bumping more strict flags as a follow-up).
Existing `uv run mypy src/scrapy_extension` (default) stays clean.

**Leverage M · Effort S** (one afternoon; mostly mechanical generic-arg annotations).

---

## Recommended next `/goal` execution order

1. **U4 + U5 + U8** as ONE round (all S-effort, file-disjoint, no API break) —
   1-line perf + OOM cap + mypy-strict. ~1 day, maximum trust-per-effort.
2. **U1** as a docs round (README Guarantees) — pairs with U9 (v1.0 stability
   artifacts) from PLAN-round8.
3. **U2** (operability signals) as the v1.0 #2 round — slightly more design
   (new monitor hooks); sequence after the cheap-wins land.

After U1 + U2 + (U3 done) the three v1.0 non-negotiables are met → v1.0 tag is defensible.

## Structural findings (U18 sweep — filled this fire)

Round-8's full pass lost the explore agent to a context-window limit; this
incremental fire ran a NARROW structural sweep (module sizes + dead-code only).
Results:

**Module sizes** — 6 files exceed 600 LOC; **2 exceed the 800-LOC project cap**
(`CLAUDE.md`: "200-400 typical, 800 max"):

| File | LOC | Over cap? |
|---|---|---|
| `backends/redis.py` | **844** | ✘ YES — Lua scripts + 4 mode-connection methods |
| `backends/kafka.py` | **801** | ✘ YES — producer/consumer wrappers |
| `backends/connectors.py` | 736 | — `resolve_backend_config` + pure helpers |
| `backends/mongodb.py` | 714 | — collection/index setup |
| `backends/rabbitmq.py` | 676 | — channel/queue-decl builders |
| `backends/pulsar.py` | 661 | — already per-class split (6 classes) |
| `schedule/scheduler.py` | 606 | — ack-gate + stats |

**Dead code — none.** The 6 `raise NotImplementedError` sites are all legitimate
(capability guards in `connectors.py:685,707,730`; the documented RocketMQ
queue-only stub at `rocketmq.py:288`; the ABC abstract method at
`dupefilter/filters/base.py:108`). The 3 `# pragma: no cover` sites are
unreachable factory ValueError guards. All large public methods have callers.

### U19 — Module split candidates `[refactor, L effort, NON-BLOCKING]`

**Rationale:** `redis.py` (844) + `kafka.py` (801) exceed the project LOC cap.
Not a correctness issue — a maintainability/readability debt. Sequence AFTER
v1.0 Tier-1 (these are not v1.0 blockers).

**Files (candidates, additive splits — no behavior change):**
- `backends/redis.py` → extract `backends/redis_scripts.py` (Lua push/pop script
  constants, ~80 LOC) + `backends/redis_connection.py` (the 4 `_connect_*` mode
  methods, ~150 LOC). `RedisBackend` becomes ~600 LOC.
- `backends/kafka.py` → extract `backends/kafka_helpers.py` (producer/consumer
  config builders + serialization). `KafkaBackend` becomes ~600 LOC.

**TDD:** behavior-preserving — existing `test_redis_backend.py` /
`test_kafka_backend.py` must stay GREEN unchanged (the split is pure extraction;
the public `RedisBackend`/`KafkaBackend` API is identical). Add a test asserting
the extracted helpers are importable + called.

**Acceptance:** both files ≤ 800 LOC; `uv run pytest tests/test_redis_backend.py tests/test_kafka_backend.py` green; ruff/mypy clean.

**Leverage M (maintainability) · Effort L** (careful extraction; sequence post-1.0).

---

## Non-goals (this SPEC)

- Executing any unit (that's the next `/goal`).
- Re-running the round-8 full 6-agent pass (incremental contract; this SPEC is the
  refinement layer).
- Distributed strategies (U10), batch API (U11), OTel (U12) — Tier-2, sequenced after v1.0.
