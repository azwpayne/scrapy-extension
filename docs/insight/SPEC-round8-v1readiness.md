# SPEC — Round 8 v1.0-Readiness: U2 Operability + U9 Stability (for the next /goal)

Second `/loop` incremental fire. Completes executable-spec coverage of the three
v1.0 non-negotiables (critic verdict, [`PLAN-round8-forward.md`](./PLAN-round8-forward.md)):
U1 #1 (README Guarantees — in [`SPEC-round8-tier1.md`](./SPEC-round8-tier1.md)),
U2 #2 (operability signals — here), U3 #3 (multi-backend e2e — DONE `3cef50c`).
Plus U9 (stability artifacts) which makes the v1.0 tag defensible. Each unit is
directly executable by a future `/goal` fan-out.

---

## U2 — Operability signals `[v1.0 non-negotiable #2, critic O1/V2, H/M]`

**Rationale (critic O1/V2):** An operator paged on a 0-req/min crawl sees counters
stop incrementing but cannot distinguish backend-down / queue-empty / throttle-pinned /
dedup-saturated / worker-crash. Monitor emits push/pop/dedup/store counters + one
`queue/depth` gauge — NO rate signal, NO saturation signal. Hardening covered
"correct under failure"; this covers "diagnose why it stopped."

**Files:**
- `src/scrapy_extension/monitor/base.py` — two new no-op hooks: `on_pop_rate(window_s: float, rate: float)` and `on_filter_saturation(used: int, capacity: int | None)`.
- `src/scrapy_extension/monitor/stats.py` — `ScrapyStatsMonitor` impls: `on_pop_rate` → `set_value("queue/pop_rate_1m", rate)`; `on_filter_saturation` → `set_value("dupefilter/filter_saturation", used/capacity if capacity else 1.0)`.
- `src/scrapy_extension/dupefilter/filters/cuckoo_filter.py` — expose `_count` + `_num_buckets * _BUCKET_SIZE` (capacity) via a read property `saturation` (no behavior change; just observable).
- `src/scrapy_extension/queue/queue.py` — rolling 1m pop counter (a small ring/deque of timestamps, or a `time.monotonic()` windowed counter) emitting `on_pop_rate` periodically (NOT every pop — sample like U4, e.g. every Nth pop compute the delta).
- `src/scrapy_extension/dupefilter/dupefilter.py` — after each `request_seen`, if the filter exposes `saturation`, emit `on_filter_saturation` (cheap; only cuckoo has it — others no-op).

**TDD:**
- `test_monitor.py`: `on_pop_rate` sets `queue/pop_rate_1m`; `on_filter_saturation` sets `dupefilter/filter_saturation` (extend the existing per-hook stat tests + parametrize table).
- `test_load_scale.py` or new `test_operability.py`: drive 100 pops within a mocked 60s window → assert `queue/pop_rate_1m` reflects ~100/min; drive cuckoo near capacity → assert `dupefilter/filter_saturation` rises.
- Default-off safety: `NullMonitor` + a bare `Monitor()` are no-op (no crash when no crawler).

**Acceptance:** a simulated stuck crawl (no pops for 60s) produces `queue/pop_rate_1m=0`; a cuckoo at 95% capacity produces `dupefilter/filter_saturation=0.95`. Operator can diagnose from stats alone.

**Leverage H · Effort M** (~1 day: 2 monitor hooks + rolling counter + saturation property + tests).

---

## U9 — v1.0 stability artifacts `[architect F-6, critic G2, H/S]`

**Rationale (critic G2, 3-way):** v1.0 implies a stability commitment; the artifacts
that commitment requires are absent. No `STABILITY.md` (which surface is frozen vs
experimental?), no `SECURITY.md` (disclosure path), no `CHANGELOG.md`, no release
runbook. A downstream user can't tell what's safe to depend on.

**Files (all NEW):**
- `STABILITY.md` — component tiers:
  - **Stable** (frozen public API, semantic-versioning promise): `BackendScheduler`, `BackendDupeFilter`, `BackendPipeline`, `BackendQueue`, `BackendSpiderMixin`, the `Monitor` ABC, the 10 bundled backends' public methods, all `SCRAPY_*` settings already shipped.
  - **Experimental** (may change in a minor bump): `BackendDescriptor` entry-point registration (round-5, no 3rd-party ecosystem yet), the new round-7 `FilterFull` + `on_filter_full` hook (fresh, want flexibility), `backpressure_pause_at`/`resume_at` (round-4, fresh).
  - **Internal** (`_`-prefixed, no stability promise): everything in `_redaction.py`, `_filter_full_warned`, all `_connect_*` methods.
  - **RocketMQ**: explicitly marked Experimental + supply-chain caveat (round-7 accepted unmaintained dep).
- `SECURITY.md` — disclosure path (report to <SECURITY_EMAIL or GitHub Security Advisories>), supported versions, the `[rocketmq]`/`[all]` supply-chain gate note, response SLA.
- `CHANGELOG.md` — seed with the rounds 1-8 arc (Keep a Changelog format; the commit history has the detail). Sections: Added (multi-backend, strategies, entry-points, monitor hooks) / Changed / Fixed (the round 1-7 hardening) / Security (round-6).
- `docs/release-runbook.md` — version bump (`uv version`), `uv lock` sync, CHANGELOG update, tag, push, PyPI publish (`uv build && uv publish`), post-release verify.

**TDD:** N/A (docs). **Acceptance:** a new downstream user can answer "is `BackendScheduler` safe to depend on?" (STABILITY.md), "how do I report a vuln?" (SECURITY.md), "what changed since 0.1.0?" (CHANGELOG.md), "how do I cut a release?" (runbook).

**Leverage H · Effort S** (half-day of writing; no code).

---

## v1.0-readiness scoreboard (after these two + SPEC-round8-tier1)

| Non-negotiable | Unit | Spec status | Execution |
|---|---|---|---|
| #1 README honesty | U1 | ✅ spec'd (SPEC-round8-tier1) | next /goal |
| #2 Operability signals | U2 | ✅ spec'd (this doc) | next /goal |
| #3 Real multi-backend e2e | U3 | ✅ DONE (`3cef50c`) | — |
| Tag-defensibility | U9 | ✅ spec'd (this doc) | next /goal |

**Once U1 + U2 + U9 execute, the v1.0 tag is defensible.** The next `/goal` should
batch U1+U9 (both docs, S effort, one round) + U2 (M effort, one round) → then tag.

## Supply-chain hygiene (dependency audit — this fire's new lens)

The dependency audit (this `/loop` fire's NEW insight dimension — rounds 1-8 only
flagged RocketMQ) found one real risk beyond the known RocketMQ + two major-version
lags. `uv lock --check` is in sync. Detail:

| dep | resolved | latest | signal | sev |
|---|---|---|---|---|
| **pymemcache** | 4.0.0 | 4.0.0 | **unmaintained 1348d** (last release 2022-10-17); the `memcached` storage backend depends on it | **H** |
| redis | 7.4.0 | 8.0.1 | major-behind (7→8), upstream active; cap `<9` permits 8.x but pin not bumped | M |
| elasticsearch | 8.19.3 | 9.4.1 | major-behind (8→9), upstream active; cap `<9` **hard-blocks** 9.x | M |
| kafka-python-ng | 2.2.3 | 2.2.3 | current-at-latest but 632d since release (fork of dead kafka-python) — watch | L |
| pydantic-settings / scrapy / pymongo / pulsar-client / hypothesis | — | — | current | OK |
| boto3 / poethepoet / pytest / ruff | — | — | patch/minor behind (fast-cadence dev tools) | L (routine) |

(RocketMQ-client-python 2381d stale is round-7 ACCEPT'd — not re-reported.)

### U20 — pymemcache unmaintained `[supply-chain, H]`
**Rationale:** `pymemcache==4.0.0` (the `memcached` storage backend's dep) has had
no release in 1348 days. Same supply-chain shape as the round-7 RocketMQ finding.
The memcached backend is storage-only (KV+TTL) — smaller surface than RocketMQ but
the same "shipped backend on an unmaintained lib" risk.
**Files:** `pyproject.toml` (`[memcached]` extra) + decision in `STABILITY.md` (mark
Memcached Experimental, mirroring RocketMQ per critic B1). Options (pick one):
(a) document risk + add a CI canary import test; (b) evaluate `python-memcached`/
`pylibmc`/pure-stdlib fallback; (c) vendor the minimal `get/set/delete/expire`
surface actually used.
**Acceptance:** Memcached risk is either documented-as-experimental or migrated;
no shipped backend silently depends on an unmaintained lib without a caveat.
**Leverage H · Effort S** (decision + doc) to **M** (if migrate).

### U21 — bump redis + elasticsearch caps, validate `[dep freshness, M]`
**Rationale:** redis 8.0.1 (3 days old) + elasticsearch 9.4.1 (31 days old) are
actively shipped; the project caps hard-block ES 9.x and the redis pin lags 8.x.
Feature lag, not breakage — but capping out major versions accumulates debt.
**Files:** `pyproject.toml` (`elasticsearch>=8.12.0,<9` → `,<10`; `redis>=7.3.0,<9`
already permits 8.x → bump the `uv.lock` pin); `uv.lock` re-resolve.
**TDD:** run the existing `test_redis_backend.py` + `test_elasticsearch_backend.py`
(+ coverage) suites against the bumped client versions; fix any client-API breakage.
Integration-tier real-broker validation (round-8 Tier-I) gates the final confidence.
**Acceptance:** `uv run pytest tests/test_redis_backend.py tests/test_elasticsearch_backend*.py` green against redis-py 8.x / elasticsearch-py 9.x; caps widened.
**Leverage M · Effort M** (re-resolve + validate 2 backends' client API).

### Watch (no unit yet)
**kafka-python-ng 632d** — current-at-latest but a long gap for a maintained fork.
Quarterly check; if no 2.2.4+ by 2026-Q4, evaluate aiokafka/confluent alternatives.
Record in `STABILITY.md` supply-chain notes.

---

## Non-goals (this SPEC)

- Executing U2/U9 (next `/goal`).
- U10+ (distributed strategies, batch API, OTel) — Tier-2, post-1.0.
- Re-running round-8 agents (incremental contract).
