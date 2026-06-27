# SPEC — Round 14 Six-Dimension Hardening (for the next /goal)

First **full-coverage** insight fan-out after rounds 9-13 closed the v1.0
execution menu. Six read-only auditors (error-handling, lifecycle, strategy,
test-quality, observability, API-stability) swept the codebase and surfaced
**~40 findings**: 3 CRITICAL (1 breaking-change, 2 operability), ~15 HIGH,
~15 MEDIUM, rest LOW. This SPEC rolls them into **8 executable units (R14-A…H)**.

Companion: [`PLAN-round14-six-dimension-hardening.md`](./PLAN-round14-six-dimension-hardening.md).

**Important v1.0 re-assessment:** the EXECUTION-INDEX declared "v1.0 tag
defensible" after R9-13. This audit **re-opens that**: R14-B (undocumented
breaking change) and R14-C (knobs the runbook promises but that don't exist)
are genuine pre-tag blockers. v1.0 should tag **after R14-B + R14-C land**.

## Findings summary (by dimension → unit)

| Dimension | CRIT | HIGH | MED | LOW | → Unit |
|---|:-:|:-:|:-:|:-:|---|
| Error-handling contract | 0 | 3 | 3 | 3 | **R14-A** (storage), R14-H (polish) |
| Resource lifecycle / leaks | 0 | 2 | 4 | 4 | **R14-E** |
| Strategy-layer correctness | 0 | 3 | 4 | 2 | **R14-F** |
| Test quality / coverage | 0 | 3 | 3 | 3 | **R14-G** |
| Observability completeness | 2 | 3 | 3 | 2 | **R14-C** (config), **R14-D** (hooks) |
| API stability / v1.0 contract | 1 | 2 | 3 | 2 | **R14-B**, R14-H |

Deduped against rounds 6/9 (SEC-*, SV1-5, U2/U4/U5) — those are NOT re-reported.

---

## R14-A — StorageBackend error-contract uniformity `[3 HIGH data-loss/leak]`

**Rationale:** The QueueBackend contract is uniformly wrapped (`QueueError`
everywhere), but the **StorageBackend contract is systematically broken in
three different ways** — a downstream `except BackendError` handler catches
the queue path but not storage:

- `backends/memcached.py:134,152,171,189` — `store/retrieve/delete/exists`
  **silently swallow ALL exceptions** to `logger.warning` + `return None/False`.
  A failed `store()` returns `None` → the item pipeline believes the item was
  stored. **Data-loss contract bug.**
- `backends/dynamodb.py:165,183,212,230,252` — same swallow pattern; worse,
  `delete()` masks `ClientError(ThrottlingException)` as "didn't exist" →
  dedup layer re-emits dropped items.
- `backends/mongodb.py:620,642,663,681,698,715` — inverse bug: storage ops have
  **no try/except at all** → raw `pymongo.errors.PyMongoError` (e.g.
  `AutoReconnect`) propagates unwrapped, breaking the uniform catch contract.

**Fix:** introduce `StorageError(BackendError)` in `exceptions/base.py`; wrap
each storage op to raise it (memcached: stop swallowing → raise; dynamodb:
selective `botocore.exceptions.ClientError` catch — raise on throttling, only
swallow `ResourceNotFoundException`; mongodb: wrap `PyMongoError`→`StorageError`).

**TDD:** RED = each storage op's failure path currently returns wrong sentinel
/ leaks raw exception; GREEN = raises `StorageError` with `operation=`/`key=`.
**Acceptance:** `except BackendError` catches every storage-path failure across
all 3 backends; no silent data loss; `uv run pytest -q` green.

**Leverage H · Effort M** (1 new exception + 3 backend files + tests).

---

## R14-B — v1.0 breaking-change disclosure + public-contract freeze `[CRITICAL breaking + HIGH contract]`

**Rationale:** Two issues make the v1.0 surface lie about its own contract:

1. **CRITICAL — undocumented breaking change.** Round-9c SV3-2 made
   `PulsarSettings(auth_token=…)` **require** `service_url` to start with
   `pulsar+ssl://` (`settings/pulsar.py:117-144`). A user running
   `SCRAPY_PULSAR_AUTH_TOKEN=xxx` + `pulsar://broker:6650` (token over plaintext,
   their network choice) will see their crawler **refuse to start** after
   upgrading. SV3-3 (Redis `ssl_enabled=True`→require `ssl_cafile`) has the same
   profile. Neither is in `CHANGELOG.md` or `STABILITY.md`. Before v1.0 tags
   this is fixable-by-documentation; after, it's a SemVer violation.
2. **`ConfigurationError.setting_name`/`setting_value` are de-facto public**
   (operators catch + log them; README:386 names them in prose) but
   `STABILITY.md`'s Stable table does **not** list them — a rename would break
   downstream log handlers silently.
3. **3rd-party-backend string rejected at the Settings layer.**
   `settings/base.py:38` `backend_type: BackendType` uses pydantic enum coercion;
   `BackendType._missing_` (`backends/base.py:244`) raises `ValueError` for
   unknown values → `SCRAPY_BACKEND_TYPE=myplugin` raises pydantic `ValidationError`
   **before** the registry-aware `resolve_backend_config` can accept it. This
   contradicts round-5 R5-1 ("3rd-party backends route through the same path").
   Same root cause gives inconsistent exception families (`ValidationError` vs
   `ConfigurationError`) for what users see as the same "bad backend type" mistake.

**Fix:** (a) Add a prominent **Breaking** section to `CHANGELOG.md` + a
"Round-9 hardening (breaking)" note in `STABILITY.md` naming Pulsar token→SSL
and Redis ssl→cafile coercion. (b) Add a Stable row to `STABILITY.md` for
`ConfigurationError` attributes + the `_SENSITIVE_NAME_FRAGMENTS` redaction
heuristic. (c) Widen `Settings.backend_type: BackendType | str` with a validator
that accepts any registry-known string, re-raising `ConfigurationError` (not
pydantic `ValidationError`) for unknown values — fixes both #3 and the
exception-family inconsistency.

**TDD:** RED = `Settings(backend_type="myplugin")` raises `ValidationError` /
CHANGELOG has no Breaking entry; GREEN = accepts registered string OR raises
`ConfigurationError(setting_name="SCRAPY_BACKEND_TYPE")`; Breaking entry exists.
**Acceptance:** 3rd-party backend selectable via `SCRAPY_BACKEND_TYPE`;
breaking changes documented; contract attrs frozen in STABILITY.

**Leverage H (v1.0 tag gate) · Effort S-M.** Depends on: nothing.

---

## R14-C — Operability configurability (deferred settings-wiring) `[CRITICAL]`

**Rationale:** Round 9 (U4/U5) and Round 12 (U2) shipped `depth_sample_every`,
`max_item_bytes`, `max_held`, `backpressure_threshold`, and the pop-rate window
as **constructor defaults only** — the settings-wiring was explicitly deferred.
`schedule/scheduler.py:343` constructs `BackendQueue(...)` **without threading
any of them**, so they're permanently stuck at defaults. `docs/runbook.md:104`
tells operators to "tune via settings" — **but the settings don't exist.** This
is the single biggest operability gap: the signals are observable but the knobs
that govern them are not configurable without code changes.

**Files + rules:**
- `settings/base.py`: add `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY`, `SCRAPY_QUEUE_MAX_ITEM_BYTES`,
  `SCRAPY_QUEUE_DELAY_MAX_HELD`, `SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD`,
  `SCRAPY_MONITOR_POP_RATE_WINDOW_S` (the orphaned `queue_max_item_bytes`/`pipeline_max_item_bytes`
  fields at lines 60/70 already exist but are never read — wire them).
- `schedule/scheduler.py:343`: read the settings in `from_settings` and pass to `BackendQueue(...)`.
- `queue/strategies/factory.py:45`: accept `max_held` kwarg → `DelayQueueStrategy`.
- `monitor/stats.py:55`: accept `backpressure_threshold` + `pop_rate_window_s`.
- `queue/queue.py:128`: accept `pop_rate_window_s`.

**TDD:** RED = setting env var doesn't change behavior; GREEN = the constructed
`BackendQueue`/strategy/monitor reflects the setting. **Acceptance:** every
runbook "tune via settings" instruction has a real `SCRAPY_*` setting behind it.

**Leverage H · Effort M.** Depends on: nothing (builds on landed U4/U5/U2 code).
**Shares `settings/base.py` + `monitor/` with R14-B/R14-D → sequence B→C→D.**

---

## R14-D — Observability completeness `[CRITICAL dead hook + HIGH gaps]`

**Rationale:** U2 landed two hooks correctly on their single happy path each,
but the observability surface is **half-wired**:

- **CRITICAL — `on_error` is dead observability.** Defined in `monitor/base.py:176`,
  implemented in `stats.py:160`, but **zero call sites** (`grep -rn on_error src/`
  → only the two definitions). Either wire it (`queue.py:189,268`,
  `scheduler.py:504,576`) or delete it from the protocol — today it's a lie on
  the interface.
- **Bloom + Memory filters skipped by `on_filter_saturation`.** The hook gates
  on `getattr(filter, "saturation", None)`; cuckoo has it, but **Bloom has a
  `capacity` and no `saturation` property**, and **Memory filter's LRU eviction**
  (`memory_filter.py:86`) signals only via a bare `logger.warning`, bypassing the
  monitor entirely. Same capacity concept, silently unobservable.
- **`on_pop` semantics contradict docstrings.** `queue.py:237` fires on every pop
  *attempt* (incl. empty) but docstrings (`base.py:87`, `stats.py:75`) say "per
  successful pop"; `on_push` fires only on success → `pop_count` is misleadingly
  named (it's attempts). Either rename to `pop_attempt_count` or split the hooks.
- **No connection-lifecycle hooks.** `ConnectionManager` retry/connect is
  observable only via internal `logger`. Add `on_connect`/`on_disconnect`/`on_retry`
  wired from `connectors.py` (stats: `backend/{connect,retry,disconnect}_count`).

**Fix:** wire `on_error` at the push/pop/deserialize fail arms; add `saturation`
+ `capacity` to `BloomMembershipFilter`; emit `on_filter_saturation` from
memory-filter eviction; fix `on_pop` docstring/stat-name (or split); add the 3
connection-lifecycle hooks.

**TDD:** RED = the hook is never called / filter has no `saturation`; GREEN =
called with correct args. **Acceptance:** every documented monitor hook is
emitted on the path it claims; Bloom/Memory saturation observable in stats.

**Leverage H · Effort M.** Depends on: R14-C (shares `monitor/` + `settings/base.py`).

---

## R14-E — Lifecycle bounds (long-run leak prevention) `[2 HIGH + MEDIUM]`

**Rationale:** ConnectionManager-layer hygiene is sound (R7 B5 + R25-A1), but
there is a **systematic "unbounded growth" leak class** that bites long-running
multi-spider processes:

- **HIGH — unbounded `_managers` registry.** `connectors.py:330` keys
  `ConnectionManager._managers` by `backend_type:settings_hash`; entries are
  evicted only on the *last* refcounted `close()`. A crawler with rotating
  settings (per-spider creds, unique `group_id`) produces a fresh entry each
  time; the prior entry (holding a live `Backend` + open sockets) is never
  reclaimed → unbounded growth until process exit.
- **HIGH — RabbitMQ partial-state.** `rabbitmq.py:264-266` assigns
  `_connection`/`_channel` **before** `_setup_qos()`/HA-policy; if QoS raises
  `AMQPError`, `is_connected()` returns True on a half-init channel. Reorder +
  reset to None on failure (mirror the R25-A1 pattern).
- **MED — Kafka partition dicts.** `kafka.py:158-167` `_in_flight`/`_watermarks`/`_high_water`
  grow per-partition, never pruned on ack — unbounded across partition churn.
- **MED — diagnostic `_in_flight` sets** (pulsar/sqs/rabbitmq) grow one entry per
  unacked pop; bound them (LRU cap or convert to Counter).
- **MED — circuit-breaker** never `reset()` on `ConnectionManager.close()`;
  orphaned managers hold a breaker stuck OPEN with stale state.

**Fix:** cap `_managers` (LRU `OrderedDict`, `MAX_MANAGERS=32`, evict victim via
`disconnect()`); reorder RabbitMQ QoS + null-on-fail; prune Kafka partition keys
when `_in_flight[p]` empties; bound diagnostic sets; call `_breaker.reset()` in
`ConnectionManager.close()`.

**TDD:** RED = simulate N distinct settings / N partitions → registry/set grows
unbounded; GREEN = capped (eviction fires, victim disconnected). **Acceptance:**
no unbounded growth under settings/partition churn; `is_connected()` truthful
post partial-failure.

**Leverage H · Effort M.** Depends on: nothing. **Touches `connectors.py` +
`backends/{kafka,rabbitmq,pulsar,sqs}.py` + `circuit_breaker.py`.**

---

## R14-F — Queue-strategy correctness `[3 HIGH real-world bugs]`

**Rationale:** The strategy layers are happy-path-correct but leak silent
contract bugs at the strategy↔queue↔backend boundary:

- **HIGH — Delay drops priority on drain.** `queue/strategies/delay.py:174`
  `_drain_ready()` calls `qb.push(queue_name, item)` with **no `priority=`**;
  `push()` accepts it (docstring claims "used once drained") but the heap tuple
  never stores it. Every delayed item lands at priority 0 → **silent priority
  inversion** for any user mixing `delay` + `priority`.
- **HIGH — RoundRobin leaks empty sources.** `round_robin.py:62-66` never
  removes drained-source keys from `_sources`; `_idx` rotates through empty
  slots → unbounded `_sources` + O(n) pop cost on a long crawl with transient
  sources.
- **HIGH — retry + delay storm.** `queue/queue.py:206-210` reads
  `delay`/`source` from `request.meta`, but `_request_to_dict` does **not** strip
  them → a re-pushed retry **re-applies the same delay**, potentially forever.
  The most likely real-world bug here (retry + delay is a common combination).
- **MED — Throttle per-instance.** `throttle.py:92-98` rate budget is
  per-instance; two spiders in one process silently double the aggregate rate.
  No bound on `min_interval` (`1e9` → queue looks permanently empty, DoS via
  misconfig).

**Fix:** store priority in the delay heap tuple + re-pass on drain; `del
_sources[source]` when drained + reset `_idx`; pop `delay`/`source` from meta
after reading in `push` (or document caller must clear); document/bound Throttle.

**TDD:** RED = delayed item drains at priority 0 / retry re-delays / RR grows;
GREEN = priority retained / retry not re-delayed / RR bounded. **Acceptance:**
priority survives delay; retries don't storm; RR bounded.

**Leverage H · Effort M.** Depends on: nothing. **Touches only `queue/` (file-disjoint from backends).**

---

## R14-G — Test-coverage hardening (backend layer → 95%+) `[3 HIGH + MEDIUM]`

**Rationale:** Total coverage is 95.19% but **propped up by tiny fully-covered
files**; the backend layer (where bugs live) is 87-95%. Systematic blind spots:
- **HIGH — MongoDB `not-connected` guards** (`mongodb.py` 13 branches across 3
  collections) — the primary corruption-prevention contract, entirely untested.
- **HIGH — registry 3rd-party-plugin error paths** (`registry.py:203-282`) —
  broken plugin should skip-with-warning, not crash discovery; untested.
- **HIGH — connectors A2 single-connect-owner error-signal** (`connectors.py:584-602`)
  — the load-bearing concurrency fix; the owner-fails branch is untested (and
  this is the region of the known `test_create_backend_redis` flake — fix its
  fixture isolation too).
- **MED — no property tests for SV3/U4/U5** (validators only tested with
  hand-picked values; no fuzz for cross-field consistency or cap boundaries).
- **MED — integration tier bit-rotted.** `tests/integration/` has 7 e2e files,
  no CI gate / marker / skip-unless-env guard → silently rotting.

**Fix:** add the missing backend-layer tests (mongodb disconnect guards,
registry plugin-error paths, connectors owner-fail threading test); add
`tests/test_property_settings.py` (Hypothesis for SV3/U4/U5); add
`@pytest.mark.integration` + skip-unless-env gate; fix the registry-mock
fixture isolation for the flake.

**TDD:** (this unit IS tests). **Acceptance:** every backend file ≥95%;
property tests green; integration tier runnable via a marker; the
`test_create_backend_redis` flake gone.

**Leverage M · Effort M-L.** Depends on: **ideally after R14-A/E/F** (so it
tests the new behavior too). **Touches only `tests/` + `conftest.py`.**

---

## R14-H — Lazy-import hygiene + polish `[HIGH + LOWs]`

**Rationale:** cleanup bundle.
- **HIGH — misleading install hint.** `__init__.py:138-144` (mirror
  `backends/__init__.py:42-52`) wraps **any** `ImportError` as "requires
  additional dependencies. Install with: pip install scrapy-extension[redis]" —
  even when redis IS installed but a real bug inside the backend module raised
  `ImportError`. Narrow: only re-wrap if `e.name` matches the documented optional
  dep; else re-raise original.
- **LOW — rocketmq redundant `except OSError`** (`rocketmq.py:115,228,266`) —
  dead branch (`Exception` already covers it); drop or make message distinct.
- **LOW — bloom/memory `_count` docstring nits; warn-once cap-message clarity.**

**Fix:** narrow the `__getattr__` except; drop dead branches; docstring polish.
**Acceptance:** a real `ImportError` inside a backend surfaces its real chain,
not the install hint.

**Leverage M · Effort S.** Depends on: nothing.

---

## Recommended /goal batching (file-disjoint waves)

See [PLAN-round14-six-dimension-hardening.md](./PLAN-round14-six-dimension-hardening.md)
for the dependency-ordered, file-disjoint execution waves + per-unit TDD outlines.

## Non-goals (this SPEC)

- Executing R14-A…H (next `/goal`).
- The Post-1.0 Tier-2/3 backlog (U19 module splits, U10-U17) — still deferred;
  R14 is pre-/at-v1.0 hardening, not feature expansion.
- Re-auditing dimensions rounds 6/9 already closed.
