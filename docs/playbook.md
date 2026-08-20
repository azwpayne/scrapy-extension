# Playbook

Operational handbook for engineers working on `scrapy-extension`. This is the
"how we work here" companion to the user-facing references: behavior contracts
live in [`README.md`](../README.md) and [`runbook.md`](runbook.md), the plugin
contract in [`backend-plugins.md`](backend-plugins.md), persisted-state
semantics in [`migration-guide.md`](migration-guide.md). When this document and
the code disagree, the code wins — then fix this document.

Generated from a full-repo deep read (2026-08-15); file:line citations are
point-in-time and may drift.

## Table of contents

1. [Sixty-second orientation](#sixty-second-orientation)
2. [Environment and everyday commands](#environment-and-everyday-commands)
3. [Quality gates and CI](#quality-gates-and-ci)
4. [Architecture map](#architecture-map)
5. [Configuration system](#configuration-system)
6. [Backend matrix](#backend-matrix)
7. [Load-bearing invariants](#load-bearing-invariants)
8. [How-to recipes](#how-to-recipes)
9. [Testing playbook](#testing-playbook)
10. [Troubleshooting and traps](#troubleshooting-and-traps)
11. [Docs map and the insight loop](#docs-map-and-the-insight-loop)
12. [Change workflow](#change-workflow)

## Sixty-second orientation

`scrapy-extension` is a Scrapy extension for distributed crawling with:

- **10 pluggable backends** (Redis / MongoDB / ElasticSearch / Kafka /
  RabbitMQ / RocketMQ / Pulsar / SQS / Memcached / DynamoDB) behind three
  capability interfaces — `QueueBackend`, `SetBackend`, `StorageBackend`
  (`src/scrapy_extension/backends/base.py`).
- **Three pluggable strategy layers** above those interfaces: dedup
  (`MembershipFilter`: set/memory/bloom/cuckoo), queue semantics
  (`QueueStrategy`: passthrough/delay/round_robin/throttle/priority/
  time_wheel/work_stealing/ring_buffer), and storage
  (`StorageStrategy`: passthrough/batched). All selected via Scrapy settings —
  no code change.
- **Five Scrapy components** wiring it into a crawl: `BackendScheduler`,
  `BackendDupeFilter`, `BackendPipeline`, `BackendQueue`, `BackendSpiderMixin`,
  plus a no-op-default `Monitor` observability protocol.
- **One connection layer**: a refcounted, thread-safe `ConnectionManager`
  registry (`backends/connectors.py`) that every component goes through, with
  retry/backoff, optional circuit breaker, and per-component backend
  resolution (queue in Redis, dedup in MongoDB, storage in ES — simultaneously).

Everything is **pure-Python, lazy-imported, and strictly typed**: `pip install
scrapy-extension` works with zero backend deps; each backend's client is an
optional extra loaded on first use (PEP 562). Python 3.10+, built with uv.

## Environment and everyday commands

The project uses **uv** exclusively (`uv_build` backend, `uv.lock` is
authoritative; CI syncs with `--locked`):

```bash
uv sync --locked --group test     # exact dev environment (CONTRIBUTING.md)
uv run pytest                     # full unit suite (integration/benchmarks auto-skip)
uv run pytest tests/test_backends.py                          # one file
uv run pytest tests/test_backends.py::TestRedisBackend::test_connect_success -v
uv run poe check                  # read-only: format-check + lint + type-check + security
uv run poe format-fix             # ruff format src tests conftest.py (the only rewriter)
uv run poe lint-fix               # ruff check --fix --unsafe-fixes
uv run mypy --strict src
uv run poe test                   # 3.10–3.14 + 3.14t matrix (uv fetches interpreters)
uv run poe test-py310             # single lane
uv build                          # sdist + wheel -> dist/
uv run poe clean                  # caches/coverage/reports
```

Integration stack (opt-in, see [Testing playbook](#testing-playbook)):

```bash
docker compose --profile optional -f tests/integration/docker-compose.yml up -d --wait
SCRAPY_TEST_INTEGRATION=1 SCRAPY_TEST_REDIS_URL=redis://localhost:6379/0 \
  uv run pytest tests/integration -m integration -q \
  --allow-hosts=localhost,127.0.0.1,::1
```

## Quality gates and CI

CI (`.github/workflows/ci.yml`) runs on every PR, in this order — a locally
green pytest is **not** a green CI:

1. `ruff check` (all lanes) — lint before anything else.
2. `ruff format --check src tests conftest.py` (3.10 lane) — the repo is
   **4-space indent, ruff-format-enforced**.
3. `mypy --strict src` (3.10 lane) — the `py.typed` promise. The only
   relaxation is per-module `disallow_untyped_calls = false` for stub-less
   3rd-party clients (redis, kafka, pika, pymemcache, rocketmq, pulsar).
4. `bandit -r src -c pyproject.toml` + `uv audit --locked` (one pinned
   advisory ignore, `PYSEC-2017-83`, with in-file rationale).
5. Build + verify + smoke-test the package artifacts (3.10 lane): build →
   grep wheel/sdist listings against a forbidden regex (secrets /
   credentials / key material / `docs/insight` must not ship — overlapping
   with, not identical to, the broader `source-exclude` list in
   `pyproject.toml`; keep both consistent) → fresh wheel install of `[all]`,
   asserting every `Backend/Mode/Settings` trio imports.
6. `pytest -m "not integration" --cov` (3.10 lane) runs in branch mode with
   the fixed `--randomly-seed=1125147632` coverage order. Coverage.py
   intentionally has `fail_under = 0`; CI reads `coverage.json` and separately
   enforces **95% statement coverage** and **91% branch coverage**. Reproduce the
   complete JSON gate locally with:

   ```bash
   uv run --no-sync pytest -m "not integration" --tb=short -q \
     --randomly-seed=1125147632 --cov=scrapy_extension \
     --cov-report=term-missing --cov-report=json:coverage.json
   uv run --no-sync python - <<'PY'
   import json
   from pathlib import Path

   totals = json.loads(Path("coverage.json").read_text())['totals']
   statement = 100 * totals['covered_lines'] / totals['num_statements']
   branch = 100 * totals['covered_branches'] / totals['num_branches']
   assert statement >= 95.0 and branch >= 91.0
   print(f"statement={statement:.2f}% branch={branch:.2f}%")
   PY
   ```
7. Unit matrix 3.10–3.14 (`fail-fast: false`); one 3.12 integration job with
   10 live containerized backends.

Also: `--disable-socket` is in global pytest addopts (unit tests must not open
sockets); `filterwarnings = ["error::UserWarning", ...]` makes warnings errors
with only exact-message exemptions; `testpaths = ["tests", "src"]` (files
under `src/` are collected too). CodeQL runs daily on schedule plus push/PR;
Dependabot bumps daily (`rebase-strategy: disabled` — expect frequent
`uv.lock`-only PRs; validate them through the same locked gates).

## Architecture map

```
Scrapy crawl
  └─ BackendScheduler ─┐            BackendDupeFilter ──► MembershipFilter
     (SCHEDULER)       │               (DUPEFILTER)        (set/memory/bloom/cuckoo)
  └─ BackendPipeline ──┤                BackendSpiderMixin (spider convenience)
     (ITEM_PIPELINES)  │
                      ▼
                 BackendQueue ──► QueueStrategy (8 built-ins; snapshots persist
                      │            to a StorageBackend for in-process strategies)
                      ▼
             ConnectionManager.get_{queue,set,storage}_backend()
             (registry: backend_type:sha256(settings); retry/backoff;
              optional CircuitBreaker proxy; monitor hooks)
                      ▼
      QueueBackend / SetBackend / StorageBackend  (backends/base.py ABCs)
                      ▼
      redis mongodb elasticsearch | kafka rabbitmq rocketmq pulsar sqs
      | memcached dynamodb   +  3rd-party entry-point plugins
```

Layer rules:

- Components never touch a backend directly — everything goes through
  `resolve_backend_config()` + `ConnectionManager`.
- Strategy layers never import backend modules; they receive interfaces.
- `backends/registry.py` stores **dotted-path strings only** — registry build
  never imports a backend module, preserving the zero-dep core.
- Two different `_redaction.py` files exist and do different jobs:
  `exceptions/_redaction.py` (error-boundary decorators that scrub tracebacks)
  vs `backends/_redaction.py` (connection-string redaction). Don't conflate.

## Configuration system

**Global `Settings`** (`settings/base.py`, env prefix `SCRAPY_`): backend_type
(`redis` default), serializer (json only), retry_attempts=3 (0–20),
retry_delay=1.0s, reactor_io_timeout=5s, queue_max_item_bytes=1 MiB,
pipeline_max_item_bytes=1 MiB,
storage_strategy=passthrough, storage_buffer_max_age_s=None,
storage_buffer_max_pending=None, dedup_strict=False,
pipeline_max_storage_errors=10 (explicit None opts into best-effort loss),
circuit_breaker_enabled=False,
circuit_breaker_failure_threshold=5, circuit_breaker_reset_timeout=30.0
(cap 3600), backpressure_pause_at/resume_at=None,
queue_depth_sample_every=100, queue_delay_max_held=100_000,
monitor_backpressure_threshold=1_000, monitor_pop_rate_window_s=60.0.

`SCRAPY_REACTOR_IO_TIMEOUT` is the bounded reactor-facing wait budget.
Pipeline writes/lifecycle, scheduler connection warm-up/manager release, and
ACK/NACK callbacks that can return Deferreds are moved to the Twisted thread
pool.
Scrapy's scheduler `enqueue_request`, `next_request`, and
`has_pending_requests` methods are inherently synchronous; they retain the
selected backend's native RPC timeout contract and only cap manager retry
waits. The setting cannot interrupt an arbitrary blocking third-party SDK call.

**Per-backend models** (`settings/<name>.py`, each with
`SCRAPY_<NAME>_` env prefix, `extra="forbid"`, `hide_input_in_errors=True`):
direct construction may raise a redacted Pydantic `ValidationError` for
field/type/range/enum constraints, and explicit policy validators may raise the
project's `ConfigurationError`. All subclass `RedactedBaseSettings`, which
rebuilds either failure so raw input and secrets do not survive in tracebacks.
At the runtime/connect boundary, `ConnectionManager` catches settings
`ValidationError` and validator `ConfigurationError` (plus construction
`TypeError`) and exposes `ConfigurationError`; configuration failures are
non-retryable rather than becoming opaque driver errors.

**Per-component backend override** (multi-backend coexistence) is resolved by
`resolve_backend_config(settings, type_key, settings_key,
required_capabilities)` in `connectors.py`, used by all three component
factories. Precedence: Scrapy per-component type → Scrapy
`SCRAPY_BACKEND_TYPE` → env per-component → env `SCRAPY_BACKEND_TYPE` →
`"redis"`. Empty string counts as unset. `required_capabilities` fail-fasts
(e.g. Kafka for dedup is rejected at config time, not first use).

**Strategy selection** is via raw Scrapy settings read by component
factories, not the Settings model: `SCRAPY_QUEUE_STRATEGY`,
`SCRAPY_DEDUP_STRATEGY`, `SCRAPY_STORAGE_STRATEGY` (+ per-strategy knobs).

**Circuit-breaker policy** (`SCRAPY_CIRCUIT_BREAKER_{ENABLED,
FAILURE_THRESHOLD, RESET_TIMEOUT}`) is resolved Scrapy-settings-first, env
second, into private `__connection_manager_circuit_breaker_*` keys inside the
manager settings hash — distinct credentials or policies mean distinct
managers.

## Backend matrix

| Backend | Queue | Set | Storage | Modes | Ack model | Notable |
|---|---|---|---|---|---|---|
| redis | ✔ | ✔ | ✔ | standalone, master_slave\*, sentinel, cluster | consume-in-pop | ZSET+hash+counter with hash-tagged keys `{ns:queue:name}:*` for cluster-slot Lua atomicity |
| mongodb | ✔ | ✔ | ✔ | standalone, replica_set, sharded_cluster, atlas | consume-in-pop | priority negated, sort (priority, created_at); counts capped at 100k |
| elasticsearch | ✔ | ✔ | ✔ | standalone, cloud | consume-in-pop | optimistic-lock delete (`if_seq_no`); primary-success mutation/refresh accepts yellow clusters; clear refreshes before and after delete-by-query |
| kafka | ✔ | — | — | standalone, cluster, confluent | token (offset watermark) | commits contiguous low-watermark only; `enable_auto_commit` forbidden |
| rabbitmq | ✔ | — | — | standalone, cluster, mirrored_queues | token (delivery_tag + channel generation) | durable push receipt; clear refuses with pending deliveries |
| rocketmq | ✔ | — | — | standalone, cluster, cloud | token (message + consumer generation) | gRPC **proxy** endpoint `:8081` (not NameServer 9876); broker `--enable-proxy`; Set/Storage rejected at config time |
| pulsar | ✔ | — | — | standalone, cluster | token (MessageId) | Shared subscription; `queue_len` raises `NotImplementedError` |
| sqs | ✔ | — | — | standalone (LocalStack), cloud | token (receipt_handle); ack=delete, nack=visibility 0 | payload ceiling 786,432 raw bytes (base64 of 1 MiB); clear sleeps the 60s purge window |
| memcached | — | — | ✔ | standalone | — | storage-only; relative TTL > 30 days converted to absolute exptime; `clear_storage(None)` needs `allow_flush_all=True` |
| dynamodb | — | — | ✔ | standalone (LocalStack), cloud | — | storage-only; items ≤ 400 KiB; app-level TTL checked on read |

\* deprecated alias.

Cross-backend rules that reviewers enforce:

- **Priority is uniformly "smaller-sorted-first"** on sorted backends (redis
  `-priority`, mongodb `-priority`, ES `priority asc`); Kafka maps
  `int(priority)` → partition instead; pulsar/sqs ignore priority.
- **`queue_len` must never swallow a failure to 0** — 0 means idle to the
  scheduler (premature `CloseSpider`). RocketMQ/Pulsar raise
  `NotImplementedError` rather than lie.
- **`clear_queue` raises on kafka/rocketmq/pulsar** (no linearizable purge);
  RabbitMQ/SQS implement fail-closed barriers instead.
- **Storage TTL is checked on read with compare-and-swap cleanup**, never an
  unconditional delete (mongodb `expireAt`-matched delete, dynamodb
  `ConditionExpression`, ES seq_no/term reap).
- **Ack tokens are generation-fenced** — never ack by bare offset/tag across a
  reconnect; stale tokens settle as no-ops ("stale"), not errors.
- Atomic-pop backends (redis/mongodb/ES) return `(pop(), None)` — no token
  exists; the message is already consumed. The legacy `ack(token=None)`
  single-slot path is only correct for `CONCURRENT_REQUESTS=1`.

## Load-bearing invariants

Break any of these and the suite (rightly) falls over:

1. **Lazy optional imports.** Registry and `__getattr__` tables store dotted
   paths only; no eagerly-imported core module may import a backend dep.
   Install hints (`pip install scrapy-extension[redis]`) are emitted only for
   genuine `ModuleNotFoundError`s of the documented dep
   (`backends/_optional.py::_is_missing_optional_dependency`).
2. **Error boundaries scrub everything.** Public backend ops are wrapped in
   boundary decorators (`exceptions/_redaction.py`) that rebuild exceptions
   after inner frames unwind — no credentials, endpoints, payloads, or logical
   keys in tracebacks; `handled_exception_types` is exact-type membership, not
   isinstance; `BaseException` control flow is never converted;
   `ConfigurationError` redacts secret-named settings at construction.
   Messages meant to survive must be **static literals** in `safe_messages` —
   never interpolated strings (the R74 lesson).
3. **Settings safe-list is contract-locked.** Every user-facing validator
   message in `settings/_redacted.py::_SAFE_SETTINGS_CONFIGURATION_MESSAGES`
   must match its validator verbatim; edit one, update the other, or the
   visible error silently degrades to the generic message.
4. **Probabilistic dedup filters never false-negative.** Bloom's `add` returns
   False only when all bits were set; Cuckoo's kick path is **undone** on
   `FilterFull`; Memory LRU refreshes on `__contains__` hit (and its eviction
   is the one documented FN exception, warned once).
5. **Durability fencing.** A replacement push carrying an ack token is
   rejected before serialization unless the prepared route is backend-backed;
   only a literal-`True` commit receipt counts as durability evidence.
   `is_push_durable()` is a legacy hint, not evidence.
6. **Telemetry can never alter the data path.** Monitor hooks and logger calls
   are guarded; diagnostics run after `except` suites unwind so custom
   handlers can't recover raw exceptions via `sys.exc_info()`; hook exceptions
   are swallowed by contract.
7. **Manager acquire/release pairing.** Every `get_manager()` pairs with
   exactly one `close()`; last holder tears down; every `from_settings`
   factory releases its manager under `except BaseException`. Registry key is
   `backend_type:sha256(normalized settings)` — secrets hash in but never
   appear in the key.
8. **Teardown order.** Scheduler closes the queue strategy **first** while
   the backend is still connected; pipeline closes strategy then manager in a
   `finally`; the spider mixin retains each component/lease/manager reference
   until that provider confirms cleanup, then releases the shared manager last.
   A failed direct mixin close is surfaced for a later retry; a
   `BaseException` never causes a premature manager release.
9. **Snapshot lifecycle.** `BackendQueue.close()` = stop admission →
   `begin_close` → drain → persist snapshot → `strategy.close()`. Unowned
   snapshot keys are length-prefixed v3; legacy fallback only for unscoped
   colon-free names; empty-state tombstone written before deleting the v3 key
   (see `migration-guide.md`).
10. **Lock discipline.** Kafka: connection lock before delivery lock
    (matching `disconnect()`). Registry: class `_registry_lock` before
    instance `_lock`; evictions disconnect outside the lock; the connect
    owner-gate runs retries without `_lock`. Circuit-breaker outcomes apply
    only when epoch+state still match (admission fencing).

## How-to recipes

### Add a bundled backend

1. `src/scrapy_extension/backends/<name>.py` implementing `Backend` + the
   capability ABCs; optional dep in module-level `try/except ImportError`
   gated by `_is_missing_optional_dependency` (redis.py is the canonical
   form; RocketMQ is the exception — its dep imports inside `connect()`).
   Declare `_push_is_durable = True` only if the broker guarantees
   push-survives-worker-crash (see rabbitmq.py's strict receipt
   classification); `requires_ack = True` if `pop` yields un-acked messages.
2. `src/scrapy_extension/settings/<name>.py` — `<Name>Mode` enum +
   `<Name>Settings(RedactedBaseSettings)`; register in
   `_TRUSTED_SETTINGS_CLASSES`; add every validator message to the safe-list;
   export from `settings/__init__.py`.
3. Registry + lazy tables: one `BackendDescriptor` in
   `backends/registry.py::_BUNDLED_DESCRIPTORS` (dotted-path strings +
   capability frozenset); plus the **four dicts** in the two `__init__.py`
   lazy-import tables (`_BACKEND_MODULES`/`_BACKEND_DEP_MODULES`/
   `_BACKEND_EXTRAS` and top-level `_OPTIONAL_IMPORTS`/`_OPTIONAL_DEP_MODULES`/
   `_BACKEND_EXTRAS`) and `__all__`.
4. `pyproject.toml`: the extra under `[project.optional-dependencies]` **and
   the verbatim-duplicated `all` list** (it's a copy, not a self-reference);
   the dep in `[dependency-groups].test` (tests import backend modules
   directly); the CI smoke tuple in `ci.yml`; the integration service fixture
   + `SCRAPY_TEST_*` env.
5. Tests: mocked suite + modes wiring; `tests/test_backend_metadata_contract.py`
   cross-checks registry/lazy-extras/pyproject automatically. Update the docs
   matrix and `.claude/CLAUDE.md`.

### Add a 3rd-party plugin backend

See [`backend-plugins.md`](backend-plugins.md) for the full contract (surface
is Experimental). Shape: one entry point
`[project.entry-points."scrapy_extension.backends"] mybackend =
"pkg.registration:register"` returning a frozen `BackendDescriptor` with
**path strings, never imports**. Bundled names win on conflict; duplicate
3rd-party names drop both; capability lies fail with `ConfigurationError` at
first use. The doc's example code block is executed by
`tests/test_backend_plugin_guide.py` — keep it runnable.

### Add a queue strategy

1. New module in `queue/strategies/`; subclass `QueueStrategy`
   (`push/pop/queue_len/clear` minimum).
2. Backend-delegating family: override `pop_with_ack` via
   `_pop_backend_with_ack` (single queue) or per-physical-queue
   (`_pop_backend_instance_with_ack`, see priority.py); fan out through
   `physical_strategy_queue_name` and gate with
   `ensure_fanout_backend_supported` (rejects kafka/rocketmq).
   In-process family: `bind()` → `_bind_single_queue`; implement
   `snapshot()`/`restore()` (versioned JSON, base64 items, *remaining* delays
   + wall-clock — never absolute monotonic values; corrupt state → start
   clean, never raise).
3. Add the enum member + factory branch (`strategies/factory.py`); selection
   is purely `SCRAPY_QUEUE_STRATEGY`.

Same pattern for storage strategies (`storage/strategies/`, factory +
`SCRAPY_STORAGE_STRATEGY`; set `emits_store_events = True` only if you emit
`on_store` at the durable boundary) and dedup filters
(`dupefilter/filters/`, factory + `SCRAPY_DEDUP_STRATEGY`; override `remove`
only if deletable; expose `saturation`/`capacity` for the monitor signal).

### Add a monitor

Subclass `Monitor` (`monitor/base.py`), override hooks
(`on_push/on_pop/on_queue_depth/on_filter_saturation/on_error/on_connect/...`),
pass via the `monitor=` kwarg or wire in a component's `from_crawler` —
override only a `NullMonitor`, and thread it into
`connection_manager.set_monitor()`. Hooks must be side-effect-light; call
sites swallow hook exceptions by contract. `ScrapyStatsMonitor` emits the
namespaced stat keys (`queue/depth`, `dupefilter/hit_count`, ...; full list in
`runbook.md`).

### Release

The project is pre-release (`0.1.0`, `[Unreleased]` changelog only, no publish
workflow). The canonical manual procedure (from `runbook.md`): `uv version
<bump>` → `uv lock` (+ `--check`) → gates → `uv build --clear` → SHA256SUMS →
tag → re-verify hashes → `uv publish` → fresh-venv install check. Never
rebuild artifacts after inspection. Pre-1.0 breaks need a minor bump +
changelog entry + migration guidance (`.github/STABILITY.md`).

## Testing playbook

**Layout**: pure-unit pyramid under `tests/` (mocks and in-memory fakes — no
broker needed), plus three opt-in tiers: integration (`tests/integration/`,
live brokers, double-gated: `SCRAPY_TEST_INTEGRATION=1` **and** a
per-backend `SCRAPY_TEST_<BACKEND>_*` env), benchmarks
(`--benchmark-enable`), and the Scrapy-engine e2e probe (subprocess — Twisted
reactors don't restart in-process). Contract/meta-tests execute documentation
code blocks and cross-check registry metadata against pyproject. Hypothesis
property tests live in `test_property_*` files. pytest-randomly shuffles
order — cross-test state leaks surface as order-dependent failures.

**Conventions that keep reviews short:**

- Patch the SDK client **where the backend module imports it**
  (`mocker.patch("scrapy_extension.backends.redis.Redis", ...)`) — never the
  SDK's own module. For SDKs with no install/import-time guard, stub the whole
  top-level surface with a `ModuleType` + `mocker.patch.dict(sys.modules,
  ...)` (copy `_patch_rocketmq` in `tests/test_rocketmq_resilience.py`).
- Autouse fixtures handle globals: `ConnectionManager._managers` is cleared
  before every test; RabbitMQ guest/guest creds are set (assert the
  required-creds contract with `monkeypatch.delenv`).
- `mock_connection_manager` mirrors the real durability contract
  (`push_is_durable` knob); production parity is pinned by
  `tests/test_mock_connection_manager_contract.py` — change one, change both.
- **TOCTOU convention**: a check-to-use window losing the client must raise a
  typed `QueueError`/`BackendConnectionError`, never raw `AttributeError`.
  Simulate deterministically via `backend._producer = None` + patched
  `is_connected`, or a class-level `PropertyMock` with
  `side_effect=[stub, stub, None]` (read order: assert_connected → guard →
  use).
- **Secret-hygiene convention**: assert the marker secret is absent from
  `str(exc)`, `repr(exc.__dict__)`, and every package-frame's locals
  (`_assert_package_traceback_locals_are_redacted`).
- Logging-sensitive tests: save `logger.level`, `setLevel`, restore in
  `finally`.
- Warn-once latches are per-process — reset them for test isolation.

**Honesty rules**: a real-broker round-trip is the only proof mocks can't
fake (the R7 lesson); RocketMQ integration asserts ≥1 delivery + body
fidelity, never N==N; zero delivery is `pytest.fail`, not skip.

## Troubleshooting and traps

- **Local pytest green ≠ CI green**: CI runs ruff/format/mypy-strict/bandit
  *before* pytest. Run `uv run poe check` locally.
- **Backend import fails in tests but not runtime**: tests import backend
  modules directly, so all SDKs are pinned in the `test` dep-group.
- **`BackendType.REDIS` as a dict key**: `str()` of it is
  `"BackendType.REDIS"` — always use `.value`.
- **Safe-list drift**: editing a validator message without updating
  `_SAFE_SETTINGS_CONFIGURATION_MESSAGES` silently degrades the error to the
  generic text.
- **Whitespace is truthy**: validators are strip-aware after historical
  bypasses (`"  "` sneaking past truthiness gates) — keep that discipline.
- **RocketMQ endpoint**: gRPC proxy `host:8081`, broker `--enable-proxy`; the
  client probes `8.8.8.8` for a local IP (integration fixture seeds the
  loopback cache instead of widening socket allow-lists).
- **ES image pinning**: client major must match server major (client 9 sends
  `compatible-with=9`, which ES 8 rejects with HTTP 400).
- **SQS names with punctuation** are silently blake2s-hashed — don't expect
  `<prefix><queue>` in the AWS console.
- **`enqueue_request` returns False only for duplicates or deterministic
  serialization rejection.** A transient queue/backend push failure rolls back
  dedup state and raises a terminal `QueueError`; it must not become
  `request_dropped`.
- **Default queue and dedup keys are isolated**: the scheduler uses
  `scheduler-queue:{project}:{spider}` and the dupefilter uses
  `dupefilter:{project}:{spider}` (`project` is `BOT_NAME`, or `default`).
  Current request envelopes are identity-fenced as well. Explicit literal
  `SCRAPY_QUEUE_KEY` / `SCRAPY_DUPEFILTER_KEY` values are the legacy/shared-key
  opt-in; add `SCRAPY_QUEUE_ALLOW_CROSS_SPIDER=True` only for intentional
  cross-spider routing, otherwise mismatched envelopes are nacked or
  re-published for their owning consumer.
- **work_stealing without a stable `SCRAPY_QUEUE_WORKER_ID`** strands the
  previous own-queue on restart (auto-UUID warns).
- **Batched storage crash-before-flush loses the in-flight batch** —
  documented failure mode, distinct from a store *exception* (unattempted
  tail re-enqueued, at-least-once).
- **Intentionally weird code — do not "fix"**: `queue/base.py` is an empty
  compat stub; `_scheduler_protocol_push = push` /
  `_atomic_protocol_request_seen = request_seen` pin hook identities against
  monkeypatching; pervasive `try: logger...() except BaseException: pass` is
  logging-handler isolation; `del args; del kwargs` in boundaries is
  traceback hygiene; `py.typed` is empty on purpose; RocketMQ's `frozenset()`
  in the dep-modules dicts is deliberate.

## Docs map and the insight loop

| Doc | What it covers |
|---|---|
| [`README.md`](../README.md) | install/extras contract, guarantees, testing entry |
| [`runbook.md`](runbook.md) | operations: strategy tables, Redis namespaces, ack/durability matrix, metrics keys, tuning, release procedure |
| [`backend-plugins.md`](backend-plugins.md) | 3rd-party backend author contract (Experimental) |
| [`migration-guide.md`](migration-guide.md) | persisted-state migrations: physical keys, snapshot v2/v3, wire codec, TTL contract, rollback |
| `.github/CONTRIBUTING.md` / `STABILITY.md` / `CHANGELOG.md` | dev setup, semver policy, changelog |
| `.claude/CLAUDE.md` | agent-facing build/test guide (mirrors this playbook's command set) |
| [`insight/LEDGER.md`](insight/LEDGER.md) | dedup ledger of every scan finding, keyed `(file:line, root-class)` |

`docs/insight/` (~400 files) is **maintainer planning history, not public
truth** — behavior questions go to README/runbook/migration-guide. Most
correctness hardening came out of the "R-round" insight loop: parallel opus
finder agents + adversarial verification (verifiers must confirm quoted
source exists) → SPEC/PLAN/TASK docs → TDD (RED→GREEN) → one atomic commit
per finding → LEDGER row. Before proposing any change that smells like a
finding, check the LEDGER for an existing `(file:line, root-class)` row —
`LANDED`/`REFUTED`/`DUPLICATE` mean "don't re-litigate". The LEDGER tail (not
memory or commit count) is the authoritative round counter.

## Change workflow

1. Branch from a clean `origin/main` (worktree if parallel); one atomic
   commit per concern, conventional-commit format
   (`fix(queue): ...`, `feat(backends): ...`, `docs: ...`, `chore(build): ...`).
2. Write the failing test first; keep the suite, `ruff check`, `ruff format`,
   and `mypy --strict` green before pushing.
3. `main` is the only long-lived branch — ff-merge and delete feature
   branches; never force-push main.
4. Behavior-affecting changes update: README/runbook as needed,
   `migration-guide.md` if persisted state is touched,
   `.github/CHANGELOG.md` `[Unreleased]`, and the settings safe-list if any
   validator message changed.
5. Durability/ack/capability changes deserve a real-broker integration check,
   not only mocks.
