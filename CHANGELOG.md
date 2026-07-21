# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

Round-9/14 hardening introduced config-time validators that **refuse to start**
crawlers carrying unsafe or incoherent config. Each item is security-motivated
and was previously a silent footgun, but they ARE behavior breaks — read before
upgrading.

- **Redis now uses a namespaced, domain-separated physical-key layout.** Queue,
  set, and storage operations no longer address legacy raw keys. The default
  namespace is `scrapy-extension`; independent applications sharing a Redis
  database must choose distinct `SCRAPY_REDIS_NAMESPACE` values. There is no
  implicit legacy fallback because it could read or delete another
  application's key. Drain or explicitly migrate persistent data before
  upgrading; see [`docs/migration-guide.md`](docs/migration-guide.md).
- **Storage TTL inputs and results are uniform across all five backends.** Direct
  `StorageBackend.store` calls accept only `None` or a positive integer number
  of seconds; zero, negatives, floats, and booleans now raise `ValueError`.
  `ttl()` returns a non-negative integer or `None`; Redis/MongoDB/ElasticSearch/
  DynamoDB no longer expose backend-specific `-2`, `-1`, or expired `0`
  sentinels. `SCRAPY_PIPELINE_TTL=0` remains the pipeline-level permanent-value
  shorthand and is normalized to `None`.
- **Memcached global clear is disabled by default.** Because Memcached cannot
  scope deletion to this extension, `clear_storage(None)` now raises
  `NotImplementedError` unless `SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=True`.
  Prefix clearing remains unsupported. Enabling the flag issues server-wide
  `flush_all` and is intended only for dedicated instances.
- **Unknown bundled-backend settings now fail fast.** Nested extras and typoed
  flat/environment names under a selected backend prefix raise
  `ConfigurationError` (with a nearest-name suggestion) instead of silently
  falling back to defaults. Pydantic type/range/enum failures continue to use
  `ValidationError`.
- **Pulsar and RocketMQ queue depth is explicitly unsupported.** Their
  `queue_len()` methods raise `NotImplementedError` instead of returning false
  zero, so scheduler idle detection remains conservative. Callers that treated
  zero as empty must handle the unsupported signal.
- **`priority` and `work_stealing` are rejected with Kafka and RocketMQ.** Those
  backends' single consumers cannot isolate a scan to one strategy-created
  physical topic; the previous combinations could rebalance, consume from the
  wrong topic, or invalidate ack correlation.
- **Malformed broker payloads are terminally consumed.** When deserialization
  deterministically fails and an ack token exists, `BackendQueue` attempts to
  ack/drop the delivery, increments `scheduler/queue/poison_dropped`, and still
  raises `SerializationError`. The prior nack/redelivery behavior could pin a
  Kafka partition or create a permanently hot poison loop.
- **Token-bearing replacements cannot enter volatile queue state.** A retry,
  redirect, or user-errback replacement that still owns an unacknowledged
  broker source now raises `QueueError` before a positive-delay `delay` /
  `time_wheel` append or any `round_robin` / `ring_buffer` append. Previously
  the source was acked after only an in-process write and a hard crash could
  lose both copies. Use a backend-durable strategy/path; zero effective delay
  still pushes directly to the backend.

- **Pulsar `auth_token` now requires `pulsar+ssl://`.** `PulsarSettings(
  auth_token=…)` rejects `service_url` values that do not start with
  `pulsar+ssl://` (round-9c SV3-2). Sending a token over plaintext `pulsar://`
  leaks it on the wire; operators who intentionally run token-auth over a
  private plaintext broker must switch to `pulsar+ssl://` (or drop the token).
  Fix: change `SCRAPY_PULSAR_SERVICE_URL` to `pulsar+ssl://broker:6651`.
- **Pulsar TLS now validates broker hostnames by default.** The backend now
  translates its public compatibility fields to the real pulsar-client
  `tls_*` constructor keywords; the previous names made every TLS client
  construction fail. New setting `SCRAPY_PULSAR_TLS_VALIDATE_HOSTNAME=True`
  also closes the SDK's insecure default. Fix mismatched broker certificates,
  or set it to `False` only as an explicit local-development compatibility
  escape hatch. Service URLs are normalized to the SDK's case-sensitive scheme
  and single-prefix cluster syntax; see
  [`docs/migration-guide.md`](docs/migration-guide.md).
- **Authenticated RocketMQ connections now require TLS.** New setting
  `SCRAPY_ROCKETMQ_TLS_ENABLED` is passed to both the Producer and
  SimpleConsumer gRPC clients. Any explicit access/secret key must be a complete
  non-empty pair and implies TLS; cloud mode always requires both credentials
  and TLS. Connection setup now revalidates one coherent value snapshot and
  redacts SDK-bound credentials and public startup failures. Anonymous
  standalone/cluster deployments retain the existing plaintext default.
- **Redis `ssl_enabled=True` now requires `ssl_cafile`.** `RedisSettings(
  ssl_enabled=True)` rejects a missing `ssl_cafile` (round-9c SV3-3). TLS
  without a pinned CA is vulnerable to MITM; operators who previously relied
  on the system CA store must now pass an explicit `SCRAPY_REDIS_SSL_CAFILE`.
- **`BackendQueue.push` pops `delay` / `source` from `request.meta`.** A delayed
  request that is re-pushed (e.g. by a retry) no longer re-applies the original
  delay (round-14 R14-F). Code that re-reads `request.meta['delay']` /
  `request.meta['source']` downstream of a push will now see them absent;
  re-push semantics changed from "re-delay every time" to "delay once".
- **Memcached / DynamoDB / MongoDB storage ops now RAISE `StorageError`
  instead of returning a silent sentinel.** A failed `store()` previously
  returned `None` (memcached/dynamodb) — the item pipeline believed the item
  was stored (data-loss contract bug). Failed ops now raise
  `StorageError(BackendError)` so `except BackendError` catches them
  uniformly (round-14 R14-A). Code that swallowed the sentinel must now
  handle the exception; the prior behavior was a silent data-loss bug.
- **`SCRAPY_BACKEND_TYPE` validation now raises `ConfigurationError`, not
  pydantic `ValidationError`.** Unknown backend-type values (and 3rd-party
  strings not present in the registry) raise the project's `ConfigurationError`
  with `setting_name='SCRAPY_BACKEND_TYPE'` (round-14 R14-B). Operators with
  `except ValidationError` handlers for bad backend types must switch to
  `except ConfigurationError` (or the broader `except BackendError`).
  **Additive:** registered 3rd-party backend strings (entry-point group
  `scrapy_extension.backends`) are now ACCEPTED at the Settings layer — they
  were previously rejected as `ValidationError`, contradicting round-5 R5-1.
- **Strategy snapshots support stable per-worker ownership.** Without an owner,
  the existing spider+queue key remains for single-worker compatibility. Setting
  `SCRAPY_QUEUE_SNAPSHOT_OWNER` (or the `SCRAPY_QUEUE_WORKER_ID` fallback)
  selects a length-prefixed v2 key so same-spider workers cannot overwrite one
  another. A restored checkpoint now remains available until the next clean
  close replaces it with current state or deletes it after a clean drain. This
  makes a crash during recovery at-least-once: completed entries may replay,
  but pending entries cannot disappear with an eagerly deleted checkpoint.
  Enabling an owner intentionally leaves the old unowned key untouched;
  migrate or discard it explicitly.
- **RocketMQ backend rewritten against apache `rocketmq-python-client` 5.1.1 gRPC** (#15/#44).
  The prior backend's `connect()` imported fictional API paths
  (`rocketmq.consumer.SimpleConsumer`, `rocketmq.endpoint.Endpoint`, …) matching
  no released client — lazy-import hid this since inception; it had never
  connected to any broker. The rewrite targets the apache pure-Python gRPC
  client. **BREAKING supply-chain change**: the `[rocketmq]` extra now installs
  `rocketmq-python-client>=5.1.1` (was `rocketmq-client-python`, the unmaintained
  ctypes wrapper). The two install to the same `site-packages/rocketmq/`
  namespace and **cannot coexist** — existing installs require a clean reinstall
  (`uv sync --reinstall` / fresh venv). The broker must run with
  `--enable-proxy` and `SCRAPY_ROCKETMQ_NAMESRV_ADDRESS` now means the gRPC
  PROXY endpoint (`host:8081`), not the legacy NameServer (9876). Topics are
  **not** auto-created via the gRPC path by default — see the README "Topic
  creation" subsection (`enableAutoTopicCreation` in `rmq-proxy.json` or
  `mqadmin updateTopic`). Two real production bugs fixed in the rewrite:
  `is_connected()` was calling `is_running()` (a bool **property**, not a
  method) → every push/pop raised "Not connected". Poll wait and processing
  invisibility are now separate controls: `timeout` changes only the receive
  wait, while `SCRAPY_ROCKETMQ_INVISIBLE_DURATION` (default 300s, minimum 10s)
  owns the delivery lease. Deferred-ack semantics are preserved: `pop` returns
  the body without acking; the caller acks via `ack(token=msg)`.

### Added

- Locked Scrapy 2.17.0 and setuptools 83.0.0. The dependency audit documents
  one exact exception, `PYSEC-2017-83`: the reviewed
  [GHSA-h7wm-ph43-c39p](https://github.com/advisories/GHSA-h7wm-ph43-c39p)
  affected range ends at Scrapy 2.15.2, so locked 2.17.0 is outside it even
  though the PyPA record lacks a fixed-version event. CI suppresses only that
  advisory ID and continues auditing the rest of the locked graph.
- `QueueBackend.ack()` / `nack()` API and concurrent-safe per-message tokens for
  Kafka, RabbitMQ, RocketMQ, Pulsar, and SQS. Atomic backends (Redis, MongoDB,
  ElasticSearch) inherit no-op defaults.
- Signal-driven ack: `BackendScheduler.open` connects Scrapy's
  `response_received` → `ack()` and `spider_error` → `nack()` for
  at-least-once delivery semantics. Warns if `spider.crawler` is absent.
- Redis pop/push atomicity via Lua scripts (`ZPOPMAX+HGET+HDEL`,
  `INCR+ZADD+HSET`). FIFO ordering within same priority via counter prefix.
- Namespaced hash-tagged queue item/payload/counter keys
  (`{<namespace>:queue:<name>}:*`) for Redis Cluster slot affinity.
- Cross-mode settings validation: Redis SENTINEL mode validates `sentinels`
  and `sentinel_master_name` at construction time.
- `[project.urls]` metadata for PyPI.
- Smart JSON serialization for common `request.meta` types: `datetime`,
  `bytes` (base64), `Decimal`, `UUID`, `set`/`frozenset`, `Enum`,
  `pathlib.Path`. Truly unexpected types raise `TypeError`.
- `BackendType._missing_` lists valid values on invalid input.
- `ConnectionManager.clear_registry()` classmethod for test isolation.
- Full-jitter exponential backoff on connection retry (thundering herd
  prevention).
- Bounded connection-manager retry controls:
  `SCRAPY_RETRY_ATTEMPTS` means retries after the initial attempt (0..20), and
  `SCRAPY_RETRY_DELAY` is the full-jitter exponential base.
- Lazy-import error-message tests for all 5 module-guard backends.
- `CHANGELOG.md`.
- Stable snapshot-owner routing through `SCRAPY_QUEUE_SNAPSHOT_OWNER`, with
  `SCRAPY_QUEUE_WORKER_ID` as the fallback.
- Poison/empty/replacement terminal-drop stats under `scheduler/queue/*`.
- ElasticSearch + RocketMQ shortcut attributes on `BackendSpiderMixin`
  (`elasticsearch_hosts` / `cloud_id` / `api_key`,
  `rocketmq_namesrv_address` / `access_key` / `secret_key`), matching the
  existing Redis / MongoDB / Kafka / RabbitMQ pattern.
- Integration test suites for Redis, MongoDB, and ElasticSearch —
  skip-by-default, gated on `SCRAPY_TEST_REDIS_URL`,
  `SCRAPY_TEST_MONGODB_URI`, and `SCRAPY_TEST_ES_HOSTS`.
- `pipeline/storage_skipped` stat counter, distinguishing "no items
  scraped" from "items silently dropped" on storage-unsupported backends
  (Kafka, RabbitMQ, RocketMQ).
- `ConfigurationError` redacts sensitive `setting_value` at construction
  (defensive against repr / traceback / debugger leaks).
- Explicit `__all__` on `base.py`, `backends/__init__.py`, and verified
  across every module with a public surface.
- ElasticSearch CLOUD mode fail-fast: `ElasticSearchSettings` now rejects
  `mode=CLOUD` without `cloud_id` at construction (was a connect-time error).
- CI workflow (`.github/workflows/ci.yml`): unit tests across Python
  3.10–3.14 on every push/PR.
- Integration test suites for RabbitMQ, Kafka, and RocketMQ (completing
  the sextet) — skip-by-default, gated on `SCRAPY_TEST_*` env vars.
- `LICENSE` (MIT).

### Changed

- All password/secret fields migrated to `pydantic.SecretStr`.
  `repr(settings)` shows `**********`; raw value only via
  `.get_secret_value()` via the `secret_value()` helper.
- Kafka `enable_auto_commit` default changed from `True` to `False`.
- RabbitMQ enables synchronous publisher confirms on every connection mode and
  publishes with `mandatory=True`. Broker nacks, unroutable messages, AMQP
  errors, and explicit negative confirmations raise `QueueError`; a successful
  `push()` now means the broker confirmed routing, not merely that the client
  accepted bytes.
- SQS `nack()` changes the specific receipt's visibility to zero for immediate
  redelivery. RocketMQ `nack()` changes the specific message's invisibility to
  the broker's 10-second minimum. Neither backend automatically renews the
  processing lease.
- SQS and DynamoDB standalone mode normalize an omitted endpoint to
  `http://localhost:4566`; cloud mode preserves an omitted endpoint for real
  AWS. This prevents zero-config standalone use from silently targeting AWS.
- RabbitMQ accepts an `amqp://` / `amqps://` URL shortcut and expands missing
  host/port/credential/vhost fields. Explicit discrete fields take precedence.
- Bundled backend config now merges nested settings over flat Scrapy settings,
  then environment/default values, while keeping generic ConnectionManager
  retry controls separate from backend-native retry fields.
- Dependency pins tightened: all deps now have upper bounds.
  `pymongo` minimum bumped to 4.8 (CVE hardening).
- Test dependency group trimmed from 48 to 19 packages. Removed unused
  HTTP mocks, data generators, mutation tools, report plugins, async
  plugins, and more.
- `BackendScheduler` no longer duplicates the dupefilter's work —
  deduplication is exclusively `BackendDupeFilter`'s responsibility.
- `ConnectionManager.close()` evicts the instance from the class-level
  registry.
- `BackendSpiderMixin` now extends `Spider` directly (removes `cast`).
- `BackendQueue.spider` is a required keyword-only argument.
- `_validate_key_name` consolidated to single canonical implementation in
  `base.py` (was duplicated across ES and RabbitMQ with diverged patterns).
- `BackendQueue._request_to_dict` body encoding changed from UTF-8/latin-1
  fallback to base64 — binary POST bodies now round-trip losslessly.
- Development Status classifier bumped from Alpha (3) to Beta (4).
- `_on_spider_closed` wrapped in try/except so close failures don't break
  the signal chain.
- **SetBackend / StorageBackend error handling (behavior change)**: `add`,
  `retrieve`, `exists`, `remove`, `contains`, `delete`, and `ttl` on Redis
  and ElasticSearch now **propagate backend errors** instead of returning a
  sentinel (`False` / `None` / `-1`). Sentinels mean exactly what the ABC
  contract says (e.g. `add` → `False` = "already existed"; `retrieve` →
  `None` = "not found"). Code that relied on errors being silently swallowed
  must now handle the exception.
- `__version__` is derived from installed package metadata
  (`importlib.metadata`) instead of a hardcoded string.
- Kafka SASL password is wrapped in a redacting `str` subclass so
  `repr(producer_config)` no longer leaks it.
- RabbitMQ emits a one-shot cleartext-credentials warning when
  `ssl_enabled=False`.
- `BackendDupeFilter` honors a configured `REQUEST_FINGERPRINTER_CLASS`
  via `crawler.request_fingerprinter` (byte-identical to the default
  fingerprinter when unset, so existing fingerprints are unchanged).
- `BackendScheduler.open` / `close` return-type annotations widened to
  `Deferred[None] | None`, matching Scrapy's scheduler protocol.
- `ConnectionManager.close()` catches `Exception` (was a narrow
  `RuntimeError, ValueError, AttributeError` tuple) so a disconnect error
  can't skip registry eviction or break the caller's close chain.
- License metadata migrated to PEP 639: `license = "MIT"` SPDX expression
  + `license-files = ["LICENSE"]` (deprecated `License ::` classifier
  removed). Distributions now bundle the license text.
- `uv_build` build-system pin widened to `<0.12` (was `<0.11.0`, which
  excluded uv 0.11 and could break builds in uv-0.11-only environments).
- **`ConnectionManager` breaker-config read hoisted out of the instance lock** (#15,
  performance). The per-manager circuit-breaker config read (`Settings()` — a
  pydantic env scan) ran inside `self._lock`, serializing peer
  `get_manager` / `close` warm-up. The read now runs above the lock;
  double-checked-lock construction stays lock-protected. Behavior is
  byte-identical — no observable change beyond reduced lock contention under
  concurrent manager resolution.

### Fixed

- Third-party backend discovery now validates that entry-point names equal
  descriptor `backend_type` values, validates both dotted class paths, and
  rejects all duplicate third-party names. Broken/conflicting plugins are
  reported through logging instead of `warnings.warn`, so an application's
  warnings-as-errors policy can no longer make them hide bundled backends.
- Redis blocking pop now polls the same atomic Lua pop used by the non-blocking
  path, eliminating the `BZPOPMIN` followed by payload-read crash window.
- Kafka ack tokens include topic, partition, offset, and consumer generation;
  stale or cross-topic tokens cannot commit an unrelated delivery.
- RabbitMQ ack tokens include channel generation; a delivery tag reused after
  reconnect cannot be acknowledged by an old completion.
- Pulsar ack tokens remain bound to the consumer/topic that issued them rather
  than following the backend's latest subscription.
- Invalid/missing Base64 SQS bodies are best-effort deleted before raising
  `QueueError`, preventing poison redelivery below the `BackendQueue` layer.
- Delay/time-wheel snapshots persist remaining delay plus wall-clock context and
  rebase to the new process's monotonic clock; old absolute monotonic deadlines
  no longer drift across host restart.
- **Registry entry-point discovery no longer emits 5 ``SelectableGroups`` deprecation warnings** (#38).
  ``_discover_entry_points`` branched on ``sys.version_info`` to use the legacy
  ``entry_points().get(group, [])`` dict form on Python 3.10/3.11 — based on the
  false premise that ``entry_points(group=...)`` was unavailable before 3.12 (it
  has been available since 3.10). The dict form emitted ``SelectableGroups dict
  interface is deprecated`` on every 3.10/3.11 run and was removed in 3.12.
  Collapsed to keyword-only; the version branch, ``import sys``, and the
  dual-shape Test 7 contract were removed.
- **Removed unreachable ``isinstance(e, (KeyboardInterrupt, SystemExit))`` re-raise inside
  ``ConnectionManager.connect()``'s ``except Exception`` block** (#39). The check was dead
  code — ``KeyboardInterrupt``/``SystemExit`` inherit from ``BaseException`` (not ``Exception``),
  so ``except Exception`` never catches them and the inner ``isinstance`` could never match.
  Behavior is unchanged (KI/SystemExit still propagate via not being caught); 4 surrounding
  coverage gaps on the hot-path module closed (96.28% → 98.55% reliable; remaining gap is
  non-deterministic concurrency-path coverage on ``get_backend()``, behaviorally tested by T9).
- Redis ZSET member collision silently dropping identical payloads.
- `BackendQueue.pop` losing callback/errback on deserialization (spider
  passthrough).
- MongoDB `ttl()` returning `-1` for missing keys (now `None` per contract).
- Redis `ttl()` returning `-1` for missing keys (now `None`).
- RocketMQ consumer never subscribed to topics (pop always returned None) —
  also fixed missing `consumer.start()`.
- RabbitMQ `_ensure_queue_exists` re-declaring queues with different args,
  killing the channel via `PRECONDITION_FAILED`. Now tracks declared queues
  in-session.
- ElasticSearch pop non-atomic (search-then-delete race) — now uses
  optimistic locking via `if_seq_no` / `if_primary_term`.
- Redis Lua pop decode_responses=True regression (str not bytes).
- `ConnectionManager._attempt_connection` leaving half-connected backend
  on failure — now assigns `self._backend` only after `connect()` succeeds.
- `BackendScheduler.close()` didn't reset `_signals_connected`, preventing
  ack/nack re-wiring on scheduler reuse.
- MongoDB `clear_storage` prefix `[:128]` silent truncation — removed;
  `re.escape()` already neutralizes regex injection.
- `JSONSerializer.serialize` `default=str` silently coerced non-JSON-native
  types (`str(b"x")` → `"b'x'"`). Replaced with smart `_json_default`.
- Dead code: RocketMQ `PushConsumer` import, Redis commented-out mode
  validation.
- Connection pool leak: a failed `connect()` now calls `disconnect()` on
  the half-built backend, releasing the pool the client constructor had
  already allocated (previously leaked one pool per retry under network
  instability).
- `BackendSpiderMixin._build_backend_settings` RabbitMQ branch fell
  through to the ElasticSearch branch when `rabbitmq_url` was unset, so a
  spider carrying both RabbitMQ and ElasticSearch shortcut attributes
  could merge ES settings into a RabbitMQ backend.
- ElasticSearch `ttl()` returned `-1` for missing keys — now `None`,
  matching Redis and MongoDB (callers can distinguish "absent" from
  "expired").
- RocketMQ `ping()` docstring overstated "responsive" — it is a
  local-state check, not a broker round-trip.
- Kafka `pop()` re-subscribed the consumer on every call (even for the
  same queue); now caches the subscription, mirroring RocketMQ's pattern.
- **Multi-backend isolation: hardened `ConnectionManager` registry key** (#14).
  The registry key used `json.dumps(settings, default=str)`, whose lossy
  `str()` could collapse two semantically-different settings to one key (e.g.
  `datetime(2024,1,1)` and the string `"2024-01-01 00:00:00"`), silently
  sharing one connection manager (wrong backend conn / wrong DB index). The
  `default` now type-tags non-JSON values so distinct types render distinctly;
  pure-JSON settings keys are byte-identical to the prior form (backward
  compatible). The `except` fallback was also hardened — the old
  `str(sorted(settings.items()))` raised on mixed-type dict keys, masking the
  real settings behind a `TypeError`.

### Removed

- `BackendQueue.peek()` — non-atomic, documented as unsafe, no production
  callers.
- 29 unused test dependencies (see Changed: test group trimmed).
- Orphaned `[tool.pyrefly]` and `[tool.mutmut]` config sections (those
  tools were removed in the test-dep trim).

### Round 8 — forward insight, testing infrastructure, v1.0 SPECs

- **Added:** 4-tier test infrastructure (unit / mock-backend / integration /
  load-scale), with skip-by-default real-broker suites gated on
  `SCRAPY_TEST_*` env vars. Multi-backend e2e integration test landed
  (`tests/integration/test_multi_backend_e2e.py` + `docker-compose.yml`,
  commit `3cef50c`) — closes v1.0 non-negotiable #3.
- **Added:** entry-point plugin registration — 3rd-party backends register
  via `[project.entry-points."scrapy_extension.backends"]`; the bundled 10
  are statically seeded in `backends/registry.py` as dotted-path strings
  (lazy-import preserved). See [`docs/backend-plugins.md`](docs/backend-plugins.md).
- **Added:** backend-plugin author contract documentation.
- **Added:** `BackendSpiderMixin` shortcut attributes for ElasticSearch +
  RocketMQ (cloud_id / api_key / namesrv_address / access_key / secret_key).
- **Added:** `pipeline/storage_skipped` stat counter (distinguishes "no
  items scraped" from "items silently dropped" on storage-unsupported
  backends).
- **Added:** three v1.0-readiness SPECs (`docs/insight/SPEC-round8-tier1.md`,
  `SPEC-round8-testing.md`, `SPEC-round8-v1readiness.md`,
  `SPEC-round8-settings-validation.md`) + consolidated execution menu
  (`docs/insight/EXECUTION-INDEX.md`).
- **Security (round 6, landed in this arc):** TLS/scheme guards SEC-1..7
  across `settings/*.py` — `_RedactedStr`, SEC-2 MongoDB insecure-TLS-in-prod
  rejection, SEC-3 ES cleartext-credentials-over-http rejection, SEC-4
  LocalStack/AWS `endpoint_url` scheme validation, SEC-7 AWS credential XOR
  at connect path. See [`SECURITY.md`](SECURITY.md).

### Round 9 — settings validation (SV1–SV5), perf (U4), OOM cap (U5)

- **Added (SV1):** `Literal` enum types for all mode/scheme fields across
  `settings/{kafka,pulsar,rabbitmq,mongodb}.py` — typos now raise
  `ConfigurationError` with valid-value enumeration at config time instead
  of an opaque runtime stack trace.
- **Added (SV2):** mode-conditional `model_validator`s across
  `settings/{mongodb,redis,kafka,rabbitmq}.py` — mode-specific required
  fields (e.g. Redis SENTINEL needs `sentinel_master_name`, MongoDB
  REPLICA_SET needs `replica_set`) enforced at construction.
- **Added (SV3, security):** cross-field auth/transport coherence
  validators across
  `settings/{kafka,pulsar,redis,mongodb,elasticsearch,sqs,dynamodb}.py` —
  closes 3 high-severity credential bugs (SASL username without password,
  TLS cert without key, mismatched auth mode).
- **Added (SV4):** URL/scheme format guards across
  `settings/{mongodb,pulsar,rocketmq,elasticsearch,sqs,dynamodb}.py` —
  malformed host URLs and missing schemes rejected before connect.
- **Added (SV5):** empty-string + unbounded-int gaps closed across
  `settings/{memcached,redis,rabbitmq,base}.py` — `Field(ge=…)`,
  `Field(gt=…)`, and `min_length=1` bounds on user-supplied values.
- **Added (U4, perf):** `BackendQueue(depth_sample_every=100)` — probes
  real backend queue depth at most once per 100 pops, reclaiming ~25% of
  the pop-path RTT budget at default config. Backpressure gates still trip
  at the right depth; `depth_sample_every=1` restores per-pop behavior.
  `monitor.on_queue_depth` now emits from the cached sample.
- **Added (U5, OOM):** `MemoryMembershipFilter(maxsize=1_000_000)` default
  (was `None` = unbounded; ~366 MB @ 1M entries, ~3.58 GB @ 10M). Explicit
  `maxsize=None` remains as advanced opt-out. `DelayQueueStrategy(max_held=100_000)`
  soft-cap warn-once on the in-process holding heap. Configurable via
  `SCRAPY_DEDUP_MEMORY_MAXSIZE`.
- **Added:** `_filter_full_warned` warn-once path on `FilterFull` (Cuckoo at
  capacity) — degrades to passthrough + `dupefilter/filter_full` stat bump
  via `monitor.on_filter_full()`.

### Round 8d — settings validation SPEC

- **Added:** `docs/insight/SPEC-round8-settings-validation.md` — 34-footgun
  settings-validation hunt resolved into 5 executable units (SV1–SV5).

### Round 10 — backlog merge sweep (2026-07-04)

Twelve previously-stalled feature branches — verified conflict-free (zero
source-file overlap) — merged to `main` in one gate-green sweep. **`mypy --strict`
is now clean across all 67 source files** (was 1 error on `rocketmq.py:321`);
pytest +84 cases (1762 → 1846); ruff and bandit both clean.

- **Security (#35):** `bandit` reports 0 active findings. Three LOW-severity
  items accepted with `# nosec` annotations matching the existing pattern
  (`BACKEND_ACK_TOKEN_META_KEY` B105 — a `request.meta` key name, not a
  credential; two type-narrowing `assert`s B101).
- **Changed (#34):** RabbitMQ `MIRRORED_QUEUES` mode now emits a `WARNING`
  that the HA policy is NOT applied via AMQP — operators must set it
  out-of-band (`rabbitmqctl set_policy`). The prior `DEBUG` log + dead
  policy dict falsely implied mirroring was active.
- **Fixed — type safety (#36):** `mypy --strict` is clean. The last internal
  `Any`-leak (`rocketmq` `msg.body`, declared `-> bytes | None`) is closed
  with a typed `cast`; the `py.typed` strict-mode promise (U8) now fully holds.
- **Internal — coverage to 100% per module (#23–#32):** every backend module
  now at 100% statement + branch coverage. `_redact` contract (#24),
  `BackendQueue` resilience (#25), RocketMQ connect + TOCTOU race guards
  (#26), SQS contract + resilience (#27), DynamoDB contract + resilience
  (#28), Kafka resilience (#29), ElasticSearch contract + resilience (#31),
  Pulsar resilience + contract (#32). Round-robin dead-code removal +
  safety-net characterization (#23).
