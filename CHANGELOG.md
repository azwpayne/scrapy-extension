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

- **Pulsar `auth_token` now requires `pulsar+ssl://`.** `PulsarSettings(
  auth_token=…)` rejects `service_url` values that do not start with
  `pulsar+ssl://` (round-9c SV3-2). Sending a token over plaintext `pulsar://`
  leaks it on the wire; operators who intentionally run token-auth over a
  private plaintext broker must switch to `pulsar+ssl://` (or drop the token).
  Fix: change `SCRAPY_PULSAR_SERVICE_URL` to `pulsar+ssl://broker:6651`.
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
- **`BackendQueue` strategy snapshots are now scoped by `(spider.name, queue_name)`** (#16).
  The snapshot storage key changed from `queue:snapshot:<queue_name>` to
  `queue:snapshot:<spider.name>:<queue_name>`. Two spiders sharing a storage
  backend with the same `queue_name` previously overwrote each other's
  `Delay`-strategy snapshot on close (and on restart one restored the wrong
  spider's held heap). **BREAKING for multi-spider deployments**: legacy
  snapshots under the old key are orphaned on upgrade (ignored, not restored);
  no migration is provided because the prior format (#13) shipped immediately
  before this fix. Single-spider deployments are unaffected.

### Added

- `QueueBackend.ack()` / `nack()` API for message-queue backends (Kafka,
  RabbitMQ). Atomic backends (Redis, MongoDB, ElasticSearch, RocketMQ)
  inherit no-op defaults.
- Signal-driven ack: `BackendScheduler.open` connects Scrapy's
  `response_received` → `ack()` and `spider_error` → `nack()` for
  at-least-once delivery semantics. Warns if `spider.crawler` is absent.
- Redis pop/push atomicity via Lua scripts (`ZPOPMAX+HGET+HDEL`,
  `INCR+ZADD+HSET`). FIFO ordering within same priority via counter prefix.
- Hash-tagged payload/counter keys (`{queue_name}:payload`, etc.) for
  Redis Cluster slot affinity.
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
- Concurrent-pop warning on Kafka/RabbitMQ when `CONCURRENT_REQUESTS > 1`.
- Lazy-import error-message tests for all 5 module-guard backends.
- `CHANGELOG.md`.
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
