# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Removed

- `BackendQueue.peek()` — non-atomic, documented as unsafe, no production
  callers.
- 29 unused test dependencies (see Changed: test group trimmed).
