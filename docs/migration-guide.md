# Migration Guide

This guide covers the persisted-state and configuration changes in the current
unreleased line. Treat a backend migration as a maintenance event: stop all old
and new workers before moving state. Mixed writers can make rollback ambiguous
and can corrupt FIFO/ack assumptions even when individual records look valid.

## Preflight

1. Inventory every Queue, Set, Storage, and strategy-snapshot key used by each
   spider and worker.
2. Record current backend types, component-specific settings, queue strategy,
   spider names, worker IDs, and effective Redis namespace.
3. Stop producers and consumers, then verify no process can write the old or
   new layout.
4. Take a backend-native backup and test restoring it in an isolated service.
5. Prefer draining old work with the old package and re-enqueuing it with the
   current package. Use physical-key copying only when a drain is impossible.

Do not use a rolling dual-write deployment. There is no supported transaction
across the old and new layouts, and message bodies from different codec
generations are not always distinguishable.

## Redis Physical-Key Layout

Redis now maps each logical name into a configured namespace and separates the
Queue, Set, and Storage domains. The default namespace is
`scrapy-extension`; deployments sharing a database must choose distinct values
with `SCRAPY_REDIS_NAMESPACE`.

| Domain | Legacy physical key | Current physical key |
|---|---|---|
| queue items (ZSET) | `<queue>` | `{<namespace>:queue:<queue>}:items` |
| queue payloads (HASH) | `{<queue>}:payload` | `{<namespace>:queue:<queue>}:payload` |
| queue FIFO counter (STRING) | `{<queue>}:counter` | `{<namespace>:queue:<queue>}:counter` |
| set (SET) | `<set>` | `<namespace>:set:<set>` |
| storage (STRING) | `<key>` | `<namespace>:storage:<key>` |

There is intentionally no read fallback to legacy keys. A raw key may belong
to another application, and automatic fallback would make read, delete, and
clear operations cross an ownership boundary.

Recommended procedure:

1. Set a unique namespace in the target configuration and keep it unchanged
   across restarts.
2. Drain queued requests under the old version when possible.
3. Copy Set and Storage values with a tool that preserves Redis types and TTLs.
4. If queues cannot be drained, move all three physical queue keys as one
   maintenance unit and validate ZSET member count against HASH field count.
5. Start one current-version worker, validate queue depth and a sample of
   dedup/storage values, then expand the deployment.
6. Retain the backup and legacy keys until the rollback window closes.

Redis Cluster cannot `RENAME` a key across hash slots. The old three-key queue
layout and the new namespaced hash tag generally occupy different slots, so use
a cluster-aware, type-preserving copy/export-import tool while writers are
stopped. Do not approximate queue migration by copying only the ZSET: its
members reference payloads in the sidecar HASH, and the counter preserves FIFO
ordering among equal priorities.

`clear_storage()` scans only the configured namespace's storage domain. Do not
use `FLUSHDB` to clean up migration leftovers on a shared database.

## Queued-Request Wire Format

Current request dictionaries mark bodies with
`_scrapy_extension_body_codec="base64-v1"`. Legacy dictionaries have no marker
and may contain raw UTF-8 text. The reader can recover an unmarked body that is
not valid Base64, but an old raw string that also happens to be valid Base64 is
inherently ambiguous and may decode to different bytes.

The safe migration is therefore:

1. Stop new producers.
2. Drain legacy queues using the old package.
3. Re-create and enqueue each outstanding request using the current package.
4. Start current consumers only after the old queue is empty.

Do not rely on rolling mixed readers to rewrite the backlog. A deterministically
malformed broker delivery with an ack token is terminally acknowledged and
dropped to avoid a permanent poison loop; monitor
`scheduler/queue/poison_dropped`,
`scheduler/queue/empty_payload_dropped`, and
`scheduler/queue/replacement_poison_dropped` during migration.

JSON is a wire format, not encryption. Queue payloads can contain request
bodies, metadata, callback arguments, cookies, tokens, or personal data. Use
authenticated TLS, least-privilege topic/key/index ACLs, and encryption at rest
or application-layer encryption before copying a backlog or snapshot.

## Strategy Snapshots

Only strategies with in-process state produce snapshots, and persistence is
available only when the queue's own `ConnectionManager` also exposes Storage.
Configuring a separate storage backend for the item pipeline does not give a
Kafka/RabbitMQ/Pulsar/SQS/RocketMQ queue manager snapshot capability.

Without an owner, the logical snapshot key remains:

```text
queue:snapshot:<spider-name>:<queue-name>
```

With `SCRAPY_QUEUE_SNAPSHOT_OWNER=<owner>` (or the
`SCRAPY_QUEUE_WORKER_ID` fallback), the logical key becomes a length-prefixed
v2 identity:

```text
queue:snapshot:v2:<owner-length>:<owner>:<spider-length>:<spider>:<queue>
```

Every worker using a stateful queue strategy must have a stable, unique owner.
Enabling an owner does not consume or delete the old unowned snapshot. Decide
while workers are stopped whether to restore the old state once, transform it
to the owner-specific key, or discard it.

A successful restore deletes the consumed snapshot and a later clean close
writes current state again. Deletion is best-effort: alert on an error because
a crash before the next successful close can replay the stale snapshot.

## TTL Contract

Direct `StorageBackend.store(key, data, ttl=...)` calls now accept only:

- `None` for no expiry;
- a positive integer number of seconds.

Zero, negative values, floats, and booleans raise `ValueError`. `ttl()` returns
a non-negative integer or `None`; backend-specific missing/no-expiry sentinels
are no longer exposed. At the Scrapy pipeline boundary only,
`SCRAPY_PIPELINE_TTL=0` remains a permanent-value shorthand and is normalized
to `None` before storage.

Audit direct API callers separately from pipeline settings. Code that used
`ttl=0` directly must change to `ttl=None`.

## Configuration Changes

The adapter now rejects unknown nested fields and unknown environment/flat keys
under the selected bundled backend prefix. Correct common legacy spellings:

| Old or unsafe form | Current form |
|---|---|
| Redis `startup_nodes` | `cluster_startup_nodes` / `SCRAPY_REDIS_CLUSTER_STARTUP_NODES` |
| Redis `ssl` | `ssl_enabled` / `SCRAPY_REDIS_SSL_ENABLED` |
| Redis `ssl_cert_reqs` | explicit `ssl_cafile`, `ssl_certfile`, `ssl_keyfile`, `ssl_check_hostname` |
| RabbitMQ host/port with implicit guest credentials | explicit username and password, or `SCRAPY_RABBITMQ_URL` |
| AWS standalone mode without an endpoint | LocalStack-compatible `endpoint_url`; use cloud mode for the AWS endpoint/credential chain |
| comma-separated environment value for a list | JSON array, for example `'["https://es1:9200"]'` |

Field type, range, enum, and Pydantic extra-field failures raise
`pydantic.ValidationError`. Unknown adapter settings, unsupported capabilities,
and project cross-field constraints raise `ConfigurationError`.

Queue-only backends must be bound with `SCRAPY_QUEUE_BACKEND_TYPE`; retain a
set-capable backend for the default distributed dedup filter and a
storage-capable backend for the item pipeline. `priority` and `work_stealing`
are rejected with Kafka and RocketMQ.

## Lease and Clear Semantics

SQS and RocketMQ deliveries have finite visibility/invisibility leases and the
extension does not renew them. Set the lease above the maximum time from pop to
Scrapy downloader response. SQS nack makes a message immediately visible;
RocketMQ nack uses its 10-second minimum delay.

Memcached cannot enumerate keys for prefix deletion. Prefix clear is always
unsupported, and global `clear_storage(None)` is disabled unless
`SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=True`. That flag issues server-wide
`flush_all`; enable it only for a dedicated Memcached instance.

## Validation and Rollback

Before opening traffic, verify:

- effective component backend types and normalized settings;
- queue counts, payload sidecar counts, and a sample request round trip;
- dedup membership and Storage values/TTLs;
- unique snapshot owner per worker;
- broker TLS, ACL, and at-rest controls;
- poison-drop, ack/nack, queue-depth, and storage-error stats;
- SQS/RocketMQ lease duration against the slowest request path.

For rollback, stop all current workers first. Restore the backend backup or
reverse the type-aware key mapping, then start only old-version workers. Never
point an old and current process at the same live backlog during rollback.
