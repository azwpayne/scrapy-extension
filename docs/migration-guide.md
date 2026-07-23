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

## Pulsar TLS Hostname Validation

Pulsar TLS client construction now uses the keyword names accepted by
`pulsar-client` 2.11–3.x. The package-level compatibility settings keep their
existing names:

- `SCRAPY_PULSAR_ALLOW_INSECURE_CONNECTION` maps to
  `tls_allow_insecure_connection`;
- `SCRAPY_PULSAR_TLS_TRUST_CERTS_FILE` maps to
  `tls_trust_certs_file_path`;
- new `SCRAPY_PULSAR_TLS_VALIDATE_HOSTNAME` maps directly to
  `tls_validate_hostname` and defaults to `True` for `pulsar+ssl://` URLs.

Before upgrading a TLS deployment, verify that each broker certificate covers
the hostname used in `SCRAPY_PULSAR_SERVICE_URL`. Replace a mismatched
certificate or service URL rather than disabling validation. Setting
`SCRAPY_PULSAR_TLS_VALIDATE_HOSTNAME=False` is an explicit insecure
compatibility escape hatch for unauthenticated isolated local environments
only. When `SCRAPY_PULSAR_AUTH_TOKEN` is configured, both hostname and
certificate verification are mandatory; `ALLOW_INSECURE_CONNECTION=True` and
`TLS_VALIDATE_HOSTNAME=False` now fail at startup. Blank tokens and URL
userinfo are also rejected without retaining their values. Plain `pulsar://`
deployments do not forward any TLS keyword and are otherwise unchanged.

The Pulsar SDK treats URL schemes as case-sensitive. Settings now trim outer
whitespace, lowercase only the scheme, and trim comma-separated endpoints
before client construction. Cluster discovery uses one prefix:
`pulsar://broker-one:6650,broker-two:6650`. A repeated form such as
`pulsar://broker-one:6650,pulsar://broker-two:6650` is rejected at startup
because the SDK interprets the second prefix as an invalid hostname.
Connection setup revalidates one captured settings snapshot and uses it for
both client and later subscription construction. Public startup errors no
longer include raw driver text or the service URL.

## RocketMQ Authenticated TLS

RocketMQ now exposes `SCRAPY_ROCKETMQ_TLS_ENABLED`. Set it to `True` on both
cloud and authenticated standalone/cluster deployments:

```python
SCRAPY_ROCKETMQ_TLS_ENABLED = True
SCRAPY_ROCKETMQ_ACCESS_KEY = "..."
SCRAPY_ROCKETMQ_SECRET_KEY = "..."
```

The access and secret keys must be supplied together and neither may be empty
or whitespace-only. Cloud mode refuses to start without this complete pair and
TLS. Anonymous standalone/cluster connections remain compatible with the
previous plaintext default, but should be limited to trusted local networks.
The TLS flag targets the RocketMQ 5.x gRPC proxy and is propagated separately
to both SDK client constructors; it is not a `ClientConfiguration` option.

## SQS Private boto3 Sessions

Every new SQS connection generation now creates a private
`boto3.session.Session` and constructs its low-level client from that Session.
It no longer calls the module-level `boto3.client()` alias or inherits the
process-wide Session installed by `boto3.setup_default_session(...)`. This
isolates independent backend instances within one process and ensures an explicit
disconnect/reconnect re-resolves ambient credentials instead of retaining a
credential object cached by an older generation.

No queue data migration is required. Before upgrading, replace any default-
Session credential injection with one of these supported sources:

- botocore's ambient credential provider chain, including credential
  environment variables and `AWS_PROFILE`-selected shared files;
- an IAM role or workload identity available to botocore; or
- the existing explicit SQS access-key and secret-key settings.

The SQS region is not inherited from the profile/default Session; it remains
controlled by `SCRAPY_SQS_REGION_NAME`. A configured SQS access/secret pair is
passed directly to the client and takes precedence over ambient credentials.
A custom endpoint URL can come only from `SCRAPY_SQS_ENDPOINT_URL`: ambient
`AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_SQS`, and service endpoints in shared AWS
config are ignored. Migrate those custom URLs to the SQS setting, whose
cloud-mode value must be HTTPS. When that setting is unset, botocore may still
select its standard AWS FIPS or dual-stack endpoint variant.

Custom botocore event hooks registered only on the process-wide default
Session are no longer injected into this backend. If those hooks are required,
provide an explicitly customized backend rather than depending on global boto3
state. Restart workers so every process constructs a new private generation;
the SQS message and receipt formats are unchanged.

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

## Redis Connection Generations

Redis connection settings and the physical-key namespace are now captured in
one immutable generation. Calling `connect()` while a generation is published
is an idempotent no-op; it does not recheck health. After `ping()` fails, use an
explicit `disconnect()` / `connect()` sequence to recover. Code that previously
mutated `RedisSettings` and called `connect()` again must use the same sequence
before expecting a changed connection-used endpoint, credential, TLS policy,
mode, or namespace to take effect.

Every bundled backend operation is pinned to its issuing generation. A timed
`pop()` that overlaps teardown now raises `QueueError` instead of continuing to
poll through a replacement client. Other already admitted operations drain;
this keeps a multi-step `clear_storage()` on one client but means shutdown can
wait for its SCAN/DELETE sequence or another active Redis command. Size socket
timeouts and maintenance windows accordingly, and stop new work before a large
clear. SCAN is not a transactional keyspace snapshot: concurrent external
writers can be missed, and a failure after accepted deletes is reported as
possibly partial. Quiesce writers and rerun a failed maintenance clear after
repairing Redis connectivity.

A new operation started after teardown completes retains lazy connection
compatibility. An operation that overlapped teardown is fenced and cannot
resurrect itself on the replacement. Direct `RedisBackend.pop()` timeout values
must now be finite, non-negative numbers; booleans, negative/non-finite values,
wrong types, and values that overflow a float raise `ValueError` before any
lazy connection attempt.

Direct callers may still use `RedisBackend.client`, which lazily returns the
current raw redis-py object. That return value is only a point-in-time escape
hatch: it carries no operation lease once the property returns and can be
closed by a concurrent disconnect. Replace retained raw-client and
multi-command usage with the bundled backend methods, or coordinate its entire
lifecycle outside the backend. No Redis data rewrite is required solely for
this lifecycle change.

## Redis Timeout Retry Policy

Redis data-plane commands no longer receive automatic redis-py transport
retries. Supported redis-py releases include timeout errors in their default
retry object even when the deprecated `retry_on_timeout=False` argument is
passed. If a push or pop Lua script committed and only its response was lost,
that default could enqueue a duplicate or consume a second item. Other
apparently idempotent mutations are also unsafe to replay invisibly because a
second result, intervening writer, or refreshed TTL can change their meaning.

`RedisSettings.retry_on_timeout` and
`SCRAPY_REDIS_RETRY_ON_TIMEOUT` remain parseable with their historical default
for Stable configuration compatibility, but are deprecated compatibility
inputs. Both values now select the same zero-replay data policy, and explicit
use emits `FutureWarning` when the backend is constructed. Remove the field
from programmatic, Scrapy, and environment configuration. Do not replace it with
`SCRAPY_RETRY_ATTEMPTS`: that setting retries connection establishment, not a
failed data command.

Zero replay guarantees that the SDK does not secretly resend a data command
after an outcome-ambiguous connection, write, or response failure. A reported
failure may follow a committed first attempt, and no automatic rollback or
reconciliation is possible. Server-confirmed non-execution paths such as
NOSCRIPT and Cluster MOVED/ASK/TRYAGAIN can still continue safely. redis-py
couples ClusterDown/SlotNotCovered recovery to the same outer retry count, so
those two Cluster errors now fail fast. An uncovered slot discovered before
command routing is not guaranteed to refresh on another ordinary call; use an
explicit `disconnect()` / `connect()` to build a fresh topology generation.
Do not blindly repeat queue push/pop operations; use an application operation
ID, deduplication, or domain-specific reconciliation where loss/duplication is
unacceptable.

The separate `sentinel_retry_on_timeout` setting remains active only for
read-only Sentinel discovery. When true, it permits at most one immediate SDK
retry after a timeout for each control request; it does not retry
authentication failures. Sentinel may still continue discovery against another
configured endpoint, and the setting does not limit ConnectionManager
connection attempts. No Redis key migration is required for this policy
change.

## Redis Deployment Modes and Endpoint Grammar

Redis configuration now distinguishes three effective topologies from the
deprecated `master_slave` compatibility alias:

| Previous configuration | Current contract | Migration action |
|------------------------|------------------|------------------|
| `mode="master_slave"` with no effective replica routing | primary-only deprecated alias | Change to `standalone` for the same runtime behavior. |
| non-empty `replicas` or `read_from_replicas=True` | rejected unsupported intent | Remove both fields. Use Sentinel for primary discovery/failover; true eventual-consistency replica reads require a custom backend/policy. |
| Cluster `db > 0` | rejected; Redis Cluster supports DB0 only | Set `db=0` and isolate with `namespace` or a separate Cluster. The old backend already discarded the configured DB and used DB0, so do not assume data exists in DB N. |
| URI/userinfo node such as `redis://user:pass@host:6379` | rejected without echoing the value | Put the bare host/port and `username`/`password` in separate fields. Use `[IPv6]:port` in endpoint lists. |
| CA/certificate/key with `ssl_enabled=False` | rejected | Enable TLS explicitly or remove the unused material; the backend never auto-enables a protocol. |
| `masters` input | rejected tombstone instead of ignored/echoed | Replace it with `cluster_startup_nodes` and select `mode="cluster"`. |
| topology nodes or non-default controls for a different selected mode | rejected instead of ignored | Remove them or select the matching `sentinel` / `cluster` mode. |
| `cluster_max_redirects > 100` | rejected | Reduce it to 100 or less and diagnose persistent redirection/topology churn instead of masking it with an unbounded loop. |
| scalar port as bool/float/bytes/signed/whitespace text | rejected | Use an integer or unsigned ASCII decimal text from 1 through 65535. |
| legacy numeric IPv4 such as `127.1`, `2130706433`, or `0x7f000001` | rejected | Write the canonical dotted quad, for example `127.0.0.1`. |

Active endpoint lists (`sentinels` and `cluster_startup_nodes`) accept ASCII
DNS/IPv4 `host:port` or `[IPv6]:port`, with a port from 1 through 65535. The
deprecated `replicas` field rejects every non-empty value because replica
routing is unsupported. Scalar `host` accepts a bare DNS name, canonical IPv4,
or IPv6 address. Schemes, userinfo, paths, queries, fragments,
whitespace/control characters, raw Unicode hostnames, and non-ASCII port
digits fail during model construction and are rechecked before SDK I/O after
mutation.

`cluster_max_redirects` remains active: 0 means no protocol follow-up after the
initial command, and N permits at most N MOVED/ASK/TRYAGAIN continuations. It
does not alter the zero-replay transport Retry object. Cluster and Sentinel
SDK failures now surface through the existing package exception types; callers
that caught raw `RedisClusterException` should catch `BackendError` (or the
specific `QueueError`, `BackendConnectionError`, or `StorageError`) instead.
The original data-plane SDK error is retained only as `__cause__`, so treat a
full chained traceback as sensitive diagnostics.

Sentinel control credentials (`sentinel_username` / `sentinel_password`) never
fall back to data-plane `username` / `password`; configure both pairs when the
same identity is required on both planes. `max_connections` is a per-pool cap:
S Sentinel endpoints create S control pools plus one discovered-master data
pool. No Redis key or wire-data migration is required solely for these mode,
endpoint, error, or binary-decoding changes.

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

Retry, redirect, and user-errback replacement requests retain the source
delivery until the replacement queue commit. An errback iterable is one commit
group: every returned request must be accepted before the source is acked. The
replacement publish and source ACK cannot be atomic across brokers, so a crash
between them can still redeliver the source and create a duplicate; retain
deduplication or make `dont_filter=True` replacements idempotent.

The bundled scheduler now checks dedup membership, durably pushes, and only
then publishes a persistent marker. This closes the failed-push ghost-marker
window but intentionally changes concurrent admission: two workers that both
observe a fresh fingerprint may enqueue it before either marker is visible.
Treat callbacks and item writes as idempotent under at-least-once replay. Queue
strategies that accept only into process-local state use a bounded local dedup
shadow instead of publishing a persistent marker.

Custom `QueueStrategy.is_push_durable(*, delay, source)` claims are no longer
accepted as durable commit evidence. They are evaluated before the item is
serialized and cannot bind a later route or backend generation to the actual
push. The hook remains callable for compatibility, but inherited, missing, and
literal-`True` implementations are all treated as volatile by the bundled
scheduler unless the strategy participates in its private operation-bound
prepared route. Ordinary requests use the bundled dupefilter's lifecycle-local
shadow; requests carrying an unacknowledged source token fail closed before
plugin-local mutation. Prefer a bundled backend-delegating strategy for those
transfers; private receipt APIs have no compatibility promise.

Third-party `QueueBackend` implementations remain source compatible because
the new push operation has a concrete default. Ordinary pushes still call the
existing public `push()` once, but receive a volatile receipt; a
durability-required source transfer is rejected before `push()` mutates the
backend. Custom queue objects keep their public return contract, and `False`,
`True`, `None`, or another truthy return value is ignored for durability.

A replacement carrying an unacknowledged source token is now rejected before
it enters volatile `delay`/`time_wheel` holding state (positive effective
delay), `round_robin`, or `ring_buffer`. Migrate those flows to a
backend-durable strategy/path; a zero effective delay remains a direct backend
push.

If `ring_buffer` uses `full_policy=drop_oldest`, the overwritten request's
volatile dedup shadow is intentionally retained until bounded-shadow eviction
or lifecycle end. Upgrading does not turn that explicitly lossy policy into an
automatic retry mechanism; use `reject` or a durable strategy when dropped work
must be resubmitted.

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

A successful restore retains its checkpoint until a later clean close writes
the current state or deletes the key after a clean drain. A crash during that
interval replays the prior checkpoint: completed work can repeat, but pending
work is not lost. Keep callbacks idempotent and alert on checkpoint store/delete
failures, which extend the duplicate-replay window.

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
| RabbitMQ URL userinfo or remote `amqp://` | credential-free `amqps://` URL plus explicit username/password fields |
| AWS standalone mode without an endpoint | LocalStack-compatible `endpoint_url`; use cloud mode for the AWS endpoint/credential chain |
| comma-separated environment value for a list | JSON array, for example `'["https://es1:9200"]'` |

Field type, range, enum, and Pydantic extra-field failures raise
`pydantic.ValidationError`. Unknown adapter settings, unsupported capabilities,
and project cross-field constraints raise `ConfigurationError`.

For Redis Sentinel, `ssl_enabled=True` now applies to Sentinel discovery as
well as the discovered master. Verify every Sentinel endpoint presents a
certificate trusted by `ssl_cafile` and covered by hostname validation. mTLS
requires both `ssl_certfile` and `ssl_keyfile`; a partial pair now fails before
network I/O. Deployments that intentionally mixed plaintext Sentinel with a
TLS data plane must align the control plane with TLS before upgrading.

RabbitMQ plaintext is now a loopback-only development path. Remove credentials
from `SCRAPY_RABBITMQ_URL`, set them through
`SCRAPY_RABBITMQ_USERNAME`/`SCRAPY_RABBITMQ_PASSWORD`, and use `amqps://` (or
`SCRAPY_RABBITMQ_SSL_ENABLED=True`) when the primary or any cluster node is
remote. TLS always enforces `CERT_REQUIRED` and hostname matching; optional
client authentication requires both certificate and key files. An explicit
`ssl_enabled=False` can no longer downgrade an `amqps://` URL, and the `guest`
user is accepted only for an all-loopback endpoint set.

Queue-only backends must be bound with `SCRAPY_QUEUE_BACKEND_TYPE`; retain a
set-capable backend for the default distributed dedup filter and a
storage-capable backend for the item pipeline. `priority` and `work_stealing`
are rejected with Kafka and RocketMQ.

## Lease and Clear Semantics

SQS and RocketMQ deliveries have finite visibility/invisibility leases and the
extension does not renew them. Set the lease above the maximum time from pop to
Scrapy downloader response. SQS nack makes a message immediately visible;
RocketMQ nack uses its 10-second minimum delay.

Kafka tokens now include the consumer generation, partition-assignment epoch,
and a unique delivery attempt. Nacking an assigned record seeks it for retry
and permanently retires that attempt; a subsequent delivery of the same offset
gets a distinct token. Rebalance callbacks and subscription changes fence all
prior tokens before the new assignment can be settled. Code that directly
calls `pop_with_ack()` must retain and return the exact token, rather than
reconstructing one from topic/partition/offset.

Pulsar tokens now allow exactly one successful terminal action across ACK and
NACK, including concurrent calls. A client exception leaves the same token
retryable. `pop_with_ack()` no longer also populates the legacy tokenless slot,
so direct integrations must retain and settle the returned token; code that
intentionally uses tokenless settlement must continue to call `pop()`.

Kafka `clear_queue()` now raises `QueueError`. The previous
delete-and-immediately-recreate sequence was not a completion barrier: topic
deletion propagates asynchronously, newly accepted records can race the old
delete, and a reused consumer group can carry incompatible offsets into the
replacement topic. Stop all producers and consumers, drain or delete the topic
with Kafka's operator tooling, verify cluster metadata convergence, and choose
an intentional consumer-group offset policy before restarting.

Kafka SASL validation is now mechanism-specific. `SASL_*` without a mechanism,
incomplete or blank PLAIN/SCRAM credentials, GSSAPI combined with ignored
PLAIN fields, and blank Confluent keys/secrets all fail before SDK I/O.
GSSAPI continues to use the ambient Kerberos context. OAUTHBEARER configurations
must migrate to a supported mechanism or a separately managed client because
this backend does not expose kafka-python's required token-provider object.

Kafka `acks=0` is no longer accepted: it completes after a socket-buffer write
and cannot satisfy the queue commit boundary. Select `acks=1` or preferably
`"all"`. `num_partitions` and `max_priority_partitions` must now be equal, and
`min_insync_replicas` cannot exceed `replication_factor`. These retention and
minimum-ISR values are applied when the extension creates a topic; it does not
alter an existing topic. Existing partition, replication, retention, and
minimum-ISR policy is verified, and a mismatch blocks publication until it is
reconciled with broker tooling.

Kafka `queue_len()` now returns consumer-group lag from committed offsets, not
the current process's fetched position. It can therefore be larger while
records are in flight and not yet acknowledged. Fresh groups use
`auto_offset_reset`: `earliest` includes existing backlog, `latest` starts at
the end, and `none` raises `QueueError` when no committed offset exists. Callers
must not convert that error to zero; scheduler pending detection deliberately
stays conservative.

Pulsar and RocketMQ `queue_len()` now raise `NotImplementedError` instead of
returning a false zero. Broker-side depth requires the Pulsar admin API or a
RocketMQ depth RPC that these clients do not expose, so a number could not be
reported honestly. The `queue/depth` Scrapy stat no longer emits for these two
backends, and depth-based backpressure (`SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD`)
is skipped per poll — scheduler pending and idle detection deliberately stay
conservative. Monitor load via pop-rate and broker-native tooling, and catch
`NotImplementedError` wherever `queue_len()` is called directly.

MongoDB `w=0` and negative write concerns are no longer accepted because an
unacknowledged PyMongo result cannot satisfy queue, set, or storage mutation
success. Use a positive integer or `"majority"`; custom replica-set tag names
are outside this backend's supported settings surface. Boolean values are not
treated as integers. `w_timeout_ms` must be a non-negative integer when set.
These rules are rechecked immediately before client construction, so code that
mutates a settings model after construction must update it to a supported value
before reconnecting.

MongoDB queue, set, and storage collection names must now be pairwise distinct.
Before upgrading a deployment that reused one collection, stop every writer,
back up the database, classify the mixed documents by their capability schema,
and create three empty destination collections. Do not rename or reuse the old
mixed collection: its queue, set-uniqueness, storage-key, and TTL indexes stay
attached and can reject otherwise valid documents from a different domain.
Configure
`SCRAPY_MONGO_QUEUE_COLLECTION`, `SCRAPY_MONGO_SET_COLLECTION`, and
`SCRAPY_MONGO_STORAGE_COLLECTION` with the new distinct names, let the backend
install each marker and the domain-specific indexes before importing business
documents, then import only the corresponding queue, set (including dedup
fingerprints), or storage documents and verify the resulting indexes before
opening writers. Do not run
`clear_storage(None)` against the old mixed collection: it preserves only the
reserved capability-domain marker and would also remove queue and set
documents. Keep the marker in each new collection; deleting it removes the
cross-component/process ownership fence until the next successful connection.

RabbitMQ `clear_queue()` now fails with `QueueError` when the target queue has
an unacknowledged local delivery. RabbitMQ purge only removes ready messages;
allowing a later nack would otherwise resurrect work from before the clear.
Direct callers must retain and settle every token before clearing. To abandon a
worker's deliveries, disconnect, wait for the broker to requeue them, reconnect,
and then retry clear. A pending delivery on another queue does not block the
target queue.

RabbitMQ no longer treats repeated `connect()` as an implicit session
replacement. A healthy call is idempotent, and queue durability, auto-delete,
exclusivity, maximum-priority, and delivery-mode values stay fixed for that
connection generation. Code that mutates `RabbitMQSettings` after startup must
call `disconnect()` and then `connect()` before expecting the new policy. Teardown
immediately invalidates the published session and any private candidate; an
old acknowledgement token becomes a local no-op, and a timed pop interrupted
by reconnect raises `QueueError` rather than consuming from the new channel.
Budget for closing the old Pika channel/connection when explicitly replacing
an unhealthy generation because that close is the broker redelivery boundary.

RocketMQ delivery tokens now serialize ack and nack across the broker call.
After either action succeeds, every later settlement for that token is a no-op;
if the client call raises, the token remains locally pending and may be retried.
`pop_with_ack()` no longer populates the legacy `pop()`/`ack(token=None)` slot,
so callers must retain its returned token. Direct callers must not interpret a
concurrent no-op as a second broker outcome.

SQS no longer treats repeated `connect()` as an implicit client replacement.
A live connection is idempotent, and endpoint, region, queue prefix, visibility
timeout, QueueUrl caches, and receipt tokens remain fixed to that generation.
Code that mutates `SqsSettings` after startup must explicitly call
`disconnect()` and then `connect()` before expecting new values. Disconnect is
now a drain barrier: operations admitted first finish on the old client, while
operations arriving after teardown begins raise `QueueError`. A receipt token
from the retired client becomes stale and is never acknowledged through the
replacement; SQS visibility timeout/redrive provides its at-least-once retry.
Allow shutdown enough time for an admitted long poll, SDK retry, or 60-second
purge barrier.

SQS `clear_queue()` now blocks the target physical queue for at least 60 seconds
after PurgeQueue returns. AWS documents that the asynchronous purge can delete
messages sent during that interval, so returning earlier was not a safe clear
boundary. Other SQS queues remain usable. An exception whose request acceptance
is ambiguous is raised only after the same safety window, and tokens delivered
before the clear are fenced. Increase caller/shutdown timeouts that previously
assumed SQS clear returned immediately.

Memcached cannot enumerate keys for prefix deletion. Prefix clear is always
unsupported, and global `clear_storage(None)` is disabled unless
`SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=True`. That flag issues server-wide
`flush_all`; enable it only for a dedicated Memcached instance.

Memcached has no authenticated or encrypted transport in this backend. A
non-loopback `SCRAPY_MEMCACHED_HOST` now fails unless
`SCRAPY_MEMCACHED_ALLOW_REMOTE_PLAINTEXT=True` explicitly acknowledges an
isolated trusted-network deployment. Loopback hosts remain unchanged. Before
upgrading a remote deployment, verify network isolation/firewall policy and add
the opt-in; otherwise migrate the storage role to a TLS-capable backend.

All Memcached mutations now wait for a server reply. This can add one response
read to `store`, `delete`, and `clear_storage`, but prevents pymemcache's default
`noreply` mode from reporting an unconfirmed command as successful. Revisit
latency budgets rather than restoring noreply: the StorageBackend contract uses
the return boundary as the write result.

Shared Memcached backend instances now serialize all operations on their single
pymemcache protocol socket, including health checks and disconnect. Applications
that previously relied on concurrent calls over one client should budget for a
single in-flight operation per generation. `allow_flush_all` now accepts only a
real boolean (or canonical `true`/`false` environment text), is captured at
connect, and cannot be enabled by mutating settings afterward. A false flush
reply is an error rather than successful completion.

DynamoDB no longer treats repeated `connect()` as an implicit table/client
replacement. A live call is idempotent, and endpoint, region, table name, and
credential configuration/source selection remain fixed for that connection
generation; ambient providers may still refresh temporary credentials. Code
that mutates `DynamoDBSettings` after startup must call `disconnect()` and then
`connect()` before expecting new values. Every candidate now owns a private
boto3 Session. Shared backend instances serialize all Resource operations,
including health checks and the complete paginated clear; budget for one
in-flight operation per generation. Disconnect drains that call and closes the
underlying botocore client. Local clear/store ordering is now linearized, but
DynamoDB Scan still has no cross-process snapshot isolation.

DynamoDB custom endpoints must now be configured with
`SCRAPY_DYNAMODB_ENDPOINT_URL`. The backend intentionally ignores
`AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_DYNAMODB`, and shared-config custom
endpoints so an ambient URL cannot bypass cloud-mode transport validation.
Ambient credentials continue to work; only endpoint routing is isolated.

DynamoDB clear no longer delegates persistent `UnprocessedItems` to boto3's
unbounded `BatchWriter` exit loop. Each 25-item batch now has eight
application-level BatchWriteItem submissions and bounded full-jitter sleeps. A
Scan/BatchWrite failure, malformed response, repeated cursor, or exhausted
batch raises `StorageError(operation="clear_storage", key=None)` instead of
hanging or claiming success. This is intentionally non-transactional: earlier
deletes may already be committed, no rollback occurs, and retrying starts a new
convergent clear. Operators requiring an empty result must stop all external
writers for the whole operation. Botocore's own retries/timeouts are a separate
inner budget, so the per-batch limit is not a wire-attempt or global shutdown
bound.

The shared SQS/DynamoDB region check now accepts multi-label region identifiers
used across AWS partitions, such as `us-gov-west-1`, `us-iso-east-1`, and
`eusc-de-east-1`. Deployments previously blocked by the old three-label regex
can remove workarounds. This remains structural validation, not an availability
allowlist; a same-shaped typo or unsupported service/region pair still fails at
the SDK/service boundary.

DynamoDB `delete()` now validates the `DeleteItem(ALL_OLD)` result. Missing
`Attributes` still means the item did not exist and returns `False`; a complete
old item with the requested partition key returns `True`. Non-standard mocks,
proxies, or emulators that return malformed or mismatched `Attributes` now
raise `StorageError(operation="delete", key=...)` instead of producing a bare
shape error or an unreliable boolean. Update test doubles to reproduce the AWS
envelope (`{"Attributes": {"pk": requested_key, ...}}`).
SDK-call failures also stop copying driver diagnostics into
`str(StorageError)`; inspect the original `__cause__` only in a protected error
channel. Code that parsed provider text from the public message must switch to
typed operation/key handling.

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
