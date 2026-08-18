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

## Cuckoo Item-Deletion Contract

`CuckooMembershipFilter.remove()` now raises `NotImplementedError` without
mutating the filter. Cuckoo stores truncated fingerprints rather than item
identities, so a never-inserted item can be indistinguishable from a resident
item; deleting that slot could make the resident item a false negative.

Direct callers that need an intentional whole-filter reset should use
`clear()`. Callers that require exact per-item removal should select the
`memory` or `set` strategy instead. `BackendDupeFilter.forget()` needs no caller
change: after a failed queue push it retains the Cuckoo fingerprint and grants
one bounded retry allowance, exactly as it does for Bloom filters. Concurrent
matching requests can consume that allowance only once.

No persisted-state migration or legacy drain is required because Cuckoo state
is process-local. Stop old workers normally; their tables disappear with the
process. Successful inserts, the no-false-negative guarantee for those inserts,
and `FilterFull` behavior are unchanged.

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
Malformed or empty endpoint members, nonnumeric/out-of-range ports, URL
userinfo, paths, queries, and fragments now fail at startup; correct the
service URL before upgrading.
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
TLS. Anonymous standalone/cluster connections are plaintext only on loopback by
default. Remote plaintext requires the explicit trusted-network override below.
The TLS flag targets the RocketMQ 5.x gRPC proxy and is propagated separately
to both SDK client constructors; it is not a `ClientConfiguration` option.

## Remote Plaintext Opt-in

Unauthenticated connections to a non-loopback endpoint now fail at settings
validation unless an operator explicitly accepts the trusted-network risk.
Loopback defaults (`localhost` and literal loopback IP addresses) remain
unchanged. Set the matching environment variable to exact boolean `true` only
when the network boundary is intentionally private and controlled:

| Backend | Explicit acknowledgement setting | Plaintext condition |
|---|---|---|
| Redis | `SCRAPY_REDIS_ALLOW_REMOTE_PLAINTEXT` | `ssl_enabled=False` with no Redis or Sentinel credentials |
| MongoDB | `SCRAPY_MONGO_ALLOW_REMOTE_PLAINTEXT` | no effective TLS and no MongoDB authentication |
| Elasticsearch | `SCRAPY_ELASTICSEARCH_ALLOW_REMOTE_PLAINTEXT` | unauthenticated remote `http://` host |
| Kafka | `SCRAPY_KAFKA_ALLOW_REMOTE_PLAINTEXT` | unauthenticated `PLAINTEXT` brokers |
| Pulsar | `SCRAPY_PULSAR_ALLOW_REMOTE_PLAINTEXT` | unauthenticated `pulsar://` endpoint |
| RocketMQ | `SCRAPY_ROCKETMQ_ALLOW_REMOTE_PLAINTEXT` | `tls_enabled=False` with no access/secret key pair |

These flags authorize only remote anonymous plaintext; they never weaken
credential or TLS validation. For example, authenticated Redis or MongoDB
connections still require their verified TLS settings, Elasticsearch credentials
over `http://` still fail, Kafka `SASL_PLAINTEXT` still fails, Pulsar tokens
still require `pulsar+ssl://`, and authenticated RocketMQ connections still
require `tls_enabled=True`.

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

Only strategies with in-process state produce snapshots. Redis, MongoDB, and
Elasticsearch queues use their own storage capability. A Kafka, RabbitMQ,
Pulsar, SQS, or RocketMQ queue can instead use the configured
`SCRAPY_STORAGE_BACKEND_*` component for snapshots; this scheduler-owned
acquire is independent of whether the item pipeline is enabled. A legacy
queue-only global configuration with no storage component continues to skip
snapshots best-effort.

Without an owner, the logical snapshot key is now a length-prefixed v3
identity:

```text
queue:snapshot:v3:<spider-length>:<spider>:<queue-length>:<queue>
```

With `SCRAPY_QUEUE_SNAPSHOT_OWNER=<owner>` (or the
`SCRAPY_QUEUE_WORKER_ID` fallback), the logical key becomes a length-prefixed
v2 identity:

```text
queue:snapshot:v2:<owner-length>:<owner>:<spider-length>:<spider>:<queue>
```

A v3 checkpoint is preferred. The package automatically checks the old
`queue:snapshot:<queue>` form only when there is no named spider and the queue
name contains no `:`; after a successful v3 store or delete it retires that
eligible old key. If a checkpoint update fails before legacy retirement, it
leaves the old key untouched. For a clean empty checkpoint, it first writes a
separate private empty marker for the v3 identity, deletes the v3 checkpoint,
then retires the eligible old key, and finally removes the marker. If the
marker persists after an interruption, it blocks legacy fallback only when the
v3 checkpoint is absent rather than replaying the old checkpoint.

Do not rely on automatic recovery of old named-spider keys such as
`queue:snapshot:<spider>:<queue>`, or of any old key with `:` in its unscoped
queue name. The old delimiter format cannot distinguish a named `(spider,
queue)` pair from an unscoped queue named `<spider>:<queue>` (and colon-bearing
components add more possible splits). To avoid restoring or deleting another
queue's checkpoint, the upgrade deliberately leaves those ambiguous keys
untouched. While all workers are stopped, drain them with the old package or
perform an explicit, operator-verified migration to the intended v3/v2 key.

Every worker using a stateful queue strategy should have a stable, unique owner.
Enabling an owner does not consume an old unowned snapshot automatically; decide
while workers are stopped whether to drain, transform, or discard that state.

A successful restore retains its v3 checkpoint until a later clean close writes
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

Several timeout settings now reject non-finite values and are capped at 86400 s
(24 h), so a deployment carrying `SCRAPY_REDIS_SOCKET_TIMEOUT=inf`,
`SCRAPY_REDIS_SOCKET_CONNECT_TIMEOUT=inf`, or `SCRAPY_ELASTICSEARCH_REQUEST_TIMEOUT=inf`
(or any huge finite value above 86400 s) now fails at config load with a
`pydantic.ValidationError`. Previously `Field(ge=0)` accepted `inf`, which made
the underlying driver call `socket.settimeout(inf)` and raise an `OverflowError`
(an `ArithmeticError`, not an `OSError`) that escaped the driver's `OSError`
trap and wedged connect retries. Pick a finite value ≤ 86400 s (the defaults —
Redis 30 s / 5 s, ElasticSearch 30 s — are unaffected). The RabbitMQ
`SCRAPY_RABBITMQ_HEARTBEAT` cap (≤ 65535, the AMQP `Tune-Ok` unsigned-short
bound) is enforced the same way and surfaces in the `ValidationError`.

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
A live connection is idempotent, and endpoint, region, queue prefix, physical
queue-name generation, visibility timeout, QueueUrl caches, and receipt tokens
remain fixed to that generation. Code that mutates `SqsSettings` after startup
must explicitly call `disconnect()` and then `connect()` before expecting new
values. Disconnect is now a drain barrier: operations admitted first finish on
the old client, while operations arriving after teardown begins raise
`QueueError`. A receipt token from the retired client becomes stale and is never
acknowledged through the replacement; SQS visibility timeout/redrive provides
its at-least-once retry. Allow shutdown enough time for an admitted long poll,
SDK retry, or 60-second purge barrier.

### SQS physical queue-name v2 migration

`SCRAPY_SQS_QUEUE_NAME_GENERATION` now defaults to `v2`. V2 hashes every
length-prefixed `(queue_name_prefix, logical_queue_name)` tuple into the single
`scrapyext-v2-*` namespace. This removes prefix-boundary aliases within v2. The
output is always a valid 53-character SQS Standard queue name. Because a legacy
direct queue can still deliberately have that same name, each v2 queue is also
bound to the full tuple with the package tag
`scrapy-extension:queue-owner=scrapy-extension:sqs:v2:<40-hex-digest>`.
Existing v2 QueueUrls are accepted and cached only after that exact owner is
read back. A missing, different, malformed, or unreadable owner fails closed
before push, pop, depth inspection, or clear; `legacy_v1` preserves its old
untagged behavior. Grant v2 workers `sqs:ListQueueTags` and permission to tag a
new queue as part of `CreateQueue`. Message bodies and receipt handles are
unchanged.

V2 deliberately does **not** auto-read the old queue. Existing deployments must
migrate without concurrent generations:

1. **Stop all producers** that can write the affected logical queues. Keep their
   configuration recorded; do not start v2 producers yet.
2. Start only drain workers with
   `SCRAPY_SQS_QUEUE_NAME_GENERATION=legacy_v1`. This deprecated mode reproduces
   the previous direct-or-hash mapping exactly. Drain every old physical queue,
   including invisible/delayed messages and any dead-letter/redrive workflow,
   until the broker and application agree that no work remains. Do not run v2
   workers or producers during this drain.
3. Stop the legacy workers. Change producers and workers to
   `SCRAPY_SQS_QUEUE_NAME_GENERATION=v2` (or remove the setting to use the safe
   default), then **atomically switch the whole worker group**. Resume producers
   only after all active workers select v2.

A mixed `legacy_v1`/`v2` fleet splits one logical queue across two physical
queues; the backend will not dual-read or reconcile them. Rollback therefore
uses the same boundary in reverse: stop producers and workers, drain the active
v2 queue with v2 workers, and only then atomically restore legacy workers. Never
use `legacy_v1` for a new deployment.

If the v2 physical name already exists without the expected owner (including a
legacy direct-name alias), keep every producer and consumer stopped and choose
one explicit maintenance path:

- **Drain/delete/recreate (preferred):** drain the existing queue with the
  configuration that owns it, delete it, wait until SQS reports the name absent
  and the post-delete reuse delay has elapsed, then let one v2 worker create the
  tagged replacement before reopening traffic.
- **Ownership adoption:** only after independently proving that the existing
  queue and its redrive/dead-letter policy belong exclusively to the intended
  full `(queue_name_prefix, logical_queue_name)` tuple, attach the exact owner
  tag with trusted operator tooling and read it back. The `<40-hex-digest>` is
  the suffix of the expected `scrapyext-v2-<40-hex-digest>` physical name. Never
  adopt a non-empty ambiguous queue merely to bypass the check.

A connection configured for `legacy_v1` emits its deprecation warning only
after that validated client generation is connected. Constructing settings or
a backend does not warn; an explicit disconnect/reconnect after changing the
setting warns for the newly selected legacy generation.

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
underlying botocore client. Local clear/store ordering is now linearized.
Package stores generate `_scrapy_revision` with `uuid.uuid4().hex`; the required
stored grammar is exactly 32 lowercase hexadecimal characters. This per-write
revision fences cross-process same-key replacements from stale clear deletes;
DynamoDB Scan still has no cross-page snapshot isolation for newly inserted
keys.

DynamoDB custom endpoints must now be configured with
`SCRAPY_DYNAMODB_ENDPOINT_URL`. The backend intentionally ignores
`AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_DYNAMODB`, and shared-config custom
endpoints so an ambient URL cannot bypass cloud-mode transport validation.
Ambient credentials continue to work; only endpoint routing is isolated.

DynamoDB package writes now include the reserved `_scrapy_revision` string
attribute generated with `uuid.uuid4().hex`. Its value is exactly 32 lowercase
hexadecimal characters (32 bytes), and its value and attribute name count toward
the 400 KiB item limit, so payloads that sat exactly at the previous maximum
must shrink by 48 bytes. Revision-fenced rows need no migration and normal clear
safely fences each observed revision. A legacy row without the attribute is
different: default clear preserves it and raises
`StorageError(operation="clear_storage", key=None)` because identical-value ABA
cannot be detected from its attributes.

Migrate revisionless rows only in a stopped-writer window. Prefer reading each
legacy item and rewriting it through the current package with a smaller payload
where necessary, which installs a fresh revision. For rows that cannot be
enlarged—especially pre-existing exact-400-KiB items—start one maintenance
generation with
`SCRAPY_DYNAMODB_ALLOW_UNFENCED_LEGACY_CLEAR=True`, run the intended clear while
all writers remain stopped, then disable the setting and reconnect before
resuming work. This high-risk override conditionally deletes the observed legacy
attributes directly, so it does not add bytes, but identical-value ABA remains
indistinguishable and is safe only because writers are quiesced. Applications
writing directly to the table must set `_scrapy_revision` to a freshly generated
`uuid.uuid4().hex` on every replacement—exactly 32 lowercase hexadecimal
characters; preserving a prior revision defeats replacement detection.

Clear now uses one conditional `DeleteItem(ALL_OLD)` per observed row and
validates the returned old-item identity. The old key-only BatchWrite path and
its application-level `UnprocessedItems` retry budget were removed because
BatchWrite cannot express conditions. Botocore's configured retries/timeouts
still apply to each delete RPC. A condition loss, Scan/delete failure, malformed
response, legacy fail-closed stop, or repeated cursor raises the typed
partial-result error instead of claiming success. Earlier deletes may already
be committed, no rollback occurs, and retrying starts a new convergent clear.
Operators requiring an empty result must still stop all external writers for the
whole operation because Scan cannot fence newly inserted, unobserved keys.

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

## Batched Storage Close Retry

Batched manual flush and shutdown now expose a strict durability result. A
direct `BatchedStorageStrategy.flush()` whose drain lock remains busy through
the bounded wait raises a fixed `StorageError` and retains its pending records;
it no longer returns as though the flush completed. Update callers that treated
`flush()` as non-raising to catch the typed failure and retry or fail the unit
of work.

A normal `BackendPipeline.close_spider()` failure now leaves the pipeline in a
distinct retry-only closing state. New `process_item()` and `open_spider()`
calls are rejected rather than returning success-shaped items while the failed
batch remains volatile. The manager acquire is intentionally retained. Shutdown
integrations must keep that pipeline instance and retry `close_spider()` until
it returns normally; only that successful durability barrier releases the
manager. Do not manually close the manager between attempts. If manager release
itself raises after the batch drained, its outcome is ambiguous and the
pipeline is terminal, so retrying would risk a duplicate release.

Before upgrading, audit custom Scrapy shutdown wrappers and tests for
best-effort one-shot close behavior. During rollback, drain all retained batches
with the version that created them before replacing workers; the retry buffer is
in-process and cannot be migrated across a restart.

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
