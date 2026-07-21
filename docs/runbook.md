# Operations Runbook

Common operational tasks for `scrapy-extension` deployments. Each recipe
assumes the Scrapy settings wiring is already in place (see
[`README.md`](../README.md) → *Quick Start*).

Before upgrading a persistent deployment, read the
[migration guide](migration-guide.md). It covers Redis physical-key changes,
strategy snapshot ownership, and queued-request wire compatibility.

For Redis Sentinel deployments with TLS enabled, verify both the Sentinel
ports and the discovered Redis master accept TLS using the configured CA.
`ssl_certfile` and `ssl_keyfile` are an inseparable mTLS pair, and hostname
verification is enabled by default. The backend never downgrades Sentinel
discovery to plaintext when the Redis data-plane TLS flag is set.

## Switch dedup strategy

Select a `MembershipFilter` via `SCRAPY_DEDUP_STRATEGY` — no code change
required.

| Strategy | When to use | Setting knobs |
|---|---|---|
| `set` (default) | Multi-worker exact dedup; cross-worker safe. | — |
| `memory` | Single-worker, in-process, optional LRU cap. | `SCRAPY_DEDUP_MEMORY_MAXSIZE` (default 1,000,000) |
| `bloom` | Single-worker, large cardinality, tolerates false positives. Never false-negatives. | `SCRAPY_DEDUP_BLOOM_CAPACITY`, `SCRAPY_DEDUP_BLOOM_ERROR_RATE` |
| `cuckoo` | Single-worker, large cardinality, needs deletion. Never false-negatives. | `SCRAPY_DEDUP_CUCKOO_CAPACITY`, `SCRAPY_DEDUP_CUCKOO_ERROR_RATE` |

```python
# settings.py
SCRAPY_DEDUP_STRATEGY = "cuckoo"
SCRAPY_DEDUP_CUCKOO_CAPACITY = 10_000_000
SCRAPY_DEDUP_CUCKOO_ERROR_RATE = 0.001
```

**Caveat (see [Guarantees](../README.md#guarantees)):** `memory`, `bloom`,
and `cuckoo` are per-process. For multi-worker exact dedup, use `set`.

When Cuckoo hits capacity it raises `FilterFull`, which `BackendDupeFilter`
catches, warn-once's, and degrades to passthrough for that fingerprint; the
`dupefilter/filter_full` stat increments on each occurrence.

## Switch queue semantics

Select a `QueueStrategy` via `SCRAPY_QUEUE_STRATEGY` — no code change
required.

| Strategy | When to use | Setting knobs |
|---|---|---|
| `passthrough` (default) | Backend queue semantics unchanged; distributed-exact. | — |
| `delay` | Per-request polite delay (e.g. rate-limit per domain). In-process; lost on crash. | `SCRAPY_QUEUE_DELAY_DEFAULT` |
| `round_robin` | Fair dispatch across `request.meta['source']`; no starvation. In-process. | — |
| `throttle` | Rate-limited pops. Effective rate under N workers = `N × (1 / min_interval)`. | `SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL` |
| `priority` | Strategy-layer priority buckets for backends without native priority. Not supported with Kafka or RocketMQ. | `SCRAPY_QUEUE_PRIORITY_LEVELS` |
| `time_wheel` | Hashed timing wheel for many short delays. In-process. | `SCRAPY_QUEUE_TIME_WHEEL_SIZE`, `SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND` |
| `work_stealing` | Own queue first, then peer queues when idle. Not supported with Kafka or RocketMQ. | `SCRAPY_QUEUE_WORKER_ID`, `SCRAPY_QUEUE_PEER_IDS`, `SCRAPY_QUEUE_STEAL_TIMEOUT` |
| `ring_buffer` | Bounded circular buffer with explicit overflow behavior. | `SCRAPY_QUEUE_RING_BUFFER_CAPACITY`, `SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY` |

```python
# settings.py
SCRAPY_QUEUE_STRATEGY = "delay"
SCRAPY_QUEUE_DELAY_DEFAULT = 2.0  # seconds
```

**Caveat:** every non-`passthrough` queue strategy keeps at least some state in-process. Only `passthrough` is distributed-exact. Treat delay heaps, timing wheels, rate limiters, work-stealing cursors, and ring buffers as performance/fairness tools rather than durable scheduling logs.

`priority` and `work_stealing` create multiple physical queues. Their factories
raise `ConfigurationError` with Kafka and RocketMQ because those backends use a
single consumer that cannot isolate a pop to the requested strategy topic.
`round_robin` and `ring_buffer` are fully local: pairing them with an MQ backend
intentionally bypasses broker durability. Other bundled backend-delegating
strategies preserve per-message ack tokens.

### Snapshot ownership

`delay`, `round_robin`, `time_wheel`, and `ring_buffer` can snapshot local state
on a clean close. This is best-effort and works only when the **queue backend's
own connection manager** also implements `StorageBackend`; configuring a
separate item-storage backend does not redirect queue snapshots. Kafka,
RabbitMQ, RocketMQ, Pulsar, and SQS therefore cannot persist strategy snapshots.

For multiple workers running the same spider, set a stable unique identity:

```python
SCRAPY_QUEUE_WORKER_ID = "worker-a"
# Optional explicit override; otherwise WORKER_ID is used.
SCRAPY_QUEUE_SNAPSHOT_OWNER = "worker-a"
```

An owner selects a length-prefixed v2 storage key and prevents workers from
overwriting one another. Without an owner, the legacy spider+queue key remains
for single-worker compatibility. A successful restore consumes and deletes the
snapshot; the next clean close writes current state again. Hard crashes can
still lose changes since the last close.

## Switch storage strategy

Select a `StorageStrategy` via `SCRAPY_STORAGE_STRATEGY` — no code change required.

| Strategy | When to use | Durability boundary |
|---|---|---|
| `passthrough` (default) | Item loss is unacceptable; backend round trips are acceptable. | Each item is written directly to the selected `StorageBackend`. |
| `batched` | Higher throughput is more important than immediate persistence. | Backend-bound items sit in a global FIFO until threshold / spider close. Each item drains through the exact backend supplied with it; hard crash before flush loses the buffer, while store exceptions re-enqueue the backend-bound unwritten tail. |

```python
# settings.py
SCRAPY_STORAGE_STRATEGY = "batched"
```

Use `passthrough` for the strongest persistence semantics. Use `batched` only with idempotent downstream consumers and an explicit crash-before-flush tolerance.

## Redis namespace rollout

Set `SCRAPY_REDIS_NAMESPACE` to a stable value unique to the application and
environment. The default is `scrapy-extension`, which separates queue/set/
storage domains but does not distinguish two unrelated deployments sharing the
same Redis database.

Current keys do not fall back to the legacy raw layout. During an upgrade:

1. Stop old writers and take a backup.
2. Prefer draining the legacy request queue with the old version.
3. Explicitly migrate retained set/storage keys into the selected namespace,
   using the physical mapping in the [migration guide](migration-guide.md#redis-physical-key-layout).
4. Start all new workers with the same namespace. Do not run old and new writers
   against one logical backlog: they address different physical keys.

`clear_storage(None)` scans only the selected namespace's storage domain and
never flushes the Redis database.

### Redis reconnect and shutdown boundary

A Redis backend publishes one fully health-checked client generation. Repeated
`connect()` calls are no-ops while that generation remains published; they do
not recheck health. After `ping()` fails, or before applying changed
connection-used endpoint, credential, TLS, mode, or namespace settings, run an
explicit `disconnect()` / `connect()` sequence. Bundled operations admitted
before teardown keep the old client and namespace until they finish. New
admission cannot splice into that generation, and a timed queue pop wakes with
`QueueError` rather than polling a replacement. Once teardown completes, a
brand-new operation may use the established lazy reconnect behavior.

Budget shutdown for the longest admitted Redis RPC and for a complete
`clear_storage()` SCAN/DELETE sequence. Quiesce writers before a maintenance
clear: SCAN is not a transactional snapshot, and a failure after earlier
deletes is explicitly reported as possibly partial. Repair connectivity and
rerun the clear while writers remain stopped. Do not invoke `disconnect()`
re-entrantly from code executing inside a backend operation; the backend fails
that call fast. Sentinel teardown closes both the discovered master and the
discovery clients. The public raw `client` property is not leased after it
returns; never retain it across reconnect or use it for maintenance sequences
that must stay on one generation.

Redis data clients never automatically replay a command after an
outcome-ambiguous connection, write, or response failure. Treat the resulting
typed backend error as outcome-ambiguous: the server may have committed the
command. In particular, blindly repeating push can enqueue twice, while a pop
response loss can hide one already-consumed item; zero replay only prevents
consuming a second item inside the same SDK call. Server-confirmed
non-execution paths such as NOSCRIPT and Cluster MOVED/ASK/TRYAGAIN may still
continue within the SDK. ClusterDown and SlotNotCovered fail fast because
redis-py couples them to the same outer count as transport retries. An
uncovered slot found during initial routing may not refresh on another ordinary
call; explicitly `disconnect()` / `connect()` to construct a fresh Cluster
topology. Reconcile through application identities or domain state before
retrying mutations. Remove the
deprecated `SCRAPY_REDIS_RETRY_ON_TIMEOUT` input; both values are retained only
for config compatibility and have no data-plane effect. The similarly named
Sentinel setting is separate and permits at most one immediate retry per
read-only control request after a timeout. This Retry policy does not retry
authentication failures, although Sentinel may continue to another configured
endpoint; it does not replace manager-level connection attempts.

Redis mode diagnostics:

- Cluster runs only database zero. If `ConfigurationError.setting_name ==
  "db"`, set `SCRAPY_REDIS_DB=0` and use a unique namespace or separate
  Cluster for isolation; the old backend already discarded non-zero DB values.
- `master_slave` is a deprecated primary-only alias. Remove `REPLICAS` and
  `READ_FROM_REPLICAS`, then select `standalone` for the same behavior or
  Sentinel for discovery/failover.
- `cluster_max_redirects` is a 0–100 per-command protocol continuation budget,
  separate from transport retry. Repeated MOVED/ASK/TRYAGAIN exhausting it
  surfaces a typed backend error; do not increase it to mask a persistently
  inconsistent topology.
- Redis addresses are bare hosts plus separate ports, or list entries in
  `host:port` / `[IPv6]:port` form. Split URI userinfo into the dedicated
  username/password fields. Configuration errors intentionally omit the raw
  endpoint.
- `masters` is a rejected historical tombstone. Replace it with
  `cluster_startup_nodes` and select Cluster mode. Topology nodes and
  non-default controls for a different selected mode are rejected rather than
  ignored.
- A CA/certificate/key with TLS disabled is an intent conflict. Enable TLS or
  remove the material; do not rely on automatic protocol selection.
- Sentinel control and data credentials do not fall back to one another. With
  S Sentinel addresses, capacity for `max_connections=N` is up to S control
  pools plus one data pool, each capped at N. An unset limit is normalized to
  the effectively-unbounded value `2**31` on every pool.

## Storage TTL semantics (expired = absent)

All five storage-capable backends (Redis, MongoDB, ElasticSearch, Memcached,
DynamoDB) uniformly enforce **an expired key is absent** — `retrieve` and
`exists` never surface a key whose TTL has elapsed. The mechanism differs by
backend family; the operator-visible contract does not.

| Backend family | TTL mechanism | `ttl()` return |
|---|---|---|
| Redis | Native server-side expiry (`SET ... EX`). | Non-negative seconds remaining, or `None` for a missing, permanent, or expired key. Redis `-1`/`-2` sentinels are normalized away. |
| Memcached | Native server-side expiry (`client.set(..., expire=)`). | Always `None`; Memcached does not expose remaining TTL, even for a live expiring key. |
| MongoDB | Native TTL index on the `expireAt` field plus read-time expiry enforcement. | Non-negative seconds remaining, or `None` for missing, permanent, or expired. |
| ElasticSearch, DynamoDB | App-level expiry field. `retrieve` / `exists` / `ttl` lazily reap an expired document and report it absent. | Non-negative seconds remaining, or `None` for missing, permanent, or expired. |

This closes a stale-data gap where a read could surface an already-expired
document before the (infrequent) server reaper reached it. No setting change;
the expired-is-absent contract is now uniform across all five storage backends.

The direct `StorageBackend.store(..., ttl=...)` contract accepts `None`
(permanent) or a positive integer number of seconds. Zero, negatives, floats,
and booleans raise `ValueError` so all backends behave consistently. The Scrapy
pipeline keeps its setting-level compatibility rule:
`SCRAPY_PIPELINE_TTL = 0` is normalized to `None` before storage.

Memcached clients run with `default_noreply=False`; a successful mutation has
parsed the server's response rather than merely written a command to the socket.
An exception is still an ambiguous transport outcome, so retry callers should
use idempotent values/keys. The ordinary pymemcache client has one protocol
socket, so this backend serializes storage calls, health probes, and disconnect;
expect one in-flight operation per connected backend generation.

## Safe clearing

- Redis clear operations scan only `<namespace>:storage:*`; they never issue
  `FLUSHDB` or `FLUSHALL`.
- MongoDB, ElasticSearch, and DynamoDB honor a validated logical prefix.
- Memcached cannot enumerate or prefix-delete keys. Any non-`None` prefix raises
  `NotImplementedError`; `clear_storage(None)` also raises unless
  `SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=True`. Enabling it issues server-wide
  `flush_all`, so use it only on a dedicated instance. Permission comes from
  the immutable connected-generation snapshot, and the server must reply with
  success; changing the settings object after connect cannot authorize it.
- Kafka queue clear is deliberately unsupported. Do not automate topic
  delete/recreate through the crawler: stop all producers/consumers, use Kafka
  operator tooling, wait for metadata convergence, and explicitly reset or
  replace the consumer group before resuming.
- RabbitMQ queue purge does not include unacknowledged deliveries. The backend
  rejects `clear_queue()` while the target queue has local in-flight work; ack
  or nack every token first. If a worker is being reset, disconnect so RabbitMQ
  requeues its deliveries, reconnect, and only then retry clear. In-flight work
  on another logical queue does not block the target.
- SQS queue clear is synchronous at the library boundary but takes at least 60
  seconds after the PurgeQueue RPC. Budget that interval in maintenance and
  shutdown deadlines. Same-queue traffic waits; unrelated SQS queues continue.

## Ack and durability matrix

| Surface | Ack / state boundary | Crash behavior | Operator action |
|---|---|---|---|
| Redis / MongoDB / ElasticSearch queue pop | Atomic pop removes the item from the backend queue. Redis never automatically replays a transport-failed pop. | A worker crash after pop can lose the request unless the spider re-enqueues or the backend implementation provides its own recovery. A lost Redis response may hide one already-consumed item even though the SDK does not consume a second. | Use idempotent callbacks and durable item storage for critical crawls. Reconcile an ambiguous Redis failure before issuing another pop. |
| Kafka / RabbitMQ / Pulsar queue pop | `pop_with_ack()` returns a per-message token; scheduler acks on Scrapy `response_received`. Kafka binds tokens to generation/assignment/attempt; Pulsar permits one successful terminal action and keeps client failures retryable. | Crash before ack redelivers. Kafka nacks/rebalances retire the old attempt so its late completion cannot commit the replacement. Crash after response but before callback/pipeline completion can lose downstream work. | Treat ack as downloader-level, not end-to-end completion. Retry the same Pulsar token after a reported client failure. RabbitMQ push waits for a publisher confirm and raises on unroutable/nacked delivery. |
| SQS queue pop | Receipt-handle token plus `SCRAPY_SQS_VISIBILITY_TIMEOUT` (default 300s). | The message can be delivered again if the pop-to-response interval exceeds the visibility lease. | No automatic renewal. Size the lease above worst-case download time. Explicit nack sets visibility to 0 for immediate redelivery. |
| RocketMQ queue pop | Single-outcome message token plus `SCRAPY_ROCKETMQ_INVISIBLE_DURATION` (default 300s). | The message can be delivered again if the pop-to-response interval exceeds the invisibility lease. | No automatic renewal. Token-aware pop never fills the legacy ack slot; ack/nack on one token serialize and a failure remains locally retryable. Explicit nack shortens the lease to RocketMQ's 10-second minimum. Pair set/storage through per-component backends. |
| Backend/plugin declaring `supports_concurrent_ack=False` | Single ack slot only. | `CONCURRENT_REQUESTS > 1` raises at startup unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=True` is set. | Keep concurrency at 1, or pick a backend with a real in-flight ack set. |
| Stateful queue strategies | In-process scheduling/fairness/rate state. | Hard crash can lose held strategy state; a token-bearing replacement is rejected before it enters volatile delay/time-wheel/round-robin/ring-buffer state. | Use a backend-durable push path when replacing an unacked broker delivery; zero effective delay remains a direct backend push. |
| `batched` storage | Backend-bound in-process write buffer. | Hard crash before flush loses buffered items; an ordinary partial failure retries the failing item and tail against their original backends. | Keep caller-owned backends alive through drain and coordinate one shared strategy lifecycle; prefer `passthrough` when persistence must happen before item acknowledgement. |

All five bundled deferred-ack backends use per-message tokens and support
`CONCURRENT_REQUESTS > 1`. The unsafe-concurrency gate remains for third-party
plugins that explicitly declare `supports_concurrent_ack=False`.

## Multi-backend coexistence

Bind each component (queue / dedup / storage) to a *different* backend via
per-component type keys. Unset keys fall back to the global `SCRAPY_BACKEND_TYPE`.

```python
# Queue in Redis-Cluster, dedup + data in MongoDB
SCRAPY_QUEUE_BACKEND_TYPE = "redis"
SCRAPY_QUEUE_BACKEND_SETTINGS = {
    "mode": "cluster",
    "cluster_startup_nodes": ["redis-1:6379", "redis-2:6379"],
    "namespace": "crawler-prod",
}

SCRAPY_SET_BACKEND_TYPE = "mongodb"
SCRAPY_SET_BACKEND_SETTINGS = {"uri": "mongodb://mongo:27017", "database": "scrapy"}

SCRAPY_STORAGE_BACKEND_TYPE = "mongodb"
SCRAPY_STORAGE_BACKEND_SETTINGS = {"uri": "mongodb://mongo:27017", "database": "scrapy"}
```

Each backend must implement the interface its component needs (see
[capabilities matrix](../README.md#backend-capabilities)). The
`ConnectionManager` registry keys one pooled connection per
`backend_type:settings_hash`, so co-located backends (set + storage both
MongoDB, same URI) share a single connection.

## Diagnose a `ConfigurationError` at startup

The project uses two validation exception families:

- `ConfigurationError` for backend capability checks, unknown/typoed flat or
  nested names, and project cross-field security rules. It carries
  `setting_name` and `setting_value` context attributes; sensitive setting
  names redact the value.
- `pydantic.ValidationError` for model field types, bounds, enum values, and
  direct `extra="forbid"` failures.

Do not catch only one family when building an operator-facing config checker.
Validation happens during component factory/connection startup before normal
crawl traffic.

For bundled backends, effective value precedence is explicit nested
`*_BACKEND_SETTINGS`, flat Scrapy setting, OS environment variable, then model
default. Scrapy project values therefore cannot be overridden by a same-named
environment variable. Unknown settings under the selected backend prefix fail
fast with a nearest-name suggestion.

Common causes (round-9 SV1–SV5 close all of these):

| Symptom | Likely cause | Where to look |
|---|---|---|
| "is not a valid Mode" / "valid values: …" | Mode field typo (SV1) — must be a member of the `Literal` enum | `settings/{kafka,pulsar,rabbitmq,mongodb}.py` mode fields |
| "X is required for mode Y" | Mode-specific required field missing (SV2) | `settings/{mongodb,redis,kafka,rabbitmq}.py` `model_validator`s |
| "SASL username without password" / "TLS cert without key" | Cross-field auth incoherence (SV3) | `settings/{kafka,pulsar,redis,mongodb,elasticsearch,sqs,dynamodb}.py` |
| "must start with a valid scheme" / "missing scheme" | Malformed host URL (SV4) | `settings/{mongodb,pulsar,rocketmq,elasticsearch,sqs,dynamodb}.py` |
| "must be >= 0" / "must be a positive integer" | Unbounded-int / empty-string gap (SV5) | `settings/{memcached,redis,rabbitmq,base}.py` |
| "Redis setting … contains an invalid address" | Redis URI/userinfo, malformed DNS/IP, unbracketed list IPv6, or invalid port | Use a bare scalar host or `host:port` / `[IPv6]:port` list entry; keep credentials separate |
| "Redis setting 'masters' is unsupported" | Historical Cluster seed input was supplied | Replace it with `cluster_startup_nodes` and select Cluster mode |
| "Redis setting … requires mode=…" | A non-default Sentinel/Cluster control belongs to another selected topology | Remove the unused setting or select the matching mode |
| "Redis Cluster supports only database 0" | Cluster was configured with a non-zero DB that older code silently dropped | Set DB0 and isolate with `SCRAPY_REDIS_NAMESPACE` or another Cluster |
| "Redis replica routing is unsupported" | Deprecated master-slave replica-read input is non-empty/true | Remove the replica fields; use standalone or Sentinel |
| "Redis TLS certificate settings require ssl_enabled=True" | Certificate intent would otherwise open plaintext | Enable Redis TLS explicitly or remove all TLS material |
| "tls_allow_invalid_certificates=True disables certificate verification" | Insecure TLS in production mode (SEC-2) | `settings/mongodb.py` |
| "credentials over cleartext http://" | ES cloud creds over http (SEC-3) | `settings/elasticsearch.py` |
| "Authenticated Pulsar connections require … verification" | Token auth attempted with plaintext or a TLS verification escape hatch | `settings/pulsar.py`; fix the broker CA/hostname and keep both validation flags secure |
| "Remote Memcached uses an unauthenticated plaintext protocol" | A non-loopback Memcached host lacks an explicit trusted-network decision | Prefer a TLS-capable storage backend, or set `SCRAPY_MEMCACHED_ALLOW_REMOTE_PLAINTEXT=True` only behind an isolated private firewall |
| "MongoDB mutations require an acknowledged write concern" | `SCRAPY_MONGO_W` is zero, negative, boolean, or unsupported text | Use a positive integer or `majority`; prefer `majority` for replicated durability |
| "MongoDB ... capability domains must use distinct physical collection names" | Two of the queue, set, and storage collection settings resolve to the same MongoDB collection | Stop writers, split or migrate the mixed documents into three collections, then configure distinct names |
| "endpoint_url must be http:// or https://" | LocalStack/AWS endpoint scheme (SEC-4) | `settings/{sqs,dynamodb}.py` |
| "aws_access_key_id and aws_secret_access_key must both be set" | Half-configured AWS creds | `settings/{sqs,dynamodb}.py` (config-time), `backends/connectors.py` (connect-time SEC-7) |

For Kafka, diagnose SASL failures by mechanism rather than assuming every mode
uses a username/password pair. PLAIN and SCRAM require both non-empty fields;
GSSAPI obtains credentials from the process Kerberos context. OAUTHBEARER is
not configurable through this backend. Confluent mode requires its dedicated
non-empty API key and secret. Validation errors name fields but never include
credential values.

Kafka push success requires `acks=1` or `acks="all"`; `acks=0` is rejected
because no broker receipt exists. The backend passes `retention.ms` and
`min.insync.replicas` only when it creates a topic. A TopicAlreadyExists result
does not authorize the crawler to alter broker policy: it verifies partition
count, replication factor, retention, and minimum ISR, then fails on drift.
Reconcile that drift out of band. Keep
`num_partitions == max_priority_partitions` because priority is the physical
partition index.

Kafka depth is consumer-group lag (`end - committed`), not local fetch lag.
Fetched but unacknowledged records still count. A new group with `earliest`
must see pre-existing backlog; `latest` intentionally starts at the end; `none`
without a committed offset is an operator error and raises. Never translate a
depth exception into zero, because the scheduler uses zero as an idle signal.

MongoDB push, set, and storage mutation success requires an acknowledged write
concern. The backend supports positive integer `w` values and `"majority"`;
`w=0` is never a valid throughput shortcut. A write-concern timeout limits how
long acknowledgement may wait but does not turn a timeout into success. Treat
the resulting exception as outcome-ambiguous and retry only idempotent work.

Keep MongoDB queue, set, and storage documents in three distinct collections.
`clear_storage(None)` intentionally deletes every non-marker document in the
storage collection; sharing that collection with another capability would make
an ordinary storage clear destructive across domains. The same separation
avoids installing incompatible unique indexes on mixed document schemas. A
reserved `_id="scrapy-extension:capability-domain:v1"` document records the
role durably and is preserved by storage clear, so separately configured
components or processes fail closed if they attempt a cross-domain reuse.
Replica-set, sharded, and Atlas claims force primary/majority reads and
majority writes for this marker even when business writes use `w=1`. The
ownership value sits below an array boundary so a valid shard key either routes
all contenders together or rejects the marker as multikey; a scatter read also
rejects any historical duplicate marker state.
When repairing a legacy mixed collection, do not rename or reuse it as one of
the three destinations: its queue, set-uniqueness, storage-key, and TTL indexes
remain attached. Back it up, create three empty collections, let the backend
create each domain's indexes, import only that domain's documents, and verify
the resulting indexes before opening writers.

If the error is in a backend's `from_settings` / `from_crawler` factory,
check `backends/connectors.py:resolve_backend_config` — it resolves
per-component config and is the single chokepoint for the fallback chain.

### Connection retry controls

| Setting | Default | Contract |
|---|---:|---|
| `SCRAPY_RETRY_ATTEMPTS` | `3` | Retries **after** the initial attempt; range 0..20. Default therefore permits at most four total attempts. |
| `SCRAPY_RETRY_DELAY` | `1.0` | Base seconds for full-jitter exponential backoff: retry `n` sleeps uniformly between 0 and `base * 2**n`. Must be finite and non-negative. |

These are ConnectionManager-level controls. Backend-native retry settings are
separate. In particular, `SCRAPY_RABBITMQ_CONNECTION_ATTEMPTS` and
`SCRAPY_RABBITMQ_RETRY_DELAY` configure pika's inner connection policy; they do
not replace the generic manager settings above. Redis's deprecated
`SCRAPY_REDIS_RETRY_ON_TIMEOUT` setting does not control manager retries and no
longer enables data-command replay; do not substitute one setting for the
other.

SQS connection teardown is a drain boundary. As soon as `disconnect()` wins
admission, new queue operations fail with `QueueError`; work already admitted
keeps one client generation through QueueUrl resolution and its final SDK call.
Disconnect waits for that work before closing the client, so the shutdown
budget must cover an SQS long poll, SDK retries, and any active 60-second purge
barrier. A live `connect()` is idempotent. Apply endpoint, region, prefix, or
visibility changes with an explicit `disconnect()` / `connect()` sequence;
receipt tokens from the retired generation are locally stale and must never be
replayed through the replacement client.

SQS and DynamoDB region names accept lowercase ASCII, hyphen-delimited,
multi-label forms used across AWS partitions and ending in a numeric label (for
example `us-gov-west-1` and `eusc-de-east-1`). A `ConfigurationError` naming
`region_name` indicates a structural problem such as casing, an empty label, an
underscore, or a non-ASCII digit. A structurally valid but unknown region is
intentionally left for the SDK/service endpoint to diagnose.

RabbitMQ publishes a connection and prepared channel as one generation. A live
`connect()` is intentionally idempotent; roll queue-policy or endpoint changes
with `disconnect()` / `connect()`. Disconnect detaches the current generation
and advances its lifecycle fence before closing handles, so it may return while
a slow private candidate is still unwinding; that candidate will close locally
and cannot resurrect the backend. Replacing an already unhealthy published
session closes the old channel and connection before the new candidate is
created, allowing RabbitMQ to recover unacknowledged deliveries. A concurrent
connect also waits for disconnect to finish retiring published handles before
it constructs the successor. Treat a `QueueError` from a timed pop during
reconnect as an interrupted operation and retry through the caller's normal
queue loop.

DynamoDB gives each candidate a private boto3 Session and publishes the prepared
Session, Resource, and data-plane-usable Table as one generation. A live
`connect()` is idempotent; apply endpoint, region, table, or credential-setting
changes with `disconnect()` / `connect()`. Storage calls, health probes, and the
entire paginated clear are serialized because boto3 Resources are not
thread-safe. Disconnect waits for an admitted call or table creation before
closing the botocore client, so the shutdown budget must cover SDK retries and a
full clear. This linearizes clear against writes made through the same backend
instance only: DynamoDB Scan does not provide snapshot isolation across other
processes or clients, so clear remains a best-effort maintenance operation under
external concurrent writes.

Custom DynamoDB endpoints must be set through the backend's validated endpoint
setting. Botocore environment variables (`AWS_ENDPOINT_URL` and
`AWS_ENDPOINT_URL_DYNAMODB`) and shared-config custom endpoints are ignored;
cloud mode therefore cannot be silently redirected to an ambient HTTP target.

For a deterministic DynamoDB maintenance clear, first quiesce every external
writer, then call `clear_storage()`, and resume writers only after it succeeds.
The backend sends at most 25 deletes per request and gives each physical batch
eight application-level BatchWriteItem submissions. Seven full-jitter sleeps
have a theoretical local maximum of 6.35 seconds per batch, but this is not a
whole-clear, wire-attempt, or disconnect deadline:
page/batch count is unbounded, botocore has its own retry and network-timeout
budget, and the operation lock has no fairness guarantee. The lock intentionally
covers Scan, BatchWrite, and backoff so local writes cannot interleave between
partial retries. A typed `StorageError` means the clear may already be partial;
after fixing the cause, rerun it as a new idempotent convergence pass rather
than attempting rollback. Never call `disconnect()` re-entrantly from a
synchronous logging hook or signal handler; schedule teardown on another
thread.

A DynamoDB `StorageError` whose public message says the `DeleteItem` response
was malformed means the deletion result is uncertain: do not reinterpret it as
"item absent" or retry a non-idempotent surrounding workflow blindly. Verify
that any LocalStack version, proxy, emulator, or test double returns no
`Attributes` for a missing key and the complete old item (including matching
string `pk`) for an existing key. This malformed-response error copies the
response into neither its message nor its `StorageError` domain fields and has
no chained cause; inspect protected proxy/emulator/service telemetry. A Python
traceback can still retain input values in frame locals, so treat traceback and
error-report attachments as sensitive. A separate SDK-call `StorageError`
keeps the original SDK exception as `__cause__` while omitting its text
publicly.

Every successful `ConnectionManager.get_manager()` acquisition must be paired
with exactly one `close()`. The registry is reference-counted; releasing the
same acquisition twice can prematurely retire a manager still expected by a
different holder.

## The depth-sampling knob (round-9 U4)

By default, `BackendQueue` probes real backend queue depth at most once per
100 pops (`depth_sample_every=100`), keeping the depth signal fresh for
backpressure gates while reclaiming ~25% of the pop-path RTT budget.

- **Tune:** set `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY=<N>` (default `100`).
  Lower for faster backpressure response (more depth RPCs); raise for lower
  pop-path overhead and a staler non-zero sample.
  `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY=1` restores per-pop behavior.
  (Round-14 R14-C: this setting was deferred in round-9 — the constructor
  default was the only path; the setting now exists and is threaded by
  `BackendScheduler.from_settings` → `BackendQueue(depth_sample_every=…)`.)
- **Inspect:** the probe counter and cached depth are private (`_depth_probe_counter`,
  `_cached_depth`); the externally visible signal is `monitor.on_queue_depth`
  emitting from the cached sample.
- **Backpressure interaction:** `backpressure_pause_at` /
  `backpressure_resume_at` compare against the sampled depth; sampling
  keeps the comparison within ~1% variance of the real depth at default
  config.

This knob cannot create a depth signal where the client has no backlog API.
Pulsar and RocketMQ `queue_len()` raise `NotImplementedError`; the scheduler
then skips depth-based backpressure for that poll, assumes pending work for idle
detection, and continues popping. Monitor those backends through pop rate and
broker-native tooling rather than `queue/depth`.

## The per-item byte cap (round-9 D2)

`SCRAPY_QUEUE_MAX_ITEM_BYTES=<bytes>` (default `1048576` — 1 MiB, matches
the Memcached 1 MB ceiling). Requests exceeding this are rejected with
`SerializationError` at push time, preventing silent drops by capped storage
backends. (Round-14 R14-C: this setting was deferred in round-9 — the
constructor default was the only path; the setting now exists and is
threaded by `BackendScheduler.from_settings` → `BackendQueue(max_item_bytes=…)`.)

## The memory-cap knobs (round-9 U5)

Two unbounded-growth paths are now capped by default:

| Surface | Default cap | Setting | Behavior on overflow |
|---|---|---|---|
| `MemoryMembershipFilter` | 1,000,000 entries | `SCRAPY_DEDUP_MEMORY_MAXSIZE` | LRU eviction (oldest entry dropped); warn-once at threshold |
| `DelayQueueStrategy` holding heap | 100,000 items | `SCRAPY_QUEUE_DELAY_MAX_HELD` (soft cap; round-14 R14-C) | Warn-once; non-positive disables |

**Advanced opt-out:** pass `maxsize=None` to `MemoryMembershipFilter` (or set
`SCRAPY_DEDUP_MEMORY_MAXSIZE=None`) for unbounded growth — only do this if
you have an external memory budget; the unbounded default was the round-9
finding (~366 MB @ 1M entries, ~3.58 GB @ 10M).

## The operability-monitor knobs (round-12 U2)

`ScrapyStatsMonitor` surfaces two operability gauges whose thresholds are
now operator-tunable (round-14 R14-C — round-12 shipped the gauges with
constructor defaults only; the settings now exist):

| Setting | Default | Surface |
|---|---|---|
| `SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD` | `1000` | Depth above which `queue/backpressure` flips on |
| `SCRAPY_MONITOR_POP_RATE_WINDOW_S` | `60.0` | Trailing window (seconds) for the `queue/pop_rate` gauge |

Both are threaded by `BackendScheduler.from_settings` → the resolved
`ScrapyStatsMonitor` (and `pop_rate_window_s` is also forwarded to
`BackendQueue`, which computes the rolling rate).

## Diagnose a stuck crawl (page on zero pop rate)

Before round-10's operability signals (U2 — `on_pop_rate` /
`on_filter_saturation`, not yet landed), a stuck crawl must be diagnosed
from the existing monitor stats:

| Stat | What it tells you |
|---|---|
| `queue/depth` (sampled, U4) | Depth of a backend that supports `queue_len`. `0` = empty only when a sample was available; Pulsar/RocketMQ do not emit a broker depth. |
| `dupefilter/filtered` | Count of duplicates filtered. High + rising = dedup saturated. |
| `dupefilter/filter_full` | Cuckoo filter hit capacity (each occurrence increments). Non-zero = Cuckoo at capacity, degrading to passthrough. |
| `pipeline/storage_skipped` | Items skipped because backend has no `StorageBackend`. Non-zero on a storage-expected backend = misconfigured `SCRAPY_STORAGE_BACKEND_TYPE`. |
| `scheduler/ack_error` | Ack commit to the queue backend failed (`QueueError` on `ack`). Non-zero on a deferred-ack backend means the broker is rejecting commits; native offset/unacked/visibility semantics may redeliver the message. |
| `scheduler/nack_error` | Nack to the queue backend failed (`QueueError` on `nack`). The broker's native retry or lease-expiry behavior, if any, determines redelivery. |
| `scheduler/queue/poison_dropped` | A deterministic-invalid serialized request was terminally acked/dropped so it cannot pin a Kafka partition or hot-loop in a broker queue. |
| `scheduler/queue/empty_payload_dropped` | A broker record with a real ack token but no request payload (for example a Kafka tombstone) was terminally consumed. |
| `scheduler/queue/replacement_poison_dropped` | A retry/redirect replacement was locally invalid, so its original broker delivery was terminally consumed. |
| `scheduler/queue/volatile_replacement_rejected` | A replacement still owned an unacked broker source but its selected strategy would retain it only in process. The push was rejected before local mutation; use a backend-durable strategy/path. |

Differential diagnosis:

- **Queue empty + zero pop rate** → backend drained; produce more requests
  (or your scheduler is paused via backpressure — check
  `backpressure_pause_at`).
- **Queue non-empty + zero pop rate** → backend-down (check `backend.ping()`
  in your logs), throttle-strategy pinned, or `CONCURRENT_REQUESTS=0`.
- **Queue non-empty + pop rising + items not landing** → ack path broken
  (Kafka/RabbitMQ under `CONCURRENT_REQUESTS > 1` without
  `supports_concurrent_ack`); check for the `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`
  gate error at startup.
- **`dupefilter/filter_full` rising** → Cuckoo at capacity; raise
  `SCRAPY_DEDUP_CUCKOO_CAPACITY` or switch to `set` for unbounded dedup.
- **`scheduler/ack_error` or `scheduler/nack_error` rising** → the broker is
  rejecting ack/nack commits on a deferred-ack backend. Native broker redelivery
  may keep delivery moving, but duplicates rise —
  check broker connectivity/permissions and watch `dupefilter/filtered` for the
  redelivery side-effect. If the failure follows a committed retry/redirect
  replacement, that replacement remains accepted and dedup-reserved; the source
  token stays unresolved so broker redelivery can reach the duplicate-ack path.

## Poison payload handling

Malformed JSON, an invalid request schema/body codec, and oversize backend
payloads are deterministic for the same bytes. When the backend supplied an ack
token, `BackendQueue` attempts to ack and drop that delivery rather than nack it
into an infinite redelivery loop. The pop still raises `SerializationError`, and
the poison-drop stat increments only when the terminal transition succeeded.
If ack fails, the original deserialization error remains primary and the broker
may redeliver according to its normal policy.

For an upgrade carrying old queued requests, drain with the old version when
possible. Unmarked legacy bodies can be ambiguous: an old raw UTF-8 body that
happens to be valid Base64 cannot be distinguished from an intermediate Base64
wire format. See [queued-request wire format](migration-guide.md#queued-request-wire-format).

## Secret-bearing payloads

The JSON codec prevents code execution; it does not encrypt data. Request meta,
request bodies, and scraped items may contain credentials or personal data, and
supported secret wrapper objects are serialized to their underlying value.
Require TLS in transit, least-privilege queue/database ACLs, and encryption at
rest. Where the backend does not provide adequate at-rest protection, encrypt
the sensitive field in application code before enqueue/store. Avoid credentials
inside plain MongoDB/other DSN strings because caller logging or settings reprs
can expose them.

For RocketMQ, set `SCRAPY_ROCKETMQ_TLS_ENABLED=True` whenever access/secret
credentials are configured; cloud mode requires that combination. Anonymous
standalone/cluster plaintext remains available only for explicitly trusted
local deployments. Both producer and consumer use the same captured TLS,
endpoint, credential, timeout, and consumer-group snapshot for each connection
attempt.

For RabbitMQ, keep `amqp://` strictly loopback-only. Remote primary or cluster
nodes require verified TLS, and credentials must be configured outside the URL.
Confirm every node certificate covers the exact configured hostname: the
backend passes that hostname to Pika for SNI and matching. `CERT_NONE`,
`CERT_OPTIONAL`, partial client-certificate pairs, remote `guest`, and
`amqps://`-to-plaintext overrides fail before connection I/O.

## Cutting a release

High-level release flow (a dedicated `docs/release-runbook.md` does not yet
exist; this is the canonical procedure until it lands):

1. **Bump version:** `uv version <bump>` (or edit `pyproject.toml`
   `[project] version`).
2. **Sync lockfile:** `uv lock` (verify `uv lock --check` passes).
3. **Update CHANGELOG:** move the [`Unreleased`](../CHANGELOG.md) entries
   into a new `## [X.Y.Z] — YYYY-MM-DD` section.
4. **Tag:** `git tag vX.Y.Z` and push the tag.
5. **Publish:** `uv build && uv publish`.
6. **Inspect artifacts:** list the wheel and sdist. Confirm `py.typed`, public
   docs, and examples are present; confirm `.omc`, local `*.db`, ignored scratch
   plans, credentials, and editor state are absent. Render the wheel `METADATA`
   description and verify every non-anchor README link is an absolute web URL.
7. **Verify gates:** `uv run ruff check`, `uv run pytest -m "not integration"`, and `uv run pytest --cov=scrapy_extension --cov-report=term-missing` (fails below 95%). For live backends, set the relevant `SCRAPY_TEST_*` variables and pass `--force-enable-socket`.
8. **Verify install:** in a fresh venv, `pip install scrapy-extension==X.Y.Z` and
   import the package + one backend; confirm `__version__` matches.

For the stability commitment each release makes, see
[`STABILITY.md`](../STABILITY.md).
