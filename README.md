# Scrapy Extension

Distributed crawling for Scrapy with pluggable backends (**Redis**, **MongoDB**, **Kafka**, **RabbitMQ**, **ElasticSearch**, **RocketMQ**, **Pulsar**, **SQS**, **Memcached**, **DynamoDB**) and pluggable strategy layers for dedup and queue semantics.

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/azwpayne/scrapy-extension/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://pypi.org/project/scrapy-extension/)

## Contents

- [Features](#features) · [Installation](#installation) · [Quick Start](#quick-start)
- [Backend Configuration](#backend-configuration) · [Backend Capabilities](#backend-capabilities)
- [Guarantees](#guarantees) · [Multi-Backend Coexistence](#multi-backend-coexistence)
- [Pluggable Strategy Layers](#pluggable-strategy-layers) — incl. the [Ack & durability matrix](#ack-and-durability-matrix)
- [Architecture](#architecture) · [Scrapy Components](#scrapy-components) · [Exceptions](#exceptions)
- [Examples](#examples) · [Security](#security) · [Testing](#testing) · [License](#license)

> **Deeper docs:** [operations](https://github.com/azwpayne/scrapy-extension/blob/main/docs/runbook.md) · [upgrade and backlog migration](https://github.com/azwpayne/scrapy-extension/blob/main/docs/migration-guide.md) · [plugin authors](https://github.com/azwpayne/scrapy-extension/blob/main/docs/backend-plugins.md) · [API/maturity](https://github.com/azwpayne/scrapy-extension/blob/main/STABILITY.md) · [runnable examples](https://github.com/azwpayne/scrapy-extension/tree/main/examples)

## Features

- **10 Backends**: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ, Pulsar, SQS, Memcached, DynamoDB
- **Multi-Mode**: Standalone, cluster, cloud deployments per backend
- **Pluggable Dedup**: Set / Memory / **Bloom** / **Cuckoo** filters via `SCRAPY_DEDUP_STRATEGY`
- **Pluggable Queue Semantics**: 8 strategies — Passthrough / **Delay** / **RoundRobin** / **Throttle** / **Priority** / **TimeWheel** / **WorkStealing** / **RingBuffer** via `SCRAPY_QUEUE_STRATEGY`
- **Multi-Backend Coexistence**: bind queue / dedup / storage to *different* backends via `SCRAPY_{QUEUE,SET,STORAGE}_BACKEND_TYPE` (e.g. queue in Redis, dedup + data in MongoDB)
- **Distributed Queue**: Priority-based request queue across spiders
- **Duplicate Filtering**: Cross-instance URL deduplication (default: `SetBackend`)
- **Item Storage**: Key-value storage with TTL support via `StorageBackend`
- **Type Safe**: Full type annotations, `py.typed` marker
- **Secure**: Input validation on key names, topic names, and queue identifiers

## Installation

```bash
pip install scrapy-extension                  # Core (no backend deps required)
pip install scrapy-extension[redis]           # Redis backend
pip install scrapy-extension[mongodb]         # MongoDB backend (pymongo)
pip install scrapy-extension[kafka]           # Kafka backend (kafka-python-ng)
pip install scrapy-extension[rabbitmq]        # RabbitMQ backend (pika)
pip install scrapy-extension[elasticsearch]   # ElasticSearch backend
pip install scrapy-extension[rocketmq]        # RocketMQ backend
pip install scrapy-extension[pulsar]          # Pulsar backend (pulsar-client)
pip install scrapy-extension[sqs]             # Amazon SQS backend (boto3)
pip install scrapy-extension[memcached]       # Memcached backend (pymemcache)
pip install scrapy-extension[dynamodb]        # DynamoDB backend (boto3)
pip install scrapy-extension[all]             # All backends
```

Backends are loaded lazily via PEP 562 — the core package works without any backend deps installed. Backend-specific dependencies are only loaded when a backend class is first accessed.

## Quick Start

```python
import scrapy


class MySpider(scrapy.Spider):
    name = "example"
    start_urls = ["https://example.com/"]

    def parse(self, response):
        yield {"url": response.url}
```

`settings.py`:

```python
SCHEDULER = "scrapy_extension.schedule.scheduler.BackendScheduler"
DUPEFILTER_CLASS = "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"
ITEM_PIPELINES = {"scrapy_extension.pipeline.pipeline.BackendPipeline": 300}

SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_HOST = "localhost"
```

This settings-driven path is the normal Scrapy integration: the scheduler,
dupefilter, and pipeline own their backend lifecycles. `BackendSpiderMixin` is
for direct programmatic access to backend interfaces; see the
[examples guide](https://github.com/azwpayne/scrapy-extension/blob/main/examples/README.md)
for that separate pattern.

## Backend Configuration

Each backend's documented `SCRAPY_...` names work as flat Scrapy settings and as OS environment variables; explicit `*_BACKEND_SETTINGS` dictionaries override flat Scrapy values.

For a selected backend the value precedence is: explicit nested component/global
dictionary, flat Scrapy setting, environment variable, then model default.
Scrapy project settings therefore take precedence over same-named environment
variables. Unknown nested fields and unknown names under the selected backend's
environment prefix fail fast instead of silently falling back to a default.
Those project checks raise `ConfigurationError`; Pydantic type/range/enum
failures raise `ValidationError`.

### Redis (standalone, master_slave, sentinel, cluster)

```python
SCRAPY_BACKEND_TYPE = "redis"
SCRAPY_REDIS_HOST = "localhost"
SCRAPY_REDIS_PORT = 6379
SCRAPY_REDIS_NAMESPACE = "my-crawler"  # unique per application/deployment
```

Redis physical keys are now isolated as `<namespace>:set:*`,
`<namespace>:storage:*`, and hash-tagged `<namespace>:queue:*` keys. There is
deliberately no fallback to the legacy unnamespaced layout because that could
read or delete another application's keys in a shared database. Persistent
deployments must drain or explicitly migrate the old keys before upgrading;
see the [migration guide](https://github.com/azwpayne/scrapy-extension/blob/main/docs/migration-guide.md#redis-physical-key-layout).

In Sentinel mode, `SCRAPY_REDIS_SSL_ENABLED=True` secures both Sentinel
discovery and the discovered master. Configure `SCRAPY_REDIS_SSL_CAFILE`; when
using mTLS, provide `SCRAPY_REDIS_SSL_CERTFILE` and
`SCRAPY_REDIS_SSL_KEYFILE` together. Hostname verification remains enabled by
default. This is one transport policy—there is no silent plaintext Sentinel
control-plane fallback.

### MongoDB (standalone, replica_set, sharded_cluster, atlas)

```python
SCRAPY_BACKEND_TYPE = "mongodb"
SCRAPY_MONGO_URI = "mongodb://localhost:27017"
SCRAPY_MONGO_DATABASE = "scrapy"
```

MongoDB mutations use an acknowledged write concern. Set `SCRAPY_MONGO_W` to
a positive integer or `"majority"` (recommended for replicated durability);
`0`, negative values, booleans, and custom tag strings are rejected before
client I/O. `SCRAPY_MONGO_W_TIMEOUT_MS`, when set, must be non-negative.

### Kafka (standalone, cluster, confluent)

```python
SCRAPY_BACKEND_TYPE = "kafka"
SCRAPY_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
```

Kafka authentication is mechanism-aware and fails before client I/O when it is
incomplete. `PLAIN` and `SCRAM-SHA-*` require a non-empty username/password
pair; `GSSAPI` uses the ambient Kerberos context and must not carry the ignored
PLAIN pair. `OAUTHBEARER` is rejected because this backend does not expose the
token-provider object required by kafka-python. Confluent mode requires a
non-empty API key and secret and always builds a `SASL_SSL` client.

Queue publication accepts only broker-confirmed `acks=1` or `acks="all"`
(default). New topics receive the configured retention and minimum in-sync
replica policy. Because priorities map directly to partitions,
`NUM_PARTITIONS` and `MAX_PRIORITY_PARTITIONS` must match. Existing topics are
not mutated automatically; the backend verifies their partition, replication,
retention, and minimum-ISR policy and refuses a mismatch until it is reconciled
with Kafka operator tooling.

`queue_len()` reports conservative consumer-group lag. It uses committed
offsets rather than the local fetch position, so fetched-but-unacknowledged
records remain pending. For a fresh group, `auto_offset_reset="earliest"`
counts existing backlog, `"latest"` begins at the end, and `"none"` fails
instead of returning a false zero. Consumer metadata calls are serialized with
poll and settlement because KafkaConsumer is not thread-safe.

### RabbitMQ (standalone, cluster, mirrored_queues)

```python
SCRAPY_BACKEND_TYPE = "rabbitmq"
SCRAPY_RABBITMQ_URL = "amqp://localhost:5672/"
SCRAPY_RABBITMQ_USERNAME = "guest"
SCRAPY_RABBITMQ_PASSWORD = "guest"
```

RabbitMQ URLs must not contain userinfo; provide both credential fields so
passwords remain secret-wrapped. Plaintext `amqp://` is accepted only when
every configured node is loopback. Remote standalone and cluster connections
must use verified TLS (`amqps://` or `SCRAPY_RABBITMQ_SSL_ENABLED=True`) with
`CERT_REQUIRED`; Pika receives each node's hostname for SNI and certificate
matching. Optional mTLS requires both `SCRAPY_RABBITMQ_SSL_CERTFILE` and
`SCRAPY_RABBITMQ_SSL_KEYFILE`. Publishes use `mandatory=True` and synchronous
publisher confirms; an unroutable or broker-nacked publish raises `QueueError`.
One fully prepared connection/channel pair is published as a generation. A
live `connect()` is idempotent; use an explicit `disconnect()` / `connect()` to
apply changed queue policy or connection settings. Disconnect immediately
fences private candidates and stale acknowledgement tokens, and a timed pop
fails rather than crossing onto a replacement channel.

### ElasticSearch (standalone, cloud)

```python
SCRAPY_BACKEND_TYPE = "elasticsearch"
SCRAPY_ELASTICSEARCH_HOSTS = ["http://localhost:9200"]
```

### RocketMQ (standalone, cluster, cloud)

```python
SCRAPY_BACKEND_TYPE = "rocketmq"
# Point at the broker's gRPC PROXY (port 8081), not the legacy NameServer.
# The broker must run with --enable-proxy (apache rocketmq-python-client 5.1.1).
SCRAPY_ROCKETMQ_NAMESRV_ADDRESS = "localhost:8081"
SCRAPY_ROCKETMQ_INVISIBLE_DURATION = 300

# Required for cloud mode and for every authenticated connection.
SCRAPY_ROCKETMQ_TLS_ENABLED = True
SCRAPY_ROCKETMQ_ACCESS_KEY = "your-access-key"
SCRAPY_ROCKETMQ_SECRET_KEY = "your-secret-key"
```

RocketMQ accepts an anonymous standalone/cluster connection with TLS either on
or off. Once either credential is configured, both must be non-empty and TLS is
mandatory. Cloud mode always requires the complete credential pair and TLS.
This policy protects both the gRPC message body and the SDK's authentication
metadata; use anonymous plaintext only for an explicitly trusted local broker.

<details><summary><b>Topic creation — required setup</b> (the first push otherwise fails)</summary>

The apache rocketmq 5.x gRPC proxy does **not** auto-create topics via the
`QueryRoute` path by default — a first push to a fresh topic fails with
`failed to fetch topic route`. `broker.conf`'s `autoCreateTopicEnable=true`
covers only the legacy remoting path, not the gRPC path the python client
speaks. Pick **one** of:

**Option A — enable proxy-side auto-create (recommended for dev):**

Mount an `rmq-proxy.json` and point the broker at it via `-pc`:
```json
{
  "rocketMQClusterName": "DefaultCluster",
  "enableAutoTopicCreation": true,
  "topicQueueConfig": { "defaultReadQueueNum": 8, "defaultWriteQueueNum": 8 }
}
```
```bash
sh mqbroker -n namesrv:9876 -c /path/broker.conf --enable-proxy -pc /path/rmq-proxy.json
```

**Option B — pre-create topics (production / locked-down brokers):**
```bash
mqadmin updateTopic -n namesrv:9876 -b broker:10911 -t scrapy-queue_<your-queue>
```

Also set `brokerIP1` in `broker.conf` to an address your client can resolve
(`127.0.0.1` for single-host docker; the broker's real IP for remote clients)
— the proxy returns this to clients, and the default container hostname is
usually unreachable from the host.

</details>

> **At-least-once delivery:** RocketMQ uses a deferred-ack model — `pop`
> returns a message body **without** acking; the scheduler acks via
> `ack(token=msg)` when Scrapy emits `response_received`. A crash before ack → the
> broker's invisible-duration window redelivers (at-least-once, not exactly-once).
> The lease is not auto-renewed: configure `INVISIBLE_DURATION` above the
> maximum expected pop-to-downloader-response time. Explicit nack shortens the
> lease to RocketMQ's 10-second minimum.
> A delivery token has one terminal outcome: concurrent ack/nack calls are
> serialized, token-aware pops cannot be settled through the legacy slot, and
> only a failed broker RPC leaves the token locally retryable.

### Pulsar (standalone, cluster)

```python
SCRAPY_BACKEND_TYPE = "pulsar"
SCRAPY_PULSAR_SERVICE_URL = "pulsar://localhost:6655"
```

Token-authenticated deployments must use fully verified TLS:

```python
SCRAPY_PULSAR_SERVICE_URL = "pulsar+ssl://broker.example:6651"
SCRAPY_PULSAR_AUTH_TOKEN = "..."
# Optional for private PKI; system roots are used when omitted.
SCRAPY_PULSAR_TLS_TRUST_CERTS_FILE = "/etc/ssl/private-pki-ca.pem"
```

Authenticated connections reject blank tokens, URL userinfo,
`SCRAPY_PULSAR_ALLOW_INSECURE_CONNECTION=True`, and
`SCRAPY_PULSAR_TLS_VALIDATE_HOSTNAME=False`.

### Amazon SQS (standalone=LocalStack, cloud=AWS)

```python
SCRAPY_BACKEND_TYPE = "sqs"
SCRAPY_SQS_MODE = "standalone"
SCRAPY_SQS_REGION_NAME = "us-east-1"
SCRAPY_SQS_ENDPOINT_URL = "http://localhost:4566"  # optional standalone override
SCRAPY_SQS_VISIBILITY_TIMEOUT = 300
```

Standalone mode defaults an omitted endpoint to loopback LocalStack. To use
real AWS, set `SCRAPY_SQS_MODE = "cloud"` and leave the endpoint unset. The
visibility lease is not auto-renewed; size it above the maximum expected
pop-to-downloader-response time. Explicit nack makes the message immediately
visible (`VisibilityTimeout=0`).

Region names use a future-compatible ASCII structural check. Multi-label region
identifiers used across AWS partitions, such as `us-gov-west-1`,
`us-iso-east-1`, and `eusc-de-east-1`, are accepted; this package does not
freeze a region allowlist, so the SDK/service endpoint still decides whether a
structurally valid name supports SQS.

An SQS connection is an immutable client generation: credentials, endpoint,
region, queue prefix, visibility timeout, QueueUrl cache, and receipt tokens do
not cross a reconnect boundary. Calling `connect()` while already connected is
an idempotent no-op. To apply changed settings, call `disconnect()` and then
`connect()`; old receipt tokens become stale without being sent through the new
client. Disconnect rejects newly arriving operations, waits for already
admitted SDK calls (including long polls or a purge barrier), and closes the
retired client only after they finish. First-use QueueUrl discovery is
single-flight per logical queue; a slow lookup does not serialize unrelated
queues or delay an already-issued receipt acknowledgement.

### Memcached (standalone, NoSQL KV)

```python
SCRAPY_BACKEND_TYPE = "memcached"
SCRAPY_MEMCACHED_HOST = "localhost"
SCRAPY_MEMCACHED_PORT = 11211
# Required only for a non-loopback server on an isolated trusted network:
# SCRAPY_MEMCACHED_ALLOW_REMOTE_PLAINTEXT = True
# SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL = True  # dedicated servers only
```

Memcached traffic in this backend is unauthenticated and unencrypted. Loopback
hosts work by default; any remote host fails at startup unless
`ALLOW_REMOTE_PLAINTEXT=True` explicitly accepts that trusted-network risk.
Mutating operations disable pymemcache's `noreply` default and wait for the
server response before reporting StorageBackend success. Because the ordinary
pymemcache client owns one request/response socket, all operations and teardown
are serialized so concurrent callers cannot consume one another's replies.
Memcached cannot enumerate or prefix-delete application keys. Consequently,
`clear_storage(prefix=...)` is unsupported and `clear_storage(None)` raises by
default. Enabling `ALLOW_FLUSH_ALL` permits a server-wide destructive flush and
is appropriate only for a dedicated Memcached instance. That permission is
captured by the connected client generation; later settings mutation cannot
enable a flush, and the server must return an explicit successful reply.

### DynamoDB (standalone=LocalStack, cloud=AWS, NoSQL KV)

```python
SCRAPY_BACKEND_TYPE = "dynamodb"
SCRAPY_DYNAMODB_MODE = "standalone"
SCRAPY_DYNAMODB_TABLE_NAME = "scrapy-extension"
SCRAPY_DYNAMODB_REGION_NAME = "us-east-1"
SCRAPY_DYNAMODB_ENDPOINT_URL = "http://localhost:4566"  # optional standalone override
```

As with SQS, standalone mode defaults to loopback LocalStack. Set mode to
`cloud` and leave `endpoint_url` unset for real AWS. The same multi-label
region grammar accepts GovCloud, ISO, and EUSC identifiers without claiming
that DynamoDB is available in every structurally valid region.

DynamoDB gives every connection candidate a private boto3 Session and publishes
the prepared Session/Resource/Table set as one generation. A live `connect()`
is idempotent; apply endpoint, region, table, or credential-setting changes with
an explicit `disconnect()` / `connect()`. Because the boto3 Resource API is not
thread-safe, storage calls and health probes are serialized for each backend
instance. Disconnect drains an admitted operation, then closes that
generation's botocore client. Paginated clears and lazy TTL cleanup remain on
their issuing table generation.

`clear_storage()` uses explicit batches of at most 25 deletes. A batch gets at
most eight application-level BatchWriteItem submissions; only a structurally
valid `UnprocessedItems` subset is retried with full-jitter backoff. Botocore's
own retry/timeout layer remains separate, so this is not a wire-attempt or
wall-clock bound. Exhaustion, malformed service responses, or SDK failures
raise `StorageError` and may leave a partial clear. The method does not roll
back accepted deletes. Stop external writers before a deterministic maintenance
clear: even a successful strongly consistent Scan is not a cross-page snapshot
and cannot prove the table stayed empty.

`delete()` returns `False` only when `DeleteItem(ALL_OLD)` omits `Attributes`,
which is AWS's missing-item result. A present old item must contain the exact
string partition key requested; malformed proxy, emulator, or SDK responses
raise typed `StorageError` rather than being mistaken for deletion success.

See the [examples directory](https://github.com/azwpayne/scrapy-extension/tree/main/examples) for representative spiders and deployment-mode recipes (Sentinel, Cluster, Atlas, Confluent, etc).

## Backend Capabilities

| Backend       | Queue | Set | Storage | Modes                                        |
|---------------|-------|-----|---------|----------------------------------------------|
| Redis         | Yes   | Yes | Yes     | standalone, master_slave, sentinel, cluster  |
| MongoDB       | Yes   | Yes | Yes     | standalone, replica_set, sharded_cluster, atlas |
| ElasticSearch | Yes   | Yes | Yes     | standalone, cloud                            |
| Kafka         | Yes   | No  | No      | standalone, cluster, confluent               |
| RabbitMQ      | Yes   | No  | No      | standalone, cluster, mirrored_queues         |
| RocketMQ      | Yes   | Guard | Guard  | standalone, cluster, cloud                   |
| Pulsar        | Yes   | No  | No      | standalone, cluster                          |
| SQS           | Yes   | No  | No      | standalone (LocalStack), cloud (AWS)         |
| Memcached     | No    | No  | Yes     | standalone                                   |
| DynamoDB      | No    | No  | Yes     | standalone (LocalStack), cloud (AWS)         |

- **Yes** — implements the capability interface; per-operation limitations are
  listed below
- **No** — not implemented (this backend doesn't expose the interface)
- **Guard** — rejected at config time (`ConfigurationError`); a guard class fails-fast if the capability gate is bypassed (RocketMQ set/storage)

**Kafka, RabbitMQ, Pulsar, SQS**: Queue-only. For deduplication and storage, use Redis, MongoDB, ElasticSearch, Memcached, or DynamoDB.

**Kafka clear limitation**: `clear_queue()` raises `NotImplementedError`.
Deleting and recreating a topic is asynchronous and can reuse consumer-group
offsets, so it cannot provide the queue abstraction's safe-clear boundary.
Stop producers/consumers and use an operator-controlled Kafka maintenance or
drain workflow instead.

**RabbitMQ clear barrier**: broker purge removes ready messages, not
unacknowledged deliveries. `clear_queue()` therefore fails before purge while
that logical queue has any locally issued, unsettled delivery. Ack/nack those
tokens first; after disconnect, reconnect before clearing so the broker has
requeued the old deliveries. Operations and purge are serialized at the channel
boundary, while in-flight deliveries on unrelated queues do not block the
target clear.

**Pulsar token settlement**: each token returned by `pop_with_ack()` accepts
one successful ACK or NACK. Repeating that action—or racing it with the
opposite action—does not issue another broker call. A client failure raises
`QueueError` and leaves the same token retryable. The token path is independent
of the legacy `pop()` / tokenless settlement slot.

**SQS clear barrier**: `clear_queue()` waits at least 60 seconds after the
PurgeQueue RPC because AWS may delete messages sent during that asynchronous
window. Operations on the same physical queue wait behind the barrier; other
queues remain live. Even an ambiguous purge failure waits out the window before
raising `QueueError`, so shutdown and maintenance timeouts must allow for it.

**RocketMQ**: Queue is functional. Set/Storage are rejected at config time (`ConfigurationError`) — pair with a full-featured backend (Redis, MongoDB, ElasticSearch, Memcached, or DynamoDB) for dedup/storage.

**Memcached, DynamoDB**: Storage-only (key-value with TTL). Pair with a queue-capable backend for request distribution.

Operational limits within otherwise supported interfaces:

- Pulsar and RocketMQ cannot query broker backlog through their bundled clients;
  `queue_len()` raises `NotImplementedError`. Scheduler pending detection stays
  conservative and continues polling, while depth-based monitoring and
  backpressure are unavailable.
- Memcached cannot prefix-clear keys. Its only global clear operation is gated
  by `SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL=False` by default.
- SQS depth is an eventually consistent approximation (visible + in-flight +
  delayed counts), not an exact instantaneous value.

## Guarantees

What the library contractually promises — and just as importantly, what it does **not**. Read this before relying on any feature in production. The stable source of truth is the named symbol in `src/scrapy_extension/`; numeric source line references are intentionally avoided because they drift as the implementation evolves.

### Per-feature cross-worker behavior

| Layer | Strategy | Cross-worker safe? | Notes |
|---|---|---|---|
| Queue | `passthrough` (default) | Yes | Items remain in the backend queue. Redis/MongoDB/ElasticSearch remove atomically; Kafka/RabbitMQ/RocketMQ/Pulsar/SQS lease or deliver with a per-message ack token. |
| Queue | `delay` | Per-process | In-process `heapq`; a clean-close snapshot is available only when the **queue backend itself** also implements `StorageBackend`. Queue-only backends cannot persist it. Hard crashes lose unsnapshotted state. In multi-worker deployments set a stable, unique `SCRAPY_QUEUE_SNAPSHOT_OWNER` (or `SCRAPY_QUEUE_WORKER_ID` fallback) to prevent workers from sharing a snapshot key. |
| Queue | `round_robin` | Per-process | Fair dispatch across `request.meta['source']` using a per-worker index. |
| Queue | `throttle` | Per-process | Effective rate under N workers = `N × (1 / min_interval)`. |
| Queue | `priority` | Yes | Items live in backend-side priority buckets; Kafka and RocketMQ are rejected because their consumers cannot isolate a scan across strategy-created topics. |
| Queue | `time_wheel` | Per-process | Timing wheel and overflow heap are local; the same snapshot capability and owner requirements as `delay` apply. |
| Queue | `work_stealing` | Yes, with explicit topology | Worker queues live in the backend; use stable worker IDs and a complete peer list. Kafka and RocketMQ are rejected. |
| Queue | `ring_buffer` | Per-process | The bounded in-process buffer is the queue; the backend is intentionally bypassed. |
| Dedup | `set` (default) | Yes — exact | Backend `SADD`/`SISMEMBER` semantics; byte-identical to pre-strategy behavior (`dupefilter/filters/set_filter.py`). |
| Dedup | `memory` | Per-process | In-process; optional LRU cap via `SCRAPY_DEDUP_MEMORY_MAXSIZE` (default 1,000,000; round-9 U5). |
| Dedup | `bloom` | Per-process | Pure-stdlib bit-vector; **never produces false negatives** (a seen URL is always reported seen); false-positive rate is configurable. |
| Dedup | `cuckoo` | Per-process | Pure-stdlib; **never produces false negatives**; supports deletion; raises `FilterFull` at capacity (degrades to passthrough + warn-once). |
| Storage | all storage-capable backends | Yes | Via the `StorageBackend` KV+TTL contract. |

**Defaults are distributed-exact.** `set` dedup + `passthrough` queue are safe for multi-worker crawls out of the box. `delay` / `throttle` / `round_robin` / `time_wheel` / `ring_buffer` / `memory` / `bloom` / `cuckoo` are **per-process opt-in**. `priority` and correctly configured `work_stealing` retain backend-side payload durability.

### Contractual promises

| Promise | Where enforced |
|---|---|
| **Fail-fast configuration.** Unknown nested fields and typoed flat `SCRAPY_<BACKEND>_*` names raise `ConfigurationError` with a suggestion. Project cross-field/capability checks also raise `ConfigurationError`; Pydantic type/range/enum validation raises `ValidationError`. `ConfigurationError.setting_name` / `.setting_value` are Stable attributes, and sensitive setting names are redacted. | `backends.connectors.resolve_backend_config`, backend settings models; see [STABILITY.md](https://github.com/azwpayne/scrapy-extension/blob/main/STABILITY.md) |
| **Structured credential redaction.** Password/token/API-key fields use `SecretStr`; selected SDK-bound values are additionally wrapped in a repr-redacting `str` subclass. That wrapper masks `repr(...)`, `!r`, and repr-based container displays only. Ordinary `str`, default/`!s` f-string, `%s`, formatting, and serialization paths expose the underlying value required for authentication. This does not cover credentials embedded in plain URI fields, caller-owned dictionaries, broker logs, or arbitrary third-party tracebacks; do not log those values. | backend settings models and `backends._redaction` |
| **No code execution on the data path.** Serialization is JSON only — never `pickle`, never `eval`. Unknown types raise `TypeError` instead of being silently `str()`-ed. | `backends.base.JSONSerializer` |
| **Input names are validated.** Queue / set / index / topic names match the documented safe subsets; injection-shaped inputs are rejected before use. | `backends.base._validate_key_name` and backend topic validators |
| **Ack correctness under `CONCURRENT_REQUESTS > 1`.** Deferred-ack backends (Kafka, RabbitMQ, RocketMQ, Pulsar, SQS) carry a per-message ack token so the *specific* popped message is acked. Kafka additionally fences tokens by consumer generation, assignment epoch, and unique delivery attempt, preventing a late completion from committing a same-offset redelivery after nack/rebalance. Retry/redirect replacements transfer the token through their queue commit; user errbacks returning one or many requests use child tokens and settle the source only after every replacement is accepted. The scheduler's `from_settings` gate refuses a backend/plugin that declares single-slot ack unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS` is set. | `backends/base.py` (`QueueBackend` ack contract), `backends/kafka.py`, `schedule/scheduler.py` |
| **Lazy optional deps.** `pip install scrapy-extension` works with **zero** backend deps. Each backend's optional dep loads on first access via PEP 562, with `ImportError` install hints. | package and backends `__getattr__` implementations |
| **Probabilistic dedup never false-negatives.** Bloom and Cuckoo may produce false positives (a fresh URL reported as "seen"); they will never let a seen URL through as fresh. | `dupefilter/filters/bloom_filter.py`, `dupefilter/filters/cuckoo_filter.py` |
| **Backend capability honesty.** A backend never silently no-ops on an unsupported interface: queue-only backends omit `SetBackend`/`StorageBackend` entirely; RocketMQ set/storage are rejected at config time (`ConfigurationError` guard). The matrix above is the contract. | `backends/base.py` ABCs; `backends/connectors.py` capability gates |
| **`py.typed` marker shipped.** Full type annotations on the public surface; downstream type-checkers consume the shipped typing. | `scrapy_extension/py.typed` in the wheel |

### What is **not** promised

- **Cross-worker behavior of `delay` / `throttle` / `round_robin` / `time_wheel` / `ring_buffer` / `memory` / `bloom` / `cuckoo` strategies** — they are per-process by design (see table above).
- **Stability of the entry-point registration API** (`BackendDescriptor`) — round-5 surface, no 3rd-party ecosystem yet; expect possible minor-bump changes. See [STABILITY.md](https://github.com/azwpayne/scrapy-extension/blob/main/STABILITY.md).
- **Stability of fresh hooks** — `on_filter_full` (round-7) and `backpressure_pause_at` / `backpressure_resume_at` (round-4) are new; the hook signatures and setting semantics may evolve in a minor bump.
- **Wire compatibility for the SQS / Memcached / DynamoDB LocalStack paths** — exercised via LocalStack in CI; not certified against every AWS region or Memcached server version.

For the full stability/maturity tiering per backend, see [STABILITY.md](https://github.com/azwpayne/scrapy-extension/blob/main/STABILITY.md). To report a security issue, see [SECURITY.md](https://github.com/azwpayne/scrapy-extension/blob/main/SECURITY.md). For what changed in each release, see [CHANGELOG.md](https://github.com/azwpayne/scrapy-extension/blob/main/CHANGELOG.md).

## Multi-Backend Coexistence

The three components — **Scheduler** (queue), **DupeFilter** (set), **Pipeline** (storage) — can each bind to a *different* backend. This unlocks hybrid topologies that play to each backend's strengths: a high-throughput queue in Kafka, exact cross-worker dedup in Redis, durable item storage in MongoDB.

| Component | Per-component keys | Fallback (backward compat) |
|-----------|-------------------|------|
| Scheduler (queue) | `SCRAPY_QUEUE_BACKEND_TYPE` / `SCRAPY_QUEUE_BACKEND_SETTINGS` | `SCRAPY_BACKEND_TYPE` / `SCRAPY_BACKEND_SETTINGS` |
| DupeFilter (set) | `SCRAPY_SET_BACKEND_TYPE` / `SCRAPY_SET_BACKEND_SETTINGS` | `SCRAPY_BACKEND_TYPE` / `SCRAPY_BACKEND_SETTINGS` |
| Pipeline (storage) | `SCRAPY_STORAGE_BACKEND_TYPE` / `SCRAPY_STORAGE_BACKEND_SETTINGS` | `SCRAPY_BACKEND_TYPE` / `SCRAPY_BACKEND_SETTINGS` |

When a per-component type key is set, that component uses its per-component settings; otherwise it falls back to the global keys — so existing single-backend configurations keep working unchanged.

**Example: queue in Redis-Cluster, dedup fingerprints + scraped data in MongoDB**

```python
# settings.py
SCHEDULER = "scrapy_extension.schedule.scheduler.BackendScheduler"
DUPEFILTER_CLASS = "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"
ITEM_PIPELINES = {"scrapy_extension.pipeline.pipeline.BackendPipeline": 300}

# Queue: Redis-Cluster
SCRAPY_QUEUE_BACKEND_TYPE = "redis"
SCRAPY_QUEUE_BACKEND_SETTINGS = {
    "mode": "cluster",
    "cluster_startup_nodes": ["redis-1:6379", "redis-2:6379"],
    "namespace": "crawler-prod",
}

# Dedup fingerprints: MongoDB
SCRAPY_SET_BACKEND_TYPE = "mongodb"
SCRAPY_SET_BACKEND_SETTINGS = {"uri": "mongodb://mongo:27017", "database": "scrapy"}

# Scraped data: MongoDB (same cluster, separate connection manager entry)
SCRAPY_STORAGE_BACKEND_TYPE = "mongodb"
SCRAPY_STORAGE_BACKEND_SETTINGS = {"uri": "mongodb://mongo:27017", "database": "scrapy"}
```

> **Constraint**: each backend must implement the interface its component needs — queue backends must implement `QueueBackend`, dedup backends `SetBackend`, storage backends `StorageBackend` (see the [capabilities matrix](#backend-capabilities)). The `ConnectionManager` registry keys one pooled connection per `backend_type:settings_hash`, so co-located backends (e.g. set + storage both MongoDB, same URI) share a single connection.

## Pluggable Strategy Layers

Three strategy layers sit above the backend interfaces, selected via Scrapy settings — no code change required. Defaults preserve prior behavior exactly.

### Dedup strategy — `SCRAPY_DEDUP_STRATEGY`

`BackendDupeFilter` delegates to a `MembershipFilter`:

| Strategy | Exact? | Cross-worker? | Delete? | Notes |
|----------|--------|---------------|---------|-------|
| `set` (default) | yes | yes | yes | `SetBackend`-backed; byte-identical to prior behavior |
| `memory` | yes | no | yes | in-process, optional LRU cap (`SCRAPY_DEDUP_MEMORY_MAXSIZE`) |
| `bloom` | no (FP) | no | no | pure-stdlib bit-vector; `SCRAPY_DEDUP_BLOOM_CAPACITY` / `_ERROR_RATE` |
| `cuckoo` | no (FP) | no | yes | pure-stdlib; `SCRAPY_DEDUP_CUCKOO_CAPACITY` / `_ERROR_RATE` |

Probabilistic filters never produce false negatives; in-memory filters are per-process (single-worker). Use `set` for multi-worker exact dedup.

The default `set` strategy requires a set-capable backend. Queue-only backends
can use a local `memory`, `bloom`, or `cuckoo` filter, but those filters do not
deduplicate across workers. For distributed exact dedup, bind
`SCRAPY_SET_BACKEND_TYPE` to Redis, MongoDB, or ElasticSearch.

### Queue semantics — `SCRAPY_QUEUE_STRATEGY`

`BackendQueue` delegates bytes-level push/pop to a `QueueStrategy` (task-queue types beyond queue/stack/priority):

| Strategy | Behavior |
|----------|----------|
| `passthrough` (default) | delegates to `QueueBackend` unchanged (prior behavior) |
| `delay` | holds items until `now + delay`; per-request via `request.meta['delay']` or `SCRAPY_QUEUE_DELAY_DEFAULT` |
| `round_robin` | fair dispatch across `request.meta['source']` (no starvation) |
| `throttle` | rate-limited pops (`SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL`); limiter state is per-process |
| `priority` | strategy-layer priority buckets for backends without native priority |
| `time_wheel` | hashed timing wheel for many short delays; in-process timing state |
| `work_stealing` | own queue first, then steal from peer queues when idle |
| `ring_buffer` | bounded in-process circular buffer with explicit overflow policy |

`delay`, `round_robin`, `throttle`, `time_wheel`, `work_stealing`, and `ring_buffer` keep some state in-process. `passthrough` is the distributed-exact default. See [Ack and durability matrix](#ack-and-durability-matrix) before using a stateful strategy in production.

`priority` and `work_stealing` fan out one logical queue into multiple physical
queues. They fail fast with Kafka and RocketMQ because those backends cannot
isolate a pop to one strategy-selected topic. Backend-delegating strategies
(`passthrough`, `delay`, `throttle`, `priority`, `time_wheel`, and
`work_stealing`) preserve MQ ack tokens where supported. `round_robin` and
`ring_buffer` are fully local and intentionally bypass broker durability.

`delay`, `round_robin`, `time_wheel`, and `ring_buffer` implement clean-close
snapshots. Persistence is available only when the queue's connection manager
also exposes storage. In a multi-worker deployment, configure a stable unique
`SCRAPY_QUEUE_SNAPSHOT_OWNER` per worker; when omitted,
`SCRAPY_QUEUE_WORKER_ID` is the fallback. A restored checkpoint remains stored
until the next clean close replaces it with current state or deletes it after a
clean drain. A crash after restore can therefore replay already-processed
entries, but cannot lose the only copy of entries not yet processed.

### Storage strategy — `SCRAPY_STORAGE_STRATEGY`

`BackendPipeline` delegates item writes to a `StorageStrategy`:

| Strategy | Behavior | Durability note |
|----------|----------|-----------------|
| `passthrough` (default) | writes each serialized item directly to the selected `StorageBackend` | backend durability applies immediately after `store()` returns |
| `batched` | buffers `(key, value, ttl)` triples and flushes at threshold / spider close | improves throughput, but a hard crash before flush loses the in-process batch; store exceptions re-enqueue the unwritten tail |

Use `passthrough` when item loss is unacceptable. Use `batched` only when throughput is worth the crash-before-flush trade-off and duplicate writes are acceptable after partial flush retry.

### Ack and durability matrix

| Surface | Ack / state boundary | Crash behavior | Operational guidance |
|---|---|---|---|
| Redis / MongoDB / ElasticSearch queue pop | atomic backend pop; scheduler ack is inert | item is removed once popped; a later callback/pipeline crash can lose downstream item work | pair with idempotent callbacks/pipelines when end-to-end exactly-once matters |
| Kafka / RabbitMQ / Pulsar queue pop | per-message token stored in request meta and acked on Scrapy `response_received` | crash before ack redelivers; crash after downloader response but before callback/pipeline completion can drop downstream processing | safe under `CONCURRENT_REQUESTS > 1`; RabbitMQ push also waits for publisher confirmation |
| SQS / RocketMQ queue pop | per-message token plus a finite broker visibility/invisibility lease | an unacked message becomes deliverable again when the lease expires, including while a slow download is still running | no automatic lease renewal; set the lease above maximum pop-to-response time. SQS nack is immediate; RocketMQ nack uses its 10-second floor |
| Backend/plugin declaring `supports_concurrent_ack=False` | single ack slot only | `CONCURRENT_REQUESTS > 1` raises at startup unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=True` | keep `CONCURRENT_REQUESTS=1` for such backends, or choose one with a real in-flight ack set |
| Stateful queue strategies | in-process scheduling/fairness/rate/buffer state, with best-effort snapshot only where implemented | hard crash can lose held strategy state even if backend queue survives; a token-bearing replacement is rejected before entering volatile delay/time-wheel/round-robin/ring-buffer state | use a backend-durable push path (`passthrough`, `priority`, `work_stealing`, `throttle`, or zero effective delay) when replacing an unacked broker delivery |
| `batched` storage | in-process item buffer before backend `store()` | hard crash before flush loses buffered items; partial store exceptions retry the unwritten tail | prefer `passthrough` when persistence must happen before item acknowledgement |

Ack is tied to Scrapy downloader response delivery, not spider callback or item pipeline completion. If a crawl must tolerate process death after response download but before item persistence, make item processing idempotent and use a durable storage strategy/topology.

Replacement publication and source acknowledgement are two broker operations,
not one distributed transaction. The scheduler orders them as “replacement
commit, then source ACK,” so a crash in between can publish a duplicate after
source redelivery but cannot lose both copies. Keep deduplication enabled and
make `dont_filter=True` replacement handlers idempotent. If the replacement
would enter only process-local strategy state (`delay`/`time_wheel` with a
positive effective delay, `round_robin`, or `ring_buffer`), the push fails
closed before mutating that state and leaves the source unacknowledged.

Pulsar and RocketMQ do not expose queue depth through the bundled clients.
Their scheduler stays conservative (`has_pending_requests()` returns true when
depth is unavailable) and continues polling, but depth-driven backpressure and
`queue/depth` monitoring cannot operate.


## Architecture

### Interface Hierarchy

```
Backend (ABC)
├── connect(), disconnect(), is_connected(), ping()
│
├── QueueBackend (ABC)
│   ├── push(queue_name, item, priority)
│   ├── pop(queue_name, timeout)
│   ├── queue_len(queue_name)
│   └── clear_queue(queue_name)
│
├── SetBackend (ABC)
│   ├── add(set_name, item)
│   ├── remove(set_name, item)
│   ├── contains(set_name, item)
│   ├── set_len(set_name)
│   └── clear_set(set_name)
│
└── StorageBackend (ABC)
    ├── store(key, data, ttl)
    ├── retrieve(key)
    ├── delete(key)
    ├── exists(key)
    ├── ttl(key)
    └── clear_storage(prefix)
```

All backends implement `Backend` plus the capability interfaces declared in the [Backend Capabilities](#backend-capabilities) matrix. Redis, MongoDB, and ElasticSearch implement all three capability interfaces; queue-only and storage-only backends are rejected at config time when selected for an unsupported component.

An interface does not imply every optional operation can be implemented by the
underlying service. In particular, Memcached cannot prefix-clear storage, and
Pulsar/RocketMQ cannot report queue depth. These cases fail explicitly rather
than returning a misleading success or zero.

### Connection Management

```python
from scrapy_extension import ConnectionManager, BackendType

manager = ConnectionManager.get_manager(backend_type=BackendType.REDIS)
try:
    queue = manager.get_queue_backend()
    queue.push("my_queue", b"item_data", priority=1.0)
finally:
    manager.close()
```

`ConnectionManager` provides:
- **Lazy singleton**: thread-safe registry keyed by `backend_type:settings_hash`
- **Retry logic**: one initial attempt plus up to `SCRAPY_RETRY_ATTEMPTS`
  retries (default 3, range 0..20), with full-jitter exponential backoff whose
  base is `SCRAPY_RETRY_DELAY` (default 1 second)
- **Reference-counted lifecycle**: every successful `get_manager()` acquisition
  must be paired with exactly one `close()`. A shared manager is disconnected
  only when its final holder releases it; do not close the same acquisition
  twice

### Per-Spider Settings

```python
class MySpider(BackendSpiderMixin, scrapy.Spider):
    backend_type = BackendType.REDIS
    backend_settings = {
        "host": "localhost",
        "port": 6379,
        "namespace": "my-spider",
    }
```

For spiders created by Scrapy, `BackendSpiderMixin.from_crawler()` performs
backend setup automatically after the crawler is attached and binds lifecycle
signals exactly once. Do not add a second `from_crawler()` or call
`setup_backend()` from `__init__`. Only code that constructs `MySpider()`
directly, outside Scrapy's factory, must call `setup_backend()` before using the
low-level backend properties and must later call `close_backend()`.

The class attributes configure the mixin's direct manager only. Scheduler,
dupefilter, and pipeline selection still comes from Scrapy settings; use the
global or component-specific settings shown above when those components must
use the same backend.

## Scrapy Components

| Component    | Class                | Purpose                                          |
|--------------|----------------------|--------------------------------------------------|
| Scheduler    | `BackendScheduler`   | Distributes requests across spider instances      |
| DupeFilter   | `BackendDupeFilter`  | Filters duplicate requests using `SetBackend`     |
| Pipeline     | `BackendPipeline`    | Stores scraped items using `StorageBackend`       |
| Queue        | `BackendQueue`       | Serializes/deserializes Scrapy requests           |
| SpiderMixin  | `BackendSpiderMixin` | Convenient access to backend components           |

All components follow Scrapy's `from_settings()` / `from_crawler()` factory pattern.

### Scheduler

```python
SCHEDULER = "scrapy_extension.schedule.scheduler.BackendScheduler"
```

Integrates `BackendQueue` for request distribution and `BackendDupeFilter` for deduplication. For Kafka, RabbitMQ, RocketMQ, Pulsar, and SQS, ack/nack is tied to Scrapy's downloader response lifecycle; it does not wait for callback or item pipeline completion.

### DupeFilter

```python
DUPEFILTER_CLASS = "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter"
```

Uses a `MembershipFilter` for duplicate detection (default:
`SetBackend.add()`). Select the strategy via `SCRAPY_DEDUP_STRATEGY` (see
[Pluggable Strategy Layers](#pluggable-strategy-layers)). The default `set`
strategy fails fast when the selected set backend lacks that capability; bind a
separate set backend for distributed dedup or deliberately choose a local
filter.

### Pipeline

```python
ITEM_PIPELINES = {"scrapy_extension.pipeline.pipeline.BackendPipeline": 300}
SCRAPY_PIPELINE_KEY_PREFIX = "items"
SCRAPY_PIPELINE_TTL = 3600  # seconds, optional
```

Stores items as JSON with keys: `{prefix}:{spider_name}:{timestamp}:{uuid}`.

## Exceptions

```
BackendError (base)
├── BackendConnectionError   — connection failures (includes backend_type)
├── QueueError               — queue operation failures (includes queue_name, operation)
├── StorageError             — storage operation failures (includes operation, key)
├── SerializationError       — serialization failures (includes data, serializer)
└── ConfigurationError       — invalid settings (includes setting_name, setting_value)
```

Project exceptions expose the context shown above where applicable. Pydantic
schema failures are `pydantic.ValidationError`, not `BackendError`, while
project cross-field, capability, and unknown-setting failures use
`ConfigurationError`.

## Examples

See the [examples guide](https://github.com/azwpayne/scrapy-extension/blob/main/examples/README.md) for representative spiders and recipes. Some shipped backends are documented as settings recipes rather than dedicated spiders:

| Spider | Backend | Features Demonstrated |
|--------|---------|----------------------|
| `quotes_redis` | Redis | Full queue + set + storage |
| `quotes_mongodb` | MongoDB | Document storage, replica set |
| `quotes_kafka` | Kafka | High-throughput streaming (queue only) |
| `quotes_rabbitmq` | RabbitMQ | Priority queues, HA (queue only) |
| `quotes_elasticsearch` | ElasticSearch | Full-text search storage |
| `quotes_multi_mode` | All | Multi-mode configurations |
| `quotes_connection_manager` | All | Direct `ConnectionManager` API |
| `quotes_programmatic` | Redis | Per-spider `backend_settings` dict |
| `quotes_crawl` | None | CrawlSpider variant |
| RocketMQ / Pulsar / SQS / Memcached / DynamoDB | recipes | Backend-specific settings; pair partial-capability backends with queue/set/storage-capable partners as needed |
| `quotes` | None | Basic Scrapy spider (no backend) |

## Security

- **Key name validation**: validated against `^[a-zA-Z0-9._:-]+$`
- **Topic name validation**: Kafka topics validated against `^[a-zA-Z0-9._-]+$`
- **Input sanitization**: all user-provided queue/set names validated before use
- **No code execution**: JSON serialization only — never pickle or eval

JSON safety is not confidentiality. Request metadata, request bodies, and
scraped items are serialized as data and may include secret-bearing values;
some supported types such as Pydantic secret wrappers are serialized to their
underlying value. Use TLS for every backend connection, least-privilege broker
and database ACLs, and encryption at rest. Do not place secrets in queued or
stored payloads unless the application encrypts them before handing them to the
extension. Credentials embedded in plain DSN/URI strings are caller-owned and
must not be logged.

See the complete [security policy](https://github.com/azwpayne/scrapy-extension/blob/main/SECURITY.md).

## Testing

```bash
# Run all tests
uv run pytest

# Run specific backend tests
uv run pytest tests/test_mongodb_backend.py

# Run with coverage report
uv run pytest --cov=scrapy_extension --cov-report=term-missing

# Run full matrix (all Python versions)
uv run poe test
```

Test infrastructure includes: pytest-xdist (parallel), pytest-randomly (randomized order), pytest-mock, pytest-cov (coverage with `fail_under = 95`), pytest-ruff (lint), pytest-socket (unit tests run with sockets disabled by default), and more. Live integration tests require explicit backend env vars plus `--force-enable-socket`.

## License

MIT — see [LICENSE](https://github.com/azwpayne/scrapy-extension/blob/main/LICENSE).
