# Scrapy Extension

Distributed crawling for Scrapy with pluggable backends (**Redis**, **MongoDB**, **Kafka**, **RabbitMQ**, **ElasticSearch**, **RocketMQ**, **Pulsar**, **SQS**, **Memcached**, **DynamoDB**) and pluggable strategy layers for dedup and queue semantics.

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/azwpayne/scrapy-extension/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://github.com/azwpayne/scrapy-extension/releases)

## Contents

- [Features](#features) · [Installation](#installation) · [Quick Start](#quick-start)
- [Backend Configuration](#backend-configuration) · [Backend Capabilities](#backend-capabilities)
- [Guarantees](#guarantees) · [Multi-Backend Coexistence](#multi-backend-coexistence)
- [Pluggable Strategy Layers](#pluggable-strategy-layers) — incl. the [Ack & durability matrix](#ack-and-durability-matrix)
- [Architecture](#architecture) · [Scrapy Components](#scrapy-components) · [Exceptions](#exceptions)
- [Examples](#examples) · [Security](#security) · [Testing](#testing) · [License](#license)

> **Deeper docs:** [operations](https://github.com/azwpayne/scrapy-extension/blob/main/docs/runbook.md) · [upgrade and backlog migration](https://github.com/azwpayne/scrapy-extension/blob/main/docs/migration-guide.md) · [plugin authors](https://github.com/azwpayne/scrapy-extension/blob/main/docs/backend-plugins.md) · [API/maturity](https://github.com/azwpayne/scrapy-extension/blob/main/.github/STABILITY.md) · [runnable examples](https://github.com/azwpayne/scrapy-extension/tree/main/examples)

## Features

- **10 Backends**: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ, Pulsar, SQS, Memcached, DynamoDB
- **Multi-Mode**: Standalone, cluster, cloud deployments per backend
- **Pluggable Dedup**: Set / Memory / **Bloom** / **Cuckoo** filters via `SCRAPY_DEDUP_STRATEGY`
- **Pluggable Queue Semantics**: 8 strategies — Passthrough / **Delay** / **RoundRobin** / **Throttle** / **Priority** / **TimeWheel** / **WorkStealing** / **RingBuffer** via `SCRAPY_QUEUE_STRATEGY`
- **Multi-Backend Coexistence**: bind queue / dedup / storage to *different* backends via `SCRAPY_{QUEUE,SET,STORAGE}_BACKEND_TYPE` (e.g. queue in Redis, dedup + data in MongoDB)
- **Distributed Queue**: Priority-based request queue across spiders
- **Duplicate Filtering**: Exact cross-instance stored membership (default:
  `SetBackend`) with crash-safe at-least-once scheduler admission
- **Item Storage**: Key-value storage with TTL support via `StorageBackend`
- **Type Safe**: Full type annotations, `py.typed` marker
- **Secure**: Input validation on key names, topic names, and queue identifiers

## Installation

### Source checkout (currently available)

The project has not published a PyPI release yet. Clone the repository and
install the extra for the backend you intend to use:

```bash
git clone https://github.com/azwpayne/scrapy-extension.git
cd scrapy-extension
pip install -e ".[redis]"
```

### PyPI (after the first public release)

Once a tagged release is published to PyPI, install the same extras with the
commands below.
The package requires `Scrapy>=2.17,<3`; this lower bound excludes older
advisory-affected Scrapy releases.

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

### Distribution artifact contents

The wheel is the minimal runtime artifact: it contains the importable
`scrapy_extension` package, `py.typed`, and distribution metadata/license
files. The source distribution is intentionally self-contained for downstream
verification: in addition to the runtime sources it carries the root README and
license, user docs, examples, tests, root `conftest.py`, `uv.lock`, and the CI
workflows those tests inspect. CI installs both artifacts and runs a locked test
subset from an extracted sdist; docs, examples, and tests are not copied into
the wheel.

## Quick Start

This example uses Redis for scheduling, duplicate filtering, and item storage.
From a source checkout, install the Redis extra and start a Redis server that is
reachable at `localhost:6379` before starting the crawl:

```bash
pip install -e ".[redis]"
docker run -d --rm --name scrapy-extension-redis -p 127.0.0.1:6379:6379 redis:7-alpine
```

> **Local development only:** This Docker command binds Redis to loopback; do
> not use it on shared, cloud, or production hosts.

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

### Reactor and scheduler latency contract

`SCRAPY_REACTOR_IO_TIMEOUT` defaults to **5 seconds** and is bounded to 60
seconds. Pipeline storage/open/close, scheduler queue close/connection warm-up/release,
and deferred broker acknowledgements run synchronous backend SDK calls in
Twisted's thread pool so a slow backend does not stop the reactor heartbeat.
Writes remain FIFO and close retains ownership until the worker operation really
finishes; a caller-visible timeout does not cancel a Python thread or pretend
that a write/ack completed.

Scrapy's `Scheduler.enqueue_request()`, `next_request()`, and
`has_pending_requests()` are inherently synchronous and cannot return a
Deferred. They remain bounded synchronous calls: manager retry **waits** are
capped by this setting, while the selected backend's native socket/RPC timeout
must bound each individual request. Configure both policies; this extension
cannot interrupt an arbitrary synchronous third-party SDK call from the reactor
thread. A slow backend can therefore still delay one scheduler poll up to its
native timeout, but cannot add unbounded retry sleeps. Use an async-capable
backend or a smaller native timeout when a tighter heartbeat budget is needed.

### Redis (standalone, sentinel, cluster; deprecated master_slave alias)

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

Redis topology settings are deliberately explicit:

| Mode | Effective topology | Required limits |
|------|--------------------|-----------------|
| `standalone` | one static primary at `host:port` | `db >= 0` |
| `master_slave` | deprecated primary-only alias of `standalone` | `replicas=[]`; `read_from_replicas=False`; no discovery, load balancing, replica reads, or failover |
| `sentinel` | Sentinel-discovered primary | non-empty `sentinels` and `sentinel_master_name`; data and control credentials are configured separately |
| `cluster` | redis-py Cluster discovery/sharding | `db=0`; startup seeds are optional and otherwise fall back to `host:port` |

The historical replica fields never routed reads. They now reject non-empty/
true values instead of silently accepting unsupported intent, and the first
validated connection that uses the `master_slave` alias emits `FutureWarning`;
use `standalone` for one static primary or Sentinel for discovery and failover.
Redis replication is
asynchronous, so suddenly enabling the old default would also make queue-depth,
deduplication, and storage reads stale.

`SCRAPY_REDIS_CLUSTER_MAX_REDIRECTS` (default 5, range 0–100) controls each
Cluster client's own MOVED/ASK/TRYAGAIN continuation budget. It does not enable
transport retry: the initial attempt is separate, and the data-plane
`Retry` count remains zero. Cluster supports database zero only; use the Redis
namespace or a separate Cluster for isolation.

Hosts are bare ASCII DNS names, canonical IPv4 addresses, or IPv6 addresses.
Active Sentinel and Cluster list entries use `host:port` or `[IPv6]:port`; the
deprecated replica list must remain empty. Scalar ports accept integers or
ASCII decimal text only. URI schemes, userinfo, paths, queries, fragments,
whitespace/control characters, non-ASCII port digits, and legacy numeric IPv4
spellings fail before SDK construction without echoing the address.
Non-selected topology nodes and non-default mode controls fail rather than
being ignored.

Redis connections are immutable generations. While a generation is published,
`connect()` is an idempotent no-op; it is not a health probe. Every bundled
queue, set, storage, and health operation stays on one client and one namespace
snapshot through completion. To recover after a failed `ping()`, or to apply a
changed connection-used endpoint, credential, TLS, mode, or namespace setting,
call `disconnect()` and then `connect()`. During disconnect, new admission is
rejected, a timed queue pop wakes with `QueueError`, and other admitted
operations drain before the retired data and Sentinel discovery clients close.
After teardown completes, a brand-new operation retains the established lazy
reconnect behavior; an already admitted loop can never make that transition.
In particular, a storage clear cannot scan with one client and delete through
a replacement. A Redis-layer clear failure may follow accepted deletes and is
reported as possibly partial.

The concrete backend's `client` property remains a lazy, point-in-time SDK
escape hatch for compatibility. Do not retain that raw object across
disconnect or use it to compose lifecycle-sensitive multi-command operations;
the generation lease protects the bundled backend methods, not external SDK
calls made after the property returns.

Bundled Redis data commands are not automatically replayed after an
outcome-ambiguous connection, write, or response failure. Redis may have
committed a push, pop, set mutation, or delete before its response was lost;
sending the command again could duplicate a push, consume another queued item,
extend a TTL, or change a boolean mutation result. The explicit zero-replay
policy discards the failed connection but cannot roll back or reveal the first
attempt's outcome. Protocol continuations after the server explicitly reports
that execution did not occur, such as NOSCRIPT or Cluster MOVED/ASK/TRYAGAIN,
remain available. Do not blindly repeat an ambiguous push or pop; reconcile or
deduplicate at the application boundary.

redis-py uses the same Cluster outer-retry count for transport failures and
ClusterDown/SlotNotCovered responses. Setting that count to zero therefore
also makes those two topology failures fail fast; MOVED/ASK/TRYAGAIN routing
uses the separately bounded protocol continuation budget above. A slot missing
during initial target selection is not guaranteed to refresh topology on a
later ordinary command. After a typed topology failure, explicitly
`disconnect()` / `connect()` to build a fresh generation; reconcile an
outcome-ambiguous mutation before repeating it.

`SCRAPY_REDIS_RETRY_ON_TIMEOUT` remains accepted with its historical default
for configuration compatibility, but is deprecated and neither value enables
data-plane replay; remove it from new configurations. Explicit use emits a
`FutureWarning` when the backend is constructed. The separate
`SCRAPY_REDIS_SENTINEL_RETRY_ON_TIMEOUT` setting applies only to read-only
Sentinel discovery: `True` permits at most one immediate SDK retry after a
timeout for each control request. This per-request Retry policy does not retry
authentication failures, although Sentinel may continue to another configured
endpoint. The ConnectionManager's connection-attempt policy remains separate,
and the discovered Redis master still uses the zero-replay data policy.

In Sentinel mode, `SCRAPY_REDIS_SSL_ENABLED=True` secures both Sentinel
discovery and the discovered master. Configure `SCRAPY_REDIS_SSL_CAFILE`; when
using mTLS, provide `SCRAPY_REDIS_SSL_CERTFILE` and
`SCRAPY_REDIS_SSL_KEYFILE` together. Hostname verification remains enabled by
default. This is one transport policy—there is no silent plaintext Sentinel
control-plane fallback. Supplying any CA/certificate/key path while TLS is
disabled is rejected rather than silently opening a plaintext connection.
`sentinel_username` / `sentinel_password` authenticate only the discovery
plane; `username` / `password` authenticate only the discovered data plane,
with no fallback between them. With S Sentinel addresses, `max_connections` is
a per-pool cap across S control pools plus one data pool, not a generation-wide
total. An unset limit is normalized to the redis-py 7.3 effectively-unbounded
value (`2**31`) so upgrading to redis-py 8 does not silently introduce its new
100-connection default.

Bundled queue and storage APIs remain byte-oriented. If
`SCRAPY_REDIS_DECODE_RESPONSES=True`, redis-py receives `surrogateescape` and
the backend losslessly converts decoded strings back to bytes, including
non-UTF-8 payloads consumed by atomic Lua pop. Public typed operation errors do
not copy redis-py exception or response text; the original data-plane SDK
exception remains available as `__cause__` for protected diagnostics.

Authenticated plaintext is preserved only for direct, literal-loopback
standalone Redis and MongoDB development connections. This narrow compatibility
path excludes Redis Sentinel/Cluster and MongoDB replica-set/sharded/Atlas
modes, even when a discovery seed is loopback. Remote authenticated Redis and
MongoDB connections require verified TLS; `ALLOW_REMOTE_PLAINTEXT` authorizes
only remote anonymous plaintext on an explicitly trusted network and never
weakens credential transport requirements.

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
`SCRAPY_MONGO_QUEUE_COLLECTION`, `SCRAPY_MONGO_SET_COLLECTION`, and
`SCRAPY_MONGO_STORAGE_COLLECTION` must name three distinct physical
collections. A storage-wide clear deletes every non-marker document, so a
local collision is rejected at construction and again before client I/O. Each
collection also carries a reserved `scrapy-extension:capability-domain:v1`
ownership marker; this rejects cross-component or cross-process attempts to
reuse the same physical collection for another capability. Replica-set,
sharded, and Atlas marker claims use isolated primary/majority read and
majority write concerns even when ordinary business writes use `w=1`.

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

**Logical queues and physical topics.** The queue name is the stable logical
identity; Kafka receives a resolved physical topic. `SCRAPY_KAFKA_TOPIC_NAME_GENERATION`
defaults to `auto`: safe historical names continue to use
`scrapy-<queue>`, while names containing `:`, Unicode, or exceeding Kafka's
limit use one deterministic `scrapyext-v2-<digest>` topic. `v2` selects the
hashed mapping for every queue and `legacy` is a migration-only mode that
rejects identities the old direct mapping could not represent. The resolver is
used consistently by create, send, pop, depth, and maintenance paths; it never
dual-reads or silently migrates a backlog. Drain the old topic before switching
an existing queue to `v2`.

A Kafka operation leases its producer/admin/consumer generation until the SDK
call returns. `disconnect()` waits for admitted work before closing opaque
handles, and reconnects fence old delivery tokens. A send timeout or transport
loss after `send()` is a `QueueOutcomeIndeterminateError`: no success receipt is
issued, retry is at-least-once and may duplicate, and callers should reconcile
by their own idempotency key.

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

Each connected generation uses the configured retry policy for safe reads and
control-plane setup, but a shared-transport `client.options(...)` view disables
all automatic retries for queue, set, and storage mutations. This prevents an
SDK replay from silently applying a committed mutation twice; it does **not**
provide exactly-once semantics. If a mutation response is lost or malformed,
the backend raises a typed `QueueOutcomeIndeterminateError`,
`SetOutcomeIndeterminateError`, or `StorageOutcomeIndeterminateError`. Callers
must reconcile by stable document/domain identity before deciding whether to
retry. Search, count, and single-document responses fail closed on shard
failures, partial results, or malformed acknowledgements. Delete-by-query has
no documented top-level `_shards` block; its synchronous response instead must
prove no timeout, failures, version conflicts, or delete-count mismatch.

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
> returns a message body **without** acking; the scheduler settles the token
> when Scrapy emits `response_received` or `spider_error` (download-level). A
> crash before settlement → the broker's invisible-duration window redelivers
> (at-least-once, not exactly-once).
> The lease is not auto-renewed: configure `INVISIBLE_DURATION` above the
> maximum expected pop-to-downloader-response time. Explicit nack shortens the
> lease to RocketMQ's 10-second minimum.
> A delivery token has one terminal outcome: concurrent ack/nack calls are
> serialized, token-aware pops cannot be settled through the legacy slot, and
> only a failed broker RPC leaves the token locally retryable.
>
> **Direct receive API restriction:** one connected RocketMQ consumer generation
> can select exactly one logical queue/topic. Calling `pop()` or `pop_with_ack()`
> with a different queue on that generation raises `QueueError`. To migrate,
> stop its waiters, call `disconnect()`, call `connect()`, and make the first pop
> on the replacement queue; tokens from the retired generation must not be
> reused. A receive-pump failure is likewise terminal and visible to every waiter
> until this disconnect/reconnect migration creates a fresh generation. Producer
> sends and token settlements lease their exact RocketMQ generation until the SDK
> call returns; a caller timeout does not release that lease. The bundled client
> still cannot prove whether a lost send response committed, so reconcile and
> retry conservatively.

### Pulsar (standalone, cluster)

```python
SCRAPY_BACKEND_TYPE = "pulsar"
SCRAPY_PULSAR_SERVICE_URL = "pulsar://localhost:6650"
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
Clusters use one scheme followed by comma-separated `host[:port]` members;
ports must be numeric and in range, and IPv6 members must be bracketed.

Pulsar producer sends and per-message token settlements retain the client
connection generation until the synchronous SDK call returns. Receive pumps
have a separate stop/close fence so disconnect can interrupt a blocked receive;
late deliveries from a retired pump are discarded rather than published into a
replacement generation. The sync client does not provide a durable send receipt
contract for response-loss reconciliation, so real-broker deployments must use
idempotent payloads.

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

Each SQS generation also owns a private `boto3.session.Session` and low-level
client. This avoids concurrent use and cross-generation credential caching in
boto3's process-wide default Session. Botocore's ambient credential provider
chain—credential environment variables (including a session token),
`AWS_PROFILE`-selected shared files, and IAM/workload identities—is resolved
independently for every new generation. Region always comes from
`SCRAPY_SQS_REGION_NAME`, a custom endpoint URL can come only from
`SCRAPY_SQS_ENDPOINT_URL`, and an explicit SQS access/secret pair overrides
ambient credentials. `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_SQS`, and
shared-config custom endpoints are ignored so they cannot bypass cloud-mode
HTTPS validation. Botocore FIPS/dual-stack endpoint selection remains
available when the SQS endpoint is unset. A custom
`boto3.setup_default_session(...)` or event hook attached only to that global
Session is intentionally not inherited; see the
[migration guide](https://github.com/azwpayne/scrapy-extension/blob/main/docs/migration-guide.md#sqs-private-boto3-sessions).

### Memcached (standalone, NoSQL KV)

```python
SCRAPY_BACKEND_TYPE = "memcached"
SCRAPY_MEMCACHED_HOST = "127.0.0.1"
SCRAPY_MEMCACHED_PORT = 11211
# Required only for a non-loopback server on an isolated trusted network:
# SCRAPY_MEMCACHED_ALLOW_REMOTE_PLAINTEXT = True
# SCRAPY_MEMCACHED_ALLOW_FLUSH_ALL = True  # dedicated servers only
```

Memcached traffic in this backend is unauthenticated and unencrypted. The
default is the explicit IPv4 loopback (`127.0.0.1`) so IPv6-first `localhost`
resolution cannot miss an IPv4-only local daemon. Loopback hosts work by
default; any remote host fails at startup unless
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
enable a flush, and the server must return an explicit successful reply. The
supported `pymemcache>=4,<5` range currently resolves to the unmaintained 4.0.0
release; treat that as an explicit supply-chain caveat when approving this
backend.

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

The item schema is a string partition key `pk`, binary `value`, optional numeric
`expire_at`, and package-reserved string `_scrapy_revision`. Every `store()`
generates `_scrapy_revision` with `uuid.uuid4().hex`; its required stored grammar
is exactly 32 lowercase hexadecimal characters, and it is included in the 400
KiB size check. Direct table writers that replace package rows must set
`_scrapy_revision` to a freshly generated `uuid.uuid4().hex` on every
replacement; copying a stale revision defeats replacement detection.

Region and custom endpoint URLs are authoritative backend settings. Ambient
`AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_DYNAMODB`, and shared-config custom
endpoints are ignored, so they cannot redirect cloud mode around its HTTPS
guard. Botocore's credential provider chain and normal FIPS/dual-stack endpoint
selection are unchanged.

`clear_storage()` issues one conditional `DeleteItem(ALL_OLD)` per observed
revision-fenced row, using the exact `_scrapy_revision` returned by the strongly
consistent Scan and validating the returned old-item identity. It never uses
key-only BatchWrite deletes. A revisionless legacy row fails closed and remains
present: an identical-value delete/recreate (ABA) cannot be distinguished from
the scanned row. Migrate such rows while writers are stopped, or enable the
high-risk `SCRAPY_DYNAMODB_ALLOW_UNFENCED_LEGACY_CLEAR=True` override for one
quiesced maintenance generation. The override conditionally deletes legacy
rows without enlarging them, including pre-existing 400 KiB items, but cannot
make identical-value ABA safe. Disable it and reconnect before resuming writers.
SDK failures, condition loss, and malformed responses have an explicit
partial-result `StorageError` contract; accepted earlier deletes are not rolled
back. Scan is not a cross-page snapshot, so even normal revision fencing cannot
exclude newly inserted, unobserved rows.

`delete()` returns `False` only when `DeleteItem(ALL_OLD)` omits `Attributes`,
which is AWS's missing-item result. A present old item must contain the exact
string partition key requested; malformed proxy, emulator, or SDK responses
raise typed `StorageError` rather than being mistaken for deletion success.

See the [examples directory](https://github.com/azwpayne/scrapy-extension/tree/main/examples) for representative spiders and deployment-mode recipes (Sentinel, Cluster, Atlas, Confluent, etc).

## Backend Capabilities

| Backend       | Queue | Set | Storage | Modes                                        |
|---------------|-------|-----|---------|----------------------------------------------|
| Redis         | Yes   | Yes | Yes     | standalone, sentinel, cluster; deprecated primary-only `master_slave` alias |
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

**Kafka clear limitation**: `clear_queue()` raises `QueueError`.
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
| Queue | `delay` | Per-process | In-process `heapq`; clean-close state snapshots use the queue backend's storage capability, or the configured storage component when the queue is queue-only. Hard crashes lose state since the last clean checkpoint. In multi-worker deployments set a stable, unique `SCRAPY_QUEUE_SNAPSHOT_OWNER` (or `SCRAPY_QUEUE_WORKER_ID` fallback) to prevent workers from sharing a snapshot key. |
| Queue | `round_robin` | Per-process | Fair dispatch across `request.meta['source']` using a per-worker index. |
| Queue | `throttle` | Per-process | Effective rate under N workers = `N × (1 / min_interval)`. |
| Queue | `priority` | Yes | Items live in backend-side priority buckets; Kafka and RocketMQ are rejected because their consumers cannot isolate a scan across strategy-created topics. |
| Queue | `time_wheel` | Per-process | Timing wheel and overflow heap are local; the same snapshot capability and owner requirements as `delay` apply. |
| Queue | `work_stealing` | Yes, with explicit topology | Worker queues live in the backend; use stable worker IDs and a complete peer list. Kafka and RocketMQ are rejected. |
| Queue | `ring_buffer` | Per-process | The bounded in-process buffer is the queue; the backend is intentionally bypassed. |
| Dedup | `set` (default) | Yes — exact stored membership | The bundled scheduler checks membership, durably pushes, then publishes the backend marker. Two workers concurrently observing an absent marker may both enqueue; after publication, backend membership is exact. |
| Dedup | `memory` | Per-process | In-process; optional LRU cap via `SCRAPY_DEDUP_MEMORY_MAXSIZE` (default 1,000,000; round-9 U5). |
| Dedup | `bloom` | Per-process | Pure-stdlib bit-vector; **never produces false negatives** (a seen URL is always reported seen); false-positive rate is configurable. |
| Dedup | `cuckoo` | Per-process | Pure-stdlib; successful inserts **never produce false negatives**; item-level deletion is fail-safe unsupported because fingerprints can collide; failed-push compensation uses one bounded retry allowance; raises `FilterFull` at capacity (degrades to passthrough + warn-once). |
| Storage | all storage-capable backends | Yes | Via the `StorageBackend` KV+TTL contract. |

**Defaults provide distributed, crash-safe at-least-once scheduler admission.**
`set` dedup + `passthrough` queue are safe for multi-worker crawls out of the
box: a failed or crashed push cannot leave a persistent marker for work no queue
accepted. This does not promise a cross-worker single winner for a brand-new
fingerprint; concurrent misses may enqueue safe replay. `delay` / `throttle` /
`round_robin` / `time_wheel` / `ring_buffer` / `memory` / `bloom` / `cuckoo`
are **per-process opt-in**. `priority` and correctly configured
`work_stealing` retain backend-side payload durability. This scheduler-admission
guarantee is not an item-pipeline at-least-once guarantee; pipeline ACK timing
and batched-storage crash loss remain separate contracts below.

### Queue telemetry freshness

Queue-depth monitoring is sampled only while a non-zero cached depth is in its
sampling window: `SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY` bounds real `queue_len()`
probes in that active state. An unknown or zero cache is probed on every call so
drain/idle detection stays fresh. A confirmed pop decrements a cached non-zero
depth immediately. A failed pop never decrements local depth, although a
scheduled depth sample on that attempt may refresh the cache from the backend.
Every pop attempt, including empty polls and backend failures, updates
`queue/last_pop_epoch` and increments `queue/pop_attempt_count`.
`ScrapyStatsMonitor` emits the event-sampled rate at
`queue/pop_rate_1m` for the default 60-second window and at
`queue/pop_rate_{N}s` for an overridden window. The rate gauge may freeze when
pop events stop; use the last-pop epoch for freshness.

### Contractual promises

| Promise | Where enforced |
|---|---|
| **Fail-fast configuration.** Unknown nested fields and typoed flat `SCRAPY_<BACKEND>_*` names raise `ConfigurationError` with a suggestion. Project cross-field/capability checks also raise `ConfigurationError`; Pydantic type/range/enum validation raises `ValidationError`. `ConfigurationError.setting_name` / `.setting_value` are Stable attributes, and sensitive setting names are redacted. | `backends.connectors.resolve_backend_config`, backend settings models; see [STABILITY.md](https://github.com/azwpayne/scrapy-extension/blob/main/.github/STABILITY.md) |
| **Structured credential redaction.** Password/token/API-key fields use `SecretStr`; selected SDK-bound values are additionally wrapped in a repr-redacting `str` subclass. `JSONSerializer` rejects `SecretStr` and `SecretBytes` so they cannot silently become queue/storage payloads. Persistence is possible only after the caller explicitly unwraps to an ordinary string or bytes value (or encrypts to another serializable value); that resulting data is caller-owned. | backend settings models, `backends._redaction`, `backends.base.JSONSerializer` |
| **No code execution on the data path.** Serialization is JSON only — never `pickle`, never `eval`. Unknown types raise `TypeError` instead of being silently `str()`-ed. | `backends.base.JSONSerializer` |
| **Input names are validated.** Queue / set / index / topic names match the documented safe subsets; injection-shaped inputs are rejected before use. | `backends.base._validate_key_name` and backend topic validators |
| **Ack correctness under `CONCURRENT_REQUESTS > 1`.** Deferred-ack backends (Kafka, RabbitMQ, RocketMQ, Pulsar, SQS) carry a per-message ack token so the *specific* popped message is acked. Kafka additionally fences tokens by consumer generation, assignment epoch, and unique delivery attempt, preventing a late completion from committing a same-offset redelivery after nack/rebalance. Retry/redirect replacements transfer the token through their queue commit; user errbacks returning one or many requests use child tokens and settle the source only after every replacement is accepted. The scheduler's `from_settings` gate refuses a backend/plugin that declares single-slot ack unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS` is set. | `backends/base.py` (`QueueBackend` ack contract), `backends/kafka.py`, `schedule/scheduler.py` |
| **Queue-before-marker publication.** On the bundled atomic scheduler/dupefilter path, a persistent dedup marker is published only after a crash-durable queue push. Failed pushes discard local intent without deleting a competing worker's marker. Volatile queue strategies use a bounded lifecycle-local shadow; broker-token replacements are rejected before volatile acceptance. | `schedule/scheduler.py`, `dupefilter/dupefilter.py`, `queue/queue.py` |
| **DupeFilter lifecycle admission.** `BackendDupeFilter` transitions `NEW → OPENING → OPEN ⇄ CLEARING → CLOSING → CLOSED`; only `NEW`/`OPEN` admit requests. Clear and close stop admission before waiting for admitted filter calls, and old-epoch commit/forget receipts cannot mutate a replacement generation. Filter and monitor callbacks run outside the lifecycle lock. A temporary lifecycle-unavailable decision is a `QueueError`/`RuntimeError` and the scheduler preserves the request through its enqueue fallback. | `dupefilter/dupefilter.py`, `schedule/scheduler.py` |
| **Lazy optional deps.** The core distribution installs with **zero** backend deps. Each backend's optional dep loads on first access via PEP 562, with `ImportError` install hints. | package and backends `__getattr__` implementations |
| **Probabilistic dedup never false-negatives.** Bloom and Cuckoo may produce false positives (a fresh URL reported as "seen"); they will never let a seen URL through as fresh. | `dupefilter/filters/bloom_filter.py`, `dupefilter/filters/cuckoo_filter.py` |
| **Backend capability honesty.** A backend never silently no-ops on an unsupported interface: queue-only backends omit `SetBackend`/`StorageBackend` entirely; RocketMQ set/storage are rejected at config time (`ConfigurationError` guard). The matrix above is the contract. | `backends/base.py` ABCs; `backends/connectors.py` capability gates |
| **`py.typed` marker shipped.** Full type annotations on the public surface; downstream type-checkers consume the shipped typing. | `scrapy_extension/py.typed` in the wheel |

### What is **not** promised

- **Cross-worker behavior of `delay` / `throttle` / `round_robin` / `time_wheel` / `ring_buffer` / `memory` / `bloom` / `cuckoo` strategies** — they are per-process by design (see table above).
- **Cross-worker single-winner enqueue for a new fingerprint.** The safe order
  is membership read → durable queue push → marker publication, so concurrent
  workers may both enqueue before either marker exists. Consumers must tolerate
  at-least-once replay.
- **Post-queue marker safety for legacy boolean-only dupefilters.** The guarantee
  applies to the bundled atomic `BackendScheduler` + `BackendDupeFilter` path
  (and explicit implementations of that extension). Compatibility fallback to
  `request_seen` / `consume_reservation` / `forget` retains its historical
  add-before-push, best-effort rollback behavior.
- **Stability of the entry-point registration API** (`BackendDescriptor`) — round-5 surface, no 3rd-party ecosystem yet; expect possible minor-bump changes. See [STABILITY.md](https://github.com/azwpayne/scrapy-extension/blob/main/.github/STABILITY.md).
- **Stability of fresh hooks** — `on_filter_full` (round-7) and `backpressure_pause_at` / `backpressure_resume_at` (round-4) are new; the hook signatures and setting semantics may evolve in a minor bump.
- **Wire compatibility for the SQS / Memcached / DynamoDB LocalStack paths** — exercised via LocalStack in CI; not certified against every AWS region or Memcached server version.

For the full stability/maturity tiering per backend, see [STABILITY.md](https://github.com/azwpayne/scrapy-extension/blob/main/.github/STABILITY.md). To report a security issue, see [SECURITY.md](https://github.com/azwpayne/scrapy-extension/blob/main/.github/SECURITY.md). For what changed in each release, see [CHANGELOG.md](https://github.com/azwpayne/scrapy-extension/blob/main/.github/CHANGELOG.md).

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
| `cuckoo` | no (FP) | no | no (clear only) | pure-stdlib; `SCRAPY_DEDUP_CUCKOO_CAPACITY` / `_ERROR_RATE` |

Probabilistic filters never produce false negatives; in-memory filters are
per-process (single-worker). Use `set` for multi-worker exact dedup. If item-level removal is required, choose the exact `memory` or `set` strategy; Cuckoo supports only `clear()` for a whole-filter reset.

A Bloom filter tests all supported hash counts from 1 through the 64-hash work
cap, chooses the smallest valid bit vector (preferring fewer hashes on ties),
and limits that vector to a conservative 128 MiB per instance.
Capacity/error-rate combinations that require more fail during configuration
before allocation.

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

Push durability is operation-bound. `BackendQueue` freezes the selected
strategy route before serialization, then obtains a receipt from the exact
backend/breaker generation that performs the push. Only a literal receipt
value of `True` permits a persistent dedup marker or source-token ACK. Unknown
custom queues, older third-party backends, and custom `QueueStrategy`
implementations remain conservatively volatile. The legacy
`is_push_durable(*, delay, source)` hook is retained for compatibility but is
not commit evidence; overriding it alone no longer opts a route into durable
publication. Ordinary volatile pushes remain usable through the lifecycle-local
dedup shadow, while source-token transfers fail closed before local mutation or
remain unacked after a backend policy rejection.

`priority` and `work_stealing` fan out one logical queue into multiple physical
queues. Their default `v2` names are versioned, length-delimited digests of the
complete strategy namespace, logical queue, and bucket/worker identity. This
keeps generated queues distinct from one another and from a literal
`passthrough` queue such as `jobs:p0`; Kafka-invalid separators and backend name
limits are avoided as well. They fail fast with Kafka and RocketMQ because those
backends cannot isolate a pop to one strategy-selected topic.

There is no automatic dual-read of old colon-delimited fan-out queues. For a
one-time migration, stop all writers, drain the old backlog with
`SCRAPY_QUEUE_NAME_GENERATION="legacy_v1"` — the validated settings knob that
selects the legacy colon layout (default `v2`; invalid values fail fast) —
stop those drainers, then switch the whole worker group back to the default
`v2` generation. Never run both generations concurrently. Backend-delegating
strategies (`passthrough`, `delay`, `throttle`, `priority`, `time_wheel`, and
`work_stealing`) preserve MQ ack tokens where supported. `round_robin` and
`ring_buffer` are fully local and intentionally bypass broker durability.
With `ring_buffer`'s explicitly lossy `drop_oldest` policy, an overwritten
request remains in the lifecycle-local dedup shadow until that bounded shadow
evicts it or the queue lifecycle ends; the drop is therefore terminal for that
worker lifecycle rather than an automatic retry signal.

`delay`, `round_robin`, `time_wheel`, and `ring_buffer` implement clean-close
snapshots. Redis, MongoDB, and Elasticsearch queues use their own storage
capability. For a queue-only backend, set `SCRAPY_STORAGE_BACKEND_TYPE` (and
its settings) to a storage-capable backend; the scheduler independently uses
that manager for snapshots even when no item pipeline is enabled. A legacy
queue-only global configuration with no storage component may still start and
may close empty local state, but a nonempty stateful strategy fails clean close
rather than silently discarding held work; use explicit `close(lossy=True)` only
when that loss is intended. In a multi-worker deployment, configure a stable unique
`SCRAPY_QUEUE_SNAPSHOT_OWNER` per worker; when omitted,
`SCRAPY_QUEUE_WORKER_ID` is the fallback. A restored checkpoint remains stored
until the next clean close replaces it with current state or deletes it after a
clean drain. A crash after restore can therefore replay already-processed
entries, but cannot lose the only copy of entries not yet processed. If a
current-format manifest or chunk cannot be read, persistence is fenced rather
than replacing that checkpoint with a clean-start snapshot; after verifying the
intended in-memory state, call `BackendQueue.reset_snapshot()` (or use the
explicit lossy abort) before teardown. v7 orphan chunks are never reclaimed
automatically: use the explicit, quiescent `SnapshotRepository.gc()` operation
only with a backend that advertises bounded key listing; unsupported storage
surfaces a maintenance-unavailable error.

### Storage strategy — `SCRAPY_STORAGE_STRATEGY`

Storage strategy names are case-sensitive and must use the documented lowercase
values (`passthrough` or `batched`). `BackendPipeline` delegates item writes to a
`StorageStrategy`:

| Strategy | Behavior | Durability note |
|----------|----------|-----------------|
| `passthrough` (default) | writes each serialized item directly to the selected `StorageBackend` | backend durability applies immediately after `store()` returns |
| `batched` | buffers backend-bound records and flushes them in global insertion order at threshold / spider close | every record retains the exact backend passed to its `store()` call through age/manual/close drains and partial-failure retry; accepted work is capped across the retry buffer and in-flight snapshot (default: `2 × threshold`), and a full strategy raises `StorageBackpressureError` before accepting another item. One `BatchedStorageStrategy` has one lifecycle owner; a distinct second pipeline attachment is rejected. A hard crash before or during flush can lose buffered or in-flight work. |

Use `passthrough` when item loss is unacceptable. Use `batched` only when throughput is worth the crash-before-flush trade-off and duplicate writes are acceptable after partial flush retry.

`SCRAPY_PIPELINE_MAX_STORAGE_ERRORS` defaults to **10**: the eleventh
consecutive storage failure raises `BackendError` instead of being reported as a
successful item. Set it explicitly to `None` only when best-effort loss is an
intentional policy; the public `Settings` model, `BackendPipeline` factory, and
runtime constructor use the same default.

### Queue and dupefilter isolation

The Scrapy factories default to `scheduler-queue:{project}:{spider}` and
`dupefilter:{project}:{spider}`. `{project}` is Scrapy's `BOT_NAME` (or
`default` when no crawler settings exist), and `{spider}` is the spider name.
This prevents two spiders or projects sharing a backend from consuming each
other's requests or fingerprints. Requests written by current versions also
carry these identities in their serialized envelope; a mismatched envelope is
nacked (or re-published for an atomic-pop backend) rather than acknowledged as
poison data.

For a deliberate legacy/shared-key migration, set explicit literal
`SCRAPY_QUEUE_KEY` and `SCRAPY_DUPEFILTER_KEY` values. Legacy envelopes without
identity tags remain readable. To intentionally let current envelopes cross
spider boundaries, additionally set `SCRAPY_QUEUE_ALLOW_CROSS_SPIDER = True`;
use this only with an application-level routing contract. Prefer draining the
old shared queue and re-enqueueing into the new namespaced keys.

Set `SCRAPY_STORAGE_BUFFER_MAX_PENDING` to an integer no smaller than the
batch threshold when a tighter or larger live backlog is appropriate. The cap
counts both records still buffered and records detached for a currently blocked
backend flush, so it prevents unbounded process memory growth during an outage.

Direct `BatchedStorageStrategy.flush()` is a truthful durability operation: if
another flush still owns the drain lock after the bounded wait, it raises
`StorageError` and retains pending work instead of returning successfully.
`close()` likewise returns only after its drain completes. If a strategy or
`BackendPipeline.close_spider()` call raises before that barrier, the pipeline
enters a retry-only closing state: `process_item()` and `open_spider()` reject
new admissions, the connection manager remains owned, and the caller **must
retry close** until it succeeds. A successful retry drains the retained tail
before releasing the manager.

### Ack and durability matrix

| Surface | Ack / state boundary | Crash behavior | Operational guidance |
|---|---|---|---|
| Redis / MongoDB / ElasticSearch queue pop | atomic backend pop; scheduler ack is inert; Redis does not replay an outcome-ambiguous transport failure | item is removed once popped; a later callback/pipeline crash can lose downstream item work. A lost Redis response may hide one already-consumed item, but the SDK will not consume a second item in the same call | pair with idempotent callbacks/pipelines when end-to-end exactly-once matters; reconcile an ambiguous Redis failure before another pop |
| Kafka / RabbitMQ / Pulsar queue pop | per-message token stored in request meta and acked on Scrapy `response_received` or `spider_error` (download-level) | crash before ack redelivers; crash after downloader response but before callback/pipeline completion can drop downstream processing | safe under `CONCURRENT_REQUESTS > 1`; RabbitMQ push waits for publisher confirmation, but a durable receipt additionally requires `durable=True`, `auto_delete=False`, `exclusive=False`, and `delivery_mode=2` on the connected generation |
| SQS / RocketMQ queue pop | per-message token plus a finite broker visibility/invisibility lease; settlement remains tied to Scrapy `response_received` or `spider_error` | an unacked message becomes deliverable again when the lease expires, including while a slow download is still running | no automatic lease renewal; set the lease above maximum pop-to-response time. SQS nack is immediate; RocketMQ nack uses its 10-second floor |
| Backend/plugin declaring `supports_concurrent_ack=False` | single ack slot only | `CONCURRENT_REQUESTS > 1` raises at startup unless `SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=True` | keep `CONCURRENT_REQUESTS=1` for such backends, or choose one with a real in-flight ack set |
| Stateful queue strategies | in-process scheduling/fairness/rate/buffer state, with best-effort snapshot only where implemented | hard crash can lose held strategy state even if backend queue survives; a token-bearing replacement is rejected before entering volatile delay/time-wheel/round-robin/ring-buffer state | use a backend-durable push path (`passthrough`, `priority`, `work_stealing`, `throttle`, or zero effective delay) when replacing an unacked broker delivery |
| `batched` storage | backend-bound in-process item buffer before backend `store()` | hard crash before flush loses buffered items and a crash during a detached in-flight flush can lose that work; partial store exceptions retry the backend-bound unwritten tail in global FIFO order; a second pipeline owner is rejected | keep every caller-provided backend alive until drain; prefer `passthrough` when persistence must happen before item acknowledgement |

Within a live TimeWheel drain, each slot entry remains owned until its backend
push returns. A failure keeps the failing item and untouched tail in their
original order, while a confirmed prefix is removed. If the backend accepts an
item and a process-control signal arrives before local removal, retry may
publish that one item again; consumers must retain at-least-once idempotence.

Ack is tied to Scrapy downloader response delivery — `response_received` or
`spider_error` — not spider callback or item pipeline completion. If a crawl must
tolerate process death after response download but before item persistence, make
item processing idempotent and use a durable storage strategy/topology; do not
read the queue ACK as a pipeline-level at-least-once guarantee.

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
  base is `SCRAPY_RETRY_DELAY` (default 1 second). On scheduler synchronous
  paths, each retry wait is additionally capped by `SCRAPY_REACTOR_IO_TIMEOUT`.
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
signals exactly once. Do not add a second `from_crawler()`; calling
`setup_backend()` from `__init__` is tolerated—setup is idempotent and
`from_crawler()` re-finalizes it once the crawler is attached—but it is not
required. Only code that constructs `MySpider()` directly, outside Scrapy's
factory, must call `setup_backend()` before using the low-level backend
properties and must later call `close_backend()`.

The class attributes configure the mixin's direct manager. `get_queue()` and
`get_scheduler()` reuse the standard scheduler/dupefilter factories with that
manager borrowed, so queue strategy, ACK-concurrency safety, monitor/stats,
snapshot owner, dedup strategy, and queue limits still come from Scrapy
settings. `close_backend()` releases the manager only after every component and
snapshot lease succeeds; a surfaced close failure is retryable by calling it
again. The mixin has no implicit lossy abort—use the explicit
`BackendQueue.close(lossy=True)` or `BackendScheduler.abort(reason)` APIs when
that lower-level ownership is available.

## Scrapy Components

| Component    | Class                | Purpose                                          |
|--------------|----------------------|--------------------------------------------------|
| Scheduler    | `BackendScheduler`   | Distributes requests across spider instances      |
| DupeFilter   | `BackendDupeFilter`  | Filters duplicate requests using `SetBackend`     |
| Pipeline     | `BackendPipeline`    | Stores scraped items using `StorageBackend`       |
| Queue        | `BackendQueue`       | Serializes/deserializes Scrapy requests           |
| SpiderMixin  | `BackendSpiderMixin` | Convenient access to backend components           |

Factory availability is component-specific:

| Component | Supported construction entry points |
|---|---|
| `BackendScheduler` | `from_settings()` and `from_crawler()` |
| `BackendDupeFilter` | `from_settings()` and `from_crawler()` |
| `BackendPipeline` | `from_settings()` and `from_crawler()` |
| `BackendQueue` | Direct constructor only; optional keyword-only `spider=None` |
| `BackendSpiderMixin` | `from_crawler()` through Scrapy, or direct construction followed by explicit `setup_backend()` |

### Public API tiers

This concise symbol guide does not add or remove exports; the complete inventory
and compatibility rules are in the [stability policy](https://github.com/azwpayne/scrapy-extension/blob/main/.github/STABILITY.md).

| Tier | Representative symbols | Compatibility contract |
|---|---|---|
| Stable | Core components above; `Backend`, `QueueBackend`, `SetBackend`, `StorageBackend`; `ConnectionManager`; `BackendType`, `Serializer`, `JSONSerializer`; root-exported exceptions | No intentional patch-release break. Before 1.0, an unavoidable break requires a minor release, changelog entry, and migration guidance. |
| Experimental | `BackendDescriptor` plugin registration; strategies, hooks, and settings explicitly marked Experimental in the stability policy | May change in a pre-1.0 minor release with notice. |
| Private (Internal) | `_`-prefixed symbols and unlisted implementation helpers | No compatibility promise; do not import directly. |

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

Lifecycle admission is explicit: `NEW` and `OPEN` accept `request_seen()`;
`OPENING`, `CLEARING`, `CLOSING`, and `CLOSED` reject new admission. `clear()`
and `close()` wait for filter calls already admitted before invoking opaque
filter/manager callbacks, and receipt epochs fence late `commit_reservation()`,
`rollback_reservation()`, and `forget()` calls. The scheduler treats a temporary
lifecycle rejection as a queue error and preserves the request rather than
silently dropping it. Direct pre-open `request_seen()` remains supported.
When used from a running reactor, scheduler-owned `open()`/`clear()`/`release()`
and mixin-owned direct dupefilter close run through authoritative worker
Deferreds;
`BackendSpiderMixin.get_queue_async()` is the off-reactor companion for queue
construction. Synchronous direct callers retain the existing APIs.

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
│   └── StorageBackpressureError — item was not admitted to a full batched buffer
├── SerializationError       — serialization failures (includes serializer)
└── ConfigurationError       — invalid settings (includes setting_name, setting_value)
```

Project exceptions expose the context shown above where applicable. Pydantic
schema failures are `pydantic.ValidationError`, not `BackendError`, while
project cross-field, capability, and unknown-setting failures use
`ConfigurationError`.

### Serialization error privacy migration

Public `BackendQueue.push()`, scheduler-facing queue push, `BackendQueue.pop()`,
and `BackendPipeline.process_item()` rebuild terminal `SerializationError`
instances with fixed operation text, `data=None`, `serializer="json"`, and no
exception chain. This prevents request URLs/headers/bodies, broker payloads,
items, and serializer diagnostics from reaching application error handlers or
monitor extensions. Treat `.data` as unavailable at these terminal boundaries;
private serializer helpers may still use it for local debugging.

## Examples

See the [examples guide](https://github.com/azwpayne/scrapy-extension/blob/main/examples/README.md) for representative spiders and recipes. Some shipped backends are documented as settings recipes rather than dedicated spiders:

| Spider | Backend | Features Demonstrated |
|--------|---------|----------------------|
| `quotes_redis` | Redis | Full queue + set + storage |
| `quotes_mongodb` | MongoDB | Document storage, replica set |
| `quotes_kafka` | Kafka | High-throughput streaming (queue only) |
| `quotes_rabbitmq` | RabbitMQ | Priority queues, HA (queue only) |
| `quotes_elasticsearch` | ElasticSearch | Full-text search storage |
| `quotes_multi_mode` | Redis | Sentinel and Cluster configurations |
| `quotes_connection_manager` | All | Direct `ConnectionManager` API |
| `quotes_programmatic` | Redis | Per-spider `backend_settings` dict |
| `quotes_crawl` | None | CrawlSpider variant |
| RocketMQ / Pulsar / SQS / Memcached / DynamoDB | recipes | Backend-specific settings; pair partial-capability backends with queue/set/storage-capable partners as needed |
| `quotes` | None | Basic Scrapy spider (no backend) |

## Security

- **Key name validation**: validated against `^[a-zA-Z0-9._:-]+$`
- **Topic name validation**: Kafka topics validated against `^[a-zA-Z0-9._-]+$`
- **Input sanitization**: all user-provided queue/set names validated before use
- **Redis endpoint isolation**: credentials belong in dedicated fields; node
  addresses reject URI/userinfo and normalize bracketed IPv6 before SDK use
- **No code execution**: JSON serialization only — never pickle or eval

JSON safety is not confidentiality. `JSONSerializer` rejects Pydantic
`SecretStr` and `SecretBytes` wrappers; it never unwraps them implicitly. A
caller can persist a secret only by explicitly calling `get_secret_value()` (or
otherwise converting/encrypting it) and passing the resulting ordinary
serializable value. Request metadata, request bodies, and scraped items may
still contain caller-owned secret-bearing strings or bytes. Use TLS for every
backend connection, least-privilege broker and database ACLs, and encryption at
rest. Do not place secrets in queued or stored payloads unless the application
encrypts them before handing them to the extension. Credentials embedded in
plain DSN/URI strings are caller-owned and must not be logged.

See the complete [security policy](https://github.com/azwpayne/scrapy-extension/blob/main/.github/SECURITY.md).

## Testing

```bash
# Run all tests
uv run pytest

# Run specific backend tests
uv run pytest tests/test_mongodb_backend.py

# Run with the same machine-readable coverage data used by CI
uv run pytest --cov=scrapy_extension --cov-report=term-missing \
  --cov-report=json:coverage.json

# Run full matrix (all Python versions)
uv run poe test
```

Test infrastructure includes pytest-xdist (parallel), pytest-randomly (randomized order), pytest-mock, pytest-cov, pytest-timeout, and pytest-socket (the default suite runs with sockets disabled). CI emits `coverage.json` and independently requires at least 95% statement coverage and 91% branch coverage; it does not report a misleading blended floor. Ruff, strict Mypy, and Bandit run as direct read-only gates through `uv run poe check`, rather than as pytest plugins.

`tests/test_reference_model_token_concurrency.py` and `tests/test_reference_model_list_queue.py` are collected under `-m reference_model`. They execute local token/set/list models and provide **no production backend evidence**. Run them with `uv run pytest -m reference_model`; do not cite them for Kafka, RabbitMQ, SQS, Redis, MongoDB, or `BackendQueue` load/concurrency claims. `tests/test_memory_membership_filter_scale.py` separately executes the production `MemoryMembershipFilter`, but still supplies no shared-backend or multi-process evidence.

Live integration tests require `SCRAPY_TEST_INTEGRATION=1`, the applicable backend environment variables, and an explicit loopback boundary, for example `uv run --no-sync pytest tests/integration -m integration --allow-hosts=localhost,127.0.0.1,::1`.

## License

MIT — see [LICENSE](https://github.com/azwpayne/scrapy-extension/blob/main/LICENSE).
