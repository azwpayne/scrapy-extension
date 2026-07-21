# Iterative hardening programme — 2026-07-21

This document is the executable result of successive eight-reader parallel
audits of architecture, backend correctness, runtime lifecycle, tests/CI,
public API and typing, security/configuration, release documentation, and
independent bug hunting. It is maintainer planning evidence, not a promise that
every item is already fixed. Public behaviour remains defined by `README.md`,
`STABILITY.md`, and the package API.

## Spec

### Goal

Make the repository truthful and safe at its declared boundaries:

1. a clean checkout can build and install both distribution formats;
2. unit tests are hermetic and do not pass only because collection order or a
   writable maintainer home directory masks missing dependencies;
3. every advertised Python version exercises the same behavioural contract;
4. Scrapy component hooks remain callable by the supported Scrapy 2.x range;
5. plugin and backend failures stay inside the package exception/capability
   contracts;
6. queue acknowledgement, reconnect, clear, and multi-spider lifecycle paths do
   not lose or silently duplicate accepted work;
7. cloud modes do not silently downgrade transport or authentication security.

### Architectural constraints

- Preserve the four deep ABC interfaces (`Backend`, `QueueBackend`,
  `SetBackend`, `StorageBackend`) and the descriptor registry. The audit found
  real leverage and stable seams there; replacing them would create churn.
- Prefer narrow compatibility adapters and invariant checks over new abstraction
  layers.
- Any behavioural fix starts with a regression test that demonstrates the old
  failure, unless the failure is a packaging or documentation-only invariant.
- Do not publish, tag, push, or change external systems as part of this
  programme. Each local iteration ends in one atomic Git commit.
- Do not deepen the Settings/component-factory seam or the monitor ownership
  model without an explicit design decision; both are real architectural
  concerns but have multiple valid product semantics.

### Verification baseline

At `fa1c005`:

- `uv build` fails because `project.license-files = ["LICENSE"]` no longer
  matches a file after five canonical release files were moved under `.github/`.
- With an isolated writable `HOME`, the default suite passes: 2,905 passed and
  44 skipped.
- Without that override, 58 RocketMQ tests fail while importing the real SDK,
  which tries to create `~/logs/rocketmq_python/rocketmq_client.log`.
- `ruff check src tests` and `mypy src` pass.
- Python 3.13 reaches one property-test failure caused by an incorrect test
  oracle for reserved request metadata.
- Python 3.14 stops during collection because a precise itemadapter/Pydantic-v1
  compatibility warning is promoted to an error.
- A clean test dependency group omits `boto3`, `pulsar-client`, and
  `pymemcache`; three public lazy-import tests fail when run before test modules
  that inject those names into `sys.modules`.

### Stop condition

The loop ends only when all of the following are true:

- no locally reproducible P0 or P1 item remains in the active task register;
- wheel and sdist build, contain their required metadata/assets, and the wheel
  installs and imports in a fresh environment;
- the declared CPython 3.10–3.14 test matrix, lint, and strict typing gates pass
  (free-threaded 3.14 remains an interpreter-compatibility lane as documented);
- unit tests do not write outside their temporary workspace and do not depend on
  collection order to supply optional dependencies;
- a final fresh-eyes audit finds only documented backend limitations, live
  integration work that requires external brokers/cloud accounts, or P2 design
  choices whose semantics must be selected by the maintainer.

## Plan

Iterations are deliberately small enough to review and revert independently.
After each commit, rerun the narrow regression gate plus the cheapest relevant
global gates, then perform a fresh audit before selecting the next item.

| Iteration | Outcome | Primary acceptance gate |
|---|---|---|
| I0 | Record this verified audit, specification, plan, and task register | links and cited commands are reproducible |
| I1 | Restore canonical release files to repository root | build/install wheel and sdist; licence metadata present |
| I2 | Make the clean test environment honest | dependency-group sync plus order-independent lazy-import tests |
| I3 | Make RocketMQ unit tests hermetic | no real SDK logger writes to user home; retain isolated SDK surface smoke |
| I4 | Repair supported-Python test contracts | CPython 3.10–3.14 unit matrix passes with only narrowly justified filters |
| I5 | Adapt pipeline hooks to current Scrapy calling conventions | no pipeline deprecation warning; old and new hook calls work |
| I6 | Enforce plugin descriptor isolation and validation | malformed/duplicate plugins cannot replace or abort bundled discovery |
| I7 | Close simple runtime failure-boundary gaps | breaker-open queue/dedup paths degrade safely; explicit reconnect works |
| I8 | Preserve the replacement enqueue commit boundary | a source-ack failure cannot reject a committed replacement or undo dedup |
| I9 | Repair the Pulsar TLS client contract | real SDK keyword smoke passes; hostname validation is explicit and secure by default |
| I10 | Make connection-generation proxy snapshots coherent | an accessor returns complete old or new backend/breaker state, never a mixed pair |
| I11 | Bind deferred acknowledgements to their issuing backend | reconnect cannot route an old token to a replacement backend or wrong physical queue |
| I12 | Close remaining runtime-generation races | accessors never publish `None`; monitor callbacks cannot self-deadlock; stale breaker outcomes are fenced |
| I13 | Harden configuration security invariants | mechanism-aware non-empty credentials and verified transports fail fast without secret leakage |
| I14 | Repair multi-spider and remaining acknowledgement invariants | spider scopes are isolated; errback replacements and local buffers never acknowledge early |
| I15 | Repair backend-specific P1 correctness | Kafka attempts, ES contention, DynamoDB reads, and SQS limits obey explicit contracts |
| I16 | Re-audit contracts, docs, CI, and remaining P2 items | stop condition met or next bounded iteration selected |
| I40 | Give Redis operations immutable leased client generations | no operation or queued connect intent can cross teardown; Sentinel resources close |
| I41 | Remove outcome-ambiguous redis-py command retries | a response timeout cannot replay queue push/pop Lua mutations |
| I42 | Make Redis deployment-mode settings truthful to redis-py | unsupported Cluster knobs and primary-only master-slave semantics fail or document explicitly |

The order may change when a regression test disproves a hypothesis or exposes a
smaller prerequisite. A disproved finding is removed rather than replaced with
speculative work.

## Task register

### Active P0/P1 tasks

- [x] **PKG-01 — canonical release artifacts.** Move `CHANGELOG.md`,
  `CONTRIBUTING.md`, `LICENSE`, `SECURITY.md`, and `STABILITY.md` back to the
  root; restore all public links; assert wheel/sdist contents and metadata.
- [x] **TEST-01 — complete test dependency group.** Add direct test dependencies
  for every backend module exercised by public lazy imports; prove the tests pass
  in a fresh synced environment and in an adversarial order.
- [x] **TEST-02 — RocketMQ import isolation.** Replace unit-test patching that
  imports the real SDK with a controlled module stub. Run the one real SDK
  contract check in a subprocess with a temporary home directory.
- [x] **TEST-03 — supported-Python truth.** Correct the Hypothesis expected-meta
  oracle for reserved keys. Resolve the exact Python 3.14 itemadapter/Pydantic-v1
  warning without hiding unrelated user warnings.
- [x] **COMPAT-01 — Scrapy pipeline hooks.** Accept both legacy explicit-spider
  calls and current crawler-owned/omitted-spider calls for open, close, and item
  processing; prove registration emits no deprecation warning.
- [x] **PLUGIN-01 — descriptor boundary.** Validate entry-point name,
  `backend_type`, dotted class paths, and duplicates. Logging a broken plugin
  must not become an exception under warnings-as-errors.
- [x] **RUN-01 — circuit-breaker boundary.** Treat breaker-open queue reads and
  dedup checks as expected temporary backend failures rather than scheduler
  crashes.
- [x] **RUN-02 — reconnect contract.** A manager with a disconnected backend
  must actually reconnect instead of returning solely because an object exists.
- [x] **RUN-05 — reconnect generation fencing.** A retained proxy from a
  retired connection generation must not be able to reopen the replacement
  generation's circuit breaker when its in-flight call finishes late.
- [x] **RUN-06 — dedup reservation provenance.** Roll back a fingerprint after
  a failed queue push only when that exact request check created a reservation;
  transient reconnect failures must remain scheduler-safe on poll and depth
  paths.
- [x] **RUN-07 — coherent connection-generation snapshots.** Replace backend
  and breaker generation state under one lock, then validate both identities
  before constructing queue, set, or storage proxies. A reconnect race may
  return a complete old or new generation, never a mixed proxy through which a
  retired backend can trip the live breaker.
- [x] **RUN-08 — issuer-bound acknowledgement settlement.** Wrap every token
  emitted by a built-in backend-delegating strategy with the exact backend
  proxy and physical queue that issued it. One successful ACK or NACK is
  terminal, a broker failure remains retryable, concurrent terminal paths are
  serialized, and custom deferred-ack strategies returning raw tokens fail
  closed before processing.
- [x] **RUN-09 — accessor and close publication safety.** Capture one non-null
  backend value before returning from a lazy accessor, reject `None` snapshots,
  make `is_connected()` single-read, and disconnect falsey third-party backend
  implementations during close.
- [x] **RUN-10 — monitor callback lock isolation.** Dispatch connect, retry, and
  disconnect monitor hooks outside manager locks so a re-entrant observer
  cannot self-deadlock or publish a competing connection transaction.
- [x] **RUN-11 — circuit-breaker outcome epochs.** Attach an epoch to admitted
  calls so a failure from an older CLOSED generation cannot reopen a breaker
  that has since completed an OPEN → HALF_OPEN → CLOSED recovery.
- [x] **RUN-03 — multi-spider manager scope.** Resolve `{spider}` before manager
  acquisition for crawler-owned construction, and use an unshareable fallback
  for unresolved direct construction, so Kafka and RocketMQ consumers are not
  accidentally shared across spiders. Apply the same isolation to backend
  spider mixins without mutating a manager's registry key after acquisition.
- [x] **RUN-04A — post-commit source acknowledgement.** Once a replacement is
  durably pushed, a source-token ack failure remains observable but cannot
  reject the push or roll back its dedup reservation; retain the unresolved
  token and let broker redelivery reach the duplicate-ack path.
- [x] **RUN-04B — errback replacement acknowledgement.** Transfer/defer the
  source token for requests and iterables returned by user errbacks until each
  replacement is durably accepted. Explicitly document the unavoidable
  publish/ack crash gap and define safe behavior for delayed local strategies.
- [x] **RUN-04C — durable replacement strategies and snapshots.** Do not ACK a
  token-bearing source after its replacement has only entered an in-process
  delay, time-wheel, round-robin, or ring buffer. Preserve recovery snapshots
  until a later clean checkpoint so a crash during restore is replay-safe.
- [x] **SEC-01A — AWS credential completeness.** For SQS and DynamoDB, distinguish
  an intentional both-unset ambient credential path from explicit empty or
  partial credentials at settings and connect time, without retaining values in
  validation errors.
- [x] **SEC-01B1 — RabbitMQ/RocketMQ credential completeness.** Reject blank
  explicit credentials, RabbitMQ URL userinfo and remote guest use, and
  incomplete RocketMQ key pairs without retaining secret values.
- [x] **SEC-01B2A — Pulsar credential completeness.** Reject blank explicit
  tokens and service-URL userinfo without retaining their values.
- [x] **SEC-01B2B-K — Kafka credential completeness.** Require an explicit SASL
  mechanism, complete non-empty PLAIN/SCRAM pairs and Confluent key pairs,
  preserve ambient GSSAPI, and reject unsupported OAUTH configuration instead
  of silently omitting authentication.
- [ ] **SEC-01B2B — remaining broker credential completeness.** Reject empty
  explicit secrets, URL/URI userinfo, and mechanism-inconsistent authentication
  for MongoDB and Elasticsearch. Preserve valid mechanism-specific modes rather
  than requiring a universal username/password pair.
- [x] **SEC-02A — AWS cloud transport.** Reject URL userinfo and explicit
  plaintext SQS/DynamoDB endpoints in cloud mode while retaining HTTP support
  for explicit standalone LocalStack-compatible endpoints.
- [x] **SEC-02B1 — RocketMQ and Redis control-plane transport.** Propagate
  authenticated RocketMQ TLS and apply Redis TLS, CA, mTLS, hostname, timeout,
  and credential policy to both Sentinel discovery and discovered masters.
- [x] **SEC-02B2A — RabbitMQ broker transport.** Limit plaintext to all-loopback
  endpoint sets, forbid `amqps://` downgrade, require certificate/hostname
  verification, and bind TLS SNI to each actual cluster node.
- [x] **SEC-02B2B1 — authenticated Pulsar transport.** Require both certificate
  and hostname verification whenever a token is configured.
- [x] **SEC-02B2B2 — Memcached transport boundary.** Define and enforce an
  explicit trusted-network boundary for remote Memcached.
- [x] **SEC-03A — AWS validated connection snapshots.** Revalidate SQS/DynamoDB
  endpoint and credential fields at connect time and use one captured set of
  connection values, so construction-time mutation and validation/use races
  cannot select an unvalidated identity or endpoint. Sanitize endpoint failures.
- [x] **SEC-03A2 — DynamoDB ambient endpoint isolation.** Make each private
  Resource ignore botocore environment/shared-config endpoint overrides so
  cloud HTTPS policy cannot be bypassed outside the validated snapshot.
- [x] **SEC-03B1 — Redis validated connection snapshots.** Capture every
  connection-used value before SDK construction, revalidate TLS after settings
  mutation, repr-redact credentials, and suppress raw startup trace text.
- [x] **SEC-03B2A — RabbitMQ validated connection snapshot.** Revalidate every
  connection and QoS value once, use only the captured values across primary
  and failover nodes, repr-redact the password, and sanitize startup failures.
- [x] **SEC-03B2B1 — Pulsar validated connection snapshot.** Revalidate and
  freeze client/subscription settings per generation, repr-redact the token,
  and sanitize URL/driver startup failures.
- [x] **SEC-03B2B2 — Memcached validated connection snapshot.** Revalidate one
  immutable endpoint/policy snapshot, publish only after a successful probe,
  make connect idempotent, and sanitize startup failures.
- [ ] **SEC-03B2B3 — remaining validated connection snapshots.** Apply copied,
  revalidated connection snapshots and sanitized URL/URI failures to Kafka,
  MongoDB, and Elasticsearch.
- [x] **TRANSPORT-01 — Pulsar TLS SDK contract.** Use the keyword names accepted
  by the locked Pulsar client, propagate hostname validation, and prove the TLS
  branch with a real-signature smoke test.
- [x] **BACKEND-01 — Kafka consumer generations.** Fence tokens by assignment
  epoch and unique delivery attempt within one backend instance, invalidate on
  rebalance/nack, validate per-topic admin responses, and replace the unsafe
  asynchronous clear implementation with an explicit unsupported boundary.
  RUN-08 already supplies the cross-backend-incarnation fence; it does not make
  two deliveries of the same offset distinguishable.
- [x] **BACKEND-04A — SQS terminal settlement.** Make direct SQS ack tokens
  one-shot across ack/nack, preserve retryability after broker/disconnect
  failures, and keep the token path independent of the legacy receipt slot.
- [x] **BACKEND-04B — SQS clear barrier.** Fence old delivery epochs and hold an
  exclusive per-queue barrier for the full asynchronous PurgeQueue window,
  including ambiguous failures, without serializing ordinary queue operations.
- [x] **BACKEND-04C1 — RabbitMQ clear lifecycle.** Track exact per-queue pending
  deliveries, keep token and legacy settlement paths independent, and reject a
  purge that could be followed by an old delivery's nack/requeue.
- [x] **BACKEND-04C2 — Pulsar terminal settlement.** Make direct Pulsar token
  settlement one-shot across ack/nack and retryable after client failures.
- [x] **BACKEND-05 — SQS physical boundaries.** Map logical queue names to
  stable AWS-compatible names without changing already-valid names, and enforce
  the 786,432-byte raw payload ceiling imposed by base64 inside the 1 MiB SQS
  message limit before issuing network calls.
- [x] **BACKEND-06 — Memcached confirmed mutations.** Disable pymemcache's
  default noreply mode so storage mutation success is based on a parsed server
  response rather than an unconfirmed socket write.
- [x] **BACKEND-06B — Memcached single-socket lifecycle.** Serialize SDK
  request/response transactions and published-client teardown, fence private
  connection probes across disconnect, snapshot the destructive flush
  capability, and reject non-successful flush replies.
- [x] **BACKEND-07A — confirmed Kafka publication.** Reject Kafka `acks=0`,
  apply advertised retention/min-ISR settings to new topics, and reject
  inconsistent replication, ISR, and partition-count policy.
- [x] **BACKEND-07B — confirmed MongoDB mutations.** Reject unacknowledged write
  concerns so queue/set/storage success cannot mean only a local socket-buffer
  handoff.
- [x] **BACKEND-08A — conservative Kafka consumer-group lag.** Base depth on
  committed offsets, apply the configured reset policy for fresh groups, and
  serialize metadata calls with poll/settlement on the non-thread-safe client.
- [ ] **BACKEND-08B — broker-safe logical queue names.** Map supported logical
  names such as `q:{spider}` and overlong values to stable Kafka/RocketMQ
  physical resources while preserving already-valid legacy names.
- [x] **CONCURRENCY-01A — RocketMQ token terminality.** Serialize settlement on
  each delivery token so concurrent ack/nack can issue only one successful
  broker action, keep token-aware pops out of the legacy settlement slot, and
  restore local retryability after a failed RPC.
- [x] **CONCURRENCY-01B — SQS client generations.** Bind the SDK client, queue
  URL and lifecycle caches, and issued receipt tokens to one generation; make
  disconnect a continuous barrier rather than a short per-queue pulse.
- [x] **CONCURRENCY-01C — RabbitMQ connection generations.** Keep candidates
  private, make live connect idempotent, preserve a healthy session after a
  failed candidate, and retire old unacknowledged deliveries through close.
- [x] **CONCURRENCY-01D — DynamoDB table generations.** Publish a private
  Session/resource/table set together, serialize the non-thread-safe boto3
  Resource API, and keep every multi-page or lazy-TTL operation on one
  generation.
- [x] **CONCURRENCY-01E — Redis connection generations.** Bind namespace and
  all SDK handles to a leased generation so clear and blocking-pop loops cannot
  cross reconnect or lazily resurrect themselves after disconnect.
- [x] **BACKEND-09A — Redis outcome-ambiguous retry policy.** Give standalone,
  Sentinel-master, and Cluster data clients an explicit no-replay SDK retry
  policy so a response timeout after a committed queue Lua script cannot push
  twice or consume a second item. Define the migration semantics of the public
  `retry_on_timeout` setting instead of forwarding a deprecated flag that
  redis-py 8 does not honor as false.
- [ ] **BACKEND-09B — truthful Redis mode/SDK parameters.** Finish aligning
  Sentinel and Cluster construction with supported redis-py parameters, reject
  Cluster database selections the server ignores, and resolve the advertised
  master-slave replica-read settings that currently route every operation to
  the primary. Map redis-py's separate `RedisClusterException` hierarchy into
  the backend health/queue/set/storage contracts. Reject userinfo-bearing node
  endpoints and sanitize direct settings-validation failures so an endpoint
  cannot retain or disclose embedded credentials. Unsupported settings must
  not remain accepted no-ops.
- [ ] **CONCURRENCY-01F — Kafka client generations.** Publish producer, admin,
  consumer, validated settings, and topic-policy caches as one generation;
  failed candidates must not clear a healthy generation.
- [ ] **CONCURRENCY-01G — RocketMQ client generations.** Publish producer,
  consumer, validated settings, and subscription cache as one generation so a
  replacement consumer cannot inherit a stale subscribed-topic hit.
- [x] **CONCURRENCY-01H — SQS private SDK sessions.** Stop using boto3's shared
  default Session alias when constructing SQS client generations so independent
  backend instances cannot race SDK client setup.
- [ ] **CONCURRENCY-01I — DynamoDB supported SDK ownership.** Replace the
  cross-thread Resource with a low-level client or dedicated owner-thread model
  so the implementation lies entirely inside boto3's documented thread-safety
  boundary, not only behind an external serialization lock.
- [ ] **BACKEND-02 — Elasticsearch commit ambiguity.** Never report an empty
  queue solely because optimistic-claim conflicts exhausted a small retry
  count. Give pushes stable identities, make claim/delete/set writes safe under
  ambiguous transport outcomes, reject partial search/count results, and make
  clear cover unrefreshed writes.
- [x] **BACKEND-03A — DynamoDB consistency and clear scope.** Use consistent
  reads where the storage contract promises immediate visibility, including
  every paginated scan, and reserve whole-table clear for an explicit `None`;
  reject an empty prefix before any AWS operation instead of silently widening
  it to the whole table.
- [x] **BACKEND-03B — DynamoDB storage boundaries.** Enforce DynamoDB's
  400 KiB item and 2,048-byte partition-key limits before network I/O, propagate
  deterministic pipeline failures, and reject malformed persisted value/TTL
  shapes through the typed storage-error contract.
- [x] **BACKEND-03C — bounded DynamoDB batch clear.** Replace BatchWriter's
  unbounded hot retry of `UnprocessedItems` with explicit 25-item writes,
  eight application-level BatchWriteItem submissions per batch with bounded
  exponential jitter, strict sent-subset response validation,
  pagination-cycle detection, and a typed partial-clear failure.
- [x] **BACKEND-03D — remaining DynamoDB response/region contracts.** Accept
  valid multi-segment AWS regions such as GovCloud, revalidate them at connect,
  and reject malformed delete responses through `StorageError`.
- [x] **DOC-SEC-01 — accurate redaction policy.** Correct the public security
  text: `_RedactedStr` protects `repr`, while ordinary string operations expose
  the underlying value required by SDK authentication.
- [x] **TEST-ISO-01 — real optional-SDK collection boundary.** Remove all
  collection-time Pulsar/boto3/pymemcache module replacement, use the SDKs
  required by the test dependency group, patch only per-test constructor
  seams, and pin cold/preloaded forward/reverse collection identity.

### Verified P2 follow-ups

These stay visible but do not justify unsafe bulk changes ahead of the active
correctness work:

- make clear/depth/priority/ack capabilities semantic rather than boolean;
- correct RabbitMQ/SQS clear semantics, Memcached >30-day TTL, Redis binary
  decode mode, MongoDB count truncation, SQS/Elasticsearch physical-name limits,
  and driver-exception normalization;
- bound batched-storage shutdown even if its flusher owns the lock;
- make integration lanes fail rather than skip when a required broker is
  configured but unusable; add real import and sdist smoke tests;
- reconcile the public `Settings` interface with Scrapy component factory
  settings, and replace the single mutable manager monitor only after choosing
  explicit ownership semantics;
- resolve the ambiguous legacy request-body codec with a versioned migration
  policy rather than another decode heuristic;
- add low-noise metrics for repeated circuit-open polls instead of logging a
  traceback on every scheduler heartbeat;
- align examples, release runbook commands, plugin example semantics, and
  remaining historical-document links with current behaviour.

### Evidence policy

An item can be marked complete only when its regression test fails against the
old behaviour (where applicable), passes after the change, and the relevant
global gates stay green. External integration items remain explicitly
"unverified locally" unless a real broker or cloud service was exercised; mocks
must not be described as end-to-end evidence.

## Resolution log

### I1 — canonical release artifacts

Restored the five standard files to the repository root and restored the README
licence link. `uv build --clear` produced both formats; the sdist contains all
five files, the wheel contains `dist-info/licenses/LICENSE` and `py.typed`, and
its metadata reports the MIT licence and Python `>=3.10`. A fresh Python 3.10
environment installed only the built wheel and imported version `0.1.0` from
`site-packages`. The repository suite then passed with 2,905 tests and 44
documented skips using an isolated home directory.

### I2 — complete, order-independent test dependencies

The fixed-seed public lazy-import slice first reproduced three failures because
`boto3` was absent. Added `boto3`, `pulsar-client`, and `pymemcache` to the test
dependency group with the same constraints as their backend extras and refreshed
the lockfile. A fresh temporary environment synced with `--locked --group test`,
exported all three direct requirements, passed the adversarial 114-test slice,
and passed the full suite with 2,905 tests and 44 documented skips.

### I3 — hermetic RocketMQ SDK boundary

Reproduced the import-time failure with a single connected-backend test and no
home-directory override. Behavioural tests now inject a complete top-level
`rocketmq` module stub instead of asking `unittest.mock` to import the real SDK.
The real 5.1.1 method/property contract still runs in a subprocess whose `HOME`
and working directory are pytest-owned. The same pass also moved `Message`
loading behind the connected-state gate, so a disconnected `push` raises the
documented `QueueError` without touching the optional dependency. Both RocketMQ
files passed all 95 tests, and the full 2,905-test suite, Ruff, and strict mypy
all passed without overriding `HOME`.

### I4 — truthful CPython 3.10–3.14 matrix

Added an explicit Hypothesis example proving that persisted request metadata
must exclude the acknowledgement token and the consumed `delay`/`source`
routing controls, then corrected the property oracle to compare only durable
metadata. On Python 3.14, itemadapter 0.13.1 imports `pydantic.v1` solely to
recognise legacy models; the resulting compatibility warning is now ignored
only for its exact message and `itemadapter._imports` origin. A warning-policy
canary proves unrelated `UserWarning` instances remain fatal. Every CPython
3.10–3.14 non-integration lane passed 2,906 tests with 7 skips and 37 integration
tests deselected; Ruff and strict mypy also passed.

### I5 — Scrapy pipeline hook compatibility

First pinned Scrapy 2.17's own registration check as a failing regression: all
three pipeline hooks required the deprecated `spider` argument. The hooks now
accept the legacy explicit argument or resolve the crawler/opened spider when
Scrapy omits it, while a direct argumentless call with no available spider fails
with a clear lifecycle error. `process_item` now reflects ItemAdapter's real
item-like input surface instead of claiming only `scrapy.Item`. All 58 focused
pipeline tests passed on Python 3.10 and 3.14, the full suite passed 2,909 tests
with 44 skips, and Ruff plus strict mypy remained green.

### I6 — isolated and deterministic plugin discovery

Five regressions first demonstrated that malformed class paths and mismatched
names were accepted, duplicate third-party names used last-write-wins, and a
broken plugin could abort discovery under warnings-as-errors. Discovery now
requires valid/equal entry-point and descriptor names, dotted Python identifier
paths, and a frozen string capability set. Duplicate third-party names register
neither claimant. Broken, conflicting, and shadowing plugins are reported via
logging so Python warning filters cannot hide the bundled registry. The author
contract and changelog carry the same semantics. All 21 registry tests passed on
Python 3.10 and 3.14; the full suite passed 2,915 tests with 44 skips, plus Ruff
and strict mypy.

### I7 — transient outage recovery boundaries

Three regressions first proved that an OPEN circuit escaped queue polling and
deduplication, while an explicit `ConnectionManager.connect()` ignored an
existing-but-disconnected backend. Queue polling now treats only the typed
circuit rejection alongside queue failures; deduplication routes the same
rejection through its existing warn-once, error-counter, not-seen degradation
path without swallowing configuration failures. Explicit reconnect performs an
unlocked health probe, atomically detaches and cleans up a stale generation,
creates a fresh circuit-breaker generation, and publishes a fresh backend
through the existing serialized retry transaction. The three regressions passed
on Python 3.10 and 3.14; 102 connection-manager tests, 74 scheduler/dupefilter
tests, the full 2,918-test suite with 44 skips, Ruff, and strict mypy all passed.

### I7 follow-up — generation-isolated outage recovery

A fresh-eyes audit disproved two assumptions left by the first I7 patch. A
retained proxy binds the old breaker as well as the old backend, so resetting
that shared object allowed a late retired-backend failure to trip the fresh
connection. Also, a dedup "not seen" result during degradation did not prove a
fingerprint had been written, so unconditional compensation could remove an
unrelated marker after the queue push failed. Reconnect now installs a new
breaker with the same immutable policy, while old proxies retain the retired
generation. The bundled dupefilter records reservation provenance by request
identity and exposes a one-shot scheduler handshake; custom dupefilters retain
the legacy fallback. Poll and pending-depth paths now also contain typed
reconnect exhaustion. Six focused regressions passed on Python 3.10 and 3.14;
142 breaker/connection tests, 106 scheduler/dupefilter tests, the full
2,923-test suite with 44 skips, Ruff, and strict mypy all passed.

### I8 — committed replacement acknowledgement boundary

Two regressions demonstrated that `BackendQueue` first committed a retry or
redirect replacement, then allowed a failed source ack to escape as though the
push itself had failed. The scheduler consequently returned `False`, removed
the replacement's fingerprint, and allowed the broker's source redelivery to
publish another copy. The strategy push is now the explicit commit boundary:
post-commit ack failures increment `scheduler/ack_error`, retain the unresolved
token, and log the terminal failure without changing the accepted push result.
The reserved fingerprint can absorb ordinary source redelivery through the
existing duplicate-ack recovery path, but Scrapy retry requests may set
`dont_filter=True`; the publish-then-ack boundary therefore remains explicitly
at-least-once and can duplicate without a stable lineage/outbox key. The two
focused regressions and all 167 queue/scheduler acknowledgement tests passed;
the focused regressions also passed on Python 3.14. The full suite passed 2,925
tests with 44 skips, followed by Ruff and strict mypy.

### I9 — Pulsar TLS SDK contract

The third audit compared the real locked `pulsar-client` 3.12.0 signature with
the backend builder. All TLS connects passed `allow_insecure_connection` and
`tls_trust_certs_file`, but the SDK accepts only
`tls_allow_insecure_connection` and `tls_trust_certs_file_path`; MagicMock-based
tests had hidden the resulting pre-network `TypeError`. The SDK also defaults
`tls_validate_hostname` to false. The same real-SDK probe disproved an older
claim that schemes were case-insensitive and confirmed that cluster URLs use
one scheme followed by a host list. Nine RED regressions captured these
defects.
The public compatibility fields remain unchanged and are translated only for
`pulsar+ssl://`; a new `tls_validate_hostname` setting defaults to true and is
forwarded explicitly. Service URLs are canonicalized to the SDK grammar, and a
subprocess contract test pins the real SDK signature. Nineteen focused tests
passed on Python 3.10 and 3.14; 280 related settings/backend tests and the full
2,932-test suite with 44 skips passed, followed by Ruff and strict mypy.

### I10 — coherent connection-generation proxy snapshots

A deterministic regression forced reconnect to replace manager state between
an accessor's backend and breaker reads. The prior implementation returned a
proxy over the retired backend but the live generation's breaker, so a late old
failure could still open the replacement circuit despite I7's distinct breaker
objects. Reconnect now detaches the backend and advances its breaker under one
state lock. Queue, set, and storage accessors share a read-then-validate loop
that performs connection and settings work outside that lock, then accepts the
pair only when both identities still match manager state. The regression first
failed by returning the retired payload and now passes on Python 3.10 and 3.14;
143 connection/breaker tests and the full 2,933-test suite with 44 skips passed,
followed by Ruff and strict mypy.

### I11 — issuer-bound acknowledgement settlement

Two deterministic regressions first showed a token popped from backend A being
sent to replacement backend B after reconnect, where broker-local generations
and delivery identifiers can collide. Built-in backend-delegating strategies
now wrap every non-null raw token with the exact queue-backend proxy and physical
queue used by that pop; priority buckets and work-stealing peer queues preserve
their actual source. `BackendQueue` settles through that retained issuer instead
of resolving the manager again. The private wrapper hides the raw token from its
representation, exposes read-only routing diagnostics, serializes concurrent
ACK/NACK paths, becomes terminal after one successful operation, and remains
pending after a broker exception so the operation can be retried. A custom
strategy returning a raw token for a deferred-ack backend now fails closed
before request processing and points authors to the binding helpers.

The exact regression set passed on Python 3.10 and 3.14, 382 related
queue/strategy/scheduler tests passed on Python 3.10, and the full suite passed
2,943 tests with 44 documented skips. Ruff and strict mypy remained green. This
iteration closes cross-manager-generation mis-acknowledgement; Kafka's
same-instance rebalance and same-offset delivery-attempt fencing remains
BACKEND-01.

### I12a — connection publication and falsey-close safety

Four regressions reproduced independent double-read and truthiness failures.
After a successful lazy connect, the owner checked `self._backend` for non-null
and then returned a second read that reconnect could already have detached;
`is_connected()` had the same check-then-dereference shape. A `None` result
could then satisfy snapshot identity as `None is self._backend` and surface as
a misleading `NotImplementedError`. Separately, `close()` treated a valid
third-party backend whose `__bool__` returned false as absent and leaked its
connection.

The owner now captures its published backend once under the manager state lock,
fans that exact value out to waiters, and returns only the local value. Snapshot
construction rejects a violated null-backend contract with
`BackendConnectionError`; `is_connected()` uses one local read; close tests
identity against `None` rather than truthiness. The four regressions passed on
Python 3.10 and 3.14, 182 connection/breaker tests passed, and the full suite
passed 2,947 tests with 44 documented skips. Ruff and strict mypy remained
green.

### I12b — re-entrant lifecycle monitor isolation

Four regressions showed every lifecycle hook executing while a manager lock was
held: connect success, retry, and stale-generation disconnect ran under the
non-reentrant `_connect_lock`, while final disconnect ran under `_lock`. A
monitor that called back into `connect()` or `backend` could therefore block its
own thread forever; swallowing callback exceptions cannot recover a deadlock.

Connection transactions now record ordered monitor events while serialized and
dispatch them only after `_connect_lock` is released. Retry observations are
therefore delivered after the transaction rather than during its backoff sleep,
without changing their count or order. Final close marks the manager retired,
detaches its handle, and resets its breaker under `_lock`, then performs network
disconnect and the lifecycle callback outside both manager and registry locks.
Re-entry consequently observes either the healthy completed generation or a
typed released-manager error. The four lock-state/re-entry regressions and three
callback-failure checks passed on Python 3.14; 243 related tests and the full
Python 3.10 suite passed, the latter with 2,951 tests and 44 documented skips.
Ruff and strict mypy remained green.

### I12c — circuit-breaker outcome epochs

Two threaded RED regressions held a CLOSED call in flight while another call
tripped the breaker. After either a successful HALF_OPEN recovery or an
explicit reset, the old call's late failure reopened the healthy breaker because
outcomes carried only their prior state. A related probe-slot regression showed
that a non-counted exception from an old HALF_OPEN probe could release a newer
generation's active probe slot.

Every admitted call now carries the breaker's state-transition epoch. The epoch
advances on CLOSED/OPEN/HALF_OPEN transitions and every explicit reset; success,
failure, signal, and non-counted-exception bookkeeping applies only when both
the state and epoch still match. Ordinary concurrent CLOSED failures continue
to share an epoch until the threshold transition, so the consecutive-failure
contract is preserved. Six exact regressions passed on Python 3.14, 246 related
breaker/manager/degradation tests passed on Python 3.10, and the full suite
passed 2,954 tests with 44 documented skips. Ruff and strict mypy remained
green.

### I15a — DynamoDB consistent reads and destructive-clear scope

Five RED regressions showed that all three point-read operations used
eventually consistent reads, every page of `clear_storage()` did the same, and
an empty string bypassed prefix validation and widened into an entire-table
delete. `retrieve()`, `exists()`, and `ttl()` now request `ConsistentRead=True`;
the flag is also present on every paginated scan. Only `prefix=None` selects a
whole-table clear, while `prefix=""` fails validation before `scan()` or
`batch_writer()` can run. The scan/delete sequence deliberately does not claim
snapshot isolation from concurrent writers.

The five exact regressions passed on Python 3.10 and 3.14, 91 related DynamoDB
and cross-backend TTL tests passed, and the full suite passed 2,959 tests with
44 documented skips. Ruff and strict mypy remained green. DynamoDB physical
size and malformed persisted-value boundaries were deferred to BACKEND-03B.

### I15b — DynamoDB physical and persisted-data boundaries

Nineteen RED regressions established four silent-failure paths: DynamoDB-sized
items and overlong partition keys reached AWS, corrupt binary/TTL attributes
returned absence sentinels or raw conversion exceptions, and the item pipeline
swallowed a backend's deterministic validation error as a success-shaped item
return. The implementation now applies AWS's documented 2,048-byte partition
key and 400 KiB names-plus-values limits before network I/O, including key and
optional numeric-TTL overhead. See the
[DynamoDB constraints](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html)
and [item-size rules](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html).

Every point operation shares the key ceiling. Reads accept bytes, bytearray, or
boto3's binary wrapper, validate finite numeric expiries (including `Decimal`),
and otherwise raise `StorageError` with the exact operation and key. The
pipeline now propagates local configuration/serialization/type/value/overflow
failures immediately while retaining its existing tolerance and threshold for
operational backend exceptions. All 19 regressions passed on Python 3.10 and
3.14, 205 related DynamoDB/pipeline/storage-strategy tests passed, and the full
suite passed 2,978 tests with 44 documented skips. Ruff, strict mypy, and the
patch integrity check remained green.

### I13a — shared AWS credential and endpoint security

Twenty-two RED regressions showed that SQS and DynamoDB accepted explicit HTTP
cloud endpoints and URL userinfo, allowed explicit empty credentials to become
ambient credentials, and trusted settings that had been mutated after initial
validation. A shared AWS policy now distinguishes `None`/`None` (the intentional
boto3 ambient chain) from every partial, blank, or whitespace-only explicit
credential. It validates absolute endpoint URLs without echoing or retaining
userinfo, requires HTTPS for cloud overrides, and requires an explicit endpoint
in standalone connect paths so mutation cannot redirect LocalStack traffic to
real AWS.

Both backend connect methods capture all connection-used fields once, validate
that captured endpoint/credential set, and pass only the same locals to boto3;
concurrent mutation can therefore affect a later connection attempt but cannot
swap values between validation and use in the current attempt. The 22 exact
regressions passed on Python 3.10 and 3.14, 415 related settings/SQS/DynamoDB
tests passed, and the full suite passed 3,000 tests with 44 documented skips.
Ruff, strict mypy, and the patch integrity check remained green.

### I15c — SQS physical names and base64-adjusted payload limits

Three initial RED regressions proved that the mixin's default colon-delimited
logical name and a prefix-expanded name over 80 characters were passed directly
to SQS, while a 786,433-byte raw payload reached AWS after expanding beyond the
MessageBody limit. A fourth compatibility assertion confirmed that an already
valid 80-character name was unchanged; a final RED regression covered SQS's
non-empty body minimum. AWS currently permits only alphanumeric, hyphen, and
underscore queue names up to 80 characters, and MessageBody is capped at 1 MiB:
[queue quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-queues.html),
[message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html).

The backend now preserves a valid `prefix + logical_name` exactly and otherwise
uses a versioned, length-delimited BLAKE2 mapping whose output is a short valid
standard-queue name. Empty payloads and raw payloads above 786,432 bytes raise a
typed `QueueError` with queue/operation context before queue URL resolution or
send I/O. All five regressions passed on Python 3.10 and 3.14, 268 related
SQS/spider/fan-out tests passed, and the full suite passed 3,005 tests with 44
documented skips. Ruff, strict mypy, and patch integrity remained green.

### I14a — single-consumer manager scope across spiders

Six initial RED regressions demonstrated that Kafka and RocketMQ schedulers
constructed from the unresolved `q:{spider}` template shared one mutable
consumer manager, and that two backend-spider-mixin instances did the same.
The mixin also allowed one such consumer to be reused for two different logical
queues. A compatibility assertion proved that explicitly fixed queue names
could continue sharing a manager.

Crawler-owned scheduler construction now resolves the spider name before
manager acquisition. Direct unresolved templates receive an opaque registry-
only discriminator, while fixed logical queue names retain deterministic
sharing. Kafka and RocketMQ spider mixins receive a stable per-instance scope
and fail fast if one instance attempts to bind its single consumer to a second
logical queue. The discriminator remains excluded from backend settings, so it
cannot mutate or leak into driver configuration. All seven regressions passed
on Python 3.10 and 3.14, 270 related scheduler/mixin/manager tests passed, and
the full suite passed 3,012 tests with 44 documented skips. Ruff, strict mypy,
and patch integrity remained green.

### I14b — errback output commit groups and volatile-strategy fail-closed

Four initial RED regressions proved that a handled user errback immediately
acked its broker source even when it returned one replacement request, a stream
of several requests, or a generator that later failed. Four additional RED
cases proved that delayed, time-wheel, round-robin, and ring-buffer replacement
pushes acked the source after only a process-local append. Two compatibility
cases preserved the direct backend commit for zero effective delay.

The scheduler now transfers a source delivery into an idempotent child-token
group. It seals the group only after synchronous or asynchronous output
enumeration completes, acks only after every replacement crosses its queue
commit boundary, and nacks on iteration failure. `BackendQueue` recognizes the
shared internal token protocol and asks each strategy whether the selected push
is crash-durable. Token-bearing replacements fail before serialization or
local mutation when the answer is false; their source remains unacknowledged
for broker redelivery. Documentation now states the unavoidable replacement-
publish/source-ack crash gap: it can produce duplicates, but ordering prevents
loss of both copies. All eleven regressions passed on Python 3.10 and 3.14, 325
related queue/strategy/scheduler tests passed, and the full suite passed 3,023
tests with 44 documented skips. Ruff, strict mypy, and patch integrity remained
green.

### I14c — replay-safe recovery checkpoints

One stateful-storage RED regression simulated a hard crash immediately after a
successful strategy restore. The eager delete removed the only persisted copy,
so the next process started without the still-unprocessed local queue state.
Existing tests also encoded that unsafe deletion as expected behavior.

Restore now leaves the prior checkpoint intact. A later clean close overwrites
it with the strategy's current state or deletes it only after the strategy has
cleanly drained. If a process dies between restore and that checkpoint,
already-completed entries can replay but pending entries cannot disappear.
This is the deliberate at-least-once side of the snapshot contract; stable
per-worker ownership remains required to prevent two live workers from sharing
one checkpoint. The exact regression passed on Python 3.10 and 3.14, all 310
snapshot/queue/strategy tests passed, and the full suite passed 3,024 tests with
44 documented skips. Ruff, strict mypy, and patch integrity remained green.

### I13b — RocketMQ authenticated transport and connection snapshots

Twelve initial RED regressions and one locked-SDK signature smoke established
that the RocketMQ 5.x Producer and SimpleConsumer both defaulted to plaintext,
cloud accepted missing credentials, partial/blank keys silently became
anonymous `Credentials()`, and one connection attempt could combine settings
read at different times. Constructor arguments and outward startup errors also
exposed raw credential values.

`tls_enabled` now reaches both SDK constructors. Anonymous standalone/cluster
connections may explicitly choose TLS or plaintext, while every authenticated
connection requires a complete non-empty key pair and TLS; cloud requires that
authenticated-TLS combination. Settings validation and connect-time validation
share the same policy. A connection captures endpoint, credentials, timeout,
consumer group, and TLS once before importing or constructing the SDK and uses
only those values for the attempt. SDK-bound credentials use the shared
repr-redacting string wrapper, public startup failures omit driver text, and
the spider mixin exposes the TLS shortcut alongside the existing RocketMQ
fields. The RocketMQ-focused set passed on Python 3.10 and 3.14, 377 related
settings/mixin/optional-dependency tests passed, and the full suite passed 3,037
tests with 44 documented skips. Ruff, strict mypy, and patch integrity remained
green.

### I16a — Kafka assignment and delivery-attempt fencing

Five RED state-machine regressions reproduced two loss paths inside one Kafka
consumer generation. After a nack/seek, a same-offset redelivery produced an
equal token, so the first request's late success could commit the replacement.
A token pop also populated the legacy `_last_record` slot, allowing a later
bare `ack()` to commit immediately after token nack. Subscription changes and
partition revocation had no epoch boundary or rebalance listener.

Tokens now include consumer generation, assignment epoch, and a monotonically
unique delivery-attempt identity. The backend registers Kafka's rebalance
listener, fences all local delivery state before subscription changes and on
both revoke/assign callbacks, and records the currently active attempt for each
topic/partition/offset. Ack and nack serialize through one lifecycle lock;
successful nack retires the attempt while seek failure leaves it retryable.
The offset remains in the watermark gap until a new attempt is delivered, so
the fix chooses duplicate redelivery over skipping work. Token-based pops no
longer populate the legacy bare-commit slot. All 135 related tests passed on
Python 3.10 and 3.14, and the full suite passed 3,042 tests with 44 documented
skips. Ruff, strict mypy, and patch integrity remained green.

### I16b — Kafka admin outcomes and honest clear capability

Six RED regressions showed that the locked Kafka admin client returns
per-topic create failures inside a response object, while the backend cached
the topic as successfully created. Thrown create errors were only logged, so a
push could continue after its prerequisite failed. The clear path issued an
asynchronous delete and immediately recreated the same name, reporting success
without a propagation barrier and retaining consumer-group offset ambiguity.

Topic creation now requires exactly one well-formed response entry for the
requested topic and accepts only broker success or already-exists; every other
code becomes a typed push error before the known-topic cache is updated. A
disconnect invalidates that cache. Kafka `clear_queue()` now fails before admin
I/O with an explicit `NotImplementedError`: delete/recreate cannot prove that
work accepted after return survives or that old group offsets are compatible
with the new topic. Operators must quiesce and reset Kafka deliberately. The 12
exact boundary tests passed on Python 3.14, 302 related Kafka/queue-strategy
tests passed on Python 3.10, and the full suite passed 3,046 tests with 44
documented skips. Ruff, strict mypy, and patch integrity remained green.

### I17a — SQS single-outcome acknowledgement tokens

Nine initial RED outcomes showed that the same SQS receipt token could issue
duplicate deletes, execute both ack and nack, or silently become locally
settled while the client was disconnected. An untracked token produced after
the diagnostic cap had the same correctness gap, broker failures were not
explicitly represented as a retryable token state, and token-aware pops also
populated the legacy last-receipt slot.

Each token now owns a lock-protected `pending -> settling -> acked|nacked`
state machine. The lock spans its broker call, so concurrent ack/nack callers
observe one final outcome; a failed call restores `pending`, retains diagnostic
tracking, and can be retried. Settlement while disconnected raises a typed
error instead of claiming success. Correctness lives on the token even when
the bounded diagnostic set overflows, and `pop_with_ack()` no longer creates a
second legacy path to the same receipt. All 99 SQS contract tests passed on
Python 3.10 and 3.14, and the full suite passed 3,053 tests with 44 documented
skips. Ruff, strict mypy, and patch integrity remained green.

### I17b — SQS asynchronous purge barrier

Seven initial RED outcomes plus one second-order concurrency regression proved
that `clear_queue()` returned as soon as the PurgeQueue RPC did, even though
AWS can continue deleting both old and newly sent messages for 60 seconds. A
lost response also surfaced immediately despite ambiguous service acceptance,
pre-clear tokens remained locally active, and a first locking design made a
normal long poll block same-queue producers.

Each physical queue now has a shared-operation/exclusive-clear barrier and a
monotonic lifecycle epoch. Ordinary push, pop, depth, and settlement operations
remain concurrent; clear first excludes them, advances the epoch, retires only
that queue's local receipts, calls PurgeQueue, and waits a full 60 seconds after
the RPC returns. The same wait applies before an ambiguous failure is raised.
Operations on other queues remain live, stale tokens perform no broker call,
and post-clear tokens use the new epoch normally. This follows AWS's current
[PurgeQueue contract](https://docs.aws.amazon.com/boto3/latest/reference/services/sqs/client/purge_queue.html).
All 107 SQS contract tests passed on Python 3.10 and 3.14, and the full suite
passed 3,061 tests with 44 documented skips. Ruff, strict mypy, and patch
integrity remained green.

### I18 — Redis Sentinel control-plane TLS and snapshots

Eight initial RED regressions showed that `ssl_enabled=True` protected the
discovered Redis master but not Sentinel discovery itself. Supplying
`sentinel_kwargs` also disabled redis-py's fallback propagation of socket
timeouts, raw Sentinel/master passwords remained visible in configuration
reprs, SDK construction could mix pre- and post-mutation settings, client
certificate halves were accepted, and driver text could expose secrets in a
rendered startup traceback.

Every Redis connection path now captures its connection-used values before SDK
construction and revalidates the captured TLS tuple. Sentinel control clients
receive the same TLS enablement, CA, optional client certificate/key, hostname
verification, and dedicated retry policy as intended for discovery; the master
retains its normal retry policy. Credentials use repr-redacting string wrappers
for standalone, cluster, Sentinel, and discovered-master constructors. Public
startup exceptions and rendered tracebacks omit raw driver/parser text. A
locked redis-py smoke test proves `sentinel_kwargs["ssl"]` selects
`SSLConnection`. All 418 related tests passed on Python 3.10 and 3.14. Ruff,
strict mypy, and patch integrity remained green. The full Python 3.10 suite
passed 3,070 tests with 44 documented skips.

### I19 — RabbitMQ authenticated transport and connection snapshots

Sixteen of twenty initial security regressions failed: remote primary and
cluster endpoints accepted plaintext credentials, `amqps://` could be
explicitly downgraded, URL userinfo was retained, weak certificate modes and
partial mTLS material were accepted, and Pika TLS options omitted the node
hostname needed for SNI and certificate matching. Connection setup also read
mutable settings separately for every failover node and QoS step, while raw
driver text—including credentials—could reach the public startup traceback.

Plaintext is now limited to all-loopback endpoint sets. Any remote node requires
TLS with `CERT_REQUIRED`; remote `guest`, URL userinfo, blank credentials,
partial certificate/key pairs, and secure-URL downgrade fail before SDK I/O.
Every attempt revalidates one immutable, repr-redacted snapshot and uses it for
the primary, every parsed IPv4/IPv6 failover node, TLS context, Pika SNI,
timeouts, retry settings, and QoS. Public startup errors suppress driver text.
All 521 related tests passed on Python 3.10 and 3.14, and the full Python 3.10
suite passed 3,093 tests with 44 documented skips. Ruff, strict mypy, and patch
integrity remained green.

### I20 — RabbitMQ clear lifecycle barrier

Six of ten initial RED regressions demonstrated that `queue_purge()` could run
while the same logical queue still had unacknowledged local deliveries. RabbitMQ
does not purge those deliveries, so a later nack could requeue pre-clear work.
The token-aware pop path also populated the legacy last-tag slot, allowing a
token delivery to be settled once through each API. Clear and pop had no local
linearization point, and the diagnostic token cap could not safely answer
whether a queue remained in flight.

Every issued delivery now increments an exact O(number-of-queues) pending
counter that is decremented only after a confirmed ack/nack. A target queue with
any pending delivery fails before broker purge; unrelated queues remain
clearable. Disconnect resets local accounting only after invalidating the
channel, allowing RabbitMQ to requeue old work for a post-reconnect purge.
Token-aware pops no longer touch the legacy slot. An RLock serializes Pika
push/pop/ack/nack/depth/purge operations, and two deterministic thread tests
prove both clear-before-pop and pop-before-clear orderings. All 231 related tests
passed on Python 3.10 and 3.14; the full Python 3.10 suite passed 3,104 tests
with 45 documented skips. Ruff, strict mypy, and patch integrity remained green.

### I21 — Pulsar terminal token settlement

Eight initial RED regressions showed that one Pulsar delivery token could issue
multiple broker operations: duplicate ACKs, ACK followed by NACK, NACK followed
by ACK, and concurrent opposite actions all remained live after an earlier
success. An ACK exception also removed the token from diagnostic in-flight
tracking despite the delivery remaining unsettled, while `pop_with_ack()`
populated the legacy last-message slot and exposed a second settlement path.

Each token now owns a settlement lock and an explicit pending/settling/terminal
state. The lock spans the broker call, success publishes exactly one terminal
ACK/NACK outcome, and an exception restores `pending` so the same token can be
retried. Stale generations retire locally without touching a replacement
consumer, and the diagnostic set has its own lock for free-threaded-safe
tracking. Token-aware pops no longer write the legacy slot. All 83 local Pulsar
tests passed on Python 3.10 and 3.14 with the real-broker test explicitly
skipped; the full Python 3.10 suite passed 3,110 tests with 45 documented skips.
Ruff, strict mypy, and patch integrity remained green.

### I22 — Pulsar authenticated transport and connection snapshots

Nine initial RED regressions showed that Pulsar accepted blank tokens, URL
userinfo, and token-authenticated TLS with certificate or hostname verification
disabled. Construction-time validation could be bypassed by mutating the
settings object before `connect()`, client and subscription construction read
different settings generations, the retained snapshot did not exist, and raw
driver text—including a token—reached the public connection error traceback.

A shared validator now normalizes and checks one captured connection value set
at settings construction and again immediately before SDK I/O. Token auth
requires `pulsar+ssl://`, certificate verification, and hostname verification;
blank tokens, userinfo, and blank trust paths fail without retaining values.
Each client generation atomically publishes an immutable snapshot containing
its repr-redacted token and all later subscription inputs, so post-capture
mutation cannot downgrade transport or change consumer identity. Public startup
errors suppress the service URL and driver text. All 327 related tests passed
on Python 3.10 and 3.14 with one real-broker test explicitly skipped; the full
Python 3.10 suite passed 3,119 tests with 45 documented skips. Ruff, strict
mypy, and patch integrity remained green.

### I23 — Memcached trusted-network and connection lifecycle boundary

Fifteen initial RED assertions showed that any remote host was accepted over
Memcached's unauthenticated plaintext protocol, malformed host components were
retained, and post-construction host/port mutation bypassed field validation.
The backend published its candidate before the `stats()` probe succeeded,
created another client on repeated `connect()`, retained no validated snapshot,
and exposed endpoint plus raw driver text in startup failures.

Loopback hosts remain zero-configuration. A non-loopback host now requires the
explicit `allow_remote_plaintext=True` trusted-private-network acknowledgement;
bare host grammar, port, mode, and policy are validated both at settings
construction and immediately before SDK I/O. Connect attempts are serialized,
the probed candidate and immutable endpoint snapshot publish together, repeated
connect is idempotent, and disconnect atomically detaches that generation.
Startup failures suppress endpoint and driver details, while an explicitly
remote connection emits an operator warning. All 430 related tests passed on
Python 3.10 and 3.14 with one real-service test explicitly skipped; the full
Python 3.10 suite passed 3,135 tests with 45 documented skips. Ruff, strict
mypy, and patch integrity remained green.

### I24 — Memcached confirmed mutation boundary

Three initial RED assertions proved that every client generation inherited
pymemcache's `default_noreply=True`. Inspection of the locked pymemcache 4.0.0
constructor and storage command path confirmed that this mode returns success
for set, delete, and flush immediately after writing the command, without
reading the server's `STORED`, `DELETED`, or error response.

Every client generation now opts into `default_noreply=False`, making a normal
StorageBackend mutation return contingent on a parsed server response. The
remaining transport-exception boundary is documented as ambiguous and callers
are directed toward idempotent keys and values. A subprocess sentinel pins the
real installed SDK default so a dependency change cannot silently invalidate
the rationale. All 49 local Memcached tests passed on Python 3.10 and 3.14 with
one real-service test explicitly skipped; the full Python 3.10 suite passed
3,136 tests with 45 documented skips. Ruff, strict mypy, and patch integrity
remained green.

### I25 — Kafka mechanism-aware authentication completeness

Twenty-one initial RED outcomes showed that SASL transports accepted no
mechanism, PLAIN/SCRAM accepted partial or whitespace-only credentials,
GSSAPI was never passed to the SDK, and OAUTHBEARER was advertised without the
required token-provider surface. Empty Confluent key pairs passed settings
validation and then missed the truthy builder branch, allowing kafka-python's
default plaintext configuration. A model mutated after construction could take
the same downgrade path before client creation.

A shared mechanism-aware validator now runs at settings construction and again
at every SDK-config boundary. PLAIN and SCRAM require complete non-empty pairs;
GSSAPI preserves ambient Kerberos and rejects ignored PLAIN fields;
OAUTHBEARER fails explicitly until a provider surface exists. Confluent
credentials are complete, non-empty, and exclusive to Confluent mode, and both
cloud credentials plus password values are repr-redacted in SDK dictionaries.
All 370 related tests passed on Python 3.10 and 3.14 with two real-broker tests
explicitly skipped; the full Python 3.10 suite passed 3,156 tests with 45
documented skips. Ruff, strict mypy, and patch integrity remained green.

### I26 — Kafka confirmed publication and topic durability

Eighteen RED outcomes across three evidence batches showed that `acks=0` and
unsupported acknowledgement values were accepted as successful queue writes,
while the public retention, minimum-ISR, and general partition settings were
ignored during topic creation. Invalid ISR/replication combinations passed,
TopicAlreadyExists cached success without inspecting real broker policy, the
integer acknowledgement value could not be supplied through an environment
variable, and a valid policy mutation bypassed the bare known-topic cache.

Kafka queue publication now accepts only broker-confirmed `acks=1` or
`acks="all"`, with exact environment text `"1"` normalized safely and booleans
rejected. New topics receive explicit retention and minimum-ISR config;
replication, ISR, and the two partition-count settings are validated as one
policy. Existing topics are never altered implicitly: their metadata and
selected config are read and a mismatch fails before publication. The cache is
policy-aware, so a changed valid policy is reverified rather than ignored. All
387 related tests passed on Python 3.10 and 3.14 with two real-broker tests
explicitly skipped; the full Python 3.10 suite passed 3,173 tests with 45
documented skips. Ruff, strict mypy, and patch integrity remained green.

### I27 — MongoDB acknowledged mutation boundary

Fourteen RED outcomes across two evidence batches showed that zero, negative,
boolean, empty, and unsupported MongoDB write concerns passed construction;
numeric environment values stayed strings; negative and boolean timeouts were
accepted; and a post-construction `w=0` mutation reached client creation. A
second RED pinned legitimate numeric timeout text so hardening would not break
environment-based configuration.

One validator now defines the supported write boundary: `w` is a positive
integer or `"majority"`, numeric environment text is normalized, and timeout is
`None` or a non-negative integer. It runs during settings parsing, client-kwargs
construction, and immediately before SDK I/O, with credential-free errors.
All 452 related tests passed on Python 3.10 and 3.14 with six real-MongoDB tests
explicitly skipped; the full Python 3.10 suite passed 3,190 tests with 45
documented skips. Ruff, strict mypy, and patch integrity remained green.

### I28 — Memcached single-socket and flush generation safety

Twelve RED outcomes across two evidence batches reproduced four concrete
failures: ordinary client calls overlapped on one response socket; disconnect
during a private `stats()` probe returned before that candidate was later
published; post-connect settings mutation could authorize server-wide flush;
and a false flush reply was reported as success. Strict boolean validation also
initially broke canonical `true`/`false` environment text, which the second RED
batch pinned before implementation was finalized.

All published-client SDK transactions now share a non-reentrant operation lock.
Disconnect fences private connect attempts with a lifecycle generation so it
returns without waiting for an unbounded probe and stale candidates close
instead of resurrecting. The destructive flush capability is normalized only
from booleans or canonical environment text, captured in the immutable
connection snapshot, and revalidated before SDK I/O; clear requires an exact
successful server reply. All 63 related tests passed on Python 3.10 and 3.14
with one real-Memcached test explicitly skipped; the full Python 3.10 suite
passed 3,204 tests with 45 documented skips. Ruff, strict mypy, and patch
integrity remained green.

### I29 — Kafka conservative consumer-group backlog

Six RED outcomes across two evidence batches showed that a fresh consumer group
could report zero depth despite an existing earliest-policy backlog, live depth
used the consumer's fetched position instead of its committed checkpoint, and
metadata calls could overlap `poll()` on kafka-python's non-thread-safe
consumer. A settings mutation after lazy consumer creation could also make
depth apply a different reset policy from the generation that would consume it.

Queue depth now computes conservative group lag from committed offsets and log
boundaries. A missing commit uses the captured generation policy: earliest uses
the beginning, latest uses the end, and none raises a typed queue error instead
of manufacturing zero. All live consumer metadata calls share the existing
delivery lock with poll and settlement; temporary consumers receive the policy
explicitly. A real-service integration regression covers depth before the first
poll. All 474 related tests passed on Python 3.10 and 3.14 with three broker
tests explicitly skipped; the full Python 3.10 suite passed 3,210 tests with 46
documented skips. Ruff, strict mypy, and patch integrity remained green.

### I30 — RocketMQ single-outcome delivery settlement

One deterministic RED barrier held an acknowledgement RPC open while a nack
for the same delivery completed concurrently. Both terminal broker actions
were issued because the token exposed only an unlocked completed flag set after
the RPC returned.

RocketMQ delivery tokens now own a settlement lock and explicit pending,
settling, terminal, and stale states. The lock covers the broker call, so a
competing ack or nack observes either the successful terminal state or the
restored pending state after an exception; it can never overlap an uncertain
settlement. Token-aware ack/nack retain the existing generation fence and clear
the legacy slot only after a successful terminal action. All 502 related tests
passed on Python 3.10 and 3.14 with three real-broker tests explicitly skipped;
the full Python 3.10 suite passed 3,212 tests with 46 documented skips. Ruff,
strict mypy, and patch integrity remained green.

### I30b — RocketMQ token and legacy settlement isolation

The post-I30 fan-out found a second deterministic RED path: `pop_with_ack()`
published the same delivery into both its locked token and the unlocked legacy
last-message slot. A caller could therefore ack through `token=None` and then
nack the still-pending token, issuing two contradictory broker actions despite
the token state machine.

Only legacy `pop()` now publishes the legacy slot. Token-aware pop returns its
delivery exclusively through the token, matching the Kafka, RabbitMQ, Pulsar,
and SQS scheduler path. The concurrency regression also waits until the
competing thread has actually started before using liveness as lock evidence.
All 503 related tests passed on Python 3.10 and 3.14 with three real-broker tests
explicitly skipped; the full Python 3.10 suite passed 3,213 tests with 46
documented skips. Ruff, strict mypy, and patch integrity remained green.

### I31 — SQS immutable client generations

Four initial deterministic REDs showed that a repeated connect replaced and
leaked the live client, post-connect prefix/visibility mutations changed the
current client's behavior, disconnect could close the client while QueueUrl
resolution was still in progress, and operations could enter while
`client.close()` was blocked. Post-implementation fan-out found two more REDs:
an old tokenless ack could erase a replacement generation's legacy receipt,
and one slow QueueUrl lookup held a generation-wide cache lock that delayed an
unrelated queue's acknowledgement. Additional guards pin client construction
versus disconnect, admitted receipt settlement versus drain, retired-token
isolation, disconnected error context, and mutated-region revalidation.

SQS now atomically publishes one client generation containing a validated
operational snapshot plus generation-local QueueUrl and queue-lifecycle caches.
Every queue transaction holds a shared generation lease from URL resolution
through its final SDK call. QueueUrl discovery is single-flight per logical
queue while the shared cache lock protects only short dictionary operations,
so unrelated queues retain acknowledgement concurrency. Disconnect detaches
first, rejects new admissions, drains existing leases, clears diagnostics, and
closes exactly that client; connect/disconnect are serialized and a live
connect is idempotent. Receipt tokens carry only an opaque generation identity,
and the tokenless compatibility slot uses a locked receipt/epoch/generation
compare-and-clear, so no stale path can erase or call a replacement delivery.
Explicit reconnect is the only boundary at which changed operational settings
take effect; the shared AWS region validator is also rerun before client I/O.

All 512 related tests passed on Python 3.10 and 3.14 with one real-SQS test
explicitly skipped; the full Python 3.10 suite passed 3,225 tests with 46
documented skips. Ruff, strict mypy, and patch integrity remained green.

### I32 — RabbitMQ immutable connection generations

Four initial deterministic REDs showed that repeated `connect()` replaced and
leaked a healthy Pika session, candidate preparation failure could clear a
healthy peer's shared handles, disconnect could return before a private
candidate later resurrected the backend, and replacement left the old handles
open. The implementation audit added four more guards for queued connect
intents, retirement-before-replacement ordering, per-generation queue policy,
and timeout polling across reconnect. Post-implementation fan-out found two
additional RED outcomes: reconnect could publish while disconnect was still
closing an old unacknowledged generation, and the public `exclusive` queue
setting was silently omitted from Pika declaration and the snapshot.

RabbitMQ now serializes connection construction, builds mode-specific
connection/channel candidates without touching published state, and publishes
the complete channel session only after a lifecycle-epoch check. Healthy
connect is idempotent. Disconnect atomically advances the epoch and detaches the
published generation, so both an in-progress candidate and every already
queued connect intent become stale and close or return without resurrection.
An unhealthy generation is closed before its successor is constructed, and a
dedicated retirement barrier prevents any new generation from publishing until
a concurrent disconnect has closed the old handles. This makes channel close
the explicit broker redelivery boundary. Queue durability, auto-delete,
exclusivity, maximum-priority, and delivery mode come from the validated
connection snapshot, and a timed basic-get loop remains pinned to its starting
channel generation.

All 142 local RabbitMQ tests passed on Python 3.10 and 3.14 with five
real-broker tests explicitly skipped; the full Python 3.10 suite passed 3,234
tests with 46 documented skips. Ruff, strict mypy, Bandit, lockfile validation,
and patch integrity remained green.

### I33 — DynamoDB immutable table generations

Deterministic RED regressions showed that a table candidate was visible
before preparation completed, repeated or concurrent connects replaced live
handles, disconnect could be followed by a late candidate resurrection, and a
queued pre-disconnect connect intent could publish afterward. They also proved
that boto3 Resource calls overlapped, disconnect closed no HTTP client and did
not drain an active write, settings mutation took effect without reconnect,
and paginated clear or lazy TTL cleanup could splice table generations.

DynamoDB now gives every candidate a private boto3 Session, privately prepares
its Resource/Table, and publishes the complete set as one immutable generation
only after the table is data-plane usable. Live connect is idempotent. The epoch
is captured before the connect single-flight lock, so disconnect fences both an
in-progress candidate and every already queued intent. Checkpoints prevent a
known-stale candidate from continuing preparation, while an already admitted
`create_table` is drained as a persistent side effect. One re-entrant operation
lock serializes the non-thread-safe Resource API and forms the retirement
barrier: every public operation captures the authoritative generation once,
disconnect waits for an admitted operation, then closes
`resource.meta.client`. Compatibility handle mirrors are updated atomically but
never drive internal operations. Connection snapshots exclude credentials,
revalidate the AWS region before I/O, and take settings changes only at explicit
reconnect. Paginated clear and read-plus-lazy-delete remain on one Table; clear
is linearized only against this backend instance because a DynamoDB Scan is not
a cross-client snapshot transaction.

All 221 order-sensitive connector and DynamoDB tests passed on Python 3.10 and
3.14 with one real-service integration test explicitly skipped. The full Python
3.10 suite passed 3,266 tests with 46 documented skips. Ruff, strict mypy,
Bandit, lockfile validation, and patch integrity remained green.

### I34 — Bounded DynamoDB batch clear

The locked boto3 1.43.34 `BatchWriter` immediately requeues
`UnprocessedItems` and loops on context-manager exit without a retry limit or
sleep. Persistent throttling could therefore spin forever while holding the
DynamoDB operation/retirement lock, blocking every storage call and
`disconnect()`. Deterministic REDs also established that a replacement must
not use a mutated table name, retry sleep cannot release local write ordering,
malformed service responses must not inject new deletes, and cyclic Scan
cursors form a second liveness hazard.

Clear now pins one generation's Table, Resource client, and table-name snapshot
for the complete operation. Each physical batch contains at most 25 native
Resource-client delete requests and permits eight application-level
BatchWriteItem submissions, with up to seven full-jitter sleeps at a 50 ms
base. Only a structurally valid multiset subset of the current pending requests
is retried; foreign tables, PutRequests, duplicate amplification, out-of-prefix
Scan items, malformed Scan/BatchWrite shapes, and non-adjacent cursor cycles
fail closed. Cursor history uses fixed-size SHA-256 digests for linear lookup
without retaining full partition keys. SDK exceptions rely on botocore's own
inner retry policy and are never retried by this loop.

Every failure remains `StorageError(operation="clear_storage", key=None)` and
warns that accepted deletes make the result possibly partial; driver text is
kept only as `__cause__`, not copied into the public error or logs. The lock
covers Scan, BatchWrite, jitter sleep, and retry so local store/teardown cannot
interleave between attempts. Success means all requests observed by that Scan
were accepted, not that external writers left the table empty. The per-batch
6.35-second application-sleep maximum excludes page count, SDK retries, and
network timeouts and is not a global shutdown deadline.

The 33 focused batch-clear regressions passed on Python 3.10, including an
isolated real boto3 Resource/Stubber transformer test. On Python 3.14, all 234
related connector and DynamoDB tests passed with one real-service integration
test explicitly skipped. The final Python 3.10 suite passed 3,299 tests with 46
documented skips. Ruff, strict mypy, Bandit, lockfile validation, and patch
integrity remained green.

### I35 — AWS partition regions and DynamoDB delete responses

The specification split two remaining protocol boundaries from the larger SDK
ownership work. First, SQS and DynamoDB must accept the lowercase ASCII,
hyphen-delimited region identifiers used by every locked boto3 partition while
remaining a structural validator, not a static availability allowlist. Both
connect paths must revalidate mutated settings before SDK construction and
freeze the accepted value into the new generation. Second,
`DeleteItem(ReturnValues="ALL_OLD")` must map no `Attributes` to `False`, a
complete old item with the exact string `pk` requested to `True`, and every
malformed/mismatched envelope to typed `StorageError(operation="delete",
key=...)`. Failures must not copy response payloads or SDK diagnostics into
their text or `StorageError` domain fields, while real SDK failures preserve
their original `__cause__`. Traceback frame locals remain outside that
redaction boundary and must be handled as sensitive diagnostics.

Twenty initial RED outcomes exposed the old three-label regex across GovCloud,
ISO/ISO-B/ISO-E/ISO-F, and EUSC settings and showed that malformed delete
responses either returned an unreliable boolean or escaped the storage-error
contract. The implementation replaced that regex with an ASCII structural
grammar, kept exact validation at settings and connection boundaries, and
added generation-freeze regressions for SQS and DynamoDB. Delete response
interpretation now runs after the SDK exception boundary and fails closed on an
empty old item, a missing/wrong/non-string partition key, or a non-mapping
shape. This table-specific semantic check is intentional: botocore's generic
`AttributeMap` model is permissive, but AWS says an existing deletion returns
the entire old item and this backend's table schema requires string `pk`.
Top-level response metadata remains accepted.

The task also removed raw driver text from the public DynamoDB delete error,
added message/domain-field payload and driver secret-marker regressions,
documented the region and delete migration/operations contract, and retained
the original chained-cause identity.
Fan-out probes accepted all 46 SQS/DynamoDB regions exposed by locked botocore
1.43.34 across eight partitions and found no new deterministic ordering issue
in the legacy fake-boto3 test seams. An isolated real boto3 Resource/Stubber
regression pins the response-transformer boundary from wire `{"S": "key"}` to
the native string validated by the backend, including the missing-item path.

All 457 focused settings/SQS/DynamoDB tests passed on Python 3.10. The full
Python 3.10 and 3.14 suites each passed 3,354 tests with 46 documented skips.
Ruff, strict mypy, Bandit, lockfile validation, and patch integrity remained
green.

### I36 — Accurate repr-only redaction policy

The specification required a documentation correction, not a runtime string
change. Pydantic `SecretStr` masks structured display until explicitly
unwrapped; `_RedactedStr` is a real SDK-bound `str` that masks only direct and
repr-based container display while the wrapper survives; sensitive
`ConfigurationError.setting_value` is replaced in the exception domain field
with its separate stable marker. Ordinary `str`, default/`!s` f-string, `%s`,
formatting, encoding/concatenation, JSON, SDK diagnostics, arbitrary traceback
capture, and process memory are outside `_RedactedStr`'s guarantee. Changing
`__str__` would break authentication and still could not turn a string subclass
into a data classification boundary. This package's request/item JSON path also
explicitly unwraps supported Pydantic secret values.

One policy RED proved that `SECURITY.md` claimed both repr and string forms
were `***` and described SEC-1 as protecting repr, str, and logs, while the
executable contract has always been `repr(value) == "<redacted>"` and
`str(value) == secret`. The implementation task replaced that promise with a
three-column boundary matrix, made the README contract explicit, separated
RocketMQ credential repr protection from startup-error sanitization, corrected
older changelog scope language, and limited helper/test docstrings to
repr-based capture. A semantic policy regression parses the protected and
exposed table columns, while a behavior matrix pins f-string, `%s`, `format`,
and JSON exposure. No production behavior, public API, configuration, or data
migration changed; `_RedactedStr` remains Internal per `STABILITY.md`.

All eight focused redaction/policy tests passed on Python 3.10. The full Python
3.10 and 3.14 suites each passed 3,356 tests with 46 documented skips. Ruff,
strict mypy, Bandit, lockfile validation, and patch integrity remained green.

### I37 — SQS private SDK sessions

The specification closed the remaining ownership gap before an SQS client
generation is published. Boto3 Sessions are not thread-safe, while the
module-level `boto3.client()` alias resolves through one process-wide lazy
default Session. The backend's instance-local connect lock therefore could not
protect two independent backends constructing clients concurrently. That
default Session also caches ambient credential/provider state beyond one
backend generation, so a disconnect followed by changed environment
credentials could still sign through the older identity.

Post-implementation contract review exposed a second P1 at the same ownership
boundary: a cloud snapshot with no explicit endpoint allowed botocore to read
`AWS_ENDPOINT_URL_SQS` or `AWS_ENDPOINT_URL`, so an ambient HTTP URL bypassed
the package's cloud HTTPS validator. A real subprocess RED reproduced both
variables. Every client now receives
`BotoConfig(ignore_configured_endpoint_urls=True)`, available across the
declared boto3 1.34–1.x range, so a custom endpoint URL can come only from the
validated SQS setting while ambient credential providers and botocore's
standard FIPS/dual-stack endpoint selection remain enabled.

Three initial deterministic REDs forbade the module alias, overlapped two
independent backend connections, and required a private client-construction
failure to remain unpublished with its original cause. The completed matrix
also covers Session-constructor failure and fresh-candidate retry, idempotent
live connect, distinct reconnect ownership, exact region/endpoint/credential
kwargs, and configuration validation before SDK construction. An isolated
real boto3/botocore subprocess poisons the global default Session and uses a
Stubber-backed low-level SQS client to verify QueueUrl resolution, base64 send,
and exactly-once client close. A second real-SDK subprocess changes ambient
credentials across reconnect and proves that each new private Session resolves
the new identity without creating `boto3.DEFAULT_SESSION`.

Every `_SqsClientGeneration` now owns its private
`boto3.session.Session`, low-level client, immutable operational snapshot,
caches, and opaque key. The Session is retained for the generation lifetime
but never used on the data path and is not closed because boto3 Session has no
close contract; disconnect continues to drain leases and close only the
retired low-level client. No global construction lock or client-wide operation
lock was added, so independent generation construction and documented
low-level-client concurrency remain available.

This deliberately stops inheriting `boto3.setup_default_session(...)` and
event hooks installed only on that process-wide object. The README, changelog,
and migration guide route users to botocore's ambient credential providers or
the explicit SQS key pair. The SQS region setting is authoritative, custom
endpoint URLs can come only from the SQS endpoint setting, and normal
FIPS/dual-stack selection remains available; queue data and wire formats do
not change. All 189 selected SQS/AWS-region tests passed with the one live-service
integration test explicitly skipped. The full Python 3.10 and 3.14 suites each
passed 3,363 tests with 46 documented skips. Ruff, strict mypy, Bandit,
lockfile validation, dependency audit, and patch integrity remained green.

### I38 — real SDK collection isolation and Pulsar enum compatibility

The first RED reproduced a deterministic collection-order-dependent pytest
failure:
`test_connectors` created a `ModuleType("pulsar")` containing only `Client`,
then `test_backend_coverage2` retained it with `setdefault`; the first receive
failed before subscription because `ConsumerType` did not exist. Reversing the
files happened to pass only because a whole-module `MagicMock` fabricated every
missing attribute. A second real-SDK RED showed that this permissiveness had
hidden a production defect: the public `Key_Shared` setting looked up the
nonexistent Python attribute `ConsumerType.Key_Shared`, while Apache's 2.11
binding source and real 3.0, 3.8, and locked 3.12 clients all expose only
`ConsumerType.KeyShared`.

The expanded audit found eleven test modules replacing Pulsar, boto3, or
pymemcache during collection. Nine also had module-scoped cleanup fixtures that
ran only after all collection imports and unconditionally popped entries they
might not own; the other two left their replacements installed. Depending on
order, a backend could remain bound to an orphan stub while later imports
received the real SDK, or one module could delete a real SDK loaded by another.
Hand-written package stubs also lacked import specs, package paths,
parent-child wiring, or strict exception/enum surfaces.

The specification therefore withdrew the initial "complete shared stub"
proposal. The test dependency group already installs every backend SDK, so
ordinary tests now import those canonical modules and patch only `Client`,
`Session`, `AuthenticationToken`, or captured client constructors within each
test. Missing-dependency behavior remains isolated in the existing subprocess
tests. The production mapping keeps the stable public string `Key_Shared` but
resolves it strictly to `ConsumerType.KeyShared`, without a fallback that could
re-legitimize inaccurate stubs.

A subprocess module-import matrix uses `runpy.run_path` to exercise the
collection-time top levels of all eleven former injectors both forward and
reverse, from cold and SDK-preloaded states. It asserts real module metadata,
unchanged preloaded constructor/exception identities, and identical SDK
objects in `sys.modules` and each backend module. All four matrix cases and the
locked real-SDK enum seam passed. All 479 affected tests passed under a
randomized serial run, and 203 Pulsar/collection tests passed with two xdist
workers. The full Python 3.10 and 3.14 suites each passed 3,368 tests with 46
documented skips. Ruff, strict mypy, Bandit, lockfile validation, dependency
audit, and patch integrity remained green. Five independent post-implementation
reviews found no remaining reproducible P0/P1/P2 in the I38 scope.

### I39 — DynamoDB ambient endpoint isolation

Eight read-only audits re-ranked the remaining backend, lifecycle, security,
SDK, and release risks. The selected atomic task closes a narrow transport
policy gap before the larger Redis/Kafka/RocketMQ/MongoDB/Elasticsearch
generation changes: a cloud DynamoDB snapshot with no explicit endpoint still
allowed botocore to consume `AWS_ENDPOINT_URL_DYNAMODB`, `AWS_ENDPOINT_URL`, or
a shared-config service endpoint after package validation had completed.

Four REDs proved the boundary at both seams. The mocked private Resource call
received no botocore Config, and hermetic real-boto3 subprocesses resolved the
global environment variable, service-specific environment variable, and a
shared-config endpoint to an attacker-controlled HTTP target. Every Resource
candidate now receives `BotoConfig(ignore_configured_endpoint_urls=True)`, so
only the validated backend endpoint can customize routing. Ambient credential
providers and standard botocore FIPS/dual-stack endpoint selection remain
available; only ambient custom endpoint routing is isolated.

Post-implementation review found no production P0/P1 defect. It did catch a
fixed-hostname assertion and inherited AWS process state in the subprocess
test; the final regression removes every unrelated `AWS_*` variable, disables
metadata access, installs an explicit negative control, and asserts only the
actual security contract: HTTPS and rejection of the poisoned host. All 179
focused DynamoDB tests passed. The full Python 3.10 and 3.14 suites each passed
3,372 tests with 46 documented skips. Ruff, strict mypy, Bandit, lockfile
validation, dependency audit, and patch integrity remained green.

### I40 — Redis immutable connection generations

The specification replaced Redis's mutable client aliases with one
authoritative generation containing the validated connection snapshot, data
client, Sentinel master/control-plane owners, namespace, lease count, and
retirement signal. A candidate is now built and pinged privately, then
published only if its lifecycle epoch is still current. A concurrent
disconnect fences queued and in-flight connect intents; repeated connect is
idempotent; a failed candidate closes only its own handles. The public `client`
property remains a point-in-time, lazy-connect compatibility escape hatch, but
internal operations never use it and it carries no lease guarantee.

Every queue, set, storage, clear, blocking-pop, and health operation now leases
exactly one generation and derives keys from its frozen namespace. Disconnect
atomically stops admission, detaches the generation and compatibility mirrors,
wakes long polls, drains admitted work, and closes distinct data and Sentinel
control-plane handles outside the lifecycle condition. A blocking pop cannot
continue on a replacement client, and clear cannot scan on one client then
delete on another. Clear failures explicitly report possible partial
completion. Pop timeout validation rejects booleans, negative/non-finite
numbers, wrong types, and float overflow before I/O, and its deadline begins
before lazy connection and script setup.

Fresh implementation audits exposed and closed additional exception-safety
windows: publication interruption now rolls back and closes the candidate;
`retired.set()` interruption is retried while teardown still drains and closes;
the first `BaseException` is re-raised only after every distinct Sentinel
handle is visited; candidate-ping connect reentry fails fast; operation and
health leases decrement before their thread-local reentry guard is cleared;
and owner/flag ordering removes same-thread teardown entry/exit deadlocks.
Connection-time settings are copied and strictly revalidated field by field
without Pydantic serialization warnings that could expose a mutated secret.
Sentinel pool limits and Cluster full-coverage policy are forwarded exactly.

Forty-one deterministic Redis generation regressions passed, including the
four final audit REDs for retirement interruption, lease-release ordering,
candidate-ping reentry, and Sentinel cleanup after a master close interrupt.
The full isolated Python 3.10 and 3.14 suites each passed 3,413 tests with 46
documented skips. Ruff, strict mypy, Bandit, lockfile validation, dependency
audit, the two-worker compatibility canary, and patch integrity remained green.
Independent contract, SDK, security, and concurrency re-reviews found no
remaining reproducible P0/P1/P2 inside the I40 scope. Retry replay semantics
and remaining mode/SDK truth gaps are deliberately bounded as I41 and I42.

### I41 — Redis outcome-ambiguous command retry policy

The specification treats a connection, write, or response failure as
outcome-ambiguous: Redis may have committed a queue Lua script or another
mutation before the client observed the failure. Standalone, master-slave,
Sentinel-master, and Cluster data clients must therefore receive a fresh
`Retry(NoBackoff(), 0)` for every candidate generation. The policy retains
redis-py's default supported data-plane error classes so its failure callback
still disconnects a poisoned connection, but it never invisibly sends the data
command again. Server-confirmed non-execution paths remain distinct:
NOSCRIPT loading and Cluster MOVED/ASK/TRYAGAIN protocol continuation are not
disabled.

The Stable `retry_on_timeout` input remains parseable with its historical
`True` default, but both values are deprecated compatibility inputs and cannot
enable data replay. Explicit use emits a static `FutureWarning` when the
backend is constructed. The warning is deliberately attributed to a fixed
library line instead of caller source because Python's default renderer prints
the attributed line and could otherwise copy inline credentials into logs.
The separate Sentinel control setting now permits at most one immediate SDK
retry per request after Redis or socket timeout; its Retry policy does not
retry authentication failures, although Sentinel discovery may continue to a
different configured endpoint. ConnectionManager retries remain separate.

redis-py couples Cluster `ConnectionError`/`TimeoutError` and
ClusterDown/SlotNotCovered recovery to one outer retry count. Zeroing that
count therefore intentionally makes the latter two failures fail fast as the
conservative safety tradeoff; MOVED/ASK/TRYAGAIN routing remains intact. A
later caller or manager attempt is visible, creates or reuses lifecycle state
under the normal contract, and is not an SDK-hidden replay. The README,
changelog, stability policy, migration guide, runbook, example, and superseded
round-7 insight now state this boundary and the first-attempt ambiguity.

Nineteen dedicated regressions cover the historical default and schema,
explicit/flat/environment compatibility, safe warning attribution, fresh
policy identity, Sentinel timeout/auth separation, and all construction modes.
The execution seams use real redis-py `Script`, `Redis.execute_command`,
`RedisCluster` outer routing plus a real node pool, and a real
`SentinelConnectionPool` managed TLS connection. A simulated committed send
followed by a lost response produces the existing typed `QueueError`, preserves
the timeout as `__cause__`, disconnects the failed connection, and sends one
EVALSHA/EVAL in standalone push/pop, Cluster push, and Sentinel-master pop.

The final isolated Python 3.10 and 3.14 suites each passed 3,432 tests with 46
documented skips. The two-worker Redis compatibility canary passed 82 tests.
Ruff, strict mypy, configured Bandit, lockfile validation, dependency audit,
and patch integrity remained green. Three successive six-specialist review
waves found and closed the real-SDK execution gaps, warning lock/attribution
risk, Sentinel authentication retry, and documentation quantifier defects;
the final review found no remaining reproducible P0/P1/P2 in I41. Deployment
mode truthfulness, typed Cluster exceptions, Cluster DB/redirect controls,
primary-only master-slave behavior, and endpoint/userinfo hardening remain the
bounded I42 scope.
