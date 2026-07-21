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
- [ ] **CONCURRENCY-01D — DynamoDB table generations.** Publish resource/table
  handles together, serialize the non-thread-safe boto3 Resource API, and keep
  every multi-page or lazy-TTL operation on one generation.
- [ ] **CONCURRENCY-01E — Redis connection generations.** Bind namespace and
  all SDK handles to a leased generation so clear and blocking-pop loops cannot
  cross reconnect or lazily resurrect themselves after disconnect.
- [ ] **CONCURRENCY-01F — Kafka client generations.** Publish producer, admin,
  consumer, validated settings, and topic-policy caches as one generation;
  failed candidates must not clear a healthy generation.
- [ ] **CONCURRENCY-01G — RocketMQ client generations.** Publish producer,
  consumer, validated settings, and subscription cache as one generation so a
  replacement consumer cannot inherit a stale subscribed-topic hit.
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
