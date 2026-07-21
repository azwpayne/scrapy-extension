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
- [ ] **SEC-01B — broker credential completeness.** Reject empty explicit
  secrets, URL userinfo, and mechanism-inconsistent authentication for Kafka,
  MongoDB, Elasticsearch, RabbitMQ, RocketMQ, and Pulsar without exposing secret
  values. Preserve valid mechanism-specific modes rather than requiring a
  universal username/password pair.
- [x] **SEC-02A — AWS cloud transport.** Reject URL userinfo and explicit
  plaintext SQS/DynamoDB endpoints in cloud mode while retaining HTTP support
  for explicit standalone LocalStack-compatible endpoints.
- [ ] **SEC-02B — broker transport.** Expose and propagate RocketMQ TLS and
  require it for cloud mode; secure Redis Sentinel's control plane; reject
  authenticated RabbitMQ/Pulsar connections without certificate and hostname
  verification; define an explicit trusted-network boundary for remote
  Memcached.
- [x] **SEC-03A — AWS validated connection snapshots.** Revalidate SQS/DynamoDB
  endpoint and credential fields at connect time and use one captured set of
  connection values, so construction-time mutation and validation/use races
  cannot select an unvalidated identity or endpoint. Sanitize endpoint failures.
- [ ] **SEC-03B — remaining validated connection snapshots.** Apply copied,
  revalidated connection snapshots and sanitized URL/URI failures to every
  remaining backend.
- [x] **TRANSPORT-01 — Pulsar TLS SDK contract.** Use the keyword names accepted
  by the locked Pulsar client, propagate hostname validation, and prove the TLS
  branch with a real-signature smoke test.
- [ ] **BACKEND-01 — Kafka consumer generations.** Fence tokens by assignment
  epoch and unique delivery attempt within one backend instance, invalidate on
  rebalance/nack/clear, validate admin responses, and wait for topic deletion
  before recreation. RUN-08 already supplies the cross-backend-incarnation
  fence; it does not make two deliveries of the same offset distinguishable.
- [ ] **BACKEND-04 — broker terminal and clear semantics.** Make direct
  Kafka/Pulsar/SQS token settlement one-shot and retryable, and specify honest
  lifecycle barriers for Kafka topic recreation, RabbitMQ in-flight deliveries,
  and SQS's asynchronous purge window.
- [x] **BACKEND-05 — SQS physical boundaries.** Map logical queue names to
  stable AWS-compatible names without changing already-valid names, and enforce
  the 786,432-byte raw payload ceiling imposed by base64 inside the 1 MiB SQS
  message limit before issuing network calls.
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
