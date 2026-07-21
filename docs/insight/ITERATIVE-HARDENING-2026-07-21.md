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
- [ ] **RUN-09 — accessor and close publication safety.** Capture one non-null
  backend value before returning from a lazy accessor, reject `None` snapshots,
  make `is_connected()` single-read, and disconnect falsey third-party backend
  implementations during close.
- [ ] **RUN-10 — monitor callback lock isolation.** Dispatch connect, retry, and
  disconnect monitor hooks outside manager locks so a re-entrant observer
  cannot self-deadlock or publish a competing connection transaction.
- [ ] **RUN-11 — circuit-breaker outcome epochs.** Attach an epoch to admitted
  calls so a failure from an older CLOSED generation cannot reopen a breaker
  that has since completed an OPEN → HALF_OPEN → CLOSED recovery.
- [ ] **RUN-03 — multi-spider manager scope.** Resolve `{spider}` before manager
  acquisition for crawler-owned construction, and use an unshareable fallback
  for unresolved direct construction, so Kafka and RocketMQ consumers are not
  accidentally shared across spiders. Apply the same isolation to backend
  spider mixins without mutating a manager's registry key after acquisition.
- [x] **RUN-04A — post-commit source acknowledgement.** Once a replacement is
  durably pushed, a source-token ack failure remains observable but cannot
  reject the push or roll back its dedup reservation; retain the unresolved
  token and let broker redelivery reach the duplicate-ack path.
- [ ] **RUN-04B — errback replacement acknowledgement.** Transfer/defer the
  source token for requests and iterables returned by user errbacks until each
  replacement is durably accepted. Explicitly document the unavoidable
  publish/ack crash gap and define safe behavior for delayed local strategies.
- [ ] **RUN-04C — durable replacement strategies and snapshots.** Do not ACK a
  token-bearing source after its replacement has only entered an in-process
  delay, time-wheel, round-robin, or ring buffer. Preserve recovery snapshots
  until a later clean checkpoint so a crash during restore is replay-safe.
- [ ] **SEC-01 — credential completeness.** Reject partial credential pairs and
  empty explicit secrets, URL userinfo, and mechanism-inconsistent
  authentication for Kafka, MongoDB, Elasticsearch, RabbitMQ, RocketMQ,
  Pulsar, SQS, and DynamoDB without exposing secret values. Preserve valid
  ambient-credential and mechanism-specific modes rather than requiring a
  universal username/password pair.
- [ ] **SEC-02 — cloud transport.** Reject explicit plaintext AWS endpoints in
  cloud mode; expose and propagate RocketMQ TLS and require it for cloud mode;
  secure Redis Sentinel's control plane; reject authenticated RabbitMQ/Pulsar
  connections without certificate and hostname verification; define an
  explicit trusted-network boundary for remote Memcached.
- [ ] **SEC-03 — validated immutable connection snapshots.** Revalidate a copied
  settings snapshot at every backend build/connect boundary so post-construction
  mutation cannot bypass transport or credential validators. Sanitize every
  URL/URI error before it reaches an exception or log record.
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
- [ ] **BACKEND-05 — SQS physical boundaries.** Map logical queue names to
  stable AWS-compatible names without changing already-valid names, and enforce
  the 786,432-byte raw payload ceiling imposed by base64 inside the 1 MiB SQS
  message limit before issuing network calls.
- [ ] **BACKEND-02 — Elasticsearch commit ambiguity.** Never report an empty
  queue solely because optimistic-claim conflicts exhausted a small retry
  count. Give pushes stable identities, make claim/delete/set writes safe under
  ambiguous transport outcomes, reject partial search/count results, and make
  clear cover unrefreshed writes.
- [ ] **BACKEND-03 — DynamoDB consistency and clear scope.** Use consistent reads
  where the storage contract promises immediate visibility, preserve an empty
  string as a real prefix instead of a whole-table clear, and enforce DynamoDB's
  item-size and persisted-value error boundaries.

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
