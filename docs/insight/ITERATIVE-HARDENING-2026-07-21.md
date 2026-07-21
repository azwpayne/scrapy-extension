# Iterative hardening programme — 2026-07-21

This document is the executable result of an eight-reader parallel audit of
architecture, backend correctness, runtime lifecycle, tests/CI, public API and
typing, security/configuration, release documentation, and independent bug
hunting. It is maintainer planning evidence, not a promise that every item is
already fixed. Public behaviour remains defined by `README.md`, `STABILITY.md`,
and the package API.

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
| I8 | Harden configuration security invariants | partial credentials and insecure cloud endpoint combinations fail fast |
| I9 | Repair multi-spider and acknowledgement lifecycle invariants | spider scopes are isolated; replacement requests never acknowledge early |
| I10 | Repair backend-specific P1 correctness | Kafka assignment epochs, ES contention, DynamoDB consistent reads |
| I11 | Re-audit contracts, docs, CI, and remaining P2 items | stop condition met or next bounded iteration selected |

The order may change when a regression test disproves a hypothesis or exposes a
smaller prerequisite. A disproved finding is removed rather than replaced with
speculative work.

## Task register

### Active P0/P1 tasks

- [ ] **PKG-01 — canonical release artifacts.** Move `CHANGELOG.md`,
  `CONTRIBUTING.md`, `LICENSE`, `SECURITY.md`, and `STABILITY.md` back to the
  root; restore all public links; assert wheel/sdist contents and metadata.
- [ ] **TEST-01 — complete test dependency group.** Add direct test dependencies
  for every backend module exercised by public lazy imports; prove the tests pass
  in a fresh synced environment and in an adversarial order.
- [ ] **TEST-02 — RocketMQ import isolation.** Replace unit-test patching that
  imports the real SDK with a controlled module stub. Run the one real SDK
  contract check in a subprocess with a temporary home directory.
- [ ] **TEST-03 — supported-Python truth.** Correct the Hypothesis expected-meta
  oracle for reserved keys. Resolve the exact Python 3.14 itemadapter/Pydantic-v1
  warning without hiding unrelated user warnings.
- [ ] **COMPAT-01 — Scrapy pipeline hooks.** Accept both legacy explicit-spider
  calls and current crawler-owned/omitted-spider calls for open, close, and item
  processing; prove registration emits no deprecation warning.
- [ ] **PLUGIN-01 — descriptor boundary.** Validate entry-point name,
  `backend_type`, dotted class paths, and duplicates. Logging a broken plugin
  must not become an exception under warnings-as-errors.
- [ ] **RUN-01 — circuit-breaker boundary.** Treat breaker-open queue reads and
  dedup checks as expected temporary backend failures rather than scheduler
  crashes.
- [ ] **RUN-02 — reconnect contract.** A manager with a disconnected backend
  must actually reconnect instead of returning solely because an object exists.
- [ ] **RUN-03 — multi-spider manager scope.** Resolve `{spider}` before manager
  acquisition so consumer-bearing backends are not accidentally shared across
  spiders.
- [ ] **RUN-04 — replacement-request acknowledgement.** Transfer/defer the
  source token until a replacement request is durably accepted; distinguish
  "replacement committed, source ack failed" from a failed push so dedup state
  is not rolled back.
- [ ] **SEC-01 — credential completeness.** Reject partial credential pairs and
  mechanism-inconsistent authentication for Kafka, MongoDB, Elasticsearch, and
  RocketMQ without exposing secret values.
- [ ] **SEC-02 — cloud transport.** Reject explicit plaintext AWS endpoints in
  cloud mode; expose and propagate RocketMQ TLS, then require it for cloud mode
  with migration notes.
- [ ] **BACKEND-01 — Kafka assignment epoch.** Invalidate stale ack/nack tokens
  on rebalance and queue clear; wait for asynchronous topic deletion before
  recreation.
- [ ] **BACKEND-02 — Elasticsearch contention.** Never report an empty queue
  solely because optimistic-claim conflicts exhausted a small retry count.
- [ ] **BACKEND-03 — DynamoDB read-after-write.** Use consistent reads for
  retrieve, exists, and TTL where the storage contract promises immediate
  visibility.

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
- align examples, release runbook commands, plugin example semantics, and
  remaining historical-document links with current behaviour.

### Evidence policy

An item can be marked complete only when its regression test fails against the
old behaviour (where applicable), passes after the change, and the relevant
global gates stay green. External integration items remain explicitly
"unverified locally" unless a real broker or cloud service was exercised; mocks
must not be described as end-to-end evidence.
