# Round 44 — SPEC / PLAN / TASK: direct-operation privacy boundaries

**Base:** `main` after Round 43 verification.

## Audit conclusion

The breaker proxy protects only calls made through a configured
`ConnectionManager`.  Its default is disabled, so direct bundled backend
operations still expose backend frames, endpoint/configuration objects,
logical queue/set/storage names, payloads, and driver exception chains.  The
same information is also reintroduced by a few public pipeline diagnostics.

The findings form five independent P1 slices rather than one broad rewrite:

1. Queue backends Redis, Kafka, RabbitMQ, and RocketMQ lack terminal direct
   operation boundaries.  Redis inherited `pop_with_ack()` and the static
   unsupported-depth operations need special handling.
2. MongoDB, Elasticsearch, DynamoDB, Memcached, and Redis direct set/storage
   operations can return a raw or trace-backed operational error.
3. Elasticsearch has a key-bearing successful-path warning and an unnormalised
   non-conflict request-error path.
4. `StoragePipeline` logs and rethrows raw key/error data in its threshold and
   backpressure paths, and forwards the raw error to a monitor extension point.
5. Pulsar/RocketMQ queue-depth capability failures are static in text but still
   retain backend configuration in their traceback graph.

## Specification

1. Every protected direct public operation must raise only a fresh terminal
   package exception after implementation frames unwind.  Its text, args,
   attributes, cause, context, formatted traceback, and package-frame locals
   must not reveal caller-controlled queue/set/key/prefix/payload or backend
   endpoint/credential data.
2. Queue failures use `QueueError` with a fixed operation and no queue name.
   Input validation runs before the boundary so existing `ValueError` contracts
   are preserved.  Token acknowledgement/retry behavior remains unchanged.
3. Storage failures use `StorageError` with a fixed operation and `key=None`.
   Set connection failures preserve `BackendConnectionError`, with a trusted
   literal bundled backend type; no source error attributes are trusted.
4. Explicit static capability errors keep their documented concrete type and
   approved fixed message, but are rebuilt only after private frames unwind.
   `KeyboardInterrupt`, `SystemExit`, and other `BaseException` control flow
   are never converted.
5. Elasticsearch keeps conflict-as-duplicate and transport-failure semantics;
   non-conflict request/API failures must be explicitly normalised to a safe
   non-transient package error before reaching the terminal boundary.
6. Pipeline diagnostics log fixed operation context only.  Public upstream
   errors and monitor error events must be freshly rebuilt without a key,
   item, raw backend text, or exception chain.  `StorageBackpressureError`
   admission semantics remain unchanged.  Serialization and lifecycle error
   contracts are deliberately separate follow-up slices because they currently
   expose payloads by documented API design.
7. Tests must exercise direct backend calls (not only the proxy) and walk the
   complete public exception graph, including traceback frame locals.

## Plan and task checklist

- [ ] Add the narrow shared terminal boundary primitives, without changing the
  behavior of unknown plugin exceptions.
- [ ] `fix(redis): redact direct queue operation errors` — include an explicit
  `pop_with_ack` override and preserve timeout validation.
- [ ] `fix(kafka): redact direct queue operation errors`.
- [ ] `fix(rabbitmq): redact direct queue operation errors`.
- [ ] `fix(rocketmq): redact direct queue operation errors` — include static
  depth capability reconstruction.
- [ ] `fix(queues): redact unsupported depth errors` — cover the shared
  Pulsar/RocketMQ `NotImplementedError` traceback primitive without changing
  scheduler fallback behavior or breaker state.
- [ ] `fix(mongodb): redact direct storage and set failures`.
- [ ] `fix(elasticsearch): redact direct storage and set diagnostics` — include
  the successful-path warning and non-conflict request errors.
- [ ] `fix(dynamodb): redact direct storage failures`.
- [ ] `fix(memcached): redact direct storage failures`.
- [ ] `fix(redis): redact direct set and storage failures`.
- [ ] `fix(pipeline): redact storage failure diagnostics`.
- [ ] Produce separate compatibility specs for pipeline serialization and
  lifecycle diagnostic surfaces before changing their documented behavior.
- [ ] Cover Pulsar static depth behavior (and any shared helper it requires)
  in the smallest compatible capability slice.
- [ ] Verify every slice with focused tests, then full non-integration coverage,
  Ruff, strict Mypy, security audit, build/install smoke tests, and final
  GitHub Actions for the exact pushed SHA.

## Acceptance criteria

- Direct driver failures cannot recover a synthetic marker from any public
  exception surface or package traceback local.
- Existing duplicate/conflict, retry/ack, validation, and unsupported-capability
  contracts are retained except for intentional removal of identifying error
  metadata.
- The coverage gate remains at or above 95%, and all local/GitHub CI jobs pass.
