# SPEC — Round 8: Test Infrastructure (unit + benchmark + load + integration)

Companion to [`PLAN-round8-forward.md`](./PLAN-round8-forward.md). This SPEC
defines the **four test tiers** the round-8 `/goal` must establish, translating
the test-engineer's forward findings (F1/F2/F3/F4/F6/F7/F8) into concrete test
artifacts. Goal: move the suite from "proves lines execute" to "proves contracts
hold under adversarial input, scale, and real backends."

## Goals

1. **Unit** — close the coverage gaps on load-bearing paths the hardening lens
   left thin (MongoDB mode constructors, scheduler close-race, registry
   entry-point loading).
2. **Benchmark** — establish a perf-regression gate (the library markets
   "distributed" but has zero throughput tests; `pytest-benchmark` is configured
   and unused).
3. **Load** — prove correctness under scale + concurrency (10k-item memory
   stability; multi-thread in-flight-set safety). **Honest scope:** single-machine
   mock-based scale + concurrency correctness. Real-broker load is infra-gated
   (see Integration).
4. **Integration** — one real multi-backend e2e test (the v1.0 non-negotiable #3)
   + a docker-compose for local/CI broker fixtures.

## Constraints (non-negotiable)

- **Honest tests only** (PUA Integrity Guard active; `tests/` is scoring-sensitive):
  no disabled/weakened assertions, no over-broad try/except that masks failures,
  no fake benchmark numbers. `xfail`/`skip` permitted ONLY with an explicit
  reason pinning a real limitation (e.g. `skip(reason="needs SCRAPY_TEST_REDIS_URL — live broker")`).
- **TDD where applicable** — but for benchmark/load/integration the test IS the
  artifact (no RED-then-GREEN for a perf measurement; the gate is "runs + produces
  a defensible number", not "fails first").
- **Mock-based tiers must measure real properties** — benchmark measures the real
  serialization CPU path against a mock queue; load measures real memory
  (`tracemalloc`) and real thread-safety against a mock backend. No mocking away
  the thing under test.
- **Integration tests env-gated** — default-skip (no live brokers in `uv run pytest`);
  activate via `SCRAPY_TEST_<BACKEND>_URL` env vars. The test CODE must be honest
  and complete; only the RUN is gated.
- **No public API change** — these are tests + fixtures only. docker-compose is
  a new infra file (not shipped metadata).

## Tier definitions + acceptance

### Unit (Tier U)
- `tests/test_mongodb_modes_coverage.py` — mock pymongo; assert each `MongoDBMode`
  (STANDALONE/REPLICA_SET/SHARDED_CLUSTER/ATLAS) constructs a different
  client/URI (closes test-eng F6: mongodb 87.22%).
- `tests/test_scheduler_close_race.py` — spider-close retains in-flight ack
  state; `enqueue_request` after close is rejected gracefully (closes F7:
  scheduler 92.99% close-path).
- `tests/test_registry_entry_point_loading.py` — synthetic entry-point via
  `unittest.mock` on `importlib.metadata.entry_points`; assert descriptor
  returned + bundled-wins precedence (closes F8: registry 87.88%).
- **Acceptance:** `uv run pytest tests/test_mongodb_modes_coverage.py tests/test_scheduler_close_race.py tests/test_registry_entry_point_loading.py` green; coverage on the 3 target files ≥ 95%.

### Benchmark (Tier B)
- `tests/test_bench_serialization.py` — `pytest-benchmark` on
  `_request_to_dict` + `request_from_dict` round-trip (the scientist-measured
  4.30µs/2.81µs path). Asserts it RUNS and produces a number; no hard threshold
  (thresholds are a later hardening once baselines exist).
- `tests/test_bench_push_pop.py` — push N → pop N against a mock `QueueBackend`
  (measures the strategy + serialization CPU path, NOT broker RTT — that's
  integration's job). Asserts monotone: batch=10 ≥ batch=1 throughput.
- **Acceptance:** `uv run pytest tests/test_bench_*.py --benchmark-only` runs
  and reports; `--benchmark-disable` still lets the suite pass (benchmarks are
  opt-in, never block CI default).

### Reference models and production-memory scale

> Credibility correction: the former `test_load_scale.py` and
> `test_load_concurrency.py` names overstated what their local classes proved.

- `tests/test_reference_model_list_queue.py` — local Python-list memory and mass
  conservation only. It imports no production queue/backend code.
- `tests/test_reference_model_token_concurrency.py` — local token/set model under
  real Python threads. It imports no Kafka/RabbitMQ/SQS backend or SDK token.
- Both are marked `reference_model`, selected with `pytest -m reference_model`,
  and provide **no production backend evidence**.
- `tests/test_memory_membership_filter_scale.py` separately executes the
  production `MemoryMembershipFilter` at 10k entries. It provides evidence only
  for that in-process class, not shared-backend or multi-process behavior.
- **Acceptance:** reference-model collection and production-memory cases remain
  green, while filenames, symbols, marker contracts, and documentation make the
  evidence boundary explicit.

### Integration (Tier I)
- `tests/integration/test_multi_backend_e2e.py` — enqueue N requests →
  `BackendScheduler.next_request` → `BackendDupeFilter` → `BackendPipeline.process_item`
  across Redis (queue) + MongoDB (dedup) + ElasticSearch (storage); assert
  ordering + dedup-set membership + storage TTL. `skipif` no
  `SCRAPY_TEST_REDIS_URL` + `_MONGO_URI` + `_ES_URL`.
- `tests/integration/docker-compose.yml` — redis + mongo + elasticsearch
  (localstack or real) + kafka + rabbitmq for local/CI fixtures.
- **Acceptance:** test file is complete + honest; `--co` collects it; with env
  vars unset it skips cleanly; docker-compose `docker compose config` validates.

## Non-goals

- Real-broker load numbers (single-machine mock here; U3/Integration provides the
  infra path; true load-test campaigns need a broker harness beyond this round).
- Mutation testing (test-eng F1) — separate work unit (round-8 U6); this SPEC is
  the four explicit tiers the `/goal` named.
- Hard perf thresholds — baselines first; thresholds are a follow-up once the
  benchmark harness has CI history.
- Re-opening any round-7 ACCEPT'd item.
