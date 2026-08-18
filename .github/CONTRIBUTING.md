# Contributing

Thanks for contributing to `scrapy-extension`! This guide covers development
setup, running the tests (unit + integration), lint/coverage/build, and the CI
wiring.

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for environment and
dependency management.

```bash
uv sync --locked --group test  # exact locked runtime + test deps (creates .venv)
```

Python 3.10+ (`requires-python = ">=3.10"`). The CI matrix tests 3.10–3.14.

## Running the tests

### Unit tests (default — no live services required)

```bash
uv run pytest                                   # full suite (integration tests skip)
uv run pytest -m "not integration"              # explicitly exclude integration
uv run pytest tests/test_backends.py            # one file
uv run pytest tests/test_backends.py::TestRedisBackend::test_connect_success -v  # one test
```

The unit suite is mock-based (no live backends needed). Pytest runs with `--disable-socket` by default so unit tests cannot accidentally open real network connections. Integration runs must keep that boundary and explicitly allow only the loopback brokers with `--allow-hosts=localhost,127.0.0.1,::1`, as shown below.

### Integration tests (require live backends)

Integration tests verify real-backend behavior the mocks cannot — atomicity,
ack/nack delivery semantics, and contract correctness. They are
**skip-by-default** behind two gates: set `SCRAPY_TEST_INTEGRATION=1` to admit
the integration tier, then set the relevant backend variable to select its
service. Without either gate the matching tests skip, so a zero-exit skipped
run is not integration verification.

| Backend | Env var | Example |
|---|---|---|
| Redis | `SCRAPY_TEST_REDIS_URL` | `redis://localhost:6379/0` |
| MongoDB | `SCRAPY_TEST_MONGODB_URI` | `mongodb://localhost:27017` |
| ElasticSearch | `SCRAPY_TEST_ES_HOSTS` | `http://localhost:9200` (comma-separated) |
| RabbitMQ | `SCRAPY_TEST_RABBITMQ_URL` | `amqp://localhost:5672/` |
| Kafka | `SCRAPY_TEST_KAFKA_BOOTSTRAP` | `localhost:9092` |
| RocketMQ | `SCRAPY_TEST_ROCKETMQ_NAMESRV` | `localhost:8081` (gRPC proxy, broker started with `--enable-proxy`) |
| Pulsar | `SCRAPY_TEST_PULSAR_URL` | `pulsar://localhost:6650` |
| Amazon SQS | `SCRAPY_TEST_SQS_ENDPOINT` | `http://localhost:4566` (LocalStack) |
| Memcached | `SCRAPY_TEST_MEMCACHED_HOST` | `localhost` |
| DynamoDB | `SCRAPY_TEST_DYNAMODB_ENDPOINT` | `http://localhost:4566` (LocalStack) |

Run any subset by setting the global gate and the relevant backend vars:

```bash
SCRAPY_TEST_INTEGRATION=1 SCRAPY_TEST_REDIS_URL=redis://localhost:6379/0 \
  uv run pytest tests/integration -m integration -q \
    --allow-hosts=localhost,127.0.0.1,::1
```

Each suite uses UUID-prefixed keys/topics so concurrent runs and leftover data
don't interfere. `SCRAPY_TEST_MONGODB_DB` optionally overrides the database.
The SQS and DynamoDB suites can share one LocalStack endpoint.

For the full local backend matrix, first start the checked-in Compose fixtures:

```bash
docker compose --profile optional -f tests/integration/docker-compose.yml up -d --wait
```

Every published port binds to `127.0.0.1`; the fixtures deliberately use
development-only authentication/TLS settings and must not be exposed beyond the
local machine. The command below selects every service from that Compose file.
The AWS values are LocalStack-only test credentials, not real cloud credentials.

```bash
SCRAPY_TEST_INTEGRATION=1 \
SCRAPY_TEST_REDIS_URL=redis://localhost:6379/0 \
SCRAPY_TEST_MONGODB_URI=mongodb://localhost:27017 \
SCRAPY_TEST_ES_HOSTS=http://localhost:9200 \
SCRAPY_TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
SCRAPY_TEST_KAFKA_BOOTSTRAP=localhost:9092 \
SCRAPY_TEST_ROCKETMQ_NAMESRV=localhost:8081 \
SCRAPY_TEST_PULSAR_URL=pulsar://localhost:6650 \
SCRAPY_TEST_MEMCACHED_HOST=localhost \
SCRAPY_TEST_SQS_ENDPOINT=http://localhost:4566 \
SCRAPY_TEST_DYNAMODB_ENDPOINT=http://localhost:4566 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  uv run pytest tests/integration -m integration -q \
    --allow-hosts=localhost,127.0.0.1,::1

docker compose --profile optional -f tests/integration/docker-compose.yml down -v
```

### Full Python matrix (local)

`poe` tasks run the suite across Python versions (uv fetches the interpreters):

```bash
uv run poe test          # 3.10, 3.11, 3.12, 3.13, 3.14, 3.14t
uv run poe test-py310    # one version
```

> Note: `poe test-py314t` (free-threaded) runs on 3.14t, but `lxml` (a scrapy
> dependency via `parsel`) re-enables the GIL on import — so it verifies
> interpreter-compat, not GIL-free concurrency. See
> `docs/code-review-2026-06-15.md` Round 81.

## Lint, types, and format

```bash
uv run poe check         # read-only Ruff format/lint, strict Mypy, and Bandit checks
uv run poe format-fix    # explicitly rewrite src/, tests/, and conftest.py
uv run poe lint-fix      # explicitly apply Ruff lint fixes
uv run mypy --strict src # verify the typed public package
uv run bandit -r src -c pyproject.toml # security scan first-party code
```

`poe full`, `poe format`, and `poe lint` are compatibility aliases for
read-only checks. They never rewrite the worktree.

## Coverage

```bash
uv run pytest --cov=scrapy_extension --cov-report=term-missing
```

Target: **≥95%**. This is enforced by `tool.coverage.report.fail_under = 95`;
coverage commands fail below that floor, and CI runs the coverage command on
the Python 3.10 lane.

## Build

```bash
uv build                 # sdist + wheel → dist/
```

## CI

`.github/workflows/ci.yml` runs the unit suite across Python 3.10–3.14 on every
push/PR. Every lane syncs the locked environment against its declared Python
minor and asserts that interpreter before testing. The minimum supported lane
also runs strict mypy, Bandit, branch coverage, and an sdist/wheel installation
smoke test. A separate Python 3.12 integration job starts Redis, MongoDB,
ElasticSearch, RabbitMQ, Kafka, RocketMQ, Pulsar, Memcached, and LocalStack
(SQS and DynamoDB), then exercises their live-service suites with localhost
sockets explicitly allowed. RocketMQ uses the pure-Python Apache gRPC client
and requires the broker proxy endpoint (usually `localhost:8081`), not the
legacy NameServer-only port.

## Architecture & rationale

- [`.claude/CLAUDE.md`](../.claude/CLAUDE.md) — project overview, backend/component
  structure, multi-mode support, lazy imports.
- [`docs/code-review-2026-06-15.md`](../docs/code-review-2026-06-15.md) — the
  multi-round adversarial review record: design rationale, every fixed bug, and
  the contract decisions the test suite enforces. The authoritative deep-dive.
