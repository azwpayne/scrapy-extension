# Contributing

Thanks for contributing to `scrapy-extension`! This guide covers development
setup, running the tests (unit + integration), lint/coverage/build, and the CI
wiring.

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for environment and
dependency management.

```bash
uv sync --group test      # runtime + test deps (creates .venv)
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

The unit suite is mock-based (no live backends needed). Pytest runs with `--disable-socket` by default so unit tests cannot accidentally open real network connections; integration tests must opt back in with `--force-enable-socket`.

### Integration tests (require live backends)

Integration tests verify real-backend behavior the mocks cannot — atomicity,
ack/nack delivery semantics, and contract correctness. They are
**skip-by-default**, gated on environment variables — unset → the whole module
skips; set → it runs against that service.

| Backend | Env var | Example |
|---|---|---|
| Redis | `SCRAPY_TEST_REDIS_URL` | `redis://localhost:6379/0` |
| MongoDB | `SCRAPY_TEST_MONGODB_URI` | `mongodb://localhost:27017` |
| ElasticSearch | `SCRAPY_TEST_ES_HOSTS` | `http://localhost:9200` (comma-separated) |
| RabbitMQ | `SCRAPY_TEST_RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` |
| Kafka | `SCRAPY_TEST_KAFKA_BOOTSTRAP` | `localhost:9092` |
| RocketMQ | `SCRAPY_TEST_ROCKETMQ_NAMESRV` | `localhost:8081` (gRPC proxy, broker started with `--enable-proxy`) |

Run any subset by setting the relevant vars:

```bash
SCRAPY_TEST_REDIS_URL=redis://localhost:6379/0 \
  uv run pytest tests/integration -m integration -q --force-enable-socket
```

Each suite uses UUID-prefixed keys/topics so concurrent runs and leftover data
don't interfere. `SCRAPY_TEST_MONGODB_DB` optionally overrides the database.

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
uv run ruff check        # lint (whole project)
uv run ruff check --fix  # auto-fix safe issues
uv run mypy --strict src # verify the typed public package
uv run bandit -r src -c pyproject.toml # security scan first-party code
```

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
push/PR and runs strict mypy, Bandit, plus branch coverage on the minimum
supported Python lane. A separate integration job starts Redis,
MongoDB, ElasticSearch, RabbitMQ, Kafka, and RocketMQ, then exercises their
live-service suites with localhost sockets explicitly allowed. RocketMQ uses
the pure-Python Apache gRPC client and requires the broker proxy endpoint
(usually `localhost:8081`), not the legacy NameServer-only port.

## Architecture & rationale

- [`.claude/CLAUDE.md`](.claude/CLAUDE.md) — project overview, backend/component
  structure, multi-mode support, lazy imports.
- [`docs/code-review-2026-06-15.md`](docs/code-review-2026-06-15.md) — the
  multi-round adversarial review record: design rationale, every fixed bug, and
  the contract decisions the test suite enforces. The authoritative deep-dive.
