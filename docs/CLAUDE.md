# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `scrapy-extension`, a Scrapy extension package providing distributed crawling capabilities with support for multiple backends: Redis (implemented), MongoDB, Kafka, and RabbitMQ (planned).

## Build System & Package Management

This project uses **uv** for Python package management and building:

- **Build backend**: `uv_build`
- **Python version**: 3.10+
- **Package source**: `src/scrapy_extension/`
- **Lock file**: `uv.lock`

### Common Commands

```bash
# Install dependencies (including dev)
uv sync

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_backends.py

# Run a specific test
uv run pytest tests/test_backends.py::TestRedisBackend::test_connect_success -v

# Run tests with verbose output
uv run pytest -v
```

## Architecture

### Backend Abstraction

The project uses a protocol-based backend abstraction defined in `src/scrapy_extension/backends/base.py`:

- **`Backend`**: Base protocol for all backends (connect, disconnect, ping)
- **`QueueBackend`**: Priority queue operations (push, pop, len, clear)
- **`SetBackend`**: Set operations for duplicate filtering (add, contains, remove, len)
- **`StorageBackend`**: Key-value storage with TTL support (store, retrieve, delete, exists)

All backends implement these protocols. Currently only **Redis** is implemented via `RedisBackend` class using:
- Sorted Sets for queues (priority ordering)
- Sets for duplicate filtering
- Strings with TTL for storage

### Scrapy Components

Components in `src/scrapy_extension/components/` wrap backend interfaces for Scrapy integration:

- **`BackendQueue`**: Request serialization/deserialization, uses `QueueBackend`
- **`BackendScheduler`**: Scrapy scheduler using `BackendQueue` + `SetBackend` for deduplication
- **`BackendDupeFilter`**: Distributed duplicate filter using `SetBackend`
- **`BackendPipeline`**: Item storage pipeline using `StorageBackend`
- **`BackendSpiderMixin`**: Mixin for spiders to easily access backend components

### Configuration

Uses **pydantic-settings** with environment variable support:

- **`Settings`**: Global settings (backend type, retry config)
- **`RedisSettings`**: Redis-specific settings (`SCRAPY_REDIS_HOST`, `SCRAPY_REDIS_PORT`, etc.)

### Connection Management

**`ConnectionManager`** (`src/scrapy_extension/connection/manager.py`): Lazy singleton pattern with retry logic. Manages backend lifecycle and provides access to typed backend interfaces.

### Request Serialization

`BackendQueue` implements custom `_request_to_dict()` method (not using Scrapy's `request_to_dict`) to serialize:
- URL, method, headers, body, cookies
- Callback/errback function names
- Meta, encoding, priority, flags, dont_filter

Uses `JSONSerializer` for encoding/decoding.

## Key Integration Points

- Register components in Scrapy settings:
  - `SCHEDULER = "scrapy_extension.components.scheduler.BackendScheduler"`
  - `DUPEFILTER_CLASS = "scrapy_extension.components.dupefilter.BackendDupeFilter"`
  - `ITEM_PIPELINES = {"scrapy_extension.components.pipeline.BackendPipeline": 300}`

- Use `BackendSpiderMixin` and call `setup_backend()` in spider `__init__`
- Backend type selection via `Settings.backend_type` or `SCRAPY_BACKEND_TYPE` env var

## Testing

Tests use pytest with mocked backends (no real Redis/MongoDB/Kafka/RabbitMQ required):

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v
```

## Type Hints

Full type annotations required. Project includes `py.typed` marker file.

## Dependencies

### Runtime
- `scrapy>=2.14.2` - Web crawling framework
- `redis>=7.3.0` - Redis client (current MVP backend)
- `pymongo>=4.5.0` - MongoDB client (planned)
- `kafka-python>=2.0.2` - Kafka client (planned)
- `pika>=1.3.2` - RabbitMQ client (planned)
- `pydantic-settings>=2.13.1` - Configuration management

### Development
- `pytest>=9.0.2` - Testing framework
- `ruff>=0.15.6` - Linting/formatting
