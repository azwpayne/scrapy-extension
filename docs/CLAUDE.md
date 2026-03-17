# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Scrapy extension package (`scrapy-extension`) that provides utilities for Scrapy web crawling projects. It's designed to work with Redis for distributed crawling and uses Pydantic Settings for configuration management.

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

# Run a single test file
uv run pytest tests/test_file.py

# Run all tests
uv run pytest

# Run tests with verbose output
uv run pytest -v

# Install the package in editable mode
uv pip install -e .
```

## Task Runner

This project uses **poethepoet** (configured in `pyproject.toml` under `[tool.poe.tasks]`). Check `pyproject.toml` for available tasks:

```bash
# List available tasks
poe --help

# Run a specific task
poe <task-name>
```

## Dependencies

### Runtime Dependencies
- `scrapy>=2.14.2` - Web crawling framework
- `redis>=7.3.0` - Redis client for distributed crawling
- `pydantic-settings>=2.13.1` - Configuration management

### Development Dependencies
- `pytest>=9.0.2` - Testing framework
- `poethepoet>=0.42.1` - Task runner

## Architecture

### Project Structure

```
src/scrapy_extension/    # Main package source
├── __init__.py          # Package entry point
└── py.typed             # PEP 561 type marker

tests/                   # Test files (empty - needs tests)
examples/                # Example usage (empty)
docs/                    # Documentation (empty)
```

### Package Design Patterns

This is a **Scrapy extension package**, so it should follow Scrapy's extension patterns:

1. **Extensions**: Subclass `scrapy.extensions.Extension` for extending Scrapy functionality
2. **Middlewares**: Implement downloader or spider middlewares
3. **Pipelines**: Implement item pipelines for data processing
4. **Settings**: Use Pydantic Settings for type-safe configuration with environment variable support

### Key Integration Points

- **Redis Integration**: The `redis` dependency suggests this extension provides Redis-based components (likely for distributed crawling, duplicate filtering, or scheduling)
- **Settings**: Use `pydantic-settings` to define configuration classes that can be populated from environment variables or settings files
- **Scrapy Integration**: Components should be registered via Scrapy's settings system (e.g., `EXTENSIONS`, `DOWNLOADER_MIDDLEWARES`, `ITEM_PIPELINES`)

## Testing

Tests are located in `tests/` directory (currently empty). The project uses **pytest**:

```bash
# Run tests
uv run pytest

# Run with coverage (if configured)
uv run pytest --cov=scrapy_extension

# Run specific test
uv run pytest tests/test_specific.py::test_function -v
```

## Type Hints

This project includes a `py.typed` marker file, indicating it provides type information. All new code should include type annotations.

## Development Workflow

1. Make changes to source code in `src/scrapy_extension/`
2. Run tests: `uv run pytest`
3. Sync dependencies if needed: `uv sync`
4. The `uv.lock` file should be committed when dependencies change
