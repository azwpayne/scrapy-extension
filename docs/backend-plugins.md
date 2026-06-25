# Backend Plugins — Authoring a 3rd-Party Backend

> Back to [Project Overview](../.claude/CLAUDE.md) · [README](../README.md)

`scrapy-extension` ships 10 bundled backends (Redis, MongoDB, Kafka, RabbitMQ,
ElasticSearch, RocketMQ, Pulsar, SQS, Memcached, DynamoDB). You do **not** need to
fork the package to add another — any installed distribution can register a backend
through the `scrapy_extension.backends` entry-point group, and it is then selectable
via `SCRAPY_BACKEND_TYPE` exactly like a bundled one.

This document is the contract for 3rd-party plugin authors. It covers the entry-point
shape, the `BackendDescriptor` dataclass, the lazy-import rule, bundled-wins
precedence, and a worked end-to-end example.

## How Registration Works

At first use, the framework builds an in-memory registry of `BackendDescriptor`
records:

1. The 10 bundled backends are **statically seeded** from
   `src/scrapy_extension/backends/registry.py` (dotted-path strings only — no
   imports of the backend modules happen at registry-build time).
2. The framework then **discovers entry-points** in the `scrapy_extension.backends`
   group from every installed distribution and calls each registration callable.
3. Each callable returns a `BackendDescriptor`. The descriptor is added to the
   registry under its `backend_type` string.

A plugin therefore consists of: a backend class, a pydantic-settings settings class,
a tiny zero-arg registration callable, and one line in `pyproject.toml`.

## The Contract

### Entry-point group

```toml
[project.entry-points."scrapy_extension.backends"]
mybackend = "mybackend_plugin.registration:register_mybackend"
```

- **Group**: `scrapy_extension.backends` (fixed).
- **Name**: the backend-type string — this is what users pass as
  `SCRAPY_BACKEND_TYPE`. Must match `^[a-z][a-z0-9_]*$` (lowercase ASCII, starts
  with a letter, underscores allowed). Example: `"mybackend"`.
- **Value**: dotted path to a **registration callable** (no args). The callable
  returns a `BackendDescriptor` (see below).

### `BackendDescriptor`

One registration declares the backend class, the settings class, and the capability
matrix in a single record:

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class BackendDescriptor:
    backend_type: str           # "mybackend" — matches the entry-point name
    backend_cls_path: str       # "mybackend_plugin.backends.MyBackend"
    settings_cls_path: str      # "mybackend_plugin.settings.MySettings"
    capabilities: frozenset[str]  # subset of {"queue", "set", "storage"}
```

- `backend_type` must match the entry-point name (and the `SCRAPY_BACKEND_TYPE`
  value).
- `backend_cls_path` and `settings_cls_path` are **dotted-path strings**, not
  classes — see the lazy-import rule below.
- `capabilities` declares which interfaces the backend implements. Selecting a
  backend for a capability it does not declare raises a `ConfigurationError`
  listing the capable backends.

### The Lazy-Import Rule (critical)

> The registration callable returns **paths only**. It must **not** import the
> backend module.

Why: the backend module imports its optional third-party dependency
(`redis`, `pymongo`, `kafka-python`, your own driver, …). The framework builds the
registry on first use of **any** backend — including from the core package, which
is expected to work with **no** backend deps installed. If your callable imports
the backend module at registration time, you would eager-import your driver for
every user of the core package, breaking the lazy-import guarantee.

Concretely, write this:

```python
# mybackend_plugin/registration.py
def register_mybackend():
    from scrapy_extension.backends.registry import BackendDescriptor
    return BackendDescriptor(
        backend_type="mybackend",
        backend_cls_path="mybackend_plugin.backends.MyBackend",
        settings_cls_path="mybackend_plugin.settings.MySettings",
        capabilities=frozenset({"queue", "set", "storage"}),
    )
```

Note that the only import inside the callable is the `BackendDescriptor` dataclass
itself (a tiny, dependency-free value object from core) — never the backend or
settings class. The framework imports `backend_cls_path` / `settings_cls_path`
lazily on first use of `mybackend`.

### Precedence — bundled wins

If an entry-point name collides with a bundled backend (e.g. a plugin also calls
itself `"redis"`), the **bundled descriptor wins** and the framework emits a
`UserWarning`. This is deterministic and safe: a misbehaving plugin can never
shadow a bundled backend. Rename your entry-point to avoid the warning
(`"myredis"`, `"acme_redis"`, …).

### Graceful skip on failure

If your registration callable raises (e.g. `ImportError` because an optional
helper is missing on the current platform), the framework **skips your plugin**
and emits a warning. The bundled 10 backends are unaffected — one broken
3rd-party plugin never breaks the registry.

## A Worked Example: `mybackend`

This is a minimal but complete plugin exposing all three capabilities
(queue / set / storage). It uses an in-process dict for storage so it has no
external dependencies and runs anywhere.

### Project layout

```
mybackend-plugin/
├── pyproject.toml
└── mybackend_plugin/
    ├── __init__.py
    ├── registration.py     # the entry-point callable
    ├── backends.py         # MyBackend (backend class)
    └── settings.py         # MySettings (pydantic settings)
```

### `pyproject.toml` (the one entry-point line)

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "mybackend-plugin"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "scrapy-extension",
    "pydantic-settings",
]

[project.entry-points."scrapy_extension.backends"]
mybackend = "mybackend_plugin.registration:register_mybackend"
```

The `[project.entry-points."scrapy_extension.backends"]` table is the entire wiring
step. After `pip install mybackend-plugin`, the framework discovers `mybackend`
automatically.

### `mybackend_plugin/registration.py` (returns paths only — no backend import)

```python
from scrapy_extension.backends.registry import BackendDescriptor


def register_mybackend() -> BackendDescriptor:
    return BackendDescriptor(
        backend_type="mybackend",
        backend_cls_path="mybackend_plugin.backends.MyBackend",
        settings_cls_path="mybackend_plugin.settings.MySettings",
        capabilities=frozenset({"queue", "set", "storage"}),
    )
```

### `mybackend_plugin/settings.py`

```python
from pydantic import BaseModel


class MySettings(BaseModel):
    """Settings for the mybackend plugin.

    Passed through SCRAPY_BACKEND_SETTINGS (dict) when mybackend is selected.
    """
    namespace: str = "mybackend"
```

### `mybackend_plugin/backends.py` (stub implementing the three interfaces)

```python
from scrapy_extension.backends.base import (
    QueueBackend,
    SetBackend,
    StorageBackend,
)


class MyBackend(QueueBackend, SetBackend, StorageBackend):
    """In-process backend for demonstration — no external service required."""

    backend_type = "mybackend"

    def __init__(self, settings: dict | None = None) -> None:
        self._settings = settings or {}
        self._queue: list[tuple[str, bytes]] = []   # (priority, item)
        self._seen: set[str] = set()
        self._store: dict[str, bytes] = {}

    # -- lifecycle -----------------------------------------------------------
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def is_connected(self) -> bool:
        return True
    def ping(self) -> bool:
        return True

    # -- QueueBackend --------------------------------------------------------
    def push(self, item: bytes, priority: int = 0) -> None:
        self._queue.append((f"{priority:010d}", item))

    def pop(self) -> bytes | None:
        if not self._queue:
            return None
        self._queue.sort(key=lambda pair: pair[0])
        return self._queue.pop(0)[1]

    def queue_len(self) -> int:
        return len(self._queue)

    def clear_queue(self) -> None:
        self._queue.clear()

    # -- SetBackend ----------------------------------------------------------
    def add(self, key: str) -> None:
        self._seen.add(key)

    def contains(self, key: str) -> bool:
        return key in self._seen

    def remove(self, key: str) -> None:
        self._seen.discard(key)

    def set_len(self) -> int:
        return len(self._seen)

    def clear_set(self) -> None:
        self._seen.clear()

    # -- StorageBackend ------------------------------------------------------
    def store(self, key: str, value: bytes, ttl: int | None = None) -> None:
        self._store[key] = value

    def retrieve(self, key: str) -> bytes | None:
        return self._store.get(key)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def exists(self, key: str) -> bool:
        return key in self._store

    def ttl(self, key: str) -> int | None:
        return None  # demo only — real backends honour the TTL
```

### Selecting it

Once `mybackend-plugin` is installed in the same environment as your Scrapy
project, select it exactly like a bundled backend:

```bash
export SCRAPY_BACKEND_TYPE=mybackend
```

```python
# settings.py
SCRAPY_BACKEND_TYPE = "mybackend"
SCRAPY_BACKEND_SETTINGS = {"namespace": "crawl_prod"}
```

Or bind it to a single component via multi-backend coexistence:

```python
SCRAPY_QUEUE_BACKEND_TYPE = "mybackend"
SCRAPY_QUEUE_BACKEND_SETTINGS = {"namespace": "crawl_queue"}
# dedup + storage still on Redis, MongoDB, etc.
```

Because the descriptor declares `{"queue", "set", "storage"}`, `mybackend` is
eligible for any of the three roles. If it declared only `{"queue"}`, selecting
it for dedup or storage would raise `ConfigurationError` with the list of
backends that *do* support the requested capability.

## Checklist for Plugin Authors

- [ ] Entry-point group is exactly `scrapy_extension.backends`.
- [ ] Entry-point name matches `^[a-z][a-z0-9_]*$` and equals `backend_type`.
- [ ] Registration callable returns a `BackendDescriptor` (paths only).
- [ ] Callable imports **only** `BackendDescriptor` from core — never the backend
      or settings module.
- [ ] `capabilities` is a subset of `{"queue", "set", "storage"}`.
- [ ] No name collision with a bundled backend (or accept the `UserWarning` and
      that the bundled descriptor wins).

## See Also

- [Project Overview (CLAUDE.md)](../.claude/CLAUDE.md) — backend implementation
  matrix, multi-mode support, connection management.
- [README](../README.md) — installation, quick start, backend configuration.
- `src/scrapy_extension/backends/base.py` — the `Backend` / `QueueBackend` /
  `SetBackend` / `StorageBackend` interfaces your class implements.
- `src/scrapy_extension/backends/registry.py` — the `BackendDescriptor`
  dataclass and `get_registry()` / `get_descriptor()` helpers.
