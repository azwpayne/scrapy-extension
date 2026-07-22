# Backend Plugins — Authoring a 3rd-Party Backend

> Back to [Codebase Overview](codebase-deep-insight.md) · [README](../README.md)

`scrapy-extension` ships 10 bundled backends (Redis, MongoDB, Kafka, RabbitMQ,
ElasticSearch, RocketMQ, Pulsar, SQS, Memcached, DynamoDB). You do **not** need to
fork the package to add another — any installed distribution can register a backend
through the `scrapy_extension.backends` entry-point group, and it is then selectable
via `SCRAPY_BACKEND_TYPE` exactly like a bundled one.

This document is the authoring contract for 3rd-party plugin authors. `BackendDescriptor` entry-point registration is currently **Experimental** (see [`STABILITY.md`](../STABILITY.md)): usable, tested, and intended for plugin authors, but still allowed to evolve in a minor `0.x` release until a third-party ecosystem validates the surface. The guide covers the entry-point shape, the descriptor dataclass, the lazy-import rule, bundled-wins precedence, and a worked end-to-end example.

## How Registration Works

At first use, the framework builds an in-memory registry of `BackendDescriptor`
records:

1. The 10 bundled backends are **statically seeded** from
   `src/scrapy_extension/backends/registry.py` (dotted-path strings only — no
   imports of the backend modules happen at registry-build time).
2. The framework then **discovers entry-points** in the `scrapy_extension.backends`
   group from every installed distribution and calls each registration callable.
3. Each callable returns a validated `BackendDescriptor`. Its `backend_type`
   must equal the entry-point name, and both class paths must be dotted Python
   identifier paths.
4. A unique descriptor is added under its `backend_type`. If multiple installed
   distributions claim the same third-party name, the registry rejects all of
   them rather than choosing one by environment-dependent discovery order.

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
  classes. Every dot-separated part must be a valid Python identifier, and the
  path must contain at least a module and attribute — see the lazy-import rule
  below.
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
warning log. This is deterministic and safe: a misbehaving plugin can never
shadow a bundled backend, and an application's Python warning filters cannot
turn discovery into an exception. Rename your entry-point to avoid the log
(`"myredis"`, `"acme_redis"`, …).

Two third-party entry-points with the same name are both rejected and logged as
an error. This avoids silently selecting whichever distribution metadata happens
to be enumerated last.

### Graceful skip on failure

If your registration callable raises (e.g. `ImportError` because an optional
helper is missing on the current platform), the framework **skips your plugin**
and emits a warning log. Invalid names, mismatched descriptor types, malformed
class paths, and unsupported capabilities follow the same path. The bundled 10
backends are unaffected — one broken 3rd-party plugin never breaks the registry,
even in applications that treat Python warnings as errors.

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
    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        self._queue.append((f"{priority:010.3f}", item))

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        if not self._queue:
            return None
        self._queue.sort(key=lambda pair: pair[0])
        return self._queue.pop(0)[1]

    def queue_len(self, queue_name: str) -> int:
        return len(self._queue)

    def clear_queue(self, queue_name: str) -> None:
        self._queue.clear()

    # -- SetBackend ----------------------------------------------------------
    def add(self, set_name: str, item: bytes) -> bool:
        key = f"{set_name}:{item.hex()}"
        if key in self._seen:
            return False
        self._seen.add(key)
        return True

    def contains(self, set_name: str, item: bytes) -> bool:
        return f"{set_name}:{item.hex()}" in self._seen

    def remove(self, set_name: str, item: bytes) -> None:
        self._seen.discard(f"{set_name}:{item.hex()}")

    def set_len(self, set_name: str) -> int:
        prefix = f"{set_name}:"
        return sum(1 for key in self._seen if key.startswith(prefix))

    def clear_set(self, set_name: str) -> None:
        prefix = f"{set_name}:"
        self._seen = {key for key in self._seen if not key.startswith(prefix)}

    # -- StorageBackend ------------------------------------------------------
    def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
        self._store[key] = data

    def retrieve(self, key: str) -> bytes | None:
        return self._store.get(key)

    def delete(self, key: str) -> bool:
        self._store.pop(key, None)
        return True

    def exists(self, key: str) -> bool:
        return key in self._store

    def ttl(self, key: str) -> int | None:
        return None  # demo only — real backends honour the TTL

    def clear_storage(self, prefix: str | None = None) -> None:
        if prefix is None:
            self._store.clear()
            return
        for key in list(self._store):
            if key.startswith(prefix):
                del self._store[key]
```

### Queue push durability

The concrete `QueueBackend.push()` contract is unchanged. The bundled
scheduler now classifies durability from a package-private receipt returned by
the exact backend/breaker generation that performs the push. Existing
third-party backends inherit a fail-closed implementation:

- an ordinary push calls the plugin's public `push()` exactly once and is
  treated as volatile for dedup-marker publication;
- a push that must replace and acknowledge an unacked broker delivery is
  rejected before the plugin's `push()` mutates its queue;
- publisher success, a strategy's legacy `is_push_durable()` claim, and custom
  queue return values are not substitutes for the operation receipt.

The in-process example above is intentionally volatile. There is currently no
Stable public opt-in for a third-party backend to mint the private receipt; do
not import or override `_QueuePushReceipt` in a published plugin. Use a bundled
backend for source-token handoff that must cross a worker-crash durable
boundary. A future plugin capability will need to bind any opt-in to the exact
connected generation/configuration rather than expose a separate probe.

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


## Compatibility Smoke Tests

Before publishing a plugin, run these checks in a fresh environment with your wheel installed. They make the prose contract executable.

```python
from scrapy.settings import Settings
from scrapy_extension.backends.connectors import resolve_backend_config
from scrapy_extension.backends.registry import get_descriptor, get_registry


def test_plugin_registry_discovery():
    registry = get_registry()
    assert "mybackend" in registry
    descriptor = get_descriptor("mybackend")
    assert descriptor.backend_cls_path == "mybackend_plugin.backends.MyBackend"
    assert descriptor.settings_cls_path == "mybackend_plugin.settings.MySettings"


def test_plugin_queue_capability_selected():
    settings = Settings({"SCRAPY_QUEUE_BACKEND_TYPE": "mybackend"})
    backend_type, backend_settings = resolve_backend_config(
        settings,
        type_key="SCRAPY_QUEUE_BACKEND_TYPE",
        settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
        required_capabilities={"queue"},
        component_name="queue",
    )
    assert backend_type == "mybackend"
    assert backend_settings == {}


def test_queue_only_plugin_rejects_storage():
    # Change the plugin descriptor in this test fixture to capabilities=frozenset({"queue"}).
    settings = Settings({"SCRAPY_STORAGE_BACKEND_TYPE": "mybackend"})
    try:
        resolve_backend_config(
            settings,
            type_key="SCRAPY_STORAGE_BACKEND_TYPE",
            settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
            required_capabilities={"storage"},
            component_name="storage",
        )
    except Exception as exc:
        assert exc.__class__.__name__ == "ConfigurationError"
    else:
        raise AssertionError("queue-only plugin must be rejected for storage")
```

Also verify the lazy-import rule manually: importing `scrapy_extension.backends.registry` and calling `get_registry()` must not import your backend driver module until the selected backend is actually constructed.

## Checklist for Plugin Authors

- [ ] Entry-point group is exactly `scrapy_extension.backends`.
- [ ] Entry-point name matches `^[a-z][a-z0-9_]*$` and equals `backend_type`.
- [ ] Registration callable returns a `BackendDescriptor` (paths only).
- [ ] `backend_cls_path` and `settings_cls_path` contain at least one dot and
      only valid Python identifier parts.
- [ ] Callable imports **only** `BackendDescriptor` from core — never the backend
      or settings module.
- [ ] `capabilities` is a subset of `{"queue", "set", "storage"}`.
- [ ] Queue-capable plugins accept the conservative volatile-receipt behavior;
      they do not rely on `QueueStrategy.is_push_durable()` to publish a
      persistent dedup marker or acknowledge a broker source.
- [ ] Compatibility smoke tests pass for registry discovery, capability selection, and unsupported-capability rejection.
- [ ] Lazy-import smoke test confirms registry discovery does not import the backend driver module.
- [ ] No name collision with a bundled or another third-party backend (bundled
      wins its collision; duplicate third-party names are all rejected).

## See Also

- [Codebase Overview](codebase-deep-insight.md) — backend implementation
  matrix, multi-mode support, connection management.
- [README](../README.md) — installation, quick start, backend configuration.
- `src/scrapy_extension/backends/base.py` — the `Backend` / `QueueBackend` /
  `SetBackend` / `StorageBackend` interfaces your class implements.
- `src/scrapy_extension/backends/registry.py` — the `BackendDescriptor`
  dataclass and `get_registry()` / `get_descriptor()` helpers.
