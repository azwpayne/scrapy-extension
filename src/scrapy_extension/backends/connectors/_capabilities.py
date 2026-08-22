"""Capability gates over the bundled backend descriptor table.

The capability sets are backward-compatible, built-in-only immutable
snapshots so importing this package never triggers third-party discovery;
callers that need installed plugin capabilities must opt in through
:func:`capable_backends` or the registry API."""

from __future__ import annotations

import importlib
from typing import Any

from scrapy_extension.backends.registry import (
    _BUNDLED_DESCRIPTORS,
    get_registry,
)

# ---------------------------------------------------------------------------
# Backward-compat built-in capability sets. These are ``frozenset[str]`` so
# importing connectors never enumerates third-party entry points. Membership
# tests against both
# plain strings and ``BackendType`` members (which compare equal to their
# string ``.value``) work unchanged.
# ---------------------------------------------------------------------------
# Kept as module-level constants so existing call sites and tests that import
# them (e.g. ``tests/test_rocketmq_backend.py``) continue to compile. The
# underlying data lives in ``registry._BUNDLED_DESCRIPTORS``. Installed plugin
# capabilities are intentionally available only through ``capable_backends``.


def capable_backends(capability: str) -> frozenset[str]:
    """Explicitly discover and return backends declaring ``capability``."""
    return frozenset(
        name
        for name, descriptor in get_registry().items()
        if capability in descriptor.capabilities
    )


def _bundled_capable_backends(capability: str) -> frozenset[str]:
    """Return immutable built-in capability data without plugin discovery."""
    return frozenset(
        name
        for name, descriptor in _BUNDLED_DESCRIPTORS.items()
        if capability in descriptor.capabilities
    )


#: Built-in backends implementing :class:`~scrapy_extension.backends.base.QueueBackend`.
#: Third-party capability discovery is available explicitly via :func:`capable_backends`.
QUEUE_CAPABLE_BACKENDS: frozenset[str] = _bundled_capable_backends("queue")

#: Built-in backends implementing :class:`~scrapy_extension.backends.base.SetBackend`.
SET_CAPABLE_BACKENDS: frozenset[str] = _bundled_capable_backends("set")

#: Built-in backends implementing :class:`~scrapy_extension.backends.base.StorageBackend`.
STORAGE_CAPABLE_BACKENDS: frozenset[str] = _bundled_capable_backends("storage")


def _load_object(dotted_path: str) -> Any:
    """Lazily import and return the attribute at ``dotted_path``.

    Mirrors ``from <module> import <name>`` so tests that patch the canonical
    module attribute (e.g. ``scrapy_extension.backends.redis.RedisBackend``)
    still intercept the resolved class.

    Args:
        dotted_path: Fully-qualified ``module.submodule.Attr`` path.

    Returns:
        The resolved attribute.

    Raises:
        ValueError: If the path has no attribute separator.
        ImportError: If the module cannot be imported.
        AttributeError: If the attribute is missing from the module.
    """
    module_path, _, name = dotted_path.rpartition(".")
    if not module_path:
        msg = f"Invalid dotted path: {dotted_path!r}"
        raise ValueError(msg)
    module = importlib.import_module(module_path)
    return getattr(module, name)
