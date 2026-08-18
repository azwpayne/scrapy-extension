"""Unified backend descriptor registry with entry-point plugin discovery.

Round-5 Unit R5-1 (Tier-2 debt paydown): consolidates the four prior
hand-synced registries in ``connectors.py`` (``_BACKEND_FACTORIES`` +
``QUEUE_CAPABLE_BACKENDS`` / ``SET_CAPABLE_BACKENDS`` /
``STORAGE_CAPABLE_BACKENDS``) into ONE ``BackendDescriptor`` table, and
adds 3rd-party plugin discovery via ``importlib.metadata.entry_points``.

Lazy-import invariant (load-bearing):

  ``_BUNDLED_DESCRIPTORS`` stores dotted-path STRINGS only. It NEVER imports
  a backend module at registry-build time. A 3rd-party plugin's registration
  callable is also expected to return PATH strings, not the imported class —
  this is the documented 3rd-party contract in ``docs/backend-plugins.md``.
  This keeps the round-2 promise: ``import scrapy_extension`` works with NO
  optional backend dependency installed; backends load on demand via
  :func:`scrapy_extension.backends.connectors._load_object`.

3rd-party contract:

  - **Group**: ``scrapy_extension.backends``.
  - **Name**: backend-type string (``^[a-z][a-z0-9_]*$``) — the
    ``SCRAPY_BACKEND_TYPE`` value.
  - **Value**: dotted path to a registration CALLABLE (no args) returning a
    :class:`BackendDescriptor`. ONE registration declares the backend class,
    settings class, AND capability matrix — no editing other registries.

Precedence: bundled-wins on name conflict + warning log (deterministic and
observable). A broken plugin callable (any ``Exception``) is skipped + logged —
never propagated — so one bad plugin cannot break the bundled set even when an
application promotes Python warnings to exceptions.
"""

from __future__ import annotations

import importlib.metadata
import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from scrapy_extension.backends.base import BackendType
from scrapy_extension.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def _log_diagnostic(
    log_call: Callable[..., object],
    message: str,
    *args: object,
    **kwargs: object,
) -> None:
    """Emit a best-effort registry diagnostic without changing discovery flow.

    Entry-point enumeration and ordinary plugin failures are explicitly
    recoverable: bundled backends must remain usable even if an application's
    logging handler raises a control-flow exception while reporting that skip.
    This boundary is deliberately limited to logging.  A plugin's own
    ``BaseException`` still propagates from discovery unchanged.
    """
    try:
        log_call(message, *args, **kwargs)
    except BaseException:
        pass


#: Entry-point group 3rd-party packages use to register a backend.
_ENTRY_POINT_GROUP: Final[str] = "scrapy_extension.backends"

#: The three interface capabilities a backend may implement.
_VALID_CAPABILITIES: Final[frozenset[str]] = frozenset({"queue", "set", "storage"})

#: Backend-type name validator (matches the 3rd-party contract).
_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class BackendDescriptor:
    """Frozen description of one backend (bundled or 3rd-party).

    Attributes:
        backend_type: The backend-type string (the ``SCRAPY_BACKEND_TYPE``
            value). For bundled backends this matches ``BackendType.X.value``;
            3rd-party backends are plain strings.
        backend_cls_path: Dotted path to the backend class
            (e.g. ``"scrapy_extension.backends.redis.RedisBackend"``).
            PATH STRING ONLY — never the imported class. The registry is
            lazy-import-safe: building it must not import any backend module.
        settings_cls_path: Dotted path to the pydantic settings class.
        capabilities: Frozen subset of ``{"queue", "set", "storage"}`` —
            the interfaces the backend implements. Used by
            :func:`scrapy_extension.backends.connectors.resolve_backend_config`
            to fail-fast when a component requests an unsupported interface.
    """

    backend_type: str
    backend_cls_path: str
    settings_cls_path: str
    capabilities: frozenset[str]


# ---------------------------------------------------------------------------
# Bundled descriptors — PATHS ONLY (no eager import).
# ---------------------------------------------------------------------------
# Consolidated from the prior ``_BACKEND_FACTORIES`` + the three capability
# sets in ``connectors.py``. Each entry's class/settings paths are the SAME
# dotted strings the old table held — so dispatch behavior is byte-identical
# for every bundled backend. The capability frozenset mirrors the old
# QUEUE_CAPABLE / SET_CAPABLE / STORAGE_CAPABLE membership exactly:
#
#   QUEUE:    redis, mongodb, kafka, rabbitmq, elasticsearch,
#             rocketmq, pulsar, sqs
#   SET:      redis, mongodb, elasticsearch
#   STORAGE:  redis, mongodb, elasticsearch, memcached, dynamodb
#
# This is the single source of truth — there is no longer a 4-way hand-sync.
_BUNDLED_DESCRIPTORS: dict[str, BackendDescriptor] = {
    "redis": BackendDescriptor(
        backend_type="redis",
        backend_cls_path="scrapy_extension.backends.redis.RedisBackend",
        settings_cls_path="scrapy_extension.settings.RedisSettings",
        capabilities=frozenset({"queue", "set", "storage"}),
    ),
    "mongodb": BackendDescriptor(
        backend_type="mongodb",
        backend_cls_path="scrapy_extension.backends.mongodb.MongoDBBackend",
        settings_cls_path="scrapy_extension.settings.MongoDBSettings",
        capabilities=frozenset({"queue", "set", "storage"}),
    ),
    "kafka": BackendDescriptor(
        backend_type="kafka",
        backend_cls_path="scrapy_extension.backends.kafka.KafkaBackend",
        settings_cls_path="scrapy_extension.settings.KafkaSettings",
        capabilities=frozenset({"queue"}),
    ),
    "rabbitmq": BackendDescriptor(
        backend_type="rabbitmq",
        backend_cls_path="scrapy_extension.backends.rabbitmq.RabbitMQBackend",
        settings_cls_path="scrapy_extension.settings.RabbitMQSettings",
        capabilities=frozenset({"queue"}),
    ),
    "elasticsearch": BackendDescriptor(
        backend_type="elasticsearch",
        backend_cls_path="scrapy_extension.backends.elasticsearch.ElasticSearchBackend",
        settings_cls_path="scrapy_extension.settings.ElasticSearchSettings",
        capabilities=frozenset({"queue", "set", "storage"}),
    ),
    "rocketmq": BackendDescriptor(
        backend_type="rocketmq",
        backend_cls_path="scrapy_extension.backends.rocketmq.RocketMQBackend",
        settings_cls_path="scrapy_extension.settings.RocketMQSettings",
        capabilities=frozenset({"queue"}),
    ),
    "pulsar": BackendDescriptor(
        backend_type="pulsar",
        backend_cls_path="scrapy_extension.backends.pulsar.PulsarBackend",
        settings_cls_path="scrapy_extension.settings.PulsarSettings",
        capabilities=frozenset({"queue"}),
    ),
    "sqs": BackendDescriptor(
        backend_type="sqs",
        backend_cls_path="scrapy_extension.backends.sqs.SqsBackend",
        settings_cls_path="scrapy_extension.settings.SqsSettings",
        capabilities=frozenset({"queue"}),
    ),
    "memcached": BackendDescriptor(
        backend_type="memcached",
        backend_cls_path="scrapy_extension.backends.memcached.MemcachedBackend",
        settings_cls_path="scrapy_extension.settings.MemcachedSettings",
        capabilities=frozenset({"storage"}),
    ),
    "dynamodb": BackendDescriptor(
        backend_type="dynamodb",
        backend_cls_path="scrapy_extension.backends.dynamodb.DynamoDBBackend",
        settings_cls_path="scrapy_extension.settings.DynamoDBSettings",
        capabilities=frozenset({"storage"}),
    ),
}


# ---------------------------------------------------------------------------
# Memoized registry (bundled + discovered entry-points).
# ---------------------------------------------------------------------------

_registry_cache: dict[str, BackendDescriptor] | None = None
_registry_lock = threading.Lock()
_registry_condition = threading.Condition(_registry_lock)
_registry_discovery_owner: int | None = None
_registry_discovery_in_progress = False


def _discover_entry_points() -> dict[str, BackendDescriptor]:
    """Discover 3rd-party backend descriptors via ``importlib.metadata``.

    Uses the modern ``entry_points(group=...)`` keyword API, available on
    every supported Python (>=3.10). The former ``sys.version_info < (3, 12)``
    branch — which used the legacy dict shape ``entry_points().get(group, [])``
    — was based on the false premise that the keyword form was unavailable
    before 3.12; it has been available since 3.10. The dict fallback emitted
    ``SelectableGroups dict interface is deprecated`` on every 3.10/3.11 run
    (5 warnings in the suite) and the dict interface was removed in 3.12.
    Keyword-only is correct, universal, and warning-free.

    Each entry-point's registration callable is invoked via ``ep.load()``.
    A broken callable (any ``Exception`` — typically ``ImportError`` when a
    plugin's optional dep is missing) is logged + SKIPPED. This is
    the load-bearing graceful-skip invariant: one bad plugin must never
    break the bundled set (round-5 R5-1 Test 5).

    Args:
        None.

    Returns:
        A dict of descriptors discovered via entry-points. Bundled backends
        are NOT included here — :func:`get_registry` merges them with
        bundled-wins precedence.
    """
    enumeration_failed = False
    try:
        # ``entry_points(group=...)`` is the modern select-by-group API,
        # available on every supported Python (>=3.10). The legacy dict form
        # ``entry_points().get(group, [])`` was formerly used on a
        # ``sys.version_info < (3, 12)`` branch, but the keyword form has been
        # available since 3.10 — the branch was based on a false premise and
        # emitted ``SelectableGroups dict interface is deprecated`` on every
        # 3.10/3.11 run (5 warnings in the suite). Keyword-only removes both
        # the version branch and the deprecation noise.
        eps: list[importlib.metadata.EntryPoint] = list(
            importlib.metadata.entry_points(group=_ENTRY_POINT_GROUP)
        )
    except Exception:  # noqa: BLE001 - registry discovery must never crash callers
        enumeration_failed = True

    if enumeration_failed:
        # Do not invoke application logging while the metadata failure is still
        # active: a handler can otherwise read the plugin traceback from
        # ``sys.exc_info()``.
        _log_diagnostic(
            logger.warning,
            "Failed to enumerate entry-points; skipping third-party backend discovery.",
        )
        return {}

    discovered: dict[str, BackendDescriptor] = {}
    conflicted_names: set[str] = set()
    for ep in eps:
        descriptor: BackendDescriptor | None = None
        plugin_load_failed = False
        try:
            descriptor = _load_plugin_descriptor(ep)
        except Exception:  # noqa: BLE001 - graceful-skip: never propagate
            plugin_load_failed = True

        if plugin_load_failed:
            # This log intentionally follows the exception suite so plugin details
            # cannot be recovered by a custom handler from ``sys.exc_info()``.
            _log_diagnostic(
                logger.warning,
                "Skipping invalid third-party backend entry-point; "
                "bundled backends remain available.",
            )
            continue
        assert descriptor is not None
        name = descriptor.backend_type
        if name in conflicted_names:
            _log_diagnostic(
                logger.error,
                "Skipping additional third-party backend entry-point with a "
                "conflicted backend name.",
            )
            continue
        if name in discovered:
            discovered.pop(name)
            conflicted_names.add(name)
            _log_diagnostic(
                logger.error,
                "Skipping duplicate third-party backend name; neither plugin is "
                "registered.",
            )
            continue
        discovered[name] = descriptor
    return discovered


def _load_plugin_descriptor(ep: importlib.metadata.EntryPoint) -> BackendDescriptor:
    """Invoke one entry-point's registration callable and validate the result.

    Validation:

    - The callable returns a :class:`BackendDescriptor` instance.
    - The entry-point name and ``backend_type`` both match
      ``^[a-z][a-z0-9_]*$`` and are equal.
    - Both class paths are non-empty dotted Python identifier paths.
    - ``capabilities`` is a frozen set of strings.
    - ``capabilities`` ⊆ ``{"queue", "set", "storage"}``.

    A validation failure raises ``ValueError``; the caller
    (:func:`_discover_entry_points`) treats it as a broken-plugin skip.

    Args:
        ep: The entry-point to load.

    Returns:
        The descriptor the entry-point's callable returned.

    Raises:
        ValueError: If the returned descriptor fails validation.
        Exception: Whatever the callable raises (propagated to the skip handler).
    """
    # Validate the external name before any descriptor can be put in a dict or
    # set.  Plugin metadata accepts exact built-ins only: subclasses can define
    # hostile hash/membership methods which must never execute at discovery.
    if type(ep.name) is not str or not _NAME_PATTERN.fullmatch(ep.name):
        raise ValueError("Entry-point has an invalid backend name.")
    # ``ep.load()`` resolves the dotted path to the registration callable.
    registration = ep.load()
    descriptor = registration()
    if type(descriptor) is not BackendDescriptor:
        raise TypeError("Entry-point registration returned an invalid descriptor.")
    if type(descriptor.backend_type) is not str or not _NAME_PATTERN.fullmatch(
        descriptor.backend_type
    ):
        raise ValueError("Entry-point descriptor has an invalid backend type.")
    if descriptor.backend_type != ep.name:
        raise ValueError("Entry-point descriptor backend name does not match.")
    for field_name, dotted_path in (
        ("backend_cls_path", descriptor.backend_cls_path),
        ("settings_cls_path", descriptor.settings_cls_path),
    ):
        if (
            type(dotted_path) is not str
            or len(dotted_path.split(".")) < 2
            or any(not part.isidentifier() for part in dotted_path.split("."))
        ):
            del field_name
            del dotted_path
            raise ValueError("Entry-point descriptor has an invalid class path.")
    if type(descriptor.capabilities) is not frozenset or any(
        type(capability) is not str for capability in descriptor.capabilities
    ):
        raise TypeError("Entry-point descriptor has invalid capabilities.")
    invalid = descriptor.capabilities - _VALID_CAPABILITIES
    if invalid:
        raise ValueError("Entry-point descriptor has unsupported capabilities.")
    return descriptor


def get_registry() -> dict[str, BackendDescriptor]:
    """Return the merged backend registry (bundled + 3rd-party).

    Memoized on first call; subsequent calls return a fresh COPY so callers
    can't mutate the cached table. Bundled descriptors WIN on name conflict
    — a 3rd-party entry-point shadowing a bundled name is dropped with a warning
    log. Logging cannot be promoted into an exception by Python's warnings
    filters, preserving the bundled-availability invariant.

    Discovery is single-flight. Concurrent callers wait for the active owner,
    while a registration callback that recursively requests the registry fails
    immediately instead of observing a partially built table or recursing.

    Returns:
        A fresh dict mapping backend-type string → :class:`BackendDescriptor`.
    """
    global _registry_cache
    global _registry_discovery_in_progress
    global _registry_discovery_owner

    owner = threading.get_ident()
    with _registry_condition:
        while _registry_cache is None and _registry_discovery_in_progress:
            if _registry_discovery_owner == owner:
                raise RuntimeError(
                    "Backend registry discovery is already in progress on this thread."
                )
            _registry_condition.wait()
        if _registry_cache is not None:
            return dict(_registry_cache)
        _registry_discovery_in_progress = True
        _registry_discovery_owner = owner

    try:
        bundled = dict(_BUNDLED_DESCRIPTORS)
        discovered = _discover_entry_points()
        for name, descriptor in discovered.items():
            if name in bundled:
                # Bundled-wins precedence: the bundled descriptor stays; the
                # 3rd-party shadow is dropped. Log so operators notice without making
                # registry availability depend on their Python warning filters.
                _log_diagnostic(
                    logger.warning,
                    "Third-party backend entry-point shadows a bundled backend; "
                    "bundled backend wins.",
                )
                continue
            bundled[name] = descriptor
    except BaseException:
        with _registry_condition:
            _registry_discovery_owner = None
            _registry_discovery_in_progress = False
            _registry_condition.notify_all()
        raise

    with _registry_condition:
        _registry_cache = bundled
        _registry_discovery_owner = None
        _registry_discovery_in_progress = False
        _registry_condition.notify_all()
        return dict(_registry_cache)


def get_descriptor(backend_type: str) -> BackendDescriptor:
    """Look up one descriptor.

    Args:
        backend_type: The backend-type string (``SCRAPY_BACKEND_TYPE`` value).

    Returns:
        The :class:`BackendDescriptor` for ``backend_type``.

    Raises:
        ConfigurationError: If ``backend_type`` is not registered. The error
            lists only bundled keys so a failed public configuration path cannot
            disclose installed third-party entry-point metadata.
    """
    if isinstance(backend_type, BackendType):
        backend_type = backend_type.value
    elif type(backend_type) is not str:
        del backend_type
        raise ConfigurationError(
            "Selected backend type is not a registered backend type.",
            setting_name="SCRAPY_BACKEND_TYPE",
        )
    registry = get_registry()
    descriptor = registry.get(backend_type)
    if descriptor is None:
        valid = ", ".join(repr(k) for k in sorted(_BUNDLED_DESCRIPTORS))
        msg = (
            "Selected backend type is not a registered backend type. "
            f"Valid bundled values: {valid}."
        )
        del backend_type
        del registry
        raise ConfigurationError(msg, setting_name="SCRAPY_BACKEND_TYPE")
    return descriptor


def has_capability(backend_type: str, capability: str) -> bool:
    """Return True if ``backend_type`` is registered AND declares ``capability``.

    Unknown backends return ``False`` (not raise) — this is a predicate for
    use in capability-gating checks where the typed error is raised separately
    by :func:`scrapy_extension.backends.connectors.resolve_backend_config`.

    Args:
        backend_type: The backend-type string.
        capability: One of ``"queue"`` / ``"set"`` / ``"storage"``.

    Returns:
        True if the backend is registered and has the capability.
    """
    if type(capability) is not str:
        return False
    try:
        descriptor = get_descriptor(backend_type)
    except ConfigurationError:
        return False
    return capability in descriptor.capabilities


def _reset_registry_cache() -> None:
    """Clear the registry cache (test-only isolation helper).

    Mirrors :meth:`scrapy_extension.backends.connectors.ConnectionManager.clear_registry`.
    Production code never needs this — the registry is memoized for the
    process lifetime. Tests call it between runs so an entry-point patched
    via ``monkeypatch`` is re-discovered.
    """
    global _registry_cache
    owner = threading.get_ident()
    with _registry_condition:
        while _registry_discovery_in_progress:
            if _registry_discovery_owner == owner:
                raise RuntimeError(
                    "Backend registry discovery cannot be reset by its owner."
                )
            _registry_condition.wait()
        _registry_cache = None
