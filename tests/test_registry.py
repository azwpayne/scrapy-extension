"""Tests for scrapy_extension/backends/registry.py.

Round-5 Unit R5-1: entry-point plugin registration. These 7 tests are the
PLAN's TDD acceptance gate. They MUST verify that:

- bundled backends still resolve (Test 1);
- 3rd-party descriptors are discovered via ``importlib.metadata.entry_points``
  (Test 2);
- capability mismatches fail fast with a typed error (Test 3);
- bundled-wins-on-conflict emits a warning log without raising (Test 4);
- a broken plugin callable never breaks the bundled set (Test 5);
- ``get_registry()`` never imports any backend module — lazy-import preserved
  (Test 6);
- entry-point discovery uses the modern ``entry_points(group=...)`` keyword API with no ``SelectableGroups`` deprecation warning (Test 7).
"""

from __future__ import annotations

import logging
import sys
import traceback
import warnings
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends import connectors
from scrapy_extension.backends.base import Backend, QueueBackend
from scrapy_extension.backends.registry import (
    BackendDescriptor,
    _reset_registry_cache,
    get_descriptor,
    get_registry,
    has_capability,
)
from scrapy_extension.exceptions import ConfigurationError

# ---------------------------------------------------------------------------
# Module-level registration callables.
# ---------------------------------------------------------------------------
# Entry-point ``value`` strings resolve via ``importlib.import_module`` +
# ``getattr``, so the registration callable MUST be a module-level attribute.
# Closures defined inside a test method would be invisible to ``getattr``.


def _register_mybackend() -> BackendDescriptor:
    return _make_descriptor("mybackend", capabilities=frozenset({"queue"}))


def _register_kwarg() -> BackendDescriptor:
    return _make_descriptor("kwargepp", capabilities=frozenset({"queue"}))


def _register_good_plugin() -> BackendDescriptor:
    return _make_descriptor("goodplugin", capabilities=frozenset({"queue"}))


def _register_metadata_secret_plugin() -> BackendDescriptor:
    return _make_descriptor(
        "pluginmetadata_secret_marker", capabilities=frozenset({"queue"})
    )


def _register_overclaimed_plugin() -> BackendDescriptor:
    return _make_descriptor(
        "overclaimed",
        capabilities=frozenset({"queue"}),
        backend_cls_path="tests.test_registry._LifecycleOnlyBackend",
    )


def _register_mismatched_plugin() -> BackendDescriptor:
    return _make_descriptor("actualname", capabilities=frozenset({"queue"}))


def _register_validname() -> BackendDescriptor:
    return _make_descriptor("validname", capabilities=frozenset({"queue"}))


def _register_invalid_backend_path() -> BackendDescriptor:
    return _make_descriptor(
        "badbackendpath",
        capabilities=frozenset({"queue"}),
        backend_cls_path="NotDotted",
    )


def _register_invalid_settings_path() -> BackendDescriptor:
    return _make_descriptor(
        "badsettingspath",
        capabilities=frozenset({"queue"}),
        settings_cls_path="tests.test_registry.not-valid",
    )


def _register_duplicate_first() -> BackendDescriptor:
    return _make_descriptor(
        "duplicate",
        capabilities=frozenset({"queue"}),
        backend_cls_path="tests.test_registry._StubBackend",
    )


def _register_duplicate_second() -> BackendDescriptor:
    return _make_descriptor(
        "duplicate",
        capabilities=frozenset({"storage"}),
        backend_cls_path="tests.test_registry._OtherStubBackend",
    )


def _register_shadow_redis() -> BackendDescriptor:
    # Deliberately shadowing a bundled name (Test 4: bundled-wins).
    return _make_descriptor(
        "redis",
        capabilities=frozenset({"queue"}),
        backend_cls_path="tests.test_registry._StubBackend",
        settings_cls_path="tests.test_registry._StubSettings",
    )


def _broken_plugin() -> BackendDescriptor:
    raise ImportError("simulated plugin import failure")


def _secret_broken_plugin() -> BackendDescriptor:
    raise RuntimeError("registry-plugin-secret-marker")


# ---------------------------------------------------------------------------
# Test helpers: fake entry-points + descriptor factory.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _FakeEntryPoint:
    """Minimal stand-in for ``importlib.metadata.EntryPoint``.

    ``importlib.metadata.EntryPoint`` is a frozen dataclass with ``name``,
    ``group``, and ``value`` (dotted path to the registration callable).
    We replicate just enough to drive ``_discover_entry_points``.
    """

    name: str
    value: str
    group: str

    def load(self) -> Any:
        """Resolve the dotted path to a callable and invoke it.

        Mirrors the real ``EntryPoint.load()``: import the module, fetch the
        attribute. Uses ``importlib.import_module`` (not bare ``__import__``)
        so the cached module in ``sys.modules`` is returned unambiguously.
        """
        import importlib

        module_path, _, attr = self.value.rpartition(".")
        if not module_path:
            msg = f"Invalid fake entry-point value: {self.value!r}"
            raise ValueError(msg)
        module = importlib.import_module(module_path)
        return getattr(module, attr)


def _make_descriptor(
    name: str,
    *,
    capabilities: frozenset[str],
    backend_cls_path: str = "tests.test_registry._StubBackend",
    settings_cls_path: str = "tests.test_registry._StubSettings",
) -> BackendDescriptor:
    """Build a descriptor with sane test defaults."""
    return BackendDescriptor(
        backend_type=name,
        backend_cls_path=backend_cls_path,
        settings_cls_path=settings_cls_path,
        capabilities=capabilities,
    )


def _assert_redacted_error(error: BaseException, *markers: str) -> None:
    """Assert marker-bearing plugin/configuration data escaped no public surface."""
    public_forms = (
        str(error),
        repr(error.__dict__),
        "".join(traceback.format_exception(error)),
    )
    for marker in markers:
        assert all(marker not in form for form in public_forms)
        trace = error.__traceback__
        while trace is not None:
            frame = trace.tb_frame
            if "/src/scrapy_extension/" in frame.f_code.co_filename:
                locals_snapshot = frame.f_locals
                assert marker not in repr(locals_snapshot)
                for local in locals_snapshot.values():
                    if type(local) is connectors.ConnectionManager:
                        settings = vars(local).get("settings")
                        if settings is not None:
                            assert marker not in repr(settings)
                    if type(local) is tuple:
                        for argument in local:
                            if type(argument) is connectors.ConnectionManager:
                                settings = vars(argument).get("settings")
                                if settings is not None:
                                    assert marker not in repr(settings)
            trace = trace.tb_next
    assert error.__cause__ is None
    assert error.__context__ is None


def _patch_entry_points(
    monkeypatch: pytest.MonkeyPatch, eps: list[_FakeEntryPoint]
) -> None:
    """Patch ``importlib.metadata.entry_points`` to return ``eps``.

    The patched callable supports BOTH the 3.12+ kwarg form
    (``entry_points(group=...)``) and the legacy 3.10/3.11 form
    (``entry_points().get(group, [])``).
    """
    import importlib.metadata as importlib_metadata

    def _entry_points(group: str | None = None) -> Any:
        if group is not None:
            # 3.12+ shape: returns a list of EntryPoint objects.
            return [ep for ep in eps if ep.group == group]
        # Legacy shape: returns a dict-like with .get(group, []).
        by_group: dict[str, list[_FakeEntryPoint]] = {}
        for ep in eps:
            by_group.setdefault(ep.group, []).append(ep)

        class _Selectable:
            def get(self, key: str, default: Any = None) -> Any:
                return by_group.get(key, default or [])

        return _Selectable()

    monkeypatch.setattr(importlib_metadata, "entry_points", _entry_points)


# ---------------------------------------------------------------------------
# Stub backend + settings classes for Test 2 (instantiation path).
# ---------------------------------------------------------------------------


class _StubBackend(Backend, QueueBackend):
    """Minimal backend stub the descriptor points at.

    Constructed as ``_StubBackend(_StubSettings(**settings))`` — so the
    descriptor path actually instantiates a real (no-op) class to prove
    the dispatch table resolves end-to-end.
    """

    def __init__(self, settings: _StubSettings) -> None:
        self.settings = settings

    @property
    def backend_type(self) -> str:
        return "mybackend"

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def ping(self) -> bool:
        return True

    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        del queue_name, item, priority

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        del queue_name, timeout
        return None

    def queue_len(self, queue_name: str) -> int:
        del queue_name
        return 0

    def clear_queue(self, queue_name: str) -> None:
        del queue_name


class _StubSettings:
    """Settings stub matching ``_StubBackend``'s constructor contract."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


class _LifecycleOnlyBackend(Backend):
    """Valid lifecycle backend deliberately missing its declared queue ABC."""

    def __init__(self, settings: _StubSettings) -> None:
        self.settings = settings

    @property
    def backend_type(self) -> str:
        return "overclaimed"

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def ping(self) -> bool:
        return True


# ---------------------------------------------------------------------------
# The 7 PLAN tests.
# ---------------------------------------------------------------------------


class TestBundledBackendsStillWork:
    """Test 1: bundled_still_work."""

    def test_bundled_still_work(self):
        """``SCRAPY_BACKEND_TYPE=redis`` resolves to a bundled descriptor and
        the descriptor's class path builds ``RedisBackend`` byte-identically.

        Verifies the consolidation: ``_BUNDLED_DESCRIPTORS`` was seeded from
        the old ``_BACKEND_FACTORIES`` + capability sets, so the redis
        descriptor's ``backend_cls_path`` is the SAME dotted string the old
        table held — no behavior change at the dispatch site.
        """
        _reset_registry_cache()
        registry = get_registry()

        assert "redis" in registry
        redis_desc = get_descriptor("redis")
        assert (
            redis_desc.backend_cls_path
            == "scrapy_extension.backends.redis.RedisBackend"
        )
        assert redis_desc.settings_cls_path == "scrapy_extension.settings.RedisSettings"
        # Redis supports all three interfaces (per the old QUEUE/SET/STORAGE sets).
        assert redis_desc.capabilities == frozenset({"queue", "set", "storage"})

        # All 10 bundled backends present.
        assert len(registry) >= 10
        for name in (
            "redis",
            "mongodb",
            "kafka",
            "rabbitmq",
            "elasticsearch",
            "rocketmq",
            "pulsar",
            "memcached",
            "sqs",
            "dynamodb",
        ):
            assert name in registry, f"Missing bundled backend: {name}"


def test_unknown_descriptor_does_not_disclose_request_or_plugin_metadata(monkeypatch):
    from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

    plugin_marker = "pluginmetadata_secret_marker"
    request_marker = "unknown-backend-request-secret-marker"
    _patch_entry_points(
        monkeypatch,
        [
            _FakeEntryPoint(
                name=plugin_marker,
                value="tests.test_registry._register_metadata_secret_plugin",
                group=_ENTRY_POINT_GROUP,
            )
        ],
    )
    _reset_registry_cache()

    try:
        with pytest.raises(ConfigurationError) as exc_info:
            get_descriptor(request_marker)
    finally:
        _reset_registry_cache()

    _assert_redacted_error(exc_info.value, plugin_marker, request_marker)
    assert "redis" in str(exc_info.value)


class TestThirdPartyDiscovered:
    """Test 2: third_party_discovered."""

    def test_third_party_discovered(self, monkeypatch):
        """A mock entry-point → registry returns its descriptor → resolves +
        instantiates the stub backend end-to-end."""
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="mybackend",
                    value="tests.test_registry._register_mybackend",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        registry = get_registry()
        assert "mybackend" in registry
        desc = get_descriptor("mybackend")
        assert desc.capabilities == frozenset({"queue"})

        # End-to-end: the class path actually imports + instantiates.
        from scrapy_extension.backends.connectors import _load_object

        backend_cls = _load_object(desc.backend_cls_path)
        settings_cls = _load_object(desc.settings_cls_path)
        instance = backend_cls(settings_cls(host="local"))
        assert isinstance(instance, _StubBackend)
        assert instance.settings.kwargs == {"host": "local"}

        # The first-use plugin boundary accepts a backend that actually fulfils
        # both its lifecycle and declared QueueBackend contracts.
        from scrapy_extension.backends.connectors import ConnectionManager

        assert isinstance(
            ConnectionManager("mybackend")._create_backend(), _StubBackend
        )

    def test_spider_mixin_accepts_plugin_registry_identifier(self, monkeypatch):
        """A plugin registry key is a valid runtime spider backend identifier."""
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP
        from scrapy_extension.spider.spider_mixin import BackendSpiderMixin

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="mybackend",
                    value="tests.test_registry._register_mybackend",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        class PluginSpider(BackendSpiderMixin):
            name = "plugin-spider"
            backend_type = "mybackend"
            backend_settings = {"host": "plugin.local"}

        spider = PluginSpider()
        try:
            manager = spider.setup_backend()
            backend = manager.get_queue_backend()
            assert isinstance(backend, _StubBackend)
            assert backend.settings.kwargs == {"host": "plugin.local"}
        finally:
            spider.close_backend()
            _reset_registry_cache()


class TestDescriptorBoundary:
    """Malformed or ambiguous plugins never enter or abort the registry."""

    @staticmethod
    def _runtime_descriptor() -> BackendDescriptor:
        return _make_descriptor("runtime_contract", capabilities=frozenset({"queue"}))

    def test_descriptor_validation_rejects_type_subclasses_without_dispatch(self):
        """External metadata must be exact built-ins before any protocol executes."""
        from scrapy_extension.backends import registry as registry_mod

        marker = "plugin-descriptor-subclass-marker"
        calls: list[str] = []

        class _EvilString(str):
            def __hash__(self) -> int:
                calls.append("hash")
                raise RuntimeError(marker)

            def __format__(self, format_spec: str) -> str:
                del format_spec
                calls.append("format")
                raise RuntimeError(marker)

        class _EvilFrozenSet(frozenset[str]):
            def __iter__(self):
                calls.append("iter")
                raise RuntimeError(marker)

        class _EvilCapabilityString(str):
            def __hash__(self) -> int:
                calls.append("capability-hash")
                return str.__hash__(self)

        class _DescriptorSubclass(BackendDescriptor):
            pass

        evil_capability = _EvilCapabilityString("queue")
        evil_capabilities = frozenset({evil_capability})
        calls.clear()
        candidates = (
            (
                _EvilString("evilplugin"),
                _make_descriptor("evilplugin", capabilities=frozenset({"queue"})),
            ),
            (
                "evilplugin",
                _DescriptorSubclass(
                    "evilplugin",
                    "tests.test_registry._StubBackend",
                    "tests.test_registry._StubSettings",
                    frozenset({"queue"}),
                ),
            ),
            (
                "evilplugin",
                BackendDescriptor(
                    _EvilString("evilplugin"),
                    "tests.test_registry._StubBackend",
                    "tests.test_registry._StubSettings",
                    frozenset({"queue"}),
                ),
            ),
            (
                "evilplugin",
                BackendDescriptor(
                    "evilplugin",
                    _EvilString("tests.test_registry._StubBackend"),
                    "tests.test_registry._StubSettings",
                    frozenset({"queue"}),
                ),
            ),
            (
                "evilplugin",
                BackendDescriptor(
                    "evilplugin",
                    "tests.test_registry._StubBackend",
                    "tests.test_registry._StubSettings",
                    _EvilFrozenSet({"queue"}),
                ),
            ),
            (
                "evilplugin",
                BackendDescriptor(
                    "evilplugin",
                    "tests.test_registry._StubBackend",
                    "tests.test_registry._StubSettings",
                    evil_capabilities,
                ),
            ),
        )

        for name, descriptor in candidates:
            calls.clear()

            class _EntryPoint:
                def __init__(
                    self, entry_name: object, entry_descriptor: object
                ) -> None:
                    self.name = entry_name
                    self._descriptor = entry_descriptor

                def load(self):
                    return lambda: self._descriptor

            with pytest.raises((TypeError, ValueError)):
                registry_mod._load_plugin_descriptor(_EntryPoint(name, descriptor))  # type: ignore[arg-type]
            assert calls == []

    def test_plugin_loader_path_errors_are_configuration_errors(self, monkeypatch):
        descriptor = self._runtime_descriptor()
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)

        def _missing_path(_: str) -> object:
            raise AttributeError("missing plugin symbol")

        monkeypatch.setattr(connectors, "_load_object", _missing_path)

        with pytest.raises(ConfigurationError, match="invalid plugin class path"):
            connectors.ConnectionManager("runtime_contract")._create_backend()

    def test_plugin_loader_metadata_is_not_retained(self, monkeypatch):
        marker = "plugin-metadata-secret-marker"
        descriptor = _make_descriptor(
            marker,
            capabilities=frozenset({"queue"}),
        )
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)

        def _missing_path(_: str) -> object:
            raise AttributeError(f"plugin loader detail: {marker}")

        monkeypatch.setattr(connectors, "_load_object", _missing_path)

        with pytest.raises(ConfigurationError) as exc_info:
            connectors.ConnectionManager(marker)._create_backend()

        _assert_redacted_error(exc_info.value, marker)

    def test_plugin_requires_callable_backend_and_settings_classes(self, monkeypatch):
        descriptor = self._runtime_descriptor()
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)
        monkeypatch.setattr(connectors, "_load_object", lambda _: object())

        with pytest.raises(ConfigurationError, match="callable backend and settings"):
            connectors.ConnectionManager("runtime_contract")._create_backend()

    def test_plugin_constructor_type_error_is_not_retried(self, monkeypatch):
        descriptor = self._runtime_descriptor()
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)

        class _BrokenBackend:
            def __init__(self, settings: object) -> None:
                del settings
                raise TypeError("unsupported settings")

        def _load(path: str) -> object:
            return (
                _BrokenBackend if path == descriptor.backend_cls_path else _StubSettings
            )

        monkeypatch.setattr(connectors, "_load_object", _load)

        with pytest.raises(ConfigurationError, match="could not be constructed"):
            connectors.ConnectionManager("runtime_contract")._create_backend()

    def test_plugin_constructor_diagnostics_are_not_retained(self, monkeypatch):
        descriptor = self._runtime_descriptor()
        marker = "plugin-constructor-secret-marker"
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)

        class _BrokenBackend:
            def __init__(self, settings: object) -> None:
                del settings
                raise RuntimeError(f"plugin constructor diagnostic included {marker}")

        def _load(path: str) -> object:
            return (
                _BrokenBackend if path == descriptor.backend_cls_path else _StubSettings
            )

        monkeypatch.setattr(connectors, "_load_object", _load)

        with pytest.raises(ConfigurationError) as exc_info:
            connectors.ConnectionManager(
                "runtime_contract", {"password": marker}
            )._create_backend()

        _assert_redacted_error(exc_info.value, marker)

    def test_plugin_settings_loader_failure_during_connect_is_not_retried(
        self, monkeypatch
    ):
        descriptor = self._runtime_descriptor()
        marker = "plugin-settings-loader-secret-marker"
        load_calls: list[str] = []
        sleep_calls: list[float] = []
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)

        def _load(path: str) -> object:
            load_calls.append(path)
            if path == descriptor.backend_cls_path:
                return _StubBackend
            if path == descriptor.settings_cls_path:
                raise ImportError(marker)
            raise AssertionError("unexpected descriptor path")

        monkeypatch.setattr(connectors, "_load_object", _load)
        monkeypatch.setattr(
            connectors,
            "_wait_for_retry_backoff",
            lambda _event, delay: sleep_calls.append(delay),
        )
        manager = connectors.ConnectionManager(
            "runtime_contract", {"retry_attempts": 3, "retry_delay": 1}
        )
        with pytest.raises(ConfigurationError) as exc_info:
            _ = manager.backend

        _assert_redacted_error(exc_info.value, marker)
        assert load_calls.count(descriptor.settings_cls_path) == 1
        assert sleep_calls == []

    def test_plugin_settings_loader_failure_during_adaptation_is_static(
        self, monkeypatch
    ):
        descriptor = self._runtime_descriptor()
        marker = "plugin-adaptation-loader-secret-marker"
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)
        monkeypatch.setattr(
            connectors,
            "_load_object",
            lambda _: (_ for _ in ()).throw(ImportError(marker)),
        )
        settings = MagicMock()
        settings.get.side_effect = lambda key, default=None: (
            "runtime_contract" if key == "SCRAPY_BACKEND_TYPE" else default
        )
        settings.getdict.return_value = {}

        with pytest.raises(ConfigurationError) as exc_info:
            connectors.resolve_backend_config(
                settings,
                type_key="SCRAPY_QUEUE_BACKEND_TYPE",
                settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
            )

        _assert_redacted_error(exc_info.value, marker)

    def test_plugin_model_field_metadata_failure_is_not_public(self, monkeypatch):
        descriptor = self._runtime_descriptor()
        marker = "plugin-model-fields-secret-marker"
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)

        class _ExplosiveMeta(type):
            @property
            def model_fields(cls) -> object:
                del cls
                raise RuntimeError(marker)

        class _ExplosiveSettings(metaclass=_ExplosiveMeta):
            def __init__(self, **kwargs: object) -> None:
                del kwargs
                raise RuntimeError(marker)

        def _load(path: str) -> object:
            return (
                _StubBackend
                if path == descriptor.backend_cls_path
                else _ExplosiveSettings
            )

        monkeypatch.setattr(connectors, "_load_object", _load)

        with pytest.raises(ConfigurationError) as exc_info:
            connectors.ConnectionManager("runtime_contract")._create_backend()

        _assert_redacted_error(exc_info.value, marker)

    def test_plugin_model_field_name_subclass_is_ignored_without_dispatch(
        self, monkeypatch
    ):
        """Plugin metadata field labels must be exact strings before set algebra."""
        descriptor = self._runtime_descriptor()
        marker = "plugin-field-name-subclass-secret-marker"
        calls: list[str] = []
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)

        class _ExplosiveFieldName(str):
            def __hash__(self) -> int:
                return hash("retry_attempts")

            def __eq__(self, other: object) -> bool:
                del other
                calls.append("eq")
                raise RuntimeError(marker)

        class _PluginSettings:
            model_fields = {_ExplosiveFieldName("retry_attempts"): object()}

            def __init__(self, **kwargs: object) -> None:
                del kwargs

        def _load(path: str) -> object:
            return (
                _StubBackend if path == descriptor.backend_cls_path else _PluginSettings
            )

        monkeypatch.setattr(connectors, "_load_object", _load)
        backend = connectors.ConnectionManager(
            "runtime_contract", {"password": marker}
        )._create_backend()

        assert isinstance(backend, _StubBackend)
        assert calls == []

    def test_bundled_optional_dependency_import_error_is_preserved(self, monkeypatch):
        descriptor = get_descriptor("redis")
        marker = "bundled-optional-dependency-marker"
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)
        monkeypatch.setattr(
            connectors,
            "_load_object",
            lambda _: (_ for _ in ()).throw(ImportError(marker)),
        )

        with pytest.raises(ImportError) as exc_info:
            connectors.ConnectionManager("redis")._create_backend()

        error = exc_info.value
        assert (
            str(error)
            == "Selected backend could not be initialized because an import failed."
        )
        _assert_redacted_error(error, marker)

    @pytest.mark.parametrize("stage", ["loader", "settings", "backend"])
    def test_bundled_constructor_import_error_is_preserved_without_retry(
        self,
        monkeypatch,
        stage,
    ):
        """Bundled optional imports retain their public type at every lazy stage."""
        descriptor = get_descriptor("redis")
        marker = f"bundled-{stage}-import-error-marker"
        sleeps: list[float] = []
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)
        monkeypatch.setattr(
            connectors,
            "_wait_for_retry_backoff",
            lambda _event, delay: sleeps.append(delay),
        )

        class _ImportErrorSettings:
            def __init__(self, **kwargs: object) -> None:
                del kwargs
                if stage == "settings":
                    raise ImportError(marker)

        class _ImportErrorBackend:
            def __init__(self, settings: object) -> None:
                del settings
                if stage == "backend":
                    raise ImportError(marker)

        def _load(path: str) -> object:
            if stage == "loader":
                raise ImportError(marker)
            if path == descriptor.settings_cls_path:
                return _ImportErrorSettings
            return _ImportErrorBackend

        monkeypatch.setattr(connectors, "_load_object", _load)
        manager = connectors.ConnectionManager(
            "redis",
            {"retry_attempts": 3, "retry_delay": 0.01},
        )

        with pytest.raises(ImportError) as exc_info:
            manager.connect()

        error = exc_info.value
        assert (
            str(error)
            == "Selected backend could not be initialized because an import failed."
        )
        _assert_redacted_error(error, marker)
        assert manager._backend is None
        assert sleeps == []

    def test_plugin_missing_backend_base_class_fails_fast(self, monkeypatch):
        descriptor = self._runtime_descriptor()
        monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)

        def _load(path: str) -> object:
            return (
                (lambda settings: object())
                if path == descriptor.backend_cls_path
                else _StubSettings
            )

        monkeypatch.setattr(connectors, "_load_object", _load)

        with pytest.raises(ConfigurationError, match="missing Backend, QueueBackend"):
            connectors.ConnectionManager("runtime_contract")._create_backend()

    def test_selected_plugin_overclaim_fails_before_connection_retry(self, monkeypatch):
        """Runtime interface validation happens on first use, not discovery."""
        from scrapy_extension.backends.connectors import ConnectionManager
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP
        from scrapy_extension.exceptions import ConfigurationError

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="overclaimed",
                    value="tests.test_registry._register_overclaimed_plugin",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        manager = ConnectionManager("overclaimed", {"retry_attempts": 3})
        with pytest.raises(ConfigurationError, match="QueueBackend"):
            manager.connect()
        assert manager._backend is None

    def test_broken_plugin_isolated_when_user_warnings_are_errors(self, monkeypatch):
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="broken",
                    value="tests.test_registry._broken_plugin",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            registry = get_registry()

        assert "redis" in registry
        assert "broken" not in registry

    def test_entry_point_name_must_match_descriptor_type(self, monkeypatch):
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="declaredname",
                    value="tests.test_registry._register_mismatched_plugin",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        registry = get_registry()

        assert "declaredname" not in registry
        assert "actualname" not in registry

    def test_entry_point_name_must_match_public_pattern(self, monkeypatch):
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="Bad-EP",
                    value="tests.test_registry._register_validname",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        assert "validname" not in get_registry()

    @pytest.mark.parametrize(
        ("name", "registration"),
        (
            ("badbackendpath", "_register_invalid_backend_path"),
            ("badsettingspath", "_register_invalid_settings_path"),
        ),
    )
    def test_class_paths_must_be_dotted_identifiers(
        self, monkeypatch, name, registration
    ):
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name=name,
                    value=f"tests.test_registry.{registration}",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        assert name not in get_registry()

    def test_duplicate_third_party_name_registers_neither_plugin(self, monkeypatch):
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="duplicate",
                    value="tests.test_registry._register_duplicate_first",
                    group=_ENTRY_POINT_GROUP,
                ),
                _FakeEntryPoint(
                    name="duplicate",
                    value="tests.test_registry._register_duplicate_second",
                    group=_ENTRY_POINT_GROUP,
                ),
            ],
        )
        _reset_registry_cache()

        assert "duplicate" not in get_registry()


class TestCapabilityGated:
    """Test 3: capability_gated."""

    def test_capability_gated_raises_configuration_error(self, monkeypatch):
        """A 3rd-party descriptor with only ``{"queue"}`` → selecting for set
        or storage → ``ConfigurationError`` w/ ``setting_name`` + the capable
        backend list in the message."""
        from scrapy_extension.backends.connectors import resolve_backend_config
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP
        from scrapy_extension.exceptions import ConfigurationError

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="mybackend",
                    value="tests.test_registry._register_mybackend",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        settings = MagicMock()

        def _get(key, default=None):
            if key == "SCRAPY_SET_BACKEND_TYPE":
                return "mybackend"
            return default

        def _getdict(key, default=None):
            return {} if default is None else default

        settings.get.side_effect = _get
        settings.getdict.side_effect = _getdict

        with pytest.raises(ConfigurationError) as exc_info:
            resolve_backend_config(
                settings,
                type_key="SCRAPY_SET_BACKEND_TYPE",
                settings_key="SCRAPY_SET_BACKEND_SETTINGS",
                required_capabilities={"set"},
                component_name="set",
            )

        assert exc_info.value.setting_name == "SCRAPY_SET_BACKEND_TYPE"
        msg = str(exc_info.value)
        # The capable backends list must name at least one bundled set-capable
        # backend, without exposing the selected third-party plugin metadata.
        assert "redis" in msg
        assert "mybackend" not in msg


class TestNameConflictBundledWins:
    """Test 4: name_conflict_bundled_wins."""

    def test_name_conflict_bundled_wins(self, monkeypatch, caplog):
        """An entry-point named ``"redis"`` → bundled descriptor wins AND a
        warning is logged.

        The registry must stay available even when applications promote Python
        warnings to exceptions, while the conflict remains observable.
        """
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="redis",
                    value="tests.test_registry._register_shadow_redis",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        with warnings.catch_warnings(), caplog.at_level(logging.WARNING):
            warnings.simplefilter("error", UserWarning)
            registry = get_registry()

        # Bundled descriptor wins — verified by the canonical path string,
        # not just any descriptor named "redis".
        desc = get_descriptor("redis")
        assert desc.backend_cls_path == "scrapy_extension.backends.redis.RedisBackend"
        assert "shadows a bundled backend" in caplog.text


class TestImportErrorGracefulSkip:
    """Test 5: import_error_graceful_skip."""

    def test_import_error_graceful_skip(self, monkeypatch, caplog):
        """An entry-point callable raising ``ImportError`` is SKIPPED + logged;
        the bundled 10 stay intact.

        A single broken 3rd-party plugin must never break the bundled set —
        operators rely on bundled backends always being usable regardless of
        which plugins are installed in the environment.
        """
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="broken",
                    value="tests.test_registry._broken_plugin",
                    group=_ENTRY_POINT_GROUP,
                ),
                _FakeEntryPoint(
                    name="goodplugin",
                    value="tests.test_registry._register_good_plugin",
                    group=_ENTRY_POINT_GROUP,
                ),
            ],
        )
        _reset_registry_cache()

        # The broken plugin must not raise even under warnings-as-errors.
        with warnings.catch_warnings(), caplog.at_level(logging.WARNING):
            warnings.simplefilter("error", UserWarning)
            registry = get_registry()

        # Bundled 10 still intact.
        for name in (
            "redis",
            "mongodb",
            "kafka",
            "rabbitmq",
            "elasticsearch",
            "rocketmq",
            "pulsar",
            "memcached",
            "sqs",
            "dynamodb",
        ):
            assert name in registry
        # The good plugin was discovered; the broken one was not.
        assert "goodplugin" in registry
        assert "broken" not in registry
        assert "Skipping invalid third-party backend entry-point" in caplog.text

    def test_plugin_skip_diagnostics_redact_untrusted_metadata(
        self, monkeypatch, caplog
    ):
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        marker = "registry-plugin-secret-marker"
        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name=marker,
                    value="tests.test_registry._secret_broken_plugin",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        with caplog.at_level(logging.WARNING):
            registry = get_registry()

        assert marker not in registry
        assert marker not in caplog.text
        for record in caplog.records:
            assert marker not in record.getMessage()
            assert marker not in repr(record.args)
            assert record.exc_info is None
            assert record.exc_text is None


class TestLazyImportPreserved:
    """Test 6: lazy_import_preserved."""

    def test_get_registry_does_not_import_backend_modules(self, monkeypatch):
        """``get_registry()`` must NOT import any backend module.

        The lazy-import invariant: ``import scrapy_extension`` works with NO
        optional dep installed, and the registry build (which happens at first
        ``get_registry()`` call) must not eager-import e.g. ``redis``.
        Otherwise a 3rd-party installing scrapy-extension without ``[redis]``
        would crash on the very first backend lookup.
        """
        # Ensure redis isn't already imported (it might be from another test in
        # the same process; the registry itself must not be what imports it).
        monkeypatch.delitem(sys.modules, "redis", raising=False)
        _reset_registry_cache()

        registry = get_registry()

        # 10 bundled descriptors returned.
        assert len(registry) >= 10
        # The descriptor table stores PATH STRINGS — the redis module itself
        # is NOT imported during registry build.
        assert "redis" not in sys.modules, (
            "get_registry() imported the redis module — registry must store "
            "path strings only (lazy-import preservation, round-5 R5-1)."
        )


class TestEntryPointApiIsModern:
    """Regression for the ``SelectableGroups dict interface is deprecated``
    warning.

    Formerly ``_discover_entry_points`` branched on ``sys.version_info`` to
    use the legacy dict shape (``entry_points().get(group, [])``) on 3.10/3.11.
    The branch rested on the false premise that ``entry_points(group=...)``
    was unavailable before 3.12 — the keyword form has been available since
    3.10. The dict fallback emitted a ``DeprecationWarning`` on every 3.10/3.11
    run and the dict interface was removed in 3.12; the keyword form works on
    every supported version, so the branch is gone. These tests lock in the
    modern single-shape API.
    """

    def test_discovery_emits_no_selectablegroups_deprecation(self):
        """The unmocked ``_discover_entry_points`` call must not emit the
        ``SelectableGroups`` deprecation warning.

        Runs against the REAL ``importlib.metadata.entry_points`` (no
        ``_patch_entry_points``) so the genuine SelectableGroups object — the
        source of the warning — is in the path. A mock would mask the bug.
        """
        import warnings

        from scrapy_extension.backends import registry as registry_mod

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            registry_mod._discover_entry_points()

        selectable = [w for w in caught if "SelectableGroups" in str(w.message)]
        assert not selectable, (
            "registry uses the deprecated entry_points() dict API; "
            f"SelectableGroups warnings leaked: {selectable}"
        )

    def test_third_party_plugin_discovered_via_group_keyword(self, monkeypatch):
        """Discovery via ``entry_points(group=...)`` resolves a 3rd-party
        plugin's registration callable (replaces the former dual-shape pair —
        only the keyword shape is used now).
        """
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="kwargepp",
                    value="tests.test_registry._register_kwarg",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        registry = get_registry()
        assert "kwargepp" in registry


# ---------------------------------------------------------------------------
# has_capability smoke (not part of the 7 PLAN tests but exercises the API).
# ---------------------------------------------------------------------------


class TestHasCapability:
    def test_has_capability_for_bundled(self):
        from scrapy_extension.backends.base import BackendType

        _reset_registry_cache()
        assert get_descriptor(BackendType.REDIS).backend_type == "redis"
        assert has_capability("redis", "queue") is True
        assert has_capability("redis", "set") is True
        assert has_capability("redis", "storage") is True
        # Kafka is queue-only per the bundled capability matrix.
        assert has_capability("kafka", "queue") is True
        assert has_capability("kafka", "set") is False

    def test_has_capability_unknown_backend(self):
        _reset_registry_cache()
        assert has_capability("not-a-backend", "queue") is False

    def test_lookup_rejects_hostile_string_subclasses_without_dispatch(self):
        marker = "registry-hostile-string-marker"
        calls: list[str] = []

        class _EvilString(str):
            def __hash__(self) -> int:
                calls.append("hash")
                raise RuntimeError(marker)

        with pytest.raises(ConfigurationError) as exc_info:
            get_descriptor(_EvilString("redis"))  # type: ignore[arg-type]

        assert marker not in str(exc_info.value)
        assert has_capability("redis", _EvilString("queue")) is False  # type: ignore[arg-type]
        assert calls == []


# ---------------------------------------------------------------------------
# Module-level broken-plugin callables for TestPluginDiscoveryErrors.
# Entry-point ``value`` strings resolve via importlib + getattr, so the
# callable must be a module-level attribute (closures won't survive the
# dotted-path lookup).
# ---------------------------------------------------------------------------


def _register_wrong_return_type() -> str:
    """Returns a non-BackendDescriptor — _load_plugin_descriptor raises TypeError."""
    return "not-a-descriptor"


def _register_unknown_capabilities() -> BackendDescriptor:
    """Declares an unsupported capability — _load_plugin_descriptor raises ValueError."""
    return _make_descriptor(
        "badcap",
        capabilities=frozenset({"queue", "streaming"}),  # 'streaming' is invalid
    )


def _register_invalid_backend_type_name() -> BackendDescriptor:
    """backend_type fails the ^[a-z][a-z0-9_]*$ pattern — raises ValueError."""
    return _make_descriptor(
        "Bad-Name",  # uppercase + hyphen violate the contract
        capabilities=frozenset({"queue"}),
    )


def _register_generic_exception() -> BackendDescriptor:
    """Callable raises a non-ImportError exception — still skip and log."""
    raise RuntimeError("plugin blew up at registration")


def _interrupting_plugin() -> BackendDescriptor:
    """A real plugin control exception must retain its normal semantics."""
    raise KeyboardInterrupt("plugin control interruption")


class TestPluginDiscoveryErrors:
    """R14-G: broken 3rd-party plugins must LOG+SKIP, never crash discovery.

    The load-bearing contract: a single misbehaving plugin must NEVER prevent
    the bundled 10 from being discovered. Every failure mode of
    ``_load_plugin_descriptor`` is caught by ``_discover_entry_points``'s broad
    ``except Exception`` is converted to a warning log + ``continue``.

    Covers:
      - callable returns the wrong type (TypeError path);
      - descriptor declares unknown capabilities (ValueError path);
      - descriptor backend_type fails the name regex (ValueError path);
      - callable raises a generic non-ImportError exception.
    """

    def _expect_skip(
        self,
        monkeypatch: pytest.MonkeyPatch,
        plugin_name: str,
        registration_value: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Patch one broken entry-point; assert bundled backends survive + log."""
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name=plugin_name,
                    value=registration_value,
                    group=_ENTRY_POINT_GROUP,
                ),
                _FakeEntryPoint(
                    name="goodplugin",
                    value="tests.test_registry._register_good_plugin",
                    group=_ENTRY_POINT_GROUP,
                ),
            ],
        )
        _reset_registry_cache()

        with warnings.catch_warnings(), caplog.at_level(logging.WARNING):
            warnings.simplefilter("error", UserWarning)
            registry = get_registry()

        # Bundled 10 always intact regardless of plugin breakage.
        for bundled in (
            "redis",
            "mongodb",
            "kafka",
            "rabbitmq",
            "elasticsearch",
            "rocketmq",
            "pulsar",
            "memcached",
            "sqs",
            "dynamodb",
        ):
            assert bundled in registry, (
                f"bundled backend {bundled!r} missing — broken plugin crashed discovery"
            )
        # The good peer plugin was discovered; the broken one was skipped.
        assert "goodplugin" in registry
        assert plugin_name not in registry
        assert "Skipping invalid third-party backend entry-point" in caplog.text

    def test_wrong_return_type_skips_with_warning(self, monkeypatch, caplog):
        """A non-BackendDescriptor return raises TypeError → skip + warning log."""
        self._expect_skip(
            monkeypatch,
            plugin_name="wrongtype",
            registration_value="tests.test_registry._register_wrong_return_type",
            caplog=caplog,
        )

    def test_unknown_capabilities_skips_with_warning(self, monkeypatch, caplog):
        """Unsupported capabilities raise ValueError → skip + warning log."""
        self._expect_skip(
            monkeypatch,
            plugin_name="badcap",
            registration_value="tests.test_registry._register_unknown_capabilities",
            caplog=caplog,
        )

    def test_invalid_backend_type_name_skips_with_warning(self, monkeypatch, caplog):
        """An invalid backend_type raises ValueError → skip + warning log."""
        self._expect_skip(
            monkeypatch,
            plugin_name="badname",
            registration_value="tests.test_registry._register_invalid_backend_type_name",
            caplog=caplog,
        )

    def test_generic_exception_skips_with_warning(self, monkeypatch, caplog):
        """A generic exception is caught, skipped, and logged."""
        self._expect_skip(
            monkeypatch,
            plugin_name="boom",
            registration_value="tests.test_registry._register_generic_exception",
            caplog=caplog,
        )

    def test_entry_points_enumeration_failure_returns_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """If ``importlib.metadata.entry_points()`` ITSELF raises (corrupted
        dist-info, broken environment), ``_discover_entry_points`` must return
        ``{}`` and never crash the caller.

        Covers the OUTER ``except Exception`` (registry.py) — distinct from the
        per-plugin load failures above: this is the enumeration call failing
        before any plugin is even inspected.
        """
        import importlib.metadata as importlib_metadata

        from scrapy_extension.backends.registry import _discover_entry_points

        def _boom(group: str | None = None) -> Any:
            raise OSError("corrupted dist-info")

        monkeypatch.setattr(importlib_metadata, "entry_points", _boom)
        _reset_registry_cache()

        # Must not raise; returns empty (no 3rd-party plugins discoverable).
        assert _discover_entry_points() == {}


class TestRegistryDiagnosticInterruptions:
    """R101: registry skip diagnostics cannot make discovery unavailable."""

    def test_enumeration_failure_diagnostic_interrupt_keeps_bundled_registry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A logger control exception cannot replace an ordinary enumeration error."""
        import importlib.metadata as importlib_metadata

        from scrapy_extension.backends import registry as registry_mod

        def _enumeration_failure(group: str | None = None) -> Any:
            raise OSError("corrupted dist-info")

        def _diagnostic_interrupt(*args: object, **kwargs: object) -> None:
            raise KeyboardInterrupt("logger interruption")

        monkeypatch.setattr(importlib_metadata, "entry_points", _enumeration_failure)
        monkeypatch.setattr(registry_mod.logger, "warning", _diagnostic_interrupt)
        _reset_registry_cache()

        registry = get_registry()

        assert registry["redis"].backend_cls_path == (
            "scrapy_extension.backends.redis.RedisBackend"
        )

    def test_broken_plugin_diagnostic_interrupt_keeps_peer_plugin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A broken-plugin warning cannot prevent later valid registrations."""
        from scrapy_extension.backends import registry as registry_mod
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        def _diagnostic_interrupt(*args: object, **kwargs: object) -> None:
            raise SystemExit("logger interruption")

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="broken",
                    value="tests.test_registry._broken_plugin",
                    group=_ENTRY_POINT_GROUP,
                ),
                _FakeEntryPoint(
                    name="goodplugin",
                    value="tests.test_registry._register_good_plugin",
                    group=_ENTRY_POINT_GROUP,
                ),
            ],
        )
        monkeypatch.setattr(registry_mod.logger, "warning", _diagnostic_interrupt)
        _reset_registry_cache()

        registry = get_registry()

        assert "redis" in registry
        assert "broken" not in registry
        assert "goodplugin" in registry

    def test_duplicate_diagnostic_interrupt_keeps_other_plugins(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A duplicate-name error cannot stop discovery of independent plugins."""
        from scrapy_extension.backends import registry as registry_mod
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        def _diagnostic_interrupt(*args: object, **kwargs: object) -> None:
            raise KeyboardInterrupt("logger interruption")

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="duplicate",
                    value="tests.test_registry._register_duplicate_first",
                    group=_ENTRY_POINT_GROUP,
                ),
                _FakeEntryPoint(
                    name="duplicate",
                    value="tests.test_registry._register_duplicate_second",
                    group=_ENTRY_POINT_GROUP,
                ),
                _FakeEntryPoint(
                    name="duplicate",
                    value="tests.test_registry._register_duplicate_first",
                    group=_ENTRY_POINT_GROUP,
                ),
                _FakeEntryPoint(
                    name="goodplugin",
                    value="tests.test_registry._register_good_plugin",
                    group=_ENTRY_POINT_GROUP,
                ),
            ],
        )
        monkeypatch.setattr(registry_mod.logger, "error", _diagnostic_interrupt)
        _reset_registry_cache()

        registry = get_registry()

        assert "redis" in registry
        assert "duplicate" not in registry
        assert "goodplugin" in registry

    def test_bundled_wins_diagnostic_interrupt_keeps_registry_usable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A shadow warning cannot turn a published bundled descriptor into failure."""
        from scrapy_extension.backends import registry as registry_mod
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        def _diagnostic_interrupt(*args: object, **kwargs: object) -> None:
            raise SystemExit("logger interruption")

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="redis",
                    value="tests.test_registry._register_shadow_redis",
                    group=_ENTRY_POINT_GROUP,
                ),
                _FakeEntryPoint(
                    name="goodplugin",
                    value="tests.test_registry._register_good_plugin",
                    group=_ENTRY_POINT_GROUP,
                ),
            ],
        )
        monkeypatch.setattr(registry_mod.logger, "warning", _diagnostic_interrupt)
        _reset_registry_cache()

        registry = get_registry()

        assert registry["redis"].backend_cls_path == (
            "scrapy_extension.backends.redis.RedisBackend"
        )
        assert "goodplugin" in registry

    def test_plugin_control_exception_still_propagates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only diagnostics are insulated; plugin control flow remains observable."""
        from scrapy_extension.backends.registry import _ENTRY_POINT_GROUP

        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint(
                    name="interrupting",
                    value="tests.test_registry._interrupting_plugin",
                    group=_ENTRY_POINT_GROUP,
                )
            ],
        )
        _reset_registry_cache()

        with pytest.raises(KeyboardInterrupt, match="plugin control interruption"):
            get_registry()
