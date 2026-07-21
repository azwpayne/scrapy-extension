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
import warnings
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.registry import (
  BackendDescriptor,
  _reset_registry_cache,
  get_descriptor,
  get_registry,
  has_capability,
)

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


def _patch_entry_points(monkeypatch: pytest.MonkeyPatch, eps: list[_FakeEntryPoint]) -> None:
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


class _StubBackend:
  """Minimal backend stub the descriptor points at.

  Constructed as ``_StubBackend(_StubSettings(**settings))`` — so the
  descriptor path actually instantiates a real (no-op) class to prove
  the dispatch table resolves end-to-end.
  """

  def __init__(self, settings: _StubSettings) -> None:
    self.settings = settings


class _StubSettings:
  """Settings stub matching ``_StubBackend``'s constructor contract."""

  def __init__(self, **kwargs: Any) -> None:
    self.kwargs = kwargs


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
    assert redis_desc.backend_cls_path == "scrapy_extension.backends.redis.RedisBackend"
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


class TestDescriptorBoundary:
  """Malformed or ambiguous plugins never enter or abort the registry."""

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
    # The capable backends list must name at least one set-capable backend
    # (redis/mongodb/elasticsearch are bundled + set-capable).
    assert "redis" in msg
    assert "mybackend" in msg


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
    assert "shadows bundled backend" in caplog.text


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
    assert "Skipping 3rd-party backend entry-point 'broken'" in caplog.text


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
    _reset_registry_cache()
    assert has_capability("redis", "queue") is True
    assert has_capability("redis", "set") is True
    assert has_capability("redis", "storage") is True
    # Kafka is queue-only per the bundled capability matrix.
    assert has_capability("kafka", "queue") is True
    assert has_capability("kafka", "set") is False

  def test_has_capability_unknown_backend(self):
    _reset_registry_cache()
    assert has_capability("not-a-backend", "queue") is False


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
    assert f"entry-point '{plugin_name}'" in caplog.text

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
