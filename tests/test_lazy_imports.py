"""Tests for PEP 562 lazy import mechanism in scrapy_extension.

Verifies that __getattr__ correctly lazy-loads optional backend classes
and settings, while core imports remain always available.
"""

from __future__ import annotations

import pytest

import scrapy_extension
import scrapy_extension.backends

# ---------------------------------------------------------------------------
# 1. Core imports always available (no optional deps needed)
# ---------------------------------------------------------------------------


class TestCoreImportsAlwaysAvailable:
    """Core classes are eagerly imported and always available."""

    @pytest.mark.parametrize(
        "name",
        [
            "Backend",
            "BackendType",
            "ConnectionManager",
            "Settings",
            "BackendError",
            "BackendConnectionError",
            "ConfigurationError",
            "QueueError",
            "SerializationError",
            "QueueBackend",
            "SetBackend",
            "StorageBackend",
            "Serializer",
            "JSONSerializer",
            "BackendDupeFilter",
            "BackendPipeline",
            "BackendQueue",
            "BackendScheduler",
            "BackendSpiderMixin",
        ],
    )
    def test_core_import_available(self, name: str) -> None:
        """Core names should be accessible without triggering __getattr__."""
        assert hasattr(scrapy_extension, name)
        assert name in dir(scrapy_extension)

    def test_core_import_from(self) -> None:
        """'from scrapy_extension import <core>' should work directly."""
        from scrapy_extension import (
            Backend,
            BackendError,
            BackendType,
            ConnectionManager,
            Settings,
        )

        assert Backend is not None
        assert BackendType is not None
        assert ConnectionManager is not None
        assert Settings is not None
        assert BackendError is not None

    def test_version_available(self) -> None:
        """__version__ should be set on the package."""
        assert hasattr(scrapy_extension, "__version__")
        assert isinstance(scrapy_extension.__version__, str)


# ---------------------------------------------------------------------------
# 2. Lazy backend imports
# ---------------------------------------------------------------------------


class TestLazyBackendImports:
    """Backend classes are lazily loaded via __getattr__."""

    @pytest.mark.parametrize(
        "name",
        [
            "RedisBackend",
            "MongoDBBackend",
            "KafkaBackend",
            "RabbitMQBackend",
            "ElasticSearchBackend",
            "RocketMQBackend",
        ],
    )
    def test_lazy_backend_import(self, name: str) -> None:
        """Each backend class should be importable from the top-level package."""
        cls = getattr(scrapy_extension, name)
        assert cls is not None
        assert callable(cls)

    def test_redis_backend_from_import(self) -> None:
        """'from scrapy_extension import RedisBackend' should work."""
        from scrapy_extension import RedisBackend

        assert RedisBackend is not None

    def test_lazy_backend_is_actual_class(self) -> None:
        """Lazily imported backends should be classes, not modules."""
        import inspect

        from scrapy_extension import RedisBackend

        assert inspect.isclass(RedisBackend)

    def test_lazy_backend_consistent_on_repeated_access(self) -> None:
        """Repeated getattr calls should return the same class."""
        cls1 = scrapy_extension.RedisBackend
        cls2 = scrapy_extension.RedisBackend
        assert cls1 is cls2


# ---------------------------------------------------------------------------
# 3. Lazy settings imports
# ---------------------------------------------------------------------------


class TestLazySettingsImports:
    """Settings classes and mode enums are lazily loaded via __getattr__."""

    def test_redis_settings_from_import(self) -> None:
        """'from scrapy_extension import RedisSettings, RedisMode' should work."""
        from scrapy_extension import RedisMode, RedisSettings

        assert RedisSettings is not None
        assert RedisMode is not None


# ---------------------------------------------------------------------------
# 4. All backend classes via lazy import
# ---------------------------------------------------------------------------


class TestAllBackendClassesLazy:
    """Every backend class in _OPTIONAL_IMPORTS should be importable."""

    BACKEND_NAMES = [
        "RedisBackend",
        "MongoDBBackend",
        "KafkaBackend",
        "RabbitMQBackend",
        "ElasticSearchBackend",
        "RocketMQBackend",
    ]

    @pytest.mark.parametrize("name", BACKEND_NAMES)
    def test_backend_class_importable(self, name: str) -> None:
        """Each backend class can be imported from scrapy_extension."""
        cls = getattr(scrapy_extension, name)
        assert cls is not None


# ---------------------------------------------------------------------------
# 5. All settings classes via lazy import
# ---------------------------------------------------------------------------


class TestAllSettingsClassesLazy:
    """Every settings class and mode enum should be importable."""

    SETTINGS_PAIRS = [
        ("RedisSettings", "RedisMode"),
        ("MongoDBSettings", "MongoDBMode"),
        ("KafkaSettings", "KafkaMode"),
        ("RabbitMQSettings", "RabbitMQMode"),
        ("ElasticSearchSettings", "ElasticSearchMode"),
        ("RocketMQSettings", "RocketMQMode"),
    ]

    @pytest.mark.parametrize("settings_cls,mode_cls", SETTINGS_PAIRS)
    def test_settings_pair_importable(
        self, settings_cls: str, mode_cls: str
    ) -> None:
        """Each (Settings, Mode) pair should be importable."""
        s_cls = getattr(scrapy_extension, settings_cls)
        m_cls = getattr(scrapy_extension, mode_cls)
        assert s_cls is not None
        assert m_cls is not None

    def test_all_settings_via_getattr(self) -> None:
        """All settings and mode names resolve via __getattr__."""
        expected = [
            "RedisSettings",
            "RedisMode",
            "MongoDBSettings",
            "MongoDBMode",
            "KafkaSettings",
            "KafkaMode",
            "RabbitMQSettings",
            "RabbitMQMode",
            "ElasticSearchSettings",
            "ElasticSearchMode",
            "RocketMQSettings",
            "RocketMQMode",
        ]
        for name in expected:
            assert hasattr(scrapy_extension, name), f"{name} should be accessible"


# ---------------------------------------------------------------------------
# 6. Invalid attribute raises AttributeError
# ---------------------------------------------------------------------------


class TestInvalidAttribute:
    """Accessing a non-existent name should raise AttributeError."""

    def test_nonexistent_class_raises_attribute_error(self) -> None:
        with pytest.raises(AttributeError, match="has no attribute"):
            _ = scrapy_extension.NonExistentClass

    def test_nonexistent_class_with_from_import(self) -> None:
        """'from scrapy_extension import NonExistent' should raise ImportError."""
        with pytest.raises(ImportError):
            from scrapy_extension import NonExistent  # noqa: F401

    @pytest.mark.parametrize(
        "name",
        ["FooBar", "redis_backend", "BackendSettings", "", "__private_nonexistent__"],
    )
    def test_various_invalid_names(self, name: str) -> None:
        """Various invalid names should all raise AttributeError."""
        with pytest.raises(AttributeError):
            _ = getattr(scrapy_extension, name)


# ---------------------------------------------------------------------------
# 7. backends/__init__.py lazy imports
# ---------------------------------------------------------------------------


class TestBackendsInitLazyImports:
    """scrapy_extension.backends uses PEP 562 __getattr__ for backend classes."""

    @pytest.mark.parametrize(
        "name",
        [
            "RedisBackend",
            "MongoDBBackend",
            "KafkaBackend",
            "RabbitMQBackend",
            "ElasticSearchBackend",
            "RocketMQBackend",
        ],
    )
    def test_backend_importable_from_backends_package(self, name: str) -> None:
        """Each backend should be importable via 'from scrapy_extension.backends import X'."""
        cls = getattr(scrapy_extension.backends, name)
        assert cls is not None

    def test_backends_core_imports(self) -> None:
        """Core classes should be eagerly available from backends package."""
        from scrapy_extension.backends import (
            Backend,
            BackendType,
            ConnectionManager,
        )

        assert Backend is not None
        assert BackendType is not None
        assert ConnectionManager is not None

    def test_backends_invalid_raises_attribute_error(self) -> None:
        """Invalid attribute on backends package should raise AttributeError."""
        with pytest.raises(AttributeError, match="has no attribute"):
            _ = scrapy_extension.backends.NonExistentBackend


# ---------------------------------------------------------------------------
# 8. __all__ contains all expected names
# ---------------------------------------------------------------------------


class TestAllExported:
    """__all__ should list all documented exports."""

    def test_top_level_all_is_list(self) -> None:
        assert isinstance(scrapy_extension.__all__, list)

    def test_top_level_all_contains_core(self) -> None:
        """__all__ must contain all core classes."""
        core_names = [
            "Backend",
            "BackendType",
            "ConnectionManager",
            "Settings",
            "BackendError",
            "BackendConnectionError",
            "ConfigurationError",
            "QueueError",
            "SerializationError",
            "QueueBackend",
            "SetBackend",
            "StorageBackend",
            "Serializer",
            "JSONSerializer",
            "BackendDupeFilter",
            "BackendPipeline",
            "BackendQueue",
            "BackendScheduler",
            "BackendSpiderMixin",
        ]
        for name in core_names:
            assert name in scrapy_extension.__all__, f"{name} missing from __all__"

    def test_top_level_all_contains_backends(self) -> None:
        """__all__ must contain all lazy backend classes."""
        backend_names = [
            "RedisBackend",
            "MongoDBBackend",
            "KafkaBackend",
            "RabbitMQBackend",
            "ElasticSearchBackend",
            "RocketMQBackend",
        ]
        for name in backend_names:
            assert name in scrapy_extension.__all__, f"{name} missing from __all__"

    def test_top_level_all_contains_settings(self) -> None:
        """__all__ must contain all lazy settings classes and mode enums."""
        settings_names = [
            "RedisSettings",
            "RedisMode",
            "MongoDBSettings",
            "MongoDBMode",
            "KafkaSettings",
            "KafkaMode",
            "RabbitMQSettings",
            "RabbitMQMode",
            "ElasticSearchSettings",
            "ElasticSearchMode",
            "RocketMQSettings",
            "RocketMQMode",
        ]
        for name in settings_names:
            assert name in scrapy_extension.__all__, f"{name} missing from __all__"

    def test_top_level_all_names_are_accessible(self) -> None:
        """Every name listed in __all__ should be accessible via getattr."""
        for name in scrapy_extension.__all__:
            assert hasattr(scrapy_extension, name), (
                f"{name} is in __all__ but not accessible"
            )

    def test_backends_all_contains_core(self) -> None:
        """backends/__init__.py __all__ must contain eagerly imported names."""
        expected = [
            "Backend",
            "BackendType",
            "ConnectionManager",
            "JSONSerializer",
            "QueueBackend",
            "Serializer",
            "SetBackend",
            "StorageBackend",
        ]
        for name in expected:
            assert name in scrapy_extension.backends.__all__, (
                f"{name} missing from backends.__all__"
            )

    def test_backends_all_contains_backend_classes(self) -> None:
        """backends.__all__ must contain every lazy backend class."""
        from scrapy_extension.backends import _BACKEND_MODULES

        for name in _BACKEND_MODULES:
            assert name in scrapy_extension.backends.__all__, (
                f"{name} missing from backends.__all__"
            )

    def test_backends_all_names_are_accessible(self) -> None:
        """Every name in backends.__all__ should be accessible."""
        for name in scrapy_extension.backends.__all__:
            assert hasattr(scrapy_extension.backends, name), (
                f"{name} is in backends.__all__ but not accessible"
            )


# ---------------------------------------------------------------------------
# 9. Lazy import internal consistency
# ---------------------------------------------------------------------------


class TestLazyImportIsolation:
    """Internal data structures should be consistent with __all__."""

    def test_optional_imports_dict_keys_match_all(self) -> None:
        """Every key in _OPTIONAL_IMPORTS should also appear in __all__."""
        from scrapy_extension import _OPTIONAL_IMPORTS

        for name in _OPTIONAL_IMPORTS:
            assert name in scrapy_extension.__all__, (
                f"{name} in _OPTIONAL_IMPORTS but not in __all__"
            )

    def test_backend_extras_covers_all_optional(self) -> None:
        """Every _OPTIONAL_IMPORTS key should have an entry in _BACKEND_EXTRAS."""
        from scrapy_extension import _BACKEND_EXTRAS, _OPTIONAL_IMPORTS

        for name in _OPTIONAL_IMPORTS:
            assert name in _BACKEND_EXTRAS, (
                f"{name} in _OPTIONAL_IMPORTS but missing from _BACKEND_EXTRAS"
            )


# ---------------------------------------------------------------------------
# 10. ImportError path — all backends give actionable install hints (R2-A3)
# ---------------------------------------------------------------------------


class TestBackendImportErrorMessage:
    """Each backend module raises ImportError with install instructions when its
    optional dep is missing. Covers all 6 backends, not just RabbitMQ.
    """

    BACKEND_MODULES = [
        ("scrapy_extension.backends.redis", "redis", "pip install scrapy-extension[redis]"),
        ("scrapy_extension.backends.mongodb", "pymongo", "pip install scrapy-extension[mongodb]"),
        ("scrapy_extension.backends.kafka", "kafka", "pip install scrapy-extension[kafka]"),
        ("scrapy_extension.backends.rabbitmq", "pika", "pip install scrapy-extension[rabbitmq]"),
        (
            "scrapy_extension.backends.elasticsearch",
            "elasticsearch",
            "pip install scrapy-extension[elasticsearch]",
        ),
        ("scrapy_extension.backends.pulsar", "pulsar", "pip install scrapy-extension[pulsar]"),
        ("scrapy_extension.backends.memcached", "pymemcache", "pip install scrapy-extension[memcached]"),
        ("scrapy_extension.backends.sqs", "boto3", "pip install scrapy-extension[sqs]"),
        ("scrapy_extension.backends.dynamodb", "boto3", "pip install scrapy-extension[dynamodb]"),
        # Note: RocketMQ uses deferred imports inside connect(), not a
        # module-level guard. Its ImportError path is tested in
        # test_rocketmq_backend.py::test_connect_*_error.
    ]

    @pytest.mark.parametrize("module_path,dep_name,install_hint", BACKEND_MODULES)
    def test_missing_dep_raises_with_install_hint(
        self,
        mocker,
        module_path: str,
        dep_name: str,
        install_hint: str,
    ) -> None:
        """Importing a backend without its dep should raise ImportError with the
        correct install hint in the message.
        """
        import builtins
        import importlib

        original_import = builtins.__import__

        def blocking_import(name, *args, **kwargs):
            if name == dep_name or name.startswith(dep_name + "."):
                raise ImportError(f"No module named '{dep_name}' (mocked)")
            return original_import(name, *args, **kwargs)

        mocker.patch.object(builtins, "__import__", side_effect=blocking_import)

        # Purge any cached version of the module so the import guard re-runs.
        import sys

        cached = sys.modules.pop(module_path, None)
        try:
            with pytest.raises(ImportError) as exc_info:
                importlib.import_module(module_path)
            assert install_hint in str(exc_info.value), (
                f"Expected install hint {install_hint!r} in error message, "
                f"got: {exc_info.value}"
            )
        finally:
            if cached is not None:
                sys.modules[module_path] = cached


class TestVersionFromPackageMetadata:
  """R26-D1: __version__ must come from package metadata, not hardcoded.

  Previously ``__version__ = "0.1.0"`` was hardcoded in ``__init__.py``.
  Bumping the version in ``pyproject.toml`` without bumping ``__init__.py``
  produced a silent drift — ``scrapy_extension.__version__`` said one
  thing, ``pip show scrapy-extension`` said another. The fix reads from
  ``importlib.metadata`` so there's a single source of truth.
  """

  def test_version_is_non_empty_string(self):
    """__version__ resolves to a non-empty string in all environments."""
    assert isinstance(scrapy_extension.__version__, str)
    assert scrapy_extension.__version__, "version must not be empty"

  def test_version_matches_installed_metadata(self):
    """When installed, __version__ matches the package's recorded version."""
    from importlib.metadata import PackageNotFoundError, version

    try:
      expected = version("scrapy-extension")
    except PackageNotFoundError:
      pytest.skip("package not installed; dev fallback path")
    assert scrapy_extension.__version__ == expected


class TestBackendsWildcardImport:
  """R36-A1: scrapy_extension.backends.__all__ must list every backend.

  Previously __all__ listed 3 of 6 backend classes (omitted
  MongoDBBackend, KafkaBackend, ElasticSearchBackend) while
  _BACKEND_MODULES had all 6. ``from scrapy_extension.backends import *``
  silently missed the 3 unlisted names; users had to know to use
  explicit imports. The fix makes __all__ match _BACKEND_MODULES.
  """

  def test_all_lists_every_backend_module(self):
    """__all__ must include every key from _BACKEND_MODULES."""
    from scrapy_extension.backends import _BACKEND_MODULES, __all__

    missing = set(_BACKEND_MODULES) - set(__all__)
    assert not missing, f"__all__ missing backend names: {sorted(missing)}"

  def test_wildcard_import_resolves_all_backend_names(self):
    """All backend names in __all__ must resolve via PEP 562 __getattr__."""
    import scrapy_extension.backends as backends

    for name in (
      "RedisBackend",
      "MongoDBBackend",
      "KafkaBackend",
      "RabbitMQBackend",
      "ElasticSearchBackend",
      "RocketMQBackend",
    ):
      assert name in backends.__all__, name
      cls = getattr(backends, name)
      assert isinstance(cls, type), f"{name} did not resolve to a class"


class TestBaseModuleAll:
  """R23-D3: scrapy_extension.backends.base must declare __all__.

  Without __all__, ``from scrapy_extension.backends.base import *`` leaks
  every non-underscored symbol including package-internal helpers like
  ``secret_value`` and ``KEY_NAME_PATTERN``. The fix lists only the 7
  user-facing ABCs / serializer / enum as the public surface.
  """

  def test_all_lists_public_surface(self):
    """__all__ must list the 7 public ABCs / serializer / enum."""
    from scrapy_extension.backends import base

    expected = {
      "Backend",
      "BackendType",
      "JSONSerializer",
      "QueueBackend",
      "Serializer",
      "SetBackend",
      "StorageBackend",
    }
    assert set(base.__all__) == expected

  def test_all_names_resolve_to_objects(self):
    """Every name in __all__ must resolve to an attribute on the module."""
    from scrapy_extension.backends import base

    for name in base.__all__:
      assert hasattr(base, name), f"__all__ lists {name!r} but module has no such attr"

  def test_helpers_not_in_all(self):
    """Package-internal helpers must not be in __all__ even if non-underscored.

  ``secret_value`` and ``KEY_NAME_PATTERN`` lack leading underscores but
  are package-internal (used by backends to unwrap SecretStr / share
  validation). End users should not depend on them — they're not in
  __all__ and may be renamed in a future refactor.
  """
    from scrapy_extension.backends import base

    assert "secret_value" not in base.__all__
    assert "KEY_NAME_PATTERN" not in base.__all__


class TestLazyImportRealBugSurfacesChain:
  """R14-H: a real bug inside a backend module must surface its real chain, NOT
  the misleading "Install with: pip install scrapy-extension[X]" hint.

  Background: ``__getattr__`` in scrapy_extension/__init__.py and
  scrapy_extension/backends/__init__.py previously wrapped ANY ImportError as
  the install hint — even when the optional dep WAS installed but a genuine
  bug inside the backend module raised ImportError. That hid the real
  traceback from the user, who was told to ``pip install`` a dep they already
  had. The fix narrows the wrap: only re-wrap when the failure is a genuine
  missing-optional-dep (``ModuleNotFoundError`` whose ``name`` is the
  backend's documented optional-dep module); otherwise re-raise the original
  so the real chain surfaces.
  """

  def _force_non_dep_import_error(self, mocker, module_path: str):
    """Patch ``importlib.import_module`` so that *only* ``module_path`` raises
    a non-ModuleNotFoundError ImportError (simulating a real bug inside the
    backend module). Other modules import normally.

    Returns the (real) ImportError instance that will be raised so the test
    can assert on its identity / message.
    """
    import importlib

    real_import = importlib.import_module
    real_bug = ImportError("real bug inside the backend module")

    def fake_import(name, package=None):
      if name == module_path:
        raise real_bug
      return real_import(name, package)

    mocker.patch.object(importlib, "import_module", side_effect=fake_import)
    return real_bug

  @pytest.mark.parametrize(
    "attr_name,module_path",
    [
      # Top-level package __getattr__ path
      ("RedisBackend", "scrapy_extension.backends.redis"),
      ("MongoDBBackend", "scrapy_extension.backends.mongodb"),
      ("KafkaBackend", "scrapy_extension.backends.kafka"),
      ("RabbitMQBackend", "scrapy_extension.backends.rabbitmq"),
      ("ElasticSearchBackend", "scrapy_extension.backends.elasticsearch"),
      ("PulsarBackend", "scrapy_extension.backends.pulsar"),
      ("SqsBackend", "scrapy_extension.backends.sqs"),
      ("DynamoDBBackend", "scrapy_extension.backends.dynamodb"),
      ("MemcachedBackend", "scrapy_extension.backends.memcached"),
    ],
  )
  def test_top_level_real_bug_surfaces_not_install_hint(
    self, mocker, attr_name: str, module_path: str
  ):
    """A non-ModuleNotFoundError from the backend module must surface the real
    chain, NOT be re-wrapped as the install hint.
    """
    import sys

    # Ensure the backend module isn't cached so importlib.import_module runs.
    # R14-G flake fix: RESTORE the popped module in finally — leaving it absent
    # from sys.modules breaks later tests that ``mocker.patch`` the backend
    # module's client class (the patch re-imports a FRESH module object while
    # the ``MongoDBBackend``/``KafkaBackend`` classes bound at those tests'
    # import time still reference the OLD module's client, so the patch never
    # applies and the real backend connects — the order-dependent flake).
    cached = sys.modules.pop(module_path, None)
    try:
      real_bug = self._force_non_dep_import_error(mocker, module_path)

      import scrapy_extension

      with pytest.raises(ImportError) as exc_info:
        getattr(scrapy_extension, attr_name)

      # The surfaced error must be the ORIGINAL ImportError, not the install hint.
      assert exc_info.value is real_bug, (
        f"Expected the original ImportError to surface (chain preserved), "
        f"but got: {exc_info.value!r}"
      )
      assert "pip install scrapy-extension" not in str(exc_info.value), (
        f"Real bug was misleadingly wrapped as install hint: {exc_info.value}"
      )
    finally:
      if cached is not None:
        sys.modules[module_path] = cached

  @pytest.mark.parametrize(
    "attr_name,module_path",
    [
      ("RedisBackend", "scrapy_extension.backends.redis"),
      ("MongoDBBackend", "scrapy_extension.backends.mongodb"),
      ("KafkaBackend", "scrapy_extension.backends.kafka"),
    ],
  )
  def test_backends_pkg_real_bug_surfaces_not_install_hint(
    self, mocker, attr_name: str, module_path: str
  ):
    """Same invariant for scrapy_extension.backends.__getattr__."""
    import sys

    # R14-G flake fix: restore the popped module in finally (see
    # test_top_level_real_bug_surfaces_not_install_hint for the rationale).
    cached = sys.modules.pop(module_path, None)
    try:
      real_bug = self._force_non_dep_import_error(mocker, module_path)

      import scrapy_extension.backends as backends_pkg

      with pytest.raises(ImportError) as exc_info:
        getattr(backends_pkg, attr_name)

      assert exc_info.value is real_bug, (
        f"Expected original ImportError to surface, got: {exc_info.value!r}"
      )
      assert "pip install scrapy-extension" not in str(exc_info.value), (
        f"Real bug was misleadingly wrapped as install hint: {exc_info.value}"
      )
    finally:
      if cached is not None:
        sys.modules[module_path] = cached

  def test_missing_optional_dep_still_gives_install_hint_top_level(
    self, mocker
  ):
    """Sanity: when the optional dep is genuinely missing, the install hint IS
    still produced (regression guard — we must not break the helpful path).
    """
    import importlib
    import sys

    module_path = "scrapy_extension.backends.redis"
    # R14-G flake fix: restore the popped module in finally (see
    # test_top_level_real_bug_surfaces_not_install_hint for the rationale).
    cached = sys.modules.pop(module_path, None)
    try:
      real_import = importlib.import_module
      missing = ModuleNotFoundError("No module named 'redis'", name="redis")

      def fake_import(name, package=None):
        if name == module_path:
          raise missing
        return real_import(name, package)

      mocker.patch.object(importlib, "import_module", side_effect=fake_import)

      import scrapy_extension

      with pytest.raises(ImportError) as exc_info:
        getattr(scrapy_extension, "RedisBackend")

      assert "pip install scrapy-extension[redis]" in str(exc_info.value)
      # And the original is preserved in the chain.
      assert exc_info.value.__cause__ is missing or exc_info.value.__cause__ is None
    finally:
      if cached is not None:
        sys.modules[module_path] = cached


class TestAllModulesInvariants:
  """R39-A1: every module with __all__ must have its names actually resolve.

  R36 closed backends/__init__.py drift; R37 closed base.py. R39 sweeps
  the remaining 4 modules (scrapy_extension/__init__, settings/__init__,
  exceptions/__init__, utils/__init__) with the same invariant: every
  name in __all__ must resolve to a real attribute on the module. This
  catches drift the moment a contributor adds a name to __all__ without
  the corresponding import (or vice versa).
  """

  @pytest.mark.parametrize(
    "module_path",
    [
      "scrapy_extension",
      "scrapy_extension.settings",
      "scrapy_extension.exceptions",
      "scrapy_extension.utils",
    ],
  )
  def test_all_names_resolve(self, module_path):
    """Every name in __all__ must resolve to an attribute on the module."""
    import importlib

    mod = importlib.import_module(module_path)
    for name in mod.__all__:
      assert hasattr(mod, name), (
        f"{module_path}.__all__ lists {name!r} but module has no such attribute"
      )


class TestBackendsGetattrInstallHint:
  """#7: backends/__init__.py __getattr__ install-hint path (lines 109-114).

  The existing suite covers the real-bug path (a non-ModuleNotFoundError
  surfaces the original via ``backends_pkg.__getattr__``) but NOT the
  genuine-missing-dep path through the BACKENDS package __getattr__ — only
  the top-level package path
  (``test_missing_optional_dep_still_gives_install_hint_top_level``) exercises
  the install hint. This closes that gap and lifts ``backends/__init__.py``
  off 61.54% (the repo's lowest-coverage file).
  """

  def test_backends_getattr_missing_dep_gives_install_hint(self, mocker):
    """Accessing ``backends.RedisBackend`` with redis missing -> install hint.

    Covers ``__getattr__`` lines 109-114 (the install-hint construction) and
    the True branch of ``_is_missing_optional_dep`` for a direct name match
    (lines 87, 90, 93-94).
    """
    import importlib
    import sys

    module_path = "scrapy_extension.backends.redis"
    # R14-G flake fix: restore the popped module in finally so later tests
    # that patch the backend module's client class still find the right
    # module object (see TestLazyImportRealBugSurfacesChain rationale).
    cached = sys.modules.pop(module_path, None)
    try:
      real_import = importlib.import_module
      missing = ModuleNotFoundError("No module named 'redis'", name="redis")

      def fake_import(name, package=None):
        if name == module_path:
          raise missing
        return real_import(name, package)

      mocker.patch.object(importlib, "import_module", side_effect=fake_import)
      import scrapy_extension.backends as backends_pkg

      with pytest.raises(ImportError) as exc_info:
        getattr(backends_pkg, "RedisBackend")

      assert "pip install scrapy-extension[redis]" in str(exc_info.value)
    finally:
      if cached is not None:
        sys.modules[module_path] = cached


class TestIsMissingOptionalDepBranches:
  """#7: direct unit tests for ``_is_missing_optional_dep`` branch coverage.

  Closes the 87-93 line gap the backends-package __getattr__ tests don't
  reach: falsy ``.name``, empty ``dep_modules`` (RocketMQ), submodule name
  match, and non-matching name.
  """

  def test_name_falsy_returns_false(self):
    """A ModuleNotFoundError with no ``.name`` -> can't classify -> False (88-89)."""
    from scrapy_extension.backends import _is_missing_optional_dep

    exc = ModuleNotFoundError()  # no args -> .name is None
    assert (
      _is_missing_optional_dep(exc, "scrapy_extension.backends.redis") is False
    )

  def test_empty_dep_modules_returns_false(self):
    """RocketMQ declares no module-level dep (frozenset()) -> always False (91-92).

    RocketMQ's optional dep (rocketmq-client-python) is imported inside
    ``connect()``, not at module level, so a module-level
    ModuleNotFoundError is never a "missing dep" signal for it.
    """
    from scrapy_extension.backends import _is_missing_optional_dep

    exc = ModuleNotFoundError("No module named 'rocketmq'", name="rocketmq")
    assert (
      _is_missing_optional_dep(exc, "scrapy_extension.backends.rocketmq")
      is False
    )

  def test_submodule_name_returns_true(self):
    """Submodule match: name 'redis.connection' -> split[0]='redis' in {redis} (95)."""
    from scrapy_extension.backends import _is_missing_optional_dep

    exc = ModuleNotFoundError(
      "No module named 'redis.connection'", name="redis.connection"
    )
    assert (
      _is_missing_optional_dep(exc, "scrapy_extension.backends.redis") is True
    )

  def test_unrelated_name_returns_false(self):
    """Name not in dep_modules -> False (real-bug-not-missing-dep case at 93-95)."""
    from scrapy_extension.backends import _is_missing_optional_dep

    exc = ModuleNotFoundError("No module named 'typo'", name="typo")
    assert (
      _is_missing_optional_dep(exc, "scrapy_extension.backends.redis") is False
    )


# ---------------------------------------------------------------------------
# 11. PEP 562 __dir__() companion — dir() and autocomplete see lazy imports
# ---------------------------------------------------------------------------


class TestDirCompanionExposesLazyImports:
    """PEP 562 __dir__() companion — dir() and IDE autocomplete see lazy imports.

    Without __dir__, ``dir(scrapy_extension)`` lists only eagerly-imported
    names; the lazily-imported __all__ members (backends, Mode enums, Settings
    classes) are invisible to ``dir()``, ``pydoc``, and IDE autocomplete even
    though they import successfully on access. The companion ``__dir__()``
    returns ``sorted(set(globals()) | set(_OPTIONAL_IMPORTS))`` so every lazy
    name is discoverable without eagerly importing its optional dep.
    """

    def test_dir_includes_lazy_backend_classes(self):
        """dir(scrapy_extension) includes all 10 lazily-imported backend classes."""
        dir_names = set(dir(scrapy_extension))
        lazy_backends = {
            "RedisBackend", "MongoDBBackend", "KafkaBackend",
            "RabbitMQBackend", "ElasticSearchBackend", "RocketMQBackend",
            "PulsarBackend", "SqsBackend", "MemcachedBackend", "DynamoDBBackend",
        }
        missing = lazy_backends - dir_names
        assert not missing, (
            f"Lazy backends missing from dir(scrapy_extension): {sorted(missing)}"
        )

    def test_dir_includes_lazy_settings_and_modes(self):
        """dir(scrapy_extension) includes lazily-imported Settings + Mode names."""
        dir_names = set(dir(scrapy_extension))
        lazy_settings = {
            "RedisSettings", "RedisMode", "SqsSettings", "SqsMode",
            "DynamoDBSettings", "DynamoDBMode", "MemcachedSettings",
            "MemcachedMode", "PulsarSettings", "PulsarMode",
        }
        missing = lazy_settings - dir_names
        assert not missing, (
            f"Lazy settings/modes missing from dir(scrapy_extension): {sorted(missing)}"
        )

    def test_dir_is_superset_of_all(self):
        """Every name in __all__ appears in dir(scrapy_extension)."""
        dir_names = set(dir(scrapy_extension))
        missing = set(scrapy_extension.__all__) - dir_names
        assert not missing, f"__all__ names missing from dir(): {sorted(missing)}"

    def test_backends_dir_includes_lazy_backend_classes(self):
        """dir(scrapy_extension.backends) includes all lazily-imported backends."""
        from scrapy_extension.backends import _BACKEND_MODULES

        dir_names = set(dir(scrapy_extension.backends))
        missing = set(_BACKEND_MODULES) - dir_names
        assert not missing, (
            f"Lazy backends missing from dir(scrapy_extension.backends): {sorted(missing)}"
        )




