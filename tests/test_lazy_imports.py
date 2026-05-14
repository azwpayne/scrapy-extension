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
        cls1 = getattr(scrapy_extension, "RedisBackend")
        cls2 = getattr(scrapy_extension, "RedisBackend")
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
            JSONSerializer,
            QueueBackend,
            Serializer,
            SetBackend,
            StorageBackend,
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
        """backends/__init__.py __all__ must contain lazy backend classes."""
        expected = [
            "RedisBackend",
            "RabbitMQBackend",
            "RocketMQBackend",
        ]
        for name in expected:
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
