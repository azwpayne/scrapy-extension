"""Contract tests for bundled backend metadata and lazy public exports.

The bundled registry is intentionally lazy: descriptors contain dotted-path
strings, so this test verifies only metadata and never resolves a backend
class or imports an optional SDK.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import scrapy_extension
import scrapy_extension.backends as public_backends
from scrapy_extension.backends.registry import _BUNDLED_DESCRIPTORS

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib

pytestmark = pytest.mark.unit


def _split_dotted_path(path: str) -> tuple[str, str]:
    module_path, separator, attribute_name = path.rpartition(".")
    assert separator and module_path and attribute_name, (
        f"Expected a dotted import path, got {path!r}"
    )
    return module_path, attribute_name


def test_bundled_registry_metadata_matches_lazy_exports_and_extras() -> None:
    """Every bundled descriptor has one matching public optional surface.

    Adding a built-in backend requires the registry descriptor, both PEP 562
    backend exports, top-level settings/mode exports, install-hint extras, and a
    corresponding project extra. This catches a missed hand-maintained mapping
    without resolving any dotted path.
    """
    repository_root = Path(__file__).resolve().parents[1]
    with (repository_root / "pyproject.toml").open("rb") as pyproject_file:
        pyproject = tomllib.load(pyproject_file)
    project_extras = pyproject["project"]["optional-dependencies"]

    expected_backend_names = {
        _split_dotted_path(descriptor.backend_cls_path)[1]
        for descriptor in _BUNDLED_DESCRIPTORS.values()
    }
    top_level_backend_names = {
        name
        for name, (module_path, _) in scrapy_extension._OPTIONAL_IMPORTS.items()
        if module_path.startswith("scrapy_extension.backends.")
    }
    top_level_backend_extra_names = {
        name for name in scrapy_extension._BACKEND_EXTRAS if name.endswith("Backend")
    }

    assert top_level_backend_names == expected_backend_names
    assert top_level_backend_extra_names == expected_backend_names
    assert set(public_backends._BACKEND_MODULES) == expected_backend_names
    assert set(public_backends._BACKEND_EXTRAS) == expected_backend_names

    for backend_type, descriptor in _BUNDLED_DESCRIPTORS.items():
        backend_module, backend_name = _split_dotted_path(descriptor.backend_cls_path)
        settings_module, settings_name = _split_dotted_path(
            descriptor.settings_cls_path
        )
        assert descriptor.backend_type == backend_type
        assert settings_name.endswith("Settings")
        mode_name = f"{settings_name.removesuffix('Settings')}Mode"

        assert scrapy_extension._OPTIONAL_IMPORTS[backend_name] == (
            backend_module,
            backend_name,
        )
        assert public_backends._BACKEND_MODULES[backend_name] == (
            backend_module,
            backend_name,
        )
        assert settings_module.startswith("scrapy_extension.settings")
        settings_export_module, settings_export_name = (
            scrapy_extension._OPTIONAL_IMPORTS[settings_name]
        )
        mode_export_module, mode_export_name = scrapy_extension._OPTIONAL_IMPORTS[
            mode_name
        ]
        assert settings_export_name == settings_name
        assert mode_export_name == mode_name
        assert settings_export_module.startswith("scrapy_extension.settings.")
        assert mode_export_module == settings_export_module

        for name in (backend_name, settings_name, mode_name):
            assert scrapy_extension._BACKEND_EXTRAS[name] == backend_type
            assert name in scrapy_extension.__all__
        assert public_backends._BACKEND_EXTRAS[backend_name] == backend_type
        assert backend_name in public_backends.__all__

        assert backend_type in project_extras
        assert project_extras[backend_type]
