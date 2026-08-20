"""Executable contracts for source-distribution completeness and hygiene."""

from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib


_REQUIRED_SOURCE_ENTRIES = {
    ".github/audit-fixtures/**",
    ".github/audit-waivers.toml",
    ".github/workflows/**",
    "conftest.py",
    "tests/**",
    "tools/**",
    "uv.lock",
}
_REQUIRED_EXCLUDES = {
    "**/.coverage.*",
    "./.cache/**",
    "**/.pytest_cache/**",
    "**/*.log",
    "**/*.sqlite3",
    "**/local_settings.py",
    "**/generated-canary*/**",
    "docs/audits/**",
}


def _pyproject() -> dict[str, object]:
    root = Path(__file__).resolve().parents[1]
    with (root / "pyproject.toml").open("rb") as stream:
        return tomllib.load(stream)


def test_isolated_build_backend_is_exactly_pinned() -> None:
    build_system = _pyproject()["build-system"]
    assert isinstance(build_system, dict)
    assert build_system["requires"] == ["uv_build==0.12.5"]


def test_sdist_configuration_keeps_tests_self_contained_and_clean() -> None:
    tool = _pyproject()["tool"]
    assert isinstance(tool, dict)
    uv = tool["uv"]
    assert isinstance(uv, dict)
    build = uv["build-backend"]
    assert isinstance(build, dict)

    includes = set(build["source-include"])
    excludes = set(build["source-exclude"])
    assert _REQUIRED_SOURCE_ENTRIES <= includes
    assert _REQUIRED_EXCLUDES <= excludes
