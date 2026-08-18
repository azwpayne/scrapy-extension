"""Regression tests for root pytest collection gates and tier boundaries."""

from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib

pytest_plugins = ("pytester",)


def _install_root_hooks(pytester) -> None:
    root_conftest = Path(__file__).resolve().parents[1] / "conftest.py"
    pytester.makeconftest(
        f"""
from importlib.util import module_from_spec, spec_from_file_location

_spec = spec_from_file_location("root_conftest", {str(root_conftest)!r})
assert _spec is not None and _spec.loader is not None
_module = module_from_spec(_spec)
_spec.loader.exec_module(_module)
pytest_collection_modifyitems = _module.pytest_collection_modifyitems
"""
    )


def test_benchmark_enable_does_not_bypass_integration_opt_in(pytester) -> None:
    """Benchmark opt-in must not run integration tests without their opt-in."""
    _install_root_hooks(pytester)
    pytester.makepyfile(
        """
import pytest


@pytest.mark.integration
def test_requires_integration_opt_in():
  pass
"""
    )

    result = pytester.runpytest("--benchmark-enable")

    result.assert_outcomes(skipped=1)


def test_integration_collection_gets_explicit_timeout_override(
    pytester, monkeypatch
) -> None:
    """Live-tier items receive the bounded override before they execute."""
    monkeypatch.setenv("SCRAPY_TEST_INTEGRATION", "1")
    _install_root_hooks(pytester)
    pytester.makepyfile(
        """
import pytest


@pytest.mark.integration
def test_timeout_contract(request):
  marker = request.node.get_closest_marker("timeout")
  assert marker is not None
  assert marker.args == (120,)
"""
    )

    result = pytester.runpytest()

    result.assert_outcomes(passed=1)


def test_every_live_broker_module_declares_the_integration_tier() -> None:
    """Path and marker stay equivalent, so CI cannot silently deselect a broker."""
    integration_dir = Path(__file__).resolve().parent / "integration"
    modules = sorted(integration_dir.glob("test_*.py"))

    assert modules
    for module in modules:
        source = module.read_text(encoding="utf-8")
        assert "pytest.mark.integration" in source, module.name


def test_marker_registry_is_strict_and_contains_only_used_labels() -> None:
    """Typos fail collection and stale aspirational labels stay unregistered."""
    pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject_path.open("rb") as stream:
        pytest_config = tomllib.load(stream)["tool"]["pytest"]["ini_options"]

    assert pytest_config["strict_markers"] is True
    registered = {entry.split(":", 1)[0] for entry in pytest_config["markers"]}
    assert registered == {"unit", "integration", "e2e", "benchmark"}


def test_ci_uses_only_complete_tier_selectors() -> None:
    """Partial unit/e2e labels must never be advertised as complete CI tiers."""
    workflow = (
        Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
    ).read_text(encoding="utf-8")

    assert '-m "not integration"' in workflow
    assert "-m integration" in workflow
    assert "-m unit" not in workflow
    assert "-m e2e" not in workflow
