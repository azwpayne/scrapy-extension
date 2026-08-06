"""Regression tests for root pytest collection gates."""

from __future__ import annotations

from pathlib import Path

pytest_plugins = ("pytester",)


def test_benchmark_enable_does_not_bypass_integration_opt_in(pytester) -> None:
    """R: benchmark opt-in must not run integration tests without their opt-in."""
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
