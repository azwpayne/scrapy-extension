"""Contract tests for the tiered connectors lease/breaker public surface.

R141-F22: the lease/breaker names published through
``scrapy_extension.backends.connectors`` must carry an explicit stability tier
(``STABILITY.md``: "A name's tier comes from this document"), a changelog
notice, and non-``Any`` public parameter types.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, get_args, get_type_hints

import scrapy_extension.backends.connectors as connectors
from scrapy_extension.backends.connectors import (
    ConnectionManager,
    release_manager_acquire,
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_STABILITY = (_REPOSITORY_ROOT / ".github" / "STABILITY.md").read_text(encoding="utf-8")
_CHANGELOG = (_REPOSITORY_ROOT / ".github" / "CHANGELOG.md").read_text(encoding="utf-8")

_TIERED_NAMES = (
    "ConnectionManagerLease",
    "release_manager_acquire",
    "acquire_lease",
    "apply_scrapy_breaker_policy",
)


def test_lease_and_breaker_names_are_public_surface() -> None:
    for name in ("ConnectionManagerLease", "release_manager_acquire"):
        assert name in connectors.__all__
        assert getattr(connectors, name, None) is not None
    for method in ("acquire_lease", "apply_scrapy_breaker_policy"):
        assert callable(getattr(ConnectionManager, method, None))


def _stability_rows_for(name: str) -> list[str]:
    return [
        line
        for line in _STABILITY.splitlines()
        if line.startswith("|") and name in line and "Surface" not in line
    ]


def test_lease_and_breaker_names_have_explicit_experimental_tiers() -> None:
    for name in _TIERED_NAMES:
        rows = _stability_rows_for(name)
        assert rows, f"{name} has no STABILITY.md tier row"
        assert any("| Experimental |" in row for row in rows), (
            f"{name} must declare an Experimental tier"
        )


def test_lease_and_breaker_exports_have_changelog_notice() -> None:
    for name in _TIERED_NAMES:
        assert name in _CHANGELOG, f"{name} has no CHANGELOG notice"


def test_lease_and_breaker_public_parameters_are_not_any_typed() -> None:
    owner_hint = get_type_hints(release_manager_acquire)["owner"]
    assert owner_hint is not Any
    owner_args = get_args(owner_hint)
    assert ConnectionManager in owner_args
    assert connectors.ConnectionManagerLease in owner_args

    settings_hint = get_type_hints(ConnectionManager.apply_scrapy_breaker_policy)[
        "settings"
    ]
    assert settings_hint is not Any
