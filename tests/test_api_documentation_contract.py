"""Executable contracts for public API and documentation truthfulness."""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin
from scrapy_extension.storage.strategies.factory import create_storage_strategy

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_README = (_REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
_RUNBOOK = (_REPOSITORY_ROOT / "docs" / "runbook.md").read_text(encoding="utf-8")
_STABILITY = (_REPOSITORY_ROOT / ".github" / "STABILITY.md").read_text(encoding="utf-8")
_SECURITY = (_REPOSITORY_ROOT / ".github" / "SECURITY.md").read_text(encoding="utf-8")
_CHANGELOG = (_REPOSITORY_ROOT / ".github" / "CHANGELOG.md").read_text(encoding="utf-8")


def test_component_factory_documentation_matches_runtime_api() -> None:
    expected = {
        BackendScheduler: (True, True),
        BackendDupeFilter: (True, True),
        BackendPipeline: (True, True),
        BackendQueue: (False, False),
        BackendSpiderMixin: (False, True),
    }
    for component, (from_settings, from_crawler) in expected.items():
        assert hasattr(component, "from_settings") is from_settings
        assert hasattr(component, "from_crawler") is from_crawler

    spider_parameter = inspect.signature(BackendQueue).parameters["spider"]
    assert spider_parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert spider_parameter.default is None

    for documented_row in (
        "| `BackendScheduler` | `from_settings()` and `from_crawler()` |",
        "| `BackendDupeFilter` | `from_settings()` and `from_crawler()` |",
        "| `BackendPipeline` | `from_settings()` and `from_crawler()` |",
        "| `BackendQueue` | Direct constructor only; optional keyword-only `spider=None` |",
        "| `BackendSpiderMixin` | `from_crawler()` through Scrapy, or direct construction followed by explicit `setup_backend()` |",
    ):
        assert documented_row in _README
    assert "`BackendQueue.spider` is a required keyword-only argument" not in _CHANGELOG


def test_storage_strategy_names_are_documented_and_enforced_as_case_sensitive() -> None:
    assert create_storage_strategy("passthrough").__class__.__name__ == (
        "PassthroughStorageStrategy"
    )
    with pytest.raises(ConfigurationError):
        create_storage_strategy("PASSTHROUGH")

    assert "Storage strategy names are case-sensitive" in _README
    assert "Names are case-sensitive: use lowercase" in _RUNBOOK
    assert "Case-sensitive strategy name" in inspect.getdoc(create_storage_strategy)


def test_pre_release_support_and_symbol_tiers_are_explicit() -> None:
    assert "latest published pre-1.0 release" in _SECURITY
    assert "older pre-1.0 releases" in _SECURITY
    assert "latest `0.1.x`" not in _SECURITY

    assert "### Public API tiers" in _README
    for tier in ("| Stable |", "| Experimental |", "| Private (Internal) |"):
        assert tier in _README
    assert "does not add or remove exports" in _README
    assert (
        "[stability policy](https://github.com/azwpayne/scrapy-extension/blob/main/"
        ".github/STABILITY.md)" in _README
    )
    assert "Public surface is determined by the owning namespace" in _STABILITY


def test_active_documentation_local_links_resolve() -> None:
    """Every repository-local link in the active policy/API docs must exist."""
    markdown_link = re.compile(r"\[[^]]+\]\(([^)]+)\)")
    documents = (
        _REPOSITORY_ROOT / "README.md",
        _REPOSITORY_ROOT / ".github" / "SECURITY.md",
        _REPOSITORY_ROOT / ".github" / "STABILITY.md",
    )

    for document in documents:
        for target in markdown_link.findall(document.read_text(encoding="utf-8")):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            path_text = target.split("#", 1)[0]
            if not path_text:
                continue
            resolved = (document.parent / path_text).resolve()
            assert resolved.exists(), f"Broken link in {document}: {target}"
