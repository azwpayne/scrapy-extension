"""Contracts for runnable examples, documentation commands, and artifact roles."""

from __future__ import annotations

import importlib
import pkgutil
import re
from pathlib import Path

from scrapy.settings import Settings
from scrapy.spiderloader import SpiderLoader

_EXPECTED_SPIDERS = {
    "quotes",
    "quotes_connection_manager",
    "quotes_crawl",
    "quotes_elasticsearch",
    "quotes_kafka",
    "quotes_mongodb",
    "quotes_multi_mode",
    "quotes_programmatic",
    "quotes_rabbitmq",
    "quotes_redis",
}


def test_examples_import_and_discover_without_broker_io(monkeypatch) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(repository_root / "examples"))
    examples = importlib.import_module("examples")

    modules = sorted(
        module.name
        for module in pkgutil.walk_packages(examples.__path__, examples.__name__ + ".")
    )
    assert modules
    for module in modules:
        importlib.import_module(module)

    loader = SpiderLoader.from_settings(
        Settings({"SPIDER_MODULES": ["examples.spiders"]})
    )
    assert set(loader.list()) == _EXPECTED_SPIDERS


def test_shipped_guidance_uses_uv_and_loopback_socket_allowlists() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    guidance = [
        repository_root / "README.md",
        repository_root / "examples" / "README.md",
        repository_root / ".github" / "CONTRIBUTING.md",
        repository_root / "docs" / "runbook.md",
        repository_root / "tests" / "integration" / "docker-compose.yml",
        *sorted((repository_root / "tests" / "integration").glob("test_*.py")),
    ]

    for path in guidance:
        text = path.read_text(encoding="utf-8")
        assert "--force-enable-socket" not in text, path
        for line in text.splitlines():
            assert not line.strip().startswith("scrapy "), (path, line)

    examples_readme = (repository_root / "examples" / "README.md").read_text(
        encoding="utf-8"
    )
    assert "uv run --no-sync scrapy list" in examples_readme
    assert "uv run --no-sync scrapy crawl" in examples_readme


def test_readme_metadata_links_are_absolute() -> None:
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(
        encoding="utf-8"
    )
    destinations = re.findall(r"\]\(([^)]+)\)", readme)
    relative = [
        destination
        for destination in destinations
        if not destination.startswith(("https://", "http://", "#", "mailto:"))
    ]
    assert relative == []


def test_ci_examples_smoke_is_discovery_only() -> None:
    workflow = (
        Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
    ).read_text(encoding="utf-8")
    block = workflow.split(
        "- name: Discover and import examples without starting a crawl", 1
    )[1].split("- name:", 1)[0]

    assert "uv run --no-sync scrapy list" in block
    assert "pkgutil.walk_packages" in block
    assert "scrapy crawl" not in block
    assert "docker run" not in block
