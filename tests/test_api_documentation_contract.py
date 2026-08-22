"""Executable contracts for public API and documentation truthfulness."""

from __future__ import annotations

import base64
import inspect
import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.monitor.base import Monitor
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings import Settings
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin
from scrapy_extension.storage.strategies.factory import create_storage_strategy
from tests.integration import bench_es_push_refresh

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib

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


def test_reliability_policy_and_identity_defaults_are_documented() -> None:
    assert Settings().pipeline_max_storage_errors == 10
    assert "SCRAPY_PIPELINE_MAX_STORAGE_ERRORS` defaults to **10**" in _README
    assert "explicitly to `None` only to opt into best-effort loss" in _RUNBOOK
    assert "default `10`, so the eleventh consecutive failure" in _RUNBOOK
    assert "scheduler-queue:{project}:{spider}" in _README
    assert "dupefilter:{project}:{spider}" in _RUNBOOK
    assert "SCRAPY_QUEUE_ALLOW_CROSS_SPIDER" in _STABILITY


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


def _coverage_gate_script() -> str:
    workflow = yaml.safe_load(
        (_REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
    )
    runs = [
        step.get("run")
        for job in workflow["jobs"].values()
        for step in job.get("steps", ())
        if isinstance(step, dict)
    ]
    coverage_runs = [
        run
        for run in runs
        if isinstance(run, str) and "--cov-report=json:coverage.json" in run
    ]
    assert len(coverage_runs) == 1
    heredoc = re.search(
        r"<<'PY'\n(?P<script>.*?)\n\s*PY(?:\n|$)",
        coverage_runs[0],
        flags=re.DOTALL,
    )
    assert heredoc is not None
    return heredoc.group("script")


def _execute_coverage_gate(
    script: str,
    totals: dict[str, int],
    temporary_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (temporary_directory / "coverage.json").write_text(
        json.dumps({"totals": totals}), encoding="utf-8"
    )
    monkeypatch.chdir(temporary_directory)
    exec(compile(script, "<ci-coverage-gate>", "exec"), {})


def test_coverage_configuration_and_ci_floors_are_enforced_semantically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with (_REPOSITORY_ROOT / "pyproject.toml").open("rb") as stream:
        pyproject = tomllib.load(stream)
    coverage = pyproject["tool"]["coverage"]
    assert coverage["run"]["branch"] is True
    assert coverage["report"]["fail_under"] == 0

    script = _coverage_gate_script()
    exact_floors = {
        "covered_lines": 95,
        "num_statements": 100,
        "covered_branches": 91,
        "num_branches": 100,
    }
    _execute_coverage_gate(script, exact_floors, tmp_path, monkeypatch)

    below_statement_floor = {
        **exact_floors,
        "covered_lines": 9499,
        "num_statements": 10000,
    }
    with pytest.raises(AssertionError):
        _execute_coverage_gate(script, below_statement_floor, tmp_path, monkeypatch)

    below_branch_floor = {
        **exact_floors,
        "covered_branches": 9099,
        "num_branches": 10000,
    }
    with pytest.raises(AssertionError):
        _execute_coverage_gate(script, below_branch_floor, tmp_path, monkeypatch)


def test_configuration_error_families_match_runtime_boundaries() -> None:
    with pytest.raises(ValidationError) as direct_error:
        Settings(retry_attempts=-1)
    assert direct_error.value.errors()[0]["loc"] == ("retry_attempts",)
    assert direct_error.value.errors()[0]["input"] is None

    manager = ConnectionManager(
        "elasticsearch", {"request_timeout": "not-a-valid-timeout"}
    )
    with pytest.raises(ConfigurationError) as manager_error:
        manager._create_backend()
    assert manager_error.value.setting_name == "backend_settings"
    assert manager_error.value.setting_value is None


class _FakeElasticSearchIndices:
    def __init__(self, owner: _FakeElasticSearchClient) -> None:
        self._owner = owner

    def refresh(self, **kwargs: Any) -> dict[str, Any]:
        self._owner.refresh_calls.append(kwargs)
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}


class _FakeElasticSearchClient:
    def __init__(self) -> None:
        self.transport = object()
        self.options_calls: list[dict[str, Any]] = []
        self.index_calls: list[dict[str, Any]] = []
        self.delete_by_query_calls: list[dict[str, Any]] = []
        self.refresh_calls: list[dict[str, Any]] = []
        self.indices = _FakeElasticSearchIndices(self)
        self.closed = False

    def options(self, **kwargs: Any) -> _FakeElasticSearchClient:
        self.options_calls.append(kwargs)
        return self

    def index(self, **kwargs: Any) -> dict[str, Any]:
        self.index_calls.append(kwargs)
        return {
            "_index": kwargs["index"],
            "_id": kwargs["id"],
            "result": "created",
            "_shards": {"total": 1, "successful": 1, "failed": 0},
        }

    def delete_by_query(self, **kwargs: Any) -> dict[str, Any]:
        self.delete_by_query_calls.append(kwargs)
        return {
            "timed_out": False,
            "failures": [],
            "total": 0,
            "deleted": 0,
            "version_conflicts": 0,
        }

    def close(self) -> None:
        self.closed = True


def test_es_push_benchmark_requires_explicit_environment_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SCRAPY_TEST_INTEGRATION", raising=False)
    monkeypatch.delenv("SCRAPY_TEST_ES_HOSTS", raising=False)

    constructed = False

    def fail_if_constructed(*_args: Any, **_kwargs: Any) -> None:
        nonlocal constructed
        constructed = True
        raise AssertionError("backend construction crossed the opt-in gate")

    monkeypatch.setattr(
        bench_es_push_refresh, "ElasticSearchBackend", fail_if_constructed
    )
    with pytest.raises(SystemExit):
        bench_es_push_refresh.main()
    assert constructed is False

    environment = {
        "SCRAPY_TEST_INTEGRATION": "1",
        "SCRAPY_TEST_ES_HOSTS": " http://127.0.0.1:9200, http://localhost:9200 ",
    }
    assert bench_es_push_refresh.configured_hosts(environment) == [
        "http://127.0.0.1:9200",
        "http://localhost:9200",
    ]


def test_es_push_benchmark_uses_production_backend_and_cleans_up_without_network() -> (
    None
):
    client = _FakeElasticSearchClient()
    backend = ElasticSearchBackend(
        ElasticSearchSettings(hosts=["http://127.0.0.1:9200"])
    )
    backend._client = client  # type: ignore[assignment]
    moments = iter((0.0, 0.1, 1.0, 1.3))

    result = bench_es_push_refresh.run_benchmark(
        backend,
        "benchmark-contract",
        sample_count=2,
        clock=lambda: next(moments),
    )

    assert result.total == pytest.approx(0.4)
    assert result.mean == pytest.approx(0.2)
    assert result.p95 == pytest.approx(0.3)
    assert result.maximum == pytest.approx(0.3)
    assert client.options_calls == [
        {"max_retries": 0, "retry_on_timeout": False, "retry_on_status": ()}
    ]
    assert len(client.index_calls) == 2
    assert [
        base64.b64decode(call["document"]["item"]) for call in client.index_calls
    ] == [b"item-000", b"item-001"]
    assert all(
        call["document"]["queue_name"] == "benchmark-contract"
        for call in client.index_calls
    )
    assert client.delete_by_query_calls == [
        {
            "index": backend.config.queue_index,
            "query": {"term": {"queue_name.keyword": "benchmark-contract"}},
        }
    ]
    assert client.refresh_calls == [
        {"index": backend.config.queue_index},
        {"index": backend.config.queue_index},
    ]
    assert client.closed is True
    assert backend.is_connected() is False


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


def _documented_attribute_names(cls: type) -> list[str]:
    """Extract entry names from a class docstring ``Attributes:`` section.

    Parses the dedented docstring so entries are exactly the lines matching
    ``    name:`` (continuation lines are indented deeper). Continues until the
    next blank line, matching how both component docstrings are formatted.
    """
    docstring = inspect.getdoc(cls)
    assert docstring is not None, f"{cls.__name__} must have a docstring"
    section = re.search(
        r"Attributes:\n(?P<body>.*?)(?:\n\n|\Z)", docstring, flags=re.DOTALL
    )
    assert section is not None, f"{cls.__name__} must document Attributes"
    names = [
        match.group("name")
        for match in (
            re.match(r"^ {4}(?P<name>\w+):", line)
            for line in section.group("body").splitlines()
        )
        if match is not None
    ]
    assert names, f"{cls.__name__} Attributes section parsed as empty"
    return names


def test_documented_attributes_are_readable_on_component_instances(
    mock_connection_manager: Any, mock_spider: Any
) -> None:
    """R141-F9: every docstring-declared Attribute must exist on the instance.

    The Stable-tier class docstrings are the public attribute contract — a
    documented name that raises ``AttributeError`` breaks downstream callers
    who trusted the docs. Read-only access on a freshly constructed (never
    started) instance is the contract surface.
    """
    queue_with_spider = BackendQueue(
        connection_manager=mock_connection_manager,
        queue_name="doc-attribute-contract",
        spider=mock_spider,
    )
    for name in _documented_attribute_names(BackendQueue):
        getattr(queue_with_spider, name)

    assert queue_with_spider.connection_manager is mock_connection_manager
    assert queue_with_spider.queue_name == "doc-attribute-contract"
    assert queue_with_spider.spider is mock_spider
    assert isinstance(queue_with_spider.serializer, JSONSerializer)
    assert queue_with_spider.serializer is queue_with_spider.serializer

    queue_without_spider = BackendQueue(
        connection_manager=mock_connection_manager,
        queue_name="doc-attribute-contract",
    )
    for name in _documented_attribute_names(BackendQueue):
        getattr(queue_without_spider, name)
    assert queue_without_spider.spider is None

    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    for name in _documented_attribute_names(BackendPipeline):
        getattr(pipeline, name)
    assert isinstance(pipeline.serializer, JSONSerializer)
    assert pipeline.serializer is pipeline.serializer


def test_monitor_docstring_hook_inventory_matches_public_hook_methods() -> None:
    """R141-F23: the ``Monitor`` Hooks list is the de-facto hook contract.

    STABILITY advertises the monitor hook set as additive; the class docstring
    list is its inventory. Every public ``on_*`` method must appear there (and
    vice versa) so custom-monitor authors cannot miss a gauge.
    """
    docstring = inspect.getdoc(Monitor)
    assert docstring is not None, "Monitor must have a docstring"
    section = re.search(
        r"Hooks \(all no-ops by default\):\n\n(?P<body>.*?)(?:\n\n|\Z)",
        docstring,
        flags=re.DOTALL,
    )
    assert section is not None, "Monitor docstring must have a Hooks section"
    documented = set(re.findall(r"``(?P<hook>on_\w+)\(", section.group("body")))

    defined = {
        name
        for name in dir(Monitor)
        if name.startswith("on_") and callable(getattr(Monitor, name, None))
    }
    assert documented, "Hooks section parsed as empty"
    assert documented == defined, (
        "Monitor docstring hook inventory diverged from the public hook "
        f"methods: missing_from_docs={sorted(defined - documented)} "
        f"stale_in_docs={sorted(documented - defined)}"
    )
