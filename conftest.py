"""Root-level pytest hooks.

The hooks cover opt-in benchmark behavior and protect interpreter trace state
between tests. ``pytest-benchmark``'s framework default
RUNS benchmarks on every ``pytest`` invocation (the ``--benchmark-disable`` and
``--benchmark-skip`` flags both default to ``False``). That would slow the
default ``uv run pytest`` and gate perf measurement behind noisy CI runs, so
the repo treats benchmarks as opt-in via a ``benchmark`` marker (registered in
``pyproject.toml``'s ``markers`` list). This hook skips marked tests unless the
caller passes ``--benchmark-only`` (run only benchmarks) or ``--benchmark-enable``
(run benchmarks alongside the rest). Either flag lifts the skip.

Kept in a dedicated root ``conftest.py`` (separate from ``tests/conftest.py``)
so the opt-in gate is isolated from the shared fixtures and auto-use isolation
that the rest of the suite depends on.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable

import pytest

_TRACE_BASELINE_KEY: pytest.StashKey[Callable[..., object] | None] = pytest.StashKey()
_UNEXPECTED_SKIPS: list[tuple[str, str]] = []


def _skip_reason(report: pytest.TestReport) -> str:
    """Return a stable reason string from pytest's skip report shape."""
    longrepr = report.longrepr
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        return str(longrepr[2])
    return str(longrepr)


def _skip_is_allowed(report: pytest.TestReport, reason: str) -> bool:
    """Keep deliberate benchmark and backend-optional skips out of the gate."""
    nodeid = report.nodeid.replace("\\", "/")
    if "/tests/integration/" in f"/{nodeid}" or nodeid.startswith("tests/integration/"):
        # Integration modules use backend-specific skipif guards for optional
        # services. Those are an explicit part of the integration contract.
        return True
    return reason.startswith(
        (
            "Skipped: benchmark opt-in:",
            "Skipped: pytest-benchmark not installed",
            "benchmark opt-in:",
            "pytest-benchmark not installed",
        )
    )


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Capture the current thread's tracer before a test starts.

    ``None`` is a valid baseline, so the baseline is kept in pytest's stash
    rather than represented by the absence of an attribute. ``sys.gettrace``
    is the inspection API; calling ``sys.settrace`` without an argument is not
    a supported getter.
    """
    item.stash[_TRACE_BASELINE_KEY] = sys.gettrace()


def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> None:
    """Fail and repair the test process when a test leaks its tracer.

    The exact baseline is restored before raising so a leaked tracer cannot
    affect the next test. This deliberately does not whitelist functions by
    module name: coverage, pytest, xdist, and a test-installed tracer are all
    valid only when the test leaves the process in the state it found it.
    """
    # A plugin may abort setup before our setup hook runs (the benchmark
    # plugin intentionally skips opt-in measurements). Such an item has no
    # baseline to validate and, importantly, cannot have executed test code.
    try:
        baseline = item.stash[_TRACE_BASELINE_KEY]
    except KeyError:
        return

    current = sys.gettrace()
    if current is baseline:
        return

    sys.settrace(baseline)
    raise AssertionError(
        f"Test {item.nodeid!r} leaked a trace function; "
        "save sys.gettrace() and restore it in a finally block"
    )


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Record skips that are not covered by an explicit test-tier contract."""
    if not os.environ.get("SCRAPY_TEST_FAIL_ON_UNEXPECTED_SKIP"):
        return
    if not report.skipped:
        return
    reason = _skip_reason(report)
    if not _skip_is_allowed(report, reason):
        _UNEXPECTED_SKIPS.append((report.nodeid, reason))


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Fail CI when a new unclassified skip makes a test run less meaningful."""
    del exitstatus
    if not _UNEXPECTED_SKIPS:
        return
    terminal = session.config.pluginmanager.get_plugin("terminalreporter")
    if terminal is not None:
        terminal.write_sep("=", "unexpected skips")
        for nodeid, reason in _UNEXPECTED_SKIPS:
            terminal.write_line(f"{nodeid}: {reason}")
        terminal.write_line(
            "Classify intentional skips as benchmark tests or integration tests; "
            "do not hide a skipped unit test."
        )
    session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip ``@pytest.mark.benchmark`` tests unless the caller opted in.

    Args:
        config: The pytest config object — used to read the opt-in flags.
        items: Collected test items; mutated in place to add skip markers.
    """
    has_bench_plugin = config.pluginmanager.hasplugin("benchmark")
    if not has_bench_plugin:
        # pytest-benchmark not installed: skip all marked tests with a clear reason.
        skip_bench = pytest.mark.skip(
            reason="pytest-benchmark not installed; install with --with pytest-benchmark",
        )
        for item in items:
            if "benchmark" in item.keywords:
                item.add_marker(skip_bench)
    else:
        only = config.getoption("--benchmark-only", default=False)
        # ``--benchmark-enable`` exists only when the plugin is loaded; guard with getattr.
        enable = config.getoption("--benchmark-enable", default=False)
        if not (only or enable):
            skip_bench = pytest.mark.skip(
                reason=(
                    "benchmark opt-in: pass --benchmark-only (run only benchmarks) or "
                    "--benchmark-enable (run benchmarks alongside the suite)"
                ),
            )
            for item in items:
                if "benchmark" in item.keywords:
                    item.add_marker(skip_bench)

    # R14-G: integration-tier gate. ``tests/integration/*`` e2e tests require a
    # real backend (Redis/Mongo/Kafka/RabbitMQ/ES/RocketMQ) and bit-rot silently
    # — they already self-skip on their per-backend ``SCRAPY_TEST_<BACKEND>_URL``
    # env var, but a single top-level opt-in gate makes the intent explicit and
    # keeps the tier discoverable. Skip unless ``SCRAPY_TEST_INTEGRATION=1`` is
    # set. Mirror the benchmark opt-in shape so the two slow tiers share a
    # consistent contract.
    import os

    integration_items = [item for item in items if "integration" in item.keywords]
    # Live SDK calls and cold broker startup need more headroom than the
    # socket-blocked default suite, while remaining bounded. The item marker
    # explicitly overrides pytest-timeout's repository default.
    for item in integration_items:
        item.add_marker(pytest.mark.timeout(120))

    if not os.environ.get("SCRAPY_TEST_INTEGRATION"):
        skip_integration = pytest.mark.skip(
            reason=(
                "integration opt-in: set SCRAPY_TEST_INTEGRATION=1 (and the "
                "per-backend SCRAPY_TEST_<BACKEND>_URL) to run tests/integration/*"
            ),
        )
        for item in integration_items:
            item.add_marker(skip_integration)
