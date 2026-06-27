"""Root-level pytest hooks.

Scoped to opt-in benchmark behavior. ``pytest-benchmark``'s framework default
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

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
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
    return

  only = config.getoption("--benchmark-only", default=False)
  # ``--benchmark-enable`` exists only when the plugin is loaded; guard with getattr.
  enable = config.getoption("--benchmark-enable", default=False)
  if only or enable:
    return

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

  if not os.environ.get("SCRAPY_TEST_INTEGRATION"):
    skip_integration = pytest.mark.skip(
      reason=(
        "integration opt-in: set SCRAPY_TEST_INTEGRATION=1 (and the "
        "per-backend SCRAPY_TEST_<BACKEND>_URL) to run tests/integration/*"
      ),
    )
    for item in items:
      if "integration" in item.keywords:
        item.add_marker(skip_integration)
