"""Run destructive interpreter-trace probes outside the pytest process.

CPython 3.12 can crash when coverage.py and a test both replace the trace
function while a traced worker raises from an opcode event.  These probes need
real interpreter tracing to interrupt otherwise non-fallible assignment
boundaries, so execute the selected test in a clean child interpreter instead
of weakening the assertion or risking the parent coverage process.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_TRACE_CHILD_ENV = "SCRAPY_TRACE_BOUNDARY_CHILD"
_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_trace_probe_in_subprocess(request: pytest.FixtureRequest) -> bool:
    """Return ``True`` after this test passed in an uninstrumented child.

    The child selects the exact current parametrized node and sets a private
    recursion guard.  Coverage/xdist environment hooks are removed so the child
    owns ``sys.settrace`` exclusively; the parent still records and reports the
    subprocess assertion as an ordinary test result.
    """
    if os.environ.get(_TRACE_CHILD_ENV) == "1":
        return False

    environment = os.environ.copy()
    for name in tuple(environment):
        if (
            name.startswith("COV_CORE_")
            or name.startswith("PYTEST_XDIST_")
            or name
            in {"COVERAGE_FILE", "COVERAGE_PROCESS_START", "PYTEST_CURRENT_TEST"}
        ):
            environment.pop(name, None)
    environment[_TRACE_CHILD_ENV] = "1"

    completed = subprocess.run(  # noqa: S603 - fixed interpreter/module/nodeid
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            request.node.nodeid,
            "--no-cov",
            "--randomly-seed=1125147632",
        ],
        cwd=_PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, (
        f"trace-boundary child failed ({completed.returncode})\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    return True
