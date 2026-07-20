from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_downstream_mypy_understands_lazy_public_exports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture = repo_root / "tests" / "typecheck" / "public_api.py"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--strict",
            "--no-incremental",
            str(fixture),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    output = "\n".join(part for part in (result.stdout, result.stderr) if part)
    assert result.returncode == 0, output
