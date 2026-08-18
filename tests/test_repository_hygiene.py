"""Executable contracts for tracked-file and documentation hygiene."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")


def _require_git_checkout() -> None:
    """Skip source-distribution runs, which intentionally have no Git metadata."""
    if not (_ROOT / ".git").exists():
        pytest.skip("repository hygiene requires a Git checkout")


def _tracked_markdown() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z", "--", "*.md"],
        cwd=_ROOT,
        check=True,
        capture_output=True,
    )
    return [
        _ROOT / path.decode()
        for path in result.stdout.rstrip(b"\0").split(b"\0")
        if path
    ]


def test_no_tracked_file_is_ignored() -> None:
    _require_git_checkout()
    result = subprocess.run(
        ["git", "ls-files", "-ci", "--exclude-standard"],
        cwd=_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout == ""


def test_tracked_markdown_relative_links_resolve() -> None:
    _require_git_checkout()
    missing: list[str] = []

    for document in _tracked_markdown():
        for match in _MARKDOWN_LINK.finditer(document.read_text(encoding="utf-8")):
            raw_target = match.group(1).strip()
            if raw_target.startswith("<") and ">" in raw_target:
                raw_target = raw_target[1 : raw_target.index(">")]
            else:
                # Markdown permits an optional title after a whitespace separator.
                raw_target = raw_target.split(maxsplit=1)[0]

            target = urlsplit(unquote(raw_target))
            if target.scheme or target.netloc or not target.path:
                continue

            destination = (document.parent / target.path).resolve()
            try:
                destination.relative_to(_ROOT)
            except ValueError:
                missing.append(
                    f"{document.relative_to(_ROOT)} -> {raw_target} (outside repo)"
                )
                continue
            if not destination.exists():
                missing.append(f"{document.relative_to(_ROOT)} -> {raw_target}")

    assert not missing, "unresolved tracked relative links:\n" + "\n".join(missing)
