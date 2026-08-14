"""Executable contract for the third-party backend authoring guide."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_GUIDE_PATH = Path(__file__).resolve().parents[1] / "docs" / "backend-plugins.md"
_BACKEND_EXAMPLE = re.compile(
    r"### `mybackend_plugin/backends\.py` .*?\n\n```python\n(.*?)\n```",
    re.DOTALL,
)


def _load_documented_backend(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Load the guide's backend block without requiring a separate plugin wheel."""
    match = _BACKEND_EXAMPLE.search(_GUIDE_PATH.read_text(encoding="utf-8"))
    assert match is not None, "Could not find the documented MyBackend code block."

    package = ModuleType("mybackend_plugin")
    package.__path__ = []
    settings_module = ModuleType("mybackend_plugin.settings")

    class MySettings:
        pass

    settings_module.__dict__["MySettings"] = MySettings
    package.__dict__["settings"] = settings_module
    monkeypatch.setitem(sys.modules, "mybackend_plugin", package)
    monkeypatch.setitem(sys.modules, "mybackend_plugin.settings", settings_module)

    namespace: dict[str, Any] = {}
    exec(compile(match.group(1), str(_GUIDE_PATH), "exec"), namespace)
    return namespace["MyBackend"](MySettings())


def test_documented_backend_example_honors_backend_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _load_documented_backend(monkeypatch)

    backend.push("alpha", b"alpha-low", priority=0.0)
    backend.push("beta", b"beta-low", priority=0.0)
    backend.push("beta", b"beta-high-first", priority=10.0)
    backend.push("beta", b"beta-high-second", priority=10.0)

    assert backend.queue_len("alpha") == 1
    assert backend.queue_len("beta") == 3
    assert backend.pop("beta") == b"beta-high-first"
    assert backend.pop("beta") == b"beta-high-second"
    assert backend.pop("beta") == b"beta-low"
    assert backend.pop("alpha") == b"alpha-low"

    backend.push("alpha", b"alpha-to-clear")
    backend.push("beta", b"beta-to-keep")
    backend.clear_queue("alpha")
    assert backend.pop("alpha") is None
    assert backend.pop("beta") == b"beta-to-keep"

    assert backend.remove("seen", b"item") is False
    assert backend.add("seen", b"item") is True
    assert backend.remove("seen", b"item") is True
    assert backend.remove("seen", b"item") is False

    assert backend.delete("stored") is False
    backend.store("stored", b"payload")
    assert backend.delete("stored") is True
    assert backend.retrieve("stored") is None
    assert backend.delete("stored") is False
