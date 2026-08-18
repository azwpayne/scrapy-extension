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


def _load_documented_backend(
    monkeypatch: pytest.MonkeyPatch,
    *,
    clock: Any | None = None,
) -> Any:
    """Load the guide's exact backend block without a separate plugin wheel."""
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
    constructor_kwargs = {} if clock is None else {"clock": clock}
    return namespace["MyBackend"](MySettings(), **constructor_kwargs)


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
    assert backend.contains("seen", b"item") is True
    assert backend.add("seen", b"other") is True
    backend.clear_set("seen")
    assert backend.contains("seen", b"item") is False
    assert backend.contains("seen", b"other") is False
    assert backend.add("seen", b"item") is True
    assert backend.remove("seen", b"item") is True
    assert backend.remove("seen", b"item") is False

    assert backend.delete("stored") is False
    backend.store("stored", b"payload")
    assert backend.exists("stored") is True
    assert backend.retrieve("stored") == b"payload"
    assert backend.ttl("stored") is None
    assert backend.delete("stored") is True
    assert backend.retrieve("stored") is None
    assert backend.delete("stored") is False

    backend.store("prefix:one", b"one")
    backend.store("prefix:two", b"two")
    backend.store("keep", b"three")
    backend.clear_storage("prefix:")
    assert backend.exists("prefix:one") is False
    assert backend.exists("prefix:two") is False
    assert backend.retrieve("keep") == b"three"
    backend.clear_storage()
    assert backend.exists("keep") is False


def test_documented_backend_example_honors_ttl_and_purges_on_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [100.0]
    backend = _load_documented_backend(monkeypatch, clock=lambda: now[0])

    backend.store("expiring", b"payload", ttl=3)
    assert backend.exists("expiring") is True
    assert backend.retrieve("expiring") == b"payload"
    assert backend.ttl("expiring") == 3

    now[0] = 102.2
    assert backend.ttl("expiring") == 1
    now[0] = 103.0
    assert backend.retrieve("expiring") is None
    assert backend.exists("expiring") is False
    assert backend.ttl("expiring") is None
    assert "expiring" not in backend._store
