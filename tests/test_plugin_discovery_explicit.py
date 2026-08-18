"""Plugin discovery is explicit, lazy, and single-flight."""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from pathlib import Path

from scrapy_extension.backends import registry


def test_core_package_imports_do_not_enumerate_entry_points() -> None:
    root = Path(__file__).resolve().parents[1]
    script = r"""
import importlib.metadata

def hostile(*args, **kwargs):
    raise BaseException("entry-point enumeration during import")

importlib.metadata.entry_points = hostile
import scrapy_extension
import scrapy_extension.backends
import scrapy_extension.backends.connectors
print("imports-ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "imports-ok"


def test_reentrant_registration_callback_is_bounded_without_partial_cache(
    monkeypatch,
) -> None:
    registration_calls = 0

    class _EntryPoint:
        name = "recursive"

        @staticmethod
        def load():
            def register():
                nonlocal registration_calls
                registration_calls += 1
                registry.get_registry()
                raise AssertionError("recursive registry request unexpectedly returned")

            return register

    monkeypatch.setattr(
        registry.importlib.metadata,
        "entry_points",
        lambda *, group: [_EntryPoint()],
    )
    registry._reset_registry_cache()

    discovered = registry.get_registry()

    assert registration_calls == 1
    assert "redis" in discovered
    assert "recursive" not in discovered
    assert registry._registry_cache is not None
    assert "recursive" not in registry._registry_cache


def test_discovery_owner_state_resets_after_control_flow_failure(monkeypatch) -> None:
    calls = 0

    def entry_points(*, group: str):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyboardInterrupt("private discovery failure")
        return []

    monkeypatch.setattr(registry.importlib.metadata, "entry_points", entry_points)
    registry._reset_registry_cache()

    try:
        registry.get_registry()
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("control-flow failure unexpectedly suppressed")

    assert registry._registry_discovery_owner is None
    assert registry._registry_discovery_in_progress is False
    assert "redis" in registry.get_registry()
    assert calls == 2


def test_concurrent_registry_discovery_is_single_flight(monkeypatch) -> None:
    calls = 0
    calls_lock = threading.Lock()
    entered = threading.Event()
    release = threading.Event()

    def entry_points(*, group: str):
        nonlocal calls
        assert group == registry._ENTRY_POINT_GROUP
        with calls_lock:
            calls += 1
        entered.set()
        assert release.wait(5)
        return []

    monkeypatch.setattr(registry.importlib.metadata, "entry_points", entry_points)
    registry._reset_registry_cache()
    results: list[dict[str, registry.BackendDescriptor]] = []
    errors: list[BaseException] = []

    def discover() -> None:
        try:
            results.append(registry.get_registry())
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=discover) for _ in range(12)]
    for thread in threads:
        thread.start()
    assert entered.wait(5)
    time.sleep(0.05)
    assert calls == 1
    release.set()
    for thread in threads:
        thread.join(5)
        assert not thread.is_alive()

    assert errors == []
    assert len(results) == len(threads)
    assert all("redis" in result for result in results)
    assert calls == 1
