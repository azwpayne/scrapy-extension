from __future__ import annotations

import pytest

from tests.integration.test_elasticsearch_outcome_safety_integration import (
    _ProxyHandler,
)


class _CapturingProxyHandler(_ProxyHandler):
    def __init__(self) -> None:
        self.captured: list[tuple[str, str]] = []

    def send_header(self, keyword: str, value: str) -> None:
        self.captured.append((keyword, value))


def test_forwarded_header_preserves_valid_name_and_sanitizes_value() -> None:
    handler = _CapturingProxyHandler()

    handler._send_forwarded_header("X-Trace", "safe\r\nX-Injected: yes")

    assert handler.captured == [("X-Trace", "safeX-Injected: yes")]


def test_forwarded_header_preserves_valid_duplicates() -> None:
    handler = _CapturingProxyHandler()

    handler._send_forwarded_header("Set-Cookie", "first=1")
    handler._send_forwarded_header("Set-Cookie", "second=2")

    assert handler.captured == [
        ("Set-Cookie", "first=1"),
        ("Set-Cookie", "second=2"),
    ]


@pytest.mark.parametrize(
    "name",
    [
        "",
        "X-Foo:\r\nX-Injected",
        "\r\nContent-Length",
        "Bad Name",
        "Bad\tName",
        "Bad\x00Name",
        "Non-ASCII-é",
    ],
)
def test_forwarded_header_rejects_invalid_names(name: str) -> None:
    handler = _CapturingProxyHandler()

    handler._send_forwarded_header(name, "value")

    assert handler.captured == []


@pytest.mark.parametrize("name", ["Content-Length", "Connection", "Keep-Alive"])
def test_forwarded_header_rejects_managed_names(name: str) -> None:
    handler = _CapturingProxyHandler()

    handler._send_forwarded_header(name, "value")

    assert handler.captured == []
