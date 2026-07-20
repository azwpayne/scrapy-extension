"""Real Scrapy reactor/engine contracts for ``BackendScheduler``.

The probe runs in a child interpreter because Twisted reactors are not
restartable.  It talks only to a loopback HTTP server and needs no external
backend service.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def engine_probe() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    pythonpath = [str(root / "src"), str(root)]
    if env.get("PYTHONPATH"):
        pythonpath.append(env["PYTHONPATH"])
    env.update(
        {
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
            "PYTHONPATH": os.pathsep.join(pythonpath),
        }
    )
    completed = subprocess.run(
        [sys.executable, str(root / "tests" / "_scrapy_engine_probe.py")],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        pytest.fail(
            "Scrapy engine probe failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    prefix = "ENGINE_PROBE_RESULT="
    result_line = next(
        (line for line in completed.stdout.splitlines() if line.startswith(prefix)),
        None,
    )
    if result_line is None:
        pytest.fail(
            "Scrapy engine probe emitted no result\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return cast("dict[str, Any]", json.loads(result_line.removeprefix(prefix)))


def _events(
    result: dict[str, Any], kind: str, *, url: str | None = None
) -> list[dict[str, Any]]:
    return [
        event
        for event in result["events"]
        if event["kind"] == kind and (url is None or event.get("url") == url)
    ]


def _event_indexes(
    result: dict[str, Any], kind: str, *, url: str | None = None
) -> Iterator[int]:
    for index, event in enumerate(result["events"]):
        if event["kind"] == kind and (url is None or event.get("url") == url):
            yield index


def _only_event(result: dict[str, Any], kind: str, *, url: str) -> dict[str, Any]:
    matches = _events(result, kind, url=url)
    assert len(matches) == 1, matches
    return matches[0]


def _terminal_events(result: dict[str, Any], token: str) -> list[dict[str, Any]]:
    return [
        event
        for event in result["events"]
        if event["kind"] in {"ack", "nack"} and event.get("token") == token
    ]


def test_success_response_acks_once_before_callback(
    engine_probe: dict[str, Any],
) -> None:
    popped = _only_event(engine_probe, "pop", url="/ok")
    assert _terminal_events(engine_probe, popped["token"]) == [
        {"kind": "ack", "token": popped["token"], "url": "/ok"}
    ]
    assert next(_event_indexes(engine_probe, "response_received", url="/ok")) < next(
        _event_indexes(engine_probe, "callback", url="/ok")
    )
    assert next(_event_indexes(engine_probe, "ack", url="/ok")) < next(
        _event_indexes(engine_probe, "callback", url="/ok")
    )
    callback = _only_event(engine_probe, "callback", url="/ok")
    assert callback["ack_token_present"] is False


@pytest.mark.parametrize(
    ("initial_url", "final_url", "expected_attempts"),
    [
        ("/redirect", "/redirect-final", {"/redirect": 1, "/redirect-final": 1}),
        ("/retry", "/retry", {"/retry": 2}),
    ],
)
def test_replacement_request_acks_each_delivery_exactly_once(
    engine_probe: dict[str, Any],
    initial_url: str,
    final_url: str,
    expected_attempts: dict[str, int],
) -> None:
    pops = _events(engine_probe, "pop", url=initial_url)
    if initial_url == final_url:
        assert len(pops) == 2
        first_pop, final_pop = pops
    else:
        assert len(pops) == 1
        first_pop = pops[0]
        final_pop = _only_event(engine_probe, "pop", url=final_url)

    assert _terminal_events(engine_probe, first_pop["token"]) == [
        {"kind": "ack", "token": first_pop["token"], "url": initial_url}
    ]
    assert _terminal_events(engine_probe, final_pop["token"]) == [
        {"kind": "ack", "token": final_pop["token"], "url": final_url}
    ]
    assert not _events(engine_probe, "nack", url=initial_url)

    first_pop_index = next(
        index
        for index in _event_indexes(engine_probe, "pop", url=initial_url)
        if engine_probe["events"][index]["token"] == first_pop["token"]
    )
    replacement_push_index = next(
        index
        for index in _event_indexes(engine_probe, "push", url=final_url)
        if index > first_pop_index
    )
    first_ack_index = next(
        index
        for index in _event_indexes(engine_probe, "ack", url=initial_url)
        if engine_probe["events"][index]["token"] == first_pop["token"]
    )
    final_pop_index = next(
        index
        for index in _event_indexes(engine_probe, "pop", url=final_url)
        if engine_probe["events"][index]["token"] == final_pop["token"]
    )
    assert first_pop_index < replacement_push_index < first_ack_index < final_pop_index
    for path, attempts in expected_attempts.items():
        assert engine_probe["http_attempts"][path] == attempts


def test_download_failure_nacks_once(engine_probe: dict[str, Any]) -> None:
    popped = _only_event(engine_probe, "pop", url="/download-failure")
    assert _terminal_events(engine_probe, popped["token"]) == [
        {"kind": "nack", "token": popped["token"], "url": "/download-failure"}
    ]
    assert (
        _only_event(engine_probe, "download_errback", url="/download-failure")[
            "ack_token_present"
        ]
        is True
    )
    _only_event(engine_probe, "spider_error", url="/download-failure")


def test_handled_download_failure_acks(engine_probe: dict[str, Any]) -> None:
    url = "/download-failure-handled"
    popped = _only_event(engine_probe, "pop", url=url)
    assert _terminal_events(engine_probe, popped["token"]) == [
        {"kind": "ack", "token": popped["token"], "url": url}
    ]
    event = _only_event(engine_probe, "download_errback_handled", url=url)
    assert event["ack_token_present"] is True


def test_unhandled_download_failure_nacks(engine_probe: dict[str, Any]) -> None:
    url = "/download-failure-unhandled"
    popped = _only_event(engine_probe, "pop", url=url)
    assert _terminal_events(engine_probe, popped["token"]) == [
        {"kind": "nack", "token": popped["token"], "url": url}
    ]


def test_duplicate_redirect_replacement_acks_original_delivery(
    engine_probe: dict[str, Any],
) -> None:
    popped = _only_event(engine_probe, "pop", url="/redirect-duplicate")
    assert _terminal_events(engine_probe, popped["token"]) == [
        {
            "kind": "ack",
            "token": popped["token"],
            "url": "/redirect-duplicate",
        }
    ]
    assert len(_events(engine_probe, "request_dropped", url="/already-seen")) == 1


def test_response_then_callback_error_has_one_terminal_transition(
    engine_probe: dict[str, Any],
) -> None:
    popped = _only_event(engine_probe, "pop", url="/callback-error")
    assert _terminal_events(engine_probe, popped["token"]) == [
        {"kind": "ack", "token": popped["token"], "url": "/callback-error"}
    ]
    response_index = next(
        _event_indexes(engine_probe, "response_received", url="/callback-error")
    )
    callback_index = next(
        _event_indexes(engine_probe, "callback", url="/callback-error")
    )
    error_index = next(
        _event_indexes(engine_probe, "spider_error", url="/callback-error")
    )
    assert response_index < callback_index < error_index
    assert not _events(engine_probe, "nack", url="/callback-error")


def test_failed_push_rolls_back_dedup_reservation(
    engine_probe: dict[str, Any],
) -> None:
    _only_event(engine_probe, "push_rejected", url="/dedup-push-failure")
    _only_event(engine_probe, "push", url="/dedup-push-failure")
    _only_event(engine_probe, "callback", url="/dedup-push-failure")
    assert engine_probe["http_attempts"]["/dedup-push-failure"] == 1
    assert len(_events(engine_probe, "request_dropped", url="/dedup-push-failure")) == 1


def test_engine_owns_dupefilter_lifecycle_once(
    engine_probe: dict[str, Any],
) -> None:
    opened = _events(engine_probe, "dupefilter_open")
    closed = _events(engine_probe, "dupefilter_close")
    assert opened == [{"kind": "dupefilter_open", "spider": "engine_probe"}]
    assert len(closed) == 1
    assert closed[0]["reason"] == "finished"
    assert next(_event_indexes(engine_probe, "dupefilter_open")) < next(
        _event_indexes(engine_probe, "dupefilter_close")
    )
    assert _events(engine_probe, "manager_connect") == [
        {"kind": "manager_connect", "role": "queue"}
    ]
    assert sorted(
        event["role"] for event in _events(engine_probe, "manager_close")
    ) == [
        "dupefilter",
        "queue",
    ]
    assert engine_probe["in_flight_tokens"] == []
    assert _events(engine_probe, "invalid_terminal") == []
