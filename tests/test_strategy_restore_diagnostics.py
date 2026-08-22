"""Regression coverage for non-fatal strategy restore diagnostics."""

from __future__ import annotations

import base64
import json
from typing import cast
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

import scrapy_extension.queue.strategies.delay as delay_module
import scrapy_extension.queue.strategies.ring_buffer as ring_buffer_module
import scrapy_extension.queue.strategies.round_robin as round_robin_module
import scrapy_extension.queue.strategies.time_wheel as time_wheel_module
from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.ring_buffer import RingBufferQueueStrategy
from scrapy_extension.queue.strategies.round_robin import RoundRobinQueueStrategy
from scrapy_extension.queue.strategies.time_wheel import TimeWheelQueueStrategy


@pytest.fixture(params=[RuntimeError, KeyboardInterrupt, SystemExit])
def diagnostic_error(request: pytest.FixtureRequest) -> BaseException:
    """Construct each error class custom logging handlers can raise."""
    error_type = cast(type[BaseException], request.param)
    return error_type("diagnostic handler failed")


def test_ring_buffer_restore_fallback_diagnostic_preserves_live_state(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    strategy = RingBufferQueueStrategy(MagicMock(), capacity=2)
    strategy.push("q", b"live")
    mocker.patch.object(
        ring_buffer_module.logger, "warning", side_effect=diagnostic_error
    )

    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(b"\xff")

    assert strategy.pop("q") == b"live"


def test_ring_buffer_restore_commit_diagnostic_preserves_recovery(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    strategy = RingBufferQueueStrategy(MagicMock(), capacity=2)
    state = json.dumps(
        {
            "version": 1,
            "strategy": "ring_buffer",
            "capacity": 2,
            "items": [base64.b64encode(b"recovered").decode()],
            "dropped": 0,
        }
    ).encode()
    mocker.patch.object(ring_buffer_module.logger, "info", side_effect=diagnostic_error)

    strategy.restore(state)

    assert strategy.pop("q") == b"recovered"


def test_delay_restore_fallback_diagnostic_preserves_live_state(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    strategy = DelayQueueStrategy(MagicMock(), clock=lambda: 100.0)
    strategy.push("q", b"live", delay=10.0)
    mocker.patch.object(delay_module.logger, "warning", side_effect=diagnostic_error)

    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(b"\xff")

    assert [entry[2] for entry in strategy._holding] == [b"live"]


def test_delay_restore_commit_diagnostic_preserves_recovery(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    strategy = DelayQueueStrategy(MagicMock(), clock=lambda: 100.0)
    state = json.dumps(
        {
            "version": 1,
            "strategy": "delay",
            "items": [
                {
                    "ready_at": 1.0,
                    "item_b64": base64.b64encode(b"recovered").decode(),
                    "priority": 0.0,
                }
            ],
        }
    ).encode()
    mocker.patch.object(delay_module.logger, "info", side_effect=diagnostic_error)

    strategy.restore(state)

    assert [entry[2] for entry in strategy._holding] == [b"recovered"]


def test_round_robin_restore_fallback_diagnostic_preserves_live_state(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    strategy = RoundRobinQueueStrategy(MagicMock())
    strategy.push("q", b"live", source="live")
    mocker.patch.object(
        round_robin_module.logger, "warning", side_effect=diagnostic_error
    )

    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(b"\xff")

    assert strategy.pop("q") == b"live"


def test_round_robin_restore_commit_diagnostic_preserves_recovery(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    strategy = RoundRobinQueueStrategy(MagicMock())
    state = json.dumps(
        {
            "version": 1,
            "strategy": "round_robin",
            "sources": [
                {
                    "source": "recovered",
                    "items": [base64.b64encode(b"recovered").decode()],
                }
            ],
        }
    ).encode()
    mocker.patch.object(round_robin_module.logger, "info", side_effect=diagnostic_error)

    strategy.restore(state)

    assert strategy.pop("q") == b"recovered"


def test_time_wheel_restore_fallback_diagnostic_preserves_live_state(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    strategy = TimeWheelQueueStrategy(MagicMock(), clock=lambda: 100.0)
    strategy.push("q", b"live", delay=10.0)
    mocker.patch.object(
        time_wheel_module.logger, "warning", side_effect=diagnostic_error
    )

    with pytest.raises(QueueError, match="snapshot restore failed"):
        strategy.restore(b"\xff")

    assert sum(len(slot) for slot in strategy._wheel) + len(strategy._overflow) == 1


def test_time_wheel_restore_commit_diagnostic_preserves_recovery(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    strategy = TimeWheelQueueStrategy(MagicMock(), clock=lambda: 100.0)
    state = json.dumps(
        {
            "version": 1,
            "strategy": "time_wheel",
            "slots_flat": [
                {
                    "ready_at": 1.0,
                    "item_b64": base64.b64encode(b"recovered").decode(),
                    "priority": 0.0,
                }
            ],
            "overflow": [],
        }
    ).encode()
    mocker.patch.object(time_wheel_module.logger, "info", side_effect=diagnostic_error)

    strategy.restore(state)

    assert sum(len(slot) for slot in strategy._wheel) + len(strategy._overflow) == 1
