"""SCRAPY_QUEUE_NAME_GENERATION knob for fan-out physical names (subsystem ②).

The fan-out strategies (``priority``, ``work_stealing``) default to the
versioned ``v2`` physical names. ``legacy_v1`` (old colon-delimited names) is
the only supported escape hatch and exists for a quiescent, one-time backlog
drain. These tests pin the settings seam: both knob values must reach the
strategies' physical-name selection through the scheduler bridge, invalid
values fail fast with a static ConfigurationError, and the default is ``v2``.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from scrapy.settings import Settings as ScrapySettings

import scrapy_extension.queue.strategies._names as _names
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.queue.strategies.factory import (
    QueueStrategyType,
    build_queue_strategy,
)
from scrapy_extension.schedule._queue_config import _QueueComponentConfig
from scrapy_extension.schedule.scheduler import BackendScheduler

pytestmark = pytest.mark.unit

#: The knob's rejection message must stay static (no interpolated value) so it
#: can be matched and safelisted without echoing operator input. The exception's
#: terminal redaction boundary still masks a supplied needle that happens to
#: appear inside the static text (e.g. ``"v1"`` inside ``legacy_v1``).
STATIC_KNOB_ERROR = "SCRAPY_QUEUE_NAME_GENERATION must be one of 'v2' or 'legacy_v1'."


def _expected_knob_error(invalid: Any) -> str:
    """Static message, with a colliding needle masked by the redaction boundary."""
    if type(invalid) is str and invalid and invalid in STATIC_KNOB_ERROR:
        return STATIC_KNOB_ERROR.replace(invalid, "***REDACTED***")
    return STATIC_KNOB_ERROR


def _strategy_config(strategy: str, extra: dict[str, Any] | None = None):
    """Parse the staged queue config exactly as the scheduler bridge does."""
    settings = ScrapySettings(
        {
            "SCRAPY_QUEUE_STRATEGY": strategy,
            **(extra or {}),
        }
    )
    config = _QueueComponentConfig.from_early_settings(settings)
    config = config.with_queue_key(settings, spider_name="spider-a")
    return config.with_strategy_settings(settings)


def _legacy_colon_manager() -> MagicMock:
    """A connection manager whose backend accepted colon-delimited names."""
    manager = MagicMock(name="ConnectionManager")
    manager.backend_type = "redis"
    return manager


def test_knob_defaults_to_v2() -> None:
    """Without the knob the fan-out generation stays the v2 default."""
    config = _strategy_config("priority")

    assert config.name_generation == "v2"


@pytest.mark.parametrize("generation", ["v2", "legacy_v1"])
def test_knob_accepts_both_supported_generations(generation: str) -> None:
    config = _strategy_config("priority", {"SCRAPY_QUEUE_NAME_GENERATION": generation})

    assert config.name_generation == generation


# Scrapy's Settings layer silently drops an explicit None (indistinguishable
# from unset), so None cannot reach the parser through the settings surface.
@pytest.mark.parametrize("invalid", ["v1", "v3", "", "V2", " v2 ", 7, True])
def test_knob_rejects_unsupported_values_with_static_message(
    invalid: Any,
) -> None:
    with pytest.raises(ConfigurationError) as raised:
        _strategy_config("priority", {"SCRAPY_QUEUE_NAME_GENERATION": invalid})

    assert str(raised.value) == _expected_knob_error(invalid)
    assert raised.value.setting_name == "SCRAPY_QUEUE_NAME_GENERATION"
    assert raised.value.setting_value == invalid


@pytest.mark.parametrize(
    ("generation", "expected_prefix_or_name"),
    [("v2", "scrapyext-v2-priority-"), ("legacy_v1", "jobs:p0")],
)
def test_knob_selects_priority_bucket_physical_names(
    generation: str, expected_prefix_or_name: str
) -> None:
    config = _strategy_config("priority", {"SCRAPY_QUEUE_NAME_GENERATION": generation})
    strategy = build_queue_strategy(
        QueueStrategyType.PRIORITY,
        _legacy_colon_manager(),
        priority_levels=3,
        name_generation=config.name_generation,
    )

    physical = strategy._bucket_queue("jobs", 0)
    if generation == "v2":
        assert physical.startswith(expected_prefix_or_name)
    else:
        assert physical == expected_prefix_or_name


@pytest.mark.parametrize(
    ("generation", "expected_prefix_or_name"),
    [("v2", "scrapyext-v2-worker-"), ("legacy_v1", "jobs:w1")],
)
def test_knob_selects_work_stealing_worker_physical_names(
    generation: str, expected_prefix_or_name: str
) -> None:
    config = _strategy_config(
        "work_stealing", {"SCRAPY_QUEUE_NAME_GENERATION": generation}
    )
    strategy = build_queue_strategy(
        QueueStrategyType.WORK_STEALING,
        _legacy_colon_manager(),
        worker_id="w1",
        name_generation=config.name_generation,
    )

    physical = strategy._own_queue("jobs")
    if generation == "v2":
        assert physical.startswith(expected_prefix_or_name)
    else:
        assert physical == expected_prefix_or_name


def _bridge_settings(extra: dict[str, Any]) -> ScrapySettings:
    return ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "redis",
            "SCRAPY_QUEUE_STRATEGY": "priority",
            **extra,
        }
    )


def _patched_bridge(mocker: MockerFixture) -> Any:
    lease = mocker.Mock()
    lease.manager = mocker.Mock()
    mocker.patch.object(ConnectionManager, "acquire_lease", return_value=lease)
    return mocker.patch(
        "scrapy_extension.queue.strategies.factory.build_queue_strategy",
        return_value=mocker.Mock(),
    )


@pytest.mark.parametrize(
    ("knob", "expected"),
    [
        (None, "v2"),
        ("v2", "v2"),
        ("legacy_v1", "legacy_v1"),
    ],
)
def test_scheduler_bridge_threads_knob_to_strategy_factory(
    mocker: MockerFixture, knob: str | None, expected: str
) -> None:
    """The scheduler's only bridge point must pass name_generation through."""
    build_strategy = _patched_bridge(mocker)
    extra = {} if knob is None else {"SCRAPY_QUEUE_NAME_GENERATION": knob}

    BackendScheduler.from_settings(_bridge_settings(extra))

    assert build_strategy.call_args.kwargs["name_generation"] == expected


def test_scheduler_bridge_rejects_unknown_generation_before_strategy_build(
    mocker: MockerFixture,
) -> None:
    build_strategy = _patched_bridge(mocker)

    with pytest.raises(ConfigurationError) as raised:
        BackendScheduler.from_settings(
            _bridge_settings({"SCRAPY_QUEUE_NAME_GENERATION": "v1"})
        )

    assert str(raised.value) == _expected_knob_error("v1")
    build_strategy.assert_not_called()


def _strict_name_manager() -> MagicMock:
    """A connection manager whose backend never hosted colon-delimited names."""
    manager = MagicMock(name="ConnectionManager")
    manager.backend_type = "sqs"
    return manager


def _reset_legacy_noop_latch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start from an un-latched one-shot warning regardless of test order."""
    monkeypatch.setattr(_names, "_legacy_generation_noop_warned", False, raising=False)


def test_legacy_v1_on_strict_backend_noops_with_one_shot_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """V4-2: legacy_v1 on a strict-name backend (e.g. SQS) must not be a
    silent no-op — a module-level latch emits one warning, then stays quiet,
    while every physical name stays on the v2 digest."""
    _reset_legacy_noop_latch(monkeypatch)
    strategy = build_queue_strategy(
        QueueStrategyType.WORK_STEALING,
        _strict_name_manager(),
        worker_id="w1",
        name_generation="legacy_v1",
    )

    with caplog.at_level(logging.WARNING, logger=_names.__name__):
        first = strategy._own_queue("jobs")
        second = strategy._own_queue("jobs")

    assert first.startswith("scrapyext-v2-worker-")
    assert second == first
    warnings = [
        record for record in caplog.records if "legacy_v1" in record.getMessage()
    ]
    assert len(warnings) == 1
    assert "sqs" in warnings[0].getMessage()


def test_v2_on_strict_backend_does_not_warn(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The default generation is the only reachable one on strict backends,
    so it must not trip the legacy no-op advisory."""
    _reset_legacy_noop_latch(monkeypatch)
    strategy = build_queue_strategy(
        QueueStrategyType.WORK_STEALING,
        _strict_name_manager(),
        worker_id="w1",
        name_generation="v2",
    )

    with caplog.at_level(logging.WARNING, logger=_names.__name__):
        assert strategy._own_queue("jobs").startswith("scrapyext-v2-worker-")

    assert not [
        record for record in caplog.records if "legacy_v1" in record.getMessage()
    ]


def test_legacy_v1_on_legacy_colon_backend_does_not_warn(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """legacy_v1 is effective on legacy-colon backends, so the drain knob
    there must stay warning-free."""
    _reset_legacy_noop_latch(monkeypatch)
    strategy = build_queue_strategy(
        QueueStrategyType.WORK_STEALING,
        _legacy_colon_manager(),
        worker_id="w1",
        name_generation="legacy_v1",
    )

    with caplog.at_level(logging.WARNING, logger=_names.__name__):
        assert strategy._own_queue("jobs") == "jobs:w1"

    assert not [
        record for record in caplog.records if "legacy_v1" in record.getMessage()
    ]
