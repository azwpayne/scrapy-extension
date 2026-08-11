"""Regression tests for scheduler configuration value parsing."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from pytest_mock import MockerFixture
from scrapy.settings import Settings as ScrapySettings

from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.schedule.scheduler import BackendScheduler

pytestmark = pytest.mark.unit


def test_queue_component_config_parses_queue_values_immutably() -> None:
    """The staged snapshot retains every value passed to the queue seam."""
    from scrapy_extension.schedule.scheduler import _QueueComponentConfig

    settings = ScrapySettings(
        {
            "SCRAPY_QUEUE_STRATEGY": "delay",
            "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY": "drop_oldest",
            "SCRAPY_QUEUE_KEY": "queue-{spider}",
            "SCRAPY_QUEUE_DELAY_DEFAULT": "1.5",
            "SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL": "0.25",
            "SCRAPY_QUEUE_DELAY_MAX_HELD": "25",
            "SCRAPY_QUEUE_PRIORITY_LEVELS": "4",
            "SCRAPY_QUEUE_TIME_WHEEL_SIZE": "90",
            "SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND": "2.0",
            "SCRAPY_QUEUE_STEAL_TIMEOUT": "0.75",
            "SCRAPY_QUEUE_RING_BUFFER_CAPACITY": "2048",
            "SCRAPY_QUEUE_WORKER_ID": " worker-a ",
            "SCRAPY_QUEUE_PEER_IDS": " worker-b, ,worker-c ",
            "SCRAPY_BACKPRESSURE_PAUSE_AT": "10",
            "SCRAPY_BACKPRESSURE_RESUME_AT": "5",
            "SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY": "25",
            "SCRAPY_QUEUE_MAX_ITEM_BYTES": "4096",
            "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD": "200",
            "SCRAPY_MONITOR_POP_RATE_WINDOW_S": "30.0",
            "SCRAPY_QUEUE_SNAPSHOT_OWNER": "snapshot-a",
        }
    )

    config = _QueueComponentConfig.from_early_settings(settings)
    config = config.with_queue_key(settings, spider_name="spider-a")
    config = config.with_strategy_settings(settings)
    config = config.with_runtime_settings(settings)

    assert config.strategy_type is not None
    assert config.strategy_type.value == "delay"
    assert config.ring_buffer_full_policy == "drop_oldest"
    assert config.queue_key == "queue-spider-a"
    assert config.default_delay == 1.5
    assert config.min_interval == 0.25
    assert config.delay_max_held == 25
    assert config.priority_levels == 4
    assert config.wheel_size == 90
    assert config.ticks_per_second == 2.0
    assert config.steal_timeout == 0.75
    assert config.capacity == 2048
    assert config.worker_id == "worker-a"
    assert config.peer_ids == ("worker-b", "worker-c")
    assert config.backpressure_pause_at == 10
    assert config.backpressure_resume_at == 5
    assert config.queue_depth_sample_every == 25
    assert config.queue_max_item_bytes == 4096
    assert config.monitor_backpressure_threshold == 200
    assert config.monitor_pop_rate_window_s == 30.0
    assert config.queue_snapshot_owner == "snapshot-a"
    with pytest.raises(FrozenInstanceError):
        config.queue_key = "queue-other"  # type: ignore[misc]


def test_strategy_is_read_once_before_ring_buffer_safety_gate(
    mocker: MockerFixture,
) -> None:
    """A mutable settings source cannot change strategy after safety validation."""
    settings = mocker.Mock()
    strategy_values = iter(("passthrough", "ring_buffer"))

    def get(key: str, default: object = None) -> object:
        if key == "SCRAPY_QUEUE_STRATEGY":
            return next(strategy_values)
        if key == "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY":
            return "block"
        if key == "SCRAPY_BACKEND_TYPE":
            return "redis"
        return default

    settings.get.side_effect = get
    settings.getdict.return_value = {}
    manager = mocker.Mock()
    mocker.patch.object(
        ConnectionManager,
        "get_manager",
        return_value=manager,
    )
    build_strategy = mocker.patch(
        "scrapy_extension.queue.strategies.factory.build_queue_strategy",
        return_value=mocker.Mock(),
    )

    BackendScheduler.from_settings(settings)

    assert build_strategy.call_args.args[0].value == "passthrough"


@pytest.mark.parametrize(
    ("raw_peer_ids", "expected"),
    [
        ("worker-b,worker-c", ("worker-b", "worker-c")),
        (["worker-b", "worker-c"], ("worker-b", "worker-c")),
        (("worker-b", "worker-c"), ("worker-b", "worker-c")),
    ],
)
def test_queue_peer_ids_accept_string_list_and_tuple(
    mocker: MockerFixture,
    raw_peer_ids: str | list[str] | tuple[str, ...],
    expected: tuple[str, ...],
) -> None:
    manager = mocker.Mock()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    build_strategy = mocker.patch(
        "scrapy_extension.queue.strategies.factory.build_queue_strategy",
        return_value=mocker.Mock(),
    )
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "redis",
            "SCRAPY_QUEUE_PEER_IDS": raw_peer_ids,
        }
    )

    BackendScheduler.from_settings(settings)

    assert build_strategy.call_args.kwargs["peer_ids"] == expected


class _SingleSlotAckBackend:
    requires_ack = True
    supports_concurrent_ack = False


@pytest.mark.parametrize("raw_value", ["false", "0"])
def test_false_string_does_not_bypass_ack_concurrency_gate(
    mocker: MockerFixture,
    raw_value: str,
) -> None:
    mocker.patch(
        "scrapy_extension.backends.connectors._load_object",
        return_value=_SingleSlotAckBackend,
    )
    settings = ScrapySettings(
        {
            "CONCURRENT_REQUESTS": 8,
            "SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS": raw_value,
        }
    )

    with pytest.raises(ConfigurationError):
        BackendScheduler._enforce_ack_concurrency_gate(settings, "sqs")


@pytest.mark.parametrize("raw_value", ["true", "1"])
def test_true_string_bypasses_ack_concurrency_gate(
    mocker: MockerFixture, raw_value: str
) -> None:
    mocker.patch(
        "scrapy_extension.backends.connectors._load_object",
        return_value=_SingleSlotAckBackend,
    )
    settings = ScrapySettings(
        {
            "CONCURRENT_REQUESTS": 8,
            "SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS": raw_value,
        }
    )

    BackendScheduler._enforce_ack_concurrency_gate(settings, "sqs")


def test_unset_ack_opt_out_defaults_to_false(mocker: MockerFixture) -> None:
    mocker.patch(
        "scrapy_extension.backends.connectors._load_object",
        return_value=_SingleSlotAckBackend,
    )
    settings = ScrapySettings({"CONCURRENT_REQUESTS": 8})

    with pytest.raises(ConfigurationError):
        BackendScheduler._enforce_ack_concurrency_gate(settings, "sqs")


@pytest.mark.parametrize(
    "diagnostic_error",
    [
        RuntimeError("logger unavailable"),
        KeyboardInterrupt("logger interrupted"),
        SystemExit("logger exited"),
    ],
)
def test_ack_bypass_warning_interruption_preserves_valid_configuration(
    mocker: MockerFixture,
    diagnostic_error: BaseException,
) -> None:
    """A pure compatibility warning cannot abort scheduler construction."""
    manager = mocker.Mock(name="ConnectionManager")
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    mocker.patch(
        "scrapy_extension.schedule.scheduler.logger.warning",
        side_effect=diagnostic_error,
    )
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "kafka",
            "SCRAPY_QUEUE_STRATEGY": "ring_buffer",
        }
    )

    scheduler = BackendScheduler.from_settings(settings)

    assert scheduler._queue_strategy is not None
    manager.close.assert_not_called()
    scheduler.close("test-finished")


@pytest.mark.parametrize("control_error", [KeyboardInterrupt, SystemExit])
def test_ack_bypass_descriptor_control_interruption_still_propagates(
    mocker: MockerFixture,
    control_error: type[BaseException],
) -> None:
    """Only the logger is advisory; descriptor resolution remains direct control."""
    mocker.patch(
        "scrapy_extension.backends.connectors._load_object",
        side_effect=control_error("descriptor interrupted"),
    )
    warning = mocker.patch("scrapy_extension.schedule.scheduler.logger.warning")

    with pytest.raises(control_error, match="descriptor interrupted"):
        BackendScheduler._warn_strategy_mq_ack_bypass(object(), "kafka")

    warning.assert_not_called()


def test_snapshot_owner_defaulted_from_key_unsafe_worker_id_names_worker_id() -> None:
    """A key-unsafe worker_id that defaults into snapshot_owner must attribute the
    ConfigurationError to SCRAPY_QUEUE_WORKER_ID (the setting the operator configured),
    not the unset SCRAPY_QUEUE_SNAPSHOT_OWNER (which would send the operator to debug a
    setting they never touched)."""
    from scrapy_extension.schedule.scheduler import _QueueComponentConfig

    settings = ScrapySettings(
        {
            # delay ignores worker_id, so 'worker/1' survives with_strategy_settings
            # unvalidated (only .strip(), no _validate_key_name).
            "SCRAPY_QUEUE_STRATEGY": "delay",
            "SCRAPY_QUEUE_WORKER_ID": "worker/1",  # '/' is rejected by KEY_NAME_PATTERN
            # SCRAPY_QUEUE_SNAPSHOT_OWNER intentionally unset -> defaults to worker_id
        }
    )
    config = _QueueComponentConfig.from_early_settings(settings)
    config = config.with_queue_key(settings, spider_name=None)
    config = config.with_strategy_settings(settings)
    with pytest.raises(ConfigurationError) as exc_info:
        config.with_runtime_settings(settings)
    assert exc_info.value.setting_name == "SCRAPY_QUEUE_WORKER_ID"
    assert exc_info.value.setting_value == "worker/1"


def test_workstealing_key_unsafe_worker_id_names_worker_id(
    mocker: MockerFixture,
) -> None:
    """R88: a key-unsafe SCRAPY_QUEUE_WORKER_ID under work_stealing must attribute the
    ConfigurationError to SCRAPY_QUEUE_WORKER_ID (the setting the operator configured), not
    the unrelated SCRAPY_QUEUE_PEER_IDS. Unfixed sibling of the snapshot_owner attribution
    fix (R80 / fe72f30) -- R80's Directive demands source-setting attribution, which the
    work_stealing constructor catch block violated."""
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "redis",
            "SCRAPY_QUEUE_KEY": "scheduler:queue",
            "SCRAPY_QUEUE_STRATEGY": "work_stealing",
            "SCRAPY_QUEUE_WORKER_ID": "worker/1",  # '/' is rejected by KEY_NAME_PATTERN
        }
    )
    with pytest.raises(ConfigurationError) as exc_info:
        BackendScheduler.from_settings(settings)
    assert exc_info.value.setting_name == "SCRAPY_QUEUE_WORKER_ID"
    assert exc_info.value.setting_value == "worker/1"


def test_workstealing_key_unsafe_peer_id_still_names_peer_ids(
    mocker: MockerFixture,
) -> None:
    """R88 hardening: when a key-unsafe PEER_ID (not worker_id) is the offender under
    work_stealing, attribution must stay on SCRAPY_QUEUE_PEER_IDS -- the worker_id
    re-check must not over-redirect when worker_id itself is valid."""
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())
    settings = ScrapySettings(
        {
            "SCRAPY_BACKEND_TYPE": "redis",
            "SCRAPY_QUEUE_KEY": "scheduler:queue",
            "SCRAPY_QUEUE_STRATEGY": "work_stealing",
            "SCRAPY_QUEUE_WORKER_ID": "worker-a",  # valid
            "SCRAPY_QUEUE_PEER_IDS": "peer/1",  # '/' rejected by KEY_NAME_PATTERN
        }
    )
    with pytest.raises(ConfigurationError) as exc_info:
        BackendScheduler.from_settings(settings)
    assert exc_info.value.setting_name == "SCRAPY_QUEUE_PEER_IDS"
