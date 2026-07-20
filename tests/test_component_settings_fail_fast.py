"""Fail-fast contracts for non-backend Scrapy component settings."""

from __future__ import annotations

from typing import Any

import pytest
from pytest_mock import MockerFixture
from scrapy.settings import Settings

from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.utils._config import parse_bool_setting

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
  ("raw", "expected"),
  [
    (True, True),
    (False, False),
    (1, True),
    (0, False),
    ("TRUE", True),
    ("false", False),
    ("1", True),
    ("0", False),
  ],
)
def test_strict_bool_parser_accepts_only_documented_spellings(
  raw: object,
  expected: bool,
) -> None:
  assert parse_bool_setting(raw, "FLAG") is expected


@pytest.mark.parametrize("raw", [2, -1, "yes", " true ", None, 1.0])
def test_strict_bool_parser_rejects_ambiguous_values(raw: object) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    parse_bool_setting(raw, "FLAG")

  assert exc_info.value.setting_name == "FLAG"


@pytest.fixture
def component_manager(mocker: MockerFixture) -> Any:
  manager = mocker.Mock()
  manager.backend_type = "redis"
  mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
  return manager


@pytest.mark.parametrize(
  ("setting_name", "setting_value", "strategy", "extra"),
  [
    ("SCRAPY_QUEUE_DELAY_DEFAULT", "invalid", "delay", {}),
    ("SCRAPY_QUEUE_DELAY_DEFAULT", float("nan"), "delay", {}),
    ("SCRAPY_QUEUE_DELAY_DEFAULT", float("inf"), "delay", {}),
    ("SCRAPY_QUEUE_DELAY_DEFAULT", -0.1, "delay", {}),
    ("SCRAPY_QUEUE_DELAY_DEFAULT", True, "delay", {}),
    ("SCRAPY_QUEUE_DELAY_MAX_HELD", "invalid", "delay", {}),
    ("SCRAPY_QUEUE_DELAY_MAX_HELD", True, "delay", {}),
    ("SCRAPY_QUEUE_DELAY_MAX_HELD", 1.5, "delay", {}),
    ("SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL", "invalid", "throttle", {}),
    ("SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL", float("nan"), "throttle", {}),
    ("SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL", float("inf"), "throttle", {}),
    ("SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL", -0.1, "throttle", {}),
    ("SCRAPY_QUEUE_PRIORITY_LEVELS", "invalid", "priority", {}),
    ("SCRAPY_QUEUE_PRIORITY_LEVELS", 0, "priority", {}),
    ("SCRAPY_QUEUE_PRIORITY_LEVELS", -1, "priority", {}),
    ("SCRAPY_QUEUE_PRIORITY_LEVELS", 257, "priority", {}),
    ("SCRAPY_QUEUE_PRIORITY_LEVELS", True, "priority", {}),
    ("SCRAPY_QUEUE_PRIORITY_LEVELS", 3.5, "priority", {}),
    ("SCRAPY_QUEUE_TIME_WHEEL_SIZE", "invalid", "time_wheel", {}),
    ("SCRAPY_QUEUE_TIME_WHEEL_SIZE", 0, "time_wheel", {}),
    ("SCRAPY_QUEUE_TIME_WHEEL_SIZE", -1, "time_wheel", {}),
    ("SCRAPY_QUEUE_TIME_WHEEL_SIZE", 100_001, "time_wheel", {}),
    (
      "SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND",
      "invalid",
      "time_wheel",
      {},
    ),
    (
      "SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND",
      float("nan"),
      "time_wheel",
      {},
    ),
    (
      "SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND",
      float("inf"),
      "time_wheel",
      {},
    ),
    ("SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND", 0, "time_wheel", {}),
    ("SCRAPY_QUEUE_TIME_WHEEL_TICKS_PER_SECOND", -1, "time_wheel", {}),
    ("SCRAPY_QUEUE_STEAL_TIMEOUT", "invalid", "work_stealing", {}),
    ("SCRAPY_QUEUE_STEAL_TIMEOUT", float("nan"), "work_stealing", {}),
    ("SCRAPY_QUEUE_STEAL_TIMEOUT", float("inf"), "work_stealing", {}),
    ("SCRAPY_QUEUE_STEAL_TIMEOUT", -0.1, "work_stealing", {}),
    ("SCRAPY_QUEUE_RING_BUFFER_CAPACITY", "invalid", "ring_buffer", {}),
    ("SCRAPY_QUEUE_RING_BUFFER_CAPACITY", 0, "ring_buffer", {}),
    ("SCRAPY_QUEUE_RING_BUFFER_CAPACITY", -1, "ring_buffer", {}),
    (
      "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY",
      "invalid",
      "ring_buffer",
      {},
    ),
    ("SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY", "invalid", "passthrough", {}),
    ("SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY", 0, "passthrough", {}),
    ("SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY", -1, "passthrough", {}),
    ("SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY", True, "passthrough", {}),
    ("SCRAPY_QUEUE_MAX_ITEM_BYTES", "invalid", "passthrough", {}),
    ("SCRAPY_QUEUE_MAX_ITEM_BYTES", 0, "passthrough", {}),
    ("SCRAPY_QUEUE_MAX_ITEM_BYTES", -1, "passthrough", {}),
    ("SCRAPY_QUEUE_MAX_ITEM_BYTES", True, "passthrough", {}),
    (
      "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD",
      "invalid",
      "passthrough",
      {},
    ),
    ("SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD", -1, "passthrough", {}),
    ("SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD", True, "passthrough", {}),
    ("SCRAPY_MONITOR_POP_RATE_WINDOW_S", "invalid", "passthrough", {}),
    (
      "SCRAPY_MONITOR_POP_RATE_WINDOW_S",
      float("nan"),
      "passthrough",
      {},
    ),
    (
      "SCRAPY_MONITOR_POP_RATE_WINDOW_S",
      float("inf"),
      "passthrough",
      {},
    ),
    ("SCRAPY_MONITOR_POP_RATE_WINDOW_S", 0, "passthrough", {}),
    ("SCRAPY_MONITOR_POP_RATE_WINDOW_S", -1, "passthrough", {}),
    ("SCRAPY_MONITOR_POP_RATE_WINDOW_S", True, "passthrough", {}),
    ("SCRAPY_BACKPRESSURE_PAUSE_AT", "invalid", "passthrough", {}),
    ("SCRAPY_BACKPRESSURE_PAUSE_AT", -1, "passthrough", {}),
    ("SCRAPY_BACKPRESSURE_PAUSE_AT", True, "passthrough", {}),
    (
      "SCRAPY_BACKPRESSURE_RESUME_AT",
      "invalid",
      "passthrough",
      {"SCRAPY_BACKPRESSURE_PAUSE_AT": 10},
    ),
    (
      "SCRAPY_BACKPRESSURE_RESUME_AT",
      -1,
      "passthrough",
      {"SCRAPY_BACKPRESSURE_PAUSE_AT": 10},
    ),
    (
      "SCRAPY_BACKPRESSURE_RESUME_AT",
      11,
      "passthrough",
      {"SCRAPY_BACKPRESSURE_PAUSE_AT": 10},
    ),
  ],
)
def test_scheduler_numeric_settings_fail_fast_as_configuration_error(
  component_manager: Any,
  setting_name: str,
  setting_value: object,
  strategy: str,
  extra: dict[str, object],
) -> None:
  values = {
    "SCRAPY_BACKEND_TYPE": "redis",
    "SCRAPY_QUEUE_STRATEGY": strategy,
    setting_name: setting_value,
    **extra,
  }

  with pytest.raises(ConfigurationError) as exc_info:
    BackendScheduler.from_settings(Settings(values))

  assert exc_info.value.setting_name == setting_name


@pytest.mark.parametrize(
  ("setting_name", "setting_value", "strategy"),
  [
    ("SCRAPY_QUEUE_WORKER_ID", 7, "work_stealing"),
    ("SCRAPY_QUEUE_PEER_IDS", ["worker-b", 7], "work_stealing"),
    ("SCRAPY_QUEUE_PEER_IDS", ["worker-b", None], "work_stealing"),
  ],
)
def test_scheduler_strategy_identifiers_reject_invalid_types_at_factory_boundary(
  component_manager: Any,
  setting_name: str,
  setting_value: object,
  strategy: str,
) -> None:
  settings = Settings(
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_QUEUE_STRATEGY": strategy,
      setting_name: setting_value,
    }
  )

  with pytest.raises(ConfigurationError) as exc_info:
    BackendScheduler.from_settings(settings)

  assert exc_info.value.setting_name == setting_name


def test_scheduler_preserves_valid_zero_boundaries(component_manager: Any) -> None:
  scheduler = BackendScheduler.from_settings(
    Settings(
      {
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_QUEUE_DELAY_DEFAULT": 0,
        "SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL": 0,
        "SCRAPY_QUEUE_STEAL_TIMEOUT": 0,
        "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD": 0,
        "SCRAPY_BACKPRESSURE_PAUSE_AT": 0,
        "SCRAPY_BACKPRESSURE_RESUME_AT": 0,
      }
    )
  )

  assert scheduler._pause_at == 0
  assert scheduler._resume_at == 0
  assert scheduler._monitor_backpressure_threshold == 0


@pytest.mark.parametrize("max_held", [0, -1])
def test_scheduler_preserves_documented_delay_warning_opt_out(
  component_manager: Any,
  max_held: int,
) -> None:
  scheduler = BackendScheduler.from_settings(
    Settings(
      {
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_QUEUE_STRATEGY": "delay",
        "SCRAPY_QUEUE_DELAY_MAX_HELD": max_held,
      }
    )
  )

  assert scheduler._queue_strategy._max_held == max_held


def test_scheduler_accepts_numeric_environment_strings(
  component_manager: Any,
) -> None:
  scheduler = BackendScheduler.from_settings(
    Settings(
      {
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY": "20",
        "SCRAPY_QUEUE_MAX_ITEM_BYTES": "2048",
        "SCRAPY_QUEUE_DELAY_MAX_HELD": "500",
        "SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD": "10",
        "SCRAPY_MONITOR_POP_RATE_WINDOW_S": "30.5",
        "SCRAPY_BACKPRESSURE_PAUSE_AT": "8",
        "SCRAPY_BACKPRESSURE_RESUME_AT": "4",
      }
    )
  )

  assert scheduler._queue_depth_sample_every == 20
  assert scheduler._queue_max_item_bytes == 2048
  assert scheduler._monitor_pop_rate_window_s == pytest.approx(30.5)
  assert scheduler._pause_at == 8
  assert scheduler._resume_at == 4


@pytest.mark.parametrize(
  ("setting_name", "setting_value"),
  [
    ("SCRAPY_QUEUE_KEY", "invalid queue"),
    ("SCRAPY_QUEUE_SNAPSHOT_OWNER", "invalid owner"),
    ("SCRAPY_QUEUE_SNAPSHOT_OWNER", ""),
    ("SCRAPY_QUEUE_SNAPSHOT_OWNER", 0),
  ],
)
def test_scheduler_names_fail_fast_before_open(
  component_manager: Any,
  setting_name: str,
  setting_value: object,
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    BackendScheduler.from_settings(
      Settings(
        {
          "SCRAPY_BACKEND_TYPE": "redis",
          setting_name: setting_value,
        }
      )
    )

  assert exc_info.value.setting_name == setting_name


class _SingleSlotAckBackend:
  requires_ack = True
  supports_concurrent_ack = False


@pytest.mark.parametrize(
  ("setting_name", "setting_value"),
  [
    ("CONCURRENT_REQUESTS", "invalid"),
    ("CONCURRENT_REQUESTS", 0),
    ("CONCURRENT_REQUESTS", True),
    ("SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS", "invalid"),
  ],
)
def test_ack_gate_settings_fail_fast_as_configuration_error(
  mocker: MockerFixture,
  setting_name: str,
  setting_value: object,
) -> None:
  mocker.patch(
    "scrapy_extension.backends.connectors._load_object",
    return_value=_SingleSlotAckBackend,
  )
  values: dict[str, object] = {
    "CONCURRENT_REQUESTS": 8,
    "SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS": False,
    setting_name: setting_value,
  }

  with pytest.raises(ConfigurationError) as exc_info:
    BackendScheduler._enforce_ack_concurrency_gate(Settings(values), "sqs")

  assert exc_info.value.setting_name == setting_name


@pytest.mark.parametrize(
  ("setting_name", "setting_value", "extra"),
  [
    ("SCRAPY_STORAGE_BUFFER_MAX_AGE_S", "invalid", {}),
    ("SCRAPY_STORAGE_BUFFER_MAX_AGE_S", float("nan"), {}),
    ("SCRAPY_STORAGE_BUFFER_MAX_AGE_S", float("inf"), {}),
    ("SCRAPY_STORAGE_BUFFER_MAX_AGE_S", 0, {}),
    ("SCRAPY_STORAGE_BUFFER_MAX_AGE_S", -1, {}),
    ("SCRAPY_STORAGE_BUFFER_MAX_AGE_S", True, {}),
    ("SCRAPY_PIPELINE_MAX_STORAGE_ERRORS", "invalid", {}),
    ("SCRAPY_PIPELINE_MAX_STORAGE_ERRORS", -1, {}),
    ("SCRAPY_PIPELINE_MAX_STORAGE_ERRORS", True, {}),
    ("SCRAPY_PIPELINE_TTL", "invalid", {}),
    ("SCRAPY_PIPELINE_TTL", -1, {}),
    ("SCRAPY_PIPELINE_TTL", True, {}),
    ("SCRAPY_PIPELINE_MAX_ITEM_BYTES", "invalid", {}),
    ("SCRAPY_PIPELINE_MAX_ITEM_BYTES", 0, {}),
    ("SCRAPY_PIPELINE_MAX_ITEM_BYTES", -1, {}),
    ("SCRAPY_PIPELINE_MAX_ITEM_BYTES", True, {}),
    ("SCRAPY_PIPELINE_KEY_PREFIX", "invalid prefix", {}),
    ("SCRAPY_STORAGE_STRATEGY", "invalid", {}),
  ],
)
def test_pipeline_settings_fail_fast_as_configuration_error(
  component_manager: Any,
  setting_name: str,
  setting_value: object,
  extra: dict[str, object],
) -> None:
  values = {
    "SCRAPY_BACKEND_TYPE": "redis",
    "SCRAPY_STORAGE_STRATEGY": "batched",
    setting_name: setting_value,
    **extra,
  }

  with pytest.raises(ConfigurationError) as exc_info:
    BackendPipeline.from_settings(Settings(values))

  assert exc_info.value.setting_name == setting_name


def test_pipeline_accepts_numeric_environment_strings(
  component_manager: Any,
) -> None:
  pipeline = BackendPipeline.from_settings(
    Settings(
      {
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_STORAGE_STRATEGY": "batched",
        "SCRAPY_STORAGE_BUFFER_MAX_AGE_S": "2.5",
        "SCRAPY_PIPELINE_MAX_STORAGE_ERRORS": "0",
        "SCRAPY_PIPELINE_TTL": "3600",
        "SCRAPY_PIPELINE_MAX_ITEM_BYTES": "2048",
      }
    )
  )

  assert pipeline.storage_strategy.max_buffer_age_s == pytest.approx(2.5)
  assert pipeline.max_storage_errors == 0
  assert pipeline.ttl == 3600
  assert pipeline.max_item_bytes == 2048


@pytest.mark.parametrize(
  ("setting_name", "setting_value", "strategy"),
  [
    ("SCRAPY_DEDUP_MEMORY_MAXSIZE", "invalid", "memory"),
    ("SCRAPY_DEDUP_MEMORY_MAXSIZE", 0, "memory"),
    ("SCRAPY_DEDUP_MEMORY_MAXSIZE", -1, "memory"),
    ("SCRAPY_DEDUP_MEMORY_MAXSIZE", True, "memory"),
    ("SCRAPY_DEDUP_BLOOM_CAPACITY", "invalid", "bloom"),
    ("SCRAPY_DEDUP_BLOOM_CAPACITY", 0, "bloom"),
    ("SCRAPY_DEDUP_BLOOM_CAPACITY", -1, "bloom"),
    ("SCRAPY_DEDUP_BLOOM_CAPACITY", True, "bloom"),
    ("SCRAPY_DEDUP_BLOOM_ERROR_RATE", "invalid", "bloom"),
    ("SCRAPY_DEDUP_BLOOM_ERROR_RATE", float("nan"), "bloom"),
    ("SCRAPY_DEDUP_BLOOM_ERROR_RATE", float("inf"), "bloom"),
    ("SCRAPY_DEDUP_BLOOM_ERROR_RATE", 0, "bloom"),
    ("SCRAPY_DEDUP_BLOOM_ERROR_RATE", 1, "bloom"),
    ("SCRAPY_DEDUP_BLOOM_ERROR_RATE", True, "bloom"),
    ("SCRAPY_DEDUP_CUCKOO_CAPACITY", "invalid", "cuckoo"),
    ("SCRAPY_DEDUP_CUCKOO_CAPACITY", 0, "cuckoo"),
    ("SCRAPY_DEDUP_CUCKOO_CAPACITY", -1, "cuckoo"),
    ("SCRAPY_DEDUP_CUCKOO_ERROR_RATE", "invalid", "cuckoo"),
    ("SCRAPY_DEDUP_CUCKOO_ERROR_RATE", float("nan"), "cuckoo"),
    ("SCRAPY_DEDUP_CUCKOO_ERROR_RATE", float("inf"), "cuckoo"),
    ("SCRAPY_DEDUP_CUCKOO_ERROR_RATE", 0, "cuckoo"),
    ("SCRAPY_DEDUP_CUCKOO_ERROR_RATE", 1, "cuckoo"),
  ],
)
def test_dupefilter_numeric_settings_fail_fast_as_configuration_error(
  component_manager: Any,
  setting_name: str,
  setting_value: object,
  strategy: str,
) -> None:
  settings = Settings(
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DEDUP_STRATEGY": strategy,
      setting_name: setting_value,
    }
  )

  with pytest.raises(ConfigurationError) as exc_info:
    BackendDupeFilter.from_settings(settings)

  assert exc_info.value.setting_name == setting_name


def test_dupefilter_accepts_numeric_environment_strings(
  component_manager: Any,
) -> None:
  dupefilter = BackendDupeFilter.from_settings(
    Settings(
      {
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_DEDUP_STRATEGY": "bloom",
        "SCRAPY_DEDUP_MEMORY_MAXSIZE": "50",
        "SCRAPY_DEDUP_BLOOM_CAPACITY": "100",
        "SCRAPY_DEDUP_BLOOM_ERROR_RATE": "0.01",
        "SCRAPY_DEDUP_CUCKOO_CAPACITY": "200",
        "SCRAPY_DEDUP_CUCKOO_ERROR_RATE": "0.02",
      }
    )
  )

  assert dupefilter._filter.capacity == 100


def test_dupefilter_key_fails_fast_as_configuration_error(
  component_manager: Any,
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    BackendDupeFilter.from_settings(
      Settings(
        {
          "SCRAPY_BACKEND_TYPE": "redis",
          "SCRAPY_DUPEFILTER_KEY": "invalid key",
        }
      )
    )

  assert exc_info.value.setting_name == "SCRAPY_DUPEFILTER_KEY"


@pytest.mark.parametrize(
  "setting_name",
  [
    "SCRAPY_DEDUP_STRICT",
    "DUPEFILTER_DEBUG",
    "SCRAPY_DUPEFILTER_CLEAR_ON_OPEN",
  ],
)
def test_dupefilter_boolean_settings_wrap_scrapy_conversion_errors(
  component_manager: Any,
  setting_name: str,
) -> None:
  settings = Settings(
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      setting_name: "not-a-boolean",
    }
  )

  with pytest.raises(ConfigurationError) as exc_info:
    BackendDupeFilter.from_settings(settings)

  assert exc_info.value.setting_name == setting_name
