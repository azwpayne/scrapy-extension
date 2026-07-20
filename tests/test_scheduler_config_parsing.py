"""Regression tests for scheduler configuration value parsing."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture
from scrapy.settings import Settings as ScrapySettings

from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.schedule.scheduler import BackendScheduler

pytestmark = pytest.mark.unit


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
