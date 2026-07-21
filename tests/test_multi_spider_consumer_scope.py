"""Consumer-manager isolation regressions for multi-spider construction."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from scrapy_extension.backends.connectors import _CONNECTION_MANAGER_SCOPE_KEY
from scrapy_extension.schedule.scheduler import BackendScheduler


def _settings(backend_type: str, queue_key: str) -> Mock:
  settings = Mock()
  values = {
    "SCRAPY_BACKEND_TYPE": backend_type,
    "SCRAPY_QUEUE_KEY": queue_key,
  }
  settings.get.side_effect = lambda key, default=None: values.get(key, default)
  settings.getdict.return_value = {}
  return settings


@pytest.mark.parametrize("backend_type", ["kafka", "rocketmq"])
def test_unresolved_direct_template_gets_unshareable_consumer_scope(
  mocker, backend_type
) -> None:
  mocker.patch.object(BackendScheduler, "_enforce_ack_concurrency_gate")
  settings = _settings(backend_type, "q:{spider}")
  first = BackendScheduler.from_settings(settings)
  second = BackendScheduler.from_settings(settings)
  try:
    assert first.connection_manager is not second.connection_manager
    first_scope = first.connection_manager.settings[_CONNECTION_MANAGER_SCOPE_KEY]
    second_scope = second.connection_manager.settings[_CONNECTION_MANAGER_SCOPE_KEY]
    assert first_scope != second_scope
    assert first_scope != "q:{spider}"
    assert second_scope != "q:{spider}"
  finally:
    first.close("test")
    second.close("test")


def test_fixed_consumer_queue_still_shares_manager(mocker) -> None:
  mocker.patch.object(BackendScheduler, "_enforce_ack_concurrency_gate")
  settings = _settings("kafka", "shared-queue")
  first = BackendScheduler.from_settings(settings)
  second = BackendScheduler.from_settings(settings)
  try:
    assert first.connection_manager is second.connection_manager
    assert (
      first.connection_manager.settings[_CONNECTION_MANAGER_SCOPE_KEY]
      == "shared-queue"
    )
  finally:
    first.close("test")
    second.close("test")


def test_from_crawler_resolves_spider_before_manager_acquire(mocker) -> None:
  mocker.patch.object(BackendScheduler, "_enforce_ack_concurrency_gate")

  def crawler(spider_name: str) -> Mock:
    value = Mock()
    value.settings = _settings("kafka", "q:{spider}")
    value.stats = Mock()
    value.spider = None
    value.spidercls = type(f"{spider_name.title()}Spider", (), {"name": spider_name})
    return value

  first = BackendScheduler.from_crawler(crawler("alpha"))
  second = BackendScheduler.from_crawler(crawler("beta"))
  try:
    assert first.queue_key == "q:alpha"
    assert second.queue_key == "q:beta"
    assert first.connection_manager is not second.connection_manager
    assert (
      first.connection_manager.settings[_CONNECTION_MANAGER_SCOPE_KEY]
      == "q:alpha"
    )
    assert (
      second.connection_manager.settings[_CONNECTION_MANAGER_SCOPE_KEY]
      == "q:beta"
    )
  finally:
    first.close("test")
    second.close("test")
