"""ConnectionManager acquire/release invariants at component factories."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from scrapy.settings import Settings

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.schedule.scheduler import BackendScheduler


class FactoryConstructionError(RuntimeError):
  """Sentinel raised after a manager has been acquired."""


def _settings(**overrides: Any) -> Settings:
  values = {"SCRAPY_BACKEND_TYPE": "redis"}
  values.update(overrides)
  return Settings(values)


def _patch_manager(mocker: Any) -> tuple[Any, Any]:
  from scrapy_extension.backends.connectors import ConnectionManager

  manager = mocker.MagicMock(name="connection-manager")
  get_manager = mocker.patch.object(
    ConnectionManager,
    "get_manager",
    return_value=manager,
  )
  return manager, get_manager


@pytest.mark.parametrize(
  ("factory", "setting_name"),
  [
    (BackendPipeline.from_settings, "SCRAPY_STORAGE_STRATEGY"),
    (BackendDupeFilter.from_settings, "SCRAPY_DEDUP_STRATEGY"),
    (BackendScheduler.from_settings, "SCRAPY_QUEUE_STRATEGY"),
  ],
)
def test_invalid_strategy_is_configuration_error_without_leaking_acquire(
  mocker: Any,
  factory: Callable[[Settings], object],
  setting_name: str,
) -> None:
  manager, get_manager = _patch_manager(mocker)

  with pytest.raises(ConfigurationError):
    factory(_settings(**{setting_name: "not-a-strategy"}))

  assert manager.close.call_count == get_manager.call_count


@pytest.mark.parametrize(
  ("component", "factory"),
  [
    (BackendPipeline, BackendPipeline.from_settings),
    (BackendDupeFilter, BackendDupeFilter.from_settings),
    (BackendScheduler, BackendScheduler.from_settings),
  ],
)
def test_constructor_failure_releases_exactly_one_acquire(
  mocker: Any,
  component: type[Any],
  factory: Callable[[Settings], object],
) -> None:
  manager, get_manager = _patch_manager(mocker)
  original_error = FactoryConstructionError(component.__name__)
  mocker.patch.object(component, "__init__", side_effect=original_error)

  with pytest.raises(FactoryConstructionError) as exc_info:
    factory(_settings())

  assert exc_info.value is original_error
  get_manager.assert_called_once()
  manager.close.assert_called_once_with()


def test_release_failure_does_not_mask_original_factory_error(mocker: Any) -> None:
  manager, _ = _patch_manager(mocker)
  original_error = FactoryConstructionError("pipeline")
  manager.close.side_effect = RuntimeError("release failed")
  mocker.patch.object(BackendPipeline, "__init__", side_effect=original_error)

  with pytest.raises(FactoryConstructionError) as exc_info:
    BackendPipeline.from_settings(_settings())

  assert exc_info.value is original_error
  manager.close.assert_called_once_with()


def test_pipeline_from_crawler_failure_releases_acquired_manager(mocker: Any) -> None:
  manager, _ = _patch_manager(mocker)
  crawler = mocker.MagicMock(name="crawler")
  crawler.settings = _settings()
  crawler.stats = mocker.MagicMock(name="stats")
  original_error = FactoryConstructionError("pipeline monitor")
  mocker.patch(
    "scrapy_extension.monitor.ScrapyStatsMonitor",
    side_effect=original_error,
  )

  with pytest.raises(FactoryConstructionError) as exc_info:
    BackendPipeline.from_crawler(crawler)

  assert exc_info.value is original_error
  manager.close.assert_called_once_with()


def test_dupefilter_from_crawler_failure_releases_acquired_manager(mocker: Any) -> None:
  manager, _ = _patch_manager(mocker)
  crawler = mocker.MagicMock(name="crawler")
  crawler.settings = _settings()
  crawler.stats = mocker.MagicMock(name="stats")
  original_error = FactoryConstructionError("dupefilter monitor")
  mocker.patch(
    "scrapy_extension.dupefilter.dupefilter.ScrapyStatsMonitor",
    side_effect=original_error,
  )

  with pytest.raises(FactoryConstructionError) as exc_info:
    BackendDupeFilter.from_crawler(crawler)

  assert exc_info.value is original_error
  manager.close.assert_called_once_with()


def test_scheduler_from_crawler_failure_releases_acquired_manager(mocker: Any) -> None:
  manager, _ = _patch_manager(mocker)
  crawler = mocker.MagicMock(name="crawler")
  crawler.settings = _settings(DUPEFILTER_CLASS="example.BrokenDupeFilter")
  crawler.stats = mocker.MagicMock(name="stats")
  original_error = FactoryConstructionError("dupefilter class load")
  mocker.patch(
    "scrapy_extension.schedule.scheduler.load_object",
    side_effect=original_error,
  )

  with pytest.raises(FactoryConstructionError) as exc_info:
    BackendScheduler.from_crawler(crawler)

  assert exc_info.value is original_error
  manager.close.assert_called_once_with()


@pytest.mark.parametrize(
  "factory",
  [
    BackendPipeline.from_settings,
    BackendDupeFilter.from_settings,
    BackendScheduler.from_settings,
  ],
)
def test_successful_factory_keeps_its_acquire_until_component_close(
  mocker: Any,
  factory: Callable[[Settings], object],
) -> None:
  manager, get_manager = _patch_manager(mocker)

  component = factory(_settings())

  assert component is not None
  get_manager.assert_called_once()
  manager.close.assert_not_called()


def test_pipeline_duplicate_close_releases_factory_acquire_once(mocker: Any) -> None:
  manager, _ = _patch_manager(mocker)
  pipeline = BackendPipeline.from_settings(_settings())
  spider = mocker.MagicMock(name="spider")
  spider.name = "factory-release"

  pipeline.close_spider(spider)
  pipeline.close_spider(spider)

  manager.close.assert_called_once_with()
