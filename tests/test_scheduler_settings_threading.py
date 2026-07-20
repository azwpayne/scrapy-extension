"""R14-C: deferred settings-wiring (U4/U5/U2 knobs reach the constructors).

Round-9 (U4/U5) + round-12 (U2) shipped these operability knobs as
constructor defaults ONLY — ``BackendScheduler.from_settings`` never
threaded them, so they were stuck at defaults and the runbook's "tune via
settings" hand-wave pointed at settings that did not exist. R14-C threads:

- ``SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY``      → ``BackendQueue.depth_sample_every``
- ``SCRAPY_QUEUE_MAX_ITEM_BYTES``          → ``BackendQueue.max_item_bytes``
- ``SCRAPY_QUEUE_DELAY_MAX_HELD``          → ``build_queue_strategy(max_held=…)``
- ``SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD``→ ``ScrapyStatsMonitor.backpressure_threshold``
- ``SCRAPY_MONITOR_POP_RATE_WINDOW_S``     → ``BackendQueue._pop_rate_window_s``
                                              + ``ScrapyStatsMonitor.pop_rate_window_s``

These tests pin the threading half of the contract (the settings-layer half
lives in ``test_config.TestR14COperabilitySettings``). Pattern: build a
Scrapy-Settings-like Mock resolving the SCRAPY_* keys, call
``BackendScheduler.from_settings`` → ``open(spider)``, then assert the
constructed ``BackendQueue`` / strategy / monitor reflect the setting.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest
from scrapy import Spider

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.schedule.scheduler import BackendScheduler


def _make_settings(
  *,
  depth_sample_every: int | None = None,
  max_item_bytes: int | None = None,
  delay_max_held: int | None = None,
  monitor_backpressure_threshold: int | None = None,
  monitor_pop_rate_window_s: float | None = None,
  queue_strategy: str = "passthrough",
  ring_buffer_full_policy: str | None = None,
  snapshot_owner: str | None = None,
) -> Mock:
  """Build a Scrapy-Settings-like Mock resolving the R14-C SCRAPY_* keys.

  Mirrors the pattern in ``test_scheduler_ack_gate._make_settings`` — Scrapy's
  ``Settings.get(key, default)`` API. Unset keys fall through to the default
  so the scheduler reads the configured value when the operator set it.
  """
  settings = Mock()
  overrides: dict[str, Any] = {
    "SCRAPY_BACKEND_TYPE": "redis",
    "SCRAPY_QUEUE_KEY": "scheduler:queue",
    "SCRAPY_QUEUE_STRATEGY": queue_strategy,
  }
  if depth_sample_every is not None:
    overrides["SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY"] = depth_sample_every
  if max_item_bytes is not None:
    overrides["SCRAPY_QUEUE_MAX_ITEM_BYTES"] = max_item_bytes
  if delay_max_held is not None:
    overrides["SCRAPY_QUEUE_DELAY_MAX_HELD"] = delay_max_held
  if monitor_backpressure_threshold is not None:
    overrides["SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD"] = monitor_backpressure_threshold
  if monitor_pop_rate_window_s is not None:
    overrides["SCRAPY_MONITOR_POP_RATE_WINDOW_S"] = monitor_pop_rate_window_s
  if ring_buffer_full_policy is not None:
    overrides["SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY"] = ring_buffer_full_policy
  if snapshot_owner is not None:
    overrides["SCRAPY_QUEUE_SNAPSHOT_OWNER"] = snapshot_owner

  def get(key: str, default: Any = None) -> Any:
    return overrides.get(key, default)

  settings.get.side_effect = get
  settings.getfloat.return_value = 0.0
  settings.getdict.return_value = {}
  return settings


class TestRingBufferBlockingPolicyGate:
  def test_block_policy_is_rejected_before_manager_acquire(self, mocker) -> None:
    settings = _make_settings(
      queue_strategy="ring_buffer",
      ring_buffer_full_policy="block",
    )
    settings.getint.side_effect = lambda _key, default=0: default
    get_manager = mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )

    with pytest.raises(ConfigurationError) as exc_info:
      BackendScheduler.from_settings(settings)

    assert exc_info.value.setting_name == "SCRAPY_QUEUE_RING_BUFFER_FULL_POLICY"
    assert exc_info.value.setting_value == "block"
    get_manager.assert_not_called()


class _FakeSpider(Spider):
  name = "r14c"

  def __init__(self) -> None:
    # Bypass Scrapy's Spider.__init__ (needs crawler context). Set just what
    # the scheduler reads (spider.name + spider.crawler.stats for monitor).
    self.crawler = None  # type: ignore[assignment]


def _open_scheduler(scheduler: BackendScheduler) -> BackendQueue:
  """Call ``open(spider)`` and return the constructed ``BackendQueue``.

  ``BackendQueue`` is built inside ``open()`` (not ``from_settings``) so the
  R14-C knobs must be carried from ``from_settings`` → instance state →
  ``open()`` → constructor. This helper drives that path and asserts the
  queue was wired.
  """
  scheduler.open(_FakeSpider())
  assert scheduler._queue is not None
  return scheduler._queue


class TestR14CDepthSampleEveryThreading:
  """``SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY`` → ``BackendQueue.depth_sample_every``."""

  def test_custom_value_threaded(self, mocker) -> None:
    """Set ``=5`` → constructed ``BackendQueue.depth_sample_every == 5``."""
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings(depth_sample_every=5)
    scheduler = BackendScheduler.from_settings(settings)
    queue = _open_scheduler(scheduler)
    assert queue.depth_sample_every == 5

  def test_default_when_unset(self, mocker) -> None:
    """Unset → constructor default (100) preserved."""
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings()
    scheduler = BackendScheduler.from_settings(settings)
    queue = _open_scheduler(scheduler)
    assert queue.depth_sample_every == BackendQueue.DEFAULT_DEPTH_SAMPLE_EVERY


class TestR14CMaxItemBytesThreading:
  """``SCRAPY_QUEUE_MAX_ITEM_BYTES`` → ``BackendQueue.max_item_bytes``."""

  def test_custom_value_threaded(self, mocker) -> None:
    """Set ``=2048`` → constructed ``BackendQueue.max_item_bytes == 2048``."""
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings(max_item_bytes=2048)
    scheduler = BackendScheduler.from_settings(settings)
    queue = _open_scheduler(scheduler)
    assert queue.max_item_bytes == 2048


class TestR14CDelayMaxHeldThreading:
  """``SCRAPY_QUEUE_DELAY_MAX_HELD`` → ``DelayQueueStrategy._max_held``."""

  def test_custom_value_threaded(self, mocker) -> None:
    """Set ``=5000`` + ``delay`` strategy → ``DelayQueueStrategy._max_held == 5000``."""
    settings = _make_settings(delay_max_held=5000)
    settings.get.side_effect = lambda key, default=None: (
      "delay" if key == "SCRAPY_QUEUE_STRATEGY" else (
        5000 if key == "SCRAPY_QUEUE_DELAY_MAX_HELD" else (
          "redis" if key == "SCRAPY_BACKEND_TYPE" else (
            "scheduler:queue" if key == "SCRAPY_QUEUE_KEY" else default
          )
        )
      )
    )
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    scheduler = BackendScheduler.from_settings(settings)
    _open_scheduler(scheduler)
    assert isinstance(scheduler._queue_strategy, DelayQueueStrategy)
    assert scheduler._queue_strategy._max_held == 5000


class TestR14CBackpressureThresholdThreading:
  """``SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD`` → ``ScrapyStatsMonitor``.

  The scheduler does not construct the monitor directly (the BackendQueue
  resolves it default-on from ``spider.crawler.stats``). R14-C threads the
  threshold via the scheduler so the value is available where the monitor
  is constructed. This test pins the scheduler-side carry; the
  ``BackendQueue``-resolved monitor reads it from the threaded value.
  """

  def test_custom_value_carried_on_scheduler(self, mocker) -> None:
    """Set ``=2500`` → ``scheduler._monitor_backpressure_threshold == 2500``."""
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings(monitor_backpressure_threshold=2500)
    scheduler = BackendScheduler.from_settings(settings)
    assert scheduler._monitor_backpressure_threshold == 2500

  def test_default_carried_when_unset(self, mocker) -> None:
    """Unset → default (1000) carried on the scheduler."""
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings()
    scheduler = BackendScheduler.from_settings(settings)
    assert scheduler._monitor_backpressure_threshold == 1000


class TestR14CPopRateWindowThreading:
  """``SCRAPY_MONITOR_POP_RATE_WINDOW_S`` → ``BackendQueue._pop_rate_window_s``."""

  def test_custom_value_threaded_to_queue(self, mocker) -> None:
    """Set ``=30.0`` → constructed ``BackendQueue._pop_rate_window_s == 30.0``."""
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings(monitor_pop_rate_window_s=30.0)
    scheduler = BackendScheduler.from_settings(settings)
    queue = _open_scheduler(scheduler)
    assert queue._pop_rate_window_s == pytest.approx(30.0)

  def test_custom_value_carried_on_scheduler(self, mocker) -> None:
    """Set ``=30.0`` → ``scheduler._monitor_pop_rate_window_s == 30.0``."""
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings(monitor_pop_rate_window_s=30.0)
    scheduler = BackendScheduler.from_settings(settings)
    assert scheduler._monitor_pop_rate_window_s == pytest.approx(30.0)


class TestSnapshotOwnerThreading:
  def test_explicit_snapshot_owner_reaches_backend_queue(self, mocker) -> None:
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings(snapshot_owner="worker-a")

    queue = _open_scheduler(BackendScheduler.from_settings(settings))

    assert queue._snapshot_owner == "worker-a"
    assert queue._snapshot_key() == (
      "queue:snapshot:v2:8:worker-a:4:r14c:scheduler:queue"
    )

  def test_worker_id_is_snapshot_owner_fallback(self, mocker) -> None:
    mocker.patch(
      "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
      return_value=mocker.Mock(),
    )
    settings = _make_settings()
    original_get = settings.get.side_effect

    def get(key: str, default: Any = None) -> Any:
      if key == "SCRAPY_QUEUE_WORKER_ID":
        return "worker-fallback"
      return original_get(key, default)

    settings.get.side_effect = get

    queue = _open_scheduler(BackendScheduler.from_settings(settings))

    assert queue._snapshot_owner == "worker-fallback"
