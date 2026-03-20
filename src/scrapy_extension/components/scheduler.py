"""Scheduler component for scrapy-extension.

This module provides a Scrapy scheduler component using backend queue
and duplicate filter interfaces.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from scrapy_extension.components.queue import BackendQueue
from scrapy_extension.exceptions import QueueError
from scrapy_extension.utils.request import request_fingerprint

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.crawler import Crawler
  from scrapy.http import Request
  from scrapy.settings import Settings
  from scrapy.statscollectors import StatsCollector

  from scrapy_extension.connection.manager import ConnectionManager

logger = logging.getLogger(__name__)


class BackendScheduler:
  """Scrapy scheduler implementation using backend interfaces.

  This scheduler uses:
  - QueueBackend for request queueing
  - SetBackend for duplicate filtering

  Attributes:
      connection_manager: The connection manager for backend access.
      queue_key: The key for the request queue.
      dupefilter_key: The key for the dupefilter set.
      stats: Optional stats collector for metrics.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    queue_key: str = "scheduler:queue",
    dupefilter_key: str = "scheduler:dupefilter",
    stats: StatsCollector | None = None,
  ) -> None:
    """Initialize the scheduler.

    Args:
        connection_manager: Connection manager for backend access.
        queue_key: Key for the request queue.
        dupefilter_key: Key for the dupefilter set.
        stats: Optional stats collector for metrics.
    """
    self.connection_manager = connection_manager
    self.queue_key = queue_key
    self.dupefilter_key = dupefilter_key
    self.stats = stats
    self._queue: BackendQueue | None = None
    self._spider: Spider | None = None

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendScheduler:
    """Create scheduler from Scrapy settings.

    Args:
        settings: Scrapy settings object.

    Returns:
        A new BackendScheduler instance.
    """
    from scrapy_extension.backends.base import BackendType
    from scrapy_extension.connection.manager import ConnectionManager

    backend_type = BackendType(settings.get("SCRAPY_BACKEND_TYPE", "redis"))
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=settings.getdict("SCRAPY_BACKEND_SETTINGS", {}),
    )
    return cls(
      connection_manager=manager,
      queue_key=settings.get("SCRAPY_QUEUE_KEY", "scheduler:queue"),
      dupefilter_key=settings.get("SCRAPY_DUPEFILTER_KEY", "scheduler:dupefilter"),
    )

  @classmethod
  def from_crawler(cls, crawler: Crawler) -> BackendScheduler:
    """Create scheduler from crawler.

    Args:
        crawler: The Scrapy crawler instance.

    Returns:
        A new BackendScheduler instance.
    """
    scheduler = cls.from_settings(crawler.settings)
    scheduler.stats = crawler.stats
    return scheduler

  def open(self, spider: Spider) -> None:
    """Open the scheduler for a spider.

    Args:
        spider: The spider instance.
    """
    self._spider = spider
    self._queue = BackendQueue(
      connection_manager=self.connection_manager,
      queue_name=f"{spider.name}:queue",
    )
    logger.info("Scheduler opened for spider %s", spider.name)

  def close(self, reason: str) -> None:
    """Close the scheduler.

    Args:
        reason: The reason for closing.
    """
    logger.info("Scheduler closed: %s", reason)
    self._queue = None
    self._spider = None

  def enqueue_request(self, request: Request) -> bool:
    """Enqueue a request.

    Args:
        request: The request to enqueue.

    Returns:
        True if the request was enqueued, False if it was a duplicate.
    """
    # Skip dedup for backends that only support queue operations (e.g. Kafka, RabbitMQ).
    fingerprint = self._request_fingerprint(request)
    try:
      set_backend = self.connection_manager.get_set_backend()
    except NotImplementedError:
      logger.debug("Backend does not support set operations; skipping dedup")
    else:
      added = set_backend.add(self.dupefilter_key, fingerprint.encode())
      if not added:
        if self.stats:
          self.stats.inc_value("scheduler/dropped_duplicates")
        return False

    # Enqueue with priority (negate because Scrapy uses higher = more urgent)
    priority = request.priority
    try:
      if self._queue is None:
        msg = "Scheduler not opened"
        raise RuntimeError(msg)
      self._queue.push(request, priority=priority)
      if self.stats:
        self.stats.inc_value("scheduler/enqueued")
    except QueueError:
      logger.exception("Failed to enqueue request")
      return False
    else:
      return True

  def next_request(self) -> Request | None:
    """Get the next request from the queue.

    Returns:
        The next request, or None if the queue is empty.
    """
    try:
      if self._queue is None:
        msg = "Scheduler not opened"
        raise RuntimeError(msg)
      request = self._queue.pop(timeout=0)
      if request and self.stats:
        self.stats.inc_value("scheduler/dequeued")
    except QueueError:
      logger.exception("Failed to get next request")
      return None
    else:
      return request

  def has_pending_requests(self) -> bool:
    """Check if there are pending requests.

    Returns:
        True if there are pending requests.
    """
    return len(self) > 0

  def __len__(self) -> int:
    """Get the number of pending requests.

    Returns:
        Number of pending requests.
    """
    if self._queue is None:
      return 0
    return len(self._queue)

  def _request_fingerprint(self, request: Request) -> str:
    """Generate a fingerprint for a request.

    Args:
        request: The request to fingerprint.

    Returns:
        A unique fingerprint string.
    """
    return request_fingerprint(request)
