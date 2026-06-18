"""Scheduler component for scrapy-extension.

This module provides a Scrapy scheduler component using backend queue
and duplicate filter interfaces.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from scrapy import signals
from scrapy.utils.misc import load_object

from scrapy_extension.backends.base import BackendType, _validate_key_name
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import QueueError, SerializationError
from scrapy_extension.queue.queue import BackendQueue

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.crawler import Crawler
  from scrapy.http import Request, Response
  from scrapy.settings import Settings
  from scrapy.statscollectors import StatsCollector
  from twisted.internet.defer import Deferred
  from twisted.python.failure import Failure

logger = logging.getLogger(__name__)


class BackendScheduler:
  """Scrapy scheduler implementation using backend interfaces.

  Uses QueueBackend for request queueing and applies duplicate filtering
  through the configured ``DUPEFILTER_CLASS`` when present.

  Message-queue backends (Kafka, RabbitMQ) ack on Scrapy's
  ``response_received`` signal and nack on ``spider_error``. This confirms
  downloader-level response delivery; it does not wait for callback or item
  pipeline completion. Atomic backends (Redis, MongoDB, ElasticSearch,
  RocketMQ) inherit no-op ack/nack. For correct ack behavior under concurrent
  processing, set ``CONCURRENT_REQUESTS=1`` when using a message-queue
  backend.

  Attributes:
      connection_manager: The connection manager for backend access.
      queue_key: The key for the request queue.
      stats: Optional stats collector for metrics.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    queue_key: str = "scheduler:queue",
    stats: StatsCollector | None = None,
    dupefilter: Any | None = None,
  ) -> None:
    """Initialize the scheduler.

    Args:
        connection_manager: Connection manager for backend access.
        queue_key: Key for the request queue.
        stats: Optional stats collector for metrics.
        dupefilter: Optional dupefilter implementing Scrapy's request_seen/log API.
    """
    self.connection_manager = connection_manager
    self.queue_key = queue_key
    self.stats = stats
    self.dupefilter = dupefilter
    self._queue: BackendQueue | None = None
    self._spider: Spider | None = None
    self._signals_connected: bool = False
    self._connected_signals = None

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendScheduler:
    """Create scheduler from Scrapy settings."""
    backend_type = BackendType(settings.get("SCRAPY_BACKEND_TYPE", "redis"))
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=settings.getdict("SCRAPY_BACKEND_SETTINGS", {}),
    )
    return cls(
      connection_manager=manager,
      queue_key=settings.get("SCRAPY_QUEUE_KEY", "scheduler:queue"),
    )

  @classmethod
  def from_crawler(cls, crawler: Crawler) -> BackendScheduler:
    """Create scheduler from crawler."""
    scheduler = cls.from_settings(crawler.settings)
    scheduler.stats = crawler.stats
    dupefilter_path = crawler.settings.get("DUPEFILTER_CLASS")
    if dupefilter_path:
      dupefilter_cls = load_object(dupefilter_path)
      scheduler.dupefilter = dupefilter_cls.from_crawler(crawler)
    return scheduler

  def open(self, spider: Spider) -> Deferred[None] | None:
    """Open the scheduler for a spider and wire ack/nack signals.

    Return type matches Scrapy's ``Scheduler.open`` protocol
    (``Deferred[None] | None``). This implementation is synchronous —
    returns ``None`` — which Scrapy's engine handles correctly via
    ``yield self.scheduler.open(spider)`` (yielding None is a no-op in
    both inlineCallbacks and async-first reactor modes).

    Args:
        spider: The spider instance.

    Raises:
        ValueError: If ``spider.name`` contains characters unsafe for use as
            a backend key (only ``[a-zA-Z0-9._:-]`` allowed). Surfaces the
            misconfiguration at open time rather than as a confusing
            "_validate_key_name" failure deep inside the first push.
    """
    _validate_key_name(spider.name, field_name="spider.name")
    self._spider = spider
    self._queue = BackendQueue(
      connection_manager=self.connection_manager,
      queue_name=self.queue_key,
      spider=spider,
    )
    self._connect_ack_signals(spider)
    logger.info("Scheduler opened for spider %s", spider.name)

  def _connect_ack_signals(self, spider: Spider) -> None:
    """Wire response_received → ack, spider_error → nack.

    Uses ``spider.crawler.signals`` so the scheduler doesn't need a
    crawler reference at construction time. Idempotent: guarded by
    ``_signals_connected`` so re-open doesn't double-register.
    """
    if self._signals_connected:
      return
    crawler = getattr(spider, "crawler", None)
    if crawler is None:
      logger.warning(
        "spider has no 'crawler' attribute — ack/nack signals not wired. "
        "Kafka/RabbitMQ messages will re-deliver on consumer restart "
        "(at-least-once) but won't be acked in-session. "
        "Ensure the spider is created via CrawlerProcess/CrawlerRunner."
      )
      return
    sig = crawler.signals
    sig.connect(self._on_response_received, signal=signals.response_received)
    sig.connect(self._on_spider_error, signal=signals.spider_error)
    self._connected_signals = sig
    self._signals_connected = True

  def _on_response_received(
    self,
    response: Response,
    request: Request,
    spider: Spider,
  ) -> None:
    """Ack the last-popped message after the download succeeded."""
    del response, request, spider
    if self._queue is None:
      return
    try:
      self._queue.ack()
    except QueueError:
      logger.exception("Failed to ack message after response_received")

  def _on_spider_error(
    self,
    failure: Failure,
    response: Response,
    spider: Spider,
  ) -> None:
    """Nack the last-popped message so it re-delivers for retry."""
    del failure, response, spider
    if self._queue is None:
      return
    try:
      self._queue.nack()
    except QueueError:
      logger.exception("Failed to nack message after spider_error")

  def close(self, reason: str) -> Deferred[None] | None:
    """Close the scheduler."""
    logger.info("Scheduler closed: %s", reason)
    if self._connected_signals is not None:
      self._connected_signals.disconnect(
        self._on_response_received,
        signal=signals.response_received,
      )
      self._connected_signals.disconnect(
        self._on_spider_error,
        signal=signals.spider_error,
      )
    self.connection_manager.close()
    self._queue = None
    self._spider = None
    self._connected_signals = None
    self._signals_connected = False

  def enqueue_request(self, request: Request) -> bool:
    """Enqueue a request.

    Applies duplicate filtering through the configured ``DUPEFILTER_CLASS``
    unless ``request.dont_filter`` is set.

    Args:
        request: The request to enqueue.

    Returns:
        True if the request was enqueued, False on duplicate or push failure.
    """
    if self._queue is None:
      msg = "Scheduler not opened"
      raise RuntimeError(msg)

    if (
      self.dupefilter is not None
      and not request.dont_filter
      and self.dupefilter.request_seen(request)
    ):
      if self._spider is not None:
        self.dupefilter.log(request, self._spider)
      return False

    priority = request.priority
    try:
      self._queue.push(request, priority=priority)
      if self.stats:
        self.stats.inc_value("scheduler/enqueued")
    except SerializationError:
      logger.exception("Failed to serialize request for enqueue")
      if self.stats:
        self.stats.inc_value("scheduler/serialization_errors")
      return False
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
    except SerializationError:
      logger.exception("Failed to deserialize queued request")
      if self.stats:
        self.stats.inc_value("scheduler/deserialization_errors")
      return None
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
    try:
      return len(self) > 0
    except (NotImplementedError, QueueError):
      logger.warning(
        "Queue length lookup is unavailable; assuming pending requests exist"
      )
      return True

  def __len__(self) -> int:
    """Get the number of pending requests.

    Returns:
        Number of pending requests.
    """
    if self._queue is None:
      return 0
    return len(self._queue)
