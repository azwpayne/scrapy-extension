"""Scheduler component for scrapy-extension.

This module provides a Scrapy scheduler component using backend queue
and duplicate filter interfaces.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from scrapy import signals
from scrapy.utils.misc import load_object

from scrapy_extension.backends.base import _validate_key_name
from scrapy_extension.backends.connectors import (
  QUEUE_CAPABLE_BACKENDS,
  ConnectionManager,
  resolve_backend_config,
)
from scrapy_extension.exceptions import (
  BackendError,
  ConfigurationError,
  QueueError,
  SerializationError,
)
from scrapy_extension.queue.queue import BackendQueue

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.crawler import Crawler
  from scrapy.http import Request, Response
  from scrapy.settings import Settings
  from scrapy.statscollectors import StatsCollector
  from twisted.internet.defer import Deferred
  from twisted.python.failure import Failure

  from scrapy_extension.queue.strategies.base import QueueStrategy

logger = logging.getLogger(__name__)


class BackendScheduler:
  """Scrapy scheduler implementation using backend interfaces.

  Uses QueueBackend for request queueing and applies duplicate filtering
  through the configured ``DUPEFILTER_CLASS`` when present.

  Ack/nack semantics (important — read before tuning concurrency):

  1. **Ack fires on ``response_received``, NOT on callback/pipeline
     completion.** For message-queue backends (Kafka, RabbitMQ, SQS,
     Pulsar), a message is acked as soon as Scrapy's downloader delivers
     the response (``signals.response_received``) and nacked on
     ``signals.spider_error``. The ack is *download-level*: it does **not**
     wait for the spider callback, the item pipeline, or any post-download
     processing. A crash between ack and pipeline completion drops the
     item (at-most-once for the pipeline side); a crash before ack
     re-delivers the message (at-least-once for the download side).

  2. **Concurrent-ack correctness is per-backend, gated at from_settings.**
     Backends declare ``QueueBackend.requires_ack`` /
     ``supports_concurrent_ack``:

     - **Atomic-pop backends** (Redis, MongoDB, ElasticSearch, RocketMQ):
       ``requires_ack=False``. pop removes the item in one step; ack/nack
       are no-ops. ``CONCURRENT_REQUESTS`` is unrestricted.
     - **Real in-flight-set backends** (Kafka, RabbitMQ):
       ``requires_ack=True``, ``supports_concurrent_ack=True``.
       ``pop_with_ack`` returns a per-message token tracked in an in-flight
       set; ``ack(token=…)`` commits the specific offset / basic_acks the
       specific delivery tag. N pops before any ack no longer overwrite a
       single slot — correct under ``CONCURRENT_REQUESTS > 1``.
       ``CONCURRENT_REQUESTS`` is unrestricted.
     - **Single-slot-ack backends** (SQS, Pulsar): ``requires_ack=True``,
       ``supports_concurrent_ack=False``. Ack tracks ONE receipt slot; N
       pops before any ack overwrite it and only the last-popped message
       is ackable → silent at-least-once violation under
       ``CONCURRENT_REQUESTS > 1``. The ``from_settings`` gate raises
       ``ConfigurationError`` here unless the explicit
       ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`` opt-out is set. The real
       in-flight-set fix for these backends is a follow-up (Tier-2).

  3. **At-least-once on crash is inherent.** A worker crash before ack
     fires leaves the message unacked (Kafka: offset uncommitted; RabbitMQ:
     delivery unacked; SQS: visibility timeout expires; Pulsar: retry
     policy redelivers) → it is re-delivered on reconnect/restart. This is
     the intended at-least-once guarantee, not a defect.

  4. **Dedup outage does not crash the spider.** ``enqueue_request`` runs
     ``dupefilter.request_seen`` INSIDE its try-block; a ``QueueError`` /
     ``BackendError`` from the dedup backend degrades to default-enqueue
     (the URL is not lost) + a ``scheduler/dupefilter_error`` stat bump.

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
    queue_strategy: QueueStrategy | None = None,
  ) -> None:
    """Initialize the scheduler.

    Args:
        connection_manager: Connection manager for backend access.
        queue_key: Key for the request queue.
        stats: Optional stats collector for metrics.
        dupefilter: Optional dupefilter implementing Scrapy's request_seen/log API.
        queue_strategy: Optional queue-semantics strategy threaded into the
            BackendQueue. When ``None`` (default), BackendQueue uses
            PassthroughQueueStrategy (current behavior).
    """
    self.connection_manager = connection_manager
    self.queue_key = queue_key
    self.stats = stats
    self.dupefilter = dupefilter
    self._queue_strategy = queue_strategy
    self._queue: BackendQueue | None = None
    self._spider: Spider | None = None
    self._signals_connected: bool = False
    self._connected_signals = None

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendScheduler:
    """Create scheduler from Scrapy settings.

    Selects the queue strategy from ``SCRAPY_QUEUE_STRATEGY`` (default
    ``passthrough``). The delay strategy reads ``SCRAPY_QUEUE_DELAY_DEFAULT``.

    Backend selection: ``SCRAPY_QUEUE_BACKEND_TYPE`` /
    ``SCRAPY_QUEUE_BACKEND_SETTINGS`` override the global
    ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so the queue can
    bind to a different backend than the dedup filter or storage pipeline
    (multi-backend coexistence). Unset → falls back to the global keys.

    **Ack-concurrency gate (round-2, C1 fix).** After the queue backend is
    resolved, the backend's ``QueueBackend.requires_ack`` /
    ``supports_concurrent_ack`` class attributes are inspected. If the
    backend requires ack but does NOT support concurrent ack (SQS, Pulsar —
    single-slot ack) AND ``CONCURRENT_REQUESTS > 1`` AND the explicit
    ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`` opt-out is NOT set, this
    raises :class:`ConfigurationError`. Atomic backends (Redis/Mongo/ES/
    RocketMQ) and real in-flight-set backends (Kafka/RabbitMQ) are
    unaffected. Read the opt-out via ``settings.get(..., False)`` — it is
    NOT a pydantic field. See the class docstring for the full contract.
    """
    from scrapy_extension.queue.strategies.factory import (
      QueueStrategyType,
      build_queue_strategy,
    )

    backend_type, backend_settings = resolve_backend_config(
      settings,
      type_key="SCRAPY_QUEUE_BACKEND_TYPE",
      settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
      required_capabilities=QUEUE_CAPABLE_BACKENDS,
      component_name="queue",
    )
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=backend_settings,
    )

    # Ack-concurrency gate (round-2 C1 fix). Inspect the backend CLASS —
    # no instantiation/connection needed. Single-slot-ack backends (SQS,
    # Pulsar) under CONCURRENT_REQUESTS>1 silently lose N-1/N acks; the
    # gate converts that silent defect into a loud fail-fast unless the
    # operator explicitly opts in via SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS.
    BackendScheduler._enforce_ack_concurrency_gate(settings, backend_type)

    strategy_type = QueueStrategyType(
      settings.get("SCRAPY_QUEUE_STRATEGY", QueueStrategyType.PASSTHROUGH.value)
    )
    queue_strategy = build_queue_strategy(
      strategy_type,
      manager,
      default_delay=settings.getfloat("SCRAPY_QUEUE_DELAY_DEFAULT", 0.0),
      min_interval=settings.getfloat("SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL", 0.0),
    )
    return cls(
      connection_manager=manager,
      queue_key=settings.get("SCRAPY_QUEUE_KEY", "scheduler:queue"),
      queue_strategy=queue_strategy,
    )

  @staticmethod
  def _enforce_ack_concurrency_gate(settings: Settings, backend_type: Any) -> None:
    """Raise ConfigurationError for single-slot-ack backends under concurrency.

    Reads ``QueueBackend.requires_ack`` / ``supports_concurrent_ack`` from
    the backend CLASS (no instantiation — pure attribute read via the
    lazy class lookup in ``_BACKEND_FACTORIES``). Single-slot-ack backends
    (SQS, Pulsar) silently lose N-1 of N acks under ``CONCURRENT_REQUESTS
    > 1``; this gate makes that loud unless the operator opts in via
    ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS``.

    Args:
        settings: Scrapy settings (read ``CONCURRENT_REQUESTS`` + opt-out).
        backend_type: The resolved ``BackendType`` for the queue component.

    Raises:
        ConfigurationError: If the backend requires ack, does not support
            concurrent ack, ``CONCURRENT_REQUESTS > 1``, and the opt-out
            is not set.
    """
    from scrapy_extension.backends.connectors import _BACKEND_FACTORIES, _load_object

    backend_cls = _load_object(_BACKEND_FACTORIES[backend_type][0])
    requires_ack = getattr(backend_cls, "requires_ack", False)
    supports_concurrent = getattr(backend_cls, "supports_concurrent_ack", True)
    if not requires_ack or supports_concurrent:
      return
    concurrent = int(settings.get("CONCURRENT_REQUESTS", 16))
    if concurrent <= 1:
      return
    opt_out = bool(settings.get("SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS", False))
    if opt_out:
      return
    msg = (
      f"Backend {backend_type.value!r} requires explicit ack but does NOT "
      f"support concurrent ack (single-slot ack). Under "
      f"CONCURRENT_REQUESTS={concurrent} (>1), only the last-popped "
      f"message is ackable and the rest are silently lost (at-least-once "
      f"violation). Either (a) pin CONCURRENT_REQUESTS=1, (b) switch to a "
      f"concurrency-safe backend (Kafka/RabbitMQ), or (c) set "
      f"SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS=True to opt in to the "
      f"known-broken mode (NOT recommended — silent data loss)."
    )
    raise ConfigurationError(
      msg,
      setting_name="CONCURRENT_REQUESTS",
      setting_value=concurrent,
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

    **Queue-key templating (round-2, C8 fix).** If ``self.queue_key``
    contains the literal token ``{spider}``, the token is substituted with
    ``spider.name`` BEFORE constructing the BackendQueue. This lets two
    spiders on the same backend use disjoint queues
    (``SCRAPY_QUEUE_KEY="q:{spider}"``) — without it, the default
    ``scheduler:queue`` is shared across spiders (silent cross-spider
    request leakage / contamination). Default key unchanged → existing
    single-spider deployments are unaffected. Multi-spider footgun: with
    templating, the dedup set is still shared unless separately templated
    (see dupefilter_key in BackendDupeFilter).

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
    # Resolve {spider} template in queue_key at open() (round-2 C8 fix).
    # str.replace (not str.format) so brace-bearing keys like
    # "q:{spider}-{date}" don't raise KeyError on the unrelated {date};
    # matches the dupefilter path's .replace() substitution.
    if "{spider}" in self.queue_key:
      self.queue_key = self.queue_key.replace("{spider}", spider.name)
    self._queue = BackendQueue(
      connection_manager=self.connection_manager,
      queue_name=self.queue_key,
      spider=spider,
      queue_strategy=self._queue_strategy,
    )
    self._connect_ack_signals(spider)
    logger.info("Scheduler opened for spider %s", spider.name)
    return None

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
    """Ack the specific popped message after the download succeeded.

    Reads the ack token the pop path injected into
    ``request.meta["_backend_ack_token"]`` and forwards it to
    ``BackendQueue.ack(token=…)`` so the backend acks the *specific*
    message (Kafka contiguous watermark / RabbitMQ per-tag basic_ack) —
    correct under ``CONCURRENT_REQUESTS > 1``.
    """
    del response, spider
    if self._queue is None:
      return
    token = (
      request.meta.get("_backend_ack_token")
      if request is not None and getattr(request, "meta", None) is not None
      else None
    )
    try:
      self._queue.ack(token=token)
    except QueueError:
      logger.exception("Failed to ack message after response_received")

  def _on_spider_error(
    self,
    failure: Failure,
    response: Response,
    spider: Spider,
  ) -> None:
    """Nack the specific popped message so it re-delivers for retry.

    Reads the ack token from ``response.request.meta`` (the request that
    failed) and forwards it to ``BackendQueue.nack(token=…)``.
    """
    del failure, spider
    if self._queue is None:
      return
    token = None
    failed_request = getattr(response, "request", None) if response is not None else None
    if failed_request is not None and getattr(failed_request, "meta", None) is not None:
      token = failed_request.meta.get("_backend_ack_token")
    try:
      self._queue.nack(token=token)
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
    # Close the queue strategy FIRST so it can warn about / release any
    # in-process held state (e.g. DelayQueueStrategy's delayed items) while
    # the backend is still connected. Must precede connection_manager.close().
    if self._queue is not None:
      try:
        self._queue.close()
      except Exception:
        logger.exception("Failed to close queue strategy during shutdown")
    self.connection_manager.close()
    self._queue = None
    self._spider = None
    self._connected_signals = None
    self._signals_connected = False
    return None

  def enqueue_request(self, request: Request) -> bool:
    """Enqueue a request.

    Applies duplicate filtering through the configured ``DUPEFILTER_CLASS``
    unless ``request.dont_filter`` is set.

    **Dedup-outage envelope (round-2, C6 fix).** The
    ``dupefilter.request_seen`` call is INSIDE the try-block. A
    ``QueueError`` / ``BackendError`` from the dedup backend (partial
    connectivity: queue up, dedup backend down) is logged, the
    ``scheduler/dupefilter_error`` stat is incremented, and the request is
    default-enqueued (NOT dropped) so no URL is lost. The spider stays up
    in degraded mode rather than crashing on an unhandled exception.

    Args:
        request: The request to enqueue.

    Returns:
        True if the request was enqueued, False on duplicate or push failure.
    """
    if self._queue is None:
      msg = "Scheduler not opened"
      raise RuntimeError(msg)

    priority = request.priority
    phase = "dedup"
    try:
      # Dedup check is INSIDE the try (round-2 C6 fix) so a dedup-backend
      # outage degrades to default-enqueue instead of crashing the spider.
      # `phase` distinguishes WHICH call raised so the stat + retry are
      # attributed correctly (review follow-up: the prior branch couldn't
      # tell a dedup raise from a push raise → wrong stat + redundant retry).
      if (
        self.dupefilter is not None
        and not request.dont_filter
        and self.dupefilter.request_seen(request)
      ):
        if self._spider is not None:
          self.dupefilter.log(request, self._spider)
        return False
      phase = "push"
      self._queue.push(request, priority=priority)
      if self.stats:
        self.stats.inc_value("scheduler/enqueued")
    except SerializationError:
      logger.exception("Failed to serialize request for enqueue")
      if self.stats:
        self.stats.inc_value("scheduler/serialization_errors")
      return False
    except (QueueError, BackendError):
      if phase == "dedup":
        # Dedup-backend outage: degrade to enqueue (don't lose the URL),
        # attribute to the dedup-error stat.
        logger.exception("Failed to consult dupefilter; defaulting to enqueue")
        if self.stats:
          self.stats.inc_value("scheduler/dupefilter_error")
        try:
          self._queue.push(request, priority=priority)
          if self.stats:
            self.stats.inc_value("scheduler/enqueued")
        except (QueueError, SerializationError, BackendError):
          logger.exception("Failed to enqueue request after dedup outage")
          return False
        return True
      # phase == "push": a plain queue-push failure (not a dedup outage).
      logger.exception("Failed to enqueue request")
      if self.stats:
        self.stats.inc_value("scheduler/queue_error")
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
