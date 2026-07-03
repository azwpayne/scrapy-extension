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
from scrapy_extension.backends.connectors import (
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

  Backpressure depth gate (round-4, BP-2):

  When ``backpressure_pause_at`` is set (not None), ``next_request`` returns
  ``None`` once the queue depth reaches ``pause_at`` (depth source:
  ``len(self._queue)``, fresh — same source ``has_pending_requests`` trusts).
  Popping resumes only after depth drains to ``resume_at`` (hysteresis,
  prevents flapping). ``resume_at`` defaults to ``pause_at`` when unset (no
  hysteresis — single threshold). The gate bumps two additive stats:
  ``scheduler/backpressure_pause`` and ``scheduler/backpressure_resume``.
  Default-off (``pause_at is None``) → byte-identical behavior to the pre-fix
  pop path. A ``QueueError`` / ``NotImplementedError`` from ``len(self._queue)``
  propagates to the existing ``next_request`` ``except QueueError`` arm and
  returns ``None`` (degraded safely; the pause flag is never left stuck).

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
    *,
    backpressure_pause_at: int | None = None,
    backpressure_resume_at: int | None = None,
    queue_depth_sample_every: int = 100,
    queue_max_item_bytes: int = 1_048_576,
    monitor_backpressure_threshold: int = 1_000,
    monitor_pop_rate_window_s: float = 60.0,
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
        backpressure_pause_at: Optional depth threshold — at and above this
            depth, ``next_request`` returns None (depth read fresh from
            ``len(self._queue)``). ``None`` (default) disables the gate
            (byte-identical to prior behavior).
        backpressure_resume_at: Optional resume threshold — depth must drain
            to this value before popping resumes (hysteresis). When ``None``
            and ``backpressure_pause_at`` is set, defaults to ``pause_at``
            (single-threshold, no hysteresis).
        queue_depth_sample_every: Round-14 R14-C — U4 depth-probe sampling
            window forwarded to ``BackendQueue(depth_sample_every=…)`` in
            ``open()``. Default ``100`` (U4 default).
        queue_max_item_bytes: Round-14 R14-C — D2 per-item serialized-byte cap
            forwarded to ``BackendQueue(max_item_bytes=…)`` in ``open()``.
            Default 1 MiB (matches Memcached ceiling).
        monitor_backpressure_threshold: Round-14 R14-C — U2 depth above which
            ``queue/backpressure`` flips on. Forwarded to the resolved
            ``ScrapyStatsMonitor`` in ``open()``. Default ``1_000`` (U2).
        monitor_pop_rate_window_s: Round-14 R14-C — U2 trailing window
            (seconds) for the ``queue/pop_rate`` gauge. Forwarded to both
            ``BackendQueue(pop_rate_window_s=…)`` and the resolved monitor
            in ``open()``. Default ``60.0`` (U2).
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
    # Backpressure gate config (round-4 BP-2). resume_at defaults to pause_at
    # (single-threshold) when unset — computed once here, not per-call.
    self._pause_at = backpressure_pause_at
    self._resume_at = (
      backpressure_resume_at
      if backpressure_resume_at is not None
      else backpressure_pause_at
    )
    # Per-spider paused state; reset on open(spider).
    self._backpressure_paused: bool = False
    # R14-C operability knobs — carried from from_settings → open() so the
    # BackendQueue / strategy / monitor constructors receive them. Pre-R14-C
    # these were stuck at constructor defaults (the settings existed only in
    # the runbook's "tune via settings" hand-wave). See ``open()`` for the
    # threading site.
    self._queue_depth_sample_every = queue_depth_sample_every
    self._queue_max_item_bytes = queue_max_item_bytes
    self._monitor_backpressure_threshold = monitor_backpressure_threshold
    self._monitor_pop_rate_window_s = monitor_pop_rate_window_s

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
      required_capabilities={"queue"},
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
    # R14-C: read the delay-strategy max_held knob (round-9 U5). Unset → None
    # → build_queue_strategy falls back to the DelayQueueStrategy constructor
    # default (100_000). Read via get(...) + int() to mirror the
    # backpressure-threshold pattern (some Scrapy versions return 0 on unset
    # via getint, colliding with a future 0-meaning config).
    delay_max_held_raw = settings.get("SCRAPY_QUEUE_DELAY_MAX_HELD")
    delay_max_held = (
      int(delay_max_held_raw) if delay_max_held_raw not in (None, "") else None
    )
    queue_strategy = build_queue_strategy(
      strategy_type,
      manager,
      default_delay=settings.getfloat("SCRAPY_QUEUE_DELAY_DEFAULT", 0.0),
      min_interval=settings.getfloat("SCRAPY_QUEUE_THROTTLE_MIN_INTERVAL", 0.0),
      max_held=delay_max_held,
    )
    # Backpressure gate (round-4 BP-2). Read via settings.get(...) + int() —
    # NOT getint (some Scrapy versions return 0 on unset, which would collide
    # with a future 0-meaning config; the codebase pattern for optional ints
    # here is int(settings.get(...)) — see CONCURRENT_REQUESTS at line ~220).
    # Unset/0/None → treat as "off" (pause_at=None, gate disabled).
    pause_raw = settings.get("SCRAPY_BACKPRESSURE_PAUSE_AT")
    resume_raw = settings.get("SCRAPY_BACKPRESSURE_RESUME_AT")
    pause_at = int(pause_raw) if pause_raw not in (None, "") else None
    resume_at = int(resume_raw) if resume_raw not in (None, "") else None
    # R14-C operability knobs (round-9 U4 depth-sample + D2 max-item-bytes +
    # round-12 U2 backpressure-threshold + pop-rate-window). Read via get(...)
    # + int()/float() — same optional-with-default pattern as the BP knobs.
    # These are non-optional in the constructor (they always have a default),
    # so unset → falls through to the constructor default via the explicit
    # default arg here.
    depth_sample_every = int(
      settings.get("SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY", 100)
    )
    queue_max_item_bytes = int(
      settings.get("SCRAPY_QUEUE_MAX_ITEM_BYTES", 1_048_576)
    )
    monitor_backpressure_threshold = int(
      settings.get("SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD", 1_000)
    )
    monitor_pop_rate_window_s = float(
      settings.get("SCRAPY_MONITOR_POP_RATE_WINDOW_S", 60.0)
    )
    return cls(
      connection_manager=manager,
      queue_key=settings.get("SCRAPY_QUEUE_KEY", "scheduler:queue"),
      queue_strategy=queue_strategy,
      backpressure_pause_at=pause_at,
      backpressure_resume_at=resume_at,
      queue_depth_sample_every=depth_sample_every,
      queue_max_item_bytes=queue_max_item_bytes,
      monitor_backpressure_threshold=monitor_backpressure_threshold,
      monitor_pop_rate_window_s=monitor_pop_rate_window_s,
    )

  @staticmethod
  def _resolve_monitor_for_spider(
    spider: Spider,
    *,
    backpressure_threshold: int,
    pop_rate_window_s: float,
  ) -> Any:
    """Resolve a ScrapyStatsMonitor threaded with the R14-C U2 knobs.

    Pre-R14-C the ``BackendQueue`` resolved its own monitor internally with
    constructor defaults, so the operator-tuned ``SCRAPY_MONITOR_*`` settings
    could never reach it. R14-C moves monitor resolution to the scheduler
    (which holds the threaded values) and forwards the monitor into
    ``BackendQueue`` explicitly, so the U2 ``backpressure_threshold`` +
    ``pop_rate_window_s`` knobs take effect.

    Falls back to ``NullMonitor`` when ``spider.crawler.stats`` is unreachable
    (no spider, no crawler, or no stats — e.g. unit-test spiders), mirroring
    ``BackendQueue._resolve_monitor``.

    Args:
        spider: The spider to resolve a stats collector from.
        backpressure_threshold: Depth above which ``queue/backpressure``
            flips on (forwarded to ``ScrapyStatsMonitor``).
        pop_rate_window_s: Trailing window for ``queue/pop_rate`` (forwarded
            to ``ScrapyStatsMonitor``).

    Returns:
        A ``ScrapyStatsMonitor`` if ``spider.crawler.stats`` is reachable,
        else a ``NullMonitor``.
    """
    from scrapy_extension.monitor import NullMonitor, ScrapyStatsMonitor

    crawler = getattr(spider, "crawler", None)
    stats = getattr(crawler, "stats", None) if crawler is not None else None
    if stats is None:
      return NullMonitor()
    return ScrapyStatsMonitor(
      stats,
      backpressure_threshold=backpressure_threshold,
      pop_rate_window_s=pop_rate_window_s,
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
    from scrapy_extension.backends.connectors import _load_object
    from scrapy_extension.backends.registry import get_descriptor

    descriptor = get_descriptor(str(backend_type))
    backend_cls = _load_object(descriptor.backend_cls_path)
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
    # ``backend_type`` is the registry-key string; format it bare (no repr
    # quoting) so the message reads naturally for both BackendType members
    # and plain strings.
    bt_name = (
      backend_type.value if isinstance(backend_type, BackendType) else backend_type
    )
    msg = (
      f"Backend {bt_name!r} requires explicit ack but does NOT "
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
    # R14-C: resolve the monitor FIRST so it can be threaded into BackendQueue
    # with the operator-tuned backpressure_threshold + pop_rate_window_s.
    # Pre-R14-C the BackendQueue resolved its own monitor internally (default
    # ScrapyStatsMonitor with constructor defaults) — but that path could not
    # see the SCRAPY_MONITOR_* settings, so the U2 knobs were stuck at
    # defaults. Resolving here + passing explicitly closes the loop.
    monitor = BackendScheduler._resolve_monitor_for_spider(
      spider,
      backpressure_threshold=self._monitor_backpressure_threshold,
      pop_rate_window_s=self._monitor_pop_rate_window_s,
    )
    # R14-D follow-up: thread the resolved monitor into the ConnectionManager
    # so the connection-lifecycle hooks (on_connect/on_disconnect/on_retry →
    # backend/{connect,disconnect,retry}_count) actually fire in production.
    # Without this, ConnectionManager defaults to NullMonitor and the hooks
    # R14-D wired are dead observability outside the queue path.
    self.connection_manager.set_monitor(monitor)
    self._queue = BackendQueue(
      connection_manager=self.connection_manager,
      queue_name=self.queue_key,
      spider=spider,
      queue_strategy=self._queue_strategy,
      max_item_bytes=self._queue_max_item_bytes,
      monitor=monitor,
      depth_sample_every=self._queue_depth_sample_every,
      pop_rate_window_s=self._monitor_pop_rate_window_s,
    )
    self._connect_ack_signals(spider)
    # Reset backpressure gate for a clean per-spider start (round-4 BP-2).
    # A prior paused state (e.g. re-open without close) should not leak in.
    self._backpressure_paused = False
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
        The next request, or None if the queue is empty or paused under the
        backpressure gate.
    """
    try:
      if self._queue is None:
        msg = "Scheduler not opened"
        raise RuntimeError(msg)
      # Backpressure depth gate (round-4 BP-2). Depth source is
      # len(self._queue) — fresh, same source has_pending_requests trusts.
      # ``len()`` raising QueueError/NotImplementedError propagates to the
      # ``except QueueError`` arm below (degraded safely, no stuck pause flag).
      if self._pause_at is not None:
        # Read depth once. len() can raise QueueError, or NotImplementedError
        # on backends whose queue_len is unsupported (e.g. RocketMQ). On either,
        # the gate can't read depth → skip it (degrade to pop) rather than
        # crash or stall — matches has_pending_requests' error handling.
        try:
          depth = len(self._queue)
        except (QueueError, NotImplementedError):
          depth = None
        if depth is not None:
          # _resume_at defaults to _pause_at in __init__, so it is non-None
          # whenever _pause_at is non-None; bind a narrowed local for the type
          # checker (the attribute itself stays int | None).
          resume_at = self._resume_at
          # bandit B101 accepted — type-checker narrowing (_resume_at
          # defaults to _pause_at in __init__, so non-None here), not a
          # security control.
          assert resume_at is not None  # nosec B101
          if not self._backpressure_paused and depth >= self._pause_at:
            self._backpressure_paused = True
            if self.stats:
              self.stats.inc_value("scheduler/backpressure_pause")
          if self._backpressure_paused:
            if depth <= resume_at:
              self._backpressure_paused = False
              if self.stats:
                self.stats.inc_value("scheduler/backpressure_resume")
            else:
              return None  # paused — Scrapy engine re-polls after backoff
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
