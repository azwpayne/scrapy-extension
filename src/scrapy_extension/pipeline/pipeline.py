"""Pipeline component for scrapy-extension.

This module provides a Scrapy item pipeline using backend storage interfaces.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.exceptions import BackendError, SerializationError
from scrapy_extension.monitor.base import Monitor, NullMonitor
from scrapy_extension.storage.strategies import (
  StorageStrategy,
  create_storage_strategy,
)
from scrapy_extension.storage.strategies.passthrough import (
  PassthroughStorageStrategy,
)

if TYPE_CHECKING:
  from scrapy import Item, Spider
  from scrapy.crawler import Crawler
  from scrapy.settings import Settings

  from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)

#: Default per-item serialized-byte cap (1 MiB — matches Memcached's 1 MB ceiling).
DEFAULT_PIPELINE_MAX_ITEM_BYTES = 1_048_576

#: Risk 5 — default ceiling on consecutive storage errors before the pipeline
#: re-raises (wrapped as BackendError) instead of swallowing forever. The
#: pre-Risk-5 from_settings default was ``None`` (infinite swallow), which meant
#: a persistent storage outage was silently absorbed as success-shaped item
#: returns — silent data loss at fleet scale. ``10`` surfaces a sustained
#: outage loudly after 11 consecutive failures while tolerating transient
#: blips. Operators who want the old infinite-swallow behavior can pass
#: ``max_storage_errors=None`` to the constructor directly.
DEFAULT_MAX_STORAGE_ERRORS = 10


class BackendPipeline:
  """Scrapy item pipeline using backend storage interface.

  This pipeline stores items in the backend storage with optional TTL.

  Attributes:
      connection_manager: The connection manager for backend access.
      key_prefix: Prefix for stored item keys.
      ttl: Optional TTL in seconds for items.
      serializer: Serializer for item encoding.
      storage_strategy: Strategy layer governing how items reach the backend
          (passthrough default — byte-identical to pre-strategy behavior;
          ``batched`` buffers + flushes on threshold/close).
      max_storage_errors: C2 escalation threshold. ``None`` (default) keeps the
          best-effort swallow-and-stat behavior; an int N re-raises the storage
          error wrapped as :class:`~scrapy_extension.exceptions.BackendError`
          once consecutive failures exceed N (counter resets on a successful
          store).
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    key_prefix: str = "items",
    ttl: int | None = None,
    max_item_bytes: int = DEFAULT_PIPELINE_MAX_ITEM_BYTES,
    storage_strategy: StorageStrategy | None = None,
    *,
    max_storage_errors: int | None = None,
    monitor: Monitor | None = None,
  ) -> None:
    """Initialize the pipeline.

    Args:
        connection_manager: Connection manager for backend access.
        key_prefix: Prefix for stored item keys.
        ttl: Optional TTL in seconds for items.
        max_item_bytes: Maximum serialized bytes permitted for a single stored
            item. Oversize payloads raise ``SerializationError`` at store time
            (D2 — DoS guard against capped storage backends like Memcached
            1 MB, DynamoDB 400 KB).
        storage_strategy: Persistence strategy. ``None`` defaults to
            :class:`PassthroughStorageStrategy` (byte-identical to the
            pre-strategy store call). Selected via ``SCRAPY_STORAGE_STRATEGY``
            in :meth:`from_settings`.
        max_storage_errors: C2 escalation. ``None`` (default) preserves the
            best-effort behavior (storage errors swallowed, item returned,
            ``pipeline/storage_errors`` stat incremented). When set to N, the
            pipeline tracks consecutive storage failures and re-raises the
            error wrapped as :class:`~scrapy_extension.exceptions.BackendError`
            once the consecutive count exceeds N — surfacing a persistent
            storage outage loudly instead of silently reporting success.
            Counter resets to 0 on every successful store.
        monitor: Optional observability monitor. When ``None`` (default),
            :class:`~scrapy_extension.monitor.base.NullMonitor` (no-op). Wired
            to a :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` in
            :meth:`from_crawler` when ``crawler.stats`` is available, so the
            ``pipeline/store_count`` stat is default-on. Emitted hooks are
            additive — existing component stats untouched.
    """
    self.connection_manager = connection_manager
    self.key_prefix = key_prefix
    self.ttl = ttl
    self.max_item_bytes = max_item_bytes
    self.storage_strategy: StorageStrategy = (
      storage_strategy if storage_strategy is not None else PassthroughStorageStrategy()
    )
    self.max_storage_errors = max_storage_errors
    self._consecutive_storage_errors = 0
    self._storage_supported: bool | None = None
    self._monitor: Monitor = monitor if monitor is not None else NullMonitor()

  @cached_property
  def _serializer(self) -> JSONSerializer:
    """Lazy-initialized JSON serializer."""
    return JSONSerializer()

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendPipeline:
    """Create pipeline from Scrapy settings.

    Backend selection: ``SCRAPY_STORAGE_BACKEND_TYPE`` /
    ``SCRAPY_STORAGE_BACKEND_SETTINGS`` override the global
    ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so item storage
    can bind to a different backend than the queue or dedup set
    (multi-backend coexistence). Unset → falls back to the global keys.

    Args:
        settings: Scrapy settings object.

    Returns:
        A new BackendPipeline instance.
    """
    from scrapy_extension.backends.connectors import (
      ConnectionManager,
      resolve_backend_config,
    )

    backend_type, backend_settings = resolve_backend_config(
      settings,
      type_key="SCRAPY_STORAGE_BACKEND_TYPE",
      settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
      required_capabilities={"storage"},
      component_name="storage",
    )
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=backend_settings,
    )
    storage_strategy_name = settings.get("SCRAPY_STORAGE_STRATEGY", "passthrough")
    # Risk 2: thread the crash-before-flush loss-window cap through to the
    # batched strategy (None = disabled = pre-Risk-2 behavior; passthrough
    # ignores it).
    raw_age = settings.get("SCRAPY_STORAGE_BUFFER_MAX_AGE_S")
    buffer_max_age_s = float(raw_age) if raw_age is not None else None
    storage_strategy = create_storage_strategy(
      storage_strategy_name, max_buffer_age_s=buffer_max_age_s
    )
    raw_max_errors = settings.get("SCRAPY_PIPELINE_MAX_STORAGE_ERRORS")
    # Risk 5: default to a sane ceiling (DEFAULT_MAX_STORAGE_ERRORS) when unset
    # so a persistent outage surfaces instead of being silently swallowed. An
    # explicit int from settings still wins; ``None`` (infinite swallow) is
    # reachable only via the constructor kwarg, not via the env var.
    max_storage_errors = (
      int(raw_max_errors)
      if raw_max_errors is not None
      else DEFAULT_MAX_STORAGE_ERRORS
    )
    return cls(
      connection_manager=manager,
      key_prefix=settings.get("SCRAPY_PIPELINE_KEY_PREFIX", "items"),
      ttl=settings.getint("SCRAPY_PIPELINE_TTL", 0) or None,
      max_item_bytes=settings.getint(
        "SCRAPY_PIPELINE_MAX_ITEM_BYTES", DEFAULT_PIPELINE_MAX_ITEM_BYTES
      ),
      storage_strategy=storage_strategy,
      max_storage_errors=max_storage_errors,
    )

  @classmethod
  def from_crawler(cls, crawler: Crawler) -> BackendPipeline:
    """Create pipeline from crawler.

    Default-on observability: wires a
    :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` when
    ``crawler.stats`` is available so the ``pipeline/store_count`` stat shows
    up on the Scrapy stats dump without an explicit ``monitor=`` kwarg.
    Additive — existing stats (``pipeline/storage_errors`` etc.) untouched.

    Args:
        crawler: The Scrapy crawler instance.

    Returns:
        A new BackendPipeline instance.
    """
    pipeline = cls.from_settings(crawler.settings)
    # Default-on observability — mirrors the dupefilter wiring. Only override
    # when no explicit monitor was provided (operators passing a custom monitor
    # via from_settings win over the default).
    stats = getattr(crawler, "stats", None)
    if stats is not None and isinstance(pipeline._monitor, NullMonitor):
      from scrapy_extension.monitor import ScrapyStatsMonitor

      pipeline._monitor = ScrapyStatsMonitor(stats)
    # Risk 2: share the pipeline's monitor with the storage strategy so
    # ``on_buffer_depth`` (batched) emits through the same collector. No-op
    # for strategies without a ``set_monitor`` hook (passthrough).
    set_monitor = getattr(pipeline.storage_strategy, "set_monitor", None)
    if callable(set_monitor):
      set_monitor(pipeline._monitor)
    return pipeline

  def open_spider(self, spider: Spider) -> None:
    """Called when a spider opens.

    Detects whether the configured backend supports storage. If not
    (Kafka, RabbitMQ, RocketMQ), the pipeline degrades to a no-op and
    logs a warning so the operator knows items aren't being persisted.

    Args:
        spider: The spider instance.
    """
    try:
      self.connection_manager.get_storage_backend()
      self._storage_supported = True
    except NotImplementedError:
      self._storage_supported = False
      logger.warning(
        "Backend %s does not support storage. "
        "Pipeline will be a no-op — items will not be persisted.",
        self.connection_manager.backend_type,
      )
    logger.info("Pipeline opened for spider %s", spider.name)

  def close_spider(self, spider: Spider) -> None:
    """Called when a spider closes.

    Flushes any buffered items via the storage strategy before shutting the
    connection manager down (so batched strategies drain on spider close).

    Args:
        spider: The spider instance.
    """
    logger.info("Pipeline closed for spider %s", spider.name)
    self.storage_strategy.close()
    self.connection_manager.close()

  def process_item(self, item: Item, spider: Spider) -> Item:
    """Process and store an item.

    Best-effort: catches storage errors so a temporary backend failure
    doesn't kill the spider. The item is returned unchanged either way
    so downstream pipelines continue. Storage errors are logged and
    counted in spider stats.

    Args:
        item: The item to process.
        spider: The spider instance.

    Returns:
        The processed item (always).
    """
    if self._storage_supported is False:
      self._inc_stat(spider, "pipeline/storage_skipped")
      return item

    key = self._generate_item_key(spider)
    data = self._serialize_item(item)

    # D2: reject oversize payloads loudly (DoS guard). Unlike a transient
    # storage error (swallowed below to keep the spider alive), this is a
    # deterministic validation failure — surfacing it prevents the silent
    # drop that capped storage backends (Memcached 1 MB, DynamoDB 400 KB)
    # would otherwise cause.
    if len(data) > self.max_item_bytes:
      # Risk 5: renamed ``oversize_dropped`` → ``oversize_rejected`` (the item
      # is rejected+raised, not silently dropped — the old name misled). The
      # legacy key is still incremented for one release so existing dashboards
      # keep working (mirrors monitor/stats.py ``queue/pop_count`` aliasing).
      self._inc_stat(spider, "pipeline/oversize_dropped")
      self._inc_stat(spider, "pipeline/oversize_rejected")
      msg = (
        f"Serialized item ({len(data)} bytes) exceeds max_item_bytes "
        f"({self.max_item_bytes}). Rejecting store to avoid silent drop by "
        f"capped storage backends."
      )
      raise SerializationError(msg, data=item, serializer="json")

    try:
      self._store_item(key, data)
    except Exception as e:
      logger.warning(
        "Failed to store item %s: %s. Item will not be persisted.",
        key,
        e,
      )
      self._inc_stat(spider, "pipeline/storage_errors")
      # C2: opt-in loud-fail. Default (max_storage_errors=None) keeps the
      # best-effort swallow-and-stat behavior — zero compat break. When set,
      # track consecutive failures and re-raise once the count exceeds N so a
      # persistent storage outage surfaces instead of being silently absorbed
      # as a success-shaped item return.
      if self.max_storage_errors is not None:
        self._consecutive_storage_errors += 1
        if self._consecutive_storage_errors > self.max_storage_errors:
          raise BackendError(
            f"Pipeline exceeded max_storage_errors ({self.max_storage_errors}): "
            f"{self._consecutive_storage_errors} consecutive storage failures "
            f"(last error on key {key!r}: {e})"
          ) from e
      return item
    # Success: reset the consecutive counter and emit the on_store hook
    # (failure path does not emit — it has its own stat, pipeline/storage_errors).
    self._consecutive_storage_errors = 0
    self._monitor.on_store(key)
    logger.debug("Stored item: %s", key)
    return item

  @staticmethod
  def _inc_stat(spider: Spider, stat_name: str) -> None:
    """Increment a Scrapy stat, tolerating missing crawler/stats.

    Defensively chains ``spider.crawler.stats`` via ``getattr`` because
    legacy spider classes (or test doubles) may not expose ``crawler``.
    Silent skip when the chain is broken — the spider continues either
    way; a missing counter is preferable to crashing the pipeline.

    Args:
        spider: The spider instance (must have ``crawler.stats`` for the
            stat to be recorded).
        stat_name: The Scrapy stats key to increment.
    """
    crawler = getattr(spider, "crawler", None)
    stats = getattr(crawler, "stats", None) if crawler is not None else None
    if stats is not None:
      stats.inc_value(stat_name)


  def _generate_item_key(self, spider: Spider) -> str:
    """Generate a unique key for the item.

    Args:
        spider: The spider instance.

    Returns:
        A unique storage key.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    unique_id = uuid.uuid4().hex[:8]
    return f"{self.key_prefix}:{spider.name}:{timestamp}:{unique_id}"

  def _serialize_item(self, item: Item) -> bytes:
    """Serialize an item.

    Args:
        item: The item to serialize.

    Returns:
        Serialized item bytes.
    """
    item_dict = dict(item) if hasattr(item, "__iter__") else {"data": str(item)}
    return self._serializer.serialize(item_dict)

  def _store_item(self, key: str, data: bytes) -> None:
    """Store serialized item via the configured storage strategy.

    The default :class:`PassthroughStorageStrategy` delegates straight to
    ``storage_backend.store(key, data, ttl=self.ttl)`` — byte-identical to the
    pre-strategy behavior. Batched strategies buffer the item and flush later.

    Args:
        key: Storage key.
        data: Serialized item data.
    """
    self.storage_strategy.store(
      self.connection_manager.get_storage_backend(),
      key,
      data,
      ttl=self.ttl,
    )
