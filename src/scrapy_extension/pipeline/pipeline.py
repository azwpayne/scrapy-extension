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
from scrapy_extension.exceptions import SerializationError
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
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    key_prefix: str = "items",
    ttl: int | None = None,
    max_item_bytes: int = DEFAULT_PIPELINE_MAX_ITEM_BYTES,
    storage_strategy: StorageStrategy | None = None,
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
    """
    self.connection_manager = connection_manager
    self.key_prefix = key_prefix
    self.ttl = ttl
    self.max_item_bytes = max_item_bytes
    self.storage_strategy: StorageStrategy = (
      storage_strategy if storage_strategy is not None else PassthroughStorageStrategy()
    )
    self._storage_supported: bool | None = None

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
      STORAGE_CAPABLE_BACKENDS,
      ConnectionManager,
      resolve_backend_config,
    )

    backend_type, backend_settings = resolve_backend_config(
      settings,
      type_key="SCRAPY_STORAGE_BACKEND_TYPE",
      settings_key="SCRAPY_STORAGE_BACKEND_SETTINGS",
      required_capabilities=STORAGE_CAPABLE_BACKENDS,
      component_name="storage",
    )
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=backend_settings,
    )
    storage_strategy_name = settings.get("SCRAPY_STORAGE_STRATEGY", "passthrough")
    storage_strategy = create_storage_strategy(storage_strategy_name)
    return cls(
      connection_manager=manager,
      key_prefix=settings.get("SCRAPY_PIPELINE_KEY_PREFIX", "items"),
      ttl=settings.getint("SCRAPY_PIPELINE_TTL", 0) or None,
      max_item_bytes=settings.getint(
        "SCRAPY_PIPELINE_MAX_ITEM_BYTES", DEFAULT_PIPELINE_MAX_ITEM_BYTES
      ),
      storage_strategy=storage_strategy,
    )

  @classmethod
  def from_crawler(cls, crawler: Crawler) -> BackendPipeline:
    """Create pipeline from crawler.

    Args:
        crawler: The Scrapy crawler instance.

    Returns:
        A new BackendPipeline instance.
    """
    return cls.from_settings(crawler.settings)

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
        self.connection_manager.backend_type.value,
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
      self._inc_stat(spider, "pipeline/oversize_dropped")
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
      return item
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
