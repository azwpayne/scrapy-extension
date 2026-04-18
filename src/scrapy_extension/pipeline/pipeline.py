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

if TYPE_CHECKING:
  from scrapy import Item, Spider
  from scrapy.crawler import Crawler
  from scrapy.settings import Settings

  from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)


class BackendPipeline:
  """Scrapy item pipeline using backend storage interface.

  This pipeline stores items in the backend storage with optional TTL.

  Attributes:
      connection_manager: The connection manager for backend access.
      key_prefix: Prefix for stored item keys.
      ttl: Optional TTL in seconds for items.
      serializer: Serializer for item encoding.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    key_prefix: str = "items",
    ttl: int | None = None,
  ) -> None:
    """Initialize the pipeline.

    Args:
        connection_manager: Connection manager for backend access.
        key_prefix: Prefix for stored item keys.
        ttl: Optional TTL in seconds for items.
    """
    self.connection_manager = connection_manager
    self.key_prefix = key_prefix
    self.ttl = ttl

  @cached_property
  def _serializer(self) -> JSONSerializer:
    """Lazy-initialized JSON serializer."""
    return JSONSerializer()

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendPipeline:
    """Create pipeline from Scrapy settings.

    Args:
        settings: Scrapy settings object.

    Returns:
        A new BackendPipeline instance.
    """
    from scrapy_extension.backends.base import BackendType
    from scrapy_extension.backends.connectors import ConnectionManager

    backend_type = BackendType(settings.get("SCRAPY_BACKEND_TYPE", "redis"))
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=settings.getdict("SCRAPY_BACKEND_SETTINGS", {}),
    )
    return cls(
      connection_manager=manager,
      key_prefix=settings.get("SCRAPY_PIPELINE_KEY_PREFIX", "items"),
      ttl=settings.getint("SCRAPY_PIPELINE_TTL", 0) or None,
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

    Args:
        spider: The spider instance.
    """
    logger.info("Pipeline opened for spider %s", spider.name)

  def close_spider(self, spider: Spider) -> None:
    """Called when a spider closes.

    Args:
        spider: The spider instance.
    """
    logger.info("Pipeline closed for spider %s", spider.name)

  def process_item(self, item: Item, spider: Spider) -> Item:
    """Process and store an item.

    Args:
        item: The item to process.
        spider: The spider instance.

    Returns:
        The processed item.
    """
    key = self._generate_item_key(spider)
    data = self._serialize_item(item)
    self._store_item(key, data)
    logger.debug("Stored item: %s", key)
    return item

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
    """Store serialized item.

    Args:
        key: Storage key.
        data: Serialized item data.
    """
    self.connection_manager.get_storage_backend().store(key, data, ttl=self.ttl)
