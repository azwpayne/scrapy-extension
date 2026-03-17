"""Pipeline component for scrapy-extension.

This module provides a Scrapy item pipeline using backend storage interfaces.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from scrapy.settings import Settings

from scrapy_extension.backends.base import JSONSerializer

if TYPE_CHECKING:
  from scrapy import Item, Spider
  from scrapy.crawler import Crawler

  from scrapy_extension.connection.manager import ConnectionManager

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
    self._serializer = JSONSerializer()

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendPipeline:
    """Create pipeline from Scrapy settings.

    Args:
        settings: Scrapy settings object.

    Returns:
        A new BackendPipeline instance.
    """
    from scrapy_extension.connection.manager import ConnectionManager
    from scrapy_extension.backends.base import BackendType

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
    """Called when spider opens.

    Args:
        spider: The spider instance.
    """
    logger.info(f"Pipeline opened for spider {spider.name}")

  def close_spider(self, spider: Spider) -> None:
    """Called when spider closes.

    Args:
        spider: The spider instance.
    """
    logger.info(f"Pipeline closed for spider {spider.name}")

  def process_item(self, item: Item, spider: Spider) -> Item:
    """Process and store an item.

    Args:
        item: The item to process.
        spider: The spider instance.

    Returns:
        The processed item.
    """
    # Generate unique key
    timestamp = datetime.utcnow().isoformat()
    unique_id = uuid.uuid4().hex[:8]
    key = f"{self.key_prefix}:{spider.name}:{timestamp}:{unique_id}"

    # Serialize item
    item_dict = dict(item) if hasattr(item, "__iter__") else {"data": str(item)}
    data = self._serializer.serialize(item_dict)

    # Store in backend
    self.connection_manager.get_storage_backend().store(key, data, ttl=self.ttl)

    logger.debug(f"Stored item: {key}")
    return item
