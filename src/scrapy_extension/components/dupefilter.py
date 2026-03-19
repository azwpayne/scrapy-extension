"""Duplicate filter component for scrapy-extension.

This module provides a Scrapy dupefilter component using backend set interfaces.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from scrapy_extension.utils.request import request_fingerprint

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.crawler import Crawler
  from scrapy.http import Request
  from scrapy.settings import Settings

  from scrapy_extension.connection.manager import ConnectionManager

logger = logging.getLogger(__name__)


class BackendDupeFilter:
  """Scrapy duplicate filter using backend set interface.

  This dupefilter uses a SetBackend to store request fingerprints
  and filter out duplicate requests.

  Attributes:
      connection_manager: The connection manager for backend access.
      key: The key for the fingerprints set.
      debug: Whether to log filtered requests.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    key: str = "dupefilter",
    *,
    debug: bool = False,
  ) -> None:
    """Initialize the dupefilter.

    Args:
        connection_manager: Connection manager for backend access.
        key: Key for the fingerprints set.
        debug: Whether to log filtered requests.
    """
    self.connection_manager = connection_manager
    self.key = key
    self.debug = debug

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendDupeFilter:
    """Create dupefilter from Scrapy settings.

    Args:
        settings: Scrapy settings object.

    Returns:
        A new BackendDupeFilter instance.
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
      key=settings.get("SCRAPY_DUPEFILTER_KEY", "dupefilter"),
      debug=settings.getbool("DUPEFILTER_DEBUG", default=False),
    )

  @classmethod
  def from_crawler(cls, crawler: Crawler) -> BackendDupeFilter:
    """Create dupefilter from crawler.

    Args:
        crawler: The Scrapy crawler instance.

    Returns:
        A new BackendDupeFilter instance.
    """
    return cls.from_settings(crawler.settings)

  def open(self) -> None:
    """Open the dupefilter (no-op for backend-based)."""

  def close(self, reason: str) -> None:
    """Close the dupefilter (no-op for backend-based).

    Args:
        reason: The reason for closing.
    """

  def log(self, request: Request, spider: Spider) -> None:
    """Log a filtered request.

    Args:
        request: The filtered request.
        spider: The spider instance.
    """
    if self.debug:
      logger.debug(
        "Filtered duplicate request: %s",
        request.url,
        extra={"spider": spider},
      )

  def request_seen(self, request: Request) -> bool:
    """Check if a request has been seen before.

    Args:
        request: The request to check.

    Returns:
        True if the request is a duplicate, False otherwise.
    """
    fingerprint = self.request_fingerprint(request)
    set_backend = self.connection_manager.get_set_backend()

    # Use atomic add — return True (duplicate) if item already existed
    added = set_backend.add(self.key, fingerprint.encode())
    return not added

  def request_fingerprint(self, request: Request) -> str:
    """Generate a fingerprint for a request.

    Args:
        request: The request to fingerprint.

    Returns:
        A unique fingerprint string.
    """
    return request_fingerprint(request)
