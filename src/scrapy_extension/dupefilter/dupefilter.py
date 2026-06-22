"""Duplicate filter component for scrapy-extension.

This module provides a Scrapy dupefilter component using backend set interfaces.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol

from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter
from scrapy_extension.utils.request import request_fingerprint

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.crawler import Crawler
  from scrapy.http import Request
  from scrapy.settings import Settings

  from scrapy_extension.backends.connectors import ConnectionManager

  class _Fingerprinter(Protocol):
    """Duck type for Scrapy's request fingerprinter.

    Mirrors ``scrapy.http.request.RequestFingerprinter`` (and any custom
    ``REQUEST_FINGERPRINTER_CLASS``) — the ``fingerprint(request) -> bytes``
    contract. Used so ``BackendDupeFilter`` can honor a configured custom
    fingerprinter instead of always defaulting to the module function.
    """

    def fingerprint(self, request: Request) -> bytes: ...

logger = logging.getLogger(__name__)


class BackendDupeFilter:
  """Scrapy duplicate filter using a pluggable membership-filter strategy.

  Delegates duplicate detection to a
  :class:`~scrapy_extension.dupefilter.filters.base.MembershipFilter`. The
  default strategy is ``SetMembershipFilter`` (exact, cross-worker,
  byte-identical to the previous hardcoded ``SetBackend`` behavior); other
  strategies (memory, bloom, cuckoo) are selected via ``SCRAPY_DEDUP_STRATEGY``
  (wired in ``from_settings``).

  Attributes:
      connection_manager: The connection manager for backend access.
      key: The key for the fingerprints set / filter scope.
      debug: Whether to log filtered requests.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    key: str = "dupefilter",
    *,
    debug: bool = False,
    fingerprinter: _Fingerprinter | None = None,
    membership_filter: MembershipFilter | None = None,
  ) -> None:
    """Initialize the dupefilter.

    Args:
        connection_manager: Connection manager for backend access.
        key: Key for the fingerprints set / filter scope.
        debug: Whether to log filtered requests.
        fingerprinter: Optional Scrapy request fingerprinter. When provided
            (normally threaded from ``crawler.request_fingerprinter`` via
            ``from_crawler``), fingerprints respect a configured
            ``REQUEST_FINGERPRINTER_CLASS``. When ``None``, falls back to
            ``scrapy.utils.request.fingerprint`` — which is byte-identical to
            the default fingerprinter, so omitting this is fully backward-
            compatible (verified R45).
        membership_filter: Optional membership-filter strategy. When ``None``
            (default), a ``SetMembershipFilter`` is built from the connection
            manager and key — preserving the pre-strategy behavior exactly.
            Pass a custom filter (memory, bloom, cuckoo, ...) to override.
    """
    self.connection_manager = connection_manager
    self.key = key
    self.debug = debug
    self._fingerprinter = fingerprinter
    # Use ``is None`` (not ``or``): a MembershipFilter defines __len__, so an
    # empty filter (len == 0) would be falsy and ``or`` would wrongly discard
    # it. Only fall through when no filter was supplied at all.
    self._filter: MembershipFilter = (
      membership_filter
      if membership_filter is not None
      else SetMembershipFilter(connection_manager, key)
    )

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendDupeFilter:
    """Create dupefilter from Scrapy settings.

    Backend selection: ``SCRAPY_SET_BACKEND_TYPE`` /
    ``SCRAPY_SET_BACKEND_SETTINGS`` override the global
    ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so the dedup set
    can bind to a different backend than the queue or storage pipeline
    (multi-backend coexistence). Unset → falls back to the global keys.

    Args:
        settings: Scrapy settings object.

    Returns:
        A new BackendDupeFilter instance.
    """
    from scrapy_extension.backends.connectors import (
      ConnectionManager,
      resolve_backend_config,
    )
    from scrapy_extension.dupefilter.filters.factory import (
      DedupeStrategy,
      build_membership_filter,
    )

    backend_type, backend_settings = resolve_backend_config(
      settings,
      type_key="SCRAPY_SET_BACKEND_TYPE",
      settings_key="SCRAPY_SET_BACKEND_SETTINGS",
    )
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=backend_settings,
    )
    key = settings.get("SCRAPY_DUPEFILTER_KEY", "dupefilter")
    strategy = DedupeStrategy(
      settings.get("SCRAPY_DEDUP_STRATEGY", DedupeStrategy.SET.value)
    )
    membership_filter = build_membership_filter(
      strategy,
      manager,
      key=key,
      memory_maxsize=settings.get("SCRAPY_DEDUP_MEMORY_MAXSIZE"),
      bloom_capacity=settings.get("SCRAPY_DEDUP_BLOOM_CAPACITY", 1_000_000),
      bloom_error_rate=settings.get("SCRAPY_DEDUP_BLOOM_ERROR_RATE", 0.001),
      cuckoo_capacity=settings.get("SCRAPY_DEDUP_CUCKOO_CAPACITY", 1_000_000),
      cuckoo_error_rate=settings.get("SCRAPY_DEDUP_CUCKOO_ERROR_RATE", 0.001),
    )
    return cls(
      connection_manager=manager,
      key=key,
      debug=settings.getbool("DUPEFILTER_DEBUG", default=False),
      membership_filter=membership_filter,
    )

  @classmethod
  def from_crawler(cls, crawler: Crawler) -> BackendDupeFilter:
    """Create dupefilter from crawler.

    Threads ``crawler.request_fingerprinter`` so the dupefilter honors a
    configured ``REQUEST_FINGERPRINTER_CLASS`` (otherwise fingerprints are
    byte-identical to the default — see ``__init__``).

    Args:
        crawler: The Scrapy crawler instance.

    Returns:
        A new BackendDupeFilter instance.
    """
    dupefilter = cls.from_settings(crawler.settings)
    dupefilter._fingerprinter = getattr(crawler, "request_fingerprinter", None)
    return dupefilter

  def open(self) -> None:
    """Open the dupefilter and its membership filter (no-op for set)."""
    self._filter.open()

  def close(self, reason: str) -> None:
    """Close the dupefilter and its membership filter.

    Args:
        reason: The reason for closing.
    """
    self._filter.close()
    self.connection_manager.close()

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

    try:
      added = self._filter.add(fingerprint.encode())
    except NotImplementedError as exc:
      raise RuntimeError(
        "Configured backend does not support set/duplicate filtering; "
        "use a backend with SetBackend or disable BackendDupeFilter."
      ) from exc

    # add() returns True when the item was newly added; a duplicate maps to False.
    return not added

  def request_fingerprint(self, request: Request) -> str:
    """Generate a fingerprint for a request.

    Uses the configured Scrapy fingerprinter (``crawler.request_fingerprinter``)
    when one was provided via ``from_crawler``; otherwise falls back to
    ``scrapy.utils.request.fingerprint``. The two are byte-identical for the
    default fingerprinter (verified R45), so this only diverges when the
    operator has set a custom ``REQUEST_FINGERPRINTER_CLASS`` — exactly the
    case that should diverge.

    Args:
        request: The request to fingerprint.

    Returns:
        A unique fingerprint string (hex).
    """
    if self._fingerprinter is not None:
      return self._fingerprinter.fingerprint(request).hex()
    return request_fingerprint(request)
