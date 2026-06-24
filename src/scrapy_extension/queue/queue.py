"""Queue component for scrapy-extension.

This module provides a Scrapy queue component that uses backend queue interfaces.
"""

from __future__ import annotations

import base64
import binascii
import logging
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from scrapy.utils.request import request_from_dict

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.exceptions import SerializationError
from scrapy_extension.monitor import NullMonitor, ScrapyStatsMonitor
from scrapy_extension.monitor.base import Monitor
from scrapy_extension.queue.strategies.base import QueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.http import Request

  from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)

#: Default per-item serialized-byte cap (1 MiB — matches Memcached's 1 MB ceiling).
DEFAULT_QUEUE_MAX_ITEM_BYTES = 1_048_576


class BackendQueue:
  """Scrapy queue implementation using backend queue interface.

  This class wraps a QueueBackend to provide Scrapy-compatible
  queue operations for requests.

  Attributes:
      connection_manager: The connection manager for backend access.
      queue_name: The name of the queue.
      serializer: Serializer for encoding/decoding requests.
      spider: Optional spider reference for callback/errback resolution during deserialization.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    queue_name: str,
    *,
    spider: Spider | None = None,
    queue_strategy: QueueStrategy | None = None,
    max_item_bytes: int = DEFAULT_QUEUE_MAX_ITEM_BYTES,
    monitor: Monitor | None = None,
  ) -> None:
    """Initialize the backend queue.

    Args:
        connection_manager: Connection manager for backend access.
        queue_name: Name of the queue.
        spider: Optional spider reference for restoring callback/errback
            functions during request deserialization.
        queue_strategy: Optional queue-semantics strategy. When ``None``
            (default), a ``PassthroughQueueStrategy`` delegates push/pop to the
            QueueBackend unchanged — preserving the pre-strategy behavior.
        max_item_bytes: Maximum serialized bytes permitted for a single queued
            request. Oversize payloads raise ``SerializationError`` at push
            time (D2 — DoS guard against capped storage backends).
        monitor: Optional observability monitor. When ``None`` (default),
            resolved default-on: if ``spider.crawler.stats`` is reachable a
            :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` is wired;
            otherwise a :class:`~scrapy_extension.monitor.NullMonitor` (no-op,
            no crash). Emitted hooks are additive — existing stat keys are
            unchanged.
    """
    self.connection_manager = connection_manager
    self.queue_name = queue_name
    self._spider = spider
    self.max_item_bytes = max_item_bytes
    self._strategy: QueueStrategy = (
      queue_strategy
      if queue_strategy is not None
      else PassthroughQueueStrategy(connection_manager)
    )
    self._monitor: Monitor = monitor if monitor is not None else self._resolve_monitor(spider)

  @cached_property
  def _serializer(self) -> JSONSerializer:
    """Lazy-initialized JSON serializer."""
    return JSONSerializer()

  def _request_to_dict(self, request: Request) -> dict[str, Any]:
    """Convert a Request to a dictionary.

    The body is base64-encoded (pure ASCII) so binary POST bodies round-trip
    losslessly through JSON + UTF-8. The previous UTF-8/latin-1 fallback
    corrupted non-ASCII bodies because Scrapy's request_from_dict re-encodes
    the string as UTF-8 — different bytes than the original latin-1 decode.

    Args:
        request: The Request to convert.

    Returns:
        Dictionary representation of the request.
    """
    body_value = None
    if request.body:
      body_value = base64.b64encode(request.body).decode("ascii")

    return {
      "url": request.url,
      "callback": request.callback.__name__ if request.callback else None,
      "errback": request.errback.__name__ if request.errback else None,
      "method": request.method,
      "headers": dict(request.headers.to_unicode_dict()),
      "body": body_value,
      "cookies": request.cookies,
      "meta": request.meta,
      "cb_kwargs": request.cb_kwargs,
      "encoding": request.encoding,
      "priority": request.priority,
      "dont_filter": request.dont_filter,
      "flags": request.flags,
    }

  def push(self, request: Request, priority: float = 0.0) -> None:
    """Push a request to the queue.

    Args:
        request: The Scrapy request to push.
        priority: Priority of the request (higher = more urgent).

    Raises:
        SerializationError: If the request cannot be serialized.
    """
    try:
      request_dict = self._request_to_dict(request)
      data = self._serializer.serialize(request_dict)
    except Exception as e:
      msg = f"Failed to serialize request: {e}"
      raise SerializationError(
        msg,
        data=request,
        serializer="json",
      ) from e

    if len(data) > self.max_item_bytes:
      self._inc_stat("scheduler/queue/oversize_dropped")
      msg = (
        f"Serialized request ({len(data)} bytes) exceeds max_item_bytes "
        f"({self.max_item_bytes}). Rejecting push to avoid silent drop by "
        f"capped storage backends."
      )
      raise SerializationError(msg, data=request, serializer="json")

    delay = float(request.meta.get("delay") or 0.0)
    source = str(request.meta.get("source") or "default")
    self._strategy.push(
      self.queue_name, data, priority=priority, delay=delay, source=source
    )
    self._monitor.on_push(self.queue_name, priority)

  def pop(self, timeout: float = 0.0) -> Request | None:
    """Pop a request from the queue.

    Args:
        timeout: Seconds to wait for an item (0 = non-blocking).

    Returns:
        The popped request, or None if the queue is empty.

    Raises:
        SerializationError: If the request cannot be deserialized.
    """
    data = self._strategy.pop(self.queue_name, timeout)
    # Emit on every pop call — ``queue/pop_count`` is the consumer-liveness
    # signal (pop attempts per second), independent of whether an item was
    # returned. A worker popping an empty queue is itself operability signal.
    self._monitor.on_pop(self.queue_name)
    # Sample depth after each pop — this is the backpressure signal (architect's
    # #1 operability gap). Cheaper than a periodic timer and aligns the sample
    # with an event that already touched the backend. Guarded so a depth-sampling
    # failure can never break a successful pop.
    try:
      self._monitor.on_queue_depth(self.queue_name, self._strategy.queue_len(self.queue_name))
    except Exception:  # noqa: BLE001
      logger.debug("monitor.on_queue_depth raised; ignored", exc_info=True)

    if data is None:
      return None

    try:
      request_dict = cast("dict[str, Any]", self._serializer.deserialize(data))
      self._decode_body(request_dict)
      return request_from_dict(request_dict, spider=self._spider)
    except Exception as e:
      msg = f"Failed to deserialize request: {e}"
      raise SerializationError(
        msg,
        data=data,
        serializer="json",
      ) from e

  @staticmethod
  def _decode_body(request_dict: dict[str, Any]) -> None:
    """Decode base64 body back to bytes in-place.

    Reverses ``_request_to_dict``'s base64 encoding so Scrapy's
    ``request_from_dict`` receives raw bytes.

    Legacy migration (D1): pre-base64 package versions wrote raw UTF-8/latin-1
    bodies to the queue. On rolling upgrade those items would hit
    ``b64decode(validate=True)`` and raise, causing the scheduler to silently
    drop them. To preserve those items, a body that fails base64 validation
    but is valid UTF-8 is migrated to its UTF-8 bytes with a one-time
    ``DeprecationWarning``. Structural corruption (neither valid base64 nor
    valid UTF-8) still raises ``SerializationError``.

    Args:
        request_dict: The deserialized request dict to mutate.
    """
    body = request_dict.get("body")
    if body is None:
      return
    try:
      request_dict["body"] = base64.b64decode(body, validate=True)
    except (binascii.Error, ValueError):
      # D1: attempt legacy migration — pre-base64 bodies were raw UTF-8.
      if isinstance(body, str):
        try:
          legacy_bytes = body.encode("utf-8")
        except UnicodeEncodeError:
          legacy_bytes = None
      else:
        legacy_bytes = None
      if legacy_bytes is not None:
        warnings.warn(
          "legacy non-base64 queue body; will be unsupported after the "
          "next major. Re-queue the request with a current package version "
          "to migrate it.",
          DeprecationWarning,
          stacklevel=2,
        )
        request_dict["body"] = legacy_bytes
        return
      msg = "Invalid base64 body in queued request: body is not valid base64"
      raise SerializationError(msg, data=body, serializer="json")

  def __len__(self) -> int:
    """Get the number of requests in the queue.

    Returns:
        Number of requests.
    """
    return self._strategy.queue_len(self.queue_name)

  def clear(self) -> None:
    """Clear all requests from the queue."""
    self._strategy.clear(self.queue_name)

  def ack(self) -> None:
    """Acknowledge the last-popped request.

    Atomic backends (Redis, MongoDB, ElasticSearch, RocketMQ) implement
    this as a no-op. Message-queue backends (Kafka, RabbitMQ) commit the
    offset / ack the delivery so the message isn't re-delivered.

    Call after the spider has successfully processed the request popped
    from this queue. Wired automatically by ``BackendScheduler`` in a
    future round; for now, callers invoke explicitly.
    """
    self.connection_manager.get_queue_backend().ack(self.queue_name)

  def nack(self) -> None:
    """Negatively acknowledge the last-popped request.

    Atomic backends: no-op. Message-queue backends: requeue the message
    so another consumer (or this one, later) can retry.

    Call when the spider failed to process the request and you want it
    re-delivered.
    """
    self.connection_manager.get_queue_backend().nack(self.queue_name)

  def _inc_stat(self, stat_name: str) -> None:
    """Increment a Scrapy stat, tolerating missing spider/crawler/stats.

    Defensively chains ``self._spider.crawler.stats`` via ``getattr`` because
    the queue may be constructed without a spider (e.g. in tests) and legacy
    spiders may not expose ``crawler``. Silent skip when the chain is broken —
    the ``SerializationError`` already surfaced the condition; a missing
    counter is preferable to crashing the push path. Mirrors the pipeline's
    ``_inc_stat``.

    Args:
        stat_name: The Scrapy stats key to increment.
    """
    crawler = getattr(self._spider, "crawler", None) if self._spider else None
    stats = getattr(crawler, "stats", None) if crawler is not None else None
    if stats is not None:
      stats.inc_value(stat_name)

  @staticmethod
  def _resolve_monitor(spider: Spider | None) -> Monitor:
    """Default-on monitor resolution from a spider.

    When a spider is present and exposes ``crawler.stats``, wire a
    :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` so observability is
    on without an explicit ``monitor=`` kwarg. Otherwise (no spider, no
    crawler, or no stats) return a :class:`~scrapy_extension.monitor.NullMonitor`
    — the no-op default that never crashes a hook call.

    The ``getattr`` chain mirrors :meth:`_inc_stat`: the queue is often built
    without a spider (unit tests, ad-hoc use), and legacy spiders may not
    expose ``crawler``. Default-on where possible, safe everywhere else.

    Args:
        spider: Optional spider to resolve a stats collector from.

    Returns:
        A ``ScrapyStatsMonitor`` if ``spider.crawler.stats`` is reachable,
        else a ``NullMonitor``.
    """
    crawler = getattr(spider, "crawler", None) if spider is not None else None
    stats = getattr(crawler, "stats", None) if crawler is not None else None
    if stats is not None:
      return ScrapyStatsMonitor(stats)
    return NullMonitor()

  def close(self) -> None:
    """Close the queue, delegating to the queue strategy's lifecycle hook.

    Forwards to ``self._strategy.close()`` so strategies that hold in-process
    state (e.g. ``DelayQueueStrategy``'s held-item heap) can emit shutdown
    warnings / release resources. The backend connection itself is owned by
    the ``ConnectionManager`` and closed separately by the scheduler.

    Safe to call when no strategy lifecycle work is needed — the default
    ``QueueStrategy.close()`` is a no-op.
    """
    self._strategy.close()
