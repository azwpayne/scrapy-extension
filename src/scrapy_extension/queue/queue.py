"""Queue component for scrapy-extension.

This module provides a Scrapy queue component that uses backend queue interfaces.
"""

from __future__ import annotations

import base64
import binascii
import logging
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from scrapy.utils.request import request_from_dict

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.exceptions import SerializationError
from scrapy_extension.queue.strategies.base import QueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.http import Request

  from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)


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
    """
    self.connection_manager = connection_manager
    self.queue_name = queue_name
    self._spider = spider
    self._strategy: QueueStrategy = (
      queue_strategy
      if queue_strategy is not None
      else PassthroughQueueStrategy(connection_manager)
    )

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

    delay = float(request.meta.get("delay") or 0.0)
    source = str(request.meta.get("source") or "default")
    self._strategy.push(
      self.queue_name, data, priority=priority, delay=delay, source=source
    )

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

    Args:
        request_dict: The deserialized request dict to mutate.
    """
    body = request_dict.get("body")
    if body is None:
      return
    try:
      request_dict["body"] = base64.b64decode(body, validate=True)
    except (binascii.Error, ValueError) as e:
      msg = f"Invalid base64 body in queued request: {e}"
      raise SerializationError(msg, data=body, serializer="json") from e

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
