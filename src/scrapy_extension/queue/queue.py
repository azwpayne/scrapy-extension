"""Queue component for scrapy-extension.

This module provides a Scrapy queue component that uses backend queue interfaces.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from scrapy.utils.request import request_from_dict

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.exceptions import SerializationError

if TYPE_CHECKING:
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
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    queue_name: str,
  ) -> None:
    """Initialize the backend queue.

    Args:
        connection_manager: Connection manager for backend access.
        queue_name: Name of the queue.
    """
    self.connection_manager = connection_manager
    self.queue_name = queue_name
    self._serializer = JSONSerializer()

  def _request_to_dict(self, request: Request) -> dict:
    """Convert a Request to a dictionary.

    Args:
        request: The Request to convert.

    Returns:
        Dictionary representation of the request.
    """
    body_value = None
    if request.body:
      try:
        body_value = request.body.decode("utf-8")
      except (UnicodeDecodeError, ValueError):
        body_value = request.body.decode("latin-1")

    return {
      "url": request.url,
      "callback": request.callback.__name__ if request.callback else None,
      "errback": request.errback.__name__ if request.errback else None,
      "method": request.method,
      "headers": dict(request.headers.to_unicode_dict()),
      "body": body_value,
      "cookies": request.cookies,
      "meta": request.meta,
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
      self.connection_manager.get_queue_backend().push(self.queue_name, data, priority)
    except Exception as e:
      msg = f"Failed to serialize request: {e}"
      raise SerializationError(
        msg,
        data=request,
        serializer="json",
      ) from e

  def pop(self, timeout: float = 0.0) -> Request | None:
    """Pop a request from the queue.

    Args:
        timeout: Seconds to wait for an item (0 = non-blocking).

    Returns:
        The popped request, or None if the queue is empty.

    Raises:
        SerializationError: If the request cannot be deserialized.
    """
    data = self.connection_manager.get_queue_backend().pop(self.queue_name, timeout)
    if data is None:
      return None

    try:
      request_dict = self._serializer.deserialize(data)
      return request_from_dict(request_dict)
    except Exception as e:
      msg = f"Failed to deserialize request: {e}"
      raise SerializationError(
        msg,
        data=data,
        serializer="json",
      ) from e

  def peek(self) -> Request | None:
    """Peek at the next request without removing it.

    Warning:
        This operation is NOT atomic. Between pop and push, another
        consumer may take the item. Use only for monitoring/debugging,
        never for request processing in concurrent environments.

    Returns:
        The next request, or None if the queue is empty.
    """
    # Non-atomic: pop then push back. NOT safe for concurrent consumers.
    request = self.pop(timeout=0)
    if request:
      # Push back with same priority to preserve ordering.
      self.push(request, priority=request.priority)
    return request

  def __len__(self) -> int:
    """Get the number of requests in the queue.

    Returns:
        Number of requests.
    """
    return self.connection_manager.get_queue_backend().queue_len(self.queue_name)

  def clear(self) -> None:
    """Clear all requests from the queue."""
    self.connection_manager.get_queue_backend().clear_queue(self.queue_name)
