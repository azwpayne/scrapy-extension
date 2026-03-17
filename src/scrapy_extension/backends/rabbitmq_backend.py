"""RabbitMQ backend implementation.

This module provides a RabbitMQ-based implementation of QueueBackend.
Note: RabbitMQ does not support SetBackend or StorageBackend operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pika
from pika.exceptions import AMQPError

from scrapy_extension.backends.base import Backend, BackendType, QueueBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError

if TYPE_CHECKING:
  from scrapy_extension.config.settings import RabbitMQSettings

logger = logging.getLogger(__name__)


class RabbitMQBackend(Backend, QueueBackend):
  """RabbitMQ backend implementation.

  Implements QueueBackend using RabbitMQ message queues with priority support.
  Does NOT implement SetBackend or StorageBackend.

  Attributes:
      config: RabbitMQSettings instance with connection parameters.
      _connection: The RabbitMQ connection instance.
      _channel: The RabbitMQ channel instance.
  """

  def __init__(self, config: RabbitMQSettings) -> None:
    """Initialize RabbitMQ backend.

    Args:
        config: Configuration for RabbitMQ connection.
    """
    self.config = config
    self._connection: pika.BlockingConnection | None = None
    self._channel: pika.channel.Channel | None = None

  def connect(self) -> None:
    """Establish connection to RabbitMQ.

    Creates RabbitMQ connection and channel.

    Raises:
        BackendConnectionError: If the connection cannot be established.
    """
    try:
      credentials = pika.PlainCredentials(
        self.config.username,
        self.config.password,
      )
      parameters = pika.ConnectionParameters(
        host=self.config.host,
        port=self.config.port,
        virtual_host=self.config.virtual_host,
        credentials=credentials,
        heartbeat=self.config.heartbeat,
        blocked_connection_timeout=self.config.blocked_connection_timeout,
      )
      self._connection = pika.BlockingConnection(parameters)
      self._channel = self._connection.channel()
      logger.debug(
        "Connected to RabbitMQ at %s:%s",
        self.config.host,
        self.config.port,
      )
    except AMQPError as e:
      msg = f"Failed to connect to RabbitMQ: {e}"
      raise BackendConnectionError(
        msg,
        backend_type="rabbitmq",
      ) from e

  def disconnect(self) -> None:
    """Close RabbitMQ connection."""
    if self._channel:
      try:
        self._channel.close()
      except AMQPError:
        pass
      self._channel = None
    if self._connection:
      try:
        self._connection.close()
      except AMQPError:
        pass
      self._connection = None

  def is_connected(self) -> bool:
    """Check if RabbitMQ is connected.

    Returns:
        True if connection is available.
    """
    return self._connection is not None and self._connection.is_open

  def ping(self) -> bool:
    """Check RabbitMQ health.

    Returns:
        True if RabbitMQ is reachable.
    """
    try:
      if self._connection and self._connection.is_open:
        # Try to get a channel to verify connection is alive
        test_channel = self._connection.channel()
        test_channel.close()
        return True
      return False
    except AMQPError:
      return False

  @property
  def backend_type(self) -> BackendType:
    """Return backend type.

    Returns:
        BackendType.RABBITMQ
    """
    return BackendType.RABBITMQ

  def _ensure_queue_exists(self, queue_name: str) -> None:
    """Ensure RabbitMQ queue exists.

    Args:
        queue_name: Name of the queue.

    Raises:
        QueueError: If queue declaration fails.
    """
    try:
      self._channel.queue_declare(
        queue=queue_name,
        durable=self.config.durable,
        auto_delete=self.config.auto_delete,
        arguments={"x-max-priority": self.config.max_priority},
      )
    except AMQPError as e:
      msg = f"Failed to declare queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="declare",
      ) from e

  # QueueBackend implementation
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push item to priority queue.

    Args:
        queue_name: Name of the queue.
        item: Item to push (bytes).
        priority: Priority value (higher = more urgent, max 255).

    Raises:
        QueueError: If the push operation fails.
    """
    try:
      self._ensure_queue_exists(queue_name)

      # Clamp priority to valid range
      clamped_priority = min(int(priority), self.config.max_priority)

      properties = pika.BasicProperties(
        priority=clamped_priority,
        delivery_mode=self.config.delivery_mode,
      )

      self._channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=item,
        properties=properties,
      )
    except AMQPError as e:
      msg = f"Failed to push to queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="push",
      ) from e

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop highest priority item from queue.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (0 = non-blocking).

    Returns:
        The popped item, or None if queue is empty.

    Raises:
        QueueError: If the pop operation fails.
    """
    try:
      self._ensure_queue_exists(queue_name)

      method_frame, header_frame, body = self._channel.basic_get(
        queue=queue_name,
        auto_ack=False,
      )

      if method_frame:
        # Acknowledge the message
        self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        return body

      return None
    except AMQPError as e:
      msg = f"Failed to pop from queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      ) from e

  def queue_len(self, queue_name: str) -> int:
    """Get queue length.

    Args:
        queue_name: Name of the queue.

    Returns:
        Number of messages in the queue.
    """
    try:
      result = self._channel.queue_declare(
        queue=queue_name,
        passive=True,
      )
      return result.method.message_count
    except AMQPError:
      return 0

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.
    """
    try:
      self._channel.queue_purge(queue=queue_name)
    except AMQPError as e:
      logger.warning("Failed to clear queue %s: %s", queue_name, e)
