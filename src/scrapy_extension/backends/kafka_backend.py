"""Kafka backend implementation.

This module provides a Kafka-based implementation of QueueBackend.
Note: Kafka does not support SetBackend or StorageBackend operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

from scrapy_extension.backends.base import Backend, BackendType, QueueBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError

if TYPE_CHECKING:
  from scrapy_extension.config.settings import KafkaSettings

logger = logging.getLogger(__name__)


class KafkaBackend(Backend, QueueBackend):
  """Kafka backend implementation.

  Implements QueueBackend using Kafka topics with partition-based priority.
  Does NOT implement SetBackend or StorageBackend.

  Attributes:
      config: KafkaSettings instance with connection parameters.
      _producer: The Kafka producer instance.
      _consumer: The Kafka consumer instance.
  """

  def __init__(self, config: KafkaSettings) -> None:
    """Initialize Kafka backend.

    Args:
        config: Configuration for Kafka connection.
    """
    self.config = config
    self._producer: KafkaProducer | None = None
    self._consumer: KafkaConsumer | None = None
    self._admin_client: KafkaAdminClient | None = None

  def connect(self) -> None:
    """Establish connection to Kafka.

    Creates Kafka producer and admin client.

    Raises:
        BackendConnectionError: If the connection cannot be established.
    """
    try:
      self._producer = KafkaProducer(
        bootstrap_servers=self.config.bootstrap_servers,
        acks=self.config.acks,
        retries=self.config.retries,
        batch_size=self.config.batch_size,
        linger_ms=self.config.linger_ms,
        compression_type=self.config.compression_type,
      )
      self._admin_client = KafkaAdminClient(
        bootstrap_servers=self.config.bootstrap_servers,
        client_id="scrapy-extension-admin",
      )
      logger.debug("Connected to Kafka at %s", self.config.bootstrap_servers)
    except KafkaError as e:
      msg = f"Failed to connect to Kafka: {e}"
      raise BackendConnectionError(
        msg,
        backend_type="kafka",
      ) from e

  def disconnect(self) -> None:
    """Close Kafka connection."""
    if self._producer:
      self._producer.close()
      self._producer = None
    if self._consumer:
      self._consumer.close()
      self._consumer = None
    if self._admin_client:
      self._admin_client.close()
      self._admin_client = None

  def is_connected(self) -> bool:
    """Check if Kafka is connected.

    Returns:
        True if producer is available.
    """
    return self._producer is not None

  def ping(self) -> bool:
    """Check Kafka health.

    Returns:
        True if Kafka brokers are reachable.
    """
    try:
      if self._admin_client:
        self._admin_client.list_topics()
        return True
      return False
    except KafkaError:
      return False

  @property
  def backend_type(self) -> BackendType:
    """Return backend type.

    Returns:
        BackendType.KAFKA
    """
    return BackendType.KAFKA

  def _ensure_topic_exists(self, queue_name: str) -> None:
    """Ensure Kafka topic exists for queue.

    Args:
        queue_name: Name of the queue/topic.
    """
    topic_name = f"scrapy-{queue_name}"
    try:
      topics = self._admin_client.list_topics()
      if topic_name not in topics:
        new_topic = NewTopic(
          name=topic_name,
          num_partitions=self.config.max_priority_partitions,
          replication_factor=self.config.replication_factor,
        )
        self._admin_client.create_topics([new_topic])
    except KafkaError as e:
      logger.warning("Failed to create topic %s: %s", topic_name, e)

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
      self._ensure_topic_exists(queue_name)
      topic_name = f"scrapy-{queue_name}"
      partition = min(int(priority), self.config.max_priority_partitions - 1)

      future = self._producer.send(topic_name, value=item, partition=partition)
      # Wait for send to complete (synchronous for reliability)
      future.get(timeout=10)
    except KafkaError as e:
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
      topic_name = f"scrapy-{queue_name}"

      # Create consumer if not exists
      if self._consumer is None:
        self._consumer = KafkaConsumer(
          bootstrap_servers=self.config.bootstrap_servers,
          group_id=self.config.group_id,
          auto_offset_reset=self.config.auto_offset_reset,
          enable_auto_commit=self.config.enable_auto_commit,
          auto_commit_interval_ms=self.config.auto_commit_interval_ms,
          max_poll_records=self.config.max_poll_records,
          session_timeout_ms=self.config.session_timeout_ms,
        )

      self._consumer.subscribe([topic_name])

      # Poll for messages
      timeout_ms = int(timeout * 1000)
      messages = self._consumer.poll(timeout_ms=timeout_ms, max_records=1)

      for topic_partition, records in messages.items():
        for record in records:
          return record.value

      return None
    except KafkaError as e:
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
        Approximate number of items in the queue.

    Note:
        This is eventually consistent and should be used for monitoring only.
    """
    try:
      topic_name = f"scrapy-{queue_name}"
      partitions = self._admin_client.describe_topics([topic_name])

      total = 0
      for partition in partitions[0]["partitions"]:
        partition_id = partition["partition"]
        end_offset = self._admin_client.list_offsets(
          topic_name, partition_id, "latest"
        )
        begin_offset = self._admin_client.list_offsets(
          topic_name, partition_id, "earliest"
        )
        total += end_offset - begin_offset

      return total
    except KafkaError:
      return 0

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.
    """
    try:
      topic_name = f"scrapy-{queue_name}"
      self._admin_client.delete_topics([topic_name])
      # Recreate topic
      new_topic = NewTopic(
        name=topic_name,
        num_partitions=self.config.max_priority_partitions,
        replication_factor=self.config.replication_factor,
      )
      self._admin_client.create_topics([new_topic])
    except KafkaError as e:
      logger.warning("Failed to clear queue %s: %s", queue_name, e)
