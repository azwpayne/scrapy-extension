"""Kafka backend implementation with multi-mode support.

This module provides a Kafka-based implementation of QueueBackend,
supporting multiple deployment modes:
- Standalone: Single Kafka broker
- Cluster: Multi-broker Kafka cluster
- Confluent: Confluent Cloud configuration

Note: Kafka does not support SetBackend or StorageBackend operations.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

try:
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import KafkaError, TopicAlreadyExistsError
except ImportError as e:
    raise ImportError(
        "Kafka backend requires 'kafka-python'. Install with: pip install scrapy-extension[kafka]"
    ) from e

from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    _get_mode_text,
    secret_value,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.settings import KafkaMode

# Topic name validation pattern - only allow alphanumeric, dots, underscores, hyphens
# Uses \Z instead of $ to match only at absolute end of string (not before trailing newline)
TOPIC_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+\Z")


class _RedactedStr(str):
  """str subclass that hides its value in repr().

  Used for SASL passwords in client config dicts so that ``repr(config)``
  and traceback dumps of locals don't reveal the raw credential. The
  underlying value remains a normal ``str`` for client libraries
  (kafka-python) that consume it via ``str()`` semantics.

  Note: this is defense-in-depth against accidental logging / Sentry
  capture, NOT against an adversary who can read process memory. The
  raw value is still reachable via ``str(instance)`` or by indexing.
  """

  __slots__ = ()

  def __repr__(self) -> str:
    return "<redacted>"



def _validate_topic_name(name: str) -> None:
    """Validate topic/queue name to prevent injection.

    Args:
        name: The name to validate.

    Raises:
        ValueError: If name contains invalid characters.
    """
    if not name or not TOPIC_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid topic/queue name: {name!r}. "
            "Only alphanumeric, dots, underscores, and hyphens allowed."
        )


if TYPE_CHECKING:
  from scrapy_extension.settings import KafkaSettings

logger = logging.getLogger(__name__)


class KafkaBackend(Backend, QueueBackend):
  """Kafka backend implementation with multi-mode support.

  Implements QueueBackend using Kafka topics with partition-based priority.
  Supports standalone, cluster, and confluent deployment modes.
  Does NOT implement SetBackend or StorageBackend.

  Attributes:
      config: KafkaSettings instance with connection parameters.
      _producer: The Kafka producer instance.
      _consumer: The Kafka consumer instance.
      _admin_client: The Kafka admin client instance.
      _known_topics: Set of topics known to exist (cached to avoid repeated checks).
  """

  def __init__(self, config: KafkaSettings) -> None:
    """Initialize Kafka backend.

    Args:
        config: Configuration for Kafka connection.
    """
    self.config = config
    self._producer: KafkaProducer | None = None
    self._consumer: KafkaConsumer | None = None
    self._last_record: Any = None
    self._admin_client: KafkaAdminClient | None = None
    # Cache known topics to avoid repeated existence checks
    self._known_topics: set[str] = set()

  def connect(self) -> None:
    """Establish connection to Kafka based on deployment mode.

    Creates Kafka producer and admin client with mode-specific configuration.

    Raises:
        BackendConnectionError: If the connection cannot be established.
        ConfigurationError: If the configuration is invalid for the mode.
    """
    if self.config.mode not in (
      KafkaMode.STANDALONE,
      KafkaMode.CLUSTER,
      KafkaMode.CONFLUENT,
    ):
      msg = f"Unsupported Kafka mode: {_get_mode_text(self.config.mode)}"
      raise ConfigurationError(
        msg,
        setting_name="mode",
        setting_value=self.config.mode,
      )
    try:
      if self.config.mode == KafkaMode.STANDALONE:
        self._connect_standalone()
      elif self.config.mode == KafkaMode.CLUSTER:
        self._connect_cluster()
      else:
        self._connect_confluent()
      logger.debug("Connected to Kafka in %s mode", self.config.mode.value)
    except KafkaError as e:
      msg = f"Failed to connect to Kafka ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="kafka",
      ) from e
    except Exception as e:
      # Unexpected errors (e.g., RuntimeError from mocking in tests)
      msg = f"Failed to connect to Kafka ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="kafka",
      ) from e

  def _build_common_config(self) -> dict[str, Any]:
    """Build common Kafka client configuration.

    Returns:
        Dictionary of Kafka client configuration options.
    """
    config: dict[str, Any] = {
      "acks": self.config.acks,
      "retries": self.config.retries,
      "batch_size": self.config.batch_size,
      "linger_ms": self.config.linger_ms,
      "compression_type": self.config.compression_type,
      "max_in_flight_requests_per_connection": self.config.max_in_flight_requests_per_connection,
      "request_timeout_ms": self.config.request_timeout_ms,
    }

    # Add SASL/SSL configuration if security is enabled
    if self.config.security_protocol != "PLAINTEXT":
      config["security_protocol"] = self.config.security_protocol

      if (
        self.config.sasl_mechanism
        and self.config.sasl_username
        and self.config.sasl_password
      ):
        config["sasl_mechanism"] = self.config.sasl_mechanism
        config["sasl_plain_username"] = self.config.sasl_username
        config["sasl_plain_password"] = _RedactedStr(secret_value(self.config.sasl_password))

      if self.config.ssl_cafile:
        config["ssl_cafile"] = self.config.ssl_cafile
      if self.config.ssl_certfile:
        config["ssl_certfile"] = self.config.ssl_certfile
      if self.config.ssl_keyfile:
        config["ssl_keyfile"] = self.config.ssl_keyfile
      config["ssl_check_hostname"] = self.config.ssl_check_hostname

    return config

  def _bootstrap_servers(self) -> str:
    """Return bootstrap servers for the current Kafka mode."""
    if self.config.mode == KafkaMode.CLUSTER and self.config.cluster_brokers:
      return ",".join(self.config.cluster_brokers)
    if self.config.mode == KafkaMode.CONFLUENT:
      return self.config.confluent_bootstrap_servers or self.config.bootstrap_servers
    return self.config.bootstrap_servers

  def _build_client_security_config(self) -> dict[str, Any]:
    """Build consumer/admin-safe security config without producer-only args."""
    if self.config.mode == KafkaMode.CONFLUENT:
      if self.config.confluent_api_key and self.config.confluent_api_secret:
        return {
          "security_protocol": "SASL_SSL",
          "sasl_mechanism": "PLAIN",
          "sasl_plain_username": secret_value(self.config.confluent_api_key),
          "sasl_plain_password": secret_value(self.config.confluent_api_secret),
        }

    common_config = self._build_common_config()
    client_config: dict[str, Any] = {}
    for key in (
      "security_protocol",
      "sasl_mechanism",
      "sasl_plain_username",
      "sasl_plain_password",
      "ssl_cafile",
      "ssl_certfile",
      "ssl_keyfile",
      "ssl_check_hostname",
    ):
      if key in common_config:
        client_config[key] = common_config[key]
    return client_config

  def _build_producer_config(self) -> dict[str, Any]:
    """Build producer config with mode-specific bootstrap and security settings."""
    config = self._build_common_config()
    config["bootstrap_servers"] = self._bootstrap_servers()
    if self.config.mode == KafkaMode.CONFLUENT:
      config.update(self._build_client_security_config())
    return config

  def _connect_standalone(self) -> None:
    """Connect to standalone Kafka broker."""
    bootstrap = self._bootstrap_servers()
    producer_config = self._build_producer_config()
    client_security_config = self._build_client_security_config()

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
    logger.debug("Connected to standalone Kafka at %s", bootstrap)

  def _connect_cluster(self) -> None:
    """Connect to Kafka cluster.

    Uses cluster_brokers if configured, otherwise falls back to bootstrap_servers.
    """
    bootstrap = self._bootstrap_servers()
    producer_config = self._build_producer_config()
    client_security_config = self._build_client_security_config()

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
    logger.debug("Connected to Kafka cluster at %s", bootstrap)

  def _connect_confluent(self) -> None:
    """Connect to Confluent Cloud.

    Uses SASL/SSL authentication with Confluent-specific settings.
    """
    bootstrap = self._bootstrap_servers()
    producer_config = self._build_producer_config()
    client_security_config = self._build_client_security_config()

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
    logger.debug("Connected to Confluent Cloud at %s", bootstrap)

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
    except KafkaError:
      return False
    else:
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

    Uses a local cache to avoid repeated existence checks. Attempts to
    create the topic and catches TopicAlreadyExistsError to avoid the
    TOCTOU (Time-of-Check-Time-of-Use) anti-pattern.

    Args:
        queue_name: Name of the queue/topic.

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    _validate_topic_name(queue_name)
    topic_name = f"scrapy-{queue_name}"

    # Skip if topic is already known to exist
    if topic_name in self._known_topics:
      return

    try:
      new_topic = NewTopic(
        name=topic_name,
        num_partitions=self.config.max_priority_partitions,
        replication_factor=self.config.replication_factor,
      )
      assert self._admin_client is not None
      self._admin_client.create_topics([new_topic])
      self._known_topics.add(topic_name)
      logger.debug("Created Kafka topic: %s", topic_name)
    except TopicAlreadyExistsError:
      # Topic already exists - this is expected
      self._known_topics.add(topic_name)
      logger.debug("Kafka topic already exists: %s", topic_name)
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
      partition = max(0, min(int(priority), self.config.max_priority_partitions - 1))

      assert self._producer is not None
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
        ValueError: If queue_name contains invalid characters.
    """
    _validate_topic_name(queue_name)
    try:
      topic_name = f"scrapy-{queue_name}"

      # Create consumer if not exists
      if self._consumer is None:
        self._consumer = KafkaConsumer(
          bootstrap_servers=self._bootstrap_servers(),
          group_id=self.config.group_id,
          auto_offset_reset=self.config.auto_offset_reset,
          enable_auto_commit=self.config.enable_auto_commit,
          auto_commit_interval_ms=self.config.auto_commit_interval_ms,
          max_poll_records=self.config.max_poll_records,
          session_timeout_ms=self.config.session_timeout_ms,
          **self._build_client_security_config(),
        )

      assert self._consumer is not None
      self._consumer.subscribe([topic_name])

      # Poll for messages
      timeout_ms = int(timeout * 1000)
      messages = self._consumer.poll(timeout_ms=timeout_ms, max_records=1)

      for records in messages.values():
        for record in records:
          if self._last_record is not None:
            logger.warning(
              "pop() called while previous message is unacked — "
              "CONCURRENT_REQUESTS>1 breaks ack tracking. "
              "Set CONCURRENT_REQUESTS=1 for correct at-least-once delivery."
            )
          self._last_record = record
          return record.value
    except KafkaError as e:
      msg = f"Failed to pop from queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      ) from e
    return None

  def ack(self, queue_name: str) -> None:
    """Commit the last-popped offset so it isn't re-delivered on restart.

    Idempotent: clears the tracked record after committing so duplicate
    ack calls are safe.
    """
    if self._consumer is None or self._last_record is None:
      return
    try:
      self._consumer.commit()
    except KafkaError as e:
      msg = f"Failed to ack Kafka message: {e}"
      raise QueueError(msg, operation="ack") from e
    finally:
      self._last_record = None

  def nack(self, queue_name: str) -> None:
    """Kafka cannot re-deliver a polled message within the same session.

    The record's offset is not committed, so on the next consumer restart
    the message is re-delivered. Within a running session, nack is a no-op.
    """
    self._last_record = None

  def queue_len(self, queue_name: str) -> int:
    """Get queue length.

    Args:
        queue_name: Name of the queue.

    Returns:
        Approximate number of items in the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.

    Note:
        This is eventually consistent and should be used for monitoring only.
    """
    _validate_topic_name(queue_name)
    if self._consumer is not None:
      try:
        assignment = self._consumer.assignment()
        if not assignment:
          return 0
        end_offsets = self._consumer.end_offsets(assignment)
        total = sum(
          max(0, end_offsets[tp] - self._consumer.position(tp))
          for tp in assignment
        )
      except KafkaError:
        return 0
      return total

    topic_name = f"scrapy-{queue_name}"
    temp_consumer = KafkaConsumer(
      bootstrap_servers=self._bootstrap_servers(),
      group_id=self.config.group_id,
      enable_auto_commit=False,
      **self._build_client_security_config(),
    )
    try:
      partitions = temp_consumer.partitions_for_topic(topic_name)
      if not partitions:
        return 0
      assignment = {TopicPartition(topic_name, partition) for partition in partitions}
      temp_consumer.assign(list(assignment))
      end_offsets = temp_consumer.end_offsets(list(assignment))
      total = sum(
        max(0, end_offsets[tp] - temp_consumer.position(tp))
        for tp in assignment
      )
    except KafkaError:
      return 0
    finally:
      temp_consumer.close()
    return total

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    _validate_topic_name(queue_name)
    try:
      topic_name = f"scrapy-{queue_name}"
      assert self._admin_client is not None
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
