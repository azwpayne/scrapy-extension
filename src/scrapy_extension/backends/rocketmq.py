"""RocketMQ backend implementation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
  secret_value,
)
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.settings import RocketMQMode

if TYPE_CHECKING:
  from scrapy_extension.settings import RocketMQSettings

logger = logging.getLogger(__name__)


class RocketMQBackend(Backend, QueueBackend):
  """RocketMQ backend implementation.

  Note: RocketMQ only supports QueueBackend operations.
  SetBackend and StorageBackend are not supported by RocketMQ. Configuring
  RocketMQ for the set/storage component is rejected at config time by
  ``resolve_backend_config`` (RocketMQ is excluded from
  ``SET_CAPABLE_BACKENDS`` / ``STORAGE_CAPABLE_BACKENDS``). If that gating is
  bypassed, instantiation fails fast via the dedicated guard classes
  ``RocketMQSetBackend`` / ``RocketMQStorageBackend`` (raise
  ``ConfigurationError`` in ``__init__``) rather than per-method
  ``NotImplementedError`` stubs.
  """

  def __init__(self, config: RocketMQSettings) -> None:
    """Initialize RocketMQ backend.

    Args:
        config: Configuration for RocketMQ connection.
    """
    self.config = config
    self._producer = None
    self._consumer = None
    self._subscribed_topics: set[str] = set()

  def connect(self) -> None:
    """Establish connection to RocketMQ.

    Raises:
      BackendConnectionError: If connection fails.
      ConfigurationError: If configuration is invalid.
    """
    try:
      from rocketmq.auth.credentials import PlainCredentials
      from rocketmq.client import Producer
      from rocketmq.consumer import SimpleConsumer
      from rocketmq.endpoint import Endpoint
    except ImportError as e:
      msg = f"rocketmq-client-python not installed: {e}"
      raise BackendConnectionError(msg, backend_type="rocketmq") from e

    if self.config.mode not in (
      RocketMQMode.STANDALONE,
      RocketMQMode.CLUSTER,
      RocketMQMode.CLOUD,
    ):
      try:
        mode_text = str(self.config.mode)
      except (TypeError, ValueError):
        mode_text = getattr(
          self.config.mode, "value", repr(self.config.mode)
        )
      msg = f"Unsupported RocketMQ mode: {mode_text}"
      raise ConfigurationError(
        msg, setting_name="mode", setting_value=self.config.mode
      )

    try:
      # Set up credentials if provided
      credentials = None
      if self.config.access_key and self.config.secret_key:
        credentials = PlainCredentials(
          secret_value(self.config.access_key),
          secret_value(self.config.secret_key),
        )

      # Create producer
      self._producer = Producer(
        self.config.producer_group,
        endpoint=Endpoint(self.config.namesrv_address),
        credentials=credentials,
      )
      if self._producer is None:
        msg = "RocketMQBackend producer initialization returned None"
        raise BackendConnectionError(msg, backend_type="rocketmq")
      self._producer.start()

      # Create simple consumer for pop operations
      self._consumer = SimpleConsumer(
        self.config.consumer_group,
        endpoint=Endpoint(self.config.namesrv_address),
        credentials=credentials,
        request_timeout_ms=self.config.send_timeout,
      )
      if self._consumer is None:
        msg = "RocketMQBackend consumer initialization returned None"
        raise BackendConnectionError(msg, backend_type="rocketmq")
      self._consumer.start()

      logger.debug(
        "Connected to RocketMQ at %s", self.config.namesrv_address
      )
    except Exception as e:
      # Connection / producer / consumer init failures (network-level
      # ``OSError`` included). R14-H: dropped the redundant ``except OSError``
      # arm — its message was identical to this one and ``Exception`` already
      # covers it.
      msg = f"Failed to connect to RocketMQ: {e}"
      raise BackendConnectionError(msg, backend_type="rocketmq") from e

  def disconnect(self) -> None:
    """Close RocketMQ connections."""
    if self._producer:
      self._producer.shutdown()
      self._producer = None
    if self._consumer:
      self._consumer.shutdown()
      self._consumer = None
    self._subscribed_topics.clear()
    logger.debug("Disconnected from RocketMQ")

  def is_connected(self) -> bool:
    """Check if RocketMQ is connected.

    Returns:
      True if producer and consumer are running.
    """
    return self._producer is not None and self._consumer is not None

  def ping(self) -> bool:
    """Check RocketMQ health.

    Returns:
      True if the backend reports connected and both the producer and
      consumer clients are initialized.

    Note:
      This is a **local-state** check (``is_connected`` + client presence),
      not a broker round-trip — unlike Redis's ``PING`` or Kafka's
      ``list_topics``. A broker that has gone down but whose socket hasn't
      timed out may still report True. A real liveness probe would need a
      broker round-trip; the right one for RocketMQ is an open design
      question (R1-P2-16) left to the operator.
    """
    if not self.is_connected():
      return False
    # Verify producer and consumer are still active
    return self._producer is not None and self._consumer is not None

  @property
  def backend_type(self) -> BackendType:
    """Return backend type.

    Returns:
        BackendType.ROCKETMQ
    """
    return BackendType.ROCKETMQ

  def _get_topic_name(self, queue_name: str) -> str:
    """Get full topic name for queue.

    Args:
      queue_name: Base queue name.

    Returns:
      Full topic name.
    """
    return f"{self.config.topic_prefix}_{queue_name}"

  def _ensure_subscribed(self, topic_name: str) -> None:
    """Ensure the consumer is subscribed to ``topic_name``.

    RocketMQ's SimpleConsumer only receives messages from topics it has
    subscribed to. Without this call, ``receive()`` returns nothing
    regardless of what producers push. Subscriptions are tracked in-session
    to avoid the overhead of re-subscribing on every pop.

    Args:
      topic_name: Full topic name to subscribe to.
    """
    if topic_name in self._subscribed_topics:
      return
    if self._consumer is not None:
      self._consumer.subscribe(topic_name)
      self._subscribed_topics.add(topic_name)

  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push item to queue.

    Args:
        queue_name: Name of the queue.
        item: Item to push (bytes).
        priority: Priority value (higher = more urgent).

    Raises:
        QueueError: If push fails.
    """
    from scrapy_extension.exceptions import QueueError

    if not self.is_connected():
      msg = "Not connected to RocketMQ"
      raise QueueError(msg)

    try:
      from rocketmq.message import Message

      topic_name = self._get_topic_name(queue_name)
      msg = Message(topic_name)
      msg.set_keys(str(priority))  # Use priority as key for ordering
      msg.set_body(item)
      if self._producer is None:
        error = "RocketMQBackend not connected: producer is None"
        raise QueueError(error)
      self._producer.send(msg)
    except Exception as e:
      # Send failures (network-level ``OSError`` included). R14-H: dropped the
      # redundant ``except OSError`` arm — its message was identical and
      # ``Exception`` already covers it.
      msg = f"Failed to push to queue: {e}"
      raise QueueError(msg) from e

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop item from queue.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (0 = non-blocking).

    Returns:
        Popped item, or None if queue is empty.
    """
    from scrapy_extension.exceptions import QueueError

    if not self.is_connected():
      msg = "Not connected to RocketMQ"
      raise QueueError(msg)

    try:
      topic_name = self._get_topic_name(queue_name)
      self._ensure_subscribed(topic_name)
      timeout_ms = int(timeout * 1000) if timeout > 0 else 3000
      if self._consumer is None:
        error = "RocketMQBackend not connected: consumer is None"
        raise QueueError(error)
      messages = self._consumer.receive(timeout_ms)
      if not messages:
        return None
      msg = messages[0]
      self._consumer.ack(msg)
      return msg.body
    except Exception as e:
      # Receive failures (network-level ``OSError`` included). R14-H: dropped
      # the redundant ``except OSError`` arm — its message was identical and
      # ``Exception`` already covers it.
      msg = f"Failed to pop from queue: {e}"
      raise QueueError(msg) from e

  def queue_len(self, queue_name: str) -> int:
    """Get queue length.

    Args:
        queue_name: Name of the queue.

    Returns:
        Number of items in queue.

    Raises:
        NotImplementedError: RocketMQ does not support queue length queries.
    """
    msg = "RocketMQ does not support queue_len(). Use pop() to consume messages."
    raise NotImplementedError(msg)

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.
    """
    from scrapy_extension.exceptions import QueueError

    if not self.is_connected():
      msg = "Not connected to RocketMQ"
      raise QueueError(msg)
    # RocketMQ doesn't support purge, log warning
    logger.warning("clear_queue not supported in RocketMQ")


# ---------------------------------------------------------------------------
# Set / Storage — class-level guard (replaces former per-method stubs)
# ---------------------------------------------------------------------------
#
# RocketMQ is excluded from SET_CAPABLE_BACKENDS and STORAGE_CAPABLE_BACKENDS
# at the connector layer, so these classes are unreachable under normal config
# resolution (resolve_backend_config raises ConfigurationError first). They
# exist as the fail-fast surface for anyone who bypasses that gating and
# instantiates them directly — replacing the former per-method
# NotImplementedError stubs that lived on RocketMQBackend and were unreachable
# dead code. The dedicated classes raise a typed ConfigurationError at
# construction with an actionable message pointing at the correct settings.


def _unsupported_component_guard(
  component: str, setting_key: str
) -> ConfigurationError:
  """Build the ConfigurationError raised when RocketMQ is bound to an
  unsupported component (set/storage) via direct instantiation that bypasses
  the connector capability gating.

  Args:
      component: The unsupported component name (``"set"`` / ``"storage"``).
      setting_key: The Scrapy setting that selects the component backend.

  Returns:
      A ``ConfigurationError`` with an actionable message.
  """
  if component == "storage":
    alternatives = "redis, mongodb, elasticsearch, memcached, or dynamodb"
  else:
    alternatives = "redis, mongodb, or elasticsearch"
  msg = (
    f"RocketMQ does not support {component} operations: it is a message "
    f"queue with no native set/membership or key-value semantics. Select a "
    f"different backend via {setting_key} (e.g. {alternatives})."
  )
  return ConfigurationError(msg, setting_name=setting_key)


class RocketMQSetBackend(RocketMQBackend):
  """Guard class: RocketMQ cannot serve the ``SetBackend`` interface.

  RocketMQ is excluded from ``SET_CAPABLE_BACKENDS`` at the connector layer, so
  this class is only reachable if an operator bypasses that gating and
  instantiates it directly. Construction fails fast with a typed
  ``ConfigurationError`` (replacing the former per-method ``NotImplementedError``
  stubs, which were unreachable dead code under normal config resolution).
  """

  def __init__(self, config: RocketMQSettings) -> None:
    """Reject construction — RocketMQ does not support the set interface.

    Raises:
        ConfigurationError: Always.
    """
    raise _unsupported_component_guard("set", "SCRAPY_SET_BACKEND_TYPE")


class RocketMQStorageBackend(RocketMQBackend):
  """Guard class: RocketMQ cannot serve the ``StorageBackend`` interface.

  RocketMQ is excluded from ``STORAGE_CAPABLE_BACKENDS`` at the connector layer,
  so this class is only reachable if an operator bypasses that gating and
  instantiates it directly. Construction fails fast with a typed
  ``ConfigurationError`` (replacing the former per-method ``NotImplementedError``
  stubs, which were unreachable dead code under normal config resolution).
  """

  def __init__(self, config: RocketMQSettings) -> None:
    """Reject construction — RocketMQ does not support the storage interface.

    Raises:
        ConfigurationError: Always.
    """
    raise _unsupported_component_guard("storage", "SCRAPY_STORAGE_BACKEND_TYPE")
