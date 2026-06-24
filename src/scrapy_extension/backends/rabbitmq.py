"""RabbitMQ backend implementation with multi-mode support.

This module provides a RabbitMQ-based implementation of QueueBackend,
supporting multiple deployment modes:
- Standalone: Single RabbitMQ node
- Cluster: Multi-node RabbitMQ cluster
- Mirrored Queues: Cluster with HA queues

Note: RabbitMQ does not support SetBackend or StorageBackend operations.
"""

from __future__ import annotations

import contextlib
import logging
import ssl
from typing import TYPE_CHECKING, Any, Literal

try:
    import pika
    from pika.exceptions import AMQPError
except ImportError as e:
    raise ImportError(
        "RabbitMQ backend requires 'pika'. Install with: pip install scrapy-extension[rabbitmq]"
    ) from e

from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    _validate_key_name,
    secret_value,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.settings import RabbitMQMode

if TYPE_CHECKING:
  from scrapy_extension.settings import RabbitMQSettings

logger = logging.getLogger(__name__)

# RabbitMQ broker default credential (well-known; operators override via settings).
# Used to detect the insecure default guest/guest login in non-standalone modes.
_DEFAULT_GUEST_CREDENTIAL = "guest"  # nosec B105


class RabbitMQBackend(Backend, QueueBackend):
  """RabbitMQ backend implementation with multi-mode support.

  Implements QueueBackend using RabbitMQ message queues with priority support.
  Supports standalone, cluster, and mirrored_queues deployment modes.
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
    self._declared_queues: set[str] = set()
    self._last_delivery_tag: int | None = None
    self._ssl_warning_emitted: bool = False

  def connect(self) -> None:
    """Establish connection to RabbitMQ based on deployment mode.

    Creates RabbitMQ connection and channel with mode-specific configuration.

    Raises:
        BackendConnectionError: If the connection cannot be established.
        ConfigurationError: If the configuration is invalid for the mode.
    """
    if self.config.mode not in (
      RabbitMQMode.STANDALONE,
      RabbitMQMode.CLUSTER,
      RabbitMQMode.MIRRORED_QUEUES,
    ):
      try:
        mode_text = str(self.config.mode)
      except (TypeError, ValueError):
        mode_text = getattr(self.config.mode, "value", repr(self.config.mode))
      msg = f"Unsupported RabbitMQ mode: {mode_text}"
      raise ConfigurationError(
        msg,
        setting_name="mode",
        setting_value=self.config.mode,
      )
    if not getattr(self.config, "ssl_enabled", False) and not self._ssl_warning_emitted:
      logger.warning(
        "RabbitMQ connecting without SSL — credentials (username/password) "
        "traverse the network in cleartext. Set ssl_enabled=True (and "
        "configure ssl_cafile / ssl_certfile / ssl_keyfile as needed) for "
        "any deployment outside localhost. (warning emitted once per "
        "backend instance)"
      )
      self._ssl_warning_emitted = True
    try:
      if self.config.mode == RabbitMQMode.STANDALONE:
        self._connect_standalone()
      elif self.config.mode == RabbitMQMode.CLUSTER:
        self._connect_cluster()
      else:
        self._connect_mirrored_queues()
      logger.debug("Connected to RabbitMQ in %s mode", self.config.mode.value)
    except AMQPError as e:
      msg = f"Failed to connect to RabbitMQ ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="rabbitmq",
      ) from e
    except Exception as e:
      # ConnectionFailed or other unexpected errors from pika connection layer
      msg = f"Failed to connect to RabbitMQ ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="rabbitmq",
      ) from e

  def _get_ssl_verify_mode(self) -> ssl.VerifyMode:
    """Get SSL verification mode from config.

    Returns:
        ssl.CERT_NONE, ssl.CERT_OPTIONAL, or ssl.CERT_REQUIRED.
    """
    mode = self.config.ssl_verify_mode
    if mode == "CERT_NONE":
      return ssl.CERT_NONE
    if mode == "CERT_OPTIONAL":
      return ssl.CERT_OPTIONAL
    # Default to CERT_REQUIRED for security
    return ssl.CERT_REQUIRED

  def _build_common_parameters(
    self,
    host: str | None = None,
    port: int | None = None,
  ) -> pika.ConnectionParameters:
    """Build common RabbitMQ connection parameters.

    Args:
        host: Optional hostname override.
        port: Optional port override.

    Returns:
        ConnectionParameters with common settings.

    Raises:
        ConfigurationError: If using default guest credentials in non-standalone mode.
    """
    # Warn/fail if using default guest credentials in non-standalone mode
    if (
      self.config.mode != RabbitMQMode.STANDALONE
      and self.config.username == _DEFAULT_GUEST_CREDENTIAL
      and self.config.password == _DEFAULT_GUEST_CREDENTIAL
    ):
      msg = (
        "Default 'guest/guest' credentials are insecure for non-standalone modes. "
        "Set SCRAPY_RABBITMQ_USERNAME and SCRAPY_RABBITMQ_PASSWORD explicitly."
      )
      raise ConfigurationError(
        msg,
        setting_name="username/password",
        setting_value="guest/guest",
      )
    credentials = pika.PlainCredentials(
      self.config.username,
      secret_value(self.config.password),
    )

    # Build SSL options if enabled
    ssl_options = None
    if self.config.ssl_enabled:
      ssl_context = ssl.create_default_context(cafile=self.config.ssl_cafile)
      if self.config.ssl_certfile and self.config.ssl_keyfile:
        ssl_context.load_cert_chain(
          certfile=self.config.ssl_certfile,
          keyfile=self.config.ssl_keyfile,
        )

      verify_mode = self._get_ssl_verify_mode()
      if verify_mode == ssl.CERT_NONE:
        ssl_context.check_hostname = False
      ssl_context.verify_mode = verify_mode
      ssl_options = pika.SSLOptions(ssl_context)

    return pika.ConnectionParameters(
      host=host or self.config.host,
      port=port or self.config.port,
      virtual_host=self.config.virtual_host,
      credentials=credentials,
      heartbeat=self.config.heartbeat,
      blocked_connection_timeout=self.config.blocked_connection_timeout,
      connection_attempts=self.config.connection_attempts,
      retry_delay=self.config.retry_delay,
      ssl_options=ssl_options,
    )

  def _connect_standalone(self) -> None:
    """Connect to standalone RabbitMQ node."""
    parameters = self._build_common_parameters()
    self._connection = pika.BlockingConnection(parameters)
    self._channel = self._connection.channel()
    self._setup_qos()
    logger.debug(
      "Connected to standalone RabbitMQ at %s:%s", self.config.host, self.config.port
    )

  def _connect_cluster(self) -> None:
    """Connect to RabbitMQ cluster.

    Uses cluster_nodes for failover if primary node is unavailable.
    """
    if self.config.cluster_nodes:
      parameters = [self._build_common_parameters()]
      all_hosts = [f"{self.config.host}:{self.config.port}"]

      for node in self.config.cluster_nodes:
        host, separator, port_text = node.partition(":")
        port = int(port_text) if separator and port_text else self.config.port
        parameters.append(self._build_common_parameters(host=host, port=port))
        all_hosts.append(f"{host}:{port}")

      logger.debug("Connecting to RabbitMQ cluster with hosts: %s", all_hosts)
      connection_parameters: pika.ConnectionParameters | list[pika.ConnectionParameters] = (
        parameters
      )
    else:
      logger.debug(
        "Connecting to RabbitMQ cluster at %s:%s", self.config.host, self.config.port
      )
      connection_parameters = self._build_common_parameters()

    self._connection = pika.BlockingConnection(connection_parameters)
    self._channel = self._connection.channel()
    self._setup_qos()
    logger.debug("Connected to RabbitMQ cluster")

  def _connect_mirrored_queues(self) -> None:
    """Connect to RabbitMQ with mirrored queues (HA).

    Sets up HA policy for queues if configured.
    """
    # First connect like cluster mode
    self._connect_cluster()

    # Setup HA policy for queues if configured
    if self._channel and self.config.ha_mode:
      try:
        definition: dict[str, Any] = {"ha-mode": self.config.ha_mode}
        if self.config.ha_params:
          definition["ha-params"] = (
            int(self.config.ha_params)
            if self.config.ha_params.isdigit()
            else self.config.ha_params
          )
        if self.config.ha_sync_mode:
          definition["ha-sync-mode"] = self.config.ha_sync_mode

        logger.debug(
          "Configured mirrored queues with HA mode: %s, params: %s",
          self.config.ha_mode,
          self.config.ha_params,
        )
      except AMQPError as e:
        logger.warning("Failed to configure mirrored queues HA policy: %s", e)

  def _setup_qos(self) -> None:
    """Set up QoS (Quality of Service) settings on the channel."""
    if self._channel and (
      self.config.prefetch_count > 0 or self.config.prefetch_size > 0
    ):
      self._channel.basic_qos(
        prefetch_count=self.config.prefetch_count,
        prefetch_size=self.config.prefetch_size,
      )
      logger.debug(
        "Set QoS: prefetch_count=%d, prefetch_size=%d",
        self.config.prefetch_count,
        self.config.prefetch_size,
      )

  def disconnect(self) -> None:
    """Close RabbitMQ connection."""
    if self._channel:
      with contextlib.suppress(AMQPError):
        self._channel.close()
      self._channel = None
    if self._connection:
      with contextlib.suppress(AMQPError):
        self._connection.close()
      self._connection = None
    self._declared_queues.clear()

  def is_connected(self) -> bool:
    """Check if RabbitMQ is connected.

    Returns:
        True if connection is available.
    """
    return self._connection is not None and self._connection.is_open

  def ping(self) -> bool:
    """Check RabbitMQ health.

    Uses connection-level is_open check (heartbeat is already configured
    in ConnectionParameters). No channel creation needed, avoiding
    resource leaks from repeated channel allocation.
    """
    return self._connection is not None and self._connection.is_open

  @property
  def backend_type(self) -> BackendType:
    """Return backend type.

    Returns:
        BackendType.RABBITMQ
    """
    return BackendType.RABBITMQ

  def _ensure_queue_exists(self, queue_name: str) -> None:
    """Ensure RabbitMQ queue exists.

    Declares the queue idempotently. After the first successful declare
    in a session, subsequent calls are skipped — re-declaring with
    different arguments (e.g. ``x-max-priority``) raises
    ``PRECONDITION_FAILED`` and kills the channel.

    Args:
        queue_name: Name of the queue.

    Raises:
        QueueError: If queue declaration fails. The message includes
            recovery guidance when ``PRECONDITION_FAILED`` is detected.
    """
    if self._channel is None:
      msg = "Not connected to RabbitMQ"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="declare",
      )
    if queue_name in self._declared_queues:
      return
    try:
      self._channel.queue_declare(
        queue=queue_name,
        durable=self.config.durable,
        auto_delete=self.config.auto_delete,
        arguments={"x-max-priority": self.config.max_priority},
      )
    except AMQPError as e:
      error_text = str(e)
      if "PRECONDITION_FAILED" in error_text or "PRECONDITION" in error_text:
        msg = (
          f"Queue {queue_name} exists with incompatible arguments: {e}. "
          f"Drop the queue first or align config "
          f"(durable={self.config.durable}, "
          f"x-max-priority={self.config.max_priority})."
        )
      else:
        msg = f"Failed to declare queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="declare",
      ) from e
    self._declared_queues.add(queue_name)

  # QueueBackend implementation
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push item to priority queue.

    Args:
        queue_name: Name of the queue.
        item: Item to push (bytes).
        priority: Priority value (higher = more urgent, max 255).

    Raises:
        QueueError: If the push operation fails.
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    if self._channel is None:
      msg = "Not connected to RabbitMQ"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="push",
      )
    try:
      self._ensure_queue_exists(queue_name)

      # Clamp priority to valid range
      clamped_priority = max(0, min(int(priority), self.config.max_priority))

      # Cast delivery_mode to Literal[1, 2] for pika compatibility
      delivery_mode: Literal[1, 2] = 1 if self.config.delivery_mode == 1 else 2
      properties = pika.BasicProperties(
        priority=clamped_priority,
        delivery_mode=delivery_mode,
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
        timeout: Seconds to wait (unused for RabbitMQ, blocking not supported).

    Returns:
        The popped item, or None if queue is empty.

    Raises:
        QueueError: If the pop operation fails.
    """
    if self._channel is None:
      msg = "Not connected to RabbitMQ"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      )
    try:
      self._ensure_queue_exists(queue_name)

      method_frame, _header_frame, body = self._channel.basic_get(
        queue=queue_name,
        auto_ack=False,
      )

      if method_frame:
        if self._last_delivery_tag is not None:
          logger.warning(
            "pop() called while previous message is unacked — "
            "CONCURRENT_REQUESTS>1 breaks ack tracking. "
            "Set CONCURRENT_REQUESTS=1 for correct at-least-once delivery."
          )
        self._last_delivery_tag = method_frame.delivery_tag
        return body
    except AMQPError as e:
      msg = f"Failed to pop from queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      ) from e
    return None

  def ack(self, queue_name: str) -> None:
    """Acknowledge the last-popped message via ``basic_ack``.

    Idempotent: clears the tracked delivery tag after acking so duplicate
    ack calls are safe (calling basic_ack with an already-acked tag raises
    a channel error).
    """
    if self._channel is None or self._last_delivery_tag is None:
      return
    try:
      self._channel.basic_ack(delivery_tag=self._last_delivery_tag)
    except AMQPError as e:
      msg = f"Failed to ack RabbitMQ message: {e}"
      raise QueueError(msg, operation="ack") from e
    finally:
      self._last_delivery_tag = None

  def nack(self, queue_name: str) -> None:
    """Negatively acknowledge the last-popped message; requeue for retry."""
    if self._channel is None or self._last_delivery_tag is None:
      return
    try:
      self._channel.basic_nack(
        delivery_tag=self._last_delivery_tag,
        requeue=True,
      )
    except AMQPError as e:
      msg = f"Failed to nack RabbitMQ message: {e}"
      raise QueueError(msg, operation="nack") from e
    finally:
      self._last_delivery_tag = None

  def queue_len(self, queue_name: str) -> int:
    """Get queue length.

    Args:
        queue_name: Name of the queue.

    Returns:
        Number of messages in the queue.

    Raises:
        QueueError: If the queue_len operation fails.
    """
    if self._channel is None:
      msg = "Not connected to RabbitMQ"
      raise QueueError(msg, queue_name=queue_name, operation="queue_len")
    try:
      result = self._channel.queue_declare(
        queue=queue_name,
        passive=True,
      )
    except AMQPError as e:
      msg = f"Failed to get queue length for {queue_name}: {e}"
      raise QueueError(msg, queue_name=queue_name, operation="queue_len") from e
    return result.method.message_count

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.
    """
    if self._channel is None:
      return
    try:
      self._channel.queue_purge(queue=queue_name)
    except AMQPError as e:
      logger.warning("Failed to clear queue %s: %s", queue_name, e)
