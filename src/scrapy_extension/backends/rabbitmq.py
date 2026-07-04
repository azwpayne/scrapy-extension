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
from typing import TYPE_CHECKING, Any, Literal, cast

try:
    import pika
    from pika.exceptions import AMQPError
except ImportError as e:
    raise ImportError(
        "RabbitMQ backend requires 'pika'. Install with: pip install scrapy-extension[rabbitmq]"
    ) from e

from scrapy_extension.backends._redaction import _redact
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

# R14-E: cap on the diagnostic in-flight delivery-tag set. Each unacked pop
# adds one entry; without a cap a long-running process with slow acks (or a
# bug that never acks) grows the set unbounded. We warn-once on overflow and
# STOP adding (the set is diagnostic — ack correctness lives in the broker's
# delivery-tag state, not in this set; dropping the entry loses only the leak
# signal, never the ack state). 10k is generous for normal CONCURRENT_REQUESTS
# backpressure and tight enough to flag a real leak.
_MAX_IN_FLIGHT = 10_000


class RabbitMQBackend(Backend, QueueBackend):
  """RabbitMQ backend implementation with multi-mode support.

  Implements QueueBackend using RabbitMQ message queues with priority support.
  Supports standalone, cluster, and mirrored_queues deployment modes.
  Does NOT implement SetBackend or StorageBackend.

  Ack capability: ``requires_ack=True``, ``supports_concurrent_ack=True``.
  Pops carry the RabbitMQ delivery tag, tracked in an in-flight set;
  :meth:`ack` ``basic_ack``s the specific tag. N pops before any ack no
  longer overwrite a single slot — ack is correct under
  ``CONCURRENT_REQUESTS > 1``.

  Attributes:
      config: RabbitMQSettings instance with connection parameters.
      _connection: The RabbitMQ connection instance.
      _channel: The RabbitMQ channel instance.
  """

  requires_ack = True
  supports_concurrent_ack = True

  def __init__(self, config: RabbitMQSettings) -> None:
    """Initialize RabbitMQ backend.

    Args:
        config: Configuration for RabbitMQ connection.
    """
    self.config = config
    self._connection: pika.BlockingConnection | None = None
    self._channel: pika.channel.Channel | None = None
    self._declared_queues: set[str] = set()
    # Legacy single-slot for the ``ack(token=None)`` fallback path. Kept so
    # external callers that pop() then ack() without a token still work.
    self._last_delivery_tag: int | None = None
    # In-flight delivery tags for correctness under CONCURRENT_REQUESTS>1.
    # Every pop_with_ack adds its tag here; ack(token) basic_acks the
    # specific tag and removes it. Without this, N pops before any ack
    # overwrite _last_delivery_tag and only the last-popped message is
    # ackable — silent at-least-once violation.
    self._in_flight_tags: set[int] = set()
    self._ssl_warning_emitted: bool = False
    # R14-E: one-shot guard for the in-flight-set-overflow warning.
    self._in_flight_overflow_warned: bool = False

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
      _redact(secret_value(self.config.password)),
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
    """Connect to standalone RabbitMQ node.

    R14-E: ``_setup_qos`` runs BEFORE the connection/channel are committed
    to instance state. On QoS failure the freshly-opened channel/connection
    are closed and ``_channel``/``_connection`` are nulled, so
    :meth:`is_connected` reports False truthfully instead of True-on-half-
    init (mirrors the R25-A1 connect-path cleanup pattern in
    ``connectors.py``).
    """
    parameters = self._build_common_parameters()
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    try:
      self._apply_qos(channel)
    except AMQPError:
      # QoS failed on a half-init channel — close the freshly-opened
      # handles and null both instance attrs so is_connected() stays
      # truthful. Re-raise so connect()'s retry loop sees the failure.
      with contextlib.suppress(AMQPError):
        channel.close()
      with contextlib.suppress(AMQPError):
        connection.close()
      self._channel = None
      self._connection = None
      raise
    self._connection = connection
    self._channel = channel
    logger.debug(
      "Connected to standalone RabbitMQ at %s:%s", self.config.host, self.config.port
    )

  def _connect_cluster(self) -> None:
    """Connect to RabbitMQ cluster.

    Uses cluster_nodes for failover if primary node is unavailable.

    R14-E: QoS runs before ``_connection``/``_channel`` are committed, with
    null-on-failure cleanup so :meth:`is_connected` stays truthful.
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

    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()
    try:
      self._apply_qos(channel)
    except AMQPError:
      with contextlib.suppress(AMQPError):
        channel.close()
      with contextlib.suppress(AMQPError):
        connection.close()
      self._channel = None
      self._connection = None
      raise
    self._connection = connection
    self._channel = channel
    logger.debug("Connected to RabbitMQ cluster")

  def _connect_mirrored_queues(self) -> None:
    """Connect to RabbitMQ with mirrored queues (HA).

    Classic mirrored-queue HA policy (``ha-mode`` / ``ha-params`` /
    ``ha-sync-mode``) is NOT applied via AMQP by this client — setting it
    requires the RabbitMQ management API or ``rabbitmqctl set_policy``
    (out-of-band), and classic mirroring itself is deprecated in modern
    RabbitMQ (prefer quorum queues). An operator who configured these
    values is warned so they don't operate under the false impression
    that this client applied the policy (#34 — previously the dict was
    built into a local ``definition`` and only logged at DEBUG as
    "Configured", which was misleading and left the policy unset).
    """
    # First connect like cluster mode.
    self._connect_cluster()
    if not (self._channel and self.config.ha_mode):
      return
    logger.warning(
      "RabbitMQ mirrored-queues HA policy (ha-mode=%s, ha-params=%s, "
      "ha-sync-mode=%s) is configured but NOT applied via AMQP by this "
      "client — set it out-of-band via `rabbitmqctl set_policy` or the "
      "management API. Classic mirroring is deprecated in modern RabbitMQ; "
      "consider quorum queues for HA.",
      self.config.ha_mode,
      self.config.ha_params,
      self.config.ha_sync_mode,
    )

  def _setup_qos(self) -> None:
    """Set up QoS (Quality of Service) settings on the current channel.

    Kept for backward-compat with external callers / tests that invoke the
    instance method directly. Internal connect paths use
    :meth:`_apply_qos` on a local channel so QoS runs BEFORE the channel
    is committed to ``self._channel`` (R14-E null-on-failure cleanup).
    """
    if self._channel is not None:
      self._apply_qos(self._channel)

  def _apply_qos(self, channel: pika.channel.Channel) -> None:
    """Apply QoS (prefetch) settings to ``channel``.

    R14-E: extracted from :meth:`_setup_qos` so the connect paths can call
    it on a freshly-opened LOCAL channel (not yet committed to
    ``self._channel``). On :class:`AMQPError` the caller is responsible
    for closing the channel/connection and nulling instance state — this
    method re-raises unchanged so the cleanup arm in each connect path
    fires.

    Args:
        channel: The freshly-opened channel to apply QoS to.

    Raises:
        AMQPError: If ``basic_qos`` fails at the AMQP layer.
    """
    if self.config.prefetch_count > 0 or self.config.prefetch_size > 0:
      channel.basic_qos(
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

    Tracks the popped delivery tag in ``_last_delivery_tag`` for the
    legacy ``ack(token=None)`` path. Prefer :meth:`pop_with_ack` under
    ``CONCURRENT_REQUESTS > 1`` — that path tracks every popped delivery
    tag in the in-flight set so ack(token) acks the *specific* message,
    not merely the last-popped one.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (unused for RabbitMQ, blocking not supported).

    Returns:
        The popped item, or None if queue is empty.

    Raises:
        QueueError: If the pop operation fails.
    """
    body, _tag = self._basic_get(queue_name)
    return body

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop an item together with its RabbitMQ delivery tag.

    Records the delivery tag in the in-flight set so :meth:`ack` can
    ``basic_ack`` the specific message — correct under
    ``CONCURRENT_REQUESTS > 1`` (no single-slot overwrite, no message
    lost/skipped).

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (unused for RabbitMQ).

    Returns:
        ``(body, delivery_tag)`` where ``delivery_tag`` is the int AMQP
        delivery tag, or ``(None, None)`` when the queue is empty.

    Raises:
        QueueError: If the pop operation fails.
    """
    del timeout  # RabbitMQ basic_get is non-blocking; timeout is unused.
    body, tag = self._basic_get(queue_name, track_in_flight=True)
    return (body, tag)

  def _track_in_flight(self, delivery_tag: int) -> None:
    """Add ``delivery_tag`` to the diagnostic in-flight set, bounded.

    R14-E: the in-flight set is diagnostic (the broker tracks delivery
    tags — ack correctness does not depend on this set). It grows one
    entry per unacked pop, so a long-running process with slow acks (or a
    bug that never acks) would grow it unbounded. We cap it at
    :data:`_MAX_IN_FLIGHT` and warn-once on overflow. The POP ITSELF is
    never dropped — the caller still receives the message and the broker
    still tracks its delivery tag for ack.

    Args:
        delivery_tag: The AMQP delivery tag to track.
    """
    if len(self._in_flight_tags) < _MAX_IN_FLIGHT:
      self._in_flight_tags.add(delivery_tag)
      return
    if not self._in_flight_overflow_warned:
      self._in_flight_overflow_warned = True
      logger.warning(
        "RabbitMQ in-flight ack set at cap (%d) — further unacked pops "
        "will not be tracked in the diagnostic set. This indicates slow "
        "acks or an ack leak; the broker still tracks delivery tags so "
        "ack correctness is unaffected.",
        _MAX_IN_FLIGHT,
      )

  def _basic_get(
    self, queue_name: str, *, track_in_flight: bool = False
  ) -> tuple[bytes | None, int | None]:
    """Fetch one message via ``basic_get(auto_ack=False)``.

    Shared by :meth:`pop` and :meth:`pop_with_ack` so channel validation,
    queue declaration, and error wrapping live in one place.

    Args:
        queue_name: Name of the queue.
        track_in_flight: When True (pop_with_ack path), add the delivery
            tag to the in-flight set so ack(token) can ack it. When False
            (legacy pop path), only set ``_last_delivery_tag``.

    Returns:
        ``(body, delivery_tag)``. ``body`` is ``None`` when the queue is
        empty.

    Raises:
        QueueError: If the get fails at the AMQP layer.
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
        delivery_tag = method_frame.delivery_tag
        self._last_delivery_tag = delivery_tag
        if track_in_flight:
          self._track_in_flight(delivery_tag)
        return (body, delivery_tag)
    except AMQPError as e:
      msg = f"Failed to pop from queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      ) from e
    return (None, None)

  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Ack a popped message via ``basic_ack``.

    With a ``token`` (the scheduler path under
    ``CONCURRENT_REQUESTS > 1``): ``basic_ack(delivery_tag=token,
    multiple=False)`` the specific message and remove it from the
    in-flight set. Order-independent — ack the right tag regardless of
    pop/ack interleaving.

    Without a ``token`` (legacy single-pop caller): ``basic_ack`` the
    tracked ``_last_delivery_tag``. Only correct for
    ``CONCURRENT_REQUESTS=1``.

    Idempotent: clears the tracked tag after acking so duplicate ack
    calls on the legacy path are safe (calling ``basic_ack`` with an
    already-acked tag raises a channel error).

    Args:
        queue_name: Name of the queue (unused; kept for interface symmetry).
        token: A delivery tag from :meth:`pop_with_ack`, or ``None`` for
            the legacy last-tag path.

    Raises:
        QueueError: If ``basic_ack`` fails at the AMQP layer.
    """
    del queue_name
    if token is not None:
      delivery_tag = int(token)
      if self._channel is None:
        return
      try:
        self._channel.basic_ack(delivery_tag=delivery_tag, multiple=False)
      except AMQPError as e:
        msg = f"Failed to ack RabbitMQ message: {e}"
        raise QueueError(msg, operation="ack") from e
      finally:
        self._in_flight_tags.discard(delivery_tag)
        if self._last_delivery_tag == delivery_tag:
          self._last_delivery_tag = None
      return
    # Legacy path: ack the tracked last-popped tag.
    if self._channel is None or self._last_delivery_tag is None:
      return
    try:
      self._channel.basic_ack(delivery_tag=self._last_delivery_tag)
    except AMQPError as e:
      msg = f"Failed to ack RabbitMQ message: {e}"
      raise QueueError(msg, operation="ack") from e
    finally:
      self._last_delivery_tag = None

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Nack a popped message; requeue it for retry.

    With a ``token``: ``basic_nack(delivery_tag=token, requeue=True)`` the
    specific message. Without a ``token``: nack the tracked last-popped
    tag (legacy).

    Args:
        queue_name: Name of the queue (unused; interface symmetry).
        token: A delivery tag from :meth:`pop_with_ack`, or ``None``.

    Raises:
        QueueError: If ``basic_nack`` fails at the AMQP layer.
    """
    del queue_name
    if token is not None:
      delivery_tag = int(token)
      if self._channel is None:
        return
      try:
        self._channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
      except AMQPError as e:
        msg = f"Failed to nack RabbitMQ message: {e}"
        raise QueueError(msg, operation="nack") from e
      finally:
        self._in_flight_tags.discard(delivery_tag)
        if self._last_delivery_tag == delivery_tag:
          self._last_delivery_tag = None
      return
    # Legacy path: nack the tracked last-popped tag.
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
    return cast(int, result.method.message_count)

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
