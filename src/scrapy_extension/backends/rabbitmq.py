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
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
    import pika
    from pika.exceptions import AMQPError
except ImportError as e:
    if not _is_missing_optional_dependency(e, "pika"):
        raise
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
from scrapy_extension.settings.rabbitmq import validate_rabbitmq_connection

if TYPE_CHECKING:
  from scrapy_extension.settings import RabbitMQSettings

logger = logging.getLogger(__name__)

# R14-E: cap on the diagnostic in-flight ack-token set. Each unacked pop
# adds one entry; without a cap a long-running process with slow acks (or a
# bug that never acks) grows the set unbounded. We warn-once on overflow and
# STOP adding (the set is diagnostic — ack correctness lives in the broker's
# delivery-tag state, not in this set; dropping the entry loses only the leak
# signal, never the ack state). 10k is generous for normal CONCURRENT_REQUESTS
# backpressure and tight enough to flag a real leak.
_MAX_IN_FLIGHT = 10_000


class _RabbitMQAckToken:
  """Opaque acknowledgement token for one channel-scoped delivery tag.

  RabbitMQ delivery tags are scoped to a channel and may restart from the
  same integer after reconnecting. The channel generation prevents a late
  completion from an old channel from acknowledging an unrelated delivery
  on the current channel.
  """

  __slots__ = ("_completed", "channel_generation", "delivery_tag")

  def __init__(self, delivery_tag: int, channel_generation: int) -> None:
    """Initialize a token for ``delivery_tag`` in one channel generation."""
    self.delivery_tag = delivery_tag
    self.channel_generation = channel_generation
    self._completed = False

  def __eq__(self, other: object) -> bool:
    if not isinstance(other, _RabbitMQAckToken):
      return NotImplemented
    return (
      self.delivery_tag == other.delivery_tag
      and self.channel_generation == other.channel_generation
    )

  def __hash__(self) -> int:
    return hash((self.delivery_tag, self.channel_generation))

  def __repr__(self) -> str:
    return (
      f"_RabbitMQAckToken(delivery_tag={self.delivery_tag}, "
      f"channel_generation={self.channel_generation})"
    )


@dataclass(frozen=True)
class _RabbitMQConnectionSnapshot:
  """One validated, repr-safe set of values used by a connect attempt."""

  mode: RabbitMQMode
  host: str
  port: int
  cluster_nodes: tuple[tuple[str, int], ...]
  username: str
  password: str
  virtual_host: str
  ssl_enabled: bool
  ssl_cafile: str | None
  ssl_certfile: str | None
  ssl_keyfile: str | None
  ssl_verify_mode: str
  heartbeat: int
  blocked_connection_timeout: int
  connection_attempts: int
  retry_delay: int
  prefetch_count: int
  prefetch_size: int
  ha_mode: str | None
  ha_params: str | None
  ha_sync_mode: str


class RabbitMQBackend(Backend, QueueBackend):
  """RabbitMQ backend implementation with multi-mode support.

  Implements QueueBackend using RabbitMQ message queues with priority support.
  Supports standalone, cluster, and mirrored_queues deployment modes.
  Does NOT implement SetBackend or StorageBackend.

  Ack capability: ``requires_ack=True``, ``supports_concurrent_ack=True``.
  Pops carry an ack token containing the RabbitMQ delivery tag and channel
  generation. :meth:`ack` confirms the specific current-channel tag. N pops
  before any ack no longer overwrite a single slot, and reconnects cannot
  redirect a stale token to a new channel.

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
    self._channel_generation = 0
    # Ack operations use this immutable snapshot instead of ``_channel`` so
    # a reconnect between validation and the broker call cannot redirect an
    # old token to the new channel.
    self._channel_session: tuple[int, pika.channel.Channel] | None = None
    self._declared_queues: set[str] = set()
    # Legacy single-slot for the ``ack(token=None)`` fallback path. Kept so
    # external callers that pop() then ack() without a token still work.
    self._last_delivery_tag: int | None = None
    # In-flight ack tokens for correctness under CONCURRENT_REQUESTS>1.
    # Every pop_with_ack adds its token here; ack(token) basic_acks the
    # specific tag and removes it. Without this, N pops before any ack
    # overwrite _last_delivery_tag and only the last-popped message is
    # ackable — silent at-least-once violation.
    self._in_flight_tags: set[_RabbitMQAckToken] = set()
    self._ssl_warning_emitted: bool = False
    # R14-E: one-shot guard for the in-flight-set-overflow warning.
    self._in_flight_overflow_warned: bool = False

  def _capture_connection_snapshot(self) -> _RabbitMQConnectionSnapshot:
    """Capture and revalidate every value consumed by one connect attempt."""
    mode = self.config.mode
    if mode not in (
      RabbitMQMode.STANDALONE,
      RabbitMQMode.CLUSTER,
      RabbitMQMode.MIRRORED_QUEUES,
    ):
      try:
        mode_text = str(mode)
      except (TypeError, ValueError):
        mode_text = getattr(mode, "value", repr(mode))
      raise ConfigurationError(
        f"Unsupported RabbitMQ mode: {mode_text}",
        setting_name="mode",
        setting_value=mode,
      )

    host = self.config.host
    port = self.config.port
    try:
      raw_cluster_nodes = tuple(self.config.cluster_nodes)
    except TypeError:
      raise ConfigurationError(
        "RabbitMQ cluster_nodes must be an iterable of host values.",
        setting_name="cluster_nodes",
      ) from None
    username = self.config.username
    raw_password = secret_value(self.config.password)
    virtual_host = self.config.virtual_host
    ssl_enabled = self.config.ssl_enabled
    ssl_cafile = self.config.ssl_cafile
    ssl_certfile = self.config.ssl_certfile
    ssl_keyfile = self.config.ssl_keyfile
    ssl_verify_mode = self.config.ssl_verify_mode
    heartbeat = self.config.heartbeat
    blocked_connection_timeout = self.config.blocked_connection_timeout
    connection_attempts = self.config.connection_attempts
    retry_delay = self.config.retry_delay
    prefetch_count = self.config.prefetch_count
    prefetch_size = self.config.prefetch_size
    ha_mode = self.config.ha_mode
    ha_params = self.config.ha_params
    ha_sync_mode = self.config.ha_sync_mode

    normalized_host, cluster_nodes = validate_rabbitmq_connection(
      host=host,
      port=port,
      cluster_nodes=raw_cluster_nodes,
      username=username,
      password=raw_password,
      ssl_enabled=ssl_enabled,
      ssl_cafile=ssl_cafile,
      ssl_certfile=ssl_certfile,
      ssl_keyfile=ssl_keyfile,
      ssl_verify_mode=ssl_verify_mode,
    )
    if mode == RabbitMQMode.CLUSTER and not cluster_nodes:
      raise ConfigurationError(
        "RabbitMQ CLUSTER mode requires at least one cluster node.",
        setting_name="cluster_nodes",
      )
    if mode == RabbitMQMode.MIRRORED_QUEUES and not ha_mode:
      raise ConfigurationError(
        "RabbitMQ MIRRORED_QUEUES mode requires ha_mode.",
        setting_name="ha_mode",
      )
    if not isinstance(virtual_host, str) or not virtual_host:
      raise ConfigurationError(
        "RabbitMQ virtual_host must be a non-empty string.",
        setting_name="virtual_host",
      )
    integer_bounds = (
      ("heartbeat", heartbeat, 0),
      ("blocked_connection_timeout", blocked_connection_timeout, 0),
      ("connection_attempts", connection_attempts, 1),
      ("retry_delay", retry_delay, 0),
      ("prefetch_count", prefetch_count, 0),
      ("prefetch_size", prefetch_size, 0),
    )
    for setting_name, value, minimum in integer_bounds:
      if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigurationError(
          f"RabbitMQ {setting_name} must be an integer >= {minimum}.",
          setting_name=setting_name,
        )

    return _RabbitMQConnectionSnapshot(
      mode=mode,
      host=normalized_host,
      port=port,
      cluster_nodes=cluster_nodes,
      username=username,
      password=cast(str, _redact(raw_password)),
      virtual_host=virtual_host,
      ssl_enabled=ssl_enabled,
      ssl_cafile=ssl_cafile,
      ssl_certfile=ssl_certfile,
      ssl_keyfile=ssl_keyfile,
      ssl_verify_mode=ssl_verify_mode,
      heartbeat=heartbeat,
      blocked_connection_timeout=blocked_connection_timeout,
      connection_attempts=connection_attempts,
      retry_delay=retry_delay,
      prefetch_count=prefetch_count,
      prefetch_size=prefetch_size,
      ha_mode=ha_mode,
      ha_params=ha_params,
      ha_sync_mode=ha_sync_mode,
    )

  def connect(self) -> None:
    """Establish connection to RabbitMQ based on deployment mode.

    Creates RabbitMQ connection and channel with mode-specific configuration.

    Raises:
        BackendConnectionError: If the connection cannot be established.
        ConfigurationError: If the configuration is invalid for the mode.
    """
    snapshot = self._capture_connection_snapshot()
    if not snapshot.ssl_enabled and not self._ssl_warning_emitted:
      logger.warning(
        "RabbitMQ loopback connection is using plaintext transport. "
        "Remote endpoints require verified TLS. (warning emitted once per "
        "backend instance)"
      )
      self._ssl_warning_emitted = True
    try:
      if snapshot.mode == RabbitMQMode.STANDALONE:
        self._connect_standalone(snapshot)
      elif snapshot.mode == RabbitMQMode.CLUSTER:
        self._connect_cluster(snapshot)
      else:
        self._connect_mirrored_queues(snapshot)
      logger.debug("Connected to RabbitMQ in %s mode", snapshot.mode.value)
    except ConfigurationError:
      self.disconnect()
      raise
    except Exception:
      self.disconnect()
      msg = f"Failed to connect to RabbitMQ ({snapshot.mode.value})"
      raise BackendConnectionError(
        msg,
        backend_type="rabbitmq",
      ) from None

  def _get_ssl_verify_mode(self, mode: str | None = None) -> ssl.VerifyMode:
    """Get SSL verification mode from config.

    Returns:
        ssl.CERT_NONE, ssl.CERT_OPTIONAL, or ssl.CERT_REQUIRED.
    """
    if mode is None:
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
    *,
    snapshot: _RabbitMQConnectionSnapshot | None = None,
  ) -> pika.ConnectionParameters:
    """Build common RabbitMQ connection parameters.

    Args:
        host: Optional hostname override.
        port: Optional port override.

    Returns:
        ConnectionParameters with common settings.

    Raises:
        ConfigurationError: If the captured connection policy is invalid.
    """
    if snapshot is None:
      snapshot = self._capture_connection_snapshot()
    connection_host = snapshot.host if host is None else host
    connection_port = snapshot.port if port is None else port
    credentials = pika.PlainCredentials(
      snapshot.username,
      snapshot.password,
    )

    # Build SSL options if enabled
    ssl_options = None
    if snapshot.ssl_enabled:
      ssl_context = ssl.create_default_context(cafile=snapshot.ssl_cafile)
      if snapshot.ssl_certfile and snapshot.ssl_keyfile:
        ssl_context.load_cert_chain(
          certfile=snapshot.ssl_certfile,
          keyfile=snapshot.ssl_keyfile,
        )

      verify_mode = self._get_ssl_verify_mode(snapshot.ssl_verify_mode)
      ssl_context.verify_mode = verify_mode
      ssl_context.check_hostname = True
      ssl_options = pika.SSLOptions(ssl_context, connection_host)

    return pika.ConnectionParameters(
      host=connection_host,
      port=connection_port,
      virtual_host=snapshot.virtual_host,
      credentials=credentials,
      heartbeat=snapshot.heartbeat,
      blocked_connection_timeout=snapshot.blocked_connection_timeout,
      connection_attempts=snapshot.connection_attempts,
      retry_delay=snapshot.retry_delay,
      ssl_options=ssl_options,
    )

  def _connect_standalone(
    self, snapshot: _RabbitMQConnectionSnapshot | None = None
  ) -> None:
    """Connect to standalone RabbitMQ node.

    R14-E: ``_setup_qos`` runs BEFORE the connection/channel are committed
    to instance state. On QoS failure the freshly-opened channel/connection
    are closed and ``_channel``/``_connection`` are nulled, so
    :meth:`is_connected` reports False truthfully instead of True-on-half-
    init (mirrors the R25-A1 connect-path cleanup pattern in
    ``connectors.py``).
    """
    if snapshot is None:
      snapshot = self._capture_connection_snapshot()
    parameters = self._build_common_parameters(snapshot=snapshot)
    connection = pika.BlockingConnection(parameters)
    channel = self._open_prepared_channel(connection, snapshot=snapshot)
    self._activate_channel(connection, channel)
    logger.debug(
      "Connected to standalone RabbitMQ at %s:%s", snapshot.host, snapshot.port
    )

  def _connect_cluster(
    self, snapshot: _RabbitMQConnectionSnapshot | None = None
  ) -> None:
    """Connect to RabbitMQ cluster.

    Uses cluster_nodes for failover if primary node is unavailable.

    R14-E: QoS runs before ``_connection``/``_channel`` are committed, with
    null-on-failure cleanup so :meth:`is_connected` stays truthful.
    """
    if snapshot is None:
      snapshot = self._capture_connection_snapshot()
    if snapshot.cluster_nodes:
      parameters = [self._build_common_parameters(snapshot=snapshot)]
      all_hosts = [f"{snapshot.host}:{snapshot.port}"]

      for host, port in snapshot.cluster_nodes:
        parameters.append(
          self._build_common_parameters(host=host, port=port, snapshot=snapshot)
        )
        all_hosts.append(f"{host}:{port}")

      logger.debug("Connecting to RabbitMQ cluster with hosts: %s", all_hosts)
      connection_parameters: pika.ConnectionParameters | list[pika.ConnectionParameters] = (
        parameters
      )
    else:
      logger.debug(
        "Connecting to RabbitMQ cluster at %s:%s", snapshot.host, snapshot.port
      )
      connection_parameters = self._build_common_parameters(snapshot=snapshot)

    connection = pika.BlockingConnection(connection_parameters)
    channel = self._open_prepared_channel(connection, snapshot=snapshot)
    self._activate_channel(connection, channel)
    logger.debug("Connected to RabbitMQ cluster")

  def _open_prepared_channel(
    self,
    connection: pika.BlockingConnection,
    *,
    snapshot: _RabbitMQConnectionSnapshot | None = None,
  ) -> pika.channel.Channel:
    """Open and initialize a channel, closing local handles on failure."""
    channel: pika.channel.Channel | None = None
    try:
      channel = connection.channel()
      self._prepare_channel(channel, snapshot=snapshot)
      return channel
    except Exception:
      if channel is not None:
        with contextlib.suppress(Exception):
          channel.close()
      with contextlib.suppress(Exception):
        connection.close()
      self._channel_session = None
      self._channel = None
      self._connection = None
      raise

  def _activate_channel(
    self,
    connection: pika.BlockingConnection,
    channel: pika.channel.Channel,
  ) -> None:
    """Commit a successfully initialized channel as a new ack generation."""
    self._declared_queues.clear()
    self._last_delivery_tag = None
    self._in_flight_tags.clear()
    self._channel_generation += 1
    self._connection = connection
    self._channel = channel
    # Assign the complete session last so ack/pop readers see either the old
    # session or the new one, never a new channel paired with an old generation.
    self._channel_session = (self._channel_generation, channel)

  def _connect_mirrored_queues(
    self, snapshot: _RabbitMQConnectionSnapshot | None = None
  ) -> None:
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
    if snapshot is None:
      snapshot = self._capture_connection_snapshot()
    self._connect_cluster(snapshot)
    if not (self._channel and snapshot.ha_mode):
      return
    logger.warning(
      "RabbitMQ mirrored-queues HA policy (ha-mode=%s, ha-params=%s, "
      "ha-sync-mode=%s) is configured but NOT applied via AMQP by this "
      "client — set it out-of-band via `rabbitmqctl set_policy` or the "
      "management API. Classic mirroring is deprecated in modern RabbitMQ; "
      "consider quorum queues for HA.",
      snapshot.ha_mode,
      snapshot.ha_params,
      snapshot.ha_sync_mode,
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

  def _prepare_channel(
    self,
    channel: pika.channel.Channel,
    *,
    snapshot: _RabbitMQConnectionSnapshot | None = None,
  ) -> None:
    """Apply consumer QoS and enable synchronous publisher confirms.

    A channel is not published to instance state until both steps succeed.
    With confirm mode enabled, ``BlockingChannel.basic_publish`` waits for a
    broker ack and can report unroutable mandatory messages or broker nacks.

    Args:
        channel: Freshly opened channel that has not been activated yet.

    Raises:
        AMQPError: If QoS or publisher-confirm setup fails.
    """
    self._apply_qos(channel, snapshot=snapshot)
    channel.confirm_delivery()

  def _apply_qos(
    self,
    channel: pika.channel.Channel,
    *,
    snapshot: _RabbitMQConnectionSnapshot | None = None,
  ) -> None:
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
    prefetch_count = (
      self.config.prefetch_count if snapshot is None else snapshot.prefetch_count
    )
    prefetch_size = self.config.prefetch_size if snapshot is None else snapshot.prefetch_size
    if prefetch_count > 0 or prefetch_size > 0:
      channel.basic_qos(
        prefetch_count=prefetch_count,
        prefetch_size=prefetch_size,
      )
      logger.debug(
        "Set QoS: prefetch_count=%d, prefetch_size=%d",
        prefetch_count,
        prefetch_size,
      )

  def disconnect(self) -> None:
    """Close RabbitMQ connection."""
    # Invalidate the ack session before closing either handle. A concurrent
    # stale completion can at worst retain the old channel snapshot; it can
    # never be redirected to a later channel.
    channel = self._channel
    connection = self._connection
    self._channel_session = None
    self._channel = None
    self._connection = None
    if channel is not None:
      with contextlib.suppress(Exception):
        channel.close()
    if connection is not None:
      with contextlib.suppress(Exception):
        connection.close()
    self._declared_queues.clear()
    # R-mq-reconnect: clear ack tracking so it cannot leak to the next channel.
    # Delivery tags are channel-scoped — a tag from the closed channel is
    # invalid on the reconnect's fresh channel (basic_ack would raise
    # PRECONDITION_FAILED). Clearing the legacy slot makes the post-reconnect
    # ack/nack take the "nothing pending" no-op branch instead of firing a
    # stale-tag basic_ack. The in-flight set is also channel-scoped and would
    # otherwise leak across reconnects (unbounded token-set growth for a
    # long-running crawler). At-least-once is preserved regardless — the
    # broker requeues unacked messages on consumer disconnect.
    self._last_delivery_tag = None
    self._in_flight_tags.clear()

  def is_connected(self) -> bool:
    """Check if RabbitMQ is connected.

    Returns:
        True if connection is available.
    """
    try:
      return bool(
        self._connection is not None
        and self._connection.is_open
        and self._channel is not None
        and self._channel.is_open
      )
    except Exception:
      return False

  def ping(self) -> bool:
    """Check RabbitMQ health.

    Uses connection-level is_open check (heartbeat is already configured
    in ConnectionParameters). No channel creation needed, avoiding
    resource leaks from repeated channel allocation.
    """
    return self.is_connected()

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
    _validate_key_name(queue_name, "queue_name")
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
    channel = self._channel
    if channel is None:
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

      confirmed = channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=item,
        properties=properties,
        mandatory=True,
      )
      # Pika's BlockingChannel returns None on confirmed success and raises
      # UnroutableError/NackError on failure. Some compatible channels return
      # a boolean instead, so reject an explicit negative confirmation too.
      if confirmed is False:
        msg = f"RabbitMQ publish to queue {queue_name} was not confirmed"
        raise QueueError(msg, queue_name=queue_name, operation="push")
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
    body, _token = self._basic_get(queue_name, timeout=timeout)
    return body

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, _RabbitMQAckToken | None]:
    """Pop an item together with a channel-scoped ack token.

    Records the delivery tag in the in-flight set so :meth:`ack` can
    ``basic_ack`` the specific message — correct under
    ``CONCURRENT_REQUESTS > 1`` (no single-slot overwrite, no message
    lost/skipped).

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (unused for RabbitMQ).

    Returns:
        ``(body, token)`` where ``token`` carries the AMQP delivery tag and
        channel generation, or ``(None, None)`` when the queue is empty.

    Raises:
        QueueError: If the pop operation fails.
    """
    body, token = self._basic_get(
      queue_name, timeout=timeout, track_in_flight=True
    )
    return (body, token)

  def _track_in_flight(self, token: _RabbitMQAckToken) -> None:
    """Add ``token`` to the diagnostic in-flight set, bounded.

    R14-E: the in-flight set is diagnostic (the broker tracks delivery
    tags — ack correctness does not depend on this set). It grows one
    entry per unacked pop, so a long-running process with slow acks (or a
    bug that never acks) would grow it unbounded. We cap it at
    :data:`_MAX_IN_FLIGHT` and warn-once on overflow. The POP ITSELF is
    never dropped — the caller still receives the message and the broker
    still tracks its delivery tag for ack.

    Args:
        token: The channel-scoped acknowledgement token to track.
    """
    if len(self._in_flight_tags) < _MAX_IN_FLIGHT:
      self._in_flight_tags.add(token)
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
    self,
    queue_name: str,
    *,
    timeout: float = 0.0,
    track_in_flight: bool = False,
  ) -> tuple[bytes | None, _RabbitMQAckToken | None]:
    """Fetch one message via ``basic_get(auto_ack=False)``.

    Shared by :meth:`pop` and :meth:`pop_with_ack` so channel validation,
    queue declaration, and error wrapping live in one place.

    Args:
        queue_name: Name of the queue.
        track_in_flight: When True (pop_with_ack path), add the delivery
            token to the in-flight set so ack(token) can ack it. When False
            (legacy pop path), only set ``_last_delivery_tag``.

    Returns:
        ``(body, token)``. ``body`` is ``None`` when the queue is empty.

    Raises:
        QueueError: If the get fails at the AMQP layer.
    """
    _validate_key_name(queue_name, "queue_name")
    session = self._channel_session
    if session is None:
      msg = "Not connected to RabbitMQ"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      )
    try:
      self._ensure_queue_exists(queue_name)

      channel_generation, channel = session
      deadline = time.monotonic() + timeout if timeout > 0 else None
      while True:
        method_frame, _header_frame, body = channel.basic_get(
          queue=queue_name,
          auto_ack=False,
        )

        if method_frame:
          delivery_tag = method_frame.delivery_tag
          token = _RabbitMQAckToken(delivery_tag, channel_generation)
          self._last_delivery_tag = delivery_tag
          if track_in_flight:
            self._track_in_flight(token)
          return (body, token)
        if deadline is None:
          return (None, None)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
          return (None, None)
        connection = self._connection
        if connection is not None:
          connection.process_data_events(time_limit=min(0.05, remaining))
        else:
          time.sleep(min(0.05, remaining))
    except AMQPError as e:
      msg = f"Failed to pop from queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      ) from e

  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Ack a popped message via ``basic_ack``.

    With a current-generation token (the scheduler path under
    ``CONCURRENT_REQUESTS > 1``), acknowledge the specific message and
    remove it from the in-flight set. Tokens from a closed channel are
    ignored because delivery tags may be reused by the next channel.

    Without a ``token`` (legacy single-pop caller): ``basic_ack`` the
    tracked ``_last_delivery_tag``. Only correct for
    ``CONCURRENT_REQUESTS=1``.

    Idempotent: clears the tracked tag after acking so duplicate ack
    calls on the legacy path are safe (calling ``basic_ack`` with an
    already-acked tag raises a channel error).

    Args:
        queue_name: Name of the queue (unused; kept for interface symmetry).
        token: An opaque token from :meth:`pop_with_ack`, or ``None`` for
            the legacy last-tag path. Unknown or stale tokens are ignored.

    Raises:
        QueueError: If ``basic_ack`` fails at the AMQP layer.
    """
    del queue_name
    if token is not None:
      if not isinstance(token, _RabbitMQAckToken):
        return
      if token._completed:
        return
      session = self._channel_session
      if session is None or token.channel_generation != session[0]:
        return
      channel = session[1]
      try:
        channel.basic_ack(delivery_tag=token.delivery_tag, multiple=False)
      except AMQPError as e:
        msg = f"Failed to ack RabbitMQ message: {e}"
        raise QueueError(msg, operation="ack") from e
      else:
        token._completed = True
        self._in_flight_tags.discard(token)
        if self._last_delivery_tag == token.delivery_tag:
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
    else:
      self._last_delivery_tag = None

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Nack a popped message; requeue it for retry.

    With a current-generation token, negatively acknowledge the specific
    message. Tokens from a closed channel are ignored. Without a token,
    nack the tracked last-popped tag (legacy).

    Args:
        queue_name: Name of the queue (unused; interface symmetry).
        token: An opaque token from :meth:`pop_with_ack`, or ``None``.

    Raises:
        QueueError: If ``basic_nack`` fails at the AMQP layer.
    """
    del queue_name
    if token is not None:
      if not isinstance(token, _RabbitMQAckToken):
        return
      if token._completed:
        return
      session = self._channel_session
      if session is None or token.channel_generation != session[0]:
        return
      channel = session[1]
      try:
        channel.basic_nack(delivery_tag=token.delivery_tag, requeue=True)
      except AMQPError as e:
        msg = f"Failed to nack RabbitMQ message: {e}"
        raise QueueError(msg, operation="nack") from e
      else:
        token._completed = True
        self._in_flight_tags.discard(token)
        if self._last_delivery_tag == token.delivery_tag:
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
    else:
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
    _validate_key_name(queue_name, "queue_name")
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

    Raises:
        QueueError: If the purge fails at the AMQP layer (broker dropped the
            channel, transient AMQPError during reset/teardown).
    """
    _validate_key_name(queue_name, "queue_name")
    if self._channel is None:
      raise QueueError(
        "Not connected to RabbitMQ",
        queue_name=queue_name,
        operation="clear_queue",
      )
    try:
      self._channel.queue_purge(queue=queue_name)
    except AMQPError as e:
      msg = f"Failed to clear queue {queue_name}: {e}"
      raise QueueError(msg, queue_name=queue_name, operation="clear_queue") from e
