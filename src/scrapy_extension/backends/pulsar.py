"""Pulsar backend implementation (queue-only) — subsystem ③.

Implements QueueBackend using Apache Pulsar topics with a Shared subscription
(competing-consumers / work-queue semantics). Does NOT implement SetBackend
or StorageBackend. Priority is ignored — Pulsar has no native priority queue;
items are delivered in topic order (FIFO per partition).

API verified against the pulsar-client sync Python client:
- ``pulsar.Client(service_url)``
- ``client.create_producer(topic)``
- ``producer.send(content)``
- ``client.subscribe(topic, subscription_name, consumer_type, initial_position)``
- ``consumer.receive(timeout_millis=...)``
- ``consumer.acknowledge(msg)``
- ``client.close()``
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

try:
  import pulsar
except ImportError as e:
  raise ImportError(
    "Pulsar backend requires 'pulsar-client'. "
    "Install with: pip install scrapy-extension[pulsar]"
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
from scrapy_extension.settings import PulsarMode

if TYPE_CHECKING:
  from scrapy_extension.settings import PulsarSettings

logger = logging.getLogger(__name__)


def _consumer_type(value: str) -> Any:
  """Map a setting string to a pulsar ConsumerType member.

  Args:
      value: One of Shared, Failover, Exclusive, Key_Shared.

  Returns:
      The corresponding ``pulsar.ConsumerType`` member.

  Raises:
      ConfigurationError: If the value is not a known ConsumerType.
  """
  mapping = {
    "Shared": getattr(pulsar.ConsumerType, "Shared", None),
    "Failover": getattr(pulsar.ConsumerType, "Failover", None),
    "Exclusive": getattr(pulsar.ConsumerType, "Exclusive", None),
    "Key_Shared": getattr(pulsar.ConsumerType, "Key_Shared", None),
  }
  member = mapping.get(value)
  if member is None:
    raise ConfigurationError(
      f"Unknown Pulsar consumer_type: {value!r}. "
      f"Valid: {', '.join(mapping)}.",
      setting_name="consumer_type",
      setting_value=value,
    )
  return member


def _initial_position(value: str) -> Any:
  """Map a setting string to a pulsar InitialPosition member.

  Args:
      value: Earliest or Latest.

  Returns:
      The corresponding ``pulsar.InitialPosition`` member.

  Raises:
      ConfigurationError: If the value is not known.
  """
  mapping = {
    "Earliest": getattr(pulsar.InitialPosition, "Earliest", None),
    "Latest": getattr(pulsar.InitialPosition, "Latest", None),
  }
  member = mapping.get(value)
  if member is None:
    raise ConfigurationError(
      f"Unknown Pulsar initial_position: {value!r}. "
      f"Valid: {', '.join(mapping)}.",
      setting_name="initial_position",
      setting_value=value,
    )
  return member


class PulsarBackend(Backend, QueueBackend):
  """Pulsar backend (queue-only) with Shared-subscription work-queue semantics.

  A Shared subscription gives competing-consumers semantics: each message is
  delivered to exactly one consumer in the subscription, which is the work-queue
  behavior Scrapy's scheduler needs for distributed crawling.

  Does NOT implement SetBackend or StorageBackend. ``queue_len`` returns 0
  (Pulsar backlog stats require the admin REST API, out of scope here).
  ``clear_queue`` is best-effort: it drops the cached consumer/producers and
  relies on topic retention / admin tooling for actual cleanup.

  Ack capability: ``requires_ack=True``, ``supports_concurrent_ack=False``.
  Pulsar ack tracks a SINGLE ``_last_msg`` slot — N pops before any ack
  overwrite it and only the last-popped message is ackable. Under
  ``CONCURRENT_REQUESTS > 1`` this silently violates at-least-once. The
  scheduler gate (round-2) raises ``ConfigurationError`` unless the
  ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`` opt-out is set. The real
  in-flight-set fix is a follow-up (Tier-2).

  Attributes:
      config: PulsarSettings instance.
      _client: The pulsar.Client instance (None until connected).
      _producers: Per-topic cached producers.
      _consumer: The current consumer (None until first pop).
      _subscribed_topic: Topic the consumer is currently subscribed to.
      _last_msg: The last-popped message, tracked for ack/nack.
  """

  requires_ack = True
  supports_concurrent_ack = False

  def __init__(self, config: PulsarSettings) -> None:
    """Initialize the Pulsar backend.

    Args:
        config: Configuration for the Pulsar connection.
    """
    self.config = config
    self._client: Any = None
    self._producers: dict[str, Any] = {}
    self._consumer: Any = None
    self._subscribed_topic: str | None = None
    self._last_msg: Any = None

  def connect(self) -> None:
    """Connect to Pulsar by creating a client from ``service_url``.

    Raises:
        BackendConnectionError: If the client cannot be created.
        ConfigurationError: If the mode is unsupported.
    """
    if self.config.mode not in (PulsarMode.STANDALONE, PulsarMode.CLUSTER):
      raise ConfigurationError(
        f"Unsupported Pulsar mode: {self.config.mode}",
        setting_name="mode",
        setting_value=self.config.mode,
      )
    try:
      kwargs: dict[str, Any] = {}
      if self.config.tls_trust_certs_file:
        kwargs["tls_trust_certs_file"] = self.config.tls_trust_certs_file
        kwargs["allow_insecure_connection"] = self.config.allow_insecure_connection
      if self.config.auth_token:
        kwargs["authentication"] = pulsar.AuthenticationToken(
          secret_value(self.config.auth_token)
        )
      self._client = pulsar.Client(self.config.service_url, **kwargs)
      logger.debug("Connected to Pulsar at %s (%s)", self.config.service_url, self.config.mode.value)
    except Exception as e:
      raise BackendConnectionError(
        f"Failed to connect to Pulsar ({self.config.service_url}): {e}",
        backend_type="pulsar",
      ) from e

  def disconnect(self) -> None:
    """Close the Pulsar client and release producers/consumers."""
    if self._consumer is not None:
      with _suppress_pulsar_errors():
        self._consumer.close()
      self._consumer = None
      self._subscribed_topic = None
    for producer in self._producers.values():
      with _suppress_pulsar_errors():
        producer.close()
    self._producers.clear()
    if self._client is not None:
      with _suppress_pulsar_errors():
        self._client.close()
      self._client = None
    self._last_msg = None

  def is_connected(self) -> bool:
    """Return True if the client has been created."""
    return self._client is not None

  def ping(self) -> bool:
    """Best-effort health check: the client is non-None.

    Pulsar has no lightweight ping on the sync client; a real health check
    requires the admin API. Returns ``is_connected()``.
    """
    return self.is_connected()

  @property
  def backend_type(self) -> BackendType:
    """Return BackendType.PULSAR."""
    return BackendType.PULSAR

  def _topic_name(self, queue_name: str) -> str:
    """Validate and return the Pulsar topic for a queue name.

    Args:
        queue_name: The queue name.

    Returns:
        The topic string ``scrapy-<queue_name>``.

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    return f"scrapy-{queue_name}"

  def _producer_for(self, topic: str) -> Any:
    """Get or create the cached producer for ``topic``.

    Args:
        topic: The Pulsar topic.

    Returns:
        A Producer instance.

    Raises:
        QueueError: If the producer cannot be created.
    """
    if topic in self._producers:
      return self._producers[topic]
    try:
      producer = self._client.create_producer(topic)
      self._producers[topic] = producer
      return producer
    except Exception as e:
      raise QueueError(
        f"Failed to create Pulsar producer for {topic}: {e}",
        queue_name=topic,
        operation="push",
      ) from e

  # QueueBackend implementation
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Publish ``item`` to the topic for ``queue_name`` (priority ignored).

    Args:
        queue_name: Name of the queue.
        item: Item to push (bytes).
        priority: Ignored — Pulsar has no native priority queue.

    Raises:
        QueueError: If the publish fails.
        ValueError: If queue_name contains invalid characters.
    """
    del priority
    topic = self._topic_name(queue_name)
    try:
      producer = self._producer_for(topic)
      producer.send(item)
    except QueueError:
      raise
    except Exception as e:
      raise QueueError(
        f"Failed to push to queue {queue_name}: {e}",
        queue_name=queue_name,
        operation="push",
      ) from e

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Receive the next message from the Shared subscription.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (0 = a short non-blocking poll).

    Returns:
        The message bytes, or None if no message arrived in time.

    Raises:
        QueueError: If the receive fails for a non-timeout reason.
        ValueError: If queue_name contains invalid characters.
    """
    topic = self._topic_name(queue_name)
    # Subscribe errors must propagate (not be masked as "empty"); only the
    # receive call maps a no-message result to None.
    self._ensure_consumer(topic)
    try:
      # timeout=0 -> a short poll; Pulsar needs a positive timeout_millis.
      timeout_ms = int(timeout * 1000) if timeout > 0 else 100
      msg = self._consumer.receive(timeout_millis=timeout_ms)
    except Exception as e:
      # No message within the timeout window is the normal "empty" case, not
      # an error. Pulsar raises on timeout; treat any receive failure as empty.
      logger.debug("Pulsar receive returned no message for %s: %s", queue_name, e)
      return None
    if msg is None:
      return None
    self._last_msg = msg
    return _message_bytes(msg)

  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Acknowledge the last-popped message so it isn't re-delivered.

    ``token`` is accepted for interface compatibility with the concurrency-
    correct ack path (see QueueBackend.pop_with_ack) but not yet used —
    Pulsar still tracks a single ``_last_msg`` slot. The full in-flight-set
    fix for Pulsar is a follow-up; until then pin ``CONCURRENT_REQUESTS=1``
    for strict at-least-once.

    Args:
        queue_name: The queue name.
        token: Unused (accepted for signature compatibility).

    Raises:
        QueueError: If the acknowledge fails.
    """
    del queue_name, token
    if self._consumer is None or self._last_msg is None:
      return
    try:
      self._consumer.acknowledge(self._last_msg)
    except Exception as e:
      raise QueueError(f"Failed to ack Pulsar message: {e}", operation="ack") from e
    finally:
      self._last_msg = None

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Negative-acknowledge the last message for re-delivery.

    If the client supports ``negative_acknowledge``, the message is scheduled
    for immediate re-delivery; otherwise the message is left unacked and
    redelivered via the unacked-timeout / consumer restart (at-least-once).

    Args:
        queue_name: The queue name.
        token: Unused (accepted for signature compatibility).
    """
    del queue_name, token
    if self._consumer is None or self._last_msg is None:
      return
    try:
      nack = getattr(self._consumer, "negative_acknowledge", None)
      if callable(nack):
        nack(self._last_msg)
      # else: leave unacked -> redelivered on timeout / restart
    except Exception as e:
      logger.warning("Pulsar nack failed; message will redeliver on restart: %s", e)
    finally:
      self._last_msg = None

  def queue_len(self, queue_name: str) -> int:
    """Return 0 — Pulsar backlog stats need the admin REST API (out of scope).

    Args:
        queue_name: Name of the queue.

    Returns:
        0 (unsupported; monitoring should query the admin API).

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    return 0

  def clear_queue(self, queue_name: str) -> None:
    """Best-effort clear: drop the cached consumer/producers for the queue.

    Full topic deletion requires the Pulsar admin API; this resets the
    in-process handles so the queue is consumed fresh on next access.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    topic = self._topic_name(queue_name)
    if self._subscribed_topic == topic and self._consumer is not None:
      with _suppress_pulsar_errors():
        self._consumer.close()
      self._consumer = None
      self._subscribed_topic = None
    producer = self._producers.pop(topic, None)
    if producer is not None:
      with _suppress_pulsar_errors():
        producer.close()
    self._last_msg = None

  def _ensure_consumer(self, topic: str) -> None:
    """Create or reuse the consumer for ``topic`` (re-subscribes on change).

    Args:
        topic: The Pulsar topic to subscribe to.
    """
    if self._consumer is not None and self._subscribed_topic == topic:
      return
    try:
      self._consumer = self._client.subscribe(
        topic,
        self.config.subscription_name,
        consumer_type=_consumer_type(self.config.consumer_type),
        initial_position=_initial_position(self.config.initial_position),
        negative_ack_redelivery_delay_ms=self.config.negative_ack_redelivery_delay_ms,
      )
      self._subscribed_topic = topic
    except Exception as e:
      raise QueueError(
        f"Failed to subscribe to Pulsar topic {topic}: {e}",
        queue_name=topic,
        operation="pop",
      ) from e


def _message_bytes(msg: Any) -> bytes:
  """Extract raw bytes from a Pulsar message.

  Uses ``msg.data()`` (the bytes accessor for schema-less producers); falls
  back to ``msg.value()`` then ``str(msg)`` defensively.

  Args:
      msg: A Pulsar Message.

  Returns:
      The message payload as bytes.
  """
  data_fn = getattr(msg, "data", None)
  if callable(data_fn):
    payload = data_fn()
    if isinstance(payload, (bytes, bytearray)):
      return bytes(payload)
    return str(payload).encode("utf-8")
  value_fn = getattr(msg, "value", None)
  if callable(value_fn):
    payload = value_fn()
    if isinstance(payload, (bytes, bytearray)):
      return bytes(payload)
    return str(payload).encode("utf-8")
  return str(msg).encode("utf-8")


class _suppress_pulsar_errors:
  """Context manager that swallows pulsar-client errors on cleanup paths.

  Close() calls during disconnect/clear must not raise — a failing close
  during teardown would mask the original error and break cleanup of the
  remaining handles.
  """

  def __enter__(self) -> _suppress_pulsar_errors:
    return self

  def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
    if exc_type is None:
      return False
    # Swallow any exception raised inside the block.
    logger.debug("Suppressed pulsar cleanup error: %s", exc)
    return True
