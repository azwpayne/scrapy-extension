"""RocketMQ backend implementation (apache ``rocketmq-python-client`` 5.1.1 gRPC).

Rewritten (#44) from the prior fictional-API stub. The original backend's
``connect()`` imports (``rocketmq.consumer.SimpleConsumer``,
``rocketmq.auth.credentials.PlainCredentials``, ``rocketmq.endpoint.Endpoint``,
``rocketmq.message.Message``) matched NO released client — lazy-import hid this
since project inception; the backend had never connected to any broker. This
implementation targets the apache RocketMQ 5.x gRPC client
(``rocketmq-python-client`` 5.1.1, pure-Python, no native lib — installable on
CI without the librocketmq native-lib pain that blocked the old ctypes client).

API map (apache 5.1.1, verified against apache/rocketmq-clients python/example):
- ``ClientConfiguration(endpoints: str, credentials, namespace='', request_timeout=3)``
- ``Credentials(ak='', sk='')``
- ``Producer(config, topics=None)`` / ``producer.startup()`` / ``producer.send(msg) -> SendReceipt``
- ``SimpleConsumer(config, consumer_group, subscription=None, await_duration=20)`` /
  ``consumer.startup()`` / ``consumer.subscribe(topic)`` /
  ``consumer.receive(max_num, invisible_duration) -> list[Message] | None`` /
  ``consumer.ack(msg)``
- ``Message()`` with ``.topic`` / ``.body`` (bytes) / ``.keys`` / ``.tag`` /
  ``.add_property(k, v)``; received messages carry ``.message_id``.

Endpoints are the gRPC PROXY (port 8081), NOT the legacy NameServer (9876) —
the broker must run with ``--enable-proxy`` (see tests/integration/docker-compose.yml).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

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
  """RocketMQ backend implementation (apache 5.1.1 gRPC client).

  Note: RocketMQ only supports QueueBackend operations.
  SetBackend and StorageBackend are not supported by RocketMQ. Configuring
  RocketMQ for the set/storage component is rejected at config time by
  ``resolve_backend_config`` (RocketMQ is excluded from
  ``SET_CAPABLE_BACKENDS`` / ``STORAGE_CAPABLE_BACKENDS``). If that gating is
  bypassed, instantiation fails fast via the dedicated guard classes
  ``RocketMQSetBackend`` / ``RocketMQStorageBackend`` (raise
  ``ConfigurationError`` in ``__init__``).
  """

  # Ack capability (initiative #4 — at-least-once): the apache SimpleConsumer
  # uses a deferred-ack model. ``receive`` yields messages WITHOUT acking; the
  # caller acks via ``ack(token=msg)`` after processing. A crash before ack →
  # the broker's invisible-duration window redelivers the message (at-least-once).
  # ``supports_concurrent_ack=True`` because ack is per-message (no single-slot
  # overwrite) — correct under ``CONCURRENT_REQUESTS > 1``.
  requires_ack = True
  supports_concurrent_ack = True

  def __init__(self, config: RocketMQSettings) -> None:
    """Initialize RocketMQ backend.

    Args:
        config: Configuration for RocketMQ connection.
    """
    self.config = config
    self._producer: Any = None
    self._consumer: Any = None
    self._subscribed_topics: set[str] = set()
    # Legacy single-slot for the ``ack(token=None)`` fallback path. Set by
    # ``pop`` / ``pop_with_ack``; cleared when ``ack`` acks the tracked message.
    # The token path (preferred under ``CONCURRENT_REQUESTS > 1``) does not
    # depend on this slot.
    self._last_msg: Any = None

  def connect(self) -> None:
    """Establish connection to RocketMQ (gRPC proxy).

    Raises:
      BackendConnectionError: If connection / startup fails, or the optional dep
        is missing.
      ConfigurationError: If configuration is invalid.
    """
    try:
      from rocketmq import ClientConfiguration, Credentials, Producer, SimpleConsumer
    except ImportError as e:
      msg = f"rocketmq-python-client not installed: {e}"
      raise BackendConnectionError(msg, backend_type="rocketmq") from e

    if self.config.mode not in (
      RocketMQMode.STANDALONE,
      RocketMQMode.CLUSTER,
      RocketMQMode.CLOUD,
    ):
      try:
        mode_text = str(self.config.mode)
      except (TypeError, ValueError):
        mode_text = getattr(self.config.mode, "value", repr(self.config.mode))
      msg = f"Unsupported RocketMQ mode: {mode_text}"
      raise ConfigurationError(
        msg, setting_name="mode", setting_value=self.config.mode
      )

    try:
      # Credentials: empty Credentials() for no-auth (the broker fixture runs
      # with auth disabled); Credentials(ak, sk) when both are provided.
      if self.config.access_key and self.config.secret_key:
        credentials = Credentials(
          secret_value(self.config.access_key),
          secret_value(self.config.secret_key),
        )
      else:
        credentials = Credentials()

      # ``namesrv_address`` is, in this gRPC rewrite, the PROXY endpoints
      # (``host:8081``). The field name is kept for settings-schema
      # compatibility; the value must point at the broker's gRPC proxy, NOT the
      # legacy NameServer (9876). The broker must run with ``--enable-proxy``.
      request_timeout = (
        self.config.send_timeout // 1000
        if self.config.send_timeout >= 1000
        else 3
      )
      config_obj = ClientConfiguration(
        endpoints=self.config.namesrv_address,
        credentials=credentials,
        request_timeout=request_timeout,
      )

      self._producer = Producer(config_obj)
      if self._producer is None:
        msg = "RocketMQBackend producer initialization returned None"
        raise BackendConnectionError(msg, backend_type="rocketmq")
      self._producer.startup()

      self._consumer = SimpleConsumer(config_obj, self.config.consumer_group)
      if self._consumer is None:
        msg = "RocketMQBackend consumer initialization returned None"
        raise BackendConnectionError(msg, backend_type="rocketmq")
      self._consumer.startup()

      logger.debug(
        "Connected to RocketMQ proxy at %s", self.config.namesrv_address
      )
    except BackendConnectionError:
      raise
    except Exception as e:
      msg = f"Failed to connect to RocketMQ: {e}"
      raise BackendConnectionError(msg, backend_type="rocketmq") from e

  def disconnect(self) -> None:
    """Close RocketMQ connections (shutdown producer + consumer)."""
    # apache Producer/Consumer shutdown is best-effort — guard each so a
    # failure in one doesn't skip the other.
    for closer, label in (
      (self._producer, "producer"),
      (self._consumer, "consumer"),
    ):
      if closer is not None:
        try:
          closer.shutdown()
        except Exception:  # noqa: BLE001 - disconnect must not raise
          logger.warning(
            "RocketMQ %s shutdown raised; ignoring", label, exc_info=True
          )
    self._producer = None
    self._consumer = None
    self._subscribed_topics.clear()
    self._last_msg = None
    logger.debug("Disconnected from RocketMQ")

  def is_connected(self) -> bool:
    """Check if RocketMQ is connected (both clients running).

    Returns:
      True if producer and consumer clients are initialized and running.
    """
    if self._producer is None or self._consumer is None:
      return False
    # apache clients expose ``is_running`` as a BOOL PROPERTY (not a method)
    # — True after startup(), False after shutdown().
    try:
      return bool(self._producer.is_running and self._consumer.is_running)
    except Exception:  # noqa: BLE001 - is_connected must not raise
      return False

  def ping(self) -> bool:
    """Check RocketMQ health (local-state check).

    Returns:
      True if ``is_connected`` reports both clients running.

    Note:
      Local-state check, not a broker round-trip — same caveat as the prior
      implementation (R1-P2-16). A real liveness probe would need a broker
      round-trip; the right one for the gRPC proxy is an open design question.
    """
    return self.is_connected()

  @property
  def backend_type(self) -> BackendType:
    """Return backend type."""
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

    The apache SimpleConsumer only receives messages from topics it has
    subscribed to. Subscriptions are tracked in-session to avoid re-subscribing
    on every pop.

    Args:
      topic_name: Full topic name to subscribe to.
    """
    if topic_name in self._subscribed_topics:
      return
    if self._consumer is not None:
      try:
        self._consumer.subscribe(topic_name)
        self._subscribed_topics.add(topic_name)
      except Exception:  # noqa: BLE001 - subscription best-effort; receive surfaces real errors
        logger.debug(
          "subscribe(%s) raised; will retry next pop",
          topic_name,
          exc_info=True,
        )

  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push item to queue.

    Args:
      queue_name: Name of the queue.
      item: Item to push (bytes).
      priority: Priority value (higher = more urgent).

    Raises:
      QueueError: If push fails.
    """
    from rocketmq import Message

    from scrapy_extension.exceptions import QueueError

    if not self.is_connected():
      msg = "Not connected to RocketMQ"
      raise QueueError(msg)

    try:
      topic_name = self._get_topic_name(queue_name)
      msg = Message()
      msg.topic = topic_name
      msg.body = item
      # apache Message has no native priority field; carry it as ``keys`` so a
      # priority-aware consumer could read it. rocketmq topic ordering is by
      # queue, not priority — the priority arg is accepted for interface
      # symmetry but does not reorder within a topic.
      msg.keys = str(priority)
      if self._producer is None:
        error = "RocketMQBackend not connected: producer is None"
        raise QueueError(error)
      self._producer.send(msg)
    except QueueError:
      raise
    except Exception as e:
      err = f"Failed to push to queue: {e}"
      raise QueueError(err, queue_name=queue_name) from e

  def _receive_message(self, queue_name: str, timeout: float) -> Any | None:
    """Receive a single message from ``queue_name`` WITHOUT acking.

    Args:
      queue_name: Name of the queue.
      timeout: Seconds to wait (apache ``receive`` uses an invisible-duration,
        not a polling timeout; this value is used as the invisible duration so
        an unacked message becomes re-visible after it).

    Returns:
      The received message object, or None if no message was available.

    Raises:
      QueueError: If not connected or the receive fails.
    """
    from scrapy_extension.exceptions import QueueError

    if not self.is_connected():
      msg = "Not connected to RocketMQ"
      raise QueueError(msg)

    try:
      topic_name = self._get_topic_name(queue_name)
      self._ensure_subscribed(topic_name)
      if self._consumer is None:
        error = "RocketMQBackend not connected: consumer is None"
        raise QueueError(error)
      # ``receive(max_message_num, invisible_duration)`` — invisible_duration
      # (SECONDS) is how long the message stays invisible to peers (the
      # redelivery window for at-least-once). The apache broker enforces a
      # 10s floor (error 40011 "the invisibleTime is too small. min is 10000"
      # below it), so clamp small/polling-style timeouts up to the floor;
      # ``receive`` itself long-polls up to the consumer's await_duration.
      invisible_duration = max(int(timeout), 10) if timeout > 0 else 15
      messages = self._consumer.receive(1, invisible_duration)
      if not messages:
        return None
      return messages[0]
    except QueueError:
      raise
    except Exception as e:
      msg = f"Failed to pop from queue: {e}"
      raise QueueError(msg, queue_name=queue_name) from e

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop item from queue WITHOUT acking (deferred-ack model).

    Returns the message body; the message itself is tracked in ``_last_msg``
    for the legacy ``ack`` (``token=None``) path. Ack fires only when the
    caller explicitly invokes :meth:`ack` — a crash before ack leaves the
    message unacked → the broker's invisible-duration redelivers it
    (at-least-once).

    Under :class:`BackendScheduler`, :meth:`pop_with_ack` is the preferred path
    (per-message token, correct under ``CONCURRENT_REQUESTS > 1``).

    Args:
      queue_name: Name of the queue.
      timeout: Seconds to wait (0 = use default invisible-duration).

    Returns:
      Popped item, or None if queue is empty.
    """
    msg = self._receive_message(queue_name, timeout)
    if msg is None:
      return None
    self._last_msg = msg
    return self._extract_body(msg)

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop an item together with an ack token (the message object itself).

    Does NOT ack — the caller acks via :meth:`ack` (``token=msg``) after
    processing. :class:`BackendScheduler` threads the token through
    ``request.meta["_backend_ack_token"]``.

    Args:
      queue_name: Name of the queue.
      timeout: Seconds to wait (0 = use default invisible-duration).

    Returns:
      ``(body_bytes, msg_token)`` or ``(None, None)`` when empty.

    Raises:
      QueueError: If not connected or the receive fails.
    """
    msg = self._receive_message(queue_name, timeout)
    if msg is None:
      return (None, None)
    self._last_msg = msg
    return (self._extract_body(msg), msg)

  @staticmethod
  def _extract_body(msg: Any) -> bytes:
    """Extract the body bytes from a received message.

    The apache ``Message.body`` is ``bytes``; defensive coercion handles any
    dynamic typing from the client.

    Args:
      msg: The received message object.

    Returns:
      The message body as bytes.
    """
    body = getattr(msg, "body", None)
    if body is None:
      return b""
    if isinstance(body, bytes):
      return body
    if isinstance(body, (bytearray, memoryview)):
      return bytes(body)
    return str(body).encode()

  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Ack a popped message (deferred from :meth:`pop` / :meth:`pop_with_ack`).

    With a ``token`` (the message object): ack the specific message. With
    ``token=None`` (legacy single-pop caller): ack the tracked ``_last_msg``.

    Args:
      queue_name: Name of the queue (unused; kept for interface symmetry).
      token: The message object returned by :meth:`pop_with_ack`, or ``None``
        to ack the last-popped message.

    Raises:
      QueueError: If the underlying ack call fails.
    """
    from scrapy_extension.exceptions import QueueError

    del queue_name
    target = token if token is not None else self._last_msg
    if target is None or self._consumer is None:
      return
    try:
      self._consumer.ack(target)
    except Exception as e:
      msg = f"Failed to ack RocketMQ message: {e}"
      raise QueueError(msg, operation="ack") from e
    finally:
      # Clear the legacy slot when we acked the tracked message so a later
      # ack(token=None) is a no-op, not a re-ack.
      if self._last_msg is target:
        self._last_msg = None

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Nack a popped message — deliberate no-op (invisible-duration redelivery).

    The apache gRPC client has no explicit nack; an unacked message is
    automatically redelivered after the invisible-duration window (same
    at-least-once model as SQS/RocketMQ remoting). Logged at debug so the
    retry path is observable without pretending work was done.

    Args:
      queue_name: Name of the queue (unused; interface symmetry).
      token: The message object (unused; interface symmetry).
    """
    del queue_name, token
    logger.debug(
      "RocketMQ nack: no explicit nack API — message will redeliver "
      "after the invisible-duration window (at-least-once)."
    )

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
    # RocketMQ doesn't support purge, log warning.
    logger.warning("clear_queue not supported in RocketMQ")


# ---------------------------------------------------------------------------
# Set / Storage — class-level guard (replaces former per-method stubs)
# ---------------------------------------------------------------------------
#
# RocketMQ is excluded from SET_CAPABLE_BACKENDS and STORAGE_CAPABLE_BACKENDS
# at the connector layer, so these classes are unreachable under normal config
# resolution (resolve_backend_config raises ConfigurationError first). They
# exist as the fail-fast surface for anyone who bypasses that gating.


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

  Construction fails fast with a typed ``ConfigurationError``.
  """

  def __init__(self, config: RocketMQSettings) -> None:
    """Reject construction — RocketMQ does not support the set interface.

    Raises:
        ConfigurationError: Always.
    """
    raise _unsupported_component_guard("set", "SCRAPY_SET_BACKEND_TYPE")


class RocketMQStorageBackend(RocketMQBackend):
  """Guard class: RocketMQ cannot serve the ``StorageBackend`` interface.

  Construction fails fast with a typed ``ConfigurationError``.
  """

  def __init__(self, config: RocketMQSettings) -> None:
    """Reject construction — RocketMQ does not support the storage interface.

    Raises:
        ConfigurationError: Always.
    """
    raise _unsupported_component_guard("storage", "SCRAPY_STORAGE_BACKEND_TYPE")
