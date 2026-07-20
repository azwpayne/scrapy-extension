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
import math
import threading
from typing import TYPE_CHECKING, Any

from scrapy_extension.backends._optional import _is_missing_optional_dependency
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
from scrapy_extension.settings import RocketMQMode

if TYPE_CHECKING:
  from scrapy_extension.settings import RocketMQSettings

logger = logging.getLogger(__name__)

# Module-level warn-once flag for the unsupported-depth signal (Risk 1).
# RocketMQ's deferred-ack model has no broker-side depth RPC, so queue_len
# raises NotImplementedError. The first call warns once per process so
# operators know idle detection / depth backpressure will degrade
# conservatively. Tests reset this for isolation.
_queue_len_warned: bool = False

# RocketMQ 5.x documents a 10-second floor for SimpleConsumer invisible time.
# ``ChangeInvisibleDuration`` uses the same range, so an explicit nack shortens
# the retry delay to this floor rather than waiting out the normal processing
# lease. Zero-delay nack is not supported by the broker.
_MIN_INVISIBLE_DURATION = 10


class _RocketMQAckToken:
  """Consumer-generation-scoped token for one RocketMQ delivery."""

  __slots__ = ("_completed", "consumer", "generation", "message")

  def __init__(self, message: Any, consumer: Any, generation: int) -> None:
    self.message = message
    self.consumer = consumer
    self.generation = generation
    self._completed = False


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
  # caller acks via the opaque token after processing. A crash before ack →
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
    self._consumer_generation = 0
    self._subscribed_topics: set[str] = set()
    # ``SimpleConsumer.await_duration`` is mutable global state on the client.
    # Serialize receive calls so concurrent callers cannot overwrite one
    # another's requested long-poll window before the RPC reads it.
    self._receive_lock = threading.Lock()
    # Legacy single-slot for the ``ack(token=None)`` fallback path. Set by
    # ``pop`` / ``pop_with_ack``; cleared when ``ack`` acks the tracked message.
    # The token path (preferred under ``CONCURRENT_REQUESTS > 1``) does not
    # depend on this slot.
    self._last_msg: Any = None
    self._last_delivery: tuple[Any, int, Any] | None = None

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
      if not _is_missing_optional_dependency(e, "rocketmq"):
        raise
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

      # QueueBackend defines ``pop(timeout=0)`` as non-blocking. The client
      # defaults await_duration to 20 seconds, so initialize it explicitly to
      # zero; each receive updates the public property from that pop's timeout.
      self._consumer = SimpleConsumer(
        config_obj, self.config.consumer_group, await_duration=0
      )
      if self._consumer is None:
        msg = "RocketMQBackend consumer initialization returned None"
        raise BackendConnectionError(msg, backend_type="rocketmq")
      self._consumer.startup()
      self._consumer_generation += 1

      logger.debug(
        "Connected to RocketMQ proxy at %s", self.config.namesrv_address
      )
    except BackendConnectionError:
      self._abort_partial_connect()
      raise
    except Exception as e:
      self._abort_partial_connect()
      msg = f"Failed to connect to RocketMQ: {e}"
      raise BackendConnectionError(msg, backend_type="rocketmq") from e

  def _abort_partial_connect(self) -> None:
    """Detach and best-effort stop clients created by a failed connect."""
    producer = self._producer
    consumer = self._consumer
    self._producer = None
    self._consumer = None
    self._consumer_generation += 1
    self._subscribed_topics.clear()
    self._last_msg = None
    self._last_delivery = None
    for closer in (consumer, producer):
      if closer is not None:
        try:
          closer.shutdown()
        except Exception:
          logger.debug("Failed to abort partial RocketMQ client", exc_info=True)

  def disconnect(self) -> None:
    """Close RocketMQ connections (shutdown producer + consumer)."""
    # apache Producer/Consumer shutdown is best-effort — guard each so a
    # failure in one doesn't skip the other.
    producer = self._producer
    consumer = self._consumer
    self._producer = None
    self._consumer = None
    self._consumer_generation += 1
    self._subscribed_topics.clear()
    self._last_msg = None
    self._last_delivery = None
    for closer, label in ((producer, "producer"), (consumer, "consumer")):
      if closer is not None:
        try:
          closer.shutdown()
        except Exception:  # noqa: BLE001 - disconnect must not raise
          logger.warning(
            "RocketMQ %s shutdown raised; ignoring", label, exc_info=True
          )
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
    _validate_key_name(queue_name, "queue_name")
    return f"{self.config.topic_prefix}_{queue_name}"

  def _ensure_subscribed(
    self, topic_name: str, queue_name: str, consumer: Any
  ) -> None:
    """Ensure the consumer is subscribed to ``topic_name``.

    The apache SimpleConsumer only receives messages from topics it has
    subscribed to. Subscriptions are tracked in-session to avoid re-subscribing
    on every pop.

    Args:
      topic_name: Full topic name to subscribe to.
    """
    if topic_name in self._subscribed_topics:
      return
    try:
      consumer.subscribe(topic_name)
    except Exception as e:
      raise QueueError(
        f"Failed to subscribe to RocketMQ queue {queue_name}: {e}",
        queue_name=queue_name,
        operation="pop",
      ) from e
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
    _validate_key_name(queue_name, "queue_name")
    from rocketmq import Message

    from scrapy_extension.exceptions import QueueError

    if not self.is_connected():
      msg = "Not connected to RocketMQ"
      raise QueueError(msg, queue_name=queue_name, operation="push")

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
        raise QueueError(error, queue_name=queue_name, operation="push")
      self._producer.send(msg)
    except QueueError:
      raise
    except Exception as e:
      err = f"Failed to push to queue: {e}"
      raise QueueError(err, queue_name=queue_name, operation="push") from e

  def _receive_delivery(
    self, queue_name: str, timeout: float
  ) -> tuple[Any | None, Any, int]:
    """Receive one message together with its consumer generation."""
    _validate_key_name(queue_name, "queue_name")
    if not self.is_connected():
      msg = "Not connected to RocketMQ"
      raise QueueError(msg, queue_name=queue_name, operation="pop")

    try:
      topic_name = self._get_topic_name(queue_name)
      consumer = self._consumer
      generation = self._consumer_generation
      if consumer is None:
        error = "RocketMQBackend not connected: consumer is None"
        raise QueueError(error, queue_name=queue_name, operation="pop")
      self._ensure_subscribed(topic_name, queue_name, consumer)
      await_duration = math.ceil(timeout) if timeout > 0 else 0
      with self._receive_lock:
        consumer.await_duration = await_duration
        messages = consumer.receive(1, self.config.invisible_duration)
      if not messages:
        return (None, consumer, generation)
      return (messages[0], consumer, generation)
    except QueueError:
      raise
    except Exception as e:
      msg = f"Failed to pop from queue: {e}"
      raise QueueError(
        msg, queue_name=queue_name, operation="pop"
      ) from e

  def _receive_message(self, queue_name: str, timeout: float) -> Any | None:
    """Receive a single message from ``queue_name`` WITHOUT acking.

    Args:
      queue_name: Name of the queue.
      timeout: Seconds to wait for a message. The SDK exposes this as the
        consumer's ``await_duration`` property, separate from the processing
        lease passed to ``receive``.

    Returns:
      The received message object, or None if no message was available.

    Raises:
      QueueError: If not connected or the receive fails.
    """
    message, _consumer, _generation = self._receive_delivery(queue_name, timeout)
    return message

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
      timeout: Seconds to wait (0 = non-blocking).

    Returns:
      Popped item, or None if queue is empty.
    """
    msg, consumer, generation = self._receive_delivery(queue_name, timeout)
    if msg is None:
      return None
    self._last_msg = msg
    self._last_delivery = (consumer, generation, msg)
    return self._extract_body(msg)

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop an item together with a consumer-generation-scoped ack token.

    Does NOT ack — the caller acks via :meth:`ack` after processing.
    :class:`BackendScheduler` threads the opaque token through
    ``request.meta["_backend_ack_token"]``.

    Args:
      queue_name: Name of the queue.
      timeout: Seconds to wait (0 = non-blocking).

    Returns:
      ``(body_bytes, msg_token)`` or ``(None, None)`` when empty.

    Raises:
      QueueError: If not connected or the receive fails.
    """
    msg, consumer, generation = self._receive_delivery(queue_name, timeout)
    if msg is None:
      return (None, None)
    self._last_msg = msg
    self._last_delivery = (consumer, generation, msg)
    token = _RocketMQAckToken(msg, consumer, generation)
    return (self._extract_body(msg), token)

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

    With a token: ack the specific message on the consumer generation that
    delivered it. With ``token=None`` (legacy single-pop caller): ack the
    tracked ``_last_msg``.

    Args:
      queue_name: Name of the queue (unused; kept for interface symmetry).
      token: The opaque token returned by :meth:`pop_with_ack`, or ``None`` to
        ack the last-popped message.

    Raises:
      QueueError: If the underlying ack call fails.
    """
    del queue_name
    token_obj: _RocketMQAckToken | None = None
    if token is not None:
      if not isinstance(token, _RocketMQAckToken) or token._completed:
        return
      if (
        token.generation != self._consumer_generation
        or token.consumer is not self._consumer
      ):
        return
      token_obj = token
      target = token.message
      consumer = token.consumer
    else:
      target = self._last_msg
      if target is None:
        return
      if self._last_delivery is not None:
        consumer, generation, delivery = self._last_delivery
        if (
          delivery is not target
          or generation != self._consumer_generation
          or consumer is not self._consumer
        ):
          return
      else:
        consumer = self._consumer
      if consumer is None:
        return
    try:
      consumer.ack(target)
    except Exception as e:
      msg = f"Failed to ack RocketMQ message: {e}"
      raise QueueError(msg, operation="ack") from e
    else:
      if token_obj is not None:
        token_obj._completed = True
      # Clear the legacy slot when we acked the tracked message so a later
      # ack(token=None) is a no-op, not a re-ack.
      if self._last_msg is target:
        self._last_msg = None
        self._last_delivery = None

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Shorten a popped message's lease to RocketMQ's 10-second floor.

    The client has no dedicated nack call, but its
    ``change_invisible_duration`` operation can schedule prompt redelivery.
    RocketMQ rejects durations below 10 seconds, so unlike SQS this cannot
    make a message immediately visible.

    Args:
      queue_name: Name of the queue (unused; interface symmetry).
      token: The opaque token returned by :meth:`pop_with_ack`, or ``None`` to
        nack the last-popped message.

    Raises:
      QueueError: If changing the message lease fails.
    """
    del queue_name
    token_obj: _RocketMQAckToken | None = None
    if token is not None:
      if not isinstance(token, _RocketMQAckToken) or token._completed:
        return
      if (
        token.generation != self._consumer_generation
        or token.consumer is not self._consumer
      ):
        return
      token_obj = token
      target = token.message
      consumer = token.consumer
    else:
      target = self._last_msg
      if target is None:
        return
      if self._last_delivery is not None:
        consumer, generation, delivery = self._last_delivery
        if (
          delivery is not target
          or generation != self._consumer_generation
          or consumer is not self._consumer
        ):
          return
      else:
        consumer = self._consumer
      if consumer is None:
        return
    try:
      consumer.change_invisible_duration(
        target, _MIN_INVISIBLE_DURATION
      )
    except Exception as e:
      msg = f"Failed to nack RocketMQ message: {e}"
      raise QueueError(msg, operation="nack") from e
    else:
      if token_obj is not None:
        token_obj._completed = True
      if self._last_msg is target:
        self._last_msg = None
        self._last_delivery = None

  def queue_len(self, queue_name: str) -> int:
    """Report that queue depth is unsupported by the RocketMQ client.

    RocketMQ's deferred-ack model has no broker-side depth RPC. Returning 0
    would falsely report an empty queue and can make Scrapy enter idle while
    work is pending. ``NotImplementedError`` lets queue monitoring ignore the
    sample, backpressure continue to pop, and pending detection stay
    conservative. A one-time warning surfaces the limitation to operators.

    Args:
      queue_name: Name of the queue.

    Raises:
      NotImplementedError: Always; the client has no broker-side depth RPC.
    """
    global _queue_len_warned
    _validate_key_name(queue_name, "queue_name")
    if not _queue_len_warned:
      _queue_len_warned = True
      logger.warning(
        "RocketMQ queue_len() is unsupported (deferred-ack model has no "
        "broker-side depth RPC). Pending detection will stay conservative; "
        "monitor via pop-rate / consumer-liveness instead. This warning "
        "fires once per process."
      )
    raise NotImplementedError(
      "RocketMQ queue depth is unsupported: no broker-side depth RPC"
    )

  def clear_queue(self, queue_name: str) -> None:
    """Report that RocketMQ broker-side queue purge is unsupported.

    Args:
      queue_name: Name of the queue.

    Raises:
      QueueError: If disconnected or because the client has no purge API.
    """
    _validate_key_name(queue_name, "queue_name")
    if not self.is_connected():
      msg = "Not connected to RocketMQ"
      raise QueueError(
        msg, queue_name=queue_name, operation="clear_queue"
      )
    msg = "clear_queue is not supported by the RocketMQ client"
    raise QueueError(
      msg, queue_name=queue_name, operation="clear_queue"
    )


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
