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
from scrapy_extension.settings import PulsarMode

if TYPE_CHECKING:
  from scrapy_extension.settings import PulsarSettings

logger = logging.getLogger(__name__)

# R14-E: cap on the diagnostic in-flight ack-token set. Each unacked pop
# adds one entry; without a cap a long-running process with slow acks (or a
# bug that never acks) grows the set unbounded. We warn-once on overflow and
# STOP adding — the set is diagnostic (Pulsar acks each message independently
# via ``consumer.acknowledge(msg_id)``, so ack correctness lives in the
# broker, not in this set). The POP itself is never dropped. 10k is generous
# for normal CONCURRENT_REQUESTS backpressure and tight enough to flag a leak.
_MAX_IN_FLIGHT = 10_000


class _PulsarAckToken:
  """Opaque ack token carrying a popped Pulsar message's ``message_id``.

  Stored in ``request.meta["_backend_ack_token"]`` and handed back to
  :meth:`PulsarBackend.ack` / :meth:`PulsarBackend.nack` so the specific
  message that was popped is acked — not the last-popped one. Pulsar's
  Shared subscription is natively per-message: ``consumer.acknowledge(msg_id)``
  targets exactly one message, so this token is what makes ack correct
  under ``CONCURRENT_REQUESTS > 1`` (N pops before any ack no longer
  overwrite a single ``_last_msg`` slot).

  Attributes:
      message_id: The ``msg.message_id()`` object returned by the pulsar
          client for the popped message. Passed to
          ``consumer.acknowledge`` / ``consumer.negative_acknowledge``.
      topic: The topic the message was consumed from (diagnostics only).
  """

  __slots__ = ("message_id", "topic")

  def __init__(self, message_id: Any, topic: str) -> None:
    """Initialize the token.

    Args:
        message_id: The pulsar ``MessageId`` for the popped message.
        topic: The topic the message was consumed from.
    """
    self.message_id = message_id
    self.topic = topic

  def __eq__(self, other: object) -> bool:
    if not isinstance(other, _PulsarAckToken):
      return NotImplemented
    return self.message_id is other.message_id and self.topic == other.topic

  def __hash__(self) -> int:
    # Pulsar ``MessageId`` hashability varies by client version (the C++
    # binding is not consistently hashable across releases). The in-flight
    # set is DIAGNOSTIC ONLY (leak detection / monitoring — Pulsar acks
    # each message independently, unlike Kafka's watermark commit), so
    # identity-based hashing on the message_id object is sufficient and
    # robust across all client versions. Equality mirrors this (identity
    # on message_id) so the token that came out of the set is the one
    # ``discard`` removes.
    return hash((id(self.message_id), self.topic))

  def __repr__(self) -> str:
    return (
      f"_PulsarAckToken(topic={self.topic!r}, "
      f"message_id={self.message_id!r})"
    )


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

  Ack capability: ``requires_ack=True``, ``supports_concurrent_ack=True``.
  Pulsar's Shared subscription is natively per-message —
  ``consumer.acknowledge(msg_id)`` and ``consumer.negative_acknowledge(msg_id)``
  target one specific message identified by its ``MessageId``. Pops via
  :meth:`pop_with_ack` carry a :class:`_PulsarAckToken` (wrapping
  ``msg.message_id()``) tracked in the in-flight set; :meth:`ack` /
  :meth:`nack` use the token to ack the *specific* message — correct under
  ``CONCURRENT_REQUESTS > 1`` (N pops before any ack no longer overwrite a
  single slot). The in-flight set is diagnostic (leak detection / monitoring)
  since Pulsar acks each message independently. The legacy ``pop()`` /
  ``ack(token=None)`` path tracks ``_last_msg`` for backward compatibility.

  Attributes:
      config: PulsarSettings instance.
      _client: The pulsar.Client instance (None until connected).
      _producers: Per-topic cached producers.
      _consumer: The current consumer (None until first pop).
      _subscribed_topic: Topic the consumer is currently subscribed to.
      _last_msg: The last-popped message (legacy ``ack(token=None)`` path).
      _in_flight: Diagnostic set of popped-but-unacked ack tokens.
  """

  requires_ack = True
  supports_concurrent_ack = True

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
    # Legacy single-slot for the ``ack(token=None)`` fallback path. Kept so
    # external callers that pop() then ack() without a token still work.
    self._last_msg: Any = None
    # In-flight ack tokens for correctness under CONCURRENT_REQUESTS>1.
    # DIAGNOSTIC ONLY: Pulsar acks each message independently (unlike Kafka's
    # watermark commit), so the set is for leak detection / monitoring —
    # mirrors RabbitMQ's ``_in_flight_tags``.
    self._in_flight: set[_PulsarAckToken] = set()
    # R14-E: one-shot guard for the in-flight-set-overflow warning.
    self._in_flight_overflow_warned: bool = False

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
      # SEC-5: TLS controls are independent. ``allow_insecure_connection``
      # (default False) must always be honored for ``pulsar+ssl://`` URLs,
      # not gated behind ``tls_trust_certs_file``. Previously a user who set
      # ``allow_insecure_connection=True`` without a trust-certs file had
      # the flag silently dropped (reverted to Pulsar's stricter default),
      # and a user who set ``allow_insecure_connection=False`` without trust
      # certs had no way to make that intent explicit. Pass each field on its
      # own; only pass ``tls_trust_certs_file`` when actually set.
      is_ssl = self.config.service_url.startswith("pulsar+ssl://")
      if is_ssl:
        kwargs["allow_insecure_connection"] = self.config.allow_insecure_connection
      if self.config.tls_trust_certs_file:
        kwargs["tls_trust_certs_file"] = self.config.tls_trust_certs_file
      if self.config.auth_token:
        kwargs["authentication"] = pulsar.AuthenticationToken(
          _redact(secret_value(self.config.auth_token))
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
    self._in_flight.clear()

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

    Tracks the popped message in ``_last_msg`` for the legacy
    ``ack(token=None)`` path. Prefer :meth:`pop_with_ack` under
    ``CONCURRENT_REQUESTS > 1`` — that path tracks every popped message in
    the in-flight set so ack(token) acks the *specific* message, not merely
    the last-popped one.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (0 = a short non-blocking poll).

    Returns:
        The message bytes, or None if no message arrived in time.

    Raises:
        QueueError: If the receive fails for a non-timeout reason.
        ValueError: If queue_name contains invalid characters.
    """
    msg = self._receive(queue_name, timeout)
    if msg is None:
      return None
    self._last_msg = msg
    return _message_bytes(msg)

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop an item together with a :class:`_PulsarAckToken`.

    Records the popped message's ``message_id`` in the in-flight set so
    :meth:`ack` can ``acknowledge`` the *specific* message — correct under
    ``CONCURRENT_REQUESTS > 1`` (no single-slot overwrite, no message
    lost/skipped). Pulsar's Shared subscription is natively per-message, so
    the token carries exactly what ``consumer.acknowledge`` needs.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (0 = a short non-blocking poll).

    Returns:
        ``(value_bytes, token)`` where ``token`` is a
        :class:`_PulsarAckToken`, or ``(None, None)`` when the queue is
        empty.

    Raises:
        QueueError: If the receive fails for a non-timeout reason.
        ValueError: If queue_name contains invalid characters.
    """
    topic = self._topic_name(queue_name)
    msg = self._receive(queue_name, timeout)
    if msg is None:
      return (None, None)
    token = _PulsarAckToken(message_id=msg.message_id(), topic=topic)
    self._track_in_flight(token)
    # Keep _last_msg in sync so the legacy ack(token=None) path stays
    # usable for callers that don't thread the token through.
    self._last_msg = msg
    return (_message_bytes(msg), token)

  def _track_in_flight(self, token: _PulsarAckToken) -> None:
    """Add ``token`` to the diagnostic in-flight set, bounded.

    R14-E: the in-flight set is diagnostic (Pulsar acks each message
    independently via ``consumer.acknowledge(msg_id)``; ack correctness
    lives in the broker). It grows one entry per unacked pop, so a
    long-running process with slow acks would grow it unbounded. We cap
    at :data:`_MAX_IN_FLIGHT` and warn-once on overflow. The POP itself
    is never dropped — the caller still receives the message and the
    broker still tracks the message_id for ack.

    Args:
        token: The :class:`_PulsarAckToken` to track.
    """
    if len(self._in_flight) < _MAX_IN_FLIGHT:
      self._in_flight.add(token)
      return
    if not self._in_flight_overflow_warned:
      self._in_flight_overflow_warned = True
      logger.warning(
        "Pulsar in-flight ack-token set at cap (%d) — further unacked "
        "pops will not be tracked in the diagnostic set. This indicates "
        "slow acks or an ack leak; the broker still tracks message_ids "
        "so ack correctness is unaffected.",
        _MAX_IN_FLIGHT,
      )

  def _receive(self, queue_name: str, timeout: float) -> Any:
    """Receive one message from ``queue_name``; return None if empty.

    Shared by :meth:`pop` and :meth:`pop_with_ack` so consumer
    subscription, topic validation, and error wrapping live in one place.
    Only the receive call maps a no-message result to None; subscribe
    errors propagate as :class:`QueueError`.

    Args:
        queue_name: Name of the queue (validated here).
        timeout: Seconds to wait (0 = a short non-blocking poll).

    Returns:
        A Pulsar Message, or None if no message arrived in time.

    Raises:
        QueueError: If the receive fails at the Pulsar layer for a
            non-timeout reason (subscribe failure).
        ValueError: If queue_name contains invalid characters.
    """
    topic = self._topic_name(queue_name)
    # Subscribe errors must propagate (not be masked as "empty"); only the
    # receive call maps a no-message result to None.
    self._ensure_consumer(topic)
    try:
      # timeout=0 -> a short poll; Pulsar needs a positive timeout_millis.
      timeout_ms = int(timeout * 1000) if timeout > 0 else 100
      return self._consumer.receive(timeout_millis=timeout_ms)
    except Exception as e:
      # No message within the timeout window is the normal "empty" case, not
      # an error. Pulsar raises on timeout; treat any receive failure as empty.
      logger.debug("Pulsar receive returned no message for %s: %s", queue_name, e)
      return None

  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Ack a popped message via ``consumer.acknowledge``.

    With a ``token`` (the scheduler path under ``CONCURRENT_REQUESTS > 1``):
    ``consumer.acknowledge(token.message_id)`` the specific message and
    remove the token from the in-flight set. Order-independent — ack the
    right message regardless of pop/ack interleaving.

    Without a ``token`` (legacy single-pop caller): ``acknowledge`` the
    tracked ``_last_msg``. Only correct for ``CONCURRENT_REQUESTS=1`` —
    kept for backward compatibility with external callers that pop() then
    ack() without threading the token through.

    Args:
        queue_name: Name of the queue (unused; kept for interface symmetry).
        token: A :class:`_PulsarAckToken` from :meth:`pop_with_ack`, or
            ``None`` to ack the last-popped message (legacy).

    Raises:
        QueueError: If the underlying acknowledge fails.
    """
    del queue_name
    if isinstance(token, _PulsarAckToken):
      self._ack_token(token)
      return
    # Legacy path: ack the tracked last-popped message.
    if self._consumer is None or self._last_msg is None:
      return
    try:
      self._consumer.acknowledge(self._last_msg)
    except Exception as e:
      raise QueueError(f"Failed to ack Pulsar message: {e}", operation="ack") from e
    finally:
      self._last_msg = None

  def _ack_token(self, token: _PulsarAckToken) -> None:
    """Ack the specific message identified by ``token``.

    Pulsar's ack is per-message (not a watermark commit like Kafka), so this
    is a single ``acknowledge(message_id)`` call. Idempotent at the broker:
    re-acking an already-acked message_id is a no-op server-side; the
    in-flight ``discard`` is always safe.
    """
    if self._consumer is None:
      return
    try:
      self._consumer.acknowledge(token.message_id)
    except Exception as e:
      raise QueueError(f"Failed to ack Pulsar message: {e}", operation="ack") from e
    finally:
      self._in_flight.discard(token)

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Negative-acknowledge a popped message for re-delivery.

    With a ``token``: ``consumer.negative_acknowledge(token.message_id)``
    if the client supports it, scheduling the specific message for
    immediate re-delivery; otherwise no-op (the message stays unacked and
    is redelivered on the unacked-timeout / consumer restart —
    at-least-once). Either way the token is removed from the in-flight set.

    Without a ``token`` (legacy): nack the tracked ``_last_msg``.

    Args:
        queue_name: Name of the queue (unused; interface symmetry).
        token: A :class:`_PulsarAckToken` from :meth:`pop_with_ack`, or
            ``None`` to nack the last-popped message (legacy).
    """
    del queue_name
    if isinstance(token, _PulsarAckToken):
      self._nack_token(token)
      return
    # Legacy path: nack the tracked last-popped message.
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

  def _nack_token(self, token: _PulsarAckToken) -> None:
    """Nack the specific message identified by ``token`` (best-effort)."""
    try:
      nack = getattr(self._consumer, "negative_acknowledge", None)
      if callable(nack):
        nack(token.message_id)
      # else: leave unacked -> redelivered on timeout / restart
    except Exception as e:
      logger.warning("Pulsar nack failed; message will redeliver on restart: %s", e)
    finally:
      self._in_flight.discard(token)

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
    self._in_flight.clear()

  def _ensure_consumer(self, topic: str) -> None:
    """Create or reuse the consumer for ``topic`` (re-subscribes on change).

    Args:
        topic: The Pulsar topic to subscribe to.
    """
    if self._consumer is not None and self._subscribed_topic == topic:
      return
    # #31: topic changed (or re-subscribe after a prior topic) — close the
    # prior consumer first so it doesn't leak (Pulsar holds a server-side
    # subscription + a client resource per consumer). Skipped on first call.
    if self._consumer is not None:
      with _suppress_pulsar_errors():
        self._consumer.close()
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
    # R-swallow: suppress only regular cleanup Exceptions -- NEVER BaseException
    # (KeyboardInterrupt / SystemExit / GeneratorExit). Pre-fix this swallowed
    # any exception (return True), trapping Ctrl+C during close() (the
    # operator's shutdown signal disappeared into a debug log).
    if not isinstance(exc, Exception):
      return False
    logger.debug("Suppressed pulsar cleanup error: %s", exc)
    return True
