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
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any, cast

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
  import pulsar
except ImportError as e:
  if not _is_missing_optional_dependency(e, "pulsar"):
    raise
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
)
from scrapy_extension.exceptions import (
  BackendConnectionError,
  ConfigurationError,
  QueueError,
)
from scrapy_extension.exceptions._redaction import (
  backend_connection_error_boundary,
  configuration_error_boundary,
  not_implemented_error_boundary,
  queue_operation_error_boundary,
)
from scrapy_extension.settings import PulsarMode, PulsarSettings
from scrapy_extension.settings.pulsar import validate_pulsar_connection

logger = logging.getLogger(__name__)

_PULSAR_CONFIGURATION_SETTING_NAMES: frozenset[str] = frozenset(
  PulsarSettings.model_fields
)
_PULSAR_SAFE_CONFIGURATION_MESSAGES: frozenset[str] = frozenset(
  {"Unsupported Pulsar mode."}
)
_PULSAR_SAFE_CONNECTION_MESSAGES: frozenset[str] = frozenset(
  {"Failed to connect to Pulsar."}
)
_PULSAR_SAFE_QUEUE_MESSAGES: frozenset[str] = frozenset(
  {"clear_queue is not supported without the Pulsar admin API"}
)
_PULSAR_QUEUE_LEN_UNSUPPORTED_MESSAGE = (
  "Pulsar queue depth requires the admin API, which is not configured"
)

# R14-E: cap on the diagnostic in-flight ack-token set. Each unacked pop
# adds one entry; without a cap a long-running process with slow acks (or a
# bug that never acks) grows the set unbounded. We warn-once on overflow and
# STOP adding — the set is diagnostic (Pulsar acks each message independently
# via ``consumer.acknowledge(msg_id)``, so ack correctness lives in the
# broker, not in this set). The POP itself is never dropped. 10k is generous
# for normal CONCURRENT_REQUESTS backpressure and tight enough to flag a leak.
_MAX_IN_FLIGHT = 10_000


def _validate_queue_name_argument(
  _backend: object,
  queue_name: str,
  *_args: Any,
  **_kwargs: Any,
) -> None:
  """Validate a public queue argument before its terminal error boundary."""
  _validate_key_name(queue_name, "queue_name")


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
      topic: The topic the message was consumed from. Used to route ack/nack
          back to the consumer that delivered the message.
      consumer: The consumer that delivered the message. Runtime-generated
          tokens use its identity to reject stale tokens after reconnect.
  """

  __slots__ = (
    "_settlement_lock",
    "_settlement_state",
    "consumer",
    "message_id",
    "topic",
  )

  def __init__(self, message_id: Any, topic: str, consumer: Any = None) -> None:
    """Initialize the token.

    Args:
        message_id: The pulsar ``MessageId`` for the popped message.
        topic: The topic the message was consumed from.
        consumer: The consumer that delivered the message. ``None`` keeps
            compatibility with tokens constructed by older callers/tests.
    """
    self.message_id = message_id
    self.topic = topic
    self.consumer = consumer
    self._settlement_lock = Lock()
    self._settlement_state = "pending"

  def _settle(
    self, terminal_state: str, operation: Callable[[], None]
  ) -> bool:
    """Run one terminal broker action, restoring retryability on failure.

    The token lock covers the broker call. A competing ack or nack therefore
    observes either the restored ``pending`` state after an exception or the
    published terminal state after success; it can never race a still-uncertain
    settlement.

    Args:
        terminal_state: State to publish after ``operation`` succeeds.
        operation: Broker action to execute while this token is claimed.

    Returns:
        True when this call completed the broker action; False when another
        successful action had already made the token terminal.
    """
    with self._settlement_lock:
      if self._settlement_state != "pending":
        return False
      self._settlement_state = "settling"
      completed = False
      try:
        operation()
        completed = True
      finally:
        self._settlement_state = terminal_state if completed else "pending"
      return True

  def __eq__(self, other: object) -> bool:
    if not isinstance(other, _PulsarAckToken):
      return NotImplemented
    return (
      self.message_id is other.message_id
      and self.topic == other.topic
      and self.consumer is other.consumer
    )

  def __hash__(self) -> int:
    # Pulsar ``MessageId`` hashability varies by client version (the C++
    # binding is not consistently hashable across releases). The in-flight
    # set is DIAGNOSTIC ONLY (leak detection / monitoring — Pulsar acks
    # each message independently, unlike Kafka's watermark commit), so
    # identity-based hashing on the message_id object is sufficient and
    # robust across all client versions. Equality mirrors this (identity
    # on message_id) so the token that came out of the set is the one
    # ``discard`` removes.
    return hash((id(self.message_id), self.topic, id(self.consumer)))

  def __repr__(self) -> str:
    return (
      f"_PulsarAckToken(topic={self.topic!r}, "
      f"message_id={self.message_id!r})"
    )


@dataclass(frozen=True)
class _PulsarConnectionSnapshot:
  """One validated, repr-safe set of values used by a client generation."""

  mode: PulsarMode
  service_url: str
  subscription_name: str
  consumer_type: str
  initial_position: str
  negative_ack_redelivery_delay_ms: int
  auth_token: str | None
  tls_trust_certs_file: str | None
  allow_insecure_connection: bool
  tls_validate_hostname: bool


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
    # The public setting follows Pulsar's documented subscription spelling,
    # while every supported Python binding exposes the enum as ``KeyShared``.
    "Key_Shared": getattr(pulsar.ConsumerType, "KeyShared", None),
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

  Does NOT implement SetBackend or StorageBackend. ``queue_len`` raises
  ``NotImplementedError`` because Pulsar backlog stats require the admin REST
  API, which is out of scope here.
  ``clear_queue`` raises ``QueueError`` because broker-side purge requires the
  admin API; local handle cleanup must not masquerade as durable deletion.

  Ack capability: ``requires_ack=True``, ``supports_concurrent_ack=True``.
  Pulsar's Shared subscription is natively per-message —
  ``consumer.acknowledge(msg_id)`` and ``consumer.negative_acknowledge(msg_id)``
  target one specific message identified by its ``MessageId``. Pops via
  :meth:`pop_with_ack` carry a :class:`_PulsarAckToken` (wrapping
  ``msg.message_id()``) tracked in the in-flight set; :meth:`ack` /
  :meth:`nack` use the token to ack the *specific* message — correct under
  ``CONCURRENT_REQUESTS > 1`` (N pops before any ack no longer overwrite a
  single slot). Each token permits one successful ack or nack; client failures
  leave it retryable, and competing terminal actions are serialized. The
  in-flight set is diagnostic (leak detection / monitoring) since Pulsar acks
  each message independently. The legacy ``pop()`` / ``ack(token=None)`` path
  separately tracks ``_last_msg`` for backward compatibility.

  Attributes:
      config: PulsarSettings instance.
      _client: The pulsar.Client instance (None until connected).
      _producers: Per-topic cached producers.
      _consumers: Per-topic cached consumers.
      _consumer: The most recently used consumer (legacy compatibility view).
      _subscribed_topic: Topic for the most recently used consumer.
      _last_msg: The last-popped message (legacy ``ack(token=None)`` path).
      _last_delivery: Consumer/message pair for the legacy ack/nack path.
      _in_flight: Diagnostic set of popped-but-unacked ack tokens.
  """

  _push_is_durable = True
  requires_ack = True
  supports_concurrent_ack = True

  def __init__(self, config: PulsarSettings) -> None:
    """Initialize the Pulsar backend.

    Args:
        config: Configuration for the Pulsar connection.
    """
    self.config = config
    self._client: Any = None
    self._connection_snapshot: _PulsarConnectionSnapshot | None = None
    self._producers: dict[str, Any] = {}
    self._consumers: dict[str, Any] = {}
    self._lifecycle_lock = Lock()
    self._lifecycle_generation = 0
    self._producer_creation_lock = Lock()
    self._consumer_creation_lock = Lock()
    # Compatibility view for callers/tests that inspect the historical
    # single-consumer state. Message-token routing uses ``_consumers``.
    self._consumer: Any = None
    self._subscribed_topic: str | None = None
    # Legacy single-slot for the ``ack(token=None)`` fallback path. Kept so
    # external callers that pop() then ack() without a token still work.
    self._last_msg: Any = None
    self._last_delivery: tuple[Any, Any] | None = None
    # In-flight ack tokens for correctness under CONCURRENT_REQUESTS>1.
    # DIAGNOSTIC ONLY: Pulsar acks each message independently (unlike Kafka's
    # watermark commit), so the set is for leak detection / monitoring —
    # mirrors RabbitMQ's ``_in_flight_tags``.
    self._in_flight: set[_PulsarAckToken] = set()
    self._in_flight_lock = Lock()
    # R14-E: one-shot guard for the in-flight-set-overflow warning.
    self._in_flight_overflow_warned: bool = False

  @configuration_error_boundary(
    "Pulsar configuration is invalid.",
    _PULSAR_CONFIGURATION_SETTING_NAMES,
    preserve_static_message=True,
    safe_messages=_PULSAR_SAFE_CONFIGURATION_MESSAGES,
  )
  def _capture_connection_snapshot(self) -> _PulsarConnectionSnapshot:
    """Capture and revalidate every value used by one client generation."""
    mode = self.config.mode
    service_url = self.config.service_url
    subscription_name = self.config.subscription_name
    consumer_type = self.config.consumer_type
    initial_position = self.config.initial_position
    negative_ack_redelivery_delay_ms = self.config.negative_ack_redelivery_delay_ms
    auth_token = self.config.auth_token
    tls_trust_certs_file = self.config.tls_trust_certs_file
    allow_insecure_connection = self.config.allow_insecure_connection
    tls_validate_hostname = self.config.tls_validate_hostname

    if mode not in (PulsarMode.STANDALONE, PulsarMode.CLUSTER):
      raise ConfigurationError(
        "Unsupported Pulsar mode.",
        setting_name="mode",
      )
    (
      normalized_url,
      token_text,
      trust_file,
      allow_insecure,
      validate_hostname,
    ) = validate_pulsar_connection(
      service_url,
      auth_token,
      tls_trust_certs_file,
      allow_insecure_connection,
      tls_validate_hostname,
    )
    if not isinstance(subscription_name, str) or not subscription_name.strip():
      raise ConfigurationError(
        "Pulsar subscription_name must be a non-empty string.",
        setting_name="subscription_name",
      )
    if consumer_type not in ("Shared", "Failover", "Exclusive", "Key_Shared"):
      raise ConfigurationError(
        "Pulsar consumer_type is invalid.", setting_name="consumer_type"
      )
    if initial_position not in ("Earliest", "Latest"):
      raise ConfigurationError(
        "Pulsar initial_position is invalid.", setting_name="initial_position"
      )
    if (
      isinstance(negative_ack_redelivery_delay_ms, bool)
      or not isinstance(negative_ack_redelivery_delay_ms, int)
      or negative_ack_redelivery_delay_ms < 0
    ):
      raise ConfigurationError(
        "Pulsar negative_ack_redelivery_delay_ms must be an integer >= 0.",
        setting_name="negative_ack_redelivery_delay_ms",
      )

    return _PulsarConnectionSnapshot(
      mode=mode,
      service_url=normalized_url,
      subscription_name=subscription_name,
      consumer_type=consumer_type,
      initial_position=initial_position,
      negative_ack_redelivery_delay_ms=negative_ack_redelivery_delay_ms,
      auth_token=(
        cast(str, _redact(token_text)) if token_text is not None else None
      ),
      tls_trust_certs_file=trust_file,
      allow_insecure_connection=allow_insecure,
      tls_validate_hostname=validate_hostname,
    )

  @backend_connection_error_boundary(
    "Failed to connect to Pulsar.",
    "pulsar",
    safe_messages=_PULSAR_SAFE_CONNECTION_MESSAGES,
  )
  @configuration_error_boundary(
    "Pulsar configuration is invalid.",
    _PULSAR_CONFIGURATION_SETTING_NAMES,
    preserve_static_message=True,
    safe_messages=_PULSAR_SAFE_CONFIGURATION_MESSAGES,
    pass_through_exception_types=(BackendConnectionError,),
  )
  def connect(self) -> None:
    """Connect to Pulsar by creating a client from ``service_url``.

    Raises:
        BackendConnectionError: If the client cannot be created.
        ConfigurationError: If the mode is unsupported.
    """
    with self._lifecycle_lock:
      if self._client is not None:
        return
    snapshot = self._capture_connection_snapshot()
    # R19-B: hoist BEFORE the try so the ``except BaseException`` arm below can
    # always reference ``client``. A Ctrl+C during kwargs-setup (notably the
    # ``pulsar.AuthenticationToken()`` call) reaches the arm before this
    # assignment otherwise, raising ``UnboundLocalError`` that masks the original
    # interrupt. Mirror rabbitmq ``_open_prepared_channel`` (hoist before try).
    client: Any = None
    # ``None`` means construction failed before publication.  Once set, this
    # lets failure cleanup distinguish our published generation from one that
    # a concurrent disconnect/reconnect has already retired and replaced.
    published_generation: int | None = None
    startup_error: BackendConnectionError | None = None
    failed_connect: tuple[Any, int | None] | None = None
    try:
      kwargs: dict[str, Any] = {}
      # Keep the package's public compatibility names, but translate them to
      # the exact pulsar-client 2.11-3.x constructor keywords. The old
      # unprefixed names were accepted by MagicMock tests yet rejected by the
      # real SDK, making every TLS connect fail before network I/O. Hostname
      # validation is explicit because the SDK itself defaults it to False.
      is_ssl = snapshot.service_url.startswith("pulsar+ssl://")
      if is_ssl:
        kwargs["tls_allow_insecure_connection"] = (
          snapshot.allow_insecure_connection
        )
        kwargs["tls_validate_hostname"] = snapshot.tls_validate_hostname
        if snapshot.tls_trust_certs_file:
          kwargs["tls_trust_certs_file_path"] = snapshot.tls_trust_certs_file
      if snapshot.auth_token is not None:
        kwargs["authentication"] = pulsar.AuthenticationToken(
          snapshot.auth_token
        )
      with self._lifecycle_lock:
        # ``connect`` is idempotent and linearizes with ``disconnect``.  Keep
        # client construction inside the lifecycle boundary so a concurrent
        # disconnect either runs before this connect or detaches the newly
        # published client afterwards; it can never miss an in-progress
        # client that is published just after teardown takes its snapshot.
        if self._client is not None:
          return
        client = pulsar.Client(snapshot.service_url, **kwargs)
        self._lifecycle_generation += 1
        published_generation = self._lifecycle_generation
        self._client = client
        self._connection_snapshot = snapshot
      # Publication above is the linearization point for this generation.
      # This message is only telemetry: a custom log handler must not turn a
      # completed connection into a failed one, nor abort its live client.
      try:
        logger.debug("Connected to Pulsar in %s mode", snapshot.mode.value)
      except BaseException:
        pass
    except ConfigurationError:
      raise
    except Exception:
      # Keep only the candidate state while this error suite is active. The
      # close and its diagnostic run below, after the raw driver error has
      # unwound, so cleanup hooks cannot recover it through ``sys.exc_info``.
      failed_connect = (client, published_generation)
      startup_error = BackendConnectionError(
        "Failed to connect to Pulsar.", backend_type="pulsar"
      )
    except BaseException:
      # R18-B/R106: a Ctrl+C/SystemExit after ``pulsar.Client(...)`` returns
      # (the C++ binding starts background IO/service threads in its
      # constructor) but before publication must not leak a private candidate.
      # Pure post-publication diagnostics are isolated above, so they never
      # enter this abort path. Cleanup never masks the original control signal.
      self._abort_failed_connect(client, published_generation)
      raise

    if failed_connect is not None:
      cleanup_failure_count = self._abort_failed_connect(*failed_connect)
      for _ in range(cleanup_failure_count):
        try:
          logger.debug("Failed to close Pulsar connect candidate")
        except BaseException:
          # Diagnostics cannot replace the static startup error below.
          pass

    if startup_error is not None:
      # Raise outside the driver exception handler so endpoint/credential text
      # cannot survive through ``__cause__`` or ``__context__``.
      raise startup_error

  def _abort_failed_connect(
    self, client: Any, published_generation: int | None
  ) -> int:
    """Detach and best-effort close only this failed connect generation.

    A normal connection failure commonly happens before publication.  The
    guarded published-generation branch remains for non-diagnostic failures
    in or around publication: detach only when both the client object and
    generation still match, because a concurrent disconnect/reconnect may
    already own and have replaced the old client. Pure post-publication
    telemetry is isolated by :meth:`connect` and must not reach this helper.
    Cleanup intentionally swallows *all* exceptions, including control-flow
    exceptions from driver ``close()``. The returned failure count lets the
    normal failure path preserve one static diagnostic per failed close after
    its original exception has unwound; callers preserving a primary
    control-flow exception ignore it.
    """
    if client is None:
      return 0

    handles: list[Any] = []
    if published_generation is None:
      # Construction completed but publication did not.  No public teardown
      # could have claimed this private candidate, so this connect owns it.
      handles.append(client)
    else:
      with self._lifecycle_lock:
        if (
          self._client is client
          and self._lifecycle_generation == published_generation
        ):
          consumers = {
            id(consumer): consumer for consumer in self._consumers.values()
          }
          if self._consumer is not None:
            consumers.setdefault(id(self._consumer), self._consumer)
          producers = {
            id(producer): producer for producer in self._producers.values()
          }
          self._consumers.clear()
          self._consumer = None
          self._subscribed_topic = None
          self._producers.clear()
          self._client = None
          self._connection_snapshot = None
          self._last_msg = None
          self._last_delivery = None
          with self._in_flight_lock:
            self._in_flight.clear()
          # Invalidate in-flight producer/consumer creations that observed
          # this client before the post-publication failure was noticed.
          self._lifecycle_generation += 1
          handles = [*consumers.values(), *producers.values(), client]

    cleanup_failure_count = 0
    for handle in handles:
      try:
        handle.close()
      except BaseException:
        # A driver close must never replace the connection failure currently
        # being handled. The normal failure path logs after this helper returns.
        cleanup_failure_count += 1
    return cleanup_failure_count

  def disconnect(self) -> None:
    """Close the Pulsar client and release producers/consumers."""
    with self._lifecycle_lock:
      consumers = {id(consumer): consumer for consumer in self._consumers.values()}
      if self._consumer is not None:
        # Include directly injected historical single-consumer state while
        # avoiding a duplicate close for the normal cached path.
        consumers.setdefault(id(self._consumer), self._consumer)
      producers = {id(producer): producer for producer in self._producers.values()}
      client = self._client
      self._lifecycle_generation += 1
      self._consumers.clear()
      self._consumer = None
      self._subscribed_topic = None
      self._producers.clear()
      self._client = None
      self._connection_snapshot = None
      self._last_msg = None
      self._last_delivery = None
      with self._in_flight_lock:
        self._in_flight.clear()
    handles = [*consumers.values(), *producers.values()]
    if client is not None:
      handles.append(client)
    self._close_detached_handles(*handles)

  @staticmethod
  def _close_detached_handles(*handles: Any) -> None:
    """Close every detached handle, retaining the first control exception."""
    primary_error: BaseException | None = None
    for handle in handles:
      cleanup = _suppress_pulsar_errors()
      try:
        with cleanup:
          handle.close()
      except BaseException as error:
        if primary_error is None:
          primary_error = error
      if cleanup.did_suppress:
        _log_suppressed_cleanup_error()
    if primary_error is not None:
      raise primary_error

  @staticmethod
  def _close_aborted_handle(handle: Any) -> None:
    """Best-effort close for a private candidate that never became live.

    This helper runs while a primary exception is already propagating.  Unlike
    normal teardown, including a control-flow exception from a driver close
    must not replace that primary error.
    """
    try:
      handle.close()
    except BaseException:
      pass

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
    with self._producer_creation_lock:
      with self._lifecycle_lock:
        producer = self._producers.get(topic)
        if producer is not None:
          return producer
        client = self._client
        generation = self._lifecycle_generation
      if client is None:
        raise QueueError(
          f"Cannot create Pulsar producer for {topic}: backend is disconnected",
          queue_name=topic,
          operation="push",
        )
      try:
        producer = client.create_producer(topic)
      except Exception as e:
        raise QueueError(
          f"Failed to create Pulsar producer for {topic}: {e}",
          queue_name=topic,
          operation="push",
        ) from e
      published = False
      try:
        with self._lifecycle_lock:
          if self._client is client and self._lifecycle_generation == generation:
            self._producers[topic] = producer
            # Mark publication before the context manager exits: an extension
            # lock may raise from ``__exit__`` after the live cache has taken
            # ownership.  That handle belongs to the active generation and
            # must not be closed by this candidate-abort path.
            published = True
            return producer
      except BaseException:
        # The SDK constructor can have allocated native resources before a
        # control-flow exception interrupts cache publication.  Once a handle
        # is published the generation owns it; otherwise this creator does.
        if not published:
          self._close_aborted_handle(producer)
        raise
      cleanup = _suppress_pulsar_errors()
      with cleanup:
        producer.close()
      if cleanup.did_suppress:
        _log_suppressed_cleanup_error()
      raise QueueError(
        f"Failed to create Pulsar producer for {topic}: connection changed",
        queue_name=topic,
        operation="push",
      )

  # QueueBackend implementation
  @queue_operation_error_boundary(
    "push",
    "Failed to push Pulsar message.",
    safe_messages=_PULSAR_SAFE_QUEUE_MESSAGES,
    validator=_validate_queue_name_argument,
  )
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

  @queue_operation_error_boundary(
    "pop",
    "Failed to pop Pulsar message.",
    safe_messages=_PULSAR_SAFE_QUEUE_MESSAGES,
    validator=_validate_queue_name_argument,
  )
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
    msg, consumer = self._receive(queue_name, timeout)
    if msg is None:
      return None
    self._last_msg = msg
    self._last_delivery = (consumer, msg)
    return _message_bytes(msg)

  @queue_operation_error_boundary(
    "pop",
    "Failed to pop Pulsar message.",
    safe_messages=_PULSAR_SAFE_QUEUE_MESSAGES,
    validator=_validate_queue_name_argument,
  )
  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop an item together with a :class:`_PulsarAckToken`.

    Records the popped message's ``message_id`` in the in-flight set so
    :meth:`ack` can ``acknowledge`` the *specific* message — correct under
    ``CONCURRENT_REQUESTS > 1`` (no single-slot overwrite, no message
    lost/skipped). Pulsar's Shared subscription is natively per-message, so
    the token carries exactly what ``consumer.acknowledge`` needs. This token
    path does not populate the legacy last-message settlement slot.

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
    msg, consumer = self._receive(queue_name, timeout)
    if msg is None:
      return (None, None)
    token = _PulsarAckToken(
      message_id=msg.message_id(),
      topic=topic,
      consumer=consumer,
    )
    self._track_in_flight(token)
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
    with self._in_flight_lock:
      if len(self._in_flight) < _MAX_IN_FLIGHT:
        self._in_flight.add(token)
        return
      if not self._in_flight_overflow_warned:
        self._in_flight_overflow_warned = True
        # The message and its settlement token already belong to the caller.
        # A broken logging handler is pure diagnostics and must not turn that
        # broker-confirmed delivery into a failed pop (or prevent its ack).
        try:
          logger.warning(
            "Pulsar in-flight ack-token set at cap (%d) — further unacked "
            "pops will not be tracked in the diagnostic set. This indicates "
            "slow acks or an ack leak; the broker still tracks message_ids "
            "so ack correctness is unaffected.",
            _MAX_IN_FLIGHT,
          )
        except BaseException:
          pass

  def _receive(self, queue_name: str, timeout: float) -> tuple[Any, Any]:
    """Receive one message and return it with the consumer that delivered it.

    Shared by :meth:`pop` and :meth:`pop_with_ack` so consumer
    subscription, topic validation, and error wrapping live in one place.
    Only the receive call maps a no-message result to None; subscribe
    errors propagate as :class:`QueueError`.

    Args:
        queue_name: Name of the queue (validated here).
        timeout: Seconds to wait (0 = a short non-blocking poll).

    Returns:
        ``(message, consumer)``. ``message`` is None if no message arrived
        in time; ``consumer`` is the topic-specific consumer used to poll.

    Raises:
        QueueError: If the receive fails at the Pulsar layer for a
            non-timeout reason (subscribe failure).
        ValueError: If queue_name contains invalid characters.
    """
    topic = self._topic_name(queue_name)
    # Subscribe errors must propagate (not be masked as "empty"); only the
    # receive call maps a no-message result to None.
    consumer = self._ensure_consumer(topic)
    timed_out = False
    try:
      # timeout=0 -> a short poll; Pulsar needs a positive timeout_millis.
      timeout_ms = int(timeout * 1000) if timeout > 0 else 100
      message = consumer.receive(timeout_millis=timeout_ms)
    except pulsar.Timeout:
      timed_out = True
    except Exception as e:
      # Broker disconnects, authorization failures, and invalid consumer state
      # are operational failures, not evidence that the queue is empty. A
      # false empty result can make Scrapy close an active crawl prematurely.
      raise QueueError(
        f"Failed to pop from Pulsar queue {queue_name}: {e}",
        queue_name=queue_name,
        operation="pop",
      ) from e

    if timed_out:
      # The timeout handler above has unwound, so a synchronous log handler
      # cannot recover the driver's exception through ``sys.exc_info``.
      try:
        logger.debug("Pulsar receive returned no message.")
      except BaseException:
        pass
      return (None, consumer)
    return (message, consumer)

  @queue_operation_error_boundary(
    "ack",
    "Failed to ack Pulsar message.",
    safe_messages=_PULSAR_SAFE_QUEUE_MESSAGES,
  )
  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Ack a popped message via ``consumer.acknowledge``.

    With a ``token`` (the scheduler path under ``CONCURRENT_REQUESTS > 1``):
    ``consumer.acknowledge(token.message_id)`` the specific message and
    remove the token from the in-flight set. Order-independent — ack the
    right message regardless of pop/ack interleaving. A successful ack is
    terminal across later ack/nack calls; a client exception raises
    :class:`QueueError` and leaves the token retryable.

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
    if token is not None:
      return
    # Legacy path: ack the tracked last-popped message.
    if self._last_msg is None:
      return
    if self._last_delivery is not None:
      consumer, message = self._last_delivery
    else:
      consumer, message = self._consumer, self._last_msg
    if consumer is None:
      return
    try:
      consumer.acknowledge(message)
    except Exception as e:
      raise QueueError(f"Failed to ack Pulsar message: {e}", operation="ack") from e
    else:
      self._last_msg = None
      self._last_delivery = None

  def _ack_token(self, token: _PulsarAckToken) -> None:
    """Ack the specific message identified by ``token``.

    Pulsar's ack is per-message (not a watermark commit like Kafka). The
    token's settlement lock guarantees that a successful ack is the only
    terminal broker action, while a client exception restores the token to
    its retryable pending state.
    """
    consumer = self._consumer_for_token(token)
    if consumer is None:
      token._settle("stale", lambda: None)
      self._discard_in_flight(token)
      return

    def acknowledge() -> None:
      try:
        consumer.acknowledge(token.message_id)
      except Exception as e:
        raise QueueError(
          f"Failed to ack Pulsar message: {e}", operation="ack"
        ) from e

    token._settle("acked", acknowledge)
    self._discard_in_flight(token)

  @queue_operation_error_boundary(
    "nack",
    "Failed to nack Pulsar message.",
    safe_messages=_PULSAR_SAFE_QUEUE_MESSAGES,
  )
  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Negative-acknowledge a popped message for re-delivery.

    With a ``token``: ``consumer.negative_acknowledge(token.message_id)``
    if the client supports it, scheduling the specific message for
    immediate re-delivery; otherwise no-op (the message stays unacked and
    is redelivered on the unacked-timeout / consumer restart —
    at-least-once). Success is terminal across later ack/nack calls and removes
    the token from the in-flight set; a client exception leaves it retryable.

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
    if token is not None:
      return
    # Legacy path: nack the tracked last-popped message.
    if self._last_msg is None:
      return
    if self._last_delivery is not None:
      consumer, message = self._last_delivery
    else:
      consumer, message = self._consumer, self._last_msg
    if consumer is None:
      return
    try:
      nack = getattr(consumer, "negative_acknowledge", None)
      if callable(nack):
        nack(message)
      # else: leave unacked -> redelivered on timeout / restart
    except Exception as e:
      raise QueueError(
        f"Failed to nack Pulsar message: {e}", operation="nack"
      ) from e
    else:
      self._last_msg = None
      self._last_delivery = None

  def _nack_token(self, token: _PulsarAckToken) -> None:
    """Nack one token exactly once, retaining it after client failure."""
    consumer = self._consumer_for_token(token)
    if consumer is None:
      token._settle("stale", lambda: None)
      self._discard_in_flight(token)
      return

    def negative_acknowledge() -> None:
      try:
        nack = getattr(consumer, "negative_acknowledge", None)
        if callable(nack):
          nack(token.message_id)
        # Older clients without the method leave the message unacked for
        # timeout/restart redelivery; accepting nack is still terminal locally.
      except Exception as e:
        raise QueueError(
          f"Failed to nack Pulsar message: {e}", operation="nack"
        ) from e

    token._settle("nacked", negative_acknowledge)
    self._discard_in_flight(token)

  def _discard_in_flight(self, token: _PulsarAckToken) -> None:
    """Remove a terminal token from the bounded diagnostic set."""
    with self._in_flight_lock:
      self._in_flight.discard(token)

  def _consumer_for_token(self, token: _PulsarAckToken) -> Any:
    """Return the active consumer that originally issued ``token``."""
    consumer = self._consumers.get(token.topic)
    if token.consumer is not None:
      return consumer if consumer is token.consumer else None
    if consumer is not None:
      return consumer
    if self._subscribed_topic in (None, token.topic):
      # Compatibility fallback for callers that inject the historical
      # single-consumer state directly.
      return self._consumer
    return None

  @not_implemented_error_boundary(
    _PULSAR_QUEUE_LEN_UNSUPPORTED_MESSAGE,
    validator=_validate_queue_name_argument,
  )
  def queue_len(self, queue_name: str) -> int:
    """Report that queue depth is unavailable without the Pulsar admin API.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
        NotImplementedError: Always; backlog depth requires the admin API.
    """
    raise NotImplementedError(_PULSAR_QUEUE_LEN_UNSUPPORTED_MESSAGE)

  @queue_operation_error_boundary(
    "clear_queue",
    "Failed to clear Pulsar queue.",
    safe_messages=_PULSAR_SAFE_QUEUE_MESSAGES,
    validator=_validate_queue_name_argument,
  )
  def clear_queue(self, queue_name: str) -> None:
    """Report that broker-side queue purge is unsupported.

    Dropping cached client handles does not clear a Pulsar subscription or
    its backlog. Returning success for that local-only cleanup would violate
    the QueueBackend contract and can make callers believe durable messages
    were deleted.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
        QueueError: Always; purging requires the Pulsar admin API, which this
            backend does not configure.
    """
    _validate_key_name(queue_name, "queue_name")
    msg = "clear_queue is not supported without the Pulsar admin API"
    raise QueueError(
      msg, queue_name=queue_name, operation="clear_queue"
    )

  def _ensure_consumer(self, topic: str) -> Any:
    """Create or reuse the cached consumer for ``topic``.

    Args:
        topic: The Pulsar topic to subscribe to.
    """
    with self._consumer_creation_lock:
      with self._lifecycle_lock:
        consumer = self._consumers.get(topic)
        if consumer is not None:
          self._consumer = consumer
          self._subscribed_topic = topic
          return consumer
        client = self._client
        generation = self._lifecycle_generation
        snapshot = self._connection_snapshot
      if client is None:
        raise QueueError(
          f"Cannot subscribe to Pulsar topic {topic}: backend is disconnected",
          queue_name=topic,
          operation="pop",
        )
      if snapshot is None:
        # Compatibility for tests/third-party instrumentation that injects a
        # private client directly; normal connected generations always publish
        # their validated snapshot atomically with the client.
        snapshot = self._capture_connection_snapshot()
      try:
        consumer = client.subscribe(
          topic,
          snapshot.subscription_name,
          consumer_type=_consumer_type(snapshot.consumer_type),
          initial_position=_initial_position(snapshot.initial_position),
          negative_ack_redelivery_delay_ms=(
            snapshot.negative_ack_redelivery_delay_ms
          ),
        )
      except Exception as e:
        raise QueueError(
          f"Failed to subscribe to Pulsar topic {topic}: {e}",
          queue_name=topic,
          operation="pop",
        ) from e
      published = False
      try:
        with self._lifecycle_lock:
          if self._client is client and self._lifecycle_generation == generation:
            self._consumers[topic] = consumer
            self._consumer = consumer
            self._subscribed_topic = topic
            # See ``_producer_for``: a context-manager failure after this
            # point must leave the active generation's handle untouched.
            published = True
            return consumer
      except BaseException:
        if not published:
          self._close_aborted_handle(consumer)
        raise
      cleanup = _suppress_pulsar_errors()
      with cleanup:
        consumer.close()
      if cleanup.did_suppress:
        _log_suppressed_cleanup_error()
      raise QueueError(
        f"Failed to subscribe to Pulsar topic {topic}: connection changed",
        queue_name=topic,
        operation="pop",
      )


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
  """Suppress regular cleanup errors and report that suppression to callers.

  Close() calls during disconnect/clear must not raise — a failing close
  during teardown would mask the original error and break cleanup of the
  remaining handles. ``__exit__`` deliberately performs no diagnostics: it
  runs with the close error active in ``sys.exc_info()``. Callers inspect
  :attr:`did_suppress` after the ``with`` statement, then log static context.
  """

  def __init__(self) -> None:
    self.did_suppress = False

  def __enter__(self) -> _suppress_pulsar_errors:
    self.did_suppress = False
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
    self.did_suppress = True
    return True


def _log_suppressed_cleanup_error() -> None:
  """Report a suppressed cleanup failure after its exception context unwinds."""
  try:
    logger.debug("Suppressed pulsar cleanup error")
  except BaseException:
    # Diagnostics must not stop later handle cleanup or replace terminal errors.
    pass
