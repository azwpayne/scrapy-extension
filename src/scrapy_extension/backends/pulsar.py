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
from _thread import start_new_thread
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Condition, Event, Lock, Thread, current_thread
from time import monotonic
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

# One sync-SDK receive worker is admitted per topic. The worker never receives
# beyond this local bound, keeping background prefetch independent between topics.
_PULSAR_RECEIVE_BUFFER_SIZE = 100
_PULSAR_RECEIVE_TIMEOUT_MS = 1_000
_PULSAR_RECEIVE_SHUTDOWN_TIMEOUT = 1.0


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

    def _settle(self, terminal_state: str, operation: Callable[[], None]) -> bool:
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
        return f"_PulsarAckToken(topic={self.topic!r}, message_id={self.message_id!r})"


@dataclass(frozen=True)
class _BufferedPulsarRecord:
    """One broker delivery retained with its exact settlement identity."""

    message: Any
    message_id: Any
    consumer: Any


@dataclass
class _PulsarCloseTask:
    """One daemon SDK close whose result is published only before its fence."""

    handle: Any
    outcome_lock: Lock = field(default_factory=Lock)
    accepting_outcome: bool = True
    completed: bool = False
    error: BaseException | None = None
    worker: Thread | None = None

    def run(self) -> None:
        """Close the handle and publish the outcome only while still admitted."""
        error: BaseException | None = None
        try:
            self.handle.close()
        except BaseException as close_error:
            error = close_error
        with self.outcome_lock:
            if self.accepting_outcome:
                self.error = error
                self.completed = True

    def fence_and_collect(self) -> tuple[bool, BaseException | None]:
        """Stop late result publication and return the admitted outcome."""
        with self.outcome_lock:
            self.accepting_outcome = False
            return self.completed, self.error


@dataclass
class _PulsarConsumerRetirement:
    """Consumer close that fences replacement subscription publication."""

    topic: str
    client: Any
    generation: int
    consumer: Any = None
    started: Event = field(default_factory=Event)
    completed: Event = field(default_factory=Event)
    worker: Thread | None = None
    control_error: BaseException | None = None


@dataclass
class _PulsarReceivePump:
    """Bounded local delivery buffer owned by one topic/client generation."""

    topic: str
    client: Any
    snapshot: Any
    generation: int
    capacity: int
    consumer: Any = None
    condition: Condition = field(default_factory=Condition)
    records: deque[_BufferedPulsarRecord] = field(default_factory=deque)
    accepting: bool = True
    failed: bool = False
    control_error: BaseException | None = None
    receive_started: Event = field(default_factory=Event)
    buffered: Event = field(default_factory=Event)
    stopped: Event = field(default_factory=Event)
    worker: Thread | None = None
    retirement: _PulsarConsumerRetirement | None = None

    def stop_admission(self) -> None:
        """Fence this pump and wake local waiters before its consumer closes."""
        with self.condition:
            self.accepting = False
            self.condition.notify_all()

    def discard_buffered(self) -> None:
        """Release local references; broker deliveries intentionally remain unacked."""
        with self.condition:
            self.records.clear()
            self.condition.notify_all()


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
            f"Unknown Pulsar consumer_type: {value!r}. Valid: {', '.join(mapping)}.",
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
            f"Unknown Pulsar initial_position: {value!r}. Valid: {', '.join(mapping)}.",
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
        self._receive_pump_creation_lock = Lock()
        self._receive_pumps: dict[str, _PulsarReceivePump] = {}
        # A failed consumer may still hold an Exclusive/Failover subscription while
        # its bounded daemon close remains blocked. Topic tombstones survive
        # disconnect/reconnect and prevent a replacement subscribe until close exits.
        self._consumer_retirements: dict[str, _PulsarConsumerRetirement] = {}
        # Worker names must never expose broker topic names. This monotonic opaque
        # identifier is allocated under the lifecycle lock and paired with the
        # client generation in each receive worker's diagnostic name.
        self._receive_pump_counter = 0
        # Kept as an instance attribute so live tests can exercise backpressure
        # with a deliberately small bound without changing production behavior.
        self._receive_buffer_size = _PULSAR_RECEIVE_BUFFER_SIZE
        # A broken SDK close must not make process shutdown wait forever. Tests
        # may reduce this bound when exercising a deliberately uninterruptible
        # receive double.
        self._receive_shutdown_timeout = _PULSAR_RECEIVE_SHUTDOWN_TIMEOUT
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
        allow_remote_plaintext = self.config.allow_remote_plaintext

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
            allow_remote_plaintext,
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
        pumps: list[_PulsarReceivePump] = []
        abort_retirements: list[_PulsarConsumerRetirement] = []
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
                    pumps = list(self._receive_pumps.values())
                    for pump in pumps:
                        pump.stop_admission()
                        if pump.consumer is not None:
                            consumers.pop(id(pump.consumer), None)
                            retirement = pump.retirement
                            if retirement is None:
                                retirement = self._start_consumer_retirement_locked(
                                    pump, pump.consumer
                                )
                            abort_retirements.append(retirement)
                        elif not pump.stopped.is_set():
                            retirement = pump.retirement
                            if retirement is None:
                                retirement = self._new_consumer_retirement_locked(pump)
                            abort_retirements.append(retirement)
                    self._receive_pumps.clear()
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
                        self._in_flight_overflow_warned = False
                    # Invalidate in-flight producer/consumer creations that observed
                    # this client before the post-publication failure was noticed.
                    self._lifecycle_generation += 1
                    handles = [*consumers.values(), *producers.values(), client]

        teardown_deadline = monotonic() + max(0.0, self._receive_shutdown_timeout)
        close_errors, close_timeout_count = self._run_bounded_close_tasks(
            *handles, deadline=teardown_deadline
        )
        # A driver close must never replace the connection failure currently being
        # handled. The normal failure path logs after this helper returns.
        cleanup_failure_count = len(close_errors) + close_timeout_count
        for retirement in abort_retirements:
            if not retirement.completed.wait(max(0.0, teardown_deadline - monotonic())):
                cleanup_failure_count += 1
                self._log_close_shutdown_timeout()
        for pump in pumps:
            worker = pump.worker
            if worker is not None and worker is not current_thread():
                try:
                    worker.join(max(0.0, teardown_deadline - monotonic()))
                    if worker.is_alive():
                        self._log_receive_shutdown_timeout()
                except BaseException:
                    cleanup_failure_count += 1
            pump.discard_buffered()
        return cleanup_failure_count

    def disconnect(self) -> None:
        """Fence receive pumps, interrupt them, and release all SDK handles."""
        with self._lifecycle_lock:
            consumers = {
                id(consumer): consumer for consumer in self._consumers.values()
            }
            if self._consumer is not None:
                # Include directly injected historical single-consumer state while
                # avoiding a duplicate close for the normal cached path.
                consumers.setdefault(id(self._consumer), self._consumer)
            producers = {
                id(producer): producer for producer in self._producers.values()
            }
            pumps = list(self._receive_pumps.values())
            disconnect_retirements: list[_PulsarConsumerRetirement] = []
            # Admission is stopped at the lifecycle linearization point. A receive
            # that completes after this fence cannot publish into its old buffer.
            # Every possibly live topic consumer receives a retirement tombstone
            # before reconnect can publish a replacement subscription. This includes
            # workers still blocked in subscribe(), whose stale candidate is attached
            # to the tombstone when bootstrap eventually returns.
            for pump in pumps:
                pump.stop_admission()
                if pump.consumer is not None:
                    consumers.pop(id(pump.consumer), None)
                    retirement = pump.retirement
                    if retirement is None:
                        retirement = self._start_consumer_retirement_locked(
                            pump, pump.consumer
                        )
                    disconnect_retirements.append(retirement)
                elif not pump.stopped.is_set():
                    retirement = pump.retirement
                    if retirement is None:
                        retirement = self._new_consumer_retirement_locked(pump)
                    disconnect_retirements.append(retirement)
            client = self._client
            self._lifecycle_generation += 1
            self._receive_pumps.clear()
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
                self._in_flight_overflow_warned = False
        handles = [*consumers.values(), *producers.values()]
        if client is not None:
            handles.append(client)
        # Detached handle closes, topic retirements, and receive-pump joins spend
        # one teardown budget. Multiple stuck topics cannot multiply the configured
        # shutdown timeout.
        teardown_deadline = monotonic() + max(0.0, self._receive_shutdown_timeout)
        close_error: BaseException | None = None
        try:
            # Consumers are first so close() interrupts any blocked sync receive.
            self._close_detached_handles(*handles, deadline=teardown_deadline)
        except BaseException as error:
            close_error = error

        for retirement in disconnect_retirements:
            if not retirement.completed.wait(max(0.0, teardown_deadline - monotonic())):
                self._log_close_shutdown_timeout()

        join_error: BaseException | None = None
        for pump in pumps:
            worker = pump.worker
            if worker is not None and worker is not current_thread():
                try:
                    worker.join(max(0.0, teardown_deadline - monotonic()))
                    if worker.is_alive():
                        self._log_receive_shutdown_timeout()
                except BaseException as error:
                    if join_error is None:
                        join_error = error
            # These records were received but never returned. Dropping only the
            # local references (without ACK/NACK) leaves them for broker redelivery.
            pump.discard_buffered()
        retirement_error = next(
            (
                retirement.control_error
                for retirement in disconnect_retirements
                if retirement.control_error is not None
            ),
            None,
        )
        terminal_error = close_error or retirement_error or join_error
        if terminal_error is not None:
            # Raise only after every pump buffer and detached handle has completed
            # its bookkeeping. The exception and its graph are caller/SDK-owned:
            # teardown may preserve and re-raise the exact control object, but must
            # not rewrite its traceback, cause, context, or suppression state.
            raise terminal_error

    @staticmethod
    def _log_receive_shutdown_timeout() -> None:
        """Emit a static diagnostic when an SDK receive worker ignores close()."""
        try:
            logger.warning(
                "Pulsar receive worker did not stop within the shutdown timeout."
            )
        except BaseException:
            pass

    @staticmethod
    def _log_close_shutdown_timeout() -> None:
        """Emit a static diagnostic when an SDK handle close does not finish."""
        try:
            logger.warning(
                "Pulsar SDK handle close did not finish within the shutdown timeout."
            )
        except BaseException:
            pass

    def _run_bounded_close_tasks(
        self, *handles: Any, deadline: float | None = None
    ) -> tuple[list[BaseException], int]:
        """Close detached SDK handles concurrently within one finite join budget.

        Every close runs on a daemon so a broken sync SDK handle cannot pin process
        shutdown. All tasks are started before any join, ensuring one stuck consumer
        cannot prevent producer/client cleanup. Once the shared deadline expires,
        each task's outcome slot is fenced before returning; a late close can neither
        publish an exception nor alter the caller's selected teardown result.
        """
        if not handles:
            return [], 0

        tasks = [_PulsarCloseTask(handle) for handle in handles]
        management_errors: list[BaseException] = []
        started: list[_PulsarCloseTask] = []
        for task in tasks:
            worker = Thread(
                target=task.run,
                name="pulsar-sdk-close-cleanup",
                daemon=True,
            )
            task.worker = worker
            try:
                worker.start()
            except BaseException as error:
                # Fence immediately: this handle has no worker that could publish.
                task.fence_and_collect()
                management_errors.append(error)
            else:
                started.append(task)

        if deadline is None:
            deadline = monotonic() + max(0.0, self._receive_shutdown_timeout)
        for task in started:
            close_worker = task.worker
            if close_worker is None:
                continue
            try:
                close_worker.join(max(0.0, deadline - monotonic()))
            except BaseException as error:
                management_errors.append(error)

        errors: list[BaseException] = []
        timeout_count = 0
        for task in started:
            completed, outcome_error = task.fence_and_collect()
            if not completed:
                timeout_count += 1
            elif outcome_error is not None:
                errors.append(outcome_error)
        errors.extend(management_errors)
        if timeout_count:
            self._log_close_shutdown_timeout()
        return errors, timeout_count

    def _close_detached_handles(
        self, *handles: Any, deadline: float | None = None
    ) -> None:
        """Close every detached handle, retaining the first control exception."""
        errors, _timeout_count = self._run_bounded_close_tasks(
            *handles, deadline=deadline
        )
        primary_error: BaseException | None = None
        for error in errors:
            if isinstance(error, Exception):
                _log_suppressed_cleanup_error()
            elif primary_error is None:
                primary_error = error
        if primary_error is not None:
            raise primary_error

    def _close_aborted_handle(self, handle: Any) -> None:
        """Best-effort close for a private candidate that never became live.

        This helper runs while a primary exception is already propagating.  Unlike
        normal teardown, including a control-flow exception from a driver close
        must not replace that primary error.
        """
        self._run_bounded_close_tasks(handle)

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
                    if (
                        self._client is client
                        and self._lifecycle_generation == generation
                    ):
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
            self._close_detached_handles(producer)
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

        def publish_legacy(record: _BufferedPulsarRecord) -> _BufferedPulsarRecord:
            # Called while the lifecycle fence still proves this pump belongs to
            # the live client generation. Disconnect therefore linearizes either
            # before extraction (no delivery) or after this state is published.
            self._last_msg = record.message
            self._last_delivery = (record.consumer, record.message)
            return record

        record = self._receive(queue_name, timeout, publish_legacy)
        if record is None:
            return None
        return _message_bytes(record.message)

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

        def publish_token(
            record: _BufferedPulsarRecord,
        ) -> tuple[_BufferedPulsarRecord, _PulsarAckToken]:
            # Token creation and diagnostic publication share the lifecycle fence
            # with generation validation and deque extraction.
            token = _PulsarAckToken(
                message_id=record.message_id,
                topic=topic,
                consumer=record.consumer,
            )
            self._track_in_flight(token)
            return (record, token)

        delivery = self._receive(queue_name, timeout, publish_token)
        if delivery is None:
            return (None, None)
        record, token = delivery
        return (_message_bytes(record.message), token)

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

    def _receive(
        self,
        queue_name: str,
        timeout: float,
        publish: Callable[[_BufferedPulsarRecord], Any],
    ) -> Any | None:
        """Extract and publish one live-generation delivery within ``timeout``.

        The caller's deadline is captured before pump creation, so a positive
        timeout includes worker startup and ``client.subscribe``. A zero timeout
        starts the worker but never waits for either subscription or SDK receive.
        Record availability is checked only after acquiring the lifecycle fence;
        extraction, generation validation, and legacy/token publication are one
        atomic step with respect to disconnect.
        """
        deadline = monotonic() + timeout if timeout > 0 else None
        topic = self._topic_name(queue_name)
        pump = self._ensure_receive_pump(topic, deadline)
        if pump is None:
            return None

        while True:
            lifecycle_held = False
            condition_held = False
            terminal_error: BaseException | None = None
            retirement: _PulsarConsumerRetirement | None = None
            self._lifecycle_lock.acquire()
            lifecycle_held = True
            pump.condition.acquire()
            condition_held = True
            try:
                generation_is_live = (
                    pump.accepting
                    and self._client is pump.client
                    and self._lifecycle_generation == pump.generation
                    and self._receive_pumps.get(topic) is pump
                )
                if not generation_is_live:
                    return None
                # This check deliberately follows the lifecycle fence. A buffered
                # record from a detached generation must never be extracted first
                # and published after disconnect has cleared legacy/token state.
                if pump.records:
                    record = pump.records.popleft()
                    if (
                        pump.consumer is None
                        or self._consumers.get(topic) is not pump.consumer
                        or record.consumer is not pump.consumer
                    ):
                        return None
                    result = publish(record)
                    pump.condition.notify_all()
                    return result
                if pump.control_error is not None:
                    terminal_error = pump.control_error
                elif pump.failed:
                    terminal_error = QueueError(
                        "Pulsar receive pump failed.",
                        queue_name=topic,
                        operation="pop",
                    )

                if terminal_error is not None:
                    # Terminal worker state belongs to this exact live pump. Retire
                    # it under the same lifecycle/condition fence used for record
                    # extraction so a later poll can create a fresh consumer without
                    # an old worker or disconnect removing its replacement.
                    pump.accepting = False
                    pump.records.clear()
                    self._receive_pumps.pop(topic, None)
                    retired_consumer = pump.consumer
                    if (
                        retired_consumer is not None
                        and self._consumers.get(topic) is retired_consumer
                    ):
                        self._consumers.pop(topic, None)
                    if (
                        retired_consumer is not None
                        and self._consumer is retired_consumer
                    ):
                        self._consumer = None
                        self._subscribed_topic = None
                        for active_topic, active_consumer in self._consumers.items():
                            self._consumer = active_consumer
                            self._subscribed_topic = active_topic
                            break
                    if retired_consumer is not None:
                        retirement = pump.retirement
                        if retirement is None:
                            retirement = self._start_consumer_retirement_locked(
                                pump, retired_consumer
                            )
                    pump.condition.notify_all()
                elif deadline is None:
                    return None
                else:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        return None

                    # Retain the condition across lifecycle release and wait so worker
                    # publication cannot be missed. Lock acquisition everywhere else
                    # is lifecycle -> condition, while the wait holds no lifecycle.
                    self._lifecycle_lock.release()
                    lifecycle_held = False
                    pump.condition.wait(remaining)
            finally:
                if condition_held:
                    pump.condition.release()
                if lifecycle_held:
                    self._lifecycle_lock.release()

            if terminal_error is not None:
                if retirement is not None:
                    shutdown_budget = max(0.0, self._receive_shutdown_timeout)
                    caller_remaining = (
                        0.0 if deadline is None else max(0.0, deadline - monotonic())
                    )
                    close_wait = min(shutdown_budget, caller_remaining)
                    if close_wait > 0 and not retirement.completed.wait(close_wait):
                        if shutdown_budget <= caller_remaining:
                            self._log_close_shutdown_timeout()
                # Retirement cleanup must not replace either a static QueueError or a
                # preserved process-control exception crossing the worker boundary.
                raise terminal_error

    def _new_consumer_retirement_locked(
        self, pump: _PulsarReceivePump, consumer: Any = None
    ) -> _PulsarConsumerRetirement:
        """Publish one topic tombstone before an old consumer can be replaced."""
        retirement = _PulsarConsumerRetirement(
            topic=pump.topic,
            client=pump.client,
            generation=pump.generation,
            consumer=consumer,
        )
        pump.retirement = retirement
        self._consumer_retirements[pump.topic] = retirement
        return retirement

    def _start_consumer_retirement_locked(
        self, pump: _PulsarReceivePump, consumer: Any
    ) -> _PulsarConsumerRetirement:
        """Publish and start one consumer retirement under the lifecycle lock."""
        retirement = self._new_consumer_retirement_locked(pump, consumer)
        worker = Thread(
            target=self._run_consumer_retirement,
            args=(retirement,),
            name="pulsar-failed-consumer-retirement",
            daemon=True,
        )
        retirement.worker = worker
        self._consumer_retirements[pump.topic] = retirement
        try:
            worker.start()
        except BaseException:
            # ``Thread.start`` is not transactional: an injected or alternate
            # implementation may launch the target and then raise. Keep its
            # tombstone whenever startup is ambiguous. Only a definitely unstarted
            # worker receives a low-level daemon fallback, so close still begins
            # without replacing the pump's already-selected terminal exception.
            if self._thread_definitely_unstarted(worker):
                retirement.worker = None
                try:
                    start_new_thread(self._run_consumer_retirement, (retirement,))
                except BaseException:
                    # With no worker available, conservatively retain the fence. A
                    # replacement subscription is less safe than a stuck tombstone.
                    pass
        return retirement

    @staticmethod
    def _thread_definitely_unstarted(worker: Thread) -> bool:
        """Return true only when CPython's launch event proves no target ran."""
        try:
            started = getattr(worker, "_started", None)
            return started is not None and not started.is_set() and worker.ident is None
        except BaseException:
            return False

    def _finish_consumer_retirement(
        self, retirement: _PulsarConsumerRetirement
    ) -> None:
        """Release a topic fence only after its consumer can no longer be live."""
        retirement.completed.set()
        try:
            with self._lifecycle_lock:
                if self._consumer_retirements.get(retirement.topic) is retirement:
                    self._consumer_retirements.pop(retirement.topic, None)
        except BaseException:
            # The completed Event is authoritative. A later poll prunes a
            # tombstone even if an injected lifecycle-lock fault hit cleanup.
            pass

    def _run_consumer_retirement(self, retirement: _PulsarConsumerRetirement) -> None:
        """Close one consumer and release its replacement fence after close exits."""
        retirement.started.set()
        ordinary_failure = False
        try:
            retirement.consumer.close()
        except Exception:
            # Ordinary SDK failures stay behind a static diagnostic boundary.
            ordinary_failure = True
        except BaseException as error:
            # Disconnect preserves process control after all sibling teardown.
            # Failed-connect abort and terminal receive paths retain their existing
            # primary errors and intentionally do not consume this stored outcome.
            retirement.control_error = error
        finally:
            if ordinary_failure:
                _log_suppressed_cleanup_error()
            self._finish_consumer_retirement(retirement)

    def _close_stale_pump_candidate(
        self, pump: _PulsarReceivePump, candidate: Any
    ) -> None:
        """Close a late bootstrap result without dropping its reconnect fence."""
        with self._lifecycle_lock:
            retirement = pump.retirement
            if retirement is None:
                # Publication faults and other stale-candidate paths can arrive
                # without a disconnect-created placeholder. Fence the topic before
                # close starts so Exclusive/Failover cannot be replaced while the
                # candidate's actual close remains blocked.
                self._start_consumer_retirement_locked(pump, candidate)
                return
            retirement.consumer = candidate
            retirement.worker = current_thread()
        self._run_consumer_retirement(retirement)

    def _ensure_receive_pump(
        self, topic: str, deadline: float | None
    ) -> _PulsarReceivePump | None:
        """Create one pump, waiting only within budget for failed retirement."""
        while True:
            retirement: _PulsarConsumerRetirement | None = None
            with self._receive_pump_creation_lock:
                with self._lifecycle_lock:
                    retirement = self._consumer_retirements.get(topic)
                    if retirement is not None and retirement.completed.is_set():
                        self._consumer_retirements.pop(topic, None)
                        retirement = None
                    pump = self._receive_pumps.get(topic)
                    if pump is not None and pump.retirement is retirement:
                        # A publication failure can start candidate retirement before
                        # its live pump transfers the terminal error to a caller.
                        return pump
                    if retirement is None:
                        if pump is not None:
                            return pump
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
                            # Compatibility for direct private-client injection.
                            snapshot = self._capture_connection_snapshot()
                        pump = _PulsarReceivePump(
                            topic=topic,
                            client=client,
                            snapshot=snapshot,
                            generation=generation,
                            capacity=self._receive_buffer_size,
                        )
                        self._receive_pump_counter += 1
                        pump_identifier = self._receive_pump_counter
                        worker = Thread(
                            target=self._run_receive_pump,
                            args=(pump,),
                            name=(
                                f"scrapy-pulsar-receive-{generation}-{pump_identifier}"
                            ),
                            daemon=True,
                        )
                        pump.worker = worker
                        self._receive_pumps[topic] = pump
                        try:
                            worker.start()
                        except BaseException:
                            pump.stop_admission()
                            self._receive_pumps.pop(topic, None)
                            # ``Thread.start`` may launch the receive target and then
                            # raise. Such a worker can still return a live subscription
                            # candidate, so publish its topic fence before releasing the
                            # lifecycle lock. The worker attaches any late candidate and
                            # releases the fence only after close actually exits.
                            if not self._thread_definitely_unstarted(worker):
                                self._new_consumer_retirement_locked(pump)
                            raise
                        return pump

            # Never hold the creation or lifecycle lock while a failed SDK close
            # blocks. Zero-time polls remain immediate; positive polls spend only
            # their remaining caller budget waiting for retirement completion.
            if deadline is None:
                return None
            remaining = deadline - monotonic()
            if remaining <= 0 or not retirement.completed.wait(remaining):
                return None

    @staticmethod
    def _subscribe_pump_consumer(pump: _PulsarReceivePump) -> Any:
        """Perform a pump's potentially blocking subscription on its worker."""
        snapshot = pump.snapshot
        return pump.client.subscribe(
            pump.topic,
            snapshot.subscription_name,
            consumer_type=_consumer_type(snapshot.consumer_type),
            initial_position=_initial_position(snapshot.initial_position),
            negative_ack_redelivery_delay_ms=(
                snapshot.negative_ack_redelivery_delay_ms
            ),
        )

    def _run_receive_pump(self, pump: _PulsarReceivePump) -> None:
        """Bootstrap and receive off-reactor, publishing only while generation-live."""
        candidate: Any = None
        try:
            bootstrap_failed = False
            bootstrap_control_error: BaseException | None = None
            try:
                candidate = self._subscribe_pump_consumer(pump)
            except Exception:
                bootstrap_failed = True
            except BaseException as error:
                bootstrap_control_error = error

            if candidate is None:
                with self._lifecycle_lock:
                    generation_is_live = (
                        pump.accepting
                        and self._client is pump.client
                        and self._lifecycle_generation == pump.generation
                        and self._receive_pumps.get(pump.topic) is pump
                    )
                    if generation_is_live:
                        with pump.condition:
                            if bootstrap_control_error is not None:
                                pump.control_error = bootstrap_control_error
                            elif bootstrap_failed:
                                pump.failed = True
                            pump.condition.notify_all()
                return

            publication_error: BaseException | None = None
            try:
                with self._lifecycle_lock:
                    generation_is_live = (
                        pump.accepting
                        and self._client is pump.client
                        and self._lifecycle_generation == pump.generation
                        and self._receive_pumps.get(pump.topic) is pump
                    )
                    if generation_is_live:
                        pump.consumer = candidate
                        self._consumers[pump.topic] = candidate
                        self._consumer = candidate
                        self._subscribed_topic = pump.topic
                        candidate = None
                        with pump.condition:
                            pump.condition.notify_all()
            except BaseException as error:
                # Publication faults are transferred through the pump just like
                # receive faults; no exception may escape as an unhandled daemon
                # thread failure. The private candidate remains worker-owned.
                publication_error = error
            if publication_error is not None:
                if candidate is not None:
                    self._close_stale_pump_candidate(pump, candidate)
                    candidate = None
                with pump.condition:
                    if pump.accepting:
                        pump.control_error = publication_error
                        pump.condition.notify_all()
                return
            if candidate is not None:
                self._close_stale_pump_candidate(pump, candidate)
                candidate = None
                return

            while True:
                with pump.condition:
                    while pump.accepting and len(pump.records) >= pump.capacity:
                        pump.condition.wait()
                    if not pump.accepting:
                        return
                pump.receive_started.set()
                timed_out = False
                failed = False
                control_error: BaseException | None = None
                message: Any = None
                message_id: Any = None
                try:
                    message = pump.consumer.receive(
                        timeout_millis=_PULSAR_RECEIVE_TIMEOUT_MS
                    )
                    message_id = message.message_id()
                except pulsar.Timeout:
                    timed_out = True
                except Exception:
                    failed = True
                except BaseException as error:
                    # Process-control exceptions retain their historical identity,
                    # but cross the worker boundary through the waiting public poll.
                    control_error = error

                if timed_out:
                    # Avoid a hot loop with test doubles or SDKs that return timeout
                    # immediately. The condition also wakes teardown without delay.
                    with pump.condition:
                        if pump.accepting:
                            pump.condition.wait(0.01)
                    try:
                        logger.debug("Pulsar receive returned no message.")
                    except BaseException:
                        pass
                    continue

                with self._lifecycle_lock:
                    generation_is_live = (
                        pump.accepting
                        and self._client is pump.client
                        and self._lifecycle_generation == pump.generation
                        and self._receive_pumps.get(pump.topic) is pump
                        and self._consumers.get(pump.topic) is pump.consumer
                    )
                    with pump.condition:
                        if not generation_is_live or not pump.accepting:
                            return
                        if control_error is not None:
                            pump.control_error = control_error
                            pump.condition.notify_all()
                            return
                        if failed:
                            pump.failed = True
                            pump.condition.notify_all()
                            return
                        pump.records.append(
                            _BufferedPulsarRecord(
                                message=message,
                                message_id=message_id,
                                consumer=pump.consumer,
                            )
                        )
                        pump.buffered.set()
                        pump.condition.notify_all()
        finally:
            if candidate is not None:
                self._close_stale_pump_candidate(pump, candidate)
            retirement = pump.retirement
            if retirement is not None and retirement.consumer is None:
                # Bootstrap exited without producing a handle. No old subscription
                # can survive, so the disconnect-created tombstone can now retire.
                self._finish_consumer_retirement(retirement)
            pump.stopped.set()

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
            raise QueueError(
                f"Failed to ack Pulsar message: {e}", operation="ack"
            ) from e
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
        raise QueueError(msg, queue_name=queue_name, operation="clear_queue")

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
                    if (
                        self._client is client
                        and self._lifecycle_generation == generation
                    ):
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
            self._close_detached_handles(consumer)
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
    """Suppress regular cleanup errors while preserving control exceptions."""

    def __init__(self) -> None:
        self.did_suppress = False

    def __enter__(self) -> _suppress_pulsar_errors:
        self.did_suppress = False
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        if exc_type is None:
            return False
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
