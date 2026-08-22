"""Kafka backend implementation with multi-mode support.

This module provides a Kafka-based implementation of QueueBackend,
supporting multiple deployment modes:
- Standalone: Single Kafka broker
- Cluster: Multi-broker Kafka cluster
- Confluent: Confluent Cloud configuration

Note: Kafka does not support SetBackend or StorageBackend operations.
"""

from __future__ import annotations

import contextlib
import logging
import re
import threading
from collections import defaultdict
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, cast

from pydantic import ValidationError

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
    from kafka import (
        ConsumerRebalanceListener,
        KafkaConsumer,
        KafkaProducer,
        TopicPartition,
    )
    from kafka.admin import (
        ConfigResource,
        ConfigResourceType,
        KafkaAdminClient,
        NewTopic,
    )
    from kafka.errors import KafkaError, TopicAlreadyExistsError
    from kafka.structs import OffsetAndMetadata
except ImportError as e:
    if not _is_missing_optional_dependency(e, "kafka"):
        raise
    raise ImportError(
        "Kafka backend requires 'kafka-python'. Install with: pip install scrapy-extension[kafka]"
    ) from e

from scrapy_extension.backends._generation import (
    GenerationLeaseGate,
    GenerationRecord,
    GenerationUnavailable,
)
from scrapy_extension.backends._redaction import (
    _diagnostic_repr,
    _RedactedStr,
)
from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
)
from scrapy_extension.backends.physical_naming import kafka_physical_topic_name
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
    QueueOutcomeIndeterminateError,
)
from scrapy_extension.exceptions._redaction import (
    backend_connection_error_boundary,
    configuration_error_boundary,
    queue_operation_error_boundary,
)
from scrapy_extension.settings import (
    KafkaMode,
    KafkaSettings,
    KafkaTopicNameGeneration,
)
from scrapy_extension.settings._broker_endpoints import (
    KAFKA_BROKER_ENDPOINTS_ERROR,
)
from scrapy_extension.settings._transport_security import (
    validate_allow_remote_plaintext,
)
from scrapy_extension.settings.kafka import (
    validate_kafka_authentication,
    validate_kafka_delivery_policy,
    validate_kafka_transport_security,
)

# Topic name validation pattern - only allow alphanumeric, dots, underscores, hyphens
# Uses \Z instead of $ to match only at absolute end of string (not before trailing newline)
TOPIC_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+\Z")


def _validate_topic_name(name: str) -> None:
    """Validate topic/queue name to prevent injection.

    Args:
        name: The name to validate.

    Raises:
        ValueError: If name contains invalid characters.
    """
    if type(name) is not str or not name or not TOPIC_NAME_PATTERN.match(name):
        # Keep rejected logical identities out of the public diagnostic.  This
        # helper is also used by strategy code outside the terminal operation
        # redaction boundary, so interpolating the invalid value would retain
        # secrets and opaque handles in ValueError messages/tracebacks.
        raise ValueError(
            "Invalid topic/queue name. Only alphanumeric, dots, underscores, "
            "and hyphens allowed."
        )


def _validate_logical_queue_name(name: str) -> None:
    """Validate a logical queue identity before physical-name resolution."""
    if (
        type(name) is not str
        or not name
        or any(character.isspace() or ord(character) < 32 for character in name)
    ):
        raise ValueError("Invalid topic/queue name: logical queue identity is invalid.")


def _validate_queue_name_argument(
    _backend: object,
    queue_name: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate a logical queue argument before its terminal error boundary."""
    _validate_logical_queue_name(queue_name)


logger = logging.getLogger(__name__)

_KAFKA_CONFIGURATION_SETTING_NAMES: frozenset[str] = frozenset(
    KafkaSettings.model_fields
)
_KAFKA_SAFE_CONFIGURATION_MESSAGES: frozenset[str] = frozenset(
    {
        KAFKA_BROKER_ENDPOINTS_ERROR,
        "Unsupported Kafka mode.",
        (
            "KafkaBackend requires enable_auto_commit=False because queue "
            "delivery completion is controlled by QueueBackend.ack(); enabling "
            "Kafka auto-commit can commit a request before Scrapy processes it."
        ),
        (
            "KafkaBackend requires enable_auto_commit=False because queue "
            "delivery completion is controlled by QueueBackend.ack()."
        ),
    }
)
_KAFKA_SAFE_CONNECTION_MESSAGES: frozenset[str] = frozenset(
    {f"Failed to connect to Kafka ({mode.value})." for mode in KafkaMode}
)
_KAFKA_CLEAR_QUEUE_UNSUPPORTED_MESSAGE = (
    "Kafka clear_queue is unsupported: asynchronous topic delete/recreate "
    "cannot preserve active consumer-group offsets or protect messages "
    "accepted after clear returns. Stop and drain the queue with an "
    "operator-controlled Kafka maintenance workflow instead."
)
_KAFKA_OUTCOME_INDETERMINATE_MESSAGE = (
    "Kafka push outcome is indeterminate; retry may duplicate the message."
)
_KAFKA_REQUEST_TIMEOUT_ERROR = (
    "Kafka request_timeout_ms must be greater than zero for durable push."
)
_KAFKA_MIXED_MODE_ACK_REFUSED_MESSAGE = (
    "Legacy ack(token=None) refused: this topic-partition still has un-acked "
    "pop_with_ack records in flight, so committing would advance the committed "
    "offset past them. Ack each pop_with_ack token instead."
)
_KAFKA_CONNECTION_CHANGED_PUSH_MESSAGE = "Kafka connection changed during push."
_KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE = (
    "Kafka connection changed during operation."
)


class _KafkaConnectionAttemptFenced(RuntimeError):
    """Internal signal that disconnect won a connection attempt."""


_KAFKA_SAFE_QUEUE_MESSAGES: frozenset[str] = frozenset(
    {
        "Kafka create-topics response has no valid topic_errors list.",
        "Kafka create-topics response contains a malformed topic entry.",
        "Kafka create-topics response did not identify the requested topic.",
        "Kafka create-topics response contains an invalid error code.",
        "Existing Kafka topic policy does not match the configured queue policy.",
        "Kafka returned an invalid end offset.",
        "Kafka returned an invalid beginning offset.",
        "Kafka returned invalid offset metadata.",
        "Kafka consumer group has no committed offset and auto_offset_reset='none'.",
        "Kafka returned an invalid committed offset.",
        _KAFKA_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
        _KAFKA_OUTCOME_INDETERMINATE_MESSAGE,
        _KAFKA_REQUEST_TIMEOUT_ERROR,
        _KAFKA_MIXED_MODE_ACK_REFUSED_MESSAGE,
        _KAFKA_CONNECTION_CHANGED_PUSH_MESSAGE,
        _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
    }
)


@dataclass(frozen=True, slots=True)
class _KafkaConnectionSnapshot:
    """Validated connection-used values fixed for one Kafka generation."""

    mode: KafkaMode
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str | None
    sasl_username: str | None
    sasl_password: _RedactedStr | None
    confluent_api_key: _RedactedStr | None
    confluent_api_secret: _RedactedStr | None
    ssl_cafile: str | None
    ssl_certfile: str | None
    ssl_keyfile: str | None
    ssl_check_hostname: bool
    acks: int | str
    retries: int
    batch_size: int
    linger_ms: int
    compression_type: str | None
    max_in_flight_requests_per_connection: int
    request_timeout_ms: int
    max_priority_partitions: int
    num_partitions: int
    replication_factor: int
    retention_ms: int
    min_insync_replicas: int
    group_id: str
    auto_offset_reset: str
    auto_commit_interval_ms: int
    max_poll_records: int
    session_timeout_ms: int
    topic_name_generation: KafkaTopicNameGeneration


@dataclass(slots=True, eq=False)
class _KafkaGenerationHandles:
    """Opaque handles, caches, and legacy delivery state for one generation."""

    producer: Any
    admin_client: Any
    consumer: Any = None
    snapshot: _KafkaConnectionSnapshot | None = None
    known_topics: set[str] | None = None
    known_topic_policies: dict[str, tuple[int, int, int, int]] | None = None
    consumer_generation: int = 0
    assignment_epoch: int = 0
    active_attempts: dict[tuple[str, int, int], int] | None = None
    active_attempts_by_partition: dict[tuple[str, int], set[int]] | None = None
    in_flight: dict[tuple[str, int], set[int]] | None = None
    watermarks: dict[tuple[str, int], int] | None = None
    high_water: dict[tuple[str, int], int] | None = None
    retired: bool = False
    legacy_record: Any = None
    rebalance_listener: Any = None


class _KafkaAckToken:
    """Opaque ack token identifying one consumer-generation delivery.

    Stored in ``request.meta["_backend_ack_token"]`` and handed back to
    :meth:`KafkaBackend.ack` / :meth:`KafkaBackend.nack` so the specific
    message that was popped is acked — not the last-popped one. This is
    what makes ack correct under ``CONCURRENT_REQUESTS > 1``: N pops before
    any ack no longer overwrite a single ``_last_record`` slot.

    Attributes:
        partition: Kafka partition the record was consumed from.
        offset: The record's offset within that partition.
        topic: The topic the record was consumed from (needed to build a
            ``TopicPartition`` for the watermark commit).
        consumer_generation: Consumer lifecycle generation that delivered it.
        assignment_epoch: Subscription/rebalance epoch that delivered it.
        delivery_attempt: Unique identity for this concrete delivery attempt.
    """

    __slots__ = (
        "assignment_epoch",
        "consumer_generation",
        "delivery_attempt",
        "offset",
        "partition",
        "topic",
    )

    def __init__(
        self,
        partition: int,
        offset: int,
        topic: str,
        consumer_generation: int = 0,
        assignment_epoch: int = 0,
        delivery_attempt: int = 0,
    ) -> None:
        """Initialize the token.

        Args:
            partition: Kafka partition.
            offset: Record offset within the partition.
            topic: The topic the record was consumed from.
            consumer_generation: Consumer lifecycle generation that delivered it.
            assignment_epoch: Subscription/rebalance epoch that delivered it.
            delivery_attempt: Unique attempt within this backend instance.
        """
        self.partition = partition
        self.offset = offset
        self.topic = topic
        self.consumer_generation = consumer_generation
        self.assignment_epoch = assignment_epoch
        self.delivery_attempt = delivery_attempt

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _KafkaAckToken):
            return NotImplemented
        return (
            self.partition == other.partition
            and self.offset == other.offset
            and self.topic == other.topic
            and self.consumer_generation == other.consumer_generation
            and self.assignment_epoch == other.assignment_epoch
            and self.delivery_attempt == other.delivery_attempt
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.partition,
                self.offset,
                self.topic,
                self.consumer_generation,
                self.assignment_epoch,
                self.delivery_attempt,
            )
        )

    def __repr__(self) -> str:
        return (
            f"_KafkaAckToken(topic={_diagnostic_repr(self.topic)}, partition={self.partition}, "
            f"offset={self.offset}, consumer_generation={self.consumer_generation}, "
            f"assignment_epoch={self.assignment_epoch}, "
            f"delivery_attempt={self.delivery_attempt})"
        )


class _KafkaRebalanceListener(
    ConsumerRebalanceListener  # type: ignore[misc]
):
    """Fence tokens only for the generation/consumer that registered us."""

    __slots__ = ("_backend", "_consumer", "_generation")

    def __init__(
        self,
        backend: KafkaBackend,
        generation: GenerationRecord[Any] | None = None,
        consumer: Any = None,
    ) -> None:
        self._backend = backend
        self._generation = generation
        self._consumer = consumer

    def on_partitions_revoked(self, revoked: Any) -> None:
        self._backend._on_assignment_changed(revoked, self._generation, self._consumer)

    def on_partitions_assigned(self, assigned: Any) -> None:
        self._backend._on_assignment_changed(assigned, self._generation, self._consumer)


class KafkaBackend(Backend, QueueBackend):
    """Kafka backend implementation with multi-mode support.

    Implements QueueBackend using Kafka topics with partition-based priority.
    Supports standalone, cluster, and confluent deployment modes.
    Does NOT implement SetBackend or StorageBackend.

    Ack capability: ``requires_ack=True``, ``supports_concurrent_ack=True``.
    Kafka pops carry an ack token with consumer generation, assignment epoch,
    and unique delivery-attempt identity. A per-topic-partition in-flight set
    lets :meth:`ack` commit only the contiguous low-watermark. Rebalances,
    subscription changes, and nacks fence prior attempts, so a late completion
    cannot commit a redelivery of the same offset.

    Attributes:
        config: KafkaSettings instance with connection parameters.
        _producer: The Kafka producer instance.
        _consumer: The Kafka consumer instance.
        _admin_client: The Kafka admin client instance.
        _known_topics: Set of topics known to exist (cached to avoid repeated checks).
    """

    _push_is_durable = True
    requires_ack = True
    supports_concurrent_ack = True

    @configuration_error_boundary(
        "Kafka configuration is invalid.",
        _KAFKA_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_KAFKA_SAFE_CONFIGURATION_MESSAGES,
    )
    def __init__(self, config: KafkaSettings) -> None:
        """Initialize Kafka backend.

        Args:
            config: Configuration for Kafka connection.
        """
        if getattr(config, "enable_auto_commit", False) is True:
            raise ConfigurationError(
                (
                    "KafkaBackend requires enable_auto_commit=False because queue "
                    "delivery completion is controlled by QueueBackend.ack(); enabling "
                    "Kafka auto-commit can commit a request before Scrapy processes it."
                ),
                setting_name="enable_auto_commit",
                setting_value=True,
            )
        self.config = config
        # Kafka producer/admin construction is a single connection generation.
        # Keep it separate from ``_delivery_lock``: connection setup and client
        # teardown can block on SDK I/O, whereas delivery-token bookkeeping must
        # remain a short critical section for ack/rebalance callbacks.
        self._connection_lock = threading.RLock()
        # Incremented before disconnect waits for the connection single-flight
        # lock. This lets a concurrent teardown fence a private constructor
        # candidate even while kafka-python is blocked in external code.
        self._lifecycle_lock = threading.RLock()
        self._lifecycle_epoch = 0
        self._connect_attempt_epoch: int | None = None
        self._generation_gate: GenerationLeaseGate[_KafkaGenerationHandles] = (
            GenerationLeaseGate()
        )
        self._connection_snapshot: _KafkaConnectionSnapshot | None = None
        self._producer: KafkaProducer | None = None
        self._consumer: KafkaConsumer | None = None
        self._consumer_auto_offset_reset: str | None = None
        # The same topic/partition/offset may be delivered again after reconnect.
        # Generation-scoped tokens keep late completions from touching that new
        # delivery on the replacement consumer.
        self._consumer_generation = 0
        # Kafka may redeliver the same topic/partition/offset within one consumer
        # generation after seek or rebalance. Epoch + attempt identity prevents a
        # late completion for the old delivery from committing its replacement.
        self._delivery_lock = threading.RLock()
        # Serialize kafka-python calls on the consumer without holding the
        # delivery-state lock across SDK code. Operations acquire this lock
        # before connection/delivery locks, so driver callbacks can re-enter
        # disconnect() without creating a lock-order cycle.
        self._consumer_io_lock = threading.RLock()
        self._assignment_epoch = 0
        self._next_delivery_attempt = 0
        self._active_attempts: dict[tuple[str, int, int], int] = {}
        self._active_attempts_by_partition: dict[tuple[str, int], set[int]] = {}
        self._rebalance_listener = _KafkaRebalanceListener(self)
        # Public helper injection tests and legacy integrations may call the
        # unleased helper from inside an admitted operation. Keep the exact
        # generation available without changing that private call signature.
        self._delivery_lease = threading.local()
        # Legacy single-slot for the ``ack(token=None)`` fallback path. Kept so
        # external callers that pop() then ack() without a token still work.
        self._last_record: Any = None
        # In-flight ack tracking for correctness under CONCURRENT_REQUESTS>1.
        # (topic, partition) -> set of popped-but-unacked offsets. ack(token)
        # records the completed offset and commits the contiguous low-watermark
        # for that topic-partition — the largest offset such that all records from
        # the last-committed offset up to it are completed (no record skipped).
        self._in_flight: dict[tuple[str, int], set[int]] = defaultdict(set)
        # (topic, partition) -> commit watermark base (lowest offset in the current
        # in-flight cohort). Seeded from the first record delivered by
        # pop_with_ack; consumer.position() is the NEXT fetch offset and would
        # incorrectly skip the records awaiting application-level ack.
        self._watermarks: dict[tuple[str, int], int] = {}
        # (topic, partition) -> highest offset ever popped + 1. Bounds the
        # watermark walk so it stops at the frontier of popped records (never
        # walks into not-yet-popped offsets or runs away on an empty set).
        self._high_water: dict[tuple[str, int], int] = {}
        self._admin_client: KafkaAdminClient | None = None
        # Cache known topics to avoid repeated existence checks
        self._known_topics: set[str] = set()
        self._known_topic_policies: dict[str, tuple[int, int, int, int]] = {}
        # Topic the consumer is currently subscribed to, so pop() only
        # re-subscribes when it changes — mirrors RocketMQ's _ensure_subscribed
        # (R7). Avoids a redundant subscribe() on every pop of the same queue
        # (Scrapy's next_request pops the same queue every tick). R2-E3.
        self._subscribed_topic: str | None = None

    def _clear_delivery_state_locked(self) -> None:
        """Drop local delivery state while ``_delivery_lock`` is held."""
        self._last_record = None
        self._in_flight.clear()
        self._watermarks.clear()
        self._high_water.clear()
        self._active_attempts.clear()
        self._active_attempts_by_partition.clear()

    def _connect_attempt_is_fenced(self) -> bool:
        with self._lifecycle_lock:
            epoch = self._connect_attempt_epoch
            return epoch is not None and epoch != self._lifecycle_epoch

    def _advance_assignment_epoch_locked(self) -> None:
        """Fence every delivery from the prior subscription/assignment epoch."""
        self._assignment_epoch += 1
        self._clear_delivery_state_locked()
        current = self._generation_gate.current
        if current is not None and not current.value.retired:
            # ``legacy_record`` is generation-local rather than part of the
            # backend compatibility mirror.  Clear it too, otherwise a bare ack
            # after a rebalance could settle a record from the old assignment.
            current.value.legacy_record = None

    def _on_assignment_changed(
        self,
        partitions: Any,
        generation: GenerationRecord[Any] | None = None,
        consumer: Any = None,
    ) -> None:
        """Fence the listener's generation after an assignment change.

        The connection lock is acquired before the delivery lock, matching the
        disconnect/pop lock order. Identity checks under both locks prevent an
        old callback from clearing or retagging a retired/replacement generation.
        """
        del partitions
        if generation is None or consumer is None:
            return
        with self._connection_lock:
            with self._delivery_lock:
                current = self._generation_gate.current
                if (
                    current is not generation
                    or not current.accepting
                    or current.value.retired
                    or current.value.consumer is not consumer
                ):
                    return
                self._advance_assignment_epoch_locked()
                # Keep the generation-local token fence synchronized with the
                # live epoch, then recheck the identity fence before publishing it.
                current = self._generation_gate.current
                if (
                    current is generation
                    and current.accepting
                    and not current.value.retired
                    and current.value.consumer is consumer
                ):
                    current.value.assignment_epoch = self._assignment_epoch

    @staticmethod
    def _attempt_key(token: _KafkaAckToken) -> tuple[str, int, int]:
        return (token.topic, token.partition, token.offset)

    def _token_is_active_locked(
        self,
        token: _KafkaAckToken,
        handles: _KafkaGenerationHandles | None = None,
    ) -> bool:
        """Return whether ``token`` owns an attempt in its admitted generation."""
        if handles is not None:
            # A lease owns this exact record even after retirement. Never switch
            # back to the live mirrors: disconnect replaces those containers so a
            # late settlement cannot mutate a replacement generation.
            consumer_generation = handles.consumer_generation
            assignment_epoch = handles.assignment_epoch
            active_attempts = (
                handles.active_attempts if handles.active_attempts is not None else {}
            )
            in_flight_map = handles.in_flight if handles.in_flight is not None else {}
        else:
            consumer_generation = self._consumer_generation
            assignment_epoch = self._assignment_epoch
            active_attempts = self._active_attempts
            in_flight_map = self._in_flight
        if (
            token.consumer_generation != consumer_generation
            or token.assignment_epoch != assignment_epoch
        ):
            return False
        attempt = active_attempts.get(self._attempt_key(token))
        if attempt is not None:
            return attempt == token.delivery_attempt
        # Compatibility for direct construction of this private token in older
        # callers/tests. Real tokens emitted by pop_with_ack always have a
        # positive unique attempt and therefore never take this branch.
        topic_partition = (token.topic, token.partition)
        in_flight = in_flight_map.get(topic_partition)
        return (
            token.delivery_attempt == 0
            and in_flight is not None
            and token.offset in in_flight
        )

    def _finish_attempt_locked(
        self,
        token: _KafkaAckToken,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        active_attempts = (
            handles.active_attempts if handles is not None else self._active_attempts
        )
        if active_attempts is None:
            return
        key = self._attempt_key(token)
        if active_attempts.get(key) == token.delivery_attempt:
            active_attempts.pop(key, None)
            attempts_by_partition = (
                handles.active_attempts_by_partition
                if handles is not None
                else self._active_attempts_by_partition
            )
            partition_attempts = (
                attempts_by_partition.get((token.topic, token.partition))
                if attempts_by_partition is not None
                else None
            )
            if attempts_by_partition is not None and partition_attempts is not None:
                partition_attempts.discard(token.delivery_attempt)
                if not partition_attempts:
                    attempts_by_partition.pop((token.topic, token.partition), None)

    def _ensure_rebalance_listener_locked(
        self,
        generation: GenerationRecord[_KafkaGenerationHandles],
        consumer: Any,
    ) -> _KafkaRebalanceListener:
        """Return a listener bound to one accepting generation and consumer."""
        handles = generation.value
        listener = handles.rebalance_listener
        if (
            not isinstance(listener, _KafkaRebalanceListener)
            or listener._generation is not generation
            or listener._consumer is not consumer
        ):
            listener = _KafkaRebalanceListener(self, generation, consumer)
            handles.rebalance_listener = listener
            # Compatibility callers see the listener of the current consumer;
            # older consumers retain their own identity-bound listener.
            self._rebalance_listener = listener
        return listener

    @staticmethod
    def _log_success_diagnostic(message: str, *args: object) -> None:
        """Emit a best-effort diagnostic after a successful state commit.

        Logging handlers are application code. A handler failure after a completed
        connection generation or topic-cache update must not turn that success into
        a failed operation.
        """
        try:
            logger.debug(message, *args)
        except BaseException:
            pass

    @staticmethod
    def _log_cleanup_diagnostic() -> None:
        """Report a completed cleanup failure without exposing its exception."""
        try:
            logger.debug("Failed to close a detached Kafka client.")
        except BaseException:
            pass

    @backend_connection_error_boundary(
        "Failed to connect to Kafka.",
        "kafka",
        safe_messages=_KAFKA_SAFE_CONNECTION_MESSAGES,
    )
    @configuration_error_boundary(
        "Kafka configuration is invalid.",
        _KAFKA_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_KAFKA_SAFE_CONFIGURATION_MESSAGES,
        pass_through_exception_types=(BackendConnectionError,),
    )
    def connect(self) -> None:
        """Establish connection to Kafka based on deployment mode.

        Creates Kafka producer and admin client with mode-specific configuration.

        Raises:
            BackendConnectionError: If the connection cannot be established.
            ConfigurationError: If the configuration is invalid for the mode.
        """
        with self._connection_lock:
            # A complete producer/admin graph belongs to the current generation and
            # must never be replaced by a redundant or overlapping connect().
            if self._producer is not None and self._admin_client is not None:
                self._publish_generation_locked()
                return

            # A prior interrupted attempt can leave exactly one handle assigned.
            # Detach it before beginning a fresh generation; otherwise a successful
            # retry would overwrite and leak that residual client.
            if self._producer is not None or self._admin_client is not None:
                residual_cleanup_failed = self._abort_partial_connect(
                    suppress_process_control=True
                )
                if residual_cleanup_failed:
                    self._log_cleanup_diagnostic()
                # A close/logger callback can synchronously publish a replacement
                # generation. Never overwrite that callback-owned generation with
                # this stale connect attempt.
                if self._producer is not None and self._admin_client is not None:
                    self._publish_generation_locked()
                    return

            mode = getattr(self.config, "mode", None)
            if mode not in (
                KafkaMode.STANDALONE,
                KafkaMode.CLUSTER,
                KafkaMode.CONFLUENT,
            ):
                raise ConfigurationError(
                    "Unsupported Kafka mode.",
                    setting_name="mode",
                )
            snapshot = self._capture_connection_snapshot()
            with self._lifecycle_lock:
                self._connect_attempt_epoch = self._lifecycle_epoch
            startup_error: BackendConnectionError | None = None
            cleanup_diagnostic_pending = False
            try:
                if snapshot.mode == KafkaMode.STANDALONE:
                    self._connect_standalone(snapshot)
                elif snapshot.mode == KafkaMode.CLUSTER:
                    self._connect_cluster(snapshot)
                else:
                    self._connect_confluent(snapshot)
                with self._lifecycle_lock:
                    if self._connect_attempt_is_fenced():
                        raise _KafkaConnectionAttemptFenced()
                    self._connection_snapshot = snapshot
                    self._publish_generation_locked()
                self._log_success_diagnostic(
                    "Connected to Kafka in %s mode", snapshot.mode.value
                )
            except _KafkaConnectionAttemptFenced:
                # Teardown won while the SDK constructors were in flight. The
                # private clients are retired below; disconnect owns the lifecycle
                # outcome, so a stale connect is not reported as a new failure.
                self._abort_partial_connect(suppress_process_control=True)
                with self._lifecycle_lock:
                    self._connect_attempt_epoch = None
                return
            except KafkaError:
                cleanup_diagnostic_pending = self._abort_partial_connect(
                    suppress_process_control=True
                )
                startup_error = BackendConnectionError(
                    f"Failed to connect to Kafka ({snapshot.mode.value}).",
                    backend_type="kafka",
                )
            except Exception:
                cleanup_diagnostic_pending = self._abort_partial_connect(
                    suppress_process_control=True
                )
                # Unexpected driver/plugin errors are not safe public diagnostics.
                startup_error = BackendConnectionError(
                    f"Failed to connect to Kafka ({snapshot.mode.value}).",
                    backend_type="kafka",
                )
            except BaseException:
                with self._lifecycle_lock:
                    self._connect_attempt_epoch = None
                # KeyboardInterrupt/SystemExit are not ``Exception`` subclasses, so the
                # arms above cannot catch them — without this arm a Ctrl+C raised in the
                # window between ``self._producer = ...`` and ``self._admin_client = ...``
                # skips ``_abort_partial_connect()``, leaking the producer (TCP socket +
                # bg thread) and leaving ``is_connected()`` lying True. Run the cleanup
                # before re-raising. Mirrors mongodb.py / elasticsearch.py / dynamodb /
                # redis ``except BaseException`` arms.
                self._abort_partial_connect(suppress_process_control=True)
                raise

            if cleanup_diagnostic_pending:
                # The driver failure's handler has finished.  Do not let a custom
                # logging handler inspect it through ``sys.exc_info()``.
                self._log_cleanup_diagnostic()

            with self._lifecycle_lock:
                self._connect_attempt_epoch = None
            if startup_error is not None:
                # Raise outside the driver exception handler so endpoint/credential
                # text cannot survive through ``__cause__`` or ``__context__``.
                raise startup_error

    def _abort_partial_connect(self, *, suppress_process_control: bool = False) -> bool:
        """Close+null any clients assigned before ``connect()`` failed.

        R-kacc: in each ``_connect_*`` path ``self._producer`` is assigned
        BEFORE ``KafkaAdminClient`` is constructed. If admin construction (or
        any later step) raises, ``self._producer`` would otherwise stay set so
        :meth:`is_connected` lies ``True`` (silent wedge — backend reports
        connected but has no admin client, so ping/queue_len/clear_queue are
        dead) and the producer leaks under the ConnectionManager retry loop.

        R17-A: null FIRST, then best-effort close the captured locals (mirror
        rocketmq ``_abort_partial_connect`` / mongodb ``_discard_client``). The
        R16-A ``except BaseException`` arm routes into this helper while an
        interrupt is already in flight; a prior close-then-null body closed under
        ``contextlib.suppress(Exception)`` — which cannot catch ``BaseException`` —
        so a second ``Ctrl+C`` raised by the blocking ``KafkaProducer.close()``
        escaped before ``self._producer = None`` ran, re-wedging the backend.
        Nulling first makes ``is_connected()`` truthful the instant the abort is
        entered, regardless of what ``close()`` raises. Both detached siblings are
        always offered ``close()``, even if the first one raises a control-flow
        exception. Ordinary ``Exception`` failures are logged at debug; direct
        callers receive the first ``BaseException`` after all cleanup has been
        attempted. ``connect()`` uses ``suppress_process_control=True`` so cleanup
        cannot replace the original failed-connect cause or prevent a residual
        generation from being retired before a retry. The return value records an
        ordinary close failure for a caller to diagnose after its exception handler
        has exited.
        """
        retired = self._retire_generation_locked()
        generation_handles = retired.value if retired is not None else None
        producer = (
            generation_handles.producer
            if generation_handles is not None
            else self._producer
        )
        admin = (
            generation_handles.admin_client
            if generation_handles is not None
            else self._admin_client
        )
        consumer = (
            generation_handles.consumer
            if generation_handles is not None
            else self._consumer
        )
        self._producer = None
        self._admin_client = None
        self._consumer = None
        self._connection_snapshot = None
        with self._delivery_lock:
            if retired is not None:
                retired.value.retired = True
                retired.value.assignment_epoch = self._assignment_epoch
                retired.value.legacy_record = self._last_record
                retired.value.active_attempts = self._active_attempts
                retired.value.active_attempts_by_partition = (
                    self._active_attempts_by_partition
                )
                retired.value.in_flight = self._in_flight
                retired.value.watermarks = self._watermarks
                retired.value.high_water = self._high_water
                self._active_attempts = {}
                self._active_attempts_by_partition = {}
                self._in_flight = defaultdict(set)
                self._watermarks = {}
                self._high_water = {}
            self._last_record = None

        ordinary_close_failed = False
        primary_error: BaseException | None = None

        def finalize() -> None:
            nonlocal ordinary_close_failed, primary_error
            for closer in (producer, consumer, admin):
                if closer is None:
                    continue
                try:
                    closer.close()
                except Exception:
                    ordinary_close_failed = True
                except BaseException as error:
                    if primary_error is None:
                        primary_error = error

        generation_control_error = self._generation_gate.drain(retired, finalize)
        if retired is None or retired.active_leases == 0:
            if generation_control_error is not None and not suppress_process_control:
                raise generation_control_error
            if primary_error is not None and not suppress_process_control:
                raise primary_error
        return ordinary_close_failed

    @configuration_error_boundary(
        "Kafka configuration is invalid.",
        _KAFKA_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_KAFKA_SAFE_CONFIGURATION_MESSAGES,
    )
    def _capture_connection_snapshot(self) -> _KafkaConnectionSnapshot:
        """Copy and revalidate every setting consumed by one client generation."""
        raw_values = self.config.__dict__.copy()
        validate_allow_remote_plaintext(raw_values.get("allow_remote_plaintext"))
        validated: KafkaSettings | None = None
        settings_error: ConfigurationError | None = None
        try:
            validated = KafkaSettings.model_validate(raw_values, strict=True)
        except ConfigurationError as exc:
            setting_name = exc.setting_name or "kafka"
            settings_error = ConfigurationError(
                f"Invalid Kafka setting '{setting_name}'.",
                setting_name=setting_name,
            )
        except ValidationError as exc:
            errors = exc.errors()
            location = errors[0].get("loc", ()) if errors else ()
            setting_name = str(location[0]) if location else "kafka"
            settings_error = ConfigurationError(
                f"Invalid Kafka setting '{setting_name}'.",
                setting_name=setting_name,
            )

        if settings_error is not None:
            # Raise outside the validator exception handler so raw mutated settings
            # cannot survive through ``__cause__`` or ``__context__``.
            raise settings_error
        assert validated is not None

        if validated.request_timeout_ms <= 0:
            raise ConfigurationError(
                _KAFKA_REQUEST_TIMEOUT_ERROR,
                setting_name="request_timeout_ms",
            )
        if validated.enable_auto_commit is not False:
            raise ConfigurationError(
                (
                    "KafkaBackend requires enable_auto_commit=False because queue "
                    "delivery completion is controlled by QueueBackend.ack()."
                ),
                setting_name="enable_auto_commit",
            )
        mechanism, username, password, api_key, api_secret = (
            validate_kafka_authentication(
                validated.mode,
                validated.security_protocol,
                validated.sasl_mechanism,
                validated.sasl_username,
                validated.sasl_password,
                validated.confluent_api_key,
                validated.confluent_api_secret,
            )
        )
        validate_kafka_transport_security(
            validated.mode,
            validated.security_protocol,
            validated.ssl_check_hostname,
        )
        acks, max_priority_partitions, replicas, retention, min_isr = (
            validate_kafka_delivery_policy(
                validated.acks,
                validated.max_priority_partitions,
                validated.num_partitions,
                validated.replication_factor,
                validated.retention_ms,
                validated.min_insync_replicas,
            )
        )
        if validated.mode == KafkaMode.CLUSTER and validated.cluster_brokers:
            bootstrap_servers = ",".join(validated.cluster_brokers)
        elif validated.mode == KafkaMode.CONFLUENT:
            bootstrap_servers = (validated.confluent_bootstrap_servers or "").strip()
            if not bootstrap_servers:
                bootstrap_servers = validated.bootstrap_servers
        else:
            bootstrap_servers = validated.bootstrap_servers

        return _KafkaConnectionSnapshot(
            mode=validated.mode,
            bootstrap_servers=bootstrap_servers,
            security_protocol=validated.security_protocol,
            sasl_mechanism=mechanism,
            sasl_username=username,
            sasl_password=(_RedactedStr(password) if password is not None else None),
            confluent_api_key=(_RedactedStr(api_key) if api_key is not None else None),
            confluent_api_secret=(
                _RedactedStr(api_secret) if api_secret is not None else None
            ),
            ssl_cafile=validated.ssl_cafile,
            ssl_certfile=validated.ssl_certfile,
            ssl_keyfile=validated.ssl_keyfile,
            ssl_check_hostname=validated.ssl_check_hostname,
            acks=acks,
            retries=validated.retries,
            batch_size=validated.batch_size,
            linger_ms=validated.linger_ms,
            compression_type=validated.compression_type,
            max_in_flight_requests_per_connection=(
                validated.max_in_flight_requests_per_connection
            ),
            request_timeout_ms=validated.request_timeout_ms,
            max_priority_partitions=max_priority_partitions,
            num_partitions=validated.num_partitions,
            replication_factor=replicas,
            retention_ms=retention,
            min_insync_replicas=min_isr,
            group_id=validated.group_id,
            auto_offset_reset=validated.auto_offset_reset,
            auto_commit_interval_ms=validated.auto_commit_interval_ms,
            max_poll_records=validated.max_poll_records,
            session_timeout_ms=validated.session_timeout_ms,
            topic_name_generation=validated.topic_name_generation,
        )

    def _connection_snapshot_or_capture(self) -> _KafkaConnectionSnapshot:
        """Return the published generation plan or validate one for direct use.

        Public operations serialize with a concurrent connect before inspecting
        generation state. A snapshot under construction is never visible: once the
        connection gate is released there is either a published generation or no
        connection attempt, in which case direct use captures a fresh plan.
        """
        with self._connection_lock:
            return self._connection_snapshot or self._capture_connection_snapshot()

    def _publish_generation_locked(self, *, allow_empty: bool = False) -> None:
        """Publish one compatible, non-empty handle graph after setup.

        A normal connection publishes producer/admin together.  Compatibility
        callers may inject the consumer alone for pop/ack tests or the admin
        alone for ping, but an accepting generation is never manufactured from
        no handles: an empty record would block a later real ``connect()`` from
        publishing its generation.
        """
        del allow_empty
        if (
            self._producer is None
            and self._admin_client is None
            and self._consumer is None
        ):
            return
        if self._generation_gate.current is not None:
            return
        snapshot = self._connection_snapshot or self._capture_connection_snapshot()
        # Caches are generation-local. Reusing the mutable live set would let a
        # late old-generation topic check write into a newly connected client.
        self._known_topics = set(self._known_topics)
        self._known_topic_policies = dict(self._known_topic_policies)
        record = _KafkaGenerationHandles(
            producer=self._producer,
            admin_client=self._admin_client,
            consumer=self._consumer,
            snapshot=snapshot,
            known_topics=self._known_topics,
            known_topic_policies=self._known_topic_policies,
            consumer_generation=self._consumer_generation,
            assignment_epoch=self._assignment_epoch,
            active_attempts=self._active_attempts,
            active_attempts_by_partition=self._active_attempts_by_partition,
            in_flight=self._in_flight,
            watermarks=self._watermarks,
            high_water=self._high_water,
        )
        published = self._generation_gate.publish(record)
        if published.value.rebalance_listener is None:
            published.value.rebalance_listener = _KafkaRebalanceListener(
                self, published, published.value.consumer
            )
            self._rebalance_listener = published.value.rebalance_listener

    def _retire_generation_locked(
        self,
    ) -> GenerationRecord[_KafkaGenerationHandles] | None:
        """Stop new operations before detaching compatibility mirrors."""
        return self._generation_gate.retire()

    @contextmanager
    def _lease_generation(
        self,
        operation: str,
        *,
        queue_name: str | None = None,
        allow_disconnected: bool = False,
    ) -> Iterator[GenerationRecord[_KafkaGenerationHandles] | None]:
        """Lease one Kafka client graph for the full SDK operation.

        No lease record is created for a disconnected backend.  ``queue_len`` is
        the sole compatibility exception: its historical temporary-consumer
        probe may run without an injected live handle, but it still must not
        publish an empty accepting generation.
        """
        with self._connection_lock:
            current = self._generation_gate.current
            if current is None:
                if operation == "push":
                    # A producer plus a cached topic is a complete push graph;
                    # the admin client is needed only for first-use topic creation.
                    required_graph = self._producer is not None and (
                        self._admin_client is not None or bool(self._known_topics)
                    )
                elif operation == "ping":
                    required_graph = self._admin_client is not None
                else:
                    required_graph = self._consumer is not None
                if required_graph:
                    self._publish_generation_locked()
                    current = self._generation_gate.current
            if current is None and not allow_disconnected:
                raise QueueError(
                    "Kafka backend is disconnected.",
                    queue_name=queue_name,
                    operation=operation,
                )
            if current is not None:
                # Compatibility callers may populate the legacy mirror after
                # publication. Copy only into the accepting record; a retired
                # record is never rebound to mutable live state.
                if (
                    current.value.legacy_record is None
                    and self._last_record is not None
                ):
                    current.value.legacy_record = self._last_record
        try:
            if current is None:
                yield None
            else:
                with self._generation_gate.lease(
                    operation, queue_name=queue_name
                ) as record:
                    yield record
        except GenerationUnavailable:
            raise QueueError(
                "Kafka backend is disconnected.",
                queue_name=queue_name,
                operation=operation,
            ) from None

    def _build_common_config(
        self, snapshot: _KafkaConnectionSnapshot | None = None
    ) -> dict[str, Any]:
        """Build common Kafka client configuration from one frozen snapshot."""
        snapshot = snapshot or self._connection_snapshot_or_capture()
        config: dict[str, Any] = {
            "acks": snapshot.acks,
            "retries": snapshot.retries,
            "batch_size": snapshot.batch_size,
            "linger_ms": snapshot.linger_ms,
            "compression_type": snapshot.compression_type,
            "max_in_flight_requests_per_connection": (
                snapshot.max_in_flight_requests_per_connection
            ),
            "request_timeout_ms": snapshot.request_timeout_ms,
        }

        if snapshot.security_protocol != "PLAINTEXT":
            config["security_protocol"] = snapshot.security_protocol
            if snapshot.sasl_mechanism is not None:
                config["sasl_mechanism"] = snapshot.sasl_mechanism
            if (
                snapshot.sasl_username is not None
                and snapshot.sasl_password is not None
            ):
                config["sasl_plain_username"] = snapshot.sasl_username
                config["sasl_plain_password"] = snapshot.sasl_password
            if snapshot.ssl_cafile:
                config["ssl_cafile"] = snapshot.ssl_cafile
            if snapshot.ssl_certfile:
                config["ssl_certfile"] = snapshot.ssl_certfile
            if snapshot.ssl_keyfile:
                config["ssl_keyfile"] = snapshot.ssl_keyfile
            config["ssl_check_hostname"] = snapshot.ssl_check_hostname

        return config

    def _bootstrap_servers(
        self, snapshot: _KafkaConnectionSnapshot | None = None
    ) -> str:
        """Return bootstrap servers from one validated connection snapshot."""
        snapshot = snapshot or self._connection_snapshot_or_capture()
        return snapshot.bootstrap_servers

    def _build_client_security_config(
        self, snapshot: _KafkaConnectionSnapshot | None = None
    ) -> dict[str, Any]:
        """Build consumer/admin-safe security config without producer-only args."""
        snapshot = snapshot or self._connection_snapshot_or_capture()
        if snapshot.mode == KafkaMode.CONFLUENT:
            if (
                snapshot.confluent_api_key is not None
                and snapshot.confluent_api_secret is not None
            ):
                return {
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                    "sasl_plain_username": snapshot.confluent_api_key,
                    "sasl_plain_password": snapshot.confluent_api_secret,
                    "ssl_check_hostname": snapshot.ssl_check_hostname,
                }

        common_config = self._build_common_config(snapshot)
        client_config: dict[str, Any] = {}
        for key in (
            "security_protocol",
            "sasl_mechanism",
            "sasl_plain_username",
            "sasl_plain_password",
            "ssl_cafile",
            "ssl_certfile",
            "ssl_keyfile",
            "ssl_check_hostname",
        ):
            if key in common_config:
                client_config[key] = common_config[key]
        return client_config

    def _build_producer_config(
        self, snapshot: _KafkaConnectionSnapshot | None = None
    ) -> dict[str, Any]:
        """Build producer config with mode-specific bootstrap and security settings."""
        snapshot = snapshot or self._connection_snapshot_or_capture()
        config = self._build_common_config(snapshot)
        config["bootstrap_servers"] = snapshot.bootstrap_servers
        if snapshot.mode == KafkaMode.CONFLUENT:
            config.update(self._build_client_security_config(snapshot))
        return config

    def _connect_standalone(
        self, snapshot: _KafkaConnectionSnapshot | None = None
    ) -> None:
        """Connect to standalone Kafka broker."""
        snapshot = snapshot or self._connection_snapshot_or_capture()
        bootstrap = snapshot.bootstrap_servers
        producer_config = self._build_producer_config(snapshot)
        client_security_config = self._build_client_security_config(snapshot)

        producer = KafkaProducer(**producer_config)
        fenced = self._connect_attempt_is_fenced()
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                client_id="scrapy-extension-admin",
                **client_security_config,
            )
        except BaseException:
            with contextlib.suppress(BaseException):
                producer.close()
            raise
        fenced = fenced or self._connect_attempt_is_fenced()
        if fenced:
            with contextlib.suppress(BaseException):
                producer.close()
            with contextlib.suppress(BaseException):
                admin_client.close()
            raise _KafkaConnectionAttemptFenced()
        self._producer = producer
        self._admin_client = admin_client
        self._log_success_diagnostic("Connected to standalone Kafka")

    def _connect_cluster(
        self, snapshot: _KafkaConnectionSnapshot | None = None
    ) -> None:
        """Connect to Kafka cluster.

        Uses cluster_brokers if configured, otherwise falls back to bootstrap_servers.
        """
        snapshot = snapshot or self._connection_snapshot_or_capture()
        bootstrap = snapshot.bootstrap_servers
        producer_config = self._build_producer_config(snapshot)
        client_security_config = self._build_client_security_config(snapshot)

        producer = KafkaProducer(**producer_config)
        fenced = self._connect_attempt_is_fenced()
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                client_id="scrapy-extension-admin",
                **client_security_config,
            )
        except BaseException:
            with contextlib.suppress(BaseException):
                producer.close()
            raise
        fenced = fenced or self._connect_attempt_is_fenced()
        if fenced:
            with contextlib.suppress(BaseException):
                producer.close()
            with contextlib.suppress(BaseException):
                admin_client.close()
            raise _KafkaConnectionAttemptFenced()
        self._producer = producer
        self._admin_client = admin_client
        self._log_success_diagnostic("Connected to Kafka cluster")

    def _connect_confluent(
        self, snapshot: _KafkaConnectionSnapshot | None = None
    ) -> None:
        """Connect to Confluent Cloud.

        Uses SASL/SSL authentication with Confluent-specific settings.
        """
        snapshot = snapshot or self._connection_snapshot_or_capture()
        bootstrap = snapshot.bootstrap_servers
        producer_config = self._build_producer_config(snapshot)
        client_security_config = self._build_client_security_config(snapshot)

        producer = KafkaProducer(**producer_config)
        fenced = self._connect_attempt_is_fenced()
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                client_id="scrapy-extension-admin",
                **client_security_config,
            )
        except BaseException:
            with contextlib.suppress(BaseException):
                producer.close()
            raise
        fenced = fenced or self._connect_attempt_is_fenced()
        if fenced:
            with contextlib.suppress(BaseException):
                producer.close()
            with contextlib.suppress(BaseException):
                admin_client.close()
            raise _KafkaConnectionAttemptFenced()
        self._producer = producer
        self._admin_client = admin_client
        self._log_success_diagnostic("Connected to Confluent Cloud")

    def disconnect(self) -> None:
        """Retire one generation, drain operations, then close its handles."""
        # Fence a constructor that is currently outside the lifecycle lock. The
        # public teardown may still wait for the connection single-flight, but the
        # attempt can no longer publish when it returns from SDK code.
        with self._lifecycle_lock:
            self._lifecycle_epoch += 1
        with self._connection_lock:
            retired = self._retire_generation_locked()
            generation_handles = retired.value if retired is not None else None
            producer = (
                generation_handles.producer
                if generation_handles is not None
                else self._producer
            )
            consumer = (
                generation_handles.consumer
                if generation_handles is not None
                else self._consumer
            )
            admin_client = (
                generation_handles.admin_client
                if generation_handles is not None
                else self._admin_client
            )
            with self._delivery_lock:
                self._producer = None
                self._consumer = None
                self._consumer_auto_offset_reset = None
                self._admin_client = None
                self._connection_snapshot = None
                self._consumer_generation += 1
                self._assignment_epoch += 1
                self._subscribed_topic = None
                if retired is not None:
                    retired.value.retired = True
                    # Disconnect increments the live epoch immediately below;
                    # retain the pre-fence epoch for settlements already admitted.
                    retired.value.assignment_epoch = self._assignment_epoch - 1
                    retired.value.legacy_record = self._last_record
                    retired.value.active_attempts = self._active_attempts
                    retired.value.active_attempts_by_partition = (
                        self._active_attempts_by_partition
                    )
                    retired.value.in_flight = self._in_flight
                    retired.value.watermarks = self._watermarks
                    retired.value.high_water = self._high_water
                    self._active_attempts = {}
                    self._active_attempts_by_partition = {}
                    self._in_flight = defaultdict(set)
                    self._watermarks = {}
                    self._high_water = {}
                else:
                    self._clear_delivery_state_locked()
                # Clearing the live compatibility slot is part of the disconnect
                # linearization point; a later legacy ack must not publish stale
                # delivery state into a replacement generation.
                self._last_record = None
                self._known_topics = set()
                self._known_topic_policies = {}

        cleanup_error: BaseException | None = None

        def finalize() -> None:
            nonlocal cleanup_error
            try:
                self._close_detached_clients(producer, consumer, admin_client)
            except BaseException as error:
                cleanup_error = error

        generation_control_error = self._generation_gate.drain(retired, finalize)
        if retired is None or retired.active_leases == 0:
            if generation_control_error is not None:
                raise generation_control_error
            if cleanup_error is not None:
                raise cleanup_error

    @staticmethod
    def _close_detached_clients(*clients: Any) -> None:
        """Close every detached client, retaining the first control exception."""
        primary_error: BaseException | None = None
        ordinary_close_failed = False
        for client in clients:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    ordinary_close_failed = True
                except BaseException as error:
                    if primary_error is None:
                        primary_error = error
        if primary_error is not None:
            raise primary_error
        if ordinary_close_failed:
            try:
                logger.debug("Ignoring Kafka client-close failure")
            except BaseException:
                pass

    def is_connected(self) -> bool:
        """Return whether the accepting generation owns a complete client graph."""
        with self._connection_lock:
            generation = self._generation_gate.current
            if generation is None or not generation.accepting:
                return False
            handles = generation.value
            return handles.producer is not None and handles.admin_client is not None

    def ping(self) -> bool:
        """Check Kafka health while leasing the exact admin generation."""
        try:
            with self._lease_generation("ping") as generation:
                if generation is None or generation.value.admin_client is None:
                    return False
                generation.value.admin_client.list_topics()
                return True
        except Exception:
            return False

    @property
    def backend_type(self) -> BackendType:
        """Return backend type.

        Returns:
            BackendType.KAFKA
        """
        return BackendType.KAFKA

    def _topic_name(
        self,
        queue_name: str,
        snapshot: _KafkaConnectionSnapshot | None = None,
    ) -> str:
        """Resolve one logical queue to its generation's physical topic."""
        _validate_logical_queue_name(queue_name)
        snapshot = snapshot or self._connection_snapshot_or_capture()
        return kafka_physical_topic_name(
            "scrapy-", queue_name, snapshot.topic_name_generation
        )

    def _ensure_topic_exists(
        self,
        queue_name: str,
        snapshot: _KafkaConnectionSnapshot | None = None,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        """Ensure Kafka topic exists for queue.

        Uses a local cache to avoid repeated existence checks. Attempts to
        create the topic and catches TopicAlreadyExistsError to avoid the
        TOCTOU (Time-of-Check-Time-of-Use) anti-pattern.

        Args:
            queue_name: Name of the queue/topic.

        Raises:
            ValueError: If queue_name contains invalid characters.
            QueueError: If Kafka rejects creation or returns a malformed response.
        """
        _validate_logical_queue_name(queue_name)
        snapshot = snapshot or self._connection_snapshot_or_capture()
        topic_name = self._topic_name(queue_name, snapshot)
        partitions = snapshot.num_partitions
        replicas = snapshot.replication_factor
        retention = snapshot.retention_ms
        min_isr = snapshot.min_insync_replicas
        policy = (partitions, replicas, retention, min_isr)
        known_topics = (
            handles.known_topics
            if handles is not None and handles.known_topics is not None
            else self._known_topics
        )
        known_topic_policies = (
            handles.known_topic_policies
            if handles is not None and handles.known_topic_policies is not None
            else self._known_topic_policies
        )
        admin_client = (
            handles.admin_client if handles is not None else self._admin_client
        )

        # Skip if topic is already known to exist
        if topic_name in known_topics:
            cached_policy = known_topic_policies.get(topic_name)
            # A missing private cache entry supports older direct tests/callers that
            # pre-populate _known_topics. Production-created entries always carry a
            # policy and are rechecked if live settings change.
            if cached_policy is None or cached_policy == policy:
                return
            self._validate_existing_topic_policy(
                topic_name=topic_name,
                queue_name=queue_name,
                partitions=partitions,
                replicas=replicas,
                retention=retention,
                min_isr=min_isr,
                admin_client=admin_client,
            )
            known_topic_policies[topic_name] = policy
            return

        created = False
        try:
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=replicas,
                topic_configs={
                    "min.insync.replicas": str(min_isr),
                    "retention.ms": str(retention),
                },
            )
            if admin_client is None:
                msg = "KafkaBackend not connected: admin client is None"
                raise BackendConnectionError(msg, backend_type="kafka")
            response = admin_client.create_topics([new_topic])
            created = self._validate_topic_creation_response(
                response,
                topic_name=topic_name,
                queue_name=queue_name,
            )
        except TopicAlreadyExistsError:
            created = False
        except KafkaError as e:
            msg = f"Failed to create Kafka topic {topic_name}."
            raise QueueError(
                msg,
                queue_name=queue_name,
                operation="push",
            ) from e
        if not created:
            try:
                self._validate_existing_topic_policy(
                    topic_name=topic_name,
                    queue_name=queue_name,
                    partitions=partitions,
                    replicas=replicas,
                    retention=retention,
                    min_isr=min_isr,
                    admin_client=admin_client,
                )
            except KafkaError as e:
                msg = f"Failed to inspect existing Kafka topic {topic_name}."
                raise QueueError(
                    msg,
                    queue_name=queue_name,
                    operation="push",
                ) from e
        known_topics.add(topic_name)
        known_topic_policies[topic_name] = policy
        self._log_success_diagnostic(
            "Kafka topic %s.",
            "created" if created else "verified as existing",
        )

    @staticmethod
    def _validate_topic_creation_response(
        response: Any,
        *,
        topic_name: str,
        queue_name: str,
    ) -> bool:
        """Return whether Kafka created the topic; reject malformed failures."""
        topic_errors = getattr(response, "topic_errors", None)
        if not isinstance(topic_errors, (list, tuple)):
            msg = "Kafka create-topics response has no valid topic_errors list."
            raise QueueError(msg, queue_name=queue_name, operation="push")

        matching_entries: list[tuple[Any, ...]] = []
        for entry in topic_errors:
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                msg = "Kafka create-topics response contains a malformed topic entry."
                raise QueueError(msg, queue_name=queue_name, operation="push")
            if entry[0] == topic_name:
                matching_entries.append(tuple(entry))
        if len(matching_entries) != 1:
            msg = "Kafka create-topics response did not identify the requested topic."
            raise QueueError(msg, queue_name=queue_name, operation="push")

        error_code = matching_entries[0][1]
        if isinstance(error_code, bool) or not isinstance(error_code, int):
            msg = "Kafka create-topics response contains an invalid error code."
            raise QueueError(msg, queue_name=queue_name, operation="push")
        if error_code == 0:
            return True
        if error_code == TopicAlreadyExistsError.errno:
            return False
        msg = f"Kafka broker rejected topic creation (error code {error_code})."
        raise QueueError(msg, queue_name=queue_name, operation="push")

    def _validate_existing_topic_policy(
        self,
        *,
        topic_name: str,
        queue_name: str,
        partitions: int,
        replicas: int,
        retention: int,
        min_isr: int,
        admin_client: Any | None = None,
    ) -> None:
        """Fail closed when an existing topic contradicts queue durability.

        Existing topics remain operator-managed: this method never alters broker
        state. It verifies the fields whose mismatch would make accepted public
        settings a silent no-op.
        """
        admin = admin_client if admin_client is not None else self._admin_client
        if admin is None:
            msg = "KafkaBackend not connected: admin client is None"
            raise BackendConnectionError(msg, backend_type="kafka")

        def policy_error() -> QueueError:
            return QueueError(
                "Existing Kafka topic policy does not match the configured queue policy.",
                queue_name=queue_name,
                operation="push",
            )

        descriptions = admin.describe_topics([topic_name])
        if not isinstance(descriptions, (list, tuple)):
            raise policy_error()
        matching = [
            entry
            for entry in descriptions
            if isinstance(entry, dict) and entry.get("topic") == topic_name
        ]
        if len(matching) != 1:
            raise policy_error()
        description = matching[0]
        if description.get("error_code") != 0:
            raise policy_error()
        partition_entries = description.get("partitions")
        if (
            not isinstance(partition_entries, (list, tuple))
            or len(partition_entries) != partitions
        ):
            raise policy_error()
        for entry in partition_entries:
            if not isinstance(entry, dict):
                raise policy_error()
            assigned_replicas = entry.get("replicas")
            if (
                not isinstance(assigned_replicas, (list, tuple))
                or len(assigned_replicas) != replicas
            ):
                raise policy_error()

        resource = ConfigResource(
            ConfigResourceType.TOPIC,
            topic_name,
            configs={"retention.ms": None, "min.insync.replicas": None},
        )
        responses = admin.describe_configs([resource])
        if not isinstance(responses, (list, tuple)):
            raise policy_error()
        resources: list[tuple[Any, ...]] = []
        for response in responses:
            response_resources = getattr(response, "resources", None)
            if not isinstance(response_resources, (list, tuple)):
                raise policy_error()
            for entry in response_resources:
                if not isinstance(entry, (list, tuple)) or len(entry) < 5:
                    raise policy_error()
                if entry[3] == topic_name:
                    resources.append(tuple(entry))
        if len(resources) != 1 or resources[0][0] != 0:
            raise policy_error()
        config_entries = resources[0][4]
        if not isinstance(config_entries, (list, tuple)):
            raise policy_error()
        actual: dict[str, str] = {}
        for entry in config_entries:
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                raise policy_error()
            name, value = entry[0], entry[1]
            if isinstance(name, str) and isinstance(value, str):
                actual[name] = value
        expected = {
            "retention.ms": str(retention),
            "min.insync.replicas": str(min_isr),
        }
        if any(actual.get(name) != value for name, value in expected.items()):
            raise policy_error()

    # QueueBackend implementation
    @queue_operation_error_boundary(
        "push",
        "Failed to push Kafka message.",
        safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        """Push item to priority queue.

        Args:
            queue_name: Name of the queue.
            item: Item to push (bytes).
            priority: Priority value (higher = more urgent, max 255).

        Raises:
            QueueError: If the push operation fails.
        """
        try:
            with self._lease_generation("push", queue_name=queue_name) as generation:
                if generation is None:
                    raise QueueError(
                        "Kafka backend is disconnected.",
                        queue_name=queue_name,
                        operation="push",
                    )
                handles = generation.value
                snapshot = handles.snapshot or self._connection_snapshot_or_capture()
                self._ensure_topic_exists(queue_name, snapshot, handles)
                topic_name = self._topic_name(queue_name, snapshot)
                partition = max(
                    0,
                    min(int(priority), snapshot.max_priority_partitions - 1),
                )

                producer = handles.producer
                if producer is None:
                    msg = "KafkaBackend not connected: producer is None"
                    raise BackendConnectionError(msg, backend_type="kafka")
                future = producer.send(topic_name, value=item, partition=partition)
                # Once send() has handed the record to kafka-python, a timeout or
                # transport loss cannot prove whether the broker committed it.
                try:
                    future.get(timeout=snapshot.request_timeout_ms / 1000.0)
                except Exception as error:
                    # Once send() returned a future, every ordinary failure from
                    # future.get() is an unknown broker outcome. Do not issue a
                    # success receipt; callers may retry at-least-once.
                    raise QueueOutcomeIndeterminateError(
                        _KAFKA_OUTCOME_INDETERMINATE_MESSAGE,
                        queue_name=queue_name,
                        operation="push",
                    ) from error
                # A callback may retire this generation after the broker future
                # resolves. The send outcome is then not a durable success for
                # this public operation, even if Kafka accepted the record.
                if not self._generation_is_current(generation):
                    raise QueueError(
                        _KAFKA_CONNECTION_CHANGED_PUSH_MESSAGE,
                        queue_name=queue_name,
                        operation="push",
                    )
        except KafkaError as e:
            msg = f"Failed to push to queue {queue_name}: {e}"
            raise QueueError(
                msg,
                queue_name=queue_name,
                operation="push",
            ) from e

    @queue_operation_error_boundary(
        "pop",
        "Failed to pop Kafka message.",
        safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop highest priority item from queue.

        Tracks the popped record in ``_last_record`` for the legacy
        ``ack(token=None)`` path. Prefer :meth:`pop_with_ack` under
        ``CONCURRENT_REQUESTS > 1`` — that path tracks every popped offset in
        the per-topic-partition in-flight set so ack(token) commits the correct
        contiguous watermark regardless of pop/ack interleaving.

        Args:
            queue_name: Name of the queue.
            timeout: Seconds to wait (0 = non-blocking).

        Returns:
            The popped item, or None if queue is empty.

        Raises:
            QueueError: If the pop operation fails.
            ValueError: If queue_name contains invalid characters.
        """
        # Consumer bootstrap reads the connection snapshot. Keep the same lock
        # order as disconnect() so a first pop cannot hold delivery while waiting
        # for a concurrent disconnect's connection lock.
        with self._consumer_io_lock:
            with self._connection_lock:
                with self._lease_generation(
                    "pop", queue_name=queue_name, allow_disconnected=True
                ) as generation:
                    with self._delivery_lock:
                        handles = generation.value if generation is not None else None
                        record = self._poll_record(queue_name, timeout, handles)
                        if record is None:
                            return None
                        if handles is None:
                            current = self._generation_gate.current
                            handles = current.value if current is not None else None
                        if handles is not None:
                            self._register_legacy_record_locked(record, handles)
                        else:
                            # The outer connection lock makes this fallback safe for
                            # legacy direct-injection callers, but never publish an
                            # empty generation just to retain a diagnostic slot.
                            self._last_record = record
                        return cast(bytes, record.value)

    @queue_operation_error_boundary(
        "pop",
        "Failed to pop Kafka message.",
        safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def pop_with_ack(
        self, queue_name: str, timeout: float = 0.0
    ) -> tuple[bytes | None, Any | None]:
        """Pop an item together with a :class:`_KafkaAckToken`.

        Records the popped (topic, partition, offset) in the topic-partition's
        in-flight set so :meth:`ack` can commit its correct contiguous watermark
        under ``CONCURRENT_REQUESTS > 1`` (no skipped message or cross-topic
        state collision).

        Args:
            queue_name: Name of the queue.
            timeout: Seconds to wait (0 = non-blocking).

        Returns:
            ``(value_bytes, token)`` where ``token`` is a
            :class:`_KafkaAckToken`, or ``(None, None)`` when the queue is
            empty.

        Raises:
            QueueError: If the pop operation fails.
        """
        # Match disconnect()'s connection -> delivery order. _poll_record() can
        # acquire the reentrant connection lock while creating the first consumer.
        with self._consumer_io_lock:
            with self._connection_lock:
                with self._lease_generation(
                    "pop", queue_name=queue_name, allow_disconnected=True
                ) as generation:
                    with self._delivery_lock:
                        handles = generation.value if generation is not None else None
                        record = self._poll_record(queue_name, timeout, handles)
                        if record is None:
                            return (None, None)
                        self._next_delivery_attempt += 1
                        token = _KafkaAckToken(
                            partition=record.partition,
                            offset=record.offset,
                            topic=record.topic,
                            consumer_generation=self._consumer_generation,
                            assignment_epoch=self._assignment_epoch,
                            delivery_attempt=self._next_delivery_attempt,
                        )
                        topic_partition = (record.topic, record.partition)
                        # KafkaConsumer.position(tp) points at the NEXT record to fetch after a
                        # poll, so it cannot seed the lowest unprocessed offset. Capture the first
                        # record actually handed to the application instead; this is the commit
                        # watermark base for the current in-flight cohort on this topic-partition.
                        self._watermarks.setdefault(topic_partition, record.offset)
                        self._in_flight[topic_partition].add(record.offset)
                        attempt_key = self._attempt_key(token)
                        previous_attempt = self._active_attempts.get(attempt_key)
                        if previous_attempt is not None:
                            previous_partition_attempts = (
                                self._active_attempts_by_partition.get(topic_partition)
                            )
                            if previous_partition_attempts is not None:
                                previous_partition_attempts.discard(previous_attempt)
                        self._active_attempts[attempt_key] = token.delivery_attempt
                        self._active_attempts_by_partition.setdefault(
                            topic_partition, set()
                        ).add(token.delivery_attempt)
                        # Track the pop frontier so the watermark walk terminates at the highest
                        # popped offset (+1) on this topic-partition — never walks into
                        # not-yet-popped offsets and never runs away on an empty in-flight set.
                        self._high_water[topic_partition] = max(
                            self._high_water.get(topic_partition, 0),
                            record.offset + 1,
                        )
                        # Keep an older legacy slot armed. Its offset remains in the
                        # same watermark cohort, so acking this token cannot skip it.
                        if handles is None:
                            current = self._generation_gate.current
                            handles = current.value if current is not None else None
                        if self._last_record is not None and handles is not None:
                            handles.legacy_record = self._last_record
                        return (record.value, token)

    def _poll_record(
        self,
        queue_name: str,
        timeout: float,
        handles: _KafkaGenerationHandles | None = None,
    ) -> Any:
        """Poll a single record while preserving connection-before-delivery order."""
        with self._consumer_io_lock:
            with self._connection_lock:
                return self._poll_record_unlocked(queue_name, timeout, handles)

    def _poll_record_unlocked(
        self,
        queue_name: str,
        timeout: float,
        handles: _KafkaGenerationHandles | None = None,
    ) -> Any:
        """Poll a single record from ``queue_name``; return None if empty.

        Shared by :meth:`pop` and :meth:`pop_with_ack` so consumer creation,
        topic-subscription caching, and error wrapping live in one place.

        Args:
            queue_name: Name of the queue (validated here).
            timeout: Seconds to wait (0 = non-blocking).

        Returns:
            The polled kafka record, or None if no message was available.

        Raises:
            QueueError: If the poll fails at the Kafka layer.
            ValueError: If ``queue_name`` is invalid.
        """
        _validate_logical_queue_name(queue_name)
        created_consumer = False
        dynamic_lease_required = False
        consumer: Any = None
        try:
            snapshot_for_operation = (
                handles.snapshot
                if handles is not None and handles.snapshot is not None
                else self._connection_snapshot_or_capture()
            )
            topic_name = self._topic_name(queue_name, snapshot_for_operation)

            # Create consumer if not exists.  Compatibility callers may inject
            # ``_consumer`` after producer/admin publication; attach that exact
            # object to the accepting generation instead of silently constructing
            # a second client whose callbacks cannot be identity-checked.
            consumer = handles.consumer if handles is not None else self._consumer
            if consumer is None and handles is not None and self._consumer is not None:
                consumer = self._consumer
                handles.consumer = consumer
                handles.consumer_generation = self._consumer_generation
                handles.assignment_epoch = self._assignment_epoch
            if consumer is None:
                snapshot = (
                    handles.snapshot
                    if handles is not None and handles.snapshot is not None
                    else self._connection_snapshot_or_capture()
                )
                auto_offset_reset = snapshot.auto_offset_reset
                # A lazy consumer is a private candidate until construction has
                # returned. A constructor callback may synchronously disconnect;
                # do not publish or close that candidate from inside its callback.
                with self._lifecycle_lock:
                    consumer_epoch = self._lifecycle_epoch
                consumer = KafkaConsumer(
                    bootstrap_servers=snapshot.bootstrap_servers,
                    group_id=snapshot.group_id,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=False,
                    auto_commit_interval_ms=snapshot.auto_commit_interval_ms,
                    max_poll_records=snapshot.max_poll_records,
                    session_timeout_ms=snapshot.session_timeout_ms,
                    **self._build_client_security_config(snapshot),
                )
                created_consumer = consumer is not None
                with self._lifecycle_lock:
                    consumer_fenced = consumer_epoch != self._lifecycle_epoch
                if consumer_fenced:
                    raise QueueError(
                        _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                        queue_name=queue_name,
                        operation="pop",
                    )
                if consumer is not None:
                    self._consumer_generation += 1
                    self._consumer = consumer
                    self._consumer_auto_offset_reset = auto_offset_reset
                    if handles is not None:
                        if handles.retired:
                            raise QueueError(
                                _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                                queue_name=queue_name,
                                operation="pop",
                            )
                        handles.consumer = consumer
                        handles.consumer_generation = self._consumer_generation
                        handles.assignment_epoch = self._assignment_epoch
                    else:
                        # First-pop compatibility creates the consumer while the
                        # caller owns ``_connection_lock``. Publish only this
                        # non-empty graph; no empty placeholder may poison a later
                        # real connect(). The caller refreshes the handle before poll.
                        self._publish_generation_locked()
                        current = self._generation_gate.current
                        handles = current.value if current is not None else None
                        dynamic_lease_required = handles is not None

            if consumer is None:
                msg = "KafkaBackend not connected: consumer is None"
                raise BackendConnectionError(msg, backend_type="kafka")

            # If this call created and published the first consumer, acquire a
            # lease before any subscribe/poll callback can re-enter disconnect.
            dynamic_lease: Any = contextlib.nullcontext()
            if dynamic_lease_required:
                dynamic_lease = self._generation_gate.lease(
                    "pop", queue_name=queue_name
                )

            with dynamic_lease as admitted:
                if admitted is not None:
                    handles = admitted.value
                # Subscribe only when the topic changes. kafka-python's
                # subscribe() is idempotent on unchanged topics, but skipping the
                # redundant call avoids needless subscription-state work.
                if self._subscribed_topic != topic_name:
                    with self._delivery_lock:
                        self._advance_assignment_epoch_locked()
                        if handles is not None:
                            # The token's assignment epoch is generation-local;
                            # keep the retained record aligned with the live
                            # counter before admitting the new subscription.
                            handles.assignment_epoch = self._assignment_epoch
                        if handles is None:
                            # Preserve direct private-helper callers that inject a
                            # consumer without publishing a generation. Public
                            # pop paths always pass an identity-bound handle.
                            listener = self._rebalance_listener
                        else:
                            current = self._generation_gate.current
                            if current is None or handles is not current.value:
                                raise QueueError(
                                    _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                                    queue_name=queue_name,
                                    operation="pop",
                                )
                            listener = self._ensure_rebalance_listener_locked(
                                current, consumer
                            )
                        with self._consumer_io_lock:
                            consumer.subscribe([topic_name], listener=listener)
                    if handles is not None and not handles.retired:
                        handles.assignment_epoch = self._assignment_epoch
                    if handles is not None and handles.retired:
                        raise QueueError(
                            _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                            queue_name=queue_name,
                            operation="pop",
                        )
                    self._subscribed_topic = topic_name

                # Poll for messages
                timeout_ms = int(timeout * 1000)
                with self._consumer_io_lock:
                    messages = consumer.poll(timeout_ms=timeout_ms, max_records=1)
                if handles is not None and handles.retired:
                    raise QueueError(
                        _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                        queue_name=queue_name,
                        operation="pop",
                    )

                for records in messages.values():
                    for record in records:
                        return record
        except KafkaError as e:
            self._cleanup_partial_consumer(consumer, handles, created_consumer)
            msg = f"Failed to pop from queue {queue_name}: {e}"
            raise QueueError(
                msg,
                queue_name=queue_name,
                operation="pop",
            ) from e
        except BaseException:
            # A failed subscribe/poll is primary; clean only a consumer created by
            # this operation so a direct-injected or already-published client is
            # never closed behind its caller's back.
            self._cleanup_partial_consumer(consumer, handles, created_consumer)
            raise
        return None

    def _cleanup_partial_consumer(
        self,
        consumer: Any,
        handles: _KafkaGenerationHandles | None,
        created: bool,
    ) -> None:
        """Detach and close a consumer that failed before becoming usable."""
        if not created or consumer is None:
            return
        with self._delivery_lock:
            if self._consumer is consumer:
                self._consumer = None
                self._consumer_auto_offset_reset = None
                self._subscribed_topic = None
                self._consumer_generation += 1
                self._assignment_epoch += 1
                self._clear_delivery_state_locked()
            current = self._generation_gate.current
            generation_handles = handles
            if generation_handles is None and current is not None:
                generation_handles = current.value
            registered_retired = (
                generation_handles is not None
                and generation_handles.consumer is consumer
                and generation_handles.retired
            )
            if (
                generation_handles is not None
                and generation_handles.consumer is consumer
                and not generation_handles.retired
            ):
                generation_handles.consumer = None
            if (
                current is not None
                and current.value.producer is None
                and current.value.admin_client is None
                and current.value.consumer is None
            ):
                self._generation_gate.retire()
        # A retired generation's finalizer owns a registered consumer. Closing it
        # here as well would make a callback-triggered teardown close the same SDK
        # handle twice. An unregistered constructor candidate remains ours.
        if registered_retired:
            return
        with contextlib.suppress(BaseException):
            consumer.close()

    def _register_legacy_record_locked(
        self, record: Any, handles: _KafkaGenerationHandles
    ) -> None:
        """Track a legacy pop in the same watermark cohort as token pops.

        Older direct-injection callers often provide only ``record.value``.  That
        is sufficient for the historical pop contract, but it cannot safely enter
        offset bookkeeping; retain only the legacy slot in that case rather than
        letting a mock or malformed driver object poison the watermark maps.
        """
        self._last_record = record
        handles.legacy_record = record
        topic = getattr(record, "topic", None)
        partition = getattr(record, "partition", None)
        offset = getattr(record, "offset", None)
        if (
            type(topic) is not str
            or type(partition) is not int
            or partition < 0
            or type(offset) is not int
            or offset < 0
        ):
            return
        in_flight = (
            handles.in_flight if handles.in_flight is not None else self._in_flight
        )
        watermarks = (
            handles.watermarks if handles.watermarks is not None else self._watermarks
        )
        high_water = (
            handles.high_water if handles.high_water is not None else self._high_water
        )
        topic_partition = (topic, partition)
        # A legacy delivery is an unacknowledged offset too. Keeping it in the
        # cohort prevents a later token acknowledgement from committing past it.
        watermarks.setdefault(topic_partition, offset)
        in_flight.setdefault(topic_partition, set()).add(offset)
        high_water[topic_partition] = max(
            high_water.get(topic_partition, 0), offset + 1
        )

    @staticmethod
    def _generation_is_current(
        generation: GenerationRecord[_KafkaGenerationHandles],
    ) -> bool:
        """Return whether an admitted Kafka generation still accepts results."""
        return generation.accepting and not generation.value.retired

    @staticmethod
    def _active_token_for_partition(
        active_attempts_by_partition: dict[tuple[str, int], set[int]] | None,
        topic_partition: tuple[str, int],
    ) -> bool:
        """Return whether an exact token delivery remains unsettled in O(1)."""
        return bool(
            active_attempts_by_partition
            and active_attempts_by_partition.get(topic_partition)
        )

    def _ack_unleased(
        self,
        queue_name: str,
        *,
        token: Any | None = None,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        """Ack a popped message while serializing consumer SDK access."""
        with self._consumer_io_lock:
            self._ack_unleased_unlocked(queue_name, token=token, handles=handles)

    def _ack_unleased_unlocked(
        self,
        queue_name: str,
        *,
        token: Any | None = None,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        """Ack a popped message.

        With a ``token`` (the path the scheduler uses under
        ``CONCURRENT_REQUESTS > 1``): mark the token's (topic, partition, offset)
        completed and **commit the contiguous low-watermark** for that
        topic-partition — the largest offset such that every record from the
        last-committed offset up to it is completed. No unprocessed record is
        ever skipped.

        Without a ``token`` (legacy single-pop caller): commit an explicit
        offset map for the tracked ``_last_record``'s topic-partition only
        (``offset + 1``), never a bare ``commit()`` — that would sweep the
        fetch position of every assigned partition, advancing committed
        offsets past concurrently in-flight token records. Refused with a
        :class:`QueueError` when the record's topic-partition still has
        un-acked ``pop_with_ack`` records in flight. Only correct for
        ``CONCURRENT_REQUESTS=1`` — kept for backward compatibility with
        external callers that pop() then ack() without threading the token
        through.

        Args:
            queue_name: Name of the queue (unused for the commit; kept for
                interface symmetry).
            token: A :class:`_KafkaAckToken` from :meth:`pop_with_ack`, or
                ``None`` to ack the last-popped record.

        Raises:
            QueueError: If the underlying commit fails, or the legacy path is
                attempted while the record's topic-partition has un-acked
                in-flight token offsets.
        """
        del queue_name
        if token is not None:
            if not isinstance(token, _KafkaAckToken):
                return
            admitted_handles = handles or getattr(self._delivery_lease, "handles", None)
            self._ack_token(token, admitted_handles)
            return
        with self._delivery_lock:
            # Legacy path: capture the exact generation and state, then release
            # delivery bookkeeping before calling kafka-python.
            legacy_record = (
                handles.legacy_record
                if handles is not None and handles.legacy_record is not None
                else self._last_record
            )
            consumer = handles.consumer if handles is not None else self._consumer
            if consumer is None or legacy_record is None:
                return
            record = legacy_record
            topic_partition = (record.topic, record.partition)
            active_attempts_by_partition = (
                handles.active_attempts_by_partition
                if handles is not None
                else self._active_attempts_by_partition
            )
            if self._active_token_for_partition(
                active_attempts_by_partition, topic_partition
            ):
                raise QueueError(
                    _KAFKA_MIXED_MODE_ACK_REFUSED_MESSAGE,
                    operation="ack",
                )
            in_flight_map = (
                handles.in_flight if handles is not None else self._in_flight
            )
            pending = in_flight_map.get(topic_partition) if in_flight_map else None
            if pending is not None and pending != {record.offset}:
                raise QueueError(
                    _KAFKA_MIXED_MODE_ACK_REFUSED_MESSAGE,
                    operation="ack",
                )
            legacy_epoch = (
                handles.assignment_epoch
                if handles is not None
                else self._assignment_epoch
            )

        try:
            with self._consumer_io_lock:
                consumer.commit(
                    {
                        TopicPartition(
                            record.topic, record.partition
                        ): OffsetAndMetadata(record.offset + 1, "")
                    }
                )
        except KafkaError as e:
            msg = f"Failed to ack Kafka message: {e}"
            raise QueueError(msg, operation="ack") from e

        with self._delivery_lock:
            same_state = (
                handles is not None
                and handles.in_flight is in_flight_map
                and handles.assignment_epoch == legacy_epoch
            ) or (
                handles is None
                and self._in_flight is in_flight_map
                and self._assignment_epoch == legacy_epoch
            )
            if same_state:
                if self._last_record is record:
                    self._last_record = None
                if handles is not None and handles.legacy_record is record:
                    handles.legacy_record = None
                if pending is not None and in_flight_map is not None:
                    pending.discard(record.offset)
                    if not pending:
                        in_flight_map.pop(topic_partition, None)
                        if handles is not None:
                            if handles.watermarks is not None:
                                handles.watermarks.pop(topic_partition, None)
                            if handles.high_water is not None:
                                handles.high_water.pop(topic_partition, None)
                        else:
                            self._watermarks.pop(topic_partition, None)
                            self._high_water.pop(topic_partition, None)
            if handles is not None and (handles.retired or not same_state):
                raise QueueError(
                    _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                    operation="ack",
                )

    @queue_operation_error_boundary(
        "ack",
        "Failed to ack Kafka message.",
        safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
    )
    def ack(self, queue_name: str, *, token: Any | None = None) -> None:
        """Ack a Kafka delivery while retaining its generation lease."""
        with self._lease_generation("ack", allow_disconnected=True) as generation:
            handles = generation.value if generation is not None else None
            previous_handles = getattr(self._delivery_lease, "handles", None)
            self._delivery_lease.handles = handles
            try:
                if token is None:
                    self._ack_unleased(queue_name, token=None, handles=handles)
                else:
                    # Keep the historical private helper call shape for token
                    # callers; the thread-local carries the admitted record.
                    self._ack_unleased(queue_name, token=token)
            finally:
                if previous_handles is None:
                    del self._delivery_lease.handles
                else:
                    self._delivery_lease.handles = previous_handles

    def _ack_token(
        self,
        token: _KafkaAckToken,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        """Record ``token`` completed and commit its topic-partition's watermark.

        The watermark is the largest ``offset + 1`` such that every record
        from the seeded base up to it is completed (removed from the in-flight
        set). Committing it advances the committed cursor past a contiguous
        run of processed records, leaving any unprocessed record's offset
        uncommitted (so it re-delivers on consumer restart — at-least-once).

        The Kafka SDK call is serialized separately from local delivery state.
        A driver callback may synchronously call ``disconnect()``; retaining
        ``_delivery_lock`` across that callback would invert the teardown lock
        order and can deadlock with another teardown.
        """
        self._ack_token_locked(token, handles)

    def _ack_token_locked(
        self,
        token: _KafkaAckToken,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        """Ack one token with a stable consumer-I/O transaction."""
        with self._consumer_io_lock:
            self._ack_token_locked_unlocked(token, handles)

    def _ack_token_locked_unlocked(
        self,
        token: _KafkaAckToken,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        """Implement exact-attempt acknowledgement without locking across SDK I/O."""
        if handles is not None:
            # A lease owns these exact maps after retirement; never dereference
            # the backend's replacement mirrors while the commit is in flight.
            consumer = handles.consumer
            in_flight_map = handles.in_flight
            watermarks_map = handles.watermarks
            high_water_map = handles.high_water
        else:
            consumer = self._consumer
            in_flight_map = self._in_flight
            watermarks_map = self._watermarks
            high_water_map = self._high_water
        with self._delivery_lock:
            if consumer is None or not self._token_is_active_locked(token, handles):
                return
            if (
                in_flight_map is None
                or watermarks_map is None
                or high_water_map is None
            ):
                return
            active_attempts_map = (
                handles.active_attempts
                if handles is not None
                else self._active_attempts
            )
            compatibility_token = token.delivery_attempt == 0 and (
                self._attempt_key(token) not in (active_attempts_map or {})
            )
            partition = token.partition
            topic_partition = (token.topic, partition)
            in_flight = in_flight_map.get(topic_partition)
            if in_flight is None or token.offset not in in_flight:
                # The offset left the in-flight set in an earlier pass of this
                # exact attempt (the removal precedes the broker commit) and no
                # restore path exists once control returns here. If that pass
                # was interrupted between a successful commit and the
                # post-commit bookkeeping, the attempt entry below can never
                # be settled through ack again — every retry takes this same
                # early return. Finish it now against the same generation-owned
                # maps so the entry does not leak and legacy bare acks on this
                # topic-partition are not refused until a rebalance. The settle
                # only pops an entry whose attempt identity matches this token,
                # so a replacement generation's mappings are never touched.
                self._finish_attempt_locked(token, handles)
                if not in_flight:
                    in_flight_map.pop(topic_partition, None)
                    watermarks_map.pop(topic_partition, None)
                    high_water_map.pop(topic_partition, None)
                return
            in_flight.remove(token.offset)
            # pop_with_ack seeds this from the first delivered record. The fallback is
            # defensive for callers/tests that construct internal state directly.
            watermarks_map.setdefault(topic_partition, token.offset)
            # Advance the watermark past the contiguous completed run. Each step is
            # O(1) set membership; the walk is bounded by _high_water (the pop
            # frontier) so it never walks into not-yet-popped offsets and never
            # runs away on an empty in-flight set.
            base = watermarks_map[topic_partition]
            high = high_water_map.get(topic_partition, base)
            watermark = base
            while watermark < high and watermark not in in_flight:
                watermark += 1

        # Commit only if the watermark advanced past the base. The state lock is
        # deliberately released before entering kafka-python: a driver callback
        # may call disconnect(), and disconnect must be able to acquire the
        # connection lock without waiting behind this delivery critical section.
        if watermark <= base:
            with self._delivery_lock:
                self._finish_attempt_locked(token, handles)
                if not in_flight:
                    in_flight_map.pop(topic_partition, None)
                    watermarks_map.pop(topic_partition, None)
                    high_water_map.pop(topic_partition, None)
            return

        try:
            with self._consumer_io_lock:
                consumer.commit(
                    {
                        TopicPartition(token.topic, partition): OffsetAndMetadata(
                            watermark, ""
                        )
                    }
                )
        except KafkaError as e:
            # Restore only the exact generation/epoch that still owns this
            # attempt. A rebalance may have cleared these maps while the broker
            # call was in flight; writing into that old map would resurrect state
            # in a replacement assignment.
            with self._delivery_lock:
                generation_matches = token.consumer_generation == (
                    handles.consumer_generation
                    if handles is not None
                    else self._consumer_generation
                ) and token.assignment_epoch == (
                    handles.assignment_epoch
                    if handles is not None
                    else self._assignment_epoch
                )
                if (
                    self._token_is_active_locked(token, handles)
                    or (compatibility_token and generation_matches)
                ) and token.offset not in in_flight:
                    in_flight.add(token.offset)
            msg = f"Failed to ack Kafka message: {e}"
            raise QueueError(msg, operation="ack") from e

        with self._delivery_lock:
            generation_matches = token.consumer_generation == (
                handles.consumer_generation
                if handles is not None
                else self._consumer_generation
            ) and token.assignment_epoch == (
                handles.assignment_epoch
                if handles is not None
                else self._assignment_epoch
            )
            still_active = self._token_is_active_locked(token, handles) or (
                compatibility_token and generation_matches
            )
            if still_active:
                watermarks_map[topic_partition] = watermark
                self._finish_attempt_locked(token, handles)
            retired = handles is not None and handles.retired
            assignment_changed = (
                handles is not None
                and handles.assignment_epoch != token.assignment_epoch
            )
            if still_active and not in_flight:
                # The maps are generation-owned references. Prune them only
                # after the exact attempt has been settled; never prune a
                # replacement generation's maps after a stale callback.
                in_flight_map.pop(topic_partition, None)
                watermarks_map.pop(topic_partition, None)
                high_water_map.pop(topic_partition, None)
            if retired or assignment_changed:
                raise QueueError(
                    _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                    operation="ack",
                )

    def _nack_unleased(
        self,
        queue_name: str,
        *,
        token: Any | None = None,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        """Nack one delivery while serializing consumer SDK access."""
        with self._consumer_io_lock:
            self._nack_unleased_unlocked(queue_name, token=token, handles=handles)

    def _nack_unleased_unlocked(
        self,
        queue_name: str,
        *,
        token: Any | None = None,
        handles: _KafkaGenerationHandles | None = None,
    ) -> None:
        """Nack a popped message without committing its offset.

        For a current, still-in-flight token, seek an assigned partition back to
        the failed offset so it can be delivered again in this consumer session.
        If a rebalance has revoked the partition, leave the offset uncommitted;
        Kafka then redelivers it after assignment/reconnect. Unknown, completed,
        or stale-generation tokens are idempotent no-ops.

        Without a token, apply the same best-effort seek to the legacy last record.

        Args:
            queue_name: Name of the queue (unused; interface symmetry).
            token: A :class:`_KafkaAckToken` from :meth:`pop_with_ack`, or
                ``None`` for the legacy last-record path.
        """
        del queue_name
        admitted_handles = handles or getattr(self._delivery_lease, "handles", None)
        if token is not None:
            if not isinstance(token, _KafkaAckToken):
                return
            with self._delivery_lock:
                consumer = (
                    admitted_handles.consumer
                    if admitted_handles is not None
                    else self._consumer
                )
                if consumer is None or not self._token_is_active_locked(
                    token, admitted_handles
                ):
                    return
                topic_partition_obj = TopicPartition(token.topic, token.partition)

            try:
                with self._consumer_io_lock:
                    assignment = consumer.assignment()
                    with self._delivery_lock:
                        if admitted_handles is not None and admitted_handles.retired:
                            raise QueueError(
                                _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                                operation="nack",
                            )
                        if not self._token_is_active_locked(token, admitted_handles):
                            return
                        should_seek = topic_partition_obj in assignment
                    if should_seek:
                        consumer.seek(topic_partition_obj, token.offset)
            except KafkaError as e:
                msg = f"Failed to nack Kafka message: {e}"
                raise QueueError(msg, operation="nack") from e

            with self._delivery_lock:
                if admitted_handles is not None and admitted_handles.retired:
                    raise QueueError(
                        _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                        operation="nack",
                    )
                if not self._token_is_active_locked(token, admitted_handles):
                    return
                self._finish_attempt_locked(token, admitted_handles)
            return

        with self._delivery_lock:
            record = (
                admitted_handles.legacy_record
                if admitted_handles is not None
                and admitted_handles.legacy_record is not None
                else self._last_record
            )
            consumer = (
                admitted_handles.consumer
                if admitted_handles is not None
                else self._consumer
            )
            if consumer is None or record is None:
                return
            topic_partition = (record.topic, record.partition)
            topic_partition_obj = TopicPartition(record.topic, record.partition)
            legacy_epoch = (
                admitted_handles.assignment_epoch
                if admitted_handles is not None
                else self._assignment_epoch
            )
            in_flight_map = (
                admitted_handles.in_flight
                if admitted_handles is not None
                else self._in_flight
            )
            pending = in_flight_map.get(topic_partition) if in_flight_map else None

        try:
            with self._consumer_io_lock:
                assignment = consumer.assignment()
                with self._delivery_lock:
                    same_state = (
                        admitted_handles is not None
                        and admitted_handles.in_flight is in_flight_map
                        and admitted_handles.assignment_epoch == legacy_epoch
                        and admitted_handles.legacy_record is record
                    ) or (
                        admitted_handles is None
                        and self._in_flight is in_flight_map
                        and self._assignment_epoch == legacy_epoch
                        and self._last_record is record
                    )
                    if admitted_handles is not None and admitted_handles.retired:
                        raise QueueError(
                            _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                            operation="nack",
                        )
                    if not same_state:
                        return
                    should_seek = topic_partition_obj in assignment
                if should_seek:
                    consumer.seek(topic_partition_obj, record.offset)
        except KafkaError as e:
            msg = f"Failed to nack Kafka message: {e}"
            raise QueueError(msg, operation="nack") from e

        with self._delivery_lock:
            same_state = (
                admitted_handles is not None
                and admitted_handles.in_flight is in_flight_map
                and admitted_handles.assignment_epoch == legacy_epoch
                and admitted_handles.legacy_record is record
            ) or (
                admitted_handles is None
                and self._in_flight is in_flight_map
                and self._assignment_epoch == legacy_epoch
                and self._last_record is record
            )
            if admitted_handles is not None and admitted_handles.retired:
                raise QueueError(
                    _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                    operation="nack",
                )
            if not same_state:
                return
            if self._last_record is record:
                self._last_record = None
            if (
                admitted_handles is not None
                and admitted_handles.legacy_record is record
            ):
                admitted_handles.legacy_record = None
            # A legacy nack has no token to settle later. Remove only its own
            # offset; exact-token attempts on the same partition remain intact.
            if pending is not None and in_flight_map is not None:
                active_attempts = (
                    admitted_handles.active_attempts_by_partition
                    if admitted_handles is not None
                    else self._active_attempts_by_partition
                )
                if not (active_attempts and active_attempts.get(topic_partition)):
                    pending.discard(record.offset)
                    if not pending:
                        in_flight_map.pop(topic_partition, None)
                        if admitted_handles is not None:
                            if admitted_handles.watermarks is not None:
                                admitted_handles.watermarks.pop(topic_partition, None)
                            if admitted_handles.high_water is not None:
                                admitted_handles.high_water.pop(topic_partition, None)
                        else:
                            self._watermarks.pop(topic_partition, None)
                            self._high_water.pop(topic_partition, None)

    @queue_operation_error_boundary(
        "nack",
        "Failed to nack Kafka message.",
        safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
    )
    def nack(self, queue_name: str, *, token: Any | None = None) -> None:
        """Nack a Kafka delivery while retaining its generation lease."""
        if token is not None and not isinstance(token, _KafkaAckToken):
            return
        with self._lease_generation("nack", allow_disconnected=True) as generation:
            handles = generation.value if generation is not None else None
            previous_handles = getattr(self._delivery_lease, "handles", None)
            self._delivery_lease.handles = handles
            try:
                if token is None:
                    self._nack_unleased(queue_name, token=None, handles=handles)
                else:
                    # Token settlement resolves its admitted consumer internally;
                    # retain the historical private helper call shape for injection.
                    self._nack_unleased(queue_name, token=token)
            finally:
                if previous_handles is None:
                    del self._delivery_lease.handles
                else:
                    self._delivery_lease.handles = previous_handles

    def _queue_len_unleased(
        self,
        queue_name: str,
        snapshot: _KafkaConnectionSnapshot | None = None,
        handles: _KafkaGenerationHandles | None = None,
    ) -> int:
        """Inspect queue depth while serializing consumer SDK access."""
        with self._consumer_io_lock:
            return self._queue_len_unlocked(queue_name, snapshot, handles)

    def _queue_len_unlocked(
        self,
        queue_name: str,
        snapshot: _KafkaConnectionSnapshot | None = None,
        handles: _KafkaGenerationHandles | None = None,
    ) -> int:
        """Get queue length.

        Args:
            queue_name: Name of the queue.

        Returns:
            Approximate number of items in the queue.

        Raises:
            ValueError: If queue_name contains invalid characters.
            QueueError: If the depth query fails (broker outage, leader
                re-election, coordinator error).

        Note:
            This is a conservative consumer-group lag snapshot. It includes
            fetched but uncommitted records and is safe for pending-work checks,
            though concurrent broker activity can change immediately afterward.
        """
        _validate_logical_queue_name(queue_name)
        snapshot = snapshot or self._connection_snapshot_or_capture()
        topic_name = self._topic_name(queue_name, snapshot)
        # Capture the exact consumer under the short state lock, then serialize
        # all kafka-python calls with the consumer-I/O lock.  Do not hold
        # ``_delivery_lock`` across SDK code: a callback may re-enter
        # disconnect() while another teardown is retiring this generation.
        with self._delivery_lock:
            consumer = handles.consumer if handles is not None else self._consumer
        if consumer is not None:
            try:
                with self._consumer_io_lock:
                    assignment = consumer.assignment()
                    topic_assignment = {
                        tp for tp in assignment if tp.topic == topic_name
                    }
                    if topic_assignment:
                        # The operation owns this generation's immutable policy;
                        # the live compatibility mirror may already describe a
                        # replacement after a reentrant teardown callback.
                        auto_offset_reset = snapshot.auto_offset_reset
                        total = self._consumer_group_lag(
                            consumer,
                            topic_assignment,
                            queue_name=queue_name,
                            auto_offset_reset=auto_offset_reset,
                        )
                        if handles is not None and handles.retired:
                            raise QueueError(
                                _KAFKA_CONNECTION_CHANGED_OPERATION_MESSAGE,
                                queue_name=queue_name,
                                operation="queue_len",
                            )
                        return total
            except KafkaError as e:
                msg = f"Failed to get Kafka queue length for {queue_name}: {e}"
                raise QueueError(
                    msg,
                    queue_name=queue_name,
                    operation="queue_len",
                ) from e

        temp_consumer: KafkaConsumer | None = None
        try:
            try:
                auto_offset_reset = snapshot.auto_offset_reset
                temp_consumer = KafkaConsumer(
                    bootstrap_servers=snapshot.bootstrap_servers,
                    group_id=snapshot.group_id,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=False,
                    **self._build_client_security_config(snapshot),
                )
                partitions = temp_consumer.partitions_for_topic(topic_name)
                if not partitions:
                    total = 0
                else:
                    assignment = {
                        TopicPartition(topic_name, partition)
                        for partition in partitions
                    }
                    temp_consumer.assign(list(assignment))
                    total = self._consumer_group_lag(
                        temp_consumer,
                        assignment,
                        queue_name=queue_name,
                        auto_offset_reset=auto_offset_reset,
                    )
            except KafkaError as e:
                msg = f"Failed to get Kafka queue length for {queue_name}: {e}"
                raise QueueError(
                    msg,
                    queue_name=queue_name,
                    operation="queue_len",
                ) from e
        except BaseException:
            # A failed depth query is the primary outcome.  Closing its private
            # consumer is leak prevention only, so a second control exception must
            # not replace the causal QueueError (or another active primary failure).
            if temp_consumer is not None:
                with contextlib.suppress(BaseException):
                    temp_consumer.close()
            raise
        else:
            if temp_consumer is not None:
                # On a successful probe there is no primary failure to preserve:
                # retain the established contract that process-control close failures
                # propagate, while ordinary SDK close failures remain best-effort.
                with contextlib.suppress(Exception):
                    temp_consumer.close()
            return total

    @queue_operation_error_boundary(
        "queue_len",
        "Failed to inspect Kafka queue.",
        safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def queue_len(self, queue_name: str) -> int:
        """Return depth from one generation-scoped Kafka consumer probe."""
        with self._lease_generation("queue_len", allow_disconnected=True) as generation:
            handles = generation.value if generation is not None else None
            snapshot = handles.snapshot if handles is not None else None
            return self._queue_len_unleased(queue_name, snapshot, handles)

    def _consumer_group_lag(
        self,
        consumer: Any,
        assignment: set[TopicPartition],
        *,
        queue_name: str,
        auto_offset_reset: str,
    ) -> int:
        """Return conservative group lag for one topic-partition assignment."""

        def offset(
            values: Mapping[TopicPartition, Any],
            topic_partition: TopicPartition,
            label: str,
        ) -> int:
            value = values.get(topic_partition)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise QueueError(
                    f"Kafka returned an invalid {label} offset.",
                    queue_name=queue_name,
                    operation="queue_len",
                )
            return value

        end_offsets = consumer.end_offsets(assignment)
        beginning_offsets = consumer.beginning_offsets(assignment)
        if not isinstance(end_offsets, Mapping) or not isinstance(
            beginning_offsets, Mapping
        ):
            raise QueueError(
                "Kafka returned invalid offset metadata.",
                queue_name=queue_name,
                operation="queue_len",
            )

        total = 0
        for topic_partition in assignment:
            end = offset(end_offsets, topic_partition, "end")
            beginning = offset(beginning_offsets, topic_partition, "beginning")
            committed = consumer.committed(topic_partition)
            if committed is None:
                if auto_offset_reset == "earliest":
                    start = beginning
                elif auto_offset_reset == "latest":
                    start = end
                else:
                    raise QueueError(
                        "Kafka consumer group has no committed offset and "
                        "auto_offset_reset='none'.",
                        queue_name=queue_name,
                        operation="queue_len",
                    )
            elif (
                isinstance(committed, bool)
                or not isinstance(committed, int)
                or committed < 0
            ):
                raise QueueError(
                    "Kafka returned an invalid committed offset.",
                    queue_name=queue_name,
                    operation="queue_len",
                )
            else:
                # Retention may have advanced the log start beyond an old committed
                # offset. Do not count records that no longer exist.
                start = max(beginning, committed)
            total += max(0, end - start)
        return total

    @queue_operation_error_boundary(
        "clear_queue",
        "Failed to clear Kafka queue.",
        safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def clear_queue(self, queue_name: str) -> None:
        """Reject Kafka clear because delete/recreate is not linearizable.

        Args:
            queue_name: Name of the queue.

        Raises:
            ValueError: If queue_name contains invalid characters.
            QueueError: Always, after validating ``queue_name``. Parity with
                pulsar/rocketmq: a caller's ``except QueueError`` arm for the
                unsupported-clear contract catches Kafka's rejection too.
        """
        _validate_logical_queue_name(queue_name)
        # The fixed capability message must remain stable at the public redaction
        # boundary; do not interpolate the caller-controlled logical name into it.
        raise QueueError(
            _KAFKA_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
            operation="clear_queue",
        )
