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
from collections.abc import Mapping
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

from scrapy_extension.backends._redaction import _RedactedStr
from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.exceptions._redaction import (
  backend_connection_error_boundary,
  configuration_error_boundary,
  queue_operation_error_boundary,
)
from scrapy_extension.settings import KafkaMode, KafkaSettings
from scrapy_extension.settings._broker_endpoints import (
  KAFKA_BROKER_ENDPOINTS_ERROR,
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
    if not name or not TOPIC_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid topic/queue name: {name!r}. "
            "Only alphanumeric, dots, underscores, and hyphens allowed."
        )


def _validate_queue_name_argument(
  _backend: object,
  queue_name: str,
  *_args: Any,
  **_kwargs: Any,
) -> None:
  """Validate a public queue argument before its terminal error boundary."""
  _validate_topic_name(queue_name)


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
  {
    f"Failed to connect to Kafka ({mode.value})."
    for mode in KafkaMode
  }
)
_KAFKA_CLEAR_QUEUE_UNSUPPORTED_MESSAGE = (
  "Kafka clear_queue is unsupported: asynchronous topic delete/recreate "
  "cannot preserve active consumer-group offsets or protect messages "
  "accepted after clear returns. Stop and drain the queue with an "
  "operator-controlled Kafka maintenance workflow instead."
)
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
    "Kafka consumer group has no committed offset and "
    "auto_offset_reset='none'.",
    "Kafka returned an invalid committed offset.",
    _KAFKA_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
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
      f"_KafkaAckToken(topic={self.topic!r}, partition={self.partition}, "
      f"offset={self.offset}, consumer_generation={self.consumer_generation}, "
      f"assignment_epoch={self.assignment_epoch}, "
      f"delivery_attempt={self.delivery_attempt})"
    )


class _KafkaRebalanceListener(
  ConsumerRebalanceListener  # type: ignore[misc]
):
  """Fence delivery tokens whenever Kafka changes partition ownership."""

  __slots__ = ("_backend",)

  def __init__(self, backend: KafkaBackend) -> None:
    self._backend = backend

  def on_partitions_revoked(self, revoked: Any) -> None:
    self._backend._on_assignment_changed(revoked)

  def on_partitions_assigned(self, assigned: Any) -> None:
    self._backend._on_assignment_changed(assigned)


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
    self._assignment_epoch = 0
    self._next_delivery_attempt = 0
    self._active_attempts: dict[tuple[str, int, int], int] = {}
    self._rebalance_listener = _KafkaRebalanceListener(self)
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

  def _advance_assignment_epoch_locked(self) -> None:
    """Fence every token from the prior subscription/assignment epoch."""
    self._assignment_epoch += 1
    self._clear_delivery_state_locked()

  def _on_assignment_changed(self, partitions: Any) -> None:
    """Rebalance-listener callback; duplicates are safer than stale commits."""
    del partitions
    with self._delivery_lock:
      self._advance_assignment_epoch_locked()

  @staticmethod
  def _attempt_key(token: _KafkaAckToken) -> tuple[str, int, int]:
    return (token.topic, token.partition, token.offset)

  def _token_is_active_locked(self, token: _KafkaAckToken) -> bool:
    """Return whether ``token`` still owns its exact delivery attempt."""
    if (
      token.consumer_generation != self._consumer_generation
      or token.assignment_epoch != self._assignment_epoch
    ):
      return False
    attempt = self._active_attempts.get(self._attempt_key(token))
    if attempt is not None:
      return attempt == token.delivery_attempt
    # Compatibility for direct construction of this private token in older
    # callers/tests. Real tokens emitted by pop_with_ack always have a
    # positive unique attempt and therefore never take this branch.
    topic_partition = (token.topic, token.partition)
    in_flight = self._in_flight.get(topic_partition)
    return (
      token.delivery_attempt == 0
      and in_flight is not None
      and token.offset in in_flight
    )

  def _finish_attempt_locked(self, token: _KafkaAckToken) -> None:
    key = self._attempt_key(token)
    if self._active_attempts.get(key) == token.delivery_attempt:
      self._active_attempts.pop(key, None)

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
        return

      # A prior interrupted attempt can leave exactly one handle assigned.
      # Detach it before beginning a fresh generation; otherwise a successful
      # retry would overwrite and leak that residual client.
      if self._producer is not None or self._admin_client is not None:
        self._abort_partial_connect(suppress_process_control=True)

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
      startup_error: BackendConnectionError | None = None
      try:
        if snapshot.mode == KafkaMode.STANDALONE:
          self._connect_standalone(snapshot)
        elif snapshot.mode == KafkaMode.CLUSTER:
          self._connect_cluster(snapshot)
        else:
          self._connect_confluent(snapshot)
        self._connection_snapshot = snapshot
        self._log_success_diagnostic(
          "Connected to Kafka in %s mode", snapshot.mode.value
        )
      except KafkaError:
        self._abort_partial_connect(suppress_process_control=True)
        startup_error = BackendConnectionError(
          f"Failed to connect to Kafka ({snapshot.mode.value}).",
          backend_type="kafka",
        )
      except Exception:
        self._abort_partial_connect(suppress_process_control=True)
        # Unexpected driver/plugin errors are not safe public diagnostics.
        startup_error = BackendConnectionError(
          f"Failed to connect to Kafka ({snapshot.mode.value}).",
          backend_type="kafka",
        )
      except BaseException:
        # KeyboardInterrupt/SystemExit are not ``Exception`` subclasses, so the
        # arms above cannot catch them — without this arm a Ctrl+C raised in the
        # window between ``self._producer = ...`` and ``self._admin_client = ...``
        # skips ``_abort_partial_connect()``, leaking the producer (TCP socket +
        # bg thread) and leaving ``is_connected()`` lying True. Run the cleanup
        # before re-raising. Mirrors mongodb.py / elasticsearch.py / dynamodb /
        # redis ``except BaseException`` arms.
        self._abort_partial_connect(suppress_process_control=True)
        raise

      if startup_error is not None:
        # Raise outside the driver exception handler so endpoint/credential
        # text cannot survive through ``__cause__`` or ``__context__``.
        raise startup_error

  def _abort_partial_connect(self, *, suppress_process_control: bool = False) -> None:
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
    generation from being retired before a retry.
    """
    producer = self._producer
    admin = self._admin_client
    self._producer = None
    self._admin_client = None
    self._connection_snapshot = None
    primary_error: BaseException | None = None
    for closer in (producer, admin):
      if closer is not None:
        try:
          closer.close()
        except Exception:
          # A logging handler is application code and may itself raise a
          # process-control exception.  This abort path is used while a
          # connect failure is already in flight, so diagnostics must not
          # interrupt teardown of the remaining detached sibling or replace
          # that causal failure.
          try:
            logger.debug("Failed to abort partial Kafka client")
          except BaseException:
            pass
        except BaseException as error:
          if primary_error is None:
            primary_error = error
    if primary_error is not None and not suppress_process_control:
      raise primary_error

  @configuration_error_boundary(
    "Kafka configuration is invalid.",
    _KAFKA_CONFIGURATION_SETTING_NAMES,
    preserve_static_message=True,
    safe_messages=_KAFKA_SAFE_CONFIGURATION_MESSAGES,
  )
  def _capture_connection_snapshot(self) -> _KafkaConnectionSnapshot:
    """Copy and revalidate every setting consumed by one client generation."""
    raw_values = self.config.__dict__.copy()
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
      sasl_password=(
        _RedactedStr(password) if password is not None else None
      ),
      confluent_api_key=(
        _RedactedStr(api_key) if api_key is not None else None
      ),
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

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
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

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
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

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
    self._log_success_diagnostic("Connected to Confluent Cloud")

  def disconnect(self) -> None:
    """Close Kafka connection."""
    # Hold the generation lock through client close so a new connect cannot
    # publish its clients while callbacks from an old consumer are still being
    # torn down. Delivery state is detached under its own short-lived lock;
    # potentially blocking close() calls deliberately happen after releasing it.
    with self._connection_lock:
      with self._delivery_lock:
        producer = self._producer
        consumer = self._consumer
        admin_client = self._admin_client

        # Invalidate state before closing handles. A close failure cannot leave a
        # half-connected backend, and a late completion cannot be redirected to a
        # later consumer generation.
        self._producer = None
        self._consumer = None
        self._consumer_auto_offset_reset = None
        self._admin_client = None
        self._connection_snapshot = None
        self._consumer_generation += 1
        self._assignment_epoch += 1
        self._subscribed_topic = None
        self._clear_delivery_state_locked()
        self._known_topics.clear()
        self._known_topic_policies.clear()

      self._close_detached_clients(producer, consumer, admin_client)

  @staticmethod
  def _close_detached_clients(*clients: Any) -> None:
    """Close every detached client, retaining the first control exception."""
    primary_error: BaseException | None = None
    for client in clients:
      if client is not None:
        try:
          client.close()
        except Exception:
          logger.debug("Ignoring Kafka client-close failure")
        except BaseException as error:
          if primary_error is None:
            primary_error = error
    if primary_error is not None:
      raise primary_error

  def is_connected(self) -> bool:
    """Check if Kafka is connected.

    Returns:
        True if producer is available.
    """
    return self._producer is not None

  def ping(self) -> bool:
    """Check Kafka health.

    Returns:
        True if Kafka brokers are reachable.
    """
    try:
      if self._admin_client:
        self._admin_client.list_topics()
        return True
    except Exception:
      return False
    else:
      return False

  @property
  def backend_type(self) -> BackendType:
    """Return backend type.

    Returns:
        BackendType.KAFKA
    """
    return BackendType.KAFKA

  def _ensure_topic_exists(
    self,
    queue_name: str,
    snapshot: _KafkaConnectionSnapshot | None = None,
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
    _validate_topic_name(queue_name)
    topic_name = f"scrapy-{queue_name}"
    snapshot = snapshot or self._connection_snapshot_or_capture()
    partitions = snapshot.num_partitions
    replicas = snapshot.replication_factor
    retention = snapshot.retention_ms
    min_isr = snapshot.min_insync_replicas
    policy = (partitions, replicas, retention, min_isr)

    # Skip if topic is already known to exist
    if topic_name in self._known_topics:
      cached_policy = self._known_topic_policies.get(topic_name)
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
      )
      self._known_topic_policies[topic_name] = policy
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
      if self._admin_client is None:
        msg = "KafkaBackend not connected: admin client is None"
        raise BackendConnectionError(msg, backend_type="kafka")
      response = self._admin_client.create_topics([new_topic])
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
        )
      except KafkaError as e:
        msg = f"Failed to inspect existing Kafka topic {topic_name}."
        raise QueueError(
          msg,
          queue_name=queue_name,
          operation="push",
        ) from e
    self._known_topics.add(topic_name)
    self._known_topic_policies[topic_name] = policy
    self._log_success_diagnostic(
      "%s Kafka topic: %s",
      "Created" if created else "Verified existing",
      topic_name,
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
  ) -> None:
    """Fail closed when an existing topic contradicts queue durability.

    Existing topics remain operator-managed: this method never alters broker
    state. It verifies the fields whose mismatch would make accepted public
    settings a silent no-op.
    """
    admin = self._admin_client
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
    if not isinstance(partition_entries, (list, tuple)) or len(
      partition_entries
    ) != partitions:
      raise policy_error()
    for entry in partition_entries:
      if not isinstance(entry, dict):
        raise policy_error()
      assigned_replicas = entry.get("replicas")
      if not isinstance(assigned_replicas, (list, tuple)) or len(
        assigned_replicas
      ) != replicas:
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
      snapshot = self._connection_snapshot_or_capture()
      self._ensure_topic_exists(queue_name, snapshot)
      topic_name = f"scrapy-{queue_name}"
      partition = max(
        0,
        min(int(priority), snapshot.max_priority_partitions - 1),
      )

      if self._producer is None:
        msg = "KafkaBackend not connected: producer is None"
        raise BackendConnectionError(msg, backend_type="kafka")
      future = self._producer.send(topic_name, value=item, partition=partition)
      # Wait for send to complete (synchronous for reliability)
      future.get(timeout=10)
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
    with self._delivery_lock:
      record = self._poll_record(queue_name, timeout)
      if record is None:
        return None
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
    with self._delivery_lock:
      record = self._poll_record(queue_name, timeout)
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
      self._active_attempts[self._attempt_key(token)] = token.delivery_attempt
      # Track the pop frontier so the watermark walk terminates at the highest
      # popped offset (+1) on this topic-partition — never walks into
      # not-yet-popped offsets and never runs away on an empty in-flight set.
      self._high_water[topic_partition] = max(
        self._high_water.get(topic_partition, 0), record.offset + 1
      )
      # Token and legacy settlement modes must not share a bare-commit slot.
      # Otherwise nack(token) followed by ack(token=None) can commit the nacked
      # offset through KafkaConsumer.commit().
      self._last_record = None
      return (record.value, token)

  def _poll_record(self, queue_name: str, timeout: float) -> Any:
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
    _validate_topic_name(queue_name)
    try:
      topic_name = f"scrapy-{queue_name}"

      # Create consumer if not exists
      if self._consumer is None:
        snapshot = self._connection_snapshot_or_capture()
        auto_offset_reset = snapshot.auto_offset_reset
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
        if consumer is not None:
          self._consumer_generation += 1
          self._consumer = consumer
          self._consumer_auto_offset_reset = auto_offset_reset

      if self._consumer is None:
        msg = "KafkaBackend not connected: consumer is None"
        raise BackendConnectionError(msg, backend_type="kafka")
      # Subscribe only when the topic changes. kafka-python's subscribe() is
      # idempotent on unchanged topics, but skipping the redundant call avoids
      # needless subscription-state work on every pop of the same queue (R2-E3).
      if self._subscribed_topic != topic_name:
        with self._delivery_lock:
          self._advance_assignment_epoch_locked()
          self._consumer.subscribe(
            [topic_name], listener=self._rebalance_listener
          )
        self._subscribed_topic = topic_name

      # Poll for messages
      timeout_ms = int(timeout * 1000)
      messages = self._consumer.poll(timeout_ms=timeout_ms, max_records=1)

      for records in messages.values():
        for record in records:
          return record
    except KafkaError as e:
      msg = f"Failed to pop from queue {queue_name}: {e}"
      raise QueueError(
        msg,
        queue_name=queue_name,
        operation="pop",
      ) from e
    return None

  @queue_operation_error_boundary(
    "ack",
    "Failed to ack Kafka message.",
    safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
  )
  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Ack a popped message.

    With a ``token`` (the path the scheduler uses under
    ``CONCURRENT_REQUESTS > 1``): mark the token's (topic, partition, offset)
    completed and **commit the contiguous low-watermark** for that
    topic-partition — the largest offset such that every record from the
    last-committed offset up to it is completed. No unprocessed record is
    ever skipped.

    Without a ``token`` (legacy single-pop caller): commit the tracked
    ``_last_record`` wholesale. Only correct for ``CONCURRENT_REQUESTS=1``
    — kept for backward compatibility with external callers that pop()
    then ack() without threading the token through.

    Args:
        queue_name: Name of the queue (unused for the commit; kept for
            interface symmetry).
        token: A :class:`_KafkaAckToken` from :meth:`pop_with_ack`, or
            ``None`` to ack the last-popped record.

    Raises:
        QueueError: If the underlying commit fails.
    """
    del queue_name
    if token is not None:
      if not isinstance(token, _KafkaAckToken):
        return
      self._ack_token(token)
      return
    with self._delivery_lock:
      # Legacy path: commit the last-popped record wholesale.
      if self._consumer is None or self._last_record is None:
        return
      try:
        self._consumer.commit()
      except KafkaError as e:
        msg = f"Failed to ack Kafka message: {e}"
        raise QueueError(msg, operation="ack") from e
      else:
        self._last_record = None

  def _ack_token(self, token: _KafkaAckToken) -> None:
    """Record ``token`` completed and commit its topic-partition's watermark.

    The watermark is the largest ``offset + 1`` such that every record
    from the seeded base up to it is completed (removed from the in-flight
    set). Committing it advances the committed cursor past a contiguous
    run of processed records, leaving any unprocessed record's offset
    uncommitted (so it re-delivers on consumer restart — at-least-once).

    Core watermark algorithm (4 lines):
    ::

        in_flight.remove(token.offset)             # mark completed
        watermark = self._watermarks[topic_partition]  # seeded base
        while watermark not in in_flight:          # contiguous run
            watermark += 1
        commit({TopicPartition(topic, p): OffsetAndMetadata(watermark, "")})

    Idempotent: acking the same token twice is a no-op (the offset is
    already removed the second time, so the watermark doesn't advance
    further and no duplicate commit fires).
    """
    with self._delivery_lock:
      self._ack_token_locked(token)

  def _ack_token_locked(self, token: _KafkaAckToken) -> None:
    """Implement exact-attempt acknowledgement under ``_delivery_lock``."""
    consumer = self._consumer
    if consumer is None or not self._token_is_active_locked(token):
      return
    partition = token.partition
    topic_partition = (token.topic, partition)
    in_flight = self._in_flight.get(topic_partition)
    if in_flight is None or token.offset not in in_flight:
      return
    in_flight.remove(token.offset)
    # pop_with_ack seeds this from the first delivered record. The fallback is
    # defensive for callers/tests that construct internal state directly.
    self._watermarks.setdefault(topic_partition, token.offset)
    # Advance the watermark past the contiguous completed run. Each step is
    # O(1) set membership; the walk is bounded by _high_water (the pop
    # frontier) so it never walks into not-yet-popped offsets and never
    # runs away on an empty in-flight set.
    base = self._watermarks[topic_partition]
    high = self._high_water.get(topic_partition, base)
    watermark = base
    while watermark < high and watermark not in in_flight:
      watermark += 1
    # Commit only if the watermark advanced past the base.
    if watermark > base:
      try:
        tp = TopicPartition(token.topic, partition)
        consumer.commit({tp: OffsetAndMetadata(watermark, "")})
      except KafkaError as e:
        # The broker did not persist the candidate watermark. Restore this
        # token as in-flight so retrying the same ack recomputes and retries
        # the identical commit instead of being mistaken for a duplicate.
        in_flight.add(token.offset)
        msg = f"Failed to ack Kafka message: {e}"
        raise QueueError(msg, operation="ack") from e
      else:
        self._watermarks[topic_partition] = watermark
    self._finish_attempt_locked(token)
    # R14-E: prune bookkeeping when a topic-partition drains.
    # ``_in_flight``/``_watermarks``/``_high_water`` grow one key per
    # topic-partition ever popped; without pruning, topic/partition churn
    # grows the dicts unbounded. When its in-flight set empties, the watermark
    # has caught up to the popped frontier (no gaps), so the seed/watermark/
    # high-water entries are stale and safe to drop. A fresh pop on the same
    # topic-partition re-seeds them lazily.
    if not in_flight:
      # ``defaultdict`` re-creates the key on access, so use ``del`` (or
      # ``pop``) to genuinely remove it; ``in_flight`` is a reference into
      # the defaultdict, so mutating it does not touch the dict key.
      self._in_flight.pop(topic_partition, None)
      self._watermarks.pop(topic_partition, None)
      self._high_water.pop(topic_partition, None)

  @queue_operation_error_boundary(
    "nack",
    "Failed to nack Kafka message.",
    safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
  )
  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
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
    with self._delivery_lock:
      consumer = self._consumer
      if token is not None:
        if (
          not isinstance(token, _KafkaAckToken)
          or consumer is None
          or not self._token_is_active_locked(token)
        ):
          return
        tp = TopicPartition(token.topic, token.partition)
        try:
          if tp in consumer.assignment():
            consumer.seek(tp, token.offset)
        except KafkaError as e:
          msg = f"Failed to nack Kafka message: {e}"
          raise QueueError(msg, operation="nack") from e
        self._finish_attempt_locked(token)
        return

      record = self._last_record
      if consumer is None or record is None:
        return
      tp = TopicPartition(record.topic, record.partition)
      try:
        if tp in consumer.assignment():
          consumer.seek(tp, record.offset)
      except KafkaError as e:
        msg = f"Failed to nack Kafka message: {e}"
        raise QueueError(msg, operation="nack") from e
      self._last_record = None

  @queue_operation_error_boundary(
    "queue_len",
    "Failed to inspect Kafka queue.",
    safe_messages=_KAFKA_SAFE_QUEUE_MESSAGES,
    validator=_validate_queue_name_argument,
  )
  def queue_len(self, queue_name: str) -> int:
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
    _validate_topic_name(queue_name)
    topic_name = f"scrapy-{queue_name}"
    snapshot = self._connection_snapshot_or_capture()
    # KafkaConsumer is explicitly not thread-safe. Keep assignment, group
    # offset, and watermark calls in the same transaction as poll/ack/nack and
    # disconnect, and capture the handle once for the whole calculation.
    with self._delivery_lock:
      consumer = self._consumer
      if consumer is not None:
        try:
          assignment = consumer.assignment()
          topic_assignment = {tp for tp in assignment if tp.topic == topic_name}
          if topic_assignment:
            auto_offset_reset = (
              self._consumer_auto_offset_reset or snapshot.auto_offset_reset
            )
            return self._consumer_group_lag(
              consumer,
              topic_assignment,
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
          assignment = {TopicPartition(topic_name, partition) for partition in partitions}
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
    _validate_topic_name(queue_name)
    raise QueueError(
      _KAFKA_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
      queue_name=queue_name,
      operation="clear_queue",
    )
