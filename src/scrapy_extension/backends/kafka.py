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
from typing import TYPE_CHECKING, Any, cast

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
    _get_mode_text,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.settings import KafkaMode
from scrapy_extension.settings.kafka import (
  validate_kafka_authentication,
  validate_kafka_delivery_policy,
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


if TYPE_CHECKING:
  from scrapy_extension.settings import KafkaSettings

logger = logging.getLogger(__name__)


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

  def connect(self) -> None:
    """Establish connection to Kafka based on deployment mode.

    Creates Kafka producer and admin client with mode-specific configuration.

    Raises:
        BackendConnectionError: If the connection cannot be established.
        ConfigurationError: If the configuration is invalid for the mode.
    """
    if self.config.mode not in (
      KafkaMode.STANDALONE,
      KafkaMode.CLUSTER,
      KafkaMode.CONFLUENT,
    ):
      msg = f"Unsupported Kafka mode: {_get_mode_text(self.config.mode)}"
      raise ConfigurationError(
        msg,
        setting_name="mode",
        setting_value=self.config.mode,
      )
    # Pydantic models are mutable after construction. Revalidate auth before
    # any SDK object can observe a post-construction downgrade.
    self._validated_authentication()
    self._validated_delivery_policy()
    try:
      if self.config.mode == KafkaMode.STANDALONE:
        self._connect_standalone()
      elif self.config.mode == KafkaMode.CLUSTER:
        self._connect_cluster()
      else:
        self._connect_confluent()
      logger.debug("Connected to Kafka in %s mode", self.config.mode.value)
    except KafkaError as e:
      self._abort_partial_connect()
      msg = f"Failed to connect to Kafka ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="kafka",
      ) from e
    except Exception as e:
      self._abort_partial_connect()
      # Unexpected errors (e.g., RuntimeError from mocking in tests)
      msg = f"Failed to connect to Kafka ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="kafka",
      ) from e

  def _abort_partial_connect(self) -> None:
    """Close+null any clients assigned before ``connect()`` failed.

    R-kacc: in each ``_connect_*`` path ``self._producer`` is assigned
    BEFORE ``KafkaAdminClient`` is constructed. If admin construction (or
    any later step) raises, ``self._producer`` would otherwise stay set so
    :meth:`is_connected` lies ``True`` (silent wedge — backend reports
    connected but has no admin client, so ping/queue_len/clear_queue are
    dead) and the producer leaks under the ConnectionManager retry loop.
    Mirror the R-mcc memcached connect-cleanup (PR #60): null the partial
    state so ``is_connected()`` stays truthful. Idempotent and close-safe
    (a failing ``close()`` during teardown cannot mask the original error).
    """
    if self._producer is not None:
      with contextlib.suppress(Exception):
        self._producer.close()
      self._producer = None
    if self._admin_client is not None:
      with contextlib.suppress(Exception):
        self._admin_client.close()
      self._admin_client = None

  def _build_common_config(self) -> dict[str, Any]:
    """Build common Kafka client configuration.

    Returns:
        Dictionary of Kafka client configuration options.
    """
    mechanism, username, password, _api_key, _api_secret = (
      self._validated_authentication()
    )
    acks, _partitions, _replicas, _retention, _min_isr = (
      self._validated_delivery_policy()
    )
    config: dict[str, Any] = {
      "acks": acks,
      "retries": self.config.retries,
      "batch_size": self.config.batch_size,
      "linger_ms": self.config.linger_ms,
      "compression_type": self.config.compression_type,
      "max_in_flight_requests_per_connection": self.config.max_in_flight_requests_per_connection,
      "request_timeout_ms": self.config.request_timeout_ms,
    }

    # Add SASL/SSL configuration if security is enabled
    if self.config.security_protocol != "PLAINTEXT":
      config["security_protocol"] = self.config.security_protocol

      if mechanism is not None:
        config["sasl_mechanism"] = mechanism
      if username is not None and password is not None:
        config["sasl_plain_username"] = username
        config["sasl_plain_password"] = _RedactedStr(password)

      if self.config.ssl_cafile:
        config["ssl_cafile"] = self.config.ssl_cafile
      if self.config.ssl_certfile:
        config["ssl_certfile"] = self.config.ssl_certfile
      if self.config.ssl_keyfile:
        config["ssl_keyfile"] = self.config.ssl_keyfile
      config["ssl_check_hostname"] = self.config.ssl_check_hostname

    return config

  def _validated_authentication(
    self,
  ) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Revalidate and extract the current mechanism-aware credentials."""
    return validate_kafka_authentication(
      self.config.mode,
      self.config.security_protocol,
      self.config.sasl_mechanism,
      self.config.sasl_username,
      self.config.sasl_password,
      self.config.confluent_api_key,
      self.config.confluent_api_secret,
    )

  def _validated_delivery_policy(
    self,
  ) -> tuple[int | str, int, int, int, int]:
    """Revalidate broker acknowledgement and topic durability settings."""
    return validate_kafka_delivery_policy(
      self.config.acks,
      self.config.max_priority_partitions,
      self.config.num_partitions,
      self.config.replication_factor,
      self.config.retention_ms,
      self.config.min_insync_replicas,
    )

  def _bootstrap_servers(self) -> str:
    """Return bootstrap servers for the current Kafka mode."""
    if self.config.mode == KafkaMode.CLUSTER and self.config.cluster_brokers:
      return ",".join(self.config.cluster_brokers)
    if self.config.mode == KafkaMode.CONFLUENT:
      return self.config.confluent_bootstrap_servers or self.config.bootstrap_servers
    return self.config.bootstrap_servers

  def _build_client_security_config(self) -> dict[str, Any]:
    """Build consumer/admin-safe security config without producer-only args."""
    _mechanism, _username, _password, api_key, api_secret = (
      self._validated_authentication()
    )
    if self.config.mode == KafkaMode.CONFLUENT:
      if api_key is not None and api_secret is not None:
        return {
          "security_protocol": "SASL_SSL",
          "sasl_mechanism": "PLAIN",
          "sasl_plain_username": _RedactedStr(api_key),
          "sasl_plain_password": _RedactedStr(api_secret),
        }

    common_config = self._build_common_config()
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

  def _build_producer_config(self) -> dict[str, Any]:
    """Build producer config with mode-specific bootstrap and security settings."""
    config = self._build_common_config()
    config["bootstrap_servers"] = self._bootstrap_servers()
    if self.config.mode == KafkaMode.CONFLUENT:
      config.update(self._build_client_security_config())
    return config

  def _connect_standalone(self) -> None:
    """Connect to standalone Kafka broker."""
    bootstrap = self._bootstrap_servers()
    producer_config = self._build_producer_config()
    client_security_config = self._build_client_security_config()

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
    logger.debug("Connected to standalone Kafka at %s", bootstrap)

  def _connect_cluster(self) -> None:
    """Connect to Kafka cluster.

    Uses cluster_brokers if configured, otherwise falls back to bootstrap_servers.
    """
    bootstrap = self._bootstrap_servers()
    producer_config = self._build_producer_config()
    client_security_config = self._build_client_security_config()

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
    logger.debug("Connected to Kafka cluster at %s", bootstrap)

  def _connect_confluent(self) -> None:
    """Connect to Confluent Cloud.

    Uses SASL/SSL authentication with Confluent-specific settings.
    """
    bootstrap = self._bootstrap_servers()
    producer_config = self._build_producer_config()
    client_security_config = self._build_client_security_config()

    self._producer = KafkaProducer(**producer_config)
    self._admin_client = KafkaAdminClient(
      bootstrap_servers=bootstrap,
      client_id="scrapy-extension-admin",
      **client_security_config,
    )
    logger.debug("Connected to Confluent Cloud at %s", bootstrap)

  def disconnect(self) -> None:
    """Close Kafka connection."""
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
      self._consumer_generation += 1
      self._assignment_epoch += 1
      self._subscribed_topic = None
      self._clear_delivery_state_locked()
      self._known_topics.clear()
      self._known_topic_policies.clear()

    for client in (producer, consumer, admin_client):
      if client is not None:
        with contextlib.suppress(Exception):
          client.close()

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
    except KafkaError:
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

  def _ensure_topic_exists(self, queue_name: str) -> None:
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
    _acks, partitions, replicas, retention, min_isr = (
      self._validated_delivery_policy()
    )
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
    logger.debug(
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
      self._ensure_topic_exists(queue_name)
      topic_name = f"scrapy-{queue_name}"
      partition = max(0, min(int(priority), self.config.max_priority_partitions - 1))

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
        auto_offset_reset = self.config.auto_offset_reset
        consumer = KafkaConsumer(
          bootstrap_servers=self._bootstrap_servers(),
          group_id=self.config.group_id,
          auto_offset_reset=auto_offset_reset,
          enable_auto_commit=False,
          auto_commit_interval_ms=self.config.auto_commit_interval_ms,
          max_poll_records=self.config.max_poll_records,
          session_timeout_ms=self.config.session_timeout_ms,
          **self._build_client_security_config(),
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
              self._consumer_auto_offset_reset or self.config.auto_offset_reset
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
      auto_offset_reset = self.config.auto_offset_reset
      temp_consumer = KafkaConsumer(
        bootstrap_servers=self._bootstrap_servers(),
        group_id=self.config.group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        **self._build_client_security_config(),
      )
      partitions = temp_consumer.partitions_for_topic(topic_name)
      if not partitions:
        return 0
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
    finally:
      if temp_consumer is not None:
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

  def clear_queue(self, queue_name: str) -> None:
    """Reject Kafka clear because delete/recreate is not linearizable.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
        NotImplementedError: Always, after validating ``queue_name``.
    """
    _validate_topic_name(queue_name)
    raise NotImplementedError(
      "Kafka clear_queue is unsupported: asynchronous topic delete/recreate "
      "cannot preserve active consumer-group offsets or protect messages "
      "accepted after clear returns. Stop and drain the queue with an "
      "operator-controlled Kafka maintenance workflow instead."
    )
