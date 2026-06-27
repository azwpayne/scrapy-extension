"""Kafka backend implementation with multi-mode support.

This module provides a Kafka-based implementation of QueueBackend,
supporting multiple deployment modes:
- Standalone: Single Kafka broker
- Cluster: Multi-broker Kafka cluster
- Confluent: Confluent Cloud configuration

Note: Kafka does not support SetBackend or StorageBackend operations.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any, cast

try:
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import KafkaError, TopicAlreadyExistsError
    from kafka.structs import OffsetAndMetadata
except ImportError as e:
    raise ImportError(
        "Kafka backend requires 'kafka-python'. Install with: pip install scrapy-extension[kafka]"
    ) from e

from scrapy_extension.backends._redaction import _RedactedStr
from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    _get_mode_text,
    secret_value,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.settings import KafkaMode

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
  """Opaque ack token carrying the (partition, offset) of a popped record.

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
  """

  __slots__ = ("offset", "partition", "topic")

  def __init__(self, partition: int, offset: int, topic: str) -> None:
    """Initialize the token.

    Args:
        partition: Kafka partition.
        offset: Record offset within the partition.
        topic: The topic the record was consumed from.
    """
    self.partition = partition
    self.offset = offset
    self.topic = topic

  def __eq__(self, other: object) -> bool:
    if not isinstance(other, _KafkaAckToken):
      return NotImplemented
    return (
      self.partition == other.partition
      and self.offset == other.offset
      and self.topic == other.topic
    )

  def __hash__(self) -> int:
    return hash((self.partition, self.offset, self.topic))

  def __repr__(self) -> str:
    return f"_KafkaAckToken(topic={self.topic!r}, partition={self.partition}, offset={self.offset})"


class KafkaBackend(Backend, QueueBackend):
  """Kafka backend implementation with multi-mode support.

  Implements QueueBackend using Kafka topics with partition-based priority.
  Supports standalone, cluster, and confluent deployment modes.
  Does NOT implement SetBackend or StorageBackend.

  Ack capability: ``requires_ack=True``, ``supports_concurrent_ack=True``.
  Kafka pops carry an ack token (partition, offset) tracked in a per-
  partition in-flight set; :meth:`ack` commits the contiguous low-watermark
  for the token's partition. N pops before any ack no longer overwrite a
  single slot — ack is correct under ``CONCURRENT_REQUESTS > 1``.

  Attributes:
      config: KafkaSettings instance with connection parameters.
      _producer: The Kafka producer instance.
      _consumer: The Kafka consumer instance.
      _admin_client: The Kafka admin client instance.
      _known_topics: Set of topics known to exist (cached to avoid repeated checks).
  """

  requires_ack = True
  supports_concurrent_ack = True

  def __init__(self, config: KafkaSettings) -> None:
    """Initialize Kafka backend.

    Args:
        config: Configuration for Kafka connection.
    """
    self.config = config
    self._producer: KafkaProducer | None = None
    self._consumer: KafkaConsumer | None = None
    # Legacy single-slot for the ``ack(token=None)`` fallback path. Kept so
    # external callers that pop() then ack() without a token still work.
    self._last_record: Any = None
    # In-flight ack tracking for correctness under CONCURRENT_REQUESTS>1.
    # partition -> set of popped-but-unacked offsets. ack(token) records the
    # completed offset and commits the contiguous low-watermark for its
    # partition — the largest offset such that all records from the last-
    # committed offset up to it are completed (no unprocessed record skipped).
    self._in_flight: dict[int, set[int]] = defaultdict(set)
    # partition -> last-committed watermark (offset of the next record to
    # consume). Seeded lazily from the consumer position on first ack so the
    # watermark math is independent of enable_auto_commit.
    self._watermarks: dict[int, int] = {}
    # partition -> highest offset ever popped + 1. Bounds the watermark walk
    # so it stops at the frontier of popped records (never walks into
    # not-yet-popped offsets, and never runs away on an empty in-flight set).
    self._high_water: dict[int, int] = {}
    self._admin_client: KafkaAdminClient | None = None
    # Cache known topics to avoid repeated existence checks
    self._known_topics: set[str] = set()
    # Topic the consumer is currently subscribed to, so pop() only
    # re-subscribes when it changes — mirrors RocketMQ's _ensure_subscribed
    # (R7). Avoids a redundant subscribe() on every pop of the same queue
    # (Scrapy's next_request pops the same queue every tick). R2-E3.
    self._subscribed_topic: str | None = None

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
    try:
      if self.config.mode == KafkaMode.STANDALONE:
        self._connect_standalone()
      elif self.config.mode == KafkaMode.CLUSTER:
        self._connect_cluster()
      else:
        self._connect_confluent()
      logger.debug("Connected to Kafka in %s mode", self.config.mode.value)
    except KafkaError as e:
      msg = f"Failed to connect to Kafka ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="kafka",
      ) from e
    except Exception as e:
      # Unexpected errors (e.g., RuntimeError from mocking in tests)
      msg = f"Failed to connect to Kafka ({self.config.mode.value}): {e}"
      raise BackendConnectionError(
        msg,
        backend_type="kafka",
      ) from e

  def _build_common_config(self) -> dict[str, Any]:
    """Build common Kafka client configuration.

    Returns:
        Dictionary of Kafka client configuration options.
    """
    config: dict[str, Any] = {
      "acks": self.config.acks,
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

      if (
        self.config.sasl_mechanism
        and self.config.sasl_username
        and self.config.sasl_password
      ):
        config["sasl_mechanism"] = self.config.sasl_mechanism
        config["sasl_plain_username"] = self.config.sasl_username
        config["sasl_plain_password"] = _RedactedStr(secret_value(self.config.sasl_password))

      if self.config.ssl_cafile:
        config["ssl_cafile"] = self.config.ssl_cafile
      if self.config.ssl_certfile:
        config["ssl_certfile"] = self.config.ssl_certfile
      if self.config.ssl_keyfile:
        config["ssl_keyfile"] = self.config.ssl_keyfile
      config["ssl_check_hostname"] = self.config.ssl_check_hostname

    return config

  def _bootstrap_servers(self) -> str:
    """Return bootstrap servers for the current Kafka mode."""
    if self.config.mode == KafkaMode.CLUSTER and self.config.cluster_brokers:
      return ",".join(self.config.cluster_brokers)
    if self.config.mode == KafkaMode.CONFLUENT:
      return self.config.confluent_bootstrap_servers or self.config.bootstrap_servers
    return self.config.bootstrap_servers

  def _build_client_security_config(self) -> dict[str, Any]:
    """Build consumer/admin-safe security config without producer-only args."""
    if self.config.mode == KafkaMode.CONFLUENT:
      if self.config.confluent_api_key and self.config.confluent_api_secret:
        return {
          "security_protocol": "SASL_SSL",
          "sasl_mechanism": "PLAIN",
          "sasl_plain_username": _RedactedStr(
            secret_value(self.config.confluent_api_key)
          ),
          "sasl_plain_password": _RedactedStr(
            secret_value(self.config.confluent_api_secret)
          ),
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
    if self._producer:
      self._producer.close()
      self._producer = None
    if self._consumer:
      self._consumer.close()
      self._consumer = None
      self._subscribed_topic = None
    if self._admin_client:
      self._admin_client.close()
      self._admin_client = None

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
    """
    _validate_topic_name(queue_name)
    topic_name = f"scrapy-{queue_name}"

    # Skip if topic is already known to exist
    if topic_name in self._known_topics:
      return

    try:
      new_topic = NewTopic(
        name=topic_name,
        num_partitions=self.config.max_priority_partitions,
        replication_factor=self.config.replication_factor,
      )
      if self._admin_client is None:
        msg = "KafkaBackend not connected: admin client is None"
        raise BackendConnectionError(msg, backend_type="kafka")
      self._admin_client.create_topics([new_topic])
      self._known_topics.add(topic_name)
      logger.debug("Created Kafka topic: %s", topic_name)
    except TopicAlreadyExistsError:
      # Topic already exists - this is expected
      self._known_topics.add(topic_name)
      logger.debug("Kafka topic already exists: %s", topic_name)
    except KafkaError as e:
      logger.warning("Failed to create topic %s: %s", topic_name, e)

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
    the per-partition in-flight set so ack(token) commits the correct
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
    record = self._poll_record(queue_name, timeout)
    if record is None:
      return None
    self._last_record = record
    return cast(bytes, record.value)

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop an item together with a :class:`_KafkaAckToken`.

    Records the popped (partition, offset) in the per-partition in-flight
    set so :meth:`ack` can commit the correct contiguous watermark for
    that partition — correct under ``CONCURRENT_REQUESTS > 1`` (no
    message skipped, no single-slot overwrite).

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
    record = self._poll_record(queue_name, timeout)
    if record is None:
      return (None, None)
    token = _KafkaAckToken(
      partition=record.partition,
      offset=record.offset,
      topic=record.topic,
    )
    self._in_flight[record.partition].add(record.offset)
    # Track the pop frontier so the watermark walk terminates at the highest
    # popped offset (+1) on this partition — never walks into not-yet-popped
    # offsets and never runs away on an empty in-flight set.
    self._high_water[record.partition] = max(
      self._high_water.get(record.partition, 0), record.offset + 1
    )
    # Keep _last_record in sync so the legacy ack(token=None) path stays
    # usable for callers that don't thread the token through.
    self._last_record = record
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
        self._consumer = KafkaConsumer(
          bootstrap_servers=self._bootstrap_servers(),
          group_id=self.config.group_id,
          auto_offset_reset=self.config.auto_offset_reset,
          enable_auto_commit=self.config.enable_auto_commit,
          auto_commit_interval_ms=self.config.auto_commit_interval_ms,
          max_poll_records=self.config.max_poll_records,
          session_timeout_ms=self.config.session_timeout_ms,
          **self._build_client_security_config(),
        )

      if self._consumer is None:
        msg = "KafkaBackend not connected: consumer is None"
        raise BackendConnectionError(msg, backend_type="kafka")
      # Subscribe only when the topic changes. kafka-python's subscribe() is
      # idempotent on unchanged topics, but skipping the redundant call avoids
      # needless subscription-state work on every pop of the same queue (R2-E3).
      if self._subscribed_topic != topic_name:
        self._consumer.subscribe([topic_name])
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
    ``CONCURRENT_REQUESTS > 1``): mark the token's (partition, offset)
    completed and **commit the contiguous low-watermark** for that
    partition — the largest offset such that every record from the
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
      self._ack_token(token)
      return
    # Legacy path: commit the last-popped record wholesale.
    if self._consumer is None or self._last_record is None:
      return
    try:
      self._consumer.commit()
    except KafkaError as e:
      msg = f"Failed to ack Kafka message: {e}"
      raise QueueError(msg, operation="ack") from e
    finally:
      self._last_record = None

  def _ack_token(self, token: _KafkaAckToken) -> None:
    """Record ``token`` completed and commit its partition's watermark.

    The watermark is the largest ``offset + 1`` such that every record
    from the seeded base up to it is completed (removed from the in-flight
    set). Committing it advances the committed cursor past a contiguous
    run of processed records, leaving any unprocessed record's offset
    uncommitted (so it re-delivers on consumer restart — at-least-once).

    Core watermark algorithm (4 lines):
    ::

        in_flight.discard(token.offset)            # mark completed
        watermark = self._watermarks[partition]    # seeded base
        while watermark not in in_flight:          # contiguous run
            watermark += 1
        commit({TopicPartition(topic, p): OffsetAndMetadata(watermark, "")})

    Idempotent: acking the same token twice is a no-op (the offset is
    already removed the second time, so the watermark doesn't advance
    further and no duplicate commit fires).
    """
    if self._consumer is None:
      return
    partition = token.partition
    in_flight = self._in_flight[partition]
    # Idempotent guard — duplicate ack of an already-completed offset.
    in_flight.discard(token.offset)
    # Seed the watermark base from the consumer position once per partition,
    # so the watermark math works whether enable_auto_commit is on or off.
    if partition not in self._watermarks:
      try:
        tp = TopicPartition(token.topic, partition)
        self._watermarks[partition] = self._consumer.position(tp)
      except KafkaError as e:
        msg = f"Failed to read consumer position for ack: {e}"
        raise QueueError(msg, operation="ack") from e
    # Advance the watermark past the contiguous completed run. Each step is
    # O(1) set membership; the walk is bounded by _high_water (the pop
    # frontier) so it never walks into not-yet-popped offsets and never
    # runs away on an empty in-flight set.
    base = self._watermarks[partition]
    high = self._high_water.get(partition, base)
    watermark = base
    while watermark < high and watermark not in in_flight:
      watermark += 1
    # Commit only if the watermark advanced past the base.
    if watermark > base:
      try:
        tp = TopicPartition(token.topic, partition)
        self._consumer.commit({tp: OffsetAndMetadata(watermark, "")})
      except KafkaError as e:
        msg = f"Failed to ack Kafka message: {e}"
        raise QueueError(msg, operation="ack") from e
      else:
        self._watermarks[partition] = watermark
    # R14-E: prune the per-partition bookkeeping when a partition drains.
    # ``_in_flight``/``_watermarks``/``_high_water`` grow one key per
    # partition ever popped; without pruning, partition churn (topics with
    # transient partitions, or long-running multi-topic crawls) grows the
    # dicts unbounded. When the in-flight set for a partition empties, the
    # watermark has caught up to the popped frontier (no gaps), so the
    # seed/watermark/high-water entries are stale and safe to drop — a
    # fresh pop on the same partition re-seeds them lazily.
    if not in_flight:
      # ``defaultdict`` re-creates the key on access, so use ``del`` (or
      # ``pop``) to genuinely remove it; ``in_flight`` is a reference into
      # the defaultdict, so mutating it does not touch the dict key.
      self._in_flight.pop(partition, None)
      self._watermarks.pop(partition, None)
      self._high_water.pop(partition, None)

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Nack a popped message — do NOT commit its offset (at-least-once retry).

    Kafka cannot re-deliver a polled message within the same consumer
    session: the offset is only re-read on the next consumer restart. So
    nack records the message as *still* unprocessed (it was never added to
    the completed set) and deliberately does not advance the watermark
    past it. On consumer restart the message re-delivers (at-least-once).

    With a ``token``: drop the (partition, offset) from the completed
    tracking (it was in-flight; removing it keeps the watermark from ever
    advancing past it, so it re-delivers on restart).

    Without a ``token``: clear the legacy ``_last_record`` slot (same
    semantics as before).

    Args:
        queue_name: Name of the queue (unused; interface symmetry).
        token: A :class:`_KafkaAckToken` from :meth:`pop_with_ack`, or
            ``None`` for the legacy last-record path.
    """
    del queue_name
    if token is not None:
      # Leave the offset in (or re-add it to) the in-flight set so the
      # watermark never advances past it → uncommitted → re-delivered on
      # consumer restart. In-session re-delivery is impossible in Kafka.
      if isinstance(token, _KafkaAckToken):
        self._in_flight[token.partition].add(token.offset)
      return
    self._last_record = None

  def queue_len(self, queue_name: str) -> int:
    """Get queue length.

    Args:
        queue_name: Name of the queue.

    Returns:
        Approximate number of items in the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.

    Note:
        This is eventually consistent and should be used for monitoring only.
    """
    _validate_topic_name(queue_name)
    if self._consumer is not None:
      try:
        assignment = self._consumer.assignment()
        if not assignment:
          return 0
        end_offsets = self._consumer.end_offsets(assignment)
        total = sum(
          max(0, end_offsets[tp] - self._consumer.position(tp))
          for tp in assignment
        )
      except KafkaError:
        return 0
      return cast(int, total)

    topic_name = f"scrapy-{queue_name}"
    temp_consumer = KafkaConsumer(
      bootstrap_servers=self._bootstrap_servers(),
      group_id=self.config.group_id,
      enable_auto_commit=False,
      **self._build_client_security_config(),
    )
    try:
      partitions = temp_consumer.partitions_for_topic(topic_name)
      if not partitions:
        return 0
      assignment = {TopicPartition(topic_name, partition) for partition in partitions}
      temp_consumer.assign(list(assignment))
      end_offsets = temp_consumer.end_offsets(list(assignment))
      total = sum(
        max(0, end_offsets[tp] - temp_consumer.position(tp))
        for tp in assignment
      )
    except KafkaError:
      return 0
    finally:
      temp_consumer.close()
    return cast(int, total)

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    _validate_topic_name(queue_name)
    try:
      topic_name = f"scrapy-{queue_name}"
      if self._admin_client is None:
        msg = "KafkaBackend not connected: admin client is None"
        raise BackendConnectionError(msg, backend_type="kafka")
      self._admin_client.delete_topics([topic_name])
      # Recreate topic
      new_topic = NewTopic(
        name=topic_name,
        num_partitions=self.config.max_priority_partitions,
        replication_factor=self.config.replication_factor,
      )
      self._admin_client.create_topics([new_topic])
    except KafkaError as e:
      logger.warning("Failed to clear queue %s: %s", queue_name, e)
