"""Amazon SQS backend (queue-only) — subsystem ③.

Implements QueueBackend using Amazon SQS Standard queues. MessageBody carries
base64-encoded item bytes (SQS MessageBody is a string). Ack = delete_message
(removes from the queue so it isn't redelivered); nack sets the message's
visibility timeout to zero for immediate re-delivery. Priority is ignored
(SQS has no native priority queue).

boto3 API (stable, well-known):
- ``boto3.client("sqs", region_name=, endpoint_url=, aws_access_key_id=, ...)``
- ``client.get_queue_url(QueueName=)`` / ``create_queue(QueueName=)``
- ``client.send_message(QueueUrl=, MessageBody=, DelaySeconds=)``
- ``client.receive_message(QueueUrl=, MaxNumberOfMessages=1, WaitTimeSeconds=, VisibilityTimeout=)``
- ``client.delete_message(QueueUrl=, ReceiptHandle=)``
- ``client.purge_queue(QueueUrl=)``
- ``client.get_queue_attributes(QueueUrl=, AttributeNames=[...])`` for visible,
  not-visible, and delayed approximate counts
- ``client.close()``
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import logging
import math
import re
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
  import boto3
except ImportError as e:
  if not _is_missing_optional_dependency(e, "boto3"):
    raise
  raise ImportError(
    "SQS backend requires 'boto3'. Install with: pip install scrapy-extension[sqs]"
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
from scrapy_extension.settings import SqsMode
from scrapy_extension.settings._aws import (
  validate_aws_credentials,
  validate_aws_endpoint,
  validate_aws_region_name,
)

if TYPE_CHECKING:
  from scrapy_extension.settings import SqsSettings

logger = logging.getLogger(__name__)

# SQS caps WaitTimeSeconds at 20.
_MAX_WAIT_SECONDS = 20

# PurgeQueue is asynchronous. AWS documents that both old messages and messages
# sent after the API call can be deleted for up to 60 seconds.
_SQS_PURGE_WINDOW_SECONDS = 60.0

# Standard queue names accept only these characters and at most 80 of them.
_SQS_QUEUE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,80}$")

# MessageBody is capped at 1 MiB. This backend base64-encodes the raw bytes;
# because 1 MiB is divisible by four, the largest encodable input is exactly
# three quarters of that limit.
_SQS_MAX_MESSAGE_BODY_BYTES = 1_048_576
_SQS_MAX_RAW_PAYLOAD_BYTES = 3 * (_SQS_MAX_MESSAGE_BODY_BYTES // 4)

# ``queue_len`` is used as a pending-work signal, so include messages that are
# temporarily invisible or delayed rather than reporting only immediately
# receivable messages.
_QUEUE_DEPTH_ATTRIBUTES = (
  "ApproximateNumberOfMessages",
  "ApproximateNumberOfMessagesNotVisible",
  "ApproximateNumberOfMessagesDelayed",
)

_QUEUE_MISSING_CODES = frozenset(
  {"QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue"}
)


def _is_queue_missing(exc: BaseException) -> bool:
  """Return whether an SQS client error specifically means queue missing."""
  response = getattr(exc, "response", None)
  if not isinstance(response, dict):
    return False
  error = response.get("Error")
  return isinstance(error, dict) and error.get("Code") in _QUEUE_MISSING_CODES


def _physical_queue_name(prefix: str, queue_name: str) -> str:
  """Return an unchanged valid SQS name or a stable portable mapping."""
  candidate = f"{prefix}{queue_name}"
  if _SQS_QUEUE_NAME_PATTERN.fullmatch(candidate):
    return candidate
  digest = hashlib.blake2s(digest_size=16)
  digest.update(b"scrapy-extension-sqs-v1")
  for part in (prefix, queue_name):
    encoded = part.encode("utf-8")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
  return f"scrapyext-q-{digest.hexdigest()}"

# R14-E: cap on the diagnostic in-flight ack-token set. Each unacked pop
# adds one entry; without a cap a long-running process with slow acks (or a
# bug that never acks) grows the set unbounded. We warn-once on overflow and
# STOP adding — the set is diagnostic (SQS acks each message independently
# via ``delete_message(ReceiptHandle)``, so ack correctness lives in the
# broker, not in this set). The POP itself is never dropped. 10k is generous
# for normal CONCURRENT_REQUESTS backpressure and tight enough to flag a leak.
_MAX_IN_FLIGHT = 10_000


class _SqsQueueLifecycle:
  """Shared-operation/exclusive-clear barrier for one physical queue."""

  __slots__ = ("_active_operations", "_clearing", "_condition", "epoch")

  def __init__(self) -> None:
    self._condition = threading.Condition()
    self._active_operations = 0
    self._clearing = False
    self.epoch = 0

  @contextmanager
  def operation(self) -> Iterator[int]:
    """Enter a normal operation, waiting only for a destructive clear."""
    with self._condition:
      while self._clearing:
        self._condition.wait()
      self._active_operations += 1
      epoch = self.epoch
    try:
      yield epoch
    finally:
      with self._condition:
        self._active_operations -= 1
        if self._active_operations == 0:
          self._condition.notify_all()

  @contextmanager
  def destructive_operation(self) -> Iterator[int]:
    """Exclude normal operations, advance the epoch, and hold the barrier."""
    with self._condition:
      while self._clearing:
        self._condition.wait()
      self._clearing = True
      while self._active_operations:
        self._condition.wait()
      self.epoch += 1
      epoch = self.epoch
    try:
      yield epoch
    finally:
      with self._condition:
        self._clearing = False
        self._condition.notify_all()


class _SqsAckToken:
  """Opaque ack token carrying the (queue_url, receipt_handle) of a popped msg.

  Stored in ``request.meta["_backend_ack_token"]`` and handed back to
  :meth:`SqsBackend.ack` / :meth:`SqsBackend.nack` so the specific message
  that was popped is acked — not the last-popped one. SQS ``ReceiptHandle``
  is natively per-message, and ``delete_message(QueueUrl, ReceiptHandle)``
  deletes exactly one message, so this token carries everything ack needs
  with no single-slot state. The ``queue_url`` preserves the round-2 C3
  multi-queue correctness (a token popped from qB acks against qB's URL).

  Attributes:
      queue_url: The QueueUrl the message was popped FROM.
      receipt_handle: The SQS ReceiptHandle of the popped message.
  """

  __slots__ = (
    "_settlement_lock",
    "_settlement_state",
    "generation_key",
    "queue_epoch",
    "queue_url",
    "receipt_handle",
  )

  def __init__(
    self,
    queue_url: str,
    receipt_handle: str,
    *,
    generation_key: object | None = None,
    queue_epoch: int | None = None,
  ) -> None:
    """Initialize the token.

    Args:
        queue_url: The QueueUrl the message was popped from.
        receipt_handle: The SQS ReceiptHandle identifying the message.
        generation_key: Opaque identity of the client generation that issued
            this receipt. ``None`` preserves compatibility for manually
            constructed tokens, which settle on the current generation.
        queue_epoch: The queue lifecycle epoch that issued the delivery.
    """
    self.queue_url = queue_url
    self.receipt_handle = receipt_handle
    self.generation_key = generation_key
    self.queue_epoch = queue_epoch
    self._settlement_lock = threading.Lock()
    self._settlement_state = "pending"

  def _settle(self, operation: Callable[[], str]) -> bool:
    """Run exactly one terminal broker operation for this delivery.

    The per-token lock remains held across the broker call. A competing ack
    or nack therefore observes either the restored ``pending`` state after a
    failure (and may retry) or the final terminal state after success. It can
    never report a no-op success while another settlement is still uncertain.

    Args:
        operation: Broker operation to execute while the token is claimed. It
            returns the terminal state to publish after success.

    Returns:
        True when this call claimed and terminalized the token, including a
        local ``stale`` or ``cleared`` outcome; False when the token was
        already terminal.
    """
    with self._settlement_lock:
      if self._settlement_state != "pending":
        return False
      self._settlement_state = "settling"
      terminal_state: str | None = None
      try:
        terminal_state = operation()
      finally:
        self._settlement_state = terminal_state or "pending"
      return True

  def __eq__(self, other: object) -> bool:
    if not isinstance(other, _SqsAckToken):
      return NotImplemented
    return (
      self.queue_url == other.queue_url
      and self.receipt_handle == other.receipt_handle
      and self.generation_key is other.generation_key
    )

  def __hash__(self) -> int:
    return hash((self.queue_url, self.receipt_handle, id(self.generation_key)))

  def __repr__(self) -> str:
    return (
      f"_SqsAckToken(queue_url={self.queue_url!r}, "
      f"receipt_handle={self.receipt_handle!r})"
    )


@dataclass(frozen=True, slots=True)
class _SqsConnectionSnapshot:
  """Validated operational values fixed for one SQS client generation."""

  mode: SqsMode
  region_name: str
  endpoint_url: str | None
  queue_name_prefix: str
  visibility_timeout: int


@dataclass(slots=True, eq=False)
class _SqsClientGeneration:
  """One atomically published SQS client and its generation-local caches."""

  key: object
  client: Any
  snapshot: _SqsConnectionSnapshot
  queue_urls: dict[str, str] = field(default_factory=dict)
  queue_resolution_locks: dict[str, threading.Lock] = field(default_factory=dict)
  queue_lifecycles: dict[str, _SqsQueueLifecycle] = field(default_factory=dict)
  cache_lock: threading.Lock = field(default_factory=threading.Lock)
  accepting: bool = True
  active_leases: int = 0


class SqsBackend(Backend, QueueBackend):
  """SQS backend (queue-only) with Standard-queue work semantics.

  Each queue maps to an SQS queue named ``<prefix><queue_name>``. Push base64-
  encodes the item into MessageBody; pop decodes it and tracks the receipt
  handle for ack. queue_len sums visible, in-flight, and delayed messages;
  clear_queue purges.

  Ack capability: ``requires_ack=True``, ``supports_concurrent_ack=True``.
  SQS ``ReceiptHandle`` is natively per-message —
  ``delete_message(QueueUrl, ReceiptHandle)`` deletes exactly one message —
  so :meth:`pop_with_ack` returns a :class:`_SqsAckToken` carrying the
  handle (and the source queue URL, for C3 multi-queue correctness) and the
  scheduler can ack each popped message by its OWN token under
  ``CONCURRENT_REQUESTS > 1``. A diagnostic ``_in_flight`` set mirrors
  RabbitMQ's ``_in_flight_tags`` for leak detection; the ``_last_receipt``
  single-slot is kept only for the legacy ``ack(token=None)`` path.

  Attributes:
      config: SqsSettings instance.
      _generation: The authoritative leased client, immutable operational
          snapshot, and generation-local queue caches.
      _client: Compatibility mirror of the current generation's boto3 client.
      _queue_urls: Compatibility mirror of its QueueUrl cache.
      _in_flight: Diagnostic set of popped-but-unacked tokens.
      _last_receipt: ``(queue_url, receipt_handle)`` of the last-popped msg
          (legacy ``ack(token=None)`` fallback only).
  """

  requires_ack = True
  supports_concurrent_ack = True

  def __init__(self, config: SqsSettings) -> None:
    self.config = config
    self._connect_lock = threading.Lock()
    self._generation_condition = threading.Condition()
    self._generation: _SqsClientGeneration | None = None
    # Compatibility mirrors for diagnostics and older tests. Internal
    # operations use only the generation captured by a lease.
    self._client: Any = None
    self._queue_urls: dict[str, str] = {}
    self._queue_lifecycles: dict[str, _SqsQueueLifecycle] = {}
    self._queue_lifecycles_lock = threading.Lock()
    self._in_flight: set[_SqsAckToken] = set()
    self._in_flight_lock = threading.Lock()
    # R14-E: one-shot guard for the in-flight-set-overflow warning.
    self._in_flight_overflow_warned: bool = False
    self._last_receipt: tuple[str, str] | None = None
    self._last_receipt_epoch: int | None = None
    self._last_receipt_generation_key: object | None = None

  def _capture_connection_snapshot(
    self,
  ) -> tuple[_SqsConnectionSnapshot, dict[str, Any]]:
    """Capture and revalidate every value used by one client generation."""
    mode = self.config.mode
    region_name = self.config.region_name
    endpoint_url = self.config.endpoint_url
    access_key = self.config.aws_access_key_id
    secret_key = self.config.aws_secret_access_key
    queue_name_prefix = self.config.queue_name_prefix
    visibility_timeout = self.config.visibility_timeout
    if not isinstance(mode, SqsMode):
      raise ConfigurationError(
        f"Unsupported SQS mode: {mode}",
        setting_name="mode",
        setting_value=mode,
      )
    validate_aws_endpoint(
      endpoint_url,
      cloud=mode == SqsMode.CLOUD,
      require_endpoint=mode == SqsMode.STANDALONE,
    )
    key_id, secret = validate_aws_credentials(access_key, secret_key)
    region_name = validate_aws_region_name(region_name)
    if not isinstance(queue_name_prefix, str):
      raise ConfigurationError(
        "queue_name_prefix must be a string.",
        setting_name="queue_name_prefix",
        setting_value=None,
      )
    if (
      isinstance(visibility_timeout, bool)
      or not isinstance(visibility_timeout, int)
      or not 1 <= visibility_timeout <= 12 * 60 * 60
    ):
      raise ConfigurationError(
        "visibility_timeout must be an integer between 1 and 43200.",
        setting_name="visibility_timeout",
        setting_value=None,
      )
    snapshot = _SqsConnectionSnapshot(
      mode=mode,
      region_name=region_name,
      endpoint_url=endpoint_url,
      queue_name_prefix=queue_name_prefix,
      visibility_timeout=visibility_timeout,
    )
    kwargs: dict[str, Any] = {"region_name": region_name}
    if endpoint_url is not None:
      kwargs["endpoint_url"] = endpoint_url
    if key_id is not None and secret is not None:
      kwargs["aws_access_key_id"] = _redact(key_id)
      kwargs["aws_secret_access_key"] = _redact(secret)
    return snapshot, kwargs

  def connect(self) -> None:
    """Publish one immutable SQS client generation, idempotently.

    Raises:
        BackendConnectionError: If the client cannot be created.
    """
    with self._connect_lock:
      with self._generation_condition:
        if self._generation is not None:
          return
      snapshot, kwargs = self._capture_connection_snapshot()
      try:
        candidate = boto3.client("sqs", **kwargs)
      except Exception as e:
        raise BackendConnectionError(
          f"Failed to create SQS client: {e}", backend_type="sqs"
        ) from e
      generation = _SqsClientGeneration(
        key=object(), client=candidate, snapshot=snapshot
      )
      with self._generation_condition:
        self._generation = generation
        self._client = candidate
        self._queue_urls = generation.queue_urls
        self._queue_lifecycles = generation.queue_lifecycles
        self._queue_lifecycles_lock = generation.cache_lock
        self._generation_condition.notify_all()
      logger.debug(
        "Connected to SQS (%s, %s)",
        snapshot.mode.value,
        snapshot.region_name,
      )

  def disconnect(self) -> None:
    """Detach, drain, and close the current SQS client generation."""
    with self._connect_lock:
      with self._generation_condition:
        generation = self._generation
        if generation is not None:
          generation.accepting = False
          self._generation = None
        self._client = None
        self._queue_urls = {}
        self._queue_lifecycles = {}
        self._queue_lifecycles_lock = threading.Lock()
        while generation is not None and generation.active_leases:
          self._generation_condition.wait()
      with self._in_flight_lock:
        self._in_flight.clear()
        self._last_receipt = None
        self._last_receipt_epoch = None
        self._last_receipt_generation_key = None
      if generation is not None:
        generation.queue_urls.clear()
        generation.queue_resolution_locks.clear()
        generation.queue_lifecycles.clear()
        with _swallow():
          generation.client.close()

  @contextmanager
  def _lease_generation(
    self,
    operation: str,
    *,
    generation_key: object | None = None,
    queue_name: str | None = None,
  ) -> Iterator[_SqsClientGeneration | None]:
    """Lease one complete generation, or return None for a retired token."""
    with self._generation_condition:
      generation = self._generation
      if generation_key is not None and (
        generation is None or generation.key is not generation_key
      ):
        leased = False
      else:
        if generation is None or not generation.accepting:
          raise QueueError(
            f"Cannot {operation} with SQS while backend is disconnected.",
            queue_name=queue_name,
            operation=operation,
          )
        generation.active_leases += 1
        leased = True
    if not leased:
      yield None
      return
    assert generation is not None  # noqa: S101  # nosec B101 - narrowed by lease
    try:
      yield generation
    finally:
      with self._generation_condition:
        generation.active_leases -= 1
        if generation.active_leases == 0:
          self._generation_condition.notify_all()

  def is_connected(self) -> bool:
    """Return True if the client has been created."""
    with self._generation_condition:
      return self._generation is not None

  def ping(self) -> bool:
    """Best-effort health: the client is non-None (boto3 is lazy)."""
    return self.is_connected()

  @property
  def backend_type(self) -> BackendType:
    """Return BackendType.SQS."""
    return BackendType.SQS

  def _queue_url(self, queue_name: str, *, operation: str = "resolve_queue") -> str:
    """Resolve (and cache) the QueueUrl for ``queue_name``.

    Args:
        queue_name: The queue name.

    Returns:
        The SQS QueueUrl.

    Raises:
        ValueError: If queue_name contains invalid characters.
        QueueError: If the URL cannot be resolved or created.
    """
    _validate_key_name(queue_name, "queue_name")
    with self._lease_generation(operation, queue_name=queue_name) as generation:
      if generation is None:  # pragma: no cover - non-token lease is required
        raise AssertionError("current SQS generation lease returned None")
      return self._queue_url_for_generation(
        generation, queue_name, operation=operation
      )

  def _queue_url_for_generation(
    self,
    generation: _SqsClientGeneration,
    queue_name: str,
    *,
    operation: str,
  ) -> str:
    """Resolve a QueueUrl through one generation and cache it there."""
    _validate_key_name(queue_name, "queue_name")
    with generation.cache_lock:
      cached = generation.queue_urls.get(queue_name)
      if cached is not None:
        return cached
      resolution_lock = generation.queue_resolution_locks.get(queue_name)
      if resolution_lock is None:
        resolution_lock = threading.Lock()
        generation.queue_resolution_locks[queue_name] = resolution_lock

    # Only callers resolving the same logical queue serialize across network
    # I/O. The generation cache lock remains a short dict critical section so
    # a slow qA lookup cannot delay an already-issued qB acknowledgement.
    with resolution_lock:
      with generation.cache_lock:
        cached = generation.queue_urls.get(queue_name)
        if cached is not None:
          return cached
      name = _physical_queue_name(
        generation.snapshot.queue_name_prefix, queue_name
      )
      try:
        resp = generation.client.get_queue_url(QueueName=name)
      except Exception as lookup_error:
        if not _is_queue_missing(lookup_error):
          raise QueueError(
            f"Failed to resolve SQS queue URL for {queue_name}: {lookup_error}",
            queue_name=queue_name,
            operation=operation,
          ) from lookup_error
        try:
          resp = generation.client.create_queue(QueueName=name)
        except Exception as create_error:
          raise QueueError(
            f"Failed to create missing SQS queue {queue_name}: {create_error}",
            queue_name=queue_name,
            operation=operation,
          ) from create_error
      try:
        url = resp["QueueUrl"]
      except Exception as e:
        raise QueueError(
          f"Failed to resolve SQS queue URL for {queue_name}: {e}",
          queue_name=queue_name,
          operation=operation,
        ) from e
      with generation.cache_lock:
        generation.queue_urls[queue_name] = url
      return cast(str, url)

  def _queue_lifecycle(self, queue_url: str) -> _SqsQueueLifecycle:
    """Return the stable lifecycle state for a physical SQS queue URL."""
    with self._lease_generation("resolve_queue_lifecycle") as generation:
      if generation is None:  # pragma: no cover - non-token lease is required
        raise AssertionError("current SQS generation lease returned None")
      return self._queue_lifecycle_for_generation(generation, queue_url)

  @staticmethod
  def _queue_lifecycle_for_generation(
    generation: _SqsClientGeneration, queue_url: str
  ) -> _SqsQueueLifecycle:
    """Return one generation's stable per-queue clear barrier."""
    with generation.cache_lock:
      lifecycle = generation.queue_lifecycles.get(queue_url)
      if lifecycle is None:
        lifecycle = _SqsQueueLifecycle()
        generation.queue_lifecycles[queue_url] = lifecycle
      return lifecycle

  # QueueBackend implementation
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Send ``item`` to the SQS queue (priority ignored).

    Args:
        queue_name: Name of the queue.
        item: Item to push (bytes).
        priority: Ignored — SQS has no native priority.

    Raises:
        QueueError: If the send fails.
        ValueError: If queue_name contains invalid characters.
    """
    del priority
    if not item:
      raise QueueError(
        "SQS payload must contain at least one raw byte because MessageBody "
        "cannot be empty.",
        queue_name=queue_name,
        operation="push",
      )
    if len(item) > _SQS_MAX_RAW_PAYLOAD_BYTES:
      raise QueueError(
        "SQS payload exceeds the 786,432 raw bytes that fit after base64 "
        "encoding within the 1 MiB MessageBody limit.",
        queue_name=queue_name,
        operation="push",
      )
    _validate_key_name(queue_name, "queue_name")
    with self._lease_generation("push", queue_name=queue_name) as generation:
      if generation is None:  # pragma: no cover - non-token lease is required
        raise AssertionError("current SQS generation lease returned None")
      url = self._queue_url_for_generation(
        generation, queue_name, operation="push"
      )
      lifecycle = self._queue_lifecycle_for_generation(generation, url)
      with lifecycle.operation():
        try:
          body = base64.b64encode(item).decode("ascii")
          generation.client.send_message(QueueUrl=url, MessageBody=body)
        except Exception as e:
          raise QueueError(
            f"Failed to push to SQS queue {queue_name}: {e}",
            queue_name=queue_name,
            operation="push",
          ) from e

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Receive one message from the SQS queue.

    Records ``(queue_url, receipt_handle)`` in ``_last_receipt`` so the
    subsequent legacy :meth:`ack` (called with ``token=None``) deletes
    against the queue this message was popped from. Prefer
    :meth:`pop_with_ack` under ``CONCURRENT_REQUESTS > 1`` — that path
    returns a per-message :class:`_SqsAckToken` so each popped message is
    ackable independently, with no single-slot overwrite.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to long-poll (capped at 20; 0 = short poll).

    Returns:
        The message bytes, or None if no message arrived.

    Raises:
        QueueError: If the receive fails.
        ValueError: If queue_name contains invalid characters.
    """
    url, body, receipt, _epoch, _token = self._receive(
      queue_name, timeout, record_legacy=True
    )
    if body is None:
      return None
    # Track the URL the message arrived FROM, so legacy ack deletes the
    # right queue (round-2 C3 fix). receipt is non-None when body is non-None.
    # receipt is non-None when body is non-None (invariant from _receive);
    # bandit B101 accepted — invariant check, not a security control.
    assert receipt is not None  # noqa: S101  # nosec B101
    return body

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, _SqsAckToken | None]:
    """Pop an item together with a :class:`_SqsAckToken`.

    SQS ``ReceiptHandle`` is natively per-message, so the token carries the
    handle and the source queue URL — :meth:`ack` then
    ``delete_message(QueueUrl=token.queue_url, ReceiptHandle=token.receipt_handle)``
    the specific message, correct under ``CONCURRENT_REQUESTS > 1``. The
    token is also added to the diagnostic ``_in_flight`` set (mirrors
    RabbitMQ's ``_in_flight_tags``) so popped-but-unacked messages are
    observable for leak detection.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to long-poll (capped at 20; 0 = short poll).

    Returns:
        ``(body, token)`` where ``token`` is a :class:`_SqsAckToken`, or
        ``(None, None)`` when the queue is empty.

    Raises:
        QueueError: If the receive fails.
        ValueError: If queue_name contains invalid characters.
    """
    _url, body, _receipt, _epoch, token = self._receive(
      queue_name, timeout, issue_token=True
    )
    if body is None or token is None:
      return (None, None)
    return (body, token)

  def _track_in_flight(self, token: _SqsAckToken) -> None:
    """Add ``token`` to the diagnostic in-flight set, bounded.

    R14-E: the in-flight set is diagnostic (SQS acks each message
    independently via ``delete_message(ReceiptHandle)``; ack correctness
    lives in the broker). It grows one entry per unacked pop, so a
    long-running process with slow acks would grow it unbounded. We cap
    at :data:`_MAX_IN_FLIGHT` and warn-once on overflow. The POP itself
    is never dropped — the caller still receives the message and the
    broker still tracks the receipt handle for ack.

    Args:
        token: The :class:`_SqsAckToken` to track.
    """
    if token.queue_epoch is not None:
      with self._lease_generation(
        "track token", generation_key=token.generation_key
      ) as generation:
        if generation is None:
          return
        lifecycle = self._queue_lifecycle_for_generation(
          generation, token.queue_url
        )
        with lifecycle.operation() as current_epoch:
          if token.queue_epoch != current_epoch:
            return
          self._add_in_flight(token)
      return
    self._add_in_flight(token)

  def _add_in_flight(self, token: _SqsAckToken) -> None:
    """Add a current token to the bounded diagnostic set."""
    with self._in_flight_lock:
      if len(self._in_flight) < _MAX_IN_FLIGHT:
        self._in_flight.add(token)
        return
      if not self._in_flight_overflow_warned:
        self._in_flight_overflow_warned = True
        logger.warning(
          "SQS in-flight ack-token set at cap (%d) — further unacked pops "
          "will not be tracked in the diagnostic set. This indicates slow "
          "acks or an ack leak; the broker still tracks receipt handles "
          "so ack correctness is unaffected.",
          _MAX_IN_FLIGHT,
        )

  def _legacy_receipt_snapshot(
    self,
  ) -> tuple[tuple[str, str], int | None, object | None] | None:
    """Return the coherent legacy receipt slot under its state lock."""
    with self._in_flight_lock:
      if self._last_receipt is None:
        return None
      return (
        self._last_receipt,
        self._last_receipt_epoch,
        self._last_receipt_generation_key,
      )

  def _legacy_receipt_is_current(
    self,
    receipt: tuple[str, str],
    epoch: int | None,
    generation_key: object | None,
  ) -> bool:
    """Return whether all fields still identify the captured legacy slot."""
    with self._in_flight_lock:
      return (
        self._last_receipt == receipt
        and self._last_receipt_epoch == epoch
        and self._last_receipt_generation_key is generation_key
      )

  def _clear_legacy_receipt_if_current(
    self,
    receipt: tuple[str, str],
    epoch: int | None,
    generation_key: object | None,
  ) -> bool:
    """Compare-and-clear exactly one captured legacy receipt generation."""
    with self._in_flight_lock:
      if (
        self._last_receipt != receipt
        or self._last_receipt_epoch != epoch
        or self._last_receipt_generation_key is not generation_key
      ):
        return False
      self._last_receipt = None
      self._last_receipt_epoch = None
      self._last_receipt_generation_key = None
      return True

  def _settle_token(self, token: _SqsAckToken, *, action: str) -> None:
    """Settle ``token`` once, restoring it to retryable state on failure."""

    def broker_operation() -> str:
      with self._lease_generation(
        action, generation_key=token.generation_key
      ) as generation:
        if generation is None:
          return "stale"
        lifecycle = self._queue_lifecycle_for_generation(
          generation, token.queue_url
        )
        with lifecycle.operation() as current_epoch:
          if (
            token.queue_epoch is not None
            and token.queue_epoch != current_epoch
          ):
            return "cleared"
          try:
            if action == "ack":
              generation.client.delete_message(
                QueueUrl=token.queue_url,
                ReceiptHandle=token.receipt_handle,
              )
            elif action == "nack":
              generation.client.change_message_visibility(
                QueueUrl=token.queue_url,
                ReceiptHandle=token.receipt_handle,
                VisibilityTimeout=0,
              )
            else:  # pragma: no cover - private helper has two fixed callers
              raise ValueError(
                f"Unsupported SQS settlement action: {action}"
              )
          except Exception as e:
            raise QueueError(
              f"Failed to {action} SQS message.", operation=action
            ) from e
          return "acked" if action == "ack" else "nacked"

    token._settle(broker_operation)
    with self._in_flight_lock:
      self._in_flight.discard(token)
      if (
        self._last_receipt == (token.queue_url, token.receipt_handle)
        and self._last_receipt_epoch == token.queue_epoch
        and self._last_receipt_generation_key is token.generation_key
      ):
        self._last_receipt = None
        self._last_receipt_epoch = None
        self._last_receipt_generation_key = None

  def _receive(
    self,
    queue_name: str,
    timeout: float,
    *,
    record_legacy: bool = False,
    issue_token: bool = False,
  ) -> tuple[str, bytes | None, str | None, int, _SqsAckToken | None]:
    """Fetch one message from ``queue_name``; shared by pop and pop_with_ack.

    Args:
        queue_name: Name of the queue (validated here).
        timeout: Seconds to long-poll (capped at 20; 0 = short poll).

    Returns:
        ``(queue_url, body, receipt_handle, queue_epoch, token)``. ``body``
        and ``receipt_handle`` are both ``None`` when the queue is empty.
        ``token`` is populated only when ``issue_token`` is true.

    Raises:
        QueueError: If the receive fails at the SQS layer.
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    with self._lease_generation("pop", queue_name=queue_name) as generation:
      if generation is None:  # pragma: no cover - non-token lease is required
        raise AssertionError("current SQS generation lease returned None")
      url = self._queue_url_for_generation(
        generation, queue_name, operation="pop"
      )
      lifecycle = self._queue_lifecycle_for_generation(generation, url)
      with lifecycle.operation() as epoch:
        try:
          wait = min(math.ceil(timeout), _MAX_WAIT_SECONDS) if timeout > 0 else 0
          resp = generation.client.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait,
            VisibilityTimeout=generation.snapshot.visibility_timeout,
          )
        except Exception as e:
          raise QueueError(
            f"Failed to pop from SQS queue {queue_name}: {e}",
            queue_name=queue_name,
            operation="pop",
          ) from e
        messages = resp.get("Messages") or []
        if not messages:
          return (url, None, None, epoch, None)
        msg = messages[0]
        receipt = msg.get("ReceiptHandle")
        if not isinstance(receipt, str) or not receipt:
          raise QueueError(
            f"Malformed SQS message in queue {queue_name}: missing ReceiptHandle",
            queue_name=queue_name,
            operation="pop",
          )
        raw_body = msg.get("Body")
        try:
          if not isinstance(raw_body, str) or not raw_body:
            raise ValueError("message body is missing or empty")
          body = base64.b64decode(raw_body, validate=True)
        except (binascii.Error, TypeError, ValueError) as e:
          # This exact body cannot become valid on retry. Best-effort deletion
          # terminates the poison delivery; failure leaves normal redrive intact.
          try:
            generation.client.delete_message(
              QueueUrl=url, ReceiptHandle=receipt
            )
          except Exception:  # noqa: BLE001 - preserve the decode failure below
            logger.exception(
              "Failed to delete malformed SQS message from queue %r",
              queue_name,
            )
          raise QueueError(
            f"Invalid base64 body in SQS queue {queue_name}: {e}",
            queue_name=queue_name,
            operation="pop",
          ) from e
        if record_legacy:
          with self._in_flight_lock:
            self._last_receipt = (url, receipt)
            self._last_receipt_epoch = epoch
            self._last_receipt_generation_key = generation.key
        token: _SqsAckToken | None = None
        if issue_token:
          token = _SqsAckToken(
            queue_url=url,
            receipt_handle=receipt,
            generation_key=generation.key,
            queue_epoch=epoch,
          )
          self._add_in_flight(token)
        return (url, body, receipt, epoch, token)

  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Delete a popped message so it isn't redelivered.

    With a ``token`` (the scheduler path under ``CONCURRENT_REQUESTS > 1``):
    ``delete_message(QueueUrl=token.queue_url, ReceiptHandle=token.receipt_handle)``
    the specific message and remove it from the diagnostic in-flight set.
    Order-independent — ack the right message regardless of pop/ack
    interleaving. The ``queue_url`` is carried on the token so multi-queue
    correctness (C3) is preserved without consulting ``queue_name``.

    Without a ``token`` (legacy single-pop caller): delete the tracked
    ``_last_receipt`` (set by :meth:`pop`). Only correct for
    ``CONCURRENT_REQUESTS=1`` — kept for backward compatibility with
    external callers that pop() then ack() without threading the token
    through.

    Stale-handle (visibility-timeout-expired) AWS errors raise
    :class:`QueueError`; at-least-once is preserved by SQS re-delivery, so
    the error is NOT swallowed (matches Kafka's raise-on-commit-failure).

    Args:
        queue_name: Name of the queue (unused when ``token`` is provided;
            kept for interface symmetry).
        token: A :class:`_SqsAckToken` from :meth:`pop_with_ack`, or
            ``None`` to ack the last-popped message (legacy).

    Raises:
        QueueError: If the delete fails at the SQS layer.
    """
    del queue_name
    if isinstance(token, _SqsAckToken):
      self._settle_token(token, action="ack")
      return
    if token is not None:
      # A token from another backend/generation must never fall through and
      # accidentally acknowledge the legacy last-receipt slot.
      return
    self._settle_legacy_receipt(action="ack")

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Make a popped message immediately visible for re-delivery.

    Calls ``change_message_visibility(..., VisibilityTimeout=0)`` for the
    specific token or legacy last receipt, then removes local tracking. This
    avoids waiting the full processing visibility timeout after an explicit
    negative acknowledgement.

    Args:
        queue_name: The queue name (unused; the source URL is tracked).
        token: A :class:`_SqsAckToken` from :meth:`pop_with_ack`, or
            ``None`` to nack the last-popped message (legacy).
    """
    del queue_name
    if isinstance(token, _SqsAckToken):
      self._settle_token(token, action="nack")
      return
    if token is not None:
      return
    self._settle_legacy_receipt(action="nack")

  def _settle_legacy_receipt(self, *, action: str) -> None:
    """Settle the legacy single receipt without crossing a clear epoch."""
    legacy_snapshot = self._legacy_receipt_snapshot()
    if legacy_snapshot is None:
      return
    last_receipt, receipt_epoch, generation_key = legacy_snapshot
    url, receipt = last_receipt
    with self._lease_generation(
      action, generation_key=generation_key
    ) as generation:
      if generation is None:
        self._clear_legacy_receipt_if_current(
          last_receipt, receipt_epoch, generation_key
        )
        return
      lifecycle = self._queue_lifecycle_for_generation(generation, url)
      with lifecycle.operation() as current_epoch:
        if not self._legacy_receipt_is_current(
          last_receipt, receipt_epoch, generation_key
        ):
          return
        if (
          receipt_epoch is not None and receipt_epoch != current_epoch
        ):
          self._clear_legacy_receipt_if_current(
            last_receipt, receipt_epoch, generation_key
          )
          return
        try:
          if action == "ack":
            generation.client.delete_message(
              QueueUrl=url, ReceiptHandle=receipt
            )
          else:
            generation.client.change_message_visibility(
              QueueUrl=url,
              ReceiptHandle=receipt,
              VisibilityTimeout=0,
            )
        except Exception as e:
          raise QueueError(
            f"Failed to {action} SQS message: {e}", operation=action
          ) from e
        else:
          self._clear_legacy_receipt_if_current(
            last_receipt, receipt_epoch, generation_key
          )

  def queue_len(self, queue_name: str) -> int:
    """Return the approximate total pending message count for the queue.

    Args:
        queue_name: Name of the queue.

    Returns:
        Sum of visible, in-flight, and delayed approximate message counts
        (eventually consistent).

    Raises:
        ValueError: If queue_name contains invalid characters.
        QueueError: If the SQS ``get_queue_attributes`` call fails or its
            response omits/malforms a requested count (R-sqs-qlen). Returning
            ``0`` would conflate an empty queue with a backend failure or an
            incomplete response, which can trigger premature idle/CloseSpider
            and lose the backpressure signal. Mirrors the Redis R-qlen contract.
    """
    _validate_key_name(queue_name, "queue_name")
    with self._lease_generation(
      "queue_len", queue_name=queue_name
    ) as generation:
      if generation is None:  # pragma: no cover - non-token lease is required
        raise AssertionError("current SQS generation lease returned None")
      url = self._queue_url_for_generation(
        generation, queue_name, operation="queue_len"
      )
      lifecycle = self._queue_lifecycle_for_generation(generation, url)
      with lifecycle.operation():
        try:
          resp = generation.client.get_queue_attributes(
            QueueUrl=url, AttributeNames=list(_QUEUE_DEPTH_ATTRIBUTES)
          )
          attributes = resp["Attributes"]
          return sum(
            int(attributes[name]) for name in _QUEUE_DEPTH_ATTRIBUTES
          )
        except Exception as e:
          # Do NOT swallow to 0: the scheduler trusts this value as a pending-work
          # signal, so a false empty result can trigger premature spider closure.
          raise QueueError(
            str(e), queue_name=queue_name, operation="queue_len"
          ) from e

  def _retire_queue_deliveries(
    self, queue_url: str, generation_key: object
  ) -> None:
    """Remove local receipt diagnostics invalidated by a destructive clear."""
    with self._in_flight_lock:
      retired = {
        token
        for token in self._in_flight
        if token.queue_url == queue_url
        and token.generation_key is generation_key
      }
      self._in_flight.difference_update(retired)
      if (
        self._last_receipt is not None
        and self._last_receipt[0] == queue_url
        and self._last_receipt_generation_key is generation_key
      ):
        self._last_receipt = None
        self._last_receipt_epoch = None
        self._last_receipt_generation_key = None

  def clear_queue(self, queue_name: str) -> None:
    """Purge the SQS queue and wait out AWS's destructive async window.

    AWS documents that PurgeQueue can keep deleting old and newly sent
    messages for up to 60 seconds. This method holds a per-queue lifecycle
    lock and does not report success (or a possibly ambiguous failure) until
    that full window has elapsed. Push/pop/depth/settlement operations for the
    same physical queue wait behind the barrier; unrelated queues remain live.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
        QueueError: If the purge fails at the SQS layer, after conservatively
            waiting out the window in case the request reached AWS.
    """
    _validate_key_name(queue_name, "queue_name")
    with self._lease_generation(
      "clear_queue", queue_name=queue_name
    ) as generation:
      if generation is None:  # pragma: no cover - non-token lease is required
        raise AssertionError("current SQS generation lease returned None")
      url = self._queue_url_for_generation(
        generation, queue_name, operation="clear_queue"
      )
      lifecycle = self._queue_lifecycle_for_generation(generation, url)
      with lifecycle.destructive_operation():
        self._retire_queue_deliveries(url, generation.key)
        purge_error: Exception | None = None
        try:
          generation.client.purge_queue(QueueUrl=url)
        except Exception as e:
          # The request may have reached AWS even when the response was lost.
          # Fence operations for the full window before surfacing the ambiguity.
          purge_error = e
        # Start the full safety interval after the RPC returns. Service-side
        # acceptance can happen at any point during a slow request, so subtracting
        # client-call latency would provide a weaker boundary.
        time.sleep(_SQS_PURGE_WINDOW_SECONDS)
        if purge_error is not None:
          raise QueueError(
            f"Failed to purge SQS queue {queue_name}.",
            queue_name=queue_name,
            operation="clear_queue",
          ) from purge_error


class _swallow:
  """Context manager that swallows cleanup-path errors (close() etc.)."""

  def __enter__(self) -> _swallow:
    return self

  def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
    if exc_type is None:
      return False
    # R-swallow: suppress only regular cleanup Exceptions -- NEVER BaseException
    # (KeyboardInterrupt / SystemExit / GeneratorExit). Pre-fix this returned
    # True for any non-None exc_type, trapping Ctrl+C during close()/disconnect
    # (the operator's shutdown signal disappeared into a debug log).
    if not isinstance(exc, Exception):
      return False
    logger.debug("Suppressed SQS cleanup error: %s", exc)
    return True
