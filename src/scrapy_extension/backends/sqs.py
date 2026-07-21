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
    "queue_epoch",
    "queue_url",
    "receipt_handle",
  )

  def __init__(
    self,
    queue_url: str,
    receipt_handle: str,
    *,
    queue_epoch: int | None = None,
  ) -> None:
    """Initialize the token.

    Args:
        queue_url: The QueueUrl the message was popped from.
        receipt_handle: The SQS ReceiptHandle identifying the message.
        queue_epoch: The queue lifecycle epoch that issued the delivery.
    """
    self.queue_url = queue_url
    self.receipt_handle = receipt_handle
    self.queue_epoch = queue_epoch
    self._settlement_lock = threading.Lock()
    self._settlement_state = "pending"

  def _settle(
    self, terminal_state: str, operation: Callable[[], None]
  ) -> bool:
    """Run exactly one terminal broker operation for this delivery.

    The per-token lock remains held across the broker call. A competing ack
    or nack therefore observes either the restored ``pending`` state after a
    failure (and may retry) or the final terminal state after success. It can
    never report a no-op success while another settlement is still uncertain.

    Args:
        terminal_state: State to publish after a successful broker operation.
        operation: Broker operation to execute while the token is claimed.

    Returns:
        True when this call performed and completed the broker operation;
        False when the token was already terminal.
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
    if not isinstance(other, _SqsAckToken):
      return NotImplemented
    return (
      self.queue_url == other.queue_url
      and self.receipt_handle == other.receipt_handle
    )

  def __hash__(self) -> int:
    return hash((self.queue_url, self.receipt_handle))

  def __repr__(self) -> str:
    return (
      f"_SqsAckToken(queue_url={self.queue_url!r}, "
      f"receipt_handle={self.receipt_handle!r})"
    )


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
      _client: The boto3 SQS client (None until connected).
      _queue_urls: Per-queue cached QueueUrl values.
      _in_flight: Diagnostic set of popped-but-unacked tokens.
      _last_receipt: ``(queue_url, receipt_handle)`` of the last-popped msg
          (legacy ``ack(token=None)`` fallback only).
  """

  requires_ack = True
  supports_concurrent_ack = True

  def __init__(self, config: SqsSettings) -> None:
    self.config = config
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

  def connect(self) -> None:
    """Create the boto3 SQS client.

    Raises:
        BackendConnectionError: If the client cannot be created.
    """
    mode = self.config.mode
    region_name = self.config.region_name
    endpoint_url = self.config.endpoint_url
    access_key = self.config.aws_access_key_id
    secret_key = self.config.aws_secret_access_key
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
    key_id, secret = validate_aws_credentials(
      access_key,
      secret_key,
    )
    try:
      kwargs: dict[str, Any] = {"region_name": region_name}
      if endpoint_url is not None:
        kwargs["endpoint_url"] = endpoint_url
      if key_id is not None and secret is not None:
        kwargs["aws_access_key_id"] = _redact(key_id)
        kwargs["aws_secret_access_key"] = _redact(secret)
      self._client = boto3.client("sqs", **kwargs)
      logger.debug("Connected to SQS (%s, %s)", mode.value, region_name)
    except Exception as e:
      raise BackendConnectionError(
        f"Failed to create SQS client: {e}", backend_type="sqs"
      ) from e

  def disconnect(self) -> None:
    """Close the SQS client."""
    with self._queue_lifecycles_lock:
      lifecycles = tuple(self._queue_lifecycles.values())
    for lifecycle in lifecycles:
      with lifecycle.destructive_operation():
        pass
    if self._client is not None:
      with _swallow():
        self._client.close()
      self._client = None
    self._queue_urls.clear()
    with self._in_flight_lock:
      self._in_flight.clear()
      self._last_receipt = None
      self._last_receipt_epoch = None

  def is_connected(self) -> bool:
    """Return True if the client has been created."""
    return self._client is not None

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
    if queue_name in self._queue_urls:
      return self._queue_urls[queue_name]
    name = _physical_queue_name(self.config.queue_name_prefix, queue_name)
    try:
      resp = self._client.get_queue_url(QueueName=name)
    except Exception as lookup_error:
      if not _is_queue_missing(lookup_error):
        raise QueueError(
          f"Failed to resolve SQS queue URL for {queue_name}: {lookup_error}",
          queue_name=queue_name,
          operation=operation,
        ) from lookup_error
      try:
        resp = self._client.create_queue(QueueName=name)
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
    self._queue_urls[queue_name] = url
    return cast(str, url)

  def _queue_lifecycle(self, queue_url: str) -> _SqsQueueLifecycle:
    """Return the stable lifecycle state for a physical SQS queue URL."""
    with self._queue_lifecycles_lock:
      lifecycle = self._queue_lifecycles.get(queue_url)
      if lifecycle is None:
        lifecycle = _SqsQueueLifecycle()
        self._queue_lifecycles[queue_url] = lifecycle
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
    url = self._queue_url(queue_name, operation="push")
    lifecycle = self._queue_lifecycle(url)
    with lifecycle.operation():
      try:
        body = base64.b64encode(item).decode("ascii")
        self._client.send_message(QueueUrl=url, MessageBody=body)
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
      lifecycle = self._queue_lifecycle(token.queue_url)
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

  def _settle_token(self, token: _SqsAckToken, *, action: str) -> None:
    """Settle ``token`` once, restoring it to retryable state on failure."""

    def broker_operation() -> None:
      client = self._client
      if client is None:
        raise QueueError(
          f"Cannot {action} SQS message while backend is disconnected.",
          operation=action,
        )
      try:
        if action == "ack":
          client.delete_message(
            QueueUrl=token.queue_url, ReceiptHandle=token.receipt_handle
          )
        elif action == "nack":
          client.change_message_visibility(
            QueueUrl=token.queue_url,
            ReceiptHandle=token.receipt_handle,
            VisibilityTimeout=0,
          )
        else:  # pragma: no cover - private helper has two fixed callers
          raise ValueError(f"Unsupported SQS settlement action: {action}")
      except QueueError:
        raise
      except Exception as e:
        raise QueueError(
          f"Failed to {action} SQS message.", operation=action
        ) from e

    lifecycle = self._queue_lifecycle(token.queue_url)
    with lifecycle.operation() as current_epoch:
      is_stale = (
        token.queue_epoch is not None and token.queue_epoch != current_epoch
      )
      if is_stale:
        token._settle("cleared", lambda: None)
      else:
        terminal_state = "acked" if action == "ack" else "nacked"
        token._settle(terminal_state, broker_operation)
      with self._in_flight_lock:
        self._in_flight.discard(token)
        if self._last_receipt == (token.queue_url, token.receipt_handle):
          self._last_receipt = None
          self._last_receipt_epoch = None

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
    url = self._queue_url(queue_name, operation="pop")
    lifecycle = self._queue_lifecycle(url)
    with lifecycle.operation() as epoch:
      try:
        wait = min(math.ceil(timeout), _MAX_WAIT_SECONDS) if timeout > 0 else 0
        resp = self._client.receive_message(
          QueueUrl=url,
          MaxNumberOfMessages=1,
          WaitTimeSeconds=wait,
          VisibilityTimeout=self.config.visibility_timeout,
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
          self._client.delete_message(QueueUrl=url, ReceiptHandle=receipt)
        except Exception:  # noqa: BLE001 - preserve the decode failure below
          logger.exception(
            "Failed to delete malformed SQS message from queue %r", queue_name
          )
        raise QueueError(
          f"Invalid base64 body in SQS queue {queue_name}: {e}",
          queue_name=queue_name,
          operation="pop",
        ) from e
      if record_legacy:
        self._last_receipt = (url, receipt)
        self._last_receipt_epoch = epoch
      token: _SqsAckToken | None = None
      if issue_token:
        token = _SqsAckToken(
          queue_url=url, receipt_handle=receipt, queue_epoch=epoch
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
    last_receipt = self._last_receipt
    if last_receipt is None:
      return
    url, receipt = last_receipt
    lifecycle = self._queue_lifecycle(url)
    with lifecycle.operation() as current_epoch:
      if self._last_receipt != last_receipt:
        return
      if (
        self._last_receipt_epoch is not None
        and self._last_receipt_epoch != current_epoch
      ):
        self._last_receipt = None
        self._last_receipt_epoch = None
        return
      client = self._client
      if client is None:
        return
      try:
        if action == "ack":
          client.delete_message(QueueUrl=url, ReceiptHandle=receipt)
        else:
          client.change_message_visibility(
            QueueUrl=url,
            ReceiptHandle=receipt,
            VisibilityTimeout=0,
          )
      except Exception as e:
        raise QueueError(
          f"Failed to {action} SQS message: {e}", operation=action
        ) from e
      else:
        self._last_receipt = None
        self._last_receipt_epoch = None

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
    url = self._queue_url(queue_name, operation="queue_len")
    lifecycle = self._queue_lifecycle(url)
    with lifecycle.operation():
      try:
        resp = self._client.get_queue_attributes(
          QueueUrl=url, AttributeNames=list(_QUEUE_DEPTH_ATTRIBUTES)
        )
        attributes = resp["Attributes"]
        return sum(int(attributes[name]) for name in _QUEUE_DEPTH_ATTRIBUTES)
      except Exception as e:
        # Do NOT swallow to 0: the scheduler trusts this value as a pending-work
        # signal, so a false empty result can trigger premature spider closure.
        raise QueueError(
          str(e), queue_name=queue_name, operation="queue_len"
        ) from e

  def _retire_queue_deliveries(self, queue_url: str) -> None:
    """Remove local receipt diagnostics invalidated by a destructive clear."""
    with self._in_flight_lock:
      retired = {
        token for token in self._in_flight if token.queue_url == queue_url
      }
      self._in_flight.difference_update(retired)
      if self._last_receipt is not None and self._last_receipt[0] == queue_url:
        self._last_receipt = None
        self._last_receipt_epoch = None

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
    url = self._queue_url(queue_name, operation="clear_queue")
    lifecycle = self._queue_lifecycle(url)
    with lifecycle.destructive_operation():
      self._retire_queue_deliveries(url)
      purge_error: Exception | None = None
      try:
        self._client.purge_queue(QueueUrl=url)
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
