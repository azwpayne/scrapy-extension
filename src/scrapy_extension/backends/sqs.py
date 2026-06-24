"""Amazon SQS backend (queue-only) — subsystem ③.

Implements QueueBackend using Amazon SQS Standard queues. MessageBody carries
base64-encoded item bytes (SQS MessageBody is a string). Ack = delete_message
(removes from the queue so it isn't redelivered); nack is a no-op (the
visibility timeout expires and the message is redelivered). Priority is
ignored (SQS has no native priority queue).

boto3 API (stable, well-known):
- ``boto3.client("sqs", region_name=, endpoint_url=, aws_access_key_id=, ...)``
- ``client.get_queue_url(QueueName=)`` / ``create_queue(QueueName=)``
- ``client.send_message(QueueUrl=, MessageBody=, DelaySeconds=)``
- ``client.receive_message(QueueUrl=, MaxNumberOfMessages=1, WaitTimeSeconds=, VisibilityTimeout=)``
- ``client.delete_message(QueueUrl=, ReceiptHandle=)``
- ``client.purge_queue(QueueUrl=)``
- ``client.get_queue_attributes(QueueUrl=, AttributeNames=["ApproximateNumberOfMessages"])``
- ``client.close()``
"""

from __future__ import annotations

import base64
import logging
from typing import TYPE_CHECKING, Any

try:
  import boto3
except ImportError as e:
  raise ImportError(
    "SQS backend requires 'boto3'. Install with: pip install scrapy-extension[sqs]"
  ) from e

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
  _validate_key_name,
  secret_value,
)
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings import SqsMode

if TYPE_CHECKING:
  from scrapy_extension.settings import SqsSettings

logger = logging.getLogger(__name__)

# SQS caps WaitTimeSeconds at 20.
_MAX_WAIT_SECONDS = 20


class SqsBackend(Backend, QueueBackend):
  """SQS backend (queue-only) with Standard-queue work semantics.

  Each queue maps to an SQS queue named ``<prefix><queue_name>``. Push base64-
  encodes the item into MessageBody; pop decodes it and tracks the receipt
  handle for ack. queue_len reads ApproximateNumberOfMessages; clear_queue
  purges.

  Attributes:
      config: SqsSettings instance.
      _client: The boto3 SQS client (None until connected).
      _queue_urls: Per-queue cached QueueUrl values.
      _last_receipt: ReceiptHandle of the last-popped message (for ack).
  """

  def __init__(self, config: SqsSettings) -> None:
    self.config = config
    self._client: Any = None
    self._queue_urls: dict[str, str] = {}
    self._last_receipt: str | None = None

  def connect(self) -> None:
    """Create the boto3 SQS client.

    Raises:
        BackendConnectionError: If the client cannot be created.
    """
    if self.config.mode not in (SqsMode.STANDALONE, SqsMode.CLOUD):
      raise BackendConnectionError(
        f"Unsupported SQS mode: {self.config.mode}", backend_type="sqs"
      )
    try:
      kwargs: dict[str, Any] = {"region_name": self.config.region_name}
      if self.config.endpoint_url:
        kwargs["endpoint_url"] = self.config.endpoint_url
      if self.config.aws_access_key_id:
        kwargs["aws_access_key_id"] = secret_value(self.config.aws_access_key_id)
        kwargs["aws_secret_access_key"] = secret_value(
          self.config.aws_secret_access_key
        )
      self._client = boto3.client("sqs", **kwargs)
      logger.debug("Connected to SQS (%s, %s)", self.config.mode.value, self.config.region_name)
    except Exception as e:
      raise BackendConnectionError(
        f"Failed to create SQS client: {e}", backend_type="sqs"
      ) from e

  def disconnect(self) -> None:
    """Close the SQS client."""
    if self._client is not None:
      with _swallow():
        self._client.close()
      self._client = None
    self._queue_urls.clear()
    self._last_receipt = None

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

  def _queue_url(self, queue_name: str) -> str:
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
    name = f"{self.config.queue_name_prefix}{queue_name}"
    try:
      try:
        resp = self._client.get_queue_url(QueueName=name)
      except Exception:
        resp = self._client.create_queue(QueueName=name)
      url = resp["QueueUrl"]
    except Exception as e:
      raise QueueError(
        f"Failed to resolve SQS queue URL for {queue_name}: {e}",
        queue_name=queue_name,
        operation="push",
      ) from e
    self._queue_urls[queue_name] = url
    return url

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
    url = self._queue_url(queue_name)
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

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to long-poll (capped at 20; 0 = short poll).

    Returns:
        The message bytes, or None if no message arrived.

    Raises:
        QueueError: If the receive fails.
        ValueError: If queue_name contains invalid characters.
    """
    try:
      url = self._queue_url(queue_name)
      wait = min(int(timeout), _MAX_WAIT_SECONDS) if timeout > 0 else 0
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
      return None
    msg = messages[0]
    self._last_receipt = msg.get("ReceiptHandle")
    body = msg.get("Body", "")
    return base64.b64decode(body)

  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Delete the last-popped message so it isn't redelivered.

    ``token`` is accepted for interface compatibility with the concurrency-
    correct ack path (see QueueBackend.pop_with_ack) but not yet used — SQS
    still tracks a single ``_last_receipt`` slot. The full in-flight-set
    fix for SQS is a follow-up; until then pin ``CONCURRENT_REQUESTS=1``
    for strict at-least-once.

    Args:
        queue_name: The queue name.
        token: Unused (accepted for signature compatibility).

    Raises:
        QueueError: If the delete fails.
    """
    del queue_name, token
    if self._client is None or self._last_receipt is None:
      return
    url = next(iter(self._queue_urls.values()), None)
    if url is None:
      return
    try:
      self._client.delete_message(QueueUrl=url, ReceiptHandle=self._last_receipt)
    except Exception as e:
      raise QueueError(f"Failed to ack SQS message: {e}", operation="ack") from e
    finally:
      self._last_receipt = None

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """No-op: SQS redelivers an unacked message after the visibility timeout.

    Args:
        queue_name: The queue name.
        token: Unused (accepted for signature compatibility).
    """
    del queue_name, token
    self._last_receipt = None

  def queue_len(self, queue_name: str) -> int:
    """Return ApproximateNumberOfMessages for the queue.

    Args:
        queue_name: Name of the queue.

    Returns:
        Approximate message count (eventually consistent).

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    try:
      url = self._queue_url(queue_name)
      resp = self._client.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
      )
    except Exception:
      return 0
    return int(resp.get("Attributes", {}).get("ApproximateNumberOfMessages", 0))

  def clear_queue(self, queue_name: str) -> None:
    """Purge the SQS queue.

    Args:
        queue_name: Name of the queue.

    Raises:
        ValueError: If queue_name contains invalid characters.
    """
    try:
      url = self._queue_url(queue_name)
      self._client.purge_queue(QueueUrl=url)
    except Exception as e:
      logger.warning("Failed to purge SQS queue %s: %s", queue_name, e)


class _swallow:
  """Context manager that swallows cleanup-path errors (close() etc.)."""

  def __enter__(self) -> _swallow:
    return self

  def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
    if exc_type is None:
      return False
    logger.debug("Suppressed SQS cleanup error: %s", exc)
    return True
