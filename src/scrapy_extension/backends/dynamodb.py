"""Amazon DynamoDB backend (StorageBackend) — NoSQL KV (subsystem ③).

Implements StorageBackend using a DynamoDB table (keyed by ``pk``). TTL is
application-level: items with a TTL carry an ``expire_at`` epoch attribute,
checked on read (expired items are deleted and reported missing). The table
is auto-created on connect if missing (PAY_PER_REQUEST, hash key ``pk``).

boto3 resource API (stable):
- ``boto3.resource("dynamodb", region_name=, endpoint_url=, ...)``
- ``resource.Table(name)`` / ``resource.create_table(...)``
- ``table.load()`` / ``table.wait_until_exists()``
- ``table.put_item(Item=)`` / ``get_item(Key=)`` / ``delete_item(Key=, ReturnValues=)``
- ``table.scan()`` / ``table.batch_writer()``
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

try:
  import boto3
except ImportError as e:
  raise ImportError(
    "DynamoDB backend requires 'boto3'. "
    "Install with: pip install scrapy-extension[dynamodb]"
  ) from e

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  StorageBackend,
  _validate_key_name,
  secret_value,
)
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.settings import DynamoDBMode

if TYPE_CHECKING:
  from scrapy_extension.settings import DynamoDBSettings

logger = logging.getLogger(__name__)


class DynamoDBBackend(Backend, StorageBackend):
  """DynamoDB storage backend (KV with application-level TTL).

  Stores values under a partition key ``pk``. Items may carry an ``expire_at``
  epoch attribute; reads treat expired items as missing and delete them. The
  table is created on connect if it does not exist (PAY_PER_REQUEST).

  Attributes:
      config: DynamoDBSettings instance.
      _resource: The boto3 dynamodb resource (None until connected).
      _table: The Table handle.
  """

  def __init__(self, config: DynamoDBSettings) -> None:
    self.config = config
    self._resource: Any = None
    self._table: Any = None

  def connect(self) -> None:
    """Create the resource and ensure the table exists.

    Raises:
        BackendConnectionError: If the resource/table cannot be set up.
    """
    if self.config.mode not in (DynamoDBMode.STANDALONE, DynamoDBMode.CLOUD):
      raise BackendConnectionError(
        f"Unsupported DynamoDB mode: {self.config.mode}",
        backend_type="dynamodb",
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
      self._resource = boto3.resource("dynamodb", **kwargs)
      self._table = self._resource.Table(self.config.table_name)
      try:
        self._table.load()
      except Exception:
        self._table = self._resource.create_table(
          TableName=self.config.table_name,
          KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
          AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"}
          ],
          BillingMode="PAY_PER_REQUEST",
        )
        self._table.wait_until_exists()
      logger.debug("Connected to DynamoDB table %s", self.config.table_name)
    except Exception as e:
      raise BackendConnectionError(
        f"Failed to connect to DynamoDB: {e}", backend_type="dynamodb"
      ) from e

  def disconnect(self) -> None:
    """Release the resource/table handles (boto3 has no explicit close)."""
    self._table = None
    self._resource = None

  def is_connected(self) -> bool:
    """Return True if the table handle has been created."""
    return self._table is not None

  def ping(self) -> bool:
    """Health check via table.load()."""
    if self._table is None:
      return False
    try:
      self._table.load()
      return True
    except Exception:
      return False

  @property
  def backend_type(self) -> BackendType:
    """Return BackendType.DYNAMODB."""
    return BackendType.DYNAMODB

  def _is_expired(self, item: dict[str, Any]) -> bool:
    """Return True if the item has an expire_at that has passed."""
    expire_at = item.get("expire_at")
    return expire_at is not None and float(expire_at) <= time.time()

  # StorageBackend implementation
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store ``data`` under ``key`` with optional TTL.

    Args:
        key: Storage key.
        data: Data to store (bytes).
        ttl: Optional time-to-live in seconds (stored as expire_at epoch).

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    item: dict[str, Any] = {"pk": key, "value": data}
    if ttl:
      item["expire_at"] = time.time() + ttl
    try:
      self._table.put_item(Item=item)
    except Exception as e:
      logger.warning("Failed to store key %s: %s", key, e)

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key (None if missing or expired).

    Args:
        key: Storage key.

    Returns:
        Stored data, or None if not found / expired.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      resp = self._table.get_item(Key={"pk": key})
    except Exception as e:
      logger.warning("Failed to retrieve key %s: %s", key, e)
      return None
    item = resp.get("Item")
    if not item:
      return None
    if self._is_expired(item):
      with _swallow():
        self._table.delete_item(Key={"pk": key})
      return None
    value = item.get("value")
    return bytes(value) if isinstance(value, (bytes, bytearray)) else None

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if the key existed and was deleted, False otherwise.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      resp = self._table.delete_item(Key={"pk": key}, ReturnValues="ALL_OLD")
      return "Attributes" in resp
    except Exception:
      return False

  def exists(self, key: str) -> bool:
    """Check if a key exists and is not expired.

    Args:
        key: Storage key.

    Returns:
        True if the key exists and is current.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      resp = self._table.get_item(Key={"pk": key})
    except Exception:
      return False
    item = resp.get("Item")
    if not item:
      return False
    return not self._is_expired(item)

  def ttl(self, key: str) -> int | None:
    """Return remaining TTL seconds if the item has expire_at, else None.

    Args:
        key: Storage key.

    Returns:
        Seconds remaining (>= 0), or None if no TTL / not found.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      resp = self._table.get_item(Key={"pk": key})
    except Exception:
      return None
    item = resp.get("Item")
    if not item or "expire_at" not in item:
      return None
    return max(0, int(float(item["expire_at"]) - time.time()))

  def clear_storage(self, prefix: str | None = None) -> None:
    """Best-effort clear via scan + batch delete (prefix not filtered).

    Args:
        prefix: Ignored — DynamoDB scan+delete clears all items in one pass.

    Raises:
        ValueError: If prefix contains invalid characters.
    """
    if prefix:
      _validate_key_name(prefix, "prefix")
    try:
      scan = self._table.scan()
      with self._table.batch_writer() as batch:
        for item in scan.get("Items", []):
          batch.delete_item(Key={"pk": item["pk"]})
    except Exception as e:
      logger.warning("Failed to clear DynamoDB table: %s", e)


class _swallow:
  """Context manager that swallows cleanup-path errors."""

  def __enter__(self) -> _swallow:
    return self

  def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
    if exc_type is None:
      return False
    logger.debug("Suppressed dynamodb cleanup error: %s", exc)
    return True
