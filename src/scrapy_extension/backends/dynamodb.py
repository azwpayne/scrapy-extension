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
import math
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
  import boto3
except ImportError as e:
  if not _is_missing_optional_dependency(e, "boto3"):
    raise
  raise ImportError(
    "DynamoDB backend requires 'boto3'. "
    "Install with: pip install scrapy-extension[dynamodb]"
  ) from e

from scrapy_extension.backends._redaction import _redact
from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  StorageBackend,
  _validate_key_name,
  _validate_ttl,
  secret_value,
)
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import DynamoDBMode

if TYPE_CHECKING:
  from scrapy_extension.settings import DynamoDBSettings

logger = logging.getLogger(__name__)

# DynamoDB ClientError codes used while establishing the table. A
# ResourceNotFoundException means the TABLE is missing, never that an item is
# absent (missing items are successful responses without Item/Attributes).
# Runtime storage operations must therefore surface this code as StorageError.
_DDB_NOT_FOUND_CODES = frozenset({"ResourceNotFoundException"})
_DDB_INUSE_CODES = frozenset({"ResourceInUseException"})
_DDB_MAX_PARTITION_KEY_BYTES = 2_048
_DDB_MAX_ITEM_BYTES = 400 * 1_024
_MISSING = object()


def _validate_partition_key(key: str) -> None:
  """Validate the package key grammar and DynamoDB's physical byte ceiling."""
  _validate_key_name(key, "key")
  key_size = len(key.encode("utf-8"))
  if key_size > _DDB_MAX_PARTITION_KEY_BYTES:
    raise ValueError(
      "DynamoDB partition key exceeds 2,048 UTF-8 bytes "
      f"({key_size} bytes)."
    )


def _number_size_upper_bound(value: int) -> int:
  """Return a safe DynamoDB byte estimate for the positive integer value."""
  digits = len(str(abs(value)))
  return (digits + 1) // 2 + 1


def _validate_item_size(key: str, data: bytes, expire_at: int | None) -> None:
  """Reject items beyond DynamoDB's 400 KiB names-plus-values limit."""
  item_size = len("pk") + len(key.encode("utf-8")) + len("value") + len(data)
  if expire_at is not None:
    item_size += len("expire_at") + _number_size_upper_bound(expire_at)
  if item_size > _DDB_MAX_ITEM_BYTES:
    raise ValueError(
      f"DynamoDB item is {item_size} bytes; the maximum is 400 KiB "
      "including attribute names and values."
    )


def _is_resource_not_found(exc: BaseException) -> bool:
  """Return True if ``exc`` is a DynamoDB ClientError for a missing resource.

  Works against both ``botocore.exceptions.ClientError`` and the test-suite's
  plain ``Exception`` carrying a ``response`` dict (the ``boto3`` module is
  mocked in tests, so importing ``botocore.exceptions`` is not reliable).
  """
  response = getattr(exc, "response", None)
  if not isinstance(response, dict):
    return False
  err = response.get("Error")
  if not isinstance(err, dict):
    return False
  return err.get("Code") in _DDB_NOT_FOUND_CODES


def _is_resource_in_use(exc: BaseException) -> bool:
  """Return True if ``exc`` is a DynamoDB ``ResourceInUseException``.

  Raised by ``create_table`` when another worker has already started creating
  the table (concurrent boot race, e.g. k8s pod rollout). Mirrors
  :func:`_is_resource_not_found` so the same test-suite ClientError stand-in
  works against the mocked ``boto3``.
  """
  response = getattr(exc, "response", None)
  if not isinstance(response, dict):
    return False
  err = response.get("Error")
  if not isinstance(err, dict):
    return False
  return err.get("Code") in _DDB_INUSE_CODES


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
      raise ConfigurationError(
        f"Unsupported DynamoDB mode: {self.config.mode}",
        setting_name="mode",
        setting_value=self.config.mode,
      )
    # SEC-7: AWS credentials must be both-or-neither (see SqsBackend.connect).
    key_id = secret_value(self.config.aws_access_key_id)
    secret = secret_value(self.config.aws_secret_access_key)
    has_key = bool(key_id)
    has_secret = bool(secret)
    if has_key != has_secret:
      missing = "aws_secret_access_key" if has_key else "aws_access_key_id"
      present = "aws_access_key_id" if has_key else "aws_secret_access_key"
      raise ConfigurationError(
        "AWS credentials must be both-or-neither: "
        f"{present} is set but {missing} is empty. "
        "Set both explicitly, or leave both unset to use the boto3 "
        "default credential chain (env / IMDS / config files).",
        setting_name=missing,
      )
    try:
      kwargs: dict[str, Any] = {"region_name": self.config.region_name}
      if self.config.endpoint_url:
        kwargs["endpoint_url"] = self.config.endpoint_url
      if has_key and has_secret:
        kwargs["aws_access_key_id"] = _redact(key_id)
        kwargs["aws_secret_access_key"] = _redact(secret)
      self._resource = boto3.resource("dynamodb", **kwargs)
      self._table = self._resource.Table(self.config.table_name)
      try:
        self._table.load()
      except Exception as e:
        # Only a genuine "table not found" triggers create_table — every other
        # error (throttle, network, auth, validation) MUST propagate (#31), or
        # a transient blip spuriously creates a conflicting table.
        if not _is_resource_not_found(e):
          raise
        try:
          self._table = self._resource.create_table(
            TableName=self.config.table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
              {"AttributeName": "pk", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
          )
        except Exception as create_err:
          # Concurrent boot race (e.g. k8s pod rollout): another worker already
          # started creating this table — ResourceInUseException. Reattach to
          # the existing table and wait. Any other error propagates (#31): a
          # transient blip must not mask as "created".
          if not _is_resource_in_use(create_err):
            raise
          self._table = self._resource.Table(self.config.table_name)
        self._table.wait_until_exists()
      logger.debug("Connected to DynamoDB table %s", self.config.table_name)
    except Exception as e:
      self._table = None
      self._resource = None
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

  @staticmethod
  def _response_item(
    response: Any, operation: str, key: str
  ) -> dict[str, Any] | None:
    """Return a structurally valid item or raise the storage error contract."""
    if not isinstance(response, dict):
      msg = "DynamoDB returned a non-mapping item response"
      raise StorageError(msg, operation=operation, key=key)
    item = response.get("Item", _MISSING)
    if item is _MISSING:
      return None
    if not isinstance(item, dict):
      msg = "DynamoDB returned a malformed Item mapping"
      raise StorageError(msg, operation=operation, key=key)
    return item

  @staticmethod
  def _validated_expiry(
    item: dict[str, Any], operation: str, key: str
  ) -> tuple[Any, float] | None:
    """Read a finite numeric expiry without leaking a raw conversion error."""
    expire_at = item.get("expire_at")
    if expire_at is None:
      return None
    if isinstance(expire_at, bool) or not isinstance(
      expire_at, (int, float, Decimal)
    ):
      msg = "DynamoDB item has a non-numeric expire_at attribute"
      raise StorageError(msg, operation=operation, key=key)
    try:
      epoch = float(expire_at)
    except (OverflowError, TypeError, ValueError) as e:
      msg = "DynamoDB item has an invalid numeric expire_at attribute"
      raise StorageError(msg, operation=operation, key=key) from e
    if not math.isfinite(epoch):
      msg = "DynamoDB item has a non-finite expire_at attribute"
      raise StorageError(msg, operation=operation, key=key)
    return expire_at, epoch

  def _lazy_reap_if_expired(
    self, expiry: tuple[Any, float] | None, key: str
  ) -> bool:
    """Lazy-reap an expired item; return True if expired (caller treats as absent).

    Centralizes the TTL-expiry contract shared by ``retrieve`` / ``exists`` /
    ``ttl``: if the item's ``expire_at`` is in the past, delete it best-effort
    (via ``_swallow``) so the table does not accumulate dead rows, and return True.

    R-dyncas: the delete is a CAS on ``expire_at`` rather than unconditional.
    A concurrent ``store()`` after the strongly consistent read therefore makes
    the condition fail instead of letting lazy cleanup clobber the fresh value.
    """
    if expiry is None or expiry[1] > time.time():
      return False
    raw_expiry, _ = expiry
    with _swallow():
      self._table.delete_item(
        Key={"pk": key},
        ConditionExpression="expire_at = :exp",
        ExpressionAttributeValues={":exp": raw_expiry},
      )
    return True

  # StorageBackend implementation
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store ``data`` under ``key`` with optional TTL.

    Args:
        key: Storage key.
        data: Data to store (bytes).
        ttl: Optional time-to-live in seconds (stored as expire_at epoch).

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: On DynamoDB operational failures (throttling /
            throughput / limit / etc.). Was previously silently swallowed,
            masking data loss in the item pipeline.
    """
    _validate_partition_key(key)
    _validate_ttl(ttl)
    item: dict[str, Any] = {"pk": key, "value": data}
    expire_at: int | None = None
    if ttl is not None:
      expire_at = math.ceil(time.time() + ttl)
      item["expire_at"] = expire_at
    _validate_item_size(key, data, expire_at)
    try:
      self._table.put_item(Item=item)
    except Exception as e:
      if _is_resource_not_found(e):
        # Table vanished mid-operation — treat as storage failure too, but
        # callers checking existence after will see the table gone.
        msg = f"DynamoDB table not found while storing key {key!r}: {e}"
      else:
        msg = f"Failed to store key {key!r} in DynamoDB: {e}"
      raise StorageError(msg, operation="store", key=key) from e

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key (None if missing or expired).

    Args:
        key: Storage key.

    Returns:
        Stored data, or None if not found / expired.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: On operational failures (was previously silently
            swallowed to ``return None``).
    """
    _validate_partition_key(key)
    try:
      resp = self._table.get_item(Key={"pk": key}, ConsistentRead=True)
    except Exception as e:
      msg = f"Failed to retrieve key {key!r} from DynamoDB: {e}"
      raise StorageError(msg, operation="retrieve", key=key) from e
    item = self._response_item(resp, "retrieve", key)
    if item is None:
      return None
    expiry = self._validated_expiry(item, "retrieve", key)
    if self._lazy_reap_if_expired(expiry, key):
      return None
    value = item.get("value", _MISSING)
    if isinstance(value, (bytes, bytearray)):
      return bytes(value)
    try:
      binary_value = getattr(value, "value", None)
    except Exception as e:
      msg = "DynamoDB item has an unreadable binary value attribute"
      raise StorageError(msg, operation="retrieve", key=key) from e
    if isinstance(binary_value, (bytes, bytearray)):
      return bytes(binary_value)
    msg = "DynamoDB item has a missing or non-binary value attribute"
    raise StorageError(msg, operation="retrieve", key=key)

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if the key existed and was deleted, False otherwise.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: On operational failures (was previously silently
            swallowed to ``return False`` — masked ``ThrottlingException`` as
            "didn't exist", causing dedup re-emission).
    """
    _validate_partition_key(key)
    try:
      resp = self._table.delete_item(Key={"pk": key}, ReturnValues="ALL_OLD")
    except Exception as e:
      msg = f"Failed to delete key {key!r} in DynamoDB: {e}"
      raise StorageError(msg, operation="delete", key=key) from e
    return "Attributes" in resp

  def exists(self, key: str) -> bool:
    """Check if a key exists and is not expired.

    Args:
        key: Storage key.

    Returns:
        True if the key exists and is current.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: On operational failures (was previously silently
            swallowed to ``return False``).
    """
    _validate_partition_key(key)
    try:
      resp = self._table.get_item(Key={"pk": key}, ConsistentRead=True)
    except Exception as e:
      msg = f"Failed to check existence of key {key!r} in DynamoDB: {e}"
      raise StorageError(msg, operation="exists", key=key) from e
    item = self._response_item(resp, "exists", key)
    if item is None:
      return False
    expiry = self._validated_expiry(item, "exists", key)
    return not self._lazy_reap_if_expired(expiry, key)

  def ttl(self, key: str) -> int | None:
    """Return remaining TTL seconds if the item has expire_at, else None.

    Args:
        key: Storage key.

    Returns:
        Seconds remaining (>= 0), or None if no TTL, not found, or expired.

    Raises:
        ValueError: If key contains invalid characters.
        StorageError: On operational failures (was previously silently
            swallowed to ``return None``).
    """
    _validate_partition_key(key)
    try:
      resp = self._table.get_item(Key={"pk": key}, ConsistentRead=True)
    except Exception as e:
      msg = f"Failed to read TTL of key {key!r} in DynamoDB: {e}"
      raise StorageError(msg, operation="ttl", key=key) from e
    item = self._response_item(resp, "ttl", key)
    if item is None:
      return None
    expiry = self._validated_expiry(item, "ttl", key)
    if expiry is None:
      return None
    # R-dynttl: symmetry with retrieve()/exists() — lazy-reap expired rows so
    # the table does not accumulate dead rows, and return None (expired =
    # absent, matching retrieve's None / exists's False). Pre-fix this
    # returned 0 for an expired key without reaping, conflating "about to
    # expire" with "expired long ago" and leaving the dead row to linger until
    # a retrieve/exists/clear_storage touched it.
    if self._lazy_reap_if_expired(expiry, key):
      return None
    return max(0, int(expiry[1] - time.time()))

  def clear_storage(self, prefix: str | None = None) -> None:
    """Best-effort clear via scan + batch delete, optionally prefix-scoped.

    Args:
        prefix: If provided, only clear keys whose ``pk`` starts with this
            prefix (honors the StorageBackend ABC contract — matches Redis's
            ``scan_iter(match=prefix*)``). If None, clears all items.

    Raises:
        ValueError: If prefix contains invalid characters.
        StorageError: On operational failures (was previously silently
            swallowed).
    """
    if prefix is not None:
      _validate_key_name(prefix, "prefix")
    # R-dynprefix: scope the scan to the prefix (StorageBackend ABC contract:
    # "only clear keys starting with this prefix"). Pre-fix the prefix was
    # validated then IGNORED -- scan+delete wiped the entire table
    # (clear_storage("tenant_a:") nuked every tenant). String-form
    # FilterExpression (no boto3.conditions import, assertable in tests); ``pk``
    # is not a DynamoDB reserved word so it needs no ExpressionAttributeNames.
    # Pagination still works: LastEvaluatedKey is returned regardless of filter.
    scan_kwargs: dict[str, Any] = {"ConsistentRead": True}
    if prefix is not None:
      scan_kwargs["FilterExpression"] = "begins_with(pk, :p)"
      scan_kwargs["ExpressionAttributeValues"] = {":p": prefix}
    try:
      # Paginate: a single ``scan`` returns at most ~1 MB per page; without
      # following ``LastEvaluatedKey`` a large table is silently partial-clear
      # (#31). Loop until the scan reports no further page.
      last_key: dict[str, Any] | None = None
      while True:
        scan = self._table.scan(
          **scan_kwargs,
          **({"ExclusiveStartKey": last_key} if last_key else {}),
        )
        with self._table.batch_writer() as batch:
          for item in scan.get("Items", []):
            batch.delete_item(Key={"pk": item["pk"]})
        last_key = scan.get("LastEvaluatedKey")
        if not last_key:
          break
    except Exception as e:
      msg = f"Failed to clear DynamoDB table: {e}"
      raise StorageError(msg, operation="clear_storage", key=None) from e


class _swallow:
  """Context manager that swallows cleanup-path errors."""

  def __enter__(self) -> _swallow:
    return self

  def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
    if exc_type is None:
      return False
    # R-swallow: suppress only regular cleanup Exceptions -- NEVER BaseException
    # (KeyboardInterrupt / SystemExit / GeneratorExit). Pre-fix this returned
    # True for any non-None exc_type, trapping Ctrl+C during the lazy-reap
    # delete_item (the operator's shutdown signal disappeared into a debug log).
    if not isinstance(exc, Exception):
      return False
    logger.debug("Suppressed dynamodb cleanup error: %s", exc)
    return True
