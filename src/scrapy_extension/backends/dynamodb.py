"""Amazon DynamoDB backend (StorageBackend) — NoSQL KV (subsystem ③).

Implements StorageBackend using a DynamoDB table (keyed by ``pk``). TTL is
application-level: items with a TTL carry an ``expire_at`` epoch attribute,
checked on read (expired items are deleted and reported missing). The table
is auto-created on connect if missing (PAY_PER_REQUEST, hash key ``pk``).

boto3 resource API (stable):
- ``boto3.session.Session().resource("dynamodb", region_name=, endpoint_url=, ...)``
- ``resource.Table(name)`` / ``resource.create_table(...)``
- ``table.load()`` / ``table.wait_until_exists()``
- ``table.put_item(Item=)`` / ``get_item(Key=)`` / ``delete_item(Key=, ReturnValues=)``
- ``table.scan()`` / ``resource.meta.client.batch_write_item(RequestItems=)``
"""

from __future__ import annotations

import hashlib
import logging
import math
import threading
import time
from dataclasses import dataclass
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
from scrapy_extension.backends._retry import compute_full_jitter_backoff
from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  StorageBackend,
  _validate_key_name,
  _validate_ttl,
)
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import DynamoDBMode
from scrapy_extension.settings._aws import (
  validate_aws_credentials,
  validate_aws_endpoint,
  validate_aws_region_name,
)

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
_DDB_BATCH_WRITE_LIMIT = 25
_DDB_BATCH_MAX_ATTEMPTS = 8
_DDB_BATCH_BACKOFF_BASE_SECONDS = 0.05
_MISSING = object()
_DDB_USABLE_TABLE_STATUSES = frozenset({"ACTIVE", "UPDATING"})


class _DynamoDBConnectCancelled(Exception):
  """Internal signal for a candidate fenced by lifecycle teardown."""


@dataclass(frozen=True)
class _DynamoDBConnectionSnapshot:
  """One validated, non-secret settings snapshot for a table generation."""

  mode: DynamoDBMode
  table_name: str
  region_name: str
  endpoint_url: str | None


@dataclass(frozen=True)
class _DynamoDBGeneration:
  """One private Session/Resource/Table set published as a single unit."""

  session: Any
  resource: Any
  table: Any
  snapshot: _DynamoDBConnectionSnapshot


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
    # The boto3 Resource API is not thread-safe. This re-entrant lock is both
    # the operation serializer and the generation retirement barrier: a
    # disconnect cannot close a Resource until its admitted operation exits.
    self._operation_lock = threading.RLock()
    self._connect_lock = threading.Lock()
    self._lifecycle_epoch = 0
    self._generation: _DynamoDBGeneration | None = None
    # Compatibility/diagnostic mirrors. Internal operations use only the
    # authoritative generation so a Resource and Table can never be mixed.
    self._resource: Any = None
    self._table: Any = None

  def _capture_connect_intent(self) -> tuple[int, bool]:
    """Capture teardown epoch before waiting for connect single-flight."""
    with self._operation_lock:
      return self._lifecycle_epoch, self._generation is not None

  def _raise_if_connect_cancelled(self, request_epoch: int) -> None:
    """Stop a stale candidate before its next externally visible SDK step."""
    with self._operation_lock:
      if (
        request_epoch != self._lifecycle_epoch
        or self._generation is not None
      ):
        raise _DynamoDBConnectCancelled

  def _capture_connection_snapshot(
    self,
  ) -> tuple[_DynamoDBConnectionSnapshot, dict[str, Any]]:
    """Capture and revalidate every value consumed by one connect attempt."""
    mode = self.config.mode
    table_name = self.config.table_name
    access_key = self.config.aws_access_key_id
    secret_key = self.config.aws_secret_access_key
    if not isinstance(mode, DynamoDBMode):
      raise ConfigurationError(
        f"Unsupported DynamoDB mode: {mode}",
        setting_name="mode",
        setting_value=mode,
      )
    region_name = validate_aws_region_name(self.config.region_name)
    endpoint_url = validate_aws_endpoint(
      self.config.endpoint_url,
      cloud=mode == DynamoDBMode.CLOUD,
      require_endpoint=mode == DynamoDBMode.STANDALONE,
    )
    key_id, secret = validate_aws_credentials(
      access_key,
      secret_key,
    )

    snapshot = _DynamoDBConnectionSnapshot(
      mode=mode,
      table_name=table_name,
      region_name=region_name,
      endpoint_url=endpoint_url,
    )
    kwargs: dict[str, Any] = {"region_name": region_name}
    if endpoint_url is not None:
      kwargs["endpoint_url"] = endpoint_url
    if key_id is not None and secret is not None:
      # Preserve the SDK's required string behavior without retaining the
      # credentials in the published settings snapshot or exposing their repr.
      kwargs["aws_access_key_id"] = _redact(key_id)
      kwargs["aws_secret_access_key"] = _redact(secret)
    return snapshot, kwargs

  @staticmethod
  def _close_resource(resource: Any) -> None:
    """Best-effort close a candidate or retired botocore HTTP client."""
    if resource is None:
      return
    try:
      resource.meta.client.close()
    except Exception as exc:
      logger.debug("Suppressed DynamoDB resource close error: %s", exc)

  def _build_candidate(
    self,
    snapshot: _DynamoDBConnectionSnapshot,
    resource_kwargs: dict[str, Any],
    request_epoch: int,
  ) -> _DynamoDBGeneration:
    """Prepare one private generation without mutating published state."""
    session: Any = None
    resource: Any = None
    try:
      self._raise_if_connect_cancelled(request_epoch)
      # boto3's module-level resource() alias shares the process-wide default
      # Session, which is not thread-safe. A candidate owns a private Session
      # so independent backend instances cannot race model/waiter/client setup.
      session = boto3.session.Session()
      self._raise_if_connect_cancelled(request_epoch)
      resource = session.resource("dynamodb", **resource_kwargs)
      self._raise_if_connect_cancelled(request_epoch)
      table = resource.Table(snapshot.table_name)
      try:
        table.load()
      except Exception as e:
        # Only a genuine "table not found" triggers create_table — every other
        # error (throttle, network, auth, validation) MUST propagate (#31), or
        # a transient blip spuriously creates a conflicting table.
        if not _is_resource_not_found(e):
          raise
        # A teardown that won while DescribeTable was in flight must prevent a
        # late candidate from creating persistent infrastructure afterward.
        # Atomically admit the persistent create side effect against teardown.
        # If create wins, disconnect drains this SDK call; if teardown wins,
        # the stale candidate never creates infrastructure after it returns.
        with self._operation_lock:
          self._raise_if_connect_cancelled(request_epoch)
          try:
            table = resource.create_table(
              TableName=snapshot.table_name,
              KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
              AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"}
              ],
              BillingMode="PAY_PER_REQUEST",
            )
          except Exception as create_err:
            # Concurrent boot race (e.g. k8s pod rollout): another worker
            # already started creating this table. Reattach and wait. Any
            # other error propagates: a transient blip must not mask as created.
            if not _is_resource_in_use(create_err):
              raise
            table = resource.Table(snapshot.table_name)
        self._raise_if_connect_cancelled(request_epoch)
        table.wait_until_exists()
      else:
        self._raise_if_connect_cancelled(request_epoch)
        # DescribeTable succeeds for transitional states too. ACTIVE and
        # UPDATING accept data-plane work; other states must reach ACTIVE before
        # the generation can truthfully be published as ready.
        if table.table_status not in _DDB_USABLE_TABLE_STATUSES:
          table.wait_until_exists()
      return _DynamoDBGeneration(
        session=session,
        resource=resource,
        table=table,
        snapshot=snapshot,
      )
    except BaseException:
      self._close_resource(resource)
      raise

  def _publish_generation_locked(
    self, generation: _DynamoDBGeneration
  ) -> None:
    """Publish one complete generation while holding ``_operation_lock``."""
    self._generation = generation
    self._resource = generation.resource
    self._table = generation.table

  def _detach_generation_locked(self) -> _DynamoDBGeneration | None:
    """Detach the current generation while holding ``_operation_lock``."""
    generation = self._generation
    self._generation = None
    self._resource = None
    self._table = None
    return generation

  def _generation_for_operation_locked(
    self, operation: str, key: str | None
  ) -> _DynamoDBGeneration:
    """Return the authoritative generation or the stable storage error."""
    generation = self._generation
    if generation is None:
      raise StorageError(
        "DynamoDB backend is not connected",
        operation=operation,
        key=key,
      )
    return generation

  def _table_for_operation_locked(self, operation: str, key: str | None) -> Any:
    """Return the authoritative table or raise the stable storage contract."""
    return self._generation_for_operation_locked(operation, key).table

  @staticmethod
  def _validated_unprocessed_deletes(
    response: Any,
    table_name: str,
    submitted: list[dict[str, Any]],
  ) -> list[dict[str, Any]]:
    """Validate and return the exact submitted deletes DynamoDB deferred.

    The Resource client's DynamoDB transformers deserialize
    ``UnprocessedItems`` back to the same native request shape that was sent.
    Treat the response as untrusted: only a multiset subset of this attempt's
    deletes may be retried, so a malformed response can never induce a new
    deletion.
    """
    malformed = StorageError(
      "DynamoDB returned a malformed batch-write response; the clear may "
      "be partially complete",
      operation="clear_storage",
      key=None,
    )
    if not isinstance(response, dict):
      raise malformed
    unprocessed = response.get("UnprocessedItems", _MISSING)
    if not isinstance(unprocessed, dict):
      raise malformed
    if any(name != table_name for name in unprocessed):
      raise malformed
    raw_pending = unprocessed.get(table_name, [])
    if not isinstance(raw_pending, list):
      raise malformed
    if table_name in unprocessed and not raw_pending:
      # The service model requires at least one WriteRequest whenever a table
      # is present; successful completion is represented by an empty map.
      raise malformed

    remaining = list(submitted)
    validated: list[dict[str, Any]] = []
    for request in raw_pending:
      if not isinstance(request, dict) or set(request) != {"DeleteRequest"}:
        raise malformed
      delete = request["DeleteRequest"]
      if not isinstance(delete, dict) or set(delete) != {"Key"}:
        raise malformed
      key = delete["Key"]
      if (
        not isinstance(key, dict)
        or set(key) != {"pk"}
        or not isinstance(key["pk"], str)
      ):
        raise malformed
      try:
        match = remaining.index(request)
      except ValueError:
        raise malformed from None
      remaining.pop(match)
      validated.append(request)
    return validated

  @staticmethod
  def _validated_scan_page(
    response: Any,
  ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Validate one Scan page without treating malformed data as success."""
    malformed = StorageError(
      "DynamoDB returned a malformed scan response; the clear may be "
      "partially complete",
      operation="clear_storage",
      key=None,
    )
    if not isinstance(response, dict):
      raise malformed
    raw_items = response.get("Items", _MISSING)
    if not isinstance(raw_items, list):
      raise malformed
    items: list[dict[str, Any]] = []
    for item in raw_items:
      if not isinstance(item, dict) or not isinstance(item.get("pk"), str):
        raise malformed
      items.append(item)

    cursor = response.get("LastEvaluatedKey")
    if cursor is None or cursor == {}:
      return items, None
    if (
      not isinstance(cursor, dict)
      or set(cursor) != {"pk"}
      or not isinstance(cursor["pk"], str)
    ):
      raise malformed
    return items, cursor

  @classmethod
  def _delete_batch_with_backoff(
    cls,
    client: Any,
    table_name: str,
    requests: list[dict[str, Any]],
  ) -> None:
    """Delete one physical batch with bounded unprocessed-item retries."""
    pending = list(requests)
    for attempt in range(_DDB_BATCH_MAX_ATTEMPTS):
      response = client.batch_write_item(
        RequestItems={table_name: pending},
      )
      pending = cls._validated_unprocessed_deletes(response, table_name, pending)
      if not pending:
        return
      if attempt == _DDB_BATCH_MAX_ATTEMPTS - 1:
        logger.warning(
          "DynamoDB clear exhausted %d attempts with %d request(s) still unprocessed",
          _DDB_BATCH_MAX_ATTEMPTS,
          len(pending),
        )
        raise StorageError(
          "DynamoDB clear is partially complete: "
          f"{len(pending)} delete request(s) remained unprocessed after "
          f"{_DDB_BATCH_MAX_ATTEMPTS} attempts",
          operation="clear_storage",
          key=None,
        )
      delay = compute_full_jitter_backoff(attempt, _DDB_BATCH_BACKOFF_BASE_SECONDS)
      logger.debug(
        "Retrying %d unprocessed DynamoDB clear request(s) after %.3fs",
        len(pending),
        delay,
      )
      time.sleep(delay)

  def connect(self) -> None:
    """Privately prepare and atomically publish one table generation.

    A live connection makes this method an idempotent no-op. Configuration
    changes take effect only after an explicit ``disconnect()`` / ``connect()``.

    Raises:
        BackendConnectionError: If the resource/table cannot be set up.
        ConfigurationError: If the captured configuration is invalid.
    """
    request_epoch, already_connected = self._capture_connect_intent()
    if already_connected:
      return
    with self._connect_lock:
      with self._operation_lock:
        if request_epoch != self._lifecycle_epoch:
          return
        if self._generation is not None:
          return
      snapshot, resource_kwargs = self._capture_connection_snapshot()
      try:
        candidate = self._build_candidate(
          snapshot, resource_kwargs, request_epoch
        )
      except _DynamoDBConnectCancelled:
        return
      except Exception as exc:
        # Teardown intentionally cancels an in-progress connection attempt.
        # A late SDK failure from that stale attempt is not a new live error.
        with self._operation_lock:
          if request_epoch != self._lifecycle_epoch:
            return
        raise BackendConnectionError(
          "Failed to connect to DynamoDB.", backend_type="dynamodb"
        ) from exc

      with self._operation_lock:
        publish = (
          request_epoch == self._lifecycle_epoch
          and self._generation is None
        )
        if publish:
          self._publish_generation_locked(candidate)
      if not publish:
        self._close_resource(candidate.resource)
        return
      logger.debug("Connected to DynamoDB table %s", snapshot.table_name)

  def disconnect(self) -> None:
    """Fence connect intents, drain operations, and close the retired client."""
    with self._operation_lock:
      self._lifecycle_epoch += 1
      generation = self._detach_generation_locked()
      if generation is not None:
        self._close_resource(generation.resource)

  def is_connected(self) -> bool:
    """Return True if a complete generation is currently published."""
    with self._operation_lock:
      return self._generation is not None

  def ping(self) -> bool:
    """Health check via table.load()."""
    with self._operation_lock:
      generation = self._generation
      if generation is None:
        return False
      try:
        generation.table.load()
        return (
          generation.table.table_status in _DDB_USABLE_TABLE_STATUSES
        )
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
  def _response_deleted(response: Any, key: str) -> bool:
    """Interpret one structurally valid DeleteItem ``ALL_OLD`` response."""
    malformed = StorageError(
      "DynamoDB returned a malformed DeleteItem response",
      operation="delete",
      key=key,
    )
    if not isinstance(response, dict):
      raise malformed
    attributes = response.get("Attributes", _MISSING)
    if attributes is _MISSING:
      return False
    # ALL_OLD returns the entire deleted item. This backend's table has one
    # required string partition key, so a success mapping must identify the
    # exact item requested rather than merely contain an Attributes field.
    if not isinstance(attributes, dict):
      raise malformed
    returned_key = attributes.get("pk", _MISSING)
    if not isinstance(returned_key, str) or returned_key != key:
      raise malformed
    return True

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
    self, table: Any, expiry: tuple[Any, float] | None, key: str
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
      table.delete_item(
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
    with self._operation_lock:
      table = self._table_for_operation_locked("store", key)
      try:
        table.put_item(Item=item)
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
    with self._operation_lock:
      table = self._table_for_operation_locked("retrieve", key)
      try:
        resp = table.get_item(Key={"pk": key}, ConsistentRead=True)
      except Exception as e:
        msg = f"Failed to retrieve key {key!r} from DynamoDB: {e}"
        raise StorageError(msg, operation="retrieve", key=key) from e
      item = self._response_item(resp, "retrieve", key)
      if item is None:
        return None
      expiry = self._validated_expiry(item, "retrieve", key)
      if self._lazy_reap_if_expired(table, expiry, key):
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
    with self._operation_lock:
      table = self._table_for_operation_locked("delete", key)
      try:
        resp = table.delete_item(Key={"pk": key}, ReturnValues="ALL_OLD")
      except Exception as e:
        # Preserve the SDK exception as the cause without copying its message:
        # endpoint URLs and provider diagnostics can contain operator secrets.
        msg = f"Failed to delete key {key!r} in DynamoDB"
        raise StorageError(msg, operation="delete", key=key) from e
      return self._response_deleted(resp, key)

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
    with self._operation_lock:
      table = self._table_for_operation_locked("exists", key)
      try:
        resp = table.get_item(Key={"pk": key}, ConsistentRead=True)
      except Exception as e:
        msg = f"Failed to check existence of key {key!r} in DynamoDB: {e}"
        raise StorageError(msg, operation="exists", key=key) from e
      item = self._response_item(resp, "exists", key)
      if item is None:
        return False
      expiry = self._validated_expiry(item, "exists", key)
      return not self._lazy_reap_if_expired(table, expiry, key)

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
    with self._operation_lock:
      table = self._table_for_operation_locked("ttl", key)
      try:
        resp = table.get_item(Key={"pk": key}, ConsistentRead=True)
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
      if self._lazy_reap_if_expired(table, expiry, key):
        return None
      return max(0, int(expiry[1] - time.time()))

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear observed keys via bounded batch deletes, optionally by prefix.

    Each physical batch contains at most 25 deletes and gets at most eight
    application-level BatchWriteItem submissions with full-jitter backoff for
    valid ``UnprocessedItems``.
    Success means every delete observed by this paginated Scan was accepted;
    it does not prove the table/prefix is empty in the presence of external
    writers because DynamoDB Scan has no cross-page snapshot isolation.

    Args:
        prefix: If provided, only clear keys whose ``pk`` starts with this
            prefix (honors the StorageBackend ABC contract — matches Redis's
            ``scan_iter(match=prefix*)``). If None, clears all items.

    Raises:
        ValueError: If prefix contains invalid characters.
        StorageError: On operational, malformed-response, repeated-cursor, or
            retry-exhaustion failures. Deletion is non-transactional, so the
            clear may already be partially complete.
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
    with self._operation_lock:
      generation = self._generation_for_operation_locked("clear_storage", None)
      table = generation.table
      client = generation.resource.meta.client
      table_name = generation.snapshot.table_name
      try:
        # Paginate: a single ``scan`` returns at most ~1 MB per page; without
        # following ``LastEvaluatedKey`` a large table is silently partial-clear
        # (#31). Loop until the scan reports no further page. The operation lock
        # pins every page and batch flush to this exact Table generation.
        last_key: dict[str, Any] | None = None
        # Retain fixed-size digests rather than up to 2 KiB of raw key text per
        # page while detecting non-adjacent pagination cycles.
        seen_cursor_digests: set[bytes] = set()
        while True:
          scan = table.scan(
            **scan_kwargs,
            **({"ExclusiveStartKey": last_key} if last_key else {}),
          )
          items, next_key = self._validated_scan_page(scan)
          if prefix is not None and any(
            not item["pk"].startswith(prefix) for item in items
          ):
            raise StorageError(
              "DynamoDB returned a malformed out-of-scope scan response; "
              "the clear may be partially complete",
              operation="clear_storage",
              key=None,
            )
          if next_key is not None:
            cursor_digest = hashlib.sha256(
              next_key["pk"].encode("utf-8")
            ).digest()
            if cursor_digest in seen_cursor_digests:
              raise StorageError(
                "DynamoDB clear is partially complete: Scan returned a "
                "repeated pagination cursor",
                operation="clear_storage",
                key=None,
              )
            seen_cursor_digests.add(cursor_digest)
          requests = [{"DeleteRequest": {"Key": {"pk": item["pk"]}}} for item in items]
          for offset in range(0, len(requests), _DDB_BATCH_WRITE_LIMIT):
            self._delete_batch_with_backoff(
              client,
              table_name,
              requests[offset : offset + _DDB_BATCH_WRITE_LIMIT],
            )
          last_key = next_key
          if not last_key:
            break
      except StorageError:
        raise
      except Exception as e:
        # Preserve the driver error as the cause without copying endpoint,
        # prefix, key, or credential-shaped text into the public exception.
        msg = "Failed to clear DynamoDB table; the clear may be partially complete"
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
