"""Amazon DynamoDB backend (StorageBackend) — NoSQL KV (subsystem ③).

Implements StorageBackend using a DynamoDB table (keyed by ``pk``). TTL is
application-level: items with a TTL carry an ``expire_at`` epoch attribute,
checked on read (expired items are deleted and reported missing). The table
is auto-created on connect if missing (PAY_PER_REQUEST, hash key ``pk``).

boto3 resource API (stable):
- ``boto3.session.Session().resource("dynamodb", region_name=, endpoint_url=, ...)``
- ``resource.Table(name)`` / ``resource.create_table(...)``
- ``table.load()`` / ``table.wait_until_exists()``
- ``table.put_item(Item=)`` / ``get_item(Key=)`` / conditional ``delete_item(...)``
- ``table.scan()`` / revision-conditioned ``delete_item(...)`` for clears
"""

from __future__ import annotations

import hashlib
import logging
import math
import threading
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
    import boto3
    from botocore.config import Config as BotoConfig
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
)
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.exceptions._redaction import (
    backend_connection_error_boundary,
    configuration_error_boundary,
    storage_operation_error_boundary,
)
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import DynamoDBMode, DynamoDBSettings
from scrapy_extension.settings._aws import (
    _AWS_SAFE_CONFIGURATION_MESSAGES,
    validate_aws_credentials,
    validate_aws_endpoint,
    validate_aws_region_name,
)

logger = logging.getLogger(__name__)

_DYNAMODB_CONFIGURATION_SETTING_NAMES: frozenset[str] = frozenset(
    DynamoDBSettings.model_fields
)
_DYNAMODB_INVALID_LEGACY_CLEAR_OVERRIDE = (
    "DynamoDB legacy clear override must be an exact boolean."
)
_DYNAMODB_SAFE_CONFIGURATION_MESSAGES: frozenset[str] = frozenset(
    {
        "Unsupported DynamoDB mode.",
        _DYNAMODB_INVALID_LEGACY_CLEAR_OVERRIDE,
    }
    | _AWS_SAFE_CONFIGURATION_MESSAGES
)
_DYNAMODB_SAFE_CONNECTION_MESSAGES: frozenset[str] = frozenset(
    {"Failed to connect to DynamoDB."}
)
_DYNAMODB_STORAGE_STORE_ERROR = "DynamoDB storage store failed."
_DYNAMODB_STORAGE_RETRIEVE_ERROR = "DynamoDB storage retrieve failed."
_DYNAMODB_STORAGE_DELETE_ERROR = "DynamoDB storage delete failed."
_DYNAMODB_STORAGE_EXISTS_ERROR = "DynamoDB storage existence check failed."
_DYNAMODB_STORAGE_TTL_ERROR = "DynamoDB storage TTL read failed."
_DYNAMODB_STORAGE_CLEAR_ERROR = "DynamoDB storage clear failed."

# DynamoDB ClientError codes used while establishing the table. A
# ResourceNotFoundException means the TABLE is missing, never that an item is
# absent (missing items are successful responses without Item/Attributes).
# Runtime storage operations must therefore surface this code as StorageError.
_DDB_NOT_FOUND_CODES = frozenset({"ResourceNotFoundException"})
_DDB_INUSE_CODES = frozenset({"ResourceInUseException"})
_DDB_CONDITION_FAILED_CODES = frozenset({"ConditionalCheckFailedException"})
_DDB_MAX_PARTITION_KEY_BYTES = 2_048
_DDB_MAX_ITEM_BYTES = 400 * 1_024
_DDB_REVISION_ATTRIBUTE = "_scrapy_revision"
_DDB_REVISION_BYTES = 32
_DYNAMODB_CLEAR_CONCURRENT_WRITE = (
    "DynamoDB clear is partially complete: an observed item changed before "
    "conditional deletion"
)
_DYNAMODB_CLEAR_UNFENCED_LEGACY = (
    "DynamoDB clear is partially complete: stopped at an unfenced legacy item; "
    "the item was preserved"
)
_DYNAMODB_CLEAR_MALFORMED_DELETE = (
    "DynamoDB returned a malformed conditional DeleteItem response; the clear may "
    "be partially complete"
)
_DYNAMODB_CLEAR_MALFORMED_REVISION = (
    "DynamoDB clear is partially complete: stopped at an item with malformed "
    "revision metadata; the item was preserved"
)
_DYNAMODB_SAFE_STORAGE_MESSAGES: frozenset[str] = frozenset(
    {
        "DynamoDB backend is not connected",
        _DYNAMODB_CLEAR_CONCURRENT_WRITE,
        _DYNAMODB_CLEAR_UNFENCED_LEGACY,
        _DYNAMODB_CLEAR_MALFORMED_DELETE,
        _DYNAMODB_CLEAR_MALFORMED_REVISION,
        "DynamoDB returned a malformed scan response; the clear may be "
        "partially complete",
        "DynamoDB returned a malformed out-of-scope scan response; the clear may "
        "be partially complete",
        "DynamoDB clear is partially complete: Scan returned a repeated pagination "
        "cursor",
        "Failed to clear DynamoDB table; the clear may be partially complete",
        "DynamoDB returned a non-mapping item response",
        "DynamoDB returned a malformed Item mapping",
        "DynamoDB returned a malformed DeleteItem response",
        "DynamoDB item has a non-numeric expire_at attribute",
        "DynamoDB item has an invalid numeric expire_at attribute",
        "DynamoDB item has a non-finite expire_at attribute",
        "DynamoDB item has an unreadable binary value attribute",
        "DynamoDB item has a missing or non-binary value attribute",
    }
)
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
    allow_unfenced_legacy_clear: bool


@dataclass(frozen=True)
class _DynamoDBGeneration:
    """One private Session/Resource/Table set published as a single unit."""

    session: Any
    resource: Any
    table: Any
    snapshot: _DynamoDBConnectionSnapshot


@dataclass
class _DynamoDBConnectCleanupState:
    """Deferred diagnostics collected while a private candidate is unwound."""

    aborted_resource_close_failed: bool = False


def _validate_partition_key(key: str) -> None:
    """Validate the package key grammar and DynamoDB's physical byte ceiling."""
    _validate_key_name(key, "key")
    key_size = len(key.encode("utf-8"))
    if key_size > _DDB_MAX_PARTITION_KEY_BYTES:
        raise ValueError(
            f"DynamoDB partition key exceeds 2,048 UTF-8 bytes ({key_size} bytes)."
        )


def _is_valid_item_revision(revision: object) -> bool:
    """Return whether a clear fence has the exact generated revision grammar."""
    return (
        isinstance(revision, str)
        and len(revision) == _DDB_REVISION_BYTES
        and all(character in "0123456789abcdef" for character in revision)
    )


def _validate_dynamodb_storage_key_argument(
    _backend: object,
    key: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate one direct DynamoDB key before a terminal error boundary."""
    _validate_partition_key(key)


def _validate_dynamodb_store_arguments(
    _backend: object,
    key: str,
    data: bytes,
    ttl: int | None = None,
) -> None:
    """Validate direct DynamoDB store inputs before implementation frames."""
    del data
    _validate_partition_key(key)
    _validate_ttl(ttl)


def _validate_dynamodb_storage_prefix_argument(
    _backend: object,
    prefix: str | None = None,
) -> None:
    """Validate an optional direct DynamoDB clear prefix before backend work."""
    if prefix is not None:
        _validate_key_name(prefix, "prefix")


def _is_safe_dynamodb_storage_message(_message: str) -> bool:
    """No dynamic DynamoDB storage diagnostics are public-safe."""
    return False


def _number_size_upper_bound(value: int) -> int:
    """Return a safe DynamoDB byte estimate for the positive integer value."""
    digits = len(str(abs(value)))
    return (digits + 1) // 2 + 1


def _validate_item_size(key: str, data: bytes, expire_at: int | None) -> None:
    """Reject items beyond DynamoDB's 400 KiB names-plus-values limit."""
    item_size = (
        len("pk")
        + len(key.encode("utf-8"))
        + len("value")
        + len(data)
        + len(_DDB_REVISION_ATTRIBUTE)
        + _DDB_REVISION_BYTES
    )
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


def _is_conditional_check_failed(exc: BaseException) -> bool:
    """Return True for DynamoDB's non-destructive condition-loss result."""
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    err = response.get("Error")
    if not isinstance(err, dict):
        return False
    return err.get("Code") in _DDB_CONDITION_FAILED_CODES


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

    Stores values under a partition key ``pk`` with a fresh opaque
    ``_scrapy_revision`` on every store. Items may carry an ``expire_at`` epoch
    attribute; reads treat expired items as missing and delete them. The table is
    created on connect if it does not exist (PAY_PER_REQUEST).

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
            if request_epoch != self._lifecycle_epoch or self._generation is not None:
                raise _DynamoDBConnectCancelled

    @configuration_error_boundary(
        "DynamoDB configuration is invalid.",
        _DYNAMODB_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_DYNAMODB_SAFE_CONFIGURATION_MESSAGES,
    )
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
                "Unsupported DynamoDB mode.",
                setting_name="mode",
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
        allow_unfenced_legacy_clear = self.config.allow_unfenced_legacy_clear
        # Pydantic validates construction, but callers can mutate settings objects
        # afterward. Never let a truthy string become a destructive generation flag.
        if type(allow_unfenced_legacy_clear) is not bool:
            raise ConfigurationError(
                _DYNAMODB_INVALID_LEGACY_CLEAR_OVERRIDE,
                setting_name="allow_unfenced_legacy_clear",
            )

        snapshot = _DynamoDBConnectionSnapshot(
            mode=mode,
            table_name=table_name,
            region_name=region_name,
            endpoint_url=endpoint_url,
            allow_unfenced_legacy_clear=allow_unfenced_legacy_clear,
        )
        kwargs: dict[str, Any] = {
            "region_name": region_name,
            # The endpoint policy belongs to this validated snapshot. Ignore
            # AWS_ENDPOINT_URL[_DYNAMODB] and shared-config endpoint overrides so an
            # ambient HTTP URL cannot bypass the cloud-mode transport guard.
            "config": BotoConfig(ignore_configured_endpoint_urls=True),
        }
        if endpoint_url is not None:
            kwargs["endpoint_url"] = endpoint_url
        if key_id is not None and secret is not None:
            # Preserve the SDK's required string behavior without retaining the
            # credentials in the published settings snapshot or exposing their repr.
            kwargs["aws_access_key_id"] = _redact(key_id)
            kwargs["aws_secret_access_key"] = _redact(secret)
        return snapshot, kwargs

    @staticmethod
    def _close_resource(resource: Any) -> bool:
        """Best-effort close a candidate or retired botocore HTTP client.

        Returns whether an ordinary close failure was suppressed.  The caller owns
        the diagnostic so a candidate-abort caller can defer it until its primary
        startup exception has fully unwound.
        """
        if resource is None:
            return False
        try:
            resource.meta.client.close()
        except Exception:
            return True
        return False

    @staticmethod
    def _log_resource_close_diagnostic() -> None:
        """Emit the ordinary detached-resource close diagnostic."""
        try:
            logger.debug("Suppressed DynamoDB resource close error")
        except BaseException:
            pass

    @staticmethod
    def _log_aborted_resource_close_diagnostic() -> None:
        """Emit the private-candidate cleanup diagnostic after startup unwinds."""
        try:
            logger.debug("Suppressed aborted DynamoDB resource close error")
        except BaseException:
            pass

    @classmethod
    def _close_aborted_resource(cls, resource: Any) -> bool:
        """Close an unpublished candidate without masking its primary failure.

        This helper is deliberately restricted to candidate-abort paths.  Normal
        ``disconnect()`` keeps ``BaseException`` observable to its caller, while
        an exception that already aborted candidate construction/publication must
        remain the causal error even if SDK cleanup is interrupted.  It reports
        completion status instead of logging because callers commonly invoke it
        under the primary failure's ``except`` suite.
        """
        try:
            return cls._close_resource(resource)
        except BaseException:
            return True

    def _build_candidate(
        self,
        snapshot: _DynamoDBConnectionSnapshot,
        resource_kwargs: dict[str, Any],
        request_epoch: int,
        cleanup_state: _DynamoDBConnectCleanupState,
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
            cleanup_state.aborted_resource_close_failed = self._close_aborted_resource(
                resource
            )
            raise

    def _publish_generation_locked(self, generation: _DynamoDBGeneration) -> None:
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
            revision = item.get(_DDB_REVISION_ATTRIBUTE, _MISSING)
            if revision is not _MISSING and not _is_valid_item_revision(revision):
                raise StorageError(
                    _DYNAMODB_CLEAR_MALFORMED_REVISION,
                    operation="clear_storage",
                    key=None,
                )
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

    @staticmethod
    def _clear_concurrent_write_error() -> StorageError:
        """Build the stable partial-result error for a lost revision condition."""
        return StorageError(
            _DYNAMODB_CLEAR_CONCURRENT_WRITE,
            operation="clear_storage",
            key=None,
        )

    @staticmethod
    def _validate_clear_delete_response(response: Any, item: dict[str, Any]) -> None:
        """Require ``ALL_OLD`` to identify the conditionally deleted row."""
        malformed = StorageError(
            _DYNAMODB_CLEAR_MALFORMED_DELETE,
            operation="clear_storage",
            key=None,
        )
        if not isinstance(response, dict):
            raise malformed
        attributes = response.get("Attributes", _MISSING)
        if not isinstance(attributes, dict):
            raise malformed
        returned_key = attributes.get("pk", _MISSING)
        if not isinstance(returned_key, str) or returned_key != item["pk"]:
            raise malformed
        expected_revision = item.get(_DDB_REVISION_ATTRIBUTE, _MISSING)
        returned_revision = attributes.get(_DDB_REVISION_ATTRIBUTE, _MISSING)
        if expected_revision is _MISSING:
            # The maintenance override has no revision CAS. Its conditional
            # expression narrows races, and ALL_OLD must independently confirm that
            # DynamoDB deleted the complete item observed by Scan.
            if returned_revision is not _MISSING or attributes != item:
                raise malformed
        elif (
            not isinstance(expected_revision, str)
            or not isinstance(returned_revision, str)
            or returned_revision != expected_revision
        ):
            raise malformed

    @classmethod
    def _delete_clear_item(
        cls,
        table: Any,
        item: dict[str, Any],
        *,
        allow_unfenced_legacy_clear: bool,
    ) -> None:
        """Conditionally delete one observed row under the selected safety policy."""
        revision = item.get(_DDB_REVISION_ATTRIBUTE, _MISSING)
        if revision is not _MISSING and not _is_valid_item_revision(revision):
            raise StorageError(
                _DYNAMODB_CLEAR_MALFORMED_REVISION,
                operation="clear_storage",
                key=None,
            )
        delete_kwargs: dict[str, Any]
        if revision is _MISSING:
            if allow_unfenced_legacy_clear is not True:
                raise StorageError(
                    _DYNAMODB_CLEAR_UNFENCED_LEGACY,
                    operation="clear_storage",
                    key=None,
                )
            # This explicit maintenance-only path deliberately does not add a
            # revision first: a legacy item may already occupy the exact 400 KiB
            # DynamoDB limit. Attribute equality narrows ordinary races, but cannot
            # distinguish identical-value ABA; operators must stop every writer.
            names = {"#revision": _DDB_REVISION_ATTRIBUTE}
            values: dict[str, Any] = {}
            conditions = ["attribute_exists(pk)", "attribute_not_exists(#revision)"]
            for index, (name, value) in enumerate(item.items()):
                if name in {"pk", _DDB_REVISION_ATTRIBUTE}:
                    continue
                name_token = f"#item{index}"
                value_token = f":item{index}"
                names[name_token] = name
                values[value_token] = value
                conditions.append(f"{name_token} = {value_token}")
            delete_kwargs = {
                "Key": {"pk": item["pk"]},
                "ConditionExpression": " AND ".join(conditions),
                "ExpressionAttributeNames": names,
                "ReturnValues": "ALL_OLD",
            }
            if values:
                delete_kwargs["ExpressionAttributeValues"] = values
        else:
            delete_kwargs = {
                "Key": {"pk": item["pk"]},
                "ConditionExpression": "#revision = :revision",
                "ExpressionAttributeNames": {"#revision": _DDB_REVISION_ATTRIBUTE},
                "ExpressionAttributeValues": {":revision": revision},
                "ReturnValues": "ALL_OLD",
            }
        try:
            response = table.delete_item(**delete_kwargs)
        except Exception as exc:
            if _is_conditional_check_failed(exc):
                raise cls._clear_concurrent_write_error() from None
            raise
        cls._validate_clear_delete_response(response, item)

    @backend_connection_error_boundary(
        "Failed to connect to DynamoDB.",
        "dynamodb",
        safe_messages=_DYNAMODB_SAFE_CONNECTION_MESSAGES,
    )
    @configuration_error_boundary(
        "DynamoDB configuration is invalid.",
        _DYNAMODB_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_DYNAMODB_SAFE_CONFIGURATION_MESSAGES,
        pass_through_exception_types=(BackendConnectionError,),
    )
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
            candidate: _DynamoDBGeneration | None = None
            startup_error: BackendConnectionError | None = None
            cleanup_state = _DynamoDBConnectCleanupState()
            connect_cancelled = False
            stale_failure = False
            try:
                candidate = self._build_candidate(
                    snapshot, resource_kwargs, request_epoch, cleanup_state
                )
            except _DynamoDBConnectCancelled:
                connect_cancelled = True
            except Exception:
                # Teardown intentionally cancels an in-progress connection attempt.
                # A late SDK failure from that stale attempt is not a new live error.
                with self._operation_lock:
                    stale_failure = request_epoch != self._lifecycle_epoch
                if not stale_failure:
                    startup_error = BackendConnectionError(
                        "Failed to connect to DynamoDB.", backend_type="dynamodb"
                    )

            if cleanup_state.aborted_resource_close_failed:
                # The raw candidate failure and any nested close interruption have both
                # unwound. A custom logging handler therefore receives only this fixed
                # diagnostic rather than an SDK exception graph through ``sys.exc_info``.
                self._log_aborted_resource_close_diagnostic()

            if connect_cancelled or stale_failure:
                return

            if startup_error is not None:
                # Raise outside the SDK exception handler so endpoint/credential text
                # cannot survive through ``__cause__`` or ``__context__``.
                raise startup_error
            assert candidate is not None

            try:
                with self._operation_lock:
                    publish = (
                        request_epoch == self._lifecycle_epoch
                        and self._generation is None
                    )
                    if publish:
                        self._publish_generation_locked(candidate)
            except BaseException:
                # R23-G: a Ctrl+C/SystemExit in the candidate→publish window must not
                # leak the candidate's already-open HTTP client. _build_candidate opened
                # the urllib3 connection pool via table.load()/wait_until_exists() (both
                # network RPCs); the build arm (L327) only covers failures BEFORE the
                # candidate returns, so this extends it to the post-build publish
                # window. Close the candidate ONLY when it was not already published as
                # the live generation — _publish_generation_locked installs it as
                # self._generation as a side effect, so the identity guard reads actual
                # post-publish state (a published candidate is the live session and
                # must not be closed). Mirrors rabbitmq.py:540-558 + the redis
                # publish-step arm. Resource leak, not wedge: an unpublished candidate
                # never reaches instance state, so is_connected() stays truthful.
                if self._generation is not candidate:
                    self._close_aborted_resource(candidate.resource)
                raise
            if not publish:
                close_failed = self._close_resource(candidate.resource)
                if close_failed:
                    self._log_resource_close_diagnostic()
                return
            # Publication is the lifecycle linearization point. A success-only
            # diagnostic handler is outside that transaction: it must not make
            # connect() report a false failure or retire the now-live generation.
            try:
                logger.debug("Connected to DynamoDB.")
            except BaseException:
                pass

    def disconnect(self) -> None:
        """Fence connect intents, drain operations, and close the retired client."""
        close_failed = False
        with self._operation_lock:
            self._lifecycle_epoch += 1
            generation = self._detach_generation_locked()
            if generation is not None:
                close_failed = self._close_resource(generation.resource)
        if close_failed:
            self._log_resource_close_diagnostic()

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
                return generation.table.table_status in _DDB_USABLE_TABLE_STATUSES
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
        so the table does not accumulate dead rows, and return True.

        R-dyncas: the delete is a CAS on ``expire_at`` rather than unconditional.
        A concurrent ``store()`` after the strongly consistent read therefore makes
        the condition fail instead of letting lazy cleanup clobber the fresh value.
        """
        if expiry is None or expiry[1] > time.time():
            return False
        raw_expiry, _ = expiry
        cleanup = _swallow()
        with cleanup:
            table.delete_item(
                Key={"pk": key},
                ConditionExpression="expire_at = :exp",
                ExpressionAttributeValues={":exp": raw_expiry},
            )
        if cleanup.suppressed_error:
            # The context manager has completed its suppression before this
            # diagnostic runs, so a logging extension cannot observe the raw delete
            # error through ``sys.exc_info()``.
            try:
                logger.debug("Suppressed DynamoDB expired-item cleanup failure.")
            except BaseException:
                pass
        return True

    # StorageBackend implementation
    @storage_operation_error_boundary(
        "store",
        _DYNAMODB_STORAGE_STORE_ERROR,
        "dynamodb",
        safe_messages=_DYNAMODB_SAFE_STORAGE_MESSAGES,
        safe_message_predicate=_is_safe_dynamodb_storage_message,
        validator=_validate_dynamodb_store_arguments,
    )
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
        item: dict[str, Any] = {
            "pk": key,
            "value": data,
            _DDB_REVISION_ATTRIBUTE: uuid.uuid4().hex,
        }
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
                    msg = f"DynamoDB table not found while storing key {key!r}"
                else:
                    msg = f"Failed to store key {key!r} in DynamoDB"
                raise StorageError(msg, operation="store", key=key) from e

    @storage_operation_error_boundary(
        "retrieve",
        _DYNAMODB_STORAGE_RETRIEVE_ERROR,
        "dynamodb",
        safe_messages=_DYNAMODB_SAFE_STORAGE_MESSAGES,
        safe_message_predicate=_is_safe_dynamodb_storage_message,
        validator=_validate_dynamodb_storage_key_argument,
    )
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
                msg = f"Failed to retrieve key {key!r} from DynamoDB"
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

    @storage_operation_error_boundary(
        "delete",
        _DYNAMODB_STORAGE_DELETE_ERROR,
        "dynamodb",
        safe_messages=_DYNAMODB_SAFE_STORAGE_MESSAGES,
        safe_message_predicate=_is_safe_dynamodb_storage_message,
        validator=_validate_dynamodb_storage_key_argument,
    )
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

    @storage_operation_error_boundary(
        "exists",
        _DYNAMODB_STORAGE_EXISTS_ERROR,
        "dynamodb",
        safe_messages=_DYNAMODB_SAFE_STORAGE_MESSAGES,
        safe_message_predicate=_is_safe_dynamodb_storage_message,
        validator=_validate_dynamodb_storage_key_argument,
    )
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
                msg = f"Failed to check existence of key {key!r} in DynamoDB"
                raise StorageError(msg, operation="exists", key=key) from e
            item = self._response_item(resp, "exists", key)
            if item is None:
                return False
            expiry = self._validated_expiry(item, "exists", key)
            return not self._lazy_reap_if_expired(table, expiry, key)

    @storage_operation_error_boundary(
        "ttl",
        _DYNAMODB_STORAGE_TTL_ERROR,
        "dynamodb",
        safe_messages=_DYNAMODB_SAFE_STORAGE_MESSAGES,
        safe_message_predicate=_is_safe_dynamodb_storage_message,
        validator=_validate_dynamodb_storage_key_argument,
    )
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
                msg = f"Failed to read TTL of key {key!r} in DynamoDB"
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

    @storage_operation_error_boundary(
        "clear_storage",
        _DYNAMODB_STORAGE_CLEAR_ERROR,
        "dynamodb",
        safe_messages=_DYNAMODB_SAFE_STORAGE_MESSAGES,
        safe_message_predicate=_is_safe_dynamodb_storage_message,
        validator=_validate_dynamodb_storage_prefix_argument,
    )
    def clear_storage(self, prefix: str | None = None) -> None:
        """Clear observed item revisions, optionally restricted by key prefix.

        Every normal delete is conditional on the opaque revision observed by
        Scan. Revisionless legacy rows fail closed and remain present unless the
        connected generation captured the explicit stopped-writer maintenance
        override. Success does not prove the table/prefix is empty because
        DynamoDB Scan has no cross-page snapshot isolation.

        Args:
            prefix: If provided, only clear keys whose ``pk`` starts with this
                prefix (honors the StorageBackend ABC contract — matches Redis's
                ``scan_iter(match=prefix*)``). If None, clears all items.

        Raises:
            ValueError: If prefix contains invalid characters.
            StorageError: On operational, malformed-response, repeated-cursor, or
                concurrent-write condition loss. Deletion is non-transactional, so
                the clear may already be partially complete.
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
        # The operation lock is the full clear boundary, not merely a generation
        # snapshot guard. Local storage operations and other clears cannot write into
        # an already-scanned page, and disconnect cannot retire or close this client's
        # generation during any conditional claim or delete RPC.
        with self._operation_lock:
            generation = self._generation_for_operation_locked("clear_storage", None)
            table = generation.table
            try:
                # Paginate: a single ``scan`` returns at most ~1 MB per page; without
                # following ``LastEvaluatedKey`` a large table is silently partial-clear
                # (#31). Loop until the scan reports no further page.
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
                    for item in items:
                        self._delete_clear_item(
                            table,
                            item,
                            allow_unfenced_legacy_clear=(
                                generation.snapshot.allow_unfenced_legacy_clear
                            ),
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
    """Context manager that records and swallows ordinary cleanup errors."""

    def __init__(self) -> None:
        self.suppressed_error = False

    def __enter__(self) -> _swallow:
        self.suppressed_error = False
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
        # The caller emits a fixed diagnostic only after this context manager has
        # completed suppression; direct BaseException remains observable.
        self.suppressed_error = True
        return True
