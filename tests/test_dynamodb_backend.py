"""Tests for DynamoDBBackend (subsystem ③) — mocked boto3.

Injects a mock ``boto3`` into ``sys.modules`` (shared with the SQS test) and
patches the private candidate ``Session.resource`` call to assert call patterns
against a fake Table.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("boto3", MagicMock())
import boto3  # noqa: E402 — the (mocked) module actually in sys.modules


@pytest.fixture(scope="module", autouse=True)
def _cleanup_sys_modules_mock_boto3():
  """Pop the module-level ``boto3`` mock after this module's tests finish.

  R14-G flake fix: the module-top-level ``sys.modules.setdefault`` runs at
  collection time and persists for the whole session, polluting later test
  modules. Popping the injected key at module teardown restores a clean
  ``sys.modules`` for subsequent modules.
  """
  yield
  sys.modules.pop("boto3", None)

from scrapy_extension.backends.base import (  # noqa: E402
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.dynamodb import DynamoDBBackend  # noqa: E402
from scrapy_extension.exceptions import (  # noqa: E402
  BackendConnectionError,
  ConfigurationError,
)
from scrapy_extension.exceptions.base import StorageError  # noqa: E402
from scrapy_extension.settings import DynamoDBMode, DynamoDBSettings  # noqa: E402


def _make_backend(**overrides) -> DynamoDBBackend:
  return DynamoDBBackend(DynamoDBSettings(**overrides))


def _patch_resource(mocker, *, return_value=None, side_effect=None):
  """Patch the private candidate Session and return its resource() mock."""
  session = mocker.MagicMock()
  if side_effect is not None:
    session.resource.side_effect = side_effect
  else:
    session.resource.return_value = return_value
  session_factory = mocker.patch.object(
    boto3.session, "Session", return_value=session
  )
  return session_factory, session.resource


def _connected(mocker):
  b = _make_backend()
  resource = mocker.MagicMock()
  table = mocker.MagicMock()
  table.load.return_value = None  # table already exists
  table.table_status = "ACTIVE"
  resource.Table.return_value = table
  _patch_resource(mocker, return_value=resource)
  b.connect()
  return b, table


class TestDynamoDBBackendType:
  def test_backend_type_is_dynamodb(self) -> None:
    assert _make_backend().backend_type is BackendType.DYNAMODB

  def test_storage_only(self) -> None:
    b = _make_backend()
    assert isinstance(b, StorageBackend)
    assert not isinstance(b, QueueBackend)
    assert not isinstance(b, SetBackend)

  def test_settings_defaults(self) -> None:
    s = DynamoDBSettings()
    assert s.mode is DynamoDBMode.STANDALONE
    assert s.table_name == "scrapy-extension"


class TestDynamoDBConnect:
  def test_unsupported_mode_is_configuration_error(self) -> None:
    b = _make_backend()
    b.config.mode = "unsupported"  # type: ignore[assignment]

    with pytest.raises(ConfigurationError) as exc_info:
      b.connect()

    assert exc_info.value.setting_name == "mode"

  def test_connect_loads_existing_table(self, mocker) -> None:
    b, table = _connected(mocker)
    table.load.assert_called_once()
    assert b.is_connected() is True

  def test_connect_creates_table_when_missing(self, mocker) -> None:
    b = _make_backend()
    resource = mocker.MagicMock()
    new_table = mocker.MagicMock()
    existing = mocker.MagicMock()
    existing.load.side_effect = _make_client_error("ResourceNotFoundException")  # triggers create
    resource.Table.return_value = existing
    resource.create_table.return_value = new_table
    _patch_resource(mocker, return_value=resource)
    b.connect()
    resource.create_table.assert_called_once()
    new_table.wait_until_exists.assert_called_once()

  def test_connect_concurrent_create_table_race(self, mocker) -> None:
    # Two workers boot concurrently; both see ResourceNotFoundException from
    # load(); the loser's create_table raises ResourceInUseException — connect
    # must reattach to the existing (in-creation) table instead of failing.
    b = _make_backend()
    resource = mocker.MagicMock()
    loser_table = mocker.MagicMock()
    loser_table.load.side_effect = _make_client_error("ResourceNotFoundException")
    reattached = mocker.MagicMock()
    resource.Table.side_effect = [loser_table, reattached]
    resource.create_table.side_effect = _make_client_error("ResourceInUseException")
    _patch_resource(mocker, return_value=resource)
    b.connect()  # must not raise BackendConnectionError
    resource.create_table.assert_called_once()
    reattached.wait_until_exists.assert_called_once()
    assert b.is_connected() is True

  def test_connect_non_resource_in_use_create_error_propagates(self, mocker) -> None:
    # Negative test (review feedback): a non-ResourceInUse create_table error
    # (e.g. LimitExceededException) must NOT be misread as a race — it
    # propagates as BackendConnectionError so real failures surface.
    b = _make_backend()
    resource = mocker.MagicMock()
    existing = mocker.MagicMock()
    existing.load.side_effect = _make_client_error("ResourceNotFoundException")
    resource.Table.return_value = existing
    resource.create_table.side_effect = _make_client_error("LimitExceededException")
    _patch_resource(mocker, return_value=resource)
    with pytest.raises(BackendConnectionError):
      b.connect()
    resource.create_table.assert_called_once()
    assert b._resource is None
    assert b._table is None
    assert b.is_connected() is False

  def test_connect_failure_raises(self, mocker) -> None:
    b = _make_backend()
    _patch_resource(mocker, side_effect=RuntimeError("boom"))
    with pytest.raises(BackendConnectionError):
      b.connect()


class TestDynamoDBStorageOps:
  def test_store_without_ttl(self, mocker) -> None:
    b, table = _connected(mocker)
    b.store("key1", b"value")
    args, kwargs = table.put_item.call_args
    assert kwargs["Item"] == {"pk": "key1", "value": b"value"}

  def test_store_with_ttl_sets_expire_at(self, mocker) -> None:
    b, table = _connected(mocker)
    b.store("key1", b"value", ttl=60)
    item = table.put_item.call_args.kwargs["Item"]
    assert item["pk"] == "key1"
    assert item["value"] == b"value"
    assert "expire_at" in item

  def test_store_with_ttl_uses_non_early_integer_epoch(self, mocker) -> None:
    b, table = _connected(mocker)
    mocker.patch(
      "scrapy_extension.backends.dynamodb.time.time", return_value=1_000.25
    )

    b.store("key1", b"value", ttl=60)

    expire_at = table.put_item.call_args.kwargs["Item"]["expire_at"]
    assert type(expire_at) is int
    assert expire_at == 1_061

  def test_store_enforces_complete_400_kib_item_limit_before_io(
    self, mocker
  ) -> None:
    b, table = _connected(mocker)
    # Item size is attribute names plus values: ``pk`` (2), key (1),
    # ``value`` (5), and raw binary bytes. Exactly 400 KiB is accepted.
    largest_value = b"x" * (400 * 1024 - 2 - 1 - 5)

    b.store("k", largest_value)

    table.put_item.assert_called_once()
    with pytest.raises(ValueError, match="400 KiB"):
      b.store("k", largest_value + b"x")
    assert table.put_item.call_count == 1

  def test_store_rejects_partition_key_over_2048_bytes_before_io(
    self, mocker
  ) -> None:
    b, table = _connected(mocker)

    with pytest.raises(ValueError, match="2,048 UTF-8 bytes"):
      b.store("k" * 2049, b"value")

    table.put_item.assert_not_called()

  def test_retrieve_returns_value(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {"Item": {"pk": "key1", "value": b"payload"}}
    assert b.retrieve("key1") == b"payload"

  def test_retrieve_converts_boto3_binary_to_bytes(self, mocker) -> None:
    class _Boto3Binary:
      value = b"payload"

      def __bytes__(self) -> bytes:
        return self.value

    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "key1", "value": _Boto3Binary()}
    }

    assert b.retrieve("key1") == b"payload"

  @pytest.mark.parametrize("stored_value", [None, "text", object()])
  def test_retrieve_rejects_malformed_persisted_value(
    self, mocker, stored_value
  ) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "key1", "value": stored_value}
    }

    with pytest.raises(StorageError) as exc_info:
      b.retrieve("key1")

    assert exc_info.value.operation == "retrieve"
    assert exc_info.value.key == "key1"

  @pytest.mark.parametrize(
    ("method_name", "operation"),
    [("retrieve", "retrieve"), ("exists", "exists"), ("ttl", "ttl")],
  )
  @pytest.mark.parametrize("expire_at", [True, "tomorrow", float("nan")])
  def test_reads_reject_malformed_persisted_expiry(
    self, mocker, method_name, operation, expire_at
  ) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "key1", "value": b"payload", "expire_at": expire_at}
    }

    with pytest.raises(StorageError) as exc_info:
      getattr(b, method_name)("key1")

    assert exc_info.value.operation == operation
    assert exc_info.value.key == "key1"
    table.delete_item.assert_not_called()

  def test_retrieve_missing_returns_none(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {}
    assert b.retrieve("key1") is None

  def test_retrieve_uses_consistent_read(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {}

    assert b.retrieve("key1") is None

    table.get_item.assert_called_once_with(
      Key={"pk": "key1"}, ConsistentRead=True
    )

  def test_retrieve_expired_deletes_and_returns_none(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "key1", "value": b"x", "expire_at": 1.0}  # epoch in 1970
    }
    assert b.retrieve("key1") is None
    table.delete_item.assert_called_once_with(
      Key={"pk": "key1"},
      ConditionExpression="expire_at = :exp",
      ExpressionAttributeValues={":exp": 1.0},
    )

  def test_delete_returns_bool(self, mocker) -> None:
    b, table = _connected(mocker)
    table.delete_item.return_value = {"Attributes": {"pk": "key1"}}
    assert b.delete("key1") is True
    table.delete_item.assert_called_once_with(
      Key={"pk": "key1"}, ReturnValues="ALL_OLD"
    )

  def test_delete_missing_returns_false(self, mocker) -> None:
    b, table = _connected(mocker)
    table.delete_item.return_value = {}
    assert b.delete("key1") is False

  def test_exists_true_for_current(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {"Item": {"pk": "k", "value": b"x"}}
    assert b.exists("k") is True

  def test_exists_uses_consistent_read(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {}

    assert b.exists("k") is False

    table.get_item.assert_called_once_with(Key={"pk": "k"}, ConsistentRead=True)

  def test_exists_false_for_expired(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {"Item": {"pk": "k", "expire_at": 1.0}}
    assert b.exists("k") is False

  def test_exists_lazy_deletes_expired_item(self, mocker) -> None:
    # Symmetry with retrieve(): exists() must lazy-reap expired rows, not
    # just return False while leaving the dead row in the table to accumulate.
    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "k", "value": b"x", "expire_at": 1.0}  # epoch in 1970
    }
    assert b.exists("k") is False
    table.delete_item.assert_called_once_with(
      Key={"pk": "k"},
      ConditionExpression="expire_at = :exp",
      ExpressionAttributeValues={":exp": 1.0},
    )

  def test_ttl_returns_remaining(self, mocker) -> None:
    b, table = _connected(mocker)
    future = 9999999999.0  # year 2286
    table.get_item.return_value = {"Item": {"pk": "k", "expire_at": future}}
    assert b.ttl("k") is not None
    assert b.ttl("k") >= 0

  def test_ttl_none_without_expire_at(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {"Item": {"pk": "k", "value": b"x"}}
    assert b.ttl("k") is None

  def test_ttl_none_when_item_is_missing(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {}
    assert b.ttl("missing") is None
    table.delete_item.assert_not_called()

  def test_ttl_uses_consistent_read(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {}

    assert b.ttl("k") is None

    table.get_item.assert_called_once_with(Key={"pk": "k"}, ConsistentRead=True)

  def test_ttl_none_with_null_expire_at(self, mocker) -> None:
    """A persisted null expiry is the same permanent-value sentinel as absence."""
    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "k", "value": b"x", "expire_at": None}
    }

    assert b.ttl("k") is None
    table.delete_item.assert_not_called()

  def test_ttl_lazy_deletes_expired_item_and_returns_none(self, mocker) -> None:
    # R-dynttl: symmetry with retrieve()/exists() — ttl() must lazy-reap
    # expired rows AND return None (consistent "expired = absent"), not return
    # 0 while leaving the dead row to accumulate. Pre-fix: ttl() returned 0
    # for an expired key AND did not reap, so an operator couldn't tell a
    # genuinely-about-to-expire key (0) from one that expired long ago, and
    # the dead row lingered until a retrieve/exists/clear_storage touched it.
    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "k", "value": b"x", "expire_at": 1.0}  # epoch in 1970
    }
    assert b.ttl("k") is None
    table.delete_item.assert_called_once_with(
      Key={"pk": "k"},
      ConditionExpression="expire_at = :exp",
      ExpressionAttributeValues={":exp": 1.0},
    )

  def test_lazy_reap_delete_is_conditional_cas(self, mocker) -> None:
    """R-dyncas: the lazy-reap delete must be a CAS on ``expire_at``.

    Pre-fix ``_lazy_reap_if_expired`` did ``delete_item(Key={"pk": key})`` from
    a stale ``get_item`` snapshot. A concurrent ``store()`` (``put_item``)
    between the read and the reap would overwrite the key with a fresh value,
    and the unconditional delete clobbered the fresh write -> data loss. The
    CAS (``ConditionExpression="expire_at = :exp"``) makes a concurrent
    overwrite fail the condition (``ConditionalCheckFailedException``, swallowed
    by ``_swallow``) so the fresh item survives. ``expire_at`` is guaranteed
    non-None here (``_is_expired`` returns False when None).
    """
    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "k", "value": b"x", "expire_at": 1.0}  # epoch in 1970
    }
    assert b.retrieve("k") is None
    table.delete_item.assert_called_once_with(
      Key={"pk": "k"},
      ConditionExpression="expire_at = :exp",
      ExpressionAttributeValues={":exp": 1.0},
    )

  def test_clear_storage_scans_and_deletes(self, mocker) -> None:
    b, table = _connected(mocker)
    table.scan.return_value = {"Items": [{"pk": "a"}, {"pk": "b"}]}
    batch = mocker.MagicMock()
    table.batch_writer.return_value.__enter__.return_value = batch
    b.clear_storage()
    assert batch.delete_item.call_count == 2

  def test_clear_storage_deletes_every_scan_page(self, mocker) -> None:
    b, table = _connected(mocker)
    page_cursor = {"pk": "tenant_a:first"}
    table.scan.side_effect = [
      {
        "Items": [{"pk": "tenant_a:first"}],
        "LastEvaluatedKey": page_cursor,
      },
      {"Items": [{"pk": "tenant_a:second"}]},
    ]
    batch = mocker.MagicMock()
    table.batch_writer.return_value.__enter__.return_value = batch

    b.clear_storage(prefix="tenant_a:")

    assert [
      delete_call.kwargs["Key"]
      for delete_call in batch.delete_item.call_args_list
    ] == [{"pk": "tenant_a:first"}, {"pk": "tenant_a:second"}]
    assert table.scan.call_count == 2
    assert table.scan.call_args_list[1].kwargs["ExclusiveStartKey"] == page_cursor

  def test_clear_storage_uses_consistent_read_on_every_page(self, mocker) -> None:
    b, table = _connected(mocker)
    page_cursor = {"pk": "tenant_a:first"}
    table.scan.side_effect = [
      {"Items": [], "LastEvaluatedKey": page_cursor},
      {"Items": []},
    ]

    b.clear_storage(prefix="tenant_a:")

    assert table.scan.call_count == 2
    for scan_call in table.scan.call_args_list:
      assert scan_call.kwargs["ConsistentRead"] is True
    assert table.scan.call_args_list[1].kwargs["ExclusiveStartKey"] == page_cursor

  def test_clear_storage_rejects_empty_prefix_before_aws_io(self, mocker) -> None:
    b, table = _connected(mocker)
    table.scan.return_value = {"Items": []}

    with pytest.raises(ValueError, match="Invalid prefix"):
      b.clear_storage(prefix="")

    table.scan.assert_not_called()
    table.batch_writer.assert_not_called()

  def test_clear_storage_prefix_applies_filter_expression(self, mocker) -> None:
    """R-dynprefix: clear_storage(prefix) scopes the scan via FilterExpression.

    Pre-fix the prefix was validated then IGNORED -- scan+delete wiped the entire
    table (``clear_storage("tenant_a:")`` nuked every tenant), violating the
    StorageBackend ABC contract ("only clear keys starting with this prefix")
    and Redis parity. Now the scan carries ``begins_with(pk, :p)`` so only
    matching keys are deleted.
    """
    b, table = _connected(mocker)
    table.scan.return_value = {"Items": []}
    b.clear_storage(prefix="tenant_a:")
    table.scan.assert_called_once()
    kwargs = table.scan.call_args.kwargs
    assert kwargs["FilterExpression"] == "begins_with(pk, :p)"
    assert kwargs["ExpressionAttributeValues"] == {":p": "tenant_a:"}

  def test_invalid_key_raises(self, mocker) -> None:
    b, _ = _connected(mocker)
    with pytest.raises(ValueError):
      b.store("bad key!", b"x")

  @pytest.mark.parametrize(
    ("method_name", "table_method"),
    [
      ("retrieve", "get_item"),
      ("delete", "delete_item"),
      ("exists", "get_item"),
      ("ttl", "get_item"),
    ],
  )
  def test_key_operations_reject_partition_key_over_2048_bytes_before_io(
    self, mocker, method_name, table_method
  ) -> None:
    b, table = _connected(mocker)

    with pytest.raises(ValueError, match="2,048 UTF-8 bytes"):
      getattr(b, method_name)("k" * 2049)

    getattr(table, table_method).assert_not_called()


# ---------------------------------------------------------------------------
# R14-A: StorageBackend error-contract uniformity.
# DynamoDB storage ops must raise StorageError on operational failures
# (throttling / throughput / limit), including a vanished table. Missing
# items are represented by successful responses without Item/Attributes.
# ---------------------------------------------------------------------------


def _make_client_error(code: str):
  """Build a minimal stand-in for botocore.exceptions.ClientError."""
  err = {"Error": {"Code": code, "Message": f"{code} hit"}}
  e = Exception(f"An error occurred ({code})")
  e.response = err  # type: ignore[attr-defined]
  return e


class TestDynamoDBStorageErrorContract:
  def test_delete_throttling_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.delete_item.side_effect = _make_client_error("ThrottlingException")
    with pytest.raises(StorageError) as exc_info:
      b.delete("key1")
    assert exc_info.value.operation == "delete"
    assert exc_info.value.key == "key1"

  def test_store_provisioned_throughput_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.put_item.side_effect = _make_client_error(
      "ProvisionedThroughputExceededException"
    )
    with pytest.raises(StorageError) as exc_info:
      b.store("key1", b"value")
    assert exc_info.value.operation == "store"

  def test_retrieve_limit_exceeded_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = _make_client_error("LimitExceededException")
    with pytest.raises(StorageError):
      b.retrieve("key1")

  def test_exists_client_error_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = _make_client_error("ThrottlingException")
    with pytest.raises(StorageError):
      b.exists("key1")

  def test_ttl_client_error_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = _make_client_error("LimitExceededException")
    with pytest.raises(StorageError):
      b.ttl("key1")

  def test_clear_storage_client_error_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.scan.side_effect = _make_client_error("ThrottlingException")
    with pytest.raises(StorageError):
      b.clear_storage()

  def test_delete_resource_not_found_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    error = _make_client_error("ResourceNotFoundException")
    table.delete_item.side_effect = error

    with pytest.raises(StorageError) as exc_info:
      b.delete("key1")

    assert exc_info.value.operation == "delete"
    assert exc_info.value.key == "key1"
    assert exc_info.value.__cause__ is error

  def test_retrieve_resource_not_found_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    error = _make_client_error("ResourceNotFoundException")
    table.get_item.side_effect = error

    with pytest.raises(StorageError) as exc_info:
      b.retrieve("key1")

    assert exc_info.value.operation == "retrieve"
    assert exc_info.value.key == "key1"
    assert exc_info.value.__cause__ is error

  def test_storage_error_is_backend_error_subclass(self, mocker) -> None:
    from scrapy_extension.exceptions.base import BackendError

    b, table = _connected(mocker)
    table.put_item.side_effect = _make_client_error("ThrottlingException")
    with pytest.raises(BackendError):
      b.store("key1", b"value")


# ---------------------------------------------------------------------------
# SEC-1 (round-6): DynamoDB AWS creds redaction in Session.resource kwargs.
# SEC-7: AWS credentials must be both-or-neither (XOR validation).
# ---------------------------------------------------------------------------


def test_dynamodb_credentials_redacted_in_resource_kwargs(mocker):
  """SEC-1: aws_access_key_id / aws_secret_access_key handed to
  Session.resource kwargs are wrapped in _RedactedStr so ``repr(call_args)`` doesn't
  leak them. The str values are preserved so boto3 still authenticates.
  """
  from scrapy_extension.backends._redaction import _RedactedStr
  from scrapy_extension.settings import DynamoDBSettings

  config = DynamoDBSettings(
    aws_access_key_id="AKIAEXAMPLEKEY",
    aws_secret_access_key="top-secret-ddb-secret",
  )
  backend = DynamoDBBackend(config)

  captured: dict[str, object] = {}

  class _FakeResource:
    def Table(self, name: str) -> object:
      table = mocker.MagicMock()
      table.load.side_effect = _make_client_error("ResourceNotFoundException")
      return table

    def create_table(self, **kwargs: object) -> object:
      created = mocker.MagicMock()
      created.wait_until_exists.return_value = None
      return created

  def _fake_resource(service: str, **kwargs: object) -> _FakeResource:
    captured.update(kwargs)
    return _FakeResource()

  _patch_resource(mocker, side_effect=_fake_resource)
  # Table creation path also needs stubbing to avoid real wait_until_exists.
  backend.connect()
  key = captured["aws_access_key_id"]
  secret = captured["aws_secret_access_key"]
  # Values preserved for boto3 auth.
  assert str(key) == "AKIAEXAMPLEKEY"
  assert str(secret) == "top-secret-ddb-secret"
  # But repr of the captured kwargs hides both.
  assert "AKIAEXAMPLEKEY" not in repr(captured)
  assert "top-secret-ddb-secret" not in repr(captured)
  assert isinstance(key, _RedactedStr)
  assert isinstance(secret, _RedactedStr)


class TestDynamoDBHalfCredentialGuard:
  """SEC-7: AWS credentials must be both-or-neither (see SqsBackend test)."""

  def test_key_without_secret_raises(self):
    from scrapy_extension.exceptions import ConfigurationError

    # SV3-6: half-cred guard now fires at config (DynamoDBSettings
    # construction), ahead of the connect-path SEC-7 defense-in-depth guard.
    with pytest.raises(ConfigurationError) as exc_info:
      _make_backend(
        aws_access_key_id="AKIAEXAMPLEKEY",
        aws_secret_access_key=None,
      )
    assert "aws_secret_access_key" in str(exc_info.value)
    assert exc_info.value.setting_name == "aws_secret_access_key"

  def test_secret_without_key_raises(self):
    from scrapy_extension.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError) as exc_info:
      _make_backend(
        aws_access_key_id=None,
        aws_secret_access_key="orphan-secret",
      )
    assert "aws_access_key_id" in str(exc_info.value)
    assert exc_info.value.setting_name == "aws_access_key_id"

  def test_both_set_proceeds(self, mocker):
    """Both set → no ConfigurationError; Session.resource called."""
    backend = _make_backend(
      aws_access_key_id="AKIAEXAMPLEKEY",
      aws_secret_access_key="top-secret",
    )
    fake_resource = mocker.MagicMock()
    fake_resource.Table.return_value.load.side_effect = _make_client_error(
      "ResourceNotFoundException"
    )
    _, resource_factory = _patch_resource(mocker, return_value=fake_resource)
    backend.connect()  # must not raise
    resource_factory.assert_called_once()

  def test_neither_set_proceeds(self, mocker):
    """Neither set → no ConfigurationError; boto3 default credential chain."""
    backend = _make_backend()  # defaults: both None
    fake_resource = mocker.MagicMock()
    fake_resource.Table.return_value.load.side_effect = _make_client_error(
      "ResourceNotFoundException"
    )
    _, resource_factory = _patch_resource(mocker, return_value=fake_resource)
    backend.connect()  # must not raise
    kwargs = resource_factory.call_args.kwargs
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs

  @pytest.mark.parametrize(
    "endpoint_url",
    [
      "http://aws-proxy.internal:4566",
      "https://operator:do-not-leak@aws-proxy.internal",
    ],
  )
  def test_connect_revalidates_mutated_cloud_endpoint(
    self, mocker, endpoint_url
  ) -> None:
    backend = _make_backend(mode=DynamoDBMode.CLOUD)
    backend.config.endpoint_url = endpoint_url
    session_factory, _ = _patch_resource(
      mocker, return_value=mocker.MagicMock()
    )

    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()

    assert "do-not-leak" not in str(exc_info.value)
    session_factory.assert_not_called()

  def test_connect_rejects_mutated_empty_explicit_credentials(self, mocker) -> None:
    backend = _make_backend()
    backend.config.aws_access_key_id = ""  # type: ignore[assignment]
    backend.config.aws_secret_access_key = ""  # type: ignore[assignment]
    session_factory, _ = _patch_resource(
      mocker, return_value=mocker.MagicMock()
    )

    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()

    assert exc_info.value.setting_name == "aws_access_key_id"
    session_factory.assert_not_called()

  def test_connect_rejects_mutated_missing_standalone_endpoint(self, mocker) -> None:
    backend = _make_backend()
    backend.config.endpoint_url = None
    session_factory, _ = _patch_resource(
      mocker, return_value=mocker.MagicMock()
    )

    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()

    assert exc_info.value.setting_name == "endpoint_url"
    session_factory.assert_not_called()
