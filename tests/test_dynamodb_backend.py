"""Tests for DynamoDBBackend (subsystem ③) — mocked boto3.

Injects a mock ``boto3`` into ``sys.modules`` (shared with the SQS test) and
patches the canonical ``boto3.resource`` (module-attribute pattern) to assert
call patterns against a fake Table.
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
from scrapy_extension.exceptions import BackendConnectionError  # noqa: E402
from scrapy_extension.exceptions.base import StorageError  # noqa: E402
from scrapy_extension.settings import DynamoDBMode, DynamoDBSettings  # noqa: E402


def _make_backend(**overrides) -> DynamoDBBackend:
  return DynamoDBBackend(DynamoDBSettings(**overrides))


def _connected(mocker):
  b = _make_backend()
  resource = mocker.MagicMock()
  table = mocker.MagicMock()
  table.load.return_value = None  # table already exists
  resource.Table.return_value = table
  mocker.patch.object(boto3, "resource", return_value=resource)
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
    mocker.patch.object(boto3, "resource", return_value=resource)
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
    mocker.patch.object(boto3, "resource", return_value=resource)
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
    mocker.patch.object(boto3, "resource", return_value=resource)
    with pytest.raises(BackendConnectionError):
      b.connect()
    resource.create_table.assert_called_once()

  def test_connect_failure_raises(self, mocker) -> None:
    b = _make_backend()
    mocker.patch.object(boto3, "resource", side_effect=RuntimeError("boom"))
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

  def test_retrieve_returns_value(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {"Item": {"pk": "key1", "value": b"payload"}}
    assert b.retrieve("key1") == b"payload"

  def test_retrieve_missing_returns_none(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {}
    assert b.retrieve("key1") is None

  def test_retrieve_expired_deletes_and_returns_none(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.return_value = {
      "Item": {"pk": "key1", "value": b"x", "expire_at": 1.0}  # epoch in 1970
    }
    assert b.retrieve("key1") is None
    table.delete_item.assert_called_once_with(Key={"pk": "key1"})

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
    table.delete_item.assert_called_once_with(Key={"pk": "k"})

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
    table.delete_item.assert_called_once_with(Key={"pk": "k"})

  def test_clear_storage_scans_and_deletes(self, mocker) -> None:
    b, table = _connected(mocker)
    table.scan.return_value = {"Items": [{"pk": "a"}, {"pk": "b"}]}
    batch = mocker.MagicMock()
    table.batch_writer.return_value.__enter__.return_value = batch
    b.clear_storage()
    assert batch.delete_item.call_count == 2

  def test_invalid_key_raises(self, mocker) -> None:
    b, _ = _connected(mocker)
    with pytest.raises(ValueError):
      b.store("bad key!", b"x")


# ---------------------------------------------------------------------------
# R14-A: StorageBackend error-contract uniformity.
# DynamoDB storage ops must raise StorageError on operational failures
# (throttling / throughput / limit). Only ResourceNotFoundException is a
# genuine "missing" signal and may be swallowed (returns None/False).
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

  def test_delete_resource_not_found_returns_false(self, mocker) -> None:
    """ResourceNotFoundException is a genuine 'missing' signal — keep swallowing."""
    b, table = _connected(mocker)
    table.delete_item.side_effect = _make_client_error("ResourceNotFoundException")
    assert b.delete("key1") is False

  def test_retrieve_resource_not_found_returns_none(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = _make_client_error("ResourceNotFoundException")
    assert b.retrieve("key1") is None

  def test_storage_error_is_backend_error_subclass(self, mocker) -> None:
    from scrapy_extension.exceptions.base import BackendError

    b, table = _connected(mocker)
    table.put_item.side_effect = _make_client_error("ThrottlingException")
    with pytest.raises(BackendError):
      b.store("key1", b"value")


# ---------------------------------------------------------------------------
# SEC-1 (round-6): DynamoDB AWS creds redaction in boto3.resource kwargs.
# SEC-7: AWS credentials must be both-or-neither (XOR validation).
# ---------------------------------------------------------------------------


def test_dynamodb_credentials_redacted_in_resource_kwargs(mocker):
  """SEC-1: aws_access_key_id / aws_secret_access_key handed to
  boto3.resource are wrapped in _RedactedStr so ``repr(call_args)`` doesn't
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

  mocker.patch.object(boto3, "resource", side_effect=_fake_resource)
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
    """Both set → no ConfigurationError; boto3.resource called."""
    backend = _make_backend(
      aws_access_key_id="AKIAEXAMPLEKEY",
      aws_secret_access_key="top-secret",
    )
    fake_resource = mocker.MagicMock()
    fake_resource.Table.return_value.load.side_effect = _make_client_error(
      "ResourceNotFoundException"
    )
    mocker.patch.object(boto3, "resource", return_value=fake_resource)
    backend.connect()  # must not raise
    boto3.resource.assert_called_once()

  def test_neither_set_proceeds(self, mocker):
    """Neither set → no ConfigurationError; boto3 default credential chain."""
    backend = _make_backend()  # defaults: both None
    fake_resource = mocker.MagicMock()
    fake_resource.Table.return_value.load.side_effect = _make_client_error(
      "ResourceNotFoundException"
    )
    mocker.patch.object(boto3, "resource", return_value=fake_resource)
    backend.connect()  # must not raise
    _, kwargs = boto3.resource.call_args.args, boto3.resource.call_args.kwargs
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs
