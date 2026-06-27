"""Error-path coverage for DynamoDBBackend (≥98% coverage goal)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

sys.modules.setdefault("boto3", MagicMock())
import boto3  # noqa: E402
import pytest  # noqa: E402

from scrapy_extension.backends.dynamodb import DynamoDBBackend  # noqa: E402
from scrapy_extension.exceptions.base import StorageError  # noqa: E402
from scrapy_extension.settings import DynamoDBSettings  # noqa: E402


def _connected(mocker, **overrides):
  b = DynamoDBBackend(DynamoDBSettings(**overrides))
  resource = mocker.MagicMock()
  table = mocker.MagicMock()
  table.load.return_value = None
  resource.Table.return_value = table
  mocker.patch.object(boto3, "resource", return_value=resource)
  b.connect()
  return b, table


class TestDynamoDBErrorPaths:
  def test_connect_with_credentials(self, mocker) -> None:
    b, _ = _connected(mocker, aws_access_key_id="k", aws_secret_access_key="s")
    assert b.is_connected()

  def test_ping_failure(self, mocker) -> None:
    b, table = _connected(mocker)
    table.load.side_effect = RuntimeError("down")
    assert b.ping() is False

  def test_disconnect(self, mocker) -> None:
    b, _ = _connected(mocker)
    b.disconnect()
    assert b.is_connected() is False

  def test_store_raises_storage_error(self, mocker) -> None:
    # R14-A: storage ops raise StorageError instead of silently swallowing
    # (the old swallow masked throttling/throughput failures as "success").
    b, table = _connected(mocker)
    table.put_item.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError) as exc_info:
      b.store("k", b"v")
    assert exc_info.value.operation == "store"
    assert exc_info.value.key == "k"

  def test_retrieve_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError) as exc_info:
      b.retrieve("k")
    assert exc_info.value.operation == "retrieve"

  def test_delete_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.delete_item.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError):
      b.delete("k")

  def test_exists_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError):
      b.exists("k")

  def test_ttl_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError):
      b.ttl("k")

  def test_clear_with_prefix_validates(self, mocker) -> None:
    b, table = _connected(mocker)
    table.scan.return_value = {"Items": []}
    b.clear_storage(prefix="foo")
    table.scan.assert_called_once()

  def test_clear_raises_storage_error(self, mocker) -> None:
    b, table = _connected(mocker)
    table.scan.side_effect = RuntimeError("boom")
    with pytest.raises(StorageError):
      b.clear_storage()

  def test_retrieve_expired_delete_failure_swallowed(self, mocker) -> None:
    """_swallow catches a delete failure during expired-item cleanup."""
    b, table = _connected(mocker)
    table.get_item.return_value = {"Item": {"pk": "k", "expire_at": 1.0}}
    table.delete_item.side_effect = RuntimeError("delete failed")
    assert b.retrieve("k") is None  # expired; inner delete raised, swallowed
