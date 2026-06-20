"""Error-path coverage for DynamoDBBackend (≥98% coverage goal)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

sys.modules.setdefault("boto3", MagicMock())
import boto3  # noqa: E402

from scrapy_extension.backends.dynamodb import DynamoDBBackend  # noqa: E402
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

  def test_store_swallows_exception(self, mocker) -> None:
    b, table = _connected(mocker)
    table.put_item.side_effect = RuntimeError("boom")
    b.store("k", b"v")  # must not raise

  def test_retrieve_swallows_exception(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = RuntimeError("boom")
    assert b.retrieve("k") is None

  def test_delete_swallows_exception(self, mocker) -> None:
    b, table = _connected(mocker)
    table.delete_item.side_effect = RuntimeError("boom")
    assert b.delete("k") is False

  def test_exists_swallows_exception(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = RuntimeError("boom")
    assert b.exists("k") is False

  def test_ttl_swallows_exception(self, mocker) -> None:
    b, table = _connected(mocker)
    table.get_item.side_effect = RuntimeError("boom")
    assert b.ttl("k") is None

  def test_clear_with_prefix_validates(self, mocker) -> None:
    b, table = _connected(mocker)
    table.scan.return_value = {"Items": []}
    b.clear_storage(prefix="foo")
    table.scan.assert_called_once()

  def test_clear_swallows_exception(self, mocker) -> None:
    b, table = _connected(mocker)
    table.scan.side_effect = RuntimeError("boom")
    b.clear_storage()  # must not raise

  def test_retrieve_expired_delete_failure_swallowed(self, mocker) -> None:
    """_swallow catches a delete failure during expired-item cleanup."""
    b, table = _connected(mocker)
    table.get_item.return_value = {"Item": {"pk": "k", "expire_at": 1.0}}
    table.delete_item.side_effect = RuntimeError("delete failed")
    assert b.retrieve("k") is None  # expired; inner delete raised, swallowed
