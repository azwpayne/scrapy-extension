"""Resilience / contract tests for DynamoDBBackend (initiative #28).

dynamodb.py was 90.50% (11 uncovered lines + 8 partial branches), below
the 95% floor. Every gap was a real documented contract with no direct
test:

- ``_is_resource_not_found`` returning False when ``Error`` is not a dict
  (defensive type-narrowing, line 67).
- connect() SEC-7 credential XOR-validation (lines 106-108) — defense-in-
  depth: DynamoDBSettings already validates at construction, so connect()'s
  re-check fires only when settings validation is bypassed (post-construction
  mutation). Same shape as SqsBackend (#27).
- connect() wiring ``endpoint_url`` into the boto3 resource (line 118,
  LocalStack path).
- ping() both branches: False when the table handle is None (line 154),
  True on a successful ``table.load()`` (line 157).
- store()/exists()/ttl() distinguish item absence from a vanished table:
  ResourceNotFoundException is a resource-level StorageError on every op.
- exists() returning False when the item is absent (line 285).
"""

from __future__ import annotations

from typing import Any

import pytest

from scrapy_extension.backends.dynamodb import (
  DynamoDBBackend,
  _is_resource_not_found,
)
from scrapy_extension.exceptions import ConfigurationError, StorageError
from scrapy_extension.settings import DynamoDBSettings


class _FakeClientError(Exception):
  """Mimics ``botocore.exceptions.ClientError`` for the ResourceNotFound paths
  (``_is_resource_not_found`` reads ``exc.response["Error"]["Code"]``)."""

  def __init__(self, code: str) -> None:
    super().__init__(code)
    self.response: dict[str, Any] = {"Error": {"Code": code}}


def _backend() -> DynamoDBBackend:
  """Constructed-but-not-connected backend (_resource / _table are None)."""
  return DynamoDBBackend(DynamoDBSettings())


# ---------------------------------------------------------------------------
# _is_resource_not_found type-narrowing (line 67)
# ---------------------------------------------------------------------------


def test_is_resource_not_found_false_when_error_is_not_a_dict() -> None:
  """Line 67: a ``response`` whose ``Error`` value is not a dict returns False
  (defensive type-narrowing — a malformed ClientError must not be mistaken
  for a ResourceNotFound signal and silently treated as 'missing')."""
  class _WeirdError(Exception):
    response = {"Error": "not-a-dict"}  # Error value is a string, not dict

  assert _is_resource_not_found(_WeirdError()) is False
  # Sanity contrast — a well-formed ResourceNotFound IS detected:
  assert _is_resource_not_found(_FakeClientError("ResourceNotFoundException")) is True


# ---------------------------------------------------------------------------
# connect() SEC-7 credential XOR (lines 106-108, defense-in-depth)
# ---------------------------------------------------------------------------


def test_connect_rejects_partial_credentials_access_key_only() -> None:
  """Lines 106-108 (SEC-7, defense-in-depth): connect() re-checks the
  both-or-neither credential invariant even though DynamoDBSettings already
  validates it at construction — so a backend whose config is mutated
  post-construction still fails fast rather than silently using boto3's
  default credential chain. Reached by mutating the config."""
  from pydantic import SecretStr

  backend = _backend()
  backend.config.aws_access_key_id = SecretStr("ak")  # bypass settings validation
  with pytest.raises(ConfigurationError) as exc:
    backend.connect()
  assert "aws_secret_access_key" in str(exc.value)


# ---------------------------------------------------------------------------
# connect() endpoint_url wiring (line 118)
# ---------------------------------------------------------------------------


def test_connect_passes_endpoint_url_into_boto3_resource(mocker) -> None:
  """Line 118: when ``endpoint_url`` is set (LocalStack), it is forwarded to
  ``boto3.resource`` so local dev routes correctly."""
  mock_boto3 = mocker.patch("scrapy_extension.backends.dynamodb.boto3")
  # Table.load() succeeds (no raise) -> existing-table path, connect completes:
  mock_boto3.resource.return_value.Table.return_value.load.return_value = None
  backend = DynamoDBBackend(DynamoDBSettings(endpoint_url="http://localhost:4566"))
  backend.connect()
  _, kwargs = mock_boto3.resource.call_args
  assert kwargs["endpoint_url"] == "http://localhost:4566"


# ---------------------------------------------------------------------------
# ping() both branches (lines 154, 157)
# ---------------------------------------------------------------------------


def test_ping_returns_false_when_table_handle_is_none() -> None:
  """Line 154: ping() with no table handle returns False without attempting
  ``table.load()`` (no AttributeError on None)."""
  backend = _backend()
  backend._table = None
  assert backend.ping() is False


def test_ping_returns_true_on_successful_load(mocker) -> None:
  """Line 157: ping() returns True when ``table.load()`` succeeds — the
  health-check contract for a connected backend."""
  backend = _backend()
  backend._table = mocker.MagicMock()
  assert backend.ping() is True
  backend._table.load.assert_called_once()


# ---------------------------------------------------------------------------
# store() ResourceNotFound -> StorageError (line 196)
# ---------------------------------------------------------------------------


def test_store_raises_storage_error_when_table_vanishes(mocker) -> None:
  """Line 196: a vanished table mid-``put_item`` (ResourceNotFoundException)
  is a StorageError (NOT silently swallowed) — preserves at-least-once: the
  caller (item pipeline) must see the failure, not a silent data loss."""
  backend = _backend()
  backend._table = mocker.MagicMock()
  backend._table.put_item.side_effect = _FakeClientError("ResourceNotFoundException")
  with pytest.raises(StorageError, match="table not found"):
    backend.store("k", b"v")


# ---------------------------------------------------------------------------
# exists() distinguishes vanished table from missing item
# ---------------------------------------------------------------------------


def test_exists_raises_storage_error_when_table_vanishes(mocker) -> None:
  backend = _backend()
  backend._table = mocker.MagicMock()
  error = _FakeClientError("ResourceNotFoundException")
  backend._table.get_item.side_effect = error
  with pytest.raises(StorageError) as exc_info:
    backend.exists("k")
  assert exc_info.value.operation == "exists"
  assert exc_info.value.__cause__ is error


def test_exists_returns_false_when_item_is_absent(mocker) -> None:
  """Line 285: a get_item response with no ``Item`` → False (key not present)."""
  backend = _backend()
  backend._table = mocker.MagicMock()
  backend._table.get_item.return_value = {}  # no "Item"
  assert backend.exists("k") is False


# ---------------------------------------------------------------------------
# ttl() treats a vanished table as operational failure
# ---------------------------------------------------------------------------


def test_ttl_raises_storage_error_when_table_vanishes(mocker) -> None:
  backend = _backend()
  backend._table = mocker.MagicMock()
  error = _FakeClientError("ResourceNotFoundException")
  backend._table.get_item.side_effect = error
  with pytest.raises(StorageError) as exc_info:
    backend.ttl("k")
  assert exc_info.value.operation == "ttl"
  assert exc_info.value.__cause__ is error
