"""Revision-fenced DynamoDB clear contracts."""

from __future__ import annotations

import threading
from typing import Any

import pytest

from scrapy_extension.backends import dynamodb as dynamodb_module
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import DynamoDBSettings

_REVISION = "_scrapy_revision"
_OLD_REVISION = "0" * 32
_NEW_REVISION = "1" * 32


def _connected(mocker, **settings: Any) -> tuple[DynamoDBBackend, Any, Any]:
    backend = DynamoDBBackend(DynamoDBSettings(**settings))
    session = mocker.MagicMock()
    resource = mocker.MagicMock()
    table = mocker.MagicMock()
    table.load.return_value = None
    table.table_status = "ACTIVE"

    def successful_conditional_delete(**kwargs: Any) -> dict[str, Any]:
        attributes = {"pk": kwargs["Key"]["pk"]}
        values = kwargs.get("ExpressionAttributeValues", {})
        revision = values.get(":revision")
        if revision is not None:
            attributes[_REVISION] = revision
        else:
            for token, name in kwargs["ExpressionAttributeNames"].items():
                if token.startswith("#item"):
                    attributes[name] = values[token.replace("#", ":", 1)]
        return {"Attributes": attributes}

    table.delete_item.side_effect = successful_conditional_delete
    resource.Table.return_value = table
    table.meta.client = resource.meta.client
    session.resource.return_value = resource
    mocker.patch.object(
        dynamodb_module.boto3.session,
        "Session",
        return_value=session,
    )
    backend.connect()
    return backend, table, resource


def _condition_failed() -> Exception:
    error = Exception("condition exposed secret key material")
    error.response = {  # type: ignore[attr-defined]
        "Error": {"Code": "ConditionalCheckFailedException"}
    }
    return error


def _revision_item(key: str, revision: str = _OLD_REVISION) -> dict[str, Any]:
    return {"pk": key, "value": b"value", _REVISION: revision}


def _assert_revision_delete(call: Any, key: str, revision: Any) -> None:
    assert call.kwargs == {
        "Key": {"pk": key},
        "ConditionExpression": "#revision = :revision",
        "ExpressionAttributeNames": {"#revision": _REVISION},
        "ExpressionAttributeValues": {":revision": revision},
        "ReturnValues": "ALL_OLD",
    }


def _join(thread: threading.Thread) -> None:
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_clear_conditions_every_delete_on_exact_observed_revision(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = {
        "Items": [_revision_item("first"), _revision_item("second", _NEW_REVISION)]
    }

    backend.clear_storage()

    assert table.delete_item.call_count == 2
    _assert_revision_delete(table.delete_item.call_args_list[0], "first", _OLD_REVISION)
    _assert_revision_delete(
        table.delete_item.call_args_list[1], "second", _NEW_REVISION
    )
    table.meta.client.batch_write_item.assert_not_called()


@pytest.mark.parametrize("replacement_has_revision", [True, False])
def test_external_same_key_replacement_survives_stale_clear(
    mocker, replacement_has_revision: bool
) -> None:
    backend, table, _resource = _connected(mocker)
    observed = _revision_item("key")
    replacement = {"pk": "key", "value": b"replacement"}
    if replacement_has_revision:
        replacement[_REVISION] = _NEW_REVISION
    current = observed.copy()
    table.scan.return_value = {"Items": [observed]}

    def replace_then_delete(**kwargs: Any) -> None:
        nonlocal current
        current = replacement.copy()
        assert kwargs["ExpressionAttributeValues"] == {":revision": _OLD_REVISION}
        if current.get(_REVISION) != _OLD_REVISION:
            raise _condition_failed()
        current = {}

    table.delete_item.side_effect = replace_then_delete

    with pytest.raises(StorageError, match="partially complete") as exc_info:
        backend.clear_storage()

    assert current == replacement
    assert exc_info.value.operation == "clear_storage"
    assert exc_info.value.key is None
    assert exc_info.value.__cause__ is None
    assert table.delete_item.call_count == 1


def test_default_clear_preserves_revisionless_row_and_fails_closed(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    legacy = {"pk": "legacy", "value": b"old", "expire_at": 123}
    table.scan.return_value = {"Items": [legacy]}

    with pytest.raises(StorageError, match="unfenced legacy item") as exc_info:
        backend.clear_storage()

    assert exc_info.value.operation == "clear_storage"
    assert exc_info.value.key is None
    table.update_item.assert_not_called()
    table.delete_item.assert_not_called()


@pytest.mark.parametrize(
    "revision",
    [
        "0" * 31,
        "0" * 33,
        "A" * 32,
        "g" * 32,
        0,
    ],
)
def test_clear_rejects_malformed_revision_before_any_delete(
    mocker, revision: Any
) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = {
        "Items": [
            _revision_item("valid"),
            {"pk": "malformed", "value": b"value", _REVISION: revision},
        ]
    }

    with pytest.raises(StorageError, match="malformed revision metadata") as exc_info:
        backend.clear_storage()

    assert exc_info.value.operation == "clear_storage"
    assert exc_info.value.key is None
    table.delete_item.assert_not_called()


def test_identical_attribute_aba_is_not_claimed_or_deleted_by_default(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    observed = {"pk": "key", "value": b"identical", "expire_at": 123}
    current = observed.copy()

    def scan_then_identical_aba(**_kwargs: Any) -> dict[str, Any]:
        scanned = current.copy()
        current.clear()  # an external writer deletes the scanned legacy row
        current.update(observed)  # then recreates byte-for-byte identical attributes
        return {"Items": [scanned]}

    table.scan.side_effect = scan_then_identical_aba

    with pytest.raises(StorageError, match="item was preserved"):
        backend.clear_storage()

    assert current == observed
    table.update_item.assert_not_called()
    table.delete_item.assert_not_called()


def test_quiesced_override_conditionally_deletes_legacy_row_without_claim(
    mocker,
) -> None:
    backend, table, _resource = _connected(mocker, allow_unfenced_legacy_clear=True)
    legacy = {"pk": "legacy", "value": b"old", "expire_at": 123}
    table.scan.return_value = {"Items": [legacy]}

    backend.clear_storage()

    table.update_item.assert_not_called()
    assert table.delete_item.call_args.kwargs == {
        "Key": {"pk": "legacy"},
        "ConditionExpression": (
            "attribute_exists(pk) AND attribute_not_exists(#revision) "
            "AND #item1 = :item1 AND #item2 = :item2"
        ),
        "ExpressionAttributeNames": {
            "#revision": _REVISION,
            "#item1": "value",
            "#item2": "expire_at",
        },
        "ExpressionAttributeValues": {":item1": b"old", ":item2": 123},
        "ReturnValues": "ALL_OLD",
    }


def test_quiesced_override_handles_exact_400_kib_legacy_row_without_update(
    mocker,
) -> None:
    backend, table, _resource = _connected(mocker, allow_unfenced_legacy_clear=True)
    # This pre-existing item is exactly 400 KiB under DynamoDB's names+values
    # accounting and cannot accept even a one-byte revision attribute.
    value = b"x" * (400 * 1024 - len("pk") - len("k") - len("value"))
    legacy = {"pk": "k", "value": value}
    table.scan.return_value = {"Items": [legacy]}

    backend.clear_storage()

    table.update_item.assert_not_called()
    assert table.delete_item.call_count == 1
    assert table.delete_item.call_args.kwargs["ExpressionAttributeValues"] == {
        ":item1": value
    }


def test_quiesced_override_condition_loss_preserves_replacement(mocker) -> None:
    backend, table, _resource = _connected(mocker, allow_unfenced_legacy_clear=True)
    table.scan.return_value = {"Items": [{"pk": "key", "value": b"old"}]}
    table.delete_item.side_effect = _condition_failed()

    with pytest.raises(StorageError, match="partially complete") as exc_info:
        backend.clear_storage()

    assert exc_info.value.__cause__ is None
    assert table.delete_item.call_count == 1
    table.update_item.assert_not_called()


@pytest.mark.parametrize(
    "returned_attributes",
    [
        {"pk": "legacy", "value": b"changed", "expire_at": 123},
        {"pk": "legacy", "value": b"old"},
        {"pk": "legacy", "value": b"old", "expire_at": 123, "extra": True},
    ],
)
def test_quiesced_override_rejects_all_old_response_mismatch(
    mocker, returned_attributes: dict[str, Any]
) -> None:
    backend, table, _resource = _connected(mocker, allow_unfenced_legacy_clear=True)
    legacy = {"pk": "legacy", "value": b"old", "expire_at": 123}
    table.scan.return_value = {"Items": [legacy]}
    table.delete_item.side_effect = None
    table.delete_item.return_value = {"Attributes": returned_attributes}

    with pytest.raises(StorageError, match="malformed conditional DeleteItem") as exc:
        backend.clear_storage()

    assert exc.value.operation == "clear_storage"
    assert exc.value.key is None
    assert exc.value.__cause__ is None
    assert table.delete_item.call_count == 1


def test_override_is_captured_by_connected_generation(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = {"Items": [{"pk": "legacy", "value": b"old"}]}
    backend.config.allow_unfenced_legacy_clear = True

    with pytest.raises(StorageError, match="item was preserved"):
        backend.clear_storage()

    table.delete_item.assert_not_called()


def test_clear_wraps_conditional_delete_transport_failure(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    marker = "tenant-secret https://user:password@example.test"
    table.scan.return_value = {"Items": [_revision_item("key")]}
    table.delete_item.side_effect = RuntimeError(marker)

    with pytest.raises(StorageError) as exc_info:
        backend.clear_storage()

    assert str(exc_info.value) == (
        "Failed to clear DynamoDB table; the clear may be partially complete"
    )
    assert marker not in repr(vars(exc_info.value))
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        {},
        {"Attributes": None},
        {"Attributes": {}},
        {"Attributes": {"pk": "different", _REVISION: _OLD_REVISION}},
        {"Attributes": {"pk": "key"}},
        {"Attributes": {"pk": "key", _REVISION: _NEW_REVISION}},
    ],
)
def test_clear_rejects_malformed_conditional_delete_response(
    mocker, response: Any
) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = {"Items": [_revision_item("key")]}
    table.delete_item.side_effect = None
    table.delete_item.return_value = response

    with pytest.raises(StorageError, match="malformed conditional DeleteItem") as exc:
        backend.clear_storage()

    assert exc.value.operation == "clear_storage"
    assert exc.value.key is None
    assert exc.value.__cause__ is None


def test_real_resource_conditional_delete_api_shape_with_stubber() -> None:
    """Pin Resource serialization and ALL_OLD deserialization for clear CAS."""
    import subprocess
    import sys

    script = "\n".join(
        (
            "import boto3",
            "from botocore.stub import Stubber",
            "from scrapy_extension.backends.dynamodb import DynamoDBBackend",
            "resource = boto3.session.Session().resource(",
            "  'dynamodb', region_name='us-east-1',",
            "  endpoint_url='http://localhost:4566',",
            "  aws_access_key_id='x', aws_secret_access_key='y',",
            ")",
            "client = resource.meta.client",
            "table = resource.Table('scrapy-extension')",
            "revision = '0' * 32",
            "item = {'pk': 'key', 'value': b'payload', '_scrapy_revision': revision}",
            "expected = {",
            "  'TableName': 'scrapy-extension',",
            "  'Key': {'pk': 'key'},",
            "  'ConditionExpression': '#revision = :revision',",
            "  'ExpressionAttributeNames': {'#revision': '_scrapy_revision'},",
            "  'ExpressionAttributeValues': {':revision': revision},",
            "  'ReturnValues': 'ALL_OLD',",
            "}",
            "wire = {'Attributes': {",
            "  'pk': {'S': 'key'}, 'value': {'B': b'payload'},",
            "  '_scrapy_revision': {'S': revision},",
            "}}",
            "legacy = {'pk': 'legacy', 'value': b'payload'}",
            "legacy_expected = {",
            "  'TableName': 'scrapy-extension',",
            "  'Key': {'pk': 'legacy'},",
            "  'ConditionExpression': 'attribute_exists(pk) AND '",
            "    'attribute_not_exists(#revision) AND #item1 = :item1',",
            "  'ExpressionAttributeNames': {",
            "    '#revision': '_scrapy_revision', '#item1': 'value',",
            "  },",
            "  'ExpressionAttributeValues': {':item1': b'payload'},",
            "  'ReturnValues': 'ALL_OLD',",
            "}",
            "legacy_wire = {'Attributes': {",
            "  'pk': {'S': 'legacy'}, 'value': {'B': b'payload'},",
            "}}",
            "with Stubber(client) as stubber:",
            "  stubber.add_response('delete_item', wire, expected)",
            "  stubber.add_response('delete_item', legacy_wire, legacy_expected)",
            "  DynamoDBBackend._delete_clear_item(",
            "    table, item, allow_unfenced_legacy_clear=False",
            "  )",
            "  DynamoDBBackend._delete_clear_item(",
            "    table, legacy, allow_unfenced_legacy_clear=True",
            "  )",
            "  stubber.assert_no_pending_responses()",
        )
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr


def test_partial_delete_failure_stops_before_later_observed_items(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = {
        "Items": [_revision_item("first"), _revision_item("second")]
    }
    table.delete_item.side_effect = _condition_failed()

    with pytest.raises(StorageError, match="partially complete"):
        backend.clear_storage()

    assert table.delete_item.call_count == 1


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        {},
        {"Items": {}},
        {"Items": [{"value": b"missing-key"}]},
        {"Items": [], "LastEvaluatedKey": "not-a-key-map"},
    ],
)
def test_clear_rejects_malformed_scan_responses(mocker, response: Any) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = response

    with pytest.raises(StorageError, match="malformed"):
        backend.clear_storage()

    table.delete_item.assert_not_called()
    table.update_item.assert_not_called()


def test_clear_rejects_non_adjacent_scan_cursor_cycle(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    cursor_a = {"pk": "cursor-a"}
    cursor_b = {"pk": "cursor-b"}
    table.scan.side_effect = [
        {"Items": [], "LastEvaluatedKey": cursor_a},
        {"Items": [], "LastEvaluatedKey": cursor_b},
        {"Items": [], "LastEvaluatedKey": cursor_a},
    ]

    with pytest.raises(StorageError, match="partially complete"):
        backend.clear_storage()

    assert table.scan.call_count == 3


def test_prefix_clear_validates_scope_and_paginates(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    cursor = {"pk": "tenant-b:cursor"}
    table.scan.side_effect = [
        {
            "Items": [_revision_item("tenant-a:first")],
            "LastEvaluatedKey": cursor,
        },
        {"Items": [_revision_item("tenant-a:second", _NEW_REVISION)]},
    ]

    backend.clear_storage(prefix="tenant-a:")

    assert table.scan.call_count == 2
    assert table.scan.call_args_list[0].kwargs == {
        "ConsistentRead": True,
        "FilterExpression": "begins_with(pk, :p)",
        "ExpressionAttributeValues": {":p": "tenant-a:"},
    }
    assert table.scan.call_args_list[1].kwargs["ExclusiveStartKey"] == cursor
    assert table.delete_item.call_count == 2


def test_prefix_clear_rejects_out_of_scope_scan_item(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = {"Items": [_revision_item("tenant-b:victim")]}

    with pytest.raises(StorageError, match="out-of-scope"):
        backend.clear_storage(prefix="tenant-a:")

    table.delete_item.assert_not_called()


def test_disconnect_drains_conditional_delete_before_closing(mocker) -> None:
    backend, table, resource = _connected(mocker)
    table.scan.return_value = {"Items": [_revision_item("key")]}
    delete_entered = threading.Event()
    delete_release = threading.Event()
    timeline: list[str] = []

    def blocked_delete(**kwargs: Any) -> dict[str, Any]:
        timeline.append("delete-enter")
        delete_entered.set()
        assert delete_release.wait(timeout=5)
        timeline.append("delete-exit")
        return {
            "Attributes": {
                "pk": kwargs["Key"]["pk"],
                _REVISION: kwargs["ExpressionAttributeValues"][":revision"],
            }
        }

    table.delete_item.side_effect = blocked_delete
    resource.meta.client.close.side_effect = lambda: timeline.append("close")
    errors: list[BaseException] = []

    def run(target: Any) -> None:
        try:
            target()
        except BaseException as exc:
            errors.append(exc)

    clear_thread = threading.Thread(target=lambda: run(backend.clear_storage))
    clear_thread.start()
    assert delete_entered.wait(timeout=5)
    disconnect_thread = threading.Thread(target=lambda: run(backend.disconnect))
    disconnect_thread.start()
    disconnect_thread.join(timeout=1)

    assert disconnect_thread.is_alive()
    resource.meta.client.close.assert_not_called()

    delete_release.set()
    _join(clear_thread)
    _join(disconnect_thread)

    assert errors == []
    assert timeline == ["delete-enter", "delete-exit", "close"]
    assert backend.is_connected() is False


def test_clear_propagates_base_exception_and_releases_lock(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = {"Items": [_revision_item("key")]}
    table.delete_item.side_effect = KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        backend.clear_storage()

    table.delete_item.side_effect = None
    backend.store("after-interrupt", b"value")
    table.put_item.assert_called_once()
