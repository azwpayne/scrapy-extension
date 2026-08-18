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


def _connected(mocker) -> tuple[DynamoDBBackend, Any, Any]:
    backend = DynamoDBBackend(DynamoDBSettings())
    session = mocker.MagicMock()
    resource = mocker.MagicMock()
    table = mocker.MagicMock()
    table.load.return_value = None
    table.table_status = "ACTIVE"
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


def test_legacy_row_is_claimed_before_revision_delete(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    legacy = {"pk": "legacy", "value": b"old", "expire_at": 123}
    table.scan.return_value = {"Items": [legacy]}
    table.update_item.return_value = {"Attributes": legacy.copy()}
    mocker.patch.object(
        dynamodb_module.uuid,
        "uuid4",
        return_value=mocker.Mock(hex=_NEW_REVISION),
    )

    backend.clear_storage()

    table.update_item.assert_called_once_with(
        Key={"pk": "legacy"},
        UpdateExpression="SET #revision = :revision",
        ConditionExpression=(
            "attribute_exists(pk) AND attribute_not_exists(#revision) "
            "AND #item1 = :item1 AND #item2 = :item2"
        ),
        ExpressionAttributeNames={
            "#revision": _REVISION,
            "#item1": "value",
            "#item2": "expire_at",
        },
        ExpressionAttributeValues={
            ":revision": _NEW_REVISION,
            ":item1": b"old",
            ":item2": 123,
        },
        ReturnValues="ALL_OLD",
    )
    _assert_revision_delete(table.delete_item.call_args, "legacy", _NEW_REVISION)


@pytest.mark.parametrize("replacement_has_revision", [True, False])
def test_replacement_wins_legacy_claim_race_without_delete(
    mocker, replacement_has_revision: bool
) -> None:
    backend, table, _resource = _connected(mocker)
    legacy = {"pk": "key", "value": b"old"}
    replacement = {"pk": "key", "value": b"replacement"}
    if replacement_has_revision:
        replacement[_REVISION] = _NEW_REVISION
    current = replacement.copy()
    table.scan.return_value = {"Items": [legacy]}
    table.update_item.side_effect = _condition_failed()

    with pytest.raises(StorageError, match="partially complete"):
        backend.clear_storage()

    assert current == replacement
    table.delete_item.assert_not_called()
    assert table.update_item.call_count == 1


def test_legacy_claim_all_old_check_catches_added_external_attribute(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    observed = {"pk": "key", "value": b"old"}
    replacement = {"pk": "key", "value": b"old", "external": "added"}
    table.scan.return_value = {"Items": [observed]}
    table.update_item.return_value = {"Attributes": replacement.copy()}

    with pytest.raises(StorageError, match="partially complete"):
        backend.clear_storage()

    table.delete_item.assert_not_called()


def test_direct_replacement_after_legacy_claim_survives_delete(mocker) -> None:
    backend, table, _resource = _connected(mocker)
    legacy = {"pk": "key", "value": b"old"}
    replacement = {"pk": "key", "value": b"external"}
    current = legacy.copy()
    table.scan.return_value = {"Items": [legacy]}

    def claim(**kwargs: Any) -> dict[str, Any]:
        current[_REVISION] = kwargs["ExpressionAttributeValues"][":revision"]
        return {"Attributes": legacy.copy()}

    def replace_then_delete(**_kwargs: Any) -> None:
        nonlocal current
        current = replacement.copy()
        raise _condition_failed()

    table.update_item.side_effect = claim
    table.delete_item.side_effect = replace_then_delete

    with pytest.raises(StorageError, match="partially complete"):
        backend.clear_storage()

    assert current == replacement
    assert table.update_item.call_count == 1
    assert table.delete_item.call_count == 1


@pytest.mark.parametrize("response", [None, [], {}, {"Attributes": []}])
def test_malformed_legacy_claim_response_is_partial_failure(
    mocker, response: Any
) -> None:
    backend, table, _resource = _connected(mocker)
    table.scan.return_value = {"Items": [{"pk": "legacy", "value": b"old"}]}
    table.update_item.return_value = response

    with pytest.raises(StorageError, match="partially complete") as exc_info:
        backend.clear_storage()

    assert exc_info.value.operation == "clear_storage"
    table.delete_item.assert_not_called()


@pytest.mark.parametrize("operation", ["claim", "delete"])
def test_clear_wraps_conditional_rpc_transport_failure(mocker, operation: str) -> None:
    backend, table, _resource = _connected(mocker)
    marker = "tenant-secret https://user:password@example.test"
    failure = RuntimeError(marker)
    if operation == "claim":
        legacy = {"pk": "key", "value": b"old"}
        table.scan.return_value = {"Items": [legacy]}
        table.update_item.side_effect = failure
    else:
        table.scan.return_value = {"Items": [_revision_item("key")]}
        table.delete_item.side_effect = failure

    with pytest.raises(StorageError) as exc_info:
        backend.clear_storage()

    assert str(exc_info.value) == (
        "Failed to clear DynamoDB table; the clear may be partially complete"
    )
    assert marker not in repr(vars(exc_info.value))
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


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

    def blocked_delete(**_kwargs: Any) -> None:
        timeline.append("delete-enter")
        delete_entered.set()
        assert delete_release.wait(timeout=5)
        timeline.append("delete-exit")

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
