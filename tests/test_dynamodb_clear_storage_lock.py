"""DynamoDB ``clear_storage`` is a complete operation boundary."""

from __future__ import annotations

import threading
from typing import Any

from scrapy_extension.backends import dynamodb as dynamodb_module
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.settings import DynamoDBSettings

_TABLE_NAME = "scrapy-extension"


def _connected(mocker) -> tuple[DynamoDBBackend, Any, Any]:
    """Build a connected backend backed by mocked boto3 resource/table/client."""
    backend = DynamoDBBackend(DynamoDBSettings())
    session = mocker.MagicMock()
    resource = mocker.MagicMock()
    table = mocker.MagicMock()
    client = resource.meta.client
    table.load.return_value = None
    table.table_status = "ACTIVE"
    resource.Table.return_value = table
    table.meta.client = client
    session.resource.return_value = resource
    mocker.patch.object(
        dynamodb_module.boto3.session,
        "Session",
        return_value=session,
    )
    backend.connect()
    return backend, table, client


def _delete_request(key: str) -> dict[str, Any]:
    # The Resource client owns AttributeValue transforms; keep the native request
    # shape that _validated_unprocessed_deletes matches against.
    return {"DeleteRequest": {"Key": {"pk": key}}}


def _join(thread: threading.Thread) -> None:
    thread.join(timeout=5)
    assert not thread.is_alive()


def _park_first_clear_in_backoff(
    mocker, table: Any, client: Any
) -> tuple[threading.Event, threading.Event]:
    request = _delete_request("clear-key")
    table.scan.return_value = {"Items": [{"pk": "clear-key"}]}
    responses = iter(
        [
            {"UnprocessedItems": {_TABLE_NAME: [request]}},
            {"UnprocessedItems": {}},
            {"UnprocessedItems": {}},
        ]
    )
    client.batch_write_item.side_effect = lambda **_kwargs: next(responses)
    sleep_entered = threading.Event()
    sleep_release = threading.Event()

    def blocked_sleep(_delay: float) -> None:
        sleep_entered.set()
        assert sleep_release.wait(timeout=5)

    mocker.patch.object(
        dynamodb_module,
        "compute_full_jitter_backoff",
        return_value=0.3,
        create=True,
    )
    mocker.patch.object(dynamodb_module.time, "sleep", side_effect=blocked_sleep)
    return sleep_entered, sleep_release


def test_concurrent_retrieve_waits_for_throttled_clear_boundary(mocker) -> None:
    backend, table, client = _connected(mocker)
    sleep_entered, sleep_release = _park_first_clear_in_backoff(mocker, table, client)
    table.get_item.return_value = {}
    errors: list[BaseException] = []
    retrieve_results: list[object] = []

    def run(target) -> None:
        try:
            target()
        except BaseException as exc:
            errors.append(exc)

    clear_thread = threading.Thread(
        target=lambda: run(backend.clear_storage), name="clear"
    )
    clear_thread.start()
    assert sleep_entered.wait(timeout=5)

    retrieve_thread = threading.Thread(
        target=lambda: run(
            lambda: retrieve_results.append(backend.retrieve("other-key"))
        ),
        name="retrieve",
    )
    retrieve_thread.start()
    retrieve_thread.join(timeout=1)

    assert retrieve_thread.is_alive()
    table.get_item.assert_not_called()

    sleep_release.set()
    _join(clear_thread)
    _join(retrieve_thread)

    assert retrieve_results == [None]
    assert errors == []
    table.get_item.assert_called_once_with(Key={"pk": "other-key"}, ConsistentRead=True)


def test_concurrent_clear_waits_for_throttled_clear_boundary(mocker) -> None:
    backend, table, client = _connected(mocker)
    sleep_entered, sleep_release = _park_first_clear_in_backoff(mocker, table, client)
    errors: list[BaseException] = []

    def run_clear() -> None:
        try:
            backend.clear_storage()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=run_clear, name="clear-1")
    first.start()
    assert sleep_entered.wait(timeout=5)

    second = threading.Thread(target=run_clear, name="clear-2")
    second.start()
    second.join(timeout=1)

    assert second.is_alive()
    assert table.scan.call_count == 1

    sleep_release.set()
    _join(first)
    _join(second)

    assert errors == []
    assert table.scan.call_count == 2
    assert client.batch_write_item.call_count == 3
