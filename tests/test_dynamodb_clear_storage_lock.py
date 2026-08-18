"""DynamoDB ``clear_storage`` remains a complete local operation boundary."""

from __future__ import annotations

import threading
from typing import Any

from scrapy_extension.backends import dynamodb as dynamodb_module
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.settings import DynamoDBSettings


def _connected(mocker) -> tuple[DynamoDBBackend, Any]:
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
    return backend, table


def _join(thread: threading.Thread) -> None:
    thread.join(timeout=5)
    assert not thread.is_alive()


def _park_clear(table: Any) -> tuple[threading.Event, threading.Event]:
    table.scan.return_value = {
        "Items": [{"pk": "clear-key", "_scrapy_revision": "0" * 32}]
    }
    delete_entered = threading.Event()
    delete_release = threading.Event()

    def blocked_delete(**_kwargs: Any) -> None:
        delete_entered.set()
        assert delete_release.wait(timeout=5)

    table.delete_item.side_effect = blocked_delete
    return delete_entered, delete_release


def test_concurrent_retrieve_waits_for_conditional_clear_boundary(mocker) -> None:
    backend, table = _connected(mocker)
    delete_entered, delete_release = _park_clear(table)
    table.get_item.return_value = {}
    errors: list[BaseException] = []
    retrieve_results: list[object] = []

    def run(target: Any) -> None:
        try:
            target()
        except BaseException as exc:
            errors.append(exc)

    clear_thread = threading.Thread(target=lambda: run(backend.clear_storage))
    clear_thread.start()
    assert delete_entered.wait(timeout=5)
    retrieve_thread = threading.Thread(
        target=lambda: run(
            lambda: retrieve_results.append(backend.retrieve("other-key"))
        )
    )
    retrieve_thread.start()
    retrieve_thread.join(timeout=1)

    assert retrieve_thread.is_alive()
    table.get_item.assert_not_called()

    delete_release.set()
    _join(clear_thread)
    _join(retrieve_thread)

    assert errors == []
    assert retrieve_results == [None]


def test_concurrent_clear_waits_for_conditional_clear_boundary(mocker) -> None:
    backend, table = _connected(mocker)
    delete_entered, delete_release = _park_clear(table)
    errors: list[BaseException] = []

    def run_clear() -> None:
        try:
            backend.clear_storage()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=run_clear)
    first.start()
    assert delete_entered.wait(timeout=5)
    second = threading.Thread(target=run_clear)
    second.start()
    second.join(timeout=1)

    assert second.is_alive()
    assert table.scan.call_count == 1

    delete_release.set()
    _join(first)
    _join(second)

    assert errors == []
    assert table.scan.call_count == 2
    assert table.delete_item.call_count == 2
