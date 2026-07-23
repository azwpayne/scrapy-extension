"""U2: DynamoDB ``clear_storage`` must not hold ``_operation_lock`` across backoff.

Defect (SPEC U2, 2026-07-23 post-hardening frontier): ``clear_storage`` acquired
``_operation_lock`` and held it across the entire paginated scan +
``_delete_batch_with_backoff`` loop. That same lock serializes
``store``/``retrieve``/``delete``/``exists``/``ttl`` and ``disconnect`` -- so a
throttled clear (``UnprocessedItems`` + full-jitter backoff sleep) froze the
whole storage pipeline and stalled shutdown.

Contract after fix (mirrors ``connectors.py`` release-lock-before-slow-work):
the lock guards only the state reads (generation snapshot + each scan page);
each ``_delete_batch_with_backoff`` call (the slow network I/O + ``time.sleep``)
runs OUTSIDE the lock. A concurrent ``retrieve()`` must therefore complete
promptly while the clear is parked in its backoff sleep.
"""

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


def test_concurrent_retrieve_is_not_blocked_by_throttled_clear_backoff(
  mocker,
) -> None:
  """retrieve() must complete while clear_storage is parked in backoff sleep.

  RED on current code: clear holds ``_operation_lock`` across the backoff
  ``time.sleep``, so the concurrent retrieve blocks on the lock for the whole
  sleep window. GREEN after fix: the lock is released around the slow
  ``_delete_batch_with_backoff`` call, so retrieve slips through promptly.
  """
  backend, table, client = _connected(mocker)
  request = _delete_request("clear-key")
  # One scanned item; its first batch_write comes back Unprocessed, which drives
  # _delete_batch_with_backoff into its full-jitter backoff sleep.
  table.scan.return_value = {"Items": [{"pk": "clear-key"}]}
  client.batch_write_item.return_value = {
    "UnprocessedItems": {_TABLE_NAME: [request]}
  }
  table.get_item.return_value = {}  # retrieve("other-key") -> missing -> None
  sleep_entered = threading.Event()
  sleep_release = threading.Event()

  def blocked_sleep(_delay: float) -> None:
    sleep_entered.set()
    # Park the clear in its backoff sleep until the test releases it.
    assert sleep_release.wait(timeout=5)

  mocker.patch.object(
    dynamodb_module,
    "compute_full_jitter_backoff",
    return_value=0.3,
    create=True,
  )
  mocker.patch.object(dynamodb_module.time, "sleep", side_effect=blocked_sleep)

  errors: list[BaseException] = []
  retrieve_results: list[object] = []

  def run_clear() -> None:
    try:
      backend.clear_storage()
    except BaseException as exc:
      errors.append(exc)

  clear_thread = threading.Thread(target=run_clear, name="clear")
  clear_thread.start()
  # Wait until the clear is parked in its backoff sleep (holding the lock on the
  # old code; not holding it after the fix).
  assert sleep_entered.wait(timeout=5)

  def run_retrieve() -> None:
    try:
      retrieve_results.append(backend.retrieve("other-key"))
    except BaseException as exc:
      errors.append(exc)

  retrieve_thread = threading.Thread(target=run_retrieve, name="retrieve")
  retrieve_thread.start()
  # Short deadline: a mocked get_item is microseconds; if retrieve is still
  # alive after this window it is blocked on _operation_lock.
  retrieve_thread.join(timeout=1.0)
  retrieve_blocked = retrieve_thread.is_alive()

  # Release the clear so both threads can finish cleanly before assertions.
  client.batch_write_item.return_value = {"UnprocessedItems": {}}
  sleep_release.set()
  _join(clear_thread)
  _join(retrieve_thread)

  assert not retrieve_blocked, (
    "retrieve() was blocked while clear_storage was parked in its backoff "
    "sleep -- _operation_lock is still held across the slow batch_write"
  )
  assert retrieve_results == [None]
  assert errors == []
  table.get_item.assert_called_once_with(
    Key={"pk": "other-key"}, ConsistentRead=True
  )
