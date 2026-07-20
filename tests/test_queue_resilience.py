"""Resilience tests for ``BackendQueue`` defensive branches (initiative #25).

Pins the documented "best-effort, never crash close / never crash startup"
contracts that had no direct tests (queue.py was 91.51%, below the 95%
floor). Every branch covered here is a real load-bearing guarantee:

- ``_persist_snapshot``: snapshot/storage-resolver/store failures never
  crash ``close()``.
- ``_restore_snapshot``: storage-resolver/retrieve failures never crash
  startup.
- pop path: a failing ``monitor.on_pop_rate`` never breaks a successful pop.
- ack/nack with token: the per-message path correct under
  ``CONCURRENT_REQUESTS > 1``.
- ``_decode_body``: a non-str body that is also invalid base64 raises
  ``SerializationError`` rather than a raw ``TypeError`` / ``binascii.Error``.

These are contract pins, not coverage padding — each asserts a behavior the
docstrings promise and production relies on.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from scrapy.http import Request

from scrapy_extension.exceptions import SerializationError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy


def _delay() -> DelayQueueStrategy:
  return DelayQueueStrategy(MagicMock(name="ConnectionManager"), clock=lambda: 100.0)


def _storage(
  *,
  store_raises: bool = False,
  retrieve_raises: bool = False,
  retrieve_return: object = None,
) -> MagicMock:
  storage = MagicMock(name="StorageBackend")
  if store_raises:
    storage.store.side_effect = RuntimeError("store boom")
  if retrieve_raises:
    storage.retrieve.side_effect = RuntimeError("retrieve boom")
  else:
    storage.retrieve.return_value = retrieve_return
  return storage


def _cm(
  *,
  storage: MagicMock | None = None,
  storage_resolver_raises: BaseException | None = None,
  queue_backend: MagicMock | None = None,
) -> MagicMock:
  cm = MagicMock(name="ConnectionManager")
  if storage_resolver_raises is not None:
    cm.get_storage_backend.side_effect = storage_resolver_raises
  else:
    cm.get_storage_backend.return_value = (
      storage if storage is not None else MagicMock(name="StorageBackend")
    )
  cm.get_queue_backend.return_value = queue_backend or MagicMock(name="QueueBackend")
  return cm


# ---------------------------------------------------------------------------
# _persist_snapshot resilience (close path)
# ---------------------------------------------------------------------------


def test_persist_snapshot_skips_when_strategy_snapshot_raises() -> None:
  """Lines 651-653: ``strategy.snapshot()`` raising must not crash ``close``
  — logged and skipped (best-effort persist contract)."""
  strategy = _delay()
  strategy.snapshot = MagicMock(side_effect=RuntimeError("snapshot boom"))  # type: ignore[method-assign]
  storage = _storage()
  bq = BackendQueue(
    connection_manager=_cm(storage=storage),
    queue_name="q",
    queue_strategy=strategy,
    monitor=MagicMock(),
  )
  bq.close()  # must not raise
  storage.store.assert_not_called()  # snapshot failed -> never reached store


def test_persist_snapshot_skips_when_storage_resolver_raises() -> None:
  """Lines 670-675: ``get_storage_backend()`` raising a non-``NotImplementedError``
  must not crash ``close`` — logged and skipped (distinct from the
  storage-incapable ``NotImplementedError`` path which only logs at info)."""
  strategy = _delay()
  strategy.push("q", b"x", delay=10.0)  # non-empty heap -> snapshot returns bytes
  bq = BackendQueue(
    connection_manager=_cm(storage_resolver_raises=RuntimeError("resolver boom")),
    queue_name="q",
    queue_strategy=strategy,
    monitor=MagicMock(),
  )
  bq.close()  # must not raise


def test_persist_snapshot_skips_when_store_raises() -> None:
  """Lines 678-679 (+ log 680-682): ``storage.store()`` raising must not crash
  ``close`` — logged and skipped."""
  strategy = _delay()
  strategy.push("q", b"x", delay=10.0)
  bq = BackendQueue(
    connection_manager=_cm(storage=_storage(store_raises=True)),
    queue_name="q",
    queue_strategy=strategy,
    monitor=MagicMock(),
  )
  bq.close()  # must not raise


# ---------------------------------------------------------------------------
# _restore_snapshot resilience (init path)
# ---------------------------------------------------------------------------


def test_restore_snapshot_skips_when_storage_resolver_raises() -> None:
  """Lines 701-706: ``get_storage_backend()`` raising a non-``NotImplementedError``
  at init must not crash startup — logged, starts clean."""
  strategy = _delay()
  # Constructing the BackendQueue runs _restore_snapshot at __init__ — must not raise:
  BackendQueue(
    connection_manager=_cm(storage_resolver_raises=RuntimeError("init resolver boom")),
    queue_name="q",
    queue_strategy=strategy,
    monitor=MagicMock(),
  )


def test_restore_snapshot_skips_when_retrieve_raises() -> None:
  """Lines 709-714: ``storage.retrieve()`` raising must not crash startup —
  logged, starts clean."""
  strategy = _delay()
  BackendQueue(
    connection_manager=_cm(storage=_storage(retrieve_raises=True)),
    queue_name="q",
    queue_strategy=strategy,
    monitor=MagicMock(),
  )


def test_restore_snapshot_delete_failure_does_not_crash_startup() -> None:
  """A restored snapshot is consumed best-effort; delete failure is logged."""
  source = _delay()
  source.push("q", b"recover", delay=10.0)
  state = source.snapshot()
  storage = _storage(retrieve_return=state)
  storage.delete.side_effect = RuntimeError("delete boom")
  strategy = _delay()

  BackendQueue(
    connection_manager=_cm(storage=storage),
    queue_name="q",
    queue_strategy=strategy,
    monitor=MagicMock(),
  )

  assert len(strategy._holding) == 1
  storage.delete.assert_called_once_with("queue:snapshot:q")


# ---------------------------------------------------------------------------
# ack / nack with token (CONCURRENT_REQUESTS > 1 path)
# ---------------------------------------------------------------------------


def test_ack_with_token_acks_specific_message() -> None:
  """Line 528: ``ack(token=...)`` calls ``backend.ack`` with the token — the
  per-message path correct under ``CONCURRENT_REQUESTS > 1`` (vs the legacy
  single-slot ``token=None`` path)."""
  qb = MagicMock(name="QueueBackend")
  bq = BackendQueue(connection_manager=_cm(queue_backend=qb), queue_name="q", monitor=MagicMock())
  bq.ack(token="msg-handle-42")
  qb.ack.assert_called_once_with("q", token="msg-handle-42")


def test_nack_with_token_nacks_specific_message() -> None:
  """Line 544: ``nack(token=...)`` calls ``backend.nack`` with the token."""
  qb = MagicMock(name="QueueBackend")
  bq = BackendQueue(connection_manager=_cm(queue_backend=qb), queue_name="q", monitor=MagicMock())
  bq.nack(token="msg-handle-99")
  qb.nack.assert_called_once_with("q", token="msg-handle-99")


# ---------------------------------------------------------------------------
# pop-path monitor resilience
# ---------------------------------------------------------------------------


def test_pop_survives_monitor_pop_rate_failure() -> None:
  """Lines 289-290: ``monitor.on_pop_rate`` raising must not break a pop —
  logged at debug, pop returns normally. The monitor hooks fire BEFORE the
  ``if data is None`` short-circuit, so an empty-queue pop still exercises
  the failing hook without needing a deserializable item."""
  qb = MagicMock(name="QueueBackend")
  qb.pop.return_value = None  # empty queue -> pop returns None before deserialize
  monitor = MagicMock()
  monitor.on_pop_rate.side_effect = RuntimeError("pop-rate boom")
  bq = BackendQueue(
    connection_manager=_cm(queue_backend=qb),
    queue_name="q",
    monitor=monitor,
    depth_sample_every=1,  # _emit_pop_rate fires on every pop
  )
  # Must return None (empty), NOT raise the RuntimeError from on_pop_rate:
  assert bq.pop(timeout=0) is None


def test_push_survives_monitor_failure_after_enqueue() -> None:
  """Telemetry failure cannot turn a committed enqueue into caller failure."""
  qb = MagicMock(name="QueueBackend")
  monitor = MagicMock()
  monitor.on_push.side_effect = RuntimeError("push monitor boom")
  bq = BackendQueue(
    connection_manager=_cm(queue_backend=qb),
    queue_name="q",
    monitor=monitor,
  )

  bq.push(Request("https://example.com"))

  qb.push.assert_called_once()


def test_pop_survives_monitor_failure_after_atomic_pop() -> None:
  """A monitor cannot discard an item already removed by an atomic backend."""
  qb = MagicMock(name="QueueBackend")
  monitor = MagicMock()
  monitor.on_pop.side_effect = RuntimeError("pop monitor boom")
  bq = BackendQueue(
    connection_manager=_cm(queue_backend=qb),
    queue_name="q",
    monitor=monitor,
  )
  request = Request("https://example.com")
  qb.pop.return_value = bq._serializer.serialize(bq._request_to_dict(request))

  restored = bq.pop()

  assert restored is not None
  assert restored.url == request.url


def test_error_monitor_failure_does_not_mask_serialization_error() -> None:
  """Error telemetry is secondary to the deterministic data-plane error."""
  monitor = MagicMock()
  monitor.on_error.side_effect = RuntimeError("error monitor boom")
  bq = BackendQueue(
    connection_manager=_cm(),
    queue_name="q",
    monitor=monitor,
  )

  with pytest.raises(SerializationError, match="Failed to serialize request"):
    bq.push(Request("https://example.com", meta={"bad": object()}))


# ---------------------------------------------------------------------------
# _decode_body non-str edge (line 401)
# ---------------------------------------------------------------------------


def test_decode_body_non_str_invalid_base64_raises_serialization_error() -> None:
  """Line 401: a body that is neither a ``str`` nor valid base64 (e.g. raw
  ``bytes`` failing ``b64decode(validate=True)``) falls through the
  legacy-migration branch (``legacy_bytes = None``) and raises a clean
  ``SerializationError`` rather than surfacing the raw ``binascii.Error``."""
  with pytest.raises(SerializationError):
    BackendQueue._decode_body({"body": b"!!not-valid-base64!!"})
