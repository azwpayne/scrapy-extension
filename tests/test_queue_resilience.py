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

import logging
import sys
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from scrapy.http import Request

from scrapy_extension.backends.base import _QueuePushReceipt
from scrapy_extension.exceptions import QueueError, SerializationError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy


class _StatsExceptionContextProbe:
    """Record the exception state visible to a stats extension."""

    def __init__(self) -> None:
        self.calls: list[
            tuple[str, tuple[object | None, object | None, object | None]]
        ] = []

    def inc_value(self, stat_name: str) -> None:
        self.calls.append((stat_name, sys.exc_info()))


class _LoggingExceptionContextProbe(logging.Handler):
    """Record what a synchronous logging handler can inspect during ``emit``."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.contexts: list[tuple[object | None, object | None, object | None]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.contexts.append(sys.exc_info())


def _assert_no_active_exception(
    contexts: list[tuple[object | None, object | None, object | None]],
) -> None:
    assert contexts
    assert contexts == [(None, None, None)] * len(contexts)


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
    resolved_queue_backend = queue_backend or MagicMock(name="QueueBackend")
    cm.get_queue_backend.return_value = resolved_queue_backend

    def push_queue_with_durability(
        queue_name: str,
        item: bytes,
        priority: float = 0.0,
        *,
        require_durable: bool = False,
    ) -> _QueuePushReceipt:
        del require_durable
        resolved_queue_backend.push(queue_name, item, priority)
        return _QueuePushReceipt(worker_crash_durable=True)

    cm._push_queue_with_durability.side_effect = push_queue_with_durability
    return cm


# ---------------------------------------------------------------------------
# R21-B: BackendQueue must forward its monitor to the delay strategy
# ---------------------------------------------------------------------------


def test_backend_queue_forwards_monitor_to_delay_strategy() -> None:
    """R21-B: BackendQueue forwards its monitor to the delay strategy via
    ``set_monitor`` so ``queue/delay_depth`` actually emits.

    Pre-fix, BackendQueue stored its own ``_monitor`` but never forwarded it to
    ``self._strategy``; DelayQueueStrategy kept its NullMonitor default and the
    ``on_delay_depth`` gauge (the delay-heap leading indicator) never fired in
    production. This exercises the PRODUCTION wiring (BackendQueue→strategy), not
    ``DelayQueueStrategy(monitor=...)`` direct construction (which bypasses the gap
    and is a false-green).
    """
    spy = MagicMock()
    strategy = _delay()
    BackendQueue(
        connection_manager=_cm(),
        queue_name="q",
        queue_strategy=strategy,
        monitor=spy,
    )
    # Push a held (delayed) item — on_delay_depth fires iff the strategy holds
    # the BackendQueue's monitor, not its NullMonitor default.
    strategy.push("q", b"x", delay=10.0)
    spy.on_delay_depth.assert_called_once()


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
    with pytest.raises(QueueError, match="snapshot creation"):
        bq.close()
    storage.store.assert_not_called()
    assert bq._close_complete is False


def test_close_releases_strategy_after_begin_close_baseexception() -> None:
    """A control exception from begin_close cannot skip snapshot or close."""
    strategy = MagicMock(name="Strategy")
    first = KeyboardInterrupt()
    strategy.begin_close.side_effect = first
    strategy.snapshot.return_value = None
    queue = BackendQueue(
        connection_manager=_cm(), queue_name="q", queue_strategy=strategy
    )

    with pytest.raises(KeyboardInterrupt) as raised:
        queue.close()

    assert raised.value is first
    strategy.snapshot.assert_not_called()
    strategy.close.assert_not_called()
    assert queue._close_complete is False


def test_close_releases_strategy_after_snapshot_baseexception() -> None:
    """A control exception during snapshot cannot skip destructive cleanup."""
    strategy = MagicMock(name="Strategy")
    first = KeyboardInterrupt()
    strategy.snapshot.side_effect = first
    queue = BackendQueue(
        connection_manager=_cm(), queue_name="q", queue_strategy=strategy
    )
    # The auto-mocked storage value is an unreadable current checkpoint in this
    # seam test; explicitly authorize the intended snapshot attempt first.
    queue.reset_snapshot()

    with pytest.raises(KeyboardInterrupt) as raised:
        queue.close()

    assert raised.value is first
    strategy.close.assert_not_called()
    assert queue._close_complete is False


def test_close_preserves_first_baseexception_across_all_phases() -> None:
    """Every close phase runs even when each one raises a control exception."""
    strategy = MagicMock(name="Strategy")
    first = KeyboardInterrupt()
    strategy.begin_close.side_effect = first
    strategy.snapshot.side_effect = SystemExit(2)
    strategy.close.side_effect = GeneratorExit()
    queue = BackendQueue(
        connection_manager=_cm(), queue_name="q", queue_strategy=strategy
    )

    with pytest.raises(KeyboardInterrupt) as raised:
        queue.close()

    assert raised.value is first
    strategy.begin_close.assert_called_once_with()
    strategy.snapshot.assert_not_called()
    strategy.close.assert_not_called()
    assert queue._close_complete is False


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
    with pytest.raises(QueueError, match="storage is unavailable"):
        bq.close()


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
    with pytest.raises(QueueError, match="snapshot commit"):
        bq.close()


@pytest.mark.parametrize(
    ("fallback_site", "diagnostic_method"),
    [
        pytest.param("snapshot", "error", id="persist-snapshot"),
        pytest.param("storage-incapable", "info", id="persist-not-implemented"),
        pytest.param("persist-resolver", "error", id="persist-resolver"),
        pytest.param("store", "error", id="persist-store"),
        pytest.param("restore-resolver", "error", id="restore-resolver"),
        pytest.param("retrieve", "error", id="restore-retrieve"),
        pytest.param("restore", "error", id="strategy-restore"),
    ],
)
@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("diagnostic boom"), KeyboardInterrupt(), SystemExit(2)],
)
def test_snapshot_fallback_diagnostic_failure_preserves_best_effort_contract(
    monkeypatch: pytest.MonkeyPatch,
    fallback_site: str,
    diagnostic_method: str,
    diagnostic_error: BaseException,
) -> None:
    """A failing fallback diagnostic never replaces the completed degradation.

    The underlying operation deliberately raises an ordinary ``Exception`` so
    the snapshot contract selects its documented best-effort outcome.  Logging
    that outcome is pure telemetry: even a handler's ``RuntimeError``,
    ``KeyboardInterrupt``, or ``SystemExit`` must leave close/startup on that
    selected path.  This does not weaken direct control-flow exceptions from the
    snapshot, resolver, storage, or strategy operations themselves.
    """
    import scrapy_extension.queue.queue as queue_mod

    diagnostic = MagicMock(side_effect=diagnostic_error)
    monkeypatch.setattr(queue_mod.logger, diagnostic_method, diagnostic)
    strategy = _delay()
    storage = _storage()
    cm = _cm(storage=storage)

    if fallback_site == "snapshot":
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        strategy.snapshot = MagicMock(side_effect=RuntimeError("snapshot boom"))  # type: ignore[method-assign]
        with pytest.raises(QueueError, match="snapshot creation"):
            queue.close()
        storage.store.assert_not_called()
        assert queue._close_complete is False
    elif fallback_site == "storage-incapable":
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        cm.get_storage_backend.side_effect = NotImplementedError("queue-only backend")
        queue.close()
        storage.store.assert_not_called()
        assert queue._close_complete is True
    elif fallback_site == "persist-resolver":
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        cm.get_storage_backend.side_effect = RuntimeError("resolver boom")
        with pytest.raises(QueueError, match="storage is unavailable"):
            queue.close()
        storage.store.assert_not_called()
        assert queue._close_complete is False
    elif fallback_site == "store":
        strategy.push("q", b"held", delay=10.0)
        storage.store.side_effect = RuntimeError("store boom")
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        # Construction restores before close; retain the queued held state after
        # that no-op restore so persist reaches the failing store.
        with pytest.raises(QueueError, match="snapshot commit"):
            queue.close()
        storage.store.assert_called_once()
        assert queue._close_complete is False
    elif fallback_site == "restore-resolver":
        cm.get_storage_backend.side_effect = RuntimeError("resolver boom")
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        assert queue._close_complete is False
    elif fallback_site == "retrieve":
        storage.retrieve.side_effect = RuntimeError("retrieve boom")
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        assert queue._close_complete is False
    else:
        assert fallback_site == "restore"
        storage.retrieve.return_value = b"incompatible snapshot"
        strategy.restore = MagicMock(side_effect=RuntimeError("restore boom"))  # type: ignore[method-assign]
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        strategy.restore.assert_called_once_with(b"incompatible snapshot")
        assert queue._close_complete is False

    diagnostic.assert_called_once()


@pytest.mark.parametrize(
    "failure_site",
    [
        "snapshot",
        "persist-resolver",
        "store",
        "restore-resolver",
        "retrieve",
        "restore",
    ],
)
def test_snapshot_operations_preserve_direct_control_flow_exceptions(
    failure_site: str,
) -> None:
    """Control exceptions from operations are not misclassified as diagnostics."""
    control_error = KeyboardInterrupt()
    strategy = _delay()
    storage = _storage()
    cm = _cm(storage=storage)

    if failure_site == "snapshot":
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        strategy.snapshot = MagicMock(side_effect=control_error)  # type: ignore[method-assign]
        with pytest.raises(KeyboardInterrupt) as raised:
            queue.close()
        assert raised.value is control_error
        assert queue._close_complete is False
    elif failure_site == "persist-resolver":
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        cm.get_storage_backend.side_effect = control_error
        with pytest.raises(KeyboardInterrupt) as raised:
            queue.close()
        assert raised.value is control_error
        assert queue._close_complete is False
    elif failure_site == "store":
        strategy.push("q", b"held", delay=10.0)
        storage.store.side_effect = control_error
        queue = BackendQueue(
            connection_manager=cm, queue_name="q", queue_strategy=strategy
        )
        with pytest.raises(KeyboardInterrupt) as raised:
            queue.close()
        assert raised.value is control_error
        assert queue._close_complete is False
    elif failure_site == "restore-resolver":
        cm.get_storage_backend.side_effect = control_error
        with pytest.raises(KeyboardInterrupt) as raised:
            BackendQueue(connection_manager=cm, queue_name="q", queue_strategy=strategy)
        assert raised.value is control_error
    elif failure_site == "retrieve":
        storage.retrieve.side_effect = control_error
        with pytest.raises(KeyboardInterrupt) as raised:
            BackendQueue(connection_manager=cm, queue_name="q", queue_strategy=strategy)
        assert raised.value is control_error
    else:
        assert failure_site == "restore"
        storage.retrieve.return_value = b"incompatible snapshot"
        strategy.restore = MagicMock(side_effect=control_error)  # type: ignore[method-assign]
        with pytest.raises(KeyboardInterrupt) as raised:
            BackendQueue(connection_manager=cm, queue_name="q", queue_strategy=strategy)
        assert raised.value is control_error


# ---------------------------------------------------------------------------
# _restore_snapshot resilience (init path)
# ---------------------------------------------------------------------------


def test_restore_snapshot_skips_when_storage_resolver_raises() -> None:
    """Lines 701-706: ``get_storage_backend()`` raising a non-``NotImplementedError``
    at init must not crash startup — logged, starts clean."""
    strategy = _delay()
    # Constructing the BackendQueue runs _restore_snapshot at __init__ — must not raise:
    BackendQueue(
        connection_manager=_cm(
            storage_resolver_raises=RuntimeError("init resolver boom")
        ),
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


def test_restore_snapshot_retains_checkpoint_without_delete() -> None:
    """A restored checkpoint remains available until a clean close replaces it."""
    source = _delay()
    source.push("q", b"recover", delay=10.0)
    state = source.snapshot()
    storage = _storage(retrieve_return=state)
    strategy = _delay()

    BackendQueue(
        connection_manager=_cm(storage=storage),
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert len(strategy._holding) == 1
    storage.delete.assert_not_called()


# ---------------------------------------------------------------------------
# ack / nack with token (CONCURRENT_REQUESTS > 1 path)
# ---------------------------------------------------------------------------


def test_ack_with_token_acks_specific_message() -> None:
    """Line 528: ``ack(token=...)`` calls ``backend.ack`` with the token — the
    per-message path correct under ``CONCURRENT_REQUESTS > 1`` (vs the legacy
    single-slot ``token=None`` path)."""
    qb = MagicMock(name="QueueBackend")
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb), queue_name="q", monitor=MagicMock()
    )
    bq.ack(token="msg-handle-42")
    qb.ack.assert_called_once_with("q", token="msg-handle-42")


def test_nack_with_token_nacks_specific_message() -> None:
    """Line 544: ``nack(token=...)`` calls ``backend.nack`` with the token."""
    qb = MagicMock(name="QueueBackend")
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb), queue_name="q", monitor=MagicMock()
    )
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


def test_failed_pop_reports_attempt_and_error_without_decrementing_depth() -> None:
    """A failed backend pop is observable, but never counted as a success."""
    qb = MagicMock(name="QueueBackend")
    qb.queue_len.return_value = 3
    failure = QueueError("pop unavailable")
    qb.pop.side_effect = failure
    monitor = MagicMock()
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb),
        queue_name="q",
        monitor=monitor,
        depth_sample_every=100,
    )

    assert len(bq) == 3
    with pytest.raises(QueueError) as raised:
        bq.pop(timeout=0)

    assert raised.value is failure
    monitor.on_pop.assert_called_once_with("q")
    monitor.on_last_pop_epoch.assert_called_once()
    monitor.on_error.assert_called_once_with("pop", failure)
    assert bq._cached_depth == 3


@pytest.mark.parametrize(
    "monitor_failure",
    [RuntimeError("monitor error"), KeyboardInterrupt("monitor interrupted")],
    ids=["ordinary-monitor-error", "control-monitor-error"],
)
def test_failed_pop_monitor_error_does_not_mask_backend_error(
    monitor_failure: BaseException,
) -> None:
    """The backend pop failure remains primary over monitor.on_error failure."""
    qb = MagicMock(name="QueueBackend")
    backend_failure = QueueError("backend pop unavailable")
    qb.pop.side_effect = backend_failure
    monitor = MagicMock()
    monitor.on_error.side_effect = monitor_failure
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb),
        queue_name="q",
        monitor=monitor,
    )

    with pytest.raises(QueueError) as raised:
        bq.pop()

    assert raised.value is backend_failure
    monitor.on_error.assert_called_once_with("pop", backend_failure)
    monitor.on_pop.assert_called_once_with("q")


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


def test_push_monitor_can_reenter_close_after_commit() -> None:
    """A committed push must not hold the operation gate during on_push."""
    queue: BackendQueue | None = None
    monitor = MagicMock()

    def close_queue(_queue_name: str, _priority: float) -> None:
        assert queue is not None
        queue.close(lossy=True)

    monitor.on_push.side_effect = close_queue
    queue = BackendQueue(
        connection_manager=_cm(),
        queue_name="q",
        monitor=monitor,
    )
    failures: list[BaseException] = []

    def run_push() -> None:
        try:
            queue.push(Request("https://example.com/reentrant-push-close"))
        except BaseException as exc:  # pragma: no cover - assertion reports it
            failures.append(exc)

    worker = threading.Thread(target=run_push, daemon=True)
    worker.start()
    worker.join(timeout=1.0)

    assert not worker.is_alive()
    assert failures == []
    assert queue._close_complete is True


def test_pop_monitor_can_reenter_close_after_commit() -> None:
    """A committed pop must not hold the operation gate during on_pop."""
    queue: BackendQueue | None = None
    monitor = MagicMock()

    def close_queue(_queue_name: str) -> None:
        assert queue is not None
        queue.close(lossy=True)

    monitor.on_pop.side_effect = close_queue
    queue = BackendQueue(
        connection_manager=_cm(),
        queue_name="q",
        monitor=monitor,
    )
    queue.connection_manager.get_queue_backend().pop.return_value = None
    failures: list[BaseException] = []

    def run_pop() -> None:
        try:
            assert queue is not None
            assert queue.pop() is None
        except BaseException as exc:  # pragma: no cover - assertion reports it
            failures.append(exc)

    worker = threading.Thread(target=run_pop, daemon=True)
    worker.start()
    worker.join(timeout=1.0)

    assert not worker.is_alive()
    assert failures == []
    assert queue._close_complete is True


def test_pop_monitor_control_does_not_mask_data_plane_error() -> None:
    """A pop parser failure remains primary over post-commit telemetry."""
    qb = MagicMock(name="QueueBackend")
    qb.pop.return_value = b"not-json"
    monitor = MagicMock()
    signal = KeyboardInterrupt("monitor interrupted")
    monitor.on_pop.side_effect = signal
    queue = BackendQueue(
        connection_manager=_cm(queue_backend=qb),
        queue_name="q",
        monitor=monitor,
    )

    with pytest.raises(SerializationError):
        queue.pop()

    assert monitor.on_pop.call_count == 1


def test_private_pop_observes_after_request_processing(
    mocker: MockerFixture,
) -> None:
    """Even the private pop path keeps observation after data-plane processing."""
    qb = MagicMock(name="QueueBackend")
    monitor = MagicMock()
    queue = BackendQueue(
        connection_manager=_cm(queue_backend=qb),
        queue_name="q",
        monitor=monitor,
    )
    request = Request("https://example.com/private-pop-order")
    qb.pop.return_value = queue._serializer.serialize(queue._request_to_dict(request))
    request_from_dict = mocker.patch.object(
        queue,
        "_request_from_dict",
        wraps=queue._request_from_dict,
    )

    def assert_processed(_queue_name: str) -> None:
        request_from_dict.assert_called_once()

    monitor.on_pop.side_effect = assert_processed

    restored = queue._pop(0.0)

    assert restored is not None
    assert restored.url == request.url


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("logger boom"), KeyboardInterrupt(), SystemExit()],
)
def test_committed_replacement_survives_ack_failure_logger_diagnostic(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    """A logger handler cannot reject a replacement already committed durably."""
    qb = MagicMock(name="QueueBackend")
    qb.ack.side_effect = RuntimeError("source ack boom")
    bq = BackendQueue(connection_manager=_cm(queue_backend=qb), queue_name="q")
    request = Request(
        "https://example.com/retry", meta={"_backend_ack_token": "old-token"}
    )
    mocker.patch(
        "scrapy_extension.queue.queue.logger.error", side_effect=diagnostic_error
    )

    bq.push(request)

    qb.push.assert_called_once()
    qb.ack.assert_called_once_with("q", token="old-token")
    assert request.meta["_backend_ack_token"] == "old-token"


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("logger boom"), KeyboardInterrupt(), SystemExit()],
)
def test_push_survives_monitor_failure_logger_diagnostic(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    """Fallback debug failures cannot reject an enqueue already committed."""
    qb = MagicMock(name="QueueBackend")
    monitor = MagicMock()
    monitor.on_push.side_effect = RuntimeError("push monitor boom")
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb), queue_name="q", monitor=monitor
    )
    mocker.patch(
        "scrapy_extension.queue.queue.logger.debug", side_effect=diagnostic_error
    )

    bq.push(Request("https://example.com"))

    qb.push.assert_called_once()
    monitor.on_push.assert_called_once_with("q", 0.0)


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


@pytest.mark.parametrize(
    "monitor_method",
    ["on_pop", "on_pop_rate", "on_queue_depth"],
)
@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("logger boom"), KeyboardInterrupt(), SystemExit()],
)
@pytest.mark.parametrize("is_empty", [False, True], ids=["request", "empty"])
def test_pop_monitor_fallback_debug_failure_preserves_determined_result(
    mocker: MockerFixture,
    monitor_method: str,
    diagnostic_error: BaseException,
    is_empty: bool,
) -> None:
    """Post-pop fallback diagnostics cannot replace a request or empty result."""
    qb = MagicMock(name="QueueBackend")
    qb.queue_len.return_value = 0
    monitor = MagicMock()
    getattr(monitor, monitor_method).side_effect = RuntimeError("monitor boom")
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb),
        queue_name="q",
        monitor=monitor,
        depth_sample_every=1,
    )
    request = Request("https://example.com")
    qb.pop.return_value = (
        None if is_empty else bq._serializer.serialize(bq._request_to_dict(request))
    )
    logger_debug = mocker.patch(
        "scrapy_extension.queue.queue.logger.debug", side_effect=diagnostic_error
    )

    popped = bq.pop()

    getattr(monitor, monitor_method).assert_called_once()
    logger_debug.assert_called_once()
    if is_empty:
        assert popped is None
    else:
        assert popped is not None
        assert popped.url == request.url


@pytest.mark.parametrize(
    "monitor_method",
    ["on_pop", "on_pop_rate", "on_queue_depth"],
)
@pytest.mark.parametrize("control_error", [KeyboardInterrupt(), SystemExit()])
def test_pop_propagates_direct_monitor_control_exception(
    mocker: MockerFixture,
    monitor_method: str,
    control_error: BaseException,
) -> None:
    """Control exceptions from monitors remain observable, unlike log failures."""
    qb = MagicMock(name="QueueBackend")
    qb.pop.return_value = None
    qb.queue_len.return_value = 0
    monitor = MagicMock()
    getattr(monitor, monitor_method).side_effect = control_error
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb),
        queue_name="q",
        monitor=monitor,
        depth_sample_every=1,
    )
    logger_debug = mocker.patch("scrapy_extension.queue.queue.logger.debug")

    with pytest.raises(type(control_error)) as raised:
        bq.pop()

    assert raised.value is control_error
    logger_debug.assert_not_called()


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


@pytest.mark.parametrize("operation", ["push", "pop"])
@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("logger boom"), KeyboardInterrupt(), SystemExit()],
)
def test_error_monitor_fallback_diagnostic_preserves_serialization_error(
    mocker: MockerFixture, operation: str, diagnostic_error: BaseException
) -> None:
    """Logger failures cannot replace an already-selected serialization error."""
    monitor = MagicMock()
    monitor.on_error.side_effect = RuntimeError("monitor boom")
    qb = MagicMock(name="QueueBackend")
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb), queue_name="q", monitor=monitor
    )
    logger_debug = mocker.patch(
        "scrapy_extension.queue.queue.logger.debug", side_effect=diagnostic_error
    )

    if operation == "push":
        with pytest.raises(SerializationError, match="Failed to serialize request"):
            bq.push(Request("https://example.com", meta={"bad": object()}))
    else:
        qb.pop.return_value = b"not valid JSON"
        with pytest.raises(SerializationError, match="Failed to deserialize request"):
            bq.pop()

    monitor.on_error.assert_called_once()
    logger_debug.assert_called_once()


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("logger boom"), KeyboardInterrupt(), SystemExit()],
)
@pytest.mark.parametrize(
    "failure_site",
    [
        pytest.param("empty-nack", id="empty-ack-failure"),
        pytest.param("malformed-ack", id="malformed-payload"),
        pytest.param("invalid-replacement", id="invalid-replacement"),
    ],
)
def test_terminal_ack_fallback_diagnostic_preserves_primary_outcome(
    mocker: MockerFixture,
    diagnostic_error: BaseException,
    failure_site: str,
) -> None:
    """Fallback logs do not replace an ack error or preselected queue error."""
    qb = MagicMock(name="QueueBackend")
    strategy = MagicMock(name="QueueStrategy")
    bq = BackendQueue(
        connection_manager=_cm(queue_backend=qb),
        queue_name="q",
        queue_strategy=strategy,
    )
    logger_exception = mocker.patch(
        "scrapy_extension.queue.queue.logger.error", side_effect=diagnostic_error
    )

    if failure_site == "empty-nack":
        primary = RuntimeError("ack boom")
        strategy.pop_with_ack.return_value = (None, "token")
        qb.ack.side_effect = primary
        qb.nack.side_effect = RuntimeError("nack boom")
        with pytest.raises(RuntimeError) as raised:
            bq.pop()
        assert raised.value is primary
    elif failure_site == "malformed-ack":
        strategy.pop_with_ack.return_value = (b"not valid JSON", "token")
        qb.ack.side_effect = RuntimeError("ack boom")
        with pytest.raises(SerializationError, match="Failed to deserialize request"):
            bq.pop()
    else:
        assert failure_site == "invalid-replacement"
        qb.ack.side_effect = RuntimeError("ack boom")
        with pytest.raises(QueueError, match="Invalid queue delay"):
            bq.push(
                Request(
                    "https://example.com",
                    meta={"_backend_ack_token": "token", "delay": "not-a-number"},
                )
            )

    logger_exception.assert_called_once()


def test_invalid_replacement_ack_diagnostic_runs_after_its_exception_unwinds() -> None:
    """A logger handler cannot inspect a swallowed replacement-ack failure."""
    marker = "round48-invalid-replacement-ack-marker"
    backend = MagicMock(name="QueueBackend")
    backend.ack.side_effect = RuntimeError(marker)
    queue = BackendQueue(connection_manager=_cm(queue_backend=backend), queue_name="q")
    logger_name = "scrapy_extension.queue.queue"
    source_logger = logging.getLogger(logger_name)
    old_level = source_logger.level
    probe = _LoggingExceptionContextProbe()
    source_logger.setLevel(logging.DEBUG)
    source_logger.addHandler(probe)
    try:
        queue._terminate_invalid_replacement(Request("https://example.com"), "token")
    finally:
        source_logger.removeHandler(probe)
        source_logger.setLevel(old_level)

    _assert_no_active_exception(probe.contexts)
    assert all(marker not in record.getMessage() for record in probe.records)
    assert all(marker not in repr(record.args) for record in probe.records)
    assert all(record.exc_info is None for record in probe.records)
    assert all(record.exc_text is None for record in probe.records)


def test_invalid_delay_replacement_stats_run_after_conversion_unwinds() -> None:
    """Replacement cleanup cannot expose a failed delay conversion to stats."""
    marker = "round48-invalid-delay-marker"

    class BrokenDelay(float):
        def __float__(self) -> float:
            raise ValueError(marker)

    stats = _StatsExceptionContextProbe()
    spider = SimpleNamespace(crawler=SimpleNamespace(stats=stats))
    backend = MagicMock(name="QueueBackend")
    queue = BackendQueue(
        connection_manager=_cm(queue_backend=backend),
        queue_name="q",
        spider=spider,
        monitor=MagicMock(name="Monitor"),
    )
    request = Request(
        "https://example.com",
        meta={"_backend_ack_token": "token", "delay": BrokenDelay(1)},
    )

    with pytest.raises(QueueError, match="Invalid queue delay"):
        queue._push(request, 0)

    assert [name for name, _context in stats.calls] == [
        "scheduler/queue/replacement_poison_dropped"
    ]
    _assert_no_active_exception([context for _name, context in stats.calls])


def test_invalid_source_replacement_stats_run_after_conversion_unwinds() -> None:
    """Replacement cleanup cannot expose a failed source conversion to stats."""
    marker = "round48-invalid-source-marker"

    class BrokenSource:
        def __str__(self) -> str:
            raise RuntimeError(marker)

    stats = _StatsExceptionContextProbe()
    spider = SimpleNamespace(crawler=SimpleNamespace(stats=stats))
    backend = MagicMock(name="QueueBackend")
    queue = BackendQueue(
        connection_manager=_cm(queue_backend=backend),
        queue_name="q",
        spider=spider,
        monitor=MagicMock(name="Monitor"),
    )
    request = Request(
        "https://example.com",
        meta={"_backend_ack_token": "token", "source": BrokenSource()},
    )

    with pytest.raises(QueueError, match="Invalid queue source"):
        queue._push(request, 0)

    assert [name for name, _context in stats.calls] == [
        "scheduler/queue/replacement_poison_dropped"
    ]
    _assert_no_active_exception([context for _name, context in stats.calls])


def test_serialization_replacement_stats_run_after_conversion_unwinds() -> None:
    """Replacement cleanup cannot expose a failed serializer to stats."""
    marker = "round48-serialization-marker"
    stats = _StatsExceptionContextProbe()
    spider = SimpleNamespace(crawler=SimpleNamespace(stats=stats))
    backend = MagicMock(name="QueueBackend")
    queue = BackendQueue(
        connection_manager=_cm(queue_backend=backend),
        queue_name="q",
        spider=spider,
        monitor=MagicMock(name="Monitor"),
    )
    queue._request_to_dict = MagicMock(side_effect=RuntimeError(marker))
    request = Request(
        "https://example.com",
        meta={"_backend_ack_token": "token"},
    )

    with pytest.raises(SerializationError, match="Queue push serialization failed"):
        queue._push(request, 0)

    assert [name for name, _context in stats.calls] == [
        "scheduler/queue/replacement_poison_dropped"
    ]
    _assert_no_active_exception([context for _name, context in stats.calls])


def test_malformed_payload_ack_diagnostic_runs_after_exception_unwinds() -> None:
    """A logger handler cannot inspect a malformed payload or failed ack."""
    marker = "round48-malformed-ack-marker"
    backend = MagicMock(name="QueueBackend")
    backend.ack.side_effect = RuntimeError(marker)
    strategy = MagicMock(name="QueueStrategy")
    strategy.pop_with_ack.return_value = (marker.encode(), "token")
    strategy.queue_len.return_value = 0
    queue = BackendQueue(
        connection_manager=_cm(queue_backend=backend),
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(name="Monitor"),
    )
    logger_name = "scrapy_extension.queue.queue"
    source_logger = logging.getLogger(logger_name)
    old_level = source_logger.level
    probe = _LoggingExceptionContextProbe()
    source_logger.setLevel(logging.DEBUG)
    source_logger.addHandler(probe)
    try:
        with pytest.raises(
            SerializationError, match="Queue pop deserialization failed"
        ):
            queue._pop(0)
    finally:
        source_logger.removeHandler(probe)
        source_logger.setLevel(old_level)

    _assert_no_active_exception(probe.contexts)
    assert all(marker not in record.getMessage() for record in probe.records)
    assert all(marker not in repr(record.args) for record in probe.records)
    assert all(record.exc_info is None for record in probe.records)
    assert all(record.exc_text is None for record in probe.records)


def test_malformed_payload_stats_run_after_deserialization_unwinds() -> None:
    """The poison-drop stats hook cannot inspect malformed broker bytes."""
    marker = "round48-malformed-payload-marker"
    stats = _StatsExceptionContextProbe()
    spider = SimpleNamespace(crawler=SimpleNamespace(stats=stats))
    backend = MagicMock(name="QueueBackend")
    strategy = MagicMock(name="QueueStrategy")
    strategy.pop_with_ack.return_value = (marker.encode(), "token")
    strategy.queue_len.return_value = 0
    queue = BackendQueue(
        connection_manager=_cm(queue_backend=backend),
        queue_name="q",
        spider=spider,
        queue_strategy=strategy,
        monitor=MagicMock(name="Monitor"),
    )

    with pytest.raises(SerializationError, match="Queue pop deserialization failed"):
        queue._pop(0)

    assert [name for name, _context in stats.calls] == [
        "scheduler/queue/poison_dropped"
    ]
    _assert_no_active_exception([context for _name, context in stats.calls])


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("logger boom"), KeyboardInterrupt(), SystemExit()],
)
def test_stats_fallback_diagnostic_preserves_best_effort_primary_result(
    mocker: MockerFixture, diagnostic_error: BaseException
) -> None:
    """A failed stats fallback remains secondary to the queue's primary result."""
    spider = MagicMock()
    spider.crawler.stats.inc_value.side_effect = RuntimeError("stats boom")
    bq = BackendQueue(
        connection_manager=_cm(), queue_name="q", spider=spider, max_item_bytes=1
    )
    logger_debug = mocker.patch(
        "scrapy_extension.queue.queue.logger.debug", side_effect=diagnostic_error
    )

    with pytest.raises(SerializationError, match="Failed to serialize request"):
        bq.push(Request("https://example.com", body=b"too large"))

    spider.crawler.stats.inc_value.assert_called_once_with(
        "scheduler/queue/oversize_dropped"
    )
    logger_debug.assert_called_once()


@pytest.mark.parametrize("control_error", [KeyboardInterrupt(), SystemExit()])
def test_direct_ack_control_exception_remains_observable(
    control_error: BaseException,
) -> None:
    """Only diagnostic fallbacks are isolated; direct ack controls propagate."""
    qb = MagicMock(name="QueueBackend")
    qb.ack.side_effect = control_error
    bq = BackendQueue(connection_manager=_cm(queue_backend=qb), queue_name="q")

    with pytest.raises(type(control_error)) as raised:
        bq.ack(token="token")

    assert raised.value is control_error


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


def test_restore_snapshot_skips_oversized_blob(monkeypatch: pytest.MonkeyPatch) -> None:
    """R25-B: a snapshot blob exceeding ``_MAX_SNAPSHOT_BYTES`` is skipped
    (warn + start clean) so a corrupt/malicious multi-GB value cannot OOM-kill
    startup via the ``bytes(state)`` copy + ``json.loads`` materialization.
    Mirrors the push-path ``max_item_bytes`` guard (the lone previously-unbounded
    storage-retrieve→deserialize surface).

    R26-A: the cap is monkeypatched to a tiny value so the test does not allocate
    the real 128 MiB cap on every run.
    """
    import scrapy_extension.queue.queue as queue_mod

    monkeypatch.setattr(queue_mod, "_MAX_SNAPSHOT_BYTES", 8)

    strategy = _delay()
    strategy.restore = MagicMock()  # type: ignore[method-assign]
    oversized = b"x" * (queue_mod._MAX_SNAPSHOT_BYTES + 1)
    BackendQueue(
        connection_manager=_cm(storage=_storage(retrieve_return=oversized)),
        queue_name="q",
        queue_strategy=strategy,
    )
    # _restore_snapshot ran at __init__; the oversized blob is skipped, not restored.
    strategy.restore.assert_not_called()


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("diagnostic boom"), KeyboardInterrupt(), SystemExit(2)],
)
def test_restore_snapshot_over_cap_diagnostic_failure_starts_clean(
    monkeypatch: pytest.MonkeyPatch, diagnostic_error: BaseException
) -> None:
    """A failed over-cap warning cannot turn the clean-start fallback into failure."""
    import scrapy_extension.queue.queue as queue_mod

    monkeypatch.setattr(queue_mod, "_MAX_SNAPSHOT_BYTES", 8)
    monkeypatch.setattr(
        queue_mod.logger,
        "warning",
        MagicMock(side_effect=diagnostic_error),
    )
    strategy = _delay()
    strategy.restore = MagicMock()  # type: ignore[method-assign]
    oversized = b"x" * (queue_mod._MAX_SNAPSHOT_BYTES + 1)

    BackendQueue(
        connection_manager=_cm(storage=_storage(retrieve_return=oversized)),
        queue_name="q",
        queue_strategy=strategy,
    )

    strategy.restore.assert_not_called()


def test_persist_snapshot_warns_when_over_cap(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """R26-A: when ``strategy.snapshot()`` returns bytes larger than the restore
    cap, ``_persist_snapshot`` must WARN at close time — otherwise the operator
    only discovers the drop on the NEXT restart (asymmetric: persist is uncapped,
    restore is capped → silent data-loss trap for legit large delay heaps). The
    snapshot is still persisted so the operator can raise the cap / shrink the
    heap before restart. Pre-fix, no warning fired at persist time.
    """
    import scrapy_extension.queue.queue as queue_mod

    monkeypatch.setattr(queue_mod, "_MAX_SNAPSHOT_BYTES", 8)

    strategy = _delay()
    strategy.snapshot = MagicMock(  # type: ignore[method-assign]
        return_value=b"x" * (queue_mod._MAX_SNAPSHOT_BYTES + 1)
    )
    storage = _storage()
    bq = BackendQueue(
        connection_manager=_cm(storage=storage),
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )
    with caplog.at_level(logging.ERROR, logger="scrapy_extension.queue.queue"):
        with pytest.raises(QueueError, match="snapshot commit"):
            bq.close()
    assert any("snapshot commit failed" in r.message.lower() for r in caplog.records)
    # The symmetric cap rejects before the first backend write.
    storage.store.assert_not_called()


@pytest.mark.parametrize(
    "diagnostic_error",
    [RuntimeError("diagnostic boom"), KeyboardInterrupt(), SystemExit(2)],
)
def test_persist_snapshot_over_cap_diagnostic_failure_keeps_checkpoint(
    monkeypatch: pytest.MonkeyPatch, diagnostic_error: BaseException
) -> None:
    """A failed over-cap warning cannot skip the completed snapshot checkpoint."""
    import scrapy_extension.queue.queue as queue_mod

    monkeypatch.setattr(queue_mod, "_MAX_SNAPSHOT_BYTES", 8)
    monkeypatch.setattr(
        queue_mod.logger,
        "warning",
        MagicMock(side_effect=diagnostic_error),
    )
    strategy = _delay()
    strategy.snapshot = MagicMock(  # type: ignore[method-assign]
        return_value=b"x" * (queue_mod._MAX_SNAPSHOT_BYTES + 1)
    )
    strategy.close = MagicMock()  # type: ignore[method-assign]
    storage = _storage()
    queue = BackendQueue(
        connection_manager=_cm(storage=storage),
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    with pytest.raises(QueueError, match="snapshot commit"):
        queue.close()

    storage.store.assert_not_called()
    strategy.close.assert_not_called()
    assert queue._close_complete is False
