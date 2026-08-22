"""Deterministic lifecycle-edge contracts for :class:`BackendSpiderMixin`."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from scrapy import Spider
from twisted.internet.defer import Deferred

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.spider import spider_mixin as spider_module
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin


class _Spider(BackendSpiderMixin, Spider):
    name = "lifecycle-edges"
    backend_type = BackendType.REDIS


class _PlainSpider(BackendSpiderMixin, Spider):
    name = "plain-lifecycle-edges"
    backend_type = None


def _spider_with_manager(mocker):
    spider = _Spider()
    manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = manager
    return spider, manager


def test_from_crawler_finalizes_only_configured_backend(mocker):
    from scrapy.settings import Settings

    crawler = MagicMock()
    crawler.settings = Settings()
    setup = mocker.patch.object(BackendSpiderMixin, "setup_backend")
    configured = _Spider.from_crawler(crawler)
    plain = _PlainSpider.from_crawler(crawler)
    assert configured.crawler is crawler
    setup.assert_called_once_with()
    assert plain.backend_type is None


def test_setup_publishes_exact_legacy_lease_after_wiring(mocker):
    spider = _Spider()
    crawler = MagicMock()
    from scrapy.settings import Settings

    crawler.settings = Settings({"SCRAPY_BACKEND_CIRCUIT_BREAKER_ENABLED": True})
    spider.crawler = crawler
    manager = mocker.MagicMock(spec=ConnectionManager)
    lease = mocker.MagicMock(name="legacy-lease")
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)
    adopt = mocker.patch.object(
        ConnectionManager, "_adopt_latest_legacy_lease", return_value=lease
    )
    wiring = mocker.patch.object(spider, "_connect_signals")
    assert spider.setup_backend() is manager
    adopt.assert_called_once_with(manager)
    wiring.assert_called_once_with()
    assert spider._connection_manager is manager
    assert spider._connection_manager_lease is lease
    spider.close_backend()


def test_plain_setup_without_backend_type_has_explicit_error():
    with pytest.raises(RuntimeError, match="backend_type must be set"):
        _PlainSpider().setup_backend()


def test_connect_signals_replaces_legacy_dispatcher(mocker):
    spider = _Spider()
    old = MagicMock(name="old-signals")
    new = MagicMock(name="new-signals")
    crawler = MagicMock()
    crawler.signals = new
    spider.crawler = crawler
    spider._signals_connected = True
    spider._connected_signals = old
    disconnect = mocker.patch.object(spider, "_disconnect_lifecycle_signals")
    spider._connect_signals()
    disconnect.assert_called_once_with(old, strict=True)
    assert new.connect.call_count == 2
    assert all(lease.manager is new for lease in spider._signal_leases)


def test_lifecycle_helpers_settle_submission_failure(monkeypatch):
    def reject(*_args, **_kwargs):
        raise KeyboardInterrupt("submit")

    monkeypatch.setattr(spider_module, "deferToThread", reject)
    result = spider_module._submit_thread(lambda: None)
    failures: list[BaseException] = []
    result.addErrback(lambda failure: failures.append(failure.value))
    assert failures and isinstance(failures[0], KeyboardInterrupt)
    spider_module._emit_diagnostic(
        lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt("log")), "ignored"
    )


def test_start_component_close_selects_authoritative_deferred(monkeypatch):
    class QueueBackend:
        def close(self):
            return authoritative

    worker: Deferred[object] = Deferred()
    authoritative: Deferred[object] = Deferred()
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(spider_module, "deferToThread", lambda *_a, **_k: worker)
    monkeypatch.setattr("scrapy_extension.queue.queue.BackendQueue", QueueBackend)
    operation, error, succeeded = _Spider()._start_component_close(
        "queue", QueueBackend(), ""
    )
    assert operation is worker and error is None and not succeeded
    worker.callback((True, authoritative))
    assert operation.result is authoritative
    authoritative.callback(None)
    assert operation.result is None


def test_start_component_close_retains_worker_when_callback_adapter_rejects(
    monkeypatch,
):
    class Rejecting:
        called = False

        def addCallback(self, _callback):
            raise KeyboardInterrupt("adapter")

    class QueueBackend:
        def close(self):
            return None

    worker = Rejecting()
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(spider_module, "deferToThread", lambda *_a, **_k: worker)
    monkeypatch.setattr("scrapy_extension.queue.queue.BackendQueue", QueueBackend)
    operation, error, succeeded = _Spider()._start_component_close(
        "queue", QueueBackend(), ""
    )
    assert operation is worker and error is None and not succeeded


def test_start_owner_operation_direct_and_offloaded_paths(mocker, monkeypatch):
    spider = _Spider()
    owner = mocker.MagicMock()
    owner.close.side_effect = KeyboardInterrupt("owner")
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: False)
    operation, error, succeeded = spider._start_owner_operation(owner, "close")
    assert operation is None and isinstance(error, KeyboardInterrupt) and not succeeded

    worker: Deferred[object] = Deferred()
    manager = ConnectionManager(BackendType.REDIS, {})
    mocker.patch.object(manager, "close", return_value=None)
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(spider_module, "deferToThread", lambda *_a, **_k: worker)
    operation, error, succeeded = spider._start_owner_operation(manager, "close")
    assert operation is worker and error is None and not succeeded
    worker.callback("normal")
    assert worker.result == "normal"


def test_start_owner_operation_selector_rejection_is_retryable(mocker, monkeypatch):
    class Rejecting:
        called = False

        def addCallback(self, _callback):
            raise RuntimeError("selector")

    manager = ConnectionManager(BackendType.REDIS, {})
    mocker.patch.object(manager, "close", return_value=None)
    worker = Rejecting()
    monkeypatch.setattr(spider_module, "reactor_is_running", lambda: True)
    monkeypatch.setattr(spider_module, "deferToThread", lambda *_a, **_k: worker)
    operation, error, succeeded = _Spider()._start_owner_operation(manager, "close")
    assert operation is worker and error is None and not succeeded


def test_orphan_candidate_failure_and_prior_failure_are_retained(mocker):
    spider, _manager = _spider_with_manager(mocker)
    lease = mocker.MagicMock(name="lease")
    candidate = SimpleNamespace(_connection_manager_lease=lease)
    spider._orphan_candidates = [("scheduler", candidate)]
    error = spider._finish_orphan_candidate_close(
        "scheduler", candidate, RuntimeError("ignored")
    )
    assert error is None
    error = spider._finish_orphan_candidate_close(
        "scheduler", candidate, spider_module.TwistedFailure(RuntimeError("failed"))
    )
    assert isinstance(error, RuntimeError)
    assert spider._orphan_candidate_failures[id(candidate)] is error
    assert spider._orphan_leases == [lease]
    assert spider._finish_orphan_candidate_close("scheduler", candidate, None) is error
    spider._orphan_candidate_failures.pop(id(candidate))
    assert spider._finish_orphan_candidate_close("scheduler", candidate, None) is None
    assert not spider._orphan_candidates


def test_dispose_candidate_is_idempotent_and_waits_for_close(mocker):
    spider, _manager = _spider_with_manager(mocker)
    cleanup: Deferred[None] = Deferred()
    candidate = SimpleNamespace(close=MagicMock(return_value=cleanup))
    spider._dispose_invalidated_candidate("scheduler", candidate, "retry")
    spider._dispose_invalidated_candidate("scheduler", candidate, "retry")
    assert candidate.close.call_count == 1
    assert id(candidate) in spider._orphan_candidate_operations
    cleanup.callback(None)
    assert not spider._orphan_candidates


def test_dispose_candidate_handles_callback_adapter_failure(mocker):
    spider, _manager = _spider_with_manager(mocker)

    class Rejecting:
        def addBoth(self, _callback):
            raise KeyboardInterrupt("both")

        def addErrback(self, _callback):
            raise KeyboardInterrupt("errback")

    candidate = SimpleNamespace(close=MagicMock())
    operation = Rejecting()
    mocker.patch.object(
        spider, "_start_component_close", return_value=(operation, None, False)
    )
    spider._dispose_invalidated_candidate("scheduler", candidate, "retry")
    assert spider._orphan_candidates == [("scheduler", candidate)]


def test_orphan_cleanup_runs_candidate_lease_and_manager_phases(mocker):
    spider, _manager = _spider_with_manager(mocker)
    candidate = SimpleNamespace(close=MagicMock(return_value=None))
    lease = mocker.MagicMock(name="lease")
    manager = mocker.MagicMock(name="manager")
    spider._orphan_candidates = [("scheduler", candidate)]
    spider._orphan_leases = [lease]
    spider._orphan_managers = [manager]
    assert spider._cleanup_orphan_candidates("retry") is None
    assert not spider._orphan_candidates
    assert not spider._orphan_leases
    assert not spider._orphan_managers
    lease.release.assert_called_once_with()
    manager.close.assert_called_once_with()


def test_orphan_cleanup_attachment_failure_becomes_retry_error(mocker):
    spider, _manager = _spider_with_manager(mocker)

    class Rejecting:
        def addCallbacks(self, _success, _failure):
            raise RuntimeError("attach")

        def addErrback(self, _failure):
            raise RuntimeError("observe")

    candidate = SimpleNamespace(close=MagicMock())
    spider._orphan_candidates = [("scheduler", candidate)]
    spider._orphan_candidate_operations[id(candidate)] = Rejecting()  # type: ignore[assignment]
    error = spider._cleanup_orphan_candidates("retry")
    assert isinstance(error, RuntimeError) and str(error) == "attach"
    assert spider._orphan_candidates == [("scheduler", candidate)]


def test_orphan_cleanup_waits_for_async_lease_and_manager(mocker):
    spider, _manager = _spider_with_manager(mocker)
    lease = mocker.MagicMock(name="lease")
    manager = mocker.MagicMock(name="manager")
    lease_done: Deferred[None] = Deferred()
    manager_done: Deferred[None] = Deferred()
    lease.release.return_value = lease_done
    manager.close.return_value = manager_done
    spider._orphan_leases = [lease]
    spider._orphan_managers = [manager]
    cleanup = spider._cleanup_orphan_candidates("retry")
    assert isinstance(cleanup, Deferred)
    lease_done.callback(None)
    manager_done.callback(None)
    assert cleanup.called
    assert not spider._orphan_leases and not spider._orphan_managers
