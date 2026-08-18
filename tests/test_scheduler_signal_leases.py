"""Per-registration scheduler signal ownership regressions."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import Mock

import pytest
from pydispatch.errors import DispatcherKeyError

from scrapy_extension.schedule.scheduler import BackendScheduler


class _SignalManager:
    def __init__(self) -> None:
        self.registrations: set[tuple[int, int]] = set()
        self.connect_after_effect: BaseException | None = None
        self.disconnect_after_effect: BaseException | None = None

    def connect(self, receiver: Callable[..., Any], *, signal: object) -> None:
        self.registrations.add((id(receiver), id(signal)))
        if self.connect_after_effect is not None:
            error = self.connect_after_effect
            self.connect_after_effect = None
            raise error

    def disconnect(self, receiver: Callable[..., Any], *, signal: object) -> None:
        key = (id(receiver), id(signal))
        if key not in self.registrations:
            raise DispatcherKeyError("already absent")
        self.registrations.remove(key)
        if self.disconnect_after_effect is not None:
            error = self.disconnect_after_effect
            self.disconnect_after_effect = None
            raise error


def _scheduler_and_spider(manager: _SignalManager) -> tuple[BackendScheduler, Mock]:
    scheduler = BackendScheduler(Mock())
    spider = Mock()
    spider.crawler.signals = manager
    return scheduler, spider


def test_connect_effect_then_raise_retains_unique_lease_for_cleanup() -> None:
    signal_manager = _SignalManager()
    signal_manager.connect_after_effect = RuntimeError("connect failed")
    scheduler, spider = _scheduler_and_spider(signal_manager)

    with pytest.raises(RuntimeError, match="connect failed"):
        scheduler._connect_ack_signals(spider)

    assert len(scheduler._signal_leases) == 1
    assert len(signal_manager.registrations) == 1

    scheduler._disconnect_signal_leases()
    assert scheduler._signal_leases == []
    assert signal_manager.registrations == set()
    assert scheduler._signals_connected is False


def test_disconnect_effect_then_raise_retries_as_idempotent_absence() -> None:
    signal_manager = _SignalManager()
    scheduler, spider = _scheduler_and_spider(signal_manager)
    scheduler._connect_ack_signals(spider)
    assert len(scheduler._signal_leases) == 2
    signal_manager.disconnect_after_effect = RuntimeError("after effect")

    with pytest.raises(RuntimeError, match="after effect"):
        scheduler._disconnect_signal_leases()

    assert len(scheduler._signal_leases) == 2
    assert len(signal_manager.registrations) == 1

    scheduler._disconnect_signal_leases()
    assert scheduler._signal_leases == []
    assert signal_manager.registrations == set()


def test_retrying_old_unique_receiver_does_not_remove_later_registration() -> None:
    signal_manager = _SignalManager()
    first, spider = _scheduler_and_spider(signal_manager)
    first._connect_ack_signals(spider)
    old_lease = first._signal_leases[0]
    first._disconnect_signal_leases()

    second, spider = _scheduler_and_spider(signal_manager)
    second._connect_ack_signals(spider)
    later = set(signal_manager.registrations)

    with pytest.raises(DispatcherKeyError):
        signal_manager.disconnect(old_lease.receiver, signal=old_lease.signal)

    assert signal_manager.registrations == later
    second._disconnect_signal_leases()
