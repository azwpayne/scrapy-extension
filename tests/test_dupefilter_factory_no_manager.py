"""Regression coverage for manager-free in-process dedup strategies."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from scrapy.settings import Settings

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.dupefilter.filters.factory import (
    DedupeStrategy,
    build_membership_filter,
)
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions import ConfigurationError

_LOCAL_STRATEGY_FILTERS: dict[str, type[Any]] = {
    "memory": MemoryMembershipFilter,
    "bloom": BloomMembershipFilter,
    "cuckoo": CuckooMembershipFilter,
}


@pytest.mark.parametrize("strategy", sorted(_LOCAL_STRATEGY_FILTERS))
def test_local_strategy_skips_backend_resolution_and_manager_lease(
    strategy: str, mocker: Any
) -> None:
    from scrapy_extension.backends import connectors as connectors_module

    resolve = mocker.patch.object(
        connectors_module,
        "resolve_backend_config",
        side_effect=AssertionError("local strategy resolved backend configuration"),
    )
    acquire = mocker.patch.object(
        connectors_module.ConnectionManager,
        "acquire_lease",
        side_effect=AssertionError("local strategy acquired a manager lease"),
    )

    dupefilter = BackendDupeFilter.from_settings(
        Settings(
            {
                "SCRAPY_DEDUP_STRATEGY": strategy,
                "SCRAPY_SET_BACKEND_TYPE": "not-a-backend",
            }
        )
    )

    assert isinstance(dupefilter._filter, _LOCAL_STRATEGY_FILTERS[strategy])
    assert dupefilter.connection_manager is None
    resolve.assert_not_called()
    acquire.assert_not_called()
    dupefilter.close("test")
    assert dupefilter._closed is True


def test_set_strategy_still_acquires_and_releases_manager_lease(mocker: Any) -> None:
    from scrapy_extension.backends.connectors import ConnectionManager

    manager = mocker.Mock(name="manager")
    lease = SimpleNamespace(manager=manager, release=mocker.Mock(name="release"))
    acquire = mocker.patch.object(
        ConnectionManager,
        "acquire_lease",
        return_value=lease,
    )

    dupefilter = BackendDupeFilter.from_settings(
        Settings({"SCRAPY_BACKEND_TYPE": "redis"})
    )

    assert dupefilter.connection_manager is manager
    acquire.assert_called_once_with(backend_type="redis", settings={})
    dupefilter.close("test")
    lease.release.assert_called_once_with()
    manager.close.assert_not_called()


def test_set_strategy_requires_set_capable_backend() -> None:
    with pytest.raises(ConfigurationError, match="missing capabilities"):
        BackendDupeFilter.from_settings(
            Settings({"SCRAPY_BACKEND_TYPE": "kafka", "SCRAPY_DEDUP_STRATEGY": "set"})
        )


def test_build_set_filter_without_manager_raises() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        build_membership_filter(DedupeStrategy.SET, None)

    assert exc_info.value.setting_name == "SCRAPY_DEDUP_STRATEGY"


@pytest.mark.parametrize("strategy", sorted(_LOCAL_STRATEGY_FILTERS))
def test_build_local_filter_accepts_no_manager(strategy: str) -> None:
    membership_filter = build_membership_filter(
        DedupeStrategy(strategy),
        None,
        bloom_capacity=10,
        cuckoo_capacity=10,
    )

    assert isinstance(membership_filter, _LOCAL_STRATEGY_FILTERS[strategy])


def test_constructor_without_manager_or_filter_raises() -> None:
    with pytest.raises(ConfigurationError):
        BackendDupeFilter(connection_manager=None)


def test_constructor_rejects_lease_without_manager(mocker: Any) -> None:
    with pytest.raises(
        ValueError, match="connection_manager_lease requires connection_manager"
    ):
        BackendDupeFilter(
            connection_manager=None,
            membership_filter=MemoryMembershipFilter(maxsize=10),
            connection_manager_lease=mocker.MagicMock(name="ConnectionManagerLease"),
        )


def test_local_strategy_from_crawler_wires_stats_without_manager(mocker: Any) -> None:
    from scrapy_extension.monitor import ScrapyStatsMonitor

    dupefilter = BackendDupeFilter(
        connection_manager=None,
        membership_filter=BloomMembershipFilter(capacity=10, error_rate=0.01),
    )
    mocker.patch.object(BackendDupeFilter, "from_settings", return_value=dupefilter)
    crawler = mocker.MagicMock(name="crawler")

    result = BackendDupeFilter.from_crawler(crawler)

    assert result is dupefilter
    assert isinstance(dupefilter._monitor, ScrapyStatsMonitor)
